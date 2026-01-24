package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

var ErrKeyEmpty = fmt.Errorf("key cannot be empty")
var ErrValueNil = fmt.Errorf("value cannot be nil")
var ErrBatchClosed = fmt.Errorf("batch has been written or closed")
var ErrUnsafeOptions = fmt.Errorf("unsafe options require AllowUnsafe")
var ErrMemtableFull = fmt.Errorf("memtable full")
var ErrMemtableValueLogPointers = errors.New("memtable value-log pointers require WAL/value-log enabled")
var ErrSplitValueLog = errors.New("split value log requires WAL/value-log enabled")
var errWALClosed = errors.New("cachingdb: wal writer closed")
var errWALUnavailable = errors.New("cachingdb: wal unavailable")

var iteratorDebugEnabled atomic.Bool

const (
	envDebugFlushPointers = "TREEDB_DEBUG_FLUSH_PTRS"

	minMemtablePrealloc        = 64 * 1024
	maxMemtablePrealloc        = 256 << 20
	adaptiveMinWrites          = 1024
	adaptiveSequentialWritePct = 0.85
	adaptiveWarmupBytes        = 16 * 1024 * 1024
	adaptiveModeSwitchStreak   = 2
	maxMemtableBytesPerShard   = int64(3 << 30)
)

// SetIteratorDebug toggles attaching debug metadata to iterators returned by
// CachingDB.Iterator. It is intended for benchmarking/diagnostics.
func SetIteratorDebug(enabled bool) {
	iteratorDebugEnabled.Store(enabled)
}

func envBool(name string) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n != 0
	}
	return false
}

func (db *DB) syncDirBestEffort(dir string) {
	if dir == "" || runtime.GOOS == "windows" {
		return
	}
	f, err := os.Open(dir)
	if err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to open dir %q for sync: %w", dir, err))
		return
	}
	if err := f.Sync(); err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to sync dir %q: %w", dir, err))
	}
	if err := f.Close(); err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to close dir %q after sync: %w", dir, err))
	}
}

func (db *DB) removeFileRetry(path string) error {
	var err error
	for i := 0; i < 20; i++ {
		err = os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		if runtime.GOOS != "windows" {
			return err
		}
		// Windows: Retry with exponential backoff up to ~1s total
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	return err
}

func warnInsecureDir(dir string, notify func(error)) {
	if dir == "" || notify == nil || runtime.GOOS == "windows" {
		return
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		notify(fmt.Errorf("cachingdb: failed to stat dir %q: %w", dir, err))
		return
	}
	if info.Mode()&os.ModeSymlink != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is a symlink; verify target permissions", dir))
		info, err = os.Stat(dir)
		if err != nil {
			notify(fmt.Errorf("cachingdb: failed to stat symlink target %q: %w", dir, err))
			return
		}
	}
	if !info.IsDir() {
		notify(fmt.Errorf("cachingdb: path %q is not a directory", dir))
		return
	}
	perms := info.Mode().Perm()
	if perms&0o002 != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is world-writable (mode %o)", dir, perms))
	} else if perms&0o020 != 0 {
		notify(fmt.Errorf("cachingdb: dir %q is group-writable (mode %o)", dir, perms))
	}
}

func memtableCapacity(flushThreshold int64) int {
	if flushThreshold <= 0 {
		return 0
	}
	capBytes := flushThreshold + flushThreshold/4 // +25% to cover skiplist overhead.
	if capBytes < minMemtablePrealloc {
		capBytes = minMemtablePrealloc
	}
	if capBytes > maxMemtablePrealloc {
		capBytes = maxMemtablePrealloc
	}
	maxInt := int64(int(^uint(0) >> 1))
	if capBytes > maxInt {
		capBytes = maxInt
	}
	return int(capBytes)
}

func normalizeShardCount(n int) int {
	if n < 1 {
		return 1
	}
	// Round down to a power of two.
	v := 1
	for v<<1 <= n {
		v <<= 1
	}
	return v
}

func defaultMemtableShards() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	n *= 2
	if n > 8 {
		n = 8
	}
	return normalizeShardCount(n)
}

func shardCapacity(totalCap, shards int) int {
	if shards <= 1 {
		return totalCap
	}
	if totalCap <= 0 {
		return 1
	}
	cap := totalCap / shards
	if cap <= 0 {
		cap = 1
	}
	return cap
}

func (db *DB) valueLogEnabled() bool {
	return !db.disableValueLog
}

func (db *DB) splitValueLogEnabled() bool {
	return db.splitValueLog && db.valueLogEnabled()
}

func (db *DB) walUsesValueLog() bool {
	return false
}

func (db *DB) pickLane(sync bool) (*lane, error) {
	if db == nil || len(db.lanes) == 0 {
		return nil, errWALUnavailable
	}
	db.laneMu.Lock()
	defer db.laneMu.Unlock()
	for {
		select {
		case <-db.closeCh:
			return nil, errWALClosed
		default:
		}
		start := db.nextLane
		for i := 0; i < len(db.lanes); i++ {
			idx := (start + i) % len(db.lanes)
			l := &db.lanes[idx]
			if l.syncing.Load() {
				continue
			}
			db.nextLane = (idx + 1) % len(db.lanes)
			if sync {
				l.syncing.Store(true)
			}
			return l, nil
		}
		db.laneCond.Wait()
	}
}

func (db *DB) releaseLaneSync(l *lane) {
	if l == nil {
		return
	}
	if !l.syncing.CompareAndSwap(true, false) {
		return
	}
	db.laneMu.Lock()
	db.laneCond.Broadcast()
	db.laneMu.Unlock()
}

func (db *DB) currentValueLogPath(l *lane) string {
	if l == nil {
		return ""
	}
	if db.splitValueLogEnabled() {
		l.vlogMu.Lock()
		path := l.vlogPath
		l.vlogMu.Unlock()
		return path
	}
	l.walMu.Lock()
	path := l.walPath
	l.walMu.Unlock()
	return path
}

func (db *DB) currentValueLogSeq(l *lane) int {
	if l == nil {
		return 0
	}
	if db.splitValueLogEnabled() {
		l.vlogMu.Lock()
		seq := l.vlogSeq
		l.vlogMu.Unlock()
		return seq
	}
	l.walMu.Lock()
	seq := l.walSeq
	l.walMu.Unlock()
	return seq
}

func (db *DB) currentWALPaths() []string {
	if db == nil || db.disableJournal {
		return nil
	}
	paths := make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		if l.walPath != "" {
			paths = append(paths, l.walPath)
		}
		l.walMu.Unlock()
	}
	return paths
}

func (db *DB) currentValueLogPaths() []string {
	if db == nil || !db.valueLogEnabled() {
		return nil
	}
	paths := make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		if db.splitValueLogEnabled() {
			l.vlogMu.Lock()
			if l.vlogPath != "" {
				paths = append(paths, l.vlogPath)
			}
			l.vlogMu.Unlock()
			continue
		}
		l.walMu.Lock()
		if l.walPath != "" {
			paths = append(paths, l.walPath)
		}
		l.walMu.Unlock()
	}
	return paths
}

// SetDictStore installs the dictionary store for current-ID freezing.
func (db *DB) SetDictStore(store DictStore) {
	if db == nil {
		return
	}
	db.dictStore = store
	db.dictCurrentCached.Store(0)
	db.dictCurrentOps.Store(0)
	if store != nil {
		if dictID, err := store.GetCurrent(context.Background()); err == nil {
			db.dictCurrentCached.Store(dictID)
		}
	}
	db.valueLogDictBytesMu.Lock()
	db.valueLogDictBytesID = 0
	db.valueLogDictBytes = nil
	db.valueLogDictBytesMu.Unlock()
	if db.valueLogReader != nil && store != nil {
		db.valueLogReader.SetDictLookup(func(dictID uint64) ([]byte, error) {
			return store.GetDictBytes(context.Background(), dictID)
		})
	}
	db.ensureValueLogDictTrainer()
}

func (db *DB) currentDictID(ctx context.Context) (uint64, error) {
	if db == nil || db.dictStore == nil {
		return 0, nil
	}
	if db.valueLogDictPaused() {
		return 0, nil
	}
	// Avoid per-write dictdb reads on the hot path; refresh every N uses.
	const refreshEvery = uint64(1 << 16)
	seq := db.dictCurrentOps.Add(1)
	if seq&(refreshEvery-1) != 0 {
		return db.dictCurrentCached.Load(), nil
	}
	dictID, err := db.dictStore.GetCurrent(ctx)
	if err != nil {
		// Fall back to cached value on transient errors (best-effort).
		return db.dictCurrentCached.Load(), nil
	}
	db.dictCurrentCached.Store(dictID)
	return dictID, nil
}

func (db *DB) dictBytes(ctx context.Context, dictID uint64) ([]byte, error) {
	if dictID == 0 {
		return nil, nil
	}
	if db == nil || db.dictStore == nil {
		return nil, valuelog.ErrMissingDict
	}
	db.valueLogDictBytesMu.Lock()
	if db.valueLogDictBytesID == dictID && len(db.valueLogDictBytes) > 0 {
		out := db.valueLogDictBytes
		db.valueLogDictBytesMu.Unlock()
		return out, nil
	}
	db.valueLogDictBytesMu.Unlock()

	out, err := db.dictStore.GetDictBytes(ctx, dictID)
	if err != nil {
		return nil, err
	}
	db.valueLogDictBytesMu.Lock()
	db.valueLogDictBytesID = dictID
	db.valueLogDictBytes = out
	db.valueLogDictBytesMu.Unlock()
	return out, nil
}

func (db *DB) readValueLog(ptr page.ValuePtr) ([]byte, error) {
	if db.valueLogReader == nil {
		return nil, errors.New("cachingdb: value-log reader unavailable")
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("cachingdb: non value-log pointer %#x", ptr.FileID)
	}
	if db.memtableValueLogPointers && db.valueLogEnabled() {
		if err := db.flushValueLogForPtr(ptr); err != nil {
			return nil, err
		}
	}
	return db.valueLogReader.Read(ptr)
}

func (db *DB) flushValueLogForPtr(ptr page.ValuePtr) error {
	if !db.valueLogEnabled() {
		return nil
	}
	laneID, seq := valuelog.DecodeFileID(ptr.FileID)
	if laneID >= uint32(len(db.lanes)) {
		return nil
	}
	l := &db.lanes[laneID]
	currentSeq := db.currentValueLogSeq(l)
	if currentSeq == int(seq) {
		return db.flushValueLogLane(l)
	}
	return nil
}

func (db *DB) flushValueLog() error {
	if !db.valueLogEnabled() {
		return nil
	}
	for i := range db.lanes {
		if err := db.flushValueLogLane(&db.lanes[i]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) syncValueLog() error {
	if !db.valueLogEnabled() {
		return nil
	}
	for i := range db.lanes {
		if err := db.syncValueLogLane(&db.lanes[i]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) flushValueLogLane(l *lane) error {
	if l == nil {
		return errWALUnavailable
	}
	if db.splitValueLogEnabled() {
		waitStart := time.Now()
		l.vlogMu.Lock()
		waited := time.Since(waitStart)
		w := l.vlog
		if w == nil {
			l.vlogMu.Unlock()
			return errWALUnavailable
		}
		start := time.Now()
		err := w.Flush()
		db.debugVlogTiming("vlog_flush", int(l.id), "vlogMu", waited, time.Since(start))
		l.vlogMu.Unlock()
		return err
	}
	waitStart := time.Now()
	l.walMu.Lock()
	waited := time.Since(waitStart)
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	start := time.Now()
	err := w.Flush()
	db.debugVlogTiming("wal_flush", int(l.id), "walMu", waited, time.Since(start))
	l.walMu.Unlock()
	return err
}

func (db *DB) syncValueLogLane(l *lane) error {
	if l == nil {
		return errWALUnavailable
	}
	if db.splitValueLogEnabled() {
		waitStart := time.Now()
		l.vlogMu.Lock()
		waited := time.Since(waitStart)
		w := l.vlog
		if w == nil {
			l.vlogMu.Unlock()
			return errWALUnavailable
		}
		start := time.Now()
		err := w.Sync()
		db.debugVlogTiming("vlog_sync", int(l.id), "vlogMu", waited, time.Since(start))
		l.vlogMu.Unlock()
		return err
	}
	waitStart := time.Now()
	l.walMu.Lock()
	waited := time.Since(waitStart)
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	start := time.Now()
	err := w.Sync()
	db.debugVlogTiming("wal_sync", int(l.id), "walMu", waited, time.Since(start))
	l.walMu.Unlock()
	return err
}

func (db *DB) logSegmentPrefix(laneID int) string {
	return fmt.Sprintf("commit-l%d-", laneID)
}

func (db *DB) markValueLogRetain(path string) {
	if path == "" {
		return
	}
	db.valueLogMu.Lock()
	if db.valueLogRetain == nil {
		db.valueLogRetain = make(map[string]struct{})
	}
	db.valueLogRetain[path] = struct{}{}
	db.valueLogMu.Unlock()
}

func (db *DB) forgetValueLogRetain(path string) {
	if path == "" {
		return
	}
	db.valueLogMu.Lock()
	if db.valueLogRetain != nil {
		delete(db.valueLogRetain, path)
	}
	db.valueLogMu.Unlock()
}

func (db *DB) dropValueLogSegment(path string) {
	if db.valueLogReader == nil || path == "" {
		return
	}
	laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
	if !ok || !valueLog {
		return
	}
	if laneID < 0 {
		return
	}
	id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
	if err != nil {
		return
	}
	_ = db.valueLogReader.RemoveSegment(id)
}

func (db *DB) valueLogRetained(path string) bool {
	if path == "" {
		return false
	}
	db.valueLogMu.Lock()
	defer db.valueLogMu.Unlock()
	_, retained := db.valueLogRetain[path]
	return retained
}

func (db *DB) valueLogRetainedStats() (segments int, bytes int64) {
	db.valueLogMu.Lock()
	if len(db.valueLogRetain) == 0 {
		db.valueLogMu.Unlock()
		return 0, 0
	}
	paths := make([]string, 0, len(db.valueLogRetain))
	for path := range db.valueLogRetain {
		paths = append(paths, path)
	}
	db.valueLogMu.Unlock()

	pathSizes := make(map[string]int64)
	currentSizes := make(map[string]int64)
	if db.splitValueLogEnabled() {
		for i := range db.lanes {
			l := &db.lanes[i]
			l.vlogMu.Lock()
			for path, size := range l.vlogClosedSizes {
				pathSizes[path] = size
			}
			if l.vlogPath != "" {
				currentSizes[l.vlogPath] = l.vlogLiveBytes.Load()
			}
			l.vlogMu.Unlock()
		}
	} else {
		for i := range db.lanes {
			l := &db.lanes[i]
			l.walMu.Lock()
			for path, size := range l.walClosedSizes {
				pathSizes[path] = size
			}
			if l.walPath != "" {
				currentSizes[l.walPath] = l.walLiveBytes.Load()
			}
			l.walMu.Unlock()
		}
	}

	for _, path := range paths {
		segments++
		if size, ok := currentSizes[path]; ok {
			bytes += size
			continue
		}
		if size, ok := pathSizes[path]; ok {
			bytes += size
		}
	}
	return segments, bytes
}

func (db *DB) valueLogRetainedPaths() []string {
	db.valueLogMu.Lock()
	if len(db.valueLogRetain) == 0 {
		db.valueLogMu.Unlock()
		return nil
	}
	paths := make([]string, 0, len(db.valueLogRetain))
	for path := range db.valueLogRetain {
		paths = append(paths, path)
	}
	db.valueLogMu.Unlock()
	return paths
}

func (db *DB) collectValueLogLiveIDs() (map[uint32]struct{}, error) {
	it, err := db.backend.Iterator(nil, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	live := make(map[uint32]struct{})
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			live[ptr.FileID] = struct{}{}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return live, nil
}

func (db *DB) checkValueLogRetention() {
	limit := db.maxValueLogRetainedBytes
	if limit <= 0 || !db.valueLogEnabled() {
		return
	}
	_, bytes := db.valueLogRetainedStats()
	if bytes <= limit {
		db.valueLogWarned.Store(false)
		return
	}
	if db.valueLogWarned.CompareAndSwap(false, true) {
		db.reportError(fmt.Errorf("cachingdb: retained value-log bytes %d exceed limit %d", bytes, limit))
	}
}

func (db *DB) allowValueLogPointers() bool {
	if !db.valueLogEnabled() {
		return false
	}
	limit := db.maxValueLogRetainedBytesHard
	if limit <= 0 {
		return true
	}
	bytes := db.valueLogRetainedClosedBytes.Load()
	if db.splitValueLogEnabled() {
		for i := range db.lanes {
			l := &db.lanes[i]
			if l.vlogPath != "" && l.vlogPath == l.vlogRetainedPath {
				bytes += l.vlogLiveBytes.Load()
			}
		}
	}
	if bytes >= limit {
		if db.valueLogHardCapWarned.CompareAndSwap(false, true) {
			db.reportError(fmt.Errorf("cachingdb: retained value-log bytes %d exceed hard cap %d; disabling new value-log pointers", bytes, limit))
		}
		return false
	}
	db.valueLogHardCapWarned.Store(false)
	return true
}

type valueLogZombieMarker interface {
	MarkValueLogZombie(id uint32) error
}

type valueLogSetRefresher interface {
	RefreshSlabSet() error
}

func (db *DB) pruneRetainedValueLogs() {
	if !db.valueLogEnabled() {
		return
	}
	paths := db.valueLogRetainedPaths()
	if len(paths) == 0 {
		return
	}

	live, err := db.collectValueLogLiveIDs()
	if err != nil {
		db.reportError(fmt.Errorf("cachingdb: failed to scan value-log pointers: %w", err))
		return
	}

	inUse := make(map[string]struct{})
	if db.splitValueLogEnabled() {
		for _, path := range db.currentValueLogPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueValueLogPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
	} else {
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
	}

	removed := false
	marked := false
	for _, path := range paths {
		if _, ok := inUse[path]; ok {
			continue
		}
		laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
		if !ok || !valueLog {
			continue
		}
		if laneID < 0 {
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
		if err != nil {
			continue
		}
		if _, ok := live[id]; ok {
			continue
		}

		if marker, ok := db.backend.(valueLogZombieMarker); ok {
			if err := marker.MarkValueLogZombie(id); err != nil {
				db.reportError(fmt.Errorf("cachingdb: failed to mark value-log %d zombie: %w", id, err))
				continue
			}
			marked = true
		} else {
			db.dropValueLogSegment(path)
			_ = db.removeFileRetry(path)
			db.mu.Lock()
			db.untrackValueLogSegmentLocked(path)
			db.mu.Unlock()
			removed = true
		}
		db.forgetValueLogRetain(path)
	}

	if marked {
		if refresher, ok := db.backend.(valueLogSetRefresher); ok {
			if err := refresher.RefreshSlabSet(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: failed to refresh value-log set: %w", err))
			}
		}
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
}

func hashKey(key []byte) uint64 {
	return xxhash.Sum64(key)
}

func (db *DB) shardIndex(key []byte) int {
	if len(db.mutableShards) <= 1 {
		return 0
	}
	return int(hashKey(key) & db.mutableShardMask)
}

func (db *DB) shardForKey(key []byte) *memShard {
	return &db.mutableShards[db.shardIndex(key)]
}

func (db *DB) shardExceedsLimit(shard *memShard, addBytes int64) bool {
	if maxMemtableBytesPerShard <= 0 {
		return false
	}
	return shard.bytes+addBytes > maxMemtableBytesPerShard
}

func (db *DB) newLargePtrMap() *largePtrMap {
	if !db.valueLogEnabled() {
		return nil
	}
	return &largePtrMap{}
}

// BackendDB defines the subset of treedb.DB needed by CachingDB.
type BackendDB interface {
	Get(key []byte) ([]byte, error)
	GetUnsafe(key []byte) ([]byte, error)
	GetAppend(key, dst []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	Iterator(start, end []byte) (iterator.UnsafeIterator, error)
	ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error)
	NewBatch() batch.Interface
	Close() error
	Print() error
	Stats() map[string]string
}

type Options struct {
	FlushThreshold int64

	// MemtableMode selects the in-memory write buffer implementation.
	// Supported: "skiplist", "hash_sorted", "btree", "adaptive".
	// Use "adaptive" or "adaptive:<mode>" to switch per-rotation based on workload.
	MemtableMode string

	// MemtableShards controls the number of mutable memtable shards. Values <= 0
	// use a default derived from GOMAXPROCS. The count is rounded down to a power
	// of two.
	MemtableShards int

	// Legacy backpressure knob: queue length limit.
	// 0 uses the default (4). <0 disables writer backpressure entirely.
	MaxQueuedMemtables int

	// Adaptive backpressure knobs (seconds/bytes). If any of these are non-zero,
	// the caching layer uses backlog-bytes thresholds instead of queue length.
	SlowdownBacklogSeconds float64
	StopBacklogSeconds     float64
	MaxBacklogBytes        int64

	// Writer flush assist limits when backpressure triggers.
	WriterFlushMaxMemtables int
	WriterFlushMaxDuration  time.Duration

	// FlushBuildConcurrency controls how many goroutines may be used to build a
	// combined flush batch from multiple immutable memtables. Values <= 1 disable
	// parallelism.
	FlushBuildConcurrency int

	// DisableWAL disables the Write-Ahead Log entirely (legacy alias for disabling
	// both the journal/redo log and value-log pointers).
	DisableWAL bool
	// DisableJournal disables the redo/journal records while still allowing
	// value-log pointers/value storage. A crash may lose writes since the last
	// checkpoint because there is no redo log to replay.
	DisableJournal bool
	// DisableValueLog forces the cached WAL to remain in legacy mode (no value-log pointers).
	DisableValueLog bool
	// SplitValueLog stores WAL records in wal/ while large values go to vlog/
	// segments, and WAL entries reference them via pointers.
	SplitValueLog bool
	// JournalLanes controls the number of active commit/value log lanes (0=default).
	// Max supported lanes is 255; value-log segment sequence per lane is capped at 8,388,607.
	JournalLanes int
	// WALMaxSegmentBytes caps the size of a single WAL segment payload.
	// 0 uses the default limit.
	WALMaxSegmentBytes int64
	// RelaxedSync disables fsync on Sync operations.
	RelaxedSync bool
	// MemtableValueLogPointers avoids storing large values in the memtable and
	// serves them by pointer from the value log (WAL/vlog). Requires WAL/value-log.
	MemtableValueLogPointers bool
	// ValueLogPointerThreshold controls when WAL/vlog pointers are used.
	// Values <= 0 use the default inline threshold (256 bytes).
	ValueLogPointerThreshold int
	// DisableReadChecksum skips CRC verification on value-log reads.
	DisableReadChecksum bool
	// AllowUnsafe acknowledges unsafe durability options.
	// When false, Open will reject DisableWAL or RelaxedSync.
	AllowUnsafe bool
	// MaxValueLogRetainedBytes emits a warning when retained value-log bytes exceed
	// this threshold (0 disables warnings).
	MaxValueLogRetainedBytes int64
	// MaxValueLogRetainedBytesHard disables value-log pointers for new large
	// values once retained bytes exceed this threshold (0 disables the cap).
	MaxValueLogRetainedBytesHard int64

	// ValueLogDictTrain configures background dictionary training for value-log frame compression.
	// TrainBytes <= 0 disables training.
	ValueLogDictTrain compression.TrainConfig
	// ValueLogDictAdaptiveRatio enables adaptive pause of dict compression when payload ratios degrade.
	// 0 disables.
	ValueLogDictAdaptiveRatio float64
	// ValueLogDictMetricsWindowBytes controls the metrics window size (0=default).
	ValueLogDictMetricsWindowBytes int
	// ValueLogDictMetricsMinRecords is a minimum record count before pausing (0=default).
	ValueLogDictMetricsMinRecords int
	// ValueLogDictMetricsPauseBytes controls pause duration in bytes (0=default).
	ValueLogDictMetricsPauseBytes int
	// ValueLogDictMinPayloadSavingsRatio rejects newly trained dictionaries whose payload ratio
	// does not improve by at least this fraction (0 uses default).
	ValueLogDictMinPayloadSavingsRatio float64

	// NotifyError is an optional hook for background maintenance failures.
	NotifyError func(error)
}

// DictStore provides access to the current dictionary ID for write freezing.
type DictStore interface {
	GetCurrent(ctx context.Context) (uint64, error)
	GetDictBytes(ctx context.Context, dictID uint64) ([]byte, error)
}

type DB struct {
	mu      sync.RWMutex
	flushMu sync.Mutex
	writeMu sync.RWMutex
	bpMu    sync.Mutex
	bpCond  *sync.Cond

	checkpointMu   sync.Mutex
	checkpointCond *sync.Cond
	checkpointing  atomic.Bool

	// Level 0 (Memory)
	mutableShards    []memShard
	mutableShardMask uint64
	mutableBytes     atomic.Int64
	mutableThreshold atomic.Int64
	rotatePending    atomic.Bool
	queue            []memtable.Table

	// memtables is an RCU-style snapshot of (mutable, queue, queueRanges).
	// Readers load it atomically to avoid holding db.mu around memtable access.
	memtables          atomic.Pointer[memtableView]
	hashSortedIndexer  *memtable.HashSortedIndexer
	queueRanges        []keyRange
	queueWALPaths      [][]string
	queueValueLogPaths [][]string
	queueLargePtrs     []*largePtrMap
	backendRange       keyRange
	backendRangeKnown  bool
	backendRangeInit   sync.Once
	backendRangeErr    error

	// Durability
	lanes         []lane
	laneMu        sync.Mutex
	laneCond      *sync.Cond
	nextLane      int
	nextCommitSeq atomic.Uint64
	walAckMu      sync.Mutex
	walErr        error
	nextRID       atomic.Uint64

	disableValueLog              bool
	splitValueLog                bool
	inlineThreshold              int
	valueLogThreshold            int
	memtableValueLogPointers     bool
	valueLogReader               *valuelog.Manager
	valueLogMu                   sync.Mutex
	valueLogRetain               map[string]struct{}
	valueLogWarned               atomic.Bool
	valueLogHardCapWarned        atomic.Bool
	valueLogRetainedClosedBytes  atomic.Int64
	maxValueLogRetainedBytes     int64
	maxValueLogRetainedBytesHard int64

	// Level 1 (Disk)
	backend   BackendDB
	dictStore DictStore

	// Value-log dictionary compression (cached mode).
	valueLogDictTrain             compression.TrainConfig
	valueLogDictAdaptiveRatio     float64
	valueLogDictMinPayloadSavings float64
	valueLogDictMetricsWindow     int
	valueLogDictMetricsMinRecords int
	valueLogDictMetricsPauseBytes int

	valueLogDictTrainerMu sync.Mutex
	valueLogDictTrainer   *compression.Trainer
	valueLogDictMetrics   compression.Metrics

	valueLogDictPauseRemaining      atomic.Uint64
	valueLogDictLastAppliedDictHash atomic.Uint64
	valueLogDictLastAppliedDictID   atomic.Uint64
	valueLogDictCurrentK            atomic.Uint32
	valueLogDictKMu                 sync.RWMutex
	valueLogDictKCache              map[uint64]int
	valueLogDictBytesMu             sync.Mutex
	valueLogDictBytesID             uint64
	valueLogDictBytes               []byte

	// Cached dictdb current pointer to avoid per-write lookups on the hot path.
	// A stale dictID is safe (it always points to a durable dict); at worst we
	// lag adoption of a newly trained dictionary.
	dictCurrentCached atomic.Uint64
	dictCurrentOps    atomic.Uint64

	// Config
	dir                       string
	flushThreshold            int64
	memtableCap               int
	memtableMode              memtable.Mode
	memtableAdaptive          bool
	memtableWarmupActive      bool
	memtableWarmupThreshold   int64
	memtableAdaptiveCandidate memtable.Mode
	memtableAdaptiveStreak    uint8
	statsMu                   sync.Mutex
	memtableStats             memtableStats
	maxQueuedMemtables        int
	slowdownBacklogSeconds    float64
	stopBacklogSeconds        float64
	maxBacklogBytes           int64
	writerFlushMaxMemtables   int
	writerFlushMaxDuration    time.Duration
	flushBuildConcurrency     int
	walMaxSegmentBytes        int64

	disableJournal     bool
	relaxedSync        bool
	notifyError        func(error)
	debugFlushPointers bool
	debugPtrEligible   atomic.Int64
	debugPtrUsed       atomic.Int64
	debugPtrNoPtr      atomic.Int64
	debugPtrDenied     atomic.Int64
	debugPtrDisabled   atomic.Int64
	bgErrMu            sync.Mutex
	bgErr              error

	// Backpressure state
	queueBacklogBytes atomic.Int64
	flushBpsEWMA      float64

	// Lifecycle
	closeCh chan struct{}
	flushCh chan struct{}
	wg      sync.WaitGroup

	autoCheckpointOnceCh  chan struct{}
	autoCheckpointWriteCh chan struct{}
	autoCheckpointOn      atomic.Bool
	// autoCheckpointSizeArmed gates the maxWALBytes size-triggered checkpoint.
	// It is disarmed after the first size-triggered checkpoint and re-armed only
	// after reclaimable WAL bytes fall below maxWALBytes/2.
	autoCheckpointSizeArmed atomic.Bool

	autoCheckpointCount                    atomic.Uint64
	autoCheckpointLastReason               atomic.Uint32
	autoCheckpointLastUnixNano             atomic.Int64
	autoCheckpointLastDurNanos             atomic.Int64
	autoCheckpointLastWALBefore            atomic.Int64
	autoCheckpointLastWALAfter             atomic.Int64
	autoCheckpointLastWALReclaimableBefore atomic.Int64
	autoCheckpointLastWALReclaimableAfter  atomic.Int64
	autoCheckpointLastWALTrimmed           atomic.Int64
	autoCheckpointLastWALBytes             atomic.Int64
	autoCheckpointMaxWALBytes              atomic.Int64
}

type keyRange struct {
	valid bool
	min   []byte
	max   []byte
}

type largePtrMap struct {
	mu sync.RWMutex
	m  map[string]page.ValuePtr
}

func (l *largePtrMap) GetString(key string) (page.ValuePtr, bool) {
	if l == nil {
		return page.ValuePtr{}, false
	}
	l.mu.RLock()
	ptr, ok := l.m[key]
	l.mu.RUnlock()
	return ptr, ok
}

func (l *largePtrMap) Get(key []byte) (page.ValuePtr, bool) {
	if len(key) == 0 {
		return page.ValuePtr{}, false
	}
	return l.GetString(bytesToStringNoCopy(key))
}

func (l *largePtrMap) Len() int {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	n := len(l.m)
	l.mu.RUnlock()
	return n
}

func (l *largePtrMap) SetString(key string, ptr page.ValuePtr) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.m == nil {
		l.m = make(map[string]page.ValuePtr)
	}
	l.m[key] = ptr
	l.mu.Unlock()
}

func (l *largePtrMap) DeleteString(key string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.m != nil {
		delete(l.m, key)
	}
	l.mu.Unlock()
}

type memShard struct {
	mu        sync.Mutex
	mem       memtable.Table
	rng       keyRange
	bytes     int64
	largePtrs *largePtrMap
}

// memtableView is an immutable snapshot of the in-memory layers.
// It is published via atomic.Pointer and treated as read-only by readers.
type memtableView struct {
	mutables    []memtable.Table
	mutablePtrs []*largePtrMap
	queue       []memtable.Table
	queueRanges []keyRange
	queuePtrs   []*largePtrMap
}

// publishMemtablesLocked publishes a new memtable snapshot.
// Caller must hold db.mu with a writer lock.
func (db *DB) publishMemtablesLocked() {
	view := &memtableView{}
	if len(db.mutableShards) > 0 {
		mutables := make([]memtable.Table, len(db.mutableShards))
		mutablePtrs := make([]*largePtrMap, len(db.mutableShards))
		for i := range db.mutableShards {
			mutables[i] = db.mutableShards[i].mem
			mutablePtrs[i] = db.mutableShards[i].largePtrs
		}
		view.mutables = mutables
		view.mutablePtrs = mutablePtrs
	}
	if len(db.queue) > 0 {
		q := make([]memtable.Table, len(db.queue))
		copy(q, db.queue)
		view.queue = q
	}
	if len(db.queueRanges) > 0 {
		qr := make([]keyRange, len(db.queueRanges))
		copy(qr, db.queueRanges)
		view.queueRanges = qr
	}
	if len(db.queueLargePtrs) > 0 {
		qp := make([]*largePtrMap, len(db.queueLargePtrs))
		copy(qp, db.queueLargePtrs)
		view.queuePtrs = qp
	}
	db.memtables.Store(view)
}

type memtableStats struct {
	writes     uint64
	seqWrites  uint64
	iterators  uint64
	rangeIters uint64
	lastKey    []byte
	hasLastKey bool
}

func (r *keyRange) add(key []byte) {
	if key == nil {
		return
	}
	if !r.valid {
		r.valid = true
		r.min = append([]byte(nil), key...)
		r.max = append([]byte(nil), key...)
		return
	}
	if bytes.Compare(key, r.min) < 0 {
		r.min = append(r.min[:0], key...)
	}
	if bytes.Compare(key, r.max) > 0 {
		r.max = append(r.max[:0], key...)
	}
}

func rangesOverlap(a, b keyRange) bool {
	if !a.valid || !b.valid {
		return false
	}
	// [a.min, a.max] overlaps [b.min, b.max] iff neither is strictly before the other.
	if bytes.Compare(a.max, b.min) < 0 {
		return false
	}
	if bytes.Compare(a.min, b.max) > 0 {
		return false
	}
	return true
}

// overlapsQuery checks if the query range [start, end) overlaps with the keyRange [r.min, r.max].
// nil start means -inf, nil end means +inf.
func overlapsQuery(start, end []byte, r keyRange) bool {
	if !r.valid {
		return false
	}
	// Range is [r.min, r.max]
	// Query is [start, end)

	// Check if Range is strictly before Query: r.max < start
	if start != nil && bytes.Compare(r.max, start) < 0 {
		return false
	}

	// Check if Range is strictly after Query: r.min >= end
	// Note: end is exclusive, so if r.min == end, it's outside.
	if end != nil && bytes.Compare(r.min, end) >= 0 {
		return false
	}

	return true
}

// queryCoversRange reports whether the query domain [start,end) fully covers the
// inclusive key range [r.min,r.max]. A nil bound is treated as -/+ infinity.
//
// Note: since end is exclusive, it must be strictly greater than r.max to cover
// the max key.
func queryCoversRange(start, end []byte, r keyRange) bool {
	if !r.valid {
		return true
	}
	if start != nil && bytes.Compare(start, r.min) > 0 {
		return false
	}
	if end != nil && bytes.Compare(end, r.max) <= 0 {
		return false
	}
	return true
}

func cloneRange(r keyRange) keyRange {
	if !r.valid {
		return r
	}
	r.min = append([]byte(nil), r.min...)
	r.max = append([]byte(nil), r.max...)
	return r
}

func (db *DB) snapshotMutableRange() keyRange {
	var out keyRange
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		r := cloneRange(shard.rng)
		shard.mu.Unlock()
		if !r.valid {
			continue
		}
		if !out.valid {
			out = r
			continue
		}
		if bytes.Compare(r.min, out.min) < 0 {
			out.min = append(out.min[:0], r.min...)
		}
		if bytes.Compare(r.max, out.max) > 0 {
			out.max = append(out.max[:0], r.max...)
		}
	}
	return out
}

func (db *DB) resetMutableShardsLocked(nextMode memtable.Mode, reuse bool) error {
	db.mutableBytes.Store(0)
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		reused := false
		if reuse {
			if r, ok := any(shard.mem).(interface{ Reset() }); ok {
				r.Reset()
				reused = true
			}
		}
		if !reused {
			mt, err := memtable.NewWithCapacityModeAndIndexer(0, nextMode, db.hashSortedIndexer)
			if err != nil {
				shard.mu.Unlock()
				return err
			}
			shard.mem = mt
		}
		shard.rng = keyRange{}
		shard.bytes = 0
		shard.largePtrs = db.newLargePtrMap()
		shard.mu.Unlock()
	}
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	return nil
}

func (db *DB) noteWriteKey(key []byte) {
	if !db.memtableAdaptive {
		return
	}
	db.statsMu.Lock()
	defer db.statsMu.Unlock()
	stats := &db.memtableStats
	stats.writes++
	if len(key) == 0 {
		stats.hasLastKey = false
		return
	}
	if stats.hasLastKey && bytes.Compare(stats.lastKey, key) < 0 {
		stats.seqWrites++
	}
	stats.lastKey = append(stats.lastKey[:0], key...)
	stats.hasLastKey = true
}

func (db *DB) noteIterator(start, end []byte) {
	if !db.memtableAdaptive {
		return
	}
	db.statsMu.Lock()
	defer db.statsMu.Unlock()
	stats := &db.memtableStats
	stats.iterators++
	if start != nil || end != nil {
		stats.rangeIters++
	}
}

func (db *DB) updateMutableThresholdLocked() {
	threshold := db.flushThreshold
	if db.memtableWarmupActive && db.memtableWarmupThreshold > 0 && db.memtableWarmupThreshold < db.flushThreshold {
		threshold = db.memtableWarmupThreshold
	}
	db.mutableThreshold.Store(threshold)
}

func (db *DB) mutableFlushThreshold() int64 {
	return db.mutableThreshold.Load()
}

func (db *DB) resetMemtableStatsLocked() {
	db.statsMu.Lock()
	db.memtableStats = memtableStats{}
	db.statsMu.Unlock()
}

func (db *DB) chooseAdaptiveMemtableModeLocked() memtable.Mode {
	db.statsMu.Lock()
	statsCopy := db.memtableStats
	db.statsMu.Unlock()
	stats := &statsCopy
	if stats.writes < adaptiveMinWrites {
		return db.memtableMode
	}
	denom := stats.writes
	if denom > 1 {
		denom--
	}
	seqRatio := float64(stats.seqWrites) / float64(denom)
	sequential := seqRatio >= adaptiveSequentialWritePct

	if stats.rangeIters > 0 {
		return memtable.ModeSkiplist
	}
	if stats.iterators > 0 {
		return memtable.ModeSkiplist
	}
	if sequential {
		return memtable.ModeSkiplist
	}
	return memtable.ModeHashSorted
}

func (db *DB) maybeSwitchAdaptiveMemtableModeLocked() memtable.Mode {
	desired := db.chooseAdaptiveMemtableModeLocked()
	if desired == db.memtableMode {
		db.memtableAdaptiveCandidate = desired
		db.memtableAdaptiveStreak = 0
		return desired
	}
	if db.memtableAdaptiveCandidate != desired {
		db.memtableAdaptiveCandidate = desired
		db.memtableAdaptiveStreak = 1
		return db.memtableMode
	}
	if db.memtableAdaptiveStreak < 255 {
		db.memtableAdaptiveStreak++
	}
	if db.memtableAdaptiveStreak >= adaptiveModeSwitchStreak {
		db.memtableMode = desired
		db.memtableAdaptiveStreak = 0
		db.memtableAdaptiveCandidate = desired
	}
	return db.memtableMode
}

func Open(dir string, backend BackendDB, opts Options) (*DB, error) {
	if !opts.AllowUnsafe && (opts.DisableWAL || opts.DisableJournal || opts.RelaxedSync || opts.DisableReadChecksum || opts.MemtableValueLogPointers) {
		return nil, ErrUnsafeOptions
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = 64 * 1024 * 1024 // 64MB default
	}
	memCap := memtableCapacity(opts.FlushThreshold)
	modeStr := opts.MemtableMode
	if modeStr == "" {
		modeStr = "adaptive"
	}
	adaptive := false
	if modeStr == "adaptive" || modeStr == "auto" {
		adaptive = true
		modeStr = ""
	} else if strings.HasPrefix(modeStr, "adaptive:") {
		adaptive = true
		modeStr = strings.TrimPrefix(modeStr, "adaptive:")
	}
	mode, err := memtable.ModeFromString(modeStr)
	if err != nil {
		return nil, err
	}
	shardCount := opts.MemtableShards
	if shardCount <= 0 {
		shardCount = defaultMemtableShards()
	}
	shardCount = normalizeShardCount(shardCount)
	if shardCount < 1 {
		shardCount = 1
	}
	if opts.MaxQueuedMemtables == 0 {
		// Keep the default queued backlog roughly stable in bytes when callers
		// tune FlushThreshold. Historically: 64MB flush threshold with a queue
		// length of 4 => ~256MB backlog.
		opts.MaxQueuedMemtables = defaultMaxQueuedMemtables(opts.FlushThreshold) * shardCount
	}
	if opts.WriterFlushMaxMemtables == 0 {
		opts.WriterFlushMaxMemtables = 1
	}
	if opts.FlushBuildConcurrency <= 0 {
		opts.FlushBuildConcurrency = 1
	}

	// Ensure wal dir exists
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0700); err != nil {
		return nil, err
	}
	warnInsecureDir(walDir, opts.NotifyError)
	segments, _ := listNonEmptyLogSegments(walDir)
	maxLaneID := -1
	maxWALSeq := make(map[int]int)
	maxVlogSeq := make(map[int]int)
	for _, seg := range segments {
		if seg.lane > maxLaneID {
			maxLaneID = seg.lane
		}
		if seg.valueLog {
			if seg.seq > maxVlogSeq[seg.lane] {
				maxVlogSeq[seg.lane] = seg.seq
			}
		} else if seg.seq > maxWALSeq[seg.lane] {
			maxWALSeq[seg.lane] = seg.seq
		}
	}
	laneCount := opts.JournalLanes
	if laneCount <= 0 {
		laneCount = 1
	}
	if maxLaneID+1 > laneCount {
		laneCount = maxLaneID + 1
	}

	inlineThreshold := page.DefaultInlineThreshold
	if provider, ok := backend.(interface{ InlineThreshold() int }); ok {
		if v := provider.InlineThreshold(); v >= 0 {
			inlineThreshold = v
		}
	}
	valueLogThreshold := opts.ValueLogPointerThreshold
	if valueLogThreshold <= 0 {
		valueLogThreshold = page.DefaultInlineThreshold
	}
	disableJournal := opts.DisableJournal || opts.DisableWAL
	disableValueLog := opts.DisableValueLog || opts.DisableWAL
	if opts.SplitValueLog && disableValueLog {
		return nil, ErrSplitValueLog
	}
	if opts.MemtableValueLogPointers && disableValueLog {
		return nil, ErrMemtableValueLogPointers
	}
	var retained map[string]struct{}
	for _, seg := range segments {
		if !seg.valueLog || disableValueLog {
			continue
		}
		if retained == nil {
			retained = make(map[string]struct{})
		}
		retained[seg.path] = struct{}{}
	}

	warmupThreshold := opts.FlushThreshold
	if adaptive && adaptiveWarmupBytes > 0 && int64(adaptiveWarmupBytes) < opts.FlushThreshold {
		warmupThreshold = int64(adaptiveWarmupBytes)
	}
	memCap = shardCapacity(memCap, shardCount)
	warmupCap := shardCapacity(memtableCapacity(warmupThreshold), shardCount)
	indexer := memtable.NewHashSortedIndexer()
	mutableShards := make([]memShard, shardCount)
	for i := range mutableShards {
		mt, err := memtable.NewWithCapacityModeAndIndexer(warmupCap, mode, indexer)
		if err != nil {
			return nil, err
		}
		mutableShards[i] = memShard{mem: mt}
	}
	if !disableValueLog {
		for i := range mutableShards {
			mutableShards[i].largePtrs = &largePtrMap{}
		}
	}

	var valueLogReader *valuelog.Manager
	if opts.MemtableValueLogPointers {
		reader, err := valuelog.NewManager(walDir)
		if err != nil {
			return nil, err
		}
		reader.SetDisableReadChecksum(opts.DisableReadChecksum)
		valueLogReader = reader
	}
	debugFlushPointers := envBool(envDebugFlushPointers)

	valueLogDictTrain := opts.ValueLogDictTrain

	valueLogDictAdaptiveRatio := opts.ValueLogDictAdaptiveRatio
	valueLogDictMetricsWindow := opts.ValueLogDictMetricsWindowBytes
	valueLogDictMetricsMinRecords := opts.ValueLogDictMetricsMinRecords
	valueLogDictMetricsPauseBytes := opts.ValueLogDictMetricsPauseBytes

	minPayloadSavings := opts.ValueLogDictMinPayloadSavingsRatio
	if minPayloadSavings <= 0 {
		minPayloadSavings = 0.005
	}

	splitValueLog := opts.SplitValueLog
	if !disableValueLog {
		splitValueLog = true
	}

	// Value-log dictionary compression is a core performance feature of the
	// value-store path. For unsafe/bench workloads (AllowUnsafe=true), enable
	// background dict training by default when the value log is active unless
	// the caller explicitly configured it.
	if opts.AllowUnsafe && splitValueLog && !disableValueLog && valueLogDictTrain.TrainBytes == 0 {
		valueLogDictTrain.TrainBytes = compression.DefaultTrainBytes
	}

	// If dict training is enabled but no adaptive ratio is specified, default to
	// a conservative pause threshold to avoid wasting CPU on incompressible
	// payload streams.
	if valueLogDictTrain.TrainBytes > 0 && splitValueLog && valueLogDictAdaptiveRatio == 0 {
		valueLogDictAdaptiveRatio = 0.995
	}
	if valueLogDictAdaptiveRatio > 0 {
		if valueLogDictMetricsWindow <= 0 {
			valueLogDictMetricsWindow = 1 << 20
		}
		if valueLogDictMetricsPauseBytes <= 0 {
			valueLogDictMetricsPauseBytes = 16 << 20
		}
	}
	lanes := make([]lane, laneCount)
	for i := range lanes {
		lanes[i].id = i
		lanes[i].walSeq = maxWALSeq[i]
		lanes[i].vlogSeq = maxVlogSeq[i]
	}
	db := &DB{
		dir:                           walDir,
		backend:                       backend,
		flushThreshold:                opts.FlushThreshold,
		memtableCap:                   memCap,
		memtableMode:                  mode,
		memtableAdaptive:              adaptive,
		memtableWarmupActive:          adaptive && warmupThreshold < opts.FlushThreshold,
		memtableWarmupThreshold:       warmupThreshold,
		memtableAdaptiveCandidate:     mode,
		maxQueuedMemtables:            opts.MaxQueuedMemtables,
		slowdownBacklogSeconds:        opts.SlowdownBacklogSeconds,
		stopBacklogSeconds:            opts.StopBacklogSeconds,
		maxBacklogBytes:               opts.MaxBacklogBytes,
		writerFlushMaxMemtables:       opts.WriterFlushMaxMemtables,
		writerFlushMaxDuration:        opts.WriterFlushMaxDuration,
		flushBuildConcurrency:         opts.FlushBuildConcurrency,
		walMaxSegmentBytes:            opts.WALMaxSegmentBytes,
		disableJournal:                disableJournal,
		disableValueLog:               disableValueLog,
		splitValueLog:                 splitValueLog,
		relaxedSync:                   opts.RelaxedSync,
		notifyError:                   opts.NotifyError,
		inlineThreshold:               inlineThreshold,
		valueLogThreshold:             valueLogThreshold,
		memtableValueLogPointers:      opts.MemtableValueLogPointers,
		valueLogReader:                valueLogReader,
		valueLogRetain:                retained,
		debugFlushPointers:            debugFlushPointers,
		maxValueLogRetainedBytes:      opts.MaxValueLogRetainedBytes,
		maxValueLogRetainedBytesHard:  opts.MaxValueLogRetainedBytesHard,
		valueLogDictTrain:             valueLogDictTrain,
		valueLogDictAdaptiveRatio:     valueLogDictAdaptiveRatio,
		valueLogDictMinPayloadSavings: minPayloadSavings,
		valueLogDictMetricsWindow:     valueLogDictMetricsWindow,
		valueLogDictMetricsMinRecords: valueLogDictMetricsMinRecords,
		valueLogDictMetricsPauseBytes: valueLogDictMetricsPauseBytes,
		mutableShards:                 mutableShards,
		mutableShardMask:              uint64(shardCount - 1),
		hashSortedIndexer:             indexer,
		closeCh:                       make(chan struct{}),
		flushCh:                       make(chan struct{}, 1),
		autoCheckpointOnceCh:          make(chan struct{}, 1),
		autoCheckpointWriteCh:         make(chan struct{}, 1),
		lanes:                         lanes,
	}
	db.bpCond = sync.NewCond(&db.bpMu)
	db.laneCond = sync.NewCond(&db.laneMu)
	db.checkpointCond = sync.NewCond(&db.checkpointMu)

	// Open initial value-log segments (if enabled) and journal/commit log
	// segments (if enabled). Journal and value log are decoupled: the value log
	// may be active even when the journal is disabled.
	if db.valueLogEnabled() && db.disableJournal {
		for i := range db.lanes {
			if err := db.rotateValueLogLocked(&db.lanes[i]); err != nil {
				if db.valueLogReader != nil {
					_ = db.valueLogReader.Close()
					db.valueLogReader = nil
				}
				for j := 0; j <= i && j < len(db.lanes); j++ {
					db.cleanupLaneWALWriters(&db.lanes[j])
				}
				return nil, err
			}
		}
	}
	if !db.disableJournal {
		for i := range db.lanes {
			if err := db.rotateWALLocked(&db.lanes[i]); err != nil {
				if db.valueLogReader != nil {
					_ = db.valueLogReader.Close()
					db.valueLogReader = nil
				}
				for j := 0; j <= i && j < len(db.lanes); j++ {
					db.cleanupLaneWALWriters(&db.lanes[j])
				}
				return nil, err
			}
		}
	}
	if len(segments) > 0 {
		for _, seg := range segments {
			if seg.lane < 0 || seg.lane >= len(db.lanes) {
				continue
			}
			l := &db.lanes[seg.lane]
			if db.splitValueLog && !disableValueLog {
				if seg.valueLog {
					if seg.path == l.vlogPath {
						continue
					}
					if l.vlogClosedSizes == nil {
						l.vlogClosedSizes = make(map[string]int64)
					}
					l.vlogClosedSizes[seg.path] = seg.size
					l.vlogClosedBytes.Add(seg.size)
				} else {
					if seg.path == l.walPath {
						continue
					}
					if l.walClosedSizes == nil {
						l.walClosedSizes = make(map[string]int64)
					}
					l.walClosedSizes[seg.path] = seg.size
					l.walClosedBytes.Add(seg.size)
				}
				continue
			}
			if seg.valueLog != db.walUsesValueLog() {
				continue
			}
			if seg.path == l.walPath {
				continue
			}
			if l.walClosedSizes == nil {
				l.walClosedSizes = make(map[string]int64)
			}
			l.walClosedSizes[seg.path] = seg.size
			l.walClosedBytes.Add(seg.size)
		}
	}
	if !db.disableJournal {
		for i := range db.lanes {
			db.startWALWriter(&db.lanes[i])
		}
	}

	// Publish initial memtable snapshot for lock-free reads.
	db.mu.Lock()
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()
	db.mu.Unlock()

	// Start background flusher
	db.wg.Add(1)
	go db.flushLoop()

	return db, nil
}

func (db *DB) reportError(err error) {
	if err == nil {
		return
	}
	if db.notifyError != nil {
		db.notifyError(err)
	}
	db.bgErrMu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.bgErrMu.Unlock()
}

func (db *DB) backgroundError() error {
	db.bgErrMu.Lock()
	defer db.bgErrMu.Unlock()
	return db.bgErr
}

// StartAutoCheckpoint enables a background loop that periodically forces a
// durable boundary and trims cached-mode WAL segments. When idleInterval > 0,
// it also triggers an opportunistic checkpoint after a period of write-idleness.
//
// interval > 0 enables periodic checkpoints. maxWALBytes is a safety cap: if > 0,
// the loop will attempt to checkpoint when the effective WAL bytes exceed this
// cap. maxWALBytes <= 0 disables the size trigger.
//
// This does not make each individual write durable; it bounds the window of
// unsynced writes for long-running workloads.
func (db *DB) StartAutoCheckpoint(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) {
	if db == nil {
		return
	}
	db.autoCheckpointMaxWALBytes.Store(maxWALBytes)
	if maxWALBytes > 0 {
		db.autoCheckpointSizeArmed.Store(true)
	}
	if interval <= 0 && idleInterval <= 0 && maxWALBytes <= 0 {
		return
	}
	if !db.autoCheckpointOn.CompareAndSwap(false, true) {
		return
	}
	db.wg.Add(1)
	go db.autoCheckpointLoop(interval, maxWALBytes, idleInterval)
}

// TriggerAutoCheckpoint schedules a best-effort immediate auto-checkpoint pass.
func (db *DB) TriggerAutoCheckpoint() {
	if db == nil || !db.autoCheckpointOn.Load() {
		return
	}
	select {
	case db.autoCheckpointOnceCh <- struct{}{}:
	default:
	}
}

func (db *DB) noteWrite() {
	if db == nil || !db.autoCheckpointOn.Load() {
		return
	}
	if db.disableJournal {
		return
	}
	const autoCheckpointWriteEveryBytes int64 = 1 << 20
	current := db.effectiveWALBytes()
	if current <= 0 {
		return
	}
	threshold := autoCheckpointWriteEveryBytes
	if max := db.autoCheckpointMaxWALBytes.Load(); max > 0 {
		// Rearm size-triggered checkpoints once WAL bytes drop substantially.
		if current < max/2 {
			db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
		} else if !db.autoCheckpointSizeArmed.Load() && db.walUsesValueLog() {
			reclaimable := db.reclaimableWALBytes()
			if reclaimable < max/2 {
				db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
			}
		}
		scaled := max / 4
		if scaled < 4*1024 {
			scaled = 4 * 1024
		}
		if scaled < threshold {
			threshold = scaled
		}
	}
	for {
		last := db.autoCheckpointLastWALBytes.Load()
		if current < last {
			if db.autoCheckpointLastWALBytes.CompareAndSwap(last, current) {
				return
			}
			continue
		}
		if current-last < threshold {
			return
		}
		if db.autoCheckpointLastWALBytes.CompareAndSwap(last, current) {
			break
		}
	}
	select {
	case db.autoCheckpointWriteCh <- struct{}{}:
	default:
	}
}

type autoCheckpointMode uint8

const (
	autoCheckpointModeInterval autoCheckpointMode = iota
	autoCheckpointModeIdle
	autoCheckpointModeSize
	autoCheckpointModeForce
)

const (
	autoCheckpointMinIdleWALBytesMin int64 = 1 << 20  // 1MiB
	autoCheckpointMinIdleWALBytesMax int64 = 32 << 20 // 32MiB
	autoCheckpointMinIdleInterval          = 10 * time.Second
)

func autoCheckpointReasonString(v uint32) string {
	switch autoCheckpointMode(v) {
	case autoCheckpointModeInterval:
		return "interval"
	case autoCheckpointModeIdle:
		return "idle"
	case autoCheckpointModeSize:
		return "size"
	case autoCheckpointModeForce:
		return "force"
	default:
		return "unknown"
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func (db *DB) effectiveWALBytes() int64 {
	if db == nil {
		return 0
	}
	var total int64
	for i := range db.lanes {
		l := &db.lanes[i]
		total += l.walClosedBytes.Load() + l.walLiveBytes.Load()
	}
	return total
}

func (db *DB) reclaimableWALBytes() int64 {
	if db == nil {
		return 0
	}
	total := db.effectiveWALBytes()
	if total <= 0 {
		return 0
	}
	if !db.walUsesValueLog() {
		return total
	}
	_, retained := db.valueLogRetainedStats()
	if retained >= total {
		return 0
	}
	return total - retained
}

func (db *DB) minIdleCheckpointWALBytes() int64 {
	if db == nil {
		return autoCheckpointMinIdleWALBytesMin
	}
	db.mu.RLock()
	ft := db.flushThreshold
	db.mu.RUnlock()
	min := ft / 16
	if min < autoCheckpointMinIdleWALBytesMin {
		min = autoCheckpointMinIdleWALBytesMin
	}
	if min > autoCheckpointMinIdleWALBytesMax {
		min = autoCheckpointMinIdleWALBytesMax
	}
	return min
}

const (
	commitLogSegmentHeaderBytes = 8
	commitLogBatchHeaderBytes   = 1 + 4
	commitLogRecordHeaderBytes  = 1 + 2 + 4 + 8 + 8
)

func (db *DB) logRecordSize(key, value []byte) int64 {
	return int64(commitLogSegmentHeaderBytes + commitLogBatchHeaderBytes + commitLogRecordHeaderBytes + len(key) + len(value))
}

func (db *DB) logBatchSize(records []logRecord) int64 {
	if len(records) == 0 {
		return 0
	}
	total := commitLogSegmentHeaderBytes + commitLogBatchHeaderBytes
	for _, r := range records {
		total += commitLogRecordHeaderBytes + len(r.Key) + len(r.Value)
	}
	return int64(total)
}

func (db *DB) assignCommitSeq(records []logRecord) {
	if len(records) == 0 {
		return
	}
	seq := db.nextCommitSeq.Add(1)
	for i := range records {
		records[i].Seq = seq
	}
}

type walWriteRequest struct {
	records []logRecord
	sync    bool
	ack     *walAck
}

type walAck struct {
	wg  sync.WaitGroup
	err error
}

var walAckPool = sync.Pool{
	New: func() any { return &walAck{} },
}

const maxEntryPoolCap = 1 << 16

var entrySlicePool sync.Pool

func getEntrySlice(capacity int) []batch.Entry {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxEntryPoolCap {
		return make([]batch.Entry, 0, capacity)
	}
	if v := entrySlicePool.Get(); v != nil {
		s := v.([]batch.Entry)
		if cap(s) >= capacity {
			return s[:0]
		}
	}
	return make([]batch.Entry, 0, capacity)
}

func putEntrySlice(entries []batch.Entry) {
	if cap(entries) > maxEntryPoolCap {
		return
	}
	for i := range entries {
		entries[i] = batch.Entry{}
	}
	entrySlicePool.Put(entries[:0])
}

type walFastItem struct {
	record logRecord
	ack    *walAck
}

const (
	walWriteBuffer   = 4096
	walWriteBatchMax = 512
	walFastBatchMax  = 2048
	walFastQueueMax  = 16384
)

func (db *DB) startWALWriter(l *lane) {
	if l == nil {
		return
	}
	l.walCh = make(chan walWriteRequest, walWriteBuffer)
	l.walFastCond = sync.NewCond(&l.walFastMu)
	db.wg.Add(1)
	go db.walWriteLoop(l)
	db.wg.Add(1)
	go db.walFastLoop(l)
}

func (db *DB) walWriteLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]walWriteRequest, 0, walWriteBatchMax)
	for {
		batch = batch[:0]

		var req walWriteRequest
		select {
		case <-db.closeCh:
			db.drainWALWriter(l, batch)
			return
		case req = <-l.walCh:
		}
		batch = append(batch, req)

	drain:
		for len(batch) < walWriteBatchMax {
			select {
			case req = <-l.walCh:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		db.walAckMu.Lock()
		walErr := db.walErr
		db.walAckMu.Unlock()
		if walErr != nil {
			db.finishWALRequests(batch, walErr)
			continue
		}

		err := db.flushWALRequests(l, batch)
		if err != nil {
			db.walAckMu.Lock()
			if db.walErr == nil {
				db.walErr = err
			}
			walErr = db.walErr
			db.walAckMu.Unlock()
			db.finishWALRequests(batch, walErr)
			continue
		}

		db.finishWALRequests(batch, nil)
	}
}

func (db *DB) walFastLoop(l *lane) {
	defer db.wg.Done()

	batch := make([]walFastItem, 0, walFastBatchMax)
	records := make([]logRecord, 0, walFastBatchMax)

	for {
		l.walFastMu.Lock()
		for !l.walFastClosed && len(l.walFastQueue)-l.walFastHead == 0 {
			l.walFastCond.Wait()
		}

		if l.walFastClosed {
			batch = append(batch[:0], l.walFastQueue[l.walFastHead:]...)
			l.walFastQueue = nil
			l.walFastHead = 0
			l.walFastMu.Unlock()

			for i := range batch {
				ack := batch[i].ack
				ack.err = errWALClosed
				ack.wg.Done()
			}
			return
		}

		available := len(l.walFastQueue) - l.walFastHead
		n := available
		if n > walFastBatchMax {
			n = walFastBatchMax
		}
		batch = append(batch[:0], l.walFastQueue[l.walFastHead:l.walFastHead+n]...)
		l.walFastHead += n

		if l.walFastHead == len(l.walFastQueue) {
			l.walFastQueue = l.walFastQueue[:0]
			l.walFastHead = 0
		} else if l.walFastHead > 1024 && l.walFastHead*2 >= len(l.walFastQueue) {
			copy(l.walFastQueue, l.walFastQueue[l.walFastHead:])
			l.walFastQueue = l.walFastQueue[:len(l.walFastQueue)-l.walFastHead]
			l.walFastHead = 0
		}
		l.walFastCond.Broadcast()
		l.walFastMu.Unlock()

		records = records[:0]
		for i := range batch {
			records = append(records, batch[i].record)
		}
		err := db.appendWALDirect(l, records, false)
		for i := range batch {
			ack := batch[i].ack
			ack.err = err
			ack.wg.Done()
		}
	}
}

func (db *DB) drainWALWriter(l *lane, batch []walWriteRequest) {
	for {
		select {
		case req := <-l.walCh:
			batch = append(batch[:0], req)
		drain:
			for len(batch) < walWriteBatchMax {
				select {
				case req = <-l.walCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			db.walAckMu.Lock()
			walErr := db.walErr
			db.walAckMu.Unlock()
			if walErr != nil {
				db.finishWALRequests(batch, walErr)
				continue
			}

			err := db.flushWALRequests(l, batch)
			if err != nil {
				db.walAckMu.Lock()
				if db.walErr == nil {
					db.walErr = err
				}
				walErr = db.walErr
				db.walAckMu.Unlock()
				db.finishWALRequests(batch, walErr)
				continue
			}
			db.finishWALRequests(batch, nil)
		default:
			return
		}
	}
}

func (db *DB) finishWALRequests(requests []walWriteRequest, err error) {
	for i := range requests {
		ack := requests[i].ack
		if ack == nil {
			continue
		}
		ack.err = err
		ack.wg.Done()
	}
}

func (db *DB) flushWALRequests(l *lane, requests []walWriteRequest) error {
	if len(requests) == 0 {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}

	var (
		totalBytes int64
		needSync   bool
	)

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	for i := range requests {
		req := &requests[i]
		if len(req.records) == 1 {
			rec := req.records[0]
			err := w.Append(rec)
			if err != nil {
				l.walMu.Unlock()
				return err
			}
			totalBytes += db.logRecordSize(rec.Key, rec.Value)
		} else {
			err := w.AppendBatch(req.records)
			if err != nil {
				l.walMu.Unlock()
				return err
			}
			totalBytes += db.logBatchSize(req.records)
		}
		if req.sync {
			needSync = true
		}
	}
	if needSync {
		if err := w.Sync(); err != nil {
			l.walMu.Unlock()
			return err
		}
	}
	l.walMu.Unlock()

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

// journalDurability represents the durability boundary for journal writes.
// When SplitValueLog is enabled, payload durability must complete before the
// commit-intent durability for sync writes.
type journalDurability uint8

const (
	journalDurabilityNone journalDurability = iota
	journalDurabilityFlush
	journalDurabilitySync
)

func (db *DB) appendWAL(l *lane, records []logRecord, durability journalDurability) error {
	if db.disableJournal {
		return nil
	}
	if len(records) == 0 {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return errWALClosed
	default:
	}
	db.walAckMu.Lock()
	if db.walErr != nil {
		err := db.walErr
		db.walAckMu.Unlock()
		return err
	}
	db.walAckMu.Unlock()

	db.assignCommitSeq(records)

	switch durability {
	case journalDurabilitySync:
		return db.appendWALDirect(l, records, true)
	case journalDurabilityFlush:
		return db.appendWALInline(l, records, true)
	default:
		return db.appendWALInline(l, records, false)
	}
}

func (db *DB) appendValueLog(l *lane, dictID uint64, dict []byte, records []valuelog.Record, durability journalDurability) ([]page.ValuePtr, error) {
	if !db.splitValueLogEnabled() {
		return nil, errWALUnavailable
	}
	if len(records) == 0 {
		return nil, nil
	}
	if l == nil {
		return nil, errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return nil, errWALClosed
	default:
	}

	var (
		totalBytes int64
		ptrs       []page.ValuePtr
		err        error
	)
	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return nil, errWALUnavailable
	}
	startSize := w.Size()

	rawPayloadBytes := 0
	for i := range records {
		rawPayloadBytes += len(records[i].Value)
	}
	if rawPayloadBytes > 0 {
		db.valueLogDictConsumePause(uint64(rawPayloadBytes))
	}
	if db.valueLogDictPaused() {
		dictID = 0
		dict = nil
	}

	db.valueLogDictCollectSamples(records)

	k := 1
	if dictID != 0 && len(dict) > 0 {
		k = db.valueLogDictK(dictID)
	} else if len(records) > 1 {
		// Even when dictionary compression is disabled/paused, grouping records into
		// frames reduces per-record overhead (CRC/header writes) on append-heavy
		// workloads.
		//
		// When no dict is available, we write raw frames (uncompressed) and still
		// benefit from fewer syscalls and less framing work.
		if cur := int(db.valueLogDictCurrentK.Load()); cur > 1 {
			k = cur
		} else {
			k = 8
		}
	}
	k = db.clampValueLogDictK(k)
	if dictID != 0 && len(dict) > 0 && db.disableJournal {
		// When the redo/journal log is disabled (ingest-mode), favor maximum frame
		// grouping for throughput. This reduces per-record framing overhead and
		// syscall pressure, and is typically safe for write-heavy workloads where
		// random point reads are not the dominant cost.
		k = valuelog.MaxFrameK
	}

	type frameStatsWriter interface {
		AppendFrameWithStats(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, valuelog.FrameStats, error)
	}
	type frameStatsWriterInto interface {
		AppendFrameWithStatsInto(dictID uint64, dict []byte, records []valuelog.Record, dst []page.ValuePtr) ([]page.ValuePtr, valuelog.FrameStats, error)
	}
	statsWriter, hasStats := w.(frameStatsWriter)
	statsWriterInto, hasInto := w.(frameStatsWriterInto)

	ptrs = make([]page.ValuePtr, 0, len(records))
	storedPayloadBytes := 0
	rawFrameBytes := 0
	frameRecords := 0
	var framePtrScratch [valuelog.MaxFrameK]page.ValuePtr
	for i := 0; i < len(records); i += k {
		end := i + k
		if end > len(records) {
			end = len(records)
		}
		if hasInto {
			scratch := framePtrScratch[:end-i]
			framePtrs, stats, frameErr := statsWriterInto.AppendFrameWithStatsInto(dictID, dict, records[i:end], scratch)
			if frameErr != nil {
				err = frameErr
				break
			}
			ptrs = append(ptrs, framePtrs...)
			rawFrameBytes += stats.RawPayloadBytes
			storedPayloadBytes += stats.StoredPayloadBytes
			frameRecords += stats.Records
			continue
		}
		if hasStats {
			framePtrs, stats, frameErr := statsWriter.AppendFrameWithStats(dictID, dict, records[i:end])
			if frameErr != nil {
				err = frameErr
				break
			}
			ptrs = append(ptrs, framePtrs...)
			rawFrameBytes += stats.RawPayloadBytes
			storedPayloadBytes += stats.StoredPayloadBytes
			frameRecords += stats.Records
			continue
		}

		framePtrs, frameErr := w.AppendFrame(dictID, dict, records[i:end])
		if frameErr != nil {
			err = frameErr
			break
		}
		ptrs = append(ptrs, framePtrs...)
	}
	if err == nil {
		switch durability {
		case journalDurabilityFlush:
			err = w.Flush()
		case journalDurabilitySync:
			err = w.Sync()
		}
	}
	if err == nil {
		totalBytes = w.Size() - startSize
	}
	l.vlogMu.Unlock()
	if err != nil {
		return nil, err
	}
	if dictID != 0 && len(dict) > 0 {
		if rawFrameBytes == 0 {
			rawFrameBytes = rawPayloadBytes
		}
		if storedPayloadBytes == 0 {
			// Best-effort fallback when writer stats are unavailable.
			storedPayloadBytes = int(totalBytes)
		}
		if frameRecords == 0 {
			frameRecords = len(records)
		}
		db.valueLogDictObservePayload(uint64(rawFrameBytes), uint64(storedPayloadBytes), frameRecords)
	}
	if totalBytes > 0 {
		l.vlogLiveBytes.Add(totalBytes)
	}
	return ptrs, nil
}

func (db *DB) appendValueLogOne(l *lane, dictID uint64, dict []byte, rid uint64, value []byte, durability journalDurability) (page.ValuePtr, string, error) {
	if !db.splitValueLogEnabled() {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if l == nil {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return page.ValuePtr{}, "", errWALClosed
	default:
	}

	var (
		totalBytes int64
		ptr        page.ValuePtr
		err        error
	)
	l.vlogMu.Lock()
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", errWALUnavailable
	}
	startSize := w.Size()

	if len(value) > 0 {
		db.valueLogDictConsumePause(uint64(len(value)))
	}
	if db.valueLogDictPaused() {
		dictID = 0
		dict = nil
	}
	db.valueLogDictCollectSample(value)

	ptr, err = w.Append(dictID, dict, rid, value)
	if err == nil {
		switch durability {
		case journalDurabilityFlush:
			err = w.Flush()
		case journalDurabilitySync:
			err = w.Sync()
		}
	}
	if err == nil {
		totalBytes = w.Size() - startSize
	}
	retainPath := ""
	if l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
		l.vlogRetainedPath = l.vlogPath
		retainPath = l.vlogPath
	}
	l.vlogMu.Unlock()
	if err != nil {
		return page.ValuePtr{}, "", err
	}
	if totalBytes > 0 {
		l.vlogLiveBytes.Add(totalBytes)
	}
	return ptr, retainPath, nil
}

func (db *DB) appendWALInline(l *lane, records []logRecord, flush bool) error {
	if l == nil {
		return errWALUnavailable
	}

	var (
		totalBytes int64
		err        error
	)

	l.walMu.Lock()
	w := l.wal
	if w == nil {
		l.walMu.Unlock()
		return errWALUnavailable
	}
	if len(records) == 1 {
		rec := records[0]
		err = w.Append(rec)
		totalBytes = db.logRecordSize(rec.Key, rec.Value)
	} else {
		err = w.AppendBatch(records)
		totalBytes = db.logBatchSize(records)
	}
	if err == nil && flush {
		err = w.Flush()
	}
	l.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return err
	}

	if totalBytes > 0 {
		l.walLiveBytes.Add(totalBytes)
	}
	return nil
}

func (db *DB) appendWALDirect(l *lane, records []logRecord, sync bool) error {
	if l == nil {
		return errWALUnavailable
	}
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.wg.Add(1)

	req := walWriteRequest{records: records, sync: sync, ack: ack}
	select {
	case l.walCh <- req:
		// wait for ack
	case <-db.closeCh:
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALClosed
	}

	ack.wg.Wait()
	err := ack.err
	walAckPool.Put(ack)
	return err
}

func (db *DB) appendWALFast(l *lane, record logRecord) error {
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.wg.Add(1)

	if l == nil {
		ack.err = errWALUnavailable
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALUnavailable
	}

	record.Seq = db.nextCommitSeq.Add(1)

	l.walFastMu.Lock()
	for !l.walFastClosed && len(l.walFastQueue)-l.walFastHead >= walFastQueueMax {
		l.walFastCond.Wait()
	}
	if l.walFastClosed {
		l.walFastMu.Unlock()
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return errWALClosed
	}
	l.walFastQueue = append(l.walFastQueue, walFastItem{record: record, ack: ack})
	l.walFastCond.Signal()
	l.walFastMu.Unlock()

	ack.wg.Wait()
	err := ack.err
	walAckPool.Put(ack)
	return err
}

func (db *DB) autoCheckpointLoop(interval time.Duration, maxWALBytes int64, idleInterval time.Duration) {
	defer db.wg.Done()

	var intervalTicker *time.Ticker
	intervalCh := (<-chan time.Time)(nil)
	if interval > 0 {
		intervalTicker = time.NewTicker(interval)
		intervalCh = intervalTicker.C
		defer intervalTicker.Stop()
	}

	var idleTimer *time.Timer
	idleCh := (<-chan time.Time)(nil)
	if idleInterval > 0 {
		idleTimer = time.NewTimer(idleInterval)
		if !idleTimer.Stop() {
			<-idleTimer.C
		}
		idleCh = idleTimer.C
	}

	for {
		select {
		case <-db.closeCh:
			return
		case <-intervalCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeInterval)
		case <-db.autoCheckpointOnceCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeForce)
		case <-db.autoCheckpointWriteCh:
			if maxWALBytes > 0 {
				db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeSize)
			}
			if idleTimer != nil {
				resetTimer(idleTimer, idleInterval)
			}
		case <-idleCh:
			db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeIdle)
		}
	}
}

func (db *DB) maybeAutoCheckpoint(maxWALBytes int64, mode autoCheckpointMode) {
	effectiveBytes := db.effectiveWALBytes()
	if effectiveBytes <= 0 {
		return
	}
	reclaimableBytes := db.reclaimableWALBytes()

	// Avoid thrashing the checkpoint path when workloads are mostly idle but
	// produce tiny write bursts.
	if mode == autoCheckpointModeIdle {
		if effectiveBytes < db.minIdleCheckpointWALBytes() {
			return
		}
		last := db.autoCheckpointLastUnixNano.Load()
		if last > 0 && time.Since(time.Unix(0, last)) < autoCheckpointMinIdleInterval {
			return
		}
	}

	switch mode {
	case autoCheckpointModeInterval, autoCheckpointModeIdle, autoCheckpointModeForce:
		// proceed
	case autoCheckpointModeSize:
		if maxWALBytes <= 0 || reclaimableBytes < maxWALBytes {
			return
		}
		// Avoid repeatedly checkpointing when WAL bytes cannot be reduced (e.g.
		// value-log segments retained for pointers). Rearm once reclaimable bytes
		// drop below maxWALBytes/2.
		if !db.autoCheckpointSizeArmed.CompareAndSwap(true, false) {
			return
		}
	default:
		// Unknown mode: be conservative and do nothing.
		return
	}

	before := effectiveBytes
	beforeReclaimable := reclaimableBytes
	start := time.Now()
	err := db.Checkpoint()
	dur := time.Since(start)
	after := db.effectiveWALBytes()
	afterReclaimable := db.reclaimableWALBytes()
	trimmed := before - after
	if trimmed < 0 {
		trimmed = 0
	}
	if maxWALBytes > 0 && afterReclaimable < maxWALBytes/2 {
		db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
	}

	// Best-effort: failures here should be surfaced via normal write paths or
	// explicit maintenance calls. Avoid printing from background maintenance.
	if err != nil {
		if mode == autoCheckpointModeSize && maxWALBytes > 0 {
			db.autoCheckpointSizeArmed.CompareAndSwap(false, true)
		}
		return
	}

	db.autoCheckpointCount.Add(1)
	db.autoCheckpointLastReason.Store(uint32(mode))
	db.autoCheckpointLastUnixNano.Store(time.Now().UnixNano())
	db.autoCheckpointLastDurNanos.Store(dur.Nanoseconds())
	db.autoCheckpointLastWALBefore.Store(before)
	db.autoCheckpointLastWALAfter.Store(after)
	db.autoCheckpointLastWALReclaimableBefore.Store(beforeReclaimable)
	db.autoCheckpointLastWALReclaimableAfter.Store(afterReclaimable)
	db.autoCheckpointLastWALTrimmed.Store(trimmed)
}

func (db *DB) ensureBackendRange() error {
	if db == nil {
		return nil
	}
	db.backendRangeInit.Do(func() {
		r, known, err := db.computeBackendRange()
		db.mu.Lock()
		defer db.mu.Unlock()
		if err != nil {
			db.backendRangeErr = err
			db.backendRangeKnown = false
			return
		}
		if r.valid {
			db.backendRange.add(r.min)
			db.backendRange.add(r.max)
		}
		db.backendRangeKnown = known
	})

	db.mu.RLock()
	err := db.backendRangeErr
	db.mu.RUnlock()
	return err
}

func (db *DB) computeBackendRange() (keyRange, bool, error) {
	minIter, err := db.backend.Iterator(nil, nil)
	if err != nil {
		return keyRange{}, false, err
	}
	defer minIter.Close()
	minIter.Seek(nil)

	r := keyRange{}
	if minIter.Valid() && !minIter.IsDeleted() {
		r.add(minIter.UnsafeKey())
	}

	maxIter, err := db.backend.ReverseIterator(nil, nil)
	if err != nil {
		// Backend doesn't support reverse iteration; disable backend-range-dependent optimizations.
		return r, false, nil
	}
	defer maxIter.Close()

	if maxIter.Valid() && !maxIter.IsDeleted() {
		r.add(maxIter.UnsafeKey())
	}

	return r, true, nil
}

const stopResumeFraction = 0.70

func (db *DB) adaptiveBackpressureEnabled() bool {
	return db.slowdownBacklogSeconds > 0 || db.stopBacklogSeconds > 0 || db.maxBacklogBytes > 0
}

func (db *DB) thresholdsLocked() (slowdownBytes, stopBytes, resumeBytes int64) {
	flushBps := db.flushBpsEWMA
	if flushBps <= 0 && db.flushThreshold > 0 {
		// Fallback until we have real measurements: assume ~1 memtable/sec.
		flushBps = float64(db.flushThreshold)
	}
	return computeBackpressureThresholds(backpressureParams{
		flushBps:               flushBps,
		flushThreshold:         db.flushThreshold,
		slowdownBacklogSeconds: db.slowdownBacklogSeconds,
		stopBacklogSeconds:     db.stopBacklogSeconds,
		maxBacklogBytes:        db.maxBacklogBytes,
		stopResumeFraction:     stopResumeFraction,
	})
}

func (db *DB) waitForCheckpoint() {
	if !db.checkpointing.Load() {
		return
	}
	db.checkpointMu.Lock()
	for db.checkpointing.Load() {
		db.checkpointCond.Wait()
	}
	db.checkpointMu.Unlock()
}

// Checkpoint forces a durable backend boundary and trims the WAL so long-running
// cached-mode runs do not accumulate unbounded `wal/` growth.
//
// It blocks writers while it:
//   - rotates the current mutable memtable (if non-empty),
//   - rotates to a fresh WAL segment,
//   - flushes all queued memtables with backend sync,
//   - forces a backend sync boundary (even if the queue is empty),
//   - removes all older WAL segments (keeping only the currently-open one).
func (db *DB) Checkpoint() error {
	db.checkpointMu.Lock()
	for db.checkpointing.Load() {
		db.checkpointCond.Wait()
	}
	db.checkpointing.Store(true)
	db.checkpointMu.Unlock()

	defer func() {
		db.checkpointMu.Lock()
		db.checkpointing.Store(false)
		db.checkpointCond.Broadcast()
		db.checkpointMu.Unlock()
	}()

	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Rotate mutable into the flush queue and ensure future writes land in a fresh
	// WAL segment (so all older segments can be trimmed after the sync boundary).
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	walDir := db.dir
	db.mu.Unlock()
	for i := range db.lanes {
		if err := db.rotateWALLocked(&db.lanes[i]); err != nil {
			return err
		}
	}

	// Flush all queued memtables with backend sync.
	for db.flushCombinedLocked(true) {
	}

	segments, nonEmptyBytes := listNonEmptyLogSegments(walDir)
	if len(segments) > 0 {
		filtered := segments[:0]
		nonEmptyBytes = 0
		for _, seg := range segments {
			if seg.valueLog != db.walUsesValueLog() {
				continue
			}
			filtered = append(filtered, seg)
			if seg.size > 0 {
				nonEmptyBytes += seg.size
			}
		}
		segments = filtered
	}
	if nonEmptyBytes > 0 {
		backendBatch := db.backend.NewBatch()
		err := backendBatch.WriteSync()
		cerr := backendBatch.Close()
		if err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}

	currentWALs := make(map[string]struct{})
	for _, path := range db.currentWALPaths() {
		currentWALs[path] = struct{}{}
	}

	removed := false
	for _, seg := range segments {
		path := seg.path
		if _, ok := currentWALs[path]; ok {
			continue
		}
		if db.valueLogRetained(path) {
			continue
		}
		db.dropValueLogSegment(path)
		if err := db.removeFileRetry(path); err != nil {
			// Best effort cleanup; ignore errors to prevent flakiness on Windows
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
		db.forgetValueLogRetain(path)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()
	db.pruneRetainedValueLogs()

	return nil
}

func (db *DB) waitForStop() {
	if !db.adaptiveBackpressureEnabled() {
		return
	}

	for {
		db.bpMu.Lock()
		_, stopBytes, resumeBytes := db.thresholdsLocked()
		if stopBytes <= 0 {
			db.bpMu.Unlock()
			return
		}
		backlog := db.queueBacklogBytes.Load()
		if backlog < stopBytes {
			db.bpMu.Unlock()
			return
		}
		db.bpMu.Unlock()

		// Ensure a background flush pass is scheduled in case backlog was created
		// without a flush trigger (e.g. iterator-driven rotations).
		db.TriggerFlush()

		// Ensure progress even if the background flusher isn't currently scheduled
		// (e.g. backlog driven by iterator rotations). This still "blocks" the write
		// in the sense that we don't accept new ops until backlog drops, but lets the
		// caller contribute bounded flush work.
		db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)

		db.bpMu.Lock()
		for {
			_, stopBytes, resumeBytes = db.thresholdsLocked()
			if stopBytes <= 0 {
				db.bpMu.Unlock()
				return
			}
			backlog = db.queueBacklogBytes.Load()
			if backlog < stopBytes {
				db.bpMu.Unlock()
				return
			}
			if backlog < resumeBytes {
				break
			}
			db.bpCond.Wait()
		}
		db.bpMu.Unlock()
	}
}

func (db *DB) maybeAssistFlush() {
	if db.writerFlushMaxMemtables <= 0 && db.writerFlushMaxDuration <= 0 {
		return
	}

	// Adaptive policy: thresholds based on queued backlog bytes.
	if db.adaptiveBackpressureEnabled() {
		db.bpMu.Lock()
		slowdownBytes, stopBytes, _ := db.thresholdsLocked()
		db.bpMu.Unlock()

		backlog := db.queueBacklogBytes.Load()
		if stopBytes > 0 && backlog >= stopBytes {
			db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		if slowdownBytes > 0 && backlog > slowdownBytes {
			db.TriggerFlush()
		}
		return
	}

	// Legacy policy: thresholds based on queue length.
	if db.maxQueuedMemtables >= 0 {
		db.mu.RLock()
		needs := len(db.queue) > db.maxQueuedMemtables
		db.mu.RUnlock()
		if needs {
			db.TriggerFlush()
		}
	}
}

func (db *DB) flushSome(sync bool, maxMemtables int, maxDuration time.Duration) {
	if maxMemtables <= 0 && maxDuration <= 0 {
		return
	}
	sync = db.flushSyncRequested(sync)
	start := time.Now()

	if !db.flushMu.TryLock() {
		return
	}
	defer db.flushMu.Unlock()

	flushed := 0
	for {
		if maxMemtables > 0 && flushed >= maxMemtables {
			return
		}
		if maxDuration > 0 && time.Since(start) >= maxDuration {
			return
		}
		if !db.flushOneLocked(sync) {
			return
		}
		flushed++
	}
}

func (db *DB) Close() error {
	var errs []error
	hadMemtables := false
	db.writeMu.Lock()
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		hadMemtables = true
		_ = db.rotateMemtableLocked(true)
	} else if len(db.queue) > 0 {
		hadMemtables = true
	}
	db.mu.Unlock()

	for i := range db.lanes {
		l := &db.lanes[i]
		l.walFastMu.Lock()
		l.walFastClosed = true
		if l.walFastCond != nil {
			l.walFastCond.Broadcast()
		}
		l.walFastMu.Unlock()
	}

	close(db.closeCh)
	db.writeMu.Unlock()
	db.wg.Wait()
	db.valueLogDictTrainerMu.Lock()
	trainer := db.valueLogDictTrainer
	db.valueLogDictTrainer = nil
	db.valueLogDictTrainerMu.Unlock()
	if trainer != nil {
		trainer.Close()
	}
	if db.hashSortedIndexer != nil {
		db.hashSortedIndexer.Close()
		db.hashSortedIndexer = nil
	}
	if db.valueLogReader != nil {
		if err := db.valueLogReader.Close(); err != nil {
			errs = append(errs, err)
		}
		db.valueLogReader = nil
	}

	var walBytes int64
	var walPaths []string
	walPaths = make([]string, 0, len(db.lanes))
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		walBytes += l.walClosedBytes.Load()
		for path := range l.walClosedSizes {
			walPaths = append(walPaths, path)
		}
		if l.wal != nil {
			walBytes += l.wal.Size()
			l.walLiveBytes.Store(0)
			if l.walPath != "" {
				walPaths = append(walPaths, l.walPath)
			}
			_ = l.wal.Close()
			l.wal = nil
		} else if l.walPath != "" {
			walPaths = append(walPaths, l.walPath)
		}
		l.walMu.Unlock()

		l.vlogMu.Lock()
		if l.vlog != nil {
			_ = l.vlog.Close()
			l.vlog = nil
			l.vlogLiveBytes.Store(0)
		}
		l.vlogMu.Unlock()
	}

	if walBytes > 0 && !hadMemtables {
		backendBatch := db.backend.NewBatch()
		db.flushMu.Lock()
		err := backendBatch.WriteSync()
		db.flushMu.Unlock()
		if cerr := backendBatch.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	seen := make(map[string]struct{}, len(walPaths))
	removed := false
	for _, path := range walPaths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		retain := db.valueLogRetained(path)
		if retain {
			continue
		}
		db.dropValueLogSegment(path)
		if err := db.removeFileRetry(path); err != nil {
			// Best effort cleanup; ignore errors to prevent flakiness on Windows
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
		db.forgetValueLogRetain(path)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}

	if err := db.backend.Close(); err != nil {
		errs = append(errs, err)
	}
	if bgErr := db.backgroundError(); bgErr != nil {
		errs = append(errs, bgErr)
	}
	return errors.Join(errs...)
}
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	db.waitForCheckpoint()
	db.waitForStop()
	return db.set(key, value, false)
}

func (db *DB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	db.waitForCheckpoint()
	db.waitForStop()
	return db.set(key, value, true)
}

func (db *DB) flushAllMemtablesForSync(sync bool) error {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	db.mu.Unlock()

	for db.flushCombinedLocked(sync) {
	}
	return db.backgroundError()
}

func (db *DB) syncBarrierAfterWrite(sync bool) error {
	if !sync {
		return nil
	}
	if !db.disableJournal {
		// Journal durability is handled by appendValueLog + appendWAL:
		// - strict: fsync
		// - relaxed: flush-to-kernel (no fsync)
		return nil
	}
	if db.relaxedSync {
		// Journal disabled: enforce a backend flush boundary without fsync.
		return db.flushAllMemtablesForSync(false)
	}
	// Journal disabled: enforce a durable backend boundary.
	return db.Checkpoint()
}

func (db *DB) set(key, value []byte, sync bool) error {
	db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false
	var ptr page.ValuePtr
	var retainPath string
	usePointer := false
	debugPtr := db.debugFlushPointers

	shard := db.shardForKey(key)
	shard.mu.Lock()
	if db.shardExceedsLimit(shard, int64(len(key)+len(value))) {
		shard.mu.Unlock()
		db.writeMu.RUnlock()
		return ErrMemtableFull
	}
	shard.mu.Unlock()

	durability := journalDurabilityNone
	if sync {
		if db.relaxedSync {
			durability = journalDurabilityFlush
		} else {
			durability = journalDurabilitySync
		}
	}
	eligible := len(value) > db.valueLogThreshold
	valueLogEnabled := db.valueLogEnabled()
	allowPointers := eligible && valueLogEnabled && db.allowValueLogPointers()
	if debugPtr && eligible {
		db.debugPtrEligible.Add(1)
	}

	var lane *lane
	if allowPointers || !db.disableJournal {
		l, err := db.pickLane(durability == journalDurabilitySync)
		if err != nil {
			db.writeMu.RUnlock()
			return err
		}
		lane = l
		if durability == journalDurabilitySync {
			defer db.releaseLaneSync(lane)
		}
	}

	if allowPointers {
		dictID := uint64(0)
		if db.valueLogDictTrain.TrainBytes > 0 {
			id, err := db.currentDictID(context.Background())
			if err != nil {
				db.writeMu.RUnlock()
				return err
			}
			dictID = id
		} else {
			dictID = db.dictCurrentCached.Load()
		}

		var dictBytes []byte
		if dictID != 0 {
			b, err := db.dictBytes(context.Background(), dictID)
			if err != nil {
				db.writeMu.RUnlock()
				return err
			}
			dictBytes = b
		}

		rid := db.nextRID.Add(1)
		var retain string
		appendPtr, retain, appendErr := db.appendValueLogOne(lane, dictID, dictBytes, rid, value, durability)
		if appendErr != nil {
			db.writeMu.RUnlock()
			return appendErr
		}
		ptr = appendPtr
		usePointer = true
		if debugPtr {
			db.debugPtrUsed.Add(1)
		}
		retainPath = retain

		if !db.disableJournal {
			rec := logRecord{Op: logOpSetRID, Key: key, RID: rid}
			if err := db.appendWAL(lane, []logRecord{rec}, durability); err != nil {
				db.writeMu.RUnlock()
				return err
			}
		}
	} else if !db.disableJournal {
		if debugPtr && eligible {
			if !valueLogEnabled {
				db.debugPtrDisabled.Add(1)
			} else {
				db.debugPtrDenied.Add(1)
			}
		}
		rec := logRecord{Op: logOpSetInline, Key: key, Value: value}
		if err := db.appendWAL(lane, []logRecord{rec}, durability); err != nil {
			db.writeMu.RUnlock()
			return err
		}
	} else if debugPtr && eligible {
		if !valueLogEnabled {
			db.debugPtrDisabled.Add(1)
		} else {
			db.debugPtrDenied.Add(1)
		}
	}

	shard.mu.Lock()
	storeValue := value
	if db.memtableValueLogPointers && usePointer {
		storeValue = nil
	}
	if shard.largePtrs != nil {
		err := shard.mem.PutWithCallback(key, storeValue, func(k, _ []byte) error {
			keyStr := bytesToStringNoCopy(k)
			if usePointer {
				shard.largePtrs.SetString(keyStr, ptr)
			} else {
				shard.largePtrs.DeleteString(keyStr)
			}
			return nil
		})
		if err != nil {
			shard.mu.Unlock()
			db.writeMu.RUnlock()
			return err
		}
	} else {
		shard.mem.Set(key, storeValue)
	}
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	db.mutableBytes.Add(delta)
	shard.mu.Unlock()
	db.noteWriteKey(key)

	// 3. Check Threshold
	if db.mutableBytes.Load() > db.mutableFlushThreshold() {
		needRotate = true
	}
	if sync && db.disableJournal {
		needSyncBarrier = true
	}
	db.writeMu.RUnlock()

	if retainPath != "" {
		db.markValueLogRetain(retainPath)
	}

	if needRotate {
		if err := db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	db.waitForStop()
	return db.delete(key, false)
}

// DeleteRange deletes all keys in the range [start, end).
//
// When WAL is disabled and the backend is empty, a full-range delete can be
// satisfied by clearing the in-memory layers without enumerating keys.
func (db *DB) DeleteRange(start, end []byte) error {
	if db == nil {
		return nil
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	db.waitForCheckpoint()
	db.waitForStop()

	// Journal-enabled mode: do a snapshot scan and apply per-key deletes directly.
	// Append journal records one-by-one to preserve batch atomicity and to avoid
	// post-journal apply divergence on partial batch failure.
	if !db.disableJournal {
		db.writeMu.Lock()
		defer db.writeMu.Unlock()

		it, err := db.Iterator(start, end)
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()

		applyDelete := func(key []byte) error {
			if maxMemtableBytesPerShard > 0 && int64(len(key)) > maxMemtableBytesPerShard {
				return ErrMemtableFull
			}
			for {
				shard := db.shardForKey(key)
				shard.mu.Lock()
				if db.shardExceedsLimit(shard, int64(len(key))) {
					shard.mu.Unlock()
					db.mu.Lock()
					err := db.rotateMemtableLocked(true)
					db.mu.Unlock()
					if err != nil {
						return err
					}
					continue
				}
				if shard.largePtrs != nil {
					if err := shard.mem.DeleteWithCallback(key, func(k, _ []byte) error {
						shard.largePtrs.DeleteString(bytesToStringNoCopy(k))
						return nil
					}); err != nil {
						shard.mu.Unlock()
						return err
					}
				} else {
					shard.mem.Delete(key)
				}
				shard.rng.add(key)
				newBytes := shard.mem.Size()
				delta := newBytes - shard.bytes
				shard.bytes = newBytes
				db.mutableBytes.Add(delta)
				shard.mu.Unlock()
				db.noteWriteKey(key)
				if db.mutableBytes.Load() > db.mutableFlushThreshold() {
					db.mu.Lock()
					if db.mutableBytes.Load() > db.mutableFlushThreshold() {
						if err := db.rotateMemtableLocked(true); err != nil {
							db.mu.Unlock()
							return err
						}
					}
					db.mu.Unlock()
				}
				return nil
			}
		}

		poisonApply := func(err error) error {
			if err == nil {
				return nil
			}
			db.reportError(fmt.Errorf("cachingdb: WAL apply failed: %w", err))
			db.walAckMu.Lock()
			if db.walErr == nil {
				db.walErr = err
			}
			db.walAckMu.Unlock()
			return err
		}

		preRotate := func(key []byte) error {
			if maxMemtableBytesPerShard > 0 && int64(len(key)) > maxMemtableBytesPerShard {
				return ErrMemtableFull
			}
			if db.mutableBytes.Load() > db.mutableFlushThreshold() {
				db.mu.Lock()
				if db.mutableBytes.Load() > db.mutableFlushThreshold() {
					err := db.rotateMemtableLocked(true)
					db.mu.Unlock()
					if err != nil {
						return err
					}
				} else {
					db.mu.Unlock()
				}
			}
			shard := db.shardForKey(key)
			shard.mu.Lock()
			exceeds := db.shardExceedsLimit(shard, int64(len(key)))
			shard.mu.Unlock()
			if exceeds {
				db.mu.Lock()
				err := db.rotateMemtableLocked(true)
				db.mu.Unlock()
				if err != nil {
					return err
				}
			}
			return nil
		}

		lane, err := db.pickLane(false)
		if err != nil {
			return err
		}
		for it.Valid() {
			key := it.Key()
			if err := preRotate(key); err != nil {
				return err
			}
			if err := db.appendWAL(lane, []logRecord{{Op: logOpDelete, Key: key}}, journalDurabilityNone); err != nil {
				return err
			}
			if err := applyDelete(key); err != nil {
				return poisonApply(err)
			}
			it.Next()
		}
		if err := it.Error(); err != nil {
			return err
		}

		db.noteWrite()
		db.maybeAssistFlush()
		return nil
	}

	// Ensure we know whether the backend currently contains any keys so that we
	// can safely take the "clear memtables" fast path on empty backends.
	if err := db.ensureBackendRange(); err != nil {
		return err
	}

	// Fast path: when the journal is disabled and there is no in-memory state to merge,
	// avoid snapshot isolation/merge iterators and delete directly from the
	// backend in a single commit.
	//
	// This is safe only when we have no queued memtables and the mutable memtable
	// is empty; otherwise we'd violate "newest wins" semantics.
	if db.disableJournal {
		db.mu.Lock()
		backendOnly := len(db.queue) == 0 && db.mutableBytes.Load() == 0
		db.mu.Unlock()
		if backendOnly {
			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			defer func() { _ = it.Close() }()

			b := db.backend.NewBatch()
			defer func() { _ = b.Close() }()
			for it.Valid() {
				if err := b.Delete(it.Key()); err != nil {
					return err
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				return err
			}
			if err := b.Write(); err != nil {
				return err
			}
			// Best-effort: backend range can shrink; force recompute later.
			db.mu.Lock()
			db.backendRangeKnown = false
			db.backendRange = keyRange{}
			db.mu.Unlock()
			return nil
		}
	}

	// Serialize against flushers while we inspect/clear the in-memory layers.
	db.flushMu.Lock()
	db.mu.Lock()

	// Compute overall min/max across in-memory layers.
	var (
		haveAny bool
		minKey  []byte
		maxKey  []byte
	)
	addRange := func(r keyRange) {
		if !r.valid {
			return
		}
		if !haveAny {
			haveAny = true
			minKey = r.min
			maxKey = r.max
			return
		}
		if bytes.Compare(r.min, minKey) < 0 {
			minKey = r.min
		}
		if bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}

	mutableRange := db.snapshotMutableRange()
	addRange(mutableRange)
	for _, r := range db.queueRanges {
		addRange(r)
	}

	backendEmpty := db.backendRangeKnown && !db.backendRange.valid
	if !backendEmpty {
		addRange(db.backendRange)
	}

	if haveAny {
		coversAll := true
		if start != nil && bytes.Compare(start, minKey) > 0 {
			coversAll = false
		}
		if end != nil && bytes.Compare(end, maxKey) <= 0 {
			// end is exclusive; to cover maxKey it must be strictly greater.
			coversAll = false
		}

		// Fast path: if the backend is empty and the delete range covers all keys we
		// currently have buffered in memory, just drop the in-memory state. This
		// avoids iterator creation, merges, and per-key tombstones.
		if coversAll && db.disableJournal && backendEmpty {
			curMode := db.memtableMode
			nextMode := curMode
			if db.memtableAdaptive {
				nextMode = db.maybeSwitchAdaptiveMemtableModeLocked()
				db.memtableWarmupActive = false
				db.resetMemtableStatsLocked()
			}

			db.queue = nil
			db.queueRanges = nil
			db.queueWALPaths = nil
			db.queueValueLogPaths = nil
			db.queueLargePtrs = nil
			db.queueBacklogBytes.Store(0)
			if err := db.resetMutableShardsLocked(nextMode, nextMode == curMode); err != nil {
				db.mu.Unlock()
				db.flushMu.Unlock()
				return err
			}

			db.mu.Unlock()
			db.flushMu.Unlock()
			return nil
		}
	}

	db.mu.Unlock()
	db.flushMu.Unlock()

	// When the journal is disabled (op-geth style "unsafe" mode), avoid snapshot
	// isolation and MergingIterator overhead. DeleteRange doesn't require sorted
	// enumeration across sources; we can scan each source independently and write
	// tombstones into the current mutable memtable.
	if db.disableJournal {
		// Serialize writers and flushers while we enumerate keys to delete and
		// apply tombstones. This keeps snapshot semantics simple and avoids
		// rotating the memtable (which can allocate large arenas).
		db.flushMu.Lock()
		defer db.flushMu.Unlock()
		db.writeMu.Lock()
		defer db.writeMu.Unlock()

		db.mu.Lock()
		mutableRange := db.snapshotMutableRange()
		coversInMemory := queryCoversRange(start, end, mutableRange)
		for _, r := range db.queueRanges {
			if !queryCoversRange(start, end, r) {
				coversInMemory = false
				break
			}
		}

		// Fast path (DisableWAL only): if the delete range covers all buffered
		// in-memory keys, clear the in-memory layers and delete directly from the
		// backend. This avoids building large tombstone sets and avoids per-key
		// copies into an intermediate slice.
		if coversInMemory {
			curMode := db.memtableMode
			nextMode := curMode
			if db.memtableAdaptive {
				nextMode = db.maybeSwitchAdaptiveMemtableModeLocked()
				db.memtableWarmupActive = false
				db.resetMemtableStatsLocked()
			}

			db.queue = nil
			db.queueRanges = nil
			db.queueWALPaths = nil
			db.queueValueLogPaths = nil
			db.queueLargePtrs = nil
			db.queueBacklogBytes.Store(0)
			if err := db.resetMutableShardsLocked(nextMode, nextMode == curMode); err != nil {
				db.mu.Unlock()
				return err
			}
			db.mu.Unlock()

			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			defer func() { _ = it.Close() }()

			b := db.backend.NewBatch()
			defer func() { _ = b.Close() }()

			for it.Valid() {
				if err := b.Delete(it.Key()); err != nil {
					return err
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				return err
			}
			if err := b.Write(); err != nil {
				return err
			}

			// Best-effort: backend range can shrink; force recompute later.
			db.mu.Lock()
			db.backendRangeKnown = false
			db.backendRange = keyRange{}
			db.mu.Unlock()

			db.noteWrite()
			return nil
		}

		backendRange := db.backendRange

		// If we need to enumerate keys from the current mutable memtable, rotate it
		// first so we never mutate a memtable while iterating it.
		if overlapsQuery(start, end, mutableRange) && db.mutableBytes.Load() > 0 {
			if queryCoversRange(start, end, mutableRange) {
				if err := db.resetMutableShardsLocked(db.memtableMode, true); err != nil {
					db.mu.Unlock()
					return err
				}
			} else {
				if err := db.rotateMemtableLocked(false); err != nil {
					db.mu.Unlock()
					return err
				}
			}
			mutableRange = db.snapshotMutableRange()
		}

		// Drop fully-covered queued memtables without enumerating their keys.
		if len(db.queue) > 0 {
			dstQueue := db.queue[:0]
			dstRanges := db.queueRanges[:0]
			dstWALPaths := db.queueWALPaths[:0]
			dstValueLogPaths := db.queueValueLogPaths[:0]
			dstLargePtrs := db.queueLargePtrs[:0]
			for i, mem := range db.queue {
				r := keyRange{}
				if i < len(db.queueRanges) {
					r = db.queueRanges[i]
				}
				if queryCoversRange(start, end, r) {
					db.queueBacklogBytes.Add(-mem.Size())
					continue
				}
				dstQueue = append(dstQueue, mem)
				dstRanges = append(dstRanges, r)
				if i < len(db.queueWALPaths) {
					dstWALPaths = append(dstWALPaths, db.queueWALPaths[i])
				} else {
					dstWALPaths = append(dstWALPaths, nil)
				}
				if i < len(db.queueValueLogPaths) {
					dstValueLogPaths = append(dstValueLogPaths, db.queueValueLogPaths[i])
				} else {
					dstValueLogPaths = append(dstValueLogPaths, nil)
				}
				if i < len(db.queueLargePtrs) {
					dstLargePtrs = append(dstLargePtrs, db.queueLargePtrs[i])
				} else {
					dstLargePtrs = append(dstLargePtrs, nil)
				}
			}
			db.queue = dstQueue
			db.queueRanges = dstRanges
			db.queueWALPaths = dstWALPaths
			db.queueValueLogPaths = dstValueLogPaths
			db.queueLargePtrs = dstLargePtrs
		}
		db.publishMemtablesLocked()

		// Snapshot sources after any rotations/drops.
		mutableHasData := db.mutableBytes.Load() > 0
		mutableRanges := make([]keyRange, len(db.mutableShards))
		mutables := make([]memtable.Table, len(db.mutableShards))
		for i := range db.mutableShards {
			shard := &db.mutableShards[i]
			shard.mu.Lock()
			mutables[i] = shard.mem
			mutableRanges[i] = cloneRange(shard.rng)
			shard.mu.Unlock()
		}
		queue := append([]memtable.Table(nil), db.queue...)
		queueRanges := append([]keyRange(nil), db.queueRanges...)
		db.mu.Unlock()

		var (
			backendIter  iterator.UnsafeIterator
			queueIters   []iterator.UnsafeIterator
			mutableIters []iterator.UnsafeIterator
		)

		if overlapsQuery(start, end, backendRange) {
			it, err := db.backend.Iterator(start, end)
			if err != nil {
				return err
			}
			backendIter = it
			defer func() { _ = backendIter.Close() }()
		}

		if mutableHasData {
			for i, mem := range mutables {
				if mem == nil {
					continue
				}
				if i < len(mutableRanges) && !overlapsQuery(start, end, mutableRanges[i]) {
					continue
				}
				it := mem.NewIterator(start, end)
				it.Seek(start)
				mutableIters = append(mutableIters, it)
				defer func(it iterator.UnsafeIterator) { _ = it.Close() }(it)
			}
		}

		for i, mem := range queue {
			if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
				continue
			}
			it := mem.NewIterator(start, end)
			it.Seek(start)
			queueIters = append(queueIters, it)
			defer func(it iterator.UnsafeIterator) { _ = it.Close() }(it)
		}

		db.mu.Lock()
		applyDelete := func(key []byte) error {
			shard := db.shardForKey(key)
			shard.mu.Lock()
			if db.shardExceedsLimit(shard, int64(len(key))) {
				shard.mu.Unlock()
				return ErrMemtableFull
			}
			if shard.largePtrs != nil {
				if err := shard.mem.DeleteWithCallback(key, func(k, _ []byte) error {
					shard.largePtrs.DeleteString(bytesToStringNoCopy(k))
					return nil
				}); err != nil {
					shard.mu.Unlock()
					return err
				}
			} else {
				shard.mem.Delete(key)
			}
			shard.rng.add(key)
			newBytes := shard.mem.Size()
			delta := newBytes - shard.bytes
			shard.bytes = newBytes
			db.mutableBytes.Add(delta)
			shard.mu.Unlock()
			if db.mutableBytes.Load() > db.mutableFlushThreshold() {
				if err := db.rotateMemtableLocked(true); err != nil {
					return err
				}
			}
			return nil
		}

		for _, it := range mutableIters {
			for it.Valid() {
				if !it.IsDeleted() {
					if err := applyDelete(it.UnsafeKey()); err != nil {
						db.mu.Unlock()
						return err
					}
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}

		if backendIter != nil {
			for backendIter.Valid() {
				if err := applyDelete(backendIter.Key()); err != nil {
					db.mu.Unlock()
					return err
				}
				backendIter.Next()
			}
			if err := backendIter.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}

		for _, it := range queueIters {
			for it.Valid() {
				if !it.IsDeleted() {
					if err := applyDelete(it.UnsafeKey()); err != nil {
						db.mu.Unlock()
						return err
					}
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				db.mu.Unlock()
				return err
			}
		}
		db.mu.Unlock()

		db.noteWrite()
		db.maybeAssistFlush()
		return nil
	}

	// Fallback: enumerate keys via a snapshot iterator.
	it, err := db.Iterator(start, end)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	b := db.NewBatch()
	defer b.Close()
	for it.Valid() {
		if err := b.Delete(it.Key()); err != nil {
			return err
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return b.Write()
}

func (db *DB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	db.waitForStop()
	return db.delete(key, true)
}

func (db *DB) delete(key []byte, sync bool) error {
	db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false

	shard := db.shardForKey(key)
	shard.mu.Lock()
	if db.shardExceedsLimit(shard, int64(len(key))) {
		shard.mu.Unlock()
		db.writeMu.RUnlock()
		return ErrMemtableFull
	}
	shard.mu.Unlock()

	if !db.disableJournal {
		durability := journalDurabilityNone
		if sync {
			if db.relaxedSync {
				durability = journalDurabilityFlush
			} else {
				durability = journalDurabilitySync
			}
		}
		lane, err := db.pickLane(durability == journalDurabilitySync)
		if err != nil {
			db.writeMu.RUnlock()
			return err
		}
		if durability == journalDurabilitySync {
			defer db.releaseLaneSync(lane)
		}
		rec := logRecord{Op: logOpDelete, Key: key}
		if err := db.appendWAL(lane, []logRecord{rec}, durability); err != nil {
			db.writeMu.RUnlock()
			return err
		}
	}

	shard.mu.Lock()
	if shard.largePtrs != nil {
		err := shard.mem.DeleteWithCallback(key, func(k, _ []byte) error {
			shard.largePtrs.DeleteString(bytesToStringNoCopy(k))
			return nil
		})
		if err != nil {
			shard.mu.Unlock()
			db.writeMu.RUnlock()
			return err
		}
	} else {
		shard.mem.Delete(key)
	}
	shard.rng.add(key)
	newBytes := shard.mem.Size()
	delta := newBytes - shard.bytes
	shard.bytes = newBytes
	db.mutableBytes.Add(delta)
	shard.mu.Unlock()
	db.noteWriteKey(key)

	// 3. Threshold
	if db.mutableBytes.Load() > db.mutableFlushThreshold() {
		needRotate = true
	}
	if sync && db.disableJournal {
		needSyncBarrier = true
	}
	db.writeMu.RUnlock()

	if needRotate {
		if err := db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) canReuseWALSegments() bool {
	for i := range db.lanes {
		l := &db.lanes[i]
		l.walMu.Lock()
		w := l.wal
		live := l.walLiveBytes.Load()
		l.walMu.Unlock()
		if w == nil {
			return false
		}
		if live >= 10*1024*1024 {
			return false
		}
	}
	return true
}

func (db *DB) rotateMemtableLockedWithCapacity(triggerFlush bool, newCapacity int) error {
	var walPaths []string
	var valueLogPaths []string
	if !db.disableJournal {
		walPaths = db.currentWALPaths()
	}
	if db.valueLogEnabled() {
		valueLogPaths = db.currentValueLogPaths()
	}
	if newCapacity < 0 {
		newCapacity = db.memtableCap
	}
	if db.memtableAdaptive {
		db.maybeSwitchAdaptiveMemtableModeLocked()
	}
	if db.memtableWarmupActive {
		db.memtableWarmupActive = false
	}
	db.mutableBytes.Store(0)
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		shard.mem.Freeze()
		memBytes := shard.mem.Size()
		db.queue = append(db.queue, shard.mem)
		db.queueBacklogBytes.Add(memBytes)
		db.queueRanges = append(db.queueRanges, shard.rng)
		db.queueWALPaths = append(db.queueWALPaths, walPaths)
		db.queueValueLogPaths = append(db.queueValueLogPaths, valueLogPaths)
		db.queueLargePtrs = append(db.queueLargePtrs, shard.largePtrs)

		mt, err := memtable.NewWithCapacityModeAndIndexer(newCapacity, db.memtableMode, db.hashSortedIndexer)
		if err != nil {
			shard.mu.Unlock()
			return err
		}
		shard.mem = mt
		shard.rng = keyRange{}
		shard.bytes = 0
		shard.largePtrs = db.newLargePtrMap()
		shard.mu.Unlock()
	}
	if db.memtableAdaptive {
		db.resetMemtableStatsLocked()
	}
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()

	// Optimization: Reuse WAL if small (e.g. < 10MB) to avoid syscall overhead
	// on frequent rotations (e.g. caused by frequent Iterator creation).
	if !db.disableJournal {
		if db.canReuseWALSegments() {
			if triggerFlush {
				select {
				case db.flushCh <- struct{}{}:
				default:
				}
			}
			return nil
		}
		for i := range db.lanes {
			if err := db.rotateWALLocked(&db.lanes[i]); err != nil {
				return err
			}
		}
	}

	if triggerFlush {
		select {
		case db.flushCh <- struct{}{}:
		default:
		}
	}
	db.bpMu.Lock()
	db.bpCond.Broadcast()
	db.bpMu.Unlock()
	return nil
}

// rotateMemtableLockedForIterator rotates the current mutable memtable into the
// immutable queue for snapshot iteration without rotating the WAL segment.
//
// This is important for concurrency: iterator creation should not need to
// serialize behind writeMu just to protect WAL rotation.
//
// Caller must hold db.mu.
func (db *DB) rotateMemtableLockedForIterator(newCapacity int) error {
	return db.rotateMutableShardsLocked(newCapacity, false)
}

func (db *DB) rotateMemtableLocked(triggerFlush bool) error {
	return db.rotateMemtableLockedWithCapacity(triggerFlush, -1)
}

func (db *DB) rotateMemtableIfNeeded(triggerFlush bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.mutableBytes.Load() <= db.mutableFlushThreshold() {
		return nil
	}
	return db.rotateMutableShardsLocked(-1, triggerFlush)
}

func (db *DB) maybeRotateMemtable(triggerFlush bool) error {
	if db.mutableBytes.Load() <= db.mutableFlushThreshold() {
		return nil
	}
	if !db.rotatePending.CompareAndSwap(false, true) {
		return nil
	}
	defer db.rotatePending.Store(false)
	return db.rotateMemtableIfNeeded(triggerFlush)
}

// rotateMutableShardsLocked rotates the current mutable shards into the queue
// while holding db.mu (write) and the affected shard locks.
//
// It intentionally does not rotate the WAL segment; checkpoint is responsible
// for establishing durable boundaries and trimming old segments. This avoids
// requiring a global writer barrier around WAL rotation.
func (db *DB) rotateMutableShardsLocked(newCapacity int, triggerFlush bool) error {
	if newCapacity < 0 {
		newCapacity = db.memtableCap
	}
	if db.memtableAdaptive {
		db.maybeSwitchAdaptiveMemtableModeLocked()
	}
	if db.memtableWarmupActive {
		db.memtableWarmupActive = false
	}
	var walPaths []string
	if !db.disableJournal {
		walPaths = db.currentWALPaths()
	}
	var valueLogPaths []string
	if db.valueLogEnabled() {
		valueLogPaths = db.currentValueLogPaths()
	}

	locked := make([]*memShard, 0, len(db.mutableShards))
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].mu.Unlock()
		}
	}()

	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		locked = append(locked, shard)

		// Remove this shard's contribution from the global byte counter before
		// resetting it, since writers may still be updating other shards.
		if shard.bytes != 0 {
			db.mutableBytes.Add(-shard.bytes)
		}

		// Freeze and enqueue the old mutable shard.
		shard.mem.Freeze()
		memBytes := shard.mem.Size()
		db.queue = append(db.queue, shard.mem)
		db.queueBacklogBytes.Add(memBytes)
		db.queueRanges = append(db.queueRanges, shard.rng)
		db.queueWALPaths = append(db.queueWALPaths, walPaths)
		db.queueValueLogPaths = append(db.queueValueLogPaths, valueLogPaths)
		db.queueLargePtrs = append(db.queueLargePtrs, shard.largePtrs)

		mt, err := memtable.NewWithCapacityModeAndIndexer(newCapacity, db.memtableMode, db.hashSortedIndexer)
		if err != nil {
			return err
		}
		shard.mem = mt
		shard.rng = keyRange{}
		shard.bytes = 0
		shard.largePtrs = db.newLargePtrMap()
	}

	if db.memtableAdaptive {
		db.resetMemtableStatsLocked()
	}
	db.updateMutableThresholdLocked()
	db.publishMemtablesLocked()

	if triggerFlush {
		select {
		case db.flushCh <- struct{}{}:
		default:
		}
	}
	db.bpMu.Lock()
	db.bpCond.Broadcast()
	db.bpMu.Unlock()
	return nil
}

func (db *DB) cleanupLaneWALWriters(l *lane) {
	if l == nil {
		return
	}
	l.walMu.Lock()
	if l.wal != nil {
		_ = l.wal.Close()
		l.wal = nil
	}
	l.walMu.Unlock()
	l.vlogMu.Lock()
	if l.vlog != nil {
		_ = l.vlog.Close()
		l.vlog = nil
	}
	l.vlogMu.Unlock()
}

func (db *DB) rotateWALLocked(l *lane) error {
	if db.disableJournal {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	l.walMu.Lock()
	defer l.walMu.Unlock()
	l.walSeq++
	name := commitLogName(l.id, l.walSeq)
	path := filepath.Join(db.dir, name)

	if l.wal != nil {
		oldPath := l.walPath
		oldSize := l.wal.Size()
		if err := l.wal.RotateTo(path); err != nil {
			return err
		}
		l.walLiveBytes.Store(0)
		if oldPath != "" {
			if l.walClosedSizes == nil {
				l.walClosedSizes = make(map[string]int64)
			}
			prev := l.walClosedSizes[oldPath]
			l.walClosedSizes[oldPath] = oldSize
			l.walClosedBytes.Add(oldSize - prev)
		}
	} else {
		w, err := commitlog.NewWriterWithOptions(path, commitlog.Options{MaxSegmentSize: db.walMaxSegmentBytes})
		if err != nil {
			return err
		}
		l.wal = w
		l.walLiveBytes.Store(0)
	}
	l.walPath = path
	l.walLiveBytes.Store(0)
	if db.splitValueLogEnabled() {
		if err := db.rotateValueLogLocked(l); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) rotateValueLogLocked(l *lane) error {
	if !db.splitValueLogEnabled() {
		return nil
	}
	if l == nil {
		return errWALUnavailable
	}
	l.vlogMu.Lock()
	defer l.vlogMu.Unlock()
	l.vlogSeq++
	name := valueLogName(l.id, l.vlogSeq)
	path := filepath.Join(db.dir, name)
	fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
	if err != nil {
		return err
	}

	if l.vlog != nil {
		oldPath := l.vlogPath
		oldSize := l.vlog.Size()
		if err := l.vlog.RotateTo(path, fileID); err != nil {
			return err
		}
		l.vlogLiveBytes.Store(0)
		if oldPath != "" {
			if l.vlogClosedSizes == nil {
				l.vlogClosedSizes = make(map[string]int64)
			}
			prev := l.vlogClosedSizes[oldPath]
			l.vlogClosedSizes[oldPath] = oldSize
			l.vlogClosedBytes.Add(oldSize - prev)
			if oldPath == l.vlogRetainedPath {
				db.valueLogRetainedClosedBytes.Add(oldSize - prev)
			}
		}
	} else {
		w, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			return err
		}
		l.vlog = w
		l.vlogLiveBytes.Store(0)
	}
	l.vlogPath = path
	l.vlogLiveBytes.Store(0)
	return nil
}

func (db *DB) untrackWALSegmentLocked(path string) {
	laneID, _, _, ok := parseLogSeq(filepath.Base(path))
	if !ok || laneID < 0 || laneID >= len(db.lanes) {
		return
	}
	l := &db.lanes[laneID]
	l.walMu.Lock()
	defer l.walMu.Unlock()
	if l.walClosedSizes == nil || path == "" {
		return
	}
	size, ok := l.walClosedSizes[path]
	if !ok {
		return
	}
	delete(l.walClosedSizes, path)
	for {
		cur := l.walClosedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if l.walClosedBytes.CompareAndSwap(cur, next) {
			break
		}
	}
}

func (db *DB) untrackValueLogSegmentLocked(path string) {
	laneID, _, _, ok := parseLogSeq(filepath.Base(path))
	if !ok || laneID < 0 || laneID >= len(db.lanes) {
		return
	}
	l := &db.lanes[laneID]
	l.vlogMu.Lock()
	defer l.vlogMu.Unlock()
	if l.vlogClosedSizes == nil || path == "" {
		return
	}
	size, ok := l.vlogClosedSizes[path]
	if !ok {
		return
	}
	delete(l.vlogClosedSizes, path)
	db.valueLogRetainedClosedBytes.Add(-size)
	for {
		cur := l.vlogClosedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if l.vlogClosedBytes.CompareAndSwap(cur, next) {
			break
		}
	}
}

func (db *DB) flushLoop() {
	defer db.wg.Done()

	for {
		select {
		case <-db.closeCh:
			// Flush all remaining with sync on close.
			db.flushAll(true)
			return
		case <-db.flushCh:
			// Background flush is async when WAL is enabled. Without a WAL, we
			// upgrade to a synced flush unless RelaxedSync is set.
			db.flushAll(false)
		}
	}
}

func (db *DB) flushSyncRequested(sync bool) bool {
	if sync {
		return true
	}
	// Only force a synced flush when operating in the legacy "everything off"
	// mode (DisableWAL semantics), where we have neither a redo/journal nor a
	// value-log durability mechanism. Disabling the journal alone must not
	// implicitly force fsync-heavy flush behavior.
	if db.disableJournal && db.disableValueLog && !db.relaxedSync {
		return true
	}
	return false
}

func (db *DB) flushAll(sync bool) {
	origSync := sync
	sync = db.flushSyncRequested(sync)
	if !origSync && sync && db.disableJournal && !db.relaxedSync {
		db.debugVlogEvent("flushAll_upgraded_sync", -1, "flushMu")
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	for db.flushCombinedLocked(sync) {
	}
}

func (db *DB) flushOne() bool {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	return db.flushOneLocked(true)
}

const (
	flushCombineTargetBytes  int64 = 64 * 1024 * 1024 // 64MiB
	flushCombineMaxMemtables       = 32
)

func (db *DB) flushCombinedLocked(sync bool) bool {
	db.mu.Lock()
	queueLen := len(db.queue)
	if queueLen == 0 {
		db.mu.Unlock()
		return false
	}

	max := queueLen
	if max > flushCombineMaxMemtables {
		max = flushCombineMaxMemtables
	}

	mems := make([]memtable.Table, max)
	ranges := make([]keyRange, max)
	walPaths := make([][]string, max)
	largePtrs := make([]*largePtrMap, max)
	copy(mems, db.queue[:max])
	copy(ranges, db.queueRanges[:max])
	copy(walPaths, db.queueWALPaths[:max])
	copy(largePtrs, db.queueLargePtrs[:max])
	db.mu.Unlock()

	targetBytes := flushCombineTargetBytes
	if db.flushThreshold > targetBytes {
		targetBytes = db.flushThreshold
	}

	type flushUnit struct {
		mem      memtable.Table
		memBytes int64
		memLen   int
		memRange keyRange
		walPaths []string
		ptrs     *largePtrMap
	}

	units := make([]flushUnit, 0, max)
	var totalBytes int64
	var totalLen int
	for i := 0; i < max; i++ {
		mem := mems[i]
		memBytes := mem.Size()
		memLen := mem.Len()

		if len(units) > 0 && totalBytes >= targetBytes {
			break
		}
		units = append(units, flushUnit{
			mem:      mem,
			memBytes: memBytes,
			memLen:   memLen,
			memRange: ranges[i],
			walPaths: walPaths[i],
			ptrs:     largePtrs[i],
		})
		totalBytes += memBytes
		totalLen += memLen
	}

	debugPtr := db.debugFlushPointers
	var ptrChecks atomic.Int64
	var ptrHits atomic.Int64
	var ptrMisses atomic.Int64
	var ptrNoMap atomic.Int64
	var ptrMapKeys atomic.Int64

	// Optimization: Skip flush if the selected memtables have no entries.
	flushStart := time.Time{}
	flushed := false
	if totalLen > 0 {
		flushStart = time.Now()

		// Ensure value-log writes are flushed from user-space buffers before we
		// commit backend pointers that may reference them.
		if db.valueLogEnabled() {
			if err := db.flushValueLog(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (pre-flush vlog): %w", err))
				return false
			}
		}

		backendBatch := db.backend.NewBatch()

		if totalLen > 2000 {
			// Preserve "newest wins" semantics by concatenating per-memtable ops
			// in queue order (oldest -> newest). Within a single memtable there are
			// no duplicate keys.
			collectOps := func(mem memtable.Table, estLen int, ptrs *largePtrMap) ([]batch.Entry, error) {
				if debugPtr {
					if ptrs == nil {
						ptrNoMap.Add(1)
					} else {
						ptrMapKeys.Add(int64(ptrs.Len()))
					}
				}
				ops := getEntrySlice(estLen)
				iter := mem.NewIterator(nil, nil)
				ptrLookups := ptrs != nil && db.valueLogEnabled()
				for iter.Valid() {
					if iter.IsDeleted() {
						ops = append(ops, batch.Entry{
							Type: batch.OpDelete,
							Key:  iter.UnsafeKey(),
						})
					} else {
						key := iter.UnsafeKey()
						if ptrLookups {
							if debugPtr {
								ptrChecks.Add(1)
							}
							if ptr, ok := ptrs.Get(key); ok {
								if debugPtr {
									ptrHits.Add(1)
								}
								ops = append(ops, batch.Entry{
									Type:     batch.OpPut,
									Key:      key,
									ValuePtr: ptr,
									IsPtr:    true,
								})
								iter.Next()
								continue
							}
							if debugPtr {
								ptrMisses.Add(1)
							}
						}
						val := iter.UnsafeValue()
						if val == nil && db.memtableValueLogPointers {
							_ = iter.Close()
							putEntrySlice(ops)
							return nil, fmt.Errorf("cachingdb: flush missing value-log ptr for key")
						}
						ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, Value: val})
					}
					iter.Next()
				}
				err := iter.Close()
				if err == nil {
					err = iter.Error()
				}
				if err != nil {
					putEntrySlice(ops)
					return nil, err
				}
				return ops, err
			}

			buildConcurrency := db.flushBuildConcurrency
			if buildConcurrency <= 1 || len(units) <= 1 {
				ops := getEntrySlice(totalLen)
				for _, unit := range units {
					memOps, err := collectOps(unit.mem, unit.memLen, unit.ptrs)
					if err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (iter): %w", err))
						_ = backendBatch.Close()
						putEntrySlice(ops)
						return false
					}
					ops = append(ops, memOps...)
					putEntrySlice(memOps)
				}

				if err := backendBatch.SetOps(ops); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (setops): %w", err))
					_ = backendBatch.Close()
					putEntrySlice(ops)
					return false
				}
				putEntrySlice(ops)
			} else {
				sem := make(chan struct{}, buildConcurrency)
				unitOps := make([][]batch.Entry, len(units))
				done := make(chan struct{}, len(units))
				errCh := make(chan error, 1)

				for i := range units {
					i := i
					unit := units[i]
					go func() {
						sem <- struct{}{}
						defer func() { <-sem }()

						ops, err := collectOps(unit.mem, unit.memLen, unit.ptrs)
						if err != nil {
							select {
							case errCh <- err:
							default:
							}
							done <- struct{}{}
							return
						}
						unitOps[i] = ops
						done <- struct{}{}
					}()
				}
				for range units {
					<-done
				}

				select {
				case err := <-errCh:
					db.reportError(fmt.Errorf("cachingdb: flush failed (iter): %w", err))
					_ = backendBatch.Close()
					for _, ops := range unitOps {
						putEntrySlice(ops)
					}
					return false
				default:
				}

				ops := getEntrySlice(totalLen)
				for i := range unitOps {
					ops = append(ops, unitOps[i]...)
					putEntrySlice(unitOps[i])
				}

				if err := backendBatch.SetOps(ops); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (setops): %w", err))
					_ = backendBatch.Close()
					putEntrySlice(ops)
					return false
				}
				putEntrySlice(ops)
			}
		} else {
			var ptrSetter interface {
				SetPointer(key []byte, ptr page.ValuePtr) error
			}
			if db.valueLogEnabled() {
				ptrSetter, _ = backendBatch.(interface {
					SetPointer(key []byte, ptr page.ValuePtr) error
				})
			}
			for _, unit := range units {
				if debugPtr {
					if unit.ptrs == nil {
						ptrNoMap.Add(1)
					} else {
						ptrMapKeys.Add(int64(unit.ptrs.Len()))
					}
				}
				iter := unit.mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator
				ptrLookups := unit.ptrs != nil && db.valueLogEnabled()
				for iter.Valid() {
					key := iter.UnsafeKey()
					if iter.IsDeleted() {
						if err := backendBatch.Delete(key); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (delete): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					} else {
						if ptrLookups {
							if debugPtr {
								ptrChecks.Add(1)
							}
							if ptr, ok := unit.ptrs.Get(key); ok {
								if debugPtr {
									ptrHits.Add(1)
								}
								if ptrSetter != nil {
									if err := ptrSetter.SetPointer(key, ptr); err != nil {
										db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
										_ = iter.Close()
										_ = backendBatch.Close()
										return false
									}
								} else if err := backendBatch.SetOps([]batch.Entry{{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}}); err != nil {
									db.reportError(fmt.Errorf("cachingdb: flush failed (setops ptr): %w", err))
									_ = iter.Close()
									_ = backendBatch.Close()
									return false
								}
								iter.Next()
								continue
							}
							if debugPtr {
								ptrMisses.Add(1)
							}
						}
						val := iter.UnsafeValue()
						if val == nil && db.memtableValueLogPointers {
							db.reportError(fmt.Errorf("cachingdb: flush failed (missing value-log ptr for key)"))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
						if err := backendBatch.Set(key, val); err != nil {
							db.reportError(fmt.Errorf("cachingdb: flush failed (set): %w", err))
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					}
					iter.Next()
				}
				_ = iter.Close()
			}
		}

		// Commit to backend
		if db.valueLogEnabled() {
			if err := db.flushValueLog(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
				_ = backendBatch.Close()
				return false
			}
			if sync && !db.relaxedSync {
				if err := db.syncValueLog(); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
					_ = backendBatch.Close()
					return false
				}
			}
		}
		var err error
		if sync {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		if err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
			_ = backendBatch.Close()
			return false
		}
		_ = backendBatch.Close()
		flushed = true
	}
	flushDur := time.Duration(0)
	if flushed {
		flushDur = time.Since(flushStart)
	}
	if debugPtr && flushed {
		fmt.Fprintf(os.Stderr, "treedb: flush ptrs entries=%d checks=%d hits=%d misses=%d no_map=%d map_keys=%d eligible=%d used=%d noptr=%d denied=%d disabled=%d\n",
			totalLen,
			ptrChecks.Load(),
			ptrHits.Load(),
			ptrMisses.Load(),
			ptrNoMap.Load(),
			ptrMapKeys.Load(),
			db.debugPtrEligible.Load(),
			db.debugPtrUsed.Load(),
			db.debugPtrNoPtr.Load(),
			db.debugPtrDenied.Load(),
			db.debugPtrDisabled.Load(),
		)
	}

	// Remove from queue and delete old WAL segments.
	db.mu.Lock()
	for _, unit := range units {
		if unit.memRange.valid {
			db.backendRange.add(unit.memRange.min)
			db.backendRange.add(unit.memRange.max)
		}
	}

	if len(db.queue) >= len(units) {
		db.queue = db.queue[len(units):]
	}
	if len(db.queueRanges) >= len(units) {
		db.queueRanges = db.queueRanges[len(units):]
	}
	if len(db.queueWALPaths) >= len(units) {
		db.queueWALPaths = db.queueWALPaths[len(units):]
	}
	if len(db.queueValueLogPaths) >= len(units) {
		db.queueValueLogPaths = db.queueValueLogPaths[len(units):]
	}
	if len(db.queueLargePtrs) >= len(units) {
		db.queueLargePtrs = db.queueLargePtrs[len(units):]
	}
	db.queueBacklogBytes.Add(-totalBytes)
	db.publishMemtablesLocked()

	deletable := make([]string, 0, len(units))
	if sync {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, unit := range units {
			for _, walPath := range unit.walPaths {
				if walPath == "" {
					continue
				}
				if _, ok := inUse[walPath]; ok {
					continue
				}
				if _, ok := seen[walPath]; ok {
					continue
				}
				if db.valueLogRetained(walPath) {
					continue
				}
				seen[walPath] = struct{}{}
				deletable = append(deletable, walPath)
			}
		}
	}
	db.mu.Unlock()

	removed := false
	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			// Best effort cleanup
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()

	if flushed && flushDur > 0 && totalBytes > 0 {
		sample := float64(totalBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	} else {
		db.bpMu.Lock()
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

func (db *DB) flushOneLocked(sync bool) bool {
	db.mu.Lock()
	if len(db.queue) == 0 {
		db.mu.Unlock()
		return false
	}
	mem := db.queue[0]
	memBytes := mem.Size()
	memLen := mem.Len()
	memRange := keyRange{}
	if len(db.queueRanges) > 0 {
		memRange = db.queueRanges[0]
	}
	var walPaths []string
	if len(db.queueWALPaths) > 0 {
		walPaths = db.queueWALPaths[0]
	}
	var ptrs *largePtrMap
	if len(db.queueLargePtrs) > 0 {
		ptrs = db.queueLargePtrs[0]
	}
	db.mu.Unlock()

	// Optimization: Skip flush for empty memtables (e.g. from frequent Iterator creation)
	flushStart := time.Time{}
	flushed := false
	if memLen > 0 {
		flushStart = time.Now()

		// Ensure value log is flushed to disk (or at least to OS) so that
		// subsequent reads (via db.valueLogReader) can resolve pointers.
		// This is critical for Copy-on-Flush where we read back what we just wrote.
		if db.memtableValueLogPointers && db.valueLogReader != nil {
			if err := db.flushValueLog(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (pre-flush vlog): %w", err))
				return false
			}
		}

		// Flush 'mem' to backend
		backendBatch := db.backend.NewBatch()
		iter := mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator

		ptrLookups := ptrs != nil && db.valueLogEnabled()

		// For larger memtables, bulk-load ops into the backend batch to reduce per-op overhead.
		if mem.Len() > 2000 {
			ops := make([]batch.Entry, 0, mem.Len())
			for iter.Valid() {
				// Zero-copy: UnsafeKey/Value point to memtable nodes (heap).
				// They are valid as long as memtable is reachable.
				// flushOneLocked holds 'mem' reference.
				// backendBatch.SetOps appends Entry structs (shallow copy of slices).
				// backendBatch.Write consumes them.
				// All within flushOneLocked (or until backendBatch.Write returns).
				// So this is safe.

				if iter.IsDeleted() {
					ops = append(ops, batch.Entry{
						Type: batch.OpDelete,
						Key:  iter.UnsafeKey(),
					})
				} else {
					key := iter.UnsafeKey()
					if ptrLookups {
						if ptr, ok := ptrs.GetString(bytesToStringNoCopy(key)); ok {
							ops = append(ops, batch.Entry{
								Type:     batch.OpPut,
								Key:      key,
								ValuePtr: ptr,
								IsPtr:    true,
							})
							iter.Next()
							continue
						}
					}
					val := iter.UnsafeValue()
					if val == nil && db.memtableValueLogPointers {
						db.reportError(fmt.Errorf("cachingdb: flush failed (missing value-log ptr for key)"))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, Value: val})
				}
				iter.Next()
			}
			if err := backendBatch.SetOps(ops); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (setops): %w", err))
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
		} else {
			var ptrSetter interface {
				SetPointer(key []byte, ptr page.ValuePtr) error
			}
			if ptrLookups {
				ptrSetter, _ = backendBatch.(interface {
					SetPointer(key []byte, ptr page.ValuePtr) error
				})
			}
			for iter.Valid() {
				if iter.IsDeleted() {
					if err := backendBatch.Delete(iter.UnsafeKey()); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (delete): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else {
					key := iter.UnsafeKey()
					if ptrLookups {
						if ptr, ok := ptrs.GetString(bytesToStringNoCopy(key)); ok {
							if ptrSetter != nil {
								if err := ptrSetter.SetPointer(key, ptr); err != nil {
									db.reportError(fmt.Errorf("cachingdb: flush failed (set ptr): %w", err))
									_ = iter.Close()
									_ = backendBatch.Close()
									return false
								}
							} else if err := backendBatch.SetOps([]batch.Entry{{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true}}); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (setops ptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
							iter.Next()
							continue
						}
					}

					val := iter.UnsafeValue()
					if val == nil && db.memtableValueLogPointers {
						db.reportError(fmt.Errorf("cachingdb: flush failed (missing value-log ptr for key)"))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
					if err := backendBatch.Set(key, val); err != nil {
						db.reportError(fmt.Errorf("cachingdb: flush failed (set): %w", err))
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				}
				iter.Next()
			}
		}
		_ = iter.Close()
		// Commit to backend
		if db.valueLogEnabled() {
			if err := db.flushValueLog(); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
				_ = backendBatch.Close()
				return false
			}
			if sync && !db.relaxedSync {
				if err := db.syncValueLog(); err != nil {
					db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
					_ = backendBatch.Close()
					return false
				}
			}
		}
		var err error
		if sync {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		if err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
			_ = backendBatch.Close()
			return false
		}
		_ = backendBatch.Close()
		flushed = true
	}
	flushDur := time.Duration(0)
	if flushed {
		flushDur = time.Since(flushStart)
	}

	// Remove from queue and delete old WAL
	db.mu.Lock()
	if memRange.valid {
		db.backendRange.add(memRange.min)
		db.backendRange.add(memRange.max)
	}
	if len(db.queue) > 0 {
		db.queue = db.queue[1:]
	}
	if len(db.queueRanges) > 0 {
		db.queueRanges = db.queueRanges[1:]
	}
	if len(db.queueWALPaths) > 0 {
		db.queueWALPaths = db.queueWALPaths[1:]
	}
	if len(db.queueValueLogPaths) > 0 {
		db.queueValueLogPaths = db.queueValueLogPaths[1:]
	}
	if len(db.queueLargePtrs) > 0 {
		db.queueLargePtrs = db.queueLargePtrs[1:]
	}
	db.queueBacklogBytes.Add(-memBytes)
	db.publishMemtablesLocked()

	deletable := make([]string, 0, len(walPaths))
	if sync {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, walPath := range walPaths {
			if walPath == "" {
				continue
			}
			if _, ok := inUse[walPath]; ok {
				continue
			}
			if _, ok := seen[walPath]; ok {
				continue
			}
			if db.valueLogRetained(walPath) {
				continue
			}
			seen[walPath] = struct{}{}
			deletable = append(deletable, walPath)
		}
	}
	db.mu.Unlock()

	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			// Best effort cleanup
			continue
		}
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()

	if flushed && flushDur > 0 && memBytes > 0 {
		sample := float64(memBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	} else {
		db.bpMu.Lock()
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

func (db *DB) getMemtable(key []byte) ([]byte, bool, error) {
	view := db.memtables.Load()
	var (
		mutables    []memtable.Table
		mutablePtrs []*largePtrMap
		queue       []memtable.Table
		queuePtrs   []*largePtrMap
	)
	if view != nil {
		mutables = view.mutables
		mutablePtrs = view.mutablePtrs
		queue = view.queue
		queuePtrs = view.queuePtrs
	} else {
		// Defensive fallback: should not happen after Open(), but keep safe
		// behavior for zero-value DBs and tests.
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			mutablePtrs = make([]*largePtrMap, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
				mutablePtrs[i] = db.mutableShards[i].largePtrs
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		queuePtrs = append([]*largePtrMap(nil), db.queueLargePtrs...)
		db.mu.RUnlock()
	}

	// check mutable
	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			val, deleted, found := mutables[idx].Get(key)
			if found {
				if deleted {
					return nil, true, nil
				}
				if db.memtableValueLogPointers && db.valueLogReader != nil && len(val) == 0 {
					if idx < len(mutablePtrs) && mutablePtrs[idx] != nil {
						if ptr, ok := mutablePtrs[idx].Get(key); ok {
							readVal, err := db.readValueLog(ptr)
							if err != nil {
								return nil, true, err
							}
							return readVal, true, nil
						}
					}
				}
				if val == nil {
					return []byte{}, true, nil
				}
				return val, true, nil
			}
		}
	}

	// check queue backwards (newest first)
	for i := len(queue) - 1; i >= 0; i-- {
		val, deleted, found := queue[i].Get(key)
		if found {
			if deleted {
				return nil, true, nil
			}
			if db.memtableValueLogPointers && db.valueLogReader != nil && len(val) == 0 {
				if i < len(queuePtrs) && queuePtrs[i] != nil {
					if ptr, ok := queuePtrs[i].Get(key); ok {
						readVal, err := db.readValueLog(ptr)
						if err != nil {
							return nil, true, err
						}
						return readVal, true, nil
					}
				}
			}
			if val == nil {
				return []byte{}, true, nil
			}
			return val, true, nil
		}
	}
	return nil, false, nil
}

// GetUnsafe returns a safe copy of the value.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// Get returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	val, found, err := db.getMemtable(key)
	if err != nil {
		return nil, err
	}
	if found {
		if val == nil {
			return nil, nil
		}
		cpy := make([]byte, len(val))
		copy(cpy, val)
		return cpy, nil
	}
	return db.backend.Get(key)
}

// GetAppend appends the value for the key to dst and returns the new slice.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	// 1. Memtable (Zero Copy)
	val, found, err := db.getMemtable(key)
	if err != nil {
		return dst, err
	}
	if found {
		if val == nil {
			// Found tombstone or empty?
			// getMemtable returns val=nil for deleted.
			// Deleted means "Not Found" effectively for GetAppend?
			// Yes.
			// Wait, getMemtable returns found=true if tombstone OR value exists.
			// If deleted (tombstone), val=nil.
			// So if val==nil && found==true -> Key is Deleted.
			// GetAppend should return ErrKeyNotFound.
			// Wait, tree.ErrKeyNotFound is in tree package.
			// caching package imports tree/tree? No.
			// But db package imports it.
			// We can define ErrKeyNotFound in caching or use a sentinel.
			// Actually caching package doesn't seem to import tree.
			// But DB struct has backend.
			// I should probably import "github.com/snissn/gomap/TreeDB/tree" or define it.
			// Let's assume tree import is fine or check imports.
			// The file header imports internal/memtable etc.
			// I will add the import.
			return dst, tree.ErrKeyNotFound
		}
		return append(dst, val...), nil
	}

	// 2. Backend
	return db.backend.GetAppend(key, dst)
}

func (db *DB) Has(key []byte) (bool, error) {
	view := db.memtables.Load()
	var (
		mutables []memtable.Table
		queue    []memtable.Table
	)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
	} else {
		db.mu.RLock()
		if len(db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(db.mutableShards))
			for i := range db.mutableShards {
				mutables[i] = db.mutableShards[i].mem
			}
		}
		queue = append([]memtable.Table(nil), db.queue...)
		db.mu.RUnlock()
	}

	if len(mutables) > 0 {
		idx := db.shardIndex(key)
		if idx < len(mutables) && mutables[idx] != nil {
			_, deleted, found := mutables[idx].Get(key)
			if found {
				return !deleted, nil
			}
		}
	}

	for i := len(queue) - 1; i >= 0; i-- {
		_, deleted, found := queue[i].Get(key)
		if found {
			return !deleted, nil
		}
	}

	return db.backend.Has(key)
}

func (db *DB) Stats() map[string]string {
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	db.mu.RLock()
	queueLen := len(db.queue)
	flushThreshold := db.flushThreshold
	memtableMode := db.memtableMode
	maxQueued := db.maxQueuedMemtables
	db.mu.RUnlock()
	var walCurrentBytes int64
	var walClosedBytes int64
	for i := range db.lanes {
		l := &db.lanes[i]
		walCurrentBytes += l.walLiveBytes.Load()
		walClosedBytes += l.walClosedBytes.Load()
	}

	stats["treedb.cache.queue_len"] = fmt.Sprintf("%d", queueLen)
	stats["treedb.cache.mutable_bytes"] = fmt.Sprintf("%d", db.mutableBytes.Load())
	stats["treedb.cache.flush_threshold_bytes"] = fmt.Sprintf("%d", flushThreshold)
	stats["treedb.cache.memtable_mode"] = memtableMode.String()
	stats["treedb.cache.max_queued_memtables"] = fmt.Sprintf("%d", maxQueued)
	stats["treedb.cache.wal_bytes_estimate"] = fmt.Sprintf("%d", walClosedBytes+walCurrentBytes)
	stats["treedb.cache.wal_closed_bytes_estimate"] = fmt.Sprintf("%d", walClosedBytes)
	stats["treedb.cache.wal_current_bytes_estimate"] = fmt.Sprintf("%d", walCurrentBytes)
	vlogSegments, vlogBytes := db.valueLogRetainedStats()
	stats["treedb.cache.vlog_retained_segments"] = fmt.Sprintf("%d", vlogSegments)
	stats["treedb.cache.vlog_retained_bytes_estimate"] = fmt.Sprintf("%d", vlogBytes)
	if db.adaptiveBackpressureEnabled() {
		stats["treedb.cache.backpressure_mode"] = "adaptive"
	} else {
		stats["treedb.cache.backpressure_mode"] = "queue_len"
	}
	stats["treedb.cache.queue_backlog_bytes"] = fmt.Sprintf("%d", db.queueBacklogBytes.Load())
	db.bpMu.Lock()
	stats["treedb.cache.flush_bps_ewma"] = fmt.Sprintf("%.0f", db.flushBpsEWMA)
	db.bpMu.Unlock()

	stats["treedb.cache.auto_checkpoint.count"] = fmt.Sprintf("%d", db.autoCheckpointCount.Load())
	stats["treedb.cache.auto_checkpoint.last_reason"] = autoCheckpointReasonString(db.autoCheckpointLastReason.Load())
	stats["treedb.cache.auto_checkpoint.last_duration_ms"] = fmt.Sprintf("%.3f", float64(db.autoCheckpointLastDurNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_before"] = fmt.Sprintf("%d", db.autoCheckpointLastWALBefore.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_after"] = fmt.Sprintf("%d", db.autoCheckpointLastWALAfter.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_reclaimable_before"] = fmt.Sprintf("%d", db.autoCheckpointLastWALReclaimableBefore.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_reclaimable_after"] = fmt.Sprintf("%d", db.autoCheckpointLastWALReclaimableAfter.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_trimmed"] = fmt.Sprintf("%d", db.autoCheckpointLastWALTrimmed.Load())
	stats["treedb.cache.auto_checkpoint.last_unix_nano"] = fmt.Sprintf("%d", db.autoCheckpointLastUnixNano.Load())
	return stats
}

// TriggerFlush schedules a background flush pass (best-effort).
func (db *DB) TriggerFlush() {
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}

// QueueBacklogBytes returns the current queued memtable backlog in bytes.
func (db *DB) QueueBacklogBytes() int64 {
	return db.queueBacklogBytes.Load()
}

// CompactionAssist performs bounded flush work when backpressure triggers. It is
// intended to be called by background maintenance (e.g. slab compaction) so that
// flush debt does not grow unbounded in the absence of foreground writes.
func (db *DB) CompactionAssist() {
	// Ensure the background flusher is scheduled even if this call ends up doing
	// no synchronous work (e.g. due to low backlog).
	db.TriggerFlush()

	// Adaptive policy: thresholds based on queued backlog bytes.
	if db.adaptiveBackpressureEnabled() {
		db.bpMu.Lock()
		slowdownBytes, stopBytes, _ := db.thresholdsLocked()
		db.bpMu.Unlock()

		backlog := db.queueBacklogBytes.Load()
		if stopBytes > 0 && backlog >= stopBytes {
			db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		if slowdownBytes > 0 && backlog > slowdownBytes {
			db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
			return
		}
		return
	}

	// Legacy policy: thresholds based on queue length.
	if db.maxQueuedMemtables >= 0 {
		db.mu.RLock()
		needs := len(db.queue) > db.maxQueuedMemtables
		db.mu.RUnlock()
		if needs {
			db.flushSome(false, db.writerFlushMaxMemtables, db.writerFlushMaxDuration)
		}
	}
}

func (db *DB) Print() error {
	return db.backend.Print()
}

// Drain flushes all currently buffered writes (mutable + queued memtables) to the
// backend. It is intended for maintenance operations that require a fully
// materialized backend state (e.g. index vacuum).
//
// Drain does not provide mutual exclusion against concurrent writers; callers
// should ensure no writes occur concurrently if they require a fully drained
// state.
func (db *DB) Drain() error {
	db.writeMu.Lock()
	db.mu.Lock()
	if db.mutableBytes.Load() > 0 {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.mu.Unlock()
	db.writeMu.Unlock()

	db.flushAll(false)
	return nil
}

// Iterator implements DB.Iterator
func (db *DB) Iterator(start, end []byte) (merging.Iterator, error) {
	if err := db.ensureBackendRange(); err != nil {
		return nil, err
	}

	db.mu.Lock()
	db.noteIterator(start, end)

	// Snapshot Isolation:
	// To ensure the iterator sees a consistent point-in-time view, we rotate the
	// mutable memtable into the immutable queue. The iterator then consumes
	// only the queue and the backend. Any subsequent writes will go to a new
	// mutable memtable which this iterator ignores.
	if db.mutableBytes.Load() > 0 {
		// Rotating is required for snapshot semantics, but allocating a large arena
		// for the *new* mutable memtable is often wasted (iterator-heavy paths may
		// not write concurrently). Use a small initial capacity and allow it to grow
		// if/when writes resume.
		if err := db.rotateMemtableLockedForIterator(minMemtablePrealloc); err != nil {
			db.mu.Unlock()
			return nil, err
		}
	}

	backendRangeKnown := db.backendRangeKnown
	backendRange := db.backendRange

	db.mu.Unlock()

	view := db.memtables.Load()
	var queue []memtable.Table
	var queueRanges []keyRange
	var queuePtrs []*largePtrMap
	if view != nil {
		queue = view.queue
		queueRanges = view.queueRanges
		queuePtrs = view.queuePtrs
	} else {
		// Defensive fallback: should not happen after Open(), but keeps Iterator safe
		// for zero-value DBs and tests.
		db.mu.RLock()
		queue = append([]memtable.Table(nil), db.queue...)
		queueRanges = append([]keyRange(nil), db.queueRanges...)
		queuePtrs = append([]*largePtrMap(nil), db.queueLargePtrs...)
		db.mu.RUnlock()
	}
	queueLen := len(queue)

	// Fast path for full scans: if the in-memory key ranges are disjoint from the
	// backend key range, we can concatenate iterators instead of merging.
	if start == nil && end == nil {
		// Only do this when the queue is empty; queued memtables imply the backend
		// might not yet include older keys, making disjoint-range checks unreliable.
		if backendRangeKnown && len(queue) == 0 && backendRange.valid {
			diskIter, err := db.backend.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}

			if iteratorDebugEnabled.Load() {
				return &debugIterator{Iterator: diskIter, queueLen: queueLen, sourcesUsed: 1}, nil
			}
			return diskIter, nil
		}
	}

	var sources []merging.IteratorSource

	// Priority 0..N: Queue (Newest first)
	// Note: We skip mutable shards because we just rotated them (so they're empty) or they were already empty.
	prio := 0
	for i := len(queue) - 1; i >= 0; i-- {
		if i < len(queueRanges) && !overlapsQuery(start, end, queueRanges[i]) {
			prio++
			continue
		}
		qIter := queue[i].NewIterator(start, end)
		if db.memtableValueLogPointers && db.valueLogReader != nil && i < len(queuePtrs) && queuePtrs[i] != nil {
			qIter = newValueLogIterator(qIter, queuePtrs[i], db.readValueLog)
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     qIter,
			Priority: prio,
		})
		prio++
	}

	// Disk Iterator
	// Only skip if we definitively know the range and it doesn't overlap.
	if !backendRangeKnown || overlapsQuery(start, end, backendRange) {
		diskIter, err := db.backend.Iterator(start, end)
		if err != nil {
			return nil, err
		}

		sources = append(sources, merging.IteratorSource{
			Iter:     diskIter,
			Priority: prio,
		})
	}

	if len(sources) == 0 {
		out := merging.Iterator(&emptyIterator{start: start, end: end})
		if iteratorDebugEnabled.Load() {
			out = &debugIterator{Iterator: out, queueLen: queueLen, sourcesUsed: 0}
		}
		return out, nil
	}

	if len(sources) == 1 {
		out := newSingleSourceIterator(sources[0].Iter, start, end)
		if iteratorDebugEnabled.Load() {
			out = &debugIterator{Iterator: out, queueLen: queueLen, sourcesUsed: 1}
		}
		return out, nil
	}

	out := merging.NewMergingIterator(sources, start, end)
	if iteratorDebugEnabled.Load() {
		out = &debugIterator{Iterator: out, queueLen: queueLen, sourcesUsed: len(sources)}
	}
	return out, nil
}

type debugIterator struct {
	merging.Iterator
	queueLen    int
	sourcesUsed int
}

func (it *debugIterator) DebugStats() (queueLen int, sourcesUsed int) {
	return it.queueLen, it.sourcesUsed
}

type concatUnsafeIterator struct {
	first  iterator.UnsafeIterator
	second iterator.UnsafeIterator

	cur        iterator.UnsafeIterator
	usingFirst bool
	valid      bool
	err        error
}

func newConcatUnsafeIterator(first, second iterator.UnsafeIterator) merging.Iterator {
	it := &concatUnsafeIterator{
		first:      first,
		second:     second,
		cur:        first,
		usingFirst: true,
	}
	it.advance()
	return it
}

func (it *concatUnsafeIterator) advance() {
	it.valid = false

	for {
		if it.cur == nil {
			return
		}

		// Switch to second iterator when first is exhausted.
		if !it.cur.Valid() {
			if it.usingFirst {
				it.cur = it.second
				it.usingFirst = false
				continue
			}
			return
		}

		if it.cur.IsDeleted() {
			it.cur.Next()
			continue
		}

		it.valid = true
		return
	}
}

func (it *concatUnsafeIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.cur.Next()
	it.advance()
}

func (it *concatUnsafeIterator) Valid() bool { return it.valid }

func (it *concatUnsafeIterator) Key() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Key()
}

func (it *concatUnsafeIterator) Value() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.Value()
}

func (it *concatUnsafeIterator) KeyCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.KeyCopy(dst)
}

func (it *concatUnsafeIterator) ValueCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.ValueCopy(dst)
}

func (it *concatUnsafeIterator) Close() error {
	var firstErr error
	if it.first != nil {
		if err := it.first.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if it.second != nil {
		if err := it.second.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (it *concatUnsafeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.first != nil && it.first.Error() != nil {
		return it.first.Error()
	}
	if it.second != nil && it.second.Error() != nil {
		return it.second.Error()
	}
	return nil
}

func (it *concatUnsafeIterator) Domain() (start, end []byte) { return nil, nil }

func (db *DB) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	// Flush everything to backend to simplify reverse iteration
	db.writeMu.Lock()
	db.mu.Lock()
	db.noteIterator(start, end)
	if db.mutableBytes.Load() > 0 {
		_ = db.rotateMemtableLockedWithCapacity(true, minMemtablePrealloc) // Flush to backend
	}
	db.mu.Unlock()
	db.writeMu.Unlock()
	db.flushAll(false)

	return db.backend.ReverseIterator(start, end)
}

// NewBatch implementation for CachingDB
// batchOp removed, using batch.Entry directly

type Batch struct {
	db           *DB
	entries      []batch.Entry
	backend      batch.Interface
	size         int
	walBuf       []logRecord
	shardIdxs    []int
	shardAdds    []int64
	shardCnts    []int
	shardEntries [][]batch.Entry

	closed         bool
	streamEligible bool
	streamTried    bool
	firstKey       []byte
	lastKey        []byte
	batchRange     keyRange
	dictID         uint64
	dictIDValid    bool
	dictBytes      []byte
	dictBytesValid bool
}

func (db *DB) NewBatch() *Batch {
	return &Batch{db: db, entries: make([]batch.Entry, 0, 16), streamEligible: true}
}

func (db *DB) NewBatchWithSize(size int) *Batch {
	return &Batch{db: db, entries: make([]batch.Entry, 0, size), streamEligible: true}
}

// Reset clears the batch for reuse without closing it.
//
// This intentionally keeps internal buffers to avoid per-batch allocations in
// callers that frequently reset (e.g. geth benchmarks).
func (b *Batch) Reset() {
	if b == nil {
		return
	}
	if b.backend != nil {
		_ = b.backend.Close()
		b.backend = nil
	}
	if b.entries != nil {
		b.entries = b.entries[:0]
	}
	if b.shardIdxs != nil {
		b.shardIdxs = b.shardIdxs[:0]
	}
	if b.shardAdds != nil {
		b.shardAdds = b.shardAdds[:0]
	}
	if b.shardCnts != nil {
		b.shardCnts = b.shardCnts[:0]
	}
	if b.shardEntries != nil {
		b.shardEntries = b.shardEntries[:0]
	}
	b.size = 0
	b.walBuf = b.walBuf[:0]
	b.streamEligible = true
	b.streamTried = false
	b.firstKey = nil
	b.lastKey = nil
	b.batchRange = keyRange{}
	b.dictID = 0
	b.dictIDValid = false
	b.dictBytes = nil
	b.dictBytesValid = false
}

func (b *Batch) Set(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	keyCopy := append([]byte(nil), key...)
	valCopy := append([]byte(nil), value...)
	if b.backend != nil {
		b.batchRange.add(keyCopy)
		b.size += len(keyCopy) + len(valCopy)
		// Use the backend's view method with owned copies to avoid aliasing.
		if sv, ok := b.backend.(interface{ SetView(key, value []byte) error }); ok {
			return sv.SetView(keyCopy, valCopy)
		}
		return b.backend.Set(keyCopy, valCopy)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = keyCopy
			b.lastKey = keyCopy
		} else {
			if bytes.Compare(keyCopy, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = keyCopy
		}
	}
	// We don't know about slabs/thresholds here, so we just store inline.
	// Backend will handle promotion to slab if needed during writeBypass,
	// or standard write will handle it via WAL/Memtable (which don't use slabs yet).
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   keyCopy,
		Value: valCopy,
	})
	b.size += len(keyCopy) + len(valCopy)

	b.maybeSwitchToStreaming()
	return nil
}

// SetView records a Put without copying key/value bytes. Callers must treat
// key/value as immutable until the batch is written or closed.
func (b *Batch) SetView(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	if b.backend != nil {
		b.batchRange.add(key)
		b.size += len(key) + len(value)
		if sv, ok := b.backend.(interface{ SetView(key, value []byte) error }); ok {
			return sv.SetView(key, value)
		}
		return b.backend.Set(key, value)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = key
			b.lastKey = key
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = key
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   key,
		Value: value,
	})
	b.size += len(key) + len(value)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) Delete(key []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	keyCopy := append([]byte(nil), key...)
	if b.backend != nil {
		b.batchRange.add(keyCopy)
		b.size += len(keyCopy)
		if dv, ok := b.backend.(interface{ DeleteView(key []byte) error }); ok {
			return dv.DeleteView(keyCopy)
		}
		return b.backend.Delete(keyCopy)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = keyCopy
			b.lastKey = keyCopy
		} else {
			if bytes.Compare(keyCopy, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = keyCopy
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  keyCopy,
	})
	b.size += len(keyCopy)

	b.maybeSwitchToStreaming()
	return nil
}

// DeleteView records a Delete without copying key bytes. Callers must treat
// key as immutable until the batch is written or closed.
func (b *Batch) DeleteView(key []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	if b.backend != nil {
		b.batchRange.add(key)
		b.size += len(key)
		if dv, ok := b.backend.(interface{ DeleteView(key []byte) error }); ok {
			return dv.DeleteView(key)
		}
		return b.backend.Delete(key)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = key
			b.lastKey = key
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = key
		}
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  key,
	})
	b.size += len(key)

	b.maybeSwitchToStreaming()
	return nil
}

func (b *Batch) SetOps(ops []batch.Entry) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		copied := make([]batch.Entry, len(ops))
		for i, op := range ops {
			copiedOp := op
			copiedOp.Key = append([]byte(nil), op.Key...)
			if op.Value != nil {
				copiedOp.Value = append([]byte(nil), op.Value...)
			}
			copied[i] = copiedOp
			b.size += len(copiedOp.Key) + len(copiedOp.Value)
			b.batchRange.add(copiedOp.Key)
		}
		return b.backend.SetOps(copied)
	}
	for _, op := range ops {
		copied := op
		copied.Key = append([]byte(nil), op.Key...)
		if op.Value != nil {
			copied.Value = append([]byte(nil), op.Value...)
		}
		b.entries = append(b.entries, copied)
		b.size += len(copied.Key) + len(copied.Value)
	}
	return nil
}

func (b *Batch) maybeSwitchToStreaming() {
	if b.streamTried || !b.streamEligible || b.backend != nil {
		return
	}
	// Streaming writes directly to the backend batch and therefore bypasses the
	// journal/value-log orchestration. Only enable it when the journal is
	// disabled and value-log pointers are disabled.
	if !b.db.disableJournal || b.db.valueLogEnabled() {
		return
	}
	// Switch to streaming (direct-to-backend batch) once a batch is "big enough"
	// that keeping all entries in memory provides little benefit.
	//
	// Why this exists:
	// - The cached/memtable path is great for small/random batches because it
	//   aggregates updates and reduces backend write amplification.
	// - For large strictly-increasing batches that start beyond the max key in
	//   the in-memory layers, we don't benefit from memtable aggregation; we just
	//   pay extra overhead storing the batch entries slice until Write().
	//
	// We intentionally use a small threshold (rather than flushThreshold) so
	// "BatchWrite1M" style workloads can switch early and avoid materializing the
	// entire batch in memory before Write().
	const streamSwitchMinEntries = 4096
	const streamSwitchMinBytes = 1 << 20 // 1MiB
	if len(b.entries) < streamSwitchMinEntries && b.size < streamSwitchMinBytes {
		return
	}

	// Only attempt streaming if the batch is strictly increasing and starts beyond
	// the maximum key present in the in-memory layers.
	b.db.mu.RLock()
	queueRanges := append([]keyRange(nil), b.db.queueRanges...)
	b.db.mu.RUnlock()

	var maxKey []byte
	mutableRange := b.db.snapshotMutableRange()
	if mutableRange.valid {
		maxKey = mutableRange.max
	}
	for _, r := range queueRanges {
		if !r.valid {
			continue
		}
		if maxKey == nil || bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}

	b.streamTried = true
	if maxKey != nil && bytes.Compare(b.firstKey, maxKey) <= 0 {
		return
	}

	backendBatch := b.db.backend.NewBatch()
	if b.firstKey != nil && b.lastKey != nil {
		// Streaming is strictly increasing and starts beyond the max key in memory,
		// so the batch range is simply [first,last]. Keep copies for backend range
		// tracking after commit.
		b.batchRange.valid = true
		b.batchRange.min = append([]byte(nil), b.firstKey...)
		b.batchRange.max = append([]byte(nil), b.lastKey...)
	}
	if err := backendBatch.SetOps(b.entries); err != nil {
		_ = backendBatch.Close()
		return
	}
	b.backend = backendBatch
	b.entries = b.entries[:0]
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) freezeDictID(ctx context.Context) error {
	if b.dictIDValid {
		return nil
	}
	if b.db == nil {
		return nil
	}
	dictID, err := b.db.currentDictID(ctx)
	if err != nil {
		return err
	}
	b.dictID = dictID
	b.dictIDValid = true
	return nil
}

func (b *Batch) ensureDictBytes(ctx context.Context) ([]byte, error) {
	if b.dictBytesValid {
		return b.dictBytes, nil
	}
	if b.db == nil {
		return nil, nil
	}
	dictBytes, err := b.db.dictBytes(ctx, b.dictID)
	if err != nil {
		return nil, err
	}
	b.dictBytes = dictBytes
	b.dictBytesValid = true
	return dictBytes, nil
}

func (b *Batch) write(sync bool) error {
	if b.closed {
		return ErrBatchClosed
	}
	b.db.waitForCheckpoint()
	b.db.waitForStop()

	if b.backend != nil {
		var err error
		if sync && !b.db.relaxedSync {
			b.db.flushMu.Lock()
			err = b.backend.WriteSync()
			b.db.flushMu.Unlock()
		} else {
			b.db.flushMu.Lock()
			err = b.backend.Write()
			b.db.flushMu.Unlock()
		}
		if cerr := b.backend.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err == nil && b.batchRange.valid {
			b.db.mu.Lock()
			b.db.backendRange.add(b.batchRange.min)
			b.db.backendRange.add(b.batchRange.max)
			b.db.mu.Unlock()
		}
		b.backend = nil
		if err == nil && b.size > 0 {
			b.db.noteWrite()
		}
		b.Reset()
		return err
	}

	// Optimization: Bypass for Large Batches
	// Generalization: Only bypass if the batch is large enough to be comparable
	// to a memtable flush. Small/Medium random batches cause high write amplification
	// if written directly to the COW backend.
	if b.size >= int(b.db.flushThreshold) {
		return b.writeBypass(sync)
	}
	return b.writeRegular(sync)
}

func (b *Batch) writeRegular(sync bool) error {
	b.db.writeMu.RLock()
	needRotate := false
	needSyncBarrier := false
	hasPtr := false

	// 1. Memtable capacity pre-check
	shardCount := len(b.db.mutableShards)
	shardAdds := b.shardAdds
	if cap(shardAdds) < shardCount {
		shardAdds = make([]int64, shardCount)
	} else {
		shardAdds = shardAdds[:shardCount]
		clear(shardAdds)
	}
	b.shardAdds = shardAdds

	shardCounts := b.shardCnts
	if cap(shardCounts) < shardCount {
		shardCounts = make([]int, shardCount)
	} else {
		shardCounts = shardCounts[:shardCount]
		clear(shardCounts)
	}
	b.shardCnts = shardCounts

	shardIdxs := b.shardIdxs
	if cap(shardIdxs) < len(b.entries) {
		shardIdxs = make([]int, len(b.entries))
	} else {
		shardIdxs = shardIdxs[:len(b.entries)]
	}
	b.shardIdxs = shardIdxs
	for i := range b.entries {
		op := &b.entries[i]
		idx := b.db.shardIndex(op.Key)
		shardIdxs[i] = idx
		shardCounts[idx]++
		add := int64(len(op.Key))
		if op.Type == batch.OpPut {
			add += int64(len(op.Value))
		}
		shardAdds[idx] += add
	}
	for i, add := range shardAdds {
		if add == 0 {
			continue
		}
		shard := &b.db.mutableShards[i]
		shard.mu.Lock()
		over := b.db.shardExceedsLimit(shard, add)
		shard.mu.Unlock()
		if over {
			b.db.writeMu.RUnlock()
			return ErrMemtableFull
		}
	}

	// 2. Optional value-log + journal append loop.
	//
	// The value log (value store) and journal (redo log) are decoupled:
	// - When DisableJournal=true, we still append large values to the value log
	//   and store pointers in memory; there is simply no redo log for crash replay.
	// - When DisableJournal=false, we also append commit-intent records that can
	//   be replayed to recover unflushed writes.
	durability := journalDurabilityNone
	if sync {
		if b.db.relaxedSync {
			durability = journalDurabilityFlush
		} else {
			durability = journalDurabilitySync
		}
	}
	debugPtr := b.db.debugFlushPointers
	valueLogEnabled := b.db.valueLogEnabled()

	eligibleIdxs := make([]int, 0, len(b.entries))
	if valueLogEnabled || debugPtr {
		for i := range b.entries {
			op := &b.entries[i]
			if op.Type != batch.OpPut || len(op.Value) <= b.db.valueLogThreshold {
				continue
			}
			eligibleIdxs = append(eligibleIdxs, i)
		}
	}
	eligibleCount := len(eligibleIdxs)
	allowPointers := eligibleCount > 0 && valueLogEnabled && b.db.allowValueLogPointers()
	if debugPtr && eligibleCount > 0 {
		b.db.debugPtrEligible.Add(int64(eligibleCount))
		if !valueLogEnabled {
			b.db.debugPtrDisabled.Add(int64(eligibleCount))
		} else if !allowPointers {
			b.db.debugPtrDenied.Add(int64(eligibleCount))
		}
	}

	var (
		lane *lane
		rids []uint64
	)
	if allowPointers || !b.db.disableJournal {
		l, err := b.db.pickLane(durability == journalDurabilitySync)
		if err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		lane = l
		if durability == journalDurabilitySync {
			defer b.db.releaseLaneSync(lane)
		}
		if !b.db.disableJournal {
			rids = make([]uint64, len(b.entries))
		}
	}

	if allowPointers && eligibleCount > 0 {
		if err := b.freezeDictID(context.Background()); err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		valueRecords := make([]valuelog.Record, eligibleCount)
		for i, idx := range eligibleIdxs {
			op := &b.entries[idx]
			rid := b.db.nextRID.Add(1)
			if rids != nil {
				rids[idx] = rid
			}
			valueRecords[i] = valuelog.Record{RID: rid, Value: op.Value}
		}
		dictBytes, err := b.ensureDictBytes(context.Background())
		if err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		ptrs, err := b.db.appendValueLog(lane, b.dictID, dictBytes, valueRecords, durability)
		if err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		if len(ptrs) == eligibleCount {
			for i, idx := range eligibleIdxs {
				op := &b.entries[idx]
				op.ValuePtr = ptrs[i]
				op.IsPtr = true
				if debugPtr {
					b.db.debugPtrUsed.Add(1)
				}
				hasPtr = true
			}
			retainPath := b.db.currentValueLogPath(lane)
			if retainPath != "" {
				b.db.markValueLogRetain(retainPath)
			}
		} else if debugPtr && eligibleCount > 0 {
			b.db.debugPtrNoPtr.Add(int64(eligibleCount))
		}
	}

	if !b.db.disableJournal {
		records := b.walBuf[:0]
		if cap(records) < len(b.entries) {
			records = make([]logRecord, 0, len(b.entries))
		}
		for i := range b.entries {
			op := &b.entries[i]
			switch op.Type {
			case batch.OpDelete:
				records = append(records, logRecord{Op: logOpDelete, Key: op.Key})
			case batch.OpPut:
				if rids != nil && rids[i] != 0 {
					records = append(records, logRecord{Op: logOpSetRID, Key: op.Key, RID: rids[i]})
				} else {
					records = append(records, logRecord{Op: logOpSetInline, Key: op.Key, Value: op.Value})
				}
			}
		}
		b.walBuf = records
		if err := b.db.appendWAL(lane, records, durability); err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
	}

	shardEntries := b.shardEntries
	if cap(shardEntries) < shardCount {
		shardEntries = make([][]batch.Entry, shardCount)
	} else {
		shardEntries = shardEntries[:shardCount]
		for i := range shardEntries {
			shardEntries[i] = shardEntries[i][:0]
		}
	}
	b.shardEntries = shardEntries
	for i, count := range shardCounts {
		if count > 0 {
			entries := shardEntries[i]
			if cap(entries) < count {
				entries = make([]batch.Entry, 0, count)
			} else {
				entries = entries[:0]
			}
			shardEntries[i] = entries
		}
	}
	for i, op := range b.entries {
		idx := shardIdxs[i]
		shardEntries[idx] = append(shardEntries[idx], op)
	}

	// 3. Memtable Update

	for i := range shardEntries {
		entries := shardEntries[i]
		if len(entries) == 0 {
			continue
		}
		shard := &b.db.mutableShards[i]
		shard.mu.Lock()
		useStream := b.streamEligible
		if useStream && b.db.memtableValueLogPointers && hasPtr {
			useStream = false
		}
		if useStream {
			if applier, ok := shard.mem.(memtable.SortedBatchApplier); ok {
				applier.ApplyStealSortedBatch(entries, func(key []byte) {
					shard.rng.add(key)
					b.db.noteWriteKey(key)
				})
			} else {
				for _, op := range entries {
					if op.Type == batch.OpDelete {
						shard.mem.DeleteSteal(op.Key)
					} else {
						if b.db.memtableValueLogPointers && op.IsPtr {
							shard.mem.SetSteal(op.Key, nil)
						} else {
							shard.mem.SetSteal(op.Key, op.Value)
						}
					}
					shard.rng.add(op.Key)
					b.db.noteWriteKey(op.Key)
				}
			}
		} else {
			for _, op := range entries {
				if op.Type == batch.OpDelete {
					shard.mem.DeleteSteal(op.Key)
				} else {
					if b.db.memtableValueLogPointers && op.IsPtr {
						shard.mem.SetSteal(op.Key, nil)
					} else {
						shard.mem.SetSteal(op.Key, op.Value)
					}
				}
				shard.rng.add(op.Key)
				b.db.noteWriteKey(op.Key)
			}
		}
		if b.db.valueLogEnabled() && shard.largePtrs != nil {
			for _, op := range entries {
				key := bytesToStringNoCopy(op.Key)
				if op.Type == batch.OpDelete {
					shard.largePtrs.DeleteString(key)
					continue
				}
				if op.IsPtr {
					shard.largePtrs.SetString(key, op.ValuePtr)
				} else {
					shard.largePtrs.DeleteString(key)
				}
			}
		}
		newBytes := shard.mem.Size()
		delta := newBytes - shard.bytes
		shard.bytes = newBytes
		b.db.mutableBytes.Add(delta)
		shard.mu.Unlock()
	}

	// 3. Threshold Check
	if b.db.mutableBytes.Load() > b.db.mutableFlushThreshold() {
		needRotate = true
	}
	if sync && b.db.disableJournal {
		needSyncBarrier = true
	}
	b.db.writeMu.RUnlock()

	if needRotate {
		if err := b.db.maybeRotateMemtable(true); err != nil {
			return err
		}
	}
	if needSyncBarrier {
		if err := b.db.syncBarrierAfterWrite(true); err != nil {
			return err
		}
	}

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.db.maybeAssistFlush()

	b.Reset()
	return nil
}

type logSegmentInfo struct {
	path     string
	size     int64
	seq      int
	lane     int
	valueLog bool
}

func listNonEmptyLogSegments(walDir string) (segments []logSegmentInfo, nonEmptyBytes int64) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, 0
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		lane, seq, valueLog, ok := parseLogSeq(name)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(walDir, name)
		segments = append(segments, logSegmentInfo{path: path, size: info.Size(), seq: seq, lane: lane, valueLog: valueLog})
		if info.Size() > 0 {
			nonEmptyBytes += info.Size()
		}
	}
	return segments, nonEmptyBytes
}

func parseLogSeq(name string) (int, int, bool, bool) {
	const (
		commitPrefix = "commit-"
		valuePrefix  = "value-"
		walPrefix    = "wal-"
		vlogPrefix   = "vlog-"
	)
	if filepath.Ext(name) != ".log" {
		return 0, 0, false, false
	}
	base := strings.TrimSuffix(name, ".log")

	parseLaneSeq := func(rest string) (int, int, bool) {
		parts := strings.SplitN(rest, "-", 2)
		if len(parts) != 2 {
			return 0, 0, false
		}
		lane, err := strconv.Atoi(parts[0])
		if err != nil || lane < 0 {
			return 0, 0, false
		}
		seq, err := strconv.Atoi(parts[1])
		if err != nil || seq < 0 {
			return 0, 0, false
		}
		return lane, seq, true
	}

	if strings.HasPrefix(base, "commit-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "commit-l"))
		return lane, seq, false, ok
	}
	if strings.HasPrefix(base, "value-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "value-l"))
		return lane, seq, true, ok
	}
	if strings.HasPrefix(base, commitPrefix) {
		core := strings.TrimPrefix(base, commitPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, valuePrefix) {
		core := strings.TrimPrefix(base, valuePrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	if strings.HasPrefix(base, walPrefix) {
		core := strings.TrimPrefix(base, walPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, vlogPrefix) {
		core := strings.TrimPrefix(base, vlogPrefix)
		if core == "" {
			return 0, 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	return 0, 0, false, false
}

func (b *Batch) writeBypass(sync bool) error {
	// Fast path: if none of these keys exist in mutable/queue, we can write directly
	// to the backend without flushing (no in-memory shadowing possible).
	// Cheap append-only check: if the batch key range does not overlap with any
	// in-memory key ranges, it cannot be shadowed.
	batchRange := keyRange{}
	for _, op := range b.entries {
		key := op.Key
		batchRange.add(key)
	}

	mutableRange := b.db.snapshotMutableRange()

	var (
		mutables    []memtable.Table
		queue       []memtable.Table
		queueRanges []keyRange
		overlaps    bool
	)

	b.db.mu.RLock()
	view := b.db.memtables.Load()
	overlaps = rangesOverlap(batchRange, mutableRange)
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		queueRanges = view.queueRanges
	} else {
		// Defensive fallback: should not happen after Open(), but keep safe
		// behavior for zero-value DBs and tests.
		if len(b.db.mutableShards) > 0 {
			mutables = make([]memtable.Table, len(b.db.mutableShards))
			for i := range b.db.mutableShards {
				mutables[i] = b.db.mutableShards[i].mem
			}
		}
		if len(b.db.queue) > 0 {
			queue = append([]memtable.Table(nil), b.db.queue...)
		}
		if len(b.db.queueRanges) > 0 {
			queueRanges = append([]keyRange(nil), b.db.queueRanges...)
		}
	}
	b.db.mu.RUnlock()

	if !overlaps {
		if len(queueRanges) == 0 && len(queue) > 0 {
			overlaps = true
		} else {
			for _, r := range queueRanges {
				if rangesOverlap(batchRange, r) {
					overlaps = true
					break
				}
			}
		}
	}

	if overlaps {
		// Slow path: verify no individual key exists in memory (handles sparse overlap).
		for _, op := range b.entries {
			key := op.Key
			if len(mutables) > 0 {
				idx := b.db.shardIndex(key)
				if idx < len(mutables) && mutables[idx] != nil {
					if _, _, found := mutables[idx].Get(key); found {
						return b.writeRegular(sync)
					}
				}
			}
			for i := len(queue) - 1; i >= 0; i-- {
				if _, _, found := queue[i].Get(key); found {
					return b.writeRegular(sync)
				}
			}
		}
	}

	// Write directly to backend
	backendBatch := b.db.backend.NewBatch()

	// Use SetOps for bulk transfer (checking slabs internally in backend)
	if err := backendBatch.SetOps(b.entries); err != nil {
		return err
	}

	var err error
	if sync && !b.db.relaxedSync {
		b.db.flushMu.Lock()
		err = backendBatch.WriteSync()
		b.db.flushMu.Unlock()
	} else {
		b.db.flushMu.Lock()
		err = backendBatch.Write()
		b.db.flushMu.Unlock()
	}

	if err != nil {
		return err
	}

	b.db.mu.Lock()
	if batchRange.valid {
		b.db.backendRange.add(batchRange.min)
		b.db.backendRange.add(batchRange.max)
	}
	b.db.mu.Unlock()

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.Reset()
	return nil
}

func (b *Batch) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	b.entries = nil
	if b.backend != nil {
		_ = b.backend.Close()
		b.backend = nil
	}
	b.walBuf = nil
	b.firstKey = nil
	b.lastKey = nil
	return nil
}

func (b *Batch) Replay(fn func(batch.Entry) error) error {
	if b.closed {
		return ErrBatchClosed
	}
	if b.backend != nil {
		return b.backend.Replay(fn)
	}

	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	if b.closed {
		return 0, ErrBatchClosed
	}
	return b.size, nil
}

// singleSourceIterator wraps a single UnsafeIterator to satisfy merging.Iterator,
// skipping tombstones.
type singleSourceIterator struct {
	iter  iterator.UnsafeIterator
	valid bool
	start []byte
	end   []byte
}

func newSingleSourceIterator(iter iterator.UnsafeIterator, start, end []byte) merging.Iterator {
	it := &singleSourceIterator{
		iter:  iter,
		start: start,
		end:   end,
	}
	// Iterator is already sought to start by the caller
	it.advance()
	return it
}

func (it *singleSourceIterator) advance() {
	it.valid = false
	for it.iter.Valid() {
		if it.end != nil && bytes.Compare(it.iter.UnsafeKey(), it.end) >= 0 {
			return
		}
		if it.iter.IsDeleted() {
			it.iter.Next()
			continue
		}
		it.valid = true
		return
	}
}

func (it *singleSourceIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	it.iter.Next()
	it.advance()
}

func (it *singleSourceIterator) Valid() bool               { return it.valid }
func (it *singleSourceIterator) Key() []byte               { return it.iter.Key() }
func (it *singleSourceIterator) Value() []byte             { return it.iter.Value() }
func (it *singleSourceIterator) KeyCopy(dst []byte) []byte { return it.iter.KeyCopy(dst) }
func (it *singleSourceIterator) ValueCopy(dst []byte) []byte {
	return it.iter.ValueCopy(dst)
}
func (it *singleSourceIterator) Close() error             { return it.iter.Close() }
func (it *singleSourceIterator) Error() error             { return it.iter.Error() }
func (it *singleSourceIterator) Domain() ([]byte, []byte) { return it.start, it.end }

// emptyIterator represents an iterator with no elements.
type emptyIterator struct {
	start, end []byte
}

func (it *emptyIterator) Next()                     { panic("iterator invalid") }
func (it *emptyIterator) Valid() bool               { return false }
func (it *emptyIterator) Key() []byte               { panic("iterator invalid") }
func (it *emptyIterator) Value() []byte             { panic("iterator invalid") }
func (it *emptyIterator) KeyCopy(_ []byte) []byte   { panic("iterator invalid") }
func (it *emptyIterator) ValueCopy(_ []byte) []byte { panic("iterator invalid") }
func (it *emptyIterator) Close() error              { return nil }
func (it *emptyIterator) Error() error              { return nil }
func (it *emptyIterator) Domain() ([]byte, []byte)  { return it.start, it.end }
