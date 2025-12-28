package caching

import (
	"bytes"
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
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/vlog"
	"github.com/snissn/gomap/TreeDB/internal/wal"
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
	if dir == "" {
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
	return !db.disableWAL && !db.disableValueLog
}

func (db *DB) readValueLog(ptr page.ValuePtr) ([]byte, error) {
	if db.valueLogReader == nil {
		return nil, errors.New("cachingdb: value-log reader unavailable")
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("cachingdb: non value-log pointer %#x", ptr.FileID)
	}
	if db.memtableValueLogPointers && db.valueLogEnabled() {
		db.mu.RLock()
		curSeq := db.walSeq
		db.mu.RUnlock()
		if page.ValueLogFileID(uint32(curSeq)) == ptr.FileID {
			if err := db.flushValueLog(); err != nil {
				return nil, err
			}
		}
	}
	return db.valueLogReader.Read(ptr)
}

func (db *DB) flushValueLog() error {
	if !db.valueLogEnabled() {
		return nil
	}
	db.mu.RLock()
	w := db.wal
	db.mu.RUnlock()
	if w == nil {
		return errWALUnavailable
	}
	db.walMu.Lock()
	err := w.Flush()
	db.walMu.Unlock()
	return err
}

func (db *DB) logSegmentPrefix() string {
	if db.valueLogEnabled() {
		return "vlog-"
	}
	return "wal-"
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
	seq, valueLog, ok := parseLogSeq(filepath.Base(path))
	if !ok || !valueLog {
		return
	}
	id := page.ValueLogFileID(uint32(seq))
	if err := db.valueLogReader.RemoveSegment(id); err != nil && !os.IsNotExist(err) {
		db.reportError(fmt.Errorf("cachingdb: failed to remove value-log segment %d: %w", id, err))
	}
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

	var closedSizes map[string]int64
	var currentPath string
	var currentBytes int64
	db.mu.RLock()
	if len(db.walClosedSizes) > 0 {
		closedSizes = make(map[string]int64, len(db.walClosedSizes))
		for path, size := range db.walClosedSizes {
			closedSizes[path] = size
		}
	}
	currentPath = db.walPath
	currentBytes = db.walLiveBytes.Load()
	db.mu.RUnlock()

	for _, path := range paths {
		segments++
		if path == currentPath {
			bytes += currentBytes
			continue
		}
		if size, ok := closedSizes[path]; ok {
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
	_, bytes := db.valueLogRetainedStats()
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
	db.mu.RLock()
	if db.walPath != "" {
		inUse[db.walPath] = struct{}{}
	}
	for _, path := range db.queueWALPaths {
		inUse[path] = struct{}{}
	}
	db.mu.RUnlock()

	removed := false
	marked := false
	for _, path := range paths {
		if _, ok := inUse[path]; ok {
			continue
		}
		seq, valueLog, ok := parseLogSeq(filepath.Base(path))
		if !ok || !valueLog {
			continue
		}
		id := page.ValueLogFileID(uint32(seq))
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
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				db.reportError(fmt.Errorf("cachingdb: failed to remove value-log %q: %w", path, err))
				continue
			}
			db.dropValueLogSegment(path)
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

	// DisableWAL disables the Write-Ahead Log.
	DisableWAL bool
	// DisableValueLog forces the cached WAL to remain in legacy mode (no value-log pointers).
	DisableValueLog bool
	// RelaxedSync disables fsync on Sync operations.
	RelaxedSync bool
	// MemtableValueLogPointers avoids storing large values in the memtable and
	// serves them by pointer from the value log (WAL/vlog). Requires WAL/value-log.
	MemtableValueLogPointers bool
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

	// NotifyError is an optional hook for background maintenance failures.
	NotifyError func(error)
}

type DB struct {
	mu      sync.RWMutex
	flushMu sync.Mutex
	writeMu sync.RWMutex
	walMu   sync.Mutex
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
	memtables         atomic.Pointer[memtableView]
	hashSortedIndexer *memtable.HashSortedIndexer
	queueRanges       []keyRange
	queueWALPaths     []string
	queueLargePtrs    []*largePtrMap
	backendRange      keyRange
	backendRangeKnown bool
	backendRangeInit  sync.Once
	backendRangeErr   error

	// Durability
	wal           logWriter
	walPath       string
	walSeq        int // Sequence number for WAL files
	walCh         chan walWriteRequest
	walStopCh     chan struct{}
	walAckMu      sync.Mutex
	walErr        error
	walFastMu     sync.Mutex
	walFastCond   *sync.Cond
	walFastQueue  []walFastItem
	walFastHead   int
	walFastClosed bool
	// walClosedBytes is an in-memory estimate of retained (non-current) WAL
	// segment bytes. It is updated on WAL rotation and segment deletion.
	walClosedBytes atomic.Int64
	// walLiveBytes tracks the current WAL size to avoid lock contention.
	walLiveBytes   atomic.Int64
	walClosedSizes map[string]int64

	disableValueLog              bool
	inlineThreshold              int
	memtableValueLogPointers     bool
	valueLogReader               *vlog.Manager
	valueLogMu                   sync.Mutex
	valueLogRetain               map[string]struct{}
	valueLogWarned               atomic.Bool
	valueLogHardCapWarned        atomic.Bool
	maxValueLogRetainedBytes     int64
	maxValueLogRetainedBytesHard int64

	// Level 1 (Disk)
	backend BackendDB

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

	disableWAL         bool
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
	if !opts.AllowUnsafe && (opts.DisableWAL || opts.RelaxedSync || opts.DisableReadChecksum) {
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
	maxWALSeq := 0
	for _, seg := range segments {
		if seg.seq > maxWALSeq {
			maxWALSeq = seg.seq
		}
	}

	inlineThreshold := page.DefaultInlineThreshold
	if provider, ok := backend.(interface{ InlineThreshold() int }); ok {
		if v := provider.InlineThreshold(); v > 0 {
			inlineThreshold = v
		}
	}
	disableValueLog := opts.DisableValueLog || opts.DisableWAL
	if opts.MemtableValueLogPointers && disableValueLog {
		return nil, ErrMemtableValueLogPointers
	}
	var retained map[string]struct{}
	for _, seg := range segments {
		if !seg.valueLog {
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

	var valueLogReader *vlog.Manager
	if opts.MemtableValueLogPointers {
		reader, err := vlog.NewManager(walDir)
		if err != nil {
			return nil, err
		}
		reader.SetDisableReadChecksum(opts.DisableReadChecksum)
		valueLogReader = reader
	}
	debugFlushPointers := envBool(envDebugFlushPointers)

	db := &DB{
		dir:                          walDir,
		backend:                      backend,
		flushThreshold:               opts.FlushThreshold,
		memtableCap:                  memCap,
		memtableMode:                 mode,
		memtableAdaptive:             adaptive,
		memtableWarmupActive:         adaptive && warmupThreshold < opts.FlushThreshold,
		memtableWarmupThreshold:      warmupThreshold,
		memtableAdaptiveCandidate:    mode,
		maxQueuedMemtables:           opts.MaxQueuedMemtables,
		slowdownBacklogSeconds:       opts.SlowdownBacklogSeconds,
		stopBacklogSeconds:           opts.StopBacklogSeconds,
		maxBacklogBytes:              opts.MaxBacklogBytes,
		writerFlushMaxMemtables:      opts.WriterFlushMaxMemtables,
		writerFlushMaxDuration:       opts.WriterFlushMaxDuration,
		flushBuildConcurrency:        opts.FlushBuildConcurrency,
		disableWAL:                   opts.DisableWAL,
		disableValueLog:              disableValueLog,
		relaxedSync:                  opts.RelaxedSync,
		notifyError:                  opts.NotifyError,
		inlineThreshold:              inlineThreshold,
		memtableValueLogPointers:     opts.MemtableValueLogPointers,
		valueLogReader:               valueLogReader,
		valueLogRetain:               retained,
		debugFlushPointers:           debugFlushPointers,
		maxValueLogRetainedBytes:     opts.MaxValueLogRetainedBytes,
		maxValueLogRetainedBytesHard: opts.MaxValueLogRetainedBytesHard,
		mutableShards:                mutableShards,
		mutableShardMask:             uint64(shardCount - 1),
		hashSortedIndexer:            indexer,
		closeCh:                      make(chan struct{}),
		flushCh:                      make(chan struct{}, 1),
		autoCheckpointOnceCh:         make(chan struct{}, 1),
		autoCheckpointWriteCh:        make(chan struct{}, 1),
		walSeq:                       maxWALSeq,
	}
	db.bpCond = sync.NewCond(&db.bpMu)
	db.checkpointCond = sync.NewCond(&db.checkpointMu)

	// Open initial WAL
	if !db.disableWAL {
		if err := db.rotateWALLocked(); err != nil {
			if db.valueLogReader != nil {
				_ = db.valueLogReader.Close()
				db.valueLogReader = nil
			}
			return nil, err
		}
		if len(segments) > 0 {
			db.mu.Lock()
			if db.walClosedSizes == nil {
				db.walClosedSizes = make(map[string]int64, len(segments))
			}
			for _, seg := range segments {
				if seg.path == db.walPath {
					continue
				}
				db.walClosedSizes[seg.path] = seg.size
				db.walClosedBytes.Add(seg.size)
			}
			db.mu.Unlock()
		}
		db.startWALWriter()
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
	if db.disableWAL {
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
		} else if !db.autoCheckpointSizeArmed.Load() && db.valueLogEnabled() {
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
	return db.walClosedBytes.Load() + db.walLiveBytes.Load()
}

func (db *DB) reclaimableWALBytes() int64 {
	if db == nil {
		return 0
	}
	total := db.effectiveWALBytes()
	if total <= 0 {
		return 0
	}
	if !db.valueLogEnabled() {
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
	walSegmentHeaderBytes = 8
	walRecordHeaderBytes  = 1 + 2 + 4
)

func (db *DB) logRecordSize(key, value []byte) int64 {
	if db.valueLogEnabled() {
		return int64(vlog.HeaderSize + len(key) + len(value))
	}
	return int64(walSegmentHeaderBytes + walRecordHeaderBytes + len(key) + len(value))
}

func (db *DB) logBatchSize(records []logRecord) int64 {
	if len(records) == 0 {
		return 0
	}
	if db.valueLogEnabled() {
		total := 0
		for _, r := range records {
			total += vlog.HeaderSize + len(r.Key) + len(r.Value)
		}
		return int64(total)
	}
	total := walSegmentHeaderBytes
	for _, r := range records {
		total += walRecordHeaderBytes + len(r.Key) + len(r.Value)
	}
	return int64(total)
}

type walWriteRequest struct {
	records []logRecord
	sync    bool
	ack     *walAck
}

type walAck struct {
	wg   sync.WaitGroup
	err  error
	ptrs []page.ValuePtr
}

var walAckPool = sync.Pool{
	New: func() any { return &walAck{} },
}

const maxEntryPoolCap = 1 << 14

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

func (db *DB) startWALWriter() {
	db.walCh = make(chan walWriteRequest, walWriteBuffer)
	db.walStopCh = make(chan struct{})
	db.walFastCond = sync.NewCond(&db.walFastMu)
	db.wg.Add(1)
	go db.walWriteLoop()
	db.wg.Add(1)
	go db.walFastLoop()
}

func (db *DB) walWriteLoop() {
	defer db.wg.Done()
	defer close(db.walStopCh)

	batch := make([]walWriteRequest, 0, walWriteBatchMax)
	for {
		batch = batch[:0]

		var req walWriteRequest
		select {
		case <-db.closeCh:
			db.drainWALWriter(batch)
			return
		case req = <-db.walCh:
		}
		batch = append(batch, req)

	drain:
		for len(batch) < walWriteBatchMax {
			select {
			case req = <-db.walCh:
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

		err := db.flushWALRequests(batch)
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

func (db *DB) walFastLoop() {
	defer db.wg.Done()

	batch := make([]walFastItem, 0, walFastBatchMax)
	records := make([]logRecord, 0, walFastBatchMax)

	for {
		db.walFastMu.Lock()
		for !db.walFastClosed && len(db.walFastQueue)-db.walFastHead == 0 {
			db.walFastCond.Wait()
		}

		if db.walFastClosed {
			batch = append(batch[:0], db.walFastQueue[db.walFastHead:]...)
			db.walFastQueue = nil
			db.walFastHead = 0
			db.walFastMu.Unlock()

			for i := range batch {
				ack := batch[i].ack
				ack.err = errWALClosed
				ack.wg.Done()
			}
			return
		}

		available := len(db.walFastQueue) - db.walFastHead
		n := available
		if n > walFastBatchMax {
			n = walFastBatchMax
		}
		batch = append(batch[:0], db.walFastQueue[db.walFastHead:db.walFastHead+n]...)
		db.walFastHead += n

		if db.walFastHead == len(db.walFastQueue) {
			db.walFastQueue = db.walFastQueue[:0]
			db.walFastHead = 0
		} else if db.walFastHead > 1024 && db.walFastHead*2 >= len(db.walFastQueue) {
			copy(db.walFastQueue, db.walFastQueue[db.walFastHead:])
			db.walFastQueue = db.walFastQueue[:len(db.walFastQueue)-db.walFastHead]
			db.walFastHead = 0
		}
		db.walFastCond.Broadcast()
		db.walFastMu.Unlock()

		records = records[:0]
		for i := range batch {
			records = append(records, batch[i].record)
		}
		ptrs, err := db.appendWALDirect(records, false)
		for i := range batch {
			ack := batch[i].ack
			ack.err = err
			if err == nil && i < len(ptrs) {
				ack.ptrs = append(ack.ptrs[:0], ptrs[i])
			}
			ack.wg.Done()
		}
	}
}

func (db *DB) drainWALWriter(batch []walWriteRequest) {
	for {
		select {
		case req := <-db.walCh:
			batch = append(batch[:0], req)
		drain:
			for len(batch) < walWriteBatchMax {
				select {
				case req = <-db.walCh:
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

			err := db.flushWALRequests(batch)
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

func (db *DB) flushWALRequests(requests []walWriteRequest) error {
	if len(requests) == 0 {
		return nil
	}

	db.mu.RLock()
	w := db.wal
	db.mu.RUnlock()
	if w == nil {
		return errWALUnavailable
	}

	var (
		totalBytes int64
		needSync   bool
	)

	db.walMu.Lock()
	for i := range requests {
		req := &requests[i]
		if len(req.records) == 1 {
			rec := req.records[0]
			ptr, err := w.Append(rec.Op, rec.Key, rec.Value)
			if err != nil {
				db.walMu.Unlock()
				return err
			}
			if req.ack != nil {
				req.ack.ptrs = append(req.ack.ptrs[:0], ptr)
			}
			totalBytes += db.logRecordSize(rec.Key, rec.Value)
		} else {
			ptrs, err := w.AppendBatch(req.records)
			if err != nil {
				db.walMu.Unlock()
				return err
			}
			if req.ack != nil {
				req.ack.ptrs = append(req.ack.ptrs[:0], ptrs...)
			}
			totalBytes += db.logBatchSize(req.records)
		}
		if req.sync {
			needSync = true
		}
	}
	if needSync {
		if err := w.Sync(); err != nil {
			db.walMu.Unlock()
			return err
		}
	}
	db.walMu.Unlock()

	if totalBytes > 0 {
		db.walLiveBytes.Add(totalBytes)
	}
	return nil
}

type walDurability uint8

const (
	walDurabilityNone walDurability = iota
	walDurabilityFlush
	walDurabilitySync
)

func (db *DB) appendWAL(records []logRecord, durability walDurability) ([]page.ValuePtr, error) {
	if db.disableWAL {
		return nil, nil
	}
	if len(records) == 0 {
		return nil, nil
	}
	select {
	case <-db.closeCh:
		return nil, errWALClosed
	default:
	}
	db.walAckMu.Lock()
	if db.walErr != nil {
		err := db.walErr
		db.walAckMu.Unlock()
		return nil, err
	}
	db.walAckMu.Unlock()

	switch durability {
	case walDurabilitySync:
		return db.appendWALDirect(records, true)
	case walDurabilityFlush:
		return db.appendWALInline(records, true)
	default:
		return db.appendWALInline(records, false)
	}
}

func (db *DB) appendWALInline(records []logRecord, flush bool) ([]page.ValuePtr, error) {
	db.mu.RLock()
	w := db.wal
	db.mu.RUnlock()
	if w == nil {
		return nil, errWALUnavailable
	}

	var (
		totalBytes int64
		ptrs       []page.ValuePtr
		err        error
	)

	db.walMu.Lock()
	if len(records) == 1 {
		rec := records[0]
		var ptr page.ValuePtr
		ptr, err = w.Append(rec.Op, rec.Key, rec.Value)
		if err == nil && db.valueLogEnabled() {
			ptrs = []page.ValuePtr{ptr}
		}
		totalBytes = db.logRecordSize(rec.Key, rec.Value)
	} else {
		ptrs, err = w.AppendBatch(records)
		totalBytes = db.logBatchSize(records)
		if !db.valueLogEnabled() {
			ptrs = nil
		}
	}
	if err == nil && flush {
		err = w.Flush()
	}
	db.walMu.Unlock()

	if err != nil {
		db.walAckMu.Lock()
		if db.walErr == nil {
			db.walErr = err
		}
		db.walAckMu.Unlock()
		return nil, err
	}

	if totalBytes > 0 {
		db.walLiveBytes.Add(totalBytes)
	}
	return ptrs, nil
}

func (db *DB) appendWALDirect(records []logRecord, sync bool) ([]page.ValuePtr, error) {
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.ptrs = ack.ptrs[:0]
	ack.wg.Add(1)

	req := walWriteRequest{records: records, sync: sync, ack: ack}
	select {
	case db.walCh <- req:
		// wait for ack
	case <-db.closeCh:
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return nil, errWALClosed
	}

	ack.wg.Wait()
	err := ack.err
	ptrs := append([]page.ValuePtr(nil), ack.ptrs...)
	ack.ptrs = nil
	walAckPool.Put(ack)
	return ptrs, err
}

func (db *DB) appendWALFast(record logRecord) (page.ValuePtr, error) {
	ack := walAckPool.Get().(*walAck)
	ack.err = nil
	ack.ptrs = ack.ptrs[:0]
	ack.wg.Add(1)

	db.walFastMu.Lock()
	for !db.walFastClosed && len(db.walFastQueue)-db.walFastHead >= walFastQueueMax {
		db.walFastCond.Wait()
	}
	if db.walFastClosed {
		db.walFastMu.Unlock()
		ack.err = errWALClosed
		ack.wg.Done()
		walAckPool.Put(ack)
		return page.ValuePtr{}, errWALClosed
	}
	db.walFastQueue = append(db.walFastQueue, walFastItem{record: record, ack: ack})
	db.walFastCond.Signal()
	db.walFastMu.Unlock()

	ack.wg.Wait()
	err := ack.err
	var ptr page.ValuePtr
	if len(ack.ptrs) > 0 {
		ptr = ack.ptrs[0]
	}
	ack.ptrs = nil
	walAckPool.Put(ack)
	return ptr, err
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
	if err := db.rotateWALLocked(); err != nil {
		db.mu.Unlock()
		return err
	}
	walDir := db.dir
	db.mu.Unlock()

	// Flush all queued memtables with backend sync.
	for db.flushCombinedLocked(true) {
	}

	segments, nonEmptyBytes := listNonEmptyLogSegments(walDir)
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

	db.mu.RLock()
	currentWAL := db.walPath
	db.mu.RUnlock()

	removed := false
	for _, seg := range segments {
		path := seg.path
		if path == currentWAL {
			continue
		}
		if db.valueLogRetained(path) {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			db.reportError(fmt.Errorf("cachingdb: failed to remove WAL segment %q: %w", path, err))
			continue
		}
		db.dropValueLogSegment(path)
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

	db.walFastMu.Lock()
	db.walFastClosed = true
	if db.walFastCond != nil {
		db.walFastCond.Broadcast()
	}
	db.walFastMu.Unlock()

	close(db.closeCh)
	db.writeMu.Unlock()
	db.wg.Wait()
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

	db.mu.Lock()
	walBytes = db.walClosedBytes.Load()
	if len(db.walClosedSizes) > 0 {
		walPaths = make([]string, 0, len(db.walClosedSizes)+1)
		for path := range db.walClosedSizes {
			walPaths = append(walPaths, path)
		}
	} else {
		walPaths = make([]string, 0, 1)
	}
	if db.wal != nil {
		walBytes += db.wal.Size()
		db.walLiveBytes.Store(0)
		if db.walPath != "" {
			walPaths = append(walPaths, db.walPath)
		}
		_ = db.wal.Close()
		db.wal = nil
	} else if db.walPath != "" {
		walPaths = append(walPaths, db.walPath)
	}
	db.mu.Unlock()

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
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			db.reportError(fmt.Errorf("cachingdb: failed to remove WAL segment %q: %w", path, err))
			continue
		}
		db.dropValueLogSegment(path)
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
	if !db.disableWAL {
		// WAL durability is handled by appendWAL:
		// - strict: fsync
		// - relaxed: flush-to-kernel (no fsync)
		return nil
	}
	if db.relaxedSync {
		// WAL disabled: enforce a backend flush boundary without fsync.
		return db.flushAllMemtablesForSync(false)
	}
	// WAL disabled: enforce a durable backend boundary.
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

	if !db.disableWAL {
		durability := walDurabilityNone
		if sync {
			if db.relaxedSync {
				durability = walDurabilityFlush
			} else {
				durability = walDurabilitySync
			}
		}
		rec := logRecord{Op: logOpSet, Key: key, Value: value}
		ptrs, err := db.appendWAL([]logRecord{rec}, durability)
		if err != nil {
			db.writeMu.RUnlock()
			return err
		}
		eligible := len(value) > db.inlineThreshold
		if debugPtr && eligible {
			db.debugPtrEligible.Add(1)
		}
		valueLogEnabled := db.valueLogEnabled()
		if valueLogEnabled && len(ptrs) > 0 && eligible {
			if db.allowValueLogPointers() {
				ptr = ptrs[0]
				usePointer = true
				if debugPtr {
					db.debugPtrUsed.Add(1)
				}
				db.mu.RLock()
				retainPath = db.walPath
				db.mu.RUnlock()
			} else if debugPtr {
				db.debugPtrDenied.Add(1)
			}
		} else if debugPtr && eligible {
			if !valueLogEnabled {
				db.debugPtrDisabled.Add(1)
			} else if len(ptrs) == 0 {
				db.debugPtrNoPtr.Add(1)
			}
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
	if sync && db.disableWAL {
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

	// WAL-enabled mode: do a snapshot scan and apply per-key deletes directly.
	// We batch WAL appends with copied keys to reduce per-record overhead.
	if !db.disableWAL {
		db.writeMu.Lock()
		defer db.writeMu.Unlock()

		it, err := db.Iterator(start, end)
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()

		const deleteRangeBatchKeys = 1024
		const deleteRangeBatchBytes = 1 << 20

		var (
			batchKeys []byte
			batch     []logRecord
		)

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

		flushBatch := func() error {
			if len(batch) == 0 {
				return nil
			}
			if _, err := db.appendWAL(batch, walDurabilityNone); err != nil {
				return err
			}
			for i := range batch {
				if err := applyDelete(batch[i].Key); err != nil {
					return err
				}
			}
			batch = batch[:0]
			batchKeys = batchKeys[:0]
			return nil
		}

		for it.Valid() {
			key := it.Key()
			start := len(batchKeys)
			batchKeys = append(batchKeys, key...)
			keyCopy := batchKeys[start:len(batchKeys)]
			batch = append(batch, logRecord{Op: logOpDelete, Key: keyCopy})

			if len(batch) >= deleteRangeBatchKeys || len(batchKeys) >= deleteRangeBatchBytes {
				if err := flushBatch(); err != nil {
					return err
				}
			}
			it.Next()
		}
		if err := flushBatch(); err != nil {
			return err
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

	// Fast path: when WAL is disabled and there is no in-memory state to merge,
	// avoid snapshot isolation/merge iterators and delete directly from the
	// backend in a single commit.
	//
	// This is safe only when we have no queued memtables and the mutable memtable
	// is empty; otherwise we'd violate "newest wins" semantics.
	if db.disableWAL {
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
		if coversAll && db.disableWAL && backendEmpty {
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

	// When WAL is disabled (op-geth style "unsafe" mode), avoid snapshot
	// isolation and MergingIterator overhead. DeleteRange doesn't require sorted
	// enumeration across sources; we can scan each source independently and write
	// tombstones into the current mutable memtable.
	if db.disableWAL {
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
					dstWALPaths = append(dstWALPaths, "")
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

	if !db.disableWAL {
		durability := walDurabilityNone
		if sync {
			if db.relaxedSync {
				durability = walDurabilityFlush
			} else {
				durability = walDurabilitySync
			}
		}
		rec := logRecord{Op: logOpDelete, Key: key}
		if _, err := db.appendWAL([]logRecord{rec}, durability); err != nil {
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
	if sync && db.disableWAL {
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

func (db *DB) rotateMemtableLockedWithCapacity(triggerFlush bool, newCapacity int) error {
	walPath := db.walPath
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
		db.queueWALPaths = append(db.queueWALPaths, walPath)
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
	if !db.disableWAL {
		if db.wal != nil && db.walLiveBytes.Load() < 10*1024*1024 {
			if triggerFlush {
				select {
				case db.flushCh <- struct{}{}:
				default:
				}
			}
			return nil
		}

		if err := db.rotateWALLocked(); err != nil {
			return err
		}
	} else {
		// WAL disabled: just trigger flush if needed
		db.walPath = "" // Ensure no WAL path is tracked
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
	walPath := ""
	if !db.disableWAL {
		walPath = db.walPath
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
		db.queueWALPaths = append(db.queueWALPaths, walPath)
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

func (db *DB) rotateWALLocked() error {
	if db.disableWAL {
		return nil
	}
	db.walSeq++
	name := fmt.Sprintf("%s%06d.log", db.logSegmentPrefix(), db.walSeq)
	path := filepath.Join(db.dir, name)
	fileID := uint32(0)
	if db.valueLogEnabled() {
		fileID = page.ValueLogFileID(uint32(db.walSeq))
	}

	if db.wal != nil {
		oldPath := db.walPath
		oldSize := db.wal.Size()
		if err := db.wal.RotateTo(path, fileID); err != nil {
			return err
		}
		db.walLiveBytes.Store(0)
		if oldPath != "" {
			if db.walClosedSizes == nil {
				db.walClosedSizes = make(map[string]int64)
			}
			prev := db.walClosedSizes[oldPath]
			db.walClosedSizes[oldPath] = oldSize
			db.walClosedBytes.Add(oldSize - prev)
		}
	} else {
		if db.valueLogEnabled() {
			w, err := vlog.NewWriter(path, fileID)
			if err != nil {
				return err
			}
			db.wal = &vlogWriterAdapter{w: w}
		} else {
			w, err := wal.NewWriter(path)
			if err != nil {
				return err
			}
			db.wal = &walWriterAdapter{w: w}
		}
		db.walLiveBytes.Store(0)
	}
	db.walPath = path
	db.walLiveBytes.Store(0)
	return nil
}

func (db *DB) untrackWALSegmentLocked(path string) {
	if db.walClosedSizes == nil || path == "" {
		return
	}
	size, ok := db.walClosedSizes[path]
	if !ok {
		return
	}
	delete(db.walClosedSizes, path)
	for {
		cur := db.walClosedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if db.walClosedBytes.CompareAndSwap(cur, next) {
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
	if db.disableWAL && !db.relaxedSync {
		return true
	}
	return false
}

func (db *DB) flushAll(sync bool) {
	sync = db.flushSyncRequested(sync)
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
	walPaths := make([]string, max)
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
		walPath  string
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
			walPath:  walPaths[i],
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
				for iter.Valid() {
					if iter.IsDeleted() {
						ops = append(ops, batch.Entry{
							Type: batch.OpDelete,
							Key:  iter.UnsafeKey(),
						})
					} else {
						key := iter.UnsafeKey()
						val := iter.UnsafeValue()
						if ptrs != nil {
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
						ops = append(ops, batch.Entry{
							Type:  batch.OpPut,
							Key:   key,
							Value: val,
						})
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
			pointerBatch, _ := backendBatch.(interface {
				SetPointer(key []byte, ptr page.ValuePtr) error
			})
			for _, unit := range units {
				if debugPtr {
					if unit.ptrs == nil {
						ptrNoMap.Add(1)
					} else {
						ptrMapKeys.Add(int64(unit.ptrs.Len()))
					}
				}
				iter := unit.mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator
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
						val := iter.UnsafeValue()
						if pointerBatch != nil && unit.ptrs != nil {
							if debugPtr {
								ptrChecks.Add(1)
							}
							if ptr, ok := unit.ptrs.Get(key); ok {
								if debugPtr {
									ptrHits.Add(1)
								}
								if err := pointerBatch.SetPointer(key, ptr); err != nil {
									db.reportError(fmt.Errorf("cachingdb: flush failed (setptr): %w", err))
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
		if err := db.flushValueLog(); err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
			_ = backendBatch.Close()
			return false
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
	if len(db.queueLargePtrs) >= len(units) {
		db.queueLargePtrs = db.queueLargePtrs[len(units):]
	}
	db.queueBacklogBytes.Add(-totalBytes)
	db.publishMemtablesLocked()

	deletable := make([]string, 0, len(units))
	if sync {
		for _, unit := range units {
			walPath := unit.walPath
			if walPath == "" {
				continue
			}

			inUse := false
			if db.walPath == walPath {
				inUse = true
			} else {
				for _, p := range db.queueWALPaths {
					if p == walPath {
						inUse = true
						break
					}
				}
			}
			if !inUse {
				if db.valueLogRetained(walPath) {
					continue
				}
				deletable = append(deletable, walPath)
			}
		}
	}
	db.mu.Unlock()

	removed := false
	for _, walPath := range deletable {
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			db.reportError(fmt.Errorf("cachingdb: failed to remove WAL segment %q: %w", walPath, err))
			continue
		}
		db.dropValueLogSegment(walPath)
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
	walPath := ""
	if len(db.queueWALPaths) > 0 {
		walPath = db.queueWALPaths[0]
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
		// Flush 'mem' to backend
		backendBatch := db.backend.NewBatch()
		iter := mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator

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
					val := iter.UnsafeValue()
					if ptrs != nil {
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
					ops = append(ops, batch.Entry{
						Type:  batch.OpPut,
						Key:   key,
						Value: val,
					})
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
			pointerBatch, _ := backendBatch.(interface {
				SetPointer(key []byte, ptr page.ValuePtr) error
			})
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
					val := iter.UnsafeValue()
					if pointerBatch != nil && ptrs != nil {
						if ptr, ok := ptrs.GetString(bytesToStringNoCopy(key)); ok {
							if err := pointerBatch.SetPointer(key, ptr); err != nil {
								db.reportError(fmt.Errorf("cachingdb: flush failed (setptr): %w", err))
								_ = iter.Close()
								_ = backendBatch.Close()
								return false
							}
							iter.Next()
							continue
						}
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
		if err := db.flushValueLog(); err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed (vlog flush): %w", err))
			_ = backendBatch.Close()
			return false
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
	if len(db.queueLargePtrs) > 0 {
		db.queueLargePtrs = db.queueLargePtrs[1:]
	}
	db.queueBacklogBytes.Add(-memBytes)
	db.publishMemtablesLocked()

	// Check if WAL is still in use
	inUse := false
	if db.walPath == walPath {
		inUse = true
	} else {
		for _, p := range db.queueWALPaths {
			if p == walPath {
				inUse = true
				break
			}
		}
	}
	db.mu.Unlock()

	if sync && walPath != "" && !inUse {
		retain := db.valueLogRetained(walPath)
		if !retain {
			if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
				db.reportError(fmt.Errorf("cachingdb: failed to remove WAL segment %q: %w", walPath, err))
			} else {
				db.dropValueLogSegment(walPath)
				db.mu.Lock()
				db.untrackWALSegmentLocked(walPath)
				db.mu.Unlock()
				db.forgetValueLogRetain(walPath)
				db.syncDirBestEffort(db.dir)
			}
		}
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
	walCurrentBytes := db.walLiveBytes.Load()
	walClosedBytes := db.walClosedBytes.Load()
	db.mu.RUnlock()

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
	db      *DB
	entries []batch.Entry
	backend batch.Interface
	size    int
	walBuf  []logRecord

	closed         bool
	streamEligible bool
	streamTried    bool
	firstKey       []byte
	lastKey        []byte
	batchRange     keyRange
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
	b.size = 0
	b.walBuf = b.walBuf[:0]
	b.streamEligible = true
	b.streamTried = false
	b.firstKey = nil
	b.lastKey = nil
	b.batchRange = keyRange{}
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
	// WAL. Only enable it when WAL is disabled.
	if !b.db.disableWAL {
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
	shardAdds := make([]int64, shardCount)
	shardCounts := make([]int, shardCount)
	for _, op := range b.entries {
		idx := b.db.shardIndex(op.Key)
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

	// 2. WAL Append loop
	if !b.db.disableWAL {
		records := b.walBuf[:0]
		if cap(records) < len(b.entries) {
			records = make([]logRecord, 0, len(b.entries))
		}
		for _, op := range b.entries {
			if op.Type == batch.OpDelete {
				records = append(records, logRecord{Op: logOpDelete, Key: op.Key})
			} else {
				records = append(records, logRecord{Op: logOpSet, Key: op.Key, Value: op.Value})
			}
		}
		b.walBuf = records

		durability := walDurabilityNone
		if sync {
			if b.db.relaxedSync {
				durability = walDurabilityFlush
			} else {
				durability = walDurabilitySync
			}
		}
		ptrs, err := b.db.appendWAL(records, durability)
		if err != nil {
			b.db.writeMu.RUnlock()
			return err
		}
		debugPtr := b.db.debugFlushPointers
		valueLogEnabled := b.db.valueLogEnabled()
		eligibleCount := 0
		allowPointers := false
		if valueLogEnabled || debugPtr {
			for i := range b.entries {
				op := &b.entries[i]
				if op.Type != batch.OpPut || len(op.Value) <= b.db.inlineThreshold {
					continue
				}
				if debugPtr {
					eligibleCount++
				}
				if valueLogEnabled && !allowPointers {
					allowPointers = b.db.allowValueLogPointers()
					if !debugPtr && allowPointers {
						break
					}
				}
			}
		}
		if debugPtr && eligibleCount > 0 {
			b.db.debugPtrEligible.Add(int64(eligibleCount))
			if !valueLogEnabled {
				b.db.debugPtrDisabled.Add(int64(eligibleCount))
			} else if !allowPointers {
				b.db.debugPtrDenied.Add(int64(eligibleCount))
			} else if len(ptrs) != len(b.entries) {
				b.db.debugPtrNoPtr.Add(int64(eligibleCount))
			}
		}
		if allowPointers && len(ptrs) == len(b.entries) {
			retain := false
			for i := range b.entries {
				op := &b.entries[i]
				if op.Type != batch.OpPut {
					continue
				}
				if len(op.Value) <= b.db.inlineThreshold {
					continue
				}
				op.ValuePtr = ptrs[i]
				op.IsPtr = true
				if debugPtr {
					b.db.debugPtrUsed.Add(1)
				}
				retain = true
				hasPtr = true
			}
			if retain {
				b.db.mu.RLock()
				retainPath := b.db.walPath
				b.db.mu.RUnlock()
				b.db.markValueLogRetain(retainPath)
			}
		}
	}

	shardEntries := make([][]batch.Entry, shardCount)
	for i, count := range shardCounts {
		if count > 0 {
			shardEntries[i] = make([]batch.Entry, 0, count)
		}
	}
	for _, op := range b.entries {
		idx := b.db.shardIndex(op.Key)
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
	if sync && b.db.disableWAL {
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
		seq, valueLog, ok := parseLogSeq(name)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(walDir, name)
		segments = append(segments, logSegmentInfo{path: path, size: info.Size(), seq: seq, valueLog: valueLog})
		if info.Size() > 0 {
			nonEmptyBytes += info.Size()
		}
	}
	return segments, nonEmptyBytes
}

func parseLogSeq(name string) (int, bool, bool) {
	const (
		walPrefix  = "wal-"
		vlogPrefix = "vlog-"
	)
	if filepath.Ext(name) != ".log" {
		return 0, false, false
	}
	if strings.HasPrefix(name, walPrefix) {
		core := strings.TrimSuffix(strings.TrimPrefix(name, walPrefix), ".log")
		if core == "" {
			return 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, false, false
		}
		return seq, false, true
	}
	if strings.HasPrefix(name, vlogPrefix) {
		core := strings.TrimSuffix(strings.TrimPrefix(name, vlogPrefix), ".log")
		if core == "" {
			return 0, false, false
		}
		seq, err := strconv.Atoi(core)
		if err != nil {
			return 0, false, false
		}
		return seq, true, true
	}
	return 0, false, false
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
