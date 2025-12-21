package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/wal"
)

var ErrKeyEmpty = fmt.Errorf("key cannot be empty")
var ErrValueNil = fmt.Errorf("value cannot be nil")
var ErrBatchClosed = fmt.Errorf("batch has been written or closed")

var iteratorDebugEnabled atomic.Bool

const (
	minMemtablePrealloc        = 64 * 1024
	maxMemtablePrealloc        = 256 << 20
	adaptiveMinWrites          = 1024
	adaptiveSequentialWritePct = 0.85
	adaptiveWarmupBytes        = 16 * 1024 * 1024
)

// SetIteratorDebug toggles attaching debug metadata to iterators returned by
// CachingDB.Iterator. It is intended for benchmarking/diagnostics.
func SetIteratorDebug(enabled bool) {
	iteratorDebugEnabled.Store(enabled)
}

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
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

// BackendDB defines the subset of treedb.DB needed by CachingDB.
type BackendDB interface {
	Get(key []byte) ([]byte, error)
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
	// RelaxedSync disables fsync on Sync operations.
	RelaxedSync bool
}

type DB struct {
	mu      sync.RWMutex
	flushMu sync.Mutex
	writeMu sync.Mutex
	bpMu    sync.Mutex
	bpCond  *sync.Cond

	checkpointMu   sync.Mutex
	checkpointCond *sync.Cond
	checkpointing  atomic.Bool

	// Level 0 (Memory)
	mutable           memtable.Table
	queue             []memtable.Table
	mutableRange      keyRange
	queueRanges       []keyRange
	queueWALPaths     []string
	backendRange      keyRange
	backendRangeKnown bool
	backendRangeInit  sync.Once
	backendRangeErr   error

	// Durability
	wal     *wal.Writer
	walPath string
	walSeq  int // Sequence number for WAL files
	// walClosedBytes is an in-memory estimate of retained (non-current) WAL
	// segment bytes. It is updated on WAL rotation and segment deletion.
	walClosedBytes atomic.Int64
	// walLiveBytes tracks the current WAL size to avoid lock contention.
	walLiveBytes   atomic.Int64
	walClosedSizes map[string]int64

	// Level 1 (Disk)
	backend BackendDB

	// Config
	dir                     string
	flushThreshold          int64
	memtableCap             int
	memtableMode            memtable.Mode
	memtableAdaptive        bool
	memtableWarmupActive    bool
	memtableWarmupThreshold int64
	memtableStats           memtableStats
	maxQueuedMemtables      int
	slowdownBacklogSeconds  float64
	stopBacklogSeconds      float64
	maxBacklogBytes         int64
	writerFlushMaxMemtables int
	writerFlushMaxDuration  time.Duration
	flushBuildConcurrency   int

	disableWAL  bool
	relaxedSync bool

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

	autoCheckpointCount          atomic.Uint64
	autoCheckpointLastReason     atomic.Uint32
	autoCheckpointLastUnixNano   atomic.Int64
	autoCheckpointLastDurNanos   atomic.Int64
	autoCheckpointLastWALBefore  atomic.Int64
	autoCheckpointLastWALAfter   atomic.Int64
	autoCheckpointLastWALTrimmed atomic.Int64
	autoCheckpointLastWALBytes   atomic.Int64
	autoCheckpointMaxWALBytes    atomic.Int64
}

type keyRange struct {
	valid bool
	min   []byte
	max   []byte
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

func (db *DB) noteWriteKeyLocked(key []byte) {
	if !db.memtableAdaptive {
		return
	}
	stats := &db.memtableStats
	stats.writes++
	if len(key) == 0 {
		stats.hasLastKey = false
		return
	}
	if stats.hasLastKey && bytes.Compare(stats.lastKey, key) < 0 {
		stats.seqWrites++
	}
	stats.lastKey = key
	stats.hasLastKey = true
}

func (db *DB) noteIteratorLocked(start, end []byte) {
	if !db.memtableAdaptive {
		return
	}
	stats := &db.memtableStats
	stats.iterators++
	if start != nil || end != nil {
		stats.rangeIters++
	}
}

func (db *DB) mutableFlushThresholdLocked() int64 {
	if db.memtableWarmupActive && db.memtableWarmupThreshold > 0 && db.memtableWarmupThreshold < db.flushThreshold {
		return db.memtableWarmupThreshold
	}
	return db.flushThreshold
}

func (db *DB) resetMemtableStatsLocked() {
	db.memtableStats = memtableStats{}
}

func (db *DB) chooseAdaptiveMemtableModeLocked() memtable.Mode {
	stats := &db.memtableStats
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

func Open(dir string, backend BackendDB, opts Options) (*DB, error) {
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
	if opts.MaxQueuedMemtables == 0 {
		// Keep the default queued backlog roughly stable in bytes when callers
		// tune FlushThreshold. Historically: 64MB flush threshold with a queue
		// length of 4 => ~256MB backlog.
		opts.MaxQueuedMemtables = defaultMaxQueuedMemtables(opts.FlushThreshold)
	}
	if opts.WriterFlushMaxMemtables == 0 {
		opts.WriterFlushMaxMemtables = 1
	}
	if opts.FlushBuildConcurrency <= 0 {
		opts.FlushBuildConcurrency = 1
	}

	// Ensure wal dir exists
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}
	segments, _ := listNonEmptyWALSegments(walDir)
	maxWALSeq := 0
	for _, seg := range segments {
		seq, ok := parseWALSeq(filepath.Base(seg.path))
		if ok && seq > maxWALSeq {
			maxWALSeq = seq
		}
	}

	warmupThreshold := opts.FlushThreshold
	if adaptive && adaptiveWarmupBytes > 0 && int64(adaptiveWarmupBytes) < opts.FlushThreshold {
		warmupThreshold = int64(adaptiveWarmupBytes)
	}
	warmupCap := memtableCapacity(warmupThreshold)
	mt, err := memtable.NewWithCapacityMode(warmupCap, mode)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dir:                     walDir,
		backend:                 backend,
		flushThreshold:          opts.FlushThreshold,
		memtableCap:             memCap,
		memtableMode:            mode,
		memtableAdaptive:        adaptive,
		memtableWarmupActive:    adaptive && warmupThreshold < opts.FlushThreshold,
		memtableWarmupThreshold: warmupThreshold,
		maxQueuedMemtables:      opts.MaxQueuedMemtables,
		slowdownBacklogSeconds:  opts.SlowdownBacklogSeconds,
		stopBacklogSeconds:      opts.StopBacklogSeconds,
		maxBacklogBytes:         opts.MaxBacklogBytes,
		writerFlushMaxMemtables: opts.WriterFlushMaxMemtables,
		writerFlushMaxDuration:  opts.WriterFlushMaxDuration,
		flushBuildConcurrency:   opts.FlushBuildConcurrency,
		disableWAL:              opts.DisableWAL,
		relaxedSync:             opts.RelaxedSync,
		mutable:                 mt,
		closeCh:                 make(chan struct{}),
		flushCh:                 make(chan struct{}, 1),
		autoCheckpointOnceCh:    make(chan struct{}, 1),
		autoCheckpointWriteCh:   make(chan struct{}, 1),
		walSeq:                  maxWALSeq,
	}
	db.bpCond = sync.NewCond(&db.bpMu)
	db.checkpointCond = sync.NewCond(&db.checkpointMu)

	// Open initial WAL
	if !db.disableWAL {
		if err := db.rotateWALLocked(); err != nil {
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
	}

	// Start background flusher
	db.wg.Add(1)
	go db.flushLoop()

	return db, nil
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

func walRecordSize(key, value []byte) int64 {
	return int64(walSegmentHeaderBytes + walRecordHeaderBytes + len(key) + len(value))
}

func walBatchSize(records []wal.Record) int64 {
	if len(records) == 0 {
		return 0
	}
	total := walSegmentHeaderBytes
	for _, r := range records {
		total += walRecordHeaderBytes + len(r.Key) + len(r.Value)
	}
	return int64(total)
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
		if maxWALBytes <= 0 || effectiveBytes < maxWALBytes {
			return
		}
	default:
		// Unknown mode: be conservative and do nothing.
		return
	}

	before := effectiveBytes
	start := time.Now()
	err := db.Checkpoint()
	dur := time.Since(start)
	after := db.effectiveWALBytes()
	trimmed := before - after
	if trimmed < 0 {
		trimmed = 0
	}

	// Best-effort: failures here should be surfaced via normal write paths or
	// explicit maintenance calls. Avoid printing from background maintenance.
	if err != nil {
		return
	}

	db.autoCheckpointCount.Add(1)
	db.autoCheckpointLastReason.Store(uint32(mode))
	db.autoCheckpointLastUnixNano.Store(time.Now().UnixNano())
	db.autoCheckpointLastDurNanos.Store(dur.Nanoseconds())
	db.autoCheckpointLastWALBefore.Store(before)
	db.autoCheckpointLastWALAfter.Store(after)
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
	if db.mutable.Len() > 0 {
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

	segments, nonEmptyBytes := listNonEmptyWALSegments(walDir)
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

	for _, seg := range segments {
		path := seg.path
		if path == currentWAL {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "cachingdb: failed to remove WAL segment %q: %v\n", path, err)
			continue
		}
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
	}

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
	hadMemtables := false
	db.writeMu.Lock()
	db.mu.Lock()
	if db.mutable.Len() > 0 {
		hadMemtables = true
		_ = db.rotateMemtableLocked(true)
	} else if len(db.queue) > 0 {
		hadMemtables = true
	}
	db.mu.Unlock()
	db.writeMu.Unlock()

	close(db.closeCh)
	db.wg.Wait()

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
			return err
		}
	}

	seen := make(map[string]struct{}, len(walPaths))
	for _, path := range walPaths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "cachingdb: failed to remove WAL segment %q: %v\n", path, err)
			continue
		}
		db.mu.Lock()
		db.untrackWALSegmentLocked(path)
		db.mu.Unlock()
	}

	return db.backend.Close()
}
func (db *DB) Set(key, value []byte) error {
	if key == nil {
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
	if key == nil {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	db.waitForCheckpoint()
	db.waitForStop()
	return db.set(key, value, true)
}

func (db *DB) set(key, value []byte, sync bool) error {
	db.writeMu.Lock()

	if !db.disableWAL {
		db.mu.RLock()
		w := db.wal
		relaxedSync := db.relaxedSync
		db.mu.RUnlock()

		if w != nil {
			if err := w.Append(wal.OpSet, key, value); err != nil {
				db.writeMu.Unlock()
				return err
			}
			db.walLiveBytes.Add(walRecordSize(key, value))
			if sync && !relaxedSync {
				if err := w.Sync(); err != nil {
					db.writeMu.Unlock()
					return err
				}
			}
		}
	}

	db.mu.Lock()
	db.mutable.Set(key, value)

	db.mutableRange.add(key)
	db.noteWriteKeyLocked(key)

	// 3. Check Threshold
	if db.mutable.Size() > db.mutableFlushThresholdLocked() {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.mu.Unlock()
	db.writeMu.Unlock()

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) Delete(key []byte) error {
	if key == nil {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	db.waitForStop()
	return db.delete(key, false)
}

func (db *DB) DeleteSync(key []byte) error {
	if key == nil {
		return ErrKeyEmpty
	}
	db.waitForCheckpoint()
	db.waitForStop()
	return db.delete(key, true)
}

func (db *DB) delete(key []byte, sync bool) error {
	db.writeMu.Lock()

	if !db.disableWAL {
		db.mu.RLock()
		w := db.wal
		relaxedSync := db.relaxedSync
		db.mu.RUnlock()

		if w != nil {
			if err := w.Append(wal.OpDelete, key, nil); err != nil {
				db.writeMu.Unlock()
				return err
			}
			db.walLiveBytes.Add(walRecordSize(key, nil))
			if sync && !relaxedSync {
				if err := w.Sync(); err != nil {
					db.writeMu.Unlock()
					return err
				}
			}
		}
	}

	db.mu.Lock()
	db.mutable.Delete(key)

	db.mutableRange.add(key)
	db.noteWriteKeyLocked(key)

	// 3. Threshold
	if db.mutable.Size() > db.mutableFlushThresholdLocked() {
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.mu.Unlock()
	db.writeMu.Unlock()

	db.noteWrite()
	db.maybeAssistFlush()
	return nil
}

func (db *DB) rotateMemtableLocked(triggerFlush bool) error {
	walPath := db.walPath
	memBytes := db.mutable.Size()
	db.mutable.Freeze()
	db.queue = append(db.queue, db.mutable)
	db.queueBacklogBytes.Add(memBytes)
	db.queueRanges = append(db.queueRanges, db.mutableRange)
	db.queueWALPaths = append(db.queueWALPaths, walPath)
	if db.memtableAdaptive {
		db.memtableMode = db.chooseAdaptiveMemtableModeLocked()
	}
	if db.memtableWarmupActive {
		db.memtableWarmupActive = false
	}
	mt, err := memtable.NewWithCapacityMode(db.memtableCap, db.memtableMode)
	if err != nil {
		return err
	}
	db.mutable = mt
	db.mutableRange = keyRange{}
	if db.memtableAdaptive {
		db.resetMemtableStatsLocked()
	}

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

func (db *DB) rotateWALLocked() error {
	if db.disableWAL {
		return nil
	}
	if db.wal != nil {
		oldPath := db.walPath
		oldSize := db.wal.Size()
		_ = db.wal.Close()
		db.walLiveBytes.Store(0)
		if oldPath != "" {
			if db.walClosedSizes == nil {
				db.walClosedSizes = make(map[string]int64)
			}
			prev := db.walClosedSizes[oldPath]
			db.walClosedSizes[oldPath] = oldSize
			db.walClosedBytes.Add(oldSize - prev)
		}
	}
	db.walSeq++
	name := fmt.Sprintf("wal-%06d.log", db.walSeq)
	path := filepath.Join(db.dir, name)
	w, err := wal.NewWriter(path)
	if err != nil {
		return err
	}
	db.wal = w
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
			// Background flush is intentionally async (no backend sync). The WAL is
			// retained so the backend can recover up to the last synced boundary.
			db.flushAll(false)
		}
	}
}

func (db *DB) flushAll(sync bool) {
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
	copy(mems, db.queue[:max])
	copy(ranges, db.queueRanges[:max])
	copy(walPaths, db.queueWALPaths[:max])
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
		})
		totalBytes += memBytes
		totalLen += memLen
	}

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
			collectOps := func(mem memtable.Table, estLen int) ([]batch.Entry, error) {
				ops := make([]batch.Entry, 0, estLen)
				iter := mem.NewIterator(nil, nil)
				iter.Seek(nil)
				for iter.Valid() {
					if iter.IsDeleted() {
						ops = append(ops, batch.Entry{
							Type: batch.OpDelete,
							Key:  iter.UnsafeKey(),
						})
					} else {
						ops = append(ops, batch.Entry{
							Type:  batch.OpPut,
							Key:   iter.UnsafeKey(),
							Value: iter.UnsafeValue(),
						})
					}
					iter.Next()
				}
				err := iter.Close()
				if err == nil {
					err = iter.Error()
				}
				return ops, err
			}

			buildConcurrency := db.flushBuildConcurrency
			if buildConcurrency <= 1 || len(units) <= 1 {
				ops := make([]batch.Entry, 0, totalLen)
				for _, unit := range units {
					memOps, err := collectOps(unit.mem, unit.memLen)
					if err != nil {
						fmt.Fprintf(os.Stderr, "cachingdb: flush failed (iter): %v\n", err)
						_ = backendBatch.Close()
						return false
					}
					ops = append(ops, memOps...)
				}

				if err := backendBatch.SetOps(ops); err != nil {
					fmt.Fprintf(os.Stderr, "cachingdb: flush failed (setops): %v\n", err)
					_ = backendBatch.Close()
					return false
				}
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

						ops, err := collectOps(unit.mem, unit.memLen)
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
					fmt.Fprintf(os.Stderr, "cachingdb: flush failed (iter): %v\n", err)
					_ = backendBatch.Close()
					return false
				default:
				}

				ops := make([]batch.Entry, 0, totalLen)
				for i := range unitOps {
					ops = append(ops, unitOps[i]...)
				}

				if err := backendBatch.SetOps(ops); err != nil {
					fmt.Fprintf(os.Stderr, "cachingdb: flush failed (setops): %v\n", err)
					_ = backendBatch.Close()
					return false
				}
			}
		} else {
			for _, unit := range units {
				iter := unit.mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator
				iter.Seek(nil)                         // Start
				for iter.Valid() {
					key := iter.UnsafeKey()
					if iter.IsDeleted() {
						if err := backendBatch.Delete(key); err != nil {
							fmt.Fprintf(os.Stderr, "cachingdb: flush failed (delete): %v\n", err)
							_ = iter.Close()
							_ = backendBatch.Close()
							return false
						}
					} else {
						if err := backendBatch.Set(key, iter.UnsafeValue()); err != nil {
							fmt.Fprintf(os.Stderr, "cachingdb: flush failed (set): %v\n", err)
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
		var err error
		if sync {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "cachingdb: flush failed: %v\n", err)
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
	db.queueBacklogBytes.Add(-totalBytes)

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
				deletable = append(deletable, walPath)
			}
		}
	}
	db.mu.Unlock()

	for _, walPath := range deletable {
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "cachingdb: failed to remove WAL segment %q: %v\n", walPath, err)
			continue
		}
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
	}

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
	db.mu.Unlock()

	// Optimization: Skip flush for empty memtables (e.g. from frequent Iterator creation)
	flushStart := time.Time{}
	flushed := false
	if memLen > 0 {
		flushStart = time.Now()
		// Flush 'mem' to backend
		backendBatch := db.backend.NewBatch()
		iter := mem.NewIterator(nil, nil) // Returns iterator.UnsafeIterator
		iter.Seek(nil)                    // Start

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
					ops = append(ops, batch.Entry{
						Type:  batch.OpPut,
						Key:   iter.UnsafeKey(),
						Value: iter.UnsafeValue(),
					})
				}
				iter.Next()
			}
			if err := backendBatch.SetOps(ops); err != nil {
				fmt.Fprintf(os.Stderr, "cachingdb: flush failed (setops): %v\n", err)
				_ = iter.Close()
				_ = backendBatch.Close()
				return false
			}
		} else {
			for iter.Valid() {
				key := iter.UnsafeKey()
				if iter.IsDeleted() {
					if err := backendBatch.Delete(key); err != nil {
						fmt.Fprintf(os.Stderr, "cachingdb: flush failed (delete): %v\n", err)
						_ = iter.Close()
						_ = backendBatch.Close()
						return false
					}
				} else {
					if err := backendBatch.Set(key, iter.UnsafeValue()); err != nil {
						fmt.Fprintf(os.Stderr, "cachingdb: flush failed (set): %v\n", err)
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
		var err error
		if sync {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "cachingdb: flush failed: %v\n", err)
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
	db.queueBacklogBytes.Add(-memBytes)

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
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "cachingdb: failed to remove WAL segment %q: %v\n", walPath, err)
		} else {
			db.mu.Lock()
			db.untrackWALSegmentLocked(walPath)
			db.mu.Unlock()
		}
	}

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

// Get implements DB.Get using Merging logic logic conceptually,
// but optimized: check mutable, then queue (newest to oldest), then disk.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	// check mutable
	val, deleted, found := db.mutable.Get(key)
	if found {
		db.mu.RUnlock()
		if deleted {
			return nil, nil
		}
		if val == nil {
			return []byte{}, nil
		}
		return val, nil
	}

	// check queue backwards (newest first)
	for i := len(db.queue) - 1; i >= 0; i-- {
		val, deleted, found = db.queue[i].Get(key)
		if found {
			db.mu.RUnlock()
			if deleted {
				return nil, nil
			}
			if val == nil {
				return []byte{}, nil
			}
			return val, nil
		}
	}
	db.mu.RUnlock()

	return db.backend.Get(key)
}

func (db *DB) Has(key []byte) (bool, error) {
	v, err := db.Get(key)
	return v != nil, err
}

func (db *DB) Stats() map[string]string {
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	db.mu.RLock()
	stats["treedb.cache.queue_len"] = fmt.Sprintf("%d", len(db.queue))
	stats["treedb.cache.mutable_bytes"] = fmt.Sprintf("%d", db.mutable.Size())
	stats["treedb.cache.flush_threshold_bytes"] = fmt.Sprintf("%d", db.flushThreshold)
	stats["treedb.cache.memtable_mode"] = db.memtableMode.String()
	stats["treedb.cache.max_queued_memtables"] = fmt.Sprintf("%d", db.maxQueuedMemtables)
	walCurrentBytes := db.walLiveBytes.Load()
	stats["treedb.cache.wal_bytes_estimate"] = fmt.Sprintf("%d", db.walClosedBytes.Load()+walCurrentBytes)
	stats["treedb.cache.wal_closed_bytes_estimate"] = fmt.Sprintf("%d", db.walClosedBytes.Load())
	stats["treedb.cache.wal_current_bytes_estimate"] = fmt.Sprintf("%d", walCurrentBytes)
	if db.adaptiveBackpressureEnabled() {
		stats["treedb.cache.backpressure_mode"] = "adaptive"
	} else {
		stats["treedb.cache.backpressure_mode"] = "queue_len"
	}
	db.mu.RUnlock()
	stats["treedb.cache.queue_backlog_bytes"] = fmt.Sprintf("%d", db.queueBacklogBytes.Load())
	db.bpMu.Lock()
	stats["treedb.cache.flush_bps_ewma"] = fmt.Sprintf("%.0f", db.flushBpsEWMA)
	db.bpMu.Unlock()

	stats["treedb.cache.auto_checkpoint.count"] = fmt.Sprintf("%d", db.autoCheckpointCount.Load())
	stats["treedb.cache.auto_checkpoint.last_reason"] = autoCheckpointReasonString(db.autoCheckpointLastReason.Load())
	stats["treedb.cache.auto_checkpoint.last_duration_ms"] = fmt.Sprintf("%.3f", float64(db.autoCheckpointLastDurNanos.Load())/float64(time.Millisecond))
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_before"] = fmt.Sprintf("%d", db.autoCheckpointLastWALBefore.Load())
	stats["treedb.cache.auto_checkpoint.last_wal_bytes_after"] = fmt.Sprintf("%d", db.autoCheckpointLastWALAfter.Load())
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
	if db.mutable.Len() > 0 {
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

	db.writeMu.Lock()
	db.mu.Lock()
	db.noteIteratorLocked(start, end)
	db.writeMu.Unlock()
	defer db.mu.Unlock()

	// Snapshot Isolation:
	// To ensure the iterator sees a consistent point-in-time view, we rotate the
	// mutable memtable into the immutable queue. The iterator then consumes
	// only the queue and the backend. Any subsequent writes will go to a new
	// mutable memtable which this iterator ignores.
	if db.mutable.Len() > 0 {
		if err := db.rotateMemtableLocked(false); err != nil {
			return nil, err
		}
	}

	queueLen := len(db.queue)

	// Fast path for full scans: if the in-memory key ranges are disjoint from the
	// backend key range, we can concatenate iterators instead of merging.
	if start == nil && end == nil {
		// Only do this when the queue is empty; queued memtables imply the backend
		// might not yet include older keys, making disjoint-range checks unreliable.
		if db.backendRangeKnown && len(db.queue) == 0 && db.mutableRange.valid && db.backendRange.valid && !rangesOverlap(db.mutableRange, db.backendRange) {
			diskIter, err := db.backend.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}

			// Since we rotated or mutable is empty, and queue is empty (checked above),
			// mutable is empty. So we just return diskIter?
			// Wait, if len(queue) == 0 and mutable.Size() == 0 (from rotate check logic),
			// then there is no memory data.
			if iteratorDebugEnabled.Load() {
				return &debugIterator{Iterator: diskIter, queueLen: queueLen, sourcesUsed: 1}, nil
			}
			return diskIter, nil
		}
	}

	var sources []merging.IteratorSource

	// Priority 0..N: Queue (Newest first)
	// Note: We skip db.mutable because we just rotated it (so it's empty) or it was already empty.
	prio := 0
	for i := len(db.queue) - 1; i >= 0; i-- {
		if overlapsQuery(start, end, db.queueRanges[i]) {
			qIter := db.queue[i].NewIterator(start, end)
			qIter.Seek(start)
			sources = append(sources, merging.IteratorSource{
				Iter:     qIter,
				Priority: prio,
			})
		}
		prio++
	}

	// Disk Iterator
	// Only skip if we definitively know the range and it doesn't overlap.
	if !db.backendRangeKnown || overlapsQuery(start, end, db.backendRange) {
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
	db.noteIteratorLocked(start, end)
	if db.mutable.Len() > 0 {
		_ = db.rotateMemtableLocked(true) // Flush to backend
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

func (b *Batch) Set(key, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	if key == nil {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}

	b.batchRange.add(key)

	if b.backend != nil {
		b.size += len(key) + len(value)
		return b.backend.Set(key, value)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = append([]byte(nil), key...)
			b.lastKey = append([]byte(nil), key...)
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = append(b.lastKey[:0], key...)
		}
	}
	// We don't know about slabs/thresholds here, so we just store inline.
	// Backend will handle promotion to slab if needed during writeBypass,
	// or standard write will handle it via WAL/Memtable (which don't use slabs yet).
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
	if key == nil {
		return ErrKeyEmpty
	}

	b.batchRange.add(key)

	if b.backend != nil {
		b.size += len(key)
		return b.backend.Delete(key)
	}

	if b.streamEligible {
		if b.firstKey == nil {
			b.firstKey = append([]byte(nil), key...)
			b.lastKey = append([]byte(nil), key...)
		} else {
			if bytes.Compare(key, b.lastKey) <= 0 {
				b.streamEligible = false
			}
			b.lastKey = append(b.lastKey[:0], key...)
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
		for _, op := range ops {
			b.size += len(op.Key) + len(op.Value)
			b.batchRange.add(op.Key)
		}
		return b.backend.SetOps(ops)
	}
	for _, op := range ops {
		b.entries = append(b.entries, op)
		b.size += len(op.Key) + len(op.Value)
		b.batchRange.add(op.Key)
	}
	return nil
}

func (b *Batch) maybeSwitchToStreaming() {
	if b.streamTried || !b.streamEligible || b.backend != nil {
		return
	}
	// The streamSwitchThreshold check is now redundant because the b.size
	// check against db.flushThreshold effectively handles this.
	// If a batch is small enough to be under the flush threshold, we want it
	// to go through the regular memtable path for aggregation, regardless
	// of sequentiality.

	// Also require the batch to be large enough to justify a direct write.
	// Otherwise, small sequential batches bypass the memtable and cause
	// write amplification in the backend.
	if b.size < int(b.db.flushThreshold) {
		return
	}

	// Only attempt streaming if the batch is strictly increasing and starts beyond
	// the maximum key present in the in-memory layers.
	b.db.mu.RLock()
	var maxKey []byte
	if b.db.mutableRange.valid {
		maxKey = b.db.mutableRange.max
	}
	for _, r := range b.db.queueRanges {
		if !r.valid {
			continue
		}
		if maxKey == nil || bytes.Compare(r.max, maxKey) > 0 {
			maxKey = r.max
		}
	}
	b.db.mu.RUnlock()

	b.streamTried = true
	if maxKey != nil && bytes.Compare(b.firstKey, maxKey) <= 0 {
		return
	}

	backendBatch := b.db.backend.NewBatch()
	if err := backendBatch.SetOps(b.entries); err != nil {
		_ = backendBatch.Close()
		return
	}
	b.backend = backendBatch
	b.entries = nil
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
		if sync {
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
		b.closed = true
		if err == nil && b.size > 0 {
			b.db.noteWrite()
		}
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
	b.db.writeMu.Lock()

	// 1. WAL Append loop
	if !b.db.disableWAL {
		records := make([]wal.Record, 0, len(b.entries))
		for _, op := range b.entries {
			if op.Type == batch.OpDelete {
				records = append(records, wal.Record{Op: wal.OpDelete, Key: op.Key})
			} else {
				records = append(records, wal.Record{Op: wal.OpSet, Key: op.Key, Value: op.Value})
			}
		}

		b.db.mu.RLock()
		w := b.db.wal
		relaxedSync := b.db.relaxedSync
		b.db.mu.RUnlock()

		if w != nil {
			if err := w.AppendBatch(records); err != nil {
				b.db.writeMu.Unlock()
				return err
			}
			b.db.walLiveBytes.Add(walBatchSize(records))
			if sync && !relaxedSync {
				if err := w.Sync(); err != nil {
					b.db.writeMu.Unlock()
					return err
				}
			}
		}
	}

	b.db.mu.Lock()
	// 2. Memtable Update
	for _, op := range b.entries {
		// We use Steal methods because b.entries owns the key/value copies
		// created in Batch.Set/Delete. We transfer ownership to Memtable.
		if op.Type == batch.OpDelete {
			b.db.mutable.DeleteSteal(op.Key)
		} else {
			b.db.mutable.SetSteal(op.Key, op.Value)
		}
		b.db.noteWriteKeyLocked(op.Key)
		b.db.mutableRange.add(op.Key)
	}

	// 3. Threshold Check
	if b.db.mutable.Size() > b.db.mutableFlushThresholdLocked() {
		if err := b.db.rotateMemtableLocked(true); err != nil {
			b.db.mu.Unlock()
			b.db.writeMu.Unlock()
			return err
		}
	}

	b.db.mu.Unlock()
	b.db.writeMu.Unlock()

	if b.size > 0 {
		b.db.noteWrite()
	}
	b.db.maybeAssistFlush()

	return b.Close()
}

type walSegmentInfo struct {
	path string
	size int64
}

func listNonEmptyWALSegments(walDir string) (segments []walSegmentInfo, nonEmptyBytes int64) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, 0
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) < len("wal-000000.log") || name[:4] != "wal-" || filepath.Ext(name) != ".log" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(walDir, name)
		segments = append(segments, walSegmentInfo{path: path, size: info.Size()})
		if info.Size() > 0 {
			nonEmptyBytes += info.Size()
		}
	}
	return segments, nonEmptyBytes
}

func parseWALSeq(name string) (int, bool) {
	if len(name) < len("wal-000000.log") || !strings.HasPrefix(name, "wal-") || filepath.Ext(name) != ".log" {
		return 0, false
	}
	core := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
	if core == "" {
		return 0, false
	}
	seq, err := strconv.Atoi(core)
	if err != nil {
		return 0, false
	}
	return seq, true
}

func (b *Batch) writeBypass(sync bool) error {
	// Fast path: if none of these keys exist in mutable/queue, we can write directly
	// to the backend without flushing (no in-memory shadowing possible).
	b.db.mu.RLock()
	mutable := b.db.mutable
	queue := append([]memtable.Table(nil), b.db.queue...)
	mutableRange := b.db.mutableRange
	queueRanges := append([]keyRange(nil), b.db.queueRanges...)
	b.db.mu.RUnlock()

	// Cheap append-only check: if the batch key range does not overlap with any
	// in-memory key ranges, it cannot be shadowed.
	batchRange := keyRange{}
	for _, op := range b.entries {
		key := op.Key
		batchRange.add(key)
	}

	overlaps := rangesOverlap(batchRange, mutableRange)
	if !overlaps {
		for _, r := range queueRanges {
			if rangesOverlap(batchRange, r) {
				overlaps = true
				break
			}
		}
	}

	if overlaps {
		// Slow path: verify no individual key exists in memory (handles sparse overlap).
		for _, op := range b.entries {
			key := op.Key
			if _, _, found := mutable.Get(key); found {
				return b.writeRegular(sync)
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
	return b.Close()
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
