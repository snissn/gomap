package treedb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/compaction"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/slab"
)

// Options configures TreeDB. It is re-exported from TreeDB/db for convenience.
type Options = db.Options

// Mode selects which TreeDB implementation to open.
type Mode = db.Mode

const (
	// ModeCached opens TreeDB with the write-back caching layer enabled.
	ModeCached = db.ModeCached
	// ModeBackend opens TreeDB in backend-only mode (no caching layer).
	ModeBackend = db.ModeBackend
)

// Iterator is the public iterator contract returned by TreeDB.
//
// Semantics (performance-first; callers must treat slices as read-only):
//   - Key() and Value() return views valid until the next Next()/Close().
//   - Use KeyCopy/ValueCopy if you need stable bytes.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
	Close() error
	Error() error
}

// Batch is the public batch contract returned by TreeDB.
// Both cached and backend implementations satisfy it.
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Write() error
	WriteSync() error
	Close() error
	Replay(func(batch.Entry) error) error
	GetByteSize() (int, error)
}

// DB is the public TreeDB handle. It can represent either cached mode (default)
// or backend-only mode depending on Options.
type DB struct {
	mode           Mode
	cached         *caching.DB
	backend        *db.DB
	dictdb         *db.DB
	bgComp         bgCompactionWorker
	bgVac          bgIndexVacuumWorker
	notifyError    func(error)
	bgErrMu        sync.Mutex
	bgErr          error
	durabilityMode string
	dir            string
}

func (db *DB) ensureOpen() error {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return ErrClosed
	}
	return nil
}

// Open opens TreeDB. By default it enables caching (write-back layer).
// To open the backend-only engine, set opts.Mode = ModeBackend.
func Open(opts Options) (*DB, error) {
	// Cached mode writes to the backend in large flush batches, so commit sequence
	// advances much more slowly than "number of writes". A large KeepRecent value
	// can therefore delay page reuse for a very long time (and cause index.db to
	// balloon under update-heavy workloads). Default to aggressive reuse in cached
	// mode unless the caller specifies otherwise.
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 64 * 1024 * 1024
	}
	if opts.KeepRecent == 0 && opts.Mode != ModeBackend {
		opts.KeepRecent = 1
	}
	if opts.DisableValueLog || opts.DisableWAL {
		opts.MemtableValueLogPointers = false
		opts.SplitValueLog = false
	}
	if opts.ReadOnly {
		// Read-only opens are backend-only: the caching layer creates and rotates
		// WAL segments (writes) and runs background maintenance loops.
		opts.Mode = ModeBackend
	}

	rootDir := opts.Dir
	maindbDir := filepath.Join(rootDir, "maindb")
	dictdbDir := filepath.Join(rootDir, "dictdb")
	if opts.ReadOnly {
		if _, err := os.Stat(maindbDir); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("treedb: maindb directory missing for read-only open: %s", maindbDir)
			}
			return nil, err
		}
		if _, err := os.Stat(dictdbDir); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("treedb: dictdb directory missing for read-only open: %s", dictdbDir)
			}
			return nil, err
		}
	} else {
		if err := os.MkdirAll(maindbDir, 0755); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(dictdbDir, 0755); err != nil {
			return nil, err
		}
	}

	dictOpts := opts
	dictOpts.Dir = dictdbDir
	dictOpts.Mode = ModeBackend
	dictOpts.DisableBackgroundPrune = true
	dictOpts.SlabCompression = slab.CompressionOptions{Kind: slab.CompressionNone}
	dictOpts.DictLookup = nil
	dictBackend, err := db.Open(dictOpts)
	if err != nil {
		return nil, err
	}
	dictStore := dictdb.New(dictBackend)

	opts.DictLookup = func(dictID uint64) ([]byte, error) {
		return dictStore.GetDictBytes(context.Background(), dictID)
	}
	opts.Dir = maindbDir
	backend, err := db.Open(opts)
	if err != nil {
		_ = dictBackend.Close()
		return nil, err
	}

	if opts.Mode == ModeBackend {
		return &DB{mode: ModeBackend, backend: backend, dictdb: dictBackend, notifyError: opts.NotifyError, durabilityMode: computeDurabilityMode(opts), dir: rootDir}, nil
	}

	if opts.SlowdownBacklogSeconds < 0 {
		opts.SlowdownBacklogSeconds = 0
	}
	if opts.StopBacklogSeconds < 0 {
		opts.StopBacklogSeconds = 0
	}
	if opts.MaxBacklogBytes < 0 {
		opts.MaxBacklogBytes = 0
	}
	if opts.SlowdownBacklogSeconds == 0 && opts.StopBacklogSeconds == 0 && opts.MaxBacklogBytes == 0 {
		opts.SlowdownBacklogSeconds = 1
		opts.StopBacklogSeconds = 2
		opts.MaxBacklogBytes = 2 << 30
	}
	if opts.MemtableMode == "" {
		opts.MemtableMode = "adaptive"
	}

	cached, err := caching.Open(opts.Dir, backend, caching.Options{
		FlushThreshold:                     opts.FlushThreshold,
		MemtableMode:                       opts.MemtableMode,
		MemtableShards:                     opts.MemtableShards,
		MaxQueuedMemtables:                 opts.MaxQueuedMemtables,
		SlowdownBacklogSeconds:             opts.SlowdownBacklogSeconds,
		StopBacklogSeconds:                 opts.StopBacklogSeconds,
		MaxBacklogBytes:                    opts.MaxBacklogBytes,
		WriterFlushMaxMemtables:            opts.WriterFlushMaxMemtables,
		WriterFlushMaxDuration:             opts.WriterFlushMaxDuration,
		FlushBuildConcurrency:              opts.FlushBuildConcurrency,
		DisableWAL:                         opts.DisableWAL,
		DisableValueLog:                    opts.DisableValueLog,
		SplitValueLog:                      opts.SplitValueLog,
		JournalLanes:                       opts.JournalLanes,
		WALMaxSegmentBytes:                 opts.WALMaxSegmentBytes,
		RelaxedSync:                        opts.RelaxedSync,
		DisableReadChecksum:                opts.DisableReadChecksum,
		MemtableValueLogPointers:           opts.MemtableValueLogPointers,
		ValueLogPointerThreshold:           opts.ValueLogPointerThreshold,
		ValueLogDictTrain:                  opts.ValueLogDictTrain,
		ValueLogDictAdaptiveRatio:          opts.ValueLogDictAdaptiveRatio,
		ValueLogDictMetricsWindowBytes:     opts.ValueLogDictMetricsWindowBytes,
		ValueLogDictMetricsMinRecords:      opts.ValueLogDictMetricsMinRecords,
		ValueLogDictMetricsPauseBytes:      opts.ValueLogDictMetricsPauseBytes,
		ValueLogDictMinPayloadSavingsRatio: opts.ValueLogDictMinPayloadSavingsRatio,
		AllowUnsafe:                        opts.AllowUnsafe,
		MaxValueLogRetainedBytes:           opts.MaxValueLogRetainedBytes,
		MaxValueLogRetainedBytesHard:       opts.MaxValueLogRetainedBytesHard,
		NotifyError:                        opts.NotifyError,
	})
	if err != nil {
		_ = backend.Close()
		_ = dictBackend.Close()
		return nil, err
	}

	cached.SetDictStore(dictStore)
	out := &DB{mode: ModeCached, cached: cached, backend: backend, dictdb: dictBackend, notifyError: opts.NotifyError, durabilityMode: computeDurabilityMode(opts), dir: rootDir}

	// Cached-mode auto checkpointing is enabled by default to keep `wal/` growth
	// bounded for long-running workloads, aligning operational expectations with
	// typical LSM engines (log segments do not grow without bound).
	autoInterval := opts.BackgroundCheckpointInterval
	if autoInterval == 0 {
		autoInterval = 30 * time.Second
	}
	if autoInterval < 0 {
		autoInterval = 0
	}
	maxWALBytes := opts.MaxWALBytes
	if maxWALBytes == 0 {
		maxWALBytes = 2 << 30 // 2GiB
	}
	if maxWALBytes < 0 {
		maxWALBytes = 0
	}
	idleInterval := opts.BackgroundCheckpointIdleDuration
	if idleInterval == 0 {
		idleInterval = 2 * time.Second
	}
	if idleInterval < 0 {
		idleInterval = 0
	}
	// Auto checkpointing only manages cached-mode WAL segments. If WAL is
	// disabled, skip starting the background loop to avoid unnecessary work.
	if !opts.DisableWAL && (autoInterval > 0 || maxWALBytes > 0 || idleInterval > 0) {
		cached.StartAutoCheckpoint(autoInterval, maxWALBytes, idleInterval)
	}

	// Background compaction is opt-in (interval > 0).
	if opts.BackgroundCompactionInterval > 0 {
		co := compaction.Options{
			MaxSlabs:           opts.BackgroundCompactionMaxSlabs,
			DeadRatioThreshold: opts.BackgroundCompactionDeadRatio,
			MinTotalBytes:      opts.BackgroundCompactionMinBytes,
			MicroBatchSize:     opts.BackgroundCompactionMicroBatch,
			CopyBytesPerSec:    opts.BackgroundCompactionCopyBytesPerSec,
			CopyBurstBytes:     opts.BackgroundCompactionCopyBurstBytes,
			RotateBeforeWrite:  opts.BackgroundCompactionRotateBeforeWrite,
			IndexSwap:          opts.BackgroundCompactionIndexSwap,
		}
		// Reasonable effective defaults for background mode.
		if co.MaxSlabs == 0 {
			co.MaxSlabs = 1
		}
		if co.DeadRatioThreshold == 0 {
			co.DeadRatioThreshold = 0.10
		}
		if co.MinTotalBytes == 0 {
			co.MinTotalBytes = 1
		}
		if co.MicroBatchSize == 0 {
			co.MicroBatchSize = 256
		}
		out.bgComp.Start(out, opts.BackgroundCompactionInterval, co)
	}

	vacuumInterval := opts.BackgroundIndexVacuumInterval
	if vacuumInterval == 0 {
		vacuumInterval = 30 * time.Second
	}
	if vacuumInterval < 0 {
		vacuumInterval = 0
	}
	if vacuumInterval > 0 {
		spanRatioPPM := opts.BackgroundIndexVacuumSpanRatioPPM
		if spanRatioPPM == 0 {
			spanRatioPPM = defaultBackgroundIndexVacuumSpanRatioPPM
		}
		out.bgVac.Start(out, vacuumInterval, spanRatioPPM)
	}

	return out, nil
}

func computeDurabilityMode(opts Options) string {
	if opts.ReadOnly {
		return "read_only"
	}
	mode := "durable"
	if opts.Mode == ModeBackend {
		if opts.RelaxedSync {
			mode = "backend_relaxed_sync"
		} else {
			mode = "backend_sync"
		}
	} else {
		if opts.DisableWAL {
			if opts.RelaxedSync {
				mode = "wal_disabled_relaxed_sync"
			} else {
				mode = "wal_disabled_sync"
			}
		} else if opts.RelaxedSync {
			mode = "wal_relaxed_sync"
		} else {
			mode = "wal_sync"
		}
	}
	if opts.DisableReadChecksum {
		mode += "+no_read_checksum"
	}
	if opts.VerifyOnRead {
		mode += "+verify_on_read"
	}
	if opts.DisableSlabTailRepairOnOpen {
		mode += "+no_slab_tail_repair"
	}
	return mode
}

const (
	envCloseCheckpoint        = "TREEDB_CLOSE_CHECKPOINT"
	envCloseCompactIndex      = "TREEDB_CLOSE_COMPACT_INDEX"
	envCloseVacuumIndexOnline = "TREEDB_CLOSE_VACUUM_INDEX_ONLINE"
	envCloseVacuumTimeout     = "TREEDB_CLOSE_VACUUM_TIMEOUT"
	envCloseLog               = "TREEDB_CLOSE_LOG"
	envCloseScopeContains     = "TREEDB_CLOSE_SCOPE_CONTAINS"
)

func envBool(name string) bool {
	val, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	if val == "" {
		return true
	}
	parsed, err := strconv.ParseBool(val)
	return err == nil && parsed
}

func envDuration(name string, def time.Duration) time.Duration {
	val, ok := os.LookupEnv(name)
	if !ok || val == "" {
		return def
	}
	if d, err := time.ParseDuration(val); err == nil {
		return d
	}
	if secs, err := strconv.Atoi(val); err == nil {
		return time.Duration(secs) * time.Second
	}
	return def
}

func (db *DB) closeMaintenanceEnabled() bool {
	scope := os.Getenv(envCloseScopeContains)
	if scope == "" {
		return true
	}
	if db == nil {
		return false
	}
	if db.dir == "" {
		return true
	}
	return strings.Contains(db.dir, scope)
}

func (db *DB) closeMaintenance() error {
	logEnabled := envBool(envCloseLog)
	if !db.closeMaintenanceEnabled() {
		if logEnabled {
			log.Printf("treedb: close maintenance skipped dir=%q", db.dir)
		}
		return nil
	}
	var err error
	if envBool(envCloseCheckpoint) {
		if logEnabled {
			log.Printf("treedb: close checkpoint start")
		}
		if e := db.Checkpoint(); e != nil {
			err = errors.Join(err, e)
		}
		if logEnabled {
			log.Printf("treedb: close checkpoint done")
		}
	}
	if envBool(envCloseCompactIndex) {
		if logEnabled {
			log.Printf("treedb: close compact index start")
		}
		if e := db.CompactIndex(); e != nil {
			err = errors.Join(err, e)
		}
		if logEnabled {
			log.Printf("treedb: close compact index done")
		}
	}
	if envBool(envCloseVacuumIndexOnline) {
		timeout := envDuration(envCloseVacuumTimeout, 30*time.Minute)
		if logEnabled {
			log.Printf("treedb: close vacuum index online start timeout=%s", timeout)
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if e := db.VacuumIndexOnline(ctx); e != nil {
			err = errors.Join(err, e)
		}
		cancel()
		if logEnabled {
			log.Printf("treedb: close vacuum index online done")
		}
	}
	return err
}

// OpenCached is an explicit cached-mode opener (alias of Open with ModeCached).
func OpenCached(opts Options) (*DB, error) {
	opts.Mode = ModeCached
	return Open(opts)
}

// OpenBackend opens TreeDB in backend-only mode (no caching).
func OpenBackend(opts Options) (*DB, error) {
	opts.Mode = ModeBackend
	// Backend-only mode is primarily used for correctness tests and profiling the
	// core engine. Keep background pruning off by default to avoid introducing
	// concurrent allocator work into single-op write benchmarks (callers can opt
	// in by using Open with ModeBackend).
	opts.DisableBackgroundPrune = true
	return Open(opts)
}

// Close closes the DB.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.bgVac.Stop()
	db.bgComp.Stop()
	var err error
	if db.cached != nil || db.backend != nil {
		if e := db.closeMaintenance(); e != nil {
			err = errors.Join(err, e)
		}
	}

	// Close cached layer first if present
	if db.cached != nil {
		err = errors.Join(err, db.cached.Close())
		db.cached = nil
	}

	// Always close backend if present
	if db.backend != nil {
		err = errors.Join(err, db.backend.Close())
		db.backend = nil
	}
	if db.dictdb != nil {
		err = errors.Join(err, db.dictdb.Close())
		db.dictdb = nil
	}

	return errors.Join(err, db.backgroundError())
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

// Get returns the value for a key.
//
// Semantics: Returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.Get(key)
	}
	return db.backend.Get(key)
}

// GetUnsafe returns the value for a key.
//
// Semantics: Returns a safe copy of the value. For zero-copy views tied to a
// snapshot lifetime, use AcquireSnapshot().GetUnsafe.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// GetAppend appends the value for the key to dst and returns the new slice.
// It avoids internal allocations by using the provided buffer.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return dst, err
	}
	if db.cached != nil {
		return db.cached.GetAppend(key, dst)
	}
	return db.backend.GetAppend(key, dst)
}

// Has reports whether a key exists in the database.
func (db *DB) Has(key []byte) (bool, error) {
	if err := db.ensureOpen(); err != nil {
		return false, err
	}
	if db.cached != nil {
		return db.cached.Has(key)
	}
	return db.backend.Has(key)
}

// Set writes a key/value pair without forcing an fsync boundary.
func (db *DB) Set(key, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Set(key, value)
	}
	return db.backend.Set(key, value)
}

// SetSync writes a key/value pair and forces a durability boundary.
// With Options.RelaxedSync enabled, Sync operations are crash-consistent
// only (no fsync) and may not survive power loss.
func (db *DB) SetSync(key, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.SetSync(key, value)
	}
	return db.backend.SetSync(key, value)
}

// Delete removes a key without forcing an fsync boundary.
func (db *DB) Delete(key []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Delete(key)
	}
	return db.backend.Delete(key)
}

// DeleteRange removes all keys in the range [start, end).
//
// This is primarily used by benchmark suites and maintenance tooling. In cached
// mode, it may use fast paths that avoid per-key tombstones when safe.
func (db *DB) DeleteRange(start, end []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.DeleteRange(start, end)
	}
	// Backend-only mode: fall back to iterating keys and issuing deletes.
	it, err := db.backend.Iterator(start, end)
	if err != nil {
		return err
	}
	defer it.Close()

	b := db.backend.NewBatch()
	defer b.Close()
	for it.Valid() {
		if !it.IsDeleted() {
			if err := b.Delete(it.UnsafeKey()); err != nil {
				return err
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return b.Write()
}

// DeleteSync removes a key and forces a durability boundary.
func (db *DB) DeleteSync(key []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.DeleteSync(key)
	}
	return db.backend.DeleteSync(key)
}

// Iterator returns a forward iterator over the range [start, end).
func (db *DB) Iterator(start, end []byte) (Iterator, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.Iterator(start, end)
	}
	return db.backend.Iterator(start, end)
}

// ReverseIterator returns a reverse iterator over the range [start, end).
func (db *DB) ReverseIterator(start, end []byte) (Iterator, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.ReverseIterator(start, end)
	}
	return db.backend.ReverseIterator(start, end)
}

// NewBatch creates a new batch for buffered writes.
func (db *DB) NewBatch() Batch {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		return db.cached.NewBatch()
	}
	return db.backend.NewBatch()
}

// NewBatchWithSize creates a new batch with a hint for the expected entry size.
func (db *DB) NewBatchWithSize(size int) Batch {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		return db.cached.NewBatchWithSize(size)
	}
	return db.backend.NewBatchWithSize(size)
}

// Snapshot is a consistent point-in-time view of the database.
type Snapshot = db.Snapshot

// AcquireSnapshot returns a new snapshot.
func (db *DB) AcquireSnapshot() *Snapshot {
	if db == nil || db.backend == nil {
		return nil
	}
	return db.backend.AcquireSnapshot()
}

// Stats returns diagnostic stats for the active backend and cached layer.
func (db *DB) Stats() map[string]string {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		stats := db.cached.Stats()
		if stats == nil {
			stats = make(map[string]string)
		}
		stats["treedb.durability_mode"] = db.durabilityMode
		bgCompactionStatsInto(stats, &db.bgComp)
		bgIndexVacuumStatsInto(stats, &db.bgVac)
		return stats
	}
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	stats["treedb.durability_mode"] = db.durabilityMode
	bgCompactionStatsInto(stats, &db.bgComp)
	bgIndexVacuumStatsInto(stats, &db.bgVac)
	return stats
}

// DurabilityMode reports the effective durability/integrity policy string.
func (db *DB) DurabilityMode() string {
	if db == nil {
		return ""
	}
	return db.durabilityMode
}

// Print dumps best-effort debug output for the underlying backend.
func (db *DB) Print() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Print()
	}
	return db.backend.Print()
}

// Checkpoint forces a durable backend boundary and trims cached-mode WAL
// segments, so long-running cached-mode workloads do not accumulate unbounded
// `wal/` growth.
//
// In cached mode this flushes queued memtables with backend sync and resets the
// WAL to a fresh segment. In backend mode it forces a sync boundary.
func (db *DB) Checkpoint() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Checkpoint()
	}
	b := db.backend.NewBatch()
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		return err
	}
	return b.Close()
}

// CompactCandidates runs slab compaction based on the provided selection options.
// In cached mode it will also perform bounded flush assist when the caching layer
// is under backpressure, so compaction does not starve the foreground flush path.
func (db *DB) CompactCandidates(opts compaction.Options) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.backend == nil {
		return ErrClosed
	}

	if db.cached != nil {
		// Kick the flusher once up front, and wire compaction to periodically
		// perform bounded flush assist when backlog is high.
		db.cached.CompactionAssist()
		userAssist := opts.Assist
		opts.Assist = func() {
			db.cached.CompactionAssist()
			if userAssist != nil {
				userAssist()
			}
		}
	}

	c := compaction.New(db.backend)
	return c.CompactCandidates(opts)
}

// CompactIndex performs an in-place index vacuum (bulk rebuild) on the backend.
// In cached mode it first drains the caching layer so the backend reflects all
// buffered writes before rebuilding.
func (db *DB) CompactIndex() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.backend == nil {
		return ErrClosed
	}

	if db.cached != nil {
		if err := db.cached.Drain(); err != nil {
			return err
		}
	}
	return db.backend.CompactIndex()
}

// VacuumIndexOnline rebuilds the user index into a new file and swaps it in with
// a short writer pause. Disk space from the old index is reclaimed once any old
// snapshots/iterators drain.
func (db *DB) VacuumIndexOnline(ctx context.Context) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.backend == nil {
		return ErrClosed
	}

	// In cached mode, ensure the backend reflects all buffered writes before
	// rebuilding/switching the index file. This avoids exposing a backend state
	// that temporarily "forgets" keys that only existed in memtables/WAL, which
	// can break higher layers that assume a stable durable boundary (e.g. IAVL
	// node storage during version application).
	if db.cached != nil {
		if err := db.cached.Checkpoint(); err != nil {
			return err
		}
	}

	return db.backend.VacuumIndexOnline(ctx)
}

// VacuumIndexOffline rewrites `index.db` into a fresh file and swaps it in.
// This is intended to reclaim space and restore locality after long churn.
//
// It is an offline operation: it acquires the exclusive open lock for opts.Dir.
func VacuumIndexOffline(opts Options) error {
	return db.VacuumIndexOffline(opts)
}

// FragmentationReport returns best-effort structural stats about the on-disk user
// index that help diagnose scan regressions after churn.
//
// Note: In cached mode this reflects the backend state only; queued memtables are
// not included unless the caller has explicitly drained the cache (e.g. via
// close+reopen or a maintenance operation that drains).
func (db *DB) FragmentationReport() (map[string]string, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.backend == nil {
		return nil, ErrClosed
	}
	return db.backend.FragmentationReport()
}
