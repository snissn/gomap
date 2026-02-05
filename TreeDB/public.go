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
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/template"
)

// Options configures TreeDB. It is re-exported from TreeDB/db for convenience.
type Options = db.Options

const (
	defaultChunkSize     = 16 * 1024 * 1024
	defaultDictChunkSize = 1 * 1024 * 1024
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

// DB is the public TreeDB handle (cached mode by default; read-only opens skip caching).
type DB struct {
	cached         *caching.DB
	backend        *db.DB
	dictdb         *db.DB
	templateDB     *DB
	writePath      writePathInfo
	bgVac          bgIndexVacuumWorker
	notifyError    func(error)
	bgErrMu        sync.Mutex
	bgErr          error
	durabilityMode string
	dir            string
}

type writePathInfo struct {
	mode       string
	valueStore string
	redoLog    string
	warn       bool
}

func writePathFromOptions(opts Options) writePathInfo {
	info := writePathInfo{
		mode:       "cached",
		valueStore: "value_log",
		redoLog:    "on",
	}
	if opts.ReadOnly {
		info.mode = "readonly"
	}
	if opts.Durability == db.DurabilityWALOffRelaxed {
		info.redoLog = "off"
	}
	return info
}

func writePathStatsInto(stats map[string]string, info writePathInfo) {
	if stats == nil {
		return
	}
	stats["treedb.write_path.mode"] = info.mode
	stats["treedb.write_path.value_store"] = info.valueStore
	stats["treedb.write_path.redo_log"] = info.redoLog
}

func (db *DB) ensureOpen() error {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return ErrClosed
	}
	return nil
}

// Open opens TreeDB. By default it enables caching (write-back layer).
func Open(opts Options) (*DB, error) {
	// Cached mode writes to the backend in large flush batches, so commit sequence
	// advances much more slowly than "number of writes". A large KeepRecent value
	// can therefore delay page reuse for a very long time (and cause index.db to
	// balloon under update-heavy workloads). Default to aggressive reuse in cached
	// mode unless the caller specifies otherwise.
	chunkSizeDefaulted := opts.ChunkSize == 0
	if chunkSizeDefaulted {
		opts.ChunkSize = defaultChunkSize
	}
	dictChunkSize := opts.DictDBChunkSize
	if dictChunkSize <= 0 {
		dictChunkSize = defaultDictChunkSize
	}
	templateChunkSize := opts.TemplateDBChunkSize
	if templateChunkSize <= 0 {
		templateChunkSize = defaultDictChunkSize
	}
	if opts.KeepRecent == 0 && !opts.ReadOnly {
		opts.KeepRecent = 1
	}

	writePath := writePathFromOptions(opts)
	if envBool(envWritePathLog) {
		fmt.Fprintf(os.Stderr, "treedb write_path mode=%s value_store=%s redo_log=%s\n", writePath.mode, writePath.valueStore, writePath.redoLog)
	}

	rootDir := opts.Dir
	maindbDir := filepath.Join(rootDir, "maindb")
	dictdbDir := filepath.Join(rootDir, "dictdb")
	templatedbDir := filepath.Join(rootDir, "templatedb")
	if opts.DisableSideStores {
		maindbDir = rootDir
		dictdbDir = ""
		templatedbDir = ""
	}
	if opts.ReadOnly {
		if _, err := os.Stat(maindbDir); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("treedb: maindb directory missing for read-only open: %s", maindbDir)
			}
			return nil, err
		}
		if !opts.DisableSideStores {
			if _, err := os.Stat(dictdbDir); err != nil {
				if os.IsNotExist(err) {
					return nil, fmt.Errorf("treedb: dictdb directory missing for read-only open: %s", dictdbDir)
				}
				return nil, err
			}
			if opts.ValueLog.TemplateMode != template.TemplateOff {
				if _, err := os.Stat(templatedbDir); err != nil {
					if os.IsNotExist(err) {
						return nil, fmt.Errorf("treedb: templatedb directory missing for read-only open: %s", templatedbDir)
					}
					return nil, err
				}
			}
		}
	} else {
		if err := os.MkdirAll(maindbDir, 0755); err != nil {
			return nil, err
		}
		if !opts.DisableSideStores {
			if err := os.MkdirAll(dictdbDir, 0755); err != nil {
				return nil, err
			}
			if opts.ValueLog.TemplateMode != template.TemplateOff {
				if err := os.MkdirAll(templatedbDir, 0755); err != nil {
					return nil, err
				}
			}
		}
	}

	var dictBackend *db.DB
	var dictStore *dictdb.Store
	var templateDB *DB
	var templateStore *templatedb.Store
	if !opts.DisableSideStores {
		dictOpts := opts
		dictOpts.Dir = dictdbDir
		dictOpts.DisableBackgroundPrune = true
		dictOpts.ValueLog.DictLookup = nil
		dictOpts.ValueLog.DictTrain = TrainConfig{TrainBytes: -1}
		// dictdb stores small metadata values (e.g. current dict id, hash->id map)
		// inline. ForcePointers would set InlineThreshold=0 and break these writes.
		dictOpts.ValueLog.ForcePointers = false
		dictOpts.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
		dictOpts.ChunkSize = dictChunkSize
		var err error
		dictBackend, err = db.Open(dictOpts)
		if err != nil {
			return nil, err
		}
		dictStore = dictdb.New(dictBackend)

		opts.ValueLog.DictLookup = func(dictID uint64) ([]byte, error) {
			return dictStore.GetDictBytes(context.Background(), dictID)
		}
	}

	if !opts.DisableSideStores && opts.ValueLog.TemplateMode != template.TemplateOff {
		templateOpts := opts
		templateOpts.Dir = templatedbDir
		templateOpts.DisableSideStores = true
		templateOpts.DisableBackgroundPrune = true
		templateOpts.ValueLog.DictLookup = nil
		templateOpts.ValueLog.DictTrain = TrainConfig{TrainBytes: -1}
		// templatedb uses batch.Set for small routing/index entries. Do not
		// propagate ForcePointers from the main DB into this internal store.
		templateOpts.ValueLog.ForcePointers = false
		templateOpts.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
		templateOpts.ValueLog.TemplateMode = template.TemplateOff
		templateOpts.ValueLog.TemplateLookup = nil
		templateOpts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}
		templateOpts.ChunkSize = templateChunkSize

		var err error
		templateDB, err = Open(templateOpts)
		if err != nil {
			if dictBackend != nil {
				_ = dictBackend.Close()
			}
			return nil, err
		}

		tcfg := template.NormalizeConfig(opts.ValueLog.TemplateConfig)
		templateStore = templatedb.New(templateKV{db: templateDB}, templatedb.Config{
			MaxCandidatesPerFP:    tcfg.MaxCandidatesPerFP,
			MaxCandidateListBytes: tcfg.MaxCandidateListBytes,
		})
		decodeOpts := template.DecodeOptions{MaxGaps: tcfg.MaxGaps, MaxDecodedBytes: tcfg.MaxDecodedBytes, DefCacheSize: tcfg.DefCacheSize}
		if decodeOpts.MaxDecodedBytes <= 0 && limits.MaxRecordSize > 0 {
			decodeOpts.MaxDecodedBytes = int(limits.MaxRecordSize)
		}
		opts.ValueLog.TemplateLookup = func(templateID uint64) ([]byte, error) {
			return templateStore.GetTemplateDef(context.Background(), templateID)
		}
		opts.ValueLog.TemplateDecodeOptions = decodeOpts
	}
	opts.Dir = maindbDir
	backend, err := db.Open(opts)
	if err != nil {
		if dictBackend != nil {
			_ = dictBackend.Close()
		}
		if templateDB != nil {
			_ = templateDB.Close()
		}
		return nil, err
	}

	if opts.ReadOnly {
		return &DB{backend: backend, dictdb: dictBackend, templateDB: templateDB, writePath: writePath, notifyError: opts.NotifyError, durabilityMode: computeDurabilityMode(opts), dir: rootDir}, nil
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

	disableWAL := opts.Durability == db.DurabilityWALOffRelaxed
	relaxedSync := opts.Durability != db.DurabilityDurable
	disableReadChecksum := opts.ValueLog.ReadIntegrity == db.IntegritySkipChecksums
	allowUnsafe := disableWAL || relaxedSync || disableReadChecksum

	cached, err := caching.Open(opts.Dir, backend, caching.Options{
		FlushThreshold:                            opts.FlushThreshold,
		MemtableMode:                              opts.MemtableMode,
		MemtableShards:                            opts.MemtableShards,
		MaxQueuedMemtables:                        opts.MaxQueuedMemtables,
		SlowdownBacklogSeconds:                    opts.SlowdownBacklogSeconds,
		StopBacklogSeconds:                        opts.StopBacklogSeconds,
		MaxBacklogBytes:                           opts.MaxBacklogBytes,
		WriterFlushMaxMemtables:                   opts.WriterFlushMaxMemtables,
		WriterFlushMaxDuration:                    opts.WriterFlushMaxDuration,
		FlushBuildConcurrency:                     opts.FlushBuildConcurrency,
		FlushBuildMinEntries:                      opts.FlushBuildMinEntries,
		FlushBuildMinUnits:                        opts.FlushBuildMinUnits,
		FlushBuildChunkCap:                        opts.FlushBuildChunkCap,
		FlushBuildChunkTargetBytes:                opts.FlushBuildChunkTargetBytes,
		FlushBuildChunkMinBytes:                   opts.FlushBuildChunkMinBytes,
		FlushBuildChunkMaxBytes:                   opts.FlushBuildChunkMaxBytes,
		FlushBuildPrefetchUnits:                   opts.FlushBuildPrefetchUnits,
		FlushBackendMaxEntries:                    opts.FlushBackendMaxEntries,
		FlushBackendMaxBatches:                    opts.FlushBackendMaxBatches,
		DisableWAL:                                disableWAL,
		JournalLanes:                              opts.JournalLanes,
		WALMaxSegmentBytes:                        opts.WALMaxSegmentBytes,
		JournalCompression:                        opts.JournalCompression,
		RelaxedSync:                               relaxedSync,
		DisableReadChecksum:                       disableReadChecksum,
		ValueLogPointerThreshold:                  opts.ValueLog.PointerThreshold,
		ForceValueLogPointers:                     opts.ValueLog.ForcePointers,
		ValueLogDictTrain:                         opts.ValueLog.DictTrain,
		ValueLogDictMaxK:                          opts.ValueLog.DictMaxK,
		ValueLogDictFrameEncodeLevel:              opts.ValueLog.DictFrameEncodeLevel,
		ValueLogDictFrameEnableEntropy:            opts.ValueLog.DictFrameEnableEntropy,
		ValueLogDictFramePipelineWorkers:          opts.ValueLog.DictFramePipelineWorkers,
		ValueLogDictFramePipelineMaxInFlightBytes: opts.ValueLog.DictFramePipelineMaxInFlightBytes,
		ValueLogDictAdaptiveRatio:                 opts.ValueLog.DictAdaptiveRatio,
		ValueLogDictMetricsWindowBytes:            opts.ValueLog.DictMetricsWindowBytes,
		ValueLogDictMetricsMinRecords:             opts.ValueLog.DictMetricsMinRecords,
		ValueLogDictMetricsPauseBytes:             opts.ValueLog.DictMetricsPauseBytes,
		ValueLogDictMinPayloadSavingsRatio:        opts.ValueLog.DictMinPayloadSavingsRatio,
		ValueLogCompressionAutotune:               opts.ValueLog.CompressionAutotune,
		ValueLogTemplateMode:                      opts.ValueLog.TemplateMode,
		ValueLogTemplateConfig:                    opts.ValueLog.TemplateConfig,
		ValueLogTemplateReadStrict:                opts.ValueLog.TemplateReadStrict,
		AllowUnsafe:                               allowUnsafe,
		MaxValueLogRetainedBytes:                  opts.ValueLog.MaxRetainedBytes,
		MaxValueLogRetainedBytesHard:              opts.ValueLog.MaxRetainedBytesHard,
		NotifyError:                               opts.NotifyError,
	})
	if err != nil {
		_ = backend.Close()
		if dictBackend != nil {
			_ = dictBackend.Close()
		}
		if templateDB != nil {
			_ = templateDB.Close()
		}
		return nil, err
	}

	cached.SetDictStore(dictStore)
	cached.SetTemplateStore(templateStore)
	out := &DB{cached: cached, backend: backend, dictdb: dictBackend, templateDB: templateDB, writePath: writePath, notifyError: opts.NotifyError, durabilityMode: computeDurabilityMode(opts), dir: rootDir}

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
	// Auto checkpointing only manages cached-mode journal segments. If WAL is
	// disabled, skip starting the background loop to avoid unnecessary work.
	if !disableWAL && (autoInterval > 0 || maxWALBytes > 0 || idleInterval > 0) {
		cached.StartAutoCheckpoint(autoInterval, maxWALBytes, idleInterval)
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
	mode := "wal_on_sync"
	switch opts.Durability {
	case db.DurabilityDurable:
		mode = "wal_on_sync"
	case db.DurabilityWALOnRelaxed:
		mode = "wal_on_relaxed_sync"
	case db.DurabilityWALOffRelaxed:
		mode = "wal_off_relaxed_sync"
	default:
		mode = fmt.Sprintf("durability_%d", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity == db.IntegritySkipChecksums {
		mode += "+no_read_checksum"
	}
	if opts.VerifyOnRead {
		mode += "+verify_on_read"
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
	envWritePathLog           = "TREEDB_WRITE_PATH_LOG"
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

// Close closes the DB.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.bgVac.Stop()
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
	if db.templateDB != nil {
		err = errors.Join(err, db.templateDB.Close())
		db.templateDB = nil
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
// With DurabilityWALOnRelaxed or DurabilityWALOffRelaxed enabled, Sync operations are
// crash-consistent only (no fsync) and may not survive power loss.
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
		writePathStatsInto(stats, db.writePath)
		stats["treedb.durability_mode"] = db.durabilityMode
		bgIndexVacuumStatsInto(stats, &db.bgVac)
		return stats
	}
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	writePathStatsInto(stats, db.writePath)
	stats["treedb.durability_mode"] = db.durabilityMode
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
	// can break higher layers that assume a stable durable boundary.
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
	maindbDir, err := resolveMainDBDir(opts.Dir)
	if err != nil {
		return err
	}
	opts.Dir = maindbDir
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
