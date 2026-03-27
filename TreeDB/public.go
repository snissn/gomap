package treedb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/compress/zstd"
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

type MaintenancePhase = caching.MaintenancePhase

const (
	MaintenancePhaseSteady  = caching.MaintenancePhaseSteady
	MaintenancePhaseRestore = caching.MaintenancePhaseRestore
	MaintenancePhaseCatchUp = caching.MaintenancePhaseCatchUp
)

var errVacuumUnsupported = db.ErrVacuumUnsupported

const (
	defaultChunkSize     = 256 * 1024
	defaultDictChunkSize = 64 * 1024
	// Template side-store pages intentionally use the same default size as dictdb
	// pages so both auxiliary stores behave consistently under the unified layout.
	defaultTemplateChunkSize = defaultDictChunkSize

	defaultSlowdownBacklogSeconds        = 1.0
	defaultStopBacklogSeconds            = 2.0
	defaultAdaptiveMaxBacklogBytes int64 = 2 << 30
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

type getManyPlanner interface {
	GetManyParallelPlan(keyCount int) (workers int, parallel bool)
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
	maintenance    maintenanceCoordinator
}

type writePathInfo struct {
	mode       string
	valueStore string
	redoLog    string
	warn       bool
}

type maintenanceCoordinator struct {
	mu sync.Mutex

	active atomic.Int32

	deferrals atomic.Uint64
	waitTotal atomic.Int64
	waitMax   atomic.Int64

	gcRuns     atomic.Uint64
	vacuumRuns atomic.Uint64

	lastGCAt     atomic.Int64
	lastVacuumAt atomic.Int64
}

const (
	maintenanceOpNone int32 = iota
	maintenanceOpGC
	maintenanceOpVacuum
	maintenanceOpOther
)

func maintenanceOpCode(op string) int32 {
	switch op {
	case "gc":
		return maintenanceOpGC
	case "vacuum":
		return maintenanceOpVacuum
	case "":
		return maintenanceOpNone
	default:
		return maintenanceOpOther
	}
}

func maintenanceActiveLabel(code int32) string {
	switch code {
	case maintenanceOpGC:
		return "gc"
	case maintenanceOpVacuum:
		return "vacuum"
	case maintenanceOpOther:
		return "other"
	default:
		return ""
	}
}

func atomicStoreMaxInt64(dst *atomic.Int64, value int64) {
	for {
		cur := dst.Load()
		if value <= cur {
			return
		}
		if dst.CompareAndSwap(cur, value) {
			return
		}
	}
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

func forceTemplateCompressionOff(opts *Options) {
	if opts == nil {
		return
	}
	// Template compression remains experimental; keep runtime paths disabled so
	// normal TreeDB opens only exercise dict/block/off value-log compression.
	opts.ValueLog.TemplateMode = template.TemplateOff
	opts.ValueLog.TemplateReadStrict = false
	opts.ValueLog.TemplateLookup = nil
	opts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}
}

func (db *DB) ensureOpen() error {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return ErrClosed
	}
	return nil
}

func (db *DB) beginFullScanMaintenance(op string) (time.Duration, func(success bool)) {
	if db == nil {
		return 0, func(bool) {}
	}
	wait := time.Duration(0)
	if db.maintenance.mu.TryLock() {
		// uncontended fast path
	} else {
		waitStart := time.Now()
		db.maintenance.mu.Lock()
		wait = time.Since(waitStart)
		db.maintenance.deferrals.Add(1)
		db.maintenance.waitTotal.Add(wait.Nanoseconds())
		atomicStoreMaxInt64(&db.maintenance.waitMax, wait.Nanoseconds())
	}
	db.maintenance.active.Store(maintenanceOpCode(op))

	return wait, func(success bool) {
		if success {
			now := time.Now().UnixNano()
			switch op {
			case "gc":
				db.maintenance.gcRuns.Add(1)
				db.maintenance.lastGCAt.Store(now)
			case "vacuum":
				db.maintenance.vacuumRuns.Add(1)
				db.maintenance.lastVacuumAt.Store(now)
			}
		}
		db.maintenance.active.Store(maintenanceOpNone)
		db.maintenance.mu.Unlock()
	}
}

func maintenanceStatsInto(stats map[string]string, m *maintenanceCoordinator) {
	if stats == nil || m == nil {
		return
	}
	active := maintenanceActiveLabel(m.active.Load())
	deferrals := m.deferrals.Load()
	waitTotal := time.Duration(m.waitTotal.Load())
	waitMax := time.Duration(m.waitMax.Load())
	gcRuns := m.gcRuns.Load()
	vacuumRuns := m.vacuumRuns.Load()
	lastGCAt := m.lastGCAt.Load()
	lastVacuumAt := m.lastVacuumAt.Load()

	stats["treedb.maintenance.full_scan.active"] = active
	stats["treedb.maintenance.full_scan.deferrals"] = fmt.Sprintf("%d", deferrals)
	stats["treedb.maintenance.full_scan.wait_total_ms"] = fmt.Sprintf("%.3f", float64(waitTotal)/float64(time.Millisecond))
	stats["treedb.maintenance.full_scan.wait_max_ms"] = fmt.Sprintf("%.3f", float64(waitMax)/float64(time.Millisecond))
	stats["treedb.maintenance.full_scan.gc_runs"] = fmt.Sprintf("%d", gcRuns)
	stats["treedb.maintenance.full_scan.vacuum_runs"] = fmt.Sprintf("%d", vacuumRuns)
	if lastGCAt > 0 {
		stats["treedb.maintenance.full_scan.last_gc_unix_nano"] = fmt.Sprintf("%d", lastGCAt)
	} else {
		stats["treedb.maintenance.full_scan.last_gc_unix_nano"] = "0"
	}
	if lastVacuumAt > 0 {
		stats["treedb.maintenance.full_scan.last_vacuum_unix_nano"] = fmt.Sprintf("%d", lastVacuumAt)
	} else {
		stats["treedb.maintenance.full_scan.last_vacuum_unix_nano"] = "0"
	}
}

func normalizeBackpressureDefaults(opts *Options) {
	if opts == nil {
		return
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
	if opts.SlowdownBacklogSeconds != 0 || opts.StopBacklogSeconds != 0 || opts.MaxBacklogBytes != 0 {
		return
	}

	slowdown := defaultSlowdownBacklogSeconds
	stop := defaultStopBacklogSeconds

	opts.SlowdownBacklogSeconds = slowdown
	opts.StopBacklogSeconds = stop
	opts.MaxBacklogBytes = defaultAdaptiveMaxBacklogBytes
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
		templateChunkSize = defaultTemplateChunkSize
	}
	if opts.KeepRecent == 0 && !opts.ReadOnly {
		opts.KeepRecent = 1
	}
	if opts.ValueLog.Compression == 0 {
		opts.ValueLog.Compression = db.ValueLogCompressionAuto
	}

	applyEnvMaintenanceOverrides(&opts)
	forceTemplateCompressionOff(&opts)

	writePath := writePathFromOptions(opts)
	if envBool(envWritePathLog) {
		effectivePolicy := opts.ValueLog.Generational.Policy
		if effectivePolicy == ValueLogGenerationDefault {
			effectivePolicy = ValueLogGenerationHotWarmCold
		}
		fmt.Fprintf(
			os.Stderr,
			"treedb write_path mode=%s value_store=%s redo_log=%s vlog_generation_policy_raw=%d vlog_generation_policy_effective=%d\n",
			writePath.mode,
			writePath.valueStore,
			writePath.redoLog,
			opts.ValueLog.Generational.Policy,
			effectivePolicy,
		)
	}

	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return nil, err
	}
	rootDir := layout.rootDir
	maindbDir := layout.mainDir
	dictdbDir := layout.dictdbDir
	templatedbDir := layout.templatedbDir

	// Apply persisted index encoding knobs so tools and apps can open existing
	// dirs without needing to re-specify format-affecting flags.
	//
	// This is intentionally limited to index encoding flags; runtime policies
	// (like value-log compression) remain controlled by opts/env unless the
	// caller opts out via IgnoreFormatConfig.
	if !opts.IgnoreFormatConfig {
		if cfg, ok, err := db.LoadFormatConfig(maindbDir); err != nil {
			return nil, err
		} else if ok {
			cfg.ApplyIndexFormatToOptions(&opts)
		}
	}

	// Keep opts.DisableSideStores consistent with the resolved layout.
	opts.DisableSideStores = layout.disableSideStores

	// Dict compression requires a persistent dict store so dictionaries can be
	// published and older dict-compressed frames remain decodable.
	//
	// DisableSideStores removes dictdb/templatedb plumbing. When it is enabled,
	// auto compression automatically runs in a no-dict configuration by
	// disabling dictionary training so block/off decisions can continue to work.
	if opts.DisableSideStores {
		switch opts.ValueLog.Compression {
		case ValueLogCompressionDict:
			return nil, fmt.Errorf("treedb: dict compression requires side stores (dictdb); set DisableSideStores=false")
		case ValueLogCompressionAuto:
			// Side stores disabled means we cannot publish dictionaries. In this
			// configuration auto mode behaves as "no-dict auto" (block/off only).
			if opts.ValueLog.DictTrain.TrainBytes >= 0 {
				train := opts.ValueLog.DictTrain
				train.TrainBytes = -1
				opts.ValueLog.DictTrain = train
			}
		}
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
		// Side stores are opened via the backend (no caching layer). They must
		// not inherit outer-leaf-in-value-log from the main DB, since that mode
		// requires a leaf-page log wired by the cached layer.
		dictOpts.IndexOuterLeavesInValueLog = false
		dictOpts.DisableBackgroundPrune = true
		dictOpts.ValueLog.DictLookup = nil
		dictOpts.ValueLog.DictTrain = TrainConfig{TrainBytes: -1}
		// dictdb stores small metadata values (e.g. current dict id, hash->id map)
		// inline. ForcePointers would set InlineThreshold=0 and break these writes.
		dictOpts.ValueLog.ForcePointers = false
		// Avoid inheriting pointer/domain placement rules from the main DB. Side
		// stores keep small values inline by default.
		dictOpts.ValueLog.PointerThreshold = 0
		dictOpts.ValueLog.DomainInlineThresholds = nil
		dictOpts.ValueLog.Compression = db.ValueLogCompressionOff
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
		// Like dictdb, templatedb is an internal side store; avoid inheriting the
		// main DB's outer-leaf value-log layout (not needed here, and it adds
		// unnecessary value-log churn).
		templateOpts.IndexOuterLeavesInValueLog = false
		templateOpts.ValueLog.DictLookup = nil
		templateOpts.ValueLog.DictTrain = TrainConfig{TrainBytes: -1}
		// templatedb uses batch.Set for small routing/index entries. Do not
		// propagate ForcePointers from the main DB into this internal store.
		templateOpts.ValueLog.ForcePointers = false
		templateOpts.ValueLog.PointerThreshold = 0
		templateOpts.ValueLog.DomainInlineThresholds = nil
		templateOpts.ValueLog.Compression = db.ValueLogCompressionOff
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

	normalizeBackpressureDefaults(&opts)
	if opts.MemtableMode == "" {
		opts.MemtableMode = "adaptive"
	}

	disableWAL := opts.Durability == db.DurabilityWALOffRelaxed
	relaxedSync := opts.Durability != db.DurabilityDurable
	disableReadChecksum := opts.ValueLog.ReadIntegrity == db.IntegritySkipChecksums
	allowUnsafe := disableWAL || relaxedSync || disableReadChecksum
	valueLogMaxSegmentBytes := int64(0)
	if opts.IndexPackedValuePtr || opts.IndexOuterLeavesInValueLog {
		valueLogMaxSegmentBytes = int64(^uint32(0)) - 4
	}

	cached, err := caching.Open(opts.Dir, backend, caching.Options{
		FlushThreshold:                           opts.FlushThreshold,
		MemtableMode:                             opts.MemtableMode,
		MemtableShards:                           opts.MemtableShards,
		DomainIngressWorkers:                     opts.DomainIngressWorkers,
		DomainIngressQueueSize:                   opts.DomainIngressQueueSize,
		MaxQueuedMemtables:                       opts.MaxQueuedMemtables,
		SlowdownBacklogSeconds:                   opts.SlowdownBacklogSeconds,
		StopBacklogSeconds:                       opts.StopBacklogSeconds,
		MaxBacklogBytes:                          opts.MaxBacklogBytes,
		WriterFlushMaxMemtables:                  opts.WriterFlushMaxMemtables,
		WriterFlushMaxDuration:                   opts.WriterFlushMaxDuration,
		FlushBuildConcurrency:                    opts.FlushBuildConcurrency,
		FlushBuildMinEntries:                     opts.FlushBuildMinEntries,
		FlushBuildMinUnits:                       opts.FlushBuildMinUnits,
		FlushBuildChunkCap:                       opts.FlushBuildChunkCap,
		FlushBuildChunkTargetBytes:               opts.FlushBuildChunkTargetBytes,
		FlushBuildChunkMinBytes:                  opts.FlushBuildChunkMinBytes,
		FlushBuildChunkMaxBytes:                  opts.FlushBuildChunkMaxBytes,
		FlushBuildPrefetchUnits:                  opts.FlushBuildPrefetchUnits,
		FlushBackendMaxEntries:                   opts.FlushBackendMaxEntries,
		FlushBackendMaxBatches:                   opts.FlushBackendMaxBatches,
		DisableWAL:                               disableWAL,
		JournalLanes:                             opts.JournalLanes,
		WALMaxSegmentBytes:                       opts.WALMaxSegmentBytes,
		JournalCompression:                       opts.JournalCompression,
		RelaxedSync:                              relaxedSync,
		DisableReadChecksum:                      disableReadChecksum,
		ValueLogPointerThreshold:                 opts.ValueLog.PointerThreshold,
		IndexOuterLeavesInValueLog:               opts.IndexOuterLeavesInValueLog,
		ValueLogDomainInlineThresholds:           opts.ValueLog.DomainInlineThresholds,
		ValueLogRawWritevMinAvgBytes:             opts.ValueLog.RawWritevMinAvgBytes,
		ValueLogRawWritevMinBatchRecords:         opts.ValueLog.RawWritevMinBatchRecords,
		ValueLogMaxSegmentBytes:                  valueLogMaxSegmentBytes,
		ValueLogCompression:                      uint8(opts.ValueLog.Compression),
		ValueLogBlockCodec:                       uint8(opts.ValueLog.BlockCodec),
		ValueLogBlockTargetCompressedBytes:       opts.ValueLog.BlockTargetCompressedBytes,
		ValueLogIncompressibleHoldBytes:          opts.ValueLog.IncompressibleHoldBytes,
		ValueLogIncompressibleProbeBytes:         opts.ValueLog.IncompressibleProbeIntervalBytes,
		ValueLogAutoPolicy:                       uint8(opts.ValueLog.AutoPolicy),
		ValueLogGenerationPolicy:                 uint8(opts.ValueLog.Generational.Policy),
		ValueLogGenerationHotSegmentTargetBytes:  opts.ValueLog.Generational.HotSegmentTargetBytes,
		ValueLogGenerationWarmSegmentTargetBytes: opts.ValueLog.Generational.WarmSegmentTargetBytes,
		ValueLogGenerationColdSegmentTargetBytes: opts.ValueLog.Generational.ColdSegmentTargetBytes,
		ValueLogRewriteBudgetBytesPerSec:         opts.ValueLog.Generational.RewriteBudgetBytesPerSec,
		ValueLogRewriteBudgetRecordsPerSec:       opts.ValueLog.Generational.RewriteBudgetRecordsPerSec,
		ValueLogRewriteTriggerStaleRatioPPM:      opts.ValueLog.Generational.RewriteTriggerStaleRatioPPM,
		ValueLogRewriteTriggerTotalBytes:         opts.ValueLog.Generational.RewriteTriggerTotalBytes,
		ValueLogRewriteTriggerChurnPerSec:        opts.ValueLog.Generational.RewriteTriggerChurnPerSec,
		ForceValueLogPointers:                    opts.ValueLog.ForcePointers,
		ValueLogDictTrain:                        opts.ValueLog.DictTrain,
		ValueLogDictMaxK:                         opts.ValueLog.DictMaxK,
		ValueLogDictClassMode:                    uint8(opts.ValueLog.DictClassMode),
		ValueLogDictFrameEncodeLevel:             opts.ValueLog.DictFrameEncodeLevel,
		ValueLogDictFrameEnableEntropy:           opts.ValueLog.DictFrameEnableEntropy,
		ValueLogDictAdaptiveRatio:                opts.ValueLog.DictAdaptiveRatio,
		ValueLogDictMetricsWindowBytes:           opts.ValueLog.DictMetricsWindowBytes,
		ValueLogDictMetricsMinRecords:            opts.ValueLog.DictMetricsMinRecords,
		ValueLogDictMetricsPauseBytes:            opts.ValueLog.DictMetricsPauseBytes,
		ValueLogDictIncompressibleHoldBytes:      opts.ValueLog.DictIncompressibleHoldBytes,
		ValueLogDictProbeIntervalBytes:           opts.ValueLog.DictProbeIntervalBytes,
		ValueLogDictMinPayloadSavingsRatio:       opts.ValueLog.DictMinPayloadSavingsRatio,
		ValueLogCompressionAutotune:              opts.ValueLog.CompressionAutotune,
		ValueLogTemplateMode:                     opts.ValueLog.TemplateMode,
		ValueLogTemplateConfig:                   opts.ValueLog.TemplateConfig,
		ValueLogTemplateReadStrict:               opts.ValueLog.TemplateReadStrict,
		AllowUnsafe:                              allowUnsafe,
		MaxValueLogRetainedBytes:                 opts.ValueLog.MaxRetainedBytes,
		MaxValueLogRetainedBytesHard:             opts.ValueLog.MaxRetainedBytesHard,
		NotifyError:                              opts.NotifyError,
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

const (
	envDisableBackgroundPrune       = "TREEDB_DISABLE_BACKGROUND_PRUNE"
	envDisableBackgroundIndexVacuum = "TREEDB_DISABLE_BACKGROUND_INDEX_VACUUM"
	envDisableVlogGeneration        = "TREEDB_DISABLE_VLOG_GENERATION"

	// Value-log compression knobs (cached mode).
	//
	// These are env-driven so downstream apps can toggle behavior without
	// plumbing new CLI flags through multiple repos.
	envVlogCompression      = "TREEDB_VLOG_COMPRESSION"        // auto|dict|block|off
	envVlogAutoPolicy       = "TREEDB_VLOG_AUTO_POLICY"        // balanced|throughput|size
	envVlogBlockCodec       = "TREEDB_VLOG_BLOCK_CODEC"        // snappy|lz4
	envVlogBlockTargetBytes = "TREEDB_VLOG_BLOCK_TARGET_BYTES" // int (compressed target bytes)

	// Value-log dictionary compression knobs (cached mode).
	//
	// Enabling dict compression requires:
	//   - ValueLog compression mode that allows dicts (auto/dict), and
	//   - Dict training enabled (TrainBytes > 0), and
	//   - Side stores enabled (dictdb), and
	//   - Split value log enabled (value pointers used).
	envVlogDictEnable            = "TREEDB_VLOG_DICT_ENABLE"                    // bool
	envVlogDictTrainBytes        = "TREEDB_VLOG_DICT_TRAIN_BYTES"               // int
	envVlogDictBytes             = "TREEDB_VLOG_DICT_BYTES"                     // int
	envVlogDictMinRecords        = "TREEDB_VLOG_DICT_MIN_RECORDS"               // int
	envVlogDictMaxRecordBytes    = "TREEDB_VLOG_DICT_MAX_RECORD_BYTES"          // int
	envVlogDictSampleStride      = "TREEDB_VLOG_DICT_SAMPLE_STRIDE"             // int
	envVlogDictDedupWindow       = "TREEDB_VLOG_DICT_DEDUP_WINDOW"              // int
	envVlogDictTrainLevel        = "TREEDB_VLOG_DICT_TRAIN_LEVEL"               // int
	envVlogDictMaxK              = "TREEDB_VLOG_DICT_MAX_K"                     // int
	envVlogDictClassMode         = "TREEDB_VLOG_DICT_CLASS_MODE"                // single|split_outer_leaf
	envVlogDictZstdLevel         = "TREEDB_VLOG_DICT_ZSTD_LEVEL"                // fastest|default|better|best|int
	envVlogDictEntropy           = "TREEDB_VLOG_DICT_ENTROPY"                   // bool
	envVlogDictAdaptiveRatio     = "TREEDB_VLOG_DICT_ADAPTIVE_RATIO"            // float64
	envVlogDictMinPayloadSavings = "TREEDB_VLOG_DICT_MIN_PAYLOAD_SAVINGS_RATIO" // float64
)

func applyEnvMaintenanceOverrides(opts *Options) {
	if opts == nil {
		return
	}
	if envBool(envDisableBackgroundPrune) {
		opts.DisableBackgroundPrune = true
	}
	if envBool(envDisableBackgroundIndexVacuum) {
		opts.BackgroundIndexVacuumInterval = -1
	}
	if envBool(envDisableVlogGeneration) {
		opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	}

	if val, ok := envString(envVlogCompression); ok {
		switch strings.ToLower(val) {
		case "auto":
			opts.ValueLog.Compression = ValueLogCompressionAuto
		case "dict":
			opts.ValueLog.Compression = ValueLogCompressionDict
		case "block":
			opts.ValueLog.Compression = ValueLogCompressionBlock
		case "off", "none", "0":
			opts.ValueLog.Compression = ValueLogCompressionOff
		}
	}
	if val, ok := envString(envVlogAutoPolicy); ok {
		switch strings.ToLower(val) {
		case "balanced":
			opts.ValueLog.AutoPolicy = ValueLogAutoBalanced
		case "throughput":
			opts.ValueLog.AutoPolicy = ValueLogAutoThroughput
		case "size":
			opts.ValueLog.AutoPolicy = ValueLogAutoSize
		}
	}
	if val, ok := envString(envVlogBlockCodec); ok {
		switch strings.ToLower(val) {
		case "snappy":
			opts.ValueLog.BlockCodec = ValueLogBlockSnappy
		case "lz4":
			opts.ValueLog.BlockCodec = ValueLogBlockLZ4
		}
	}
	if v, ok := envInt(envVlogBlockTargetBytes); ok {
		opts.ValueLog.BlockTargetCompressedBytes = v
	}

	if enabled, ok := envBoolSet(envVlogDictEnable); ok {
		if enabled {
			EnableValueLogDictCompression(opts)
		} else {
			DisableValueLogDictCompression(opts)
		}
	}
	train := opts.ValueLog.DictTrain
	trainTouched := false
	if v, ok := envInt(envVlogDictTrainBytes); ok {
		train.TrainBytes = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictBytes); ok {
		train.DictBytes = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictMinRecords); ok {
		train.MinRecords = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictMaxRecordBytes); ok {
		train.MaxRecordBytes = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictSampleStride); ok {
		train.SampleStride = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictDedupWindow); ok {
		train.DedupWindow = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictTrainLevel); ok {
		train.Level = v
		trainTouched = true
	}
	if trainTouched {
		opts.ValueLog.DictTrain = train
	}
	if v, ok := envInt(envVlogDictMaxK); ok {
		opts.ValueLog.DictMaxK = v
	}
	if v, ok := envString(envVlogDictClassMode); ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "single", "":
			opts.ValueLog.DictClassMode = ValueLogDictClassSingle
		case "split_outer_leaf", "split-outer-leaf", "split", "outer_leaf_split":
			opts.ValueLog.DictClassMode = ValueLogDictClassSplitOuterLeaf
		}
	}
	if v, ok := envString(envVlogDictZstdLevel); ok {
		if level, ok := parseZstdEncoderLevel(v); ok {
			opts.ValueLog.DictFrameEncodeLevel = level
		}
	}
	if enabled, ok := envBoolSet(envVlogDictEntropy); ok {
		opts.ValueLog.DictFrameEnableEntropy = enabled
	}
	if v, ok := envFloat64(envVlogDictAdaptiveRatio); ok {
		opts.ValueLog.DictAdaptiveRatio = v
	}
	if v, ok := envFloat64(envVlogDictMinPayloadSavings); ok {
		opts.ValueLog.DictMinPayloadSavingsRatio = v
	}
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

func envBoolSet(name string) (bool, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return false, false
	}
	if val == "" {
		return true, true
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return false, false
	}
	return parsed, true
}

func envString(name string) (string, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return "", false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return "", false
	}
	return val, true
}

func envInt(name string) (int, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return 0, false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func envFloat64(name string) (float64, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return 0, false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func parseZstdEncoderLevel(val string) (zstd.EncoderLevel, bool) {
	v := strings.TrimSpace(strings.ToLower(val))
	switch v {
	case "fastest":
		return zstd.SpeedFastest, true
	case "default":
		return zstd.SpeedDefault, true
	case "better":
		return zstd.SpeedBetterCompression, true
	case "best":
		return zstd.SpeedBestCompression, true
	default:
		// Accept integer levels for convenience.
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, false
		}
		level := zstd.EncoderLevel(i)
		switch level {
		case zstd.SpeedFastest, zstd.SpeedDefault, zstd.SpeedBetterCompression, zstd.SpeedBestCompression:
			return level, true
		default:
			return 0, false
		}
	}
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
			if errors.Is(e, errVacuumUnsupported) {
				if logEnabled {
					log.Printf("treedb: close vacuum index online skipped: %v", e)
				}
			} else {
				err = errors.Join(err, e)
			}
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

// GetMany returns values for keys.
//
// Semantics: Returns safe copies of values. Missing keys are returned as nil
// entries with no error.
func (db *DB) GetMany(keys [][]byte) ([][]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.GetMany(keys)
	}
	return db.backend.GetMany(keys)
}

// GetManyParallelPlan reports how TreeDB would schedule GetMany for the given
// key count. It can be used by adapters to enforce external worker budgets
// without duplicating TreeDB scheduler constants.
func (db *DB) GetManyParallelPlan(keyCount int) (workers int, parallel bool) {
	if db == nil {
		return 1, false
	}
	if keyCount <= 0 {
		return 1, false
	}
	if db.cached != nil {
		if planner, ok := any(db.cached).(getManyPlanner); ok {
			return planner.GetManyParallelPlan(keyCount)
		}
	}
	if db.backend != nil {
		if planner, ok := any(db.backend).(getManyPlanner); ok {
			return planner.GetManyParallelPlan(keyCount)
		}
	}
	workers = runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > keyCount {
		workers = keyCount
	}
	return workers, workers > 1
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

// NewBatchWithSize creates a new batch with a best-effort capacity hint.
//
// The size parameter is a best-effort hint, not a strict limit. Small hints are
// treated like approximate entry reserves. Larger hints may be interpreted as a
// byte budget and normalized into an internal entry estimate instead.
// Extremely large hints may also be capped internally.
//
// Callers must not rely on an exact 1:1 mapping between size and the number of
// entries or bytes that can be written to the batch.
func (db *DB) NewBatchWithSize(size int) Batch {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		return db.cached.NewBatchWithSize(size)
	}
	return db.backend.NewBatchWithSize(size)
}

// AcquireSnapshot returns a new snapshot.
//
// In cached mode, snapshots include writes that are buffered in memtables.
// In read-only mode (no caching), snapshots are backend-only.
//
// Callers may use GetUnsafe to obtain zero-copy views tied to the snapshot lifetime.
func (db *DB) AcquireSnapshot() Snapshot {
	if db == nil {
		return nil
	}
	if db.cached != nil {
		return db.cached.AcquireSnapshot()
	}
	if db.backend == nil {
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
		maintenanceStatsInto(stats, &db.maintenance)
		return stats
	}
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	writePathStatsInto(stats, db.writePath)
	stats["treedb.durability_mode"] = db.durabilityMode
	bgIndexVacuumStatsInto(stats, &db.bgVac)
	maintenanceStatsInto(stats, &db.maintenance)
	return stats
}

// DurabilityMode reports the effective durability/integrity policy string.
func (db *DB) DurabilityMode() string {
	if db == nil {
		return ""
	}
	return db.durabilityMode
}

func (db *DB) MaintenancePhase() MaintenancePhase {
	if db == nil || db.cached == nil {
		return MaintenancePhaseSteady
	}
	return db.cached.MaintenancePhase()
}

func (db *DB) SetMaintenancePhase(phase MaintenancePhase) {
	if db == nil || db.cached == nil {
		return
	}
	db.cached.SetMaintenancePhase(phase)
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
	_, finishMaintenance := db.beginFullScanMaintenance("vacuum")
	success := false
	defer func() { finishMaintenance(success) }()

	// In cached mode, ensure the backend reflects all buffered writes before
	// rebuilding/switching the index file. This avoids exposing a backend state
	// that temporarily "forgets" keys that only existed in memtables/WAL, which
	// can break higher layers that assume a stable durable boundary.
	if db.cached != nil {
		if err := db.cached.Checkpoint(); err != nil {
			return err
		}
	}

	if err := db.backend.VacuumIndexOnline(ctx); err != nil {
		return err
	}
	success = true
	return nil
}

// VacuumIndexOffline rewrites `index.db` into a fresh file and swaps it in.
// This is intended to reclaim space and restore locality after long churn.
//
// It is an offline operation: it acquires the exclusive open lock for opts.Dir.
func VacuumIndexOffline(opts Options) error {
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return err
	}
	opts.Dir = layout.mainDir
	opts.DisableSideStores = layout.disableSideStores

	// Preserve the persisted on-disk format knobs by default so offline index
	// maintenance doesn't accidentally rewrite the DB into a different layout.
	if !opts.IgnoreFormatConfig {
		if cfg, ok, err := db.LoadFormatConfig(layout.mainDir); err != nil {
			return err
		} else if ok {
			cfg.ApplyToOptions(&opts)
		}
	}

	sideCleanup, err := wireSideStoreLookups(layout.rootDir, &opts)
	if err != nil {
		return err
	}
	defer func() { _ = sideCleanup() }()

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
