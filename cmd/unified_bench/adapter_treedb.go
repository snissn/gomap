package main

import (
	"flag"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbcaching "github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

var (
	treedbFlushThreshold            = flag.Int64("treedb-flush-threshold", 64*1024*1024, "TreeDB (cached): flush threshold in bytes")
	treedbFlushBuildConcurrency     = flag.Int("treedb-flush-build-concurrency", 0, "TreeDB (cached): flush build concurrency (0=default)")
	treedbFlushBuildMinEntries      = flag.Int("treedb-flush-build-min-entries", 0, "TreeDB (cached): minimum entries to enable parallel flush build (0=default)")
	treedbFlushBuildMinUnits        = flag.Int("treedb-flush-build-min-units", 0, "TreeDB (cached): minimum queued memtables to enable parallel flush build (0=default)")
	treedbFlushBuildChunkCap        = flag.Int("treedb-flush-build-chunk-cap", 0, "TreeDB (cached): max entries per parallel build chunk (0=adaptive, <0=fixed default, >0=fixed cap)")
	treedbFlushBuildChunkTarget     = flag.Int("treedb-flush-build-chunk-target-bytes", 0, "TreeDB (cached): adaptive chunk target bytes (0=default)")
	treedbFlushBuildChunkMinBytes   = flag.Int("treedb-flush-build-chunk-min-bytes", 0, "TreeDB (cached): adaptive chunk min bytes (0=default)")
	treedbFlushBuildChunkMaxBytes   = flag.Int("treedb-flush-build-chunk-max-bytes", 0, "TreeDB (cached): adaptive chunk max bytes (0=default)")
	treedbFlushBuildPrefetchUnits   = flag.Int("treedb-flush-build-prefetch-units", 0, "TreeDB (cached): prefetch units for parallel flush build (0=default)")
	treedbPagerSyncConcurrency      = flag.Int("treedb-pager-sync-concurrency", 0, "TreeDB: pager msync concurrency (0=default)")
	treedbChunkSize                 = flag.Int64("treedb-chunk-size", 64*1024*1024, "TreeDB: pager chunk size in bytes (default 64MiB)")
	treedbJournalLanes              = flag.Int("treedb-journal-lanes", 0, "TreeDB: journal lane count (0=default)")
	treedbJournalCompress           = flag.Bool("treedb-journal-compress", false, "TreeDB: compress journal/commitlog segments (zstd)")
	treedbKeepRecent                = flag.Uint64("treedb-keep-recent", 0, "TreeDB: KeepRecent commit versions to retain before page reuse (0=default; cached defaults to 1)")
	treedbMaxQueuedMems             = flag.Int("treedb-max-queued-memtables", 0, "TreeDB (cached): max queued immutable memtables before backpressure flush (0=default, <0=disable)")
	treedbSlowdownBacklogSeconds    = flag.Float64("treedb-slowdown-backlog-seconds", 1, "TreeDB (cached): begin writer backpressure when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbStopBacklogSeconds        = flag.Float64("treedb-stop-backlog-seconds", 2, "TreeDB (cached): block writers when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbMaxBacklogBytes           = flag.Int64("treedb-max-backlog-bytes", 2<<30, "TreeDB (cached): absolute cap on queued flush backlog bytes (0=disabled)")
	treedbWriterFlushMaxMems        = flag.Int("treedb-writer-flush-max-memtables", 0, "TreeDB (cached): max memtables a writer will help flush per op when backpressure triggers (0=default)")
	treedbWriterFlushMaxMs          = flag.Int("treedb-writer-flush-max-ms", 0, "TreeDB (cached): max milliseconds a writer will help flush per op when backpressure triggers (0=disabled)")
	treedbPreferAppendAlloc         = flag.Bool("treedb-prefer-append-alloc", false, "TreeDB: allocate new index pages by appending instead of freelist reuse (improves scan locality under churn; grows index.db)")
	treedbFreelistRegionPages       = flag.Uint64("treedb-freelist-region-pages", 0, "TreeDB: freelist reuse region size in pages (0=default)")
	treedbFreelistRegionRadius      = flag.Int("treedb-freelist-region-radius", 0, "TreeDB: freelist reuse region radius (0=default, <0=disable bias)")
	treedbLeafFillPPM               = flag.Int("treedb-leaf-fill-ppm", 0, "TreeDB: leaf fill target (ppm). Lower reduces split churn at cost of more pages (0=default=1_000_000)")
	treedbInternalFillPPM           = flag.Int("treedb-internal-fill-ppm", 0, "TreeDB: internal fill target (ppm). Lower reduces split churn at cost of more pages (0=default=1_000_000)")
	treedbMaintenanceOpsPerCoalesce = flag.Int("treedb-maintenance-ops-per-coalesce", 0, "TreeDB: ops-per-coalesce maintenance budget (0=default, <0=disable budget)")
	treedbIterDebug                 = flag.Bool("treedb-iter-debug", false, "TreeDB: print prefix_scan iterator build/iterate timing and debug stats (queueLen, sourcesUsed)")
	treedbIterDebugLimit            = flag.Int("treedb-iter-debug-limit", 20, "TreeDB: maximum prefix_scan queries to print per DB run when -treedb-iter-debug is set")
	treedbForceValuePointers        = flag.Bool("treedb-force-value-pointers", false, "TreeDB: store all values out-of-line in the value log (no inline values)")
	treedbLeafPrefixCompression     = flag.Bool("treedb-leaf-prefix-compression", false, "TreeDB: enable prefix-compressed leaf nodes")
	treedbValueLogThreshold         = flag.Int("treedb-value-log-threshold", 0, "TreeDB: value-log pointer threshold in bytes (0=default)")
	treedbVlogDictTrainBytes        = flag.Int("treedb-vlog-dict-train-bytes", 0, "TreeDB: value-log dict training raw sample bytes (0=default, <0=disable)")
	treedbVlogDictDictBytes         = flag.Int("treedb-vlog-dict-dict-bytes", 0, "TreeDB: value-log dict size in bytes (0=default)")
	treedbVlogDictMinRecords        = flag.Int("treedb-vlog-dict-min-records", 0, "TreeDB: value-log dict minimum records to train (0=default)")
	treedbVlogDictMaxRecordBytes    = flag.Int("treedb-vlog-dict-max-record-bytes", 0, "TreeDB: value-log dict max record bytes sampled (0=default)")
	treedbVlogDictSampleStride      = flag.Int("treedb-vlog-dict-sample-stride", 0, "TreeDB: value-log dict sample stride (0=default)")
	treedbVlogDictDedupWindow       = flag.Int("treedb-vlog-dict-dedup-window", 0, "TreeDB: value-log dict dedup window size (0=default)")
	treedbVlogDictAdaptiveRatio     = flag.Float64("treedb-vlog-dict-adaptive-ratio", 0, "TreeDB: value-log dict adaptive pause threshold ratio (0=default)")
	treedbVlogDictMetricsWindow     = flag.Int("treedb-vlog-dict-metrics-window-bytes", 0, "TreeDB: value-log dict metrics window bytes (0=default)")
	treedbVlogDictMetricsMinRecords = flag.Int("treedb-vlog-dict-metrics-min-records", 0, "TreeDB: value-log dict metrics min records (0=default)")
	treedbVlogDictMetricsPauseBytes = flag.Int("treedb-vlog-dict-metrics-pause-bytes", 0, "TreeDB: value-log dict pause bytes when degraded (0=default)")
	treedbVlogDictMinSavingsRatio   = flag.Float64("treedb-vlog-dict-min-savings-ratio", 0, "TreeDB: value-log dict min payload savings ratio (0=default)")
	treedbVlogCompressionAutotune   = flag.String("treedb-vlog-compression-autotune", "off", "TreeDB: value-log compression autotune mode (off|medium|aggressive|default)")
	treedbIndexColumnarLeaves       = flag.Bool("treedb-index-columnar-leaves", false, "TreeDB: enable columnar leaf encoding")
	treedbIndexInternalBaseDelta    = flag.Bool("treedb-index-internal-base-delta", false, "TreeDB: enable internal base-delta encoding")

	treedbDisableWAL          = flag.Bool("treedb-disable-wal", false, "TreeDB: disable journal/redo log while keeping value-log pointers (unsafe)")
	treedbRelaxedSync         = flag.Bool("treedb-relaxed-sync", false, "TreeDB: relaxed sync (unsafe)")
	treedbDisableReadChecksum = flag.Bool("treedb-disable-read-checksum", false, "TreeDB: disable read checksum (unsafe)")
	treedbAllowUnsafe         = flag.Bool("treedb-allow-unsafe", false, "TreeDB: allow unsafe durability/integrity options (required for -treedb-disable-wal/-treedb-relaxed-sync/-treedb-disable-read-checksum)")
	treedbDisablePiggyback    = flag.Bool("treedb-disable-piggyback-compaction", false, "TreeDB: disable piggyback compaction")
	treedbMemtableMode        = flag.String("treedb-memtable-mode", "", "TreeDB (cached): memtable mode (skiplist|hash_sorted|btree|adaptive|adaptive:<mode>|auto)")
)

func init() {
	RegisterDB("treedb", NewTreeDB)
	RegisterAlias("treedbcached", "treedb")
}

func clampPPM(v int) int {
	if v < 0 {
		return 0
	}
	if v > 1_000_000 {
		return 1_000_000
	}
	return v
}

func clampUint32(v uint64) uint32 {
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

func fieldByPath(root reflect.Value, path ...string) reflect.Value {
	v := root
	for _, name := range path {
		if !v.IsValid() {
			return reflect.Value{}
		}
		v = v.FieldByName(name)
	}
	return v
}

func setOptionalVlogTrainConfig(opts *treedb.Options, trainBytes, dictBytes, minRecords, maxRecordBytes, sampleStride, dedupWindow int) {
	root := reflect.ValueOf(opts).Elem()
	field := fieldByPath(root, "ValueLog", "DictTrain")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Struct {
		return
	}
	setInt := func(child string, val int) {
		f := field.FieldByName(child)
		if !f.IsValid() || !f.CanSet() || f.Kind() != reflect.Int {
			return
		}
		f.SetInt(int64(val))
	}
	setInt("TrainBytes", trainBytes)
	setInt("DictBytes", dictBytes)
	setInt("MinRecords", minRecords)
	setInt("MaxRecordBytes", maxRecordBytes)
	setInt("SampleStride", sampleStride)
	setInt("DedupWindow", dedupWindow)
}

func setOptionalVlogAutotuneMode(opts *treedb.Options, mode uint64) {
	root := reflect.ValueOf(opts).Elem()
	field := fieldByPath(root, "ValueLog", "CompressionAutotune")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Struct {
		return
	}
	modeField := field.FieldByName("Mode")
	if !modeField.IsValid() || !modeField.CanSet() {
		return
	}
	switch modeField.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		modeField.SetUint(mode)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		modeField.SetInt(int64(mode))
	}
}

func parseVlogCompressionAutotuneMode(s string) (uint64, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "default", "unset", "auto":
		// The actual default is decided by TreeDB.
		return 0, nil
	case "off", "false", "0":
		return 1, nil
	case "medium":
		return 2, nil
	case "aggressive":
		return 3, nil
	default:
		return 0, fmt.Errorf("unsupported -treedb-vlog-compression-autotune=%q (expected off|medium|aggressive|default)", s)
	}
}

func NewTreeDB(dir string) (kvstore.DB, error) {
	treedbcaching.SetIteratorDebug(*treedbIterDebug)

	if !*treedbAllowUnsafe && (*treedbDisableWAL || *treedbRelaxedSync || *treedbDisableReadChecksum) {
		return nil, fmt.Errorf("TreeDB: unsafe flags require -treedb-allow-unsafe")
	}

	durability := treedb.DurabilityDurable
	if *treedbDisableWAL {
		durability = treedb.DurabilityWALOffRelaxed
	} else if *treedbRelaxedSync {
		durability = treedb.DurabilityWALOnRelaxed
	}
	readIntegrity := treedb.IntegrityVerify
	if *treedbDisableReadChecksum {
		readIntegrity = treedb.IntegritySkipChecksums
	}

	opts := treedb.Options{
		Dir:                       dir,
		KeepRecent:                *treedbKeepRecent,
		Durability:                durability,
		ChunkSize:                 *treedbChunkSize,
		PagerSyncConcurrency:      *treedbPagerSyncConcurrency,
		PreferAppendAlloc:         *treedbPreferAppendAlloc,
		FreelistRegionPages:       *treedbFreelistRegionPages,
		FreelistRegionRadius:      *treedbFreelistRegionRadius,
		LeafFillTargetPPM:         uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM:     uint32(clampPPM(*treedbInternalFillPPM)),
		MaintenanceOpsPerCoalesce: *treedbMaintenanceOpsPerCoalesce,

		LeafPrefixCompression:  *treedbLeafPrefixCompression,
		IndexColumnarLeaves:    *treedbIndexColumnarLeaves,
		IndexInternalBaseDelta: *treedbIndexInternalBaseDelta,

		MemtableMode:               *treedbMemtableMode,
		FlushThreshold:             *treedbFlushThreshold,
		MaxQueuedMemtables:         *treedbMaxQueuedMems,
		SlowdownBacklogSeconds:     *treedbSlowdownBacklogSeconds,
		StopBacklogSeconds:         *treedbStopBacklogSeconds,
		MaxBacklogBytes:            *treedbMaxBacklogBytes,
		WriterFlushMaxMemtables:    *treedbWriterFlushMaxMems,
		FlushBuildConcurrency:      *treedbFlushBuildConcurrency,
		FlushBuildMinEntries:       *treedbFlushBuildMinEntries,
		FlushBuildMinUnits:         *treedbFlushBuildMinUnits,
		FlushBuildChunkCap:         *treedbFlushBuildChunkCap,
		FlushBuildChunkTargetBytes: *treedbFlushBuildChunkTarget,
		FlushBuildChunkMinBytes:    *treedbFlushBuildChunkMinBytes,
		FlushBuildChunkMaxBytes:    *treedbFlushBuildChunkMaxBytes,
		FlushBuildPrefetchUnits:    *treedbFlushBuildPrefetchUnits,

		JournalLanes:               *treedbJournalLanes,
		JournalCompression:         *treedbJournalCompress,
		DisablePiggybackCompaction: *treedbDisablePiggyback,

		ValueLog: treedb.ValueLogOptions{
			ForcePointers:              *treedbForceValuePointers,
			PointerThreshold:           *treedbValueLogThreshold,
			ReadIntegrity:              readIntegrity,
			DictAdaptiveRatio:          *treedbVlogDictAdaptiveRatio,
			DictMetricsWindowBytes:     *treedbVlogDictMetricsWindow,
			DictMetricsMinRecords:      *treedbVlogDictMetricsMinRecords,
			DictMetricsPauseBytes:      *treedbVlogDictMetricsPauseBytes,
			DictMinPayloadSavingsRatio: *treedbVlogDictMinSavingsRatio,
		},
	}
	if *treedbWriterFlushMaxMs > 0 {
		opts.WriterFlushMaxDuration = time.Duration(*treedbWriterFlushMaxMs) * time.Millisecond
	}

	setOptionalVlogTrainConfig(&opts,
		*treedbVlogDictTrainBytes,
		*treedbVlogDictDictBytes,
		*treedbVlogDictMinRecords,
		*treedbVlogDictMaxRecordBytes,
		*treedbVlogDictSampleStride,
		*treedbVlogDictDedupWindow,
	)

	autotuneMode, err := parseVlogCompressionAutotuneMode(*treedbVlogCompressionAutotune)
	if err != nil {
		return nil, err
	}
	setOptionalVlogAutotuneMode(&opts, autotuneMode)
	if autotuneMode == 1 {
		// Compression off should also force dict training off, even if the caller
		// specified training flags. TreeDB enforces the same invariant internally,
		// but we keep unified_bench behavior explicit and deterministic.
		setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
	}

	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return treedbadapter.WrapNamed(db, "TreeDB"), nil
}
