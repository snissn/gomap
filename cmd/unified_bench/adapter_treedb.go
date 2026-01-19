package main

import (
	"flag"
	"math"
	"reflect"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbcaching "github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

var (
	treedbFlushThreshold            = flag.Int64("treedb-flush-threshold", 64*1024*1024, "TreeDB (cached): flush threshold in bytes")
	treedbJournalLanes              = flag.Int("treedb-journal-lanes", 0, "TreeDB: journal lane count (0=default)")
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
	treedbIterDebug                 = flag.Bool("treedb-iter-debug", false, "TreeDB: print prefix_scan iterator build/iterate timing and debug stats (queueLen, sourcesUsed)")
	treedbIterDebugLimit            = flag.Int("treedb-iter-debug-limit", 20, "TreeDB: maximum prefix_scan queries to print per DB run when -treedb-iter-debug is set")
	treedbCompactBeforeScans        = flag.Bool("treedb-compact-before-scans", false, "TreeDB: run slab compaction before scan tests (typically used with -settle-before-scans)")
	treedbCompactDeadRatio          = flag.Float64("treedb-compact-dead-ratio", 0.50, "TreeDB: slab compaction candidate dead ratio threshold")
	treedbCompactMinBytes           = flag.Uint64("treedb-compact-min-bytes", 1*1024*1024, "TreeDB: minimum slab total bytes to consider for compaction")
	treedbCompactMaxSlabs           = flag.Int("treedb-compact-max-slabs", 1, "TreeDB: maximum slabs to compact per run (0=unlimited)")
	treedbCompactMicroBatch         = flag.Int("treedb-compact-microbatch", 256, "TreeDB: compaction apply micro-batch size (keys per commit)")
	treedbCompactRotateBeforeWrite  = flag.Bool("treedb-compact-rotate-before-write", false, "TreeDB: rotate to a fresh active slab before copying live records")
	treedbCompactCopyBps            = flag.Int64("treedb-compact-copy-bps", 0, "TreeDB: compaction copy throttling (bytes/sec), 0=disabled")
	treedbCompactCopyBurst          = flag.Int64("treedb-compact-copy-burst", 0, "TreeDB: compaction copy throttling burst (bytes), 0=default")
	treedbVacuumBeforeScans         = flag.Bool("treedb-vacuum-before-scans", false, "TreeDB: vacuum (rebuild) the user index before scan tests (typically used with -settle-before-scans)")
	treedbForceValuePointers        = flag.Bool("treedb-force-value-pointers", false, "TreeDB: store all values out-of-line in slabs (no inline values)")
	treedbLeafPrefixCompression     = flag.Bool("treedb-leaf-prefix-compression", false, "TreeDB: enable prefix-compressed leaf nodes")
	treedbSlabCompression           = flag.String("treedb-slab-compression", "none", "TreeDB: slab compression (none|zstd)")
	treedbSlabCompressionMinBytes   = flag.Int("treedb-slab-compression-min-bytes", 0, "TreeDB: minimum value size to attempt compression (0=default)")
	treedbSlabCompressionMinSavings = flag.Int("treedb-slab-compression-min-savings", 0, "TreeDB: minimum bytes saved to keep compressed (0=default)")
	treedbValueLogThreshold         = flag.Int("treedb-value-log-threshold", 0, "TreeDB: value-log pointer threshold in bytes (0=default)")
	treedbMemtableValueLogPointers  = flag.Bool("treedb-memtable-value-log-pointers", false, "TreeDB: store large values as value-log pointers in memtables")
	treedbSplitValueLog             = flag.Bool("treedb-split-value-log", false, "TreeDB: store WAL and value-log in separate segments")
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
	treedbIndexColumnarLeaves       = flag.Bool("treedb-index-columnar-leaves", false, "TreeDB: enable columnar leaf encoding")
	treedbIndexInternalBaseDelta    = flag.Bool("treedb-index-internal-base-delta", false, "TreeDB: enable internal base-delta encoding")

	treedbDisableWAL           = flag.Bool("treedb-disable-wal", false, "TreeDB: disable WAL (unsafe)")
	treedbDisableValueLog      = flag.Bool("treedb-disable-value-log", false, "TreeDB: disable value-log pointers (forces legacy WAL framing; also implied by -treedb-disable-wal)")
	treedbRelaxedSync          = flag.Bool("treedb-relaxed-sync", false, "TreeDB: relaxed sync (unsafe)")
	treedbDisableReadChecksum  = flag.Bool("treedb-disable-read-checksum", false, "TreeDB: disable read checksum (unsafe)")
	treedbAllowUnsafe          = flag.Bool("treedb-allow-unsafe", false, "TreeDB: allow unsafe durability/integrity options (required for -treedb-disable-wal/-treedb-relaxed-sync/-treedb-disable-read-checksum)")
	treedbBgCompactionInterval = flag.Duration("treedb-bg-compaction-interval", 0, "TreeDB: background compaction interval (0=disabled)")
	treedbDisablePiggyback     = flag.Bool("treedb-disable-piggyback-compaction", false, "TreeDB: disable piggyback compaction")
	treedbBgVacuumInterval     = flag.Duration("treedb-bg-vacuum-interval", 0, "TreeDB: background index vacuum interval (0=disabled)")
	treedbBgVacuumSpanPPM      = flag.Uint64("treedb-bg-vacuum-span-ppm", 0, "TreeDB: background index vacuum span ratio threshold (ppm), 0=default")
)

func init() {
	RegisterDB("treedb", NewTreeDB)
	RegisterAlias("treedbcached", "treedb")
	RegisterDB("treedbbackend", NewTreeDBBackend)
	RegisterAlias("treedbraw", "treedbbackend")
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

func setOptionalBoolOption(opts *treedb.Options, name string, value bool) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName(name)
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Bool {
		return
	}
	field.SetBool(value)
}

func setOptionalIntOption(opts *treedb.Options, name string, value int) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName(name)
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Int {
		return
	}
	field.SetInt(int64(value))
}

func setOptionalFloat64Option(opts *treedb.Options, name string, value float64) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName(name)
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Float64 {
		return
	}
	field.SetFloat(value)
}

func setOptionalTrainConfig(opts *treedb.Options, name string, trainBytes, dictBytes, minRecords, maxRecordBytes, sampleStride, dedupWindow int) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName(name)
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

func parseSlabCompression(name string, minBytes int, minSavings int) slab.CompressionOptions {
	opts := slab.CompressionOptions{
		Kind:            slab.CompressionNone,
		MinBytes:        minBytes,
		MinSavingsBytes: minSavings,
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "none", "off", "false":
		opts.Kind = slab.CompressionNone
	case "zstd":
		opts.Kind = slab.CompressionZSTD
	}
	return opts
}

func NewTreeDB(dir string) (kvstore.DB, error) {
	treedbcaching.SetIteratorDebug(*treedbIterDebug)
	compOpts := parseSlabCompression(*treedbSlabCompression, *treedbSlabCompressionMinBytes, *treedbSlabCompressionMinSavings)
	opts := treedb.Options{
		Dir:                               dir,
		ChunkSize:                         64 * 1024 * 1024,
		KeepRecent:                        *treedbKeepRecent,
		PreferAppendAlloc:                 *treedbPreferAppendAlloc,
		FreelistRegionPages:               *treedbFreelistRegionPages,
		FreelistRegionRadius:              *treedbFreelistRegionRadius,
		LeafFillTargetPPM:                 uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM:             uint32(clampPPM(*treedbInternalFillPPM)),
		LeafPrefixCompression:             *treedbLeafPrefixCompression,
		FlushThreshold:                    *treedbFlushThreshold,
		MaxQueuedMemtables:                *treedbMaxQueuedMems,
		SlowdownBacklogSeconds:            *treedbSlowdownBacklogSeconds,
		StopBacklogSeconds:                *treedbStopBacklogSeconds,
		MaxBacklogBytes:                   *treedbMaxBacklogBytes,
		WriterFlushMaxMemtables:           *treedbWriterFlushMaxMems,
		ForceValuePointers:                *treedbForceValuePointers,
		ValueLogPointerThreshold:          *treedbValueLogThreshold,
		SlabCompression:                   compOpts,
		DisableWAL:                        *treedbDisableWAL,
		DisableValueLog:                   *treedbDisableValueLog,
		RelaxedSync:                       *treedbRelaxedSync,
		DisableReadChecksum:               *treedbDisableReadChecksum,
		AllowUnsafe:                       *treedbAllowUnsafe,
		JournalLanes:                      *treedbJournalLanes,
		BackgroundCompactionInterval:      *treedbBgCompactionInterval,
		BackgroundIndexVacuumInterval:     *treedbBgVacuumInterval,
		BackgroundIndexVacuumSpanRatioPPM: clampUint32(*treedbBgVacuumSpanPPM),
		DisablePiggybackCompaction:        *treedbDisablePiggyback,
	}
	if *treedbWriterFlushMaxMs > 0 {
		opts.WriterFlushMaxDuration = time.Duration(*treedbWriterFlushMaxMs) * time.Millisecond
	}
	setOptionalBoolOption(&opts, "MemtableValueLogPointers", *treedbMemtableValueLogPointers)
	setOptionalBoolOption(&opts, "SplitValueLog", *treedbSplitValueLog)
	setOptionalIntOption(&opts, "JournalLanes", *treedbJournalLanes)
	setOptionalTrainConfig(&opts, "ValueLogDictTrain",
		*treedbVlogDictTrainBytes,
		*treedbVlogDictDictBytes,
		*treedbVlogDictMinRecords,
		*treedbVlogDictMaxRecordBytes,
		*treedbVlogDictSampleStride,
		*treedbVlogDictDedupWindow,
	)
	setOptionalFloat64Option(&opts, "ValueLogDictAdaptiveRatio", *treedbVlogDictAdaptiveRatio)
	setOptionalIntOption(&opts, "ValueLogDictMetricsWindowBytes", *treedbVlogDictMetricsWindow)
	setOptionalIntOption(&opts, "ValueLogDictMetricsMinRecords", *treedbVlogDictMetricsMinRecords)
	setOptionalIntOption(&opts, "ValueLogDictMetricsPauseBytes", *treedbVlogDictMetricsPauseBytes)
	setOptionalFloat64Option(&opts, "ValueLogDictMinPayloadSavingsRatio", *treedbVlogDictMinSavingsRatio)
	setOptionalBoolOption(&opts, "IndexColumnarLeaves", *treedbIndexColumnarLeaves)
	setOptionalBoolOption(&opts, "IndexInternalBaseDelta", *treedbIndexInternalBaseDelta)
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return treedbadapter.WrapNamed(db, "TreeDB"), nil
}

func NewTreeDBBackend(dir string) (kvstore.DB, error) {
	compOpts := parseSlabCompression(*treedbSlabCompression, *treedbSlabCompressionMinBytes, *treedbSlabCompressionMinSavings)
	opts := treedb.Options{
		Dir:                               dir,
		ChunkSize:                         64 * 1024 * 1024,
		KeepRecent:                        *treedbKeepRecent,
		PreferAppendAlloc:                 *treedbPreferAppendAlloc,
		FreelistRegionPages:               *treedbFreelistRegionPages,
		FreelistRegionRadius:              *treedbFreelistRegionRadius,
		LeafFillTargetPPM:                 uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM:             uint32(clampPPM(*treedbInternalFillPPM)),
		LeafPrefixCompression:             *treedbLeafPrefixCompression,
		ForceValuePointers:                *treedbForceValuePointers,
		SlabCompression:                   compOpts,
		AllowUnsafe:                       *treedbAllowUnsafe,
		JournalLanes:                      *treedbJournalLanes,
		BackgroundIndexVacuumInterval:     *treedbBgVacuumInterval,
		BackgroundIndexVacuumSpanRatioPPM: clampUint32(*treedbBgVacuumSpanPPM),
	}
	setOptionalBoolOption(&opts, "MemtableValueLogPointers", *treedbMemtableValueLogPointers)
	setOptionalBoolOption(&opts, "SplitValueLog", *treedbSplitValueLog)
	setOptionalIntOption(&opts, "JournalLanes", *treedbJournalLanes)
	setOptionalTrainConfig(&opts, "ValueLogDictTrain",
		*treedbVlogDictTrainBytes,
		*treedbVlogDictDictBytes,
		*treedbVlogDictMinRecords,
		*treedbVlogDictMaxRecordBytes,
		*treedbVlogDictSampleStride,
		*treedbVlogDictDedupWindow,
	)
	setOptionalFloat64Option(&opts, "ValueLogDictAdaptiveRatio", *treedbVlogDictAdaptiveRatio)
	setOptionalIntOption(&opts, "ValueLogDictMetricsWindowBytes", *treedbVlogDictMetricsWindow)
	setOptionalIntOption(&opts, "ValueLogDictMetricsMinRecords", *treedbVlogDictMetricsMinRecords)
	setOptionalIntOption(&opts, "ValueLogDictMetricsPauseBytes", *treedbVlogDictMetricsPauseBytes)
	setOptionalFloat64Option(&opts, "ValueLogDictMinPayloadSavingsRatio", *treedbVlogDictMinSavingsRatio)
	setOptionalBoolOption(&opts, "IndexColumnarLeaves", *treedbIndexColumnarLeaves)
	setOptionalBoolOption(&opts, "IndexInternalBaseDelta", *treedbIndexInternalBaseDelta)
	db, err := treedb.OpenBackend(opts)
	if err != nil {
		return nil, err
	}
	return treedbadapter.WrapNamed(db, "TreeDBBackend"), nil
}
