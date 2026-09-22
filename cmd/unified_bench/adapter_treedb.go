package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbcaching "github.com/snissn/gomap/TreeDB/caching"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

const (
	defaultTreeDBChunkSizeBytes int64 = 256 * 1024

	defaultTreeDBFlushBacklogCoalescingMaxMemtables           = 64
	defaultTreeDBFlushBacklogCoalescingHardMaxMemtables       = 128
	defaultTreeDBFlushBacklogCoalescingMaxBytes         int64 = 512 << 20
	defaultTreeDBFlushBacklogCoalescingMaxOps                 = 2 << 20
	defaultTreeDBFlushBacklogCoalescingSingleOpRatio          = 0.5
	defaultTreeDBFlushBacklogCoalescingMaxOpsPerSpan          = 4.0
)

var (
	treedbFlushThreshold                  = flag.Int64("treedb-flush-threshold", 64*1024*1024, "TreeDB (cached): flush threshold in bytes")
	treedbFlushBuildConcurrency           = flag.Int("treedb-flush-build-concurrency", 0, "TreeDB (cached): flush build concurrency (0=default)")
	treedbFlushBuildMinEntries            = flag.Int("treedb-flush-build-min-entries", 0, "TreeDB (cached): minimum entries to enable parallel flush build (0=default)")
	treedbFlushBuildMinUnits              = flag.Int("treedb-flush-build-min-units", 0, "TreeDB (cached): minimum queued memtables to enable parallel flush build (0=default)")
	treedbFlushBuildChunkCap              = flag.Int("treedb-flush-build-chunk-cap", 0, "TreeDB (cached): max entries per parallel build chunk (0=adaptive, <0=fixed default, >0=fixed cap)")
	treedbFlushBuildChunkTarget           = flag.Int("treedb-flush-build-chunk-target-bytes", 0, "TreeDB (cached): adaptive chunk target bytes (0=default)")
	treedbFlushBuildChunkMinBytes         = flag.Int("treedb-flush-build-chunk-min-bytes", 0, "TreeDB (cached): adaptive chunk min bytes (0=default)")
	treedbFlushBuildChunkMaxBytes         = flag.Int("treedb-flush-build-chunk-max-bytes", 0, "TreeDB (cached): adaptive chunk max bytes (0=default)")
	treedbFlushBuildPrefetchUnits         = flag.Int("treedb-flush-build-prefetch-units", 0, "TreeDB (cached): prefetch units for parallel flush build (0=default)")
	treedbFlushAdmissionPolicy            = flag.String("treedb-flush-admission-policy", "auto", "TreeDB: span-native/backlog/concurrency admission policy (auto|explicit|off; default auto selects physical-core-aware capped adaptive when admitted)")
	treedbFlushApplyConcurrency           = flag.Int("treedb-flush-apply-concurrency", 0, "TreeDB: flush/apply COW worker-pool concurrency override (auto default is physical-core-aware capped; configured values override and cap only at GOMAXPROCS; 0/1 disables under explicit)")
	treedbFlushApplyMinEntries            = flag.Int("treedb-flush-apply-min-entries", 0, "TreeDB: minimum planned span ops to enable parallel apply (0=policy default)")
	treedbFlushApplyMinSpans              = flag.Int("treedb-flush-apply-min-spans", 0, "TreeDB: minimum planned leaf spans to enable parallel apply (0=policy default)")
	treedbFlushApplyMinBytes              = flag.Int("treedb-flush-apply-min-bytes", 0, "TreeDB: minimum planned span bytes to enable parallel apply (0=policy default)")
	treedbFlushApplySpanNative            = flag.Bool("treedb-flush-apply-span-native", false, "TreeDB: M10 span-native apply/reducer override for eligible exact point spans (auto enables when admitted)")
	treedbFlushBackendMaxEntries          = flag.Int("treedb-flush-backend-max-entries", 0, "TreeDB (cached): max entries per backend flush batch before intermediate commit (0=default, <0=disable chunking)")
	treedbFlushBackendMaxBatches          = flag.Int("treedb-flush-backend-max-batches", 0, "TreeDB (cached): max intermediate backend commits per flush (0=default, <0=disable cap)")
	treedbFlushSpanRunTargetPlanning      = flag.Bool("treedb-flush-span-run-target-planning", false, "TreeDB (cached): diagnostic opt-in read-only target-leaf planning for canonical flush runs")
	treedbFlushBacklogCoalescing          = flag.Bool("treedb-flush-backlog-coalescing", false, "TreeDB (cached): M11 bounded adaptive backlog coalescing override (auto enables when admitted)")
	treedbFlushBacklogCoalescingMaxMems   = flag.Int("treedb-flush-backlog-coalescing-max-memtables", 0, "TreeDB (cached): M11 coalescing max memtables per run (0=default)")
	treedbFlushBacklogCoalescingMaxBytes  = flag.Int64("treedb-flush-backlog-coalescing-max-bytes", 0, "TreeDB (cached): M11 coalescing soft max queued bytes per run; first included memtable may exceed (0=default)")
	treedbFlushBacklogCoalescingMaxOps    = flag.Int("treedb-flush-backlog-coalescing-max-ops", 0, "TreeDB (cached): M11 coalescing soft max point ops per run; first included memtable may exceed (0=default)")
	treedbFlushBacklogCoalescingMinAgeMS  = flag.Int("treedb-flush-backlog-coalescing-min-age-ms", 0, "TreeDB (cached): M11 coalescing oldest queued memtable age floor in ms (0=none)")
	treedbFlushBacklogCoalescingRatio     = flag.Float64("treedb-flush-backlog-coalescing-single-op-ratio", 0, "TreeDB (cached): M11 coalescing single-op span ratio trigger (0=default)")
	treedbFlushBacklogCoalescingOpsSpan   = flag.Float64("treedb-flush-backlog-coalescing-max-ops-per-span", 0, "TreeDB (cached): M11 coalescing max observed ops/span trigger (0=default)")
	treedbFlushBacklogCoalescingOldLeafB  = flag.Float64("treedb-flush-backlog-coalescing-min-old-leaf-bytes-per-op", 0, "TreeDB (cached): M11 coalescing min old-leaf decode bytes/op trigger (0=disabled)")
	treedbPagerSyncConcurrency            = flag.Int("treedb-pager-sync-concurrency", 0, "TreeDB: pager msync concurrency (0=default)")
	treedbPagerMmapPopulate               = flag.Bool("treedb-pager-mmap-populate", false, "TreeDB (Linux): enable MAP_POPULATE on index.db mmap")
	treedbPagerPrefetchOnRead             = flag.Bool("treedb-pager-prefetch-on-read", false, "TreeDB (Linux): enable best-effort mmap prefetch hints (madvise WILLNEED) during checkpoint/merge rewrites")
	treedbLeafPageReadCacheEntries        = flag.Int("treedb-leaf-page-read-cache-entries", 0, "TreeDB: outer-leaf read cache entries for leaf pages stored in the value log (0=default/env, <0=disable)")
	treedbLeafPageReadCacheWriteAdmission = flag.String("treedb-leaf-page-read-cache-write-admission", "immediate", "TreeDB: write-side outer-leaf read-cache admission policy (immediate|adaptive)")
	treedbChunkSize                       = flag.Int64("treedb-chunk-size", defaultTreeDBChunkSizeBytes, "TreeDB: pager chunk size in bytes (default 256KiB)")
	treedbJournalLanes                    = flag.Int("treedb-journal-lanes", 0, "TreeDB: journal/value-log lane count (0=coalescing-safe auto; explicit values override)")
	treedbJournalCompress                 = flag.Bool("treedb-journal-compress", false, "TreeDB: request generic journal compression; strict command-WAL V2 frames remain raw")
	treedbKeepRecent                      = flag.Uint64("treedb-keep-recent", 0, "TreeDB: KeepRecent commit versions to retain before page reuse (0=default; cached defaults to 1)")
	treedbMaxQueuedMems                   = flag.Int("treedb-max-queued-memtables", 0, "TreeDB (cached): max queued immutable memtables before backpressure flush (0=default, <0=disable)")
	treedbSlowdownBacklogSeconds          = flag.Float64("treedb-slowdown-backlog-seconds", 1, "TreeDB (cached): begin writer backpressure when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbStopBacklogSeconds              = flag.Float64("treedb-stop-backlog-seconds", 2, "TreeDB (cached): block writers when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbMaxBacklogBytes                 = flag.Int64("treedb-max-backlog-bytes", 2<<30, "TreeDB (cached): absolute cap on queued flush backlog bytes (0=disabled)")
	treedbWriterFlushMaxMems              = flag.Int("treedb-writer-flush-max-memtables", 0, "TreeDB (cached): max memtables a writer will help flush per op when backpressure triggers (0=default)")
	treedbWriterFlushMaxMs                = flag.Int("treedb-writer-flush-max-ms", 0, "TreeDB (cached): max milliseconds a writer will help flush per op when backpressure triggers (0=disabled)")
	treedbPreferAppendAlloc               = flag.Bool("treedb-prefer-append-alloc", false, "TreeDB: allocate new index pages by appending instead of freelist reuse (improves scan locality under churn; grows index.db)")
	treedbFreelistRegionPages             = flag.Uint64("treedb-freelist-region-pages", 0, "TreeDB: freelist reuse region size in pages (0=default)")
	treedbFreelistRegionRadius            = flag.Int("treedb-freelist-region-radius", 0, "TreeDB: freelist reuse region radius (0=default, <0=disable bias)")
	treedbLeafFillPPM                     = flag.Int("treedb-leaf-fill-ppm", 0, "TreeDB: leaf fill target (ppm). Lower reduces split churn at cost of more pages (0=default=1_000_000)")
	treedbInternalFillPPM                 = flag.Int("treedb-internal-fill-ppm", 0, "TreeDB: internal fill target (ppm). Lower reduces split churn at cost of more pages (0=default=1_000_000)")
	treedbMaintenanceOpsPerCoalesce       = flag.Int("treedb-maintenance-ops-per-coalesce", 0, "TreeDB: ops-per-coalesce maintenance budget (0=default, <0=disable budget)")
	treedbIterDebug                       = flag.Bool("treedb-iter-debug", false, "TreeDB: print prefix_scan iterator build/iterate timing and debug stats (queueLen, sourcesUsed)")
	treedbIterDebugLimit                  = flag.Int("treedb-iter-debug-limit", 20, "TreeDB: maximum prefix_scan queries to print per DB run when -treedb-iter-debug is set")
	treedbForceValuePointers              = flag.Bool("treedb-force-value-pointers", false, "TreeDB: store all values out-of-line in the value log (no inline values)")
	treedbIndexOptimizations              = flag.Bool("treedb-index-optimizations", false, "TreeDB: enable profile-driven index optimization bundle (leaf prefix compression + columnar leaves + packed value pointers + internal base-delta)")
	treedbLeafPrefixCompression           = flag.Bool("treedb-leaf-prefix-compression", false, "TreeDB: enable front-coded leaf key compression (restart points; compact entry header)")
	treedbValueLogThreshold               = flag.Int("treedb-value-log-threshold", 0, "TreeDB: value-log pointer threshold in bytes (0=default)")
	treedbVlogRawWritevMinAvgBytes        = flag.Int("treedb-vlog-raw-writev-min-avg-bytes", 0, "TreeDB: raw grouped-frame writev min average payload bytes/record (0=adaptive)")
	treedbVlogRawWritevMinBatchRecs       = flag.Int("treedb-vlog-raw-writev-min-batch-records", 0, "TreeDB: raw grouped-frame writev min records/batch (0=default)")
	treedbVlogCompression                 = flag.String("treedb-vlog-compression", "default", "TreeDB: value-log compression mode (default=auto; values: off|block|dict|auto)")
	treedbVlogBlockCodec                  = flag.String("treedb-vlog-block-codec", "snappy", "TreeDB: value-log block codec (snappy|lz4|zstd)")
	treedbVlogAutoPolicy                  = flag.String("treedb-vlog-auto-policy", "balanced", "TreeDB: value-log auto policy (balanced|throughput|size)")
	treedbVlogGenerationPolicy            = flag.String("treedb-vlog-generation-policy", "default", "TreeDB: value-log generation policy (default|off|hot_warm_cold)")
	treedbVlogGenerationHotSegmentBytes   = flag.Int64("treedb-vlog-generation-hot-segment-bytes", 0, "TreeDB: generational hot segment target bytes (0=default)")
	treedbVlogGenerationWarmSegmentBytes  = flag.Int64("treedb-vlog-generation-warm-segment-bytes", 0, "TreeDB: generational warm segment target bytes (0=default)")
	treedbVlogGenerationColdSegmentBytes  = flag.Int64("treedb-vlog-generation-cold-segment-bytes", 0, "TreeDB: generational cold segment target bytes (0=default)")
	treedbVlogRewriteBudgetBytesPerSec    = flag.Int64("treedb-vlog-rewrite-budget-bytes-per-sec", 0, "TreeDB: generational rewrite byte budget (0=disabled)")
	treedbVlogRewriteBudgetRecordsPerSec  = flag.Int("treedb-vlog-rewrite-budget-records-per-sec", 0, "TreeDB: generational rewrite record budget (0=disabled)")
	treedbVlogRewriteTriggerStaleRatioPPM = flag.Uint("treedb-vlog-rewrite-trigger-stale-ratio-ppm", 0, "TreeDB: generational rewrite stale/live trigger in ppm (0=disabled)")
	treedbVlogRewriteTriggerTotalBytes    = flag.Int64("treedb-vlog-rewrite-trigger-total-bytes", 0, "TreeDB: generational rewrite total retained bytes trigger (0=disabled)")
	treedbVlogRewriteTriggerChurnPerSec   = flag.Int64("treedb-vlog-rewrite-trigger-churn-per-sec", 0, "TreeDB: generational rewrite churn trigger in bytes/sec (0=disabled)")
	treedbVlogRewriteMinSegmentAgeMS      = flag.Int("treedb-vlog-rewrite-min-segment-age-ms", 0, "TreeDB: generational rewrite minimum source segment age in milliseconds (0=default)")
	treedbVlogBlockTargetBytes            = flag.Int("treedb-vlog-block-target-bytes", 0, "TreeDB: value-log block target compressed bytes (0=default)")
	treedbVlogIncompressibleHoldBytes     = flag.Int("treedb-vlog-incompressible-hold-bytes", 0, "TreeDB: auto-mode incompressible hold bytes (0=default)")
	treedbVlogIncompressibleProbeBytes    = flag.Int("treedb-vlog-incompressible-probe-bytes", 0, "TreeDB: auto-mode incompressible probe interval bytes (0=default)")
	treedbVlogDictTrainBytes              = flag.Int("treedb-vlog-dict-train-bytes", 0, "TreeDB: value-log dict training raw sample bytes (0=default, <0=disable)")
	treedbVlogDictDictBytes               = flag.Int("treedb-vlog-dict-dict-bytes", 0, "TreeDB: value-log dict size in bytes (0=default)")
	treedbVlogDictMinRecords              = flag.Int("treedb-vlog-dict-min-records", 0, "TreeDB: value-log dict minimum records to train (0=default)")
	treedbVlogDictMaxRecordBytes          = flag.Int("treedb-vlog-dict-max-record-bytes", 0, "TreeDB: value-log dict max record bytes sampled (0=default)")
	treedbVlogDictSampleStride            = flag.Int("treedb-vlog-dict-sample-stride", 0, "TreeDB: value-log dict sample stride (0=default)")
	treedbVlogDictDedupWindow             = flag.Int("treedb-vlog-dict-dedup-window", 0, "TreeDB: value-log dict dedup window size (0=default)")
	treedbVlogDictAdaptiveRatio           = flag.Float64("treedb-vlog-dict-adaptive-ratio", 0, "TreeDB: value-log dict adaptive pause threshold ratio (0=default)")
	treedbVlogDictMetricsWindow           = flag.Int("treedb-vlog-dict-metrics-window-bytes", 0, "TreeDB: value-log dict metrics window bytes (0=default)")
	treedbVlogDictMetricsMinRecords       = flag.Int("treedb-vlog-dict-metrics-min-records", 0, "TreeDB: value-log dict metrics min records (0=default)")
	treedbVlogDictMetricsPauseBytes       = flag.Int("treedb-vlog-dict-metrics-pause-bytes", 0, "TreeDB: value-log dict pause bytes when degraded (0=default)")
	treedbVlogDictIncompressibleHoldBytes = flag.Int("treedb-vlog-dict-incompressible-hold-bytes", 0, "TreeDB: incompressible-stream hold bytes for dict suppression (0=default)")
	treedbVlogDictProbeIntervalBytes      = flag.Int("treedb-vlog-dict-probe-interval-bytes", 0, "TreeDB: probe interval bytes while incompressible hold is active (0=default)")
	treedbVlogDictMinSavingsRatio         = flag.Float64("treedb-vlog-dict-min-savings-ratio", 0, "TreeDB: value-log dict min payload savings ratio (0=default)")
	treedbVlogDictK                       = flag.Int("treedb-vlog-dict-k", 0, "TreeDB: value-log dict frame grouping K (records/frame, 0=default)")
	treedbVlogDictClassMode               = flag.String("treedb-vlog-dict-class-mode", "single", "TreeDB: value-log dict class mode (single|split_outer_leaf)")
	treedbVlogCompressionAutotune         = flag.String("treedb-vlog-compression-autotune", "default", "TreeDB: value-log compression autotune mode (default|off|medium|aggressive)")
	treedbVlogDictFrameEncodeLevel        = flag.String("treedb-vlog-dict-frame-encode-level", "engine", "TreeDB: zstd encoder level for dict-compressed value-log frames (engine|fastest|default|better|best|all|<int>)")
	treedbVlogDictFrameEntropyMode        = flag.String("treedb-vlog-dict-frame-entropy", "engine", "TreeDB: dict-frame entropy mode (engine|on|off|both). Controls WithNoEntropyCompression.")
	treedbVlogDictMode                    = flag.String("treedb-vlog-dict", "default", "TreeDB: value-log dict compression mode for unified_bench (default|on|off|both). Overrides dict/compression settings for TreeDB benchmarks.")
	treedbVlogCompressionVariant          = flag.String("treedb-vlog-compression-variant", "default", "TreeDB: value-log compression variant expansion for unified_bench (default|off|dict|block_snappy|block_lz4|block_zstd|auto|all). Overrides -treedb-vlog-dict when set.")
	treedbIndexColumnarLeaves             = flag.Bool("treedb-index-columnar-leaves", false, "TreeDB: enable columnar leaf encoding")
	treedbIndexPackedValuePtr             = flag.Bool("treedb-index-packed-valueptr", false, "TreeDB: enable packed 12-byte ValuePtr encoding for pointer entries in leaf pages")
	treedbIndexInternalBaseDelta          = flag.Bool("treedb-index-internal-base-delta", false, "TreeDB: enable internal base-delta encoding")
	treedbIndexOuterLeavesInVlog          = flag.Bool("treedb-index-outer-leaves-in-vlog", true, "TreeDB: store B+Tree leaf pages (outer leaves) in the value log instead of index.db")
	treedbCommandWALStatsScan             = flag.Bool("treedb-command-wal-stats-scan", false, "TreeDB command-WAL variants: scan WAL segments in Stats for diagnostic segment inventory; live accepted/covered counters are reported without this scan")

	treedbDisableWAL             = flag.Bool("treedb-disable-wal", false, "TreeDB: disable journal/redo log while keeping value-log pointers (unsafe)")
	treedbRelaxedSync            = flag.Bool("treedb-relaxed-sync", false, "TreeDB: relaxed sync (unsafe)")
	treedbDisableReadChecksum    = flag.Bool("treedb-disable-read-checksum", false, "TreeDB: disable read checksum (unsafe)")
	treedbAllowUnsafe            = flag.Bool("treedb-allow-unsafe", false, "TreeDB: allow unsafe durability/integrity options (required for -treedb-disable-wal/-treedb-relaxed-sync/-treedb-disable-read-checksum)")
	treedbDisablePiggyback       = flag.Bool("treedb-disable-piggyback-compaction", false, "TreeDB: disable piggyback compaction")
	treedbMaintenanceMode        = flag.String("treedb-maintenance-mode", "normal", "TreeDB: maintenance preset (normal|bench)")
	treedbMemtableMode           = flag.String("treedb-memtable-mode", "", "TreeDB (cached): memtable mode (adaptive|adaptive:<mode>|skiplist|hash_sorted|btree|append_only)")
	treedbDomainIngressWorkers   = flag.Int("treedb-domain-ingress-workers", 0, "TreeDB (cached): experimental domain ingress worker count (0=disabled)")
	treedbDomainIngressQueueSize = flag.Int("treedb-domain-ingress-queue-size", 0, "TreeDB (cached): per-worker ingress queue length (0=default)")
)

func init() {
	RegisterDB("treedb", NewTreeDB)
	RegisterAlias("treedbcached", "treedb")
	RegisterHiddenDB("treedb_public_command_wal", NewTreeDBPublicCommandWAL)
	RegisterAlias("treedb_cached_command_wal", "treedb_public_command_wal")
	RegisterHiddenDB("treedb_bench_unsafe", NewTreeDBBenchUnsafe)
	RegisterHiddenDB("treedb_backend", NewTreeDBBackend)
	RegisterHiddenDB("treedb_backend_command_wal", NewTreeDBBackendCommandWAL)
	RegisterAlias("treedb_command_wal", "treedb_backend_command_wal")
	RegisterHiddenDB("treedb_vlog_off", NewTreeDBVlogOff)
	RegisterHiddenDB("treedb_vlog_dict", NewTreeDBVlogDict)
	RegisterHiddenDB("treedb_vlog_block_snappy", NewTreeDBVlogBlockSnappy)
	RegisterHiddenDB("treedb_vlog_block_lz4", NewTreeDBVlogBlockLZ4)
	RegisterHiddenDB("treedb_vlog_block_zstd", NewTreeDBVlogBlockZSTD)
	RegisterHiddenDB("treedb_vlog_auto", NewTreeDBVlogAuto)
	RegisterHiddenDB("treedb_vlog_dict_off", NewTreeDBVlogDictOff)
	RegisterHiddenDB("treedb_vlog_dict_on", NewTreeDBVlogDictOn)
	RegisterHiddenDB("treedb_vlog_dict_on_entropy", func(dir string) (kvstore.DB, error) {
		return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelFastest, true, "TreeDB (vlog_dict=on, level=fastest, entropy=on)")
	})
	RegisterHiddenDB("treedb_vlog_dict_on_level_default", func(dir string) (kvstore.DB, error) {
		return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelDefault, false, "TreeDB (vlog_dict=on, level=default, entropy=off)")
	})
	RegisterHiddenDB("treedb_vlog_dict_on_level_default_entropy", func(dir string) (kvstore.DB, error) {
		return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelDefault, true, "TreeDB (vlog_dict=on, level=default, entropy=on)")
	})
	RegisterHiddenDB("treedb_vlog_dict_on_level_better", func(dir string) (kvstore.DB, error) {
		return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelBetter, false, "TreeDB (vlog_dict=on, level=better, entropy=off)")
	})
	RegisterHiddenDB("treedb_vlog_dict_on_level_better_entropy", func(dir string) (kvstore.DB, error) {
		return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelBetter, true, "TreeDB (vlog_dict=on, level=better, entropy=on)")
	})
	RegisterHiddenDB("treedb_vlog_dict_on_level_best", func(dir string) (kvstore.DB, error) {
		return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelBest, false, "TreeDB (vlog_dict=on, level=best, entropy=off)")
	})
	RegisterHiddenDB("treedb_vlog_dict_on_level_best_entropy", func(dir string) (kvstore.DB, error) {
		return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelBest, true, "TreeDB (vlog_dict=on, level=best, entropy=on)")
	})
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

func parseTreeDBVlogCompressionMode(s string) (treedb.ValueLogCompressionMode, bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "default", "unset":
		return treedb.ValueLogCompressionAuto, false, nil
	case "off", "false", "0":
		return treedb.ValueLogCompressionOff, true, nil
	case "block":
		return treedb.ValueLogCompressionBlock, true, nil
	case "dict":
		return treedb.ValueLogCompressionDict, true, nil
	case "auto":
		return treedb.ValueLogCompressionAuto, true, nil
	default:
		return treedb.ValueLogCompressionOff, false, fmt.Errorf("unsupported -treedb-vlog-compression=%q (expected default|off|block|dict|auto)", s)
	}
}

func parseTreeDBVlogBlockCodec(s string) (treedb.ValueLogBlockCodec, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "snappy":
		return treedb.ValueLogBlockSnappy, nil
	case "lz4":
		return treedb.ValueLogBlockLZ4, nil
	case "zstd":
		return treedb.ValueLogBlockZSTD, nil
	default:
		return treedb.ValueLogBlockSnappy, fmt.Errorf("unsupported -treedb-vlog-block-codec=%q (expected snappy|lz4|zstd)", s)
	}
}

func parseTreeDBVlogAutoPolicy(s string) (treedb.ValueLogAutoPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "balanced", "default":
		return treedb.ValueLogAutoBalanced, nil
	case "throughput", "speed":
		return treedb.ValueLogAutoThroughput, nil
	case "size":
		return treedb.ValueLogAutoSize, nil
	default:
		return treedb.ValueLogAutoBalanced, fmt.Errorf("unsupported -treedb-vlog-auto-policy=%q (expected balanced|throughput|size)", s)
	}
}

func parseTreeDBVlogGenerationPolicy(s string) (treedb.ValueLogGenerationPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "default":
		return treedb.ValueLogGenerationDefault, nil
	case "off":
		return treedb.ValueLogGenerationOff, nil
	case "hot_warm_cold", "hotwarmcold", "generational":
		return treedb.ValueLogGenerationHotWarmCold, nil
	default:
		return treedb.ValueLogGenerationOff, fmt.Errorf("unsupported -treedb-vlog-generation-policy=%q (expected default|off|hot_warm_cold)", s)
	}
}

func formatTreeDBVlogGenerationPolicy(p treedb.ValueLogGenerationPolicy) string {
	switch p {
	case treedb.ValueLogGenerationDefault:
		return "default"
	case treedb.ValueLogGenerationOff:
		return "off"
	case treedb.ValueLogGenerationHotWarmCold:
		return "hot_warm_cold"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

func normalizeTreeDBMaintenanceMode(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "normal":
		return "normal", nil
	case "bench", "benchmark":
		return "bench", nil
	default:
		return "", fmt.Errorf("unsupported -treedb-maintenance-mode=%q (expected normal|bench)", s)
	}
}

func formatTreeDBDefaultedInt(v, effective int) string {
	if v <= 0 {
		return fmt.Sprintf("default (effective=%d)", effective)
	}
	return fmt.Sprintf("%d", v)
}

func formatTreeDBDefaultedInt64(v, effective int64) string {
	if v <= 0 {
		return fmt.Sprintf("default (effective=%d)", effective)
	}
	return fmt.Sprintf("%d", v)
}

func formatTreeDBDefaultedFloat(v, effective float64) string {
	if v <= 0 {
		return fmt.Sprintf("default (effective=%.6f)", effective)
	}
	return fmt.Sprintf("%.6f", v)
}

func formatTreeDBFlushBacklogCoalescingMaxMemtables(v int) string {
	if v <= 0 {
		return fmt.Sprintf("default (effective=%d)", defaultTreeDBFlushBacklogCoalescingMaxMemtables)
	}
	if v > defaultTreeDBFlushBacklogCoalescingHardMaxMemtables {
		return fmt.Sprintf("%d (effective=%d cap)", v, defaultTreeDBFlushBacklogCoalescingHardMaxMemtables)
	}
	return fmt.Sprintf("%d", v)
}

func formatTreeDBFlushBacklogCoalescingSingleOpRatio(v float64) string {
	if v <= 0 {
		return fmt.Sprintf("default (effective=%.6f)", defaultTreeDBFlushBacklogCoalescingSingleOpRatio)
	}
	if v > 1 {
		return fmt.Sprintf("%.6f (effective=1.000000 cap)", v)
	}
	return fmt.Sprintf("%.6f", v)
}

func parseTreeDBFlushAdmissionPolicy(raw string) (treedb.FlushAdmissionPolicy, error) {
	policy, err := treedbdb.ParseFlushAdmissionPolicy(raw)
	if err != nil {
		return policy, fmt.Errorf("TreeDB: %w", err)
	}
	return policy, nil
}

type treeDBOptionsReport struct {
	opts            treedb.Options
	maintenanceMode string
	notes           []string
	warnings        []string
}

func (r treeDBOptionsReport) hasReport() bool {
	if len(r.notes) > 0 || len(r.warnings) > 0 {
		return true
	}
	return r.opts.Dir != ""
}

func (r treeDBOptionsReport) formatText(indent string) string {
	var lines []string
	lines = append(lines, fmt.Sprintf("profile_resolved=%s", r.opts.ResolvedProfile))
	lines = append(lines, fmt.Sprintf("durability=%s", formatTreeDBDurability(r.opts.Durability)))
	lines = append(lines, fmt.Sprintf("read_integrity=%s", formatTreeDBIntegrity(r.opts.ValueLog.ReadIntegrity)))
	if strings.TrimSpace(r.maintenanceMode) != "" {
		lines = append(lines, fmt.Sprintf("maintenance_mode=%s", strings.TrimSpace(r.maintenanceMode)))
	}
	lines = append(lines, fmt.Sprintf("index_optimizations=%t",
		r.opts.LeafPrefixCompression &&
			r.opts.IndexColumnarLeaves &&
			r.opts.IndexPackedValuePtr &&
			r.opts.IndexInternalBaseDelta))
	lines = append(lines, fmt.Sprintf("leaf_prefix_compression=%t", r.opts.LeafPrefixCompression))
	lines = append(lines, fmt.Sprintf("index_columnar_leaves=%t", r.opts.IndexColumnarLeaves))
	lines = append(lines, fmt.Sprintf("index_packed_valueptr=%t", r.opts.IndexPackedValuePtr))
	lines = append(lines, fmt.Sprintf("index_internal_base_delta=%t", r.opts.IndexInternalBaseDelta))
	lines = append(lines, fmt.Sprintf("index_outer_leaves_in_vlog=%t", r.opts.IndexOuterLeavesInValueLog))
	lines = append(lines, fmt.Sprintf("outer_leaf_read_cache_entries=%s", formatTreeDBLeafPageReadCacheEntries(r.opts.LeafPageReadCacheEntries)))
	lines = append(lines, fmt.Sprintf("outer_leaf_read_cache_write_admission=%s", r.opts.LeafPageReadCacheWriteAdmission.String()))
	lines = append(lines, fmt.Sprintf("cached.domain_ingress_workers=%d", r.opts.DomainIngressWorkers))
	lines = append(lines, fmt.Sprintf("cached.domain_ingress_queue_size=%d", r.opts.DomainIngressQueueSize))
	admission := treedbdb.FlushAdmissionDecisionForOptions(r.opts)
	lines = append(lines, fmt.Sprintf("flush_admission_policy=%s", admission.Policy.String()))
	lines = append(lines, fmt.Sprintf("flush_admission_admitted=%t", admission.Admitted))
	lines = append(lines, fmt.Sprintf("flush_admission_reason=%s", admission.Reason))
	lines = append(lines, fmt.Sprintf("flush_admission_configured_concurrency=%d", admission.FlushApplyConcurrencyConfigured))
	lines = append(lines, fmt.Sprintf("flush_admission_effective_concurrency=%d", admission.FlushApplyConcurrency))
	lines = append(lines, fmt.Sprintf("flush_admission_concurrency_cap_reason=%s", admission.FlushApplyConcurrencyCapReason))
	lines = append(lines, fmt.Sprintf("flush_admission_concurrency_defaulted=%t", admission.FlushApplyConcurrencyDefaulted))
	lines = append(lines, fmt.Sprintf("runtime_gomaxprocs=%d", admission.RuntimeGOMAXPROCS))
	lines = append(lines, fmt.Sprintf("flush_admission_flush_apply_span_native=%t", admission.FlushApplySpanNative))
	lines = append(lines, fmt.Sprintf("flush_admission_flush_backlog_coalescing=%t", admission.FlushBacklogCoalescing))
	if admission.PhysicalCores > 0 {
		lines = append(lines, fmt.Sprintf("hardware_physical_cores=%d", admission.PhysicalCores))
	} else {
		lines = append(lines, "hardware_physical_cores=unknown")
	}
	lines = append(lines, fmt.Sprintf("flush_apply_concurrency=%d", r.opts.FlushApplyConcurrency))
	lines = append(lines, fmt.Sprintf("flush_apply_min_entries_configured=%d", r.opts.FlushApplyMinEntries))
	lines = append(lines, fmt.Sprintf("flush_apply_min_spans_configured=%d", r.opts.FlushApplyMinSpans))
	lines = append(lines, fmt.Sprintf("flush_apply_min_bytes_configured=%d", r.opts.FlushApplyMinBytes))
	lines = append(lines, fmt.Sprintf("flush_apply_span_native=%t", r.opts.FlushApplySpanNative))
	lines = append(lines, fmt.Sprintf("flush_span_run_target_planning=%t", r.opts.FlushSpanRunTargetPlanning))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing=%t", r.opts.FlushBacklogCoalescing))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing_max_memtables=%s", formatTreeDBFlushBacklogCoalescingMaxMemtables(r.opts.FlushBacklogCoalescingMaxMemtables)))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing_max_bytes=%s", formatTreeDBDefaultedInt64(r.opts.FlushBacklogCoalescingMaxBytes, defaultTreeDBFlushBacklogCoalescingMaxBytes)))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing_max_ops=%s", formatTreeDBDefaultedInt(r.opts.FlushBacklogCoalescingMaxOps, defaultTreeDBFlushBacklogCoalescingMaxOps)))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing_min_age_ms=%d", r.opts.FlushBacklogCoalescingMinAge/time.Millisecond))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing_single_op_ratio=%s", formatTreeDBFlushBacklogCoalescingSingleOpRatio(r.opts.FlushBacklogCoalescingSingleOpSpanRatio)))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing_max_ops_per_span=%s", formatTreeDBDefaultedFloat(r.opts.FlushBacklogCoalescingMaxOpsPerSpan, defaultTreeDBFlushBacklogCoalescingMaxOpsPerSpan)))
	lines = append(lines, fmt.Sprintf("flush_backlog_coalescing_min_old_leaf_bytes_per_op=%.6f", r.opts.FlushBacklogCoalescingMinOldLeafBytesPerOp))
	journalLanes := treedbdb.ResolveJournalLaneDefaults(r.opts.JournalLanes, runtime.GOMAXPROCS(0), treedbdb.DetectPhysicalCores(), r.opts.ValueLog.Generational.Policy)
	lines = append(lines, fmt.Sprintf("journal_lanes_configured=%d", r.opts.JournalLanes))
	lines = append(lines, fmt.Sprintf("journal_lanes_effective_default=%d", journalLanes.Effective))
	lines = append(lines, fmt.Sprintf("journal_lanes_defaulted=%t", journalLanes.Defaulted))
	lines = append(lines, fmt.Sprintf("journal_lanes_hot=%d", journalLanes.HotLanes))
	lines = append(lines, fmt.Sprintf("journal_lanes_warm=%d", journalLanes.WarmLanes))
	lines = append(lines, fmt.Sprintf("journal_lanes_cold=%d", journalLanes.ColdLanes))
	lines = append(lines, fmt.Sprintf("vlog.force_pointers=%t", r.opts.ValueLog.ForcePointers))

	threshold := r.opts.ValueLog.PointerThreshold
	if threshold <= 0 {
		effective := page.DefaultInlineThreshold
		lines = append(lines, fmt.Sprintf("vlog.pointer_threshold=default (effective=%dB)", effective))
	} else {
		lines = append(lines, fmt.Sprintf("vlog.pointer_threshold=%dB", threshold))
	}
	lines = append(lines, fmt.Sprintf("vlog.compression=%s", formatTreeDBVlogCompression(r.opts.ValueLog.Compression)))
	lines = append(lines, fmt.Sprintf("vlog.block_codec=%s", formatTreeDBVlogBlockCodec(r.opts.ValueLog.BlockCodec)))
	lines = append(lines, fmt.Sprintf("vlog.auto_policy=%s", formatTreeDBVlogAutoPolicy(r.opts.ValueLog.AutoPolicy)))
	if mode := r.opts.ValueLog.CompressionAutotune.Mode; mode == treedb.AutotuneUnset {
		lines = append(lines, "vlog.compression_autotune=default (effective=medium)")
	} else {
		lines = append(lines, fmt.Sprintf("vlog.compression_autotune=%s", formatTreeDBVlogCompressionAutotune(mode)))
	}
	lines = append(lines, fmt.Sprintf("vlog.dict_class_mode=%s", formatTreeDBVlogDictClassMode(r.opts.ValueLog.DictClassMode)))
	lines = append(lines, fmt.Sprintf("vlog.generation_policy=%s", formatTreeDBVlogGenerationPolicy(r.opts.ValueLog.Generational.Policy)))
	lines = append(lines, fmt.Sprintf("vlog.generation_hot_segment_bytes=%d", r.opts.ValueLog.Generational.HotSegmentTargetBytes))
	lines = append(lines, fmt.Sprintf("vlog.generation_warm_segment_bytes=%d", r.opts.ValueLog.Generational.WarmSegmentTargetBytes))
	lines = append(lines, fmt.Sprintf("vlog.generation_cold_segment_bytes=%d", r.opts.ValueLog.Generational.ColdSegmentTargetBytes))
	lines = append(lines, fmt.Sprintf("vlog.rewrite_budget_bytes_per_sec=%d", r.opts.ValueLog.Generational.RewriteBudgetBytesPerSec))
	lines = append(lines, fmt.Sprintf("vlog.rewrite_budget_records_per_sec=%d", r.opts.ValueLog.Generational.RewriteBudgetRecordsPerSec))
	lines = append(lines, fmt.Sprintf("vlog.rewrite_trigger_stale_ratio_ppm=%d", r.opts.ValueLog.Generational.RewriteTriggerStaleRatioPPM))
	lines = append(lines, fmt.Sprintf("vlog.rewrite_trigger_total_bytes=%d", r.opts.ValueLog.Generational.RewriteTriggerTotalBytes))
	lines = append(lines, fmt.Sprintf("vlog.rewrite_trigger_churn_per_sec=%d", r.opts.ValueLog.Generational.RewriteTriggerChurnPerSec))
	if minAge := r.opts.ValueLog.Generational.RewriteMinSegmentAge; minAge <= 0 {
		lines = append(lines, fmt.Sprintf("vlog.rewrite_min_segment_age_ms=default (effective=%d)", int((30*time.Second)/time.Millisecond)))
	} else {
		lines = append(lines, fmt.Sprintf("vlog.rewrite_min_segment_age_ms=%d", int(minAge/time.Millisecond)))
	}
	if target := r.opts.ValueLog.BlockTargetCompressedBytes; target <= 0 {
		lines = append(lines, "vlog.block_target_bytes=default (effective=4096B)")
	} else {
		lines = append(lines, fmt.Sprintf("vlog.block_target_bytes=%dB", target))
	}
	if hold := r.opts.ValueLog.IncompressibleHoldBytes; hold <= 0 {
		lines = append(lines, "vlog.incompressible_hold_bytes=default (effective=67108864B)")
	} else {
		lines = append(lines, fmt.Sprintf("vlog.incompressible_hold_bytes=%dB", hold))
	}
	if probe := r.opts.ValueLog.IncompressibleProbeIntervalBytes; probe <= 0 {
		lines = append(lines, "vlog.incompressible_probe_bytes=default (effective=8388608B)")
	} else {
		lines = append(lines, fmt.Sprintf("vlog.incompressible_probe_bytes=%dB", probe))
	}

	// Keep output stable and readable for copy/paste.
	if len(r.warnings) > 0 {
		sort.Strings(r.warnings)
		lines = append(lines, "warnings:")
		for _, w := range r.warnings {
			lines = append(lines, "  - "+w)
		}
	}
	if len(r.notes) > 0 {
		sort.Strings(r.notes)
		lines = append(lines, "notes:")
		for _, n := range r.notes {
			lines = append(lines, "  - "+n)
		}
	}

	var sb strings.Builder
	for i, line := range lines {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(indent)
		sb.WriteString(line)
	}
	return sb.String()
}

func formatTreeDBDurability(mode treedb.DurabilityMode) string {
	switch mode {
	case treedb.DurabilityDurable:
		return "durable"
	case treedb.DurabilityWALOnRelaxed:
		return "wal_on_relaxed"
	case treedb.DurabilityWALOffRelaxed:
		return "wal_off_relaxed"
	default:
		return fmt.Sprintf("durability_%d", mode)
	}
}

func formatTreeDBIntegrity(mode treedb.IntegrityMode) string {
	switch mode {
	case treedb.IntegrityVerify:
		return "verify"
	case treedb.IntegritySkipChecksums:
		return "skip_checksums"
	default:
		return fmt.Sprintf("integrity_%d", mode)
	}
}

func formatTreeDBLeafPageReadCacheEntries(entries int) string {
	switch {
	case entries < 0:
		return "disabled"
	case entries == 0:
		effective, err := treedbdb.ResolveLeafPageReadCacheEntries(entries)
		if err != nil {
			return fmt.Sprintf("default/env (invalid: %v)", err)
		}
		return fmt.Sprintf("default/env (effective=%d)", effective)
	default:
		return fmt.Sprintf("%d", entries)
	}
}

func parseTreeDBLeafPageReadCacheWriteAdmission(raw string) (treedb.LeafPageReadCacheWriteAdmissionPolicy, error) {
	policy, err := treedbdb.ParseLeafPageReadCacheWriteAdmissionPolicy(raw)
	if err != nil {
		return policy, fmt.Errorf("TreeDB: %w", err)
	}
	return policy, nil
}

func formatTreeDBVlogCompression(mode treedb.ValueLogCompressionMode) string {
	switch mode {
	case 0:
		return "default"
	case treedb.ValueLogCompressionOff:
		return "off"
	case treedb.ValueLogCompressionBlock:
		return "block"
	case treedb.ValueLogCompressionDict:
		return "dict"
	case treedb.ValueLogCompressionAuto:
		return "auto"
	default:
		return fmt.Sprintf("compression_%d", mode)
	}
}

func formatTreeDBVlogCompressionFlagValue(mode treedb.ValueLogCompressionMode) string {
	if mode == 0 {
		return "default"
	}
	return formatTreeDBVlogCompression(mode)
}

func formatTreeDBVlogBlockCodec(codec treedb.ValueLogBlockCodec) string {
	switch codec {
	case treedb.ValueLogBlockSnappy:
		return "snappy"
	case treedb.ValueLogBlockLZ4:
		return "lz4"
	case treedb.ValueLogBlockZSTD:
		return "zstd"
	default:
		return fmt.Sprintf("block_codec_%d", codec)
	}
}

func formatTreeDBVlogAutoPolicy(policy treedb.ValueLogAutoPolicy) string {
	switch policy {
	case treedb.ValueLogAutoBalanced:
		return "balanced"
	case treedb.ValueLogAutoThroughput:
		return "throughput"
	case treedb.ValueLogAutoSize:
		return "size"
	default:
		return fmt.Sprintf("auto_policy_%d", policy)
	}
}

func formatTreeDBVlogCompressionAutotune(mode treedb.AutotuneMode) string {
	switch mode {
	case treedb.AutotuneUnset:
		return "default"
	case treedb.AutotuneOff:
		return "off"
	case treedb.AutotuneMedium:
		return "medium"
	case treedb.AutotuneAggressive:
		return "aggressive"
	default:
		return fmt.Sprintf("autotune_%d", mode)
	}
}

func formatTreeDBVlogDictClassMode(mode treedb.ValueLogDictClassMode) string {
	switch mode {
	case treedb.ValueLogDictClassSingle:
		return "single"
	case treedb.ValueLogDictClassSplitOuterLeaf:
		return "split_outer_leaf"
	default:
		return fmt.Sprintf("dict_class_mode_%d", mode)
	}
}

func parseTreeDBVlogDictClassMode(s string) (treedb.ValueLogDictClassMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "default", "single":
		return treedb.ValueLogDictClassSingle, nil
	case "split_outer_leaf":
		return treedb.ValueLogDictClassSplitOuterLeaf, nil
	default:
		return treedb.ValueLogDictClassSingle, fmt.Errorf("unsupported -treedb-vlog-dict-class-mode=%q (expected single|split_outer_leaf)", s)
	}
}

func resolveIndexOptimizationBool(flagName string, flagValue bool, compositeValue bool, compositeExplicit bool) bool {
	if flagExplicit(flagName) {
		return flagValue
	}
	if compositeExplicit {
		return compositeValue
	}
	if compositeValue {
		return true
	}
	return flagValue
}

type treeDBOptionsBuildConfig struct {
	forceWALOn           bool
	forceBenchmarkUnsafe bool
}

func buildTreeDBOptions(dir string) (treedb.Options, treeDBOptionsReport, error) {
	return buildTreeDBOptionsWithConfig(dir, treeDBOptionsBuildConfig{})
}

func buildTreeDBOptionsWithConfig(dir string, cfg treeDBOptionsBuildConfig) (treedb.Options, treeDBOptionsReport, error) {
	treedbcaching.SetIteratorDebug(*treedbIterDebug)

	if cfg.forceWALOn && cfg.forceBenchmarkUnsafe {
		return treedb.Options{}, treeDBOptionsReport{}, fmt.Errorf("TreeDB: forceWALOn and forceBenchmarkUnsafe are mutually exclusive")
	}
	disableWALEffective := cfg.forceBenchmarkUnsafe || (*treedbDisableWAL && !cfg.forceWALOn)
	manualUnsafe := (*treedbDisableWAL && !cfg.forceWALOn) || *treedbRelaxedSync || *treedbDisableReadChecksum
	if !cfg.forceBenchmarkUnsafe && !*treedbAllowUnsafe && manualUnsafe {
		return treedb.Options{}, treeDBOptionsReport{}, fmt.Errorf("TreeDB: unsafe flags require -treedb-allow-unsafe")
	}
	resolvedProfile, benchmarkProfile, err := resolveUnifiedBenchTreeDBProfile(disableWALEffective, cfg.forceBenchmarkUnsafe)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	maintenanceMode, err := normalizeTreeDBMaintenanceMode(*treedbMaintenanceMode)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}

	durability := treedb.DurabilityDurable
	if disableWALEffective {
		durability = treedb.DurabilityWALOffRelaxed
	} else if *treedbRelaxedSync {
		durability = treedb.DurabilityWALOnRelaxed
	}
	readIntegrity := treedb.IntegrityVerify
	if cfg.forceBenchmarkUnsafe || *treedbDisableReadChecksum {
		readIntegrity = treedb.IntegritySkipChecksums
	}

	indexOptimizationsRequested := *treedbIndexOptimizations
	indexOptimizationsExplicit := flagExplicit("treedb-index-optimizations")
	forcePointersEffective := *treedbForceValuePointers
	leafPrefixEffective := resolveIndexOptimizationBool("treedb-leaf-prefix-compression", *treedbLeafPrefixCompression, indexOptimizationsRequested, indexOptimizationsExplicit)
	columnarLeavesEffective := resolveIndexOptimizationBool("treedb-index-columnar-leaves", *treedbIndexColumnarLeaves, indexOptimizationsRequested, indexOptimizationsExplicit)
	packedValuePtrEffective := resolveIndexOptimizationBool("treedb-index-packed-valueptr", *treedbIndexPackedValuePtr, indexOptimizationsRequested, indexOptimizationsExplicit)
	internalBaseDeltaEffective := resolveIndexOptimizationBool("treedb-index-internal-base-delta", *treedbIndexInternalBaseDelta, indexOptimizationsRequested, indexOptimizationsExplicit)
	var notes []string
	var warnings []string
	if indexOptimizationsRequested {
		notes = append(notes, "index_optimizations enables leaf prefix compression + columnar leaves + packed value pointers + internal base-delta")
	}
	if leafPrefixEffective {
		notes = append(notes, "leaf_prefix_compression uses front-coding with restart points (compact v2 leaf entry header for new pages)")
	}
	if leafPrefixEffective && columnarLeavesEffective {
		notes = append(notes, "leaf_prefix_compression + index_columnar_leaves: enabling combined columnar+prefix leaf encoding for new pages")
	}
	if disableWALEffective && *treedbRelaxedSync {
		// This is not an error, but it can be confusing. Document precedence.
		notes = append(notes, "disable_wal takes precedence over relaxed_sync (durability=wal_off_relaxed)")
	} else if cfg.forceWALOn && *treedbDisableWAL {
		notes = append(notes, "command_wal_v1 forces WAL on; ignoring -treedb-disable-wal for this variant")
	}
	if packedValuePtrEffective {
		notes = append(notes, "index_packed_valueptr uses a packed 12B leaf ValuePtr encoding (u32 offset cap; cached mode rotates value-log segments automatically)")
	}
	if *treedbIndexOuterLeavesInVlog && internalBaseDeltaEffective {
		internalBaseDeltaEffective = false
		notes = append(notes, "index_internal_base_delta disabled: leaf-log child pages use explicit LogRecordRef entries")
	}
	writeAdmissionPolicy, err := parseTreeDBLeafPageReadCacheWriteAdmission(*treedbLeafPageReadCacheWriteAdmission)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	flushAdmissionPolicy, err := parseTreeDBFlushAdmissionPolicy(*treedbFlushAdmissionPolicy)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	if writeAdmissionPolicy == treedb.LeafPageReadCacheWriteAdmissionAdaptive {
		notes = append(notes, "outer_leaf_read_cache_write_admission=adaptive is opt-in; skipped writes remain durable and read misses can still admit")
	}

	opts := treedb.Options{
		Dir:                             dir,
		KeepRecent:                      *treedbKeepRecent,
		Durability:                      durability,
		ChunkSize:                       *treedbChunkSize,
		PagerSyncConcurrency:            *treedbPagerSyncConcurrency,
		PagerMmapPopulate:               *treedbPagerMmapPopulate,
		PagerPrefetchOnRead:             *treedbPagerPrefetchOnRead,
		LeafPageReadCacheEntries:        *treedbLeafPageReadCacheEntries,
		LeafPageReadCacheWriteAdmission: writeAdmissionPolicy,
		PreferAppendAlloc:               *treedbPreferAppendAlloc,
		FreelistRegionPages:             *treedbFreelistRegionPages,
		FreelistRegionRadius:            *treedbFreelistRegionRadius,
		LeafFillTargetPPM:               uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM:           uint32(clampPPM(*treedbInternalFillPPM)),
		MaintenanceOpsPerCoalesce:       *treedbMaintenanceOpsPerCoalesce,

		LeafPrefixCompression:      leafPrefixEffective,
		IndexColumnarLeaves:        columnarLeavesEffective,
		IndexPackedValuePtr:        packedValuePtrEffective,
		IndexInternalBaseDelta:     internalBaseDeltaEffective,
		IndexOuterLeavesInValueLog: *treedbIndexOuterLeavesInVlog,

		MemtableMode:                               *treedbMemtableMode,
		DomainIngressWorkers:                       *treedbDomainIngressWorkers,
		DomainIngressQueueSize:                     *treedbDomainIngressQueueSize,
		FlushThreshold:                             *treedbFlushThreshold,
		MaxQueuedMemtables:                         *treedbMaxQueuedMems,
		SlowdownBacklogSeconds:                     *treedbSlowdownBacklogSeconds,
		StopBacklogSeconds:                         *treedbStopBacklogSeconds,
		MaxBacklogBytes:                            *treedbMaxBacklogBytes,
		WriterFlushMaxMemtables:                    *treedbWriterFlushMaxMems,
		FlushBuildConcurrency:                      *treedbFlushBuildConcurrency,
		FlushBuildMinEntries:                       *treedbFlushBuildMinEntries,
		FlushBuildMinUnits:                         *treedbFlushBuildMinUnits,
		FlushBuildChunkCap:                         *treedbFlushBuildChunkCap,
		FlushBuildChunkTargetBytes:                 *treedbFlushBuildChunkTarget,
		FlushBuildChunkMinBytes:                    *treedbFlushBuildChunkMinBytes,
		FlushBuildChunkMaxBytes:                    *treedbFlushBuildChunkMaxBytes,
		FlushBuildPrefetchUnits:                    *treedbFlushBuildPrefetchUnits,
		FlushAdmissionPolicy:                       flushAdmissionPolicy,
		FlushApplyConcurrency:                      *treedbFlushApplyConcurrency,
		FlushApplyMinEntries:                       *treedbFlushApplyMinEntries,
		FlushApplyMinSpans:                         *treedbFlushApplyMinSpans,
		FlushApplyMinBytes:                         *treedbFlushApplyMinBytes,
		FlushApplySpanNative:                       *treedbFlushApplySpanNative,
		FlushBackendMaxEntries:                     *treedbFlushBackendMaxEntries,
		FlushBackendMaxBatches:                     *treedbFlushBackendMaxBatches,
		FlushSpanRunTargetPlanning:                 *treedbFlushSpanRunTargetPlanning,
		FlushBacklogCoalescing:                     *treedbFlushBacklogCoalescing,
		FlushBacklogCoalescingMaxMemtables:         *treedbFlushBacklogCoalescingMaxMems,
		FlushBacklogCoalescingMaxBytes:             *treedbFlushBacklogCoalescingMaxBytes,
		FlushBacklogCoalescingMaxOps:               *treedbFlushBacklogCoalescingMaxOps,
		FlushBacklogCoalescingMinAge:               time.Duration(*treedbFlushBacklogCoalescingMinAgeMS) * time.Millisecond,
		FlushBacklogCoalescingSingleOpSpanRatio:    *treedbFlushBacklogCoalescingRatio,
		FlushBacklogCoalescingMaxOpsPerSpan:        *treedbFlushBacklogCoalescingOpsSpan,
		FlushBacklogCoalescingMinOldLeafBytesPerOp: *treedbFlushBacklogCoalescingOldLeafB,

		JournalLanes:               *treedbJournalLanes,
		JournalCompression:         *treedbJournalCompress,
		DisablePiggybackCompaction: *treedbDisablePiggyback,

		ValueLog: treedb.ValueLogOptions{
			ForcePointers:                    forcePointersEffective,
			PointerThreshold:                 *treedbValueLogThreshold,
			RawWritevMinAvgBytes:             *treedbVlogRawWritevMinAvgBytes,
			RawWritevMinBatchRecords:         *treedbVlogRawWritevMinBatchRecs,
			BlockTargetCompressedBytes:       *treedbVlogBlockTargetBytes,
			IncompressibleHoldBytes:          *treedbVlogIncompressibleHoldBytes,
			IncompressibleProbeIntervalBytes: *treedbVlogIncompressibleProbeBytes,
			ReadIntegrity:                    readIntegrity,
			DictAdaptiveRatio:                *treedbVlogDictAdaptiveRatio,
			DictMetricsWindowBytes:           *treedbVlogDictMetricsWindow,
			DictMetricsMinRecords:            *treedbVlogDictMetricsMinRecords,
			DictMetricsPauseBytes:            *treedbVlogDictMetricsPauseBytes,
			DictIncompressibleHoldBytes:      *treedbVlogDictIncompressibleHoldBytes,
			DictProbeIntervalBytes:           *treedbVlogDictProbeIntervalBytes,
			DictMinPayloadSavingsRatio:       *treedbVlogDictMinSavingsRatio,
		},
	}
	if *treedbWriterFlushMaxMs > 0 {
		opts.WriterFlushMaxDuration = time.Duration(*treedbWriterFlushMaxMs) * time.Millisecond
	}

	compressionMode, compressionExplicit, err := parseTreeDBVlogCompressionMode(*treedbVlogCompression)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	opts.ValueLog.Compression = compressionMode
	blockCodec, err := parseTreeDBVlogBlockCodec(*treedbVlogBlockCodec)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	opts.ValueLog.BlockCodec = blockCodec
	autoPolicy, err := parseTreeDBVlogAutoPolicy(*treedbVlogAutoPolicy)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	opts.ValueLog.AutoPolicy = autoPolicy
	dictClassMode, err := parseTreeDBVlogDictClassMode(*treedbVlogDictClassMode)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	opts.ValueLog.DictClassMode = dictClassMode
	genPolicy, err := parseTreeDBVlogGenerationPolicy(*treedbVlogGenerationPolicy)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	opts.ValueLog.Generational.Policy = genPolicy
	genPolicyExplicit := flagExplicit("treedb-vlog-generation-policy")
	if maintenanceMode == "normal" && !genPolicyExplicit {
		opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
		notes = append(notes, "maintenance_mode=normal defaults vlog.generation_policy=hot_warm_cold")
	}
	if maintenanceMode == "bench" && !genPolicyExplicit {
		opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
		notes = append(notes, "maintenance_mode=bench defaults vlog.generation_policy=off")
	}
	opts.ValueLog.Generational.HotSegmentTargetBytes = *treedbVlogGenerationHotSegmentBytes
	opts.ValueLog.Generational.WarmSegmentTargetBytes = *treedbVlogGenerationWarmSegmentBytes
	opts.ValueLog.Generational.ColdSegmentTargetBytes = *treedbVlogGenerationColdSegmentBytes
	opts.ValueLog.Generational.RewriteBudgetBytesPerSec = *treedbVlogRewriteBudgetBytesPerSec
	opts.ValueLog.Generational.RewriteBudgetRecordsPerSec = *treedbVlogRewriteBudgetRecordsPerSec
	opts.ValueLog.Generational.RewriteTriggerStaleRatioPPM = clampUint32(uint64(*treedbVlogRewriteTriggerStaleRatioPPM))
	opts.ValueLog.Generational.RewriteTriggerTotalBytes = *treedbVlogRewriteTriggerTotalBytes
	opts.ValueLog.Generational.RewriteTriggerChurnPerSec = *treedbVlogRewriteTriggerChurnPerSec
	opts.ValueLog.Generational.RewriteMinSegmentAge = time.Duration(*treedbVlogRewriteMinSegmentAgeMS) * time.Millisecond

	if maintenanceMode == "bench" {
		// Disable background maintenance loops. "bench" mode aims for stable
		// throughput measurements without background GC/vacuum/checkpoint noise.
		//
		// Note: these are TreeDB-level cached-mode loops; value-log generation
		// scheduling is controlled via vlog.generation_policy.
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		opts.MaxWALBytes = -1
		opts.BackgroundIndexVacuumInterval = -1
	}

	setOptionalVlogTrainConfig(&opts,
		*treedbVlogDictTrainBytes,
		*treedbVlogDictDictBytes,
		*treedbVlogDictMinRecords,
		*treedbVlogDictMaxRecordBytes,
		*treedbVlogDictSampleStride,
		*treedbVlogDictDedupWindow,
	)

	levelEngine, levels, err := parseTreeDBVlogDictFrameEncodeLevels("treedb-vlog-dict-frame-encode-level", *treedbVlogDictFrameEncodeLevel)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	if !levelEngine {
		if len(levels) == 1 {
			switch levels[0] {
			case "fastest":
				opts.ValueLog.DictFrameEncodeLevel = treedb.ZSTDLevelFastest
			case "default":
				opts.ValueLog.DictFrameEncodeLevel = treedb.ZSTDLevelDefault
			case "better":
				opts.ValueLog.DictFrameEncodeLevel = treedb.ZSTDLevelBetter
			case "best":
				opts.ValueLog.DictFrameEncodeLevel = treedb.ZSTDLevelBest
			}
		} else {
			warnings = append(warnings, "vlog_dict_frame_encode_level requests a matrix; use -treedb-vlog-dict=on/both to expand DB variants")
		}
	}

	entropyEngine, entropies, err := parseTreeDBVlogDictFrameEntropyMode("treedb-vlog-dict-frame-entropy", *treedbVlogDictFrameEntropyMode)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	if !entropyEngine {
		if len(entropies) == 1 {
			opts.ValueLog.DictFrameEnableEntropy = entropies[0]
		} else {
			warnings = append(warnings, "vlog_dict_frame_entropy requests a matrix; use -treedb-vlog-dict=on/both to expand DB variants")
		}
	}

	autotuneMode, err := parseVlogCompressionAutotuneMode(*treedbVlogCompressionAutotune)
	if err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	if compressionExplicit {
		switch compressionMode {
		case treedb.ValueLogCompressionOff:
			setOptionalVlogAutotuneMode(&opts, 1)
			if *treedbVlogDictTrainBytes > 0 {
				notes = append(notes, "vlog_compression=off: forcing vlog_dict_train_bytes=off")
			}
			setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
		case treedb.ValueLogCompressionBlock:
			setOptionalVlogAutotuneMode(&opts, 1)
			if *treedbVlogDictTrainBytes > 0 {
				notes = append(notes, "vlog_compression=block: forcing vlog_dict_train_bytes=off")
			}
			setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
		default:
			setOptionalVlogAutotuneMode(&opts, autotuneMode)
		}
	} else {
		setOptionalVlogAutotuneMode(&opts, autotuneMode)
		if autotuneMode == 1 {
			// Compression off should also force dict training off, even if the caller
			// specified training flags. TreeDB enforces the same invariant internally,
			// but we keep unified_bench behavior explicit and deterministic.
			if *treedbVlogDictTrainBytes > 0 {
				notes = append(notes, "vlog_compression_autotune=off: forcing vlog_dict_train_bytes=off")
			}
			setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
		}
	}
	if *treedbVlogDictK > 0 {
		opts.ValueLog.CompressionAutotune.CandidateK = []int{*treedbVlogDictK}
	}
	if opts.ValueLog.Compression == treedb.ValueLogCompressionAuto {
		notes = append(notes, "vlog.compression=auto selects the actual per-frame codec at runtime; vlog.block_codec is the forced block-mode/default block codec, not proof that auto used that codec. Use treedb.cache.vlog_auto.* and treedb.cache.vlog_write_mode.* stats for actual selection.")
	}
	if opts.ValueLog.ForcePointers && opts.ValueLog.PointerThreshold > 0 {
		notes = append(notes, "vlog.force_pointers=true: pointer_threshold does not affect pointer eligibility")
	}
	attachUnifiedBenchTreeDBProfile(&opts, resolvedProfile, benchmarkProfile)
	if _, err := treedbdb.ResolveLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries); err != nil {
		return treedb.Options{}, treeDBOptionsReport{}, err
	}
	admission := treedbdb.NormalizeFlushAdmissionOptions(&opts)
	if admission.Policy == treedb.FlushAdmissionPolicyOff {
		notes = append(notes, "flush_admission_policy=off force-disables span-native, backlog coalescing, and flush-apply concurrency")
	} else if admission.Policy == treedb.FlushAdmissionPolicyAuto && admission.Admitted {
		notes = append(notes, "flush_admission_policy=auto admitted: "+admission.Reason)
	} else if admission.Policy == treedb.FlushAdmissionPolicyAuto && !admission.Admitted {
		notes = append(notes, "flush_admission_policy=auto declined: "+admission.Reason)
	}

	rep := treeDBOptionsReport{opts: opts, maintenanceMode: maintenanceMode, notes: notes, warnings: warnings}
	return opts, rep, nil
}

func resolveUnifiedBenchTreeDBProfile(disableWAL, forceBenchmarkUnsafe bool) (treedb.Profile, bool, error) {
	if forceBenchmarkUnsafe {
		return treedb.ProfileBenchUnsafe, true, nil
	}
	if *treedbDisableReadChecksum {
		if !disableWAL {
			return "", false, fmt.Errorf("TreeDB: -treedb-disable-read-checksum is only available through the no-WAL bench_unsafe contract; use -profile fast or also set -treedb-disable-wal")
		}
		return treedb.ProfileBenchUnsafe, true, nil
	}
	if disableWAL {
		return treedb.ProfileNoWALFast, false, nil
	}
	if *treedbRelaxedSync {
		return treedb.ProfileCommandWALRelaxed, false, nil
	}
	return treedb.ProfileCommandWALDurable, false, nil
}

func attachUnifiedBenchTreeDBProfile(opts *treedb.Options, profile treedb.Profile, benchmark bool) {
	proof := treedb.Options{}
	if benchmark {
		treedb.ApplyBenchmarkProfile(&proof, profile)
	} else {
		treedb.ApplyProfile(&proof, profile)
	}
	opts.ResolvedProfile = proof.ResolvedProfile
	opts.DeprecatedProfileAlias = proof.DeprecatedProfileAlias
	opts.UnsafeBenchmarkProfile = proof.UnsafeBenchmarkProfile
}

func treeDBResolvedOptionsText(indent string) (string, error) {
	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		return "", err
	}
	// Avoid printing a misleading dir in a banner/report.
	opts.Dir = ""
	rep.opts = opts
	return rep.formatText(indent), nil
}

func wrapTreeDBAdapter(db *treedb.DB, name string) kvstore.DB {
	// Keep adapter ReadBatch worker policy aligned with -read-workers so any
	// BatchReader consumers use the same explicit concurrency budget as the
	// parallel read benchmarks. random_read_batch itself uses GetMany/Get.
	out := treedbadapter.WrapNamed(db, name)
	out.SetReadWorkers(resolveReadWorkers(*readWorkers))
	return out
}

func NewTreeDB(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}

	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	// Adapter/registry name: "treedb". Wrapper name: "TreeDB" (pretty display).
	return wrapTreeDBAdapter(db, "TreeDB"), nil
}

func NewTreeDBPublicCommandWAL(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptionsWithConfig(dir, treeDBOptionsBuildConfig{forceWALOn: true})
	if err != nil {
		return nil, err
	}
	opts.CommandWAL = true
	opts.CommandWALStatsScan = *treedbCommandWALStatsScan

	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB (public cached command_wal_v1)"), nil
}

func NewTreeDBBenchUnsafe(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptionsWithConfig(dir, treeDBOptionsBuildConfig{forceBenchmarkUnsafe: true})
	if err != nil {
		return nil, err
	}
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB"), nil
}

func resolvedTreeDBVlogCompressionModeForDictVariants() (uint64, error) {
	mode, err := parseVlogCompressionAutotuneMode(*treedbVlogCompressionAutotune)
	if err != nil {
		return 0, err
	}
	// If the caller hasn't enabled compression, but requested dict variants,
	// force a deterministic non-off mode so dict on/off comparisons are
	// meaningful.
	if mode == 0 || mode == 1 {
		return 2, nil // medium
	}
	return mode, nil
}

func resolvedTreeDBVlogTrainDefaults(trainBytes, dictBytes int) (int, int) {
	if trainBytes == 0 {
		// Match engine defaults so dict variants preserve self-tuning behavior
		// while still bootstrapping quickly in short benchmark runs.
		trainBytes = 1 << 20
	}
	if trainBytes < 0 {
		return trainBytes, dictBytes
	}
	if dictBytes <= 0 {
		dictBytes = 32 << 10
	}
	return trainBytes, dictBytes
}

func NewTreeDBVlogDictOff(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	mode, err := resolvedTreeDBVlogCompressionModeForDictVariants()
	if err != nil {
		return nil, err
	}
	setOptionalVlogAutotuneMode(&opts, mode)
	setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)

	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB (vlog_dict=off)"), nil
}

func NewTreeDBVlogOff(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	setOptionalVlogAutotuneMode(&opts, 1)
	setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB (vlog=off)"), nil
}

func NewTreeDBVlogDict(dir string) (kvstore.DB, error) {
	return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelFastest, false, "TreeDB (vlog=dict)")
}

func NewTreeDBVlogBlockSnappy(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.ValueLog.Compression = treedb.ValueLogCompressionBlock
	opts.ValueLog.BlockCodec = treedb.ValueLogBlockSnappy
	setOptionalVlogAutotuneMode(&opts, 1)
	setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB (vlog=block/snappy)"), nil
}

func NewTreeDBVlogBlockLZ4(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.ValueLog.Compression = treedb.ValueLogCompressionBlock
	opts.ValueLog.BlockCodec = treedb.ValueLogBlockLZ4
	setOptionalVlogAutotuneMode(&opts, 1)
	setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB (vlog=block/lz4)"), nil
}

func NewTreeDBVlogBlockZSTD(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.ValueLog.Compression = treedb.ValueLogCompressionBlock
	opts.ValueLog.BlockCodec = treedb.ValueLogBlockZSTD
	setOptionalVlogAutotuneMode(&opts, 1)
	setOptionalVlogTrainConfig(&opts, -1, 0, 0, 0, 0, 0)
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB (vlog=block/zstd)"), nil
}

func NewTreeDBVlogAuto(dir string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.ValueLog.Compression = treedb.ValueLogCompressionAuto

	mode, err := resolvedTreeDBVlogCompressionModeForDictVariants()
	if err != nil {
		return nil, err
	}
	setOptionalVlogAutotuneMode(&opts, mode)

	trainBytes := *treedbVlogDictTrainBytes
	dictBytes := *treedbVlogDictDictBytes
	trainBytes, dictBytes = resolvedTreeDBVlogTrainDefaults(trainBytes, dictBytes)
	setOptionalVlogTrainConfig(&opts,
		trainBytes,
		dictBytes,
		*treedbVlogDictMinRecords,
		*treedbVlogDictMaxRecordBytes,
		*treedbVlogDictSampleStride,
		*treedbVlogDictDedupWindow,
	)

	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, "TreeDB (vlog=auto)"), nil
}

func newTreeDBVlogDictOnVariant(dir string, level treedb.ZSTDEncoderLevel, enableEntropy bool, wrapperName string) (kvstore.DB, error) {
	opts, _, err := buildTreeDBOptions(dir)
	if err != nil {
		return nil, err
	}
	opts.ValueLog.Compression = treedb.ValueLogCompressionDict
	mode, err := resolvedTreeDBVlogCompressionModeForDictVariants()
	if err != nil {
		return nil, err
	}
	setOptionalVlogAutotuneMode(&opts, mode)

	trainBytes := *treedbVlogDictTrainBytes
	dictBytes := *treedbVlogDictDictBytes
	trainBytes, dictBytes = resolvedTreeDBVlogTrainDefaults(trainBytes, dictBytes)

	setOptionalVlogTrainConfig(&opts,
		trainBytes,
		dictBytes,
		*treedbVlogDictMinRecords,
		*treedbVlogDictMaxRecordBytes,
		*treedbVlogDictSampleStride,
		*treedbVlogDictDedupWindow,
	)

	opts.ValueLog.DictFrameEncodeLevel = level
	opts.ValueLog.DictFrameEnableEntropy = enableEntropy

	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return wrapTreeDBAdapter(db, wrapperName), nil
}

func NewTreeDBVlogDictOn(dir string) (kvstore.DB, error) {
	return newTreeDBVlogDictOnVariant(dir, treedb.ZSTDLevelFastest, false, "TreeDB (vlog_dict=on)")
}

func logResolvedTreeDBOptions() {
	dbNames := resolveDBs(*dbsArg, *dbsExcludeArg)
	if !contains(dbNames, "treedb") {
		return
	}
	text, err := treeDBResolvedOptionsText("  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "TreeDB options: (error: %v)\n\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "TreeDB options (resolved):\n%s\n\n", text)
}
