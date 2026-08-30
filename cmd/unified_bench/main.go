package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/cmd/internal/treedbstats"
	"github.com/snissn/gomap/internal/benchprof"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

// --- Benchmark Runner ---

var (
	numKeys                   = flag.Int("keys", 100000, "Number of keys")
	keyShapeArg               = flag.String("key-shape", "be8", "Key generation shape for non-dataset 8-byte workloads (be8|be8_prefix4)")
	valSize                   = flag.Int("valsize", 128, "Value size in bytes")
	valPattern                = flag.String("val-pattern", "zero", "Value pattern for write tests (zero|repeat|repeat_tail64|ultra_compressible_repeat|highly_compressible_notail|half_repeat_half_random|medium_compressible_sparse|celestia_height_prefix_fill|random)")
	valPoolSize               = flag.Int("val-pool-size", 0, "Number of distinct values to cycle through for -val-pattern (0=auto)")
	batchSize                 = flag.Int("batchsize", 8000, "Size of batches")
	writeWorkers              = flag.Int("write-workers", 1, "Number of goroutines for *_parallel write tests (default 1)")
	readWorkers               = flag.Int("read-workers", runtime.GOMAXPROCS(0), "Number of goroutines for random_read_parallel and random_read_parallel_acquire_snapshot (default GOMAXPROCS)")
	readRequireHit            = flag.Bool("read-require-hit", false, "Fail read benchmarks on misses and validate value length matches -valsize")
	rangeQueries              = flag.Int("range-queries", 200, "number of range queries")
	rangeSpan                 = flag.Int("range-span", 100, "number of keys per range")
	batchDeleteRangeWidth     = flag.Int("batch-delete-range-width", 100, "batch_delete_range: affected keys per DeleteRange call")
	batchDeleteRangesPerBatch = flag.Int("batch-delete-ranges-per-batch", 100, "batch_delete_range: DeleteRange calls per batch commit")
	batchDeleteRangeValidate  = flag.Bool("batch-delete-range-validate", false, "batch_delete_range: validate after measured deletes that loaded keys were removed")
	batchDeleteRangeRefill    = flag.Bool("batch-delete-range-refill", false, "batch_delete_range: reload deleted keys after measured deletes/validation so later tests see the dense dataset")
	keyCountsArg              = flag.String("keycounts", "", "Comma-separated key counts to sweep over (overrides -keys)")
	keyScaleArg               = flag.String("keyscale", "", "Generate keycounts by scale: log10 or doubling (uses -keys-min/-keys-max)")
	keysMin                   = flag.Int("keys-min", 1000, "Minimum key count for -keyscale")
	keysMax                   = flag.Int("keys-max", 10000000, "Maximum key count for -keyscale")
	dbsArg                    = flag.String("dbs", "all", "Comma-separated list of DBs to run. Use 'all' for registered DBs.")
	dbsExcludeArg             = flag.String("exclude-dbs", "", "Comma-separated list of DBs to exclude")
	testArg                   = flag.String("test", "all", "Comma-separated list of tests (sequential_write,random_read,random_read_parallel,random_read_parallel_acquire_snapshot,random_read_batch,random_write,random_write_parallel,dataset_write_random,dataset_write_sorted,dataset_update_fork_choice,dataset_read_random,random_delete,full_scan,prefix_scan,batch_write,batch_write_steady,batch_random,batch_delete,batch_delete_range,update_fork_choice); aliases: write_seq->sequential_write, write_rand->random_write, write_sorted->dataset_write_sorted, write_dataset->dataset_write_random, read_rand->random_read, read_rand_parallel->random_read_parallel, read_rand_batch->random_read_batch, read_random_batch->random_read_batch, delete_rand->random_delete, scan->full_scan, range_scan->prefix_scan, batch_write_ss->batch_write_steady, batch_range_delete->batch_delete_range, delete_range->batch_delete_range, forkchoice->update_fork_choice")
	formatArg                 = flag.String("format", "table", "Output format: table or markdown")
	suiteArg                  = flag.String("suite", "", "Named benchmark suite (e.g. readme)")
	outDirArg                 = flag.String("outdir", "", "Write plots/results to this directory (used by -suite readme)")
	keepDir                   = flag.Bool("keep", false, "Keep data directories after run")
	progress                  = flag.Bool("progress", true, "Live-update the results table on stderr (cell-by-cell) while running; final table prints once to stdout")
	seed                      = flag.Int64("seed", 1, "PRNG seed for randomized tests (0 = time-based)")
	cpuProfile                = flag.String("cpuprofile", "", "write cpu profile to file")
	cpuProfileTestsArg        = flag.String("cpuprofile-tests", "", "Comma-separated list of tests to profile when -cpuprofile is set (default: all selected tests)")
	profileDir                = flag.String("profile-dir", "", "Write profiling artifacts to this directory (enables defaults for -cpuprofile, -allocsprofile, -checkpoint-cpuprofile, -blockprofile, -mutexprofile, -trace unless explicitly set)")
	pathLabel                 = flag.String("path-label", "", "Benchmark execution-path label for -profile-dir artifacts (oracle|native-fastpath|m8-m14-10mm-gate|span-native-default-gate|span-native-read-scan-guardrail; default native-fastpath)")
	allocsProfile             = flag.String("allocsprofile", "", "write per-test allocation delta profile prefix to file")
	allocsProfileTests        = flag.String("allocsprofile-tests", "", "Comma-separated list of tests to profile when -allocsprofile is set (default: all selected tests)")
	allocsProfileRate         = flag.Int("allocsprofilerate", 512*1024, "runtime.MemProfileRate sampling rate in bytes for -allocsprofile")

	blockProfile              = flag.String("blockprofile", "", "write goroutine blocking profile (pprof) to file")
	blockRate                 = flag.Int("blockprofilerate", 1, "runtime.SetBlockProfileRate sampling rate (1 = sample all)")
	mutexProfile              = flag.String("mutexprofile", "", "write mutex contention profile (pprof) to file")
	mutexFrac                 = flag.Int("mutexprofilefraction", 1, "runtime.SetMutexProfileFraction sampling fraction (1 = sample all)")
	traceProfile              = flag.String("trace", "", "write runtime execution trace to file")
	checkpointCPUProfile      = flag.String("checkpoint-cpuprofile", "", "write cpu profile for checkpoints to this path prefix")
	checkpointCPUProfileTests = flag.String("checkpoint-cpuprofile-tests", "", "comma-separated list of tests to profile for checkpoints")

	maxWall  = flag.Duration("max-wall", 0, "Abort the benchmark run if wall time exceeds this (0=disabled)")
	maxRSSMB = flag.Int("max-rss-mb", 0, "Abort the benchmark run if RSS exceeds this many MiB (0=disabled; Linux-only)")

	checkpointBetweenTests          = flag.Bool("checkpoint-between-tests", false, "Force a best-effort durability checkpoint between each benchmark test (DBs that support Checkpoint())")
	checkpointSettleBeforeTestsArg  = flag.String("checkpoint-settle-before-tests", "", "Comma-separated checkpoint-before-test labels that should wait for TreeDB queue/background debt to drain before checkpointing (use all for every checkpoint; supports post-run)")
	checkpointSettleTimeout         = flag.Duration("checkpoint-settle-timeout", defaultCheckpointSettleTimeout, "Maximum time to wait for TreeDB queue/background debt to drain before a selected checkpoint")
	vacuumBetweenTests              = flag.Bool("vacuum-between-tests", false, "Vacuum supported DBs between each benchmark test (implies -checkpoint-between-tests; TreeDB: VacuumIndexOnline)")
	checkpointEveryOps              = flag.Int("checkpoint-every-ops", 0, "Force a best-effort durability checkpoint every N ops during write-heavy tests (0=disabled; DBs that support Checkpoint())")
	checkpointEveryBytes            = flag.Int64("checkpoint-every-bytes", 0, "Force a best-effort durability checkpoint every N approx bytes during write-heavy tests (0=disabled; DBs that support Checkpoint())")
	batchWriteSteadyCheckpointBytes = flag.Int64("batch-write-steady-checkpoint-bytes", 64<<20, "batch_write_steady: default periodic checkpoint interval in bytes when checkpoint-every-* flags are unset (0 disables)")
	batchWriteDictWarmup            = flag.Bool("batch-write-dict-warmup", false, "Enable pre-measurement dict warmup writes for batch_write* (TreeDB dict mode); off by default to keep measured runs on empty DB state")

	flushdrainCheckpointMax = flag.Duration("flushdrain-checkpoint-max", 0, "Abort flushdrain suite if checkpoint-before-random_read exceeds this duration (0=disabled)")

	settleBeforeScans               = flag.Bool("settle-before-scans", false, "Close+reopen DBs before scan tests to measure settled scan performance (flushes caches/WAL)")
	treedbCacheStatsBeforeReads     = flag.Bool("treedb-cache-stats-before-reads", false, "Print select treedb.cache.* stats before read/scan tests (treedb only)")
	treedbCacheStatsAfterTests      = flag.Bool("treedb-cache-stats-after-tests", false, "Print select treedb.cache.* stats after each benchmark test (treedb only)")
	treedbVlogRewriteAfterRun       = flag.Bool("treedb-vlog-rewrite-after-run", false, "Run full TreeDB CompactStorage after the benchmark run and report before/after disk usage (treedb only; flag name kept for compatibility)")
	treedbVacuumAfterVlogRewriteRun = flag.Bool("treedb-vacuum-after-vlog-rewrite-run", true, "Run offline TreeDB index vacuum after -treedb-vlog-rewrite-after-run before reporting final compacted disk usage")
)

var explicitFlags = map[string]bool{}

func flagExplicit(name string) bool {
	return explicitFlags[name]
}

const checkpointPostRunLabel = "post-run"

const defaultCheckpointSettleTimeout = 10 * time.Minute

const (
	defaultBatchDeleteRangeWidth     = 100
	defaultBatchDeleteRangesPerBatch = 100
)

func normalizeBatchDeleteRangeDimensions(width, rangesPerBatch int) (int, int) {
	if width == 0 {
		width = defaultBatchDeleteRangeWidth
	}
	if rangesPerBatch == 0 {
		rangesPerBatch = defaultBatchDeleteRangesPerBatch
	}
	return width, rangesPerBatch
}

type DBInstance struct {
	Name                         string
	Wrapper                      kvstore.DB
	Dir                          string
	TreeDBVlogCompressionMode    treedb.ValueLogCompressionMode
	TreeDBVlogCompressionModeSet bool
}

type BenchConfig struct {
	Keys                      int
	KeyShape                  string
	ValueSize                 int
	BatchSize                 int
	WriteWorkers              int
	ReadWorkers               int
	RangeQueries              int
	RangeSpan                 int
	BatchDeleteRangeWidth     int
	BatchDeleteRangesPerBatch int
	BatchDeleteRangeValidate  bool
	BatchDeleteRangeRefill    bool
	ValuePattern              string
	ValuePoolSize             int
	// ReadRequireHit makes read benchmarks fail fast when a read misses.
	// It is intended for correctness guardrails, not throughput reporting.
	//
	// When enabled, read paths assert both:
	//   - no error (or miss) is returned
	//   - the returned value length equals ValueSize
	ReadRequireHit bool

	DBsArg        string
	DBsExcludeArg string
	TestsArg      string
	Profile       string

	KeepDir  bool
	Progress bool
	SeedUsed int64

	CPUProfile string
	// CPUProfileTests, when non-empty, restricts per-test cpu profiling to the
	// listed benchmark tests (lowercased).
	CPUProfileTests map[string]struct{}
	AllocsProfile   string
	// AllocsProfileTests, when non-empty, restricts per-test alloc profiling to
	// the listed benchmark tests (lowercased).
	AllocsProfileTests map[string]struct{}
	AllocsProfileRate  int

	CheckpointCPUProfile      string
	CheckpointCPUProfileTests map[string]struct{}

	BlockProfile         string
	BlockProfileRate     int
	MutexProfile         string
	MutexProfileFraction int
	TraceProfile         string

	MaxWall  time.Duration
	MaxRSSMB int

	CheckpointBetweenTests          bool
	CheckpointSettleBeforeTests     map[string]struct{}
	CheckpointSettleBeforeAll       bool
	CheckpointSettleTimeout         time.Duration
	VacuumBetweenTests              bool
	CheckpointEveryOps              int
	CheckpointEveryBytes            int64
	BatchWriteSteadyCheckpointBytes int64
	BatchWriteDictWarmup            bool

	SettleBeforeScans bool

	// TreeDB options (config only)
	TreeDBIterDebug      bool
	TreeDBIterDebugLimit int

	TreeDBDisableWAL                 bool
	TreeDBRelaxedSync                bool
	TreeDBDisableReadChecksum        bool
	TreeDBDisablePiggybackCompaction bool

	TreeDBCacheStatsBeforeReads bool
	TreeDBCacheStatsAfterTests  bool
	TreeDBVlogRewriteAfterRun   bool

	profileHooks *benchmarkProfileHooks
}

type dirDiskUsage struct {
	TotalBytes uint64
	TotalFiles int
}

type BenchRun struct {
	Config                    BenchConfig
	Instances                 []*DBInstance
	TestOrder                 []string
	DisplayNames              map[string]string
	Results                   map[string]map[string]float64
	CheckpointDurations       map[string]map[string]time.Duration
	CheckpointSettleDurations map[string]map[string]time.Duration
	CheckpointTreeDBStats     map[string]map[string]map[string]string
	VacuumDurations           map[string]map[string]time.Duration
	VacuumIndexBytes          map[string]map[string][2]uint64 // [0]=before, [1]=after (best-effort; treedb only)
	TreeDBDiskUsage           map[string]treeDBDiskUsage
	TreeDBVlogRewrite         map[string]treeDBVlogRewriteReport
	TreeDBPerf                map[string]map[string]treeDBPerfMetrics
	TreeDBStats               map[string]map[string]string
	DiskUsage                 map[string]dirDiskUsage
	BatchDeleteRange          map[string]map[string]batchDeleteRangeReport
	CollectionWorkloads       []benchprofCollectionWorkload
}

type treeDBPerfMetrics struct {
	Mmap                       treeDBMmapPerfMetrics     `json:"mmap,omitempty"`
	Snapshot                   treeDBSnapshotPerfMetrics `json:"snapshot,omitempty"`
	LeafGenerationsPinnedAfter int64                     `json:"leaf_generations_pinned_after,omitempty"`
	LeafPinsTotalAfter         int64                     `json:"leaf_pins_total_after,omitempty"`
}

type treeDBMmapPerfMetrics struct {
	Hits           uint64  `json:"hits,omitempty"`
	MissOutOfRange uint64  `json:"miss_out_of_range,omitempty"`
	MissNoMapping  uint64  `json:"miss_no_mapping,omitempty"`
	MissDeadMapCap uint64  `json:"miss_dead_mapping_cap,omitempty"`
	FallbackReadAt uint64  `json:"fallback_readat,omitempty"`
	HitRatio       float64 `json:"hit_ratio,omitempty"`
}

type treeDBSnapshotPerfMetrics struct {
	AcquireCalls      uint64  `json:"acquire_calls,omitempty"`
	AcquireTotalNanos uint64  `json:"acquire_total_nanos,omitempty"`
	AcquireAvgMicros  float64 `json:"acquire_avg_micros,omitempty"`
	CloseCalls        uint64  `json:"close_calls,omitempty"`
	CloseTotalNanos   uint64  `json:"close_total_nanos,omitempty"`
	CloseAvgMicros    float64 `json:"close_avg_micros,omitempty"`
}

type treeDBSelectedStats struct {
	mmapHits           uint64
	mmapMissOutOfRange uint64
	mmapMissNoMapping  uint64
	mmapMissDeadCap    uint64
	mmapFallbackReadAt uint64
	leafGenerationsPin int64
	leafPinsTotal      int64
}

type treeDBMmapReadStatDef struct {
	label  string
	suffix string
}

var treeDBMmapReadStatDefs = []treeDBMmapReadStatDef{
	{label: "vlog_mmap.read.hits", suffix: "hits"},
	{label: "vlog_mmap.read.miss_out_of_range", suffix: "miss_out_of_range"},
	{label: "vlog_mmap.read.miss_no_mapping", suffix: "miss_no_mapping"},
	{label: "vlog_mmap.read.miss_dead_mapping_cap", suffix: "miss_dead_mapping_cap"},
	{label: "vlog_mmap.read.fallback_readat", suffix: "fallback_readat"},
	{label: "vlog_mmap.read.hit_ratio", suffix: "hit_ratio"},
}

const (
	treeDBVlogMmapReadBackendPrefix = "treedb.vlog.mmap_read."
	treeDBVlogMmapReadCachePrefix   = "treedb.cache.vlog_mmap.read."
)

type benchprofExport struct {
	GeneratedAt string               `json:"generated_at"`
	Runs        []benchprofExportRun `json:"runs"`
}

type benchprofExportRun struct {
	Keys                       int                                          `json:"keys"`
	Profile                    string                                       `json:"profile,omitempty"`
	ExecutionPath              string                                       `json:"execution_path,omitempty"`
	Results                    map[string]map[string]float64                `json:"results,omitempty"`
	CheckpointDurationsSeconds map[string]map[string]float64                `json:"checkpoint_durations_seconds,omitempty"`
	CheckpointSettleSeconds    map[string]map[string]float64                `json:"checkpoint_settle_seconds,omitempty"`
	CheckpointTreeDBStats      map[string]map[string]map[string]string      `json:"checkpoint_treedb_stats,omitempty"`
	TreeDBPerf                 map[string]map[string]treeDBPerfMetrics      `json:"treedb_perf,omitempty"`
	TreeDBStats                map[string]map[string]string                 `json:"treedb_stats,omitempty"`
	BatchDeleteRange           map[string]map[string]batchDeleteRangeReport `json:"batch_delete_range,omitempty"`
	CollectionWorkloads        []benchprofCollectionWorkload                `json:"collection_workloads,omitempty"`
}

type scanDiag struct {
	dbName              string
	queueBacklogBytes   string
	queueLen            string
	flushThresholdBytes string
	maxQueuedMemtables  string
	backpressureMode    string
	flushBpsEWMA        string
}

type walDiskUsage struct {
	TotalBytes uint64
	TotalFiles int

	CommitBytes uint64
	CommitFiles int

	WALBytes uint64
	WALFiles int

	ValueBytes uint64
	ValueFiles int

	VlogBytes uint64
	VlogFiles int

	OtherBytes uint64
	OtherFiles int
}

type treeDBDiskUsage struct {
	MainIndexBytes uint64
	MainWAL        walDiskUsage
	MainValueLog   walDiskUsage
	MainLeafLog    walDiskUsage

	DictIndexBytes uint64
	DictWAL        walDiskUsage
	DictValueLog   walDiskUsage
}

type treeDBVlogRewriteReport struct {
	Dir string

	BeforeUsage dirDiskUsage
	AfterUsage  dirDiskUsage
	AfterVacuum dirDiskUsage
	VacuumRan   bool

	BeforeTree      treeDBDiskUsage
	AfterTree       treeDBDiskUsage
	AfterVacuumTree treeDBDiskUsage

	SegmentsBefore int
	SegmentsAfter  int
	BytesBefore    int64
	BytesAfter     int64
	RecordsCopied  int
}

type batchDeleteRangeReport struct {
	Mode                 string  `json:"mode"`
	LoadedKeys           int     `json:"loaded_keys"`
	RangeWidth           int     `json:"range_width"`
	RangesPerBatch       int     `json:"ranges_per_batch"`
	RangeCount           int     `json:"range_count"`
	AffectedKeys         int     `json:"affected_keys"`
	AffectedKeysPerRange float64 `json:"affected_keys_per_range"`
	ValueSize            int     `json:"value_size"`
	DeleteDurationMS     float64 `json:"delete_duration_ms"`
	RangeOpsPerSec       float64 `json:"range_ops_per_sec"`
	AffectedKeysPerSec   float64 `json:"affected_keys_per_sec"`
	Validation           string  `json:"validation"`
	Refill               bool    `json:"refill"`
}

type benchKeyShape uint8

const (
	benchKeyShapeBE8 benchKeyShape = iota
	benchKeyShapeBE8Prefix4
)

func parseBenchKeyShape(s string) (benchKeyShape, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "be8":
		return benchKeyShapeBE8, nil
	case "be8_prefix4":
		return benchKeyShapeBE8Prefix4, nil
	default:
		return 0, fmt.Errorf("unsupported -key-shape=%q (expected be8|be8_prefix4)", s)
	}
}

func resolveReadWorkers(workers int) int {
	// Keep this local to avoid coupling benchmark CLI parsing with storage adapters.
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		return 1
	}
	return workers
}

func (s benchKeyShape) encode(dst []byte, key uint64) {
	switch s {
	case benchKeyShapeBE8:
		binary.BigEndian.PutUint64(dst, key)
	case benchKeyShapeBE8Prefix4:
		// A fixed 4-byte high prefix plus a variable low u32 key space.
		dst[0], dst[1], dst[2], dst[3] = 0, 0, 0, 1
		binary.BigEndian.PutUint32(dst[4:], uint32(key))
	default:
		binary.BigEndian.PutUint64(dst, key)
	}
}

func (s benchKeyShape) validate(maxKey uint64) error {
	if s == benchKeyShapeBE8Prefix4 && maxKey > uint64(math.MaxUint32) {
		return fmt.Errorf("-key-shape=be8_prefix4 max key %d exceeds uint32 range", maxKey)
	}
	return nil
}

func (s benchKeyShape) maxKey() uint64 {
	if s == benchKeyShapeBE8Prefix4 {
		return uint64(math.MaxUint32)
	}
	return math.MaxUint64
}

func clampWarmupKeyCount(shape benchKeyShape, warmupBase uint64, warmupKeys int) int {
	if warmupKeys <= 0 {
		return 0
	}
	maxKey := shape.maxKey()
	if warmupBase > maxKey {
		return 0
	}
	remaining := maxKey - warmupBase + 1
	if uint64(warmupKeys) > remaining {
		return int(remaining)
	}
	return warmupKeys
}

func main() {
	flag.Usage = customUsage
	flag.Parse()
	if extras := flag.Args(); len(extras) > 0 {
		log.Fatalf("unexpected positional args: %q (tip: for bool flags, use -flag=false, e.g. -progress=false)", extras)
	}

	isSet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		isSet[f.Name] = true
	})
	explicitFlags = isSet
	if err := applyProfile(*profileArg, isSet); err != nil {
		log.Fatalf("profile: %v", err)
	}
	if err := applyProfileArtifactDir(*profileDir, isSet); err != nil {
		log.Fatalf("profile-dir: %v", err)
	}
	if strings.TrimSpace(*profileDir) != "" {
		executionPath, err := normalizeBenchprofExecutionPath(*pathLabel)
		if err != nil {
			log.Fatalf("profile-dir: %v", err)
		}
		*pathLabel = executionPath
	}

	seedUsed := *seed
	if seedUsed == 0 {
		seedUsed = time.Now().UnixNano()
	}
	selectedTests := normalizeTests(parseList(*testArg))
	effectiveBatchDeleteRangeWidth, effectiveBatchDeleteRangesPerBatch := normalizeBatchDeleteRangeDimensions(*batchDeleteRangeWidth, *batchDeleteRangesPerBatch)

	fmt.Fprintf(os.Stderr, "Unified Benchmark Runner\n")
	fmt.Fprintf(os.Stderr, "========================\n")
	if *profileArg != "" {
		fmt.Fprintf(os.Stderr, "Profile:     %s (%s)\n", *profileArg, profiles[strings.ToLower(*profileArg)].Description)
	} else {
		fmt.Fprintf(os.Stderr, "Profile:     (none/custom)\n")
	}
	fmt.Fprintf(os.Stderr, "Settings:    keys=%d valsize=%d batchsize=%d val_pattern=%s\n", *numKeys, *valSize, *batchSize, *valPattern)
	fmt.Fprintf(os.Stderr, "             read_workers=%d\n", resolveReadWorkers(*readWorkers))
	fmt.Fprintf(os.Stderr, "             key_shape=%s\n", strings.TrimSpace(*keyShapeArg))
	if *rangeQueries > 0 {
		fmt.Fprintf(os.Stderr, "             range_queries=%d range_span=%d\n", *rangeQueries, *rangeSpan)
	}
	if contains(selectedTests, "batch_delete_range") {
		fmt.Fprintf(os.Stderr, "             batch_delete_range_width=%d batch_delete_ranges_per_batch=%d validate=%t refill=%t\n",
			effectiveBatchDeleteRangeWidth, effectiveBatchDeleteRangesPerBatch, *batchDeleteRangeValidate, *batchDeleteRangeRefill)
	}
	fmt.Fprintf(os.Stderr, "DBs:         %s\n", *dbsArg)
	fmt.Fprintf(os.Stderr, "Tests:       %s\n", *testArg)
	selectedDBs := resolveDBs(*dbsArg, *dbsExcludeArg)
	runsTreeDB := false
	for _, name := range selectedDBs {
		if strings.HasPrefix(strings.ToLower(name), "treedb") {
			runsTreeDB = true
			break
		}
	}
	hasAllTests := contains(selectedTests, "all")
	runsBatchWrite := hasAllTests || contains(selectedTests, "batch_write")
	runsBatchWriteSteady := hasAllTests || contains(selectedTests, "batch_write_steady")
	if runsTreeDB && runsBatchWrite && !runsBatchWriteSteady {
		fmt.Fprintf(os.Stderr, "Note:        TreeDB batch_write reports front-end ingest only; use batch_write_steady for settled backend throughput.\n")
	}
	fmt.Fprintf(os.Stderr, "Seed:        %d\n", seedUsed)
	fmt.Fprintf(os.Stderr, "\n")

	logResolvedTreeDBOptions()

	// Populate TreeDB specific config into BenchConfig by reading the flags defined in adapter_treedb.go
	effectiveReadWorkers := resolveReadWorkers(*readWorkers)
	checkpointSettleBeforeTests, checkpointSettleBeforeAll := parseCheckpointSettleBeforeTests(*checkpointSettleBeforeTestsArg)
	baseCfg := BenchConfig{
		Keys:                             *numKeys,
		KeyShape:                         *keyShapeArg,
		ValueSize:                        *valSize,
		BatchSize:                        *batchSize,
		WriteWorkers:                     *writeWorkers,
		ReadWorkers:                      effectiveReadWorkers,
		RangeQueries:                     *rangeQueries,
		RangeSpan:                        *rangeSpan,
		BatchDeleteRangeWidth:            effectiveBatchDeleteRangeWidth,
		BatchDeleteRangesPerBatch:        effectiveBatchDeleteRangesPerBatch,
		BatchDeleteRangeValidate:         *batchDeleteRangeValidate,
		BatchDeleteRangeRefill:           *batchDeleteRangeRefill,
		ValuePattern:                     *valPattern,
		ValuePoolSize:                    *valPoolSize,
		ReadRequireHit:                   *readRequireHit,
		DBsArg:                           *dbsArg,
		DBsExcludeArg:                    *dbsExcludeArg,
		TestsArg:                         *testArg,
		Profile:                          *profileArg,
		KeepDir:                          *keepDir,
		Progress:                         *progress,
		SeedUsed:                         seedUsed,
		CPUProfile:                       *cpuProfile,
		AllocsProfile:                    *allocsProfile,
		AllocsProfileRate:                *allocsProfileRate,
		BlockProfile:                     *blockProfile,
		BlockProfileRate:                 *blockRate,
		MutexProfile:                     *mutexProfile,
		MutexProfileFraction:             *mutexFrac,
		TraceProfile:                     *traceProfile,
		CheckpointCPUProfile:             *checkpointCPUProfile,
		MaxWall:                          *maxWall,
		MaxRSSMB:                         *maxRSSMB,
		CheckpointBetweenTests:           *checkpointBetweenTests || *vacuumBetweenTests,
		CheckpointSettleBeforeTests:      checkpointSettleBeforeTests,
		CheckpointSettleBeforeAll:        checkpointSettleBeforeAll,
		CheckpointSettleTimeout:          *checkpointSettleTimeout,
		VacuumBetweenTests:               *vacuumBetweenTests,
		CheckpointEveryOps:               *checkpointEveryOps,
		CheckpointEveryBytes:             *checkpointEveryBytes,
		BatchWriteSteadyCheckpointBytes:  *batchWriteSteadyCheckpointBytes,
		BatchWriteDictWarmup:             *batchWriteDictWarmup,
		SettleBeforeScans:                *settleBeforeScans,
		TreeDBCacheStatsBeforeReads:      *treedbCacheStatsBeforeReads,
		TreeDBCacheStatsAfterTests:       *treedbCacheStatsAfterTests,
		TreeDBVlogRewriteAfterRun:        *treedbVlogRewriteAfterRun,
		TreeDBIterDebug:                  *treedbIterDebug,
		TreeDBIterDebugLimit:             *treedbIterDebugLimit,
		TreeDBDisableWAL:                 *treedbDisableWAL,
		TreeDBRelaxedSync:                *treedbRelaxedSync,
		TreeDBDisableReadChecksum:        *treedbDisableReadChecksum,
		TreeDBDisablePiggybackCompaction: *treedbDisablePiggyback,
	}
	if strings.TrimSpace(baseCfg.CPUProfile) != "" {
		tests := parseList(*cpuProfileTestsArg)
		if len(tests) > 0 && tests[0] != "" {
			baseCfg.CPUProfileTests = make(map[string]struct{}, len(tests))
			for _, t := range tests {
				if t == "" {
					continue
				}
				baseCfg.CPUProfileTests[t] = struct{}{}
			}
		}
	}
	if strings.TrimSpace(baseCfg.AllocsProfile) != "" {
		tests := parseList(*allocsProfileTests)
		if len(tests) > 0 && tests[0] != "" {
			baseCfg.AllocsProfileTests = make(map[string]struct{}, len(tests))
			for _, t := range tests {
				if t == "" {
					continue
				}
				baseCfg.AllocsProfileTests[t] = struct{}{}
			}
		}
	}
	if strings.TrimSpace(baseCfg.CheckpointCPUProfile) != "" {
		tests := parseList(*checkpointCPUProfileTests)
		if len(tests) > 0 && tests[0] != "" {
			baseCfg.CheckpointCPUProfileTests = make(map[string]struct{}, len(tests))
			for _, t := range tests {
				if t == "" {
					continue
				}
				baseCfg.CheckpointCPUProfileTests[t] = struct{}{}
			}
		}
	}

	suite := strings.ToLower(strings.TrimSpace(*suiteArg))
	if suite != "" {
		switch suite {
		case "geth_hot_kv", "geth-hot-kv":
			out, err := runGethHotKVSuite(baseCfg)
			if err != nil {
				log.Fatalf("geth_hot_kv suite: %v", err)
			}
			fmt.Print(out)
		case "readme":
			out, err := runReadmeSuite(baseCfg)
			if err != nil {
				log.Fatalf("readme suite: %v", err)
			}
			fmt.Print(out)
		case "churn":
			out, err := runChurnSuite(baseCfg)
			if err != nil {
				log.Fatalf("churn suite: %v", err)
			}
			fmt.Print(out)
		case "churnvacuum":
			out, err := runChurnVacuumSuite(baseCfg)
			if err != nil {
				log.Fatalf("churnvacuum suite: %v", err)
			}
			fmt.Print(out)
		case "flushthrash":
			out, err := runFlushThrashSuite(baseCfg)
			if err != nil {
				log.Fatalf("flushthrash suite: %v", err)
			}
			fmt.Print(out)
		case "flushdrain", "flush-drain":
			out, err := runFlushDrainSuite(baseCfg)
			if err != nil {
				log.Fatalf("flushdrain suite: %v", err)
			}
			fmt.Print(out)
		case "bigkeys_guard":
			out, err := runBigKeysGuardSuite(baseCfg)
			if err != nil {
				log.Fatalf("bigkeys_guard suite: %v", err)
			}
			fmt.Print(out)
		case "longmix":
			out, err := runLongMixSuite(baseCfg)
			if err != nil {
				log.Fatalf("longmix suite: %v", err)
			}
			fmt.Print(out)
		case "sload_readheavy", "sload-readheavy":
			out, err := runSloadReadHeavySuite(baseCfg)
			if err != nil {
				log.Fatalf("sload_readheavy suite: %v", err)
			}
			fmt.Print(out)
		case "lanes_probe", "lanes-probe":
			out, err := runLaneProbeSuite(baseCfg)
			if err != nil {
				log.Fatalf("lanes_probe suite: %v", err)
			}
			fmt.Print(out)
		case "vlog_queue_lag", "vlog-queue-lag":
			out, err := runVlogQueueLagSuite(baseCfg)
			if err != nil {
				log.Fatalf("vlog_queue_lag suite: %v", err)
			}
			fmt.Print(out)
		case "backend_saturation", "backend-saturation":
			out, err := runBackendSaturationSuite(baseCfg)
			if err != nil {
				log.Fatalf("backend_saturation suite: %v", err)
			}
			fmt.Print(out)
		case "backend_materialization_debt", "backend-materialization-debt":
			out, err := runBackendMaterializationDebtSuite(baseCfg)
			if err != nil {
				log.Fatalf("backend_materialization_debt suite: %v", err)
			}
			fmt.Print(out)
		case "maintenance_gc", "maintenance-gc":
			out, err := runMaintenanceGCSuite(baseCfg)
			if err != nil {
				log.Fatalf("maintenance_gc suite: %v", err)
			}
			fmt.Print(out)
		case "maintenance_coordination", "maintenance-coordination":
			out, err := runMaintenanceCoordinationSuite(baseCfg)
			if err != nil {
				log.Fatalf("maintenance_coordination suite: %v", err)
			}
			fmt.Print(out)
		case "backend_skew_fairness", "backend-skew-fairness":
			out, err := runBackendSkewFairnessSuite(baseCfg)
			if err != nil {
				log.Fatalf("backend_skew_fairness suite: %v", err)
			}
			fmt.Print(out)
		case "backend_sync_matrix", "backend-sync-matrix":
			out, err := runBackendSyncMatrixSuite(baseCfg)
			if err != nil {
				log.Fatalf("backend_sync_matrix suite: %v", err)
			}
			fmt.Print(out)
		case "publish_watermark", "publish-watermark":
			out, err := runPublishWatermarkSuite(baseCfg)
			if err != nil {
				log.Fatalf("publish_watermark suite: %v", err)
			}
			fmt.Print(out)
		case "hotspot_rebalance", "hotspot-rebalance":
			out, err := runHotspotRebalanceSuite(baseCfg)
			if err != nil {
				log.Fatalf("hotspot_rebalance suite: %v", err)
			}
			fmt.Print(out)
		case "fence_lag", "fence-lag":
			out, err := runFenceLagSuite(baseCfg)
			if err != nil {
				log.Fatalf("fence_lag suite: %v", err)
			}
			fmt.Print(out)
		case "storage_ceiling", "storage-ceiling":
			out, err := runStorageCeilingSuite(baseCfg)
			if err != nil {
				log.Fatalf("storage_ceiling suite: %v", err)
			}
			fmt.Print(out)
		case "vlog_dict", "vlog-dict":
			out, err := runValueLogDictSuite(baseCfg)
			if err != nil {
				log.Fatalf("vlog_dict suite: %v", err)
			}
			fmt.Print(out)
		case "maintenance_budget", "maintenance-budget":
			out, err := runMaintenanceBudgetSuite(baseCfg)
			if err != nil {
				log.Fatalf("maintenance_budget suite: %v", err)
			}
			fmt.Print(out)
		case "column_store", "column-store":
			out, err := runColumnStoreSuite(baseCfg, columnStoreSuiteOptions{
				ProfileDir:    strings.TrimSpace(*profileDir),
				ExecutionPath: strings.TrimSpace(*pathLabel),
				ForcedPath:    strings.TrimSpace(*columnStoreSuitePathArg),
				Fixture:       strings.TrimSpace(*columnStoreSuiteFixtureArg),
				RunBenchprof:  true,
			})
			if err != nil {
				log.Fatalf("column_store suite: %v", err)
			}
			fmt.Print(out)
		case "collection_storage", "collection-storage":
			out, err := runCollectionStorageSuite(baseCfg, collectionStorageSuiteOptions{
				ProfileDir:               strings.TrimSpace(*profileDir),
				ExecutionPath:            strings.TrimSpace(*pathLabel),
				ModesArg:                 strings.TrimSpace(*collectionStorageModesArg),
				WorkloadsArg:             strings.TrimSpace(*collectionStorageWorkloadsArg),
				QueryCount:               *collectionStorageQueryCountArg,
				PointGetCount:            *collectionStoragePointGetCountArg,
				FieldCount:               *collectionStorageFieldCountArg,
				PayloadSize:              *collectionStoragePayloadSizeArg,
				Cardinality:              *collectionStorageCardinalityArg,
				Selectivity:              *collectionStorageSelectivityArg,
				VectorDims:               *collectionStorageVectorDimsArg,
				VectorTopK:               *collectionStorageVectorTopKArg,
				IncludeFinalFetch:        *collectionStorageIncludeFinalFetchArg,
				VectorFullDocuments:      *collectionStorageVectorFullDocumentsArg,
				CheckpointReopen:         *collectionStorageCheckpointReopenArg,
				ColumnAssetReadIntegrity: collections.ColumnAssetReadIntegrity(strings.TrimSpace(*collectionStorageAssetReadIntegrityArg)),
				RunBenchprof:             true,
			})
			if err != nil {
				log.Fatalf("collection_storage suite: %v", err)
			}
			fmt.Print(out)
		default:
			log.Fatalf("unknown suite: %q", suite)
		}
		return
	}

	format := strings.ToLower(strings.TrimSpace(*formatArg))
	keyCounts, err := resolveKeyCounts(*numKeys, *keyCountsArg, *keyScaleArg, *keysMin, *keysMax)
	if err != nil {
		log.Fatalf("keycounts: %v", err)
	}

	if benchConfigHasAnyProfileOutput(baseCfg) && len(keyCounts) > 1 {
		log.Fatalf("profiling flags require a single key count (got %d): disable sweep keycounts or omit -cpuprofile/-allocsprofile/-checkpoint-cpuprofile/-blockprofile/-mutexprofile/-trace", len(keyCounts))
	}

	if len(keyCounts) == 1 {
		cfg := baseCfg
		cfg.Keys = keyCounts[0]
		if format == "markdown" {
			cfg.Progress = false
			run, err := runBenchmark(cfg)
			if err != nil {
				log.Fatalf("benchmark: %v", err)
			}
			hasArtifacts := maybeWriteBenchprofArtifacts(*profileDir, []BenchRun{run})
			fmt.Print(renderMarkdownSingle(run))
			if hasArtifacts {
				runBenchprof(*profileDir)
			}
			return
		}

		run, err := runBenchmark(cfg)
		if err != nil {
			log.Fatalf("benchmark: %v", err)
		}
		hasArtifacts := maybeWriteBenchprofArtifacts(*profileDir, []BenchRun{run})
		printResultsTable(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
		if details := strings.TrimSpace(renderBatchDeleteRangeReportsString(run.Instances, run.BatchDeleteRange)); details != "" {
			fmt.Println()
			fmt.Println("Batch DeleteRange Metrics")
			fmt.Println(details)
		}
		if len(run.CheckpointDurations) > 0 {
			fmt.Println()
			printCheckpointDurationsTable(run.Instances, run.TestOrder, run.DisplayNames, run.CheckpointDurations)
		}
		if len(run.VacuumDurations) > 0 {
			fmt.Println()
			printVacuumDurationsTable(run.Instances, run.TestOrder, run.DisplayNames, run.VacuumDurations)
			if len(run.VacuumIndexBytes) > 0 {
				fmt.Println()
				printVacuumIndexBytesTable(run.Instances, run.TestOrder, run.DisplayNames, run.VacuumIndexBytes)
			}
		}
		if len(run.TreeDBDiskUsage) > 0 || len(run.DiskUsage) > 0 {
			fmt.Println()
			fmt.Println("Disk Usage (End of Run)")
			if len(run.TreeDBDiskUsage) > 0 {
				fmt.Print(renderTreeDBDiskUsageString(run.TreeDBDiskUsage))
			}
			if other := renderNonTreeDBDiskUsageString(run.DiskUsage, run.TreeDBDiskUsage); strings.TrimSpace(other) != "" {
				fmt.Println()
				fmt.Println("Other DBs:")
				for _, line := range strings.Split(strings.TrimSpace(other), "\n") {
					if strings.TrimSpace(line) == "" {
						continue
					}
					fmt.Printf("  %s\n", line)
				}
			}
		}
		if len(run.TreeDBVlogRewrite) > 0 {
			fmt.Println()
			fmt.Println("TreeDB CompactStorage (After Run)")
			fmt.Print(renderTreeDBVlogRewriteString(run.TreeDBVlogRewrite))
		}
		if run.Config.KeepDir {
			if kept := strings.TrimSpace(renderKeptDirsString(run.Instances)); kept != "" {
				fmt.Println()
				fmt.Println("Kept Data Directories")
				fmt.Print(kept)
			}
		}
		if hasArtifacts {
			runBenchprof(*profileDir)
		}
		return
	}

	cfg := baseCfg
	cfg.Progress = false
	runs, err := runSweep(cfg, keyCounts)
	if err != nil {
		log.Fatalf("benchmark sweep: %v", err)
	}
	hasArtifacts := maybeWriteBenchprofArtifacts(*profileDir, runs)

	switch format {
	case "table":
		for _, run := range runs {
			fmt.Printf("\nkeys=%s valsize=%d batchsize=%d range-queries=%d range-span=%d\n\n",
				formatInt(run.Config.Keys), run.Config.ValueSize, run.Config.BatchSize, run.Config.RangeQueries, run.Config.RangeSpan)
			printResultsTable(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
			if details := strings.TrimSpace(renderBatchDeleteRangeReportsString(run.Instances, run.BatchDeleteRange)); details != "" {
				fmt.Println()
				fmt.Println("Batch DeleteRange Metrics")
				fmt.Println(details)
			}
			if len(run.CheckpointDurations) > 0 {
				fmt.Println()
				printCheckpointDurationsTable(run.Instances, run.TestOrder, run.DisplayNames, run.CheckpointDurations)
			}
			if len(run.VacuumDurations) > 0 {
				fmt.Println()
				printVacuumDurationsTable(run.Instances, run.TestOrder, run.DisplayNames, run.VacuumDurations)
				if len(run.VacuumIndexBytes) > 0 {
					fmt.Println()
					printVacuumIndexBytesTable(run.Instances, run.TestOrder, run.DisplayNames, run.VacuumIndexBytes)
				}
			}
			if len(run.TreeDBDiskUsage) > 0 || len(run.DiskUsage) > 0 {
				fmt.Println()
				fmt.Println("Disk Usage (End of Run)")
				if len(run.TreeDBDiskUsage) > 0 {
					fmt.Print(renderTreeDBDiskUsageString(run.TreeDBDiskUsage))
				}
				if other := renderNonTreeDBDiskUsageString(run.DiskUsage, run.TreeDBDiskUsage); strings.TrimSpace(other) != "" {
					fmt.Println()
					fmt.Println("Other DBs:")
					for _, line := range strings.Split(strings.TrimSpace(other), "\n") {
						if strings.TrimSpace(line) == "" {
							continue
						}
						fmt.Printf("  %s\n", line)
					}
				}
			}
			if len(run.TreeDBVlogRewrite) > 0 {
				fmt.Println()
				fmt.Println("TreeDB CompactStorage (After Run)")
				fmt.Print(renderTreeDBVlogRewriteString(run.TreeDBVlogRewrite))
			}
			if run.Config.KeepDir {
				if kept := strings.TrimSpace(renderKeptDirsString(run.Instances)); kept != "" {
					fmt.Println()
					fmt.Println("Kept Data Directories")
					fmt.Print(kept)
				}
			}
		}
	case "markdown":
		fmt.Print(renderMarkdownSweep(runs))
	default:
		log.Fatalf("unknown -format: %q", format)
	}
	if hasArtifacts {
		runBenchprof(*profileDir)
	}
}

func shouldCPUProfile(cfg BenchConfig, testName string) bool {
	if strings.TrimSpace(cfg.CPUProfile) == "" {
		return false
	}
	if len(cfg.CPUProfileTests) == 0 {
		return true
	}
	_, ok := cfg.CPUProfileTests[strings.ToLower(testName)]
	return ok
}

func shouldAllocsProfile(cfg BenchConfig, testName string) bool {
	if strings.TrimSpace(cfg.AllocsProfile) == "" {
		return false
	}
	if len(cfg.AllocsProfileTests) == 0 {
		return true
	}
	_, ok := cfg.AllocsProfileTests[strings.ToLower(testName)]
	return ok
}

func benchConfigHasAnyProfileOutput(cfg BenchConfig) bool {
	return strings.TrimSpace(cfg.CPUProfile) != "" ||
		strings.TrimSpace(cfg.AllocsProfile) != "" ||
		strings.TrimSpace(cfg.CheckpointCPUProfile) != "" ||
		strings.TrimSpace(cfg.BlockProfile) != "" ||
		strings.TrimSpace(cfg.MutexProfile) != "" ||
		strings.TrimSpace(cfg.TraceProfile) != ""
}

func benchConfigUsesAllocsProfile(cfg BenchConfig) bool {
	if strings.TrimSpace(cfg.AllocsProfile) == "" {
		return false
	}
	if len(cfg.AllocsProfileTests) == 0 {
		return true
	}
	tests := normalizeTests(parseList(cfg.TestsArg))
	if len(tests) == 0 {
		return true
	}
	if contains(tests, "all") {
		return true
	}
	for _, testName := range tests {
		if _, ok := cfg.AllocsProfileTests[strings.ToLower(testName)]; ok {
			return true
		}
	}
	return false
}

func installAllocsProfileRateForEnabled(enabled bool, rate int) func() {
	if !enabled {
		return func() {}
	}
	if rate <= 0 {
		rate = 512 * 1024
	}
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = rate
	return func() {
		runtime.MemProfileRate = prevRate
	}
}

func installAllocsProfileRate(cfg BenchConfig) func() {
	return installAllocsProfileRateForEnabled(benchConfigUsesAllocsProfile(cfg), cfg.AllocsProfileRate)
}

func shouldCheckpointCPUProfile(cfg BenchConfig, testName string) bool {
	if strings.TrimSpace(cfg.CheckpointCPUProfile) == "" {
		return false
	}
	if len(cfg.CheckpointCPUProfileTests) == 0 {
		return true
	}
	_, ok := cfg.CheckpointCPUProfileTests[strings.ToLower(testName)]
	return ok
}

func startCheckpointCPUProfile(cfg BenchConfig, profileHooks benchmarkProfileHooks, testName, dbName string) (*os.File, error) {
	path := fmt.Sprintf("%s_checkpoint_%s_%s.pprof", strings.TrimSpace(cfg.CheckpointCPUProfile), sanitizeProfileSegment(testName), sanitizeProfileSegment(dbName))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint cpu profile (%s/%s): %w", testName, dbName, err)
	}
	if profileHooks.startCPUProfile == nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("checkpoint cpu profile (%s/%s) start hook is nil: path=%s", testName, dbName, path)
	}
	if err := profileHooks.startCPUProfile(f); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("checkpoint cpu profile (%s/%s) start path=%s: %w", testName, dbName, path, err)
	}
	return f, nil
}

type benchmarkProfileHooks struct {
	startCPUProfile                 func(io.Writer) error
	stopCPUProfile                  func()
	writeAllocsSnapshotTemp         func(string) (string, error)
	writeAllocsDeltaProfile         func(string, string, string) error
	writeRuntimeProfileSnapshotTemp func(string, string) (string, error)
	writeRuntimeProfileDeltaProfile func(string, string, string) (bool, error)
}

func defaultBenchmarkProfileHooks() benchmarkProfileHooks {
	return benchmarkProfileHooks{
		startCPUProfile:                 pprof.StartCPUProfile,
		stopCPUProfile:                  pprof.StopCPUProfile,
		writeAllocsSnapshotTemp:         writeAllocsSnapshotTemp,
		writeAllocsDeltaProfile:         writeAllocsDeltaProfile,
		writeRuntimeProfileSnapshotTemp: writeRuntimeProfileSnapshotTemp,
		writeRuntimeProfileDeltaProfile: writeRuntimeProfileDeltaProfile,
	}
}

func profileHooksFromConfig(cfg BenchConfig) benchmarkProfileHooks {
	hooks := defaultBenchmarkProfileHooks()
	if cfg.profileHooks == nil {
		return hooks
	}
	override := *cfg.profileHooks
	if override.startCPUProfile != nil {
		hooks.startCPUProfile = override.startCPUProfile
	}
	if override.stopCPUProfile != nil {
		hooks.stopCPUProfile = override.stopCPUProfile
	}
	if override.writeAllocsSnapshotTemp != nil {
		hooks.writeAllocsSnapshotTemp = override.writeAllocsSnapshotTemp
	}
	if override.writeAllocsDeltaProfile != nil {
		hooks.writeAllocsDeltaProfile = override.writeAllocsDeltaProfile
	}
	if override.writeRuntimeProfileSnapshotTemp != nil {
		hooks.writeRuntimeProfileSnapshotTemp = override.writeRuntimeProfileSnapshotTemp
	}
	if override.writeRuntimeProfileDeltaProfile != nil {
		hooks.writeRuntimeProfileDeltaProfile = override.writeRuntimeProfileDeltaProfile
	}
	return hooks
}

var errEmptyPprofDeltaOutput = errors.New("empty pprof delta output")

func writeAllocsSnapshot(path string) error {
	// MemProfile data can lag behind current allocations until GC updates the
	// profile tables. Run two collections to reduce staleness before snapshotting.
	runtime.GC()
	runtime.GC()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	prof := pprof.Lookup("allocs")
	if prof == nil {
		return fmt.Errorf("allocs profile unavailable")
	}
	if err := prof.WriteTo(f, 0); err != nil {
		return err
	}
	return nil
}

func writeAllocsSnapshotTemp(prefix string) (string, error) {
	f, err := os.CreateTemp("", prefix+"_*.pprof")
	if err != nil {
		return "", err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if err := writeAllocsSnapshot(path); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func writeAllocsDeltaProfile(basePath, afterPath, outPath string) error {
	return writePprofDeltaProfile(basePath, afterPath, outPath)
}

func writeRuntimeProfileSnapshot(path, profileName string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	prof := pprof.Lookup(profileName)
	if prof == nil {
		return fmt.Errorf("%s profile unavailable", profileName)
	}
	if err := prof.WriteTo(f, 0); err != nil {
		return err
	}
	return nil
}

func writeRuntimeProfileSnapshotTemp(prefix, profileName string) (string, error) {
	f, err := os.CreateTemp("", prefix+"_*.pprof")
	if err != nil {
		return "", err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if err := writeRuntimeProfileSnapshot(path, profileName); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func writeRuntimeProfileDeltaProfile(basePath, afterPath, outPath string) (bool, error) {
	return writeRuntimeProfileDeltaProfileWithRunner(basePath, afterPath, outPath, runPprofDeltaCommand)
}

func writeRuntimeProfileDeltaProfileWithRunner(basePath, afterPath, outPath string, runner func(string, string) ([]byte, string, error)) (bool, error) {
	err := writePprofDeltaProfileWithRunner(basePath, afterPath, outPath, runner)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, errEmptyPprofDeltaOutput) {
		_ = os.Remove(outPath)
		return false, nil
	}
	return false, err
}

func writePprofDeltaProfile(basePath, afterPath, outPath string) error {
	return writePprofDeltaProfileWithRunner(basePath, afterPath, outPath, runPprofDeltaCommand)
}

func writePprofDeltaProfileWithRunner(basePath, afterPath, outPath string, runner func(string, string) ([]byte, string, error)) error {
	stdout, stderrText, err := runner(basePath, afterPath)
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(stderrText))
	}
	if len(stdout) == 0 {
		return errEmptyPprofDeltaOutput
	}
	if err := os.WriteFile(outPath, stdout, 0o644); err != nil {
		return err
	}
	return nil
}

func runPprofDeltaCommand(basePath, afterPath string) ([]byte, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(goToolExecutable(), "tool", "pprof", "-proto", "-base", basePath, afterPath)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, stderr.String(), err
	}
	return stdout.Bytes(), stderr.String(), nil
}

func goToolExecutable() string {
	if path, err := exec.LookPath("go"); err == nil {
		if filepath.IsAbs(path) {
			return path
		}
	}
	name := "go"
	if runtime.GOOS == "windows" {
		name = "go.exe"
	}
	if goroot := runtime.GOROOT(); goroot != "" {
		candidate := filepath.Join(goroot, "bin", name)
		if info, err := os.Stat(candidate); err == nil && goToolCandidateExecutable(info) {
			return candidate
		}
	}
	return "go"
}

func goToolCandidateExecutable(info os.FileInfo) bool {
	if info == nil || info.IsDir() {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	return info.Mode().Perm()&0o111 != 0
}

func contentionProfilePath(globalPath, kind, testName, dbName string) string {
	baseDir := filepath.Dir(strings.TrimSpace(globalPath))
	return filepath.Join(baseDir, fmt.Sprintf("%s_%s_%s.pprof",
		sanitizeProfileSegment(kind),
		sanitizeProfileSegment(testName),
		sanitizeProfileSegment(dbName),
	))
}

func sanitizeProfileSegment(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			return r
		case r == '_' || r == '-' || r == '.':
			return '_'
		default:
			return '_'
		}
	}, s)
}

func applyProfileArtifactDir(dir string, isSet map[string]bool) error {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create profile dir %q: %w", dir, err)
	}
	setStringIfUnset("cpuprofile", filepath.Join(dir, "cpu"), isSet, cpuProfile)
	setStringIfUnset("allocsprofile", filepath.Join(dir, "allocs"), isSet, allocsProfile)
	setStringIfUnset("checkpoint-cpuprofile", filepath.Join(dir, "checkpoint_cpu"), isSet, checkpointCPUProfile)
	setStringIfUnset("blockprofile", filepath.Join(dir, "block.pprof"), isSet, blockProfile)
	setStringIfUnset("mutexprofile", filepath.Join(dir, "mutex.pprof"), isSet, mutexProfile)
	setStringIfUnset("trace", filepath.Join(dir, "trace.out"), isSet, traceProfile)
	return nil
}

func maybeWriteBenchprofArtifacts(dir string, runs []BenchRun) bool {
	dir = strings.TrimSpace(dir)
	if dir == "" || len(runs) == 0 {
		return false
	}
	executionPath, err := normalizeBenchprofExecutionPath(*pathLabel)
	if err != nil {
		log.Printf("benchprof artifacts: %v", err)
		return false
	}
	if err := writeBenchprofArtifacts(dir, executionPath, runs); err != nil {
		log.Printf("benchprof artifacts: %v", err)
		return false
	}
	return true
}

func runBenchprof(dir string) {
	if err := benchprof.RunFromProfilesDir(dir); err != nil {
		log.Printf("benchprof: %v", err)
	}
}

func runBenchprofStrict(dir string) error {
	if err := benchprof.RunFromProfilesDir(dir); err != nil {
		return fmt.Errorf("benchprof: %w", err)
	}
	for _, name := range []string{"insights.md", "insights.json", "insights.html"} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("benchprof: expected %s: %w", path, err)
		}
	}
	return nil
}

func writeBenchprofArtifacts(dir, executionPath string, runs []BenchRun) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}
	return writeBenchprofArtifactsToPaths(filepath.Join(dir, "benchprof_results.json"), filepath.Join(dir, "benchprof_results.md"), executionPath, runs)
}

func benchprofJSONResults(results map[string]map[string]float64) map[string]map[string]float64 {
	if len(results) == 0 {
		return nil
	}
	out := make(map[string]map[string]float64, len(results))
	for testName, perDB := range results {
		if len(perDB) == 0 {
			continue
		}
		clean := make(map[string]float64, len(perDB))
		for dbName, value := range perDB {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				continue
			}
			clean[dbName] = value
		}
		out[testName] = clean
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func benchprofJSONDurationSeconds(durations map[string]map[string]time.Duration) map[string]map[string]float64 {
	if len(durations) == 0 {
		return nil
	}
	out := make(map[string]map[string]float64, len(durations))
	for label, perDB := range durations {
		if len(perDB) == 0 {
			continue
		}
		clean := make(map[string]float64, len(perDB))
		for dbName, d := range perDB {
			if d < 0 {
				continue
			}
			clean[dbName] = d.Seconds()
		}
		if len(clean) > 0 {
			out[label] = clean
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func writeBenchprofArtifactsToPaths(jsonPath, markdownPath, executionPath string, runs []BenchRun) error {
	jsonPath = strings.TrimSpace(jsonPath)
	markdownPath = strings.TrimSpace(markdownPath)
	if jsonPath == "" || markdownPath == "" {
		return errors.New("benchprof: json and markdown artifact paths are required")
	}
	if err := os.MkdirAll(filepath.Dir(jsonPath), 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", filepath.Dir(jsonPath), err)
	}
	if err := os.MkdirAll(filepath.Dir(markdownPath), 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", filepath.Dir(markdownPath), err)
	}
	normalizedExecutionPath, err := normalizeBenchprofExecutionPath(executionPath)
	if err != nil {
		return err
	}
	executionPath = normalizedExecutionPath

	out := benchprofExport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Runs:        make([]benchprofExportRun, 0, len(runs)),
	}
	for _, run := range runs {
		out.Runs = append(out.Runs, benchprofExportRun{
			Keys:                       run.Config.Keys,
			Profile:                    strings.TrimSpace(run.Config.Profile),
			ExecutionPath:              executionPath,
			Results:                    benchprofJSONResults(run.Results),
			CheckpointDurationsSeconds: benchprofJSONDurationSeconds(run.CheckpointDurations),
			CheckpointSettleSeconds:    benchprofJSONDurationSeconds(run.CheckpointSettleDurations),
			CheckpointTreeDBStats:      selectedBenchprofCheckpointTreeDBStats(run.CheckpointTreeDBStats),
			TreeDBPerf:                 run.TreeDBPerf,
			TreeDBStats:                selectedBenchprofTreeDBStats(run.TreeDBStats),
			BatchDeleteRange:           run.BatchDeleteRange,
			CollectionWorkloads:        run.CollectionWorkloads,
		})
	}

	js, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal benchprof_results.json: %w", err)
	}
	if err := os.WriteFile(jsonPath, js, 0o644); err != nil {
		return fmt.Errorf("write benchprof_results.json: %w", err)
	}

	var md string
	if len(runs) == 1 {
		md = renderMarkdownSingle(runs[0])
	} else {
		md = renderMarkdownSweep(runs)
	}
	if executionPath != "" {
		md = fmt.Sprintf("- execution path: `%s`\n\n%s", executionPath, md)
	}
	if err := os.WriteFile(markdownPath, []byte(md), 0o644); err != nil {
		return fmt.Errorf("write benchprof_results.md: %w", err)
	}
	return nil
}

func selectedBenchprofTreeDBStats(stats map[string]map[string]string) map[string]map[string]string {
	if len(stats) == 0 {
		return nil
	}
	out := make(map[string]map[string]string)
	for dbName, dbStats := range stats {
		selected := treedbstats.Selected(dbStats)
		if len(selected) == 0 {
			continue
		}
		out[dbName] = selected
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func selectedBenchprofCheckpointTreeDBStats(stats map[string]map[string]map[string]string) map[string]map[string]map[string]string {
	if len(stats) == 0 {
		return nil
	}
	out := make(map[string]map[string]map[string]string)
	for label, perDB := range stats {
		selectedPerDB := selectedBenchprofTreeDBStats(perDB)
		if len(selectedPerDB) == 0 {
			continue
		}
		out[label] = selectedPerDB
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func recordDuration(dst map[string]map[string]time.Duration, label, dbName string, d time.Duration) {
	if dst == nil || label == "" || dbName == "" {
		return
	}
	perDB := dst[label]
	if perDB == nil {
		perDB = make(map[string]time.Duration)
		dst[label] = perDB
	}
	perDB[dbName] = d
}

func recordCheckpointTreeDBStats(dst map[string]map[string]map[string]string, label string, db kvstore.DB) {
	if dst == nil || label == "" || db == nil {
		return
	}
	sp, ok := db.(kvstore.StatsProvider)
	if !ok {
		return
	}
	selected := treedbstats.Selected(sp.Stats())
	if len(selected) == 0 {
		return
	}
	copySnap := make(map[string]string, len(selected))
	for k, v := range selected {
		copySnap[k] = v
	}
	perDB := dst[label]
	if perDB == nil {
		perDB = make(map[string]map[string]string)
		dst[label] = perDB
	}
	perDB[db.Name()] = copySnap
}

func validateBenchprofExecutionPath(executionPath string) error {
	_, err := normalizeBenchprofExecutionPath(executionPath)
	return err
}

func normalizeBenchprofExecutionPath(executionPath string) (string, error) {
	executionPath = strings.TrimSpace(executionPath)
	if executionPath == "" {
		return "native-fastpath", nil
	}
	if executionPath == "oracle" || executionPath == "native-fastpath" || executionPath == "m8-m14-10mm-gate" || executionPath == "span-native-default-gate" || executionPath == "span-native-read-scan-guardrail" {
		return executionPath, nil
	}
	if strings.ContainsAny(executionPath, ",+") {
		return "", fmt.Errorf("invalid execution path %q: mixed-path labels are forbidden; expected one of oracle|native-fastpath|m8-m14-10mm-gate|span-native-default-gate|span-native-read-scan-guardrail", executionPath)
	}
	return "", fmt.Errorf("invalid execution path %q: expected one of oracle|native-fastpath|m8-m14-10mm-gate|span-native-default-gate|span-native-read-scan-guardrail", executionPath)
}

func printTreeDBCacheStats(w io.Writer, inst *DBInstance, prefix string) {
	if inst == nil || inst.Wrapper == nil || !isTreeDBInstance(inst) {
		return
	}
	sp, ok := inst.Wrapper.(kvstore.StatsProvider)
	if !ok {
		return
	}
	stats := sp.Stats()
	if len(stats) == 0 {
		return
	}

	// Keep this intentionally small and stable; full dumps are available via
	// suiteTreeDBCacheStats().
	keys := []string{
		"treedb.cache.memtable_mode",
		"treedb.cache.memtable_mode_config",
		"treedb.cache.queue_len",
		"treedb.cache.queue_backlog_bytes",
		"treedb.cache.memtable_residency.mutable.append_only.entry_backing_bytes",
		"treedb.cache.memtable_residency.queue.append_only.entry_backing_bytes",
		"treedb.process.memtable_residency.mutable.append_only.entry_backing_bytes",
		"treedb.process.memtable_residency.queue.append_only.entry_backing_bytes",
		"treedb.cache.flush_threshold_bytes",
		"treedb.cache.mutable_flush_threshold_base_bytes",
		"treedb.cache.mutable_flush_threshold_effective_bytes",
		"treedb.cache.max_queued_memtables",
		"treedb.cache.flush_bps_ewma",
		"treedb.cache.stats.backend_write_batches_total",
		"treedb.cache.wal_bytes_estimate",
		"treedb.cache.vlog_retained_bytes_estimate",
		"treedb.cache.materialization.lag_age_ms",
		"treedb.cache.batch_arena.pool_budget_bytes",
		"treedb.cache.batch_arena.pool_budget_effective_bytes",
		"treedb.cache.batch_arena.pool_bytes_estimate",
		"treedb.cache.batch_arena.pool_bytes_global_max_estimate",
		"treedb.cache.batch_arena.in_flight_bytes_estimate",
		"treedb.cache.batch_arena.in_flight_bytes_global_max_estimate",
		"treedb.cache.batch_arena.leased_bytes",
		"treedb.cache.batch_arena.leased_bytes_max",
		"treedb.cache.batch_arena.leased_bytes_global_estimate",
		"treedb.cache.batch_arena.leased_bytes_global_max_estimate",
		"treedb.cache.batch_arena.retained_bytes_global_estimate",
		"treedb.cache.batch_arena.retained_bytes_global_max_estimate",
		"treedb.cache.batch_arena.alloc_requested_bytes_total",
		"treedb.cache.batch_arena.alloc_class_bytes_total",
		"treedb.cache.batch_arena.used_bytes_total",
		"treedb.cache.batch_arena.tail_waste_bytes_total",
		"treedb.cache.batch_arena.tail_compact_runs_total",
		"treedb.cache.batch_arena.tail_compact_copied_bytes_total",
		"treedb.cache.batch_arena.tail_compact_saved_bytes_total",
		"treedb.cache.batch_arena.pool_skip_zero_budget_total",
		"treedb.cache.batch_arena.pool_drop_bytes_total",
		"treedb.cache.batch_arena.pool_drop_hard_cap_bytes_total",
		"treedb.cache.batch_arena.borrow_blocked_total",
		"treedb.cache.batch_arena.borrow_preflight_blocked_total",
		"treedb.cache.batch_arena.borrow_preflight_blocked_bytes_total",
		"treedb.cache.batch_arena.steal_suppressed_deferred_total",
		"treedb.cache.batch_arena.steal_suppressed_deferred_entries_total",
		"treedb.cache.entry_slice.pool_budget_bytes",
		"treedb.cache.entry_slice.pool_budget_effective_bytes",
		"treedb.cache.entry_slice.retained_bytes_estimate",
		"treedb.cache.entry_slice.trim_runs_total",
		"treedb.cache.entry_slice.trim_drop_bytes_total",
		"treedb.cache.entry_slice.get.lease_hits_total",
		"treedb.cache.entry_slice.get.lease_hit_bytes_total",
		"treedb.cache.entry_slice.get.pool_hits_total",
		"treedb.cache.entry_slice.get.pool_hit_bytes_total",
		"treedb.cache.entry_slice.get.fresh_alloc_total",
		"treedb.cache.entry_slice.get.fresh_alloc_bytes_total",
		"treedb.cache.entry_slice.put.lease_total",
		"treedb.cache.entry_slice.put.lease_bytes_total",
		"treedb.cache.entry_slice.put.pool_total",
		"treedb.cache.entry_slice.put.pool_bytes_total",
		"treedb.cache.entry_slice.put.drop_budget_total",
		"treedb.cache.entry_slice.put.drop_budget_bytes_total",
		"treedb.cache.flush_merge.shadowed_ops_total",
		"treedb.cache.flush_merge.applied_ops_total",
		"treedb.cache.flush_merge.shadowed_per_applied",
		"treedb.cache.flush_merge.deferred.shadowed_ops_total",
		"treedb.cache.flush_merge.deferred.applied_ops_total",
		"treedb.cache.flush_merge.deferred.shadowed_per_applied",
		"treedb.cache.flush_merge.parallel.shadowed_ops_total",
		"treedb.cache.flush_merge.parallel.applied_ops_total",
		"treedb.cache.flush_merge.parallel.shadowed_per_applied",
		"treedb.cache.append_only.entry_hint_entries",
		"treedb.cache.append_only.entry_hint_capacity_bytes",
		"treedb.cache.append_only.entry_pool_retained_bytes_estimate",
		"treedb.cache.append_only.entry_pool_retained_bytes_max_estimate",
		"treedb.cache.append_only.mem_lease_count",
		"treedb.cache.append_only.mem_lease_entry_capacity",
		"treedb.cache.append_only.mem_lease_entry_backing_bytes",
		"treedb.cache.append_only.mutable_count",
		"treedb.cache.append_only.mutable_entry_capacity",
		"treedb.cache.append_only.mutable_entry_backing_bytes",
		"treedb.cache.append_only.mutable_trim_total",
		"treedb.cache.append_only.mutable_trim_dropped_bytes_total",
		"treedb.cache.append_only.mutable_from_lease_total",
		"treedb.cache.append_only.mutable_from_pool_total",
		"treedb.cache.append_only.mutable_pool_puts_total",
		"treedb.cache.append_only.mutable_pool_entry_backing_dropped_bytes_total",
		"treedb.cache.append_only.mutable_new_alloc_total",
		"treedb.cache.append_only.mutable_new_alloc_with_queue_total",
		"treedb.cache.append_only.mutable_new_alloc_queue_bytes_sum",
		"treedb.cache.append_only_direct_arena.retain_max_bytes_effective",
		"treedb.cache.append_only_direct_arena.retain_max_chunks_effective",
		"treedb.cache.append_only_direct_arena.pool_hit_chunks_total",
		"treedb.cache.append_only_direct_arena.pool_hit_bytes_total",
		"treedb.cache.append_only_direct_arena.retained_hit_chunks_total",
		"treedb.cache.append_only_direct_arena.retained_hit_bytes_total",
		"treedb.cache.append_only_direct_arena.fresh_alloc_chunks_total",
		"treedb.cache.append_only_direct_arena.fresh_alloc_bytes_total",
		"treedb.process.memory.pool_pressure_level",
		"treedb.process.memory.rss_bytes",
		"treedb.process.memory.rss_hwm_bytes",
		"treedb.process.memory.rss_minus_heap_inuse_bytes",
		"treedb.process.memory.rss_minus_total_sys_bytes",
		"treedb.process.memory.peak_rss_bytes",
		"treedb.process.memory.peak_heap_alloc_bytes",
		"treedb.process.memory.peak_heap_inuse_bytes",
		"treedb.process.memory.peak_total_sys_bytes",
		"treedb.process.memory.heap_alloc_bytes",
		"treedb.process.memory.heap_inuse_bytes",
		"treedb.process.memory.heap_sys_bytes",
		"treedb.process.memory.heap_idle_unreleased_bytes",
		"treedb.process.memory.gomemlimit_bytes",
		"treedb.process.memory.pool_pressure_normal_samples_total",
		"treedb.process.memory.pool_pressure_high_samples_total",
		"treedb.process.memory.pool_pressure_critical_samples_total",
		"treedb.process.memory.vlog_mmap_active_bytes",
		"treedb.process.memory.vlog_mmap_current_bytes",
		"treedb.process.memory.vlog_mmap_sealed_bytes",
		"treedb.process.memory.vlog_mmap_active_segments",
		"treedb.process.memory.vlog_mmap_current_segments",
		"treedb.process.memory.vlog_mmap_sealed_segments",
		"treedb.process.memory.peak_vlog_mmap_active_bytes",
		"treedb.process.memory.peak_vlog_mmap_current_bytes",
		"treedb.process.memory.peak_vlog_mmap_sealed_bytes",
		"treedb.process.memory.peak_vlog_mmap_active_segments",
		"treedb.process.memory.peak_vlog_mmap_current_segments",
		"treedb.process.memory.peak_vlog_mmap_sealed_segments",
		"treedb.cache.vlog_auto.frames.off",
		"treedb.cache.vlog_auto.frames.dict",
		"treedb.cache.vlog_auto.frames.block_snappy",
		"treedb.cache.vlog_auto.frames.block_lz4",
		"treedb.cache.vlog_auto.frames.block_zstd",
		"treedb.cache.vlog_auto.probe_attempts",
		"treedb.cache.vlog_auto.probe_successes",
		"treedb.cache.vlog_auto.hold_enters",
		"treedb.cache.vlog_auto.hold_exits",
		"treedb.cache.vlog_auto.bypass_bytes",
		"treedb.cache.vlog_write_mode.raw_bytes.off",
		"treedb.cache.vlog_write_mode.raw_bytes.block",
		"treedb.cache.vlog_write_mode.raw_bytes.dict",
		"treedb.cache.vlog_write_mode.stored_bytes.off",
		"treedb.cache.vlog_write_mode.stored_bytes.block",
		"treedb.cache.vlog_write_mode.stored_bytes.dict",
		"treedb.cache.vlog_write_mode.stored_ratio.off",
		"treedb.cache.vlog_write_mode.stored_ratio.block",
		"treedb.cache.vlog_write_mode.stored_ratio.dict",
		"treedb.cache.vlog_write_mode.frames.off",
		"treedb.cache.vlog_write_mode.frames.block",
		"treedb.cache.vlog_write_mode.frames.dict",
		"treedb.cache.vlog_payload_kind.raw_bytes.single_value",
		"treedb.cache.vlog_payload_kind.raw_bytes.outer_leaf",
		"treedb.cache.vlog_payload_kind.raw_bytes.mixed",
		"treedb.cache.vlog_payload_kind.stored_bytes.single_value",
		"treedb.cache.vlog_payload_kind.stored_bytes.outer_leaf",
		"treedb.cache.vlog_payload_kind.stored_bytes.mixed",
		"treedb.cache.vlog_payload_kind.stored_ratio.single_value",
		"treedb.cache.vlog_payload_kind.stored_ratio.outer_leaf",
		"treedb.cache.vlog_payload_kind.stored_ratio.mixed",
		"treedb.cache.vlog_payload_kind.frames.single_value",
		"treedb.cache.vlog_payload_kind.frames.outer_leaf",
		"treedb.cache.vlog_payload_kind.frames.mixed",
		"treedb.cache.vlog_payload_split.raw_bytes.single_value",
		"treedb.cache.vlog_payload_split.raw_bytes.outer_leaf",
		"treedb.cache.vlog_payload_split.stored_bytes.single_value",
		"treedb.cache.vlog_payload_split.stored_bytes.outer_leaf",
		"treedb.cache.vlog_payload_split.stored_ratio.single_value",
		"treedb.cache.vlog_payload_split.stored_ratio.outer_leaf",
		"treedb.cache.vlog_payload_split.records.single_value",
		"treedb.cache.vlog_payload_split.records.outer_leaf",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.none",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.snappy",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.legacy_page",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.unknown",
		"treedb.cache.vlog_outer_leaf_codec.raw_bytes.mixed",
		"treedb.cache.vlog_outer_leaf_codec.stored_bytes.none",
		"treedb.cache.vlog_outer_leaf_codec.stored_bytes.snappy",
		"treedb.cache.vlog_outer_leaf_codec.stored_bytes.lz4",
		"treedb.cache.vlog_outer_leaf_codec.stored_bytes.legacy_page",
		"treedb.cache.vlog_outer_leaf_codec.stored_bytes.unknown",
		"treedb.cache.vlog_outer_leaf_codec.stored_bytes.mixed",
		"treedb.cache.vlog_outer_leaf_codec.stored_ratio.none",
		"treedb.cache.vlog_outer_leaf_codec.stored_ratio.snappy",
		"treedb.cache.vlog_outer_leaf_codec.stored_ratio.lz4",
		"treedb.cache.vlog_outer_leaf_codec.stored_ratio.legacy_page",
		"treedb.cache.vlog_outer_leaf_codec.stored_ratio.unknown",
		"treedb.cache.vlog_outer_leaf_codec.stored_ratio.mixed",
		"treedb.cache.vlog_outer_leaf_codec.frames.none",
		"treedb.cache.vlog_outer_leaf_codec.frames.snappy",
		"treedb.cache.vlog_outer_leaf_codec.frames.lz4",
		"treedb.cache.vlog_outer_leaf_codec.frames.legacy_page",
		"treedb.cache.vlog_outer_leaf_codec.frames.unknown",
		"treedb.cache.vlog_outer_leaf_codec.frames.mixed",
		"treedb.cache.vlog_block.k.count.snappy",
		"treedb.cache.vlog_block.k.avg.snappy",
		"treedb.cache.vlog_block.k.max.snappy",
		"treedb.cache.vlog_block.k.count.lz4",
		"treedb.cache.vlog_block.k.avg.lz4",
		"treedb.cache.vlog_block.k.max.lz4",
		"treedb.cache.vlog_block.ratio.snappy",
		"treedb.cache.vlog_block.ratio.lz4",
		"treedb.cache.vlog_dict.last_applied_dict_id",
		"treedb.cache.vlog_dict.frames_attempted",
		"treedb.cache.vlog_dict.frames_kept",
		"treedb.cache.vlog_dict.current_k",
		"treedb.cache.vlog_writev.syscalls",
		"treedb.cache.vlog_writev.bytes",
		"treedb.cache.vlog_writev.iovecs",
		"treedb.cache.vlog_writev.flushes",
		"treedb.cache.vlog_writev.bytes_per_syscall",
		"treedb.cache.vlog_writev.iovecs_per_syscall",
		"treedb.cache.vlog_writev.bytes_per_flush",
		"treedb.cache.vlog_writev.syscalls_per_flush",
		"treedb.cache.vlog_write.syscalls",
		"treedb.cache.vlog_write.bytes",
		"treedb.cache.vlog_write.calls",
		"treedb.cache.vlog_write.bytes_per_syscall",
		"treedb.cache.vlog_write.bytes_per_call",
		"treedb.cache.vlog_write.syscalls_per_call",
		"treedb.cache.vlog_io.syscalls",
		"treedb.cache.vlog_io.bytes",
		"treedb.cache.vlog_io.bytes_per_syscall",
		"treedb.cache.vlog_mmap.remaps",
		"treedb.cache.vlog_mmap.dead_mappings",
		"treedb.cache.vlog_mmap.dead_mappings.cap_base",
		"treedb.cache.vlog_mmap.current_writable_map_target_bytes",
		"treedb.cache.vlog_mmap.max_mapped_sealed_segments",
		"treedb.cache.vlog_mmap.max_mapped_sealed_bytes",
		"treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments",
		"treedb.cache.vlog_mmap.max_mapped_leaf_sealed_bytes",
		"treedb.cache.vlog_mmap.sealed_map_denied",
		"treedb.cache.vlog_mmap.sealed_map_denied.count_cap",
		"treedb.cache.vlog_mmap.sealed_map_denied.bytes_cap",
		"treedb.cache.vlog_mmap.read.hits",
		"treedb.cache.vlog_mmap.read.miss_out_of_range",
		"treedb.cache.vlog_mmap.read.miss_no_mapping",
		"treedb.cache.vlog_mmap.read.miss_dead_mapping_cap",
		"treedb.cache.vlog_mmap.read.fallback_readat",
		"treedb.cache.vlog_mmap.read.hit_ratio",
		"treedb.cache.vlog_grouped_frame_cache.hits",
		"treedb.cache.vlog_grouped_frame_cache.misses",
		"treedb.cache.vlog_grouped_frame_cache.stores",
		"treedb.cache.vlog_grouped_frame_cache.evictions",
		"treedb.cache.vlog_grouped_frame_cache.releases",
		"treedb.cache.vlog_grouped_frame_cache.retained_bytes",
		"treedb.cache.vlog_grouped_frame_cache.budget_bytes",
		"treedb.cache.vlog_grouped_frame_cache.skipped_disabled",
		"treedb.cache.vlog_grouped_frame_cache.skipped_oversize",
		"treedb.cache.vlog_grouped_frame_cache.skipped_budget",
		"treedb.cache.vlog_grouped_frame_cache.skipped_contention",
		"treedb.cache.vlog_grouped_frame_cache.entries",
		"treedb.cache.vlog_grouped_frame_cache.capacity",
		"treedb.cache.vlog_grouped_frame_cache.allocated_shards",
		"treedb.cache.vlog_grouped_frame_cache.allocated_slots",
		"treedb.cache.vlog_grouped_frame_cache.hit_ratio",
		"treedb.vlog.grouped_frame_cache.hits",
		"treedb.vlog.grouped_frame_cache.misses",
		"treedb.vlog.grouped_frame_cache.stores",
		"treedb.vlog.grouped_frame_cache.evictions",
		"treedb.vlog.grouped_frame_cache.releases",
		"treedb.vlog.grouped_frame_cache.retained_bytes",
		"treedb.vlog.grouped_frame_cache.budget_bytes",
		"treedb.vlog.grouped_frame_cache.skipped_disabled",
		"treedb.vlog.grouped_frame_cache.skipped_oversize",
		"treedb.vlog.grouped_frame_cache.skipped_budget",
		"treedb.vlog.grouped_frame_cache.skipped_contention",
		"treedb.vlog.grouped_frame_cache.entries",
		"treedb.vlog.grouped_frame_cache.capacity",
		"treedb.vlog.grouped_frame_cache.allocated_shards",
		"treedb.vlog.grouped_frame_cache.allocated_slots",
		"treedb.vlog.grouped_frame_cache.hit_ratio",
		"treedb.process.read_path.outer_leaf.cache.hits",
		"treedb.process.read_path.outer_leaf.cache.misses",
		"treedb.process.read_path.outer_leaf.cache.stores",
		"treedb.process.read_path.outer_leaf.cache.evictions",
		"treedb.process.read_path.outer_leaf.cache.conflict_evictions",
		"treedb.process.read_path.outer_leaf.cache.capacity_evictions",
		"treedb.process.read_path.outer_leaf.cache.entries",
		"treedb.process.read_path.outer_leaf.cache.capacity",
		"treedb.process.read_path.outer_leaf.cache.buckets",
		"treedb.process.read_path.outer_leaf.cache.ways",
		"treedb.process.read_path.outer_leaf.cache.write_admission_policy",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_skips",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_candidate_skips",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_lock_skips",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_stores",
		"treedb.process.read_path.outer_leaf.cache.write_admission_attempts",
		"treedb.process.read_path.outer_leaf.cache.write_admission_stores",
		"treedb.process.read_path.outer_leaf.cache.write_admission_skips",
		"treedb.process.read_path.outer_leaf.cache.write_admission_lock_skips",
		"treedb.vlog.decode_buffer_grow.calls_total",
		"treedb.vlog.decode_buffer_grow.realloc_calls_total",
		"treedb.vlog.decode_buffer_grow.realloc_rate",
		"treedb.cache.vlog_generation.enabled",
		"treedb.cache.vlog_generation.policy",
		"treedb.cache.vlog_generation.scheduler_state",
		"treedb.cache.vlog_generation.scheduler_last_reason",
		"treedb.cache.vlog_generation.maintenance_phase",
		"treedb.cache.vlog_generation.maintenance.attempts",
		"treedb.cache.vlog_generation.maintenance.acquired",
		"treedb.cache.vlog_generation.maintenance.collisions",
		"treedb.cache.vlog_generation.maintenance.skip.wal_on_periodic",
		"treedb.cache.vlog_generation.maintenance.skip.maintenance_phase",
		"treedb.cache.vlog_generation.maintenance.skip.stage_gate",
		"treedb.cache.vlog_generation.maintenance.skip.stage_gate_not_due",
		"treedb.cache.vlog_generation.maintenance.skip.stage_gate_due_reserved",
		"treedb.cache.vlog_generation.maintenance.skip.age_blocked_gate",
		"treedb.cache.vlog_generation.maintenance.skip.priority_pending",
		"treedb.cache.vlog_generation.maintenance.skip.quiet_window",
		"treedb.cache.vlog_generation.maintenance.skip.before_first_checkpoint",
		"treedb.cache.vlog_generation.maintenance.skip.checkpoint_inflight",
		"treedb.cache.vlog_generation.churn_bytes_total",
		"treedb.cache.vlog_generation.churn_bytes_per_sec",
		"treedb.cache.vlog_generation.rewrite_trigger.stale_ratio_ppm",
		"treedb.cache.vlog_generation.rewrite_trigger.total_bytes",
		"treedb.cache.vlog_generation.rewrite_trigger.churn_per_sec",
		"treedb.cache.vlog_generation.rewrite.min_segment_age_ms",
		"treedb.cache.vlog_generation.bytes.live.total",
		"treedb.cache.vlog_generation.bytes.live.hot",
		"treedb.cache.vlog_generation.bytes.live.warm",
		"treedb.cache.vlog_generation.bytes.live.cold",
		"treedb.cache.vlog_generation.bytes.stale.total",
		"treedb.cache.vlog_generation.bytes.total.total",
		"treedb.cache.vlog_generation.bytes.total.hot",
		"treedb.cache.vlog_generation.bytes.total.warm",
		"treedb.cache.vlog_generation.bytes.total.cold",
		"treedb.cache.vlog_generation.segments.total",
		"treedb.cache.vlog_generation.segments.hot",
		"treedb.cache.vlog_generation.segments.warm",
		"treedb.cache.vlog_generation.segments.cold",
		"treedb.cache.vlog_generation.rewrite.queue_len",
		"treedb.cache.vlog_generation.rewrite.queue_loaded",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.checkpoint_kick",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.checkpoint_kick",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.checkpoint_kick",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.checkpoint_kick",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.fresh_plan",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.fresh_plan",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.fresh_plan",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.fresh_plan",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions.fresh_plan",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.budget_tokens",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.debt_drain_cap",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.checkpoint_kick_safety",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_queue_threshold.fresh_plan",
		"treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_cap.fresh_plan",
		"treedb.cache.vlog_generation.rewrite.queue_config.resume_max_segments",
		"treedb.cache.vlog_generation.rewrite.queue_config.debt_drain_max_segments",
		"treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_min_segments",
		"treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_max_segments",
		"treedb.cache.vlog_generation.rewrite.ledger_segments",
		"treedb.cache.vlog_generation.rewrite.ledger_bytes_total",
		"treedb.cache.vlog_generation.rewrite.ledger_bytes_live",
		"treedb.cache.vlog_generation.rewrite.ledger_bytes_stale",
		"treedb.cache.vlog_generation.rewrite.ledger_stale_ratio_ppm",
		"treedb.cache.vlog_generation.rewrite.stage_pending",
		"treedb.cache.vlog_generation.rewrite.stage_observed_unix_nano",
		"treedb.cache.vlog_generation.rewrite.penalties_active",
		"treedb.cache.vlog_generation.rewrite.age_blocked_until_unix_nano",
		"treedb.cache.vlog_generation.rewrite.age_blocked_remaining_ms",
		"treedb.cache.vlog_generation.rewrite.plan_runs",
		"treedb.cache.vlog_generation.rewrite.plan_canceled",
		"treedb.cache.vlog_generation.rewrite.plan_errors",
		"treedb.cache.vlog_generation.rewrite.plan_empty",
		"treedb.cache.vlog_generation.rewrite.plan_empty.age_blocked",
		"treedb.cache.vlog_generation.rewrite.plan_empty.no_selection",
		"treedb.cache.vlog_generation.rewrite.plan_selected",
		"treedb.cache.vlog_generation.rewrite.plan_selected_segments_total",
		"treedb.cache.vlog_generation.rewrite.plan_selected_bytes_total",
		"treedb.cache.vlog_generation.rewrite.plan_selected_bytes_live",
		"treedb.cache.vlog_generation.rewrite.plan_selected_bytes_stale",
		"treedb.cache.vlog_generation.rewrite.bytes_in",
		"treedb.cache.vlog_generation.rewrite.bytes_out",
		"treedb.cache.vlog_generation.rewrite.value_records_copied",
		"treedb.cache.vlog_generation.rewrite.value_bytes_copied",
		"treedb.cache.vlog_generation.rewrite.leafref_records_copied",
		"treedb.cache.vlog_generation.rewrite.leafref_bytes_copied",
		"treedb.cache.vlog_generation.rewrite.reclaim_ratio",
		"treedb.cache.vlog_generation.rewrite.output_ratio",
		"treedb.cache.vlog_generation.rewrite.processed_stale_ratio",
		"treedb.cache.vlog_generation.rewrite.exec.bytes_in_per_sec",
		"treedb.cache.vlog_generation.rewrite.exec.bytes_out_per_sec",
		"treedb.cache.vlog_generation.rewrite.exec.reclaimed_bytes_per_sec",
		"treedb.cache.vlog_generation.rewrite.exec.reclaimed_vs_churn_ratio",
		"treedb.cache.vlog_generation.rewrite.no_reclaim_runs",
		"treedb.cache.vlog_generation.rewrite.no_reclaim_stale_bytes",
		"treedb.cache.vlog_generation.rewrite.canceled_runs",
		"treedb.cache.vlog_generation.rewrite.deadline_runs",
		"treedb.cache.vlog_generation.rewrite.ineffective_runs",
		"treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_per_sec",
		"treedb.cache.vlog_generation.rewrite_budget.consumed_share_of_budget_pct",
		"treedb.cache.vlog_generation.rewrite_budget.bytes_per_sec",
		"treedb.cache.vlog_generation.rewrite_budget.records_per_sec",
		"treedb.cache.vlog_generation.rewrite_budget.tokens_bytes",
		"treedb.cache.vlog_generation.rewrite_budget.tokens_cap_bytes",
		"treedb.cache.vlog_generation.rewrite_budget.tokens_utilization_pct",
		"treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total",
		"treedb.cache.vlog_generation.rewrite.runs",
		"treedb.cache.vlog_generation.gc.deleted_segments",
		"treedb.cache.vlog_generation.gc.deleted_bytes",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_referenced",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_active",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_protected",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_eligible",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_deleted",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_pending",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_retained",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_in_use",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_overlap",
		"treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_other",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_referenced",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_active",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_eligible",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_deleted",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_pending",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_retained",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_in_use",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_overlap",
		"treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_other",
		"treedb.cache.vlog_generation.gc.runs",
		"treedb.cache.vlog_retained_prune.runs",
		"treedb.cache.vlog_retained_prune.forced_runs",
		"treedb.cache.vlog_retained_prune.schedule_requests",
		"treedb.cache.vlog_retained_prune.schedule_forced_requests",
		"treedb.cache.vlog_retained_prune.schedule_skip.closing",
		"treedb.cache.vlog_retained_prune.schedule_skip.inflight",
		"treedb.cache.vlog_retained_prune.schedule_skip.no_closed_bytes",
		"treedb.cache.vlog_retained_prune.schedule_skip.below_pressure",
		"treedb.cache.vlog_retained_prune.schedule_skip.min_interval",
		"treedb.cache.vlog_retained_prune.force_pending",
		"treedb.cache.vlog_retained_prune.closed_bytes",
		"treedb.cache.vlog_retained_prune.removed_segments",
		"treedb.cache.vlog_retained_prune.removed_bytes",
		"treedb.cache.vlog_retained_prune.in_use_skipped_segments",
		"treedb.cache.vlog_retained_prune.in_use_skipped_bytes",
		"treedb.cache.vlog_retained_prune.live_skipped_segments",
		"treedb.cache.vlog_retained_prune.live_skipped_bytes",
		"treedb.cache.vlog_retained_prune.zombie_marked_segments",
		"treedb.cache.vlog_retained_prune.zombie_marked_bytes",
		"treedb.cache.vlog_generation.vacuum.runs",
		"treedb.cache.vlog_generation.vacuum.failures",
		"treedb.cache.vlog_generation.checkpoint_kick.pending",
		"treedb.cache.vlog_generation.checkpoint_kick.runs",
		"treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs",
		"treedb.cache.vlog_generation.checkpoint_kick.gc_runs",
		"treedb.cache.vlog_generation.remap.successes",
		"treedb.cache.vlog_generation.remap.failures",
		"treedb.process.read_path.outer_leaf.cache.bytes",
		"treedb.process.read_path.outer_leaf.cache.write_admission_policy",
		"treedb.process.read_path.outer_leaf.cache.write_admission_attempts",
		"treedb.process.read_path.outer_leaf.cache.write_admission_stores",
		"treedb.process.read_path.outer_leaf.cache.write_admission_skips",
		"treedb.process.read_path.outer_leaf.cache.write_admission_lock_skips",
		"treedb.process.read_path.outer_leaf.cache.record_checksum_verified_stores",
		"treedb.process.read_path.outer_leaf.cache.page_checksum_verified_marks",
		"treedb.process.read_path.outer_leaf.cache.page_checksum_verified_hits",
		"treedb.process.read_path.outer_leaf.cache.page_checksum_unverified_hits",
		"treedb.process.read_path.outer_leaf.checksum.verifications_total",
		"treedb.process.read_path.outer_leaf.checksum.skips_total",
		"treedb.vlog.outer_leaf_block_cache.policy",
		"treedb.vlog.outer_leaf_block_cache.hits",
		"treedb.vlog.outer_leaf_block_cache.misses",
		"treedb.vlog.outer_leaf_block_cache.hit_ratio",
		"treedb.vlog.outer_leaf_block_cache.entries",
		"treedb.vlog.outer_leaf_block_cache.capacity",
		"treedb.vlog.outer_leaf_block_cache.put_attempts",
		"treedb.vlog.outer_leaf_block_cache.put_admitted",
		"treedb.vlog.outer_leaf_block_cache.put_duplicate_drops",
		"treedb.vlog.outer_leaf_block_cache.put_lock_contention",
		"treedb.vlog.mmap_remaps",
		"treedb.vlog.mmap_dead_mappings",
		"treedb.vlog.mmap_dead_mappings.cap_base",
		"treedb.vlog.mmap_current_writable_map_target_bytes",
		"treedb.vlog.mmap_max_mapped_sealed_segments",
		"treedb.vlog.mmap_max_mapped_sealed_bytes",
		"treedb.vlog.mmap_max_mapped_leaf_sealed_segments",
		"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes",
		"treedb.vlog.mmap_sealed_map_denied",
		"treedb.vlog.mmap_sealed_map_denied.count_cap",
		"treedb.vlog.mmap_sealed_map_denied.bytes_cap",
		"treedb.vlog.mmap_read.hits",
		"treedb.vlog.mmap_read.miss_out_of_range",
		"treedb.vlog.mmap_read.miss_no_mapping",
		"treedb.vlog.mmap_read.miss_dead_mapping_cap",
		"treedb.vlog.mmap_read.fallback_readat",
		"treedb.vlog.mmap_read.hit_ratio",
	}

	fmt.Fprintf(w, "%s (%s):", prefix, inst.Wrapper.Name())
	for _, k := range keys {
		v, ok := stats[k]
		if !ok || v == "" {
			continue
		}
		fmt.Fprintf(w, " %s=%s", k, v)
	}
	fmt.Fprintln(w)
}

func resolveKeyCounts(keys int, keyCountsArg, keyScaleArg string, keysMin, keysMax int) ([]int, error) {
	if strings.TrimSpace(keyCountsArg) != "" {
		parts := strings.Split(keyCountsArg, ",")
		out := make([]int, 0, len(parts))
		for _, p := range parts {
			v, err := parseKeyCount(p)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return sortUniquePositive(out), nil
	}

	if strings.TrimSpace(keyScaleArg) != "" {
		scale := strings.ToLower(strings.TrimSpace(keyScaleArg))
		if keysMin <= 0 || keysMax <= 0 || keysMin > keysMax {
			return nil, fmt.Errorf("invalid -keys-min/-keys-max: %d..%d", keysMin, keysMax)
		}

		var out []int
		switch scale {
		case "log10":
			for v := keysMin; v <= keysMax; {
				out = append(out, v)
				if v > keysMax/10 {
					break
				}
				v *= 10
			}
		case "doubling":
			for v := keysMin; v <= keysMax; {
				out = append(out, v)
				if v > keysMax/2 {
					break
				}
				v *= 2
			}
		default:
			return nil, fmt.Errorf("unknown -keyscale: %q (supported: log10, doubling)", scale)
		}
		return sortUniquePositive(out), nil
	}

	if keys <= 0 {
		return nil, fmt.Errorf("invalid -keys: %d", keys)
	}
	return []int{keys}, nil
}

func parseKeyCount(s string) (int, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty keycount")
	}

	multiplier := 1.0
	switch s[len(s)-1] {
	case 'k':
		multiplier = 1e3
		s = strings.TrimSpace(s[:len(s)-1])
	case 'm':
		multiplier = 1e6
		s = strings.TrimSpace(s[:len(s)-1])
	case 'g':
		multiplier = 1e9
		s = strings.TrimSpace(s[:len(s)-1])
	}

	s = strings.ReplaceAll(s, "_", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse keycount %q: %w", s, err)
	}
	v := int(f * multiplier)
	if v <= 0 {
		return 0, fmt.Errorf("invalid keycount %q", s)
	}
	return v, nil
}

func sortUniquePositive(vals []int) []int {
	if len(vals) == 0 {
		return nil
	}
	sort.Ints(vals)
	out := vals[:0]
	for _, v := range vals {
		if v <= 0 {
			continue
		}
		if len(out) == 0 || v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func runSweep(baseCfg BenchConfig, keyCounts []int) ([]BenchRun, error) {
	runs := make([]BenchRun, 0, len(keyCounts))
	for _, kc := range keyCounts {
		cfg := baseCfg
		cfg.Keys = kc
		run, err := runBenchmark(cfg)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

type benchGuard struct {
	maxWall  time.Duration
	deadline time.Time

	maxRSSMB    int
	maxRSSBytes uint64
}

func newBenchGuard(cfg BenchConfig) *benchGuard {
	g := &benchGuard{
		maxWall:  cfg.MaxWall,
		maxRSSMB: cfg.MaxRSSMB,
	}
	if cfg.MaxWall > 0 {
		g.deadline = time.Now().Add(cfg.MaxWall)
	}
	if cfg.MaxRSSMB > 0 {
		g.maxRSSBytes = uint64(cfg.MaxRSSMB) * 1024 * 1024
	}
	return g
}

func (g *benchGuard) Checkpoint() error {
	if g == nil {
		return nil
	}
	if !g.deadline.IsZero() && time.Now().After(g.deadline) {
		return fmt.Errorf("guard: max-wall exceeded (%s)", g.maxWall)
	}
	if g.maxRSSBytes > 0 {
		rss, ok, err := currentRSSBytes()
		if err != nil {
			return fmt.Errorf("guard: rss: %w", err)
		}
		if !ok {
			return fmt.Errorf("guard: max-rss-mb is only supported on linux")
		}
		if rss > g.maxRSSBytes {
			return fmt.Errorf("guard: max-rss-mb exceeded (rss=%dMiB cap=%dMiB)", rss/(1024*1024), g.maxRSSMB)
		}
	}
	return nil
}

type checkpointer interface {
	Checkpoint() error
}

type vacuumIndexOnline interface {
	VacuumIndexOnline(ctx context.Context) error
}

type periodicCheckpoint struct {
	everyOps   int
	everyBytes int64

	ops   int
	bytes int64
}

func newPeriodicCheckpoint(cfg BenchConfig) *periodicCheckpoint {
	if cfg.CheckpointEveryOps <= 0 && cfg.CheckpointEveryBytes <= 0 {
		return nil
	}
	return &periodicCheckpoint{
		everyOps:   cfg.CheckpointEveryOps,
		everyBytes: cfg.CheckpointEveryBytes,
	}
}

func parseUint64StatValue(stats map[string]string, keys ...string) (uint64, bool) {
	if stats == nil {
		return 0, false
	}
	for _, key := range keys {
		raw, ok := stats[key]
		if !ok {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err == nil {
			return v, true
		}
	}
	return 0, false
}

func parseInt64StatValue(stats map[string]string, keys ...string) (int64, bool) {
	if stats == nil {
		return 0, false
	}
	for _, key := range keys {
		raw, ok := stats[key]
		if !ok {
			continue
		}
		v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err == nil {
			return v, true
		}
	}
	return 0, false
}

func treeDBMmapReadStatSource(stats map[string]string) string {
	if len(stats) == 0 {
		return ""
	}
	if hasTreeDBMmapReadStatSource(stats, treeDBVlogMmapReadBackendPrefix) {
		return treeDBVlogMmapReadBackendPrefix
	}
	if hasTreeDBMmapReadStatSource(stats, treeDBVlogMmapReadCachePrefix) {
		return treeDBVlogMmapReadCachePrefix
	}
	return ""
}

func hasTreeDBMmapReadStatSource(stats map[string]string, prefix string) bool {
	for _, def := range treeDBMmapReadStatDefs {
		if _, ok := stats[prefix+def.suffix]; ok {
			return true
		}
	}
	return false
}

func parseTreeDBMmapReadUint64Stat(stats map[string]string, suffix string) (uint64, bool) {
	prefix := treeDBMmapReadStatSource(stats)
	if prefix == "" {
		return 0, false
	}
	return parseUint64StatValue(stats, prefix+suffix)
}

func snapshotSelectedTreeDBStats(db kvstore.DB) treeDBSelectedStats {
	sp, ok := db.(kvstore.StatsProvider)
	if !ok {
		return treeDBSelectedStats{}
	}
	stats := sp.Stats()
	var snap treeDBSelectedStats
	snap.mmapHits, _ = parseTreeDBMmapReadUint64Stat(stats, "hits")
	snap.mmapMissOutOfRange, _ = parseTreeDBMmapReadUint64Stat(stats, "miss_out_of_range")
	snap.mmapMissNoMapping, _ = parseTreeDBMmapReadUint64Stat(stats, "miss_no_mapping")
	snap.mmapMissDeadCap, _ = parseTreeDBMmapReadUint64Stat(stats, "miss_dead_mapping_cap")
	snap.mmapFallbackReadAt, _ = parseTreeDBMmapReadUint64Stat(stats, "fallback_readat")
	snap.leafGenerationsPin, _ = parseInt64StatValue(stats, "treedb.leaf_generation.generations.pinned")
	snap.leafPinsTotal, _ = parseInt64StatValue(stats, "treedb.leaf_generation.pins.total")
	return snap
}

func computeTreeDBPerfMetrics(before, after treeDBSelectedStats, snapshot treeDBSnapshotPerfMetrics) treeDBPerfMetrics {
	m := treeDBPerfMetrics{
		Mmap: treeDBMmapPerfMetrics{
			Hits:           saturatingUint64Delta(after.mmapHits, before.mmapHits),
			MissOutOfRange: saturatingUint64Delta(after.mmapMissOutOfRange, before.mmapMissOutOfRange),
			MissNoMapping:  saturatingUint64Delta(after.mmapMissNoMapping, before.mmapMissNoMapping),
			MissDeadMapCap: saturatingUint64Delta(after.mmapMissDeadCap, before.mmapMissDeadCap),
			FallbackReadAt: saturatingUint64Delta(after.mmapFallbackReadAt, before.mmapFallbackReadAt),
		},
		Snapshot:                   snapshot,
		LeafGenerationsPinnedAfter: after.leafGenerationsPin,
		LeafPinsTotalAfter:         after.leafPinsTotal,
	}
	totalReads := m.Mmap.Hits + m.Mmap.FallbackReadAt
	if totalReads > 0 {
		m.Mmap.HitRatio = float64(m.Mmap.Hits) / float64(totalReads)
	}
	if m.Snapshot.AcquireCalls > 0 {
		m.Snapshot.AcquireAvgMicros = float64(m.Snapshot.AcquireTotalNanos) / float64(m.Snapshot.AcquireCalls) / 1_000.0
	}
	if m.Snapshot.CloseCalls > 0 {
		m.Snapshot.CloseAvgMicros = float64(m.Snapshot.CloseTotalNanos) / float64(m.Snapshot.CloseCalls) / 1_000.0
	}
	return m
}

func saturatingUint64Delta(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func treeDBPerfMetricsEmpty(m treeDBPerfMetrics) bool {
	return m.Mmap.Hits == 0 &&
		m.Mmap.MissOutOfRange == 0 &&
		m.Mmap.MissNoMapping == 0 &&
		m.Mmap.MissDeadMapCap == 0 &&
		m.Mmap.FallbackReadAt == 0 &&
		m.Mmap.HitRatio == 0 &&
		m.Snapshot.AcquireCalls == 0 &&
		m.Snapshot.AcquireTotalNanos == 0 &&
		m.Snapshot.CloseCalls == 0 &&
		m.Snapshot.CloseTotalNanos == 0 &&
		m.LeafGenerationsPinnedAfter == 0 &&
		m.LeafPinsTotalAfter == 0
}

func (p *periodicCheckpoint) Add(db kvstore.DB, opsDelta int, bytesDelta int64) error {
	if p == nil {
		return nil
	}
	if p.everyOps <= 0 && p.everyBytes <= 0 {
		return nil
	}
	p.ops += opsDelta
	p.bytes += bytesDelta

	hitOps := p.everyOps > 0 && p.ops >= p.everyOps
	hitBytes := p.everyBytes > 0 && p.bytes >= p.everyBytes
	if !hitOps && !hitBytes {
		return nil
	}

	p.ops = 0
	p.bytes = 0

	cp, ok := db.(checkpointer)
	if !ok {
		return nil
	}
	return cp.Checkpoint()
}

func runBenchmark(cfg BenchConfig) (BenchRun, error) {
	if cfg.Keys <= 0 {
		return BenchRun{}, fmt.Errorf("invalid keys: %d", cfg.Keys)
	}
	cfg.ReadWorkers = resolveReadWorkers(cfg.ReadWorkers)
	profileHooks := profileHooksFromConfig(cfg)

	defer installAllocsProfileRate(cfg)()
	keyShapeName := strings.ToLower(strings.TrimSpace(cfg.KeyShape))
	if keyShapeName == "" {
		keyShapeName = "be8"
	}
	keyShape, keyShapeErr := parseBenchKeyShape(keyShapeName)
	if keyShapeErr != nil {
		return BenchRun{}, keyShapeErr
	}
	cfg.KeyShape = keyShapeName
	if cfg.ValueSize < 0 {
		return BenchRun{}, fmt.Errorf("invalid valsize: %d", cfg.ValueSize)
	}
	if cfg.BatchSize <= 0 {
		return BenchRun{}, fmt.Errorf("invalid batchsize: %d", cfg.BatchSize)
	}
	cfg.BatchDeleteRangeWidth, cfg.BatchDeleteRangesPerBatch = normalizeBatchDeleteRangeDimensions(cfg.BatchDeleteRangeWidth, cfg.BatchDeleteRangesPerBatch)
	if cfg.BatchDeleteRangeWidth < 0 || cfg.BatchDeleteRangesPerBatch < 0 {
		return BenchRun{}, fmt.Errorf("invalid batch_delete_range settings: width=%d ranges_per_batch=%d", cfg.BatchDeleteRangeWidth, cfg.BatchDeleteRangesPerBatch)
	}
	if cfg.RangeQueries < 0 || cfg.RangeSpan < 0 {
		return BenchRun{}, fmt.Errorf("invalid range settings: queries=%d span=%d", cfg.RangeQueries, cfg.RangeSpan)
	}

	dbNames := resolveDBs(cfg.DBsArg, cfg.DBsExcludeArg)
	var err error
	dbNames, err = applyCompressionVariants(dbNames, cfg.DBsExcludeArg)
	if err != nil {
		return BenchRun{}, err
	}
	testsToRun := normalizeTests(parseList(cfg.TestsArg))

	// Initialize DBs
	instances := make([]*DBInstance, 0)

	for _, name := range dbNames {
		factory, err := GetDBFactory(name)
		if err != nil {
			return BenchRun{}, err
		}

		dir, err := os.MkdirTemp("", "bench-"+name+"*")
		if err != nil {
			return BenchRun{}, fmt.Errorf("temp dir: %w", err)
		}

		db, err := factory(dir)
		if err != nil {
			_ = os.RemoveAll(dir)
			return BenchRun{}, fmt.Errorf("init %s: %w", name, err)
		}
		vlogCompressionMode, vlogCompressionModeSet := selectedTreeDBVlogCompressionMode(name)
		instances = append(instances, &DBInstance{Name: name, Wrapper: db, Dir: dir, TreeDBVlogCompressionMode: vlogCompressionMode, TreeDBVlogCompressionModeSet: vlogCompressionModeSet})
	}
	if len(instances) == 0 {
		return BenchRun{}, fmt.Errorf("no DBs selected")
	}

	// Define Tests
	type TestFunc func(db kvstore.DB, rng *rand.Rand) (float64, error)

	guard := newBenchGuard(cfg)
	checkpointDurations := make(map[string]map[string]time.Duration)
	checkpointSettleDurations := make(map[string]map[string]time.Duration)
	checkpointTreeDBStats := make(map[string]map[string]map[string]string)
	vacuumDurations := make(map[string]map[string]time.Duration)
	vacuumIndexBytes := make(map[string]map[string][2]uint64)
	treeDBPerf := make(map[string]map[string]treeDBPerfMetrics)
	snapshotPerfByTest := make(map[string]map[string]treeDBSnapshotPerfMetrics)
	batchDeleteRangeReports := make(map[string]map[string]batchDeleteRangeReport)

	// dataset_write_* mirrors op-geth's Write1M when -keys=1_000_000 and -valsize=32 (32B keys).
	datasetKeySize := 32
	datasetValSize := cfg.ValueSize
	datasetCount := cfg.Keys
	var (
		datasetRandomKeys [][]byte
		datasetRandomVals [][]byte
		datasetSortedKeys [][]byte
		datasetSortedVals [][]byte
		datasetErr        error
	)
	// dataset_write_* uses the same normalized -val-pattern pipeline as the
	// regular write tests (via makeValuePool). Legacy names remain accepted by
	// normalizeWriteValuePattern, but generation semantics are unified here:
	// "repeat" maps to the shared repeat+tail behavior and "random" is seeded
	// deterministically from cfg.SeedUsed.
	makeWriteDataset := func(count, ksize, vsize int, order bool) ([][]byte, [][]byte, error) {
		mode, err := normalizeWriteValuePattern(cfg.ValuePattern)
		if err != nil {
			return nil, nil, err
		}
		keys := make([][]byte, count)
		vals := makeValuePool(cfg.SeedUsed, mode, vsize, count)
		for i := 0; i < count; i++ {
			k := make([]byte, ksize)
			if _, err := io.ReadFull(crand.Reader, k); err != nil {
				return nil, nil, fmt.Errorf("dataset key %d: %w", i, err)
			}
			keys[i] = k
		}
		if order {
			sort.Slice(keys, func(i, j int) bool {
				return bytes.Compare(keys[i], keys[j]) < 0
			})
		}
		return keys, vals, nil
	}
	ensureWriteDatasets := func() error {
		if datasetErr != nil {
			return datasetErr
		}
		if datasetRandomKeys != nil && datasetSortedKeys != nil {
			return nil
		}
		datasetRandomKeys, datasetRandomVals, datasetErr = makeWriteDataset(datasetCount, datasetKeySize, datasetValSize, false)
		if datasetErr != nil {
			return datasetErr
		}
		datasetSortedKeys, datasetSortedVals, datasetErr = makeWriteDataset(datasetCount, datasetKeySize, datasetValSize, true)
		return datasetErr
	}

	var (
		writeValuePool     [][]byte
		writeValuePoolErr  error
		writeValuePoolInit bool
	)
	getWriteValuePool := func() ([][]byte, error) {
		if writeValuePoolInit {
			return writeValuePool, writeValuePoolErr
		}
		writeValuePoolInit = true
		writeValuePool, writeValuePoolErr = makeWriteValuePool(cfg.SeedUsed, cfg.ValuePattern, cfg.ValueSize, cfg.ValuePoolSize)
		return writeValuePool, writeValuePoolErr
	}
	var (
		batchWriteSteadyCPUProfileStart func() error
		batchWriteSteadyCPUProfileStop  func()
	)

	prefixScanBase := 0
	expectedFullScanCount := -1
	checkPrefixCounts := false
	encodeKey := func(dst []byte, key uint64) {
		keyShape.encode(dst, key)
	}
	runBatchWrite := func(db kvstore.DB, steady bool) (float64, error) {
		batcher, ok := db.(kvstore.Batcher)
		if !ok {
			return math.NaN(), nil
		}
		testLabel := "batch_write"
		if steady {
			testLabel = "batch_write_steady"
		}
		values, err := getWriteValuePool()
		if err != nil {
			return 0, fmt.Errorf("%s values: %w", testLabel, err)
		}
		total := cfg.Keys
		valPos := 0
		// Keep the precomputed-key optimization bounded so very large runs do
		// not retain a huge contiguous key buffer for the whole benchmark.
		const maxPrecomputedKeyBytes = 128 << 20 // 128 MiB
		precomputeKeys := total > 0 && total <= maxPrecomputedKeyBytes/8
		var keyBytes []byte
		if precomputeKeys {
			keyBytes = make([]byte, total*8)
			for j := 0; j < total; j++ {
				encodeKey(keyBytes[j*8:(j+1)*8], uint64(j+cfg.Keys))
			}
		}
		type batcherWithSize interface {
			NewBatchWithSize(size int) (kvstore.Batch, error)
		}
		type batchSetView interface {
			SetView(key, value []byte) error
		}
		type resettableBatch interface {
			Reset()
		}
		var (
			batch      kvstore.Batch
			setView    func(key, value []byte) error
			resetBatch func()
		)
		openBatch := func() error {
			var err error
			if bs, ok := batcher.(batcherWithSize); ok {
				batch, err = bs.NewBatchWithSize(cfg.BatchSize)
			} else {
				batch, err = batcher.NewBatch()
			}
			if err != nil {
				return err
			}
			setView = nil
			if sv, ok := batch.(batchSetView); ok {
				setView = sv.SetView
			}
			resetBatch = nil
			if rb, ok := batch.(resettableBatch); ok {
				resetBatch = rb.Reset
			}
			return nil
		}
		defer func() {
			if batch != nil {
				_ = batch.Close()
			}
		}()

		treeDBDictPublished := func() bool {
			sp, ok := db.(kvstore.StatsProvider)
			if !ok {
				return false
			}
			stats := sp.Stats()
			raw := strings.TrimSpace(stats["treedb.cache.vlog_dict.last_applied_dict_id"])
			if raw == "" {
				return false
			}
			v, err := strconv.ParseUint(raw, 10, 64)
			return err == nil && v != 0
		}
		shouldWarmupDictBatchWrite := func() bool {
			if !cfg.BatchWriteDictWarmup {
				return false
			}
			if total <= 0 {
				return false
			}
			if _, ok := db.(*treedbadapter.DB); !ok {
				return false
			}
			mode, _, err := parseTreeDBVlogCompressionMode(*treedbVlogCompression)
			if err == nil && mode == treedb.ValueLogCompressionDict {
				return true
			}
			name := strings.ToLower(strings.TrimSpace(db.Name()))
			return strings.Contains(name, "vlog=dict") || strings.Contains(name, "vlog_dict=on")
		}
		if shouldWarmupDictBatchWrite() {
			warmupKeys := total / 4
			if warmupKeys < cfg.BatchSize {
				warmupKeys = cfg.BatchSize
			}
			if warmupKeys > total {
				warmupKeys = total
			}
			if warmupKeys > 128_000 {
				warmupKeys = 128_000
			}
			// Keep warmup writes disjoint from measured keys so throughput/IO
			// remains comparable across modes.
			warmupBase := uint64(cfg.Keys) * 2
			if uint64(cfg.Keys) > math.MaxUint64/2 {
				warmupBase = math.MaxUint64
			}
			warmupKeys = clampWarmupKeyCount(keyShape, warmupBase, warmupKeys)
			var warmupKeyBytes []byte
			if precomputeKeys && warmupKeys > 0 {
				warmupKeyBytes = make([]byte, warmupKeys*8)
				for j := 0; j < warmupKeys; j++ {
					encodeKey(warmupKeyBytes[j*8:(j+1)*8], uint64(j)+warmupBase)
				}
			}
			for i := 0; i < warmupKeys; i += cfg.BatchSize {
				if batch == nil {
					if err := openBatch(); err != nil {
						return 0, fmt.Errorf("%s warmup: new batch: %w", testLabel, err)
					}
				} else if resetBatch != nil {
					resetBatch()
				} else {
					if err := batch.Close(); err != nil {
						return 0, fmt.Errorf("%s warmup: close: %w", testLabel, err)
					}
					batch = nil
					if err := openBatch(); err != nil {
						return 0, fmt.Errorf("%s warmup: new batch: %w", testLabel, err)
					}
				}

				end := i + cfg.BatchSize
				if end > warmupKeys {
					end = warmupKeys
				}
				if setView != nil {
					if precomputeKeys {
						for j := i; j < end; j++ {
							keyView := warmupKeyBytes[j*8 : (j+1)*8]
							value := values[valPos%len(values)]
							valPos++
							if err := setView(keyView, value); err != nil {
								return 0, fmt.Errorf("%s warmup: set: %w", testLabel, err)
							}
						}
					} else {
						// SetView is zero-copy: keep keys in a stable owned slab until commit.
						need := (end - i) * 8
						keysView := make([]byte, need)
						for j := i; j < end; j++ {
							off := (j - i) * 8
							keyView := keysView[off : off+8]
							encodeKey(keyView, uint64(j)+warmupBase)
							value := values[valPos%len(values)]
							valPos++
							if err := setView(keyView, value); err != nil {
								return 0, fmt.Errorf("%s warmup: set: %w", testLabel, err)
							}
						}
					}
				} else {
					for j := i; j < end; j++ {
						var keyView []byte
						if precomputeKeys {
							keyView = warmupKeyBytes[j*8 : (j+1)*8]
						} else {
							var key [8]byte
							encodeKey(key[:], uint64(j)+warmupBase)
							keyView = key[:]
						}
						value := values[valPos%len(values)]
						valPos++
						if err := batch.Set(keyView, value); err != nil {
							return 0, fmt.Errorf("%s warmup: set: %w", testLabel, err)
						}
					}
				}
				if err := batch.Commit(); err != nil {
					return 0, fmt.Errorf("%s warmup: commit: %w", testLabel, err)
				}
				if resetBatch == nil {
					if err := batch.Close(); err != nil {
						return 0, fmt.Errorf("%s warmup: close: %w", testLabel, err)
					}
					batch = nil
				}
				if treeDBDictPublished() {
					break
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			for !treeDBDictPublished() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			valPos = 0
		}

		var stopSteadyCPUProfile func()
		if steady && batchWriteSteadyCPUProfileStart != nil {
			if err := batchWriteSteadyCPUProfileStart(); err != nil {
				return 0, fmt.Errorf("%s cpu profile: %w", testLabel, err)
			}
			stopSteadyCPUProfile = batchWriteSteadyCPUProfileStop
			defer func() {
				if stopSteadyCPUProfile != nil {
					stopSteadyCPUProfile()
				}
			}()
		}
		start := time.Now()
		pc := newPeriodicCheckpoint(cfg)
		if steady && pc == nil && cfg.BatchWriteSteadyCheckpointBytes > 0 {
			pc = &periodicCheckpoint{everyBytes: cfg.BatchWriteSteadyCheckpointBytes}
		}
		perOpBytes := int64(8 + cfg.ValueSize)
		for i := 0; i < total; i += cfg.BatchSize {
			if i&8191 == 0 {
				if err := guard.Checkpoint(); err != nil {
					return 0, err
				}
			}
			if batch == nil {
				if err := openBatch(); err != nil {
					return 0, fmt.Errorf("%s: new batch: %w", testLabel, err)
				}
			} else if resetBatch != nil {
				resetBatch()
			} else {
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("%s: close: %w", testLabel, err)
				}
				batch = nil
				if err := openBatch(); err != nil {
					return 0, fmt.Errorf("%s: new batch: %w", testLabel, err)
				}
			}

			end := i + cfg.BatchSize
			if end > total {
				end = total
			}
			if setView != nil {
				if precomputeKeys {
					// batch_write uses immutable values from a prebuilt pool and
					// precomputed immutable keys.
					for j := i; j < end; j++ {
						keyView := keyBytes[j*8 : (j+1)*8]
						value := values[valPos%len(values)]
						valPos++
						if err := setView(keyView, value); err != nil {
							return 0, fmt.Errorf("%s: set: %w", testLabel, err)
						}
					}
				} else {
					// Keep the zero-copy SetView path for large key counts by using
					// one owned key slab per batch (stable beyond Commit()).
					need := (end - i) * 8
					keysView := make([]byte, need)
					for j := i; j < end; j++ {
						off := (j - i) * 8
						keyView := keysView[off : off+8]
						encodeKey(keyView, uint64(j+cfg.Keys))
						value := values[valPos%len(values)]
						valPos++
						if err := setView(keyView, value); err != nil {
							return 0, fmt.Errorf("%s: set: %w", testLabel, err)
						}
					}
				}
			} else {
				for j := i; j < end; j++ {
					var keyView []byte
					if precomputeKeys {
						keyView = keyBytes[j*8 : (j+1)*8]
					} else {
						var key [8]byte
						encodeKey(key[:], uint64(j+cfg.Keys))
						keyView = key[:]
					}
					value := values[valPos%len(values)]
					valPos++
					if err := batch.Set(keyView, value); err != nil {
						return 0, fmt.Errorf("%s: set: %w", testLabel, err)
					}
				}
			}
			if err := batch.Commit(); err != nil {
				return 0, fmt.Errorf("%s: commit: %w", testLabel, err)
			}
			if resetBatch == nil {
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("%s: close: %w", testLabel, err)
				}
				batch = nil
			}
			if err := pc.Add(db, end-i, int64(end-i)*perOpBytes); err != nil {
				return 0, fmt.Errorf("%s checkpoint: %w", testLabel, err)
			}
		}
		if steady {
			if cp, ok := db.(checkpointer); ok {
				if err := cp.Checkpoint(); err != nil {
					return 0, fmt.Errorf("%s final checkpoint: %w", testLabel, err)
				}
			}
		}
		elapsed := time.Since(start)
		if stopSteadyCPUProfile != nil {
			stopSteadyCPUProfile()
			stopSteadyCPUProfile = nil
		}
		return float64(total) / elapsed.Seconds(), nil
	}
	recordBatchDeleteRangeReport := func(testName string, db kvstore.DB, report batchDeleteRangeReport) {
		if db == nil {
			return
		}
		perDB := batchDeleteRangeReports[testName]
		if perDB == nil {
			perDB = make(map[string]batchDeleteRangeReport)
			batchDeleteRangeReports[testName] = perDB
		}
		perDB[db.Name()] = report
	}
	batchDeleteRangeMode := func(db kvstore.DB) string {
		if reporter, ok := db.(kvstore.RangeDeleteModeReporter); ok {
			mode := strings.TrimSpace(reporter.RangeDeleteMode())
			if mode != "" {
				return mode
			}
		}
		return "unknown"
	}
	loadBatchDeleteRangeKeys := func(db kvstore.DB, batcher kvstore.Batcher, values [][]byte) error {
		if len(values) == 0 {
			return errors.New("batch_delete_range: empty value pool")
		}
		var k [8]byte
		valPos := 0
		for i := 0; i < cfg.Keys; i += cfg.BatchSize {
			if i&8191 == 0 {
				if err := guard.Checkpoint(); err != nil {
					return err
				}
			}
			batch, err := batcher.NewBatch()
			if err != nil {
				return fmt.Errorf("new load batch: %w", err)
			}
			end := i + cfg.BatchSize
			if end > cfg.Keys {
				end = cfg.Keys
			}
			for j := i; j < end; j++ {
				encodeKey(k[:], uint64(j))
				value := values[valPos%len(values)]
				valPos++
				if err := batch.Set(k[:], value); err != nil {
					_ = batch.Close()
					return fmt.Errorf("load set: %w", err)
				}
			}
			if err := batch.Commit(); err != nil {
				_ = batch.Close()
				return fmt.Errorf("load commit: %w", err)
			}
			if err := batch.Close(); err != nil {
				return fmt.Errorf("load close: %w", err)
			}
		}
		if cp, ok := db.(checkpointer); ok {
			if err := cp.Checkpoint(); err != nil {
				return fmt.Errorf("load checkpoint: %w", err)
			}
		}
		return nil
	}
	validateBatchDeleteRange := func(db kvstore.DB) error {
		rs, ok := db.(kvstore.RangeScanner)
		if !ok {
			return fmt.Errorf("validation requires RangeScanner")
		}
		var startBuf, endBuf [8]byte
		encodeKey(startBuf[:], 0)
		encodeKey(endBuf[:], uint64(cfg.Keys))
		iter, err := rs.Iterator(startBuf[:], endBuf[:])
		if err != nil {
			return fmt.Errorf("validation iterator: %w", err)
		}
		defer func() { _ = iter.Close() }()
		if iter.Valid() {
			key := iter.KeyCopy(nil)
			return fmt.Errorf("validation found live key after DeleteRange: %x", key)
		}
		if err := iter.Error(); err != nil {
			return fmt.Errorf("validation iterator error: %w", err)
		}
		return nil
	}
	type batchDeleteRangePrep struct {
		checked   bool
		supported bool
		values    [][]byte
	}
	batchDeleteRangePreps := make(map[string]batchDeleteRangePrep)
	probeBatchDeleteRange := func(batcher kvstore.Batcher) (bool, error) {
		probeBatch, err := batcher.NewBatch()
		if err != nil {
			return false, fmt.Errorf("batch_delete_range: new batch probe: %w", err)
		}
		_, ok := probeBatch.(kvstore.BatchRangeDeleter)
		if closeErr := probeBatch.Close(); closeErr != nil {
			return false, fmt.Errorf("batch_delete_range: close probe: %w", closeErr)
		}
		return ok, nil
	}
	prepareBatchDeleteRange := func(db kvstore.DB) error {
		name := db.Name()
		if prep, ok := batchDeleteRangePreps[name]; ok && prep.checked {
			return nil
		}
		prep := batchDeleteRangePrep{checked: true}
		batcher, ok := db.(kvstore.Batcher)
		if !ok {
			batchDeleteRangePreps[name] = prep
			return nil
		}
		supported, err := probeBatchDeleteRange(batcher)
		if err != nil {
			return err
		}
		prep.supported = supported
		if !supported {
			batchDeleteRangePreps[name] = prep
			return nil
		}
		width := cfg.BatchDeleteRangeWidth
		rangesPerBatch := cfg.BatchDeleteRangesPerBatch
		if width <= 0 || rangesPerBatch <= 0 {
			return fmt.Errorf("batch_delete_range requires positive width and ranges_per_batch (got width=%d ranges_per_batch=%d)", width, rangesPerBatch)
		}
		values, err := getWriteValuePool()
		if err != nil {
			return fmt.Errorf("batch_delete_range values: %w", err)
		}
		if err := loadBatchDeleteRangeKeys(db, batcher, values); err != nil {
			return fmt.Errorf("batch_delete_range load: %w", err)
		}
		prep.values = values
		batchDeleteRangePreps[name] = prep
		return nil
	}
	runBatchDeleteRange := func(db kvstore.DB) (float64, error) {
		prep, ok := batchDeleteRangePreps[db.Name()]
		if !ok || !prep.checked {
			if err := prepareBatchDeleteRange(db); err != nil {
				return 0, err
			}
			prep = batchDeleteRangePreps[db.Name()]
		}
		if !prep.supported {
			return math.NaN(), nil
		}
		batcher, ok := db.(kvstore.Batcher)
		if !ok {
			return math.NaN(), nil
		}
		width := cfg.BatchDeleteRangeWidth
		rangesPerBatch := cfg.BatchDeleteRangesPerBatch
		if width <= 0 || rangesPerBatch <= 0 {
			return 0, fmt.Errorf("batch_delete_range requires positive width and ranges_per_batch (got width=%d ranges_per_batch=%d)", width, rangesPerBatch)
		}

		rangeCount := (cfg.Keys + width - 1) / width
		if rangeCount <= 0 {
			return math.NaN(), nil
		}
		mode := batchDeleteRangeMode(db)
		pc := newPeriodicCheckpoint(cfg)
		var startBuf, endBuf [8]byte
		start := time.Now()
		deletedRanges := 0
		for r := 0; r < rangeCount; {
			if r&8191 == 0 {
				if err := guard.Checkpoint(); err != nil {
					return 0, err
				}
			}
			batch, err := batcher.NewBatch()
			if err != nil {
				return 0, fmt.Errorf("batch_delete_range: new batch: %w", err)
			}
			deleter, ok := batch.(kvstore.BatchRangeDeleter)
			if !ok {
				_ = batch.Close()
				return math.NaN(), nil
			}
			batchRanges := 0
			batchAffected := 0
			for batchRanges < rangesPerBatch && r < rangeCount {
				startIdx := r * width
				endIdx := startIdx + width
				if endIdx > cfg.Keys {
					endIdx = cfg.Keys
				}
				encodeKey(startBuf[:], uint64(startIdx))
				encodeKey(endBuf[:], uint64(endIdx))
				if err := deleter.DeleteRange(startBuf[:], endBuf[:]); err != nil {
					_ = batch.Close()
					return 0, fmt.Errorf("batch_delete_range: delete range [%d,%d): %w", startIdx, endIdx, err)
				}
				batchRanges++
				batchAffected += endIdx - startIdx
				r++
			}
			if err := batch.Commit(); err != nil {
				_ = batch.Close()
				return 0, fmt.Errorf("batch_delete_range: commit: %w", err)
			}
			if err := batch.Close(); err != nil {
				return 0, fmt.Errorf("batch_delete_range: close: %w", err)
			}
			deletedRanges += batchRanges
			if err := pc.Add(db, batchRanges, int64(batchAffected)*8); err != nil {
				return 0, fmt.Errorf("batch_delete_range checkpoint: %w", err)
			}
		}
		duration := time.Since(start)
		if duration <= 0 {
			duration = time.Nanosecond
		}
		rangeOpsPerSec := float64(deletedRanges) / duration.Seconds()
		affectedKeysPerSec := float64(cfg.Keys) / duration.Seconds()
		validation := "not_run"
		if cfg.BatchDeleteRangeValidate {
			if err := validateBatchDeleteRange(db); err != nil {
				return 0, fmt.Errorf("batch_delete_range validation: %w", err)
			}
			validation = "passed"
		}
		if cfg.BatchDeleteRangeRefill {
			values := prep.values
			if len(values) == 0 {
				var err error
				values, err = getWriteValuePool()
				if err != nil {
					return 0, fmt.Errorf("batch_delete_range refill values: %w", err)
				}
			}
			if err := loadBatchDeleteRangeKeys(db, batcher, values); err != nil {
				return 0, fmt.Errorf("batch_delete_range refill: %w", err)
			}
		}
		report := batchDeleteRangeReport{
			Mode:                 mode,
			LoadedKeys:           cfg.Keys,
			RangeWidth:           width,
			RangesPerBatch:       rangesPerBatch,
			RangeCount:           deletedRanges,
			AffectedKeys:         cfg.Keys,
			AffectedKeysPerRange: float64(cfg.Keys) / float64(deletedRanges),
			ValueSize:            cfg.ValueSize,
			DeleteDurationMS:     float64(duration.Nanoseconds()) / 1_000_000.0,
			RangeOpsPerSec:       rangeOpsPerSec,
			AffectedKeysPerSec:   affectedKeysPerSec,
			Validation:           validation,
			Refill:               cfg.BatchDeleteRangeRefill,
		}
		recordBatchDeleteRangeReport("batch_delete_range", db, report)
		return rangeOpsPerSec, nil
	}
	recordSnapshotPerf := func(testName string, db kvstore.DB, perf treeDBSnapshotPerfMetrics) {
		if db == nil {
			return
		}
		if perf.AcquireCalls == 0 && perf.CloseCalls == 0 {
			return
		}
		perDB := snapshotPerfByTest[testName]
		if perDB == nil {
			perDB = make(map[string]treeDBSnapshotPerfMetrics)
			snapshotPerfByTest[testName] = perDB
		}
		perDB[db.Name()] = perf
	}
	testFuncs := map[string]TestFunc{
		"vacuum_index": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			td, ok := db.(*treedbadapter.DB)
			if !ok || td == nil || td.DB == nil {
				return math.NaN(), nil
			}
			var err error
			switch td.DB.ResolvedProfile() {
			case treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed:
				err = td.DB.VacuumIndexOnline(context.Background())
			default:
				err = td.DB.CompactIndex()
			}
			if err != nil {
				return 0, fmt.Errorf("vacuum_index: %w", err)
			}
			return math.NaN(), nil
		},
		"fragmentation_report_pre": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			td, ok := db.(*treedbadapter.DB)
			if !ok || td == nil || td.DB == nil {
				return math.NaN(), nil
			}
			rep, err := td.DB.FragmentationReport()
			if err != nil {
				return 0, fmt.Errorf("fragmentation_report_pre: %w", err)
			}
			if err := treedbdb.ValidateFragmentationReport(rep); err != nil {
				return 0, fmt.Errorf("fragmentation_report_pre validate: %w", err)
			}
			printFragmentationReport(os.Stderr, "pre_settle", db.Name(), rep)
			return math.NaN(), nil
		},
		"fragmentation_report_post": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			td, ok := db.(*treedbadapter.DB)
			if !ok || td == nil || td.DB == nil {
				return math.NaN(), nil
			}
			rep, err := td.DB.FragmentationReport()
			if err != nil {
				return 0, fmt.Errorf("fragmentation_report_post: %w", err)
			}
			if err := treedbdb.ValidateFragmentationReport(rep); err != nil {
				return 0, fmt.Errorf("fragmentation_report_post validate: %w", err)
			}
			printFragmentationReport(os.Stderr, "post_settle", db.Name(), rep)
			return math.NaN(), nil
		},
		"sequential_write": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			values, err := getWriteValuePool()
			if err != nil {
				return 0, fmt.Errorf("sequential_write values: %w", err)
			}
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8 + cfg.ValueSize)
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				encodeKey(k[:], uint64(i))
				value := values[i%len(values)]
				if err := db.Set(k[:], value); err != nil {
					return 0, fmt.Errorf("sequential_write: %w", err)
				}
				if err := pc.Add(db, 1, perOpBytes); err != nil {
					return 0, fmt.Errorf("sequential_write checkpoint: %w", err)
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"random_write": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			values, err := getWriteValuePool()
			if err != nil {
				return 0, fmt.Errorf("random_write values: %w", err)
			}
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8 + cfg.ValueSize)
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				encodeKey(k[:], uint64(rng.Intn(cfg.Keys*10))) // Use a larger range for randomness
				value := values[i%len(values)]
				if err := db.Set(k[:], value); err != nil {
					return 0, fmt.Errorf("random_write: %w", err)
				}
				if err := pc.Add(db, 1, perOpBytes); err != nil {
					return 0, fmt.Errorf("random_write checkpoint: %w", err)
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"random_write_parallel": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			workers := cfg.WriteWorkers
			if workers <= 1 {
				start := time.Now()
				val := make([]byte, cfg.ValueSize)
				rng := rand.New(rand.NewSource(cfg.SeedUsed))
				pc := newPeriodicCheckpoint(cfg)
				perOpBytes := int64(8 + len(val))
				var k [8]byte
				for i := 0; i < cfg.Keys; i++ {
					if i&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							return 0, err
						}
					}
					encodeKey(k[:], uint64(rng.Intn(cfg.Keys*10)))
					if err := db.Set(k[:], val); err != nil {
						return 0, fmt.Errorf("random_write_parallel: %w", err)
					}
					if err := pc.Add(db, 1, perOpBytes); err != nil {
						return 0, fmt.Errorf("random_write_parallel checkpoint: %w", err)
					}
				}
				return float64(cfg.Keys) / time.Since(start).Seconds(), nil
			}
			if cfg.Keys <= 0 {
				return 0, nil
			}

			total := cfg.Keys
			perWorker := total / workers
			rem := total % workers

			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8 + len(val))
			var pcMu sync.Mutex

			var stop atomic.Bool
			errCh := make(chan error, 1)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				n := perWorker
				if w < rem {
					n++
				}
				seedW := cfg.SeedUsed + int64(w)
				rngW := rand.New(rand.NewSource(seedW))
				wg.Add(1)
				go func(n int, rng *rand.Rand) {
					defer wg.Done()
					var k [8]byte
					for i := 0; i < n; i++ {
						if stop.Load() {
							return
						}
						if i&8191 == 0 {
							if err := guard.Checkpoint(); err != nil {
								if stop.CompareAndSwap(false, true) {
									select {
									case errCh <- err:
									default:
									}
								}
								return
							}
						}
						encodeKey(k[:], uint64(rng.Intn(total*10)))
						if err := db.Set(k[:], val); err != nil {
							if stop.CompareAndSwap(false, true) {
								select {
								case errCh <- fmt.Errorf("random_write_parallel: %w", err):
								default:
								}
							}
							return
						}
						if pc != nil {
							pcMu.Lock()
							err := pc.Add(db, 1, perOpBytes)
							pcMu.Unlock()
							if err != nil {
								if stop.CompareAndSwap(false, true) {
									select {
									case errCh <- fmt.Errorf("random_write_parallel checkpoint: %w", err):
									default:
									}
								}
								return
							}
						}
					}
				}(n, rngW)
			}

			wg.Wait()
			select {
			case err := <-errCh:
				return 0, err
			default:
			}
			return float64(total) / time.Since(start).Seconds(), nil
		},
		"dataset_write_random": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			if err := ensureWriteDatasets(); err != nil {
				return 0, fmt.Errorf("dataset_write_random: %w", err)
			}
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(datasetKeySize + cfg.ValueSize)
			for i := 0; i < len(datasetRandomKeys); i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				if err := db.Set(datasetRandomKeys[i], datasetRandomVals[i]); err != nil {
					return 0, fmt.Errorf("dataset_write_random: %w", err)
				}
				if err := pc.Add(db, 1, perOpBytes); err != nil {
					return 0, fmt.Errorf("dataset_write_random checkpoint: %w", err)
				}
			}
			return float64(len(datasetRandomKeys)) / time.Since(start).Seconds(), nil
		},
		"dataset_write_sorted": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			if err := ensureWriteDatasets(); err != nil {
				return 0, fmt.Errorf("dataset_write_sorted: %w", err)
			}
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(datasetKeySize + cfg.ValueSize)
			for i := 0; i < len(datasetSortedKeys); i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				if err := db.Set(datasetSortedKeys[i], datasetSortedVals[i]); err != nil {
					return 0, fmt.Errorf("dataset_write_sorted: %w", err)
				}
				if err := pc.Add(db, 1, perOpBytes); err != nil {
					return 0, fmt.Errorf("dataset_write_sorted checkpoint: %w", err)
				}
			}
			return float64(len(datasetSortedKeys)) / time.Since(start).Seconds(), nil
		},
		"batch_write": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			return runBatchWrite(db, false)
		},
		"batch_write_steady": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			return runBatchWrite(db, true)
		},
		"batch_random": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			batcher, ok := db.(kvstore.Batcher)
			if !ok {
				return math.NaN(), nil
			}
			values, err := getWriteValuePool()
			if err != nil {
				return 0, fmt.Errorf("batch_random values: %w", err)
			}
			total := cfg.Keys
			batchSize := cfg.BatchSize

			const keySize = 8
			allKeys := make([]byte, total*keySize)
			for i := 0; i < total; i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				offset := i * keySize
				encodeKey(allKeys[offset:offset+keySize], uint64(rng.Intn(total*10))) // Spread out to cause random I/O
			}
			// Reset timer to exclude setup
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8 + cfg.ValueSize)
			valPos := 0

			for i := 0; i < total; i += batchSize {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				batch, err := batcher.NewBatch()
				if err != nil {
					return 0, fmt.Errorf("batch_random: new batch: %w", err)
				}

				end := i + batchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					offset := j * keySize
					key := allKeys[offset : offset+keySize]
					value := values[valPos%len(values)]
					valPos++
					if err := batch.Set(key, value); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("batch_random: set: %w", err)
					}
				}
				if err := batch.Commit(); err != nil {
					_ = batch.Close()
					return 0, fmt.Errorf("batch_random: commit: %w", err)
				}
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("batch_random: close: %w", err)
				}
				if err := pc.Add(db, end-i, int64(end-i)*perOpBytes); err != nil {
					return 0, fmt.Errorf("batch_random checkpoint: %w", err)
				}
			}
			return float64(total) / time.Since(start).Seconds(), nil
		},
		"batch_delete": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			batcher, ok := db.(kvstore.Batcher)
			if !ok {
				return math.NaN(), nil
			}
			total := cfg.Keys
			batchSize := cfg.BatchSize
			type batcherWithSize interface {
				NewBatchWithSize(size int) (kvstore.Batch, error)
			}
			type batchDeleteView interface {
				DeleteView(key []byte) error
			}
			type resettableBatch interface {
				Reset()
			}
			var (
				batch      kvstore.Batch
				deleteOp   func(key []byte) error
				resetBatch func()
			)
			openBatch := func() error {
				var err error
				if bs, ok := batcher.(batcherWithSize); ok {
					batch, err = bs.NewBatchWithSize(batchSize)
				} else {
					batch, err = batcher.NewBatch()
				}
				if err != nil {
					return err
				}
				if batch == nil {
					return fmt.Errorf("batch_delete: new batch returned nil")
				}
				deleteOp = batch.Delete
				if dv, ok := batch.(batchDeleteView); ok {
					deleteOp = dv.DeleteView
				}
				resetBatch = nil
				if rb, ok := batch.(resettableBatch); ok {
					resetBatch = rb.Reset
				}
				return nil
			}
			closeBatch := func() error {
				if batch == nil {
					return nil
				}
				b := batch
				batch = nil
				deleteOp = nil
				resetBatch = nil
				return b.Close()
			}
			defer func() {
				_ = closeBatch()
			}()

			const keySize = 8
			allKeys := make([]byte, total*keySize)
			for i := 0; i < total; i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				offset := i * keySize
				encodeKey(allKeys[offset:offset+keySize], uint64(rng.Intn(total)))
			}
			// Reset timer to exclude setup
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8)

			for i := 0; i < total; i += batchSize {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				if batch == nil {
					if err := openBatch(); err != nil {
						return 0, fmt.Errorf("batch_delete: new batch: %w", err)
					}
				} else if resetBatch != nil {
					resetBatch()
				} else {
					if err := closeBatch(); err != nil {
						return 0, fmt.Errorf("batch_delete: close: %w", err)
					}
					if err := openBatch(); err != nil {
						return 0, fmt.Errorf("batch_delete: new batch: %w", err)
					}
				}

				end := i + batchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					offset := j * keySize
					key := allKeys[offset : offset+keySize]
					if err := deleteOp(key); err != nil {
						_ = closeBatch()
						return 0, fmt.Errorf("batch_delete: delete: %w", err)
					}
				}
				if err := batch.Commit(); err != nil {
					_ = closeBatch()
					return 0, fmt.Errorf("batch_delete: commit: %w", err)
				}
				if resetBatch == nil {
					if err := closeBatch(); err != nil {
						return 0, fmt.Errorf("batch_delete: close: %w", err)
					}
				}
				if err := pc.Add(db, end-i, int64(end-i)*perOpBytes); err != nil {
					return 0, fmt.Errorf("batch_delete checkpoint: %w", err)
				}
			}
			return float64(total) / time.Since(start).Seconds(), nil
		},
		"batch_delete_range": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			return runBatchDeleteRange(db)
		},
		"batch_small_seq": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			batcher, ok := db.(kvstore.Batcher)
			if !ok {
				return math.NaN(), nil
			}
			values, err := getWriteValuePool()
			if err != nil {
				return 0, fmt.Errorf("batch_small_seq values: %w", err)
			}
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8 + cfg.ValueSize)
			total := cfg.Keys
			batchSize := 100 // Pathological: small enough to hurt if not buffered, sequential to trigger streaming
			var k [8]byte
			valPos := 0

			for i := 0; i < total; i += batchSize {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				batch, err := batcher.NewBatch()
				if err != nil {
					return 0, fmt.Errorf("batch_small_seq: new batch: %w", err)
				}

				end := i + batchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					encodeKey(k[:], uint64(j)) // Sequential
					value := values[valPos%len(values)]
					valPos++
					if err := batch.Set(k[:], value); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("batch_small_seq: set: %w", err)
					}
				}
				if err := batch.Commit(); err != nil {
					_ = batch.Close()
					return 0, fmt.Errorf("batch_small_seq: commit: %w", err)
				}
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("batch_small_seq: close: %w", err)
				}
				if err := pc.Add(db, end-i, int64(end-i)*perOpBytes); err != nil {
					return 0, fmt.Errorf("batch_small_seq checkpoint: %w", err)
				}
			}
			return float64(total) / time.Since(start).Seconds(), nil
		},
		"update_fork_choice": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			start := time.Now()
			values, err := getWriteValuePool()
			if err != nil {
				return 0, fmt.Errorf("update_fork_choice values: %w", err)
			}
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8 + cfg.ValueSize)
			total := cfg.Keys
			batchSize := cfg.BatchSize
			if batchSize <= 0 {
				batchSize = 1
			}
			if batchSize == 1 {
				const maxSyncOps = 20_000
				if total > maxSyncOps {
					total = maxSyncOps
				}
			}
			var k [8]byte
			valPos := 0

			for i := 0; i < total; i += batchSize {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}

				end := i + batchSize
				if end > total {
					end = total
				}

				if batcher, ok := db.(kvstore.Batcher); ok {
					batch, err := batcher.NewBatch()
					if err != nil {
						return 0, fmt.Errorf("update_fork_choice: new batch: %w", err)
					}
					for j := i; j < end; j++ {
						encodeKey(k[:], uint64(rng.Intn(total*10)))
						value := values[valPos%len(values)]
						valPos++
						if err := batch.Set(k[:], value); err != nil {
							_ = batch.Close()
							return 0, fmt.Errorf("update_fork_choice: set: %w", err)
						}
					}
					if err := batch.CommitSync(); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("update_fork_choice: commit sync: %w", err)
					}
					if err := batch.Close(); err != nil {
						return 0, fmt.Errorf("update_fork_choice: close: %w", err)
					}
				} else if syncer, ok := db.(kvstore.Syncer); ok {
					for j := i; j < end; j++ {
						encodeKey(k[:], uint64(rng.Intn(total*10)))
						value := values[valPos%len(values)]
						valPos++
						if err := syncer.SetSync(k[:], value); err != nil {
							return 0, fmt.Errorf("update_fork_choice: set sync: %w", err)
						}
					}
				} else {
					for j := i; j < end; j++ {
						encodeKey(k[:], uint64(rng.Intn(total*10)))
						value := values[valPos%len(values)]
						valPos++
						if err := db.Set(k[:], value); err != nil {
							return 0, fmt.Errorf("update_fork_choice: set: %w", err)
						}
					}
				}

				if err := pc.Add(db, end-i, int64(end-i)*perOpBytes); err != nil {
					return 0, fmt.Errorf("update_fork_choice checkpoint: %w", err)
				}
			}

			return float64(total) / time.Since(start).Seconds(), nil
		},
		"dataset_update_fork_choice": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			// Like update_fork_choice but uses a 32-byte random key dataset to more
			// closely match geth key distributions.
			start := time.Now()
			values, err := getWriteValuePool()
			if err != nil {
				return 0, fmt.Errorf("dataset_update_fork_choice values: %w", err)
			}
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(datasetKeySize + cfg.ValueSize)
			total := cfg.Keys
			batchSize := cfg.BatchSize
			if batchSize <= 0 {
				batchSize = 1
			}
			if batchSize == 1 {
				const maxSyncOps = 20_000
				if total > maxSyncOps {
					total = maxSyncOps
				}
			}
			valPos := 0
			// Build a bounded random 32-byte key pool specifically for this workload.
			// Avoid allocating full dataset key/value arrays at very large -keys.
			keyPoolSize := total
			const maxKeyPool = 200_000
			if keyPoolSize > maxKeyPool {
				keyPoolSize = maxKeyPool
			}
			if keyPoolSize <= 0 {
				keyPoolSize = 1
			}
			datasetKeys := make([][]byte, keyPoolSize)
			for i := 0; i < keyPoolSize; i++ {
				k := make([]byte, datasetKeySize)
				if _, err := io.ReadFull(crand.Reader, k); err != nil {
					return 0, fmt.Errorf("dataset_update_fork_choice key %d: %w", i, err)
				}
				datasetKeys[i] = k
			}

			for i := 0; i < total; i += batchSize {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}

				end := i + batchSize
				if end > total {
					end = total
				}

				if batcher, ok := db.(kvstore.Batcher); ok {
					batch, err := batcher.NewBatch()
					if err != nil {
						return 0, fmt.Errorf("dataset_update_fork_choice: new batch: %w", err)
					}
					for j := i; j < end; j++ {
						key := datasetKeys[rng.Intn(len(datasetKeys))]
						value := values[valPos%len(values)]
						valPos++
						if err := batch.Set(key, value); err != nil {
							_ = batch.Close()
							return 0, fmt.Errorf("dataset_update_fork_choice: set: %w", err)
						}
					}
					if err := batch.CommitSync(); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("dataset_update_fork_choice: commit sync: %w", err)
					}
					if err := batch.Close(); err != nil {
						return 0, fmt.Errorf("dataset_update_fork_choice: close: %w", err)
					}
				} else if syncer, ok := db.(kvstore.Syncer); ok {
					for j := i; j < end; j++ {
						key := datasetKeys[rng.Intn(len(datasetKeys))]
						value := values[valPos%len(values)]
						valPos++
						if err := syncer.SetSync(key, value); err != nil {
							return 0, fmt.Errorf("dataset_update_fork_choice: set sync: %w", err)
						}
					}
				} else {
					for j := i; j < end; j++ {
						key := datasetKeys[rng.Intn(len(datasetKeys))]
						value := values[valPos%len(values)]
						valPos++
						if err := db.Set(key, value); err != nil {
							return 0, fmt.Errorf("dataset_update_fork_choice: set: %w", err)
						}
					}
				}

				if err := pc.Add(db, end-i, int64(end-i)*perOpBytes); err != nil {
					return 0, fmt.Errorf("dataset_update_fork_choice checkpoint: %w", err)
				}
			}

			return float64(total) / time.Since(start).Seconds(), nil
		},
		"random_delete": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			start := time.Now()
			pc := newPeriodicCheckpoint(cfg)
			perOpBytes := int64(8)
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				encodeKey(k[:], uint64(rng.Intn(cfg.Keys)))
				_ = db.Delete(k[:])
				if err := pc.Add(db, 1, perOpBytes); err != nil {
					return 0, fmt.Errorf("random_delete checkpoint: %w", err)
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"random_read": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			start := time.Now()
			var k [8]byte
			appendGetter, hasAppendGetter := db.(interface {
				GetAppend(key, dst []byte) ([]byte, error)
			})
			buf := make([]byte, 0, cfg.ValueSize)
			for i := 0; i < cfg.Keys; i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				encodeKey(k[:], uint64(rng.Intn(cfg.Keys)))
				if hasAppendGetter {
					var err error
					buf, err = appendGetter.GetAppend(k[:], buf[:0])
					if cfg.ReadRequireHit {
						if err != nil {
							return 0, fmt.Errorf("random_read: miss: %w", err)
						}
						if len(buf) != cfg.ValueSize {
							return 0, fmt.Errorf("random_read: value length mismatch: got=%d want=%d", len(buf), cfg.ValueSize)
						}
					}
				} else {
					val, err := db.Get(k[:])
					if cfg.ReadRequireHit {
						if err != nil {
							return 0, fmt.Errorf("random_read: miss: %w", err)
						}
						if len(val) != cfg.ValueSize {
							return 0, fmt.Errorf("random_read: value length mismatch: got=%d want=%d", len(val), cfg.ValueSize)
						}
					}
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"random_read_parallel": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			workers := cfg.ReadWorkers
			if workers <= 0 {
				workers = 1
			}
			if cfg.Keys <= 0 {
				return 0, nil
			}

			snapshotter, hasSnapshotter := db.(kvstore.ReadSnapshotter)
			appendGetter, hasAppendGetter := db.(interface {
				GetAppend(key, dst []byte) ([]byte, error)
			})
			baseSeed := testSeed(cfg.SeedUsed, "random_read_parallel")
			var snapshotAcquireCalls atomic.Uint64
			var snapshotAcquireNanos atomic.Uint64
			var snapshotCloseCalls atomic.Uint64
			var snapshotCloseNanos atomic.Uint64

			runWorker := func(workerRng *rand.Rand, stop *atomic.Bool) error {
				getter := interface {
					Get(key []byte) ([]byte, error)
				}(db)
				workerAppendGetter := appendGetter
				var closeSnapshot func() error
				if hasSnapshotter {
					acquireStarted := time.Now()
					snap, err := snapshotter.AcquireReadSnapshot()
					if err != nil && !errors.Is(err, kvstore.ErrUnsupported) {
						return err
					}
					if err == nil {
						snapshotAcquireCalls.Add(1)
						snapshotAcquireNanos.Add(uint64(time.Since(acquireStarted)))
						getter = snap
						workerAppendGetter = snap
						closeSnapshot = func() error {
							closeStarted := time.Now()
							err := snap.Close()
							snapshotCloseCalls.Add(1)
							snapshotCloseNanos.Add(uint64(time.Since(closeStarted)))
							return err
						}
					} else if !hasAppendGetter {
						workerAppendGetter = nil
					}
				} else if !hasAppendGetter {
					workerAppendGetter = nil
				}
				if closeSnapshot != nil {
					defer func() { _ = closeSnapshot() }()
				}
				var k [8]byte
				buf := make([]byte, 0, cfg.ValueSize)
				for i := 0; i < cfg.Keys; i++ {
					if stop != nil && stop.Load() {
						return nil
					}
					if i&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							return err
						}
					}
					encodeKey(k[:], uint64(workerRng.Intn(cfg.Keys)))
					if workerAppendGetter != nil {
						var err error
						buf, err = workerAppendGetter.GetAppend(k[:], buf[:0])
						if cfg.ReadRequireHit {
							if err != nil {
								return err
							}
							if len(buf) != cfg.ValueSize {
								return fmt.Errorf("random_read_parallel: value length mismatch: got=%d want=%d", len(buf), cfg.ValueSize)
							}
						}
					} else {
						val, err := getter.Get(k[:])
						if cfg.ReadRequireHit {
							if err != nil {
								return err
							}
							if len(val) != cfg.ValueSize {
								return fmt.Errorf("random_read_parallel: value length mismatch: got=%d want=%d", len(val), cfg.ValueSize)
							}
						}
					}
				}
				return nil
			}

			start := time.Now()
			if workers == 1 {
				if err := runWorker(rng, nil); err != nil {
					return 0, fmt.Errorf("random_read_parallel: %w", err)
				}
				recordSnapshotPerf("random_read_parallel", db, treeDBSnapshotPerfMetrics{
					AcquireCalls:      snapshotAcquireCalls.Load(),
					AcquireTotalNanos: snapshotAcquireNanos.Load(),
					CloseCalls:        snapshotCloseCalls.Load(),
					CloseTotalNanos:   snapshotCloseNanos.Load(),
				})
				return float64(cfg.Keys) / time.Since(start).Seconds(), nil
			}

			var stop atomic.Bool
			errCh := make(chan error, 1)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				seedW := baseSeed + int64(w)
				rngW := rand.New(rand.NewSource(seedW))
				wg.Add(1)
				go func(rng *rand.Rand) {
					defer wg.Done()
					if err := runWorker(rng, &stop); err != nil {
						if stop.CompareAndSwap(false, true) {
							select {
							case errCh <- err:
							default:
							}
						}
					}
				}(rngW)
			}
			wg.Wait()

			select {
			case err := <-errCh:
				return 0, fmt.Errorf("random_read_parallel: %w", err)
			default:
			}
			recordSnapshotPerf("random_read_parallel", db, treeDBSnapshotPerfMetrics{
				AcquireCalls:      snapshotAcquireCalls.Load(),
				AcquireTotalNanos: snapshotAcquireNanos.Load(),
				CloseCalls:        snapshotCloseCalls.Load(),
				CloseTotalNanos:   snapshotCloseNanos.Load(),
			})
			totalOps := float64(cfg.Keys) * float64(workers)
			return totalOps / time.Since(start).Seconds(), nil
		},
		"random_read_parallel_acquire_snapshot": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			workers := cfg.ReadWorkers
			if workers <= 0 {
				workers = 1
			}
			if cfg.Keys <= 0 {
				return 0, nil
			}

			snapshotter, hasSnapshotter := db.(kvstore.ReadSnapshotter)
			if !hasSnapshotter {
				return math.NaN(), nil
			}
			snapProbe, err := snapshotter.AcquireReadSnapshot()
			if err != nil {
				if errors.Is(err, kvstore.ErrUnsupported) {
					return math.NaN(), nil
				}
				return 0, fmt.Errorf("random_read_parallel_acquire_snapshot: %w", err)
			}
			if snapProbe == nil {
				return math.NaN(), nil
			}
			if err := snapProbe.Close(); err != nil {
				return 0, fmt.Errorf("random_read_parallel_acquire_snapshot probe close: %w", err)
			}
			baseSeed := testSeed(cfg.SeedUsed, "random_read_parallel_acquire_snapshot")
			var snapshotAcquireCalls atomic.Uint64
			var snapshotAcquireNanos atomic.Uint64
			var snapshotCloseCalls atomic.Uint64
			var snapshotCloseNanos atomic.Uint64

			runWorker := func(workerRng *rand.Rand, stop *atomic.Bool) error {
				var k [8]byte
				buf := make([]byte, 0, cfg.ValueSize)
				for i := 0; i < cfg.Keys; i++ {
					if stop != nil && stop.Load() {
						return nil
					}
					if i&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							return err
						}
					}
					encodeKey(k[:], uint64(workerRng.Intn(cfg.Keys)))

					acquireStarted := time.Now()
					snap, err := snapshotter.AcquireReadSnapshot()
					if err != nil {
						return err
					}
					snapshotAcquireCalls.Add(1)
					snapshotAcquireNanos.Add(uint64(time.Since(acquireStarted)))
					nextBuf, getErr := snap.GetAppend(k[:], buf[:0])
					closeStarted := time.Now()
					closeErr := snap.Close()
					snapshotCloseCalls.Add(1)
					snapshotCloseNanos.Add(uint64(time.Since(closeStarted)))
					if closeErr != nil {
						return fmt.Errorf("random_read_parallel_acquire_snapshot close: %w", closeErr)
					}
					if cfg.ReadRequireHit {
						if getErr != nil {
							return getErr
						}
						if len(nextBuf) != cfg.ValueSize {
							return fmt.Errorf("random_read_parallel_acquire_snapshot: value length mismatch: got=%d want=%d", len(nextBuf), cfg.ValueSize)
						}
						buf = nextBuf
						continue
					}
					// Keep parity with random_read/random_read_parallel semantics:
					// this benchmark does not fail on point-read misses.
					if getErr == nil {
						buf = nextBuf
					}
				}
				return nil
			}

			start := time.Now()
			if workers == 1 {
				if err := runWorker(rng, nil); err != nil {
					return 0, fmt.Errorf("random_read_parallel_acquire_snapshot: %w", err)
				}
				recordSnapshotPerf("random_read_parallel_acquire_snapshot", db, treeDBSnapshotPerfMetrics{
					AcquireCalls:      snapshotAcquireCalls.Load(),
					AcquireTotalNanos: snapshotAcquireNanos.Load(),
					CloseCalls:        snapshotCloseCalls.Load(),
					CloseTotalNanos:   snapshotCloseNanos.Load(),
				})
				return float64(cfg.Keys) / time.Since(start).Seconds(), nil
			}

			var stop atomic.Bool
			errCh := make(chan error, 1)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				seedW := baseSeed + int64(w)
				rngW := rand.New(rand.NewSource(seedW))
				wg.Add(1)
				go func(rng *rand.Rand) {
					defer wg.Done()
					if err := runWorker(rng, &stop); err != nil {
						if stop.CompareAndSwap(false, true) {
							select {
							case errCh <- err:
							default:
							}
						}
					}
				}(rngW)
			}
			wg.Wait()

			select {
			case err := <-errCh:
				return 0, fmt.Errorf("random_read_parallel_acquire_snapshot: %w", err)
			default:
			}
			recordSnapshotPerf("random_read_parallel_acquire_snapshot", db, treeDBSnapshotPerfMetrics{
				AcquireCalls:      snapshotAcquireCalls.Load(),
				AcquireTotalNanos: snapshotAcquireNanos.Load(),
				CloseCalls:        snapshotCloseCalls.Load(),
				CloseTotalNanos:   snapshotCloseNanos.Load(),
			})
			totalOps := float64(cfg.Keys) * float64(workers)
			return totalOps / time.Since(start).Seconds(), nil
		},
		"random_read_batch": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			start := time.Now()
			batchSize := cfg.BatchSize
			if batchSize <= 0 {
				batchSize = 1
			}
			keys := make([][]byte, batchSize)
			keyBuf := make([]byte, batchSize*8)
			for i := 0; i < batchSize; i++ {
				keys[i] = keyBuf[i*8 : (i+1)*8]
			}
			mgv, hasManyView := db.(kvstore.MultiGetterView)
			mg, hasMany := db.(kvstore.MultiGetter)
			nextGuard := 0
			for i := 0; i < cfg.Keys; {
				if i >= nextGuard {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
					nextGuard = i + 8192
				}
				n := batchSize
				if remaining := cfg.Keys - i; remaining < n {
					n = remaining
				}
				for j := 0; j < n; j++ {
					encodeKey(keys[j], uint64(rng.Intn(cfg.Keys)))
				}
				if hasManyView {
					var seen atomic.Int64
					err := mgv.GetManyView(keys[:n], func(index int, _ []byte, value []byte, found bool) error {
						seen.Add(1)
						if index < 0 || index >= n {
							return fmt.Errorf("GetManyView callback index %d outside %d keys", index, n)
						}
						if cfg.ReadRequireHit {
							if !found {
								return fmt.Errorf("value missing at index %d", index)
							}
							if len(value) != cfg.ValueSize {
								return fmt.Errorf("value length mismatch: got=%d want=%d", len(value), cfg.ValueSize)
							}
						}
						return nil
					})
					if err != nil {
						return 0, fmt.Errorf("random_read_batch: %w", err)
					}
					if cfg.ReadRequireHit && int(seen.Load()) != n {
						return 0, fmt.Errorf("random_read_batch: GetManyView returned %d callbacks for %d keys", seen.Load(), n)
					}
				} else if hasMany {
					vals, err := mg.GetMany(keys[:n])
					if err != nil {
						return 0, fmt.Errorf("random_read_batch: %w", err)
					}
					if cfg.ReadRequireHit {
						if len(vals) != n {
							return 0, fmt.Errorf("random_read_batch: GetMany returned %d values for %d keys", len(vals), n)
						}
						for j := 0; j < n; j++ {
							if len(vals[j]) != cfg.ValueSize {
								return 0, fmt.Errorf("random_read_batch: value length mismatch: got=%d want=%d", len(vals[j]), cfg.ValueSize)
							}
						}
					}
				} else {
					for j := 0; j < n; j++ {
						val, err := db.Get(keys[j])
						if err != nil {
							return 0, fmt.Errorf("random_read_batch: %w", err)
						}
						if cfg.ReadRequireHit && len(val) != cfg.ValueSize {
							return 0, fmt.Errorf("random_read_batch: value length mismatch: got=%d want=%d", len(val), cfg.ValueSize)
						}
					}
				}
				i += n
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"dataset_read_random": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			if err := ensureWriteDatasets(); err != nil {
				return 0, fmt.Errorf("dataset_read_random: %w", err)
			}

			start := time.Now()
			for i := 0; i < cfg.Keys; i++ {
				if i&8191 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				key := datasetRandomKeys[rng.Intn(len(datasetRandomKeys))]
				_, _ = db.Get(key)
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"full_scan": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			start := time.Now()
			if rs, ok := db.(kvstore.RangeScanner); ok {
				iter, err := rs.Iterator(nil, nil)
				if err != nil {
					return 0, fmt.Errorf("full_scan: iterator: %w", err)
				}
				defer func() { _ = iter.Close() }()

				count := 0
				for iter.Valid() {
					if count&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							return 0, err
						}
					}
					_ = iter.Key()
					_ = iter.Value()
					iter.Next()
					count++
				}
				if err := iter.Error(); err != nil {
					return 0, fmt.Errorf("full_scan: %w", err)
				}
				if expectedFullScanCount >= 0 && count != expectedFullScanCount {
					return 0, fmt.Errorf("full_scan: expected %d items, got %d", expectedFullScanCount, count)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			if fe, ok := db.(kvstore.ForEacher); ok {
				count := 0
				if err := fe.ForEach(func(_ []byte, _ []byte) error {
					count++
					if count&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					return 0, fmt.Errorf("full_scan: foreach: %w", err)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			return math.NaN(), nil
		},
		"full_scan2": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			start := time.Now()
			if rs, ok := db.(kvstore.RangeScanner); ok {
				iter, err := rs.Iterator(nil, nil)
				if err != nil {
					return 0, fmt.Errorf("full_scan2: iterator: %w", err)
				}
				defer func() { _ = iter.Close() }()

				count := 0
				for iter.Valid() {
					if count&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							return 0, err
						}
					}
					_ = iter.Key()
					_ = iter.Value()
					iter.Next()
					count++
				}
				if err := iter.Error(); err != nil {
					return 0, fmt.Errorf("full_scan2: %w", err)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			if fe, ok := db.(kvstore.ForEacher); ok {
				count := 0
				if err := fe.ForEach(func(_ []byte, _ []byte) error {
					count++
					if count&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					return 0, fmt.Errorf("full_scan2: foreach: %w", err)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			return math.NaN(), nil
		},
		"prefix_scan": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			rs, ok := db.(kvstore.RangeScanner)
			if !ok {
				return math.NaN(), nil
			}
			start := time.Now()
			var totalBuild time.Duration
			var totalIter time.Duration
			var debugQueueSum int
			var debugSourcesSum int
			var debugStatsCount int
			totalItems := 0
			maxKey := prefixScanBase + cfg.Keys
			for i := 0; i < cfg.RangeQueries; i++ {
				if i&1023 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				startIdx := prefixScanBase + rng.Intn(cfg.Keys)
				endIdx := startIdx + cfg.RangeSpan
				if endIdx > maxKey {
					endIdx = maxKey
				}

				var startKeyBuf [8]byte
				encodeKey(startKeyBuf[:], uint64(startIdx))
				startKey := startKeyBuf[:]

				var endKeyBuf [8]byte
				encodeKey(endKeyBuf[:], uint64(endIdx))
				endKey := endKeyBuf[:]

				buildStart := time.Now()
				iter, err := rs.Iterator(startKey, endKey)
				if err != nil {
					return 0, fmt.Errorf("prefix_scan: iterator: %w", err)
				}
				buildDur := time.Since(buildStart)
				totalBuild += buildDur

				expected := endIdx - startIdx
				itemsThisQuery := 0
				iterStart := time.Now()
				for iter.Valid() {
					if itemsThisQuery&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							_ = iter.Close()
							return 0, err
						}
					}
					_ = iter.Key()
					iter.Next()
					itemsThisQuery++
				}
				iterDur := time.Since(iterStart)
				totalIter += iterDur
				if err := iter.Error(); err != nil {
					_ = iter.Close()
					return 0, fmt.Errorf("prefix_scan: %w", err)
				}
				if checkPrefixCounts && itemsThisQuery != expected {
					_ = iter.Close()
					return 0, fmt.Errorf("prefix_scan: expected %d items in query %d, got %d", expected, i, itemsThisQuery)
				}
				totalItems += itemsThisQuery

				type debugStats interface {
					DebugStats() (queueLen int, sourcesUsed int)
				}
				if ds, ok := iter.(debugStats); ok {
					q, s := ds.DebugStats()
					debugQueueSum += q
					debugSourcesSum += s
					debugStatsCount++
					if cfg.TreeDBIterDebug && i < cfg.TreeDBIterDebugLimit {
						fmt.Fprintf(os.Stderr, "prefix_scan/%s query=%d items=%d build=%s iter=%s queue=%d sources=%d\n",
							db.Name(), i, itemsThisQuery, buildDur, iterDur, q, s)
					}
				} else if cfg.TreeDBIterDebug && i < cfg.TreeDBIterDebugLimit {
					fmt.Fprintf(os.Stderr, "prefix_scan/%s query=%d items=%d build=%s iter=%s\n",
						db.Name(), i, itemsThisQuery, buildDur, iterDur)
				}

				_ = iter.Close()
			}
			if cfg.TreeDBIterDebug && cfg.RangeQueries > 0 {
				avgBuild := totalBuild / time.Duration(cfg.RangeQueries)
				avgIter := totalIter / time.Duration(cfg.RangeQueries)
				if debugStatsCount > 0 {
					fmt.Fprintf(os.Stderr, "prefix_scan/%s summary queries=%d span=%d items=%d build_avg=%s iter_avg=%s queue_avg=%.2f sources_avg=%.2f\n",
						db.Name(), cfg.RangeQueries, cfg.RangeSpan, totalItems, avgBuild, avgIter,
						float64(debugQueueSum)/float64(debugStatsCount),
						float64(debugSourcesSum)/float64(debugStatsCount))
				} else {
					fmt.Fprintf(os.Stderr, "prefix_scan/%s summary queries=%d span=%d items=%d build_avg=%s iter_avg=%s\n",
						db.Name(), cfg.RangeQueries, cfg.RangeSpan, totalItems, avgBuild, avgIter)
				}
			}
			return float64(totalItems) / time.Since(start).Seconds(), nil
		},
		"prefix_scan2": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			rs, ok := db.(kvstore.RangeScanner)
			if !ok {
				return math.NaN(), nil
			}
			start := time.Now()
			totalItems := 0
			maxKey := prefixScanBase + cfg.Keys
			for i := 0; i < cfg.RangeQueries; i++ {
				if i&1023 == 0 {
					if err := guard.Checkpoint(); err != nil {
						return 0, err
					}
				}
				startIdx := prefixScanBase + rng.Intn(cfg.Keys)
				endIdx := startIdx + cfg.RangeSpan
				if endIdx > maxKey {
					endIdx = maxKey
				}

				var startKeyBuf [8]byte
				encodeKey(startKeyBuf[:], uint64(startIdx))
				startKey := startKeyBuf[:]

				var endKeyBuf [8]byte
				encodeKey(endKeyBuf[:], uint64(endIdx))
				endKey := endKeyBuf[:]

				iter, err := rs.Iterator(startKey, endKey)
				if err != nil {
					return 0, fmt.Errorf("prefix_scan2: iterator: %w", err)
				}

				itemsThisQuery := 0
				for iter.Valid() {
					if itemsThisQuery&8191 == 0 {
						if err := guard.Checkpoint(); err != nil {
							_ = iter.Close()
							return 0, err
						}
					}
					_ = iter.Key()
					_ = iter.Value()
					iter.Next()
					itemsThisQuery++
				}
				if err := iter.Error(); err != nil {
					_ = iter.Close()
					return 0, fmt.Errorf("prefix_scan2: %w", err)
				}
				if err := iter.Close(); err != nil {
					return 0, fmt.Errorf("prefix_scan2: close: %w", err)
				}

				totalItems += itemsThisQuery
			}
			return float64(totalItems) / time.Since(start).Seconds(), nil
		},
	}

	// Keep destructive opt-in workloads that drain the dense keyspace out of the
	// default `all` suite so later read/scan tests do not measure depleted data.
	allTestOrder := []string{"sequential_write", "random_write", "dataset_write_random", "dataset_write_sorted", "batch_write", "batch_write_steady", "batch_random", "batch_delete", "batch_small_seq", "random_delete", "random_read", "random_read_parallel", "random_read_parallel_acquire_snapshot", "random_read_batch", "full_scan", "prefix_scan"}
	displayNames := map[string]string{
		"vacuum_index":                          "VACUUM (Index)",
		"fragmentation_report_pre":              "Fragmentation Report (Pre-Settle)",
		"fragmentation_report_post":             "Fragmentation Report (Post-Settle)",
		"sequential_write":                      "Sequential Write",
		"random_write":                          "Random Write",
		"random_write_parallel":                 "Random Write (Parallel)",
		"dataset_write_random":                  "Dataset Write (Random)",
		"dataset_write_sorted":                  "Dataset Write (Sorted)",
		"random_read":                           "Random Read",
		"random_read_parallel":                  "Random Read (Parallel)",
		"random_read_parallel_acquire_snapshot": "Random Read (Parallel, Snapshot Per Key)",
		"random_read_batch":                     "Random Read (Batch)",
		"full_scan":                             "Full Scan",
		"full_scan2":                            "Full Scan (After VACUUM)",
		"prefix_scan":                           "Prefix Scan",
		"prefix_scan2":                          "Prefix Scan (After VACUUM)",
		"batch_write":                           "Batch Write",
		"batch_write_steady":                    "Batch Write (Steady)",
		"batch_random":                          "Batch Random",
		"batch_delete":                          "Batch Delete",
		"batch_delete_range":                    "Batch DeleteRange (ranges/s)",
		"batch_small_seq":                       "Batch Small Seq",
		"update_fork_choice":                    "Update ForkChoice (Batch CommitSync)",
		"dataset_update_fork_choice":            "Update ForkChoice (Dataset Keys)",
		"random_delete":                         "Random Delete",
		"dataset_read_random":                   "Random Read (Dataset Keys)",
	}

	finalTestOrder := make([]string, 0)
	if contains(testsToRun, "all") {
		finalTestOrder = append(finalTestOrder, allTestOrder...)
	}
	for _, t := range testsToRun {
		if t == "" || t == "all" {
			continue
		}
		if _, ok := testFuncs[t]; !ok {
			return BenchRun{}, fmt.Errorf("unknown test: %q", t)
		}
		if contains(finalTestOrder, t) {
			continue
		}
		finalTestOrder = append(finalTestOrder, t)
	}
	if len(finalTestOrder) == 0 {
		return BenchRun{}, fmt.Errorf("no tests selected")
	}
	if err := validateCheckpointSettleBeforeTests(cfg, finalTestOrder); err != nil {
		return BenchRun{}, err
	}

	maxEncodedKey := uint64(cfg.Keys)
	setMaxEncoded := func(v uint64) {
		if v > maxEncodedKey {
			maxEncodedKey = v
		}
	}
	mulUint64Cap := func(v uint64, factor uint64) uint64 {
		if factor > 0 && v > math.MaxUint64/factor {
			return math.MaxUint64
		}
		return v * factor
	}
	if containsAny(finalTestOrder, "random_write", "random_write_parallel", "batch_random", "update_fork_choice") {
		setMaxEncoded(mulUint64Cap(uint64(cfg.Keys), 10))
	}
	if containsAny(finalTestOrder, "batch_write", "batch_write_steady", "prefix_scan", "prefix_scan2") {
		setMaxEncoded(mulUint64Cap(uint64(cfg.Keys), 2))
	}
	if err := keyShape.validate(maxEncodedKey); err != nil {
		return BenchRun{}, err
	}

	// If the user selects only read/scan/delete tests, the DBs are empty unless we
	// preload a baseline dataset. We intentionally keep this setup out of the
	// per-test timings so that read/scan numbers reflect a populated DB.
	hasMeasuredWrites := containsAny(finalTestOrder,
		"sequential_write",
		"random_write",
		"random_write_parallel",
		"dataset_write_random",
		"dataset_write_sorted",
		"batch_write",
		"batch_write_steady",
		"batch_random",
		"batch_small_seq",
	)
	needsExistingData := containsAny(finalTestOrder,
		"random_read",
		"random_read_parallel",
		"random_read_parallel_acquire_snapshot",
		"random_read_batch",
		"random_delete",
		"batch_delete",
		"full_scan",
		"prefix_scan",
	)
	preloadedOnly := needsExistingData && !hasMeasuredWrites
	if preloadedOnly {
		values, err := getWriteValuePool()
		if err != nil {
			return BenchRun{}, fmt.Errorf("preload values: %w", err)
		}
		var k [8]byte
		for _, inst := range instances {
			for i := 0; i < cfg.Keys; i++ {
				encodeKey(k[:], uint64(i))
				value := values[i%len(values)]
				if err := inst.Wrapper.Set(k[:], value); err != nil {
					return BenchRun{}, fmt.Errorf("preload/%s: %w", inst.Wrapper.Name(), err)
				}
			}
		}
	}

	if preloadedOnly {
		expectedFullScanCount = cfg.Keys
	}

	if containsAny(finalTestOrder, "batch_write", "batch_write_steady") && !contains(finalTestOrder, "sequential_write") && !contains(finalTestOrder, "random_write") && !contains(finalTestOrder, "dataset_write_random") && !contains(finalTestOrder, "dataset_write_sorted") {
		prefixScanBase = cfg.Keys
	}

	// Materialize reusable dataset fixtures before profiling starts. The
	// dataset_write_* timers already exclude this setup; keeping it outside CPU,
	// allocation, contention, and trace profiles makes those artifacts represent
	// the DB hot path instead of fixture generation.
	if containsAny(finalTestOrder, "dataset_write_random", "dataset_write_sorted", "dataset_read_random") {
		if err := ensureWriteDatasets(); err != nil {
			return BenchRun{}, fmt.Errorf("dataset fixture setup: %w", err)
		}
	}

	settledBeforeScans := false
	blockProfilePath := strings.TrimSpace(cfg.BlockProfile)
	mutexProfilePath := strings.TrimSpace(cfg.MutexProfile)
	traceProfilePath := strings.TrimSpace(cfg.TraceProfile)

	if blockProfilePath != "" {
		rate := cfg.BlockProfileRate
		if rate <= 0 {
			rate = 1
		}
		f, err := os.Create(blockProfilePath)
		if err != nil {
			return BenchRun{}, fmt.Errorf("blockprofile: %w", err)
		}
		runtime.SetBlockProfileRate(rate)
		defer func() {
			runtime.SetBlockProfileRate(0)
			if prof := pprof.Lookup("block"); prof != nil {
				_ = prof.WriteTo(f, 0)
			}
			_ = f.Close()
		}()
	}

	if mutexProfilePath != "" {
		frac := cfg.MutexProfileFraction
		if frac <= 0 {
			frac = 1
		}
		f, err := os.Create(mutexProfilePath)
		if err != nil {
			return BenchRun{}, fmt.Errorf("mutexprofile: %w", err)
		}
		prevFrac := runtime.SetMutexProfileFraction(frac)
		defer func() {
			runtime.SetMutexProfileFraction(0)
			if prof := pprof.Lookup("mutex"); prof != nil {
				_ = prof.WriteTo(f, 0)
			}
			_ = f.Close()
			runtime.SetMutexProfileFraction(prevFrac)
		}()
	}

	if traceProfilePath != "" {
		f, err := os.Create(traceProfilePath)
		if err != nil {
			return BenchRun{}, fmt.Errorf("trace: %w", err)
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			return BenchRun{}, fmt.Errorf("trace start: %w", err)
		}
		defer func() {
			trace.Stop()
			_ = f.Close()
		}()
	}

	// Run Tests
	results := make(map[string]map[string]float64)
	for _, testName := range finalTestOrder {
		results[testName] = make(map[string]float64)
		for _, inst := range instances {
			results[testName][inst.Wrapper.Name()] = math.NaN()
		}
	}

	// For live progress table
	liveTbl := newLiveTable(os.Stderr, instances, finalTestOrder, displayNames)

	for _, testName := range finalTestOrder {
		fn := testFuncs[testName]
		seed := testSeed(cfg.SeedUsed, testName)

		if cfg.SettleBeforeScans && !settledBeforeScans && isSettleBeforeScanTest(testName) {
			fmt.Fprintf(os.Stderr, "Settling DBs (Close/Open) before %s...\n", testName)
			if err := settleBenchInstances(instances); err != nil {
				return BenchRun{}, err
			}
			settledBeforeScans = true
		}

		if cfg.TreeDBCacheStatsBeforeReads && containsAny([]string{testName}, "random_read", "random_read_parallel", "random_read_parallel_acquire_snapshot", "random_read_batch", "dataset_read_random", "full_scan", "prefix_scan", "full_scan2", "prefix_scan2") {
			for _, inst := range instances {
				printTreeDBCacheStats(os.Stderr, inst, "pre-"+testName+" treedb.cache")
			}
		}

		if cfg.CheckpointBetweenTests {
			// Checkpoint before starting the new test across all DBs
			// to reduce interference from background flushes of the previous test.
			// We record the time it takes.
			chkMap := make(map[string]time.Duration)
			for _, inst := range instances {
				cp, ok := inst.Wrapper.(checkpointer)
				if !ok {
					continue
				}

				if shouldSettleBeforeCheckpoint(cfg, testName) {
					if err := guard.Checkpoint(); err != nil {
						return BenchRun{}, err
					}
					settleTimeout := checkpointSettleTimeoutWithGuard(cfg.CheckpointSettleTimeout, guard)
					settleDur, settled, settleErr := waitForTreeDBQueueDrainInstance(inst, settleTimeout)
					if settleErr != nil {
						return BenchRun{}, fmt.Errorf("settle %s before checkpoint %s: %w", inst.Name, testName, settleErr)
					}
					if settled {
						recordDuration(checkpointSettleDurations, testName, inst.Wrapper.Name(), settleDur)
					}
					if err := guard.Checkpoint(); err != nil {
						return BenchRun{}, err
					}
				}

				var (
					checkpointCPUFile *os.File
					err               error
				)
				if shouldCheckpointCPUProfile(cfg, testName) {
					checkpointCPUFile, err = startCheckpointCPUProfile(cfg, profileHooks, testName, inst.Wrapper.Name())
					if err != nil {
						return BenchRun{}, fmt.Errorf("checkpoint %s before %s profiling: %w", inst.Name, testName, err)
					}
				}

				start := time.Now()
				checkpointErr := cp.Checkpoint()

				if checkpointCPUFile != nil {
					profileHooks.stopCPUProfile()
					_ = checkpointCPUFile.Close()
				}

				if checkpointErr != nil {
					return BenchRun{}, fmt.Errorf("checkpoint %s before %s: %w", inst.Name, testName, checkpointErr)
				}
				chkMap[inst.Wrapper.Name()] = time.Since(start)
				recordCheckpointTreeDBStats(checkpointTreeDBStats, testName, inst.Wrapper)
			}
			checkpointDurations[testName] = chkMap

			if cfg.VacuumBetweenTests {
				vacMap := make(map[string]time.Duration)
				bytesMap := make(map[string][2]uint64)
				for _, inst := range instances {
					vac, ok := inst.Wrapper.(vacuumIndexOnline)
					if !ok {
						continue
					}

					// Best-effort index.db size reporting (primarily for TreeDB).
					var before uint64
					// TreeDB stores its main index under "maindb/index.db" within the
					// per-DB temp root directory.
					indexPath := filepath.Join(inst.Dir, "maindb", "index.db")
					if st, err := os.Stat(indexPath); err == nil {
						before = uint64(st.Size())
					}

					start := time.Now()
					if err := vac.VacuumIndexOnline(context.Background()); err != nil {
						return BenchRun{}, fmt.Errorf("vacuum %s before %s: %w", inst.Name, testName, err)
					}
					vacMap[inst.Wrapper.Name()] = time.Since(start)

					var after uint64
					if st, err := os.Stat(indexPath); err == nil {
						after = uint64(st.Size())
					}
					if before != 0 || after != 0 {
						bytesMap[inst.Wrapper.Name()] = [2]uint64{before, after}
					}
				}
				if len(vacMap) > 0 {
					vacuumDurations[testName] = vacMap
				}
				if len(bytesMap) > 0 {
					vacuumIndexBytes[testName] = bytesMap
				}
			}

			// For read tests we want the post-checkpoint state to reflect settled
			// persisted-tree reads rather than transient queued memtables.
			if isSettleBeforeScanTest(testName) {
				if err := waitForTreeDBQueueDrain(instances, 2*time.Second); err != nil {
					return BenchRun{}, err
				}
			}
		}

		if err := liveTbl.Render(results); err != nil {
			// ignore output errors
		}

		for _, inst := range instances {
			if err := guard.Checkpoint(); err != nil {
				return BenchRun{}, err
			}
			if testName == "batch_delete_range" {
				if err := prepareBatchDeleteRange(inst.Wrapper); err != nil {
					return BenchRun{}, fmt.Errorf("prepare %s on %s: %w", testName, inst.Name, err)
				}
			}

			var treeStatsBefore treeDBSelectedStats
			if isTreeDBInstance(inst) {
				treeStatsBefore = snapshotSelectedTreeDBStats(inst.Wrapper)
			}

			// Create a fresh PRNG for each DB so they get the same sequence
			rng := rand.New(rand.NewSource(seed))

			// Run
			// CPU profile if enabled (only for single key count).
			// batch_write_steady starts and stops the profile inside runBatchWrite
			// so fixture/key setup and optional dictionary warmup cannot dilute the
			// CPU service ratio for its timed steady-state span.
			var (
				cpuFile           *os.File
				cpuProfilePath    string
				cpuProfileStarted bool
				cpuProfileStopped bool
			)
			startCPUProfile := func() error {
				if cpuFile == nil || cpuProfileStarted {
					return nil
				}
				if err := profileHooks.startCPUProfile(cpuFile); err != nil {
					return err
				}
				cpuProfileStarted = true
				return nil
			}
			stopCPUProfile := func() {
				if !cpuProfileStarted || cpuProfileStopped {
					return
				}
				profileHooks.stopCPUProfile()
				cpuProfileStopped = true
			}
			finishCPUProfile := func() {
				stopCPUProfile()
				if cpuFile == nil {
					return
				}
				_ = cpuFile.Close()
				if !cpuProfileStarted {
					_ = os.Remove(cpuProfilePath)
				}
				cpuFile = nil
			}
			batchWriteSteadyCPUProfileStart = nil
			batchWriteSteadyCPUProfileStop = nil
			if shouldCPUProfile(cfg, testName) {
				cpuProfilePath = strings.TrimSpace(cfg.CPUProfile) + "_" + testName + "_" + inst.Name + ".pprof"
				f, err := os.Create(cpuProfilePath)
				if err != nil {
					return BenchRun{}, fmt.Errorf("cpuprofile %s: %w", cpuProfilePath, err)
				}
				cpuFile = f
				if testName == "batch_write_steady" {
					batchWriteSteadyCPUProfileStart = func() error {
						if err := startCPUProfile(); err != nil {
							return fmt.Errorf("cpuprofile start %s: %w", cpuProfilePath, err)
						}
						return nil
					}
					batchWriteSteadyCPUProfileStop = stopCPUProfile
				} else if err := startCPUProfile(); err != nil {
					finishCPUProfile()
					return BenchRun{}, fmt.Errorf("cpuprofile start %s: %w", cpuProfilePath, err)
				}
			}

			allocBasePath := ""
			if shouldAllocsProfile(cfg, testName) {
				allocBasePath, err = profileHooks.writeAllocsSnapshotTemp("unified_bench_allocs_base")
				if err != nil {
					batchWriteSteadyCPUProfileStart = nil
					batchWriteSteadyCPUProfileStop = nil
					finishCPUProfile()
					return BenchRun{}, fmt.Errorf("allocsprofile baseline %s/%s: %w", testName, inst.Name, err)
				}
			}
			blockBasePath := ""
			if blockProfilePath != "" {
				blockBasePath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_block_base", "block")
				if err != nil {
					batchWriteSteadyCPUProfileStart = nil
					batchWriteSteadyCPUProfileStop = nil
					finishCPUProfile()
					_ = os.Remove(allocBasePath)
					return BenchRun{}, fmt.Errorf("blockprofile baseline %s/%s: %w", testName, inst.Name, err)
				}
			}
			mutexBasePath := ""
			if mutexProfilePath != "" {
				mutexBasePath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_mutex_base", "mutex")
				if err != nil {
					batchWriteSteadyCPUProfileStart = nil
					batchWriteSteadyCPUProfileStop = nil
					finishCPUProfile()
					_ = os.Remove(allocBasePath)
					_ = os.Remove(blockBasePath)
					return BenchRun{}, fmt.Errorf("mutexprofile baseline %s/%s: %w", testName, inst.Name, err)
				}
			}

			opsPerSec, runErr := fn(inst.Wrapper, rng)
			batchWriteSteadyCPUProfileStart = nil
			batchWriteSteadyCPUProfileStop = nil
			finishCPUProfile()

			blockAfterPath := ""
			mutexAfterPath := ""
			if runErr == nil {
				if blockBasePath != "" {
					blockAfterPath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_block_after", "block")
					if err != nil {
						_ = os.Remove(allocBasePath)
						_ = os.Remove(blockBasePath)
						_ = os.Remove(mutexBasePath)
						return BenchRun{}, fmt.Errorf("blockprofile snapshot %s/%s: %w", testName, inst.Name, err)
					}
				}
				if mutexBasePath != "" {
					mutexAfterPath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_mutex_after", "mutex")
					if err != nil {
						_ = os.Remove(allocBasePath)
						_ = os.Remove(blockBasePath)
						_ = os.Remove(blockAfterPath)
						_ = os.Remove(mutexBasePath)
						return BenchRun{}, fmt.Errorf("mutexprofile snapshot %s/%s: %w", testName, inst.Name, err)
					}
				}
			}

			if allocBasePath != "" {
				if runErr != nil {
					_ = os.Remove(allocBasePath)
				} else {
					allocAfterPath, snapErr := profileHooks.writeAllocsSnapshotTemp("unified_bench_allocs_after")
					if snapErr != nil {
						_ = os.Remove(allocBasePath)
						_ = os.Remove(blockBasePath)
						_ = os.Remove(blockAfterPath)
						_ = os.Remove(mutexBasePath)
						_ = os.Remove(mutexAfterPath)
						return BenchRun{}, fmt.Errorf("allocsprofile snapshot %s/%s: %w", testName, inst.Name, snapErr)
					}
					allocPath := strings.TrimSpace(cfg.AllocsProfile) + "_" + testName + "_" + inst.Name + ".pprof"
					deltaErr := profileHooks.writeAllocsDeltaProfile(allocBasePath, allocAfterPath, allocPath)
					_ = os.Remove(allocBasePath)
					_ = os.Remove(allocAfterPath)
					if deltaErr != nil {
						_ = os.Remove(blockBasePath)
						_ = os.Remove(blockAfterPath)
						_ = os.Remove(mutexBasePath)
						_ = os.Remove(mutexAfterPath)
						return BenchRun{}, fmt.Errorf("allocsprofile %s/%s (%s): %w", testName, inst.Name, allocPath, deltaErr)
					}
				}
			}
			if blockBasePath != "" {
				if runErr != nil {
					_ = os.Remove(blockBasePath)
					_ = os.Remove(blockAfterPath)
				} else {
					blockPath := contentionProfilePath(blockProfilePath, "block", testName, inst.Name)
					wrote, deltaErr := profileHooks.writeRuntimeProfileDeltaProfile(blockBasePath, blockAfterPath, blockPath)
					_ = os.Remove(blockBasePath)
					_ = os.Remove(blockAfterPath)
					if deltaErr != nil {
						_ = os.Remove(mutexBasePath)
						_ = os.Remove(mutexAfterPath)
						return BenchRun{}, fmt.Errorf("blockprofile %s/%s (%s): %w", testName, inst.Name, blockPath, deltaErr)
					}
					if !wrote {
						_ = os.Remove(blockPath)
					}
				}
			}
			if mutexBasePath != "" {
				if runErr != nil {
					_ = os.Remove(mutexBasePath)
					_ = os.Remove(mutexAfterPath)
				} else {
					mutexPath := contentionProfilePath(mutexProfilePath, "mutex", testName, inst.Name)
					wrote, deltaErr := profileHooks.writeRuntimeProfileDeltaProfile(mutexBasePath, mutexAfterPath, mutexPath)
					_ = os.Remove(mutexBasePath)
					_ = os.Remove(mutexAfterPath)
					if deltaErr != nil {
						return BenchRun{}, fmt.Errorf("mutexprofile %s/%s (%s): %w", testName, inst.Name, mutexPath, deltaErr)
					}
					if !wrote {
						_ = os.Remove(mutexPath)
					}
				}
			}

			if runErr != nil {
				return BenchRun{}, fmt.Errorf("test %s on %s: %w", testName, inst.Name, runErr)
			}

			if isTreeDBInstance(inst) {
				treeStatsAfter := snapshotSelectedTreeDBStats(inst.Wrapper)
				snapshotPerf := snapshotPerfByTest[testName][inst.Wrapper.Name()]
				metrics := computeTreeDBPerfMetrics(treeStatsBefore, treeStatsAfter, snapshotPerf)
				if !treeDBPerfMetricsEmpty(metrics) {
					perDB := treeDBPerf[testName]
					if perDB == nil {
						perDB = make(map[string]treeDBPerfMetrics)
						treeDBPerf[testName] = perDB
					}
					perDB[inst.Wrapper.Name()] = metrics
				}
			}

			results[testName][inst.Wrapper.Name()] = opsPerSec

			// Update live table cell
			_ = liveTbl.UpdateCell(testName, inst.Wrapper.Name(), opsPerSec)
		}
		if cfg.TreeDBCacheStatsAfterTests {
			for _, inst := range instances {
				printTreeDBCacheStats(os.Stderr, inst, "post-"+testName+" treedb.cache")
			}
		}
	}

	// Final clear of live table
	_ = liveTbl.Clear()

	// If the user requests checkpoints between tests, also checkpoint after the
	// final test so disk usage reflects a settled backend state (especially for
	// front-end ingest benchmarks like TreeDB batch_write).
	if cfg.CheckpointBetweenTests {
		chkMap := make(map[string]time.Duration)
		for _, inst := range instances {
			cp, ok := inst.Wrapper.(checkpointer)
			if !ok {
				continue
			}

			if shouldSettleBeforeCheckpoint(cfg, checkpointPostRunLabel) {
				if err := guard.Checkpoint(); err != nil {
					return BenchRun{}, err
				}
				settleTimeout := checkpointSettleTimeoutWithGuard(cfg.CheckpointSettleTimeout, guard)
				settleDur, settled, settleErr := waitForTreeDBQueueDrainInstance(inst, settleTimeout)
				if settleErr != nil {
					return BenchRun{}, fmt.Errorf("settle %s before checkpoint %s: %w", inst.Name, checkpointPostRunLabel, settleErr)
				}
				if settled {
					recordDuration(checkpointSettleDurations, checkpointPostRunLabel, inst.Wrapper.Name(), settleDur)
				}
				if err := guard.Checkpoint(); err != nil {
					return BenchRun{}, err
				}
			}

			var (
				checkpointCPUFile *os.File
				err               error
			)
			if shouldCheckpointCPUProfile(cfg, checkpointPostRunLabel) {
				checkpointCPUFile, err = startCheckpointCPUProfile(cfg, profileHooks, checkpointPostRunLabel, inst.Wrapper.Name())
				if err != nil {
					return BenchRun{}, fmt.Errorf("checkpoint %s after run profiling: %w", inst.Name, err)
				}
			}

			start := time.Now()
			checkpointErr := cp.Checkpoint()

			if checkpointCPUFile != nil {
				profileHooks.stopCPUProfile()
				_ = checkpointCPUFile.Close()
			}

			if checkpointErr != nil {
				return BenchRun{}, fmt.Errorf("checkpoint %s after run: %w", inst.Name, checkpointErr)
			}
			chkMap[inst.Wrapper.Name()] = time.Since(start)
			recordCheckpointTreeDBStats(checkpointTreeDBStats, checkpointPostRunLabel, inst.Wrapper)
		}
		if len(chkMap) > 0 {
			checkpointDurations[checkpointPostRunLabel] = chkMap
			displayNames[checkpointPostRunLabel] = "After Run"
		}
	}

	// Shutdown
	treedbDisk := make(map[string]treeDBDiskUsage)
	treedbRewrite := make(map[string]treeDBVlogRewriteReport)
	treedbStats := make(map[string]map[string]string)
	diskUsage := make(map[string]dirDiskUsage)
	for _, inst := range instances {
		wrapperName := inst.Wrapper.Name()
		sp, hasStatsProvider := inst.Wrapper.(kvstore.StatsProvider)
		var statsSnapshot map[string]string
		if hasStatsProvider {
			if cp, ok := inst.Wrapper.(checkpointer); ok {
				if err := cp.Checkpoint(); err != nil {
					return BenchRun{}, fmt.Errorf("checkpoint %s before final stats: %w", inst.Name, err)
				}
			}
			statsSnapshot = sp.Stats()
		}
		if err := inst.Wrapper.Close(); err != nil {
			return BenchRun{}, fmt.Errorf("close %s: %w", inst.Name, err)
		}
		if hasStatsProvider && isTreeDBInstance(inst) {
			if postCloseStats := sp.Stats(); len(postCloseStats) > 0 {
				statsSnapshot = postCloseStats
			}
		}
		if isTreeDBInstance(inst) {
			leafStats, err := scanTreeDBLeafVLogCodecStats(inst.Dir, treeDBInstanceCountsAutoVlogCandidates(inst))
			if err != nil {
				log.Printf("scan %s leaf value-log codec stats: %v", inst.Name, err)
			}
			if len(leafStats) > 0 {
				if statsSnapshot == nil {
					statsSnapshot = make(map[string]string, len(leafStats))
				}
				mergeTreeDBLeafVLogCodecStats(statsSnapshot, leafStats)
			}
		}
		if len(statsSnapshot) > 0 {
			copySnap := make(map[string]string, len(statsSnapshot))
			for k, v := range statsSnapshot {
				copySnap[k] = v
			}
			treedbStats[wrapperName] = copySnap
		}
		if cfg.TreeDBVlogRewriteAfterRun && isTreeDBInstance(inst) {
			beforeUsage, _ := computeDirDiskUsage(inst.Dir)
			beforeTree, _ := computeTreeDBDiskUsage(inst.Dir)

			opts, _, err := buildTreeDBOptions(inst.Dir)
			if err != nil {
				return BenchRun{}, err
			}
			db, err := treedb.Open(opts)
			if err != nil {
				return BenchRun{}, fmt.Errorf("open treedb for compact storage: %w", err)
			}
			stats, compactErr := db.CompactStorage(context.Background(), treedb.CompactStorageOptions{
				Mode:          treedb.CompactStorageFull,
				SyncEachPhase: true,
			})
			closeErr := db.Close()
			if compactErr != nil {
				return BenchRun{}, fmt.Errorf("treedb compact storage: %w", compactErr)
			}
			if closeErr != nil {
				return BenchRun{}, fmt.Errorf("close treedb after compact storage: %w", closeErr)
			}

			afterUsage, _ := computeDirDiskUsage(inst.Dir)
			afterTree, _ := computeTreeDBDiskUsage(inst.Dir)
			var afterVacuumUsage dirDiskUsage
			var afterVacuumTree treeDBDiskUsage
			vacuumRan := false
			if *treedbVacuumAfterVlogRewriteRun {
				if err := treedb.VacuumIndexOffline(opts); err != nil {
					return BenchRun{}, err
				}
				vacuumRan = true
				afterVacuumUsage, _ = computeDirDiskUsage(inst.Dir)
				afterVacuumTree, _ = computeTreeDBDiskUsage(inst.Dir)
			}
			treedbRewrite[inst.Wrapper.Name()] = treeDBVlogRewriteReport{
				Dir:             inst.Dir,
				BeforeUsage:     beforeUsage,
				AfterUsage:      afterUsage,
				AfterVacuum:     afterVacuumUsage,
				VacuumRan:       vacuumRan,
				BeforeTree:      beforeTree,
				AfterTree:       afterTree,
				AfterVacuumTree: afterVacuumTree,
				SegmentsBefore:  stats.ValueLogRewrite.SegmentsBefore,
				SegmentsAfter:   stats.ValueLogRewrite.SegmentsAfter,
				BytesBefore:     stats.ValueLogRewrite.BytesBefore,
				BytesAfter:      stats.ValueLogRewrite.BytesAfter,
				RecordsCopied:   stats.ValueLogRewrite.RecordsCopied,
			}
		}
		if usage, err := computeDirDiskUsage(inst.Dir); err == nil {
			if usage.TotalBytes > 0 || usage.TotalFiles > 0 {
				diskUsage[wrapperName] = usage
			}
		}
		if isTreeDBInstance(inst) {
			if usage, err := computeTreeDBDiskUsage(inst.Dir); err == nil {
				if usage.MainIndexBytes > 0 || usage.MainWAL.TotalBytes > 0 || usage.MainValueLog.TotalBytes > 0 || usage.MainLeafLog.TotalBytes > 0 || usage.DictIndexBytes > 0 || usage.DictWAL.TotalBytes > 0 || usage.DictValueLog.TotalBytes > 0 {
					treedbDisk[wrapperName] = usage
				}
			}
		}
		if !cfg.KeepDir {
			_ = os.RemoveAll(inst.Dir)
		}
	}

	return BenchRun{
		Config:                    cfg,
		Instances:                 instances,
		TestOrder:                 finalTestOrder,
		DisplayNames:              displayNames,
		Results:                   results,
		CheckpointDurations:       checkpointDurations,
		CheckpointSettleDurations: checkpointSettleDurations,
		CheckpointTreeDBStats:     checkpointTreeDBStats,
		VacuumDurations:           vacuumDurations,
		VacuumIndexBytes:          vacuumIndexBytes,
		TreeDBDiskUsage:           treedbDisk,
		TreeDBVlogRewrite:         treedbRewrite,
		TreeDBPerf:                treeDBPerf,
		TreeDBStats:               treedbStats,
		DiskUsage:                 diskUsage,
		BatchDeleteRange:          batchDeleteRangeReports,
	}, nil
}

func printFragmentationReport(w io.Writer, phase, dbName string, rep map[string]string) {
	keys := make([]string, 0, len(rep))
	for k := range rep {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Fprintln(w)
	fmt.Fprintf(w, "fragmentation report (%s): %s\n", phase, dbName)
	for _, k := range keys {
		fmt.Fprintf(w, "  %s=%s\n", k, rep[k])
	}
}

func hasInstance(instances []*DBInstance, name string) bool {
	for _, inst := range instances {
		if inst != nil && inst.Name == name {
			return true
		}
	}
	return false
}

const treedbAdapterName = "treedb"
const treedbWrapperName = "TreeDB"

func isTreeDBInstance(inst *DBInstance) bool {
	if inst == nil {
		return false
	}
	if inst.Wrapper == nil {
		return false
	}
	if _, ok := inst.Wrapper.(*treedbadapter.DB); ok {
		return true
	}
	// The adapter/registry name is "treedb", while the display wrapper name is
	// typically "TreeDB". Keep name-based checks as a fallback.
	if inst.Name == treedbAdapterName || strings.HasPrefix(inst.Name, treedbAdapterName+"_") {
		return true
	}
	if inst.Wrapper.Name() == treedbWrapperName || strings.HasPrefix(inst.Wrapper.Name(), treedbWrapperName+" ") {
		return true
	}
	return false
}

func selectedTreeDBVlogCompressionMode(dbName string) (treedb.ValueLogCompressionMode, bool) {
	switch strings.ToLower(strings.TrimSpace(dbName)) {
	case "treedb_vlog_off", "treedb_vlog_dict_off":
		return treedb.ValueLogCompressionOff, true
	case "treedb_vlog_block_snappy", "treedb_vlog_block_lz4", "treedb_vlog_block_zstd":
		return treedb.ValueLogCompressionBlock, true
	case "treedb_vlog_dict":
		return treedb.ValueLogCompressionDict, true
	case "treedb_vlog_dict_on", "treedb_vlog_dict_on_entropy",
		"treedb_vlog_dict_on_level_default", "treedb_vlog_dict_on_level_default_entropy",
		"treedb_vlog_dict_on_level_better", "treedb_vlog_dict_on_level_better_entropy",
		"treedb_vlog_dict_on_level_best", "treedb_vlog_dict_on_level_best_entropy":
		return treedb.ValueLogCompressionDict, true
	case "treedb_vlog_auto":
		return treedb.ValueLogCompressionAuto, true
	case treedbAdapterName:
		mode, _, err := parseTreeDBVlogCompressionMode(*treedbVlogCompression)
		if err == nil {
			return mode, true
		}
	}
	return treedb.ValueLogCompressionOff, false
}

func treeDBInstanceCountsAutoVlogCandidates(inst *DBInstance) bool {
	return inst != nil && inst.TreeDBVlogCompressionModeSet && inst.TreeDBVlogCompressionMode == treedb.ValueLogCompressionAuto
}

func computeWalDiskUsage(dir string) (walDiskUsage, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return walDiskUsage{}, err
	}

	var out walDiskUsage
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		size := info.Size()
		if size < 0 {
			continue
		}

		out.TotalFiles++
		out.TotalBytes += uint64(size)

		name := entry.Name()
		switch {
		case strings.HasPrefix(name, "commit-"):
			out.CommitFiles++
			out.CommitBytes += uint64(size)
		case strings.HasPrefix(name, "wal-"):
			out.WALFiles++
			out.WALBytes += uint64(size)
		case strings.HasPrefix(name, "value-"):
			out.ValueFiles++
			out.ValueBytes += uint64(size)
		case strings.HasPrefix(name, "vlog-"):
			out.VlogFiles++
			out.VlogBytes += uint64(size)
		default:
			out.OtherFiles++
			out.OtherBytes += uint64(size)
		}
	}
	return out, nil
}

func computeDirDiskUsage(rootDir string) (dirDiskUsage, error) {
	var out dirDiskUsage
	if strings.TrimSpace(rootDir) == "" {
		return out, fmt.Errorf("disk usage: empty root dir")
	}
	if err := filepath.WalkDir(rootDir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if sz := info.Size(); sz > 0 {
			out.TotalBytes += uint64(sz)
		}
		out.TotalFiles++
		return nil
	}); err != nil {
		return out, err
	}
	return out, nil
}

func computeTreeDBDiskUsage(rootDir string) (treeDBDiskUsage, error) {
	var out treeDBDiskUsage
	if strings.TrimSpace(rootDir) == "" {
		return out, fmt.Errorf("disk usage: empty root dir")
	}

	mainIndex := filepath.Join(rootDir, "maindb", "index.db")
	if st, err := os.Stat(mainIndex); err == nil {
		if sz := st.Size(); sz > 0 {
			out.MainIndexBytes = uint64(sz)
		}
	}
	mainWAL := filepath.Join(rootDir, "maindb", "wal")
	if u, err := computeWalDiskUsage(mainWAL); err == nil {
		out.MainWAL = u
	}
	mainValueLog := filepath.Join(rootDir, "maindb", "value_vlog")
	if u, err := computeWalDiskUsage(mainValueLog); err == nil {
		out.MainValueLog = u
	}
	mainLeafLog := filepath.Join(rootDir, "maindb", "leaf_vlog")
	if u, err := computeWalDiskUsage(mainLeafLog); err == nil {
		out.MainLeafLog = u
	}

	dictIndex := filepath.Join(rootDir, "dictdb", "index.db")
	if st, err := os.Stat(dictIndex); err == nil {
		if sz := st.Size(); sz > 0 {
			out.DictIndexBytes = uint64(sz)
		}
	}
	dictWAL := filepath.Join(rootDir, "dictdb", "wal")
	if u, err := computeWalDiskUsage(dictWAL); err == nil {
		out.DictWAL = u
	}
	dictValueLog := filepath.Join(rootDir, "dictdb", "value_vlog")
	if u, err := computeWalDiskUsage(dictValueLog); err == nil {
		out.DictValueLog = u
	}

	return out, nil
}

const (
	treeDBVlogScanHeaderSize          = 20
	treeDBVlogScanVersion             = 1
	treeDBVlogScanFrameHeaderSize     = 12
	treeDBVlogScanMaxBodyLen          = 64 << 20
	treeDBVlogScanRecordFlagGrouped   = 1 << 0
	treeDBVlogScanFrameVersion        = 1
	treeDBVlogScanFrameFlagCompressed = 1 << 0
	treeDBVlogScanBlockCodecSnappy    = 1
	treeDBVlogScanBlockCodecLZ4       = 2
	treeDBVlogScanBlockCodecZSTD      = 3
	treeDBVlogScanMaxFrameK           = 128

	treeDBVlogScanOuterLeafCodecHeaderOffset = 5
	treeDBVlogScanOuterLeafCodecNoneID       = 0
	treeDBVlogScanOuterLeafCodecSnappyID     = 1
	treeDBVlogScanOuterLeafCodecLZ4ID        = 2
)

var treeDBVlogScanKBucketUpperBounds = []int{1, 2, 4, 8, 16, 32, 64, treeDBVlogScanMaxFrameK}

type treeDBVlogCodecScanCounters struct {
	Frames      uint64
	Records     uint64
	RawBytes    uint64
	StoredBytes uint64
}

type treeDBVlogCodecScanStats struct {
	WriteModes      map[string]treeDBVlogCodecScanCounters
	PayloadKinds    map[string]treeDBVlogCodecScanCounters
	PayloadSplits   map[string]treeDBVlogCodecScanCounters
	OuterLeafCodecs map[string]treeDBVlogCodecScanCounters
	BlockCodecs     map[string]treeDBVlogCodecScanCounters
	BlockKCount     map[string]uint64
	BlockKSum       map[string]uint64
	BlockKMax       map[string]uint64
	BlockKBuckets   map[string][]uint64
	AutoCandidates  map[string]treeDBVlogCodecScanCounters
}

func newTreeDBVlogCodecScanStats() *treeDBVlogCodecScanStats {
	return &treeDBVlogCodecScanStats{
		WriteModes:      map[string]treeDBVlogCodecScanCounters{},
		PayloadKinds:    map[string]treeDBVlogCodecScanCounters{},
		PayloadSplits:   map[string]treeDBVlogCodecScanCounters{},
		OuterLeafCodecs: map[string]treeDBVlogCodecScanCounters{},
		BlockCodecs:     map[string]treeDBVlogCodecScanCounters{},
		BlockKCount:     map[string]uint64{},
		BlockKSum:       map[string]uint64{},
		BlockKMax:       map[string]uint64{},
		BlockKBuckets:   map[string][]uint64{},
		AutoCandidates:  map[string]treeDBVlogCodecScanCounters{},
	}
}

func hasExistingTreeDBVlogCodecStats(stats map[string]string) bool {
	for key, value := range stats {
		switch {
		case strings.HasPrefix(key, "treedb.cache.vlog_auto.frames."),
			strings.HasPrefix(key, "treedb.cache.vlog_auto.bytes."),
			strings.HasPrefix(key, "treedb.cache.vlog_write_mode."),
			strings.HasPrefix(key, "treedb.cache.vlog_payload_kind."),
			strings.HasPrefix(key, "treedb.cache.vlog_payload_split."),
			strings.HasPrefix(key, "treedb.cache.vlog_outer_leaf_codec."),
			strings.HasPrefix(key, "treedb.cache.vlog_block."):
			if treeDBVlogStatValueHasSignal(value) {
				return true
			}
		}
	}
	return false
}

func mergeTreeDBLeafVLogCodecStats(dst, leafStats map[string]string) {
	if len(dst) == 0 || !hasExistingTreeDBVlogCodecStats(dst) {
		for key, value := range leafStats {
			dst[key] = value
		}
		return
	}
	for key, value := range leafStats {
		dst[treeDBLeafVLogScanStatKey(key)] = value
	}
}

func treeDBLeafVLogScanStatKey(key string) string {
	const cacheVlogPrefix = "treedb.cache.vlog_"
	if strings.HasPrefix(key, cacheVlogPrefix) {
		return "treedb.cache.vlog_leaf_scan." + strings.TrimPrefix(key, cacheVlogPrefix)
	}
	return "treedb.cache.vlog_leaf_scan." + strings.TrimPrefix(key, "treedb.cache.")
}

func scanTreeDBLeafVLogCodecStats(rootDir string, countAuto bool) (map[string]string, error) {
	if strings.TrimSpace(rootDir) == "" {
		return nil, nil
	}
	leafDir := filepath.Join(rootDir, "maindb", "leaf_vlog")
	paths, err := filepath.Glob(filepath.Join(leafDir, "value-l*.log"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}
	sort.Strings(paths)
	scan := newTreeDBVlogCodecScanStats()
	var firstErr error
	for _, path := range paths {
		if err := scanTreeDBVLogCodecStatsFile(path, scan, countAuto); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	return scan.treeDBStats(), firstErr
}

func scanTreeDBVLogCodecStatsFile(path string, scan *treeDBVlogCodecScanStats, countAuto bool) error {
	if scan == nil {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var header [treeDBVlogScanHeaderSize]byte
	for {
		_, err := io.ReadFull(f, header[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if header[4] != treeDBVlogScanVersion {
			return fmt.Errorf("%s: unsupported value-log version %d", path, header[4])
		}
		bodyLen := binary.LittleEndian.Uint32(header[16:20])
		if header[5]&treeDBVlogScanRecordFlagGrouped == 0 {
			if _, err := io.CopyN(io.Discard, f, int64(bodyLen)); err != nil {
				return err
			}
			continue
		}
		if bodyLen < treeDBVlogScanFrameHeaderSize {
			return fmt.Errorf("%s: value-log body too short: %d", path, bodyLen)
		}
		if bodyLen > treeDBVlogScanMaxBodyLen {
			return fmt.Errorf("%s: value-log body too large: %d > %d", path, bodyLen, treeDBVlogScanMaxBodyLen)
		}
		body := make([]byte, int(bodyLen))
		if _, err := io.ReadFull(f, body); err != nil {
			return err
		}
		if err := scan.observeValueLogFrame(body, countAuto); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
}

func (s *treeDBVlogCodecScanStats) observeValueLogFrame(body []byte, countAuto bool) error {
	if s == nil || len(body) == 0 {
		return nil
	}
	if len(body) < treeDBVlogScanFrameHeaderSize {
		return fmt.Errorf("frame body too short: %d", len(body))
	}
	if body[0] != treeDBVlogScanFrameVersion {
		return fmt.Errorf("unsupported frame version %d", body[0])
	}
	flags := body[1]
	k := int(body[2])
	if k <= 0 || k > treeDBVlogScanMaxFrameK {
		return fmt.Errorf("invalid frame K=%d", k)
	}
	prefixLen := treeDBVlogScanFrameHeaderSize + (k * 8) + ((k + 1) * 4)
	if len(body) < prefixLen {
		return fmt.Errorf("frame body length %d shorter than prefix %d", len(body), prefixLen)
	}
	offsetStart := treeDBVlogScanFrameHeaderSize + (k * 8)
	offsets := make([]uint32, k+1)
	var prevOffset uint32
	for i := 0; i <= k; i++ {
		cur := binary.LittleEndian.Uint32(body[offsetStart+(i*4) : offsetStart+((i+1)*4)])
		if i == 0 && cur != 0 {
			return fmt.Errorf("frame first offset=%d want 0", cur)
		}
		if i > 0 && cur < prevOffset {
			return fmt.Errorf("frame offsets are not monotonic at index %d", i)
		}
		offsets[i] = cur
		prevOffset = cur
	}
	rawPayloadBytes := uint64(offsets[k])
	if rawPayloadBytes > treeDBVlogScanMaxBodyLen {
		return fmt.Errorf("frame raw payload too large: %d > %d", rawPayloadBytes, treeDBVlogScanMaxBodyLen)
	}
	storedPayloadBytes := uint64(len(body) - prefixLen)
	dictID := binary.LittleEndian.Uint64(body[4:12])
	compressed := flags&treeDBVlogScanFrameFlagCompressed != 0
	if !compressed && rawPayloadBytes != storedPayloadBytes {
		return fmt.Errorf("uncompressed frame raw/stored size mismatch: raw=%d stored=%d", rawPayloadBytes, storedPayloadBytes)
	}

	writeMode := "off"
	outerCodec := "unknown"
	blockCodec := ""
	autoCandidate := "off"
	if dictID != 0 {
		writeMode = "dict"
		autoCandidate = "dict"
		if !compressed {
			outerCodec = treeDBVlogScanOuterLeafCodecFromPayload(body[prefixLen:], offsets)
		}
	} else if !compressed {
		outerCodec = treeDBVlogScanOuterLeafCodecFromPayload(body[prefixLen:], offsets)
	} else {
		writeMode = "block"
		switch body[3] {
		case treeDBVlogScanBlockCodecLZ4:
			blockCodec = "lz4"
			autoCandidate = "block_lz4"
		case treeDBVlogScanBlockCodecZSTD:
			blockCodec = "zstd"
			autoCandidate = "block_zstd"
		case treeDBVlogScanBlockCodecSnappy:
			blockCodec = "snappy"
			autoCandidate = "block_snappy"
		default:
			autoCandidate = ""
		}
	}

	s.addCounters(s.WriteModes, writeMode, k, rawPayloadBytes, storedPayloadBytes)
	s.addCounters(s.PayloadKinds, "outer_leaf", k, rawPayloadBytes, storedPayloadBytes)
	s.addCounters(s.PayloadSplits, "outer_leaf", k, rawPayloadBytes, storedPayloadBytes)
	s.addCounters(s.OuterLeafCodecs, outerCodec, k, rawPayloadBytes, storedPayloadBytes)
	if blockCodec != "" {
		s.addCounters(s.BlockCodecs, blockCodec, k, rawPayloadBytes, storedPayloadBytes)
	}
	if countAuto && autoCandidate != "" {
		s.addCounters(s.AutoCandidates, autoCandidate, k, rawPayloadBytes, storedPayloadBytes)
	}
	if writeMode == "block" && (blockCodec == "snappy" || blockCodec == "lz4" || blockCodec == "zstd") {
		s.BlockKCount[blockCodec]++
		s.BlockKSum[blockCodec] += uint64(k)
		if uint64(k) > s.BlockKMax[blockCodec] {
			s.BlockKMax[blockCodec] = uint64(k)
		}
		buckets := s.BlockKBuckets[blockCodec]
		if len(buckets) == 0 {
			buckets = make([]uint64, len(treeDBVlogScanKBucketUpperBounds))
		}
		for i, upper := range treeDBVlogScanKBucketUpperBounds {
			if k <= upper {
				buckets[i]++
				break
			}
		}
		s.BlockKBuckets[blockCodec] = buckets
	}
	return nil
}

func treeDBVlogScanOuterLeafCodecFromPayload(payload []byte, offsets []uint32) string {
	if len(offsets) < 2 {
		return "unknown"
	}
	kind := ""
	for i := 0; i+1 < len(offsets); i++ {
		start, end := int(offsets[i]), int(offsets[i+1])
		if start < 0 || end < start || end > len(payload) {
			return "unknown"
		}
		next := treeDBVlogScanOuterLeafCodecFromValue(payload[start:end])
		if kind == "" {
			kind = next
			continue
		}
		if next != kind {
			return "mixed"
		}
	}
	if kind == "" {
		return "unknown"
	}
	return kind
}

func treeDBVlogScanOuterLeafCodecFromValue(value []byte) string {
	if len(value) == 0 {
		return "unknown"
	}
	if len(value) >= 4 && value[0] == 'T' && value[1] == 'O' && value[2] == 'L' && value[3] == '2' {
		if len(value) <= treeDBVlogScanOuterLeafCodecHeaderOffset {
			return "unknown"
		}
		switch value[treeDBVlogScanOuterLeafCodecHeaderOffset] {
		case treeDBVlogScanOuterLeafCodecNoneID:
			return "none"
		case treeDBVlogScanOuterLeafCodecSnappyID:
			return "snappy"
		case treeDBVlogScanOuterLeafCodecLZ4ID:
			return "lz4"
		default:
			return "unknown"
		}
	}
	return "legacy_page"
}

func (s *treeDBVlogCodecScanStats) addCounters(dst map[string]treeDBVlogCodecScanCounters, key string, records int, rawBytes, storedBytes uint64) {
	c := dst[key]
	c.Frames++
	if records > 0 {
		c.Records += uint64(records)
	}
	c.RawBytes += rawBytes
	c.StoredBytes += storedBytes
	dst[key] = c
}

func (s *treeDBVlogCodecScanStats) treeDBStats() map[string]string {
	if s == nil {
		return nil
	}
	out := map[string]string{}
	putCounters := func(prefix string, counters map[string]treeDBVlogCodecScanCounters, includeRecords bool) {
		for name, c := range counters {
			if c.Frames > 0 {
				out[prefix+".frames."+name] = fmt.Sprintf("%d", c.Frames)
			}
			if includeRecords && c.Records > 0 {
				out[prefix+".records."+name] = fmt.Sprintf("%d", c.Records)
			}
			if c.RawBytes > 0 {
				out[prefix+".raw_bytes."+name] = fmt.Sprintf("%d", c.RawBytes)
			}
			if c.StoredBytes > 0 {
				out[prefix+".stored_bytes."+name] = fmt.Sprintf("%d", c.StoredBytes)
			}
			if c.RawBytes > 0 {
				out[prefix+".stored_ratio."+name] = fmt.Sprintf("%.6f", float64(c.StoredBytes)/float64(c.RawBytes))
			}
		}
	}
	putCounters("treedb.cache.vlog_write_mode", s.WriteModes, false)
	putCounters("treedb.cache.vlog_payload_kind", s.PayloadKinds, false)
	putCounters("treedb.cache.vlog_payload_split", s.PayloadSplits, true)
	putCounters("treedb.cache.vlog_outer_leaf_codec", s.OuterLeafCodecs, false)

	totalAutoFrames := uint64(0)
	for _, c := range s.AutoCandidates {
		totalAutoFrames += c.Frames
	}
	for name, c := range s.AutoCandidates {
		if c.Frames > 0 {
			out["treedb.cache.vlog_auto.frames."+name] = fmt.Sprintf("%d", c.Frames)
		}
		if c.RawBytes > 0 {
			out["treedb.cache.vlog_auto.bytes."+name] = fmt.Sprintf("%d", c.RawBytes)
		}
		if totalAutoFrames > 0 && c.Frames > 0 {
			out["treedb.cache.vlog_auto.frames_frac."+name] = fmt.Sprintf("%.6f", float64(c.Frames)/float64(totalAutoFrames))
		}
	}

	for _, codec := range []string{"snappy", "lz4", "zstd"} {
		count := s.BlockKCount[codec]
		if count == 0 {
			continue
		}
		out["treedb.cache.vlog_block.k.count."+codec] = fmt.Sprintf("%d", count)
		out["treedb.cache.vlog_block.k.avg."+codec] = fmt.Sprintf("%.3f", float64(s.BlockKSum[codec])/float64(count))
		out["treedb.cache.vlog_block.k.max."+codec] = fmt.Sprintf("%d", s.BlockKMax[codec])
		if c := s.BlockCodecs[codec]; c.RawBytes > 0 {
			out["treedb.cache.vlog_block.ratio."+codec] = fmt.Sprintf("%.6f", float64(c.StoredBytes)/float64(c.RawBytes))
		}
		buckets := s.BlockKBuckets[codec]
		for i, upper := range treeDBVlogScanKBucketUpperBounds {
			if i < len(buckets) && buckets[i] > 0 {
				out[fmt.Sprintf("treedb.cache.vlog_block.k.bucket.%s.le_%d", codec, upper)] = fmt.Sprintf("%d", buckets[i])
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func renderTreeDBDiskUsageString(usage map[string]treeDBDiskUsage) string {
	if len(usage) == 0 {
		return ""
	}

	names := make([]string, 0, len(usage))
	for name := range usage {
		names = append(names, name)
	}
	sort.Strings(names)

	walLine := func(prefix string, u walDiskUsage) string {
		parts := []string{
			fmt.Sprintf("total=%s", formatBytes(u.TotalBytes)),
			fmt.Sprintf("files=%d", u.TotalFiles),
		}
		if u.CommitBytes > 0 {
			parts = append(parts, fmt.Sprintf("commit=%s", formatBytes(u.CommitBytes)))
		}
		if u.WALBytes > 0 {
			parts = append(parts, fmt.Sprintf("wal=%s", formatBytes(u.WALBytes)))
		}
		if u.ValueBytes > 0 {
			parts = append(parts, fmt.Sprintf("value=%s", formatBytes(u.ValueBytes)))
		}
		if u.VlogBytes > 0 {
			parts = append(parts, fmt.Sprintf("vlog=%s", formatBytes(u.VlogBytes)))
		}
		if u.OtherBytes > 0 {
			parts = append(parts, fmt.Sprintf("other=%s", formatBytes(u.OtherBytes)))
		}
		return prefix + strings.Join(parts, " ")
	}

	var sb strings.Builder
	for i, name := range names {
		if i > 0 {
			sb.WriteByte('\n')
		}
		u := usage[name]
		sb.WriteString(name)
		sb.WriteString(":\n")
		if u.MainIndexBytes > 0 {
			sb.WriteString(fmt.Sprintf("  maindb/index.db: %s\n", formatBytes(u.MainIndexBytes)))
		}
		if u.MainWAL.TotalFiles > 0 || u.MainWAL.TotalBytes > 0 {
			sb.WriteString(walLine("  maindb/wal: ", u.MainWAL))
			sb.WriteByte('\n')
		}
		if u.MainValueLog.TotalFiles > 0 || u.MainValueLog.TotalBytes > 0 {
			sb.WriteString(walLine("  maindb/value_vlog: ", u.MainValueLog))
			sb.WriteByte('\n')
		}
		if u.MainLeafLog.TotalFiles > 0 || u.MainLeafLog.TotalBytes > 0 {
			sb.WriteString(walLine("  maindb/leaf_vlog: ", u.MainLeafLog))
			sb.WriteByte('\n')
		}
		if u.DictIndexBytes > 0 {
			sb.WriteString(fmt.Sprintf("  dictdb/index.db: %s\n", formatBytes(u.DictIndexBytes)))
		}
		if u.DictWAL.TotalFiles > 0 || u.DictWAL.TotalBytes > 0 {
			sb.WriteString(walLine("  dictdb/wal: ", u.DictWAL))
			sb.WriteByte('\n')
		}
		if u.DictValueLog.TotalFiles > 0 || u.DictValueLog.TotalBytes > 0 {
			sb.WriteString(walLine("  dictdb/value_vlog: ", u.DictValueLog))
			sb.WriteByte('\n')
		}
	}
	return sb.String()
}

func renderTreeDBVlogRewriteString(reports map[string]treeDBVlogRewriteReport) string {
	if len(reports) == 0 {
		return ""
	}

	names := make([]string, 0, len(reports))
	for name := range reports {
		names = append(names, name)
	}
	sort.Strings(names)

	formatBytesSigned := func(v int64) string {
		if v <= 0 {
			return "0 B"
		}
		return formatBytes(uint64(v))
	}

	var sb strings.Builder
	for i, name := range names {
		if i > 0 {
			sb.WriteByte('\n')
		}
		rep := reports[name]
		sb.WriteString(name)
		sb.WriteString(":\n")
		if strings.TrimSpace(rep.Dir) != "" {
			sb.WriteString(fmt.Sprintf("  dir: %s\n", rep.Dir))
		}
		sb.WriteString(fmt.Sprintf("  bytes: %s -> %s", formatBytes(rep.BeforeUsage.TotalBytes), formatBytes(rep.AfterUsage.TotalBytes)))
		if rep.VacuumRan {
			sb.WriteString(fmt.Sprintf(" -> %s after index vacuum", formatBytes(rep.AfterVacuum.TotalBytes)))
		}
		sb.WriteByte('\n')
		if rep.BeforeTree.MainIndexBytes > 0 || rep.AfterTree.MainIndexBytes > 0 {
			sb.WriteString(fmt.Sprintf("  maindb/index.db: %s -> %s\n", formatBytes(rep.BeforeTree.MainIndexBytes), formatBytes(rep.AfterTree.MainIndexBytes)))
		}
		if rep.VacuumRan && rep.AfterVacuumTree.MainIndexBytes != rep.AfterTree.MainIndexBytes {
			sb.WriteString(fmt.Sprintf("  maindb/index.db after vacuum: %s\n", formatBytes(rep.AfterVacuumTree.MainIndexBytes)))
		}
		if rep.BeforeTree.MainWAL.TotalBytes > 0 || rep.AfterTree.MainWAL.TotalBytes > 0 {
			sb.WriteString(fmt.Sprintf("  maindb/wal: %s -> %s\n", formatBytes(rep.BeforeTree.MainWAL.TotalBytes), formatBytes(rep.AfterTree.MainWAL.TotalBytes)))
		}
		if rep.BeforeTree.MainValueLog.TotalBytes > 0 || rep.AfterTree.MainValueLog.TotalBytes > 0 {
			sb.WriteString(fmt.Sprintf("  maindb/value_vlog: %s -> %s\n", formatBytes(rep.BeforeTree.MainValueLog.TotalBytes), formatBytes(rep.AfterTree.MainValueLog.TotalBytes)))
		}
		if rep.BeforeTree.MainLeafLog.TotalBytes > 0 || rep.AfterTree.MainLeafLog.TotalBytes > 0 {
			sb.WriteString(fmt.Sprintf("  maindb/leaf_vlog: %s -> %s\n", formatBytes(rep.BeforeTree.MainLeafLog.TotalBytes), formatBytes(rep.AfterTree.MainLeafLog.TotalBytes)))
		}
		sb.WriteString(fmt.Sprintf("  compact-storage value-log rewrite: segments %d -> %d bytes %s -> %s records=%d\n",
			rep.SegmentsBefore, rep.SegmentsAfter,
			formatBytesSigned(rep.BytesBefore), formatBytesSigned(rep.BytesAfter),
			rep.RecordsCopied))
	}
	return sb.String()
}

func renderDirDiskUsageString(usage map[string]dirDiskUsage) string {
	if len(usage) == 0 {
		return ""
	}
	names := make([]string, 0, len(usage))
	for name := range usage {
		names = append(names, name)
	}
	sort.Strings(names)

	var sb strings.Builder
	for i, name := range names {
		if i > 0 {
			sb.WriteByte('\n')
		}
		u := usage[name]
		sb.WriteString(fmt.Sprintf("%s: total=%s files=%d", name, formatBytes(u.TotalBytes), u.TotalFiles))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func renderNonTreeDBDiskUsageString(usage map[string]dirDiskUsage, treedbUsage map[string]treeDBDiskUsage) string {
	if len(usage) == 0 {
		return ""
	}

	names := make([]string, 0, len(usage))
	for name, u := range usage {
		if treedbUsage != nil {
			if _, ok := treedbUsage[name]; ok {
				continue
			}
		}
		if u.TotalBytes == 0 && u.TotalFiles == 0 {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)

	var sb strings.Builder
	for i, name := range names {
		if i > 0 {
			sb.WriteByte('\n')
		}
		u := usage[name]
		sb.WriteString(fmt.Sprintf("%s: total=%s files=%d", name, formatBytes(u.TotalBytes), u.TotalFiles))
		sb.WriteByte('\n')
	}
	return sb.String()
}

type keptDirEntry struct {
	name string
	dir  string
}

func renderKeptDirsString(instances []*DBInstance) string {
	if len(instances) == 0 {
		return ""
	}
	rows := make([]keptDirEntry, 0, len(instances))
	for _, inst := range instances {
		if inst == nil {
			continue
		}
		dir := strings.TrimSpace(inst.Dir)
		if dir == "" {
			continue
		}
		name := strings.TrimSpace(inst.Name)
		if inst.Wrapper != nil {
			if wrapperName := strings.TrimSpace(inst.Wrapper.Name()); wrapperName != "" {
				name = wrapperName
			}
		}
		if name == "" {
			name = "(unnamed)"
		}
		rows = append(rows, keptDirEntry{name: name, dir: dir})
	}
	if len(rows) == 0 {
		return ""
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].name == rows[j].name {
			return rows[i].dir < rows[j].dir
		}
		return rows[i].name < rows[j].name
	})
	var sb strings.Builder
	for _, row := range rows {
		sb.WriteString(row.name)
		sb.WriteString(": ")
		sb.WriteString(row.dir)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func renderMarkdownSingle(run BenchRun) string {
	var sb strings.Builder
	sb.WriteString("# unified_bench\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	if strings.TrimSpace(run.Config.KeyShape) != "" {
		sb.WriteString(fmt.Sprintf("- key-shape: %s\n", strings.TrimSpace(run.Config.KeyShape)))
	}
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", run.Config.BatchSize))
	sb.WriteString(fmt.Sprintf("- range-queries: %d\n", run.Config.RangeQueries))
	sb.WriteString(fmt.Sprintf("- range-span: %d\n", run.Config.RangeSpan))
	if contains(run.TestOrder, "batch_delete_range") {
		sb.WriteString(fmt.Sprintf("- batch-delete-range-width: %d\n", run.Config.BatchDeleteRangeWidth))
		sb.WriteString(fmt.Sprintf("- batch-delete-ranges-per-batch: %d\n", run.Config.BatchDeleteRangesPerBatch))
		sb.WriteString(fmt.Sprintf("- batch-delete-range-validate: %t\n", run.Config.BatchDeleteRangeValidate))
		sb.WriteString(fmt.Sprintf("- batch-delete-range-refill: %t\n", run.Config.BatchDeleteRangeRefill))
	}
	if strings.TrimSpace(run.Config.ValuePattern) != "" {
		sb.WriteString(fmt.Sprintf("- val-pattern: %s\n", strings.TrimSpace(run.Config.ValuePattern)))
		sb.WriteString(fmt.Sprintf("- val-pool-size: %d\n", run.Config.ValuePoolSize))
	}
	if strings.TrimSpace(run.Config.Profile) != "" {
		sb.WriteString(fmt.Sprintf("- profile: %s\n", strings.TrimSpace(run.Config.Profile)))
	}
	if strings.TrimSpace(run.Config.DBsArg) != "" {
		sb.WriteString(fmt.Sprintf("- dbs: %s\n", strings.TrimSpace(run.Config.DBsArg)))
	}
	if strings.TrimSpace(run.Config.TestsArg) != "" {
		sb.WriteString(fmt.Sprintf("- tests: %s\n", strings.TrimSpace(run.Config.TestsArg)))
	}
	if run.Config.SeedUsed != 0 {
		sb.WriteString(fmt.Sprintf("- seed: %d\n", run.Config.SeedUsed))
	}
	sb.WriteString("\n")

	if hasInstance(run.Instances, "treedb") {
		if text, err := treeDBResolvedOptionsText(""); err == nil && strings.TrimSpace(text) != "" {
			sb.WriteString("## Resolved TreeDB Options\n\n")
			sb.WriteString("```text\n")
			sb.WriteString(text)
			sb.WriteString("\n```\n\n")
		}
	}

	sb.WriteString("```text\n")
	table, _, _, _ := renderResultsTableStringWithLayout(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
	sb.WriteString(table)
	sb.WriteString("```\n")

	if details := strings.TrimSpace(renderBatchDeleteRangeReportsString(run.Instances, run.BatchDeleteRange)); details != "" {
		sb.WriteString("\n")
		sb.WriteString("## Batch DeleteRange Metrics\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(details)
		sb.WriteString("\n```")
		sb.WriteString("\n")
	}

	if len(run.CheckpointDurations) > 0 {
		sb.WriteString("\n")
		title := "## Checkpoint Time (Between Tests)\n\n"
		if _, ok := run.CheckpointDurations[checkpointPostRunLabel]; ok {
			title = "## Checkpoint Time (Between Tests + Post-run)\n\n"
		}
		sb.WriteString(title)
		sb.WriteString("```text\n")
		sb.WriteString(renderCheckpointDurationsTableString(run.Instances, run.TestOrder, run.DisplayNames, run.CheckpointDurations))
		sb.WriteString("```\n")
	}
	if len(run.CheckpointSettleDurations) > 0 {
		sb.WriteString("\n")
		sb.WriteString("## Checkpoint Settle Time (Before Selected Checkpoints)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(renderCheckpointDurationsTableString(run.Instances, run.TestOrder, run.DisplayNames, run.CheckpointSettleDurations))
		sb.WriteString("```\n")
	}
	if checkpointStats := strings.TrimSpace(renderCheckpointTreeDBStatsString(run.Instances, run.TestOrder, run.DisplayNames, run.CheckpointTreeDBStats)); checkpointStats != "" {
		sb.WriteString("\n")
		sb.WriteString("## TreeDB Selected Stats (Checkpoint Snapshots)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(checkpointStats)
		sb.WriteString("\n```\n")
	}
	if len(run.VacuumDurations) > 0 {
		sb.WriteString("\n")
		sb.WriteString("## Vacuum Time (Between Tests)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(renderVacuumDurationsTableString(run.Instances, run.TestOrder, run.DisplayNames, run.VacuumDurations))
		sb.WriteString("```\n")
		if len(run.VacuumIndexBytes) > 0 {
			sb.WriteString("\n")
			sb.WriteString("## Vacuum Index Bytes (Between Tests)\n\n")
			sb.WriteString("```text\n")
			sb.WriteString(renderVacuumIndexBytesTableString(run.Instances, run.TestOrder, run.DisplayNames, run.VacuumIndexBytes))
			sb.WriteString("```\n")
		}
	}
	if len(run.TreeDBDiskUsage) > 0 || len(run.DiskUsage) > 0 {
		sb.WriteString("\n")
		sb.WriteString("## Disk Usage (End of Run)\n\n")
		sb.WriteString("```text\n")
		if len(run.TreeDBDiskUsage) > 0 {
			sb.WriteString(renderTreeDBDiskUsageString(run.TreeDBDiskUsage))
			if other := renderNonTreeDBDiskUsageString(run.DiskUsage, run.TreeDBDiskUsage); strings.TrimSpace(other) != "" {
				sb.WriteByte('\n')
				sb.WriteString("Other DBs:\n")
				for _, line := range strings.Split(strings.TrimSpace(other), "\n") {
					if strings.TrimSpace(line) == "" {
						continue
					}
					sb.WriteString("  ")
					sb.WriteString(line)
					sb.WriteByte('\n')
				}
			}
		} else {
			sb.WriteString(renderDirDiskUsageString(run.DiskUsage))
		}
		sb.WriteString("```\n")
	}
	if len(run.TreeDBVlogRewrite) > 0 {
		sb.WriteString("\n")
		sb.WriteString("## TreeDB CompactStorage (After Run)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(renderTreeDBVlogRewriteString(run.TreeDBVlogRewrite))
		sb.WriteString("```\n")
	}
	if perf := strings.TrimSpace(renderTreeDBPerfString(run.Instances, run.TestOrder, run.DisplayNames, run.TreeDBPerf)); perf != "" {
		sb.WriteString("\n")
		sb.WriteString("## TreeDB Perf Instrumentation\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(perf)
		sb.WriteString("\n```\n")
	}
	if codecStats := strings.TrimSpace(renderTreeDBVlogCodecSummaryString(run.Instances, run.TreeDBStats)); codecStats != "" {
		sb.WriteString("\n")
		sb.WriteString("## TreeDB Value-Log Codec Summary (End of Run)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(codecStats)
		sb.WriteString("\n```\n")
	}
	if stats := strings.TrimSpace(renderTreeDBSelectedStatsString(run.Instances, run.TreeDBStats)); stats != "" {
		sb.WriteString("\n")
		sb.WriteString("## TreeDB Selected Stats (End of Run)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(stats)
		sb.WriteString("\n```\n")
	}
	if run.Config.KeepDir {
		if kept := strings.TrimSpace(renderKeptDirsString(run.Instances)); kept != "" {
			sb.WriteString("\n")
			sb.WriteString("## Kept Data Directories\n\n")
			sb.WriteString("```text\n")
			sb.WriteString(kept)
			sb.WriteString("\n```\n")
		}
	}
	return sb.String()
}

func renderCheckpointTreeDBStatsString(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, checkpointStats map[string]map[string]map[string]string) string {
	if len(checkpointStats) == 0 {
		return ""
	}
	order := make([]string, 0, len(finalTestOrder)+1)
	seen := make(map[string]struct{}, len(checkpointStats))
	for _, testName := range finalTestOrder {
		if _, ok := checkpointStats[testName]; !ok {
			continue
		}
		order = append(order, testName)
		seen[testName] = struct{}{}
	}
	if _, ok := checkpointStats[checkpointPostRunLabel]; ok {
		if _, already := seen[checkpointPostRunLabel]; !already {
			order = append(order, checkpointPostRunLabel)
			seen[checkpointPostRunLabel] = struct{}{}
		}
	}
	var extras []string
	for label := range checkpointStats {
		if _, ok := seen[label]; ok {
			continue
		}
		extras = append(extras, label)
	}
	sort.Strings(extras)
	order = append(order, extras...)

	var sb strings.Builder
	for _, label := range order {
		statsText := strings.TrimSpace(renderTreeDBSelectedStatsString(instances, checkpointStats[label]))
		if statsText == "" {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		display := displayNames[label]
		if strings.TrimSpace(display) == "" {
			display = label
		}
		if label == checkpointPostRunLabel {
			sb.WriteString("after run")
		} else {
			sb.WriteString("before ")
			sb.WriteString(display)
		}
		sb.WriteByte('\n')
		sb.WriteString(statsText)
		sb.WriteByte('\n')
	}
	return strings.TrimSpace(sb.String())
}

func renderTreeDBPerfString(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, perf map[string]map[string]treeDBPerfMetrics) string {
	if len(perf) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, testName := range finalTestOrder {
		perDB := perf[testName]
		if len(perDB) == 0 {
			continue
		}
		label := displayNames[testName]
		if strings.TrimSpace(label) == "" {
			label = testName
		}
		wroteHeader := false
		for _, inst := range instances {
			if inst == nil || inst.Wrapper == nil {
				continue
			}
			dbName := inst.Wrapper.Name()
			m, ok := perDB[dbName]
			if !ok || treeDBPerfMetricsEmpty(m) {
				continue
			}
			if wroteHeader {
				sb.WriteByte('\n')
			}
			wroteHeader = true
			sb.WriteString(label)
			sb.WriteString(" / ")
			sb.WriteString(dbName)
			sb.WriteByte('\n')
			if m.Snapshot.AcquireCalls > 0 || m.Snapshot.CloseCalls > 0 {
				sb.WriteString(fmt.Sprintf("  snapshot.acquire.calls=%d total_ms=%.3f avg_us=%.3f\n",
					m.Snapshot.AcquireCalls,
					float64(m.Snapshot.AcquireTotalNanos)/1_000_000.0,
					m.Snapshot.AcquireAvgMicros,
				))
				sb.WriteString(fmt.Sprintf("  snapshot.close.calls=%d total_ms=%.3f avg_us=%.3f\n",
					m.Snapshot.CloseCalls,
					float64(m.Snapshot.CloseTotalNanos)/1_000_000.0,
					m.Snapshot.CloseAvgMicros,
				))
			}
			mmapTotal := m.Mmap.Hits + m.Mmap.FallbackReadAt
			if mmapTotal > 0 || m.Mmap.MissOutOfRange > 0 || m.Mmap.MissNoMapping > 0 || m.Mmap.MissDeadMapCap > 0 {
				sb.WriteString(fmt.Sprintf("  vlog_mmap.read.hits.delta=%d miss_out_of_range.delta=%d miss_no_mapping.delta=%d miss_dead_mapping_cap.delta=%d fallback_readat.delta=%d hit_ratio.delta=%.6f\n",
					m.Mmap.Hits,
					m.Mmap.MissOutOfRange,
					m.Mmap.MissNoMapping,
					m.Mmap.MissDeadMapCap,
					m.Mmap.FallbackReadAt,
					m.Mmap.HitRatio,
				))
			}
			sb.WriteString(fmt.Sprintf("  leaf_generation.generations.pinned.after=%d leaf_generation.pins.total.after=%d\n",
				m.LeafGenerationsPinnedAfter,
				m.LeafPinsTotalAfter,
			))
		}
		if wroteHeader {
			sb.WriteByte('\n')
		}
	}
	return strings.TrimSpace(sb.String())
}

func renderBatchDeleteRangeReportsString(instances []*DBInstance, reports map[string]map[string]batchDeleteRangeReport) string {
	perDB := reports["batch_delete_range"]
	if len(perDB) == 0 {
		return ""
	}
	orderedNames := make([]string, 0, len(perDB))
	seen := make(map[string]struct{}, len(perDB))
	for _, inst := range instances {
		if inst == nil || inst.Wrapper == nil {
			continue
		}
		name := inst.Wrapper.Name()
		if _, ok := perDB[name]; !ok {
			continue
		}
		orderedNames = append(orderedNames, name)
		seen[name] = struct{}{}
	}
	for name := range perDB {
		if _, ok := seen[name]; !ok {
			orderedNames = append(orderedNames, name)
		}
	}
	sort.Strings(orderedNames[len(seen):])

	headers := []string{"DB", "mode", "loaded", "width", "ranges/batch", "ranges", "affected", "range_ops/s", "affected_keys/s", "affected/range", "validation", "refill"}
	rows := make([][]string, 0, len(orderedNames)+1)
	rows = append(rows, headers)
	for _, name := range orderedNames {
		r := perDB[name]
		rows = append(rows, []string{
			name,
			r.Mode,
			formatInt(r.LoadedKeys),
			formatInt(r.RangeWidth),
			formatInt(r.RangesPerBatch),
			formatInt(r.RangeCount),
			formatInt(r.AffectedKeys),
			formatFloat(r.RangeOpsPerSec),
			formatFloat(r.AffectedKeysPerSec),
			fmt.Sprintf("%.2f", r.AffectedKeysPerRange),
			r.Validation,
			strconv.FormatBool(r.Refill),
		})
	}
	widths := make([]int, len(headers))
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	var sb strings.Builder
	for rowIdx, row := range rows {
		for i, cell := range row {
			if i > 0 {
				sb.WriteString("  ")
			}
			if i == 0 || rowIdx == 0 || i == 1 || i >= 10 {
				sb.WriteString(fmt.Sprintf("%-*s", widths[i], cell))
			} else {
				sb.WriteString(fmt.Sprintf("%*s", widths[i], cell))
			}
		}
		sb.WriteByte('\n')
		if rowIdx == 0 {
			for i, width := range widths {
				if i > 0 {
					sb.WriteString("  ")
				}
				sb.WriteString(strings.Repeat("-", width))
			}
			sb.WriteByte('\n')
		}
	}
	return sb.String()
}

type treeDBVlogSummaryMetric struct {
	label string
	key   string
}

func renderTreeDBVlogCodecSummaryString(instances []*DBInstance, treeStats map[string]map[string]string) string {
	if len(treeStats) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, inst := range instances {
		if inst == nil || inst.Wrapper == nil {
			continue
		}
		dbName := inst.Wrapper.Name()
		stats := treeStats[dbName]
		if len(stats) == 0 {
			continue
		}
		var dbSB strings.Builder
		appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_auto.frames", []treeDBVlogSummaryMetric{
			{label: "off", key: "treedb.cache.vlog_auto.frames.off"},
			{label: "dict", key: "treedb.cache.vlog_auto.frames.dict"},
			{label: "block_snappy", key: "treedb.cache.vlog_auto.frames.block_snappy"},
			{label: "block_lz4", key: "treedb.cache.vlog_auto.frames.block_lz4"},
			{label: "block_zstd", key: "treedb.cache.vlog_auto.frames.block_zstd"},
		})
		appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_auto.bytes", []treeDBVlogSummaryMetric{
			{label: "off", key: "treedb.cache.vlog_auto.bytes.off"},
			{label: "dict", key: "treedb.cache.vlog_auto.bytes.dict"},
			{label: "block_snappy", key: "treedb.cache.vlog_auto.bytes.block_snappy"},
			{label: "block_lz4", key: "treedb.cache.vlog_auto.bytes.block_lz4"},
			{label: "block_zstd", key: "treedb.cache.vlog_auto.bytes.block_zstd"},
		})
		appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_auto.frames_frac", []treeDBVlogSummaryMetric{
			{label: "off", key: "treedb.cache.vlog_auto.frames_frac.off"},
			{label: "dict", key: "treedb.cache.vlog_auto.frames_frac.dict"},
			{label: "block_snappy", key: "treedb.cache.vlog_auto.frames_frac.block_snappy"},
			{label: "block_lz4", key: "treedb.cache.vlog_auto.frames_frac.block_lz4"},
			{label: "block_zstd", key: "treedb.cache.vlog_auto.frames_frac.block_zstd"},
		})
		appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_auto.probes", []treeDBVlogSummaryMetric{
			{label: "attempts", key: "treedb.cache.vlog_auto.probe_attempts"},
			{label: "successes", key: "treedb.cache.vlog_auto.probe_successes"},
			{label: "success_frac", key: "treedb.cache.vlog_auto.probe_success_frac"},
			{label: "hold_enters", key: "treedb.cache.vlog_auto.hold_enters"},
			{label: "hold_exits", key: "treedb.cache.vlog_auto.hold_exits"},
			{label: "bypass_bytes", key: "treedb.cache.vlog_auto.bypass_bytes"},
		})
		for _, mode := range []string{"off", "block", "dict"} {
			appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_write_mode."+mode, []treeDBVlogSummaryMetric{
				{label: "frames", key: "treedb.cache.vlog_write_mode.frames." + mode},
				{label: "raw_bytes", key: "treedb.cache.vlog_write_mode.raw_bytes." + mode},
				{label: "stored_bytes", key: "treedb.cache.vlog_write_mode.stored_bytes." + mode},
				{label: "stored_ratio", key: "treedb.cache.vlog_write_mode.stored_ratio." + mode},
			})
		}
		for _, kind := range []string{"single_value", "outer_leaf", "mixed"} {
			appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_payload_kind."+kind, []treeDBVlogSummaryMetric{
				{label: "frames", key: "treedb.cache.vlog_payload_kind.frames." + kind},
				{label: "raw_bytes", key: "treedb.cache.vlog_payload_kind.raw_bytes." + kind},
				{label: "stored_bytes", key: "treedb.cache.vlog_payload_kind.stored_bytes." + kind},
				{label: "stored_ratio", key: "treedb.cache.vlog_payload_kind.stored_ratio." + kind},
			})
		}
		for _, kind := range []string{"single_value", "outer_leaf"} {
			appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_payload_split."+kind, []treeDBVlogSummaryMetric{
				{label: "records", key: "treedb.cache.vlog_payload_split.records." + kind},
				{label: "raw_bytes", key: "treedb.cache.vlog_payload_split.raw_bytes." + kind},
				{label: "stored_bytes", key: "treedb.cache.vlog_payload_split.stored_bytes." + kind},
				{label: "stored_ratio", key: "treedb.cache.vlog_payload_split.stored_ratio." + kind},
			})
		}
		for _, codec := range []string{"none", "snappy", "lz4", "legacy_page", "unknown", "mixed"} {
			appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_outer_leaf_codec."+codec, []treeDBVlogSummaryMetric{
				{label: "frames", key: "treedb.cache.vlog_outer_leaf_codec.frames." + codec},
				{label: "raw_bytes", key: "treedb.cache.vlog_outer_leaf_codec.raw_bytes." + codec},
				{label: "stored_bytes", key: "treedb.cache.vlog_outer_leaf_codec.stored_bytes." + codec},
				{label: "stored_ratio", key: "treedb.cache.vlog_outer_leaf_codec.stored_ratio." + codec},
			})
		}
		for _, codec := range []string{"snappy", "lz4", "zstd"} {
			appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_block.k."+codec, []treeDBVlogSummaryMetric{
				{label: "count", key: "treedb.cache.vlog_block.k.count." + codec},
				{label: "avg", key: "treedb.cache.vlog_block.k.avg." + codec},
				{label: "max", key: "treedb.cache.vlog_block.k.max." + codec},
				{label: "ratio", key: "treedb.cache.vlog_block.ratio." + codec},
			})
			appendTreeDBVlogSummaryLine(&dbSB, stats, "vlog_block.k.bucket."+codec, []treeDBVlogSummaryMetric{
				{label: "le_1", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_1"},
				{label: "le_2", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_2"},
				{label: "le_4", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_4"},
				{label: "le_8", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_8"},
				{label: "le_16", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_16"},
				{label: "le_32", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_32"},
				{label: "le_64", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_64"},
				{label: "le_128", key: "treedb.cache.vlog_block.k.bucket." + codec + ".le_128"},
			})
		}
		appendTreeDBLeafScanVlogSummaryLines(&dbSB, stats)
		if dbSB.Len() == 0 {
			continue
		}
		sb.WriteString(dbName)
		sb.WriteString(":\n")
		sb.WriteString(dbSB.String())
	}
	return strings.TrimSpace(sb.String())
}

func appendTreeDBLeafScanVlogSummaryLines(sb *strings.Builder, stats map[string]string) {
	appendTreeDBVlogSummaryLine(sb, stats, "vlog_leaf_scan.auto.frames", []treeDBVlogSummaryMetric{
		{label: "off", key: "treedb.cache.vlog_leaf_scan.auto.frames.off"},
		{label: "dict", key: "treedb.cache.vlog_leaf_scan.auto.frames.dict"},
		{label: "block_snappy", key: "treedb.cache.vlog_leaf_scan.auto.frames.block_snappy"},
		{label: "block_lz4", key: "treedb.cache.vlog_leaf_scan.auto.frames.block_lz4"},
		{label: "block_zstd", key: "treedb.cache.vlog_leaf_scan.auto.frames.block_zstd"},
	})
	for _, mode := range []string{"off", "block", "dict"} {
		appendTreeDBVlogSummaryLine(sb, stats, "vlog_leaf_scan.write_mode."+mode, []treeDBVlogSummaryMetric{
			{label: "frames", key: "treedb.cache.vlog_leaf_scan.write_mode.frames." + mode},
			{label: "raw_bytes", key: "treedb.cache.vlog_leaf_scan.write_mode.raw_bytes." + mode},
			{label: "stored_bytes", key: "treedb.cache.vlog_leaf_scan.write_mode.stored_bytes." + mode},
			{label: "stored_ratio", key: "treedb.cache.vlog_leaf_scan.write_mode.stored_ratio." + mode},
		})
	}
	for _, codec := range []string{"none", "snappy", "lz4", "legacy_page", "unknown", "mixed"} {
		appendTreeDBVlogSummaryLine(sb, stats, "vlog_leaf_scan.outer_leaf_codec."+codec, []treeDBVlogSummaryMetric{
			{label: "frames", key: "treedb.cache.vlog_leaf_scan.outer_leaf_codec.frames." + codec},
			{label: "raw_bytes", key: "treedb.cache.vlog_leaf_scan.outer_leaf_codec.raw_bytes." + codec},
			{label: "stored_bytes", key: "treedb.cache.vlog_leaf_scan.outer_leaf_codec.stored_bytes." + codec},
			{label: "stored_ratio", key: "treedb.cache.vlog_leaf_scan.outer_leaf_codec.stored_ratio." + codec},
		})
	}
	for _, codec := range []string{"snappy", "lz4", "zstd"} {
		appendTreeDBVlogSummaryLine(sb, stats, "vlog_leaf_scan.block.k."+codec, []treeDBVlogSummaryMetric{
			{label: "count", key: "treedb.cache.vlog_leaf_scan.block.k.count." + codec},
			{label: "avg", key: "treedb.cache.vlog_leaf_scan.block.k.avg." + codec},
			{label: "max", key: "treedb.cache.vlog_leaf_scan.block.k.max." + codec},
			{label: "ratio", key: "treedb.cache.vlog_leaf_scan.block.ratio." + codec},
		})
	}
}

func appendTreeDBVlogSummaryLine(sb *strings.Builder, stats map[string]string, label string, metrics []treeDBVlogSummaryMetric) bool {
	parts := make([]string, 0, len(metrics))
	hasSignal := false
	for _, metric := range metrics {
		value, ok := stats[metric.key]
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		parts = append(parts, metric.label+"="+value)
		if treeDBVlogStatValueHasSignal(value) {
			hasSignal = true
		}
	}
	if len(parts) == 0 || !hasSignal {
		return false
	}
	sb.WriteString("  ")
	sb.WriteString(label)
	sb.WriteString(": ")
	sb.WriteString(strings.Join(parts, " "))
	sb.WriteByte('\n')
	return true
}

func treeDBVlogStatValueHasSignal(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f != 0
	}
	return value != "false"
}

func renderTreeDBSelectedStatsString(instances []*DBInstance, treeStats map[string]map[string]string) string {
	if len(treeStats) == 0 {
		return ""
	}
	keys := []struct {
		label string
		alts  []string
	}{
		{label: "write_path.mode", alts: []string{"treedb.write_path.mode"}},
		{label: "write_path.redo_log", alts: []string{"treedb.write_path.redo_log"}},
		{label: "flush_admission.policy", alts: []string{"treedb.flush_admission.policy"}},
		{label: "flush_admission.admitted", alts: []string{"treedb.flush_admission.admitted"}},
		{label: "flush_admission.reason", alts: []string{"treedb.flush_admission.reason"}},
		{label: "flush_admission.flush_apply_concurrency_configured", alts: []string{"treedb.flush_admission.flush_apply_concurrency_configured"}},
		{label: "flush_admission.flush_apply_concurrency", alts: []string{"treedb.flush_admission.flush_apply_concurrency"}},
		{label: "flush_admission.flush_apply_concurrency_cap_reason", alts: []string{"treedb.flush_admission.flush_apply_concurrency_cap_reason"}},
		{label: "flush_admission.flush_apply_concurrency_defaulted", alts: []string{"treedb.flush_admission.flush_apply_concurrency_defaulted"}},
		{label: "flush_admission.gomaxprocs", alts: []string{"treedb.flush_admission.gomaxprocs"}},
		{label: "flush_admission.physical_cores", alts: []string{"treedb.flush_admission.physical_cores"}},
		{label: "flush_admission.flush_apply_span_native", alts: []string{"treedb.flush_admission.flush_apply_span_native"}},
		{label: "flush_admission.flush_backlog_coalescing", alts: []string{"treedb.flush_admission.flush_backlog_coalescing"}},
		{label: "flush_admission.leaf_page_read_cache_write_admission", alts: []string{"treedb.flush_admission.leaf_page_read_cache_write_admission"}},
		{label: "vlog_mmap.current_writable_map_target_bytes", alts: []string{"treedb.vlog.mmap_current_writable_map_target_bytes", "treedb.cache.vlog_mmap.current_writable_map_target_bytes"}},
		{label: "vlog_mmap.max_mapped_sealed_segments", alts: []string{"treedb.vlog.mmap_max_mapped_sealed_segments", "treedb.cache.vlog_mmap.max_mapped_sealed_segments"}},
		{label: "vlog_mmap.max_mapped_sealed_bytes", alts: []string{"treedb.vlog.mmap_max_mapped_sealed_bytes", "treedb.cache.vlog_mmap.max_mapped_sealed_bytes"}},
		{label: "vlog_mmap.max_mapped_leaf_sealed_segments", alts: []string{"treedb.vlog.mmap_max_mapped_leaf_sealed_segments", "treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments"}},
		{label: "vlog_mmap.max_mapped_leaf_sealed_bytes", alts: []string{"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes", "treedb.cache.vlog_mmap.max_mapped_leaf_sealed_bytes"}},
		{label: "vlog_mmap.sealed_map_denied", alts: []string{"treedb.vlog.mmap_sealed_map_denied", "treedb.cache.vlog_mmap.sealed_map_denied"}},
		{label: "vlog_mmap.sealed_map_denied.count_cap", alts: []string{"treedb.vlog.mmap_sealed_map_denied.count_cap", "treedb.cache.vlog_mmap.sealed_map_denied.count_cap"}},
		{label: "vlog_mmap.sealed_map_denied.bytes_cap", alts: []string{"treedb.vlog.mmap_sealed_map_denied.bytes_cap", "treedb.cache.vlog_mmap.sealed_map_denied.bytes_cap"}},
		{label: "applied_command_lsn", alts: []string{"treedb.applied_command_lsn"}},
		{label: "command_wal.enabled", alts: []string{"treedb.command_wal.enabled"}},
		{label: "command_wal.required_feature", alts: []string{"treedb.command_wal.required_feature"}},
		{label: "command_wal.live_accepted_frames", alts: []string{"treedb.command_wal.live_accepted_frames"}},
		{label: "command_wal.live_accepted_max_lsn", alts: []string{"treedb.command_wal.live_accepted_max_lsn"}},
		{label: "command_wal.live_covered_frames", alts: []string{"treedb.command_wal.live_covered_frames"}},
		{label: "command_wal.live_covered_max_lsn", alts: []string{"treedb.command_wal.live_covered_max_lsn"}},
		{label: "command_wal.frames", alts: []string{"treedb.command_wal.frames"}},
		{label: "command_wal.typed_segments", alts: []string{"treedb.command_wal.typed_segments"}},
		{label: "command_wal.max_lsn", alts: []string{"treedb.command_wal.max_lsn"}},
		{label: "leaf_generation.generations.pinned", alts: []string{"treedb.leaf_generation.generations.pinned"}},
		{label: "leaf_generation.pins.total", alts: []string{"treedb.leaf_generation.pins.total"}},
		{label: "flush_apply.cache.batches_total", alts: []string{"treedb.cache.flush_apply.batches_total"}},
		{label: "flush_apply.cache.planning_ns_total", alts: []string{"treedb.cache.flush_apply.planning_ns_total"}},
		{label: "flush_apply.cache.build_ns_total", alts: []string{"treedb.cache.flush_apply.build_ns_total"}},
		{label: "flush_apply.cache.backend_write_ns_total", alts: []string{"treedb.cache.flush_apply.backend_write_ns_total"}},
		{label: "flush_apply.cache.leaf_log_append_wait_ns_total", alts: []string{"treedb.cache.flush_apply.leaf_log_append_wait_ns_total"}},
		{label: "flush_apply.cache.leaf_log_append_ns_total", alts: []string{"treedb.cache.flush_apply.leaf_log_append_ns_total"}},
		{label: "flush_apply.cache.leaf_log_append_bytes_total", alts: []string{"treedb.cache.flush_apply.leaf_log_append_bytes_total"}},
		{label: "flush_apply.cache.leaf_log_append_frames_total", alts: []string{"treedb.cache.flush_apply.leaf_log_append_frames_total"}},
		{label: "flush_apply.cache.leaf_log_append_frames_per_op", alts: []string{"treedb.cache.flush_apply.leaf_log_append_frames_per_op"}},
		{label: "flush_apply.cache.leaf_log_append_records_total", alts: []string{"treedb.cache.flush_apply.leaf_log_append_records_total"}},
		{label: "leaf_log_lanes.configured", alts: []string{"treedb.cache.leaf_log_lanes.configured"}},
		{label: "leaf_log_lanes.active", alts: []string{"treedb.cache.leaf_log_lanes.active"}},
		{label: "leaf_log_lanes.append_lanes_used", alts: []string{"treedb.cache.leaf_log_lanes.append_lanes_used"}},
		{label: "leaf_log_lanes.append_calls_total", alts: []string{"treedb.cache.leaf_log_lanes.append_calls_total"}},
		{label: "leaf_log_lanes.append_pages_total", alts: []string{"treedb.cache.leaf_log_lanes.append_pages_total"}},
		{label: "leaf_log_lanes.append_bytes_total", alts: []string{"treedb.cache.leaf_log_lanes.append_bytes_total"}},
		{label: "leaf_log_lanes.append_lock_wait_ns_total", alts: []string{"treedb.cache.leaf_log_lanes.append_lock_wait_ns_total"}},
		{label: "leaf_log_lanes.append_lock_hold_ns_total", alts: []string{"treedb.cache.leaf_log_lanes.append_lock_hold_ns_total"}},
		{label: "leaf_log_lanes.append_errors_total", alts: []string{"treedb.cache.leaf_log_lanes.append_errors_total"}},
		{label: "leaf_log_lanes.segment_rotations_total", alts: []string{"treedb.cache.leaf_log_lanes.segment_rotations_total"}},
		{label: "journal_lanes.configured", alts: []string{"treedb.cache.journal_lanes.configured"}},
		{label: "journal_lanes.defaulted", alts: []string{"treedb.cache.journal_lanes.defaulted"}},
		{label: "journal_lanes.effective", alts: []string{"treedb.cache.journal_lanes.effective"}},
		{label: "journal_lanes.hot", alts: []string{"treedb.cache.journal_lanes.hot"}},
		{label: "journal_lanes.warm", alts: []string{"treedb.cache.journal_lanes.warm"}},
		{label: "journal_lanes.cold", alts: []string{"treedb.cache.journal_lanes.cold"}},
		{label: "memtable_shards", alts: []string{"treedb.cache.memtable_shards"}},
		{label: "flush_span_run.target_leaves_split_across_chunks_total", alts: []string{"treedb.cache.flush_span_run.target_leaves_split_across_chunks_total"}},
		{label: "flush_span_run.target_leaf_spans_total", alts: []string{"treedb.cache.flush_span_run.target_leaf_spans_total"}},
		{label: "flush_span_run.single_op_spans_total", alts: []string{"treedb.cache.flush_span_run.single_op_spans_total"}},
		{label: "flush_span_run.ops_per_span", alts: []string{"treedb.cache.flush_span_run.ops_per_span"}},
		{label: "flush_span_run.single_op_span_ratio", alts: []string{"treedb.cache.flush_span_run.single_op_span_ratio"}},
		{label: "flush_span_run.source_point_ops_total", alts: []string{"treedb.cache.flush_span_run.source_point_ops_total"}},
		{label: "flush_span_run.planned_ops_total", alts: []string{"treedb.cache.flush_span_run.planned_ops_total"}},
		{label: "flush_span_run.planned_point_ops_total", alts: []string{"treedb.cache.flush_span_run.planned_point_ops_total"}},
		{label: "flush_span_run.source_memtables_total", alts: []string{"treedb.cache.flush_span_run.source_memtables_total"}},
		{label: "flush_span_run.shadowed_ops_total", alts: []string{"treedb.cache.flush_span_run.shadowed_ops_total"}},
		{label: "flush_span_run.range_barriers_total", alts: []string{"treedb.cache.flush_span_run.range_barriers_total"}},
		{label: "flush_span_run.backend_chunks_total", alts: []string{"treedb.cache.flush_span_run.backend_chunks_total"}},
		{label: "flush_backlog_coalescing.admitted_runs_total", alts: []string{"treedb.cache.flush_backlog_coalescing.admitted_runs_total"}},
		{label: "flush_backlog_coalescing.admitted_extra_memtables_total", alts: []string{"treedb.cache.flush_backlog_coalescing.admitted_extra_memtables_total"}},
		{label: "flush_backlog_coalescing.admitted_extra_ops_total", alts: []string{"treedb.cache.flush_backlog_coalescing.admitted_extra_ops_total"}},
		{label: "flush_backlog_coalescing.selected_memtables_max", alts: []string{"treedb.cache.flush_backlog_coalescing.selected_memtables_max"}},
		{label: "flush_backlog_coalescing.selected_ops_max", alts: []string{"treedb.cache.flush_backlog_coalescing.selected_ops_max"}},
		{label: "flush_backlog_coalescing.checkpoint.admitted_runs_total", alts: []string{"treedb.cache.flush_backlog_coalescing.checkpoint.admitted_runs_total"}},
		{label: "flush_backlog_coalescing.checkpoint.selected_memtables_max", alts: []string{"treedb.cache.flush_backlog_coalescing.checkpoint.selected_memtables_max"}},
		{label: "flush_backlog_coalescing.checkpoint.selected_ops_max", alts: []string{"treedb.cache.flush_backlog_coalescing.checkpoint.selected_ops_max"}},
		{label: "flush_backlog_coalescing.checkpoint.base_budget_covered_total", alts: []string{"treedb.cache.flush_backlog_coalescing.checkpoint.base_budget_covered_total"}},
		{label: "flush_backlog_coalescing.last_single_op_span_ratio", alts: []string{"treedb.cache.flush_backlog_coalescing.last_single_op_span_ratio"}},
		{label: "flush_backlog_coalescing.last_ops_per_span", alts: []string{"treedb.cache.flush_backlog_coalescing.last_ops_per_span"}},
		{label: "flush_backlog_coalescing.skip.memory_budget_total", alts: []string{"treedb.cache.flush_backlog_coalescing.skip.reason.memory_budget_total"}},
		{label: "flush_backlog_coalescing.skip.ops_budget_total", alts: []string{"treedb.cache.flush_backlog_coalescing.skip.reason.ops_budget_total"}},
		{label: "flush_backlog_coalescing.skip.range_barrier_total", alts: []string{"treedb.cache.flush_backlog_coalescing.skip.reason.range_barrier_total"}},
		{label: "flush_backlog_coalescing.skip.lane_barrier_total", alts: []string{"treedb.cache.flush_backlog_coalescing.skip.reason.lane_barrier_total"}},
		{label: "flush_backlog_coalescing.skip.stop_pressure_total", alts: []string{"treedb.cache.flush_backlog_coalescing.skip.reason.stop_pressure_total"}},
		{label: "flush_apply.prepared_output.leaf_log_pages_prepared_total", alts: []string{"treedb.flush_apply.prepared_output.leaf_log_pages_prepared_total"}},
		{label: "flush_apply.prepared_output.leaf_log_pages_installed_total", alts: []string{"treedb.flush_apply.prepared_output.leaf_log_pages_installed_total"}},
		{label: "flush_apply.prepared_output.leaf_log_pages_abandoned_total", alts: []string{"treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total"}},
		{label: "flush_apply.cache.foreground_assist_wait_ns_total", alts: []string{"treedb.cache.flush_apply.foreground_assist_wait_ns_total"}},
		{label: "flush_apply.cache.coordinator.active", alts: []string{"treedb.cache.flush_apply.coordinator.active"}},
		{label: "flush_apply.cache.coordinator.active_workers", alts: []string{"treedb.cache.flush_apply.coordinator.active_workers"}},
		{label: "flush_apply.cache.coordinator.in_flight_bytes", alts: []string{"treedb.cache.flush_apply.coordinator.in_flight_bytes"}},
		{label: "flush_apply.cache.coordinator.active_assist_skips_total", alts: []string{"treedb.cache.flush_apply.coordinator.active_assist_skips_total"}},
		{label: "flush_apply.cache.coordinator.progress_wait_ns_total", alts: []string{"treedb.cache.flush_apply.coordinator.progress_wait_ns_total"}},
		{label: "flush_apply.cache.coordinator.stall_waits_total", alts: []string{"treedb.cache.flush_apply.coordinator.stall_waits_total"}},
		{label: "flush_apply.cache.coordinator.checkpoint_preemptions_total", alts: []string{"treedb.cache.flush_apply.coordinator.checkpoint_preemptions_total"}},
		{label: "flush_apply.cache.coordinator.blocking_fallbacks_total", alts: []string{"treedb.cache.flush_apply.coordinator.blocking_fallbacks_total"}},
		{label: "flush_apply.cache.coordinator.hard_overload_fallbacks_total", alts: []string{"treedb.cache.flush_apply.coordinator.hard_overload_fallbacks_total"}},
		{label: "checkpoint.flushmu_wait_total_ms", alts: []string{"treedb.cache.checkpoint.flushmu_wait_total_ms"}},
		{label: "checkpoint.active_background_flush_wait_ns_total", alts: []string{"treedb.cache.checkpoint.active_background_flush_wait_ns_total"}},
		{label: "checkpoint.flush_preempt_requests_total", alts: []string{"treedb.cache.checkpoint.flush_preempt_requests_total"}},
		{label: "checkpoint.preflush_kick_skips_total", alts: []string{"treedb.cache.checkpoint.preflush_kick_skips_total"}},
		{label: "checkpoint.shared_drain.flushmu_releases_total", alts: []string{"treedb.cache.checkpoint.shared_drain.flushmu_releases_total"}},
		{label: "checkpoint.shared_drain.checkpoint_units_total", alts: []string{"treedb.cache.checkpoint.shared_drain.checkpoint_units_total"}},
		{label: "checkpoint.shared_drain.background_units_total", alts: []string{"treedb.cache.checkpoint.shared_drain.background_units_total"}},
		{label: "checkpoint.shared_drain.checkpoint_ops_total", alts: []string{"treedb.cache.checkpoint.shared_drain.checkpoint_ops_total"}},
		{label: "checkpoint.shared_drain.background_ops_total", alts: []string{"treedb.cache.checkpoint.shared_drain.background_ops_total"}},
		{label: "checkpoint.shared_drain.checkpoint_bytes_total", alts: []string{"treedb.cache.checkpoint.shared_drain.checkpoint_bytes_total"}},
		{label: "checkpoint.shared_drain.background_bytes_total", alts: []string{"treedb.cache.checkpoint.shared_drain.background_bytes_total"}},
		{label: "checkpoint.shared_drain.wait_ns_total", alts: []string{"treedb.cache.checkpoint.shared_drain.wait_ns_total"}},
		{label: "checkpoint.shared_drain.wait_ns_max", alts: []string{"treedb.cache.checkpoint.shared_drain.wait_ns_max"}},
		{label: "checkpoint.stage.value_log_flush.total_ns", alts: []string{"treedb.cache.checkpoint.stage.value_log_flush.total_ns"}},
		{label: "checkpoint.stage.flush_all.total_ns", alts: []string{"treedb.cache.checkpoint.stage.flush_all.total_ns"}},
		{label: "checkpoint.stage.leaf_value_log_sync.total_ns", alts: []string{"treedb.cache.checkpoint.stage.leaf_value_log_sync.total_ns"}},
		{label: "checkpoint.stage.reducer_publish.total_ns", alts: []string{"treedb.cache.checkpoint.stage.reducer_publish.total_ns"}},
		{label: "flush_apply.apply_ns_total", alts: []string{"treedb.flush_apply.apply_ns_total"}},
		{label: "flush_apply.old_leaf_read_decode.bytes_total", alts: []string{"treedb.flush_apply.old_leaf_read_decode.bytes_total"}},
		{label: "flush_apply.old_leaf_read_decode.bytes_per_op", alts: []string{"treedb.flush_apply.old_leaf_read_decode.bytes_per_op"}},
		{label: "flush_apply.merge_build.leaf_merges_total", alts: []string{"treedb.flush_apply.merge_build.leaf_merges_total"}},
		{label: "flush_apply.merge_build.leaf_merges_per_op", alts: []string{"treedb.flush_apply.merge_build.leaf_merges_per_op"}},
		{label: "flush_apply.merge_build.replacement_leaf_pages_per_op", alts: []string{"treedb.flush_apply.merge_build.replacement_leaf_pages_per_op"}},
		{label: "flush_apply.leaf_log_output.reservation_wait_ns_total", alts: []string{"treedb.flush_apply.leaf_log_output.reservation_wait_ns_total"}},
		{label: "flush_apply.leaf_log_output.append_wait_ns_total", alts: []string{"treedb.flush_apply.leaf_log_output.append_wait_ns_total"}},
		{label: "flush_apply.leaf_log_output.append_calls_total", alts: []string{"treedb.flush_apply.leaf_log_output.append_calls_total"}},
		{label: "flush_apply.leaf_log_output.append_pages_total", alts: []string{"treedb.flush_apply.leaf_log_output.append_pages_total"}},
		{label: "flush_apply.leaf_log_output.lane.tasks_total", alts: []string{"treedb.flush_apply.leaf_log_output.lane.tasks_total"}},
		{label: "flush_apply.leaf_log_output.lane.tasks_lanes_used", alts: []string{"treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used"}},
		{label: "flush_apply.leaf_log_output.lane.tasks_max", alts: []string{"treedb.flush_apply.leaf_log_output.lane.tasks_max"}},
		{label: "flush_apply.leaf_log_output.lane.tasks_overflow_total", alts: []string{"treedb.flush_apply.leaf_log_output.lane.tasks_overflow_total"}},
		{label: "flush_apply.span_run.target_leaf_spans_total", alts: []string{"treedb.flush_apply.span_run.target_leaf_spans_total"}},
		{label: "flush_apply.span_run.single_op_spans_total", alts: []string{"treedb.flush_apply.span_run.single_op_spans_total"}},
		{label: "flush_apply.span_run.ops_per_span", alts: []string{"treedb.flush_apply.span_run.ops_per_span"}},
		{label: "flush_apply.span_run.bytes_per_span", alts: []string{"treedb.flush_apply.span_run.bytes_per_span"}},
		{label: "flush_apply.span_native.candidate_ops_total", alts: []string{"treedb.flush_apply.span_native.candidate_ops_total"}},
		{label: "flush_apply.span_native.eligible_ops_total", alts: []string{"treedb.flush_apply.span_native.eligible_ops_total"}},
		{label: "flush_apply.span_native.ineligible_ops_total", alts: []string{"treedb.flush_apply.span_native.ineligible_ops_total"}},
		{label: "flush_apply.span_native.scheduler.worker_busy_ns_total", alts: []string{"treedb.flush_apply.span_native.scheduler.worker_busy_ns_total"}},
		{label: "flush_apply.span_native.scheduler.worker_idle_ns_total", alts: []string{"treedb.flush_apply.span_native.scheduler.worker_idle_ns_total"}},
		{label: "flush_apply.span_native.scheduler.worker_wait_ns_total", alts: []string{"treedb.flush_apply.span_native.scheduler.worker_wait_ns_total"}},
		{label: "flush_apply.span_native.scheduler.ready_tasks_total", alts: []string{"treedb.flush_apply.span_native.scheduler.ready_tasks_total"}},
		{label: "flush_apply.span_native.scheduler.dispatched_tasks_total", alts: []string{"treedb.flush_apply.span_native.scheduler.dispatched_tasks_total"}},
		{label: "flush_apply.span_native.scheduler.completed_tasks_total", alts: []string{"treedb.flush_apply.span_native.scheduler.completed_tasks_total"}},
		{label: "flush_apply.span_native.scheduler.queue_depth_max", alts: []string{"treedb.flush_apply.span_native.scheduler.queue_depth_max"}},
		{label: "flush_apply.span_native.scheduler.scheduled_workers_total", alts: []string{"treedb.flush_apply.span_native.scheduler.scheduled_workers_total"}},
		{label: "flush_apply.span_native.scheduler.scheduled_workers_max", alts: []string{"treedb.flush_apply.span_native.scheduler.scheduled_workers_max"}},
		{label: "flush_apply.span_native.scheduler.task_spans_per_task", alts: []string{"treedb.flush_apply.span_native.scheduler.task_spans_per_task"}},
		{label: "flush_apply.span_native.scheduler.task_spans_max", alts: []string{"treedb.flush_apply.span_native.scheduler.task_spans_max"}},
		{label: "flush_apply.span_native.scheduler.task_ops_per_task", alts: []string{"treedb.flush_apply.span_native.scheduler.task_ops_per_task"}},
		{label: "flush_apply.span_native.scheduler.task_ops_max", alts: []string{"treedb.flush_apply.span_native.scheduler.task_ops_max"}},
		{label: "flush_apply.span_native.scheduler.task_bytes_per_task", alts: []string{"treedb.flush_apply.span_native.scheduler.task_bytes_per_task"}},
		{label: "flush_apply.span_native.scheduler.task_bytes_max", alts: []string{"treedb.flush_apply.span_native.scheduler.task_bytes_max"}},
		{label: "flush_apply.span_native.scheduler.single_span_tasks_total", alts: []string{"treedb.flush_apply.span_native.scheduler.single_span_tasks_total"}},
		{label: "flush_apply.span_native.fallback.not_implemented_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total"}},
		{label: "flush_apply.span_native.fallback.root_mismatch_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.root_mismatch.ops_total"}},
		{label: "flush_apply.span_native.fallback.range_delete_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.range_delete_barrier.ops_total"}},
		{label: "flush_apply.span_native.fallback.maintenance_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.maintenance.ops_total"}},
		{label: "flush_apply.span_native.fallback.inexact_leaf_spans_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.inexact_leaf_spans.ops_total"}},
		{label: "flush_apply.span_native.fallback.close_or_checkpoint_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"}},
		{label: "flush_apply.span_native.fallback.memory_or_emergency_cap_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.memory_or_emergency_cap.ops_total"}},
		{label: "flush_apply.span_native.fallback.output_ownership_failure_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.output_ownership_failure.ops_total"}},
		{label: "flush_apply.span_native.fallback.reducer_validation_failed_ops_total", alts: []string{"treedb.flush_apply.span_native.fallback.reason.reducer_validation_failed.ops_total"}},
		{label: "flush_apply.root_reduce.ns_total", alts: []string{"treedb.flush_apply.root_reduce.ns_total"}},
		{label: "flush_apply.commit_wait_ns_total", alts: []string{"treedb.flush_apply.commit_wait_ns_total"}},
		{label: "flush_apply.publish_prepare.ns_total", alts: []string{"treedb.flush_apply.publish_prepare.ns_total"}},
		{label: "flush_apply.guarded_publish.ns_total", alts: []string{"treedb.flush_apply.guarded_publish.ns_total"}},
		{label: "flush_apply.publish_final_install.ns_total", alts: []string{"treedb.flush_apply.publish_final_install.ns_total"}},
		{label: "flush_apply.publish_total.ns_total", alts: []string{"treedb.flush_apply.publish_total.ns_total"}},
		{label: "flush_apply.reducer_publish.ns_total", alts: []string{"treedb.flush_apply.reducer_publish.ns_total"}},
		{label: "flush_apply.retry_total", alts: []string{"treedb.flush_apply.retry_total"}},
		{label: "flush_apply.mismatch_total", alts: []string{"treedb.flush_apply.mismatch_total"}},
		{label: "publish.ordered_root_delta_group.calls_total", alts: []string{"treedb.publish.ordered_root_delta_group.calls_total"}},
		{label: "publish.ordered_root_delta_group.roots_total", alts: []string{"treedb.publish.ordered_root_delta_group.roots_total"}},
		{label: "publish.ordered_root_delta_group.avg_roots_per_call", alts: []string{"treedb.publish.ordered_root_delta_group.avg_roots_per_call"}},
		{label: "publish.ordered_root_delta_group.span_native.candidate_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.eligible_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.eligible_ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.used_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.used_ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.ineligible_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.ineligible_ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallbacks_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallbacks_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.not_implemented_count_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.count_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.not_implemented_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.prepare_error_count_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.count_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.prepare_error_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.route_ineligible_count_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.route_ineligible.count_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.route_ineligible_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.route_ineligible.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.disabled_count_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.disabled.count_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.disabled_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.disabled.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.admission_policy_decline_count_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.admission_policy_decline.count_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.admission_policy_decline_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.admission_policy_decline.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.validation_failed_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.validation_failed.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.range_delete_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.range_delete_barrier.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.cold_build_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.cold_build.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.inexact_leaf_spans_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.inexact_leaf_spans.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.memory_or_emergency_cap_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.memory_or_emergency_cap.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.output_ownership_failure_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.output_ownership_failure.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.reducer_validation_failed_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.reducer_validation_failed.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.root_mismatch_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.root_mismatch.ops_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.unknown_count_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.unknown.count_total"}},
		{label: "publish.ordered_root_delta_group.span_native.fallback.unknown_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.unknown.ops_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_calls_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_calls_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_ns_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_ns_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_ops_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_node_loads_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_node_loads_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total"}},
		{label: "publish.ordered_root_delta_group.publish_prepare_ns_total", alts: []string{"treedb.publish.ordered_root_delta_group.publish_prepare_ns_total"}},
		{label: "publish.ordered_root_delta_group.write_lock_wait_ns_total", alts: []string{"treedb.publish.ordered_root_delta_group.write_lock_wait_ns_total"}},
		{label: "publish.ordered_root_delta_group.write_lock_hold_ns_total", alts: []string{"treedb.publish.ordered_root_delta_group.write_lock_hold_ns_total"}},
	}
	orderedRootRoutes := []string{
		string(treedbdb.OrderedRootSpanNativeRouteDirectPublish),
		string(treedbdb.OrderedRootSpanNativeRouteGroupedPublish),
		string(treedbdb.OrderedRootSpanNativeRouteSystemDeltaBuilderPublish),
		string(treedbdb.OrderedRootSpanNativeRouteCommandWALPublish),
		string(treedbdb.OrderedRootSpanNativeRouteCollectionBufferedRoots),
		string(treedbdb.OrderedRootSpanNativeRouteOverlayColdBuild),
		string(treedbdb.OrderedRootSpanNativeRouteMultiIndexGroupPublish),
		string(treedbdb.OrderedRootSpanNativeRouteDeltaBatchPublish),
		string(treedbdb.OrderedRootSpanNativeRouteReadOnlyPrepare),
	}
	for _, route := range orderedRootRoutes {
		for _, field := range []string{
			"observations_total",
			"candidate_ops_total",
			"eligible_ops_total",
			"used_ops_total",
			"ineligible_ops_total",
			"fallbacks_total",
			"fallback.reason.span_native_not_implemented.count_total",
			"fallback.reason.span_native_not_implemented.ops_total",
			"fallback.reason.prepare_error.count_total",
			"fallback.reason.prepare_error.ops_total",
			"fallback.reason.route_ineligible.count_total",
			"fallback.reason.route_ineligible.ops_total",
			"fallback.reason.disabled.count_total",
			"fallback.reason.disabled.ops_total",
			"fallback.reason.admission_policy_decline.count_total",
			"fallback.reason.admission_policy_decline.ops_total",
			"fallback.reason.cold_build.ops_total",
			"fallback.reason.validation_failed.ops_total",
			"fallback.reason.range_delete_barrier.ops_total",
			"fallback.reason.inexact_leaf_spans.ops_total",
			"fallback.reason.unknown.count_total",
			"fallback.reason.unknown.ops_total",
		} {
			keys = append(keys, struct {
				label string
				alts  []string
			}{
				label: "publish.ordered_root_delta_group.span_native.route." + route + "." + field,
				alts:  []string{"treedb.publish.ordered_root_delta_group.span_native.route." + route + "." + field},
			})
		}
		for _, field := range []string{
			"context",
			"status",
			"candidate",
			"eligible",
			"used",
			"fallback_reason",
			"fallback_class",
			"admission_policy",
			"admission_admitted",
			"admission_reason",
			"selected_workers",
			"detail",
		} {
			keys = append(keys, struct {
				label string
				alts  []string
			}{
				label: "publish.ordered_root_delta_group.span_native.triage." + route + "." + field,
				alts:  []string{"treedb.publish.ordered_root_delta_group.span_native.triage.route." + route + "." + field},
			})
		}
	}
	var sb strings.Builder
	for _, inst := range instances {
		if inst == nil || inst.Wrapper == nil {
			continue
		}
		dbName := inst.Wrapper.Name()
		stats := treeStats[dbName]
		if len(stats) == 0 {
			continue
		}
		var dbSB strings.Builder
		foundSelectedStat := false
		if mmapReadPrefix := treeDBMmapReadStatSource(stats); mmapReadPrefix != "" {
			for _, def := range treeDBMmapReadStatDefs {
				if value, ok := stats[mmapReadPrefix+def.suffix]; ok {
					dbSB.WriteString("  ")
					dbSB.WriteString(def.label)
					dbSB.WriteString(": ")
					dbSB.WriteString(value)
					dbSB.WriteByte('\n')
					foundSelectedStat = true
				}
			}
		}
		for _, key := range keys {
			for _, alt := range key.alts {
				if value, ok := stats[alt]; ok {
					dbSB.WriteString("  ")
					dbSB.WriteString(key.label)
					dbSB.WriteString(": ")
					dbSB.WriteString(value)
					dbSB.WriteByte('\n')
					foundSelectedStat = true
					break
				}
			}
		}
		if !foundSelectedStat {
			continue
		}
		sb.WriteString(dbName)
		sb.WriteString(":\n")
		sb.WriteString(dbSB.String())
	}
	return strings.TrimSpace(sb.String())
}

func renderMarkdownSweep(runs []BenchRun) string {
	if len(runs) == 0 {
		return ""
	}

	dbNames := make([]string, 0, len(runs[0].Instances))
	for _, inst := range runs[0].Instances {
		dbNames = append(dbNames, inst.Wrapper.Name())
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench sweep\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatKeyCounts(runs)))
	if strings.TrimSpace(runs[0].Config.KeyShape) != "" {
		sb.WriteString(fmt.Sprintf("- key-shape: %s\n", strings.TrimSpace(runs[0].Config.KeyShape)))
	}
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", runs[0].Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", runs[0].Config.BatchSize))
	sb.WriteString(fmt.Sprintf("- range-queries: %d\n", runs[0].Config.RangeQueries))
	sb.WriteString(fmt.Sprintf("- range-span: %d\n", runs[0].Config.RangeSpan))
	if contains(runs[0].TestOrder, "batch_delete_range") {
		sb.WriteString(fmt.Sprintf("- batch-delete-range-width: %d\n", runs[0].Config.BatchDeleteRangeWidth))
		sb.WriteString(fmt.Sprintf("- batch-delete-ranges-per-batch: %d\n", runs[0].Config.BatchDeleteRangesPerBatch))
		sb.WriteString(fmt.Sprintf("- batch-delete-range-validate: %t\n", runs[0].Config.BatchDeleteRangeValidate))
		sb.WriteString(fmt.Sprintf("- batch-delete-range-refill: %t\n", runs[0].Config.BatchDeleteRangeRefill))
	}
	if strings.TrimSpace(runs[0].Config.ValuePattern) != "" {
		sb.WriteString(fmt.Sprintf("- val-pattern: %s\n", strings.TrimSpace(runs[0].Config.ValuePattern)))
		sb.WriteString(fmt.Sprintf("- val-pool-size: %d\n", runs[0].Config.ValuePoolSize))
	}
	if strings.TrimSpace(runs[0].Config.Profile) != "" {
		sb.WriteString(fmt.Sprintf("- profile: %s\n", strings.TrimSpace(runs[0].Config.Profile)))
	}
	if strings.TrimSpace(runs[0].Config.DBsArg) != "" {
		sb.WriteString(fmt.Sprintf("- dbs: %s\n", strings.TrimSpace(runs[0].Config.DBsArg)))
	}
	if strings.TrimSpace(runs[0].Config.TestsArg) != "" {
		sb.WriteString(fmt.Sprintf("- tests: %s\n", strings.TrimSpace(runs[0].Config.TestsArg)))
	}
	sb.WriteString("\n")

	if hasInstance(runs[0].Instances, "treedb") {
		if text, err := treeDBResolvedOptionsText(""); err == nil && strings.TrimSpace(text) != "" {
			sb.WriteString("## Resolved TreeDB Options\n\n")
			sb.WriteString("```text\n")
			sb.WriteString(text)
			sb.WriteString("\n```\n\n")
		}
	}

	for _, testName := range runs[0].TestOrder {
		sb.WriteString("## ")
		sb.WriteString(runs[0].DisplayNames[testName])
		sb.WriteString("\n\n")
		sb.WriteString(renderMarkdownTestSweep(testName, runs, dbNames))
		sb.WriteString("\n")
	}

	anyBatchDeleteRange := false
	for _, run := range runs {
		if strings.TrimSpace(renderBatchDeleteRangeReportsString(run.Instances, run.BatchDeleteRange)) != "" {
			anyBatchDeleteRange = true
			break
		}
	}
	if anyBatchDeleteRange {
		sb.WriteString("## Batch DeleteRange Metrics\n\n")
		for _, run := range runs {
			details := strings.TrimSpace(renderBatchDeleteRangeReportsString(run.Instances, run.BatchDeleteRange))
			if details == "" {
				continue
			}
			sb.WriteString(fmt.Sprintf("keys=%s\n\n", formatInt(run.Config.Keys)))
			sb.WriteString("```text\n")
			sb.WriteString(details)
			sb.WriteString("\n```\n\n")
		}
	}

	// Best-effort disk usage reporting (end-of-run sizes) for all DBs.
	anyDisk := false
	for _, run := range runs {
		if len(run.TreeDBDiskUsage) > 0 || len(run.DiskUsage) > 0 {
			anyDisk = true
			break
		}
	}
	if anyDisk {
		sb.WriteString("## Disk Usage (End of Run)\n\n")
		for _, run := range runs {
			if len(run.TreeDBDiskUsage) == 0 && len(run.DiskUsage) == 0 {
				continue
			}
			sb.WriteString(fmt.Sprintf("keys=%s\n\n", formatInt(run.Config.Keys)))
			sb.WriteString("```text\n")
			if len(run.TreeDBDiskUsage) > 0 {
				sb.WriteString(renderTreeDBDiskUsageString(run.TreeDBDiskUsage))
				if other := renderNonTreeDBDiskUsageString(run.DiskUsage, run.TreeDBDiskUsage); strings.TrimSpace(other) != "" {
					sb.WriteByte('\n')
					sb.WriteString("Other DBs:\n")
					for _, line := range strings.Split(strings.TrimSpace(other), "\n") {
						if strings.TrimSpace(line) == "" {
							continue
						}
						sb.WriteString("  ")
						sb.WriteString(line)
						sb.WriteByte('\n')
					}
				}
			} else {
				sb.WriteString(renderDirDiskUsageString(run.DiskUsage))
			}
			sb.WriteString("```\n\n")
		}
	}

	anyRewrite := false
	for _, run := range runs {
		if len(run.TreeDBVlogRewrite) > 0 {
			anyRewrite = true
			break
		}
	}
	if anyRewrite {
		sb.WriteString("## TreeDB CompactStorage (After Run)\n\n")
		for _, run := range runs {
			if len(run.TreeDBVlogRewrite) == 0 {
				continue
			}
			sb.WriteString(fmt.Sprintf("keys=%s\n\n", formatInt(run.Config.Keys)))
			sb.WriteString("```text\n")
			sb.WriteString(renderTreeDBVlogRewriteString(run.TreeDBVlogRewrite))
			sb.WriteString("```\n\n")
		}
	}

	anyPerf := false
	for _, run := range runs {
		if strings.TrimSpace(renderTreeDBPerfString(run.Instances, run.TestOrder, run.DisplayNames, run.TreeDBPerf)) != "" {
			anyPerf = true
			break
		}
	}
	if anyPerf {
		sb.WriteString("## TreeDB Perf Instrumentation\n\n")
		for _, run := range runs {
			perf := strings.TrimSpace(renderTreeDBPerfString(run.Instances, run.TestOrder, run.DisplayNames, run.TreeDBPerf))
			if perf == "" {
				continue
			}
			sb.WriteString(fmt.Sprintf("keys=%s\n\n", formatInt(run.Config.Keys)))
			sb.WriteString("```text\n")
			sb.WriteString(perf)
			sb.WriteString("\n```\n\n")
		}
	}

	anyCodecStats := false
	for _, run := range runs {
		if strings.TrimSpace(renderTreeDBVlogCodecSummaryString(run.Instances, run.TreeDBStats)) != "" {
			anyCodecStats = true
			break
		}
	}
	if anyCodecStats {
		sb.WriteString("## TreeDB Value-Log Codec Summary (End of Run)\n\n")
		for _, run := range runs {
			codecStats := strings.TrimSpace(renderTreeDBVlogCodecSummaryString(run.Instances, run.TreeDBStats))
			if codecStats == "" {
				continue
			}
			sb.WriteString(fmt.Sprintf("keys=%s\n\n", formatInt(run.Config.Keys)))
			sb.WriteString("```text\n")
			sb.WriteString(codecStats)
			sb.WriteString("\n```\n\n")
		}
	}

	anyTreeStats := false
	for _, run := range runs {
		if strings.TrimSpace(renderTreeDBSelectedStatsString(run.Instances, run.TreeDBStats)) != "" {
			anyTreeStats = true
			break
		}
	}
	if anyTreeStats {
		sb.WriteString("## TreeDB Selected Stats (End of Run)\n\n")
		for _, run := range runs {
			stats := strings.TrimSpace(renderTreeDBSelectedStatsString(run.Instances, run.TreeDBStats))
			if stats == "" {
				continue
			}
			sb.WriteString(fmt.Sprintf("keys=%s\n\n", formatInt(run.Config.Keys)))
			sb.WriteString("```text\n")
			sb.WriteString(stats)
			sb.WriteString("\n```\n\n")
		}
	}

	anyKept := false
	for _, run := range runs {
		if !run.Config.KeepDir {
			continue
		}
		if strings.TrimSpace(renderKeptDirsString(run.Instances)) != "" {
			anyKept = true
			break
		}
	}
	if anyKept {
		sb.WriteString("## Kept Data Directories\n\n")
		for _, run := range runs {
			if !run.Config.KeepDir {
				continue
			}
			kept := strings.TrimSpace(renderKeptDirsString(run.Instances))
			if kept == "" {
				continue
			}
			sb.WriteString(fmt.Sprintf("keys=%s\n\n", formatInt(run.Config.Keys)))
			sb.WriteString("```text\n")
			sb.WriteString(kept)
			sb.WriteString("\n```\n\n")
		}
	}

	return sb.String()
}

func formatKeyCounts(runs []BenchRun) string {
	parts := make([]string, 0, len(runs))
	for _, r := range runs {
		parts = append(parts, formatInt(r.Config.Keys))
	}
	return strings.Join(parts, ", ")
}

func renderMarkdownTestSweep(testName string, runs []BenchRun, dbNames []string) string {
	var sb strings.Builder

	sb.WriteString("| keys |")
	for _, db := range dbNames {
		sb.WriteString(" ")
		sb.WriteString(db)
		sb.WriteString(" |")
	}
	sb.WriteString("\n")

	sb.WriteString("|---:|")
	for range dbNames {
		sb.WriteString("---:|")
	}
	sb.WriteString("\n")

	for _, run := range runs {
		sb.WriteString("| ")
		sb.WriteString(formatInt(run.Config.Keys))
		sb.WriteString(" |")

		maxVal := math.NaN()
		for _, db := range dbNames {
			v := run.Results[testName][db]
			if math.IsNaN(v) {
				continue
			}
			if math.IsNaN(maxVal) || v > maxVal {
				maxVal = v
			}
		}

		for _, db := range dbNames {
			v := run.Results[testName][db]
			cell := formatMarkdownValue(v)
			if !math.IsNaN(maxVal) && !math.IsNaN(v) && v == maxVal {
				cell = "**" + cell + "**"
			}
			sb.WriteString(" ")
			sb.WriteString(cell)
			sb.WriteString(" |")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func formatMarkdownValue(f float64) string {
	if math.IsNaN(f) {
		return "—"
	}
	return formatFloat(f)
}

func formatInt(v int) string { return formatFloat(float64(v)) }

func runGethHotKVSuite(baseCfg BenchConfig) (string, error) {
	// Small geth/Nitro-like hot-KV proxy matching the #2392 comparison shape:
	// sequential point writes, point reads, full ordered iteration, then dense DeleteRange.
	// TreeDB uses the public cached command-WAL path by default so the workload is
	// closer to downstream geth/Nitro persistence than the raw cached benchmark.
	cfg := baseCfg
	cfg.Progress = false
	if !flagExplicit("keys") {
		cfg.Keys = 30_000
	}
	if !flagExplicit("dbs") {
		cfg.DBsArg = "treedb_public_command_wal,pebble,leveldb"
	}
	if !flagExplicit("test") {
		cfg.TestsArg = "sequential_write,random_read,full_scan,batch_delete_range"
	}
	if !flagExplicit("val-pattern") {
		cfg.ValuePattern = "random"
	}
	if !flagExplicit("batch-delete-range-width") {
		cfg.BatchDeleteRangeWidth = 100
	}
	if !flagExplicit("batch-delete-ranges-per-batch") {
		cfg.BatchDeleteRangesPerBatch = 100
	}
	if !flagExplicit("batch-delete-range-validate") {
		cfg.BatchDeleteRangeValidate = true
	}
	if !flagExplicit("read-require-hit") {
		cfg.ReadRequireHit = true
	}

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	if maybeWriteBenchprofArtifacts(*profileDir, []BenchRun{run}) {
		runBenchprof(*profileDir)
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: geth_hot_kv\n\n")
	sb.WriteString("This is a small raw-KV proxy for geth/Nitro hot database behavior: sequential point writes, random point reads, full ordered iteration, then dense `DeleteRange`. It is not a full geth node sync benchmark.\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- val-pattern: %s\n", run.Config.ValuePattern))
	sb.WriteString(fmt.Sprintf("- dbs: %s\n", run.Config.DBsArg))
	sb.WriteString(fmt.Sprintf("- tests: %s\n", run.Config.TestsArg))
	sb.WriteString(fmt.Sprintf("- batch-delete-range-width: %d\n", run.Config.BatchDeleteRangeWidth))
	sb.WriteString(fmt.Sprintf("- batch-delete-ranges-per-batch: %d\n", run.Config.BatchDeleteRangesPerBatch))
	sb.WriteString(fmt.Sprintf("- seed: %d\n\n", run.Config.SeedUsed))
	sb.WriteString("## Compact summary\n\n")
	sb.WriteString("`write ops/sec` uses `sequential_write` when present, falling back to `batch_write` for custom test overrides. `DeleteRange keys/sec` uses affected keys/sec, not range calls/sec. `size bytes` is end-of-run directory usage after the DeleteRange phase has closed the DB.\n\n")
	sb.WriteString(renderGethHotKVSummary(run))
	sb.WriteString("\n## Full unified_bench report\n\n")
	sb.WriteString(renderMarkdownSingle(run))
	return sb.String(), nil
}

func renderGethHotKVSummary(run BenchRun) string {
	var sb strings.Builder
	sb.WriteString("| engine | write ops/sec | read ops/sec | iterate keys/sec | DeleteRange keys/sec | size bytes |\n")
	sb.WriteString("|---|---:|---:|---:|---:|---:|\n")
	for _, inst := range run.Instances {
		if inst == nil || inst.Wrapper == nil {
			continue
		}
		name := inst.Wrapper.Name()
		writeOps := math.NaN()
		if byDB := run.Results["sequential_write"]; byDB != nil {
			writeOps = byDB[name]
		}
		if math.IsNaN(writeOps) {
			if byDB := run.Results["batch_write"]; byDB != nil {
				writeOps = byDB[name]
			}
		}
		readOps := math.NaN()
		if byDB := run.Results["random_read"]; byDB != nil {
			readOps = byDB[name]
		}
		iterOps := math.NaN()
		if byDB := run.Results["full_scan"]; byDB != nil {
			iterOps = byDB[name]
		}
		deleteKeysOps := math.NaN()
		if byDB := run.BatchDeleteRange["batch_delete_range"]; byDB != nil {
			if report, ok := byDB[name]; ok {
				deleteKeysOps = report.AffectedKeysPerSec
			}
		}
		sizeBytes := math.NaN()
		if usage, ok := run.DiskUsage[name]; ok {
			sizeBytes = float64(usage.TotalBytes)
		}
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s |\n",
			gethHotKVSummaryEngineName(name),
			formatMarkdownValue(writeOps),
			formatMarkdownValue(readOps),
			formatMarkdownValue(iterOps),
			formatMarkdownValue(deleteKeysOps),
			formatMarkdownValue(sizeBytes),
		))
	}
	return sb.String()
}

func gethHotKVSummaryEngineName(name string) string {
	if name == "TreeDB (public cached command_wal_v1)" {
		return "TreeDB"
	}
	return name
}

func runReadmeSuite(baseCfg BenchConfig) (string, error) {
	outDir := strings.TrimSpace(*outDirArg)

	keyCounts := []int{100000, 1000000}
	if strings.TrimSpace(*keyCountsArg) != "" || strings.TrimSpace(*keyScaleArg) != "" {
		var err error
		keyCounts, err = resolveKeyCounts(baseCfg.Keys, *keyCountsArg, *keyScaleArg, *keysMin, *keysMax)
		if err != nil {
			return "", err
		}
	}

	pointCfg := baseCfg
	pointCfg.DBsArg = "hashdb,treedb,badger,leveldb"
	pointCfg.TestsArg = "sequential_write,random_write,random_read"
	pointCfg.Progress = false

	scanCfg := baseCfg
	scanCfg.DBsArg = "treedb,badger,leveldb"
	scanCfg.TestsArg = "batch_write,full_scan,prefix_scan"
	scanCfg.Progress = false

	pointRuns, err := runSweep(pointCfg, keyCounts)
	if err != nil {
		return "", err
	}
	scanRuns, err := runSweep(scanCfg, keyCounts)
	if err != nil {
		return "", err
	}

	generatedAt := time.Now().UTC()
	env := getHostInfo()

	var (
		pointOpsPlotPath  string
		batchScanPlotPath string
	)
	if outDir != "" {
		pointOpsPlotPath, batchScanPlotPath, err = writeReadmePlots(outDir, pointRuns, scanRuns)
		if err != nil {
			return "", err
		}
	}

	var sb strings.Builder
	sb.WriteString("_Generated by `go run ./cmd/unified_bench -suite readme -format markdown`._\n\n")
	sb.WriteString(fmt.Sprintf("_Generated at:_ %s\n", generatedAt.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("_Environment:_ %s\n", env.MarkdownSummary()))
	sb.WriteString(fmt.Sprintf("_Seed:_ %d\n\n", baseCfg.SeedUsed))
	sb.WriteString(fmt.Sprintf("_Key counts:_ %s (valsize=%d, batchsize=%d, range-queries=%d, range-span=%d)\n\n",
		formatInt(keyCounts[0])+"…"+formatInt(keyCounts[len(keyCounts)-1]),
		baseCfg.ValueSize, baseCfg.BatchSize, baseCfg.RangeQueries, baseCfg.RangeSpan))

	sb.WriteString("Notes:\n")
	sb.WriteString("- Results depend on hardware and OS.\n")
	sb.WriteString("- `HashDB` does not support ordered scans.\n\n")

	if pointOpsPlotPath != "" && batchScanPlotPath != "" {
		sb.WriteString("### Graphs\n\n")
		sb.WriteString(fmt.Sprintf("![Unified bench: point ops scaling](%s)\n\n", pointOpsPlotPath))
		sb.WriteString(fmt.Sprintf("![Unified bench: batch + scans scaling](%s)\n\n", batchScanPlotPath))
	}

	sb.WriteString("### Point Ops (writes + gets)\n\n")
	sb.WriteString(renderMarkdownSuiteSection(pointRuns))

	sb.WriteString("\n### Batch + Scans\n\n")
	sb.WriteString(renderMarkdownSuiteSection(scanRuns))

	sb.WriteString("\n### Quick takeaways\n\n")
	sb.WriteString("- `HashDB`: great for high-throughput point reads/writes; no ordered scan API yet.\n")
	sb.WriteString("- `TreeDB` (cached): strong default for random-write-heavy workloads; scans include merge overhead.\n")
	sb.WriteString("- `Badger`/`LevelDB`: useful baselines with different storage tradeoffs.\n")

	return sb.String(), nil
}

func runChurnSuite(baseCfg BenchConfig) (string, error) {
	// Default churn suite parameters (override via regular flags like -keys, -valsize).
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb,leveldb"
	cfg.TestsArg = "random_write,random_delete,random_write,full_scan,prefix_scan"
	cfg.SettleBeforeScans = true

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	return renderMarkdownSingle(run), nil
}

func runChurnVacuumSuite(baseCfg BenchConfig) (string, error) {
	// Churn + settled scans, then VACUUM and scan again on the same dataset.
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb_bench_unsafe,leveldb"
	cfg.TestsArg = "random_write,random_delete,random_write,full_scan,prefix_scan,vacuum_index,full_scan2,prefix_scan2"
	cfg.SettleBeforeScans = true

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	return renderMarkdownSingle(run), nil
}

func runFlushThrashSuite(baseCfg BenchConfig) (string, error) {
	// Stress cached-mode flush batching by forcing many small flushes.
	// This suite exists to catch regressions where a small flush threshold causes
	// runaway memory usage or severe commit thrash.
	cfg := baseCfg
	cfg.Progress = false
	cfg.KeepDir = true
	cfg.DBsArg = "treedb,leveldb"
	cfg.TestsArg = "random_write,batch_write"

	// If the caller didn't specify -keys (default is 100k), use a larger default
	// so the suite exercises long-run behavior. Tests can override cfg.Keys.
	if cfg.Keys == 100_000 {
		cfg.Keys = 5_000_000
	}

	const thrashFlushThresholdBytes = 6_108_864
	prevFlush := *treedbFlushThreshold
	*treedbFlushThreshold = thrashFlushThresholdBytes
	defer func() { *treedbFlushThreshold = prevFlush }()

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}

	diag, diagErr := suiteTreeDBCacheStats(run.Instances)
	cleanErr := suiteCleanupDirs(run.Instances)
	if diagErr != nil {
		return "", diagErr
	}
	if cleanErr != nil {
		return "", cleanErr
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: flushthrash\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", run.Config.BatchSize))
	sb.WriteString(fmt.Sprintf("- treedb-flush-threshold: %d\n", thrashFlushThresholdBytes))
	sb.WriteString("\n")

	sb.WriteString("```text\n")
	table, _, _, _ := renderResultsTableStringWithLayout(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
	sb.WriteString(table)
	sb.WriteString("```\n\n")

	if diag != "" {
		sb.WriteString("## TreeDB cache stats (post-run)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(diag)
		sb.WriteString("```\n")
	}

	return sb.String(), nil
}

func runBigKeysGuardSuite(baseCfg BenchConfig) (string, error) {
	// Guardrail suite for the prior failure mode where small FlushThreshold +
	// big keycount led to stalls or runaway memory/backlog.
	cfg := baseCfg
	cfg.Progress = false
	cfg.KeepDir = true
	cfg.DBsArg = "treedb"
	cfg.TestsArg = "random_write,batch_write"

	// If the caller didn't specify -keys (default is 100k), use a larger default.
	if cfg.Keys == 100_000 {
		cfg.Keys = 1_000_000
	}

	// Ensure the suite never runs unbounded even if the caller forgets to set
	// -max-wall (CI should still set it explicitly).
	if cfg.MaxWall == 0 {
		cfg.MaxWall = 15 * time.Minute
	}

	const guardFlushThresholdBytes = 6_108_864
	prevFlush := *treedbFlushThreshold
	*treedbFlushThreshold = guardFlushThresholdBytes
	defer func() { *treedbFlushThreshold = prevFlush }()

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}

	diag, diagErr := suiteTreeDBCacheStats(run.Instances)
	cleanErr := suiteCleanupDirs(run.Instances)
	if diagErr != nil {
		return "", diagErr
	}
	if cleanErr != nil {
		return "", cleanErr
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: bigkeys_guard\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", run.Config.BatchSize))
	sb.WriteString(fmt.Sprintf("- treedb-flush-threshold: %d\n", guardFlushThresholdBytes))
	if run.Config.MaxWall > 0 {
		sb.WriteString(fmt.Sprintf("- max-wall: %s\n", run.Config.MaxWall))
	}
	if run.Config.MaxRSSMB > 0 {
		sb.WriteString(fmt.Sprintf("- max-rss-mb: %d\n", run.Config.MaxRSSMB))
	}
	sb.WriteString("\n")

	sb.WriteString("```text\n")
	table, _, _, _ := renderResultsTableStringWithLayout(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
	sb.WriteString(table)
	sb.WriteString("```\n\n")

	if diag != "" {
		sb.WriteString("## TreeDB cache stats (post-run)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(diag)
		sb.WriteString("```\n")
	}

	return sb.String(), nil
}

func runLongMixSuite(baseCfg BenchConfig) (string, error) {
	// Long-ish mixed workload with an explicit settle boundary plus fragmentation
	// reports before/after settle to make scan regressions diagnosable.
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb,leveldb"
	cfg.TestsArg = "random_write,random_delete,random_write,fragmentation_report_pre,fragmentation_report_post,full_scan,prefix_scan"
	cfg.SettleBeforeScans = true

	// If the caller didn't specify -keys (default is 100k), use a larger default.
	if cfg.Keys == 100_000 {
		cfg.Keys = 1_000_000
	}
	// Prefer pointer values so the workload exercises the value-log path.
	if cfg.ValueSize < 2048 {
		cfg.ValueSize = 2048
	}

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: longmix\n\n")
	sb.WriteString(renderMarkdownSingle(run))
	return sb.String(), nil
}

func runSloadReadHeavySuite(baseCfg BenchConfig) (string, error) {
	// Read-heavy suite intended to approximate base-bench's "sload-readheavy"
	// access pattern: random 32-byte keys, mixed write batches (CommitSync), and
	// settled point reads (close+reopen before reads).
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb,leveldb"
	cfg.TestsArg = "dataset_write_random,dataset_update_fork_choice,dataset_read_random"
	cfg.SettleBeforeScans = true

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	return renderMarkdownSingle(run), nil
}

func suiteTreeDBCacheStats(instances []*DBInstance) (string, error) {
	var sb strings.Builder
	for _, inst := range instances {
		if inst == nil || inst.Dir == "" || !isTreeDBInstance(inst) {
			continue
		}
		factory, err := GetDBFactory(inst.Name)
		if err != nil {
			return "", fmt.Errorf("suite: reopen treedb for stats (%s): %w", inst.Name, err)
		}
		db, err := factory(inst.Dir)
		if err != nil {
			return "", fmt.Errorf("suite: reopen treedb for stats (%s): %w", inst.Name, err)
		}
		sp, ok := db.(kvstore.StatsProvider)
		if ok {
			stats := sp.Stats()
			keys := make([]string, 0, len(stats))
			for k := range stats {
				if strings.HasPrefix(k, "treedb.cache.") {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			for _, k := range keys {
				sb.WriteString(k)
				sb.WriteString("=")
				sb.WriteString(stats[k])
				sb.WriteString("\n")
			}
		}
		_ = db.Close()
	}
	return sb.String(), nil
}

func suiteCleanupDirs(instances []*DBInstance) error {
	for _, inst := range instances {
		if inst == nil {
			continue
		}
		if err := os.RemoveAll(inst.Dir); err != nil {
			return err
		}
	}
	return nil
}

func renderMarkdownSuiteSection(runs []BenchRun) string {
	if len(runs) == 0 {
		return ""
	}

	dbNames := make([]string, 0, len(runs[0].Instances))
	for _, inst := range runs[0].Instances {
		dbNames = append(dbNames, inst.Wrapper.Name())
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("_Key counts:_ %s\n\n", formatKeyCounts(runs)))

	for _, testName := range runs[0].TestOrder {
		sb.WriteString("#### ")
		sb.WriteString(runs[0].DisplayNames[testName])
		sb.WriteString("\n\n")
		sb.WriteString(renderMarkdownTestSweep(testName, runs, dbNames))
		sb.WriteString("\n")
	}
	return sb.String()
}

func renderMarkdownBaseline(run BenchRun) string {
	if len(run.Instances) == 0 {
		return ""
	}
	dbName := run.Instances[0].Wrapper.Name()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("_Key count:_ %s\n\n", formatInt(run.Config.Keys)))
	sb.WriteString("| Test | ")
	sb.WriteString(dbName)
	sb.WriteString(" |\n")
	sb.WriteString("|---|---:|\n")
	for _, testName := range run.TestOrder {
		sb.WriteString("| ")
		sb.WriteString(run.DisplayNames[testName])
		sb.WriteString(" | ")
		sb.WriteString(formatMarkdownValue(run.Results[testName][dbName]))
		sb.WriteString(" |\n")
	}
	return sb.String()
}

func testSeed(seed int64, testName string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(testName))
	return seed ^ int64(h.Sum64())
}

func printResultsTable(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, results map[string]map[string]float64) {
	// Dynamically determine column widths based on content

	colNames := []string{"Test"}
	for _, inst := range instances {
		colNames = append(colNames, inst.Wrapper.Name())
	}

	colWidths := make(map[string]int)
	for _, colName := range colNames {
		colWidths[colName] = len(colName) // Start with header length
	}

	// Update widths based on test names (using display names)
	for _, testName := range finalTestOrder {
		dispName := displayNames[testName]
		if len(dispName) > colWidths["Test"] {
			colWidths["Test"] = len(dispName)
		}
	}

	// Update widths based on results (or "-" for not-yet-run)
	for _, testName := range finalTestOrder {
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			valStr := formatFloat(results[testName][dbName])
			if len(valStr) > colWidths[dbName] {
				colWidths[dbName] = len(valStr)
			}
		}
	}

	// Print Header
	headerRow := fmt.Sprintf("%*s", colWidths["Test"], "Test")
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		headerRow += fmt.Sprintf("  %*s", colWidths[dbName], dbName) // Right-align DB names for consistency with data
	}
	fmt.Println(headerRow)

	// Print Separator
	separatorRow := fmt.Sprintf("%*s", colWidths["Test"], strings.Repeat("-", colWidths["Test"]))
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		separatorRow += fmt.Sprintf("  %*s", colWidths[dbName], strings.Repeat("-", colWidths[dbName]))
	}
	fmt.Println(separatorRow)

	// Print Data Rows
	for _, testName := range finalTestOrder {
		dataRow := fmt.Sprintf("%*s", colWidths["Test"], displayNames[testName])
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			val := results[testName][dbName]
			dataRow += fmt.Sprintf("  %*s", colWidths[dbName], formatFloat(val)) // Right-align values
		}
		fmt.Println(dataRow)
	}
}

func renderCheckpointDurationsTableString(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, durs map[string]map[string]time.Duration) string {

	rows := make([]string, 0, len(finalTestOrder))
	inOrder := make(map[string]struct{}, len(finalTestOrder))
	for _, testName := range finalTestOrder {
		inOrder[testName] = struct{}{}
		if _, ok := durs[testName]; ok {

			rows = append(rows, testName)
		}
	}
	extra := make([]string, 0, len(durs))
	for testName := range durs {
		if _, ok := inOrder[testName]; ok {
			continue
		}
		extra = append(extra, testName)
	}
	sort.Strings(extra)
	rows = append(rows, extra...)
	if len(rows) == 0 {
		return ""
	}

	colNames := []string{"Before Test"}
	for _, inst := range instances {
		colNames = append(colNames, inst.Wrapper.Name())
	}

	colWidths := make(map[string]int, len(colNames))
	for _, colName := range colNames {
		colWidths[colName] = len(colName)
	}

	for _, testName := range rows {
		disp := displayNames[testName]
		if disp == "" {
			disp = testName
		}
		if len(disp) > colWidths["Before Test"] {
			colWidths["Before Test"] = len(disp)
		}
	}

	for _, testName := range rows {
		perDB := durs[testName]
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			cell := "-"
			if perDB != nil {
				if d, ok := perDB[dbName]; ok {
					cell = formatDuration(d)
				}
			}
			if len(cell) > colWidths[dbName] {
				colWidths[dbName] = len(cell)
			}
		}
	}

	var sb strings.Builder

	headerRow := fmt.Sprintf("%*s", colWidths["Before Test"], "Before Test")
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		headerRow += fmt.Sprintf("  %*s", colWidths[dbName], dbName)
	}
	sb.WriteString(headerRow)
	sb.WriteString("\n")

	separatorRow := fmt.Sprintf("%*s", colWidths["Before Test"], strings.Repeat("-", colWidths["Before Test"]))
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		separatorRow += fmt.Sprintf("  %*s", colWidths[dbName], strings.Repeat("-", colWidths[dbName]))
	}
	sb.WriteString(separatorRow)
	sb.WriteString("\n")

	for _, testName := range rows {
		disp := displayNames[testName]
		if disp == "" {
			disp = testName
		}
		dataRow := fmt.Sprintf("%*s", colWidths["Before Test"], disp)
		perDB := durs[testName]
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			cell := "-"
			if perDB != nil {
				if d, ok := perDB[dbName]; ok {
					cell = formatDuration(d)
				}
			}
			dataRow += fmt.Sprintf("  %*s", colWidths[dbName], cell)
		}
		sb.WriteString(dataRow)
		sb.WriteString("\n")
	}

	return sb.String()
}

func printCheckpointDurationsTable(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, durs map[string]map[string]time.Duration) {
	table := renderCheckpointDurationsTableString(instances, finalTestOrder, displayNames, durs)
	if table == "" {
		return
	}
	title := "Checkpoint Time (Between Tests)"
	if _, ok := durs[checkpointPostRunLabel]; ok {
		title = "Checkpoint Time (Between Tests + Post-run)"
	}
	fmt.Println(title)
	fmt.Print(table)
}

func renderVacuumDurationsTableString(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, durs map[string]map[string]time.Duration) string {
	// Same shape/meaning as checkpoint durations: a per-test row, per-DB duration.
	return renderCheckpointDurationsTableString(instances, finalTestOrder, displayNames, durs)
}

func printVacuumDurationsTable(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, durs map[string]map[string]time.Duration) {
	table := renderVacuumDurationsTableString(instances, finalTestOrder, displayNames, durs)
	if table == "" {
		return
	}
	fmt.Println("Vacuum Time (Between Tests)")
	fmt.Print(table)
}

func renderVacuumIndexBytesTableString(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, bytes map[string]map[string][2]uint64) string {
	rows := make([]string, 0, len(finalTestOrder))
	for _, testName := range finalTestOrder {
		if _, ok := bytes[testName]; ok {
			rows = append(rows, testName)
		}
	}
	if len(rows) == 0 {
		return ""
	}

	colNames := []string{"Before Test"}
	for _, inst := range instances {
		colNames = append(colNames, inst.Wrapper.Name())
	}

	colWidths := make(map[string]int, len(colNames))
	for _, colName := range colNames {
		colWidths[colName] = len(colName)
	}

	for _, testName := range rows {
		disp := displayNames[testName]
		if len(disp) > colWidths["Before Test"] {
			colWidths["Before Test"] = len(disp)
		}
	}

	for _, testName := range rows {
		perDB := bytes[testName]
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			cell := "-"
			if perDB != nil {
				if pair, ok := perDB[dbName]; ok {
					before, after := pair[0], pair[1]
					if before != 0 || after != 0 {
						cell = fmt.Sprintf("%s -> %s", formatBytes(before), formatBytes(after))
					}
				}
			}
			if len(cell) > colWidths[dbName] {
				colWidths[dbName] = len(cell)
			}
		}
	}

	var sb strings.Builder
	headerRow := fmt.Sprintf("%*s", colWidths["Before Test"], "Before Test")
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		headerRow += fmt.Sprintf("  %*s", colWidths[dbName], dbName)
	}
	sb.WriteString(headerRow)
	sb.WriteString("\n")

	separatorRow := fmt.Sprintf("%*s", colWidths["Before Test"], strings.Repeat("-", colWidths["Before Test"]))
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		separatorRow += fmt.Sprintf("  %*s", colWidths[dbName], strings.Repeat("-", colWidths[dbName]))
	}
	sb.WriteString(separatorRow)
	sb.WriteString("\n")

	for _, testName := range rows {
		dataRow := fmt.Sprintf("%*s", colWidths["Before Test"], displayNames[testName])
		perDB := bytes[testName]
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			cell := "-"
			if perDB != nil {
				if pair, ok := perDB[dbName]; ok {
					before, after := pair[0], pair[1]
					if before != 0 || after != 0 {
						cell = fmt.Sprintf("%s -> %s", formatBytes(before), formatBytes(after))
					}
				}
			}
			dataRow += fmt.Sprintf("  %*s", colWidths[dbName], cell)
		}
		sb.WriteString(dataRow)
		sb.WriteString("\n")
	}

	return sb.String()
}

func printVacuumIndexBytesTable(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, bytes map[string]map[string][2]uint64) {
	table := renderVacuumIndexBytesTableString(instances, finalTestOrder, displayNames, bytes)
	if table == "" {
		return
	}
	fmt.Println("Vacuum Index Bytes (Between Tests)")
	fmt.Print(table)
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	if d >= time.Millisecond {
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%dµs", d.Microseconds())
}

func parseCheckpointSettleBeforeTests(arg string) (map[string]struct{}, bool) {
	parts := parseList(arg)
	if len(parts) == 0 {
		return nil, false
	}
	out := make(map[string]struct{}, len(parts))
	all := false
	for _, raw := range parts {
		if raw == "" {
			continue
		}
		switch raw {
		case "all", "*":
			all = true
			continue
		case "after_run", "after-run", "postrun", "post_run":
			raw = checkpointPostRunLabel
		}
		if raw != checkpointPostRunLabel {
			normalized := normalizeTests([]string{raw})
			if len(normalized) == 0 {
				continue
			}
			raw = normalized[0]
		}
		out[raw] = struct{}{}
	}
	if len(out) == 0 {
		out = nil
	}
	return out, all
}

func parseList(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.ToLower(strings.TrimSpace(parts[i]))
	}
	return parts
}

func normalizeTests(list []string) []string {
	seen := make(map[string]struct{}, len(list))
	out := make([]string, 0, len(list))
	for _, t := range list {
		switch t {
		case "full_scan":
			// keep
		case "scan":
			t = "full_scan"
		case "range_scan":
			t = "prefix_scan"
		case "prefix_scan":
			// keep
		case "forkchoice":
			t = "update_fork_choice"
		case "write_seq":
			t = "sequential_write"
		case "write_rand":
			t = "random_write"
		case "write_sorted":
			t = "dataset_write_sorted"
		case "write_dataset":
			t = "dataset_write_random"
		case "read_rand":
			t = "random_read"
		case "read_rand_parallel":
			t = "random_read_parallel"
		case "read_rand_batch", "read_random_batch":
			t = "random_read_batch"
		case "delete_rand":
			t = "random_delete"
		case "batch_write_random":
			t = "batch_random"
		case "batch_write_small_seq":
			t = "batch_small_seq"
		case "batch_write_ss":
			t = "batch_write_steady"
		case "batch_range_delete", "delete_range":
			t = "batch_delete_range"
		}
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func contains(list []string, item string) bool {
	for _, s := range list {
		if s == item {
			return true
		}
	}
	return false
}

func isSettleBeforeScanTest(testName string) bool {
	switch testName {
	case "full_scan", "prefix_scan",
		"random_read", "random_read_parallel", "random_read_parallel_acquire_snapshot", "random_read_batch",
		"dataset_read_random":
		return true
	default:
		return false
	}
}

func settleBenchInstances(instances []*DBInstance) error {
	for _, inst := range instances {
		if inst == nil || inst.Wrapper == nil {
			continue
		}
		if err := inst.Wrapper.Close(); err != nil {
			return fmt.Errorf("settle/close %s: %w", inst.Name, err)
		}
		factory, err := GetDBFactory(inst.Name)
		if err != nil {
			return err
		}
		newWrapper, err := factory(inst.Dir)
		if err != nil {
			return fmt.Errorf("settle/reopen %s: %w", inst.Name, err)
		}
		inst.Wrapper = newWrapper
	}
	return nil
}

func shouldSettleBeforeCheckpoint(cfg BenchConfig, label string) bool {
	if label == "" {
		return false
	}
	if cfg.CheckpointSettleBeforeAll {
		return true
	}
	if len(cfg.CheckpointSettleBeforeTests) == 0 {
		return false
	}
	_, ok := cfg.CheckpointSettleBeforeTests[label]
	return ok
}

func validateCheckpointSettleBeforeTests(cfg BenchConfig, finalTestOrder []string) error {
	if len(cfg.CheckpointSettleBeforeTests) == 0 {
		return nil
	}
	valid := make(map[string]struct{}, len(finalTestOrder)+1)
	for _, label := range finalTestOrder {
		valid[label] = struct{}{}
	}
	valid[checkpointPostRunLabel] = struct{}{}
	var unknown []string
	for label := range cfg.CheckpointSettleBeforeTests {
		if _, ok := valid[label]; !ok {
			unknown = append(unknown, label)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("unknown checkpoint settle label(s): %s", strings.Join(unknown, ","))
}

func checkpointSettleTimeoutOrDefault(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return defaultCheckpointSettleTimeout
	}
	return timeout
}

func checkpointSettleTimeoutWithGuard(timeout time.Duration, guard *benchGuard) time.Duration {
	timeout = checkpointSettleTimeoutOrDefault(timeout)
	if guard == nil || guard.deadline.IsZero() {
		return timeout
	}
	remaining := time.Until(guard.deadline)
	if remaining <= 0 {
		return time.Nanosecond
	}
	if remaining < timeout {
		return remaining
	}
	return timeout
}

func waitForTreeDBQueueDrainInstance(inst *DBInstance, timeout time.Duration) (time.Duration, bool, error) {
	if inst == nil || inst.Wrapper == nil || !isTreeDBInstance(inst) {
		return 0, false, nil
	}
	sp, ok := inst.Wrapper.(kvstore.StatsProvider)
	if !ok {
		return 0, false, nil
	}
	timeout = checkpointSettleTimeoutOrDefault(timeout)
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		stats := sp.Stats()
		pending := false
		if queueLen, ok := parseStatInt64(stats, "treedb.cache.queue_len"); ok && queueLen > 0 {
			pending = true
		}
		if backlogBytes, ok := parseStatInt64(stats, "treedb.cache.queue_backlog_bytes"); ok && backlogBytes > 0 {
			pending = true
		}
		if active, ok := parseStatInt64(stats, "treedb.cache.flush_apply.coordinator.active"); ok && active > 0 {
			pending = true
		}
		if activeWorkers, ok := parseStatInt64(stats, "treedb.cache.flush_apply.coordinator.active_workers"); ok && activeWorkers > 0 {
			pending = true
		}
		if inFlightBytes, ok := parseStatInt64(stats, "treedb.cache.flush_apply.coordinator.in_flight_bytes"); ok && inFlightBytes > 0 {
			pending = true
		}
		if !pending {
			return time.Since(start), true, nil
		}
		if time.Now().After(deadline) {
			return time.Since(start), true, fmt.Errorf("checkpoint settle timeout: treedb cache queue did not drain within %s", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTreeDBQueueDrain(instances []*DBInstance, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		pending := false
		for _, inst := range instances {
			if inst == nil || inst.Wrapper == nil || !isTreeDBInstance(inst) {
				continue
			}
			sp, ok := inst.Wrapper.(kvstore.StatsProvider)
			if !ok {
				continue
			}
			stats := sp.Stats()
			if len(stats) == 0 {
				continue
			}
			if queueLen, ok := parseStatInt64(stats, "treedb.cache.queue_len"); ok && queueLen > 0 {
				pending = true
				break
			}
			if backlogBytes, ok := parseStatInt64(stats, "treedb.cache.queue_backlog_bytes"); ok && backlogBytes > 0 {
				pending = true
				break
			}
			if active, ok := parseStatInt64(stats, "treedb.cache.flush_apply.coordinator.active"); ok && active > 0 {
				pending = true
				break
			}
			if activeWorkers, ok := parseStatInt64(stats, "treedb.cache.flush_apply.coordinator.active_workers"); ok && activeWorkers > 0 {
				pending = true
				break
			}
			if inFlightBytes, ok := parseStatInt64(stats, "treedb.cache.flush_apply.coordinator.in_flight_bytes"); ok && inFlightBytes > 0 {
				pending = true
				break
			}
		}
		if !pending {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("checkpoint settle timeout: treedb cache queue did not drain within %s", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func containsAny(list []string, items ...string) bool {
	for _, item := range items {
		if contains(list, item) {
			return true
		}
	}
	return false
}

type liveTable struct {
	w              io.Writer
	instances      []*DBInstance
	finalTestOrder []string
	displayNames   map[string]string
	printedLines   int
	enabledVT100   bool
	colWidths      map[string]int
	dbColStart     map[string]int
	testRowIndex   map[string]int
}

func newLiveTable(w io.Writer, instances []*DBInstance, finalTestOrder []string, displayNames map[string]string) *liveTable {
	enabledVT100 := false
	if w == os.Stderr {
		if fi, err := os.Stderr.Stat(); err == nil {
			enabledVT100 = (fi.Mode() & os.ModeCharDevice) != 0
		}
	}

	return &liveTable{
		w:              w,
		instances:      instances,
		finalTestOrder: finalTestOrder,
		displayNames:   displayNames,
		enabledVT100:   enabledVT100,
	}
}

func (t *liveTable) Render(results map[string]map[string]float64) error {
	table, colWidths, dbColStart, testRowIndex := renderResultsTableStringWithLayout(t.instances, t.finalTestOrder, t.displayNames, results)
	lines := 2 + len(t.finalTestOrder) // header + separator + rows

	if t.printedLines == 0 {
		_, err := fmt.Fprint(t.w, table)
		if err != nil {
			return err
		}
		t.printedLines = lines
		t.colWidths = colWidths
		t.dbColStart = dbColStart
		t.testRowIndex = testRowIndex
		return nil
	}

	// Once printed, we only do cell updates; re-render is a no-op.
	return nil
}

func (t *liveTable) Clear() error {
	if t.printedLines == 0 {
		return nil
	}
	if !t.enabledVT100 {
		return nil
	}
	// Move cursor back up over the previously printed table and clear to end of screen.
	_, err := fmt.Fprintf(t.w, "\r\x1b[%dA\x1b[J", t.printedLines)
	return err
}

func (t *liveTable) UpdateCell(testName, dbName string, val float64) error {
	if t.printedLines == 0 {
		return nil
	}
	rowIdx, ok := t.testRowIndex[testName]
	if !ok {
		return nil
	}
	colStart, ok := t.dbColStart[dbName]
	if !ok {
		return nil
	}
	colWidth, ok := t.colWidths[dbName]
	if !ok {
		return nil
	}

	cell := fmt.Sprintf("%*s", colWidth, formatFloat(val))

	if !t.enabledVT100 {
		// Fallback: emit a simple progress line rather than trying to "page" in non-TTY output.
		_, err := fmt.Fprintf(t.w, "%s / %s = %s\n", t.displayNames[testName], dbName, strings.TrimSpace(cell))
		return err
	}

	// Table layout:
	// line 1: header
	// line 2: separator
	// line 3..: rows (in finalTestOrder order)
	targetLineFromTop := 3 + rowIdx

	// Save cursor, jump to target cell, write, restore cursor.
	// Cursor is currently below the table after initial Render().
	_, err := fmt.Fprintf(t.w, "\x1b7\r\x1b[%dA\x1b[%dB\x1b[%dC%s\x1b8",
		t.printedLines,      // up to top
		targetLineFromTop-1, // down to row
		colStart-1,          // right to col
		cell,
	)
	return err
}

func renderResultsTableStringWithLayout(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, results map[string]map[string]float64) (table string, colWidths map[string]int, dbColStart map[string]int, testRowIndex map[string]int) {
	// Dynamically determine column widths based on content

	colNames := []string{"Test"}
	for _, inst := range instances {
		colNames = append(colNames, inst.Wrapper.Name())
	}

	colWidths = make(map[string]int)
	for _, colName := range colNames {
		colWidths[colName] = len(colName)
	}

	for _, testName := range finalTestOrder {
		dispName := displayNames[testName]
		if len(dispName) > colWidths["Test"] {
			colWidths["Test"] = len(dispName)
		}
	}

	// Minimum width so early placeholder "-" columns don't shrink.
	const minValWidth = 13
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		if colWidths[dbName] < minValWidth {
			colWidths[dbName] = minValWidth
		}
	}

	// Update widths based on known results so far.
	for _, testName := range finalTestOrder {
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			valStr := formatFloat(results[testName][dbName])
			if len(valStr) > colWidths[dbName] {
				colWidths[dbName] = len(valStr)
			}
		}
	}

	var sb strings.Builder

	dbColStart = make(map[string]int, len(instances))
	// First DB column starts after the Test column plus two spaces.
	pos := colWidths["Test"] + 3 // 1-based index
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		dbColStart[dbName] = pos
		pos += colWidths[dbName] + 2
	}

	testRowIndex = make(map[string]int, len(finalTestOrder))
	for i, tn := range finalTestOrder {
		testRowIndex[tn] = i
	}

	// Header
	headerRow := fmt.Sprintf("%*s", colWidths["Test"], "Test")
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		headerRow += fmt.Sprintf("  %*s", colWidths[dbName], dbName)
	}
	sb.WriteString(headerRow)
	sb.WriteString("\n")

	// Separator
	separatorRow := fmt.Sprintf("%*s", colWidths["Test"], strings.Repeat("-", colWidths["Test"]))
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		separatorRow += fmt.Sprintf("  %*s", colWidths[dbName], strings.Repeat("-", colWidths[dbName]))
	}
	sb.WriteString(separatorRow)
	sb.WriteString("\n")

	// Rows
	for _, testName := range finalTestOrder {
		dataRow := fmt.Sprintf("%*s", colWidths["Test"], displayNames[testName])
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			val := results[testName][dbName]
			dataRow += fmt.Sprintf("  %*s", colWidths[dbName], formatFloat(val))
		}
		sb.WriteString(dataRow)
		sb.WriteString("\n")
	}

	return sb.String(), colWidths, dbColStart, testRowIndex
}

// formatFloat formats a float with commas (e.g. 1,234,567)
func formatFloat(f float64) string {
	if math.IsNaN(f) {
		return "-"
	}
	s := fmt.Sprintf("%.0f", f)
	if f == 0 {
		return "0"
	}
	n := len(s)
	if n <= 3 {
		return s
	}
	var sb strings.Builder
	remainder := n % 3
	if remainder == 0 {
		remainder = 3
	}
	sb.WriteString(s[:remainder])
	for i := remainder; i < n; i += 3 {
		sb.WriteString(",")
		sb.WriteString(s[i : i+3])
	}
	return sb.String()
}
