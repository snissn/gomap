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
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/cmd/internal/treedbstats"
	"github.com/snissn/gomap/internal/benchprof"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

// --- Benchmark Runner ---

var (
	numKeys            = flag.Int("keys", 100000, "Number of keys")
	keyShapeArg        = flag.String("key-shape", "be8", "Key generation shape for non-dataset 8-byte workloads (be8|be8_prefix4)")
	valSize            = flag.Int("valsize", 128, "Value size in bytes")
	valPattern         = flag.String("val-pattern", "zero", "Value pattern for write tests (zero|repeat|repeat_tail64|ultra_compressible_repeat|highly_compressible_notail|half_repeat_half_random|medium_compressible_sparse|celestia_height_prefix_fill|random)")
	valPoolSize        = flag.Int("val-pool-size", 0, "Number of distinct values to cycle through for -val-pattern (0=auto)")
	batchSize          = flag.Int("batchsize", 8000, "Size of batches")
	writeWorkers       = flag.Int("write-workers", 1, "Number of goroutines for *_parallel write tests (default 1)")
	readWorkers        = flag.Int("read-workers", runtime.GOMAXPROCS(0), "Number of goroutines for random_read_parallel and random_read_parallel_acquire_snapshot (default GOMAXPROCS)")
	readRequireHit     = flag.Bool("read-require-hit", false, "Fail read benchmarks on misses and validate value length matches -valsize")
	rangeQueries       = flag.Int("range-queries", 200, "number of range queries")
	rangeSpan          = flag.Int("range-span", 100, "number of keys per range")
	keyCountsArg       = flag.String("keycounts", "", "Comma-separated key counts to sweep over (overrides -keys)")
	keyScaleArg        = flag.String("keyscale", "", "Generate keycounts by scale: log10 or doubling (uses -keys-min/-keys-max)")
	keysMin            = flag.Int("keys-min", 1000, "Minimum key count for -keyscale")
	keysMax            = flag.Int("keys-max", 10000000, "Maximum key count for -keyscale")
	dbsArg             = flag.String("dbs", "all", "Comma-separated list of DBs to run. Use 'all' for registered DBs.")
	dbsExcludeArg      = flag.String("exclude-dbs", "", "Comma-separated list of DBs to exclude")
	testArg            = flag.String("test", "all", "Comma-separated list of tests (sequential_write,random_read,random_read_parallel,random_read_parallel_acquire_snapshot,random_read_batch,random_write,random_write_parallel,dataset_write_random,dataset_write_sorted,dataset_update_fork_choice,dataset_read_random,random_delete,full_scan,prefix_scan,batch_write,batch_write_steady,batch_random,batch_delete,update_fork_choice); aliases: write_seq->sequential_write, write_rand->random_write, write_sorted->dataset_write_sorted, write_dataset->dataset_write_random, read_rand->random_read, read_rand_parallel->random_read_parallel, read_rand_batch->random_read_batch, read_random_batch->random_read_batch, delete_rand->random_delete, scan->full_scan, range_scan->prefix_scan, batch_write_ss->batch_write_steady, forkchoice->update_fork_choice")
	formatArg          = flag.String("format", "table", "Output format: table or markdown")
	suiteArg           = flag.String("suite", "", "Named benchmark suite (e.g. readme)")
	outDirArg          = flag.String("outdir", "", "Write plots/results to this directory (used by -suite readme)")
	keepDir            = flag.Bool("keep", false, "Keep data directories after run")
	progress           = flag.Bool("progress", true, "Live-update the results table on stderr (cell-by-cell) while running; final table prints once to stdout")
	seed               = flag.Int64("seed", 1, "PRNG seed for randomized tests (0 = time-based)")
	cpuProfile         = flag.String("cpuprofile", "", "write cpu profile to file")
	cpuProfileTestsArg = flag.String("cpuprofile-tests", "", "Comma-separated list of tests to profile when -cpuprofile is set (default: all selected tests)")
	profileDir         = flag.String("profile-dir", "", "Write profiling artifacts to this directory (enables defaults for -cpuprofile, -allocsprofile, -checkpoint-cpuprofile, -blockprofile, -mutexprofile, -trace unless explicitly set)")
	pathLabel          = flag.String("path-label", "", "Benchmark execution-path label required with -profile-dir (oracle|native-fastpath)")
	allocsProfile      = flag.String("allocsprofile", "", "write per-test allocation delta profile prefix to file")
	allocsProfileTests = flag.String("allocsprofile-tests", "", "Comma-separated list of tests to profile when -allocsprofile is set (default: all selected tests)")
	allocsProfileRate  = flag.Int("allocsprofilerate", 512*1024, "runtime.MemProfileRate sampling rate in bytes for -allocsprofile")

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

type DBInstance struct {
	Name    string
	Wrapper kvstore.DB
	Dir     string
}

type BenchConfig struct {
	Keys          int
	KeyShape      string
	ValueSize     int
	BatchSize     int
	WriteWorkers  int
	ReadWorkers   int
	RangeQueries  int
	RangeSpan     int
	ValuePattern  string
	ValuePoolSize int
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
}

type dirDiskUsage struct {
	TotalBytes uint64
	TotalFiles int
}

type BenchRun struct {
	Config              BenchConfig
	Instances           []*DBInstance
	TestOrder           []string
	DisplayNames        map[string]string
	Results             map[string]map[string]float64
	CheckpointDurations map[string]map[string]time.Duration
	VacuumDurations     map[string]map[string]time.Duration
	VacuumIndexBytes    map[string]map[string][2]uint64 // [0]=before, [1]=after (best-effort; treedb only)
	TreeDBDiskUsage     map[string]treeDBDiskUsage
	TreeDBVlogRewrite   map[string]treeDBVlogRewriteReport
	TreeDBPerf          map[string]map[string]treeDBPerfMetrics
	TreeDBStats         map[string]map[string]string
	DiskUsage           map[string]dirDiskUsage
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

type benchprofExport struct {
	GeneratedAt string               `json:"generated_at"`
	Runs        []benchprofExportRun `json:"runs"`
}

type benchprofExportRun struct {
	Keys          int                                     `json:"keys"`
	Profile       string                                  `json:"profile,omitempty"`
	ExecutionPath string                                  `json:"execution_path,omitempty"`
	Results       map[string]map[string]float64           `json:"results,omitempty"`
	TreeDBPerf    map[string]map[string]treeDBPerfMetrics `json:"treedb_perf,omitempty"`
	TreeDBStats   map[string]map[string]string            `json:"treedb_stats,omitempty"`
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
		if err := validateBenchprofExecutionPath(*pathLabel); err != nil {
			log.Fatalf("profile-dir: %v", err)
		}
	}

	seedUsed := *seed
	if seedUsed == 0 {
		seedUsed = time.Now().UnixNano()
	}

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
	fmt.Fprintf(os.Stderr, "DBs:         %s\n", *dbsArg)
	fmt.Fprintf(os.Stderr, "Tests:       %s\n", *testArg)
	selectedTests := normalizeTests(parseList(*testArg))
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
	baseCfg := BenchConfig{
		Keys:                             *numKeys,
		KeyShape:                         *keyShapeArg,
		ValueSize:                        *valSize,
		BatchSize:                        *batchSize,
		WriteWorkers:                     *writeWorkers,
		ReadWorkers:                      effectiveReadWorkers,
		RangeQueries:                     *rangeQueries,
		RangeSpan:                        *rangeSpan,
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
	if baseCfg.CPUProfile != "" {
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
	if baseCfg.AllocsProfile != "" {
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
	if baseCfg.CheckpointCPUProfile != "" {
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

	hasAnyProfiling := baseCfg.CPUProfile != "" || baseCfg.AllocsProfile != "" || baseCfg.BlockProfile != "" || baseCfg.MutexProfile != "" || baseCfg.TraceProfile != ""
	if hasAnyProfiling && len(keyCounts) > 1 {
		log.Fatalf("profiling flags require a single key count (got %d): disable sweep keycounts or omit -cpuprofile/-allocsprofile/-blockprofile/-mutexprofile/-trace", len(keyCounts))
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
	if cfg.CPUProfile == "" {
		return false
	}
	if len(cfg.CPUProfileTests) == 0 {
		return true
	}
	_, ok := cfg.CPUProfileTests[strings.ToLower(testName)]
	return ok
}

func shouldAllocsProfile(cfg BenchConfig, testName string) bool {
	if cfg.AllocsProfile == "" {
		return false
	}
	if len(cfg.AllocsProfileTests) == 0 {
		return true
	}
	_, ok := cfg.AllocsProfileTests[strings.ToLower(testName)]
	return ok
}

func shouldCheckpointCPUProfile(cfg BenchConfig, testName string) bool {
	if cfg.CheckpointCPUProfile == "" {
		return false
	}
	if len(cfg.CheckpointCPUProfileTests) == 0 {
		return true
	}
	_, ok := cfg.CheckpointCPUProfileTests[strings.ToLower(testName)]
	return ok
}

func startCheckpointCPUProfile(cfg BenchConfig, testName, dbName string) (*os.File, error) {
	path := fmt.Sprintf("%s_checkpoint_%s_%s.pprof", cfg.CheckpointCPUProfile, sanitizeProfileSegment(testName), sanitizeProfileSegment(dbName))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint cpu profile (%s/%s): %w", testName, dbName, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("checkpoint cpu profile start: %w", err)
	}
	return f, nil
}

var (
	stopCPUProfileFn                  = pprof.StopCPUProfile
	writeAllocsSnapshotTempFn         = writeAllocsSnapshotTemp
	writeAllocsDeltaProfileFn         = writeAllocsDeltaProfile
	writeRuntimeProfileSnapshotTempFn = writeRuntimeProfileSnapshotTemp
	writeRuntimeProfileDeltaProfileFn = writeRuntimeProfileDeltaProfile
	runPprofDeltaCommandFn            = runPprofDeltaCommand

	errEmptyPprofDeltaOutput = errors.New("empty pprof delta output")
)

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
	err := writePprofDeltaProfile(basePath, afterPath, outPath)
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
	stdout, stderrText, err := runPprofDeltaCommandFn(basePath, afterPath)
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
	cmd := exec.Command("go", "tool", "pprof", "-proto", "-base", basePath, afterPath)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, stderr.String(), err
	}
	return stdout.Bytes(), stderr.String(), nil
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
	if err := writeBenchprofArtifacts(dir, strings.TrimSpace(*pathLabel), runs); err != nil {
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

func writeBenchprofArtifacts(dir, executionPath string, runs []BenchRun) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}
	if err := validateBenchprofExecutionPath(executionPath); err != nil {
		return err
	}

	out := benchprofExport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Runs:        make([]benchprofExportRun, 0, len(runs)),
	}
	for _, run := range runs {
		out.Runs = append(out.Runs, benchprofExportRun{
			Keys:          run.Config.Keys,
			Profile:       strings.TrimSpace(run.Config.Profile),
			ExecutionPath: executionPath,
			Results:       run.Results,
			TreeDBPerf:    run.TreeDBPerf,
			TreeDBStats:   selectedBenchprofTreeDBStats(run.TreeDBStats),
		})
	}

	js, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal benchprof_results.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "benchprof_results.json"), js, 0o644); err != nil {
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
	if err := os.WriteFile(filepath.Join(dir, "benchprof_results.md"), []byte(md), 0o644); err != nil {
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

func validateBenchprofExecutionPath(executionPath string) error {
	if executionPath == "" {
		return fmt.Errorf("execution path is required for profile-dir artifacts; hidden or implied path labels are forbidden")
	}
	if executionPath == "oracle" || executionPath == "native-fastpath" {
		return nil
	}
	if strings.ContainsAny(executionPath, ",+") {
		return fmt.Errorf("invalid execution path %q: mixed-path labels are forbidden; expected one of oracle|native-fastpath", executionPath)
	}
	return fmt.Errorf("invalid execution path %q: expected one of oracle|native-fastpath", executionPath)
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
		"treedb.cache.append_only.mutable_from_lease_total",
		"treedb.cache.append_only.mutable_from_pool_total",
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
		"treedb.cache.vlog_mmap.read.hits",
		"treedb.cache.vlog_mmap.read.miss_out_of_range",
		"treedb.cache.vlog_mmap.read.miss_no_mapping",
		"treedb.cache.vlog_mmap.read.miss_dead_mapping_cap",
		"treedb.cache.vlog_mmap.read.fallback_readat",
		"treedb.cache.vlog_mmap.read.hit_ratio",
		"treedb.vlog.grouped_frame_cache.hits",
		"treedb.vlog.grouped_frame_cache.misses",
		"treedb.vlog.grouped_frame_cache.entries",
		"treedb.vlog.grouped_frame_cache.capacity",
		"treedb.vlog.grouped_frame_cache.hit_ratio",
		"treedb.process.read_path.outer_leaf.cache.hits",
		"treedb.process.read_path.outer_leaf.cache.misses",
		"treedb.process.read_path.outer_leaf.cache.stores",
		"treedb.process.read_path.outer_leaf.cache.evictions",
		"treedb.process.read_path.outer_leaf.cache.entries",
		"treedb.process.read_path.outer_leaf.cache.capacity",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_skips",
		"treedb.process.read_path.outer_leaf.cache.read_miss_admission_stores",
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

func snapshotSelectedTreeDBStats(db kvstore.DB) treeDBSelectedStats {
	sp, ok := db.(kvstore.StatsProvider)
	if !ok {
		return treeDBSelectedStats{}
	}
	stats := sp.Stats()
	var snap treeDBSelectedStats
	snap.mmapHits, _ = parseUint64StatValue(stats,
		"treedb.cache.vlog_mmap.read.hits",
		"treedb.vlog.mmap_read.hits",
	)
	snap.mmapMissOutOfRange, _ = parseUint64StatValue(stats,
		"treedb.cache.vlog_mmap.read.miss_out_of_range",
		"treedb.vlog.mmap_read.miss_out_of_range",
	)
	snap.mmapMissNoMapping, _ = parseUint64StatValue(stats,
		"treedb.cache.vlog_mmap.read.miss_no_mapping",
		"treedb.vlog.mmap_read.miss_no_mapping",
	)
	snap.mmapMissDeadCap, _ = parseUint64StatValue(stats,
		"treedb.cache.vlog_mmap.read.miss_dead_mapping_cap",
		"treedb.vlog.mmap_read.miss_dead_mapping_cap",
	)
	snap.mmapFallbackReadAt, _ = parseUint64StatValue(stats,
		"treedb.cache.vlog_mmap.read.fallback_readat",
		"treedb.vlog.mmap_read.fallback_readat",
	)
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

	if cfg.AllocsProfile != "" {
		rate := cfg.AllocsProfileRate
		if rate <= 0 {
			rate = 512 * 1024
		}
		prevRate := runtime.MemProfileRate
		runtime.MemProfileRate = rate
		defer func() {
			runtime.MemProfileRate = prevRate
		}()
	}
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
		instances = append(instances, &DBInstance{Name: name, Wrapper: db, Dir: dir})
	}
	if len(instances) == 0 {
		return BenchRun{}, fmt.Errorf("no DBs selected")
	}

	// Define Tests
	type TestFunc func(db kvstore.DB, rng *rand.Rand) (float64, error)

	guard := newBenchGuard(cfg)
	checkpointDurations := make(map[string]map[string]time.Duration)
	vacuumDurations := make(map[string]map[string]time.Duration)
	vacuumIndexBytes := make(map[string]map[string][2]uint64)
	treeDBPerf := make(map[string]map[string]treeDBPerfMetrics)
	snapshotPerfByTest := make(map[string]map[string]treeDBSnapshotPerfMetrics)

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
		return float64(total) / time.Since(start).Seconds(), nil
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
			if err := td.DB.CompactIndex(); err != nil {
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
				batch, err := batcher.NewBatch()
				if err != nil {
					return 0, fmt.Errorf("batch_delete: new batch: %w", err)
				}

				end := i + batchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					offset := j * keySize
					key := allKeys[offset : offset+keySize]
					if err := batch.Delete(key); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("batch_delete: delete: %w", err)
					}
				}
				if err := batch.Commit(); err != nil {
					_ = batch.Close()
					return 0, fmt.Errorf("batch_delete: commit: %w", err)
				}
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("batch_delete: close: %w", err)
				}
				if err := pc.Add(db, end-i, int64(end-i)*perOpBytes); err != nil {
					return 0, fmt.Errorf("batch_delete checkpoint: %w", err)
				}
			}
			return float64(total) / time.Since(start).Seconds(), nil
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
				if hasMany {
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
		"batch_small_seq":                       "Batch Small Seq",
		"update_fork_choice":                    "Update ForkChoice (Batch CommitSync)",
		"dataset_update_fork_choice":            "Update ForkChoice (Dataset Keys)",
		"random_delete":                         "Random Delete",
		"dataset_read_random":                   "Random Read (Dataset Keys)",
	}

	finalTestOrder := make([]string, 0)
	if contains(testsToRun, "all") {
		finalTestOrder = allTestOrder
	} else {
		for _, t := range testsToRun {
			if t == "" {
				continue
			}
			if _, ok := testFuncs[t]; !ok {
				return BenchRun{}, fmt.Errorf("unknown test: %q", t)
			}
			finalTestOrder = append(finalTestOrder, t)
		}
	}
	if len(finalTestOrder) == 0 {
		return BenchRun{}, fmt.Errorf("no tests selected")
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

	settledBeforeScans := false

	if cfg.BlockProfile != "" {
		rate := cfg.BlockProfileRate
		if rate <= 0 {
			rate = 1
		}
		runtime.SetBlockProfileRate(rate)
		defer runtime.SetBlockProfileRate(0)

		f, err := os.Create(cfg.BlockProfile)
		if err != nil {
			return BenchRun{}, fmt.Errorf("blockprofile: %w", err)
		}
		defer func() {
			_ = pprof.Lookup("block").WriteTo(f, 0)
			_ = f.Close()
		}()
	}

	if cfg.MutexProfile != "" {
		frac := cfg.MutexProfileFraction
		if frac <= 0 {
			frac = 1
		}
		runtime.SetMutexProfileFraction(frac)
		defer runtime.SetMutexProfileFraction(0)

		f, err := os.Create(cfg.MutexProfile)
		if err != nil {
			return BenchRun{}, fmt.Errorf("mutexprofile: %w", err)
		}
		defer func() {
			_ = pprof.Lookup("mutex").WriteTo(f, 0)
			_ = f.Close()
		}()
	}

	if cfg.TraceProfile != "" {
		f, err := os.Create(cfg.TraceProfile)
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

				var (
					checkpointCPUFile *os.File
					err               error
				)
				if shouldCheckpointCPUProfile(cfg, testName) {
					checkpointCPUFile, err = startCheckpointCPUProfile(cfg, testName, inst.Wrapper.Name())
					if err != nil {
						return BenchRun{}, fmt.Errorf("checkpoint %s before %s profiling: %w", inst.Name, testName, err)
					}
				}

				start := time.Now()
				checkpointErr := cp.Checkpoint()

				if checkpointCPUFile != nil {
					stopCPUProfileFn()
					_ = checkpointCPUFile.Close()
				}

				if checkpointErr != nil {
					return BenchRun{}, fmt.Errorf("checkpoint %s before %s: %w", inst.Name, testName, checkpointErr)
				}
				chkMap[inst.Wrapper.Name()] = time.Since(start)
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

			var treeStatsBefore treeDBSelectedStats
			if isTreeDBInstance(inst) {
				treeStatsBefore = snapshotSelectedTreeDBStats(inst.Wrapper)
			}

			// Create a fresh PRNG for each DB so they get the same sequence
			rng := rand.New(rand.NewSource(seed))

			// Run
			// CPU profile if enabled (only for single key count)
			var cpuFile *os.File
			if shouldCPUProfile(cfg, testName) {
				path := cfg.CPUProfile + "_" + testName + "_" + inst.Name + ".pprof"
				f, err := os.Create(path)
				if err != nil {
					return BenchRun{}, fmt.Errorf("cpuprofile %s: %w", path, err)
				}
				cpuFile = f
				if err := pprof.StartCPUProfile(cpuFile); err != nil {
					_ = cpuFile.Close()
					return BenchRun{}, fmt.Errorf("cpuprofile start %s: %w", path, err)
				}
			}

			allocBasePath := ""
			if shouldAllocsProfile(cfg, testName) {
				allocBasePath, err = writeAllocsSnapshotTempFn("unified_bench_allocs_base")
				if err != nil {
					if cpuFile != nil {
						stopCPUProfileFn()
						_ = cpuFile.Close()
					}
					return BenchRun{}, fmt.Errorf("allocsprofile baseline %s/%s: %w", testName, inst.Name, err)
				}
			}
			blockBasePath := ""
			if cfg.BlockProfile != "" {
				blockBasePath, err = writeRuntimeProfileSnapshotTempFn("unified_bench_block_base", "block")
				if err != nil {
					if cpuFile != nil {
						stopCPUProfileFn()
						_ = cpuFile.Close()
					}
					_ = os.Remove(allocBasePath)
					return BenchRun{}, fmt.Errorf("blockprofile baseline %s/%s: %w", testName, inst.Name, err)
				}
			}
			mutexBasePath := ""
			if cfg.MutexProfile != "" {
				mutexBasePath, err = writeRuntimeProfileSnapshotTempFn("unified_bench_mutex_base", "mutex")
				if err != nil {
					if cpuFile != nil {
						stopCPUProfileFn()
						_ = cpuFile.Close()
					}
					_ = os.Remove(allocBasePath)
					_ = os.Remove(blockBasePath)
					return BenchRun{}, fmt.Errorf("mutexprofile baseline %s/%s: %w", testName, inst.Name, err)
				}
			}

			opsPerSec, runErr := fn(inst.Wrapper, rng)

			if cpuFile != nil {
				stopCPUProfileFn()
				_ = cpuFile.Close()
			}

			blockAfterPath := ""
			mutexAfterPath := ""
			if runErr == nil {
				if blockBasePath != "" {
					blockAfterPath, err = writeRuntimeProfileSnapshotTempFn("unified_bench_block_after", "block")
					if err != nil {
						_ = os.Remove(allocBasePath)
						_ = os.Remove(blockBasePath)
						_ = os.Remove(mutexBasePath)
						return BenchRun{}, fmt.Errorf("blockprofile snapshot %s/%s: %w", testName, inst.Name, err)
					}
				}
				if mutexBasePath != "" {
					mutexAfterPath, err = writeRuntimeProfileSnapshotTempFn("unified_bench_mutex_after", "mutex")
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
					allocAfterPath, snapErr := writeAllocsSnapshotTempFn("unified_bench_allocs_after")
					if snapErr != nil {
						_ = os.Remove(allocBasePath)
						_ = os.Remove(blockBasePath)
						_ = os.Remove(blockAfterPath)
						_ = os.Remove(mutexBasePath)
						_ = os.Remove(mutexAfterPath)
						return BenchRun{}, fmt.Errorf("allocsprofile snapshot %s/%s: %w", testName, inst.Name, snapErr)
					}
					allocPath := cfg.AllocsProfile + "_" + testName + "_" + inst.Name + ".pprof"
					deltaErr := writeAllocsDeltaProfileFn(allocBasePath, allocAfterPath, allocPath)
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
					blockPath := contentionProfilePath(cfg.BlockProfile, "block", testName, inst.Name)
					_, deltaErr := writeRuntimeProfileDeltaProfileFn(blockBasePath, blockAfterPath, blockPath)
					_ = os.Remove(blockBasePath)
					_ = os.Remove(blockAfterPath)
					if deltaErr != nil {
						_ = os.Remove(mutexBasePath)
						_ = os.Remove(mutexAfterPath)
						return BenchRun{}, fmt.Errorf("blockprofile %s/%s (%s): %w", testName, inst.Name, blockPath, deltaErr)
					}
				}
			}
			if mutexBasePath != "" {
				if runErr != nil {
					_ = os.Remove(mutexBasePath)
					_ = os.Remove(mutexAfterPath)
				} else {
					mutexPath := contentionProfilePath(cfg.MutexProfile, "mutex", testName, inst.Name)
					_, deltaErr := writeRuntimeProfileDeltaProfileFn(mutexBasePath, mutexAfterPath, mutexPath)
					_ = os.Remove(mutexBasePath)
					_ = os.Remove(mutexAfterPath)
					if deltaErr != nil {
						return BenchRun{}, fmt.Errorf("mutexprofile %s/%s (%s): %w", testName, inst.Name, mutexPath, deltaErr)
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

			var (
				checkpointCPUFile *os.File
				err               error
			)
			if shouldCheckpointCPUProfile(cfg, checkpointPostRunLabel) {
				checkpointCPUFile, err = startCheckpointCPUProfile(cfg, checkpointPostRunLabel, inst.Wrapper.Name())
				if err != nil {
					return BenchRun{}, fmt.Errorf("checkpoint %s after run profiling: %w", inst.Name, err)
				}
			}

			start := time.Now()
			checkpointErr := cp.Checkpoint()

			if checkpointCPUFile != nil {
				stopCPUProfileFn()
				_ = checkpointCPUFile.Close()
			}

			if checkpointErr != nil {
				return BenchRun{}, fmt.Errorf("checkpoint %s after run: %w", inst.Name, checkpointErr)
			}
			chkMap[inst.Wrapper.Name()] = time.Since(start)
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
		if hasStatsProvider {
			if postCloseStats := sp.Stats(); len(postCloseStats) > 0 {
				statsSnapshot = postCloseStats
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
		Config:              cfg,
		Instances:           instances,
		TestOrder:           finalTestOrder,
		DisplayNames:        displayNames,
		Results:             results,
		CheckpointDurations: checkpointDurations,
		VacuumDurations:     vacuumDurations,
		VacuumIndexBytes:    vacuumIndexBytes,
		TreeDBDiskUsage:     treedbDisk,
		TreeDBVlogRewrite:   treedbRewrite,
		TreeDBPerf:          treeDBPerf,
		TreeDBStats:         treedbStats,
		DiskUsage:           diskUsage,
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
		{label: "vlog_mmap.read.hits", alts: []string{"treedb.cache.vlog_mmap.read.hits", "treedb.vlog.mmap_read.hits"}},
		{label: "vlog_mmap.read.miss_out_of_range", alts: []string{"treedb.cache.vlog_mmap.read.miss_out_of_range", "treedb.vlog.mmap_read.miss_out_of_range"}},
		{label: "vlog_mmap.read.miss_no_mapping", alts: []string{"treedb.cache.vlog_mmap.read.miss_no_mapping", "treedb.vlog.mmap_read.miss_no_mapping"}},
		{label: "vlog_mmap.read.miss_dead_mapping_cap", alts: []string{"treedb.cache.vlog_mmap.read.miss_dead_mapping_cap", "treedb.vlog.mmap_read.miss_dead_mapping_cap"}},
		{label: "vlog_mmap.read.fallback_readat", alts: []string{"treedb.cache.vlog_mmap.read.fallback_readat", "treedb.vlog.mmap_read.fallback_readat"}},
		{label: "vlog_mmap.read.hit_ratio", alts: []string{"treedb.cache.vlog_mmap.read.hit_ratio", "treedb.vlog.mmap_read.hit_ratio"}},
		{label: "applied_command_lsn", alts: []string{"treedb.applied_command_lsn"}},
		{label: "command_wal.enabled", alts: []string{"treedb.command_wal.enabled"}},
		{label: "command_wal.required_feature", alts: []string{"treedb.command_wal.required_feature"}},
		{label: "command_wal.frames", alts: []string{"treedb.command_wal.frames"}},
		{label: "command_wal.typed_segments", alts: []string{"treedb.command_wal.typed_segments"}},
		{label: "command_wal.max_lsn", alts: []string{"treedb.command_wal.max_lsn"}},
		{label: "leaf_generation.generations.pinned", alts: []string{"treedb.leaf_generation.generations.pinned"}},
		{label: "leaf_generation.pins.total", alts: []string{"treedb.leaf_generation.pins.total"}},
		{label: "publish.ordered_root_delta_group.calls_total", alts: []string{"treedb.publish.ordered_root_delta_group.calls_total"}},
		{label: "publish.ordered_root_delta_group.roots_total", alts: []string{"treedb.publish.ordered_root_delta_group.roots_total"}},
		{label: "publish.ordered_root_delta_group.avg_roots_per_call", alts: []string{"treedb.publish.ordered_root_delta_group.avg_roots_per_call"}},
		{label: "publish.ordered_root_delta_group.root_apply_calls_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_calls_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_ns_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_ns_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_ops_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_ops_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_node_loads_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_node_loads_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total"}},
		{label: "publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total", alts: []string{"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total"}},
		{label: "publish.ordered_root_delta_group.write_lock_wait_ns_total", alts: []string{"treedb.publish.ordered_root_delta_group.write_lock_wait_ns_total"}},
		{label: "publish.ordered_root_delta_group.write_lock_hold_ns_total", alts: []string{"treedb.publish.ordered_root_delta_group.write_lock_hold_ns_total"}},
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
	cfg.DBsArg = "treedb,leveldb"
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

func parseList(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.ToLower(strings.TrimSpace(parts[i]))
	}
	return parts
}

func normalizeTests(list []string) []string {
	if contains(list, "all") {
		return list
	}
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
