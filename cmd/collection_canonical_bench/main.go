package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const schemaVersion = "collections-canonical-benchmark/v1"
const envCanonicalCommandLine = "COLLECTION_CANONICAL_BENCH_COMMAND"
const runDirSentinel = ".collection_canonical_bench_run"

const (
	phasePostInsert               = "post_insert"
	phaseOnlineOnePassMaintenance = "online_one_pass_maintenance"
	phaseOfflineCompact           = "offline_compact"
	phaseExhaustiveCompact        = "exhaustive_compact"
	phaseFullLeafgenPackGC        = "full_leafgen_pack_gc"
	phaseSQLiteVacuum             = "sqlite_vacuum"
)

type config struct {
	RepoRoot                  string
	OutDir                    string
	Docs                      int
	BatchSize                 int
	Indexes                   int
	Benchtime                 string
	Count                     int
	TreeEngine                string
	Profile                   string
	Formats                   string
	StorageCells              string
	LeafSegmentTargetBytes    int64
	LeafgenPackFrameK         int
	FullLeafgenForce          bool
	FullLeafgenMaxGenerations int
	FullLeafgenIndexVacuum    string
	SkipTimedMatrix           bool
	SkipOfflineRewrite        bool
	SkipFullLeafgen           bool
	SkipSQLite                bool
	ProfileTimedMatrix        bool
	ProfileBenchtime          string
	ProfileCount              int
	AllowIncomplete           bool
}

type canonicalRun struct {
	SchemaVersion string            `json:"schema_version"`
	GeneratedAt   string            `json:"generated_at"`
	RunDir        string            `json:"run_dir"`
	Worktree      string            `json:"worktree"`
	Branch        string            `json:"branch"`
	Commit        string            `json:"commit"`
	CommandLine   []string          `json:"command_line"`
	Config        canonicalConfig   `json:"config"`
	Artifacts     map[string]string `json:"artifacts"`
	Commands      []commandRecord   `json:"commands"`
	Results       []resultRow       `json:"results"`
	Comparisons   []comparisonRow   `json:"comparisons"`
	Checks        []guardrailCheck  `json:"guardrail_checks"`
}

type canonicalConfig struct {
	Docs                      int      `json:"docs"`
	BatchSize                 int      `json:"batch_size"`
	Indexes                   int      `json:"indexes"`
	Benchtime                 string   `json:"benchtime"`
	Count                     int      `json:"count"`
	TreeEngine                string   `json:"tree_engine"`
	Profile                   string   `json:"profile"`
	Formats                   []string `json:"formats"`
	StorageCells              string   `json:"storage_cells"`
	LeafSegmentTargetBytes    int64    `json:"leaf_segment_target_bytes"`
	LeafgenPackFrameK         int      `json:"leafgen_pack_frame_k"`
	FullLeafgenForce          bool     `json:"full_leafgen_force"`
	FullLeafgenMaxGenerations int      `json:"full_leafgen_max_generations"`
	FullLeafgenIndexVacuum    string   `json:"full_leafgen_index_vacuum"`
	SkipSQLite                bool     `json:"skip_sqlite"`
	ProfileTimedMatrix        bool     `json:"profile_timed_matrix"`
	ProfileBenchtime          string   `json:"profile_benchtime,omitempty"`
	ProfileCount              int      `json:"profile_count,omitempty"`
}

type commandRecord struct {
	Name     string            `json:"name"`
	Command  []string          `json:"command"`
	Env      map[string]string `json:"env,omitempty"`
	WorkDir  string            `json:"work_dir"`
	LogPath  string            `json:"log_path,omitempty"`
	ExitCode int               `json:"exit_code"`
	Duration string            `json:"duration"`
}

type resultRow struct {
	GeneratedAt        string              `json:"generated_at,omitempty"`
	RunDir             string              `json:"run_dir,omitempty"`
	Worktree           string              `json:"worktree,omitempty"`
	Branch             string              `json:"branch,omitempty"`
	Commit             string              `json:"commit,omitempty"`
	CommandLine        []string            `json:"command_line,omitempty"`
	ConfigName         string              `json:"config_name"`
	Engine             string              `json:"engine"`
	Format             string              `json:"format"`
	Shape              string              `json:"shape"`
	IndexCount         int                 `json:"index_count"`
	DocumentCount      int                 `json:"document_count"`
	BenchmarkName      string              `json:"benchmark_name"`
	Phase              string              `json:"phase"`
	MaintenanceMode    string              `json:"maintenance_mode"`
	TotalBytes         *int64              `json:"total_bytes,omitempty"`
	BytesPerDoc        *float64            `json:"bytes_per_doc,omitempty"`
	DocsPerSec         *float64            `json:"docs_per_sec,omitempty"`
	NsPerDoc           *float64            `json:"ns_per_doc,omitempty"`
	BatchSize          int                 `json:"batch_size,omitempty"`
	BenchmarkTimed     bool                `json:"benchmark_timed"`
	MeasurementKind    string              `json:"measurement_kind"`
	MeasurementNote    string              `json:"measurement_note,omitempty"`
	CompactionFlags    map[string]string   `json:"compaction_flags,omitempty"`
	SourceArtifact     string              `json:"source_artifact,omitempty"`
	MaintenanceStats   map[string]float64  `json:"maintenance_stats,omitempty"`
	Extra              map[string]string   `json:"extra,omitempty"`
	ProductionEvidence *productionEvidence `json:"production_evidence,omitempty"`
}

type comparisonRow struct {
	GeneratedAt       string   `json:"generated_at,omitempty"`
	RunDir            string   `json:"run_dir,omitempty"`
	Worktree          string   `json:"worktree,omitempty"`
	Branch            string   `json:"branch,omitempty"`
	Commit            string   `json:"commit,omitempty"`
	CommandLine       []string `json:"command_line,omitempty"`
	ComparisonName    string   `json:"comparison_name"`
	TreeDBConfigName  string   `json:"treedb_config_name"`
	TreeDBPhase       string   `json:"treedb_phase"`
	SQLiteConfigName  string   `json:"sqlite_config_name"`
	SQLitePhase       string   `json:"sqlite_phase"`
	TreeDBBytesPerDoc float64  `json:"treedb_bytes_per_doc"`
	SQLiteBytesPerDoc float64  `json:"sqlite_bytes_per_doc"`
	SmallerRatio      float64  `json:"smaller_ratio"`
	ComparisonBasis   string   `json:"comparison_basis"`
}

type guardrailCheck struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

type collectionReport struct {
	GeneratedAt          string          `json:"generated_at"`
	Status               string          `json:"status"`
	ExecutionPath        string          `json:"execution_path"`
	BenchmarkEngine      string          `json:"benchmark_engine"`
	DocumentFormat       string          `json:"document_format"`
	StoragePolicy        string          `json:"storage_policy"`
	PagerChunkSize       string          `json:"pager_chunk_size"`
	PagerSyncConcurrency string          `json:"pager_sync_concurrency"`
	Worktree             string          `json:"worktree"`
	Branch               string          `json:"branch"`
	Commit               string          `json:"commit"`
	BenchPattern         string          `json:"bench_pattern"`
	Count                int             `json:"count"`
	CollectionBatchSize  int             `json:"collection_batch_size"`
	Sections             []reportSection `json:"sections"`
}

type reportSection struct {
	Title      string            `json:"title"`
	Benchmarks []reportBenchmark `json:"benchmarks"`
}

type reportBenchmark struct {
	Name        string             `json:"name"`
	Section     string             `json:"section"`
	Description string             `json:"description"`
	MeanNSPerOp float64            `json:"mean_ns_per_op"`
	OpsPerSec   float64            `json:"ops_per_sec"`
	MeanMetrics map[string]float64 `json:"mean_metrics"`
}

type productionEvidence struct {
	ProducerRoute                       string   `json:"producer_route,omitempty"`
	ProducerRouteObservations           *float64 `json:"producer_route_observations,omitempty"`
	ProducerRouteCandidateOps           *float64 `json:"producer_route_candidate_ops,omitempty"`
	ProducerRouteEligibleOps            *float64 `json:"producer_route_eligible_ops,omitempty"`
	ProducerRouteUsedOps                *float64 `json:"producer_route_used_ops,omitempty"`
	ProducerRouteFallbacks              *float64 `json:"producer_route_fallbacks,omitempty"`
	StoragePolicy                       string   `json:"storage_policy,omitempty"`
	StorageCells                        string   `json:"storage_cells,omitempty"`
	PagerChunkSize                      string   `json:"pager_chunk_size,omitempty"`
	PagerSyncConcurrency                string   `json:"pager_sync_concurrency,omitempty"`
	LeafSegmentTargetBytes              int64    `json:"leaf_segment_target_bytes,omitempty"`
	GOMAXPROCS                          *int     `json:"gomaxprocs,omitempty"`
	PhysicalCores                       *int     `json:"physical_cores,omitempty"`
	FlushAdmissionConfiguredConcurrency *int     `json:"flush_admission_configured_concurrency,omitempty"`
	FlushAdmissionEffectiveConcurrency  *int     `json:"flush_admission_effective_concurrency,omitempty"`
	FlushAdmissionGOMAXPROCS            *int     `json:"flush_admission_gomaxprocs,omitempty"`
	FlushAdmissionPhysicalCores         *int     `json:"flush_admission_physical_cores,omitempty"`
	FlushAdmissionAdmitted              *bool    `json:"flush_admission_admitted,omitempty"`
	FlushAdmissionSpanNative            *bool    `json:"flush_admission_span_native,omitempty"`
	FlushAdmissionBacklogCoalescing     *bool    `json:"flush_admission_backlog_coalescing,omitempty"`
	FlushSpanCandidateOps               *float64 `json:"flush_span_candidate_ops,omitempty"`
	FlushSpanEligibleOps                *float64 `json:"flush_span_eligible_ops,omitempty"`
	FlushSpanUsedOps                    *float64 `json:"flush_span_used_ops,omitempty"`
	FlushSpanFallbacks                  *float64 `json:"flush_span_fallbacks,omitempty"`
	OrderedRootSpanCandidateOps         *float64 `json:"ordered_root_span_candidate_ops,omitempty"`
	OrderedRootSpanEligibleOps          *float64 `json:"ordered_root_span_eligible_ops,omitempty"`
	OrderedRootSpanUsedOps              *float64 `json:"ordered_root_span_used_ops,omitempty"`
	OrderedRootSpanFallbacks            *float64 `json:"ordered_root_span_fallbacks,omitempty"`
}

const noSecondaryIndexProducerRoute = "flush_apply_span_native_no_secondary_indexes"

type fixtureSummary struct {
	GeneratedAt                string                 `json:"generated_at"`
	Dir                        string                 `json:"dir"`
	Collection                 string                 `json:"collection"`
	DocumentFormat             string                 `json:"document_format"`
	Profile                    string                 `json:"profile"`
	Docs                       int                    `json:"docs"`
	BatchSize                  int                    `json:"batch_size"`
	IndexCount                 int                    `json:"index_count"`
	DataOuterLeavesInValueLog  bool                   `json:"data_outer_leaves_in_value_log"`
	IndexOuterLeavesInValueLog bool                   `json:"index_outer_leaves_in_value_log"`
	WallTiming                 timingSummary          `json:"wall_timing"`
	InsertTiming               timingSummary          `json:"insert_timing"`
	DiskUsageBeforeMaintenance *diskUsageSummary      `json:"disk_usage_before_maintenance"`
	DiskUsageFinal             *diskUsageSummary      `json:"disk_usage_final"`
	LeafGeneration             *leafGenerationSummary `json:"leaf_generation"`
	IndexVacuum                *fixtureIndexVacuum    `json:"index_vacuum"`
}

type timingSummary struct {
	Seconds   float64 `json:"seconds"`
	SecPerOp  float64 `json:"sec_per_op"`
	OpsPerSec float64 `json:"ops_per_sec"`
}

type diskUsageSummary struct {
	TotalBytes  int64   `json:"total_bytes"`
	BytesPerDoc float64 `json:"bytes_per_doc"`
}

type leafGenerationSummary struct {
	Enabled                bool   `json:"enabled"`
	Force                  bool   `json:"force"`
	MaxGenerations         int    `json:"max_generations,omitempty"`
	PlanAdmission          string `json:"plan_admission,omitempty"`
	DiskBytesBefore        int64  `json:"disk_bytes_before,omitempty"`
	DiskBytesAfterPack     int64  `json:"disk_bytes_after_pack,omitempty"`
	DiskBytesAfterGC       int64  `json:"disk_bytes_after_gc,omitempty"`
	CandidateGenerations   int    `json:"candidate_generations,omitempty"`
	CandidateBytesTotal    int64  `json:"candidate_bytes_total,omitempty"`
	CandidateBytesLive     int64  `json:"candidate_bytes_live,omitempty"`
	CandidateBytesDead     int64  `json:"candidate_bytes_dead,omitempty"`
	CandidateBytesToCopy   int64  `json:"candidate_bytes_to_copy,omitempty"`
	ExpectedReclaimBytes   int64  `json:"expected_reclaim_bytes,omitempty"`
	PackGenerationsMatched int    `json:"pack_generations_matched,omitempty"`
	PackLeafPagesCopied    int    `json:"pack_leaf_pages_copied,omitempty"`
	PackLeafFramesWritten  int    `json:"pack_leaf_frames_written,omitempty"`
	PackMaxLeafFrameK      int    `json:"pack_max_leaf_frame_k,omitempty"`
	PackBytesCopied        int64  `json:"pack_bytes_copied,omitempty"`
	GCGenerationsDeleted   int    `json:"gc_generations_deleted,omitempty"`
	GCFilesDeleted         int    `json:"gc_files_deleted,omitempty"`
	GCBytesDeleted         int64  `json:"gc_bytes_deleted,omitempty"`
}

type fixtureIndexVacuum struct {
	Mode               string        `json:"mode"`
	Enabled            bool          `json:"enabled"`
	Timing             timingSummary `json:"timing"`
	DiskBytesBefore    int64         `json:"disk_bytes_before,omitempty"`
	DiskBytesAfter     int64         `json:"disk_bytes_after,omitempty"`
	IndexDBBytesBefore int64         `json:"index_db_bytes_before,omitempty"`
	IndexDBBytesAfter  int64         `json:"index_db_bytes_after,omitempty"`
}

func main() {
	if err := run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "collection-canonical-bench: %v\n", err)
		os.Exit(1)
	}
}

func run(argv []string) error {
	cfg, err := parseConfig(argv[1:])
	if err != nil {
		return err
	}
	if cfg.RepoRoot == "" {
		cfg.RepoRoot, err = gitOutput("", "rev-parse", "--show-toplevel")
		if err != nil {
			return err
		}
	}
	if cfg.OutDir == "" {
		cfg.OutDir = filepath.Join(os.TempDir(), "collection_canonical_bench_"+time.Now().Format("20060102_150405"))
	}
	if err := normalizeRunPaths(&cfg); err != nil {
		return err
	}
	if cfg.Benchtime == "" {
		cfg.Benchtime = fmt.Sprintf("%dx", cfg.Docs)
	}
	if cfg.ProfileBenchtime == "" {
		cfg.ProfileBenchtime = cfg.Benchtime
	}
	formats := canonicalFormatList(splitCSV(cfg.Formats))
	if len(formats) == 0 {
		return errors.New("-formats must contain at least one value")
	}
	cfg.Formats = strings.Join(formats, ",")
	if err := prepareRunDir(cfg); err != nil {
		return err
	}

	branch := resolveBranch(cfg.RepoRoot)
	commit, err := gitOutput(cfg.RepoRoot, "rev-parse", "--short=12", "HEAD")
	if err != nil {
		return fmt.Errorf("resolve git commit: %w", err)
	}
	commandLine := argv
	if rawCommand := strings.TrimSpace(os.Getenv(envCanonicalCommandLine)); rawCommand != "" {
		commandLine = []string{rawCommand}
	}
	canon := &canonicalRun{
		SchemaVersion: schemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		RunDir:        cfg.OutDir,
		Worktree:      cfg.RepoRoot,
		Branch:        branch,
		Commit:        commit,
		CommandLine:   commandLine,
		Config: canonicalConfig{
			Docs:                      cfg.Docs,
			BatchSize:                 cfg.BatchSize,
			Indexes:                   cfg.Indexes,
			Benchtime:                 cfg.Benchtime,
			Count:                     cfg.Count,
			TreeEngine:                cfg.TreeEngine,
			Profile:                   cfg.Profile,
			Formats:                   formats,
			StorageCells:              cfg.StorageCells,
			LeafSegmentTargetBytes:    cfg.LeafSegmentTargetBytes,
			LeafgenPackFrameK:         cfg.LeafgenPackFrameK,
			FullLeafgenForce:          cfg.FullLeafgenForce,
			FullLeafgenMaxGenerations: cfg.FullLeafgenMaxGenerations,
			FullLeafgenIndexVacuum:    cfg.FullLeafgenIndexVacuum,
			SkipSQLite:                cfg.SkipSQLite,
			ProfileTimedMatrix:        cfg.ProfileTimedMatrix,
			ProfileBenchtime:          cfg.ProfileBenchtime,
			ProfileCount:              cfg.ProfileCount,
		},
		Artifacts: map[string]string{},
	}

	fmt.Printf("canonical collections benchmark run dir: %s\n", cfg.OutDir)
	if err := runBuildHelpers(cfg, canon); err != nil {
		return err
	}
	if !cfg.SkipTimedMatrix {
		if err := runTimedMatrix(cfg, canon); err != nil {
			return err
		}
	}
	if !cfg.SkipOfflineRewrite {
		runReportPhase(canon, "error", "phase.offline_compact.failed", "offline compact matrix", func() error {
			return runOfflineRewriteMatrix(cfg, canon, "offline_compact", phaseOfflineCompact, "full")
		})
		runReportPhase(canon, "error", "phase.exhaustive_compact.failed", "exhaustive compact matrix", func() error {
			return runOfflineRewriteMatrix(cfg, canon, "exhaustive_compact", phaseExhaustiveCompact, "exhaustive")
		})
	}
	if !cfg.SkipFullLeafgen {
		runReportPhase(canon, "warning", "phase.full_leafgen_pack_gc.failed", "full leafgen pack/GC fixture", func() error {
			return runFullLeafgenFixture(cfg, canon, formats)
		})
	}

	canon.Comparisons = buildCompactedComparisons(canon)
	finalizeRunMetadata(canon)
	canon.Checks = append(canon.Checks, validateCanonicalRun(canon)...)
	if err := writeOutputs(canon); err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", filepath.Join(cfg.OutDir, "benchmark_summary.md"))
	fmt.Printf("wrote %s\n", filepath.Join(cfg.OutDir, "benchmark_results.json"))
	fmt.Printf("wrote %s\n", filepath.Join(cfg.OutDir, "benchmark_matrix.csv"))
	if hasErrorCheck(canon.Checks) && !cfg.AllowIncomplete {
		return errors.New("guardrail validation failed; rerun with -allow-incomplete to keep partial results")
	}
	return nil
}

func runReportPhase(canon *canonicalRun, severity, code, label string, fn func() error) {
	if err := fn(); err != nil {
		canon.Checks = append(canon.Checks, guardrailCheck{
			Severity: severity,
			Code:     code,
			Message:  fmt.Sprintf("%s failed; writing completed canonical phases anyway: %v", label, err),
		})
	}
}

func normalizeRunPaths(cfg *config) error {
	outDir, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return fmt.Errorf("resolve -out-dir: %w", err)
	}
	cfg.OutDir = outDir
	return nil
}

func prepareRunDir(cfg config) error {
	if err := validateSafeRunDir(cfg.OutDir); err != nil {
		return err
	}
	paths := []string{
		"logs",
		"timed_matrix",
		"offline_compact",
		"exhaustive_compact",
		"full_leafgen_pack_gc",
		"benchmark_results.json",
		"benchmark_summary.md",
		"benchmark_matrix.csv",
	}
	for _, rel := range paths {
		if err := os.RemoveAll(filepath.Join(cfg.OutDir, rel)); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(cfg.OutDir, "logs"), 0755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(cfg.OutDir, runDirSentinel), []byte(schemaVersion+"\n"), 0644)
}

func validateSafeRunDir(outDir string) error {
	if outDir == "" {
		return errors.New("-out-dir must not be empty")
	}
	clean := filepath.Clean(outDir)
	if filepath.Dir(clean) == clean {
		return fmt.Errorf("refusing to use filesystem root as -out-dir: %s", outDir)
	}
	info, err := os.Stat(clean)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat -out-dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("-out-dir is not a directory: %s", outDir)
	}
	if _, err := os.Stat(filepath.Join(clean, runDirSentinel)); err == nil {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat run-dir sentinel: %w", err)
	}
	entries, err := os.ReadDir(clean)
	if err != nil {
		return fmt.Errorf("read -out-dir: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	return fmt.Errorf("-out-dir already exists and is not an empty canonical benchmark run dir: %s; choose a new directory or use a directory containing %s", outDir, runDirSentinel)
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		Docs:                      100000,
		BatchSize:                 16000,
		Indexes:                   2,
		Count:                     1,
		TreeEngine:                "command_wal_relaxed",
		Profile:                   "command_wal_relaxed",
		Formats:                   "template-v1,bson,json",
		StorageCells:              "index-vlog",
		LeafSegmentTargetBytes:    1048576,
		LeafgenPackFrameK:         16,
		FullLeafgenForce:          true,
		FullLeafgenMaxGenerations: 0,
		FullLeafgenIndexVacuum:    "offline",
		ProfileCount:              1,
	}
	fs := flag.NewFlagSet("collection_canonical_bench", flag.ContinueOnError)
	fs.StringVar(&cfg.RepoRoot, "repo-root", "", "Repository root; defaults to git rev-parse --show-toplevel")
	fs.StringVar(&cfg.OutDir, "out-dir", "", "Output directory; defaults under os.TempDir() as collection_canonical_bench_<timestamp>")
	fs.IntVar(&cfg.Docs, "docs", cfg.Docs, "Document count for canonical runs")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Batch size for collection inserts")
	fs.IntVar(&cfg.Indexes, "indexes", cfg.Indexes, "Secondary index count for primary comparison")
	fs.StringVar(&cfg.Benchtime, "benchtime", "", "go test -benchtime for timed matrix; defaults to <docs>x")
	fs.IntVar(&cfg.Count, "count", cfg.Count, "go test -count for timed matrix")
	fs.StringVar(&cfg.TreeEngine, "tree-engine", cfg.TreeEngine, "TreeDB collection benchmark engine")
	fs.StringVar(&cfg.Profile, "profile", cfg.Profile, "Fixture/raw TreeDB profile")
	fs.StringVar(&cfg.Formats, "formats", cfg.Formats, "Comma-separated TreeDB document formats for timed matrix and full leafgen fixture")
	fs.StringVar(&cfg.StorageCells, "storage-cells", cfg.StorageCells, "collection_bench_matrix storage cells")
	fs.Int64Var(&cfg.LeafSegmentTargetBytes, "leaf-segment-target-bytes", cfg.LeafSegmentTargetBytes, "Leaf vlog segment target bytes used by canonical compacted runs")
	fs.IntVar(&cfg.LeafgenPackFrameK, "leafgen-pack-frame-k", cfg.LeafgenPackFrameK, "Leaf pages per grouped frame for full leafgen pack")
	fs.BoolVar(&cfg.FullLeafgenForce, "leafgen-pack-force", cfg.FullLeafgenForce, "Force full leafgen pack candidate selection")
	fs.IntVar(&cfg.FullLeafgenMaxGenerations, "leafgen-pack-max-generations", cfg.FullLeafgenMaxGenerations, "Max generations to pack in full leafgen phase; 0 means no limit")
	fs.StringVar(&cfg.FullLeafgenIndexVacuum, "index-vacuum", cfg.FullLeafgenIndexVacuum, "Index vacuum mode for full leafgen fixture: offline, online, auto, or none")
	fs.BoolVar(&cfg.SkipTimedMatrix, "skip-timed-matrix", false, "Skip timed TreeDB/SQLite benchmark matrix")
	fs.BoolVar(&cfg.SkipOfflineRewrite, "skip-offline-rewrite", false, "Skip high-level offline compact matrix")
	fs.BoolVar(&cfg.SkipFullLeafgen, "skip-full-leafgen", false, "Skip full leafgen pack/GC fixture")
	fs.BoolVar(&cfg.SkipSQLite, "skip-sqlite", false, "Skip SQLite cells in timed matrix")
	fs.BoolVar(&cfg.ProfileTimedMatrix, "profile-timed-matrix", cfg.ProfileTimedMatrix, "Run separate pprof capture pass for timed TreeDB/SQLite matrix cells")
	fs.StringVar(&cfg.ProfileBenchtime, "profile-benchtime", cfg.ProfileBenchtime, "go test -benchtime for timed-matrix profile pass; defaults to -benchtime")
	fs.IntVar(&cfg.ProfileCount, "profile-count", cfg.ProfileCount, "go test -count for timed-matrix profile pass")
	fs.BoolVar(&cfg.AllowIncomplete, "allow-incomplete", false, "Write report even when guardrail checks fail")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.Docs <= 0 {
		return config{}, errors.New("-docs must be > 0")
	}
	if cfg.BatchSize <= 0 {
		return config{}, errors.New("-batch-size must be > 0")
	}
	if cfg.Indexes < 0 || cfg.Indexes > 3 {
		return config{}, errors.New("-indexes must be 0, 1, 2, or 3")
	}
	cfg.FullLeafgenIndexVacuum = strings.ToLower(strings.TrimSpace(cfg.FullLeafgenIndexVacuum))
	switch cfg.FullLeafgenIndexVacuum {
	case "offline", "online", "auto", "none":
	default:
		return config{}, errors.New("-index-vacuum must be one of offline, online, auto, or none")
	}
	if cfg.Count <= 0 {
		return config{}, errors.New("-count must be > 0")
	}
	if cfg.LeafSegmentTargetBytes < 0 {
		return config{}, errors.New("-leaf-segment-target-bytes must be >= 0")
	}
	if cfg.LeafgenPackFrameK < 0 {
		return config{}, errors.New("-leafgen-pack-frame-k must be >= 0")
	}
	if cfg.FullLeafgenMaxGenerations < 0 {
		return config{}, errors.New("-leafgen-pack-max-generations must be >= 0")
	}
	if cfg.ProfileCount <= 0 {
		return config{}, errors.New("-profile-count must be > 0")
	}
	return cfg, nil
}

func runBuildHelpers(cfg config, canon *canonicalRun) error {
	return runLoggedCommand(cfg, canon, "build_helpers", nil, "make", "collection-load-fixture", "treemap-bin")
}

func runTimedMatrix(cfg config, canon *canonicalRun) error {
	matrixDir := filepath.Join(cfg.OutDir, "timed_matrix")
	treePattern := fmt.Sprintf("^BenchmarkCollectionShapeInsertBatch$/^indexes_%d$", cfg.Indexes)
	sqlitePattern := fmt.Sprintf("^(BenchmarkSQLiteShapeInsertBatchJSON|BenchmarkSQLiteShapeInsertBatchNativeColumns)$/^indexes_%d$", cfg.Indexes)
	args := []string{
		"run", "./cmd/collection_bench_matrix",
		"-out-dir", matrixDir,
		"-formats", cfg.Formats,
		"-engine", cfg.TreeEngine,
		"-storage-cells", cfg.StorageCells,
		"-tree-bench-pattern", treePattern,
		"-sqlite-bench-pattern", sqlitePattern,
		"-batch-size", strconv.Itoa(cfg.BatchSize),
		"-benchtime", cfg.Benchtime,
		"-count", strconv.Itoa(cfg.Count),
		"-leaf-segment-target-bytes", strconv.FormatInt(cfg.LeafSegmentTargetBytes, 10),
		"-leafgen-pack-frame-k", strconv.Itoa(cfg.LeafgenPackFrameK),
	}
	if cfg.SkipSQLite {
		args = append(args, "-skip-sqlite")
	}
	if cfg.ProfileTimedMatrix {
		args = append(args,
			"-profile-cells",
			"-profile-benchtime", cfg.ProfileBenchtime,
			"-profile-count", strconv.Itoa(cfg.ProfileCount),
		)
	}
	if err := runLoggedCommand(cfg, canon, "timed_matrix", nil, "go", args...); err != nil {
		return err
	}
	canon.Artifacts["timed_matrix_dir"] = matrixDir
	if cfg.ProfileTimedMatrix {
		canon.Artifacts["timed_matrix_profiles"] = filepath.Join(matrixDir, "*", "profiles", "collection_profile_manifest.json")
	}
	return parseTimedMatrixReports(canon, matrixDir, cfg)
}

func runOfflineRewriteMatrix(cfg config, canon *canonicalRun, dirName, compactPhase, compactMode string) error {
	rewriteDir := filepath.Join(cfg.OutDir, dirName)
	env := map[string]string{
		"RUN_DIR":            rewriteDir,
		"DOCS":               strconv.Itoa(cfg.Docs),
		"BATCH":              strconv.Itoa(cfg.BatchSize),
		"PROFILE":            cfg.Profile,
		"COLLECTION_INDEXES": strings.Join(offlineRewriteIndexArgs(cfg.Indexes), " "),
		"COMPACT_MODE":       compactMode,
	}
	commandName := dirName + "_matrix"
	if err := runLoggedCommand(cfg, canon, commandName, env, "bash", "./scripts/treedb_collection_compression_matrix.sh"); err != nil {
		return err
	}
	canon.Artifacts[dirName+"_dir"] = rewriteDir
	canon.Artifacts[dirName+"_tsv"] = filepath.Join(rewriteDir, "compression_matrix.tsv")
	return parseOfflineRewriteTSV(canon, filepath.Join(rewriteDir, "compression_matrix.tsv"), cfg, compactPhase, compactMode)
}

func runFullLeafgenFixture(cfg config, canon *canonicalRun, formats []string) error {
	var errs []error
	for _, format := range formats {
		if err := runFullLeafgenFixtureForFormat(cfg, canon, format); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func runFullLeafgenFixtureForFormat(cfg config, canon *canonicalRun, format string) error {
	format = canonicalFormat(format)
	configName := canonicalConfigName("treedb", format, "collection", cfg.Indexes)
	fullDir := filepath.Join(cfg.OutDir, "full_leafgen_pack_gc", configName)
	summaryPath := filepath.Join(cfg.OutDir, "full_leafgen_pack_gc", configName+".fixture_summary.json")
	stderrPath := filepath.Join(cfg.OutDir, "logs", "full_leafgen_pack_gc_"+strings.ReplaceAll(format, "-", "_")+".stderr.log")
	if err := os.MkdirAll(filepath.Dir(summaryPath), 0755); err != nil {
		return err
	}
	args := []string{
		"-json",
		"-dir", fullDir,
		"-reset",
		"-docs", strconv.Itoa(cfg.Docs),
		"-batch-size", strconv.Itoa(cfg.BatchSize),
		"-format", format,
		"-indexes", strconv.Itoa(cfg.Indexes),
		"-profile", cfg.Profile,
		"-progress=false",
		"-leaf-segment-target-bytes", strconv.FormatInt(cfg.LeafSegmentTargetBytes, 10),
		"-leafgen-pack-gc",
		"-leafgen-pack-force=" + strconv.FormatBool(cfg.FullLeafgenForce),
		"-leafgen-pack-max-generations", strconv.Itoa(cfg.FullLeafgenMaxGenerations),
		"-leafgen-pack-frame-k", strconv.Itoa(cfg.LeafgenPackFrameK),
		"-index-vacuum", cfg.FullLeafgenIndexVacuum,
	}
	start := time.Now()
	cmd := exec.Command(filepath.Join(cfg.RepoRoot, "bin", "collection-load-fixture"), args...)
	cmd.Dir = cfg.RepoRoot
	stdout, err := os.Create(summaryPath)
	if err != nil {
		return err
	}
	defer stdout.Close()
	stderr, err := os.Create(stderrPath)
	if err != nil {
		return err
	}
	defer stderr.Close()
	cmd.Stdout = stdout
	cmd.Stderr = io.MultiWriter(os.Stderr, stderr)
	err = cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}
	canon.Commands = append(canon.Commands, commandRecord{
		Name:     "full_leafgen_pack_gc_" + strings.ReplaceAll(format, "-", "_"),
		Command:  append([]string{filepath.Join(cfg.RepoRoot, "bin", "collection-load-fixture")}, args...),
		WorkDir:  cfg.RepoRoot,
		LogPath:  stderrPath,
		ExitCode: exitCode,
		Duration: time.Since(start).String(),
	})
	if err != nil {
		return fmt.Errorf("full leafgen fixture %s: %w", format, err)
	}
	canon.Artifacts["full_leafgen_dir"] = filepath.Dir(summaryPath)
	canon.Artifacts["full_leafgen_fixture_dir_"+strings.ReplaceAll(format, "-", "_")] = fullDir
	canon.Artifacts["full_leafgen_summary_json_"+strings.ReplaceAll(format, "-", "_")] = summaryPath
	return parseFullLeafgenSummary(canon, summaryPath, cfg)
}

func runLoggedCommand(cfg config, canon *canonicalRun, name string, env map[string]string, bin string, args ...string) error {
	logPath := filepath.Join(cfg.OutDir, "logs", name+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return err
	}
	logFile, err := os.Create(logPath)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(bin, args...)
	cmd.Dir = cfg.RepoRoot
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = io.MultiWriter(os.Stdout, logFile)
	cmd.Stderr = io.MultiWriter(os.Stderr, logFile)
	start := time.Now()
	err = cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}
	canon.Commands = append(canon.Commands, commandRecord{
		Name:     name,
		Command:  append([]string{bin}, args...),
		Env:      env,
		WorkDir:  cfg.RepoRoot,
		LogPath:  logPath,
		ExitCode: exitCode,
		Duration: time.Since(start).String(),
	})
	if err != nil {
		return fmt.Errorf("%s: %w (log: %s)", name, err, logPath)
	}
	return nil
}

func parseTimedMatrixReports(canon *canonicalRun, matrixDir string, cfg config) error {
	return filepath.WalkDir(matrixDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Base(path) != "collections_report.json" {
			return nil
		}
		var report collectionReport
		if err := readJSONFile(path, &report); err != nil {
			return err
		}
		for _, section := range report.Sections {
			for _, bench := range section.Benchmarks {
				if strings.Contains(bench.Name, "BenchmarkCollectionShapeInsertBatch") {
					addTreeDBTimedRows(canon, report, bench, path, cfg)
				}
				if strings.Contains(bench.Name, "BenchmarkSQLiteShapeInsertBatch") {
					addSQLiteTimedRows(canon, report, bench, path, cfg)
				}
			}
		}
		return nil
	})
}

func addTreeDBTimedRows(canon *canonicalRun, report collectionReport, bench reportBenchmark, path string, cfg config) {
	metrics := bench.MeanMetrics
	docs := int(metricDefault(metrics, "stored_docs", float64(cfg.Docs)))
	indexes := int(metricDefault(metrics, "indexes/doc", float64(cfg.Indexes)))
	format := canonicalFormat(report.DocumentFormat)
	configName := canonicalConfigName("treedb", format, "collection", indexes)
	total := int64(metricDefault(metrics, "disk_total_bytes", 0))
	bpd := bytesPerDoc(total, docs)
	ns := bench.MeanNSPerOp
	evidence := productionEvidenceFromTreeDBTimedReport(report, metrics, cfg)
	canon.Results = append(canon.Results, resultRow{
		ConfigName:         configName,
		Engine:             report.BenchmarkEngine,
		Format:             format,
		Shape:              "collection",
		IndexCount:         indexes,
		DocumentCount:      docs,
		BenchmarkName:      bench.Name,
		Phase:              phasePostInsert,
		MaintenanceMode:    "none",
		TotalBytes:         int64Ptr(total),
		BytesPerDoc:        floatPtr(bpd),
		DocsPerSec:         floatPtr(bench.OpsPerSec),
		NsPerDoc:           floatPtr(ns),
		BatchSize:          cfg.BatchSize,
		BenchmarkTimed:     true,
		MeasurementKind:    "go_benchmark",
		MeasurementNote:    "post-insert benchmark size; not compacted",
		SourceArtifact:     path,
		MaintenanceStats:   copyMetrics(metrics, "prepare_ns/doc", "publish_ns/doc", "secondary_runs_ns/doc", "disk_total_bytes"),
		ProductionEvidence: evidence,
	})

	onlineTotal, onlineMetric := firstMetric(metrics,
		"leafgen_pack_gc_vacuum_disk_total_bytes_after",
		"leafgen_pack_gc_disk_total_bytes_after",
		"vlog_rewrite_gc_vacuum_disk_total_bytes_after",
		"vlog_rewrite_gc_disk_total_bytes_after",
	)
	if onlineMetric == "" {
		return
	}
	onlineStats := copyMetrics(metrics,
		"vlog_rewrite_ns/op",
		"vlog_rewrite_records_copied",
		"vlog_rewrite_segments_before",
		"vlog_rewrite_segments_after",
		"leafgen_plan_candidate_generations",
		"leafgen_pack_generations_matched",
		"leafgen_pack_leaf_frames_written",
		"leafgen_pack_max_leaf_frame_k",
		"leafgen_gc_bytes_deleted",
		"leafgen_pack_gc_vacuum_ns/op",
	)
	canon.Results = append(canon.Results, resultRow{
		ConfigName:      configName,
		Engine:          report.BenchmarkEngine,
		Format:          format,
		Shape:           "collection",
		IndexCount:      indexes,
		DocumentCount:   docs,
		BenchmarkName:   bench.Name,
		Phase:           phaseOnlineOnePassMaintenance,
		MaintenanceMode: "online_one_pass_vlog_rewrite_then_one_leafgen_pack",
		TotalBytes:      int64Ptr(int64(onlineTotal)),
		BytesPerDoc:     floatPtr(onlineTotal / float64(docs)),
		BatchSize:       cfg.BatchSize,
		BenchmarkTimed:  false,
		MeasurementKind: "benchmark_post_processing",
		MeasurementNote: "partial online maintenance from the timed benchmark harness; not full compaction",
		CompactionFlags: map[string]string{
			"report-vlog-rewrite":                "true",
			"report-leafgen-pack-gc":             "true",
			"leafgen-pack-force":                 "false",
			"leafgen-pack-max-generations":       "1",
			"leafgen-pack-frame-k":               strconv.Itoa(cfg.LeafgenPackFrameK),
			"leaf-segment-target-bytes":          strconv.FormatInt(cfg.LeafSegmentTargetBytes, 10),
			"post-maintenance-index-vacuum":      "online",
			"selected-online-total-bytes-metric": onlineMetric,
		},
		SourceArtifact:     path,
		MaintenanceStats:   onlineStats,
		ProductionEvidence: cloneProductionEvidence(evidence),
	})
}

func addSQLiteTimedRows(canon *canonicalRun, report collectionReport, bench reportBenchmark, path string, cfg config) {
	metrics := bench.MeanMetrics
	docs := int(metricDefault(metrics, "stored_docs", float64(cfg.Docs)))
	indexes := int(metricDefault(metrics, "indexes/doc", float64(cfg.Indexes)))
	format := "json"
	if strings.Contains(strings.ToLower(bench.Name), "nativecolumns") {
		format = "native-columns"
	}
	configName := canonicalConfigName("sqlite", format, "collection", indexes)
	total := int64(metricDefault(metrics, "disk_total_bytes", 0))
	canon.Results = append(canon.Results, resultRow{
		ConfigName:      configName,
		Engine:          report.BenchmarkEngine,
		Format:          format,
		Shape:           "collection",
		IndexCount:      indexes,
		DocumentCount:   docs,
		BenchmarkName:   bench.Name,
		Phase:           phasePostInsert,
		MaintenanceMode: "none",
		TotalBytes:      int64Ptr(total),
		BytesPerDoc:     floatPtr(bytesPerDoc(total, docs)),
		DocsPerSec:      floatPtr(bench.OpsPerSec),
		NsPerDoc:        floatPtr(bench.MeanNSPerOp),
		BatchSize:       cfg.BatchSize,
		BenchmarkTimed:  true,
		MeasurementKind: "go_benchmark",
		MeasurementNote: "SQLite post-insert size before VACUUM; not compacted",
		SourceArtifact:  path,
	})
	vacuumTotal, ok := metrics["sqlite_vacuum_disk_total_bytes_after"]
	if !ok {
		return
	}
	vacuumNS := metricDefault(metrics, "sqlite_vacuum_ns/op", 0)
	row := resultRow{
		ConfigName:      configName,
		Engine:          report.BenchmarkEngine,
		Format:          format,
		Shape:           "collection",
		IndexCount:      indexes,
		DocumentCount:   docs,
		BenchmarkName:   bench.Name,
		Phase:           phaseSQLiteVacuum,
		MaintenanceMode: "sqlite_vacuum",
		TotalBytes:      int64Ptr(int64(vacuumTotal)),
		BytesPerDoc:     floatPtr(vacuumTotal / float64(docs)),
		BatchSize:       cfg.BatchSize,
		BenchmarkTimed:  false,
		MeasurementKind: "benchmark_post_processing",
		MeasurementNote: "SQLite compacted baseline; use this for compacted-state comparisons",
		CompactionFlags: map[string]string{"sqlite": "VACUUM"},
		SourceArtifact:  path,
		MaintenanceStats: copyMetrics(metrics,
			"sqlite_vacuum_ns/op",
			"sqlite_vacuum_disk_total_bytes_before",
			"sqlite_vacuum_disk_total_bytes_after",
			"sqlite_vacuum_disk_total_bytes_delta",
		),
	}
	if vacuumNS > 0 {
		row.MaintenanceStats["sqlite_vacuum_ops/sec"] = 1e9 / vacuumNS
	}
	canon.Results = append(canon.Results, row)
}

func parseOfflineRewriteTSV(canon *canonicalRun, path string, cfg config, compactPhase, compactMode string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var header []string
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if header == nil {
			header = fields
			continue
		}
		row := map[string]string{}
		for i, key := range header {
			if i < len(fields) {
				row[key] = fields[i]
			}
		}
		mode := row["mode"]
		indexLabel := row["indexes"]
		shape := "collection"
		indexes := cfg.Indexes
		if mode == "raw_treedb" {
			shape = "raw"
			indexes = 0
		} else if indexLabel != "-" {
			parsed, err := strconv.Atoi(indexLabel)
			if err == nil {
				indexes = parsed
			}
		}
		total := parseInt64(row["total_bytes"])
		docs := cfg.Docs
		phase := phasePostInsert
		maintenance := "none"
		note := "offline compact matrix before-compact size; not compacted"
		flags := map[string]string(nil)
		if row["phase"] == "after_compact" || row["phase"] == "after_rewrite" {
			phase = compactPhase
			maintenance = "treemap_compact_rw_" + compactMode
			note = "High-level CompactStorage path using treemap compact -rw -mode " + compactMode
			flags = map[string]string{
				"treemap":                   "compact -rw -mode " + compactMode,
				"template-mode":             "off",
				"leaf-segment-target-bytes": "persisted/default",
				"index-vacuum":              "via CompactStorage/offline vacuum path",
			}
		}
		extra := map[string]string{
			"total_gzip_bytes":              row["total_gzip_bytes"],
			"total_bytes_per_gzip_byte":     row["total_bytes_per_gzip_byte"],
			"leaf_vlog_bytes":               row["leaf_vlog_bytes"],
			"leaf_vlog_bytes_per_gzip_byte": row["leaf_vlog_bytes_per_gzip_byte"],
			"index_db_bytes":                row["index_db_bytes"],
			"value_vlog_bytes":              row["value_vlog_bytes"],
		}
		evidence := &productionEvidence{
			StoragePolicy:          cfg.StorageCells,
			StorageCells:           cfg.StorageCells,
			LeafSegmentTargetBytes: cfg.LeafSegmentTargetBytes,
		}
		canon.Results = append(canon.Results, resultRow{
			ConfigName:         canonicalConfigName("treedb", "template-v1", shape, indexes),
			Engine:             "treedb_" + cfg.Profile,
			Format:             "template-v1",
			Shape:              shape,
			IndexCount:         indexes,
			DocumentCount:      docs,
			BenchmarkName:      "treedb_collection_compression_matrix",
			Phase:              phase,
			MaintenanceMode:    maintenance,
			TotalBytes:         int64Ptr(total),
			BytesPerDoc:        floatPtr(bytesPerDoc(total, docs)),
			BatchSize:          cfg.BatchSize,
			BenchmarkTimed:     false,
			MeasurementKind:    "offline_script",
			MeasurementNote:    note,
			CompactionFlags:    flags,
			SourceArtifact:     path,
			Extra:              extra,
			ProductionEvidence: evidence,
		})
	}
	return sc.Err()
}

func parseFullLeafgenSummary(canon *canonicalRun, path string, cfg config) error {
	var summary fixtureSummary
	if err := readJSONFile(path, &summary); err != nil {
		return err
	}
	docs := summary.Docs
	if docs == 0 {
		docs = cfg.Docs
	}
	indexes := summary.IndexCount
	format := canonicalFormat(summary.DocumentFormat)
	configName := canonicalConfigName("treedb", format, "collection", indexes)
	if summary.DiskUsageBeforeMaintenance != nil {
		total := summary.DiskUsageBeforeMaintenance.TotalBytes
		row := resultRow{
			ConfigName:      configName,
			Engine:          "treedb_" + summary.Profile,
			Format:          format,
			Shape:           "collection",
			IndexCount:      indexes,
			DocumentCount:   docs,
			BenchmarkName:   "collection-load-fixture",
			Phase:           phasePostInsert,
			MaintenanceMode: "none",
			TotalBytes:      int64Ptr(total),
			BytesPerDoc:     floatPtr(bytesPerDoc(total, docs)),
			DocsPerSec:      floatPtr(summary.InsertTiming.OpsPerSec),
			BatchSize:       summary.BatchSize,
			BenchmarkTimed:  false,
			MeasurementKind: "fixture_wall_timed",
			MeasurementNote: "full leafgen fixture pre-maintenance size; use benchmark-timed rows for primary throughput",
			SourceArtifact:  path,
		}
		if summary.InsertTiming.SecPerOp > 0 {
			row.NsPerDoc = floatPtr(summary.InsertTiming.SecPerOp * 1e9)
		}
		canon.Results = append(canon.Results, row)
	}
	if summary.DiskUsageFinal == nil {
		return nil
	}
	total := summary.DiskUsageFinal.TotalBytes
	stats := map[string]float64{}
	if summary.LeafGeneration != nil {
		stats["leafgen_candidate_generations"] = float64(summary.LeafGeneration.CandidateGenerations)
		stats["leafgen_candidate_bytes_total"] = float64(summary.LeafGeneration.CandidateBytesTotal)
		stats["leafgen_candidate_bytes_live"] = float64(summary.LeafGeneration.CandidateBytesLive)
		stats["leafgen_candidate_bytes_dead"] = float64(summary.LeafGeneration.CandidateBytesDead)
		stats["leafgen_expected_reclaim_bytes"] = float64(summary.LeafGeneration.ExpectedReclaimBytes)
		stats["leafgen_pack_generations_matched"] = float64(summary.LeafGeneration.PackGenerationsMatched)
		stats["leafgen_pack_leaf_pages_copied"] = float64(summary.LeafGeneration.PackLeafPagesCopied)
		stats["leafgen_pack_leaf_frames_written"] = float64(summary.LeafGeneration.PackLeafFramesWritten)
		stats["leafgen_pack_max_leaf_frame_k"] = float64(summary.LeafGeneration.PackMaxLeafFrameK)
		stats["leafgen_gc_generations_deleted"] = float64(summary.LeafGeneration.GCGenerationsDeleted)
		stats["leafgen_gc_files_deleted"] = float64(summary.LeafGeneration.GCFilesDeleted)
		stats["leafgen_gc_bytes_deleted"] = float64(summary.LeafGeneration.GCBytesDeleted)
	}
	if summary.IndexVacuum != nil {
		stats["index_vacuum_seconds"] = summary.IndexVacuum.Timing.Seconds
		stats["index_vacuum_disk_bytes_before"] = float64(summary.IndexVacuum.DiskBytesBefore)
		stats["index_vacuum_disk_bytes_after"] = float64(summary.IndexVacuum.DiskBytesAfter)
		stats["index_vacuum_index_db_bytes_before"] = float64(summary.IndexVacuum.IndexDBBytesBefore)
		stats["index_vacuum_index_db_bytes_after"] = float64(summary.IndexVacuum.IndexDBBytesAfter)
	}
	canon.Results = append(canon.Results, resultRow{
		ConfigName:      configName,
		Engine:          "treedb_" + summary.Profile,
		Format:          format,
		Shape:           "collection",
		IndexCount:      indexes,
		DocumentCount:   docs,
		BenchmarkName:   "collection-load-fixture",
		Phase:           phaseFullLeafgenPackGC,
		MaintenanceMode: "full_leafgen_pack_gc",
		TotalBytes:      int64Ptr(total),
		BytesPerDoc:     floatPtr(bytesPerDoc(total, docs)),
		BatchSize:       summary.BatchSize,
		BenchmarkTimed:  false,
		MeasurementKind: "fixture_post_processing",
		MeasurementNote: "full leafgen pack/GC with explicit flags followed by configured index vacuum",
		CompactionFlags: map[string]string{
			"leaf-segment-target-bytes":    strconv.FormatInt(cfg.LeafSegmentTargetBytes, 10),
			"leafgen-pack-gc":              "true",
			"leafgen-pack-force":           strconv.FormatBool(cfg.FullLeafgenForce),
			"leafgen-pack-max-generations": strconv.Itoa(cfg.FullLeafgenMaxGenerations),
			"leafgen-pack-frame-k":         strconv.Itoa(cfg.LeafgenPackFrameK),
			"index-vacuum":                 cfg.FullLeafgenIndexVacuum,
		},
		SourceArtifact:   path,
		MaintenanceStats: stats,
		ProductionEvidence: &productionEvidence{
			StoragePolicy:          storagePolicyFromFixture(summary),
			StorageCells:           cfg.StorageCells,
			LeafSegmentTargetBytes: cfg.LeafSegmentTargetBytes,
		},
	})
	return nil
}

func productionEvidenceFromTreeDBTimedReport(report collectionReport, metrics map[string]float64, cfg config) *productionEvidence {
	ev := &productionEvidence{
		StoragePolicy:                       report.StoragePolicy,
		StorageCells:                        cfg.StorageCells,
		PagerChunkSize:                      report.PagerChunkSize,
		PagerSyncConcurrency:                report.PagerSyncConcurrency,
		LeafSegmentTargetBytes:              cfg.LeafSegmentTargetBytes,
		GOMAXPROCS:                          intMetricPtr(metrics, "gomaxprocs"),
		PhysicalCores:                       intMetricPtr(metrics, "physical_cores"),
		FlushAdmissionConfiguredConcurrency: intMetricPtr(metrics, "flush_admission_configured_concurrency"),
		FlushAdmissionEffectiveConcurrency:  intMetricPtr(metrics, "flush_admission_effective_concurrency"),
		FlushAdmissionGOMAXPROCS:            intMetricPtr(metrics, "flush_admission_gomaxprocs"),
		FlushAdmissionPhysicalCores:         intMetricPtr(metrics, "flush_admission_physical_cores"),
		FlushAdmissionAdmitted:              boolMetricPtr(metrics, "flush_admission_admitted"),
		FlushAdmissionSpanNative:            boolMetricPtr(metrics, "flush_admission_span_native"),
		FlushAdmissionBacklogCoalescing:     boolMetricPtr(metrics, "flush_admission_backlog_coalescing"),
		FlushSpanCandidateOps:               floatMetricPtr(metrics, "flush_span_candidate_ops"),
		FlushSpanEligibleOps:                floatMetricPtr(metrics, "flush_span_eligible_ops"),
		FlushSpanUsedOps:                    floatMetricPtr(metrics, "flush_span_used_ops"),
		FlushSpanFallbacks:                  floatMetricPtr(metrics, "flush_span_fallbacks"),
		OrderedRootSpanCandidateOps:         floatMetricPtr(metrics, "ordered_root_span_candidate_ops"),
		OrderedRootSpanEligibleOps:          floatMetricPtr(metrics, "ordered_root_span_eligible_ops"),
		OrderedRootSpanUsedOps:              floatMetricPtr(metrics, "ordered_root_span_used_ops"),
		OrderedRootSpanFallbacks:            floatMetricPtr(metrics, "ordered_root_span_fallbacks"),
	}
	route, observations, candidate, eligible, used, fallbacks := selectProducerRouteEvidence(metrics)
	if route != "" {
		ev.ProducerRoute = route
		ev.ProducerRouteObservations = floatPtr(observations)
		ev.ProducerRouteCandidateOps = floatPtr(candidate)
		ev.ProducerRouteEligibleOps = floatPtr(eligible)
		ev.ProducerRouteUsedOps = floatPtr(used)
		ev.ProducerRouteFallbacks = floatPtr(fallbacks)
	} else if intMetricDefault(metrics, "indexes/doc", cfg.Indexes) == 0 &&
		floatPtrPositive(ev.FlushSpanCandidateOps) &&
		floatPtrPositive(ev.FlushSpanUsedOps) {
		ev.ProducerRoute = noSecondaryIndexProducerRoute
		ev.ProducerRouteCandidateOps = cloneFloatPtr(ev.FlushSpanCandidateOps)
		ev.ProducerRouteEligibleOps = cloneFloatPtr(ev.FlushSpanEligibleOps)
		ev.ProducerRouteUsedOps = cloneFloatPtr(ev.FlushSpanUsedOps)
		ev.ProducerRouteFallbacks = cloneFloatPtr(ev.FlushSpanFallbacks)
	}
	return ev
}

func selectProducerRouteEvidence(metrics map[string]float64) (route string, observations, candidate, eligible, used, fallbacks float64) {
	bestScore := 0.0
	for _, candidateRoute := range orderedRootProducerRoutes() {
		prefix := "ordered_root_route_" + candidateRoute + "_"
		routeObservations := metrics[prefix+"observations"]
		routeCandidate := metrics[prefix+"candidate_ops"]
		routeEligible := metrics[prefix+"eligible_ops"]
		routeUsed := metrics[prefix+"used_ops"]
		routeFallbacks := metrics[prefix+"fallbacks"]
		score := routeUsed*1_000_000 + routeFallbacks*10_000 + routeEligible*100 + routeCandidate + routeObservations
		if score <= bestScore {
			continue
		}
		bestScore = score
		route = candidateRoute
		observations = routeObservations
		candidate = routeCandidate
		eligible = routeEligible
		used = routeUsed
		fallbacks = routeFallbacks
	}
	return route, observations, candidate, eligible, used, fallbacks
}

func orderedRootProducerRoutes() []string {
	return []string{
		"command_wal_publish",
		"collection_buffered_roots",
		"system_delta_builder_publish",
		"multi_index_group_publish",
		"delta_batch_publish",
		"overlay_cold_build",
		"direct_publish",
		"grouped_publish",
		"read_only_prepare",
	}
}

func cloneProductionEvidence(ev *productionEvidence) *productionEvidence {
	if ev == nil {
		return nil
	}
	cp := *ev
	return &cp
}

func storagePolicyFromFixture(summary fixtureSummary) string {
	return fmt.Sprintf("data_outer=%t,index_outer=%t", summary.DataOuterLeavesInValueLog, summary.IndexOuterLeavesInValueLog)
}

func validateCanonicalRun(canon *canonicalRun) []guardrailCheck {
	var checks []guardrailCheck
	add := func(sev, code, msg string) {
		checks = append(checks, guardrailCheck{Severity: sev, Code: code, Message: msg})
	}
	add("warning", "phase.online_one_pass.partial", "online_one_pass_maintenance is partial online maintenance; do not describe it as full compaction")
	if checksBlockCompactedClaims(canon.Checks) || !hasCanonicalExhaustiveCompactEvidence(canon) {
		add("error", "exhaustive_compact_required", "exhaustive_compact must complete before canonical TreeDB compacted-size comparisons or byte-minimized headlines are reportable")
	}
	if !hasCanonicalProductionEvidence(canon) {
		add("error", "production_evidence_required", "canonical TreeDB compacted-size comparisons require matching post-insert producer route, concurrency, admission, and fallback-counter evidence")
	}
	for _, r := range canon.Results {
		if r.GeneratedAt == "" || r.RunDir == "" || r.Worktree == "" || r.Commit == "" || len(r.CommandLine) == 0 {
			add("error", "missing_row_run_metadata", fmt.Sprintf("%s/%s is missing row-level run metadata", r.ConfigName, r.Phase))
		}
		if r.TotalBytes != nil && r.DocumentCount <= 0 {
			add("error", "missing_document_count", fmt.Sprintf("%s/%s has bytes but no document count", r.ConfigName, r.Phase))
		}
		if r.Shape != "collection" && r.Shape != "raw" {
			add("error", "missing_shape_label", fmt.Sprintf("%s/%s has non-canonical shape label %q", r.ConfigName, r.Phase, r.Shape))
		}
		if (isTreeDBCompactedPhase(r.Phase) || r.Phase == phaseSQLiteVacuum) && len(r.CompactionFlags) == 0 {
			add("error", "missing_compaction_flags", fmt.Sprintf("%s/%s is missing compaction flags", r.ConfigName, r.Phase))
		}
		if isTreeDBTimedPostInsertProductionRow(r) && !hasRowProductionEvidence(r) {
			add("error", "missing_production_evidence", fmt.Sprintf("%s/%s is missing producer route, selected concurrency/admission, span-native used ops, or fallback-counter evidence", r.ConfigName, r.Phase))
		}
	}
	for _, treeDBConfig := range compactedTreeDBConfigNames(canon) {
		if hasTreeDBConfigExhaustiveCompactEvidence(canon, treeDBConfig) && !hasTreeDBConfigProductionEvidence(canon, treeDBConfig) {
			add("error", "production_evidence_required", fmt.Sprintf("%s has compacted evidence but no matching post-insert production route/concurrency evidence", treeDBConfig))
		}
	}
	sqliteJSONConfig := sqliteJSONConfigName(canon)
	sqliteNativeConfig := sqliteNativeConfigName(canon)
	sqliteRows := hasSQLiteRows(canon.Results)
	sqliteSkipped := canon.Config.SkipSQLite || !sqliteRows
	if canon.Config.SkipSQLite {
		add("warning", "sqlite.skipped", "SQLite rows were intentionally skipped; fair compacted SQLite comparisons are omitted")
	} else if !sqliteRows {
		add("warning", "sqlite.auto_skipped", "SQLite rows are absent; treating them as auto-skipped because SQLite benchmarks may be unavailable")
	} else {
		if findResult(canon.Results, sqliteJSONConfig, phaseSQLiteVacuum) == nil {
			add("error", "missing_sqlite_json_vacuum", "SQLite JSON VACUUM result is required for fair compacted-state comparison")
		}
		if findResult(canon.Results, sqliteNativeConfig, phaseSQLiteVacuum) == nil {
			add("error", "missing_sqlite_native_vacuum", "SQLite native-columns VACUUM result is required for fair compacted-state comparison")
		}
		if findResult(canon.Results, sqliteNativeConfig, phaseSQLiteVacuum) == nil {
			for _, treeDBConfig := range compactedTreeDBConfigNames(canon) {
				if findResult(canon.Results, treeDBConfig, phaseOfflineCompact) != nil {
					add("error", "unfair_compacted_comparison", "TreeDB offline/full compaction must be compared against SQLite after VACUUM, not SQLite post-insert")
					break
				}
			}
		}
	}
	if findResult(canon.Results, "treedb_template_v1_raw", phaseOfflineCompact) != nil {
		add("info", "raw_shape_labeled", "raw TreeDB rows are labeled shape=raw and should not be mixed with collection rows without that label")
	}
	for _, cmp := range comparisonsForReport(canon) {
		if isTreeDBCompactedPhase(cmp.TreeDBPhase) && cmp.SQLitePhase != phaseSQLiteVacuum {
			add("error", "compacted_compared_to_sqlite_post_insert", fmt.Sprintf("%s compares TreeDB compacted phase %s against SQLite phase %s", cmp.ComparisonName, cmp.TreeDBPhase, cmp.SQLitePhase))
		}
	}
	if !sqliteSkipped {
		for _, treeDBConfig := range compactedTreeDBConfigNames(canon) {
			if !hasTreeDBConfigExhaustiveCompactEvidence(canon, treeDBConfig) {
				continue
			}
			for _, treedbPhase := range compactedTreeDBPhases() {
				if findResult(canon.Results, treeDBConfig, treedbPhase) == nil {
					continue
				}
				for _, sqliteName := range []string{sqliteNativeConfig, sqliteJSONConfig} {
					if findComparison(canon.Comparisons, treeDBConfig, treedbPhase, sqliteName, phaseSQLiteVacuum) == nil {
						add("error", "missing_compacted_ratio", fmt.Sprintf("missing derived compacted comparison for %s/%s vs %s/%s", treeDBConfig, treedbPhase, sqliteName, phaseSQLiteVacuum))
					}
				}
			}
		}
	}
	return checks
}

func hasSQLiteRows(rows []resultRow) bool {
	for _, r := range rows {
		if strings.HasPrefix(r.ConfigName, "sqlite_") || strings.HasPrefix(r.Engine, "sqlite") {
			return true
		}
	}
	return false
}

func finalizeRunMetadata(canon *canonicalRun) {
	if canon == nil {
		return
	}
	for i := range canon.Results {
		canon.Results[i].GeneratedAt = canon.GeneratedAt
		canon.Results[i].RunDir = canon.RunDir
		canon.Results[i].Worktree = canon.Worktree
		canon.Results[i].Branch = canon.Branch
		canon.Results[i].Commit = canon.Commit
		canon.Results[i].CommandLine = append([]string(nil), canon.CommandLine...)
	}
	for i := range canon.Comparisons {
		canon.Comparisons[i].GeneratedAt = canon.GeneratedAt
		canon.Comparisons[i].RunDir = canon.RunDir
		canon.Comparisons[i].Worktree = canon.Worktree
		canon.Comparisons[i].Branch = canon.Branch
		canon.Comparisons[i].Commit = canon.Commit
		canon.Comparisons[i].CommandLine = append([]string(nil), canon.CommandLine...)
	}
}

func buildCompactedComparisons(canon *canonicalRun) []comparisonRow {
	if canon == nil {
		return nil
	}
	if compactedClaimsBlocked(canon) {
		return nil
	}
	var comparisons []comparisonRow
	sqliteConfigs := []string{sqliteNativeConfigName(canon), sqliteJSONConfigName(canon)}
	for _, treeDBConfig := range compactedTreeDBConfigNames(canon) {
		if !hasTreeDBConfigExhaustiveCompactEvidence(canon, treeDBConfig) || !hasTreeDBConfigProductionEvidence(canon, treeDBConfig) {
			continue
		}
		for _, treedbPhase := range compactedTreeDBPhases() {
			treeRow := findResult(canon.Results, treeDBConfig, treedbPhase)
			if !hasPositiveBytesPerDoc(treeRow) {
				continue
			}
			for _, sqliteConfig := range sqliteConfigs {
				sqliteRow := findResult(canon.Results, sqliteConfig, phaseSQLiteVacuum)
				if !hasPositiveBytesPerDoc(sqliteRow) {
					continue
				}
				comparisonName := fmt.Sprintf("%s_%s_vs_%s_%s", treeDBConfig, treedbPhase, sqliteConfig, phaseSQLiteVacuum)
				comparisons = append(comparisons, comparisonRow{
					ComparisonName:    comparisonName,
					TreeDBConfigName:  treeDBConfig,
					TreeDBPhase:       treedbPhase,
					SQLiteConfigName:  sqliteConfig,
					SQLitePhase:       phaseSQLiteVacuum,
					TreeDBBytesPerDoc: *treeRow.BytesPerDoc,
					SQLiteBytesPerDoc: *sqliteRow.BytesPerDoc,
					SmallerRatio:      *sqliteRow.BytesPerDoc / *treeRow.BytesPerDoc,
					ComparisonBasis:   "TreeDB compacted phase versus SQLite after VACUUM",
				})
			}
		}
	}
	return comparisons
}

func comparisonsForReport(canon *canonicalRun) []comparisonRow {
	if canon == nil {
		return nil
	}
	if compactedClaimsBlocked(canon) {
		return nil
	}
	if len(canon.Comparisons) > 0 {
		return filterCompactedComparisonsForEvidence(canon, canon.Comparisons)
	}
	return buildCompactedComparisons(canon)
}

func compactedClaimsBlocked(canon *canonicalRun) bool {
	if canon == nil {
		return false
	}
	return checksBlockCompactedClaims(canon.Checks) || !hasCanonicalExhaustiveCompactEvidence(canon) || !hasCanonicalProductionEvidence(canon)
}

func hasCanonicalExhaustiveCompactEvidence(canon *canonicalRun) bool {
	if canon == nil {
		return false
	}
	return hasTreeDBConfigExhaustiveCompactEvidence(canon, compactedTreeDBConfigName(canon))
}

func hasTreeDBConfigExhaustiveCompactEvidence(canon *canonicalRun, treeDBConfig string) bool {
	if canon == nil || strings.TrimSpace(treeDBConfig) == "" {
		return false
	}
	return hasPositiveBytesPerDoc(findResult(canon.Results, treeDBConfig, phaseExhaustiveCompact))
}

func hasCanonicalProductionEvidence(canon *canonicalRun) bool {
	if canon == nil {
		return false
	}
	return hasTreeDBConfigProductionEvidence(canon, compactedTreeDBConfigName(canon))
}

func hasTreeDBConfigProductionEvidence(canon *canonicalRun, treeDBConfig string) bool {
	if canon == nil || strings.TrimSpace(treeDBConfig) == "" {
		return false
	}
	for _, row := range canon.Results {
		if row.ConfigName == treeDBConfig && isTreeDBTimedPostInsertProductionRow(row) && hasRowProductionEvidence(row) {
			return true
		}
	}
	return false
}

func isTreeDBTimedPostInsertProductionRow(row resultRow) bool {
	return row.Phase == phasePostInsert &&
		row.Shape == "collection" &&
		strings.HasPrefix(row.ConfigName, "treedb_") &&
		row.BenchmarkTimed
}

func hasRowProductionEvidence(row resultRow) bool {
	ev := row.ProductionEvidence
	if ev == nil {
		return false
	}
	return strings.TrimSpace(ev.StoragePolicy) != "" &&
		ev.GOMAXPROCS != nil &&
		ev.FlushAdmissionEffectiveConcurrency != nil &&
		ev.FlushAdmissionAdmitted != nil &&
		ev.FlushAdmissionSpanNative != nil &&
		ev.FlushAdmissionBacklogCoalescing != nil &&
		ev.FlushSpanFallbacks != nil &&
		ev.OrderedRootSpanFallbacks != nil &&
		hasProducerPathEvidence(row)
}

func hasProducerPathEvidence(row resultRow) bool {
	ev := row.ProductionEvidence
	if ev == nil {
		return false
	}
	if strings.TrimSpace(ev.ProducerRoute) != "" &&
		floatPtrPositive(ev.ProducerRouteCandidateOps) &&
		floatPtrPositive(ev.ProducerRouteUsedOps) {
		return true
	}
	return row.IndexCount == 0 &&
		floatPtrPositive(ev.FlushSpanCandidateOps) &&
		floatPtrPositive(ev.FlushSpanUsedOps)
}

func filterCompactedComparisonsForEvidence(canon *canonicalRun, comps []comparisonRow) []comparisonRow {
	if canon == nil || len(comps) == 0 {
		return comps
	}
	var filtered []comparisonRow
	for _, cmp := range comps {
		if strings.HasPrefix(cmp.TreeDBConfigName, "treedb_") &&
			isTreeDBCompactedPhase(cmp.TreeDBPhase) &&
			(!hasTreeDBConfigExhaustiveCompactEvidence(canon, cmp.TreeDBConfigName) ||
				!hasTreeDBConfigProductionEvidence(canon, cmp.TreeDBConfigName)) {
			continue
		}
		filtered = append(filtered, cmp)
	}
	return filtered
}

func checksBlockCompactedClaims(checks []guardrailCheck) bool {
	for _, check := range checks {
		if check.Code == "phase.exhaustive_compact.failed" {
			return true
		}
	}
	return false
}

func findComparison(comparisons []comparisonRow, treeConfig, treePhase, sqliteConfig, sqlitePhase string) *comparisonRow {
	for i := range comparisons {
		cmp := &comparisons[i]
		if cmp.TreeDBConfigName == treeConfig && cmp.TreeDBPhase == treePhase &&
			cmp.SQLiteConfigName == sqliteConfig && cmp.SQLitePhase == sqlitePhase {
			return cmp
		}
	}
	return nil
}

func isTreeDBCompactedPhase(phase string) bool {
	for _, compactedPhase := range compactedTreeDBPhases() {
		if phase == compactedPhase {
			return true
		}
	}
	return false
}

func compactedTreeDBPhases() []string {
	return []string{phaseOfflineCompact, phaseExhaustiveCompact, phaseFullLeafgenPackGC}
}

func writeOutputs(canon *canonicalRun) error {
	sortResults(canon.Results)
	resultsPath := filepath.Join(canon.RunDir, "benchmark_results.json")
	canon.Artifacts["benchmark_results_json"] = resultsPath
	canon.Artifacts["benchmark_summary_md"] = filepath.Join(canon.RunDir, "benchmark_summary.md")
	canon.Artifacts["benchmark_matrix_csv"] = filepath.Join(canon.RunDir, "benchmark_matrix.csv")
	if err := writeJSON(resultsPath, canon); err != nil {
		return err
	}
	if err := os.WriteFile(canon.Artifacts["benchmark_summary_md"], []byte(renderMarkdownReport(canon)), 0644); err != nil {
		return err
	}
	return writeCSV(canon.Artifacts["benchmark_matrix_csv"], canon.Results)
}

func renderMarkdownReport(canon *canonicalRun) string {
	var sb strings.Builder
	sb.WriteString("# TreeDB Collections Canonical Benchmark\n\n")
	sb.WriteString("## Executive Summary\n\n")
	sb.WriteString(renderExecutiveSummary(canon))
	sb.WriteString("\n\n")

	sb.WriteString("## Benchmark Configuration\n\n")
	writeKVTable(&sb, [][2]string{
		{"generated_at", canon.GeneratedAt},
		{"run_dir", canon.RunDir},
		{"worktree", canon.Worktree},
		{"branch", canon.Branch},
		{"commit", canon.Commit},
		{"command_line", displayCommandLine(canon.CommandLine)},
		{"docs", strconv.Itoa(canon.Config.Docs)},
		{"batch_size", strconv.Itoa(canon.Config.BatchSize)},
		{"indexes", strconv.Itoa(canon.Config.Indexes)},
		{"tree_engine", canon.Config.TreeEngine},
		{"profile", canon.Config.Profile},
		{"formats", strings.Join(canon.Config.Formats, ",")},
		{"leaf_segment_target_bytes", strconv.FormatInt(canon.Config.LeafSegmentTargetBytes, 10)},
		{"leafgen_pack_force", strconv.FormatBool(canon.Config.FullLeafgenForce)},
		{"leafgen_pack_max_generations", strconv.Itoa(canon.Config.FullLeafgenMaxGenerations)},
		{"leafgen_pack_frame_k", strconv.Itoa(canon.Config.LeafgenPackFrameK)},
		{"index_vacuum", canon.Config.FullLeafgenIndexVacuum},
		{"profile_timed_matrix", strconv.FormatBool(canon.Config.ProfileTimedMatrix)},
		{"profile_benchtime", canon.Config.ProfileBenchtime},
		{"profile_count", strconv.Itoa(canon.Config.ProfileCount)},
	})
	sb.WriteString("\n")

	sb.WriteString("## Throughput Results\n\n")
	sb.WriteString("These rows are benchmark-timed post-insert measurements. Maintenance rows are excluded from throughput ranking.\n\n")
	sb.WriteString("| Config | Format | Indexes | Docs/sec | ns/doc | Batch size | Source |\n")
	sb.WriteString("| --- | --- | ---: | ---: | ---: | ---: | --- |\n")
	for _, r := range throughputRows(canon.Results) {
		sb.WriteString(fmt.Sprintf("| `%s` | %s | %d | %s | %s | %d | %s |\n",
			r.ConfigName, r.Format, r.IndexCount, formatFloatPtr(r.DocsPerSec, 0), formatFloatPtr(r.NsPerDoc, 1), r.BatchSize, r.MeasurementKind))
	}
	sb.WriteString("\n")

	sb.WriteString("## Post-Insert Storage Comparison\n\n")
	sb.WriteString("These rows use the timed benchmark matrix post-insert basis. They are fair to compare with each other, but they are not compacted-state numbers.\n\n")
	sb.WriteString("| Config | Format | Indexes | Total bytes | B/doc | Docs/sec |\n")
	sb.WriteString("| --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, r := range postInsertComparisonRows(canon.Results) {
		sb.WriteString(fmt.Sprintf("| `%s` | %s | %d | %s | %s | %s |\n",
			r.ConfigName, r.Format, r.IndexCount, formatIntPtr(r.TotalBytes), formatFloatPtr(r.BytesPerDoc, 1), formatFloatPtr(r.DocsPerSec, 0)))
	}
	sb.WriteString("\n")

	sb.WriteString("## Storage Results by Phase\n\n")
	sb.WriteString("| Config | Shape | Phase | Total bytes | B/doc | Measurement | Note |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | --- | --- |\n")
	for _, r := range storageRows(canon.Results) {
		sb.WriteString(fmt.Sprintf("| `%s` | %s | `%s` | %s | %s | %s | %s |\n",
			r.ConfigName, r.Shape, r.Phase, formatIntPtr(r.TotalBytes), formatFloatPtr(r.BytesPerDoc, 1), r.MeasurementKind, markdownEscape(r.MeasurementNote)))
	}
	sb.WriteString("\n")

	sb.WriteString("## Production Evidence\n\n")
	writeProductionEvidence(&sb, canon)
	sb.WriteString("\n")

	sb.WriteString("## Fair Compacted-State Comparison\n\n")
	sb.WriteString("TreeDB compacted rows are compared with SQLite after `VACUUM`. Use `exhaustive_compact` for byte-minimized benchmark/VACUUM-equivalent claims; do not compare TreeDB compacted rows only against SQLite `post_insert` rows.\n\n")
	writeFairComparison(&sb, canon)
	sb.WriteString("\n")

	sb.WriteString("## Maintenance/Compaction Details\n\n")
	sb.WriteString("| Phase | Canonical meaning | Required comparison basis |\n")
	sb.WriteString("| --- | --- | --- |\n")
	sb.WriteString("| `post_insert` | Size after insert benchmark/fixture and correctness flush/checkpoint. | Compare with other post-insert rows only. |\n")
	sb.WriteString("| `online_one_pass_maintenance` | Partial online maintenance from benchmark harness; currently one online rewrite/GC path and one leafgen-pack pass. | Do not present as full compaction. |\n")
	sb.WriteString("| `offline_compact` | Production high-level `treemap compact <dir> -rw -mode full` path. | Compare with SQLite `sqlite_vacuum`; not the byte-minimized headline. |\n")
	sb.WriteString("| `exhaustive_compact` | Benchmark/VACUUM-equivalent `treemap compact <dir> -rw -mode exhaustive` path. | Preferred TreeDB compacted-size headline versus SQLite `sqlite_vacuum`. |\n")
	sb.WriteString("| `full_leafgen_pack_gc` | Diagnostic forced/full leaf generation pack/GC with explicit knobs and configured index vacuum. | Diagnostic only; compare with SQLite `sqlite_vacuum` if reported. |\n")
	sb.WriteString("| `sqlite_vacuum` | SQLite compacted baseline after `VACUUM`. | Required baseline for compacted-state comparison. |\n\n")
	sb.WriteString("Full leafgen knobs recorded for this run:\n\n")
	writeKVTable(&sb, [][2]string{
		{"leaf-segment-target-bytes", strconv.FormatInt(canon.Config.LeafSegmentTargetBytes, 10)},
		{"leafgen-pack-gc", "true"},
		{"leafgen-pack-force", strconv.FormatBool(canon.Config.FullLeafgenForce)},
		{"leafgen-pack-max-generations", strconv.Itoa(canon.Config.FullLeafgenMaxGenerations)},
		{"leafgen-pack-frame-k", strconv.Itoa(canon.Config.LeafgenPackFrameK)},
		{"index-vacuum", canon.Config.FullLeafgenIndexVacuum},
	})
	sb.WriteString("\n")

	sb.WriteString("## Guardrail Checks\n\n")
	sb.WriteString("| Severity | Code | Message |\n")
	sb.WriteString("| --- | --- | --- |\n")
	for _, check := range canon.Checks {
		sb.WriteString(fmt.Sprintf("| %s | `%s` | %s |\n", check.Severity, check.Code, markdownEscape(check.Message)))
	}
	sb.WriteString("\n")

	sb.WriteString("## Raw Results Appendix\n\n")
	sb.WriteString(fmt.Sprintf("Machine-readable JSON: `%s`\n\n", filepath.Base(canon.Artifacts["benchmark_results_json"])))
	sb.WriteString(fmt.Sprintf("CSV matrix: `%s`\n\n", filepath.Base(canon.Artifacts["benchmark_matrix_csv"])))
	sb.WriteString("| Config | Phase | Engine | Format | Indexes | Docs | Bytes | B/doc | Docs/sec | ns/doc |\n")
	sb.WriteString("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range canon.Results {
		sb.WriteString(fmt.Sprintf("| `%s` | `%s` | %s | %s | %d | %d | %s | %s | %s | %s |\n",
			r.ConfigName, r.Phase, r.Engine, r.Format, r.IndexCount, r.DocumentCount, formatIntPtr(r.TotalBytes), formatFloatPtr(r.BytesPerDoc, 1), formatFloatPtr(r.DocsPerSec, 0), formatFloatPtr(r.NsPerDoc, 1)))
	}
	sb.WriteString("\n")

	sb.WriteString("## Reproduction Commands\n\n")
	sb.WriteString("Canonical entry point:\n\n")
	sb.WriteString("```bash\n")
	sb.WriteString("./scripts/bench_collections_canonical.sh\n")
	sb.WriteString("# or\n")
	sb.WriteString("make bench-collections-canonical\n")
	sb.WriteString("```\n\n")
	sb.WriteString("Exact command records from this run:\n\n")
	for _, cmd := range canon.Commands {
		sb.WriteString(fmt.Sprintf("### %s\n\n", cmd.Name))
		if len(cmd.Env) > 0 {
			keys := make([]string, 0, len(cmd.Env))
			for k := range cmd.Env {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				sb.WriteString(fmt.Sprintf("%s=%s ", k, shellQuote(cmd.Env[k])))
			}
		}
		sb.WriteString(shellQuoteCommand(cmd.Command))
		sb.WriteString("\n\n")
	}
	return sb.String()
}

func renderExecutiveSummary(canon *canonicalRun) string {
	fastest := fastestThroughput(canon.Results)
	offline := findResult(canon.Results, compactedTreeDBConfigName(canon), phaseOfflineCompact)
	exhaustive := findResult(canon.Results, compactedTreeDBConfigName(canon), phaseExhaustiveCompact)
	sqliteJSON := findResult(canon.Results, sqliteJSONConfigName(canon), phaseSQLiteVacuum)
	sqliteNative := findResult(canon.Results, sqliteNativeConfigName(canon), phaseSQLiteVacuum)
	if !compactedClaimsBlocked(canon) && hasPositiveBytesPerDoc(offline) && hasPositiveBytesPerDoc(exhaustive) &&
		hasPositiveBytesPerDoc(sqliteJSON) && hasPositiveBytesPerDoc(sqliteNative) {
		engine := "TreeDB"
		if fastest != nil {
			engine = fastest.ConfigName
		}
		offlineNative := *sqliteNative.BytesPerDoc / *offline.BytesPerDoc
		exhaustiveNative := *sqliteNative.BytesPerDoc / *exhaustive.BytesPerDoc
		offlineJSON := *sqliteJSON.BytesPerDoc / *offline.BytesPerDoc
		exhaustiveJSON := *sqliteJSON.BytesPerDoc / *exhaustive.BytesPerDoc
		return fmt.Sprintf("`%s` is the fastest indexed ingest row in this run. TreeDB byte-minimized %s %s collection storage is %.1f B/doc via exhaustive compact; production offline compact is %.1f B/doc. Compared with SQLite after `VACUUM`, exhaustive compact is about %.1fx smaller than SQLite native columns and %.1fx smaller than SQLite JSON; production offline compact is about %.1fx and %.1fx smaller, respectively.",
			engine, indexCountLabel(canonicalIndexCount(canon)), primaryFormat(canon), *exhaustive.BytesPerDoc, *offline.BytesPerDoc, exhaustiveNative, exhaustiveJSON, offlineNative, offlineJSON)
	}
	return "This report separates post-insert, partial online maintenance, production offline compact, exhaustive compact, full leafgen pack/GC diagnostics, and SQLite VACUUM states. Some compacted-state rows are missing, so the fair compacted-state headline could not be generated."
}

func writeFairComparison(sb *strings.Builder, canon *canonicalRun) {
	sqliteJSONConfig := sqliteJSONConfigName(canon)
	sqliteNativeConfig := sqliteNativeConfigName(canon)
	sqliteJSON := findResult(canon.Results, sqliteJSONConfig, phaseSQLiteVacuum)
	sqliteNative := findResult(canon.Results, sqliteNativeConfig, phaseSQLiteVacuum)
	comparisons := comparisonsForReport(canon)
	sb.WriteString("SQLite compacted baselines:\n\n")
	sb.WriteString("| Config | Phase | B/doc | Total bytes |\n")
	sb.WriteString("| --- | --- | ---: | ---: |\n")
	for _, r := range []*resultRow{sqliteJSON, sqliteNative} {
		if r == nil {
			continue
		}
		sb.WriteString(fmt.Sprintf("| `%s` | `%s` | %s | %s |\n", r.ConfigName, r.Phase, formatFloatPtr(r.BytesPerDoc, 1), formatIntPtr(r.TotalBytes)))
	}
	sb.WriteString("\nTreeDB compacted rows versus SQLite after `VACUUM`:\n\n")
	if compactedClaimsBlocked(canon) {
		sb.WriteString("TreeDB compacted-size comparisons are suppressed because `exhaustive_compact` did not complete for the supported canonical path.\n")
		return
	}
	sb.WriteString("| TreeDB config | Phase | B/doc | vs SQLite native-columns VACUUM | vs SQLite JSON VACUUM |\n")
	sb.WriteString("| --- | --- | ---: | ---: | ---: |\n")
	for _, treeDBConfig := range compactedTreeDBConfigNames(canon) {
		for _, phase := range compactedTreeDBPhases() {
			nativeCmp := findComparison(comparisons, treeDBConfig, phase, sqliteNativeConfig, phaseSQLiteVacuum)
			jsonCmp := findComparison(comparisons, treeDBConfig, phase, sqliteJSONConfig, phaseSQLiteVacuum)
			if nativeCmp == nil && jsonCmp == nil {
				continue
			}
			bpd := 0.0
			if nativeCmp != nil {
				bpd = nativeCmp.TreeDBBytesPerDoc
			} else {
				bpd = jsonCmp.TreeDBBytesPerDoc
			}
			nativeRatio := "-"
			jsonRatio := "-"
			if nativeCmp != nil {
				nativeRatio = fmt.Sprintf("%.1fx smaller", nativeCmp.SmallerRatio)
			}
			if jsonCmp != nil {
				jsonRatio = fmt.Sprintf("%.1fx smaller", jsonCmp.SmallerRatio)
			}
			sb.WriteString(fmt.Sprintf("| `%s` | `%s` | %.1f | %s | %s |\n", treeDBConfig, phase, bpd, nativeRatio, jsonRatio))
		}
	}
	sb.WriteString("\nDerived comparison rows are also emitted in `benchmark_results.json` under `comparisons`.\n")
}

func writeProductionEvidence(sb *strings.Builder, canon *canonicalRun) {
	var rows []resultRow
	for _, row := range canon.Results {
		if row.ProductionEvidence != nil && strings.HasPrefix(row.ConfigName, "treedb_") {
			rows = append(rows, row)
		}
	}
	if len(rows) == 0 {
		sb.WriteString("No TreeDB production evidence rows were emitted.\n")
		return
	}
	sb.WriteString("| Config | Phase | Producer route | Route used ops | Route fallbacks | GOMAXPROCS | Physical cores | Effective concurrency | Admitted | Span-native | Backlog coalescing | Storage policy |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |\n")
	for _, row := range rows {
		ev := row.ProductionEvidence
		sb.WriteString(fmt.Sprintf("| `%s` | `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			row.ConfigName,
			row.Phase,
			markdownInlineCode(ev.ProducerRoute),
			formatFloatPtr(ev.ProducerRouteUsedOps, 0),
			formatFloatPtr(ev.ProducerRouteFallbacks, 0),
			formatIntMetricPtr(ev.GOMAXPROCS),
			formatIntMetricPtr(ev.PhysicalCores),
			formatIntMetricPtr(ev.FlushAdmissionEffectiveConcurrency),
			formatBoolPtr(ev.FlushAdmissionAdmitted),
			formatBoolPtr(ev.FlushAdmissionSpanNative),
			formatBoolPtr(ev.FlushAdmissionBacklogCoalescing),
			markdownEscape(ev.StoragePolicy)))
	}
}

func writeCSV(path string, rows []resultRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"generated_at", "run_dir", "branch", "commit", "config_name", "phase", "engine", "format", "shape", "index_count", "document_count", "benchmark_name", "maintenance_mode", "total_bytes", "bytes_per_doc", "docs_per_sec", "ns_per_doc", "batch_size", "benchmark_timed", "measurement_kind", "source_artifact"}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, r := range rows {
		record := []string{
			r.GeneratedAt,
			r.RunDir,
			r.Branch,
			r.Commit,
			r.ConfigName,
			r.Phase,
			r.Engine,
			r.Format,
			r.Shape,
			strconv.Itoa(r.IndexCount),
			strconv.Itoa(r.DocumentCount),
			r.BenchmarkName,
			r.MaintenanceMode,
			intPtrString(r.TotalBytes),
			floatPtrString(r.BytesPerDoc),
			floatPtrString(r.DocsPerSec),
			floatPtrString(r.NsPerDoc),
			strconv.Itoa(r.BatchSize),
			strconv.FormatBool(r.BenchmarkTimed),
			r.MeasurementKind,
			r.SourceArtifact,
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeJSON(path string, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

func readJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func gitOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func resolveBranch(repoRoot string) string {
	if branch, _ := gitOutput(repoRoot, "branch", "--show-current"); branch != "" {
		return branch
	}
	if branch, _ := gitOutput(repoRoot, "rev-parse", "--abbrev-ref", "HEAD"); branch != "" && branch != "HEAD" {
		return branch
	}
	branch, _ := gitOutput(repoRoot, "describe", "--all", "--always", "HEAD")
	return branch
}

func splitCSV(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func canonicalFormatList(formats []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, format := range formats {
		format = canonicalFormat(format)
		if format == "" || seen[format] {
			continue
		}
		seen[format] = true
		out = append(out, format)
	}
	return out
}

func canonicalFormat(format string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(format), "_", "-"))
}

func canonicalConfigName(engine, format, shape string, indexes int) string {
	format = strings.ReplaceAll(canonicalFormat(format), "-", "_")
	if engine == "sqlite" {
		return fmt.Sprintf("sqlite_%s_%d_indexes", format, indexes)
	}
	if shape == "raw" {
		return fmt.Sprintf("%s_%s_raw", engine, format)
	}
	return fmt.Sprintf("%s_%s_%s_%d_indexes", engine, format, shape, indexes)
}

func primaryFormat(canon *canonicalRun) string {
	if canon != nil {
		for _, f := range canon.Config.Formats {
			format := canonicalFormat(f)
			if format == "template-v1" {
				return "template-v1"
			}
		}
		if len(canon.Config.Formats) > 0 {
			return canonicalFormat(canon.Config.Formats[0])
		}
	}
	return "template-v1"
}

func compactedTreeDBConfigName(canon *canonicalRun) string {
	return canonicalConfigName("treedb", primaryFormat(canon), "collection", canonicalIndexCount(canon))
}

func compactedTreeDBConfigNames(canon *canonicalRun) []string {
	if canon == nil {
		return nil
	}
	seen := make(map[string]bool)
	var out []string
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		out = append(out, name)
	}
	for _, format := range canon.Config.Formats {
		format = canonicalFormat(format)
		add(canonicalConfigName("treedb", format, "collection", canonicalIndexCount(canon)))
	}
	for _, row := range canon.Results {
		if row.Shape != "collection" || !strings.HasPrefix(row.ConfigName, "treedb_") {
			continue
		}
		if row.IndexCount != canonicalIndexCount(canon) {
			continue
		}
		if row.Phase != phaseOfflineCompact && row.Phase != phaseExhaustiveCompact && row.Phase != phaseFullLeafgenPackGC {
			continue
		}
		add(row.ConfigName)
	}
	if len(out) == 0 {
		add(compactedTreeDBConfigName(canon))
	}
	return out
}

func sqliteJSONConfigName(canon *canonicalRun) string {
	return canonicalConfigName("sqlite", "json", "collection", canonicalIndexCount(canon))
}

func sqliteNativeConfigName(canon *canonicalRun) string {
	return canonicalConfigName("sqlite", "native-columns", "collection", canonicalIndexCount(canon))
}

func canonicalIndexCount(canon *canonicalRun) int {
	if canon != nil && canon.Config.Indexes >= 0 {
		return canon.Config.Indexes
	}
	return 0
}

func offlineRewriteIndexArgs(configured int) []string {
	seen := make(map[int]bool, 4)
	var indexes []int
	for _, idx := range []int{0, 1, 2, configured} {
		if idx < 0 || seen[idx] {
			continue
		}
		seen[idx] = true
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	out := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		out = append(out, strconv.Itoa(idx))
	}
	return out
}

func indexCountLabel(indexes int) string {
	switch indexes {
	case 1:
		return "one-index"
	case 2:
		return "two-index"
	default:
		return fmt.Sprintf("%d-index", indexes)
	}
}

func hasPositiveBytesPerDoc(row *resultRow) bool {
	return row != nil && row.BytesPerDoc != nil && *row.BytesPerDoc > 0
}

func floatPtrPositive(v *float64) bool {
	return v != nil && *v > 0
}

func metricDefault(metrics map[string]float64, key string, def float64) float64 {
	if metrics == nil {
		return def
	}
	if v, ok := metrics[key]; ok {
		return v
	}
	return def
}

func firstMetric(metrics map[string]float64, keys ...string) (float64, string) {
	for _, key := range keys {
		if v, ok := metrics[key]; ok {
			return v, key
		}
	}
	return 0, ""
}

func copyMetrics(metrics map[string]float64, keys ...string) map[string]float64 {
	out := map[string]float64{}
	for _, key := range keys {
		if v, ok := metrics[key]; ok {
			out[key] = v
		}
	}
	return out
}

func intMetricPtr(metrics map[string]float64, key string) *int {
	value, ok := metrics[key]
	if !ok {
		return nil
	}
	n := int(math.Round(value))
	return &n
}

func intMetricDefault(metrics map[string]float64, key string, def int) int {
	value, ok := metrics[key]
	if !ok {
		return def
	}
	return int(math.Round(value))
}

func boolMetricPtr(metrics map[string]float64, key string) *bool {
	value, ok := metrics[key]
	if !ok {
		return nil
	}
	b := value >= 0.5
	return &b
}

func floatMetricPtr(metrics map[string]float64, key string) *float64 {
	value, ok := metrics[key]
	if !ok {
		return nil
	}
	return floatPtr(value)
}

func cloneFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	return floatPtr(*v)
}

func bytesPerDoc(total int64, docs int) float64 {
	if docs <= 0 {
		return 0
	}
	return float64(total) / float64(docs)
}

func parseInt64(v string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return n
}

func int64Ptr(v int64) *int64     { return &v }
func floatPtr(v float64) *float64 { return &v }

func findResult(rows []resultRow, configName, phase string) *resultRow {
	for i := range rows {
		if rows[i].ConfigName == configName && rows[i].Phase == phase {
			return &rows[i]
		}
	}
	return nil
}

func throughputRows(rows []resultRow) []resultRow {
	var out []resultRow
	for _, r := range rows {
		if r.Phase == phasePostInsert && r.BenchmarkTimed && r.DocsPerSec != nil {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return *out[i].DocsPerSec > *out[j].DocsPerSec
	})
	return out
}

func storageRows(rows []resultRow) []resultRow {
	var out []resultRow
	for _, r := range rows {
		if r.TotalBytes != nil {
			out = append(out, r)
		}
	}
	sortResults(out)
	return out
}

func postInsertComparisonRows(rows []resultRow) []resultRow {
	var out []resultRow
	for _, r := range rows {
		if r.Phase == phasePostInsert && r.BenchmarkTimed && r.TotalBytes != nil {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BytesPerDoc != nil && out[j].BytesPerDoc != nil && *out[i].BytesPerDoc != *out[j].BytesPerDoc {
			return *out[i].BytesPerDoc < *out[j].BytesPerDoc
		}
		return out[i].ConfigName < out[j].ConfigName
	})
	return out
}

func fastestThroughput(rows []resultRow) *resultRow {
	var best *resultRow
	for i := range rows {
		r := &rows[i]
		if r.Phase != phasePostInsert || !r.BenchmarkTimed || r.DocsPerSec == nil {
			continue
		}
		if best == nil || *r.DocsPerSec > *best.DocsPerSec {
			best = r
		}
	}
	return best
}

func sortResults(rows []resultRow) {
	phaseRank := map[string]int{
		phasePostInsert:               0,
		phaseOnlineOnePassMaintenance: 1,
		phaseOfflineCompact:           2,
		phaseFullLeafgenPackGC:        3,
		phaseSQLiteVacuum:             4,
	}
	sort.Slice(rows, func(i, j int) bool {
		ri, rj := rows[i], rows[j]
		if ri.ConfigName != rj.ConfigName {
			return ri.ConfigName < rj.ConfigName
		}
		if phaseRank[ri.Phase] != phaseRank[rj.Phase] {
			return phaseRank[ri.Phase] < phaseRank[rj.Phase]
		}
		return ri.MeasurementKind < rj.MeasurementKind
	})
}

func hasErrorCheck(checks []guardrailCheck) bool {
	for _, c := range checks {
		if c.Severity == "error" {
			return true
		}
	}
	return false
}

func formatIntPtr(v *int64) string {
	if v == nil {
		return "-"
	}
	return formatInt(*v)
}

func formatInt(v int64) string {
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	s := strconv.FormatInt(v, 10)
	var parts []string
	for len(s) > 3 {
		parts = append(parts, s[len(s)-3:])
		s = s[:len(s)-3]
	}
	parts = append(parts, s)
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return sign + strings.Join(parts, ",")
}

func formatFloatPtr(v *float64, prec int) string {
	if v == nil {
		return "-"
	}
	return formatFloat(*v, prec)
}

func formatFloat(v float64, prec int) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "-"
	}
	return strconv.FormatFloat(v, 'f', prec, 64)
}

func formatIntMetricPtr(v *int) string {
	if v == nil {
		return "-"
	}
	return strconv.Itoa(*v)
}

func formatBoolPtr(v *bool) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatBool(*v)
}

func intPtrString(v *int64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatInt(*v, 10)
}

func floatPtrString(v *float64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatFloat(*v, 'f', -1, 64)
}

func markdownEscape(v string) string {
	v = strings.ReplaceAll(v, "|", "\\|")
	v = strings.ReplaceAll(v, "\n", " ")
	return v
}

func markdownInlineCode(v string) string {
	v = markdownEscape(v)
	maxRun := 0
	run := 0
	for _, r := range v {
		if r == '`' {
			run++
			if run > maxRun {
				maxRun = run
			}
			continue
		}
		run = 0
	}
	fence := strings.Repeat("`", maxRun+1)
	if strings.HasPrefix(v, "`") || strings.HasSuffix(v, "`") {
		return fence + " " + v + " " + fence
	}
	return fence + v + fence
}

func writeKVTable(sb *strings.Builder, rows [][2]string) {
	sb.WriteString("| Key | Value |\n")
	sb.WriteString("| --- | --- |\n")
	for _, row := range rows {
		sb.WriteString(fmt.Sprintf("| %s | %s |\n", markdownInlineCode(row[0]), markdownInlineCode(row[1])))
	}
}

func shellQuoteCommand(args []string) string {
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		parts = append(parts, shellQuote(arg))
	}
	return strings.Join(parts, " ")
}

func displayCommandLine(args []string) string {
	if len(args) == 1 {
		return args[0]
	}
	return shellQuoteCommand(args)
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return !(r == '/' || r == '.' || r == '-' || r == '_' || r == '=' || r == ':' || r == ',' || r == '+' ||
			(r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'))
	}) == -1 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}
