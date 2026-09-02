package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/cmd/internal/treedbstats"
)

const schemaVersion = "treedb-out-of-core-smoke/v1"
const envCommandLine = "TREEDB_OUT_OF_CORE_SMOKE_COMMAND"
const runDirSentinel = ".treedb_out_of_core_smoke_run"

type config struct {
	RepoRoot               string
	OutDir                 string
	RawKeys                int
	CollectionDocs         int
	BatchSize              int
	ValueSize              int
	ReadWorkers            int
	FormatsCSV             string
	IndexesCSV             string
	Profile                string
	LeafSegmentTargetBytes int64
	CacheBudgetBytes       int64
	RetiredMmapBudgetBytes int64
	MaxDeadMappings        int
	IncludeMongo           bool
	SkipRaw                bool
	SkipCollections        bool
	FailOnWarnings         bool
	AllowExistingRunDir    bool
	internalRawWorker      bool
	rawDir                 string
	rawJSONPath            string
	formats                []string
	indexes                []int
}

type smokeRun struct {
	SchemaVersion string             `json:"schema_version"`
	GeneratedAt   string             `json:"generated_at"`
	RunDir        string             `json:"run_dir"`
	Worktree      string             `json:"worktree"`
	Branch        string             `json:"branch"`
	Commit        string             `json:"commit"`
	CommandLine   []string           `json:"command_line"`
	Config        smokeConfig        `json:"config"`
	Artifacts     map[string]string  `json:"artifacts"`
	Commands      []commandRecord    `json:"commands"`
	Results       []resultRow        `json:"results"`
	Deferred      []deferredWorkload `json:"deferred_workloads,omitempty"`
	Checks        []guardrailCheck   `json:"guardrail_checks"`
}

type smokeConfig struct {
	RawKeys                int      `json:"raw_keys"`
	CollectionDocs         int      `json:"collection_docs"`
	BatchSize              int      `json:"batch_size"`
	ValueSize              int      `json:"value_size"`
	ReadWorkers            int      `json:"read_workers"`
	Formats                []string `json:"formats"`
	Indexes                []int    `json:"indexes"`
	Profile                string   `json:"profile"`
	LeafSegmentTargetBytes int64    `json:"leaf_segment_target_bytes"`
	Budgets                budgets  `json:"budgets"`
	IncludeMongo           bool     `json:"include_mongo"`
}

type budgets struct {
	CacheBudgetBytes       int64 `json:"cache_budget_bytes"`
	RetiredMmapBudgetBytes int64 `json:"retired_mmap_budget_bytes"`
	MaxDeadMappings        int   `json:"max_dead_mappings"`
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
	GeneratedAt      string            `json:"generated_at,omitempty"`
	RunDir           string            `json:"run_dir,omitempty"`
	CommandLine      []string          `json:"command_line,omitempty"`
	ConfigName       string            `json:"config_name"`
	WorkloadName     string            `json:"workload_name"`
	Engine           string            `json:"engine"`
	Shape            string            `json:"shape"`
	Format           string            `json:"format,omitempty"`
	IndexCount       int               `json:"index_count,omitempty"`
	DocumentCount    int               `json:"document_count,omitempty"`
	KeyCount         int               `json:"key_count,omitempty"`
	ItemCount        int               `json:"item_count"`
	BatchSize        int               `json:"batch_size,omitempty"`
	Phase            string            `json:"phase"`
	MeasurementKind  string            `json:"measurement_kind"`
	BenchmarkTimed   bool              `json:"benchmark_timed"`
	OpsPerSec        *float64          `json:"ops_per_sec,omitempty"`
	DocsPerSec       *float64          `json:"docs_per_sec,omitempty"`
	NsPerItem        *float64          `json:"ns_per_item,omitempty"`
	TotalBytes       *uint64           `json:"total_bytes,omitempty"`
	BytesPerItem     *float64          `json:"bytes_per_item,omitempty"`
	ComponentBytes   map[string]uint64 `json:"component_bytes,omitempty"`
	Budgets          budgets           `json:"budgets"`
	PressureClaimed  bool              `json:"pressure_claimed"`
	Mmap             *mmapStats        `json:"mmap,omitempty"`
	Cache            *cacheStats       `json:"cache,omitempty"`
	StatsAvailable   []string          `json:"stats_available,omitempty"`
	SourceArtifact   string            `json:"source_artifact,omitempty"`
	MeasurementNotes string            `json:"measurement_notes,omitempty"`
}

type mmapStats struct {
	Remaps             uint64   `json:"remaps,omitempty"`
	DeadMappings       uint64   `json:"dead_mappings,omitempty"`
	DeadMappingCap     uint64   `json:"dead_mapping_cap,omitempty"`
	ActiveSegments     uint64   `json:"active_segments,omitempty"`
	ActiveBytes        uint64   `json:"active_bytes,omitempty"`
	CurrentSegments    uint64   `json:"current_segments,omitempty"`
	CurrentBytes       uint64   `json:"current_bytes,omitempty"`
	SealedSegments     uint64   `json:"sealed_segments,omitempty"`
	SealedBytes        uint64   `json:"sealed_bytes,omitempty"`
	DeadBytes          uint64   `json:"dead_bytes,omitempty"`
	Hits               uint64   `json:"hits,omitempty"`
	MissOutOfRange     uint64   `json:"miss_out_of_range,omitempty"`
	MissNoMapping      uint64   `json:"miss_no_mapping,omitempty"`
	MissDeadMappingCap uint64   `json:"miss_dead_mapping_cap,omitempty"`
	FallbackReadAt     uint64   `json:"fallback_readat,omitempty"`
	HitRatio           *float64 `json:"hit_ratio,omitempty"`
}

type cacheStats struct {
	OuterLeafBlockHits     *uint64  `json:"outer_leaf_block_hits,omitempty"`
	OuterLeafBlockMisses   *uint64  `json:"outer_leaf_block_misses,omitempty"`
	OuterLeafBlockHitRatio *float64 `json:"outer_leaf_block_hit_ratio,omitempty"`
	OuterLeafBlockEntries  *uint64  `json:"outer_leaf_block_entries,omitempty"`
	OuterLeafBlockCapacity *uint64  `json:"outer_leaf_block_capacity,omitempty"`
}

type guardrailCheck struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
	Subject  string `json:"subject,omitempty"`
}

type deferredWorkload struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
	Issue  string `json:"issue,omitempty"`
}

type rawWorkerSummary struct {
	GeneratedAt       string               `json:"generated_at"`
	Dir               string               `json:"dir"`
	Keys              int                  `json:"keys"`
	BatchSize         int                  `json:"batch_size"`
	ValueSize         int                  `json:"value_size"`
	Profile           string               `json:"profile"`
	LeafSegmentTarget int64                `json:"leaf_segment_target_bytes"`
	Timings           map[string]timing    `json:"timings"`
	DiskUsageFinal    diskUsage            `json:"disk_usage_final"`
	DiskUsageByPhase  map[string]diskUsage `json:"disk_usage_by_phase,omitempty"`
	TreeDBStatsFinal  map[string]string    `json:"treedb_stats_final,omitempty"`
}

type fixtureSummary struct {
	GeneratedAt                string            `json:"generated_at"`
	Dir                        string            `json:"dir"`
	Collection                 string            `json:"collection"`
	DocumentFormat             string            `json:"document_format"`
	Profile                    string            `json:"profile"`
	Docs                       int               `json:"docs"`
	BatchSize                  int               `json:"batch_size"`
	IndexCount                 int               `json:"index_count"`
	LeafSegmentTargetBytes     int64             `json:"leaf_segment_target_bytes,omitempty"`
	InsertTiming               timing            `json:"insert_timing"`
	DiskUsageFinal             diskUsage         `json:"disk_usage_final"`
	TreeDBStatsFinal           map[string]string `json:"treedb_stats_final,omitempty"`
	DataOuterLeavesInValueLog  bool              `json:"data_outer_leaves_in_value_log"`
	IndexOuterLeavesInValueLog bool              `json:"index_outer_leaves_in_value_log"`
}

type mongoGatewaySummary struct {
	Target                     string                 `json:"target"`
	TreeDBDir                  string                 `json:"treedb_dir,omitempty"`
	Documents                  int                    `json:"documents"`
	BatchSize                  int                    `json:"batch_size"`
	SecondaryIndexes           int                    `json:"secondary_indexes"`
	ClientMode                 string                 `json:"client_mode"`
	TreeDBProfile              string                 `json:"treedb_profile,omitempty"`
	TreeDBDocumentFormat       string                 `json:"treedb_document_format,omitempty"`
	TreeDBMaintenanceMode      string                 `json:"treedb_maintenance_mode,omitempty"`
	Phases                     []mongoGatewayPhase    `json:"phases"`
	TreeDBDiskAfterLoad        *mongoGatewayDiskUsage `json:"treedb_disk_after_load,omitempty"`
	TreeDBDiskAfterCheckpoint  *mongoGatewayDiskUsage `json:"treedb_disk_after_checkpoint,omitempty"`
	TreeDBDiskAfterMaintenance *mongoGatewayDiskUsage `json:"treedb_disk_after_maintenance,omitempty"`
	TreeDBStatsFinal           map[string]string      `json:"treedb_stats_final,omitempty"`
}

type mongoGatewayPhase struct {
	Name           string  `json:"name"`
	Operations     int     `json:"operations"`
	OpsPerSecond   float64 `json:"ops_per_sec"`
	SampledNsPerOp float64 `json:"sampled_ns_per_op,omitempty"`
	DurationMillis float64 `json:"duration_ms,omitempty"`
}

type mongoGatewayDiskUsage struct {
	TotalBytes int64            `json:"total_bytes"`
	Paths      map[string]int64 `json:"paths,omitempty"`
}

type timing struct {
	Seconds   float64 `json:"seconds"`
	SecPerOp  float64 `json:"sec_per_op,omitempty"`
	OpsPerSec float64 `json:"ops_per_sec,omitempty"`
}

type diskUsage struct {
	TotalBytes  uint64        `json:"total_bytes"`
	BytesPerDoc float64       `json:"bytes_per_doc,omitempty"`
	FileCount   int           `json:"file_count"`
	TopFiles    []fileSummary `json:"top_files,omitempty"`
}

type fileSummary struct {
	Path  string `json:"path"`
	Bytes uint64 `json:"bytes"`
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "treedb-out-of-core-smoke: %v\n", err)
		os.Exit(2)
	}
	if cfg.internalRawWorker {
		if err := runRawWorker(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "raw worker: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "treedb-out-of-core-smoke: %v\n", err)
		os.Exit(1)
	}
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		RawKeys:                5000,
		CollectionDocs:         1500,
		BatchSize:              500,
		ValueSize:              256,
		ReadWorkers:            2,
		FormatsCSV:             "template-v1,bson,json",
		IndexesCSV:             "0,1,2",
		Profile:                string(treedb.ProfileBenchUnsafe),
		LeafSegmentTargetBytes: 32 << 10,
		CacheBudgetBytes:       32 << 10,
		RetiredMmapBudgetBytes: 32 << 10,
		MaxDeadMappings:        2,
	}
	fs := flag.NewFlagSet("treedb-out-of-core-smoke", flag.ContinueOnError)
	fs.StringVar(&cfg.OutDir, "out-dir", "", "output run directory; empty creates a timestamped /tmp directory")
	fs.IntVar(&cfg.RawKeys, "raw-keys", cfg.RawKeys, "raw TreeDB key count")
	fs.IntVar(&cfg.CollectionDocs, "collection-docs", cfg.CollectionDocs, "collection document count per shape")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "batch/write size")
	fs.IntVar(&cfg.ValueSize, "value-size", cfg.ValueSize, "raw TreeDB value size in bytes")
	fs.IntVar(&cfg.ReadWorkers, "read-workers", cfg.ReadWorkers, "raw TreeDB parallel read workers")
	fs.StringVar(&cfg.FormatsCSV, "formats", cfg.FormatsCSV, "collection formats CSV: template-v1,bson,json")
	fs.StringVar(&cfg.IndexesCSV, "indexes", cfg.IndexesCSV, "collection index counts CSV")
	fs.StringVar(&cfg.Profile, "profile", cfg.Profile, "TreeDB profile: "+treedb.BenchmarkProfileFlagHelp)
	fs.Int64Var(&cfg.LeafSegmentTargetBytes, "leaf-segment-target-bytes", cfg.LeafSegmentTargetBytes, "small leaf_vlog segment target used to force churn")
	fs.Int64Var(&cfg.CacheBudgetBytes, "cache-budget-bytes", cfg.CacheBudgetBytes, "reported current-leaf/page cache budget for pressure validation")
	fs.Int64Var(&cfg.RetiredMmapBudgetBytes, "retired-mmap-budget-bytes", cfg.RetiredMmapBudgetBytes, "reported retired mmap budget for pressure validation")
	fs.IntVar(&cfg.MaxDeadMappings, "max-dead-mappings", cfg.MaxDeadMappings, "TREEDB_VLOG_MAX_DEAD_MAPPINGS for child workloads")
	fs.BoolVar(&cfg.IncludeMongo, "include-mongo", false, "include a tiny Mongo gateway smoke workload; normally deferred to #1141")
	fs.BoolVar(&cfg.SkipRaw, "skip-raw", false, "skip raw TreeDB focused helper")
	fs.BoolVar(&cfg.SkipCollections, "skip-collections", false, "skip collection fixture matrix")
	fs.BoolVar(&cfg.FailOnWarnings, "fail-on-warnings", false, "return non-zero when guardrail warnings are present")
	fs.BoolVar(&cfg.AllowExistingRunDir, "allow-existing-run-dir", false, "allow writing into an existing smoke run directory")
	fs.BoolVar(&cfg.internalRawWorker, "internal-raw-worker", false, "internal raw TreeDB worker mode")
	fs.StringVar(&cfg.rawDir, "raw-dir", "", "internal raw worker DB directory")
	fs.StringVar(&cfg.rawJSONPath, "raw-json", "", "internal raw worker JSON output path")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	if cfg.RawKeys <= 0 {
		return cfg, errors.New("-raw-keys must be > 0")
	}
	if cfg.CollectionDocs <= 0 {
		return cfg, errors.New("-collection-docs must be > 0")
	}
	if cfg.BatchSize <= 0 {
		return cfg, errors.New("-batch-size must be > 0")
	}
	if cfg.ValueSize <= 0 {
		return cfg, errors.New("-value-size must be > 0")
	}
	if cfg.ReadWorkers <= 0 {
		return cfg, errors.New("-read-workers must be > 0")
	}
	if cfg.LeafSegmentTargetBytes <= 0 {
		return cfg, errors.New("-leaf-segment-target-bytes must be > 0")
	}
	if cfg.CacheBudgetBytes <= 0 {
		return cfg, errors.New("-cache-budget-bytes must be > 0")
	}
	if cfg.RetiredMmapBudgetBytes <= 0 {
		return cfg, errors.New("-retired-mmap-budget-bytes must be > 0")
	}
	if cfg.MaxDeadMappings <= 0 {
		return cfg, errors.New("-max-dead-mappings must be > 0")
	}
	var err error
	cfg.formats, err = parseFormats(cfg.FormatsCSV)
	if err != nil {
		return cfg, err
	}
	cfg.indexes, err = parseIndexes(cfg.IndexesCSV)
	if err != nil {
		return cfg, err
	}
	cfg.RepoRoot, err = os.Getwd()
	if err != nil {
		return cfg, err
	}
	if cfg.OutDir == "" && !cfg.internalRawWorker {
		cfg.OutDir = filepath.Join(os.TempDir(), fmt.Sprintf("gomap_out_of_core_smoke_%s", time.Now().UTC().Format("20060102T150405Z")))
	}
	if cfg.OutDir != "" {
		cfg.OutDir, err = filepath.Abs(cfg.OutDir)
		if err != nil {
			return cfg, err
		}
	}
	return cfg, nil
}

func parseFormats(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		v := strings.ToLower(strings.TrimSpace(part))
		if v == "" {
			continue
		}
		switch v {
		case "json", "template-v1", "bson":
		default:
			return nil, fmt.Errorf("unsupported format %q", v)
		}
		if _, ok := seen[v]; !ok {
			out = append(out, v)
			seen[v] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("at least one format is required")
	}
	return out, nil
}

func parseIndexes(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := map[int]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("parse index count %q: %w", part, err)
		}
		if v < 0 || v > 3 {
			return nil, fmt.Errorf("index count %d is unsupported; use 0, 1, 2, or 3", v)
		}
		if _, ok := seen[v]; !ok {
			out = append(out, v)
			seen[v] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("at least one index count is required")
	}
	sort.Ints(out)
	return out, nil
}

func run(cfg config) error {
	if err := prepareRunDir(cfg.OutDir, cfg.AllowExistingRunDir); err != nil {
		return err
	}
	logDir := filepath.Join(cfg.OutDir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return err
	}
	branch := gitOutput(cfg.RepoRoot, "rev-parse", "--abbrev-ref", "HEAD")
	commit := gitOutput(cfg.RepoRoot, "rev-parse", "HEAD")
	run := &smokeRun{
		SchemaVersion: schemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		RunDir:        cfg.OutDir,
		Worktree:      cfg.RepoRoot,
		Branch:        branch,
		Commit:        commit,
		CommandLine:   commandLine(),
		Config: smokeConfig{
			RawKeys:                cfg.RawKeys,
			CollectionDocs:         cfg.CollectionDocs,
			BatchSize:              cfg.BatchSize,
			ValueSize:              cfg.ValueSize,
			ReadWorkers:            cfg.ReadWorkers,
			Formats:                append([]string(nil), cfg.formats...),
			Indexes:                append([]int(nil), cfg.indexes...),
			Profile:                cfg.Profile,
			LeafSegmentTargetBytes: cfg.LeafSegmentTargetBytes,
			Budgets:                cfg.budgets(),
			IncludeMongo:           cfg.IncludeMongo,
		},
		Artifacts: map[string]string{},
	}
	if !cfg.SkipRaw {
		if err := runRawShape(cfg, run, logDir); err != nil {
			return err
		}
	}
	if !cfg.SkipCollections {
		for _, format := range cfg.formats {
			for _, indexes := range cfg.indexes {
				if err := runCollectionShape(cfg, run, logDir, format, indexes); err != nil {
					return err
				}
			}
		}
	}
	if cfg.IncludeMongo {
		if err := runMongoGatewayShape(cfg, run, logDir); err != nil {
			return err
		}
	} else {
		run.Deferred = append(run.Deferred, deferredWorkload{
			Name:   "mongo_gateway_smoke",
			Issue:  "#1141",
			Reason: "Mongo gateway out-of-core/concurrency coverage is intentionally split from the CI-sized raw/collection smoke harness so client/protocol/gateway costs are not conflated with core TreeDB storage guardrails.",
		})
	}
	run.Checks = validateSmokeRun(run)
	resultsPath := filepath.Join(cfg.OutDir, "out_of_core_smoke_results.json")
	ndjsonPath := filepath.Join(cfg.OutDir, "out_of_core_smoke_results.ndjson")
	mdPath := filepath.Join(cfg.OutDir, "out_of_core_smoke_summary.md")
	run.Artifacts["json"] = resultsPath
	run.Artifacts["ndjson"] = ndjsonPath
	run.Artifacts["markdown"] = mdPath
	if err := writeJSON(resultsPath, run); err != nil {
		return err
	}
	if err := writeNDJSON(ndjsonPath, run.Results); err != nil {
		return err
	}
	if err := os.WriteFile(mdPath, []byte(renderMarkdown(run)), 0o644); err != nil {
		return err
	}
	fmt.Printf("out-of-core smoke run: %s\n", cfg.OutDir)
	fmt.Printf("summary: %s\n", mdPath)
	fmt.Printf("json: %s\n", resultsPath)
	fmt.Printf("ndjson: %s\n", ndjsonPath)
	if cfg.FailOnWarnings && hasWarnings(run.Checks) {
		return errors.New("guardrail warnings present")
	}
	if hasErrors(run.Checks) {
		return errors.New("guardrail errors present")
	}
	return nil
}

func prepareRunDir(dir string, allowExisting bool) error {
	if dir == "" {
		return errors.New("missing run dir")
	}
	if filepath.Clean(dir) == filepath.Clean(string(os.PathSeparator)) {
		return errors.New("refusing to use filesystem root as run dir")
	}
	entries, err := os.ReadDir(dir)
	if err == nil && len(entries) > 0 {
		if _, statErr := os.Stat(filepath.Join(dir, runDirSentinel)); statErr != nil {
			return fmt.Errorf("%s is non-empty and is not an out-of-core smoke run dir", dir)
		}
		if !allowExisting {
			return fmt.Errorf("%s is an existing out-of-core smoke run dir; use -allow-existing-run-dir to append", dir)
		}
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, runDirSentinel), []byte(schemaVersion+"\n"), 0o644)
}

func (cfg config) budgets() budgets {
	return budgets{
		CacheBudgetBytes:       cfg.CacheBudgetBytes,
		RetiredMmapBudgetBytes: cfg.RetiredMmapBudgetBytes,
		MaxDeadMappings:        cfg.MaxDeadMappings,
	}
}

func runRawShape(cfg config, run *smokeRun, logDir string) error {
	dir := filepath.Join(cfg.OutDir, "raw_treedb")
	jsonPath := filepath.Join(cfg.OutDir, "raw_treedb_summary.json")
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	args := []string{
		"-internal-raw-worker",
		"-raw-dir", dir,
		"-raw-json", jsonPath,
		"-raw-keys", strconv.Itoa(cfg.RawKeys),
		"-batch-size", strconv.Itoa(cfg.BatchSize),
		"-value-size", strconv.Itoa(cfg.ValueSize),
		"-read-workers", strconv.Itoa(cfg.ReadWorkers),
		"-profile", cfg.Profile,
		"-leaf-segment-target-bytes", strconv.FormatInt(cfg.LeafSegmentTargetBytes, 10),
	}
	rec, stdout, err := runLoggedCommand("raw_treedb", exe, args, cfg.RepoRoot, cfg.childEnv(), filepath.Join(logDir, "raw_treedb.log"))
	run.Commands = append(run.Commands, rec)
	if err != nil {
		return err
	}
	if strings.TrimSpace(stdout) != "" {
		jsonPath = strings.TrimSpace(stdout)
	}
	var summary rawWorkerSummary
	if err := readJSON(jsonPath, &summary); err != nil {
		return err
	}
	postInsertUsage := rawDiskUsageForPhase(summary, "post_insert")
	postOverwriteUsage := rawDiskUsageForPhase(summary, "post_overwrite")
	postInsertTotal := postInsertUsage.TotalBytes
	postOverwriteTotal := postOverwriteUsage.TotalBytes
	postInsertBytesPerItem := bytesPer(postInsertTotal, summary.Keys)
	postOverwriteBytesPerItem := bytesPer(postOverwriteTotal, summary.Keys)
	postInsertComponents := componentBytesFromDiskUsage(postInsertUsage)
	postOverwriteComponents := componentBytesFromDiskUsage(postOverwriteUsage)
	write := summary.Timings["batch_write"]
	read := summary.Timings["random_read_parallel"]
	overwrite := summary.Timings["overwrite"]
	postOverwriteRead := summary.Timings["post_overwrite_random_read_parallel"]
	run.Results = append(run.Results,
		resultRow{
			ConfigName:      "treedb_raw",
			WorkloadName:    "raw_batch_write",
			Engine:          "treedb",
			Shape:           "raw",
			KeyCount:        summary.Keys,
			ItemCount:       summary.Keys,
			BatchSize:       summary.BatchSize,
			Phase:           "post_insert",
			MeasurementKind: "benchmark_timed",
			BenchmarkTimed:  true,
			OpsPerSec:       ptrFloat(write.OpsPerSec),
			NsPerItem:       nsPerItem(write),
			TotalBytes:      &postInsertTotal,
			BytesPerItem:    postInsertBytesPerItem,
			ComponentBytes:  postInsertComponents,
			Budgets:         cfg.budgets(),
			PressureClaimed: true,
			Mmap:            extractMmapStats(summary.TreeDBStatsFinal),
			Cache:           extractCacheStats(summary.TreeDBStatsFinal),
			StatsAvailable:  sortedKeys(summary.TreeDBStatsFinal),
			SourceArtifact:  jsonPath,
		},
		resultRow{
			ConfigName:      "treedb_raw",
			WorkloadName:    "raw_random_read_parallel",
			Engine:          "treedb",
			Shape:           "raw",
			KeyCount:        summary.Keys,
			ItemCount:       summary.Keys,
			BatchSize:       summary.BatchSize,
			Phase:           "post_insert_read",
			MeasurementKind: "benchmark_timed",
			BenchmarkTimed:  true,
			OpsPerSec:       ptrFloat(read.OpsPerSec),
			NsPerItem:       nsPerItem(read),
			TotalBytes:      &postInsertTotal,
			BytesPerItem:    postInsertBytesPerItem,
			ComponentBytes:  postInsertComponents,
			Budgets:         cfg.budgets(),
			PressureClaimed: true,
			Mmap:            extractMmapStats(summary.TreeDBStatsFinal),
			Cache:           extractCacheStats(summary.TreeDBStatsFinal),
			StatsAvailable:  sortedKeys(summary.TreeDBStatsFinal),
			SourceArtifact:  jsonPath,
		},
		resultRow{
			ConfigName:      "treedb_raw",
			WorkloadName:    "raw_overwrite",
			Engine:          "treedb",
			Shape:           "raw",
			KeyCount:        summary.Keys,
			ItemCount:       summary.Keys,
			BatchSize:       summary.BatchSize,
			Phase:           "post_overwrite",
			MeasurementKind: "benchmark_timed",
			BenchmarkTimed:  true,
			OpsPerSec:       ptrFloat(overwrite.OpsPerSec),
			NsPerItem:       nsPerItem(overwrite),
			TotalBytes:      &postOverwriteTotal,
			BytesPerItem:    postOverwriteBytesPerItem,
			ComponentBytes:  postOverwriteComponents,
			Budgets:         cfg.budgets(),
			PressureClaimed: true,
			Mmap:            extractMmapStats(summary.TreeDBStatsFinal),
			Cache:           extractCacheStats(summary.TreeDBStatsFinal),
			StatsAvailable:  sortedKeys(summary.TreeDBStatsFinal),
			SourceArtifact:  jsonPath,
		},
		resultRow{
			ConfigName:      "treedb_raw",
			WorkloadName:    "raw_post_overwrite_random_read_parallel",
			Engine:          "treedb",
			Shape:           "raw",
			KeyCount:        summary.Keys,
			ItemCount:       summary.Keys,
			BatchSize:       summary.BatchSize,
			Phase:           "post_overwrite_read",
			MeasurementKind: "benchmark_timed",
			BenchmarkTimed:  true,
			OpsPerSec:       ptrFloat(postOverwriteRead.OpsPerSec),
			NsPerItem:       nsPerItem(postOverwriteRead),
			TotalBytes:      &postOverwriteTotal,
			BytesPerItem:    postOverwriteBytesPerItem,
			ComponentBytes:  postOverwriteComponents,
			Budgets:         cfg.budgets(),
			PressureClaimed: true,
			Mmap:            extractMmapStats(summary.TreeDBStatsFinal),
			Cache:           extractCacheStats(summary.TreeDBStatsFinal),
			StatsAvailable:  sortedKeys(summary.TreeDBStatsFinal),
			SourceArtifact:  jsonPath,
		},
	)
	return nil
}

func rawDiskUsageForPhase(summary rawWorkerSummary, phase string) diskUsage {
	if summary.DiskUsageByPhase != nil {
		if usage, ok := summary.DiskUsageByPhase[phase]; ok {
			return usage
		}
	}
	return summary.DiskUsageFinal
}

func componentBytesFromDiskUsage(usage diskUsage) map[string]uint64 {
	if len(usage.TopFiles) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(usage.TopFiles))
	for _, file := range usage.TopFiles {
		out[file.Path] = file.Bytes
	}
	return out
}

func runCollectionShape(cfg config, run *smokeRun, logDir, format string, indexes int) error {
	name := fmt.Sprintf("collection_%s_%d_indexes", sanitizeName(format), indexes)
	dir := filepath.Join(cfg.OutDir, name)
	args := []string{
		"run", "./cmd/collection_load_fixture",
		"-dir", dir,
		"-reset",
		"-docs", strconv.Itoa(cfg.CollectionDocs),
		"-batch-size", strconv.Itoa(cfg.BatchSize),
		"-format", format,
		"-indexes", strconv.Itoa(indexes),
		"-profile", cfg.Profile,
		"-data-outer-leaves-in-vlog",
		"-index-outer-leaves-in-vlog",
		"-leaf-segment-target-bytes", strconv.FormatInt(cfg.LeafSegmentTargetBytes, 10),
		"-checkpoint",
		"-reopen-verify",
		"-verify-samples", strconv.Itoa(min(50, cfg.CollectionDocs)),
		"-progress=false",
		"-json",
	}
	rec, stdout, err := runLoggedCommand(name, "go", args, cfg.RepoRoot, cfg.childEnv(), filepath.Join(logDir, name+".log"))
	run.Commands = append(run.Commands, rec)
	if err != nil {
		return err
	}
	artifact := filepath.Join(cfg.OutDir, name+"_summary.json")
	if err := os.WriteFile(artifact, []byte(stdout), 0o644); err != nil {
		return err
	}
	var summary fixtureSummary
	if err := json.Unmarshal([]byte(stdout), &summary); err != nil {
		return fmt.Errorf("parse %s JSON: %w", name, err)
	}
	components, _ := collectComponentBytes(summary.Dir)
	total := summary.DiskUsageFinal.TotalBytes
	bytesPerItem := bytesPer(total, summary.Docs)
	row := resultRow{
		ConfigName:       fmt.Sprintf("treedb_%s_collection_%d_indexes", sanitizeName(summary.DocumentFormat), summary.IndexCount),
		WorkloadName:     "collection_insert",
		Engine:           "treedb",
		Shape:            "collection",
		Format:           summary.DocumentFormat,
		IndexCount:       summary.IndexCount,
		DocumentCount:    summary.Docs,
		ItemCount:        summary.Docs,
		BatchSize:        summary.BatchSize,
		Phase:            "post_insert",
		MeasurementKind:  "benchmark_timed",
		BenchmarkTimed:   true,
		DocsPerSec:       ptrFloat(summary.InsertTiming.OpsPerSec),
		OpsPerSec:        ptrFloat(summary.InsertTiming.OpsPerSec),
		NsPerItem:        nsPerItem(summary.InsertTiming),
		TotalBytes:       &total,
		BytesPerItem:     bytesPerItem,
		ComponentBytes:   components,
		Budgets:          cfg.budgets(),
		PressureClaimed:  true,
		Mmap:             extractMmapStats(summary.TreeDBStatsFinal),
		Cache:            extractCacheStats(summary.TreeDBStatsFinal),
		StatsAvailable:   sortedKeys(summary.TreeDBStatsFinal),
		SourceArtifact:   artifact,
		MeasurementNotes: "collection-load-fixture insert plus checkpoint and reopen/index smoke verification",
	}
	run.Results = append(run.Results, row)
	return nil
}

func runMongoGatewayShape(cfg config, run *smokeRun, logDir string) error {
	format := preferredMongoFormat(cfg.formats)
	indexes := preferredMongoIndexCount(cfg.indexes)
	name := fmt.Sprintf("mongo_gateway_%s_%d_indexes", sanitizeName(format), indexes)
	baseDir := cfg.OutDir
	if resolved, err := filepath.EvalSymlinks(cfg.OutDir); err == nil {
		baseDir = resolved
	}
	dir := filepath.Join(baseDir, name)
	docs := cfg.CollectionDocs
	reads := min(100, docs)
	rangeReads := min(20, docs)
	updates := min(50, docs)
	args := []string{
		"run", "./cmd/mongo_gateway_bench",
		"-target", "treedb",
		"-treedb-dir", dir,
		"-keep-treedb-dir",
		"-documents", strconv.Itoa(docs),
		"-batch-size", strconv.Itoa(cfg.BatchSize),
		"-reads", strconv.Itoa(reads),
		"-range-reads", strconv.Itoa(rangeReads),
		"-updates", strconv.Itoa(updates),
		"-secondary-indexes", strconv.Itoa(indexes),
		"-treedb-profile", cfg.Profile,
		"-treedb-document-format", format,
		"-treedb-maintenance", "checkpoint",
		"-format", "json",
	}
	rec, stdout, err := runLoggedCommand(name, "go", args, cfg.RepoRoot, cfg.childEnv(), filepath.Join(logDir, name+".log"))
	run.Commands = append(run.Commands, rec)
	if err != nil {
		return err
	}
	artifact := filepath.Join(cfg.OutDir, name+"_summary.json")
	if err := os.WriteFile(artifact, []byte(stdout), 0o644); err != nil {
		return err
	}
	var summary mongoGatewaySummary
	if err := json.Unmarshal([]byte(stdout), &summary); err != nil {
		return fmt.Errorf("parse %s JSON: %w", name, err)
	}
	row := mongoGatewayResultRow(cfg, summary, artifact)
	run.Results = append(run.Results, row)
	return nil
}

func mongoGatewayResultRow(cfg config, summary mongoGatewaySummary, artifact string) resultRow {
	load := mongoGatewayPhaseByName(summary.Phases, "load_insert_many")
	disk := firstMongoDisk(summary.TreeDBDiskAfterCheckpoint, summary.TreeDBDiskAfterLoad, summary.TreeDBDiskAfterMaintenance)
	var totalPtr *uint64
	var components map[string]uint64
	if disk != nil && disk.TotalBytes >= 0 {
		total := uint64(disk.TotalBytes)
		totalPtr = &total
		components = mongoDiskComponentBytes(disk)
	} else if summary.TreeDBDir != "" {
		components, _ = collectComponentBytes(summary.TreeDBDir)
		if components != nil {
			var total uint64
			for _, bytes := range components {
				total += bytes
			}
			totalPtr = &total
		}
	}
	format := summary.TreeDBDocumentFormat
	if format == "" {
		format = preferredMongoFormat(cfg.formats)
	}
	indexes := summary.SecondaryIndexes
	return resultRow{
		ConfigName:       fmt.Sprintf("treedb_mongo_gateway_%s_%d_indexes", sanitizeName(format), indexes),
		WorkloadName:     "mongo_gateway_insert",
		Engine:           "treedb",
		Shape:            "mongo",
		Format:           format,
		IndexCount:       indexes,
		DocumentCount:    summary.Documents,
		ItemCount:        summary.Documents,
		BatchSize:        summary.BatchSize,
		Phase:            "post_insert",
		MeasurementKind:  "benchmark_timed",
		BenchmarkTimed:   true,
		DocsPerSec:       ptrFloat(load.OpsPerSecond),
		OpsPerSec:        ptrFloat(load.OpsPerSecond),
		NsPerItem:        ptrFloat(load.SampledNsPerOp),
		TotalBytes:       totalPtr,
		BytesPerItem:     bytesPerFromPtr(totalPtr, summary.Documents),
		ComponentBytes:   components,
		Budgets:          cfg.budgets(),
		PressureClaimed:  true,
		Mmap:             extractMmapStats(summary.TreeDBStatsFinal),
		Cache:            extractCacheStats(summary.TreeDBStatsFinal),
		StatsAvailable:   sortedKeys(summary.TreeDBStatsFinal),
		SourceArtifact:   artifact,
		MeasurementNotes: "mongo_gateway_bench in-process TreeDB gateway smoke; maintenance=checkpoint, not full compaction",
	}
}

func mongoGatewayPhaseByName(phases []mongoGatewayPhase, name string) mongoGatewayPhase {
	for _, phase := range phases {
		if phase.Name == name {
			return phase
		}
	}
	return mongoGatewayPhase{Name: name}
}

func firstMongoDisk(disks ...*mongoGatewayDiskUsage) *mongoGatewayDiskUsage {
	for _, disk := range disks {
		if disk != nil {
			return disk
		}
	}
	return nil
}

func mongoDiskComponentBytes(disk *mongoGatewayDiskUsage) map[string]uint64 {
	if disk == nil || len(disk.Paths) == 0 {
		return nil
	}
	out := make(map[string]uint64)
	for path, bytes := range disk.Paths {
		if bytes >= 0 {
			out[path] = uint64(bytes)
		}
	}
	return out
}

func preferredMongoFormat(formats []string) string {
	for _, format := range formats {
		if format == "bson" {
			return format
		}
	}
	for _, format := range formats {
		if format == "template-v1" {
			return format
		}
	}
	if len(formats) > 0 {
		return formats[0]
	}
	return "bson"
}

func preferredMongoIndexCount(indexes []int) int {
	out := 0
	for _, indexCount := range indexes {
		if indexCount > out {
			out = indexCount
		}
	}
	if out > 2 {
		return 2
	}
	return out
}

func (cfg config) childEnv() map[string]string {
	return map[string]string{
		"TREEDB_VLOG_MAX_DEAD_MAPPINGS":     strconv.Itoa(cfg.MaxDeadMappings),
		"TREEDB_OUTER_LEAF_READ_SAMPLE_MOD": "1",
	}
}

func runLoggedCommand(name, bin string, args []string, workDir string, env map[string]string, logPath string) (commandRecord, string, error) {
	start := time.Now()
	cmd := exec.Command(bin, args...)
	cmd.Dir = workDir
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		exitCode = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
	}
	log := bytes.Buffer{}
	log.WriteString("$ ")
	log.WriteString(bin)
	for _, arg := range args {
		log.WriteByte(' ')
		log.WriteString(shellQuote(arg))
	}
	log.WriteString("\n\n# stdout\n")
	log.Write(stdout.Bytes())
	log.WriteString("\n# stderr\n")
	log.Write(stderr.Bytes())
	if writeErr := os.WriteFile(logPath, log.Bytes(), 0o644); writeErr != nil {
		writeErr = fmt.Errorf("write command log %s: %w", logPath, writeErr)
		if err != nil {
			err = errors.Join(err, writeErr)
		} else {
			err = writeErr
		}
	}
	rec := commandRecord{
		Name:     name,
		Command:  append([]string{bin}, args...),
		Env:      env,
		WorkDir:  workDir,
		LogPath:  logPath,
		ExitCode: exitCode,
		Duration: time.Since(start).String(),
	}
	if err != nil {
		return rec, stdout.String(), fmt.Errorf("%s failed (exit=%d, log=%s): %w", name, exitCode, logPath, err)
	}
	return rec, stdout.String(), nil
}

func runRawWorker(cfg config) error {
	if cfg.rawDir == "" {
		return errors.New("-raw-dir is required")
	}
	if cfg.rawJSONPath == "" {
		return errors.New("-raw-json is required")
	}
	if err := os.RemoveAll(cfg.rawDir); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.rawDir, 0o755); err != nil {
		return err
	}
	profile, err := parseProfile(cfg.Profile)
	if err != nil {
		return err
	}
	opts := treedb.OptionsForBenchmark(profile, cfg.rawDir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.LeafSegmentTargetBytes = cfg.LeafSegmentTargetBytes
	db, err := treedb.Open(opts)
	if err != nil {
		return err
	}
	timings := make(map[string]timing)
	start := time.Now()
	if err := rawBatchSet(db, 0, cfg.RawKeys, cfg.BatchSize, cfg.ValueSize, 0); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return err
	}
	timings["batch_write"] = elapsedTiming(time.Since(start), cfg.RawKeys)
	usageAfterInsert, err := directoryUsage(cfg.rawDir, cfg.RawKeys)
	if err != nil {
		_ = db.Close()
		return err
	}

	start = time.Now()
	if err := rawParallelRead(db, cfg.RawKeys, cfg.ValueSize, 0, cfg.ReadWorkers); err != nil {
		_ = db.Close()
		return err
	}
	timings["random_read_parallel"] = elapsedTiming(time.Since(start), cfg.RawKeys)

	start = time.Now()
	if err := rawBatchSet(db, 0, cfg.RawKeys, cfg.BatchSize, cfg.ValueSize, 1); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return err
	}
	timings["overwrite"] = elapsedTiming(time.Since(start), cfg.RawKeys)
	usageAfterOverwrite, err := directoryUsage(cfg.rawDir, cfg.RawKeys)
	if err != nil {
		_ = db.Close()
		return err
	}

	start = time.Now()
	if err := rawParallelRead(db, cfg.RawKeys, cfg.ValueSize, 1, cfg.ReadWorkers); err != nil {
		_ = db.Close()
		return err
	}
	timings["post_overwrite_random_read_parallel"] = elapsedTiming(time.Since(start), cfg.RawKeys)

	stats := treedbstats.Selected(db.Stats())
	if err := db.Close(); err != nil {
		return err
	}
	usageFinal, err := directoryUsage(cfg.rawDir, cfg.RawKeys)
	if err != nil {
		return err
	}
	summary := rawWorkerSummary{
		GeneratedAt:       time.Now().UTC().Format(time.RFC3339),
		Dir:               cfg.rawDir,
		Keys:              cfg.RawKeys,
		BatchSize:         cfg.BatchSize,
		ValueSize:         cfg.ValueSize,
		Profile:           cfg.Profile,
		LeafSegmentTarget: cfg.LeafSegmentTargetBytes,
		Timings:           timings,
		DiskUsageFinal:    usageFinal,
		DiskUsageByPhase: map[string]diskUsage{
			"post_insert":    usageAfterInsert,
			"post_overwrite": usageAfterOverwrite,
		},
		TreeDBStatsFinal: stats,
	}
	if err := writeJSON(cfg.rawJSONPath, summary); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, cfg.rawJSONPath)
	return nil
}

func parseProfile(raw string) (treedb.Profile, error) {
	if profile, ok := treedb.ParseBenchmarkProfile(raw, treedb.ProfileBenchUnsafe); ok {
		return profile, nil
	}
	return "", fmt.Errorf("unsupported profile %q; allowed: %s", raw, treedb.BenchmarkProfileFlagHelp)
}

func rawBatchSet(db *treedb.DB, start, count, batchSize, valueSize, generation int) error {
	for pos := start; pos < start+count; {
		n := min(batchSize, start+count-pos)
		batch := db.NewBatchWithSize(n)
		if batch == nil {
			return errors.New("NewBatchWithSize returned nil")
		}
		for i := 0; i < n; i++ {
			keyID := pos + i
			if err := batch.Set(rawKey(keyID), rawValue(keyID, generation, valueSize)); err != nil {
				_ = batch.Close()
				return err
			}
		}
		if err := batch.Write(); err != nil {
			_ = batch.Close()
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
		pos += n
	}
	return nil
}

func rawParallelRead(db *treedb.DB, keys, valueSize, generation, workers int) error {
	if workers <= 0 {
		workers = 1
	}
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			var dst []byte
			for i := worker; i < keys; i += workers {
				got, err := db.GetAppend(rawKey(i), dst[:0])
				if err != nil {
					errCh <- err
					return
				}
				want := rawValue(i, generation, valueSize)
				if !bytes.Equal(got, want) {
					errCh <- fmt.Errorf("key %d mismatch: got %d bytes", i, len(got))
					return
				}
				dst = got[:0]
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func rawKey(i int) []byte {
	return []byte(fmt.Sprintf("raw:%012d", i))
}

func rawValue(i, generation, size int) []byte {
	out := make([]byte, size)
	prefix := fmt.Sprintf("value:%012d:generation:%02d:", i, generation)
	copy(out, prefix)
	for j := len(prefix); j < len(out); j++ {
		out[j] = byte('a' + (i+j+generation)%26)
	}
	return out
}

func elapsedTiming(d time.Duration, n int) timing {
	out := timing{Seconds: d.Seconds()}
	if n > 0 && d > 0 {
		out.SecPerOp = d.Seconds() / float64(n)
		out.OpsPerSec = float64(n) / d.Seconds()
	}
	return out
}

func directoryUsage(root string, docs int) (diskUsage, error) {
	var out diskUsage
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size := uint64(info.Size())
		out.TotalBytes += size
		out.FileCount++
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out.TopFiles = append(out.TopFiles, fileSummary{Path: filepath.ToSlash(rel), Bytes: size})
		return nil
	})
	if err != nil {
		return out, err
	}
	sort.Slice(out.TopFiles, func(i, j int) bool {
		if out.TopFiles[i].Bytes == out.TopFiles[j].Bytes {
			return out.TopFiles[i].Path < out.TopFiles[j].Path
		}
		return out.TopFiles[i].Bytes > out.TopFiles[j].Bytes
	})
	if len(out.TopFiles) > 20 {
		out.TopFiles = out.TopFiles[:20]
	}
	if docs > 0 {
		out.BytesPerDoc = float64(out.TotalBytes) / float64(docs)
	}
	return out, nil
}

func collectComponentBytes(root string) (map[string]uint64, error) {
	out := map[string]uint64{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(filepath.Clean(rel))
		size := uint64(info.Size())
		out["total"] += size
		parts := strings.Split(rel, "/")
		if len(parts) == 0 {
			return nil
		}
		out[parts[0]] += size
		if strings.HasSuffix(rel, "index.db") {
			out["index.db"] += size
			if len(parts) >= 2 {
				out[parts[0]+"/index.db"] += size
			}
		}
		if len(parts) >= 2 {
			switch parts[1] {
			case "leaf_vlog", "value_vlog", "wal":
				out[parts[0]+"/"+parts[1]] += size
			}
		}
		return nil
	})
	return out, err
}

func extractMmapStats(stats map[string]string) *mmapStats {
	if len(stats) == 0 {
		return nil
	}
	var out mmapStats
	var saw bool
	set := func(dst *uint64, keys ...string) {
		if v, ok := statUint(stats, keys...); ok {
			*dst = v
			saw = true
		}
	}
	set(&out.Remaps, "treedb.cache.vlog_mmap.remaps", "treedb.vlog.mmap_remaps")
	set(&out.DeadMappings, "treedb.cache.vlog_mmap.dead_mappings", "treedb.vlog.mmap_dead_mappings")
	set(&out.DeadMappingCap, "treedb.cache.vlog_mmap.dead_mappings.cap_base", "treedb.vlog.mmap_dead_mappings.cap_base")
	set(&out.ActiveSegments, "treedb.cache.vlog_mmap.active_segments", "treedb.vlog.mmap_active_segments")
	set(&out.ActiveBytes, "treedb.cache.vlog_mmap.active_bytes", "treedb.vlog.mmap_active_bytes")
	set(&out.CurrentSegments, "treedb.cache.vlog_mmap.current_segments", "treedb.vlog.mmap_current_segments")
	set(&out.CurrentBytes, "treedb.cache.vlog_mmap.current_bytes", "treedb.vlog.mmap_current_bytes")
	set(&out.SealedSegments, "treedb.cache.vlog_mmap.sealed_segments", "treedb.vlog.mmap_sealed_segments")
	set(&out.SealedBytes, "treedb.cache.vlog_mmap.sealed_bytes", "treedb.vlog.mmap_sealed_bytes")
	set(&out.DeadBytes, "treedb.cache.vlog_mmap.dead_bytes", "treedb.vlog.mmap_dead_bytes")
	set(&out.Hits, "treedb.cache.vlog_mmap.read.hits", "treedb.vlog.mmap_read.hits")
	set(&out.MissOutOfRange, "treedb.cache.vlog_mmap.read.miss_out_of_range", "treedb.vlog.mmap_read.miss_out_of_range")
	set(&out.MissNoMapping, "treedb.cache.vlog_mmap.read.miss_no_mapping", "treedb.vlog.mmap_read.miss_no_mapping")
	set(&out.MissDeadMappingCap, "treedb.cache.vlog_mmap.read.miss_dead_mapping_cap", "treedb.vlog.mmap_read.miss_dead_mapping_cap")
	set(&out.FallbackReadAt, "treedb.cache.vlog_mmap.read.fallback_readat", "treedb.vlog.mmap_read.fallback_readat")
	if v, ok := statFloat(stats, "treedb.cache.vlog_mmap.read.hit_ratio", "treedb.vlog.mmap_read.hit_ratio"); ok {
		out.HitRatio = &v
		saw = true
	}
	if !saw {
		return nil
	}
	return &out
}

func extractCacheStats(stats map[string]string) *cacheStats {
	if len(stats) == 0 {
		return nil
	}
	var out cacheStats
	var saw bool
	if v, ok := statUint(stats, "treedb.vlog.outer_leaf_block_cache.hits"); ok {
		out.OuterLeafBlockHits = &v
		saw = true
	}
	if v, ok := statUint(stats, "treedb.vlog.outer_leaf_block_cache.misses"); ok {
		out.OuterLeafBlockMisses = &v
		saw = true
	}
	if v, ok := statFloat(stats, "treedb.vlog.outer_leaf_block_cache.hit_ratio"); ok {
		out.OuterLeafBlockHitRatio = &v
		saw = true
	}
	if v, ok := statUint(stats, "treedb.vlog.outer_leaf_block_cache.entries"); ok {
		out.OuterLeafBlockEntries = &v
		saw = true
	}
	if v, ok := statUint(stats, "treedb.vlog.outer_leaf_block_cache.capacity"); ok {
		out.OuterLeafBlockCapacity = &v
		saw = true
	}
	if !saw {
		return nil
	}
	return &out
}

func validateSmokeRun(run *smokeRun) []guardrailCheck {
	var checks []guardrailCheck
	if run == nil {
		return []guardrailCheck{{Severity: "error", Code: "nil_run", Message: "smoke run is nil"}}
	}
	if len(run.Results) == 0 {
		checks = append(checks, guardrailCheck{Severity: "error", Code: "missing_results", Message: "no benchmark results were recorded"})
	}
	for _, row := range run.Results {
		subject := row.ConfigName + "/" + row.WorkloadName
		shape := strings.TrimSpace(row.Shape)
		if shape == "" {
			checks = append(checks, guardrailCheck{Severity: "error", Code: "missing_shape_label", Subject: subject, Message: "result row is missing raw/collection/Mongo shape label"})
		} else if shape != "raw" && shape != "collection" && shape != "mongo" {
			checks = append(checks, guardrailCheck{Severity: "error", Code: "invalid_shape_label", Subject: subject, Message: fmt.Sprintf("result row has unsupported shape label %q", row.Shape)})
		}
		if row.ItemCount <= 0 {
			checks = append(checks, guardrailCheck{Severity: "error", Code: "missing_item_count", Subject: subject, Message: "result row is missing document/key count, making bytes-per-item ambiguous"})
		}
		if row.BytesPerItem != nil && row.ItemCount <= 0 {
			checks = append(checks, guardrailCheck{Severity: "error", Code: "bytes_per_item_without_count", Subject: subject, Message: "bytes-per-item was reported without a positive document/key count"})
		}
		if row.PressureClaimed {
			if row.TotalBytes == nil {
				checks = append(checks, guardrailCheck{Severity: "error", Code: "pressure_without_total_bytes", Subject: subject, Message: "pressure/out-of-core guardrail row is missing total bytes"})
			} else {
				budget := maxInt64(row.Budgets.CacheBudgetBytes, row.Budgets.RetiredMmapBudgetBytes)
				if budget <= 0 {
					checks = append(checks, guardrailCheck{Severity: "error", Code: "missing_budget", Subject: subject, Message: "pressure/out-of-core guardrail row is missing configured budget bytes"})
				} else if datasetDoesNotExceedDoubleBudget(*row.TotalBytes, budget) {
					checks = append(checks, guardrailCheck{Severity: "warning", Code: "dataset_not_larger_than_budget", Subject: subject, Message: fmt.Sprintf("dataset bytes %d do not exceed the largest configured budget %d by more than 2x", *row.TotalBytes, budget)})
				}
			}
		}
		if row.Engine == "treedb" && row.Mmap == nil {
			checks = append(checks, guardrailCheck{Severity: "warning", Code: "missing_mmap_counters", Subject: subject, Message: "TreeDB row has no mmap counters; #1135/#1136 validation needs these counters"})
		}
		if row.Mmap != nil && row.Budgets.RetiredMmapBudgetBytes > 0 && row.Mmap.DeadBytes > uint64(row.Budgets.RetiredMmapBudgetBytes) {
			checks = append(checks, guardrailCheck{Severity: "warning", Code: "retired_mmap_budget_exceeded", Subject: subject, Message: fmt.Sprintf("dead mmap bytes %d exceed configured smoke budget %d", row.Mmap.DeadBytes, row.Budgets.RetiredMmapBudgetBytes)})
		}
		if row.Budgets.MaxDeadMappings <= 0 {
			checks = append(checks, guardrailCheck{Severity: "error", Code: "missing_dead_mapping_budget", Subject: subject, Message: "row is missing max dead mapping budget"})
		}
	}
	if mixesRawAndCollectionWithoutLabels(run.Results) {
		checks = append(checks, guardrailCheck{Severity: "error", Code: "mixed_shapes_without_labels", Message: "raw TreeDB and collection rows are mixed without explicit shape labels"})
	}
	if len(checks) == 0 {
		checks = append(checks, guardrailCheck{Severity: "pass", Code: "ok", Message: "all out-of-core smoke guardrails passed"})
	}
	return checks
}

func datasetDoesNotExceedDoubleBudget(total uint64, budget int64) bool {
	if budget <= 0 {
		return false
	}
	// budget is positive int64, so doubling after conversion cannot overflow
	// uint64: 2*math.MaxInt64 == math.MaxUint64-1.
	return total <= uint64(budget)*2
}

func mixesRawAndCollectionWithoutLabels(rows []resultRow) bool {
	var raw, collection bool
	for _, row := range rows {
		raw = raw || row.Shape == "raw"
		collection = collection || row.Shape == "collection"
		if row.Shape == "" && (raw || collection) {
			return true
		}
	}
	return false
}

func renderMarkdown(run *smokeRun) string {
	var sb strings.Builder
	sb.WriteString("# TreeDB Out-of-Core Smoke Report\n\n")
	sb.WriteString("## Executive Summary\n\n")
	errorsCount, warningsCount := countChecks(run.Checks)
	sb.WriteString(fmt.Sprintf("- Results: %d rows across raw TreeDB and collection smoke shapes.\n", len(run.Results)))
	sb.WriteString(fmt.Sprintf("- Guardrails: %d errors, %d warnings.\n", errorsCount, warningsCount))
	sb.WriteString("- This is a CI-sized budget-pressure smoke, not a true larger-than-RAM benchmark. It intentionally uses tiny configured budgets to exercise bounded behavior before #1135/#1136 changes.\n\n")

	sb.WriteString("## Benchmark Configuration\n\n")
	sb.WriteString("| Field | Value |\n| --- | --- |\n")
	sb.WriteString(fmt.Sprintf("| `schema_version` | `%s` |\n", run.SchemaVersion))
	sb.WriteString(fmt.Sprintf("| `generated_at` | `%s` |\n", run.GeneratedAt))
	sb.WriteString(fmt.Sprintf("| `run_dir` | `%s` |\n", run.RunDir))
	sb.WriteString(fmt.Sprintf("| `branch` | `%s` |\n", run.Branch))
	sb.WriteString(fmt.Sprintf("| `commit` | `%s` |\n", run.Commit))
	sb.WriteString(fmt.Sprintf("| `command_line` | `%s` |\n", strings.Join(run.CommandLine, " ")))
	sb.WriteString(fmt.Sprintf("| `raw_keys` | `%d` |\n", run.Config.RawKeys))
	sb.WriteString(fmt.Sprintf("| `collection_docs` | `%d` |\n", run.Config.CollectionDocs))
	sb.WriteString(fmt.Sprintf("| `batch_size` | `%d` |\n", run.Config.BatchSize))
	sb.WriteString(fmt.Sprintf("| `formats` | `%s` |\n", strings.Join(run.Config.Formats, ",")))
	sb.WriteString(fmt.Sprintf("| `indexes` | `%s` |\n", joinInts(run.Config.Indexes)))
	sb.WriteString(fmt.Sprintf("| `leaf_segment_target_bytes` | `%d` |\n", run.Config.LeafSegmentTargetBytes))
	sb.WriteString(fmt.Sprintf("| `cache_budget_bytes` | `%d` |\n", run.Config.Budgets.CacheBudgetBytes))
	sb.WriteString(fmt.Sprintf("| `retired_mmap_budget_bytes` | `%d` |\n", run.Config.Budgets.RetiredMmapBudgetBytes))
	sb.WriteString(fmt.Sprintf("| `max_dead_mappings` | `%d` |\n", run.Config.Budgets.MaxDeadMappings))
	sb.WriteString("\n")

	sb.WriteString("## Throughput And Storage Results\n\n")
	sb.WriteString("| Config | Shape | Format | Indexes | Items | Phase | ops/sec | ns/item | Total bytes | B/item | mmap hits | ReadAt fallback | dead mmap bytes |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range run.Results {
		sb.WriteString(fmt.Sprintf("| `%s` | `%s` | `%s` | %d | %d | `%s` | %s | %s | %s | %s | %s | %s | %s |\n",
			row.ConfigName,
			row.Shape,
			emptyDash(row.Format),
			row.IndexCount,
			row.ItemCount,
			row.Phase,
			formatFloatPtr(firstFloat(row.OpsPerSec, row.DocsPerSec)),
			formatFloatPtr(row.NsPerItem),
			formatUintPtr(row.TotalBytes),
			formatFloatPtr(row.BytesPerItem),
			formatMmapUint(row.Mmap, func(m *mmapStats) uint64 { return m.Hits }),
			formatMmapUint(row.Mmap, func(m *mmapStats) uint64 { return m.FallbackReadAt }),
			formatMmapUint(row.Mmap, func(m *mmapStats) uint64 { return m.DeadBytes }),
		))
	}
	sb.WriteString("\n")

	sb.WriteString("## Component Bytes\n\n")
	sb.WriteString("| Config | Shape | Phase | Component | Bytes |\n| --- | --- | --- | --- | ---: |\n")
	seenComponents := make(map[string]struct{})
	for _, row := range run.Results {
		seenKey := row.ConfigName + "\x00" + row.Shape + "\x00" + row.Phase + "\x00" + row.SourceArtifact
		if _, ok := seenComponents[seenKey]; ok {
			continue
		}
		seenComponents[seenKey] = struct{}{}
		keys := sortedComponentKeys(row.ComponentBytes)
		for _, key := range keys {
			sb.WriteString(fmt.Sprintf("| `%s` | `%s` | `%s` | `%s` | %d |\n", row.ConfigName, row.Shape, row.Phase, key, row.ComponentBytes[key]))
		}
	}
	sb.WriteString("\n")

	sb.WriteString("## Guardrails\n\n")
	sb.WriteString("| Severity | Code | Subject | Message |\n| --- | --- | --- | --- |\n")
	for _, check := range run.Checks {
		sb.WriteString(fmt.Sprintf("| `%s` | `%s` | `%s` | %s |\n", check.Severity, check.Code, check.Subject, escapePipes(check.Message)))
	}
	sb.WriteString("\n")

	if len(run.Deferred) > 0 {
		sb.WriteString("## Deferred Workloads\n\n")
		for _, deferred := range run.Deferred {
			sb.WriteString(fmt.Sprintf("- `%s`: %s", deferred.Name, deferred.Reason))
			if deferred.Issue != "" {
				sb.WriteString(fmt.Sprintf(" (%s)", deferred.Issue))
			}
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}

	sb.WriteString("## Reproduction Commands\n\n")
	sb.WriteString("Canonical entry point:\n\n")
	sb.WriteString("```bash\nmake bench-out-of-core-smoke\n```\n\n")
	sb.WriteString("Exact command captured for this run:\n\n")
	sb.WriteString("```bash\n")
	sb.WriteString(strings.Join(run.CommandLine, " "))
	sb.WriteString("\n```\n\n")

	sb.WriteString("## Command Log\n\n")
	sb.WriteString("| Name | Exit | Duration | Log |\n| --- | ---: | ---: | --- |\n")
	for _, cmd := range run.Commands {
		sb.WriteString(fmt.Sprintf("| `%s` | %d | `%s` | `%s` |\n", cmd.Name, cmd.ExitCode, cmd.Duration, cmd.LogPath))
	}
	sb.WriteString("\n")
	return sb.String()
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeNDJSON(path string, rows []resultRow) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func statUint(stats map[string]string, keys ...string) (uint64, bool) {
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

func statFloat(stats map[string]string, keys ...string) (float64, bool) {
	for _, key := range keys {
		raw, ok := stats[key]
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err == nil {
			return v, true
		}
	}
	return 0, false
}

func commandLine() []string {
	if raw := strings.TrimSpace(os.Getenv(envCommandLine)); raw != "" {
		return []string{raw}
	}
	return append([]string(nil), os.Args...)
}

func gitOutput(workDir string, args ...string) string {
	cmd := exec.Command("git", args...)
	cmd.Dir = workDir
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func sortedComponentKeys(m map[string]uint64) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func bytesPer(total uint64, count int) *float64 {
	if count <= 0 {
		return nil
	}
	v := float64(total) / float64(count)
	return &v
}

func bytesPerFromPtr(total *uint64, count int) *float64 {
	if total == nil {
		return nil
	}
	return bytesPer(*total, count)
}

func nsPerItem(t timing) *float64 {
	if t.SecPerOp > 0 {
		v := t.SecPerOp * 1e9
		return &v
	}
	if t.OpsPerSec > 0 {
		v := 1e9 / t.OpsPerSec
		return &v
	}
	return nil
}

func ptrFloat(v float64) *float64 {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

func firstFloat(vals ...*float64) *float64 {
	for _, v := range vals {
		if v != nil {
			return v
		}
	}
	return nil
}

func formatFloatPtr(v *float64) string {
	if v == nil {
		return "-"
	}
	if *v >= 1000 {
		return fmt.Sprintf("%.0f", *v)
	}
	return fmt.Sprintf("%.2f", *v)
}

func formatUintPtr(v *uint64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatUint(*v, 10)
}

func formatMmapUint(m *mmapStats, fn func(*mmapStats) uint64) string {
	if m == nil {
		return "-"
	}
	return strconv.FormatUint(fn(m), 10)
}

func emptyDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func joinInts(vals []int) string {
	parts := make([]string, 0, len(vals))
	for _, v := range vals {
		parts = append(parts, strconv.Itoa(v))
	}
	return strings.Join(parts, ",")
}

func escapePipes(s string) string {
	return strings.ReplaceAll(s, "|", "\\|")
}

func sanitizeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, "/", "_")
	return s
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return !(r == '-' || r == '_' || r == '.' || r == '/' || r == '=' || r == ':' || r == ',' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	}) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func countChecks(checks []guardrailCheck) (errorsCount, warningsCount int) {
	for _, check := range checks {
		switch check.Severity {
		case "error":
			errorsCount++
		case "warning":
			warningsCount++
		}
	}
	return errorsCount, warningsCount
}

func hasErrors(checks []guardrailCheck) bool {
	errorsCount, _ := countChecks(checks)
	return errorsCount > 0
}

func hasWarnings(checks []guardrailCheck) bool {
	_, warningsCount := countChecks(checks)
	return warningsCount > 0
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
