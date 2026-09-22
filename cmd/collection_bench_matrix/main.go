package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTreeBenchmarkPattern   = `^(BenchmarkCollectionOverheadIndexStateJSONExtraction|BenchmarkCollectionOverheadIndexStateTemplateV1Extraction|BenchmarkCollectionOverheadPlanIndexedTemplateV1|BenchmarkCollectionOverheadPlanIndexedPrecomputedState|BenchmarkCollectionInsertBatchWithSecondaryIndexes|BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes|BenchmarkCollectionShapeInsertBatch|BenchmarkCollectionShapeInsertBatchCheckpoint|BenchmarkCollectionShapeInsertBatchSingleStringJSON|BenchmarkCollectionShapeInsertBatchCheckpointSingleStringJSON|BenchmarkCollectionShapeReadPrimary|BenchmarkCollectionShapeReadPrimaryParallel|BenchmarkCollectionShapeReadPrimaryInto|BenchmarkCollectionShapeReadPrimaryIntoParallel|BenchmarkCollectionMixedReadWritePrimary|BenchmarkCollectionMixedReadWriteSecondaryUnique|BenchmarkCollectionMixedReadWriteScalingPrimary|BenchmarkCollectionMixedReadWriteScalingSecondaryUnique|BenchmarkSecondaryLookupRangeString|BenchmarkSecondaryLookupRangeStringScanFallback)$`
	defaultSQLiteBenchmarkPattern = `^(BenchmarkSQLiteInsertBatchWithSecondaryIndexes|BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes|BenchmarkSQLiteShapeInsertBatchJSON|BenchmarkSQLiteShapeInsertBatchCheckpointJSON|BenchmarkSQLiteShapeInsertBatchNativeColumns|BenchmarkSQLiteShapeInsertBatchCheckpointNativeColumns|BenchmarkSQLiteShapeReadPrimaryJSON|BenchmarkSQLiteShapeReadPrimaryNativeColumns|BenchmarkSQLiteShapeSecondaryLookupJSON|BenchmarkSQLiteShapeSecondaryLookupNativeColumns|BenchmarkSQLiteShapeSecondaryRangeJSON|BenchmarkSQLiteShapeSecondaryRangeNativeColumns)$`
)

type config struct {
	repoRoot               string
	outDir                 string
	goBinary               string
	goTestTimeout          string
	benchtime              string
	count                  int
	batchSize              int
	engine                 string
	formats                []string
	storageCells           []string
	treeBenchmarkPattern   string
	sqliteBenchmarkPattern string
	pagerChunkSize         int64
	pagerSyncConcurrency   int
	leafSegmentTargetBytes int64
	leafGenPackFrameK      int
	reportVLogRewrite      bool
	reportLeafGenPackGC    bool
	reportPostMaintVacuum  bool
	reportSQLiteVacuum     bool
	profileCells           bool
	profileBenchtime       string
	profileCount           int
	profileBlockRate       int
	profileMutexFraction   int
	availableBenchmarks    bool
	skipSQLite             bool
	dryRun                 bool
}

type storageCell struct {
	name       string
	dataOuter  bool
	indexOuter bool
}

type matrixCell struct {
	Name                   string
	ExecutionPath          string
	Engine                 string
	DocumentFormat         string
	DataOuterLeavesInVLog  string
	IndexOuterLeavesInVLog string
	PagerChunkSize         string
	PagerSyncConcurrency   string
	StoragePolicy          string
	BenchmarkPattern       string
	Tags                   []string
	Env                    []string
	ReportMarkdownPath     string
	ReportJSONPath         string
	RawJSONPath            string
	ProfileDir             string
	ProfileManifestPath    string
}

type collectionProfileManifest struct {
	SchemaVersion    string                      `json:"schema_version"`
	CreatedAt        string                      `json:"created_at"`
	Cell             string                      `json:"cell"`
	ProfileDir       string                      `json:"profile_dir"`
	ExecutionPath    string                      `json:"execution_path"`
	Engine           string                      `json:"engine"`
	DocumentFormat   string                      `json:"document_format"`
	StoragePolicy    string                      `json:"storage_policy"`
	BenchmarkPattern string                      `json:"benchmark_pattern"`
	Benchtime        string                      `json:"benchtime"`
	Count            int                         `json:"count"`
	Command          []string                    `json:"command"`
	Env              []string                    `json:"env,omitempty"`
	DurationMillis   float64                     `json:"duration_ms"`
	RunError         string                      `json:"run_error,omitempty"`
	Artifacts        []collectionProfileArtifact `json:"artifacts"`
}

type collectionProfileArtifact struct {
	Phase         string `json:"phase"`
	CPUProfile    string `json:"cpu_profile,omitempty"`
	AllocsProfile string `json:"allocs_profile,omitempty"`
	BlockProfile  string `json:"block_profile,omitempty"`
	MutexProfile  string `json:"mutex_profile,omitempty"`
	Output        string `json:"output,omitempty"`
	Error         string `json:"error,omitempty"`
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "collection_bench_matrix: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg, reproducibleCommandLine(os.Args)); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_matrix: %v\n", err)
		os.Exit(1)
	}
}

func reproducibleCommandLine(args []string) []string {
	command := []string{"go", "run", "./cmd/collection_bench_matrix"}
	if len(args) > 1 {
		command = append(command, args[1:]...)
	}
	return command
}

func parseFlags(args []string) (config, error) {
	var rawFormats string
	var rawStorageCells string
	cfg := config{
		goBinary:               "go",
		goTestTimeout:          "0",
		benchtime:              "100000x",
		count:                  1,
		batchSize:              16000,
		engine:                 "command_wal_relaxed",
		treeBenchmarkPattern:   defaultTreeBenchmarkPattern,
		sqliteBenchmarkPattern: defaultSQLiteBenchmarkPattern,
		reportVLogRewrite:      true,
		reportLeafGenPackGC:    true,
		reportPostMaintVacuum:  true,
		reportSQLiteVacuum:     true,
		profileCount:           1,
		profileBlockRate:       1,
		profileMutexFraction:   1,
		availableBenchmarks:    true,
	}

	fs := flag.NewFlagSet("collection_bench_matrix", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.repoRoot, "repo-root", "", "Repository root; defaults to git rev-parse --show-toplevel")
	fs.StringVar(&cfg.outDir, "out-dir", "", "Output directory; defaults to /tmp/collection_bench_matrix_<timestamp>")
	fs.StringVar(&cfg.goBinary, "go", cfg.goBinary, "go binary to execute")
	fs.StringVar(&cfg.goTestTimeout, "go-test-timeout", cfg.goTestTimeout, "go test -timeout value for benchmark cells; 0 disables the timeout")
	fs.StringVar(&cfg.benchtime, "benchtime", cfg.benchtime, "go test -benchtime value")
	fs.IntVar(&cfg.count, "count", cfg.count, "go test -count value")
	fs.IntVar(&cfg.batchSize, "batch-size", cfg.batchSize, "TREEDB_COLLECTION_BENCH_BATCH_SIZE")
	fs.StringVar(&cfg.engine, "engine", cfg.engine, "TREEDB_COLLECTION_BENCH_ENGINE for TreeDB cells")
	fs.StringVar(&rawFormats, "formats", "json,template-v1", "Comma-separated TreeDB document formats")
	fs.StringVar(&rawStorageCells, "storage-cells", "mainline,index-vlog", "Comma-separated storage cells: mainline,index-vlog,inline")
	fs.StringVar(&cfg.treeBenchmarkPattern, "tree-bench-pattern", cfg.treeBenchmarkPattern, "go test -bench regex for TreeDB collection cells")
	fs.StringVar(&cfg.sqliteBenchmarkPattern, "sqlite-bench-pattern", cfg.sqliteBenchmarkPattern, "go test -bench regex for SQLite cells")
	fs.Int64Var(&cfg.pagerChunkSize, "pager-chunk-size", 0, "Optional TREEDB_COLLECTION_CHUNK_SIZE override; 0 means profile/default")
	fs.IntVar(&cfg.pagerSyncConcurrency, "pager-sync-concurrency", 0, "Optional TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY override; 0 means profile/default")
	fs.Int64Var(&cfg.leafSegmentTargetBytes, "leaf-segment-target-bytes", 0, "Optional TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES override; 0 means engine default")
	fs.IntVar(&cfg.leafGenPackFrameK, "leafgen-pack-frame-k", 0, "Optional TREEDB_COLLECTION_LEAFGEN_PACK_FRAME_K override; 0 means engine default")
	fs.BoolVar(&cfg.reportVLogRewrite, "report-vlog-rewrite", cfg.reportVLogRewrite, "Run TreeDB online value_vlog rewrite/GC measurement after insert-shape benchmarks")
	fs.BoolVar(&cfg.reportLeafGenPackGC, "report-leafgen-pack-gc", cfg.reportLeafGenPackGC, "Run TreeDB leaf_vlog generation pack/GC measurement after insert-shape benchmarks")
	fs.BoolVar(&cfg.reportPostMaintVacuum, "report-post-maintenance-index-vacuum", cfg.reportPostMaintVacuum, "Run TreeDB index vacuum after post-rewrite/pack size measurements and report compacted bytes")
	fs.BoolVar(&cfg.reportSQLiteVacuum, "report-sqlite-vacuum", cfg.reportSQLiteVacuum, "Run SQLite VACUUM measurement after insert-shape benchmarks")
	fs.BoolVar(&cfg.profileCells, "profile-cells", cfg.profileCells, "Run a separate pprof capture pass for each matrix cell after timed benchmarks")
	fs.StringVar(&cfg.profileBenchtime, "profile-benchtime", cfg.profileBenchtime, "go test -benchtime for profile pass; defaults to -benchtime")
	fs.IntVar(&cfg.profileCount, "profile-count", cfg.profileCount, "go test -count for profile pass")
	fs.IntVar(&cfg.profileBlockRate, "profile-block-rate", cfg.profileBlockRate, "go test -blockprofilerate for profile pass")
	fs.IntVar(&cfg.profileMutexFraction, "profile-mutex-fraction", cfg.profileMutexFraction, "go test -mutexprofilefraction for profile pass")
	fs.BoolVar(&cfg.availableBenchmarks, "available-benchmarks", cfg.availableBenchmarks, "Summarize all benchmark rows present in each report")
	fs.BoolVar(&cfg.skipSQLite, "skip-sqlite", false, "Skip SQLite comparison cell")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "Print planned commands without executing benchmarks")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	var err error
	cfg.formats, err = splitList(rawFormats, "formats")
	if err != nil {
		return config{}, err
	}
	cfg.storageCells, err = splitList(rawStorageCells, "storage-cells")
	if err != nil {
		return config{}, err
	}
	if cfg.count <= 0 {
		return config{}, fmt.Errorf("-count must be positive")
	}
	if cfg.batchSize <= 0 {
		return config{}, fmt.Errorf("-batch-size must be positive")
	}
	if cfg.pagerChunkSize < 0 {
		return config{}, fmt.Errorf("-pager-chunk-size must be >= 0")
	}
	if cfg.pagerSyncConcurrency < 0 {
		return config{}, fmt.Errorf("-pager-sync-concurrency must be >= 0")
	}
	if cfg.leafSegmentTargetBytes < 0 {
		return config{}, fmt.Errorf("-leaf-segment-target-bytes must be >= 0")
	}
	if cfg.leafGenPackFrameK < 0 {
		return config{}, fmt.Errorf("-leafgen-pack-frame-k must be >= 0")
	}
	if strings.TrimSpace(cfg.benchtime) == "" {
		return config{}, fmt.Errorf("-benchtime is required")
	}
	if strings.TrimSpace(cfg.goBinary) == "" {
		return config{}, fmt.Errorf("-go is required")
	}
	if cfg.profileCount <= 0 {
		return config{}, fmt.Errorf("-profile-count must be positive")
	}
	if cfg.profileBlockRate < 0 {
		return config{}, fmt.Errorf("-profile-block-rate must be >= 0")
	}
	if cfg.profileMutexFraction < 0 {
		return config{}, fmt.Errorf("-profile-mutex-fraction must be >= 0")
	}
	if strings.TrimSpace(cfg.profileBenchtime) == "" {
		cfg.profileBenchtime = cfg.benchtime
	}
	return cfg, nil
}

func splitList(raw, name string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-%s must include at least one value", name)
	}
	return out, nil
}

func run(cfg config, commandLine []string) error {
	repoRoot, err := resolveRepoRoot(cfg)
	if err != nil {
		return err
	}
	cfg.repoRoot = repoRoot
	if strings.TrimSpace(cfg.goTestTimeout) == "" {
		cfg.goTestTimeout = "0"
	}
	if cfg.outDir == "" {
		cfg.outDir = defaultOutputDir(time.Now().UTC())
	}
	absOutDir, err := filepath.Abs(cfg.outDir)
	if err != nil {
		return fmt.Errorf("resolve output directory %q: %w", cfg.outDir, err)
	}
	cfg.outDir = absOutDir
	if !cfg.dryRun {
		if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
	}
	if !cfg.skipSQLite && !cfg.dryRun && !sqliteBenchmarksAvailable(cfg) {
		fmt.Fprintln(os.Stderr, "collection_bench_matrix: skipping sqlite cell; sqlite_bench+cgo benchmarks are unavailable")
		cfg.skipSQLite = true
	}

	branch := gitOutput(cfg.repoRoot, "branch", "--show-current")
	commit := gitOutput(cfg.repoRoot, "rev-parse", "--short=12", "HEAD")
	cells, err := buildMatrixCells(cfg)
	if err != nil {
		return err
	}

	for i := range cells {
		cell := &cells[i]
		cellDir := filepath.Join(cfg.outDir, cell.Name)
		if !cfg.dryRun {
			if err := os.MkdirAll(cellDir, 0o755); err != nil {
				return fmt.Errorf("create cell directory %s: %w", cell.Name, err)
			}
		}
		cell.RawJSONPath = filepath.Join(cellDir, "go_test.json")
		cell.ReportMarkdownPath = filepath.Join(cellDir, "collections_report.md")
		cell.ReportJSONPath = filepath.Join(cellDir, "collections_report.json")
		if cfg.profileCells {
			cell.ProfileDir = filepath.Join(cellDir, "profiles")
			cell.ProfileManifestPath = filepath.Join(cell.ProfileDir, "collection_profile_manifest.json")
		}
		if err := runBenchmarkCell(cfg, *cell); err != nil {
			return err
		}
		if err := runCellReport(cfg, *cell, branch, commit); err != nil {
			return err
		}
	}

	matrixIndexPath := filepath.Join(cfg.outDir, "matrix_index.tsv")
	if !cfg.dryRun {
		if err := writeMatrixIndex(matrixIndexPath, cells); err != nil {
			return err
		}
	}
	if err := runMatrixSummary(cfg, matrixIndexPath); err != nil {
		return err
	}
	if cfg.profileCells {
		for _, err := range runProfileCells(cfg, cells) {
			fmt.Fprintf(os.Stderr, "collection_bench_matrix: profile warning: %v\n", err)
		}
	}
	if err := writeRunREADME(cfg, commandLine, cells, matrixIndexPath, branch, commit); err != nil {
		return err
	}

	fmt.Printf("collection benchmark matrix complete\n")
	fmt.Printf("output directory: %s\n", cfg.outDir)
	fmt.Printf("summary markdown: %s\n", filepath.Join(cfg.outDir, "collections_matrix_summary.md"))
	fmt.Printf("summary html:     %s\n", filepath.Join(cfg.outDir, "collections_matrix_summary.html"))
	return nil
}

func runProfileCells(cfg config, cells []matrixCell) []error {
	var errs []error
	for _, cell := range cells {
		if err := runProfileCell(cfg, cell); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func defaultOutputDir(now time.Time) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("collection_bench_matrix_%s_%d", now.Format("20060102_150405"), now.UnixNano()))
}

func resolveRepoRoot(cfg config) (string, error) {
	if strings.TrimSpace(cfg.repoRoot) != "" {
		return filepath.Abs(cfg.repoRoot)
	}
	cmd := exec.Command(cfg.goBinary, "env", "GOMOD")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err == nil {
		gomod := strings.TrimSpace(string(out))
		if gomod != "" && gomod != os.DevNull {
			return filepath.Dir(gomod), nil
		}
	}
	git := exec.Command("git", "rev-parse", "--show-toplevel")
	var gitErr bytes.Buffer
	git.Stderr = &gitErr
	out, err = git.Output()
	if err != nil {
		return "", fmt.Errorf("resolve repo root with go env GOMOD (%s) and git rev-parse (%s): %w", strings.TrimSpace(stderr.String()), strings.TrimSpace(gitErr.String()), err)
	}
	return strings.TrimSpace(string(out)), nil
}

func buildMatrixCells(cfg config) ([]matrixCell, error) {
	storage, err := resolveStorageCells(cfg.storageCells)
	if err != nil {
		return nil, err
	}
	var cells []matrixCell
	seenCellNames := make(map[string]struct{})
	for _, format := range cfg.formats {
		format = strings.TrimSpace(format)
		if format == "" {
			continue
		}
		for _, storageCell := range storage {
			cellName := "treedb_" + sanitizeCellPart(cfg.engine) + "_" + sanitizeCellPart(format) + "_" + storageCell.name
			if _, ok := seenCellNames[cellName]; ok {
				return nil, fmt.Errorf("duplicate matrix cell %q from format %q and storage cell %q", cellName, format, storageCell.name)
			}
			seenCellNames[cellName] = struct{}{}
			cell := matrixCell{
				Name:                   cellName,
				ExecutionPath:          "native-fastpath",
				Engine:                 cfg.engine,
				DocumentFormat:         format,
				DataOuterLeavesInVLog:  strconv.FormatBool(storageCell.dataOuter),
				IndexOuterLeavesInVLog: strconv.FormatBool(storageCell.indexOuter),
				PagerChunkSize:         pagerChunkLabel(cfg.pagerChunkSize),
				PagerSyncConcurrency:   pagerSyncLabel(cfg.pagerSyncConcurrency),
				StoragePolicy:          fmt.Sprintf("data_outer=%t,index_outer=%t", storageCell.dataOuter, storageCell.indexOuter),
				BenchmarkPattern:       cfg.treeBenchmarkPattern,
				Env: []string{
					"TREEDB_COLLECTION_BENCH_ENGINE=" + cfg.engine,
					"TREEDB_COLLECTION_DOCUMENT_FORMAT=" + format,
					"TREEDB_COLLECTION_BENCH_BATCH_SIZE=" + strconv.Itoa(cfg.batchSize),
					"TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=" + strconv.FormatBool(storageCell.dataOuter),
					"TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=" + strconv.FormatBool(storageCell.indexOuter),
					"TREEDB_COLLECTION_REPORT_VLOG_REWRITE=" + strconv.FormatBool(cfg.reportVLogRewrite),
					"TREEDB_COLLECTION_REPORT_LEAFGEN_PACK_GC=" + strconv.FormatBool(cfg.reportLeafGenPackGC),
					"TREEDB_COLLECTION_REPORT_POST_MAINTENANCE_INDEX_VACUUM=" + strconv.FormatBool(cfg.reportPostMaintVacuum),
					"TREEDB_COLLECTION_LEAFGEN_PACK_FRAME_K=" + strconv.Itoa(cfg.leafGenPackFrameK),
				},
			}
			cell.Env = appendPagerEnv(cell.Env, cfg)
			cells = append(cells, cell)
		}
	}
	if !cfg.skipSQLite {
		cell := matrixCell{
			Name:                   "sqlite_wal_normal",
			ExecutionPath:          "sqlite",
			Engine:                 "sqlite_wal_normal",
			DocumentFormat:         "json",
			DataOuterLeavesInVLog:  "-",
			IndexOuterLeavesInVLog: "-",
			PagerChunkSize:         "-",
			PagerSyncConcurrency:   "-",
			StoragePolicy:          "sqlite_wal_normal",
			BenchmarkPattern:       cfg.sqliteBenchmarkPattern,
			Tags:                   []string{"sqlite_bench"},
			Env: []string{
				"TREEDB_COLLECTION_BENCH_BATCH_SIZE=" + strconv.Itoa(cfg.batchSize),
				"TREEDB_COLLECTION_REPORT_SQLITE_VACUUM=" + strconv.FormatBool(cfg.reportSQLiteVacuum),
			},
		}
		if _, ok := seenCellNames[cell.Name]; ok {
			return nil, fmt.Errorf("duplicate matrix cell %q", cell.Name)
		}
		seenCellNames[cell.Name] = struct{}{}
		cells = append(cells, cell)
	}
	if len(cells) == 0 {
		return nil, fmt.Errorf("matrix has no cells")
	}
	return cells, nil
}

func resolveStorageCells(names []string) ([]storageCell, error) {
	out := make([]storageCell, 0, len(names))
	for _, name := range names {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "mainline", "data-vlog", "data-vlog-index-pager":
			out = append(out, storageCell{name: "data_vlog_index_pager", dataOuter: true, indexOuter: false})
		case "index-vlog", "data-vlog-index-vlog":
			out = append(out, storageCell{name: "data_vlog_index_vlog", dataOuter: true, indexOuter: true})
		case "inline", "index-pager", "pager":
			out = append(out, storageCell{name: "inline_index_pager", dataOuter: false, indexOuter: false})
		default:
			return nil, fmt.Errorf("unknown storage cell %q", name)
		}
	}
	return out, nil
}

func appendPagerEnv(env []string, cfg config) []string {
	if cfg.pagerChunkSize > 0 {
		env = append(env, "TREEDB_COLLECTION_CHUNK_SIZE="+strconv.FormatInt(cfg.pagerChunkSize, 10))
	}
	if cfg.pagerSyncConcurrency > 0 {
		env = append(env, "TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY="+strconv.Itoa(cfg.pagerSyncConcurrency))
	}
	if cfg.leafSegmentTargetBytes > 0 {
		env = append(env, "TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES="+strconv.FormatInt(cfg.leafSegmentTargetBytes, 10))
	}
	return env
}

func pagerChunkLabel(value int64) string {
	if value <= 0 {
		return "profile/default"
	}
	return strconv.FormatInt(value, 10)
}

func pagerSyncLabel(value int) string {
	if value <= 0 {
		return "profile/default"
	}
	return strconv.Itoa(value)
}

func sanitizeCellPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var sb strings.Builder
	previousUnderscore := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			sb.WriteRune(r)
			previousUnderscore = false
			continue
		}
		if !previousUnderscore {
			sb.WriteByte('_')
			previousUnderscore = true
		}
	}
	out := strings.Trim(sb.String(), "_")
	if out == "" {
		return "cell"
	}
	return out
}

func runBenchmarkCell(cfg config, cell matrixCell) error {
	args := goTestArgs(cell, cfg)
	fmt.Printf("running %s: %s %s\n", cell.Name, cfg.goBinary, strings.Join(args, " "))
	if cfg.dryRun {
		return nil
	}
	out, err := os.Create(cell.RawJSONPath)
	if err != nil {
		return fmt.Errorf("create raw benchmark json for %s: %w", cell.Name, err)
	}
	defer out.Close()

	cmd := exec.Command(cfg.goBinary, args...)
	cmd.Dir = cfg.repoRoot
	cmd.Env = envWithOverrides(os.Environ(), cell.Env...)
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run benchmark cell %s: %w", cell.Name, err)
	}
	return nil
}

func goTestArgs(cell matrixCell, cfg config) []string {
	args := []string{"test"}
	if len(cell.Tags) > 0 {
		args = append(args, "-tags", strings.Join(cell.Tags, ","))
	}
	args = append(args,
		"./TreeDB/collections",
		"-run", "^$",
		"-bench", cell.BenchmarkPattern,
		"-benchmem",
		"-timeout", cfg.goTestTimeout,
		"-count", strconv.Itoa(cfg.count),
		"-benchtime", cfg.benchtime,
		"-json",
	)
	return args
}

func runProfileCell(cfg config, cell matrixCell) error {
	if strings.TrimSpace(cell.ProfileDir) == "" {
		return fmt.Errorf("profile dir is empty for %s", cell.Name)
	}
	if cfg.dryRun {
		fmt.Printf("profiling %s: %s %s\n", cell.Name, cfg.goBinary, strings.Join(goTestProfileArgs(cell, cfg), " "))
		return nil
	}
	if err := os.MkdirAll(cell.ProfileDir, 0o755); err != nil {
		return fmt.Errorf("create profile dir for %s: %w", cell.Name, err)
	}
	args := goTestProfileArgs(cell, cfg)
	outputName := "profile_go_test.txt"
	outputPath := filepath.Join(cell.ProfileDir, outputName)
	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create profile output for %s: %w", cell.Name, err)
	}
	defer out.Close()

	fmt.Printf("profiling %s: %s %s\n", cell.Name, cfg.goBinary, strings.Join(args, " "))
	cmd := exec.Command(cfg.goBinary, args...)
	cmd.Dir = cfg.repoRoot
	cmd.Env = envWithOverrides(os.Environ(), profileCellEnv(cell.Env)...)
	cmd.Stdout = out
	cmd.Stderr = io.MultiWriter(os.Stderr, out)
	start := time.Now()
	runErr := cmd.Run()
	duration := time.Since(start)
	errText := ""
	if runErr != nil {
		errText = runErr.Error()
	}
	manifest := collectionProfileManifest{
		SchemaVersion:    "collection-profile-manifest/v1",
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		Cell:             cell.Name,
		ProfileDir:       cell.ProfileDir,
		ExecutionPath:    cell.ExecutionPath,
		Engine:           cell.Engine,
		DocumentFormat:   cell.DocumentFormat,
		StoragePolicy:    cell.StoragePolicy,
		BenchmarkPattern: cell.BenchmarkPattern,
		Benchtime:        cfg.profileBenchtime,
		Count:            cfg.profileCount,
		Command:          append([]string{cfg.goBinary}, args...),
		Env:              profileCellEnv(cell.Env),
		DurationMillis:   float64(duration) / float64(time.Millisecond),
		RunError:         errText,
		Artifacts: []collectionProfileArtifact{{
			Phase:         "benchmark_cell",
			CPUProfile:    "cpu.pprof",
			AllocsProfile: "allocs.pprof",
			BlockProfile:  "block.pprof",
			MutexProfile:  "mutex.pprof",
			Output:        outputName,
			Error:         errText,
		}},
	}
	if err := writeJSONFile(cell.ProfileManifestPath, manifest); err != nil {
		return fmt.Errorf("write profile manifest for %s: %w", cell.Name, err)
	}
	if runErr != nil {
		return fmt.Errorf("profile benchmark cell %s: %w", cell.Name, runErr)
	}
	return nil
}

func profileCellEnv(env []string) []string {
	return envWithOverrides(env,
		"TREEDB_COLLECTION_REPORT_DISK_USAGE=false",
		"TREEDB_COLLECTION_REPORT_VLOG_REWRITE=false",
		"TREEDB_COLLECTION_REPORT_LEAFGEN_PACK_GC=false",
		"TREEDB_COLLECTION_REPORT_POST_MAINTENANCE_INDEX_VACUUM=false",
		"TREEDB_COLLECTION_REPORT_SQLITE_VACUUM=false",
	)
}

func envWithOverrides(env []string, overrides ...string) []string {
	keys := make(map[string]struct{}, len(overrides))
	for _, item := range overrides {
		key, _, ok := strings.Cut(item, "=")
		if ok {
			keys[key] = struct{}{}
		}
	}
	out := make([]string, 0, len(env)+len(overrides))
	for _, item := range env {
		key, _, ok := strings.Cut(item, "=")
		if ok {
			if _, replace := keys[key]; replace {
				continue
			}
		}
		out = append(out, item)
	}
	out = append(out, overrides...)
	return out
}

func goTestProfileArgs(cell matrixCell, cfg config) []string {
	args := []string{"test"}
	if len(cell.Tags) > 0 {
		args = append(args, "-tags", strings.Join(cell.Tags, ","))
	}
	args = append(args,
		"./TreeDB/collections",
		"-run", "^$",
		"-bench", cell.BenchmarkPattern,
		"-benchmem",
		"-timeout", cfg.goTestTimeout,
		"-count", strconv.Itoa(cfg.profileCount),
		"-benchtime", cfg.profileBenchtime,
		"-cpuprofile", filepath.Join(cell.ProfileDir, "cpu.pprof"),
		"-memprofile", filepath.Join(cell.ProfileDir, "allocs.pprof"),
		"-blockprofile", filepath.Join(cell.ProfileDir, "block.pprof"),
		"-mutexprofile", filepath.Join(cell.ProfileDir, "mutex.pprof"),
		"-blockprofilerate", strconv.Itoa(cfg.profileBlockRate),
		"-mutexprofilefraction", strconv.Itoa(cfg.profileMutexFraction),
	)
	return args
}

func sqliteBenchmarksAvailable(cfg config) bool {
	args := []string{
		"test",
		"-tags", "sqlite_bench",
		"./TreeDB/collections",
		"-run", "^$",
		"-list", "^BenchmarkSQLite",
	}
	cmd := exec.Command(cfg.goBinary, args...)
	cmd.Dir = cfg.repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}
	return sqliteBenchmarkListHasSQLite(out)
}

func sqliteBenchmarkListHasSQLite(out []byte) bool {
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "BenchmarkSQLite") {
			return true
		}
	}
	return false
}

func runCellReport(cfg config, cell matrixCell, branch, commit string) error {
	args := []string{
		"run", "./cmd/collection_bench_report",
		"-in", cell.RawJSONPath,
		"-out-dir", filepath.Dir(cell.ReportJSONPath),
		"-branch", branch,
		"-commit", commit,
		"-worktree", cfg.repoRoot,
		"-execution-path", cell.ExecutionPath,
		"-bench-pattern", cell.BenchmarkPattern,
		"-count", strconv.Itoa(cfg.count),
		"-benchmark-engine", cell.Engine,
		"-document-format", cell.DocumentFormat,
		"-storage-policy", cell.StoragePolicy,
		"-pager-chunk-size", cell.PagerChunkSize,
		"-pager-sync-concurrency", cell.PagerSyncConcurrency,
		"-collection-batch-size", strconv.Itoa(cfg.batchSize),
	}
	fmt.Printf("reporting %s: %s %s\n", cell.Name, cfg.goBinary, strings.Join(args, " "))
	if cfg.dryRun {
		return nil
	}
	cmd := exec.Command(cfg.goBinary, args...)
	cmd.Dir = cfg.repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("generate report for %s: %w", cell.Name, err)
	}
	return nil
}

func writeMatrixIndex(path string, cells []matrixCell) error {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.Comma = '\t'
	if err := writer.Write([]string{
		"cell",
		"engine",
		"document_format",
		"data_outer_leaves_in_vlog",
		"index_outer_leaves_in_vlog",
		"pager_chunk_size",
		"pager_sync_concurrency",
		"report_md",
		"report_json",
		"profile_manifest",
	}); err != nil {
		return err
	}
	for _, cell := range cells {
		reportMarkdownPath, err := relativeArtifactPath(filepath.Dir(path), cell.ReportMarkdownPath)
		if err != nil {
			return fmt.Errorf("matrix index %s report_md: %w", cell.Name, err)
		}
		reportJSONPath, err := relativeArtifactPath(filepath.Dir(path), cell.ReportJSONPath)
		if err != nil {
			return fmt.Errorf("matrix index %s report_json: %w", cell.Name, err)
		}
		profileManifestPath := ""
		if cell.ProfileManifestPath != "" {
			rel, err := relativeArtifactPath(filepath.Dir(path), cell.ProfileManifestPath)
			if err != nil {
				return fmt.Errorf("matrix index %s profile_manifest: %w", cell.Name, err)
			}
			profileManifestPath = filepath.ToSlash(rel)
		}
		if err := writer.Write([]string{
			cell.Name,
			cell.Engine,
			cell.DocumentFormat,
			cell.DataOuterLeavesInVLog,
			cell.IndexOuterLeavesInVLog,
			cell.PagerChunkSize,
			cell.PagerSyncConcurrency,
			filepath.ToSlash(reportMarkdownPath),
			filepath.ToSlash(reportJSONPath),
			profileManifestPath,
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("render matrix index: %w", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write matrix index: %w", err)
	}
	return nil
}

func writeJSONFile(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func runMatrixSummary(cfg config, matrixIndexPath string) error {
	args := []string{
		"run", "./cmd/collection_bench_matrix_summary",
		"-matrix-index", matrixIndexPath,
		"-out-dir", cfg.outDir,
	}
	if cfg.availableBenchmarks {
		args = append(args, "-available-benchmarks")
	}
	fmt.Printf("summarizing matrix: %s %s\n", cfg.goBinary, strings.Join(args, " "))
	if cfg.dryRun {
		return nil
	}
	cmd := exec.Command(cfg.goBinary, args...)
	cmd.Dir = cfg.repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("generate matrix summary: %w", err)
	}
	return nil
}

func writeRunREADME(cfg config, commandLine []string, cells []matrixCell, matrixIndexPath, branch, commit string) error {
	if cfg.dryRun {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("# Collections Benchmark Matrix Run\n\n")
	sb.WriteString("## Run Metadata\n\n")
	writeMarkdownCodeLine(&sb, "generated at", time.Now().UTC().Format(time.RFC3339))
	writeMarkdownCodeLine(&sb, "worktree", cfg.repoRoot)
	writeMarkdownCodeLine(&sb, "branch", branch)
	writeMarkdownCodeLine(&sb, "commit", commit)
	writeMarkdownCodeLine(&sb, "bash command", shellQuoteCommand(commandLine))
	if argvJSON, err := json.Marshal(commandLine); err == nil {
		writeMarkdownCodeLine(&sb, "argv json", string(argvJSON))
	}
	writeMarkdownCodeLine(&sb, "benchtime", cfg.benchtime)
	writeMarkdownCodeLine(&sb, "count", strconv.Itoa(cfg.count))
	writeMarkdownCodeLine(&sb, "collection batch size", strconv.Itoa(cfg.batchSize))
	if cfg.profileCells {
		writeMarkdownCodeLine(&sb, "profile cells", "true")
		writeMarkdownCodeLine(&sb, "profile benchtime", cfg.profileBenchtime)
		writeMarkdownCodeLine(&sb, "profile count", strconv.Itoa(cfg.profileCount))
	}
	sb.WriteByte('\n')

	sb.WriteString("## Primary Artifacts\n\n")
	sb.WriteString("- matrix summary markdown: `collections_matrix_summary.md`\n")
	sb.WriteString("- matrix summary html: `collections_matrix_summary.html`\n")
	sb.WriteString("- raw matrix tsv: `collections_matrix_summary.tsv`\n")
	sb.WriteString("- user-story tsv: `collections_user_story_summary.tsv`\n")
	sb.WriteString("- disk usage tsv: `collections_disk_usage_summary.tsv`\n")
	sb.WriteString("- maintenance tsv: `collections_maintenance_summary.tsv` (`treedb_vlog_rewrite` is value_vlog maintenance; `treedb_leafgen_pack_gc` is leaf_vlog generation maintenance)\n")
	sb.WriteString("- matrix index: `")
	sb.WriteString(filepath.Base(matrixIndexPath))
	sb.WriteString("`\n\n")

	sb.WriteString("## Cells\n\n")
	sb.WriteString("| Cell | Engine | Format | Data vlog | Index vlog | Bench pattern | Raw JSON | Report | Profiles |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | --- | --- | --- | --- |\n")
	for _, cell := range cells {
		rawRel, err := relativeArtifactPath(cfg.outDir, cell.RawJSONPath)
		if err != nil {
			return fmt.Errorf("README raw artifact path for %s: %w", cell.Name, err)
		}
		reportRel, err := relativeArtifactPath(cfg.outDir, cell.ReportMarkdownPath)
		if err != nil {
			return fmt.Errorf("README report artifact path for %s: %w", cell.Name, err)
		}
		sb.WriteString("| `")
		sb.WriteString(cell.Name)
		sb.WriteString("` | `")
		sb.WriteString(cell.Engine)
		sb.WriteString("` | `")
		sb.WriteString(cell.DocumentFormat)
		sb.WriteString("` | `")
		sb.WriteString(cell.DataOuterLeavesInVLog)
		sb.WriteString("` | `")
		sb.WriteString(cell.IndexOuterLeavesInVLog)
		sb.WriteString("` | `")
		sb.WriteString(escapeMarkdownTableCell(cell.BenchmarkPattern))
		sb.WriteString("` | `")
		sb.WriteString(filepath.ToSlash(rawRel))
		sb.WriteString("` | [report](")
		sb.WriteString(filepath.ToSlash(reportRel))
		sb.WriteString(") | ")
		if cell.ProfileManifestPath != "" {
			profileRel, err := relativeArtifactPath(cfg.outDir, cell.ProfileManifestPath)
			if err != nil {
				return fmt.Errorf("README profile artifact path for %s: %w", cell.Name, err)
			}
			sb.WriteString("[manifest](")
			sb.WriteString(filepath.ToSlash(profileRel))
			sb.WriteString(")")
		} else {
			sb.WriteString("-")
		}
		sb.WriteString(" |\n")
	}
	readmePath := filepath.Join(cfg.outDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(sb.String()), 0o644); err != nil {
		return fmt.Errorf("write run README: %w", err)
	}
	return nil
}

func writeMarkdownCodeLine(sb *strings.Builder, label, value string) {
	sb.WriteString("- ")
	sb.WriteString(label)
	sb.WriteString(": ")
	sb.WriteString(markdownInlineCode(value))
	sb.WriteByte('\n')
}

func markdownInlineCode(value string) string {
	if !strings.ContainsAny(value, "`\r\n") {
		return "`" + value + "`"
	}
	value = strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(value)
	escaped := html.EscapeString(value)
	escaped = strings.ReplaceAll(escaped, "\n", "&#10;")
	return "<code>" + escaped + "</code>"
}

func relativeArtifactPath(baseDir, artifactPath string) (string, error) {
	if strings.TrimSpace(artifactPath) == "" {
		return "", fmt.Errorf("artifact path is empty")
	}
	rel, err := filepath.Rel(baseDir, artifactPath)
	if err != nil {
		return "", fmt.Errorf("make %q relative to %q: %w", artifactPath, baseDir, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("artifact path %q escapes output directory %q", artifactPath, baseDir)
	}
	return rel, nil
}

func shellQuoteCommand(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuoteArg(arg))
	}
	return strings.Join(quoted, " ")
}

func shellQuoteArg(arg string) string {
	if arg == "" {
		return "''"
	}
	if !strings.ContainsAny(arg, " \t\n'\"\\$`!*?[]{}()<>|&;") {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", "'\"'\"'") + "'"
}

func gitOutput(repoRoot string, args ...string) string {
	cmd := exec.Command("git", args...)
	cmd.Dir = repoRoot
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func escapeMarkdownTableCell(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "|", `\|`)
	return value
}
