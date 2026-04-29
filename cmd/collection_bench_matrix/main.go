package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTreeBenchmarkPattern   = `^(BenchmarkCollectionShapeInsertBatch|BenchmarkCollectionShapeInsertBatchCheckpoint|BenchmarkCollectionShapeInsertBatchSingleStringJSON|BenchmarkCollectionShapeInsertBatchCheckpointSingleStringJSON|BenchmarkCollectionShapeReadPrimary|BenchmarkCollectionShapeReadPrimaryParallel|BenchmarkCollectionMixedReadWritePrimary|BenchmarkCollectionMixedReadWriteSecondaryUnique)$`
	defaultSQLiteBenchmarkPattern = `^(BenchmarkSQLiteShapeInsertBatchJSON|BenchmarkSQLiteShapeInsertBatchCheckpointJSON|BenchmarkSQLiteShapeInsertBatchNativeColumns|BenchmarkSQLiteShapeInsertBatchCheckpointNativeColumns|BenchmarkSQLiteShapeReadPrimaryJSON|BenchmarkSQLiteShapeReadPrimaryNativeColumns|BenchmarkSQLiteShapeSecondaryLookupJSON|BenchmarkSQLiteShapeSecondaryLookupNativeColumns)$`
)

type config struct {
	repoRoot               string
	outDir                 string
	goBinary               string
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
	reportVLogRewrite      bool
	reportLeafGenPackGC    bool
	reportSQLiteVacuum     bool
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
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_matrix: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_matrix: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	var rawFormats string
	var rawStorageCells string
	cfg := config{
		goBinary:               "go",
		benchtime:              "100000x",
		count:                  1,
		batchSize:              16000,
		engine:                 "production_fast",
		treeBenchmarkPattern:   defaultTreeBenchmarkPattern,
		sqliteBenchmarkPattern: defaultSQLiteBenchmarkPattern,
		reportVLogRewrite:      true,
		reportLeafGenPackGC:    true,
		reportSQLiteVacuum:     true,
		availableBenchmarks:    true,
	}

	fs := flag.NewFlagSet("collection_bench_matrix", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.repoRoot, "repo-root", "", "Repository root; defaults to git rev-parse --show-toplevel")
	fs.StringVar(&cfg.outDir, "out-dir", "", "Output directory; defaults to /tmp/collection_bench_matrix_<timestamp>")
	fs.StringVar(&cfg.goBinary, "go", cfg.goBinary, "go binary to execute")
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
	fs.BoolVar(&cfg.reportVLogRewrite, "report-vlog-rewrite", cfg.reportVLogRewrite, "Run TreeDB online value_vlog rewrite/GC measurement after insert-shape benchmarks")
	fs.BoolVar(&cfg.reportLeafGenPackGC, "report-leafgen-pack-gc", cfg.reportLeafGenPackGC, "Run TreeDB leaf_vlog generation pack/GC measurement after insert-shape benchmarks")
	fs.BoolVar(&cfg.reportSQLiteVacuum, "report-sqlite-vacuum", cfg.reportSQLiteVacuum, "Run SQLite VACUUM measurement after insert-shape benchmarks")
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
	if cfg.leafSegmentTargetBytes < 0 {
		return config{}, fmt.Errorf("-leaf-segment-target-bytes must be >= 0")
	}
	if strings.TrimSpace(cfg.benchtime) == "" {
		return config{}, fmt.Errorf("-benchtime is required")
	}
	if strings.TrimSpace(cfg.goBinary) == "" {
		return config{}, fmt.Errorf("-go is required")
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
	if cfg.outDir == "" {
		cfg.outDir = defaultOutputDir(time.Now().UTC())
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
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
		if err := os.MkdirAll(cellDir, 0o755); err != nil {
			return fmt.Errorf("create cell directory %s: %w", cell.Name, err)
		}
		cell.RawJSONPath = filepath.Join(cellDir, "go_test.json")
		cell.ReportMarkdownPath = filepath.Join(cellDir, "collections_report.md")
		cell.ReportJSONPath = filepath.Join(cellDir, "collections_report.json")
		if err := runBenchmarkCell(cfg, *cell); err != nil {
			return err
		}
		if err := runCellReport(cfg, *cell, branch, commit); err != nil {
			return err
		}
	}

	matrixIndexPath := filepath.Join(cfg.outDir, "matrix_index.tsv")
	if err := writeMatrixIndex(matrixIndexPath, cells); err != nil {
		return err
	}
	if err := runMatrixSummary(cfg, matrixIndexPath); err != nil {
		return err
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
	cmd.Env = append(os.Environ(), cell.Env...)
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
		"-count", strconv.Itoa(cfg.count),
		"-benchtime", cfg.benchtime,
		"-json",
	)
	return args
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
	}); err != nil {
		return err
	}
	for _, cell := range cells {
		if err := writer.Write([]string{
			cell.Name,
			cell.Engine,
			cell.DocumentFormat,
			cell.DataOuterLeavesInVLog,
			cell.IndexOuterLeavesInVLog,
			cell.PagerChunkSize,
			cell.PagerSyncConcurrency,
			cell.ReportMarkdownPath,
			cell.ReportJSONPath,
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
	sb.WriteString("- generated at: `")
	sb.WriteString(time.Now().UTC().Format(time.RFC3339))
	sb.WriteString("`\n")
	sb.WriteString("- worktree: `")
	sb.WriteString(cfg.repoRoot)
	sb.WriteString("`\n")
	sb.WriteString("- branch: `")
	sb.WriteString(branch)
	sb.WriteString("`\n")
	sb.WriteString("- commit: `")
	sb.WriteString(commit)
	sb.WriteString("`\n")
	sb.WriteString("- command: `")
	sb.WriteString(strings.Join(commandLine, " "))
	sb.WriteString("`\n")
	sb.WriteString("- benchtime: `")
	sb.WriteString(cfg.benchtime)
	sb.WriteString("`\n")
	sb.WriteString("- count: `")
	sb.WriteString(strconv.Itoa(cfg.count))
	sb.WriteString("`\n")
	sb.WriteString("- collection batch size: `")
	sb.WriteString(strconv.Itoa(cfg.batchSize))
	sb.WriteString("`\n\n")

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
	sb.WriteString("| Cell | Engine | Format | Data vlog | Index vlog | Bench pattern | Raw JSON | Report |\n")
	sb.WriteString("| --- | --- | --- | ---: | ---: | --- | --- | --- |\n")
	for _, cell := range cells {
		rawRel := relativeArtifactPath(cfg.outDir, cell.RawJSONPath)
		reportRel := relativeArtifactPath(cfg.outDir, cell.ReportMarkdownPath)
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
		sb.WriteString(") |\n")
	}
	readmePath := filepath.Join(cfg.outDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(sb.String()), 0o644); err != nil {
		return fmt.Errorf("write run README: %w", err)
	}
	return nil
}

func relativeArtifactPath(baseDir, artifactPath string) string {
	rel, err := filepath.Rel(baseDir, artifactPath)
	if err != nil {
		return artifactPath
	}
	return rel
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
