package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestBuildMatrixCellsDefaults(t *testing.T) {
	cfg, err := parseFlags([]string{"-out-dir", t.TempDir()})
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	cells, err := buildMatrixCells(cfg)
	if err != nil {
		t.Fatalf("build cells: %v", err)
	}
	if got, want := len(cells), 5; got != want {
		t.Fatalf("len(cells)=%d want %d", got, want)
	}
	names := make(map[string]matrixCell)
	for _, cell := range cells {
		names[cell.Name] = cell
	}
	for _, want := range []string{
		"treedb_command_wal_relaxed_json_data_vlog_index_pager",
		"treedb_command_wal_relaxed_json_data_vlog_index_vlog",
		"treedb_command_wal_relaxed_template_v1_data_vlog_index_pager",
		"treedb_command_wal_relaxed_template_v1_data_vlog_index_vlog",
		"sqlite_wal_normal",
	} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing default cell %q in %#v", want, cells)
		}
	}
	tree := names["treedb_command_wal_relaxed_template_v1_data_vlog_index_vlog"]
	if tree.ExecutionPath != "native-fastpath" {
		t.Fatalf("tree execution path=%q", tree.ExecutionPath)
	}
	if tree.DocumentFormat != "template-v1" {
		t.Fatalf("tree document format=%q", tree.DocumentFormat)
	}
	if tree.DataOuterLeavesInVLog != "true" || tree.IndexOuterLeavesInVLog != "true" {
		t.Fatalf("tree storage data=%q index=%q", tree.DataOuterLeavesInVLog, tree.IndexOuterLeavesInVLog)
	}
	if !containsEnv(tree.Env, "TREEDB_COLLECTION_REPORT_VLOG_REWRITE=true") {
		t.Fatalf("tree env missing rewrite toggle: %#v", tree.Env)
	}
	if !containsEnv(tree.Env, "TREEDB_COLLECTION_REPORT_POST_MAINTENANCE_INDEX_VACUUM=true") {
		t.Fatalf("tree env missing post-maintenance vacuum toggle: %#v", tree.Env)
	}
	if !containsEnv(tree.Env, "TREEDB_COLLECTION_LEAFGEN_PACK_FRAME_K=0") {
		t.Fatalf("tree env missing leafgen frame K: %#v", tree.Env)
	}
	sqlite := names["sqlite_wal_normal"]
	if sqlite.ExecutionPath != "sqlite" {
		t.Fatalf("sqlite execution path=%q", sqlite.ExecutionPath)
	}
	if len(sqlite.Tags) != 1 || sqlite.Tags[0] != "sqlite_bench" {
		t.Fatalf("sqlite tags=%#v", sqlite.Tags)
	}
	if !containsEnv(sqlite.Env, "TREEDB_COLLECTION_REPORT_SQLITE_VACUUM=true") {
		t.Fatalf("sqlite env missing vacuum toggle: %#v", sqlite.Env)
	}
}

func TestBuildMatrixCellsCanSkipSQLiteAndSelectInline(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-out-dir", t.TempDir(),
		"-formats", "json",
		"-storage-cells", "inline",
		"-skip-sqlite",
		"-batch-size", "32000",
		"-pager-chunk-size", "65536",
		"-pager-sync-concurrency", "8",
		"-leafgen-pack-frame-k", "4",
	})
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	cells, err := buildMatrixCells(cfg)
	if err != nil {
		t.Fatalf("build cells: %v", err)
	}
	if got, want := len(cells), 1; got != want {
		t.Fatalf("len(cells)=%d want %d", got, want)
	}
	cell := cells[0]
	if cell.DataOuterLeavesInVLog != "false" || cell.IndexOuterLeavesInVLog != "false" {
		t.Fatalf("inline cell data=%q index=%q", cell.DataOuterLeavesInVLog, cell.IndexOuterLeavesInVLog)
	}
	for _, want := range []string{
		"TREEDB_COLLECTION_BENCH_BATCH_SIZE=32000",
		"TREEDB_COLLECTION_CHUNK_SIZE=65536",
		"TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY=8",
		"TREEDB_COLLECTION_LEAFGEN_PACK_FRAME_K=4",
	} {
		if !containsEnv(cell.Env, want) {
			t.Fatalf("env missing %q: %#v", want, cell.Env)
		}
	}
	if cell.PagerChunkSize != "65536" || cell.PagerSyncConcurrency != "8" {
		t.Fatalf("pager labels chunk=%q sync=%q", cell.PagerChunkSize, cell.PagerSyncConcurrency)
	}
}

func TestBuildMatrixCellsRejectsDuplicateNames(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-out-dir", t.TempDir(),
		"-formats", "json",
		"-storage-cells", "mainline,data-vlog",
		"-skip-sqlite",
	})
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	_, err = buildMatrixCells(cfg)
	if err == nil || !strings.Contains(err.Error(), "duplicate matrix cell") {
		t.Fatalf("build cells err=%v want duplicate matrix cell", err)
	}
}

func TestDefaultBenchmarkPatternsCoverStrictSummary(t *testing.T) {
	treePattern := regexp.MustCompile(defaultTreeBenchmarkPattern)
	for _, name := range []string{
		"BenchmarkCollectionOverheadIndexStateJSONExtraction",
		"BenchmarkCollectionOverheadIndexStateTemplateV1Extraction",
		"BenchmarkCollectionOverheadPlanIndexedTemplateV1",
		"BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
		"BenchmarkCollectionInsertBatchWithSecondaryIndexes",
		"BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes",
		"BenchmarkCollectionShapeReadPrimaryInto",
		"BenchmarkCollectionShapeReadPrimaryIntoParallel",
		"BenchmarkCollectionMixedReadWriteScalingPrimary",
		"BenchmarkCollectionMixedReadWriteScalingSecondaryUnique",
		"BenchmarkSecondaryLookupRangeString",
		"BenchmarkSecondaryLookupRangeStringScanFallback",
	} {
		if !treePattern.MatchString(name) {
			t.Fatalf("default tree pattern does not match strict benchmark %q", name)
		}
	}

	sqlitePattern := regexp.MustCompile(defaultSQLiteBenchmarkPattern)
	for _, name := range []string{
		"BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
		"BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
		"BenchmarkSQLiteShapeSecondaryRangeJSON",
		"BenchmarkSQLiteShapeSecondaryRangeNativeColumns",
	} {
		if !sqlitePattern.MatchString(name) {
			t.Fatalf("default sqlite pattern does not match strict benchmark %q", name)
		}
	}
}

func TestParseFlagsRejectsNegativePagerOverrides(t *testing.T) {
	for _, args := range [][]string{
		{"-out-dir", t.TempDir(), "-pager-chunk-size", "-1"},
		{"-out-dir", t.TempDir(), "-pager-sync-concurrency", "-1"},
	} {
		if _, err := parseFlags(args); err == nil {
			t.Fatalf("parseFlags(%v) succeeded, want error", args)
		}
	}
}

func TestGoTestArgsIncludeSQLiteTagsOnlyForSQLiteCell(t *testing.T) {
	cfg, err := parseFlags([]string{"-out-dir", t.TempDir(), "-benchtime", "10x"})
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	tree := matrixCell{BenchmarkPattern: "BenchmarkCollectionShapeInsertBatch"}
	treeArgs := strings.Join(goTestArgs(tree, cfg), " ")
	if strings.Contains(treeArgs, "-tags") {
		t.Fatalf("tree args unexpectedly include tags: %s", treeArgs)
	}
	if !strings.Contains(treeArgs, "-benchtime 10x") {
		t.Fatalf("tree args missing benchtime: %s", treeArgs)
	}
	if !strings.Contains(treeArgs, "-timeout 0") {
		t.Fatalf("tree args missing disabled timeout: %s", treeArgs)
	}

	sqlite := matrixCell{BenchmarkPattern: "BenchmarkSQLiteShapeInsertBatchJSON", Tags: []string{"sqlite_bench"}}
	sqliteArgs := strings.Join(goTestArgs(sqlite, cfg), " ")
	if !strings.Contains(sqliteArgs, "-tags sqlite_bench") {
		t.Fatalf("sqlite args missing tags: %s", sqliteArgs)
	}
}

func TestGoTestProfileArgsWritePprofArtifacts(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-out-dir", t.TempDir(),
		"-benchtime", "10x",
		"-profile-cells",
		"-profile-benchtime", "3x",
		"-profile-count", "2",
	})
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	cell := matrixCell{
		BenchmarkPattern: "BenchmarkCollectionShapeInsertBatch",
		ProfileDir:       filepath.Join(t.TempDir(), "profiles"),
	}
	args := strings.Join(goTestProfileArgs(cell, cfg), " ")
	for _, want := range []string{
		"-benchtime 3x",
		"-count 2",
		"-cpuprofile " + filepath.Join(cell.ProfileDir, "cpu.pprof"),
		"-memprofile " + filepath.Join(cell.ProfileDir, "allocs.pprof"),
		"-blockprofile " + filepath.Join(cell.ProfileDir, "block.pprof"),
		"-mutexprofile " + filepath.Join(cell.ProfileDir, "mutex.pprof"),
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("profile args missing %q: %s", want, args)
		}
	}
}

func TestProfileCellEnvDisablesMaintenanceArtifacts(t *testing.T) {
	env := profileCellEnv([]string{
		"TREEDB_COLLECTION_REPORT_VLOG_REWRITE=true",
		"TREEDB_COLLECTION_REPORT_SQLITE_VACUUM=true",
	})
	for _, want := range []string{
		"TREEDB_COLLECTION_REPORT_DISK_USAGE=false",
		"TREEDB_COLLECTION_REPORT_VLOG_REWRITE=false",
		"TREEDB_COLLECTION_REPORT_LEAFGEN_PACK_GC=false",
		"TREEDB_COLLECTION_REPORT_POST_MAINTENANCE_INDEX_VACUUM=false",
		"TREEDB_COLLECTION_REPORT_SQLITE_VACUUM=false",
	} {
		if !containsEnv(env, want) {
			t.Fatalf("profile env missing %q: %#v", want, env)
		}
	}
	for _, unwanted := range []string{
		"TREEDB_COLLECTION_REPORT_VLOG_REWRITE=true",
		"TREEDB_COLLECTION_REPORT_SQLITE_VACUUM=true",
	} {
		if containsEnv(env, unwanted) {
			t.Fatalf("profile env retained overridden value %q: %#v", unwanted, env)
		}
	}
}

func TestEnvWithOverridesRemovesParentValues(t *testing.T) {
	env := envWithOverrides([]string{
		"TREEDB_COLLECTION_REPORT_VLOG_REWRITE=true",
		"KEEP=this",
	}, "TREEDB_COLLECTION_REPORT_VLOG_REWRITE=false")
	if containsEnv(env, "TREEDB_COLLECTION_REPORT_VLOG_REWRITE=true") {
		t.Fatalf("env retained parent value: %#v", env)
	}
	if !containsEnv(env, "TREEDB_COLLECTION_REPORT_VLOG_REWRITE=false") || !containsEnv(env, "KEEP=this") {
		t.Fatalf("env missing override or unrelated value: %#v", env)
	}
}

func TestSQLiteBenchmarkListHasSQLite(t *testing.T) {
	if !sqliteBenchmarkListHasSQLite([]byte("BenchmarkSQLiteShapeInsertBatchJSON/indexes_0\nok package\n")) {
		t.Fatal("sqliteBenchmarkListHasSQLite=false want true")
	}
	if sqliteBenchmarkListHasSQLite([]byte("ok package\n")) {
		t.Fatal("sqliteBenchmarkListHasSQLite=true want false")
	}
}

func TestDefaultOutputDirIncludesSubsecondEntropy(t *testing.T) {
	base := time.Date(2026, 4, 28, 12, 30, 45, 1, time.UTC)
	first := defaultOutputDir(base)
	second := defaultOutputDir(base.Add(time.Nanosecond))
	if first == second {
		t.Fatalf("default output dirs collided: %q", first)
	}
	if !strings.Contains(filepath.Base(first), "collection_bench_matrix_20260428_123045_") {
		t.Fatalf("default output dir missing stable prefix: %q", first)
	}
}

func TestRelativeArtifactPath(t *testing.T) {
	base := t.TempDir()
	artifact := filepath.Join(base, "cell", "collections_report.md")
	got, err := relativeArtifactPath(base, artifact)
	if err != nil {
		t.Fatalf("relativeArtifactPath: %v", err)
	}
	if want := filepath.Join("cell", "collections_report.md"); got != want {
		t.Fatalf("relativeArtifactPath=%q want %q", got, want)
	}
}

func TestRelativeArtifactPathRejectsEscapes(t *testing.T) {
	base := t.TempDir()
	artifact := filepath.Join(filepath.Dir(base), "outside.md")
	if _, err := relativeArtifactPath(base, artifact); err == nil || !strings.Contains(err.Error(), "escapes output directory") {
		t.Fatalf("relativeArtifactPath err=%v want escape rejection", err)
	}
}

func TestShellQuoteCommand(t *testing.T) {
	got := shellQuoteCommand([]string{"collection_bench_matrix", "-out-dir", "/tmp/bench run", "-tree-bench-pattern", "^Benchmark(foo)$", "a'b"})
	want := "collection_bench_matrix -out-dir '/tmp/bench run' -tree-bench-pattern '^Benchmark(foo)$' 'a'\"'\"'b'"
	if got != want {
		t.Fatalf("shellQuoteCommand=%q want %q", got, want)
	}
}

func TestReproducibleCommandLine(t *testing.T) {
	got := reproducibleCommandLine([]string{filepath.Join(os.TempDir(), "go-build123", "collection_bench_matrix"), "-out-dir", "/tmp/run"})
	want := []string{"go", "run", "./cmd/collection_bench_matrix", "-out-dir", "/tmp/run"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("reproducibleCommandLine=%#v want %#v", got, want)
	}
}

func TestWriteMatrixIndex(t *testing.T) {
	dir := t.TempDir()
	cells := []matrixCell{
		{
			Name:                   "treedb_json",
			Engine:                 "bench",
			DocumentFormat:         "json",
			DataOuterLeavesInVLog:  "true",
			IndexOuterLeavesInVLog: "false",
			PagerChunkSize:         "profile/default",
			PagerSyncConcurrency:   "profile/default",
			ReportMarkdownPath:     filepath.Join(dir, "treedb_json", "collections_report.md"),
			ReportJSONPath:         filepath.Join(dir, "treedb_json", "collections_report.json"),
		},
	}
	path := filepath.Join(dir, "matrix_index.tsv")
	if err := writeMatrixIndex(path, cells); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read matrix index: %v", err)
	}
	got := string(raw)
	if !strings.Contains(got, "cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json") {
		t.Fatalf("missing header:\n%s", got)
	}
	if !strings.Contains(got, "treedb_json\tbench\tjson\ttrue\tfalse\tprofile/default\tprofile/default\t") {
		t.Fatalf("missing row:\n%s", got)
	}
	if strings.Contains(got, dir) {
		t.Fatalf("matrix index contains non-portable absolute path:\n%s", got)
	}
	if !strings.Contains(got, "treedb_json/collections_report.md") {
		t.Fatalf("missing relative report path:\n%s", got)
	}
	if strings.Contains(got, `treedb_json\collections_report.md`) {
		t.Fatalf("matrix index contains OS-native separators:\n%s", got)
	}
}

func TestWriteRunREADMERecordsBashCommandAndArgv(t *testing.T) {
	dir := t.TempDir()
	cellDir := filepath.Join(dir, "cell_a")
	cells := []matrixCell{
		{
			Name:                   "cell_a",
			Engine:                 "bench",
			DocumentFormat:         "json",
			DataOuterLeavesInVLog:  "true",
			IndexOuterLeavesInVLog: "true",
			BenchmarkPattern:       "^Benchmark$",
			RawJSONPath:            filepath.Join(cellDir, "go_test.json"),
			ReportMarkdownPath:     filepath.Join(cellDir, "collections_report.md"),
		},
	}
	commandLine := []string{"go", "run", "./cmd/collection_bench_matrix", "-out-dir", "/tmp/run"}
	if err := writeRunREADME(config{outDir: dir, benchtime: "1x", count: 1, batchSize: 42}, commandLine, cells, filepath.Join(dir, "matrix_index.tsv"), "branch", "commit"); err != nil {
		t.Fatalf("writeRunREADME: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	got := string(raw)
	if !strings.Contains(got, "- bash command: `go run ./cmd/collection_bench_matrix -out-dir /tmp/run`") {
		t.Fatalf("README missing bash command:\n%s", got)
	}
	if !strings.Contains(got, `- argv json: `+"`"+`["go","run","./cmd/collection_bench_matrix","-out-dir","/tmp/run"]`+"`") {
		t.Fatalf("README missing argv json:\n%s", got)
	}
}

func TestWriteRunREADMEEscapesInlineMetadata(t *testing.T) {
	dir := t.TempDir()
	cellDir := filepath.Join(dir, "cell_a")
	cells := []matrixCell{
		{
			Name:                   "cell_a",
			Engine:                 "bench",
			DocumentFormat:         "json",
			DataOuterLeavesInVLog:  "true",
			IndexOuterLeavesInVLog: "true",
			BenchmarkPattern:       "^Benchmark$",
			RawJSONPath:            filepath.Join(cellDir, "go_test.json"),
			ReportMarkdownPath:     filepath.Join(cellDir, "collections_report.md"),
		},
	}
	commandLine := []string{"go", "run", "./cmd/collection_bench_matrix", "-out-dir", "path`with\nnewline"}
	cfg := config{
		repoRoot:  "repo`root\nnext",
		outDir:    dir,
		benchtime: "1x",
		count:     1,
		batchSize: 42,
	}
	if err := writeRunREADME(cfg, commandLine, cells, filepath.Join(dir, "matrix_index.tsv"), "branch`name", "commit\nsha"); err != nil {
		t.Fatalf("writeRunREADME: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	got := string(raw)
	for _, want := range []string{
		"- worktree: <code>repo`root&#10;next</code>",
		"- branch: <code>branch`name</code>",
		"- commit: <code>commit&#10;sha</code>",
		"- bash command: <code>go run ./cmd/collection_bench_matrix -out-dir &#39;path`with&#10;newline&#39;</code>",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("README missing escaped metadata %q:\n%s", want, got)
		}
	}
}

func TestRunDryRunDoesNotCreateArtifacts(t *testing.T) {
	parent := t.TempDir()
	outDir := filepath.Join(parent, "dry-run-output")
	err := run(config{
		repoRoot:     ".",
		outDir:       outDir,
		formats:      []string{"json"},
		storageCells: []string{"index-vlog"},
		benchtime:    "1x",
		count:        1,
		goBinary:     "go",
		batchSize:    1,
		dryRun:       true,
		skipSQLite:   true,
	}, []string{"collection_bench_matrix", "-dry-run"})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if _, err := os.Stat(outDir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("dry run created output directory %s, err=%v", outDir, err)
	}
}

func containsEnv(env []string, want string) bool {
	for _, got := range env {
		if got == want {
			return true
		}
	}
	return false
}
