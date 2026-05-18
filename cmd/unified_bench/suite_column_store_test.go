package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestRunColumnStoreSuiteWritesArtifactsAndMetricsM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:                 64,
		BatchSize:            16,
		DBsArg:               "treedb",
		Profile:              "durable",
		Progress:             false,
		SeedUsed:             1,
		CPUProfile:           filepath.Join(dir, "cpu"),
		AllocsProfile:        filepath.Join(dir, "allocs"),
		AllocsProfileRate:    1,
		CheckpointCPUProfile: filepath.Join(dir, "checkpoint_cpu"),
		BlockProfile:         filepath.Join(dir, "block.pprof"),
		BlockProfileRate:     1,
		MutexProfile:         filepath.Join(dir, "mutex.pprof"),
		MutexProfileFraction: 1,
		TraceProfile:         filepath.Join(dir, "trace.out"),
	}

	out, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathRowStoreBaseline,
		RunBenchprof:  true,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite: %v", err)
	}

	for _, want := range []string{
		"# unified_bench suite: column_store",
		"q1",
		"q4a",
		"q4b",
		"q5_metadata",
		"row_store_baseline",
		"Parity",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("suite output missing %q:\n%s", want, out)
		}
	}

	for _, name := range []string{
		"benchprof_results.json",
		"benchprof_results.md",
		"insights.md",
		"insights.json",
		"insights.html",
		"column_store_results.json",
		"column_store_results.md",
		"column_store_results.html",
		"cpu_column_store_treedb_column_store.pprof",
		"allocs_column_store_treedb_column_store.pprof",
		"checkpoint_cpu_checkpoint_column_store_treedb_column_store.pprof",
		"block.pprof",
		"block_column_store_treedb_column_store.pprof",
		"mutex.pprof",
		"mutex_column_store_treedb_column_store.pprof",
		"trace.out",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
	}

	var benchprof benchprofExport
	benchprofData, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatalf("read benchprof_results.json: %v", err)
	}
	if err := json.Unmarshal(benchprofData, &benchprof); err != nil {
		t.Fatalf("unmarshal benchprof_results.json: %v", err)
	}
	if len(benchprof.Runs) != 1 {
		t.Fatalf("benchprof runs=%d want 1", len(benchprof.Runs))
	}
	if got := benchprof.Runs[0].Results["column_store"]["TreeDB Column Store"]; got <= 0 {
		t.Fatalf("benchprof column_store aggregate rows/sec=%f want positive", got)
	}
	benchprofMarkdown, err := os.ReadFile(filepath.Join(dir, "benchprof_results.md"))
	if err != nil {
		t.Fatalf("read benchprof_results.md: %v", err)
	}
	if !strings.Contains(string(benchprofMarkdown), "Column store query phase") {
		t.Fatalf("benchprof markdown missing aggregate column_store display name:\n%s", benchprofMarkdown)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	if got, want := report.Suite, "column_store"; got != want {
		t.Fatalf("suite=%q want %q", got, want)
	}
	if got, want := report.Profile, "durable"; got != want {
		t.Fatalf("profile=%q want %q", got, want)
	}
	if got, want := report.PathLabel, "native-fastpath"; got != want {
		t.Fatalf("path_label=%q want %q", got, want)
	}
	if got, want := report.ForcedPath, columnStorePathRowStoreBaseline; got != want {
		t.Fatalf("forced_path=%q want %q", got, want)
	}
	if len(report.Queries) < 7 {
		t.Fatalf("expected q1-q5/q4a/q4b/q5_metadata query metrics, got %d", len(report.Queries))
	}
	for _, q := range report.Queries {
		if q.PlanLabel != columnStorePathRowStoreBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathRowStoreBaseline)
		}
		if q.DurationMS < 0 || q.RowsPerSecond < 0 {
			t.Fatalf("query %s has invalid timing metrics: %+v", q.Name, q)
		}
		if q.BytesRead <= 0 {
			t.Fatalf("query %s bytes_read=%d", q.Name, q.BytesRead)
		}
		if q.ScanDurationMS < 0 {
			t.Fatalf("query %s scan_duration_ms=%v", q.Name, q.ScanDurationMS)
		}
		if q.PlannerCandidates == 0 || q.PlannerReason == "" {
			t.Fatalf("query %s missing planner diagnostics: %+v", q.Name, q)
		}
		if q.RowMaterializations != report.Rows {
			t.Fatalf("query %s row_materializations=%d want %d", q.Name, q.RowMaterializations, report.Rows)
		}
	}
	assertColumnStoreParityCoverageM11A(t, report.Parity)
	for name, parity := range report.Parity {
		if !parity.Pass {
			t.Fatalf("parity %s failed: %+v", name, parity)
		}
		if parity.RawHash == 0 || parity.ProductionHash == 0 {
			t.Fatalf("parity %s has zero hash: %+v", name, parity)
		}
	}
	if report.ByteAccounting.CommandWALBytes == 0 {
		t.Fatalf("expected command WAL bytes in byte accounting: %+v", report.ByteAccounting)
	}
	if report.ByteAccounting.ManifestControlBytes == 0 || report.ByteAccounting.DBTotalBytes == 0 || report.ByteAccounting.DBTotalFiles == 0 {
		t.Fatalf("expected measured manifest/control and DB byte accounting: %+v", report.ByteAccounting)
	}
	if report.Manifest.ActiveGeneration == 0 || report.Manifest.AppliedCommandLSN == 0 {
		t.Fatalf("expected active/recovery-authoritative manifest identity: %+v", report.Manifest)
	}
	if len(report.UnsupportedForcedPaths) == 0 {
		t.Fatalf("expected unsupported forced path labels to be recorded")
	}
	if report.Artifacts.CPUProfile == "" ||
		report.Artifacts.AllocsProfile == "" ||
		report.Artifacts.CheckpointCPUProfile == "" ||
		report.Artifacts.BlockProfile == "" ||
		report.Artifacts.BlockDeltaProfile == "" ||
		report.Artifacts.MutexProfile == "" ||
		report.Artifacts.MutexDeltaProfile == "" ||
		report.Artifacts.TraceProfile == "" {
		t.Fatalf("expected all configured profile artifacts to be recorded: %+v", report.Artifacts)
	}
}

func TestColumnStoreSuiteRuntimeProfilesDoNotEnableOnCreateFailureM11A(t *testing.T) {
	prevMutexFraction := runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(prevMutexFraction)
	})

	_, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		MutexProfile:         filepath.Join(t.TempDir(), "missing", "mutex.pprof"),
		MutexProfileFraction: 1,
	})
	if err == nil {
		t.Fatal("expected mutex profile create failure")
	}
	if !strings.Contains(err.Error(), "mutexprofile") {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPrev := runtime.SetMutexProfileFraction(0); gotPrev != 0 {
		t.Fatalf("mutex profiler was left enabled after create failure: previous fraction=%d", gotPrev)
	}
}

func TestColumnStoreSuiteCPUProfileStartsAfterProfileBaselinesM11A(t *testing.T) {
	origStartCPUProfileFn := startCPUProfileFn
	origStopCPUProfileFn := stopCPUProfileFn
	origWriteAllocsSnapshotTempFn := writeAllocsSnapshotTempFn
	origWriteAllocsDeltaProfileFn := writeAllocsDeltaProfileFn
	origWriteRuntimeProfileSnapshotTempFn := writeRuntimeProfileSnapshotTempFn
	origWriteRuntimeProfileDeltaProfileFn := writeRuntimeProfileDeltaProfileFn
	t.Cleanup(func() {
		startCPUProfileFn = origStartCPUProfileFn
		stopCPUProfileFn = origStopCPUProfileFn
		writeAllocsSnapshotTempFn = origWriteAllocsSnapshotTempFn
		writeAllocsDeltaProfileFn = origWriteAllocsDeltaProfileFn
		writeRuntimeProfileSnapshotTempFn = origWriteRuntimeProfileSnapshotTempFn
		writeRuntimeProfileDeltaProfileFn = origWriteRuntimeProfileDeltaProfileFn
	})

	var events []string
	profileTmpDir := t.TempDir()
	newProfilePath := func(prefix string) (string, error) {
		f, err := os.CreateTemp(profileTmpDir, prefix+"_*.pprof")
		if err != nil {
			return "", err
		}
		path := f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}
	startCPUProfileFn = func(_ io.Writer) error {
		events = append(events, "cpu_start")
		return nil
	}
	stopCPUProfileFn = func() {
		events = append(events, "cpu_stop")
	}
	writeAllocsSnapshotTempFn = func(prefix string) (string, error) {
		events = append(events, prefix)
		return newProfilePath(prefix)
	}
	writeRuntimeProfileSnapshotTempFn = func(prefix, profileName string) (string, error) {
		events = append(events, prefix)
		return newProfilePath(prefix)
	}
	writeAllocsDeltaProfileFn = func(basePath, afterPath, outPath string) error {
		events = append(events, "alloc_delta")
		return nil
	}
	writeRuntimeProfileDeltaProfileFn = func(basePath, afterPath, outPath string) (bool, error) {
		events = append(events, filepath.Base(outPath))
		return true, nil
	}

	fixture, _ := buildColumnStoreSyntheticFixture(8, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfig(),
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, fixture, 4); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	rawHashes, err := columnStoreReferenceHashes(fixture)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}

	_, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		CPUProfile:           filepath.Join(profileTmpDir, "cpu"),
		AllocsProfile:        filepath.Join(profileTmpDir, "allocs"),
		AllocsProfileRate:    1,
		BlockProfile:         filepath.Join(profileTmpDir, "block.pprof"),
		MutexProfile:         filepath.Join(profileTmpDir, "mutex.pprof"),
		MutexProfileFraction: 1,
	}, collection, len(fixture), rawHashes, columnStorePathRowStoreBaseline)
	if err != nil {
		t.Fatalf("profiled queries: %v", err)
	}
	if parityErr != nil {
		t.Fatalf("parity: %v", parityErr)
	}
	assertColumnStoreParityCoverageM11A(t, parity)

	indexOf := func(target string) int {
		for i, event := range events {
			if event == target {
				return i
			}
		}
		return -1
	}
	cpuStartIdx := indexOf("cpu_start")
	cpuStopIdx := indexOf("cpu_stop")
	if cpuStartIdx < 0 || cpuStopIdx < 0 {
		t.Fatalf("missing CPU profile events: %v", events)
	}
	for _, baseline := range []string{
		"unified_bench_column_store_allocs_base",
		"unified_bench_column_store_block_base",
		"unified_bench_column_store_mutex_base",
	} {
		idx := indexOf(baseline)
		if idx < 0 {
			t.Fatalf("missing baseline event %s: %v", baseline, events)
		}
		if idx > cpuStartIdx {
			t.Fatalf("expected %s before cpu_start: %v", baseline, events)
		}
	}
	for _, after := range []string{
		"unified_bench_column_store_allocs_after",
		"unified_bench_column_store_block_after",
		"unified_bench_column_store_mutex_after",
	} {
		idx := indexOf(after)
		if idx < 0 {
			t.Fatalf("missing after event %s: %v", after, events)
		}
		if cpuStopIdx > idx {
			t.Fatalf("expected cpu_stop before %s: %v", after, events)
		}
	}
}

func TestColumnStoreQueryHashRejectsUnknownQueryM11A(t *testing.T) {
	_, _, err := columnStoreQueryHash("missing_query", nil)
	if err == nil {
		t.Fatal("expected unknown query to fail")
	}
	if !strings.Contains(err.Error(), "unknown column_store query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteRejectsForcedColumnPathM11B(t *testing.T) {
	cfg := BenchConfig{Keys: 8, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ForcedPath: "serial_column_scan",
	})
	if err == nil {
		t.Fatal("expected forced serial column scan to fail closed")
	}
	if !errors.Is(err, collections.ErrColumnQueryPlanUnsupported) {
		t.Fatalf("expected ErrColumnQueryPlanUnsupported, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "serial_column_scan") || !strings.Contains(msg, "unsupported") || !strings.Contains(msg, "reason=") || !strings.Contains(msg, "physical column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteRunsBTreeIndexBaselineM11B(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:      64,
		BatchSize: 16,
		DBsArg:    "treedb",
		Profile:   "durable",
		Progress:  false,
		SeedUsed:  1,
	}

	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathBTreeIndexBaseline,
		RunBenchprof:  false,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite btree baseline: %v", err)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	for _, q := range report.Queries {
		if q.PlanLabel != columnStorePathBTreeIndexBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathBTreeIndexBaseline)
		}
		if q.PlannerReason == "" || q.PlannerCandidates == 0 {
			t.Fatalf("query %s missing planner diagnostics: %+v", q.Name, q)
		}
		if q.RowMaterializations != report.Rows {
			t.Fatalf("query %s row_materializations=%d want %d", q.Name, q.RowMaterializations, report.Rows)
		}
	}
	for name, parity := range report.Parity {
		if !parity.Pass {
			t.Fatalf("parity %s failed: %+v", name, parity)
		}
	}
}

func TestColumnStoreSuiteQueriesNormalizeForcedPathAliasesM11B(t *testing.T) {
	const rows = 16
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	db, err := openColumnStoreSuiteDB(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(columnStorePathRowStoreBaseline)); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, 8); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}

	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}
	queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, "row")
	if err != nil {
		t.Fatalf("runColumnStoreSuiteQueries row alias: %v", err)
	}
	if len(queries) == 0 || len(parity) == 0 {
		t.Fatalf("queries=%d parity=%d want non-empty", len(queries), len(parity))
	}
	for _, q := range queries {
		if q.PlanLabel != columnStorePathRowStoreBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathRowStoreBaseline)
		}
	}
}

func TestColumnStoreSuiteM11BIndexCandidateCoverage(t *testing.T) {
	for _, name := range columnStoreQueryNames() {
		if got := columnStoreSuiteQueryIndexCandidates(name); len(got) == 0 {
			t.Fatalf("query %s has no B-tree baseline candidate columns", name)
		}
	}
}

func TestColumnStoreSuiteReportsParityMismatchM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{Keys: 16, BatchSize: 8, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:              dir,
		ExecutionPath:           "native-fastpath",
		ForcedPath:              columnStorePathRowStoreBaseline,
		CorruptReferenceForTest: "q1",
	})
	if err == nil {
		t.Fatal("expected parity mismatch")
	}
	if !strings.Contains(err.Error(), "parity mismatch") || !strings.Contains(err.Error(), "q1") {
		t.Fatalf("unexpected parity error: %v", err)
	}

	var report columnStoreSuiteReport
	data, readErr := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if readErr != nil {
		t.Fatalf("expected column_store_results.json after mismatch: %v", readErr)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	assertColumnStoreParityCoverageM11A(t, report.Parity)
	q1, ok := report.Parity["q1"]
	if !ok {
		t.Fatal("missing q1 parity entry")
	}
	if q1.Pass {
		t.Fatalf("expected q1 parity to be recorded as failed: %+v", q1)
	}
}

func TestColumnStoreSuiteRejectsMixedDBSelectionM11A(t *testing.T) {
	err := validateColumnStoreSuiteDBSelection("treedb,leveldb", "")
	if err == nil {
		t.Fatal("expected mixed DB selection to fail")
	}
	if !strings.Contains(err.Error(), "only supports TreeDB") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteRejectsExcludedTreeDBM11A(t *testing.T) {
	err := validateColumnStoreSuiteDBSelection("all", "treedb")
	if err == nil {
		t.Fatal("expected TreeDB exclusion to fail")
	}
	if !strings.Contains(err.Error(), "excludes it") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteDirUsageFailsOnMissingPathM11A(t *testing.T) {
	_, _, err := columnStoreSuiteDirUsage(filepath.Join(t.TempDir(), "missing"))
	if err == nil {
		t.Fatal("expected missing path to fail")
	}
}

func TestColumnStoreSuiteManifestControlUsageTreatsMissingFilesAsZeroM11A(t *testing.T) {
	bytes, missing, err := columnStoreSuiteManifestControlUsage(t.TempDir())
	if err != nil {
		t.Fatalf("manifest/control usage: %v", err)
	}
	if bytes != 0 {
		t.Fatalf("manifest/control bytes=%d want 0", bytes)
	}
	if len(missing) != len(columnStoreSuiteManifestControlFiles) {
		t.Fatalf("missing manifest/control files=%v want %v", missing, columnStoreSuiteManifestControlFiles)
	}
}

func TestColumnStoreSuiteREADMEDocumentsCommandM11A(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"-suite column_store",
		"-column-store-path row_store_baseline",
		"column_store_results.html",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("README missing %q", want)
		}
	}
}

func BenchmarkColumnStoreSuiteRowBaselineQueriesM11B(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathRowStoreBaseline)
}

func BenchmarkColumnStoreSuiteBTreeIndexBaselineQueriesM11B(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathBTreeIndexBaseline)
}

func benchmarkColumnStoreSuiteQueriesM11B(b *testing.B, path string) {
	const rows = 10_000
	const batchSize = 1_000

	events, sourceBytes := buildColumnStoreSyntheticFixture(rows, 1)
	dir := b.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(path)); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, batchSize); err != nil {
		b.Fatalf("insert fixture: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	db, err = openColumnStoreSuiteDB(dir)
	if err != nil {
		b.Fatalf("reopen db: %v", err)
	}
	defer db.Close()
	collection, err = collections.NewCollectionManager(db).OpenCollection("events")
	if err != nil {
		b.Fatalf("reopen collection: %v", err)
	}

	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		b.Fatalf("reference hashes: %v", err)
	}
	queryCount := len(columnStoreQueryNames())
	b.ReportAllocs()
	b.SetBytes(sourceBytes * int64(queryCount))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, path)
		if err != nil {
			b.Fatalf("queries: %v", err)
		}
		if len(queries) != queryCount {
			b.Fatalf("queries=%d want %d", len(queries), queryCount)
		}
		for name, p := range parity {
			if !p.Pass {
				b.Fatalf("parity failed for %s: %+v", name, p)
			}
		}
		if len(parity) != queryCount {
			b.Fatalf("parity=%d want %d", len(parity), queryCount)
		}
	}
	b.ReportMetric(float64(rows*queryCount), "rows/op")
}

func assertColumnStoreParityCoverageM11A(t *testing.T, parity map[string]columnStoreParity) {
	t.Helper()
	names := columnStoreQueryNames()
	if len(parity) != len(names) {
		t.Fatalf("parity entries=%d want %d", len(parity), len(names))
	}
	for _, name := range names {
		if _, ok := parity[name]; !ok {
			t.Fatalf("missing parity entry for %s", name)
		}
	}
}
