package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestRunColumnStoreSuiteWritesArtifactsAndMetricsM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:      64,
		BatchSize: 16,
		DBsArg:    "treedb",
		Profile:   "durable",
		Progress:  false,
		SeedUsed:  1,
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
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
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
		if q.RowsPerSecond <= 0 {
			t.Fatalf("query %s rows_per_second=%v", q.Name, q.RowsPerSecond)
		}
		if q.RowMaterializations != report.Rows {
			t.Fatalf("query %s row_materializations=%d want %d", q.Name, q.RowMaterializations, report.Rows)
		}
	}
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
	if report.Manifest.ActiveGeneration == 0 || report.Manifest.AppliedCommandLSN == 0 {
		t.Fatalf("expected active/recovery-authoritative manifest identity: %+v", report.Manifest)
	}
	if len(report.UnsupportedForcedPaths) == 0 {
		t.Fatalf("expected unsupported forced path labels to be recorded")
	}
}

func TestColumnStoreSuiteRejectsForcedColumnPathM11A(t *testing.T) {
	cfg := BenchConfig{Keys: 8, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ForcedPath: "serial_column_scan",
	})
	if err == nil {
		t.Fatal("expected forced serial column scan to fail closed")
	}
	msg := err.Error()
	if !strings.Contains(msg, "serial_column_scan") || !strings.Contains(msg, "refusing to route through row store") {
		t.Fatalf("unexpected error: %v", err)
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
	if report.Parity["q1"].Pass {
		t.Fatalf("expected q1 parity to be recorded as failed: %+v", report.Parity["q1"])
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

func BenchmarkColumnStoreSuiteRowBaselineQueriesM11A(b *testing.B) {
	const rows = 10_000
	const batchSize = 1_000

	events, sourceBytes := buildColumnStoreSyntheticFixture(rows, 1)
	dir := b.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfig(),
		},
	}); err != nil {
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

	rawHashes := columnStoreReferenceHashes(events)
	queryCount := len(columnStoreQueryNames())
	b.ReportAllocs()
	b.SetBytes(sourceBytes * int64(queryCount))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, columnStorePathRowStoreBaseline)
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
	}
	b.ReportMetric(float64(rows*queryCount), "rows/op")
}
