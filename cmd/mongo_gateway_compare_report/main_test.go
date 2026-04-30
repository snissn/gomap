package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReportFromMatrix(t *testing.T) {
	dir := t.TempDir()
	treedbPath := filepath.Join(dir, "treedb.json")
	mongoPath := filepath.Join(dir, "mongo.json")
	matrixPath := filepath.Join(dir, "matrix.tsv")
	reportPath := filepath.Join(dir, "report.md")
	summaryPath := filepath.Join(dir, "summary.tsv")

	writeFile(t, treedbPath, `{
  "target": "treedb",
  "database": "bench",
  "collection": "docs",
  "documents": 100,
  "secondary_indexes": 1,
  "phases": [
    {"name": "load_insert_many", "operations": 100, "driver_calls": 1, "duration_ms": 10, "ops_per_sec": 10000, "latency_micros": {"p50": 10, "p95": 20, "p99": 30}},
    {"name": "id_find_one", "operations": 100, "driver_calls": 100, "duration_ms": 5, "ops_per_sec": 20000, "latency_micros": {"p50": 5, "p95": 8, "p99": 10}}
  ],
  "treedb_disk_after_checkpoint": {"total_bytes": 1000}
}`)
	writeFile(t, mongoPath, `{
  "target": "mongo",
  "database": "bench",
  "collection": "docs",
  "documents": 100,
  "secondary_indexes": 1,
  "phases": [
    {"name": "load_insert_many", "operations": 100, "driver_calls": 1, "duration_ms": 20, "ops_per_sec": 5000, "latency_micros": {"p50": 20, "p95": 40, "p99": 50}},
    {"name": "id_find_one", "operations": 100, "driver_calls": 100, "duration_ms": 4, "ops_per_sec": 25000, "latency_micros": {"p50": 4, "p95": 7, "p99": 9}}
  ],
  "mongodb_stats_final": {"dataSize": 1300, "storageSize": 1200, "indexSize": 300, "totalSize": 1500}
}`)
	writeFile(t, matrixPath, "target\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\t100\t1\ttreedb.json\t2048\n"+
		"mongo\t100\t1\tmongo.json\t4096\n")

	if err := run([]string{
		"-matrix", matrixPath,
		"-report", reportPath,
		"-summary", summaryPath,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	report := readFile(t, reportPath)
	for _, want := range []string{
		"# Mongo Gateway Benchmark Comparison",
		"Largest TreeDB ops/sec lead: `load_insert_many`",
		"Largest MongoDB ops/sec lead: `id_find_one`",
		"| 100 | 1 | `treedb` | `load_insert_many` | 10000 | 5000 | 2.00x | 20.0 | 40.0 |",
		"| 100 | 1 | `treedb` | 1000 B | 10.0 B | 2.00 KiB | 20.5 B | 1.27 KiB | 1.46 KiB | 4.00 KiB | 41.0 B | 0.67x | 0.50x |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}

	summary := readFile(t, summaryPath)
	if !strings.Contains(summary, "load_insert_many\t10000.000000\t5000.000000\t2.000000") {
		t.Fatalf("summary missing load ratio:\n%s", summary)
	}
}

func TestReportRejectsIncompleteCell(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb.json"), `{
  "target": "treedb",
  "documents": 10,
  "secondary_indexes": 0,
  "phases": [],
  "treedb_disk_after_checkpoint": {"total_bytes": 100}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\t10\t0\ttreedb.json\t100\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete comparison cell") {
		t.Fatalf("expected incomplete cell error, got %v", err)
	}
}

func TestReportSupportsMultipleTreeDBConfigsPerMongoCell(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb_json.json"), `{
  "target": "treedb",
  "treedb_document_format": "json",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "load_insert_many", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_maintenance": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "treedb_bson.json"), `{
  "target": "treedb",
  "treedb_document_format": "bson",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "load_insert_many", "operations": 100, "ops_per_sec": 2000, "latency_micros": {}}],
  "treedb_disk_after_maintenance": {"total_bytes": 1500}
}`)
	writeFile(t, filepath.Join(dir, "mongo.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "load_insert_many", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	reportPath := filepath.Join(dir, "report.md")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_json\t100\t2\ttreedb_json.json\t2000\n"+
		"treedb\ttreedb_bson\t100\t2\ttreedb_bson.json\t1500\n"+
		"mongo\tmongo\t100\t2\tmongo.json\t5000\n")

	if err := run([]string{"-matrix", matrixPath, "-report", reportPath}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	report := readFile(t, reportPath)
	for _, want := range []string{
		"comparison cells: `2`",
		"| 100 | 2 | `treedb_bson` | 1.46 KiB",
		"| 100 | 2 | `treedb_json` | 1.95 KiB",
		"| 100 | 2 | `mongo` | mongo | `mongo.json` |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}
	if got := strings.Count(report, "| 100 | 2 | `mongo` | mongo | `mongo.json` |"); got != 1 {
		t.Fatalf("mongo raw input rows=%d want 1\n%s", got, report)
	}
}

func TestLargestDiskCellUsesDiskMetrics(t *testing.T) {
	cells := []cellComparison{
		{
			Key: cellKey{Documents: 1000, SecondaryIndexes: 0},
			TreeDB: &runRecord{
				Row: matrixRow{PhysicalBytes: 20_000},
				Result: benchmarkResult{
					TreeDBDiskAfterCheckpoint: &diskSnapshot{TotalBytes: 15_000},
				},
			},
			Mongo: &runRecord{
				Row:    matrixRow{PhysicalBytes: 10_000},
				Result: benchmarkResult{MongoDBStatsFinal: map[string]any{"totalSize": float64(10_000)}},
			},
		},
		{
			Key: cellKey{Documents: 10_000, SecondaryIndexes: 2},
			TreeDB: &runRecord{
				Row: matrixRow{PhysicalBytes: 100},
				Result: benchmarkResult{
					TreeDBDiskAfterCheckpoint: &diskSnapshot{TotalBytes: 100},
				},
			},
			Mongo: &runRecord{
				Row:    matrixRow{PhysicalBytes: 100},
				Result: benchmarkResult{MongoDBStatsFinal: map[string]any{"totalSize": float64(100)}},
			},
		},
	}
	got := largestDiskCell(cells)
	if got == nil || got.Key.Documents != 1000 {
		t.Fatalf("largestDiskCell chose %+v, want the higher-disk 1000-doc cell", got)
	}
}

func TestTreeDBBytesPrefersMaintenanceSnapshot(t *testing.T) {
	got, ok := treeDBBytes(benchmarkResult{
		TreeDBDiskAfterLoad:        &diskSnapshot{TotalBytes: 3_000},
		TreeDBDiskAfterCheckpoint:  &diskSnapshot{TotalBytes: 2_000},
		TreeDBDiskAfterMaintenance: &diskSnapshot{TotalBytes: 1_000},
	})
	if !ok || got != 1_000 {
		t.Fatalf("treeDBBytes=%d ok=%v want maintenance snapshot", got, ok)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
