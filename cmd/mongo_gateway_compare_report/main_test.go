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
    {"name": "load_insert_many", "operations": 100, "driver_calls": 1, "duration_ms": 10, "ops_per_sec": 10000, "sampled_ops_per_sec": 12500, "sampled_ns_per_op": 80, "latency_micros": {"p50": 10, "p95": 20, "p99": 30}},
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
    {"name": "load_insert_many", "operations": 100, "driver_calls": 1, "duration_ms": 20, "ops_per_sec": 5000, "sampled_ops_per_sec": 6250, "sampled_ns_per_op": 160, "latency_micros": {"p50": 20, "p95": 40, "p99": 50}},
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
		"| 100 | 1 | false | n/a | `treedb` | `load_insert_many` | 10000 | 12500 | 5000 | 6250 | 2.00x | 2.00x | 20.0 | 40.0 |",
		"| 100 | 1 | false | `treedb` | checkpoint | 1000 B | 10.0 B | 2.00 KiB | 20.5 B | 1.27 KiB | 1.46 KiB | 4.00 KiB | 41.0 B | 0.67x | 0.50x |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}

	summary := readFile(t, summaryPath)
	if !strings.Contains(summary, "load_insert_many\t10000.000000\t12500.000000\t80.000000\t5000.000000\t6250.000000\t160.000000\t2.000000\t2.000000") {
		t.Fatalf("summary missing load ratio:\n%s", summary)
	}
}

func TestReportShowsRangeIndexModeAndProfileDir(t *testing.T) {
	dir := t.TempDir()
	treedbPath := filepath.Join(dir, "treedb_range.json")
	mongoPath := filepath.Join(dir, "mongo_range.json")
	matrixPath := filepath.Join(dir, "matrix.tsv")
	reportPath := filepath.Join(dir, "report.md")
	summaryPath := filepath.Join(dir, "summary.tsv")

	writeFile(t, treedbPath, `{
  "target": "treedb",
  "database": "bench",
  "collection": "docs",
  "documents": 1000,
  "secondary_indexes": 0,
  "range_index": true,
  "profile_dir": "/tmp/range-profiles",
  "profile_manifest": "profile_manifest.json",
  "profile_result": "benchmark_result.json",
  "phases": [
    {"name": "age_range_indexed_limit_10", "operations": 100, "driver_calls": 100, "ops_per_sec": 3000, "sampled_ops_per_sec": 3100, "sampled_ns_per_op": 322000, "latency_micros": {"p50": 300, "p95": 400, "p99": 500}}
  ],
  "treedb_disk_after_load": {"total_bytes": 5000}
}`)
	writeFile(t, mongoPath, `{
  "target": "mongo",
  "database": "bench",
  "collection": "docs",
  "documents": 1000,
  "secondary_indexes": 0,
  "range_index": true,
  "phases": [
    {"name": "age_range_indexed_limit_10", "operations": 100, "driver_calls": 100, "ops_per_sec": 4000, "sampled_ops_per_sec": 4100, "sampled_ns_per_op": 244000, "latency_micros": {"p50": 200, "p95": 300, "p99": 450}}
  ],
  "mongodb_stats_final": {"dataSize": 10000, "totalSize": 20000}
}`)
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_template_v1_driver_range_index\t1000\t0\ttreedb_range.json\t6000\n"+
		"mongo\tmongo_range_index\t1000\t0\tmongo_range.json\t300000\n")

	if err := run([]string{
		"-matrix", matrixPath,
		"-report", reportPath,
		"-summary", summaryPath,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	report := readFile(t, reportPath)
	for _, want := range []string{
		"| 1000 | 0 | true | `indexed` | `treedb_template_v1_driver_range_index` | `age_range_indexed_limit_10` | 3000 | 3100 | 4000 | 4100 | 0.75x | 0.76x | 400 | 300 |",
		"| 1000 | 0 | true | `treedb_template_v1_driver_range_index` | treedb | `treedb_range.json` | `/tmp/range-profiles` |",
		"Range-query benchmark rows use explicit phase names",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}
	summary := readFile(t, summaryPath)
	if !strings.Contains(summary, "1000\t0\ttrue\tindexed\ttreedb_template_v1_driver_range_index\tage_range_indexed_limit_10") {
		t.Fatalf("summary missing indexed range mode:\n%s", summary)
	}
}

func TestRangeModeLabelsIndexedAndScanPhases(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "age_range_indexed_limit_10", want: "indexed"},
		{name: "age_range_scan_limit_10", want: "scan"},
		{name: "age_range_limit_10", want: "scan"},
		{name: "id_find_one", want: ""},
	} {
		if got := rangeMode(tc.name); got != tc.want {
			t.Fatalf("rangeMode(%q)=%q want %q", tc.name, got, tc.want)
		}
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

func TestReportRejectsDuplicateTreeDBRows(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb_a.json"), `{
  "target": "treedb",
  "documents": 10,
  "secondary_indexes": 0,
  "phases": [],
  "treedb_disk_after_checkpoint": {"total_bytes": 100}
}`)
	writeFile(t, filepath.Join(dir, "treedb_b.json"), `{
  "target": "treedb",
  "documents": 10,
  "secondary_indexes": 0,
  "phases": [],
  "treedb_disk_after_checkpoint": {"total_bytes": 120}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_writers_1\t10\t0\ttreedb_a.json\t100\n"+
		"treedb\ttreedb_bson_writers_1\t10\t0\ttreedb_b.json\t120\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
		"-allow-incomplete",
	})
	if err == nil || !strings.Contains(err.Error(), `duplicate treedb row`) {
		t.Fatalf("expected duplicate treedb row error, got %v", err)
	}
}

func TestReportAllowsIncompleteTreeDBOnlyCell(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb.json"), `{
  "target": "treedb",
  "documents": 10,
  "secondary_indexes": 0,
  "phases": [{"name": "concurrent_id_update_set_w4", "operations": 40, "ops_per_sec": 4000, "sampled_ops_per_sec": 5000, "sampled_ns_per_op": 200, "latency_micros": {"p50": 1, "p95": 2, "p99": 3}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 100}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	reportPath := filepath.Join(dir, "report.md")
	summaryPath := filepath.Join(dir, "summary.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_writers_4\t10\t0\ttreedb.json\t100\n")

	if err := run([]string{
		"-matrix", matrixPath,
		"-report", reportPath,
		"-summary", summaryPath,
		"-allow-incomplete",
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	report := readFile(t, reportPath)
	for _, want := range []string{
		"- targets: `treedb`",
		"No MongoDB baseline rows were present",
		"`treedb_bson_writers_4`",
		"`concurrent_id_update_set_w4`",
		"| 4000 | 5000 |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}
	if strings.Contains(report, "No phase in this matrix had MongoDB ahead on ops/sec.") {
		t.Fatalf("TreeDB-only report should not mention MongoDB lead fallback:\n%s", report)
	}
	summary := readFile(t, summaryPath)
	if !strings.Contains(summary, "concurrent_id_update_set_w4\t4000.000000\t5000.000000\t200.000000\t\t\t\t\t") {
		t.Fatalf("summary missing TreeDB-only row:\n%s", summary)
	}
}

func TestReportAllowsMixedIncompleteCells(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb_complete.json"), `{
  "target": "treedb",
  "documents": 10,
  "secondary_indexes": 0,
  "phases": [{"name": "load_insert_many", "operations": 10, "ops_per_sec": 1000, "sampled_ops_per_sec": 1100, "sampled_ns_per_op": 900, "latency_micros": {"p95": 2}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 100}
}`)
	writeFile(t, filepath.Join(dir, "mongo_complete.json"), `{
  "target": "mongo",
  "documents": 10,
  "secondary_indexes": 0,
  "phases": [{"name": "load_insert_many", "operations": 10, "ops_per_sec": 500, "sampled_ops_per_sec": 550, "sampled_ns_per_op": 1800, "latency_micros": {"p95": 4}}],
  "mongodb_stats_final": {"dataSize": 200, "totalSize": 300}
}`)
	writeFile(t, filepath.Join(dir, "treedb_only.json"), `{
  "target": "treedb",
  "documents": 20,
  "secondary_indexes": 1,
  "phases": [{"name": "concurrent_id_find_one_r2", "operations": 20, "ops_per_sec": 2000, "sampled_ops_per_sec": 2200, "sampled_ns_per_op": 450, "latency_micros": {"p95": 3}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 400}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	reportPath := filepath.Join(dir, "report.md")
	summaryPath := filepath.Join(dir, "summary.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_complete\t10\t0\ttreedb_complete.json\t100\n"+
		"mongo\tmongo\t10\t0\tmongo_complete.json\t300\n"+
		"treedb\ttreedb_only\t20\t1\ttreedb_only.json\t400\n")

	if err := run([]string{
		"-matrix", matrixPath,
		"-report", reportPath,
		"-summary", summaryPath,
		"-allow-incomplete",
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	report := readFile(t, reportPath)
	for _, want := range []string{
		"| 10 | 0 | false | n/a | `treedb_complete` | `load_insert_many` | 1000 | 1100 | 500 | 550 | 2.00x | 2.00x | 2.00 | 4.00 |",
		"| 20 | 1 | false | n/a | `treedb_only` | `concurrent_id_find_one_r2` | 2000 | 2200 | n/a | n/a | n/a | n/a | 3.00 | n/a |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}
	summary := readFile(t, summaryPath)
	if !strings.Contains(summary, "load_insert_many\t1000.000000\t1100.000000\t900.000000\t500.000000\t550.000000\t1800.000000\t2.000000\t2.000000") {
		t.Fatalf("summary missing complete comparison:\n%s", summary)
	}
	if !strings.Contains(summary, "concurrent_id_find_one_r2\t2000.000000\t2200.000000\t450.000000\t\t\t\t\t") {
		t.Fatalf("summary missing incomplete comparison:\n%s", summary)
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
		"| 100 | 2 | false | `treedb_bson` | maintenance | 1.46 KiB",
		"| 100 | 2 | false | `treedb_json` | maintenance | 1.95 KiB",
		"| 100 | 2 | false | `mongo` | mongo | `mongo.json` | n/a |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}
	if got := strings.Count(report, "| 100 | 2 | false | `mongo` | mongo | `mongo.json` | n/a |"); got != 1 {
		t.Fatalf("mongo raw input rows=%d want 1\n%s", got, report)
	}
}

func TestReportSupportsScalingMongoConfigsPerScenario(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb_w1.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 1000, "latency_micros": {"p95": 10}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "treedb_w2.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w2", "operations": 100, "ops_per_sec": 1500, "latency_micros": {"p95": 20}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2200}
}`)
	writeFile(t, filepath.Join(dir, "mongo_w1.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 500, "latency_micros": {"p95": 30}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	writeFile(t, filepath.Join(dir, "mongo_w2.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w2", "operations": 100, "ops_per_sec": 750, "latency_micros": {"p95": 40}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4100}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	reportPath := filepath.Join(dir, "report.md")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_1\t100\t2\ttreedb_w1.json\t2000\n"+
		"mongo\tmongo_writers_1\t100\t2\tmongo_w1.json\t4000\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_2\t100\t2\ttreedb_w2.json\t2200\n"+
		"mongo\tmongo_writers_2\t100\t2\tmongo_w2.json\t4100\n")

	if err := run([]string{"-matrix", matrixPath, "-report", reportPath}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	report := readFile(t, reportPath)
	for _, want := range []string{
		"| 100 | 2 | false | n/a | `treedb_bson_driver-command-raw_writers_1` | `concurrent_id_update_set_w1` | 1000 | n/a | 500 | n/a | 2.00x | n/a | 10.0 | 30.0 |",
		"| 100 | 2 | false | n/a | `treedb_bson_driver-command-raw_writers_2` | `concurrent_id_update_set_w2` | 1500 | n/a | 750 | n/a | 2.00x | n/a | 20.0 | 40.0 |",
		"| 100 | 2 | false | `mongo_writers_1` | mongo | `mongo_w1.json` | n/a |",
		"| 100 | 2 | false | `mongo_writers_2` | mongo | `mongo_w2.json` | n/a |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}
}

func TestReportRejectsLoneMismatchedMongoScenario(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb_w1.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "treedb_w2.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w2", "operations": 100, "ops_per_sec": 1500, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2200}
}`)
	writeFile(t, filepath.Join(dir, "mongo_w1.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_1\t100\t2\ttreedb_w1.json\t2000\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_2\t100\t2\ttreedb_w2.json\t2200\n"+
		"mongo\tmongo_writers_1\t100\t2\tmongo_w1.json\t4000\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
	})
	if err == nil ||
		!strings.Contains(err.Error(), `config="treedb_bson_driver-command-raw_writers_2"`) ||
		!strings.Contains(err.Error(), `tree_scenario="writers_2"`) ||
		!strings.Contains(err.Error(), `available_mongo_configs=[mongo_writers_1]`) {
		t.Fatalf("err=%v want missing writers_2 mongo scenario with context", err)
	}
}

func TestReportAllowIncompleteStillRejectsMismatchedMongoScenario(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb_w1.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "mongo.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_1\t100\t2\ttreedb_w1.json\t2000\n"+
		"mongo\tmongo\t100\t2\tmongo.json\t4000\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
		"-allow-incomplete",
	})
	if err == nil ||
		!strings.Contains(err.Error(), `config="treedb_bson_driver-command-raw_writers_1"`) ||
		!strings.Contains(err.Error(), `tree_scenario="writers_1"`) ||
		!strings.Contains(err.Error(), `available_mongo_configs=[mongo]`) {
		t.Fatalf("err=%v want mismatched mongo scenario despite allow-incomplete with context", err)
	}
}

func TestMatchingMongoRecordNoScalingMarkerErrorSaysSo(t *testing.T) {
	index := mongoScenarioIndex{
		exact: map[string]*runRecord{
			"mongo_writers_1": {Row: matrixRow{Config: "mongo_writers_1"}},
		},
		bySuffix: map[string][]*runRecord{
			"writers_1": {{Row: matrixRow{Config: "mongo_writers_1"}}},
		},
		suffixConfig: map[string][]string{
			"writers_1": {"mongo_writers_1"},
		},
	}
	_, err := matchingMongoRecord(baseCellKey{Documents: 100, SecondaryIndexes: 2}, "treedb_without_marker", index, false)
	if err == nil ||
		!strings.Contains(err.Error(), `config="treedb_without_marker"`) ||
		!strings.Contains(err.Error(), `tree_scenario="no scaling marker present"`) {
		t.Fatalf("err=%v want explicit no scaling marker context", err)
	}
}

func TestScalingScenarioSuffixRequiresTerminalCount(t *testing.T) {
	for _, tc := range []struct {
		config string
		want   string
		valid  bool
		marker bool
	}{
		{config: "treedb_writers_4", want: "writers_4", valid: true, marker: true},
		{config: "treedb_readers_16", want: "readers_16", valid: true, marker: true},
		{config: "treedb_writers_4_readers_16", want: "readers_16", valid: true, marker: true},
		{config: "treedb_readers_16_writers_4", want: "writers_4", valid: true, marker: true},
		{config: "treedb_writers_4_extra", want: "", valid: false, marker: true},
		{config: "treedb_writers_", want: "", valid: false, marker: true},
		{config: "treedb_without_marker", want: "", valid: true, marker: false},
	} {
		if got := scalingScenarioSuffix(tc.config); got != tc.want {
			t.Fatalf("scalingScenarioSuffix(%q)=%q want %q", tc.config, got, tc.want)
		}
		got := parseScalingScenario(tc.config)
		if got.valid != tc.valid || got.hasMarker != tc.marker {
			t.Fatalf("parseScalingScenario(%q)=%+v want valid=%v marker=%v", tc.config, got, tc.valid, tc.marker)
		}
	}
}

func TestReportRejectsInvalidScalingScenarioWithUnsuffixedMongo(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "mongo.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_1_extra\t100\t2\ttreedb.json\t2000\n"+
		"mongo\tmongo\t100\t2\tmongo.json\t4000\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
	})
	if err == nil || !strings.Contains(err.Error(), "invalid scaling scenario suffix") {
		t.Fatalf("err=%v want invalid scaling scenario suffix", err)
	}
}

func TestReportRejectsInvalidScalingScenarioMongoConfig(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "mongo.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_1\t100\t2\ttreedb.json\t2000\n"+
		"mongo\tmongo_writers_bad\t100\t2\tmongo.json\t4000\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
	})
	if err == nil ||
		!strings.Contains(err.Error(), "invalid scaling scenario suffix") ||
		!strings.Contains(err.Error(), `mongo config="mongo_writers_bad"`) {
		t.Fatalf("err=%v want invalid mongo scaling scenario suffix", err)
	}
}

func TestReportRejectsLoneMongoScenarioForUnsuffixedTreeConfig(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "mongo_w1.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw\t100\t2\ttreedb.json\t2000\n"+
		"mongo\tmongo_writers_1\t100\t2\tmongo_w1.json\t4000\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
	})
	if err == nil || !strings.Contains(err.Error(), `config="treedb_bson_driver-command-raw"`) {
		t.Fatalf("err=%v want missing unsuffixed mongo baseline", err)
	}
}

func TestReportMatchesUnsuffixedTreeConfigWithMixedMongoRows(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "mongo.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "insert", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	writeFile(t, filepath.Join(dir, "mongo_w1.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 600, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4100}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	reportPath := filepath.Join(dir, "report.md")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw\t100\t2\ttreedb.json\t2000\n"+
		"mongo\tmongo\t100\t2\tmongo.json\t4000\n"+
		"mongo\tmongo_writers_1\t100\t2\tmongo_w1.json\t4100\n")

	if err := run([]string{"-matrix", matrixPath, "-report", reportPath}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	report := readFile(t, reportPath)
	if !strings.Contains(report, "| 100 | 2 | false | n/a | `treedb_bson_driver-command-raw` | `insert` | 1000 | n/a | 500 | n/a | 2.00x | n/a |") {
		t.Fatalf("report missing unsuffixed comparison\n%s", report)
	}
}

func TestReportRejectsAmbiguousScalingMongoConfigsPerScenario(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "treedb_w1.json"), `{
  "target": "treedb",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 1000, "latency_micros": {}}],
  "treedb_disk_after_checkpoint": {"total_bytes": 2000}
}`)
	writeFile(t, filepath.Join(dir, "mongo_w1_a.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 500, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3000, "totalSize": 4000}
}`)
	writeFile(t, filepath.Join(dir, "mongo_w1_b.json"), `{
  "target": "mongo",
  "documents": 100,
  "secondary_indexes": 2,
  "phases": [{"name": "concurrent_id_update_set_w1", "operations": 100, "ops_per_sec": 550, "latency_micros": {}}],
  "mongodb_stats_final": {"dataSize": 3100, "totalSize": 4100}
}`)
	matrixPath := filepath.Join(dir, "matrix.tsv")
	writeFile(t, matrixPath, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver-command-raw_writers_1\t100\t2\ttreedb_w1.json\t2000\n"+
		"mongo\tmongo_a_writers_1\t100\t2\tmongo_w1_a.json\t4000\n"+
		"mongo\tmongo_b_writers_1\t100\t2\tmongo_w1_b.json\t4100\n")

	err := run([]string{
		"-matrix", matrixPath,
		"-report", filepath.Join(dir, "report.md"),
	})
	if err == nil || !strings.Contains(err.Error(), "ambiguous mongo rows") {
		t.Fatalf("err=%v want ambiguous mongo rows", err)
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
	got, label, ok := treeDBBytesSnapshot(benchmarkResult{
		TreeDBDiskAfterCheckpoint: &diskSnapshot{TotalBytes: 2_000},
	})
	if !ok || got != 2_000 || label != "checkpoint" {
		t.Fatalf("treeDBBytesSnapshot=%d %q ok=%v want checkpoint", got, label, ok)
	}
	got, label, ok = treeDBBytesSnapshot(benchmarkResult{})
	if ok || got != 0 || label != "n/a" {
		t.Fatalf("treeDBBytesSnapshot empty=%d %q ok=%v want n/a", got, label, ok)
	}
}

func TestMissingTreeDBDiskSnapshotRendersNA(t *testing.T) {
	phase := phaseResult{Name: "load_insert_many", OpsPerSecond: 100}
	cells := []cellComparison{{
		Key: cellKey{Documents: 100, SecondaryIndexes: 1},
		TreeDB: &runRecord{
			Result:   benchmarkResult{Phases: []phaseResult{phase}},
			PhaseMap: map[string]phaseResult{phase.Name: phase},
		},
		Mongo: &runRecord{
			Result: benchmarkResult{
				Phases:            []phaseResult{phase},
				MongoDBStatsFinal: map[string]any{"dataSize": float64(1300), "totalSize": float64(1500)},
			},
			PhaseMap: map[string]phaseResult{phase.Name: phase},
		},
	}}

	lines := strings.Join(highlightLines(cells), "\n")
	if !strings.Contains(lines, "TreeDB disk snapshot was n/a") || strings.Contains(lines, "used 0 B") {
		t.Fatalf("highlight rendered missing TreeDB disk snapshot incorrectly:\n%s", lines)
	}

	var table strings.Builder
	renderDiskTable(&table, cells)
	row := "| 100 | 1 | false | `` | n/a | n/a | n/a | n/a | n/a | 1.27 KiB | 1.46 KiB | n/a | n/a | n/a | n/a |"
	if !strings.Contains(table.String(), row) {
		t.Fatalf("disk table missing n/a row %q:\n%s", row, table.String())
	}

	summaryPath := filepath.Join(t.TempDir(), "summary.tsv")
	if err := writeSummaryTSV(summaryPath, cells); err != nil {
		t.Fatalf("write summary: %v", err)
	}
	summary := readFile(t, summaryPath)
	if !strings.Contains(summary, "\tn/a\t\t0\t1300\t1500\t0\t\t") {
		t.Fatalf("summary TSV should leave missing TreeDB disk bytes and ratio blank:\n%s", summary)
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
