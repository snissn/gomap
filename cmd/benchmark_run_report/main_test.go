package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDeepReportFromRunRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "HEAD.txt"), "HEAD=abc123def456\norigin/main=999888777666\n")
	writeFile(t, filepath.Join(root, "raw_engine_full_matrix", "wal_on_fast_checkpoint_between_tests", "benchprof_results.json"), `{
  "runs": [{
    "profile": "wal_on_fast",
    "results": {
      "sequential_write": {"TreeDB": 1000},
      "random_read": {"TreeDB": 2000}
    }
  }]
}`)
	writeFile(t, filepath.Join(root, "raw_engine_full_matrix", "wal_on_fast_no_checkpoint_between_tests", "benchprof_results.json"), `{
  "runs": [{
    "profile": "wal_on_fast",
    "results": {
      "sequential_write": {"TreeDB": 1100},
      "random_read": {"TreeDB": 2100}
    }
  }]
}`)
	writeFile(t, filepath.Join(root, "raw_engine_full_matrix", "fast_checkpoint_between_tests", "benchprof_results.json"), `{
  "runs": [{
    "profile": "fast",
    "results": {
      "sequential_write": {"TreeDB": 1200},
      "random_read": {"TreeDB": 2200}
    }
  }]
}`)
	writeFile(t, filepath.Join(root, "raw_engine_full_matrix", "fast_no_checkpoint_between_tests", "benchprof_results.json"), `{
  "runs": [{
    "profile": "fast",
    "results": {
      "sequential_write": {"TreeDB": 1300},
      "random_read": {"TreeDB": 2300}
    }
  }]
}`)
	writeFile(t, filepath.Join(root, "raw_engine_full_matrix", "wal_on_fast_checkpoint_between_tests", "unified-bench.log"), "Disk Usage (End of Run)\nTreeDB:\n  maindb/index.db: 1 MiB\n  maindb/leaf_vlog: total=2 MiB files=1 value=2 MiB other=0 B\n")
	writeFile(t, filepath.Join(root, "collections_sqlite_canonical_1m", "indexes_0", "benchmark_results.json"), `{
  "results": [
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":1000,"bytes_per_doc":10,"docs_per_sec":10000,"measurement_kind":"go_benchmark"},
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":500,"bytes_per_doc":5,"measurement_kind":"fixture"},
    {"config_name":"treedb_bson_collection_0_indexes","engine":"treedb_fast","format":"bson","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":800,"bytes_per_doc":8,"docs_per_sec":20000,"measurement_kind":"go_benchmark"},
    {"config_name":"treedb_bson_collection_0_indexes","engine":"treedb_fast","format":"bson","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":400,"bytes_per_doc":4,"measurement_kind":"fixture"},
    {"config_name":"treedb_json_collection_0_indexes","engine":"treedb_fast","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":1200,"bytes_per_doc":12,"docs_per_sec":8000,"measurement_kind":"go_benchmark"},
    {"config_name":"treedb_json_collection_0_indexes","engine":"treedb_fast","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":600,"bytes_per_doc":6,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_0_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture"},
    {"config_name":"sqlite_json_0_indexes","engine":"sqlite_wal_normal","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":3000,"bytes_per_doc":30,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"tree_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"full_leafgen_pack_gc","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":5,"sqlite_bytes_per_doc":20,"smaller_ratio":4,"comparison_basis":"test"}
  ]
}`)
	summary0 := mongoSummaryFixture(0)
	summary4 := mongoSummaryFixture(4)
	writeFile(t, filepath.Join(root, "mongo_gateway_full_sweep_1m_expanded", "summary.tsv"), summary0+strings.TrimPrefix(summary4, mongoSummaryHeader))
	writeFile(t, filepath.Join(root, "mongo_gateway_reader_writer_scaling_1m", "indexes_0", "summary.tsv"), summary0)
	writeFile(t, filepath.Join(root, "mongo_gateway_reader_writer_scaling_1m", "indexes_4", "summary.tsv"), summary4)
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "matrix.tsv"), "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver\t100\t0\traw/treedb.json\t5000000000\n"+
		"treedb\ttreedb_bson_raw_wire_tcp\t100\t0\traw/treedb_raw_wire_tcp.json\t3000\n"+
		"treedb\ttreedb_bson_raw_wire\t100\t0\traw/treedb_raw_wire.json\t2500\n"+
		"mongo\tmongo_driver\t100\t0\traw/mongo.json\t5000\n")
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "treedb.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"phases":[{"name":"load_insert_many","ops_per_sec":1000}]}`)
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "treedb_raw_wire_tcp.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"phases":[{"name":"load_insert_many","ops_per_sec":1500}]}`)
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "treedb_raw_wire.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"phases":[{"name":"load_insert_many","ops_per_sec":1800}]}`)
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "mongo.json"), `{"target":"mongo","documents":100,"secondary_indexes":0,"phases":[{"name":"load_insert_many","ops_per_sec":500}]}`)

	out := filepath.Join(root, "deep_report.html")
	if err := run([]string{"-run-root", root, "-out", out, "-title", "test report"}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	html := readFile(t, out)
	for _, want := range []string{
		"test report",
		"HEAD=abc123def456",
		"origin/main=999888777666",
		"Mongo Gateway Full Sweep",
		"Load-Only Client-Mode Matrix",
		"Dedicated Reader/Writer Scaling",
		"Collections vs SQLite",
		"Raw TreeDB Engine",
		"<svg",
		"All full-sweep rows",
		"sequential_write",
		"Sequential</text>",
		"Write</text>",
		"WAL On Fast / Checkpoint Between Tests",
		"Fast / No Checkpoint Between Tests",
		"Secondary Indexes",
		"Reader Count",
		"Load interpretation:",
		"pure ingest client-mode matrix",
		"4 indexes: phase detail",
		"All scaling rows for 4 indexes",
		"TreeDB BSON",
		"TreeDB JSON",
		"SQLite native VACUUM: 20",
		"Client modes marked with <strong>*</strong>",
		"BSON Raw</text><text",
		"Wire TCP *</text>",
		"Wire *</text>",
		"* Raw-wire modes:",
		"bypassing the MongoDB Go driver",
		"same raw command/gateway path in process",
		"one centered TreeDB bar",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("report missing %q\n%s", want, html)
		}
	}
	for _, unwanted := range []string{
		"TreeDB Raw Wire Ceiling Modes",
		"BSON raw_wire_tcp MongoDB",
		"BSON raw_wire MongoDB",
	} {
		if strings.Contains(html, unwanted) {
			t.Fatalf("report still contains unwanted split/missing raw-wire marker %q\n%s", unwanted, html)
		}
	}
}

func TestRenderHTMLNavOmitsMissingSections(t *testing.T) {
	html := renderHTML(reportData{
		Config: config{Title: "partial", RunRoot: t.TempDir()},
		MongoLoadModes: []loadModeRow{
			{Indexes: 0, Target: "treedb", Config: "treedb_bson_driver", OpsPerSec: 1000},
		},
	})
	if !strings.Contains(html, "href=\"#mongo-load\"") {
		t.Fatalf("nav missing present load-mode section\n%s", html)
	}
	for _, absent := range []string{"href=\"#mongo-full\"", "href=\"#scaling\"", "href=\"#collections\"", "href=\"#raw-engine\""} {
		if strings.Contains(html, absent) {
			t.Fatalf("nav includes missing section %s\n%s", absent, html)
		}
	}
}

func TestRenderCollectionsWithOnlyComparisons(t *testing.T) {
	html := renderHTML(reportData{
		Config: config{Title: "comparison only", RunRoot: t.TempDir()},
		CollectionComparisons: []collectionComparison{{
			TreeDBConfig:      "treedb_bson_collection_0_indexes",
			TreeDBPhase:       "full_leafgen_pack_gc",
			SQLiteConfig:      "sqlite_native_columns_0_indexes",
			SQLitePhase:       "sqlite_vacuum",
			TreeDBBytesPerDoc: 4,
			SQLiteBytesPerDoc: 20,
			SmallerRatio:      5,
			ComparisonBasis:   "fixture",
		}},
	})
	for _, want := range []string{"href=\"#collections\"", "<section id=\"collections\"", "Compacted-state comparisons", "treedb_bson_collection_0_indexes"} {
		if !strings.Contains(html, want) {
			t.Fatalf("comparison-only collection report missing %q\n%s", want, html)
		}
	}
	if strings.Contains(html, "Collection highlight rows") {
		t.Fatalf("comparison-only collection report renders row-only highlights\n%s", html)
	}
}

func TestRawEngineChartOmitsMissingVariants(t *testing.T) {
	html := renderHTML(reportData{
		Config: config{Title: "partial raw", RunRoot: t.TempDir()},
		RawEngine: []rawEngineRun{
			{
				Profile:    "wal_on_fast",
				Checkpoint: "checkpoint between tests",
				Results:    map[string]float64{"sequential_write": 1000},
			},
		},
	})
	if strings.Contains(html, "fast / no checkpoint between tests") {
		t.Fatalf("chart legend includes an absent raw-engine variant\n%s", html)
	}
	if strings.Contains(html, ": 0 ops/sec") {
		t.Fatalf("chart renders a missing raw-engine cell as zero throughput\n%s", html)
	}
}

func TestReadMatrixPreservesLargePhysicalBytes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "matrix.tsv")
	writeFile(t, path, " target \t config \t documents \t secondary_indexes \t raw_json \t physical_bytes \n"+
		" treedb \t treedb_bson_driver \t 100 \t 0 \t raw/treedb.json \t 5000000000 \n")

	rows, err := readMatrix(path)
	if err != nil {
		t.Fatalf("readMatrix failed: %v", err)
	}
	if got, want := rows[0].PhysicalBytes, int64(5_000_000_000); got != want {
		t.Fatalf("PhysicalBytes = %d, want %d", got, want)
	}
}

func TestReadMatrixRejectsInvalidNumbers(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "matrix.tsv")
	writeFile(t, path, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver\t100\t0\traw/treedb.json\tnope\n")

	_, err := readMatrix(path)
	if err == nil || !strings.Contains(err.Error(), "physical_bytes") {
		t.Fatalf("readMatrix error = %v, want physical_bytes parse failure", err)
	}
}

func TestReadMongoSummaryStrictParsing(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "summary.tsv")
	paddedHeader := " " + strings.ReplaceAll(strings.TrimSuffix(mongoSummaryHeader, "\n"), "\t", " \t ") + " \n"
	writeFile(t, path, paddedHeader+strings.TrimPrefix(mongoSummaryFixture(0), mongoSummaryHeader))

	rows, err := readMongoSummary(path)
	if err != nil {
		t.Fatalf("readMongoSummary failed: %v", err)
	}
	if got, want := rows[0].Documents, 100; got != want {
		t.Fatalf("Documents = %d, want %d", got, want)
	}

	writeFile(t, path, strings.Replace(mongoSummaryFixture(0), "100\t0\t", "oops\t0\t", 1))
	_, err = readMongoSummary(path)
	if err == nil || !strings.Contains(err.Error(), "documents") {
		t.Fatalf("readMongoSummary error = %v, want documents parse failure", err)
	}
}

func TestMongoSweepCountsObservedOnly(t *testing.T) {
	rows := []mongoSummaryRow{
		{SecondaryIndexes: 0, Phase: "concurrent_id_find_one_r16", TreeDBOpsSec: 160, MongoOpsSec: 80},
		{SecondaryIndexes: 0, Phase: "concurrent_id_find_one_r1", TreeDBOpsSec: 10, MongoOpsSec: 5},
		{SecondaryIndexes: 0, Phase: "concurrent_id_update_set_w8", TreeDBOpsSec: 800, MongoOpsSec: 400},
	}
	counts := mongoSweepCounts(rows, 0, "concurrent_id_find_one_r")
	if want := []int{1, 16}; !reflect.DeepEqual(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}
	values := mongoSweep(rows, 0, "concurrent_id_find_one_r", "tree", counts)
	if want := []float64{10, 160}; !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestLoadMongoLoadModesBestEffort(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "matrix.tsv"), "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver\t100\t0\traw/good.json\t2000\n"+
		"mongo\tmongo_driver\t100\t0\traw/missing.json\t5000\n")
	writeFile(t, filepath.Join(root, "raw", "good.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"phases":[{"name":"load_insert_many","ops_per_sec":1000}]}`)

	rows, warnings, err := loadMongoLoadModes(root)
	if err != nil {
		t.Fatalf("loadMongoLoadModes failed: %v", err)
	}
	if got, want := len(rows), 1; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if got, want := len(warnings), 1; got != want {
		t.Fatalf("warnings = %d, want %d", got, want)
	}
	if !strings.Contains(warnings[0], "raw/missing.json") {
		t.Fatalf("warning %q does not name missing raw JSON", warnings[0])
	}
}

func TestParseConfigRejectsInvalidRunRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := parseConfig([]string{"-run-root", missing}); err == nil {
		t.Fatal("parseConfig accepted a missing run root")
	}

	file := filepath.Join(t.TempDir(), "not-a-dir")
	writeFile(t, file, "not a directory")
	if _, err := parseConfig([]string{"-run-root", file}); err == nil {
		t.Fatal("parseConfig accepted a file as run root")
	}
}

const mongoSummaryHeader = "documents\tsecondary_indexes\trange_index\trange_mode\ttreedb_config\tmongo_config\tphase\ttreedb_ops_sec\ttreedb_sampled_ops_sec\ttreedb_sampled_ns_per_op\tmongo_ops_sec\tmongo_sampled_ops_sec\tmongo_sampled_ns_per_op\ttreedb_to_mongo_ops_ratio\ttreedb_to_mongo_sampled_ops_ratio\ttreedb_p50_us\tmongo_p50_us\ttreedb_p95_us\tmongo_p95_us\ttreedb_p99_us\tmongo_p99_us\ttreedb_disk_snapshot\ttreedb_disk_bytes\ttreedb_physical_bytes\tmongo_dbstats_data_size_bytes\tmongo_dbstats_total_size_bytes\tmongo_physical_bytes\ttreedb_to_mongo_dbstats_total_ratio\ttreedb_to_mongo_physical_ratio\n"

func mongoSummaryFixture(indexes int) string {
	return mongoSummaryHeader +
		fmt.Sprintf("100\t%d\tfalse\t\ttreedb_bson\tmongo\tload_insert_many\t1000\t1000\t1000\t500\t500\t2000\t2\t2\t1\t2\t3\t4\t5\t6\tmaintenance\t1000\t2000\t3000\t4000\t5000\t0.25\t0.4\n", indexes) +
		fmt.Sprintf("100\t%d\tfalse\t\ttreedb_bson\tmongo\tconcurrent_id_find_one_r1\t2000\t2000\t500\t1000\t1000\t1000\t2\t2\t1\t2\t3\t4\t5\t6\tmaintenance\t1000\t2000\t3000\t4000\t5000\t0.25\t0.4\n", indexes)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
