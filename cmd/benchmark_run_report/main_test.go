package main

import (
	"fmt"
	"os"
	"path/filepath"
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
		"mongo\tmongo_driver\t100\t0\traw/mongo.json\t5000\n")
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "treedb.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"phases":[{"name":"load_insert_many","ops_per_sec":1000}]}`)
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
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("report missing %q\n%s", want, html)
		}
	}
}

func TestReadMatrixPreservesLargePhysicalBytes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "matrix.tsv")
	writeFile(t, path, "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver\t100\t0\traw/treedb.json\t5000000000\n")

	rows, err := readMatrix(path)
	if err != nil {
		t.Fatalf("readMatrix failed: %v", err)
	}
	if got, want := rows[0].PhysicalBytes, int64(5_000_000_000); got != want {
		t.Fatalf("PhysicalBytes = %d, want %d", got, want)
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
