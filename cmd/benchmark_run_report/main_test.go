package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestDeepReportFromRunRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "HEAD.txt"), "HEAD=abc123def456\norigin/main=999888777666\n")
	writeFile(t, filepath.Join(root, "commands.log"), "command: go test ./cmd/benchmark_run_report\nexit_status: 0 duration_sec: 2\ncommand: scripts/bench_collections_canonical.sh --smoke\nexit_status: 1 duration_sec: 5\nwarning: collections failed with exit status 1; continuing so the final report can render\n")
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
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":1000,"bytes_per_doc":10,"docs_per_sec":10000,"measurement_kind":"go_benchmark","production_evidence":{"producer_route":"command_wal_publish","producer_route_candidate_ops":100,"producer_route_used_ops":100,"producer_route_fallbacks":0,"storage_policy":"data_outer=true,index_outer=true","gomaxprocs":16,"physical_cores":8,"flush_admission_effective_concurrency":8,"flush_admission_admitted":true,"flush_admission_span_native":true,"flush_admission_backlog_coalescing":true,"flush_span_fallbacks":0,"ordered_root_span_fallbacks":0}},
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"offline_rewrite","maintenance_mode":"offline_rewrite","total_bytes":700,"bytes_per_doc":7,"measurement_kind":"fixture","extra":{"index_db_bytes":"400","leaf_vlog_bytes":"300"}},
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"exhaustive_compact","maintenance_mode":"exhaustive_compact","total_bytes":450,"bytes_per_doc":4.5,"measurement_kind":"fixture"},
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":500,"bytes_per_doc":5,"measurement_kind":"fixture"},
    {"config_name":"treedb_bson_collection_0_indexes","engine":"treedb_fast","format":"bson","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":800,"bytes_per_doc":8,"docs_per_sec":20000,"measurement_kind":"go_benchmark"},
    {"config_name":"treedb_bson_collection_0_indexes","engine":"treedb_fast","format":"bson","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":400,"bytes_per_doc":4,"measurement_kind":"fixture"},
    {"config_name":"treedb_json_collection_0_indexes","engine":"treedb_fast","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":1200,"bytes_per_doc":12,"docs_per_sec":8000,"measurement_kind":"go_benchmark"},
    {"config_name":"treedb_json_collection_0_indexes","engine":"treedb_fast","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":600,"bytes_per_doc":6,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_0_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture","maintenance_stats":{"sqlite_vacuum_ops_per_sec":123},"extra":{"sqlite_vacuum_bytes_before":"2500","sqlite_vacuum_bytes_after":"2000","sqlite_vacuum_bytes_delta":"500"}},
    {"config_name":"sqlite_json_0_indexes","engine":"sqlite_wal_normal","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":3000,"bytes_per_doc":30,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"tree_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"exhaustive_compact","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":4.5,"sqlite_bytes_per_doc":20,"smaller_ratio":4.4444444444,"comparison_basis":"test"}
  ]
}`)
	writeFile(t, filepath.Join(root, "raw_engine_full_matrix", "wal_on_fast_checkpoint_between_tests", "insights.json"), `{
  "profiles_dir": "raw_engine_full_matrix/wal_on_fast_checkpoint_between_tests",
  "ops_source": "benchprof_results.json",
  "insights": ["random_read: decode/read I/O hotspot"],
  "cpu_profiles": [{}],
  "alloc_space_profiles": [{}],
  "investigation_targets": [{"db_tag":"treedb","test":"random_read","category":"decode/read I/O","function":"TreeDB/db.Get","flat_pct":12.5,"file":"TreeDB/db/db.go","line":123,"why":"hot read path"}]
}`)
	writeFile(t, filepath.Join(root, "collections_sqlite_canonical_1m", "indexes_0", "timed_matrix", "treedb_json", "profiles", "collection_profile_manifest.json"), `{
  "profile_dir": "profiles/treedb_json",
  "cell": "treedb_json",
  "execution_path": "native-fastpath",
  "engine": "production_fast",
  "document_format": "json",
  "storage_policy": "data_outer=true,index_outer=true",
  "benchmark_pattern": "^BenchmarkCollectionShapeInsertBatch$",
  "benchtime": "100x",
  "count": 1,
  "duration_ms": 42.5,
  "artifacts": [{"phase":"benchmark_cell","cpu_profile":"cpu.pprof","allocs_profile":"allocs.pprof","block_profile":"block.pprof","mutex_profile":"mutex.pprof","output":"profile_go_test.txt"}]
}`)
	writeFile(t, filepath.Join(root, "mongo_gateway_full_sweep_1m_expanded", "profiles", "treedb_cell", "profile_manifest.json"), `{
  "profile_dir": "profiles/treedb_cell",
  "artifacts": [{"phase":"load_insert_many","prefix":"load_insert_many","duration_ms":12.3,"cpu_profile":"load_insert_many.cpu.pprof","allocs_profile":"load_insert_many.allocs.pprof"}]
}`)
	writeFile(t, filepath.Join(root, "mongo_gateway_full_sweep_1m_expanded", "profiles", "treedb_cell", "benchmark_result.json"), `{
  "target":"treedb",
  "documents":100,
  "secondary_indexes":0,
  "client_mode":"driver-command-raw",
  "treedb_document_format":"bson",
  "treedb_profile":"wal_on_fast",
  "phases":[{"name":"load_insert_many","ops_per_sec":12345,"latency_micros":{"p95":99}}]
}`)
	summary0 := mongoSummaryFixture(0)
	summary4 := mongoSummaryFixture(4)
	writeFile(t, filepath.Join(root, "mongo_gateway_full_sweep_1m_expanded", "summary.tsv"), summary0+strings.TrimPrefix(summary4, mongoSummaryHeader))
	writeFile(t, filepath.Join(root, "mongo_gateway_load_scaling_1m", "summary.tsv"), mongoLoadScalingSummaryFixture())
	writeFile(t, filepath.Join(root, "mongo_gateway_reader_writer_scaling_1m", "indexes_0", "summary.tsv"), summary0)
	writeFile(t, filepath.Join(root, "mongo_gateway_reader_writer_scaling_1m", "indexes_4", "summary.tsv"), summary4)
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "matrix.tsv"), "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver\t100\t0\traw/treedb.json\t5000000000\n"+
		"treedb\ttreedb_bson_direct\t100\t0\traw/treedb_direct.json\t2800\n"+
		"treedb\ttreedb_json_direct\t100\t0\traw/treedb_json_direct.json\t2900\n"+
		"treedb\ttreedb_bson_raw_wire_tcp\t100\t0\traw/treedb_raw_wire_tcp.json\t3000\n"+
		"treedb\ttreedb_bson_raw_wire\t100\t0\traw/treedb_raw_wire.json\t2500\n"+
		"mongo\tmongo_driver\t100\t0\traw/mongo.json\t5000\n")
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "treedb.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"phases":[{"name":"load_insert_many","ops_per_sec":1000}]}`)
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "treedb_direct.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"client_mode":"direct","phases":[{"name":"load_insert_many","ops_per_sec":1700}]}`)
	writeFile(t, filepath.Join(root, "mongo_client_mode_load_matrix_1m", "raw", "treedb_json_direct.json"), `{"target":"treedb","documents":100,"secondary_indexes":0,"client_mode":"direct","treedb_document_format":"json","phases":[{"name":"load_insert_many","ops_per_sec":1750}]}`)
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
		"Run Status",
		"Partial run: 1 of 2 recorded commands exited nonzero.",
		"collections failed with exit status 1",
		"Mongo API Full Sweep",
		"Mongo API InsertMany Producer Scaling",
		"Load-Only Client-Mode Matrix",
		"Mongo API Reader/Writer Scaling",
		"Collections vs SQLite",
		"Raw TreeDB Engine",
		"Profiling Follow-Up",
		"<svg",
		"All raw full-sweep TSV rows",
		"sequential_write",
		"Sequential</text>",
		"Write</text>",
		"WAL On Fast / Checkpoint Between Tests",
		"Fast / No Checkpoint Between Tests",
		"Secondary Indexes",
		"Client Count",
		"Load interpretation:",
		"pure ingest client-mode matrix",
		"Index throughput retention",
		"Producer scaling summary",
		"Insert Throughput Vs Insert Producers, 0 Indexes",
		"requested producers",
		"effective producers",
		"4 Indexes: Mongo API Scaling",
		"Single threaded client",
		"ID Find One: Throughput Vs Reader Clients",
		"Throughput summary",
		"largest TreeDB/MongoDB",
		"Fresh 100-document load per cell",
		"Raw full-sweep TSV rows for 4 indexes",
		"Raw scaling TSV rows, 4 indexes",
		"TreeDB BSON",
		"TreeDB JSON",
		"SQLite native VACUUM",
		"Collection summary",
		"storage lifecycle",
		"Compacted Size: SQLite Bytes/Doc Divided By TreeDB Bytes/Doc",
		"Maintenance Evidence",
		"Raw Engine Benchprof Insights",
		"Collections vs SQLite Profile Manifests",
		"Artifact filenames are relative to the profile directory",
		"profile dir",
		"Mongo Gateway Profile Manifests",
		"decode/read I/O hotspot",
		"cpu.pprof",
		"profile_go_test.txt",
		"load_insert_many.cpu.pprof",
		"Client modes marked with <strong>*</strong>",
		"Raw load-mode rows",
		"BSON Direct</text><text",
		"BSON JSON</text><text",
		"BSON Raw</text><text",
		"Wire TCP *</text>",
		"Wire *</text>",
		"* TreeDB-only modes:",
		"calls the collection API directly",
		"bypassing the MongoDB Go driver",
		"same raw command/gateway path in process",
		"one centered TreeDB bar",
		"Physical Storage By Client Mode",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("report missing %q\n%s", want, html)
		}
	}
	for _, unwanted := range []string{
		"TreeDB Raw Wire Ceiling Modes",
		"BSON direct MongoDB",
		"BSON json_direct MongoDB",
		"BSON raw_wire_tcp MongoDB",
		"BSON raw_wire MongoDB",
	} {
		if strings.Contains(html, unwanted) {
			t.Fatalf("report still contains unwanted split/missing raw-wire marker %q\n%s", unwanted, html)
		}
	}
	if strings.Contains(html, "<details open><summary>Raw ") {
		t.Fatalf("raw tables should be hidden by default\n%s", html)
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
	for _, absent := range []string{"href=\"#run-status\"", "href=\"#mongo-full\"", "href=\"#mongo-load-scaling\"", "href=\"#scaling\"", "href=\"#collections\"", "href=\"#raw-engine\""} {
		if strings.Contains(html, absent) {
			t.Fatalf("nav includes missing section %s\n%s", absent, html)
		}
	}
}

func TestLoadCommandLogAndRenderRunStatus(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "commands.log")
	writeFile(t, path, "command: go test ./...\nexit_status: 0 duration_sec: 12\ncommand: scripts/fail.sh\nexit_status: 2 duration_sec: 3\nwarning: fail.sh failed with exit status 2; continuing so the final report can render\n")

	commands, warnings := loadCommandLog(path)
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none", warnings)
	}
	if len(commands) != 2 {
		t.Fatalf("commands = %v, want 2", commands)
	}
	if commands[1].ExitStatus != 2 || commands[1].DurationSec != 3 || !strings.Contains(commands[1].Warning, "exit status 2") {
		t.Fatalf("failed command parsed incorrectly: %+v", commands[1])
	}
	if !commands[0].Complete || !commands[1].Complete {
		t.Fatalf("completed commands parsed as incomplete: %+v", commands)
	}

	var b strings.Builder
	renderRunStatus(&b, commands, nil)
	html := b.String()
	for _, want := range []string{
		"Run Status",
		"Partial run: 1 of 2 recorded commands exited nonzero.",
		"scripts/fail.sh",
		"fail.sh failed with exit status 2",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("run status missing %q\n%s", want, html)
		}
	}
}

func TestLoadCommandLogSkipsTrailingCommandWithoutExit(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "commands.log")
	writeFile(t, path, "command: go test ./cmd/benchmark_run_report\nexit_status: 0 duration_sec: 2\ncommand: go run ./cmd/benchmark_run_report\n")

	commands, warnings := loadCommandLog(path)
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none", warnings)
	}
	if len(commands) != 1 {
		t.Fatalf("commands = %v, want 1", commands)
	}
	if !commands[0].Complete {
		t.Fatalf("first command parsed as incomplete: %+v", commands[0])
	}

	var b strings.Builder
	renderRunStatus(&b, commands, nil)
	html := b.String()
	for _, want := range []string{
		"Complete run: all 1 recorded commands exited 0.",
		"go test ./cmd/benchmark_run_report",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("run status missing %q\n%s", want, html)
		}
	}
	if strings.Contains(html, "go run ./cmd/benchmark_run_report") {
		t.Fatalf("trailing command without exit status was rendered\n%s", html)
	}
}

func TestRunStatusReportsSkippedMissingAndOptionalSections(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "RUNBOOK.md"), `# Full TreeDB Benchmark Report Run

- skip_raw: false
- skip_collections: true
- skip_mongo: false
- skip_load_modes: true
- skip_load_scaling: false
- skip_scaling: false
`)
	writeFile(t, filepath.Join(root, "commands.log"), "command: smoke\nexit_status: 0 duration_sec: 1\n")
	if err := os.MkdirAll(filepath.Join(root, "mongo_gateway_load_scaling_1m"), 0o755); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(root, "deep_report.html")
	if err := run([]string{"-run-root", root, "-out", out, "-title", "partial fixture"}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	html := readFile(t, out)
	for _, want := range []string{
		"Run Status",
		"Partial run:",
		"expected artifact section(s) are missing or partial",
		"artifact section(s) were intentionally skipped",
		"Artifact Sections",
		"Raw TreeDB engine",
		"missing required",
		"Collections vs SQLite",
		"skipped",
		"skip flag set in RUNBOOK.md",
		"Mongo InsertMany producer scaling",
		"partial",
		"missing optional",
		"profile manifests",
		"mongo load scaling: mongo_gateway_load_scaling_1m is missing summary.tsv",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("partial status missing %q\n%s", want, html)
		}
	}
}

func TestOldArtifactWithoutCommandsLogStillRenders(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "mongo_gateway_full_sweep_1m_expanded", "summary.tsv"), mongoSummaryFixture(0))

	out := filepath.Join(root, "deep_report.html")
	if err := run([]string{"-run-root", root, "-out", out, "-title", "old artifact"}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	html := readFile(t, out)
	for _, want := range []string{"old artifact", "Mongo API Full Sweep"} {
		if !strings.Contains(html, want) {
			t.Fatalf("legacy report missing %q\n%s", want, html)
		}
	}
	if strings.Contains(html, "Run Status") {
		t.Fatalf("legacy report without RUNBOOK.md/commands.log should not require status block\n%s", html)
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

func TestLoadCollectionsSuppressesWarningOnlyExhaustiveCompactClaims(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "indexes_0", "benchmark_results.json"), `{
  "results": [
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"offline_compact","maintenance_mode":"offline_compact","total_bytes":700,"bytes_per_doc":7,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_0_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"offline_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"offline_compact","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":7,"sqlite_bytes_per_doc":20,"smaller_ratio":2.857142857,"comparison_basis":"legacy fixture"}
  ],
  "guardrail_checks": [
    {"severity":"warning","code":"phase.exhaustive_compact.failed","message":"legacy warning-only exhaustive compact failure"}
  ]
}`)

	rows, comps, warnings := loadCollections(root)
	if len(rows) == 0 {
		t.Fatalf("expected rows to remain available for lifecycle auditing")
	}
	if len(comps) != 0 {
		t.Fatalf("warning-only exhaustive compact evidence should suppress comparisons, got %#v", comps)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "suppressing TreeDB compacted-size comparisons") {
		t.Fatalf("expected suppression warning, got %#v", warnings)
	}

	html := renderHTML(reportData{
		Config:      config{Title: "warning-only compact", RunRoot: t.TempDir()},
		Collections: rows,
		Warnings:    warnings,
	})
	for _, forbidden := range []string{"Compacted size: SQLite bytes/doc divided by TreeDB bytes/doc", "Compacted-state comparisons"} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("warning-only compact evidence should not render %q\n%s", forbidden, html)
		}
	}
	if !strings.Contains(html, "suppressing TreeDB compacted-size comparisons") {
		t.Fatalf("report should preserve suppression warning\n%s", html)
	}
}

func TestLoadCollectionsSuppressesCompactedClaimsWithoutExhaustiveEvidence(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "indexes_0", "benchmark_results.json"), `{
  "results": [
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"offline_compact","maintenance_mode":"offline_compact","total_bytes":700,"bytes_per_doc":7,"measurement_kind":"fixture"},
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":500,"bytes_per_doc":5,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_0_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"offline_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"offline_compact","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":7,"sqlite_bytes_per_doc":20,"smaller_ratio":2.857142857,"comparison_basis":"legacy fixture"},
    {"comparison_name":"leafgen_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"full_leafgen_pack_gc","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":5,"sqlite_bytes_per_doc":20,"smaller_ratio":4,"comparison_basis":"legacy fixture"}
  ],
  "guardrail_checks": []
}`)

	rows, comps, warnings := loadCollections(root)
	if len(rows) == 0 {
		t.Fatalf("expected rows to remain available for lifecycle auditing")
	}
	if len(comps) != 0 {
		t.Fatalf("missing exhaustive compact evidence should suppress comparisons, got %#v", comps)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "no positive exhaustive_compact row") {
		t.Fatalf("expected missing-evidence suppression warning, got %#v", warnings)
	}

	html := renderHTML(reportData{
		Config:      config{Title: "missing exhaustive compact", RunRoot: t.TempDir()},
		Collections: rows,
		Warnings:    warnings,
	})
	for _, forbidden := range []string{"Compacted size: SQLite bytes/doc divided by TreeDB bytes/doc", "Compacted-state comparisons"} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("missing exhaustive compact evidence should not render %q\n%s", forbidden, html)
		}
	}
}

func TestLoadCollectionsSuppressesCompactedClaimsWithoutProductionEvidence(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "indexes_0", "benchmark_results.json"), `{
  "results": [
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"exhaustive_compact","maintenance_mode":"exhaustive_compact","total_bytes":500,"bytes_per_doc":5,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_0_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"tree_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"exhaustive_compact","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":5,"sqlite_bytes_per_doc":20,"smaller_ratio":4,"comparison_basis":"legacy fixture"}
  ],
  "guardrail_checks": []
}`)

	rows, comps, warnings := loadCollections(root)
	if len(rows) == 0 {
		t.Fatalf("expected rows to remain available for lifecycle auditing")
	}
	if len(comps) != 0 {
		t.Fatalf("missing production evidence should suppress comparisons, got %#v", comps)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "matching production evidence") {
		t.Fatalf("expected production-evidence suppression warning, got %#v", warnings)
	}
}

func TestLoadCollectionsKeepsNoSecondaryIndexFlushProductionEvidence(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "indexes_0", "benchmark_results.json"), `{
  "results": [
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":10000,"bytes_per_doc":100,"measurement_kind":"go_benchmark","production_evidence":{"storage_policy":"data_outer=true,index_outer=true","gomaxprocs":16,"physical_cores":8,"flush_admission_effective_concurrency":8,"flush_admission_admitted":true,"flush_admission_span_native":true,"flush_admission_backlog_coalescing":true,"flush_span_candidate_ops":2,"flush_span_used_ops":2,"flush_span_fallbacks":0,"ordered_root_span_fallbacks":0}},
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"exhaustive_compact","maintenance_mode":"exhaustive_compact","total_bytes":500,"bytes_per_doc":5,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_0_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"tree_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"exhaustive_compact","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":5,"sqlite_bytes_per_doc":20,"smaller_ratio":4,"comparison_basis":"fixture"}
  ],
  "guardrail_checks": []
}`)

	rows, comps, warnings := loadCollections(root)
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings with no-secondary-index flush evidence: %#v", warnings)
	}
	if len(comps) != 1 {
		t.Fatalf("no-secondary-index flush evidence should preserve comparison, got %#v", comps)
	}
	html := renderHTML(reportData{
		Config:                config{Title: "production evidence", RunRoot: t.TempDir()},
		Collections:           rows,
		CollectionComparisons: comps,
	})
	for _, want := range []string{"TreeDB production evidence", "effective concurrency", "data_outer=true,index_outer=true"} {
		if !strings.Contains(html, want) {
			t.Fatalf("production evidence report missing %q\n%s", want, html)
		}
	}
}

func TestLoadCollectionsRequiresExhaustiveEvidenceForComparedIndex(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "indexes_2", "benchmark_results.json"), `{
  "results": [
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"exhaustive_compact","maintenance_mode":"exhaustive_compact","total_bytes":450,"bytes_per_doc":4.5,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_2_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":2,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"tree_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_2_indexes","treedb_phase":"exhaustive_compact","sqlite_config_name":"sqlite_native_columns_2_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":5,"sqlite_bytes_per_doc":20,"smaller_ratio":4,"comparison_basis":"legacy mixed-index fixture"}
  ],
  "guardrail_checks": []
}`)

	rows, comps, warnings := loadCollections(root)
	if len(rows) == 0 {
		t.Fatalf("expected rows to remain available for lifecycle auditing")
	}
	if len(comps) != 0 {
		t.Fatalf("index-0 exhaustive evidence should not authorize index-2 comparisons, got %#v", comps)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "compared config/index") {
		t.Fatalf("expected compared-index suppression warning, got %#v", warnings)
	}
}

func TestLoadCollectionsKeepsCompactedClaimsWithMatchingExhaustiveEvidence(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "indexes_2", "benchmark_results.json"), `{
  "results": [
    {"config_name":"treedb_template_v1_collection_2_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":2,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":10000,"bytes_per_doc":100,"measurement_kind":"go_benchmark","production_evidence":{"producer_route":"command_wal_publish","producer_route_candidate_ops":100,"producer_route_used_ops":100,"producer_route_fallbacks":0,"storage_policy":"data_outer=true,index_outer=true","gomaxprocs":16,"physical_cores":8,"flush_admission_effective_concurrency":8,"flush_admission_admitted":true,"flush_admission_span_native":true,"flush_admission_backlog_coalescing":true,"flush_span_fallbacks":0,"ordered_root_span_fallbacks":0}},
    {"config_name":"treedb_template_v1_collection_2_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":2,"document_count":100,"phase":"exhaustive_compact","maintenance_mode":"exhaustive_compact","total_bytes":500,"bytes_per_doc":5,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_2_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":2,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"tree_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_2_indexes","treedb_phase":"exhaustive_compact","sqlite_config_name":"sqlite_native_columns_2_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":5,"sqlite_bytes_per_doc":20,"smaller_ratio":4,"comparison_basis":"fixture"}
  ],
  "guardrail_checks": []
}`)

	rows, comps, warnings := loadCollections(root)
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings with matching exhaustive evidence: %#v", warnings)
	}
	if len(comps) != 1 {
		t.Fatalf("matching exhaustive evidence should preserve comparison, got %#v", comps)
	}
	if comps[0].TreeDBConfig != "treedb_template_v1_collection_2_indexes" {
		t.Fatalf("unexpected comparison preserved: %#v", comps[0])
	}
	html := renderHTML(reportData{
		Config:                config{Title: "production evidence", RunRoot: t.TempDir()},
		Collections:           rows,
		CollectionComparisons: comps,
	})
	for _, want := range []string{"TreeDB production evidence", "command_wal_publish", "effective concurrency"} {
		if !strings.Contains(html, want) {
			t.Fatalf("production evidence report missing %q\n%s", want, html)
		}
	}
}

func TestCollectionChartsIncludeAdditionalFormats(t *testing.T) {
	html := renderHTML(reportData{
		Config: config{Title: "extra formats", RunRoot: t.TempDir()},
		Collections: []collectionRow{
			{ConfigName: "treedb_msgpack_collection_0_indexes", Engine: "treedb_fast", Format: "msgpack", Shape: "collection", IndexCount: 0, Phase: "post_insert", DocsPerSec: 1234},
			{ConfigName: "treedb_msgpack_collection_0_indexes", Engine: "treedb_fast", Format: "msgpack", Shape: "collection", IndexCount: 0, Phase: "exhaustive_compact", BytesPerDoc: 12.3},
		},
	})
	for _, want := range []string{"TreeDB Msgpack", "TreeDB Msgpack Compacted"} {
		if !strings.Contains(html, want) {
			t.Fatalf("collection chart missing additional format label %q\n%s", want, html)
		}
	}
}

func TestCollectionDocsRowsPreferBenchmarkThroughput(t *testing.T) {
	rows := []collectionRow{
		{ConfigName: "treedb_template_v1_collection_0_indexes", Engine: "treedb_fast", Format: "template-v1", Shape: "collection", IndexCount: 0, Phase: "post_insert", DocsPerSec: 1000, MeasurementKind: "fixture_wall_timed"},
		{ConfigName: "treedb_template_v1_collection_0_indexes", Engine: "production_fast", Format: "template-v1", Shape: "collection", IndexCount: 0, Phase: "post_insert", DocsPerSec: 2000, MeasurementKind: "go_benchmark"},
		{ConfigName: "treedb_template_v1_collection_0_indexes", Engine: "treedb_fast", Format: "template-v1", Shape: "collection", IndexCount: 0, Phase: "post_insert", MeasurementKind: "offline_script"},
	}

	chartRows := collectionDocsRows(rows)
	if got, want := len(chartRows.Series), 1; got != want {
		t.Fatalf("series count = %d, want %d: %#v", got, want, chartRows.Series)
	}
	series := chartRows.Series[0]
	if got, want := series.Name, "TreeDB template-v1"; got != want {
		t.Fatalf("series name = %q, want %q", got, want)
	}
	if got, want := series.Values[0], 2000.0; got != want {
		t.Fatalf("docs/sec = %v, want %v", got, want)
	}
}

func TestLoadModeLabelNormalizesSingleMongoConfig(t *testing.T) {
	if got, want := loadModeLabel(loadModeRow{Target: "mongo", Config: "mongo"}), "BSON driver"; got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

func TestTreeDBOnlyLoadModeIncludesNonBSONDirectFormats(t *testing.T) {
	for _, mode := range []string{
		"BSON direct",
		"BSON json_direct",
		"BSON template_v1_direct",
		"BSON raw_wire_tcp",
		"BSON json_raw_wire_tcp",
		"BSON raw_wire",
		"BSON json_raw_wire",
	} {
		if !isTreeDBOnlyLoadMode(mode) {
			t.Fatalf("%q should be TreeDB-only", mode)
		}
	}
	for _, mode := range []string{
		"BSON driver",
		"BSON driver_command_raw",
		"BSON json_driver",
	} {
		if isTreeDBOnlyLoadMode(mode) {
			t.Fatalf("%q should not be TreeDB-only", mode)
		}
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

func TestMongoRowDiskFallsBackToDBStatsTotalSize(t *testing.T) {
	rows := []mongoSummaryRow{
		{TreeDBPhysicalBytes: 2048, MongoPhysicalBytes: 0, MongoDBStatsTotalSize: 4096},
		{TreeDBPhysicalBytes: 3072, MongoPhysicalBytes: 8192, MongoDBStatsTotalSize: 16384},
	}

	if got, want := mongoRowDisk(rows, "mongo"), []float64{4096, 8192}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mongoRowDisk fallback = %v, want %v", got, want)
	}
}

func TestCollectionDiskRowsRequireExhaustiveCompactTreeDBPhase(t *testing.T) {
	rows := []collectionRow{
		{Shape: "collection", Engine: "treedb_fast", ConfigName: "treedb_bson_collection_0_indexes", Format: "bson", IndexCount: 0, Phase: "full_leafgen_pack_gc", BytesPerDoc: 30},
		{Shape: "collection", Engine: "treedb_fast", ConfigName: "treedb_bson_collection_0_indexes", Format: "bson", IndexCount: 0, Phase: "offline_compact", BytesPerDoc: 20},
		{Shape: "collection", Engine: "treedb_fast", ConfigName: "treedb_bson_collection_0_indexes", Format: "bson", IndexCount: 0, Phase: "exhaustive_compact", BytesPerDoc: 25},
		{Shape: "collection", Engine: "treedb_fast", ConfigName: "treedb_bson_collection_0_indexes", Format: "bson", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 80},
	}

	diskRows := collectionDiskRows(rows)
	if len(diskRows.Series) != 1 || len(diskRows.Series[0].Values) != 1 {
		t.Fatalf("unexpected disk rows: %+v", diskRows)
	}
	if got, want := diskRows.Series[0].Values[0], 25.0; got != want {
		t.Fatalf("compacted bytes/doc = %v, want %v", got, want)
	}
}

func TestCollectionComparisonRatioRowsDeduplicateIndexLabels(t *testing.T) {
	rows := collectionComparisonRatioRows([]collectionComparison{
		{
			TreeDBConfig:    "treedb_template_v1_collection_2_indexes",
			TreeDBPhase:     "offline_compact",
			SQLiteConfig:    "sqlite_native_columns_2_indexes",
			SQLitePhase:     "sqlite_vacuum",
			SmallerRatio:    3,
			ComparisonBasis: "fixture",
		},
		{
			TreeDBConfig:    "treedb_template_v1_collection_2_indexes",
			TreeDBPhase:     "full_leafgen_pack_gc",
			SQLiteConfig:    "sqlite_native_columns_2_indexes",
			SQLitePhase:     "sqlite_vacuum",
			SmallerRatio:    7,
			ComparisonBasis: "fixture",
		},
		{
			TreeDBConfig:    "treedb_template_v1_collection_2_indexes",
			TreeDBPhase:     "exhaustive_compact",
			SQLiteConfig:    "sqlite_native_columns_2_indexes",
			SQLitePhase:     "sqlite_vacuum",
			SmallerRatio:    5,
			ComparisonBasis: "fixture",
		},
	})
	if got, want := rows.Categories, []string{"2 indexes"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("categories = %v, want %v", got, want)
	}
	if len(rows.Series) != 1 || len(rows.Series[0].Values) != 1 {
		t.Fatalf("unexpected ratio series: %+v", rows.Series)
	}
	if got, want := rows.Series[0].Values[0], 5.0; got != want {
		t.Fatalf("ratio value = %v, want %v", got, want)
	}
	if got, want := rows.Series[0].Name, "Template V1 vs Native Columns"; got != want {
		t.Fatalf("ratio series name = %q, want %q", got, want)
	}
}

func TestCollectionLifecycleRowsOmitAbsentPhases(t *testing.T) {
	rows := []collectionRow{
		{ConfigName: "treedb_template_v1_collection_2_indexes", Engine: "treedb_fast", Format: "template-v1", IndexCount: 2, Phase: "post_insert", BytesPerDoc: 80},
		{ConfigName: "treedb_template_v1_collection_2_indexes", Engine: "treedb_fast", Format: "template-v1", IndexCount: 2, Phase: "offline_compact", BytesPerDoc: 40},
		{ConfigName: "treedb_template_v1_collection_2_indexes", Engine: "treedb_fast", Format: "template-v1", IndexCount: 2, Phase: "full_leafgen_pack_gc", BytesPerDoc: 20},
		{ConfigName: "sqlite_native_columns_2_indexes", Engine: "sqlite_wal_normal", Format: "native-columns", IndexCount: 2, Phase: "sqlite_vacuum", BytesPerDoc: 120},
	}
	lifecycle := collectionLifecycleRows(rows)
	if !reflect.DeepEqual(lifecycle.Categories, []string{"post insert", "offline compact", "leafgen GC", "SQLite VACUUM"}) {
		t.Fatalf("lifecycle categories = %v", lifecycle.Categories)
	}
	for _, series := range lifecycle.Series {
		if len(series.Values) != len(lifecycle.Categories) {
			t.Fatalf("series %q values = %v for categories %v", series.Name, series.Values, lifecycle.Categories)
		}
	}
}

func TestMongoStorageBasisTextUsesRunSpecificMetric(t *testing.T) {
	rows := []mongoSummaryRow{
		{TreeDBDiskSnapshot: "maintenance", MongoDBStatsTotalSize: 4096},
		{TreeDBDiskSnapshot: "maintenance", MongoDBStatsTotalSize: 8192},
	}
	got := mongoStorageBasisText(rows)
	for _, want := range []string{"TreeDB physical bytes after maintenance", "MongoDB dbStats.totalSize from this run"} {
		if !strings.Contains(got, want) {
			t.Fatalf("storage basis %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "fall back") || strings.Contains(got, "when ") {
		t.Fatalf("storage basis should describe this run without fallback prose: %q", got)
	}
}

func TestMongoLoadModeStorageMarksUnavailableMongoPhysicalBytes(t *testing.T) {
	html := renderHTML(reportData{
		Config: config{Title: "load storage", RunRoot: t.TempDir()},
		MongoLoadModes: []loadModeRow{
			{Indexes: 0, Target: "treedb", Config: "treedb_bson_driver", OpsPerSec: 1000, PhysicalBytes: 4096},
			{Indexes: 0, Target: "mongo", Config: "mongo_driver", OpsPerSec: 900, PhysicalBytes: 0},
		},
	})
	for _, want := range []string{
		"MongoDB physical_bytes unavailable in this matrix",
		"unavailable",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("load-mode report missing %q\n%s", want, html)
		}
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

	withEmptyOptionalFloat := strings.Replace(mongoSummaryFixture(0), "\t1000\t1000\t1000\t500", "\t1000\t\t1000\t500", 1)
	writeFile(t, path, withEmptyOptionalFloat)
	rows, err = readMongoSummary(path)
	if err != nil {
		t.Fatalf("readMongoSummary rejected empty optional float: %v", err)
	}
	if got := rows[0].TreeDBSampledOpsSec; got != 0 {
		t.Fatalf("TreeDBSampledOpsSec = %v, want 0 for empty optional float", got)
	}

	withInvalidFloat := strings.Replace(mongoSummaryFixture(0), "\t1000\t1000\t1000\t500", "\tnope\t1000\t1000\t500", 1)
	writeFile(t, path, withInvalidFloat)
	_, err = readMongoSummary(path)
	if err == nil || !strings.Contains(err.Error(), "treedb_ops_sec") {
		t.Fatalf("readMongoSummary error = %v, want treedb_ops_sec parse failure", err)
	}

	writeFile(t, path, strings.Replace(mongoSummaryFixture(0), "100\t0\t", "oops\t0\t", 1))
	_, err = readMongoSummary(path)
	if err == nil || !strings.Contains(err.Error(), "documents") {
		t.Fatalf("readMongoSummary error = %v, want documents parse failure", err)
	}
}

func TestReadMongoSummaryOptionalLoadMetadata(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "summary.tsv")

	writeFile(t, path, mongoSummaryFixture(0))
	rows, err := readMongoSummary(path)
	if err != nil {
		t.Fatalf("read old summary: %v", err)
	}
	if rows[0].BatchSize != 0 || rows[0].InsertProducers != 0 || rows[0].EffectiveProducers != 0 || rows[0].DriverCalls != 0 || rows[0].LoadBatchCount != 0 {
		t.Fatalf("old summary parsed metadata: %+v", rows[0])
	}

	writeFile(t, path, mongoSummaryFixtureWithLoadMetadata(0, 25, 8, 4))
	rows, err = readMongoSummary(path)
	if err != nil {
		t.Fatalf("read summary with load metadata: %v", err)
	}
	got := rows[0]
	if got.BatchSize != 25 || got.InsertProducers != 8 || got.EffectiveProducers != 4 || got.DriverCalls != 4 || got.LoadBatchCount != 4 {
		t.Fatalf("load metadata = batch=%d requested=%d effective=%d calls=%d batches=%d",
			got.BatchSize, got.InsertProducers, got.EffectiveProducers, got.DriverCalls, got.LoadBatchCount)
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

func TestMongoRowsUsePrimaryScope(t *testing.T) {
	rows := []mongoSummaryRow{
		{Documents: 100, SecondaryIndexes: 0, TreeDBConfig: "treedb_a", MongoConfig: "mongo", Phase: "load_insert_many"},
		{Documents: 100, SecondaryIndexes: 0, TreeDBConfig: "treedb_a", MongoConfig: "mongo", Phase: "id_find_one", TreeDBOpsSec: 100},
		{Documents: 1_000, SecondaryIndexes: 0, TreeDBConfig: "treedb_b", MongoConfig: "mongo", Phase: "load_insert_many"},
		{Documents: 1_000, SecondaryIndexes: 0, TreeDBConfig: "treedb_b", MongoConfig: "mongo", Phase: "id_find_one", TreeDBOpsSec: 1000},
		{Documents: 100, SecondaryIndexes: 0, TreeDBConfig: "treedb_a", MongoConfig: "mongo", Phase: "concurrent_id_find_one_r2"},
		{Documents: 1_000, SecondaryIndexes: 0, TreeDBConfig: "treedb_b", MongoConfig: "mongo", Phase: "concurrent_id_find_one_r8"},
	}
	row, ok := mongoRow(rows, 0, "id_find_one")
	if !ok || row.Documents != 1_000 || row.TreeDBOpsSec != 1000 {
		t.Fatalf("mongoRow = %+v, %v; want primary 1000-doc scope", row, ok)
	}
	counts := mongoSweepCountsInPrimaryScope(rows, 0, "concurrent_id_find_one_r")
	if want := []int{8}; !reflect.DeepEqual(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}
}

func TestMongoPrimaryScopeKeepsRangeModeRows(t *testing.T) {
	rows := []mongoSummaryRow{
		{Documents: 100, SecondaryIndexes: 0, TreeDBConfig: "treedb_a", MongoConfig: "mongo", RangeIndex: true, Phase: "load_insert_many"},
		{Documents: 100, SecondaryIndexes: 0, TreeDBConfig: "treedb_a", MongoConfig: "mongo", RangeIndex: true, RangeMode: "indexed", Phase: "age_range_indexed_limit_10", TreeDBOpsSec: 100, MongoOpsSec: 50},
		{Documents: 100, SecondaryIndexes: 0, TreeDBConfig: "treedb_a", MongoConfig: "mongo", RangeIndex: true, RangeMode: "indexed", Phase: "concurrent_age_range_indexed_limit_10_r4", TreeDBOpsSec: 400, MongoOpsSec: 100},
		{Documents: 1_000, SecondaryIndexes: 0, TreeDBConfig: "treedb_b", MongoConfig: "mongo", RangeIndex: true, Phase: "load_insert_many"},
		{Documents: 1_000, SecondaryIndexes: 0, TreeDBConfig: "treedb_b", MongoConfig: "mongo", RangeIndex: true, RangeMode: "indexed", Phase: "age_range_indexed_limit_10", TreeDBOpsSec: 1_000, MongoOpsSec: 500},
		{Documents: 1_000, SecondaryIndexes: 0, TreeDBConfig: "treedb_b", MongoConfig: "mongo", RangeIndex: true, RangeMode: "indexed", Phase: "concurrent_age_range_indexed_limit_10_r16", TreeDBOpsSec: 1_600, MongoOpsSec: 200},
	}
	summary := mongoOperationThroughputRows(rows, 0, true)
	var found bool
	for _, row := range summary {
		if row.Label != "range read" {
			continue
		}
		found = true
		if !row.HasSingle || row.Single.RangeMode != "indexed" {
			t.Fatalf("range single row = %+v, want indexed range row", row.Single)
		}
		if !row.HasTreePeak || row.TreePeakCount != 16 || row.TreePeak.RangeMode != "indexed" {
			t.Fatalf("range peak row = %+v count=%d, want primary indexed range sweep", row.TreePeak, row.TreePeakCount)
		}
	}
	if !found {
		t.Fatalf("range read summary missing from %+v", summary)
	}
}

func TestMongoScalingCountsAcrossPerCountConfigs(t *testing.T) {
	rows := []mongoSummaryRow{
		{Documents: 1000, SecondaryIndexes: 0, TreeDBConfig: "treedb_readers_1", MongoConfig: "mongo_readers_1", Phase: "load_insert_many"},
		{Documents: 1000, SecondaryIndexes: 0, TreeDBConfig: "treedb_readers_1", MongoConfig: "mongo_readers_1", Phase: "concurrent_id_find_one_r1", TreeDBOpsSec: 10, MongoOpsSec: 5},
		{Documents: 1000, SecondaryIndexes: 0, TreeDBConfig: "treedb_readers_16", MongoConfig: "mongo_readers_16", Phase: "load_insert_many"},
		{Documents: 1000, SecondaryIndexes: 0, TreeDBConfig: "treedb_readers_16", MongoConfig: "mongo_readers_16", Phase: "concurrent_id_find_one_r16", TreeDBOpsSec: 160, MongoOpsSec: 80},
	}
	counts := mongoSweepCounts(rows, 0, "concurrent_id_find_one_r")
	if want := []int{1, 16}; !reflect.DeepEqual(counts, want) {
		t.Fatalf("scaling counts = %v, want %v", counts, want)
	}
	values := mongoSweepAny(rows, 0, "concurrent_id_find_one_r", "tree", counts)
	if want := []float64{10, 160}; !reflect.DeepEqual(values, want) {
		t.Fatalf("scaling values = %v, want %v", values, want)
	}
	summary := mongoOperationThroughputRows(rows, 0, false)
	if len(summary) == 0 || !summary[0].HasTreePeak || summary[0].TreePeakCount != 16 || summary[0].TreePeak.TreeDBOpsSec != 160 {
		t.Fatalf("scaling summary = %+v, want full per-count sweep peak", summary)
	}
	scoped := mongoOperationThroughputRows(rows, 0, true)
	if len(scoped) == 0 || !scoped[0].HasTreePeak || scoped[0].TreePeakCount != 1 {
		t.Fatalf("scoped summary = %+v, want primary-scope-only peak", scoped)
	}
}

func TestMongoProfileMissingResultDoesNotLookLikeZeroIndexRun(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, "mongo_gateway_full_sweep_1m_expanded", "profiles", "treedb_cell", "profile_manifest.json")
	writeFile(t, manifest, `{
  "profile_dir": "profiles/treedb_cell",
  "artifacts": [{"phase":"load_insert_many","duration_ms":12.3,"cpu_profile":"load.cpu.pprof"}]
}`)
	item, err := readMongoProfileSummary(root, manifest)
	if err != nil {
		t.Fatalf("read profile summary: %v", err)
	}
	if item.HasResult {
		t.Fatalf("profile HasResult=true despite missing benchmark_result.json")
	}
	if got := profileIndexesLabel(item); got != "-" {
		t.Fatalf("profile indexes label = %q, want dash for missing result", got)
	}
	var b strings.Builder
	renderProfiles(&b, profileReportData{Mongo: []mongoProfileSummary{item}})
	html := b.String()
	if strings.Contains(html, "<td class=\"num\">0</td><td>load_insert_many") {
		t.Fatalf("missing result rendered as 0-index run\n%s", html)
	}
	if !strings.Contains(html, "benchmark_result.json") {
		t.Fatalf("missing result error not rendered\n%s", html)
	}
}

func TestMongoThroughputSummaryUsesSameConcurrencyRatio(t *testing.T) {
	rows := []mongoSummaryRow{
		{SecondaryIndexes: 0, Phase: "id_find_one", TreeDBOpsSec: 10, MongoOpsSec: 8},
		{SecondaryIndexes: 0, Phase: "concurrent_id_find_one_r1", TreeDBOpsSec: 100, MongoOpsSec: 80},
		{SecondaryIndexes: 0, Phase: "concurrent_id_find_one_r16", TreeDBOpsSec: 500, MongoOpsSec: 250},
		{SecondaryIndexes: 0, Phase: "concurrent_id_find_one_r32", TreeDBOpsSec: 200, MongoOpsSec: 40},
	}
	var b strings.Builder
	writeMongoThroughputSummary(&b, mongoOperationThroughputRows(rows, 0, true))
	html := b.String()
	for _, want := range []string{
		"Throughput summary",
		"single-threaded TreeDB",
		"peak TreeDB",
		"largest TreeDB/MongoDB",
		"5.00x",
		"32 readers",
		"200",
		"40",
		"TreeDB scale-up",
		"20.00x",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("summary missing %q\n%s", want, html)
		}
	}
	if strings.Contains(html, "<div class=\"metric\"") {
		t.Fatalf("summary should not render metric cards\n%s", html)
	}
	for _, unwanted := range []string{"<div class=\"metric\"", "peak ratio"} {
		if strings.Contains(html, unwanted) {
			t.Fatalf("summary still contains independent peak artifact %q\n%s", unwanted, html)
		}
	}
}

func TestMongoRowsForPhaseSkipsMissingPhase(t *testing.T) {
	rows := []mongoSummaryRow{
		{SecondaryIndexes: 0, Phase: "load_insert_many", TreeDBOpsSec: 10},
		{SecondaryIndexes: 1, Phase: "id_find_one"},
		{SecondaryIndexes: 2, Phase: "load_insert_many", TreeDBOpsSec: 20},
	}
	loadRows := mongoRowsForPhase(rows, "load_insert_many")
	if got, want := mongoRowIndexLabels(loadRows), []string{"0", "2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("indexes = %v, want %v", got, want)
	}
	if got, want := mongoRowOps(loadRows, "tree"), []float64{10, 20}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ops = %v, want %v", got, want)
	}
}

func TestFullSweepLoadNoteRangeIndexMentionsDisplayedZeroOnly(t *testing.T) {
	note := fullSweepLoadNote([]mongoSummaryRow{{SecondaryIndexes: 1, Phase: "load_insert_many", RangeIndex: true}})
	if strings.Contains(note, "0-secondary-index load cell") {
		t.Fatalf("note mentions absent 0-index cell: %s", note)
	}
	note = fullSweepLoadNote([]mongoSummaryRow{{SecondaryIndexes: 0, Phase: "load_insert_many", RangeIndex: true}})
	if !strings.Contains(note, "0-secondary-index load cell") {
		t.Fatalf("note omits displayed 0-index cell caveat: %s", note)
	}
}

func TestFullSweepLoadNoteReportsProducerMetadata(t *testing.T) {
	note := fullSweepLoadNote([]mongoSummaryRow{{
		Documents:          100,
		SecondaryIndexes:   0,
		Phase:              "load_insert_many",
		BatchSize:          25,
		InsertProducers:    8,
		EffectiveProducers: 8,
		DriverCalls:        4,
		LoadBatchCount:     4,
	}})
	for _, want := range []string{
		"Measured load metadata",
		"100 docs",
		"batch size 25",
		"requested insert producers 8",
		"effective 4",
		"capped by 4 load batches",
		"driver calls 4",
	} {
		if !strings.Contains(note, want) {
			t.Fatalf("note missing %q\n%s", want, note)
		}
	}
}

func TestRenderMongoLoadScalingSection(t *testing.T) {
	rows := []mongoSummaryRow{
		{Documents: 100, BatchSize: 25, LoadBatchCount: 4, SecondaryIndexes: 0, TreeDBConfig: "treedb_bson", MongoConfig: "mongo", Phase: "load_insert_many", InsertProducers: 1, EffectiveProducers: 1, TreeDBOpsSec: 1000, MongoOpsSec: 500, TreeDBToMongoRatio: 2},
		{Documents: 100, BatchSize: 25, LoadBatchCount: 4, SecondaryIndexes: 0, TreeDBConfig: "treedb_bson", MongoConfig: "mongo", Phase: "load_insert_many", InsertProducers: 2, EffectiveProducers: 2, TreeDBOpsSec: 1600, MongoOpsSec: 700, TreeDBToMongoRatio: 2.29},
		{Documents: 100, BatchSize: 25, LoadBatchCount: 4, SecondaryIndexes: 1, TreeDBConfig: "treedb_bson", MongoConfig: "mongo", Phase: "load_insert_many", InsertProducers: 1, EffectiveProducers: 1, TreeDBOpsSec: 800, MongoOpsSec: 300, TreeDBToMongoRatio: 2.67},
	}
	var b strings.Builder
	renderMongoLoadScaling(&b, rows)
	html := b.String()
	for _, want := range []string{
		"Mongo API InsertMany Producer Scaling",
		"Insert Throughput Vs Insert Producers, 0 Indexes",
		"Insert Throughput Vs Insert Producers, 1 Index",
		"Producer scaling summary",
		"requested producers",
		"effective producers",
		"Raw load-scaling TSV rows",
		"1,600",
		"2.29x",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("load-scaling section missing %q\n%s", want, html)
		}
	}
}

func TestMongoIndexRetentionTable(t *testing.T) {
	rows := []mongoSummaryRow{
		{SecondaryIndexes: 0, TreeDBOpsSec: 100, MongoOpsSec: 80, TreeDBToMongoRatio: 1.25, TreeDBToMongoPhysRatio: 0.4},
		{SecondaryIndexes: 1, TreeDBOpsSec: 80, MongoOpsSec: 40, TreeDBToMongoRatio: 2, TreeDBToMongoPhysRatio: 0.3},
	}
	var b strings.Builder
	writeMongoIndexRetentionTable(&b, rows)
	html := b.String()
	for _, want := range []string{
		"Index throughput retention",
		"TreeDB retained",
		"MongoDB retained",
		"80.0%",
		"50.0%",
		"2.00x",
		"0.30x",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("retention table missing %q\n%s", want, html)
		}
	}
}

func TestFormatChartTooltipValueDoesNotDoubleBytes(t *testing.T) {
	if got := formatChartTooltipValue(1024*1024, "bytes"); got != "1.00 MiB" {
		t.Fatalf("bytes tooltip = %q, want 1.00 MiB", got)
	}
	if got := formatChartTooltipValue(1200, "docs/sec"); got != "1K docs/sec" {
		t.Fatalf("docs tooltip = %q, want 1K docs/sec", got)
	}
}

func TestLoadMongoScalingWarnsMissingSummary(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "indexes_0"), 0o755); err != nil {
		t.Fatal(err)
	}
	rows, warnings := loadMongoScaling(root)
	if len(rows) != 0 {
		t.Fatalf("rows = %v, want none", rows)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "indexes_0") || !strings.Contains(warnings[0], "summary.tsv") {
		t.Fatalf("warnings = %v, want missing summary warning", warnings)
	}
}

func TestLoadMongoLoadScalingWarnsMissingSummary(t *testing.T) {
	root := t.TempDir()
	rows, warnings := loadMongoLoadScaling(root)
	if len(rows) != 0 {
		t.Fatalf("rows = %v, want none", rows)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "mongo load scaling") || !strings.Contains(warnings[0], "summary.tsv") {
		t.Fatalf("warnings = %v, want missing load-scaling summary warning", warnings)
	}

	missingRoot := filepath.Join(t.TempDir(), "absent")
	rows, warnings = loadMongoLoadScaling(missingRoot)
	if len(rows) != 0 || len(warnings) != 0 {
		t.Fatalf("absent root rows=%v warnings=%v, want none", rows, warnings)
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

func TestLoadMongoLoadModesRejectsEscapingRawJSONPath(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "matrix.tsv"), "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n"+
		"treedb\ttreedb_bson_driver\t100\t0\t../outside.json\t2000\n")

	rows, warnings, err := loadMongoLoadModes(root)
	if err != nil {
		t.Fatalf("loadMongoLoadModes failed: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %v, want none", rows)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "escapes") {
		t.Fatalf("warnings = %v, want path escape warning", warnings)
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

const mongoSummaryHeaderWithLoadMetadata = "documents\tsecondary_indexes\trange_index\trange_mode\ttreedb_config\tmongo_config\tphase\ttreedb_ops_sec\ttreedb_sampled_ops_sec\ttreedb_sampled_ns_per_op\tmongo_ops_sec\tmongo_sampled_ops_sec\tmongo_sampled_ns_per_op\ttreedb_to_mongo_ops_ratio\ttreedb_to_mongo_sampled_ops_ratio\ttreedb_p50_us\tmongo_p50_us\ttreedb_p95_us\tmongo_p95_us\ttreedb_p99_us\tmongo_p99_us\ttreedb_disk_snapshot\ttreedb_disk_bytes\ttreedb_physical_bytes\tmongo_dbstats_data_size_bytes\tmongo_dbstats_total_size_bytes\tmongo_physical_bytes\ttreedb_to_mongo_dbstats_total_ratio\ttreedb_to_mongo_physical_ratio\tbatch_size\tinsert_producers\teffective_producers\tdriver_calls\tload_batch_count\n"

func mongoSummaryFixture(indexes int) string {
	return mongoSummaryHeader +
		fmt.Sprintf("100\t%d\tfalse\t\ttreedb_bson\tmongo\tload_insert_many\t1000\t1000\t1000\t500\t500\t2000\t2\t2\t1\t2\t3\t4\t5\t6\tmaintenance\t1000\t2000\t3000\t4000\t5000\t0.25\t0.4\n", indexes) +
		fmt.Sprintf("100\t%d\tfalse\t\ttreedb_bson\tmongo\tconcurrent_id_find_one_r1\t2000\t2000\t500\t1000\t1000\t1000\t2\t2\t1\t2\t3\t4\t5\t6\tmaintenance\t1000\t2000\t3000\t4000\t5000\t0.25\t0.4\n", indexes)
}

func mongoSummaryFixtureWithLoadMetadata(indexes, batchSize, requestedProducers, effectiveProducers int) string {
	loadBatchCount := (100 + batchSize - 1) / batchSize
	return mongoSummaryHeaderWithLoadMetadata +
		fmt.Sprintf("100\t%d\tfalse\t\ttreedb_bson\tmongo\tload_insert_many\t1000\t1000\t1000\t500\t500\t2000\t2\t2\t1\t2\t3\t4\t5\t6\tmaintenance\t1000\t2000\t3000\t4000\t5000\t0.25\t0.4\t%d\t%d\t%d\t%d\t%d\n",
			indexes, batchSize, requestedProducers, effectiveProducers, loadBatchCount, loadBatchCount)
}

func mongoLoadScalingSummaryFixture() string {
	return mongoSummaryHeaderWithLoadMetadata +
		mongoLoadScalingSummaryRow(0, 1, 1, 1000, 500) +
		mongoLoadScalingSummaryRow(0, 2, 2, 1600, 700) +
		mongoLoadScalingSummaryRow(1, 1, 1, 800, 300) +
		mongoLoadScalingSummaryRow(1, 2, 2, 1200, 450)
}

func mongoLoadScalingSummaryRow(indexes, requestedProducers, effectiveProducers int, treeOps, mongoOps float64) string {
	batchSize := 25
	loadBatchCount := 4
	ratio := treeOps / mongoOps
	return fmt.Sprintf("100\t%d\tfalse\t\ttreedb_bson\tmongo\tload_insert_many\t%.0f\t%.0f\t1000\t%.0f\t%.0f\t2000\t%.2f\t%.2f\t1\t2\t3\t4\t5\t6\tmaintenance\t1000\t2000\t3000\t4000\t5000\t0.25\t0.4\t%d\t%d\t%d\t%d\t%d\n",
		indexes, treeOps, treeOps, mongoOps, mongoOps, ratio, ratio, batchSize, requestedProducers, effectiveProducers, loadBatchCount, loadBatchCount)
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

func TestCollectionLifecycleRowsDistinguishTreeDBAndSQLiteJSON(t *testing.T) {
	rows := []collectionRow{
		{ConfigName: "sqlite_json_0_indexes", Engine: "sqlite_wal_normal", Format: "json", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 397.2},
		{ConfigName: "sqlite_json_0_indexes", Engine: "sqlite_wal_normal", Format: "json", IndexCount: 0, Phase: "sqlite_vacuum", BytesPerDoc: 368.4},
		{ConfigName: "sqlite_native_columns_0_indexes", Engine: "sqlite_wal_normal", Format: "native-columns", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 240.0},
		{ConfigName: "sqlite_native_columns_0_indexes", Engine: "sqlite_wal_normal", Format: "native-columns", IndexCount: 0, Phase: "sqlite_vacuum", BytesPerDoc: 221.5},
		{ConfigName: "treedb_json_collection_0_indexes", Engine: "treedb_fast", Format: "json", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 195.0},
		{ConfigName: "treedb_json_collection_0_indexes", Engine: "treedb_fast", Format: "json", IndexCount: 0, Phase: "exhaustive_compact", BytesPerDoc: 24.2},
		{ConfigName: "treedb_bson_collection_0_indexes", Engine: "treedb_fast", Format: "bson", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 180.0},
		{ConfigName: "treedb_bson_collection_0_indexes", Engine: "treedb_fast", Format: "bson", IndexCount: 0, Phase: "exhaustive_compact", BytesPerDoc: 21.8},
		{ConfigName: "treedb_template_v1_raw_collection_0_indexes", Engine: "treedb_fast", Format: "template-v1", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 150.0},
		{ConfigName: "treedb_template_v1_raw_collection_0_indexes", Engine: "treedb_fast", Format: "template-v1", IndexCount: 0, Phase: "exhaustive_compact", BytesPerDoc: 20.1},
		{ConfigName: "treedb_template_v1_collection_0_indexes", Engine: "treedb_fast", Format: "template-v1", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 140.0},
		{ConfigName: "treedb_template_v1_collection_0_indexes", Engine: "treedb_fast", Format: "template-v1", IndexCount: 0, Phase: "exhaustive_compact", BytesPerDoc: 18.8},
	}

	lifecycle := collectionLifecycleRows(rows)
	wantOrder := []string{
		"TreeDB Template V1",
		"TreeDB Template V1 Raw",
		"TreeDB BSON",
		"TreeDB JSON",
		"SQLite Native",
		"SQLite JSON",
	}
	if len(lifecycle.Series) != len(wantOrder) {
		t.Fatalf("series count = %d, want %d: %+v", len(lifecycle.Series), len(wantOrder), lifecycle.Series)
	}
	seenColors := make(map[string]string)
	byName := make(map[string]chartSeries)
	for i, s := range lifecycle.Series {
		if s.Name != wantOrder[i] {
			t.Fatalf("series[%d].Name = %q, want %q", i, s.Name, wantOrder[i])
		}
		if strings.Contains(s.Name, "/ i0") {
			t.Fatalf("lifecycle series %q should not include redundant index suffix", s.Name)
		}
		if prev, dup := seenColors[s.Color]; dup {
			t.Fatalf("color collision %q between %q and %q", s.Color, prev, s.Name)
		}
		seenColors[s.Color] = s.Name
		byName[s.Name] = s
	}

	postIdx := indexOf(lifecycle.Categories, "post insert")
	vacIdx := indexOf(lifecycle.Categories, "SQLite VACUUM")
	if postIdx < 0 || vacIdx < 0 {
		t.Fatalf("expected post insert and SQLite VACUUM phases, got %v", lifecycle.Categories)
	}
	if got := byName["TreeDB JSON"].Values[postIdx]; got != 195.0 {
		t.Fatalf("TreeDB JSON post_insert = %v, want 195.0 (collided with SQLite JSON?)", got)
	}
	if got := byName["SQLite JSON"].Values[postIdx]; got != 397.2 {
		t.Fatalf("SQLite JSON post_insert = %v, want 397.2 (collided with TreeDB JSON?)", got)
	}
	if got := byName["TreeDB JSON"].Values[vacIdx]; got != 0 {
		t.Fatalf("TreeDB JSON should not have SQLite VACUUM value, got %v", got)
	}
	if got := byName["SQLite JSON"].Values[vacIdx]; got != 368.4 {
		t.Fatalf("SQLite JSON SQLite VACUUM = %v, want 368.4", got)
	}
}

func TestCollectionSeriesColorsDistinctAndDeterministic(t *testing.T) {
	cases := []struct {
		key  string
		want string
	}{
		{"TreeDB\x00template-v1", "#2563eb"},
		{"TreeDB Template V1", "#2563eb"},
		{"TreeDB\x00template-v1-raw", "#0891b2"},
		{"TreeDB Template V1 Raw", "#0891b2"},
		{"TreeDB\x00bson", "#7c3aed"},
		{"TreeDB BSON", "#7c3aed"},
		{"TreeDB\x00json", "#d97706"},
		{"TreeDB JSON", "#d97706"},
		{"SQLite\x00native-columns", "#059669"},
		{"SQLite Native", "#059669"},
		{"SQLite\x00json", "#dc2626"},
		{"SQLite JSON", "#dc2626"},
		{"MongoDB", "#15803d"},
		{"TreeDB", "#2563eb"},
	}
	for _, tc := range cases {
		if got := collectionSeriesColor(tc.key); got != tc.want {
			t.Errorf("collectionSeriesColor(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}

	// Verify per-chart deduplication guarantees zero collisions even for arbitrary series.
	series := assignDistinctSeriesColors([]chartSeries{
		{Name: "Template V1 vs Native Columns", Color: "#d97706"},
		{Name: "BSON vs Native Columns", Color: "#d97706"},
		{Name: "JSON vs JSON", Color: "#d97706"},
		{Name: "Msgpack vs Native Columns", Color: "#d97706"},
		{Name: "TreeDB BSON", Color: "#d97706"},
	})
	used := make(map[string]string)
	for _, s := range series {
		if prev, dup := used[s.Color]; dup {
			t.Fatalf("duplicate color %q assigned to %q and %q", s.Color, prev, s.Name)
		}
		used[s.Color] = s.Name
	}
	if got := series[4].Color; got != "#7c3aed" {
		t.Fatalf("known series TreeDB BSON color = %q, want #7c3aed", got)
	}
}

func TestNiceLinearAndLogAxisTicks(t *testing.T) {
	axisMax, ticks := niceLinearAxis(397.2, "B/doc")
	if axisMax != 400 {
		t.Fatalf("niceLinearAxis(397.2) max = %v, want 400", axisMax)
	}
	wantTicks := []float64{0, 100, 200, 300, 400}
	if !reflect.DeepEqual(ticks, wantTicks) {
		t.Fatalf("niceLinearAxis(397.2) ticks = %v, want %v", ticks, wantTicks)
	}

	byteMax, byteTicks := niceLinearAxis(228*1024*1024, "bytes")
	if byteMax != 250*1024*1024 {
		t.Fatalf("niceLinearAxis(228 MiB) max = %v, want 250 MiB", byteMax)
	}
	if len(byteTicks) != 6 || formatAxisTick(byteTicks[len(byteTicks)-1], "bytes") != "250 MiB" {
		t.Fatalf("unexpected byte ticks: %v", byteTicks)
	}

	logMin, logMax, logTicks := niceLogAxis(18.8, 397.2, "B/doc")
	if !(logMin < 18.8 && logMax > 397.2) {
		t.Fatalf("niceLogAxis(18.8, 397.2) bounds [%v, %v] do not enclose range", logMin, logMax)
	}
	if len(logTicks) < 4 {
		t.Fatalf("expected >=4 nice log ticks, got %v", logTicks)
	}
}

func TestBarChartNarrowAndNearZeroBarsReadable(t *testing.T) {
	categories := []string{"post insert", "online maint", "offline compact", "offline rewrite", "exhaustive", "SQLite VACUUM"}
	series := []chartSeries{
		{Name: "TreeDB Template V1", Values: []float64{140, 90, 45, 30, 18.8, 0}, Color: "#2563eb"},
		{Name: "TreeDB BSON", Values: []float64{180, 110, 50, 32, 2.1, 0}, Color: "#7c3aed"},
		{Name: "TreeDB JSON", Values: []float64{195, 120, 55, 35, 24.4, 0}, Color: "#d97706"},
		{Name: "SQLite Native", Values: []float64{240, 0, 0, 0, 0, 221.5}, Color: "#059669"},
		{Name: "SQLite JSON", Values: []float64{397.2, 0, 0, 0, 0, 368.4}, Color: "#dc2626"},
	}

	html := compactVerticalBarChart("Bytes/doc by lifecycle phase, 0 indexes", categories, "phase", series, "B/doc")
	for _, want := range []string{
		"rotate(-90)",           // narrow bars get crisp rotated labels
		">18.8</text>",          // near-zero exhaustive value is labeled
		">2.1</text>",           // tiny value is labeled
		"height=\"3.0\"",        // minimum visible >= 3px bar stub for 2.1 on 400 scale
		"scale-toggle",          // wide dynamic range gets Linear/Log toggle
		"data-scale=\"linear\"", // static HTML defaults to linear
		"scale-linear",          // linear SVG layer
		"scale-log",             // log SVG layer
		">400</text>",           // nice rounded Y-axis top tick
		"<details class=\"chart-values\"><summary>Exact values</summary>",
		"18.8 B/doc",
		"397.2 B/doc",
		"class=\"legend-item\"",
		"data-series=\"0\"",
		"data-cat=\"4\"",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("narrow/near-zero chart missing %q\n%s", want, html)
		}
	}

	// Wide bars (2 categories x 1 series -> barW >> 26) should render horizontal labels without -90 rotation on bar-val.
	wideHTML := compactVerticalBarChart("Wide bars", []string{"a", "b"}, "cat", []chartSeries{
		{Name: "TreeDB BSON", Values: []float64{100, 120}, Color: "#7c3aed"},
	}, "B/doc")
	if strings.Contains(wideHTML, "class=\"bar-val\" data-series=\"0\" transform=\"translate(") {
		t.Fatalf("wide bars should use horizontal labels, not rotated\n%s", wideHTML)
	}
	if !strings.Contains(wideHTML, ">100</text>") || !strings.Contains(wideHTML, ">120</text>") {
		t.Fatalf("wide bars missing value labels\n%s", wideHTML)
	}
	if strings.Contains(wideHTML, "scale-toggle") {
		t.Fatalf("narrow dynamic range (120/100 < 4) should omit Linear/Log toggle\n%s", wideHTML)
	}
}

func TestChartInteractiveQOLTooltipsLegendAndExactValues(t *testing.T) {
	// Include NaN in a lineChart series to verify chart-meta JSON serialization never fails.
	html := lineChart("Storage footprint vs secondary indexes", []string{"0", "1", "2"}, "secondary indexes", "storage bytes", []chartSeries{
		{Name: "TreeDB", Values: []float64{13.8 * 1024 * 1024, math.NaN(), 25 * 1024 * 1024}, Color: "#2563eb"},
		{Name: "MongoDB", Values: []float64{228 * 1024 * 1024, 240 * 1024 * 1024, 260 * 1024 * 1024}, Color: "#15803d"},
	}, "bytes")

	for _, want := range []string{
		"scale-toggle",
		"scale-log",
		"class=\"chart-meta\"",
		"class=\"chart-item\"",
		"class=\"legend-item\"",
		"<details class=\"chart-values\"><summary>Exact values</summary>",
		"13.80 MiB",
		"228.00 MiB",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("line chart missing %q\n%s", want, html)
		}
	}

	re := regexp.MustCompile(`<script type="application/json" class="chart-meta">([^<]+)</script>`)
	m := re.FindStringSubmatch(html)
	if len(m) < 2 {
		t.Fatalf("failed to locate chart-meta JSON payload\n%s", html)
	}
	var payload chartMetaPayload
	if err := json.Unmarshal([]byte(m[1]), &payload); err != nil {
		t.Fatalf("invalid chart-meta JSON: %v\nraw=%s", err, m[1])
	}
	if len(payload.Series) != 2 || payload.Series[0].Values[1] != nil || payload.Series[0].Formatted[1] != "-" {
		t.Fatalf("unexpected chart-meta payload for NaN point: %+v", payload)
	}
}

func TestReportCSSPreventsHorizontalClipping(t *testing.T) {
	css := reportCSS()
	for _, want := range []string{
		"overflow-x:clip",
		"overflow-wrap:anywhere",
		"min-width:0",
		".mini-table { min-width:0;",
		".chart:not([data-scale=\"log\"]) .scale-log { display:none; }",
		".chart[data-scale=\"log\"] .scale-linear { display:none; }",
	} {
		if !strings.Contains(css, want) {
			t.Fatalf("reportCSS missing layout/clipping guard %q", want)
		}
	}
	if strings.Contains(css, "100vw") {
		t.Fatalf("reportCSS should avoid 100vw scrollbar overflow")
	}
}

func TestBenchmarkContextBannerRendersAndOmitsGracefully(t *testing.T) {
	fullHTML := renderHTML(reportData{
		Config: config{Title: "banner test", RunRoot: t.TempDir()},
		RunMetadata: runMetadata{
			"tier":                       "full",
			"mongo_treedb_open_mode":     "cached",
			"mongo_treedb_profile":       "command_wal_relaxed",
			"mongo_treedb_memtable_mode": "auto",
			"mongo_mode":                 "docker",
			"mongo_image":                "mongo:8.0",
			"mongo_write_concern":        "w:1",
		},
		Collections: []collectionRow{
			{ConfigName: "sqlite_native_columns_0_indexes", Engine: "sqlite_wal_normal", Format: "native-columns", IndexCount: 0, Phase: "sqlite_vacuum", BytesPerDoc: 20},
		},
	})
	for _, want := range []string{
		"context-banner",
		"Benchmark Configuration &amp; Durability Contract",
		"TreeDB open mode:",
		"cached",
		"TreeDB durability:",
		"command_wal_relaxed",
		"tone-relaxed",
		"mongo:8.0 (docker)",
		"MongoDB write concern:",
		"w:1",
		"SQLite journal/sync:",
		"WAL / NORMAL (sqlite_wal_normal)",
	} {
		if !strings.Contains(fullHTML, want) {
			t.Fatalf("context banner missing %q\n%s", want, fullHTML)
		}
	}

	durableHTML := renderHTML(reportData{
		Config:      config{Title: "durable test", RunRoot: t.TempDir()},
		RunMetadata: runMetadata{"mongo_treedb_profile": "command_wal_durable"},
	})
	if !strings.Contains(durableHTML, "tone-durable") || !strings.Contains(durableHTML, "command_wal_durable") {
		t.Fatalf("durable profile should render tone-durable badge\n%s", durableHTML)
	}

	fastHTML := renderHTML(reportData{
		Config:      config{Title: "fast test", RunRoot: t.TempDir()},
		RunMetadata: runMetadata{"mongo_treedb_profile": "no_wal_fast"},
	})
	if !strings.Contains(fastHTML, "tone-fast") || !strings.Contains(fastHTML, "no_wal_fast") {
		t.Fatalf("no_wal_fast profile should render tone-fast badge\n%s", fastHTML)
	}

	// Minimal fixture without RUNBOOK.md or durability metadata omits banner cleanly.
	minimalHTML := renderHTML(reportData{
		Config: config{Title: "minimal", RunRoot: t.TempDir()},
		MongoLoadModes: []loadModeRow{
			{Indexes: 0, Target: "treedb", Config: "treedb_bson_driver", OpsPerSec: 1000},
		},
	})
	if strings.Contains(minimalHTML, "class=\"context-banner\"") {
		t.Fatalf("minimal report without durability metadata should omit context banner\n%s", minimalHTML)
	}
}

func TestBenchmarkContextBannerDurabilityTiers(t *testing.T) {
	// Fast tier parses RUNBOOK keys and shows Mongo 8 --nojournal caveat + SQLite WAL/OFF.
	fastRoot := t.TempDir()
	writeFile(t, filepath.Join(fastRoot, "RUNBOOK.md"), `# Full TreeDB Benchmark Report Run

- tier: pr
- durability_tier: fast
- collection_treedb_profile: no_wal_fast
- mongo_treedb_profile: no_wal_fast
- mongo_treedb_open_mode: standalone
- mongo_image: mongo:8
- mongo_mode: docker
- mongo_write_concern: w1
- sqlite_journal_mode: WAL
- sqlite_synchronous: OFF
- durability_note: fast tier: TreeDB no_wal_fast, SQLite WAL+OFF; MongoDB 8 removed --nojournal, so w:1 (j:false) is MongoDB 8's fastest acknowledged write mode
`)
	fastMeta := loadRunMetadata(filepath.Join(fastRoot, "RUNBOOK.md"))
	fastHTML := renderHTML(reportData{
		Config:      config{Title: "fast durability tier", RunRoot: fastRoot},
		RunMetadata: fastMeta,
		Collections: []collectionRow{
			{ConfigName: "sqlite_native_columns_0_indexes", Engine: "sqlite_wal_off", Format: "native-columns", IndexCount: 0, Phase: "sqlite_vacuum", BytesPerDoc: 20},
		},
	})
	for _, want := range []string{
		"Durability tier:",
		"fast",
		"tone-fast",
		"no_wal_fast",
		"w:1, j:false (Mongo 8 fastest ack; --nojournal removed)",
		"WAL / OFF",
		"context-note",
		"MongoDB 8 removed --nojournal",
	} {
		if !strings.Contains(fastHTML, want) {
			t.Fatalf("fast durability tier banner missing %q\n%s", want, fastHTML)
		}
	}

	// Durable tier renders tone-durable badges for TreeDB command_wal_durable, Mongo w1_j, and SQLite WAL/FULL.
	durableHTML := renderHTML(reportData{
		Config: config{Title: "durable tier", RunRoot: t.TempDir()},
		RunMetadata: runMetadata{
			"tier":                      "large",
			"durability_tier":           "durable",
			"collection_treedb_profile": "command_wal_durable",
			"mongo_treedb_profile":      "command_wal_durable",
			"mongo_write_concern":       "w1_j",
			"sqlite_journal_mode":       "WAL",
			"sqlite_synchronous":        "FULL",
		},
		Collections: []collectionRow{
			{ConfigName: "sqlite_native_columns_0_indexes", Engine: "sqlite_wal_full", Format: "native-columns", IndexCount: 0, Phase: "sqlite_vacuum", BytesPerDoc: 20},
		},
	})
	for _, want := range []string{
		"Durability tier:",
		"durable",
		"tone-durable",
		"command_wal_durable",
		"w:1, j:true (w1_j)",
		"WAL / FULL",
	} {
		if !strings.Contains(durableHTML, want) {
			t.Fatalf("durable tier banner missing %q\n%s", want, durableHTML)
		}
	}
	if got := durabilityTone("w1_j"); got != "durable" {
		t.Fatalf("durabilityTone(w1_j)=%q want durable", got)
	}
	if got := durabilityTone("majority_j"); got != "durable" {
		t.Fatalf("durabilityTone(majority_j)=%q want durable", got)
	}
	if got := durabilityTone("w:1, j:false (Mongo 8 fastest ack; --nojournal removed)"); got != "relaxed" {
		t.Fatalf("durabilityTone(Mongo 8 fastest ack)=%q want relaxed", got)
	}
	for _, eng := range []string{"sqlite_wal_off", "sqlite_wal_normal", "sqlite_wal_full", "sqlite_delete_full"} {
		if got := collectionEngineFamily(collectionRow{Engine: eng}); got != "SQLite" {
			t.Fatalf("collectionEngineFamily(%q)=%q want SQLite", eng, got)
		}
	}
}

func TestFormatExactChartValueBytesPreservesExactIntegerCount(t *testing.T) {
	if got, want := formatExactChartValue(512, "bytes"), "512 B"; got != want {
		t.Fatalf("512 bytes = %q, want %q", got, want)
	}
	got1024 := formatExactChartValue(1024, "bytes")
	got1025 := formatExactChartValue(1025, "bytes")
	if got1024 != "1,024 B (1.00 KiB)" {
		t.Fatalf("1024 bytes = %q, want %q", got1024, "1,024 B (1.00 KiB)")
	}
	if got1025 != "1,025 B (1.00 KiB)" {
		t.Fatalf("1025 bytes = %q, want %q", got1025, "1,025 B (1.00 KiB)")
	}
	if got1024 == got1025 {
		t.Fatalf("1024 and 1025 bytes should remain distinguishable in exact value formatting")
	}
	// Axis ticks and bar labels stay compact.
	if got, want := formatAxisTick(1024*1024, "bytes"), "1 MiB"; got != want {
		t.Fatalf("axis tick 1 MiB = %q, want %q", got, want)
	}
	if got, want := formatBarLabelValue(1024*1024, "bytes"), "1M"; got != want {
		t.Fatalf("bar label 1 MiB = %q, want %q", got, want)
	}
}

func TestChartItemsKeyboardAccessibleWithFocusAndEscapeHandlers(t *testing.T) {
	barHTML := compactVerticalBarChart("Bytes/doc", []string{"post insert"}, "phase", []chartSeries{
		{Name: "TreeDB BSON", Values: []float64{1025}, Color: "#7c3aed"},
	}, "bytes")
	lineHTML := lineChart("Footprint", []string{"0", "1"}, "indexes", "bytes", []chartSeries{
		{Name: "TreeDB", Values: []float64{1024, 1025}, Color: "#2563eb"},
	}, "bytes")
	loadHTML := loadModeBarChart("Load", []string{"BSON driver", "BSON direct"}, []float64{1024, 2048}, []float64{1025, 0}, "bytes", "Bytes")
	rawHTML := rawEngineGroupedBarChart("Raw", []rawEngineRun{{Profile: "fast", Checkpoint: "no checkpoint between tests", Results: map[string]float64{"random_read": 5000}}}, []string{"random_read"}, []rawEngineVariant{{Profile: "fast", Checkpoint: "no checkpoint between tests", Label: "fast / no checkpoint between tests", Color: "#d97706"}})

	for name, doc := range map[string]string{"bar": barHTML, "line": lineHTML, "load": loadHTML, "raw": rawHTML} {
		for _, want := range []string{
			`class="chart-item" tabindex="0" role="graphics-symbol" aria-label="`,
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s chart missing keyboard-accessible chart-item attributes %q\n%s", name, want, doc)
			}
		}
	}
	if !strings.Contains(barHTML, "1,025 B (1.00 KiB)") {
		t.Fatalf("bar chart exact values missing exact integer byte count\n%s", barHTML)
	}

	js := reportInteractiveJS()
	for _, want := range []string{
		"addEventListener('focusin'",
		"addEventListener('focusout'",
		"addEventListener('keydown'",
		"ev.key === 'Escape'",
		"aria-describedby",
		"positionTipForElement",
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("reportInteractiveJS missing keyboard tooltip handler %q", want)
		}
	}
}
