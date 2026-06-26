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
