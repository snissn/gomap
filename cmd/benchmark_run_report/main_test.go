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
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"offline_rewrite","maintenance_mode":"offline_rewrite","total_bytes":700,"bytes_per_doc":7,"measurement_kind":"fixture","extra":{"index_db_bytes":"400","leaf_vlog_bytes":"300"}},
    {"config_name":"treedb_template_v1_collection_0_indexes","engine":"treedb_fast","format":"template-v1","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":500,"bytes_per_doc":5,"measurement_kind":"fixture"},
    {"config_name":"treedb_bson_collection_0_indexes","engine":"treedb_fast","format":"bson","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":800,"bytes_per_doc":8,"docs_per_sec":20000,"measurement_kind":"go_benchmark"},
    {"config_name":"treedb_bson_collection_0_indexes","engine":"treedb_fast","format":"bson","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":400,"bytes_per_doc":4,"measurement_kind":"fixture"},
    {"config_name":"treedb_json_collection_0_indexes","engine":"treedb_fast","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"post_insert","maintenance_mode":"none","total_bytes":1200,"bytes_per_doc":12,"docs_per_sec":8000,"measurement_kind":"go_benchmark"},
    {"config_name":"treedb_json_collection_0_indexes","engine":"treedb_fast","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"full_leafgen_pack_gc","maintenance_mode":"full_leafgen_pack_gc","total_bytes":600,"bytes_per_doc":6,"measurement_kind":"fixture"},
    {"config_name":"sqlite_native_columns_0_indexes","engine":"sqlite_wal_normal","format":"native-columns","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":2000,"bytes_per_doc":20,"measurement_kind":"fixture","maintenance_stats":{"sqlite_vacuum_ops_per_sec":123},"extra":{"sqlite_vacuum_bytes_before":"2500","sqlite_vacuum_bytes_after":"2000","sqlite_vacuum_bytes_delta":"500"}},
    {"config_name":"sqlite_json_0_indexes","engine":"sqlite_wal_normal","format":"json","shape":"collection","index_count":0,"document_count":100,"phase":"sqlite_vacuum","maintenance_mode":"sqlite_vacuum","total_bytes":3000,"bytes_per_doc":30,"measurement_kind":"fixture"}
  ],
  "comparisons": [
    {"comparison_name":"tree_vs_sqlite","treedb_config_name":"treedb_template_v1_collection_0_indexes","treedb_phase":"full_leafgen_pack_gc","sqlite_config_name":"sqlite_native_columns_0_indexes","sqlite_phase":"sqlite_vacuum","treedb_bytes_per_doc":5,"sqlite_bytes_per_doc":20,"smaller_ratio":4,"comparison_basis":"test"}
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
		"Mongo API Full Sweep",
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
	if strings.Contains(html, "<details open") {
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

func TestCollectionChartsIncludeAdditionalFormats(t *testing.T) {
	html := renderHTML(reportData{
		Config: config{Title: "extra formats", RunRoot: t.TempDir()},
		Collections: []collectionRow{
			{ConfigName: "treedb_msgpack_collection_0_indexes", Engine: "treedb_fast", Format: "msgpack", Shape: "collection", IndexCount: 0, Phase: "post_insert", DocsPerSec: 1234},
			{ConfigName: "treedb_msgpack_collection_0_indexes", Engine: "treedb_fast", Format: "msgpack", Shape: "collection", IndexCount: 0, Phase: "full_leafgen_pack_gc", BytesPerDoc: 12.3},
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

func TestCollectionDiskRowsUseBestCompactedTreeDBPhase(t *testing.T) {
	rows := []collectionRow{
		{Shape: "collection", Engine: "treedb_fast", ConfigName: "treedb_bson_collection_0_indexes", Format: "bson", IndexCount: 0, Phase: "full_leafgen_pack_gc", BytesPerDoc: 30},
		{Shape: "collection", Engine: "treedb_fast", ConfigName: "treedb_bson_collection_0_indexes", Format: "bson", IndexCount: 0, Phase: "offline_compact", BytesPerDoc: 20},
		{Shape: "collection", Engine: "treedb_fast", ConfigName: "treedb_bson_collection_0_indexes", Format: "bson", IndexCount: 0, Phase: "post_insert", BytesPerDoc: 80},
	}

	diskRows := collectionDiskRows(rows)
	if len(diskRows.Series) != 1 || len(diskRows.Series[0].Values) != 1 {
		t.Fatalf("unexpected disk rows: %+v", diskRows)
	}
	if got, want := diskRows.Series[0].Values[0], 20.0; got != want {
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
	})
	if got, want := rows.Categories, []string{"2 indexes"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("categories = %v, want %v", got, want)
	}
	if len(rows.Series) != 1 || len(rows.Series[0].Values) != 1 {
		t.Fatalf("unexpected ratio series: %+v", rows.Series)
	}
	if got, want := rows.Series[0].Values[0], 7.0; got != want {
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
