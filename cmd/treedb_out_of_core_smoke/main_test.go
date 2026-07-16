package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestValidateSmokeRunRequiresShapeLabel(t *testing.T) {
	run := smokeRunWithRows([]resultRow{{
		ConfigName:      "treedb_raw",
		WorkloadName:    "raw_batch_write",
		Engine:          "treedb",
		ItemCount:       10,
		TotalBytes:      ptrUint(1024),
		BytesPerItem:    ptrFloatForTest(102.4),
		Budgets:         testBudgets(),
		PressureClaimed: true,
		Mmap:            &mmapStats{},
	}})
	checks := validateSmokeRun(&run)
	if !hasCheck(checks, "error", "missing_shape_label") {
		t.Fatalf("expected missing_shape_label error, got %#v", checks)
	}
	if hasCheck(checks, "error", "invalid_shape_label") {
		t.Fatalf("empty shape should not also report invalid_shape_label: %#v", checks)
	}
}

func TestParseProfileRejectsDeprecatedProfileNames(t *testing.T) {
	if got, err := parseProfile("bench_unsafe"); err != nil || got != treedb.ProfileBenchUnsafe {
		t.Fatalf("parseProfile bench_unsafe = %q err=%v", got, err)
	}
	for _, raw := range []string{"fast", "wal_on_fast", "walonfast", "durable", "legacy_wal_durable", "legacy_wal_relaxed_fast", "bench", "production_fast", "production_wal_on_fast"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parseProfile(raw)
			if err == nil {
				t.Fatal("parseProfile succeeded, want error")
			}
			if !strings.Contains(err.Error(), treedb.BenchmarkProfileFlagHelp) {
				t.Fatalf("error=%v, want profile help", err)
			}
		})
	}
}

func TestValidateSmokeRunRequiresItemCountForBytesPerItem(t *testing.T) {
	run := smokeRunWithRows([]resultRow{{
		ConfigName:      "treedb_raw",
		WorkloadName:    "raw_batch_write",
		Engine:          "treedb",
		Shape:           "raw",
		TotalBytes:      ptrUint(1024),
		BytesPerItem:    ptrFloatForTest(102.4),
		Budgets:         testBudgets(),
		PressureClaimed: true,
		Mmap:            &mmapStats{},
	}})
	checks := validateSmokeRun(&run)
	if !hasCheck(checks, "error", "missing_item_count") {
		t.Fatalf("expected missing_item_count error, got %#v", checks)
	}
	if !hasCheck(checks, "error", "bytes_per_item_without_count") {
		t.Fatalf("expected bytes_per_item_without_count error, got %#v", checks)
	}
}

func TestValidateSmokeRunWarnsWhenDatasetDoesNotExceedBudget(t *testing.T) {
	run := smokeRunWithRows([]resultRow{validRow("treedb_raw", "raw", 100, 1024)})
	checks := validateSmokeRun(&run)
	if !hasCheck(checks, "warning", "dataset_not_larger_than_budget") {
		t.Fatalf("expected dataset_not_larger_than_budget warning, got %#v", checks)
	}
}

func TestValidateSmokeRunLargeBudgetComparisonDoesNotOverflow(t *testing.T) {
	row := validRow("treedb_raw", "raw", 100, 1)
	row.Budgets.CacheBudgetBytes = 1 << 62
	row.Budgets.RetiredMmapBudgetBytes = 1
	run := smokeRunWithRows([]resultRow{row})
	checks := validateSmokeRun(&run)
	if !hasCheck(checks, "warning", "dataset_not_larger_than_budget") {
		t.Fatalf("expected dataset_not_larger_than_budget warning for huge budget, got %#v", checks)
	}
}

func TestValidateSmokeRunWarnsOnRetiredMmapBudgetOverflow(t *testing.T) {
	row := validRow("treedb_raw", "raw", 100, 1<<20)
	row.Mmap.DeadBytes = 128 << 10
	run := smokeRunWithRows([]resultRow{row})
	checks := validateSmokeRun(&run)
	if !hasCheck(checks, "warning", "retired_mmap_budget_exceeded") {
		t.Fatalf("expected retired_mmap_budget_exceeded warning, got %#v", checks)
	}
}

func TestRenderMarkdownAndJSONPreserveShapeLabels(t *testing.T) {
	run := smokeRunWithRows([]resultRow{
		validRow("treedb_raw", "raw", 100, 1<<20),
		validRow("treedb_template_v1_collection_2_indexes", "collection", 100, 2<<20),
	})
	run.Checks = validateSmokeRun(&run)
	md := renderMarkdown(&run)
	for _, want := range []string{
		"`treedb_raw` | `raw`",
		"`treedb_template_v1_collection_2_indexes` | `collection`",
		"make bench-out-of-core-smoke",
		"`cache_budget_bytes`",
		"`retired_mmap_budget_bytes`",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n%s", want, md)
		}
	}
	data, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"shape":"raw"`) || !strings.Contains(string(data), `"shape":"collection"`) {
		t.Fatalf("json did not preserve shape labels: %s", data)
	}
}

func TestMongoGatewayResultRowLabelsOptInWorkload(t *testing.T) {
	cfg := testConfig()
	summary := mongoGatewaySummary{
		Target:               "treedb",
		TreeDBDir:            "/tmp/treedb-mongo",
		Documents:            100,
		BatchSize:            25,
		SecondaryIndexes:     2,
		TreeDBDocumentFormat: "bson",
		Phases: []mongoGatewayPhase{{
			Name:           "load_insert_many",
			Operations:     100,
			OpsPerSecond:   5000,
			SampledNsPerOp: 200000,
		}},
		TreeDBDiskAfterCheckpoint: &mongoGatewayDiskUsage{
			TotalBytes: 6400,
			Paths: map[string]int64{
				"format.json": 64,
				"index.db":    1024,
			},
		},
		TreeDBStatsFinal: map[string]string{
			"treedb.vlog.mmap_read.hits":            "3",
			"treedb.vlog.mmap_read.fallback_readat": "1",
		},
	}
	row := mongoGatewayResultRow(cfg, summary, "/tmp/mongo.json")
	if row.Shape != "mongo" || row.Engine != "treedb" {
		t.Fatalf("row shape/engine = %s/%s, want mongo/treedb", row.Shape, row.Engine)
	}
	if row.ConfigName != "treedb_mongo_gateway_bson_2_indexes" {
		t.Fatalf("config name = %s", row.ConfigName)
	}
	if row.TotalBytes == nil || *row.TotalBytes != 6400 {
		t.Fatalf("total bytes = %v", row.TotalBytes)
	}
	if row.BytesPerItem == nil || *row.BytesPerItem != 64 {
		t.Fatalf("bytes per item = %v", row.BytesPerItem)
	}
	if row.Mmap == nil || row.Mmap.Hits != 3 || row.Mmap.FallbackReadAt != 1 {
		t.Fatalf("mmap stats = %#v", row.Mmap)
	}
	run := smokeRunWithRows([]resultRow{row})
	checks := validateSmokeRun(&run)
	if hasErrors(checks) {
		t.Fatalf("mongo row should pass guardrail errors, got %#v", checks)
	}
}

func TestMongoGatewayResultRowPreservesZeroIndexShape(t *testing.T) {
	cfg := testConfig()
	summary := mongoGatewaySummary{
		Target:               "treedb",
		Documents:            100,
		BatchSize:            25,
		SecondaryIndexes:     0,
		TreeDBDocumentFormat: "bson",
		Phases: []mongoGatewayPhase{{
			Name:         "load_insert_many",
			Operations:   100,
			OpsPerSecond: 5000,
		}},
		TreeDBDiskAfterCheckpoint: &mongoGatewayDiskUsage{TotalBytes: 6400},
		TreeDBStatsFinal: map[string]string{
			"treedb.vlog.mmap_read.hits": "3",
		},
	}
	row := mongoGatewayResultRow(cfg, summary, "/tmp/mongo.json")
	if row.IndexCount != 0 || row.ConfigName != "treedb_mongo_gateway_bson_0_indexes" {
		t.Fatalf("row index shape = %d/%s, want 0-index config", row.IndexCount, row.ConfigName)
	}
}

func TestRawDiskUsageForPhasePrefersPhaseSnapshot(t *testing.T) {
	summary := rawWorkerSummary{
		DiskUsageFinal: diskUsage{TotalBytes: 300},
		DiskUsageByPhase: map[string]diskUsage{
			"post_insert":    {TotalBytes: 100, TopFiles: []fileSummary{{Path: "index.db", Bytes: 80}}},
			"post_overwrite": {TotalBytes: 200},
		},
	}
	insert := rawDiskUsageForPhase(summary, "post_insert")
	if insert.TotalBytes != 100 {
		t.Fatalf("post_insert bytes=%d want 100", insert.TotalBytes)
	}
	components := componentBytesFromDiskUsage(insert)
	if components["index.db"] != 80 {
		t.Fatalf("components=%v want index.db=80", components)
	}
	if got := rawDiskUsageForPhase(summary, "unknown").TotalBytes; got != 300 {
		t.Fatalf("fallback bytes=%d want final 300", got)
	}
}

func TestPrepareRunDirRequiresSentinelAndExplicitReuse(t *testing.T) {
	nonSmoke := t.TempDir()
	if err := os.WriteFile(filepath.Join(nonSmoke, "unrelated.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := prepareRunDir(nonSmoke, true); err == nil {
		t.Fatalf("expected non-smoke non-empty dir to be rejected")
	}

	smokeDir := t.TempDir()
	if err := prepareRunDir(smokeDir, false); err != nil {
		t.Fatalf("prepare empty smoke dir: %v", err)
	}
	if err := prepareRunDir(smokeDir, false); err == nil {
		t.Fatalf("expected existing smoke dir to require -allow-existing-run-dir")
	}
	if err := prepareRunDir(smokeDir, true); err != nil {
		t.Fatalf("allow existing smoke dir: %v", err)
	}
}

func TestCommandLinePreservesWrapperDisplayString(t *testing.T) {
	raw := `./scripts/bench_out_of_core_smoke.sh -out-dir /tmp/has\ space`
	t.Setenv(envCommandLine, raw)
	got := commandLine()
	if len(got) != 1 || got[0] != raw {
		t.Fatalf("commandLine=%q, want single preserved display string %q", got, raw)
	}
}

func testConfig() config {
	return config{
		FormatsCSV:             "template-v1,bson,json",
		IndexesCSV:             "0,1,2",
		formats:                []string{"template-v1", "bson", "json"},
		indexes:                []int{0, 1, 2},
		BatchSize:              10,
		CollectionDocs:         100,
		CacheBudgetBytes:       32 << 10,
		RetiredMmapBudgetBytes: 32 << 10,
		MaxDeadMappings:        2,
	}
}

func smokeRunWithRows(rows []resultRow) smokeRun {
	return smokeRun{
		SchemaVersion: schemaVersion,
		GeneratedAt:   "2026-05-01T00:00:00Z",
		RunDir:        "/tmp/smoke",
		Worktree:      "/repo",
		Branch:        "test",
		Commit:        "deadbeef",
		CommandLine:   []string{"./scripts/bench_out_of_core_smoke.sh"},
		Config: smokeConfig{
			RawKeys:        100,
			CollectionDocs: 100,
			BatchSize:      10,
			Formats:        []string{"template-v1"},
			Indexes:        []int{0, 1, 2},
			Budgets:        testBudgets(),
		},
		Results: rows,
	}
}

func validRow(configName, shape string, items int, total uint64) resultRow {
	return resultRow{
		ConfigName:      configName,
		WorkloadName:    "workload",
		Engine:          "treedb",
		Shape:           shape,
		ItemCount:       items,
		TotalBytes:      &total,
		BytesPerItem:    bytesPer(total, items),
		Budgets:         testBudgets(),
		PressureClaimed: true,
		Mmap:            &mmapStats{Hits: 1, DeadMappingCap: 2},
	}
}

func testBudgets() budgets {
	return budgets{
		CacheBudgetBytes:       32 << 10,
		RetiredMmapBudgetBytes: 32 << 10,
		MaxDeadMappings:        2,
	}
}

func hasCheck(checks []guardrailCheck, severity, code string) bool {
	for _, check := range checks {
		if check.Severity == severity && check.Code == code {
			return true
		}
	}
	return false
}

func ptrUint(v uint64) *uint64 {
	return &v
}

func ptrFloatForTest(v float64) *float64 {
	return &v
}
