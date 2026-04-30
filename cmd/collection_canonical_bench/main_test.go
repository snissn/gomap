package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalReportKnownCompressionShape(t *testing.T) {
	canon := knownExampleRun()
	canon.Checks = validateCanonicalRun(canon)
	md := renderMarkdownReport(canon)

	required := []string{
		"| `command_line` | `./scripts/bench_collections_canonical.sh -docs 100000` |",
		"44.3 B/doc via PR 1096-style offline rewrite and 31.7 B/doc via full leafgen pack/GC",
		"offline rewrite is about 3.5x smaller than SQLite native columns and 5.2x smaller than SQLite JSON",
		"full leafgen pack/GC is about 4.9x and 7.3x smaller, respectively",
		"`online_one_pass_maintenance`",
		"Do not compare TreeDB `offline_rewrite` or `full_leafgen_pack_gc` only against SQLite `post_insert` rows.",
		"`treedb_template_v1_collection_2_indexes` | `offline_rewrite` | 44.3",
		"`treedb_template_v1_collection_2_indexes` | `full_leafgen_pack_gc` | 31.7",
		"make bench-collections-canonical",
	}
	for _, want := range required {
		if !strings.Contains(md, want) {
			t.Fatalf("report missing %q\n%s", want, md)
		}
	}
	for _, check := range canon.Checks {
		if check.Severity == "error" {
			t.Fatalf("unexpected guardrail error: %+v", check)
		}
	}
	if strings.Contains(md, "online_one_pass_maintenance` | Full") ||
		strings.Contains(md, "online_one_pass_maintenance` | full") {
		t.Fatalf("online one-pass maintenance appears to be labeled as full compaction\n%s", md)
	}
	fairSection := markdownSection(md, "## Fair Compacted-State Comparison", "## Maintenance/Compaction Details")
	if strings.Contains(fairSection, "treedb_template_v1_raw") {
		t.Fatalf("fair compacted comparison must not include raw TreeDB rows\n%s", fairSection)
	}
}

func TestPrepareRunDirRemovesPriorCanonicalArtifacts(t *testing.T) {
	dir := t.TempDir()
	staleReport := filepath.Join(dir, "timed_matrix", "stale", "collections_report.json")
	if err := os.MkdirAll(filepath.Dir(staleReport), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staleReport, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	keepPath := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(keepPath, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := prepareRunDir(config{OutDir: dir}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "timed_matrix")); !os.IsNotExist(err) {
		t.Fatalf("expected stale timed_matrix to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "logs")); err != nil {
		t.Fatalf("expected fresh logs directory: %v", err)
	}
	if _, err := os.Stat(keepPath); err != nil {
		t.Fatalf("non-canonical files should be preserved: %v", err)
	}
}

func TestOfflineRewriteIndexArgsIncludesConfiguredShape(t *testing.T) {
	tests := []struct {
		configured int
		want       string
	}{
		{configured: 0, want: "0,1,2"},
		{configured: 2, want: "0,1,2"},
		{configured: 3, want: "0,1,2,3"},
	}
	for _, tt := range tests {
		got := strings.Join(offlineRewriteIndexArgs(tt.configured), ",")
		if got != tt.want {
			t.Fatalf("offlineRewriteIndexArgs(%d) = %q, want %q", tt.configured, got, tt.want)
		}
	}
}

func TestGuardrailRequiresSQLiteVacuumForCompactedComparison(t *testing.T) {
	canon := knownExampleRun()
	var filtered []resultRow
	for _, row := range canon.Results {
		if row.ConfigName == "sqlite_native_columns_2_indexes" && row.Phase == phaseSQLiteVacuum {
			continue
		}
		filtered = append(filtered, row)
	}
	canon.Results = filtered
	checks := validateCanonicalRun(canon)
	if !hasErrorCheck(checks) {
		t.Fatalf("expected guardrail error, got %#v", checks)
	}
	var saw bool
	for _, check := range checks {
		if check.Code == "missing_sqlite_native_vacuum" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("expected missing_sqlite_native_vacuum check, got %#v", checks)
	}
}

func TestCanonicalDerivedCompactedComparisons(t *testing.T) {
	canon := knownExampleRun()
	comparisons := buildCompactedComparisons(canon)
	cases := []struct {
		phase        string
		sqliteConfig string
		wantRatio    float64
	}{
		{phaseOfflineRewrite, "sqlite_native_columns_2_indexes", 156.7 / 44.3},
		{phaseFullLeafgenPackGC, "sqlite_native_columns_2_indexes", 156.7 / 31.7},
		{phaseOfflineRewrite, "sqlite_json_2_indexes", 231.7 / 44.3},
		{phaseFullLeafgenPackGC, "sqlite_json_2_indexes", 231.7 / 31.7},
	}
	for _, tc := range cases {
		got := findComparison(comparisons, "treedb_template_v1_collection_2_indexes", tc.phase, tc.sqliteConfig, phaseSQLiteVacuum)
		if got == nil {
			t.Fatalf("missing comparison for %s vs %s", tc.phase, tc.sqliteConfig)
		}
		if diff := got.SmallerRatio - tc.wantRatio; diff < -0.0001 || diff > 0.0001 {
			t.Fatalf("ratio for %s vs %s = %f, want %f", tc.phase, tc.sqliteConfig, got.SmallerRatio, tc.wantRatio)
		}
	}
}

func TestCanonicalDerivedCompactedComparisonsUseConfiguredIndexCount(t *testing.T) {
	canon := knownExampleRun()
	canon.Config.Indexes = 1
	canon.Results = append(canon.Results,
		testStorageRow("sqlite_json_1_indexes", "sqlite_wal_normal", "json", "collection", phaseSQLiteVacuum, 1, 18000000, 180.0),
		testStorageRow("sqlite_native_columns_1_indexes", "sqlite_wal_normal", "native-columns", "collection", phaseSQLiteVacuum, 1, 12000000, 120.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseFullLeafgenPackGC, 1, 2800000, 28.0),
	)
	finalizeRunMetadata(canon)

	comparisons := buildCompactedComparisons(canon)
	if got := findComparison(comparisons, "treedb_template_v1_collection_1_indexes", phaseOfflineRewrite, "sqlite_native_columns_1_indexes", phaseSQLiteVacuum); got == nil {
		t.Fatalf("missing configured one-index comparison, got %#v", comparisons)
	}
	if got := findComparison(comparisons, "treedb_template_v1_collection_2_indexes", phaseOfflineRewrite, "sqlite_native_columns_2_indexes", phaseSQLiteVacuum); got != nil {
		t.Fatalf("unexpected hardcoded two-index comparison: %#v", got)
	}
}

func TestGuardrailAllowsDetachedHeadBranchMetadata(t *testing.T) {
	canon := knownExampleRun()
	canon.Branch = ""
	finalizeRunMetadata(canon)

	checks := validateCanonicalRun(canon)
	for _, check := range checks {
		if check.Code == "missing_row_run_metadata" {
			t.Fatalf("detached-head branch metadata should be allowed, got %#v", checks)
		}
	}
}

func TestExecutiveSummarySkipsZeroBytesPerDocRatios(t *testing.T) {
	canon := knownExampleRun()
	for i := range canon.Results {
		if canon.Results[i].ConfigName == "treedb_template_v1_collection_2_indexes" && canon.Results[i].Phase == phaseOfflineRewrite {
			canon.Results[i].BytesPerDoc = floatPtr(0)
			break
		}
	}

	summary := renderExecutiveSummary(canon)
	if strings.Contains(summary, "+Inf") || strings.Contains(summary, "NaN") {
		t.Fatalf("summary should not emit invalid ratios: %s", summary)
	}
	if !strings.Contains(summary, "fair compacted-state headline could not be generated") {
		t.Fatalf("summary should fall back when ratio inputs are unusable: %s", summary)
	}
}

func TestExecutiveSummaryUsesConfiguredIndexCountLabel(t *testing.T) {
	canon := knownExampleRun()
	canon.Config.Indexes = 1
	canon.Results = append(canon.Results,
		testStorageRow("sqlite_json_1_indexes", "sqlite_wal_normal", "json", "collection", phaseSQLiteVacuum, 1, 18000000, 180.0),
		testStorageRow("sqlite_native_columns_1_indexes", "sqlite_wal_normal", "native-columns", "collection", phaseSQLiteVacuum, 1, 12000000, 120.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseFullLeafgenPackGC, 1, 2800000, 28.0),
	)
	finalizeRunMetadata(canon)

	summary := renderExecutiveSummary(canon)
	if !strings.Contains(summary, "fully compacted one-index template-v1 collection storage") {
		t.Fatalf("summary did not use configured index count: %s", summary)
	}
	if strings.Contains(summary, "two-index") {
		t.Fatalf("summary should not hardcode two-index for one-index run: %s", summary)
	}
}

func TestGuardrailRespectsSkipSQLite(t *testing.T) {
	canon := knownExampleRun()
	canon.Config.SkipSQLite = true
	var filtered []resultRow
	for _, row := range canon.Results {
		if strings.HasPrefix(row.ConfigName, "sqlite_") {
			continue
		}
		filtered = append(filtered, row)
	}
	canon.Results = filtered
	canon.Comparisons = buildCompactedComparisons(canon)
	finalizeRunMetadata(canon)

	checks := validateCanonicalRun(canon)
	for _, check := range checks {
		if check.Code == "missing_sqlite_json_vacuum" ||
			check.Code == "missing_sqlite_native_vacuum" ||
			check.Code == "missing_compacted_ratio" {
			t.Fatalf("skip-sqlite should not fail SQLite comparison guardrails, got %#v", checks)
		}
	}
	assertCheckCode(t, checks, "sqlite.skipped")
}

func TestGuardrailRejectsBadCompactedComparisonBasis(t *testing.T) {
	canon := knownExampleRun()
	canon.Comparisons = append(canon.Comparisons, comparisonRow{
		ComparisonName:    "bad",
		TreeDBConfigName:  "treedb_template_v1_collection_2_indexes",
		TreeDBPhase:       phaseFullLeafgenPackGC,
		SQLiteConfigName:  "sqlite_native_columns_2_indexes",
		SQLitePhase:       phasePostInsert,
		TreeDBBytesPerDoc: 31.7,
		SQLiteBytesPerDoc: 176.1,
		SmallerRatio:      176.1 / 31.7,
	})
	checks := validateCanonicalRun(canon)
	assertCheckCode(t, checks, "compacted_compared_to_sqlite_post_insert")
}

func TestGuardrailRequiresDocumentCountForBytesPerDoc(t *testing.T) {
	canon := knownExampleRun()
	canon.Results[0].DocumentCount = 0
	checks := validateCanonicalRun(canon)
	assertCheckCode(t, checks, "missing_document_count")
}

func TestGuardrailRequiresCompactionFlags(t *testing.T) {
	canon := knownExampleRun()
	for i := range canon.Results {
		if canon.Results[i].Phase == phaseFullLeafgenPackGC {
			canon.Results[i].CompactionFlags = nil
			break
		}
	}
	checks := validateCanonicalRun(canon)
	assertCheckCode(t, checks, "missing_compaction_flags")
}

func TestGuardrailRequiresExplicitShapeLabel(t *testing.T) {
	canon := knownExampleRun()
	canon.Results[0].Shape = ""
	checks := validateCanonicalRun(canon)
	assertCheckCode(t, checks, "missing_shape_label")
}

func assertCheckCode(t *testing.T, checks []guardrailCheck, code string) {
	t.Helper()
	for _, check := range checks {
		if check.Code == code {
			return
		}
	}
	t.Fatalf("expected guardrail check %q, got %#v", code, checks)
}

func markdownSection(md, start, end string) string {
	startIdx := strings.Index(md, start)
	if startIdx < 0 {
		return ""
	}
	rest := md[startIdx:]
	endIdx := strings.Index(rest, end)
	if endIdx < 0 {
		return rest
	}
	return rest[:endIdx]
}

func testStorageRow(config, engine, format, shape, phase string, indexes int, bytes int64, bpd float64) resultRow {
	return resultRow{
		ConfigName:      config,
		Engine:          engine,
		Format:          format,
		Shape:           shape,
		IndexCount:      indexes,
		DocumentCount:   100000,
		BenchmarkName:   "test-fixture",
		Phase:           phase,
		MaintenanceMode: phase,
		TotalBytes:      int64Ptr(bytes),
		BytesPerDoc:     floatPtr(bpd),
		BatchSize:       16000,
		MeasurementKind: "test_fixture",
		CompactionFlags: map[string]string{"test": "true"},
	}
}

func knownExampleRun() *canonicalRun {
	docs := 100000
	batch := 16000
	row := func(config, engine, format, shape, phase string, indexes int, bytes int64, bpd float64) resultRow {
		return resultRow{
			ConfigName:      config,
			Engine:          engine,
			Format:          format,
			Shape:           shape,
			IndexCount:      indexes,
			DocumentCount:   docs,
			BenchmarkName:   "known-example",
			Phase:           phase,
			MaintenanceMode: phase,
			TotalBytes:      int64Ptr(bytes),
			BytesPerDoc:     floatPtr(bpd),
			BatchSize:       batch,
			MeasurementKind: "test_fixture",
			CompactionFlags: map[string]string{"test": "true"},
		}
	}
	canon := &canonicalRun{
		SchemaVersion: schemaVersion,
		GeneratedAt:   "2026-04-29T00:00:00Z",
		RunDir:        "/tmp/example",
		Worktree:      "/repo",
		Branch:        "test",
		Commit:        "abcdef",
		CommandLine:   []string{"./scripts/bench_collections_canonical.sh -docs 100000"},
		Config: canonicalConfig{
			Docs:                      docs,
			BatchSize:                 batch,
			Indexes:                   2,
			TreeEngine:                "production_fast",
			Profile:                   "fast",
			Formats:                   []string{"json", "template-v1"},
			LeafSegmentTargetBytes:    1048576,
			LeafgenPackFrameK:         16,
			FullLeafgenForce:          true,
			FullLeafgenMaxGenerations: 0,
			FullLeafgenIndexVacuum:    "offline",
		},
		Artifacts: map[string]string{
			"benchmark_results_json": "benchmark_results.json",
			"benchmark_matrix_csv":   "benchmark_matrix.csv",
		},
		Results: []resultRow{
			{
				ConfigName:      "treedb_template_v1_collection_2_indexes",
				Engine:          "production_fast",
				Format:          "template-v1",
				Shape:           "collection",
				IndexCount:      2,
				DocumentCount:   docs,
				BenchmarkName:   "BenchmarkCollectionShapeInsertBatch/indexes_2",
				Phase:           phasePostInsert,
				MaintenanceMode: "none",
				TotalBytes:      int64Ptr(10555966),
				BytesPerDoc:     floatPtr(105.6),
				DocsPerSec:      floatPtr(584112),
				NsPerDoc:        floatPtr(1712),
				BatchSize:       batch,
				BenchmarkTimed:  true,
				MeasurementKind: "go_benchmark",
			},
			{
				ConfigName:      "treedb_template_v1_collection_2_indexes",
				Engine:          "production_fast",
				Format:          "template-v1",
				Shape:           "collection",
				IndexCount:      2,
				DocumentCount:   docs,
				BenchmarkName:   "BenchmarkCollectionShapeInsertBatch/indexes_2",
				Phase:           phaseOnlineOnePassMaintenance,
				MaintenanceMode: "online_one_pass_vlog_rewrite_then_one_leafgen_pack",
				TotalBytes:      int64Ptr(9923257),
				BytesPerDoc:     floatPtr(99.2),
				BatchSize:       batch,
				MeasurementKind: "benchmark_post_processing",
				CompactionFlags: map[string]string{"leafgen-pack-max-generations": "1"},
			},
			row("treedb_template_v1_raw", "treedb_fast", "template-v1", "raw", phaseOfflineRewrite, 0, 1983120, 19.8),
			row("treedb_template_v1_collection_0_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineRewrite, 0, 1948075, 19.5),
			row("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineRewrite, 1, 3751406, 37.5),
			row("treedb_template_v1_collection_2_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineRewrite, 2, 4434451, 44.3),
			row("treedb_template_v1_collection_2_indexes", "treedb_fast", "template-v1", "collection", phaseFullLeafgenPackGC, 2, 3174681, 31.7),
			row("sqlite_json_2_indexes", "sqlite_wal_normal", "json", "collection", phaseSQLiteVacuum, 2, 23166976, 231.7),
			row("sqlite_native_columns_2_indexes", "sqlite_wal_normal", "native-columns", "collection", phaseSQLiteVacuum, 2, 15671296, 156.7),
		},
	}
	canon.Comparisons = buildCompactedComparisons(canon)
	finalizeRunMetadata(canon)
	return canon
}
