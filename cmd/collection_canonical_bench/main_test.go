package main

import (
	"errors"
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
		"23.2 B/doc via exhaustive compact; production offline compact is 44.3 B/doc",
		"exhaustive compact is about 6.8x smaller than SQLite native columns and 10.0x smaller than SQLite JSON",
		"production offline compact is about 3.5x and 5.2x smaller, respectively",
		"`online_one_pass_maintenance`",
		"## Production Evidence",
		"`command_wal_publish`",
		"Effective concurrency",
		"Use `exhaustive_compact` for byte-minimized benchmark/VACUUM-equivalent claims",
		"`treedb_template_v1_collection_2_indexes` | `offline_compact` | 44.3",
		"`treedb_template_v1_collection_2_indexes` | `exhaustive_compact` | 23.2",
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
	if err := os.WriteFile(filepath.Join(dir, runDirSentinel), []byte(schemaVersion+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
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
	if _, err := os.Stat(filepath.Join(dir, runDirSentinel)); err != nil {
		t.Fatalf("run-dir sentinel should be present: %v", err)
	}
}

func TestRunReportPhaseRecordsWarningAndContinues(t *testing.T) {
	canon := &canonicalRun{}
	called := false

	runReportPhase(canon, "warning", "phase.test.failed", "test phase", func() error {
		called = true
		return errors.New("boom")
	})

	if !called {
		t.Fatal("optional phase was not called")
	}
	if len(canon.Checks) != 1 {
		t.Fatalf("checks = %#v, want one warning", canon.Checks)
	}
	check := canon.Checks[0]
	if check.Severity != "warning" || check.Code != "phase.test.failed" || !strings.Contains(check.Message, "boom") {
		t.Fatalf("unexpected optional phase warning: %#v", check)
	}
}

func TestRunReportPhaseRecordsErrorAndContinues(t *testing.T) {
	canon := &canonicalRun{}

	runReportPhase(canon, "error", "phase.required.failed", "required phase", func() error {
		return errors.New("boom")
	})

	if len(canon.Checks) != 1 {
		t.Fatalf("checks = %#v, want one error", canon.Checks)
	}
	check := canon.Checks[0]
	if check.Severity != "error" || check.Code != "phase.required.failed" || !strings.Contains(check.Message, "boom") {
		t.Fatalf("unexpected required phase error: %#v", check)
	}
}

func TestCanonicalValidationPreservesExistingWarnings(t *testing.T) {
	canon := knownExampleRun()
	canon.Checks = append(canon.Checks, guardrailCheck{
		Severity: "warning",
		Code:     "phase.full_leafgen_pack_gc.failed",
		Message:  "full leafgen pack/GC fixture failed",
	})
	canon.Checks = append(canon.Checks, validateCanonicalRun(canon)...)

	var sawOptionalWarning bool
	for _, check := range canon.Checks {
		if check.Code == "phase.full_leafgen_pack_gc.failed" {
			sawOptionalWarning = true
		}
		if check.Severity == "error" {
			t.Fatalf("unexpected guardrail error: %#v", check)
		}
	}
	if !sawOptionalWarning {
		t.Fatalf("optional phase warning was not preserved: %#v", canon.Checks)
	}
}

func TestCanonicalValidationRejectsWarningOnlyExhaustiveCompactFailure(t *testing.T) {
	canon := knownExampleRun()
	canon.Checks = append(canon.Checks, guardrailCheck{
		Severity: "warning",
		Code:     "phase.exhaustive_compact.failed",
		Message:  "legacy warning-only exhaustive compact failure",
	})

	checks := validateCanonicalRun(canon)
	var sawRequired bool
	for _, check := range checks {
		if check.Severity == "error" && check.Code == "exhaustive_compact_required" {
			sawRequired = true
		}
	}
	if !sawRequired {
		t.Fatalf("expected exhaustive compact failure to become a blocking guardrail, got %#v", checks)
	}
}

func TestCanonicalComparisonsSuppressedAfterExhaustiveCompactFailure(t *testing.T) {
	canon := knownExampleRun()
	canon.Checks = append(canon.Checks, guardrailCheck{
		Severity: "error",
		Code:     "phase.exhaustive_compact.failed",
		Message:  "exhaustive compact matrix failed",
	})

	if comparisons := buildCompactedComparisons(canon); len(comparisons) != 0 {
		t.Fatalf("warning-only compact evidence should not emit compacted comparisons: %#v", comparisons)
	}
	summary := renderExecutiveSummary(canon)
	if strings.Contains(summary, "byte-minimized") || strings.Contains(summary, "via exhaustive compact") {
		t.Fatalf("summary should not claim byte-minimized storage after exhaustive compact failure: %s", summary)
	}
	var fair strings.Builder
	writeFairComparison(&fair, canon)
	if strings.Contains(fair.String(), "| `treedb_template_v1_collection_2_indexes` |") {
		t.Fatalf("fair comparison table should suppress TreeDB compacted claims after failure:\n%s", fair.String())
	}
}

func TestCanonicalComparisonsRequirePositiveExhaustiveCompactEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*canonicalRun)
	}{
		{
			name: "missing_row",
			mutate: func(canon *canonicalRun) {
				var filtered []resultRow
				for _, row := range canon.Results {
					if row.Phase == phaseExhaustiveCompact {
						continue
					}
					filtered = append(filtered, row)
				}
				canon.Results = filtered
			},
		},
		{
			name: "zero_bytes_per_doc",
			mutate: func(canon *canonicalRun) {
				for i := range canon.Results {
					if canon.Results[i].Phase == phaseExhaustiveCompact {
						canon.Results[i].BytesPerDoc = floatPtr(0)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canon := knownExampleRun()
			tt.mutate(canon)

			checks := validateCanonicalRun(canon)
			assertCheckCode(t, checks, "exhaustive_compact_required")
			if comparisons := buildCompactedComparisons(canon); len(comparisons) != 0 {
				t.Fatalf("missing positive exhaustive evidence should not build compacted comparisons: %#v", comparisons)
			}
			if comparisons := comparisonsForReport(canon); len(comparisons) != 0 {
				t.Fatalf("missing positive exhaustive evidence should suppress stored compacted comparisons: %#v", comparisons)
			}
			summary := renderExecutiveSummary(canon)
			if strings.Contains(summary, "byte-minimized") || strings.Contains(summary, "via exhaustive compact") {
				t.Fatalf("summary should not claim byte-minimized storage without positive exhaustive evidence: %s", summary)
			}
			var fair strings.Builder
			writeFairComparison(&fair, canon)
			if strings.Contains(fair.String(), "| `treedb_template_v1_collection_2_indexes` |") {
				t.Fatalf("fair comparison table should suppress TreeDB compacted claims without positive exhaustive evidence:\n%s", fair.String())
			}
		})
	}
}

func TestCanonicalValidationRequiresProductionEvidenceForCompactedClaims(t *testing.T) {
	canon := knownExampleRun()
	for i := range canon.Results {
		if canon.Results[i].ConfigName == "treedb_template_v1_collection_2_indexes" && canon.Results[i].Phase == phasePostInsert {
			canon.Results[i].ProductionEvidence = nil
		}
	}
	canon.Comparisons = buildCompactedComparisons(canon)

	checks := validateCanonicalRun(canon)
	assertCheckCode(t, checks, "production_evidence_required")
	if comparisons := buildCompactedComparisons(canon); len(comparisons) != 0 {
		t.Fatalf("missing producer evidence should suppress compacted comparisons: %#v", comparisons)
	}
	if comparisons := comparisonsForReport(canon); len(comparisons) != 0 {
		t.Fatalf("missing producer evidence should suppress stored compacted comparisons: %#v", comparisons)
	}
	summary := renderExecutiveSummary(canon)
	if strings.Contains(summary, "byte-minimized") || strings.Contains(summary, "via exhaustive compact") {
		t.Fatalf("summary should not claim compacted-state win without production evidence: %s", summary)
	}
}

func TestCanonicalValidationSkipsAuxiliaryPostInsertRowsForProductionEvidence(t *testing.T) {
	canon := knownExampleRun()
	offline := testStorageRow("treedb_template_v1_collection_2_indexes", "treedb_fast", "template-v1", "collection", phasePostInsert, 2, 12000000, 120.0)
	offline.BenchmarkName = "treedb_collection_compression_matrix"
	offline.MaintenanceMode = "none"
	offline.BenchmarkTimed = false
	offline.MeasurementKind = "offline_script"
	offline.MeasurementNote = "offline compact matrix before-compact size; not compacted"
	offline.CompactionFlags = nil
	offline.ProductionEvidence = &productionEvidence{
		StoragePolicy:          "index-vlog",
		StorageCells:           "index-vlog",
		LeafSegmentTargetBytes: 1048576,
	}
	fixture := testStorageRow("treedb_template_v1_collection_2_indexes", "treedb_fast", "template-v1", "collection", phasePostInsert, 2, 12500000, 125.0)
	fixture.BenchmarkName = "collection-load-fixture"
	fixture.MaintenanceMode = "none"
	fixture.BenchmarkTimed = false
	fixture.MeasurementKind = "fixture_wall_timed"
	fixture.MeasurementNote = "full leafgen fixture pre-maintenance size; use benchmark-timed rows for primary throughput"
	fixture.CompactionFlags = nil
	fixture.DocsPerSec = floatPtr(100000)
	canon.Results = append(canon.Results, offline, fixture)
	finalizeRunMetadata(canon)

	for _, check := range validateCanonicalRun(canon) {
		if check.Code == "missing_production_evidence" {
			t.Fatalf("auxiliary post-insert rows should not require producer-path evidence: %#v", check)
		}
	}
}

func TestCanonicalValidationAcceptsNoSecondaryIndexFlushSpanEvidence(t *testing.T) {
	canon := knownExampleRun()
	canon.Config.Indexes = 0
	canon.Results = append(canon.Results,
		testNoSecondaryIndexPostInsertProductionRow("treedb_template_v1_collection_0_indexes"),
		testStorageRow("treedb_template_v1_collection_0_indexes", "treedb_fast", "template-v1", "collection", phaseExhaustiveCompact, 0, 450000, 4.5),
		testStorageRow("sqlite_json_0_indexes", "sqlite_wal_normal", "json", "collection", phaseSQLiteVacuum, 0, 2500000, 25.0),
		testStorageRow("sqlite_native_columns_0_indexes", "sqlite_wal_normal", "native-columns", "collection", phaseSQLiteVacuum, 0, 2000000, 20.0),
	)
	finalizeRunMetadata(canon)
	canon.Comparisons = buildCompactedComparisons(canon)

	for _, check := range validateCanonicalRun(canon) {
		if check.Severity == "error" {
			t.Fatalf("no-secondary-index flush evidence should satisfy production guardrails: %#v", check)
		}
	}
	if got := findComparison(canon.Comparisons, "treedb_template_v1_collection_0_indexes", phaseExhaustiveCompact, "sqlite_native_columns_0_indexes", phaseSQLiteVacuum); got == nil {
		t.Fatalf("no-secondary-index production evidence should preserve compacted comparison: %#v", canon.Comparisons)
	}
}

func TestProductionEvidenceFromTreeDBTimedReportUsesNoSecondaryIndexFlushRoute(t *testing.T) {
	ev := productionEvidenceFromTreeDBTimedReport(collectionReport{
		BenchmarkEngine:      "command_wal_relaxed",
		DocumentFormat:       "template-v1",
		StoragePolicy:        "data_outer=true,index_outer=true",
		PagerChunkSize:       "profile/default",
		PagerSyncConcurrency: "profile/default",
	}, map[string]float64{
		"indexes/doc":                           0,
		"gomaxprocs":                            16,
		"physical_cores":                        8,
		"flush_admission_effective_concurrency": 8,
		"flush_admission_admitted":              1,
		"flush_admission_span_native":           1,
		"flush_admission_backlog_coalescing":    1,
		"flush_span_candidate_ops":              2,
		"flush_span_eligible_ops":               2,
		"flush_span_used_ops":                   2,
		"flush_span_fallbacks":                  0,
		"ordered_root_span_fallbacks":           0,
	}, config{Indexes: 0, StorageCells: "index-vlog", LeafSegmentTargetBytes: 1048576})
	if ev.ProducerRoute != noSecondaryIndexProducerRoute {
		t.Fatalf("producer route=%q want %q", ev.ProducerRoute, noSecondaryIndexProducerRoute)
	}
	if ev.ProducerRouteUsedOps == nil || *ev.ProducerRouteUsedOps != 2 {
		t.Fatalf("fallback producer used ops not copied from flush span evidence: %#v", ev.ProducerRouteUsedOps)
	}
}

func TestNormalizeRunPathsMakesOutDirAbsolute(t *testing.T) {
	cfg := config{OutDir: filepath.Join("relative", "bench-run")}
	if err := normalizeRunPaths(&cfg); err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(cfg.OutDir) {
		t.Fatalf("out dir was not normalized to an absolute path: %q", cfg.OutDir)
	}
	if !strings.HasSuffix(cfg.OutDir, filepath.Join("relative", "bench-run")) {
		t.Fatalf("out dir lost the requested suffix: %q", cfg.OutDir)
	}
}

func TestCanonicalFormatListDedupesCanonicalAliases(t *testing.T) {
	got := strings.Join(canonicalFormatList([]string{"JSON", "json", "template_v1", "template-v1", "bson", ""}), ",")
	want := "json,template-v1,bson"
	if got != want {
		t.Fatalf("canonicalFormatList = %q, want %q", got, want)
	}
}

func TestValidateSafeRunDirRefusesUnsafeExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unrelated.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	err := validateSafeRunDir(dir)
	if err == nil || !strings.Contains(err.Error(), "not an empty canonical benchmark run dir") {
		t.Fatalf("expected unsafe existing directory to be rejected, err=%v", err)
	}
}

func TestValidateSafeRunDirAllowsEmptyOrSentinelDirectory(t *testing.T) {
	emptyDir := t.TempDir()
	if err := validateSafeRunDir(emptyDir); err != nil {
		t.Fatalf("empty directory should be allowed: %v", err)
	}

	sentinelDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sentinelDir, runDirSentinel), []byte(schemaVersion+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sentinelDir, "unrelated.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := validateSafeRunDir(sentinelDir); err != nil {
		t.Fatalf("sentinel directory should be allowed: %v", err)
	}
}

func TestValidateSafeRunDirRefusesFilesystemRoot(t *testing.T) {
	root := filepath.Clean(string(os.PathSeparator))
	err := validateSafeRunDir(root)
	if err == nil || !strings.Contains(err.Error(), "filesystem root") {
		t.Fatalf("expected filesystem root to be rejected, err=%v", err)
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

func TestParseConfigRejectsUnsupportedIndexCount(t *testing.T) {
	_, err := parseConfig([]string{"-indexes", "4"})
	if err == nil || !strings.Contains(err.Error(), "-indexes must be 0, 1, 2, or 3") {
		t.Fatalf("parseConfig accepted unsupported index count, err=%v", err)
	}
}

func TestParseConfigValidatesIndexVacuum(t *testing.T) {
	cfg, err := parseConfig([]string{"-index-vacuum", "ONLINE"})
	if err != nil {
		t.Fatalf("parseConfig rejected valid normalized index vacuum: %v", err)
	}
	if cfg.FullLeafgenIndexVacuum != "online" {
		t.Fatalf("index vacuum was not normalized: %q", cfg.FullLeafgenIndexVacuum)
	}
	_, err = parseConfig([]string{"-index-vacuum", "bogus"})
	if err == nil || !strings.Contains(err.Error(), "-index-vacuum must be one of offline, online, auto, or none") {
		t.Fatalf("parseConfig accepted unsupported index vacuum, err=%v", err)
	}
}

func TestParseFullLeafgenSummaryPreservesZeroIndexCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	summary := fixtureSummary{
		DocumentFormat: "template-v1",
		Profile:        "fast",
		Docs:           100,
		BatchSize:      16,
		IndexCount:     0,
		DiskUsageFinal: &diskUsageSummary{TotalBytes: 1234},
	}
	if err := writeJSON(path, summary); err != nil {
		t.Fatal(err)
	}
	canon := knownExampleRun()
	canon.Config.Indexes = 2
	canon.Results = nil
	if err := parseFullLeafgenSummary(canon, path, config{Docs: 100, Indexes: 2, FullLeafgenIndexVacuum: "offline"}); err != nil {
		t.Fatal(err)
	}
	if len(canon.Results) != 1 {
		t.Fatalf("expected one full leafgen row, got %d", len(canon.Results))
	}
	row := canon.Results[0]
	if row.IndexCount != 0 || row.ConfigName != "treedb_template_v1_collection_0_indexes" {
		t.Fatalf("zero-index full leafgen row was mislabeled: index=%d config=%s", row.IndexCount, row.ConfigName)
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
		{phaseOfflineCompact, "sqlite_native_columns_2_indexes", 156.7 / 44.3},
		{phaseFullLeafgenPackGC, "sqlite_native_columns_2_indexes", 156.7 / 31.7},
		{phaseOfflineCompact, "sqlite_json_2_indexes", 231.7 / 44.3},
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

func TestCanonicalDerivedCompactedComparisonsRequireEvidencePerTreeDBConfig(t *testing.T) {
	canon := knownExampleRun()
	canon.Results = append(canon.Results,
		testStorageRow("treedb_json_collection_2_indexes", "treedb_fast", "json", "collection", phaseOfflineCompact, 2, 3900000, 39.0),
		testStorageRow("treedb_json_collection_2_indexes", "treedb_fast", "json", "collection", phaseFullLeafgenPackGC, 2, 2900000, 29.0),
	)
	finalizeRunMetadata(canon)

	canon.Comparisons = buildCompactedComparisons(canon)
	if got := findComparison(canon.Comparisons, "treedb_template_v1_collection_2_indexes", phaseOfflineCompact, "sqlite_native_columns_2_indexes", phaseSQLiteVacuum); got == nil {
		t.Fatalf("template-v1 comparison with matching exhaustive evidence was suppressed: %#v", canon.Comparisons)
	}
	if got := findComparison(canon.Comparisons, "treedb_json_collection_2_indexes", phaseOfflineCompact, "sqlite_native_columns_2_indexes", phaseSQLiteVacuum); got != nil {
		t.Fatalf("json comparison without matching exhaustive evidence was emitted: %#v", got)
	}
	for _, check := range validateCanonicalRun(canon) {
		if check.Code == "missing_compacted_ratio" && strings.Contains(check.Message, "treedb_json_collection_2_indexes") {
			t.Fatalf("validation should not require a compacted ratio without matching exhaustive evidence: %#v", check)
		}
	}

	canon.Comparisons = append(canon.Comparisons, comparisonRow{
		ComparisonName:    "stale_json_without_exhaustive_evidence",
		TreeDBConfigName:  "treedb_json_collection_2_indexes",
		TreeDBPhase:       phaseOfflineCompact,
		SQLiteConfigName:  "sqlite_native_columns_2_indexes",
		SQLitePhase:       phaseSQLiteVacuum,
		TreeDBBytesPerDoc: 39.0,
		SQLiteBytesPerDoc: 156.7,
		SmallerRatio:      156.7 / 39.0,
	})
	if got := findComparison(comparisonsForReport(canon), "treedb_json_collection_2_indexes", phaseOfflineCompact, "sqlite_native_columns_2_indexes", phaseSQLiteVacuum); got != nil {
		t.Fatalf("stored stale comparison without matching exhaustive evidence was reportable: %#v", got)
	}

	canon.Results = append(canon.Results,
		testStorageRow("treedb_json_collection_2_indexes", "treedb_fast", "json", "collection", phaseExhaustiveCompact, 2, 2100000, 21.0),
	)
	canon.Comparisons = buildCompactedComparisons(canon)
	if got := findComparison(canon.Comparisons, "treedb_json_collection_2_indexes", phaseOfflineCompact, "sqlite_native_columns_2_indexes", phaseSQLiteVacuum); got != nil {
		t.Fatalf("json comparison with compact evidence but no production evidence was emitted: %#v", got)
	}

	canon.Results = append(canon.Results, testPostInsertProductionRow("treedb_json_collection_2_indexes", "treedb_fast", "json", 2))
	finalizeRunMetadata(canon)
	canon.Comparisons = buildCompactedComparisons(canon)
	if got := findComparison(canon.Comparisons, "treedb_json_collection_2_indexes", phaseOfflineCompact, "sqlite_native_columns_2_indexes", phaseSQLiteVacuum); got == nil {
		t.Fatalf("json comparison with matching exhaustive evidence was not emitted: %#v", canon.Comparisons)
	}
}

func TestCanonicalDerivedCompactedComparisonsUseConfiguredIndexCount(t *testing.T) {
	canon := knownExampleRun()
	canon.Config.Indexes = 1
	canon.Results = append(canon.Results,
		testPostInsertProductionRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", 1),
		testStorageRow("sqlite_json_1_indexes", "sqlite_wal_normal", "json", "collection", phaseSQLiteVacuum, 1, 18000000, 180.0),
		testStorageRow("sqlite_native_columns_1_indexes", "sqlite_wal_normal", "native-columns", "collection", phaseSQLiteVacuum, 1, 12000000, 120.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineCompact, 1, 3600000, 36.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseExhaustiveCompact, 1, 2400000, 24.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseFullLeafgenPackGC, 1, 2800000, 28.0),
	)
	finalizeRunMetadata(canon)

	comparisons := buildCompactedComparisons(canon)
	if got := findComparison(comparisons, "treedb_template_v1_collection_1_indexes", phaseOfflineCompact, "sqlite_native_columns_1_indexes", phaseSQLiteVacuum); got == nil {
		t.Fatalf("missing configured one-index comparison, got %#v", comparisons)
	}
	if got := findComparison(comparisons, "treedb_template_v1_collection_2_indexes", phaseOfflineCompact, "sqlite_native_columns_2_indexes", phaseSQLiteVacuum); got != nil {
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
		if canon.Results[i].ConfigName == "treedb_template_v1_collection_2_indexes" && canon.Results[i].Phase == phaseOfflineCompact {
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
		testPostInsertProductionRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", 1),
		testStorageRow("sqlite_json_1_indexes", "sqlite_wal_normal", "json", "collection", phaseSQLiteVacuum, 1, 18000000, 180.0),
		testStorageRow("sqlite_native_columns_1_indexes", "sqlite_wal_normal", "native-columns", "collection", phaseSQLiteVacuum, 1, 12000000, 120.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineCompact, 1, 3600000, 36.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseExhaustiveCompact, 1, 2400000, 24.0),
		testStorageRow("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseFullLeafgenPackGC, 1, 2800000, 28.0),
	)
	finalizeRunMetadata(canon)

	summary := renderExecutiveSummary(canon)
	if !strings.Contains(summary, "byte-minimized one-index template-v1 collection storage") {
		t.Fatalf("summary did not use configured index count: %s", summary)
	}
	if strings.Contains(summary, "two-index") {
		t.Fatalf("summary should not hardcode two-index for one-index run: %s", summary)
	}
}

func TestPrimaryFormatNormalizesConfiguredFormats(t *testing.T) {
	canon := knownExampleRun()
	canon.Config.Formats = []string{" JSON ", "template_v1"}
	if got, want := primaryFormat(canon), "template-v1"; got != want {
		t.Fatalf("primaryFormat = %q, want %q", got, want)
	}

	canon.Config.Formats = []string{" JSON "}
	if got, want := primaryFormat(canon), "json"; got != want {
		t.Fatalf("primaryFormat = %q, want %q", got, want)
	}

	canon.Config.Formats = []string{" JSON ", "bson", "JSON"}
	wantNames := []string{"treedb_json_collection_2_indexes", "treedb_bson_collection_2_indexes", "treedb_template_v1_collection_2_indexes"}
	if got := compactedTreeDBConfigNames(canon); strings.Join(got, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("compactedTreeDBConfigNames = %v, want %v", got, wantNames)
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

func TestGuardrailTreatsAbsentSQLiteRowsAsAutoSkipped(t *testing.T) {
	canon := knownExampleRun()
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
			t.Fatalf("auto-skipped SQLite should not fail SQLite comparison guardrails, got %#v", checks)
		}
	}
	assertCheckCode(t, checks, "sqlite.auto_skipped")
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

func TestMarkdownInlineCodeHandlesBackticks(t *testing.T) {
	got := markdownInlineCode("run `quoted` value")
	if got != "``run `quoted` value``" {
		t.Fatalf("markdownInlineCode with embedded backticks = %q", got)
	}
	got = markdownInlineCode("`edge`")
	if got != "`` `edge` ``" {
		t.Fatalf("markdownInlineCode with edge backticks = %q", got)
	}
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

func testPostInsertProductionRow(config, engine, format string, indexes int) resultRow {
	return resultRow{
		ConfigName:         config,
		Engine:             engine,
		Format:             format,
		Shape:              "collection",
		IndexCount:         indexes,
		DocumentCount:      100000,
		BenchmarkName:      "BenchmarkCollectionShapeInsertBatch",
		Phase:              phasePostInsert,
		MaintenanceMode:    "none",
		TotalBytes:         int64Ptr(10000000),
		BytesPerDoc:        floatPtr(100),
		DocsPerSec:         floatPtr(500000),
		NsPerDoc:           floatPtr(2000),
		BatchSize:          16000,
		BenchmarkTimed:     true,
		MeasurementKind:    "go_benchmark",
		ProductionEvidence: testProductionEvidence(),
	}
}

func testNoSecondaryIndexPostInsertProductionRow(config string) resultRow {
	return resultRow{
		ConfigName:         config,
		Engine:             "command_wal_relaxed",
		Format:             "template-v1",
		Shape:              "collection",
		IndexCount:         0,
		DocumentCount:      100000,
		BenchmarkName:      "BenchmarkCollectionShapeInsertBatch/indexes_0",
		Phase:              phasePostInsert,
		MaintenanceMode:    "none",
		TotalBytes:         int64Ptr(10000000),
		BytesPerDoc:        floatPtr(100),
		DocsPerSec:         floatPtr(500000),
		NsPerDoc:           floatPtr(2000),
		BatchSize:          16000,
		BenchmarkTimed:     true,
		MeasurementKind:    "go_benchmark",
		ProductionEvidence: testNoSecondaryIndexProductionEvidence(),
	}
}

func testProductionEvidence() *productionEvidence {
	gomax := 16
	physical := 8
	configured := 0
	effective := 8
	admitted := true
	spanNative := true
	backlog := true
	observations := 100.0
	candidate := 100000.0
	eligible := 100000.0
	used := 100000.0
	fallbacks := 0.0
	return &productionEvidence{
		ProducerRoute:                       "command_wal_publish",
		ProducerRouteObservations:           &observations,
		ProducerRouteCandidateOps:           &candidate,
		ProducerRouteEligibleOps:            &eligible,
		ProducerRouteUsedOps:                &used,
		ProducerRouteFallbacks:              &fallbacks,
		StoragePolicy:                       "data_outer=true,index_outer=true",
		StorageCells:                        "index-vlog",
		PagerChunkSize:                      "default",
		PagerSyncConcurrency:                "default",
		LeafSegmentTargetBytes:              1048576,
		GOMAXPROCS:                          &gomax,
		PhysicalCores:                       &physical,
		FlushAdmissionConfiguredConcurrency: &configured,
		FlushAdmissionEffectiveConcurrency:  &effective,
		FlushAdmissionGOMAXPROCS:            &gomax,
		FlushAdmissionPhysicalCores:         &physical,
		FlushAdmissionAdmitted:              &admitted,
		FlushAdmissionSpanNative:            &spanNative,
		FlushAdmissionBacklogCoalescing:     &backlog,
		FlushSpanCandidateOps:               &candidate,
		FlushSpanEligibleOps:                &eligible,
		FlushSpanUsedOps:                    &used,
		FlushSpanFallbacks:                  &fallbacks,
		OrderedRootSpanCandidateOps:         &candidate,
		OrderedRootSpanEligibleOps:          &eligible,
		OrderedRootSpanUsedOps:              &used,
		OrderedRootSpanFallbacks:            &fallbacks,
	}
}

func testNoSecondaryIndexProductionEvidence() *productionEvidence {
	ev := testProductionEvidence()
	ev.ProducerRoute = ""
	ev.ProducerRouteObservations = nil
	ev.ProducerRouteCandidateOps = nil
	ev.ProducerRouteEligibleOps = nil
	ev.ProducerRouteUsedOps = nil
	ev.ProducerRouteFallbacks = nil
	return ev
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
			TreeEngine:                "command_wal_relaxed",
			Profile:                   "command_wal_relaxed",
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
				ConfigName:         "treedb_template_v1_collection_2_indexes",
				Engine:             "command_wal_relaxed",
				Format:             "template-v1",
				Shape:              "collection",
				IndexCount:         2,
				DocumentCount:      docs,
				BenchmarkName:      "BenchmarkCollectionShapeInsertBatch/indexes_2",
				Phase:              phasePostInsert,
				MaintenanceMode:    "none",
				TotalBytes:         int64Ptr(10555966),
				BytesPerDoc:        floatPtr(105.6),
				DocsPerSec:         floatPtr(584112),
				NsPerDoc:           floatPtr(1712),
				BatchSize:          batch,
				BenchmarkTimed:     true,
				MeasurementKind:    "go_benchmark",
				ProductionEvidence: testProductionEvidence(),
			},
			{
				ConfigName:      "treedb_template_v1_collection_2_indexes",
				Engine:          "command_wal_relaxed",
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
			row("treedb_template_v1_raw", "treedb_fast", "template-v1", "raw", phaseOfflineCompact, 0, 1983120, 19.8),
			row("treedb_template_v1_collection_0_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineCompact, 0, 1948075, 19.5),
			row("treedb_template_v1_collection_1_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineCompact, 1, 3751406, 37.5),
			row("treedb_template_v1_collection_2_indexes", "treedb_fast", "template-v1", "collection", phaseOfflineCompact, 2, 4434451, 44.3),
			row("treedb_template_v1_collection_2_indexes", "treedb_fast", "template-v1", "collection", phaseExhaustiveCompact, 2, 2319517, 23.2),
			row("treedb_template_v1_collection_2_indexes", "treedb_fast", "template-v1", "collection", phaseFullLeafgenPackGC, 2, 3174681, 31.7),
			row("sqlite_json_2_indexes", "sqlite_wal_normal", "json", "collection", phaseSQLiteVacuum, 2, 23166976, 231.7),
			row("sqlite_native_columns_2_indexes", "sqlite_wal_normal", "native-columns", "collection", phaseSQLiteVacuum, 2, 15671296, 156.7),
		},
	}
	canon.Comparisons = buildCompactedComparisons(canon)
	finalizeRunMetadata(canon)
	return canon
}
