package jsonbenchcontract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalJSONBenchResultRecordsActualTreeDBProfile(t *testing.T) {
	manifest := validManifest(t)
	manifest.TreeDB.RequestedProfile = "durable"
	manifest.TreeDB.ResultPath = writeResult(t, map[string]any{
		"schema_version": "jsonbench-treedb-report/v1",
		"rows": []map[string]any{{
			"query":                  "q1",
			"profile":                "fast",
			"dataset_size":           1_000_000,
			"query_mode":             "one_shot_end_to_end",
			"metadata_mode":          "no_aggregate_metadata",
			"document_scan_fallback": false,
			"reconstruction_status":  "validated",
			"storage_layout":         "column-store-full-prepared",
			"projection":             "full",
			"attempts_seconds":       []float64{0.001, 0.0011, 0.0009, 0.0012, 0.001},
		}},
	})

	err := Validate(manifest, filepath.Dir(manifest.TreeDB.ResultPath))
	assertErrorContains(t, err, `requested profile "durable" does not match recorded profile "fast"`)
}

func TestCanonicalJSONBenchRequiresDurableTreeDBProfile(t *testing.T) {
	manifest := validManifest(t)
	manifest.TreeDB.RequestedProfile = "fast"

	err := Validate(manifest, manifest.ArtifactRoot)
	assertErrorContains(t, err, `treedb.requested_profile must be "durable" for canonical evidence`)
}

func TestCanonicalJSONBenchReportValidatesEveryRow(t *testing.T) {
	manifest := validManifest(t)
	rows := []map[string]any{
		{
			"query":                  "q1",
			"profile":                "durable",
			"dataset_size":           1_000_000,
			"query_mode":             "one_shot_end_to_end",
			"metadata_mode":          "no_aggregate_metadata",
			"document_scan_fallback": false,
			"reconstruction_status":  "not_validated",
			"storage_layout":         "column-store-full-prepared",
			"projection":             "full",
			"attempts_seconds":       []float64{0.001, 0.0011, 0.0009, 0.0012, 0.001},
		},
		{
			"query":                  "q2",
			"profile":                "durable",
			"dataset_size":           999_999,
			"query_mode":             "hot_prepared_run",
			"metadata_mode":          "no_aggregate_metadata",
			"document_scan_fallback": false,
			"reconstruction_status":  "not_validated",
			"storage_layout":         "column-store-full-prepared",
			"projection":             "full",
			"attempts_seconds":       []float64{0.001, 0.0011, 0.0009, 0.0012, 0.001},
		},
	}
	manifest.TreeDB.ResultPath = writeResult(t, map[string]any{
		"schema_version": "jsonbench-treedb-report/v1",
		"rows":           rows,
	})

	err := Validate(manifest, filepath.Dir(manifest.TreeDB.ResultPath))
	assertErrorContains(t, err, "treedb result row[1].dataset_size 999999 does not match pinned rows 1000000")
	assertErrorContains(t, err, `treedb result row[1].query_mode "hot_prepared_run" does not match comparison.query_mode "one_shot_end_to_end"`)
}

func TestCanonicalJSONBenchReportRequiresUniqueQueryCoverage(t *testing.T) {
	manifest := validManifest(t)
	rows := validTreeDBRows()
	rows[len(rows)-1]["query"] = "q5"
	manifest.TreeDB.ResultPath = writeResultAt(t, manifest.ArtifactRoot, map[string]any{
		"schema_version": "jsonbench-treedb-report/v1",
		"rows":           rows,
	})

	err := Validate(manifest, manifest.ArtifactRoot)
	assertErrorContains(t, err, `treedb result query "q5" is duplicated within the selected canonical lane`)
	assertErrorContains(t, err, `treedb result is missing required query "qexpr" from the selected canonical lane`)
}

func TestCanonicalJSONBenchReportRequiresFivePositiveTreeDBAttempts(t *testing.T) {
	manifest := validManifest(t)
	rows := validTreeDBRows()
	rows[1]["attempts_seconds"] = []float64{0.001}
	rows[2]["attempts_seconds"] = []float64{0.001, 0.001, 0, 0.001, 0.001}
	manifest.TreeDB.ResultPath = writeResultAt(t, manifest.ArtifactRoot, map[string]any{
		"schema_version": "jsonbench-treedb-report/v1",
		"rows":           rows,
	})

	err := Validate(manifest, manifest.ArtifactRoot)
	assertErrorContains(t, err, "treedb result row[1].attempts_seconds has 1 attempts, want at least 5")
	assertErrorContains(t, err, "treedb result row[2].attempts_seconds timings must be positive")
}

func TestCanonicalJSONBenchRequiresClickHouseComparisonArtifact(t *testing.T) {
	manifest := validManifest(t)
	manifest.ClickHouse.ResultPath = filepath.Join(manifest.ArtifactRoot, "missing_clickhouse.json")

	err := Validate(manifest, manifest.ArtifactRoot)
	assertErrorContains(t, err, "clickhouse result:")
}

func TestCanonicalJSONBenchCrossChecksClickHouseIdentity(t *testing.T) {
	manifest := validManifest(t)
	result := validClickHouseResult(1_000_000)
	result["version"] = "unexpected"
	manifest.ClickHouse.ResultPath = writeResultAt(t, manifest.ArtifactRoot, result)

	err := Validate(manifest, manifest.ArtifactRoot)
	assertErrorContains(t, err, `clickhouse result.version "unexpected" does not match pins.clickhouse_version "26.4.2.10"`)
}

func TestCanonicalJSONBenchResultIncludesPinnedComparisonDimensions(t *testing.T) {
	manifest := validManifest(t)
	manifest.Pins.JSONBenchCommit = ""
	manifest.Comparison.CachePolicy = ""

	err := Validate(manifest, t.TempDir())
	assertErrorContains(t, err, "pins.jsonbench_commit is required")
	assertErrorContains(t, err, "comparison.cache_policy is required")
}

func TestQueryReadyEvidenceRejectsMissingFallbackAndGenerationCounters(t *testing.T) {
	manifest := validManifest(t)
	delete(manifest.Counters, "visible_delta_generations")
	delete(manifest.Counters, "document_fallbacks")

	err := Validate(manifest, t.TempDir())
	assertErrorContains(t, err, "counters.visible_delta_generations is required")
	assertErrorContains(t, err, "counters.document_fallbacks is required")
}

func TestCanonicalValidationIsSeparateFromMeasuredIntervals(t *testing.T) {
	manifest := validManifest(t)
	manifest.Validation.TimingBoundary = "inside_query_timer"

	err := Validate(manifest, t.TempDir())
	assertErrorContains(t, err, `validation.timing_boundary must be "outside_measured_intervals"`)
}

func TestQueryResourceEvidenceDistinguishesBenchmemFromCumulativeProfiles(t *testing.T) {
	manifest := validManifest(t)
	manifest.Resources = []ResourceEvidence{
		{
			Scope:      "query/q2",
			SourceKind: ResourceCumulativeAllocProfile,
			Artifact:   "profiles/allocs_q2.pprof",
			Metrics: ResourceMetrics{
				BytesPerOp:  uint64Pointer(1024),
				AllocsPerOp: uint64Pointer(4),
			},
		},
		{
			Scope:      "load",
			SourceKind: ResourceProcessPeak,
			Artifact:   "profiles/load.time-v.txt",
		},
	}

	err := Validate(manifest, t.TempDir())
	assertErrorContains(t, err, "cumulative_alloc_profile must be contextual_only")
	assertErrorContains(t, err, "cumulative_alloc_profile cannot report bytes_per_op or allocs_per_op")
	assertErrorContains(t, err, "query/q2 requires direct go_benchmem evidence")
	assertErrorContains(t, err, "load process_peak requires peak_rss_bytes or live_heap_bytes")
}

func TestCanonicalSmokeManifestPasses(t *testing.T) {
	manifest := validManifest(t)
	if err := Validate(manifest, t.TempDir()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func validManifest(t *testing.T) Manifest {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"validation/reconstruction.json": `{"status":"passed"}`,
		"profiles/q2.bench.txt":          "BenchmarkQ2 1 1000 ns/op 1024 B/op 4 allocs/op\n",
		"profiles/q3.bench.txt":          "BenchmarkQ3 1 1000 ns/op 1024 B/op 4 allocs/op\n",
		"profiles/q5.bench.txt":          "BenchmarkQ5 1 1000 ns/op 1024 B/op 4 allocs/op\n",
		"profiles/load.time-v.txt":       "Maximum resident set size: 4\n",
		"profiles/allocs_q2.pprof":       "test profile placeholder\n",
	} {
		fullPath := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	resultPath := writeResultAt(t, root, map[string]any{
		"schema_version": "jsonbench-treedb-report/v1",
		"rows":           validTreeDBRows(),
	})
	clickHousePath := filepath.Join(root, "clickhouse_result.json")
	writeJSONFile(t, clickHousePath, validClickHouseResult(1_000_000))
	return Manifest{
		SchemaVersion: SchemaVersion,
		Canonical:     true,
		Pins: Pins{
			GomapCommit:       "d1b2d909ee6a1fd409d63ef895245c04b0f1376f",
			JSONBenchCommit:   "7886cc3ff909e733b3aa5d68aa8203db67349be2",
			ClickHouseVersion: "26.4.2.10",
			Dataset:           DatasetPin{Identity: "bluesky/file_0001.json.gz", Rows: 1_000_000, SHA256: strings.Repeat("a", 64)},
		},
		Host:         Host{Identity: "test-host/linux-amd64"},
		ArtifactRoot: root,
		TreeDB: TreeDBRun{
			RequestedProfile: "durable",
			ResultPath:       resultPath,
			RowSelector:      ResultRowSelector{StorageLayout: "column-store-full-prepared", Projection: "full"},
		},
		ClickHouse: ClickHouseRun{ResultPath: clickHousePath},
		Comparison: Comparison{
			QueryMode:             "one_shot_end_to_end",
			AggregateMetadataMode: "no_aggregate_metadata",
			FallbackPolicy:        "forbid",
			CachePolicy:           "drop-os-page-cache-not-required",
			WarmthPolicy:          "cold-open-one-shot",
			Attempts:              5,
			QueryOrder:            []string{"q1", "q2", "q3", "q4", "q5", "qexpr"},
			Statistic:             "median",
			QueryMaxRatio:         1.5,
			LoadMaxRatio:          1.5,
			Q4RegressionMaxRatio:  1.05,
			TargetRevisionPolicy:  "revise only with linked same-host evidence and tracker approval",
			ValidationPolicy:      "canonical-result-hash-and-reconstruction",
		},
		Validation: ValidationEvidence{
			Status:                  "passed",
			Artifact:                "validation/reconstruction.json",
			TimingBoundary:          "outside_measured_intervals",
			ResultHashesValidated:   true,
			ReconstructionValidated: true,
		},
		Counters: map[string]CounterEvidence{
			"visible_base_generations":      {Value: 1, Source: "smoke"},
			"visible_delta_generations":     {Value: 0, Source: "smoke"},
			"tombstones_applied":            {Value: 0, Source: "smoke"},
			"parts_decoded":                 {Value: 0, Source: "smoke"},
			"query_time_dictionaries_built": {Value: 0, Source: "smoke"},
			"query_time_ranks_built":        {Value: 0, Source: "smoke"},
			"query_time_offsets_built":      {Value: 0, Source: "smoke"},
			"document_fallbacks":            {Value: 0, Source: "smoke"},
			"row_fallbacks":                 {Value: 0, Source: "smoke"},
			"result_hash_validated":         {Value: 1, Source: "smoke"},
		},
		Resources: []ResourceEvidence{
			{
				Scope:       "query/q2",
				SourceKind:  ResourceGoBenchmem,
				Artifact:    "profiles/q2.bench.txt",
				SampleCount: 5,
				Metrics:     ResourceMetrics{NanosPerOp: float64Pointer(1000), BytesPerOp: uint64Pointer(1024), AllocsPerOp: uint64Pointer(4)},
			},
			{
				Scope:       "query/q3",
				SourceKind:  ResourceGoBenchmem,
				Artifact:    "profiles/q3.bench.txt",
				SampleCount: 5,
				Metrics:     ResourceMetrics{NanosPerOp: float64Pointer(1000), BytesPerOp: uint64Pointer(1024), AllocsPerOp: uint64Pointer(4)},
			},
			{
				Scope:       "query/q5",
				SourceKind:  ResourceGoBenchmem,
				Artifact:    "profiles/q5.bench.txt",
				SampleCount: 5,
				Metrics:     ResourceMetrics{NanosPerOp: float64Pointer(1000), BytesPerOp: uint64Pointer(1024), AllocsPerOp: uint64Pointer(4)},
			},
			{
				Scope:      "load",
				SourceKind: ResourceProcessPeak,
				Artifact:   "profiles/load.time-v.txt",
				Metrics:    ResourceMetrics{PeakRSSBytes: uint64Pointer(4096)},
			},
			{
				Scope:          "query/q2",
				SourceKind:     ResourceCumulativeAllocProfile,
				Artifact:       "profiles/allocs_q2.pprof",
				ContextualOnly: true,
			},
		},
	}
}

func validTreeDBRows() []map[string]any {
	rows := make([]map[string]any, 0, 6)
	for _, query := range []string{"q1", "q2", "q3", "q4", "q5", "qexpr"} {
		rows = append(rows, map[string]any{
			"query":                  query,
			"profile":                "durable",
			"dataset_size":           1_000_000,
			"query_mode":             "one_shot_end_to_end",
			"metadata_mode":          "no_aggregate_metadata",
			"document_scan_fallback": false,
			"reconstruction_status":  "validated",
			"storage_layout":         "column-store-full-prepared",
			"projection":             "full",
			"attempts_seconds":       []float64{0.001, 0.0011, 0.0009, 0.0012, 0.001},
		})
	}
	return rows
}

func validClickHouseResult(rows int64) map[string]any {
	results := make([][]float64, 6)
	for index := range results {
		results[index] = []float64{0.001, 0.0011, 0.0009, 0.0012, 0.001}
	}
	return map[string]any{
		"system":               "ClickHouse",
		"version":              "26.4.2.10",
		"requested_rows":       rows,
		"dataset_size":         rows,
		"num_loaded_documents": rows,
		"result":               results,
	}
}

func float64Pointer(value float64) *float64 { return &value }

func uint64Pointer(value uint64) *uint64 { return &value }

func writeResult(t *testing.T, value any) string {
	t.Helper()
	return writeResultAt(t, t.TempDir(), value)
}

func writeResultAt(t *testing.T, root string, value any) string {
	t.Helper()
	path := filepath.Join(root, "result.json")
	writeJSONFile(t, path, value)
	return path
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want substring %q", err, want)
	}
}
