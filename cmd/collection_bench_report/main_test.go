package main

import (
	"strings"
	"testing"
)

func TestParseFlagsRejectsMixedExecutionPathLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "comma separated", path: "oracle,native-fastpath"},
		{name: "plus separated", path: "native-fastpath+legacy"},
		{name: "mixed literal", path: "mixed"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseFlagsFrom([]string{
				"-out-dir", t.TempDir(),
				"-execution-path", tc.path,
				"-unavailable-reason", "N/A before R0 harness bring-up",
			})
			if err == nil {
				t.Fatalf("expected execution path %q to fail", tc.path)
			}
			if !strings.Contains(err.Error(), "mixed-path labels are forbidden") {
				t.Fatalf("unexpected error for %q: %v", tc.path, err)
			}
		})
	}
}

func TestParseFlagsRequiresExecutionPath(t *testing.T) {
	t.Parallel()

	_, err := parseFlagsFrom([]string{
		"-out-dir", t.TempDir(),
		"-unavailable-reason", "N/A before R0 harness bring-up",
	})
	if err == nil {
		t.Fatal("expected missing execution path to fail")
	}
	if !strings.Contains(err.Error(), "-execution-path is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseFlagsAllowsSQLiteExecutionPath(t *testing.T) {
	t.Parallel()

	if _, err := parseFlagsFrom([]string{
		"-out-dir", t.TempDir(),
		"-execution-path", "sqlite",
		"-unavailable-reason", "sqlite comparison disabled",
	}); err != nil {
		t.Fatalf("sqlite execution path rejected: %v", err)
	}
}

func TestBuildReportAndRenderMarkdown(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"output","Output":"BenchmarkCollectionInsertProvidedID-12\t1000\t2000 ns/op\t128 B/op\t4 allocs/op\n"}`,
		`{"Action":"output","Output":"BenchmarkCollectionInsertProvidedID-12\t900\t2200 ns/op\t136 B/op\t5 allocs/op\n"}`,
		`{"Action":"output","Output":"BenchmarkCollectionInsertBatchWithSecondaryIndexes-12\t700\t3200 ns/op\t256 B/op\t7 allocs/op\t8000 target_docs/batch\t0 per_item_key_probe_fallback_count\t0 per_item_prefix_probe_fallback_count\n"}`,
		`{"Action":"output","Output":"BenchmarkSQLiteInsertBatchWithSecondaryIndexes-12\t650\t3500 ns/op\t300 B/op\t9 allocs/op\t8000 target_docs/batch\n"}`,
		`{"Action":"output","Output":"BenchmarkSecondaryLookupNonUnique-12\t5000\t450 ns/op\t96 B/op\t2 allocs/op\n"}`,
		`{"Action":"output","Output":"BenchmarkCollectionDeleteWithSecondaryIndexes-12\t"}`,
		`{"Action":"output","Output":"123\t9000 ns/op\t512 B/op\t8 allocs/op\n"}`,
		`{"Action":"output","Output":"PASS\n"}`,
	}, "\n"))

	samples, err := parseBenchmarkSamples(input)
	if err != nil {
		t.Fatalf("parseBenchmarkSamples: %v", err)
	}
	if got, want := len(samples), 6; got != want {
		t.Fatalf("len(samples)=%d want %d", got, want)
	}

	aggregates := aggregateSamples(samples)
	insert := aggregates["BenchmarkCollectionInsertProvidedID"]
	if got, want := insert.Samples, 2; got != want {
		t.Fatalf("insert samples=%d want %d", got, want)
	}
	if got, want := insert.MeanNsPerOp, 2100.0; got != want {
		t.Fatalf("insert mean ns/op=%v want %v", got, want)
	}
	indexedBatch := aggregates["BenchmarkCollectionInsertBatchWithSecondaryIndexes"]
	if got, want := indexedBatch.MeanMetrics["target_docs/batch"], 8000.0; got != want {
		t.Fatalf("indexed batch target_docs/batch=%v want %v", got, want)
	}
	if got, want := indexedBatch.MeanMetrics["per_item_key_probe_fallback_count"], 0.0; got != want {
		t.Fatalf("indexed batch per_item_key_probe_fallback_count=%v want %v", got, want)
	}

	rep := &report{
		GeneratedAt:         "2026-03-05T00:00:00Z",
		Status:              "ok",
		ExecutionPath:       "oracle",
		BenchmarkEngine:     "cached",
		StoragePolicy:       "data_outer=true,index_outer=false",
		Worktree:            "/tmp/oracle",
		Branch:              "pr/oracle",
		Commit:              "deadbeef",
		CollectionBatchSize: 8000,
		RawJSONPath:         "/tmp/collections_bench.json",
		Sections:            buildSections(aggregates),
	}
	md := renderMarkdown(rep)
	if !strings.Contains(md, "- execution path: `oracle`") {
		t.Fatalf("markdown missing execution path:\n%s", md)
	}
	if !strings.Contains(md, "- worktree: `/tmp/oracle`") {
		t.Fatalf("markdown missing worktree:\n%s", md)
	}
	if !strings.Contains(md, "- collection batch size: `8000`") {
		t.Fatalf("markdown missing collection batch size:\n%s", md)
	}
	if !strings.Contains(md, "- storage policy: `data_outer=true,index_outer=false`") {
		t.Fatalf("markdown missing storage policy:\n%s", md)
	}
	if !strings.Contains(md, "## Document Path") {
		t.Fatalf("markdown missing document section:\n%s", md)
	}
	if !strings.Contains(md, "`BenchmarkSecondaryLookupNonUnique`") {
		t.Fatalf("markdown missing secondary benchmark row:\n%s", md)
	}
	if !strings.Contains(md, "`target_docs/batch`") {
		t.Fatalf("markdown missing custom metric column:\n%s", md)
	}
	if !strings.Contains(md, "`per_item_key_probe_fallback_count`") {
		t.Fatalf("markdown missing native fallback metric column:\n%s", md)
	}
	if !strings.Contains(md, "`BenchmarkCollectionInsertBatchWithSecondaryIndexes`") {
		t.Fatalf("markdown missing indexed batch benchmark row:\n%s", md)
	}
	if !strings.Contains(md, "## SQLite Comparison") {
		t.Fatalf("markdown missing sqlite section:\n%s", md)
	}
	if !strings.Contains(md, "`BenchmarkSQLiteInsertBatchWithSecondaryIndexes`") {
		t.Fatalf("markdown missing sqlite benchmark row:\n%s", md)
	}
}

func TestBuildReportUnavailable(t *testing.T) {
	rep, err := buildReport(config{
		outDir:            t.TempDir(),
		branch:            "pr/native-fastpath-r0-oracle-baseline",
		commit:            "abc1234",
		worktree:          "/tmp/native",
		executionPath:     "native-fastpath",
		benchmarkEngine:   "cached",
		unavailableReason: "N/A before R0 harness bring-up",
	})
	if err != nil {
		t.Fatalf("buildReport: %v", err)
	}
	if got, want := rep.Status, "unavailable"; got != want {
		t.Fatalf("status=%q want %q", got, want)
	}
	if got, want := rep.UnavailableReason, "N/A before R0 harness bring-up"; got != want {
		t.Fatalf("unavailableReason=%q want %q", got, want)
	}
	md := renderMarkdown(rep)
	if !strings.Contains(md, "This branch does not currently contain a runnable collections benchmark harness.") {
		t.Fatalf("markdown missing unavailable explanation:\n%s", md)
	}
}
