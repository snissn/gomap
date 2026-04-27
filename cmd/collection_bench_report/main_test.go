package main

import (
	"encoding/json"
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

func TestBuildReportAndRenderMarkdown(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"output","Output":"BenchmarkCollectionInsertProvidedID-12\t1000\t2000 ns/op\t128 B/op\t4 allocs/op\n"}`,
		`{"Action":"output","Output":"BenchmarkCollectionInsertProvidedID-12\t900\t2200 ns/op\t136 B/op\t5 allocs/op\n"}`,
		`{"Action":"output","Output":"BenchmarkSecondaryLookupNonUnique-12\t5000\t450 ns/op\t96 B/op\t2 allocs/op\n"}`,
		`{"Action":"output","Output":"BenchmarkCollectionDeleteWithSecondaryIndexes-12\t"}`,
		`{"Action":"output","Output":"123\t9000 ns/op\t512 B/op\t8 allocs/op\n"}`,
		`{"Action":"output","Output":"PASS\n"}`,
	}, "\n"))

	samples, err := parseBenchmarkSamples(input)
	if err != nil {
		t.Fatalf("parseBenchmarkSamples: %v", err)
	}
	if got, want := len(samples), 4; got != want {
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

	rep := &report{
		GeneratedAt:     "2026-03-05T00:00:00Z",
		Status:          "ok",
		ExecutionPath:   "oracle",
		BenchmarkEngine: "cached",
		Worktree:        "/tmp/oracle",
		Branch:          "pr/oracle",
		Commit:          "deadbeef",
		RawJSONPath:     "/tmp/collections_bench.json",
		Sections:        buildSections(aggregates),
	}
	md := renderMarkdown(rep)
	if !strings.Contains(md, "- execution path: `oracle`") {
		t.Fatalf("markdown missing execution path:\n%s", md)
	}
	if !strings.Contains(md, "- worktree: `/tmp/oracle`") {
		t.Fatalf("markdown missing worktree:\n%s", md)
	}
	if !strings.Contains(md, "## Document Path") {
		t.Fatalf("markdown missing document section:\n%s", md)
	}
	if !strings.Contains(md, "`BenchmarkSecondaryLookupNonUnique`") {
		t.Fatalf("markdown missing secondary benchmark row:\n%s", md)
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
	if rep.Sections == nil {
		t.Fatal("unavailable report sections should be an empty slice, not nil")
	}
	data, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	if strings.Contains(string(data), `"sections":null`) {
		t.Fatalf("unavailable report encoded null sections: %s", data)
	}
	if !strings.Contains(string(data), `"sections":[]`) {
		t.Fatalf("unavailable report missing empty sections array: %s", data)
	}
	md := renderMarkdown(rep)
	if !strings.Contains(md, "This branch does not currently contain a runnable collections benchmark harness.") {
		t.Fatalf("markdown missing unavailable explanation:\n%s", md)
	}
}

func TestMarkdownToHTMLDocRendersGFMTables(t *testing.T) {
	t.Parallel()

	html, err := markdownToHTMLDoc("| name | value |\n| --- | ---: |\n| docs | 42 |\n")
	if err != nil {
		t.Fatalf("markdownToHTMLDoc: %v", err)
	}
	if !strings.Contains(string(html), "<table>") {
		t.Fatalf("expected rendered HTML table, got:\n%s", html)
	}
}
