package main

import (
	"strings"
	"testing"
)

func TestParseBenchmarkSamplesAndRenderMarkdown(t *testing.T) {
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
	deleteBench := aggregates["BenchmarkCollectionDeleteWithSecondaryIndexes"]
	if got, want := deleteBench.Samples, 1; got != want {
		t.Fatalf("delete samples=%d want %d", got, want)
	}

	report := &report{
		GeneratedAt: "2026-03-05T00:00:00Z",
		RawJSONPath: "/tmp/collections_bench.json",
		Sections:    buildSections(aggregates),
	}
	md := renderMarkdown(report)
	if !strings.Contains(md, "## Document Path") {
		t.Fatalf("markdown missing document section:\n%s", md)
	}
	if !strings.Contains(md, "`BenchmarkSecondaryLookupNonUnique`") {
		t.Fatalf("markdown missing secondary benchmark row:\n%s", md)
	}
}
