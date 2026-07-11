package main

import (
	"fmt"
	"strings"
	"testing"
)

func benchmarkFixture(snapshotNS, publicNS, snapshotBytes, publicBytes float64, samples int) string {
	var b strings.Builder
	for _, keys := range []int{1024, 16384} {
		for _, operation := range []string{"seek", "next"} {
			for _, mode := range []string{"snapshot", "public"} {
				ns := publicNS
				bytesPerOp := publicBytes
				if mode == "snapshot" {
					ns = snapshotNS
					bytesPerOp = snapshotBytes
				}
				rowName := mode + "_" + operation
				if mode == "public" {
					rowName += "_baseline"
				}
				for i := 0; i < samples; i++ {
					fmt.Fprintf(&b, "BenchmarkSnapshotIteratorSeekNext/keys=%d/%s-4 1000 %.3f ns/op %.0f B/op 0 allocs/op\n",
						keys, rowName, ns+float64(i%3), bytesPerOp)
				}
			}
		}
	}
	return b.String()
}

func TestEvaluatePassesSameBinaryGate(t *testing.T) {
	rows, err := parseBenchmarkOutput(benchmarkFixture(104, 100, 0, 0, 7))
	if err != nil {
		t.Fatal(err)
	}
	rep := evaluate(rows, metadata{Head: "deadbeef"}, 7, 0.05)
	if !rep.Passed || len(rep.Cases) != 4 || len(rep.Violations) != 0 {
		t.Fatalf("report = %+v, want passing four-case report", rep)
	}
	if got := markdown(rep); !strings.Contains(got, "Exact head: `deadbeef`") || !strings.Contains(got, "PASS") {
		t.Fatalf("unexpected Markdown report:\n%s", got)
	}
}

func TestEvaluateRejectsTimingAndAllocationRegression(t *testing.T) {
	rows, err := parseBenchmarkOutput(benchmarkFixture(106, 100, 8, 0, 7))
	if err != nil {
		t.Fatal(err)
	}
	rep := evaluate(rows, metadata{}, 7, 0.05)
	if rep.Passed || len(rep.Violations) != 8 {
		t.Fatalf("violations = %d, passed = %v; want two violations per case", len(rep.Violations), rep.Passed)
	}
}

func TestEvaluateRejectsMissingSamples(t *testing.T) {
	rows, err := parseBenchmarkOutput(benchmarkFixture(100, 100, 0, 0, 6))
	if err != nil {
		t.Fatal(err)
	}
	rep := evaluate(rows, metadata{}, 7, 0.05)
	if rep.Passed || len(rep.Violations) != 4 || len(rep.Cases) != 0 {
		t.Fatalf("report = %+v, want four missing-sample violations", rep)
	}
}

func TestParseBenchmarkOutputRejectsMissingAllocationMetric(t *testing.T) {
	_, err := parseBenchmarkOutput("BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek-4 1000 100 ns/op 0 B/op\n")
	if err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("error = %v, want malformed row", err)
	}
}
