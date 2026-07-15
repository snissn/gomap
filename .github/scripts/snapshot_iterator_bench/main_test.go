package main

import (
	"fmt"
	"strings"
	"testing"
)

func fixture(snapshot, public []float64) string {
	var b strings.Builder
	for _, keys := range []int{1024, 16384} {
		for _, op := range []string{"seek", "next"} {
			for i := range snapshot {
				order := "AB"
				modes := []string{"snapshot", "public"}
				if i%2 == 1 {
					order = "BA"
					modes = []string{"public", "snapshot"}
				}
				for _, mode := range modes {
					ns := public[i]
					name := "public_" + op + "_baseline"
					if mode == "snapshot" {
						ns = snapshot[i]
						name = "snapshot_" + op
					}
					fmt.Fprintf(&b, "# pair=%d order=%s\nBenchmarkSnapshotIteratorSeekNext/keys=%d/%s-1 1000 %.3f ns/op 0 B/op 0 allocs/op\n", i+1, order, keys, name, ns)
				}
			}
		}
	}
	return b.String()
}
func testMeta() metadata {
	return metadata{Head: "deadbeef", BinarySHA256: "abc", RunnerImage: "ubuntu", CPU: "cpu", Affinity: "1", GoVersion: "go"}
}
func TestEvaluateObservedBimodalFalseFailureUsesAlignedPairs(t *testing.T) {
	// The seven independently grouped hosted samples in #3788 have an over-5%
	// median delta even though the aligned AB/BA protocol below is within budget.
	if legacy := (median([]float64{355, 356, 357, 407, 408, 409, 410})/median([]float64{362, 363, 364, 362, 363, 364, 363}) - 1) * 100; legacy <= 5 {
		t.Fatalf("observed independent fixture delta=%f, want >5%%", legacy)
	}
	rows, err := parseBenchmarkOutput(fixture([]float64{355, 356, 357, 356, 407, 408, 409, 408}, []float64{362, 363, 364, 363, 410, 411, 412, 411}))
	if err != nil {
		t.Fatal(err)
	}
	rep := evaluate(rows, testMeta(), 8, .05)
	if !rep.Passed {
		t.Fatalf("paired report should pass: %+v", rep)
	}
	if rep.Cases[0].PairedMedianDeltaPercent >= 5 {
		t.Fatalf("aligned paired evidence must stay below the 5%% budget: %+v", rep.Cases[0])
	}
	if !strings.Contains(markdown(rep), "paired delta") {
		t.Fatal("report missing paired field")
	}
}
func TestEvaluateRejectsGenuinePairedRegressionAndAllocation(t *testing.T) {
	rows, err := parseBenchmarkOutput(fixture([]float64{108, 108, 108, 108, 108, 108, 108, 108}, []float64{100, 100, 100, 100, 100, 100, 100, 100}))
	if err != nil {
		t.Fatal(err)
	}
	rep := evaluate(rows, testMeta(), 8, .05)
	if rep.Passed || len(rep.Violations) != 4 {
		t.Fatalf("genuine paired regression report=%+v", rep)
	}
}
func TestEvaluateFailsClosedOnUnbalancedPairs(t *testing.T) {
	raw := fixture([]float64{100, 100, 100, 100, 100, 100, 100, 100}, []float64{100, 100, 100, 100, 100, 100, 100, 100})
	raw = strings.Replace(raw, "# pair=2 order=BA", "# pair=2 order=AB", 1)
	rows, err := parseBenchmarkOutput(raw)
	if err != nil {
		t.Fatal(err)
	}
	rep := evaluate(rows, testMeta(), 8, .05)
	if rep.Passed || len(rep.Cases) != 3 {
		t.Fatalf("unbalanced pairs report=%+v", rep)
	}
}
func TestParseRejectsMissingAnnotationAndMalformedMetrics(t *testing.T) {
	if _, err := parseBenchmarkOutput("BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek-1 1000 1 ns/op 0 B/op 0 allocs/op\n"); err == nil {
		t.Fatal("missing annotation passed")
	}
	if _, err := parseBenchmarkOutput("# pair=1 order=AB\nBenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek-1 1000 nope ns/op 0 B/op 0 allocs/op\n"); err == nil {
		t.Fatal("malformed metric passed")
	}
}
