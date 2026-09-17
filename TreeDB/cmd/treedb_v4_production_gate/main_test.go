package main

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestMeasureLaneWarmsAndAlternatesCompleteArms(t *testing.T) {
	queries := [][]float32{{1, 0}, {0, 1}}
	calls := 0
	result, err := measureLane("test", queries, func(_ context.Context, query []float32, mode collections.VectorIndexQueryMode) (measuredResult, error) {
		calls++
		if len(query) != 2 || (mode != collections.VectorIndexQueryModeExact && mode != collections.VectorIndexQueryModeQuantizedRerank) {
			t.Fatalf("query=%v mode=%q", query, mode)
		}
		return measuredResult{
			Checksum: uint64(calls),
			Results:  []resultObservation{{ID: "row", Score: float64(calls)}},
			Route:    map[string]any{"mode": mode},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := 2*len(queries) + gateRepetitions*2*len(queries)
	if calls != wantCalls || result.WarmupCount != len(queries) || len(result.Exact.Repetitions) != gateRepetitions || len(result.SQ8.Repetitions) != gateRepetitions {
		t.Fatalf("calls=%d want=%d result=%+v", calls, wantCalls, result)
	}
	for rep := range gateRepetitions {
		wantExactOrder := rep % 2
		if result.Exact.Repetitions[rep].ArmOrder != wantExactOrder || result.SQ8.Repetitions[rep].ArmOrder != 1-wantExactOrder {
			t.Fatalf("repetition %d exact/sq8 order=%d/%d", rep, result.Exact.Repetitions[rep].ArmOrder, result.SQ8.Repetitions[rep].ArmOrder)
		}
		for _, arm := range []*repetition{&result.Exact.Repetitions[rep], &result.SQ8.Repetitions[rep]} {
			if len(arm.Observations) != len(queries) || arm.Observations[0].Query != 0 || len(arm.Observations[0].Results) != 1 {
				t.Fatalf("repetition %d omitted ordered observations: %+v", rep, arm.Observations)
			}
		}
	}
}
