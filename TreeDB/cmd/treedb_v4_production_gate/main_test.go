package main

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestMeasureLaneWarmsAndAlternatesCompleteArms(t *testing.T) {
	queries := [][]float32{{1, 0}, {0, 1}}
	calls := 0
	result, err := measureLane("test", queries, func(_ context.Context, query []float32, mode collections.VectorIndexQueryMode) (uint64, error) {
		calls++
		if len(query) != 2 || (mode != collections.VectorIndexQueryModeExact && mode != collections.VectorIndexQueryModeQuantizedRerank) {
			t.Fatalf("query=%v mode=%q", query, mode)
		}
		return uint64(calls), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := 2*len(queries) + gateRepetitions*2*len(queries)
	if calls != wantCalls || result.WarmupCount != len(queries) || len(result.Exact.Repetitions) != gateRepetitions || len(result.SQ8.Repetitions) != gateRepetitions {
		t.Fatalf("calls=%d want=%d result=%+v", calls, wantCalls, result)
	}
	for repetition := range gateRepetitions {
		wantExactOrder := repetition % 2
		if result.Exact.Repetitions[repetition].ArmOrder != wantExactOrder || result.SQ8.Repetitions[repetition].ArmOrder != 1-wantExactOrder {
			t.Fatalf("repetition %d exact/sq8 order=%d/%d", repetition, result.Exact.Repetitions[repetition].ArmOrder, result.SQ8.Repetitions[repetition].ArmOrder)
		}
	}
}
