package main

import (
	"math"
	"testing"
)

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-12
}

// TestRecallAtKHandComputed validates recall@k on hand-computed rankings.
func TestRecallAtKHandComputed(t *testing.T) {
	cases := []struct {
		name     string
		ranked   []string
		relevant map[string]bool
		k        int
		want     float64
	}{
		{"hit_at_2_of_1", []string{"a", "b", "c"}, map[string]bool{"b": true}, 2, 1.0},
		{"miss_beyond_k", []string{"a", "b", "c"}, map[string]bool{"b": true}, 1, 0.0},
		{"two_relevant_partial", []string{"d", "c", "a", "b"}, map[string]bool{"a": true, "b": true, "c": true}, 2, 1.0 / 3.0},
		{"all_in_topk", []string{"d", "c", "a", "b", "e", "f", "g", "h", "i", "j"}, map[string]bool{"a": true, "b": true, "c": true}, 10, 1.0},
	}
	for _, tc := range cases {
		got, err := recallAtK(tc.ranked, tc.relevant, tc.k)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !almostEqual(got, tc.want) {
			t.Fatalf("%s: recall@%d=%f want %f", tc.name, tc.k, got, tc.want)
		}
	}
	if _, err := recallAtK([]string{"a"}, map[string]bool{}, 5); err == nil {
		t.Fatal("empty relevant set must fail closed")
	}
	if _, err := recallAtK([]string{"a"}, map[string]bool{"a": true}, 0); err == nil {
		t.Fatal("non-positive k must fail closed")
	}
	if _, err := recallAtK([]string{"a"}, map[string]bool{"a": true}, 5); err == nil {
		t.Fatal("ranking depth below k must fail closed")
	}
}

// TestMRRAtKHandComputed validates MRR@10 on hand-computed rankings.
func TestMRRAtKHandComputed(t *testing.T) {
	cases := []struct {
		name     string
		ranked   []string
		relevant map[string]bool
		k        int
		want     float64
	}{
		{"first_rank", []string{"x", "y", "z"}, map[string]bool{"x": true}, 10, 1.0},
		{"second_rank", []string{"x", "y", "z"}, map[string]bool{"y": true}, 10, 0.5},
		{"third_rank", []string{"x", "y", "z"}, map[string]bool{"z": true}, 10, 1.0 / 3.0},
		{"beyond_k", []string{"w", "x", "y", "z"}, map[string]bool{"z": true}, 3, 0.0},
		{"no_hit", []string{"w", "x", "y"}, map[string]bool{"z": true}, 10, 0.0},
	}
	for _, tc := range cases {
		got, err := mrrAtK(tc.ranked, tc.relevant, tc.k)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !almostEqual(got, tc.want) {
			t.Fatalf("%s: mrr@%d=%f want %f", tc.name, tc.k, got, tc.want)
		}
	}
}

// TestPercentileHandComputed validates percentile interpolation.
func TestPercentileHandComputed(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i + 1)
	}
	p50, err := percentile(values, 50)
	if err != nil || !almostEqual(p50, 50.5) {
		t.Fatalf("p50=%v err=%v want 50.5", p50, err)
	}
	p99, err := percentile(values, 99)
	if err != nil || !almostEqual(p99, 99.01) {
		t.Fatalf("p99=%v err=%v want 99.01", p99, err)
	}
	single, err := percentile([]float64{7}, 99)
	if err != nil || single != 7 {
		t.Fatalf("single sample percentile=%v want 7", single)
	}
	if _, err := percentile(nil, 50); err == nil {
		t.Fatal("empty samples must fail closed")
	}
}

// TestAccumulateQualityTinyFixture is the metric-validation test from the
// issue: hand-computed expected recall/MRR on a tiny constructed fixture match
// the harness accumulation path exactly.
func TestAccumulateQualityTinyFixture(t *testing.T) {
	// Tiny constructed fixture: 3 queries over one relevant set of 3 chunks.
	rel := map[string]bool{"c1": true, "c2": true, "c3": true}
	row := &rowResult{Counters: map[string]float64{}}

	// Every ranking is at least 10 deep so recall@10 is a valid claim.
	queries := [][]string{
		{"c1", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9"},
		{"x1", "c2", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9"},
		{"x1", "x2", "x3", "x4", "x5", "c3", "x6", "x7", "x8", "x9"},
	}
	for _, ranked := range queries {
		if err := accumulateQuality(row, ranked, rel); err != nil {
			t.Fatalf("accumulateQuality: %v", err)
		}
	}
	samples := float64(len(queries))
	row.RecallAt5 /= samples
	row.RecallAt10 /= samples
	row.MRRAt10 /= samples

	wantR5 := (1.0/3.0 + 1.0/3.0 + 0) / 3
	wantR10 := 1.0 / 3.0
	wantMRR := (1.0 + 0.5 + 1.0/6.0) / 3
	if !almostEqual(row.RecallAt5, wantR5) {
		t.Fatalf("recall@5=%f want %f", row.RecallAt5, wantR5)
	}
	if !almostEqual(row.RecallAt10, wantR10) {
		t.Fatalf("recall@10=%f want %f", row.RecallAt10, wantR10)
	}
	if !almostEqual(row.MRRAt10, wantMRR) {
		t.Fatalf("mrr@10=%f want %f", row.MRRAt10, wantMRR)
	}
}
