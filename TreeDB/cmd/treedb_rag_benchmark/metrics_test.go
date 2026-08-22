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
		{"all_in_topk", []string{"d", "c", "a", "b"}, map[string]bool{"a": true, "b": true, "c": true}, 10, 1.0},
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
	// Tiny constructed fixture: 4 queries over one relevant set of 3 chunks.
	rel := map[string]bool{"c1": true, "c2": true, "c3": true}
	row := &rowResult{Counters: map[string]float64{}}

	// Hand-computed per-query contributions:
	//   q1 ranked [c1, x, y]:            r5=|{c1}|/3=1/3, r10=1/3, mrr@10=1
	//   q2 ranked [x, c2, y, z]:         r5=1/3, r10=1/3, mrr@10=1/2
	//   q3 ranked [x, y, z, w, v, c3]:   r5=0,   r10=1/3, mrr@10=1/6
	queries := []struct {
		ranked []string
	}{
		{[]string{"c1", "x", "y"}},
		{[]string{"x", "c2", "y", "z"}},
		{[]string{"x", "y", "z", "w", "v", "c3"}},
	}
	for _, q := range queries {
		if err := accumulateQuality(row, q.ranked, rel); err != nil {
			t.Fatalf("accumulateQuality: %v", err)
		}
	}
	samples := 3.0
	row.RecallAt5 /= samples
	row.RecallAt10 /= samples
	row.RecallAt100 /= samples
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
	if !almostEqual(row.RecallAt100, 1.0/3.0) {
		t.Fatalf("recall@100=%f want %f", row.RecallAt100, 1.0/3.0)
	}
	if !almostEqual(row.MRRAt10, wantMRR) {
		t.Fatalf("mrr@10=%f want %f", row.MRRAt10, wantMRR)
	}
}
