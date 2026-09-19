package main

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"reflect"
	"testing"
)

var coverageTestLimits = m8CoverageLimitsV1{WorkUnits: 100_000_000, Bytes: 8 << 20}

func coverageExhaustive(masks []uint16, costs []int64, budget int64, maxDomains int) int {
	best := 0
	for subset := 0; subset < 1<<uint(len(masks)); subset++ {
		if bits.OnesCount(uint(subset)) > maxDomains {
			continue
		}
		var cost int64
		var mask uint16
		for d := range masks {
			if subset&(1<<uint(d)) != 0 {
				cost += costs[d]
				mask |= masks[d]
			}
		}
		if cost <= budget {
			best = max(best, bits.OnesCount16(mask))
		}
	}
	return best
}

func TestM8CoverageCostDPMatchesExhaustiveSmallGraphs(t *testing.T) {
	rng := rand.New(rand.NewSource(4744))
	var scratch m8CoverageScratchV1
	for trial := 0; trial < 180; trial++ {
		k, d := 1+rng.Intn(10), 1+rng.Intn(8)
		masks, costs := make([]uint16, d), make([]int64, d)
		var sum int64
		for i := range masks {
			masks[i] = uint16(rng.Intn(1 << uint(k)))
			costs[i] = int64(1 + rng.Intn(9))
			sum += costs[i]
		}
		curve, err := m8CoverageCostCurveV1(context.Background(), k, masks, costs, coverageTestLimits, &scratch)
		if err != nil {
			t.Fatal(err)
		}
		for b := int64(0); b <= sum; b++ {
			got, err := curve.hitsAtBudget(b)
			if err != nil {
				t.Fatal(err)
			}
			want := coverageExhaustive(masks, costs, b, d)
			if got != want {
				t.Fatalf("trial %d budget %d got=%d want=%d", trial, b, got, want)
			}
		}
	}
}

func TestM8CoverageCostIndependentMinimaDoNotProveJointFeasibility(t *testing.T) {
	masks := []uint16{1023, 31, 992}
	packs := []int64{8, 1, 1}
	var scratch m8CoverageScratchV1
	for _, tc := range []struct {
		costs  []int64
		budget int64
		want   int
	}{{[]int64{1, 1, 1}, 1, 10}, {packs, 2, 10}, {packs, 1, 5}} {
		curve, err := m8CoverageCostCurveV1(context.Background(), 10, masks, tc.costs, coverageTestLimits, &scratch)
		if err != nil {
			t.Fatal(err)
		}
		hits, err := curve.hitsAtBudget(tc.budget)
		if err != nil || hits != tc.want {
			t.Fatalf("%d %v", hits, err)
		}
	}
	for _, tc := range []struct {
		domains int
		want    int
	}{{0, 0}, {1, 5}, {2, 10}} {
		got, err := m8CoverageJointHitsV1(context.Background(), 10, masks, packs, tc.domains, 2, coverageTestLimits)
		if err != nil || got != tc.want {
			t.Fatalf("joint=%d err=%v want=%d", got, err, tc.want)
		}
	}
}

func TestM8CoverageCostJointDPMatchesExhaustive(t *testing.T) {
	rng := rand.New(rand.NewSource(917))
	for trial := 0; trial < 75; trial++ {
		k, d := 1+rng.Intn(10), 1+rng.Intn(7)
		masks, costs := make([]uint16, d), make([]int64, d)
		for i := range masks {
			masks[i] = uint16(rng.Intn(1 << uint(k)))
			costs[i] = int64(1 + rng.Intn(8))
		}
		for count := 0; count <= d; count++ {
			budget := int64(rng.Intn(32))
			got, err := m8CoverageJointHitsV1(context.Background(), k, masks, costs, count, budget, coverageTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			if want := coverageExhaustive(masks, costs, budget, count); got != want {
				t.Fatalf("trial=%d count=%d got=%d want=%d", trial, count, got, want)
			}
		}
	}
}

func TestM8CoverageCostRejectsInvalidIdentityAndOverflow(t *testing.T) {
	for _, tc := range []struct {
		k int
		m []uint16
		c []int64
	}{{0, []uint16{1}, []int64{1}}, {11, []uint16{1}, []int64{1}}, {1, []uint16{2}, []int64{1}}, {1, []uint16{1}, []int64{0}}, {1, []uint16{1}, []int64{-1}}, {1, []uint16{1}, []int64{math.MaxInt64}}, {1, []uint16{1, 0}, []int64{math.MaxInt64 - 1, 2}}, {1, nil, nil}, {1, []uint16{1}, nil}} {
		var s m8CoverageScratchV1
		if _, err := m8CoverageCostCurveV1(context.Background(), tc.k, tc.m, tc.c, coverageTestLimits, &s); err == nil {
			t.Fatalf("accepted %+v", tc)
		}
		if s.current != nil || s.next != nil {
			t.Fatal("allocated on invalid shape")
		}
	}
	for _, limits := range []m8CoverageLimitsV1{{1, 1 << 20}, {1 << 20, 1}, {0, 0}} {
		var s m8CoverageScratchV1
		if _, err := m8CoverageCostCurveV1(context.Background(), 10, []uint16{1023}, []int64{1}, limits, &s); err == nil {
			t.Fatal("accepted undersized cap")
		}
		if s.current != nil || s.next != nil {
			t.Fatal("allocated before cap check")
		}
	}
	if _, err := m8CoverageJointHitsV1(context.Background(), 10, []uint16{1023}, []int64{1}, 1, 1, m8CoverageLimitsV1{1, 1}); err == nil {
		t.Fatal("joint cap accepted")
	}
}

func TestM8CoverageCostCancellationAndScratchOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var s m8CoverageScratchV1
	if _, err := m8CoverageCostCurveV1(ctx, 1, []uint16{1}, []int64{1}, coverageTestLimits, &s); err != context.Canceled {
		t.Fatal(err)
	}
	first, err := m8CoverageCostCurveV1(context.Background(), 10, []uint16{1023}, []int64{2}, coverageTestLimits, &s)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]int64(nil), first.MinimumCosts...)
	_, err = m8CoverageCostCurveV1(context.Background(), 1, []uint16{1}, []int64{1}, coverageTestLimits, &s)
	if err != nil {
		t.Fatal(err)
	}
	if cap(s.current) != 2 || cap(s.next) != 2 {
		t.Fatal("oversized retained scratch")
	}
	if !reflect.DeepEqual(first.MinimumCosts, want) {
		t.Fatal("returned curve aliases reusable scratch")
	}
}

func TestM8CoverageMasksValidationAndOverlap(t *testing.T) {
	got, err := m8CoverageMasksV1([]string{"a", "b"}, map[string][]uint32{"a": {0, 1}, "b": {1}}, 2)
	if err != nil || !reflect.DeepEqual(got, []uint16{1, 3}) {
		t.Fatalf("%v %v", got, err)
	}
	for _, tc := range []struct {
		truth   []string
		members map[string][]uint32
	}{{[]string{"a", "a"}, map[string][]uint32{"a": {0}}}, {[]string{""}, map[string][]uint32{"": {0}}}, {[]string{"a"}, nil}, {[]string{"a"}, map[string][]uint32{"a": {0, 0}}}, {[]string{"a"}, map[string][]uint32{"a": {2}}}} {
		if _, err := m8CoverageMasksV1(tc.truth, tc.members, 2); err == nil {
			t.Fatal("accepted invalid membership")
		}
	}
}

func TestM8CoverageCostCurveValidation(t *testing.T) {
	for _, costs := range [][]int64{{1, 2}, {0, 2, 1}, {0, -1, 2}, {0, -2}} {
		c := m8CoverageCurveV1{TruthCount: len(costs) - 1, MinimumCosts: costs}
		if _, err := c.hitsAtBudget(10); err == nil {
			t.Fatal("accepted malformed curve")
		}
	}
	var s m8CoverageScratchV1
	curve, err := m8CoverageCostCurveV1(nil, 10, []uint16{1023}, []int64{math.MaxInt64 - 1}, coverageTestLimits, &s)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := curve.hitsAtBudget(math.MaxInt64); err != nil || got != 10 {
		t.Fatalf("%d %v", got, err)
	}
}

func BenchmarkM8CoverageCost(b *testing.B) {
	for _, domains := range []int{4, 16, 40, 128} {
		b.Run(fmt.Sprintf("domains=%d/k=10", domains), func(b *testing.B) {
			masks, costs := make([]uint16, domains), make([]int64, domains)
			for i := range masks {
				masks[i] = uint16(1 << uint(i%10))
				costs[i] = int64(1 + i%4)
			}
			var s m8CoverageScratchV1
			_, err := m8CoverageCostCurveV1(context.Background(), 10, masks, costs, coverageTestLimits, &s)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := m8CoverageCostCurveV1(context.Background(), 10, masks, costs, coverageTestLimits, &s); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// This benchmark compares the same single-cost oracle answers across a probe
// sweep. It does not time ANN queries or imply an end-to-end search speedup.
func BenchmarkM8CoverageOracleComparison(b *testing.B) {
	truth := make([]m8CanonicalResultV1, 10)
	members := make(map[string][]uint32, 10)
	ids := make([]string, 10)
	for i := range truth {
		id := fmt.Sprintf("truth-%02d", i)
		truth[i] = m8CanonicalResultV1{ID: id, Score: float32(1) - float32(i)/20}
		ids[i] = id
		a, c := uint32(i%16), uint32((i+5)%16)
		if a > c {
			a, c = c, a
		}
		members[id] = []uint32{a, c}
	}
	masks, err := m8CoverageMasksV1(ids, members, 16)
	if err != nil {
		b.Fatal(err)
	}
	costs := make([]int64, 16)
	for i := range costs {
		costs[i] = 1
	}
	probes := []int{1, 2, 4, 8, 16}
	limits := m8CoverageLimitsV1{WorkUnits: 1 << 30, Bytes: 1 << 20}
	b.Run("legacy-subsets", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			for _, p := range probes {
				if _, err := m8BestMembershipOracleRecallV1(truth, members, 16, p); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("cached-truth-mask", func(b *testing.B) {
		var scratch m8CoverageScratchV1
		if _, err := m8CoverageCostCurveV1(context.Background(), 10, masks, costs, limits, &scratch); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			curve, err := m8CoverageCostCurveV1(context.Background(), 10, masks, costs, limits, &scratch)
			if err != nil {
				b.Fatal(err)
			}
			for _, p := range probes {
				if _, err := curve.hitsAtBudget(int64(p)); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
