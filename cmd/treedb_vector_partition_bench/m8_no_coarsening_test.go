package main

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestM8NoCoarseningOneNearestVersusNineNeighbors(t *testing.T) {
	order, cost, err := m8NoCoarseningDomainsV1([]m8ExactPackBestV1{{true, "a", .99}, {true, "b", .98}}, []uint32{0, 1}, 2)
	if err != nil || !reflect.DeepEqual(order, []uint32{0, 1}) {
		t.Fatalf("%v %v", order, err)
	}
	mask, _, err := m8RouteCoverageV1([]uint16{1, 1022}, cost, order[:1])
	if err != nil || mask != 1 {
		t.Fatalf("%d %v", mask, err)
	}
	var s m8CoverageScratchV1
	curve, err := m8CoverageCostCurveV1(context.Background(), 10, []uint16{1, 1022}, cost, coverageTestLimits, &s)
	if err != nil {
		t.Fatal(err)
	}
	if hits, err := curve.hitsAtBudget(1); err != nil || hits != 9 {
		t.Fatalf("%d %v", hits, err)
	}
}
func TestM8NoCoarseningUsesNonTruthMembersAndPackUnion(t *testing.T) {
	best := []m8ExactPackBestV1{{true, "z", .7}, {true, "a", .9}, {true, "outside_truth", .8}, {}}
	order, cost, err := m8NoCoarseningDomainsV1(best, []uint32{0, 0, 1, 2}, 3)
	if err != nil || !reflect.DeepEqual(order, []uint32{0, 1, 2}) || !reflect.DeepEqual(cost, []int64{2, 1, 1}) {
		t.Fatalf("%v %v %v", order, cost, err)
	}
	mask, c, err := m8RouteCoverageV1([]uint16{1, 2, 0}, cost, order[:2])
	if err != nil || mask != 3 || c != 3 {
		t.Fatalf("%d %d %v", mask, c, err)
	}
	if _, _, err = m8RouteCoverageV1([]uint16{1, 2}, []int64{1, 1}, []uint32{0, 0}); err == nil {
		t.Fatal("duplicate route accepted")
	}
	best[2].Score = .9
	order, _, err = m8NoCoarseningDomainsV1(best, []uint32{0, 0, 1, 2}, 3)
	if err != nil || order[0] != 0 {
		t.Fatal("domain tie not stable")
	}
}
func TestM8NoCoarseningRejectsInvalidPackClosure(t *testing.T) {
	for _, tc := range []struct {
		best    []m8ExactPackBestV1
		owner   []uint32
		domains int
	}{
		{[]m8ExactPackBestV1{{true, "a", 1}}, nil, 1},
		{[]m8ExactPackBestV1{{true, "a", 1}}, []uint32{2}, 1},
		{[]m8ExactPackBestV1{{true, "a", 1}, {}}, []uint32{0, 0}, 2},
		{[]m8ExactPackBestV1{{true, "a", float32(math.NaN())}}, []uint32{0}, 1},
		{[]m8ExactPackBestV1{{true, "", 1}}, []uint32{0}, 1},
		{[]m8ExactPackBestV1{{false, "fabricated", 0}}, []uint32{0}, 1},
	} {
		if _, _, err := m8NoCoarseningDomainsV1(tc.best, tc.owner, tc.domains); err == nil {
			t.Fatal("invalid best/ownership accepted")
		}
	}
}
func TestM8AttributionAllowsMissingObservationButRejectsLostScoredTruth(t *testing.T) {
	scored, retained := uint16(3), uint16(1)
	if err := (m8ObservedTruthMasksV1{7, &scored, &retained, 1}).validate(3); err != nil {
		t.Fatal(err)
	}
	if err := (m8ObservedTruthMasksV1{7, nil, nil, 1}).validate(3); err != nil {
		t.Fatal(err)
	}
	if err := (m8ObservedTruthMasksV1{7, &scored, &retained, 4}).validate(3); err == nil {
		t.Fatal("unscored truth returned")
	}
	retained = 7
	if err := (m8ObservedTruthMasksV1{7, &scored, &retained, 1}).validate(3); err == nil {
		t.Fatal("retention added unscored truth")
	}
	if err := (m8ObservedTruthMasksV1{8, nil, nil, 0}).validate(3); err == nil {
		t.Fatal("out-of-range bit")
	}
}
