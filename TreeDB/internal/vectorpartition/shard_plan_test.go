package vectorpartition

import (
	"math"
	"slices"
	"strings"
	"testing"
)

func TestPlanByteBoundedShardsV1Deterministic100k250kEnvelope(t *testing.T) {
	for _, fixture := range []struct {
		name                string
		vectors             int
		wantPartitions      int
		wantHomeCapacity    int
		wantOverlapCapacity int
		wantRequested       int
		wantPlanned         int
	}{
		{name: "100k", vectors: 100_000, wantPartitions: 16, wantHomeCapacity: partitionCap(100_000, 16, DefaultConfig().Imbalance), wantOverlapCapacity: 7500, wantRequested: 20_000, wantPlanned: 120_000},
		{name: "250k", vectors: 250_000, wantPartitions: 40, wantHomeCapacity: partitionCap(250_000, 40, DefaultConfig().Imbalance), wantOverlapCapacity: 7500, wantRequested: 50_000, wantPlanned: 300_000},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			in := SelectedShardPlanRequestV1(fixture.vectors, 128)
			first, err := PlanByteBoundedShardsV1(in)
			if err != nil {
				t.Fatal(err)
			}
			second, err := PlanByteBoundedShardsV1(in)
			if err != nil {
				t.Fatal(err)
			}
			if first != second {
				t.Fatalf("nondeterministic plan\n%+v\n%+v", first, second)
			}
			if first.Partitions != fixture.wantPartitions || first.HomeCapacity != fixture.wantHomeCapacity || first.OverlapCapacity != fixture.wantOverlapCapacity || first.MaxMembershipsPerPack != 7500 || first.RequestedOverlap != fixture.wantRequested || first.PlannedMemberships != fixture.wantPlanned {
				t.Fatalf("plan=%+v", first)
			}
			if first.TargetHotBytes != SelectedTargetHotBytesV1 || first.TraversalRowBytes != 128*FP32BytesPerDimensionV1 || first.GraphIdentityOverhead != GraphIdentityOverheadBytesV1 || first.PackFixedOverhead != PackFixedOverheadBytesV1 {
				t.Fatalf("accounting=%+v", first)
			}
		})
	}
}

func TestPlanByteBoundedShardsV1FailsClosedBeforeAllocation(t *testing.T) {
	base := SelectedShardPlanRequestV1(100_000, 128)
	if _, err := PlanByteBoundedShardsV1(base); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	// Vectors is rejected by the maxVectors bound before any overlap arithmetic
	// runs, which is also why the planner's checked add/multiply guards are
	// unreachable for any accepted input: they exist so a future bound change
	// cannot silently wrap.
	outOfBounds := base
	outOfBounds.Vectors = math.MaxInt
	outOfBounds.OverlapRatio = 1
	undersized := base
	undersized.TargetHotBytes = uint64(PackFixedOverheadBytesV1 + 128*FP32BytesPerDimensionV1 + GraphIdentityOverheadBytesV1 - 1)
	impossible := base
	impossible.TargetHotBytes = uint64(PackFixedOverheadBytesV1 + 128*FP32BytesPerDimensionV1 + GraphIdentityOverheadBytesV1)
	nan := base
	nan.OverlapRatio = math.NaN()
	zero := base
	zero.Vectors = 0
	tooManyPartitions := base
	tooManyPartitions.MaxPartitions = maxPartitions + 1
	for name, in := range map[string]ShardPlanRequestV1{
		"vectors above bound":    outOfBounds,
		"undersized target":      undersized,
		"impossible balance":     impossible,
		"nan ratio":              nan,
		"zero vectors":           zero,
		"partitions above bound": tooManyPartitions,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := PlanByteBoundedShardsV1(in)
			if err == nil || got.Partitions != 0 {
				t.Fatalf("accepted %+v plan=%+v", in, got)
			}
			if !strings.Contains(err.Error(), "vectorpartition:") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestPlanByteBoundedShardsV1UsesExplicitTargetNotRuntimeLLC(t *testing.T) {
	in := SelectedShardPlanRequestV1(250_000, 128)
	in.TargetHotBytes = SelectedTargetHotBytesV1 * 2
	got, err := PlanByteBoundedShardsV1(in)
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetHotBytes != SelectedTargetHotBytesV1*2 || got.Partitions >= 40 {
		t.Fatalf("explicit larger target must reduce partition count: %+v", got)
	}
}

func TestPlanByteBoundedShardsV1SeparatesLogicalDomainsFromPhysicalPacks(t *testing.T) {
	for _, tc := range []struct {
		domains, packs, packsPerDomain, domainCapacity, packCapacity int
	}{
		{domains: 4, packs: 40, packsPerDomain: 10, domainCapacity: 75_000, packCapacity: 7_500},
		{domains: 16, packs: 48, packsPerDomain: 3, domainCapacity: 18_750, packCapacity: 6_250},
		{domains: 40, packs: 40, packsPerDomain: 1, domainCapacity: 7_500, packCapacity: 7_500},
	} {
		in := SelectedShardPlanRequestV1(250_000, 128)
		in.LogicalDomains = tc.domains
		got, err := PlanByteBoundedShardsV1(in)
		if err != nil {
			t.Fatalf("domains=%d: %v", tc.domains, err)
		}
		if got.LogicalDomains != tc.domains || got.Partitions != tc.packs || got.PacksPerDomain != tc.packsPerDomain ||
			got.DomainOverlapCapacity != tc.domainCapacity || got.OverlapCapacity != tc.packCapacity {
			t.Fatalf("domains=%d plan=%+v", tc.domains, got)
		}
	}
}

func TestPackDomainMembershipsV1DeterministicAndBounded(t *testing.T) {
	in := ShardPlanInputV1{
		Vectors: 8, Dimensions: 2, LogicalDomains: 2, OverlapRatio: .5, Imbalance: 0,
		TargetHotBytes: uint64(PackFixedOverheadBytesV1 + 3*(alignedRowBytesForTest(2)+GraphIdentityOverheadPerRowV1)),
	}
	plan, err := PlanByteBoundedShardsV1(in)
	if err != nil {
		t.Fatal(err)
	}
	logical := OverlapResult{
		Capacity: 6, Budget: 4, Used: 4, Useful: 4, Loads: []int{6, 6},
		Memberships: []Membership{
			{VectorOrdinal: 0, Partition: 0, Home: true}, {VectorOrdinal: 0, Partition: 1},
			{VectorOrdinal: 1, Partition: 0, Home: true}, {VectorOrdinal: 1, Partition: 1},
			{VectorOrdinal: 2, Partition: 0, Home: true},
			{VectorOrdinal: 3, Partition: 0, Home: true},
			{VectorOrdinal: 4, Partition: 0}, {VectorOrdinal: 4, Partition: 1, Home: true},
			{VectorOrdinal: 5, Partition: 0}, {VectorOrdinal: 5, Partition: 1, Home: true},
			{VectorOrdinal: 6, Partition: 1, Home: true},
			{VectorOrdinal: 7, Partition: 1, Home: true},
		},
	}
	first, err := PackDomainMembershipsV1(plan, logical)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PackDomainMembershipsV1(plan, logical)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.Memberships, second.Memberships) || !slices.Equal(first.Loads, []int{3, 3, 3, 3}) || first.Capacity != 3 {
		t.Fatalf("packed=%+v", first)
	}
	for _, membership := range first.Memberships {
		gotDomain := membership.Partition / plan.PacksPerDomain
		if !slices.ContainsFunc(logical.Memberships, func(candidate Membership) bool {
			return candidate.VectorOrdinal == membership.VectorOrdinal && candidate.Home == membership.Home && candidate.Partition == gotDomain
		}) {
			t.Fatalf("membership escaped logical domain: %+v", membership)
		}
	}
	bad := logical
	bad.Loads = []int{5, 7}
	if _, err := PackDomainMembershipsV1(plan, bad); err == nil {
		t.Fatal("accepted logical loads unrelated to memberships")
	}

	smallPlan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{
		Vectors: 6, Dimensions: 2, LogicalDomains: 3, OverlapRatio: .5, Imbalance: 0,
		TargetHotBytes: uint64(PackFixedOverheadBytesV1 + 2*(alignedRowBytesForTest(2)+GraphIdentityOverheadPerRowV1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	tooSmall := OverlapResult{Capacity: 3, Loads: []int{1, 2, 3}, Memberships: []Membership{
		{VectorOrdinal: 0, Partition: 0, Home: true},
		{VectorOrdinal: 1, Partition: 1, Home: true}, {VectorOrdinal: 2, Partition: 1, Home: true},
		{VectorOrdinal: 3, Partition: 2, Home: true}, {VectorOrdinal: 4, Partition: 2, Home: true}, {VectorOrdinal: 5, Partition: 2, Home: true},
	}}
	if _, err := PackDomainMembershipsV1(smallPlan, tooSmall); err == nil || !strings.Contains(err.Error(), "physical pack 1 is empty") {
		t.Fatalf("undersized domain err=%v", err)
	}
}
