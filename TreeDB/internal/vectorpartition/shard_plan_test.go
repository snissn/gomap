package vectorpartition

import (
	"math"
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
			if first.TargetHotBytes != SelectedTargetHotBytesV1 || first.TraversalRowBytes != 128*FP32BytesPerDimensionV1 || first.GraphIdentityOverhead != GraphIdentityOverheadBytesV1 {
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
	undersized.TargetHotBytes = uint64(128*FP32BytesPerDimensionV1 + GraphIdentityOverheadBytesV1 - 1)
	impossible := base
	impossible.TargetHotBytes = uint64(128*FP32BytesPerDimensionV1 + GraphIdentityOverheadBytesV1)
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
