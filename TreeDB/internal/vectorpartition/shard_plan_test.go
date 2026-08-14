package vectorpartition

import (
	"math"
	"strings"
	"testing"
)

func TestPlanByteBoundedShardsV1Deterministic100k250kEnvelope(t *testing.T) {
	const dims = 128
	for _, fixture := range []struct {
		name                    string
		vectors                 int
		wantPartitions          int
		wantHomeCapacity        int
		wantOverlapCapacity     int
		wantMaxMemberships      int
		wantRequested           int
		wantPlanned             int
	}{
		{name: "100k", vectors: 100_000, wantPartitions: 16, wantHomeCapacity: 6563, wantOverlapCapacity: 7500, wantMaxMemberships: 7500, wantRequested: 20_000, wantPlanned: 120_000},
		{name: "250k", vectors: 250_000, wantPartitions: 40, wantHomeCapacity: 6563, wantOverlapCapacity: 7500, wantMaxMemberships: 7500, wantRequested: 50_000, wantPlanned: 300_000},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			in := ShardPlanInputV1{Vectors: fixture.vectors, Dimensions: dims, OverlapRatio: SelectedOverlapRatioV1, Imbalance: 0.05}
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
			if first.Partitions != fixture.wantPartitions || first.HomeCapacity != fixture.wantHomeCapacity || first.OverlapCapacity != fixture.wantOverlapCapacity || first.MaxMembershipsPerPack != fixture.wantMaxMemberships || first.RequestedOverlap != fixture.wantRequested || first.PlannedMemberships != fixture.wantPlanned {
				t.Fatalf("plan=%+v", first)
			}
			if first.TargetHotBytes != DefaultTargetHotBytesV1 || first.TraversalRowBytes != dims*FP32BytesPerDimensionV1 || first.GraphIdentityOverhead != GraphIdentityOverheadPerRowV1 || first.PackFixedOverhead != PackFixedOverheadBytesV1 {
				t.Fatalf("accounting=%+v", first)
			}
			if first.MaxPackBytes != DefaultTargetHotBytesV1 || first.MaxPackBytes > first.TargetHotBytes {
				t.Fatalf("pack bytes=%d target=%d", first.MaxPackBytes, first.TargetHotBytes)
			}
		})
	}
}

func TestPlanByteBoundedShardsV1FailsClosedBeforeAllocation(t *testing.T) {
	base := ShardPlanInputV1{Vectors: 100_000, Dimensions: 128, OverlapRatio: SelectedOverlapRatioV1, Imbalance: 0.05}
	if _, err := PlanByteBoundedShardsV1(base); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	for name, in := range map[string]ShardPlanInputV1{
		"overflow requested extras": {Vectors: math.MaxInt, Dimensions: 128, OverlapRatio: 1, Imbalance: 0.05},
		"undersized target":         {Vectors: 100_000, Dimensions: 128, OverlapRatio: SelectedOverlapRatioV1, Imbalance: 0.05, TargetHotBytes: uint64(PackFixedOverheadBytesV1 + 128*FP32BytesPerDimensionV1 + GraphIdentityOverheadPerRowV1 - 1)},
		"impossible balance":        {Vectors: 100_000, Dimensions: 128, OverlapRatio: SelectedOverlapRatioV1, Imbalance: 0.05, TargetHotBytes: DefaultTargetHotBytesV1, MaxPartitions: 15},
		"nan ratio":                 {Vectors: 8, Dimensions: 2, OverlapRatio: math.NaN(), Imbalance: 0.05},
		"zero vectors":              {Vectors: 0, Dimensions: 128, OverlapRatio: 0, Imbalance: 0.05},
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
	in := ShardPlanInputV1{Vectors: 250_000, Dimensions: 128, OverlapRatio: SelectedOverlapRatioV1, Imbalance: 0.05, TargetHotBytes: DefaultTargetHotBytesV1 * 2}
	got, err := PlanByteBoundedShardsV1(in)
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetHotBytes != DefaultTargetHotBytesV1*2 || got.Partitions >= 40 {
		t.Fatalf("explicit larger target must reduce partition count: %+v", got)
	}
}
