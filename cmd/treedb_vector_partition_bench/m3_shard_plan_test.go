package main

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func byteBoundedShardPlanConfigV1() config {
	return config{
		shardPlanMode:  shardPlanModeByteBoundedV1,
		shardPlanRatio: -1,
		overlaps:       []float64{vectorpartition.SelectedOverlapRatioV1},
		probes:         []int{1},
		partition:      vectorpartition.DefaultConfig(),
	}
}

// TestByteBoundedShardPlanDerivesPartitionsBeforeConstructionV1 pins the
// selected 128-dimensional contract: the partition count and per-pack capacity
// come from the explicit hot-byte budget and the authoritative fixture shape,
// not from an operator-declared partition count.
func TestByteBoundedShardPlanDerivesPartitionsBeforeConstructionV1(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		vectors               int
		wantPartitions        int
		contradictingPartions int
	}{
		{"100k", 100_000, 16, 40},
		{"250k", 250_000, 40, 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := byteBoundedShardPlanConfigV1()
			fixture := fixtureManifest{Vectors: tc.vectors, Dimensions: vectorpartition.DefaultFP32DimensionsV1}
			planned, err := applyByteBoundedShardPlanV1(cfg, fixture)
			if err != nil {
				t.Fatal(err)
			}
			plan := planned.shardPlan
			if planned.partitions != tc.wantPartitions || planned.partition.Partitions != tc.wantPartitions || plan.Partitions != tc.wantPartitions {
				t.Fatalf("derived partitions=%d config=%d plan=%d want %d", planned.partitions, planned.partition.Partitions, plan.Partitions, tc.wantPartitions)
			}
			if plan.OverlapCapacity != vectorpartition.SelectedSearchableRowsPerPackV1 || plan.MaxMembershipsPerPack != vectorpartition.SelectedSearchableRowsPerPackV1 {
				t.Fatalf("plan capacity=%d memberships=%d want %d", plan.OverlapCapacity, plan.MaxMembershipsPerPack, vectorpartition.SelectedSearchableRowsPerPackV1)
			}
			if plan.HomeCapacity > plan.MaxMembershipsPerPack || plan.TargetHotBytes != vectorpartition.DefaultTargetHotBytesV1 {
				t.Fatalf("plan home=%d target=%d", plan.HomeCapacity, plan.TargetHotBytes)
			}
			// A declared count that contradicts the budget must fail before any
			// artifact, pack, or router asset is allocated.
			contradicting := cfg
			contradicting.partitions = tc.contradictingPartions
			if _, err := applyByteBoundedShardPlanV1(contradicting, fixture); err == nil || !strings.Contains(err.Error(), "contradicts the byte-bounded plan") {
				t.Fatalf("accepted -partitions %d against the %d-partition plan: %v", tc.contradictingPartions, tc.wantPartitions, err)
			}
		})
	}
}

// TestM3ShardPlanRejectsUngovernedArtifactV1 fails if a 250k/128-dimensional
// build still materializes 16 partitions while reporting the byte-bounded plan.
func TestM3ShardPlanRejectsUngovernedArtifactV1(t *testing.T) {
	cfg := byteBoundedShardPlanConfigV1()
	fixture := fixtureManifest{Vectors: 250_000, Dimensions: vectorpartition.DefaultFP32DimensionsV1}
	planned, err := applyByteBoundedShardPlanV1(cfg, fixture)
	if err != nil {
		t.Fatal(err)
	}
	plan := planned.shardPlan
	governed := vectorpartition.Artifact{
		IDs:     make([]string, fixture.Vectors),
		Source:  vectorpartition.Source{Dimensions: fixture.Dimensions},
		Config:  vectorpartition.Config{Partitions: plan.Partitions, Imbalance: plan.Imbalance},
		Metrics: vectorpartition.Metrics{Cap: plan.HomeCapacity},
	}
	ratios := []float64{vectorpartition.SelectedOverlapRatioV1}
	if err := m3ValidateShardPlanGovernsArtifactV1(plan, governed, ratios); err != nil {
		t.Fatalf("planned artifact rejected: %v", err)
	}
	for name, mutate := range map[string]func(*vectorpartition.Artifact){
		"legacy sixteen partitions": func(a *vectorpartition.Artifact) { a.Config.Partitions = 16 },
		"home capacity drift":       func(a *vectorpartition.Artifact) { a.Metrics.Cap = plan.MaxMembershipsPerPack + 1 },
		"dimension drift":           func(a *vectorpartition.Artifact) { a.Source.Dimensions++ },
		"row drift":                 func(a *vectorpartition.Artifact) { a.IDs = a.IDs[:len(a.IDs)-1] },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := governed
			mutate(&candidate)
			if err := m3ValidateShardPlanGovernsArtifactV1(plan, candidate, ratios); err == nil {
				t.Fatalf("accepted %s: partitions=%d cap=%d", name, candidate.Config.Partitions, candidate.Metrics.Cap)
			}
		})
	}
	if err := m3ValidateShardPlanGovernsArtifactV1(plan, governed, []float64{plan.OverlapRatio + .01}); err == nil {
		t.Fatal("accepted an overlap ratio the plan never provisioned")
	}
}

// TestM3ShardPackBudgetRejectsOversizedPacksV1 covers the realized-pack half of
// the contract: memberships that exceed the planned per-pack budget fail closed
// before the packs are materialized.
func TestM3ShardPackBudgetRejectsOversizedPacksV1(t *testing.T) {
	plan, err := vectorpartition.PlanByteBoundedShardsV1(vectorpartition.ShardPlanInputV1{
		Vectors: 4, Dimensions: 2, OverlapRatio: .5, Imbalance: 0,
		TargetHotBytes: 3 * (2*vectorpartition.FP32BytesPerDimensionV1 + vectorpartition.GraphIdentityOverheadPerRowV1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Partitions != 2 || plan.MaxMembershipsPerPack != 3 {
		t.Fatalf("unexpected fixture plan=%+v", plan)
	}
	overlap := vectorpartition.OverlapResult{
		Memberships: []vectorpartition.Membership{
			{VectorOrdinal: 0, Partition: 0, Home: true},
			{VectorOrdinal: 1, Partition: 0, Home: true},
			{VectorOrdinal: 2, Partition: 0},
			{VectorOrdinal: 2, Partition: 1, Home: true},
			{VectorOrdinal: 3, Partition: 1, Home: true},
		},
		Loads: []int{3, 2},
	}
	if err := m3ValidateShardPackBudgetV1(plan, overlap); err != nil {
		t.Fatalf("planned packs rejected: %v", err)
	}
	oversized := overlap
	oversized.Memberships = append(append([]vectorpartition.Membership(nil), overlap.Memberships...), vectorpartition.Membership{VectorOrdinal: 3, Partition: 0})
	oversized.Loads = []int{4, 2}
	if err := m3ValidateShardPackBudgetV1(plan, oversized); err == nil {
		t.Fatal("accepted a pack above the planned membership budget")
	}
	relabeled := overlap
	relabeled.Loads = []int{2, 3}
	if err := m3ValidateShardPackBudgetV1(plan, relabeled); err == nil {
		t.Fatal("accepted realized loads unrelated to the membership list")
	}
	// -shard-plan off keeps the legacy path unplanned rather than half-checked.
	if err := m3ValidateShardPackBudgetV1(vectorpartition.ShardPlanV1{}, overlap); err != nil {
		t.Fatalf("unplanned build rejected: %v", err)
	}
}

// TestM3ShardGenerationDescriptorPersistsAndReopensV1 covers the retained
// artifact itself: a byte-bounded build must leave a shard-generation record
// that reopens under its bound digest, must refuse to overwrite it, and must
// refuse a row ratio outside the planned envelope.
func TestM3ShardGenerationDescriptorPersistsAndReopensV1(t *testing.T) {
	plan, err := vectorpartition.PlanByteBoundedShardsV1(vectorpartition.ShardPlanInputV1{
		Vectors: 4, Dimensions: 2, OverlapRatio: .5, Imbalance: 0,
		TargetHotBytes: 3 * (2*vectorpartition.FP32BytesPerDimensionV1 + vectorpartition.GraphIdentityOverheadPerRowV1),
	})
	if err != nil {
		t.Fatal(err)
	}
	overlap := vectorpartition.OverlapResult{
		Memberships: []vectorpartition.Membership{
			{VectorOrdinal: 0, Partition: 0, Home: true},
			{VectorOrdinal: 1, Partition: 0, Home: true},
			{VectorOrdinal: 2, Partition: 0},
			{VectorOrdinal: 2, Partition: 1, Home: true},
			{VectorOrdinal: 3, Partition: 1, Home: true},
		},
		Loads: []int{3, 2}, Capacity: plan.OverlapCapacity,
	}
	raw, digest, err := m3ShardGenerationRecordV1(plan, plan.OverlapRatio, overlap)
	if err != nil || len(raw) == 0 || !m8SHA256V1(digest) {
		t.Fatalf("record bytes=%d digest=%q err=%v", len(raw), digest, err)
	}
	dir := t.TempDir()
	if err := m3WriteShardGenerationRecordV1(dir, raw, digest); err != nil {
		t.Fatal(err)
	}
	got, err := m3ReadShardGenerationDescriptorV1(dir, digest)
	if err != nil {
		t.Fatal(err)
	}
	if got.Plan != plan || len(got.PackSummaries) != plan.Partitions || got.MembershipDigest == "" {
		t.Fatalf("reopened descriptor=%+v", got)
	}
	var home, extra int
	for _, summary := range got.PackSummaries {
		home += summary.HomeRows
		extra += summary.OverlapRows
	}
	if home != plan.Vectors || extra != 1 {
		t.Fatalf("derived home=%d overlap=%d", home, extra)
	}
	// A retained database must present the record under its bound digest.
	if _, err := m3ReadShardGenerationDescriptorV1(dir, strings.Repeat("a", 64)); err == nil {
		t.Fatal("accepted a record under an unbound digest")
	}
	if _, err := m3ReadShardGenerationDescriptorV1(t.TempDir(), digest); err == nil {
		t.Fatal("accepted a byte-bounded database with no retained record")
	}
	if err := m3WriteShardGenerationRecordV1(dir, raw, digest); err == nil {
		t.Fatal("overwrote immutable shard generation record")
	}
	if _, _, err := m3ShardGenerationRecordV1(plan, plan.OverlapRatio+.1, overlap); err == nil {
		t.Fatal("encoded a row ratio outside the planned envelope")
	}
	// A variant may materialize less than the planned envelope so comparison
	// variants can share one geometry.
	disjoint := vectorpartition.OverlapResult{
		Memberships: []vectorpartition.Membership{
			{VectorOrdinal: 0, Partition: 0, Home: true},
			{VectorOrdinal: 1, Partition: 0, Home: true},
			{VectorOrdinal: 2, Partition: 1, Home: true},
			{VectorOrdinal: 3, Partition: 1, Home: true},
		},
		Loads: []int{2, 2}, Capacity: plan.OverlapCapacity,
	}
	if _, _, err := m3ShardGenerationRecordV1(plan, 0, disjoint); err != nil {
		t.Fatalf("disjoint variant rejected on a shared envelope: %v", err)
	}
	// A record holding replicas may not relabel itself as disjoint.
	if _, _, err := m3ShardGenerationRecordV1(plan, 0, overlap); err == nil {
		t.Fatal("accepted replicas under a ratio that requests none")
	}
	// -shard-plan off retains no record rather than an unbound one.
	if raw, digest, err := m3ShardGenerationRecordV1(vectorpartition.ShardPlanV1{}, 0, overlap); err != nil || raw != nil || digest != "" {
		t.Fatalf("unplanned build produced a record bytes=%d digest=%q err=%v", len(raw), digest, err)
	}
}
