package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func alignedRowBytesForTest(dimensions int) int {
	traversal, ok := vectorpartition.AlignedTraversalRowBytesV1(dimensions)
	if !ok {
		panic("aligned traversal charge")
	}
	return traversal
}

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
		name              string
		vectors           int
		wantPartitions    int
		explicitDomains   int
		wantExplicitPacks int
	}{
		{"100k", 100_000, 16, 40, 40},
		{"250k", 250_000, 40, 16, 48},
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
			// An explicit count selects logical graph domains. The planner then
			// derives enough physical packs for every domain's membership bound.
			explicit := cfg
			explicit.partitions = tc.explicitDomains
			explicitPlan, err := applyByteBoundedShardPlanV1(explicit, fixture)
			if err != nil || explicitPlan.shardPlan.LogicalDomains != tc.explicitDomains || explicitPlan.partitions != tc.wantExplicitPacks || explicitPlan.partition.Partitions != tc.explicitDomains {
				t.Fatalf("domains=%d packs=%d graph=%d err=%v", explicitPlan.shardPlan.LogicalDomains, explicitPlan.partitions, explicitPlan.partition.Partitions, err)
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
		TargetHotBytes: uint64(vectorpartition.PackFixedOverheadBytesV1 + 3*(alignedRowBytesForTest(2)+vectorpartition.GraphIdentityOverheadPerRowV1)),
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

func TestM3ActualShardPackBytesStayInsidePlannedEnvelopeV1(t *testing.T) {
	summaries := []vectorpartition.ShardPackSummaryV1{
		{Partition: 0, Rows: 2, Bytes: 100},
		{Partition: 1, Rows: 1, Bytes: 80},
	}
	assets := []collections.VectorPartitionAssetV1{
		{PartitionID: 1, Bytes: 80},
		{PartitionID: 0, Bytes: 99},
	}
	if err := m3ValidateActualShardPackBytesV1(assets, summaries, 1); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]collections.VectorPartitionAssetV1){
		"over":      func(a []collections.VectorPartitionAssetV1) { a[0].Bytes++ },
		"zero":      func(a []collections.VectorPartitionAssetV1) { a[1].Bytes = 0 },
		"duplicate": func(a []collections.VectorPartitionAssetV1) { a[1].PartitionID = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := append([]collections.VectorPartitionAssetV1(nil), assets...)
			mutate(candidate)
			if err := m3ValidateActualShardPackBytesV1(candidate, summaries, 1); err == nil {
				t.Fatal("accepted pack bytes outside the planned envelope")
			}
		})
	}
	if err := m3ValidateActualShardPackBytesV1(assets[:1], summaries, 1); err == nil {
		t.Fatal("accepted incomplete pack coverage")
	}
	if err := m3ValidateActualShardPackBytesV1(assets, nil, 1); err != nil {
		t.Fatalf("unplanned packs rejected: %v", err)
	}
	domainSummaries := []vectorpartition.ShardPackSummaryV1{
		{Partition: 0, Bytes: 40}, {Partition: 1, Bytes: 60},
		{Partition: 2, Bytes: 30}, {Partition: 3, Bytes: 50},
	}
	domainAssets := []collections.VectorPartitionAssetV1{
		{PartitionID: 0, Bytes: 20}, {PartitionID: 0, Bytes: 80},
		{PartitionID: 2, Bytes: 80},
	}
	if err := m3ValidateActualShardPackBytesV1(domainAssets, domainSummaries, 2); err != nil {
		t.Fatalf("domain chunks rejected: %v", err)
	}
	domainAssets[0].PartitionID = 1
	if err := m3ValidateActualShardPackBytesV1(domainAssets, domainSummaries, 2); err == nil {
		t.Fatal("accepted a chunk on a non-anchor pack")
	}
}

func TestM3ServingPartitionsCoalescePhysicalDomainPacksV1(t *testing.T) {
	manifest := collections.VectorPartitionManifestV1{
		PartitionCount: 4, DomainCount: 2,
		DomainPacks: []collections.VectorPartitionDomainPackV1{
			{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1},
			{DomainID: 1, PackID: 2}, {DomainID: 1, PackID: 3},
		},
	}
	partitions, members, err := m3ServingPartitionsV1(manifest, [][]int{{3, 1}, {2, 1}, {5}, {4}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(partitions, []uint32{0, 2}) || !reflect.DeepEqual(members, [][]int{{1, 2, 3}, {4, 5}}) {
		t.Fatalf("serving partitions=%v members=%v", partitions, members)
	}
}

// TestM3ShardGenerationDescriptorPersistsAndReopensV1 covers the retained
// artifact itself: a byte-bounded build must leave a shard-generation record
// that reopens under its bound digest, must refuse to overwrite it, and must
// refuse a row ratio outside the planned envelope.
func TestM3ShardGenerationDescriptorPersistsAndReopensV1(t *testing.T) {
	plan, err := vectorpartition.PlanByteBoundedShardsV1(vectorpartition.ShardPlanInputV1{
		Vectors: 4, Dimensions: 2, LogicalDomains: 1, OverlapRatio: .5, Imbalance: 0,
		TargetHotBytes: uint64(vectorpartition.PackFixedOverheadBytesV1 + 3*(alignedRowBytesForTest(2)+vectorpartition.GraphIdentityOverheadPerRowV1)),
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
	raw, digest, err := m3ShardGenerationRecordV1(plan, plan.OverlapRatio, overlap, nil)
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
	if _, _, err := m3ShardGenerationRecordV1(plan, plan.OverlapRatio+.1, overlap, nil); err == nil {
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
	if _, _, err := m3ShardGenerationRecordV1(plan, 0, disjoint, nil); err != nil {
		t.Fatalf("disjoint variant rejected on a shared envelope: %v", err)
	}
	// A record holding replicas may not relabel itself as disjoint.
	if _, _, err := m3ShardGenerationRecordV1(plan, 0, overlap, nil); err == nil {
		t.Fatal("accepted replicas under a ratio that requests none")
	}
	// A record from another variant that shares this plan must not be accepted
	// for a descriptor whose own accounting disagrees with it.
	descriptor := m3VariantDescriptorV1{
		ShardPlan: plan, ShardGenerationDigest: digest, OverlapRatio: plan.OverlapRatio,
		Capacity: plan.OverlapCapacity, PartitionLoads: []int{3, 2},
		OverlapRealized: 1, OverlapMemberships: 1, SourceRows: uint64(plan.Vectors),
	}
	if err := m3VerifyRetainedShardGenerationV1(dir, descriptor); err != nil {
		t.Fatalf("matching record rejected: %v", err)
	}
	assets := []collections.VectorPartitionAssetV1{{
		PartitionID: 0,
		Bytes:       got.PackSummaries[0].Bytes + got.PackSummaries[1].Bytes,
	}}
	if _, err := m3ValidateRetainedShardPackBytesV1(dir, descriptor, assets); err != nil {
		t.Fatalf("matching retained pack bytes rejected: %v", err)
	}
	legacyPerPack := append(append([]collections.VectorPartitionAssetV1(nil), assets...), collections.VectorPartitionAssetV1{PartitionID: 1, Bytes: got.PackSummaries[1].Bytes})
	if _, err := m3ValidateRetainedShardPackBytesV1(dir, descriptor, legacyPerPack); err == nil {
		t.Fatal("accepted legacy per-pack materialization for a domain graph")
	}
	assets[0].Bytes++
	if _, err := m3ValidateRetainedShardPackBytesV1(dir, descriptor, assets); err == nil {
		t.Fatal("retained admission accepted a pack above its encoded byte envelope")
	}
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"ratio":       func(c *m3VariantDescriptorV1) { c.OverlapRatio = 0 },
		"capacity":    func(c *m3VariantDescriptorV1) { c.Capacity++ },
		"loads":       func(c *m3VariantDescriptorV1) { c.PartitionLoads = []int{2, 3} },
		"pack count":  func(c *m3VariantDescriptorV1) { c.PartitionLoads = []int{3} },
		"realized":    func(c *m3VariantDescriptorV1) { c.OverlapRealized = 0 },
		"memberships": func(c *m3VariantDescriptorV1) { c.OverlapMemberships = 0 },
		"source rows": func(c *m3VariantDescriptorV1) { c.SourceRows++ },
		"missing home receipt": func(c *m3VariantDescriptorV1) {
			c.KaHIPAdapterSHA256 = kahipHomePackingAdapterSHA256
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := descriptor
			candidate.PartitionLoads = append([]int(nil), descriptor.PartitionLoads...)
			mutate(&candidate)
			if err := m3VerifyRetainedShardGenerationV1(dir, candidate); err == nil {
				t.Fatalf("accepted a record whose %s disagrees with the descriptor", name)
			}
		})
	}
	// -shard-plan off retains no record rather than an unbound one.
	if raw, digest, err := m3ShardGenerationRecordV1(vectorpartition.ShardPlanV1{}, 0, overlap, nil); err != nil || raw != nil || digest != "" {
		t.Fatalf("unplanned build produced a record bytes=%d digest=%q err=%v", len(raw), digest, err)
	}
}

// TestM3ShardGenerationMembershipsBindMaterializationV1 pins the membership-pair
// binding. Aggregate per-partition loads cannot separate two assignments that
// share them, so a record that swaps equal numbers of vectors between packs
// must still be rejected against what materialized the packs.
func TestM3ShardGenerationMembershipsBindMaterializationV1(t *testing.T) {
	record := vectorpartition.ShardGenerationDescriptorV1{
		Plan: vectorpartition.ShardPlanV1{PacksPerDomain: 1},
		Memberships: []vectorpartition.Membership{
			{VectorOrdinal: 0, Partition: 0, Home: true},
			{VectorOrdinal: 1, Partition: 0, Home: true},
			{VectorOrdinal: 2, Partition: 1, Home: true},
			{VectorOrdinal: 3, Partition: 1, Home: true},
		},
		PackSummaries: []vectorpartition.ShardPackSummaryV1{{Partition: 0, Rows: 2}, {Partition: 1, Rows: 2}},
	}
	assignment := []int{0, 0, 1, 1}
	if err := m3VerifyShardGenerationMembershipsV1(record, [][]int{{0, 1}, {2, 3}}, assignment); err != nil {
		t.Fatalf("matching memberships rejected: %v", err)
	}
	// Same per-partition counts, different vectors: the aggregate checks cannot
	// see this, so the pair comparison has to.
	if err := m3VerifyShardGenerationMembershipsV1(record, [][]int{{0, 2}, {1, 3}}, assignment); err == nil {
		t.Fatal("accepted a record that swapped vectors between packs")
	}
	if err := m3VerifyShardGenerationMembershipsV1(record, [][]int{{0, 1, 2}, {3}}, assignment); err == nil {
		t.Fatal("accepted a record whose pack sizes disagree with materialization")
	}
	if err := m3VerifyShardGenerationMembershipsV1(record, [][]int{{0, 1}}, assignment); err == nil {
		t.Fatal("accepted a record covering more packs than materialization")
	}

	// Moving a vector's home onto one of its own overlap partitions preserves
	// every (vector, partition) pair, every pack row count, and the overlap
	// total, so the home/overlap classification has to be bound too.
	flipped := vectorpartition.ShardGenerationDescriptorV1{
		Plan: vectorpartition.ShardPlanV1{PacksPerDomain: 1},
		Memberships: []vectorpartition.Membership{
			{VectorOrdinal: 0, Partition: 0, Home: true},
			{VectorOrdinal: 1, Partition: 0},
			{VectorOrdinal: 1, Partition: 1, Home: true},
			{VectorOrdinal: 2, Partition: 1, Home: true},
		},
		PackSummaries: []vectorpartition.ShardPackSummaryV1{{Partition: 0, Rows: 2}, {Partition: 1, Rows: 2}},
	}
	// Vector 1's true home is partition 0, so this record is a home flip.
	if err := m3VerifyShardGenerationMembershipsV1(flipped, [][]int{{0, 1}, {1, 2}}, []int{0, 0, 1}); err == nil {
		t.Fatal("accepted a record that moved a home onto an overlap partition")
	}
	if err := m3VerifyShardGenerationMembershipsV1(record, [][]int{{0, 1}, {2, 3}}, []int{0, 0, 1}); err == nil {
		t.Fatal("accepted a membership naming a vector outside the assignment")
	}
}

// TestM3ShardGenerationInputsValidateWithoutEncodingV1 pins the temporary-run
// path: a run that will not retain the record must still fail closed on inputs
// that could not have produced one, without paying the encoding cost that would
// inflate the process peak-RSS evidence.
func TestM3ShardGenerationInputsValidateWithoutEncodingV1(t *testing.T) {
	plan, err := vectorpartition.PlanByteBoundedShardsV1(vectorpartition.ShardPlanInputV1{
		Vectors: 4, Dimensions: 2, OverlapRatio: .5, Imbalance: 0,
		TargetHotBytes: uint64(vectorpartition.PackFixedOverheadBytesV1 + 3*(alignedRowBytesForTest(2)+vectorpartition.GraphIdentityOverheadPerRowV1)),
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
	if err := m3ValidateShardGenerationInputsV1(plan, plan.OverlapRatio, overlap); err != nil {
		t.Fatalf("valid inputs rejected: %v", err)
	}
	// Same rejections the encoding path applies.
	if err := m3ValidateShardGenerationInputsV1(plan, plan.OverlapRatio+.1, overlap); err == nil {
		t.Fatal("accepted a row ratio outside the planned envelope")
	}
	mismatched := overlap
	mismatched.Loads = []int{2, 3}
	if err := m3ValidateShardGenerationInputsV1(plan, plan.OverlapRatio, mismatched); err == nil {
		t.Fatal("accepted loads unrelated to the membership list")
	}
	// -shard-plan off has no record to validate.
	if err := m3ValidateShardGenerationInputsV1(vectorpartition.ShardPlanV1{}, 0, overlap); err != nil {
		t.Fatalf("unplanned run rejected: %v", err)
	}
}
