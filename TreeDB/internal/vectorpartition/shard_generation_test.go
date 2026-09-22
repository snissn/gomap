package vectorpartition

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func testShardRequestV1(a Artifact, ratio float64) ShardPlanRequestV1 {
	req := SelectedShardPlanRequestV1(len(a.IDs), a.Source.Dimensions)
	req.OverlapRatio = ratio
	req.Imbalance = a.Config.Imbalance
	traversal, ok := AlignedTraversalRowBytesV1(a.Source.Dimensions)
	if !ok {
		panic("aligned traversal charge")
	}
	req.TargetHotBytes = uint64(PackFixedOverheadBytesV1 + 3*(traversal+GraphIdentityOverheadBytesV1))
	return req
}

func TestShardGenerationDescriptorDeterministicAndReopenV1(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}, []int{0, 0, 1, 1})
	req := testShardRequestV1(a, .5)
	plan, err := PlanByteBoundedShardsV1(req)
	if err != nil {
		t.Fatal(err)
	}
	cfg := OverlapConfig{Ratio: .5, Capacity: plan.OverlapCapacity, UsefulOnly: true}
	overlap, err := BuildOverlap(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewShardGenerationDescriptorV1(plan, cfg, overlap)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewShardGenerationDescriptorV1(plan, cfg, overlap)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := CanonicalShardGenerationJSONV1(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := CanonicalShardGenerationJSONV1(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) || first.MembershipDigest == "" || first.MembershipDigest != second.MembershipDigest {
		t.Fatalf("descriptor bytes or digest are not deterministic")
	}
	reopened, err := DecodeShardGenerationDescriptorV1(firstBytes, len(firstBytes))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reopened, first) {
		t.Fatalf("reopen=%+v want %+v", reopened, first)
	}
	if reopened.SchemaVersion != ShardGenerationDescriptorSchemaV1 || len(reopened.PackSummaries) != len(overlap.Loads) {
		t.Fatalf("reopened descriptor=%+v", reopened)
	}
	for _, summary := range reopened.PackSummaries {
		if summary.Rows <= 0 || summary.Bytes == 0 || summary.Bytes > plan.TargetHotBytes {
			t.Fatalf("pack summary=%+v plan=%+v", summary, plan)
		}
	}
}

func TestShardGenerationDescriptorRejectsStaleVersionAndConfigV1(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}, []int{0, 0, 1, 1})
	req := testShardRequestV1(a, .5)
	plan, err := PlanByteBoundedShardsV1(req)
	if err != nil {
		t.Fatal(err)
	}
	cfg := OverlapConfig{Ratio: .5, Capacity: plan.OverlapCapacity, UsefulOnly: true}
	overlap, err := BuildOverlap(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := NewShardGenerationDescriptorV1(plan, cfg, overlap)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalShardGenerationJSONV1(desc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeShardGenerationDescriptorV1(raw, len(raw)-1); err == nil {
		t.Fatal("accepted oversized-cap mismatch")
	}
	stale := append([]byte(nil), raw...)
	stale = bytes.Replace(stale, []byte(`"schema_version":1`), []byte(`"schema_version":0`), 1)
	if _, err := DecodeShardGenerationDescriptorV1(stale, len(stale)); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("stale version err=%v", err)
	}
	unknown := bytes.Replace(append([]byte(nil), raw...), []byte(`{"schema_version"`), []byte(`{"unknown_field":1,"schema_version"`), 1)
	if _, err := DecodeShardGenerationDescriptorV1(unknown, len(unknown)); err == nil {
		t.Fatal("accepted unknown field")
	}
	trailing := append(append([]byte(nil), raw...), []byte(`{}`)...)
	if _, err := DecodeShardGenerationDescriptorV1(trailing, len(trailing)); err == nil {
		t.Fatal("accepted trailing JSON")
	}
	mismatched := desc
	mismatched.Plan.TargetHotBytes++
	if err := ValidateShardGenerationDescriptorV1(mismatched); err == nil {
		t.Fatal("accepted recomputed-plan mismatch")
	}
	// The plan derives its requested budget from the ratio, so a descriptor may
	// not declare an overlap config the plan never authorized.
	relabeledRatio := desc
	relabeledRatio.OverlapConfig.Ratio = 0
	if err := ValidateShardGenerationDescriptorV1(relabeledRatio); err == nil {
		t.Fatal("accepted overlap ratio that contradicts the plan")
	}
	relabeledCapacity := desc
	relabeledCapacity.OverlapConfig.Capacity++
	if err := ValidateShardGenerationDescriptorV1(relabeledCapacity); err == nil {
		t.Fatal("accepted overlap capacity that contradicts the plan")
	}
	wrongDigest := desc
	wrongDigest.MembershipDigest = strings.Repeat("0", 64)
	if err := ValidateShardGenerationDescriptorV1(wrongDigest); err == nil {
		t.Fatal("accepted membership digest mismatch")
	}
}

func TestShardGenerationSummariesAreDerivedFromMembershipsV1(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}, []int{0, 0, 1, 1})
	plan, err := PlanByteBoundedShardsV1(testShardRequestV1(a, .5))
	if err != nil {
		t.Fatal(err)
	}
	cfg := OverlapConfig{Ratio: .5, Capacity: plan.OverlapCapacity, UsefulOnly: true}
	overlap, err := BuildOverlap(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := NewShardGenerationDescriptorV1(plan, cfg, overlap)
	if err != nil {
		t.Fatal(err)
	}
	if len(desc.PackSummaries) != plan.Partitions {
		t.Fatalf("summaries=%d want planned partitions=%d", len(desc.PackSummaries), plan.Partitions)
	}
	var homeRows, overlapRows int
	for _, summary := range desc.PackSummaries {
		homeRows += summary.HomeRows
		overlapRows += summary.OverlapRows
	}
	if homeRows != plan.Vectors || overlapRows != overlap.Used {
		t.Fatalf("derived home=%d overlap=%d want %d/%d", homeRows, overlapRows, plan.Vectors, overlap.Used)
	}
	for name, mutate := range map[string]func(*ShardGenerationDescriptorV1){
		"omitted pack": func(candidate *ShardGenerationDescriptorV1) {
			candidate.PackSummaries = candidate.PackSummaries[:len(candidate.PackSummaries)-1]
		},
		"unrelated load": func(candidate *ShardGenerationDescriptorV1) {
			candidate.PackSummaries[0].OverlapRows++
			candidate.PackSummaries[0].Rows++
		},
		"unrelated bytes": func(candidate *ShardGenerationDescriptorV1) {
			candidate.PackSummaries[0].Bytes++
		},
		"dropped membership": func(candidate *ShardGenerationDescriptorV1) {
			candidate.Memberships = candidate.Memberships[:len(candidate.Memberships)-1]
			candidate.MembershipDigest, _ = MembershipDigestV1(candidate.Memberships)
		},
		"duplicated membership": func(candidate *ShardGenerationDescriptorV1) {
			candidate.Memberships[1] = candidate.Memberships[0]
			candidate.MembershipDigest, _ = MembershipDigestV1(candidate.Memberships)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := desc
			candidate.Memberships = append([]Membership(nil), desc.Memberships...)
			candidate.PackSummaries = append([]ShardPackSummaryV1(nil), desc.PackSummaries...)
			mutate(&candidate)
			if err := ValidateShardGenerationDescriptorV1(candidate); err == nil {
				t.Fatalf("accepted %s: %+v", name, candidate.PackSummaries)
			}
		})
	}
}

func TestRouterPartitionsContainExactlyRealizedMembershipV1(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}, []int{0, 0, 1, 1})
	overlap, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 3, UsefulOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	vectors := [][]float32{{1, 0}, {.9, .1}, {.8, .2}, {.7, .3}}
	got, err := RouterPartitionsFromMembershipsV1(overlap.Memberships, vectors, a.Config.Partitions)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != a.Config.Partitions {
		t.Fatalf("partitions=%d", len(got))
	}
	var count int
	seen := map[[2]uint64]struct{}{}
	for _, partition := range got {
		for _, vector := range partition.Vectors {
			key := [2]uint64{vector.Ordinal, uint64(partition.PartitionID)}
			if _, dup := seen[key]; dup {
				t.Fatalf("duplicate router membership %+v", key)
			}
			seen[key] = struct{}{}
			count++
		}
	}
	if count != len(overlap.Memberships) {
		t.Fatalf("router memberships=%d overlap=%d", count, len(overlap.Memberships))
	}
	for _, membership := range overlap.Memberships {
		if _, ok := seen[[2]uint64{uint64(membership.VectorOrdinal), uint64(membership.Partition)}]; !ok {
			t.Fatalf("missing realized membership %+v", membership)
		}
	}
}

// TestAccountShardPacksRejectsPerVectorOverlapCapV1 pins the durable per-vector
// membership cap on the decode path. BuildOverlap enforces it while building,
// but a persisted descriptor is only ever checked through AccountShardPacksV1,
// so without this a self-consistent record could replicate one ordinal into
// more partitions than the builder could ever produce while every pack total
// stayed inside capacity.
func TestAccountShardPacksRejectsPerVectorOverlapCapV1(t *testing.T) {
	vectors := MaxOverlapMembershipsPerVector + 4
	plan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{
		Vectors: vectors, Dimensions: 2, OverlapRatio: 1, Imbalance: 1,
		TargetHotBytes: uint64(PackFixedOverheadBytesV1 + 2*(alignedRowBytesForTest(2)+GraphIdentityOverheadPerRowV1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Partitions < MaxOverlapMembershipsPerVector+2 {
		t.Fatalf("fixture needs more partitions than the cap: %+v", plan)
	}
	// One home per vector, then replicate ordinal 0 into as many partitions as
	// the cap allows. Every pack stays far inside its capacity.
	base := make([]Membership, 0, vectors+MaxOverlapMembershipsPerVector+1)
	for ordinal := 0; ordinal < vectors; ordinal++ {
		base = append(base, Membership{VectorOrdinal: ordinal, Partition: ordinal % plan.Partitions, Home: true})
	}
	replicate := func(count int) []Membership {
		out := make([]Membership, 0, len(base)+count)
		for partition := 1; partition <= count; partition++ {
			out = append(out, Membership{VectorOrdinal: 0, Partition: partition})
		}
		out = append(out, base...)
		sortMembershipsForTest(out)
		return out
	}
	if _, err := AccountShardPacksV1(plan, replicate(MaxOverlapMembershipsPerVector)); err != nil {
		t.Fatalf("rejected a vector at exactly the cap: %v", err)
	}
	if _, err := AccountShardPacksV1(plan, replicate(MaxOverlapMembershipsPerVector+1)); err == nil {
		t.Fatalf("accepted %d non-home memberships for one vector", MaxOverlapMembershipsPerVector+1)
	}
}

func sortMembershipsForTest(m []Membership) {
	sort.Slice(m, func(i, j int) bool {
		if m[i].VectorOrdinal != m[j].VectorOrdinal {
			return m[i].VectorOrdinal < m[j].VectorOrdinal
		}
		return m[i].Partition < m[j].Partition
	})
}

func alignedRowBytesForTest(dimensions int) int {
	traversal, ok := AlignedTraversalRowBytesV1(dimensions)
	if !ok {
		panic("aligned traversal charge")
	}
	return traversal
}
