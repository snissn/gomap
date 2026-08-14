package vectorpartition

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestShardGenerationDescriptorDeterministicAndReopenV1(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}, []int{0, 0, 1, 1})
	plan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{Vectors: len(a.IDs), Dimensions: a.Source.Dimensions, OverlapRatio: .5, Imbalance: a.Config.Imbalance, TargetHotBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	cfg := SelectedOverlapConfigV1(plan.OverlapCapacity)
	cfg.Ratio = .5
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
	if reopened.SchemaVersion != ShardGenerationDescriptorSchemaV1 || len(reopened.PackSummaries) != plan.Partitions {
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
	plan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{Vectors: len(a.IDs), Dimensions: a.Source.Dimensions, OverlapRatio: .5, Imbalance: a.Config.Imbalance, TargetHotBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	cfg := SelectedOverlapConfigV1(plan.OverlapCapacity)
	cfg.Ratio = .5
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
	mismatched := desc
	mismatched.Plan.TargetHotBytes++
	if err := ValidateShardGenerationDescriptorV1(mismatched); err == nil {
		t.Fatal("accepted recomputed-plan mismatch")
	}
	wrongDigest := desc
	wrongDigest.MembershipDigest = strings.Repeat("0", 64)
	if err := ValidateShardGenerationDescriptorV1(wrongDigest); err == nil {
		t.Fatal("accepted membership digest mismatch")
	}
}

func TestRouterPartitionsContainExactlyRealizedMembershipV1(t *testing.T) {
	a := usefulOnlyArtifact(t, [][]int{{1, 2}, {0, 2}, {0, 1, 3}, {2}}, []int{0, 0, 1, 1})
	overlap, err := BuildOverlap(a, OverlapConfig{Ratio: .5, Capacity: 3, UsefulOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	vectors := [][]float32{{1, 0}, {.9, .1}, {.8, .2}, {.7, .3}}
	got, err := RouterPartitionsFromMembershipV1(a, overlap, vectors)
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
