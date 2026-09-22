package collections

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestVectorPartitionOwnerSearchOpenPlanDoesNotRetainRemoteMembershipsV2(t *testing.T) {
	manifest := VectorPartitionManifestV1{
		Collection:            "docs",
		IndexName:             "embedding",
		IndexDefinitionDigest: "definition",
		SourceGeneration:      11,
		SourceChecksum:        22,
		SourceSchemaHash:      33,
		SourceRowCount:        4097,
		Generation:            7,
		PartitionCount:        2,
		Placements: []VectorPartitionPlacementV1{
			{PartitionID: 0, GroupID: "local"},
			{PartitionID: 1, GroupID: "remote"},
		},
		Assets: []VectorPartitionAssetV1{
			{ID: vectorPartitionLocalAssetIDV1(0), PartitionID: 0},
			{ID: vectorPartitionLocalAssetIDV1(1), PartitionID: 1},
		},
		Memberships: []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}},
	}
	for ordinal := uint64(1); ordinal < manifest.SourceRowCount; ordinal++ {
		manifest.Memberships = append(manifest.Memberships, VectorPartitionMembershipV1{VectorOrdinal: ordinal, PartitionID: 1})
	}
	plan, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(t.Context(), manifest, "local")
	if err != nil {
		t.Fatal(err)
	}
	manifest.Memberships[0].VectorOrdinal = 99
	manifest.Assets[0].ID = "changed-after-plan"
	asset, members, home, overlap, err := plan.partition(0)
	if err != nil || len(members) != 1 || members[0].ordinal != 0 || home != 1 || overlap != 0 {
		t.Fatalf("local partition members=%v home=%d overlap=%d err=%v", members, home, overlap, err)
	}
	if asset.ID != vectorPartitionLocalAssetIDV1(0) {
		t.Fatal("owner plan retained caller-owned asset storage")
	}
	if len(plan.members) != 1 {
		t.Fatalf("owner-local plan retained %d memberships; want 1 local membership and no remote memberships", len(plan.members))
	}
	if _, _, _, _, err := plan.partition(1); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("remote partition admitted by owner-local plan: %v", err)
	}
}

func ownerPlanAllocationManifestV2(remote int) VectorPartitionManifestV1 {
	manifest := VectorPartitionManifestV1{
		Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "definition",
		Generation: 7, PartitionCount: 2, SourceRowCount: uint64(remote + 1),
		Placements: []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "local"}, {PartitionID: 1, GroupID: "remote"}},
		Assets: []VectorPartitionAssetV1{{ID: vectorPartitionLocalAssetIDV1(0), PartitionID: 0}, {ID: vectorPartitionLocalAssetIDV1(1), PartitionID: 1}},
		Memberships: make([]VectorPartitionMembershipV1, remote+1),
	}
	for i := 1; i <= remote; i++ {
		manifest.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 1}
	}
	return manifest
}

func TestVectorPartitionOwnerSearchOpenPlanAllocationGrowthV2(t *testing.T) {
	// This guard covers plan construction only. It deliberately does not claim
	// that the V1 input decode or source reader is owner-local.
	measure := func(remote int) int64 {
		manifest := ownerPlanAllocationManifestV2(remote)
		result := testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				plan, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(b.Context(), manifest, "local")
				if err != nil || len(plan.members) != 1 {
					b.Fatalf("owner plan err=%v", err)
				}
			}
		})
		return result.AllocedBytesPerOp()
	}
	small, large := measure(32), measure(32768)
	if large > small+2048 {
		t.Fatalf("owner plan allocations grew with remote rows: small=%d B/op large=%d B/op", small, large)
	}
}

func BenchmarkVectorPartitionOwnerSearchOpenPlanV2(b *testing.B) {
	for _, remote := range []int{32, 32768} {
		manifest := ownerPlanAllocationManifestV2(remote)
		b.Run(fmt.Sprintf("remote_%d/legacy_full", remote), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := NewVectorPartitionGenerationSearchOpenPlanWithContextV1(b.Context(), manifest); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("remote_%d/owner_plan", remote), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(b.Context(), manifest, "local"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestVectorPartitionOwnerSearchOpenPlanRefusesInvalidOwnerSelectionV2(t *testing.T) {
	manifest := VectorPartitionManifestV1{
		Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "definition",
		Generation: 7, PartitionCount: 2,
		Placements: []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "local"}, {PartitionID: 1, GroupID: "remote"}},
		Assets: []VectorPartitionAssetV1{{ID: vectorPartitionLocalAssetIDV1(0), PartitionID: 0}},
		Memberships: []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}},
	}
	for _, owner := range []string{"", "unknown"} {
		if _, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(t.Context(), manifest, owner); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
			t.Errorf("owner %q err=%v", owner, err)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(canceled, manifest, "local"); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled owner plan err=%v", err)
	}
	manifest.Placements[1].PartitionID = 0
	if _, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(t.Context(), manifest, "local"); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Errorf("duplicate placement err=%v", err)
	}
}

func TestVectorPartitionOwnerSearchOpenPlanPreservesColocatedDomainV2(t *testing.T) {
	manifest := VectorPartitionManifestV1{
		Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "definition",
		Generation: 7, PartitionCount: 4, DomainCount: 2,
		Placements: []VectorPartitionPlacementV1{
			{PartitionID: 0, GroupID: "local"}, {PartitionID: 1, GroupID: "local"},
			{PartitionID: 2, GroupID: "remote"}, {PartitionID: 3, GroupID: "remote"},
		},
		DomainPacks: []VectorPartitionDomainPackV1{
			{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1},
			{DomainID: 1, PackID: 2}, {DomainID: 1, PackID: 3},
		},
		Assets: []VectorPartitionAssetV1{
			{ID: vectorPartitionLocalAssetIDV1(0), PartitionID: 0},
			{ID: vectorPartitionLocalAssetIDV1(0) + "/section/edges/0", PartitionID: 0},
			{ID: vectorPartitionLocalAssetIDV1(2), PartitionID: 2},
			{ID: vectorPartitionLocalAssetIDV1(2) + "/section/edges/0", PartitionID: 2},
		},
		Memberships: []VectorPartitionMembershipV1{
			{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 1},
			{VectorOrdinal: 2, PartitionID: 2}, {VectorOrdinal: 3, PartitionID: 3},
		},
		OverlapMemberships: []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 1}},
	}
	plan, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(t.Context(), manifest, "local")
	if err != nil {
		t.Fatal(err)
	}
	_, members, home, overlap, err := plan.partition(0)
	if err != nil || len(members) != 2 || home != 2 || overlap != 0 {
		t.Fatalf("domain home-wins normalization members=%v home=%d overlap=%d err=%v", members, home, overlap, err)
	}
	if len(plan.partitionAssets(0)) != 2 || len(plan.members) != 2 {
		t.Fatalf("local domain lost chunks or retained remote metadata: chunks=%d members=%d", len(plan.partitionAssets(0)), len(plan.members))
	}
	for _, remote := range []uint32{1, 2, 3} {
		if _, _, _, _, err := plan.partition(remote); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
			t.Errorf("non-anchor or remote pack %d admitted: %v", remote, err)
		}
	}
	manifest.Placements[1].GroupID = "remote"
	if _, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(t.Context(), manifest, "local"); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Errorf("split domain owner accepted: %v", err)
	}
}
