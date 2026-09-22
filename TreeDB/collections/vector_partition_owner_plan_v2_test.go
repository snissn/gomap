package collections

import (
	"errors"
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
	_, members, home, overlap, err := plan.partition(0)
	if err != nil || len(members) != 1 || members[0].ordinal != 0 || home != 1 || overlap != 0 {
		t.Fatalf("local partition members=%v home=%d overlap=%d err=%v", members, home, overlap, err)
	}
	if len(plan.members) != 1 {
		t.Fatalf("owner-local plan retained %d memberships; want 1 local membership and no remote memberships", len(plan.members))
	}
	if _, _, _, _, err := plan.partition(1); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("remote partition admitted by owner-local plan: %v", err)
	}
}
