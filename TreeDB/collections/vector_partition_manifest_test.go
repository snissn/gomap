package collections

import (
	"strings"
	"testing"
)

func TestVectorPartitionManifestV1CanonicalRoundTripAndReopen(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got.ReadySetDigest == "" || got.Placements[0].PartitionID != 0 {
		t.Fatalf("bad round trip: %+v", got)
	}
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(m); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("docs", "embedding", 7); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionManifestV1RejectsTrailingAndMixedReadySet(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionManifestV1(append(raw, 0), DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("trailing accepted")
	}
	m.ReadySetDigest = strings.Repeat("0", 64)
	if _, err := EncodeVectorPartitionManifestV1(m); err != nil {
		t.Fatal(err)
	} // encoder canonicalizes the digest
	m.Canonicalize()
	m.Placements[0].GroupID = "other"
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("mixed ready set accepted")
	}
}

func TestVectorPartitionManifestV1DecodeCapsBeforeAllocation(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultVectorPartitionManifestLimits()
	limits.MaxBytes = len(raw) - 1
	if _, err := DecodeVectorPartitionManifestV1(raw, limits); err == nil {
		t.Fatal("byte cap accepted")
	}
}

func testVectorPartitionManifestV1() VectorPartitionManifestV1 {
	h := strings.Repeat("a", 64)
	b := strings.Repeat("b", 64)
	m := VectorPartitionManifestV1{Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: h, SourceGeneration: 4, SourceChecksum: 9, SourceSchemaHash: 11, Generation: 7, RouterGeneration: 7, PartitionCount: 2, BalancePolicy: "disjoint_v1", Placements: []VectorPartitionPlacementV1{{0, "raft-a"}, {1, "raft-a"}}, Memberships: []VectorPartitionMembershipV1{{2, 0}, {3, 1}}, Representatives: []VectorPartitionMembershipV1{{2, 0}}, Assets: []VectorPartitionAssetV1{{"partition/0", "assets/p0", b, 12}, {"partition/1", "assets/p1", b, 13}}, RouterAsset: VectorPartitionAssetV1{"router", "assets/router", b, 14}}
	m.Canonicalize()
	return m
}
