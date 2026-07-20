package collections

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollectionVectorPartitionManifestV1BindsIndexAndReopens(t *testing.T) {
	d := openCollectionCommandWALDB(t, t.TempDir())
	defer d.Close()
	def := VectorIndexDefinition{Name: "embedding", Field: "v", Metric: VectorMetricCosine, Dimensions: 3, Strategy: VectorIndexStrategyColumnGraph}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(col.meta.VectorIndexes[0])
	writeVectorPartitionAssetsForTest(t, d.Dir(), &m)
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m); err != nil {
		t.Fatal(err)
	}
	status, err := col.VectorPartitionStatusV1("embedding", 7)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Ready || status.GroupCount != 1 || status.AssetBytes != 28 {
		t.Fatalf("unexpected status: %+v", status)
	}
	m.IndexDefinitionDigest = strings.Repeat("f", 64)
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m); err == nil {
		t.Fatal("wrong index digest accepted")
	}
}

func writeVectorPartitionAssetsForTest(t *testing.T, root string, m *VectorPartitionManifestV1) {
	t.Helper()
	assets := append(append([]VectorPartitionAssetV1(nil), m.Assets...), m.RouterAsset)
	for i := range assets {
		raw := []byte(assets[i].ID)
		p := filepath.Join(root, assets[i].Path)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, raw, 0600); err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(raw)
		assets[i].Bytes = uint64(len(raw))
		assets[i].Checksum = hex.EncodeToString(h[:])
	}
	copy(m.Assets, assets[:len(m.Assets)])
	m.RouterAsset = assets[len(assets)-1]
	m.Canonicalize()
}

func TestVectorPartitionStoreV1CleanupRefusesReachableGeneration(t *testing.T) {
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.Publish(m); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("docs", "embedding", 7, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("active generation deleted")
	}
	newer := m
	newer.Generation = 8
	newer.RouterGeneration = 8
	newer.Canonicalize()
	if err := s.Publish(newer); err != nil {
		t.Fatal(err)
	}
	for _, e := range []VectorPartitionCleanupEligibilityV1{{Active: true}, {ReaderPins: 1}, {SnapshotReferences: 1}, {CatalogReferences: 1}} {
		if err := s.Delete("docs", "embedding", 7, e); err == nil {
			t.Fatalf("eligible=%+v deleted", e)
		}
	}
	if err := s.Delete("docs", "embedding", 7, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
}

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
	m := VectorPartitionManifestV1{State: "ready", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: h, SourceGeneration: 4, SourceChecksum: 9, SourceSchemaHash: 11, SourceRowCount: 2, Generation: 7, RouterGeneration: 7, PartitionCount: 2, BalancePolicy: "disjoint_v1", Placements: []VectorPartitionPlacementV1{{0, "raft-a"}, {1, "raft-a"}}, Memberships: []VectorPartitionMembershipV1{{0, 0}, {1, 1}}, Representatives: []VectorPartitionMembershipV1{{0, 0}}, Assets: []VectorPartitionAssetV1{{"partition/0", "assets/p0", b, 12}, {"partition/1", "assets/p1", b, 13}}, RouterAsset: VectorPartitionAssetV1{"router", "assets/router", b, 14}}
	m.Canonicalize()
	return m
}
