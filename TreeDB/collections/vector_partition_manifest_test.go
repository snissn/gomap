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
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName = def.Name
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	_, graph, view, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	if len(view.VectorIndexState.Assets) == 0 {
		t.Fatal("missing typed state asset")
	}
	ref := view.VectorIndexState.Assets[0].Ref
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	for i := range m.Assets {
		m.Assets[i].Ref = ref
		m.Assets[i].Bytes = uint64(ref.Length)
		m.Assets[i].Path = ""
		m.Assets[i].Checksum = digest
	}
	m.RouterAsset.Ref = ref
	m.RouterAsset.Bytes = uint64(ref.Length)
	m.RouterAsset.Path = ""
	m.RouterAsset.Checksum = digest
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m); err != nil {
		t.Fatal(err)
	}
	status, err := col.VectorPartitionStatusV1(def.Name, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Ready || status.GroupCount != 1 || status.AssetBytes != uint64(ref.Length*3) {
		t.Fatalf("unexpected status: %+v", status)
	}
	plan, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Sources.PreparedRefs != 1 || plan.Sources.PinnedRefs != 1 {
		t.Fatalf("partition assets were not retained as prepared+pinned: %+v", plan.Sources)
	}
	store, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active"), []byte("not-a-generation\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
		t.Fatal("corrupt active vector partition pointer did not fail closed")
	}
	if err := store.Publish(m); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+"-999.vpm"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
		t.Fatal("corrupt retained vector partition manifest did not fail closed")
	}
	m.IndexDefinitionDigest = strings.Repeat("f", 64)
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m); err == nil {
		t.Fatal("wrong index digest accepted")
	}
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	for _, mutate := range []func(*VectorPartitionManifestV1){func(x *VectorPartitionManifestV1) { x.SourceGeneration++ }, func(x *VectorPartitionManifestV1) { x.SourceChecksum++ }, func(x *VectorPartitionManifestV1) { x.SourceSchemaHash++ }, func(x *VectorPartitionManifestV1) { x.SourceRowCount++ }} {
		bad := m
		mutate(&bad)
		bad.Canonicalize()
		if err := col.PublishVectorPartitionManifestV1(bad); err == nil {
			t.Fatal("stale source identity accepted")
		}
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
	if err := os.WriteFile(filepath.Join(s.dir, safeVPM("docs")+"-"+safeVPM("embedding")+".active"), []byte("8\ntrailing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("docs", "embedding", 8, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("corrupt active pointer did not fail closed")
	}
}

func TestCollectionVectorPartitionManifestV1PublicationSharesMutationBarrier(t *testing.T) {
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, view, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName = def.Name
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	ref := view.VectorIndexState.Assets[0].Ref
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	for i := range m.Assets {
		m.Assets[i].Ref = ref
		m.Assets[i].Bytes = uint64(ref.Length)
		m.Assets[i].Path = ""
		m.Assets[i].Checksum = digest
	}
	m.RouterAsset.Ref = ref
	m.RouterAsset.Bytes = uint64(ref.Length)
	m.RouterAsset.Path = ""
	m.RouterAsset.Checksum = digest
	m.Canonicalize()
	held := col.lockMutation()
	done := make(chan error, 1)
	go func() { done <- col.PublishVectorPartitionManifestV1(m) }()
	select {
	case err := <-done:
		t.Fatalf("publish escaped mutation barrier: %v", err)
	default:
	}
	held.Unlock()
	if err := <-done; err != nil {
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

func TestVectorPartitionManifestV1RejectsUntypedOrPathAssetAuthority(t *testing.T) {
	m := testVectorPartitionManifestV1()
	m.Assets[0].Ref = ColumnAssetRef{}
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("untyped asset reference accepted")
	}
	m = testVectorPartitionManifestV1()
	m.Assets[0].Path = "relative/path"
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("path asset authority accepted")
	}
}

func testVectorPartitionManifestV1() VectorPartitionManifestV1 {
	h := strings.Repeat("a", 64)
	b := strings.Repeat("b", 64)
	ref := func(partID uint64, fileID uint32, bytes int64) ColumnAssetRef {
		return ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "test", Generation: 4, PartID: partID, FileID: fileID, Length: bytes}
	}
	m := VectorPartitionManifestV1{State: "ready", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: h, SourceGeneration: 4, SourceChecksum: 9, SourceSchemaHash: 11, SourceRowCount: 2, Generation: 7, RouterGeneration: 7, PartitionCount: 2, BalancePolicy: "disjoint_v1", Placements: []VectorPartitionPlacementV1{{0, "raft-a"}, {1, "raft-a"}}, Memberships: []VectorPartitionMembershipV1{{0, 0}, {1, 1}}, Representatives: []VectorPartitionMembershipV1{{0, 0}}, Assets: []VectorPartitionAssetV1{{ID: "partition/0", Checksum: b, Bytes: 12, Ref: ref(1, 1, 12)}, {ID: "partition/1", Checksum: b, Bytes: 13, Ref: ref(2, 2, 13)}}, RouterAsset: VectorPartitionAssetV1{ID: "router", Checksum: b, Bytes: 14, Ref: ref(3, 3, 14)}}
	m.Canonicalize()
	return m
}
