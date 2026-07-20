package collections

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	_ = view
	refs := []ColumnAssetRef{writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 701, []byte("partition-0")), writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 702, []byte("partition-1")), writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 703, []byte("router"))}
	for i := range m.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.Assets[i].Ref, m.Assets[i].Bytes, m.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[2])
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	m.RouterAsset.Ref, m.RouterAsset.Bytes, m.RouterAsset.Checksum = refs[2], uint64(refs[2].Length), hex.EncodeToString(sum[:])
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m); err != nil {
		t.Fatal(err)
	}
	status, err := col.VectorPartitionStatusV1(def.Name, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Ready || status.GroupCount != 1 || status.AssetBytes != uint64(refs[0].Length+refs[1].Length+refs[2].Length) {
		t.Fatalf("unexpected status: %+v", status)
	}
	plan, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Sources.PreparedRefs != 3 || plan.Sources.PinnedRefs != 3 {
		t.Fatalf("partition assets were not retained as prepared+pinned: %+v", plan.Sources)
	}
	store, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	activeGC, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{DryRun: true, CandidateRefs: refs})
	if err != nil {
		t.Fatal(err)
	}
	if activeGC.BytesEligible != 0 {
		t.Fatalf("active VPM assets became reclaimable: %+v", activeGC)
	}
	if err := store.Deactivate(m.Collection, m.IndexName); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	retiredGC, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{DryRun: true, CandidateRefs: refs})
	if err != nil {
		t.Fatal(err)
	}
	if retiredGC.BytesEligible == 0 {
		t.Fatalf("retired/deleted VPM assets stayed protected: %+v", retiredGC)
	}
	if err := store.Publish(m); err != nil {
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

func TestVectorPartitionStoreV1DeactivateDurablyRetiresActiveGeneration(t *testing.T) {
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.Publish(m); err != nil {
		t.Fatal(err)
	}
	if err := s.Deactivate(m.Collection, m.IndexName); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenActive(m.Collection, m.IndexName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active after deactivate err=%v, want not exist", err)
	}
	retired, err := s.OpenRetired(m.Collection, m.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	if retired.Generation != m.Generation {
		t.Fatalf("retired generation=%d want %d", retired.Generation, m.Generation)
	}
	if err := s.Delete(m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatalf("retired generation should be cleanup eligible: %v", err)
	}
}

func TestVectorPartitionStoreV1SameGenerationPublicationIsExactByteIdempotent(t *testing.T) {
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.Publish(m); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(m); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	changed := m
	changed.Assets[0].Checksum = strings.Repeat("c", 64)
	changed.Canonicalize()
	if err := s.Publish(changed); err == nil {
		t.Fatal("same generation with different bytes overwrote published manifest")
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
	_ = view
	refs := []ColumnAssetRef{writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 711, []byte("partition-0")), writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 712, []byte("partition-1")), writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 713, []byte("router"))}
	for i := range m.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.Assets[i].Ref, m.Assets[i].Bytes, m.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[2])
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	m.RouterAsset.Ref, m.RouterAsset.Bytes, m.RouterAsset.Checksum = refs[2], uint64(refs[2].Length), hex.EncodeToString(sum[:])
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

func TestVectorPartitionStoreV1OpenBindsDecodedIdentityAndBoundsPointers(t *testing.T) {
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName = "other"
	m.Canonicalize()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	spoofed := filepath.Join(s.dir, fmt.Sprintf("%s-%s-%d.vpm", safeVPM("docs"), safeVPM("embedding"), m.Generation))
	if err := os.WriteFile(spoofed, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("docs", "embedding", m.Generation); err == nil {
		t.Fatal("stored manifest identity mismatch accepted")
	}
	if err := os.WriteFile(filepath.Join(s.dir, safeVPM("docs")+"-"+safeVPM("embedding")+".active"), []byte(strings.Repeat("1", 33)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenActive("docs", "embedding"); err == nil {
		t.Fatal("oversized active pointer accepted")
	}
}

func TestVectorPartitionManifestJSONV1RejectsUnknownAndTrailing(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestJSONV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionManifestJSONV1(raw, DefaultVectorPartitionManifestLimits()); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionManifestJSONV1(append(raw, []byte(` {}`)...), DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	if _, err := DecodeVectorPartitionManifestJSONV1([]byte(`{"unknown":1}`), DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("unknown JSON field accepted")
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

func TestVectorPartitionManifestV1RejectsUntypedAssetAuthority(t *testing.T) {
	m := testVectorPartitionManifestV1()
	m.Assets[0].Ref = ColumnAssetRef{}
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("untyped asset reference accepted")
	}
}

func testVectorPartitionManifestV1() VectorPartitionManifestV1 {
	h := strings.Repeat("a", 64)
	b := strings.Repeat("b", 64)
	ref := func(partID uint64, fileID uint32, bytes int64) ColumnAssetRef {
		return ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "test", Generation: 4, PartID: partID, FileID: fileID, Length: bytes}
	}
	m := VectorPartitionManifestV1{State: "ready", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: h, SourceGeneration: 4, SourceChecksum: 9, SourceSchemaHash: 11, SourceRowCount: 2, Generation: 7, RouterGeneration: 7, PartitionCount: 2, BalancePolicy: "disjoint_v1", Placements: []VectorPartitionPlacementV1{{0, "raft-a"}, {1, "raft-a"}}, Memberships: []VectorPartitionMembershipV1{{0, 0}, {1, 1}}, Representatives: []VectorPartitionMembershipV1{{0, 0}}, Assets: []VectorPartitionAssetV1{{ID: "partition/0", PartitionID: 0, Checksum: b, Bytes: 12, Ref: ref(1, 1, 12)}, {ID: "partition/1", PartitionID: 1, Checksum: b, Bytes: 13, Ref: ref(2, 2, 13)}}, RouterAsset: VectorPartitionAssetV1{ID: "router", Checksum: b, Bytes: 14, Ref: ref(3, 3, 14)}}
	m.Canonicalize()
	return m
}

func BenchmarkVectorPartitionManifestV1Scale(b *testing.B) {
	for _, rows := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			m := testVectorPartitionManifestV1()
			m.SourceRowCount = uint64(rows)
			m.PartitionCount = 1
			m.Placements = []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}}
			m.Memberships = make([]VectorPartitionMembershipV1, rows)
			for i := range m.Memberships {
				m.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
			}
			m.Assets = m.Assets[:1]
			m.Canonicalize()
			raw, err := EncodeVectorPartitionManifestV1(m)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(raw))/float64(rows), "metadata-bytes/vector")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
				if err != nil || got.SourceRowCount != uint64(rows) {
					b.Fatalf("decode: %v", err)
				}
			}
		})
	}
}
