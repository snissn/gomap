package collections

import (
	"errors"
	"os"
	"testing"
)

func TestVectorPartitionPersistentLocalSearcherReopenCorruptionAndPinsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	makeGeneration := func(g uint64, file uint32) VectorPartitionManifestV1 {
		m := testVectorPartitionManifestV1()
		m.State = "building"
		m.RouterGeneration = 0
		m.RouterAsset = VectorPartitionAssetV1{}
		m.ReadySetDigest = ""
		m.Generation = g
		m.IndexName = def.Name
		m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
		_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
		if err != nil {
			t.Fatal(err)
		}
		m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
		in := []VectorPartitionSearchAssetV1{{ManifestChecksum: m.IntegrityDigest, Generation: g, PartitionID: 0, Dimensions: 3, IDs: []string{"a"}, Vectors: [][]float32{{1, 0, 0}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1}, Adjacency: [][]uint32{{}}}, {ManifestChecksum: m.IntegrityDigest, Generation: g, PartitionID: 1, Dimensions: 3, IDs: []string{"b"}, Vectors: [][]float32{{0, 1, 0}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1}, Adjacency: [][]uint32{{}}}}
		assets, res, err := col.MaterializeVectorPartitionLocalSearchAssetsV1(g, file, in)
		if err != nil {
			t.Fatal(err)
		}
		res.Release()
		m.Assets = assets
		m.Canonicalize()
		if err := col.PublishVectorPartitionManifestV1(m, nil); err != nil {
			t.Fatal(err)
		}
		return m
	}
	m1 := makeGeneration(41, 941)
	s, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Search([]float32{1, 0, 0}, 1); err != nil || len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("search=%+v err=%v", got, err)
	}
	if err := col.DeleteVectorPartitionGenerationV1(def.Name, m1.Generation, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("delete succeeded while local searcher pinned")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Close rejects new work but defers the M1 pin release until an explicit
	// local lease drains, so deletion cannot race an in-flight caller.
	lease, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Acquire(); err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	if err := col.DeleteVectorPartitionGenerationV1(def.Name, m1.Generation, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("delete succeeded while closing local lease pinned")
	}
	lease.Release()
	// Two complete immutable generations may be read concurrently; each opener
	// is bound to the requested manifest generation rather than an active alias.
	m2 := makeGeneration(42, 942)
	old, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m2.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := old.Search([]float32{1, 0, 0}, 1); err != nil || got[0].ID != "a" {
		t.Fatalf("old=%+v err=%v", got, err)
	}
	if got, err := newer.Search([]float32{0, 1, 0}, 1); err != nil || got[0].ID != "b" {
		t.Fatalf("new=%+v err=%v", got, err)
	}
	_ = old.Close()
	_ = newer.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openCollectionCommandWALDB(t, dir)
	var openErr error
	col, openErr = NewCollectionManager(d).OpenCollection("docs")
	if openErr != nil {
		t.Fatal(openErr)
	}
	s, err = col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	store, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := store.Open("docs", def.Name, m1.Generation)
	if err != nil {
		t.Fatal(err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), m.Assets[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, m.Assets[0].Ref.Offset); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("corrupt open err=%v", err)
	}
	_ = d.Close()
}
