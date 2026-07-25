package collections

import (
	"errors"
	"os"
	"testing"
)

func TestStageVectorPartitionManifestV1PublishesPreparedWithoutLocalActivation(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, definition := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, 0, 0}},
		{id: "b", vector: []float32{0, 1, 0}},
	})
	defer database.Close()
	if _, err := collection.RebuildVectorIndex(definition.Name); err != nil {
		t.Fatal(err)
	}
	source, err := collection.VectorPartitionSourceIdentityV1(definition.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := testVectorPartitionManifestV1()
	manifest.IndexName = definition.Name
	manifest.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(definition)
	manifest.SourceGeneration = source.Generation
	manifest.SourceChecksum = source.Checksum
	manifest.SourceSchemaHash = source.SchemaHash
	manifest.SourceRowCount = source.RowCount
	manifest, resources := vectorPartitionManifestWithFreshStableAssetsV1(t, database, collection, manifest, 9701)

	if err := collection.StageVectorPartitionManifestV1(manifest, resources); err != nil {
		t.Fatal(err)
	}
	store, err := OpenExistingVectorPartitionStoreV1(database.Dir())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.Open(manifest.Collection, manifest.IndexName, manifest.Generation)
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(prepared, manifest) {
		t.Fatalf("prepared=%+v err=%v", prepared, err)
	}
	if _, err := store.OpenActive(manifest.Collection, manifest.IndexName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged generation unexpectedly became locally active: %v", err)
	}
	opened, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), manifest.IndexName, manifest.Generation)
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(opened, manifest) {
		t.Fatalf("prepared open=%+v err=%v", opened, err)
	}
	if _, err := collection.ActiveVectorPartitionManifestWithContextV1(t.Context(), manifest.IndexName, manifest.Generation); err == nil {
		t.Fatal("prepared generation passed standalone active admission")
	}
}

func TestStageVectorPartitionManifestLifecycleV1PreservesExistingActive(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	active := testVectorPartitionManifestV1()
	if err := store.publishValidatedReady(active); err != nil {
		t.Fatal(err)
	}
	staged := cloneVectorPartitionManifestForCheckpointV1(active)
	staged.Generation++
	staged.RouterGeneration = staged.Generation
	for i := range staged.Assets {
		staged.Assets[i].Ref.Generation = staged.Generation
	}
	staged.RouterAsset.Ref.Generation = staged.Generation
	staged.Canonicalize()
	if err := store.stageVectorPartitionManifestLifecycleV1(staged); err != nil {
		t.Fatal(err)
	}
	got, err := store.OpenActive(active.Collection, active.IndexName)
	if err != nil || got.Generation != active.Generation {
		t.Fatalf("active generation=%d err=%v want %d", got.Generation, err, active.Generation)
	}
	if prepared, err := store.Open(staged.Collection, staged.IndexName, staged.Generation); err != nil || prepared.Generation != staged.Generation {
		t.Fatalf("staged generation=%d err=%v", prepared.Generation, err)
	}
}
