package collections

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"
)

type scopedStageAuthorityFuncV1 func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error

func (f scopedStageAuthorityFuncV1) ValidateVectorPartitionScopedStageV1(_ context.Context, manifest VectorPartitionManifestV1, scope VectorPartitionLocalScopeV1) error {
	return f(manifest, scope)
}

func TestPreparedVectorPartitionScopedManifestVerifiesHostedBytesV1(t *testing.T) {
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
	ready := testVectorPartitionManifestV1()
	ready.IndexName = definition.Name
	ready.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(definition)
	ready.SourceGeneration, ready.SourceChecksum, ready.SourceSchemaHash, ready.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	ready, resources := vectorPartitionManifestWithFreshStableAssetsV1(t, database, collection, ready, 9711)
	raw, err := EncodeVectorPartitionManifestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := VectorPartitionPlacementDigestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	scope := VectorPartitionLocalScopeV1{HostedGroup: "raft-a", Router: true, ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(raw)), PlacementDigest: placementDigest}
	checks := 0
	authority := scopedStageAuthorityFuncV1(func(manifest VectorPartitionManifestV1, gotScope VectorPartitionLocalScopeV1) error {
		checks++
		if !vectorPartitionManifestCanonicalEqualV1(manifest, ready) || gotScope != scope {
			return errors.New("wrong scoped stage authority input")
		}
		return nil
	})
	if err := collection.StageVectorPartitionScopedManifestWithContextV1(t.Context(), ready, scope, resources, authority); err != nil {
		t.Fatal(err)
	}
	if checks != 3 {
		t.Fatalf("catalog checks=%d want 3", checks)
	}
	if _, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), ready.IndexName, ready.Generation); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("full-local prepared read accepted scoped generation: %v", err)
	}
	store, err := OpenExistingVectorPartitionStoreV1(database.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenWithContext(t.Context(), ready.Collection, ready.IndexName, ready.Generation); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("full-local store open accepted scoped generation: %v", err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1(ready.Collection, ready.IndexName, vectorPartitionLifecycleLocalActivateV1, ready.Generation, nil); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("local activation accepted scoped generation: %v", err)
	}
	got, gotScope, err := collection.PreparedVectorPartitionScopedManifestWithContextV1(t.Context(), ready.IndexName, ready.Generation)
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(got, ready) || gotScope != scope {
		t.Fatalf("scoped prepared read scope=%+v err=%v", gotScope, err)
	}
	segment, err := columnAssetSegmentPath(database.ColumnAssetRootDir(), ready.Assets[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(segment); err != nil {
		t.Fatal(err)
	}
	if _, _, err := collection.PreparedVectorPartitionScopedManifestWithContextV1(t.Context(), ready.IndexName, ready.Generation); err == nil {
		t.Fatal("scoped prepared read accepted missing hosted bytes")
	}
}

func TestStageVectorPartitionScopedManifestRejectsChangedCatalogBeforePublicationV1(t *testing.T) {
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
	ready := testVectorPartitionManifestV1()
	ready.IndexName, ready.IndexDefinitionDigest = definition.Name, VectorIndexDefinitionDigestV1(definition)
	ready.SourceGeneration, ready.SourceChecksum, ready.SourceSchemaHash, ready.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	ready, resources := vectorPartitionManifestWithFreshStableAssetsV1(t, database, collection, ready, 9712)
	raw, err := EncodeVectorPartitionManifestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := VectorPartitionPlacementDigestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	scope := VectorPartitionLocalScopeV1{HostedGroup: "raft-a", Router: true, ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(raw)), PlacementDigest: placementDigest}
	checks := 0
	stale := errors.New("catalog moved")
	authority := scopedStageAuthorityFuncV1(func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error {
		checks++
		if checks == 2 {
			return stale
		}
		return nil
	})
	if err := collection.StageVectorPartitionScopedManifestWithContextV1(t.Context(), ready, scope, resources, authority); !errors.Is(err, stale) || checks != 2 {
		t.Fatalf("changed catalog stage err=%v checks=%d", err, checks)
	}
	store, err := OpenExistingVectorPartitionStoreV1(database.Dir())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err == nil {
		if _, err := store.Open(ready.Collection, ready.IndexName, ready.Generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("precommit catalog movement persisted scoped generation: %v", err)
		}
	}
}

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
