package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

type scopedStageAuthorityFuncV1 func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error

type scopedRouterAuthorityFuncV1 func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error
type scopedOwnerAuthorityFuncV1 struct {
	full func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error
	pair func(VectorPartitionLocalScopeV1) error
}

func (f scopedOwnerAuthorityFuncV1) ValidateVectorPartitionScopedOwnerV1(_ context.Context, manifest VectorPartitionManifestV1, scope VectorPartitionLocalScopeV1) error {
	return f.full(manifest, scope)
}

func (f scopedOwnerAuthorityFuncV1) ValidateVectorPartitionScopedOwnerPairV1(_ context.Context, scope VectorPartitionLocalScopeV1) error {
	if f.pair != nil {
		return f.pair(scope)
	}
	return nil
}

func (f scopedRouterAuthorityFuncV1) ValidateVectorPartitionScopedRouterV1(_ context.Context, manifest VectorPartitionManifestV1, scope VectorPartitionLocalScopeV1) error {
	return f(manifest, scope)
}

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
	config := internalrouter.DefaultRouterConfigV1()
	config.BranchFactor, config.LeafSize, config.RepresentativeBudget = 2, 1, 2
	model, err := internalrouter.BuildRouterV1([]internalrouter.RouterPartitionV1{
		{PartitionID: 0, Vectors: []internalrouter.RouterVectorV1{{Ordinal: 0, Values: []float32{1, 0, 0}, MembershipKind: string(VectorPartitionMembershipHomeV1)}}},
		{PartitionID: 1, Vectors: []internalrouter.RouterVectorV1{{Ordinal: 1, Values: []float32{0, 1, 0}, MembershipKind: string(VectorPartitionMembershipHomeV1)}}},
	}, config)
	if err != nil {
		t.Fatal(err)
	}
	modelDigest, err := internalrouter.RouterDigestV1(model)
	if err != nil {
		t.Fatal(err)
	}
	ready.Representatives = ready.Representatives[:0]
	for _, representative := range model.Representatives {
		ready.Representatives = append(ready.Representatives, VectorPartitionRepresentativeV2{
			VectorOrdinal: representative.SourceOrdinal, PartitionID: representative.PartitionID, NodeID: representative.NodeID,
		})
	}
	ready.Canonicalize()
	routerBytes, err := buildVectorPartitionRouterPackV1(ready, model, modelDigest, definition)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := database.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		database.ColumnAssetRootDir(), *collection.meta.Options.ColumnStore, 9711,
		[]StableColumnPhysicalAssetAppend{
			{Payload: []byte("partition-0"), Kind: ColumnAssetKindTCS1PartImage, Generation: ready.Generation, PartID: 1},
			{Payload: []byte("partition-1"), Kind: ColumnAssetKindTCS1PartImage, Generation: ready.Generation, PartID: 2},
			{Payload: routerBytes, Kind: ColumnAssetKindTCS1HNSWSearchPack, Generation: ready.Generation, PartID: 3},
		}, database.StableResourceIdentityPinRegistry(), lease)
	lease.Release()
	if err != nil {
		t.Fatal(err)
	}
	for i := range ready.Assets {
		payload := []byte("partition-0")
		if i == 1 {
			payload = []byte("partition-1")
		}
		sum := sha256.Sum256(payload)
		ready.Assets[i].Ref, ready.Assets[i].Bytes, ready.Assets[i].Checksum = refs[i], uint64(len(payload)), hex.EncodeToString(sum[:])
	}
	routerSum := sha256.Sum256(routerBytes)
	ready.RouterAsset.ID = "router/krt-hnsw-v3/" + modelDigest
	ready.RouterAsset.Ref, ready.RouterAsset.Bytes, ready.RouterAsset.Checksum = refs[2], uint64(len(routerBytes)), hex.EncodeToString(routerSum[:])
	ready.Canonicalize()
	raw, err := EncodeVectorPartitionManifestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := VectorPartitionPlacementDigestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	scope := VectorPartitionLocalScopeV1{HostedGroup: "raft-a", Router: true, ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(raw)), PlacementDigest: placementDigest}
	// A real owner receives the files first, then recaptures local tokens;
	// the builder's append-time tokens are not transferred between processes.
	resources.Release()
	resources, err = collection.CaptureVectorPartitionScopedExistingAssetsV1(ready, scope)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := store.persistVerifiedVectorPartitionManifestLifecycleModeV1(ready, false); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("ordinary ready staging accepted scoped generation: %v", err)
	}
	if _, err := collection.VectorPartitionStatusV1(ready.IndexName, ready.Generation); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("full-local status accepted scoped generation: %v", err)
	}
	if router, _, err := collection.OpenPreparedVectorPartitionRouterForGenerationWithContextV1(t.Context(), ready.IndexName, ready.Generation); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		if router != nil {
			router.Close()
		}
		t.Fatalf("full-local prepared router accepted scoped generation: %v", err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1(ready.Collection, ready.IndexName, vectorPartitionLifecycleLocalActivateV1, ready.Generation, nil); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("local activation accepted scoped generation: %v", err)
	}
	got, gotScope, err := collection.PreparedVectorPartitionScopedManifestWithContextV1(t.Context(), ready.IndexName, ready.Generation)
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(got, ready) || gotScope != scope {
		t.Fatalf("scoped prepared read scope=%+v err=%v", gotScope, err)
	}
	refuse := errors.New("active catalog pair changed")
	if router, _, err := collection.OpenPreparedVectorPartitionScopedRouterWithContextV1(t.Context(), ready.IndexName, ready.Generation,
		scopedRouterAuthorityFuncV1(func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error { return refuse })); !errors.Is(err, refuse) || router != nil {
		t.Fatalf("scoped router accepted failed catalog check: router=%v err=%v", router, err)
	}
	checks = 0
	if router, _, err := collection.OpenPreparedVectorPartitionScopedRouterWithContextV1(t.Context(), ready.IndexName, ready.Generation,
		scopedRouterAuthorityFuncV1(func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error {
			checks++
			if checks == 2 {
				return refuse
			}
			return nil
		})); !errors.Is(err, refuse) || router != nil || vectorPartitionReaderPinCountV1(database.Dir(), ready.Collection, ready.IndexName, ready.Generation) != 0 {
		t.Fatalf("scoped router accepted catalog movement after open: router=%v err=%v", router, err)
	}
	checks = 0
	router, _, err := collection.OpenPreparedVectorPartitionScopedRouterWithContextV1(t.Context(), ready.IndexName, ready.Generation,
		scopedRouterAuthorityFuncV1(func(manifest VectorPartitionManifestV1, gotScope VectorPartitionLocalScopeV1) error {
			checks++
			if !vectorPartitionManifestCanonicalEqualV1(manifest, ready) || gotScope != scope {
				return refuse
			}
			barrierCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := WithVectorPartitionStorageBarrierWithContextV1(barrierCtx, database.Dir(), func() error { return nil }); err != nil {
				return fmt.Errorf("catalog check held the storage barrier: %w", err)
			}
			return nil
		}))
	if err != nil || router == nil || checks != 2 {
		t.Fatalf("scoped router open checks=%d err=%v", checks, err)
	}
	if err := router.Close(); err != nil || vectorPartitionReaderPinCountV1(database.Dir(), ready.Collection, ready.IndexName, ready.Generation) != 0 {
		t.Fatalf("scoped router close left a reader pin: %v", err)
	}
	checks = 0
	if _, _, pin, err := collection.AcquireVectorPartitionScopedOwnerPinWithContextV1(t.Context(), ready.IndexName, ready.Generation, scope.HostedGroup,
		scopedOwnerAuthorityFuncV1{full: func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error {
			checks++
			return nil
		}, pair: func(VectorPartitionLocalScopeV1) error {
			checks++
			return refuse
		}}); !errors.Is(err, refuse) || pin != nil || checks != 2 || vectorPartitionReaderPinCountV1(database.Dir(), ready.Collection, ready.IndexName, ready.Generation) != 0 {
		t.Fatalf("scoped owner pin survived changed catalog: pin=%v err=%v", pin, err)
	}
	checks = 0
	got, gotScope, pin, err := collection.AcquireVectorPartitionScopedOwnerPinWithContextV1(t.Context(), ready.IndexName, ready.Generation, scope.HostedGroup,
		scopedOwnerAuthorityFuncV1{full: func(manifest VectorPartitionManifestV1, gotScope VectorPartitionLocalScopeV1) error {
			checks++
			if !vectorPartitionManifestCanonicalEqualV1(manifest, ready) || gotScope != scope {
				return refuse
			}
			return nil
		}, pair: func(gotScope VectorPartitionLocalScopeV1) error {
			checks++
			if gotScope != scope {
				return refuse
			}
			return nil
		}})
	if err != nil || pin == nil || checks != 2 || !vectorPartitionManifestCanonicalEqualV1(got, ready) || gotScope != scope {
		t.Fatalf("scoped owner pin failed checks=%d pin=%v err=%v", checks, pin, err)
	}
	// This synthetic TCS1 fixture has no search-root payload. An ordinary
	// unbound plan must be rejected before the pack opener sees those bytes.
	ordinaryPlan := &VectorPartitionGenerationOwnerSearchOpenPlanV2{
		collection: ready.Collection, indexName: ready.IndexName,
		generation: ready.Generation, owner: scope.HostedGroup,
	}
	if searcher, err := collection.OpenVectorPartitionScopedLocalSearcherForOwnerPlanWithContextV1(
		t.Context(), ready.IndexName, ready.Generation, 0, scope.HostedGroup, gotScope, ordinaryPlan, pin,
		scopedOwnerAuthorityFuncV1{full: func(VectorPartitionManifestV1, VectorPartitionLocalScopeV1) error {
			t.Fatal("unbound owner plan reached catalog authority")
			return nil
		}}); !errors.Is(err, ErrVectorPartitionSearchUnavailable) || searcher != nil {
		t.Fatalf("unbound owner plan opened scoped searcher: searcher=%v err=%v", searcher, err)
	}
	pin.Release()
	if vectorPartitionReaderPinCountV1(database.Dir(), ready.Collection, ready.IndexName, ready.Generation) != 0 {
		t.Fatal("scoped owner pin survived release")
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
