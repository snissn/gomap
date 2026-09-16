package collections

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func TestVectorIndexPartitionLiveMutationMoveAndDeleteV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: "embedding", IndexDefinitionDigest: "definition",
		SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2,
		Generation: 7, DomainCount: 2, PartitionCount: 3,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1}, {DomainID: 1, PackID: 2}},
	}
	representatives := []vectorPartitionLiveRepresentativeV1{
		{domain: 0, vector: []float32{1, 0}},
		{domain: 1, vector: []float32{0, 1}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, representatives); err != nil {
		t.Fatal(err)
	}

	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Unlock()
	pin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !pin.ExcludesBaseIDV1("doc") || pin.DomainDeltaCountV1(0) != 1 || pin.DomainDeltaCountV1(1) != 0 {
		t.Fatalf("insert pin=%+v", pin.StatusV1())
	}
	pin.Release()

	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Unlock()
	pin, err = idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if pin.DomainDeltaCountV1(0) != 0 || pin.DomainDeltaCountV1(1) != 1 {
		t.Fatalf("move pin=%+v", pin.StatusV1())
	}
	results, _, err := pin.SearchDomainV1(context.Background(), 1, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if err != nil || len(results) != 1 || results[0].ID != "doc" {
		t.Fatalf("move search results=%v err=%v", results, err)
	}
	pin.Release()

	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), nil); err != nil {
		t.Fatal(err)
	}
	idx.mu.Unlock()
	pin, err = idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	if !pin.ExcludesBaseIDV1("doc") || pin.DomainDeltaCountV1(0) != 0 || pin.DomainDeltaCountV1(1) != 0 {
		t.Fatalf("delete pin=%+v", pin.StatusV1())
	}
}

func TestVectorIndexPartitionLiveSnapshotRecoveryAndMismatchV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2, Generation: 7, DomainCount: 2, PartitionCount: 2, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}}}
	reps := []vectorPartitionLiveRepresentativeV1{
		{domain: 1, vector: []float32{0, 1}},
		{domain: 0, vector: []float32{1, 0}},
		{domain: 0, vector: []float32{.8, .2}},
	}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, reps); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	idx.recordSourceDocumentStateLocked(9, backenddb.StateToken{})
	idx.mu.Unlock()
	snapshot, _ := idx.persistSnapshot()
	restored, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := restored.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("restore reason=%q", reason)
	}
	if got := restored.partitionLive.representatives; len(got) != 3 || got[0].domain != 0 || got[1].domain != 0 || got[2].domain != 1 {
		t.Fatalf("restored representatives=%+v", got)
	}
	pin, err := restored.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, err := pin.SearchDomainV1(t.Context(), 1, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	pin.Release()
	if err != nil || len(results) != 1 || results[0].ID != "doc" {
		t.Fatalf("results=%+v err=%v", results, err)
	}
	tampered := snapshot
	tampered.Meta.PartitionLive = idx.partitionLivePersistLocked()
	tampered.Meta.PartitionLive.Coverage++
	mismatch, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if reason := mismatch.loadPersistSnapshot(tampered); reason != "invalid_partition_live_meta" {
		t.Fatalf("mismatch reason=%q", reason)
	}
	orphan := idx.partitionLivePersistLocked()
	orphan.Owners = nil
	if _, reason := restored.restorePartitionLiveV1(orphan, orphan.Coverage); reason != "invalid_partition_live_owner_node" {
		t.Fatalf("orphan reason=%q", reason)
	}
	unordered := idx.partitionLivePersistLocked()
	unordered.Representatives[0], unordered.Representatives[1] = unordered.Representatives[1], unordered.Representatives[0]
	if _, reason := restored.restorePartitionLiveV1(unordered, unordered.Coverage); reason != "invalid_partition_live_representative" {
		t.Fatalf("unordered representatives reason=%q", reason)
	}
	nested := idx.partitionLivePersistLocked()
	nested.Domains[0].Snapshot.Meta.PartitionLive = &vectorIndexPartitionLivePersistV1{Version: 1}
	if _, reason := restored.restorePartitionLiveV1(nested, nested.Coverage); reason != "invalid_partition_live_domain" {
		t.Fatalf("nested live domain reason=%q", reason)
	}
}

func TestVectorIndexPartitionLivePinnedReaderAcrossRevisionV1(t *testing.T) {
	idx, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2, Generation: 7, DomainCount: 2, PartitionCount: 2, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}, {domain: 1, vector: []float32{0, 1}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	_ = idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0})
	idx.mu.Unlock()
	oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	_ = idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{0, 1})
	idx.mu.Unlock()
	oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	oldPin.Release()
	newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer newPin.Release()
	newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 1, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || len(newResults) != 1 || newResults[0].ID != "doc" {
		t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
	}
}

func TestVectorIndexPartitionLiveGenerationCutoverKeepsOldPinV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	oldManifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	reps := []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}
	if err := idx.bindVectorPartitionLiveV1(oldManifest, oldManifest.SourceGeneration, reps); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	idx.mu.Unlock()
	oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(oldManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer oldPin.Release()

	newManifest := oldManifest
	newManifest.Generation = 8
	newManifest.SourceGeneration = 4
	newManifest.SourceChecksum = 6
	newCoverage := uint64(41)
	if err := idx.bindVectorPartitionLiveV1(newManifest, newCoverage, reps); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("cutover without exact source err=%v", err)
	}
	if pin, err := idx.acquireVectorPartitionLiveSearchPinV1(oldManifest); err != nil {
		t.Fatalf("rejected cutover disturbed old generation: %v", err)
	} else {
		pin.Release()
	}

	idx.mu.Lock()
	idx.sourceDocumentRootsValid = true
	idx.sourceDocumentGeneration = newCoverage
	idx.mu.Unlock()
	if err := idx.bindVectorPartitionLiveV1(newManifest, newCoverage, reps); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.acquireVectorPartitionLiveSearchPinV1(oldManifest); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("retired generation err=%v", err)
	}
	newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(newManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer newPin.Release()
	if status := newPin.StatusV1(); status.Generation != newManifest.Generation || status.Revision != 0 || status.Coverage != newCoverage || status.MutatedIDs != 0 || status.LiveIDs != 0 {
		t.Fatalf("new generation status=%+v", status)
	}
	oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if oldErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || newErr != nil || len(newResults) != 0 {
		t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
	}
	persisted := idx.partitionLivePersistLocked()
	if persisted == nil || persisted.Generation != newManifest.Generation || len(persisted.Owners) != 0 || len(persisted.Domains) != 0 {
		t.Fatalf("persisted cutover=%+v", persisted)
	}
}

func TestVectorIndexPartitionLiveRepeatedUpdateCutoverKeepsPinnedViewV1(t *testing.T) {
	idx, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	idx.partitionLive.nodeCapacity = 3
	if err := idx.preflightVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		idx.mu.Unlock()
		t.Fatal(err)
	}
	idx.mu.Unlock()
	oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	oldDelta := oldPin.domains[0].index

	for _, vector := range [][]float32{{.8, .2}, {.6, .4}, {0, 1}} {
		idx.mu.Lock()
		if err := idx.preflightVectorPartitionMutationLocked([]byte("doc"), vector); err != nil {
			idx.mu.Unlock()
			oldPin.Release()
			t.Fatal(err)
		}
		if err := idx.reconcileVectorPartitionMutationLocked([]byte("doc"), vector); err != nil {
			idx.mu.Unlock()
			oldPin.Release()
			t.Fatal(err)
		}
		idx.mu.Unlock()
	}

	oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	oldPin.Release()
	newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer newPin.Release()
	newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || len(newResults) != 1 || newResults[0].ID != "doc" {
		t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
	}
	if newPin.StatusV1().Cutovers != 1 {
		t.Fatalf("cutovers=%d, want 1", newPin.StatusV1().Cutovers)
	}
	oldPinStatus := oldPin.StatusV1()
	if oldDelta == newPin.domains[0].index || oldPinStatus.Cutovers != 0 {
		t.Fatal("cutover did not retire the old domain behind its pin")
	}
}

func TestVectorIndexPartitionLiveCutoverPublishesOncePerDomainV1(t *testing.T) {
	idx, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 3, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	idx.partitionLive.nodeCapacity = 4
	idx.mu.Unlock()
	update := func(id string, vector []float32) {
		t.Helper()
		idx.mu.Lock()
		defer idx.mu.Unlock()
		if err := idx.preflightVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
		if err := idx.reconcileVectorPartitionMutationLocked([]byte(id), vector); err != nil {
			t.Fatal(err)
		}
	}
	update("a", []float32{1, 0})
	update("b", []float32{.9, .1})
	update("c", []float32{.8, .2})
	update("a", []float32{.7, .3})
	update("b", []float32{.6, .4}) // compacts three owners before this insert

	idx.mu.RLock()
	publications := idx.partitionLive.cutoverPublications
	cutovers := idx.partitionLive.cutovers
	idx.mu.RUnlock()
	if cutovers != 1 || publications != 1 {
		t.Fatalf("cutovers=%d publications=%d, want one publication for one rebuilt domain", cutovers, publications)
	}
}

func TestVectorPartitionLiveColumnGraphCarrierLoadsWithoutRebuildV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0}}, {id: "b", vector: []float32{0, 1}}}
	_, database, collection, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer database.Close()
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	rebuilds := 0
	restoreHook := setColumnVectorGraphRebuildBeforeBuildTestHook(func() { rebuilds++ })
	defer restoreHook()

	missing, missingStatus, err := collection.loadVectorPartitionLiveIndexForServingV1(def)
	if err != nil || missing != nil || missingStatus.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot || rebuilds != 0 {
		t.Fatalf("missing carrier loaded=%v status=%+v rebuilds=%d err=%v", missing != nil, missingStatus, rebuilds, err)
	}
	source, _, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: def.Name, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	carrier, err := newVectorIndex(collection, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	carrier.setPartitionLiveCarrier(true)
	generation, state, err := collection.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	carrier.recordSourceDocumentState(generation, state)
	if err := carrier.bindVectorPartitionLiveV1(manifest, generation, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	collection.registerVectorIndexCurrentCatalog(carrier)
	if status, err := carrier.SaveNativeDeltaSnapshot(); err != nil || !status.Loaded {
		t.Fatalf("persist carrier status=%+v err=%v", status, err)
	}
	collection.UnregisterVectorIndex(def.Name)
	loaded, status, err := collection.loadVectorPartitionLiveIndexForServingV1(def)
	if err != nil || loaded == nil || !status.Loaded || !loaded.isPartitionLiveCarrier() || rebuilds != 0 {
		t.Fatalf("loaded carrier=%v status=%+v rebuilds=%d err=%v", loaded != nil, status, rebuilds, err)
	}
	replayCollection, err := newCommandWALReplayCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if got := replayCollection.registeredVectorIndex(def.Name); got != loaded {
		t.Fatalf("replay manager carrier=%p want shared carrier=%p", got, loaded)
	}
	pin, err := replayCollection.AcquireVectorPartitionLiveCoordinatorSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorStatus := pin.StatusV1()
	loaded.mu.RLock()
	pinnedState := loaded.sourceDocumentState
	loaded.mu.RUnlock()
	otherManifest := manifest
	otherManifest.IndexName = "other_embedding_graph"
	otherManifest.IndexDefinitionDigest = "other-definition"
	other, err := newVectorIndex(replayCollection, VectorIndexOptions{
		Name: otherManifest.IndexName, Field: def.Field, Metric: def.Metric, Dimensions: def.Dimensions,
		M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch,
	})
	if err != nil {
		t.Fatal(err)
	}
	other.recordSourceDocumentState(generation, pinnedState)
	if err := other.bindVectorPartitionLiveV1(otherManifest, generation, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	other.mu.Lock()
	other.partitionLive.bindingDurable = true
	other.mu.Unlock()
	replayCollection.registerVectorIndexCurrentCatalog(other)
	if replayCollection.vectorPartitionLiveCoordinatorBindingPinnedV1(otherManifest) {
		t.Fatal("index A coordinator pin authorized index B binding")
	}
	if err := replayCollection.validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(otherManifest.IndexName, otherManifest.Generation, pinnedState.CommitSeq, pinnedState.SystemRootPageID); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("index A coordinator pin authorized index B authority: %v", err)
	}
	replayCollection.UnregisterVectorIndex(otherManifest.IndexName)
	replayIDs := [][]byte{[]byte("replayed")}
	replayDocuments := [][]byte{[]byte(`{"time_us":3,"kind":"vector","did":"replayed","embedding":[0.5,0.5]}`)}
	if _, err := replayCollection.insertBatchWithCommandWALIntent(replayIDs, replayDocuments, false, nil, nil, insertBatchExecutionOptions{returnResultIDs: true}); err != nil {
		t.Fatal(err)
	}
	if currentState, ok := database.StateToken(); !ok || currentState == pinnedState {
		t.Fatalf("mutation did not advance DB state: current=%+v pinned=%+v", currentState, pinnedState)
	}
	if err := replayCollection.validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(def.Name, manifest.Generation, pinnedState.CommitSeq, pinnedState.SystemRootPageID); err != nil {
		t.Fatalf("coordinator-pinned authority: %v", err)
	}
	if err := replayCollection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatalf("coordinator-pinned binding after accepted mutation: %v", err)
	}
	coord := replayCollection.collectionSchemaCoordinator()
	if coord.partitionLivePublishMu.TryLock() {
		coord.partitionLivePublishMu.Unlock()
		t.Fatal("live mutation publication lock crossed coordinator pin")
	}
	shardPin, err := replayCollection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if shardStatus := shardPin.StatusV1(); shardStatus.Revision != coordinatorStatus.Revision || shardStatus.Coverage != coordinatorStatus.Coverage {
		t.Fatalf("shard live status=%+v want coordinator status=%+v", shardStatus, coordinatorStatus)
	}
	shardPin.Release()
	pin.Release()
	if err := replayCollection.validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(def.Name, manifest.Generation, pinnedState.CommitSeq, pinnedState.SystemRootPageID); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("released coordinator pin retained authority: %v", err)
	}
	if err := replayCollection.reconcileVectorPartitionLiveReplay(replayIDs); err != nil {
		t.Fatal(err)
	}
	pin, err = loaded.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0.5, 0.5}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	pin.Release()
	if searchErr != nil || len(results) != 1 || results[0].ID != "replayed" || rebuilds != 0 {
		t.Fatalf("carrier-only replay results=%+v rebuilds=%d err=%v", results, rebuilds, searchErr)
	}

	if _, err := collection.Insert([]byte("c"), []byte(`{"time_us":3,"kind":"vector","did":"c","embedding":[0.5,0.5]}`)); err != nil {
		t.Fatal(err)
	}
	collection.UnregisterVectorIndex(def.Name)
	stale, staleStatus, err := collection.loadVectorPartitionLiveIndexForServingV1(def)
	if err != nil || stale != nil || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleDocumentRoot || rebuilds != 0 {
		t.Fatalf("stale carrier loaded=%v status=%+v rebuilds=%d err=%v", stale != nil, staleStatus, rebuilds, err)
	}
}

func TestVectorPartitionLiveMissingCarrierAfterMutationFailsWithoutRebuildV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0}}, {id: "b", vector: []float32{0, 1}}}
	_, database, collection, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer database.Close()
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	source, _, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		IndexName: def.Name, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
	}
	rebuilds := 0
	restoreHook := setColumnVectorGraphRebuildBeforeBuildTestHook(func() { rebuilds++ })
	defer restoreHook()

	if _, err := collection.Insert([]byte("d"), []byte(`{"time_us":4,"kind":"vector","did":"d","embedding":[0.5,0.5]}`)); err != nil {
		t.Fatal(err)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err == nil {
		t.Fatal("missing carrier admitted after the immutable column graph became stale")
	}
	if rebuilds != 0 {
		t.Fatalf("request-path column graph rebuilds=%d, want 0", rebuilds)
	}
}

func TestVectorPartitionHNSWExcludesMoreThanTopKBeforeAdmissionV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	ordinals, err := vectorPartitionPreparedStableIDOrdinalsV1(view)
	if err != nil {
		t.Fatal(err)
	}
	searcher := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 0, Dimensions: 3}, prepared: view, opened: 1, searchRoute: VectorPartitionSearchRouteHNSWSearchPackV1, stableIDOrdinals: ordinals}
	t.Cleanup(func() { _ = searcher.Close() })
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 3, ExcludedStableIDs: map[string]struct{}{"doc-a": {}, "doc-b": {}}}
	if _, scratchBytes, err := searcher.SearchPreflightV1(opts); err != nil || scratchBytes == 0 {
		t.Fatalf("preflight bytes=%d err=%v", scratchBytes, err)
	}
	results, metrics, err := searcher.SearchWithOptionsV1(t.Context(), []float32{1, 0, 0}, opts)
	if err != nil || len(results) != 1 || results[0].ID != "doc-c" || metrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("results=%+v metrics=%+v err=%v", results, metrics, err)
	}
}

func TestVectorPartitionSearcherExcludesStaleBeforeTopKV1(t *testing.T) {
	searcher, err := OpenVectorPartitionLocalSearcherV1(VectorPartitionSearchAssetV1{
		ManifestChecksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation:       1, PartitionID: 0, Dimensions: 2,
		IDs: []string{"stale", "fresh"}, Vectors: [][]float32{{1, 0}, {.8, .2}},
		Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1, VectorPartitionMembershipHomeV1},
	})
	if err != nil {
		t.Fatal(err)
	}
	results, _, err := searcher.SearchWithOptionsV1(context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{
		TopK: 1, EfSearch: 8, ExcludedStableIDs: map[string]struct{}{"stale": {}},
	})
	if err != nil || len(results) != 1 || results[0].ID != "fresh" {
		t.Fatalf("results=%v err=%v", results, err)
	}
}

func TestVectorIndexPartitionLiveFirstBindingConcurrentMutationV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()

	publicationReached := make(chan struct{})
	continuePublication := make(chan struct{})
	var publicationOnce sync.Once
	restoreHook := setVectorPartitionLiveBeforeBindingPublicationHookForTest(func() {
		publicationOnce.Do(func() { close(publicationReached) })
		<-continuePublication
	})
	defer restoreHook()

	ensureDone := make(chan error, 1)
	go func() {
		ensureDone <- collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest)
	}()
	select {
	case <-publicationReached:
	case <-time.After(5 * time.Second):
		t.Fatal("first binding did not reach durable publication")
	}

	replacement, err := json.Marshal(map[string]any{
		"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutationDone := make(chan error, 1)
	go func() {
		matched, err := collection.Replace([]byte("a"), replacement)
		if err == nil && !matched {
			err = errors.New("replacement did not match")
		}
		mutationDone <- err
	}()
	select {
	case err := <-mutationDone:
		if err != nil {
			close(continuePublication)
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(continuePublication)
		t.Fatal("concurrent mutation deadlocked behind first binding publication")
	}
	close(continuePublication)
	select {
	case err := <-ensureDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first binding did not finish after concurrent mutation")
	}

	wantCoverage, _, err := collection.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, err := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if err != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Coverage != wantCoverage {
		t.Fatalf("post-state results=%+v status=%+v wantCoverage=%d err=%v", results, pin.StatusV1(), wantCoverage, err)
	}
}

func TestVectorIndexPartitionLiveFirstBindingCheckpointCommandWALReplayV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	replayDir := t.TempDir()
	dir, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		database.Close()
		t.Fatalf("first binding was not durably admitted: %v", err)
	}
	pin.Release()
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	copyTypedStorageCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
	maxBaselineLSN := uint64(0)
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN > maxBaselineLSN {
			maxBaselineLSN = frame.LSN
		}
	}
	database = openCollectionCommandWALDB(t, dir)
	collection, err = NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatalf("reload checkpointed binding: %v", err)
	}

	replacement, err := json.Marshal(map[string]any{
		"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
	})
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
		database.Close()
		t.Fatalf("replacement matched=%v err=%v", matched, err)
	}
	expectedCoverage, _, err := collection.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	pin, err = collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		database.Close()
		t.Fatalf("acknowledged mutation was not immediately searchable: %v", err)
	}
	immediate, _, immediateErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	pin.Release()
	if immediateErr != nil || len(immediate) != 1 || immediate[0].ID != "a" {
		database.Close()
		t.Fatalf("immediate results=%+v err=%v", immediate, immediateErr)
	}
	replayedMutation := false
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN <= maxBaselineLSN {
			continue
		}
		writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
		if frame.Kind == commitlog.CommandKindCollectionPersistPartitionLive {
			database.Close()
			t.Fatal("post-checkpoint replay unexpectedly depended on a partition-live binding command")
		}
		switch frame.Kind {
		case commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionDeleteBatchByID,
			commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionReplaceSourceByID:
			replayedMutation = true
		}
	}
	if !replayedMutation {
		database.Close()
		t.Fatal("missing post-checkpoint document mutation frame")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedDB := openCollectionCommandWALDB(t, replayDir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	reopenedManifest, err := reopened.PreparedVectorPartitionManifestWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureVectorPartitionLiveBindingV1(t.Context(), reopenedManifest); err != nil {
		t.Fatalf("recover durable binding after document replay: %v", err)
	}
	recoveredPin, err := reopened.AcquireVectorPartitionLiveSearchPinV1(reopenedManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredPin.Release()
	recovered, _, recoveredErr := recoveredPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if recoveredErr != nil || len(recovered) != 1 || recovered[0].ID != "a" || recoveredPin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("recovered=%+v status=%+v err=%v", recovered, recoveredPin.StatusV1(), recoveredErr)
	}
}

func newVectorPartitionLiveProductionFixtureV1(t *testing.T) (string, *backenddb.DB, *Collection, VectorIndexDefinition, VectorPartitionManifestV1) {
	t.Helper()
	rows := []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, 0}},
		{id: "b", vector: []float32{.8, .2}},
		{id: "c", vector: []float32{0, 1}},
	}
	dir, database, collection, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		database.Close()
		t.Fatal(err)
	}
	source, sourceRows, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{
		State: "building", Collection: collection.name, IndexName: def.Name,
		IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum,
		SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks:   []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
		BalancePolicy: "disjoint_v1",
		Placements:    []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}},
	}
	partition := internalrouter.RouterPartitionV1{PartitionID: 0}
	for _, row := range sourceRows {
		manifest.Memberships = append(manifest.Memberships, VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		partition.Vectors = append(partition.Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(VectorPartitionMembershipHomeV1)})
	}
	manifest.Canonicalize()
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, manifest, 7801, []VectorPartitionSearchAssetV1{{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: def.Dimensions}})
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	defer resources.Release()
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		database.Close()
		t.Fatal(err)
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.RepresentativesPerPartition = 1
	cfg.MaxDepth = 4
	cfg.MaxIterations = 8
	cfg.MaxVectors = len(sourceRows)
	cfg.MaxDimensions = 8
	cfg.MaxRepresentatives = 32
	cfg.MaxScalarWork = 1_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(t.Context(), manifest, []internalrouter.RouterPartitionV1{partition}, VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 7802, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	ready, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	return dir, database, collection, def, ready
}
