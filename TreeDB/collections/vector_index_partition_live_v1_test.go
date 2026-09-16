package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
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

func TestVectorIndexPartitionLiveDomainSearchBoundsV1(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	err = idx.reconcileVectorPartitionMutationLocked([]byte("oversized"), []float32{1, 0})
	idx.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	pin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	tooSmall := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 8}
	if _, _, err := pin.DomainSearchPreflightV1(0, tooSmall); !errors.Is(err, ErrVectorIndexPartitionLiveUnavailableV1) {
		t.Fatalf("preflight err=%v want unavailable", err)
	}
	if _, _, err := pin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, tooSmall); !errors.Is(err, ErrVectorIndexPartitionLiveUnavailableV1) {
		t.Fatalf("search err=%v want unavailable", err)
	}
	if nodes, scratch, err := pin.DomainSearchPreflightV1(0, VectorPartitionSearchOptionsV1{TopK: 256, EfSearch: 256, MaxStableIDBytes: 4096}); err != nil || nodes != 1 || scratch != 64+4096 {
		t.Fatalf("nodes=%d scratch=%d err=%v", nodes, scratch, err)
	}

	view := &vectorIndexSearchView{nodes: make([]vectorIndexNode, 1), liveDocs: 1}
	many := &VectorIndexPartitionLiveSearchPinV1{domains: make(map[uint32]vectorIndexPartitionLiveDomainPinV1, 19)}
	total := uint64(64_000_000)
	for domain := uint32(0); domain < 19; domain++ {
		many.domains[domain] = vectorIndexPartitionLiveDomainPinV1{view: view, maxStableIDBytes: 1}
		_, scratch, err := many.DomainSearchPreflightV1(domain, VectorPartitionSearchOptionsV1{TopK: 256, EfSearch: 256, MaxStableIDBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		total += scratch
	}
	if total != 64_079_040 || total > 80<<20 {
		t.Fatalf("multi-domain bytes=%d", total)
	}
}

func TestVectorIndexPartitionLiveReplayRejectsDifferentInstalledCarrierV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	current := collection.registeredVectorIndex(def.Name)
	rootID, err := collection.currentNativeVectorIndexRootID(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, beforeSeq := current.persistSnapshot()
	candidate, err := newVectorIndex(collection, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if reason := candidate.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("candidate restore reason=%q", reason)
	}
	candidate.recordPersistentDefinition(def)
	candidate.recordLoadedSnapshot(rootID, current.Stats().BytesDisk)
	if err := current.InsertDocument([]byte("a")); err != nil {
		t.Fatal(err)
	}
	attempt := &vectorPartitionLiveReplayAttemptV1{entries: []vectorPartitionLiveReplayEntryV1{{
		spec:      vectorPartitionLiveReplaySpecV1{before: current, definition: def, beforeSeq: beforeSeq, rootName: collectionVectorIndexRootName(collection.name, def.Name)},
		candidate: candidate, snapshotSeq: candidate.nativeMutationSequence(),
	}}}
	err = collection.installVectorPartitionLiveReplayAttemptV1(attempt, []string{attempt.entries[0].spec.rootName}, []uint64{rootID})
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("install err=%v want concurrent mutation", err)
	}
	if !current.needsNativeAutoPersist() {
		t.Fatal("concurrent carrier mutation was incorrectly marked persisted")
	}
}

func TestVectorIndexPartitionLiveReplayPreservesInstalledCarrierMutationV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	current := collection.registeredVectorIndex(def.Name)
	rootID, err := collection.currentNativeVectorIndexRootID(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, beforeSeq := current.persistSnapshot()
	candidate, err := newVectorIndex(collection, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if reason := candidate.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("candidate restore reason=%q", reason)
	}
	candidate.recordPersistentDefinition(def)
	candidate.recordLoadedSnapshot(rootID, current.Stats().BytesDisk)
	snapshotSeq := candidate.nativeMutationSequence()
	candidate.mu.Lock()
	candidate.partitionLive.coverage = 0
	candidate.mu.Unlock()
	attempt := &vectorPartitionLiveReplayAttemptV1{entries: []vectorPartitionLiveReplayEntryV1{{
		spec:      vectorPartitionLiveReplaySpecV1{before: current, definition: def, beforeSeq: beforeSeq, rootName: collectionVectorIndexRootName(collection.name, def.Name)},
		candidate: candidate, snapshotSeq: snapshotSeq,
	}}}
	if err := collection.installVectorPartitionLiveReplayAttemptV1(attempt, []string{attempt.entries[0].spec.rootName}, []uint64{rootID}); err != nil {
		t.Fatal(err)
	}
	if collection.registeredVectorIndex(def.Name) != candidate || !candidate.needsNativeAutoPersist() {
		t.Fatal("installed carrier mutation was incorrectly marked persisted")
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
	if restored.partitionLive.byteCapacity != vectorIndexPartitionLiveMaxBytesV1 || restored.partitionLive.nodeIDBytes != uint64(len("doc")) || restored.partitionLive.ownerIDBytes != uint64(len("doc")) {
		t.Fatalf("restored byte accounting capacity=%d node_ids=%d owner_ids=%d", restored.partitionLive.byteCapacity, restored.partitionLive.nodeIDBytes, restored.partitionLive.ownerIDBytes)
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

func TestVectorIndexPartitionLiveByteCapacityRejectsBeforePublicationV1(t *testing.T) {
	const dimensions = 4096
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: dimensions, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, dimensions)
	vector[0] = 1
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: vector}}); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	idx.partitionLive.byteCapacity = 150 << 10
	revision := idx.partitionLive.revision
	err = idx.preflightVectorPartitionMutationLocked([]byte("high-dimensional"), vector)
	owners, nodes, afterRevision := len(idx.partitionLive.owners), idx.partitionLiveNodeCountLocked(), idx.partitionLive.revision
	idx.mu.Unlock()
	if !errors.Is(err, ErrVectorIndexPartitionLiveCapacityV1) {
		t.Fatalf("preflight err=%v, want capacity", err)
	}
	if owners != 0 || nodes != 0 || afterRevision != revision {
		t.Fatalf("rejected mutation published owners=%d nodes=%d revision=%d want=%d", owners, nodes, afterRevision, revision)
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

func TestVectorIndexPartitionLiveGenerationRebindWaitsForCoordinatorPinV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, def, oldManifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), oldManifest); err != nil {
		t.Fatal(err)
	}
	oldPin, err := collection.AcquireVectorPartitionLiveCoordinatorSearchPinV1(oldManifest)
	if err != nil {
		t.Fatal(err)
	}

	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	newManifest := VectorPartitionManifestV1{
		State: "building", Collection: collection.name, IndexName: def.Name,
		IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: oldManifest.Generation + 1, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}, BalancePolicy: "disjoint_v1",
		Placements: []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}},
	}
	partition := internalrouter.RouterPartitionV1{PartitionID: 0}
	for _, row := range rows {
		newManifest.Memberships = append(newManifest.Memberships, VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		partition.Vectors = append(partition.Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(VectorPartitionMembershipHomeV1)})
	}
	newManifest.Canonicalize()
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, newManifest, 7811, []VectorPartitionSearchAssetV1{{Source: source, Generation: newManifest.Generation, PartitionID: 0, Dimensions: def.Dimensions}})
	if err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	resources.Release()
	newManifest.Assets = assets
	newManifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(newManifest, nil); err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor, cfg.LeafSize, cfg.RepresentativesPerPartition = 2, 1, 1
	cfg.MaxDepth, cfg.MaxIterations, cfg.MaxVectors = 4, 8, len(rows)
	cfg.MaxDimensions, cfg.MaxRepresentatives, cfg.MaxScalarWork = 8, 32, 1_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(t.Context(), newManifest, []internalrouter.RouterPartitionV1{partition}, VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 7812, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		oldPin.Release()
		t.Fatal(err)
	}
	ready, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), def.Name, newManifest.Generation)
	if err != nil {
		oldPin.Release()
		t.Fatal(err)
	}

	transitionReached := make(chan struct{})
	continueTransition := make(chan struct{})
	var transitionOnce sync.Once
	restoreHook := setVectorPartitionLiveBeforeBindingTransitionHookForTest(func() {
		transitionOnce.Do(func() { close(transitionReached) })
		<-continueTransition
	})
	defer restoreHook()
	ensureDone := make(chan error, 1)
	go func() { ensureDone <- collection.EnsureVectorPartitionLiveBindingV1(t.Context(), ready) }()
	select {
	case <-transitionReached:
	case <-time.After(5 * time.Second):
		oldPin.Release()
		t.Fatal("new generation did not reach binding transition")
	}
	close(continueTransition)
	coord := collection.collectionSchemaCoordinator()
	if coord == nil {
		oldPin.Release()
		t.Fatal("missing collection schema coordinator")
	}
	deadline := time.Now().Add(5 * time.Second)
	for coord.partitionLivePublishMu.TryRLock() {
		coord.partitionLivePublishMu.RUnlock()
		if time.Now().After(deadline) {
			oldPin.Release()
			t.Fatal("generation rebind did not queue behind old coordinator pin")
		}
		runtime.Gosched()
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), oldManifest); err != nil {
		oldPin.Release()
		t.Fatalf("cold shard old binding: %v", err)
	}
	shardPin, err := collection.AcquireVectorPartitionLiveSearchPinV1(oldManifest)
	if err != nil {
		oldPin.Release()
		t.Fatalf("cold shard old pin: %v", err)
	}
	if got, want := shardPin.StatusV1(), oldPin.StatusV1(); got.Generation != want.Generation || got.Revision != want.Revision || got.Coverage != want.Coverage {
		shardPin.Release()
		oldPin.Release()
		t.Fatalf("cold shard status=%+v want coordinator=%+v", got, want)
	}
	shardPin.Release()
	oldPin.Release()
	select {
	case err := <-ensureDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("generation rebind remained blocked after old coordinator release")
	}
	newPin, err := collection.AcquireVectorPartitionLiveSearchPinV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	newPin.Release()
	if _, err := collection.AcquireVectorPartitionLiveSearchPinV1(oldManifest); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("retired generation remained searchable: %v", err)
	}
}

func TestVectorIndexPartitionLiveRepeatedUpdateCutoverKeepsPinnedViewV1(t *testing.T) {
	idx, _ := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	idx.sourceDocumentGeneration = manifest.SourceGeneration
	idx.sourceDocumentRootsValid = true
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
	oldPinStatus := oldPin.StatusV1()
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
	if oldDelta == newPin.domains[0].index || oldPinStatus.Cutovers != 0 {
		t.Fatal("cutover did not retire the old domain behind its pin")
	}
	idx.mu.RLock()
	wantNodeIDBytes := uint64(idx.partitionLiveNodeCountLocked() * len("doc"))
	gotNodeIDBytes := idx.partitionLive.nodeIDBytes
	idx.mu.RUnlock()
	if gotNodeIDBytes != wantNodeIDBytes {
		t.Fatalf("retained node ID bytes=%d want=%d", gotNodeIDBytes, wantNodeIDBytes)
	}
	snapshot, _ := idx.persistSnapshot()
	restored, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reason := restored.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("restore reason=%q", reason)
	}
	if restored.partitionLive.nodeIDBytes != wantNodeIDBytes || restored.partitionLive.ownerIDBytes != uint64(len("doc")) {
		t.Fatalf("restored retained bytes node_ids=%d owner_ids=%d", restored.partitionLive.nodeIDBytes, restored.partitionLive.ownerIDBytes)
	}
}

func TestVectorIndexPartitionLiveByteCapacityCompactsBeforeRejectingV1(t *testing.T) {
	newIndex := func(t *testing.T) (*VectorIndex, VectorPartitionManifestV1) {
		t.Helper()
		idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
		if err != nil {
			t.Fatal(err)
		}
		manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 2, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
		if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
			t.Fatal(err)
		}
		idx.mu.Lock()
		idx.partitionLive.nodeCapacity = 1024
		idx.mu.Unlock()
		return idx, manifest
	}
	apply := func(t *testing.T, idx *VectorIndex, id string, vector []float32) {
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

	t.Run("single replacement and held pin", func(t *testing.T) {
		idx, manifest := newIndex(t)
		apply(t, idx, "doc", []float32{1, 0})
		oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		defer oldPin.Release()

		idx.mu.Lock()
		idx.partitionLive.byteCapacity = 50 << 10
		idx.mu.Unlock()
		apply(t, idx, "doc", []float32{.5, .5})
		apply(t, idx, "doc", []float32{0, 1})

		oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin.Release()
		if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "doc" || len(newResults) != 1 || newResults[0].ID != "doc" {
			t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
		}
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		if idx.partitionLive.cutovers != 1 || idx.partitionLiveNodeCountLocked() != 2 || idx.partitionLive.nodeIDBytes != 2*uint64(len("doc")) || idx.partitionLive.ownerIDBytes != uint64(len("doc")) {
			t.Fatalf("cutovers=%d nodes=%d node_id_bytes=%d owner_id_bytes=%d", idx.partitionLive.cutovers, idx.partitionLiveNodeCountLocked(), idx.partitionLive.nodeIDBytes, idx.partitionLive.ownerIDBytes)
		}
	})

	t.Run("batch replacement and unreclaimable cap", func(t *testing.T) {
		idx, manifest := newIndex(t)
		applyBatch := func(vectors [][]float32) error {
			ids := [][]byte{[]byte("a"), []byte("b")}
			idx.mu.Lock()
			defer idx.mu.Unlock()
			if err := idx.preflightVectorPartitionMutationBatchLocked(ids, vectors); err != nil {
				return err
			}
			for i := range ids {
				if err := idx.reconcileVectorPartitionMutationLocked(ids[i], vectors[i]); err != nil {
					return err
				}
			}
			return nil
		}
		if err := applyBatch([][]float32{{1, 0}, {.8, .2}}); err != nil {
			t.Fatal(err)
		}
		idx.mu.Lock()
		idx.partitionLive.byteCapacity = 90 << 10
		idx.mu.Unlock()
		if err := applyBatch([][]float32{{.6, .4}, {.4, .6}}); err != nil {
			t.Fatal(err)
		}
		oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		defer oldPin.Release()
		if err := applyBatch([][]float32{{0, 1}, {1, 0}}); err != nil {
			t.Fatal(err)
		}
		oldResults, _, oldErr := oldPin.SearchDomainV1(t.Context(), 0, []float32{.4, .6}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		newResults, _, newErr := newPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		newPin.Release()
		if oldErr != nil || newErr != nil || len(oldResults) != 1 || oldResults[0].ID != "b" || len(newResults) != 1 || newResults[0].ID != "a" {
			t.Fatalf("old=%+v/%v new=%+v/%v", oldResults, oldErr, newResults, newErr)
		}

		idx.mu.Lock()
		idx.partitionLive.byteCapacity = 60 << 10
		beforeRevision := idx.partitionLive.revision
		err = idx.preflightVectorPartitionMutationLocked([]byte("c"), []float32{1, 0})
		owners := len(idx.partitionLive.owners)
		nodes := idx.partitionLiveNodeCountLocked()
		nodeIDBytes := idx.partitionLive.nodeIDBytes
		ownerIDBytes := idx.partitionLive.ownerIDBytes
		cutovers := idx.partitionLive.cutovers
		afterRevision := idx.partitionLive.revision
		idx.mu.Unlock()
		if !errors.Is(err, ErrVectorIndexPartitionLiveCapacityV1) {
			t.Fatalf("unreclaimable preflight err=%v, want capacity", err)
		}
		if owners != 2 || nodes != 2 || nodeIDBytes != 2 || ownerIDBytes != 2 || cutovers != 2 || afterRevision != beforeRevision {
			t.Fatalf("rejected state owners=%d nodes=%d node_id_bytes=%d owner_id_bytes=%d cutovers=%d revision=%d want=%d", owners, nodes, nodeIDBytes, ownerIDBytes, cutovers, afterRevision, beforeRevision)
		}
	})
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
	insertDone := make(chan error, 1)
	go func() {
		_, err := replayCollection.insertBatchWithCommandWALIntent(replayIDs, replayDocuments, false, nil, nil, insertBatchExecutionOptions{returnResultIDs: true})
		insertDone <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if currentState, ok := database.StateToken(); ok && currentState != pinnedState {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mutation did not publish documents while coordinator pin was held")
		}
		runtime.Gosched()
	}
	select {
	case err := <-insertDone:
		t.Fatalf("mutation crossed coordinator pin: %v", err)
	default:
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
	select {
	case err := <-insertDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mutation did not complete after coordinator pin release")
	}
	if err := replayCollection.validateVectorPartitionLiveCoordinatorPinnedAuthorityV1(def.Name, manifest.Generation, pinnedState.CommitSeq, pinnedState.SystemRootPageID); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("released coordinator pin retained authority: %v", err)
	}
	if err := replayCollection.reconcileVectorPartitionLiveReplay(replayIDs); err != nil {
		t.Fatal(err)
	}
	current := replayCollection.registeredVectorIndex(def.Name)
	if current == nil || current == loaded {
		t.Fatalf("atomic carrier install current=%p old=%p", current, loaded)
	}
	pin, err = current.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0.5, 0.5}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	pin.Release()
	if searchErr != nil || len(results) != 1 || results[0].ID != "replayed" || rebuilds != 0 {
		t.Fatalf("carrier-only replay results=%+v rebuilds=%d err=%v", results, rebuilds, searchErr)
	}

	if _, err := collection.Insert([]byte("c"), []byte(`{"time_us":3,"kind":"vector","did":"c","embedding":[0.3,0.7]}`)); err != nil {
		t.Fatal(err)
	}
	collection.UnregisterVectorIndex(def.Name)
	current, currentStatus, err := collection.loadVectorPartitionLiveIndexForServingV1(def)
	if err != nil || current == nil || !currentStatus.Loaded || rebuilds != 0 {
		t.Fatalf("current carrier loaded=%v status=%+v rebuilds=%d err=%v", current != nil, currentStatus, rebuilds, err)
	}
	pin, err = current.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, searchErr = pin.SearchDomainV1(t.Context(), 0, []float32{0.3, 0.7}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
	pin.Release()
	if searchErr != nil || len(results) != 1 || results[0].ID != "c" || rebuilds != 0 {
		t.Fatalf("foreground carrier results=%+v rebuilds=%d err=%v", results, rebuilds, searchErr)
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
	results, metrics, err := searcher.SearchWithOptionsV1(context.Background(), []float32{1, 0}, VectorPartitionSearchOptionsV1{
		TopK: 1, EfSearch: 8, ExcludedStableIDs: map[string]struct{}{"stale": {}},
	})
	if err != nil || len(results) != 1 || results[0].ID != "fresh" || metrics.Candidates != 1 {
		t.Fatalf("results=%v metrics=%+v err=%v", results, metrics, err)
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
	mutationStarted := make(chan struct{})
	go func() {
		close(mutationStarted)
		matched, err := collection.Replace([]byte("a"), replacement)
		if err == nil && !matched {
			err = errors.New("replacement did not match")
		}
		mutationDone <- err
	}()
	<-mutationStarted
	select {
	case err := <-mutationDone:
		close(continuePublication)
		t.Fatalf("mutation acknowledged before first binding durability: %v", err)
	default:
	}
	if current, err := collection.Get([]byte("a")); err != nil || bytes.Contains(current, []byte(`"time_us":99`)) {
		close(continuePublication)
		t.Fatalf("mutation visible before first binding durability: document=%s err=%v", current, err)
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
	select {
	case err := <-mutationDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent mutation did not finish after durable first binding")
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
	if dir := os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_CRASH_DIR"); dir != "" {
		if os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_REPLAY_PUBLICATION_CRASH") == "1" {
			vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
			vectorPartitionLiveReplayAfterAcceptedHookV1.fn = func() {
				// The document, column, locator, and live carrier roots have been
				// accepted atomically. Crash before either in-memory candidate is
				// installed or the command handler can reconcile/finalize it.
				os.Exit(0)
			}
			vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
			database := openCollectionCommandWALDB(t, dir)
			database.Close()
			t.Fatal("replay completed without reaching the accepted-publication crash hook")
		}
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_GENERATION"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		database := openCollectionCommandWALDB(t, dir)
		collection, err := NewCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatalf("reload checkpointed binding: %v", err)
		}
		maxBaselineLSN := uint64(0)
		for _, frame := range collectionCommandWALFrames(t, dir) {
			if frame.LSN > maxBaselineLSN {
				maxBaselineLSN = frame.LSN
			}
		}
		replacement, err := json.Marshal(map[string]any{
			"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
			t.Fatalf("replacement matched=%v err=%v", matched, err)
		}
		replayedMutation := false
		for _, frame := range collectionCommandWALFrames(t, dir) {
			if frame.LSN <= maxBaselineLSN {
				continue
			}
			if frame.Kind == commitlog.CommandKindCollectionPersistPartitionLive {
				t.Fatal("post-checkpoint replay unexpectedly depended on a partition-live binding command")
			}
			switch frame.Kind {
			case commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionDeleteBatchByID,
				commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionReplaceSourceByID:
				replayedMutation = true
			}
		}
		if !replayedMutation {
			t.Fatal("missing post-checkpoint document mutation frame")
		}
		os.Exit(0) // Process loss: preserve the checkpoint and replay only the acknowledged document WAL frame.
	}
	requireVectorPartitionPersistenceV1(t)
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
	maxBaselineLSN := uint64(0)
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN > maxBaselineLSN {
			maxBaselineLSN = frame.LSN
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	crashRaceOptions := "GORACE=" + strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveFirstBindingCheckpointCommandWALReplayV1$")
	cmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_GENERATION="+strconv.FormatUint(manifest.Generation, 10),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	replayedMutation := false
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN <= maxBaselineLSN {
			continue
		}
		if frame.Kind == commitlog.CommandKindCollectionPersistPartitionLive {
			t.Fatal("post-checkpoint replay unexpectedly depended on a partition-live binding command")
		}
		switch frame.Kind {
		case commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandKindCollectionDeleteBatchByID,
			commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionReplaceSourceByID:
			replayedMutation = true
		}
	}
	if !replayedMutation {
		t.Fatal("missing post-checkpoint document mutation frame")
	}
	replayCrashCmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveFirstBindingCheckpointCommandWALReplayV1$")
	replayCrashCmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_REPLAY_PUBLICATION_CRASH=1",
	)
	if output, err := replayCrashCmd.CombinedOutput(); err != nil {
		t.Fatalf("replay publication crash helper: %v\n%s", err, output)
	}

	reopenedDB := openCollectionCommandWALDB(t, dir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	recoveredDocument, err := reopened.Get([]byte("a"))
	if err != nil || !bytes.Contains(recoveredDocument, []byte(`"time_us":99`)) {
		t.Fatalf("recovered document=%s err=%v", recoveredDocument, err)
	}
	reopenedManifest, err := reopened.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureVectorPartitionLiveBindingV1(t.Context(), reopenedManifest); err != nil {
		t.Fatalf("recover durable binding after document replay: %v", err)
	}
	validatedManifest, authorityToken, err := reopened.ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	authorityToken.Release()
	if validatedManifest.IntegrityDigest != reopenedManifest.IntegrityDigest {
		t.Fatal("live recovery authority manifest changed")
	}
	expectedCoverage, _, err := reopened.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	recoveredPin, err := reopened.AcquireVectorPartitionLiveSearchPinV1(reopenedManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredPin.Release()
	recovered, _, recoveredErr := recoveredPin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if recoveredErr != nil || len(recovered) != 1 || recovered[0].ID != "a" || recoveredPin.StatusV1().Revision != 1 || recoveredPin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("recovered=%+v status=%+v err=%v", recovered, recoveredPin.StatusV1(), recoveredErr)
	}
}

func TestVectorIndexPartitionLiveForegroundDurablePublicationCrashV1(t *testing.T) {
	if dir := os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_FOREGROUND_CRASH_DIR"); dir != "" {
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_GENERATION"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		database := openVectorPartitionLiveDurableDBV1(t, dir)
		collection, err := NewCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := collection.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatal(err)
		}
		vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
		vectorPartitionLiveReplayAfterAcceptedHookV1.fn = func() {
			// The durable foreground command and the document, column, locator,
			// and live-carrier roots are accepted together. Crash before the
			// in-memory candidate install or ordinary reconciliation.
			os.Exit(0)
		}
		vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
		replacement, err := json.Marshal(map[string]any{
			"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
			t.Fatalf("replacement matched=%v err=%v", matched, err)
		}
		database.Close()
		t.Fatal("foreground replacement completed without reaching the accepted-publication crash hook")
	}

	requireVectorPartitionPersistenceV1(t)
	dir, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{
		CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable,
	})
	if database.ResolvedProfile() != backenddb.ProfileCommandWALDurable {
		database.Close()
		t.Fatalf("profile=%s want %s", database.ResolvedProfile(), backenddb.ProfileCommandWALDurable)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	crashRaceOptions := "GORACE=" + strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveForegroundDurablePublicationCrashV1$")
	cmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_FOREGROUND_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_GENERATION="+strconv.FormatUint(manifest.Generation, 10),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("foreground publication crash helper: %v\n%s", err, output)
	}

	reopenedDB := openVectorPartitionLiveDurableDBV1(t, dir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	recoveredDocument, err := reopened.Get([]byte("a"))
	if err != nil || !bytes.Contains(recoveredDocument, []byte(`"time_us":99`)) {
		t.Fatalf("recovered document=%s err=%v", recoveredDocument, err)
	}
	reopenedManifest, err := reopened.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureVectorPartitionLiveBindingV1(t.Context(), reopenedManifest); err != nil {
		t.Fatal(err)
	}
	expectedCoverage, _, err := reopened.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := reopened.AcquireVectorPartitionLiveSearchPinV1(reopenedManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != 1 || pin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("recovered=%+v status=%+v coverage=%d err=%v", results, pin.StatusV1(), expectedCoverage, searchErr)
	}
}

func TestVectorIndexPartitionLiveForegroundRelaxedCheckpointCrashV1(t *testing.T) {
	if dir := os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_RELAXED_CHECKPOINT_CRASH_DIR"); dir != "" {
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_VECTOR_PARTITION_LIVE_GENERATION"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		database := openCollectionCommandWALDB(t, dir)
		collection, err := NewCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := collection.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatal(err)
		}
		replacement, err := json.Marshal(map[string]any{
			"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
			t.Fatalf("replacement matched=%v err=%v", matched, err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Process loss after a successful checkpoint, before Close can persist anything else.
	}

	requireVectorPartitionPersistenceV1(t)
	dir, database, collection, def, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	crashRaceOptions := "GORACE=" + strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVectorIndexPartitionLiveForegroundRelaxedCheckpointCrashV1$")
	cmd.Env = append(os.Environ(),
		crashRaceOptions,
		"GOMAP_VECTOR_PARTITION_LIVE_RELAXED_CHECKPOINT_CRASH_DIR="+dir,
		"GOMAP_VECTOR_PARTITION_LIVE_GENERATION="+strconv.FormatUint(manifest.Generation, 10),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("foreground relaxed checkpoint crash helper: %v\n%s", err, output)
	}

	reopenedDB := openCollectionCommandWALDB(t, dir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	recoveredDocument, err := reopened.Get([]byte("a"))
	if err != nil || !bytes.Contains(recoveredDocument, []byte(`"time_us":99`)) {
		t.Fatalf("recovered document=%s err=%v", recoveredDocument, err)
	}
	reopenedManifest, err := reopened.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnsureVectorPartitionLiveBindingV1(t.Context(), reopenedManifest); err != nil {
		t.Fatal(err)
	}
	expectedCoverage, _, err := reopened.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := reopened.AcquireVectorPartitionLiveSearchPinV1(reopenedManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != 1 || pin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("recovered=%+v status=%+v coverage=%d err=%v", results, pin.StatusV1(), expectedCoverage, searchErr)
	}
}

func TestVectorIndexPartitionLiveForegroundDurableInstallsOnceV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{
		CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable,
	})
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	replacement, err := json.Marshal(map[string]any{
		"time_us": int64(99), "kind": "vector", "did": "a", "embedding": []float32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
		t.Fatalf("replacement matched=%v err=%v", matched, err)
	}
	expectedCoverage, _, err := collection.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
	if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != 1 || pin.StatusV1().Coverage != expectedCoverage {
		t.Fatalf("results=%+v status=%+v coverage=%d err=%v", results, pin.StatusV1(), expectedCoverage, searchErr)
	}
}

func TestVectorIndexPartitionLiveAlternatesCollectionManagerDomainsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name         string
		withOrdinary bool
	}{
		{name: "partition_live_only"},
		{name: "partition_live_and_native", withOrdinary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ordinaryDef := VectorIndexDefinition{
				Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine,
				Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8, Strategy: VectorIndexStrategyNativeRuntime,
			}
			var extra []VectorIndexDefinition
			if tc.withOrdinary {
				extra = []VectorIndexDefinition{ordinaryDef}
			}
			_, database, collectionA, _, manifest := newVectorPartitionLiveProductionFixtureWithIndexesV1(t, extra)
			defer database.Close()
			var ordinaryA *VectorIndex
			if tc.withOrdinary {
				ordinaryDef, _ = findVectorIndex(collectionA.Meta().VectorIndexes, ordinaryDef.Name)
				if _, err := collectionA.RebuildVectorIndex(ordinaryDef.Name); err != nil {
					t.Fatal(err)
				}
				ordinaryA = collectionA.registeredVectorIndex(ordinaryDef.Name)
				if ordinaryA == nil {
					t.Fatal("manager A ordinary native index is not loaded")
				}
			}
			if err := collectionA.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
				t.Fatal(err)
			}
			collectionB, err := NewCollectionManager(database).OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			var ordinaryB *VectorIndex
			if tc.withOrdinary {
				var loadStatus VectorIndexLoadStatus
				ordinaryB, loadStatus, err = collectionB.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(ordinaryDef))
				if err != nil || ordinaryB == nil || !loadStatus.Loaded {
					t.Fatalf("manager B ordinary native load=%v status=%+v err=%v", ordinaryB != nil, loadStatus, err)
				}
			}
			replace := func(collection *Collection, sequence int64, vector []float32) {
				t.Helper()
				replacement, err := json.Marshal(map[string]any{
					"time_us": sequence, "kind": "vector", "did": "a", "embedding": vector,
				})
				if err != nil {
					t.Fatal(err)
				}
				if matched, err := collection.Replace([]byte("a"), replacement); err != nil || !matched {
					t.Fatalf("replacement %d matched=%v err=%v", sequence, matched, err)
				}
			}
			assertRevision := func(collection *Collection, wantRevision uint64, query []float32) {
				t.Helper()
				pin, err := collection.AcquireVectorPartitionLiveSearchPinV1(manifest)
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Release()
				results, _, searchErr := pin.SearchDomainV1(t.Context(), 0, query, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16})
				if searchErr != nil || len(results) != 1 || results[0].ID != "a" || pin.StatusV1().Revision != wantRevision {
					t.Fatalf("revision=%d results=%+v status=%+v err=%v", wantRevision, results, pin.StatusV1(), searchErr)
				}
			}

			replace(collectionB, 1, []float32{0.2, 1})
			assertRevision(collectionB, 1, []float32{0.2, 1})
			replace(collectionA, 2, []float32{1, 0.2})
			if ordinaryA != nil && ordinaryA.hasValidSourceDocumentRoots() {
				t.Fatal("manager A stale ordinary native index was not invalidated before rebuild")
			}
			assertRevision(collectionA, 2, []float32{1, 0.2})
			replace(collectionB, 3, []float32{0.3, 1})
			if ordinaryB != nil && ordinaryB.hasValidSourceDocumentRoots() {
				t.Fatal("manager B stale ordinary native index was not invalidated before rebuild")
			}
			assertRevision(collectionB, 3, []float32{0.3, 1})
			collectionA.invalidateOtherVectorIndexDocumentCoverage()
			assertRevision(collectionB, 3, []float32{0.3, 1})
		})
	}
}

func openVectorPartitionLiveDurableDBV1(t testing.TB, dir string) *backenddb.DB {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{
		Dir: dir, DisableBackgroundPrune: true, CommandWAL: true,
		ResolvedProfile: backenddb.ProfileCommandWALDurable,
	})
	if err != nil {
		t.Fatalf("Open command WAL durable DB: %v", err)
	}
	return database
}

func TestVectorPartitionLiveReplayMutationCoalescingV1(t *testing.T) {
	vector := []float32{0, 1}
	declared := func(id string) columnDeclaredRow {
		return columnDeclaredRow{ID: []byte(id), Values: []columnDeclaredValue{{
			Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vector,
		}}}
	}
	for _, tc := range []struct {
		name  string
		input columnWritePublishInput
		ids   []string
		live  []bool
	}{
		{name: "ordinary_insert", input: columnWritePublishInput{operation: ColumnPublishOperationInsert, declaredRows: []columnDeclaredRow{declared("insert")}}, ids: []string{"insert"}, live: []bool{true}},
		{name: "ordinary_update", input: columnWritePublishInput{operation: ColumnPublishOperationUpdate, declaredRows: []columnDeclaredRow{declared("update")}}, ids: []string{"update"}, live: []bool{true}},
		{name: "delete", input: columnWritePublishInput{operation: ColumnPublishOperationDelete, documents: []columnWriteDocument{{ID: []byte("delete")}}}, ids: []string{"delete"}, live: []bool{false}},
		{
			name: "source_delete_then_insert_wins",
			input: columnWritePublishInput{
				operation:             ColumnPublishOperationInsert,
				sourceDeleteDocuments: []columnWriteDocument{{ID: []byte("same")}, {ID: []byte("gone")}},
				declaredRows:          []columnDeclaredRow{declared("same")},
			},
			ids: []string{"same", "gone"}, live: []bool{true, false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids, vectors, err := vectorPartitionLiveReplayMutationsV1(tc.input, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != len(tc.ids) || len(vectors) != len(tc.live) {
				t.Fatalf("ids=%q vectors=%v", ids, vectors)
			}
			for i := range ids {
				if string(ids[i]) != tc.ids[i] || (vectors[i] != nil) != tc.live[i] {
					t.Fatalf("effect[%d]=%q/%v want=%q/live=%t", i, ids[i], vectors[i], tc.ids[i], tc.live[i])
				}
			}
		})
	}
}

func newVectorPartitionLiveProductionFixtureV1(t *testing.T, openOptions ...backenddb.Options) (string, *backenddb.DB, *Collection, VectorIndexDefinition, VectorPartitionManifestV1) {
	return newVectorPartitionLiveProductionFixtureWithIndexesV1(t, nil, openOptions...)
}

func newVectorPartitionLiveProductionFixtureWithIndexesV1(t *testing.T, extraVectorIndexes []VectorIndexDefinition, openOptions ...backenddb.Options) (string, *backenddb.DB, *Collection, VectorIndexDefinition, VectorPartitionManifestV1) {
	t.Helper()
	rows := []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, 0}},
		{id: "b", vector: []float32{.8, .2}},
		{id: "c", vector: []float32{0, 1}},
	}
	dir, database, collection, def := openColumnGraphTypedColumnVectorTestCollectionWithIndexes1782(t, 2, 2, rows, extraVectorIndexes, openOptions...)
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
