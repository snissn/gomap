package collections

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// A hidden raw buffer has the same durable generation as its old graph. One
// index's completed maintenance must not certify a second installed identity.

// Sequential repair must copy borrowed scalar bytes into both graph planes,
// and clear prior scalar presence before parsing the next document.

func TestBufferedVectorRepairPreservesLiveOwnerRevision(t *testing.T) {
	index, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := index.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	index.partitionLiveOnly = true
	index.mu.Lock()
	if err := index.reconcileVectorPartitionMutationLocked([]byte("doc"), []float32{1, 0}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	beforeRevision, beforeSeq, beforeNodes := index.partitionLive.revision, index.mutationSeq, index.partitionLiveNodeCountLocked()
	index.mu.Unlock()
	if err := index.reconcileBufferedStoredDocument(&StoredDocumentJSONMaterializer{documentFormat: DocumentFormatJSON}, []byte("doc"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	index.mu.RLock()
	unchanged := index.partitionLive.revision == beforeRevision && index.mutationSeq == beforeSeq && index.partitionLiveNodeCountLocked() == beforeNodes
	index.mu.RUnlock()
	if !unchanged {
		t.Fatal("unchanged live owner/vector created revision or sequence debt")
	}
	index.setNativePersistent(true)
	index.recordSourceDocumentState(4, backenddb.StateToken{})
	index.mu.RLock()
	persistable := index.partitionLive.coverage == 4 && index.dirtyMeta
	index.mu.RUnlock()
	if !persistable {
		t.Fatal("real source coverage publication lost persistence debt")
	}
	index.mu.Lock()
	index.unnotifiedDocumentIDs = map[string]struct{}{"unrelated": {}}
	sequence := index.mutationSeq
	index.mu.Unlock()
	candidate, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4})
	if err != nil {
		t.Fatal(err)
	}
	if reason := candidate.clonePartitionLiveReplayStateV2(index, sequence, 1); reason == "" {
		t.Fatal("live replay clone erased an unrelated missing-ID proof")
	}
}

// Drive the real FIFO publisher with an owned newer same-ID overlay and a raw
// unrelated tail. Requeue must retain the gap, prefix repair must retain the
// newer vector, and only the later tail publication may clear the remaining ID.

// Public notification must retain a loaded baseline across its exact-ID gap.
// A genuinely invalid sibling still rebuilds and preflushes under that owner.

func TestBufferedNativeCoverageColumnGraphNotificationRetainsCarrier(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def, manifest := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	defer func() { _ = d.Close() }()
	if err := col.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	before := col.registeredColumnGraphPartitionLiveCarrier(def.Name)
	if before == nil || !before.isPartitionLiveCarrier() {
		t.Fatal("fixture has no installed live carrier")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("new")}, [][]byte{[]byte(`{"time_us":4,"kind":"vector","did":"new","embedding":[0,1]}`)}); err != nil {
		t.Fatal(err)
	}
	current := col.registeredColumnGraphPartitionLiveCarrier(def.Name)
	generation, err := col.currentVectorIndexDocumentGeneration()
	if err != nil || current == nil || !current.isPartitionLiveCarrier() || !current.coversSourceDocumentGeneration(generation) {
		t.Fatalf("live notification coverage generation=%d err=%v", generation, err)
	}
	pin, err := current.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, _, err := pin.SearchDomainV1(t.Context(), 0, []float32{0, 1}, VectorPartitionSearchOptionsV1{TopK: 4, EfSearch: 8})
	pin.Release()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, result := range results {
		found = found || result.ID == "new"
	}
	if !found {
		t.Fatalf("public live insert absent from maintained carrier: %+v", results)
	}
}

// Setup maintains the logical graph outside the timer. The measured operation
// is real primary publication plus exact-ID repair and native persistence.
// Use a fixed iteration count: unique rows accumulate in the loaded graph.

// The default async threshold must not start a publisher beneath the coverage
// owner that may need to load or rebuild an installed sibling.

// A real assigned prepared callback retains admission through owned flush and
// Finalize. Its threshold work must stay in the existing queue until then.

// Time both public notification/admission and the physical publication boundary.
// Public APIs only: the identical harness can run on the original candidate.

// A local exact-ID gap cannot explain an external generation that predates
// this owner's admission. Both managers use the real public executor.

// A second ordinary notification under one owner uses that installed index's
// completed receipt, rather than treating the owner's first publication as an
// unexplained generation or certifying before the owner releases admission.
