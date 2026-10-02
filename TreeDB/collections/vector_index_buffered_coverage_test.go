package collections

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// A hidden raw buffer has the same durable generation as its old graph. One
// index's completed maintenance must not certify a second installed identity.
func TestBufferedNativeCoverageTracksUnnotifiedIDsPerIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	defs := []VectorIndexDefinition{
		{Name: "first", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime},
		{Name: "second", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime},
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 1024, DisableBufferedIndexedAsyncFlush: true}, VectorIndexes: defs}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range defs {
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := col.InsertBatch([][]byte{[]byte("seed")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	generation, err := col.currentVectorIndexDocumentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	first, second := col.registeredVectorIndex("first"), col.registeredVectorIndex("second")
	ids, err := col.insertBatch([][]byte{[]byte("raw")}, [][]byte{[]byte(`{"embedding":[0,1]}`)}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	after, err := col.currentVectorIndexDocumentGeneration()
	if err != nil || after != generation || mgr.StatsSnapshot().PendingDocuments == 0 {
		t.Fatalf("raw buffer changed durable generation or drained: before=%d after=%d err=%v", generation, after, err)
	}
	for _, def := range defs {
		index := col.registeredVectorIndex(def.Name)
		if index.coversSourceDocumentGeneration(generation) {
			t.Fatalf("%s certified hidden raw buffer", def.Name)
		}
		var buffer VectorIndexSearchBuffer
		response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1}, TopK: 2, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
		if !errors.Is(err, ErrVectorIndexSearchUnavailable) || response.Status.ExactFallbackReason != vectorIndexFallbackStaleDocumentRoot {
			t.Fatalf("%s hidden-buffer search response=%+v err=%v", def.Name, response, err)
		}
	}
	if mgr.StatsSnapshot().PendingDocuments == 0 {
		t.Fatal("stale refusal drained hidden buffer")
	}
	unlock := col.lockVectorIndexCoverageMutation()
	err = col.reconcileLoadedVectorIndexes(ids, []*VectorIndex{first}, nil, true)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !first.coversSourceDocumentGeneration(generation) || second.coversSourceDocumentGeneration(generation) {
		t.Fatal("one index receipt certified another installed index")
	}
	first.mu.RLock()
	beforeNodes, beforeSeq := len(first.nodes), first.mutationSeq
	first.mu.RUnlock()
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	current, err := col.currentVectorIndexDocumentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	if !first.coversSourceDocumentGeneration(current) || !second.coversSourceDocumentGeneration(current) || mgr.StatsSnapshot().PendingDocuments != 0 {
		t.Fatal("exact publication did not repair and drain both installed indexes")
	}
	first.mu.RLock()
	if len(first.nodes) != beforeNodes || first.mutationSeq != beforeSeq {
		first.mu.RUnlock()
		t.Fatal("publication duplicated already maintained graph nodes")
	}
	first.mu.RUnlock()
	for _, def := range defs {
		var buffer VectorIndexSearchBuffer
		response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1}, TopK: 2, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
		if err != nil || len(response.Results) != 2 || string(response.Results[0].ID) != "raw" {
			t.Fatalf("%s repaired search response=%+v err=%v", def.Name, response, err)
		}
	}
}

func TestBufferedVectorRepairPreservesScalarDeltaAndMissingValues(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch([][]byte{[]byte("doc")}, [][]byte{[]byte(`{"embedding":[1,0],"tenant":"old"}`)}); err != nil {
		t.Fatal(err)
	}
	index := col.registeredVectorIndex(def.Name)
	if _, err := index.SaveNativeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.Update([]byte("doc"), func([]byte) ([]byte, bool, error) { return []byte(`{"embedding":[0,1],"tenant":"tail"}`), true, nil }); err != nil {
		t.Fatal(err)
	}
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = materializer.Close() }()
	index.mu.RLock()
	beforeNodes, beforeSeq := len(index.nodes), index.mutationSeq
	beforeDelta := 0
	if index.liveDelta != nil {
		beforeDelta = len(index.liveDelta.nodes)
	}
	index.mu.RUnlock()
	if err := index.reconcileBufferedStoredDocument(materializer, []byte("doc"), []byte(`{"embedding":[0,1],"tenant":"tail"}`)); err != nil {
		t.Fatal(err)
	}
	index.mu.RLock()
	afterDelta := 0
	if index.liveDelta != nil {
		afterDelta = len(index.liveDelta.nodes)
	}
	unchanged := len(index.nodes) == beforeNodes && index.mutationSeq == beforeSeq && afterDelta == beforeDelta
	index.mu.RUnlock()
	if !unchanged {
		t.Fatal("same effective scalar/delta row created graph debt")
	}
	// Equal vectors with a missing scalar are a real row change.
	if err := index.reconcileBufferedStoredDocument(materializer, []byte("doc"), []byte(`{"embedding":[0,1]}`)); err != nil {
		t.Fatal(err)
	}
	tailScalars, err := index.nativeScalarRow(materializer, []byte(`{"embedding":[0,1],"tenant":"tail"}`))
	if err != nil {
		t.Fatal(err)
	}
	index.mu.RLock()
	stillOld := index.bufferedStoredVectorMatchesLocked([]byte("doc"), []float32{0, 1}, tailScalars)
	missing := index.bufferedStoredVectorMatchesLocked([]byte("doc"), []float32{0, 1}, nil)
	index.mu.RUnlock()

	if stillOld || !missing {
		t.Fatal("scalar presence change was mistaken for vector-only equality")
	}
	// Batch filtering must keep a changed row after a same-row delta shadow.
	beforeBatch := index.Stats().Nodes
	ids := [][]byte{[]byte("doc"), []byte("new")}
	if err := index.reconcileStoredDocumentsUnpublished(materializer, ids, [][]byte{
		[]byte(`{"embedding":[0,1]}`), []byte(`{"embedding":[1,0],"tenant":"fresh"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if index.Stats().Nodes != beforeBatch+1 || string(ids[0]) != "doc" || string(ids[1]) != "new" {
		t.Fatal("batch equality duplicated a row or modified caller IDs")
	}
	if err := index.reconcileStoredDocumentsUnpublished(materializer, ids, [][]byte{
		[]byte(`{"embedding":[0,1],"tenant":"restored"}`), []byte(`{"embedding":[1,0],"tenant":"fresh"}`),
	}); err != nil {
		t.Fatal(err)
	}
	restoredScalars, err := index.nativeScalarRow(materializer, []byte(`{"embedding":[0,1],"tenant":"restored"}`))
	if err != nil {
		t.Fatal(err)
	}
	index.mu.RLock()
	restored := index.bufferedStoredVectorMatchesLocked([]byte("doc"), []float32{0, 1}, restoredScalars)
	index.mu.RUnlock()
	if !restored {
		t.Fatal("batch equality discarded a scalar presence/value change")
	}
}

func TestBufferedVectorRepairPreservesLiveOwnerRevision(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
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
	candidate, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4})
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
func TestBufferedNativeCoveragePrefixRepairPreservesOwnedTailAndRequeue(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 1024, DisableBufferedIndexedAsyncFlush: true}, Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := col.insertBatch([][]byte{[]byte("same")}, [][]byte{[]byte(`{"kind":"vector","embedding":[1,0]}`)}, false, nil); err != nil {
		t.Fatal(err)
	}
	index := col.registeredVectorIndex(def.Name)
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil || work == nil {
		t.Fatalf("prepare prefix work=%v err=%v", work, err)
	}
	injected := errors.New("publication did not accept prefix")
	if err := col.completePreparedIndexedFlush(work, 0, nil, injected, 0, 0, 0); !errors.Is(err, injected) {
		t.Fatalf("requeue error=%v", err)
	}
	collectionTestCloseIndexedFlushWork(work)
	index.mu.RLock()
	_, missing := index.unnotifiedDocumentIDs["same"]
	index.mu.RUnlock()
	if !missing || mgr.StatsSnapshot().PendingDocuments == 0 {
		t.Fatal("failed publication lost owned buffer or gap proof")
	}
	work, err = col.prepareIndexedAsyncPublish()
	if err != nil || work == nil {
		t.Fatalf("prepare requeued prefix work=%v err=%v", work, err)
	}
	defer collectionTestCloseIndexedFlushWork(work)
	if _, err := col.insertBatch([][]byte{[]byte("rawtail")}, [][]byte{[]byte(`{"kind":"vector","embedding":[1,0]}`)}, false, nil); err != nil {
		t.Fatal(err)
	}
	tailDocument := []byte(`{"kind":"vector","embedding":[0,1]}`)
	err = func() error {
		unlockSchema := col.lockCollectionSchemaRead()
		defer unlockSchema()
		admission := col.lockCollectionCommandWALAdmission()
		defer admission.unlock()
		items := []UpdateBatchItem{{DocumentID: []byte("same"), Update: func([]byte) ([]byte, bool, error) { return tailDocument, true, nil }}}
		results, batched, err := col.updateBatchSchemaLocked(items, updateBatchModeNoSecondaryUniqueIndexes, &admission)
		if err != nil {
			return err
		}
		if !batched || len(results) != 1 || !results[0].Matched || !results[0].Modified {
			return errors.New("owned same-ID tail did not stage its update")
		}
		return col.notifyVectorIndexesUpdateBatch(items, results)
	}()
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = materializer.Close() }()
	tailScalars, err := index.nativeScalarRow(materializer, tailDocument)
	if err != nil {
		t.Fatal(err)
	}
	index.mu.RLock()
	beforeMatches := index.bufferedStoredVectorMatchesLocked([]byte("same"), []float32{0, 1}, tailScalars)
	index.mu.RUnlock()
	if !beforeMatches {
		t.Fatal("owned tail vector/scalars not maintained before prefix publication")
	}
	index.mu.RLock()
	beforeNodes, beforeSeq := len(index.nodes), index.mutationSeq
	index.mu.RUnlock()
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatal(err)
	}
	generation, err := col.currentVectorIndexDocumentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	index.mu.RLock()
	_, tailMissing := index.unnotifiedDocumentIDs["rawtail"]
	newer := index.bufferedStoredVectorMatchesLocked([]byte("same"), []float32{0, 1}, tailScalars)
	unchanged := len(index.nodes) == beforeNodes && index.mutationSeq == beforeSeq
	index.mu.RUnlock()
	if !tailMissing || !newer || !unchanged || index.coversSourceDocumentGeneration(generation) {
		t.Fatalf("prefix downgraded owned tail or certified unrelated raw gap: tailMissing=%v newer=%v unchanged=%v", tailMissing, newer, unchanged)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	generation, err = col.currentVectorIndexDocumentGeneration()
	if err != nil || !index.coversSourceDocumentGeneration(generation) || mgr.StatsSnapshot().PendingDocuments != 0 {
		t.Fatalf("tail publication coverage generation=%d err=%v", generation, err)
	}
	var buffer VectorIndexSearchBuffer
	response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1}, TopK: 2, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil || len(response.Results) != 2 || string(response.Results[0].ID) != "same" {
		t.Fatalf("tail search response=%+v err=%v", response, err)
	}
}

// Public notification must retain a loaded baseline across its exact-ID gap.
// A genuinely invalid sibling still rebuilds and preflushes under that owner.
func TestBufferedNativeCoveragePublicInsertRetainsLoadedBaseline(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, indexed := range []bool{false, true} {
			for _, invalidSibling := range []bool{false, true} {
				if invalidSibling && !buffered {
					continue
				}
				t.Run(fmt.Sprintf("buffered=%v/indexed=%v/invalid-sibling=%v", buffered, indexed, invalidSibling), func(t *testing.T) {
					d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = d.Close() }()
					defs := []VectorIndexDefinition{
						{Name: "first", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime},
						{Name: "second", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime},
					}
					meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: buffered, BufferedIndexedWriteMaxDocuments: 1024, DisableBufferedIndexedAsyncFlush: true}, VectorIndexes: defs}
					if indexed {
						meta.Indexes = []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}}
					}
					mgr := NewCollectionManager(d)
					if _, err := mgr.CreateCollection(meta); err != nil {
						t.Fatal(err)
					}
					col, err := mgr.OpenCollection(meta.Name)
					if err != nil {
						t.Fatal(err)
					}
					for _, def := range defs {
						if _, err := col.RebuildVectorIndex(def.Name); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := col.InsertBatch([][]byte{[]byte("seed")}, [][]byte{[]byte(`{"kind":"vector","embedding":[1,0]}`)}); err != nil {
						t.Fatal(err)
					}
					if err := col.Flush(); err != nil {
						t.Fatal(err)
					}
					first, second := col.registeredVectorIndex("first"), col.registeredVectorIndex("second")
					before := first.Stats()
					if invalidSibling {
						second.invalidateSourceDocumentRoots()
					}
					if _, err := col.InsertBatch([][]byte{[]byte("new-a"), []byte("new-b")}, [][]byte{
						[]byte(`{"kind":"vector","embedding":[0,1]}`), []byte(`{"kind":"vector","embedding":[0.2,0.8]}`),
					}); err != nil {
						t.Fatal(err)
					}
					if col.registeredVectorIndex("first") != first {
						t.Fatal("exact-ID notification replaced the retained baseline")
					}
					after := first.Stats()
					if after.LiveANNFullRebuilds != before.LiveANNFullRebuilds || after.Nodes != before.Nodes+2 {
						t.Fatalf("notification rebuilt or duplicated valid sibling: before=%+v after=%+v", before, after)
					}
					if invalidSibling {
						if col.registeredVectorIndex("second") == second || second.hasValidSourceDocumentRoots() {
							t.Fatal("genuinely invalid sibling was certified instead of rebuilt")
						}
					} else if col.registeredVectorIndex("second") != second {
						t.Fatal("notification replaced valid second baseline")
					}
					if buffered && !invalidSibling && mgr.StatsSnapshot().PendingDocuments == 0 {
						t.Fatal("notification unnecessarily drained public buffer")
					}
					generation, err := col.currentVectorIndexDocumentGeneration()
					if err != nil {
						t.Fatal(err)
					}
					for _, def := range defs {
						index := col.registeredVectorIndex(def.Name)
						if !index.coversSourceDocumentGeneration(generation) {
							t.Fatalf("%s notification did not complete exact maintenance", def.Name)
						}
						var buffer VectorIndexSearchBuffer
						response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1}, TopK: 3, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
						if err != nil || len(response.Results) != 3 || string(response.Results[0].ID) != "new-a" {
							t.Fatalf("%s public search response=%+v err=%v", def.Name, response, err)
						}
					}
					if err := col.Flush(); err != nil {
						t.Fatal(err)
					}
					if mgr.StatsSnapshot().PendingDocuments != 0 || first.Stats().Nodes != after.Nodes {
						t.Fatal("publication did not drain cleanly or duplicated graph nodes")
					}
				})
			}
		}
	}
}

func TestBufferedNativeCoverageColumnGraphNotificationRetainsCarrier(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def, manifest := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	defer func() { _ = d.Close() }()
	if err := col.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	before := col.registeredVectorIndex(def.Name)
	if before == nil || !before.isPartitionLiveCarrier() {
		t.Fatal("fixture has no installed live carrier")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("new")}, [][]byte{[]byte(`{"time_us":4,"kind":"vector","did":"new","embedding":[0,1]}`)}); err != nil {
		t.Fatal(err)
	}
	current := col.registeredVectorIndex(def.Name)
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
func BenchmarkBufferedNativeCoveragePublication(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		for _, indexCount := range []int{0, 1, 2} {
			for _, batchSize := range []int{1, 64, 256} {
				b.Run(fmt.Sprintf("indexed=%v/native=%d/batch=%d", indexed, indexCount, batchSize), func(b *testing.B) {
					d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = d.Close() }()
					meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 1048576, DisableBufferedIndexedAsyncFlush: true}}
					if indexed {
						meta.Indexes = []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}}
					}
					for i := 0; i < indexCount; i++ {
						meta.VectorIndexes = append(meta.VectorIndexes, VectorIndexDefinition{Name: fmt.Sprintf("vector%d", i), Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime})
					}
					mgr := NewCollectionManager(d)
					if _, err := mgr.CreateCollection(meta); err != nil {
						b.Fatal(err)
					}
					col, err := mgr.OpenCollection(meta.Name)
					if err != nil {
						b.Fatal(err)
					}
					for _, def := range meta.VectorIndexes {
						if _, err := col.RebuildVectorIndex(def.Name); err != nil {
							b.Fatal(err)
						}
					}
					document := []byte(`{"kind":"vector","embedding":[0.2,0.8]}`)
					b.ReportAllocs()
					b.SetBytes(int64(batchSize * len(document)))
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						b.StopTimer()
						ids, documents := make([][]byte, batchSize), make([][]byte, batchSize)
						for i := range ids {
							ids[i], documents[i] = []byte(fmt.Sprintf("%012d", iteration*batchSize+i)), document
						}
						if _, err := col.InsertBatch(ids, documents); err != nil {
							b.Fatal(err)
						}
						if mgr.StatsSnapshot().PendingDocuments == 0 {
							b.Fatal("setup did not retain buffered primary rows")
						}
						b.StartTimer()
						if err := col.Flush(); err != nil {
							b.Fatal(err)
						}
						b.StopTimer()
						if mgr.StatsSnapshot().PendingDocuments != 0 {
							b.Fatal("publication did not drain pending documents")
						}
						b.StartTimer()
					}
					b.StopTimer()
					b.ReportMetric(float64(batchSize), "documents/op")
				})
			}
		}
	}
}

// The default async threshold must not start a publisher beneath the coverage
// owner that may need to load or rebuild an installed sibling.
func TestBufferedNativeCoverageDefaultAsyncThresholdAdmission(t *testing.T) {
	for _, mode := range []string{"no-native", "healthy", "mixed-invalid", "missing"} {
		t.Run(mode, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
			if err != nil {
				t.Fatal(err)
			}
			safeClose := true
			defer func() {
				if safeClose {
					_ = d.Close()
				}
			}()
			meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 1}, Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}}}
			if mode != "no-native" {
				for _, name := range []string{"first", "second"} {
					meta.VectorIndexes = append(meta.VectorIndexes, VectorIndexDefinition{Name: name, Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime})
				}
			}
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(meta); err != nil {
				t.Fatal(err)
			}
			col, err := mgr.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !col.Meta().Options.BufferedIndexedAsyncFlush {
				t.Fatal("fixture disabled default async publication")
			}
			var first, second *VectorIndex
			var before VectorIndexStats
			wantRows := 1
			if mode == "healthy" || mode == "mixed-invalid" {
				for _, def := range meta.VectorIndexes {
					if _, err := col.RebuildVectorIndex(def.Name); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := col.InsertBatch([][]byte{[]byte("seed")}, [][]byte{[]byte(`{"kind":"vector","embedding":[1,0]}`)}); err != nil {
					t.Fatal(err)
				}
				if err := col.Flush(); err != nil {
					t.Fatal(err)
				}
				first, second = col.registeredVectorIndex("first"), col.registeredVectorIndex("second")
				before = first.Stats()
				wantRows++
				if mode == "mixed-invalid" {
					second.invalidateSourceDocumentRoots()
				}
			}
			scheduled := col.writeDomain.indexedAsyncFlushScheduled.Load()
			done := make(chan error, 1)
			safeClose = false
			go func() {
				_, err := col.InsertBatch([][]byte{[]byte("new")}, [][]byte{[]byte(`{"kind":"vector","embedding":[0,1]}`)})
				if err == nil {
					err = col.Flush()
				}
				done <- err
			}()
			select {
			case err := <-done:
				safeClose = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				stacks := make([]byte, 128<<10)
				n := runtime.Stack(stacks, true)
				t.Fatalf("public threshold insert/Flush blocked under admission; Close skipped to retain failure evidence:\n%s", stacks[:n])
			}
			if mgr.StatsSnapshot().PendingDocuments != 0 || col.writeDomain.indexedAsyncFlushRunning() {
				t.Fatal("public threshold operation did not fully drain")
			}
			col.writeDomain.mu.RLock()
			deferred := col.writeDomain.indexedAsyncFlushDeferred
			col.writeDomain.mu.RUnlock()
			if deferred {
				t.Fatal("drained owner retained a deferred async start")
			}
			if (mode == "no-native" || mode == "healthy") && col.writeDomain.indexedAsyncFlushScheduled.Load() <= scheduled {
				t.Fatal("threshold no longer schedules actual async publication")
			}
			if first != nil {
				if col.registeredVectorIndex("first") != first {
					t.Fatal("healthy baseline replaced")
				}
				after := first.Stats()
				if after.Nodes != before.Nodes+1 || after.LiveANNFullRebuilds != before.LiveANNFullRebuilds {
					t.Fatalf("healthy sibling duplicated or rebuilt: before=%+v after=%+v", before, after)
				}
			}
			if mode == "mixed-invalid" && col.registeredVectorIndex("second") == second {
				t.Fatal("genuinely invalid sibling was retained")
			}
			generation, err := col.currentVectorIndexDocumentGeneration()
			if err != nil {
				t.Fatal(err)
			}
			for _, def := range meta.VectorIndexes {
				index := col.registeredVectorIndex(def.Name)
				if index == nil || !index.coversSourceDocumentGeneration(generation) {
					t.Fatalf("%s not current after threshold publication", def.Name)
				}
				var buffer VectorIndexSearchBuffer
				response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1}, TopK: wantRows, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
				if err != nil || len(response.Results) != wantRows || string(response.Results[0].ID) != "new" {
					t.Fatalf("%s threshold search response=%+v err=%v", def.Name, response, err)
				}
			}
		})
	}
}

// A real assigned prepared callback retains admission through owned flush and
// Finalize. Its threshold work must stay in the existing queue until then.
func TestBufferedNativeCoveragePreparedOwnerDefaultAsyncThreshold(t *testing.T) {
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	meta := CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 1}, Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}}, VectorIndexes: []VectorIndexDefinition{def}}
	dir := prepareCollectionCommandWALDir(t, meta)
	d := openCollectionCommandWALDB(t, dir)
	safeClose := true
	defer func() {
		if safeClose {
			_ = d.Close()
		}
	}()
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if !col.Meta().Options.BufferedIndexedAsyncFlush {
		t.Fatal("fixture disabled default async publication")
	}
	document := []byte(`{"kind":"vector","embedding":[0,1]}`)
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(meta.Name, []commitlog.CollectionDocument{{ID: []byte("new"), Document: document}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := commandwalapply.CollectionInsertBatchByIDFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	safeClose = false
	go func() {
		done <- col.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
			handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
			if err != nil {
				return err
			}
			complete := false
			defer func() {
				if !complete {
					commandwalapply.Abort(d, handle)
				}
			}()
			if _, err := owner.InsertBatchWithCommandWALIntent([][]byte{[]byte("new")}, [][]byte{document}, false, handle.CommandWALIntent()); err != nil {
				return err
			}
			if col.writeDomain.indexedAsyncFlushRunning() {
				return errors.New("assigned owner started a publisher needing its admission")
			}
			// This is the actual admitted flush entrypoint, not an ordinary wrapper.
			if err := col.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
				return err
			}
			if _, err := commandwalapply.Finalize(d, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true}); err != nil {
				return err
			}
			complete = true
			return nil
		})
	}()
	select {
	case err := <-done:
		safeClose = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		stacks := make([]byte, 128<<10)
		n := runtime.Stack(stacks, true)
		t.Fatalf("prepared threshold callback blocked; Close skipped to retain failure evidence:\n%s", stacks[:n])
	}
	if mgr.StatsSnapshot().PendingDocuments != 0 || col.writeDomain.indexedAsyncFlushRunning() {
		t.Fatal("owned publication did not drain")
	}
	if d.State().AppliedCommandLSN == 0 || d.CommandWALNextLSN() != d.State().AppliedCommandLSN+1 {
		t.Fatal("real Finalize did not cover the appended frame")
	}
	index := col.registeredVectorIndex(def.Name)
	generation, err := col.currentVectorIndexDocumentGeneration()
	if err != nil || index == nil || !index.coversSourceDocumentGeneration(generation) {
		t.Fatalf("prepared publication not current: generation=%d err=%v", generation, err)
	}
	var buffer VectorIndexSearchBuffer
	response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil || len(response.Results) != 1 || string(response.Results[0].ID) != "new" {
		t.Fatalf("prepared threshold search response=%+v err=%v", response, err)
	}
}

// Time both public notification/admission and the physical publication boundary.
// Public APIs only: the identical harness can run on the original candidate.
func BenchmarkBufferedNativeCoverageInsertAndPublication(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		for _, indexCount := range []int{0, 1, 2} {
			for _, batchSize := range []int{1, 64} {
				b.Run(fmt.Sprintf("indexed=%v/native=%d/batch=%d", indexed, indexCount, batchSize), func(b *testing.B) {
					d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = d.Close() }()
					meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 1}}
					if indexed {
						meta.Indexes = []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}}
					}
					for i := 0; i < indexCount; i++ {
						meta.VectorIndexes = append(meta.VectorIndexes, VectorIndexDefinition{Name: fmt.Sprintf("vector%d", i), Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime})
					}
					mgr := NewCollectionManager(d)
					if _, err := mgr.CreateCollection(meta); err != nil {
						b.Fatal(err)
					}
					col, err := mgr.OpenCollection(meta.Name)
					if err != nil {
						b.Fatal(err)
					}
					if (indexed || indexCount > 0) && !col.Meta().Options.BufferedIndexedAsyncFlush {
						b.Fatal("default async threshold is disabled")
					}
					rebuilds := make(map[string]uint64, indexCount)
					for _, def := range meta.VectorIndexes {
						status, err := col.RebuildVectorIndex(def.Name)
						if err != nil {
							b.Fatal(err)
						}
						rebuilds[def.Name] = status.Stats.LiveANNFullRebuilds
					}
					document := []byte(`{"kind":"vector","embedding":[0.2,0.8]}`)
					b.ReportAllocs()
					b.SetBytes(int64(batchSize * len(document)))
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						b.StopTimer()
						ids, documents := make([][]byte, batchSize), make([][]byte, batchSize)
						for i := range ids {
							ids[i], documents[i] = []byte(fmt.Sprintf("%012d", iteration*batchSize+i)), document
						}
						b.StartTimer()
						if _, err := col.InsertBatch(ids, documents); err != nil {
							b.Fatal(err)
						}
						if err := col.Flush(); err != nil {
							b.Fatal(err)
						}
						b.StopTimer()
						if mgr.StatsSnapshot().PendingDocuments != 0 {
							b.Fatal("timed InsertBatch+Flush did not drain primary publication")
						}
						for _, def := range meta.VectorIndexes {
							status, err := col.VectorIndexStatus(def.Name)
							if err != nil {
								b.Fatal(err)
							}
							if status.ExactFallbackReason != "" || status.Stats.LiveDocs != (iteration+1)*batchSize || status.Stats.Nodes != (iteration+1)*batchSize || status.Stats.LiveANNFullRebuilds != rebuilds[def.Name] {
								b.Fatalf("timed public mutation rebuilt, duplicated or lost source proof: %+v", status)
							}
							var buffer VectorIndexSearchBuffer
							response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0.2, 0.8}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
							if err != nil || response.Path != VectorIndexSearchPathNativeRuntime || len(response.Results) != 1 {
								b.Fatalf("timed publication did not leave a current native search: response=%+v err=%v", response, err)
							}
						}
						b.StartTimer()
					}
					b.StopTimer()
					b.ReportMetric(float64(batchSize), "documents/op")
				})
			}
		}
	}
}

// A local exact-ID gap cannot explain an external generation that predates
// this owner's admission. Both managers use the real public executor.
func TestBufferedNativeCoverageExternalGenerationBeforeLocalGap(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close() }()
			def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
			manager := NewCollectionManager(d)
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, BufferedIndexedWrites: buffered, BufferedIndexedWriteMaxDocuments: 1024, DisableBufferedIndexedAsyncFlush: true}, Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			first, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.InsertBatch([][]byte{[]byte("seed")}, [][]byte{[]byte(`{"kind":"vector","embedding":[1,0]}`)}); err != nil {
				t.Fatal(err)
			}
			if err := first.Flush(); err != nil {
				t.Fatal(err)
			}
			original := first.registeredVectorIndex(def.Name)
			second, err := NewCollectionManager(d).OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := second.InsertBatch([][]byte{[]byte("external")}, [][]byte{[]byte(`{"kind":"vector","embedding":[0,1]}`)}); err != nil {
				t.Fatal(err)
			}
			if err := second.Flush(); err != nil {
				t.Fatal(err)
			}
			generation, err := first.currentVectorIndexDocumentGeneration()
			if err != nil {
				t.Fatal(err)
			}
			if original.hasSourceDocumentReconciliationBaseline(generation) {
				t.Fatal("external generation was already accounted")
			}
			if _, err := first.InsertBatch([][]byte{[]byte("local")}, [][]byte{[]byte(`{"kind":"vector","embedding":[1,1]}`)}); err != nil {
				t.Fatal(err)
			}
			if err := first.Flush(); err != nil {
				t.Fatal(err)
			}
			refreshed := first.registeredVectorIndex(def.Name)
			if refreshed == original {
				t.Fatal("local gap retained an unaccounted external baseline")
			}
			results, _, err := refreshed.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
			if err != nil {
				t.Fatal(err)
			}
			requireVectorResultIDs(t, results, "seed", "local", "external")
			generation, err = first.currentVectorIndexDocumentGeneration()
			if err != nil || !refreshed.coversSourceDocumentGeneration(generation) || manager.StatsSnapshot().PendingDocuments != 0 {
				t.Fatalf("completed public coverage generation=%d err=%v", generation, err)
			}
		})
	}
}

// A second ordinary notification under one owner uses that installed index's
// completed receipt, rather than treating the owner's first publication as an
// unexplained generation or certifying before the owner releases admission.
func TestNativeVectorCoverageRepeatedOwnedUpdateRetainsAccountedRuntime(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	manager := NewCollectionManager(d)
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("seed")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		t.Fatal(err)
	}
	original := col.registeredVectorIndex(def.Name)
	if original == nil {
		t.Fatal("seed runtime missing")
	}
	err = func() error {
		unlockSchema := col.lockCollectionSchemaRead()
		defer unlockSchema()
		admission := col.lockCollectionCommandWALAdmission()
		defer admission.unlock()
		for _, document := range [][]byte{[]byte(`{"embedding":[0,1]}`), []byte(`{"embedding":[1,1]}`)} {
			items := []UpdateBatchItem{{DocumentID: []byte("seed"), Update: func([]byte) ([]byte, bool, error) { return document, true, nil }}}
			results, batched, err := col.updateBatchSchemaLocked(items, updateBatchModeAny, &admission)
			if err != nil {
				return err
			}
			if !batched || len(results) != 1 || !results[0].Modified {
				return errors.New("owned update did not modify seed")
			}
			if err := col.notifyVectorIndexesUpdateBatch(items, results); err != nil {
				return err
			}
			if col.registeredVectorIndex(def.Name) != original {
				return errors.New("known ordinary notification rebuilt the loaded runtime")
			}
			generation, err := col.currentVectorIndexDocumentGeneration()
			if err != nil {
				return err
			}
			if original.coversSourceDocumentGeneration(generation) {
				return errors.New("ordinary notification certified before owner release")
			}
		}
		return nil
	}()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := col.currentVectorIndexDocumentGeneration()
	if err != nil || !original.coversSourceDocumentGeneration(generation) {
		t.Fatalf("owner release coverage generation=%d err=%v", generation, err)
	}
	original.mu.RLock()
	matches := original.bufferedStoredVectorMatchesLocked([]byte("seed"), []float32{1, 1}, nil)
	original.mu.RUnlock()
	if !matches {
		t.Fatal("completed ordinary updates lost the final vector")
	}
}
