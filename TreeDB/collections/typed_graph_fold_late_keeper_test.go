package collections

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestTypedGraphPublicEnsureLateSiblingKeeper(t *testing.T) {
	for _, fold := range []bool{false, true} {
		name := "same_base_suffix_keeps_warm"
		if fold {
			name = "new_nonempty_base_releases_late_warm"
		}
		t.Run(name, func(t *testing.T) {
			requireTypedGraphPublicServingTest(t)
			col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
			index := base.indexName
			if err := base.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, opts := context.Background(), typedGraphPublicTestOptions()
			if err := col.EnsureColumnGraphServing(ctx, index, opts); err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(col.db)
			sibling, err := manager.OpenCollection(col.Name())
			if err != nil {
				t.Fatal(err)
			}
			query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
			var buffer VectorIndexSearchBuffer
			response, held, err := col.SearchVectorIndexWithBufferReadView(query, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			before, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(before.Results) != 1 {
				t.Fatalf("old fetch=%+v error=%v", before.Results, err)
			}
			oldDocument := bytes.Clone(before.Results[0].Document)

			captured, release := make(chan *collectionVectorIndexPreparedSearch, 1), make(chan struct{})
			var paused, released atomic.Bool
			collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
			collectionVectorIndexPreparedSearchBuildHookForTest.afterBuild = func(p *collectionVectorIndexPreparedSearch) {
				if p != nil && p.collection == sibling && paused.CompareAndSwap(false, true) {
					captured <- p
					<-release
				}
			}
			collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
			defer func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
				collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
				collectionVectorIndexPreparedSearchBuildHookForTest.afterBuild = nil
				collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
			}()
			done := make(chan error, 1)
			go func() { done <- sibling.EnsureColumnGraphServing(ctx, index, opts) }()
			var keeper *collectionVectorIndexPreparedSearch
			select {
			case keeper = <-captured:
			case <-time.After(10 * time.Second):
				t.Fatal("sibling did not reach post-capture pause")
			}
			manager.collectionsMu.RLock()
			_, registered := manager.collections[sibling]
			manager.collectionsMu.RUnlock()
			if registered {
				t.Fatal("fixture already registered sibling before its first warm completed")
			}
			resources := keeper.capturedBase
			oldKey := resources.ref.key
			changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"late-sibling-update"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
			if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
				t.Fatal(err)
			}
			if fold {
				if err := col.FoldColumnGraphServing(ctx, index); err != nil {
					t.Fatal(err)
				}
			}
			state := col.collectionSchemaCoordinator().typedPublication.Load()
			if state.servingBase.graph.RowCount == 0 || (state.servingBase.preparedKey != oldKey) != fold {
				t.Fatal("fixture did not retain/change the nonempty base as requested")
			}
			accounting := resources.accounting
			accounting.Lock()
			ownersBefore, bytesBefore := accounting.baseOwners, accounting.baseAssetBytes
			descriptorsBefore, backingBefore := accounting.baseDescriptorBytes, accounting.baseBackingBytes
			accounting.Unlock()
			released.Store(true)
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("late sibling Ensure: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("late sibling Ensure blocked")
			}
			slot := collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}
			sibling.vectorBufferedSearchMu.Lock()
			entry := sibling.vectorBufferedSearch[slot]
			sibling.vectorBufferedSearchMu.Unlock()
			if fold {
				if entry != nil || !keeper.closed || resources.accounting != nil || resources.pin != nil || resources.ref != nil {
					t.Fatal("late first warm retained an obsolete keeper after nonempty fold")
				}
				accounting.Lock()
				exact := accounting.baseOwners == ownersBefore-1 && accounting.baseAssetBytes == bytesBefore-resources.assetBytes && accounting.baseDescriptorBytes == descriptorsBefore-resources.descriptorBytes && accounting.baseBackingBytes == backingBefore-resources.backingBytes
				accounting.Unlock()
				if !exact {
					t.Fatal("late cleanup did not release exactly its own accounted keeper")
				}
			} else if entry == nil || entry.prepared != keeper || keeper.closed || resources.ref == nil {
				t.Fatal("suffix-only advance discarded its still-current keeper")
			}
			after, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(after.Results) != 1 || !bytes.Equal(after.Results[0].Document, oldDocument) {
				t.Fatalf("independent old reader changed: %+v error=%v", after.Results, err)
			}
			if err := sibling.EnsureColumnGraphServing(ctx, index, opts); err != nil {
				t.Fatal(err)
			}
			query.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "new"}
			var latestBuffer VectorIndexSearchBuffer
			latest, view, err := sibling.SearchVectorIndexWithBufferReadView(query, &latestBuffer)
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			docs, err := view.FetchDocumentsForVectorIndexSearchResults(latest.Results, DocumentFetchOptions{})
			if err != nil || len(docs.Results) != 1 || !bytes.Equal(docs.Results[0].ID, ids[0]) || !bytes.Contains(docs.Results[0].Document, []byte("late-sibling-update")) {
				t.Fatalf("current owner=%+v error=%v", docs.Results, err)
			}
		})
	}
}
