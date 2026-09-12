package collections

import (
	"bytes"
	"context"
	"testing"
)

func TestTypedGraphFoldRetiresSiblingKeepers(t *testing.T) {
	for _, separateManager := range []bool{false, true} {
		name := "same_manager"
		if separateManager {
			name = "separate_manager"
		}
		t.Run(name, func(t *testing.T) {
			requireTypedGraphPublicServingTest(t)
			col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
			index := base.indexName
			if err := base.Close(); err != nil {
				t.Fatal(err)
			}
			opts := typedGraphPublicTestOptions()
			if err := col.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
				t.Fatal(err)
			}
			manager := col.manager
			if separateManager {
				manager = NewCollectionManager(col.db)
			}
			sibling, err := manager.OpenCollection(col.Name())
			if err != nil {
				t.Fatal(err)
			}
			if err := sibling.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
				t.Fatal(err)
			}
			slot := collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}
			sibling.vectorBufferedSearchMu.Lock()
			keeper := sibling.vectorBufferedSearch[slot].prepared
			sibling.vectorBufferedSearchMu.Unlock()
			query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
			var buffer VectorIndexSearchBuffer
			response, held, err := sibling.SearchVectorIndexWithBufferReadView(query, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			before, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(before.Results) != 1 {
				t.Fatalf("old fetch=%+v error=%v", before.Results, err)
			}
			oldDocument := bytes.Clone(before.Results[0].Document)
			changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"sibling-fold-update"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
			if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
				t.Fatal(err)
			}
			if err := col.FoldColumnGraphServing(context.Background(), index); err != nil {
				t.Fatal(err)
			}
			sibling.vectorBufferedSearchMu.Lock()
			entry := sibling.vectorBufferedSearch[slot]
			sibling.vectorBufferedSearchMu.Unlock()
			if entry != nil {
				t.Fatal("idle sibling retained its captured-base keeper after fold")
			}
			keeper.mu.RLock()
			closed := keeper.closed && keeper.capturedBase == nil
			keeper.mu.RUnlock()
			if !closed {
				t.Fatal("detached sibling keeper still owns captured resources")
			}
			after, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(after.Results) != 1 || !bytes.Equal(after.Results[0].Document, oldDocument) {
				t.Fatalf("independent old reader changed: %+v error=%v", after.Results, err)
			}
			if err := sibling.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
				t.Fatalf("rewarm sibling: %v", err)
			}
			query.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "new"}
			var currentBuffer VectorIndexSearchBuffer
			current, view, err := sibling.SearchVectorIndexWithBufferReadView(query, &currentBuffer)
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			documents, err := view.FetchDocumentsForVectorIndexSearchResults(current.Results, DocumentFetchOptions{})
			if err != nil || len(documents.Results) != 1 || !bytes.Contains(documents.Results[0].Document, []byte("sibling-fold-update")) {
				t.Fatalf("current sibling result=%+v error=%v", documents.Results, err)
			}
		})
	}
}
