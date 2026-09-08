package collections

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestTypedGraphPublicEmptyLifecycle(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	for _, degree := range []int{2, 16} {
		t.Run(fmt.Sprintf("M%d", degree), func(t *testing.T) {
			meta := typedMinimaCollectionMeta()
			meta.VectorIndexes[0].M = degree
			dir, db, col := openTypedMinimaCollectionMeta(t, meta)
			defer func() { _ = db.Close() }()
			var canonical atomic.Uint64
			restore := setColumnVectorGraphCanonicalRowsTestHook(func() { canonical.Add(1) })
			defer restore()
			ctx := context.Background()
			opts := typedGraphPublicTestOptions()
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			if err := col.EnsureColumnGraphServing(ctx, "embedding_graph", opts); err != nil {
				t.Fatalf("empty ensure: %v", err)
			}
			account := &col.collectionSchemaCoordinator().typedGraphOwners
			account.Lock()
			retainedEmpty := account.baseOwners != 0 || account.baseAssetBytes != 0 || account.baseDescriptorBytes != 0 || account.baseBackingBytes != 0
			account.Unlock()
			if retainedEmpty {
				t.Fatal("empty ensure retained a prepared holder")
			}
			query := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 4, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
			check := func(want string) {
				t.Helper()
				for _, filtered := range []bool{false, true} {
					q := query
					if filtered {
						q.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "source"}
					}
					var buffer VectorIndexSearchBuffer
					beforeGraph := workstats.Read().Graph
					response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
					if err != nil {
						t.Fatal(err)
					}
					docs, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
					closeErr := view.Close()
					if err != nil || closeErr != nil {
						t.Fatalf("fetch=%v close=%v", err, closeErr)
					}
					count := 0
					if want != "" {
						count = 1
					}
					work := response.Stats.ColumnGraphWork
					afterGraph := workstats.Read().Graph
					if !work.Available || afterGraph.Requests.Attempts-beforeGraph.Requests.Attempts != 1 || afterGraph.Requests.Completed-beforeGraph.Requests.Completed != 1 || afterGraph.Requests.Errors != beforeGraph.Requests.Errors || afterGraph.DeltaScored-beforeGraph.DeltaScored != work.DeltaScored || afterGraph.ExactBaseScored-beforeGraph.ExactBaseScored != work.ExactBaseScored || afterGraph.BaseANNScored-beforeGraph.BaseANNScored != work.BaseANNScored {
						t.Fatalf("public work=%+v before=%+v after=%+v", work, beforeGraph, afterGraph)
					}
					if count == 0 && (work.Route != "typed_empty" || work.BaseANNScored+work.ExactBaseScored+work.DeltaScored != 0) {
						t.Fatalf("empty proof=%+v", work)
					}
					if count == 1 && work.BaseANNScored+work.ExactBaseScored+work.DeltaScored == 0 {
						t.Fatalf("nonempty proof=%+v", work)
					}
					if filtered && (!work.Filter.Attempted || !work.Filter.Completed || work.Filter.EligibleRows != uint64(count)) {
						t.Fatalf("filter proof=%+v", work.Filter)
					}
					if count == 0 && response.Stats.SearchRouteHNSWSearchPack != 0 {
						t.Fatalf("empty query reported executed HNSW: %+v", response.Stats)
					}
					if len(docs.Results) != count {
						t.Fatalf("count=%d want%d", len(docs.Results), count)
					}
					if count == 1 && (!bytes.Equal(docs.Results[0].ID, []byte("one")) || !bytes.Contains(docs.Results[0].Document, []byte(want))) {
						t.Fatalf("wrong document:%+v", docs.Results)
					}
				}
			}
			insert := func(content string) {
				t.Helper()
				columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{query.Query}}, {Name: "content", Strings: []string{content}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
				if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("one")}, [][]byte{[]byte(`{"id":"one"}`)}, columns); err != nil {
					t.Fatal(err)
				}
			}
			check("")
			insert("original")
			check("original")
			for _, noMatch := range []bool{false, true} {
				q := query
				if noMatch {
					q.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "missing"}
				} else {
					q.TopK = 0
				}
				var buffer VectorIndexSearchBuffer
				response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
				if err != nil {
					t.Fatal(err)
				}
				if len(response.Results) != 0 || response.Stats.ColumnGraphWork.Route != "typed_empty" || response.Stats.ColumnGraphWork.DeltaScored != 0 {
					t.Fatalf("empty branch=%+v", response.Stats.ColumnGraphWork)
				}
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := col.FoldColumnGraphServing(ctx, "embedding_graph"); err != nil {
				t.Fatal(err)
			}
			var heldBuffer VectorIndexSearchBuffer
			heldResult, held, err := col.SearchVectorIndexWithBufferReadView(query, &heldBuffer)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			if err := col.Delete([]byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := col.FoldColumnGraphServing(ctx, "embedding_graph"); err != nil {
				t.Fatalf("fold empty: %v", err)
			}
			check("")
			old, err := held.FetchDocumentsForVectorIndexSearchResults(heldResult.Results, DocumentFetchOptions{})
			if err != nil || len(old.Results) != 1 || !bytes.Equal(old.Results[0].ID, []byte("one")) || !bytes.Contains(old.Results[0].Document, []byte("original")) {
				t.Fatalf("held:%+v err=%v", old, err)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			account.Lock()
			retainedEmpty = account.baseOwners != 0 || account.baseAssetBytes != 0 || account.baseDescriptorBytes != 0 || account.baseBackingBytes != 0
			account.Unlock()
			if retainedEmpty {
				t.Fatal("empty fold retained an obsolete prepared holder")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTypedMinimaDB(t, dir)
			col, err = NewCollectionManager(db).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			if err := col.EnsureColumnGraphServing(ctx, "embedding_graph", opts); err != nil {
				t.Fatalf("reopen empty ensure:%v", err)
			}
			check("")
			insert("reinserted")
			check("reinserted")
			if err := col.FoldColumnGraphServing(ctx, "embedding_graph"); err != nil {
				t.Fatal(err)
			}
			check("reinserted")
			if canonical.Load() != 0 {
				t.Fatalf("canonical row reconstruction=%d", canonical.Load())
			}
		})
	}
}

func TestTypedGraphPublicEmptyFoldLateKeeper(t *testing.T) {
	testTypedGraphPublicEmptyLateKeeper(t, false)
}

func TestTypedGraphPublicEmptyEnsureLateKeeper(t *testing.T) {
	testTypedGraphPublicEmptyLateKeeper(t, true)
}

func testTypedGraphPublicEmptyLateKeeper(t *testing.T, ensure bool) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer col.db.Close()
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const index = "embedding_graph"
	if err := col.EnsureColumnGraphServing(ctx, index, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	if ensure {
		if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
			t.Fatal(err)
		}
	}
	captured, release := make(chan struct{}, 1), make(chan struct{})
	var paused, released atomic.Bool
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
	collectionVectorIndexPreparedSearchBuildHookForTest.afterBuild = func(p *collectionVectorIndexPreparedSearch) {
		if p != nil && p.collection == col && paused.CompareAndSwap(false, true) {
			captured <- struct{}{}
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
	go func() {
		if ensure {
			done <- col.EnsureColumnGraphServing(ctx, index, typedGraphPublicTestOptions())
		} else {
			done <- col.FoldColumnGraphServing(ctx, index)
		}
	}()
	select {
	case <-captured:
	case <-time.After(10 * time.Second):
		t.Fatal("nonempty setup did not capture a keeper")
	}
	if _, err := col.DeleteBatch(ids); err != nil {
		t.Fatal(err)
	}
	if err := col.FoldColumnGraphServing(ctx, index); err != nil {
		t.Fatalf("empty fold: %v", err)
	}
	released.Store(true)
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("late nonempty setup: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("late nonempty setup blocked")
	}
	a := &col.collectionSchemaCoordinator().typedGraphOwners
	a.Lock()
	defer a.Unlock()
	if a.baseOwners != 0 || a.baseAssetBytes != 0 || a.baseDescriptorBytes != 0 || a.baseBackingBytes != 0 {
		t.Fatal("late setup installed an obsolete keeper after empty cutover")
	}
}
