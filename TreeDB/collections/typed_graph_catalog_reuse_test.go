package collections

import (
	"context"
	"maps"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestTypedGraphReadOwnerReusesExactLocalCatalog(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	fixture := openTypedGraphVectorReadViewBenchFixture(t, 8, 128, VectorIndexRepresentationCosineNormalizedF32V1)
	defer fixture.close()
	col := fixture.col
	limits := typedGraphPublicTestOptions().Owners
	warm, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}
	var loads atomic.Int64
	restore := setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
		if ctx.Collection == col.Name() && ctx.Stage == collectionCatalogLoadFaultMeta {
			loads.Add(1)
		}
		return nil
	})
	defer restore()
	for range 3 {
		owner, err := col.openTypedGraphReadOwner(limits)
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
		IndexName: "embedding_graph", Query: fixture.vectors[0], TopK: 10, EfSearch: 64,
		QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: typedGraphVectorReadViewBenchQuantizedName,
		QuantizedRerankCandidates: 64, StatsMode: VectorIndexSearchStatsModeProduction,
	}, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	fetched, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
	if err != nil || len(fetched.Results) != 10 || fetched.Stats.DocumentsFetched != 10 || fetched.Stats.DocumentsMissing != 0 {
		t.Fatalf("public full fetch: documents=%d err=%v", len(fetched.Results), err)
	}
	if fetched.Stats.TypedColumnRows != 0 {
		t.Fatal("vector-off full fetch read the embedding column")
	}
	if got := loads.Load(); got != 0 {
		t.Fatalf("warmed owners/public SQ8 search persistently reloaded catalog %d times; want 0", got)
	}
	if proof := response.Stats.ColumnGraphWork.ScorePlane; proof.PackedScoreBatchCalls != 1 || proof.ForbiddenStableScoreCalls != 0 {
		t.Fatalf("public SQ8 route changed packed rerank: %+v", proof)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	accounting := &col.collectionSchemaCoordinator().typedGraphOwners
	accounting.Lock()
	owners, states := accounting.owners, len(accounting.states)
	accounting.Unlock()
	if owners != 0 || states != 0 {
		t.Fatalf("metadata reuse retained closed request owners=%d states=%d", owners, states)
	}
}

func TestTypedGraphReadOwnerCatalogMatchesPublishers(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	check := func(stage string) {
		t.Helper()
		owner, err := col.openTypedGraphReadOwner(opts.Owners)
		if err != nil {
			t.Fatalf("%s: open owner: %v", stage, err)
		}
		defer owner.Close()
		snap := owner.overlay.base.snapshot
		direct, err := loadCollectionCatalog(snap, col.Name())
		if err != nil {
			t.Fatal(err)
		}
		col.catalogMu.RLock()
		cached := col.catalog
		exact := col.catalogSystemRoot == snapshotSystemRoot(snap) && col.catalogCommitSeq == snapshotCommitSeq(snap)
		col.catalogMu.RUnlock()
		if !exact || cached == nil || cached.pager != direct.pager || !collectionMetaValuesEqual(cached.meta, direct.meta) {
			t.Fatalf("%s: cached catalog identity/metadata differs from persistent load", stage)
		}
		assertTypedGraphCatalogRootsEqual(t, stage, cached, direct)
	}
	check("initial build")
	changed := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]},
		{Name: "content", Strings: []string{"changed"}},
		{Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}},
	}
	if _, err := col.UpsertTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	check("upsert")
	if err := col.Delete(ids[1]); err != nil {
		t.Fatal(err)
	}
	check("delete")
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); err != nil {
		t.Fatal(err)
	}
	check("fold")
	if _, err := col.RebuildVectorIndex(base.indexName); err != nil {
		t.Fatal(err)
	}
	check("rebuild")
}

func assertTypedGraphCatalogRootsEqual(t *testing.T, stage string, cached, direct *collectionCatalog) {
	t.Helper()
	if !maps.Equal(cached.roots, direct.roots) || !reflect.DeepEqual(cached.rootOverlays, direct.rootOverlays) {
		t.Fatalf("%s: cached current roots/overlays differ from persistent load", stage)
	}
	if cached.typedGraphBase == nil || direct.typedGraphBase == nil ||
		!collectionMetaValuesEqual(cached.typedGraphBase.meta, direct.typedGraphBase.meta) ||
		!maps.Equal(cached.typedGraphBase.roots, direct.typedGraphBase.roots) {
		t.Fatalf("%s: cached captured base metadata/roots differ from persistent load", stage)
	}
}

func TestTypedGraphReadOwnerRejectsStaleOrIncompleteLocalCatalog(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"cold", "pager", "system-root", "commit", "missing-base", "missing-base-roots", "base-schema"} {
		t.Run(kind, func(t *testing.T) {
			snap := col.db.AcquireSnapshot()
			catalog, err := loadCollectionCatalog(snap, col.Name())
			if err != nil {
				t.Fatal(err)
			}
			copy := *catalog
			col.rememberCatalog(snap, &copy)
			col.catalogMu.Lock()
			switch kind {
			case "cold":
				col.catalog = nil
			case "pager":
				copy.pager = nil
			case "system-root":
				col.catalogSystemRoot++
			case "commit":
				col.catalogCommitSeq++
			case "missing-base":
				copy.typedGraphBase = nil
			case "missing-base-roots", "base-schema":
				alias := *copy.typedGraphBase
				copy.typedGraphBase = &alias
				if kind == "missing-base-roots" {
					alias.roots = nil
				} else {
					alias.meta = copyCollectionMeta(alias.meta)
					alias.meta.VectorIndexes[0].Dimensions++
				}
			}
			col.catalogMu.Unlock()
			if err := snap.Close(); err != nil {
				t.Fatal(err)
			}
			var loads atomic.Int64
			restore := setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
				if ctx.Collection == col.Name() && ctx.Stage == collectionCatalogLoadFaultMeta {
					loads.Add(1)
				}
				return nil
			})
			defer restore()
			for i := range 2 {
				owner, err := col.openTypedGraphReadOwner(opts.Owners)
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				if loads.Load() != 1 {
					t.Fatalf("open %d loads=%d; want one cold repair then local reuse", i, loads.Load())
				}
			}
		})
	}
}
