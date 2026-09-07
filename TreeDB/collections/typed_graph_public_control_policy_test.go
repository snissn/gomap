package collections

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestTypedGraphPublicFoldControlRootPolicy(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	for _, policy := range []RootStoragePolicy{RootStorageDefault, RootStorageFast, RootStorageCompressed} {
		t.Run(fmt.Sprintf("policy_%s", policy), func(t *testing.T) {
			openOpts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			db, cleanup, _, _, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(openOpts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cleanup != nil {
					if err := cleanup(); err != nil {
						t.Errorf("wrapper close: %v", err)
					}
				}
			}()
			if db.Stats()["treedb.leaf_generation.enabled"] != "true" {
				t.Fatal("native leaf generation not enabled")
			}
			meta := typedMinimaCollectionMeta()
			meta.VectorIndexes[0].M = 16
			meta.Options.ColumnStore.ControlRootStoragePolicy = policy
			manager := NewCollectionManager(db)
			if _, err := manager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if col != nil {
					if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
						t.Errorf("prepared cache close: %v", err)
					}
				}
			}()
			const rows = 1024
			ids, retained := make([][]byte, rows), make([][]byte, rows)
			columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
			for i := range rows {
				ids[i] = []byte(fmt.Sprintf("row-%05d", i))
				retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
				columns[0].Float32Vectors = append(columns[0].Float32Vectors, vectorBenchmarkEmbedding(i, 8))
				columns[1].Strings = append(columns[1].Strings, "original")
				columns[2].Strings = append(columns[2].Strings, fmt.Sprint(i))
				columns[3].Strings = append(columns[3].Strings, "source")
			}
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
				t.Fatal(err)
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			opts := typedGraphPublicTestOptions()
			opts.Publication = ColumnGraphPublicationLimits{Rows: 4096, Tombstones: 4096, ValueSlots: 16384, OwnedBytes: 64 << 20, EncodedOutputBytes: 256 << 20}
			opts.Owners.StateBytes, opts.Owners.AssetBytes = 512<<20, 512<<20
			opts.Owners.Cold = ColumnGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 16 << 20, AssetBytes: 512 << 20, DecodedTermBytes: 512 << 20}
			opts.CandidateOutput = ColumnGraphCandidateOutputLimits{Bytes: 256 << 20, AppenderAttempts: 64}
			opts.Maintenance.NativeBytes, opts.Maintenance.ColumnBytes, opts.Maintenance.RetainedBytes = 512<<20, 256<<20, 512<<20
			opts.Maintenance.PagerPages = 1 << 20
			opts.FoldRows = rows * 2
			opts.Filter.SourceIDs, opts.Filter.InspectedEntries = rows*2, rows*2
			ctx := context.Background()
			if err := col.EnsureColumnGraphServing(ctx, "embedding_graph", opts); err != nil {
				t.Fatal(err)
			}
			q := VectorIndexSearchOptions{IndexName: "embedding_graph", Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "user", Value: "0"}}
			var buffer VectorIndexSearchBuffer
			response, held, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"before-fold"}}, {Name: "user", Strings: []string{"0"}}, {Name: "path", Strings: []string{"source"}}}
			if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
				t.Fatal(err)
			}
			if err := col.FoldColumnGraphServing(ctx, "embedding_graph"); err != nil {
				t.Fatalf("fold: %v", err)
			}
			snap := db.AcquireSnapshot()
			catalog, err := loadCollectionCatalog(snap, col.Name())
			if err != nil {
				snap.Close()
				t.Fatal(err)
			}
			t.Logf("policy=%q current=%v captured=%v", policy, catalog.roots, catalog.typedGraphBase.roots)
			snap.Close()
			changed[1].Strings[0] = "after-fold"
			if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
				t.Fatalf("post-fold mutation: %v", err)
			}
			old, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(old.Results) != 1 || !bytes.Equal(old.Results[0].ID, ids[0]) || !bytes.Contains(old.Results[0].Document, []byte("original")) {
				t.Fatalf("held=%+v error=%v", old, err)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				var b VectorIndexSearchBuffer
				r, view, err := col.SearchVectorIndexWithBufferReadView(q, &b)
				if err != nil {
					t.Fatal(err)
				}
				docs, err := view.FetchDocumentsForVectorIndexSearchResults(r.Results, DocumentFetchOptions{})
				closeErr := view.Close()
				if err != nil || closeErr != nil || len(docs.Results) != 1 || !bytes.Equal(docs.Results[0].ID, ids[0]) || !bytes.Contains(docs.Results[0].Document, []byte("after-fold")) {
					t.Fatalf("current=%+v error=%v close=%v", docs, err, closeErr)
				}
			}
			check()
			if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
				t.Fatal(err)
			}
			if err := cleanup(); err != nil {
				cleanup = nil
				t.Fatal(err)
			}
			cleanup = nil
			db, cleanup, _, _, err = treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(openOpts)
			if err != nil {
				t.Fatal(err)
			}
			col, err = NewCollectionManager(db).OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			if err := col.EnsureColumnGraphServing(ctx, "embedding_graph", opts); err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}
