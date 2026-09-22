package documentservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

func TestServiceColumnGraphCrossCollectionClosure(t *testing.T) {
	for _, other := range []bool{false, true} {
		t.Run(fmt.Sprintf("intervening_collection_%t", other), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.BackgroundIndexVacuumInterval = -1
			db, cleanup, _, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cleanup != nil {
					if err := cleanup(); err != nil {
						t.Errorf("close: %v", err)
					}
				}
			}()
			svc := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(db), maintenance)
			defer func() {
				if svc != nil {
					if err := svc.Close(); err != nil {
						t.Errorf("close service: %v", err)
					}
				}
			}()
			ctx := context.Background()
			for _, name := range []string{"a", "b"} {
				if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, Metric: MetricCosine}); err != nil {
					t.Fatal(err)
				}
			}
			put := func(name, id string) {
				t.Helper()
				if _, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: []Document{{ID: id, Content: name + "/" + id, Embedding: []float32{1, 0}}}}); err != nil {
					t.Fatalf("put %s/%s: %v cause=%v", name, id, err, errors.Unwrap(err))
				}
			}
			put("a", "first")
			col, err := svc.manager.OpenCollection("a")
			if err != nil {
				t.Fatal(err)
			}
			held, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			before, err := held.FetchDocumentsByID([][]byte{[]byte("first")}, collections.DocumentFetchOptions{})
			if err != nil || len(before.Results) != 1 || !before.Results[0].Found {
				t.Fatalf("initial held fetch=%+v err=%v", before, err)
			}
			if other {
				put("b", "other")
			}
			put("a", "second")
			after, err := held.FetchDocumentsByID([][]byte{[]byte("first"), []byte("second")}, collections.DocumentFetchOptions{})
			if err != nil || len(after.Results) != 2 || !after.Results[0].Found || after.Results[1].Found || !bytes.Equal(after.Results[0].Document, before.Results[0].Document) {
				t.Fatalf("held view changed: %+v err=%v", after, err)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			check := func(name string, ids ...string) {
				t.Helper()
				response, err := svc.SearchDenseVector(ctx, name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: len(ids), EfSearch: 8, ReturnEmbedding: true})
				if err != nil || len(response.Documents) != len(ids) {
					t.Fatalf("search %s=%+v err=%v", name, response, err)
				}
				want := make(map[string]bool)
				for _, id := range ids {
					want[id] = true
				}
				for _, doc := range response.Documents {
					if !want[doc.ID] || doc.Content != name+"/"+doc.ID || len(doc.Embedding) != 2 || doc.Embedding[0] != 1 || doc.Embedding[1] != 0 {
						t.Fatalf("incorrect full fetch: %+v", doc)
					}
					delete(want, doc.ID)
				}
			}
			check("a", "first", "second")
			if other {
				check("b", "other")
			}
			if err := svc.Close(); err != nil {
				t.Fatal(err)
			}
			svc = nil
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
			cleanup = nil
			db, cleanup, _, maintenance, err = treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
			if err != nil {
				t.Fatal(err)
			}
			svc = NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(db), maintenance)
			check("a", "first", "second")
			if other {
				check("b", "other")
			}
		})
	}
}
