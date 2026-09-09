package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestServiceTypedFreshUpsertConcurrentDenseAfterColumnGraphBuildAndVacuum(t *testing.T) {
	requireTypedServiceServingTest(t)

	errorChain := func(err error) string {
		var parts []string
		var walk func(error)
		walk = func(err error) {
			if err == nil {
				return
			}
			parts = append(parts, fmt.Sprintf("%T: %+v", err, err))
			if joined, ok := err.(interface{ Unwrap() []error }); ok {
				for _, child := range joined.Unwrap() {
					walk(child)
				}
				return
			}
			walk(errors.Unwrap(err))
		}
		walk(err)
		var ambiguous *collections.CommitAmbiguousError
		if errors.As(err, &ambiguous) {
			parts = append(parts, fmt.Sprintf("ambiguous operation=%q inner=%T: %+v", ambiguous.Operation, ambiguous.Err, ambiguous.Err))
		}
		return fmt.Sprintf("code=%s chain=[%s]", ErrorCodeOf(err), strings.Join(parts, " | "))
	}

	for repetition := range 3 {
		if !t.Run(fmt.Sprintf("fresh-%d", repetition), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			backend, cleanup, _, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
			if err != nil {
				t.Fatal(errorChain(err))
			}
			svc := NewWithDeferredVectorBuildMaintenance(collections.NewCollectionManager(backend), maintenance)
			t.Cleanup(func() {
				if err := svc.Close(); err != nil {
					t.Errorf("service close: %s", errorChain(err))
				}
				if err := cleanup(); err != nil {
					t.Errorf("backend close: %s", errorChain(err))
				}
			})

			create := CreateIndexRequest{Name: "typed-overlap", Dimension: 8, Metric: MetricCosine, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 16, EfConstruction: 128, EfSearch: 2048}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}, {Field: "meta.fpath", ValueType: ScalarFieldString}}}
			info, err := svc.CreateIndex(ctx, create)
			if err != nil {
				t.Fatal(errorChain(err))
			}
			makeRequest := func(start, count int) TypedDocumentsRequest {
				req := TypedDocumentsRequest{ExpectedGeneration: info.Generation, IDs: make([][]byte, count), Retained: make([][]byte, count), Columns: []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, count)}, {Name: "content", Strings: make([]string, count)}, {Name: "meta.user_id", Strings: make([]string, count)}, {Name: "meta.fpath", Strings: make([]string, count)}}}
				for i := range count {
					n := start + i
					id := fmt.Sprintf("doc-%03d", n)
					req.IDs[i] = []byte(id)
					req.Retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, id))
					req.Columns[0].Float32Vectors[i] = []float32{float32(n + 1), 1, 2, 3, 4, 5, 6, 7}
					req.Columns[1].Strings[i] = "content-" + id
					req.Columns[2].Strings[i] = fmt.Sprintf("user-%02d", n%8)
					req.Columns[3].Strings[i] = "/" + id
				}
				return req
			}
			if out, err := svc.UpsertTypedDocuments(ctx, create.Name, makeRequest(0, 112)); err != nil || out.Inserted != 112 {
				t.Fatalf("initial typed upsert=%+v err=%s", out, errorChain(err))
			}
			serving := typedServiceTestOptions()
			if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "build", ColumnGraphServing: &serving}); err != nil {
				t.Fatal(errorChain(err))
			}

			beforeVacuum := backend.Pager()
			if err := backend.VacuumIndexOnline(ctx); err != nil {
				t.Fatalf("actual vacuum: %s", errorChain(err))
			}
			if backend.Pager() == beforeVacuum {
				t.Fatal("vacuum did not replace the pager")
			}

			baseQuery := DenseVectorSearchRequest{ExpectedGeneration: info.Generation, QueryEmbedding: []float32{1, 1, 2, 3, 4, 5, 6, 7}, TopK: 5, Route: RouteAnn, EfSearch: 2048}
			for range 32 {
				out, err := svc.SearchDenseVectorNativeRawInto(ctx, create.Name, baseQuery, nil)
				if err != nil || out.DenseWork == nil || !out.DenseWork.Completed || !out.DenseWork.Graph.Completed {
					t.Fatalf("warmup=%+v err=%s", out.DenseWork, errorChain(err))
				}
			}

			filters := []Filter{
				{Field: "meta.fpath", Operator: "==", Value: "/doc-001"},
				{Field: "meta.user_id", Operator: "==", Value: "user-01"},
				{Field: "meta.user_id", Operator: "==", Value: "missing"},
				{Operator: "AND", Conditions: []Filter{{Field: "meta.user_id", Operator: "==", Value: "user-01"}, {Field: "meta.fpath", Operator: "==", Value: "/doc-001"}}},
			}
			fresh := makeRequest(112, 16)
			type result struct {
				reader int
				search RawDenseVectorSearchResponse
				upsert UpsertDocumentsResponse
				err    error
			}
			ready, start, results := make(chan struct{}, 5), make(chan struct{}), make(chan result, 5)
			go func() {
				ready <- struct{}{}
				<-start
				out, err := svc.UpsertTypedDocuments(ctx, create.Name, fresh)
				results <- result{reader: -1, upsert: out, err: err}
			}()
			for i := range filters {
				go func(i int) {
					ready <- struct{}{}
					<-start
					query := baseQuery
					query.Filter = &filters[i]
					out, err := svc.SearchDenseVectorNativeRawInto(ctx, create.Name, query, nil)
					results <- result{reader: i, search: out, err: err}
				}(i)
			}
			for range 5 {
				<-ready
			}
			close(start)
			done := ctx.Done()
			for received := 0; received < 5; {
				select {
				case got := <-results:
					received++
					if got.err != nil {
						t.Errorf("operation reader=%d: %s", got.reader, errorChain(got.err))
						continue
					}
					if got.reader < 0 {
						if got.upsert.Inserted != 16 {
							t.Errorf("fresh typed upsert=%+v", got.upsert)
						}
					} else {
						want := []int{1, 5, 0, 1}[got.reader]
						work := got.search.DenseWork
						if !got.search.TypedColumnGraph || work == nil || !work.Completed || !work.Graph.Completed || len(got.search.Results) != want {
							t.Errorf("reader %d result=%+v work=%+v", got.reader, got.search.Results, work)
						}
						for _, row := range got.search.Results {
							id := string(row.ID)
							n, err := strconv.Atoi(strings.TrimPrefix(id, "doc-"))
							if (got.reader == 1 && (err != nil || n < 0 || n >= 128 || n%8 != 1)) || (got.reader != 1 && id != "doc-001") {
								t.Errorf("reader %d returned nonmatching ID %q", got.reader, id)
							}
						}
					}
				case <-done:
					t.Errorf("concurrent operations timed out: %v", ctx.Err())
					done = nil
				}
			}

			if t.Failed() {
				return
			}

			visible := baseQuery
			visible.QueryEmbedding = []float32{128, 1, 2, 3, 4, 5, 6, 7}
			visible.Filter = &Filter{Field: "meta.fpath", Operator: "==", Value: "/doc-127"}
			out, err := svc.SearchDenseVectorNativeRawInto(ctx, create.Name, visible, nil)
			if err != nil {
				t.Fatalf("visibility: %s", errorChain(err))
			}
			if len(out.Results) != 1 || string(out.Results[0].ID) != "doc-127" {
				t.Fatalf("visibility results=%+v", out.Results)
			}
		}) {
			return
		}
	}
}

func TestServiceTypedPreparedHandleLifecycle(t *testing.T) {
	requireTypedServiceServingTest(t)
	ctx := context.Background()
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(collections.NewCollectionManager(db))
	_ = svc.DiagnosticsHandler(nil)
	defer func() { _ = svc.Close(); _ = db.Close() }()
	create := CreateIndexRequest{Name: "keeper", Dimension: 8, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}}}
	info, err := svc.CreateIndex(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	docs := []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "u", "residual": "keep"}}, {ID: "b", Content: "beta", Embedding: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "v"}}}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	options := typedServiceTestOptions()
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	cached := func() *collections.Collection {
		t.Helper()
		svc.benchmarkSearchCacheMu.RLock()
		entry := svc.benchmarkSearchCache[create.Name]
		svc.benchmarkSearchCacheMu.RUnlock()
		if entry == nil || entry.collection == nil {
			t.Fatal("successful explicit setup did not retain its service collection")
		}
		return entry.collection
	}
	handle := cached()
	beforeState, ok := handle.ColumnGraphServingSnapshot()
	if !ok || !beforeState.ServingReady || beforeState.BaseRows != 2 || beforeState.BaseCoverageLSN == 0 {
		t.Fatalf("serving snapshot=%+v available=%v", beforeState, ok)
	}
	beforeWork := workstats.Read()
	for range 3 {
		snapshot := svc.DiagnosticsSnapshot(nil)
		if snapshot.LastOpened == nil || snapshot.LastOpened.TypedGraph == nil || !reflect.DeepEqual(*snapshot.LastOpened.TypedGraph, beforeState) {
			t.Fatalf("cached diagnostics=%+v state=%+v", snapshot.LastOpened, beforeState)
		}
	}
	afterWork := workstats.Read()
	afterState, _ := handle.ColumnGraphServingSnapshot()
	if !reflect.DeepEqual(beforeState, afterState) || beforeWork.Graph.Requests != afterWork.Graph.Requests || beforeWork.Fold != afterWork.Fold || beforeWork.RowIndexCache.Builds != afterWork.RowIndexCache.Builds {
		t.Fatal("diagnostics changed serving state or started work")
	}
	query := DenseVectorSearchRequest{QueryEmbedding: docs[0].Embedding, TopK: 1, EfSearch: 8, ReturnEmbedding: true, ExpectedGeneration: info.Generation}
	check := func(content string) {
		t.Helper()
		for range 2 {
			var httpOut DenseVectorSearchResponse
			postJSON(t, NewHandler(svc), "/v1/indexes/keeper/search/vector", query, http.StatusOK, &httpOut)
			if httpOut.Route != RouteAnn || len(httpOut.Documents) != 1 {
				t.Fatalf("HTTP=%+v", httpOut)
			}
			beforeOutput := workstats.Output.Search.Read()
			raw, err := svc.SearchDenseVectorNativeRaw(ctx, create.Name, query)
			if err != nil || !raw.TypedColumnGraph || raw.Route != RouteAnn || len(raw.Results) != 1 {
				t.Fatalf("native=%+v err=%v", raw, err)
			}
			for _, proof := range []*DenseSearchWork{httpOut.DenseWork, raw.DenseWork} {
				if proof == nil || !proof.Completed || !proof.Graph.Completed || !proof.Graph.Snapshot.Available || proof.Graph.Snapshot.SchemaGeneration != info.Generation || proof.Graph.Route != "typed_hnsw" || proof.Graph.BaseANNScored == 0 || !proof.Output.Completed || proof.Output.Fetched != 1 || proof.Output.OutputBytes != uint64(len(raw.Results[0].Document)) {
					t.Fatalf("public owned work=%+v", proof)
				}
			}
			if *httpOut.DenseWork != *raw.DenseWork {
				t.Fatalf("HTTP and native proof differ: %+v / %+v", httpOut.DenseWork, raw.DenseWork)
			}
			afterOutput := workstats.Output.Search.Read()
			if !raw.searchStats.ColumnGraphWork.Available || afterOutput.Attempts-beforeOutput.Attempts != 1 || afterOutput.Completed-beforeOutput.Completed != 1 || afterOutput.Fetched-beforeOutput.Fetched != 1 || afterOutput.OutputBytes-beforeOutput.OutputBytes != uint64(len(raw.Results[0].Document)) {
				t.Fatalf("search output before=%+v after=%+v", beforeOutput, afterOutput)
			}
			var nativeDoc Document
			if err := json.Unmarshal(raw.Results[0].Document, &nativeDoc); err != nil {
				t.Fatal(err)
			}
			for _, doc := range []Document{httpOut.Documents[0], nativeDoc} {
				if doc.ID != "a" || doc.Content != content || doc.Meta["residual"] != "keep" || !reflect.DeepEqual(doc.Embedding, docs[0].Embedding) {
					t.Fatalf("full payload=%+v", doc)
				}
			}
			if cached() != handle {
				t.Fatal("ordinary query replaced the explicit setup handle")
			}
		}
	}
	check("alpha")
	docs[0].Content = "updated"
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs[:1]}); err != nil {
		t.Fatal(err)
	}
	check("updated")
	if _, err := svc.DeleteDocuments(ctx, create.Name, DeleteDocumentsRequest{IDs: []string{"b"}}); err != nil {
		t.Fatal(err)
	}
	check("updated")
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "fold"}); err != nil {
		t.Fatal(err)
	}
	check("updated")
	if err := svc.invalidateBenchmarkSearchCache(create.Name); err != nil {
		t.Fatal(err)
	}
	if svc.benchmarkSearchCacheSizeForTest() != 0 {
		t.Fatal("explicit invalidation retained service handle")
	}
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "ensure", ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	next := cached()
	if next == handle {
		t.Fatal("invalidation did not replace old handle")
	}
	handle = next
	check("updated")
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if svc.benchmarkSearchCacheSizeForTest() != 0 {
		t.Fatal("Close retained service handle")
	}
	svc = New(collections.NewCollectionManager(db))
	create.ColumnGraphServing = &options
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatal(err)
	}
	handle = cached()
	check("updated")
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	svc = New(collections.NewCollectionManager(db))
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "ensure", ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	handle = cached()
	check("updated")
}
