package documentservice

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// The ordinary Dense64/v2 handler calls this same native service method. This
// fixture measures service owner/filter/search/full-fetch work, excluding wire
// encoding and the client. It is not the frozen Minima dataset or qualification.
func typedFilterReuseFixture(t testing.TB, rows int) (*Service, [8]DenseVectorSearchRequest) {
	t.Helper()
	requireTypedServiceServingTest(t)
	svc, db := newTestService(t)
	t.Cleanup(func() { _ = svc.Close(); _ = db.Close() })
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "filter-reuse", Dimension: 8, Metric: MetricCosine, TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 16, EfConstruction: 128},
		ScalarFields:       []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(19, 61))
	input := TypedDocumentsRequest{ExpectedGeneration: info.Generation, IDs: make([][]byte, rows), Retained: make([][]byte, rows),
		Columns: []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, rows)}, {Name: "content", Strings: make([]string, rows)}, {Name: "meta.user_id", Strings: make([]string, rows)}},
	}
	for i := range rows {
		input.IDs[i] = []byte(fmt.Sprintf("doc-%05d", i))
		input.Retained[i] = []byte(fmt.Sprintf(`{"id":%q,"meta":{"residual":"kept"}}`, input.IDs[i]))
		input.Columns[0].Float32Vectors[i] = make([]float32, 8)
		for d := range 8 {
			input.Columns[0].Float32Vectors[i][d] = random.Float32() - 0.5
		}
		input.Columns[1].Strings[i] = "full requested content"
		input.Columns[2].Strings[i] = fmt.Sprintf("user-%d", i%8)
	}
	if result, err := svc.UpsertTypedDocuments(ctx, info.Name, input); err != nil || result.Inserted != rows {
		t.Fatalf("initial upsert=%+v err=%v", result, err)
	}
	serving := typedServiceTestOptions()
	serving.Owners.StateBytes, serving.Owners.AssetBytes = 256<<20, 256<<20
	serving.Filter.SourceIDs, serving.Filter.InspectedEntries = rows+512, 2*rows+512
	serving.Filter.MappingWork = 8 << 20
	serving.SearchCandidates = 8192
	if _, err := svc.OptimizeIndex(ctx, info.Name, OptimizeIndexRequest{ColumnGraphAction: "build", ColumnGraphServing: &serving}); err != nil {
		t.Fatal(err)
	}
	var queries [8]DenseVectorSearchRequest
	for i := range queries {
		queries[i] = DenseVectorSearchRequest{ExpectedGeneration: info.Generation, QueryEmbedding: input.Columns[0].Float32Vectors[19%rows], TopK: 5, Route: RouteAnn, EfSearch: 256, ReturnEmbedding: true,
			Filter: &Filter{Field: "meta.user_id", Operator: "==", Value: fmt.Sprintf("user-%d", i)},
		}
	}
	return svc, queries
}

func TestServiceTypedFilterReuseEightPredicates(t *testing.T) {
	svc, queries := typedFilterReuseFixture(t, 128)
	ctx := context.Background()
	var expected [8][]RawDenseVectorResult
	for i, query := range queries {
		out, err := svc.SearchDenseVectorNativeRawInto(ctx, "filter-reuse", query, nil)
		if err != nil || len(out.Results) != 5 || out.DenseWork == nil || !out.DenseWork.Completed || out.DenseWork.Graph.Filter.InspectedEntries == 0 {
			t.Fatalf("cold predicate %d: work=%+v err=%v", i, out.DenseWork, err)
		}
		expected[i] = out.Results
	}
	var wg sync.WaitGroup
	for reader := range 4 {
		wg.Go(func() {
			var dst []RawDenseVectorResult
			// Frozen workload schedule shape: four readers, each alternating
			// between two of eight predicates by ordinal modulo eight.
			for ordinal := reader; ordinal < 64; ordinal += 4 {
				i := ordinal % 8
				out, err := svc.SearchDenseVectorNativeRawInto(ctx, "filter-reuse", queries[i], dst)
				if err != nil || !reflect.DeepEqual(out.Results, expected[i]) {
					t.Errorf("reader=%d predicate=%d parity err=%v", reader, i, err)
					return
				}
				if out.DenseWork == nil || !out.DenseWork.Completed || !out.DenseWork.Graph.Filter.Completed || out.DenseWork.Graph.Filter.InspectedEntries != 0 || out.DenseWork.Graph.Filter.SourceIDs != 0 {
					t.Errorf("repeated predicate %d rescanned immutable base: %+v", i, out.DenseWork)
					return
				}
				dst = out.Results[:0]
			}
		})
	}
	wg.Wait()
}

func BenchmarkServiceTypedFilterReuse(b *testing.B) {
	svc, queries := typedFilterReuseFixture(b, 50000)
	ctx := context.Background()
	for _, query := range queries {
		out, err := svc.SearchDenseVectorNativeRawInto(ctx, "filter-reuse", query, nil)
		if err != nil || len(out.Results) != 5 || out.DenseWork == nil || !out.DenseWork.Completed || !out.DenseWork.Graph.Filter.Completed || out.DenseWork.Graph.Filter.SourceIDs != 6250 || !out.DenseWork.Output.Completed || out.DenseWork.Output.Fetched != 5 || out.DenseWork.Output.Missing != 0 {
			b.Fatalf("cold native service filter/fetch: work=%+v err=%v", out.DenseWork, err)
		}
	}
	var totals [4]struct{ sourceIDs, inspected, scored uint64 }
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for reader := range 4 {
		wg.Go(func() {
			var dst []RawDenseVectorResult
			for ordinal := reader; ordinal < b.N; ordinal += 4 {
				out, err := svc.SearchDenseVectorNativeRawInto(ctx, "filter-reuse", queries[ordinal%8], dst)
				if err != nil || len(out.Results) != 5 || out.DenseWork == nil || !out.DenseWork.Completed || !out.DenseWork.Graph.Filter.Completed || out.DenseWork.Graph.Filter.EligibleRows != 6250 || out.DenseWork.Graph.Route != "typed_hnsw" || out.DenseWork.Graph.BaseANNScored == 0 || !out.DenseWork.Output.Completed || out.DenseWork.Output.Fetched != 5 || out.DenseWork.Output.Missing != 0 {
					b.Errorf("native service search: results=%d err=%v", len(out.Results), err)
					return
				}
				work := out.DenseWork.Graph
				totals[reader].sourceIDs += work.Filter.SourceIDs
				totals[reader].inspected += work.Filter.InspectedEntries
				totals[reader].scored += work.BaseANNScored + work.ExactBaseScored + work.DeltaScored
				dst = out.Results[:0]
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	var sourceIDs, inspected, scored uint64
	for _, v := range totals {
		sourceIDs += v.sourceIDs
		inspected += v.inspected
		scored += v.scored
	}
	b.ReportMetric(float64(sourceIDs)/float64(b.N), "filter-source-IDs/op")
	b.ReportMetric(float64(inspected)/float64(b.N), "filter-inspected/op")
	b.ReportMetric(float64(scored)/float64(b.N), "scored/op")
}
