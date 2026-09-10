package documentservice

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// The ordinary Dense64/v2 handler calls this same native service method. This
// fixture measures service owner/filter/search/full-fetch work, excluding wire
// encoding and the client. It is not the frozen Minima dataset or qualification.
func typedFilterReuseFixture(t testing.TB, rows, batchSize int) (*Service, [8]DenseVectorSearchRequest) {
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
	for start := 0; start < rows; start += batchSize {
		end := min(rows, start+batchSize)
		part := input
		part.IDs, part.Retained = input.IDs[start:end], input.Retained[start:end]
		part.Columns = make([]collections.TypedColumnBatch, len(input.Columns))
		for i, column := range input.Columns {
			part.Columns[i] = column
			if column.Float32Vectors != nil {
				part.Columns[i].Float32Vectors = column.Float32Vectors[start:end]
			} else {
				part.Columns[i].Strings = column.Strings[start:end]
			}
		}
		if result, err := svc.UpsertTypedDocuments(ctx, info.Name, part); err != nil || result.Inserted != end-start {
			t.Fatalf("initial upsert=%+v err=%v", result, err)
		}
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
	svc, queries := typedFilterReuseFixture(t, 128, 128)
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
	benchmarkServiceTypedFilterReuse(b, 50000, false)
}

func BenchmarkServiceTypedCurrentMaterializer(b *testing.B) {
	b.Run("base", func(b *testing.B) { benchmarkServiceTypedFilterReuse(b, 256, false) })
	b.Run("suffix", func(b *testing.B) { benchmarkServiceTypedFilterReuse(b, 256, true) })
}

func benchmarkServiceTypedFilterReuse(b *testing.B, batchSize int, suffix bool) {
	svc, queries := typedFilterReuseFixture(b, 50000, batchSize)
	ctx := context.Background()
	var expected [8][]RawDenseVectorResult
	for i, query := range queries {
		out, err := svc.SearchDenseVectorNativeRawInto(ctx, "filter-reuse", query, nil)
		if err != nil || len(out.Results) != 5 || out.DenseWork == nil || !out.DenseWork.Completed || !out.DenseWork.Graph.Filter.Completed || out.DenseWork.Graph.Filter.SourceIDs != 6250 || !out.DenseWork.Output.Completed || out.DenseWork.Output.Fetched != 5 || out.DenseWork.Output.Missing != 0 {
			b.Fatalf("cold native service filter/fetch: work=%+v err=%v", out.DenseWork, err)
		}
		if suffix {
			expected[i] = out.Results
		}
	}
	if suffix {
		in := TypedDocumentsRequest{ExpectedGeneration: queries[0].ExpectedGeneration,
			IDs: [][]byte{[]byte("doc-00019")}, Retained: [][]byte{[]byte(`{"id":"doc-00019","meta":{"residual":"changed"}}`)},
			Columns: []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{queries[0].QueryEmbedding}}, {Name: "content", Strings: []string{"changed full content"}}, {Name: "meta.user_id", Strings: []string{"user-3"}}},
		}
		if out, err := svc.UpsertTypedDocuments(ctx, "filter-reuse", in); err != nil || out.Updated != 1 {
			b.Fatalf("suffix replacement=%+v err=%v", out, err)
		}
		sawChanged := false
		for i, query := range queries {
			out, err := svc.SearchDenseVectorNativeRawInto(ctx, "filter-reuse", query, nil)
			if err != nil || len(out.Results) != len(expected[i]) {
				b.Fatalf("suffix parity query=%d err=%v", i, err)
			}
			for j, got := range out.Results {
				want := expected[i][j]
				if !bytes.Equal(got.ID, want.ID) || math.Abs(got.Score-want.Score) > 1e-6 {
					b.Fatalf("suffix changed ranking query=%d result=%d", i, j)
				}
				if bytes.Equal(got.ID, in.IDs[0]) {
					sawChanged = true
					if !bytes.Contains(got.Document, []byte("changed full content")) || !bytes.Contains(got.Document, []byte(`"residual":"changed"`)) {
						b.Fatal("suffix full fetch lost replacement payload")
					}
				} else if !bytes.Equal(got.Document, want.Document) {
					b.Fatal("suffix changed another document's full payload")
				}
			}
		}
		if !sawChanged {
			b.Fatal("suffix parity did not fetch the replacement document")
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

// Every operation replaces one real row, including durable publication and
// serving metadata preparation. Initial load/Build and request storage are setup.
func BenchmarkServiceTypedCurrentMaterializerPublish(b *testing.B) {
	if b.N > 128 {
		b.Skip("bounded serving epoch: use -benchtime=64x")
	}
	svc, queries := typedFilterReuseFixture(b, 50000, 256)
	in := TypedDocumentsRequest{ExpectedGeneration: queries[0].ExpectedGeneration,
		IDs: [][]byte{[]byte("doc-00019")}, Retained: [][]byte{[]byte(`{"id":"doc-00019","meta":{"residual":"kept"}}`)},
		Columns: []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{queries[0].QueryEmbedding}}, {Name: "content", Strings: []string{"changed"}}, {Name: "meta.user_id", Strings: []string{"user-3"}}},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		in.Columns[1].Strings[0] = []string{"changed a", "changed b"}[i%2]
		if out, err := svc.UpsertTypedDocuments(ctx, "filter-reuse", in); err != nil || out.Updated != 1 {
			b.Fatalf("replacement=%+v err=%v", out, err)
		}
	}
}

// One operation is all eight cold predicates, including preparation and full
// fetch. Keeper retirement/Ensure are setup outside the measured cohort.
func BenchmarkServiceTypedFilterReuseCold(b *testing.B) {
	svc, queries := typedFilterReuseFixture(b, 50000, 256)
	ctx := context.Background()
	col, _, err := svc.openIndex(ctx, "filter-reuse", 0)
	if err != nil {
		b.Fatal(err)
	}
	serving := typedServiceTestOptions()
	serving.Owners.StateBytes, serving.Owners.AssetBytes = 256<<20, 256<<20
	serving.Filter.SourceIDs, serving.Filter.InspectedEntries = 50000+512, 100000+512
	serving.Filter.MappingWork = 8 << 20
	serving.SearchCandidates = 8192
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
			b.Fatal(err)
		}
		if _, err := svc.OptimizeIndex(ctx, "filter-reuse", OptimizeIndexRequest{ColumnGraphAction: "ensure", ColumnGraphServing: &serving}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for _, query := range queries {
			out, err := svc.SearchDenseVectorNativeRawInto(ctx, "filter-reuse", query, nil)
			if err != nil || len(out.Results) != 5 || out.DenseWork == nil || !out.DenseWork.Completed || out.DenseWork.Graph.Filter.SourceIDs != 6250 || out.DenseWork.Output.Fetched != 5 {
				b.Fatalf("cold native service cohort: work=%+v err=%v", out.DenseWork, err)
			}
		}
	}
	b.ReportMetric(8, "requests/op")
}
