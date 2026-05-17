package collections

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

var columnVectorGraphBenchSink int64

func TestColumnVectorGraphSearchMatchesExactCompleteGraph(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(32, 16, 0, true))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	query, ok := graph.VectorAt(nil, 17)
	if !ok {
		t.Fatal("missing query vector")
	}
	var scratch ColumnVectorGraphSearchScratch
	results, trace, err := graph.SearchCosine(query, ColumnVectorGraphSearchOptions{TopK: 7, EfSearch: 32}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if trace.ReturnedCount != 7 || trace.CandidatesExamined != graph.Rows() {
		t.Fatalf("trace=%+v want returned=7 candidates=%d", trace, graph.Rows())
	}
	exact := exactColumnVectorGraphCosine(t, graph, query, 7)
	if len(results) != len(exact) {
		t.Fatalf("results=%d exact=%d", len(results), len(exact))
	}
	for i := range results {
		if !bytes.Equal(results[i].DocumentID, exact[i].DocumentID) {
			t.Fatalf("result %d document ID=%q want %q", i, results[i].DocumentID, exact[i].DocumentID)
		}
		if math.Abs(float64(results[i].Distance-exact[i].Distance)) > 1e-6 {
			t.Fatalf("result %d distance=%g want %g", i, results[i].Distance, exact[i].Distance)
		}
	}
}

func TestColumnVectorGraphRejectsStaleInvNorm(t *testing.T) {
	columns := columnVectorGraphTestColumns(8, 4, 2, false)
	columns.InvNorms[3] *= 1.25
	_, err := NewColumnVectorGraphFromColumns(columns)
	if err == nil {
		t.Fatal("NewColumnVectorGraphFromColumns succeeded with stale inverse norm")
	}
	if !strings.Contains(err.Error(), "inverse norm") {
		t.Fatalf("error=%q want inverse norm validation", err)
	}
}

func TestColumnVectorGraphRejectsLooseTinyInvNorm(t *testing.T) {
	_, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-large")},
		Vectors:         []float32{1e7},
		InvNorms:        []float32{1e-8},
		NeighborOffsets: []uint32{0, 0},
		Dimensions:      1,
		EntryPoint:      0,
	})
	if err == nil {
		t.Fatal("NewColumnVectorGraphFromColumns succeeded with materially wrong tiny inverse norm")
	}
	if !strings.Contains(err.Error(), "inverse norm") {
		t.Fatalf("error=%q want inverse norm validation", err)
	}
}

func TestColumnVectorGraphZeroValueRows(t *testing.T) {
	var graph ColumnVectorGraph
	if got := graph.Rows(); got != 0 {
		t.Fatalf("zero-value Rows()=%d want 0", got)
	}
}

func TestColumnVectorGraphRejectsUnrepresentableQueryInvNorm(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(4, 4, 2, false))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	query := []float32{math.SmallestNonzeroFloat32, 0, 0, 0}
	_, _, err = graph.SearchCosine(query, ColumnVectorGraphSearchOptions{TopK: 1}, &ColumnVectorGraphSearchScratch{})
	if err == nil {
		t.Fatal("SearchCosine succeeded with unrepresentable query inverse norm")
	}
	if !strings.Contains(err.Error(), "inverse norm") {
		t.Fatalf("error=%q want inverse norm validation", err)
	}
}

func TestColumnVectorGraphSearchAllocs(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(1024, 64, 16, false))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	query, ok := graph.VectorAt(nil, 511)
	if !ok {
		t.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	var scratch ColumnVectorGraphSearchScratch
	if results, _, err := graph.SearchCosine(query, opts, &scratch); err != nil {
		t.Fatalf("warm SearchCosine: %v", err)
	} else if len(results) != opts.TopK {
		t.Fatalf("warm results=%d want %d", len(results), opts.TopK)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		results, trace, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			panic(err)
		}
		if len(results) != opts.TopK {
			panic("unexpected column vector graph result count")
		}
		columnVectorGraphBenchSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
	})
	if allocs != 0 {
		t.Fatalf("hot SearchCosine allocs/run=%g want 0", allocs)
	}
}

func BenchmarkColumnVectorGraphSearchCosine(b *testing.B) {
	graph, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(8192, 128, 16, false))
	if err != nil {
		b.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	query, ok := graph.VectorAt(nil, 4096)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	var scratch ColumnVectorGraphSearchScratch
	warm, warmTrace, err := graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	if len(warm) != opts.TopK {
		b.Fatalf("warm results=%d want %d", len(warm), opts.TopK)
	}
	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, trace, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			b.Fatalf("SearchCosine: %v", err)
		}
		if len(results) != opts.TopK {
			b.Fatalf("results=%d want %d", len(results), opts.TopK)
		}
		columnVectorGraphBenchSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
	}
	b.ReportMetric(float64(graph.Edges())/float64(graph.Rows()), "edges/node")
	b.ReportMetric(float64(warmTrace.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(warmTrace.EdgesVisited), "edges/search")
}

func BenchmarkColumnVectorGraphBuildFromColumns(b *testing.B) {
	columns := columnVectorGraphTestColumns(8192, 128, 16, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		graph, err := NewColumnVectorGraphFromColumns(columns)
		if err != nil {
			b.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
		}
		columnVectorGraphBenchSink += int64(graph.Rows()*graph.Dims() + graph.Edges())
	}
}

func columnVectorGraphTestColumns(rows int, dims int, degree int, complete bool) ColumnVectorGraphColumns {
	ids := make([][]byte, rows)
	vectors := make([]float32, rows*dims)
	invNorms := make([]float32, rows)
	offsets := make([]uint32, rows+1)
	neighbors := make([]uint32, 0, rows*maxColumnVectorGraphDegree(rows, degree, complete))
	for row := 0; row < rows; row++ {
		ids[row] = []byte(fmt.Sprintf("doc-%06d", row))
		vector := vectorBenchmarkEmbedding(row, dims)
		copy(vectors[row*dims:(row+1)*dims], vector)
		invNorms[row] = float32(1 / math.Sqrt(vectorNormSquared(vector)))
		offsets[row] = uint32(len(neighbors))
		if complete {
			for neighbor := 0; neighbor < rows; neighbor++ {
				if neighbor != row {
					neighbors = append(neighbors, uint32(neighbor))
				}
			}
			continue
		}
		for edge := 0; edge < degree; edge++ {
			step := edge/2 + 1
			neighbor := row + step
			if edge%2 == 1 {
				neighbor = row - step
			}
			neighbor %= rows
			if neighbor < 0 {
				neighbor += rows
			}
			neighbors = append(neighbors, uint32(neighbor))
		}
	}
	offsets[rows] = uint32(len(neighbors))
	return ColumnVectorGraphColumns{
		DocumentIDs:     ids,
		Vectors:         vectors,
		InvNorms:        invNorms,
		NeighborOffsets: offsets,
		Neighbors:       neighbors,
		Dimensions:      dims,
		EntryPoint:      0,
		EfSearch:        defaultVectorIndexEfSearch,
	}
}

func maxColumnVectorGraphDegree(rows int, degree int, complete bool) int {
	if complete {
		return rows - 1
	}
	return degree
}

func exactColumnVectorGraphCosine(t *testing.T, graph *ColumnVectorGraph, query []float32, topK int) []VectorSearchResult {
	t.Helper()
	queryNormSquared := vectorNormSquared(query)
	if queryNormSquared == 0 {
		t.Fatal("zero query norm")
	}
	queryInvNorm := float32(1 / math.Sqrt(queryNormSquared))
	results := make([]VectorSearchResult, graph.Rows())
	for ordinal := 0; ordinal < graph.Rows(); ordinal++ {
		results[ordinal] = VectorSearchResult{
			DocumentID: graph.documentID(ordinal),
			Distance:   exactColumnVectorGraphDistance(t, graph, query, queryInvNorm, ordinal),
		}
	}
	slices.SortFunc(results, compareVectorSearchResults)
	if topK < 0 {
		topK = 0
	}
	if topK > len(results) {
		topK = len(results)
	}
	return results[:topK]
}

func exactColumnVectorGraphDistance(t *testing.T, graph *ColumnVectorGraph, query []float32, queryInvNorm float32, ordinal int) float32 {
	t.Helper()
	vector, ok := graph.VectorAt(nil, ordinal)
	if !ok {
		return float32(math.Inf(1))
	}
	var dot float32
	var normSquared float64
	for dim, value := range vector {
		dot += query[dim] * value
		normSquared += float64(value) * float64(value)
	}
	if normSquared == 0 {
		return float32(math.Inf(1))
	}
	return 1 - dot*queryInvNorm*float32(1/math.Sqrt(normSquared))
}
