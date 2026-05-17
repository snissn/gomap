package colgranule

import (
	"math"
	"slices"
	"strings"
	"testing"
)

func TestColumnVectorGraphSearchMatchesExactOnCompleteGraph(t *testing.T) {
	reader := columnVectorGraphTestReader(t, 16, 8, 0, true)
	graph, stats, err := NewColumnVectorGraphFromPartSet(reader, ColumnVectorGraphOptions{})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromPartSet: %v", err)
	}
	if stats.Rows != 16 || stats.Dims != 8 || stats.Edges != 16*15 {
		t.Fatalf("load stats=%+v want rows=16 dims=8 edges=%d", stats, 16*15)
	}
	query, ok := graph.VectorAt(nil, 9)
	if !ok {
		t.Fatal("missing graph vector ordinal 9")
	}
	var scratch ColumnVectorGraphSearchScratch
	results, searchStats, err := graph.SearchCosine(query, ColumnVectorGraphSearchOptions{TopK: 5, EfSearch: 16}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if searchStats.Returned != 5 || searchStats.CandidatesExamined != 16 {
		t.Fatalf("search stats=%+v want returned=5 candidates=16", searchStats)
	}
	exact := exactColumnVectorGraphCosine(t, graph, query, 5)
	if len(results) != len(exact) {
		t.Fatalf("results=%d exact=%d", len(results), len(exact))
	}
	for i := range results {
		if results[i].PrimaryID != exact[i].PrimaryID || results[i].Ordinal != exact[i].Ordinal {
			t.Fatalf("result %d=(id=%d ord=%d) want (id=%d ord=%d)", i, results[i].PrimaryID, results[i].Ordinal, exact[i].PrimaryID, exact[i].Ordinal)
		}
		if math.Abs(float64(results[i].Distance-exact[i].Distance)) > 1e-6 {
			t.Fatalf("result %d distance=%g want %g", i, results[i].Distance, exact[i].Distance)
		}
	}
}

func TestColumnVectorGraphSearchAllocs(t *testing.T) {
	reader := columnVectorGraphTestReader(t, 1024, 64, 16, false)
	graph, _, err := NewColumnVectorGraphFromPartSet(reader, ColumnVectorGraphOptions{})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromPartSet: %v", err)
	}
	query, ok := graph.VectorAt(nil, 511)
	if !ok {
		t.Fatal("missing graph vector ordinal 511")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	var scratch ColumnVectorGraphSearchScratch
	if results, _, err := graph.SearchCosine(query, opts, &scratch); err != nil {
		t.Fatalf("warm SearchCosine: %v", err)
	} else if len(results) != opts.TopK {
		t.Fatalf("warm results=%d want %d", len(results), opts.TopK)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		results, stats, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			panic(err)
		}
		if len(results) != opts.TopK {
			panic("unexpected column vector graph result count")
		}
		benchSink += int64(results[0].Ordinal + stats.EdgesVisited)
	})
	if allocs != 0 {
		t.Fatalf("hot SearchCosine allocs/run=%g want 0", allocs)
	}
}

func TestColumnVectorGraphRejectsStaleInvNorm(t *testing.T) {
	batch := columnVectorGraphTestBatch(8, 4, 2, false)
	batch.Float32Vectors["embedding_inv_norm"] = Float32VectorColumn{
		Dims:   1,
		Values: append([]float32(nil), batch.Float32Vectors["embedding_inv_norm"].Values...),
	}
	batch.Float32Vectors["embedding_inv_norm"].Values[3] *= 1.25

	vectors := batch.Float32Vectors["embedding"]
	invNorms := batch.Float32Vectors["embedding_inv_norm"]
	neighbors := batch.AdjacencyLists["neighbors"]
	err := validateColumnVectorGraphStorage(vectors.Values, vectors.Dims, invNorms.Values, neighbors.Offsets, neighbors.Values, batch.Rows)
	if err == nil {
		t.Fatal("validateColumnVectorGraphStorage succeeded with stale inverse norm")
	}
	if !strings.Contains(err.Error(), "inv-norm") {
		t.Fatalf("error=%q want inv-norm validation", err)
	}
}

func TestColumnVectorGraphRejectsLooseTinyInvNorm(t *testing.T) {
	err := validateColumnVectorGraphStorage(
		[]float32{1e7},
		1,
		[]float32{1e-8},
		[]uint32{0, 0},
		nil,
		1,
	)
	if err == nil {
		t.Fatal("validateColumnVectorGraphStorage succeeded with materially wrong tiny inverse norm")
	}
	if !strings.Contains(err.Error(), "inv-norm") {
		t.Fatalf("error=%q want inv-norm validation", err)
	}
}

func TestColumnVectorGraphRejectsUnrepresentableQueryInvNorm(t *testing.T) {
	reader := columnVectorGraphTestReader(t, 4, 4, 2, false)
	graph, _, err := NewColumnVectorGraphFromPartSet(reader, ColumnVectorGraphOptions{})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromPartSet: %v", err)
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

func TestColumnVectorGraphScratchClearsFullVisitedCapacityOnWrap(t *testing.T) {
	var scratch ColumnVectorGraphSearchScratch
	visited, mark := scratch.nextVisitedEpoch(8)
	for i := range visited {
		visited[i] = mark
	}

	scratch.visitedEpoch = math.MaxUint32
	if _, mark := scratch.nextVisitedEpoch(4); mark != 1 {
		t.Fatalf("wrapped mark=%d want 1", mark)
	}
	full := scratch.visitedEpochs[:cap(scratch.visitedEpochs)]
	for i, value := range full {
		if value != 0 {
			t.Fatalf("visited epoch slot %d=%d after wrap; want 0", i, value)
		}
	}
}

func BenchmarkColumnVectorGraphSearchCosine(b *testing.B) {
	reader := columnVectorGraphTestReader(b, 8192, 128, 16, false)
	graph, loadStats, err := NewColumnVectorGraphFromPartSet(reader, ColumnVectorGraphOptions{})
	if err != nil {
		b.Fatalf("NewColumnVectorGraphFromPartSet: %v", err)
	}
	query, ok := graph.VectorAt(nil, 4096)
	if !ok {
		b.Fatal("missing graph vector ordinal 4096")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	var scratch ColumnVectorGraphSearchScratch
	warm, warmStats, err := graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	if len(warm) != opts.TopK {
		b.Fatalf("warm results=%d want %d", len(warm), opts.TopK)
	}
	b.ReportAllocs()
	b.SetBytes(int64(warmStats.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, stats, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			b.Fatalf("SearchCosine: %v", err)
		}
		if len(results) != opts.TopK {
			b.Fatalf("results=%d want %d", len(results), opts.TopK)
		}
		benchSink += int64(results[0].Ordinal + stats.CandidatesExamined + stats.EdgesVisited)
	}
	b.ReportMetric(float64(loadStats.Edges)/float64(loadStats.Rows), "edges/node")
	b.ReportMetric(float64(warmStats.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(warmStats.EdgesVisited), "edges/search")
}

func BenchmarkColumnVectorGraphLoadFromPartSet(b *testing.B) {
	reader := columnVectorGraphTestReader(b, 8192, 128, 16, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		graph, stats, err := NewColumnVectorGraphFromPartSet(reader, ColumnVectorGraphOptions{})
		if err != nil {
			b.Fatalf("NewColumnVectorGraphFromPartSet: %v", err)
		}
		benchSink += int64(graph.Rows()*graph.Dims() + stats.Edges)
	}
}

func columnVectorGraphTestReader(tb testing.TB, rows int, dims int, degree int, complete bool) *ColumnPartSetReader {
	tb.Helper()
	opts := columnVectorGraphTestOptions(rows, dims)
	workspace, err := OpenColumnWorkspace(tb.TempDir(), ColumnWorkspaceOptions{Collection: "vector_graph"})
	if err != nil {
		tb.Fatalf("OpenColumnWorkspace: %v", err)
	}
	tb.Cleanup(func() {
		if err := workspace.Close(); err != nil {
			tb.Fatalf("Close workspace: %v", err)
		}
	})
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:        "vector_graph",
		StoreOptions:      opts,
		InitialPartID:     1,
		InitialGeneration: 1,
	})
	if err != nil {
		tb.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	if _, err := adapter.PublishBaseBatch(columnVectorGraphTestBatch(rows, dims, degree, complete), ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(rows)}); err != nil {
		tb.Fatalf("PublishBaseBatch: %v", err)
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		tb.Fatalf("Reader: %v", err)
	}
	return reader
}

func columnVectorGraphTestOptions(rows int, dims int) ColumnStoreOptions {
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CodecBlockRows: rows},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, VectorDims: dims, Compression: CompressionNone, CodecBlockRows: rows},
			{Name: "embedding_inv_norm", Type: ColumnTypeFloat32Vector, VectorDims: 1, Compression: CompressionNone, CodecBlockRows: rows},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Compression: CompressionNone, CodecBlockRows: rows},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy: ColumnPartPolicy{
			RowsPerGranule:        rows,
			DefaultCodecBlockRows: rows,
		},
	}
}

func columnVectorGraphTestBatch(rows int, dims int, degree int, complete bool) ColumnBatch {
	ids := make([]int64, rows)
	vectors := make([]float32, rows*dims)
	invNorms := make([]float32, rows)
	offsets := make([]uint32, rows+1)
	neighbors := make([]int64, 0, rows*maxColumnVectorGraphDegree(rows, degree, complete))
	for row := 0; row < rows; row++ {
		ids[row] = int64(row)
		var normSquared float64
		for dim := 0; dim < dims; dim++ {
			value := float32(((row+1)*(dim+17)*1103515245%4093)+1) / 4096
			vectors[row*dims+dim] = value
			normSquared += float64(value) * float64(value)
		}
		invNorms[row] = float32(1 / math.Sqrt(normSquared))
		offsets[row] = uint32(len(neighbors))
		if complete {
			for neighbor := 0; neighbor < rows; neighbor++ {
				if neighbor != row {
					neighbors = append(neighbors, int64(neighbor))
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
			neighbors = append(neighbors, int64(neighbor))
		}
	}
	offsets[rows] = uint32(len(neighbors))
	return ColumnBatch{
		Rows: rows,
		Columns: map[string][]int64{
			"id": ids,
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding":          {Dims: dims, Values: vectors},
			"embedding_inv_norm": {Dims: 1, Values: invNorms},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {Offsets: offsets, Values: neighbors},
		},
	}
}

func maxColumnVectorGraphDegree(rows int, degree int, complete bool) int {
	if complete {
		return rows - 1
	}
	return degree
}

func exactColumnVectorGraphCosine(t *testing.T, graph *ColumnVectorGraph, query []float32, topK int) []ColumnVectorGraphSearchResult {
	t.Helper()
	queryInvNorm, err := columnVectorGraphQueryInvNorm(query)
	if err != nil {
		t.Fatalf("columnVectorGraphQueryInvNorm: %v", err)
	}
	ids := graph.PrimaryIDs(nil)
	results := make([]ColumnVectorGraphSearchResult, len(ids))
	for ordinal := range ids {
		results[ordinal] = ColumnVectorGraphSearchResult{
			PrimaryID: ids[ordinal],
			Ordinal:   ordinal,
			Distance:  exactColumnVectorGraphDistance(t, graph, query, queryInvNorm, ordinal),
		}
	}
	slices.SortFunc(results, func(left, right ColumnVectorGraphSearchResult) int {
		switch {
		case left.Distance < right.Distance:
			return -1
		case left.Distance > right.Distance:
			return 1
		case left.PrimaryID < right.PrimaryID:
			return -1
		case left.PrimaryID > right.PrimaryID:
			return 1
		default:
			return 0
		}
	})
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
