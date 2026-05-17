package colgranule

import (
	"math"
	"slices"
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
			Distance:  graph.cosineDistance(query, queryInvNorm, ordinal),
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
	return results[:topK]
}
