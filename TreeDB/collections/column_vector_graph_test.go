package collections

import (
	"bytes"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	if !strings.Contains(err.Error(), "diff=") || !strings.Contains(err.Error(), "relative tolerance=") {
		t.Fatalf("error=%q want inverse norm diff and relative tolerance", err)
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

func TestColumnVectorGraphRejectsTinyInvNormAbsoluteToleranceHole(t *testing.T) {
	_, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-huge")},
		Vectors:         []float32{1e20},
		InvNorms:        []float32{1e-13},
		NeighborOffsets: []uint32{0, 0},
		Dimensions:      1,
		EntryPoint:      0,
	})
	if err == nil {
		t.Fatal("NewColumnVectorGraphFromColumns succeeded with stale tiny inverse norm")
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
	if got := graph.EntryPoint(); got != -1 {
		t.Fatalf("zero-value EntryPoint()=%d want -1", got)
	}
}

func TestColumnVectorGraphRejectsRowsOutsideUint32OrdinalSpace(t *testing.T) {
	overflowRows := columnVectorGraphMaxUint32 + 1
	if overflowRows > uint64(^uint(0)>>1) {
		t.Skip("test target cannot represent the overflow row count as int")
	}
	err := validateColumnVectorGraphRowCount(int(overflowRows))
	if err == nil {
		t.Fatal("validateColumnVectorGraphRowCount accepted rows outside uint32 ordinal space")
	}
	if !strings.Contains(err.Error(), "uint32 ordinal") {
		t.Fatalf("error=%q want uint32 ordinal validation", err)
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

func TestColumnVectorGraphRejectsOverflowedQueryNorm(t *testing.T) {
	_, err := validateColumnVectorGraphQueryInvNorm(math.Inf(1))
	if err == nil {
		t.Fatal("validateColumnVectorGraphQueryInvNorm succeeded with overflowed norm")
	}
	if !strings.Contains(err.Error(), "magnitude") {
		t.Fatalf("error=%q want magnitude validation", err)
	}
}

func TestColumnVectorGraphSearchHandlesOverflowingFloat32Dot(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-large"), []byte("doc-orthogonal")},
		Vectors:         []float32{1e20, 0, 0, 1e20},
		InvNorms:        []float32{1e-20, 1e-20},
		NeighborOffsets: []uint32{0, 1, 2},
		Neighbors:       []uint32{1, 0},
		Dimensions:      2,
		EntryPoint:      0,
		EfSearch:        2,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}

	results, _, err := graph.SearchCosine([]float32{1e20, 0}, ColumnVectorGraphSearchOptions{TopK: 1, EfSearch: 2}, &ColumnVectorGraphSearchScratch{})
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results=%d want 1", len(results))
	}
	if !bytes.Equal(results[0].DocumentID, []byte("doc-large")) {
		t.Fatalf("result document ID=%q want doc-large", results[0].DocumentID)
	}
	if math.IsInf(float64(results[0].Distance), 0) || math.IsNaN(float64(results[0].Distance)) {
		t.Fatalf("distance=%g want finite", results[0].Distance)
	}
	if math.Abs(float64(results[0].Distance)) > 1e-6 {
		t.Fatalf("distance=%g want near 0", results[0].Distance)
	}
}

func TestColumnVectorGraphSearchHandlesOverflowingScaledCosine(t *testing.T) {
	const dims = 128
	vectorValue := float32(1e38)
	queryValue := float32(0.1 / math.Sqrt(float64(dims)))
	invNorm := float32(1 / math.Sqrt(float64(dims)*1e76))
	if invNorm <= 0 || math.IsInf(float64(invNorm), 0) || math.IsNaN(float64(invNorm)) {
		t.Fatalf("invNorm=%g want positive finite", invNorm)
	}

	vectors := make([]float32, dims*2)
	for dim := 0; dim < dims; dim++ {
		vectors[dim] = vectorValue
		vectors[dims+dim] = -vectorValue
	}
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-positive"), []byte("doc-negative")},
		Vectors:         vectors,
		InvNorms:        []float32{invNorm, invNorm},
		NeighborOffsets: []uint32{0, 1, 2},
		Neighbors:       []uint32{1, 0},
		Dimensions:      dims,
		EntryPoint:      0,
		EfSearch:        2,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}

	query := make([]float32, dims)
	for dim := range query {
		query[dim] = queryValue
	}
	results, _, err := graph.SearchCosine(query, ColumnVectorGraphSearchOptions{TopK: 1, EfSearch: 2}, &ColumnVectorGraphSearchScratch{})
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results=%d want 1", len(results))
	}
	if !bytes.Equal(results[0].DocumentID, []byte("doc-positive")) {
		t.Fatalf("result document ID=%q want doc-positive", results[0].DocumentID)
	}
	if math.IsInf(float64(results[0].Distance), 0) || math.IsNaN(float64(results[0].Distance)) {
		t.Fatalf("distance=%g want finite", results[0].Distance)
	}
	if math.Abs(float64(results[0].Distance)) > 1e-5 {
		t.Fatalf("distance=%g want near 0", results[0].Distance)
	}
}

func TestColumnVectorGraphSearchRecomputesTinyDotUnderflow(t *testing.T) {
	const tiny = float32(1e-38)
	invNorm := float32(1e38)
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-z-positive"), []byte("doc-a-negative")},
		Vectors:         []float32{tiny, -tiny},
		InvNorms:        []float32{invNorm, invNorm},
		NeighborOffsets: []uint32{0, 1, 2},
		Neighbors:       []uint32{1, 0},
		Dimensions:      1,
		EntryPoint:      0,
		EfSearch:        2,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}

	results, _, err := graph.SearchCosine([]float32{tiny}, ColumnVectorGraphSearchOptions{TopK: 1, EfSearch: 2}, &ColumnVectorGraphSearchScratch{})
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results=%d want 1", len(results))
	}
	if !bytes.Equal(results[0].DocumentID, []byte("doc-z-positive")) {
		t.Fatalf("result document ID=%q want doc-z-positive", results[0].DocumentID)
	}
	if math.Abs(float64(results[0].Distance)) > 1e-5 {
		t.Fatalf("distance=%g want near 0", results[0].Distance)
	}
}

func TestColumnVectorGraphCosineDistanceKeepsTrueOrthogonalDotFast(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-orthogonal")},
		Vectors:         []float32{1, 0},
		InvNorms:        []float32{1},
		NeighborOffsets: []uint32{0, 0},
		Dimensions:      2,
		EntryPoint:      0,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}

	// Directly use an invalid inverse norm sentinel to prove a true zero dot
	// returns before the wide scaled fallback path can observe scale values.
	distance := graph.cosineDistance([]float32{0, 1}, float32(math.NaN()), 0)
	if distance != columnVectorGraphOrthogonalDistance {
		t.Fatalf("distance=%g want orthogonal distance %g", distance, float32(columnVectorGraphOrthogonalDistance))
	}
}

func TestColumnVectorGraphCosineDistanceClampsAcceptedInvNormDrift(t *testing.T) {
	const driftedInvNorm = 1.00005
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-positive"), []byte("doc-negative")},
		Vectors:         []float32{1, -1},
		InvNorms:        []float32{driftedInvNorm, driftedInvNorm},
		NeighborOffsets: []uint32{0, 0, 0},
		Dimensions:      1,
		EntryPoint:      0,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}

	if distance := graph.cosineDistance([]float32{1}, 1, 0); distance != 0 {
		t.Fatalf("positive drift distance=%g want 0", distance)
	}
	if distance := graph.cosineDistance([]float32{1}, 1, 1); distance != 2 {
		t.Fatalf("negative drift distance=%g want 2", distance)
	}
}

func TestColumnVectorGraphSearchUsesDocumentIDTieOrdering(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-z"), []byte("doc-a"), []byte("doc-b")},
		Vectors:         []float32{1, 0, 1, 0, 1, 0},
		InvNorms:        []float32{1, 1, 1},
		NeighborOffsets: []uint32{0, 2, 4, 6},
		Neighbors:       []uint32{1, 2, 0, 2, 0, 1},
		Dimensions:      2,
		EntryPoint:      0,
		EfSearch:        2,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	if graph.ordinalTieOrder {
		t.Fatal("graph stayed on ordinal tie path for out-of-order document IDs")
	}
	if len(graph.idRanks) != graph.Rows() {
		t.Fatalf("idRanks=%d want rows=%d", len(graph.idRanks), graph.Rows())
	}

	results, _, err := graph.SearchCosine([]float32{1, 0}, ColumnVectorGraphSearchOptions{TopK: 2, EfSearch: 2}, &ColumnVectorGraphSearchScratch{})
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results=%d want 2", len(results))
	}
	if !bytes.Equal(results[0].DocumentID, []byte("doc-a")) || !bytes.Equal(results[1].DocumentID, []byte("doc-b")) {
		t.Fatalf("result IDs=(%q,%q) want (doc-a,doc-b)", results[0].DocumentID, results[1].DocumentID)
	}
}

func TestColumnVectorGraphSearchExploresEqualDistanceOnOrdinalTiePath(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-a"), []byte("doc-b"), []byte("doc-c")},
		Vectors:         []float32{1, 0, 1, 0, 1, 0},
		InvNorms:        []float32{1, 1, 1},
		NeighborOffsets: []uint32{0, 0, 1, 2},
		Neighbors:       []uint32{2, 0},
		Dimensions:      2,
		EntryPoint:      1,
		EfSearch:        1,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	if !graph.ordinalTieOrder {
		t.Fatal("graph did not use ordinal tie path for sorted document IDs")
	}

	results, _, err := graph.SearchCosine([]float32{1, 0}, ColumnVectorGraphSearchOptions{TopK: 1, EfSearch: 1}, &ColumnVectorGraphSearchScratch{})
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results=%d want 1", len(results))
	}
	if !bytes.Equal(results[0].DocumentID, []byte("doc-a")) {
		t.Fatalf("result ID=%q want doc-a", results[0].DocumentID)
	}
}

func TestColumnVectorGraphSearchExploresEqualDistanceOnDocumentTiePath(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("doc-b"), []byte("doc-c"), []byte("doc-a")},
		Vectors:         []float32{1, 0, 1, 0, 1, 0},
		InvNorms:        []float32{1, 1, 1},
		NeighborOffsets: []uint32{0, 1, 2, 2},
		Neighbors:       []uint32{1, 2},
		Dimensions:      2,
		EntryPoint:      0,
		EfSearch:        1,
	})
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	if graph.ordinalTieOrder {
		t.Fatal("graph stayed on ordinal tie path for out-of-order document IDs")
	}

	results, _, err := graph.SearchCosine([]float32{1, 0}, ColumnVectorGraphSearchOptions{TopK: 1, EfSearch: 1}, &ColumnVectorGraphSearchScratch{})
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results=%d want 1", len(results))
	}
	if !bytes.Equal(results[0].DocumentID, []byte("doc-a")) {
		t.Fatalf("result ID=%q want doc-a", results[0].DocumentID)
	}
}

func TestColumnVectorGraphSearchBoundsEqualDistanceBridgeTraversal(t *testing.T) {
	rows := 12
	sortedIDs := make([][]byte, 0, rows)
	for i := 0; i < rows; i++ {
		sortedIDs = append(sortedIDs, []byte(fmt.Sprintf("doc-%02d", i)))
	}
	outOfOrderIDs := make([][]byte, len(sortedIDs))
	copy(outOfOrderIDs, sortedIDs)
	outOfOrderIDs[1], outOfOrderIDs[2] = outOfOrderIDs[2], outOfOrderIDs[1]

	for _, tc := range []struct {
		name           string
		ids            [][]byte
		wantOrdinal    bool
		wantCandidates int
		wantEdges      int
	}{
		{name: "ordinal", ids: sortedIDs, wantOrdinal: true, wantCandidates: 5, wantEdges: 4},
		{name: "document", ids: outOfOrderIDs, wantOrdinal: false, wantCandidates: 5, wantEdges: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vectors := make([]float32, 0, rows*2)
			invNorms := make([]float32, 0, rows)
			neighbors := make([]uint32, 0, rows-1)
			offsets := make([]uint32, rows+1)
			for i := 0; i < rows; i++ {
				vectors = append(vectors, 1, 0)
				invNorms = append(invNorms, 1)
				offsets[i] = uint32(len(neighbors))
				if i+1 < rows {
					neighbors = append(neighbors, uint32(i+1))
				}
			}
			offsets[rows] = uint32(len(neighbors))

			graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
				DocumentIDs:     tc.ids,
				Vectors:         vectors,
				InvNorms:        invNorms,
				NeighborOffsets: offsets,
				Neighbors:       neighbors,
				Dimensions:      2,
				EntryPoint:      0,
				EfSearch:        2,
			})
			if err != nil {
				t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
			}
			if graph.ordinalTieOrder != tc.wantOrdinal {
				t.Fatalf("ordinalTieOrder=%v want %v", graph.ordinalTieOrder, tc.wantOrdinal)
			}

			results, trace, err := graph.SearchCosine([]float32{1, 0}, ColumnVectorGraphSearchOptions{TopK: 1, EfSearch: 2}, &ColumnVectorGraphSearchScratch{})
			if err != nil {
				t.Fatalf("SearchCosine: %v", err)
			}
			if len(results) != 1 || !bytes.Equal(results[0].DocumentID, []byte("doc-00")) {
				t.Fatalf("results=%+v want doc-00", results)
			}
			if trace.CandidatesExamined != tc.wantCandidates || trace.EdgesVisited != tc.wantEdges {
				t.Fatalf("trace=%+v want bounded bridge traversal with candidates=%d edges=%d", trace, tc.wantCandidates, tc.wantEdges)
			}
		})
	}
}

func TestColumnVectorGraphSearchBoundsBetterEqualDistanceBridgeTraversal(t *testing.T) {
	rows := 12
	sortedIDs := make([][]byte, 0, rows)
	reverseIDs := make([][]byte, 0, rows)
	for i := 0; i < rows; i++ {
		sortedIDs = append(sortedIDs, []byte(fmt.Sprintf("doc-%02d", i)))
		reverseIDs = append(reverseIDs, []byte(fmt.Sprintf("doc-%02d", rows-1-i)))
	}

	for _, tc := range []struct {
		name        string
		ids         [][]byte
		entryPoint  int
		reverseEdge bool
		wantOrdinal bool
	}{
		{name: "ordinal", ids: sortedIDs, entryPoint: rows - 1, reverseEdge: true, wantOrdinal: true},
		{name: "document", ids: reverseIDs, entryPoint: 0, wantOrdinal: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vectors := make([]float32, 0, rows*2)
			invNorms := make([]float32, 0, rows)
			neighbors := make([]uint32, 0, rows-1)
			offsets := make([]uint32, rows+1)
			for i := 0; i < rows; i++ {
				vectors = append(vectors, 1, 0)
				invNorms = append(invNorms, 1)
				offsets[i] = uint32(len(neighbors))
				switch {
				case tc.reverseEdge && i > 0:
					neighbors = append(neighbors, uint32(i-1))
				case !tc.reverseEdge && i+1 < rows:
					neighbors = append(neighbors, uint32(i+1))
				}
			}
			offsets[rows] = uint32(len(neighbors))

			graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
				DocumentIDs:     tc.ids,
				Vectors:         vectors,
				InvNorms:        invNorms,
				NeighborOffsets: offsets,
				Neighbors:       neighbors,
				Dimensions:      2,
				EntryPoint:      tc.entryPoint,
				EfSearch:        1,
			})
			if err != nil {
				t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
			}
			if graph.ordinalTieOrder != tc.wantOrdinal {
				t.Fatalf("ordinalTieOrder=%v want %v", graph.ordinalTieOrder, tc.wantOrdinal)
			}

			results, trace, err := graph.SearchCosine([]float32{1, 0}, ColumnVectorGraphSearchOptions{TopK: 1, EfSearch: 1}, &ColumnVectorGraphSearchScratch{})
			if err != nil {
				t.Fatalf("SearchCosine: %v", err)
			}
			if len(results) != 1 || !bytes.Equal(results[0].DocumentID, []byte("doc-09")) {
				t.Fatalf("results=%+v want doc-09 after bounded better-tie traversal", results)
			}
			if trace.CandidatesExamined != 3 || trace.EdgesVisited != 2 {
				t.Fatalf("trace=%+v want better-tie traversal bounded to candidates=3 edges=2", trace)
			}
		})
	}
}

func TestColumnVectorGraphSkipsRankTableForOrdinalDocumentIDs(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(32, 16, 4, false))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	if !graph.ordinalTieOrder {
		t.Fatal("graph did not use ordinal tie path for sorted document IDs")
	}
	if graph.idRanks != nil {
		t.Fatalf("idRanks allocated on ordinal tie path: len=%d", len(graph.idRanks))
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

func TestColumnVectorGraphSearchParallelIndependentScratch(t *testing.T) {
	graph, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(1024, 64, 16, false))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	query, ok := graph.VectorAt(nil, 511)
	if !ok {
		t.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	var expectedScratch ColumnVectorGraphSearchScratch
	expected, expectedTrace, err := graph.SearchCosine(query, opts, &expectedScratch)
	if err != nil {
		t.Fatalf("expected SearchCosine: %v", err)
	}
	if len(expected) != opts.TopK {
		t.Fatalf("expected results=%d want %d", len(expected), opts.TopK)
	}
	expectedFirstID := bytes.Clone(expected[0].DocumentID)
	expectedFirstDistance := expected[0].Distance

	const iterations = 100
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var scratch ColumnVectorGraphSearchScratch
			if results, _, err := graph.SearchCosine(query, opts, &scratch); err != nil {
				errs <- fmt.Errorf("worker %d warm SearchCosine: %w", worker, err)
				return
			} else if len(results) != opts.TopK {
				errs <- fmt.Errorf("worker %d warm results=%d want %d", worker, len(results), opts.TopK)
				return
			}
			for i := 0; i < iterations; i++ {
				results, trace, err := graph.SearchCosine(query, opts, &scratch)
				if err != nil {
					errs <- fmt.Errorf("worker %d iteration %d SearchCosine: %w", worker, i, err)
					return
				}
				if len(results) != opts.TopK {
					errs <- fmt.Errorf("worker %d iteration %d results=%d want %d", worker, i, len(results), opts.TopK)
					return
				}
				if trace.CandidatesExamined != expectedTrace.CandidatesExamined || trace.EdgesVisited != expectedTrace.EdgesVisited {
					errs <- fmt.Errorf("worker %d iteration %d trace=%+v want candidates=%d edges=%d", worker, i, trace, expectedTrace.CandidatesExamined, expectedTrace.EdgesVisited)
					return
				}
				if !bytes.Equal(results[0].DocumentID, expectedFirstID) {
					errs <- fmt.Errorf("worker %d iteration %d first document ID=%q want %q", worker, i, results[0].DocumentID, expectedFirstID)
					return
				}
				if results[0].Distance != expectedFirstDistance {
					errs <- fmt.Errorf("worker %d iteration %d first distance=%g want %g", worker, i, results[0].Distance, expectedFirstDistance)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	failed := false
	for err := range errs {
		t.Error(err)
		failed = true
	}
	if failed {
		t.FailNow()
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

func BenchmarkColumnVectorGraphSearchCosineParallel(b *testing.B) {
	graph, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(8192, 128, 16, false))
	if err != nil {
		b.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	query, ok := graph.VectorAt(nil, 4096)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	b.SetParallelism(1)
	// RunParallel starts parallelism*GOMAXPROCS workers. Prewarm one exclusive
	// scratch per worker before ResetTimer so the timed loop measures hot search,
	// not per-worker scratch allocation or warmup. If Go ever changes this worker
	// contract, fail instead of sharing scratch or allocating inside the timed loop.
	workers := runtime.GOMAXPROCS(0)
	var warmTrace ColumnVectorGraphSearchTrace
	scratches := make([]*ColumnVectorGraphSearchScratch, workers)
	for i := 0; i < workers; i++ {
		scratch := new(ColumnVectorGraphSearchScratch)
		warm, trace, err := graph.SearchCosine(query, opts, scratch)
		if err != nil {
			b.Fatalf("warm SearchCosine worker %d: %v", i, err)
		}
		if len(warm) != opts.TopK {
			b.Fatalf("warm worker %d results=%d want %d", i, len(warm), opts.TopK)
		}
		warmTrace = trace
		scratches[i] = scratch
	}
	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	var nextWorker uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddUint64(&nextWorker, 1)) - 1
		if workerID >= len(scratches) {
			panic(fmt.Sprintf("RunParallel spawned %d workers, but only %d scratches were prewarmed", workerID+1, len(scratches)))
		}
		scratch := scratches[workerID]
		var localSink int64
		for pb.Next() {
			results, trace, err := graph.SearchCosine(query, opts, scratch)
			if err != nil {
				panic(err)
			}
			if len(results) != opts.TopK {
				panic("unexpected column vector graph result count")
			}
			localSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
		}
		atomic.AddInt64(&columnVectorGraphBenchSink, localSink)
	})
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
	var dot float64
	var normSquared float64
	for dim, value := range vector {
		dot += float64(query[dim]) * float64(value)
		normSquared += float64(value) * float64(value)
	}
	if normSquared == 0 {
		return float32(math.Inf(1))
	}
	return float32(1 - dot*float64(queryInvNorm)*(1/math.Sqrt(normSquared)))
}
