package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
)

const (
	columnVectorGraphStrategyCosine      = "column_graph_cosine"
	columnVectorGraphMaxUint32           = uint64(^uint32(0))
	columnVectorGraphInvNormRelTolerance = 1e-4
	columnVectorGraphOrthogonalDistance  = 1
)

// ColumnVectorGraphColumns supplies immutable column-store buffers for a
// single-layer ANN graph. Vectors are row-major float32 values, InvNorms holds
// one cosine inverse norm per row, and NeighborOffsets/Neighbors form a CSR
// adjacency-list column over zero-based row ordinals.
type ColumnVectorGraphColumns struct {
	DocumentIDs     [][]byte
	Vectors         []float32
	InvNorms        []float32
	NeighborOffsets []uint32
	Neighbors       []uint32
	Dimensions      int
	EntryPoint      int
	EfSearch        int
}

// ColumnVectorGraphSearchOptions configures one column-backed graph search.
type ColumnVectorGraphSearchOptions struct {
	TopK     int
	EfSearch int
}

// ColumnVectorGraphSearchTrace reports how one column-backed graph search ran.
type ColumnVectorGraphSearchTrace struct {
	VectorIndexTrace
	TopK         int
	EdgesVisited int
}

// ColumnVectorGraphSearchScratch is caller-owned reusable search workspace.
// It is not safe for concurrent use. SearchCosine returns slices backed by this
// scratch, and reusing the scratch invalidates previously returned result
// slices. Result document IDs alias graph-owned immutable ID storage. A
// ColumnVectorGraph can be searched concurrently only when each goroutine uses
// its own scratch and the borrowed graph columns remain immutable.
type ColumnVectorGraphSearchScratch struct {
	graph   vectorIndexSearchScratch
	queue   columnVectorGraphMinCandidateHeap
	best    columnVectorGraphMaxCandidateHeap
	out     []vectorIndexCandidate
	results []VectorSearchResult
}

// ColumnVectorGraph is a column-store-backed ANN graph over row ordinals.
// It borrows vector, inverse-norm, and adjacency buffers from immutable column
// storage and copies document IDs into a compact arena for stable result IDs.
type ColumnVectorGraph struct {
	dims            int
	entryPoint      int
	efSearch        int
	vectors         []float32
	invNorms        []float32
	neighborOffsets []uint32
	neighbors       []uint32
	idArena         []byte
	idOffsets       []uint32
	idRanks         []uint32
	ordinalTieOrder bool
}

// NewColumnVectorGraphFromColumns validates column-store vector graph buffers
// and returns a graph that borrows the vector/inv-norm/adjacency columns. Those
// borrowed column buffers must remain immutable for the graph lifetime.
// Self-loop neighbors are tolerated; search visited-epoch bookkeeping
// deduplicates a row that re-encounters its own ordinal.
func NewColumnVectorGraphFromColumns(columns ColumnVectorGraphColumns) (*ColumnVectorGraph, error) {
	rows, err := validateColumnVectorGraphColumns(columns)
	if err != nil {
		return nil, err
	}
	idArena, idOffsets, err := copyColumnVectorGraphIDs(columns.DocumentIDs)
	if err != nil {
		return nil, err
	}
	ordinalTieOrder := columnVectorGraphDocumentIDsMatchOrdinalTieOrder(columns.DocumentIDs)
	var idRanks []uint32
	if !ordinalTieOrder {
		idRanks = columnVectorGraphDocumentIDRanks(columns.DocumentIDs)
	}
	efSearch := columns.EfSearch
	if efSearch <= 0 {
		efSearch = defaultVectorIndexEfSearch
	}
	return &ColumnVectorGraph{
		dims:            columns.Dimensions,
		entryPoint:      columns.EntryPoint,
		efSearch:        efSearch,
		vectors:         columns.Vectors,
		invNorms:        columns.InvNorms,
		neighborOffsets: columns.NeighborOffsets,
		neighbors:       columns.Neighbors,
		idArena:         idArena,
		idOffsets:       idOffsets[:rows+1],
		idRanks:         idRanks,
		ordinalTieOrder: ordinalTieOrder,
	}, nil
}

// Rows returns the number of graph rows.
func (g *ColumnVectorGraph) Rows() int {
	if g == nil || len(g.idOffsets) == 0 {
		return 0
	}
	return len(g.idOffsets) - 1
}

// Dims returns the vector dimensionality.
func (g *ColumnVectorGraph) Dims() int {
	if g == nil {
		return 0
	}
	return g.dims
}

// Edges returns the number of CSR adjacency entries.
func (g *ColumnVectorGraph) Edges() int {
	if g == nil {
		return 0
	}
	return len(g.neighbors)
}

// EntryPoint returns the graph entry row ordinal.
func (g *ColumnVectorGraph) EntryPoint() int {
	if g == nil || len(g.idOffsets) == 0 {
		return -1
	}
	return g.entryPoint
}

// DocumentID appends the row document ID to dst and reports whether it exists.
func (g *ColumnVectorGraph) DocumentID(dst []byte, ordinal int) ([]byte, bool) {
	if g == nil || ordinal < 0 || ordinal >= g.Rows() {
		return dst, false
	}
	return append(dst, g.documentID(ordinal)...), true
}

// VectorAt appends the row vector to dst and reports whether it exists.
func (g *ColumnVectorGraph) VectorAt(dst []float32, ordinal int) ([]float32, bool) {
	if g == nil || ordinal < 0 || ordinal >= g.Rows() {
		return dst, false
	}
	return append(dst, g.vectorAt(ordinal)...), true
}

// SearchCosine runs cosine ANN search over the column-backed graph. A non-nil
// scratch is required so hot searches can run without hidden allocations.
func (g *ColumnVectorGraph) SearchCosine(query []float32, opts ColumnVectorGraphSearchOptions, scratch *ColumnVectorGraphSearchScratch) ([]VectorSearchResult, ColumnVectorGraphSearchTrace, error) {
	trace := ColumnVectorGraphSearchTrace{
		VectorIndexTrace: VectorIndexTrace{Strategy: columnVectorGraphStrategyCosine},
		TopK:             opts.TopK,
	}
	if g == nil {
		return nil, trace, errors.New("collections: nil column vector graph")
	}
	if scratch == nil {
		return nil, trace, errors.New("collections: nil column vector graph search scratch")
	}
	if opts.TopK <= 0 {
		return nil, trace, errors.New("collections: column vector graph TopK must be positive")
	}
	if len(query) != g.dims {
		return nil, trace, fmt.Errorf("collections: column vector graph query has dimension %d, want %d", len(query), g.dims)
	}
	queryNormSquared, badDim, badValue := columnVectorGraphNormSquared(query)
	if badDim >= 0 {
		return nil, trace, fmt.Errorf("collections: column vector graph query dim %d is not finite: %g", badDim, badValue)
	}
	queryInvNorm, err := validateColumnVectorGraphQueryInvNorm(queryNormSquared)
	if err != nil {
		return nil, trace, err
	}
	efSearch := g.normalizeEfSearch(opts.EfSearch, opts.TopK)
	trace.EfSearch = efSearch
	candidates, edgesVisited, candidatesExamined := g.searchCandidates(query, queryInvNorm, efSearch, scratch)
	trace.CandidatesExamined = candidatesExamined
	trace.CandidatesAfterTombstone = len(candidates)
	trace.CandidatesAfterFilter = len(candidates)
	trace.RerankCount = len(candidates)
	trace.EdgesVisited = edgesVisited
	resultLimit := opts.TopK
	if rows := g.Rows(); resultLimit > rows {
		resultLimit = rows
	}
	if resultLimit > len(candidates) {
		resultLimit = len(candidates)
	}
	if cap(scratch.results) < resultLimit {
		scratch.results = make([]VectorSearchResult, 0, resultLimit)
	}
	results := scratch.results[:0]
	for _, candidate := range candidates {
		if len(results) >= resultLimit {
			break
		}
		if candidate.nodeID < 0 || candidate.nodeID >= g.Rows() {
			continue
		}
		results = append(results, VectorSearchResult{
			DocumentID: g.documentID(candidate.nodeID),
			Distance:   candidate.distance,
		})
	}
	scratch.results = results
	trace.ReturnedCount = len(results)
	if results == nil {
		results = []VectorSearchResult{}
	}
	return results, trace, nil
}

func validateColumnVectorGraphColumns(columns ColumnVectorGraphColumns) (int, error) {
	if columns.Dimensions <= 0 {
		return 0, fmt.Errorf("collections: column vector graph dimensions=%d", columns.Dimensions)
	}
	if len(columns.Vectors)%columns.Dimensions != 0 {
		return 0, fmt.Errorf("collections: column vector graph vector values=%d not divisible by dimensions=%d", len(columns.Vectors), columns.Dimensions)
	}
	rows := len(columns.Vectors) / columns.Dimensions
	if rows == 0 {
		return 0, errors.New("collections: column vector graph is empty")
	}
	if err := validateColumnVectorGraphRowCount(rows); err != nil {
		return 0, err
	}
	if len(columns.DocumentIDs) != rows {
		return 0, fmt.Errorf("collections: column vector graph document IDs=%d want rows=%d", len(columns.DocumentIDs), rows)
	}
	if len(columns.InvNorms) != rows {
		return 0, fmt.Errorf("collections: column vector graph inverse norms=%d want rows=%d", len(columns.InvNorms), rows)
	}
	if len(columns.NeighborOffsets) != rows+1 {
		return 0, fmt.Errorf("collections: column vector graph neighbor offsets=%d want %d", len(columns.NeighborOffsets), rows+1)
	}
	if columns.EntryPoint < 0 || columns.EntryPoint >= rows {
		return 0, fmt.Errorf("collections: column vector graph entry point=%d outside rows=%d", columns.EntryPoint, rows)
	}
	if columns.NeighborOffsets[0] != 0 {
		return 0, fmt.Errorf("collections: column vector graph first neighbor offset=%d want 0", columns.NeighborOffsets[0])
	}
	prev := uint32(0)
	for row := 0; row < rows; row++ {
		next := columns.NeighborOffsets[row+1]
		if next < prev {
			return 0, fmt.Errorf("collections: column vector graph neighbor offset row=%d value=%d before previous=%d", row+1, next, prev)
		}
		prev = next
		start := row * columns.Dimensions
		vector := columns.Vectors[start : start+columns.Dimensions]
		normSquared, badDim, badValue := columnVectorGraphNormSquared(vector)
		if badDim >= 0 {
			return 0, fmt.Errorf("collections: column vector graph row %d vector dim %d is not finite: %g", row, badDim, badValue)
		}
		if normSquared == 0 {
			return 0, fmt.Errorf("collections: column vector graph row %d vector has zero magnitude", row)
		}
		if err := validateColumnVectorGraphInvNorm(row, columns.InvNorms[row], normSquared); err != nil {
			return 0, err
		}
	}
	if uint64(prev) != uint64(len(columns.Neighbors)) {
		return 0, fmt.Errorf("collections: column vector graph final neighbor offset=%d values=%d", prev, len(columns.Neighbors))
	}
	for edge, neighbor := range columns.Neighbors {
		if uint64(neighbor) >= uint64(rows) {
			return 0, fmt.Errorf("collections: column vector graph edge %d neighbor ordinal=%d outside rows=%d", edge, neighbor, rows)
		}
	}
	return rows, nil
}

func validateColumnVectorGraphRowCount(rows int) error {
	if uint64(rows) > columnVectorGraphMaxUint32 {
		return fmt.Errorf("collections: column vector graph rows=%d exceed uint32 ordinal space", rows)
	}
	return nil
}

func validateColumnVectorGraphInvNorm(row int, invNorm float32, normSquared float64) error {
	f := float64(invNorm)
	if f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("collections: column vector graph row %d inverse norm=%g is invalid", row, invNorm)
	}
	want := 1 / math.Sqrt(normSquared)
	diff := math.Abs(f - want)
	allowed := math.Abs(want) * columnVectorGraphInvNormRelTolerance
	if diff > allowed {
		return fmt.Errorf("collections: column vector graph row %d inverse norm=%g want %g diff=%g relative tolerance=%g", row, invNorm, want, diff, columnVectorGraphInvNormRelTolerance)
	}
	return nil
}

func validateColumnVectorGraphQueryInvNorm(normSquared float64) (float32, error) {
	if math.IsNaN(normSquared) || math.IsInf(normSquared, 0) {
		return 0, errors.New("collections: cosine column vector graph query magnitude is not representable")
	}
	if normSquared == 0 {
		return 0, errors.New("collections: cosine column vector graph query cannot have zero magnitude")
	}
	queryInvNorm64 := 1 / math.Sqrt(normSquared)
	if math.IsNaN(queryInvNorm64) || math.IsInf(queryInvNorm64, 0) || queryInvNorm64 > math.MaxFloat32 {
		return 0, errors.New("collections: cosine column vector graph query inverse norm is not representable")
	}
	return float32(queryInvNorm64), nil
}

func columnVectorGraphNormSquared(vector []float32) (float64, int, float32) {
	var normSquared float64
	for dim, value := range vector {
		f := float64(value)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, dim, value
		}
		normSquared += f * f
	}
	return normSquared, -1, 0
}

func copyColumnVectorGraphIDs(ids [][]byte) ([]byte, []uint32, error) {
	var total uint64
	for i, id := range ids {
		total += uint64(len(id))
		if total > columnVectorGraphMaxUint32 {
			return nil, nil, fmt.Errorf("collections: column vector graph document IDs exceed uint32 offset range at ordinal %d (running total=%d, limit=%d)", i, total, columnVectorGraphMaxUint32)
		}
	}
	arena := make([]byte, 0, int(total))
	offsets := make([]uint32, len(ids)+1)
	for i, id := range ids {
		offsets[i] = uint32(len(arena))
		arena = append(arena, id...)
	}
	offsets[len(ids)] = uint32(len(arena))
	return arena, offsets, nil
}

func columnVectorGraphDocumentIDRanks(ids [][]byte) []uint32 {
	ordinals := make([]int, len(ids))
	for ordinal := range ordinals {
		ordinals[ordinal] = ordinal
	}
	slices.SortFunc(ordinals, func(left, right int) int {
		if cmp := bytes.Compare(ids[left], ids[right]); cmp != 0 {
			return cmp
		}
		switch {
		case left < right:
			return -1
		case left > right:
			return 1
		default:
			return 0
		}
	})
	ranks := make([]uint32, len(ids))
	for rank, ordinal := range ordinals {
		ranks[ordinal] = uint32(rank)
	}
	return ranks
}

// columnVectorGraphDocumentIDsMatchOrdinalTieOrder reports whether document IDs
// are in non-decreasing (sorted) order. Non-decreasing order, including
// duplicate adjacent IDs, is sufficient for the ordinal-tie fast path because:
//   - In the ordinal-tie path, equal-distance candidates are ordered by nodeID.
//   - In the document-tie path, columnVectorGraphDocumentIDRanks breaks equal-ID
//     ties by ordinal position (lower ordinal = better rank).
//
// Both paths therefore produce identical ordering for equal document IDs, so
// the ordinal-tie fast path is safe to use whenever IDs are non-decreasing.
func columnVectorGraphDocumentIDsMatchOrdinalTieOrder(ids [][]byte) bool {
	for ordinal := 1; ordinal < len(ids); ordinal++ {
		if bytes.Compare(ids[ordinal-1], ids[ordinal]) > 0 {
			return false
		}
	}
	return true
}

func (g *ColumnVectorGraph) normalizeEfSearch(efSearch int, topK int) int {
	if efSearch <= 0 {
		efSearch = g.efSearch
	}
	if efSearch <= 0 {
		efSearch = defaultVectorIndexEfSearch
	}
	if efSearch < topK {
		efSearch = topK
	}
	if rows := g.Rows(); efSearch > rows {
		efSearch = rows
	}
	return efSearch
}

func (g *ColumnVectorGraph) searchCandidates(query []float32, queryInvNorm float32, limit int, scratch *ColumnVectorGraphSearchScratch) ([]vectorIndexCandidate, int, int) {
	if g.ordinalTieOrder {
		return g.searchCandidatesOrdinalTies(query, queryInvNorm, limit, &scratch.graph)
	}
	return g.searchCandidatesDocumentTies(query, queryInvNorm, limit, scratch)
}

func (g *ColumnVectorGraph) searchCandidatesOrdinalTies(query []float32, queryInvNorm float32, limit int, scratch *vectorIndexSearchScratch) ([]vectorIndexCandidate, int, int) {
	if g.entryPoint < 0 || g.entryPoint >= g.Rows() || limit <= 0 {
		return nil, 0, 0
	}
	visited, mark := scratch.nextVisitedEpoch(g.Rows())
	entry := vectorIndexCandidate{nodeID: g.entryPoint, distance: g.cosineDistance(query, queryInvNorm, g.entryPoint)}
	if math.IsInf(float64(entry.distance), 1) {
		return nil, 0, 1
	}
	visited[entry.nodeID] = mark
	queue := scratch.queue[:0]
	queue.push(entry)
	best := scratch.best[:0]
	best.pushBounded(entry, limit)
	edgesVisited := 0
	candidatesExamined := 1
	equalDistanceBridgeBudget := limit
	for len(queue) > 0 {
		current := queue.pop()
		if len(best) >= limit && current.distance > best[0].distance {
			break
		}
		start := int(g.neighborOffsets[current.nodeID])
		end := int(g.neighborOffsets[current.nodeID+1])
		edgesVisited += end - start
		for _, rawNeighbor := range g.neighbors[start:end] {
			neighbor := int(rawNeighbor)
			if visited[neighbor] == mark {
				continue
			}
			visited[neighbor] = mark
			candidate := vectorIndexCandidate{nodeID: neighbor, distance: g.cosineDistance(query, queryInvNorm, neighbor)}
			candidatesExamined++
			if math.IsInf(float64(candidate.distance), 1) {
				continue
			}
			if len(best) < limit || candidate.distance < best[0].distance {
				queue.push(candidate)
				best.pushBounded(candidate, limit)
				continue
			}
			if candidate.distance == best[0].distance {
				if vectorIndexCandidateLess(candidate, best[0]) {
					queue.push(candidate)
					best.pushBounded(candidate, limit)
				} else if equalDistanceBridgeBudget > 0 {
					queue.push(candidate)
					equalDistanceBridgeBudget--
				}
			}
		}
	}
	scratch.queue = queue[:0]
	scratch.best = best[:0]
	out := append(scratch.out[:0], best...)
	scratch.out = out
	sortVectorIndexCandidates(out)
	return out, edgesVisited, candidatesExamined
}

func (g *ColumnVectorGraph) searchCandidatesDocumentTies(query []float32, queryInvNorm float32, limit int, scratch *ColumnVectorGraphSearchScratch) ([]vectorIndexCandidate, int, int) {
	if g.entryPoint < 0 || g.entryPoint >= g.Rows() || limit <= 0 {
		return nil, 0, 0
	}
	visited, mark := scratch.graph.nextVisitedEpoch(g.Rows())
	entry := vectorIndexCandidate{nodeID: g.entryPoint, distance: g.cosineDistance(query, queryInvNorm, g.entryPoint)}
	if math.IsInf(float64(entry.distance), 1) {
		return nil, 0, 1
	}
	visited[entry.nodeID] = mark
	queue := scratch.queue[:0]
	queue.push(g, entry)
	best := scratch.best[:0]
	best.pushBounded(g, entry, limit)
	edgesVisited := 0
	candidatesExamined := 1
	equalDistanceBridgeBudget := limit
	for len(queue) > 0 {
		current := queue.pop(g)
		if len(best) >= limit && current.distance > best[0].distance {
			break
		}
		start := int(g.neighborOffsets[current.nodeID])
		end := int(g.neighborOffsets[current.nodeID+1])
		edgesVisited += end - start
		for _, rawNeighbor := range g.neighbors[start:end] {
			neighbor := int(rawNeighbor)
			if visited[neighbor] == mark {
				continue
			}
			visited[neighbor] = mark
			candidate := vectorIndexCandidate{nodeID: neighbor, distance: g.cosineDistance(query, queryInvNorm, neighbor)}
			candidatesExamined++
			if math.IsInf(float64(candidate.distance), 1) {
				continue
			}
			if len(best) < limit || candidate.distance < best[0].distance {
				queue.push(g, candidate)
				best.pushBounded(g, candidate, limit)
				continue
			}
			if candidate.distance == best[0].distance {
				if g.candidateLess(candidate, best[0]) {
					queue.push(g, candidate)
					best.pushBounded(g, candidate, limit)
				} else if equalDistanceBridgeBudget > 0 {
					queue.push(g, candidate)
					equalDistanceBridgeBudget--
				}
			}
		}
	}
	scratch.queue = queue[:0]
	scratch.best = best[:0]
	out := append(scratch.out[:0], best...)
	scratch.out = out
	g.sortCandidates(out)
	return out, edgesVisited, candidatesExamined
}

func (g *ColumnVectorGraph) sortCandidates(candidates []vectorIndexCandidate) {
	slices.SortFunc(candidates, g.compareCandidates)
}

func (g *ColumnVectorGraph) compareCandidates(left, right vectorIndexCandidate) int {
	if left.distance < right.distance {
		return -1
	}
	if left.distance > right.distance {
		return 1
	}
	leftRank := g.idRanks[left.nodeID]
	rightRank := g.idRanks[right.nodeID]
	switch {
	case leftRank < rightRank:
		return -1
	case leftRank > rightRank:
		return 1
	default:
		return 0
	}
}

func (g *ColumnVectorGraph) candidateLess(left, right vectorIndexCandidate) bool {
	return g.compareCandidates(left, right) < 0
}

func (g *ColumnVectorGraph) candidateWorse(left, right vectorIndexCandidate) bool {
	return g.candidateLess(right, left)
}

type columnVectorGraphMinCandidateHeap []vectorIndexCandidate

func (h *columnVectorGraphMinCandidateHeap) push(graph *ColumnVectorGraph, candidate vectorIndexCandidate) {
	*h = append(*h, candidate)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !graph.candidateLess((*h)[child], (*h)[parent]) {
			break
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *columnVectorGraphMinCandidateHeap) pop(graph *ColumnVectorGraph) vectorIndexCandidate {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	h.down(graph, 0)
	return out
}

func (h columnVectorGraphMinCandidateHeap) down(graph *ColumnVectorGraph, parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && graph.candidateLess(h[right], h[left]) {
			child = right
		}
		if !graph.candidateLess(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

type columnVectorGraphMaxCandidateHeap []vectorIndexCandidate

func (h *columnVectorGraphMaxCandidateHeap) pushBounded(graph *ColumnVectorGraph, candidate vectorIndexCandidate, limit int) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.up(graph, len(*h)-1)
		return
	}
	if !graph.candidateLess(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.down(graph, 0)
}

func (h *columnVectorGraphMaxCandidateHeap) up(graph *ColumnVectorGraph, child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !graph.candidateWorse((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h columnVectorGraphMaxCandidateHeap) down(graph *ColumnVectorGraph, parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && graph.candidateWorse(h[right], h[left]) {
			child = right
		}
		if !graph.candidateWorse(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

func (g *ColumnVectorGraph) cosineDistance(query []float32, queryInvNorm float32, ordinal int) float32 {
	if ordinal < 0 || ordinal >= g.Rows() {
		return float32(math.Inf(1))
	}
	vector := g.vectorAt(ordinal)
	dot := vectorDotProductFloat32(query, vector)
	if dot == 0 {
		if columnVectorGraphDotProductFloat64(query, vector) == 0 {
			return columnVectorGraphOrthogonalDistance
		}
		return columnVectorGraphCosineDistanceWide(query, vector, queryInvNorm, g.invNorms[ordinal])
	}
	if math.IsInf(float64(dot), 0) || math.IsNaN(float64(dot)) {
		return columnVectorGraphCosineDistanceWide(query, vector, queryInvNorm, g.invNorms[ordinal])
	}
	cosine := float64(dot) * float64(queryInvNorm) * float64(g.invNorms[ordinal])
	if math.IsInf(cosine, 0) || math.IsNaN(cosine) {
		return columnVectorGraphCosineDistanceWide(query, vector, queryInvNorm, g.invNorms[ordinal])
	}
	distance := 1 - cosine
	if math.IsInf(distance, 0) || math.IsNaN(distance) {
		return columnVectorGraphCosineDistanceWide(query, vector, queryInvNorm, g.invNorms[ordinal])
	}
	return float32(distance)
}

func columnVectorGraphDotProductFloat64(left []float32, right []float32) float64 {
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot
}

func columnVectorGraphCosineDistanceWide(query []float32, vector []float32, queryInvNorm float32, vectorInvNorm float32) float32 {
	dot := columnVectorGraphDotProductFloat64(query, vector)
	distance := 1 - dot*float64(queryInvNorm)*float64(vectorInvNorm)
	if math.IsNaN(distance) || math.IsInf(distance, 0) {
		return float32(math.Inf(1))
	}
	return float32(distance)
}

func (g *ColumnVectorGraph) vectorAt(ordinal int) []float32 {
	start := ordinal * g.dims
	return g.vectors[start : start+g.dims]
}

func (g *ColumnVectorGraph) documentID(ordinal int) []byte {
	start := g.idOffsets[ordinal]
	end := g.idOffsets[ordinal+1]
	return g.idArena[start:end:end]
}
