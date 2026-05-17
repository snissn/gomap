package colgranule

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

const (
	DefaultColumnVectorGraphIDColumn        = "id"
	DefaultColumnVectorGraphVectorColumn    = "embedding"
	DefaultColumnVectorGraphInvNormColumn   = "embedding_inv_norm"
	DefaultColumnVectorGraphAdjacencyColumn = "neighbors"
	DefaultColumnVectorGraphEfSearch        = 64
)

// ColumnVectorGraphOptions names the columns that back a single-layer ANN graph.
// Adjacency values are zero-based visible-row ordinals, matching the current
// TreeDB vector runtime's node IDs while keeping primary IDs as a separate column.
type ColumnVectorGraphOptions struct {
	IDColumn        string
	VectorColumn    string
	InvNormColumn   string
	AdjacencyColumn string
	EntryOrdinal    int
}

type ColumnVectorGraphLoadStats struct {
	Rows            int
	Dims            int
	Edges           int
	BlocksDecoded   int
	BytesDecoded    int
	VisibleRows     int
	PartsConsidered int
}

type ColumnVectorGraphSearchOptions struct {
	TopK     int
	EfSearch int
}

type ColumnVectorGraphSearchStats struct {
	Strategy           string
	TopK               int
	EfSearch           int
	CandidatesExamined int
	EdgesVisited       int
	Returned           int
}

type ColumnVectorGraphSearchResult struct {
	PrimaryID int64
	Ordinal   int
	Distance  float32
}

// ColumnVectorGraph is an immutable, single-layer ANN graph loaded from column
// store vector, inverse-norm, and adjacency-list columns.
type ColumnVectorGraph struct {
	ids              []int64
	dims             int
	vectors          []float32
	invNorms         []float32
	neighborOffsets  []uint32
	neighborOrdinals []int64
	entryOrdinal     int
}

// ColumnVectorGraphSearchScratch is caller-owned reusable search workspace.
// It is not safe for concurrent use. Results returned by SearchCosine may alias
// scratch memory and are invalidated by later SearchCosine calls using the same
// scratch; callers that need long-lived results should copy them.
type ColumnVectorGraphSearchScratch struct {
	visitedEpochs []uint32
	visitedEpoch  uint32
	queue         columnVectorGraphMinCandidateHeap
	best          columnVectorGraphMaxCandidateHeap
	out           []columnVectorGraphCandidate
	results       []ColumnVectorGraphSearchResult
}

type columnVectorGraphCandidate struct {
	ordinal  int
	distance float32
}

func NewColumnVectorGraphFromPartSet(reader *ColumnPartSetReader, opts ColumnVectorGraphOptions) (*ColumnVectorGraph, ColumnVectorGraphLoadStats, error) {
	if reader == nil {
		return nil, ColumnVectorGraphLoadStats{}, errors.New("colgranule: nil column vector graph reader")
	}
	opts = normalizeColumnVectorGraphOptions(opts)
	ids, err := reader.ScanProjected([]string{opts.IDColumn})
	if err != nil {
		return nil, ColumnVectorGraphLoadStats{}, err
	}
	vectors, err := reader.ScanFloat32VectorsInto(opts.VectorColumn, nil)
	if err != nil {
		return nil, ColumnVectorGraphLoadStats{}, err
	}
	invNorms, err := reader.ScanFloat32VectorsInto(opts.InvNormColumn, nil)
	if err != nil {
		return nil, ColumnVectorGraphLoadStats{}, err
	}
	neighbors, err := reader.ScanAdjacencyListsInto(opts.AdjacencyColumn, nil, nil)
	if err != nil {
		return nil, ColumnVectorGraphLoadStats{}, err
	}

	rows := ids.Rows
	if rows <= 0 {
		return nil, ColumnVectorGraphLoadStats{}, errors.New("colgranule: column vector graph is empty")
	}
	if len(ids.Columns[opts.IDColumn]) != rows {
		return nil, ColumnVectorGraphLoadStats{}, fmt.Errorf("colgranule: graph id values=%d want rows=%d", len(ids.Columns[opts.IDColumn]), rows)
	}
	if vectors.Rows != rows {
		return nil, ColumnVectorGraphLoadStats{}, fmt.Errorf("colgranule: graph vector rows=%d want %d", vectors.Rows, rows)
	}
	if vectors.Dims <= 0 {
		return nil, ColumnVectorGraphLoadStats{}, fmt.Errorf("colgranule: graph vector dims=%d", vectors.Dims)
	}
	if invNorms.Rows != rows || invNorms.Dims != 1 {
		return nil, ColumnVectorGraphLoadStats{}, fmt.Errorf("colgranule: graph inv-norm shape=(%d,%d) want (%d,1)", invNorms.Rows, invNorms.Dims, rows)
	}
	if neighbors.Rows != rows {
		return nil, ColumnVectorGraphLoadStats{}, fmt.Errorf("colgranule: graph adjacency rows=%d want %d", neighbors.Rows, rows)
	}
	if len(neighbors.Offsets) != rows+1 {
		return nil, ColumnVectorGraphLoadStats{}, fmt.Errorf("colgranule: graph adjacency offsets=%d want %d", len(neighbors.Offsets), rows+1)
	}
	if opts.EntryOrdinal < 0 || opts.EntryOrdinal >= rows {
		return nil, ColumnVectorGraphLoadStats{}, fmt.Errorf("colgranule: graph entry ordinal=%d outside rows=%d", opts.EntryOrdinal, rows)
	}
	if err := validateColumnVectorGraphStorage(vectors.Values, vectors.Dims, invNorms.Values, neighbors.Offsets, neighbors.Values, rows); err != nil {
		return nil, ColumnVectorGraphLoadStats{}, err
	}

	stats := ColumnVectorGraphLoadStats{
		Rows:            rows,
		Dims:            vectors.Dims,
		Edges:           len(neighbors.Values),
		BlocksDecoded:   ids.Diagnostics.BlocksDecoded + vectors.Diagnostics.BlocksDecoded + invNorms.Diagnostics.BlocksDecoded + neighbors.Diagnostics.BlocksDecoded,
		BytesDecoded:    ids.Diagnostics.BytesDecoded + vectors.Diagnostics.BytesDecoded + invNorms.Diagnostics.BytesDecoded + neighbors.Diagnostics.BytesDecoded,
		VisibleRows:     reader.VisibilityStats().VisibleRows,
		PartsConsidered: neighbors.Diagnostics.PartsConsidered,
	}
	graph := &ColumnVectorGraph{
		ids:              ids.Columns[opts.IDColumn],
		dims:             vectors.Dims,
		vectors:          vectors.Values,
		invNorms:         invNorms.Values,
		neighborOffsets:  neighbors.Offsets,
		neighborOrdinals: neighbors.Values,
		entryOrdinal:     opts.EntryOrdinal,
	}
	return graph, stats, nil
}

// Rows returns the number of rows in the graph.
func (g *ColumnVectorGraph) Rows() int {
	if g == nil {
		return 0
	}
	return len(g.ids)
}

// Dims returns the vector dimensionality of each graph row.
func (g *ColumnVectorGraph) Dims() int {
	if g == nil {
		return 0
	}
	return g.dims
}

// EntryOrdinal returns the graph entry row ordinal.
func (g *ColumnVectorGraph) EntryOrdinal() int {
	if g == nil {
		return -1
	}
	return g.entryOrdinal
}

// PrimaryIDs appends the graph primary IDs to dst and returns the extended slice.
func (g *ColumnVectorGraph) PrimaryIDs(dst []int64) []int64 {
	if g == nil {
		return dst
	}
	return append(dst, g.ids...)
}

// VectorAt appends the vector for ordinal to dst and reports whether it exists.
func (g *ColumnVectorGraph) VectorAt(dst []float32, ordinal int) ([]float32, bool) {
	if g == nil || ordinal < 0 || ordinal >= len(g.ids) {
		return dst, false
	}
	start := ordinal * g.dims
	return append(dst, g.vectors[start:start+g.dims]...), true
}

// NeighborOffsets appends the CSR adjacency offsets to dst.
func (g *ColumnVectorGraph) NeighborOffsets(dst []uint32) []uint32 {
	if g == nil {
		return dst
	}
	return append(dst, g.neighborOffsets...)
}

// NeighborOrdinals appends the CSR adjacency ordinals to dst.
func (g *ColumnVectorGraph) NeighborOrdinals(dst []int64) []int64 {
	if g == nil {
		return dst
	}
	return append(dst, g.neighborOrdinals...)
}

// SearchCosine runs cosine ANN search over the column-backed graph. If scratch
// is non-nil, returned results may alias scratch memory and are invalidated by
// subsequent calls that reuse that scratch.
func (g *ColumnVectorGraph) SearchCosine(query []float32, opts ColumnVectorGraphSearchOptions, scratch *ColumnVectorGraphSearchScratch) ([]ColumnVectorGraphSearchResult, ColumnVectorGraphSearchStats, error) {
	stats := ColumnVectorGraphSearchStats{Strategy: "column_graph_cosine"}
	if g == nil {
		return nil, stats, errors.New("colgranule: nil column vector graph")
	}
	if opts.TopK <= 0 {
		return nil, stats, errors.New("colgranule: column vector graph TopK must be positive")
	}
	if len(query) != g.dims {
		return nil, stats, fmt.Errorf("colgranule: column vector graph query dims=%d want %d", len(query), g.dims)
	}
	queryInvNorm, err := columnVectorGraphQueryInvNorm(query)
	if err != nil {
		return nil, stats, err
	}
	efSearch := opts.EfSearch
	if efSearch <= 0 {
		efSearch = DefaultColumnVectorGraphEfSearch
	}
	if efSearch < opts.TopK {
		efSearch = opts.TopK
	}
	stats.TopK = opts.TopK
	stats.EfSearch = efSearch
	if scratch == nil {
		scratch = &ColumnVectorGraphSearchScratch{}
	}

	candidates, edgesVisited, candidatesExamined := g.searchCandidates(query, queryInvNorm, efSearch, scratch)
	stats.CandidatesExamined = candidatesExamined
	stats.EdgesVisited = edgesVisited
	if cap(scratch.results) < opts.TopK {
		scratch.results = make([]ColumnVectorGraphSearchResult, 0, opts.TopK)
	}
	results := scratch.results[:0]
	for _, candidate := range candidates {
		if len(results) >= opts.TopK {
			break
		}
		results = append(results, ColumnVectorGraphSearchResult{
			PrimaryID: g.ids[candidate.ordinal],
			Ordinal:   candidate.ordinal,
			Distance:  candidate.distance,
		})
	}
	scratch.results = results
	stats.Returned = len(results)
	if results == nil {
		results = []ColumnVectorGraphSearchResult{}
	}
	return results, stats, nil
}

func normalizeColumnVectorGraphOptions(opts ColumnVectorGraphOptions) ColumnVectorGraphOptions {
	if opts.IDColumn == "" {
		opts.IDColumn = DefaultColumnVectorGraphIDColumn
	}
	if opts.VectorColumn == "" {
		opts.VectorColumn = DefaultColumnVectorGraphVectorColumn
	}
	if opts.InvNormColumn == "" {
		opts.InvNormColumn = DefaultColumnVectorGraphInvNormColumn
	}
	if opts.AdjacencyColumn == "" {
		opts.AdjacencyColumn = DefaultColumnVectorGraphAdjacencyColumn
	}
	return opts
}

func validateColumnVectorGraphStorage(vectors []float32, dims int, invNorms []float32, offsets []uint32, neighbors []int64, rows int) error {
	vectorValues, err := checkedMulInt(rows, dims, "column vector graph values")
	if err != nil {
		return err
	}
	if len(vectors) != vectorValues {
		return fmt.Errorf("colgranule: graph vector values=%d want %d", len(vectors), vectorValues)
	}
	if len(invNorms) != rows {
		return fmt.Errorf("colgranule: graph inv-norm values=%d want %d", len(invNorms), rows)
	}
	if len(offsets) != rows+1 {
		return fmt.Errorf("colgranule: graph offsets=%d want %d", len(offsets), rows+1)
	}
	if offsets[0] != 0 {
		return fmt.Errorf("colgranule: graph first adjacency offset=%d want 0", offsets[0])
	}
	prev := uint32(0)
	for row := 0; row < rows; row++ {
		next := offsets[row+1]
		if next < prev {
			return fmt.Errorf("colgranule: graph adjacency offset row=%d value=%d before previous=%d", row+1, next, prev)
		}
		prev = next
		invNorm := invNorms[row]
		if invNorm <= 0 || !columnVectorGraphFinite(invNorm) {
			return fmt.Errorf("colgranule: graph inv-norm row=%d value=%g is invalid", row, invNorm)
		}
		start := row * dims
		var normSquared float64
		for dim := 0; dim < dims; dim++ {
			value := vectors[start+dim]
			if !columnVectorGraphFinite(value) {
				return fmt.Errorf("colgranule: graph vector row=%d dim=%d value=%g is invalid", row, dim, value)
			}
			normSquared += float64(value) * float64(value)
		}
		if normSquared == 0 {
			return fmt.Errorf("colgranule: graph vector row=%d has zero magnitude", row)
		}
		if err := validateColumnVectorGraphInvNorm(row, invNorm, normSquared); err != nil {
			return err
		}
	}
	if int(prev) != len(neighbors) {
		return fmt.Errorf("colgranule: graph final adjacency offset=%d values=%d", prev, len(neighbors))
	}
	for edge, neighbor := range neighbors {
		if neighbor < 0 || neighbor >= int64(rows) {
			return fmt.Errorf("colgranule: graph edge %d neighbor ordinal=%d outside rows=%d", edge, neighbor, rows)
		}
	}
	return nil
}

func validateColumnVectorGraphInvNorm(row int, invNorm float32, normSquared float64) error {
	expected := 1 / math.Sqrt(normSquared)
	diff := math.Abs(float64(invNorm) - expected)
	allowed := math.Max(1e-5, math.Abs(expected)*1e-4)
	if diff > allowed {
		return fmt.Errorf("colgranule: graph inv-norm row=%d value=%g does not match vector norm=%g", row, invNorm, expected)
	}
	return nil
}

func columnVectorGraphQueryInvNorm(query []float32) (float32, error) {
	var normSquared float64
	for dim, value := range query {
		if !columnVectorGraphFinite(value) {
			return 0, fmt.Errorf("colgranule: graph query dim=%d value=%g is invalid", dim, value)
		}
		normSquared += float64(value) * float64(value)
	}
	if normSquared == 0 {
		return 0, errors.New("colgranule: cosine column vector graph query cannot have zero magnitude")
	}
	return float32(1 / math.Sqrt(normSquared)), nil
}

func columnVectorGraphFinite(value float32) bool {
	return !math.IsNaN(float64(value)) && !math.IsInf(float64(value), 0)
}

func (g *ColumnVectorGraph) searchCandidates(query []float32, queryInvNorm float32, limit int, scratch *ColumnVectorGraphSearchScratch) ([]columnVectorGraphCandidate, int, int) {
	if g.entryOrdinal < 0 || g.entryOrdinal >= len(g.ids) || limit <= 0 {
		return nil, 0, 0
	}
	visited, mark := scratch.nextVisitedEpoch(len(g.ids))
	entry := columnVectorGraphCandidate{
		ordinal:  g.entryOrdinal,
		distance: g.cosineDistance(query, queryInvNorm, g.entryOrdinal),
	}
	if math.IsInf(float64(entry.distance), 1) {
		return nil, 0, 1
	}
	visited[entry.ordinal] = mark
	queue := scratch.queue[:0]
	queue.push(entry)
	best := scratch.best[:0]
	best.pushBounded(entry, limit)
	edgesVisited := 0
	candidatesExamined := 1
	for len(queue) > 0 {
		current := queue.pop()
		if len(best) >= limit && columnVectorGraphCandidateWorse(current, best[0]) {
			break
		}
		start := int(g.neighborOffsets[current.ordinal])
		end := int(g.neighborOffsets[current.ordinal+1])
		edgesVisited += end - start
		for _, rawNeighbor := range g.neighborOrdinals[start:end] {
			neighbor := int(rawNeighbor)
			if visited[neighbor] == mark {
				continue
			}
			visited[neighbor] = mark
			candidate := columnVectorGraphCandidate{
				ordinal:  neighbor,
				distance: g.cosineDistance(query, queryInvNorm, neighbor),
			}
			candidatesExamined++
			if math.IsInf(float64(candidate.distance), 1) {
				continue
			}
			if len(best) < limit || columnVectorGraphCandidateLess(candidate, best[0]) {
				queue.push(candidate)
				best.pushBounded(candidate, limit)
			}
		}
	}
	scratch.queue = queue[:0]
	scratch.best = best[:0]
	out := append(scratch.out[:0], best...)
	scratch.out = out
	sortColumnVectorGraphCandidates(out)
	return out, edgesVisited, candidatesExamined
}

func (g *ColumnVectorGraph) cosineDistance(query []float32, queryInvNorm float32, ordinal int) float32 {
	if ordinal < 0 || ordinal >= len(g.ids) {
		return float32(math.Inf(1))
	}
	start := ordinal * g.dims
	vector := g.vectors[start : start+g.dims]
	dot := columnVectorGraphDotProductFloat32(query, vector)
	return 1 - dot*queryInvNorm*g.invNorms[ordinal]
}

func (scratch *ColumnVectorGraphSearchScratch) nextVisitedEpoch(nodes int) ([]uint32, uint32) {
	if cap(scratch.visitedEpochs) < nodes {
		scratch.visitedEpochs = make([]uint32, nodes, growColumnVectorGraphScratchCapacity(cap(scratch.visitedEpochs), nodes))
	} else {
		scratch.visitedEpochs = scratch.visitedEpochs[:nodes]
	}
	scratch.visitedEpoch++
	if scratch.visitedEpoch == 0 {
		clear(scratch.visitedEpochs)
		scratch.visitedEpoch = 1
	}
	return scratch.visitedEpochs, scratch.visitedEpoch
}

func growColumnVectorGraphScratchCapacity(current int, required int) int {
	next := current
	if next < 64 {
		next = 64
	}
	for next < required {
		next *= 2
	}
	return next
}

func sortColumnVectorGraphCandidates(candidates []columnVectorGraphCandidate) {
	slices.SortFunc(candidates, func(left, right columnVectorGraphCandidate) int {
		switch {
		case left.distance < right.distance:
			return -1
		case left.distance > right.distance:
			return 1
		case left.ordinal < right.ordinal:
			return -1
		case left.ordinal > right.ordinal:
			return 1
		default:
			return 0
		}
	})
}

func columnVectorGraphCandidateLess(left, right columnVectorGraphCandidate) bool {
	if left.distance != right.distance {
		return left.distance < right.distance
	}
	return left.ordinal < right.ordinal
}

func columnVectorGraphCandidateWorse(left, right columnVectorGraphCandidate) bool {
	return columnVectorGraphCandidateLess(right, left)
}

type columnVectorGraphMinCandidateHeap []columnVectorGraphCandidate

func (h *columnVectorGraphMinCandidateHeap) push(candidate columnVectorGraphCandidate) {
	*h = append(*h, candidate)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !columnVectorGraphCandidateLess((*h)[child], (*h)[parent]) {
			break
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *columnVectorGraphMinCandidateHeap) pop() columnVectorGraphCandidate {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	h.down(0)
	return out
}

func (h columnVectorGraphMinCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && columnVectorGraphCandidateLess(h[right], h[left]) {
			child = right
		}
		if !columnVectorGraphCandidateLess(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

type columnVectorGraphMaxCandidateHeap []columnVectorGraphCandidate

func (h *columnVectorGraphMaxCandidateHeap) pushBounded(candidate columnVectorGraphCandidate, limit int) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.up(len(*h) - 1)
		return
	}
	if !columnVectorGraphCandidateLess(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.down(0)
}

func (h *columnVectorGraphMaxCandidateHeap) up(child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !columnVectorGraphCandidateWorse((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h columnVectorGraphMaxCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && columnVectorGraphCandidateWorse(h[right], h[left]) {
			child = right
		}
		if !columnVectorGraphCandidateWorse(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}
