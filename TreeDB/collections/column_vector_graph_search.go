package collections

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// Keep modest scratch overgrowth to avoid realloc churn when callers vary
// TopK/EfSearch slightly, while still releasing oversized scratch after large
// probes.
const columnVectorGraphNativeScratchOversizeSlack = 16

// Default TopK values are small; insertion order avoids sort overhead there.
// Larger result sets switch to sort.Slice so result ordering does not go O(k^2).
const columnVectorGraphNativeResultOrderInsertionSortLimit = 32

var (
	errColumnVectorGraphNativeSearchScratchRequired            = errors.New("collections: column_graph native search requires caller-owned scratch")
	errColumnVectorGraphNativeSearchQueryDimensionMismatch     = errors.New("collections: column_graph native search query dimension mismatch")
	errColumnVectorGraphNativeSearchQueryNormInvalid           = errors.New("collections: column_graph native search query norm invalid")
	errColumnVectorGraphNativeSearchTopKNegative               = errors.New("collections: column_graph native search top_k cannot be negative")
	errColumnVectorGraphNativeSearchEfSearchNegative           = errors.New("collections: column_graph native search ef_search cannot be negative")
	errColumnVectorGraphNativeSearchCandidateDimensionMismatch = errors.New("collections: column_graph native search candidate dimension mismatch")
)

type columnVectorGraphNativeSearchOptions struct {
	TopK     int
	EfSearch int
}

type columnVectorGraphNativeSearchStats struct {
	Candidates              uint64
	Edges                   uint64
	CandidateFetches        uint64
	ExpansionFetches        uint64
	ResultFetches           uint64
	ScoreBatches            uint64
	OrdinalsGrouped         uint64
	BlockViewHits           uint64
	BlockViewMisses         uint64
	BlockViewBuilds         uint64
	AdjacencyExpansions     uint64
	AdjacencyScratchDecodes uint64
	AdjacencyDirectViews    uint64
}

// columnVectorGraphNativeSearchResult aliases buffers owned by the search
// scratch. Callers must copy the returned result slice and any retained result
// IDs before the next search with the same scratch.
type columnVectorGraphNativeSearchResult struct {
	Ordinal int
	ID      []byte
	Score   float64
}

type columnVectorGraphSearchCandidate struct {
	ordinal int
	score   float64
}

// columnVectorGraphNativeSearchScratch is caller-owned mutable search state.
// It is not concurrency-safe. Parallel searches over immutable graph assets are
// valid with one reader and one scratch per worker.
type columnVectorGraphNativeSearchScratch struct {
	scoreScratch   columnPhysicalRowReaderScratch
	expandScratch  columnPhysicalRowReaderScratch
	resultScratch  columnPhysicalRowReaderScratch
	visitMarks     []uint64
	visitEpoch     uint64
	frontier       []columnVectorGraphSearchCandidate
	top            []columnVectorGraphSearchCandidate
	results        []columnVectorGraphNativeSearchResult
	idBuffers      [][]byte
	resultOrder    []int
	resultOrdinals []int
	searchPlan     columnVectorGraphSearchPlan
}

func (s *columnVectorGraphNativeSearchScratch) prepare(rowCount, dimensions, degree, topK, efSearch int) error {
	if s == nil {
		return errColumnVectorGraphNativeSearchScratchRequired
	}
	if rowCount < 0 || dimensions < 0 || degree < 0 || topK < 0 || efSearch < 0 {
		return fmt.Errorf("collections: column_graph native search received negative sizing input: rowCount=%d dimensions=%d degree=%d topK=%d efSearch=%d", rowCount, dimensions, degree, topK, efSearch)
	}
	for _, rowScratch := range []*columnPhysicalRowReaderScratch{&s.scoreScratch, &s.expandScratch, &s.resultScratch} {
		prepareColumnVectorGraphNativeRowScratch(rowScratch, dimensions, degree)
	}
	s.visitMarks = resizeColumnVectorGraphNativeUint64Scratch(s.visitMarks, rowCount)
	s.visitEpoch++
	if s.visitEpoch == 0 {
		clear(s.visitMarks)
		s.visitEpoch = 1
	}
	frontierCap := efSearch
	if frontierCap > rowCount {
		frontierCap = rowCount
	}
	if frontierCap < topK {
		frontierCap = topK
	}
	s.frontier = resizeColumnVectorGraphNativeCandidateScratch(s.frontier, frontierCap)
	s.top = resizeColumnVectorGraphNativeCandidateScratch(s.top, topK)
	s.results = resizeColumnVectorGraphNativeResultScratch(s.results, topK)
	s.idBuffers = resizeColumnVectorGraphNativeIDBuffersScratch(s.idBuffers, topK)
	s.resultOrder = resizeColumnVectorGraphNativeIntScratch(s.resultOrder, topK)
	s.resultOrdinals = resizeColumnVectorGraphNativeIntScratch(s.resultOrdinals, topK)
	return nil
}

func prepareColumnVectorGraphNativeRowScratch(s *columnPhysicalRowReaderScratch, dimensions, degree int) {
	if cap(s.Values) < columnVectorGraphPhysicalRowValueCount {
		s.Values = make([]columnDeclaredValue, 0, columnVectorGraphPhysicalRowValueCount)
	} else {
		clear(s.Values)
		s.Values = s.Values[:0]
	}
	if cap(s.Float32Values) < dimensions {
		s.Float32Values = make([]float32, 0, dimensions)
	} else {
		s.Float32Values = s.Float32Values[:0]
	}
	if cap(s.Uint32Values) < degree {
		s.Uint32Values = make([]uint32, 0, degree)
	} else {
		s.Uint32Values = s.Uint32Values[:0]
	}
}

func resizeColumnVectorGraphNativeCandidateScratch(dst []columnVectorGraphSearchCandidate, target int) []columnVectorGraphSearchCandidate {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]columnVectorGraphSearchCandidate, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeResultScratch(dst []columnVectorGraphNativeSearchResult, target int) []columnVectorGraphNativeSearchResult {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		clear(dst)
		return make([]columnVectorGraphNativeSearchResult, 0, target)
	}
	if len(dst) > 0 {
		clear(dst)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeIntScratch(dst []int, target int) []int {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]int, 0, target)
	}
	return dst[:0]
}

func resizeColumnVectorGraphNativeUint64Scratch(dst []uint64, target int) []uint64 {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]uint64, target)
	}
	return dst[:target]
}

func resizeColumnVectorGraphNativeIDBuffersScratch(dst [][]byte, target int) [][]byte {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		next := make([][]byte, target)
		copy(next, dst)
		return next
	}
	if len(dst) < target {
		oldLen := len(dst)
		dst = dst[:target]
		clear(dst[oldLen:target])
		return dst
	}
	for i := target; i < len(dst); i++ {
		dst[i] = nil
	}
	return dst[:target]
}

func columnVectorGraphNativeScratchCapOversized(capacity, target int) bool {
	if target < 0 {
		return true
	}
	if target > (math.MaxInt-columnVectorGraphNativeScratchOversizeSlack)/2 {
		return false
	}
	return capacity > target*2+columnVectorGraphNativeScratchOversizeSlack
}

// SearchCosine traverses the persisted column graph through the physical row
// reader. It fetches only graph rows: document materialization stays outside
// this kernel. Returned results and result IDs alias scratch-owned buffers and
// must be copied before the next SearchCosine call with the same scratch.
func (r *columnVectorGraphPhysicalRowReader) SearchCosine(query []float32, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	if r == nil || r.reader == nil {
		return nil, columnVectorGraphNativeSearchStats{}, errNilColumnVectorGraphPhysicalRowReader
	}
	if len(query) != r.def.Dimensions {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	rowCount := r.RowCount()
	topK := opts.TopK
	if topK < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search top_k cannot be negative: %w", r.def.Name, errColumnVectorGraphNativeSearchTopKNegative)
	}
	efSearch := opts.EfSearch
	if efSearch < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search ef_search cannot be negative: %w", r.def.Name, errColumnVectorGraphNativeSearchEfSearchNegative)
	}
	if topK == 0 || rowCount == 0 {
		return nil, columnVectorGraphNativeSearchStats{}, nil
	}
	if scratch == nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query norm: %w: %w", r.def.Name, errColumnVectorGraphNativeSearchQueryNormInvalid, err)
	}
	if topK > rowCount {
		topK = rowCount
	}
	if efSearch == 0 {
		efSearch = r.def.EfSearch
	}
	if efSearch < topK {
		efSearch = topK
	}
	if efSearch > rowCount {
		efSearch = rowCount
	}
	degree := r.def.M
	if degree < 0 {
		degree = 0
	}
	if err := scratch.prepare(rowCount, r.def.Dimensions, degree, topK, efSearch); err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search scratch prepare: %w", r.def.Name, err)
	}

	var stats columnVectorGraphNativeSearchStats
	plan, err := scratch.prepareSearchPlan(r)
	if err != nil {
		return nil, stats, err
	}
	if err := r.scoreAndPushFrontier(plan, query, queryInvNorm, 0, topK, scratch, &stats); err != nil {
		return nil, stats, err
	}
	nextSeed := 0
	for stats.Candidates < uint64(efSearch) {
		candidate, ok := scratch.popFrontier()
		if !ok {
			for nextSeed < rowCount && scratch.visitMarks[nextSeed] == scratch.visitEpoch {
				nextSeed++
			}
			if nextSeed >= rowCount {
				break
			}
			if err := r.scoreAndPushFrontier(plan, query, queryInvNorm, nextSeed, topK, scratch, &stats); err != nil {
				return nil, stats, err
			}
			continue
		}
		adjacency, err := r.expandCandidateAdjacency(plan, candidate.ordinal, scratch, &stats)
		if err != nil {
			return nil, stats, err
		}
		for i, neighbor := range adjacency {
			if stats.Candidates >= uint64(efSearch) {
				break
			}
			stats.Edges++
			if err := validateColumnVectorGraphAdjacencyOrdinal(r.def.Name, candidate.ordinal, i, neighbor, rowCount); err != nil {
				return nil, stats, err
			}
			if err := r.scoreAndPushFrontier(plan, query, queryInvNorm, int(neighbor), topK, scratch, &stats); err != nil {
				return nil, stats, err
			}
		}
	}

	if len(scratch.top) == 0 {
		return scratch.results, stats, nil
	}
	if err := r.fetchTopSearchResults(plan, scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func (r *columnVectorGraphPhysicalRowReader) fetchTopSearchResults(plan *columnVectorGraphSearchPlan, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	n := len(scratch.top)
	scratch.resultOrder = scratch.resultOrder[:n]
	scratch.resultOrdinals = scratch.resultOrdinals[:n]
	for i := 0; i < n; i++ {
		scratch.resultOrder[i] = i
	}
	sortColumnVectorGraphResultOrderByOrdinal(scratch.resultOrder, scratch.top)
	for fetchPos, topIndex := range scratch.resultOrder {
		scratch.resultOrdinals[fetchPos] = scratch.top[topIndex].ordinal
	}
	for resultPos, topIndex := range scratch.resultOrder {
		ordinal := scratch.resultOrdinals[resultPos]
		view, ref, err := plan.blockViewForOrdinal(ordinal)
		if err != nil {
			return err
		}
		id, err := view.id(ref.rowIndex)
		if err != nil {
			return err
		}
		if cap(scratch.idBuffers[topIndex]) < len(id) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		} else if columnVectorGraphNativeScratchCapOversized(cap(scratch.idBuffers[topIndex]), len(id)) {
			scratch.idBuffers[topIndex] = make([]byte, len(id))
		}
		scratch.idBuffers[topIndex] = scratch.idBuffers[topIndex][:len(id)]
		copy(scratch.idBuffers[topIndex], id)
	}
	stats.ResultFetches += uint64(n)
	stats.BlockViewHits = plan.hits
	stats.BlockViewMisses = plan.misses
	stats.BlockViewBuilds = plan.builds
	for i, candidate := range scratch.top {
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
			Ordinal: candidate.ordinal,
			ID:      scratch.idBuffers[i],
			Score:   candidate.score,
		})
	}
	return nil
}

func sortColumnVectorGraphResultOrderByOrdinal(order []int, top []columnVectorGraphSearchCandidate) {
	if len(order) <= columnVectorGraphNativeResultOrderInsertionSortLimit {
		for i := 1; i < len(order); i++ {
			item := order[i]
			ordinal := top[item].ordinal
			j := i - 1
			for j >= 0 && top[order[j]].ordinal > ordinal {
				order[j+1] = order[j]
				j--
			}
			order[j+1] = item
		}
		return
	}
	sort.Slice(order, func(i, j int) bool {
		return top[order[i]].ordinal < top[order[j]].ordinal
	})
}

func (r *columnVectorGraphPhysicalRowReader) scoreAndPushFrontier(plan *columnVectorGraphSearchPlan, query []float32, queryInvNorm float32, ordinal, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	if scratch.markVisited(ordinal) {
		view, ref, err := plan.blockViewForOrdinal(ordinal)
		if err != nil {
			return err
		}
		stats.ScoreBatches++
		stats.OrdinalsGrouped++
		stats.BlockViewHits = plan.hits
		stats.BlockViewMisses = plan.misses
		stats.BlockViewBuilds = plan.builds
		scratch.scoreScratch.Float32Values = scratch.scoreScratch.Float32Values[:0]
		vector, vectorScratch, err := view.vector(ref.rowIndex, scratch.scoreScratch.Float32Values)
		if err != nil {
			return err
		}
		scratch.scoreScratch.Float32Values = vectorScratch
		invNorm, err := view.invNorm(ref.rowIndex)
		if err != nil {
			return err
		}
		stats.CandidateFetches++
		score, err := columnVectorGraphNativeCosineScore(query, queryInvNorm, columnVectorGraphPhysicalRow{Ordinal: ordinal, Vector: vector, InvNorm: invNorm})
		if err != nil {
			return err
		}
		if math.IsNaN(score) || math.IsInf(score, 0) {
			return fmt.Errorf("collections: column_graph %q candidate ordinal=%d cosine score is not finite", r.def.Name, ordinal)
		}
		stats.Candidates++
		candidate := columnVectorGraphSearchCandidate{
			ordinal: ordinal,
			score:   score,
		}
		scratch.insertTop(topK, candidate)
		scratch.pushFrontier(candidate)
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) expandCandidateAdjacency(plan *columnVectorGraphSearchPlan, ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]uint32, error) {
	view, ref, err := plan.blockViewForOrdinal(ordinal)
	if err != nil {
		return nil, err
	}
	scratch.expandScratch.Uint32Values = scratch.expandScratch.Uint32Values[:0]
	adjacency, adjacencyScratch, err := view.adjacency(ref.rowIndex, scratch.expandScratch.Uint32Values)
	if err != nil {
		return nil, err
	}
	scratch.expandScratch.Uint32Values = adjacencyScratch
	stats.AdjacencyExpansions++
	stats.AdjacencyScratchDecodes++
	stats.BlockViewHits = plan.hits
	stats.BlockViewMisses = plan.misses
	stats.BlockViewBuilds = plan.builds
	return adjacency, nil
}

func (s *columnVectorGraphNativeSearchScratch) markVisited(ordinal int) bool {
	if ordinal < 0 || ordinal >= len(s.visitMarks) || s.visitMarks[ordinal] == s.visitEpoch {
		return false
	}
	s.visitMarks[ordinal] = s.visitEpoch
	return true
}

func (s *columnVectorGraphNativeSearchScratch) pushFrontier(candidate columnVectorGraphSearchCandidate) {
	s.frontier = append(s.frontier, candidate)
	s.frontierSiftUp(len(s.frontier) - 1)
}

func (s *columnVectorGraphNativeSearchScratch) popFrontier() (columnVectorGraphSearchCandidate, bool) {
	if len(s.frontier) == 0 {
		return columnVectorGraphSearchCandidate{}, false
	}
	lastIdx := len(s.frontier) - 1
	best := s.frontier[0]
	last := s.frontier[lastIdx]
	s.frontier = s.frontier[:lastIdx]
	if len(s.frontier) > 0 {
		s.frontier[0] = last
		s.frontierSiftDown(0)
	}
	return best, true
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !columnVectorGraphSearchCandidateBetter(s.frontier[idx], s.frontier[parent]) {
			return
		}
		s.frontier[idx], s.frontier[parent] = s.frontier[parent], s.frontier[idx]
		idx = parent
	}
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftDown(idx int) {
	for {
		left := idx*2 + 1
		if left >= len(s.frontier) {
			return
		}
		child := left
		if right := left + 1; right < len(s.frontier) && columnVectorGraphSearchCandidateBetter(s.frontier[right], s.frontier[left]) {
			child = right
		}
		if !columnVectorGraphSearchCandidateBetter(s.frontier[child], s.frontier[idx]) {
			return
		}
		s.frontier[idx], s.frontier[child] = s.frontier[child], s.frontier[idx]
		idx = child
	}
}

func (s *columnVectorGraphNativeSearchScratch) insertTop(limit int, candidate columnVectorGraphSearchCandidate) {
	if limit <= 0 {
		return
	}
	pos := len(s.top)
	for pos > 0 && columnVectorGraphSearchCandidateBetter(candidate, s.top[pos-1]) {
		pos--
	}
	if pos >= limit {
		return
	}
	if len(s.top) < limit {
		s.top = append(s.top, columnVectorGraphSearchCandidate{})
	}
	copy(s.top[pos+1:], s.top[pos:len(s.top)-1])
	s.top[pos] = candidate
}

func columnVectorGraphSearchCandidateBetter(left, right columnVectorGraphSearchCandidate) bool {
	if left.score == right.score {
		return left.ordinal < right.ordinal
	}
	return left.score > right.score
}

func columnVectorGraphNativeCosineScore(query []float32, queryInvNorm float32, row columnVectorGraphPhysicalRow) (float64, error) {
	if len(row.Vector) != len(query) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d: %w", row.Ordinal, len(row.Vector), len(query), errColumnVectorGraphNativeSearchCandidateDimensionMismatch)
	}
	dot := float64(vectorDotProductFloat32(query, row.Vector))
	if dot != 0 && !math.IsInf(dot, 0) && !math.IsNaN(dot) {
		return dot * float64(queryInvNorm) * float64(row.InvNorm), nil
	}
	dot = columnVectorGraphNativeDotProductFloat64(query, row.Vector)
	return dot * float64(queryInvNorm) * float64(row.InvNorm), nil
}

func columnVectorGraphNativeDotProductFloat64(left, right []float32) float64 {
	var dot float64
	for i, v := range left {
		dot += float64(v) * float64(right[i])
	}
	return dot
}
