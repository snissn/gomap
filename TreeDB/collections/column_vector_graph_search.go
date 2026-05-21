package collections

import (
	"errors"
	"fmt"
	"math"
)

type columnVectorGraphNativeSearchOptions struct {
	TopK     int
	EfSearch int
}

type columnVectorGraphNativeSearchStats struct {
	Candidates       uint64
	Edges            uint64
	CandidateFetches uint64
	ExpansionFetches uint64
	ResultFetches    uint64
}

// columnVectorGraphNativeSearchResult aliases buffers owned by the search
// scratch. Copy ID before the next search with the same scratch if retention is
// required.
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
}

func (s *columnVectorGraphNativeSearchScratch) prepare(rowCount, dimensions, degree, topK, efSearch int) error {
	if s == nil {
		return errors.New("collections: column_graph native search requires caller-owned scratch")
	}
	if rowCount < 0 || dimensions < 0 || degree < 0 || topK < 0 || efSearch < 0 {
		return errors.New("collections: column_graph native search received negative sizing input")
	}
	for _, rowScratch := range []*columnPhysicalRowReaderScratch{&s.scoreScratch, &s.expandScratch, &s.resultScratch} {
		prepareColumnVectorGraphNativeRowScratch(rowScratch, dimensions, degree)
	}
	if cap(s.visitMarks) < rowCount {
		s.visitMarks = make([]uint64, rowCount)
	} else {
		s.visitMarks = s.visitMarks[:rowCount]
	}
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
	if cap(s.frontier) < frontierCap || cap(s.frontier) > frontierCap*2+16 {
		s.frontier = make([]columnVectorGraphSearchCandidate, 0, frontierCap)
	} else {
		s.frontier = s.frontier[:0]
	}
	s.top = s.top[:0]
	if cap(s.top) < topK {
		s.top = make([]columnVectorGraphSearchCandidate, 0, topK)
	}
	s.results = s.results[:0]
	if cap(s.results) < topK {
		s.results = make([]columnVectorGraphNativeSearchResult, 0, topK)
	}
	if len(s.idBuffers) < topK {
		next := make([][]byte, topK)
		copy(next, s.idBuffers)
		s.idBuffers = next
	}
	if cap(s.resultOrder) < topK {
		s.resultOrder = make([]int, 0, topK)
	}
	if cap(s.resultOrdinals) < topK {
		s.resultOrdinals = make([]int, 0, topK)
	}
	return nil
}

func prepareColumnVectorGraphNativeRowScratch(s *columnPhysicalRowReaderScratch, dimensions, degree int) {
	if cap(s.Values) < 3 {
		s.Values = make([]columnDeclaredValue, 0, 3)
	}
	if cap(s.Float32Values) < dimensions {
		s.Float32Values = make([]float32, 0, dimensions)
	}
	if cap(s.Uint32Values) < degree {
		s.Uint32Values = make([]uint32, 0, degree)
	}
}

// SearchCosine traverses the persisted column graph through the physical row
// reader. It fetches only graph rows: document materialization stays outside
// this kernel.
func (r *columnVectorGraphPhysicalRowReader) SearchCosine(query []float32, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	if r == nil || r.reader == nil {
		return nil, columnVectorGraphNativeSearchStats{}, errors.New("collections: nil column vector graph physical row reader")
	}
	if len(query) != r.def.Dimensions {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query dims=%d want %d", r.def.Name, len(query), r.def.Dimensions)
	}
	rowCount := r.RowCount()
	topK := opts.TopK
	if topK < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, errors.New("collections: column_graph native search top_k cannot be negative")
	}
	if topK == 0 || rowCount == 0 {
		return nil, columnVectorGraphNativeSearchStats{}, nil
	}
	if scratch == nil {
		return nil, columnVectorGraphNativeSearchStats{}, errors.New("collections: column_graph native search requires caller-owned scratch")
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query norm: %w", r.def.Name, err)
	}
	if topK > rowCount {
		topK = rowCount
	}
	efSearch := opts.EfSearch
	if efSearch < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, errors.New("collections: column_graph native search ef_search cannot be negative")
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
		return nil, columnVectorGraphNativeSearchStats{}, err
	}

	var stats columnVectorGraphNativeSearchStats
	if err := r.scoreAndPushFrontier(query, queryInvNorm, 0, topK, scratch, &stats); err != nil {
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
			if err := r.scoreAndPushFrontier(query, queryInvNorm, nextSeed, topK, scratch, &stats); err != nil {
				return nil, stats, err
			}
			continue
		}
		row, err := r.FetchRow(candidate.ordinal, &scratch.expandScratch)
		if err != nil {
			return nil, stats, err
		}
		stats.ExpansionFetches++
		for _, neighbor := range row.Adjacency {
			if stats.Candidates >= uint64(efSearch) {
				break
			}
			stats.Edges++
			if err := r.scoreAndPushFrontier(query, queryInvNorm, int(neighbor), topK, scratch, &stats); err != nil {
				return nil, stats, err
			}
		}
	}

	if len(scratch.top) == 0 {
		return scratch.results, stats, nil
	}
	if err := r.fetchTopSearchResults(scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func (r *columnVectorGraphPhysicalRowReader) fetchTopSearchResults(scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	n := len(scratch.top)
	scratch.resultOrder = scratch.resultOrder[:n]
	scratch.resultOrdinals = scratch.resultOrdinals[:n]
	for i := 0; i < n; i++ {
		scratch.resultOrder[i] = i
	}
	for i := 1; i < n; i++ {
		order := scratch.resultOrder[i]
		ordinal := scratch.top[order].ordinal
		j := i - 1
		for j >= 0 && scratch.top[scratch.resultOrder[j]].ordinal > ordinal {
			scratch.resultOrder[j+1] = scratch.resultOrder[j]
			j--
		}
		scratch.resultOrder[j+1] = order
	}
	for fetchPos, topIndex := range scratch.resultOrder {
		scratch.resultOrdinals[fetchPos] = scratch.top[topIndex].ordinal
	}
	fetchPos := 0
	if err := r.FetchBatch(scratch.resultOrdinals, &scratch.resultScratch, func(row columnVectorGraphPhysicalRow) error {
		if fetchPos >= n {
			return fmt.Errorf("collections: column_graph %q result batch returned extra row ordinal=%d", r.def.Name, row.Ordinal)
		}
		topIndex := scratch.resultOrder[fetchPos]
		candidate := scratch.top[topIndex]
		if row.Ordinal != candidate.ordinal {
			return fmt.Errorf("collections: column_graph %q result batch row ordinal=%d want %d", r.def.Name, row.Ordinal, candidate.ordinal)
		}
		if cap(scratch.idBuffers[topIndex]) < len(row.ID) {
			scratch.idBuffers[topIndex] = make([]byte, len(row.ID))
		}
		scratch.idBuffers[topIndex] = scratch.idBuffers[topIndex][:len(row.ID)]
		copy(scratch.idBuffers[topIndex], row.ID)
		fetchPos++
		return nil
	}); err != nil {
		return err
	}
	if fetchPos != n {
		return fmt.Errorf("collections: column_graph %q result batch rows=%d want %d", r.def.Name, fetchPos, n)
	}
	stats.ResultFetches += uint64(n)
	for i, candidate := range scratch.top {
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
			Ordinal: candidate.ordinal,
			ID:      scratch.idBuffers[i],
			Score:   candidate.score,
		})
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) scoreAndPushFrontier(query []float32, queryInvNorm float32, ordinal, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	if scratch.markVisited(ordinal) {
		row, err := r.FetchRow(ordinal, &scratch.scoreScratch)
		if err != nil {
			return err
		}
		stats.CandidateFetches++
		score, err := columnVectorGraphNativeCosineScore(query, queryInvNorm, row)
		if err != nil {
			return err
		}
		if math.IsNaN(score) {
			return fmt.Errorf("collections: column_graph %q candidate ordinal=%d cosine score is NaN", r.def.Name, ordinal)
		}
		stats.Candidates++
		candidate := columnVectorGraphSearchCandidate{ordinal: ordinal, score: score}
		scratch.insertTop(topK, candidate)
		scratch.pushFrontier(candidate)
	}
	return nil
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
	best := s.frontier[0]
	last := s.frontier[len(s.frontier)-1]
	s.frontier = s.frontier[:len(s.frontier)-1]
	if len(s.frontier) > 0 {
		s.frontier[0] = last
		s.frontierSiftDown(0)
	}
	return best, true
}

func (s *columnVectorGraphNativeSearchScratch) frontierSiftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !columnVectorGraphSearchCandidateLess(s.frontier[idx], s.frontier[parent]) {
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
		if right := left + 1; right < len(s.frontier) && columnVectorGraphSearchCandidateLess(s.frontier[right], s.frontier[left]) {
			child = right
		}
		if !columnVectorGraphSearchCandidateLess(s.frontier[child], s.frontier[idx]) {
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
	for pos > 0 && columnVectorGraphSearchCandidateLess(candidate, s.top[pos-1]) {
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

func columnVectorGraphSearchCandidateLess(left, right columnVectorGraphSearchCandidate) bool {
	if left.score == right.score {
		return left.ordinal < right.ordinal
	}
	return left.score > right.score
}

func columnVectorGraphNativeCosineScore(query []float32, queryInvNorm float32, row columnVectorGraphPhysicalRow) (float64, error) {
	if len(row.Vector) != len(query) {
		return 0, fmt.Errorf("collections: column_graph candidate ordinal=%d vector dims=%d want %d", row.Ordinal, len(row.Vector), len(query))
	}
	var dot float64
	for i, v := range query {
		dot += float64(v) * float64(row.Vector[i])
	}
	return dot * float64(queryInvNorm) * float64(row.InvNorm), nil
}
