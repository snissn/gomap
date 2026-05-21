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
	rowScratch columnPhysicalRowReaderScratch
	visitMarks []uint64
	visitEpoch uint64
	queue      []int
	top        []columnVectorGraphSearchCandidate
	results    []columnVectorGraphNativeSearchResult
	idBuffers  [][]byte
}

func (s *columnVectorGraphNativeSearchScratch) prepare(rowCount, dimensions, degree, topK int) error {
	if s == nil {
		return errors.New("collections: column_graph native search requires caller-owned scratch")
	}
	if rowCount < 0 || dimensions < 0 || degree < 0 || topK < 0 {
		return errors.New("collections: column_graph native search received negative sizing input")
	}
	if cap(s.rowScratch.Values) < 3 {
		s.rowScratch.Values = make([]columnDeclaredValue, 0, 3)
	}
	if cap(s.rowScratch.Float32Values) < dimensions {
		s.rowScratch.Float32Values = make([]float32, 0, dimensions)
	}
	if cap(s.rowScratch.Uint32Values) < degree {
		s.rowScratch.Uint32Values = make([]uint32, 0, degree)
	}
	if len(s.visitMarks) < rowCount {
		s.visitMarks = make([]uint64, rowCount)
	}
	s.visitEpoch++
	if s.visitEpoch == 0 {
		clear(s.visitMarks)
		s.visitEpoch = 1
	}
	s.queue = s.queue[:0]
	if cap(s.queue) < rowCount {
		s.queue = make([]int, 0, rowCount)
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
	return nil
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
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q query norm: %w", r.def.Name, err)
	}
	rowCount := r.RowCount()
	topK := opts.TopK
	if topK < 0 {
		return nil, columnVectorGraphNativeSearchStats{}, errors.New("collections: column_graph native search top_k cannot be negative")
	}
	if topK == 0 || rowCount == 0 {
		if scratch != nil {
			if err := scratch.prepare(rowCount, r.def.Dimensions, r.def.M, 0); err != nil {
				return nil, columnVectorGraphNativeSearchStats{}, err
			}
		}
		return nil, columnVectorGraphNativeSearchStats{}, nil
	}
	if topK > rowCount {
		topK = rowCount
	}
	efSearch := opts.EfSearch
	if efSearch <= 0 {
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
	if err := scratch.prepare(rowCount, r.def.Dimensions, degree, topK); err != nil {
		return nil, columnVectorGraphNativeSearchStats{}, err
	}

	scratch.markAndEnqueue(0)
	var stats columnVectorGraphNativeSearchStats
	nextSeed := 0
	for head := 0; stats.Candidates < uint64(efSearch); {
		if head >= len(scratch.queue) {
			for nextSeed < rowCount && scratch.visitMarks[nextSeed] == scratch.visitEpoch {
				nextSeed++
			}
			if nextSeed >= rowCount {
				break
			}
			scratch.markAndEnqueue(nextSeed)
			continue
		}
		ordinal := scratch.queue[head]
		head++
		row, err := r.FetchRow(ordinal, &scratch.rowScratch)
		if err != nil {
			return nil, stats, err
		}
		stats.CandidateFetches++
		stats.Candidates++
		score := columnVectorGraphNativeCosineScore(query, queryInvNorm, row)
		if math.IsNaN(score) {
			return nil, stats, fmt.Errorf("collections: column_graph %q candidate ordinal=%d cosine score is NaN", r.def.Name, ordinal)
		}
		scratch.insertTop(topK, columnVectorGraphSearchCandidate{ordinal: ordinal, score: score})
		for _, neighbor := range row.Adjacency {
			stats.Edges++
			scratch.markAndEnqueue(int(neighbor))
		}
	}

	if len(scratch.top) == 0 {
		return scratch.results, stats, nil
	}
	for i, candidate := range scratch.top {
		row, err := r.FetchRow(candidate.ordinal, &scratch.rowScratch)
		if err != nil {
			return nil, stats, err
		}
		stats.ResultFetches++
		if cap(scratch.idBuffers[i]) < len(row.ID) {
			scratch.idBuffers[i] = make([]byte, len(row.ID))
		}
		scratch.idBuffers[i] = scratch.idBuffers[i][:len(row.ID)]
		copy(scratch.idBuffers[i], row.ID)
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
			Ordinal: candidate.ordinal,
			ID:      scratch.idBuffers[i],
			Score:   candidate.score,
		})
	}
	return scratch.results, stats, nil
}

func (s *columnVectorGraphNativeSearchScratch) markAndEnqueue(ordinal int) {
	if ordinal < 0 || ordinal >= len(s.visitMarks) || s.visitMarks[ordinal] == s.visitEpoch {
		return
	}
	s.visitMarks[ordinal] = s.visitEpoch
	s.queue = append(s.queue, ordinal)
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

func columnVectorGraphNativeCosineScore(query []float32, queryInvNorm float32, row columnVectorGraphPhysicalRow) float64 {
	var dot float64
	for i, v := range query {
		dot += float64(v) * float64(row.Vector[i])
	}
	return dot * float64(queryInvNorm) * float64(row.InvNorm)
}
