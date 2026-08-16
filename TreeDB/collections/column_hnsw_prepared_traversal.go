package collections

import (
	"errors"
	"fmt"
	"math"
)

var errColumnHNSWPreparedTraversalScorePlaneUnavailable = errors.New("collections: hnsw prepared traversal score plane unavailable")

type columnHNSWPreparedTraversalScorePlaneKind uint8

const (
	columnHNSWPreparedTraversalScorePlaneKindExactFP32 columnHNSWPreparedTraversalScorePlaneKind = iota + 1
	columnHNSWPreparedTraversalScorePlaneKindQuantized
)

func (k columnHNSWPreparedTraversalScorePlaneKind) String() string {
	switch k {
	case columnHNSWPreparedTraversalScorePlaneKindExactFP32:
		return "exact_fp32"
	case columnHNSWPreparedTraversalScorePlaneKindQuantized:
		return "quantized"
	default:
		return "unknown"
	}
}

type columnHNSWPreparedTraversalOptions struct {
	TopK     int
	EfSearch int

	// RetainedCandidateLimit optionally asks traversal to retain more score-plane
	// candidates than the final TopK. Zero uses normalized ef_search. This keeps
	// the seam usable by future quantized_rerank callers without adding rerank
	// semantics to the traversal itself.
	RetainedCandidateLimit int

	ScoreBatchMode columnVectorGraphScoreBatchMode
	StatsMode      columnVectorGraphNativeSearchStatsMode

	// OmitResultMaterialization returns ordinal/score candidates without reading
	// result IDs or row refs from the pack. Future quantized rerank callers can use
	// this to collect an explicit score-plane shortlist before exact reranking.
	OmitResultMaterialization bool
	// SuppressOmittedResultMaterialization keeps the retained candidates in
	// scratch.top for internal rerank callers without also appending ordinal-only
	// results that the caller will immediately discard.
	SuppressOmittedResultMaterialization bool
}

// columnHNSWPreparedTraversalScorePlane is the explicit scoring seam for the
// pack-style HNSW traversal. Implementations are per-query/per-scratch objects:
// exact FP32 owns a normalized-query scratch slice, and quantized score planes
// own codec-prepared query state. Immutable pack adjacency/result identity stays
// in columnHNSWSearchPackPreparedView and is not reinterpreted as quantized
// storage.
type columnHNSWPreparedTraversalScorePlane interface {
	kind() columnHNSWPreparedTraversalScorePlaneKind
	prepareForHNSWPreparedTraversal(pack *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch) error
	scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error)
	scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error)
}

// columnHNSWPreparedTraversalRowIDScorePlane is the direct row-ID scoring seam
// for prepared-pack traversal. Callers must validate every row ID against the
// pack row count before invoking these methods. That lets scalar_u8 mirror the
// exact FP32 pack route by scoring adjacency []uint32 directly instead of
// staging through []int ordinals and the codec-generic quantized scorer.
type columnHNSWPreparedTraversalRowIDScorePlane interface {
	scoreRowIDsPrevalidated(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error)
	scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error)
}

// columnHNSWPreparedTraversalRowIDGreedyScorePlane is the scalar_u8 prepared
// traversal fusion seam for upper layers. It scores already-validated adjacency
// row IDs and updates the greedy best directly, preserving the float64 score
// formula and lower-ordinal tie behavior without staging a float64 score slice
// followed by a second generic compare loop. It intentionally fuses the default
// float64 prepared traversal because the raw-dot scalar_u8 traversal remains
// default-off/no-promote until it is separately proven neutral or faster.
type columnHNSWPreparedTraversalRowIDGreedyScorePlane interface {
	scoreGreedyBestRowIDsPrevalidated(rowIDs []uint32, best int, bestScore float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (newBest int, newBestScore float64, changed bool, err error)
}

// columnHNSWPreparedTraversalRawDotScorePlane is the scalar_u8-only ordering
// seam for prepared traversal. It keeps raw centered dot products in the
// frontier/top-k queues and converts to public float64 scores only when leaving
// traversal for result materialization, exact rerank, or a generic score seam.
type columnHNSWPreparedTraversalRawDotScorePlane interface {
	scoreRawDotOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int64, error)
	scoreRawDotRowIDsPrevalidated(rowIDs []uint32, dst []int64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]int64, error)
	scoreAndPushRawDotFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error)
}

type columnHNSWPreparedExactFP32ScorePlane struct {
	pack            *columnHNSWSearchPackPreparedView
	normalizedQuery []float32
	scoreBatchMode  columnVectorGraphScoreBatchMode
}

func (p *columnHNSWPreparedExactFP32ScorePlane) kind() columnHNSWPreparedTraversalScorePlaneKind {
	return columnHNSWPreparedTraversalScorePlaneKindExactFP32
}

func (p *columnHNSWPreparedExactFP32ScorePlane) prepareForHNSWPreparedTraversal(pack *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch) error {
	if p == nil || pack == nil || scratch == nil {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal exact query norm: %w: %w", errColumnVectorGraphNativeSearchQueryNormInvalid, err)
	}
	if cap(scratch.scoreScratch.Float32Values) < pack.Header.VectorStride {
		scratch.scoreScratch.Float32Values = ensureColumnVectorGraphNativeFloat32Scratch(scratch.scoreScratch.Float32Values, pack.Header.VectorStride)
	}
	// The exact score plane intentionally borrows scoreScratch.Float32Values
	// for the normalized query. Pack scoring methods must treat this slice as
	// read-only while the plane is active and use scoreTile* fields for staging;
	// allocating a per-query copy would violate the zero-allocation seam contract.
	normalizedQuery := scratch.scoreScratch.Float32Values[:pack.Header.VectorStride]
	for i := 0; i < pack.Header.Dimensions; i++ {
		normalizedQuery[i] = query[i] * queryInvNorm
	}
	if pack.Header.VectorStride > pack.Header.Dimensions {
		clear(normalizedQuery[pack.Header.Dimensions:])
	}
	p.pack = pack
	p.normalizedQuery = normalizedQuery
	p.scoreBatchMode = opts.ScoreBatchMode
	return nil
}

func (p *columnHNSWPreparedExactFP32ScorePlane) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if p == nil || p.pack == nil || len(p.normalizedQuery) < p.pack.Header.VectorStride {
		return 0, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.pack.scoreOrdinal(p.normalizedQuery, ordinal, p.scoreBatchMode, scratch, stats)
}

func (p *columnHNSWPreparedExactFP32ScorePlane) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if p == nil || p.pack == nil || len(p.normalizedQuery) < p.pack.Header.VectorStride {
		return dst[:0], errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if scratch == nil {
		return dst[:0], errColumnVectorGraphNativeSearchScratchRequired
	}
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	if len(ordinals) == 0 {
		return dst, nil
	}
	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, len(ordinals))
	rowIDs := scratch.scoreTileRowIDs[:len(ordinals)]
	for i, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= p.pack.Header.Rows || uint64(ordinal) > math.MaxUint32 {
			return dst[:i], fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal exact ordinal=%d outside row_count=%d", ordinal, p.pack.Header.Rows)
		}
		rowIDs[i] = uint32(ordinal)
	}
	return p.scoreRowIDsPrevalidated(rowIDs, dst, scratch, stats)
}

func (p *columnHNSWPreparedExactFP32ScorePlane) scoreRowIDsPrevalidated(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if p == nil || p.pack == nil || len(p.normalizedQuery) < p.pack.Header.VectorStride {
		return dst[:0], errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.pack.scoreRowIDs(p.normalizedQuery, rowIDs, dst, p.scoreBatchMode, scratch, stats)
}

func (p *columnHNSWPreparedExactFP32ScorePlane) scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	if p == nil || p.pack == nil || len(p.normalizedQuery) < p.pack.Header.VectorStride {
		return 0, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	var visited uint64
	if err := p.pack.scoreAndPushFrontierVisitedTile(p.normalizedQuery, rowIDs, topK, p.scoreBatchMode, scratch, stats, &visited, nil, nil); err != nil {
		return int(visited), err
	}
	return int(visited), nil
}

type columnHNSWPreparedQuantizedScorePlane struct {
	scorer *columnVectorGraphQuantizedScorer
}

func (p *columnHNSWPreparedQuantizedScorePlane) kind() columnHNSWPreparedTraversalScorePlaneKind {
	return columnHNSWPreparedTraversalScorePlaneKindQuantized
}

func (p *columnHNSWPreparedQuantizedScorePlane) prepareForHNSWPreparedTraversal(pack *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch) error {
	_ = pack
	_ = query
	_ = opts
	_ = scratch
	if p == nil || p.scorer == nil || p.scorer.kind == columnVectorGraphQuantizedScorerKindNone {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return nil
}

func (p *columnHNSWPreparedQuantizedScorePlane) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if p == nil || p.scorer == nil {
		return 0, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreOrdinal(ordinal, scratch, stats)
}

func (p *columnHNSWPreparedQuantizedScorePlane) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if p == nil || p.scorer == nil {
		return dst[:0], errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreOrdinals(ordinals, dst, scratch, stats)
}

func (v *columnHNSWSearchPackPreparedView) searchCosinePreparedScorePlane(query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch, scorePlane columnHNSWPreparedTraversalScorePlane) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	if v == nil {
		return nil, columnVectorGraphNativeSearchStats{}, errColumnHNSWSearchPackSearchUnavailable
	}
	switch status := v.fastStatus(""); status {
	case columnHNSWSearchPackPreparedStatusDirect, columnHNSWSearchPackPreparedStatusHeap:
	default:
		return nil, columnVectorGraphNativeSearchStats{}, columnHNSWSearchPackStatusError(status)
	}
	if scratch == nil {
		return nil, columnVectorGraphNativeSearchStats{}, errColumnVectorGraphNativeSearchScratchRequired
	}
	stats := &scratch.preparedTraversalStats
	*stats = columnVectorGraphNativeSearchStats{}
	if scorePlane == nil {
		return nil, *stats, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if len(query) != v.Header.Dimensions {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal query dims=%d want %d: %w", len(query), v.Header.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	statsMode := opts.StatsMode.normalized()
	if !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		return nil, *stats, errColumnHNSWSearchPackSearchUnsupportedMode
	}
	columnVectorGraphNativeSearchStartWorkAccounting(stats, statsMode)
	rowCount := v.Header.Rows
	topK := opts.TopK
	if topK < 0 {
		return nil, *stats, errColumnVectorGraphNativeSearchTopKNegative
	}
	efSearch := opts.EfSearch
	if efSearch < 0 {
		return nil, *stats, errColumnVectorGraphNativeSearchEfSearchNegative
	}
	retainedCandidateLimit := opts.RetainedCandidateLimit
	if retainedCandidateLimit < 0 {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal retained candidates=%d cannot be negative", retainedCandidateLimit)
	}
	if topK == 0 || rowCount == 0 {
		return nil, *stats, nil
	}
	stats.CandidateRows = uint64(rowCount)
	if topK > rowCount {
		topK = rowCount
	}
	if efSearch == 0 {
		efSearch = v.Header.EfSearch
	}
	if efSearch < topK {
		efSearch = topK
	}
	if efSearch > rowCount {
		efSearch = rowCount
	}
	if retainedCandidateLimit == 0 {
		retainedCandidateLimit = efSearch
	}
	if retainedCandidateLimit < topK {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal retained candidates=%d below normalized top_k=%d", retainedCandidateLimit, topK)
	}
	if retainedCandidateLimit > rowCount {
		retainedCandidateLimit = rowCount
	}
	degree := v.Header.M
	if degree < 1 {
		degree = 1
	}
	if degree <= math.MaxInt/2 {
		degree *= 2
	}
	resultScratchK := topK
	if opts.OmitResultMaterialization && !opts.SuppressOmittedResultMaterialization {
		resultScratchK = retainedCandidateLimit
	}
	if err := scratch.prepareHNSWSearchPack(rowCount, v.Header.VectorStride, degree, resultScratchK, retainedCandidateLimit, degree, degree); err != nil {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal scratch prepare: %w", err)
	}
	if err := scorePlane.prepareForHNSWPreparedTraversal(v, query, opts, scratch); err != nil {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal %s score plane: %w", scorePlane.kind(), err)
	}
	rowIDScorePlane, _ := scorePlane.(columnHNSWPreparedTraversalRowIDScorePlane)
	countLoopEdges := !statsMode.minimal()
	var loopEdgeVisits uint64
	var visitedCandidates uint64
	entryOrdinal := v.Header.EntryOrdinal
	if entryOrdinal < 0 || entryOrdinal >= rowCount {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal entry ordinal=%d outside rows=%d", entryOrdinal, rowCount)
	}
	maxLayer, err := v.maxLayerForOrdinal(entryOrdinal)
	if err != nil {
		return nil, *stats, err
	}
	traversalStart, traversalDistanceBefore := columnVectorGraphNativeSearchStartGraphTraversal(stats)
	for layer := maxLayer; layer > 0; layer-- {
		entryOrdinal, err = v.greedyNearestAtLayerPreparedScorePlane(scorePlane, rowIDScorePlane, entryOrdinal, layer, scratch, stats, countLoopEdges, &loopEdgeVisits)
		if err != nil {
			return nil, *stats, err
		}
	}
	visitMarks := scratch.visitMarks
	visitEpoch := scratch.visitEpoch
	visitMarks[entryOrdinal] = visitEpoch
	if err := v.scoreAndPushFrontierVisitedPreparedScorePlane(scorePlane, entryOrdinal, retainedCandidateLimit, scratch, stats, &visitedCandidates); err != nil {
		return nil, *stats, err
	}
	nextSeed := 0
	rowCount64 := uint64(rowCount)
	for {
		candidate, ok := scratch.popFrontierAccounting(stats)
		if !ok {
			if len(scratch.top) >= retainedCandidateLimit {
				break
			}
			seed, seedOK := columnHNSWSearchPackNextCandidateSeed(nextSeed, rowCount, visitMarks, visitEpoch)
			if !seedOK {
				break
			}
			nextSeed = seed + 1
			visitMarks[seed] = visitEpoch
			if err := v.scoreAndPushFrontierVisitedPreparedScorePlane(scorePlane, seed, retainedCandidateLimit, scratch, stats, &visitedCandidates); err != nil {
				return nil, *stats, err
			}
			continue
		}
		if columnVectorGraphLayer0SearchShouldStop(candidate, scratch.top, retainedCandidateLimit) {
			break
		}
		adjacency, err := v.adjacencyLayerForOrdinal(candidate.ordinal, 0, stats)
		if err != nil {
			return nil, *stats, err
		}
		if len(adjacency) == 0 {
			continue
		}
		if countLoopEdges {
			loopEdgeVisits += uint64(len(adjacency))
		}
		if rowIDScorePlane != nil {
			scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, len(adjacency))
			tile := scratch.scoreTileRowIDs[:0]
			for i, neighbor := range adjacency {
				if uint64(neighbor) >= rowCount64 {
					return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
				}
				neighborOrdinal := int(neighbor)
				if visitMarks[neighborOrdinal] == visitEpoch {
					continue
				}
				visitMarks[neighborOrdinal] = visitEpoch
				tile = append(tile, neighbor)
			}
			if len(tile) != 0 {
				scored, err := rowIDScorePlane.scoreAndPushFrontierVisitedRowIDsPrevalidated(tile, retainedCandidateLimit, scratch, stats)
				if err != nil {
					return nil, *stats, err
				}
				visitedCandidates += uint64(scored)
			}
			continue
		}
		scratch.scoreTileOrdinals = ensureColumnVectorGraphNativeIntScratch(scratch.scoreTileOrdinals, len(adjacency))
		tile := scratch.scoreTileOrdinals[:0]
		for i, neighbor := range adjacency {
			if uint64(neighbor) >= rowCount64 {
				return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
			neighborOrdinal := int(neighbor)
			if visitMarks[neighborOrdinal] == visitEpoch {
				continue
			}
			visitMarks[neighborOrdinal] = visitEpoch
			tile = append(tile, neighborOrdinal)
		}
		if len(tile) != 0 {
			if err := v.scoreAndPushFrontierVisitedTilePreparedScorePlane(scorePlane, tile, retainedCandidateLimit, scratch, stats, &visitedCandidates); err != nil {
				return nil, *stats, err
			}
		}
	}
	if countLoopEdges {
		stats.Edges = loopEdgeVisits
		stats.VisitedEdges = loopEdgeVisits
		stats.Candidates = visitedCandidates
	}
	columnVectorGraphNativeSearchFinishGraphTraversal(stats, traversalStart, traversalDistanceBefore)
	if len(scratch.top) == 0 {
		return scratch.results, *stats, nil
	}
	if opts.OmitResultMaterialization {
		if opts.SuppressOmittedResultMaterialization {
			return scratch.results, *stats, nil
		}
		scratch.retainTopBestFirst(retainedCandidateLimit)
		for _, candidate := range scratch.top {
			scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, Score: candidate.score})
		}
		return scratch.results, *stats, nil
	}
	scratch.retainTopBestFirst(topK)
	if err := v.fetchTopSearchResults(scratch, stats); err != nil {
		return nil, *stats, err
	}
	return scratch.results, *stats, nil
}

func (v *columnHNSWSearchPackPreparedView) greedyNearestAtLayerPreparedScorePlane(scorePlane columnHNSWPreparedTraversalScorePlane, rowIDScorePlane columnHNSWPreparedTraversalRowIDScorePlane, entryOrdinal int, layer int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, countLoopEdges bool, loopEdgeVisits *uint64) (int, error) {
	greedyScorePlane, _ := rowIDScorePlane.(columnHNSWPreparedTraversalRowIDGreedyScorePlane)
	best := entryOrdinal
	bestScore, err := scorePlane.scoreOrdinal(best, scratch, stats)
	if err != nil {
		return 0, err
	}
	changed := true
	for changed {
		changed = false
		adjacency, err := v.adjacencyLayerForOrdinal(best, layer, stats)
		if err != nil {
			return 0, err
		}
		if len(adjacency) == 0 {
			continue
		}
		if countLoopEdges && loopEdgeVisits != nil {
			*loopEdgeVisits += uint64(len(adjacency))
		}
		for i, neighbor := range adjacency {
			if uint64(neighbor) >= uint64(v.Header.Rows) {
				return 0, fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal ordinal=%d layer=%d adjacency[%d]=%d outside row_count=%d: %w", best, layer, i, neighbor, v.Header.Rows, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
		}
		if greedyScorePlane != nil {
			newBest, newBestScore, greedyChanged, err := greedyScorePlane.scoreGreedyBestRowIDsPrevalidated(adjacency, best, bestScore, scratch, stats)
			if err != nil {
				return 0, err
			}
			if greedyChanged {
				best = newBest
				bestScore = newBestScore
				changed = true
			}
			continue
		}
		if rowIDScorePlane != nil {
			scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(adjacency))
			scores, err := rowIDScorePlane.scoreRowIDsPrevalidated(adjacency, scratch.scoreTileScores, scratch, stats)
			if err != nil {
				return 0, err
			}
			for i, neighbor := range adjacency {
				neighborOrdinal := int(neighbor)
				score := scores[i]
				// Keep exact-pack tie handling stable with the current route.
				if score > bestScore || (score == bestScore && neighborOrdinal < best) {
					best = neighborOrdinal
					bestScore = score
					changed = true
				}
			}
			continue
		}
		scratch.scoreTileOrdinals = ensureColumnVectorGraphNativeIntScratch(scratch.scoreTileOrdinals, len(adjacency))
		tile := scratch.scoreTileOrdinals[:0]
		for _, neighbor := range adjacency {
			tile = append(tile, int(neighbor))
		}
		scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(tile))
		scores, err := scorePlane.scoreOrdinals(tile, scratch.scoreTileScores, scratch, stats)
		if err != nil {
			return 0, err
		}
		for i, neighborOrdinal := range tile {
			score := scores[i]
			// Keep exact-pack tie handling stable with the current route.
			if score > bestScore || (score == bestScore && neighborOrdinal < best) {
				best = neighborOrdinal
				bestScore = score
				changed = true
			}
		}
	}
	return best, nil
}

func (v *columnHNSWSearchPackPreparedView) scoreAndPushFrontierVisitedPreparedScorePlane(scorePlane columnHNSWPreparedTraversalScorePlane, ordinal, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64) error {
	_ = v
	score, err := scorePlane.scoreOrdinal(ordinal, scratch, stats)
	if err != nil {
		return err
	}
	if visitedCandidates != nil {
		(*visitedCandidates)++
	}
	candidate := columnVectorGraphSearchCandidate{ordinal: ordinal, score: score}
	if scratch.insertTop(topK, candidate) {
		scratch.pushFrontierAccounting(candidate, stats)
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) scoreAndPushFrontierVisitedTilePreparedScorePlane(scorePlane columnHNSWPreparedTraversalScorePlane, ordinals []int, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64) error {
	_ = v
	if len(ordinals) == 0 {
		return nil
	}
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(ordinals))
	scores, err := scorePlane.scoreOrdinals(ordinals, scratch.scoreTileScores, scratch, stats)
	if err != nil {
		return err
	}
	if visitedCandidates != nil {
		*visitedCandidates += uint64(len(ordinals))
	}
	for i, ordinal := range ordinals {
		candidate := columnVectorGraphSearchCandidate{ordinal: ordinal, score: scores[i]}
		if scratch.insertTop(topK, candidate) {
			scratch.pushFrontierAccounting(candidate, stats)
		}
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) exactRerankPreparedTraversalRowIDCandidates(query []float32, topK int, rerankLimit int, scoreBatchMode columnVectorGraphScoreBatchMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	if v == nil {
		return errColumnHNSWSearchPackSearchUnavailable
	}
	if scratch == nil {
		return errColumnVectorGraphNativeSearchScratchRequired
	}
	if len(scratch.top) == 0 || topK <= 0 || rerankLimit <= 0 {
		scratch.top = scratch.top[:0]
		return nil
	}
	scratch.retainTopBestFirst(rerankLimit)
	if len(scratch.top) < topK {
		topK = len(scratch.top)
	}
	n := len(scratch.top)
	scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, n)
	rowIDs := scratch.scoreTileRowIDs[:0]
	for _, candidate := range scratch.top {
		ordinal := candidate.ordinal
		if ordinal < 0 || ordinal >= v.Header.Rows || uint64(ordinal) > math.MaxUint32 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal exact rerank ordinal=%d outside row_count=%d", ordinal, v.Header.Rows)
		}
		rowIDs = append(rowIDs, uint32(ordinal))
	}
	var exactPlane columnHNSWPreparedExactFP32ScorePlane
	if err := exactPlane.prepareForHNSWPreparedTraversal(v, query, columnHNSWPreparedTraversalOptions{ScoreBatchMode: scoreBatchMode}, scratch); err != nil {
		return err
	}
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, n)
	exactScores, err := exactPlane.scoreRowIDsPrevalidated(rowIDs, scratch.scoreTileScores, scratch, stats)
	if err != nil {
		return err
	}
	if len(exactScores) != n {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal exact rerank scored %d candidates want %d", len(exactScores), n)
	}
	if stats != nil {
		n64 := uint64(n)
		stats.QuantizedRerankCandidates += n64
		stats.QuantizedRerankExactScoreCalls += n64
		// The pack-native exact scorer reads normalized vectors directly, but this
		// quantized_rerank route still reports the logical exact FP32 vector+norm
		// byte contract exposed by the generic exact rerank path.
		stats.NormBytesRead += n64 * 4
	}
	scratch.top = scratch.top[:0]
	for i, rowID := range rowIDs {
		candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: exactScores[i]}
		scratch.insertTop(topK, candidate)
	}
	scratch.sortTopBestFirst()
	return nil
}

func (v *columnHNSWSearchPackPreparedView) exactRerankPreparedTraversalCandidates(query []float32, topK int, scoreBatchMode columnVectorGraphScoreBatchMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	if v == nil {
		return errColumnHNSWSearchPackSearchUnavailable
	}
	if scratch == nil {
		return errColumnVectorGraphNativeSearchScratchRequired
	}
	if len(scratch.top) == 0 || topK <= 0 {
		scratch.top = scratch.top[:0]
		return nil
	}
	if len(scratch.top) < topK {
		topK = len(scratch.top)
	}
	scratch.sortTopBestFirst()
	n := len(scratch.top)
	scratch.scoreTileOrdinals = ensureColumnVectorGraphNativeIntScratch(scratch.scoreTileOrdinals, n)
	ordinals := scratch.scoreTileOrdinals[:0]
	for _, candidate := range scratch.top {
		ordinals = append(ordinals, candidate.ordinal)
	}
	var exactPlane columnHNSWPreparedExactFP32ScorePlane
	if err := exactPlane.prepareForHNSWPreparedTraversal(v, query, columnHNSWPreparedTraversalOptions{ScoreBatchMode: scoreBatchMode}, scratch); err != nil {
		return err
	}
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, n)
	exactScores, err := exactPlane.scoreOrdinals(ordinals, scratch.scoreTileScores, scratch, stats)
	if err != nil {
		return err
	}
	if len(exactScores) != n {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared traversal exact rerank scored %d candidates want %d", len(exactScores), n)
	}
	if stats != nil {
		stats.QuantizedRerankCandidates += uint64(n)
		stats.QuantizedRerankExactScoreCalls += uint64(n)
	}
	scratch.top = scratch.top[:0]
	for i, ordinal := range ordinals {
		candidate := columnVectorGraphSearchCandidate{ordinal: ordinal, score: exactScores[i]}
		scratch.insertTop(topK, candidate)
	}
	scratch.sortTopBestFirst()
	return nil
}
