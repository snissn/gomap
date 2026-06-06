package collections

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

var (
	errColumnHNSWSearchPackSearchUnavailable     = errors.New("collections: hnsw_search_pack_v1 search unavailable")
	errColumnHNSWSearchPackSearchUnsupportedMode = errors.New("collections: hnsw_search_pack_v1 search supports only exact no-document mode")
	errColumnHNSWSearchPackSearchCandidateRows   = errors.New("collections: hnsw_search_pack_v1 search does not support candidate row filters")
)

func columnHNSWSearchPackStatsModeSupportedForSearch(mode columnVectorGraphNativeSearchStatsMode) bool {
	return mode.normalized() != columnVectorGraphNativeSearchStatsModeBenchmarkDebug
}

func (v *columnHNSWSearchPackPreparedView) searchCosine(query []float32, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	var stats columnVectorGraphNativeSearchStats
	if v == nil {
		return nil, stats, errColumnHNSWSearchPackSearchUnavailable
	}
	switch status := v.fastStatus(""); status {
	case columnHNSWSearchPackPreparedStatusDirect, columnHNSWSearchPackPreparedStatusHeap:
	default:
		return nil, stats, columnHNSWSearchPackStatusError(status)
	}
	if scratch == nil {
		return nil, stats, errColumnVectorGraphNativeSearchScratchRequired
	}
	if len(query) != v.Header.Dimensions {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 query dims=%d want %d: %w", len(query), v.Header.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	queryMode, err := opts.QueryMode.normalized()
	if err != nil {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 query mode: %w", err)
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeExact || opts.QuantizedIndexName != "" || opts.QuantizedRerankCandidates != 0 {
		return nil, stats, errColumnHNSWSearchPackSearchUnsupportedMode
	}
	if opts.HasCandidateRows {
		return nil, stats, errColumnHNSWSearchPackSearchCandidateRows
	}
	statsMode := opts.StatsMode.normalized()
	if !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		return nil, stats, errColumnHNSWSearchPackSearchUnsupportedMode
	}
	rowCount := v.Header.Rows
	topK := opts.TopK
	if topK < 0 {
		return nil, stats, errColumnVectorGraphNativeSearchTopKNegative
	}
	efSearch := opts.EfSearch
	if efSearch < 0 {
		return nil, stats, errColumnVectorGraphNativeSearchEfSearchNegative
	}
	if topK == 0 || rowCount == 0 {
		return nil, stats, nil
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
	degree := v.Header.M
	if degree < 1 {
		degree = 1
	}
	if degree <= math.MaxInt/2 {
		degree *= 2
	}
	if err := scratch.prepareHNSWSearchPack(rowCount, v.Header.VectorStride, degree, topK, efSearch); err != nil {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 search scratch prepare: %w", err)
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 query norm: %w: %w", errColumnVectorGraphNativeSearchQueryNormInvalid, err)
	}
	normalizedQuery := scratch.scoreScratch.Float32Values[:v.Header.VectorStride]
	for i := 0; i < v.Header.Dimensions; i++ {
		normalizedQuery[i] = query[i] * queryInvNorm
	}
	if v.Header.VectorStride > v.Header.Dimensions {
		clear(normalizedQuery[v.Header.Dimensions:])
	}
	countLoopEdges := !statsMode.minimal()
	var loopEdgeVisits uint64
	var visitedCandidates uint64
	entryOrdinal := v.Header.EntryOrdinal
	if entryOrdinal < 0 || entryOrdinal >= rowCount {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 entry ordinal=%d outside rows=%d", entryOrdinal, rowCount)
	}
	maxLayer, err := v.maxLayerForOrdinal(entryOrdinal)
	if err != nil {
		return nil, stats, err
	}
	for layer := maxLayer; layer > 0; layer-- {
		entryOrdinal, err = v.greedyNearestAtLayer(normalizedQuery, entryOrdinal, layer, opts.ScoreBatchMode, scratch, &stats, countLoopEdges, &loopEdgeVisits)
		if err != nil {
			return nil, stats, err
		}
	}
	visitMarks := scratch.visitMarks
	visitEpoch := scratch.visitEpoch
	visitMarks[entryOrdinal] = visitEpoch
	if err := v.scoreAndPushFrontierVisited(normalizedQuery, entryOrdinal, efSearch, opts.ScoreBatchMode, scratch, &stats, &visitedCandidates); err != nil {
		return nil, stats, err
	}
	nextSeed := 0
	rowCount64 := uint64(rowCount)
	for {
		candidate, ok := scratch.popFrontier()
		if !ok {
			if len(scratch.top) >= efSearch {
				break
			}
			seed, seedOK := columnHNSWSearchPackNextCandidateSeed(nextSeed, rowCount, visitMarks, visitEpoch)
			if !seedOK {
				break
			}
			nextSeed = seed + 1
			visitMarks[seed] = visitEpoch
			if err := v.scoreAndPushFrontierVisited(normalizedQuery, seed, efSearch, opts.ScoreBatchMode, scratch, &stats, &visitedCandidates); err != nil {
				return nil, stats, err
			}
			continue
		}
		if columnVectorGraphLayer0SearchShouldStop(candidate, scratch.top, efSearch) {
			break
		}
		adjacency, err := v.adjacencyLayerForOrdinal(candidate.ordinal, 0, &stats)
		if err != nil {
			return nil, stats, err
		}
		if len(adjacency) == 0 {
			continue
		}
		scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, len(adjacency))
		tile := scratch.scoreTileRowIDs[:0]
		for i, neighbor := range adjacency {
			if countLoopEdges {
				loopEdgeVisits++
			}
			if uint64(neighbor) >= rowCount64 {
				return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
			neighborOrdinal := int(neighbor)
			if visitMarks[neighborOrdinal] == visitEpoch {
				continue
			}
			visitMarks[neighborOrdinal] = visitEpoch
			tile = append(tile, neighbor)
		}
		if len(tile) != 0 {
			if err := v.scoreAndPushFrontierVisitedTile(normalizedQuery, tile, efSearch, opts.ScoreBatchMode, scratch, &stats, &visitedCandidates); err != nil {
				return nil, stats, err
			}
		}
	}
	if countLoopEdges {
		stats.Edges = loopEdgeVisits
		stats.VisitedEdges = loopEdgeVisits
		stats.Candidates = visitedCandidates
	}
	if len(scratch.top) == 0 {
		return scratch.results, stats, nil
	}
	if len(scratch.top) > topK {
		scratch.top = scratch.top[:topK]
	}
	if opts.OmitResultMaterialization {
		for _, candidate := range scratch.top {
			scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, Score: candidate.score})
		}
		return scratch.results, stats, nil
	}
	if err := v.fetchTopSearchResults(scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func columnHNSWSearchPackNextCandidateSeed(start int, rowCount int, visitMarks []uint64, visitEpoch uint64) (int, bool) {
	if start < 0 {
		start = 0
	}
	for ordinal := start; ordinal < rowCount; ordinal++ {
		if len(visitMarks) > ordinal && visitMarks[ordinal] == visitEpoch {
			continue
		}
		return ordinal, true
	}
	return 0, false
}

func (v *columnHNSWSearchPackPreparedView) maxLayerForOrdinal(ordinal int) (int, error) {
	if v == nil || ordinal < 0 || ordinal >= len(v.Levels) {
		return 0, fmt.Errorf("collections: hnsw_search_pack_v1 max-layer ordinal=%d unavailable", ordinal)
	}
	return int(v.Levels[ordinal]), nil
}

func (v *columnHNSWSearchPackPreparedView) greedyNearestAtLayer(normalizedQuery []float32, entryOrdinal int, layer int, scoreBatchMode columnVectorGraphScoreBatchMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, countLoopEdges bool, loopEdgeVisits *uint64) (int, error) {
	best := entryOrdinal
	bestScore, err := v.scoreOrdinal(normalizedQuery, best, scoreBatchMode, scratch, stats)
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
		for i, neighbor := range adjacency {
			if countLoopEdges && loopEdgeVisits != nil {
				(*loopEdgeVisits)++
			}
			if uint64(neighbor) >= uint64(v.Header.Rows) {
				return 0, fmt.Errorf("collections: hnsw_search_pack_v1 ordinal=%d layer=%d adjacency[%d]=%d outside row_count=%d: %w", best, layer, i, neighbor, v.Header.Rows, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
		}
		scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(adjacency))
		scores, err := v.scoreRowIDs(normalizedQuery, adjacency, scratch.scoreTileScores, scoreBatchMode, scratch, stats)
		if err != nil {
			return 0, err
		}
		for i, neighborRowID := range adjacency {
			neighborOrdinal := int(neighborRowID)
			score := scores[i]
			if score > bestScore || (score == bestScore && neighborOrdinal < best) {
				best = neighborOrdinal
				bestScore = score
				changed = true
			}
		}
	}
	return best, nil
}

func (v *columnHNSWSearchPackPreparedView) scoreAndPushFrontierVisited(normalizedQuery []float32, ordinal, topK int, scoreBatchMode columnVectorGraphScoreBatchMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64) error {
	score, err := v.scoreOrdinal(normalizedQuery, ordinal, scoreBatchMode, scratch, stats)
	if err != nil {
		return err
	}
	if visitedCandidates != nil {
		(*visitedCandidates)++
	}
	candidate := columnVectorGraphSearchCandidate{ordinal: ordinal, score: score}
	if scratch.insertTop(topK, candidate) {
		scratch.pushFrontier(candidate)
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) scoreAndPushFrontierVisitedTile(normalizedQuery []float32, rowIDs []uint32, topK int, scoreBatchMode columnVectorGraphScoreBatchMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64) error {
	if len(rowIDs) == 0 {
		return nil
	}
	if scoreBatchMode != columnVectorGraphScoreBatchModeScalar && len(rowIDs) > 1 && scratch != nil && len(normalizedQuery) >= v.Header.VectorStride {
		scratch.scoreTileDots = ensureColumnVectorGraphNativeFloat32Scratch(scratch.scoreTileDots, len(rowIDs))
		dots := scratch.scoreTileDots[:len(rowIDs)]
		status := vectorops.DotFloat32IndexedPrevalidated(dots, v.NormalizedVectors, normalizedQuery[:v.Header.VectorStride], rowIDs, v.Header.VectorStride)
		if !status.Invalid && status.Rows == len(rowIDs) {
			recordColumnHNSWSearchPackScoreBatchStats(stats, len(rowIDs), status.Optimized, status.Fallback)
			v.recordScoreStats(stats, len(rowIDs))
			if visitedCandidates != nil {
				*visitedCandidates += uint64(len(rowIDs))
			}
			for i, rowID := range rowIDs {
				candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: float64(dots[i])}
				if scratch.insertTop(topK, candidate) {
					scratch.pushFrontier(candidate)
				}
			}
			return nil
		}
	}
	scratch.scoreTileScores = ensureColumnVectorGraphNativeFloat64Scratch(scratch.scoreTileScores, len(rowIDs))
	scores, err := v.scoreRowIDs(normalizedQuery, rowIDs, scratch.scoreTileScores, scoreBatchMode, scratch, stats)
	if err != nil {
		return err
	}
	if visitedCandidates != nil {
		*visitedCandidates += uint64(len(rowIDs))
	}
	for i, rowID := range rowIDs {
		candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: scores[i]}
		if scratch.insertTop(topK, candidate) {
			scratch.pushFrontier(candidate)
		}
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) scoreOrdinal(normalizedQuery []float32, ordinal int, scoreBatchMode columnVectorGraphScoreBatchMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if v == nil || ordinal < 0 || ordinal >= v.Header.Rows {
		return 0, fmt.Errorf("collections: hnsw_search_pack_v1 vector ordinal=%d unavailable", ordinal)
	}
	start := ordinal * v.Header.VectorStride
	end := start + v.Header.Dimensions
	if start < 0 || end < start || end > len(v.NormalizedVectors) || len(normalizedQuery) < v.Header.Dimensions {
		return 0, fmt.Errorf("collections: hnsw_search_pack_v1 vector ordinal=%d shape mismatch", ordinal)
	}
	vector := v.NormalizedVectors[start:end]
	score := float64(vectorDotProductFloat32(normalizedQuery[:v.Header.Dimensions], vector))
	optimized := scoreBatchMode != columnVectorGraphScoreBatchModeScalar && vectorops.DotFloat32OptimizedEligible(v.Header.Dimensions)
	recordColumnHNSWSearchPackScoreBatchStats(stats, 1, optimized, !optimized)
	v.recordScoreStats(stats, 1)
	_ = scratch
	return score, nil
}

func (v *columnHNSWSearchPackPreparedView) scoreRowIDs(normalizedQuery []float32, rowIDs []uint32, dst []float64, scoreBatchMode columnVectorGraphScoreBatchMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if cap(dst) < len(rowIDs) {
		dst = make([]float64, len(rowIDs))
	} else {
		dst = dst[:len(rowIDs)]
	}
	if len(rowIDs) == 0 {
		return dst, nil
	}
	if scoreBatchMode != columnVectorGraphScoreBatchModeScalar && len(rowIDs) > 1 && scratch != nil && len(normalizedQuery) >= v.Header.VectorStride {
		scratch.scoreTileDots = ensureColumnVectorGraphNativeFloat32Scratch(scratch.scoreTileDots, len(rowIDs))
		dots := scratch.scoreTileDots[:len(rowIDs)]
		status := vectorops.DotFloat32IndexedPrevalidated(dots, v.NormalizedVectors, normalizedQuery[:v.Header.VectorStride], rowIDs, v.Header.VectorStride)
		if !status.Invalid && status.Rows == len(rowIDs) {
			for i := range rowIDs {
				dst[i] = float64(dots[i])
			}
			recordColumnHNSWSearchPackScoreBatchStats(stats, len(rowIDs), status.Optimized, status.Fallback)
			v.recordScoreStats(stats, len(rowIDs))
			return dst, nil
		}
	}
	for i, rowID := range rowIDs {
		score, err := v.scoreOrdinal(normalizedQuery, int(rowID), scoreBatchMode, scratch, nil)
		if err != nil {
			return dst[:i], err
		}
		dst[i] = score
	}
	optimized := len(rowIDs) == 1 && scoreBatchMode != columnVectorGraphScoreBatchModeScalar && vectorops.DotFloat32OptimizedEligible(v.Header.Dimensions)
	recordColumnHNSWSearchPackScoreBatchStats(stats, len(rowIDs), optimized, !optimized)
	v.recordScoreStats(stats, len(rowIDs))
	return dst, nil
}

func recordColumnHNSWSearchPackScoreBatchStats(stats *columnVectorGraphNativeSearchStats, tileSize int, optimized bool, scalarFallback bool) {
	recordColumnVectorGraphScoreBatchStats(stats, tileSize, optimized, scalarFallback)
}

func (v *columnHNSWSearchPackPreparedView) recordScoreStats(stats *columnVectorGraphNativeSearchStats, count int) {
	if stats == nil || v == nil || count <= 0 {
		return
	}
	count64 := uint64(count)
	stats.PreparedScoreCalls += count64
	stats.VisitedNodes += count64
	stats.CandidateFetches += count64
	stats.VectorBytesRead += count64 * uint64(v.Header.Dimensions) * 4
	switch v.status {
	case columnHNSWSearchPackPreparedStatusDirect:
		stats.VectorDirectViews += count64
		stats.VectorMmapDirectViews += count64
		stats.VectorPreparedDirectViews += count64
	case columnHNSWSearchPackPreparedStatusHeap:
		stats.VectorHeapCopyTypedViews += count64
		stats.VectorPreparedDirectViews += count64
	}
}

func (v *columnHNSWSearchPackPreparedView) adjacencyLayerForOrdinal(ordinal int, layer int, stats *columnVectorGraphNativeSearchStats) ([]uint32, error) {
	if v == nil || layer < 0 || layer >= len(v.AdjacencyLayers) || ordinal < 0 || ordinal >= v.Header.Rows {
		return nil, fmt.Errorf("collections: hnsw_search_pack_v1 adjacency ordinal=%d layer=%d unavailable", ordinal, layer)
	}
	adjacency := v.AdjacencyLayers[layer]
	if ordinal+1 >= len(adjacency.Offsets) {
		return nil, fmt.Errorf("collections: hnsw_search_pack_v1 adjacency ordinal=%d layer=%d offsets unavailable", ordinal, layer)
	}
	start64 := adjacency.Offsets[ordinal]
	end64 := adjacency.Offsets[ordinal+1]
	if end64 < start64 || end64 > uint64(len(adjacency.Neighbors)) || end64 > uint64(math.MaxInt) {
		return nil, fmt.Errorf("collections: hnsw_search_pack_v1 adjacency ordinal=%d layer=%d bounds invalid", ordinal, layer)
	}
	neighbors := adjacency.Neighbors[int(start64):int(end64)]
	v.recordAdjacencyStats(stats, len(neighbors))
	return neighbors, nil
}

func (v *columnHNSWSearchPackPreparedView) recordAdjacencyStats(stats *columnVectorGraphNativeSearchStats, adjacencyLen int) {
	if stats == nil || adjacencyLen < 0 {
		return
	}
	stats.ExpansionFetches++
	stats.AdjacencyExpansions++
	stats.AdjacencyBytesRead += uint64(adjacencyLen) * 4
	switch v.status {
	case columnHNSWSearchPackPreparedStatusDirect:
		stats.AdjacencyDirectViews++
		stats.AdjacencyMmapDirectViews++
		stats.AdjacencyPreparedCSRDirectViews++
		stats.AdjacencyPreparedCSRMmapDirectViews++
	case columnHNSWSearchPackPreparedStatusHeap:
		stats.AdjacencyHeapCopyTypedViews++
	}
}

func (v *columnHNSWSearchPackPreparedView) fetchTopSearchResults(scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) error {
	if v == nil {
		return errColumnHNSWSearchPackSearchUnavailable
	}
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
	scratch.resultRowRefs = scratch.resultRowRefs[:n]
	scratch.resultHasRefs = scratch.resultHasRefs[:n]
	clear(scratch.resultRowRefs)
	clear(scratch.resultHasRefs)
	for resultPos, topIndex := range scratch.resultOrder {
		ordinal := scratch.resultOrdinals[resultPos]
		id, ok := v.documentIDForOrdinal(ordinal)
		if !ok {
			return fmt.Errorf("collections: hnsw_search_pack_v1 result-id unavailable for ordinal=%d", ordinal)
		}
		scratch.resultIDViews[topIndex] = id
		if stats != nil {
			stats.ResultIDTypedBytesState++
		}
		rowRef, ok := v.rowRefForOrdinal(ordinal)
		if !ok {
			return fmt.Errorf("collections: hnsw_search_pack_v1 row-ref unavailable for ordinal=%d", ordinal)
		}
		rowRef.DocumentID = id
		scratch.resultRowRefs[topIndex] = rowRef
		scratch.resultHasRefs[topIndex] = true
		if stats != nil {
			stats.RowRefStateResultRefs++
		}
	}
	if stats != nil {
		stats.ResultFetches += uint64(n)
		stats.ResultIDPreparedBytesViews = 1
		stats.RowRefStatePreparedViews = 1
		if v.status == columnHNSWSearchPackPreparedStatusDirect {
			stats.RowRefStateMmapDirectFields = uint64(len(columnVectorGraphRowRefStateFields))
		}
	}
	for i, candidate := range scratch.top {
		scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{
			Ordinal:   candidate.ordinal,
			ID:        scratch.resultIDViews[i],
			RowRef:    scratch.resultRowRefs[i],
			HasRowRef: scratch.resultHasRefs[i],
			Score:     candidate.score,
		})
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) documentIDForOrdinal(ordinal int) ([]byte, bool) {
	if v == nil || ordinal < 0 || ordinal >= v.Header.Rows || ordinal+1 >= len(v.DocumentIDOffsets) {
		return nil, false
	}
	start64 := v.DocumentIDOffsets[ordinal]
	end64 := v.DocumentIDOffsets[ordinal+1]
	if end64 < start64 || end64 > uint64(len(v.DocumentIDBytes)) || end64 > uint64(math.MaxInt) {
		return nil, false
	}
	return v.DocumentIDBytes[int(start64):int(end64)], true
}

func (v *columnHNSWSearchPackPreparedView) rowRefForOrdinal(ordinal int) (DocumentRowRef, bool) {
	if v == nil || ordinal < 0 || ordinal >= v.Header.Rows || ordinal >= len(v.RowRefGenerations) || ordinal >= len(v.RowRefPartIDs) || ordinal >= len(v.RowRefRowIndexes) || ordinal >= len(v.RowRefAppliedLSNs) {
		return DocumentRowRef{}, false
	}
	generation := v.RowRefGenerations[ordinal]
	partID := v.RowRefPartIDs[ordinal]
	rowIndex := v.RowRefRowIndexes[ordinal]
	appliedLSN := v.RowRefAppliedLSNs[ordinal]
	if generation <= 0 || partID <= 0 || rowIndex < 0 || appliedLSN <= 0 {
		return DocumentRowRef{}, false
	}
	return DocumentRowRef{
		Generation:        uint64(generation),
		PartID:            uint64(partID),
		RowIndex:          int(rowIndex),
		AppliedCommandLSN: uint64(appliedLSN),
	}, true
}
