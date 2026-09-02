package collections

import (
	"fmt"
	"math"
)

func (v *columnHNSWSearchPackPreparedView) searchCosinePreparedRawDotTraversal(query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch, scorePlane columnHNSWPreparedTraversalScorePlane) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
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
	rawDotScorePlane, ok := scorePlane.(columnHNSWPreparedTraversalRawDotScorePlane)
	if !ok {
		return nil, *stats, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if len(query) != v.Header.Dimensions {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal query dims=%d want %d: %w", len(query), v.Header.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
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
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal retained candidates=%d cannot be negative", retainedCandidateLimit)
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
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal retained candidates=%d below normalized top_k=%d", retainedCandidateLimit, topK)
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
	if err := scratch.prepareHNSWSearchPack(rowCount, v.Header.VectorStride, degree, resultScratchK, retainedCandidateLimit, opts.ScoreTileCapacity, degree, degree); err != nil {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal scratch prepare: %w", err)
	}
	if err := scorePlane.prepareForHNSWPreparedTraversal(v, query, opts, scratch); err != nil {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal %s score plane: %w", scorePlane.kind(), err)
	}
	return v.searchCosinePreparedRawDotScorePlane(rawDotScorePlane, opts, rowCount, topK, retainedCandidateLimit, degree, statsMode, scratch, stats)
}

func (v *columnHNSWSearchPackPreparedView) searchCosinePreparedRawDotScorePlane(rawDotScorePlane columnHNSWPreparedTraversalRawDotScorePlane, opts columnHNSWPreparedTraversalOptions, rowCount, topK, retainedCandidateLimit, degree int, statsMode columnVectorGraphNativeSearchStatsMode, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	if rawDotScorePlane == nil {
		return nil, *stats, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	scratch.prepareRawDotCandidateQueues(rowCount, degree, topK, retainedCandidateLimit)
	countLoopEdges := !statsMode.minimal()
	var loopEdgeVisits uint64
	var visitedCandidates uint64
	entryOrdinal := v.Header.EntryOrdinal
	if entryOrdinal < 0 || entryOrdinal >= rowCount {
		return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal entry ordinal=%d outside rows=%d", entryOrdinal, rowCount)
	}
	maxLayer, err := v.maxLayerForOrdinal(entryOrdinal)
	if err != nil {
		return nil, *stats, err
	}
	traversalStart, traversalDistanceBefore := columnVectorGraphNativeSearchStartGraphTraversal(stats)
	for layer := maxLayer; layer > 0; layer-- {
		entryOrdinal, err = v.greedyNearestAtLayerPreparedRawDotScorePlane(rawDotScorePlane, entryOrdinal, layer, scratch, stats, countLoopEdges, &loopEdgeVisits)
		if err != nil {
			return nil, *stats, err
		}
	}
	visitMarks := scratch.visitMarks
	visitEpoch := scratch.visitEpoch
	visitMarks[entryOrdinal] = visitEpoch
	if err := v.scoreAndPushRawDotFrontierVisitedPreparedScorePlane(rawDotScorePlane, entryOrdinal, retainedCandidateLimit, scratch, stats, &visitedCandidates); err != nil {
		return nil, *stats, err
	}
	nextSeed := 0
	rowCount64 := uint64(rowCount)
	for {
		candidate, ok := scratch.popRawDotFrontierAccounting(stats)
		if !ok {
			if len(scratch.rawDot.top) >= retainedCandidateLimit {
				break
			}
			seed, seedOK := columnHNSWSearchPackNextCandidateSeed(nextSeed, rowCount, visitMarks, visitEpoch)
			if !seedOK {
				break
			}
			nextSeed = seed + 1
			visitMarks[seed] = visitEpoch
			if err := v.scoreAndPushRawDotFrontierVisitedPreparedScorePlane(rawDotScorePlane, seed, retainedCandidateLimit, scratch, stats, &visitedCandidates); err != nil {
				return nil, *stats, err
			}
			continue
		}
		if columnVectorGraphLayer0RawDotSearchShouldStop(candidate, scratch.rawDot.top, retainedCandidateLimit) {
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
		scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, len(adjacency))
		tile := scratch.scoreTileRowIDs[:0]
		for i, neighbor := range adjacency {
			if uint64(neighbor) >= rowCount64 {
				return nil, *stats, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
			neighborOrdinal := int(neighbor)
			if visitMarks[neighborOrdinal] == visitEpoch {
				continue
			}
			visitMarks[neighborOrdinal] = visitEpoch
			tile = append(tile, neighbor)
		}
		if len(tile) != 0 {
			scored, err := rawDotScorePlane.scoreAndPushRawDotFrontierVisitedRowIDsPrevalidated(tile, retainedCandidateLimit, scratch, stats)
			if err != nil {
				return nil, *stats, err
			}
			visitedCandidates += uint64(scored)
		}
	}
	if countLoopEdges {
		stats.Edges = loopEdgeVisits
		stats.VisitedEdges = loopEdgeVisits
		stats.Candidates = visitedCandidates
	}
	columnVectorGraphNativeSearchFinishGraphTraversal(stats, traversalStart, traversalDistanceBefore)
	return v.finishPreparedRawDotScorePlaneResults(topK, retainedCandidateLimit, opts, scratch, stats)
}

func (v *columnHNSWSearchPackPreparedView) greedyNearestAtLayerPreparedRawDotScorePlane(rawDotScorePlane columnHNSWPreparedTraversalRawDotScorePlane, entryOrdinal int, layer int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, countLoopEdges bool, loopEdgeVisits *uint64) (int, error) {
	best := entryOrdinal
	bestDot, err := rawDotScorePlane.scoreRawDotOrdinal(best, scratch, stats)
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
				return 0, fmt.Errorf("collections: hnsw_search_pack_v1 prepared raw-dot traversal ordinal=%d layer=%d adjacency[%d]=%d outside row_count=%d: %w", best, layer, i, neighbor, v.Header.Rows, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
		}
		scratch.scoreTileQuantizedDots = ensureColumnVectorGraphNativeInt64Scratch(scratch.scoreTileQuantizedDots, len(adjacency))
		dots, err := rawDotScorePlane.scoreRawDotRowIDsPrevalidated(adjacency, scratch.scoreTileQuantizedDots, scratch, stats)
		if err != nil {
			return 0, err
		}
		for i, neighbor := range adjacency {
			neighborOrdinal := int(neighbor)
			dot := dots[i]
			// Match the float64 traversal tie contract: lower ordinal wins exact ties.
			if dot > bestDot || (dot == bestDot && neighborOrdinal < best) {
				best = neighborOrdinal
				bestDot = dot
				changed = true
			}
		}
	}
	return best, nil
}

func (v *columnHNSWSearchPackPreparedView) scoreAndPushRawDotFrontierVisitedPreparedScorePlane(rawDotScorePlane columnHNSWPreparedTraversalRawDotScorePlane, ordinal, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats, visitedCandidates *uint64) error {
	_ = v
	dot, err := rawDotScorePlane.scoreRawDotOrdinal(ordinal, scratch, stats)
	if err != nil {
		return err
	}
	if visitedCandidates != nil {
		(*visitedCandidates)++
	}
	candidate := columnVectorGraphRawDotSearchCandidate{ordinal: ordinal, dot: dot}
	if scratch.insertRawDotTop(topK, candidate) {
		scratch.pushRawDotFrontierAccounting(candidate, stats)
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) finishPreparedRawDotScorePlaneResults(topK int, retainedCandidateLimit int, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	if len(scratch.rawDot.top) == 0 {
		return scratch.results, *stats, nil
	}
	if opts.OmitResultMaterialization {
		materializeLimit := retainedCandidateLimit
		if opts.SuppressOmittedResultMaterialization {
			scratch.promoteRawDotTopOrdinalsOnly(materializeLimit)
			return scratch.results, *stats, nil
		}
		scratch.promoteRawDotTopToFloat(materializeLimit)
		for _, candidate := range scratch.top {
			scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, Score: candidate.score})
		}
		return scratch.results, *stats, nil
	}
	scratch.promoteRawDotTopToFloat(topK)
	if err := v.fetchTopSearchResults(scratch, stats); err != nil {
		return nil, *stats, err
	}
	return scratch.results, *stats, nil
}
