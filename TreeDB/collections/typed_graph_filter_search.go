package collections

import (
	"context"
	"slices"
)

// searchPreparedFilter does no posting or locator work. The plan's current pin
// owns eligibility; the older base is only a checked vector/graph accelerator.
func (v *typedGraphOverlaySearch) searchPreparedFilter(plan *typedGraphPreparedFilter, query []float32, topK, efSearch, candidateLimit int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, typedGraphOverlaySearchStats, error) {
	return v.searchPreparedFilterWithContext(context.Background(), plan, query, topK, efSearch, candidateLimit, buffer)
}

func (v *typedGraphOverlaySearch) searchPreparedFilterWithContext(ctx context.Context, plan *typedGraphPreparedFilter, query []float32, topK, efSearch, candidateLimit int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, typedGraphOverlaySearchStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var stats typedGraphOverlaySearchStats
	defer stats.recordWork()
	completed := false
	if buffer != nil {
		buffer.resetView()
		defer func() {
			if !completed {
				buffer.resetView()
			}
		}()
	}
	if buffer == nil || !plan.validFor(v) {
		return nil, stats, ErrVectorIndexSnapshotMismatch
	}
	if err := validateVectorIndexSearchRequest(topK, efSearch); err != nil {
		return nil, stats, err
	}
	if len(query) != v.base.reader.def.Dimensions {
		return nil, stats, errColumnVectorGraphNativeSearchQueryDimensionMismatch
	}
	queryNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, stats, err
	}
	switch v.pack.fastStatus("") {
	case columnHNSWSearchPackPreparedStatusDirect:
		stats.PackMmapDirect = true
	case columnHNSWSearchPackPreparedStatusHeap:
		stats.PackHeapCopy = true
	default:
		return nil, stats, errColumnHNSWSearchPackSearchUnavailable
	}
	stats.FilteredExact = plan.count <= typedGraphScalarExactLimit
	if candidateLimit <= 0 {
		return nil, stats, errTypedGraphSearchBudget
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	if topK == 0 || plan.count == 0 {
		stats.Route = "typed_empty"
		completed = true
		return nil, stats, nil
	}
	baseK := min(topK, plan.base.Count()-len(plan.excludedBase))
	if stats.FilteredExact {
		if plan.count > candidateLimit {
			return nil, stats, errTypedGraphSearchBudget
		}
		stats.Route = "typed_exact"
		// Retain only K scored ordinals; document IDs are read at the result
		// boundary, not for every eligible candidate.
		scratch := &buffer.searchScratch
		scratch.top = scratch.top[:0]
		for rank, ordinal := range plan.exactBaseByID {
			if rank&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, stats, err
				}
			}
			vector, _, _, ok := v.base.reader.typedVectorSource.vectorForOrdinal(ordinal)
			if !ok {
				return nil, stats, ErrVectorIndexSnapshotMismatch
			}
			norm, _, _, ok := v.base.reader.invNormForOrdinal(ordinal)
			if !ok {
				return nil, stats, ErrVectorIndexSnapshotMismatch
			}
			score, err := columnVectorGraphNativeCosineScoreVector(query, queryNorm, ordinal, vector, norm)
			if err != nil {
				return nil, stats, err
			}
			stats.ExactBaseScored++
			scratch.insertTop(baseK, columnVectorGraphSearchCandidate{ordinal: rank, score: score})
		}
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		scratch.retainTopBestFirst(baseK)
		for i, candidate := range scratch.top {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, stats, err
				}
			}
			id, ok := v.pack.documentIDForOrdinal(plan.exactBaseByID[candidate.ordinal])
			if !ok {
				return nil, stats, ErrVectorIndexSnapshotMismatch
			}
			buffer.baseResults = append(buffer.baseResults, VectorIndexSearchResult{ID: id, Score: candidate.score})
		}
		stats.BaseResultIDs = len(buffer.baseResults)
	} else {
		baseLimit := candidateLimit - len(plan.delta)
		if baseK == 0 || baseLimit < baseK || len(plan.excludedBase) > baseLimit-baseK || efSearch > baseLimit {
			return nil, stats, errTypedGraphSearchBudget
		}
		baseRequestK := baseK + len(plan.excludedBase)
		if efSearch == 0 {
			efSearch = min(v.base.reader.def.EfSearch, baseLimit)
		}
		stats.Route = "typed_hnsw"
		results, baseStats, err := v.pack.searchCosineWithContext(ctx, query, columnVectorGraphNativeSearchOptions{TopK: baseRequestK, EfSearch: max(baseRequestK, efSearch), CandidateLimit: baseLimit, CandidateRows: plan.base, HasCandidateRows: true, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &buffer.searchScratch)
		stats.Base = baseStats
		stats.BaseResultIDs = len(results)
		if err != nil {
			return nil, stats, err
		}
		for i, result := range results {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, stats, err
				}
			}
			if plan.excludesBaseOrdinal(result.Ordinal) {
				stats.BaseShadowed++
				continue
			}
			buffer.baseResults = append(buffer.baseResults, VectorIndexSearchResult{ID: result.ID, Score: result.Score})
		}
	}
	for rank, i := range plan.delta {
		if rank&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, stats, err
			}
		}
		row := v.rows[i]
		score, err := columnVectorGraphNativeCosineScoreVector(query, queryNorm, i, row.Values[v.vectorColumn].Float32Vector, v.invNorms[i])
		if err != nil {
			return nil, stats, err
		}
		stats.DeltaScored++
		buffer.deltaResults = append(buffer.deltaResults, VectorIndexSearchResult{ID: row.ID, Score: score})
	}
	compare := func(a, b VectorIndexSearchResult) int {
		if vectorIndexSearchResultBefore(a, b) {
			return -1
		}
		if vectorIndexSearchResultBefore(b, a) {
			return 1
		}
		return 0
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	slices.SortFunc(buffer.baseResults, compare)
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	slices.SortFunc(buffer.deltaResults, compare)
	results, err := mergeVectorIndexViewResultsWithContext(ctx, buffer.baseResults, buffer.deltaResults, topK, buffer)
	if err != nil {
		return nil, stats, err
	}
	if len(results) < min(topK, plan.count) {
		return nil, stats, errTypedGraphSearchBudget
	}
	completed = true
	return results, stats, nil
}
