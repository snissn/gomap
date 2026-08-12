package collections

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rabitq"
)

func columnVectorGraphQuantizedIndexIsRabitQ1Bit(def VectorIndexDefinition, name string) bool {
	qdef, ok := findQuantizedVectorIndex(def, name)
	return ok && qdef.Codec == rabitq.CodecName && qdef.Version == rabitq.CodecVersion
}

func (r *columnVectorGraphPhysicalRowReader) rabitqHNSWSearchPackPreparedRouteEligible(queryMode columnVectorGraphNativeSearchQueryMode, quantizedIndexName string, statsMode columnVectorGraphNativeSearchStatsMode) bool {
	if r == nil || !queryMode.quantized() || !columnVectorGraphQuantizedIndexIsRabitQ1Bit(r.def, quantizedIndexName) {
		return false
	}
	if !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		return false
	}
	pack := r.hnswSearchPack
	if pack == nil {
		return false
	}
	status := pack.fastStatus(r.hnswSearchPackStatus)
	return status == columnHNSWSearchPackPreparedStatusDirect || status == columnHNSWSearchPackPreparedStatusHeap
}

func (r *columnVectorGraphPhysicalRowReader) searchRabitQCosinePreparedHNSWPack(query []float32, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	var stats columnVectorGraphNativeSearchStats
	if r == nil {
		return nil, stats, errNilColumnVectorGraphPhysicalRowReader
	}
	queryMode, err := opts.QueryMode.normalized()
	if err != nil {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 rabitq_1bit prepared traversal query mode: %w", err)
	}
	if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedOnly {
		stats.SearchRouteQuantizedOnly = 1
	} else if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank {
		stats.SearchRouteQuantizedRerank = 1
	} else {
		return nil, stats, errColumnHNSWSearchPackSearchUnsupportedMode
	}
	if opts.HasCandidateRows {
		return nil, stats, errColumnHNSWSearchPackSearchCandidateRows
	}
	if len(query) != r.def.Dimensions {
		return nil, stats, fmt.Errorf("collections: column_graph %q query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	r.populateQuantizedAssetSearchStats(opts.QuantizedIndexName, &stats)
	if err := r.validateQuantizedNativeSearchOptions(queryMode, opts); err != nil {
		return nil, stats, err
	}
	if !columnVectorGraphQuantizedIndexIsRabitQ1Bit(r.def, opts.QuantizedIndexName) {
		return nil, stats, fmt.Errorf("%w: column_graph %q quantized index %q is not rabitq_1bit v%d", ErrVectorIndexSearchUnavailable, r.def.Name, opts.QuantizedIndexName, rabitq.CodecVersion)
	}
	if scratch == nil {
		return nil, stats, fmt.Errorf("collections: column_graph %q: %w", r.def.Name, errColumnVectorGraphNativeSearchScratchRequired)
	}
	pack := r.hnswSearchPack
	status := columnHNSWSearchPackPreparedStatusMissing
	if pack != nil {
		status = pack.fastStatus(r.hnswSearchPackStatus)
	}
	if status != columnHNSWSearchPackPreparedStatusDirect && status != columnHNSWSearchPackPreparedStatusHeap {
		return nil, stats, columnHNSWSearchPackStatusError(status)
	}
	if opts.TopK == 0 || r.RowCount() == 0 {
		return nil, stats, nil
	}

	scorer, err := r.prepareRabitQQuantizedScorer(queryMode, opts.QuantizedIndexName, query, scratch)
	if err != nil {
		recordColumnVectorGraphQuantizedAssetErrorStats(&stats, err)
		return nil, stats, err
	}
	scratch.searchPlan.quantizedScorer = columnVectorGraphQuantizedScorer{kind: columnVectorGraphQuantizedScorerKindRabitQ1Bit, rabitq: scorer}
	scratch.preparedQuantizedPlane.scorer = &scratch.searchPlan.quantizedScorer
	traversalStatsMode := opts.StatsMode.normalized()
	if traversalStatsMode == columnVectorGraphNativeSearchStatsModeMinimal {
		// Preserve the existing quantized route counter contract in production mode:
		// candidate/edge counters are part of the quantized traversal evidence even
		// though exact FP32 pack search can omit them for minimal stats.
		traversalStatsMode = columnVectorGraphNativeSearchStatsModeFullDiagnostics
	}
	traversalOpts := columnHNSWPreparedTraversalOptions{
		TopK:                      opts.TopK,
		EfSearch:                  opts.EfSearch,
		RetainedCandidateLimit:    0,
		ScoreBatchMode:            opts.ScoreBatchMode,
		StatsMode:                 traversalStatsMode,
		OmitResultMaterialization: opts.OmitResultMaterialization,
	}
	rerankCandidateLimit := 0
	if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank {
		rerankCandidateLimit = opts.QuantizedRerankCandidates
		traversalOpts.OmitResultMaterialization = true
		traversalOpts.SuppressOmittedResultMaterialization = true
	}
	results, searchStats, err := pack.searchCosinePreparedScorePlane(query, traversalOpts, scratch, &scratch.preparedQuantizedPlane)
	applyColumnVectorGraphQuantizedBaseStats(&searchStats, stats)
	searchStats.QuantizedScorerActive = 1
	if err != nil {
		return results, searchStats, err
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeQuantizedRerank {
		return results, searchStats, nil
	}
	scratch.retainTopBestFirst(rerankCandidateLimit)
	if err := pack.exactRerankPreparedTraversalCandidates(query, opts.TopK, opts.ScoreBatchMode, scratch, &searchStats); err != nil {
		return nil, searchStats, err
	}
	if opts.OmitResultMaterialization {
		scratch.results = scratch.results[:0]
		for _, candidate := range scratch.top {
			scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, Score: candidate.score})
		}
		return scratch.results, searchStats, nil
	}
	scratch.results = scratch.results[:0]
	if err := pack.fetchTopSearchResults(scratch, &searchStats); err != nil {
		return nil, searchStats, err
	}
	return scratch.results, searchStats, nil
}

func applyColumnVectorGraphQuantizedBaseStats(dst *columnVectorGraphNativeSearchStats, base columnVectorGraphNativeSearchStats) {
	if dst == nil {
		return
	}
	dst.SearchRouteQuantizedOnly = base.SearchRouteQuantizedOnly
	dst.SearchRouteQuantizedRerank = base.SearchRouteQuantizedRerank
	dst.QuantizedAssetMissing = base.QuantizedAssetMissing
	dst.QuantizedAssetInvalid = base.QuantizedAssetInvalid
	dst.QuantizedAssetStale = base.QuantizedAssetStale
	dst.QuantizedAssetClosed = base.QuantizedAssetClosed
	dst.QuantizedAssetUnavailable = base.QuantizedAssetUnavailable
	dst.QuantizedAssetMmapDirect = base.QuantizedAssetMmapDirect
	dst.QuantizedAssetHeapCopy = base.QuantizedAssetHeapCopy
	dst.QuantizedAssetOpenNanos = base.QuantizedAssetOpenNanos
	dst.QuantizedAssetMappedBytes = base.QuantizedAssetMappedBytes
	dst.QuantizedAssetHeapCopyBytes = base.QuantizedAssetHeapCopyBytes
	dst.QuantizedAssetActiveHandles = base.QuantizedAssetActiveHandles
}
