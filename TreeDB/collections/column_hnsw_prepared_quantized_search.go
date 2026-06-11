package collections

import "fmt"

var errColumnHNSWPreparedQuantizedTraversalUnavailable = fmt.Errorf("%w: scalar_u8 hnsw prepared quantized traversal unavailable", ErrVectorIndexSearchUnavailable)

func collectionScalarU8PreparedTraversalPackForReader(reader *columnVectorGraphPhysicalRowReader, queryMode columnVectorGraphNativeSearchQueryMode, statsMode columnVectorGraphNativeSearchStatsMode, quantizedIndexName string) (*columnHNSWSearchPackPreparedView, bool) {
	if reader == nil || !queryMode.quantized() || !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		return nil, false
	}
	qdef, ok := findQuantizedVectorIndex(reader.def, quantizedIndexName)
	if !ok || qdef.Codec != QuantizedVectorCodecScalarU8 || qdef.Version != 1 {
		return nil, false
	}
	pack := reader.hnswSearchPack
	switch status := pack.fastStatus(reader.hnswSearchPackStatus); status {
	case columnHNSWSearchPackPreparedStatusDirect, columnHNSWSearchPackPreparedStatusHeap:
		return pack, pack != nil
	default:
		return nil, false
	}
}

func (r *columnVectorGraphPhysicalRowReader) SearchCosineScalarU8PreparedTraversal(pack *columnHNSWSearchPackPreparedView, query []float32, opts columnVectorGraphNativeSearchOptions, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	var setupStats columnVectorGraphNativeSearchStats
	if r == nil {
		return nil, setupStats, errNilColumnVectorGraphPhysicalRowReader
	}
	queryMode, err := opts.QueryMode.normalized()
	if err != nil {
		return nil, setupStats, fmt.Errorf("collections: column_graph %q hnsw prepared quantized traversal query mode: %w", r.def.Name, err)
	}
	switch queryMode {
	case columnVectorGraphNativeSearchQueryModeQuantizedOnly:
		setupStats.SearchRouteQuantizedOnly = 1
	case columnVectorGraphNativeSearchQueryModeQuantizedRerank:
		setupStats.SearchRouteQuantizedRerank = 1
	default:
		return nil, setupStats, fmt.Errorf("%w: column_graph %q query_mode=%s is not quantized", errColumnHNSWPreparedQuantizedTraversalUnavailable, r.def.Name, queryMode.String())
	}
	r.populateQuantizedAssetSearchStats(opts.QuantizedIndexName, &setupStats)
	if err := r.validateQuantizedNativeSearchOptions(queryMode, opts); err != nil {
		return nil, setupStats, err
	}
	if opts.HasCandidateRows {
		return nil, setupStats, errColumnHNSWSearchPackSearchCandidateRows
	}
	traversalMode, wavefrontWidth, err := columnVectorGraphNativeSearchTraversalOptions(opts)
	if err != nil {
		return nil, setupStats, fmt.Errorf("collections: column_graph %q hnsw prepared quantized traversal: %w", r.def.Name, err)
	}
	if traversalMode != columnVectorGraphNativeSearchTraversalModeExact || wavefrontWidth != 0 {
		return nil, setupStats, fmt.Errorf("%w: column_graph %q traversal_mode=%s wavefront_width=%d", errColumnHNSWPreparedQuantizedTraversalUnavailable, r.def.Name, traversalMode.String(), wavefrontWidth)
	}
	qdef, ok := findQuantizedVectorIndex(r.def, opts.QuantizedIndexName)
	if !ok || qdef.Codec != QuantizedVectorCodecScalarU8 || qdef.Version != 1 {
		return nil, setupStats, fmt.Errorf("%w: column_graph %q quantized index %q codec/version=(%q,%d) is not scalar_u8 v1", errColumnHNSWPreparedQuantizedTraversalUnavailable, r.def.Name, opts.QuantizedIndexName, qdef.Codec, qdef.Version)
	}
	if pack == nil {
		return nil, setupStats, errColumnHNSWPreparedQuantizedTraversalUnavailable
	}
	switch status := pack.fastStatus(r.hnswSearchPackStatus); status {
	case columnHNSWSearchPackPreparedStatusDirect, columnHNSWSearchPackPreparedStatusHeap:
	default:
		return nil, setupStats, columnHNSWSearchPackStatusError(status)
	}
	rowCount := r.RowCount()
	if pack.Header.Rows != rowCount || pack.Header.Dimensions != r.def.Dimensions {
		return nil, setupStats, fmt.Errorf("%w: column_graph %q hnsw_search_pack_v1 shape rows/dims=(%d,%d) want (%d,%d)", errColumnHNSWPreparedQuantizedTraversalUnavailable, r.def.Name, pack.Header.Rows, pack.Header.Dimensions, rowCount, r.def.Dimensions)
	}
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
	setupStats.CandidateRows = uint64(rowCount)
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
	rerankCandidateLimit := 0
	if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank {
		rerankCandidateLimit = opts.QuantizedRerankCandidates
		if rerankCandidateLimit == 0 {
			rerankCandidateLimit = efSearch
		}
		if rerankCandidateLimit < topK {
			return nil, columnVectorGraphNativeSearchStats{}, fmt.Errorf("collections: column_graph %q native search quantized rerank candidates=%d below normalized top_k=%d: %w", r.def.Name, rerankCandidateLimit, topK, errColumnVectorGraphNativeSearchQuantizedRerankLimit)
		}
		if rerankCandidateLimit > rowCount {
			rerankCandidateLimit = rowCount
		}
	}
	scorer, err := r.prepareQuantizedScorer(queryMode, opts.QuantizedIndexName, query, queryInvNorm, scratch)
	if err != nil {
		recordColumnVectorGraphQuantizedAssetErrorStats(&setupStats, err)
		return nil, setupStats, err
	}
	scratch.searchPlan.quantizedScorer = scorer
	scratch.preparedQuantizedPlane.scorer = &scratch.searchPlan.quantizedScorer
	setupStats.QuantizedScorerActive = 1
	if r.preparedSearch != nil && r.preparedSearch.ready() {
		setupStats.PreparedGraphSearchViews = 1
	}
	traversalStatsMode := opts.StatsMode.normalized()
	if traversalStatsMode == columnVectorGraphNativeSearchStatsModeMinimal {
		// The existing quantized column_graph route keeps candidate/edge counters even
		// for production/minimal stats because quantized traversal is the thing being
		// measured. Preserve that counter contract while reusing the pack traversal.
		traversalStatsMode = columnVectorGraphNativeSearchStatsModeFullDiagnostics
	}
	packOpts := columnHNSWPreparedTraversalOptions{
		TopK:                                 opts.TopK,
		EfSearch:                             opts.EfSearch,
		RetainedCandidateLimit:               0,
		ScoreBatchMode:                       opts.ScoreBatchMode,
		StatsMode:                            traversalStatsMode,
		OmitResultMaterialization:            opts.OmitResultMaterialization || queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		SuppressOmittedResultMaterialization: queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank,
	}
	results, stats, err := pack.searchCosinePreparedScorePlane(query, packOpts, scratch, &scratch.preparedQuantizedPlane)
	columnVectorGraphApplyQuantizedPreparedTraversalSetupStats(&stats, setupStats)
	r.populateScalarU8PreparedTraversalSearchStats(&stats)
	if err != nil {
		return nil, stats, err
	}
	if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedOnly {
		return results, stats, nil
	}

	// The pack traversal retained the quantized candidate set in scratch.top without
	// reading result IDs/row refs. Rerank only the configured shortlist with the
	// existing exact FP32 scorer so vector/norm read and exact-call counters stay
	// bounded by QuantizedRerankCandidates.
	scratch.results = scratch.results[:0]
	plan, err := scratch.prepareSearchPlanForNativeSearch(r)
	if err != nil {
		return nil, stats, err
	}
	plan.scoreBatchMode = columnVectorGraphScoreBatchModeForSearchPlan(opts.ScoreBatchMode, plan)
	var singleBlockView *columnVectorGraphBlockView
	if plan.physicalReader != nil && len(plan.physicalReader.ranges) == 1 && (plan.scoreSource.vectorKind == columnVectorGraphSearchVectorSourceGraphRows || plan.scoreSource.normKind == columnVectorGraphSearchNormSourceGraphRows) {
		singleBlockView, err = plan.blockViewForAssetOrdinal(0)
		if err != nil {
			return nil, stats, err
		}
	}
	if err := r.exactRerankQuantizedCandidates(plan, singleBlockView, query, queryInvNorm, topK, rerankCandidateLimit, scratch, &stats); err != nil {
		return nil, stats, err
	}
	if opts.OmitResultMaterialization {
		for _, candidate := range scratch.top {
			scratch.results = append(scratch.results, columnVectorGraphNativeSearchResult{Ordinal: candidate.ordinal, Score: candidate.score})
		}
		return scratch.results, stats, nil
	}
	if err := pack.fetchTopSearchResults(scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func columnVectorGraphApplyQuantizedPreparedTraversalSetupStats(dst *columnVectorGraphNativeSearchStats, src columnVectorGraphNativeSearchStats) {
	if dst == nil {
		return
	}
	dst.SearchRouteQuantizedOnly = src.SearchRouteQuantizedOnly
	dst.SearchRouteQuantizedRerank = src.SearchRouteQuantizedRerank
	dst.QuantizedScorerActive = src.QuantizedScorerActive
	dst.QuantizedAssetMissing = src.QuantizedAssetMissing
	dst.QuantizedAssetInvalid = src.QuantizedAssetInvalid
	dst.QuantizedAssetStale = src.QuantizedAssetStale
	dst.QuantizedAssetClosed = src.QuantizedAssetClosed
	dst.QuantizedAssetUnavailable = src.QuantizedAssetUnavailable
	dst.QuantizedAssetMmapDirect = src.QuantizedAssetMmapDirect
	dst.QuantizedAssetHeapCopy = src.QuantizedAssetHeapCopy
	dst.QuantizedAssetOpenNanos = src.QuantizedAssetOpenNanos
	dst.QuantizedAssetMappedBytes = src.QuantizedAssetMappedBytes
	dst.QuantizedAssetHeapCopyBytes = src.QuantizedAssetHeapCopyBytes
	dst.QuantizedAssetActiveHandles = src.QuantizedAssetActiveHandles
	if src.CandidateRows != 0 && dst.CandidateRows == 0 {
		dst.CandidateRows = src.CandidateRows
	}
	if src.PreparedGraphSearchViews != 0 {
		dst.PreparedGraphSearchViews = src.PreparedGraphSearchViews
	}
}

func (r *columnVectorGraphPhysicalRowReader) populateScalarU8PreparedTraversalSearchStats(stats *columnVectorGraphNativeSearchStats) {
	if r == nil || stats == nil {
		return
	}
	r.populateTypedColumnVectorSearchStats(stats)
	r.populateInvNormStateSearchStats(stats)
	r.populateRowRefStateSearchStats(stats)
	r.populateDocumentIDStateSearchStats(stats)
	r.populateLayer0AdjacencySourceSearchStats(stats)
}
