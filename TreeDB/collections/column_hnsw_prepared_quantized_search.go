package collections

import "fmt"

var errColumnHNSWPreparedQuantizedTraversalUnavailable = fmt.Errorf("%w: scalar_u8 hnsw prepared quantized traversal unavailable", ErrVectorIndexSearchUnavailable)

// columnHNSWPreparedScalarU8RawDotTraversalEnabled gates the scalar_u8 raw-dot
// prepared traversal seam. The seam is kept off by default until the remaining
// adapter-overhead follow-ups can make the raw-dot path performance-neutral or
// better across qonly/rerank production rows.
const columnHNSWPreparedScalarU8RawDotTraversalEnabled = false

type columnHNSWPreparedScalarU8ScorePlane struct {
	scorer columnVectorGraphScalarU8QuantizedScorer
	ready  bool
}

func (p *columnHNSWPreparedScalarU8ScorePlane) kind() columnHNSWPreparedTraversalScorePlaneKind {
	return columnHNSWPreparedTraversalScorePlaneKindQuantized
}

func (p *columnHNSWPreparedScalarU8ScorePlane) prepareForHNSWPreparedTraversal(pack *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch) error {
	_ = opts
	_ = scratch
	if p == nil || !p.ready || pack == nil {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	if len(query) != pack.Header.Dimensions || p.scorer.dims != pack.Header.Dimensions || p.scorer.codeRows.Rows() != pack.Header.Rows {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.validatePrepared()
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if p == nil || !p.ready {
		return 0, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreOrdinal(ordinal, scratch, stats)
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if p == nil || !p.ready {
		return dst[:0], errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreOrdinals(ordinals, dst, scratch, stats)
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreRowIDsPrevalidated(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if p == nil || !p.ready {
		return dst[:0], errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreRowIDsPrevalidated(rowIDs, dst, scratch, stats)
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreGreedyBestRowIDsPrevalidated(rowIDs []uint32, best int, bestScore float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, float64, bool, error) {
	if p == nil || !p.ready {
		return best, bestScore, false, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreGreedyBestRowIDsPrevalidated(rowIDs, best, bestScore, scratch, stats)
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreRawDotOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int64, error) {
	if p == nil || !p.ready {
		return 0, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreRawDotOrdinal(ordinal, scratch, stats)
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreRawDotRowIDsPrevalidated(rowIDs []uint32, dst []int64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]int64, error) {
	if p == nil || !p.ready {
		return dst[:0], errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreRawDotRowIDsPrevalidated(rowIDs, dst, scratch, stats)
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	if p == nil || !p.ready {
		return 0, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs, topK, scratch, stats)
}

func (p *columnHNSWPreparedScalarU8ScorePlane) scoreAndPushRawDotFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	if p == nil || !p.ready {
		return 0, errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	return p.scorer.scoreAndPushRawDotFrontierVisitedRowIDsPrevalidated(rowIDs, topK, scratch, stats)
}

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
	if len(query) != r.def.Dimensions {
		return nil, setupStats, fmt.Errorf("collections: column_graph %q hnsw prepared quantized traversal query dims=%d want %d: %w", r.def.Name, len(query), r.def.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
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
	scratch.preparedScalarU8Plane.ready = false
	scorer, err := r.prepareScalarU8QuantizedScorer(queryMode, opts.QuantizedIndexName, query, queryInvNorm, scratch)
	if err != nil {
		recordColumnVectorGraphQuantizedAssetErrorStats(&setupStats, err)
		return nil, setupStats, err
	}
	scratch.preparedScalarU8Plane.scorer = scorer
	scratch.preparedScalarU8Plane.ready = true
	setupStats.QuantizedScorerActive = 1
	if r.preparedSearch != nil && r.preparedSearch.ready() {
		setupStats.PreparedGraphSearchViews = 1
	}
	packOpts := columnHNSWPreparedTraversalOptions{
		TopK:                                 opts.TopK,
		EfSearch:                             opts.EfSearch,
		ScoreTileCapacity:                    min(rerankCandidateLimit, efSearch),
		RetainedCandidateLimit:               0,
		ScoreBatchMode:                       opts.ScoreBatchMode,
		StatsMode:                            opts.StatsMode,
		OmitResultMaterialization:            opts.OmitResultMaterialization || queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		SuppressOmittedResultMaterialization: queryMode == columnVectorGraphNativeSearchQueryModeQuantizedRerank,
	}
	var results []columnVectorGraphNativeSearchResult
	var stats columnVectorGraphNativeSearchStats
	if columnHNSWPreparedScalarU8RawDotTraversalEnabled {
		results, stats, err = pack.searchCosinePreparedRawDotTraversal(query, packOpts, scratch, &scratch.preparedScalarU8Plane)
	} else {
		results, stats, err = pack.searchCosinePreparedScorePlane(query, packOpts, scratch, &scratch.preparedScalarU8Plane)
	}
	columnVectorGraphApplyQuantizedPreparedTraversalSetupStats(&stats, setupStats)
	r.populateScalarU8PreparedTraversalSearchStats(&stats)
	if err != nil {
		return nil, stats, err
	}
	if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedOnly {
		return results, stats, nil
	}

	// The pack traversal retained the quantized candidate set in scratch.top without
	// reading result IDs/row refs. Rerank only the configured shortlist through the
	// prepared pack's exact FP32 row-ID scorer; do not build the generic native
	// search plan or re-enter the row-reader scorer stack on this prepared route.
	scratch.results = scratch.results[:0]
	if err := pack.exactRerankPreparedTraversalRowIDCandidates(query, topK, rerankCandidateLimit, opts.ScoreBatchMode, scratch, &stats); err != nil {
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
