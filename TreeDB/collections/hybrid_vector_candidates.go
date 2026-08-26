package collections

import (
	"errors"
	"fmt"
)

const (
	hybridVectorScalarPrefilterExactMax         = 4 * 1024
	hybridVectorScalarPrefilterExactMaxScanRows = 4 * 1024
)

var errHybridVectorAllowSetScanBudgetExceeded = errors.New("collections: hybrid vector scalar allow-set exact scan budget exceeded")

// SearchHybridVectorCandidates adapts an existing collection vector-index
// search into the shared hybrid candidate shape.
//
// Candidate generation is no-document by construction: it uses
// SearchVectorIndexWithBuffer with IncludeDocuments=false, copies stable result
// IDs into response-owned HybridSearchCandidate values, assigns one-based source
// ranks, and fails closed if the backing vector path reports any document
// materialization or unavailable vector-index state.
func (c *Collection) SearchHybridVectorCandidates(query HybridVectorQuery) (HybridCandidateResponse, error) {
	return c.searchHybridVectorCandidates(query, nil)
}

func (c *Collection) searchHybridVectorCandidates(query HybridVectorQuery, allowSet hybridScalarAllowSet) (HybridCandidateResponse, error) {
	return c.searchHybridVectorCandidatesWithAllowSetBudget(query, allowSet, query.CandidateLimit)
}

func (c *Collection) searchHybridVectorCandidatesWithAllowSetBudget(query HybridVectorQuery, allowSet hybridScalarAllowSet, allowSetBudget int) (HybridCandidateResponse, error) {
	if c == nil {
		return HybridCandidateResponse{}, errCollectionNil
	}
	if c.db == nil {
		return HybridCandidateResponse{}, errCollectionDBNil
	}
	requested := query.CandidateLimit
	if err := validateHybridVectorCandidateQuery(query); err != nil {
		response := HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, VectorIndexSearchStats{}, 0)}
		response.Stats.FailClosed = 1
		response.Stats.FailClosedReason = HybridFailClosedReasonUnsupported
		return response, err
	}
	if allowSet != nil && len(allowSet) == 0 {
		return HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, VectorIndexSearchStats{}, 0)}, nil
	}
	if allowSetBudget < requested {
		allowSetBudget = requested
	}
	if hybridVectorCandidateUseExactAllowSetBudget(query, allowSet, allowSetBudget) {
		response, err := c.searchHybridVectorCandidatesAllowSet(query, allowSet)
		if !errors.Is(err, errHybridVectorAllowSetScanBudgetExceeded) {
			return response, err
		}
	}

	opts := hybridVectorSearchOptions(query)
	var buffer VectorIndexSearchBuffer
	vectorResponse, err := c.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		response := HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, vectorResponse.Stats, 0)}
		response.Stats.FailClosed = 1
		response.Stats.FailClosedReason = hybridVectorCandidateFailClosedReason(err)
		return response, hybridVectorCandidateError(err, query.IndexName)
	}

	return hybridVectorCandidatesFromSearchResponse(requested, query.IndexName, vectorResponse)
}
func (c *Collection) searchHybridVectorCandidatesNativeScalar(query HybridVectorQuery, filter *HybridScalarFilter) (HybridCandidateResponse, error) {
	if err := validateHybridVectorCandidateQuery(query); err != nil {
		return HybridCandidateResponse{}, err
	}
	opts := hybridVectorSearchOptions(query)
	opts.StatsMode = VectorIndexSearchStatsModeProduction
	opts.DeclaredScalarFilter = filter
	var buffer VectorIndexSearchBuffer
	vectorResponse, err := c.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		stats := hybridVectorCandidateStatsFromSearch(query.CandidateLimit, vectorResponse.Stats, 0)
		stats.FailClosed = 1
		stats.FailClosedReason = hybridVectorCandidateFailClosedReason(err)
		return HybridCandidateResponse{Stats: stats}, hybridVectorCandidateError(err, query.IndexName)
	}
	return hybridVectorCandidatesFromSearchResponse(query.CandidateLimit, query.IndexName, vectorResponse)
}

func hybridVectorSearchOptions(query HybridVectorQuery) VectorIndexSearchOptions {
	return VectorIndexSearchOptions{
		IndexName:                 query.IndexName,
		Query:                     query.Query,
		QueryMode:                 query.QueryMode,
		QuantizedIndexName:        query.QuantizedIndexName,
		QuantizedRerankCandidates: query.QuantizedRerankCandidates,
		TopK:                      query.CandidateLimit,
		EfSearch:                  query.EfSearch,
		StatsMode:                 VectorIndexSearchStatsModeFullDiagnostics,
	}
}

func hybridVectorCandidateUseExactAllowSet(query HybridVectorQuery, allowSet hybridScalarAllowSet) bool {
	return hybridVectorCandidateUseExactAllowSetBudget(query, allowSet, query.CandidateLimit)
}

func hybridVectorCandidateUseExactAllowSetBudget(query HybridVectorQuery, allowSet hybridScalarAllowSet, allowSetBudget int) bool {
	if allowSet == nil || len(allowSet) == 0 || query.CandidateLimit <= 0 || allowSetBudget <= 0 {
		return false
	}
	if allowSetBudget > hybridVectorScalarPrefilterExactMax {
		return false
	}
	return len(allowSet) <= allowSetBudget
}

func validateHybridVectorCandidateQuery(query HybridVectorQuery) error {
	if query.CandidateLimit < 0 {
		return fmt.Errorf("%w: vector candidate limit cannot be negative", ErrHybridSearchUnsupported)
	}
	if query.EfSearch < 0 {
		return fmt.Errorf("%w: vector candidate ef_search cannot be negative", ErrHybridSearchUnsupported)
	}
	if query.QueryMode != "" && query.QueryMode != VectorIndexQueryModeExact {
		return fmt.Errorf("%w: vector candidate query mode %q is unsupported by the vector candidate adapter", ErrHybridSearchUnsupported, query.QueryMode)
	}
	if query.QuantizedIndexName != "" {
		return fmt.Errorf("%w: vector candidate quantized index %q is unsupported by the vector-only #2503 split", ErrHybridSearchUnsupported, query.QuantizedIndexName)
	}
	if query.QuantizedRerankCandidates != 0 {
		return fmt.Errorf("%w: vector candidate quantized rerank candidates are unsupported by the vector-only #2503 split", ErrHybridSearchUnsupported)
	}
	return nil
}

func (c *Collection) searchHybridVectorCandidatesAllowSet(query HybridVectorQuery, allowSet hybridScalarAllowSet) (HybridCandidateResponse, error) {
	requested := query.CandidateLimit
	opts := hybridVectorSearchOptions(query)
	statsMode, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		response := HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, VectorIndexSearchStats{}, 0)}
		response.Stats.FailClosed = 1
		response.Stats.FailClosedReason = HybridFailClosedReasonUnsupported
		return response, hybridVectorCandidateError(err, query.IndexName)
	}
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		response := HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, VectorIndexSearchStats{}, 0)}
		response.Stats.FailClosed = 1
		response.Stats.FailClosedReason = HybridFailClosedReasonUnsupported
		return response, hybridVectorCandidateError(err, query.IndexName)
	}

	slot := collectionVectorIndexPreparedSearchCacheSlotForOptions(opts, queryMode)
	var lastResponse VectorIndexSearchResponse
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		prepared, vectorResponse, acquireStats, err := c.acquireCollectionVectorIndexPreparedSearch(opts)
		if err != nil {
			acquireStats.apply(&vectorResponse.Stats)
			response := HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, vectorResponse.Stats, 0)}
			response.Stats.FailClosed = 1
			response.Stats.FailClosedReason = hybridVectorCandidateFailClosedReason(err)
			return response, hybridVectorCandidateError(err, query.IndexName)
		}
		vectorResponse, err = prepared.searchHybridVectorAllowSetNoDocuments(opts, statsMode, allowSet)
		acquireStats.apply(&vectorResponse.Stats)
		if errors.Is(err, errHybridVectorAllowSetScanBudgetExceeded) {
			return HybridCandidateResponse{}, err
		}
		if err == nil && hybridVectorAllowSetNoDocumentGuardrailsOK(vectorResponse) {
			response, err := hybridVectorCandidatesFromSearchResponse(requested, query.IndexName, vectorResponse)
			if err != nil {
				return response, err
			}
			hybridVectorCandidateAddAllowSetStats(&response.Stats, vectorResponse.Stats)
			return response, nil
		}
		if err != nil && hybridVectorAllowSetNoDocumentGuardrailsOK(vectorResponse) {
			response := HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, vectorResponse.Stats, 0)}
			response.Stats.FailClosed = 1
			response.Stats.FailClosedReason = hybridVectorCandidateFailClosedReason(err)
			return response, hybridVectorCandidateError(err, query.IndexName)
		}
		lastResponse = vectorResponse
		lastErr = err
		c.invalidateCollectionVectorIndexPreparedSearch(slot, prepared)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%w: vector index %q scalar allow-set route did not satisfy no-document guardrails", ErrVectorIndexSearchUnavailable, query.IndexName)
	}
	response := HybridCandidateResponse{Stats: hybridVectorCandidateStatsFromSearch(requested, lastResponse.Stats, 0)}
	response.Stats.FailClosed = 1
	response.Stats.FailClosedReason = hybridVectorCandidateFailClosedReason(lastErr)
	return response, hybridVectorCandidateError(lastErr, query.IndexName)
}

func (p *collectionVectorIndexPreparedSearch) searchHybridVectorAllowSetNoDocuments(opts VectorIndexSearchOptions, statsMode columnVectorGraphNativeSearchStatsMode, allowSet hybridScalarAllowSet) (VectorIndexSearchResponse, error) {
	if p == nil {
		return VectorIndexSearchResponse{}, errors.New("collections: nil collection vector index prepared search")
	}
	response := p.responseForSearch()
	var scratch columnVectorGraphNativeSearchScratch
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed || p.pack == nil {
		response.Stats = VectorIndexSearchStats{HNSWSearchPackClosed: 1, HNSWSearchPackFallbacks: 1}
		return response, errColumnHNSWSearchPackPreparedViewClosed
	}
	status := p.pack.fastStatus(p.packStatus)
	if status != columnHNSWSearchPackPreparedStatusDirect && status != columnHNSWSearchPackPreparedStatusHeap {
		routeStats := collectionVectorIndexPreparedSearchRouteStatsForUnavailable(p.routeStats, status)
		routeStats.apply(&response.Stats)
		return response, columnHNSWSearchPackStatusError(status)
	}
	results, searchStats, err := p.pack.searchCosineAllowSet(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:           opts.TopK,
		EfSearch:       opts.EfSearch,
		ScoreBatchMode: opts.scoreBatchMode,
		StatsMode:      statsMode,
		QueryMode:      columnVectorGraphNativeSearchQueryModeExact,
	}, allowSet, &scratch)
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, columnPhysicalRowReaderStats{})
	p.routeStats.apply(&response.Stats)
	if err != nil {
		return response, err
	}
	response.Results, err = copyVectorIndexSearchResultsToOwned(results)
	if err != nil {
		return response, err
	}
	markVectorIndexSearchResponseOwnedResultAllocs(&response)
	return response, nil
}

func (v *columnHNSWSearchPackPreparedView) searchCosineAllowSet(query []float32, opts columnVectorGraphNativeSearchOptions, allowSet hybridScalarAllowSet, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
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
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 allow-set query dims=%d want %d: %w", len(query), v.Header.Dimensions, errColumnVectorGraphNativeSearchQueryDimensionMismatch)
	}
	queryMode, err := opts.QueryMode.normalized()
	if err != nil {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 allow-set query mode: %w", err)
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeExact || opts.QuantizedIndexName != "" || opts.QuantizedRerankCandidates != 0 {
		return nil, stats, errColumnHNSWSearchPackSearchUnsupportedMode
	}
	statsMode := opts.StatsMode.normalized()
	if !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		return nil, stats, errColumnHNSWSearchPackSearchUnsupportedMode
	}
	columnVectorGraphNativeSearchStartWorkAccounting(&stats, statsMode)
	rowCount := v.Header.Rows
	topK := opts.TopK
	if topK < 0 {
		return nil, stats, errColumnVectorGraphNativeSearchTopKNegative
	}
	if topK == 0 || rowCount == 0 || len(allowSet) == 0 {
		return nil, stats, nil
	}
	if !hybridVectorAllowSetExactScanEligible(rowCount) {
		return nil, stats, errHybridVectorAllowSetScanBudgetExceeded
	}
	if topK > len(allowSet) {
		topK = len(allowSet)
	}
	if topK > rowCount {
		topK = rowCount
	}
	degree := v.Header.M
	if degree < 1 {
		degree = 1
	}
	if degree <= maxCollectionInt/2 {
		degree *= 2
	}
	if err := scratch.prepareHNSWSearchPack(rowCount, v.Header.VectorStride, degree, topK, topK, 0, 0, 0); err != nil {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 allow-set search scratch prepare: %w", err)
	}
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 allow-set query norm: %w: %w", errColumnVectorGraphNativeSearchQueryNormInvalid, err)
	}
	normalizedQuery := scratch.scoreScratch.Float32Values[:v.Header.VectorStride]
	for i := 0; i < v.Header.Dimensions; i++ {
		normalizedQuery[i] = query[i] * queryInvNorm
	}
	if v.Header.VectorStride > v.Header.Dimensions {
		clear(normalizedQuery[v.Header.Dimensions:])
	}

	rowIDs := make([]uint32, 0, minInt(len(allowSet), rowCount))
	for ordinal := 0; ordinal < rowCount; ordinal++ {
		id, ok := v.documentIDForOrdinal(ordinal)
		if !ok {
			return nil, stats, fmt.Errorf("collections: hnsw_search_pack_v1 allow-set result-id unavailable for ordinal=%d", ordinal)
		}
		if _, allowed := allowSet[unsafeStringFromBytes(id)]; !allowed {
			stats.FilterSkips++
			continue
		}
		rowIDs = append(rowIDs, uint32(ordinal))
	}
	stats.CandidateRows = uint64(len(rowIDs))
	stats.Candidates = uint64(len(rowIDs))
	if len(rowIDs) == 0 {
		return scratch.results, stats, nil
	}
	scores, err := v.scoreRowIDs(normalizedQuery, rowIDs, scratch.scoreTileScores, opts.ScoreBatchMode, scratch, &stats)
	if err != nil {
		return nil, stats, err
	}
	for i, rowID := range rowIDs {
		scratch.insertTop(topK, columnVectorGraphSearchCandidate{ordinal: int(rowID), score: scores[i]})
	}
	if len(scratch.top) == 0 {
		return scratch.results, stats, nil
	}
	scratch.retainTopBestFirst(topK)
	if err := v.fetchTopSearchResults(scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func hybridVectorAllowSetExactScanEligible(rowCount int) bool {
	return rowCount >= 0 && rowCount <= hybridVectorScalarPrefilterExactMaxScanRows
}

func hybridVectorAllowSetNoDocumentGuardrailsOK(response VectorIndexSearchResponse) bool {
	stats := response.Stats
	return stats.SearchRouteHNSWSearchPack == 1 &&
		stats.HNSWSearchPackActive == 1 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.DocumentsFetched == 0 &&
		stats.DocumentsMissing == 0 &&
		stats.DocumentBytes == 0 &&
		stats.DocumentOutputBytes == 0 &&
		stats.DocumentFetchNanos == 0
}

func hybridVectorCandidateAddAllowSetStats(stats *HybridSearchStats, vectorStats VectorIndexSearchStats) {
	if stats == nil {
		return
	}
	// Returned candidates still pass through the executor's normal scalar ID
	// filter, which records matched/check counters once. The vector allow-set
	// route adds only rows pruned before vector scoring so selectivity counters do
	// not double-count already-allowed candidates.
	stats.ScalarFilterRejected += vectorStats.FilterSkips
}

func hybridVectorCandidatesFromSearchResponse(requested int, vectorIndexName string, vectorResponse VectorIndexSearchResponse) (HybridCandidateResponse, error) {
	stats := hybridVectorCandidateStatsFromSearch(requested, vectorResponse.Stats, 0)
	if !hybridVectorCandidateNoDocumentGuardrailsOK(vectorResponse) {
		stats.FailClosed = 1
		stats.FailClosedReason = HybridFailClosedReasonFullDocumentScanForbidden
		return HybridCandidateResponse{Stats: stats}, fmt.Errorf("%w: vector candidate generation fetched documents", ErrHybridSearchUnsupported)
	}

	indexName := vectorResponse.IndexName
	if indexName == "" {
		indexName = vectorIndexName
	}
	candidates := make([]HybridSearchCandidate, len(vectorResponse.Results))
	for i, result := range vectorResponse.Results {
		id := append([]byte(nil), result.ID...)
		id = id[:len(id):len(id)]
		candidates[i] = HybridSearchCandidate{
			ID:         id,
			Source:     HybridCandidateSourceVector,
			IndexName:  indexName,
			SourceRank: i + 1,
			Score:      result.Score,
			ScoreKind:  HybridScoreKindVectorSimilarity,
		}
	}
	stats = hybridVectorCandidateStatsFromSearch(requested, vectorResponse.Stats, len(candidates))
	return HybridCandidateResponse{Stats: stats, Candidates: candidates}, nil
}

func hybridVectorCandidateStatsFromSearch(requested int, vectorStats VectorIndexSearchStats, returned int) HybridSearchStats {
	var requested64 uint64
	if requested > 0 {
		requested64 = uint64(requested)
	}
	returned64 := uint64(returned)
	stats := HybridSearchStats{
		VectorCandidatesRequested:      requested64,
		VectorCandidateBudgetEffective: requested64,
		VectorCandidatesReturned:       returned64,
		VectorCandidatesExamined: hybridMaxUint64(
			vectorStats.Candidates,
			vectorStats.VisitedNodes,
			vectorStats.CandidateFetches,
			vectorStats.ScoreBatchCandidates,
			vectorStats.PreparedScoreCalls,
			vectorStats.QuantizedScoreCalls,
			vectorStats.QuantizedRerankExactScoreCalls,
		),
		VectorEdgesVisited: hybridMaxUint64(
			vectorStats.VisitedEdges,
			vectorStats.Edges,
			vectorStats.UpperLayerEdgeVisits+vectorStats.Layer0EdgeVisits,
		),
		DocumentsFetched: vectorStats.DocumentsFetched,
		DocumentsMissing: vectorStats.DocumentsMissing,
	}
	stats.ScalarFilterPlan = vectorStats.ScalarFilterPlan
	stats.ScalarFilterInputIDs = vectorStats.ScalarFilterProbeIDs
	stats.ScalarFilterProbeTruncated = vectorStats.ScalarFilterProbeTruncated
	stats.ScalarFilterFinalIDs = vectorStats.ScalarFilterCandidateIDs
	stats.ScalarFilterVisited = vectorStats.ScalarFilterVisited
	stats.ScalarFilterMatched = vectorStats.ScalarFilterAdmitted
	stats.ScalarFilterUnderfill = vectorStats.ScalarFilterUnderfill
	stats.ScalarFilterExactScoring = vectorStats.ScalarFilterExactScoring
	if vectorStats.CandidateRows > returned64 {
		stats.Truncated = vectorStats.CandidateRows - returned64
	}
	return stats
}

func hybridVectorCandidateNoDocumentGuardrailsOK(response VectorIndexSearchResponse) bool {
	stats := response.Stats
	if stats.DocumentsFetched != 0 || stats.DocumentsMissing != 0 || stats.DocumentBytes != 0 || stats.DocumentOutputBytes != 0 || stats.DocumentFetchNanos != 0 {
		return false
	}
	if stats.DocumentFieldsReconstructed != 0 || stats.DocumentFieldsSkipped != 0 || stats.DocumentRetainedFetches != 0 || stats.DocumentRetainedBytes != 0 {
		return false
	}
	if stats.DocumentVisibilityScans != 0 || stats.DocumentVisibilityRowsScanned != 0 || stats.DocumentVisibilityRows != 0 || stats.DocumentVisibilityPhysicalBytes != 0 || stats.DocumentVisibilityNanos != 0 {
		return false
	}
	if stats.DocumentTypedColumnRows != 0 || stats.DocumentTypedColumnPartLoads != 0 || stats.DocumentTypedColumnPartDecodes != 0 || stats.DocumentJSONReconstructionRows != 0 {
		return false
	}
	if stats.DocumentPointRowFetches != 0 || stats.DocumentPointRowDecodes != 0 || stats.DocumentRowRefFallbackScans != 0 || stats.DocumentRowRefUnsupported != 0 {
		return false
	}
	for _, result := range response.Results {
		if len(result.Document) != 0 {
			return false
		}
	}
	return true
}

func hybridVectorCandidateFailClosedReason(err error) HybridFailClosedReason {
	if errors.Is(err, ErrVectorIndexSearchUnavailable) || errors.Is(err, ErrIndexNotFound) {
		return HybridFailClosedReasonVectorIndexUnavailable
	}
	return HybridFailClosedReasonUnsupported
}

func hybridVectorCandidateError(err error, indexName string) error {
	if errors.Is(err, ErrVectorIndexSearchUnavailable) || errors.Is(err, ErrIndexNotFound) {
		return fmt.Errorf("%w: vector index %q candidate generation unavailable: %w", ErrHybridSearchIndexUnavailable, indexName, err)
	}
	return fmt.Errorf("%w: vector index %q candidate generation unsupported: %w", ErrHybridSearchUnsupported, indexName, err)
}
