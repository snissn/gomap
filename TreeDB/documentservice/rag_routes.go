package documentservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// searchKeywordWithScalarFilter serves filtered keyword search through the
// collection hybrid executor's bounded scalar prefilter vocabulary. Equality
// and range leaves joined by AND compile into one finite indexed intersection
// before candidate generation. The executor consumes the final allow-set inside
// posting-block scans and fails closed on any incomplete lookup.
func (s *Service) searchKeywordWithScalarFilter(ctx context.Context, col *collections.Collection, info IndexInfo, req KeywordSearchRequest, operator collections.TextSearchOperator) (KeywordSearchResponse, error) {
	schema := newScalarSchema(info.ScalarFields)
	scalarFilter, err := translateScalarFilter(req.Filter, schema)
	if err != nil {
		return KeywordSearchResponse{}, err
	}
	opts := collections.HybridSearchOptions{
		Context: ctx,
		TopK:    req.TopK,
		Text: &collections.HybridTextQuery{
			IndexName:          defaultTextIndexName,
			Query:              req.Query,
			QueryMode:          req.TextQueryMode,
			Operator:           operator,
			CandidateLimit:     req.CandidateLimit,
			MaxPostingsScanned: req.MaxPostingsScanned,
			IncludeTextMatches: true,
		},
		ScalarFilter:         scalarFilter,
		IncludeDocuments:     true,
		DocumentFetchOptions: serviceDocumentFetchOptions(req.ReturnEmbedding),
	}
	response := KeywordSearchResponse{Index: info, TextIndex: defaultTextIndexName}
	if err := ctxErr(ctx); err != nil {
		return response, err
	}
	hybrid, err := col.SearchHybrid(opts)
	mapped, statsErr := keywordResponseFromHybrid(hybrid, info, req.ReturnEmbedding)
	response = mapped
	if statsErr != nil {
		return response, statsErr
	}
	if err != nil {
		if errors.Is(err, collections.ErrHybridSearchUnsupported) {
			return response, wrapServiceError(CodeInvalidRequest, "filtered keyword search request is invalid or unsupported", err)
		}
		return response, mappedHybridSearchError("filtered keyword search", err, hybrid.Stats)
	}
	return response, nil
}

// keywordResponseFromHybrid maps text-only hybrid executor results back into
// the keyword contract shape: native BM25F scores, deterministic rank order,
// and keyword-style _treedb_search attribution.
func keywordResponseFromHybrid(hybrid collections.HybridSearchResponse, info IndexInfo, returnEmbedding bool) (KeywordSearchResponse, error) {
	docs := make([]Document, 0, len(hybrid.Results))
	for _, result := range hybrid.Results {
		if !result.DocumentFound || len(result.Document) == 0 {
			return KeywordSearchResponse{}, serviceErrorf(CodeIndexUnavailable, "filtered keyword result document %q was not fetched", string(result.ID))
		}
		doc, err := decodeStoredDocument(result.ID, result.Document)
		if err != nil {
			return KeywordSearchResponse{}, err
		}
		if !returnEmbedding {
			doc.Embedding = nil
		}
		var text *collections.HybridSourceContribution
		for i := range result.Sources {
			if result.Sources[i].Source == collections.HybridCandidateSourceText {
				text = &result.Sources[i]
				break
			}
		}
		if text == nil {
			return KeywordSearchResponse{}, serviceErrorf(CodeInternal, "filtered keyword result %q has no text contribution", string(result.ID))
		}
		doc.Score = scorePtr(text.Score)
		attachSearchMeta(&doc, filteredKeywordMeta(defaultTextIndexName, result.Rank, text))
		docs = append(docs, doc)
	}
	stats := hybrid.Stats
	return KeywordSearchResponse{
		Index:     info,
		TextIndex: defaultTextIndexName,
		Documents: docs,
		Stats: KeywordSearchStats{
			CandidatesRequested:           stats.TextCandidatesRequested,
			CandidatesReturned:            stats.TextCandidatesReturned,
			PostingsScanned:               stats.TextPostingsScanned,
			CandidatesScored:              stats.TextCandidatesScored,
			DocumentsFetched:              stats.DocumentsFetched,
			DocumentsMissing:              stats.DocumentsMissing,
			FullDocumentScanFallbacks:     stats.FullDocumentScanFallbacks,
			Truncated:                     stats.Truncated > 0,
			FailClosed:                    stats.FailClosed,
			FailClosedReason:              string(stats.FailClosedReason),
			ScalarPrefilterIDs:            stats.ScalarPrefilterIDs,
			ScalarFilterLookups:           stats.ScalarFilterLookups,
			ScalarFilterInputIDs:          stats.ScalarFilterInputIDs,
			ScalarFilterIntersectionSteps: stats.ScalarFilterIntersectionSteps,
			ScalarFilterFinalIDs:          stats.ScalarFilterFinalIDs,
		},
	}, nil
}

func filteredKeywordMeta(textIndex string, rank int, text *collections.HybridSourceContribution) map[string]any {
	meta := map[string]any{
		"type":       "keyword",
		"text_index": textIndex,
		"rank":       rank,
		"score_kind": string(text.ScoreKind),
	}
	fields := map[string]struct{}{}
	terms := map[string]struct{}{}
	for _, match := range text.TextMatches {
		fields[match.Field] = struct{}{}
		for _, term := range match.Terms {
			terms[term] = struct{}{}
		}
	}
	meta["matched_terms"] = sortedSetStrings(terms)
	meta["matched_fields"] = sortedSetStrings(fields)
	if len(text.TextMatches) > 0 {
		meta["text_matches"] = hybridTextMatchesMeta(text.TextMatches)
	}
	return meta
}

func sortedSetStrings(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// searchDenseVectorAnn serves the selected ColumnGraph ANN route.
func (s *Service) searchDenseVectorAnn(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest) (DenseVectorSearchResponse, error) {
	if info.TypedInput {
		return s.searchDenseVectorNative(ctx, col, info, req)
	}
	raw, err := s.searchDenseVectorOrdinaryAnnRaw(ctx, col, info, req, nil)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	return decodeDenseRawDocuments(raw, req.ReturnEmbedding)
}

// The ordinary service and wire route share the maintained search/fetch call.
// Its returned documents belong to that search; no primary lookup follows it.
func (s *Service) searchDenseVectorOrdinaryAnnRaw(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest, dst []RawDenseVectorResult) (RawDenseVectorSearchResponse, error) {
	if !info.Capabilities.ColumnGraphVectorSearch {
		return RawDenseVectorSearchResponse{}, serviceError(CodeUnsupported, "dense route \"ann\" requires a cosine column_graph vector index; use route \"exact\" for this index")
	}
	if err := ctxErr(ctx); err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	fetchOptions := serviceDocumentFetchOptions(req.ReturnEmbedding)
	fetchOptions.Context = ctx
	search, err := col.SearchVectorIndex(collections.VectorIndexSearchOptions{
		Context:              ctx,
		IndexName:            defaultVectorIndexName,
		Query:                append([]float32(nil), req.QueryEmbedding...),
		QueryMode:            collections.VectorIndexQueryModeExact,
		TopK:                 req.TopK,
		EfSearch:             req.EfSearch,
		IncludeDocuments:     true,
		DocumentFetchOptions: fetchOptions,
	})
	if err != nil {
		return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("ann vector search", err)
	}
	return ordinaryDenseVectorANNResponse(info, search, dst)
}

// ordinaryDenseVectorANNResponse preserves the collection route proof, including
// compatibility traversal on platforms without mmap. It never selects exact search.
func ordinaryDenseVectorANNResponse(info IndexInfo, search collections.VectorIndexSearchResponse, dst []RawDenseVectorResult) (RawDenseVectorSearchResponse, error) {
	stats := search.Stats
	prepared, pack, fallback := stats.SearchRouteColumnGraphPrepared, stats.SearchRouteHNSWSearchPack, stats.SearchRouteColumnGraphFallback
	oneRoute := (prepared == 1 && pack == 0 && fallback == 0) ||
		(prepared == 0 && pack == 1 && fallback == 0) ||
		(prepared == 0 && pack == 0 && fallback == 1)
	if search.Strategy != collections.VectorIndexStrategyColumnGraph || search.Path != collections.VectorIndexSearchPathColumnGraphNativeReader || !oneRoute || stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 0 {
		return RawDenseVectorSearchResponse{}, serviceError(CodeIndexUnavailable, "ordinary ann search did not prove one ColumnGraph traversal route")
	}
	dst = resizeDenseRawResults(dst, len(search.Results))
	for i, result := range search.Results {
		if len(result.Document) == 0 {
			return RawDenseVectorSearchResponse{}, serviceErrorf(CodeIndexUnavailable, "ann vector result %q was not materialized in the search snapshot", string(result.ID))
		}
		dst[i] = RawDenseVectorResult{ID: result.ID, Score: result.Score, Document: result.Document}
	}
	return RawDenseVectorSearchResponse{
		Results: dst, Route: RouteAnn, Candidates: len(search.Results),
		ColumnGraphPreparedSearch: search.Stats.SearchRouteColumnGraphPrepared,
		SearchPackSearch:          search.Stats.SearchRouteHNSWSearchPack,
		ColumnGraphFallbackSearch: search.Stats.SearchRouteColumnGraphFallback,
		ColumnGraphDeltaScored:    search.Stats.ColumnGraphDeltaScored,
		info:                      info, searchStats: search.Stats,
	}, nil
}

func (s *Service) searchDenseVectorNative(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest) (DenseVectorSearchResponse, error) {
	raw, err := s.searchDenseVectorNativeRaw(ctx, col, info, req, nil)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	return decodeDenseRawDocuments(raw, req.ReturnEmbedding)
}

func decodeDenseRawDocuments(raw RawDenseVectorSearchResponse, returnEmbedding bool) (DenseVectorSearchResponse, error) {
	docs := make([]Document, 0, len(raw.Results))
	for _, result := range raw.Results {
		doc, err := decodeStoredDocument(result.ID, result.Document)
		if err != nil {
			return DenseVectorSearchResponse{}, withDenseSearchWorkAndScorePlane(err, raw.DenseWork, raw.ScorePlane)
		}
		if !returnEmbedding {
			doc.Embedding = nil
		}
		doc.Score = scorePtr(result.Score)
		docs = append(docs, doc)
	}
	return raw.response(docs), nil
}

// RawDenseVectorResult is the native ANN result before document JSON decoding.
type RawDenseVectorResult struct {
	ID       []byte
	Score    float64
	Document []byte
}

// RawDenseVectorSearchResponse reuses caller-provided result storage when
// supplied; each ID and Document is response-owned.
type RawDenseVectorSearchResponse struct {
	DenseWork     *DenseSearchWork
	ScorePlane    *collections.ColumnGraphScorePlaneWork
	RouteIdentity *DenseSearchRouteIdentity
	Diagnostics   bool
	// TypedColumnGraph identifies the validated selected route, not a work proof.
	TypedColumnGraph          bool
	ColumnGraphPreparedSearch uint64
	SearchPackSearch          uint64
	// ColumnGraphFallbackSearch counts compatibility graph traversal, never exact/full scan.
	ColumnGraphFallbackSearch uint64
	ColumnGraphDeltaScored    uint64
	DocumentsScanned          uint64
	Exact                     bool
	Results                   []RawDenseVectorResult
	Route                     Route
	Candidates                int
	ExactFallbacks            uint64
	FullDocumentScanFallbacks uint64
	info                      IndexInfo
	searchStats               collections.VectorIndexSearchStats
	fetchedStats              collections.DocumentMaterializationStats
}

func resizeDenseRawResults(dst []RawDenseVectorResult, count int) []RawDenseVectorResult {
	if count > cap(dst) {
		return make([]RawDenseVectorResult, count)
	}
	clear(dst[count:cap(dst)])
	return dst[:count]
}

func denseScorePlaneForResponse(plane collections.ColumnGraphScorePlaneWork, selected bool) *collections.ColumnGraphScorePlaneWork {
	if !selected || !plane.Available {
		return nil
	}
	owned := plane
	return &owned
}

func (r RawDenseVectorSearchResponse) response(docs []Document) DenseVectorSearchResponse {
	stats := r.searchStats
	out := DenseVectorSearchResponse{
		RouteIdentity:             r.RouteIdentity,
		Index:                     r.info,
		Documents:                 docs,
		Metric:                    r.info.Metric,
		Route:                     r.Route,
		Candidates:                r.Candidates,
		Exact:                     r.Exact,
		DocumentsScanned:          r.DocumentsScanned,
		ColumnGraphPreparedSearch: r.ColumnGraphPreparedSearch,
		SearchPackSearch:          r.SearchPackSearch,
		ColumnGraphFallbackSearch: r.ColumnGraphFallbackSearch,
		ColumnGraphDeltaScored:    r.ColumnGraphDeltaScored,
	}
	if !r.Diagnostics {
		return out
	}
	out.DenseWork = r.DenseWork
	out.ScorePlane = r.ScorePlane
	out.ColumnGraphPreparedSearch = stats.SearchRouteColumnGraphPrepared
	out.ColumnGraphDeltaScored = stats.ColumnGraphDeltaScored
	out.ScalarFilterProbeIDs = stats.ScalarFilterProbeIDs
	out.ScalarFilterProbeTruncated = stats.ScalarFilterProbeTruncated
	out.ScalarFilterCandidates = stats.ScalarFilterCandidates
	out.ScalarFilterCandidateIDs = stats.ScalarFilterCandidateIDs
	out.ScalarFilterRetainedCandidateIDs = stats.ScalarFilterRetainedCandidateIDs
	out.ScalarFilterRefinedCandidateIDs = stats.ScalarFilterRefinedCandidateIDs
	out.ScalarFilterVisited = stats.ScalarFilterVisited
	out.ScalarFilterScored = stats.ScalarFilterScored
	out.ScalarFilterAdmitted = stats.ScalarFilterAdmitted
	out.ScalarFilterExactScoring = stats.ScalarFilterExactScoring > 0
	out.ScalarFilterUnderfill = stats.ScalarFilterUnderfill > 0
	out.ScalarFilterPlanCacheHits = stats.ScalarFilterPlanCacheHits
	out.ScalarFilterPlanCacheMisses = stats.ScalarFilterPlanCacheMisses
	out.ScalarFilterPlanCacheInvalidations = stats.ScalarFilterPlanCacheInvalidations
	out.ScalarFilterPlanCacheGenerationBypasses = stats.ScalarFilterPlanCacheGenerationBypasses
	out.ScalarFilterPlanCacheEvictions = stats.ScalarFilterPlanCacheEvictions
	out.ScalarFilterPlanCacheEntries = stats.ScalarFilterPlanCacheEntries
	out.ScalarFilterPlanCacheRetainedBytes = stats.ScalarFilterPlanCacheRetainedBytes
	out.ExactFallbacks = r.ExactFallbacks
	out.FullDocumentScanFallbacks = r.FullDocumentScanFallbacks
	out.AllowedIDMaterializationRows = stats.ScalarFilterRetainedCandidateIDs
	out.DocumentMaterializationRows = r.fetchedStats.DocumentsFetched
	return out
}

// SearchDenseVectorNativeRaw exposes the typed ColumnGraph search and fetch path.
func (s *Service) SearchDenseVectorNativeRaw(ctx context.Context, index string, req DenseVectorSearchRequest) (RawDenseVectorSearchResponse, error) {
	return s.SearchDenseVectorNativeRawInto(ctx, index, req, nil)
}

// SearchDenseVectorNativeRawInto reuses dst for the returned result envelope.
func (s *Service) SearchDenseVectorNativeRawInto(ctx context.Context, index string, req DenseVectorSearchRequest, dst []RawDenseVectorResult) (RawDenseVectorSearchResponse, error) {
	return s.searchDenseVectorRawInto(ctx, index, req, dst, false)
}

// SearchDenseVectorRawInto follows the ordinary public service route policy.
// It neither admits typed requests nor changes the strict native-wire API.
func (s *Service) SearchDenseVectorRawInto(ctx context.Context, index string, req DenseVectorSearchRequest, dst []RawDenseVectorResult) (RawDenseVectorSearchResponse, error) {
	return s.searchDenseVectorRawInto(ctx, index, req, dst, true)
}

func (s *Service) searchDenseVectorRawInto(ctx context.Context, index string, req DenseVectorSearchRequest, dst []RawDenseVectorResult, ordinary bool) (RawDenseVectorSearchResponse, error) {
	col, info, release, err := s.acquireBenchmarkSearchIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	defer release()
	if err := bindDenseVectorRepresentation(&req, info); err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	if req.TopK <= 0 {
		return RawDenseVectorSearchResponse{}, serviceError(CodeInvalidRequest, "top_k must be positive")
	}
	if err := validateEmbedding("query_embedding", req.QueryEmbedding, info.Dimension, info.Metric); err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	if req.EfSearch < 0 {
		return RawDenseVectorSearchResponse{}, serviceError(CodeInvalidRequest, "ef_search must be non-negative")
	}
	if normalized, err := normalizeDenseQueryOptions(&req, info); err != nil {
		return RawDenseVectorSearchResponse{}, err
	} else {
		req.QueryMode = normalized
	}
	route, err := resolveDenseSearchRoute(req, info)
	if err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	if ordinary {
		if info.TypedInput {
			return RawDenseVectorSearchResponse{}, serviceError(CodeUnsupported, "ordinary dense wire search does not admit typed declarations")
		}
		if route == RouteExact {
			response, err := s.searchDenseVectorExact(ctx, col, info, req)
			if err != nil {
				return RawDenseVectorSearchResponse{}, err
			}
			dst = resizeDenseRawResults(dst, len(response.Documents))
			for i, doc := range response.Documents {
				score := *doc.Score
				doc.Score = nil
				encoded, err := json.Marshal(doc)
				if err != nil {
					return RawDenseVectorSearchResponse{}, wrapServiceError(CodeInternal, "exact document response encoding failed", err)
				}
				dst[i] = RawDenseVectorResult{ID: []byte(doc.ID), Score: score, Document: encoded}
			}
			return RawDenseVectorSearchResponse{Results: dst, Route: RouteExact, Exact: true, Candidates: response.Candidates, DocumentsScanned: response.DocumentsScanned, info: info}, nil
		}
		if info.VectorStrategy == collections.VectorIndexStrategyColumnGraph {
			return s.searchDenseVectorOrdinaryAnnRaw(ctx, col, info, req, dst)
		}
	}
	if route != RouteAnn || !info.TypedInput || !info.Capabilities.NoDocumentVectorSearch {
		return RawDenseVectorSearchResponse{}, serviceError(CodeUnsupported, "dense typed wire search requires a selected typed column_graph index")
	}
	return s.searchDenseVectorNativeRawLocked(ctx, col, info, req, dst)
}

func (s *Service) searchDenseVectorNativeRaw(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest, dst []RawDenseVectorResult) (RawDenseVectorSearchResponse, error) {
	s.benchmarkSearchCacheMu.RLock()
	defer s.benchmarkSearchCacheMu.RUnlock()
	if s.closed {
		return RawDenseVectorSearchResponse{}, serviceClosedError()
	}
	return s.searchDenseVectorNativeRawLocked(ctx, col, info, req, dst)
}

func (s *Service) searchDenseVectorNativeRawLocked(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest, dst []RawDenseVectorResult) (response RawDenseVectorSearchResponse, err error) {
	if err := bindDenseVectorRepresentation(&req, info); err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	includeDiagnostics := req.Diagnostics || info.VectorRepresentation != collections.VectorIndexRepresentationCosineNormalizedF32V1
	var proof *DenseSearchWork
	if info.TypedInput && includeDiagnostics {
		proof = &DenseSearchWork{Version: 1}
	}
	defer func() {
		if proof == nil {
			return
		}
		if err != nil {
			clear(dst[:cap(dst)])
			response = RawDenseVectorSearchResponse{}
			err = withDenseSearchWork(err, proof)
			return
		}
		proof.Completed = true
		response.DenseWork = proof
	}()
	workstats.Output.Search.Attempts.Add(1)
	defer func() { workstats.Output.Search.Finish(err == nil) }()
	if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return RawDenseVectorSearchResponse{}, err
		}
	}
	scalarFilter, err := translateScalarFilter(req.Filter, newScalarSchema(info.ScalarFields))
	if err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	buffer := s.benchmarkSearchBufferPool.Get().(*collections.VectorIndexSearchBuffer)
	defer func() { buffer.Reset(); s.benchmarkSearchBufferPool.Put(buffer) }()
	{
		if err := ctxErr(ctx); err != nil {
			return RawDenseVectorSearchResponse{}, err
		}
		statsMode := collections.VectorIndexSearchStatsModeMinimal
		if includeDiagnostics {
			statsMode = collections.VectorIndexSearchStatsModeProduction
		}
		search, view, err := col.SearchVectorIndexWithBufferReadView(collections.VectorIndexSearchOptions{Context: ctx, IndexName: defaultVectorIndexName, Query: req.QueryEmbedding, QueryMode: req.QueryMode, QuantizedIndexName: req.QuantizedIndexName, QuantizedRerankCandidates: req.QuantizedRerankCandidates, TopK: req.TopK, EfSearch: req.EfSearch, StatsMode: statsMode, DeclaredScalarFilter: scalarFilter}, buffer)
		if proof != nil {
			proof.Graph = search.Stats.ColumnGraphWork
		}
		if err != nil {
			return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann vector search", err)
		}
		if err := validateDenseTypedVectorSearchRoute(search); err != nil {
			_ = view.Close()
			return RawDenseVectorSearchResponse{}, err
		}
		if info.TypedInput && info.VectorRepresentation == collections.VectorIndexRepresentationCosineNormalizedF32V1 {
			if err := validateDenseTypedNormalizedVectorSearchRoute(search, req, info); err != nil {
				_ = view.Close()
				return RawDenseVectorSearchResponse{}, err
			}
			if includeDiagnostics && req.QueryMode == collections.VectorIndexQueryModeQuantizedRerank {
				if err := validateDenseTypedQuantizedVectorSearchRoute(search, req, info.Generation, info.VectorEfSearch); err != nil {
					_ = view.Close()
					return RawDenseVectorSearchResponse{}, err
				}
			}
		}
		if s.denseVectorNativeAfterSearch != nil {
			if err := s.denseVectorNativeAfterSearch(0, search); err != nil {
				_ = view.Close()
				return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann vector search hook", err)
			}
		}
		if err := ctxErr(ctx); err != nil {
			_ = view.Close()
			return RawDenseVectorSearchResponse{}, err
		}
		fetchOptions := serviceDocumentFetchOptions(req.ReturnEmbedding)
		fetchOptions.Context = ctx
		fetched, fetchErr := view.FetchDocumentsForVectorIndexSearchResults(search.Results, fetchOptions)
		if proof != nil {
			stats := fetched.Stats
			proof.Output = DenseSearchOutputWork{Attempted: true, Completed: fetchErr == nil,
				Requested: uint64(len(search.Results)), Fetched: stats.DocumentsFetched, Missing: stats.DocumentsMissing,
				OutputBytes: stats.OutputBytes, RetainedPayloadFetches: stats.RetainedPayloadFetches,
				JSONReconstructionRows: stats.JSONReconstructionRows, TypedColumnRows: stats.TypedColumnRows}
		}
		work := serviceMaterializationWork(fetched.Stats)
		work.Requested = uint64(len(search.Results))
		workstats.Output.Search.Add(work)
		closeErr := view.Close()
		if err := ctxErr(ctx); err != nil {
			return RawDenseVectorSearchResponse{}, err
		}
		if fetchErr != nil {
			return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann document fetch", errors.Join(fetchErr, closeErr))
		}
		if closeErr != nil {
			return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann document read view close", closeErr)
		}
		if fetched.Stats.DocumentsRequested != uint64(len(search.Results)) || fetched.Stats.DocumentsFetched != uint64(len(search.Results)) || fetched.Stats.DocumentsMissing != 0 || len(fetched.Results) != len(search.Results) {
			return RawDenseVectorSearchResponse{}, serviceErrorf(CodeIndexUnavailable, "native ann document fetch did not materialize exactly the returned candidates: candidates=%d requested=%d fetched=%d missing=%d results=%d", len(search.Results), fetched.Stats.DocumentsRequested, fetched.Stats.DocumentsFetched, fetched.Stats.DocumentsMissing, len(fetched.Results))
		}
		if len(search.Results) <= cap(dst) {
			if len(search.Results) < cap(dst) {
				clear(dst[len(search.Results):cap(dst)])
			}
			dst = dst[:len(search.Results)]
		} else {
			dst = make([]RawDenseVectorResult, len(search.Results))
		}
		out := RawDenseVectorSearchResponse{
			Diagnostics:               includeDiagnostics,
			TypedColumnGraph:          info.TypedInput,
			ScorePlane:                denseScorePlaneForResponse(search.Stats.ColumnGraphWork.ScorePlane, includeDiagnostics && info.TypedInput && req.QueryMode == collections.VectorIndexQueryModeQuantizedRerank),
			Results:                   dst,
			Route:                     RouteAnn,
			Candidates:                len(search.Results),
			FullDocumentScanFallbacks: 0,
			info:                      info,
			searchStats:               search.Stats,
			fetchedStats:              fetched.Stats,
		}
		if info.TypedInput && info.VectorRepresentation == collections.VectorIndexRepresentationCosineNormalizedF32V1 {
			identity := denseSearchRouteIdentity(req, info, search.Stats, fetched.Stats)
			if err := validateDenseSearchRouteIdentity(identity, req, info, len(search.Results)); err != nil {
				return RawDenseVectorSearchResponse{}, err
			}
			out.RouteIdentity = &identity
		}
		for i, result := range search.Results {
			m := fetched.Results[i]
			if !m.Found || len(m.Document) == 0 || !bytes.Equal(m.ID, result.ID) {
				return RawDenseVectorSearchResponse{}, serviceErrorf(CodeIndexUnavailable, "native ann vector result %q was not materialized from the matching read view", string(result.ID))
			}
			out.Results[i] = RawDenseVectorResult{ID: m.ID, Score: result.Score, Document: m.Document}
		}
		return out, nil
	}
}

func validateDenseTypedVectorSearchRoute(response collections.VectorIndexSearchResponse) error {
	if response.Strategy != collections.VectorIndexStrategyColumnGraph || response.Path != collections.VectorIndexSearchPathColumnGraphNativeReader || response.Stats.SearchRouteColumnGraphPrepared != 1 || response.Stats.DocumentsFetched != 0 {
		return serviceError(CodeIndexUnavailable, "typed graph search left the admitted no-document route")
	}
	return nil
}

func validateDenseTypedNormalizedVectorSearchRoute(response collections.VectorIndexSearchResponse, req DenseVectorSearchRequest, info IndexInfo) error {
	receipt := response.Stats.ColumnGraphReceipt
	if !receipt.Available || receipt.Representation != collections.VectorIndexRepresentationCosineNormalizedF32V1 || receipt.Representation != req.VectorRepresentation || receipt.Representation != info.VectorRepresentation {
		return serviceError(CodeIndexUnavailable, "typed dense search did not bind the admitted canonical representation")
	}
	if receipt.QueryMode != req.QueryMode || receipt.ResultCount != uint64(len(response.Results)) {
		return serviceError(CodeIndexUnavailable, "typed dense search route receipt does not match the request and results")
	}
	if receipt.Route != "typed_empty" && receipt.Route != "typed_exact" && receipt.Route != "typed_hnsw" {
		return serviceErrorf(CodeIndexUnavailable, "typed dense search used unsupported route %q", receipt.Route)
	}
	if receipt.SchemaHash == 0 || receipt.SchemaGeneration == 0 || info.Generation == 0 || receipt.SchemaGeneration > info.Generation ||
		receipt.BaseManifestGeneration == 0 || receipt.BaseManifestChecksum == 0 ||
		receipt.CurrentManifestGeneration == 0 || receipt.CurrentManifestChecksum == 0 ||
		receipt.CurrentManifestGeneration < receipt.BaseManifestGeneration || receipt.CurrentCoverageLSN == 0 ||
		(receipt.CurrentManifestGeneration == receipt.BaseManifestGeneration && receipt.CurrentManifestChecksum != receipt.BaseManifestChecksum) {
		return serviceError(CodeIndexUnavailable, "typed dense search route receipt has an invalid owner identity")
	}
	stats := response.Stats
	if stats.DocumentsFetched != 0 || stats.DocumentBytes != 0 || stats.DocumentOutputBytes != 0 || stats.NormBytesRead != 0 {
		return serviceError(CodeIndexUnavailable, "typed dense search left the canonical no-document score plane")
	}
	fp32ScoreCalls := stats.FP32ScoreCalls
	if req.QueryMode == collections.VectorIndexQueryModeExact {
		if stats.PreparedScoreCalls > ^uint64(0)-stats.ColumnGraphDeltaScored {
			return serviceError(CodeIndexUnavailable, "typed dense search FP32 work accounting overflowed")
		}
		fp32ScoreCalls = stats.PreparedScoreCalls + stats.ColumnGraphDeltaScored
	}
	wantVectorBytes, ok := denseUint64Product(fp32ScoreCalls, uint64(info.Dimension), 4)
	if !ok || stats.VectorBytesRead != wantVectorBytes {
		return serviceError(CodeIndexUnavailable, "typed dense search FP32 work accounting does not match the canonical vector plane")
	}
	if req.QueryMode != collections.VectorIndexQueryModeQuantizedRerank {
		if receipt.QuantizedIndexName != "" || receipt.QuantizedCodec != "" || receipt.QuantizedVersion != 0 || stats.QuantizedScoreCalls != 0 || stats.QuantizedCodeBytesRead != 0 || stats.QuantizedRerankCandidates != 0 || stats.QuantizedRerankExactScoreCalls != 0 {
			return serviceError(CodeIndexUnavailable, "typed exact dense search carried quantized route state")
		}
		return nil
	}
	return validateDenseTypedQuantizedVectorSearchRouteLean(response, req, info)
}

func validateDenseTypedQuantizedVectorSearchRouteLean(response collections.VectorIndexSearchResponse, req DenseVectorSearchRequest, info IndexInfo) error {
	stats := response.Stats
	receipt := stats.ColumnGraphReceipt
	if receipt.QuantizedIndexName != req.QuantizedIndexName || receipt.QuantizedCodec != collections.QuantizedVectorCodecScalarU8 || receipt.QuantizedVersion != 1 || stats.SearchRouteQuantizedRerank != 1 || stats.SearchRouteQuantizedOnly != 0 {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search route identity does not match the request")
	}
	wantCodeBytes, ok := denseUint64Product(stats.QuantizedScoreCalls, uint64(info.Dimension))
	if !ok || stats.QuantizedCodeBytesRead != wantCodeBytes {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search code-byte accounting is invalid")
	}
	wantPackedBytes, ok := denseUint64Product(stats.PackedExactScoreCandidates, uint64(info.Dimension), 4)
	if !ok || stats.PackedExactVectorBytesRead != wantPackedBytes {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search packed rerank byte accounting is invalid")
	}
	if stats.PackedExactScoreCandidates == 0 {
		if stats.PackedExactScoreCalls != 0 {
			return serviceError(CodeIndexUnavailable, "typed quantized dense search reported an empty packed rerank call")
		}
	} else if stats.PackedExactScoreCalls != 1 {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search did not use one packed rerank call")
	}
	// Packed counters describe the base batch only. Shadow suppression can
	// leave no live base candidates while the exact mutable suffix supplies results.
	if stats.PackedExactScoreCandidates > ^uint64(0)-stats.ColumnGraphDeltaScored ||
		stats.FP32ScoreCalls != stats.PackedExactScoreCandidates+stats.ColumnGraphDeltaScored ||
		(receipt.CurrentManifestGeneration == receipt.BaseManifestGeneration && stats.ColumnGraphDeltaScored != 0) {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search base/suffix score accounting is invalid")
	}
	if receipt.Route != "typed_hnsw" {
		if stats.QuantizedScoreCalls != 0 || stats.QuantizedCodeBytesRead != 0 || stats.QuantizedRerankCandidates != 0 || stats.QuantizedRerankExactScoreCalls != 0 {
			return serviceError(CodeIndexUnavailable, "typed quantized exact shortcut carried traversal or rerank work")
		}
		if receipt.Route == "typed_empty" && (stats.PackedExactScoreCalls != 0 || stats.PackedExactScoreCandidates != 0 || stats.PackedExactVectorBytesRead != 0) {
			return serviceError(CodeIndexUnavailable, "typed quantized empty shortcut carried packed score work")
		}
		return nil
	}
	if stats.QuantizedRerankCandidates != stats.QuantizedRerankExactScoreCalls || stats.PackedExactScoreCandidates != stats.QuantizedRerankExactScoreCalls {
		return serviceErrorf(CodeIndexUnavailable, "typed quantized dense search exact rerank accounting is invalid: candidates=%d exact_calls=%d packed_candidates=%d", stats.QuantizedRerankCandidates, stats.QuantizedRerankExactScoreCalls, stats.PackedExactScoreCandidates)
	}
	_, effectiveR := effectiveDenseSearchWidths(req, info)
	if effectiveR <= 0 || stats.QuantizedRerankCandidates > uint64(effectiveR) {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search exceeded the admitted rerank width")
	}
	if stats.QuantizedScorerActive != 1 || stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead == 0 {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search did not execute the admitted traversal and rerank planes")
	}
	return nil
}

func effectiveDenseSearchWidths(req DenseVectorSearchRequest, info IndexInfo) (int, int) {
	efSearch := req.EfSearch
	if efSearch == 0 {
		efSearch = info.VectorEfSearch
	}
	rerank := req.QuantizedRerankCandidates
	if req.QueryMode == collections.VectorIndexQueryModeQuantizedRerank && rerank == 0 {
		rerank = efSearch
	}
	return efSearch, rerank
}

func denseUint64Product(values ...uint64) (uint64, bool) {
	product := uint64(1)
	for _, value := range values {
		if value != 0 && product > ^uint64(0)/value {
			return 0, false
		}
		product *= value
	}
	return product, true
}

func denseSearchRouteIdentity(req DenseVectorSearchRequest, info IndexInfo, stats collections.VectorIndexSearchStats, fetched collections.DocumentMaterializationStats) DenseSearchRouteIdentity {
	receipt := stats.ColumnGraphReceipt
	efSearch, rerank := effectiveDenseSearchWidths(req, info)
	fp32ScoreCalls := stats.FP32ScoreCalls
	if req.QueryMode == collections.VectorIndexQueryModeExact {
		fp32ScoreCalls = stats.PreparedScoreCalls + stats.ColumnGraphDeltaScored
	}
	return DenseSearchRouteIdentity{
		Version:                   1,
		Representation:            receipt.Representation,
		QueryMode:                 receipt.QueryMode,
		ExecutionRoute:            receipt.Route,
		QuantizedCodec:            receipt.QuantizedCodec,
		QuantizedIndexName:        receipt.QuantizedIndexName,
		QuantizedVersion:          receipt.QuantizedVersion,
		ReturnEmbedding:           req.ReturnEmbedding,
		Diagnostics:               req.Diagnostics,
		Filter:                    req.Filter != nil,
		SchemaHash:                receipt.SchemaHash,
		SchemaGeneration:          receipt.SchemaGeneration,
		BaseManifestGeneration:    receipt.BaseManifestGeneration,
		BaseManifestChecksum:      receipt.BaseManifestChecksum,
		CurrentManifestGeneration: receipt.CurrentManifestGeneration,
		CurrentManifestChecksum:   receipt.CurrentManifestChecksum,
		CurrentCoverageLSN:        receipt.CurrentCoverageLSN,
		TopK:                      uint64(req.TopK),
		EfSearch:                  uint64(efSearch),
		RerankCandidates:          uint64(rerank),
		ResultCount:               receipt.ResultCount,
		QuantizedScoreCalls:       stats.QuantizedScoreCalls,
		QuantizedCodeBytesRead:    stats.QuantizedCodeBytesRead,
		FP32ScoreCalls:            fp32ScoreCalls,
		FP32VectorBytesRead:       stats.VectorBytesRead,
		PackedScoreCalls:          stats.PackedExactScoreCalls,
		PackedScoreCandidates:     stats.PackedExactScoreCandidates,
		PackedVectorBytesRead:     stats.PackedExactVectorBytesRead,
		EmbeddingVectorReads:      fetched.EmbeddingVectorReads,
		EmbeddingVectorBytes:      fetched.EmbeddingVectorBytes,
		EmbeddingOutputBytes:      fetched.EmbeddingOutputBytes,
	}
}

func validateDenseSearchRouteIdentity(identity DenseSearchRouteIdentity, req DenseVectorSearchRequest, info IndexInfo, resultCount int) error {
	efSearch, rerank := effectiveDenseSearchWidths(req, info)
	if resultCount < 0 || identity.Version != 1 ||
		identity.Representation != collections.VectorIndexRepresentationCosineNormalizedF32V1 || identity.Representation != req.VectorRepresentation || identity.Representation != info.VectorRepresentation ||
		identity.QueryMode != req.QueryMode || identity.ReturnEmbedding != req.ReturnEmbedding || identity.Diagnostics != req.Diagnostics || identity.Filter != (req.Filter != nil) ||
		identity.TopK != uint64(req.TopK) || identity.EfSearch != uint64(efSearch) || identity.RerankCandidates != uint64(rerank) || identity.ResultCount != uint64(resultCount) ||
		identity.CurrentCoverageLSN == 0 ||
		(identity.CurrentManifestGeneration == identity.BaseManifestGeneration && identity.CurrentManifestChecksum != identity.BaseManifestChecksum) {
		return serviceError(CodeIndexUnavailable, "typed dense search route identity does not match the completed request")
	}
	wantFP32Bytes, ok := denseUint64Product(identity.FP32ScoreCalls, uint64(info.Dimension), 4)
	if !ok || identity.FP32VectorBytesRead != wantFP32Bytes ||
		(resultCount != 0 && identity.FP32ScoreCalls == 0) || identity.PackedScoreCandidates > identity.FP32ScoreCalls {
		return serviceError(CodeIndexUnavailable, "typed dense search route identity has invalid FP32 byte accounting")
	}
	wantCodeBytes, ok := denseUint64Product(identity.QuantizedScoreCalls, uint64(info.Dimension))
	if !ok || identity.QuantizedCodeBytesRead != wantCodeBytes {
		return serviceError(CodeIndexUnavailable, "typed dense search route identity has invalid quantized byte accounting")
	}
	wantPackedBytes, ok := denseUint64Product(identity.PackedScoreCandidates, uint64(info.Dimension), 4)
	if !ok || identity.PackedVectorBytesRead != wantPackedBytes ||
		(identity.PackedScoreCandidates == 0 && identity.PackedScoreCalls != 0) ||
		(identity.PackedScoreCandidates != 0 && (identity.PackedScoreCalls == 0 || identity.PackedScoreCalls > identity.PackedScoreCandidates)) {
		return serviceError(CodeIndexUnavailable, "typed dense search route identity has invalid packed score accounting")
	}
	if !req.ReturnEmbedding {
		if identity.EmbeddingVectorReads != 0 || identity.EmbeddingVectorBytes != 0 || identity.EmbeddingOutputBytes != 0 {
			return serviceError(CodeIndexUnavailable, "typed dense search materialized an excluded embedding")
		}
		return nil
	}
	wantEmbeddingBytes, ok := denseUint64Product(uint64(resultCount), uint64(info.Dimension), 4)
	if !ok || identity.EmbeddingVectorReads != uint64(resultCount) || identity.EmbeddingVectorBytes != wantEmbeddingBytes || (resultCount != 0 && identity.EmbeddingOutputBytes == 0) {
		return serviceError(CodeIndexUnavailable, "typed dense search embedding materialization accounting is invalid")
	}
	return nil
}

func validateDenseTypedQuantizedVectorSearchRoute(response collections.VectorIndexSearchResponse, req DenseVectorSearchRequest, serviceGeneration uint64, indexEFSearch int) error {
	proof := response.Stats.ColumnGraphWork.ScorePlane
	if !proof.Available || !proof.Completed || !proof.Snapshot.Available || proof.RequestedMode != collections.VectorIndexQueryModeQuantizedRerank || proof.EffectiveMode != collections.VectorIndexQueryModeQuantizedRerank {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search did not produce a completed quantized score-plane proof")
	}
	if serviceGeneration == 0 || proof.Snapshot.SchemaGeneration > serviceGeneration {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search snapshot exceeds the admitted service generation")
	}
	if proof.QuantizedIndexName != req.QuantizedIndexName || proof.QuantizedCodec != collections.QuantizedVectorCodecScalarU8 || proof.QuantizedVersion != 1 || proof.RequestedTopK != uint64(req.TopK) || proof.RequestedEFSearch != uint64(req.EfSearch) || proof.RequestedRerankCandidates != uint64(req.QuantizedRerankCandidates) {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search score-plane proof does not match the request")
	}
	resolvedEFSearch := req.EfSearch
	if resolvedEFSearch == 0 {
		resolvedEFSearch = indexEFSearch
	}
	if resolvedEFSearch <= 0 || proof.NormalizedCandidateWidth > uint64(max(req.TopK, resolvedEFSearch)) {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search candidate width exceeds the resolved ef_search")
	}
	if proof.Route != "typed_empty" && proof.Route != "typed_exact" && proof.Route != "quantized_rerank" {
		return serviceErrorf(CodeIndexUnavailable, "typed quantized dense search used unsupported route %q", proof.Route)
	}
	if proof.Route == "quantized_rerank" && proof.QuantizedScoreCalls == 0 {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search did not record quantized score calls")
	}
	if response.Stats.DocumentsFetched != 0 || response.Stats.DocumentBytes != 0 || response.Stats.DocumentOutputBytes != 0 {
		return serviceError(CodeIndexUnavailable, "typed quantized dense search left the no-document scoring route")
	}
	return nil
}

// mappedHybridSearchError layers the executor's typed fail-closed reasons onto
// the service error vocabulary. Truncated scalar allow-sets surface the
// inherited scalar_filter_unbounded reason explicitly instead of shrinking to
// partial results.
func mappedHybridSearchError(operation string, err error, stats collections.HybridSearchStats) error {
	if err == nil {
		return nil
	}
	if stats.Truncated > 0 && (stats.FailClosedReason == collections.HybridFailClosedReasonScalarFilterUnbounded || containsScalarFilterUnbounded(err)) {
		return wrapServiceError(CodeIndexUnavailable, operation+" failed closed: "+string(collections.HybridFailClosedReasonScalarFilterUnbounded)+" (a bounded scalar allow-set lookup exceeded its limit; no partial results are returned)", err)
	}
	return mapHybridSearchError(err)
}

func containsScalarFilterUnbounded(err error) bool {
	return err != nil && strings.Contains(err.Error(), string(collections.HybridFailClosedReasonScalarFilterUnbounded))
}
