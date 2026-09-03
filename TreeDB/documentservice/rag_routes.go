package documentservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
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
	if operator == collections.TextSearchOperatorAND {
		return KeywordSearchResponse{}, serviceError(CodeUnsupported, "operator \"and\" with a metadata filter is unsupported for keyword search; only the default \"or\" operator composes with bounded scalar allow-sets")
	}
	opts := collections.HybridSearchOptions{
		TopK: req.TopK,
		Text: &collections.HybridTextQuery{
			IndexName:          defaultTextIndexName,
			Query:              req.Query,
			CandidateLimit:     req.CandidateLimit,
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

const denseVectorNativeSnapshotAttempts = 3

// searchDenseVectorAnn serves route=ann without silent fallback. The existing
// column_graph branch keeps one-shot snapshot materialization; native_runtime
// uses the buffered no-document route plus a generation-validated read view.
func (s *Service) searchDenseVectorAnn(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest) (DenseVectorSearchResponse, error) {
	if info.VectorStrategy == collections.VectorIndexStrategyNativeRuntime {
		if !info.Capabilities.NoDocumentVectorSearch {
			return DenseVectorSearchResponse{}, serviceError(CodeUnsupported, "dense route \"ann\" requires a cosine float32 native_runtime vector index; use route \"exact\" for this index")
		}
		return s.searchDenseVectorNative(ctx, col, info, req)
	}
	if !info.Capabilities.ColumnGraphVectorSearch {
		return DenseVectorSearchResponse{}, serviceError(CodeUnsupported, "dense route \"ann\" requires a cosine column_graph vector index; use route \"exact\" for this index")
	}
	if err := ctxErr(ctx); err != nil {
		return DenseVectorSearchResponse{}, err
	}
	search, err := col.SearchVectorIndex(collections.VectorIndexSearchOptions{
		IndexName:            defaultVectorIndexName,
		Query:                append([]float32(nil), req.QueryEmbedding...),
		QueryMode:            collections.VectorIndexQueryModeExact,
		TopK:                 req.TopK,
		EfSearch:             req.EfSearch,
		IncludeDocuments:     true,
		DocumentFetchOptions: serviceDocumentFetchOptions(req.ReturnEmbedding),
	})
	if err != nil {
		return DenseVectorSearchResponse{}, mapVectorIndexSearchError("ann vector search", err)
	}
	docs := make([]Document, 0, len(search.Results))
	for _, result := range search.Results {
		if len(result.Document) == 0 {
			return DenseVectorSearchResponse{}, serviceErrorf(CodeIndexUnavailable, "ann vector result %q was not materialized in the search snapshot", string(result.ID))
		}
		doc, err := decodeStoredDocument(result.ID, result.Document)
		if err != nil {
			return DenseVectorSearchResponse{}, err
		}
		if !req.ReturnEmbedding {
			doc.Embedding = nil
		}
		doc.Score = scorePtr(result.Score)
		docs = append(docs, doc)
	}
	return DenseVectorSearchResponse{
		Index:      info,
		Documents:  docs,
		Metric:     info.Metric,
		Route:      RouteAnn,
		Exact:      false,
		Candidates: len(search.Results),
	}, nil
}

func (s *Service) searchDenseVectorNative(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest) (DenseVectorSearchResponse, error) {
	raw, err := s.searchDenseVectorNativeRaw(ctx, col, info, req, nil)
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	docs := make([]Document, 0, len(raw.Results))
	for _, result := range raw.Results {
		doc, err := decodeStoredDocument(result.ID, result.Document)
		if err != nil {
			return DenseVectorSearchResponse{}, err
		}
		if !req.ReturnEmbedding {
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
	Results                   []RawDenseVectorResult
	Route                     Route
	Candidates                int
	NativeBasePlusLiveDelta   bool
	ExactFallbacks            uint64
	FullDocumentScanFallbacks uint64
	info                      IndexInfo
	searchStats               collections.VectorIndexSearchStats
	fetchedStats              collections.DocumentMaterializationStats
	attempts                  int
}

func (r RawDenseVectorSearchResponse) response(docs []Document) DenseVectorSearchResponse {
	stats := r.searchStats
	return DenseVectorSearchResponse{
		Index:                                   r.info,
		Documents:                               docs,
		Metric:                                  r.info.Metric,
		Route:                                   r.Route,
		Candidates:                              r.Candidates,
		NativeBasePlusLiveDelta:                 r.NativeBasePlusLiveDelta,
		ScalarFilterMembershipSource:            denseNativeScalarMembershipSource(stats.ScalarFilterPlan),
		ScalarFilterPlan:                        stats.ScalarFilterPlan,
		ScalarFilterProbeIDs:                    stats.ScalarFilterProbeIDs,
		ScalarFilterProbeTruncated:              stats.ScalarFilterProbeTruncated,
		ScalarFilterCandidates:                  stats.ScalarFilterCandidates,
		ScalarFilterCandidateIDs:                stats.ScalarFilterCandidateIDs,
		ScalarFilterRetainedCandidateIDs:        stats.ScalarFilterRetainedCandidateIDs,
		ScalarFilterRefinedCandidateIDs:         stats.ScalarFilterRefinedCandidateIDs,
		ScalarFilterVisited:                     stats.ScalarFilterVisited,
		ScalarFilterScored:                      stats.ScalarFilterScored,
		ScalarFilterAdmitted:                    stats.ScalarFilterAdmitted,
		ScalarFilterExactScoring:                stats.ScalarFilterExactScoring > 0,
		ScalarFilterUnderfill:                   stats.ScalarFilterUnderfill > 0,
		ScalarFilterPlanCacheHits:               stats.ScalarFilterPlanCacheHits,
		ScalarFilterPlanCacheMisses:             stats.ScalarFilterPlanCacheMisses,
		ScalarFilterPlanCacheInvalidations:      stats.ScalarFilterPlanCacheInvalidations,
		ScalarFilterPlanCacheGenerationBypasses: stats.ScalarFilterPlanCacheGenerationBypasses,
		ScalarFilterPlanCacheEvictions:          stats.ScalarFilterPlanCacheEvictions,
		ScalarFilterPlanCacheEntries:            stats.ScalarFilterPlanCacheEntries,
		ScalarFilterPlanCacheRetainedBytes:      stats.ScalarFilterPlanCacheRetainedBytes,
		ExactFallbacks:                          r.ExactFallbacks,
		FullDocumentScanFallbacks:               r.FullDocumentScanFallbacks,
		AllowedIDMaterializationRows:            stats.ScalarFilterRetainedCandidateIDs,
		DocumentMaterializationRows:             r.fetchedStats.DocumentsFetched,
		VisibilityMismatchCount:                 uint64(r.attempts),
		VisibilityRetryCount:                    uint64(r.attempts),
	}
}

// SearchDenseVectorNativeRaw exposes the existing native_runtime snapshot/search/fetch path without decoding stored documents.
func (s *Service) SearchDenseVectorNativeRaw(ctx context.Context, index string, req DenseVectorSearchRequest) (RawDenseVectorSearchResponse, error) {
	return s.SearchDenseVectorNativeRawInto(ctx, index, req, nil)
}

// SearchDenseVectorNativeRawInto reuses dst for the returned result envelope.
func (s *Service) SearchDenseVectorNativeRawInto(ctx context.Context, index string, req DenseVectorSearchRequest, dst []RawDenseVectorResult) (RawDenseVectorSearchResponse, error) {
	col, info, release, err := s.acquireBenchmarkSearchIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	defer release()
	if req.TopK <= 0 {
		return RawDenseVectorSearchResponse{}, serviceError(CodeInvalidRequest, "top_k must be positive")
	}
	if err := validateEmbedding("query_embedding", req.QueryEmbedding, info.Dimension, info.Metric); err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	if req.EfSearch < 0 {
		return RawDenseVectorSearchResponse{}, serviceError(CodeInvalidRequest, "ef_search must be non-negative")
	}
	route, err := resolveDenseSearchRoute(req, info)
	if err != nil {
		return RawDenseVectorSearchResponse{}, err
	}
	if route != RouteAnn || info.VectorStrategy != collections.VectorIndexStrategyNativeRuntime || !info.Capabilities.NoDocumentVectorSearch {
		return RawDenseVectorSearchResponse{}, serviceError(CodeUnsupported, "dense nativewire search requires a cosine float32 native_runtime vector index")
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

func (s *Service) searchDenseVectorNativeRawLocked(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest, dst []RawDenseVectorResult) (RawDenseVectorSearchResponse, error) {
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
	for attempt := range denseVectorNativeSnapshotAttempts {
		if err := ctxErr(ctx); err != nil {
			return RawDenseVectorSearchResponse{}, err
		}
		search, view, err := col.SearchVectorIndexWithBufferReadView(collections.VectorIndexSearchOptions{Context: ctx, IndexName: defaultVectorIndexName, Query: req.QueryEmbedding, QueryMode: collections.VectorIndexQueryModeExact, TopK: req.TopK, EfSearch: req.EfSearch, StatsMode: collections.VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: scalarFilter}, buffer)
		if err != nil {
			if errors.Is(err, collections.ErrVectorIndexSnapshotMismatch) {
				buffer.Reset()
				continue
			}
			return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann vector search", err)
		}
		if err := validateDenseNativeVectorSearchRoute(search); err != nil {
			_ = view.Close()
			return RawDenseVectorSearchResponse{}, err
		}
		if s.denseVectorNativeAfterSearch != nil {
			if err := s.denseVectorNativeAfterSearch(attempt, search); err != nil {
				_ = view.Close()
				return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann vector search hook", err)
			}
		}
		if err := ctxErr(ctx); err != nil {
			_ = view.Close()
			return RawDenseVectorSearchResponse{}, err
		}
		fetched, fetchErr := view.FetchDocumentsForVectorIndexSearchResults(search.Results, serviceDocumentFetchOptions(req.ReturnEmbedding))
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
		diagnostics := search.Diagnostics()
		if len(search.Results) <= cap(dst) {
			if len(search.Results) < cap(dst) {
				clear(dst[len(search.Results):cap(dst)])
			}
			dst = dst[:len(search.Results)]
		} else {
			dst = make([]RawDenseVectorResult, len(search.Results))
		}
		out := RawDenseVectorSearchResponse{
			Results:                   dst,
			Route:                     RouteAnn,
			Candidates:                len(search.Results),
			NativeBasePlusLiveDelta:   true,
			ExactFallbacks:            diagnostics.LiveANN.ExactFallbacks,
			FullDocumentScanFallbacks: 0,
			info:                      info,
			searchStats:               search.Stats,
			fetchedStats:              fetched.Stats,
			attempts:                  attempt,
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
	return RawDenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann vector search", fmt.Errorf("%w after %d attempts", collections.ErrVectorIndexSnapshotMismatch, denseVectorNativeSnapshotAttempts))
}

func denseNativeScalarMembershipSource(plan collections.NativeScalarFilterPlan) string {
	switch plan {
	case collections.NativeScalarFilterPlanCompleteExact, collections.NativeScalarFilterPlanCompleteFinite:
		return "bounded_complete_set"
	case collections.NativeScalarFilterPlanMixed:
		return "bounded_candidate_refinement"
	case collections.NativeScalarFilterPlanVectorAligned:
		return "vector_aligned_scalar"
	default:
		return ""
	}
}

func validateDenseNativeVectorSearchRoute(response collections.VectorIndexSearchResponse) error {
	diagnostics := response.Diagnostics()
	stats := response.Stats
	if response.Strategy != collections.VectorIndexStrategyNativeRuntime ||
		response.Path != collections.VectorIndexSearchPathNativeRuntime ||
		diagnostics.Route != collections.VectorIndexSearchRouteNativeRuntime ||
		!diagnostics.LiveANN.Enabled ||
		diagnostics.LiveANN.ExactFallbacks != 0 ||
		diagnostics.FallbackReason != collections.VectorIndexSearchFallbackReasonNone ||
		!diagnostics.NoDocumentGuardrailsOK ||
		stats.SearchRouteNativeRuntime != 1 ||
		stats.DocumentsFetched != 0 ||
		stats.DocumentBytes != 0 ||
		stats.DocumentOutputBytes != 0 {
		return serviceErrorf(CodeIndexUnavailable, "native ann vector search left the buffered no-document native_runtime route: diagnostics=%+v", diagnostics)
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
