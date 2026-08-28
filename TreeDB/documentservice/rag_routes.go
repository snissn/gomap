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
	if req.Filter != nil {
		if err := req.Filter.Validate(); err != nil {
			return DenseVectorSearchResponse{}, err
		}
	}
	scalarFilter, err := translateScalarFilter(req.Filter, newScalarSchema(info.ScalarFields))
	if err != nil {
		return DenseVectorSearchResponse{}, err
	}
	s.benchmarkSearchCacheMu.RLock()
	if s.closed {
		s.benchmarkSearchCacheMu.RUnlock()
		return DenseVectorSearchResponse{}, serviceClosedError()
	}
	buffer := s.benchmarkSearchBufferPool.Get().(*collections.VectorIndexSearchBuffer)
	defer func() {
		buffer.Reset()
		s.benchmarkSearchBufferPool.Put(buffer)
		s.benchmarkSearchCacheMu.RUnlock()
	}()
	for attempt := range denseVectorNativeSnapshotAttempts {
		if err := ctxErr(ctx); err != nil {
			return DenseVectorSearchResponse{}, err
		}
		search, view, err := col.SearchVectorIndexWithBufferReadView(collections.VectorIndexSearchOptions{
			IndexName:            defaultVectorIndexName,
			Query:                req.QueryEmbedding,
			QueryMode:            collections.VectorIndexQueryModeExact,
			TopK:                 req.TopK,
			EfSearch:             req.EfSearch,
			StatsMode:            collections.VectorIndexSearchStatsModeProduction,
			DeclaredScalarFilter: scalarFilter,
		}, buffer)
		if err != nil {
			if errors.Is(err, collections.ErrVectorIndexSnapshotMismatch) {
				buffer.Reset()
				continue
			}
			return DenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann vector search", err)
		}
		if err := validateDenseNativeVectorSearchRoute(search); err != nil {
			_ = view.Close()
			return DenseVectorSearchResponse{}, err
		}
		if s.denseVectorNativeAfterSearch != nil {
			if err := s.denseVectorNativeAfterSearch(attempt, search); err != nil {
				_ = view.Close()
				return DenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann vector search hook", err)
			}
		}
		fetched, fetchErr := view.FetchDocumentsForVectorIndexSearchResults(search.Results, serviceDocumentFetchOptions(req.ReturnEmbedding))
		closeErr := view.Close()
		if fetchErr != nil {
			return DenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann document fetch", errors.Join(fetchErr, closeErr))
		}
		if closeErr != nil {
			return DenseVectorSearchResponse{}, mapVectorIndexSearchError("native ann document read view close", closeErr)
		}
		if fetched.Stats.DocumentsRequested != uint64(len(search.Results)) ||
			fetched.Stats.DocumentsFetched != uint64(len(search.Results)) ||
			fetched.Stats.DocumentsMissing != 0 ||
			len(fetched.Results) != len(search.Results) {
			return DenseVectorSearchResponse{}, serviceErrorf(
				CodeIndexUnavailable,
				"native ann document fetch did not materialize exactly the returned candidates: candidates=%d requested=%d fetched=%d missing=%d results=%d",
				len(search.Results),
				fetched.Stats.DocumentsRequested,
				fetched.Stats.DocumentsFetched,
				fetched.Stats.DocumentsMissing,
				len(fetched.Results),
			)
		}
		docs := make([]Document, 0, len(search.Results))
		for i, result := range search.Results {
			materialized := fetched.Results[i]
			if !materialized.Found || len(materialized.Document) == 0 || !bytes.Equal(materialized.ID, result.ID) {
				return DenseVectorSearchResponse{}, serviceErrorf(CodeIndexUnavailable, "native ann vector result %q was not materialized from the matching read view", string(result.ID))
			}
			doc, err := decodeStoredDocument(result.ID, materialized.Document)
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
			Index:                            info,
			Documents:                        docs,
			Metric:                           info.Metric,
			Route:                            RouteAnn,
			Exact:                            false,
			Candidates:                       len(search.Results),
			NativeBasePlusLiveDelta:          true,
			ScalarFilterMembershipSource:     denseNativeScalarMembershipSource(search.Stats.ScalarFilterPlan),
			ScalarFilterPlan:                 search.Stats.ScalarFilterPlan,
			ScalarFilterProbeIDs:             search.Stats.ScalarFilterProbeIDs,
			ScalarFilterProbeTruncated:       search.Stats.ScalarFilterProbeTruncated,
			ScalarFilterCandidates:           search.Stats.ScalarFilterCandidates,
			ScalarFilterCandidateIDs:         search.Stats.ScalarFilterCandidateIDs,
			ScalarFilterRetainedCandidateIDs: search.Stats.ScalarFilterRetainedCandidateIDs,
			ScalarFilterRefinedCandidateIDs:  search.Stats.ScalarFilterRefinedCandidateIDs,
			ScalarFilterVisited:              search.Stats.ScalarFilterVisited,
			ScalarFilterScored:               search.Stats.ScalarFilterScored,
			ScalarFilterAdmitted:             search.Stats.ScalarFilterAdmitted,
			ScalarFilterExactScoring:         search.Stats.ScalarFilterExactScoring > 0,
			ScalarFilterUnderfill:            search.Stats.ScalarFilterUnderfill > 0,
			AllowedIDMaterializationRows:     search.Stats.ScalarFilterRetainedCandidateIDs,
			DocumentMaterializationRows:      fetched.Stats.DocumentsFetched,
			VisibilityMismatchCount:          uint64(attempt),
			VisibilityRetryCount:             uint64(attempt),
		}, nil
	}
	return DenseVectorSearchResponse{}, mapVectorIndexSearchError(
		"native ann vector search",
		fmt.Errorf("%w after %d attempts", collections.ErrVectorIndexSnapshotMismatch, denseVectorNativeSnapshotAttempts),
	)
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
