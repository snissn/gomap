package documentservice

import (
	"context"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

// searchKeywordWithScalarFilter serves filtered keyword search through the
// collection hybrid executor's bounded scalar prefilter vocabulary. The filter
// is compiled into a single indexed allow-set before candidate generation; the
// executor consumes it inside posting-block scans and fails closed with
// scalar_filter_unbounded on truncation instead of returning partial rankings.
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
			CandidatesRequested:       stats.TextCandidatesRequested,
			CandidatesReturned:        stats.TextCandidatesReturned,
			PostingsScanned:           stats.TextPostingsScanned,
			CandidatesScored:          stats.TextCandidatesScored,
			DocumentsFetched:          stats.DocumentsFetched,
			DocumentsMissing:          stats.DocumentsMissing,
			FullDocumentScanFallbacks: stats.FullDocumentScanFallbacks,
			Truncated:                 stats.Truncated > 0,
			FailClosed:                stats.FailClosed,
			FailClosedReason:          string(stats.FailClosedReason),
			ScalarPrefilterIDs:        stats.ScalarPrefilterIDs,
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
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// searchDenseVectorAnn serves the dense route=ann path through the
// column_graph vector index (lossless exact scoring over the graph's
// approximate candidate set). It never scans documents and rejects filters:
// filtered dense retrieval stays on the exact route.
func (s *Service) searchDenseVectorAnn(ctx context.Context, col *collections.Collection, info IndexInfo, req DenseVectorSearchRequest) (DenseVectorSearchResponse, error) {
	if !info.Capabilities.ColumnGraphVectorSearch {
		return DenseVectorSearchResponse{}, serviceError(CodeUnsupported, "dense route \"ann\" requires a cosine column_graph vector index; use route \"exact\" for this index")
	}
	if err := ctxErr(ctx); err != nil {
		return DenseVectorSearchResponse{}, err
	}
	buffer := &collections.VectorIndexSearchBuffer{}
	defer buffer.Reset()
	search, err := col.SearchVectorIndexWithBuffer(collections.VectorIndexSearchOptions{
		IndexName: defaultVectorIndexName,
		Query:     append([]float32(nil), req.QueryEmbedding...),
		QueryMode: collections.VectorIndexQueryModeExact,
		TopK:      req.TopK,
		EfSearch:  req.EfSearch,
	}, buffer)
	if err != nil {
		return DenseVectorSearchResponse{}, mapVectorIndexSearchError("ann vector search", err)
	}
	docs := make([]Document, 0, len(search.Results))
	for _, result := range search.Results {
		id := []byte(string(result.ID))
		raw, err := col.Get(id)
		if err != nil {
			return DenseVectorSearchResponse{}, wrapServiceError(CodeInternal, "fetch ann result document failed", err)
		}
		if raw == nil {
			return DenseVectorSearchResponse{}, serviceErrorf(CodeIndexUnavailable, "ann vector result %q was not found in primary storage", string(id))
		}
		doc, err := decodeStoredDocument(id, raw)
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

// mappedHybridSearchError layers the executor's typed fail-closed reasons onto
// the service error vocabulary. Truncated scalar allow-sets surface the
// inherited scalar_filter_unbounded reason explicitly instead of shrinking to
// partial results.
func mappedHybridSearchError(operation string, err error, stats collections.HybridSearchStats) error {
	if err == nil {
		return nil
	}
	if stats.FailClosedReason == collections.HybridFailClosedReasonScalarFilterUnbounded || containsScalarFilterUnbounded(err) {
		return wrapServiceError(CodeIndexUnavailable, operation+" failed closed: "+string(collections.HybridFailClosedReasonScalarFilterUnbounded)+" (bounded scalar allow-set lookup exceeded its limit; no partial results are returned)", err)
	}
	return mapHybridSearchError(err)
}

func containsScalarFilterUnbounded(err error) bool {
	return err != nil && strings.Contains(err.Error(), string(collections.HybridFailClosedReasonScalarFilterUnbounded))
}
