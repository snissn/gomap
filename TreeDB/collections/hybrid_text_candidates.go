package collections

import (
	"errors"
	"fmt"
)

// SearchHybridTextCandidates adapts collection-native ranked text search into
// the shared hybrid candidate shape. It is candidate-only: it requests no
// documents from SearchText, reuses response-owned result IDs for response-owned
// HybridSearchCandidate values, assigns one-based source ranks, preserves text
// match attribution, and fails closed if the backing text path reports any full
// document materialization or scan fallback.
func (c *Collection) SearchHybridTextCandidates(query HybridTextQuery) (HybridCandidateResponse, error) {
	if c == nil {
		return HybridCandidateResponse{}, errCollectionNil
	}
	if c.db == nil {
		return HybridCandidateResponse{}, errCollectionDBNil
	}
	requested := query.CandidateLimit
	if err := validateHybridTextCandidateQuery(query); err != nil {
		response := HybridCandidateResponse{Stats: hybridTextCandidateStatsFromSearch(requested, TextSearchStats{}, 0)}
		response.Stats.FailClosed = 1
		response.Stats.FailClosedReason = HybridFailClosedReasonUnsupported
		return response, err
	}

	textResponse, err := c.searchText(TextSearchOptions{
		IndexName:        query.IndexName,
		Query:            query.Query,
		TopK:             requested,
		IncludeDocuments: false,
	}, textSearchResultTextMatchesOnly)
	if err != nil {
		response := HybridCandidateResponse{Stats: hybridTextCandidateStatsFromSearch(requested, textResponse.Stats, 0)}
		response.Stats.FailClosed = 1
		response.Stats.FailClosedReason = hybridTextCandidateFailClosedReason(err)
		return response, hybridTextCandidateError(err, query.IndexName)
	}

	return hybridTextCandidatesFromSearchResponse(requested, query.IndexName, textResponse)
}

func validateHybridTextCandidateQuery(query HybridTextQuery) error {
	if query.CandidateLimit <= 0 {
		return fmt.Errorf("%w: text candidate limit must be positive", ErrHybridSearchUnsupported)
	}
	return nil
}

func hybridTextCandidatesFromSearchResponse(requested int, textIndexName string, textResponse TextSearchResponse) (HybridCandidateResponse, error) {
	stats := hybridTextCandidateStatsFromSearch(requested, textResponse.Stats, 0)
	if !hybridTextCandidateNoDocumentGuardrailsOK(textResponse) {
		stats.FailClosed = 1
		stats.FailClosedReason = HybridFailClosedReasonFullDocumentScanForbidden
		return HybridCandidateResponse{Stats: stats}, fmt.Errorf("%w: text candidate generation fetched documents or used a full-document fallback", ErrHybridSearchUnsupported)
	}

	indexName := textResponse.IndexName
	if indexName == "" {
		indexName = textIndexName
	}
	candidates := make([]HybridSearchCandidate, len(textResponse.Results))
	for i, result := range textResponse.Results {
		id := result.DocumentID
		id = id[:len(id):len(id)]
		rank := result.Rank
		if rank <= 0 {
			rank = i + 1
		}
		scoreKind := result.ScoreKind
		if scoreKind == "" {
			scoreKind = HybridScoreKindBM25F
		}
		candidates[i] = HybridSearchCandidate{
			ID:          id,
			Source:      HybridCandidateSourceText,
			IndexName:   indexName,
			SourceRank:  rank,
			Score:       result.Score,
			ScoreKind:   scoreKind,
			TextMatches: hybridTextMatchesFromSearchResult(result),
		}
	}
	stats = hybridTextCandidateStatsFromSearch(requested, textResponse.Stats, len(candidates))
	return HybridCandidateResponse{Stats: stats, Candidates: candidates}, nil
}

func hybridTextCandidateStatsFromSearch(requested int, textStats TextSearchStats, returned int) HybridSearchStats {
	var requested64 uint64
	if requested > 0 {
		requested64 = uint64(requested)
	}
	returned64 := uint64(returned)
	textCandidatesScored := hybridMaxUint64(textStats.TextCandidatesScored, textStats.CandidatesScored)
	stats := HybridSearchStats{
		TextCandidatesRequested:   requested64,
		TextCandidatesReturned:    returned64,
		TextPostingsScanned:       hybridMaxUint64(textStats.TextPostingsScanned, textStats.PostingsScanned),
		TextCandidatesScored:      textCandidatesScored,
		DocumentsFetched:          textStats.DocumentsFetched,
		DocumentsMissing:          textStats.DocumentsMissing,
		FullDocumentScanFallbacks: textStats.FullDocumentScanFallbacks,
	}
	if textCandidatesScored > returned64 {
		stats.Truncated = textCandidatesScored - returned64
	}
	if textStats.Truncated && stats.Truncated == 0 {
		stats.Truncated = 1
	}
	if textStats.FailClosed != 0 {
		stats.FailClosed = textStats.FailClosed
		stats.FailClosedReason = HybridFailClosedReasonTextIndexUnavailable
	}
	return stats
}

func hybridTextCandidateNoDocumentGuardrailsOK(response TextSearchResponse) bool {
	stats := response.Stats
	if stats.DocumentsFetched != 0 || stats.DocumentsMissing != 0 || stats.DocumentFetchNanos != 0 || stats.FullDocumentScanFallbacks != 0 {
		return false
	}
	for _, result := range response.Results {
		if len(result.Document) != 0 {
			return false
		}
	}
	return true
}

func hybridTextMatchesFromSearchResult(result TextSearchResult) []HybridTextMatch {
	if len(result.TextMatches) == 0 {
		return nil
	}
	matches := make([]HybridTextMatch, len(result.TextMatches))
	for i, match := range result.TextMatches {
		terms := match.Terms
		if len(terms) > 0 {
			terms = terms[:len(terms):len(terms)]
		}
		matches[i] = HybridTextMatch{Field: match.Field, Terms: terms}
	}
	return matches
}

func hybridTextCandidateFailClosedReason(err error) HybridFailClosedReason {
	if errors.Is(err, ErrTextIndexUnavailable) || errors.Is(err, ErrIndexNotFound) {
		return HybridFailClosedReasonTextIndexUnavailable
	}
	return HybridFailClosedReasonUnsupported
}

func hybridTextCandidateError(err error, indexName string) error {
	if errors.Is(err, ErrTextIndexUnavailable) || errors.Is(err, ErrIndexNotFound) {
		return fmt.Errorf("%w: text index %q candidate generation unavailable: %w", ErrHybridSearchIndexUnavailable, indexName, err)
	}
	return fmt.Errorf("%w: text index %q candidate generation unsupported: %w", ErrHybridSearchUnsupported, indexName, err)
}
