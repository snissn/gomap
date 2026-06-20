package collections

import (
	"errors"
	"fmt"
)

// hybridTextCandidateDefaultScanCandidateLimit is the finite internal
// postings/scoring guardrail used to produce exact top-N hybrid text candidates
// for modest common-term corpora without changing the public returned-candidate
// budget.
const hybridTextCandidateDefaultScanCandidateLimit = 8 * 1024

// SearchHybridTextCandidates adapts collection-native ranked text search into
// the shared hybrid candidate shape. It is candidate-only: it requests no
// documents from SearchText, reuses response-owned result IDs for response-owned
// HybridSearchCandidate values, assigns one-based source ranks, and fails closed
// if the backing text path reports any full document materialization or scan
// fallback. Match attribution is opt-in through HybridTextQuery.IncludeTextMatches;
// the default candidate path is score-only.
func (c *Collection) SearchHybridTextCandidates(query HybridTextQuery) (HybridCandidateResponse, error) {
	return c.searchHybridTextCandidates(query, nil)
}

func (c *Collection) searchHybridTextCandidates(query HybridTextQuery, allowSet hybridScalarAllowSet) (HybridCandidateResponse, error) {
	return c.searchHybridTextCandidatesWithScanBudget(query, allowSet, query.CandidateLimit)
}

func (c *Collection) searchHybridTextCandidatesWithScanBudget(query HybridTextQuery, allowSet hybridScalarAllowSet, scanBudget int) (HybridCandidateResponse, error) {
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
	if scanBudget < requested {
		scanBudget = requested
	}

	resultMode := textSearchResultScoreOnly
	if query.IncludeTextMatches {
		resultMode = textSearchResultTextMatchesOnly
	}
	textResponse, err := c.searchText(TextSearchOptions{
		IndexName:                query.IndexName,
		Query:                    query.Query,
		TopK:                     requested,
		CandidateLimit:           hybridTextCandidateScanCandidateLimit(scanBudget),
		IncludeDocuments:         false,
		textV2AllowedDocumentIDs: allowSet,
	}, resultMode)
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

func hybridTextCandidateScanCandidateLimit(requested int) int {
	if requested <= 0 {
		return 0
	}
	limit := textSearchDefaultMinCandidateLimit
	if requested > maxCollectionInt/64 {
		limit = maxCollectionInt
	} else if scaled := requested * 64; scaled > limit {
		limit = scaled
	}
	if limit < hybridTextCandidateDefaultScanCandidateLimit {
		limit = hybridTextCandidateDefaultScanCandidateLimit
	}
	return limit
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
		TextCandidatesRequested:        requested64,
		TextCandidateBudgetEffective:   requested64,
		TextCandidatesReturned:         returned64,
		TextPostingsScanned:            hybridMaxUint64(textStats.TextPostingsScanned, textStats.PostingsScanned),
		TextPostingBlocksVisited:       textStats.TextPostingBlocksVisited,
		TextPostingBlocksSkipped:       textStats.TextPostingBlocksSkipped,
		TextBlockMaxFallbacks:          textStats.TextBlockMaxFallbacks,
		TextBlockMaxThresholds:         textStats.TextBlockMaxThresholds,
		TextWANDPivots:                 textStats.TextWANDPivots,
		TextScalarPrefilterIDs:         textStats.TextScalarPrefilterIDs,
		TextScalarPostingBlocksSkipped: textStats.TextScalarPostingBlocksSkipped,
		TextScalarPostingsRejected:     textStats.TextScalarPostingsRejected,
		TextCandidatesScored:           textCandidatesScored,
		TextStateLookups:               textStats.TextStateLookups,
		TextNormLookups:                textStats.TextNormLookups,
		TextMatchDetailsBuilt:          textStats.TextMatchDetailsBuilt,
		TextPositionLookups:            textStats.TextPositionLookups,
		TextPhraseCandidatesChecked:    textStats.TextPhraseCandidatesChecked,
		TextPhraseCandidatesMatched:    textStats.TextPhraseCandidatesMatched,
		DocumentsFetched:               textStats.DocumentsFetched,
		DocumentsMissing:               textStats.DocumentsMissing,
		FullDocumentScanFallbacks:      textStats.FullDocumentScanFallbacks,
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
