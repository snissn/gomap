package collections

import (
	"errors"
	"fmt"
)

// SearchHybridVectorCandidates adapts an existing collection vector-index
// search into the shared hybrid candidate shape.
//
// Candidate generation is no-document by construction: it uses
// SearchVectorIndexWithBuffer with IncludeDocuments=false, copies stable result
// IDs into response-owned HybridSearchCandidate values, assigns one-based source
// ranks, and fails closed if the backing vector path reports any document
// materialization or unavailable vector-index state.
func (c *Collection) SearchHybridVectorCandidates(query HybridVectorQuery) (HybridCandidateResponse, error) {
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

	opts := VectorIndexSearchOptions{
		IndexName:                 query.IndexName,
		Query:                     query.Query,
		QueryMode:                 query.QueryMode,
		QuantizedIndexName:        query.QuantizedIndexName,
		QuantizedRerankCandidates: query.QuantizedRerankCandidates,
		TopK:                      requested,
		EfSearch:                  query.EfSearch,
		StatsMode:                 VectorIndexSearchStatsModeFullDiagnostics,
	}

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
		VectorCandidatesRequested: requested64,
		VectorCandidatesReturned:  returned64,
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
