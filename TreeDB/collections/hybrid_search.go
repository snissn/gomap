package collections

import "errors"

// Hybrid-search errors are fail-closed sentinels. Implementations may wrap them
// with source/index-specific detail, but must not silently scan all documents or
// downgrade to stale/unavailable indexes for normal hybrid queries.
var (
	// ErrHybridSearchUnsupported reports an unsupported hybrid query shape or a
	// not-yet-implemented hybrid executor.
	ErrHybridSearchUnsupported = errors.New("collections: hybrid search unsupported")
	// ErrHybridSearchIndexUnavailable reports a missing, closed, corrupt, or
	// otherwise unavailable text/vector/scalar source required by a hybrid query.
	ErrHybridSearchIndexUnavailable = errors.New("collections: hybrid search index unavailable")
	// ErrHybridSearchStaleIndex reports a source index whose epoch does not match
	// the collection snapshot selected for a hybrid query.
	ErrHybridSearchStaleIndex = errors.New("collections: hybrid search index stale")
)

// HybridCandidateSource identifies the candidate-producing subsystem.
type HybridCandidateSource string

const (
	HybridCandidateSourceText   HybridCandidateSource = "text"
	HybridCandidateSourceVector HybridCandidateSource = "vector"
)

// HybridScoreKind identifies the native score scale carried on a candidate.
// Higher scores are better after conversion into this shared candidate shape.
type HybridScoreKind string

const (
	HybridScoreKindBM25             HybridScoreKind = "bm25"
	HybridScoreKindBM25F            HybridScoreKind = "bm25f"
	HybridScoreKindVectorSimilarity HybridScoreKind = "vector_similarity"
)

// HybridFusionMethod selects the rank-fusion algorithm. The zero value defaults
// to reciprocal-rank fusion for the first implementation.
type HybridFusionMethod string

const (
	HybridFusionMethodRRF             HybridFusionMethod = "rrf"
	HybridFusionMethodWeightedRRF     HybridFusionMethod = "weighted_rrf"
	HybridFusionMethodNormalizedScore HybridFusionMethod = "normalized_score"
)

// HybridFusionTiePolicy names the deterministic total-order policy applied
// after fused scores are computed.
type HybridFusionTiePolicy string

const (
	HybridFusionTiePolicyScoreBestRankSourceID HybridFusionTiePolicy = "fused_score_best_rank_source_order_id"
)

// HybridScalarFilterStrategy describes where scalar/metadata filters are
// applied relative to candidate generation and fusion.
type HybridScalarFilterStrategy string

const (
	HybridScalarFilterStrategyPrefilter   HybridScalarFilterStrategy = "prefilter"
	HybridScalarFilterStrategyPostfilter  HybridScalarFilterStrategy = "postfilter"
	HybridScalarFilterStrategyTextFirst   HybridScalarFilterStrategy = "text_first"
	HybridScalarFilterStrategyVectorFirst HybridScalarFilterStrategy = "vector_first"
	HybridScalarFilterStrategyUnionFusion HybridScalarFilterStrategy = "union_fusion"
)

// HybridConsistencyMode identifies how text, vector, scalar, and final document
// fetch phases bind to a collection snapshot.
type HybridConsistencyMode string

const (
	HybridConsistencyCurrentSnapshot HybridConsistencyMode = "current_snapshot"
	HybridConsistencyBoundSnapshot   HybridConsistencyMode = "bound_snapshot"
)

// HybridFailClosedReason is a low-cardinality reason suitable for counters and
// debug traces when a hybrid query refuses to run.
type HybridFailClosedReason string

const (
	HybridFailClosedReasonNone                      HybridFailClosedReason = "none"
	HybridFailClosedReasonUnsupported               HybridFailClosedReason = "unsupported"
	HybridFailClosedReasonTextIndexUnavailable      HybridFailClosedReason = "text_index_unavailable"
	HybridFailClosedReasonVectorIndexUnavailable    HybridFailClosedReason = "vector_index_unavailable"
	HybridFailClosedReasonTextIndexStale            HybridFailClosedReason = "text_index_stale"
	HybridFailClosedReasonVectorIndexStale          HybridFailClosedReason = "vector_index_stale"
	HybridFailClosedReasonScalarFilterUnbounded     HybridFailClosedReason = "scalar_filter_unbounded"
	HybridFailClosedReasonSnapshotMismatch          HybridFailClosedReason = "snapshot_mismatch"
	HybridFailClosedReasonDocumentFetchUnavailable  HybridFailClosedReason = "document_fetch_unavailable"
	HybridFailClosedReasonFullDocumentScanForbidden HybridFailClosedReason = "full_document_scan_forbidden"
)

// HybridCandidateBudgetPolicy reports whether SearchHybrid used the fixed
// caller-provided source budgets or an exact adaptive RRF budget proof.
type HybridCandidateBudgetPolicy string

const (
	HybridCandidateBudgetPolicyFixed       HybridCandidateBudgetPolicy = "fixed"
	HybridCandidateBudgetPolicyAdaptiveRRF HybridCandidateBudgetPolicy = "adaptive_rrf"
)

// HybridCandidateBudgetStopReason is a low-cardinality explanation for the
// candidate-budget decision. Fallback reasons use the same vocabulary.
type HybridCandidateBudgetStopReason string

const (
	HybridCandidateBudgetStopReasonNone                   HybridCandidateBudgetStopReason = "none"
	HybridCandidateBudgetStopReasonFixedPolicy            HybridCandidateBudgetStopReason = "fixed_policy"
	HybridCandidateBudgetStopReasonNoReduction            HybridCandidateBudgetStopReason = "no_reduction"
	HybridCandidateBudgetStopReasonEmptyScalarAllowSet    HybridCandidateBudgetStopReason = "empty_scalar_allow_set"
	HybridCandidateBudgetStopReasonSingleSourceTopK       HybridCandidateBudgetStopReason = "single_source_topk"
	HybridCandidateBudgetStopReasonExactRRFBound          HybridCandidateBudgetStopReason = "exact_rrf_bound"
	HybridCandidateBudgetStopReasonRequestedBudgetReached HybridCandidateBudgetStopReason = "requested_budget_reached"
	HybridCandidateBudgetStopReasonUnsupportedFusion      HybridCandidateBudgetStopReason = "unsupported_fusion"
	HybridCandidateBudgetStopReasonPostfilterUnsupported  HybridCandidateBudgetStopReason = "postfilter_unsupported"
	HybridCandidateBudgetStopReasonExactBoundInsufficient HybridCandidateBudgetStopReason = "exact_bound_insufficient"
)

// HybridTextQuery configures the lexical candidate source. Query syntax,
// analyzers, and BM25/BM25F scoring are owned by the text-search tracker; this
// contract fixes only the candidate budget and source naming consumed by hybrid
// planning and fusion.
type HybridTextQuery struct {
	IndexName          string `json:"index_name"`
	Query              string `json:"query"`
	CandidateLimit     int    `json:"candidate_limit,omitempty"`
	MaxPostingsScanned int    `json:"max_postings_scanned,omitempty"`
	// IncludeTextMatches opts into compact field/term attribution on text-source
	// candidates. The default hybrid candidate path is score-only so candidate
	// generation does not allocate match details for non-final candidates.
	IncludeTextMatches bool `json:"include_text_matches,omitempty"`
}

// HybridVectorQuery configures the vector candidate source. It intentionally
// carries no document materialization knobs; hybrid document fetch is a bounded
// final phase after fusion.
type HybridVectorQuery struct {
	IndexName                 string               `json:"index_name"`
	Query                     []float32            `json:"query"`
	CandidateLimit            int                  `json:"candidate_limit,omitempty"`
	EfSearch                  int                  `json:"ef_search,omitempty"`
	QueryMode                 VectorIndexQueryMode `json:"query_mode,omitempty"`
	QuantizedIndexName        string               `json:"quantized_index_name,omitempty"`
	QuantizedRerankCandidates int                  `json:"quantized_rerank_candidates,omitempty"`
}

// HybridScalarFilter describes either one bounded scalar-index leaf or an
// ordered, flat conjunction in And. Equality leaves use Value; range leaves use
// Range. Conjunction children must be leaves; arbitrary Boolean trees are not
// accepted. Strategies that need an indexed allow-set fail closed when any leaf
// cannot be served completely by its declared scalar index.
type HybridScalarFilter struct {
	IndexName string               `json:"index_name,omitempty"`
	Value     any                  `json:"value,omitempty"`
	Range     *IndexRangeOptions   `json:"range,omitempty"`
	And       []HybridScalarFilter `json:"and,omitempty"`
}

// HybridFusionOptions configures deterministic rank fusion. For RRF, RRFK=0
// means the implementation default (documented as 60 by the contract spec).
type HybridFusionOptions struct {
	Method       HybridFusionMethod      `json:"method,omitempty"`
	RRFK         int                     `json:"rrf_k,omitempty"`
	TiePolicy    HybridFusionTiePolicy   `json:"tie_policy,omitempty"`
	SourceOrder  []HybridCandidateSource `json:"source_order,omitempty"`
	TextWeight   float64                 `json:"text_weight,omitempty"`
	VectorWeight float64                 `json:"vector_weight,omitempty"`
}

// HybridConsistencyOptions describes the snapshot binding requested by the
// caller. The zero value uses the current collection snapshot after flushing
// buffered writes, matching existing collection read visibility.
type HybridConsistencyOptions struct {
	Mode HybridConsistencyMode `json:"mode,omitempty"`
}

// HybridSearchDebugOptions controls optional trace payloads. Implementations
// must keep normal production stats available without requiring candidate echo.
type HybridSearchDebugOptions struct {
	IncludeCandidates bool `json:"include_candidates,omitempty"`
}

// HybridResultMode controls how much final result payload SearchHybrid returns.
// Score-only and compact modes do not fetch final documents; full mode fetches
// bounded top-k documents after fusion.
type HybridResultMode string

const (
	HybridResultModeCompact   HybridResultMode = "compact"
	HybridResultModeScoreOnly HybridResultMode = "score_only"
	HybridResultModeFull      HybridResultMode = "full"
)

// HybridSearchOptions is the collection-level combined retrieval contract for
// scalar filters, lexical candidates, vector candidates, deterministic fusion,
// and bounded final document fetch.
type HybridSearchOptions struct {
	TopK                 int                        `json:"top_k"`
	Text                 *HybridTextQuery           `json:"text,omitempty"`
	Vector               *HybridVectorQuery         `json:"vector,omitempty"`
	ScalarFilter         *HybridScalarFilter        `json:"scalar_filter,omitempty"`
	ScalarFilterStrategy HybridScalarFilterStrategy `json:"scalar_filter_strategy,omitempty"`
	Fusion               HybridFusionOptions        `json:"fusion,omitempty"`
	// MaxChunksPerParent applies a deterministic cap to built-in chunk children
	// after fusion and scalar filtering but before final document fetch. Zero
	// disables collapse; negative values are invalid.
	MaxChunksPerParent   int                      `json:"max_chunks_per_parent,omitempty"`
	ResultMode           HybridResultMode         `json:"result_mode,omitempty"`
	IncludeDocuments     bool                     `json:"include_documents,omitempty"`
	DocumentFetchOptions DocumentFetchOptions     `json:"document_fetch_options,omitempty"`
	Consistency          HybridConsistencyOptions `json:"consistency,omitempty"`
	Debug                HybridSearchDebugOptions `json:"debug,omitempty"`
}

// HybridTextMatch carries text attribution when the lexical source can provide
// it. Terms are analyzer-normalized query/index terms where available.
type HybridTextMatch struct {
	Field string   `json:"field"`
	Terms []string `json:"terms,omitempty"`
}

// HybridSearchCandidate is the shared candidate shape produced by text and
// vector candidate-only paths before fusion.
type HybridSearchCandidate struct {
	ID          []byte                `json:"id"`
	Source      HybridCandidateSource `json:"source"`
	IndexName   string                `json:"index_name"`
	SourceRank  int                   `json:"source_rank"`
	Score       float64               `json:"score"`
	ScoreKind   HybridScoreKind       `json:"score_kind"`
	TextMatches []HybridTextMatch     `json:"text_matches,omitempty"`
}

// HybridSourceContribution records one source's contribution to a final fused
// result. FusionScore is the normalized contribution (for example one source's
// RRF term), not the native BM25/BM25F/vector score.
type HybridSourceContribution struct {
	Source      HybridCandidateSource `json:"source"`
	IndexName   string                `json:"index_name"`
	SourceRank  int                   `json:"source_rank"`
	Score       float64               `json:"score"`
	ScoreKind   HybridScoreKind       `json:"score_kind"`
	FusionScore float64               `json:"fusion_score"`
	TextMatches []HybridTextMatch     `json:"text_matches,omitempty"`
}

// HybridSearchResult is one fused final result. Rank is one-based in the final
// fused order. FusedScore is higher-is-better. Document is populated only in
// full result mode (or legacy IncludeDocuments=true) and only after bounded
// top-k selection.
type HybridSearchResult struct {
	ID            []byte                     `json:"id"`
	Rank          int                        `json:"rank"`
	FusedScore    float64                    `json:"fused_score"`
	Sources       []HybridSourceContribution `json:"sources,omitempty"`
	Document      []byte                     `json:"document,omitempty"`
	DocumentFound bool                       `json:"document_found,omitempty"`
}

// HybridSearchPlan reports the planner choices that affected source ordering,
// scalar filtering, fusion, and final fetch bounds.
type HybridSearchPlan struct {
	ScalarFilterStrategy       HybridScalarFilterStrategy `json:"scalar_filter_strategy,omitempty"`
	ScalarFilterLookupCount    int                        `json:"scalar_filter_lookup_count,omitempty"`
	ScalarFilterLookupLimit    int                        `json:"scalar_filter_lookup_limit,omitempty"`
	ScalarFilterAggregateLimit int                        `json:"scalar_filter_aggregate_limit,omitempty"`
	FusionMethod               HybridFusionMethod         `json:"fusion_method,omitempty"`
	FusionTiePolicy            HybridFusionTiePolicy      `json:"fusion_tie_policy,omitempty"`
	ResultMode                 HybridResultMode           `json:"result_mode,omitempty"`
	TextCandidateLimit         int                        `json:"text_candidate_limit,omitempty"`
	VectorCandidateLimit       int                        `json:"vector_candidate_limit,omitempty"`
	MaxChunksPerParent         int                        `json:"max_chunks_per_parent,omitempty"`
	FinalTopK                  int                        `json:"final_top_k,omitempty"`
}

// HybridSearchSnapshot reports the snapshot/epoch identity used by the query.
// Generation fields are filled when the corresponding source can expose an
// index/catalog epoch; zero means unavailable or not applicable.
type HybridSearchSnapshot struct {
	Consistency          HybridConsistencyMode `json:"consistency,omitempty"`
	CommitSeq            uint64                `json:"commit_seq,omitempty"`
	SystemRootPageID     uint64                `json:"system_root_page_id,omitempty"`
	CollectionGeneration uint64                `json:"collection_generation,omitempty"`
	TextIndexEpoch       uint64                `json:"text_index_epoch,omitempty"`
	VectorIndexEpoch     uint64                `json:"vector_index_epoch,omitempty"`
}

// HybridSearchStats is the common debug/counter vocabulary for hybrid planning,
// candidate generation, fusion, scalar filtering, and bounded final fetch.
type HybridSearchStats struct {
	TextCandidatesRequested        uint64                          `json:"text_candidates_requested,omitempty"`
	TextCandidateBudgetEffective   uint64                          `json:"text_candidate_budget_effective,omitempty"`
	TextCandidatesReturned         uint64                          `json:"text_candidates_returned,omitempty"`
	TextPostingsScanned            uint64                          `json:"text_postings_scanned,omitempty"`
	TextPostingBlocksVisited       uint64                          `json:"text_posting_blocks_visited,omitempty"`
	TextPostingBlocksSkipped       uint64                          `json:"text_posting_blocks_skipped,omitempty"`
	TextBlockMaxFallbacks          uint64                          `json:"text_block_max_fallbacks,omitempty"`
	TextBlockMaxThresholds         uint64                          `json:"text_block_max_thresholds,omitempty"`
	TextWANDPivots                 uint64                          `json:"text_wand_pivots,omitempty"`
	TextScalarPrefilterIDs         uint64                          `json:"text_scalar_prefilter_ids,omitempty"`
	TextScalarPostingBlocksSkipped uint64                          `json:"text_scalar_posting_blocks_skipped,omitempty"`
	TextScalarPostingsRejected     uint64                          `json:"text_scalar_postings_rejected,omitempty"`
	TextCandidatesScored           uint64                          `json:"text_candidates_scored,omitempty"`
	TextStateLookups               uint64                          `json:"text_state_lookups,omitempty"`
	TextNormLookups                uint64                          `json:"text_norm_lookups,omitempty"`
	TextMatchDetailsBuilt          uint64                          `json:"text_match_details_built,omitempty"`
	TextPositionLookups            uint64                          `json:"text_position_lookups,omitempty"`
	TextPhraseCandidatesChecked    uint64                          `json:"text_phrase_candidates_checked,omitempty"`
	TextPhraseCandidatesMatched    uint64                          `json:"text_phrase_candidates_matched,omitempty"`
	VectorCandidatesRequested      uint64                          `json:"vector_candidates_requested,omitempty"`
	VectorCandidateBudgetEffective uint64                          `json:"vector_candidate_budget_effective,omitempty"`
	VectorCandidatesReturned       uint64                          `json:"vector_candidates_returned,omitempty"`
	VectorCandidatesExamined       uint64                          `json:"vector_candidates_examined,omitempty"`
	VectorEdgesVisited             uint64                          `json:"vector_edges_visited,omitempty"`
	ScalarPrefilterIDs             uint64                          `json:"scalar_prefilter_ids,omitempty"`
	ScalarFilterLookups            uint64                          `json:"scalar_filter_lookups,omitempty"`
	ScalarFilterInputIDs           uint64                          `json:"scalar_filter_input_ids,omitempty"`
	ScalarFilterIntersectionSteps  uint64                          `json:"scalar_filter_intersection_steps,omitempty"`
	ScalarFilterFinalIDs           uint64                          `json:"scalar_filter_final_ids,omitempty"`
	ScalarPostfilterChecks         uint64                          `json:"scalar_postfilter_checks,omitempty"`
	ScalarFilterMatched            uint64                          `json:"scalar_filter_matched,omitempty"`
	ScalarFilterRejected           uint64                          `json:"scalar_filter_rejected,omitempty"`
	ScalarFilterSelectivityPPM     uint64                          `json:"scalar_filter_selectivity_ppm,omitempty"`
	ScalarFilterPlan               NativeScalarFilterPlan          `json:"scalar_filter_plan,omitempty"`
	ScalarFilterProbeTruncated     uint64                          `json:"scalar_filter_probe_truncated,omitempty"`
	ScalarFilterVisited            uint64                          `json:"scalar_filter_visited,omitempty"`
	ScalarFilterUnderfill          uint64                          `json:"scalar_filter_underfill,omitempty"`
	ScalarFilterExactScoring       uint64                          `json:"scalar_filter_exact_scoring,omitempty"`
	CandidatesFused                uint64                          `json:"candidates_fused,omitempty"`
	CandidatesAfterFusion          uint64                          `json:"candidates_after_fusion,omitempty"`
	FusionTextOnly                 uint64                          `json:"fusion_text_only,omitempty"`
	FusionVectorOnly               uint64                          `json:"fusion_vector_only,omitempty"`
	FusionBoth                     uint64                          `json:"fusion_both,omitempty"`
	FusionDuplicateCandidates      uint64                          `json:"fusion_duplicate_candidates,omitempty"`
	CollapseRejections             uint64                          `json:"collapse_rejections,omitempty"`
	CollapseExhaustions            uint64                          `json:"collapse_exhaustions,omitempty"`
	CandidatesAfterFilter          uint64                          `json:"candidates_after_filter,omitempty"`
	DocumentsFetched               uint64                          `json:"documents_fetched,omitempty"`
	DocumentsMissing               uint64                          `json:"documents_missing,omitempty"`
	FullDocumentScanFallbacks      uint64                          `json:"full_document_scan_fallbacks,omitempty"`
	Truncated                      uint64                          `json:"truncated,omitempty"`
	CandidateBudgetPolicy          HybridCandidateBudgetPolicy     `json:"candidate_budget_policy,omitempty"`
	CandidateBudgetStopReason      HybridCandidateBudgetStopReason `json:"candidate_budget_stop_reason,omitempty"`
	CandidateBudgetFallbacks       uint64                          `json:"candidate_budget_fallbacks,omitempty"`
	CandidateBudgetFallbackReason  HybridCandidateBudgetStopReason `json:"candidate_budget_fallback_reason,omitempty"`
	CandidateBudgetIterations      uint64                          `json:"candidate_budget_iterations,omitempty"`
	FailClosed                     uint64                          `json:"fail_closed,omitempty"`
	FailClosedReason               HybridFailClosedReason          `json:"fail_closed_reason,omitempty"`
}

// HybridSearchResponse carries the selected snapshot and plan, bounded work
// counters, optional debug candidates, and final ranked results.
type HybridSearchResponse struct {
	Snapshot   HybridSearchSnapshot    `json:"snapshot,omitempty"`
	Plan       HybridSearchPlan        `json:"plan,omitempty"`
	Stats      HybridSearchStats       `json:"stats,omitempty"`
	Candidates []HybridSearchCandidate `json:"candidates,omitempty"`
	Results    []HybridSearchResult    `json:"results,omitempty"`
}

// SearchHybrid executes a bounded hybrid search over optional text and vector
// candidate sources, an optional scalar-index filter, deterministic rank fusion,
// and optional final top-k document materialization. It fails closed instead of
// scanning primary documents when a requested source/filter/fetch path is
// unavailable or unsupported.
func (c *Collection) SearchHybrid(opts HybridSearchOptions) (HybridSearchResponse, error) {
	if c == nil {
		return HybridSearchResponse{}, errCollectionNil
	}
	if c.db == nil {
		return HybridSearchResponse{}, errCollectionDBNil
	}
	return c.searchHybrid(opts)
}
