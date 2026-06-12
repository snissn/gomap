package collections

import (
	"bytes"
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const hybridDefaultCandidateLimitMultiplier = 4

type hybridSearchExecutionPlan struct {
	public HybridSearchPlan

	text                 *HybridTextQuery
	vector               *HybridVectorQuery
	scalarFilter         *HybridScalarFilter
	scalarFilterStrategy HybridScalarFilterStrategy
	fusion               HybridFusionOptions
	topK                 int
	scalarLookupLimit    int
}

func (c *Collection) searchHybrid(opts HybridSearchOptions) (HybridSearchResponse, error) {
	plan, err := planHybridSearch(opts)
	response := HybridSearchResponse{Plan: plan.public}
	if err != nil {
		return hybridSearchFailClosed(response, hybridPlanFailClosedReason(err), err)
	}

	if err := c.flushBufferedWrites(); err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, fmt.Errorf("%w: hybrid search cannot flush current snapshot: %w", ErrHybridSearchIndexUnavailable, err))
	}
	baseState := c.db.State()
	response.Snapshot = hybridSearchSnapshotFromState(baseState)
	if baseState == nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, fmt.Errorf("%w: hybrid search current snapshot unavailable", ErrHybridSearchIndexUnavailable))
	}

	allowSet, scalarStats, err := c.hybridScalarAllowSet(plan)
	hybridMergeStats(&response.Stats, scalarStats)
	if err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonScalarFilterUnbounded, err)
	}
	if err := hybridSearchCheckCurrentSnapshot(c.db.State(), baseState); err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, err)
	}

	candidates, candidateStats, err := c.hybridSearchCandidates(plan)
	hybridMergeStats(&response.Stats, candidateStats)
	if err != nil {
		return hybridSearchFailClosed(response, hybridStatsFailClosedReason(candidateStats, hybridCandidateErrorFailClosedReason(err)), err)
	}
	if err := hybridSearchCheckCurrentSnapshot(c.db.State(), baseState); err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, err)
	}

	fusionCandidates := candidates
	if allowSet != nil && plan.scalarFilterStrategy != HybridScalarFilterStrategyPostfilter {
		fusionCandidates = hybridFilterCandidatesByScalarAllowSet(candidates, allowSet, &response.Stats)
		response.Stats.CandidatesAfterFilter = uint64(len(fusionCandidates))
	}
	if opts.Debug.IncludeCandidates {
		response.Candidates = cloneHybridSearchCandidates(fusionCandidates)
	}

	results, fusionStats, err := hybridFusePlannedCandidates(fusionCandidates, plan)
	hybridMergeStats(&response.Stats, fusionStats)
	if err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonUnsupported, err)
	}
	if allowSet != nil && plan.scalarFilterStrategy == HybridScalarFilterStrategyPostfilter {
		results = hybridFilterResultsByScalarAllowSet(results, allowSet, plan.topK, &response.Stats)
	} else if response.Stats.CandidatesAfterFilter == 0 {
		response.Stats.CandidatesAfterFilter = uint64(len(results))
	}
	response.Results = results

	if err := hybridSearchCheckCurrentSnapshot(c.db.State(), baseState); err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, err)
	}
	if opts.IncludeDocuments {
		if err := c.hybridFetchResultDocuments(&response, opts.DocumentFetchOptions, baseState); err != nil {
			reason := HybridFailClosedReasonDocumentFetchUnavailable
			if errors.Is(err, ErrHybridSearchStaleIndex) {
				reason = HybridFailClosedReasonSnapshotMismatch
			}
			return hybridSearchFailClosed(response, reason, err)
		}
	}
	hybridSetScalarFilterSelectivity(&response.Stats)
	return response, nil
}

func planHybridSearch(opts HybridSearchOptions) (hybridSearchExecutionPlan, error) {
	var plan hybridSearchExecutionPlan
	if opts.TopK <= 0 {
		return plan, fmt.Errorf("%w: hybrid search top_k must be positive", ErrHybridSearchUnsupported)
	}
	mode := opts.Consistency.Mode
	if mode == "" {
		mode = HybridConsistencyCurrentSnapshot
	}
	if mode != HybridConsistencyCurrentSnapshot {
		return plan, fmt.Errorf("%w: hybrid search consistency mode %q requires an explicit bound snapshot API", ErrHybridSearchUnsupported, mode)
	}
	if opts.Text == nil && opts.Vector == nil {
		return plan, fmt.Errorf("%w: hybrid search requires at least one text or vector source", ErrHybridSearchUnsupported)
	}

	plan.topK = opts.TopK
	plan.fusion = opts.Fusion
	plan.public.FinalTopK = opts.TopK
	plan.public.FusionMethod = opts.Fusion.Method
	if plan.public.FusionMethod == "" {
		plan.public.FusionMethod = HybridFusionMethodRRF
	}
	plan.public.FusionTiePolicy = opts.Fusion.TiePolicy
	if plan.public.FusionTiePolicy == "" {
		plan.public.FusionTiePolicy = HybridFusionTiePolicyScoreBestRankSourceID
	}

	if opts.Text != nil {
		text := *opts.Text
		if text.CandidateLimit < 0 {
			return plan, fmt.Errorf("%w: text candidate limit cannot be negative", ErrHybridSearchUnsupported)
		}
		if text.CandidateLimit == 0 {
			text.CandidateLimit = hybridDefaultCandidateLimit(opts.TopK)
		}
		plan.text = &text
		plan.public.TextCandidateLimit = text.CandidateLimit
	}
	if opts.Vector != nil {
		vector := *opts.Vector
		if vector.CandidateLimit < 0 {
			return plan, fmt.Errorf("%w: vector candidate limit cannot be negative", ErrHybridSearchUnsupported)
		}
		if vector.CandidateLimit == 0 {
			vector.CandidateLimit = hybridDefaultCandidateLimit(opts.TopK)
		}
		plan.vector = &vector
		plan.public.VectorCandidateLimit = vector.CandidateLimit
	}

	strategy, err := normalizeHybridScalarFilterStrategy(opts.ScalarFilterStrategy, plan.text != nil, plan.vector != nil, opts.ScalarFilter != nil)
	if err != nil {
		return plan, err
	}
	plan.scalarFilterStrategy = strategy
	plan.public.ScalarFilterStrategy = strategy

	if opts.ScalarFilter != nil {
		filter := *opts.ScalarFilter
		if err := validateHybridScalarFilter(filter); err != nil {
			return plan, err
		}
		plan.scalarFilter = &filter
		plan.scalarLookupLimit = hybridScalarLookupLimit(plan)
	}
	return plan, nil
}

func hybridDefaultCandidateLimit(topK int) int {
	if topK <= 0 {
		return 0
	}
	if topK > maxCollectionInt/hybridDefaultCandidateLimitMultiplier {
		return maxCollectionInt
	}
	return topK * hybridDefaultCandidateLimitMultiplier
}

func normalizeHybridScalarFilterStrategy(strategy HybridScalarFilterStrategy, hasText, hasVector, hasScalar bool) (HybridScalarFilterStrategy, error) {
	if strategy != "" {
		switch strategy {
		case HybridScalarFilterStrategyPrefilter,
			HybridScalarFilterStrategyPostfilter,
			HybridScalarFilterStrategyTextFirst,
			HybridScalarFilterStrategyVectorFirst,
			HybridScalarFilterStrategyUnionFusion:
			return strategy, nil
		default:
			return "", fmt.Errorf("%w: hybrid scalar filter strategy %q", ErrHybridSearchUnsupported, strategy)
		}
	}
	if hasScalar {
		return HybridScalarFilterStrategyPrefilter, nil
	}
	if hasText && hasVector {
		return HybridScalarFilterStrategyUnionFusion, nil
	}
	if hasText {
		return HybridScalarFilterStrategyTextFirst, nil
	}
	return HybridScalarFilterStrategyVectorFirst, nil
}

func validateHybridScalarFilter(filter HybridScalarFilter) error {
	if filter.IndexName == "" {
		return fmt.Errorf("%w: hybrid scalar filter requires an index name", ErrHybridSearchUnsupported)
	}
	if filter.Range != nil && filter.Value != nil {
		return fmt.Errorf("%w: hybrid scalar filter cannot set both value and range", ErrHybridSearchUnsupported)
	}
	if filter.Range != nil && filter.Range.Limit < 0 {
		return fmt.Errorf("%w: hybrid scalar filter range limit cannot be negative", ErrHybridSearchUnsupported)
	}
	return nil
}

func hybridScalarLookupLimit(plan hybridSearchExecutionPlan) int {
	limit := plan.topK
	if plan.text != nil {
		if limit > maxCollectionInt-plan.text.CandidateLimit {
			return maxCollectionInt
		}
		limit += plan.text.CandidateLimit
	}
	if plan.vector != nil {
		if limit > maxCollectionInt-plan.vector.CandidateLimit {
			return maxCollectionInt
		}
		limit += plan.vector.CandidateLimit
	}
	if limit <= 0 {
		return plan.topK
	}
	return limit
}

type hybridScalarAllowSet map[string]struct{}

func (c *Collection) hybridScalarAllowSet(plan hybridSearchExecutionPlan) (hybridScalarAllowSet, HybridSearchStats, error) {
	if plan.scalarFilter == nil {
		return nil, HybridSearchStats{}, nil
	}
	limit := plan.scalarLookupLimit
	if limit <= 0 {
		limit = plan.topK
	}
	filter := plan.scalarFilter
	exists, err := c.hybridScalarIndexExists(filter.IndexName)
	if err != nil {
		return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid scalar filter index %q lookup failed: %w", ErrHybridSearchIndexUnavailable, filter.IndexName, err)
	}
	if !exists {
		return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid scalar filter index %q is unavailable", ErrHybridSearchIndexUnavailable, filter.IndexName)
	}

	var ids [][]byte
	var truncated bool
	if filter.Range != nil {
		rangeOpts := *filter.Range
		rangeOpts.Limit = limit
		ids, truncated, err = c.FindByIndexRange(filter.IndexName, rangeOpts)
	} else {
		ids, truncated, err = c.FindByIndexValueLimit(filter.IndexName, filter.Value, limit)
	}
	if err != nil {
		return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid scalar filter index %q lookup failed: %w", ErrHybridSearchIndexUnavailable, filter.IndexName, err)
	}
	if ids == nil {
		ids = [][]byte{}
	}
	if truncated {
		stats := HybridSearchStats{Truncated: 1}
		return nil, stats, fmt.Errorf("%w: hybrid scalar filter index %q exceeded bounded lookup limit %d", ErrHybridSearchIndexUnavailable, filter.IndexName, limit)
	}
	set := make(hybridScalarAllowSet, len(ids))
	for _, id := range ids {
		set[string(id)] = struct{}{}
	}
	stats := HybridSearchStats{}
	if plan.scalarFilterStrategy == HybridScalarFilterStrategyPrefilter {
		stats.ScalarPrefilterIDs = uint64(len(set))
	}
	return set, stats, nil
}

func (c *Collection) hybridScalarIndexExists(indexName string) (bool, error) {
	if err := ValidateIndexName(indexName); err != nil {
		return false, err
	}
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return false, err
	}
	if catalog == nil {
		return false, errCollectionNotFound
	}
	_, ok := findIndex(catalog.meta.Indexes, indexName)
	return ok, nil
}

func (c *Collection) hybridSearchCandidates(plan hybridSearchExecutionPlan) ([]HybridSearchCandidate, HybridSearchStats, error) {
	var out []HybridSearchCandidate
	var stats HybridSearchStats
	runText := func() error {
		if plan.text == nil {
			return nil
		}
		response, err := c.SearchHybridTextCandidates(*plan.text)
		hybridMergeStats(&stats, response.Stats)
		if err != nil {
			return hybridCandidateSourceError{source: HybridCandidateSourceText, err: err}
		}
		out = append(out, response.Candidates...)
		return nil
	}
	runVector := func() error {
		if plan.vector == nil {
			return nil
		}
		response, err := c.SearchHybridVectorCandidates(*plan.vector)
		hybridMergeStats(&stats, response.Stats)
		if err != nil {
			return hybridCandidateSourceError{source: HybridCandidateSourceVector, err: err}
		}
		out = append(out, response.Candidates...)
		return nil
	}

	switch plan.scalarFilterStrategy {
	case HybridScalarFilterStrategyVectorFirst:
		if err := runVector(); err != nil {
			return nil, stats, err
		}
		if err := runText(); err != nil {
			return nil, stats, err
		}
	default:
		if err := runText(); err != nil {
			return nil, stats, err
		}
		if err := runVector(); err != nil {
			return nil, stats, err
		}
	}
	return out, stats, nil
}

func hybridFilterCandidatesByScalarAllowSet(candidates []HybridSearchCandidate, allowSet hybridScalarAllowSet, stats *HybridSearchStats) []HybridSearchCandidate {
	if allowSet == nil {
		return candidates
	}
	out := make([]HybridSearchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if stats != nil {
			stats.ScalarPostfilterChecks++
		}
		if _, ok := allowSet[string(candidate.ID)]; ok {
			if stats != nil {
				stats.ScalarFilterMatched++
			}
			out = append(out, candidate)
		} else if stats != nil {
			stats.ScalarFilterRejected++
		}
	}
	return out
}

func hybridFusePlannedCandidates(candidates []HybridSearchCandidate, plan hybridSearchExecutionPlan) ([]HybridSearchResult, HybridSearchStats, error) {
	topK := plan.topK
	if plan.scalarFilter != nil && plan.scalarFilterStrategy == HybridScalarFilterStrategyPostfilter {
		topK = len(candidates)
	}
	return FuseHybridSearchCandidates(candidates, plan.fusion, topK)
}

func hybridFilterResultsByScalarAllowSet(results []HybridSearchResult, allowSet hybridScalarAllowSet, topK int, stats *HybridSearchStats) []HybridSearchResult {
	if allowSet == nil {
		return results
	}
	filtered := make([]HybridSearchResult, 0, len(results))
	for _, result := range results {
		if stats != nil {
			stats.ScalarPostfilterChecks++
		}
		if _, ok := allowSet[string(result.ID)]; ok {
			if stats != nil {
				stats.ScalarFilterMatched++
			}
			filtered = append(filtered, result)
		} else if stats != nil {
			stats.ScalarFilterRejected++
		}
	}
	limit := topK
	if limit < 0 {
		limit = 0
	}
	if limit > len(filtered) {
		limit = len(filtered)
	}
	if stats != nil {
		stats.CandidatesAfterFilter = uint64(limit)
		if limit < len(filtered) {
			stats.Truncated += uint64(len(filtered) - limit)
		}
	}
	out := filtered[:limit]
	for i := range out {
		out[i].Rank = i + 1
	}
	return out
}

func (c *Collection) hybridFetchResultDocuments(response *HybridSearchResponse, opts DocumentFetchOptions, baseState *backenddb.DBState) error {
	if response == nil || len(response.Results) == 0 {
		return nil
	}
	ids := make([][]byte, len(response.Results))
	for i := range response.Results {
		ids[i] = response.Results[i].ID
	}
	view, err := c.OpenCollectionReadView()
	if err != nil {
		return fmt.Errorf("%w: hybrid final document read view unavailable: %w", ErrHybridSearchIndexUnavailable, err)
	}
	defer func() { _ = view.Close() }()
	if err := hybridSearchCheckCurrentSnapshot(view.snapshot.State(), baseState); err != nil {
		return err
	}
	fetch, err := view.FetchDocumentsByID(ids, opts)
	hybridMergeDocumentFetchStats(&response.Stats, fetch.Stats)
	if err != nil {
		return fmt.Errorf("%w: hybrid final document fetch failed: %w", ErrHybridSearchIndexUnavailable, err)
	}
	if len(fetch.Results) != len(response.Results) {
		return fmt.Errorf("%w: hybrid final document fetch returned %d results for %d ids", ErrHybridSearchIndexUnavailable, len(fetch.Results), len(response.Results))
	}
	for i := range fetch.Results {
		if !fetch.Results[i].Found {
			return fmt.Errorf("%w: hybrid final document %q not found", ErrHybridSearchIndexUnavailable, string(fetch.Results[i].ID))
		}
		response.Results[i].Document = bytes.Clone(fetch.Results[i].Document)
		response.Results[i].DocumentFound = true
	}
	return nil
}

func hybridSearchSnapshotFromState(state *backenddb.DBState) HybridSearchSnapshot {
	snapshot := HybridSearchSnapshot{Consistency: HybridConsistencyCurrentSnapshot}
	if state == nil {
		return snapshot
	}
	snapshot.CommitSeq = state.CommitSeq
	snapshot.SystemRootPageID = state.SystemRootPageID
	snapshot.CollectionGeneration = state.CommitSeq
	return snapshot
}

func hybridSearchCheckCurrentSnapshot(current, base *backenddb.DBState) error {
	if current == nil || base == nil {
		return fmt.Errorf("%w: hybrid search current snapshot disappeared", ErrHybridSearchStaleIndex)
	}
	if current.CommitSeq != base.CommitSeq || current.SystemRootPageID != base.SystemRootPageID || current.RootPageID != base.RootPageID {
		return fmt.Errorf("%w: hybrid search snapshot changed from commit=%d system_root=%d root=%d to commit=%d system_root=%d root=%d", ErrHybridSearchStaleIndex, base.CommitSeq, base.SystemRootPageID, base.RootPageID, current.CommitSeq, current.SystemRootPageID, current.RootPageID)
	}
	return nil
}

func hybridMergeStats(dst *HybridSearchStats, src HybridSearchStats) {
	if dst == nil {
		return
	}
	dst.TextCandidatesRequested += src.TextCandidatesRequested
	dst.TextCandidatesReturned += src.TextCandidatesReturned
	dst.TextPostingsScanned += src.TextPostingsScanned
	dst.TextPostingBlocksVisited += src.TextPostingBlocksVisited
	dst.TextPostingBlocksSkipped += src.TextPostingBlocksSkipped
	dst.TextCandidatesScored += src.TextCandidatesScored
	dst.TextStateLookups += src.TextStateLookups
	dst.TextNormLookups += src.TextNormLookups
	dst.TextMatchDetailsBuilt += src.TextMatchDetailsBuilt
	dst.VectorCandidatesRequested += src.VectorCandidatesRequested
	dst.VectorCandidatesReturned += src.VectorCandidatesReturned
	dst.VectorCandidatesExamined += src.VectorCandidatesExamined
	dst.VectorEdgesVisited += src.VectorEdgesVisited
	dst.ScalarPrefilterIDs += src.ScalarPrefilterIDs
	dst.ScalarPostfilterChecks += src.ScalarPostfilterChecks
	dst.ScalarFilterMatched += src.ScalarFilterMatched
	dst.ScalarFilterRejected += src.ScalarFilterRejected
	if src.ScalarFilterSelectivityPPM != 0 {
		dst.ScalarFilterSelectivityPPM = src.ScalarFilterSelectivityPPM
	}
	dst.CandidatesFused += src.CandidatesFused
	dst.CandidatesAfterFusion += src.CandidatesAfterFusion
	dst.FusionTextOnly += src.FusionTextOnly
	dst.FusionVectorOnly += src.FusionVectorOnly
	dst.FusionBoth += src.FusionBoth
	dst.FusionDuplicateCandidates += src.FusionDuplicateCandidates
	dst.CandidatesAfterFilter += src.CandidatesAfterFilter
	dst.DocumentsFetched += src.DocumentsFetched
	dst.DocumentsMissing += src.DocumentsMissing
	dst.FullDocumentScanFallbacks += src.FullDocumentScanFallbacks
	dst.Truncated += src.Truncated
	dst.FailClosed += src.FailClosed
	if dst.FailClosedReason == "" || dst.FailClosedReason == HybridFailClosedReasonNone {
		dst.FailClosedReason = src.FailClosedReason
	}
}

func hybridSetScalarFilterSelectivity(stats *HybridSearchStats) {
	if stats == nil {
		return
	}
	total := stats.ScalarFilterMatched + stats.ScalarFilterRejected
	if total == 0 {
		return
	}
	stats.ScalarFilterSelectivityPPM = stats.ScalarFilterMatched * 1_000_000 / total
}

func hybridMergeDocumentFetchStats(dst *HybridSearchStats, src DocumentMaterializationStats) {
	if dst == nil {
		return
	}
	dst.DocumentsFetched += src.DocumentsFetched
	dst.DocumentsMissing += src.DocumentsMissing
}

func hybridSearchFailClosed(response HybridSearchResponse, reason HybridFailClosedReason, err error) (HybridSearchResponse, error) {
	if reason == "" {
		reason = HybridFailClosedReasonUnsupported
	}
	if response.Stats.FailClosed == 0 {
		response.Stats.FailClosed = 1
	}
	response.Stats.FailClosedReason = reason
	response.Results = nil
	response.Candidates = nil
	return response, err
}

func hybridPlanFailClosedReason(err error) HybridFailClosedReason {
	if err == nil {
		return HybridFailClosedReasonNone
	}
	if stringsContainsSnapshotMode(err.Error()) {
		return HybridFailClosedReasonSnapshotMismatch
	}
	return HybridFailClosedReasonUnsupported
}

func stringsContainsSnapshotMode(s string) bool {
	return bytes.Contains([]byte(s), []byte("consistency mode")) || bytes.Contains([]byte(s), []byte("snapshot"))
}

func hybridStatsFailClosedReason(stats HybridSearchStats, fallback HybridFailClosedReason) HybridFailClosedReason {
	if stats.FailClosedReason != "" && stats.FailClosedReason != HybridFailClosedReasonNone {
		return stats.FailClosedReason
	}
	return fallback
}

type hybridCandidateSourceError struct {
	source HybridCandidateSource
	err    error
}

func (e hybridCandidateSourceError) Error() string {
	if e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e hybridCandidateSourceError) Unwrap() error { return e.err }

func hybridCandidateErrorFailClosedReason(err error) HybridFailClosedReason {
	switch {
	case err == nil:
		return HybridFailClosedReasonNone
	case errors.Is(err, ErrHybridSearchStaleIndex):
		return HybridFailClosedReasonSnapshotMismatch
	case errors.Is(err, ErrHybridSearchIndexUnavailable):
		var sourceErr hybridCandidateSourceError
		if errors.As(err, &sourceErr) {
			switch sourceErr.source {
			case HybridCandidateSourceText:
				return HybridFailClosedReasonTextIndexUnavailable
			case HybridCandidateSourceVector:
				return HybridFailClosedReasonVectorIndexUnavailable
			}
		}
		if errors.Is(err, ErrIndexNotFound) {
			return HybridFailClosedReasonTextIndexUnavailable
		}
		return HybridFailClosedReasonVectorIndexUnavailable
	default:
		return HybridFailClosedReasonUnsupported
	}
}

func cloneHybridSearchCandidates(in []HybridSearchCandidate) []HybridSearchCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]HybridSearchCandidate, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].ID = bytes.Clone(in[i].ID)
		out[i].TextMatches = cloneHybridTextMatches(in[i].TextMatches)
	}
	return out
}
