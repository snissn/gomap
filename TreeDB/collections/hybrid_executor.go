package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	hybridDefaultCandidateLimitMultiplier = 4
	// hybridCandidatePreallocLimit caps speculative combined text+vector
	// candidate preallocation. Candidate limits are caller-controlled; keep the
	// optimization bounded and let appends grow proportionally to actual source
	// results for unusually large requests.
	hybridCandidatePreallocLimit = 4 * 1024
	// hybridScalarDefaultLookupLimit is the finite indexed allow-set guardrail
	// for default scalar prefilters. Broader filters still fail closed as
	// scalar_filter_unbounded instead of falling back to document scans.
	hybridScalarDefaultLookupLimit = 4 * 1024
	// hybridScalarMaxConjuncts bounds multi-field lookup fan-out and therefore
	// caps aggregate retained IDs at lookup_limit*hybridScalarMaxConjuncts.
	hybridScalarMaxConjuncts = 16
)

type hybridSearchExecutionPlan struct {
	public HybridSearchPlan

	text                          *HybridTextQuery
	textCandidateScanBudget       int
	vector                        *HybridVectorQuery
	vectorCandidateAllowSetBudget int
	scalarFilter                  *HybridScalarFilter
	scalarFilterStrategy          HybridScalarFilterStrategy
	fusion                        HybridFusionOptions
	resultMode                    HybridResultMode
	maxChunksPerParent            int
	topK                          int
	scalarLookupLimit             int
	scalarAggregateLimit          int
}

type hybridSearchStateToken struct {
	state     backenddb.StateToken
	available bool
}

func hybridSearchDBStateToken(db *backenddb.DB) hybridSearchStateToken {
	if db == nil {
		return hybridSearchStateToken{}
	}
	state, ok := db.StateToken()
	return hybridSearchStateToken{state: state, available: ok}
}

func hybridSearchSnapshotStateToken(snapshot *backenddb.Snapshot) hybridSearchStateToken {
	if snapshot == nil {
		return hybridSearchStateToken{}
	}
	state, ok := snapshot.StateToken()
	return hybridSearchStateToken{state: state, available: ok}
}

func (c *Collection) searchHybrid(opts HybridSearchOptions) (HybridSearchResponse, error) {
	return c.searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyDefault)
}

func (c *Collection) searchHybridWithCandidateBudgetPolicy(opts HybridSearchOptions, budgetPolicy hybridCandidateBudgetPolicyMode) (HybridSearchResponse, error) {
	plan, err := planHybridSearch(opts)
	response := HybridSearchResponse{Plan: plan.public}
	if err != nil {
		return hybridSearchFailClosed(response, hybridPlanFailClosedReason(err), err)
	}

	if err := c.flushBufferedWrites(); err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, fmt.Errorf("%w: hybrid search cannot flush current snapshot: %w", ErrHybridSearchIndexUnavailable, err))
	}
	baseState := hybridSearchDBStateToken(c.db)
	response.Snapshot = hybridSearchSnapshotFromState(baseState)
	if !baseState.available {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, fmt.Errorf("%w: hybrid search current snapshot unavailable", ErrHybridSearchIndexUnavailable))
	}

	allowSet, scalarStats, err := c.hybridScalarAllowSet(plan)
	hybridMergeStats(&response.Stats, scalarStats)
	if err != nil {
		reason := HybridFailClosedReasonScalarFilterUnbounded
		if errors.Is(err, ErrHybridSearchStaleIndex) {
			reason = HybridFailClosedReasonSnapshotMismatch
		}
		return hybridSearchFailClosed(response, reason, err)
	}
	if err := hybridSearchCheckCurrentSnapshot(hybridSearchDBStateToken(c.db), baseState); err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, err)
	}

	var candidateAllowSet hybridScalarAllowSet
	if plan.scalarFilterStrategy == HybridScalarFilterStrategyPrefilter {
		candidateAllowSet = allowSet
	}
	candidates, candidateStats, err := c.hybridSearchCandidatesWithBudgetPolicy(plan, candidateAllowSet, allowSet, budgetPolicy)
	hybridMergeStats(&response.Stats, candidateStats)
	if err != nil {
		return hybridSearchFailClosed(response, hybridStatsFailClosedReason(candidateStats, hybridCandidateErrorFailClosedReason(err)), err)
	}
	if err := hybridSearchCheckCurrentSnapshot(hybridSearchDBStateToken(c.db), baseState); err != nil {
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
		filterTopK := plan.topK
		if hybridParentCollapseBinding(plan.topK, plan.maxChunksPerParent) {
			filterTopK = len(results)
		}
		results = hybridFilterResultsByScalarAllowSet(results, allowSet, filterTopK, &response.Stats)
	} else if response.Stats.CandidatesAfterFilter == 0 {
		response.Stats.CandidatesAfterFilter = uint64(len(results))
	}
	results = hybridCollapseResultsByParent(results, plan.topK, plan.maxChunksPerParent, &response.Stats)
	if plan.resultMode == HybridResultModeScoreOnly {
		hybridStripResultSources(results)
	}
	response.Results = results

	if err := hybridSearchCheckCurrentSnapshot(hybridSearchDBStateToken(c.db), baseState); err != nil {
		return hybridSearchFailClosed(response, HybridFailClosedReasonSnapshotMismatch, err)
	}
	if plan.resultMode == HybridResultModeFull {
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

	resultMode, err := normalizeHybridResultMode(opts.ResultMode, opts.IncludeDocuments, opts.DocumentFetchOptions)
	if err != nil {
		return plan, err
	}

	if opts.MaxChunksPerParent < 0 {
		return plan, fmt.Errorf("%w: max_chunks_per_parent cannot be negative", ErrHybridSearchUnsupported)
	}
	plan.topK = opts.TopK
	plan.fusion = opts.Fusion
	plan.resultMode = resultMode
	plan.maxChunksPerParent = opts.MaxChunksPerParent
	plan.public.FinalTopK = opts.TopK
	plan.public.MaxChunksPerParent = opts.MaxChunksPerParent
	plan.public.ResultMode = resultMode
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
		plan.textCandidateScanBudget = text.CandidateLimit
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
		plan.vectorCandidateAllowSetBudget = vector.CandidateLimit
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
		lookupCount := hybridScalarFilterLookupCount(filter)
		plan.scalarAggregateLimit = hybridScalarAggregateLimit(plan.scalarLookupLimit, lookupCount)
		plan.public.ScalarFilterLookupCount = lookupCount
		plan.public.ScalarFilterLookupLimit = plan.scalarLookupLimit
		plan.public.ScalarFilterAggregateLimit = plan.scalarAggregateLimit
	}
	return plan, nil
}

func normalizeHybridResultMode(mode HybridResultMode, includeDocuments bool, fetchOptions DocumentFetchOptions) (HybridResultMode, error) {
	if mode == "" {
		if includeDocuments {
			return HybridResultModeFull, nil
		}
		if vectorIndexDocumentFetchOptionsNonZero(fetchOptions) {
			return "", fmt.Errorf("%w: hybrid result mode %q cannot set document_fetch_options", ErrHybridSearchUnsupported, HybridResultModeCompact)
		}
		return HybridResultModeCompact, nil
	}
	switch mode {
	case HybridResultModeCompact, HybridResultModeScoreOnly:
		if includeDocuments {
			return "", fmt.Errorf("%w: hybrid result mode %q conflicts with include_documents=true", ErrHybridSearchUnsupported, mode)
		}
		if vectorIndexDocumentFetchOptionsNonZero(fetchOptions) {
			return "", fmt.Errorf("%w: hybrid result mode %q cannot set document_fetch_options", ErrHybridSearchUnsupported, mode)
		}
		return mode, nil
	case HybridResultModeFull:
		return mode, nil
	default:
		return "", fmt.Errorf("%w: hybrid result mode %q", ErrHybridSearchUnsupported, mode)
	}
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
	if len(filter.And) == 0 {
		return validateHybridScalarFilterLeaf(filter)
	}
	if filter.IndexName != "" || filter.Value != nil || filter.Range != nil {
		return fmt.Errorf("%w: hybrid scalar conjunction cannot also set index_name, value, or range", ErrHybridSearchUnsupported)
	}
	if len(filter.And) < 2 {
		return fmt.Errorf("%w: hybrid scalar conjunction requires at least two leaves", ErrHybridSearchUnsupported)
	}
	if len(filter.And) > hybridScalarMaxConjuncts {
		return fmt.Errorf("%w: hybrid scalar conjunction has %d leaves; maximum is %d", ErrHybridSearchUnsupported, len(filter.And), hybridScalarMaxConjuncts)
	}
	for i := range filter.And {
		if len(filter.And[i].And) != 0 {
			return fmt.Errorf("%w: hybrid scalar conjunction leaf %d cannot contain a nested conjunction", ErrHybridSearchUnsupported, i)
		}
		if err := validateHybridScalarFilterLeaf(filter.And[i]); err != nil {
			return fmt.Errorf("hybrid scalar conjunction leaf %d: %w", i, err)
		}
	}
	return nil
}

func validateHybridScalarFilterLeaf(filter HybridScalarFilter) error {
	if filter.IndexName == "" {
		return fmt.Errorf("%w: hybrid scalar filter requires an index name", ErrHybridSearchUnsupported)
	}
	if err := ValidateIndexName(filter.IndexName); err != nil {
		return fmt.Errorf("%w: hybrid scalar filter index name %q is invalid: %v", ErrHybridSearchUnsupported, filter.IndexName, err)
	}
	if filter.Range != nil && filter.Value != nil {
		return fmt.Errorf("%w: hybrid scalar filter cannot set both value and range", ErrHybridSearchUnsupported)
	}
	if filter.Range == nil && filter.Value == nil {
		return fmt.Errorf("%w: hybrid scalar equality filter requires a value", ErrHybridSearchUnsupported)
	}
	if filter.Range != nil {
		if filter.Range.Limit < 0 {
			return fmt.Errorf("%w: hybrid scalar filter range limit cannot be negative", ErrHybridSearchUnsupported)
		}
		if filter.Range.Lower.Unbounded && filter.Range.Upper.Unbounded {
			return fmt.Errorf("%w: hybrid scalar filter range requires at least one bound", ErrHybridSearchUnsupported)
		}
		if !filter.Range.Lower.Unbounded && filter.Range.Lower.Value == nil {
			return fmt.Errorf("%w: hybrid scalar filter lower bound requires a value", ErrHybridSearchUnsupported)
		}
		if !filter.Range.Upper.Unbounded && filter.Range.Upper.Value == nil {
			return fmt.Errorf("%w: hybrid scalar filter upper bound requires a value", ErrHybridSearchUnsupported)
		}
	}
	return nil
}

func hybridScalarFilterLookupCount(filter HybridScalarFilter) int {
	if len(filter.And) != 0 {
		return len(filter.And)
	}
	return 1
}

func hybridScalarAggregateLimit(lookupLimit, lookupCount int) int {
	if lookupLimit <= 0 || lookupCount <= 0 {
		return 0
	}
	if lookupLimit > maxCollectionInt/lookupCount {
		return maxCollectionInt
	}
	return lookupLimit * lookupCount
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
		limit = plan.topK
	}
	if limit < hybridScalarDefaultLookupLimit {
		limit = hybridScalarDefaultLookupLimit
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
	aggregateLimit := plan.scalarAggregateLimit
	if aggregateLimit <= 0 {
		aggregateLimit = hybridScalarAggregateLimit(limit, hybridScalarFilterLookupCount(*plan.scalarFilter))
	}
	lookupState := hybridSearchDBStateToken(c.db)
	if !lookupState.available {
		return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid scalar lookup snapshot unavailable", ErrHybridSearchStaleIndex)
	}

	filters := plan.scalarFilter.And
	if len(filters) == 0 {
		filters = []HybridScalarFilter{*plan.scalarFilter}
	}
	sets := make([]hybridScalarAllowSet, 0, len(filters))
	stats := HybridSearchStats{}
	for i := range filters {
		stats.ScalarFilterLookups++
		set, inputIDs, truncated, err := c.hybridScalarLeafAllowSet(filters[i], limit)
		stats.ScalarFilterInputIDs += inputIDs
		if truncated {
			stats.Truncated++
		}
		if err != nil {
			return nil, stats, err
		}
		if stats.ScalarFilterInputIDs > uint64(aggregateLimit) {
			stats.Truncated++
			return nil, stats, fmt.Errorf("%w: hybrid scalar conjunction exceeded aggregate input-ID limit %d", ErrHybridSearchIndexUnavailable, aggregateLimit)
		}
		if err := hybridSearchCheckCurrentSnapshot(hybridSearchDBStateToken(c.db), lookupState); err != nil {
			return nil, stats, err
		}
		sets = append(sets, set)
	}

	sort.SliceStable(sets, func(i, j int) bool {
		return len(sets[i]) < len(sets[j])
	})
	allowSet := sets[0]
	for i := 1; i < len(sets); i++ {
		stats.ScalarFilterIntersectionSteps++
		for id := range allowSet {
			if _, ok := sets[i][id]; !ok {
				delete(allowSet, id)
			}
		}
	}
	stats.ScalarFilterFinalIDs = uint64(len(allowSet))
	if plan.scalarFilterStrategy == HybridScalarFilterStrategyPrefilter {
		stats.ScalarPrefilterIDs = uint64(len(allowSet))
	}
	return allowSet, stats, nil
}

func (c *Collection) hybridScalarLeafAllowSet(filter HybridScalarFilter, limit int) (hybridScalarAllowSet, uint64, bool, error) {
	exists, err := c.hybridScalarIndexExists(filter.IndexName)
	if err != nil {
		return nil, 0, false, fmt.Errorf("%w: hybrid scalar filter index %q lookup failed: %w", ErrHybridSearchIndexUnavailable, filter.IndexName, err)
	}
	if !exists {
		return nil, 0, false, fmt.Errorf("%w: hybrid scalar filter index %q is unavailable", ErrHybridSearchIndexUnavailable, filter.IndexName)
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
	inputIDs := uint64(len(ids))
	if err != nil {
		return nil, inputIDs, false, fmt.Errorf("%w: hybrid scalar filter index %q lookup failed: %w", ErrHybridSearchIndexUnavailable, filter.IndexName, err)
	}
	if truncated {
		return nil, inputIDs, true, fmt.Errorf("%w: hybrid scalar filter index %q exceeded bounded lookup limit %d", ErrHybridSearchIndexUnavailable, filter.IndexName, limit)
	}
	set := make(hybridScalarAllowSet, len(ids))
	for _, id := range ids {
		set[string(id)] = struct{}{}
	}
	return set, inputIDs, false, nil
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
func (c *Collection) hybridSearchCandidates(plan hybridSearchExecutionPlan, allowSet hybridScalarAllowSet) ([]HybridSearchCandidate, HybridSearchStats, error) {
	var out []HybridSearchCandidate
	if capHint := hybridSearchCandidatePreallocHint(plan); capHint > 0 {
		out = make([]HybridSearchCandidate, 0, capHint)
	}
	var stats HybridSearchStats
	var textResponse, vectorResponse HybridCandidateResponse
	var textErr, vectorErr error

	// Explicit source-order strategies retain sequential short-circuit
	// semantics. Other strategies can overlap independent candidate reads.
	switch plan.scalarFilterStrategy {
	case HybridScalarFilterStrategyTextFirst:
		if plan.text != nil {
			textResponse, textErr = c.searchHybridTextCandidatesWithScanBudget(*plan.text, allowSet, plan.textCandidateScanBudget)
		}
		if textErr == nil && plan.vector != nil {
			vectorResponse, vectorErr = c.searchHybridVectorCandidatesWithAllowSetBudget(*plan.vector, allowSet, plan.vectorCandidateAllowSetBudget)
		}
	case HybridScalarFilterStrategyVectorFirst:
		if plan.vector != nil {
			vectorResponse, vectorErr = c.searchHybridVectorCandidatesWithAllowSetBudget(*plan.vector, allowSet, plan.vectorCandidateAllowSetBudget)
		}
		if vectorErr == nil && plan.text != nil {
			textResponse, textErr = c.searchHybridTextCandidatesWithScanBudget(*plan.text, allowSet, plan.textCandidateScanBudget)
		}
	default:
		if plan.text != nil && plan.vector != nil {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				vectorResponse, vectorErr = c.searchHybridVectorCandidatesWithAllowSetBudget(*plan.vector, allowSet, plan.vectorCandidateAllowSetBudget)
			}()
			textResponse, textErr = c.searchHybridTextCandidatesWithScanBudget(*plan.text, allowSet, plan.textCandidateScanBudget)
			wg.Wait()
		} else {
			if plan.text != nil {
				textResponse, textErr = c.searchHybridTextCandidatesWithScanBudget(*plan.text, allowSet, plan.textCandidateScanBudget)
			}
			if plan.vector != nil {
				vectorResponse, vectorErr = c.searchHybridVectorCandidatesWithAllowSetBudget(*plan.vector, allowSet, plan.vectorCandidateAllowSetBudget)
			}
		}
	}
	if plan.scalarFilterStrategy == HybridScalarFilterStrategyVectorFirst {
		hybridMergeStats(&stats, vectorResponse.Stats)
		hybridMergeStats(&stats, textResponse.Stats)
	} else {
		hybridMergeStats(&stats, textResponse.Stats)
		hybridMergeStats(&stats, vectorResponse.Stats)
	}
	if plan.scalarFilterStrategy == HybridScalarFilterStrategyVectorFirst {
		if vectorErr != nil {
			return nil, stats, hybridCandidateSourceError{source: HybridCandidateSourceVector, err: vectorErr}
		}
		if textErr != nil {
			return nil, stats, hybridCandidateSourceError{source: HybridCandidateSourceText, err: textErr}
		}
		out = appendHybridSearchCandidates(out, vectorResponse.Candidates)
		out = appendHybridSearchCandidates(out, textResponse.Candidates)
		return out, stats, nil
	}
	if textErr != nil {
		return nil, stats, hybridCandidateSourceError{source: HybridCandidateSourceText, err: textErr}
	}
	if vectorErr != nil {
		return nil, stats, hybridCandidateSourceError{source: HybridCandidateSourceVector, err: vectorErr}
	}
	out = appendHybridSearchCandidates(out, textResponse.Candidates)
	out = appendHybridSearchCandidates(out, vectorResponse.Candidates)
	return out, stats, nil
}

func hybridSearchCandidatePreallocHint(plan hybridSearchExecutionPlan) int {
	if plan.text == nil || plan.vector == nil {
		return 0
	}
	hint := 0
	if plan.text.CandidateLimit > 0 {
		hint = plan.text.CandidateLimit
		if hint >= hybridCandidatePreallocLimit {
			return hybridCandidatePreallocLimit
		}
	}
	if plan.vector.CandidateLimit > 0 {
		if plan.vector.CandidateLimit >= hybridCandidatePreallocLimit-hint {
			return hybridCandidatePreallocLimit
		}
		hint += plan.vector.CandidateLimit
	}
	return hint
}

func appendHybridSearchCandidates(dst, src []HybridSearchCandidate) []HybridSearchCandidate {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		return src
	}
	return append(dst, src...)
}

func hybridFilterCandidatesByScalarAllowSet(candidates []HybridSearchCandidate, allowSet hybridScalarAllowSet, stats *HybridSearchStats) []HybridSearchCandidate {
	if allowSet == nil {
		return candidates
	}
	out := candidates[:0]
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

func hybridParentCollapseBinding(topK, maxChunksPerParent int) bool {
	return maxChunksPerParent > 0 && maxChunksPerParent < topK
}

func hybridFusePlannedCandidates(candidates []HybridSearchCandidate, plan hybridSearchExecutionPlan) ([]HybridSearchResult, HybridSearchStats, error) {
	topK := plan.topK
	if hybridParentCollapseBinding(plan.topK, plan.maxChunksPerParent) || (plan.scalarFilter != nil && plan.scalarFilterStrategy == HybridScalarFilterStrategyPostfilter) {
		topK = len(candidates)
	}
	return FuseHybridSearchCandidates(candidates, plan.fusion, topK)
}

func hybridFilterResultsByScalarAllowSet(results []HybridSearchResult, allowSet hybridScalarAllowSet, topK int, stats *HybridSearchStats) []HybridSearchResult {
	if allowSet == nil {
		return results
	}
	filtered := results[:0]
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

// hybridCollapseResultsByParent preserves fused order while enforcing the
// canonical built-in <parent>#<ordinal> chunk identity. IDs that do not both
// parse and round-trip through ChildDocumentID are independent documents and
// never share a parent-count key.
func hybridCollapseResultsByParent(results []HybridSearchResult, topK, maxChunksPerParent int, stats *HybridSearchStats) []HybridSearchResult {
	if !hybridParentCollapseBinding(topK, maxChunksPerParent) {
		return results
	}
	parentCounts := make(map[string]int, min(len(results), topK))
	out := results[:0]
	for _, result := range results {
		if len(out) == topK {
			break
		}
		id := string(result.ID)
		parentID, ordinal, child := chunking.ParseChildID(id)
		if child && chunking.ChildDocumentID(parentID, ordinal) == id && chunking.ValidateParentID(parentID) == nil {
			if parentCounts[parentID] >= maxChunksPerParent {
				if stats != nil {
					stats.CollapseRejections++
				}
				continue
			}
			parentCounts[parentID]++
		}
		result.Rank = len(out) + 1
		out = append(out, result)
	}
	if stats != nil {
		stats.Truncated += uint64(len(results) - len(out))
		if len(out) < topK {
			stats.CollapseExhaustions++
		}
	}
	return out
}

func (c *Collection) hybridFetchResultDocuments(response *HybridSearchResponse, opts DocumentFetchOptions, baseState hybridSearchStateToken) error {
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
	if err := hybridSearchCheckCurrentSnapshot(hybridSearchSnapshotStateToken(view.snapshot), baseState); err != nil {
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

func hybridSearchSnapshotFromState(token hybridSearchStateToken) HybridSearchSnapshot {
	snapshot := HybridSearchSnapshot{Consistency: HybridConsistencyCurrentSnapshot}
	if !token.available {
		return snapshot
	}
	state := token.state
	snapshot.CommitSeq = state.CommitSeq
	snapshot.SystemRootPageID = state.SystemRootPageID
	snapshot.CollectionGeneration = state.CommitSeq
	return snapshot
}

func hybridSearchCheckCurrentSnapshot(current, base hybridSearchStateToken) error {
	if !current.available || !base.available {
		return fmt.Errorf("%w: hybrid search current snapshot disappeared", ErrHybridSearchStaleIndex)
	}
	currentState := current.state
	baseState := base.state
	if currentState.CommitSeq != baseState.CommitSeq || currentState.SystemRootPageID != baseState.SystemRootPageID || currentState.RootPageID != baseState.RootPageID {
		return fmt.Errorf("%w: hybrid search snapshot changed from commit=%d system_root=%d root=%d to commit=%d system_root=%d root=%d", ErrHybridSearchStaleIndex, baseState.CommitSeq, baseState.SystemRootPageID, baseState.RootPageID, currentState.CommitSeq, currentState.SystemRootPageID, currentState.RootPageID)
	}
	return nil
}

func hybridMergeStats(dst *HybridSearchStats, src HybridSearchStats) {
	if dst == nil {
		return
	}
	dst.TextCandidatesRequested += src.TextCandidatesRequested
	dst.TextCandidateBudgetEffective += src.TextCandidateBudgetEffective
	dst.TextCandidatesReturned += src.TextCandidatesReturned
	dst.TextPostingsScanned += src.TextPostingsScanned
	dst.TextPostingBlocksVisited += src.TextPostingBlocksVisited
	dst.TextPostingBlocksSkipped += src.TextPostingBlocksSkipped
	dst.TextBlockMaxFallbacks += src.TextBlockMaxFallbacks
	dst.TextBlockMaxThresholds += src.TextBlockMaxThresholds
	dst.TextWANDPivots += src.TextWANDPivots
	dst.TextScalarPrefilterIDs += src.TextScalarPrefilterIDs
	dst.TextScalarPostingBlocksSkipped += src.TextScalarPostingBlocksSkipped
	dst.TextScalarPostingsRejected += src.TextScalarPostingsRejected
	dst.TextCandidatesScored += src.TextCandidatesScored
	dst.TextStateLookups += src.TextStateLookups
	dst.TextNormLookups += src.TextNormLookups
	dst.TextMatchDetailsBuilt += src.TextMatchDetailsBuilt
	dst.TextPositionLookups += src.TextPositionLookups
	dst.TextPhraseCandidatesChecked += src.TextPhraseCandidatesChecked
	dst.TextPhraseCandidatesMatched += src.TextPhraseCandidatesMatched
	dst.VectorCandidatesRequested += src.VectorCandidatesRequested
	dst.VectorCandidateBudgetEffective += src.VectorCandidateBudgetEffective
	dst.VectorCandidatesReturned += src.VectorCandidatesReturned
	dst.VectorCandidatesExamined += src.VectorCandidatesExamined
	dst.VectorEdgesVisited += src.VectorEdgesVisited
	dst.ScalarPrefilterIDs += src.ScalarPrefilterIDs
	dst.ScalarFilterLookups += src.ScalarFilterLookups
	dst.ScalarFilterInputIDs += src.ScalarFilterInputIDs
	dst.ScalarFilterIntersectionSteps += src.ScalarFilterIntersectionSteps
	dst.ScalarFilterFinalIDs += src.ScalarFilterFinalIDs
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
	dst.CollapseRejections += src.CollapseRejections
	dst.CollapseExhaustions += src.CollapseExhaustions
	dst.FusionDuplicateCandidates += src.FusionDuplicateCandidates
	dst.CandidatesAfterFilter += src.CandidatesAfterFilter
	dst.DocumentsFetched += src.DocumentsFetched
	dst.DocumentsMissing += src.DocumentsMissing
	dst.FullDocumentScanFallbacks += src.FullDocumentScanFallbacks
	dst.Truncated += src.Truncated
	if dst.CandidateBudgetPolicy == "" {
		dst.CandidateBudgetPolicy = src.CandidateBudgetPolicy
	}
	if dst.CandidateBudgetStopReason == "" || dst.CandidateBudgetStopReason == HybridCandidateBudgetStopReasonNone {
		dst.CandidateBudgetStopReason = src.CandidateBudgetStopReason
	}
	dst.CandidateBudgetFallbacks += src.CandidateBudgetFallbacks
	if dst.CandidateBudgetFallbackReason == "" || dst.CandidateBudgetFallbackReason == HybridCandidateBudgetStopReasonNone {
		dst.CandidateBudgetFallbackReason = src.CandidateBudgetFallbackReason
	}
	dst.CandidateBudgetIterations += src.CandidateBudgetIterations
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
	if response.Stats.FailClosed != 1 {
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

func hybridStripResultSources(results []HybridSearchResult) {
	for i := range results {
		results[i].Sources = nil
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
