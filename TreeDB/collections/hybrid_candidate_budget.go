package collections

const hybridAdaptiveCandidateBudgetMaxProbeIterations = 4

type hybridCandidateBudgetPolicyMode uint8

const (
	hybridCandidateBudgetPolicyDefault hybridCandidateBudgetPolicyMode = iota
	hybridCandidateBudgetPolicyFixed
	hybridCandidateBudgetPolicyAdaptive
)

type hybridCandidateBudgetRRFParams struct {
	rrfK         int
	textWeight   float64
	vectorWeight float64
}

type hybridCandidateBudgetSourceState struct {
	present   bool
	requested int
	effective int
	returned  int
}

type hybridCandidateBudgetProofAccumulator struct {
	fusedScore  float64
	textScore   float64
	vectorScore float64
	hasText     bool
	hasVector   bool
}

func hybridCandidateBudgetPolicyIsAdaptive(policy HybridCandidateBudgetPolicy) bool {
	switch policy {
	case HybridCandidateBudgetPolicyAdaptiveRRF:
		return true
	default:
		return false
	}
}

func (c *Collection) hybridSearchCandidatesWithBudgetPolicy(plan hybridSearchExecutionPlan, candidateAllowSet, filterAllowSet hybridScalarAllowSet, mode hybridCandidateBudgetPolicyMode) ([]HybridSearchCandidate, HybridSearchStats, error) {
	defaultMode := mode == hybridCandidateBudgetPolicyDefault
	if defaultMode {
		mode = hybridCandidateBudgetPolicyAdaptive
	}
	if filterAllowSet != nil && len(filterAllowSet) == 0 && plan.scalarFilterStrategy != HybridScalarFilterStrategyPostfilter {
		policy := HybridCandidateBudgetPolicyFixed
		stop := HybridCandidateBudgetStopReasonFixedPolicy
		fallback := HybridCandidateBudgetStopReasonNone
		if mode != hybridCandidateBudgetPolicyFixed {
			if _, fallbackReason, ok := hybridCandidateBudgetRRFParamsForPlan(plan); ok {
				policy = HybridCandidateBudgetPolicyAdaptiveRRF
				stop = HybridCandidateBudgetStopReasonEmptyScalarAllowSet
			} else {
				fallback = fallbackReason
			}
		}
		stats := HybridSearchStats{}
		hybridCandidateBudgetFinalizeStats(&stats, plan, 0, 0, policy, stop, fallback, 1)
		return nil, stats, nil
	}
	if mode == hybridCandidateBudgetPolicyFixed {
		return c.hybridSearchCandidatesFixedBudget(plan, candidateAllowSet, HybridCandidateBudgetStopReasonFixedPolicy, HybridCandidateBudgetStopReasonNone)
	}
	// Exact top-k budget proofs do not prove enough distinct chunk parents for
	// collapse backfill. Honor the declared source budgets rather than reducing
	// them or expanding them implicitly.
	if hybridParentCollapseBinding(plan.topK, plan.maxChunksPerParent) {
		return c.hybridSearchCandidatesFixedBudget(plan, candidateAllowSet, HybridCandidateBudgetStopReasonExactBoundInsufficient, HybridCandidateBudgetStopReasonExactBoundInsufficient)
	}

	params, fallbackReason, ok := hybridCandidateBudgetRRFParamsForPlan(plan)
	if !ok {
		return c.hybridSearchCandidatesFixedBudget(plan, candidateAllowSet, fallbackReason, fallbackReason)
	}
	if plan.scalarFilter != nil && plan.scalarFilterStrategy == HybridScalarFilterStrategyPostfilter {
		return c.hybridSearchCandidatesFixedBudget(plan, candidateAllowSet, HybridCandidateBudgetStopReasonPostfilterUnsupported, HybridCandidateBudgetStopReasonPostfilterUnsupported)
	}
	if defaultMode && (hybridCandidateBudgetSourceCount(plan) > 1 || plan.scalarFilter != nil) {
		return c.hybridSearchCandidatesFixedBudget(plan, candidateAllowSet, HybridCandidateBudgetStopReasonExactBoundInsufficient, HybridCandidateBudgetStopReasonExactBoundInsufficient)
	}
	if !hybridCandidateBudgetCanReduce(plan) {
		return c.hybridSearchCandidatesFixedBudget(plan, candidateAllowSet, HybridCandidateBudgetStopReasonNoReduction, HybridCandidateBudgetStopReasonNone)
	}
	if hybridCandidateBudgetSourceCount(plan) == 1 && plan.scalarFilter == nil {
		return c.hybridSearchCandidatesSingleSourceAdaptive(plan, candidateAllowSet)
	}
	return c.hybridSearchCandidatesHybridAdaptive(plan, candidateAllowSet, filterAllowSet, params)
}

func (c *Collection) hybridSearchCandidatesFixedBudget(plan hybridSearchExecutionPlan, allowSet hybridScalarAllowSet, stopReason, fallbackReason HybridCandidateBudgetStopReason) ([]HybridSearchCandidate, HybridSearchStats, error) {
	candidates, stats, err := c.hybridSearchCandidates(plan, allowSet)
	hybridCandidateBudgetFinalizeStats(&stats, plan, hybridCandidateBudgetRequestedText(plan), hybridCandidateBudgetRequestedVector(plan), HybridCandidateBudgetPolicyFixed, stopReason, fallbackReason, 1)
	return candidates, stats, err
}

func (c *Collection) hybridSearchCandidatesSingleSourceAdaptive(plan hybridSearchExecutionPlan, allowSet hybridScalarAllowSet) ([]HybridSearchCandidate, HybridSearchStats, error) {
	textBudget := hybridCandidateBudgetRequestedText(plan)
	vectorBudget := hybridCandidateBudgetRequestedVector(plan)
	if plan.text != nil {
		textBudget = hybridAdaptiveInitialCandidateBudget(textBudget, plan.topK)
	}
	if plan.vector != nil {
		vectorBudget = hybridAdaptiveInitialCandidateBudget(vectorBudget, plan.topK)
	}
	attemptPlan := hybridSearchPlanWithCandidateBudgets(plan, textBudget, vectorBudget)
	candidates, stats, err := c.hybridSearchCandidates(attemptPlan, allowSet)
	policy := HybridCandidateBudgetPolicyAdaptiveRRF
	stopReason := HybridCandidateBudgetStopReasonSingleSourceTopK
	if err != nil {
		// Candidate-source errors already fail closed. Report the budget actually
		// attempted; fixed-budget fallback remains available to tests through the
		// internal policy seam but is not safer after a source has failed closed.
		stopReason = HybridCandidateBudgetStopReasonExactBoundInsufficient
	}
	hybridCandidateBudgetFinalizeStats(&stats, plan, textBudget, vectorBudget, policy, stopReason, HybridCandidateBudgetStopReasonNone, 1)
	return candidates, stats, err
}

func (c *Collection) hybridSearchCandidatesHybridAdaptive(plan hybridSearchExecutionPlan, candidateAllowSet, filterAllowSet hybridScalarAllowSet, params hybridCandidateBudgetRRFParams) ([]HybridSearchCandidate, HybridSearchStats, error) {
	textRequested := hybridCandidateBudgetRequestedText(plan)
	vectorRequested := hybridCandidateBudgetRequestedVector(plan)
	textBudget := hybridAdaptiveInitialCandidateBudget(textRequested, plan.topK)
	vectorBudget := hybridAdaptiveInitialCandidateBudget(vectorRequested, plan.topK)

	var work HybridSearchStats
	var lastCandidates []HybridSearchCandidate
	var lastStats HybridSearchStats
	iterations := 0
	for {
		iterations++
		attemptPlan := hybridSearchPlanWithCandidateBudgets(plan, textBudget, vectorBudget)
		candidates, stats, err := c.hybridSearchCandidates(attemptPlan, candidateAllowSet)
		hybridCandidateBudgetAccumulateAttemptWork(&work, stats)
		lastCandidates = candidates
		lastStats = stats
		if err != nil {
			hybridCandidateBudgetApplyAccumulatedWork(&lastStats, work)
			hybridCandidateBudgetFinalizeStats(&lastStats, plan, textBudget, vectorBudget, HybridCandidateBudgetPolicyAdaptiveRRF, HybridCandidateBudgetStopReasonExactBoundInsufficient, HybridCandidateBudgetStopReasonNone, uint64(iterations))
			return candidates, lastStats, err
		}

		if textBudget >= textRequested && vectorBudget >= vectorRequested {
			break
		}

		proofCandidates := hybridCandidateBudgetProofCandidates(candidates, filterAllowSet, plan)
		results, _, fuseErr := hybridFusePlannedCandidates(proofCandidates, plan)
		if fuseErr == nil {
			state := hybridCandidateBudgetState(plan, stats, textBudget, vectorBudget)
			if hybridCandidateBudgetExactRRFStop(proofCandidates, results, state, params, plan.topK) {
				hybridCandidateBudgetApplyAccumulatedWork(&stats, work)
				hybridCandidateBudgetFinalizeStats(&stats, plan, textBudget, vectorBudget, HybridCandidateBudgetPolicyAdaptiveRRF, HybridCandidateBudgetStopReasonExactRRFBound, HybridCandidateBudgetStopReasonNone, uint64(iterations))
				return candidates, stats, nil
			}
		}
		if iterations >= hybridAdaptiveCandidateBudgetMaxProbeIterations {
			textBudget = textRequested
			vectorBudget = vectorRequested
			continue
		}
		textBudget = hybridAdaptiveNextCandidateBudget(textBudget, textRequested)
		vectorBudget = hybridAdaptiveNextCandidateBudget(vectorBudget, vectorRequested)
	}

	hybridCandidateBudgetApplyAccumulatedWork(&lastStats, work)
	hybridCandidateBudgetFinalizeStats(&lastStats, plan, textRequested, vectorRequested, HybridCandidateBudgetPolicyFixed, HybridCandidateBudgetStopReasonRequestedBudgetReached, HybridCandidateBudgetStopReasonExactBoundInsufficient, uint64(iterations))
	return lastCandidates, lastStats, nil
}

func hybridCandidateBudgetRRFParamsForPlan(plan hybridSearchExecutionPlan) (hybridCandidateBudgetRRFParams, HybridCandidateBudgetStopReason, bool) {
	method := plan.fusion.Method
	if method == "" {
		method = HybridFusionMethodRRF
	}
	if method != HybridFusionMethodRRF && method != HybridFusionMethodWeightedRRF {
		return hybridCandidateBudgetRRFParams{}, HybridCandidateBudgetStopReasonUnsupportedFusion, false
	}
	rrfK := plan.fusion.RRFK
	if rrfK == 0 {
		rrfK = HybridFusionDefaultRRFK
	}
	if rrfK < 0 {
		return hybridCandidateBudgetRRFParams{}, HybridCandidateBudgetStopReasonUnsupportedFusion, false
	}
	weights := hybridFusionSourceWeights{text: 1, vector: 1}
	if method == HybridFusionMethodWeightedRRF {
		var err error
		weights, err = normalizeHybridFusionSourceWeights(plan.fusion)
		if err != nil {
			return hybridCandidateBudgetRRFParams{}, HybridCandidateBudgetStopReasonUnsupportedFusion, false
		}
	}
	return hybridCandidateBudgetRRFParams{rrfK: rrfK, textWeight: weights.text, vectorWeight: weights.vector}, HybridCandidateBudgetStopReasonNone, true
}

func hybridCandidateBudgetCanReduce(plan hybridSearchExecutionPlan) bool {
	if plan.text != nil && hybridAdaptiveInitialCandidateBudget(plan.text.CandidateLimit, plan.topK) < plan.text.CandidateLimit {
		return true
	}
	if plan.vector != nil && hybridAdaptiveInitialCandidateBudget(plan.vector.CandidateLimit, plan.topK) < plan.vector.CandidateLimit {
		return true
	}
	return false
}

func hybridCandidateBudgetSourceCount(plan hybridSearchExecutionPlan) int {
	count := 0
	if plan.text != nil {
		count++
	}
	if plan.vector != nil {
		count++
	}
	return count
}

func hybridCandidateBudgetRequestedText(plan hybridSearchExecutionPlan) int {
	if plan.text == nil {
		return 0
	}
	return plan.text.CandidateLimit
}

func hybridCandidateBudgetRequestedVector(plan hybridSearchExecutionPlan) int {
	if plan.vector == nil {
		return 0
	}
	return plan.vector.CandidateLimit
}

func hybridAdaptiveInitialCandidateBudget(requested, topK int) int {
	if requested <= 0 {
		return 0
	}
	budget := topK
	if budget < 1 {
		budget = 1
	}
	if budget > requested {
		budget = requested
	}
	return budget
}

func hybridAdaptiveNextCandidateBudget(current, requested int) int {
	if requested <= 0 || current >= requested {
		return requested
	}
	next := current * 2
	if next <= current {
		next = current + 1
	}
	if next > requested {
		next = requested
	}
	return next
}

func hybridSearchPlanWithCandidateBudgets(plan hybridSearchExecutionPlan, textBudget, vectorBudget int) hybridSearchExecutionPlan {
	out := plan
	if plan.text != nil {
		text := *plan.text
		text.CandidateLimit = textBudget
		out.text = &text
		out.public.TextCandidateLimit = textBudget
	}
	if plan.vector != nil {
		vector := *plan.vector
		vector.CandidateLimit = vectorBudget
		out.vector = &vector
		out.public.VectorCandidateLimit = vectorBudget
	}
	return out
}

func hybridCandidateBudgetState(plan hybridSearchExecutionPlan, stats HybridSearchStats, textBudget, vectorBudget int) [2]hybridCandidateBudgetSourceState {
	return [2]hybridCandidateBudgetSourceState{
		{
			present:   plan.text != nil,
			requested: hybridCandidateBudgetRequestedText(plan),
			effective: textBudget,
			returned:  int(stats.TextCandidatesReturned),
		},
		{
			present:   plan.vector != nil,
			requested: hybridCandidateBudgetRequestedVector(plan),
			effective: vectorBudget,
			returned:  int(stats.VectorCandidatesReturned),
		},
	}
}

func hybridCandidateBudgetProofCandidates(candidates []HybridSearchCandidate, allowSet hybridScalarAllowSet, plan hybridSearchExecutionPlan) []HybridSearchCandidate {
	if allowSet == nil || plan.scalarFilterStrategy == HybridScalarFilterStrategyPostfilter {
		return candidates
	}
	filtered := make([]HybridSearchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := allowSet[string(candidate.ID)]; ok {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func hybridCandidateBudgetExactRRFStop(candidates []HybridSearchCandidate, results []HybridSearchResult, state [2]hybridCandidateBudgetSourceState, params hybridCandidateBudgetRRFParams, topK int) bool {
	textState := state[0]
	vectorState := state[1]
	if len(results) == 0 {
		return !textState.canHaveUnseen() && !vectorState.canHaveUnseen()
	}
	if topK > 0 && len(results) < topK && (textState.canHaveUnseen() || vectorState.canHaveUnseen()) {
		return false
	}
	accumulators, ok := hybridCandidateBudgetProofAccumulators(candidates, params)
	if !ok {
		return false
	}
	resultIDs := make(map[string]struct{}, len(results))
	for _, result := range results {
		idKey := string(result.ID)
		acc, ok := accumulators[idKey]
		if !ok {
			return false
		}
		if textState.canHaveUnseen() && !acc.hasText {
			return false
		}
		if vectorState.canHaveUnseen() && !acc.hasVector {
			return false
		}
		resultIDs[idKey] = struct{}{}
	}
	threshold := results[len(results)-1].FusedScore
	for idKey, acc := range accumulators {
		if _, isResult := resultIDs[idKey]; isResult {
			continue
		}
		if hybridCandidateBudgetUpperBound(acc, textState, vectorState, params) >= threshold {
			return false
		}
	}
	if hybridCandidateBudgetUnseenUpperBound(textState, vectorState, params) >= threshold {
		return false
	}
	return true
}

func hybridCandidateBudgetProofAccumulators(candidates []HybridSearchCandidate, params hybridCandidateBudgetRRFParams) (map[string]hybridCandidateBudgetProofAccumulator, bool) {
	accumulators := make(map[string]hybridCandidateBudgetProofAccumulator, len(candidates))
	for _, candidate := range candidates {
		if candidate.SourceRank <= 0 {
			return nil, false
		}
		score, ok := params.contribution(candidate.Source, candidate.SourceRank)
		if !ok {
			return nil, false
		}
		idKey := string(candidate.ID)
		acc := accumulators[idKey]
		switch candidate.Source {
		case HybridCandidateSourceText:
			if !acc.hasText || score > acc.textScore {
				if acc.hasText {
					acc.fusedScore -= acc.textScore
				}
				acc.textScore = score
				acc.fusedScore += score
				acc.hasText = true
			}
		case HybridCandidateSourceVector:
			if !acc.hasVector || score > acc.vectorScore {
				if acc.hasVector {
					acc.fusedScore -= acc.vectorScore
				}
				acc.vectorScore = score
				acc.fusedScore += score
				acc.hasVector = true
			}
		default:
			return nil, false
		}
		accumulators[idKey] = acc
	}
	return accumulators, true
}

func (s hybridCandidateBudgetSourceState) canHaveUnseen() bool {
	return s.present && s.effective < s.requested && s.returned >= s.effective
}

func (p hybridCandidateBudgetRRFParams) contribution(source HybridCandidateSource, rank int) (float64, bool) {
	denominator := p.rrfK + rank
	if denominator <= 0 {
		return 0, false
	}
	switch source {
	case HybridCandidateSourceText:
		return p.textWeight / float64(denominator), true
	case HybridCandidateSourceVector:
		return p.vectorWeight / float64(denominator), true
	default:
		return 0, false
	}
}

func (p hybridCandidateBudgetRRFParams) nextContribution(source HybridCandidateSource, state hybridCandidateBudgetSourceState) float64 {
	if !state.canHaveUnseen() {
		return 0
	}
	score, ok := p.contribution(source, state.effective+1)
	if !ok {
		return 0
	}
	return score
}

func hybridCandidateBudgetUpperBound(acc hybridCandidateBudgetProofAccumulator, textState, vectorState hybridCandidateBudgetSourceState, params hybridCandidateBudgetRRFParams) float64 {
	upper := acc.fusedScore
	if !acc.hasText {
		upper += params.nextContribution(HybridCandidateSourceText, textState)
	}
	if !acc.hasVector {
		upper += params.nextContribution(HybridCandidateSourceVector, vectorState)
	}
	return upper
}

func hybridCandidateBudgetUnseenUpperBound(textState, vectorState hybridCandidateBudgetSourceState, params hybridCandidateBudgetRRFParams) float64 {
	return params.nextContribution(HybridCandidateSourceText, textState) + params.nextContribution(HybridCandidateSourceVector, vectorState)
}

func hybridCandidateBudgetFinalizeStats(stats *HybridSearchStats, plan hybridSearchExecutionPlan, textEffective, vectorEffective int, policy HybridCandidateBudgetPolicy, stopReason, fallbackReason HybridCandidateBudgetStopReason, iterations uint64) {
	if stats == nil {
		return
	}
	if plan.text != nil {
		stats.TextCandidatesRequested = uint64(plan.text.CandidateLimit)
		if textEffective > 0 {
			stats.TextCandidateBudgetEffective = uint64(textEffective)
		}
	} else {
		stats.TextCandidatesRequested = 0
		stats.TextCandidateBudgetEffective = 0
	}
	if plan.vector != nil {
		stats.VectorCandidatesRequested = uint64(plan.vector.CandidateLimit)
		if vectorEffective > 0 {
			stats.VectorCandidateBudgetEffective = uint64(vectorEffective)
		}
	} else {
		stats.VectorCandidatesRequested = 0
		stats.VectorCandidateBudgetEffective = 0
	}
	if policy == "" {
		policy = HybridCandidateBudgetPolicyFixed
	}
	if stopReason == "" {
		stopReason = HybridCandidateBudgetStopReasonNone
	}
	stats.CandidateBudgetPolicy = policy
	stats.CandidateBudgetStopReason = stopReason
	if fallbackReason != "" && fallbackReason != HybridCandidateBudgetStopReasonNone {
		stats.CandidateBudgetFallbacks = 1
		stats.CandidateBudgetFallbackReason = fallbackReason
	}
	if iterations == 0 {
		iterations = 1
	}
	stats.CandidateBudgetIterations = iterations
}

func hybridCandidateBudgetAccumulateAttemptWork(dst *HybridSearchStats, src HybridSearchStats) {
	if dst == nil {
		return
	}
	dst.TextPostingsScanned += src.TextPostingsScanned
	dst.TextPostingBlocksVisited += src.TextPostingBlocksVisited
	dst.TextPostingBlocksSkipped += src.TextPostingBlocksSkipped
	dst.TextBlockMaxFallbacks += src.TextBlockMaxFallbacks
	dst.TextBlockMaxThresholds += src.TextBlockMaxThresholds
	dst.TextWANDPivots += src.TextWANDPivots
	dst.TextScalarPostingBlocksSkipped += src.TextScalarPostingBlocksSkipped
	dst.TextScalarPostingsRejected += src.TextScalarPostingsRejected
	dst.TextCandidatesScored += src.TextCandidatesScored
	dst.TextStateLookups += src.TextStateLookups
	dst.TextNormLookups += src.TextNormLookups
	dst.TextMatchDetailsBuilt += src.TextMatchDetailsBuilt
	dst.TextPositionLookups += src.TextPositionLookups
	dst.TextPhraseCandidatesChecked += src.TextPhraseCandidatesChecked
	dst.TextPhraseCandidatesMatched += src.TextPhraseCandidatesMatched
	dst.VectorCandidatesExamined += src.VectorCandidatesExamined
	dst.VectorEdgesVisited += src.VectorEdgesVisited
	dst.ScalarFilterRejected += src.ScalarFilterRejected
	dst.DocumentsFetched += src.DocumentsFetched
	dst.DocumentsMissing += src.DocumentsMissing
	dst.FullDocumentScanFallbacks += src.FullDocumentScanFallbacks
}

func hybridCandidateBudgetApplyAccumulatedWork(stats *HybridSearchStats, work HybridSearchStats) {
	if stats == nil {
		return
	}
	stats.TextPostingsScanned = work.TextPostingsScanned
	stats.TextPostingBlocksVisited = work.TextPostingBlocksVisited
	stats.TextPostingBlocksSkipped = work.TextPostingBlocksSkipped
	stats.TextBlockMaxFallbacks = work.TextBlockMaxFallbacks
	stats.TextBlockMaxThresholds = work.TextBlockMaxThresholds
	stats.TextWANDPivots = work.TextWANDPivots
	stats.TextScalarPostingBlocksSkipped = work.TextScalarPostingBlocksSkipped
	stats.TextScalarPostingsRejected = work.TextScalarPostingsRejected
	stats.TextCandidatesScored = work.TextCandidatesScored
	stats.TextStateLookups = work.TextStateLookups
	stats.TextNormLookups = work.TextNormLookups
	stats.TextMatchDetailsBuilt = work.TextMatchDetailsBuilt
	stats.TextPositionLookups = work.TextPositionLookups
	stats.TextPhraseCandidatesChecked = work.TextPhraseCandidatesChecked
	stats.TextPhraseCandidatesMatched = work.TextPhraseCandidatesMatched
	stats.VectorCandidatesExamined = work.VectorCandidatesExamined
	stats.VectorEdgesVisited = work.VectorEdgesVisited
	stats.ScalarFilterRejected = work.ScalarFilterRejected
	stats.DocumentsFetched = work.DocumentsFetched
	stats.DocumentsMissing = work.DocumentsMissing
	stats.FullDocumentScanFallbacks = work.FullDocumentScanFallbacks
}
