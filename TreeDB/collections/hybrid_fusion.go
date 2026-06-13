package collections

import (
	"fmt"
	"math"
	"sort"
)

// HybridFusionDefaultRRFK is the contract default RRF denominator offset used
// when HybridFusionOptions.RRFK is zero.
const HybridFusionDefaultRRFK = 60

// FuseHybridSearchCandidates deterministically fuses already-generated hybrid
// text/vector candidates into a bounded final result slice. The helper does not
// run text or vector searches, open storage, apply scalar filters, or fetch
// documents; callers must pass the already-bounded candidate lists they want to
// fuse.
//
// The zero-value fusion method and tie policy use the #2502/#2729 contract
// defaults: reciprocal-rank fusion and fused_score_best_rank_source_order_id.
// For RRF methods, RRFK=0 means HybridFusionDefaultRRFK. Normalized-score fusion
// is exact over the supplied candidates only. topK bounds only the returned
// result slice; counters still describe the full supplied candidate set.
func FuseHybridSearchCandidates(candidates []HybridSearchCandidate, fusion HybridFusionOptions, topK int) ([]HybridSearchResult, HybridSearchStats, error) {
	method := fusion.Method
	if method == "" {
		method = HybridFusionMethodRRF
	}
	if method != HybridFusionMethodRRF && method != HybridFusionMethodWeightedRRF && method != HybridFusionMethodNormalizedScore {
		return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid fusion method %q", ErrHybridSearchUnsupported, method)
	}

	tiePolicy := fusion.TiePolicy
	if tiePolicy == "" {
		tiePolicy = HybridFusionTiePolicyScoreBestRankSourceID
	}
	if tiePolicy != HybridFusionTiePolicyScoreBestRankSourceID {
		return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid fusion tie policy %q", ErrHybridSearchUnsupported, tiePolicy)
	}

	rrfK := fusion.RRFK
	if rrfK == 0 {
		rrfK = HybridFusionDefaultRRFK
	}
	if rrfK < 0 {
		return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid fusion rrf_k must be non-negative", ErrHybridSearchUnsupported)
	}

	sourceOrder, err := normalizeHybridFusionSourceOrder(fusion.SourceOrder)
	if err != nil {
		return nil, HybridSearchStats{}, err
	}
	weights := hybridFusionSourceWeights{text: 1, vector: 1}
	if method == HybridFusionMethodWeightedRRF || method == HybridFusionMethodNormalizedScore {
		var err error
		weights, err = normalizeHybridFusionSourceWeights(fusion)
		if err != nil {
			return nil, HybridSearchStats{}, err
		}
	}
	scoreRanges, err := hybridFusionScoreRanges(candidates, method, sourceOrder)
	if err != nil {
		return nil, HybridSearchStats{}, err
	}

	stats := HybridSearchStats{CandidatesFused: uint64(len(candidates))}
	if len(candidates) == 0 {
		return nil, stats, nil
	}
	accByID := make(map[string]int, len(candidates))
	accumulators := make([]hybridFusionAccumulator, 0, len(candidates))

	for _, candidate := range candidates {
		if candidate.SourceRank <= 0 {
			return nil, HybridSearchStats{}, fmt.Errorf("%w: hybrid candidate source_rank must be one-based", ErrHybridSearchUnsupported)
		}
		if _, err := hybridFusionSourceRank(candidate.Source, sourceOrder); err != nil {
			return nil, HybridSearchStats{}, err
		}
		contribution, err := hybridSourceContributionFromCandidate(candidate, method, rrfK, weights, scoreRanges)
		if err != nil {
			return nil, HybridSearchStats{}, err
		}

		idx, ok := accByID[string(candidate.ID)]
		if !ok {
			idKey := string(candidate.ID)
			idx = len(accumulators)
			accByID[idKey] = idx
			accumulators = append(accumulators, hybridFusionAccumulator{idKey: idKey})
		}

		if accumulators[idx].add(contribution) {
			stats.FusionDuplicateCandidates++
		}
	}

	stats.CandidatesAfterFusion = uint64(len(accumulators))
	for i := range accumulators {
		accumulators[i].finalize(sourceOrder)
		switch {
		case accumulators[i].hasText && accumulators[i].hasVector:
			stats.FusionBoth++
		case accumulators[i].hasText:
			stats.FusionTextOnly++
		case accumulators[i].hasVector:
			stats.FusionVectorOnly++
		}
	}

	sort.Slice(accumulators, func(i, j int) bool {
		return hybridFusionAccumulatorLess(accumulators[i], accumulators[j])
	})

	limit := topK
	if limit < 0 {
		limit = 0
	}
	if limit > len(accumulators) {
		limit = len(accumulators)
	}
	if limit < len(accumulators) {
		stats.Truncated = uint64(len(accumulators) - limit)
	}
	if limit == 0 {
		return nil, stats, nil
	}

	results := make([]HybridSearchResult, limit)
	for i := 0; i < limit; i++ {
		acc := accumulators[i]
		sources := acc.sources(sourceOrder)
		results[i] = HybridSearchResult{
			ID:         []byte(acc.idKey),
			Rank:       i + 1,
			FusedScore: acc.fusedScore,
			Sources:    sources,
		}
	}
	return results, stats, nil
}

type hybridFusionAccumulator struct {
	idKey string

	text      HybridSourceContribution
	hasText   bool
	vector    HybridSourceContribution
	hasVector bool

	fusedScore        float64
	bestRank          int
	contributionCount int
	bestSourceOrder   int
}

// add returns true when the candidate was a duplicate for the same exact
// document ID and source. Same-source duplicates keep the best source-rank
// contribution and never double-count RRF score.
func (acc *hybridFusionAccumulator) add(contribution HybridSourceContribution) bool {
	switch contribution.Source {
	case HybridCandidateSourceText:
		if acc.hasText {
			if hybridSourceContributionPreferred(contribution, acc.text) {
				acc.text = contribution
			}
			return true
		}
		acc.text = contribution
		acc.hasText = true
		return false
	case HybridCandidateSourceVector:
		if acc.hasVector {
			if hybridSourceContributionPreferred(contribution, acc.vector) {
				acc.vector = contribution
			}
			return true
		}
		acc.vector = contribution
		acc.hasVector = true
		return false
	default:
		// Source validation happens before add. Keep the default branch as a guard
		// for future call-site changes.
		return false
	}
}

func (acc *hybridFusionAccumulator) finalize(sourceOrder hybridFusionSourceOrder) {
	acc.fusedScore = 0
	acc.bestRank = 0
	acc.contributionCount = 0
	acc.bestSourceOrder = hybridFusionSourceOrderMissing
	if acc.hasText {
		acc.includeContribution(acc.text, sourceOrder)
	}
	if acc.hasVector {
		acc.includeContribution(acc.vector, sourceOrder)
	}
}

func (acc *hybridFusionAccumulator) includeContribution(contribution HybridSourceContribution, sourceOrder hybridFusionSourceOrder) {
	acc.fusedScore += contribution.FusionScore
	acc.contributionCount++

	contributionSourceOrder := hybridFusionSourceOrderMissing
	if rank, err := hybridFusionSourceRank(contribution.Source, sourceOrder); err == nil {
		contributionSourceOrder = rank
	}
	if acc.bestRank == 0 || contribution.SourceRank < acc.bestRank {
		acc.bestRank = contribution.SourceRank
		acc.bestSourceOrder = contributionSourceOrder
		return
	}
	if contribution.SourceRank == acc.bestRank && contributionSourceOrder < acc.bestSourceOrder {
		acc.bestSourceOrder = contributionSourceOrder
	}
}

func (acc hybridFusionAccumulator) sources(sourceOrder hybridFusionSourceOrder) []HybridSourceContribution {
	if acc.contributionCount == 0 {
		return nil
	}
	if acc.hasText && acc.hasVector {
		text := cloneHybridSourceContribution(acc.text)
		vector := cloneHybridSourceContribution(acc.vector)
		if sourceOrder.vectorRank < sourceOrder.textRank {
			return []HybridSourceContribution{vector, text}
		}
		return []HybridSourceContribution{text, vector}
	}
	if acc.hasText {
		return []HybridSourceContribution{cloneHybridSourceContribution(acc.text)}
	}
	return []HybridSourceContribution{cloneHybridSourceContribution(acc.vector)}
}

func hybridSourceContributionFromCandidate(candidate HybridSearchCandidate, method HybridFusionMethod, rrfK int, weights hybridFusionSourceWeights, ranges hybridFusionScoreRangesBySource) (HybridSourceContribution, error) {
	fusionScore, err := hybridFusionContributionScore(candidate, method, rrfK, weights, ranges)
	if err != nil {
		return HybridSourceContribution{}, err
	}
	return HybridSourceContribution{
		Source:      candidate.Source,
		IndexName:   candidate.IndexName,
		SourceRank:  candidate.SourceRank,
		Score:       candidate.Score,
		ScoreKind:   candidate.ScoreKind,
		FusionScore: fusionScore,
		TextMatches: candidate.TextMatches,
	}, nil
}

func hybridFusionContributionScore(candidate HybridSearchCandidate, method HybridFusionMethod, rrfK int, weights hybridFusionSourceWeights, ranges hybridFusionScoreRangesBySource) (float64, error) {
	switch method {
	case HybridFusionMethodRRF:
		denominator := rrfK + candidate.SourceRank
		if denominator <= 0 {
			return 0, fmt.Errorf("%w: hybrid fusion rrf denominator overflow", ErrHybridSearchUnsupported)
		}
		return 1 / float64(denominator), nil
	case HybridFusionMethodWeightedRRF:
		weight, err := weights.weight(candidate.Source)
		if err != nil {
			return 0, err
		}
		denominator := rrfK + candidate.SourceRank
		if denominator <= 0 {
			return 0, fmt.Errorf("%w: hybrid fusion rrf denominator overflow", ErrHybridSearchUnsupported)
		}
		return weight / float64(denominator), nil
	case HybridFusionMethodNormalizedScore:
		weight, err := weights.weight(candidate.Source)
		if err != nil {
			return 0, err
		}
		if math.IsNaN(candidate.Score) || math.IsInf(candidate.Score, 0) {
			return 0, fmt.Errorf("%w: hybrid normalized-score fusion requires finite source scores", ErrHybridSearchUnsupported)
		}
		normalized, err := ranges.normalized(candidate.Source, candidate.Score)
		if err != nil {
			return 0, err
		}
		return weight * normalized, nil
	default:
		return 0, fmt.Errorf("%w: hybrid fusion method %q", ErrHybridSearchUnsupported, method)
	}
}

func hybridSourceContributionPreferred(candidate, existing HybridSourceContribution) bool {
	if candidate.SourceRank != existing.SourceRank {
		return candidate.SourceRank < existing.SourceRank
	}
	candidateScoreNaN := math.IsNaN(candidate.Score)
	existingScoreNaN := math.IsNaN(existing.Score)
	if candidateScoreNaN != existingScoreNaN {
		return !candidateScoreNaN
	}
	if !candidateScoreNaN && candidate.Score != existing.Score {
		return candidate.Score > existing.Score
	}
	if candidate.IndexName != existing.IndexName {
		return candidate.IndexName < existing.IndexName
	}
	return string(candidate.ScoreKind) < string(existing.ScoreKind)
}

func hybridFusionAccumulatorLess(a, b hybridFusionAccumulator) bool {
	if a.fusedScore != b.fusedScore {
		return a.fusedScore > b.fusedScore
	}
	if a.bestRank != b.bestRank {
		return a.bestRank < b.bestRank
	}
	if a.contributionCount != b.contributionCount {
		return a.contributionCount > b.contributionCount
	}
	if a.bestSourceOrder != b.bestSourceOrder {
		return a.bestSourceOrder < b.bestSourceOrder
	}
	return a.idKey < b.idKey
}

type hybridFusionSourceOrder struct {
	textRank   int
	vectorRank int
}

const hybridFusionSourceOrderMissing = 1 << 30

func normalizeHybridFusionSourceOrder(sourceOrder []HybridCandidateSource) (hybridFusionSourceOrder, error) {
	order := hybridFusionSourceOrder{textRank: hybridFusionSourceOrderMissing, vectorRank: hybridFusionSourceOrderMissing}
	nextRank := 0
	assign := func(source HybridCandidateSource) error {
		switch source {
		case "":
			return nil
		case HybridCandidateSourceText:
			if order.textRank == hybridFusionSourceOrderMissing {
				order.textRank = nextRank
				nextRank++
			}
			return nil
		case HybridCandidateSourceVector:
			if order.vectorRank == hybridFusionSourceOrderMissing {
				order.vectorRank = nextRank
				nextRank++
			}
			return nil
		default:
			return fmt.Errorf("%w: hybrid fusion source order contains source %q", ErrHybridSearchUnsupported, source)
		}
	}
	for _, source := range sourceOrder {
		if err := assign(source); err != nil {
			return hybridFusionSourceOrder{}, err
		}
	}
	if err := assign(HybridCandidateSourceText); err != nil {
		return hybridFusionSourceOrder{}, err
	}
	if err := assign(HybridCandidateSourceVector); err != nil {
		return hybridFusionSourceOrder{}, err
	}
	return order, nil
}

func hybridFusionSourceRank(source HybridCandidateSource, sourceOrder hybridFusionSourceOrder) (int, error) {
	switch source {
	case HybridCandidateSourceText:
		return sourceOrder.textRank, nil
	case HybridCandidateSourceVector:
		return sourceOrder.vectorRank, nil
	default:
		return 0, fmt.Errorf("%w: hybrid candidate source %q", ErrHybridSearchUnsupported, source)
	}
}

type hybridFusionSourceWeights struct {
	text   float64
	vector float64
}

func normalizeHybridFusionSourceWeights(fusion HybridFusionOptions) (hybridFusionSourceWeights, error) {
	weights := hybridFusionSourceWeights{text: fusion.TextWeight, vector: fusion.VectorWeight}
	if weights.text == 0 {
		weights.text = 1
	}
	if weights.vector == 0 {
		weights.vector = 1
	}
	if !hybridFusionWeightOK(weights.text) || !hybridFusionWeightOK(weights.vector) {
		return hybridFusionSourceWeights{}, fmt.Errorf("%w: hybrid fusion source weights must be finite and non-negative", ErrHybridSearchUnsupported)
	}
	return weights, nil
}

func hybridFusionWeightOK(weight float64) bool {
	return weight >= 0 && !math.IsNaN(weight) && !math.IsInf(weight, 0)
}

func (w hybridFusionSourceWeights) weight(source HybridCandidateSource) (float64, error) {
	switch source {
	case HybridCandidateSourceText:
		return w.text, nil
	case HybridCandidateSourceVector:
		return w.vector, nil
	default:
		return 0, fmt.Errorf("%w: hybrid candidate source %q", ErrHybridSearchUnsupported, source)
	}
}

type hybridFusionScoreRange struct {
	min  float64
	max  float64
	seen bool
}

type hybridFusionScoreRangesBySource struct {
	text   hybridFusionScoreRange
	vector hybridFusionScoreRange
}

func hybridFusionScoreRanges(candidates []HybridSearchCandidate, method HybridFusionMethod, sourceOrder hybridFusionSourceOrder) (hybridFusionScoreRangesBySource, error) {
	var ranges hybridFusionScoreRangesBySource
	for _, candidate := range candidates {
		if candidate.SourceRank <= 0 {
			return ranges, fmt.Errorf("%w: hybrid candidate source_rank must be one-based", ErrHybridSearchUnsupported)
		}
		if _, err := hybridFusionSourceRank(candidate.Source, sourceOrder); err != nil {
			return ranges, err
		}
		if method != HybridFusionMethodNormalizedScore {
			continue
		}
		if math.IsNaN(candidate.Score) || math.IsInf(candidate.Score, 0) {
			return ranges, fmt.Errorf("%w: hybrid normalized-score fusion requires finite source scores", ErrHybridSearchUnsupported)
		}
		rangeForSource := ranges.rangeForSource(candidate.Source)
		if rangeForSource == nil {
			return ranges, fmt.Errorf("%w: hybrid candidate source %q", ErrHybridSearchUnsupported, candidate.Source)
		}
		rangeForSource.include(candidate.Score)
	}
	return ranges, nil
}

func (r *hybridFusionScoreRangesBySource) rangeForSource(source HybridCandidateSource) *hybridFusionScoreRange {
	switch source {
	case HybridCandidateSourceText:
		return &r.text
	case HybridCandidateSourceVector:
		return &r.vector
	default:
		return nil
	}
}

func (r *hybridFusionScoreRange) include(score float64) {
	if !r.seen {
		r.min = score
		r.max = score
		r.seen = true
		return
	}
	if score < r.min {
		r.min = score
	}
	if score > r.max {
		r.max = score
	}
}

func (r hybridFusionScoreRangesBySource) normalized(source HybridCandidateSource, score float64) (float64, error) {
	var scoreRange hybridFusionScoreRange
	switch source {
	case HybridCandidateSourceText:
		scoreRange = r.text
	case HybridCandidateSourceVector:
		scoreRange = r.vector
	default:
		return 0, fmt.Errorf("%w: hybrid candidate source %q", ErrHybridSearchUnsupported, source)
	}
	if !scoreRange.seen {
		return 0, fmt.Errorf("%w: hybrid normalized-score fusion missing source range for %q", ErrHybridSearchUnsupported, source)
	}
	span := scoreRange.max - scoreRange.min
	if span == 0 {
		return 1, nil
	}
	return (score - scoreRange.min) / span, nil
}

func cloneHybridSourceContribution(in HybridSourceContribution) HybridSourceContribution {
	out := in
	out.TextMatches = cloneHybridTextMatches(in.TextMatches)
	return out
}

func cloneHybridTextMatches(in []HybridTextMatch) []HybridTextMatch {
	if len(in) == 0 {
		return nil
	}
	out := make([]HybridTextMatch, len(in))
	for i := range in {
		out[i].Field = in[i].Field
		if len(in[i].Terms) > 0 {
			out[i].Terms = append([]string(nil), in[i].Terms...)
		}
	}
	return out
}
