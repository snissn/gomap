package collections

import (
	"errors"
	"math"
	"testing"
)

func TestHybridRRFFusionTextOnlyVectorOnlyAndOverlap(t *testing.T) {
	candidates := []HybridSearchCandidate{
		hybridFusionCandidate("doc-text-1", HybridCandidateSourceText, 1, 12, HybridScoreKindBM25),
		hybridFusionCandidate("doc-shared", HybridCandidateSourceText, 2, 9, HybridScoreKindBM25),
		hybridFusionCandidate("doc-vector-1", HybridCandidateSourceVector, 1, 0.95, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("doc-shared", HybridCandidateSourceVector, 2, 0.90, HybridScoreKindVectorSimilarity),
	}

	results, stats, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, 10)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results)=%d want 3", len(results))
	}
	if got, want := string(results[0].ID), "doc-shared"; got != want {
		t.Fatalf("top result id=%q want %q; results=%+v", got, want, results)
	}
	wantSharedScore := 1.0/62.0 + 1.0/62.0
	if !hybridFloatClose(results[0].FusedScore, wantSharedScore) {
		t.Fatalf("shared fused score=%g want %g", results[0].FusedScore, wantSharedScore)
	}
	if len(results[0].Sources) != 2 || results[0].Sources[0].Source != HybridCandidateSourceText || results[0].Sources[1].Source != HybridCandidateSourceVector {
		t.Fatalf("shared sources=%+v want text then vector contributions", results[0].Sources)
	}
	if got, want := results[1].FusedScore, 1.0/61.0; !hybridFloatClose(got, want) {
		t.Fatalf("single-source fused score=%g want %g", got, want)
	}
	if got := []string{string(results[1].ID), string(results[2].ID)}; got[0] != "doc-text-1" || got[1] != "doc-vector-1" {
		t.Fatalf("single-source tie order=%v want text-only before vector-only", got)
	}
	if stats.CandidatesFused != 4 || stats.CandidatesAfterFusion != 3 || stats.FusionTextOnly != 1 || stats.FusionVectorOnly != 1 || stats.FusionBoth != 1 || stats.Truncated != 0 {
		t.Fatalf("stats=%+v want fused=4 unique=3 text-only=1 vector-only=1 both=1 truncated=0", stats)
	}
	for i, result := range results {
		if result.Rank != i+1 {
			t.Fatalf("result[%d].Rank=%d want %d", i, result.Rank, i+1)
		}
	}
}

func TestHybridRRFFusionVectorOnlyUsesRanks(t *testing.T) {
	candidates := []HybridSearchCandidate{
		hybridFusionCandidate("v3", HybridCandidateSourceVector, 3, 0.99, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("v1", HybridCandidateSourceVector, 1, 0.80, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("v2", HybridCandidateSourceVector, 2, 0.70, HybridScoreKindVectorSimilarity),
	}

	results, stats, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{RRFK: 10}, 3)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates: %v", err)
	}
	gotIDs := []string{string(results[0].ID), string(results[1].ID), string(results[2].ID)}
	wantIDs := []string{"v1", "v2", "v3"}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("ids=%v want %v", gotIDs, wantIDs)
		}
	}
	if got, want := results[0].FusedScore, 1.0/11.0; !hybridFloatClose(got, want) {
		t.Fatalf("rank-1 score=%g want %g", got, want)
	}
	if stats.FusionVectorOnly != 3 || stats.FusionTextOnly != 0 || stats.FusionBoth != 0 {
		t.Fatalf("stats=%+v want three vector-only returned results", stats)
	}
}

func TestHybridRRFIgnoresWeights2729(t *testing.T) {
	candidates := []HybridSearchCandidate{
		hybridFusionCandidate("text", HybridCandidateSourceText, 1, 10, HybridScoreKindBM25),
		hybridFusionCandidate("vector", HybridCandidateSourceVector, 1, 0.9, HybridScoreKindVectorSimilarity),
	}

	results, _, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{Method: HybridFusionMethodRRF, TextWeight: 0.01, VectorWeight: 99}, 2)
	if err != nil {
		t.Fatalf("RRF with weights: %v", err)
	}
	if len(results) != 2 || string(results[0].ID) != "text" || string(results[1].ID) != "vector" {
		t.Fatalf("RRF weighted-input results=%+v want source-order tie, not vector-weighted ordering", results)
	}
	wantScore := 1.0 / float64(HybridFusionDefaultRRFK+1)
	if !hybridFloatClose(results[0].FusedScore, wantScore) || !hybridFloatClose(results[1].FusedScore, wantScore) {
		t.Fatalf("RRF scores=%g/%g want unweighted %g", results[0].FusedScore, results[1].FusedScore, wantScore)
	}
	if _, _, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{Method: HybridFusionMethodRRF, TextWeight: math.NaN()}, 1); err != nil {
		t.Fatalf("RRF should ignore invalid weight fields: %v", err)
	}
}

func TestHybridFusionWeightedAndNormalizedScores(t *testing.T) {
	candidates := []HybridSearchCandidate{
		hybridFusionCandidate("text-1", HybridCandidateSourceText, 1, 10, HybridScoreKindBM25),
		hybridFusionCandidate("text-2", HybridCandidateSourceText, 2, 5, HybridScoreKindBM25),
		hybridFusionCandidate("vector-1", HybridCandidateSourceVector, 1, 0.7, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("shared", HybridCandidateSourceText, 3, 1, HybridScoreKindBM25),
		hybridFusionCandidate("shared", HybridCandidateSourceVector, 2, 0.9, HybridScoreKindVectorSimilarity),
	}

	weighted, _, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{Method: HybridFusionMethodWeightedRRF, TextWeight: 0.5, VectorWeight: 2}, 10)
	if err != nil {
		t.Fatalf("weighted FuseHybridSearchCandidates: %v", err)
	}
	if got := string(weighted[0].ID); got != "shared" {
		t.Fatalf("weighted top=%q want shared overlap from weighted text+vector contributions; results=%+v", got, weighted)
	}
	if got, want := weighted[0].FusedScore, 0.5/63.0+2.0/62.0; !hybridFloatClose(got, want) {
		t.Fatalf("weighted score=%g want %g", got, want)
	}

	normalized, _, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{Method: HybridFusionMethodNormalizedScore, TextWeight: 1, VectorWeight: 1}, 10)
	if err != nil {
		t.Fatalf("normalized FuseHybridSearchCandidates: %v", err)
	}
	if got := string(normalized[0].ID); got != "text-1" {
		t.Fatalf("normalized top=%q want text-1; results=%+v", got, normalized)
	}
	var shared HybridSearchResult
	for _, result := range normalized {
		if string(result.ID) == "shared" {
			shared = result
			break
		}
	}
	if len(shared.Sources) != 2 {
		t.Fatalf("normalized shared sources=%+v want text+vector", shared.Sources)
	}
	if got, want := shared.FusedScore, 1.0; !hybridFloatClose(got, want) {
		t.Fatalf("normalized shared fused=%g want vector max contribution 1 and text min contribution 0", got)
	}

	_, _, err = FuseHybridSearchCandidates([]HybridSearchCandidate{hybridFusionCandidate("bad", HybridCandidateSourceText, 1, math.NaN(), HybridScoreKindBM25)}, HybridFusionOptions{Method: HybridFusionMethodNormalizedScore}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("normalized NaN err=%v want ErrHybridSearchUnsupported", err)
	}
	equalScores, _, err := FuseHybridSearchCandidates([]HybridSearchCandidate{
		hybridFusionCandidate("a", HybridCandidateSourceText, 2, 5, HybridScoreKindBM25),
		hybridFusionCandidate("b", HybridCandidateSourceText, 1, 5, HybridScoreKindBM25),
	}, HybridFusionOptions{Method: HybridFusionMethodNormalizedScore}, 10)
	if err != nil {
		t.Fatalf("normalized equal-score FuseHybridSearchCandidates: %v", err)
	}
	if len(equalScores) != 2 || string(equalScores[0].ID) != "b" || !hybridFloatClose(equalScores[0].FusedScore, 1) || !hybridFloatClose(equalScores[1].FusedScore, 1) {
		t.Fatalf("equal-score normalized results=%+v want span-zero contributions of 1 ordered by source rank", equalScores)
	}

	_, _, err = FuseHybridSearchCandidates(candidates, HybridFusionOptions{Method: HybridFusionMethodWeightedRRF, TextWeight: math.Inf(1)}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("bad weight err=%v want ErrHybridSearchUnsupported", err)
	}
}

func TestHybridRRFFusionDeduplicatesExactIDsAndPreservesAttribution(t *testing.T) {
	candidates := []HybridSearchCandidate{
		{
			ID:         []byte("same"),
			Source:     HybridCandidateSourceText,
			IndexName:  "body",
			SourceRank: 2,
			Score:      4.2,
			ScoreKind:  HybridScoreKindBM25F,
			TextMatches: []HybridTextMatch{{
				Field: "body",
				Terms: []string{"tree", "db"},
			}},
		},
		// Same exact document ID and same source should not double-count; the better
		// source rank remains the text contribution.
		{
			ID:         []byte("same"),
			Source:     HybridCandidateSourceText,
			IndexName:  "body",
			SourceRank: 5,
			Score:      99,
			ScoreKind:  HybridScoreKindBM25F,
		},
		hybridFusionCandidate("same", HybridCandidateSourceVector, 3, 0.75, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("missing-text-side", HybridCandidateSourceVector, 1, 0.9, HybridScoreKindVectorSimilarity),
	}

	results, stats, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, 10)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates: %v", err)
	}
	var shared *HybridSearchResult
	for i := range results {
		if string(results[i].ID) == "same" {
			shared = &results[i]
		}
	}
	if shared == nil {
		t.Fatalf("shared result missing: %+v", results)
	}
	if len(shared.Sources) != 2 {
		t.Fatalf("shared sources=%+v want exactly text and vector", shared.Sources)
	}
	if shared.Sources[0].Source != HybridCandidateSourceText || shared.Sources[0].SourceRank != 2 || shared.Sources[0].Score != 4.2 || shared.Sources[0].ScoreKind != HybridScoreKindBM25F {
		t.Fatalf("text contribution=%+v want best text duplicate with attribution", shared.Sources[0])
	}
	if shared.Sources[1].Source != HybridCandidateSourceVector || shared.Sources[1].SourceRank != 3 {
		t.Fatalf("vector contribution=%+v want vector rank 3", shared.Sources[1])
	}
	wantScore := 1.0/62.0 + 1.0/63.0
	if !hybridFloatClose(shared.FusedScore, wantScore) {
		t.Fatalf("shared fused score=%g want %g", shared.FusedScore, wantScore)
	}
	if len(shared.Sources[0].TextMatches) != 1 || shared.Sources[0].TextMatches[0].Terms[0] != "tree" {
		t.Fatalf("text matches=%+v want preserved terms", shared.Sources[0].TextMatches)
	}

	candidates[0].ID[0] = 'X'
	candidates[0].TextMatches[0].Terms[0] = "mutated"
	if got := string(shared.ID); got != "same" {
		t.Fatalf("result ID mutated to %q; want response-owned copy", got)
	}
	if got := shared.Sources[0].TextMatches[0].Terms[0]; got != "tree" {
		t.Fatalf("text match term mutated to %q; want response-owned copy", got)
	}
	if stats.CandidatesFused != 4 || stats.CandidatesAfterFusion != 2 || stats.FusionDuplicateCandidates != 1 || stats.FusionBoth != 1 || stats.FusionVectorOnly != 1 {
		t.Fatalf("stats=%+v want input, dedup, both, and vector-only counters", stats)
	}
}

func TestHybridRRFFusionStableTiesSourceOrderAndIDBytes(t *testing.T) {
	candidates := []HybridSearchCandidate{
		hybridFusionCandidate("vector-doc", HybridCandidateSourceVector, 1, 1, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("text-doc", HybridCandidateSourceText, 1, 1, HybridScoreKindBM25),
		hybridFusionCandidate("b", HybridCandidateSourceText, 5, 1, HybridScoreKindBM25),
		hybridFusionCandidate("a", HybridCandidateSourceText, 5, 1, HybridScoreKindBM25),
	}

	results, _, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, 10)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates default order: %v", err)
	}
	if got := []string{string(results[0].ID), string(results[1].ID), string(results[2].ID), string(results[3].ID)}; got[0] != "text-doc" || got[1] != "vector-doc" || got[2] != "a" || got[3] != "b" {
		t.Fatalf("default tie order=%v want text-doc, vector-doc, a, b", got)
	}

	results, _, err = FuseHybridSearchCandidates(candidates, HybridFusionOptions{SourceOrder: []HybridCandidateSource{HybridCandidateSourceVector, HybridCandidateSourceText}}, 10)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates custom order: %v", err)
	}
	if got := []string{string(results[0].ID), string(results[1].ID)}; got[0] != "vector-doc" || got[1] != "text-doc" {
		t.Fatalf("custom source order tie=%v want vector-doc before text-doc", got)
	}

	byteTieCandidates := []HybridSearchCandidate{
		{ID: []byte{0x01}, Source: HybridCandidateSourceText, SourceRank: 7, Score: 1, ScoreKind: HybridScoreKindBM25},
		{ID: []byte{0x00, 0xff}, Source: HybridCandidateSourceText, SourceRank: 7, Score: 1, ScoreKind: HybridScoreKindBM25},
	}
	results, _, err = FuseHybridSearchCandidates(byteTieCandidates, HybridFusionOptions{}, 10)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates byte ties: %v", err)
	}
	if got := results[0].ID; len(got) != 2 || got[0] != 0x00 || got[1] != 0xff {
		t.Fatalf("byte tie winner=%v want lexicographically smallest opaque bytes", got)
	}
}

func TestHybridRRFFusionTieUsesBestRankedContributionSourceOrder(t *testing.T) {
	candidates := []HybridSearchCandidate{
		// The two documents have equal fused score, equal best rank, and equal
		// contributing-source count. The source-order tie breaker must use the
		// source that supplied the best-ranked contribution, not just the earliest
		// source among all contributions.
		hybridFusionCandidate("a-vector-best", HybridCandidateSourceVector, 1, 1, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("a-vector-best", HybridCandidateSourceText, 2, 1, HybridScoreKindBM25),
		hybridFusionCandidate("z-text-best", HybridCandidateSourceText, 1, 1, HybridScoreKindBM25),
		hybridFusionCandidate("z-text-best", HybridCandidateSourceVector, 2, 1, HybridScoreKindVectorSimilarity),
	}

	results, _, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, 2)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates default source order: %v", err)
	}
	if got := []string{string(results[0].ID), string(results[1].ID)}; got[0] != "z-text-best" || got[1] != "a-vector-best" {
		t.Fatalf("default best-ranked source tie order=%v want text-best before vector-best", got)
	}

	results, _, err = FuseHybridSearchCandidates(candidates, HybridFusionOptions{SourceOrder: []HybridCandidateSource{HybridCandidateSourceVector, HybridCandidateSourceText}}, 2)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates custom source order: %v", err)
	}
	if got := []string{string(results[0].ID), string(results[1].ID)}; got[0] != "a-vector-best" || got[1] != "z-text-best" {
		t.Fatalf("custom best-ranked source tie order=%v want vector-best before text-best", got)
	}
}

func TestHybridRRFFusionTopKAndCandidateBounds(t *testing.T) {
	candidates := []HybridSearchCandidate{
		hybridFusionCandidate("d1", HybridCandidateSourceText, 1, 1, HybridScoreKindBM25),
		hybridFusionCandidate("d2", HybridCandidateSourceText, 2, 1, HybridScoreKindBM25),
		hybridFusionCandidate("d3", HybridCandidateSourceVector, 3, 1, HybridScoreKindVectorSimilarity),
	}

	results, stats, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, 2)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates topK=2: %v", err)
	}
	if len(results) != 2 || results[0].Rank != 1 || results[1].Rank != 2 {
		t.Fatalf("results=%+v want exactly top 2 with one-based ranks", results)
	}
	if stats.CandidatesFused != 3 || stats.CandidatesAfterFusion != 3 || stats.Truncated != 1 || stats.FusionTextOnly != 2 || stats.FusionVectorOnly != 1 {
		t.Fatalf("stats=%+v want fused=3 after=3 truncated=1 and full fused-set source counters", stats)
	}

	results, stats, err = FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, 10)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates topK=10: %v", err)
	}
	if len(results) != 3 || stats.Truncated != 0 || stats.FusionVectorOnly != 1 {
		t.Fatalf("len=%d stats=%+v want all candidates and no truncation", len(results), stats)
	}

	results, stats, err = FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, 0)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates topK=0: %v", err)
	}
	if len(results) != 0 || stats.CandidatesAfterFusion != 3 || stats.Truncated != 3 || stats.FusionTextOnly != 2 || stats.FusionVectorOnly != 1 {
		t.Fatalf("topK=0 results=%+v stats=%+v want no results, full fused counters, truncated count", results, stats)
	}
}

func TestHybridRRFFusionRejectsUnsupportedOptions(t *testing.T) {
	_, _, err := FuseHybridSearchCandidates([]HybridSearchCandidate{hybridFusionCandidate("d1", HybridCandidateSourceText, 1, 1, HybridScoreKindBM25)}, HybridFusionOptions{Method: HybridFusionMethod("linear")}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("unsupported method err=%v want ErrHybridSearchUnsupported", err)
	}

	_, _, err = FuseHybridSearchCandidates([]HybridSearchCandidate{hybridFusionCandidate("d1", HybridCandidateSourceText, 1, 1, HybridScoreKindBM25)}, HybridFusionOptions{TiePolicy: HybridFusionTiePolicy("score_only")}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("unsupported tie err=%v want ErrHybridSearchUnsupported", err)
	}

	_, _, err = FuseHybridSearchCandidates(nil, HybridFusionOptions{SourceOrder: []HybridCandidateSource{HybridCandidateSource("image")}}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("bad source order on empty candidates err=%v want ErrHybridSearchUnsupported", err)
	}

	_, _, err = FuseHybridSearchCandidates([]HybridSearchCandidate{hybridFusionCandidate("d1", HybridCandidateSourceText, 1, 1, HybridScoreKindBM25)}, HybridFusionOptions{SourceOrder: []HybridCandidateSource{HybridCandidateSource("image")}}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("bad source order err=%v want ErrHybridSearchUnsupported", err)
	}

	_, _, err = FuseHybridSearchCandidates([]HybridSearchCandidate{hybridFusionCandidate("d1", HybridCandidateSourceText, 0, 1, HybridScoreKindBM25)}, HybridFusionOptions{}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("bad rank err=%v want ErrHybridSearchUnsupported", err)
	}

	_, _, err = FuseHybridSearchCandidates([]HybridSearchCandidate{{ID: []byte("d1"), Source: HybridCandidateSource("image"), SourceRank: 1}}, HybridFusionOptions{}, 1)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("bad source err=%v want ErrHybridSearchUnsupported", err)
	}
}

func hybridFusionCandidate(id string, source HybridCandidateSource, rank int, score float64, kind HybridScoreKind) HybridSearchCandidate {
	indexName := "body"
	if source == HybridCandidateSourceVector {
		indexName = "embedding"
	}
	return HybridSearchCandidate{
		ID:         []byte(id),
		Source:     source,
		IndexName:  indexName,
		SourceRank: rank,
		Score:      score,
		ScoreKind:  kind,
	}
}

func hybridFloatClose(got, want float64) bool {
	return math.Abs(got-want) <= 1e-12
}
