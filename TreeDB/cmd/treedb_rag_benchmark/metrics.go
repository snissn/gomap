package main

import (
	"fmt"
	"sort"
)

// recallAtK returns |top-k(ranked) ∩ relevant| / |relevant|. An empty relevant
// set is a caller bug; the harness validates non-empty judgments up front.
func recallAtK(ranked []string, relevant map[string]bool, k int) (float64, error) {
	if len(relevant) == 0 {
		return 0, fmt.Errorf("recall: empty relevant set")
	}
	if k <= 0 {
		return 0, fmt.Errorf("recall: non-positive k=%d", k)
	}
	if len(ranked) < k {
		return 0, fmt.Errorf("recall@%d: ranking depth=%d is below k", k, len(ranked))
	}
	hits := 0
	for i, id := range ranked {
		if i >= k {
			break
		}
		if relevant[id] {
			hits++
		}
	}
	return float64(hits) / float64(len(relevant)), nil
}

// mrrAtK returns the mean reciprocal rank of the first relevant result within
// the top k of ranked (0 if none appear).
func mrrAtK(ranked []string, relevant map[string]bool, k int) (float64, error) {
	if len(relevant) == 0 {
		return 0, fmt.Errorf("mrr: empty relevant set")
	}
	if k <= 0 {
		return 0, fmt.Errorf("mrr: non-positive k=%d", k)
	}
	for i, id := range ranked {
		if i >= k {
			break
		}
		if relevant[id] {
			return 1 / float64(i+1), nil
		}
	}
	return 0, nil
}

// accumulateQuality adds one timed query's hand-verifiable quality terms to
// the running row aggregates. Both the runner and the metric-validation test
// use this exact path.
func accumulateQuality(row *rowResult, ranked []string, rel map[string]bool) error {
	r5, err := recallAtK(ranked, rel, 5)
	if err != nil {
		return err
	}
	r10, err := recallAtK(ranked, rel, 10)
	if err != nil {
		return err
	}
	mrr, err := mrrAtK(ranked, rel, 10)
	if err != nil {
		return err
	}
	row.RecallAt5 += r5
	row.RecallAt10 += r10
	row.MRRAt10 += mrr
	return nil
}

// percentile returns the p-quantile (0..100) of values using linear
// interpolation over the sorted copy. Empty input fails closed.
func percentile(values []float64, p float64) (float64, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("percentile: no samples")
	}
	if p < 0 || p > 100 {
		return 0, fmt.Errorf("percentile: out-of-range p=%.2f", p)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0], nil
	}
	rank := (p / 100) * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[len(sorted)-1], nil
	}
	frac := rank - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo]), nil
}

// relevanceMap indexes ground truth by query id.
func relevanceMap(gts []ragGroundTruth) map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(gts))
	for _, gt := range gts {
		set := make(map[string]bool, len(gt.Relevant))
		for _, id := range gt.Relevant {
			set[id] = true
		}
		out[gt.QueryID] = set
	}
	return out
}
