package collections

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

func nativeScalarFixtureExactOracle(totalRows, topK int, matches func(int) bool) []VectorIndexSearchResult {
	results := make([]VectorIndexSearchResult, 0, topK)
	for i := 0; i < totalRows; i++ {
		if !matches(i) {
			continue
		}
		y := float32(float64(i+1) / 100000)
		results = append(results, VectorIndexSearchResult{
			ID:    []byte(fmt.Sprintf("doc-%05d", i)),
			Score: 1 / math.Sqrt(1+float64(y)*float64(y)),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return string(results[i].ID) < string(results[j].ID)
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results
}

func assertNativeScalarExactOracle(t *testing.T, got, oracle []VectorIndexSearchResult) {
	t.Helper()
	if len(got) != len(oracle) {
		t.Fatalf("exact result count=%d want=%d results=%+v", len(got), len(oracle), got)
	}
	for i := range oracle {
		if string(got[i].ID) != string(oracle[i].ID) || math.Abs(got[i].Score-oracle[i].Score) > 1e-6 {
			t.Fatalf("exact result[%d]=%+v want=%+v", i, got[i], oracle[i])
		}
	}
}

func assertNativeScalarANNOracleContract(t *testing.T, got, oracle []VectorIndexSearchResult) {
	t.Helper()
	if len(got) != len(oracle) {
		t.Fatalf("ANN result count=%d want=%d results=%+v", len(got), len(oracle), got)
	}
	oracleIDs := make(map[string]struct{}, len(oracle))
	for _, result := range oracle {
		oracleIDs[string(result.ID)] = struct{}{}
	}
	overlap := 0
	for i, result := range got {
		if math.IsNaN(result.Score) || math.IsInf(result.Score, 0) {
			t.Fatalf("ANN result[%d] has non-finite score: %+v", i, result)
		}
		if i > 0 && (got[i-1].Score < result.Score ||
			got[i-1].Score == result.Score && string(got[i-1].ID) > string(result.ID)) {
			t.Fatalf("ANN results violate deterministic score/id order at %d: %+v", i, got)
		}
		if _, ok := oracleIDs[string(result.ID)]; ok {
			overlap++
		}
	}
	if overlap != len(oracle) {
		t.Fatalf("ANN top-k recall=%d/%d results=%+v oracle=%+v", overlap, len(oracle), got, oracle)
	}
}

func sameVectorIndexResultIDs(left, right []VectorIndexSearchResult) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if string(left[i].ID) != string(right[i].ID) {
			return false
		}
	}
	return true
}
