package collections

import (
	"errors"
	"testing"
)

func TestOrdinaryFilteredVectorHybridFailsBeforeReadOwner(t *testing.T) {
	for _, withText := range []bool{false, true} {
		t.Run(map[bool]string{false: "vector", true: "text_and_vector"}[withText], func(t *testing.T) {
			opts := HybridSearchOptions{
				TopK:         1,
				Vector:       &HybridVectorQuery{IndexName: "embedding", Query: []float32{1, 0}},
				ScalarFilter: &HybridScalarFilter{IndexName: "category", Value: "a"},
			}
			if withText {
				opts.Text = &HybridTextQuery{IndexName: "body", Query: "hello"}
			}
			// An empty collection has no usable read owner. Reaching one would fail
			// or panic, so this also checks that rejection precedes read acquisition.
			response, err := (&Collection{}).searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyDefault)
			if !errors.Is(err, ErrHybridSearchUnsupported) || response.Stats.FailClosed != 1 || response.Stats.FailClosedReason != HybridFailClosedReasonUnsupported {
				t.Fatalf("got response %+v, error %v; want unsupported and fail-closed", response, err)
			}
			if len(response.Results) != 0 || len(response.Candidates) != 0 {
				t.Fatalf("unsupported query returned results: %+v", response)
			}
		})
	}
}

func TestOrdinaryFilteredQuantizedHybridFailsBeforeReadOwner(t *testing.T) {
	opts := HybridSearchOptions{
		TopK:         1,
		Vector:       &HybridVectorQuery{IndexName: "embedding", Query: []float32{1, 0}, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "q", QuantizedRerankCandidates: 4},
		ScalarFilter: &HybridScalarFilter{IndexName: "category", Value: "a"},
	}
	response, err := (&Collection{}).searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyDefault)
	if !errors.Is(err, ErrHybridSearchUnsupported) || response.Stats.FailClosedReason != HybridFailClosedReasonUnsupported || len(response.Results) != 0 {
		t.Fatalf("got response %+v, error %v; want unsupported before read", response, err)
	}
}
