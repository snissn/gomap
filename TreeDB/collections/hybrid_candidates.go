package collections

// HybridCandidateResponse is returned by candidate-only source adapters. The
// adapters are pre-fusion and pre-final-fetch: Candidates contains source hits
// only, and Stats reports candidate-generation counters in the shared hybrid
// vocabulary.
type HybridCandidateResponse struct {
	Stats      HybridSearchStats       `json:"stats,omitempty"`
	Candidates []HybridSearchCandidate `json:"candidates,omitempty"`
}

func hybridMaxUint64(values ...uint64) uint64 {
	var max uint64
	for _, value := range values {
		if value > max {
			max = value
		}
	}
	return max
}
