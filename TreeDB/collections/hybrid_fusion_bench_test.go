package collections

import (
	"fmt"
	"testing"
)

var (
	hybridFusionBenchmarkResults []HybridSearchResult
	hybridFusionBenchmarkStats   HybridSearchStats
)

func BenchmarkHybridRRFFusion(b *testing.B) {
	for _, candidateCount := range []int{10, 100, 1000, 10000} {
		candidateCount := candidateCount
		b.Run(fmt.Sprintf("candidates_%d", candidateCount), func(b *testing.B) {
			candidates := makeHybridFusionBenchmarkCandidates(candidateCount)
			topK := 25
			if candidateCount < topK {
				topK = candidateCount
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, stats, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, topK)
				if err != nil {
					b.Fatalf("FuseHybridSearchCandidates: %v", err)
				}
				hybridFusionBenchmarkResults = results
				hybridFusionBenchmarkStats = stats
			}
		})
	}
}

func makeHybridFusionBenchmarkCandidates(n int) []HybridSearchCandidate {
	candidates := make([]HybridSearchCandidate, 0, n)
	for i := 0; i < n; i++ {
		source := HybridCandidateSourceText
		scoreKind := HybridScoreKindBM25
		indexName := "body"
		sourceRank := i/2 + 1
		// Every fourth text/vector pair overlaps on the same document ID so the
		// benchmark covers both union and duplicate-contribution paths.
		docOrdinal := i
		if i%2 == 1 {
			source = HybridCandidateSourceVector
			scoreKind = HybridScoreKindVectorSimilarity
			indexName = "embedding"
			if (i/2)%4 == 0 {
				docOrdinal = i - 1
			}
		}
		candidates = append(candidates, HybridSearchCandidate{
			ID:         []byte(fmt.Sprintf("doc-%08d", docOrdinal)),
			Source:     source,
			IndexName:  indexName,
			SourceRank: sourceRank,
			Score:      float64(n-i) / float64(n+1),
			ScoreKind:  scoreKind,
		})
	}
	return candidates
}
