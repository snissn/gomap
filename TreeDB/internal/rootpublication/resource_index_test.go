package rootpublication

import (
	"fmt"
	"testing"
)

func benchmarkResourceTokens(count int) ([]*StableResourceToken, []*StableResourceSet) {
	tokens := make([]*StableResourceToken, count)
	sets := make([]*StableResourceSet, count)
	for n := range count {
		token := &StableResourceToken{
			kind: ResourceValueLogSegment, logicalNamespace: "benchmark",
			resourceID: fmt.Sprintf("segment/%d", n), generation: 1,
			identity: StableIdentity{Device: 1, File: uint64(n + 1)}, requiredFrontier: 64,
			reachabilityField: fmt.Sprintf("benchmark.resources[%d]", n),
		}
		tokens[n] = token
		sets[n] = &StableResourceSet{entries: []stableResourceEntry{{token: token}}}
	}
	return tokens, sets
}

func BenchmarkNormalizeStableResourceTokens(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 65_000} {
		b.Run(fmt.Sprintf("tokens_%d", count), func(b *testing.B) {
			tokens, _ := benchmarkResourceTokens(count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				entries, err := normalizeStableResourceTokens(tokens)
				if err != nil || len(entries) != count {
					b.Fatalf("normalize entries=%d err=%v", len(entries), err)
				}
			}
		})
	}
}

func BenchmarkPendingResourceIndexOneTokenCandidates(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 65_000} {
		b.Run(fmt.Sprintf("candidates_%d", count), func(b *testing.B) {
			_, sets := benchmarkResourceTokens(count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var index pendingResourceIndex
				for _, set := range sets {
					if err := index.add(set); err != nil {
						b.Fatal(err)
					}
				}
				if snapshot := index.borrowedSnapshot(); snapshot == nil || len(snapshot.entries) != count {
					b.Fatalf("snapshot entries=%v", snapshot)
				}
			}
		})
	}
}
