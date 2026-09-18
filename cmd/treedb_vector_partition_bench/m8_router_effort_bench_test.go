package main

import (
	"context"
	"github.com/snissn/gomap/TreeDB/collections"
	"testing"
)

// Collection plus unchanged reducers/hashes, not a receipt-cache lookup. Setup
// and local-pack search are excluded; budget failures are counted, not dropped.
func BenchmarkM8RouterEffortPreparedV1(b *testing.B) {
	h, queries := m8RouterPreparedBenchFixtureV1(b)
	n := int(h.status.Representatives)
	for _, mode := range []string{collections.VectorPartitionRouterEffortLegacyV1, collections.VectorPartitionRouterEffortHierarchicalV1} {
		b.Run(mode, func(b *testing.B) {
			cap := n
			if mode == collections.VectorPartitionRouterEffortHierarchicalV1 {
				cap = 4 * n
			}
			o := collections.VectorPartitionRouterEffortOptionsV1{Mode: mode, ReturnedWidth: n, Beam: n, ScoreBudget: cap}
			for _, q := range queries {
				if _, err := h.router.CompareRankingPoliciesWithEffortForDiagnosticsV1(context.Background(), q, o, 2); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			var scores uint64
			for i := 0; i < b.N; i++ {
				r, err := h.router.CompareRankingPoliciesWithEffortForDiagnosticsV1(context.Background(), queries[i%len(queries)], o, 2)
				if err != nil {
					b.Fatal(err)
				}
				scores += r.Work.TotalScoreCalls
			}
			b.ReportMetric(float64(scores)/float64(b.N), "scores/op")
		})
	}
}
