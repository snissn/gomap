package main

import (
	"context"
	"github.com/snissn/gomap/TreeDB/collections"
	"testing"
)

// Feature-enabled cost includes candidate collection, three reducers, hashes,
// and owned result allocation. It deliberately does not time a cache hit.
func BenchmarkM8RouterPolicyPreparedV1(b *testing.B) {
	h, queries := m8RouterPreparedBenchFixtureV1(b)
	opts := collections.VectorPartitionRouterPolicyDiagnosticOptionsV1{Mode: collections.VectorPartitionRouterModeApproxV1, ScoreBudget: defaultRouterScoreBudgetV2, ReturnedWidth: int(h.status.Representatives), BeamWidth: int(h.status.Representatives), PartitionProbes: 2}
	ctx := context.Background()
	for _, q := range queries {
		if _, err := h.router.CompareRankingPoliciesForDiagnosticsV1(ctx, q, opts); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.router.CompareRankingPoliciesForDiagnosticsV1(ctx, queries[i%len(queries)], opts); err != nil {
			b.Fatal(err)
		}
	}
}
