package main

import (
	"context"
	"github.com/snissn/gomap/TreeDB/collections"
	"testing"
)

// This fixture uses the actual immutable M8 source/router/local packs, without
// TCP or document materialization. Setup is explicitly outside the timer.
func m8RouterPreparedBenchFixtureV1(b *testing.B) (*m8ProductionMultiGroupAssetsV1, [][]float32) {
	b.Helper()
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		b.Skip("partition persistence unsupported")
	}
	f := m8QualificationFixturesV1[0]
	f.Vectors = 2048
	f.Dimensions = 32
	f.Queries = 16
	vectors, queries := fixtureData(f)
	h, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := h.Close(); err != nil {
			b.Error(err)
		}
	})
	out := make([][]float32, len(queries))
	for i := range queries {
		out[i] = m8Query32V1(queries[i])
	}
	return h, out
}

// The identical benchmark is run on the base and candidate production sources.
// This is router query cost, NOT full partition ANN QPS or public-service QPS.
func BenchmarkM8RouterOrdinaryPathV1(b *testing.B) {
	h, queries := m8RouterPreparedBenchFixtureV1(b)
	opts := collections.VectorPartitionRouterSearchOptionsV1{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: int(h.status.Representatives), PartitionProbes: 2}
	ctx := context.Background()
	for _, q := range queries {
		if _, err := h.router.SearchWithContextV1(ctx, q, opts); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.router.SearchWithContextV1(ctx, queries[i%len(queries)], opts); err != nil {
			b.Fatal(err)
		}
	}
}
