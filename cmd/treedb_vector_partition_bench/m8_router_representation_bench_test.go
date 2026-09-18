package main

import (
	"context"
	"github.com/snissn/gomap/TreeDB/collections"
	"testing"
)

func BenchmarkM8RouterRepresentationBuildV1(b *testing.B) {
	h, _ := m8RouterPreparedBenchFixtureV1(b)
	for _, arm := range []string{"multilevel", "centroid_geometry"} {
		b.Run(arm, func(b *testing.B) {
			o := collections.VectorPartitionRouterRepresentationOptionsV1{Arm: arm, ReturnedWidth: 4, MaxBuildWork: 1_000_000_000, MaxBuildBytes: 256 << 20}
			b.ReportAllocs()
			b.ResetTimer()
			var count int
			for i := 0; i < b.N; i++ {
				e, err := h.router.BuildRepresentationForDiagnosticsV1(context.Background(), h.collection, o)
				if err != nil {
					b.Fatal(err)
				}
				info := e.InfoV1()
				count = 0
				for _, m := range info.Models {
					count += m.Representatives
				}
			}
			b.ReportMetric(float64(count), "centroids/build")
		})
	}
}

// Exact scoring plus reduction and hashes on the prepared centroid-only owner.
// This is not local-pack ANN, service QPS, or a cached-receipt lookup.
func BenchmarkM8RouterRepresentationPreparedV1(b *testing.B) {
	h, queries := m8RouterPreparedBenchFixtureV1(b)
	for _, arm := range []string{"multilevel", "centroid_geometry"} {
		b.Run(arm, func(b *testing.B) {
			o := collections.VectorPartitionRouterRepresentationOptionsV1{Arm: arm, ReturnedWidth: 4, MaxBuildWork: 1_000_000_000, MaxBuildBytes: 256 << 20}
			e, err := h.router.BuildRepresentationForDiagnosticsV1(context.Background(), h.collection, o)
			if err != nil {
				b.Fatal(err)
			}
			for _, q := range queries {
				if _, err := e.CompareWithContextV1(context.Background(), q, 2); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			var scores, failures uint64
			for i := 0; i < b.N; i++ {
				rs, err := e.CompareWithContextV1(context.Background(), queries[i%len(queries)], 2)
				if err != nil {
					b.Fatal(err)
				}
				for _, r := range rs {
					scores += r.ScoreInvocations
					if r.Status != "pass" {
						failures++
					}
				}
			}
			b.ReportMetric(float64(scores)/float64(b.N), "scores/op")
			b.ReportMetric(float64(failures)/float64(b.N), "refused-arms/op")
		})
	}
}
