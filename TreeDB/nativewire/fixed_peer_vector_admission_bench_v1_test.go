package nativewire

import (
	"context"
	"testing"
)

// This file can be overlaid unchanged on the pre-fix source. The old mutation
// path takes the raw RWMutex; the candidate calls its context-checked helper.
// Setup and method selection are excluded. No graph, Raft, or public-path
// throughput inference follows from these uncontended admission microbenchmarks.
func BenchmarkFixedPeerVectorAdmissionUncontendedV1(b *testing.B) {
	for _, name := range []string{"search", "mutation"} {
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			vector := &fixedPeerVectorRuntimeV1{}
			ctx := context.Background()
			var enter func() error
			var leave func()
			if name == "search" {
				enter = func() error { return vector.lockSearchAdmissionV1(ctx) }
				leave = vector.mutationMu.RUnlock
			} else {
				if candidate, ok := any(vector).(interface{ lockMutationAdmissionV1(context.Context) error }); ok {
					enter = func() error { return candidate.lockMutationAdmissionV1(ctx) }
				} else {
					enter = func() error { vector.mutationMu.Lock(); return nil }
				}
				leave = vector.mutationMu.Unlock
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := enter(); err != nil {
					b.Fatal(err)
				}
				leave()
			}
			b.StopTimer()
		})
	}
}
