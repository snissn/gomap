package rootpublication

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type countingPublisher struct{ calls atomic.Uint64 }

func (p *countingPublisher) Publish(context.Context, *PreparedRootCandidate) PublishResult {
	p.calls.Add(1)
	return PublishResult{Outcome: PublishSucceeded}
}

func BenchmarkCoordinatorPrimitive(b *testing.B) {
	for _, producers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
			publisher := new(countingPublisher)
			coordinator, err := New(Options{Publisher: publisher})
			if err != nil {
				b.Fatal(err)
			}
			defer stopClean(b, coordinator)
			var seq atomic.Uint64
			b.ReportAllocs()
			b.SetParallelism(producers)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := seq.Add(1)
					prepared, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(n, n, n, n, n), IndexBytes: 1})
					if err != nil {
						b.Fatal(err)
					}
					// Serialize frontier installation exactly as the future commit
					// combiner does; producer contention remains part of the result.
					for {
						err = coordinator.Enqueue(context.Background(), prepared)
						if err == nil {
							break
						}
						if !errorsIsInvalidCandidate(err) {
							b.Fatal(err)
						}
						// A later producer won visibility; create the next frontier.
						n = seq.Add(1)
						prepared, _ = NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(n, n, n, n, n), IndexBytes: 1})
					}
				}
			})
			b.StopTimer()
			if err := coordinator.Drain(context.Background()); err != nil {
				b.Fatal(err)
			}
			if publisher.calls.Load() == 0 {
				b.Fatal("benchmark did not drain")
			}
		})
	}
}

func errorsIsInvalidCandidate(err error) bool {
	return err != nil // benchmark retries only monotonic-frontier races
}

func BenchmarkCoordinatorSupersede(b *testing.B) {
	publisher := new(countingPublisher)
	coordinator, err := New(Options{Publisher: publisher})
	if err != nil {
		b.Fatal(err)
	}
	defer stopClean(b, coordinator)
	var mu sync.Mutex
	var seq uint64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		seq++
		prepared, _ := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), DependencyBytes: 1})
		if err := coordinator.Enqueue(context.Background(), prepared); err != nil {
			b.Fatal(err)
		}
		mu.Unlock()
	}
	b.StopTimer()
	if publisher.calls.Load() != 0 {
		b.Fatalf("stable I/O calls during enqueue=%d", publisher.calls.Load())
	}
}
