package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingPublisher struct{ calls atomic.Uint64 }

func (p *countingPublisher) Publish(context.Context, *PreparedRootCandidate) PublishResult {
	p.calls.Add(1)
	return PublishResult{Outcome: PublishSucceeded}
}

func runProducers(b *testing.B, producers int, operation func()) {
	b.Helper()
	var next atomic.Int64
	next.Store(int64(b.N))
	var workers sync.WaitGroup
	workers.Add(producers)
	for range producers {
		go func() {
			defer workers.Done()
			for next.Add(-1) >= 0 {
				operation()
			}
		}()
	}
	workers.Wait()
}

func BenchmarkCoordinatorEnqueue(b *testing.B) {
	for _, producers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
			clock := NewFakeClock(time.Unix(1, 0))
			publisher := new(countingPublisher)
			coordinator, err := New(Options{Clock: clock, Publisher: publisher})
			if err != nil {
				b.Fatal(err)
			}
			defer stopClean(b, coordinator)
			var seq atomic.Uint64
			var retries atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			runProducers(b, producers, func() {
				for {
					n := seq.Add(1)
					prepared, _ := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(n, n, n, n, n)})
					err := coordinator.Enqueue(context.Background(), prepared)
					if err == nil {
						return
					}
					if !errors.Is(err, ErrInvalidCandidate) {
						b.Error(err)
						return
					}
					retries.Add(1)
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(retries.Load())/float64(b.N), "contention-retries/op")
			b.ReportMetric(float64(publisher.calls.Load())/float64(b.N), "stable-io/op")
		})
	}
}

func BenchmarkCoordinatorSupersede(b *testing.B) {
	for _, producers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
			clock := NewFakeClock(time.Unix(1, 0))
			publisher := new(countingPublisher)
			coordinator, err := New(Options{Clock: clock, Publisher: publisher})
			if err != nil {
				b.Fatal(err)
			}
			defer stopClean(b, coordinator)
			seed, _ := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)})
			if err := coordinator.Enqueue(context.Background(), seed); err != nil {
				b.Fatal(err)
			}
			var seq atomic.Uint64
			seq.Store(1)
			var retries atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			runProducers(b, producers, func() {
				for {
					n := seq.Add(1)
					prepared, _ := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(n, n, n, n, n)})
					err := coordinator.Supersede(context.Background(), prepared)
					if err == nil {
						return
					}
					if !errors.Is(err, ErrInvalidCandidate) {
						b.Error(err)
						return
					}
					retries.Add(1)
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(retries.Load())/float64(b.N), "contention-retries/op")
			b.ReportMetric(float64(publisher.calls.Load())/float64(b.N), "stable-io/op")
		})
	}
}

func BenchmarkCoordinatorWait(b *testing.B) {
	for _, producers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
			publisher := new(countingPublisher)
			coordinator, err := New(Options{Publisher: publisher})
			if err != nil {
				b.Fatal(err)
			}
			defer stopClean(b, coordinator)
			var seq atomic.Uint64
			var retries atomic.Uint64
			started := time.Now()
			b.ReportAllocs()
			b.ResetTimer()
			runProducers(b, producers, func() {
				var mySeq uint64
				for {
					mySeq = seq.Add(1)
					prepared, _ := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(mySeq, mySeq, mySeq, mySeq, mySeq)})
					err := coordinator.Enqueue(context.Background(), prepared)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrInvalidCandidate) {
						b.Error(err)
						return
					}
					retries.Add(1)
				}
				err := coordinator.WaitThrough(context.Background(), mySeq)
				if err != nil {
					b.Error(err)
				}
			})
			b.StopTimer()
			elapsed := time.Since(started).Seconds()
			if elapsed > 0 {
				b.ReportMetric(float64(publisher.calls.Load())/elapsed, "groups/s")
			}
			b.ReportMetric(float64(retries.Load())/float64(b.N), "contention-retries/op")
		})
	}
}

func TestBenchmarkHelpersRecognizeExpectedStop(t *testing.T) {
	// Keep the benchmark cleanup contract explicit: unpublished ordinary debt is
	// reported, never silently called durable.
	c, err := New(Options{Clock: NewFakeClock(time.Unix(1, 0)), Publisher: new(countingPublisher)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop=%v", err)
	}
}
