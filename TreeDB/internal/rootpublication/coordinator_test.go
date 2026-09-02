package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func candidate(t testing.TB, seq, bytes uint64, obligations ...byte) *PreparedRootCandidate {
	t.Helper()
	ids := make([]ObligationID, len(obligations))
	for i, value := range obligations {
		ids[i][0] = value
	}
	c, err := NewPreparedRootCandidate(CandidateSpec{
		Frontier:        NewFrontier(seq, seq*10, seq*20, seq, seq),
		DependencyBytes: bytes,
		Obligations:     ids,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func stopClean(t testing.TB, c *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Stop(ctx); err != nil && !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop: %v", err)
	}
}

func waitForCall(t testing.TB, calls <-chan *PreparedRootCandidate) *PreparedRootCandidate {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("publisher was not called")
		return nil
	}
}

func waitFor(t testing.TB, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPreparedCandidateCopiesAndNormalizesObligations(t *testing.T) {
	input := []ObligationID{{3}, {1}, {3}}
	prepared, err := NewPreparedRootCandidate(CandidateSpec{
		Frontier: NewFrontier(1, 2, 3, 4, 5), Obligations: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	input[0][0] = 9
	got := prepared.Obligations()
	got[0][0] = 8
	if want := []ObligationID{{1}, {3}}; len(prepared.Obligations()) != 2 || prepared.Obligations()[0] != want[0] || prepared.Obligations()[1] != want[1] {
		t.Fatalf("candidate obligations mutated: %v", prepared.Obligations())
	}
}

type testExtension uint64

func (e testExtension) union(other immutableExtension) (immutableExtension, error) {
	return e | other.(testExtension), nil
}

func TestCoalescingUnionsEveryReservedExtensionSlot(t *testing.T) {
	one, err := newPreparedRootCandidateWithExtensions(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, extensionSlots{
		resourceSet: testExtension(1), cowFreelist: testExtension(2), durableRootRecord: testExtension(4),
	})
	if err != nil {
		t.Fatal(err)
	}
	two, err := newPreparedRootCandidateWithExtensions(CandidateSpec{Frontier: NewFrontier(2, 2, 2, 2, 2)}, extensionSlots{
		resourceSet: testExtension(8), cowFreelist: testExtension(16), durableRootRecord: testExtension(32),
	})
	if err != nil {
		t.Fatal(err)
	}
	coalesced, err := coalesceCandidates([]*PreparedRootCandidate{one, two})
	if err != nil {
		t.Fatalf("coalesce candidates: %v", err)
	}
	got := coalesced.extensions
	if got.resourceSet != testExtension(9) || got.cowFreelist != testExtension(18) || got.durableRootRecord != testExtension(36) {
		t.Fatalf("extension union=%+v", got)
	}
}

func duplicatePhysicalCandidates(tb testing.TB, sourceCount int) []*PreparedRootCandidate {
	tb.Helper()
	sets := duplicatePhysicalStableResourceSets(tb, sourceCount)
	candidates := make([]*PreparedRootCandidate, sourceCount)
	for i, set := range sets {
		candidate, err := NewPreparedRootCandidate(CandidateSpec{
			Frontier:    NewFrontier(uint64(i+1), uint64(i+1), uint64(i+1), uint64(i+1), uint64(i+1)),
			ResourceSet: set,
		})
		if err != nil {
			tb.Fatal(err)
		}
		candidates[i] = candidate
	}
	tb.Cleanup(func() {
		for _, candidate := range candidates {
			candidate.AbandonResources()
		}
	})
	return candidates
}

func TestCoalesceCandidatesDuplicatePinsHasBoundedAllocationGrowth(t *testing.T) {
	const sourceCount = 1024
	candidates := duplicatePhysicalCandidates(t, sourceCount)
	result := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			coalesced, err := coalesceCandidates(candidates)
			if err != nil {
				b.Fatal(err)
			}
			if got := coalesced.Resources().Len(); got != 1 {
				b.Fatalf("coalesced durability obligations=%d want 1", got)
			}
			runtime.KeepAlive(coalesced)
		}
	})
	if allocated := result.AllocedBytesPerOp(); allocated > 2<<20 {
		t.Fatalf("coalescing %d duplicate-resource candidates allocated %d bytes/op; want <= %d", sourceCount, allocated, 2<<20)
	}
}

func BenchmarkCoalesceCandidatesDuplicatePhysicalScale(b *testing.B) {
	for _, sourceCount := range []int{8, 64, 256, 1024} {
		b.Run(fmt.Sprintf("sources=%d", sourceCount), func(b *testing.B) {
			candidates := duplicatePhysicalCandidates(b, sourceCount)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				coalesced, err := coalesceCandidates(candidates)
				if err != nil {
					b.Fatal(err)
				}
				if got := coalesced.Resources().Len(); got != 1 {
					b.Fatalf("coalesced durability obligations=%d want 1", got)
				}
			}
		})
	}
}

func TestNewRejectsRecoverableFrontierNewerThanDurable(t *testing.T) {
	_, err := New(Options{
		Publisher:              PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult { return PublishResult{} }),
		InitialDurableFrontier: NewFrontier(4, 0, 0, 0, 0), OldestRecoverableCommitSeq: 5,
	})
	if err == nil {
		t.Fatal("accepted oldest recoverable frontier newer than durable")
	}
}

func TestReachabilityEpochInvalidatesOnEnqueueAndDurablePublish(t *testing.T) {
	calls := make(chan *PreparedRootCandidate, 1)
	release := make(chan struct{})
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate
		select {
		case <-release:
			return PublishResult{Outcome: PublishSucceeded}
		case <-ctx.Done():
			return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)

	initial, err := c.CaptureReachability()
	if err != nil {
		t.Fatal(err)
	}
	defer initial.Release()
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.RevalidateReachability(initial.Epoch); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("initial snapshot after enqueue=%v", err)
	}

	pending, err := c.CaptureReachability()
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Release()
	if pending.Visible.CommitSeq() != 1 || pending.Durable.CommitSeq() != 0 || len(pending.Pending) != 1 || pending.Pending[0].CommitSeq() != 1 {
		t.Fatalf("pending snapshot=%+v", pending)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- c.WaitThrough(context.Background(), 1) }()
	waitForCall(t, calls)
	close(release)
	if err := <-waitDone; err != nil {
		t.Fatal(err)
	}
	if err := c.RevalidateReachability(pending.Epoch); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("pending snapshot after durable publication=%v", err)
	}

	durable, err := c.CaptureReachability()
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Release()
	if durable.Visible.CommitSeq() != 1 || durable.Durable.CommitSeq() != 1 || len(durable.Pending) != 0 {
		t.Fatalf("durable snapshot=%+v", durable)
	}
	if err := c.RevalidateReachability(durable.Epoch); err != nil {
		t.Fatalf("unchanged durable snapshot=%v", err)
	}
}

func TestReachabilityEpochSurvivesRetryableFailure(t *testing.T) {
	want := errors.New("pre-meta retry")
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishRetryableFailure, Err: want}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.CaptureReachability()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if err := c.WaitThrough(context.Background(), 1); !errors.Is(err, want) {
		t.Fatalf("wait=%v", err)
	}
	if err := c.RevalidateReachability(snapshot.Epoch); err != nil {
		t.Fatalf("retryable failure changed reachability=%v", err)
	}
}

func TestReachabilityRevalidationFailsClosedAfterAmbiguousPublish(t *testing.T) {
	want := errors.New("meta sync ambiguous")
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishAmbiguous, Err: want}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.CaptureReachability()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if err := c.WaitThrough(context.Background(), 1); !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, want) {
		t.Fatalf("wait=%v", err)
	}
	if err := c.RevalidateReachability(snapshot.Epoch); !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, want) {
		t.Fatalf("ambiguous revalidation=%v", err)
	}
}

func TestVisibleFrontierDoesNotSatisfyDurabilityWaiter(t *testing.T) {
	calls := make(chan *PreparedRootCandidate, 1)
	release := make(chan struct{})
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate
		select {
		case <-release:
			return PublishResult{Outcome: PublishSucceeded}
		case <-ctx.Done():
			return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 10)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.WaitThrough(context.Background(), 1) }()
	waitForCall(t, calls)
	select {
	case err := <-done:
		t.Fatalf("visible-only frontier satisfied waiter: %v", err)
	default:
	}
	stats := c.Stats()
	if stats.VisibleCommitSeq != 1 || stats.DurableCommitSeq != 0 || stats.WaiterCount != 1 {
		t.Fatalf("frontier/waiter stats=%+v", stats)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWaiterRemovalClearsDiscardedBackingSlots(t *testing.T) {
	newWaiters := func() []*durabilityWaiter {
		return []*durabilityWaiter{
			{seq: 1, ch: make(chan error, 1)},
			{seq: 2, ch: make(chan error, 1)},
			{seq: 3, ch: make(chan error, 1)},
		}
	}
	assertClearedTail := func(t *testing.T, waiters []*durabilityWaiter) {
		t.Helper()
		backing := waiters[:cap(waiters)]
		for i := len(waiters); i < len(backing); i++ {
			if backing[i] != nil {
				t.Fatalf("discarded waiter retained at backing slot %d", i)
			}
		}
	}
	t.Run("remove", func(t *testing.T) {
		c := &Coordinator{waiters: newWaiters()}
		c.removeWaiterLocked(c.waiters[1])
		assertClearedTail(t, c.waiters)
	})
	t.Run("captured failure", func(t *testing.T) {
		c := &Coordinator{waiters: newWaiters()}
		c.failCapturedWaitersLocked(errors.New("captured"), []*durabilityWaiter{c.waiters[0], c.waiters[2]})
		assertClearedTail(t, c.waiters)
	})
	t.Run("durable satisfaction", func(t *testing.T) {
		c := &Coordinator{waiters: newWaiters(), durable: NewFrontier(2, 0, 0, 0, 0)}
		c.satisfyWaitersLocked()
		assertClearedTail(t, c.waiters)
	})
	t.Run("terminal failure", func(t *testing.T) {
		c := &Coordinator{waiters: newWaiters()}
		c.failWaitersLocked(ErrPublicationStopped)
		if c.waiters != nil {
			t.Fatalf("terminal waiter storage retained: len=%d cap=%d", len(c.waiters), cap(c.waiters))
		}
	})
}

func TestStopDropsWaiterBackingStorage(t *testing.T) {
	c, err := New(Options{Clock: NewFakeClock(time.Unix(150, 0)), Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- c.WaitThrough(context.Background(), 1) }()
	waitFor(t, func() bool { return c.Stats().WaiterCount == 1 && c.Stats().PublishCalls == 1 })
	if err := c.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop=%v", err)
	}
	if err := <-waitDone; !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("wait after stop=%v", err)
	}
	c.mu.Lock()
	waiters := c.waiters
	c.mu.Unlock()
	if waiters != nil {
		t.Fatalf("stop retained waiter storage: len=%d cap=%d", len(waiters), cap(waiters))
	}
}

func TestSupersedeRetainsDebtWaitersAndDeterministicObligationUnion(t *testing.T) {
	calls := make(chan *PreparedRootCandidate, 1)
	release := make(chan struct{})
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate
		select {
		case <-release:
			return PublishResult{Outcome: PublishSucceeded}
		case <-ctx.Done():
			return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 11, 3, 1)); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- c.WaitThrough(context.Background(), 1) }()
	// The waiter wakes publication. Keep the first callback blocked, then enqueue
	// a later candidate; it must remain as a fresh window after commit 1.
	first := waitForCall(t, calls)
	if first.Frontier().CommitSeq() != 1 {
		t.Fatalf("first seq=%d", first.Frontier().CommitSeq())
	}
	if err := c.Supersede(context.Background(), candidate(t, 2, 13, 2, 1)); err != nil {
		t.Fatal(err)
	}
	if got := c.Stats().PendingBytes; got != 24 {
		t.Fatalf("pending bytes=%d", got)
	}
	close(release)
	if err := <-waitDone; err != nil {
		t.Fatal(err)
	}
	// A second waiter captures the remaining window. The first debt was removed;
	// the second candidate keeps its own deterministic normalized obligations.
	secondDone := make(chan error, 1)
	go func() { secondDone <- c.WaitThrough(context.Background(), 2) }()
	second := waitForCall(t, calls)
	if got, want := second.Obligations(), []ObligationID{{1}, {2}}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("second obligations=%v want=%v", got, want)
	}
	if second.DependencyBytes() != 13 {
		t.Fatalf("second debt=%d", second.DependencyBytes())
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestCoalescedSupersedeUnionsAllUncoveredObligations(t *testing.T) {
	calls := make(chan *PreparedRootCandidate, 1)
	c, err := New(Options{Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 7, 3, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.Supersede(context.Background(), candidate(t, 2, 9, 2, 3)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.WaitThrough(context.Background(), 2) }()
	got := waitForCall(t, calls)
	if got.DependencyBytes() != 16 {
		t.Fatalf("coalesced dependency bytes=%d", got.DependencyBytes())
	}
	want := []ObligationID{{1}, {2}, {3}}
	if obligations := got.Obligations(); len(obligations) != 3 || obligations[0] != want[0] || obligations[1] != want[1] || obligations[2] != want[2] {
		t.Fatalf("coalesced obligations=%v want=%v", obligations, want)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTimerAnchorsFirstCandidateAndAdaptiveDelay(t *testing.T) {
	clock := NewFakeClock(time.Unix(100, 0))
	calls := make(chan uint64, 4)
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate.Frontier().CommitSeq()
		clock.Advance(3 * time.Millisecond)
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Millisecond)
	if err := c.Supersede(context.Background(), candidate(t, 2, 1)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(4 * time.Millisecond)
	select {
	case seq := <-calls:
		t.Fatalf("timer reset/early call seq=%d", seq)
	default:
	}
	clock.Advance(time.Millisecond)
	select {
	case seq := <-calls:
		if seq != 2 {
			t.Fatalf("coalesced timer seq=%d", seq)
		}
	case <-time.After(time.Second):
		t.Fatal("anchored timer did not fire")
	}
	waitFor(t, func() bool { return c.Stats().DurableCommitSeq == 2 })
	if delay := c.Stats().PublishDelay; delay != 60*time.Millisecond {
		t.Fatalf("adaptive delay=%s", delay)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 3, 1)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(59 * time.Millisecond)
	select {
	case seq := <-calls:
		t.Fatalf("adaptive timer early seq=%d", seq)
	default:
	}
	clock.Advance(time.Millisecond)
	select {
	case seq := <-calls:
		if seq != 3 {
			t.Fatalf("seq=%d", seq)
		}
	case <-time.After(time.Second):
		t.Fatal("adaptive timer did not fire")
	}
}

func TestSatisfiedInFlightWaiterDoesNotLeakImmediateWake(t *testing.T) {
	clock := NewFakeClock(time.Unix(200, 0))
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	calls := make(chan uint64, 2)
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		seq := candidate.Frontier().CommitSeq()
		calls <- seq
		if seq == 1 {
			close(firstEntered)
			select {
			case <-firstRelease:
			case <-ctx.Done():
				return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
			}
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- c.WaitThrough(context.Background(), 1) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first publication did not start")
	}
	if seq := <-calls; seq != 1 {
		t.Fatalf("first publication seq=%d", seq)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- c.WaitThrough(context.Background(), 1) }()
	waitFor(t, func() bool { return c.Stats().WaiterCount == 2 })
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		stats := c.Stats()
		return stats.DurableCommitSeq == 1 && stats.PendingCommits == 0
	})
	c.mu.Lock()
	publishNow, wakeReason := c.publishNow, c.wakeReason
	c.mu.Unlock()
	if publishNow || wakeReason != WakeNone {
		t.Fatalf("satisfied waiter leaked wake: publishNow=%t reason=%q", publishNow, wakeReason)
	}

	if err := c.Enqueue(context.Background(), candidate(t, 2, 1)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	publishNow, timerInstalled := c.publishNow, c.timer != nil
	c.mu.Unlock()
	if publishNow || !timerInstalled {
		t.Fatalf("below-soft enqueue skipped timer: publishNow=%t timerInstalled=%t", publishNow, timerInstalled)
	}
	clock.Advance(minPublishDelay - time.Millisecond)
	select {
	case seq := <-calls:
		t.Fatalf("seq %d published before timer window", seq)
	default:
	}
	clock.Advance(time.Millisecond)
	select {
	case seq := <-calls:
		if seq != 2 {
			t.Fatalf("timer publication seq=%d", seq)
		}
	case <-time.After(time.Second):
		t.Fatal("timer publication did not start")
	}
}

func TestSuccessfulPublishPreservesRealRemainingImmediateTriggers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bytes uint64
		wake  func(*Coordinator) <-chan error
	}{
		{name: "waiter", bytes: 1, wake: func(c *Coordinator) <-chan error {
			done := make(chan error, 1)
			go func() { done <- c.WaitThrough(context.Background(), 2) }()
			waitFor(t, func() bool { return c.Stats().WaiterCount == 2 })
			return done
		}},
		{name: "drain", bytes: 1, wake: func(c *Coordinator) <-chan error {
			done := make(chan error, 1)
			go func() { done <- c.Drain(context.Background()) }()
			waitFor(t, func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return len(c.drains) == 1 && c.publishNow
			})
			return done
		}},
		{name: "soft debt", bytes: SoftPendingBytes, wake: func(c *Coordinator) <-chan error {
			waitFor(t, func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return c.publishNow && c.wakeReason == WakeSoftBytes
			})
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := NewFakeClock(time.Unix(300, 0))
			firstEntered := make(chan struct{})
			firstRelease := make(chan struct{})
			calls := make(chan uint64, 2)
			c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
				seq := candidate.Frontier().CommitSeq()
				calls <- seq
				if seq == 1 {
					close(firstEntered)
					select {
					case <-firstRelease:
					case <-ctx.Done():
						return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
					}
				}
				return PublishResult{Outcome: PublishSucceeded}
			})})
			if err != nil {
				t.Fatal(err)
			}
			defer stopClean(t, c)
			if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
				t.Fatal(err)
			}
			firstDone := make(chan error, 1)
			go func() { firstDone <- c.WaitThrough(context.Background(), 1) }()
			select {
			case <-firstEntered:
			case <-time.After(time.Second):
				t.Fatal("first publication did not start")
			}
			if seq := <-calls; seq != 1 {
				t.Fatalf("first publication seq=%d", seq)
			}
			if err := c.Supersede(context.Background(), candidate(t, 2, tc.bytes)); err != nil {
				t.Fatal(err)
			}
			triggerDone := tc.wake(c)
			close(firstRelease)
			select {
			case seq := <-calls:
				if seq != 2 {
					t.Fatalf("remaining immediate publication seq=%d", seq)
				}
			case <-time.After(time.Second):
				t.Fatal("remaining immediate trigger was lost")
			}
			if err := <-firstDone; err != nil {
				t.Fatal(err)
			}
			if triggerDone != nil {
				if err := <-triggerDone; err != nil {
					t.Fatal(err)
				}
			} else {
				waitFor(t, func() bool { return c.Stats().DurableCommitSeq == 2 })
			}
		})
	}
}

func TestHardAdmissionBlocksBeforeAcknowledgement(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		entered <- struct{}{}
		select {
		case <-release:
			return PublishResult{Outcome: PublishSucceeded}
		case <-ctx.Done():
			return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	done := make(chan error, 1)
	go func() { done <- c.Enqueue(context.Background(), candidate(t, 1, HardPendingBytes+1)) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("hard admission did not trigger publish")
	}
	select {
	case err := <-done:
		t.Fatalf("hard enqueue acknowledged before durable progress: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c.Stats().AdmissionWaits != 1 {
		t.Fatalf("stats=%+v", c.Stats())
	}
}

func TestExactHardByteBoundaryAcknowledgesWithoutWaiting(t *testing.T) {
	if HardPendingCommits != 65_536 || HardPendingBytes != 256<<20 || SoftPendingBytes != 64<<20 {
		t.Fatalf("scheduler constants soft=%d hard-bytes=%d hard-commits=%d", SoftPendingBytes, HardPendingBytes, HardPendingCommits)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	c, err := New(Options{Clock: NewFakeClock(time.Unix(250, 0)), Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		entered <- struct{}{}
		select {
		case <-release:
			return PublishResult{Outcome: PublishSucceeded}
		case <-ctx.Done():
			return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, HardPendingBytes)); err != nil {
		t.Fatalf("exact hard boundary waited/failed: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("soft trigger did not start publisher")
	}
	done := make(chan error, 1)
	go func() { done <- c.Supersede(context.Background(), candidate(t, 2, 1)) }()
	waitFor(t, func() bool { return c.Stats().AdmissionWaits == 1 })
	select {
	case err := <-done:
		t.Fatalf("hard crossing acknowledged before progress: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestExactHardCommitBoundaryAcknowledgesThenNextCommitWaits(t *testing.T) {
	clock := NewFakeClock(time.Unix(275, 0))
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		entered <- struct{}{}
		select {
		case <-release:
			return PublishResult{Outcome: PublishSucceeded}
		case <-ctx.Done():
			return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	for seq := uint64(1); seq <= HardPendingCommits; seq++ {
		if err := c.Enqueue(context.Background(), candidate(t, seq, 0)); err != nil {
			t.Fatalf("exact-boundary seq=%d: %v", seq, err)
		}
	}
	if stats := c.Stats(); stats.PendingCommits != HardPendingCommits || stats.AdmissionWaits != 0 {
		t.Fatalf("exact commit boundary stats=%+v", stats)
	}
	done := make(chan error, 1)
	go func() { done <- c.Supersede(context.Background(), candidate(t, HardPendingCommits+1, 0)) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("commit hard crossing did not publish")
	}
	waitFor(t, func() bool { return c.Stats().AdmissionWaits == 1 })
	select {
	case err := <-done:
		t.Fatalf("65,537th commit acknowledged before progress: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHardAdmissionReturnsPublisherErrorOrContext(t *testing.T) {
	want := errors.New("pre-meta")
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishRetryableFailure, Err: want}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, HardPendingBytes+1)); !errors.Is(err, want) {
		t.Fatalf("hard error=%v", err)
	}

	stalled, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, stalled)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := stalled.Enqueue(ctx, candidate(t, 1, HardPendingBytes+1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("hard context error=%v", err)
	}
}

func TestRetryableFailureRetainsDebtAndFailsOnlyCapturedWaiters(t *testing.T) {
	want := errors.New("sync dependency")
	var attempt atomic.Uint64
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		if attempt.Add(1) == 1 {
			return PublishResult{Outcome: PublishRetryableFailure, Err: want}
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 99)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(context.Background(), 1); !errors.Is(err, want) {
		t.Fatalf("first wait=%v", err)
	}
	stats := c.Stats()
	if stats.PendingCommits != 1 || stats.PendingBytes != 99 || stats.PreMetaFailures != 1 || stats.WaiterCount != 0 {
		t.Fatalf("retained stats=%+v", stats)
	}
	if err := c.WaitThrough(context.Background(), 1); err != nil {
		t.Fatalf("retry wait=%v", err)
	}
	if stats := c.Stats(); stats.PendingCommits != 0 || stats.Retries != 1 {
		t.Fatalf("retry stats=%+v", stats)
	}
}

func TestRetryableFailureDoesNotFailWaiterAddedInFlight(t *testing.T) {
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})
	secondRelease := make(chan struct{})
	want := errors.New("first pre-meta failure")
	var calls atomic.Uint64
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		switch calls.Add(1) {
		case 1:
			close(firstEntered)
			select {
			case <-firstRelease:
				return PublishResult{Outcome: PublishRetryableFailure, Err: want}
			case <-ctx.Done():
				return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
			}
		default:
			close(secondEntered)
			select {
			case <-secondRelease:
				return PublishResult{Outcome: PublishSucceeded}
			case <-ctx.Done():
				return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
			}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- c.WaitThrough(context.Background(), 1) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first attempt did not start")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- c.WaitThrough(context.Background(), 1) }()
	waitFor(t, func() bool { return c.Stats().WaiterCount == 2 })
	close(firstRelease)
	if err := <-firstDone; !errors.Is(err, want) {
		t.Fatalf("captured waiter error=%v", err)
	}
	select {
	case err := <-secondDone:
		t.Fatalf("in-flight waiter received prior failure: %v", err)
	default:
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("in-flight waiter did not trigger retry")
	}
	close(secondRelease)
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledInFlightWaiterDoesNotLeakRetryWake(t *testing.T) {
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	calls := make(chan uint64, 2)
	want := errors.New("first pre-meta failure")
	var attempts atomic.Uint64
	c, err := New(Options{Clock: NewFakeClock(time.Unix(400, 0)), Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate.Frontier().CommitSeq()
		if attempts.Add(1) == 1 {
			close(firstEntered)
			<-firstRelease
			return PublishResult{Outcome: PublishRetryableFailure, Err: want}
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- c.WaitThrough(context.Background(), 1) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first publication did not start")
	}
	if seq := <-calls; seq != 1 {
		t.Fatalf("first publication seq=%d", seq)
	}
	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- c.WaitThrough(ctx, 1) }()
	waitFor(t, func() bool { return c.Stats().WaiterCount == 2 })
	builder, err := c.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	close(firstRelease)
	if err := <-firstDone; !errors.Is(err, want) {
		t.Fatalf("first wait=%v", err)
	}
	waitFor(t, func() bool { return !c.Stats().Poisoned && c.Stats().PreMetaFailures == 1 })
	cancel()
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("second wait=%v", err)
	}
	c.mu.Lock()
	publishNow, wakeReason, timerInstalled := c.publishNow, c.wakeReason, c.timer != nil
	c.mu.Unlock()
	if publishNow || wakeReason != WakeNone || timerInstalled {
		t.Fatalf("canceled in-flight waiter leaked retry: publishNow=%t reason=%q timerInstalled=%t", publishNow, wakeReason, timerInstalled)
	}
	builder.Release()
	select {
	case seq := <-calls:
		t.Fatalf("canceled in-flight waiter retried seq %d", seq)
	default:
	}
	if err := c.WaitThrough(context.Background(), 1); err != nil {
		t.Fatalf("explicit retry=%v", err)
	}
	if seq := <-calls; seq != 1 {
		t.Fatalf("explicit retry seq=%d", seq)
	}
}

func TestInFlightEnqueueCrossingSoftBoundaryRetainsRetryWake(t *testing.T) {
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})
	want := errors.New("first pre-meta failure")
	var attempts atomic.Uint64
	c, err := New(Options{Clock: NewFakeClock(time.Unix(410, 0)), Publisher: PublisherFunc(func(_ context.Context, _ *PreparedRootCandidate) PublishResult {
		if attempts.Add(1) == 1 {
			close(firstEntered)
			<-firstRelease
			return PublishResult{Outcome: PublishRetryableFailure, Err: want}
		}
		close(secondEntered)
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, SoftPendingBytes-(1<<20))); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- c.WaitThrough(context.Background(), 1) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first publication did not start")
	}
	if err := c.Supersede(context.Background(), candidate(t, 2, 2<<20)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pendingBytes, publishNow, wakeReason := c.pendingBytes, c.publishNow, c.wakeReason
	c.mu.Unlock()
	if pendingBytes < SoftPendingBytes || !publishNow || wakeReason != WakeSoftBytes {
		t.Fatalf("in-flight soft crossing not recorded: bytes=%d publishNow=%t reason=%q", pendingBytes, publishNow, wakeReason)
	}
	close(firstRelease)
	if err := <-firstDone; !errors.Is(err, want) {
		t.Fatalf("first wait=%v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("fresh soft crossing did not trigger retry")
	}
	waitFor(t, func() bool { return c.Stats().DurableCommitSeq == 2 })
}

func TestDrainIdentityTurnoverPreservesFreshInFlightDrain(t *testing.T) {
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})
	want := errors.New("first pre-meta failure")
	var attempts atomic.Uint64
	c, err := New(Options{Clock: NewFakeClock(time.Unix(420, 0)), Publisher: PublisherFunc(func(_ context.Context, _ *PreparedRootCandidate) PublishResult {
		if attempts.Add(1) == 1 {
			close(firstEntered)
			<-firstRelease
			return PublishResult{Outcome: PublishRetryableFailure, Err: want}
		}
		close(secondEntered)
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- c.Drain(firstCtx) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first drain did not start publication")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- c.Drain(context.Background()) }()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.drains) == 2 && c.publishNow && c.wakeReason == WakeDrain
	})
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("captured drain cancellation=%v", err)
	}
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.drains) == 1 && c.publishNow && c.wakeReason == WakeDrain
	})
	close(firstRelease)
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("fresh drain was stranded by captured drain turnover")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("fresh drain=%v", err)
	}
}

func TestErroredCapturedDrainDoesNotOwnRetryWakeAfterFreshDrainEnds(t *testing.T) {
	want := errors.New("captured drain failed")
	captured := &drainRequest{err: want}
	fresh := &drainRequest{}
	entry := pendingEntry{candidate: candidate(t, 1, 1), bytes: 1}
	c := &Coordinator{
		clock: NewFakeClock(time.Unix(430, 0)), pending: []pendingEntry{entry}, pendingBytes: 1,
		firstPendingAt: time.Unix(430, 0), drains: []*drainRequest{captured, fresh},
		publishNow: true, wakeReason: WakeDrain, lastRetryableError: want,
		retryAttempt: PublishAttempt{groupSize: 1, drains: []*drainRequest{captured}},
	}
	c.endDrain(fresh)
	if len(c.drains) != 1 || c.drains[0] != captured {
		t.Fatalf("drain ownership after fresh removal=%v", c.drains)
	}
	if c.publishNow || c.wakeReason != WakeNone || c.timer != nil {
		t.Fatalf("errored captured drain retained retry wake: publishNow=%t reason=%q timer=%v", c.publishNow, c.wakeReason, c.timer)
	}
}

func TestHardAdmissionAddedInFlightIgnoresPriorGroupFailure(t *testing.T) {
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})
	secondRelease := make(chan struct{})
	priorFailure := errors.New("prior group failed")
	var calls atomic.Uint64
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		if calls.Add(1) == 1 {
			close(firstEntered)
			select {
			case <-firstRelease:
				return PublishResult{Outcome: PublishRetryableFailure, Err: priorFailure}
			case <-ctx.Done():
				return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
			}
		}
		close(secondEntered)
		select {
		case <-secondRelease:
			return PublishResult{Outcome: PublishSucceeded}
		case <-ctx.Done():
			return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, SoftPendingBytes)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first group did not start")
	}
	hardDone := make(chan error, 1)
	go func() { hardDone <- c.Supersede(context.Background(), candidate(t, 2, HardPendingBytes)) }()
	waitFor(t, func() bool { return c.Stats().AdmissionWaits == 1 })
	close(firstRelease)
	select {
	case err := <-hardDone:
		t.Fatalf("later hard admission received prior failure: %v", err)
	default:
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("later hard group did not publish")
	}
	close(secondRelease)
	if err := <-hardDone; err != nil {
		t.Fatal(err)
	}
}

func TestAmbiguousResultPoisonsFutureOperations(t *testing.T) {
	want := errors.New("meta sync unknown")
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishAmbiguous, Err: want}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(context.Background(), 1); !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, want) {
		t.Fatalf("poison wait=%v", err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 2, 1)); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("enqueue after poison=%v", err)
	}
	if _, err := c.AcquireBuilder(context.Background()); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("admission after poison=%v", err)
	}
	if err := c.Drain(context.Background()); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("drain after poison=%v", err)
	}
}

func TestPoisonedWaitThroughFailsClosedBeforeDurableFastPath(t *testing.T) {
	want := errors.New("later meta sync unknown")
	c, err := New(Options{
		InitialDurableFrontier: NewFrontier(5, 50, 100, 5, 5),
		Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishAmbiguous, Err: want}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 6, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(context.Background(), 6); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("poisoning wait=%v", err)
	}
	if err := c.WaitThrough(context.Background(), 5); !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, want) {
		t.Fatalf("already-durable wait bypassed poison: %v", err)
	}
	if err := c.Enqueue(context.Background(), nil); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("invalid enqueue bypassed poison: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.AcquireBuilder(canceled); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("canceled admission bypassed poison: %v", err)
	}
}

func TestStopCancelsStalledPublisherWithoutLeaks(t *testing.T) {
	entered := make(chan struct{})
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		close(entered)
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, SoftPendingBytes)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("publisher did not stall")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Stop(ctx); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop=%v", err)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("scheduler goroutine leaked")
	}
	if stats := c.Stats(); stats.WaiterCount != 0 || stats.ActiveBuilders != 0 {
		t.Fatalf("leaked ownership stats=%+v", stats)
	}
}

func TestStopReturnsSameDebtResultToEveryCaller(t *testing.T) {
	c, err := New(Options{Clock: NewFakeClock(time.Unix(280, 0)), Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("first stop=%v", err)
	}
	if err := c.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("second stop=%v", err)
	}
}

func TestConcurrentDrainAndStop(t *testing.T) {
	c, err := New(Options{Clock: NewFakeClock(time.Unix(290, 0)), Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := c.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	drained := make(chan error, 1)
	go func() { drained <- c.Drain(context.Background()) }()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.drains) == 1 && c.publishNow && c.wakeReason == WakeDrain
	})
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop(context.Background()) }()
	if err := <-drained; !errors.Is(err, ErrClosed) {
		t.Fatalf("drain during stop=%v", err)
	}
	if err := <-stopped; !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop with debt=%v", err)
	}
	builder.Release()
	c.mu.Lock()
	drains, publishNow, wakeReason := len(c.drains), c.publishNow, c.wakeReason
	c.mu.Unlock()
	if drains != 0 || publishNow || wakeReason != WakeNone {
		t.Fatalf("terminal ownership leaked: drains=%d publishNow=%t reason=%q", drains, publishNow, wakeReason)
	}
}

func TestNestedAdmissionUsesOneTokenAndCallbackRunsUnlocked(t *testing.T) {
	callback := make(chan struct{})
	var c *Coordinator
	candidateOne := candidate(t, 1, SoftPendingBytes)
	created, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, got *PreparedRootCandidate) PublishResult {
		if got != candidateOne {
			t.Errorf("candidate identity changed without coalescing")
		}
		_ = c.Stats()                       // coordinator lock is not held
		token, err := c.AcquireBuilder(ctx) // no admission/build lock is held
		if err != nil {
			t.Errorf("callback admission: %v", err)
		} else {
			token.Release()
		}
		close(callback)
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	c = created
	defer stopClean(t, c)
	outer, err := c.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	nested, err := outer.Nested()
	if err != nil {
		t.Fatal(err)
	}
	if c.Stats().ActiveBuilders != 1 {
		t.Fatalf("nested token double-counted: %+v", c.Stats())
	}
	if err := c.Enqueue(context.Background(), candidateOne); err != nil {
		t.Fatal(err)
	}
	select {
	case <-callback:
		t.Fatal("publisher ran with active builder")
	default:
	}
	outer.Release()
	select {
	case <-callback:
		t.Fatal("publisher ran before nested release")
	default:
	}
	nested.Release()
	select {
	case <-callback:
	case <-time.After(time.Second):
		t.Fatal("publisher deadlocked after nested release")
	}
}

func TestCopiedAndReleasedBuilderHandleCannotResurrectOrDoubleReleaseLease(t *testing.T) {
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	outer, err := c.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	child, err := outer.Nested()
	if err != nil {
		t.Fatal(err)
	}
	clone := *outer
	outer.Release()
	clone.Release()
	outer.handle.mu.Lock()
	retainsLease := outer.handle.lease != nil
	outer.handle.mu.Unlock()
	if retainsLease {
		t.Fatal("released copied handle retained shared lease")
	}
	if stats := c.Stats(); stats.ActiveBuilders != 1 {
		t.Fatalf("copied handle double-released shared lease: %+v", stats)
	}
	if _, err := outer.Nested(); !errors.Is(err, ErrClosed) {
		t.Fatalf("released outer nested again: %v", err)
	}
	if _, err := clone.Nested(); !errors.Is(err, ErrClosed) {
		t.Fatalf("released clone nested again: %v", err)
	}
	grandchild, err := child.Nested()
	if err != nil {
		t.Fatalf("live child could not nest: %v", err)
	}
	child.Release()
	if stats := c.Stats(); stats.ActiveBuilders != 1 {
		t.Fatalf("shared lease released before final live handle: %+v", stats)
	}
	grandchild.Release()
	if stats := c.Stats(); stats.ActiveBuilders != 0 {
		t.Fatalf("shared lease not released exactly once: %+v", stats)
	}
	if _, err := child.Nested(); !errors.Is(err, ErrClosed) {
		t.Fatalf("released child nested again: %v", err)
	}
}

func TestWaitCancellationRaceRechecksDurableFrontier(t *testing.T) {
	release := make(chan struct{})
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		<-release
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.WaitThrough(ctx, 1) }()
	close(release)
	waitFor(t, func() bool { return c.Stats().DurableCommitSeq == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("durable frontier lost cancellation race: %v", err)
	}
	if c.Stats().WaiterCount != 0 {
		t.Fatalf("waiter leaked: %+v", c.Stats())
	}
}

func TestCanceledDrainDoesNotLeavePermanentDrainMode(t *testing.T) {
	clock := NewFakeClock(time.Unix(200, 0))
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	calls := make(chan uint64, 4)
	var count atomic.Uint64
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate.Frontier().CommitSeq()
		if count.Add(1) == 1 {
			close(firstEntered)
			select {
			case <-firstRelease:
			case <-ctx.Done():
				return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
			}
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	drained := make(chan error, 1)
	go func() { drained <- c.Drain(ctx) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("drain did not publish")
	}
	cancel()
	if err := <-drained; !errors.Is(err, context.Canceled) {
		t.Fatalf("drain cancel=%v", err)
	}
	if err := c.Supersede(context.Background(), candidate(t, 2, 1)); err != nil {
		t.Fatal(err)
	}
	close(firstRelease)
	for c.Stats().DurableCommitSeq != 1 {
		time.Sleep(time.Millisecond)
	}
	select {
	case seq := <-calls:
		if seq != 1 {
			t.Fatalf("first call=%d", seq)
		}
	default:
	}
	select {
	case seq := <-calls:
		t.Fatalf("canceled drain auto-published remaining seq=%d", seq)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestCanceledDrainRestoresAnchoredTimerInsteadOfLeakingImmediateWake(t *testing.T) {
	clock := NewFakeClock(time.Unix(350, 0))
	calls := make(chan uint64, 1)
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate.Frontier().CommitSeq()
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	builder, err := c.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	drained := make(chan error, 1)
	go func() { drained <- c.Drain(ctx) }()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.drains) == 1 && c.publishNow && c.wakeReason == WakeDrain
	})
	cancel()
	if err := <-drained; !errors.Is(err, context.Canceled) {
		t.Fatalf("drain=%v", err)
	}
	c.mu.Lock()
	publishNow, wakeReason, timerInstalled := c.publishNow, c.wakeReason, c.timer != nil
	c.mu.Unlock()
	if publishNow || wakeReason != WakeNone || !timerInstalled {
		t.Fatalf("canceled drain wake retained: publishNow=%t reason=%q timerInstalled=%t", publishNow, wakeReason, timerInstalled)
	}
	builder.Release()
	select {
	case seq := <-calls:
		t.Fatalf("canceled drain published seq %d immediately", seq)
	default:
	}
	clock.Advance(4 * time.Millisecond)
	select {
	case seq := <-calls:
		t.Fatalf("canceled drain published seq %d before anchored timer", seq)
	default:
	}
	clock.Advance(time.Millisecond)
	select {
	case seq := <-calls:
		if seq != 1 {
			t.Fatalf("timer publication seq=%d", seq)
		}
	case <-time.After(time.Second):
		t.Fatal("restored anchored timer did not publish")
	}
}

func TestCanceledWaiterRestoresAnchoredTimerInsteadOfLeakingImmediateWake(t *testing.T) {
	clock := NewFakeClock(time.Unix(360, 0))
	calls := make(chan uint64, 1)
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		calls <- candidate.Frontier().CommitSeq()
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	builder, err := c.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan error, 1)
	go func() { waitDone <- c.WaitThrough(ctx, 1) }()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.waiters) == 1 && c.publishNow && c.wakeReason == WakeWaiter
	})
	cancel()
	if err := <-waitDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait=%v", err)
	}
	c.mu.Lock()
	publishNow, wakeReason, timerInstalled := c.publishNow, c.wakeReason, c.timer != nil
	c.mu.Unlock()
	if publishNow || wakeReason != WakeNone || !timerInstalled {
		t.Fatalf("canceled waiter wake retained: publishNow=%t reason=%q timerInstalled=%t", publishNow, wakeReason, timerInstalled)
	}
	builder.Release()
	clock.Advance(4 * time.Millisecond)
	select {
	case seq := <-calls:
		t.Fatalf("canceled waiter published seq %d before anchored timer", seq)
	default:
	}
	clock.Advance(time.Millisecond)
	select {
	case seq := <-calls:
		if seq != 1 {
			t.Fatalf("timer publication seq=%d", seq)
		}
	case <-time.After(time.Second):
		t.Fatal("restored anchored timer did not publish")
	}
}

func TestCanceledExplicitWakePublishesWhenAnchoredDeadlineElapsed(t *testing.T) {
	for _, kind := range []string{"waiter", "drain"} {
		t.Run(kind, func(t *testing.T) {
			clock := NewFakeClock(time.Unix(370, 0))
			calls := make(chan uint64, 1)
			c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
				calls <- candidate.Frontier().CommitSeq()
				return PublishResult{Outcome: PublishSucceeded}
			})})
			if err != nil {
				t.Fatal(err)
			}
			defer stopClean(t, c)
			builder, err := c.AcquireBuilder(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			if kind == "waiter" {
				go func() { done <- c.WaitThrough(ctx, 1) }()
				waitFor(t, func() bool { return c.Stats().WaiterCount == 1 })
			} else {
				go func() { done <- c.Drain(ctx) }()
				waitFor(t, func() bool {
					c.mu.Lock()
					defer c.mu.Unlock()
					return len(c.drains) == 1
				})
			}
			clock.Advance(minPublishDelay + time.Millisecond)
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled %s=%v", kind, err)
			}
			c.mu.Lock()
			publishNow, wakeReason := c.publishNow, c.wakeReason
			c.mu.Unlock()
			if !publishNow || wakeReason != WakeTimer {
				t.Fatalf("elapsed deadline not restored: publishNow=%t reason=%q", publishNow, wakeReason)
			}
			builder.Release()
			select {
			case seq := <-calls:
				if seq != 1 {
					t.Fatalf("timer publication seq=%d", seq)
				}
			case <-time.After(time.Second):
				t.Fatal("elapsed anchored deadline did not publish")
			}
		})
	}
}

func TestReportPublishResultRejectsStaleMismatchedAndDuplicateAttempts(t *testing.T) {
	var saved PublishAttempt
	var c *Coordinator
	created, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		c.mu.Lock()
		saved = c.activeAttempt
		c.mu.Unlock()
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	c = created
	defer stopClean(t, c)
	bogus := PublishAttempt{id: 1, candidate: candidate(t, 1, 1), groupSize: 1}
	if err := c.ReportPublishResult(bogus, PublishResult{Outcome: PublishSucceeded}); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatalf("bogus report=%v", err)
	}
	if stats := c.Stats(); stats.DurableCommitSeq != 0 || stats.PendingCommits != 0 {
		t.Fatalf("bogus report mutated state: %+v", stats)
	}
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := c.ReportPublishResult(saved, PublishResult{Outcome: PublishSucceeded}); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatalf("duplicate report=%v", err)
	}
	mismatched := saved
	mismatched.groupSize++
	if err := c.ReportPublishResult(mismatched, PublishResult{Outcome: PublishSucceeded}); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatalf("mismatched report=%v", err)
	}
	if stats := c.Stats(); stats.DurableCommitSeq != 1 || stats.PendingCommits != 0 {
		t.Fatalf("stale reports mutated state: %+v", stats)
	}
}

func TestSuccessCannotClaimDifferentDurableSequence(t *testing.T) {
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded, DurableCommitSeq: 2}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(context.Background(), 1); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatalf("mixed frontier result=%v", err)
	}
	if stats := c.Stats(); stats.DurableCommitSeq != 0 || stats.PendingCommits != 1 {
		t.Fatalf("mixed frontier mutated state: %+v", stats)
	}
}

func TestOldestRecoverableMustAdvanceMonotonicallyWithinDurableFrontier(t *testing.T) {
	for _, test := range []struct {
		name   string
		oldest uint64
	}{{"regression", 2}, {"beyond-durable", 7}} {
		t.Run(test.name, func(t *testing.T) {
			c, err := New(Options{
				InitialDurableFrontier: NewFrontier(5, 5, 5, 5, 5), OldestRecoverableCommitSeq: 3,
				Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
					return PublishResult{Outcome: PublishSucceeded, OldestRecoverableCommitSeq: test.oldest}
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer stopClean(t, c)
			if err := c.Enqueue(context.Background(), candidate(t, 6, 1)); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitThrough(context.Background(), 6); !errors.Is(err, ErrPublisherProtocol) {
				t.Fatalf("oldest=%d error=%v", test.oldest, err)
			}
			if stats := c.Stats(); stats.DurableCommitSeq != 5 || stats.OldestRecoverableCommitSeq != 3 || stats.PendingCommits != 1 {
				t.Fatalf("invalid oldest mutated state: %+v", stats)
			}
		})
	}
	c, err := New(Options{
		InitialDurableFrontier: NewFrontier(5, 5, 5, 5, 5), OldestRecoverableCommitSeq: 3,
		Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded, OldestRecoverableCommitSeq: 4}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.Enqueue(context.Background(), candidate(t, 6, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(context.Background(), 6); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.DurableCommitSeq != 6 || stats.OldestRecoverableCommitSeq != 4 {
		t.Fatalf("valid oldest stats=%+v", stats)
	}
}

func TestRandomizedCoordinatorModel(t *testing.T) {
	rng := rand.New(rand.NewSource(3675))
	clock := NewFakeClock(time.Unix(300, 0))
	var mu sync.Mutex
	next := PublishResult{Outcome: PublishSucceeded}
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		mu.Lock()
		result := next
		mu.Unlock()
		return result
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	var seq, durable, pendingBytes, pendingCommits uint64
	for step := 0; step < 400; step++ {
		seq++
		bytes := uint64(rng.Intn(1024) + 1)
		hardAdmission := rng.Intn(25) == 0
		if hardAdmission {
			bytes = HardPendingBytes + 1
			mu.Lock()
			next = PublishResult{Outcome: PublishSucceeded}
			mu.Unlock()
		}
		prepared := candidate(t, seq, bytes, byte(rng.Intn(16)))
		var enqueueErr error
		if pendingCommits == 0 || rng.Intn(2) == 0 {
			enqueueErr = c.Enqueue(context.Background(), prepared)
		} else {
			enqueueErr = c.Supersede(context.Background(), prepared)
		}
		if enqueueErr != nil {
			t.Fatalf("step %d enqueue: %v", step, enqueueErr)
		}
		pendingBytes += bytes
		pendingCommits++
		if hardAdmission {
			durable = seq
			pendingBytes, pendingCommits = 0, 0
		}
		if !hardAdmission && rng.Intn(4) == 0 {
			failure := rng.Intn(5) == 0
			mu.Lock()
			if failure {
				next = PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("model retry")}
			} else {
				next = PublishResult{Outcome: PublishSucceeded}
			}
			mu.Unlock()
			err := c.WaitThrough(context.Background(), seq)
			if failure {
				if err == nil {
					t.Fatalf("step %d expected retry failure", step)
				}
				stats := c.Stats()
				if stats.PendingBytes != pendingBytes || stats.PendingCommits != pendingCommits {
					t.Fatalf("step %d retry lost debt: %+v", step, stats)
				}
				mu.Lock()
				next = PublishResult{Outcome: PublishSucceeded}
				mu.Unlock()
				if err := c.WaitThrough(context.Background(), seq); err != nil {
					t.Fatalf("step %d retry: %v", step, err)
				}
			}
			durable = seq
			pendingBytes, pendingCommits = 0, 0
		}
		if rng.Intn(8) == 0 {
			outer, err := c.AcquireBuilder(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			nested, err := outer.Nested()
			if err != nil {
				t.Fatal(err)
			}
			if rng.Intn(2) == 0 {
				outer.Release()
				nested.Release()
			} else {
				nested.Release()
				outer.Release()
			}
		}
		stats := c.Stats()
		if stats.VisibleCommitSeq != seq || stats.DurableCommitSeq != durable || stats.PendingBytes != pendingBytes || stats.PendingCommits != pendingCommits || stats.ActiveBuilders != 0 {
			t.Fatalf("step %d model=%d/%d/%d/%d stats=%+v", step, seq, durable, pendingBytes, pendingCommits, stats)
		}
	}
	mu.Lock()
	next = PublishResult{Outcome: PublishSucceeded}
	mu.Unlock()
	if err := c.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.DurableCommitSeq != seq || stats.PendingCommits != 0 {
		t.Fatalf("final stats=%+v", stats)
	}
}

func TestRandomizedPoisonAdmissionAndShutdownModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1595_3675))
	for cycle := 0; cycle < 80; cycle++ {
		clock := NewFakeClock(time.Unix(int64(500+cycle), 0))
		poison := rng.Intn(7) == 0
		var publishCalls atomic.Uint64
		c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			publishCalls.Add(1)
			if poison {
				return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("model ambiguous meta")}
			}
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		seq := uint64(1)
		outer, err := c.AcquireBuilder(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		nested, err := outer.Nested()
		if err != nil {
			t.Fatal(err)
		}
		prepared := candidate(t, seq, HardPendingBytes+1, byte(cycle))
		done := make(chan error, 1)
		go func() { done <- c.Enqueue(context.Background(), prepared) }()
		select {
		case err := <-done:
			t.Fatalf("cycle %d hard admission passed active builder: %v", cycle, err)
		default:
		}
		if rng.Intn(2) == 0 {
			outer.Release()
			nested.Release()
		} else {
			nested.Release()
			outer.Release()
		}
		err = <-done
		if poison {
			if !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("cycle %d poison=%v", cycle, err)
			}
			if err := c.WaitThrough(context.Background(), seq); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("cycle %d poison wait=%v", cycle, err)
			}
		} else if err != nil {
			t.Fatalf("cycle %d hard admission=%v", cycle, err)
		}
		if rng.Intn(2) == 0 && !poison {
			if err := c.Drain(context.Background()); err != nil {
				t.Fatalf("cycle %d drain=%v", cycle, err)
			}
			if err := c.Stop(context.Background()); err != nil {
				t.Fatalf("cycle %d drained stop=%v", cycle, err)
			}
		} else {
			err := c.Stop(context.Background())
			if poison && !errors.Is(err, ErrPublicationStopped) {
				t.Fatalf("cycle %d poison stop=%v", cycle, err)
			}
		}
		if _, err := c.AcquireBuilder(context.Background()); !errors.Is(err, ErrClosed) && !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("cycle %d admission after shutdown=%v", cycle, err)
		}
		if stats := c.Stats(); stats.ActiveBuilders != 0 || stats.WaiterCount != 0 || publishCalls.Load() == 0 {
			t.Fatalf("cycle %d shutdown stats=%+v calls=%d", cycle, stats, publishCalls.Load())
		}
	}
}

func TestOrdinaryEnqueueHasZeroStableIOAndNoPerEnqueueGoroutine(t *testing.T) {
	clock := NewFakeClock(time.Unix(400, 0))
	var calls atomic.Uint64
	before := runtime.NumGoroutine()
	c, err := New(Options{Clock: clock, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		calls.Add(1)
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 10_000; seq++ {
		if err := c.Enqueue(context.Background(), candidate(t, seq, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("ordinary enqueue made %d stable-I/O calls", calls.Load())
	}
	if delta := runtime.NumGoroutine() - before; delta > 2 {
		t.Fatalf("enqueue goroutine growth=%d", delta)
	}
	stopClean(t, c)
}
