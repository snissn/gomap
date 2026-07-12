package rootpublication

import (
	"context"
	"errors"
	"math/rand"
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
	for c.Stats().DurableCommitSeq != 2 {
		time.Sleep(time.Millisecond)
	}
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
	for c.Stats().DurableCommitSeq != 1 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("durable frontier lost cancellation race: %v", err)
	}
	if c.Stats().WaiterCount != 0 {
		t.Fatalf("waiter leaked: %+v", c.Stats())
	}
}

func TestCanceledDrainDoesNotLeavePermanentDrainMode(t *testing.T) {
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	calls := make(chan uint64, 4)
	var count atomic.Uint64
	c, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
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

func TestReportPublishResultRejectsStaleMismatchedAndDuplicateAttempts(t *testing.T) {
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	bogus := PublishAttempt{id: 1, candidate: candidate(t, 1, 1), groupSize: 1}
	if err := c.ReportPublishResult(bogus, PublishResult{Outcome: PublishSucceeded}); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatalf("bogus report=%v", err)
	}
	if stats := c.Stats(); stats.DurableCommitSeq != 0 || stats.PendingCommits != 0 {
		t.Fatalf("bogus report mutated state: %+v", stats)
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

func TestRandomizedCoordinatorModel(t *testing.T) {
	rng := rand.New(rand.NewSource(3675))
	var mu sync.Mutex
	next := PublishResult{Outcome: PublishSucceeded}
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
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
		if rng.Intn(4) == 0 {
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
