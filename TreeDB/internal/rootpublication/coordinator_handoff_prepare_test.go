package rootpublication

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type testPreparingPublisher struct {
	prepare func(context.Context, *PreparedRootCandidate) error
	publish func(context.Context, *PreparedRootCandidate) PublishResult
}

func (publisher *testPreparingPublisher) Prepare(ctx context.Context, candidate *PreparedRootCandidate) error {
	return publisher.prepare(ctx, candidate)
}

func (publisher *testPreparingPublisher) Publish(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
	return publisher.publish(ctx, candidate)
}

func candidateWithEmptyResourceSet(t testing.TB, seq, bytes uint64) (*PreparedRootCandidate, *StableResourceSet) {
	t.Helper()
	set, err := NewStableResourceSetBuilder().Freeze()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := NewPreparedRootCandidate(CandidateSpec{
		Frontier: NewFrontier(seq, seq*10, seq*20, seq, seq), DependencyBytes: bytes, ResourceSet: set,
	})
	if err != nil {
		t.Fatal(err)
	}
	return prepared, set
}

func TestEnqueueBuiltRejectsForeignReleasedAndNestedLeasesBeforeCandidateMutation(t *testing.T) {
	newCoordinator := func(t testing.TB) *Coordinator {
		t.Helper()
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stopClean(t, coordinator) })
		return coordinator
	}

	t.Run("foreign", func(t *testing.T) {
		coordinator := newCoordinator(t)
		foreign := newCoordinator(t)
		token, err := foreign.AcquireBuilder(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer token.Release()
		prepared, set := candidateWithEmptyResourceSet(t, 1, 1)
		defer prepared.AbandonResources()
		if _, err := coordinator.EnqueueBuilt(prepared, token); !errors.Is(err, ErrBuilderLease) {
			t.Fatalf("foreign lease enqueue=%v", err)
		}
		if set.Owner() != ResourceOwnerCandidate || foreign.Stats().ActiveBuilders != 1 || coordinator.Stats().VisibleCommitSeq != 0 {
			t.Fatalf("foreign rejection mutated candidate/lease/coordinator: owner=%v foreign=%+v local=%+v", set.Owner(), foreign.Stats(), coordinator.Stats())
		}
	})

	t.Run("released", func(t *testing.T) {
		coordinator := newCoordinator(t)
		token, err := coordinator.AcquireBuilder(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		token.Release()
		prepared, set := candidateWithEmptyResourceSet(t, 1, 1)
		defer prepared.AbandonResources()
		if _, err := coordinator.EnqueueBuilt(prepared, token); !errors.Is(err, ErrBuilderLease) {
			t.Fatalf("released lease enqueue=%v", err)
		}
		if set.Owner() != ResourceOwnerCandidate || coordinator.Stats().VisibleCommitSeq != 0 {
			t.Fatalf("released rejection mutated candidate/coordinator: owner=%v stats=%+v", set.Owner(), coordinator.Stats())
		}
	})

	t.Run("nested", func(t *testing.T) {
		coordinator := newCoordinator(t)
		outer, err := coordinator.AcquireBuilder(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		nested, err := outer.Nested()
		if err != nil {
			t.Fatal(err)
		}
		prepared, set := candidateWithEmptyResourceSet(t, 1, 1)
		if _, err := coordinator.EnqueueBuilt(prepared, outer); !errors.Is(err, ErrBuilderLease) {
			t.Fatalf("nested lease enqueue=%v", err)
		}
		if set.Owner() != ResourceOwnerCandidate || coordinator.Stats().ActiveBuilders != 1 || coordinator.Stats().VisibleCommitSeq != 0 {
			t.Fatalf("nested rejection mutated candidate/lease/coordinator: owner=%v stats=%+v", set.Owner(), coordinator.Stats())
		}
		nested.Release()
		receipt, err := coordinator.EnqueueBuilt(prepared, outer)
		if err != nil {
			t.Fatalf("final lease handoff: %v", err)
		}
		if err := coordinator.WaitForAdmission(context.Background(), receipt); err != nil {
			t.Fatalf("final lease admission: %v", err)
		}
		if err := coordinator.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if set.Owner() != ResourceOwnerReleased || coordinator.Stats().ActiveBuilders != 0 {
			t.Fatalf("successful handoff owner/stats=%v/%+v", set.Owner(), coordinator.Stats())
		}
	})
}

func TestEnqueueBuiltValidationAndActivationFailurePreserveFinalLease(t *testing.T) {
	coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, coordinator)
	token, err := coordinator.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.EnqueueBuilt(nil, token); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("nil candidate enqueue=%v", err)
	}
	probe, err := token.Nested()
	if err != nil {
		t.Fatalf("validation failure consumed final lease: %v", err)
	}
	probe.Release()

	fixture := newDurableRootTransactionFixture(t, durableLineage(9), 2)
	fixture.activateErr = errors.New("activation rejected")
	if _, err := coordinator.EnqueueBuilt(fixture.candidate, token); !errors.Is(err, fixture.activateErr) {
		t.Fatalf("activation failure=%v", err)
	}
	if fixture.tx.Owner() != ResourceOwnerCandidate || fixture.resources.Owner() != ResourceOwnerCandidate || coordinator.Stats().ActiveBuilders != 1 {
		t.Fatalf("activation failure ownership/stats=(%v,%v,%+v)", fixture.tx.Owner(), fixture.resources.Owner(), coordinator.Stats())
	}
	probe, err = token.Nested()
	if err != nil {
		t.Fatalf("activation failure consumed final lease: %v", err)
	}
	probe.Release()
	if err := fixture.candidate.Abandon(); err != nil {
		t.Fatal(err)
	}
	token.Release()
}

func TestEnqueueBuiltAppendsBeforeFinalLeaseReleaseAndHardAdmissionWait(t *testing.T) {
	calls := make(chan *PreparedRootCandidate, 1)
	coordinator, err := New(Options{Publisher: PublisherFunc(func(_ context.Context, prepared *PreparedRootCandidate) PublishResult {
		calls <- prepared
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, coordinator)
	token, err := coordinator.AcquireBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), candidate(t, 1, SoftPendingBytes)); err != nil {
		t.Fatal(err)
	}
	receipt, err := coordinator.EnqueueBuilt(candidate(t, 2, HardPendingBytes), token)
	if err != nil {
		t.Fatalf("atomic hard-admission handoff: %v", err)
	}
	if err := coordinator.WaitForAdmission(context.Background(), receipt); err != nil {
		t.Fatalf("hard-admission wait: %v", err)
	}
	published := waitForCall(t, calls)
	if published.Frontier().CommitSeq() != 2 || coordinator.Stats().LastGroupSize != 2 {
		t.Fatalf("publisher captured before built candidate: frontier=%d stats=%+v", published.Frontier().CommitSeq(), coordinator.Stats())
	}
	if coordinator.Stats().ActiveBuilders != 0 {
		t.Fatalf("hard wait retained final lease: %+v", coordinator.Stats())
	}
	if _, err := token.Nested(); !errors.Is(err, ErrClosed) {
		t.Fatalf("consumed handoff token nested again: %v", err)
	}
}

func TestEnqueueBuiltConcurrentReleaseHasNoLockInversionOrDoubleConsume(t *testing.T) {
	coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, coordinator)
	seq := uint64(1)
	for iteration := 0; iteration < 64; iteration++ {
		token, err := coordinator.AcquireBuilder(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		prepared := candidate(t, seq, 1)
		start := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-start
			receipt, err := coordinator.EnqueueBuilt(prepared, token)
			if err == nil {
				err = coordinator.WaitForAdmission(context.Background(), receipt)
			}
			done <- err
		}()
		close(start)
		runtime.Gosched()
		token.Release()
		err = <-done
		if err == nil {
			if err := coordinator.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			seq++
		} else if !errors.Is(err, ErrBuilderLease) {
			t.Fatalf("iteration %d handoff/release race=%v", iteration, err)
		}
		if coordinator.Stats().ActiveBuilders != 0 {
			t.Fatalf("iteration %d leaked/double-counted builder: %+v", iteration, coordinator.Stats())
		}
	}
}

func TestPublisherPrepareBlocksBuildersButStablePublishDoesNot(t *testing.T) {
	prepareEntered := make(chan struct{})
	prepareRelease := make(chan struct{})
	publishEntered := make(chan struct{})
	publishRelease := make(chan struct{})
	publisher := &testPreparingPublisher{
		prepare: func(ctx context.Context, _ *PreparedRootCandidate) error {
			close(prepareEntered)
			select {
			case <-prepareRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		publish: func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
			close(publishEntered)
			select {
			case <-publishRelease:
				return PublishResult{Outcome: PublishSucceeded}
			case <-ctx.Done():
				return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
			}
		},
	}
	coordinator, err := New(Options{Publisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, coordinator)
	if err := coordinator.Enqueue(context.Background(), candidate(t, 1, SoftPendingBytes)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-prepareEntered:
	case <-time.After(time.Second):
		t.Fatal("prepare did not start")
	}

	canceled, cancel := context.WithCancel(context.Background())
	canceledResult := make(chan error, 1)
	go func() {
		_, err := coordinator.AcquireBuilder(canceled)
		canceledResult <- err
	}()
	cancel()
	if err := <-canceledResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked builder cancellation=%v", err)
	}
	if coordinator.Stats().ActiveBuilders != 0 {
		t.Fatalf("builder admitted during prepare: %+v", coordinator.Stats())
	}

	acquired := make(chan *BuilderToken, 1)
	go func() {
		token, err := coordinator.AcquireBuilder(context.Background())
		if err == nil {
			acquired <- token
		}
	}()
	runtime.Gosched()
	select {
	case token := <-acquired:
		token.Release()
		t.Fatal("builder admitted while Prepare held the gate")
	case <-time.After(25 * time.Millisecond):
	}
	close(prepareRelease)
	select {
	case <-publishEntered:
	case <-time.After(time.Second):
		t.Fatal("stable Publish did not start")
	}
	var token *BuilderToken
	select {
	case token = <-acquired:
	case <-time.After(time.Second):
		t.Fatal("builder remained gated during stable Publish")
	}
	if coordinator.Stats().ActiveBuilders != 1 {
		t.Fatalf("builder not accounted during stable Publish: %+v", coordinator.Stats())
	}
	token.Release()
	close(publishRelease)
	if err := coordinator.WaitThrough(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
}

func TestPublisherPrepareFailureIsRetryableAndRetryPreparesAgain(t *testing.T) {
	want := errors.New("prepare dependency")
	var prepares atomic.Uint64
	var publishes atomic.Uint64
	publisher := &testPreparingPublisher{
		prepare: func(context.Context, *PreparedRootCandidate) error {
			if prepares.Add(1) == 1 {
				return want
			}
			return nil
		},
		publish: func(context.Context, *PreparedRootCandidate) PublishResult {
			publishes.Add(1)
			return PublishResult{Outcome: PublishSucceeded}
		},
	}
	coordinator, err := New(Options{Publisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, coordinator)
	if err := coordinator.Enqueue(context.Background(), candidate(t, 1, 99)); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitThrough(context.Background(), 1); !errors.Is(err, want) {
		t.Fatalf("first prepare wait=%v", err)
	}
	if stats := coordinator.Stats(); stats.PendingCommits != 1 || stats.PreMetaFailures != 1 || stats.WaiterCount != 0 {
		t.Fatalf("prepare failure did not retain exact debt/fail captured waiter: %+v", stats)
	}
	if prepares.Load() != 1 || publishes.Load() != 0 {
		t.Fatalf("prepare/publish calls after failure=(%d,%d)", prepares.Load(), publishes.Load())
	}
	if err := coordinator.WaitThrough(context.Background(), 1); err != nil {
		t.Fatalf("prepare retry: %v", err)
	}
	if prepares.Load() != 2 || publishes.Load() != 1 || coordinator.Stats().Retries != 1 {
		t.Fatalf("prepare/publish/retry=(%d,%d,%d)", prepares.Load(), publishes.Load(), coordinator.Stats().Retries)
	}
}

func TestStopCancelsPrepareAndWakesBuilderBlockedOnGate(t *testing.T) {
	prepareEntered := make(chan struct{})
	publisher := &testPreparingPublisher{
		prepare: func(ctx context.Context, _ *PreparedRootCandidate) error {
			close(prepareEntered)
			<-ctx.Done()
			return ctx.Err()
		},
		publish: func(context.Context, *PreparedRootCandidate) PublishResult {
			t.Fatal("Publish called after canceled Prepare")
			return PublishResult{Outcome: PublishSucceeded}
		},
	}
	coordinator, err := New(Options{Publisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), candidate(t, 1, SoftPendingBytes)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-prepareEntered:
	case <-time.After(time.Second):
		t.Fatal("prepare did not start")
	}
	acquired := make(chan error, 1)
	go func() {
		token, err := coordinator.AcquireBuilder(context.Background())
		if token != nil {
			token.Release()
		}
		acquired <- err
	}()
	runtime.Gosched()
	if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop=%v", err)
	}
	if err := <-acquired; !errors.Is(err, ErrClosed) {
		t.Fatalf("builder blocked on stopped prepare gate=%v", err)
	}
	coordinator.mu.Lock()
	preparing := coordinator.preparing
	coordinator.mu.Unlock()
	if preparing || coordinator.Stats().ActiveBuilders != 0 {
		t.Fatalf("stop leaked prepare gate/builder: preparing=%t stats=%+v", preparing, coordinator.Stats())
	}
}
