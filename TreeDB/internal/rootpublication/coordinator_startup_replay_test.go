package rootpublication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartupReplaySameCoordinatorWaitDrainAndActivation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	clock := NewFakeClock(time.Unix(500, 0))
	refused := errors.New("original startup prepare refused")
	var prepares, publishes atomic.Int64
	publisher := &testPreparingPublisher{
		prepare: func(context.Context, *PreparedRootCandidate) error {
			if prepares.Add(1) == 1 {
				return refused
			}
			return nil
		},
		publish: func(context.Context, *PreparedRootCandidate) PublishResult {
			publishes.Add(1)
			return PublishResult{Outcome: PublishSucceeded}
		},
	}
	c, err := New(Options{Clock: clock, Publisher: publisher, StartupSynchronous: true})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	prepared, original := candidateWithEmptyResourceSet(t, 1, SoftPendingBytes)
	if err = c.Enqueue(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err = c.CompleteStartup(); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatalf("unfinished startup activation: %v", err)
	}
	if prepares.Load() != 0 || publishes.Load() != 0 {
		t.Fatal("startup scheduler consumed a caller-owned attempt")
	}
	if err = c.WaitThrough(ctx, 1); !errors.Is(err, refused) {
		t.Fatalf("original prepare outcome: %v", err)
	}
	if original.Owner() != ResourceOwnerCoordinator || c.Stats().PendingCommits != 1 || publishes.Load() != 0 {
		t.Fatal("failed startup wait lost original resource custody or advanced durability")
	}
	if err = c.CompleteStartup(); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatalf("failed startup activation: %v", err)
	}
	if err = c.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if prepares.Load() != 2 || publishes.Load() != 1 || c.Stats().DurableCommitSeq != 1 || original.Owner() != ResourceOwnerReleased {
		t.Fatal("startup drain did not complete the same original attempt and resource")
	}
	if err = c.CompleteStartup(); err != nil {
		t.Fatal(err)
	}
	if c.StartupSynchronous() {
		t.Fatal("startup mode did not end")
	}
	if err = c.Enqueue(ctx, candidate(t, 2, 1)); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if prepares.Load() != 3 || publishes.Load() != 2 {
		t.Fatal("normal scheduler did not reuse the same publisher")
	}
}

func TestStartupReplayStopRetainsExactFailedResource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	refused := errors.New("original startup physical refusal")
	c, err := New(Options{StartupSynchronous: true, Publisher: &testPreparingPublisher{
		prepare: func(context.Context, *PreparedRootCandidate) error { return refused },
		publish: func(context.Context, *PreparedRootCandidate) PublishResult {
			t.Fatal("publish after failed prepare")
			return PublishResult{}
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	prepared, original := candidateWithEmptyResourceSet(t, 1, 1)
	if err = c.Enqueue(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitThrough(ctx, 1); !errors.Is(err, refused) {
		t.Fatal(err)
	}
	if err = c.Stop(ctx); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("startup stop: %v", err)
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	if handoff.Len() != 1 || handoff.Sets()[0] != original || original.Owner() != ResourceOwnerRecovery {
		t.Fatal("startup stop lost the original failed physical owner")
	}
	if err = handoff.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if original.Owner() != ResourceOwnerReleased {
		t.Fatal("original recovery handoff did not complete")
	}
}

// A real original allocator Fail callback can refuse capture. Stop's pending
// outcome must not conceal that refusal or authorize a handoff/second disposal.
func TestStartupReplayCaptureRefusalRetainsOriginalTransaction(t *testing.T) {
	fixture := newDurableRootTransactionFixture(t, durableLineage(1), 2)
	refused := errors.New("original allocator capture refused")
	originalFail := fixture.tx.fail
	fixture.tx.fail = func(input DurableRootCallbackInput, cause error) error {
		if err := originalFail(input, cause); err != nil {
			return err
		}
		return refused
	}
	c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		t.Fatal("startup scheduler published without caller")
		return PublishResult{}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Enqueue(context.Background(), fixture.candidate); err != nil {
		t.Fatal(err)
	}
	if err = c.Stop(context.Background()); !errors.Is(err, refused) || !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop lost pending/capture outcome: %v", err)
	}
	first, err := c.TakeRecoveryHandoff()
	if first != nil || !errors.Is(err, refused) {
		t.Fatalf("capture refusal handed off: %p %v", first, err)
	}
	if len(c.recoveryDurableRoots) != 1 || c.recoveryDurableRoots[0] != fixture.tx || fixture.tx.Owner() != ResourceOwnerCoordinator || fixture.failures.Load() != 1 {
		t.Fatal("capture refusal lost same original transaction or repeated Fail")
	}
	second, again := c.TakeRecoveryHandoff()
	if second != nil || again != err || fixture.failures.Load() != 1 {
		t.Fatal("handoff observation retried original physical refusal")
	}
	if err = c.Stop(context.Background()); !errors.Is(err, refused) || fixture.failures.Load() != 1 {
		t.Fatal("repeated stop lost original capture refusal")
	}
}
