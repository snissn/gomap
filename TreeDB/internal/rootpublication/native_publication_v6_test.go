package rootpublication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

type nativePublicationOwnerControlV6 struct {
	mu       sync.Mutex
	c        *Coordinator
	receipt  NativePublicationV6
	original *StableResourceSet
	closes   int
	refuse   error
	retained bool
}

func (o *nativePublicationOwnerControlV6) CloseNativePublicationV6() (retainedalloc.Lease, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.c.mu.Lock()
	complete := o.receipt.complete
	o.c.mu.Unlock()
	if complete {
		return nil, o.receipt.firstError
	}
	o.closes++
	if o.refuse != nil {
		o.retained = true // This control remains the original preborn error operand.
		return nil, o.c.CompleteNativePublicationV6(&o.receipt, o.refuse, true)
	}
	if o.original != nil {
		o.c.mu.Lock()
		err := o.original.releaseFromWithActivePins(ResourceOwnerCoordinator, o.c.resourceActivePins)
		o.c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		o.original = nil
	}
	return nil, o.c.CompleteNativePublicationV6(&o.receipt, nil, false)
}
func nativePublicationControlV6(t *testing.T) (*Coordinator, *nativePublicationOwnerControlV6) {
	t.Helper()
	c, err := New(Options{Clock: NewFakeClock(time.Unix(1, 0)), Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	o := &nativePublicationOwnerControlV6{c: c}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := c.BindNativePublicationV6(&o.receipt, o, &w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	return c, o
}
func nativeControlFenceV6(t *testing.T, c *Coordinator, o *nativePublicationOwnerControlV6) {
	t.Helper()
	first, set := candidateWithEmptyResourceSet(t, 1, 1)
	o.original = set
	if err := c.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := c.BeginNativePublicationTurnV6(&o.receipt, first, &w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if ready, err := c.ReleaseNativePublicationTurnV6(&o.receipt, PublishResult{Outcome: PublishSucceeded}, &w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if set.Owner() != ResourceOwnerCoordinator || c.Stats().PendingCommits != 0 {
		t.Fatal("fenced release consumed original resources")
	}
}
func TestNativePublicationPausedFinishAllowsOrdinaryDurableSuccessor(t *testing.T) {
	c, o := nativePublicationControlV6(t)
	defer stopClean(t, c)
	nativeControlFenceV6(t, c, o)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Enqueue(ctx, candidate(t, 2, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if c.Stats().DurableCommitSeq != 2 {
		t.Fatal("ordinary successor did not become durable")
	}
	c.mu.Lock()
	joined := c.activeDispatch == 1 && c.nativeDispatchV6 == 1
	c.mu.Unlock()
	if !joined || o.closes != 0 {
		t.Fatal("publication turn release completed native physical receipt")
	}
	if _, err := o.CloseNativePublicationV6(); err != nil {
		t.Fatal(err)
	}
	if c.Stats().DurableCommitSeq != 2 {
		t.Fatal("late Finish regressed authoritative frontier")
	}
	if o.closes != 1 {
		t.Fatal("native completion replay")
	}
}
func TestNativePublicationStopJoinsIdleOriginalBeforeSchedulerStop(t *testing.T) {
	c, o := nativePublicationControlV6(t)
	nativeControlFenceV6(t, c, o)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if o.closes != 1 || o.original != nil {
		t.Fatal("Stop overtook or replayed original physical completion")
	}
	if err := c.Stop(ctx); err != nil || o.closes != 1 {
		t.Fatal(err, o.closes)
	}
}
func TestNativePublicationLateRefusalRequiresCustodyWithoutPoisoningSuccessor(t *testing.T) {
	c, o := nativePublicationControlV6(t)
	nativeControlFenceV6(t, c, o)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Enqueue(ctx, candidate(t, 2, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("actual original completion refused")
	if err := c.CompleteNativePublicationV6(&o.receipt, refusal, false); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatal(err)
	}
	o.refuse = refusal
	if _, err := o.CloseNativePublicationV6(); !errors.Is(err, refusal) {
		t.Fatal(err)
	}
	if !o.retained || o.original.Owner() != ResourceOwnerCoordinator || c.Stats().Poisoned || c.Stats().DurableCommitSeq != 2 {
		t.Fatal("late refusal lost original custody or invalidated successor")
	}
	if err := c.WaitThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// Explicit successful disposal of this test-owned empty set ends its fixture.
	if err := o.original.releaseFrom(ResourceOwnerCoordinator); err != nil {
		t.Fatal(err)
	}
}
func TestNativePublicationAmbiguityCannotReleaseTurnAsSuccess(t *testing.T) {
	c, o := nativePublicationControlV6(t)
	first, set := candidateWithEmptyResourceSet(t, 1, 1)
	o.original = set
	if err := c.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := c.BeginNativePublicationTurnV6(&o.receipt, first, &w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	refusal := errors.New("ambiguous actual fence")
	if ready, err := c.ReleaseNativePublicationTurnV6(&o.receipt, PublishResult{Outcome: PublishAmbiguous, Err: refusal}, &w); ready || !errors.Is(err, refusal) {
		t.Fatal(ready, err)
	}
	if c.Stats().DurableCommitSeq != 0 || !c.Stats().Poisoned || set.Owner() != ResourceOwnerCoordinator {
		t.Fatal("ambiguous publication advanced authority or lost custody")
	}
	o.refuse = refusal
	if _, err := o.CloseNativePublicationV6(); !errors.Is(err, refusal) {
		t.Fatal(err)
	}
	if err := c.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatal(err)
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	if err := handoff.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
}

// Stop must not treat physical failure as permission to leave its ordinary
// scheduler running. The original fixture retains failed Set custody.
func TestNativePublicationStopSettlesFailedOriginalAndStillStops(t *testing.T) {
	c, o := nativePublicationControlV6(t)
	nativeControlFenceV6(t, c, o)
	refusal := errors.New("last original physical refusal")
	o.refuse = refusal
	if err := c.Stop(context.Background()); !errors.Is(err, refusal) {
		t.Fatal(err)
	}
	c.mu.Lock()
	stopped, joins := c.stopped, c.activeDispatch
	c.mu.Unlock()
	if !stopped || joins != 0 || o.closes != 1 || !o.retained {
		t.Fatal(stopped, joins, o.closes, o.retained)
	}
	if err := c.Stop(context.Background()); !errors.Is(err, refusal) || o.closes != 1 {
		t.Fatal(err, o.closes)
	}
	if err := o.original.releaseFrom(ResourceOwnerCoordinator); err != nil {
		t.Fatal(err)
	}
}
