package rootpublication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type prefixOriginalCustody struct{ release func() error }

func (c *prefixOriginalCustody) ReleaseAccepted() error { return c.release() }

func prefixPhysicalCandidate(t *testing.T, sequence, lsn uint64) *PreparedRootCandidate {
	t.Helper()
	p, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(sequence, sequence*10, sequence*20, lsn, sequence)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAcceptedPrefixFiniteCutExactSealAndRetirement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err = c.ConfigureAcceptedPrefix(2, LogicalFrontiers{}); err != nil {
		t.Fatal(err)
	}
	defer c.CloseAccepted()
	builder, err := c.AcquireBuilder(ctx)
	if err != nil {
		t.Fatal(err)
	}
	older, err := c.EnqueueBuilt(prefixPhysicalCandidate(t, 1, 0), builder)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	custody := &prefixOriginalCustody{release: func() error {
		calls.Add(1)
		if !c.accepted.mu.TryLock() {
			t.Error("original release under logical owner")
		} else {
			c.accepted.mu.Unlock()
		}
		if !c.mu.TryLock() {
			t.Error("original release under physical owner")
		} else {
			c.mu.Unlock()
		}
		close(entered)
		<-release
		return nil
	}}
	first, err := c.ReserveAccepted(false)
	if err != nil {
		t.Fatal(err)
	}
	if err = first.RecordAssigned(1, nil, false); err != nil {
		t.Fatal(err)
	}
	native := &struct{ identity int }{7}
	projections := []ParticipantProjection{{Domain: 7, Incarnation: 9, Plan: native, Assets: native}}
	if err = first.InstallAccepted(AcceptedDescriptor{Sequence: 1, Projections: projections, Custody: custody}); err != nil {
		t.Fatal(err)
	}
	roots := LogicalRootSet{Raw: native, Semantic: native}
	cut, err := c.CaptureAcceptedCut(SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}, roots)
	if err != nil {
		t.Fatal(err)
	}
	// A caller copy cannot rewrite the captured tuple or original native identity.
	forged := cut
	forged.through.CanonicalLSN = 2
	if err = forged.validatePhysicalCandidate(c, prefixPhysicalCandidate(t, 40, 2)); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("forged Through accepted", err)
	}
	forged = cut
	forged.rootSet.Raw = &struct{ identity int }{8}
	if err = forged.Range(func(AcceptedDescriptor, AcceptedReceipt) error { return nil }); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("foreign native root identity accepted", err)
	}
	second, err := c.ReserveAccepted(false)
	if err != nil {
		t.Fatal(err)
	}
	if err = second.RecordAssigned(2, nil, false); err != nil {
		t.Fatal(err)
	}
	if err = second.InstallAccepted(AcceptedDescriptor{Sequence: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ReserveAccepted(false); !errors.Is(err, ErrAcceptedCapacity) {
		t.Fatalf("unbounded producer: %v", err)
	}
	maintenance, err := c.ReserveAccepted(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = maintenance.RecordAssigned(3, nil, false); err != nil {
		t.Fatal(err)
	}
	if err = maintenance.InstallAccepted(AcceptedDescriptor{Sequence: 2}); err != nil {
		t.Fatal(err)
	}
	if err = cut.Range(func(d AcceptedDescriptor, r AcceptedReceipt) error {
		if d.Sequence != 1 || r.CanonicalLSN != 1 || &d.Projections[0] != &projections[0] || d.Projections[0].Plan != native {
			t.Fatal("cut imported suffix or copied native assets")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = cut.CompleteHandoff(older.SealReceipt()); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("older physical receipt proved newer logical cut")
	}
	builder, err = c.AcquireBuilder(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := c.EnqueueBuiltAcceptedCut(prefixPhysicalCandidate(t, 41, 1), builder, cut)
	if err != nil {
		t.Fatal(err)
	}
	if err = cut.CompleteHandoff(accepted.SealReceipt()); err != nil {
		t.Fatal(err)
	}
	if got := c.LogicalFrontiers(); got.V != 2 || got.C != 3 || got.W != 0 || got.S != (SealedPrefix{}) {
		t.Fatalf("frontiers conflated: %+v", got)
	}
	sealed := make(chan error, 1)
	go func() { sealed <- accepted.SealReceipt().Wait(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	captured := make(chan AcceptedCut, 1)
	captureErr := make(chan error, 1)
	go func() {
		next, e := c.CaptureAcceptedCut(SealedPrefix{LogicalSequence: 2, CanonicalLSN: 3}, roots)
		if e != nil {
			captureErr <- e
		} else {
			captured <- next
		}
	}()
	select {
	case <-captured:
		t.Fatal("capture crossed in-flight original retirement")
	case e := <-captureErr:
		t.Fatal(e)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case err = <-sealed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var next AcceptedCut
	select {
	case next = <-captured:
	case err = <-captureErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	visits := 0
	if err = next.Range(func(d AcceptedDescriptor, r AcceptedReceipt) error {
		visits++
		if d.Sequence != 2 {
			t.Fatal("captured slots shifted after retirement")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if visits != 2 || calls.Load() != 1 {
		t.Fatalf("visits=%d original releases=%d", visits, calls.Load())
	}
	if err = next.Abandon(); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitAccepted(ctx, SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}); err != nil {
		t.Fatal(err)
	}
	if got := c.LogicalFrontiers(); got.W != 0 || got.S != (SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}) {
		t.Fatalf("physical seal invented W: %+v", got)
	}
	if err = first.CancelAccepted(); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("stale nonce accepted")
	}
}

func TestAcceptedPrefixAssignedRefusalNeverPartiallyInstalls(t *testing.T) {
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err = c.ConfigureAcceptedPrefix(1, LogicalFrontiers{}); err != nil {
		t.Fatal(err)
	}
	ticket, err := c.ReserveAccepted(false)
	if err != nil {
		t.Fatal(err)
	}
	if err = ticket.RecordAssigned(2, nil, false); err != nil {
		t.Fatal(err)
	}
	if err = ticket.InstallAccepted(AcceptedDescriptor{Sequence: 1}); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal(err)
	}
	if got := c.LogicalFrontiers(); got.V != 0 || got.C != 0 {
		t.Fatalf("partially installed refused prefix: %+v", got)
	}
	if !errors.Is(c.AcceptedError(), ErrAcceptedReceipt) {
		t.Fatal("postassignment refusal did not poison")
	}
	if err = ticket.CancelAccepted(); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("assigned receipt was cancelled")
	}
	if err = c.CloseAccepted(); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("Close discarded exact terminal refusal")
	}
}

func TestAcceptedPrefixPreservesOriginalAssignedFailure(t *testing.T) {
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err = c.ConfigureAcceptedPrefix(2, LogicalFrontiers{}); err != nil {
		t.Fatal(err)
	}
	first, _ := c.ReserveAccepted(false)
	later, _ := c.ReserveAccepted(false)
	original := errors.New("original uncertain append")
	if err = first.RecordAssigned(1, original, false); !errors.Is(err, original) {
		t.Fatal(err)
	}
	if err = first.InstallAccepted(AcceptedDescriptor{Sequence: 1}); !errors.Is(err, original) {
		t.Fatal("install replaced original failure", err)
	}
	if err = later.RecordAssigned(2, errors.New("later append"), false); !errors.Is(err, original) {
		t.Fatal("assignment replaced original failure", err)
	}
	if err = c.CloseAccepted(); !errors.Is(err, original) {
		t.Fatal(err)
	}
	if err = c.CloseAccepted(); !errors.Is(err, original) {
		t.Fatal("repeated Close lost original failure", err)
	}
}

func TestAcceptedCloseRetainsOriginalFailedCustodyAndContainingArrays(t *testing.T) {
	c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err = c.ConfigureAcceptedPrefix(2, LogicalFrontiers{}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("original destruction failed")
	failedCalls, successCalls := 0, 0
	resource := &struct{ charge uint64 }{8192}
	failed := &prefixOriginalCustody{release: func() error { failedCalls++; return failure }}
	successful := &prefixOriginalCustody{release: func() error { successCalls++; return nil }}
	for i, custody := range []*prefixOriginalCustody{failed, successful} {
		ticket, e := c.ReserveAccepted(false)
		if e != nil {
			t.Fatal(e)
		}
		if e = ticket.RecordAssigned(uint64(i+1), nil, false); e != nil {
			t.Fatal(e)
		}
		if e = ticket.InstallAccepted(AcceptedDescriptor{Sequence: uint64(i + 1), Custody: custody, Projections: []ParticipantProjection{{Domain: 1, Plan: resource, Assets: resource}}}); e != nil {
			t.Fatal(e)
		}
	}
	originalEntries, originalOrder, originalFree := &c.accepted.entries[0], &c.accepted.order[0], &c.accepted.free[0]
	for attempt := 0; attempt < 2; attempt++ {
		if err = c.CloseAccepted(); !errors.Is(err, failure) {
			t.Fatalf("close %d: %v", attempt, err)
		}
		if !c.AcceptedCleanupFailed() || &c.accepted.entries[0] != originalEntries || &c.accepted.order[0] != originalOrder || &c.accepted.free[0] != originalFree {
			t.Fatal("lost original containing owner")
		}
		found := false
		for i := range c.accepted.entries {
			entry := &c.accepted.entries[i]
			if entry.descriptor.Custody == failed {
				found = true
				if entry.releaseErr != failure || entry.descriptor.Projections[0].Assets != resource || resource.charge != 8192 {
					t.Fatal("lost original resource/failure/charge")
				}
			}
			if entry.descriptor.Custody == successful {
				t.Fatal("successful sibling retained")
			}
		}
		if !found || failedCalls != 1 || successCalls != 1 {
			t.Fatalf("replayed/lost originals: %v %d %d", found, failedCalls, successCalls)
		}
	}
}
