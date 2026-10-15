package rootpublication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// The original governor's shrink is the actual outside-coordinator terminal
// accounting operation, rather than a replacement publisher or completion hook.
type terminalJoinBudgetV6 struct {
	armed   atomic.Bool
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}
type terminalJoinLeaseV6 struct {
	b     *terminalJoinBudgetV6
	bytes uint64
}

func (b *terminalJoinBudgetV6) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	return &terminalJoinLeaseV6{b: b, bytes: n}, nil
}
func (l *terminalJoinLeaseV6) Resize(n uint64) error {
	if n < l.bytes && l.b.armed.Load() {
		l.b.once.Do(func() { close(l.b.entered); <-l.b.resume })
	}
	l.bytes = n
	return nil
}
func (l *terminalJoinLeaseV6) Close() { l.bytes = 0 }

func TestConsumedAdmissionLineagePrecedesPhysicalTerminalJoinV6(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	budget := &terminalJoinBudgetV6{entered: make(chan struct{}), resume: make(chan struct{})}
	var unblock sync.Once
	release := func() { unblock.Do(func() { close(budget.resume) }) }
	defer release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	enrollmentBytes := owner.Bytes()
	f := newSelectedDataTerminalFixture(t, &owner, nil, durableLineage(1), 2, true)
	c, err := New(Options{StartupSynchronous: true, InitialDurableFrontier: NewFrontier(1, 2, 3, 0, 0), DurableRootLineage: durableLineage(1), DurableRootSequence: 1, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { release(); _ = c.Stop(ctx) }()
	if err = c.Enqueue(ctx, f.candidate); err != nil {
		t.Fatal(err)
	}
	budget.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- c.WaitThrough(ctx, 2) }()
	select {
	case <-budget.entered:
	case <-ctx.Done():
		t.Fatal("original terminal shrink was not reached")
	}
	// A physically consumed root is the admission predecessor, even though
	// its original containing charge has not yet been physically joined.
	var nextOwner, wrongOwner, skipOwner retainedalloc.Owner
	nextOwner.Initialize(0)
	wrongOwner.Initialize(0)
	skipOwner.Initialize(0)
	next := newSelectedDataTerminalFixture(t, &nextOwner, nil, durableLineage(1), 3, true)
	wrong := newSelectedDataTerminalFixture(t, &wrongOwner, nil, durableLineage(9), 3, true)
	skipped := newSelectedDataTerminalFixture(t, &skipOwner, nil, durableLineage(1), 4, true)
	defer wrong.candidate.Abandon()
	defer skipped.candidate.Abandon()
	if err = c.Enqueue(ctx, wrong.candidate); !errors.Is(err, ErrDurableRootLineage) {
		t.Errorf("wrong lineage admitted: %v", err)
	}
	if err = c.Enqueue(ctx, skipped.candidate); !errors.Is(err, ErrDurableRootLineage) {
		t.Errorf("skipped successor admitted: %v", err)
	}
	if err = c.Enqueue(ctx, next.candidate); err != nil {
		_ = next.candidate.Abandon()
		t.Errorf("consecutive successor rejected during original physical Finish: %v", err)
	}
	c.mu.Lock()
	durable, admitted, publishing := c.durable.CommitSeq(), c.durableRootSequence, c.publishing
	c.mu.Unlock()
	if durable != 1 || admitted != 2 || !publishing {
		t.Errorf("join moved wrong frontier: durable=%d admission=%d publishing=%v", durable, admitted, publishing)
	}
	select {
	case err := <-done:
		t.Fatalf("ACK escaped original terminal join: %v", err)
	default:
	}
	release()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = c.WaitThrough(ctx, 3); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	durable, admitted = c.durable.CommitSeq(), c.durableRootSequence
	c.mu.Unlock()
	if durable != 3 || admitted != 3 || owner.Bytes() != enrollmentBytes || nextOwner.Bytes() != 0 {
		t.Fatalf("successor completion lost: durable=%d admission=%d charges=%d/%d", durable, admitted, owner.Bytes(), nextOwner.Bytes())
	}
	enrollment.Close()
	if owner.Bytes() != 0 {
		t.Fatalf("original governing enrollment retained after terminal completion: %d", owner.Bytes())
	}
}
