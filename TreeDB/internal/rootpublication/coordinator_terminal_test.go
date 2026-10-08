package rootpublication

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

type terminalTestHold struct{ released bool }

func (h *terminalTestHold) RetainedBackingCensus() (BackingCensus, error) {
	return BackingCensus{}, nil
}
func (h *terminalTestHold) Release() error { return h.ReleaseWithTerminal(nil) }
func (h *terminalTestHold) ValidateTerminalRelease(c StableSegmentTerminalConsumer) error {
	if h.released {
		return nil
	}
	if c == nil {
		return ErrStableTerminalConsumerRequired
	}
	return c.ValidateSegmentRetention(h)
}
func (h *terminalTestHold) ReleaseWithTerminal(c StableSegmentTerminalConsumer) error {
	if h.released {
		return nil
	}
	if err := h.ValidateTerminalRelease(c); err != nil {
		return err
	}
	return c.ReleaseSegmentRetention(h)
}

type terminalTestConsumer struct {
	groups     []StableSegmentTerminalGroup
	fail       error
	begin      func()
	release    func()
	end        func()
	prepares   int
	prepareErr error
}

func (c *terminalTestConsumer) BeginTerminalRelease() (bool, error) {
	if c.begin != nil {
		c.begin()
	}
	return true, nil
}
func (c *terminalTestConsumer) PrepareTerminalRelease(groups []StableSegmentTerminalGroup) error {
	c.groups = groups
	c.prepares++
	return c.prepareErr
}
func (c *terminalTestConsumer) ValidateSegmentRetention(h StableSegmentRetention) error {
	if _, ok := h.(*terminalTestHold); !ok {
		return ErrStableTerminalConsumerMismatch
	}
	return nil
}
func (c *terminalTestConsumer) ReleaseSegmentRetention(h StableSegmentRetention) error {
	if c.release != nil {
		c.release()
	}
	if c.fail != nil {
		return c.fail
	}
	h.(*terminalTestHold).released = true
	return nil
}
func (c *terminalTestConsumer) EndTerminalRelease(bool) {
	c.groups = nil
	if c.end != nil {
		c.end()
	}
}

func terminalTestOwnedSet(t *testing.T, account *testStableMetadataAccount, count int, owner ResourceOwnerState) (*StableResourceSet, []*terminalTestHold) {
	t.Helper()
	set := &StableResourceSet{metadataAccount: account, finiteMetadata: true, entries: make([]stableResourceEntry, count)}
	if err := account.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	set.owner.Store(uint32(owner))
	holds := make([]*terminalTestHold, count)
	for i := range set.entries {
		f, err := os.CreateTemp(t.TempDir(), "terminal")
		if err != nil {
			t.Fatal(err)
		}
		spec := finiteMetadataTestSpec(f)
		token, err := NewStableResourceTokenWithMetadataAccount(spec, account)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		holds[i] = &terminalTestHold{}
		token.segmentRetention = holds[i]
		if err := token.claim(owner); err != nil {
			t.Fatal(err)
		}
		set.entries[i].token = token
	}
	return set, holds
}
func terminalTestReportingCoordinator(t *testing.T, set *StableResourceSet) (*Coordinator, PublishAttempt, *durableRootTransactionFixture) {
	t.Helper()
	f := newDurableRootTransactionFixture(t, durableLineage(91), 1)
	if err := f.resources.releaseFrom(ResourceOwnerCandidate); err != nil {
		t.Fatal(err)
	}
	f.resources = set
	f.candidate.extensions.resourceSet = set
	if err := f.tx.transfer(ResourceOwnerCandidate, ResourceOwnerCoordinator); err != nil {
		t.Fatal(err)
	}
	if err := f.tx.activateFromCoordinator(); err != nil {
		t.Fatal(err)
	}
	clock := realClock{}
	a := PublishAttempt{id: 1, candidate: f.candidate, groupSize: 1, started: clock.Now()}
	c := &Coordinator{clock: clock, pending: []pendingEntry{{candidate: f.candidate, bytes: 1}}, pendingBytes: 1,
		publishing: true, reportingAttempt: true, activeAttempt: a, visible: f.candidate.frontier,
		changed: make(chan struct{}), wake: make(chan struct{}, 1), resourceActivePins: make(map[ResourceKind]uint64), resourcePinHighWater: make(map[ResourceKind]uint64)}
	return c, a, f
}

func TestStableTerminalReportMissingConsumerPreservesCOWAndActualOwners(t *testing.T) {
	finiteMetadataTestPlatform(t)
	account := &testStableMetadataAccount{}
	set, holds := terminalTestOwnedSet(t, account, 1, ResourceOwnerCoordinator)
	c, a, f := terminalTestReportingCoordinator(t, set)
	births, refs := account.bytes, account.retained()
	err := c.ReportPublishResult(a, PublishResult{Outcome: PublishSucceeded})
	if !errors.Is(err, ErrStableTerminalConsumerRequired) || f.consumes.Load() != 0 || holds[0].released || account.bytes != births || account.retained() != refs || set.Owner() != ResourceOwnerCoordinator {
		t.Fatalf("preflight err=%v consume=%d owner=%v births=%d refs=%d", err, f.consumes.Load(), set.Owner(), account.bytes, account.retained())
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil || handoff.Len() != 1 || handoff.DurableRootLen() != 1 {
		t.Fatalf("handoff=%v err=%v", handoff, err)
	}
	consumer := &terminalTestConsumer{}
	if err := handoff.ReleaseWithTerminal(consumer); err != nil {
		t.Fatal(err)
	}
	if account.retained() != 0 || !holds[0].released || f.consumes.Load() != 0 {
		t.Fatal("recovery rebirthed COW or lost terminal holder")
	}
}

func TestStableTerminalReportCleanupFailureHandoffNeverConsumesCOWTwice(t *testing.T) {
	finiteMetadataTestPlatform(t)
	account := &testStableMetadataAccount{}
	set, holds := terminalTestOwnedSet(t, account, 2, ResourceOwnerCoordinator)
	c, a, f := terminalTestReportingCoordinator(t, set)
	ioFailure := errors.New("namespace persistence failed")
	consumer := &terminalTestConsumer{}
	consumer.release = func() {
		_ = c.Stats() // actual cleanup may read coordinator state without lock inversion.
		if holds[0].released {
			consumer.fail = ioFailure
		}
	}
	if err := c.ReportPublishResultWithTerminal(a, PublishResult{Outcome: PublishSucceeded}, consumer); !errors.Is(err, ioFailure) {
		t.Fatal(err)
	}
	if f.consumes.Load() != 1 || !holds[0].released || holds[1].released || account.retained() == 0 || set.Owner() != ResourceOwnerCoordinator || consumer.groups != nil {
		t.Fatal("failure did not preserve exact unfinished closure")
	}
	if err := c.ReportPublishResultWithTerminal(a, PublishResult{Outcome: PublishSucceeded}, consumer); !errors.Is(err, ErrPublisherProtocol) {
		t.Fatal("consumed report was replayable", err)
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil || handoff.Len() != 1 || handoff.DurableRootLen() != 0 {
		t.Fatalf("handoff=%v err=%v", handoff, err)
	}
	if err := handoff.Release(); !errors.Is(err, ErrStableTerminalConsumerRequired) || handoff.Len() != 1 {
		t.Fatal("generic recovery lost unfinished holder", err)
	}
	consumer.fail = nil
	consumer.release = nil
	if err := handoff.ReleaseWithTerminal(consumer); err != nil {
		t.Fatal(err)
	}
	if account.retained() != 0 || !holds[1].released || f.consumes.Load() != 1 || handoff.Len() != 0 {
		t.Fatal("recovery reconsumed COW or retained terminal credits")
	}
}

func TestStableTerminalReportEndsBeforeACKAndProtectsFinishingReservation(t *testing.T) {
	finiteMetadataTestPlatform(t)
	account := &testStableMetadataAccount{}
	set, _ := terminalTestOwnedSet(t, account, 1, ResourceOwnerCoordinator)
	c, a, f := terminalTestReportingCoordinator(t, set)
	waiter := &durabilityWaiter{seq: 1, ch: make(chan error, 1)}
	c.waiters = []*durabilityWaiter{waiter}
	consumer := &terminalTestConsumer{}
	consumer.begin = func() {
		if _, err := c.TakeRecoveryHandoff(); !errors.Is(err, ErrRecoveryHandoffUnavailable) {
			t.Fatal("finishing closure was stealable", err)
		}
		if err := c.ReportPublishResult(a, PublishResult{Outcome: PublishSucceeded}); !errors.Is(err, ErrPublisherProtocol) {
			t.Fatal("reentrant report accepted", err)
		}
	}
	consumer.release = func() { _ = c.Stats() }
	consumer.end = func() {
		select {
		case <-waiter.ch:
			t.Fatal("ACK before terminal End")
		default:
		}
	}
	if err := c.ReportPublishResultWithTerminal(a, PublishResult{Outcome: PublishSucceeded}, consumer); err != nil {
		t.Fatal(err)
	}
	if err := <-waiter.ch; err != nil {
		t.Fatal(err)
	}
	if f.consumes.Load() != 1 || account.retained() != 0 || c.Stats().DurableCommitSeq != 1 {
		t.Fatal("successful terminal did not report actual durable root")
	}
}

type terminalReportingPublisher struct{ reports int }

func (p *terminalReportingPublisher) Publish(context.Context, *PreparedRootCandidate) PublishResult {
	return PublishResult{Outcome: PublishSucceeded}
}
func (p *terminalReportingPublisher) ReportRootPublishTerminal(c *Coordinator, a PublishAttempt, r PublishResult) error {
	p.reports++
	return c.ReportPublishResultWithTerminal(a, r, nil)
}
func TestStableTerminalReporterUsesActualSchedulerAndOrdinaryRelease(t *testing.T) {
	p := &terminalReportingPublisher{}
	c, err := New(Options{Publisher: p})
	if err != nil {
		t.Fatal(err)
	}
	candidate, set := candidateWithEmptyResourceSet(t, 1, 1)
	if err := c.Enqueue(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if p.reports != 1 || set.Owner() != ResourceOwnerReleased {
		t.Fatal("scheduler skipped concrete reporter or ordinary cleanup")
	}
}

func TestStableTerminalCandidateAbandonPreflightsActualClosureBeforeCOW(t *testing.T) {
	finiteMetadataTestPlatform(t)
	for _, wrongPhase := range []bool{true, false} {
		account := &testStableMetadataAccount{}
		phase := ResourceOwnerCandidate
		if wrongPhase {
			phase = ResourceOwnerRecovery
		}
		set, holds := terminalTestOwnedSet(t, account, 1, phase)
		f := newDurableRootTransactionFixture(t, durableLineage(92), 1)
		if err := f.resources.releaseFrom(ResourceOwnerCandidate); err != nil {
			t.Fatal(err)
		}
		f.candidate.extensions.resourceSet = set
		if !wrongPhase {
			set.entries[0].pins = []*StableResourceToken{set.entries[0].token}
		}
		births, refs := account.bytes, account.retained()
		consumer := &terminalTestConsumer{}
		err := f.candidate.AbandonWithTerminal(consumer)
		if err == nil || f.aborts.Load() != 0 || holds[0].released || account.bytes != births || account.retained() != refs {
			t.Fatalf("invalid phase/closure changed COW/account: %v", err)
		}
		if wrongPhase {
			if err := set.transfer(ResourceOwnerRecovery, ResourceOwnerCandidate); err != nil {
				t.Fatal(err)
			}
		} else {
			set.entries[0].pins = nil
		}
		if err := f.candidate.AbandonWithTerminal(consumer); err != nil {
			t.Fatal(err)
		}
		if f.aborts.Load() != 1 || account.retained() != 0 || !holds[0].released {
			t.Fatal("valid actual closure did not abort and clean exactly once")
		}
	}
}
