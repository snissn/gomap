package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Options struct {
	Clock     Clock
	Publisher Publisher
	// FixedPublishDelay runs the normal coordinator contract with a fixed
	// timer delay. Zero keeps the adaptive 20x-service-time policy. The fixed
	// mode exists for deterministic safety-equivalent benchmark matrices.
	FixedPublishDelay          time.Duration
	InitialDurableFrontier     Frontier
	OldestRecoverableCommitSeq uint64
	// DurableRootLineage and DurableRootSequence seed an already-open durable
	// allocator lineage. Both must be zero for legacy/dormant callers or both
	// must identify InitialDurableFrontier.
	DurableRootLineage  DurableRootLineageID
	DurableRootSequence uint64
}

type pendingEntry struct {
	candidate *PreparedRootCandidate
	bytes     uint64
}

type durabilityWaiter struct {
	seq uint64
	ch  chan error
}

type drainRequest struct {
	err error
}

// Coordinator owns only in-memory scheduling state. It is intentionally not
// reachable from any production TreeDB handle in this ticket.
type Coordinator struct {
	mu sync.Mutex

	clock             Clock
	publisher         Publisher
	fixedPublishDelay time.Duration
	ctx               context.Context
	cancel            context.CancelFunc
	wake              chan struct{}
	changed           chan struct{}
	done              chan struct{}

	pending         []pendingEntry
	pendingBytes    uint64
	firstPendingAt  time.Time
	timer           Timer
	timerGeneration uint64
	wakeReason      WakeReason
	publishNow      bool
	publishing      bool
	drains          []*drainRequest
	stopping        bool
	stopped         bool
	stopErr         error
	poison          error

	visible           Frontier
	durable           Frontier
	oldestRecoverable uint64
	waiters           []*durabilityWaiter
	activeBuilders    uint64
	preparing         bool

	ewmaService            time.Duration
	lastService            time.Duration
	lastGroupSize          uint64
	admissionWaits         uint64
	preMetaFailures        uint64
	retries                uint64
	publishCalls           uint64
	timerPublishes         uint64
	waiterPublishes        uint64
	softBytesPublishes     uint64
	hardAdmissionPublishes uint64
	retryPublishes         uint64
	drainPublishes         uint64
	failureGeneration      uint64
	lastRetryableError     error
	lastFailureSeq         uint64
	nextAttemptID          uint64
	activeAttempt          PublishAttempt
	retryAttempt           PublishAttempt
	reportingAttempt       bool

	resourceCoalesces    uint64
	resourceConflicts    uint64
	rejectedCandidates   uint64
	recoverySets         []*StableResourceSet
	recoveryDurableRoots []*DurableRootTransaction
	resourceActivePins   map[ResourceKind]uint64
	resourcePinHighWater map[ResourceKind]uint64
	durableRootLineage   DurableRootLineageID
	durableRootSequence  uint64
	reachabilityEpoch    uint64
}

// ReachabilitySnapshot is a bounded, independently pinned view of every root
// and stable resource that can still become recovery-selectable. The epoch is
// opaque to callers; it is used only for exact pre-mutation revalidation.
type ReachabilitySnapshot struct {
	Epoch     uint64
	Visible   Frontier
	Durable   Frontier
	Pending   []Frontier
	Resources *StableResourceSet
}

// Release drops the independently cloned resource pins owned by the snapshot.
func (snapshot *ReachabilitySnapshot) Release() {
	if snapshot == nil {
		return
	}
	snapshot.Resources.Release()
	snapshot.Resources = nil
}

// PublishAttempt is an opaque identity for exactly one captured callback.
// ReportPublishResult rejects zero, stale, duplicate, and mismatched attempts.
type PublishAttempt struct {
	id        uint64
	candidate *PreparedRootCandidate
	groupSize uint64
	drains    []*drainRequest
	started   time.Time
	waiters   []*durabilityWaiter
}

func New(options Options) (*Coordinator, error) {
	if options.Publisher == nil {
		return nil, errors.New("root publication: nil publisher")
	}
	if options.FixedPublishDelay != 0 &&
		(options.FixedPublishDelay < minPublishDelay || options.FixedPublishDelay > maxPublishDelay) {
		return nil, fmt.Errorf("root publication: fixed publish delay %s outside [%s,%s]",
			options.FixedPublishDelay, minPublishDelay, maxPublishDelay)
	}
	if options.OldestRecoverableCommitSeq > options.InitialDurableFrontier.commitSeq {
		return nil, fmt.Errorf("root publication: oldest recoverable sequence %d exceeds durable sequence %d",
			options.OldestRecoverableCommitSeq, options.InitialDurableFrontier.commitSeq)
	}
	if (options.DurableRootLineage == (DurableRootLineageID{})) != (options.DurableRootSequence == 0) ||
		(options.DurableRootSequence != 0 && options.DurableRootSequence != options.InitialDurableFrontier.commitSeq) {
		return nil, fmt.Errorf("root publication: durable-root lineage sequence %d does not match durable frontier %d",
			options.DurableRootSequence, options.InitialDurableFrontier.commitSeq)
	}
	clock := options.Clock
	if clock == nil {
		clock = realClock{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		clock: clock, publisher: options.Publisher, fixedPublishDelay: options.FixedPublishDelay, ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{}), wakeReason: WakeNone,
		visible: options.InitialDurableFrontier, durable: options.InitialDurableFrontier,
		oldestRecoverable:  options.OldestRecoverableCommitSeq,
		resourceActivePins: make(map[ResourceKind]uint64), resourcePinHighWater: make(map[ResourceKind]uint64),
		durableRootLineage: options.DurableRootLineage, durableRootSequence: options.DurableRootSequence,
		reachabilityEpoch: 1,
	}
	if c.oldestRecoverable == 0 {
		c.oldestRecoverable = c.durable.commitSeq
	}
	go c.run()
	return c, nil
}

// Enqueue transfers one immutable candidate into the pending frontier. Below
// hard admission this never waits for Publish. If accepting it crosses either
// hard limit, acknowledgement waits for durable progress or a publisher error.
func (c *Coordinator) Enqueue(ctx context.Context, candidate *PreparedRootCandidate) error {
	return c.enqueue(ctx, candidate, false)
}

// Supersede has the same admission contract as Enqueue and names the semantic
// replacement explicitly for callers and tests.
func (c *Coordinator) Supersede(ctx context.Context, candidate *PreparedRootCandidate) error {
	return c.enqueue(ctx, candidate, true)
}

type enqueueDecision struct {
	sequence          uint64
	failureGeneration uint64
	hard              bool
}

func (c *Coordinator) enqueue(ctx context.Context, candidate *PreparedRootCandidate, supersede bool) error {
	c.mu.Lock()
	decision, err := c.enqueueLocked(candidate, supersede)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	c.signal()
	if !decision.hard {
		return nil
	}
	return c.waitForAdmission(ctx, decision.sequence, decision.failureGeneration)
}

// enqueueLocked validates, activates, and appends one candidate while c.mu is
// held. Its successful return contains everything needed for an admission wait
// after the caller has released any builder lease and all locks.
func (c *Coordinator) enqueueLocked(candidate *PreparedRootCandidate, supersede bool) (enqueueDecision, error) {
	if err := c.terminalErrorLocked(); err != nil {
		return enqueueDecision{}, err
	}
	if candidate == nil {
		return enqueueDecision{}, fmt.Errorf("%w: nil candidate", ErrInvalidCandidate)
	}
	if candidate.frontier.commitSeq <= c.visible.commitSeq ||
		(c.visible.commitSeq != 0 && !candidate.frontier.Dominates(c.visible)) {
		return enqueueDecision{}, fmt.Errorf("%w: frontier %d does not dominate visible %d", ErrInvalidCandidate, candidate.frontier.commitSeq, c.visible.commitSeq)
	}
	if supersede && len(c.pending) == 0 {
		return enqueueDecision{}, fmt.Errorf("%w: no pending candidate to supersede", ErrInvalidCandidate)
	}
	if err := c.preflightDurableRootLocked(candidate); err != nil {
		c.rejectedCandidates++
		return enqueueDecision{}, err
	}
	if candidateSet := candidate.resourceSet(); candidateSet != nil {
		sets := make([]*StableResourceSet, 0, len(c.pending)+1)
		for _, entry := range c.pending {
			if set := entry.candidate.resourceSet(); set != nil {
				sets = append(sets, set)
			}
		}
		sets = append(sets, candidateSet)
		union, err := UnionStableResourceSets(sets...)
		if err != nil {
			c.rejectedCandidates++
			if resourceSetConflict(err) {
				c.resourceConflicts++
			}
			return enqueueDecision{}, err
		}
		if physical := stableSetPhysicalCount(sets); physical > union.Len() {
			c.resourceCoalesces = saturatingAdd(c.resourceCoalesces, uint64(physical-union.Len()))
		}
		if err := candidateSet.transfer(ResourceOwnerCandidate, ResourceOwnerCoordinator); err != nil {
			c.rejectedCandidates++
			return enqueueDecision{}, fmt.Errorf("%w: enqueue resource transfer: %w", ErrInvalidCandidate, err)
		}
	}
	if err := transferDurableRootGroup(candidate.durableRootGroup(), ResourceOwnerCandidate, ResourceOwnerCoordinator); err != nil {
		if candidateSet := candidate.resourceSet(); candidateSet != nil {
			_ = candidateSet.transfer(ResourceOwnerCoordinator, ResourceOwnerCandidate)
		}
		c.rejectedCandidates++
		return enqueueDecision{}, fmt.Errorf("%w: enqueue durable-root transfer: %w", ErrInvalidCandidate, err)
	}
	if err := activateDurableRootGroup(candidate.durableRootGroup()); err != nil {
		_ = transferDurableRootGroup(candidate.durableRootGroup(), ResourceOwnerCoordinator, ResourceOwnerCandidate)
		if candidateSet := candidate.resourceSet(); candidateSet != nil {
			_ = candidateSet.transfer(ResourceOwnerCoordinator, ResourceOwnerCandidate)
		}
		c.rejectedCandidates++
		return enqueueDecision{}, fmt.Errorf("%w: activate durable-root transaction: %w", ErrInvalidCandidate, err)
	}
	// Activation is the final fallible enqueue step. From here through pending
	// append and visible-frontier advance the coordinator only mutates owned
	// in-memory state and must commit the accepted debt.
	wasEmpty := len(c.pending) == 0
	entryBytes := candidate.OwnedBytes()
	c.pending = append(c.pending, pendingEntry{candidate: candidate, bytes: entryBytes})
	c.pendingBytes = saturatingAdd(c.pendingBytes, entryBytes)
	if candidateSet := candidate.resourceSet(); candidateSet != nil {
		candidateSet.adjustActivePinsByKind(c.resourceActivePins, true)
	}
	c.observeResourcePinHighWaterLocked()
	c.visible = candidate.frontier
	c.reachabilityEpoch++
	if wasEmpty {
		c.firstPendingAt = c.clock.Now()
		c.installTimerLocked()
	}
	hard := c.pendingBytes > HardPendingBytes || uint64(len(c.pending)) > HardPendingCommits
	if c.pendingBytes >= SoftPendingBytes {
		c.requestPublishLocked(WakeSoftBytes)
	}
	if hard {
		c.admissionWaits++
		c.requestPublishLocked(WakeHardAdmission)
	}
	return enqueueDecision{sequence: candidate.frontier.commitSeq, failureGeneration: c.failureGeneration, hard: hard}, nil
}

// CaptureReachability independently pins the exact pending/recovery resource
// union while copying the scalar visible, durable, and pending root frontiers.
// It performs no tree scan and its work is bounded by coordinator debt.
func (c *Coordinator) CaptureReachability() (ReachabilitySnapshot, error) {
	if c == nil {
		return ReachabilitySnapshot{}, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.terminalErrorLocked(); err != nil {
		return ReachabilitySnapshot{}, err
	}

	sets := make([]*StableResourceSet, 0, len(c.pending)+len(c.recoverySets))
	pending := make([]Frontier, 0, len(c.pending))
	for _, entry := range c.pending {
		pending = append(pending, entry.candidate.frontier)
		if resources := entry.candidate.resourceSet(); resources != nil {
			sets = append(sets, resources)
		}
	}
	sets = append(sets, c.recoverySets...)
	var resources *StableResourceSet
	if len(sets) != 0 {
		view, err := UnionStableResourceSets(sets...)
		if err != nil {
			return ReachabilitySnapshot{}, err
		}
		resources, err = CloneStableResourceSetExcludingKinds(view)
		if err != nil {
			return ReachabilitySnapshot{}, err
		}
	}
	return ReachabilitySnapshot{
		Epoch: c.reachabilityEpoch, Visible: c.visible, Durable: c.durable,
		Pending: pending, Resources: resources,
	}, nil
}

// RevalidateReachability proves that no candidate was enqueued and no durable
// prefix was consumed since CaptureReachability returned.
func (c *Coordinator) RevalidateReachability(epoch uint64) error {
	if c == nil {
		return ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.terminalErrorLocked(); err != nil {
		return err
	}
	if epoch == 0 || epoch != c.reachabilityEpoch {
		return ErrInvalidCandidate
	}
	return nil
}

func (c *Coordinator) preflightDurableRootLocked(candidate *PreparedRootCandidate) error {
	group := candidate.durableRootGroup()
	if len(group.members) == 0 {
		if c.durableRootSequence != 0 {
			return fmt.Errorf("%w: candidate %d is missing the next durable-root transaction", ErrDurableRootLineage, candidate.frontier.commitSeq)
		}
		for _, entry := range c.pending {
			if entry.candidate.DurableRoot() != nil {
				return fmt.Errorf("%w: candidate %d is missing the pending durable-root lineage", ErrDurableRootLineage, candidate.frontier.commitSeq)
			}
		}
		return nil
	}
	if len(group.members) != 1 || group.members[0].Owner() != ResourceOwnerCandidate {
		return ErrDurableRootOwnership
	}
	next := group.members[0]
	for i := len(c.pending) - 1; i >= 0; i-- {
		previous := c.pending[i].candidate.DurableRoot()
		if previous != nil {
			return validateConsecutiveDurableRootTransactions(previous, next)
		}
	}
	if len(c.pending) != 0 {
		return fmt.Errorf("%w: cannot start durable-root lineage %d behind rootless pending debt", ErrDurableRootLineage, next.Sequence())
	}
	if c.durableRootSequence == 0 {
		return nil
	}
	if next.Lineage() != c.durableRootLineage || c.durableRootSequence == ^uint64(0) || next.Sequence() != c.durableRootSequence+1 {
		return fmt.Errorf("%w: durable lineage sequence=%d candidate=%d", ErrDurableRootLineage, c.durableRootSequence, next.Sequence())
	}
	return nil
}

func transferDurableRootGroup(group durableRootGroupExtension, from, to ResourceOwnerState) error {
	transferred := 0
	for _, transaction := range group.members {
		if err := transaction.transfer(from, to); err != nil {
			for i := transferred - 1; i >= 0; i-- {
				_ = group.members[i].transfer(to, from)
			}
			return err
		}
		transferred++
	}
	return nil
}

func activateDurableRootGroup(group durableRootGroupExtension) error {
	if len(group.members) > 1 {
		return ErrDurableRootOwnership
	}
	for _, transaction := range group.members {
		if err := transaction.activateFromCoordinator(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) waitForAdmission(ctx context.Context, seq, failureGeneration uint64) error {
	for {
		c.mu.Lock()
		if err := c.terminalErrorLocked(); err != nil {
			c.mu.Unlock()
			return err
		}
		if c.durable.commitSeq >= seq ||
			(c.pendingBytes <= HardPendingBytes && uint64(len(c.pending)) <= HardPendingCommits) {
			c.mu.Unlock()
			return nil
		}
		if c.failureGeneration > failureGeneration {
			if seq <= c.lastFailureSeq {
				err := c.lastRetryableError
				c.mu.Unlock()
				return err
			}
			failureGeneration = c.failureGeneration
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// WaitThrough returns only when a durable frontier reaches seq. Installing a
// waiter is an immediate scheduler trigger. Retryable failure is returned to
// every waiter captured by that attempt; a later call explicitly retries.
func (c *Coordinator) WaitThrough(ctx context.Context, seq uint64) error {
	c.mu.Lock()
	if err := c.terminalErrorLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.durable.commitSeq >= seq {
		c.mu.Unlock()
		return nil
	}
	if seq > c.visible.commitSeq {
		c.mu.Unlock()
		return fmt.Errorf("%w: wait sequence %d exceeds visible %d", ErrInvalidCandidate, seq, c.visible.commitSeq)
	}
	waiter := &durabilityWaiter{seq: seq, ch: make(chan error, 1)}
	c.waiters = append(c.waiters, waiter)
	c.requestPublishLocked(WakeWaiter)
	c.mu.Unlock()
	c.signal()
	select {
	case err := <-waiter.ch:
		return err
	case <-ctx.Done():
		c.mu.Lock()
		if err := c.terminalErrorLocked(); err != nil {
			c.removeWaiterLocked(waiter)
			c.mu.Unlock()
			return err
		}
		if c.durable.commitSeq >= seq {
			c.removeWaiterLocked(waiter)
			c.mu.Unlock()
			return nil
		}
		select {
		case err := <-waiter.ch:
			c.mu.Unlock()
			return err
		default:
		}
		c.removeWaiterLocked(waiter)
		c.mu.Unlock()
		return ctx.Err()
	}
}

func (c *Coordinator) removeWaiterLocked(target *durabilityWaiter) {
	removed := false
	for i, waiter := range c.waiters {
		if waiter == target {
			copy(c.waiters[i:], c.waiters[i+1:])
			last := len(c.waiters) - 1
			c.waiters[last] = nil
			c.waiters = c.waiters[:last]
			removed = true
			break
		}
	}
	if removed && c.wakeReason == WakeWaiter {
		if c.publishing {
			c.recomputeRetryPublishRequestLocked(c.activeAttempt)
		} else if c.lastRetryableError != nil {
			c.recomputeRetryPublishRequestLocked(c.retryAttempt)
		} else {
			c.recomputePublishRequestLocked(false)
		}
	}
}

// ReportPublishResult applies the result for the exact active attempt. The
// synchronous scheduler is the attempt owner and opens the reporting window
// only after Publish returns. Tests may use this method to prove stale and
// duplicate reports are rejected, but cannot manufacture state transitions.
func (c *Coordinator) ReportPublishResult(attempt PublishAttempt, result PublishResult) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reportingAttempt || attempt.id == 0 || attempt.id != c.activeAttempt.id ||
		attempt.candidate != c.activeAttempt.candidate || attempt.groupSize != c.activeAttempt.groupSize {
		return fmt.Errorf("%w: stale or mismatched attempt", ErrPublisherProtocol)
	}
	c.reportingAttempt = false
	c.finishPublishLocked(attempt.candidate, attempt.groupSize, result, c.clock.Now().Sub(attempt.started))
	c.activeAttempt = PublishAttempt{}
	return nil
}

func (c *Coordinator) Drain(ctx context.Context) error {
	c.mu.Lock()
	if err := c.terminalErrorLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if len(c.pending) == 0 && !c.publishing {
		c.mu.Unlock()
		return nil
	}
	request := &drainRequest{}
	c.drains = append(c.drains, request)
	c.requestPublishLocked(WakeDrain)
	c.mu.Unlock()
	c.signal()
	defer c.endDrain(request)
	for {
		c.mu.Lock()
		if err := c.terminalErrorLocked(); err != nil {
			c.mu.Unlock()
			return err
		}
		if request.err != nil {
			err := request.err
			c.mu.Unlock()
			return err
		}
		if len(c.pending) == 0 && !c.publishing {
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (c *Coordinator) endDrain(target *drainRequest) {
	c.mu.Lock()
	removed := false
	for i, request := range c.drains {
		if request == target {
			copy(c.drains[i:], c.drains[i+1:])
			last := len(c.drains) - 1
			c.drains[last] = nil
			c.drains = c.drains[:last]
			removed = true
			break
		}
	}
	recomputed := removed && c.wakeReason == WakeDrain
	if recomputed {
		if c.publishing {
			c.recomputeRetryPublishRequestLocked(c.activeAttempt)
		} else if c.lastRetryableError != nil {
			c.recomputeRetryPublishRequestLocked(c.retryAttempt)
		} else {
			c.recomputePublishRequestLocked(false)
		}
	}
	c.mu.Unlock()
	if recomputed {
		c.signal()
	}
}

// Stop cancels a stalled Publisher through its context, fails all waiters, and
// waits for the single scheduler goroutine. It does not silently claim pending
// debt was durable.
func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		err := c.stopErr
		c.mu.Unlock()
		return err
	}
	if !c.stopping {
		c.stopping = true
		c.cancel()
		c.clearPublishRequestLocked()
		c.failWaitersLocked(ErrPublicationStopped)
		c.notifyLocked()
	}
	c.mu.Unlock()
	c.signal()
	select {
	case <-c.done:
		c.mu.Lock()
		if !c.stopped {
			if len(c.pending) != 0 {
				c.stopErr = ErrPublicationStopped
			}
			c.captureRecoveryResourcesLocked()
			c.stopped = true
		}
		err := c.stopErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Coordinator) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	age := time.Duration(0)
	if len(c.pending) != 0 {
		age = c.clock.Now().Sub(c.firstPendingAt)
	}
	return Stats{
		VisibleCommitSeq: c.visible.commitSeq, DurableCommitSeq: c.durable.commitSeq,
		OldestRecoverableCommitSeq: c.oldestRecoverable, PendingCommits: uint64(len(c.pending)),
		PendingBytes: c.pendingBytes, PendingAge: age, LastGroupSize: c.lastGroupSize,
		LastServiceDuration: c.lastService, EWMAServiceDuration: c.ewmaService,
		PublishDelay: c.publishDelayLocked(), AdmissionWaits: c.admissionWaits,
		ActiveBuilders: c.activeBuilders, WaiterCount: uint64(len(c.waiters)),
		PreMetaFailures: c.preMetaFailures, Retries: c.retries, Poisoned: c.poison != nil,
		WakeReason: c.wakeReason, PublishCalls: c.publishCalls,
		TimerPublishes: c.timerPublishes, WaiterPublishes: c.waiterPublishes,
		SoftBytesPublishes: c.softBytesPublishes, HardAdmissionPublishes: c.hardAdmissionPublishes,
		RetryPublishes: c.retryPublishes, DrainPublishes: c.drainPublishes,
		ResourceCoalesces: c.resourceCoalesces, ResourceConflicts: c.resourceConflicts,
		RejectedCandidates: c.rejectedCandidates, Resources: c.resourceStatsLocked(),
	}
}

func (c *Coordinator) run() {
	defer close(c.done)
	for {
		c.mu.Lock()
		if c.stopping {
			c.mu.Unlock()
			return
		}
		var timerC <-chan time.Time
		var timerGeneration uint64
		if c.timer != nil {
			timerC = c.timer.C()
			timerGeneration = c.timerGeneration
		}
		ready := c.publishNow && !c.publishing && len(c.pending) != 0 && c.activeBuilders == 0 && c.poison == nil
		if ready {
			groupSize := len(c.pending)
			group := make([]*PreparedRootCandidate, groupSize)
			for i := range group {
				group[i] = c.pending[i].candidate
			}
			candidate, coalesceErr := coalesceCandidates(group)
			if coalesceErr != nil {
				c.resourceConflicts++
				c.poison = errors.Join(ErrRecoveryRequired, coalesceErr)
				c.failWaitersLocked(c.poison)
				c.clearPublishRequestLocked()
				c.captureRecoveryResourcesLocked()
				c.notifyLocked()
				c.mu.Unlock()
				continue
			}
			c.recordPublishTriggerLocked(c.wakeReason)
			c.publishNow = false
			c.wakeReason = WakeNone
			c.publishing = true
			c.publishCalls++
			if c.lastRetryableError != nil {
				c.retries++
			}
			c.nextAttemptID++
			attempt := PublishAttempt{
				id: c.nextAttemptID, candidate: candidate, groupSize: uint64(groupSize),
				drains: append([]*drainRequest(nil), c.drains...), started: c.clock.Now(),
				waiters: append([]*durabilityWaiter(nil), c.waiters...),
			}
			c.activeAttempt = attempt
			preparer, prepares := c.publisher.(PublisherPreparer)
			if prepares {
				c.preparing = true
			}
			c.mu.Unlock()
			if prepares {
				prepareErr := preparer.Prepare(c.ctx, candidate)
				c.mu.Lock()
				c.preparing = false
				c.notifyLocked()
				c.reportingAttempt = prepareErr != nil
				c.mu.Unlock()
				if prepareErr != nil {
					_ = c.ReportPublishResult(attempt, PublishResult{Outcome: PublishRetryableFailure, Err: prepareErr})
					continue
				}
			}
			result := c.publisher.Publish(c.ctx, candidate)
			c.mu.Lock()
			c.reportingAttempt = true
			c.mu.Unlock()
			_ = c.ReportPublishResult(attempt, result)
			continue
		}
		c.mu.Unlock()
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
		case <-timerC:
			c.mu.Lock()
			c.handleTimerFiredLocked(timerGeneration)
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) finishPublishLocked(candidate *PreparedRootCandidate, groupSize uint64, result PublishResult, service time.Duration) {
	c.publishing = false
	if c.stopping {
		return
	}
	switch result.Outcome {
	case PublishSucceeded:
		durableSeq := result.DurableCommitSeq
		if durableSeq == 0 {
			durableSeq = candidate.frontier.commitSeq
		}
		if result.Err != nil || durableSeq != candidate.frontier.commitSeq {
			err := result.Err
			if err == nil {
				err = fmt.Errorf("durable sequence %d differs from candidate %d", durableSeq, candidate.frontier.commitSeq)
			}
			c.recordRetryableLocked(errors.Join(ErrPublisherProtocol, err), c.activeAttempt)
			return
		}
		oldest := result.OldestRecoverableCommitSeq
		if oldest == 0 {
			oldest = c.oldestRecoverable
		}
		if oldest < c.oldestRecoverable || oldest > durableSeq {
			c.recordRetryableLocked(errors.Join(ErrPublisherProtocol,
				fmt.Errorf("oldest recoverable sequence %d outside monotonic range [%d,%d]", oldest, c.oldestRecoverable, durableSeq)), c.activeAttempt)
			return
		}
		group := candidate.durableRootGroup()
		if err := consumeDurableRootGroup(group); err != nil {
			cause := errors.Join(ErrPublisherProtocol, fmt.Errorf("consume durable-root lineage: %w", err))
			if failErr := failDurableRootGroup(group, cause); failErr != nil {
				cause = errors.Join(cause, failErr)
			}
			c.poison = errors.Join(ErrRecoveryRequired, cause)
			c.retryAttempt = PublishAttempt{}
			c.failWaitersLocked(c.poison)
			c.clearPublishRequestLocked()
			c.notifyLocked()
			c.signal()
			return
		}
		remove := int(groupSize)
		if remove > len(c.pending) {
			remove = len(c.pending)
		}
		var removedBytes uint64
		for _, entry := range c.pending[:remove] {
			removedBytes = saturatingAdd(removedBytes, entry.bytes)
			if set := entry.candidate.resourceSet(); set != nil {
				set.adjustActivePinsByKind(c.resourceActivePins, false)
				set.releaseFrom(ResourceOwnerCoordinator)
			}
		}
		c.pending = append([]pendingEntry(nil), c.pending[remove:]...)
		if removedBytes <= c.pendingBytes {
			c.pendingBytes -= removedBytes
		} else {
			c.pendingBytes = 0
		}
		c.durable = candidate.frontier
		c.durable.commitSeq = durableSeq
		c.oldestRecoverable = oldest
		c.reachabilityEpoch++
		if latest := (DurableRootGroup{members: group.members}).Latest(); latest != nil {
			c.durableRootLineage = latest.Lineage()
			c.durableRootSequence = latest.Sequence()
		}
		c.lastGroupSize = groupSize
		c.lastService = service
		if c.ewmaService == 0 {
			c.ewmaService = service
		} else {
			c.ewmaService = (7*c.ewmaService + service) / 8
		}
		c.lastRetryableError = nil
		c.retryAttempt = PublishAttempt{}
		c.satisfyWaitersLocked()
		c.recomputePublishRequestLocked(true)
	case PublishRetryableFailure:
		c.recordRetryableLocked(result.Err, c.activeAttempt)
	case PublishAmbiguous:
		err := result.Err
		if err == nil {
			err = errors.New("ambiguous target-meta mutation")
		}
		if failErr := failDurableRootGroup(candidate.durableRootGroup(), err); failErr != nil {
			err = errors.Join(err, failErr)
		}
		c.poison = errors.Join(ErrRecoveryRequired, err)
		c.retryAttempt = PublishAttempt{}
		c.failWaitersLocked(c.poison)
		c.clearPublishRequestLocked()
	default:
		c.recordRetryableLocked(fmt.Errorf("%w: unknown outcome %d", ErrPublisherProtocol, result.Outcome), c.activeAttempt)
	}
	c.notifyLocked()
	c.signal()
}

func consumeDurableRootGroup(group durableRootGroupExtension) error {
	for _, transaction := range group.members {
		if err := transaction.consumeFromCoordinator(); err != nil {
			return fmt.Errorf("sequence %d: %w", transaction.Sequence(), err)
		}
	}
	return nil
}

func failDurableRootGroup(group durableRootGroupExtension, cause error) error {
	var errs []error
	for _, transaction := range group.members {
		if err := transaction.failFromCoordinator(cause); err != nil {
			errs = append(errs, fmt.Errorf("fail durable-root sequence %d: %w", transaction.Sequence(), err))
		}
	}
	return errors.Join(errs...)
}

func (c *Coordinator) recordRetryableLocked(err error, attempt PublishAttempt) {
	if err == nil {
		err = errors.New("retryable publication failure")
	}
	c.preMetaFailures++
	c.failureGeneration++
	c.lastRetryableError = err
	c.retryAttempt = attempt
	c.lastFailureSeq = attempt.candidate.frontier.commitSeq
	c.failCapturedWaitersLocked(err, attempt.waiters)
	c.failCapturedDrainsLocked(err, attempt.drains)
	c.recomputeRetryPublishRequestLocked(attempt)
	c.notifyLocked()
}

func (c *Coordinator) clearPublishRequestLocked() {
	c.publishNow = false
	c.wakeReason = WakeNone
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

func (c *Coordinator) recomputePublishRequestLocked(resetWindow bool) {
	c.clearPublishRequestLocked()
	if len(c.pending) == 0 {
		return
	}
	if resetWindow {
		c.firstPendingAt = c.clock.Now()
	}
	switch {
	case c.pendingBytes > HardPendingBytes || uint64(len(c.pending)) > HardPendingCommits:
		c.requestPublishLocked(WakeHardAdmission)
	case len(c.waiters) != 0:
		c.requestPublishLocked(WakeWaiter)
	case c.pendingBytes >= SoftPendingBytes:
		c.requestPublishLocked(WakeSoftBytes)
	case hasLiveDrain(c.drains):
		c.requestPublishLocked(WakeDrain)
	case !c.publishing:
		c.installTimerLocked()
	}
}

func (c *Coordinator) recomputeRetryPublishRequestLocked(attempt PublishAttempt) {
	c.clearPublishRequestLocked()
	if len(c.pending) == 0 {
		return
	}
	start := int(attempt.groupSize)
	if start > len(c.pending) {
		start = len(c.pending)
	}
	remaining := c.pending[start:]
	switch {
	case len(remaining) != 0 && (c.pendingBytes > HardPendingBytes || uint64(len(c.pending)) > HardPendingCommits):
		c.requestPublishLocked(WakeHardAdmission)
	case len(c.waiters) != 0:
		c.requestPublishLocked(WakeWaiter)
	case len(remaining) != 0 && c.pendingBytes >= SoftPendingBytes:
		c.requestPublishLocked(WakeSoftBytes)
	case hasUncapturedDrain(c.drains, attempt.drains):
		c.requestPublishLocked(WakeDrain)
	}
}

func hasUncapturedDrain(current, captured []*drainRequest) bool {
	if len(current) == 0 {
		return false
	}
	set := make(map[*drainRequest]struct{}, len(captured))
	for _, request := range captured {
		set[request] = struct{}{}
	}
	for _, request := range current {
		if request.err == nil {
			if _, ok := set[request]; !ok {
				return true
			}
		}
	}
	return false
}

func hasLiveDrain(current []*drainRequest) bool {
	for _, request := range current {
		if request.err == nil {
			return true
		}
	}
	return false
}

func (c *Coordinator) failCapturedDrainsLocked(err error, captured []*drainRequest) {
	if len(captured) == 0 {
		return
	}
	set := make(map[*drainRequest]struct{}, len(captured))
	for _, request := range captured {
		set[request] = struct{}{}
	}
	for _, request := range c.drains {
		if _, ok := set[request]; ok {
			request.err = err
		}
	}
}

func (c *Coordinator) failCapturedWaitersLocked(err error, captured []*durabilityWaiter) {
	if len(captured) == 0 {
		return
	}
	set := make(map[*durabilityWaiter]struct{}, len(captured))
	for _, waiter := range captured {
		set[waiter] = struct{}{}
	}
	waiters := c.waiters
	remaining := waiters[:0]
	for _, waiter := range waiters {
		if _, ok := set[waiter]; ok {
			waiter.ch <- err
		} else {
			remaining = append(remaining, waiter)
		}
	}
	clear(waiters[len(remaining):])
	c.waiters = remaining
}

func (c *Coordinator) satisfyWaitersLocked() {
	waiters := c.waiters
	remaining := waiters[:0]
	for _, waiter := range waiters {
		if waiter.seq <= c.durable.commitSeq {
			waiter.ch <- nil
		} else {
			remaining = append(remaining, waiter)
		}
	}
	clear(waiters[len(remaining):])
	c.waiters = remaining
}

func (c *Coordinator) failWaitersLocked(err error) {
	for _, waiter := range c.waiters {
		waiter.ch <- err
	}
	clear(c.waiters)
	c.waiters = nil
}

func (c *Coordinator) terminalErrorLocked() error {
	if c.poison != nil {
		return c.poison
	}
	if c.stopping || c.stopped {
		return ErrClosed
	}
	return nil
}

func (c *Coordinator) publishDelayLocked() time.Duration {
	if c.fixedPublishDelay != 0 {
		return c.fixedPublishDelay
	}
	delay := 20 * c.ewmaService
	if delay < minPublishDelay {
		return minPublishDelay
	}
	if delay > maxPublishDelay {
		return maxPublishDelay
	}
	return delay
}

func (c *Coordinator) recordPublishTriggerLocked(reason WakeReason) {
	switch reason {
	case WakeTimer:
		c.timerPublishes++
	case WakeWaiter:
		c.waiterPublishes++
	case WakeSoftBytes:
		c.softBytesPublishes++
	case WakeHardAdmission:
		c.hardAdmissionPublishes++
	case WakeRetry:
		c.retryPublishes++
	case WakeDrain:
		c.drainPublishes++
	}
}

func (c *Coordinator) installTimerLocked() {
	if c.timer != nil {
		return
	}
	remaining := c.publishDelayLocked() - c.clock.Now().Sub(c.firstPendingAt)
	if remaining <= 0 {
		c.requestPublishLocked(WakeTimer)
		return
	}
	c.timerGeneration++
	c.timer = c.clock.NewTimer(remaining)
}

func (c *Coordinator) handleTimerFiredLocked(generation uint64) {
	if c.timer == nil || generation != c.timerGeneration {
		return
	}
	c.timer = nil
	c.requestPublishLocked(WakeTimer)
}

func (c *Coordinator) requestPublishLocked(reason WakeReason) {
	c.publishNow = true
	c.wakeReason = reason
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

func (c *Coordinator) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Coordinator) notifyLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *Coordinator) resourceStatsLocked() []ResourceKindStats {
	sets := make([]*StableResourceSet, 0, len(c.pending))
	for _, entry := range c.pending {
		if set := entry.candidate.resourceSet(); set != nil {
			sets = append(sets, set)
		}
	}
	union, err := UnionStableResourceSets(sets...)
	if err != nil {
		return nil
	}
	current := union.Stats(c.clock.Now())
	byKind := make(map[ResourceKind]ResourceKindStats, len(current)+len(c.resourcePinHighWater))
	for _, stats := range current {
		stats.ActivePins = c.resourceActivePins[stats.Kind]
		if highWater := c.resourcePinHighWater[stats.Kind]; highWater > stats.PinHighWater {
			stats.PinHighWater = highWater
		}
		byKind[stats.Kind] = stats
	}
	for kind, highWater := range c.resourcePinHighWater {
		if _, ok := byKind[kind]; !ok {
			byKind[kind] = ResourceKindStats{Kind: kind, ActivePins: c.resourceActivePins[kind], PinHighWater: highWater}
		}
	}
	result := make([]ResourceKindStats, 0, len(byKind))
	for _, stats := range byKind {
		result = append(result, stats)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Kind < result[j].Kind })
	return result
}

func (c *Coordinator) observeResourcePinHighWaterLocked() {
	for kind, activePins := range c.resourceActivePins {
		if activePins > c.resourcePinHighWater[kind] {
			c.resourcePinHighWater[kind] = activePins
		}
	}
}

func (c *Coordinator) captureRecoveryResourcesLocked() {
	seen := make(map[*StableResourceSet]struct{}, len(c.recoverySets)+len(c.pending))
	for _, set := range c.recoverySets {
		seen[set] = struct{}{}
	}
	for _, entry := range c.pending {
		set := entry.candidate.resourceSet()
		if set == nil {
			continue
		}
		if _, ok := seen[set]; !ok {
			c.recoverySets = append(c.recoverySets, set)
			seen[set] = struct{}{}
		}
	}
	seenRoots := make(map[*DurableRootTransaction]struct{}, len(c.recoveryDurableRoots)+len(c.pending))
	for _, transaction := range c.recoveryDurableRoots {
		seenRoots[transaction] = struct{}{}
	}
	cause := c.poison
	if cause == nil {
		cause = ErrPublicationStopped
	}
	for _, entry := range c.pending {
		for _, transaction := range entry.candidate.durableRootGroup().members {
			if transaction == nil || transaction.Owner() != ResourceOwnerCoordinator {
				continue
			}
			if _, ok := seenRoots[transaction]; ok {
				continue
			}
			if err := transaction.failFromCoordinator(cause); err != nil {
				failure := errors.Join(ErrRecoveryRequired, fmt.Errorf("retain durable-root sequence %d: %w", transaction.Sequence(), err))
				if c.poison != nil {
					c.poison = errors.Join(c.poison, failure)
				} else {
					c.stopErr = errors.Join(c.stopErr, failure)
				}
			}
			c.recoveryDurableRoots = append(c.recoveryDurableRoots, transaction)
			seenRoots[transaction] = struct{}{}
		}
	}
	c.pending = nil
	c.pendingBytes = 0
	clear(c.resourceActivePins)
	c.firstPendingAt = time.Time{}
}

// RecoveryResourceHandoff owns pins retained after shutdown or ambiguous
// publication. Reopen/recovery consumes the evidence and then releases it.
type RecoveryResourceHandoff struct {
	sets         []*StableResourceSet
	durableRoots []*DurableRootTransaction
	once         sync.Once
}

func (handoff *RecoveryResourceHandoff) Len() int {
	if handoff == nil {
		return 0
	}
	return len(handoff.sets)
}

func (handoff *RecoveryResourceHandoff) Sets() []*StableResourceSet {
	if handoff == nil {
		return nil
	}
	return append([]*StableResourceSet(nil), handoff.sets...)
}

func (handoff *RecoveryResourceHandoff) DurableRootLen() int {
	if handoff == nil {
		return 0
	}
	return len(handoff.durableRoots)
}

// DurableRoots returns the ordered exact allocator transactions retained for
// recovery. The returned slice is a copy; ownership stays with the handoff.
func (handoff *RecoveryResourceHandoff) DurableRoots() []*DurableRootTransaction {
	if handoff == nil {
		return nil
	}
	return append([]*DurableRootTransaction(nil), handoff.durableRoots...)
}

func (handoff *RecoveryResourceHandoff) Release() {
	if handoff == nil {
		return
	}
	handoff.once.Do(func() {
		for _, set := range handoff.sets {
			set.releaseFrom(ResourceOwnerRecovery)
		}
		for _, transaction := range handoff.durableRoots {
			transaction.releaseFromRecovery()
		}
		handoff.sets = nil
		handoff.durableRoots = nil
	})
}

// TakeRecoveryHandoff transfers all retained coordinator pins exactly once.
// It is valid only after poison or completed Stop; an empty terminal handoff
// is harmless. In particular it cannot steal handles from an active Publish.
func (c *Coordinator) TakeRecoveryHandoff() (*RecoveryResourceHandoff, error) {
	if c == nil {
		return &RecoveryResourceHandoff{}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped && c.poison == nil {
		return nil, ErrRecoveryHandoffUnavailable
	}
	c.captureRecoveryResourcesLocked()
	sets := append([]*StableResourceSet(nil), c.recoverySets...)
	durableRoots := append([]*DurableRootTransaction(nil), c.recoveryDurableRoots...)
	transferred := make([]*StableResourceSet, 0, len(sets))
	for _, set := range sets {
		if err := set.transfer(ResourceOwnerCoordinator, ResourceOwnerRecovery); err != nil {
			for i := len(transferred) - 1; i >= 0; i-- {
				_ = transferred[i].transfer(ResourceOwnerRecovery, ResourceOwnerCoordinator)
			}
			return nil, fmt.Errorf("%w: recovery ownership transfer: %w", ErrResourceOwnership, err)
		}
		transferred = append(transferred, set)
	}
	transferredRoots := make([]*DurableRootTransaction, 0, len(durableRoots))
	for _, transaction := range durableRoots {
		if err := transaction.transfer(ResourceOwnerCoordinator, ResourceOwnerRecovery); err != nil {
			for i := len(transferredRoots) - 1; i >= 0; i-- {
				_ = transferredRoots[i].transfer(ResourceOwnerRecovery, ResourceOwnerCoordinator)
			}
			for i := len(transferred) - 1; i >= 0; i-- {
				_ = transferred[i].transfer(ResourceOwnerRecovery, ResourceOwnerCoordinator)
			}
			return nil, fmt.Errorf("%w: durable-root recovery ownership transfer: %w", ErrDurableRootOwnership, err)
		}
		transferredRoots = append(transferredRoots, transaction)
	}
	c.recoverySets = nil
	c.recoveryDurableRoots = nil
	return &RecoveryResourceHandoff{sets: transferred, durableRoots: transferredRoots}, nil
}
