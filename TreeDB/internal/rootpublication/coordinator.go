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
	Clock                      Clock
	Publisher                  Publisher
	InitialDurableFrontier     Frontier
	OldestRecoverableCommitSeq uint64
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

	clock     Clock
	publisher Publisher
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	changed   chan struct{}
	done      chan struct{}

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

	ewmaService        time.Duration
	lastService        time.Duration
	lastGroupSize      uint64
	admissionWaits     uint64
	preMetaFailures    uint64
	retries            uint64
	publishCalls       uint64
	failureGeneration  uint64
	lastRetryableError error
	lastFailureSeq     uint64
	nextAttemptID      uint64
	activeAttempt      PublishAttempt
	retryAttempt       PublishAttempt
	reportingAttempt   bool

	resourceCoalesces    uint64
	resourceConflicts    uint64
	rejectedCandidates   uint64
	recoverySets         []*StableResourceSet
	resourcePinHighWater map[ResourceKind]uint64
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
	if options.OldestRecoverableCommitSeq > options.InitialDurableFrontier.commitSeq {
		return nil, fmt.Errorf("root publication: oldest recoverable sequence %d exceeds durable sequence %d",
			options.OldestRecoverableCommitSeq, options.InitialDurableFrontier.commitSeq)
	}
	clock := options.Clock
	if clock == nil {
		clock = realClock{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		clock: clock, publisher: options.Publisher, ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{}), wakeReason: WakeNone,
		visible: options.InitialDurableFrontier, durable: options.InitialDurableFrontier,
		oldestRecoverable: options.OldestRecoverableCommitSeq,
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

func (c *Coordinator) enqueue(ctx context.Context, candidate *PreparedRootCandidate, supersede bool) error {
	c.mu.Lock()
	if err := c.terminalErrorLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if candidate == nil {
		c.mu.Unlock()
		return fmt.Errorf("%w: nil candidate", ErrInvalidCandidate)
	}
	if candidate.frontier.commitSeq <= c.visible.commitSeq ||
		(c.visible.commitSeq != 0 && !candidate.frontier.Dominates(c.visible)) {
		c.mu.Unlock()
		return fmt.Errorf("%w: frontier %d does not dominate visible %d", ErrInvalidCandidate, candidate.frontier.commitSeq, c.visible.commitSeq)
	}
	if supersede && len(c.pending) == 0 {
		c.mu.Unlock()
		return fmt.Errorf("%w: no pending candidate to supersede", ErrInvalidCandidate)
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
			c.mu.Unlock()
			return err
		}
		if physical := stableSetPhysicalCount(sets); physical > union.Len() {
			c.resourceCoalesces = saturatingAdd(c.resourceCoalesces, uint64(physical-union.Len()))
		}
		if err := candidateSet.transfer(ResourceOwnerCandidate, ResourceOwnerCoordinator); err != nil {
			c.rejectedCandidates++
			c.mu.Unlock()
			return fmt.Errorf("%w: enqueue resource transfer: %w", ErrInvalidCandidate, err)
		}
	}
	wasEmpty := len(c.pending) == 0
	entryBytes := candidate.OwnedBytes()
	c.pending = append(c.pending, pendingEntry{candidate: candidate, bytes: entryBytes})
	c.pendingBytes = saturatingAdd(c.pendingBytes, entryBytes)
	c.observeResourcePinHighWaterLocked()
	c.visible = candidate.frontier
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
	failureGeneration := c.failureGeneration
	seq := candidate.frontier.commitSeq
	c.mu.Unlock()
	c.signal()
	if !hard {
		return nil
	}
	return c.waitForAdmission(ctx, seq, failureGeneration)
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
			c.mu.Unlock()
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
		remove := int(groupSize)
		if remove > len(c.pending) {
			remove = len(c.pending)
		}
		var removedBytes uint64
		for _, entry := range c.pending[:remove] {
			removedBytes = saturatingAdd(removedBytes, entry.bytes)
			if set := entry.candidate.resourceSet(); set != nil {
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
	delay := 20 * c.ewmaService
	if delay < minPublishDelay {
		return minPublishDelay
	}
	if delay > maxPublishDelay {
		return maxPublishDelay
	}
	return delay
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
		if highWater := c.resourcePinHighWater[stats.Kind]; highWater > stats.PinHighWater {
			stats.PinHighWater = highWater
		}
		byKind[stats.Kind] = stats
	}
	for kind, highWater := range c.resourcePinHighWater {
		if _, ok := byKind[kind]; !ok {
			byKind[kind] = ResourceKindStats{Kind: kind, PinHighWater: highWater}
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
	sets := make([]*StableResourceSet, 0, len(c.pending))
	for _, entry := range c.pending {
		if set := entry.candidate.resourceSet(); set != nil {
			sets = append(sets, set)
		}
	}
	union, err := UnionStableResourceSets(sets...)
	if err != nil {
		return
	}
	if c.resourcePinHighWater == nil {
		c.resourcePinHighWater = make(map[ResourceKind]uint64)
	}
	for _, stats := range union.Stats(c.clock.Now()) {
		if stats.ActivePins > c.resourcePinHighWater[stats.Kind] {
			c.resourcePinHighWater[stats.Kind] = stats.ActivePins
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
	c.pending = nil
	c.pendingBytes = 0
	c.firstPendingAt = time.Time{}
}

// RecoveryResourceHandoff owns pins retained after shutdown or ambiguous
// publication. Reopen/recovery consumes the evidence and then releases it.
type RecoveryResourceHandoff struct {
	sets []*StableResourceSet
	once sync.Once
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

func (handoff *RecoveryResourceHandoff) Release() {
	if handoff == nil {
		return
	}
	handoff.once.Do(func() {
		for _, set := range handoff.sets {
			set.releaseFrom(ResourceOwnerRecovery)
		}
		handoff.sets = nil
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
	c.recoverySets = nil
	return &RecoveryResourceHandoff{sets: transferred}, nil
}
