package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// NativePublicationOwnerV6 is the original, already admitted caller of the
// existing transaction. Close drives that callers SAME physical stages. It is
// invoked only by an explicit caller/Stop, never by the scheduler. It must call
// CompleteNativePublicationV6 after consumption, or after rooting its original
// first-error receipt and unresolved operands in the existing DB error graph.
type NativePublicationOwnerV6 interface {
	// The original caller detaches physical/callback aliases and unlocks before
	// returning its existing allocation lease. The joiner clears its owner and
	// receipt aliases BEFORE closing that lease. Physical refusal returns no lease.
	CloseNativePublicationV6() (retainedalloc.Lease, error)
}

// NativePublicationV6 is embedded in that admitted original caller. The
// coordinator links this very receipt; there is no separately allocated keeper,
// completion callback or resource owner. All fields are protected by c.mu.
// A released turn and a completed physical receipt are different states.
type NativePublicationV6 struct {
	coordinator       *Coordinator
	owner             NativePublicationOwnerV6
	next              *NativePublicationV6
	candidate         *PreparedRootCandidate
	attempt           PublishAttempt
	firstError        error
	recoveryError     nativeRecoveryErrorV6
	recoveryRequired  bool
	pendingBytes      uint64
	refusedCompletion retainedalloc.Lease
	// The single-member native handoff borrows this admitted backing. It is
	// detached from Coordinator.pending at the real fence, but its candidate
	// alias remains live until all physical consumers finish. Ordinary enqueue
	// continues to own its original independently grown pending backing.
	pending [1]pendingEntry
	// SAME original handoff controls, born in this admitted frame. No native
	// exported/nested builder handle or separately allocated receipt escapes.
	builderLease                  builderLease
	builderHandle                 builderHandle
	builderToken                  BuilderToken
	handoff                       BuilderHandoffReceipt
	builderReady, handoffReady    bool
	bound, turn, fenced, complete bool
}

func (c *Coordinator) pendingNativeOwnedV6() bool {
	for _, e := range c.pending {
		if t := e.candidate.DurableRoot(); t != nil {
			t.mu.Lock()
			native := t.constructionV6 != nil && t.constructionV6.nativeWorkOwned
			t.mu.Unlock()
			if native {
				return true
			}
		}
	}
	return false
}

// BindNativePublicationV6 retains the original caller before a private native
// operation can escape a bounded return. Its containing storage must already be
// admitted by that caller. Ordinary publications remain eligible while no
// native publication turn is held; activeDispatch still joins every receipt.
func (c *Coordinator) BindNativePublicationV6(r *NativePublicationV6, owner NativePublicationOwnerV6, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || owner == nil || w == nil {
		return false, ErrPublisherProtocol
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*r))+64) {
		return false, nil
	}
	notify, ready, err := admitNativeNotifyV6(w)
	if !ready || err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.bound {
		if r.coordinator != c || r.complete {
			return false, ErrPublisherProtocol
		}
		return true, nil
	}
	if err := c.terminalErrorLocked(); err != nil {
		return false, err
	}
	if c.activeDispatch == ^uint64(0) || c.nativeDispatchV6 == ^uint64(0) {
		return false, ErrPublisherProtocol
	}
	r.coordinator, r.owner, r.next, r.bound = c, owner, c.nativePublicationsV6, true
	c.nativePublicationsV6 = r
	c.activeDispatch++
	c.nativeDispatchV6++
	notify.commit(c)
	return true, nil
}

// BeginNativePublicationTurnV6 captures the already enqueued original SINGLE
// candidate. Its actual activation and owned constructor are prerequisites;
// this method neither manufactures a candidate nor runs a publisher callback.
// Native callbacks cannot be stolen by the ordinary scheduler.
func (c *Coordinator) BeginNativePublicationTurnV6(r *NativePublicationV6, candidate *PreparedRootCandidate, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || candidate == nil || w == nil {
		return false, ErrPublisherProtocol
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*r))+128) {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound || r.complete || r.fenced {
		return false, ErrPublisherProtocol
	}
	if r.turn {
		if r.candidate != candidate {
			return false, ErrPublisherProtocol
		}
		return true, nil
	}
	if err := c.terminalErrorLocked(); err != nil {
		return false, err
	}
	if c.publishing || c.activeDispatch != c.nativeDispatchV6 || c.activeBuilders != 0 {
		return false, nil
	}
	if len(c.pending) != 1 || c.pending[0].candidate != candidate {
		return false, ErrPublisherProtocol
	}
	notify, ready, err := admitNativeNotifyV6(w)
	if !ready || err != nil {
		return false, err
	}
	clock, ready, err := c.admitNativeClockLockedV6(w)
	if !ready || err != nil {
		return false, err
	}
	c.startNativePublicationTurnLockedV6(r, candidate, &notify, &clock)
	return true, nil
}

// Eligibility and the actual clock/notification are admitted before activation.
// The final enqueue caller executes this under the SAME c.mu after consuming its
// original builder, leaving no interval for ordinary eligibility mutation.
func (c *Coordinator) startNativePublicationTurnLockedV6(r *NativePublicationV6, candidate *PreparedRootCandidate, notify *nativeNotifyPermitV6, clock *nativeClockPermitV6) {
	c.nextAttemptID++
	r.candidate = candidate
	r.pendingBytes = c.pending[0].bytes
	r.attempt = PublishAttempt{id: c.nextAttemptID, candidate: candidate, groupSize: 1, started: clock.now(c)}
	r.turn = true
	c.activeAttempt = r.attempt
	c.publishing = true
	c.publishNow = false
	c.wakeReason = WakeNone
	c.publishCalls++
	notify.commit(c)
}

// NativePublicationCompletionPlanV6 is an invocation-local plan for actual
// fixed original completion operations. It cannot survive a yield, attach to a
// cursor, or grant callbacks a replacement budget. Admit it before authority;
// Complete performs no later work reservation or mutable queue traversal.
type NativePublicationCompletionPlanV6 struct {
	coordinator *Coordinator
	receipt     *NativePublicationV6
	candidate   *PreparedRootCandidate
	notify      nativeNotifyPermitV6
	wake        nativeWakePermitV6
	clock       nativeClockPermitV6
	admitted    bool
}

// The first cause is held in the SAME admitted receipt. This view has no owner,
// root or callback aliases, no error slice allocation, and never changes once
// it has been published under c.mu. Known unfenced is not PublishAmbiguous.
type nativeRecoveryErrorV6 struct{ cause error }

func (e *nativeRecoveryErrorV6) Error() string        { return e.cause.Error() }
func (e *nativeRecoveryErrorV6) Unwrap() error        { return e.cause }
func (e *nativeRecoveryErrorV6) Is(target error) bool { return target == ErrRecoveryRequired }

// AdmitNativePublicationCompletionV6 covers both success and the sticky genuine
// hard-I/O disposition. Timer stop is priced for an original real timer even
// when it is absent now: enqueue may install it before this plan is consumed.
func (c *Coordinator) AdmitNativePublicationCompletionV6(r *NativePublicationV6, candidate *PreparedRootCandidate, w *iterator.OrdinalScanWork) (NativePublicationCompletionPlanV6, bool, error) {
	var p NativePublicationCompletionPlanV6
	if c == nil || r == nil || candidate == nil || w == nil {
		return p, false, ErrPublisherProtocol
	}
	if !w.Reserve(4, 2*uint64(unsafe.Sizeof(p))+2*uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(candidate))+uint64(unsafe.Sizeof(DurableRootTransaction{}))+uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) {
		return p, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound || r.complete || r.recoveryRequired || r.turn || r.fenced {
		return p, false, ErrPublisherProtocol
	}
	if _, ok := c.clock.(realClock); !ok {
		return p, false, ErrNativeMVCCRootWorkRequiredV6
	}
	// Group/singleton identity is inspected only after header admission above.
	if len(candidate.durableRootGroup().members) != 1 || candidate.DurableRoot() == nil {
		return p, false, ErrDurableRootOwnership
	}
	var ready bool
	var err error
	p.notify, ready, err = admitNativeNotifyV6(w)
	if !ready || err != nil {
		return p, false, err
	}
	p.wake, ready, err = admitNativeWakeV6(w)
	if !ready || err != nil {
		return p, false, err
	}
	p.clock, ready, err = c.admitNativeClockLockedV6(w)
	if !ready || err != nil {
		return p, false, err
	}
	// Actual stop+clear (upper branch), exact pending-head detach/subtraction,
	// frontier/service/lineage, attempt clears, error-view latch, dirty request,
	// publishing/turn transitions. No waiter/drain iteration or heap error view.
	if !w.Reserve(19, uint64(unsafe.Sizeof(c.timer))+uint64(unsafe.Sizeof(time.Timer{}))+2*uint64(unsafe.Sizeof(pendingEntry{}))+uint64(unsafe.Sizeof(primaryPublicationCompletionV6{}))+4*uint64(unsafe.Sizeof(Frontier{}))+2*uint64(unsafe.Sizeof(PublishAttempt{}))+2*uint64(unsafe.Sizeof(nativeRecoveryErrorV6{}))+2*uint64(unsafe.Sizeof(c.pending))+16*8) {
		return p, false, nil
	}
	p.coordinator, p.receipt, p.candidate, p.admitted = c, r, candidate, true
	return p, true, nil
}

// Complete consumes the original invocation-local plan only after the physical
// publisher returned and unlocked. hardIO must mean a genuine storage failure,
// never budget/unsupported/stale/cancellation/contention. Its error state keeps
// the entire original pending receipt and both dispatch counters linked.
func (p *NativePublicationCompletionPlanV6) Complete(result PublishResult, hardIO bool) (bool, error) {
	if p == nil || !p.admitted {
		return false, ErrPublisherProtocol
	}
	c, r := p.coordinator, p.receipt
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.turn || r.complete || r.fenced || c.activeAttempt.id != r.attempt.id || c.activeAttempt.candidate != p.candidate || r.candidate != p.candidate || len(c.pending) == 0 || c.pending[0].candidate != p.candidate || c.pending[0].bytes != r.pendingBytes || c.pendingBytes < r.pendingBytes {
		return false, ErrPublisherProtocol
	}
	if result.Outcome != PublishSucceeded {
		if !hardIO || result.Err == nil {
			return false, ErrPublisherProtocol
		}
		r.firstError = result.Err
		r.recoveryError.cause = result.Err
		r.recoveryRequired = true
		if c.poison == nil {
			c.poison = &r.recoveryError
		}
		if c.timer != nil {
			c.timer.Stop()
			c.timer = nil
		}
		c.publishNow, c.wakeReason, c.nativeDemandDirtyV6 = false, WakeNone, false
		c.activeAttempt, r.attempt = PublishAttempt{}, PublishAttempt{}
		c.publishing, r.turn = false, false
		p.admitted = false
		p.notify.commit(c)
		p.wake.commit(c)
		return false, &r.recoveryError
	}
	seq, oldest := result.DurableCommitSeq, result.OldestRecoverableCommitSeq
	if seq == 0 {
		seq = p.candidate.frontier.commitSeq
	}
	if oldest == 0 {
		oldest = c.oldestRecoverable
	}
	if result.Err != nil || seq != p.candidate.frontier.commitSeq || oldest < c.oldestRecoverable || oldest > seq {
		return false, ErrPublisherProtocol
	}
	t := p.candidate.DurableRoot()
	t.mu.Lock()
	consumed := t.phase == durableRootFinishing || t.phase == durableRootConsumed
	lineage, sequence := t.input.Lineage, t.input.Sequence
	t.mu.Unlock()
	if !consumed {
		return false, ErrDurableRootOwnership
	}
	now := p.clock.now(c)
	done := primaryPublicationCompletionV6{frontier: p.candidate.frontier, lineage: lineage, sequence: sequence, oldest: oldest, groupSize: 1, service: now.Sub(r.attempt.started), success: true}
	// Initial cap1 means any later ordinary append already owns grown backing.
	// Touch only the original native head; never copy/clear an ordinary tail.
	c.pending[0] = pendingEntry{}
	c.pendingBytes -= r.pendingBytes
	if len(c.pending) == 1 {
		c.pending = nil
	} else {
		c.pending = c.pending[1:]
	}
	c.activeAttempt, r.attempt = PublishAttempt{}, PublishAttempt{}
	r.fenced, r.turn = true, false
	c.completePublicationScalarsLockedV6(done)
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.firstPendingAt = now
	c.publishNow, c.wakeReason, c.nativeDemandDirtyV6 = false, WakeNone, true
	p.admitted = false
	p.notify.commit(c)
	p.wake.commit(c)
	return true, nil
}

// ReleaseNativePublicationTurnV6 reports the actual successful fence ONLY after
// the original transaction Consume phase has completed. It advances authority
// exactly once. It does NOT finish resources, decrement pending resource pins,
// refund constructor storage, or settle the original dispatch join. Physical
// completion later must never reapply this older frontier over a successor.
func (c *Coordinator) ReleaseNativePublicationTurnV6(r *NativePublicationV6, result PublishResult, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || w == nil {
		return false, ErrPublisherProtocol
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*r))+128) {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.turn || r.complete {
		return false, ErrPublisherProtocol
	}
	if r.fenced {
		return true, r.firstError
	}
	if c.activeAttempt.id != r.attempt.id || c.activeAttempt.candidate != r.candidate || len(c.pending) == 0 || c.pending[0].candidate != r.candidate {
		return false, ErrPublisherProtocol
	}
	// The narrow diagnostic admits the actual singleton/zero-waiter original
	// completion branch. A returned cursor never holds this turn. Unknown
	// waiter/group geometry refuses before any completion mutation.
	if !w.Reserve(2, uint64(unsafe.Sizeof(c.pending))+uint64(unsafe.Sizeof(c.waiters))+uint64(unsafe.Sizeof(durableRootGroupExtension{}))) {
		return false, nil
	}
	group := r.candidate.durableRootGroup()
	if len(c.pending) != 1 || len(c.waiters) != 0 || len(group.members) != 1 {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	notify, ready, err := admitNativeNotifyV6(w)
	if !ready || err != nil {
		return false, err
	}
	wake, ready, err := admitNativeWakeV6(w)
	if !ready || err != nil {
		return false, err
	}
	stop, ready, err := c.admitNativeTimerStopLockedV6(w)
	if !ready || err != nil {
		return false, err
	}
	if result.Outcome != PublishSucceeded {
		// First-error receipt, optional joinError + two-error backing, empty
		// waiter disposal, and poison/request fields are fixed original work.
		if !w.Reserve(6, 2*uint64(unsafe.Sizeof(result.Err))+uint64(unsafe.Sizeof([]error{}))+2*uint64(unsafe.Sizeof(error(nil)))+uint64(unsafe.Sizeof(c.waiters))+uint64(unsafe.Sizeof(c.poison))+uint64(unsafe.Sizeof(c.publishNow))+uint64(unsafe.Sizeof(c.wakeReason))+2*uint64(unsafe.Sizeof(PublishAttempt{}))+16) {
			return false, nil
		}
		cause := result.Err
		if cause == nil {
			cause = ErrPublisherProtocol
		}
		if r.firstError == nil {
			r.firstError = cause
		}
		// No success transition on retry/ambiguity. The SAME original receipt stays
		// reachable; its caller applies the existing physical failure disposition.
		if result.Outcome == PublishAmbiguous {
			if c.poison == nil {
				c.poison = errors.Join(ErrRecoveryRequired, cause)
			}
			c.failWaitersLocked(c.poison)
			c.clearPublishRequestLockedWithNativeV6(&stop)
		}
		// Exactly as ordinary finishPublishLocked, a failed invocation ends its
		// publication turn. The original pending entry/candidate and native join
		// remain retained error/recovery custody; no containing refund is allowed.
		c.activeAttempt, r.attempt = PublishAttempt{}, PublishAttempt{}
		c.publishing, r.turn = false, false
		notify.commit(c)
		wake.commit(c)
		return false, cause
	}
	candidate := r.candidate
	seq := result.DurableCommitSeq
	if seq == 0 {
		seq = candidate.frontier.commitSeq
	}
	oldest := result.OldestRecoverableCommitSeq
	if oldest == 0 {
		oldest = c.oldestRecoverable
	}
	if result.Err != nil || seq != candidate.frontier.commitSeq || oldest < c.oldestRecoverable || oldest > seq {
		return false, ErrPublisherProtocol
	}
	clock, ready, err := c.admitNativeClockLockedV6(w)
	if !ready || err != nil {
		return false, err
	}
	// Singleton transaction inspection is admitted before dereferencing it.
	if !w.Reserve(1, uint64(unsafe.Sizeof(DurableRootTransaction{}))+uint64(unsafe.Sizeof(DurableRootCallbackInput{}))) {
		return false, nil
	}
	// Canonical completion: pending detach/bytes; two attempt clears; fenced
	// flag; durable+oldest; epoch; lineage; service+EWMA; retry; publishing;
	// empty waiter header; request flags and empty-pending branch.
	if !w.Reserve(14, uint64(unsafe.Sizeof(primaryPublicationCompletionV6{}))+4*uint64(unsafe.Sizeof(Frontier{}))+2*uint64(unsafe.Sizeof(PublishAttempt{}))+2*uint64(unsafe.Sizeof(c.pending))+2*uint64(unsafe.Sizeof(c.waiters))+12*uint64(unsafe.Sizeof(uint64(0)))) {
		return false, nil
	}
	var lineage DurableRootLineageID
	var sequence uint64
	for _, transaction := range group.members {
		transaction.mu.Lock()
		consumed := transaction.phase == durableRootFinishing || transaction.phase == durableRootConsumed
		lineage, sequence = transaction.input.Lineage, transaction.input.Sequence
		transaction.mu.Unlock()
		if !consumed {
			return false, ErrDurableRootOwnership
		}
	}
	done := primaryPublicationCompletionV6{frontier: candidate.frontier, lineage: lineage, sequence: sequence, oldest: oldest, groupSize: 1, service: clock.now(c).Sub(r.attempt.started), success: true}
	c.removeCompletedPendingPrefixV6(1, c.pending[0].bytes)
	c.activeAttempt = PublishAttempt{}
	r.attempt = PublishAttempt{}
	r.fenced = true
	c.completePublicationLockedWithNativeV6(done, &stop, &notify)
	wake.commit(c)
	return true, nil
}

// NativePublicationRecoveryErrorV6 borrows only the immutable error view. It
// neither consumes nor releases the still-linked physical receipt.
func (c *Coordinator) NativePublicationRecoveryErrorV6(r *NativePublicationV6, w *iterator.OrdinalScanWork) (error, bool) {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))) {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound {
		return nil, true
	}
	if r.recoveryRequired {
		return &r.recoveryError, true
	}
	return nil, true
}

// CompleteNativePublicationV6 settles ONLY the original physical join. On hard
// refusal retained must mean the SAME receipt/operands are already rooted in
// the original DB error graph; an error alone is not lifetime custody. A failed
// late Finish does not poison a later genuine ordinary durable publication.
func (c *Coordinator) CompleteNativePublicationV6(r *NativePublicationV6, physicalErr error, retained bool) error {
	_, err := c.completeNativePublicationWithWorkV6(r, physicalErr, retained, nil)
	return err
}

// CompleteNativePublicationWithWorkV6 prices original list traversal, inline
// alias detachment, counters and notify/wake before terminal frame refund.
// False retains the same receipt/frame; it never authorizes an ordinary retry.
func (c *Coordinator) CompleteNativePublicationWithWorkV6(r *NativePublicationV6, physicalErr error, retained bool, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	return c.completeNativePublicationWithWorkV6(r, physicalErr, retained, w)
}

func (c *Coordinator) completeNativePublicationWithWorkV6(r *NativePublicationV6, physicalErr error, retained bool, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil {
		return false, ErrPublisherProtocol
	}
	var notify nativeNotifyPermitV6
	var wake nativeWakePermitV6
	if w != nil {
		if !w.Reserve(3, uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(builderHandle{}))+uint64(unsafe.Sizeof(builderLease{}))+uint64(unsafe.Sizeof(BuilderHandoffReceipt{}))+4*uint64(unsafe.Sizeof(uint64(0)))) {
			return false, nil
		}
		var ready bool
		var err error
		notify, ready, err = admitNativeNotifyV6(w)
		if !ready || err != nil {
			return false, err
		}
		wake, ready, err = admitNativeWakeV6(w)
		if !ready || err != nil {
			return false, err
		}
		// At most one original protocol-error Join escapes this invocation.
		// Its public []error representation plus two interfaces is admitted;
		// no physical backing or new owner is hidden in that diagnostic value.
		if !w.Reserve(1, uint64(unsafe.Sizeof([]error{}))+2*uint64(unsafe.Sizeof(error(nil)))) {
			return false, nil
		}
	}
	// Inline original builder controls retain their normal lock discipline.
	// Never inspect or scrub these fields under coordinator.mu alone.
	r.builderHandle.mu.Lock()
	defer r.builderHandle.mu.Unlock()
	r.builderLease.mu.Lock()
	defer r.builderLease.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound {
		return false, ErrPublisherProtocol
	}
	if r.complete {
		return true, r.firstError
	}
	if r.recoveryRequired {
		return false, &r.recoveryError
	}
	if physicalErr != nil && !retained {
		return false, errors.Join(physicalErr, ErrPublisherProtocol)
	}
	// An error-graph claim alone cannot refund storage still borrowed by the
	// coordinator. In particular an unfenced/ambiguous handoff retains this
	// SAME receipt until the original pending/error disposition detaches it.
	// Join keeps any returned lease in refusedCompletion on this refusal.
	if len(c.pending) != 0 && &c.pending[0] == &r.pending[0] {
		return false, errors.Join(physicalErr, ErrPublisherProtocol)
	}
	if r.builderReady && (!r.builderHandle.released || !r.builderLease.released) {
		return false, errors.Join(physicalErr, ErrBuilderLease)
	}
	if r.turn && !r.fenced && physicalErr == nil {
		return false, ErrPublisherProtocol
	}
	link := &c.nativePublicationsV6
	for {
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(link))+uint64(unsafe.Sizeof(r.next))) {
			return false, nil
		}
		if *link == nil || *link == r {
			break
		}
		link = &(*link).next
	}
	if *link != r || c.activeDispatch == 0 || c.nativeDispatchV6 == 0 {
		return false, ErrPublisherProtocol
	}
	if w != nil && !w.Reserve(6, 2*uint64(unsafe.Sizeof(*r))+2*uint64(unsafe.Sizeof(PublishAttempt{}))+4*uint64(unsafe.Sizeof(uint64(0)))) {
		return false, nil
	}
	if r.firstError == nil {
		r.firstError = physicalErr
	}
	// Unfenced failure releases the attempted turn only after original custody
	// transfer; ambiguity poison/frontier semantics remain unchanged.
	if r.turn && !r.fenced {
		if c.activeAttempt.id != r.attempt.id {
			return false, ErrPublisherProtocol
		}
		c.activeAttempt = PublishAttempt{}
		c.publishing = false
	}
	*link = r.next
	r.next, r.owner, r.candidate, r.attempt = nil, nil, nil, PublishAttempt{}
	r.pending[0] = pendingEntry{}
	// These controls were never independently exported. All original consumers
	// have ended before this final completion; clear their backreferences while
	// the containing frame is still owned. Do not copy/reset live mutex values.
	r.builderToken.handle = nil
	r.builderHandle.lease = nil
	r.builderLease.coordinator = nil
	r.handoff = BuilderHandoffReceipt{}
	r.complete = true
	c.activeDispatch--
	c.nativeDispatchV6--
	if w != nil {
		notify.commit(c)
		wake.commit(c)
	} else {
		c.notifyLocked()
		c.signal()
	}
	return true, r.firstError
}

// HasOutstandingNativePublicationsV6 is the original protected teardown
// disposition. A failed join is not permission to detach the coordinator while
// either its linked original receipt or its dispatch still owns native work.
func (c *Coordinator) HasOutstandingNativePublicationsV6() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nativePublicationsV6 != nil || c.nativeDispatchV6 != 0
}

// JoinNativePublicationsV6 is explicit Close/Stop joining of the original
// receipt, outside every coordinator lock. In particular DB.Close must call it
// before acquiring teardownMu, not only in its later capture-teardown hooks.
// The original owner serializes Close against bounded cursor progress.
func (c *Coordinator) JoinNativePublicationsV6(ctx context.Context) error {
	if c == nil {
		return nil
	}
	var first error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(first, err)
		}
		c.mu.Lock()
		r := c.nativePublicationsV6
		if r == nil {
			c.mu.Unlock()
			return first
		}
		owner := r.owner
		if r.recoveryRequired {
			err := &r.recoveryError
			c.mu.Unlock()
			return err
		}
		if r.fenced && r.firstError != nil {
			err := r.firstError
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		if owner == nil {
			return errors.Join(first, ErrPublisherProtocol)
		}
		completion, err := owner.CloseNativePublicationV6()
		owner = nil
		c.mu.Lock()
		complete := r.complete
		lateFailure := !complete && r.fenced && err != nil
		if lateFailure && r.firstError == nil {
			r.firstError = err
		}
		if !complete && completion != nil {
			// A protocol refusal cannot discard the original returned admission.
			// The still-linked, preadmitted receipt retains it without refund.
			if r.refusedCompletion != nil {
				c.mu.Unlock()
				panic("duplicate native completion lease")
			}
			r.refusedCompletion, completion = completion, nil
		}
		c.mu.Unlock()
		if lateFailure {
			err = r.firstError
		}
		r = nil
		if err != nil && first == nil {
			first = err
		}
		if lateFailure {
			return first
		}
		if !complete {
			return errors.Join(first, fmt.Errorf("%w: native Close did not settle original join", ErrPublisherProtocol))
		}
		// Only scalar accounting remains, outside the original containing owner.
		if completion != nil {
			completion.Close()
			completion = nil
		}
	}
}

// ConsumeNativePublicationResourcesV6 hands the SAME constructor Set edge to
// genuine Coordinator-owner Finish after the physical fence. It consumes no
// physical edge, performs no shared-visible-clone proof, and touches no global
// pin map: the admitted immutable empty native Set contributed zero pins.
// visible is retained only for source API compatibility and is not borrowed.
// The final caller must preflight the entire remaining Consume/turn capacity.
func (c *Coordinator) ConsumeNativePublicationResourcesV6(r *NativePublicationV6, result PublishResult, visible *StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || w == nil {
		return false, ErrPublisherProtocol
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*r))) {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.turn || r.fenced || r.complete ||
		c.activeAttempt.id != r.attempt.id || c.activeAttempt.candidate != r.candidate ||
		len(c.pending) == 0 || c.pending[0].candidate != r.candidate ||
		result.Outcome != PublishSucceeded || result.Err != nil ||
		(result.DurableCommitSeq != 0 && result.DurableCommitSeq != r.candidate.frontier.commitSeq) {
		return false, ErrPublisherProtocol
	}
	txn := r.candidate.DurableRoot()
	if txn == nil {
		return false, ErrDurableRootOwnership
	}
	return txn.handoffPrimaryResourcesToFinishV6(w)
}

// ConsumeNativePublicationTransactionV6 consumes the SAME activated transaction
// after the original successful fence and resource-edge consumption. The
// enclosing driver preadmits its final window; false never authorizes Finish or
// turn release. No callback is run by the ordinary scheduler on this path.
func (c *Coordinator) ConsumeNativePublicationTransactionV6(r *NativePublicationV6, result PublishResult, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || w == nil {
		return false, ErrPublisherProtocol
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(DurableRootTransaction{}))+uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.turn || r.fenced || r.complete ||
		c.activeAttempt.id != r.attempt.id || c.activeAttempt.candidate != r.candidate ||
		result.Outcome != PublishSucceeded || result.Err != nil ||
		(result.DurableCommitSeq != 0 && result.DurableCommitSeq != r.candidate.frontier.commitSeq) {
		return false, ErrPublisherProtocol
	}
	txn := r.candidate.DurableRoot()
	if txn == nil {
		return false, ErrDurableRootOwnership
	}
	txn.mu.Lock()
	construction := txn.constructionV6
	consumed := construction != nil && (construction.setReleaseConsumed || construction.setFinishHandoff)
	txn.mu.Unlock()
	if !consumed {
		return false, ErrDurableRootOwnership
	}
	return txn.consumeFromCoordinatorStepWithWorkV6(w)
}
