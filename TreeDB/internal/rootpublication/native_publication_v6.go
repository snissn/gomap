package rootpublication

import (
	"context"
	"errors"
	"fmt"
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
	coordinator                   *Coordinator
	owner                         NativePublicationOwnerV6
	next                          *NativePublicationV6
	candidate                     *PreparedRootCandidate
	attempt                       PublishAttempt
	firstError                    error
	refusedCompletion             retainedalloc.Lease
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
	c.notifyLocked()
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
	c.nextAttemptID++
	r.candidate = candidate
	r.attempt = PublishAttempt{id: c.nextAttemptID, candidate: candidate, groupSize: 1, started: c.clock.Now()}
	r.turn = true
	c.activeAttempt = r.attempt
	c.publishing = true
	c.publishNow = false
	c.wakeReason = WakeNone
	c.publishCalls++
	c.notifyLocked()
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
	if result.Outcome != PublishSucceeded {
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
			c.poison = errors.Join(ErrRecoveryRequired, cause)
			c.failWaitersLocked(c.poison)
			c.clearPublishRequestLocked()
		}
		c.notifyLocked()
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
	var lineage DurableRootLineageID
	var sequence uint64
	for _, transaction := range candidate.durableRootGroup().members {
		transaction.mu.Lock()
		consumed := transaction.phase == durableRootFinishing || transaction.phase == durableRootConsumed
		lineage, sequence = transaction.input.Lineage, transaction.input.Sequence
		transaction.mu.Unlock()
		if !consumed {
			return false, ErrDurableRootOwnership
		}
	}
	done := primaryPublicationCompletionV6{frontier: candidate.frontier, lineage: lineage, sequence: sequence, oldest: oldest, groupSize: 1, service: c.clock.Now().Sub(r.attempt.started), success: true}
	c.removeCompletedPendingPrefixV6(1, c.pending[0].bytes)
	c.activeAttempt = PublishAttempt{}
	r.attempt = PublishAttempt{}
	r.fenced = true
	c.completePublicationLockedV6(done)
	c.signal()
	return true, nil
}

// CompleteNativePublicationV6 settles ONLY the original physical join. On hard
// refusal retained must mean the SAME receipt/operands are already rooted in
// the original DB error graph; an error alone is not lifetime custody. A failed
// late Finish does not poison a later genuine ordinary durable publication.
func (c *Coordinator) CompleteNativePublicationV6(r *NativePublicationV6, physicalErr error, retained bool) error {
	if c == nil || r == nil {
		return ErrPublisherProtocol
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.bound {
		return ErrPublisherProtocol
	}
	if r.complete {
		return r.firstError
	}
	if physicalErr != nil && !retained {
		return errors.Join(physicalErr, ErrPublisherProtocol)
	}
	if r.turn && !r.fenced && physicalErr == nil {
		return ErrPublisherProtocol
	}
	if r.firstError == nil {
		r.firstError = physicalErr
	}
	// Unfenced failure releases the attempted turn only after original custody
	// transfer; ambiguity poison/frontier semantics remain unchanged.
	if r.turn && !r.fenced {
		if c.activeAttempt.id != r.attempt.id {
			return ErrPublisherProtocol
		}
		c.activeAttempt = PublishAttempt{}
		c.publishing = false
	}
	link := &c.nativePublicationsV6
	for *link != nil && *link != r {
		link = &(*link).next
	}
	if *link != r || c.activeDispatch == 0 || c.nativeDispatchV6 == 0 {
		return ErrPublisherProtocol
	}
	*link = r.next
	r.next, r.owner, r.candidate, r.attempt = nil, nil, nil, PublishAttempt{}
	r.complete = true
	c.activeDispatch--
	c.nativeDispatchV6--
	c.notifyLocked()
	c.signal()
	return r.firstError
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
		c.mu.Unlock()
		if owner == nil {
			return errors.Join(first, ErrPublisherProtocol)
		}
		completion, err := owner.CloseNativePublicationV6()
		owner = nil
		c.mu.Lock()
		complete := r.complete
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
		r = nil
		if err != nil && first == nil {
			first = err
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

// ConsumeNativePublicationResourcesV6 consumes only the original constructor's
// shared Set edges after the SAME successful physical fence, before transaction
// Consume and turn release. The exact visible clone is protected by its Set
// lock; its lifetime need not extend into native Finish. Existing resource pin
// contributions end at these actual edge drops under c.mu.
//
// The final driver must preadmit this whole operation before its fence. Calling
// it with finite work is not, by itself, proof that a no-yield final window fits.
// An unexpected refusal after the fence follows the original ambiguity/custody
// path; it may not be retried as an ordinary publication or relabeled success.
func (c *Coordinator) ConsumeNativePublicationResourcesV6(r *NativePublicationV6, result PublishResult, visible *StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || r == nil || visible == nil || w == nil {
		return false, ErrPublisherProtocol
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*r))) {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.coordinator != c || !r.turn || r.fenced || r.complete ||
		c.activeAttempt.id != r.attempt.id || c.activeAttempt.candidate != r.candidate ||
		len(c.pending) != 1 || c.pending[0].candidate != r.candidate ||
		result.Outcome != PublishSucceeded || result.Err != nil ||
		(result.DurableCommitSeq != 0 && result.DurableCommitSeq != r.candidate.frontier.commitSeq) {
		return false, ErrPublisherProtocol
	}
	txn := r.candidate.DurableRoot()
	if txn == nil {
		return false, ErrDurableRootOwnership
	}
	return txn.consumePrimarySharedResourcesV6(visible, c.resourceActivePins, w)
}
