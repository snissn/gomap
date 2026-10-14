package rootpublication

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// OwnedPublicationReceiptV5 retains the same irreversible member through its
// finite Finish phases. The durable report and Finish completion are separate
// facts. No public waiter receives the callback or this ownership capability.
type OwnedPublicationReceiptV5 struct {
	mu                sync.Mutex
	coordinator       *Coordinator
	legacyCandidate   *PreparedRootCandidate
	transaction       *DurableRootTransaction
	pinCounts         map[ResourceKind]uint64
	durable, finished bool
	finishing         bool                   // guarded by coordinator.mu
	terminal          *TerminalOwnedFinishV5 // guarded by receipt.mu
}

// EnqueueOwnedBuiltAndPublishV5 consumes the genuine final builder lease and
// runs stable storage plus the common durable report before returning Accepted.
// It admits the coordinator work before activation; publish must independently
// admit its complete physical operation set before any irreversible mutation.
// A failed physical publication is terminal for this owned call. It is never
// rescheduled on an ordinary nil-work waiter or inherited by a legal writer.
//
// Frozen shared custody removes per-token transfer work from this handoff. This
// metadata reservation is not a whole-publication fit claim; physical operand
// validation, storage and callback retirement remain separately charged.
func (c *Coordinator) EnqueueOwnedBuiltAndPublishV5(candidate *PreparedRootCandidate, token *BuilderToken, w *iterator.OrdinalScanWork, publish func(*PreparedRootCandidate, *iterator.OrdinalScanWork) PublishResult) (*OwnedPublicationReceiptV5, bool, error) {
	if c == nil || candidate == nil || token == nil || token.handle == nil || w == nil || publish == nil {
		return nil, false, ErrDurableRootOwnership
	}
	transaction := candidate.DurableRoot()
	if transaction == nil || transaction.owned == nil {
		return nil, false, ErrDurableRootOwnership
	}
	set := candidate.resourceSet()
	// A shared frozen kind-root owns every token through immutable chunk custody.
	// Its owner transfer is one real wrapper edge; no physical token is individually
	// transferred or revalidated here. Native preparation must pay constructor
	// validation and retain exact handles before producing this immutable candidate.
	if !candidate.durableValidated || set == nil || set.physicalOnly || set.kindViews == nil {
		return nil, false, ErrResourceOwnership
	}
	set.mu.Lock()
	kinds := len(set.kindViews)
	count := stableResourceKindViewCount(set.kindViews)
	set.mu.Unlock()
	if count > 16 || kinds > 16 {
		return nil, false, ErrResourceOwnership
	}
	// Coupled final-builder/admission control, frozen set ownership, transaction
	// ownership, per-kind aggregate pin edge, and common report/pending removal.
	// Actual storage/DB callbacks and every physical token operation are separate.
	records := uint64(5 + kinds)
	byteCount := 4*uint64(unsafe.Sizeof(Coordinator{})) + 4*uint64(unsafe.Sizeof(PreparedRootCandidate{})) + 4*uint64(unsafe.Sizeof(DurableRootTransaction{})) + 2*uint64(unsafe.Sizeof(builderHandle{})) + 2*uint64(unsafe.Sizeof(builderLease{})) + 4*uint64(unsafe.Sizeof(StableResourceSet{})) + 4*uint64(unsafe.Sizeof(OwnedPublicationReceiptV5{})) + uint64(kinds)*4*uint64(unsafe.Sizeof(stableResourceKindView{}))
	if !w.Reserve(records, byteCount) {
		return nil, false, nil
	}
	handle := token.handle
	handle.mu.Lock()
	handleLocked := true
	defer func() {
		if handleLocked {
			handle.mu.Unlock()
		}
	}()
	if handle.released || handle.lease == nil {
		return nil, false, ErrBuilderLease
	}
	lease := handle.lease
	lease.mu.Lock()
	leaseLocked := true
	defer func() {
		if leaseLocked {
			lease.mu.Unlock()
		}
	}()
	if lease.released || lease.coordinator != c || lease.refs != 1 {
		return nil, false, ErrBuilderLease
	}
	c.mu.Lock()
	if c.activeBuilders != 1 || c.publishing || len(c.pending) != 0 || len(c.ownedFinish) != 0 || c.ownedPublishing != nil {
		c.mu.Unlock()
		return nil, false, ErrBuilderLease
	}
	pinCounts := make(map[ResourceKind]uint64, kinds)
	set.mu.Lock()
	for kind, view := range set.kindViews {
		pinCounts[kind] = uint64(view.count)
	}
	set.mu.Unlock()
	_, e := c.enqueueOwnedFrozenLocked(candidate, transaction, set, pinCounts, w)
	if e != nil {
		c.mu.Unlock()
		return nil, false, e
	}
	var receipt *OwnedPublicationReceiptV5
	if transaction.constructionV6 != nil {
		// The receipt is a view of the SAME construction owner. Candidate and
		// physical resources are read through that owner's immutable descriptor.
		receipt = &transaction.constructionV6.receipt
		receipt.coordinator, receipt.transaction, receipt.pinCounts = c, transaction, pinCounts
	} else {
		receipt = &OwnedPublicationReceiptV5{coordinator: c, legacyCandidate: candidate, transaction: transaction, pinCounts: pinCounts}
	}
	c.ownedPublishing = receipt
	c.clearPublishRequestLocked()
	c.publishing = true
	c.publishCalls++
	c.nextAttemptID++
	attempt := PublishAttempt{id: c.nextAttemptID, candidate: candidate, groupSize: 1, started: c.clock.Now()}
	c.activeAttempt = attempt
	handle.released = true
	handle.lease = nil
	lease.refs = 0
	lease.released = true
	c.activeBuilders--
	c.notifyLocked()
	c.mu.Unlock()
	lease.mu.Unlock()
	leaseLocked = false
	handle.mu.Unlock()
	handleLocked = false
	result := publish(candidate, w)
	// Failed owned storage is recovery-only. Retryable scheduler work would give
	// ordinary WaitThrough/Drain authority to finish this native member.
	if result.Outcome != PublishSucceeded || result.Err != nil {
		if result.Err == nil {
			result.Err = ErrRecoveryRequired
		}
		result.Outcome = PublishAmbiguous
	}
	c.mu.Lock()
	c.ownedReportWork = w
	c.reportingAttempt = true
	c.finishPublishLocked(candidate, 1, result, c.clock.Now().Sub(attempt.started))
	c.ownedReportWork = nil
	c.reportingAttempt = false
	if c.poison == nil && c.durable.commitSeq == candidate.frontier.commitSeq && transaction.Owner() == ResourceOwnerFinish {
		receipt.durable = true
		c.ownedFinish = append(c.ownedFinish, receipt)
	} else if c.poison == nil {
		c.poison = errors.Join(ErrRecoveryRequired, ErrPublisherProtocol)
		c.failWaitersLocked(c.poison)
	}
	c.ownedPublishing = nil
	c.activeAttempt = PublishAttempt{}
	c.notifyLocked()
	e = c.poison
	c.mu.Unlock()
	c.signal()
	return receipt, true, e
}

// enqueueOwnedFrozenLocked reuses genuine immutable candidate validation. Its
// fresh checks cover current admission/lineage and exact live ownership. It
// does not rescan unchanged physical identity declarations or transfer tokens
// already held by a shared immutable resource root. Publication still freshly
// validates mutable DB-current/winner/closure authority in its typed callback.
func (c *Coordinator) enqueueOwnedFrozenLocked(candidate *PreparedRootCandidate, transaction *DurableRootTransaction, set *StableResourceSet, pinCounts map[ResourceKind]uint64, w *iterator.OrdinalScanWork) (enqueueDecision, error) {
	if e := c.terminalErrorLocked(); e != nil {
		return enqueueDecision{}, e
	}
	if candidate.frontier.commitSeq <= c.visible.commitSeq || (c.visible.commitSeq != 0 && !candidate.frontier.Dominates(c.visible)) {
		return enqueueDecision{}, ErrInvalidCandidate
	}
	if e := c.preflightDurableRootLocked(candidate); e != nil {
		return enqueueDecision{}, e
	}
	if set.Owner() != ResourceOwnerCandidate || transaction.Owner() != ResourceOwnerCandidate {
		return enqueueDecision{}, ErrResourceOwnership
	}
	if e := set.transfer(ResourceOwnerCandidate, ResourceOwnerCoordinator); e != nil {
		return enqueueDecision{}, e
	}
	if e := transaction.transfer(ResourceOwnerCandidate, ResourceOwnerCoordinator); e != nil {
		_ = set.transfer(ResourceOwnerCoordinator, ResourceOwnerCandidate)
		return enqueueDecision{}, e
	}
	if e := transaction.activateFromCoordinatorWithWork(w); e != nil {
		_ = transaction.transfer(ResourceOwnerCoordinator, ResourceOwnerCandidate)
		_ = set.transfer(ResourceOwnerCoordinator, ResourceOwnerCandidate)
		return enqueueDecision{}, e
	}
	return c.appendEnqueuedCandidateWithPinsLocked(candidate, pinCounts), nil
}

// FinishOwnedPublicationV5 is the only callback route for retained Finish debt.
// Legal writers may append after the durable report. They cannot drain, restart
// or append this member's Finish cursor. Its callback must retire real custody
// with the same work object, including dependency/owner release and bank queue.
func (c *Coordinator) FinishOwnedPublicationV5(receipt *OwnedPublicationReceiptV5, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || receipt == nil || receipt.coordinator != c || w == nil {
		return false, ErrDurableRootOwnership
	}
	receipt.mu.Lock()
	defer receipt.mu.Unlock()
	if receipt.terminal != nil {
		return false, ErrDurableRootOwnership
	}
	ready, _, e := c.finishOwnedReceiptV5(receipt, w)
	return ready, e
}

// finishOwnedReceiptV5 requires receipt.mu. The same cursor and callback serve
// the original owner and terminal owner; there is no mirrored lifecycle.
func (c *Coordinator) finishOwnedReceiptV5(receipt *OwnedPublicationReceiptV5, w *iterator.OrdinalScanWork) (bool, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if receipt.finished {
		return true, false, nil
	}
	if !receipt.durable || len(c.ownedFinish) != 1 || c.ownedFinish[0] != receipt {
		return false, false, ErrDurableRootOwnership
	}
	count := len(receipt.pinCounts)
	if !w.Reserve(uint64(2+count), 4*uint64(unsafe.Sizeof(OwnedPublicationReceiptV5{}))+2*uint64(unsafe.Sizeof(Coordinator{}))+uint64(count)*2*uint64(unsafe.Sizeof(stableResourceEntry{}))) {
		return false, false, nil
	}
	// Validate captured diagnostic ownership before the irreversible callback.
	for kind, count := range receipt.pinCounts {
		if c.resourceActivePins[kind] < count {
			return false, false, ErrResourceOwnership
		}
	}
	receipt.finishing = true
	c.mu.Unlock()
	ready, progress, e := c.finishOwnedTransactionWithScopeV6(receipt, w)
	c.mu.Lock()
	receipt.finishing = false
	if e != nil || !ready {
		return ready, progress, e
	}
	for kind, count := range receipt.pinCounts {
		if c.resourceActivePins[kind] == count {
			delete(c.resourceActivePins, kind)
		} else {
			c.resourceActivePins[kind] -= count
		}
	}
	receipt.finished = true
	c.ownedFinish[0] = nil
	c.ownedFinish = c.ownedFinish[:0]
	c.reachabilityEpoch++
	c.notifyLocked()
	return true, true, nil
}

func (receipt *OwnedPublicationReceiptV5) Durable() bool {
	if receipt == nil {
		return false
	}
	receipt.mu.Lock()
	defer receipt.mu.Unlock()
	return receipt.durable
}
func (c *Coordinator) HasOwnedFinishV5() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ownedFinish) != 0 || c.ownedPublishing != nil
}

// TerminalOwnedFinishV5 is an exclusive shutdown capability for an exact
// registered receipt. Its progress remains in that receipt's transaction and
// typed callback cursor. Stop never runs this work through a nil-work waiter.
type TerminalOwnedFinishV5 struct{ receipt *OwnedPublicationReceiptV5 }

func (c *Coordinator) TakeTerminalOwnedFinishV5(w *iterator.OrdinalScanWork) (*TerminalOwnedFinishV5, bool, error) {
	if c == nil || w == nil {
		return nil, false, ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Coordinator{}))) {
		return nil, false, nil
	}
	c.mu.Lock()
	if !c.stopped && c.poison == nil {
		c.mu.Unlock()
		return nil, false, ErrDurableRootOwnership
	}
	if len(c.ownedFinish) == 0 {
		c.mu.Unlock()
		return nil, true, nil
	}
	if len(c.ownedFinish) != 1 {
		c.mu.Unlock()
		return nil, false, ErrDurableRootOwnership
	}
	receipt := c.ownedFinish[0]
	c.mu.Unlock()
	// Never hold coordinator.mu while joining an active callback. That callback
	// may re-enter coordinator/transaction ownership before relinquishing receipt.mu.
	receipt.mu.Lock()
	defer receipt.mu.Unlock()
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(OwnedPublicationReceiptV5{}))+uint64(unsafe.Sizeof(TerminalOwnedFinishV5{}))) {
		return nil, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if receipt.finished {
		return nil, true, nil
	}
	if (!c.stopped && c.poison == nil) || len(c.ownedFinish) != 1 || c.ownedFinish[0] != receipt || !receipt.durable {
		return nil, false, ErrDurableRootOwnership
	}
	if receipt.terminal == nil {
		receipt.terminal = &TerminalOwnedFinishV5{receipt: receipt}
	}
	return receipt.terminal, true, nil
}

func (owner *TerminalOwnedFinishV5) Step(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if owner == nil || owner.receipt == nil || w == nil {
		return false, false, ErrDurableRootOwnership
	}
	receipt := owner.receipt
	receipt.mu.Lock()
	defer receipt.mu.Unlock()
	if receipt.terminal != owner {
		return false, false, ErrDurableRootOwnership
	}
	return receipt.coordinator.finishOwnedReceiptV5(receipt, w)
}

// OwnedFinishCallbackScopeV6 marks only the active callback's caller on its
// actual publisher. The receipt remains the sole resource and cursor owner.
// Enter reserves both finite marker operations; Leave must always run.
type OwnedFinishCallbackScopeV6 interface {
	EnterOwnedFinishCallbackV6(*iterator.OrdinalScanWork) (bool, error)
	LeaveOwnedFinishCallbackV6()
}

func (c *Coordinator) finishOwnedTransactionWithScopeV6(receipt *OwnedPublicationReceiptV5, w *iterator.OrdinalScanWork) (bool, bool, error) {
	if scope, ok := c.publisher.(OwnedFinishCallbackScopeV6); ok {
		entered, e := scope.EnterOwnedFinishCallbackV6(w)
		if e != nil || !entered {
			return false, false, e
		}
		defer scope.LeaveOwnedFinishCallbackV6()
	}
	return receipt.transaction.finishOwned(receipt.resourceSetV6(), w)
}

// Ordinary constructed transactions use the SAME callback admission policy and
// transaction Finish. This is synchronous ordinary work, not bounded native ACK.
func (c *Coordinator) finishOrdinaryPrimaryWithScopeV6(txn *DurableRootTransaction) error {
	w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	if scope, ok := c.publisher.(OwnedFinishCallbackScopeV6); ok {
		entered, err := scope.EnterOwnedFinishCallbackV6(w)
		if !entered || err != nil {
			return errors.Join(err, ErrDurableRootOwnership)
		}
		defer scope.LeaveOwnedFinishCallbackV6()
	}
	return txn.finishOrdinaryPrimaryV6(w)
}

// Descriptor views contain no second resource lifecycle. Legacy generic members
// retain their old candidate identity; selected constructors own it in-place.
func (r *OwnedPublicationReceiptV5) candidateV6() *PreparedRootCandidate {
	if r == nil {
		return nil
	}
	if r.transaction != nil && r.transaction.constructionV6 != nil {
		return r.transaction.PublicationCandidateV6()
	}
	return r.legacyCandidate
}
func (r *OwnedPublicationReceiptV5) resourceSetV6() *StableResourceSet {
	return r.candidateV6().resourceSet()
}
