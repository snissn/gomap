package freelist

import (
	"errors"
	"fmt"
	"sync"
)

var ErrCOWCandidatePrepared = errors.New("COW freelist candidate publication pending")

// TestHookRetireCOWBeforeUnlock is a test-only hook that fires after a COW
// retirement has updated the candidate transaction and counters but before the
// allocator lock is released. It should remain nil in production.
var TestHookRetireCOWBeforeUnlock func()

// TestHookCOWWaitBeforeSleep is a test-only hook that fires after an allocator
// observes a prepared COW candidate and immediately before it waits for that
// candidate to publish or fail. It should remain nil in production.
var TestHookCOWWaitBeforeSleep func()

// TestHookAbortCOWCandidateFailure injects a failure before an owned prepared
// candidate is rolled back. It is test-only and must remain nil in production.
var TestHookAbortCOWCandidateFailure func() error

type allocatorCOWStateV1 struct {
	generation *FreelistGenerationV1
	txn        *FreelistTxn
	ledger     *ReservationLedger
	prepared   *PreparedCOWCandidateV1
	activated  []*PreparedCOWCandidateV1
	waitErr    error
	ready      *sync.Cond
}

// COWRetirementV1 is one retirement set staged atomically with a COW
// candidate. The live allocator transaction is unchanged if preparation fails
// or the caller aborts before publication.
type COWRetirementV1 struct {
	PageIDs                []uint64
	LastReachableCommitSeq uint64
}

// PreparedCOWCandidateV1 is the immutable allocator handoff consumed by the
// durable-root publisher. Auxiliary page IDs are reserved as ordinary data
// pages in the same generation so manifest and root-record pages cannot race
// allocator metadata or later index allocation.
type PreparedCOWCandidateV1 struct {
	candidate     *FreelistCandidateV1
	candidateID   CandidateIDV1
	auxiliary     []uint64
	rollbackTxn   *FreelistTxn
	rollbackStats Stats
	activated     bool
	published     bool
}

func (prepared *PreparedCOWCandidateV1) Candidate() *FreelistCandidateV1 {
	if prepared == nil {
		return nil
	}
	return prepared.candidate
}

func (prepared *PreparedCOWCandidateV1) CandidateID() CandidateIDV1 {
	if prepared == nil {
		return CandidateIDV1{}
	}
	return prepared.candidateID
}

func (prepared *PreparedCOWCandidateV1) AuxiliaryPageIDs() []uint64 {
	if prepared == nil {
		return nil
	}
	return append([]uint64(nil), prepared.auxiliary...)
}

func (a *Allocator) EnableCOWV1(generation *FreelistGenerationV1, ledger *ReservationLedger) error {
	if a == nil || a.pager == nil || generation == nil {
		return ErrGenerationFormat
	}
	if err := generation.Validate(); err != nil {
		return err
	}
	if a.pager.PageCount() != generation.HighWater() {
		return fmt.Errorf("%w: pager pages %d do not match generation high-water %d", ErrGenerationFormat, a.pager.PageCount(), generation.HighWater())
	}
	if ledger == nil {
		ledger = NewReservationLedger()
	}
	txn, err := BeginCandidateV1(generation, generation.GenerationRef(), ledger)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow != nil {
		return fmt.Errorf("%w: allocator already uses COW freelist", ErrGenerationFormat)
	}
	state := &allocatorCOWStateV1{generation: generation, txn: txn, ledger: ledger}
	state.ready = sync.NewCond(&a.mu)
	a.cow = state
	a.head = 0
	a.stats.Pages = 0
	a.stats.FreeIDs = generation.FreeCount()
	return nil
}

func (a *Allocator) waitCOWReadyLocked() error {
	for a.cow != nil && a.cow.prepared != nil && a.cow.waitErr == nil {
		if TestHookCOWWaitBeforeSleep != nil {
			TestHookCOWWaitBeforeSleep()
		}
		a.cow.ready.Wait()
	}
	if a.cow != nil && a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	return nil
}

func (a *Allocator) allocCOWLocked(hint uint64) (uint64, error) {
	if err := a.waitCOWReadyLocked(); err != nil {
		return 0, err
	}
	if a.cow == nil || a.cow.txn == nil {
		return 0, ErrGenerationFormat
	}
	var (
		id  uint64
		err error
	)
	if a.preferAppend {
		id, err = a.cow.txn.AllocateAppend()
	} else {
		id, err = a.cow.txn.Allocate(hint)
	}
	if err != nil {
		return 0, err
	}
	if id >= a.pager.PageCount() {
		if err := a.pager.Truncate(id + 1); err != nil {
			return 0, err
		}
		a.stats.AppendAllocPages++
	} else {
		a.stats.ReuseAllocPages++
	}
	a.stats.AllocPages++
	a.lastAlloc = id
	return id, nil
}

// AllocAppend allocates one page above the current logical high-water without
// consulting reusable pages. Unlike toggling SetPreferAppend around Alloc, the
// choice is scoped to this allocation while the allocator lock is held.
func (a *Allocator) AllocAppend() (uint64, error) {
	if a == nil || a.pager == nil {
		return 0, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil {
		id, err := a.pager.Alloc(1)
		if err == nil {
			a.stats.AllocPages++
			a.stats.AppendAllocPages++
			a.lastAlloc = id
		}
		return id, err
	}
	if err := a.waitCOWReadyLocked(); err != nil {
		return 0, err
	}
	if a.cow == nil || a.cow.txn == nil {
		return 0, ErrGenerationFormat
	}
	// Activated build-ahead generations may reserve a virtual tail in an
	// immutable memory-backed candidate before the publisher materializes that
	// tail in the pager. Append after the transaction's logical high-water so it
	// cannot overlap those reservations. A physical tail beyond the transaction
	// remains invalid because the allocator has no authority for those pages.
	if a.cow.txn.highWater < a.pager.PageCount() {
		return 0, fmt.Errorf("%w: append high-water %d is behind pager pages %d", ErrGenerationFormat, a.cow.txn.highWater, a.pager.PageCount())
	}
	id, err := a.cow.txn.AllocateAppend()
	if err != nil {
		return 0, err
	}
	if id >= a.pager.PageCount() {
		if err := a.pager.Truncate(id + 1); err != nil {
			return 0, err
		}
	}
	a.stats.AllocPages++
	a.stats.AppendAllocPages++
	a.lastAlloc = id
	return id, nil
}

func (a *Allocator) retireCOWLocked(ids []uint64, lastReachableCommitSeq uint64) error {
	if err := a.waitCOWReadyLocked(); err != nil {
		return err
	}
	if a.cow == nil || a.cow.txn == nil || lastReachableCommitSeq == 0 {
		return ErrGenerationFormat
	}
	for _, id := range ids {
		if id < 2 {
			return errCannotFreePageZero
		}
	}
	if len(ids) == 1 {
		a.cow.txn.Retire(ids[0], lastReachableCommitSeq)
	} else {
		retired := make([]retiredPage, len(ids))
		for i, id := range ids {
			retired[i] = retiredPage{id: id, lastReachableCommitSeq: lastReachableCommitSeq}
		}
		a.cow.txn.retireMany(retired)
	}
	a.stats.FreePages += uint64(len(ids))
	if TestHookRetireCOWBeforeUnlock != nil {
		TestHookRetireCOWBeforeUnlock()
	}
	return nil
}

func (a *Allocator) RetireCOWV1(ids []uint64, lastReachableCommitSeq uint64) error {
	if len(ids) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil {
		return ErrGenerationFormat
	}
	return a.retireCOWLocked(ids, lastReachableCommitSeq)
}

func (a *Allocator) PrepareCOWCandidateV1(generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, auxiliaryPageCount int, sink AppendPageSink) (*PreparedCOWCandidateV1, error) {
	return a.PrepareCOWCandidateRetiringV1(generationID, commitSeq, candidateID, capability, nil, auxiliaryPageCount, sink)
}

// PrepareCOWCandidateRetiringV1 stages retirements and candidate
// materialization as one allocator transaction. The pre-prepare transaction
// remains available for rollback until the candidate publishes.
func (a *Allocator) PrepareCOWCandidateRetiringV1(generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, retirements []COWRetirementV1, auxiliaryPageCount int, sink AppendPageSink) (*PreparedCOWCandidateV1, error) {
	if auxiliaryPageCount < 0 || sink == nil {
		return nil, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.txn == nil {
		return nil, ErrGenerationFormat
	}
	if a.cow.waitErr != nil {
		return nil, a.cow.waitErr
	}
	if a.cow.prepared != nil {
		generation := a.cow.prepared.candidate.Generation()
		if generation == nil || generation.GenerationID() != generationID || generation.CommitSeq() != commitSeq ||
			a.cow.prepared.candidateID != candidateID || len(a.cow.prepared.auxiliary) != auxiliaryPageCount {
			return nil, ErrCOWCandidatePrepared
		}
		return a.cow.prepared, nil
	}
	rollbackTxn := a.cow.txn
	rollbackStats := a.stats
	staged, err := rollbackTxn.cloneForAllocatorPrepare()
	if err != nil {
		return nil, err
	}
	a.cow.txn = staged
	rollback := func(cause error) (*PreparedCOWCandidateV1, error) {
		a.cow.txn = rollbackTxn
		a.stats = rollbackStats
		if rollbackErr := a.cow.ledger.RollbackPreVisible(candidateID); rollbackErr != nil {
			return nil, errors.Join(cause, fmt.Errorf("rollback COW reservation: %w", rollbackErr))
		}
		return nil, cause
	}
	for _, retirement := range retirements {
		if len(retirement.PageIDs) == 0 {
			continue
		}
		if err := a.retireCOWLocked(retirement.PageIDs, retirement.LastReachableCommitSeq); err != nil {
			return rollback(err)
		}
	}
	// Encode every page that the caller's sealed reuse capability permits as
	// free in this exact durable generation. A prepared candidate is immutable;
	// retry returns it above instead of applying a fresher capability after the
	// caller releases its reader-admission gate.
	a.cow.txn.PruneWithCapability(capability)
	auxiliary, err := a.cow.txn.allocateAppendedRange(auxiliaryPageCount)
	if err != nil {
		return rollback(err)
	}
	candidate, err := a.cow.txn.MaterializeCandidate(generationID, commitSeq, candidateID, sink)
	if err != nil {
		return rollback(err)
	}
	prepared := &PreparedCOWCandidateV1{
		candidate: candidate, candidateID: candidateID, auxiliary: auxiliary,
		rollbackTxn: rollbackTxn, rollbackStats: rollbackStats,
	}
	a.cow.prepared = prepared
	return prepared, nil
}

// AbortCOWCandidateV1 rolls back a candidate that has not crossed visibility.
// Reservation tails accepted by an arbitrary sink remain conservatively
// burned, while the live allocation and retirement transaction is restored.
func (a *Allocator) AbortCOWCandidateV1(prepared *PreparedCOWCandidateV1) error {
	if a == nil || prepared == nil || prepared.rollbackTxn == nil {
		return ErrGenerationFormat
	}
	if TestHookAbortCOWCandidateFailure != nil {
		if err := TestHookAbortCOWCandidateFailure(); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.prepared != prepared {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	if err := a.cow.ledger.RollbackPreVisible(prepared.candidateID); err != nil {
		return err
	}
	a.cow.txn = prepared.rollbackTxn
	a.stats = prepared.rollbackStats
	a.cow.prepared = nil
	prepared.rollbackTxn = nil
	a.cow.ready.Broadcast()
	return nil
}

// ActivateCOWCandidateV1 makes a prepared allocator generation visible to
// subsequent builders without claiming that its durable root has published.
// The exact candidate remains ledger-owned until PublishActivatedCOWThroughV1
// consumes it. This split is what permits several visible COW generations to
// accumulate behind one dependency-closed durable-root publication.
func (a *Allocator) ActivateCOWCandidateV1(prepared *PreparedCOWCandidateV1) error {
	if a == nil || prepared == nil || prepared.candidate == nil || prepared.candidate.Generation() == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.prepared != prepared || prepared.activated || prepared.published {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	generation := prepared.candidate.Generation()
	next, err := BeginCandidateV1(generation, generation.GenerationRef(), a.cow.ledger)
	if err != nil {
		return err
	}
	if err := a.cow.ledger.MarkVisible(prepared.candidateID); err != nil {
		return err
	}
	a.cow.generation = generation
	a.cow.txn = next
	a.cow.prepared = nil
	a.cow.activated = append(a.cow.activated, prepared)
	prepared.rollbackTxn = nil
	prepared.activated = true
	a.stats.FreeIDs = generation.FreeCount()
	a.cow.ready.Broadcast()
	return nil
}

// PublishActivatedCOWPrefixV1 consumes exactly the caller-proven ordered
// visible prefix after the matching durable meta is stable. Missing,
// reordered, or extra members are rejected before the ledger or allocator is
// mutated. Newer activated generations remain reserved.
func (a *Allocator) PublishActivatedCOWPrefixV1(prefix []*PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if a == nil || len(prefix) == 0 {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.publishActivatedCOWPrefixLockedV1(prefix, nextCapability)
}

func (a *Allocator) publishActivatedCOWPrefixLockedV1(prefix []*PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if a.cow == nil {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	if len(prefix) > len(a.cow.activated) {
		return ErrCandidateConsumed
	}
	for i, prepared := range prefix {
		if prepared == nil || !prepared.activated || prepared.published {
			return ErrGenerationFormat
		}
		if a.cow.activated[i] != prepared {
			return fmt.Errorf("%w: activated prefix member %d does not match", ErrCandidateConsumed, i)
		}
	}
	prepared := prefix[len(prefix)-1]
	generation := prepared.candidate.Generation()
	if generation == nil {
		return ErrGenerationFormat
	}
	if err := a.validateCOWPhysicalTailLockedV1(generation.HighWater()); err != nil {
		return err
	}
	ids := make([]CandidateIDV1, len(prefix))
	for i, candidate := range prefix {
		ids[i] = candidate.candidateID
	}
	if err := a.cow.ledger.PublishBatch(ids); err != nil {
		return err
	}
	for _, candidate := range prefix {
		candidate.published = true
	}
	copy(a.cow.activated, a.cow.activated[len(prefix):])
	clear(a.cow.activated[len(a.cow.activated)-len(prefix):])
	a.cow.activated = a.cow.activated[:len(a.cow.activated)-len(prefix)]
	// The live transaction may already be based on a newer visible generation.
	// Pruning it with the newly advanced recovery horizon is conservative and
	// does not alter any immutable activated generation.
	a.cow.txn.PruneWithCapability(nextCapability)
	a.cow.ready.Broadcast()
	return nil
}

// ValidateCOWPhysicalTailV1 proves that the physical pager covers required
// publication state without extending beyond the live allocator transaction's
// logical authority. A physical tail above the published prefix is valid when
// it belongs to a newer activated or in-progress generation.
func (a *Allocator) ValidateCOWPhysicalTailV1(requiredHighWater uint64) error {
	if a == nil || a.pager == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.validateCOWPhysicalTailLockedV1(requiredHighWater)
}

func (a *Allocator) validateCOWPhysicalTailLockedV1(requiredHighWater uint64) error {
	if a.cow == nil || a.cow.txn == nil {
		return ErrGenerationFormat
	}
	pageCount := a.pager.PageCount()
	if pageCount < requiredHighWater {
		return fmt.Errorf("%w: pager pages %d do not cover activated generation high-water %d", ErrGenerationFormat, pageCount, requiredHighWater)
	}
	if pageCount > a.cow.txn.highWater {
		return fmt.Errorf("%w: pager pages %d exceed live logical high-water %d", ErrGenerationFormat, pageCount, a.cow.txn.highWater)
	}
	return nil
}

// PublishActivatedCOWThroughV1 is the compatibility form for callers that
// identify an activated prefix by its final member. New durable-root code
// should pass its complete sealed prefix to PublishActivatedCOWPrefixV1.
func (a *Allocator) PublishActivatedCOWThroughV1(prepared *PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if a == nil || prepared == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil {
		return ErrCandidateConsumed
	}
	through := -1
	for i, candidate := range a.cow.activated {
		if candidate == prepared {
			through = i
			break
		}
	}
	if through < 0 {
		return ErrCandidateConsumed
	}
	return a.publishActivatedCOWPrefixLockedV1(a.cow.activated[:through+1], nextCapability)
}

// FailCOWCandidateV1 preserves the exact prepared candidate for close/reopen
// ownership while failing and waking allocator calls that were already
// admitted before durable-root publication became unrecoverable in-process.
// It is idempotent for the same prepared candidate.
func (a *Allocator) FailCOWCandidateV1(prepared *PreparedCOWCandidateV1, cause error) error {
	if a == nil || prepared == nil || cause == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil {
		return ErrCandidateConsumed
	}
	owned := a.cow.prepared == prepared
	if !owned {
		for _, candidate := range a.cow.activated {
			if candidate == prepared {
				owned = true
				break
			}
		}
	}
	if !owned {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr == nil {
		a.cow.waitErr = cause
	}
	a.cow.ready.Broadcast()
	return nil
}

func (a *Allocator) PublishCOWCandidateV1(prepared *PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if prepared == nil || prepared.candidate == nil || prepared.candidate.Generation() == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.prepared != prepared || prepared.activated || prepared.published {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	generation := prepared.candidate.Generation()
	if pageCount := a.pager.PageCount(); pageCount != generation.HighWater() {
		return fmt.Errorf("%w: pager pages %d do not match prepared generation high-water %d", ErrGenerationFormat, pageCount, generation.HighWater())
	}
	if err := a.cow.ledger.MarkVisible(prepared.candidateID); err != nil {
		return err
	}
	if err := a.cow.ledger.Publish(prepared.candidateID); err != nil {
		return err
	}
	next, err := BeginCandidateV1(generation, prepared.candidate.GenerationRef(), a.cow.ledger)
	if err != nil {
		return err
	}
	next.PruneWithCapability(nextCapability)
	a.cow.generation = generation
	a.cow.txn = next
	a.cow.prepared = nil
	prepared.rollbackTxn = nil
	prepared.activated = true
	prepared.published = true
	a.cow.waitErr = nil
	a.stats.FreeIDs = a.cow.generation.FreeCount()
	a.cow.ready.Broadcast()
	return nil
}

func (a *Allocator) COWGenerationV1() *FreelistGenerationV1 {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil {
		return nil
	}
	return a.cow.generation
}
