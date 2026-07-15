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

type allocatorCOWStateV1 struct {
	generation *FreelistGenerationV1
	txn        *FreelistTxn
	ledger     *ReservationLedger
	prepared   *PreparedCOWCandidateV1
	ready      *sync.Cond
}

// PreparedCOWCandidateV1 is the immutable allocator handoff consumed by the
// durable-root publisher. Auxiliary page IDs are reserved as ordinary data
// pages in the same generation so manifest and root-record pages cannot race
// allocator metadata or later index allocation.
type PreparedCOWCandidateV1 struct {
	candidate   *FreelistCandidateV1
	candidateID CandidateIDV1
	auxiliary   []uint64
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

func (a *Allocator) waitCOWReadyLocked() {
	for a.cow != nil && a.cow.prepared != nil {
		a.cow.ready.Wait()
	}
}

func (a *Allocator) allocCOWLocked(hint uint64) (uint64, error) {
	a.waitCOWReadyLocked()
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

func (a *Allocator) retireCOWLocked(ids []uint64, lastReachableCommitSeq uint64) error {
	a.waitCOWReadyLocked()
	if a.cow == nil || a.cow.txn == nil || lastReachableCommitSeq == 0 {
		return ErrGenerationFormat
	}
	for _, id := range ids {
		if id < 2 {
			return errCannotFreePageZero
		}
		a.cow.txn.Retire(id, lastReachableCommitSeq)
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
	if auxiliaryPageCount < 0 || sink == nil {
		return nil, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.txn == nil {
		return nil, ErrGenerationFormat
	}
	if a.cow.prepared != nil {
		generation := a.cow.prepared.candidate.Generation()
		if generation == nil || generation.GenerationID() != generationID || generation.CommitSeq() != commitSeq ||
			a.cow.prepared.candidateID != candidateID || len(a.cow.prepared.auxiliary) != auxiliaryPageCount {
			return nil, ErrCOWCandidatePrepared
		}
		return a.cow.prepared, nil
	}
	// Encode every page that the caller's sealed reuse capability permits as
	// free in this exact durable generation. A prepared candidate is immutable;
	// retry returns it above instead of applying a fresher capability after the
	// caller releases its reader-admission gate.
	a.cow.txn.PruneWithCapability(capability)
	auxiliary, err := a.cow.txn.allocateAppendedRange(auxiliaryPageCount)
	if err != nil {
		return nil, err
	}
	for _, id := range auxiliary {
		if id >= a.pager.PageCount() {
			if err := a.pager.Truncate(id + 1); err != nil {
				return nil, err
			}
		}
	}
	candidate, err := a.cow.txn.MaterializeCandidate(generationID, commitSeq, candidateID, sink)
	if err != nil {
		return nil, err
	}
	prepared := &PreparedCOWCandidateV1{candidate: candidate, candidateID: candidateID, auxiliary: auxiliary}
	a.cow.prepared = prepared
	return prepared, nil
}

func (a *Allocator) PublishCOWCandidateV1(prepared *PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	if prepared == nil || prepared.candidate == nil || prepared.candidate.Generation() == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.prepared != prepared {
		return ErrCandidateConsumed
	}
	if err := a.cow.ledger.MarkVisible(prepared.candidateID); err != nil {
		return err
	}
	if err := a.cow.ledger.Publish(prepared.candidateID); err != nil {
		return err
	}
	next, err := BeginCandidateV1(prepared.candidate.Generation(), prepared.candidate.GenerationRef(), a.cow.ledger)
	if err != nil {
		return err
	}
	next.PruneWithCapability(nextCapability)
	a.cow.generation = prepared.candidate.Generation()
	a.cow.txn = next
	a.cow.prepared = nil
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
