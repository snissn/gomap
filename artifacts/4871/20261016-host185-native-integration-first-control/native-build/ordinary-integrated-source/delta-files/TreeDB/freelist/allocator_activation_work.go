package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// COWActivationWorkV1 owns only preparation of the original next transaction.
// Prepare it from the private immutable candidate BEFORE installing the single
// prepared allocator slot. It owns neither a second allocator nor a live txn
// rollback edge, and its scratch is not generation-owned encoded-page storage.
type COWActivationWorkV1 struct {
	allocator *Allocator
	candidate *FreelistCandidateV1
	ledger    *ReservationLedger
	begin     candidateBeginWorkV1
	successor *FreelistTxn
	ready     bool
}

func (s *COWActivationWorkV1) prepareLocked(a *Allocator, candidate *FreelistCandidateV1, w *iterator.OrdinalScanWork) (bool, error) {
	if candidate == nil || candidate.Generation() == nil || a.cow == nil {
		return false, ErrGenerationFormat
	}
	if s.allocator != nil && (s.allocator != a || s.candidate != candidate || s.ledger != a.cow.ledger) {
		return false, ErrCandidateConsumed
	}
	if s.ready {
		return true, nil
	}
	s.allocator, s.candidate, s.ledger = a, candidate, a.cow.ledger
	generation := candidate.Generation()
	next, ready, err := s.begin.step(generation, generation.GenerationRef(), s.ledger, w)
	if !ready || err != nil {
		return false, err
	}
	s.successor, s.ready = next, true
	return true, nil
}

// PrepareCOWActivationWithWorkV1 can yield only while no prepared allocator
// slot exists. A caller that has already installed that slot must finish or
// abort in the same public call, not resume this preparation on a later call.
func (a *Allocator) PrepareCOWActivationWithWorkV1(candidate *FreelistCandidateV1, s *COWActivationWorkV1, w *iterator.OrdinalScanWork) (bool, error) {
	if a == nil || s == nil || w == nil {
		return false, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil {
		return false, ErrGenerationFormat
	}
	if a.cow.waitErr != nil {
		return false, a.cow.waitErr
	}
	if a.cow.prepared != nil {
		return false, ErrCOWCandidatePrepared
	}
	return s.prepareLocked(a, candidate, w)
}

// ActivateCOWCandidateWithWorkV1 shares the original activation. The finite
// route accepts only an exact completed successor prepared outside the slot.
// It admits current activated-vector growth, ledger lookup/transition and all
// original field transfers before changing visibility. There is no legal
// post-effect refusal and no physical release callback under these locks.
func (a *Allocator) ActivateCOWCandidateWithWorkV1(prepared *PreparedCOWCandidateV1, s *COWActivationWorkV1, w *iterator.OrdinalScanWork) (bool, error) {
	if a == nil || prepared == nil || prepared.candidate == nil || prepared.candidate.Generation() == nil {
		return false, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.prepared != prepared || prepared.activated || prepared.published {
		return false, ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return false, a.cow.waitErr
	}
	var ordinary COWActivationWorkV1
	if w == nil {
		if s == nil {
			s = &ordinary
		}
		ready, err := s.prepareLocked(a, prepared.candidate, nil)
		if !ready || err != nil {
			return false, err
		}
	} else if s == nil || !s.ready || s.allocator != a || s.candidate != prepared.candidate || s.ledger != a.cow.ledger || s.successor == nil {
		return false, ErrGenerationFormat
	}
	records, bytes := a.activationAdmissionV6()
	if ok, err := appendReserve(w, records, bytes); !ok || err != nil {
		return false, err
	}
	if err := a.cow.ledger.MarkVisible(prepared.candidateID); err != nil {
		return false, err
	}
	a.installActivatedCandidateV6(prepared, s)
	return true, nil
}

// activationAdmissionV6 prices the original append and ownership transfers.
// The caller holds a.mu; the returned bound has no effects.
func (a *Allocator) activationAdmissionV6() (records, bytes uint64) {
	// append may allocate/copy the complete current vector. Price that actual
	// shape, not a scalar tranche that conceals a larger backing allocation.
	records, bytes = uint64(2), uint64(12*8)
	if len(a.cow.activated) == cap(a.cow.activated) {
		count := uint64(len(a.cow.activated))
		records += 1 + count
		bytes += uint64(unsafe.Sizeof((*PreparedCOWCandidateV1)(nil))) * (3*count + 1)
	}
	records++
	bytes += uint64(unsafe.Sizeof(candidateBeginWorkV1{}))
	return records, bytes
}

// installActivatedCandidateV6 is the nonfallible original allocator install.
// The exact ledger member must already have made its original Visible transition.
func (a *Allocator) installActivatedCandidateV6(prepared *PreparedCOWCandidateV1, s *COWActivationWorkV1) {
	generation := prepared.candidate.Generation()
	a.cow.generation = generation
	a.cow.txn = s.successor
	a.cow.prepared = nil
	a.cow.activated = append(a.cow.activated, prepared)
	prepared.activationOwner = a.cow
	prepared.rollbackTxn = nil
	prepared.activated = true
	a.stats.FreeIDs = generation.FreeCount()
	// The live allocator now owns the successor. Drop the preparer's duplicate
	// aliases before its containing owner may retire this scratch.
	s.successor = nil
	s.begin = candidateBeginWorkV1{}
	s.ready = false
	s.allocator, s.candidate, s.ledger = nil, nil, nil
	a.cow.ready.Broadcast()
}
