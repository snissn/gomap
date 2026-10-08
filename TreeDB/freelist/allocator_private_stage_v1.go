package freelist

import (
	"errors"
	"fmt"
	"unsafe"
)

// preparePrivateCOWStageLockedV1 is the ordinary engine's explicit-source stage.
// It owns no public prepared slot, performs no visibility transition and never
// reenters public Prepare. The caller owns rollbackTxn and receives two private
// owning results on success. Every supplied request is a synchronous borrow.
func (a *Allocator) preparePrivateCOWStageLockedV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, rollbackTxn *FreelistTxn, generationID, commitSeq uint64, candidateID CandidateIDV1, capability ReuseCapability, retirements []COWRetirementV1, auxiliaryPageCount int, sink AppendPageSink, owned bool) (*FreelistTxn, *PreparedCOWCandidateV1, error) {
	if err := reserveAllocationScratchV1(request, scratch, rollbackTxn.buildCreator, 1, uint64(unsafe.Sizeof(privateCOWStageControlV1{})), true); err != nil {
		return nil, nil, err
	}
	control := &privateCOWStageControlV1{rollbackStats: a.stats, beforeCopyWork: rollbackTxn.stats.FreelistStateCopyWorkV1}
	staged, err := rollbackTxn.cloneForPrivateAllocatorPrepare(request)
	if err != nil {
		*control = privateCOWStageControlV1{}
		return nil, nil, err
	}
	control.staged = staged
	defer a.finishPrivateCOWStageControlV1(control)
	for _, retirement := range retirements {
		if len(retirement.PageIDs) == 0 {
			continue
		}
		if err := retirePrivateCOWTxnV1(request, scratch, staged, retirement.PageIDs, retirement.LastReachableCommitSeq); err != nil {
			return a.rollbackPrivateCOWStageLockedV1(request, staged, candidateID, control.rollbackStats, err)
		}
		a.stats.FreePages += uint64(len(retirement.PageIDs))
		if TestHookRetireCOWBeforeUnlock != nil {
			TestHookRetireCOWBeforeUnlock()
		}
	}
	// Encode every page that the caller's sealed reuse capability permits as
	// free in this exact durable generation. A prepared candidate is immutable;
	// retry returns it above instead of applying a fresher capability after the
	// caller releases its reader-admission gate.
	a.pruneCOWLockedV1(request, scratch, staged, capability)
	preparedCreator := staged.buildCreator
	if err := preparedCreator.reserve(request, allocationClassV1(uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})), true), 1); err != nil {
		return a.rollbackPrivateCOWStageLockedV1(request, staged, candidateID, control.rollbackStats, err)
	}
	prepared := &PreparedCOWCandidateV1{creator: preparedCreator, allocator: a, candidateID: candidateID, ownedBacking: owned}
	control.prepared = prepared
	auxiliary, err := staged.allocateContiguousRange(request, auxiliaryPageCount)
	if err != nil {
		return a.rollbackPrivateCOWStageLockedV1(request, staged, candidateID, control.rollbackStats, err)
	}
	prepared.auxiliary = auxiliary
	candidate, err := staged.materializeCandidateOwnedV1(request, scratch, generationID, commitSeq, candidateID, sink)
	if err != nil {
		return a.rollbackPrivateCOWStageLockedV1(request, staged, candidateID, control.rollbackStats, err)
	}
	var activationTxn *FreelistTxn
	if preparedCreator != nil {
		if err := a.growActivatedBackingLockedV1(request, preparedCreator); err != nil {
			releaseCandidateOwnedBackingV1(candidate)
			return a.rollbackPrivateCOWStageLockedV1(request, staged, candidateID, control.rollbackStats, err)
		}
		activationTxn, err = beginCandidateOwnedV1(request, scratch, candidate.generation, candidate.generation.ref, a.cow.ledger, preparedCreator)
		if err != nil {
			releaseCandidateOwnedBackingV1(candidate)
			return a.rollbackPrivateCOWStageLockedV1(request, staged, candidateID, control.rollbackStats, err)
		}
		activationTxn.pruneCursor = staged.pruneCursor
	}
	prepared.activationTxn, prepared.candidate, prepared.auxiliary = activationTxn, candidate, auxiliary
	control.preparedHeaderTransferred = true
	return staged, prepared, nil
}

func retirePrivateCOWTxnV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, txn *FreelistTxn, ids []uint64, horizon uint64) error {
	if txn == nil || horizon == 0 {
		return ErrGenerationFormat
	}
	if err := txn.requireBirthCreditV1(request); err != nil {
		return err
	}
	for _, id := range ids {
		if id < 2 {
			return errCannotFreePageZero
		}
	}
	if len(ids) == 1 {
		txn.RetireWithAllocationRequestV1(request, ids[0], horizon)
	} else {
		if err := reserveAllocationScratchV1(request, scratch, txn.buildCreator, uint64(len(ids)), uint64(unsafe.Sizeof(retiredPage{})), false); err != nil {
			return err
		}
		retired := make([]retiredPage, len(ids))
		defer clear(retired[:cap(retired)])
		for i, id := range ids {
			retired[i] = retiredPage{id: id, lastReachableCommitSeq: horizon}
		}
		txn.retireMany(request, retired)
	}
	return txn.valid()
}

// A concrete short-scope control replaces captured rollback/defer closures.
// Its full class is prepaid before birth and all aliases are scrubbed before
// the caller closes the independent scratch creator. No request is stored.
type privateCOWStageControlV1 struct {
	rollbackStats             Stats
	beforeCopyWork            FreelistStateCopyWorkV1
	staged                    *FreelistTxn
	prepared                  *PreparedCOWCandidateV1
	preparedHeaderTransferred bool
}

func (a *Allocator) finishPrivateCOWStageControlV1(control *privateCOWStageControlV1) {
	work := control.staged.stats.FreelistStateCopyWorkV1
	a.cow.preparationCopyWork.StateNodeCopies += work.StateNodeCopies - control.beforeCopyWork.StateNodeCopies
	a.cow.preparationCopyWork.StateChunkCopies += work.StateChunkCopies - control.beforeCopyWork.StateChunkCopies
	a.cow.preparationCopyWork.StateCopyBytes += work.StateCopyBytes - control.beforeCopyWork.StateCopyBytes
	a.cow.preparationCopyWork.StateIsolationVisits += work.StateIsolationVisits - control.beforeCopyWork.StateIsolationVisits
	if control.prepared != nil && !control.preparedHeaderTransferred {
		clear(control.prepared.auxiliary[:cap(control.prepared.auxiliary)])
		control.prepared.auxiliary = nil
		creator := control.prepared.creator
		control.prepared.creator, control.prepared.allocator = nil, nil
		creator.release()
	}
	*control = privateCOWStageControlV1{}
}
func (a *Allocator) rollbackPrivateCOWStageLockedV1(request AllocationRequestCreditV1, staged *FreelistTxn, candidateID CandidateIDV1, rollbackStats Stats, cause error) (*FreelistTxn, *PreparedCOWCandidateV1, error) {
	a.stats = rollbackStats
	releaseTxnV1(staged)
	if rollbackErr := a.cow.ledger.RollbackPreVisibleWithAllocationRequestV1(request, candidateID); rollbackErr != nil {
		return nil, nil, errors.Join(cause, fmt.Errorf("rollback COW reservation: %w", rollbackErr))
	}
	return nil, nil, cause
}
