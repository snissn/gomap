package freelist

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

// COWPublicationStageSpecV1 is borrowed only during preparation. Retirement
// slices, limits and capabilities are supplied by the existing root/pin owner.
// No spec, request, scratch creator or user callback is retained by the packet.
type COWPublicationStageSpecV1 struct {
	GenerationID       uint64
	CommitSeq          uint64
	CandidateID        CandidateIDV1
	Capability         ReuseCapability
	Retirements        []COWRetirementV1
	AuxiliaryPageCount int
	Limits             *COWPrepareLimitsV1
}

// PreparedCOWPublicationPacketV1 owns a private visible/seal/prefix future in
// the ordinary allocator. It is not a finite certificate or a second engine.
// Every mutable transaction has ONE owning role; phase aliases are borrows.
// After visible activation the only supported future is exact forward
// completion, or retained uncertainty through joined shutdown/recovery.
type PreparedCOWPublicationPacketV1 struct {
	allocator        atomic.Pointer[Allocator]
	creator          *AllocationCreatorV1
	visible, seal    *PreparedCOWCandidateV1 // borrows: existing ownedCandidates owns backing
	rollback         *FreelistTxn
	visibleNext      *FreelistTxn
	sealStage        *FreelistTxn
	sealNext         *FreelistTxn
	finalTxn         *FreelistTxn
	abortTxn         *FreelistTxn
	expectedLiveTxn  *FreelistTxn              // borrow only, exact installed phase origin
	prefix           []*PreparedCOWCandidateV1 // exact preborn prefix, including visible and seal
	ids              []CandidateIDV1
	coverage         []packetReservationProofV1
	nextCapability   ReuseCapability
	rollbackStats    Stats
	finalPruneWork   BoundedPruneStats
	storageAttempted bool  // irreversible caller WAL or exact physical installer attempt
	phase            uint8 // 0 private, 1 visible, 2 seal visible, 3 consumed, 4 aborted/closed
}

// VisibleCandidateV1 and SealCandidateV1 return exact controls, not raw page or
// generation exports. The DB owns each control through its existing terminal
// handoff and may write only its immutable candidate images to the concrete pager.
func (p *PreparedCOWPublicationPacketV1) VisibleCandidateV1() *PreparedCOWCandidateV1 {
	if p == nil {
		return nil
	}
	a := p.allocator.Load()
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if p.phase >= 3 {
		return nil
	}
	return p.visible
}
func (p *PreparedCOWPublicationPacketV1) SealCandidateV1() *PreparedCOWCandidateV1 {
	if p == nil {
		return nil
	}
	a := p.allocator.Load()
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if p.phase >= 3 {
		return nil
	}
	return p.seal
}

// PrepareOwnedCOWPublicationPacketV1 builds the complete selected future before
// the DB may append WAL or physically publish any role. It uses two explicit
// private stages of the same engine, never two public Prepare calls. The final
// capability is the caller-proven future recovery/pin cut; no horizon is inferred
// from candidate count. Missing/unsupported authority refuses before publication.
func (a *Allocator) PrepareOwnedCOWPublicationPacketV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, visible, seal COWPublicationStageSpecV1, prefix []*PreparedCOWCandidateV1, nextCapability ReuseCapability) (*PreparedCOWPublicationPacketV1, error) {
	if a == nil || request == nil || scratch == nil {
		return nil, ErrAllocationCertificateIncompleteV1
	}
	if visible.GenerationID == 0 || visible.CommitSeq == 0 || visible.CandidateID == (CandidateIDV1{}) || visible.AuxiliaryPageCount < 0 || seal.GenerationID <= visible.GenerationID || seal.CommitSeq < visible.CommitSeq || seal.CandidateID == (CandidateIDV1{}) || seal.CandidateID == visible.CandidateID || seal.AuxiliaryPageCount < 0 {
		return nil, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.cow
	if a.closed {
		return nil, ErrCandidateConsumed
	}
	if state == nil || state.txn == nil || state.ledger == nil {
		return nil, ErrGenerationFormat
	}
	if state.waitErr != nil {
		return nil, state.waitErr
	}
	if state.packet != nil || state.prepared != nil {
		return nil, ErrCOWCandidatePrepared
	}
	if scratch == state.txn.buildCreator || state.txn.buildCreator == nil || state.residentAdmissionCreator != state.txn.buildCreator {
		return nil, ErrAllocationCertificateIncompleteV1
	}
	if err := state.txn.requireBirthCreditV1(request); err != nil {
		return nil, err
	}
	state.ledger.mu.Lock()
	provenanceErr := a.residentProvenanceLockedV1()
	if provenanceErr == nil && (state.ledger.candidates.Value(visible.CandidateID) != nil || state.ledger.candidates.Value(seal.CandidateID) != nil) {
		provenanceErr = ErrCandidateConsumed
	}
	state.ledger.mu.Unlock()
	if provenanceErr != nil {
		return nil, provenanceErr
	}
	if len(prefix) != len(state.activated) {
		return nil, ErrCandidateConsumed
	}
	for i, member := range prefix {
		if member == nil || state.activated[i] != member || !member.activated || member.published || !member.ownedBacking {
			return nil, ErrCandidateConsumed
		}
		if member.candidateID == visible.CandidateID || member.candidateID == seal.CandidateID {
			return nil, ErrGenerationFormat
		}
		for j := 0; j < i; j++ {
			if prefix[j] == member || prefix[j].candidateID == member.candidateID {
				return nil, ErrGenerationFormat
			}
		}
	}
	// The public finite limits branch remains closed for both stages.
	for _, spec := range [2]COWPublicationStageSpecV1{visible, seal} {
		if spec.Limits != nil && spec.Limits.AllocationCredit != nil {
			return nil, ErrAllocationCertificateIncompleteV1
		}
		if err := checkCOWPrepareLimitsV1(a.cowPrepareProfileLockedV1(), spec.Retirements, spec.AuxiliaryPageCount, spec.Limits); err != nil {
			return nil, err
		}
	}
	maximum := int(^uint(0) >> 1)
	if len(prefix) > maximum-2 {
		return nil, ErrNoAllocatablePage
	}
	count := len(prefix) + 2
	if uint64(count) > uint64(maximum)/uint64(unsafe.Sizeof(CandidateIDV1{})) {
		return nil, ErrNoAllocatablePage
	}
	creator := state.txn.buildCreator
	bytes := allocationClassV1(uint64(unsafe.Sizeof(PreparedCOWPublicationPacketV1{})), true)
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(count)*uint64(unsafe.Sizeof((*PreparedCOWCandidateV1)(nil))), true))
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(count)*uint64(unsafe.Sizeof(CandidateIDV1{})), false))
	if bytes == ^uint64(0) {
		return nil, ErrNoAllocatablePage
	}
	if err := creator.reserve(request, bytes, 1); err != nil {
		return nil, err
	}
	p := &PreparedCOWPublicationPacketV1{creator: creator, nextCapability: nextCapability, rollbackStats: a.stats, prefix: make([]*PreparedCOWCandidateV1, count), ids: make([]CandidateIDV1, count)}
	p.allocator.Store(a)
	copy(p.prefix, prefix)
	// Admit BOTH activated slots and conservative attempted-tail rollback before
	// either private stage can emit. Full old/new buffer overlap is charged.
	if err := a.growActivatedCapacityLockedV1(request, creator, 2); err != nil {
		p.releasePrivateRolesLockedV1()
		return nil, err
	}
	state.ledger.mu.Lock()
	err := state.ledger.growBurnedTailCapacityLockedV1(request, creator, 2)
	state.ledger.mu.Unlock()
	if err != nil {
		p.releasePrivateRolesLockedV1()
		return nil, err
	}
	// Abort isolation is actual preborn backing, never a post-WAL reserve promise.
	p.abortTxn, err = state.txn.cloneForPrivateAllocatorPrepare(request)
	if err != nil {
		p.releasePrivateRolesLockedV1()
		return nil, err
	}
	var visibleStage *FreelistTxn
	visibleStage, p.visible, err = a.preparePrivateCOWStageLockedV1(request, scratch, state.txn, visible.GenerationID, visible.CommitSeq, visible.CandidateID, visible.Capability, visible.Retirements, visible.AuxiliaryPageCount, ownedCandidatePageSinkV1{}, true)
	if err != nil {
		rollbackErr := state.ledger.rollbackPreparedPacketV1([2]CandidateIDV1{visible.CandidateID, seal.CandidateID})
		p.releasePrivateRolesLockedV1()
		a.stats = p.rollbackStats
		if rollbackErr != nil {
			state.waitErr = errors.Join(err, rollbackErr)
			state.ready.Broadcast()
			return nil, state.waitErr
		}
		return nil, err
	}
	p.visibleNext, p.visible.activationTxn = p.visible.activationTxn, nil
	// Exactly one owner for this intermediate transaction: the packet, not a
	// duplicate prepared.rollbackTxn edge. It becomes seal's private source.
	if err = checkCOWPrepareLimitsV1(a.cowPrepareProfileForTxnLockedV1(p.visibleNext), seal.Retirements, seal.AuxiliaryPageCount, seal.Limits); err == nil {
		p.sealStage, p.seal, err = a.preparePrivateCOWStageLockedV1(request, scratch, p.visibleNext, seal.GenerationID, seal.CommitSeq, seal.CandidateID, seal.Capability, seal.Retirements, seal.AuxiliaryPageCount, ownedCandidatePageSinkV1{}, true)
	}
	if err == nil {
		p.sealNext, p.seal.activationTxn = p.seal.activationTxn, nil
		p.finalTxn, err = p.sealNext.cloneForPrivateAllocatorPrepare(request)
	}
	if err == nil {
		if state.boundedPrune {
			plan, planErr := p.finalTxn.prepareBoundedPruneV1(request, scratch, nextCapability)
			if planErr != nil {
				err = planErr
			} else {
				p.finalPruneWork, err = p.finalTxn.applyBoundedPruneV1(request, &plan)
				plan.operation.close()
			}
		} else {
			p.finalTxn.PruneWithCapabilityWithAllocationRequestV1(request, scratch, nextCapability)
			err = p.finalTxn.valid()
		}
	}
	if err == nil {
		p.prefix[len(prefix)], p.prefix[len(prefix)+1] = p.visible, p.seal
		for i, member := range p.prefix {
			p.ids[i] = member.candidateID
		}
		err = p.prepareCoverageLockedV1(request, scratch)
	}
	if err != nil {
		// No publication effect crossed. Both reservations were preadmitted for
		// conservative attempted tails; failure preserves ledger authority on any
		// inconsistent rollback instead of silently acknowledging partial cleanup.
		rollbackErr := state.ledger.rollbackPreparedPacketV1([2]CandidateIDV1{visible.CandidateID, seal.CandidateID})
		releaseTxnV1(visibleStage)
		p.releaseUninstalledCandidatesLockedV1()
		p.releasePrivateRolesLockedV1()
		a.stats = p.rollbackStats
		if rollbackErr != nil {
			state.waitErr = errors.Join(err, rollbackErr)
			state.ready.Broadcast()
			return nil, state.waitErr
		}
		return nil, err
	}
	p.rollback = state.txn
	state.txn = visibleStage
	p.expectedLiveTxn = visibleStage
	p.visible.packet, p.seal.packet = p, p
	p.visible.ownedNext = state.ownedCandidates
	state.ownedCandidates = p.visible
	p.seal.ownedNext = state.ownedCandidates
	state.ownedCandidates = p.seal
	state.packet, state.prepared = p, p.visible
	return p, nil
}

// packetOwnedTransactionsV1 returns unique actual owning roles. The live state
// txn is owned separately; every alias is removed before its last edge release.
func (p *PreparedCOWPublicationPacketV1) packetOwnedTransactionsV1() [6]*FreelistTxn {
	if p == nil {
		return [6]*FreelistTxn{}
	}
	return [6]*FreelistTxn{p.rollback, p.visibleNext, p.sealStage, p.sealNext, p.finalTxn, p.abortTxn}
}
func (p *PreparedCOWPublicationPacketV1) releasePrivateRolesLockedV1() {
	roles := p.packetOwnedTransactionsV1()
	p.rollback, p.visibleNext, p.sealStage, p.sealNext, p.finalTxn, p.abortTxn = nil, nil, nil, nil, nil, nil
	p.expectedLiveTxn = nil
	for i := range p.coverage[:cap(p.coverage)] {
		proof := &p.coverage[i]
		clear(proof.ids[:cap(proof.ids)])
		clear(proof.abandoned[:cap(proof.abandoned)])
		*proof = packetReservationProofV1{}
	}
	clear(p.coverage[:cap(p.coverage)])
	p.coverage = nil
	clear(p.prefix[:cap(p.prefix)])
	clear(p.ids[:cap(p.ids)])
	p.prefix, p.ids = nil, nil
	creator := p.creator
	p.creator = nil
	p.allocator.Store(nil)
	p.phase = 4
	for _, txn := range roles {
		releaseTxnV1(txn)
	}
	creator.release()
}
func (p *PreparedCOWPublicationPacketV1) releaseUninstalledCandidatesLockedV1() {
	for _, prepared := range [2]*PreparedCOWCandidateV1{p.visible, p.seal} {
		if prepared == nil {
			continue
		}
		releaseTxnV1(prepared.activationTxn)
		prepared.activationTxn = nil
		releaseCandidateOwnedBackingV1(prepared.candidate)
		prepared.candidate = nil
		clear(prepared.auxiliary[:cap(prepared.auxiliary)])
		prepared.auxiliary = nil
		creator := prepared.creator
		prepared.creator = nil
		prepared.allocator = nil
		creator.release()
	}
	p.visible, p.seal = nil, nil
}

func (a *Allocator) activateCOWPacketRoleLockedV1(prepared *PreparedCOWCandidateV1) error {
	p := prepared.packet
	if p == nil || a.cow.packet != p || p.allocator.Load() != a {
		return ErrCandidateConsumed
	}
	var next *FreelistTxn
	switch p.phase {
	case 0:
		if prepared != p.visible {
			return ErrCandidateConsumed
		}
		next = p.sealStage
	case 1:
		if prepared != p.seal {
			return ErrCandidateConsumed
		}
		next = p.sealNext
	default:
		return ErrCandidateConsumed
	}
	if a.cow.txn != p.expectedLiveTxn || next == nil || next.base != prepared.candidate.generation || next.ledger != a.cow.ledger || cap(a.cow.activated) <= len(a.cow.activated) {
		return ErrAllocationCertificateIncompleteV1
	}
	if err := a.cow.ledger.markPreparedPacketVisibleV1(prepared.candidateID); err != nil {
		return err
	}
	oldTxn, oldGeneration := a.cow.txn, a.cow.generation
	a.cow.txn = next
	p.expectedLiveTxn = next
	a.cow.generation = retainGenerationV1(prepared.candidate.generation)
	if p.phase == 0 {
		p.sealStage = nil
		p.phase = 1
		a.cow.prepared = p.seal
	} else {
		p.sealNext = nil
		p.phase = 2
		a.cow.prepared = nil
	}
	a.cow.activated = append(a.cow.activated, prepared)
	prepared.activated = true
	releaseTxnV1(oldTxn)
	releaseGenerationV1(oldGeneration)
	a.stats.FreeIDs = prepared.candidate.generation.FreeCount()
	a.cow.ready.Broadcast()
	return nil
}

func (a *Allocator) abortCOWPublicationPacketLockedV1(p *PreparedCOWPublicationPacketV1, prepared *PreparedCOWCandidateV1) error {
	if a.closed || a.cow == nil || a.cow.packet != p || p.allocator.Load() != a || p.phase != 0 || prepared != p.visible || a.cow.prepared != p.visible {
		return ErrCandidateConsumed
	}
	if p.storageAttempted {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	if p.abortTxn == nil {
		return ErrAllocationCertificateIncompleteV1
	}
	if err := a.cow.ledger.rollbackPreparedPacketV1([2]CandidateIDV1{p.visible.candidateID, p.seal.candidateID}); err != nil {
		return err
	}
	stage := a.cow.txn
	a.cow.txn, p.abortTxn = p.abortTxn, nil
	a.cow.prepared, a.cow.packet = nil, nil
	a.stats = p.rollbackStats
	p.visible.packet, p.seal.packet = nil, nil
	// Both controls become terminal nonvisible; their owned image edges remain
	// on ownedCandidates until their exact ClearTerminalBacking calls or shutdown.
	p.visible.rollbackTxn, p.seal.rollbackTxn = nil, nil
	p.visible, p.seal = nil, nil
	releaseTxnV1(stage)
	p.releasePrivateRolesLockedV1()
	a.cow.ready.Broadcast()
	return nil
}

func (a *Allocator) publishCOWPacketPrefixLockedV1(prefix []*PreparedCOWCandidateV1, nextCapability ReuseCapability) error {
	p := a.cow.packet
	if p == nil || p.allocator.Load() != a || p.phase != 2 || p.finalTxn == nil || nextCapability != p.nextCapability || len(prefix) != len(p.prefix) || len(a.cow.activated) < len(prefix) {
		return ErrCandidateConsumed
	}
	for i, member := range prefix {
		if member == nil || member != p.prefix[i] || a.cow.activated[i] != member || !member.activated || member.published || member.candidateID != p.ids[i] {
			return ErrCandidateConsumed
		}
	}
	if a.cow.txn != p.expectedLiveTxn || a.cow.txn.base != p.seal.candidate.generation {
		return ErrCandidateConsumed
	}
	if err := a.validateCOWPhysicalTailLockedV1(p.seal.candidate.generation.highWater); err != nil {
		return err
	}
	// Allocation-free exact duplicate/coverage/state validation precedes the
	// first ownership mutation. No radix, scratch, request or callback is created.
	if err := a.cow.ledger.publishPreparedPacketV1(p.ids, p.coverage); err != nil {
		return err
	}
	for _, member := range prefix {
		member.published = true
	}
	copy(a.cow.activated, a.cow.activated[len(prefix):])
	clear(a.cow.activated[len(a.cow.activated)-len(prefix):])
	a.cow.activated = a.cow.activated[:len(a.cow.activated)-len(prefix)]
	oldTxn := a.cow.txn
	a.cow.txn, p.finalTxn = p.finalTxn, nil
	a.recordBoundedPruneWorkV1(p.finalPruneWork)
	a.cow.packet = nil
	p.visible.packet, p.seal.packet = nil, nil
	p.visible, p.seal = nil, nil
	releaseTxnV1(oldTxn)
	p.phase = 3
	p.releasePrivateRolesLockedV1()
	a.cow.ready.Broadcast()
	return nil
}
