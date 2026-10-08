package freelist

// TryCloseCOWOwnersV1 closes a healthy retired allocator. Pager closure does
// not prove that an exact publication/recovery owner has become terminal.
func (a *Allocator) TryCloseCOWOwnersV1() bool {
	if a == nil {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.closed {
		a.closed = true
		return true
	}
	if a.cow.packet != nil || a.cow.prepared != nil || len(a.cow.activated) != 0 || a.cow.ownedCandidates != nil || a.cow.waitErr != nil {
		return false
	}
	a.closeCOWOwnersLockedV1()
	return true
}

// CloseCOWOwnersAfterShutdownV1 is the final DB shutdown seam. Call only after
// writer/publisher shutdown and the exact runtime and recovery handoff releases.
// It never revokes independently retained physical-cut generation edges.
func (a *Allocator) CloseCOWOwnersAfterShutdownV1() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closeCOWOwnersLockedV1()
}

func (a *Allocator) closeCOWOwnersLockedV1() {
	a.closed = true
	if a.cow == nil || a.cow.closed {
		return
	}
	state := a.cow
	state.closed = true
	state.residentAdmissionCreator = nil
	state.waitErr = ErrCandidateConsumed
	if packet := state.packet; packet != nil {
		state.packet = nil
		if packet.visible != nil {
			packet.visible.packet = nil
		}
		if packet.seal != nil {
			packet.seal.packet = nil
		}
		packet.visible, packet.seal = nil, nil
		packet.releasePrivateRolesLockedV1()
	}
	closePrepared := func(prepared *PreparedCOWCandidateV1) {
		if prepared == nil {
			return
		}
		prepared.backingMu.Lock()
		defer prepared.backingMu.Unlock()
		releaseTxnV1(prepared.activationTxn)
		prepared.activationTxn = nil
		releaseTxnV1(prepared.rollbackTxn)
		prepared.rollbackTxn = nil
		candidate := prepared.candidate
		if candidate == nil {
			return
		}
		// Ordinary public candidates and their retaining writers preserve their
		// indefinite API. Finite backing cannot escape through those paths.
		if !candidate.hasFiniteBackingV1() && (!prepared.ownedBacking || state.rawGenerationEscaped) {
			return
		}
		// Finite generic callbacks are refused; ordinary retaining callbacks
		// must never be waited on while their allocator getter can reenter.
		candidate.writeMu.Lock()
		defer candidate.writeMu.Unlock()
		generation, creator, preparedCreator := candidate.generation, candidate.creator, prepared.creator
		clear(candidate.pages[:cap(candidate.pages)])
		clear(candidate.dirtyIDs[:cap(candidate.dirtyIDs)])
		clear(prepared.auxiliary[:cap(prepared.auxiliary)])
		candidate.pages, candidate.dirtyIDs, candidate.generation = nil, nil, nil
		candidate.creator = nil
		prepared.candidate, prepared.auxiliary, prepared.creator = nil, nil, nil
		prepared.allocator = nil
		releaseGenerationV1(generation)
		creator.release()
		preparedCreator.release()
	}
	if state.prepared != nil && !state.prepared.ownedBacking {
		closePrepared(state.prepared)
	}
	for _, prepared := range state.activated {
		if !prepared.ownedBacking {
			closePrepared(prepared)
		}
	}
	for prepared := state.ownedCandidates; prepared != nil; {
		next := prepared.ownedNext
		closePrepared(prepared)
		prepared.ownedNext, prepared.ownedBacking = nil, false
		prepared = next
	}
	state.ownedCandidates = nil
	clear(state.activated[:cap(state.activated)])
	state.prepared, state.activated = nil, nil
	activatedCreator := state.activatedCreator
	state.activatedCreator = nil
	activatedCreator.release()
	txn, generation, ledger := state.txn, state.generation, state.ledger
	state.txn, state.generation, state.ledger = nil, nil, nil
	releaseTxnV1(txn)
	releaseGenerationV1(generation)
	releaseReservationLedgerV1(ledger)
	if state.ready != nil {
		state.ready.Broadcast()
	}
	a.releaseClosedCOWStateCreatorLockedV1()
}

// These are actual allocator/transaction edges on the existing shared ledger,
// not an owner registry. Unmanaged ordinary callers retain their scalar ledger
// contract; only finite reservation backing is scrubbed at the last edge.
func retainReservationLedgerV1(ledger *ReservationLedger) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	ledger.ownedRefs++
	ledger.mu.Unlock()
}
func releaseReservationLedgerV1(ledger *ReservationLedger) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.ownedRefs == 0 {
		return
	}
	ledger.ownedRefs--
	if ledger.ownedRefs != 0 {
		return
	}
	for {
		var candidate CandidateIDV1
		var backing *reservation
		ledger.candidates.Range(func(key CandidateIDV1, r *reservation) bool {
			if r != nil && (r.creator != nil || r.idsCreator != nil || r.coverageCreator != nil) {
				candidate, backing = key, r
				return false
			}
			return true
		})
		if backing == nil {
			creator, burnedCreator := ledger.creator, ledger.burnedCreator
			if creator != nil && !ledger.rawEscaped {
				ledger.owners.Clear()
				ledger.candidates.Clear()
			}
			if burnedCreator != nil || creator != nil && !ledger.rawEscaped {
				clear(ledger.burnedTails[:cap(ledger.burnedTails)])
				ledger.burnedTails = nil
			}
			ledger.creator, ledger.burnedCreator = nil, nil
			creator.release()
			burnedCreator.release()

			return
		}
		for _, id := range backing.ids {
			ledger.owners.Delete(id)
		}
		ledger.candidates.Delete(candidate)
		releaseReservationBackingV1(backing)
	}
}

// Only the last actual managed index/cut/Cond edge can detach control backing.
// a.closed is permanent and prevents cow=nil from restoring legacy dispatch.
func (a *Allocator) releaseClosedCOWStateCreatorLockedV1() {
	state := a.cow
	if state == nil || !state.closed || state.generationLeases != nil || state.readyWaiters != 0 ||
		a.writerAuthority != nil && !a.writerDetached {
		return
	}
	creator := state.creator
	state.creator, state.ready = nil, nil
	a.cow, a.writerAuthority = nil, nil
	creator.release()
}
