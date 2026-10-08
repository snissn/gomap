package freelist

// ReserveCOWPageBeforePublicationV1 reserves a real page ID in the SAME current
// transaction without growing or writing the pager. The installed serialized
// DB staging builder borrows request on this call stack, supplies every exact
// staged ID to the packet and later installs images only after its WAL boundary.
// This managed seam supplies no finite certificate or alternative allocator.
// Ordinary Alloc/AllocAppend continue their existing physical-growth behavior.
func (a *Allocator) ReserveCOWPageBeforePublicationV1(request AllocationRequestCreditV1, hint uint64, appendOnly bool) (uint64, error) {
	if a == nil || request == nil {
		return 0, ErrAllocationCertificateIncompleteV1
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0, ErrCandidateConsumed
	}
	state := a.cow
	if state == nil || state.txn == nil || state.ledger == nil || a.pager == nil {
		return 0, ErrGenerationFormat
	}
	if state.waitErr != nil {
		return 0, state.waitErr
	}
	if state.prepared != nil || state.packet != nil {
		return 0, ErrCOWCandidatePrepared
	}
	if state.txn.buildCreator == nil || state.residentAdmissionCreator != state.txn.buildCreator {
		return 0, ErrAllocationCertificateIncompleteV1
	}
	state.ledger.mu.Lock()
	err := a.residentProvenanceLockedV1()
	state.ledger.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if err = state.txn.requireBirthCreditV1(request); err != nil {
		return 0, err
	}
	if state.txn.highWater < a.pager.PageCount() {
		return 0, ErrGenerationFormat
	}
	var id uint64
	if appendOnly || a.preferAppend {
		id, err = state.txn.AllocateAppendWithAllocationRequestV1(request)
	} else {
		id, err = state.txn.AllocateWithAllocationRequestV1(request, hint)
	}
	if err != nil {
		return 0, err
	}
	if appendOnly || id >= a.pager.PageCount() {
		a.stats.AppendAllocPages++
	} else {
		a.stats.ReuseAllocPages++
	}
	a.stats.AllocPages++
	a.lastAlloc = id
	return id, nil
}

// GrowCOWPublicationPacketPagerV1 is the checked storage-only installer seam.
// Call only AFTER the DB's actual WAL boundary while holding its existing
// installed serializer/lifecycle ownership. It accepts only the exact current
// packet role and grows to that role's preborn immutable high-water, never a
// caller-supplied larger value. Unknown/stale role, coverage or phase refuses
// before physical growth. The library cannot infer whether WAL is durable.
func (a *Allocator) GrowCOWPublicationPacketPagerV1(packet *PreparedCOWPublicationPacketV1, prepared *PreparedCOWCandidateV1) error {
	if a == nil || packet == nil || prepared == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	highWater, err := a.currentCOWPacketPageRoleLockedV1(packet, prepared)
	if err != nil {
		return err
	}
	if a.pager.PageCount() > highWater {
		return ErrGenerationFormat
	}
	// Validate exact selected coverage while the same lock holds the frontier.
	// A changed descriptor cannot become physically visible through this seam.
	a.cow.ledger.mu.Lock()
	err = a.cow.ledger.validatePreparedPacketProofsLockedV1(packet.ids, packet.coverage, false)
	a.cow.ledger.mu.Unlock()
	if err != nil {
		return err
	}
	// Once an actual installer attempt is possible, no original-frontier Abort
	// is permitted even when GrowTo returns an ambiguous or partial error.
	packet.storageAttempted = true
	return a.pager.GrowTo(highWater)
}

// currentCOWPacketPageRoleLockedV1 checks the same installed role for a read-only
// image proof and physical growth. The caller holds a.mu. It deliberately does
// not require the pager to have grown: image admission precedes the WAL boundary.
func (a *Allocator) currentCOWPacketPageRoleLockedV1(packet *PreparedCOWPublicationPacketV1, prepared *PreparedCOWCandidateV1) (uint64, error) {
	if a.closed {
		return 0, ErrCandidateConsumed
	}
	if a.cow == nil || a.pager == nil || a.cow.packet != packet || packet.allocator.Load() != a || a.cow.prepared != prepared || prepared.packet != packet || a.cow.txn != packet.expectedLiveTxn {
		return 0, ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return 0, a.cow.waitErr
	}
	if packet.phase == 0 {
		if prepared != packet.visible {
			return 0, ErrCandidateConsumed
		}
	} else if packet.phase == 1 {
		if prepared != packet.seal {
			return 0, ErrCandidateConsumed
		}
	} else {
		return 0, ErrCandidateConsumed
	}
	if prepared.candidate == nil || prepared.candidate.generation == nil {
		return 0, ErrGenerationFormat
	}
	highWater := prepared.candidate.generation.highWater
	if highWater < 2 {
		return 0, ErrGenerationFormat
	}
	return highWater, nil
}

// ValidateCOWPublicationPacketPageIDsV1 proves that every supplied staged image
// is data owned by the exact current packet role, including reused pages. The
// sorted unique vector is borrowed only on this stack; no copy, sorting,
// callback, request debit, backing birth or pager effect occurs. Metadata tails
// and other members' data are not admitted by a high-water comparison.
//
// This validates the existing full immutable packet proof before each image
// membership lookup. Its work includes whole owner-trie cardinality checks; it
// is not a finite-work or performance certificate. Empty input proves only an
// empty supplied set. The caller still proves complete output coverage, exact
// installed Pager/writer/lifecycle and an unchanged serializer through install.
func (a *Allocator) ValidateCOWPublicationPacketPageIDsV1(packet *PreparedCOWPublicationPacketV1, prepared *PreparedCOWCandidateV1, sortedUniqueImageIDs []uint64) error {
	if a == nil || packet == nil || prepared == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	highWater, err := a.currentCOWPacketPageRoleLockedV1(packet, prepared)
	if err != nil {
		return err
	}
	// Bind the identity to the exact immutable role, not a caller-supplied ID.
	index := len(packet.ids) - 2 + int(packet.phase)
	if index < 0 || index >= len(packet.ids) || index >= len(packet.prefix) || packet.prefix[index] != prepared || packet.ids[index] != prepared.candidateID || prepared.candidate.generation.record.CandidateID != prepared.candidateID {
		return ErrGenerationFormat
	}
	roleID := prepared.candidateID
	a.cow.ledger.mu.Lock()
	defer a.cow.ledger.mu.Unlock()
	if err = a.cow.ledger.validatePreparedPacketProofsLockedV1(packet.ids, packet.coverage, false); err != nil {
		return err
	}
	for i, id := range sortedUniqueImageIDs {
		if id < 2 || id >= highWater || i > 0 && sortedUniqueImageIDs[i-1] >= id {
			return ErrGenerationFormat
		}
		owner, ok := a.cow.ledger.owners.Get(id)
		if !ok || owner != roleID {
			return ErrGenerationFormat
		}
	}
	return nil
}

// MarkCOWPublicationPacketStorageAttemptV1 irreversibly closes original-frontier
// Abort BEFORE the installed caller attempts WAL append. It creates no backing
// and retains no request. The exact packet stays available for forward retry;
// uncertainty is reported through the ordinary FailCOWCandidateV1 mechanism.
func (a *Allocator) MarkCOWPublicationPacketStorageAttemptV1(packet *PreparedCOWPublicationPacketV1) error {
	if a == nil || packet == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.cow == nil || a.cow.packet != packet || packet.allocator.Load() != a || packet.phase != 0 || a.cow.prepared != packet.visible || a.cow.txn != packet.expectedLiveTxn {
		return ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return a.cow.waitErr
	}
	packet.storageAttempted = true
	return nil
}
