package freelist

import (
	"slices"
	"unsafe"
)

// packetReservationProofV1 owns exact immutable descriptor copies made before
// WAL. It borrows the actual ledger-owned reservation, never a copied identity
// ledger. Content/order, full allocated coverage and abandoned intervals are
// revalidated without allocation before the first consume/delete.
type packetReservationProofV1 struct {
	reservation                                      *reservation
	ids                                              []uint64
	abandoned                                        []reservationInterval
	tailStart, tailCount                             uint64
	tailReserved, tailWriteAttempted, reusedMetadata bool
}

func (p *PreparedCOWPublicationPacketV1) retainedControlBytesV1() uint64 {
	bytes := allocationClassV1(uint64(unsafe.Sizeof(*p)), true)
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(cap(p.prefix))*uint64(unsafe.Sizeof((*PreparedCOWCandidateV1)(nil))), true))
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(cap(p.ids))*uint64(unsafe.Sizeof(CandidateIDV1{})), false))
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(cap(p.coverage))*uint64(unsafe.Sizeof(packetReservationProofV1{})), true))
	for _, proof := range p.coverage[:cap(p.coverage)] {
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(cap(proof.ids))*8, false))
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(cap(proof.abandoned))*uint64(unsafe.Sizeof(reservationInterval{})), false))
	}
	return bytes
}

func (p *PreparedCOWPublicationPacketV1) prepareCoverageLockedV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1) error {
	l := p.allocator.Load().cow.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	count := len(p.prefix)
	maximum := uint64(^uint(0) >> 1)
	if uint64(count) > maximum/uint64(unsafe.Sizeof(packetReservationProofV1{})) {
		return ErrNoAllocatablePage
	}
	bytes := allocationClassV1(uint64(count)*uint64(unsafe.Sizeof(packetReservationProofV1{})), true)
	for i, member := range p.prefix {
		if member == nil || member.candidate == nil || member.candidate.generation == nil || member.candidate.generation.record.CandidateID != p.ids[i] {
			return ErrGenerationFormat
		}
		r := l.candidates.Value(p.ids[i])
		if r == nil {
			return ErrCandidateConsumed
		}
		expected := CandidateVisible
		if member == p.visible || member == p.seal {
			expected = CandidatePreVisible
		}
		if r.state != expected || !r.tailReserved || r.tailCount == 0 || r.tailStart > ^uint64(0)-r.tailCount {
			return ErrCandidateConsumed
		}
		if uint64(len(r.ids)) > maximum/8 || uint64(len(r.abandonedCoverage)) > maximum/uint64(unsafe.Sizeof(reservationInterval{})) {
			return ErrNoAllocatablePage
		}
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(len(r.ids))*8, false))
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(len(r.abandonedCoverage))*uint64(unsafe.Sizeof(reservationInterval{})), false))
		// Validate the exact whole allocated set while request/scratch exist. This
		// extra full-capacity sorting copy is scrubbed before retained cut transfer.
		if err := reserveAllocationScratchV1(request, scratch, p.creator, uint64(len(r.ids)), 8, false); err != nil {
			return err
		}
		sorted := make([]uint64, len(r.ids))
		copy(sorted, r.ids)
		slices.Sort(sorted)
		valid := true
		for j, id := range sorted {
			owner, ok := l.owners.Get(id)
			if id < 2 || !ok || owner != p.ids[i] || j > 0 && sorted[j-1] == id {
				valid = false
				break
			}
		}
		clear(sorted[:cap(sorted)])
		sorted = nil
		if !valid || countPacketOwnerEntriesV1(l.owners.root, p.ids[i]) != len(r.ids) {
			return ErrGenerationFormat
		}
		for _, extent := range r.abandonedCoverage {
			if extent.count == 0 || extent.start > ^uint64(0)-extent.count {
				return ErrGenerationFormat
			}
		}
	}
	if bytes == ^uint64(0) {
		return ErrNoAllocatablePage
	}
	// Actual escaping proof controls share the packet's intrinsic creator edge.
	// The request is borrowed only for this reserve; no new account is installed.
	if err := p.creator.reserve(request, bytes, 0); err != nil {
		return err
	}
	p.coverage = make([]packetReservationProofV1, count)
	for i, id := range p.ids {
		r := l.candidates.Value(id)
		proof := &p.coverage[i]
		proof.reservation = r
		proof.ids = make([]uint64, len(r.ids))
		copy(proof.ids, r.ids)
		proof.abandoned = make([]reservationInterval, len(r.abandonedCoverage))
		copy(proof.abandoned, r.abandonedCoverage)
		proof.tailStart, proof.tailCount = r.tailStart, r.tailCount
		proof.tailReserved, proof.tailWriteAttempted, proof.reusedMetadata = r.tailReserved, r.tailWriteAttempted, r.reusedMetadata
	}
	return nil
}

func (l *ReservationLedger) publishPreparedPacketV1(ids []CandidateIDV1, proofs []packetReservationProofV1) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.validatePreparedPacketProofsLockedV1(ids, proofs, true); err != nil {
		return err
	}
	l.publishValidatedBatchLockedV1(ids)
	return nil
}

func (l *ReservationLedger) validatePreparedPacketProofsLockedV1(ids []CandidateIDV1, proofs []packetReservationProofV1, allVisible bool) error {
	if len(ids) == 0 || len(ids) != len(proofs) {
		return ErrGenerationFormat
	}
	for i, id := range ids {
		if id == (CandidateIDV1{}) {
			return ErrGenerationFormat
		}
		for j := 0; j < i; j++ {
			if ids[j] == id {
				return ErrGenerationFormat
			}
		}
		r := l.candidates.Value(id)
		proof := &proofs[i]
		if r == nil || r != proof.reservation || (allVisible && r.state != CandidateVisible || !allVisible && r.state != CandidateVisible && r.state != CandidatePreVisible) || r.tailReserved != proof.tailReserved || r.tailWriteAttempted != proof.tailWriteAttempted || r.reusedMetadata != proof.reusedMetadata || r.tailStart != proof.tailStart || r.tailCount != proof.tailCount || len(r.ids) != len(proof.ids) || len(r.abandonedCoverage) != len(proof.abandoned) {
			return ErrCandidateConsumed
		}
		if countPacketOwnerEntriesV1(l.owners.root, id) != len(proof.ids) {
			return ErrGenerationFormat
		}
		for j, pageID := range r.ids {
			owner, ok := l.owners.Get(pageID)
			if pageID != proof.ids[j] || !ok || owner != id {
				return ErrGenerationFormat
			}
		}
		for j, extent := range r.abandonedCoverage {
			if extent != proof.abandoned[j] {
				return ErrGenerationFormat
			}
		}
	}
	// The same release/burn coverage loop used by ordinary PublishBatch. No new
	// backing, callback or request survives into this terminal consume operation.
	return nil
}

// Direct read-only walk of the EXISTING owners trie. No new key registry,
// callback/control closure or allocation is needed to prove no owner omitted
// from the reservation's exact allocated descriptor set.
func countPacketOwnerEntriesV1(ref numericRadixRefV1[uint64, CandidateIDV1], id CandidateIDV1) int {
	if ref.chunk == nil {
		return 0
	}
	node := ref.node()
	if node.leaf {
		if node.value == id {
			return 1
		}
		return 0
	}
	return countPacketOwnerEntriesV1(node.child[0], id) + countPacketOwnerEntriesV1(node.child[1], id)
}
