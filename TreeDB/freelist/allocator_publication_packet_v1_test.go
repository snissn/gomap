package freelist

import (
	"errors"
	"testing"
	"unsafe"
)

// Component-only synthetic accounts: the actual installed M creator/provenance,
// complete caller classes, fixed cap fit and Linux witness remain separate gates.
func packetFixture5108(t *testing.T) (*Allocator, *componentBorrowedRequest5108, *radixCredit5105, *AllocationCreatorV1, *AllocationCreatorV1) {
	t.Helper()
	a := managedTerminalAllocator5105(t)
	resident, creator := buildCreditLease5105(t)
	request := &componentBorrowedRequest5108{}
	_, scratch := buildCreditLease5105(t)
	if _, err := a.AdmitManagedResidentAllocationV1(request, creator, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		authority := a.writerAuthority
		a.CloseCOWOwnersAfterShutdownV1()
		a.DetachManagedIndexWriterV1(authority)
		scratch.release()
		creator.release()
	})
	return a, request, resident, creator, scratch
}
func packetSpecs5108() (COWPublicationStageSpecV1, COWPublicationStageSpecV1) {
	return COWPublicationStageSpecV1{GenerationID: 2, CommitSeq: 2, CandidateID: candidateIDFromString("packet-visible"), AuxiliaryPageCount: 1}, COWPublicationStageSpecV1{GenerationID: 3, CommitSeq: 3, CandidateID: candidateIDFromString("packet-seal"), AuxiliaryPageCount: 1}
}
func preparePacket5108(t *testing.T, a *Allocator, request AllocationRequestCreditV1, scratch *AllocationCreatorV1) *PreparedCOWPublicationPacketV1 {
	t.Helper()
	visible, seal := packetSpecs5108()
	p, err := a.PrepareOwnedCOWPublicationPacketV1(request, scratch, visible, seal, nil, ReuseCapability{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func installPacketRole5108(t *testing.T, a *Allocator, p *PreparedCOWPublicationPacketV1, role *PreparedCOWCandidateV1) {
	t.Helper()
	if err := a.GrowCOWPublicationPacketPagerV1(p, role); err != nil {
		t.Fatal(err)
	}
	if err := role.WritePagesToPagerV1(a.pager); err != nil {
		t.Fatal(err)
	}
	if err := a.ActivateCOWCandidateV1(role); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedPublicationPacketNoPagerEffectsBeforeWAL5108(t *testing.T) {
	a, request, _, _, scratch := packetFixture5108(t)
	pages := a.pager.PageCount()
	id, err := a.ReserveCOWPageBeforePublicationV1(request, 0, true)
	if err != nil || id < pages || a.pager.PageCount() != pages {
		t.Fatalf("reserve id=%d pages=%d error=%v", id, a.pager.PageCount(), err)
	}
	p := preparePacket5108(t, a, request, scratch)
	if a.pager.PageCount() != pages {
		t.Fatal("private stage physically grew pager")
	}
	if p.visible.candidate.generation == p.seal.candidate.generation || p.seal.candidate.generation.parentGenerationID != p.visible.candidate.generation.generationID {
		t.Fatal("visible and seal authority fused")
	}
	r := a.cow.ledger.candidates.Value(p.visible.candidateID)
	found := false
	for _, reserved := range r.ids {
		found = found || reserved == id
	}
	if !found {
		t.Fatal("real staged COW ID absent from actual ledger coverage")
	}
	if err = a.GrowCOWPublicationPacketPagerV1(p, p.seal); !errors.Is(err, ErrCandidateConsumed) || a.pager.PageCount() != pages {
		t.Fatal("out-of-phase installer grew pager", err)
	}
	if _, err = a.ReserveCOWPageBeforePublicationV1(request, 0, true); !errors.Is(err, ErrCOWCandidatePrepared) || a.pager.PageCount() != pages {
		t.Fatal("packet barrier admitted new builder", err)
	}
	installPacketRole5108(t, a, p, p.visible)
	installPacketRole5108(t, a, p, p.seal)
	if err = a.PublishActivatedCOWPrefixV1(p.prefix, ReuseCapability{}); err != nil {
		t.Fatal(err)
	}
	if a.cow.packet != nil || len(a.cow.activated) != 0 || a.cow.ledger.candidates.Len() != 0 {
		t.Fatal("terminal prefix left packet debt")
	}
}

func TestPreparedPublicationPacketMissingAuthorityAndLimitsRefuse5108(t *testing.T) {
	for _, kind := range []string{"request", "scratch", "same-scratch", "finite", "visible-limit", "seal-limit", "escaped", "identity"} {
		t.Run(kind, func(t *testing.T) {
			a, request, resident, creator, scratch := packetFixture5108(t)
			visible, seal := packetSpecs5108()
			var borrowed AllocationRequestCreditV1 = request
			switch kind {
			case "request":
				borrowed = nil
			case "scratch":
				scratch = nil
			case "same-scratch":
				scratch = creator
			case "finite":
				visible.Limits = &COWPrepareLimitsV1{AllocationCredit: resident}
			case "visible-limit":
				visible.Limits = &COWPrepareLimitsV1{}
			case "seal-limit":
				seal.Limits = &COWPrepareLimitsV1{}
			case "escaped":
				a.rawWriterEscaped = true
			case "identity":
				if err := a.cow.ledger.reserve(request, visible.CandidateID, []uint64{2}, creator); err != nil {
					t.Fatal(err)
				}
			}
			beforeRequest, beforeResident, beforeRefs, pages := request.bytes, resident.bytes, creator.refs, a.pager.PageCount()
			txn := a.cow.txn
			beforeCandidates := a.cow.ledger.candidates.Len()
			packet, err := a.PrepareOwnedCOWPublicationPacketV1(borrowed, scratch, visible, seal, nil, ReuseCapability{})
			if packet != nil || err == nil || request.bytes != beforeRequest || resident.bytes != beforeResident || creator.refs != beforeRefs || a.cow.txn != txn || a.cow.prepared != nil || a.cow.packet != nil || a.pager.PageCount() != pages || a.cow.ledger.candidates.Len() != beforeCandidates {
				t.Fatal("refused packet changed effects/ownership", err)
			}
		})
	}
}

func TestPreparedPublicationPacketPrevisibleAbortBurnsBothAndRetries5108(t *testing.T) {
	a, request, resident, creator, scratch := packetFixture5108(t)
	original := a.cow.txn
	originalHeaderOwner := original.creator
	originalHighWater := original.highWater
	p := preparePacket5108(t, a, request, scratch)
	v, s := p.visible, p.seal
	tails := [2]reservationInterval{}
	for i, role := range [2]*PreparedCOWCandidateV1{v, s} {
		r := a.cow.ledger.candidates.Value(role.candidateID)
		if !r.tailWriteAttempted {
			t.Fatal("private emission lost conservative attempted tail")
		}
		tails[i] = reservationInterval{start: r.tailStart, count: r.tailCount}
	}
	if err := a.EndManagedResidentAllocationV1(creator, 1); err != nil {
		t.Fatal(err)
	}
	beforeRequest, beforeResident := request.bytes, resident.bytes
	if err := a.AbortCOWCandidateV1(v); err != nil {
		t.Fatal(err)
	}
	if request.bytes != beforeRequest || resident.bytes != beforeResident || a.cow.txn.highWater != originalHighWater || a.cow.txn.creator != originalHeaderOwner || a.cow.prepared != nil || a.cow.packet != nil {
		t.Fatal("abort borrowed/created/rebound old creator")
	}
	for _, tail := range tails {
		if !a.cow.ledger.Reserved(tail.start) || !a.cow.ledger.Reserved(tail.start+tail.count-1) {
			t.Fatal("abort released attempted tail")
		}
	}
	if err := v.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	nextResident, nextCreator := buildCreditLease5105(t)
	defer nextCreator.release()
	if _, err := a.AdmitManagedResidentAllocationV1(request, nextCreator, 2); err != nil {
		t.Fatal(err)
	}
	retry := preparePacket5108(t, a, request, scratch)
	r := a.cow.ledger.candidates.Value(retry.visible.candidateID)
	for _, tail := range tails {
		if r.tailStart < tail.start+tail.count {
			t.Fatal("retry overlapped attempted tail")
		}
	}
	if a.cow.txn.creator != nextCreator || retry.rollback.creator != originalHeaderOwner || resident.released != 0 || nextResident.bytes == 0 {
		t.Fatal("retry lost historical creating ownership")
	}
}

func TestPreparedPublicationPacketEndedEpochConsumesWithoutBirth5108(t *testing.T) {
	// AllocsPerRun performs one warmup and one measured consume. Both packets,
	// physical pager growth and immutable image writes occur outside measurement.
	type ready struct {
		a                             *Allocator
		p                             *PreparedCOWPublicationPacketV1
		request                       *componentBorrowedRequest5108
		resident                      *radixCredit5105
		beforeRequest, beforeResident uint64
	}
	var fixtures [2]ready
	for i := range fixtures {
		a, request, resident, creator, scratch := packetFixture5108(t)
		p := preparePacket5108(t, a, request, scratch)
		// Visible images are physically installed; both activations are measured.
		if err := a.GrowCOWPublicationPacketPagerV1(p, p.visible); err != nil {
			t.Fatal(err)
		}
		if err := p.visible.WritePagesToPagerV1(a.pager); err != nil {
			t.Fatal(err)
		}
		if err := a.EndManagedResidentAllocationV1(creator, 1); err != nil {
			t.Fatal(err)
		}
		for _, txn := range mutableCOWTransactionsV1(a.cow) {
			if txn != nil && (txn.buildCreator != nil || !txn.allocationRequired) {
				t.Fatal("packet role escaped epoch revocation")
			}
		}
		fixtures[i] = ready{a: a, p: p, request: request, resident: resident, beforeRequest: request.bytes, beforeResident: resident.bytes}
	}
	index := 0
	var consumeErr error
	visibleAllocations := testing.AllocsPerRun(1, func() { f := &fixtures[index]; index++; consumeErr = f.a.ActivateCOWCandidateV1(f.p.visible) })
	if consumeErr != nil || visibleAllocations != 0 {
		t.Fatalf("post-WAL visible allocation=%g err=%v", visibleAllocations, consumeErr)
	}
	for _, f := range fixtures {
		if err := f.a.GrowCOWPublicationPacketPagerV1(f.p, f.p.seal); err != nil {
			t.Fatal(err)
		}
		if err := f.p.seal.WritePagesToPagerV1(f.a.pager); err != nil {
			t.Fatal(err)
		}
	}
	index = 0
	allocations := testing.AllocsPerRun(1, func() {
		f := &fixtures[index]
		index++
		consumeErr = f.a.ActivateCOWCandidateV1(f.p.seal)
		if consumeErr == nil {
			consumeErr = f.a.PublishActivatedCOWPrefixV1(f.p.prefix, ReuseCapability{})
		}
	})
	if consumeErr != nil || allocations != 0 {
		t.Fatalf("post-WAL consumes allocation=%g err=%v", allocations, consumeErr)
	}
	for _, f := range fixtures {
		if f.request.bytes != f.beforeRequest || f.resident.bytes != f.beforeResident || f.a.cow.packet != nil {
			t.Fatal("consume debited retained request or birthed backing")
		}
		if _, err := f.a.ReserveCOWPageBeforePublicationV1(f.request, 0, true); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
			t.Fatal("ended epoch admitted future birth", err)
		}
	}
}

func TestPreparedPublicationPacketPrefixRefusalAtomicAndForwardOnly5108(t *testing.T) {
	a, request, _, _, scratch := packetFixture5108(t)
	p := preparePacket5108(t, a, request, scratch)
	v, s := p.visible, p.seal
	installPacketRole5108(t, a, p, v)
	if err := a.AbortCOWCandidateV1(v); !errors.Is(err, ErrCandidateConsumed) || a.cow.packet != p || !v.activated || a.cow.prepared != s {
		t.Fatal("post-visible abort discarded debt", err)
	}
	if err := v.ClearTerminalBackingV1(); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal("visible packet prematurely scrubbed", err)
	}
	installPacketRole5108(t, a, p, s)
	beforeOwners, beforeCandidates := a.cow.ledger.owners.Len(), a.cow.ledger.candidates.Len()
	a.cow.boundedPrune = true
	if _, err := a.PruneCOWBoundedStepV1WithAllocationRequestV1(request, scratch, RecoveryHorizon{OldestRecoverableCommitSeq: 4}.capability()); !errors.Is(err, ErrCOWCandidatePrepared) {
		t.Fatal("seal-visible packet lost prune barrier", err)
	}
	for _, bad := range [][]*PreparedCOWCandidateV1{{s, v}, {v, v}, {v}, {v, s, v}} {
		if err := a.PublishActivatedCOWPrefixV1(bad, ReuseCapability{}); err == nil || a.cow.ledger.owners.Len() != beforeOwners || a.cow.ledger.candidates.Len() != beforeCandidates || v.published || s.published {
			t.Fatal("bad prefix partially consumed", err)
		}
	}
	badCapability := RecoveryHorizon{OldestRecoverableCommitSeq: 4}.capability()
	if err := a.PublishActivatedCOWPrefixV1(p.prefix, badCapability); err == nil {
		t.Fatal("unprepared capability admitted")
	}
	proof := &p.coverage[0]
	oldCount := proof.tailCount
	proof.tailCount++
	if err := a.PublishActivatedCOWPrefixV1(p.prefix, ReuseCapability{}); err == nil || a.cow.ledger.owners.Len() != beforeOwners || a.cow.ledger.candidates.Len() != beforeCandidates {
		t.Fatal("coverage mismatch partially consumed", err)
	}
	proof.tailCount = oldCount
	if err := a.PublishActivatedCOWPrefixV1(p.prefix, ReuseCapability{}); err != nil {
		t.Fatal(err)
	}
	if v.packet != nil || s.packet != nil || p.expectedLiveTxn != nil || p.creator != nil || p.allocator.Load() != nil || p.prefix != nil || p.coverage != nil {
		t.Fatal("terminal packet retained outgoing aliases")
	}
	if err := v.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedPublicationPacketFullCensusReadmissionAndJoinedClose5108(t *testing.T) {
	a := managedTerminalAllocator5105(t)
	cut, err := a.AcquirePublishedGenerationLeaseV1(a.cow.generation.ref)
	if err != nil {
		t.Fatal(err)
	}
	resident, creator := buildCreditLease5105(t)
	_, scratch := buildCreditLease5105(t)
	defer scratch.release()
	request := &componentBorrowedRequest5108{}
	if _, err = a.AdmitManagedResidentAllocationV1(request, creator, 1); err != nil {
		t.Fatal(err)
	}
	p := preparePacket5108(t, a, request, scratch)
	v, s := p.visible, p.seal
	roles := mutableCOWTransactionsV1(a.cow)
	count := 0
	for _, txn := range roles {
		if txn != nil {
			count++
		}
	}
	if count != 7 {
		t.Fatalf("actual preborn mutable roles=%d want7", count)
	}
	if err = a.EndManagedResidentAllocationV1(creator, 1); err != nil {
		t.Fatal(err)
	}
	nextResident, nextCreator := buildCreditLease5105(t)
	if _, err = a.AdmitManagedResidentAllocationV1(request, nextCreator, 2); err != nil {
		t.Fatal(err)
	}
	for _, txn := range mutableCOWTransactionsV1(a.cow) {
		if txn != nil && (txn.buildCreator != nextCreator || txn.creator != creator) {
			t.Fatal("readmission missed role/rebound historical header")
		}
	}
	if p.creator != creator || v.creator != creator || s.creator != creator || cut.creator != creator || p.visibleNext.base != v.candidate.generation {
		t.Fatal("readmission rebound intrinsic future/cut")
	}
	if err = a.EndManagedResidentAllocationV1(nextCreator, 2); err != nil {
		t.Fatal(err)
	}
	installPacketRole5108(t, a, p, v)
	if a.TryCloseCOWOwnersV1() {
		t.Fatal("visible debt treated as terminal")
	}
	authority := a.writerAuthority
	a.CloseCOWOwnersAfterShutdownV1()
	a.DetachManagedIndexWriterV1(authority)
	creator.release()
	nextCreator.release()
	if nextResident.released != 1 || resident.released != 0 || p.allocator.Load() != nil || p.visible != nil || p.seal != nil || v.candidate != nil || s.candidate != nil {
		t.Fatal("joined Close retained packet graph or revoked cut")
	}
	if _, err = cut.SnapshotPageUnusedV1(2, 1); err != nil {
		t.Fatal("held old cut after Close", err)
	}
	cut.Close()
	if resident.released != 1 {
		t.Fatal("last actual cut did not release old creator")
	}
}

func TestPreparedPublicationPacketActualClassInventory5108(t *testing.T) {
	a, request, _, _, scratch := packetFixture5108(t)
	p := preparePacket5108(t, a, request, scratch)
	classes := p.retainedControlBytesV1()
	minimum := allocationClassV1(uint64(unsafe.Sizeof(*p)), true) + allocationClassV1(uint64(cap(p.prefix))*8, true) + allocationClassV1(uint64(cap(p.ids))*uint64(unsafe.Sizeof(CandidateIDV1{})), false) + allocationClassV1(uint64(cap(p.coverage))*uint64(unsafe.Sizeof(packetReservationProofV1{})), true)
	if classes < minimum || classes == ^uint64(0) {
		t.Fatal("packet omitted actual full retained capacities")
	}
	profile := a.ResidentGenerationProfileV1()
	if profile.TransactionCount != 7 || profile.CandidateCount != 2 || profile.RawGenerationEscaped || profile.RawLedgerEscaped || profile.RawWriterEscaped {
		t.Fatalf("packet census=%+v", profile)
	}
	t.Logf("packet raw=%d class=%d proof raw=%d class=%d prepared raw=%d class=%d COW raw=%d class=%d retainedControl=%d totalResident=%d", unsafe.Sizeof(*p), allocationClassV1(uint64(unsafe.Sizeof(*p)), true), unsafe.Sizeof(packetReservationProofV1{}), allocationClassV1(uint64(unsafe.Sizeof(packetReservationProofV1{})), true), unsafe.Sizeof(PreparedCOWCandidateV1{}), allocationClassV1(uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})), true), unsafe.Sizeof(allocatorCOWStateV1{}), allocationClassV1(uint64(unsafe.Sizeof(allocatorCOWStateV1{})), true), classes, profile.TotalBytes)
}

func TestPreparedPublicationPacketUncertaintyRetainsWholeFuture5108(t *testing.T) {
	a, request, resident, creator, scratch := packetFixture5108(t)
	p := preparePacket5108(t, a, request, scratch)
	v, s := p.visible, p.seal
	installPacketRole5108(t, a, p, v)
	uncertainty := errors.New("retained storage uncertainty")
	if err := a.FailCOWCandidateV1(v, uncertainty); err != nil {
		t.Fatal(err)
	}
	beforeRequest, beforeResident, profile, pages := request.bytes, resident.bytes, a.ResidentGenerationProfileV1(), a.pager.PageCount()
	if err := a.GrowCOWPublicationPacketPagerV1(p, s); !errors.Is(err, uncertainty) {
		t.Fatal("uncertainty admitted installer", err)
	}
	if err := a.ActivateCOWCandidateV1(s); !errors.Is(err, uncertainty) {
		t.Fatal("uncertainty admitted activation", err)
	}
	if err := a.AbortCOWCandidateV1(v); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal("uncertain visible debt discarded", err)
	}
	if request.bytes != beforeRequest || resident.bytes != beforeResident || a.ResidentGenerationProfileV1() != profile || a.pager.PageCount() != pages || p.creator != creator || a.TryCloseCOWOwnersV1() {
		t.Fatal("uncertainty lost complete retained future")
	}
	a.CloseCOWOwnersAfterShutdownV1()
	if p.allocator.Load() != nil || p.expectedLiveTxn != nil || p.creator != nil || v.candidate != nil || s.candidate != nil {
		t.Fatal("joined shutdown failed exact future scrub")
	}
}

func TestPreparedPublicationPacketPrevisibleStorageAttemptRetainsFuture5108(t *testing.T) {
	for _, boundary := range []string{"wal", "installer"} {
		t.Run(boundary, func(t *testing.T) {
			a, request, resident, _, scratch := packetFixture5108(t)
			p := preparePacket5108(t, a, request, scratch)
			v, s := p.visible, p.seal
			beforeRequest, beforeResident := request.bytes, resident.bytes
			if boundary == "wal" {
				if err := a.MarkCOWPublicationPacketStorageAttemptV1(p); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := a.GrowCOWPublicationPacketPagerV1(p, v); err != nil {
					t.Fatal(err)
				}
			}
			beforeTxn, owners, candidates := a.cow.txn, a.cow.ledger.owners.Len(), a.cow.ledger.candidates.Len()
			if err := a.AbortCOWCandidateV1(v); !errors.Is(err, ErrCandidateConsumed) || a.cow.txn != beforeTxn || a.cow.packet != p || a.cow.prepared != v || a.cow.ledger.owners.Len() != owners || a.cow.ledger.candidates.Len() != candidates || request.bytes != beforeRequest || resident.bytes != beforeResident {
				t.Fatal("post-attempt Abort discarded private authority", err)
			}
			// Exact forward retry remains usable; no recomputed candidate or request.
			installPacketRole5108(t, a, p, v)
			installPacketRole5108(t, a, p, s)
			if err := a.PublishActivatedCOWPrefixV1(p.prefix, ReuseCapability{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
