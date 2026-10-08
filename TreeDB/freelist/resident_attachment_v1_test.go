package freelist

import (
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
)

func ownedPrepared5105(t *testing.T, a *Allocator, id string) *PreparedCOWCandidateV1 {
	t.Helper()
	p, err := a.PrepareOwnedCOWCandidateRetiringWithAllocationRequestV1(requestForAllocator5108(a), scratchForAllocator5108(a), 2, 2, candidateIDFromString(id), ReuseCapability{}, nil, 0, NewOwnedCandidatePageSinkV1(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestResidentAttachmentDeniedBeforeAnyCreator5105(t *testing.T) {
	a := managedTerminalAllocator5105(t)
	prepared := ownedPrepared5105(t, a, "resident-denied")
	account, creator := buildCreditLease5105(t)
	before, refs := account.bytes, creator.refs
	account.limit = before
	if _, err := a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 1); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
		t.Fatal(err)
	}
	if account.bytes != before || creator.refs != refs || a.cow.creator != nil || a.cow.txn.creator != nil || a.cow.txn.buildCreator != nil || prepared.creator != nil || prepared.candidate.creator != nil || a.cow.ledger.creator != nil || a.cow.residentAdmissionEpoch != 0 {
		t.Fatal("denied census partially attached or charged")
	}
	creator.release()
	authority := a.writerAuthority
	a.CloseCOWOwnersAfterShutdownV1()
	a.DetachManagedIndexWriterV1(authority)
	if account.released != 1 {
		t.Fatal("denied creator leaked")
	}
}

func TestResidentAttachmentManagedCandidatesCutsAndExactRetry5105(t *testing.T) {
	a := managedTerminalAllocator5105(t)
	cut, err := a.AcquirePublishedGenerationLeaseV1(a.cow.generation.ref)
	if err != nil {
		t.Fatal(err)
	}
	prepared := ownedPrepared5105(t, a, "resident-managed")
	account, creator := buildCreditLease5105(t)
	charged, err := a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 7)
	if err != nil || charged == 0 {
		t.Fatalf("bytes=%d error=%v", charged, err)
	}
	before, refs := account.bytes, creator.refs
	again, err := a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 7)
	if err != nil || again != charged || account.bytes != before || creator.refs != refs {
		t.Fatal("exact retry charged or retained again", err)
	}
	otherAccount, other := buildCreditLease5105(t)
	if _, err = a.admitResidentAllocationCreditV1(requestForCreator5108(other), other, 7); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal("facet mismatch admitted", err)
	}
	other.release()
	if otherAccount.released != 1 {
		t.Fatal("unadmitted other facet leaked")
	}
	if a.cow.creator != creator || a.cow.ledger.creator != creator || prepared.creator != creator || prepared.candidate.creator != creator || cut.creator != creator || a.cow.txn.buildCreator != creator {
		t.Fatal("actual resident owner missing")
	}
	if err = a.endResidentAllocationCreditV1(creator, 7); err != nil {
		t.Fatal(err)
	}
	if err = a.endResidentAllocationCreditV1(creator, 7); err != nil {
		t.Fatal("terminal retry", err)
	}
	if _, err = a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 7); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal("terminal epoch resurrected", err)
	}
	creator.release()
	// Ended rollback and successor roles cannot create isolation for a new
	// operation until the exact new epoch is admitted through the census.
	for _, role := range mutableCOWTransactionsV1(a.cow) {
		if role != nil && (!role.allocationRequired || role.buildCreator != nil) {
			t.Fatal("ended mutable role retained birth authority")
		}
	}
	if err = a.AbortCOWCandidateV1(prepared); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
		t.Fatal("ended epoch rollback admitted", err)
	}
	// The already prepared publication is refused before the constructor's
	// missing-credit branch. Preserve that ordering and prove zero new backing.
	cutProfile := a.ResidentGenerationProfileV1()
	cutHead, cutBytes, cutRefs := a.cow.generationLeases, account.bytes, creator.refs
	refusedCut, cutErr := a.AcquirePublishedGenerationLeaseV1(a.cow.generation.ref)
	if refusedCut != nil || !errors.Is(cutErr, ErrCOWCandidatePrepared) {
		t.Fatal("pending publication admitted ended-epoch cut", cutErr)
	}
	if a.cow.generationLeases != cutHead || a.ResidentGenerationProfileV1() != cutProfile || account.bytes != cutBytes || creator.refs != cutRefs {
		t.Fatal("pending cut refusal created backing, debit, or creator edge")
	}
	nextAccount, nextCreator := buildCreditLease5105(t)
	if _, err = a.admitResidentAllocationCreditV1(requestForCreator5108(nextCreator), nextCreator, 8); err != nil {
		t.Fatal(err)
	}
	if prepared.creator != creator || cut.creator != creator {
		t.Fatal("new epoch rebound old backing")
	}
	if err = a.AbortCOWCandidateV1WithAllocationRequestV1(requestForAllocator5108(a), prepared); err != nil {
		t.Fatal(err)
	}
	if err = prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	if prepared.candidate != nil || prepared.allocator != nil || a.cow.ownedCandidates != nil {
		t.Fatal("terminal prepared outgoing graph retained")
	}
	// Now that publication debt is gone, the actual creator-control refusal
	// branch is reachable. It must also refuse without new debit or backing.
	cutProfile = a.ResidentGenerationProfileV1()
	cutHead, cutBytes, cutRefs = a.cow.generationLeases, nextAccount.bytes, nextCreator.refs
	refusedCut, cutErr = a.AcquirePublishedGenerationLeaseV1(a.cow.generation.ref)
	if refusedCut != nil || !errors.Is(cutErr, ErrAllocationCertificateIncompleteV1) {
		t.Fatal("credited epoch admitted unowned cut control", cutErr)
	}
	if a.cow.generationLeases != cutHead || a.ResidentGenerationProfileV1() != cutProfile || nextAccount.bytes != cutBytes || nextCreator.refs != cutRefs {
		t.Fatal("credit refusal created backing, debit, or creator edge")
	}
	authority := a.writerAuthority
	a.CloseCOWOwnersAfterShutdownV1()
	a.DetachManagedIndexWriterV1(authority)
	nextCreator.release()
	if nextAccount.released != 1 {
		t.Fatal("new mutable epoch creator leaked")
	}
	if account.released != 0 {
		t.Fatal("held cut lost resident ownership")
	}
	if _, err = cut.SnapshotPageUnusedV1(2, 1); err != nil {
		t.Fatal("cut after allocator close", err)
	}
	cut.Close()
	if account.released != 1 {
		t.Fatal("last real cut did not release resident credit", creator.refs)
	}
}

func TestResidentAttachmentUnknownOwnersRefusedBeforeDebit5105(t *testing.T) {
	for _, route := range []string{"standalone", "writer-export", "generation", "ledger", "candidate", "direct-builder", "extra-generation", "extra-ledger"} {
		t.Run(route, func(t *testing.T) {
			a := managedTerminalAllocator5105(t)
			var unknown *FreelistGenerationV1
			var unknownTxn *FreelistTxn
			switch route {
			case "standalone":
				a.writerAuthority = nil
			case "writer-export":
				if !a.MarkOrdinaryWriterEscapeV1() {
					t.Fatal("ordinary export refused")
				}
			case "generation":
				_ = a.COWGenerationV1()
			case "ledger":
				a.cow.ledger.rawEscaped = true
			case "candidate":
				_, err := a.PrepareCOWCandidateRetiringWithLimitsV1(2, 2, candidateIDFromString(route), ReuseCapability{}, nil, 0, NewOwnedCandidatePageSinkV1(), nil)
				if err != nil {
					t.Fatal(err)
				}
			case "direct-builder":
				var err error
				unknownTxn, err = BeginCandidateV1(a.cow.generation, a.cow.generation.ref, nil)
				if err != nil {
					t.Fatal(err)
				}
			case "extra-generation":
				unknown = retainGenerationV1(a.cow.generation)
			case "extra-ledger":
				retainReservationLedgerV1(a.cow.ledger)
			}
			account, creator := buildCreditLease5105(t)
			before, refs := account.bytes, creator.refs
			if _, err := a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 1); !errors.Is(err, ErrFiniteAllocationExportV1) {
				t.Fatal(err)
			}
			if account.bytes != before || creator.refs != refs || a.cow.creator != nil {
				t.Fatal("unknown ownership charged or attached")
			}
			releaseTxnV1(unknownTxn)
			if unknown != nil {
				releaseGenerationV1(unknown)
			}
			if route == "extra-ledger" {
				releaseReservationLedgerV1(a.cow.ledger)
			}
			creator.release()
			authority := a.writerAuthority
			a.CloseCOWOwnersAfterShutdownV1()
			a.DetachManagedIndexWriterV1(authority)
		})
	}
}

func TestOwnedCandidatePublishedBackingLivesUntilTerminal5105(t *testing.T) {
	a := managedTerminalAllocator5105(t)
	prepared := ownedPrepared5105(t, a, "owned-published")
	info, err := prepared.InfoV1()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.pager.Truncate(info.HighWater()); err != nil {
		t.Fatal(err)
	}
	if err = prepared.WritePagesToPagerV1(a.pager); err != nil {
		t.Fatal(err)
	}
	if err = a.PublishCOWCandidateV1WithAllocationRequestV1(requestForAllocator5108(a), scratchForAllocator5108(a), prepared, ReuseCapability{}); err != nil {
		t.Fatal(err)
	}
	if a.cow.ownedCandidates != prepared || prepared.candidate == nil || len(prepared.candidate.pages) == 0 || a.TryCloseCOWOwnersV1() {
		t.Fatal("publication inferred physical terminal")
	}
	if err = prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	if a.cow.ownedCandidates != nil || prepared.candidate != nil || !a.TryCloseCOWOwnersV1() {
		t.Fatal("owned terminal failed")
	}
}

func TestResidentAttachmentCutReaderAndCloseSynchronization5105(t *testing.T) {
	a := managedTerminalAllocator5105(t)
	cut, err := a.AcquirePublishedGenerationLeaseV1(a.cow.generation.ref)
	if err != nil {
		t.Fatal(err)
	}
	account, creator := buildCreditLease5105(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_, _ = cut.SnapshotPageUnusedV1(2, 1)
		}
	}()
	if _, err = a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 1); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err = a.endResidentAllocationCreditV1(creator, 1); err != nil {
		t.Fatal(err)
	}
	creator.release()
	authority := a.writerAuthority
	wg.Add(2)
	go func() { defer wg.Done(); cut.Close() }()
	go func() {
		defer wg.Done()
		a.CloseCOWOwnersAfterShutdownV1()
		a.DetachManagedIndexWriterV1(authority)
	}()
	wg.Wait()
	if account.released != 1 {
		t.Fatal("concurrent last owners leaked", creator.refs)
	}
}

func TestResidentAllocationClassWitness5105(t *testing.T) {
	t.Logf("authority=%d/%d allocator=%d/%d state=%d/%d prepared=%d/%d ledger=%d/%d cut=%d/%d cond=%d/%d",
		unsafe.Sizeof(allocatorownership.ManagedWriter{}), allocationClassV1(uint64(unsafe.Sizeof(allocatorownership.ManagedWriter{})), false),
		unsafe.Sizeof(Allocator{}), allocationClassV1(uint64(unsafe.Sizeof(Allocator{})), true),
		unsafe.Sizeof(allocatorCOWStateV1{}), allocationClassV1(uint64(unsafe.Sizeof(allocatorCOWStateV1{})), true),
		unsafe.Sizeof(PreparedCOWCandidateV1{}), allocationClassV1(uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})), true),
		unsafe.Sizeof(ReservationLedger{}), allocationClassV1(uint64(unsafe.Sizeof(ReservationLedger{})), true),
		unsafe.Sizeof(PublishedGenerationLeaseV1{}), allocationClassV1(uint64(unsafe.Sizeof(PublishedGenerationLeaseV1{})), true),
		unsafe.Sizeof(sync.Cond{}), allocationClassV1(uint64(unsafe.Sizeof(sync.Cond{})), true))
}

func TestBurnedTailGrowthAdmissionBeforeReservationDeletion5105(t *testing.T) {
	ledger := newReservationLedgerOwnedV1()
	retainReservationLedgerV1(ledger)
	account, creator := buildCreditLease5105(t)
	id := candidateIDFromString("burned-growth")
	if err := ledger.reserve(requestForCreator5108(creator), id, []uint64{2}, creator); err != nil {
		t.Fatal(err)
	}
	r := ledger.candidates.Value(id)
	r.tailReserved, r.tailWriteAttempted, r.tailStart, r.tailCount = true, true, 4, 1
	before, refs := account.bytes, creator.refs
	account.limit = before
	if err := ledger.FailWithAllocationRequestV1(syntheticBorrowedRequest5108{}, id); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
		t.Fatal(err)
	}
	if !ledger.Reserved(2) || ledger.candidates.Value(id) != r || len(ledger.burnedTails) != 0 || creator.refs != refs || account.bytes != before {
		t.Fatal("growth denial partially deleted reservation")
	}
	account.limit = ^uint64(0)
	if err := ledger.FailWithAllocationRequestV1(syntheticBorrowedRequest5108{}, id); err != nil {
		t.Fatal(err)
	}
	creator.release()
	if account.released != 0 || ledger.burnedCreator == nil || !ledger.Reserved(4) {
		t.Fatal("deleted reservation released burned backing credit")
	}
	releaseReservationLedgerV1(ledger)
	if account.released != 1 || len(ledger.burnedTails) != 0 || ledger.burnedCreator != nil {
		t.Fatal("last ledger owner retained burned backing")
	}
}

func managedTerminalAllocator5105(t *testing.T) *Allocator {
	t.Helper()
	a := terminalAllocator5105(t, nil)
	if err := a.BindManagedIndexWriterV1(allocatorownership.NewManagedWriter()); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNumericRadixResidentPartialRetiredChunk5105(t *testing.T) {
	a, old := buildCreditLease5105(t)
	radix := newPageRadixV1[CandidateIDV1]()
	value := candidateIDFromString("resident-chunk")
	for _, key := range []uint64{1, 2, 3} {
		if err := radix.PutWithCredit(requestForCreator5108(old), key, value, old); err != nil {
			t.Fatal(err)
		}
	}
	old.release()
	radix.Delete(2)
	block := radix.head
	census := residentAttachmentV1{}
	residentRadixAttachmentV1(radix, &census)
	want := uint64(80) + numericRadixChunkClassV1[uint64, CandidateIDV1]() + 32
	if census.bytes != want || census.refs != 1 || radix.residentBytesV1() != want {
		t.Fatal("partial historical chunk or wrapper excluded", census.bytes, census.refs)
	}
	b, current := buildCreditLease5105(t)
	if err := current.reserve(requestForCreator5108(current), census.bytes, census.refs); err != nil {
		t.Fatal(err)
	}
	attach := residentAttachmentV1{creator: current, refs: census.refs, attach: true}
	residentRadixAttachmentV1(radix, &attach)
	if attach.refs != 0 || radix.credit != current || block.credit != old {
		t.Fatal("resident attachment replaced historic creator")
	}
	current.release()
	before := b.bytes
	if err := radix.PutWithCredit(requestForCreator5108(current), 4, value, current); err != nil {
		t.Fatal(err)
	}
	if b.bytes != before || a.released != 0 || block.credit != old {
		t.Fatal("new request reused whole chunk with another owner")
	}
	assertRadixChunks5105(t, radix)
	radix.Clear()
	if a.released != 1 || b.released != 1 || *block != (numericRadixChunkV1[uint64, CandidateIDV1]{}) {
		t.Fatal("whole chunk and header not independently scrubbed/released")
	}
}

func TestResidentChunkCloseDetachHeldOldReader5105(t *testing.T) {
	a := managedTerminalAllocator5105(t)
	if err := a.cow.ledger.reserve(nil, candidateIDFromString("whole-chunk-seed"), []uint64{2}); err != nil {
		t.Fatal(err)
	}
	cut, err := a.AcquirePublishedGenerationLeaseV1(a.cow.generation.ref)
	if err != nil {
		t.Fatal(err)
	}
	prepared := ownedPrepared5105(t, a, "whole-chunk-old-reader")
	account, creator := buildCreditLease5105(t)
	if _, err = a.admitResidentAllocationCreditV1(requestForCreator5108(creator), creator, 91); err != nil {
		t.Fatal(err)
	}
	ledger := a.cow.ledger
	ownerChunk, candidateChunk := ledger.owners.head, ledger.candidates.head
	if ownerChunk == nil || candidateChunk == nil || ownerChunk.credit != creator || candidateChunk.credit != creator {
		t.Fatal("actual ledger chunks absent from resident admission")
	}
	if err = a.endResidentAllocationCreditV1(creator, 91); err != nil {
		t.Fatal(err)
	}
	creator.release()
	if err = a.AbortCOWCandidateV1(prepared); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
		t.Fatal("ended epoch admitted isolation birth", err)
	}
	nextAccount, nextCreator := buildCreditLease5105(t)
	if _, err = a.admitResidentAllocationCreditV1(requestForCreator5108(nextCreator), nextCreator, 92); err != nil {
		t.Fatal("new epoch admission", err)
	}
	if cut.creator != creator || prepared.creator != creator || ownerChunk.credit != creator || candidateChunk.credit != creator {
		t.Fatal("new epoch rebound historical physical creators")
	}
	if err = a.AbortCOWCandidateV1WithAllocationRequestV1(requestForAllocator5108(a), prepared); err != nil {
		t.Fatal(err)
	}
	if err = prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	authority := a.writerAuthority
	a.CloseCOWOwnersAfterShutdownV1()
	a.DetachManagedIndexWriterV1(authority)
	nextCreator.release()
	if nextAccount.released != 1 {
		t.Fatal("new epoch creator retained after terminal close")
	}
	if account.released != 0 {
		t.Fatal("held old reader lost independent creating facet")
	}
	if *ownerChunk != (numericRadixChunkV1[uint64, CandidateIDV1]{}) || *candidateChunk != (numericRadixChunkV1[CandidateIDV1, *reservation]{}) {
		t.Fatal("terminal chunk backing retained aliases")
	}
	if _, err = cut.SnapshotPageUnusedV1(2, 1); err != nil {
		t.Fatal("old reader after writer detach", err)
	}
	cut.Close()
	if account.released != 1 {
		t.Fatal("last actual old reader did not release facet")
	}
}
