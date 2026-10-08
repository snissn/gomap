package freelist

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
	"unsafe"
)

type materializationCredit5108 struct {
	radixCredit5105
	reject uint64
}

func (credit *materializationCredit5108) ReserveAllocation(bytes uint64) error {
	if credit.reject != 0 && bytes == credit.reject {
		return ErrAllocationCertificateIncompleteV1
	}
	return credit.radixCredit5105.ReserveAllocation(bytes)
}

type recordingCountSink5108 struct{ writes int }

func (s *recordingCountSink5108) WritePage(_ uint64, _ []byte) error { s.writes++; return nil }

// Owned test setup uses the same private constructor as allocator installation;
// public constructors deliberately mark escape and cannot acquire finite credit.
func materializationOwnedBase5108(t *testing.T, highWater uint64, free []uint64, retired map[uint64]uint64) *FreelistGenerationV1 {
	t.Helper()
	base, err := newFreelistGenerationOwnedV1(1, highWater, free, retired)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseGenerationV1(base) })
	return base
}

func materializationGrowPager5108(t *testing.T, a *Allocator, prepared *PreparedCOWCandidateV1) {
	t.Helper()
	info, err := prepared.InfoV1()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.pager.GrowTo(info.HighWater()); err != nil {
		t.Fatal(err)
	}
}

func TestMaterializationBackingPredebitsAndKeepsCreator5108(t *testing.T) {
	// Synthetic test-only facets exercise the callback/lifetime contract on both
	// test platforms; this does not bypass the production Linux creator guard.
	t.Run("output-refusal-before-first-write", func(t *testing.T) {
		ledger := NewReservationLedger()
		base := MustNewFreelistGenerationV1(1, 300, []uint64{2, 256}, nil)
		txn := NewFreelistTxn(base, ledger)
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}, reject: allocationClassV1(5*uint64(unsafe.Sizeof(candidatePageV1{})), true)}
		txn.buildCreator = &allocationCreditLeaseV1{facet: credit, refs: 1}
		sink := NewOwnedCandidatePageSinkV1()
		id := candidateIDFromString("5108-output-denial")
		candidate, err := txn.materializeCandidateOwnedV1(2, 2, id, sink)
		if candidate != nil || !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
			t.Fatalf("candidate=%v err=%v", candidate, err)
		}
		reservation := ledger.candidates.Value(id)
		if reservation == nil || reservation.tailWriteAttempted {
			t.Fatal("output refusal crossed first write")
		}
		if err := ledger.RollbackPreVisible(id); err != nil {
			t.Fatal(err)
		}
		if credit.released != 0 {
			t.Fatal("refusal prematurely released creator")
		}
		releaseTxnV1(txn)
		if credit.released != 1 {
			t.Fatalf("terminal creator releases=%d", credit.released)
		}
	})
	t.Run("each-plan-whole-class", func(t *testing.T) {
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		if err := reserveMaterializationObjectsV1(creator, 3, uint64(unsafe.Sizeof(indexPagePlanV1{})), false); err != nil {
			t.Fatal(err)
		}
		if credit.bytes != 3*640 || creator.refs != 1 {
			t.Fatalf("whole-plan debit=%d refs=%d", credit.bytes, creator.refs)
		}
		before := credit.bytes
		if err := reserveMaterializationBackingV1(creator, ^uint64(0), 8, false); !errors.Is(err, ErrNoAllocatablePage) || credit.bytes != before {
			t.Fatalf("overflow debit=%d err=%v", credit.bytes, err)
		}
		creator.release()
		if credit.released != 1 {
			t.Fatal("scratch introduced a retained alias")
		}
	})
	t.Run("successor-current-creator-and-header-refusal", func(t *testing.T) {
		base := materializationOwnedBase5108(t, 300, []uint64{2, 256}, nil)
		ledger := newReservationLedgerOwnedV1()
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}, reject: allocationClassV1(uint64(unsafe.Sizeof(FreelistTxn{})), true)}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		root := base.root
		txn, err := beginCandidateOwnedV1(base, base.ref, ledger, creator)
		if txn != nil || !errors.Is(err, ErrAllocationCertificateIncompleteV1) || creator.refs != 1 || ledger.ownedRefs != 0 || base.root != root {
			t.Fatalf("denied successor changed owner graph: txn=%v err=%v refs=%d ledger=%d", txn, err, creator.refs, ledger.ownedRefs)
		}
		credit.reject = 0
		txn, err = beginCandidateOwnedV1(base, base.ref, ledger, creator)
		if err != nil {
			t.Fatal(err)
		}
		if txn.creator != creator || txn.buildCreator != creator || txn.changedChunks.credit != creator || txn.replacedMetadata.credit != creator {
			t.Fatal("successor lost exact current birth owner")
		}
		if err = txn.endBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		if txn.creator != creator || txn.buildCreator != nil || txn.privatePreparation {
			t.Fatal("epoch exit removed header owner or left private authority")
		}
		creator.release()
		if credit.released != 0 {
			t.Fatal("epoch/root release ignored retained successor")
		}
		releaseTxnV1(txn)
		if credit.released != 1 {
			t.Fatalf("successor terminal releases=%d", credit.released)
		}
	})
	t.Run("escaped-base-and-ledger-refuse-before-birth", func(t *testing.T) {
		for _, kind := range []string{"base", "ledger"} {
			base := materializationOwnedBase5108(t, 300, []uint64{2, 256}, nil)
			ledger := newReservationLedgerOwnedV1()
			if kind == "base" {
				base.markOrdinaryEscapeV1()
			} else {
				ledger.rawEscaped = true
			}
			credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
			creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
			root := base.root
			txn, err := beginCandidateOwnedV1(base, base.ref, ledger, creator)
			if txn != nil || !errors.Is(err, ErrFiniteAllocationExportV1) || credit.bytes != 0 || creator.refs != 1 || ledger.ownedRefs != 0 || base.root != root {
				t.Fatal(kind, "escaped graph acquired birth authority", err)
			}
			txn, err = beginCandidateOwnedV1(base, base.ref, ledger)
			if err != nil {
				t.Fatal(err)
			}
			if err = txn.bindBuildCreatorV1(creator); !errors.Is(err, ErrFiniteAllocationExportV1) || credit.bytes != 0 || txn.buildCreator != nil || creator.refs != 1 {
				t.Fatal(kind, "escaped builder acquired birth authority", err)
			}
			releaseTxnV1(txn)
			creator.release()
			if credit.released != 1 {
				t.Fatal(kind, "refused creator was retained")
			}
		}
	})
	t.Run("bounded-prune-planning-refusal-is-read-only", func(t *testing.T) {
		base := materializationOwnedBase5108(t, 300, nil, map[uint64]uint64{2: 2, 3: 2})
		txn, err := beginCandidateOwnedV1(base, base.ref, newReservationLedgerOwnedV1())
		if err != nil {
			t.Fatal(err)
		}
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}, reject: allocationClassV1(uint64(unsafe.Sizeof(boundedPrunePlanV1{})), true)}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		if err = txn.bindBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		root, stats, cursor := txn.root, txn.stats, txn.pruneCursor
		cap := RecoveryHorizon{OldestRecoverableCommitSeq: 3}.capability()
		_, err = txn.prepareBoundedPruneV1(cap)
		if !errors.Is(err, ErrAllocationCertificateIncompleteV1) || txn.root != root || txn.stats != stats || txn.pruneCursor != cursor || txn.allocationErr != nil {
			t.Fatal("planning refusal changed live state", err)
		}
		credit.reject = 0
		plan, err := txn.prepareBoundedPruneV1(cap)
		if err != nil {
			t.Fatal(err)
		}
		if txn.root != root || txn.stats != stats || txn.pruneCursor != cursor || plan.work.PromotedPages != 2 {
			t.Fatal("admission modified live prune state")
		}
		before := credit.bytes
		credit.limit = before // application must not invoke a facet after admission
		work, err := txn.applyBoundedPruneV1(&plan)
		plan.operation.close()
		if err != nil || work.PromotedPages != 2 || txn.root.freeCount() != 2 || txn.root.retiredCount() != 0 || credit.bytes != before {
			t.Fatal("receipt application allocated or lost promotion", err)
		}
		if err = txn.endBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		creator.release()
		releaseTxnV1(txn)
		if credit.released != 1 {
			t.Fatal("prune receipt retained a phantom owner")
		}
	})
	t.Run("unbounded-prune-and-reserve-scratch-refusals", func(t *testing.T) {
		retired := map[uint64]uint64{2: 2, 3: 2, 4: 2}
		base := materializationOwnedBase5108(t, 300, nil, retired)
		txn, err := beginCandidateOwnedV1(base, base.ref, newReservationLedgerOwnedV1())
		if err != nil {
			t.Fatal(err)
		}
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}, reject: allocationClassV1(3*uint64(unsafe.Sizeof(retiredPage{})), false)}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		if err = txn.bindBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		root, stats := txn.root, txn.stats
		txn.PruneWithCapability(RecoveryHorizon{OldestRecoverableCommitSeq: 3}.capability())
		if !errors.Is(txn.allocationErr, ErrAllocationCertificateIncompleteV1) || txn.root != root || txn.stats != stats {
			t.Fatal("legacy scratch refusal crossed traversal or mutation")
		}
		txn.allocationErr = nil
		credit.reject = 0
		if _, err = txn.AllocateAppend(); err != nil {
			t.Fatal(err)
		}
		id := candidateIDFromString("5108-reserve-scratch-denial")
		credit.reject = allocationClassV1(8, false)
		if err = txn.Reserve(id); !errors.Is(err, ErrAllocationCertificateIncompleteV1) || txn.ledger.candidates.Value(id) != nil {
			t.Fatal("reserve scratch refusal changed ledger", err)
		}
		if err = txn.endBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		creator.release()
		releaseTxnV1(txn)
		if credit.released != 1 {
			t.Fatal("scratch introduced a retained edge")
		}
	})
	t.Run("capacity-overflow-before-facet", func(t *testing.T) {
		ledger := newReservationLedgerOwnedV1()
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		_, err := ledger.admitReservationV1(candidateIDFromString("5108-bad-cap"), nil, int(^uint(0)>>1), 0, 0, creator)
		if !errors.Is(err, ErrNoAllocatablePage) || credit.bytes != 0 || creator.refs != 1 {
			t.Fatal("overflow reached allocation facet", err)
		}
		_, err = ledger.admitReservationV1(candidateIDFromString("5108-negative-cap"), nil, -1, 0, 0, creator)
		if !errors.Is(err, ErrNoAllocatablePage) || credit.bytes != 0 {
			t.Fatal("negative capacity reached allocation facet", err)
		}
		creator.release()
	})

	for _, route := range []string{"activate", "direct-publish"} {
		t.Run(route+"-birth-refusal-before-ledger", func(t *testing.T) {
			a := terminalAllocator5105(t, nil)
			credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
			creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
			if err := a.cow.txn.bindBuildCreatorV1(creator); err != nil {
				t.Fatal(err)
			}
			prepared := ownedPrepared5105(t, a, "5108-current-"+route)
			if route == "direct-publish" {
				materializationGrowPager5108(t, a, prepared)
				if err := prepared.WritePagesToPagerV1(a.pager); err != nil {
					t.Fatal(err)
				}
			}
			stage := a.cow.txn
			credit.reject = allocationClassV1(uint64(unsafe.Sizeof(FreelistTxn{})), true)
			invoke := func() error {
				if route == "activate" {
					return a.ActivateCOWCandidateV1(prepared)
				}
				return a.PublishCOWCandidateV1(prepared, ReuseCapability{})
			}
			if err := invoke(); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
				t.Fatal("successor birth was not refused", err)
			}
			reservation := a.cow.ledger.candidates.Value(prepared.candidateID)
			if reservation == nil || reservation.state != CandidatePreVisible || a.cow.txn != stage || a.cow.prepared != prepared || prepared.activated || prepared.published {
				t.Fatal("birth refusal crossed shared ledger/allocator visibility")
			}
			credit.reject = 0
			if err := invoke(); err != nil {
				t.Fatal("exact candidate retry", err)
			}
			if a.cow.txn.creator != creator || a.cow.txn.buildCreator != creator {
				t.Fatal("successor used a historical or absent birth owner")
			}
			if err := a.cow.txn.endBuildCreatorV1(creator); err != nil {
				t.Fatal(err)
			}
			creator.release()
			a.CloseCOWOwnersAfterShutdownV1()
			if credit.released != 1 {
				t.Fatalf("terminal creator releases=%d refs=%d", credit.released, creator.refs)
			}
		})
	}
	t.Run("activated-prefix-scratch-before-publish", func(t *testing.T) {
		a := terminalAllocator5105(t, nil)
		a.EnableBoundedPruneV1()
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		if err := a.cow.txn.bindBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		prepared := ownedPrepared5105(t, a, "5108-prefix-scratch")
		materializationGrowPager5108(t, a, prepared)
		if err := prepared.WritePagesToPagerV1(a.pager); err != nil {
			t.Fatal(err)
		}
		if err := a.ActivateCOWCandidateV1(prepared); err != nil {
			t.Fatal(err)
		}
		credit.reject = allocationClassV1(uint64(unsafe.Sizeof(CandidateIDV1{})), false)
		if err := a.PublishActivatedCOWThroughV1(prepared, ReuseCapability{}); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
			t.Fatal("prefix scratch was not refused", err)
		}
		reservation := a.cow.ledger.candidates.Value(prepared.candidateID)
		if reservation == nil || reservation.state != CandidateVisible || prepared.published || len(a.cow.activated) != 1 {
			t.Fatal("scratch refusal crossed PublishBatch")
		}
		credit.reject = 0
		if err := a.PublishActivatedCOWThroughV1(prepared, ReuseCapability{}); err != nil {
			t.Fatal("exact prefix retry", err)
		}
		if err := a.cow.txn.endBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		creator.release()
		a.CloseCOWOwnersAfterShutdownV1()
		if credit.released != 1 {
			t.Fatal("prefix terminal lost creator")
		}
	})
	t.Run("abort-isolation-refusal-preserves-exact-retry", func(t *testing.T) {
		a := terminalAllocator5105(t, nil)
		a.cow.txn.Retire(2, 1)
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		if err := a.cow.txn.bindBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		prepared := ownedPrepared5105(t, a, "5108-abort-isolation")
		stage, rollback := a.cow.txn, prepared.rollbackTxn
		root := rollback.root
		credit.reject = stateChunkCopyCapacityV1
		if err := a.AbortCOWCandidateV1(prepared); !errors.Is(err, ErrAllocationCertificateIncompleteV1) {
			t.Fatal("abort isolation was not refused", err)
		}
		if a.cow.txn != stage || prepared.rollbackTxn != rollback || rollback.root != root || a.cow.prepared != prepared || a.cow.ledger.candidates.Value(prepared.candidateID) == nil {
			t.Fatal("abort refusal lost exact candidate/rollback owner")
		}
		credit.reject = 0
		if err := a.AbortCOWCandidateV1(prepared); err != nil {
			t.Fatal("exact abort retry", err)
		}
		if a.cow.txn != rollback || a.cow.txn.root == root || a.cow.txn.buildCreator != creator {
			t.Fatal("abort did not install isolated current-owner rollback")
		}
		if err := prepared.ClearTerminalBackingV1(); err != nil {
			t.Fatal(err)
		}
		if err := a.cow.txn.endBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		creator.release()
		a.CloseCOWOwnersAfterShutdownV1()
		if credit.released != 1 {
			t.Fatal("abort terminal leaked creator")
		}
	})

	t.Run("exact-owned-sink-refuses-ordinary-and-custom", func(t *testing.T) {
		for _, sink := range []AppendPageSink{NewCandidatePageSinkV1(), &recordingCountSink5108{}} {
			base := materializationOwnedBase5108(t, 300, []uint64{2, 256}, nil)
			ledger := newReservationLedgerOwnedV1()
			txn, err := beginCandidateOwnedV1(base, base.ref, ledger)
			if err != nil {
				t.Fatal(err)
			}
			credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
			creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
			if err = txn.bindBuildCreatorV1(creator); err != nil {
				t.Fatal(err)
			}
			before, root := credit.bytes, txn.root
			id := candidateIDFromString("5108-unowned-sink")
			candidate, err := txn.materializeCandidateOwnedV1(2, 2, id, sink)
			if candidate != nil || !errors.Is(err, ErrFiniteAllocationExportV1) || credit.bytes != before || txn.consumed || txn.root != root || ledger.candidates.Value(id) != nil {
				t.Fatal("wrong sink crossed admission/consumption", err)
			}
			if err = txn.endBuildCreatorV1(creator); err != nil {
				t.Fatal(err)
			}
			creator.release()
			releaseTxnV1(txn)
			if credit.released != 1 {
				t.Fatal("refused sink retained creator")
			}
		}
		sink := NewCandidatePageSinkV1()
		data := make([]byte, page.PageSize)
		if err := sink.WritePage(5, data); err != nil {
			t.Fatal(err)
		}
		if err := sink.WritePage(5, data); !errors.Is(err, ErrGenerationFormat) {
			t.Fatal("ordinary duplicate contract changed", err)
		}
		if unsafe.Sizeof(ownedCandidatePageSinkV1{}) != 0 {
			t.Fatal("owned sink retains control/directory")
		}
	})
	t.Run("scalar-ids-refused-before-callback-or-tail-mark", func(t *testing.T) {
		for _, kind := range []string{"wrong", "skipped", "overflow", "full"} {
			ledger := NewReservationLedger()
			id := candidateIDFromString("5108-scalar-" + kind)
			start, count, err := ledger.reserveTail(id, 300, 1, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			sink := &recordingCountSink5108{}
			recorded := recordingSink{sink: sink, pages: make([]candidatePageV1, 0, count), metadataStart: start, reservedCount: count, ledger: ledger, candidate: id}
			bad := start - 1
			if kind == "skipped" {
				bad = start + 1
			}
			if kind == "overflow" {
				recorded.metadataStart = ^uint64(0)
				recorded.reservedCount = 2
				bad = ^uint64(0)
			}
			if kind == "full" {
				recorded.pages = recorded.pages[:cap(recorded.pages)]
				bad = start + count
			}
			if err = recorded.write(bad, make([]byte, page.PageSize)); !errors.Is(err, ErrGenerationFormat) || sink.writes != 0 || ledger.candidates.Value(id).tailWriteAttempted {
				t.Fatal(kind, "crossed callback/tail mark", err)
			}
			if err = ledger.RollbackPreVisible(id); err != nil {
				t.Fatal(err)
			}
		}
		sink := &recordingCountSink5108{}
		recorded := recordingSink{sink: sink, pages: make([]candidatePageV1, 0, 2), metadataStart: 300, reservedCount: 2}
		data := make([]byte, page.PageSize)
		if err := recorded.write(300, data); err != nil {
			t.Fatal(err)
		}
		if err := recorded.write(300, data); !errors.Is(err, ErrGenerationFormat) || sink.writes != 1 {
			t.Fatal("repeated ID reached callback", err)
		}
		if err := recorded.complete(); !errors.Is(err, ErrGenerationFormat) {
			t.Fatal("short interval exposed candidate", err)
		}
		if err := recorded.write(301, data); err != nil {
			t.Fatal(err)
		}
		if err := recorded.complete(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("append-and-reuse-match-exact-reserved-interval", func(t *testing.T) {
		for _, reuse := range []bool{false, true} {
			var free []uint64
			if reuse {
				for id := uint64(2); id < 200; id++ {
					free = append(free, id)
				}
			}
			base := materializationOwnedBase5108(t, 512, free, nil)
			ledger := newReservationLedgerOwnedV1()
			txn, err := beginCandidateOwnedV1(base, base.ref, ledger)
			if err != nil {
				t.Fatal(err)
			}
			credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
			creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
			if err = txn.bindBuildCreatorV1(creator); err != nil {
				t.Fatal(err)
			}
			id := candidateIDFromString("5108-owned-append")
			if reuse {
				id = candidateIDFromString("5108-owned-reuse")
			}
			candidate, err := txn.materializeCandidateOwnedV1(2, 2, id, NewOwnedCandidatePageSinkV1())
			if err != nil {
				t.Fatal(err)
			}
			reservation := ledger.candidates.Value(id)
			if reservation == nil || reservation.reusedMetadata != reuse || uint64(len(candidate.pages)) != reservation.tailCount {
				t.Fatal("emitter/reservation agreement", reuse)
			}
			for i, p := range candidate.pages {
				if p.PageID != reservation.tailStart+uint64(i) {
					t.Fatal("noncontiguous candidate", reuse, i, p.PageID)
				}
			}
			if reuse && candidate.generation.highWater != 512 {
				t.Fatal("reuse extended high-water")
			}
			if err = txn.endBuildCreatorV1(creator); err != nil {
				t.Fatal(err)
			}
			prepared := &PreparedCOWCandidateV1{candidate: candidate, ownedBacking: true}
			if err = prepared.ClearTerminalBackingV1(); err != nil {
				t.Fatal(err)
			}
			creator.release()
			releaseTxnV1(txn)
			if credit.released != 1 {
				t.Fatal("append/reuse creator retained after terminal ownership")
			}
		}
	})
	t.Run("skipped-tail-coverage-paid-before-ledger", func(t *testing.T) {
		ledger := newReservationLedgerOwnedV1()
		blocker := candidateIDFromString("5108-block-tail")
		if err := ledger.reserve(blocker, []uint64{300}); err != nil {
			t.Fatal(err)
		}
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		id := candidateIDFromString("5108-skipped-tail")
		// Exact reservation/control/chunk classes plus the newly required interval.
		plan, err := ledger.admitReservationV1(id, nil, 0, 0, 1, creator)
		if err != nil {
			t.Fatal(err)
		}
		bytes := plan.operation.bytes
		plan.operation.close()
		credit.reject = bytes
		if _, _, err = ledger.reserveTail(id, 300, 1, nil, nil, creator); !errors.Is(err, ErrAllocationCertificateIncompleteV1) || ledger.candidates.Value(id) != nil {
			t.Fatal("coverage refusal crossed reservation", err)
		}
		credit.reject = 0
		start, _, err := ledger.reserveTail(id, 300, 1, nil, nil, creator)
		if err != nil {
			t.Fatal(err)
		}
		r := ledger.candidates.Value(id)
		if start != 301 || len(r.abandonedCoverage) != 1 || cap(r.abandonedCoverage) != 1 || r.coverageCreator != creator || r.abandonedCoverage[0] != (reservationInterval{start: 300, count: 1}) {
			t.Fatal("skipped prefix lacks exact paid backing")
		}
		if err = ledger.RollbackPreVisible(id); err != nil {
			t.Fatal(err)
		}
		creator.release()
		if credit.released != 1 {
			t.Fatal("retired coverage retained creator")
		}
	})

	t.Run("owned-partial-output-refusal-restores-backup-and-retries", func(t *testing.T) {
		a := terminalAllocator5105(t, nil)
		credit := &materializationCredit5108{radixCredit5105: radixCredit5105{limit: ^uint64(0), retained: 1}}
		creator := &allocationCreditLeaseV1{facet: credit, refs: 1}
		if err := a.cow.txn.bindBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		before := a.cow.txn.highWater
		credit.reject = allocationClassV1(uint64(unsafe.Sizeof(FreelistCandidateV1{})), true)
		id := candidateIDFromString("5108-owned-output-retry")
		invoke := func() (*PreparedCOWCandidateV1, error) {
			return a.PrepareOwnedCOWCandidateRetiringWithLimitsV1(2, 2, id, ReuseCapability{}, nil, 0, NewOwnedCandidatePageSinkV1(), nil)
		}
		prepared, err := invoke()
		if prepared != nil || !errors.Is(err, ErrAllocationCertificateIncompleteV1) || a.cow.prepared != nil || a.cow.txn.consumed || a.cow.txn.highWater != before || a.cow.ledger.candidates.Value(id) != nil || len(a.cow.ledger.burnedTails) != 1 {
			t.Fatal("partial owned output refusal lost rollback/burn contract", err)
		}
		burned := a.cow.ledger.burnedTails[0]
		credit.reject = 0
		prepared, err = invoke()
		if err != nil {
			t.Fatal("exact owned sink retry", err)
		}
		r := a.cow.ledger.candidates.Value(id)
		if r == nil || r.tailStart < burned.start+burned.count || r.tailCount != uint64(len(prepared.candidate.pages)) {
			t.Fatal("retry collided with attempted tail")
		}
		if err = a.AbortCOWCandidateV1(prepared); err != nil {
			t.Fatal(err)
		}
		if err = prepared.ClearTerminalBackingV1(); err != nil {
			t.Fatal(err)
		}
		if err = a.cow.txn.endBuildCreatorV1(creator); err != nil {
			t.Fatal(err)
		}
		creator.release()
		a.CloseCOWOwnersAfterShutdownV1()
		if credit.released != 1 {
			t.Fatalf("partial/retry creator released=%d refs=%d", credit.released, creator.refs)
		}
	})

}
