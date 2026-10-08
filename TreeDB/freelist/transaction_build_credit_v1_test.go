package freelist

import (
	"errors"
	"testing"
	"unsafe"
)

func buildCreditTxn5105(t *testing.T, ledger *ReservationLedger) *FreelistTxn {
	t.Helper()
	base, err := newFreelistGenerationOwnedV1(nil, 1, 512, []uint64{2, 3}, nil)
	if err != nil {
		t.Fatal(err)
	}
	txn, err := beginCandidateOwnedV1(nil, nil, base, GenerationRefV1{}, ledger)
	releaseGenerationV1(base)
	if err != nil {
		t.Fatal(err)
	}
	return txn
}

func buildCreditLease5105(t *testing.T) (*radixCredit5105, *allocationCreditLeaseV1) {
	t.Helper()
	account := &radixCredit5105{limit: ^uint64(0)}
	creator, err := newComponentAllocationCreator5108(&componentBorrowedRequest5108{}, account)
	if err != nil {
		t.Fatal(err)
	}
	return account, creator
}

func TestTransactionHeaderAndCrossRequestBufferCreators5105(t *testing.T) {
	txn := buildCreditTxn5105(t, nil)
	a, header := buildCreditLease5105(t)
	if err := header.reserve(requestForCreator5108(header), allocationClassV1(uint64(unsafe.Sizeof(*txn)), true), 1); err != nil {
		t.Fatal(err)
	}
	txn.creator = header
	header.release()
	b, birth := buildCreditLease5105(t)
	if err := txn.bindBuildCreatorV1(birth); err != nil {
		t.Fatal(err)
	}
	birth.release()
	if err := txn.ledger.reserve(nil, candidateIDFromString("vector-gap"), []uint64{512}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.AllocateAppendWithAllocationRequestV1(requestForCreator5108(txn.buildCreator)); err != nil {
		t.Fatal(err)
	}
	if txn.abandonedCreator != birth || len(txn.abandonedAppends) != 1 {
		t.Fatal("abandoned buffer lost creating owner")
	}
	if txn.creator != header || txn.allocatedCreator != birth {
		t.Fatal("header owner overwritten or birth owner lost")
	}
	if err := txn.endBuildCreatorV1(birth); err != nil {
		t.Fatal(err)
	}
	if a.released != 0 || b.released != 0 || txn.privatePreparation {
		t.Fatal("terminal build edge discarded live header/vector credit")
	}
	c, next := buildCreditLease5105(t)
	if err := txn.bindBuildCreatorV1(next); err != nil {
		t.Fatal(err)
	}
	next.release()
	old, oldAbandoned := txn.allocated, txn.abandonedAppends
	if err := txn.growAppendVectorsV1(requestForCreator5108(txn.buildCreator), cap(old)+1, cap(oldAbandoned)+1); err != nil {
		t.Fatal(err)
	}
	if b.released != 1 || txn.allocatedCreator != next || txn.allocated[0].id != 513 || old[0] != (allocatedPage{}) || oldAbandoned[0] != (ReservationExtentV1{}) || txn.abandonedCreator != next {
		t.Fatal("new birth failed to scrub/drop old buffer before releasing its creating owner")
	}
	if a.released != 0 || c.released != 0 {
		t.Fatal("live header/new vector released")
	}
	if err := txn.endBuildCreatorV1(next); err != nil {
		t.Fatal(err)
	}
	if c.released != 0 {
		t.Fatal("new vector lost creating credit at build end")
	}
	releaseTxnV1(txn)
	if a.released != 1 || c.released != 1 {
		t.Fatal("last transaction edges failed to release header and vector")
	}
}

func TestTransactionBirthCloneAndRetainedNodeCredit5105(t *testing.T) {
	txn := buildCreditTxn5105(t, nil)
	b, birth := buildCreditLease5105(t)
	if err := txn.bindBuildCreatorV1(birth); err != nil {
		t.Fatal(err)
	}
	birth.release()
	if err := txn.ReservePageWithAllocationRequestV1(requestForCreator5108(txn.buildCreator), 2); err != nil {
		t.Fatal(err)
	}
	clone, err := txn.cloneForAllocatorPrepare(requestForCreator5108(txn.buildCreator))
	if err != nil {
		t.Fatal(err)
	}
	if clone.creator != birth || clone.buildCreator != birth || clone.allocatedCreator != birth {
		t.Fatal("clone header/build/vector were not born on current request")
	}
	if err := txn.endBuildCreatorV1(birth); err != nil {
		t.Fatal(err)
	}
	releaseTxnV1(txn)
	if b.released != 0 || !clone.rootAllocatable(3) {
		t.Fatal("shared node or clone lost creating owner")
	}
	if err := clone.endBuildCreatorV1(birth); err != nil {
		t.Fatal(err)
	}
	if b.released != 0 {
		t.Fatal("clone terminal released retained header/node/vector")
	}
	releaseTxnV1(clone)
	if b.released != 1 {
		t.Fatal("last clone failed to release creator")
	}
}

func TestTransactionBuildExactRetryAndRawLedgerRefusal5105(t *testing.T) {
	for _, raw := range []bool{false, true} {
		ledger := newReservationLedgerOwnedV1()
		if raw {
			ledger = NewReservationLedger()
		}
		txn := buildCreditTxn5105(t, ledger)
		account, birth := buildCreditLease5105(t)
		bytes, refs := account.bytes, birth.refs
		err := txn.bindBuildCreatorV1(birth)
		if raw {
			if !errors.Is(err, ErrFiniteAllocationExportV1) || txn.buildCreator != nil || birth.refs != refs || account.bytes != bytes {
				t.Fatal("raw-ledger admission did not refuse before state/debit")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			firstRefs := birth.refs
			if err := txn.bindBuildCreatorV1(birth); err != nil || birth.refs != firstRefs || account.bytes != bytes {
				t.Fatal("exact retry duplicated credit/debit")
			}
			_, other := buildCreditLease5105(t)
			if err := txn.bindBuildCreatorV1(other); !errors.Is(err, ErrCandidateConsumed) {
				t.Fatal("second active owner admitted")
			}
			if err := txn.endBuildCreatorV1(other); !errors.Is(err, ErrCandidateConsumed) {
				t.Fatal("wrong terminal owner released active build")
			}
			other.release()
			if err := txn.endBuildCreatorV1(birth); err != nil {
				t.Fatal(err)
			}
			if err := txn.endBuildCreatorV1(birth); err != nil {
				t.Fatal(err)
			}
		}
		releaseTxnV1(txn)
		birth.release()
		if account.released != 1 {
			t.Fatal("terminal/refused binding leaked facet")
		}
	}
}

func TestTransactionAppendGrowthDeniedBeforeMutation5105(t *testing.T) {
	txn := buildCreditTxn5105(t, nil)
	account, birth := buildCreditLease5105(t)
	if err := txn.bindBuildCreatorV1(birth); err != nil {
		t.Fatal(err)
	}
	birth.release()
	account.limit = account.bytes
	root, high := txn.root, txn.highWater
	if _, err := txn.AllocateAppendWithAllocationRequestV1(requestForCreator5108(txn.buildCreator)); err == nil {
		t.Fatal("growth admitted after credit exhaustion")
	}
	if txn.root != root || txn.highWater != high || len(txn.allocated) != 0 || txn.allocatedCreator != nil || len(txn.abandonedAppends) != 0 {
		t.Fatal("denied growth changed logical state or backing")
	}
	if err := txn.endBuildCreatorV1(birth); err != nil {
		t.Fatal(err)
	}
	releaseTxnV1(txn)
	if account.released != 1 {
		t.Fatal("denied growth leaked creator")
	}
}

func TestManagedRawCandidateEscapeRemainsPermanent5105(t *testing.T) {
	g, err := newFreelistGenerationOwnedV1(nil, 1, 4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := &Allocator{cow: &allocatorCOWStateV1{generation: g}}
	prepared := &PreparedCOWCandidateV1{allocator: a, candidate: &FreelistCandidateV1{generation: g}}
	if prepared.Candidate() == nil || !a.cow.rawGenerationEscaped || g.escaped == 0 {
		t.Fatal("raw candidate failed to taint allocator's whole managed lineage")
	}
	a.cow.prepared, a.cow.activated = nil, nil
	if !a.ResidentGenerationProfileV1().RawGenerationEscaped {
		t.Fatal("escape disappeared when old candidate history left ledger")
	}
}

func TestBuildOwnerPinnedClassWitness5105(t *testing.T) {
	if got := unsafe.Sizeof(FreelistTxn{}); got != 368 {
		t.Fatalf("transaction raw bytes=%d want368", got)
	}
	if got := allocationClassV1(uint64(unsafe.Sizeof(FreelistTxn{})), true); got != 384 {
		t.Fatalf("transaction class=%d want384", got)
	}
	t.Logf("pinned layouts txn=%d/class%d ledger=%d/class%d creator=%d/class%d", unsafe.Sizeof(FreelistTxn{}), allocationClassV1(uint64(unsafe.Sizeof(FreelistTxn{})), true), unsafe.Sizeof(ReservationLedger{}), allocationClassV1(uint64(unsafe.Sizeof(ReservationLedger{})), true), unsafe.Sizeof(allocationCreditLeaseV1{}), allocationClassV1(uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), true))
}

func TestOrdinaryPreparedWriterReentryAcrossTerminalClose5105(t *testing.T) {
	g, err := newFreelistGenerationOwnedV1(nil, 1, 4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := &Allocator{cow: &allocatorCOWStateV1{generation: retainGenerationV1(g)}}
	prepared := &PreparedCOWCandidateV1{allocator: a, candidate: &FreelistCandidateV1{generation: g, pages: []candidatePageV1{{PageID: 2, view: CandidatePageViewV1{data: make([]byte, 4096)}}}}}
	a.cow.prepared = prepared
	started, proceed := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- prepared.WritePagesToV1(loanWriter5105(func(_ uint64, _ CandidatePageViewV1) error {
			close(started)
			<-proceed
			if prepared.Candidate() == nil {
				return errors.New("ordinary terminal revoked retained callback generation")
			}
			return nil
		}))
	}()
	<-started
	// Must finish while callback still owns writeMu. The ordinary candidate is
	// retained indefinitely and its callback may reenter the allocator getter.
	a.CloseCOWOwnersAfterShutdownV1()
	close(proceed)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
