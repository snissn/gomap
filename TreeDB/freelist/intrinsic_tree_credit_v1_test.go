package freelist

import (
	"errors"
	"testing"
)

func TestIntrinsicTreeCreatingCreditOutlivesTransferredParent5105(t *testing.T) {
	a, b := &radixCredit5105{limit: ^uint64(0)}, &radixCredit5105{limit: ^uint64(0)}
	first, err := newAllocationCreditLeaseV1(a)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := cloneStateChunkOwnedV1(nil, false, first)
	if err != nil {
		t.Fatal(err)
	}
	chunk.setFree(2, true)
	leaf, err := cloneStateBranchOwnedV1(nil, false, first)
	if err != nil {
		t.Fatal(err)
	}
	chunk.chunkNo = 0
	refreshChunkSummaryV1(chunk)
	other, err := cloneStateChunkOwnedV1(nil, false, first)
	if err != nil {
		t.Fatal(err)
	}
	other.chunkNo = 1
	other.setFree(2, true)
	refreshChunkSummaryV1(other)
	leaf.depth = 13
	leaf.child[0] = stateRefV1{chunk: chunk}
	leaf.child[1] = stateRefV1{chunk: other}
	recomputeStateNode(leaf, 13)
	first.release()
	second, err := newAllocationCreditLeaseV1(b)
	if err != nil {
		t.Fatal(err)
	}
	copied, err := cloneStateBranchOwnedV1(leaf, false, second)
	if err != nil {
		t.Fatal(err)
	}
	second.release()
	releaseStateNodeV1(stateRefV1{branch: leaf})
	if a.released != 0 || copied.child[0].chunk != chunk || !chunk.isFree(2) {
		t.Fatal("old creating credit released while descendant alias survived")
	}
	g := &FreelistGenerationV1{ownedRefs: 1, root: stateRefV1{branch: copied}}
	if !g.hasFiniteBackingV1() {
		t.Fatal("ordinary successor lost transitive finite origin")
	}
	g.markOrdinaryEscapeV1()
	if g.escaped != 0 {
		t.Fatal("finite descendant escaped through ordinary generation")
	}
	retained := retainGenerationV1(g)
	releaseGenerationV1(g)
	if a.released != 0 || b.released != 0 {
		t.Fatal("physical generation lease failed to retain backing")
	}
	releaseGenerationV1(retained)
	if a.released != 1 || b.released != 1 || chunk.creator != nil || !copied.child[0].zero() || !leaf.child[0].zero() {
		t.Fatal("last edges failed to scrub/release both creating credits")
	}
}

func TestIntrinsicTreeBirthDeniedBeforeAliasOrMutation5105(t *testing.T) {
	account := &radixCredit5105{limit: ^uint64(0)}
	creator, err := newAllocationCreditLeaseV1(account)
	if err != nil {
		t.Fatal(err)
	}
	original, err := cloneStateBranchOwnedV1(nil, false, creator)
	if err != nil {
		t.Fatal(err)
	}
	original.freePages = 23
	account.limit = account.bytes
	refs := original.ownedRefs
	if copied, err := cloneStateBranchOwnedV1(original, false, creator); err == nil || copied != nil {
		t.Fatal("denied node birth succeeded")
	}
	if original.freePages != 23 || original.ownedRefs != refs {
		t.Fatal("denial changed original or published an alias")
	}
	finite := &FreelistGenerationV1{ownedRefs: 1, root: stateRefV1{branch: original}}
	if _, err = BeginCandidateV1(finite, GenerationRefV1{}, nil); !errors.Is(err, ErrFiniteAllocationExportV1) {
		t.Fatal("finite raw builder accepted")
	}
	if finite.ReservationRecord().Extents != nil {
		t.Fatal("finite record escaped")
	}
	releaseGenerationV1(finite)
	creator.release()
	if account.released != 1 {
		t.Fatal("denied birth leaked creator retention")
	}
}

func TestMetadataFixedOverlayMatchesNormalizedReference5105(t *testing.T) {
	base := []ReservationExtentV1{{StartPageID: 2, Count: 3, Kind: ReservationAppendedData}, {StartPageID: 20, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 9}}
	overlay := []ReservationExtentV1{{StartPageID: 19, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 9}, {StartPageID: 22, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 9}}
	count, err := mergeReservationPlanV1(base, overlay, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]ReservationExtentV1, count)
	if _, err = mergeReservationPlanV1(base, overlay, out); err != nil {
		t.Fatal(err)
	}
	want, err := normalizeExtents(append(append([]ReservationExtentV1(nil), base...), overlay...))
	if err != nil {
		t.Fatal(err)
	}
	if len(want) != len(out) {
		t.Fatalf("count %d != %d", len(out), len(want))
	}
	for i := range want {
		if want[i] != out[i] {
			t.Fatalf("extent %d changed", i)
		}
	}
	if base[1].Count != 2 {
		t.Fatal("sizing/filling mutated borrowed base scratch")
	}
}

type metadataAtomicCredit5105 struct {
	radixCredit5105
	ledger     *ReservationLedger
	calls      int
	afterClaim bool
}

func (c *metadataAtomicCredit5105) ReserveAllocation(bytes uint64) error {
	c.calls++
	if c.ledger != nil && c.ledger.candidates.Len() != 0 {
		c.afterClaim = true
	}
	return c.radixCredit5105.ReserveAllocation(bytes)
}
func TestReusedMetadataJointAdmissionBeforeClaim5105(t *testing.T) {
	for _, deny := range []bool{true, false} {
		t.Run(map[bool]string{true: "deny", false: "admit"}[deny], func(t *testing.T) {
			free := make([]uint64, 98)
			for i := range free {
				free[i] = uint64(i + 2)
			}
			base, err := newFreelistGenerationOwnedV1(1, 512, free, nil)
			if err != nil {
				t.Fatal(err)
			}
			ledger := newReservationLedgerOwnedV1()
			txn, err := beginCandidateOwnedV1(base, GenerationRefV1{}, ledger)
			if err != nil {
				t.Fatal(err)
			}
			releaseGenerationV1(base)
			account := &metadataAtomicCredit5105{radixCredit5105: radixCredit5105{limit: ^uint64(0)}, ledger: ledger}
			creator, err := newAllocationCreditLeaseV1(account)
			if err != nil {
				t.Fatal(err)
			}
			txn.buildCreator = creator
			defer releaseTxnV1(txn)
			root, count := txn.root, txn.root.freeCount()
			beforeCalls := account.calls
			if deny {
				account.limit = account.bytes
			}
			_, _, _, ok := txn.tryReusedMetadata(candidateIDFromString("joint-admission"))
			if account.calls != beforeCalls+1 || account.afterClaim {
				t.Fatalf("calls=%d afterClaim=%v", account.calls-beforeCalls, account.afterClaim)
			}
			if deny {
				if ok || txn.allocationErr == nil || ledger.candidates.Len() != 0 || ledger.owners.Len() != 0 || txn.root != root || txn.root.freeCount() != count {
					t.Fatal("denied admission changed tree/ledger authority")
				}
			}
			if !deny {
				if !ok || txn.allocationErr != nil || ledger.candidates.Len() != 1 || txn.root.freeCount() >= count {
					t.Fatal("admitted operation failed")
				}
				if err = ledger.Abandon(candidateIDFromString("joint-admission")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
