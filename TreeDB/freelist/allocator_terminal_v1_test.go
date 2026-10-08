package freelist

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func terminalAllocator5105(t *testing.T, ledger *ReservationLedger) *Allocator {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err = a.EnableNewCOWGenerationV1(1, 4, ledger); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAllocatorTerminalCutLastEdgeReleasesFiniteCredit5105(t *testing.T) {
	for _, debt := range []string{"healthy", "prepared", "activated", "ambiguous"} {
		t.Run(debt, func(t *testing.T) {
			a := terminalAllocator5105(t, nil)
			account := &radixCredit5105{limit: ^uint64(0)}
			creator, err := newAllocationCreditLeaseV1(account)
			if err != nil {
				t.Fatal(err)
			}
			root, err := cloneStateRefOwnedV1(stateRefV1{}, false, creator)
			if err != nil {
				t.Fatal(err)
			}
			creator.release()
			// This is an owned concrete generation, never a raw public export.
			g := a.cow.generation
			replaceStateNodeV1(&g.root, root)
			lease, err := a.AcquirePublishedGenerationLeaseV1(g.ref)
			if err != nil {
				t.Fatal(err)
			}
			prepared := &PreparedCOWCandidateV1{allocator: a, candidate: &FreelistCandidateV1{generation: retainGenerationV1(g)}}
			switch debt {
			case "prepared":
				a.cow.prepared = prepared
			case "activated":
				a.cow.activated = []*PreparedCOWCandidateV1{prepared}
				prepared.activated = true
			case "ambiguous":
				a.cow.prepared = prepared
				a.cow.waitErr = errors.New("ambiguous publication")
			default:
				releaseGenerationV1(prepared.candidate.generation)
			}
			if debt != "healthy" && a.TryCloseCOWOwnersV1() {
				t.Fatal("retirement treated publication/recovery debt as terminal")
			}
			a.CloseCOWOwnersAfterShutdownV1()
			a.CloseCOWOwnersAfterShutdownV1()
			if account.released != 0 || g.root.zero() {
				t.Fatal("allocator close revoked held physical generation edge")
			}
			if _, err = lease.SnapshotPageUnusedV1(2, 1); err != nil {
				t.Fatal(err)
			}
			if _, err = a.Alloc(0); !errors.Is(err, ErrCandidateConsumed) {
				t.Fatalf("closed allocation: %v", err)
			}
			lease.Close()
			lease.Close()
			if account.released != 1 || !g.root.zero() || root.creator() != nil {
				t.Fatal("last real cut edge did not scrub and release credit exactly once")
			}
		})
	}
}

func TestAllocatorTerminalSharedLedgerCreatingCredit5105(t *testing.T) {
	ledger := NewReservationLedger()
	first, second := terminalAllocator5105(t, ledger), terminalAllocator5105(t, ledger)
	account := &radixCredit5105{limit: ^uint64(0)}
	creator, err := newAllocationCreditLeaseV1(account)
	if err != nil {
		t.Fatal(err)
	}
	candidate := candidateIDFromString("shared-terminal")
	if err = ledger.reserve(candidate, []uint64{2}, creator); err != nil {
		t.Fatal(err)
	}
	creator.release()
	first.CloseCOWOwnersAfterShutdownV1()
	if account.released != 0 || !ledger.Reserved(2) {
		t.Fatal("one allocator closed another allocator's shared ledger backing")
	}
	second.CloseCOWOwnersAfterShutdownV1()
	if account.released != 1 || ledger.Reserved(2) || ledger.ownedRefs != 0 {
		t.Fatal("last managed ledger edge failed to release finite memberships")
	}
}

func TestAllocatorTerminalPreservesOrdinaryEscapedGenerationAndLedger5105(t *testing.T) {
	ledger := NewReservationLedger()
	a := terminalAllocator5105(t, ledger)
	g, err := a.PublishedSnapshotGenerationV1(a.cow.generation.ref)
	if err != nil {
		t.Fatal(err)
	}
	g.record.Extents = []ReservationExtentV1{{StartPageID: 2, Count: 1, Kind: ReservationAppendedData}}
	raw := g.ReservationRecord()
	candidate := candidateIDFromString("ordinary-terminal")
	if err = ledger.reserve(candidate, []uint64{2}); err != nil {
		t.Fatal(err)
	}
	before := g.root
	a.CloseCOWOwnersAfterShutdownV1()
	if g.root != before || len(g.ReservationRecord().Extents) != 1 || raw.Extents[0].StartPageID != 2 || !ledger.Reserved(2) {
		t.Fatal("close changed ordinary escaped generation/record/ledger aliases")
	}
	if err = g.Validate(); err != nil {
		t.Fatal(err)
	}
}
