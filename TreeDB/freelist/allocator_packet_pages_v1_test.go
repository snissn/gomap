package freelist

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Synthetic component fixture with real reused and appended transaction IDs.
// It grants no installed native provenance, full caller fit or finite admission.
func packetImageFixture5108(t *testing.T) (*Allocator, *componentBorrowedRequest5108, *radixCredit5105, *AllocationCreatorV1, *AllocationCreatorV1) {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.Alloc(16); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	g, err := newFreelistGenerationOwnedV1(nil, 1, 16, []uint64{2, 3, 4, 5}, map[uint64]uint64{6: 1})
	if err != nil {
		t.Fatal(err)
	}
	err = a.enableCOWOwnedV1(nil, nil, g, nil)
	releaseGenerationV1(g)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.BindManagedIndexWriterV1(allocatorownership.NewManagedWriter()); err != nil {
		t.Fatal(err)
	}
	resident, creator := buildCreditLease5105(t)
	request := &componentBorrowedRequest5108{}
	_, scratch := buildCreditLease5105(t)
	if _, err = a.AdmitManagedResidentAllocationV1(request, creator, 1); err != nil {
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

type packetImageEffects5108 struct {
	pages, requestBytes, requestCalls, residentBytes uint64
	creatorRefs                                      uint64
	residentRetained, residentReleased               int
	stats                                            Stats
	phase                                            uint8
	storageAttempted                                 bool
	owners, candidates                               int
	txn                                              *FreelistTxn
}

func packetImageEffectsFor5108(a *Allocator, p *PreparedCOWPublicationPacketV1, request *componentBorrowedRequest5108, resident *radixCredit5105, creator *AllocationCreatorV1) packetImageEffects5108 {
	e := packetImageEffects5108{pages: a.pager.PageCount(), requestBytes: request.bytes, requestCalls: request.calls, residentBytes: resident.bytes, creatorRefs: creator.refs, residentRetained: resident.retained, residentReleased: resident.released, stats: a.stats, phase: p.phase, storageAttempted: p.storageAttempted}
	if a.cow != nil {
		e.txn = a.cow.txn
		e.owners, e.candidates = a.cow.ledger.owners.Len(), a.cow.ledger.candidates.Len()
	}
	return e
}

func packetImageIDs5108(t *testing.T, a *Allocator, request AllocationRequestCreditV1) []uint64 {
	t.Helper()
	physicalPages, highWater := a.pager.PageCount(), a.cow.txn.highWater
	allocated := len(a.cow.txn.allocated)
	reused, err := a.ReserveCOWPageBeforePublicationV1(request, 2, false)
	if err != nil {
		t.Fatalf("reused=%d error=%v", reused, err)
	}
	// The hint does not select a particular free-stack entry. Prove actual
	// reuse from the immutable base and transaction allocation record instead.
	if reused < 2 || reused >= physicalPages || !a.cow.txn.base.Allocatable(reused) || a.cow.txn.rootAllocatable(reused) || a.cow.txn.highWater != highWater || a.pager.PageCount() != physicalPages {
		t.Fatalf("page %d was not reserved from the existing free frontier", reused)
	}
	if len(a.cow.txn.allocated) != allocated+1 || a.cow.txn.allocated[allocated] != (allocatedPage{reused, ReservationReusedData}) {
		t.Fatal("reuse lacks exact transaction ownership record")
	}
	appended, err := a.ReserveCOWPageBeforePublicationV1(request, 0, true)
	if err != nil || appended != highWater {
		t.Fatalf("appended=%d highWater=%d error=%v", appended, highWater, err)
	}
	if a.cow.txn.highWater != highWater+1 || a.pager.PageCount() != physicalPages || len(a.cow.txn.allocated) != allocated+2 || a.cow.txn.allocated[allocated+1] != (allocatedPage{appended, ReservationAppendedData}) {
		t.Fatal("append lacks exact no-growth transaction ownership record")
	}
	return []uint64{reused, appended}
}

func TestPacketPageMembershipAcceptsRealReuseAppendAndEmpty5108(t *testing.T) {
	a, request, resident, creator, scratch := packetImageFixture5108(t)
	ids := packetImageIDs5108(t, a, request)
	p := preparePacket5108(t, a, request, scratch)
	before := packetImageEffectsFor5108(a, p, request, resident, creator)
	for _, input := range [][]uint64{ids, ids[:1], nil} {
		if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, input); err != nil {
			t.Fatal(err)
		}
	}
	if after := packetImageEffectsFor5108(a, p, request, resident, creator); after != before {
		t.Fatal("membership changed frontier, phase, credit or ownership")
	}
	// The initial physical frontier is still smaller than the immutable role.
	if a.pager.PageCount() != 16 || ids[1] >= p.visible.candidate.generation.highWater {
		t.Fatal("fixture lacks pre-growth image admission")
	}
	installPacketRole5108(t, a, p, p.visible)
	before = packetImageEffectsFor5108(a, p, request, resident, creator)
	if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.seal, nil); err != nil {
		t.Fatal(err)
	}
	sealReservation := a.cow.ledger.candidates.Value(p.seal.candidateID)
	if len(sealReservation.ids) == 0 {
		t.Fatal("fixture lacks actual seal-owned auxiliary data")
	}
	if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.seal, sealReservation.ids[:1]); err != nil {
		t.Fatal("exact current seal image refused", err)
	}
	if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.seal, ids); !errors.Is(err, ErrGenerationFormat) {
		t.Fatal("seal admitted visible member images", err)
	}
	if after := packetImageEffectsFor5108(a, p, request, resident, creator); after != before {
		t.Fatal("seal membership changed effects")
	}
}

func TestPacketPageMembershipRejectsUnownedTailHistoryAndOrder5108(t *testing.T) {
	a, request, resident, creator, scratch := packetImageFixture5108(t)
	ids := packetImageIDs5108(t, a, request)
	p := preparePacket5108(t, a, request, scratch)
	r := a.cow.ledger.candidates.Value(p.visible.candidateID)
	freeID := uint64(0)
	for id := uint64(2); id < 16; id++ {
		_, owned := a.cow.ledger.owners.Get(id)
		if !owned && p.visible.candidate.generation.Allocatable(id) {
			freeID = id
			break
		}
	}
	if freeID == 0 || r.tailStart >= p.visible.candidate.generation.highWater {
		t.Fatal("fixture lacks free page and metadata tail")
	}
	cases := []struct {
		name string
		ids  []uint64
	}{
		{"metadata-tail", []uint64{r.tailStart}},
		{"free", []uint64{freeID}},
		{"retired", []uint64{6}},
		{"historical", []uint64{7}},
		{"gap", []uint64{15}},
		{"reserved-header", []uint64{1}},
		{"highwater", []uint64{p.visible.candidate.generation.highWater}},
		{"overflow", []uint64{^uint64(0)}},
		{"duplicate", []uint64{ids[0], ids[0]}},
		{"unsorted", []uint64{ids[1], ids[0]}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := packetImageEffectsFor5108(a, p, request, resident, creator)
			if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, tc.ids); !errors.Is(err, ErrGenerationFormat) {
				t.Fatal(err)
			}
			if after := packetImageEffectsFor5108(a, p, request, resident, creator); after != before {
				t.Fatal("refusal changed effects")
			}
		})
	}
	if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, ids); err != nil {
		t.Fatal("refusal damaged valid retry", err)
	}
}

func TestPacketPageMembershipRejectsOtherVisiblePrefixOwner5108(t *testing.T) {
	a, request, resident, creator, scratch := packetImageFixture5108(t)
	oldID, err := a.ReserveCOWPageBeforePublicationV1(request, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	old, err := a.PrepareOwnedCOWCandidateRetiringWithAllocationRequestV1(request, scratch, 2, 2, candidateIDFromString("image-old-prefix"), ReuseCapability{}, nil, 0, NewOwnedCandidatePageSinkV1(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.pager.GrowTo(old.candidate.generation.highWater); err != nil {
		t.Fatal(err)
	}
	if err = old.WritePagesToPagerV1(a.pager); err != nil {
		t.Fatal(err)
	}
	if err = a.ActivateCOWCandidateV1(old); err != nil {
		t.Fatal(err)
	}
	newID, err := a.ReserveCOWPageBeforePublicationV1(request, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	visible, seal := packetSpecs5108()
	visible.GenerationID, visible.CommitSeq = 3, 3
	seal.GenerationID, seal.CommitSeq = 4, 4
	p, err := a.PrepareOwnedCOWPublicationPacketV1(request, scratch, visible, seal, []*PreparedCOWCandidateV1{old}, ReuseCapability{})
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := a.cow.ledger.owners.Get(oldID)
	if !ok || owner != old.candidateID || oldID >= p.visible.candidate.generation.highWater {
		t.Fatal("fixture lacks actual foreign prefix ownership")
	}
	before := packetImageEffectsFor5108(a, p, request, resident, creator)
	if err = a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, []uint64{oldID}); !errors.Is(err, ErrGenerationFormat) {
		t.Fatal("admitted other prefix data", err)
	}
	if err = a.ValidateCOWPublicationPacketPageIDsV1(p, old, []uint64{oldID}); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal("admitted old prefix role", err)
	}
	if err = a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, []uint64{newID}); err != nil {
		t.Fatal(err)
	}
	if after := packetImageEffectsFor5108(a, p, request, resident, creator); after != before {
		t.Fatal("foreign prefix checks changed effects")
	}
}

func TestPacketPageMembershipRejectsForeignStalePhaseAndUncertainty5108(t *testing.T) {
	for _, kind := range []string{"foreign-packet", "foreign-role", "seal-too-early", "wrong-transaction", "phase", "wait", "closed", "aborted"} {
		t.Run(kind, func(t *testing.T) {
			a, request, resident, creator, scratch := packetImageFixture5108(t)
			ids := packetImageIDs5108(t, a, request)
			p := preparePacket5108(t, a, request, scratch)
			packet, role := p, p.visible
			want := ErrCandidateConsumed
			switch kind {
			case "foreign-packet", "foreign-role":
				other, borrowed, _, _, transient := packetImageFixture5108(t)
				foreign := preparePacket5108(t, other, borrowed, transient)
				if kind == "foreign-packet" {
					packet = foreign
				} else {
					role = foreign.visible
				}
			case "seal-too-early":
				role = p.seal
			case "wrong-transaction":
				p.expectedLiveTxn = p.rollback
			case "phase":
				p.phase = 2
			case "wait":
				want = errors.New("retained image uncertainty")
				a.cow.waitErr = want
			case "closed":
				a.closed = true
			case "aborted":
				if err := a.AbortCOWCandidateV1(p.visible); err != nil {
					t.Fatal(err)
				}
			}
			before := packetImageEffectsFor5108(a, p, request, resident, creator)
			if err := a.ValidateCOWPublicationPacketPageIDsV1(packet, role, ids); !errors.Is(err, want) {
				t.Fatalf("error=%v want=%v", err, want)
			}
			if after := packetImageEffectsFor5108(a, p, request, resident, creator); after != before {
				t.Fatal("role refusal changed effects")
			}
		})
	}
	var a *Allocator
	if err := a.ValidateCOWPublicationPacketPageIDsV1(nil, nil, nil); !errors.Is(err, ErrGenerationFormat) {
		t.Fatal(err)
	}
}

func TestPacketPageMembershipRevalidatesWholeProofEvenForEmpty5108(t *testing.T) {
	for _, kind := range []string{"proof-ids", "reservation-ids", "tail", "owner", "extra-owner", "role-identity"} {
		t.Run(kind, func(t *testing.T) {
			a, request, resident, creator, scratch := packetImageFixture5108(t)
			ids := packetImageIDs5108(t, a, request)
			p := preparePacket5108(t, a, request, scratch)
			r := a.cow.ledger.candidates.Value(p.visible.candidateID)
			switch kind {
			case "proof-ids":
				p.coverage[0].ids[0]++
			case "reservation-ids":
				r.ids[0]++
			case "tail":
				r.tailCount++
			case "owner":
				if err := a.cow.ledger.owners.PutWithCredit(request, ids[0], p.seal.candidateID, creator); err != nil {
					t.Fatal(err)
				}
			case "extra-owner":
				if err := a.cow.ledger.owners.PutWithCredit(request, 15, p.visible.candidateID, creator); err != nil {
					t.Fatal(err)
				}
			case "role-identity":
				p.visible.candidateID = p.seal.candidateID
			}
			before := packetImageEffectsFor5108(a, p, request, resident, creator)
			if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, nil); err == nil {
				t.Fatal("empty image set bypassed full immutable proof")
			}
			if after := packetImageEffectsFor5108(a, p, request, resident, creator); after != before {
				t.Fatal("corruption refusal changed effects")
			}
		})
	}
}

func TestPacketPageMembershipEndedEpochAndZeroAllocation5108(t *testing.T) {
	a, request, resident, creator, scratch := packetImageFixture5108(t)
	ids := packetImageIDs5108(t, a, request)
	p := preparePacket5108(t, a, request, scratch)
	if err := a.EndManagedResidentAllocationV1(creator, 1); err != nil {
		t.Fatal(err)
	}
	before := packetImageEffectsFor5108(a, p, request, resident, creator)
	bad := []uint64{ids[0], ids[0]}
	var validErr, refusedErr error
	allocations := testing.AllocsPerRun(100, func() {
		validErr = a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, ids)
		refusedErr = a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, bad)
	})
	if allocations != 0 || validErr != nil || !errors.Is(refusedErr, ErrGenerationFormat) {
		t.Fatalf("allocations=%g valid=%v refusal=%v", allocations, validErr, refusedErr)
	}
	// The input remains caller-owned. Reusing it changes only subsequent proofs,
	// never the packet's preborn reservation vector or creator census.
	reusedID := ids[0]
	ids[0] = 7
	if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, ids); !errors.Is(err, ErrGenerationFormat) {
		t.Fatal(err)
	}
	ids[0] = reusedID
	if err := a.ValidateCOWPublicationPacketPageIDsV1(p, p.visible, ids); err != nil {
		t.Fatal(err)
	}
	if after := packetImageEffectsFor5108(a, p, request, resident, creator); after != before {
		t.Fatal("read-only proof borrowed retired request or changed phase/backing")
	}
	if p.coverage[0].ids[0] != reusedID || p.storageAttempted {
		t.Fatal("input retained or read-only validation crossed storage boundary")
	}
}
