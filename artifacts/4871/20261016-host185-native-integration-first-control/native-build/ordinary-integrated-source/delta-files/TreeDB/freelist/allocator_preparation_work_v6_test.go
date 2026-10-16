package freelist

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
)

func newPrivatePrepareAllocatorV6(t *testing.T) *Allocator {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err := a.EnableCOWV1(MustNewFreelistGenerationV1(1, 4, []uint64{2}, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	if id, err := a.Alloc(0); err != nil || id != 2 {
		t.Fatal("allocate original page", id, err)
	}
	return a
}

func TestPrivateCOWPreparationUsesOriginalSlotOnlyAtAdoptionV6(t *testing.T) {
	a := newPrivatePrepareAllocatorV6(t)
	ordinary := newPrivatePrepareAllocatorV6(t)
	id := candidateIDFromString("private-prepare-identical")
	retirements := []COWRetirementV1{{PageIDs: []uint64{3}, LastReachableCommitSeq: 1}}
	want, err := ordinary.PrepareCOWCandidateRetiringV1(2, 2, id, ReuseCapability{}, retirements, 0, NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	var s COWPreparationWorkV6
	sink := NewCandidatePageSinkV1()
	var candidate *FreelistCandidateV1
	for calls := 0; calls < 512; calls++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		var ready bool
		candidate, ready, err = s.Step(a, 2, 2, id, ReuseCapability{}, retirements, sink, &w)
		if err != nil {
			t.Fatalf("phase=%d: %v", s.phase, err)
		}
		if w.Records > 32 || w.Bytes > 1<<20 || a.cow.prepared != nil || a.cow.txn.consumed {
			t.Fatal("private preparation escaped allowance or installed live slot")
		}
		if ready {
			break
		}
	}
	if candidate == nil || candidate.GenerationRef() != want.Candidate().GenerationRef() || len(candidate.pages) != len(want.Candidate().pages) {
		t.Fatal("private preparation changed canonical candidate")
	}
	for i, p := range candidate.pages {
		q := want.Candidate().pages[i]
		if p.PageID != q.PageID || !bytes.Equal(p.Data, q.Data) {
			t.Fatalf("different canonical page %d", i)
		}
	}
	construction := s.PreparedForConstructionV6()
	if construction == nil || construction.Candidate() != candidate || !construction.privatePrepared {
		t.Fatal("missing original private construction descriptor")
	}
	if _, err := a.ActivateCOWCandidateWithWorkV1(construction, nil, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatal("private construction descriptor authorized activation", err)
	}
	var activation COWActivationWorkV1
	for calls := 0; calls < 512; calls++ {
		ready, err := a.PrepareCOWActivationWithWorkV1(candidate, &activation, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	prepared, ready, err := s.Adopt(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil || a.cow.prepared != prepared || prepared.Candidate() != candidate || prepared != construction || prepared.privatePrepared {
		t.Fatal("exact original slot was not adopted", ready, err)
	}
	// The prepared slot cannot survive a public yield. This fixture immediately
	// aborts in the same call and proves the original transaction is restored.
	if err := a.AbortCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	chunk := lookupChunk(a.cow.txn.root, 0)
	if a.cow.prepared != nil || a.cow.txn.consumed || a.cow.txn.highWater != 4 || chunk != nil && chunk.retired[3] != 0 {
		t.Fatal("abort did not restore exact original transaction")
	}
}

func TestPrivateCOWPreparationCannotAdoptOverRealForegroundWriterV6(t *testing.T) {
	a := newPrivatePrepareAllocatorV6(t)
	var s COWPreparationWorkV6
	sink := NewCandidatePageSinkV1()
	id := candidateIDFromString("private-prepare-stale")
	var foreground uint64
	var ready bool
	for calls := 0; calls < 512; calls++ {
		var err error
		_, ready, err = s.Step(a, 2, 2, id, ReuseCapability{}, nil, sink, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if a.cow.prepared != nil {
			t.Fatal("yield installed original prepared slot")
		}
		if foreground == 0 && s.phase >= 5 {
			// This is the SAME live allocator, not a separately created model.
			foreground, err = a.Alloc(0)
			if err != nil {
				t.Fatal(err)
			}
		}
		if ready {
			break
		}
	}
	if !ready || foreground == 0 {
		t.Fatal("private candidate or actual writer stalled")
	}
	_, adopted, err := s.Adopt(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if adopted || !errors.Is(err, ErrCOWPreparationStaleV6) || a.cow.prepared != nil {
		t.Fatal("stale constructor installed over foreground writer", adopted, err)
	}
	for calls := 0; calls < 128; calls++ {
		ready, err = s.Cancel(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready || a.cow.txn.highWater != foreground+1 || a.cow.txn.consumed || a.cow.ledger.candidates[id] != nil {
		t.Fatal("cancel lost foreground allocation or original ledger receipt")
	}
}

func prepareAtomicPrivateCandidateV6(t *testing.T, a *Allocator, name string) (*COWPreparationWorkV6, *COWActivationWorkV1) {
	t.Helper()
	s := &COWPreparationWorkV6{}
	id, sink := candidateIDFromString(name), NewCandidatePageSinkV1()
	var candidate *FreelistCandidateV1
	for calls := 0; calls < 512; calls++ {
		var ready bool
		var err error
		candidate, ready, err = s.Step(a, 2, 2, id, ReuseCapability{}, nil, sink, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if a.cow.prepared != nil {
			t.Fatal("private preparation installed allocator slot")
		}
		if ready {
			break
		}
	}
	if candidate == nil {
		t.Fatal("private candidate did not complete")
	}
	activation := &COWActivationWorkV1{}
	for calls := 0; calls < 512; calls++ {
		ready, err := a.PrepareCOWActivationWithWorkV1(candidate, activation, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			return s, activation
		}
	}
	t.Fatal("private successor did not complete")
	return nil, nil
}

func TestPrivateCOWAtomicAdoptionActivationRefusesBeforeSlotOrLedgerEffectsV6(t *testing.T) {
	a := newPrivatePrepareAllocatorV6(t)
	s, activation := prepareAtomicPrivateCandidateV6(t, a, "atomic-private")
	original, generation, stats := a.cow.txn, a.cow.generation, a.stats
	prepared := s.PreparedForConstructionV6()
	claim := a.cow.ledger.candidates[s.candidateID]
	if claim == nil || claim.state != CandidatePreVisible {
		t.Fatal("missing original previsible claim")
	}
	got, ready, err := s.AdoptAndActivateV6(activation, &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20})
	if got != nil || ready || err == nil {
		t.Fatal("undersized cut accepted", ready, err)
	}
	if a.cow.prepared != nil || a.cow.txn != original || a.cow.generation != generation || a.stats != stats ||
		claim.state != CandidatePreVisible || !prepared.privatePrepared || s.adopted || !activation.ready {
		t.Fatal("refusal changed original allocator or ledger custody")
	}
	got, ready, err = s.AdoptAndActivateV6(activation, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil || got != prepared || !s.adopted || a.cow.prepared != nil ||
		a.cow.generation != prepared.Candidate().Generation() || a.cow.txn == original ||
		claim.state != CandidateVisible || !prepared.activated || prepared.privatePrepared || len(a.cow.activated) != 1 || a.cow.activated[0] != prepared {
		t.Fatal("atomic cut did not activate exact original candidate", ready, err)
	}
	if activation.ready || activation.successor != nil || activation.allocator != nil {
		t.Fatal("successor duplicate custody survived activation")
	}
	if _, ready, err := s.AdoptAndActivateV6(activation, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || err == nil {
		t.Fatal("atomic cut replay accepted", ready, err)
	}
}

func TestPrivateCOWAtomicAdoptionCannotReplaceForegroundTxnV6(t *testing.T) {
	a := newPrivatePrepareAllocatorV6(t)
	s, activation := prepareAtomicPrivateCandidateV6(t, a, "atomic-private-stale")
	foreground, err := a.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	original, stats := a.cow.txn, a.stats
	_, ready, err := s.AdoptAndActivateV6(activation, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if ready || !errors.Is(err, ErrCOWPreparationStaleV6) || a.cow.prepared != nil || a.cow.txn != original || a.stats != stats ||
		a.cow.txn.highWater != foreground+1 || a.cow.ledger.candidates[s.candidateID].state != CandidatePreVisible {
		t.Fatal("stale atomic cut changed foreground transaction", ready, err)
	}
	for calls := 0; calls < 128; calls++ {
		ready, err = s.Cancel(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready || a.cow.txn != original || a.cow.txn.highWater != foreground+1 || a.cow.ledger.candidates[candidateIDFromString("atomic-private-stale")] != nil {
		t.Fatal("original private cancellation lost foreground custody")
	}
}

func newPrivateMetadataPlacementAllocatorV6(t *testing.T, reuse bool) *Allocator {
	if !reuse {
		return newPrivatePrepareAllocatorV6(t)
	}
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := p.Alloc(128); err != nil {
		t.Fatal(err)
	}
	free := []uint64{2}
	for id := uint64(16); id < 64; id++ {
		free = append(free, id)
	}
	a := New(p, 0)
	if err := a.EnableCOWV1(MustNewFreelistGenerationV1(1, 128, free, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	if id, err := a.Alloc(0); id == 0 || err != nil {
		t.Fatal(id, err)
	}
	return a
}

func TestPrivateCOWMetadataClaimLeavesSharedDATAWithOriginalTxnV6(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "reuse"}[reuse], func(t *testing.T) {
			a := newPrivateMetadataPlacementAllocatorV6(t, reuse)
			s, activation := prepareAtomicPrivateCandidateV6(t, a, "metadata-only-native")
			ledger := a.cow.ledger
			claim := ledger.candidates[s.candidateID]
			dataID := s.private.allocated[0].id
			if claim == nil || claim.reusedMetadata != reuse || len(claim.ids) != 0 || ledger.Reserved(dataID) {
				t.Fatal("private metadata captured shared DATA", claim)
			}
			foreground, err := a.Alloc(0)
			if err != nil {
				t.Fatal(err)
			}
			id := candidateIDFromString("ordinary-while-private")
			ordinary, err := a.PrepareCOWCandidateV1(2, 2, id, ReuseCapability{}, 0, NewCandidatePageSinkV1())
			if err != nil {
				t.Fatal("ordinary prepare blocked by private candidate", err)
			}
			materializeAllocatorCOWCandidateForTest(t, a.pager, ordinary)
			if err := a.ActivateCOWCandidateV1(ordinary); err != nil {
				t.Fatal(err)
			}
			if err := a.PublishActivatedCOWPrefixV1([]*PreparedCOWCandidateV1{ordinary}, ReuseCapability{}); err != nil {
				t.Fatal(err)
			}
			current := a.cow.generation
			if _, ready, err := s.AdoptAndActivateV6(activation, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(err, ErrCOWPreparationStaleV6) {
				t.Fatal("stale private claim accepted", ready, err)
			}
			done := false
			for calls := 0; calls < 512; calls++ {
				var err error
				done, err = s.Cancel(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done || a.cow.generation != current || a.cow.prepared != nil || current.Allocatable(dataID) || current.Allocatable(foreground) || ledger.candidates[candidateIDFromString("metadata-only-native")] != nil {
				t.Fatal("private cancel damaged newer ordinary allocation")
			}
		})
	}
}

func TestPrivateCOWLateDATAClaimConflictHasNoPartialEffectsV6(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "reuse"}[reuse], func(t *testing.T) {
			a := newPrivateMetadataPlacementAllocatorV6(t, reuse)
			s, activation := prepareAtomicPrivateCandidateV6(t, a, "late-data-conflict")
			ledger := a.cow.ledger
			dataID := s.private.allocated[0].id
			foreign := candidateIDFromString("foreign-original-reservation")
			if err := ledger.reserve(foreign, []uint64{dataID}); err != nil {
				t.Fatal(err)
			}
			original, stats := a.cow.txn, a.stats
			claim := ledger.candidates[s.candidateID]
			work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			_, ready, err := s.AdoptAndActivateV6(activation, &work)
			t.Logf("late DATA claim refusal work=%d/%d", work.Records, work.Bytes)
			if ready || !errors.Is(err, ErrPageReserved) || a.cow.prepared != nil || a.cow.txn != original || a.stats != stats || s.adopted || claim.state != CandidatePreVisible || len(claim.ids) != 0 || len(ledger.owners) != 1 || ledger.owners[dataID] != foreign {
				t.Fatal("late conflict partially claimed or adopted", ready, err)
			}
			done := false
			for calls := 0; calls < 512; calls++ {
				done, err = s.Cancel(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done || ledger.candidates[foreign] == nil || ledger.owners[dataID] != foreign || ledger.candidates[candidateIDFromString("late-data-conflict")] != nil {
				t.Fatal("native cancellation consumed foreign claim")
			}
			if err := ledger.RollbackPreVisible(foreign); err != nil {
				t.Fatal(err)
			}
		})
	}
}
