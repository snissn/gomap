package freelist

import (
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/pager"
)

// These are actual structural-copy counters, not modeled loop counts. The
// persistent side reproduces the repeated copies on a retained dirty path.
func TestPrivatePreparationCopiesDirtyPathOnce5105(t *testing.T) {
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 1024, []uint64{2, 9, 33}, nil), 2, NewMemoryPageStoreV1())
	makeTxn := func(private bool) *FreelistTxn {
		live := NewFreelistTxn(base, NewReservationLedger())
		live.Retire(50, 2)
		var staged *FreelistTxn
		var err error
		if private {
			staged, err = live.cloneForPrivateAllocatorPrepare()
		} else {
			staged, err = live.cloneForAllocatorPrepare()
		}
		if err != nil {
			t.Fatal(err)
		}
		return staged
	}
	reference, private := makeTxn(false), makeTxn(true)
	beforeReference, beforePrivate := reference.Stats(), private.Stats()
	for i := uint64(0); i < 24; i++ {
		reference.Retire(50+i, 2)
		private.Retire(50+i, 2)
	}
	r, p := reference.Stats(), private.Stats()
	t.Logf("actual copies: persistent nodes=%d chunks=%d bytes=%d; private isolation nodes=%d chunks=%d bytes=%d visits=%d; repeated private copies nodes=%d chunks=%d", r.StateNodeCopies-beforeReference.StateNodeCopies, r.StateChunkCopies-beforeReference.StateChunkCopies, r.StateCopyBytes-beforeReference.StateCopyBytes, beforePrivate.StateNodeCopies-beforeReference.StateNodeCopies, beforePrivate.StateChunkCopies-beforeReference.StateChunkCopies, beforePrivate.StateCopyBytes-beforeReference.StateCopyBytes, beforePrivate.StateIsolationVisits-beforeReference.StateIsolationVisits, p.StateNodeCopies-beforePrivate.StateNodeCopies, p.StateChunkCopies-beforePrivate.StateChunkCopies)
	if r.StateNodeCopies-beforeReference.StateNodeCopies != 24*(chunkTrieDepth+1) || r.StateChunkCopies-beforeReference.StateChunkCopies != 24 {
		t.Fatal("persistent reference did not reproduce repeated path/chunk copies")
	}
	if p.StateNodeCopies != beforePrivate.StateNodeCopies || p.StateChunkCopies != beforePrivate.StateChunkCopies {
		t.Fatal("already isolated dirty path copied again")
	}
	if p.StateMutationPaths-beforePrivate.StateMutationPaths != 24 || p.StateMutationItems-beforePrivate.StateMutationItems != 24 {
		t.Fatal("private editing omitted logical mutation work")
	}
	if !maps.Equal(snapshotPrivateState5105(t, reference), snapshotPrivateState5105(t, private)) {
		t.Fatal("private mutation changed logical state")
	}
}

func snapshotPrivateState5105(t *testing.T, txn *FreelistTxn) map[uint64]stateChunk {
	t.Helper()
	result := make(map[uint64]stateChunk)
	if err := walkState(txn.root, 0, func(c *stateChunk) error { result[c.chunkNo] = *c; return nil }); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPrivatePreparationCapacityAndNoTreeOwner5105(t *testing.T) {
	// The frozen e52d transaction had this same pointer/slice/map prefix and
	// 21 uint64 statistics. Keep the structural capacity delta explicit:
	// ownership adds no per-node field and the new counters add exactly 32 B.
	type previousTxnLayout struct {
		base             *FreelistGenerationV1
		ledger           *ReservationLedger
		root             *stateNode
		highWater        uint64
		allocated        []allocatedPage
		abandonedAppends []ReservationExtentV1
		consumed         bool
		changedChunks    map[uint64]struct{}
		replacedMetadata map[uint64]struct{}
		pruneCursor      uint64
		stats            [21]uint64
	}
	if unsafe.Offsetof(FreelistTxn{}.stats) != unsafe.Offsetof(previousTxnLayout{}.stats) ||
		unsafe.Sizeof(FreelistTxn{})-unsafe.Sizeof(previousTxnLayout{}) != unsafe.Sizeof(FreelistStateCopyWorkV1{})+unsafe.Sizeof(AllocationCreditV1(nil))+4*unsafe.Sizeof((*allocationCreditLeaseV1)(nil))+unsafe.Sizeof(error(nil)) {
		t.Fatal("transaction capacity grew beyond the admitted fixed copy counters")
	}
	t.Logf("measured frozen-layout/current txn=%d/%d delta=%d; allocator state=%d profile=%d", unsafe.Sizeof(previousTxnLayout{}), unsafe.Sizeof(FreelistTxn{}), unsafe.Sizeof(FreelistTxn{})-unsafe.Sizeof(previousTxnLayout{}), unsafe.Sizeof(allocatorCOWStateV1{}), unsafe.Sizeof(COWPrepareProfileV1{}))
	t.Logf("unsafe layout bytes: stateNode=%d stateChunk=%d txn=%d stats=%d work=%d; consumed/private/map offsets=%d/%d/%d; state allocation classes=%d/%d", unsafe.Sizeof(stateNode{}), unsafe.Sizeof(stateChunk{}), unsafe.Sizeof(FreelistTxn{}), unsafe.Sizeof(FreelistTxnStats{}), unsafe.Sizeof(FreelistStateCopyWorkV1{}), unsafe.Offsetof(FreelistTxn{}.consumed), unsafe.Offsetof(FreelistTxn{}.privatePreparation), unsafe.Offsetof(FreelistTxn{}.changedChunks), stateNodeCopyCapacityV1, stateChunkCopyCapacityV1)
	// The capacities include allocator rounding; the transaction flag uses
	// existing bool padding rather than adding one pointer per state node.
	if unsafe.Sizeof(stateNode{}) > stateNodeCopyCapacityV1 || unsafe.Sizeof(stateChunk{}) > stateChunkCopyCapacityV1 {
		t.Fatal("state copy exceeds its declared allocation-class capacity")
	}
	if unsafe.Offsetof(FreelistTxn{}.privatePreparation) != unsafe.Offsetof(FreelistTxn{}.consumed)+1 || unsafe.Offsetof(FreelistTxn{}.changedChunks) < unsafe.Offsetof(FreelistTxn{}.privatePreparation)+1 {
		t.Fatal("private flag no longer fits consumed-field padding")
	}
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, []uint64{2, 257, 513}, nil), NewReservationLedger())
	private, err := txn.cloneForPrivateAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	stats := private.Stats()
	if stats.StateCopyBytes != stats.StateNodeCopies*stateNodeCopyCapacityV1+stats.StateChunkCopies*stateChunkCopyCapacityV1 || stats.StateIsolationVisits == 0 {
		t.Fatal("isolation capacity or visits omitted")
	}
	before := snapshotPrivateState5105(t, txn)
	private.Retire(2, 1)
	if !maps.Equal(before, snapshotPrivateState5105(t, txn)) {
		t.Fatal("unmaterialized model base or rollback alias mutated")
	}
}

func TestPrivatePreparationPersistentCloneRevokesBothAliases5105(t *testing.T) {
	source := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, []uint64{2, 9, 257}, nil), NewReservationLedger())
	original, err := source.cloneForPrivateAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	original.Retire(30, 1)
	clone, err := original.cloneForAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	if original.privatePreparation || clone.privatePreparation {
		t.Fatal("shared root retains private edit permission")
	}
	held := snapshotPrivateState5105(t, clone)
	original.Retire(31, 1)
	if !maps.Equal(held, snapshotPrivateState5105(t, clone)) {
		t.Fatal("original edited clone backing")
	}
	heldOriginal := snapshotPrivateState5105(t, original)
	clone.Retire(32, 1)
	if !maps.Equal(heldOriginal, snapshotPrivateState5105(t, original)) {
		t.Fatal("clone edited original backing")
	}
	// A second private preparation also isolates aliases before enabling edits.
	second, err := clone.cloneForPrivateAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	beforeClone := snapshotPrivateState5105(t, clone)
	second.Retire(33, 1)
	if !maps.Equal(beforeClone, snapshotPrivateState5105(t, clone)) {
		t.Fatal("private fork edited predecessor")
	}
}

func TestPrivatePreparationExactPersistentCandidateOracle5105(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary-prune", true: "bounded-prune"}[bounded], func(t *testing.T) {
			var free []uint64
			for id := uint64(2); id < 400; id++ {
				free = append(free, id)
			}
			free = append(free, 700, (1<<18)+9)
			base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 1<<20, free, map[uint64]uint64{1100: 1, (1 << 18) + 10: 1}), 2, NewMemoryPageStoreV1())
			run := func(private bool, sink AppendPageSink) (*FreelistTxn, *FreelistCandidateV1, []uint64) {
				live := NewFreelistTxn(base, NewReservationLedger())
				live.Retire(500, 2) // Retained dirty alias exists before the private boundary.
				var txn *FreelistTxn
				var err error
				if private {
					txn, err = live.cloneForPrivateAllocatorPrepare()
				} else {
					txn, err = live.cloneForAllocatorPrepare()
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, id := range []uint64{501, 502, 700, (1 << 18) + 11} {
					txn.Retire(id, 2)
				}
				cap, err := NewReuseCapability(3, 3, 0)
				if err != nil {
					t.Fatal(err)
				}
				if bounded {
					txn.PruneWithCapabilityBounded(cap)
				} else {
					txn.PruneWithCapability(cap)
				}
				var chosen []uint64
				for _, hint := range []uint64{0, 0, 256, 1 << 18} {
					id, err := txn.Allocate(hint)
					if err != nil {
						t.Fatal(err)
					}
					chosen = append(chosen, id)
				}
				aux, err := txn.allocateContiguousRange(3)
				if err != nil {
					t.Fatal(err)
				}
				chosen = append(chosen, aux...)
				candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("private-reference"), sink)
				if err != nil {
					t.Fatal(err)
				}
				if txn.privatePreparation || !txn.consumed {
					t.Fatal("candidate retained editable builder authority")
				}
				return txn, candidate, chosen
			}
			reference, want, chosen := run(false, NewCandidatePageSinkV1())
			private, got, actual := run(true, NewCandidatePageSinkV1())
			if !slices.Equal(chosen, actual) || want.GenerationRef() != got.GenerationRef() || !pageImagesEqual(want.Pages(), got.Pages()) || !slices.Equal(want.DirtyPageIDs(), got.DirtyPageIDs()) {
				t.Fatal("private builder changed allocation order, exact pages/CRC, or generation identity")
			}
			if !slices.Equal(want.Generation().record.Extents, got.Generation().record.Extents) || !maps.Equal(snapshotPrivateState5105(t, reference), snapshotPrivateState5105(t, private)) {
				t.Fatal("private builder changed reservations, summaries, or prune outcomes")
			}
			a, b := reference.Stats(), private.Stats()
			if a.COWPages != b.COWPages || a.COWBytes != b.COWBytes || a.StateMutationPaths != b.StateMutationPaths || a.StateMutationItems != b.StateMutationItems || a.PageVisits != b.PageVisits {
				t.Fatal("private builder changed metadata count or omitted mutation/visit work")
			}
			mutating := &mutatingRetainingPageSinkV1{}
			_, generic, _ := run(true, mutating)
			for _, image := range mutating.pages {
				clear(image.Data)
			}
			if !pageImagesEqual(want.Pages(), generic.Pages()) {
				t.Fatal("generic sink retained mutable candidate aliases")
			}
			writer := &candidatePageRecordingWriterV1{}
			if err := got.WritePagesToV1(writer); err != nil {
				t.Fatal(err)
			}
			forkLive := NewFreelistTxn(got.Generation(), NewReservationLedger())
			fork, err := forkLive.cloneForPrivateAllocatorPrepare()
			if err != nil {
				t.Fatal(err)
			}
			fork.Retire(900, 3)
			if _, err := fork.MaterializeCandidate(4, 4, candidateIDFromString("private-fork"), NewCandidatePageSinkV1()); err != nil {
				t.Fatal(err)
			}
			got = nil
			private = nil
			fork = nil
			assertRetainedPlanViews(t, writer)
		})
	}
}

func TestPrivatePreparationErrorsRevokeAuthority5105(t *testing.T) {
	for _, sink := range []AppendPageSink{nil, failingPageSinkV1{}} {
		live := NewFreelistTxn(MustNewFreelistGenerationV1(1, 64, []uint64{2, 9}, nil), NewReservationLedger())
		txn, err := live.cloneForPrivateAllocatorPrepare()
		if err != nil {
			t.Fatal(err)
		}
		_, err = txn.MaterializeCandidate(2, 2, candidateIDFromString("private-fail"), sink)
		if err == nil || txn.privatePreparation {
			t.Fatal("error return retained private edit permission")
		}
	}
}

func TestPrivatePreparationDirtyRollbackAfterSinkFailure5105(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 1024, []uint64{2, 9, 33, 257}, nil), 2, NewMemoryPageStoreV1())
	if _, err := p.Alloc(int(base.HighWater())); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err := a.EnableCOWV1(base, NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Alloc(0); err != nil {
		t.Fatal(err)
	}
	rollback := a.cow.txn
	state := snapshotPrivateState5105(t, rollback)
	allocated := slices.Clone(rollback.allocated)
	stats := a.Counters()
	cap, err := NewReuseCapability(2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.PrepareCOWCandidateRetiringV1(3, 3, candidateIDFromString("private-rollback"), cap, []COWRetirementV1{{PageIDs: []uint64{50, 51, 52}, LastReachableCommitSeq: 2}}, 3, failingPageSinkV1{})
	if err == nil {
		t.Fatal("missing sink failure")
	}
	if a.cow.txn != rollback || rollback.privatePreparation || !maps.Equal(state, snapshotPrivateState5105(t, rollback)) || !slices.Equal(allocated, rollback.allocated) || a.Counters() != stats {
		t.Fatal("failed private preparation changed rollback bytes/state/counters")
	}
	if a.cow.prepared != nil || a.cow.ledger.candidates.Len() != 0 || a.cow.ledger.owners.Len() != 0 {
		t.Fatal("failed preparation retained active candidate reservation")
	}
	// The generic sink's attempted append tail remains conservatively burned,
	// as it does on the persistent path; rollback must not release that horizon.
	if len(a.cow.ledger.burnedTails) == 0 || a.cow.ledger.Reservations() == 0 {
		t.Fatal("failed generic sink lost attempted append-tail burn")
	}
	t.Logf("failed preparation: active candidates=%d owners=%d burned ranges=%d reserved burned pages=%d", a.cow.ledger.candidates.Len(), a.cow.ledger.owners.Len(), len(a.cow.ledger.burnedTails), a.cow.ledger.Reservations())
	profile := a.COWPrepareProfileV1()
	if profile.PreparationCopyWork.StateNodeCopies == 0 || profile.PreparationCopyWork.StateCopyBytes == 0 {
		t.Fatal("failed preparation work disappeared with rollback")
	}
	if again := a.COWPrepareProfileV1(); again != profile {
		t.Fatal("counter read changed work, credits, or authority")
	}
	if _, err := rollback.Allocate(0); err != nil && !errors.Is(err, ErrPageReserved) {
		t.Fatal(err)
	}
}

func TestPrivatePreparationAbortKeepsWorkAndRetainedViews5105(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(1024); err != nil {
		t.Fatal(err)
	}
	var free []uint64
	for id := uint64(2); id < 200; id++ {
		free = append(free, id)
	}
	a := New(p, 0)
	if err := a.EnableCOWV1(MustNewFreelistGenerationV1(1, 1024, free, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	cap, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	id := candidateIDFromString("private-abort")
	prepared, err := a.PrepareCOWCandidateV1(2, 2, id, cap, 0, NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	work := a.COWPrepareProfileV1().PreparationCopyWork
	if work.StateCopyBytes == 0 {
		t.Fatal("preparation did not attribute isolation")
	}
	same, err := a.PrepareCOWCandidateV1(2, 2, id, cap, 0, NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	if same != prepared || a.COWPrepareProfileV1().PreparationCopyWork != work {
		t.Fatal("prepared retry repeated or erased work")
	}
	writer := &candidatePageRecordingWriterV1{}
	if err := prepared.Candidate().WritePagesToV1(writer); err != nil {
		t.Fatal(err)
	}
	if err := a.AbortCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	if a.COWPrepareProfileV1().PreparationCopyWork != work {
		t.Fatal("abort erased incurred work")
	}
	prepared = nil
	same = nil
	if _, err := a.Alloc(0); err != nil {
		t.Fatal(err)
	}
	retry, err := a.PrepareCOWCandidateV1(2, 2, candidateIDFromString("private-abort-retry"), cap, 0, NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.AbortCOWCandidateV1(retry); err != nil {
		t.Fatal(err)
	}
	retry = nil
	assertRetainedPlanViews(t, writer)
}

// Logical scans omit empty summaries. Retain every backing object instead so
// rebirth cannot silently change an old generation or its rollback graph.
type retainedPrivateGraph5105 struct {
	nodes  map[*stateNode]stateNode
	chunks map[*stateChunk]stateChunk
}

func retainPrivateGraph5105(t *testing.T, root *stateNode) retainedPrivateGraph5105 {
	t.Helper()
	retainStateNodeV1(root)
	t.Cleanup(func() { releaseStateNodeV1(root) })
	held := retainedPrivateGraph5105{make(map[*stateNode]stateNode), make(map[*stateChunk]stateChunk)}
	var visit func(*stateNode)
	visit = func(n *stateNode) {
		if n == nil {
			return
		}
		held.nodes[n] = *n
		if n.chunk != nil {
			held.chunks[n.chunk] = *n.chunk
		}
		for _, child := range n.child {
			visit(child)
		}
	}
	visit(root)
	return held
}

func (held retainedPrivateGraph5105) assertUnchanged(t *testing.T) {
	t.Helper()
	for n, before := range held.nodes {
		actual := *n
		actual.ownedRefs, before.ownedRefs = 0, 0
		if actual != before {
			t.Fatal("retained node backing changed, including hidden empty descendants")
		}
	}
	for chunk, before := range held.chunks {
		actual := *chunk
		actual.ownedRefs, before.ownedRefs = 0, 0
		if actual != before {
			t.Fatal("retained chunk backing changed")
		}
	}
}

func hiddenEmptyGeneration5105(t *testing.T, target uint64) *FreelistCandidateV1 {
	t.Helper()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, target+8192, []uint64{2, target, target + 256}, nil), 2, NewMemoryPageStoreV1())
	txn := NewFreelistTxn(base, NewReservationLedger())
	chosen, err := txn.Allocate(target)
	if err != nil || chosen != target {
		t.Fatalf("empty fixture allocation = %d, %v; want %d", chosen, err, target)
	}
	candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("hidden-empty-base"), NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	n := candidate.Generation().root
	boundary := false
	for depth := 0; depth < chunkTrieDepth; depth++ {
		child := n.child[chunkNibble(target>>freelistChunkShift, depth)]
		if child == nil {
			t.Fatal("materialization dropped fixture empty path")
		}
		if n.pageID != 0 && child.pageID == 0 && child.freeCount+child.retiredCount == 0 {
			boundary = true
		}
		n = child
	}
	if !boundary || n.pageID != 0 || n.chunk != nil {
		t.Fatal("fixture lacks hidden empty descendants below durable ancestor")
	}
	return candidate
}

func TestPrivatePreparationHiddenEmptyRebirthPersistentOracle5105(t *testing.T) {
	for _, target := range []uint64{257, 4098, 65538} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			baseCandidate := hiddenEmptyGeneration5105(t, target)
			base := baseCandidate.Generation()
			heldBase := retainPrivateGraph5105(t, base.root)
			basePages := baseCandidate.Pages()
			writer := &candidatePageRecordingWriterV1{}
			if err := baseCandidate.WritePagesToV1(writer); err != nil {
				t.Fatal(err)
			}
			run := func(private bool) (*FreelistTxn, *FreelistCandidateV1) {
				live := NewFreelistTxn(base, NewReservationLedger())
				heldRollback := retainPrivateGraph5105(t, live.root)
				var staged *FreelistTxn
				var err error
				if private {
					staged, err = live.cloneForPrivateAllocatorPrepare()
				} else {
					staged, err = live.cloneForAllocatorPrepare()
				}
				if err != nil {
					t.Fatal(err)
				}
				before := staged.Stats()
				staged.Retire(target, 3)
				heldBase.assertUnchanged(t)
				heldRollback.assertUnchanged(t)
				if private {
					work := staged.Stats()
					if work.StateIsolationVisits <= before.StateIsolationVisits || work.StateNodeCopies <= before.StateNodeCopies {
						t.Fatal("durable first-copy isolation omitted visits or copies")
					}
					if work.StateCopyBytes != work.StateNodeCopies*stateNodeCopyCapacityV1+work.StateChunkCopies*stateChunkCopyCapacityV1 {
						t.Fatal("hidden-subtree copy capacity omitted")
					}
					staged.Retire(target+1, 3)
					if staged.Stats().FreelistStateCopyWorkV1 != work.FreelistStateCopyWorkV1 {
						t.Fatal("reborn private path copied more than once")
					}
				} else {
					staged.Retire(target+1, 3)
				}
				candidate, err := staged.MaterializeCandidate(4, 4, candidateIDFromString("hidden-empty-rebirth"), NewCandidatePageSinkV1())
				if err != nil {
					t.Fatal(err)
				}
				heldBase.assertUnchanged(t)
				heldRollback.assertUnchanged(t)
				return staged, candidate
			}
			reference, want := run(false)
			private, got := run(true)
			if want.GenerationRef() != got.GenerationRef() || !pageImagesEqual(want.Pages(), got.Pages()) || !slices.Equal(want.DirtyPageIDs(), got.DirtyPageIDs()) || !slices.Equal(want.Generation().record.Extents, got.Generation().record.Extents) || !maps.Equal(snapshotPrivateState5105(t, reference), snapshotPrivateState5105(t, private)) {
				t.Fatal("hidden-empty rebirth differs from persistent page/CRC/order/reservation/state oracle")
			}
			a, b := reference.Stats(), private.Stats()
			if a.COWPages != b.COWPages || a.COWBytes != b.COWBytes || a.StateMutationPaths != b.StateMutationPaths || a.StateMutationItems != b.StateMutationItems || a.PageVisits != b.PageVisits {
				t.Fatal("rebirth changed logical work or output accounting")
			}
			if !pageImagesEqual(basePages, baseCandidate.Pages()) {
				t.Fatal("base candidate bytes changed")
			}
			assertRetainedPlanViews(t, writer)
		})
	}
}

func TestPrivatePreparationHiddenEmptyRebirthAbortAndFailure5105(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			const target = uint64(4098)
			baseCandidate := hiddenEmptyGeneration5105(t, target)
			base := baseCandidate.Generation()
			heldBase := retainPrivateGraph5105(t, base.root)
			p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.Alloc(int(base.HighWater())); err != nil {
				t.Fatal(err)
			}
			a := New(p, 0)
			if err := a.EnableCOWV1(base, NewReservationLedger()); err != nil {
				t.Fatal(err)
			}
			rollback := a.cow.txn
			heldRollback := retainPrivateGraph5105(t, rollback.root)
			cap, err := NewReuseCapability(3, 3, 0)
			if err != nil {
				t.Fatal(err)
			}
			var sink AppendPageSink = NewCandidatePageSinkV1()
			if fail {
				sink = failingPageSinkV1{}
			}
			prepared, err := a.PrepareCOWCandidateRetiringV1(4, 4, candidateIDFromString("hidden-empty-abort"), cap, []COWRetirementV1{{PageIDs: []uint64{target}, LastReachableCommitSeq: 3}}, 0, sink)
			if fail {
				if err == nil {
					t.Fatal("missing sink failure")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				writer := &candidatePageRecordingWriterV1{}
				if err := prepared.Candidate().WritePagesToV1(writer); err != nil {
					t.Fatal(err)
				}
				if err := a.AbortCOWCandidateV1(prepared); err != nil {
					t.Fatal(err)
				}
				assertRetainedPlanViews(t, writer)
			}
			if a.cow.txn != rollback || rollback.privatePreparation != !fail || a.cow.prepared != nil {
				t.Fatal("rebirth abort/failure did not restore rollback")
			}
			heldBase.assertUnchanged(t)
			heldRollback.assertUnchanged(t)
			work := a.COWPrepareProfileV1().PreparationCopyWork
			if work.StateIsolationVisits == 0 || work.StateCopyBytes == 0 || work.StateCopyBytes != work.StateNodeCopies*stateNodeCopyCapacityV1+work.StateChunkCopies*stateChunkCopyCapacityV1 {
				t.Fatal("rebirth abort/failure erased copy/visit/capacity work")
			}
		})
	}
}

func TestPrivatePreparationDurableLeafZeroIDChunk5105(t *testing.T) {
	// A direct ownership-boundary fixture covers the leaf rule independently
	// of today's emitter, which normally removes an emptied chunk pointer.
	root := mutateChunk(nil, 0, 0, func(c *stateChunk) { c.setFree(2, true) })
	n := root
	var path [chunkTrieDepth + 1]*stateNode
	for depth := 0; depth <= chunkTrieDepth; depth++ {
		path[depth] = n
		n.pageID = uint64(depth + 1)
		if depth < chunkTrieDepth {
			n = n.child[0]
		}
	}
	n.chunk.free = [4]uint64{}
	n.chunk.pageID = 0
	for depth := chunkTrieDepth; depth >= 0; depth-- {
		recomputeStateNode(path[depth], depth)
	}
	held := retainPrivateGraph5105(t, root)
	var stats FreelistTxnStats
	private := mutateChunkForPreparation(root, 0, 0, func(c *stateChunk) { c.setFree(3, true) }, true, &stats)
	held.assertUnchanged(t)
	if lookupChunk(private, 0) == n.chunk || !lookupChunk(private, 0).isFree(3) || stats.StateChunkCopies != 1 || stats.StateCopyBytes != stats.StateNodeCopies*stateNodeCopyCapacityV1+stateChunkCopyCapacityV1 {
		t.Fatal("durable leaf failed to isolate/charge zero-ID chunk")
	}
}
