package rootpublication

import (
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func capsuleFixtureV6(t *testing.T, seq uint64) (DurablePrimaryRootRecordV5, []byte) {
	t.Helper()
	base := primaryRootTestLeaf(2, "base", "retained", 1)
	system := primaryRootTestLeaf(3, "system", "retained", 1)
	dir := make([]byte, page.PageSize)
	if e := node.EncodePrimaryDirectory(dir, primaryarena.Namespace+20, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256(base)}, nil); e != nil {
		t.Fatal(e)
	}
	return DurablePrimaryRootRecordV5{Record: DurableRootRecordV1{CommitSeq: seq, DurableSeq: seq, UserRootPageID: primaryarena.Namespace + 20, SystemRootPageID: 3, TotalPages: 64, Freelist: freelist.GenerationRefV1{HeaderPageID: 4, GenerationID: 1, CommitSeq: 1, HighWater: 64, Digest: [32]byte{1}}, Directory: DependencyDirectoryRefV2{RootPageID: primaryarena.Namespace + 8}, MetaProjectionDigest: [32]byte{1}}, Primary: PrimaryProjectionV5{ArenaUUID: [16]byte{9}, ArenaHighWater: 21, DirectoryDigest: sha256.Sum256(dir), BaseRootPageID: 2, BaseSequence: 1, DataCommitSeq: 1, BaseDigest: sha256.Sum256(base), SystemDigest: sha256.Sum256(system)}}, dir
}

func TestPrimaryCapsuleV6CompleteIndependentSlots(t *testing.T) {
	parent, pdir := capsuleFixtureV6(t, 1)
	child, cdir := capsuleFixtureV6(t, 2)
	a, b := make([]byte, PrimaryCapsuleSizeV6), make([]byte, PrimaryCapsuleSizeV6)
	if e := EncodePrimaryCapsuleV6(a, 0, parent, pdir, nil, nil); e != nil {
		t.Fatal(e)
	}
	if e := EncodePrimaryCapsuleV6(b, 1, child, cdir, &parent, pdir); e != nil {
		t.Fatal(e)
	}
	selected, e := SelectPrimaryCapsulesV6([2][]byte{a, b}, [16]byte{9}, func(v PrimaryCapsuleViewV6) error { return nil })
	if e != nil || selected.Slot() != 1 || selected.Current().Record.CommitSeq != 2 {
		t.Fatalf("newest: %v", e)
	}
	retained := selected.CopyOwned()
	// A retained read/cut owns bytes, rather than borrowing the overwritten slot.
	clear(b)
	if retained.Current().Record.CommitSeq != 2 || retained.Parent().Record.CommitSeq != 1 {
		t.Fatal("copied capsule borrowed slot")
	}
	for _, offset := range []int{0, 80, page.PageSize + 200, 2*page.PageSize + 200} {
		bad := append([]byte(nil), retained.Image()...)
		bad[offset] ^= 1
		got, e := SelectPrimaryCapsulesV6([2][]byte{a, bad}, [16]byte{9}, func(v PrimaryCapsuleViewV6) error { return nil })
		if e != nil || got.Slot() != 0 || got.Current().Record.CommitSeq != 1 {
			t.Fatalf("torn/corrupt offset %d: %v", offset, e)
		}
	}
	if _, e := SelectPrimaryCapsulesV6([2][]byte{nil, b}, [16]byte{9}, func(v PrimaryCapsuleViewV6) error { return nil }); e == nil {
		t.Fatal("both invalid resurrected a non-slot root")
	}
	for _, n := range []int{page.PageSize, 2 * page.PageSize, PrimaryCapsuleSizeV6 - 1} {
		if _, e := DecodePrimaryCapsuleV6(retained.Image()[:n], 1, [16]byte{9}); e == nil {
			t.Fatalf("truncated %d accepted", n)
		}
	}
	if _, e := DecodePrimaryCapsuleV6(retained.Image(), 0, [16]byte{9}); e == nil {
		t.Fatal("slot substitution accepted")
	}
	if _, e := DecodePrimaryCapsuleV6(retained.Image(), 1, [16]byte{8}); e == nil {
		t.Fatal("namespace substitution accepted")
	}
}

func TestPrimaryCapsuleV6ActualIndependentPhysicalClosures(t *testing.T) {
	data := freelist.NewMemoryPageStoreV1()
	banks := freelist.NewMemoryPageStoreV1()
	data.Pages[2] = primaryRootTestLeaf(2, "base", "retained", 1)
	data.Pages[3] = primaryRootTestLeaf(3, "system", "retained", 1)
	g := freelist.MustNewFreelistGenerationV1(1, 64, nil, nil)
	tx := freelist.NewFreelistTxn(g, freelist.NewReservationLedger())
	candidate, e := tx.MaterializeCandidate(7, 7, freelist.CandidateIDV1{7}, data)
	if e != nil {
		t.Fatal(e)
	}
	makeOperand := func(seq uint64, key string, id uint64) (DurablePrimaryRootRecordV5, []byte) {
		v, _ := capsuleFixtureV6(t, seq)
		v.Record.Freelist = candidate.Generation().GenerationRef()
		v.Record.TotalPages = v.Record.Freelist.HighWater
		v.Primary.DataCommitSeq = 7
		v.Primary.ArenaHighWater = 21
		component := primaryRootTestLeaf(primaryarena.Namespace+id, key, "component", page.EntryRevision(seq))
		banks.Pages[id] = component
		dir := make([]byte, page.PageSize)
		if e := node.EncodePrimaryDirectory(dir, v.Record.UserRootPageID, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256(data.Pages[2])}, []node.PrimaryDirectoryEntry{{Key: []byte(key), Revision: page.EntryRevision(seq), Kind: node.PrimaryPut, Operand: node.PrimaryOperand{Ref: page.PageChildRef(primaryarena.Namespace + id), Digest: sha256.Sum256(component)}}}); e != nil {
			t.Fatal(e)
		}
		v.Primary.DirectoryDigest = sha256.Sum256(dir)
		return v, dir
	}
	parent, pdir := makeOperand(7, "prior-only", 8)
	child, cdir := makeOperand(8, "new-only", 9)
	parent.Record.DurableSeq = 1
	child.Record.DurableSeq = 2
	var slots [2][]byte
	for i := range slots {
		slots[i] = make([]byte, PrimaryCapsuleSizeV6)
	}
	if e := EncodePrimaryCapsuleV6(slots[0], 0, parent, pdir, nil, nil); e != nil {
		t.Fatal(e)
	}
	if e := EncodePrimaryCapsuleV6(slots[1], 1, child, cdir, &parent, pdir); e != nil {
		t.Fatal(e)
	}
	validate := func(v PrimaryCapsuleViewV6) error {
		_, e := v.ValidatePhysicalProjectionV6(data, banks, uint64(len(data.Pages))+100, 21)
		return e
	}
	selected, e := SelectPrimaryCapsulesV6(slots, [16]byte{9}, validate)
	if e != nil || selected.Slot() != 1 {
		t.Fatalf("newest physical: %v", e)
	}
	// Loss of the newest actual component selects only the older eligible slot.
	newestComponent := banks.Pages[9]
	delete(banks.Pages, 9)
	selected, e = SelectPrimaryCapsulesV6(slots, [16]byte{9}, validate)
	if e != nil || selected.Slot() != 0 || selected.Current().Record.CommitSeq != 7 {
		t.Fatalf("older complete fallback: %v", e)
	}
	// Restore the child component: now only its independently retained parent
	// closure is broken. The child must reject despite a valid current operand.
	banks.Pages[9] = newestComponent
	delete(banks.Pages, 8)
	childView, err := DecodePrimaryCapsuleV6(slots[1], 1, [16]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	if err = validate(childView); err == nil {
		t.Fatal("child accepted missing parent component")
	}
	if _, e = SelectPrimaryCapsulesV6(slots, [16]byte{9}, validate); e == nil {
		t.Fatal("missing both complete closures accepted")
	}
}
