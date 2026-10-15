package db

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"testing"
)

func TestPrimaryPromotionSharesImmutableLogicalRootV6(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.Set([]byte("key"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	old := d.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing original logical reader")
	}
	defer old.Close()
	logical := old.primaryRoot.ref
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := d.durableRoot.primary.records[d.durableRoot.slot]; got != logical {
		t.Fatalf("slot owns duplicate promotion RAM image: slot=%+v original=%+v", got, logical)
	}
	if got, err := old.Get([]byte("key")); err != nil || !bytes.Equal(got, []byte("before")) {
		t.Fatal("old reader", err, got)
	}
}

// Both physical capsule slots keep independent dependency edges, while old
// logical readers retain exactly the closure admitted when acquired.
func TestPrimarySlotCustodyBothSlotsOverwriteAndReopenV6(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			d.Close()
		}
	}()
	if err = d.Set([]byte("key"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	old := d.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing reader")
	}
	defer old.Close()
	a := old.primaryRoot.arena
	initial, err := a.CapturePhysicalPagesOrdinary(a.Pager().PageCount(), []primaryarena.Ref{old.primaryRoot.ref})
	if err != nil {
		t.Fatal(err)
	}
	count := func(x []bool) int {
		n := 0
		for _, v := range x {
			if v {
				n++
			}
		}
		return n
	}
	for i := 0; i < 5; i++ {
		if err = d.Set([]byte("key"), []byte(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatal(err)
		}
		if err = d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		runtime := d.durableRoot.primary
		slot := d.durableRoot.slot
		if runtime.records[slot] != d.State().primaryRoot.ref {
			t.Fatal("slot cloned logical root")
		}
		if runtime.closures[slot] == (primaryarena.Ref{}) || runtime.capsuleCharges[slot] == 0 {
			t.Fatal("slot lost independent closure/image charge")
		}
		if runtime.proofClosures[slot] == (primaryarena.Ref{}) || runtime.proofs[slot] == (primaryarena.Ref{}) {
			t.Fatal("parent proof lost independent custody")
		}
	}
	runtime := d.durableRoot.primary
	for i := range runtime.records {
		if runtime.records[i] == (primaryarena.Ref{}) || runtime.closures[i] == (primaryarena.Ref{}) || runtime.capsuleCharges[i] == 0 {
			t.Fatalf("slot %d incomplete", i)
		}
	}
	after, err := a.CapturePhysicalPagesOrdinary(a.Pager().PageCount(), []primaryarena.Ref{old.primaryRoot.ref})
	if err != nil {
		t.Fatal(err)
	}
	if count(after) != count(initial) {
		t.Fatal("reader closure grew")
	}
	if got, e := old.Get([]byte("key")); e != nil || !bytes.Equal(got, []byte("before")) {
		t.Fatalf("old reader %q %v", got, e)
	}
	old.Close()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	d, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed = false
	if got, e := d.Get([]byte("key")); e != nil || !bytes.Equal(got, []byte("after-4")) {
		t.Fatalf("reopen %q %v", got, e)
	}
	runtime = d.durableRoot.primary
	for i := range runtime.records {
		if runtime.records[i] == (primaryarena.Ref{}) || runtime.closures[i] == (primaryarena.Ref{}) || runtime.capsuleCharges[i] == 0 {
			t.Fatalf("recovered slot %d incomplete", i)
		}
	}
	if err = d.Set([]byte("key"), []byte("after-reopen")); err != nil {
		t.Fatal(err)
	}
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
}

func TestPrimaryPromotionCustodyRefusalHasNoPartialAcquireV6(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a := d.idx.Load().primary
	runtime := d.durableRoot.primary
	root := runtime.records[d.durableRoot.slot]
	edge := primaryCapsuleDependencyEdgeV6(d.durableRoot.record)
	before := a.Counters()
	w := &iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: 1 << 20}
	if _, _, ready, e := a.AcquirePromotionCustodyV6(root, root, edge, edge, w); ready || e != nil {
		t.Fatalf("partial admission %t %v", ready, e)
	}
	if got := a.Counters(); got.RootAcquires != before.RootAcquires || got.EdgeWrites != before.EdgeWrites {
		t.Fatal("refusal changed refs")
	}
	bad := edge
	bad.PageID++
	w = &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	if _, _, ready, e := a.AcquirePromotionCustodyV6(root, root, edge, bad, w); ready || e == nil {
		t.Fatalf("invalid fourth edge %t %v", ready, e)
	}
	if got := a.Counters(); got.RootAcquires != before.RootAcquires || got.EdgeWrites != before.EdgeWrites {
		t.Fatal("invalid final operand partially acquired")
	}
	w = &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	closure, parentClosure, ready, e := a.AcquirePromotionCustodyV6(root, root, edge, edge, w)
	if !ready || e != nil {
		t.Fatalf("same root/edge multiplicity %t %v", ready, e)
	}
	for _, ref := range [4]primaryarena.Ref{root, root, closure, parentClosure} {
		if _, e := a.Drop(ref, nil); e != nil {
			t.Fatal(e)
		}
	}
}
