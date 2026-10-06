package caching

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWEmptyBasisIteratorNeedsNoReadWorkspace(t *testing.T) {
	db := cowPointerFixture(t)
	db.valueLogThreshold = 64
	if err := db.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if empty, err := old.cowCut.basis.snapshot.OwnedUserRootEmpty(); err != nil || !empty {
		t.Fatalf("empty basis=%t err=%v", empty, err)
	}
	// Leave enough for owning cached cursors, but not the former eager disk/page
	// workspace. This is real shared-budget pressure, not mocked allocation.
	before := db.cow.budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead - (32 << 10))
	if err != nil {
		t.Fatal(err)
	}
	it, err := old.Iterator(nil, nil)
	if err != nil {
		pressure.Close()
		t.Fatal(err)
	}
	inner := it.(*snapshotBoundIterator).inner.(*cowMergeIterator)
	if inner.workspace != nil {
		t.Fatal("inline iterator created read workspace")
	}
	if !it.Valid() || string(it.Key()) != "a" || string(it.Value()) != "old" {
		t.Fatalf("old entry key=%q value=%q err=%v", it.Key(), it.Value(), it.Error())
	}
	if inner.workspace != nil {
		t.Fatal("inline value created read workspace")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	pressure.Close()
	if err := db.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("d"), []byte("disk")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	current, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if empty, err := current.cowCut.basis.snapshot.OwnedUserRootEmpty(); err != nil || empty {
		t.Fatalf("nonempty basis=%t err=%v", empty, err)
	}
	if empty, err := old.cowCut.basis.snapshot.OwnedUserRootEmpty(); err != nil || !empty {
		t.Fatalf("old basis changed: empty=%t err=%v", empty, err)
	}
	for _, s := range []*Snapshot{old, current} {
		it, err := s.Iterator(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := "old"
		if s == current {
			want = "new"
		}
		if !it.Valid() || string(it.Key()) != "a" || string(it.Value()) != want {
			t.Fatalf("captured winner=%q err=%v", it.Value(), it.Error())
		}
		if s == current {
			it.Next()
			if !it.Valid() || string(it.Key()) != "d" || string(it.Value()) != "disk" {
				t.Fatalf("disk source missing: key=%q err=%v", it.Key(), it.Error())
			}
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// A retained nonempty backend basis also remains exact across a later
	// accepted checkpoint; skipping a current empty/dirty state cannot replace it.
	if err := db.Set([]byte("a"), []byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := current.Get([]byte("a")); err != nil || string(got) != "new" {
		t.Fatalf("old disk basis=%q err=%v", got, err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "later" {
		t.Fatalf("current basis=%q err=%v", got, err)
	}

}

func TestCOWCodecSlotsGrowWithinActualAdmission(t *testing.T) {
	db := cowPointerFixture(t)
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w, err := newCOWReadWorkspace(s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if len(w.codecs) != 0 || cap(w.codecs) != 0 || w.point != nil {
		t.Fatal("new workspace eagerly allocated decoder/page backing")
	}
	limits := db.cow.budget.Limits()
	// Every newly grown backing is preadmitted. At full pressure it refuses,
	// leaving both length and capacity unchanged; closing pressure permits retry.
	before := db.cow.budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(limits.MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.addCodecSlot(); !errors.Is(err, memtable.ErrCOWCapacity) || cap(w.codecs) != 0 {
		t.Fatalf("slot refused err=%v capacity=%d", err, cap(w.codecs))
	}
	pressure.Close()
	for i := 0; i < limits.MaxResources; i++ {
		slot, err := w.addCodecSlot()
		if err != nil || slot != i || cap(w.codecs) > limits.MaxResources {
			t.Fatalf("slot=%d want=%d cap=%d err=%v", slot, i, cap(w.codecs), err)
		}
	}
	if _, err := w.addCodecSlot(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("finite codec slots: %v", err)
	}
}
