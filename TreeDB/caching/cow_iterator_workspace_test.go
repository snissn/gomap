package caching

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A warmed public physical iterator needs one bound-reader lease and one
// construction lease. Its independent workspace shares that construction
// lifetime instead of adding another allocation/admission on every cache hit.
func TestCOWPhysicalIteratorWorkspaceSharesAdmission(t *testing.T) {
	for _, direction := range []string{"forward", "reverse"} {
		t.Run(direction, func(t *testing.T) {
			reverse := direction == "reverse"
			db, backend, want := cowLeafCacheModeFixture(t, 128, true, "cow_ordered")
			if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("warm physical cache: value=%q error=%v", got, err)
			}
			snapshot, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			start, end := []byte("a000"), []byte("a002")
			open := snapshot.Iterator
			if reverse {
				open = snapshot.ReverseIterator
			}
			allocations := testing.AllocsPerRun(8, func() {
				it, err := open(start, end)
				if err != nil {
					t.Fatal(err)
				}
				if !it.Valid() || !bytes.Equal(it.Value(), want) {
					t.Fatalf("warmed iterator allocation control: %v", it.Error())
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
			})
			t.Logf("warmed public %s iterator allocations/op=%g", direction, allocations)
			before := db.cow.budget.Stats()
			hits := cowLeafCacheCounter(t, backend, "hits")
			it, err := open(start, end)
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			if !it.Valid() || !bytes.Equal(it.Value(), want) {
				t.Fatalf("physical iterator: valid=%t error=%v", it.Valid(), it.Error())
			}
			if cowLeafCacheCounter(t, backend, "hits") <= hits {
				t.Fatal("public iterator did not use the warmed verified physical cache")
			}
			if leases := db.cow.budget.Stats().ExternalLeases - before.ExternalLeases; leases != 2 {
				t.Fatalf("warmed iterator added %d leases, want bound reader + construction (2)", leases)
			}
			// Admission covers the rounded concrete owner, every construction
			// backing, the inspection envelope and both lease objects before seek.
			wrapper, heap := merging.ForwardAllocationSizes(1)
			if reverse {
				wrapper, heap = merging.ReverseAllocationSizes(1)
			}
			physicalOwnerSize := uint64(unsafe.Sizeof(struct {
				cowMergeIterator
				workspace cowReadWorkspace
			}{}))
			required := memtable.COWAllocationCharge(physicalOwnerSize) +
				memtable.COWAllocationCharge(uint64(unsafe.Sizeof(merging.IteratorSource{}))) +
				memtable.COWAllocationCharge(wrapper) + memtable.COWAllocationCharge(heap) +
				memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize()) +
				memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0)))) +
				memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0)))) +
				memtable.COWAllocationCharge(uint64(unsafe.Sizeof(snapshotBoundIterator{}))) +
				2*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			for _, size := range valuelog.COWInspectionMetadataAllocationSizes() {
				required += memtable.COWAllocationCharge(size)
			}
			if actual := db.cow.budget.Stats().ReservedBytes - before.ReservedBytes; actual != required {
				t.Fatalf("physical iterator charged %d bytes, want complete rounded envelope %d", actual, required)
			}
			other, err := open(start, end)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			a := it.(*snapshotBoundIterator).inner.(*cowMergeIterator)
			b := other.(*snapshotBoundIterator).inner.(*cowMergeIterator)
			if !reverse {
				if _, ok := a.Iterator.(*merging.SingleSourceMerger); !ok {
					t.Fatalf("physical-only iterator used %T, want the admitted one-source adapter", a.Iterator)
				}
			}
			if a.workspace == b.workspace {
				t.Fatal("independent iterators shared mutable workspace")
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			if !other.Valid() || !bytes.Equal(other.Value(), want) {
				t.Fatalf("closing another iterator changed this reader: %v", other.Error())
			}
			if err := other.Close(); err != nil {
				t.Fatal(err)
			}
			after := db.cow.budget.Stats()
			if after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases || after.ReservedBytes != before.ReservedBytes {
				t.Fatalf("iterator admission did not drain: before=%+v after=%+v", before, after)
			}
			leaseOverhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - required + 1 - leaseOverhead)
			if err != nil {
				t.Fatal(err)
			}
			pressured := db.cow.budget.Stats()
			it, err = open(start, end)
			if !errors.Is(err, memtable.ErrCOWCapacity) || it != nil {
				if it != nil {
					_ = it.Close()
				}
				pressure.Close()
				t.Fatalf("physical iterator bypassed complete admission: iterator=%v error=%v", it, err)
			}
			if got := db.cow.budget.Stats(); got.ReservedBytes != pressured.ReservedBytes || got.ExternalLeases != pressured.ExternalLeases || got.ExternalBytes != pressured.ExternalBytes {
				pressure.Close()
				t.Fatalf("refused physical iterator leaked admission: before=%+v after=%+v", pressured, got)
			}
			pressure.Close()
			it, err = open(start, end)
			if err != nil {
				t.Fatal(err)
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Exercise the constructor's post-source error path after its disk cursor has
// actually created decoder children. The injected source error must close both
// real source cursors and the whole physical workspace before refunding owners.
func TestCOWPhysicalIteratorConstructorErrorDrainsWorkspace(t *testing.T) {
	db, _, _ := cowLeafCacheModeFixture(t, -1, true, "cow_ordered")
	if err := db.Set([]byte("pending"), []byte("inline")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	original := snapshot.rootIterator.immutables
	sources := append([]memtable.Table(nil), original...)
	stop := errors.New("injected source cursor failure")
	closed := 0
	for i, source := range sources {
		if source.Len() != 0 {
			sources[i] = cowIteratorErrorTable{Table: source, err: stop, closed: &closed}
			break
		}
	}
	snapshot.rootIterator.immutables = sources
	defer func() { snapshot.rootIterator.immutables = original }()
	before := db.cow.budget.Stats()
	it, err := snapshot.Iterator([]byte("a000"), nil)
	if !errors.Is(err, stop) || it != nil || closed != 1 {
		if it != nil {
			_ = it.Close()
		}
		t.Fatalf("constructor source error: iterator=%v error=%v source closes=%d", it, err, closed)
	}
	after := db.cow.budget.Stats()
	if after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases || after.ReservedBytes != before.ReservedBytes {
		t.Fatalf("failed physical iterator leaked child admission: before=%+v after=%+v", before, after)
	}
}

type cowIteratorErrorTable struct {
	memtable.Table
	err    error
	closed *int
}

func (table cowIteratorErrorTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	return &cowIteratorErrorCursor{UnsafeIterator: table.Table.NewIterator(start, end), err: table.err, closed: table.closed}
}

type cowIteratorErrorCursor struct {
	iterator.UnsafeIterator
	err    error
	closed *int
}

func (cursor *cowIteratorErrorCursor) Error() error { return cursor.err }
func (cursor *cowIteratorErrorCursor) Close() error {
	(*cursor.closed)++
	return cursor.UnsafeIterator.Close()
}
