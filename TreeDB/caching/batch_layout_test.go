package caching

import (
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestBatchManualStagingResetAndClose(t *testing.T) {
	var b Batch
	b.DisableStreamingBypass()
	b.Reserve(2)
	key, value := []byte("owned"), []byte("original")
	if err := b.Set(key, value); err != nil {
		t.Fatal(err)
	}
	key[0], value[0] = 'X', 'X'
	var got []batch.Entry
	if err := b.Replay(func(e batch.Entry) error { got = append(got, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Key) != "owned" || string(got[0].Value) != "original" {
		t.Fatalf("owned staging changed with caller: %+v", got)
	}
	state := b.ordinaryBatchState
	b.Reset()
	if state == nil || b.ordinaryBatchState != state {
		t.Fatal("reset replaced ordinary staging state")
	}
	b.DisableStreamingBypass()
	if err := b.SetView([]byte("second"), []byte("view")); err != nil {
		t.Fatal(err)
	}
	got = nil
	if err := b.Replay(func(e batch.Entry) error { got = append(got, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Key) != "second" || string(got[0].Value) != "view" {
		t.Fatalf("reset replay retained earlier operations: %+v", got)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("closed"), nil); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("closed staging accepted: %v", err)
	}
}

func TestCOWBatchCompactOwnerAdmissionAndClose(t *testing.T) {
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats().ExternalBytes
	// No entry/payload backing is requested: only the concrete coallocated
	// wrapper and its independent original budget receipt must be charged.
	b := db.newCOWBatch(0)
	if b.ordinaryBatchState != nil {
		t.Fatal("COW constructor materialized ordinary state")
	}
	want := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if got := db.cow.budget.Stats().ExternalBytes - baseline; got != want {
		t.Fatalf("wrapper charge=%d want actual owner and receipt=%d", got, want)
	}
	b.Reset()
	if got := db.cow.budget.Stats().ExternalBytes - baseline; got != want {
		t.Fatalf("reset changed live wrapper charge=%d want=%d", got, want)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if got := db.cow.budget.Stats().ExternalBytes; got != baseline {
		t.Fatalf("closed wrapper charge=%d want baseline=%d", got, baseline)
	}
	if err := b.Set([]byte("closed"), nil); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("closed COW staging accepted: %v", err)
	}
	t.Logf("Batch=%d ordinaryState=%d ordinaryOwner=%d COWState=%d COWOwner=%d", unsafe.Sizeof(Batch{}), unsafe.Sizeof(ordinaryBatchState{}), unsafe.Sizeof(ordinaryBatchOwner{}), unsafe.Sizeof(cowBatchState{}), unsafe.Sizeof(cowBatchOwner{}))
}

// Refused constructors share this carrier across callers, so even a transient
// lazy-state assignment would violate its immutable ownership contract.
func TestBatchClosedAndDeniedDoNotMaterializeOrdinaryState(t *testing.T) {
	key, value := []byte("key"), []byte("value")
	operations := []struct {
		name   string
		run    func(*Batch) error
		denied error
	}{
		{"Set", func(b *Batch) error { return b.Set(key, value) }, memtable.ErrCOWCapacity},
		{"SetView", func(b *Batch) error { return b.SetView(key, value) }, memtable.ErrCOWCapacity},
		{"SetViewValidated", func(b *Batch) error { return b.SetViewValidated(key, value) }, memtable.ErrCOWCapacity},
		{"Delete", func(b *Batch) error { return b.Delete(key) }, memtable.ErrCOWCapacity},
		{"DeleteView", func(b *Batch) error { return b.DeleteView(key) }, memtable.ErrCOWCapacity},
		{"DeleteViewValidated", func(b *Batch) error { return b.DeleteViewValidated(key) }, memtable.ErrCOWCapacity},
		{"SetOps", func(b *Batch) error { return b.SetOps([]batch.Entry{{Type: batch.OpPut, Key: key, Value: value}}) }, memtable.ErrCOWCapacity},
		{"DeleteRange", func(b *Batch) error { return b.DeleteRange(key, value) }, ErrBatchClosed},
		{"Write", func(b *Batch) error { return b.Write() }, memtable.ErrCOWCapacity},
		{"WriteSync", func(b *Batch) error { return b.WriteSync() }, memtable.ErrCOWCapacity},
	}
	before, beforeState := cowDeniedBatch, *cowDeniedBatch.cowState
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			closed := &Batch{closed: true}
			if err := op.run(closed); !errors.Is(err, ErrBatchClosed) {
				t.Fatalf("closed error=%v", err)
			}
			if closed.ordinaryBatchState != nil {
				t.Fatal("closed operation materialized ordinary state")
			}
			if err := op.run(&cowDeniedBatch); !errors.Is(err, op.denied) {
				t.Fatalf("denied error=%v want=%v", err, op.denied)
			}
		})
	}
	called := false
	if err := cowDeniedBatch.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error { called = true; return nil }); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal(err)
	}
	cowDeniedBatch.Reserve(8)
	cowDeniedBatch.Reset()
	if err := cowDeniedBatch.Close(); err != nil {
		t.Fatal(err)
	}
	if called || !reflect.DeepEqual(cowDeniedBatch, before) || !reflect.DeepEqual(*cowDeniedBatch.cowState, beforeState) {
		t.Fatal("shared refusal carrier changed or invoked callback")
	}
	closed := &Batch{closed: true}
	closed.Reserve(8)
	closed.Reset()
	if closed.ordinaryBatchState != nil || cap(closed.entries) != 0 {
		t.Fatal("closed cleanup or reserve materialized storage")
	}
	var empty Batch
	empty.Reset()
	if err := empty.Close(); err != nil || empty.ordinaryBatchState != nil {
		t.Fatalf("empty cleanup materialized ordinary state: %v", err)
	}
}

func TestCOWBatchLayoutPublicWriteResetAndHeldCut(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.NewBatchWithSize(1)
	defer b.Close()
	key := []byte("layout-key")
	if err := b.Set(key, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	old := db.AcquireSnapshot()
	defer old.Close()
	b.Reset()
	if err := b.Set(key, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if b.ordinaryBatchState != nil {
		t.Fatal("public COW write materialized ordinary state")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if value, err := old.Get(key); err != nil || string(value) != "first" {
		t.Fatalf("held cut value=%q err=%v", value, err)
	}
	if value, err := db.Get(key); err != nil || string(value) != "second" {
		t.Fatalf("current value=%q err=%v", value, err)
	}
	if b.ordinaryBatchState != nil {
		t.Fatal("COW close materialized ordinary state")
	}
}
