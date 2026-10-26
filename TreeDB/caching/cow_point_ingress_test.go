package caching

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

// The public point helper will reuse MVCC-aware view staging. A constructor
// denied by the original finite owner must retain its capacity error, even
// though its shared immutable refusal carrier is also marked closed.
func TestCOWPointIngressDeniedViewKeepsCapacityError(t *testing.T) {
	db := cowPointerFixture(t)
	before := db.cow.budget.Stats()
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - leaseBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	full := db.cow.budget.Stats()
	denied := db.NewBatchWithSize(1)
	if denied != &cowDeniedBatch {
		t.Fatal("finite constructor refusal did not return the immutable carrier")
	}
	defer denied.Close()
	for _, test := range []struct {
		name string
		set  func() error
	}{
		{"owned", func() error { return denied.Set([]byte("key"), []byte("value")) }},
		{"view", func() error { return denied.SetView([]byte("key"), []byte("value")) }},
		{"mvcc_view", func() error {
			return denied.SetViewWithMVCCInput([]byte("key"), []byte("value"), mvccadmission.Input{})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.set(); !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("constructor error=%v want ErrCOWCapacity", err)
			}
		})
	}
	called := false
	if err := denied.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error { called = true; return nil }); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("denied write=%v", err)
	}
	if called || len(denied.entries) != 0 {
		t.Fatal("denied carrier staged or appended")
	}
	if err := denied.Close(); err != nil {
		t.Fatal(err)
	}
	if err := denied.Close(); err != nil {
		t.Fatal(err)
	}
	if got := db.cow.budget.Stats(); got != full {
		t.Fatalf("denied carrier changed original admission: before=%+v after=%+v", full, got)
	}
	// The ordinary closed case remains different from a denied constructor.
	filler.Close()
	closed := db.NewBatchWithSize(1)
	if closed == &cowDeniedBatch {
		t.Fatal("constructor did not resume after releasing filler")
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.SetViewWithMVCCInput([]byte("key"), nil, mvccadmission.Input{}); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("closed live batch=%v", err)
	}
}
