package retainedalloc

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
	"unsafe"
)

// Pure owner/lease control: no DB, publication, enqueue or physical callback.
func TestNativeAtomicOwnerGrowthAdmitsBeforeEffects(t *testing.T) {
	var o Owner
	o.Initialize(256)
	first, last := &testBudget{cap: 1 << 20}, &testBudget{cap: 1 << 20}
	e1, err := o.Enroll(first)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := o.Enroll(last)
	if err != nil {
		t.Fatal(err)
	}
	before, pending, firstBefore, lastBefore := o.Bytes(), o.pending, first.bytes, last.bytes
	// One record cannot price the original binding visits and concrete
	// forward+rollback operations. Earlier admission must not resize a lease.
	tiny := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ready, err := o.AddPendingWithWork(64, tiny); ready || err != nil {
		t.Fatal(ready, err)
	}
	if o.Bytes() != before || o.pending != pending || first.bytes != firstBefore || last.bytes != lastBefore {
		t.Fatal("pre-effect refusal changed original accounting")
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := o.AddPendingWithWork(64, work); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if o.Bytes() != before+64 || o.pending != pending+64 || first.bytes != firstBefore+64 || last.bytes != lastBefore+64 {
		t.Fatal("original atomic growth was not applied")
	}
	expectedRecords := uint64(1 + 5*2)
	expectedBytes := 2*uint64(unsafe.Sizeof(Owner{})) + 2*uint64(unsafe.Sizeof(EnrollmentWork{})) + 12*uint64(unsafe.Sizeof(binding{})) + 4*(2*uint64(unsafe.Sizeof(testLease{}))+2*uint64(unsafe.Sizeof(testBudget{})))
	if work.Records != expectedRecords || work.Bytes != expectedBytes {
		t.Fatalf("price got %d/%d want %d/%d", work.Records, work.Bytes, expectedRecords, expectedBytes)
	}
	o.RemovePending(64)
	e1.Close()
	e2.Close()
}

func TestNativeAtomicOwnerGrowthPricesOriginalRollback(t *testing.T) {
	var o Owner
	o.Initialize(256)
	// Enroll inserts at the head; allow the first original resize, then
	// force the later lease to refuse. Original rollback must restore both.
	refusing, accepting := &testBudget{cap: 1 << 20}, &testBudget{cap: 1 << 20}
	e1, err := o.Enroll(refusing)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := o.Enroll(accepting)
	if err != nil {
		t.Fatal(err)
	}
	refusing.cap = refusing.bytes
	before, a, b := o.Bytes(), refusing.bytes, accepting.bytes
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := o.AddPendingWithWork(64, w); ready || err != ErrCapacity {
		t.Fatal(ready, err)
	}
	if o.Bytes() != before || o.pending != 0 || refusing.bytes != a || accepting.bytes != b {
		t.Fatal("original rollback lost accounting")
	}
	e1.Close()
	e2.Close()
}
