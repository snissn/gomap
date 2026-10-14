package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
	"unsafe"
)

func TestManagerWrapperActualContainingBorrowDefersCloseRefund(t *testing.T) {
	registry := rootpublication.NewIdentityPinRegistry()
	m, err := NewManagerWithStableResourcePinRegistry(t.TempDir(), registry)
	if err != nil {
		t.Fatal(err)
	}
	charge := m.producerCharge
	if charge == 0 || !m.RetainSnapshotWrapper() {
		t.Fatal("missing admitted original Manager borrow")
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if m.producerCharge != charge || m.producerMetadata == nil {
		t.Fatal("Manager refunded while original Snapshot containing alias remained")
	}
	m.ReleaseSnapshotWrapper()
	if m.producerCharge != 0 || m.wrapperBorrows.Load() != 0 {
		t.Fatal("original actual last containing borrower did not refund")
	}
}
func TestManagerWrapperBoundedOriginalManyGovernorsAndFutureProducer(t *testing.T) {
	registry := rootpublication.NewIdentityPinRegistry()
	owner := registry.MetadataOwner()
	m, err := NewManagerWithStableResourcePinRegistry(t.TempDir(), registry)
	if err != nil {
		t.Fatal(err)
	}
	budgets := make([]*memtable.COWBudget, 80)
	captures := make([]*retainedalloc.Enrollment, 80)
	for i := range budgets {
		budgets[i], err = memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		captures[i], err = owner.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	receiptBudget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	receiptAdmission, err := receiptBudget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(ManagerWrapperReleaseWork{}))))
	if err != nil {
		t.Fatal(err)
	}
	var receipt ManagerWrapperReleaseWork
	if !m.RetainSnapshotWrapper() {
		t.Fatal("missing original borrower")
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if ready, err := receipt.BindSnapshotWrapper(m, &iterator.OrdinalScanWork{}); ready || err != nil || m.wrapperBorrows.Load() != 1 {
		t.Fatal("zero work captured disposal", err)
	}
	bind := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := receipt.BindSnapshotWrapper(m, &bind); !ready || err != nil {
		t.Fatal("bind original receipt", err)
	}
	turns := 0
	for {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := receipt.ReleaseWork(&w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("original finite quantum", err)
		}
		if ready {
			break
		}
		turns++
		if turns > 100 {
			t.Fatal("permanent original lease refusal")
		}
		if err = m.Close(); err != nil {
			t.Fatal(err)
		}
		// The SAME registry producer may grow after a bounded refund returns.
		if err = owner.AddPending(16); err != nil {
			t.Fatal(err)
		}
		future := make([]byte, 16)
		_ = future
	}
	if turns < 2 || m.producerCharge != 0 || receipt.manager != nil {
		t.Fatal("original many-budget disposal was unbounded or retained disposed Manager")
	}
	owner.RemovePending(uint64(turns) * 16)
	for i, e := range captures {
		e.Close()
		budgets[i].Close()
		if budgets[i].Stats().TotalBytes != 0 {
			t.Fatal("capture governor leaked")
		}
	}
	receiptAdmission.Close()
	receiptBudget.Close()
	if receiptBudget.Stats().TotalBytes != 0 {
		t.Fatal("actual completed external receipt admission leaked")
	}
}
