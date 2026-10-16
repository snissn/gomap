package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestNamespaceProofCloseWorkActualHandlesManyCapturesAndFutureAdd(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	proof, err := NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner)
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
	admission, err := receiptBudget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NamespaceProofReleaseWork{}))))
	if err != nil {
		t.Fatal(err)
	}
	var receipt NamespaceProofReleaseWork
	if ready, err := receipt.Bind(proof, &iterator.OrdinalScanWork{}); ready || err != nil || proof.released.Load() {
		t.Fatal("zero bind changed owner", err)
	}
	bind := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := receipt.Bind(proof, &bind); !ready || err != nil {
		t.Fatal(err)
	}
	// This is the actual parent-alias transfer before containing refund.
	held := proof
	proof = nil
	if ready, err := receipt.ContinueWork(&iterator.OrdinalScanWork{}); ready || err != nil || held.released.Load() {
		t.Fatal("zero close changed handle", err)
	}
	if !errors.Is(held.ReleaseWithError(), retainedalloc.ErrWorkRequired) {
		t.Fatal("ordinary driver stole native receipt")
	}
	turns := 0
	for {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := receipt.ContinueWork(&w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual cleanup quantum", err)
		}
		if ready {
			break
		}
		turns++
		if turns > 100 {
			t.Fatal("permanent refusal")
		}
		if err = owner.AddPending(16); err != nil {
			t.Fatal(err)
		}
		future := make([]byte, 16)
		_ = future
	}
	if turns < 2 || receipt.proof != nil || held.parent != nil || held.persistence != nil || held.metadata != nil {
		t.Fatal("actual disposal/aliases incomplete")
	}
	if err = held.ReleaseWithError(); !errors.Is(err, retainedalloc.ErrWorkRequired) {
		t.Fatal(err)
	}
	owner.RemovePending(uint64(turns) * 16)
	for i, e := range captures {
		e.Close()
		budgets[i].Close()
		if budgets[i].Stats().TotalBytes != 0 {
			t.Fatal("capture admission leaked")
		}
	}
	owner.Close()
	admission.Close()
	receiptBudget.Close()
	if receiptBudget.Stats().TotalBytes != 0 || owner.Bytes() != 0 {
		t.Fatal("actual completed receipts retained admission")
	}
}
