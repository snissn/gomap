package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
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

func TestManagerContainingRefundAfterOriginalUnlock(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "Close", true: "LastWrapper"}[held], func(t *testing.T) {
			registry := rootpublication.NewIdentityPinRegistry()
			m, err := NewManagerWithStableResourcePinRegistry(t.TempDir(), registry)
			if err != nil {
				t.Fatal(err)
			}
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			observed, locked := false, false
			capture, err := registry.MetadataOwner().Enroll(&containingUnlockBudget{actual: budget, observe: func() {
				if m.producerMetadata != nil {
					return
				}
				observed = true
				if !m.mu.TryLock() {
					locked = true
				} else {
					m.mu.Unlock()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			if held && !m.RetainSnapshotWrapper() {
				t.Fatal("missing original wrapper borrow")
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			if held {
				if observed {
					t.Fatal("containing refund preceded last wrapper release")
				}
				m.ReleaseSnapshotWrapper()
			}
			if !observed || locked {
				t.Fatalf("containing refund before original unlock: observed=%t locked=%t", observed, locked)
			}
			capture.Close()
			budget.Close()
		})
	}
}
func TestManagerWrapperOriginalManyGovernors(t *testing.T) {
	registry := rootpublication.NewIdentityPinRegistry()
	owner := registry.MetadataOwner()
	m, err := NewManagerWithStableResourcePinRegistry(t.TempDir(), registry)
	if err != nil {
		t.Fatal(err)
	}
	budgets := make([]*memtable.COWBudget, 80)
	captures := make([]*retainedalloc.Enrollment, len(budgets))
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
	if !m.RetainSnapshotWrapper() {
		t.Fatal("missing original borrower")
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if m.producerCharge == 0 {
		t.Fatal("refunded before original borrower release")
	}
	m.ReleaseSnapshotWrapper()
	if m.producerCharge != 0 {
		t.Fatal("retained disposed Manager")
	}
	for i, e := range captures {
		e.Close()
		budgets[i].Close()
		if budgets[i].Stats().TotalBytes != 0 {
			t.Fatal("capture governor leaked")
		}
	}
}
