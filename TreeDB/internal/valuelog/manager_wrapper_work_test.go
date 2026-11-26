package valuelog

import (
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

func TestManagerWrapperConcurrentRetainReleaseRacingCloseAndRefresh(t *testing.T) {
	for trial := 0; trial < 25; trial++ {
		registry := rootpublication.NewIdentityPinRegistry()
		budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		capture, err := registry.MetadataOwner().Enroll(budget)
		if err != nil {
			t.Fatal(err)
		}
		m, err := NewManagerWithStableResourcePinRegistry(t.TempDir(), registry)
		if err != nil {
			t.Fatal(err)
		}
		if m.producerCharge == 0 {
			t.Fatal("expected nonzero producer charge")
		}
		baseSet := m.CurrentSetNoRefresh()

		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				<-start
				for i := 0; i < 200; i++ {
					if m.RetainSnapshotWrapper() {
						m.Acquire(baseSet)
						if err := m.Release(baseSet); err != nil {
							t.Errorf("trial %d Release(baseSet): %v", trial, err)
						}
						m.ReleaseSnapshotWrapper()
					}
					if id == 0 && i%25 == 0 {
						_ = m.Refresh()
					}
				}
			}(g)
		}

		var closeErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			closeErr = m.Close()
		}()

		close(start)
		wg.Wait()

		if closeErr != nil {
			t.Fatalf("trial %d Close: %v", trial, closeErr)
		}
		if err := m.Release(baseSet); err != nil {
			t.Fatalf("trial %d final Release(baseSet): %v", trial, err)
		}
		if m.RetainSnapshotWrapper() {
			t.Fatalf("trial %d RetainSnapshotWrapper succeeded after Close", trial)
		}
		if got := m.wrapperBorrows.Load(); got != 0 {
			t.Fatalf("trial %d wrapperBorrows=%d want 0", trial, got)
		}
		if m.producerCharge != 0 || m.producerMetadata != nil {
			t.Fatalf("trial %d terminal metadata not refunded: charge=%d owner=%v", trial, m.producerCharge, m.producerMetadata)
		}
		capture.Close()
		budget.Close()
		if got := budget.Stats().TotalBytes; got != 0 {
			t.Fatalf("trial %d budget leaked %d bytes", trial, got)
		}
	}
}

func TestManagerReleaseSetConcurrentFastPathAndFinalDrop(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	set := m.CurrentSetNoRefresh()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.Acquire(set)
				if err := m.Release(set); err != nil {
					t.Errorf("concurrent Release: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := set.RefCount.Load(); got != 1 {
		t.Fatalf("set.RefCount=%d want 1 before final release", got)
	}
	if err := m.Release(set); err != nil {
		t.Fatalf("final Release: %v", err)
	}
	if got := set.RefCount.Load(); got != 0 || set.Files != nil {
		t.Fatalf("after final release: RefCount=%d Files=%v", got, set.Files)
	}
	if err := m.Release(set); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("over-release err=%v want ErrResourceOwnership", err)
	}
}
