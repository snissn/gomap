package caching

import (
	"errors"
	"sync"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestBorrowNativeSemanticCutZeroAllocAndConcurrentRace(t *testing.T) {
	db, c := cowCutFixtureWithExpectedCloseError(t, nil)
	c.ownerDB = db

	// Before semanticRootSet is installed, BorrowNativeSemanticCut must fail
	// closed with ErrNativeSemanticUnsupported so callers fall back to cold
	// catalog projection.
	if _, err := db.BorrowNativeSemanticCut(); err != backenddb.ErrNativeSemanticUnsupported {
		t.Fatalf("without semantic state err=%v want ErrNativeSemanticUnsupported", err)
	}

	state := &testSemanticState{}
	c.cut.semanticRootSet = state
	t.Cleanup(func() {
		c.cut.semanticRootSet = nil
	})

	allocs := testing.AllocsPerRun(1000, func() {
		cut, err := db.BorrowNativeSemanticCut()
		if err != nil {
			t.Fatalf("BorrowNativeSemanticCut: %v", err)
		}
		if cut.State() != state {
			t.Fatalf("unexpected state %#v", cut.State())
		}
		if err := cut.Release(); err != nil {
			t.Fatalf("Release: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("BorrowNativeSemanticCut+Release allocated %.2f objects/op, want 0", allocs)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				cut, err := db.BorrowNativeSemanticCut()
				if err != nil {
					t.Errorf("BorrowNativeSemanticCut: %v", err)
					return
				}
				if cut.State() != state {
					t.Errorf("state mismatch")
				}
				if err := cut.Release(); err != nil {
					t.Errorf("Release: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Exhaust in-flight COW budget: BorrowNativeSemanticCut must decline so
	// callers fall back to locked admission and observe ErrCOWCapacity.
	blocker, err := c.budget.AdmitNativeBytes(0)
	if err != nil {
		t.Fatalf("AdmitNativeBytes: %v", err)
	}
	if err := blocker.Resize(c.budget.Limits().MaxInFlightBytes - c.budget.Stats().ReservedBytes); err != nil {
		blocker.Close()
		t.Fatalf("Resize blocker: %v", err)
	}
	if _, err := db.BorrowNativeSemanticCut(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		blocker.Close()
		t.Fatalf("full budget BorrowNativeSemanticCut err=%v want ErrNativeSemanticUnsupported", err)
	}
	if _, err := c.budget.AcquireExternalValue(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeSemanticCapture{})))); !errors.Is(err, memtable.ErrCOWCapacity) {
		blocker.Close()
		t.Fatalf("full budget AcquireExternalValue err=%v want ErrCOWCapacity", err)
	}
	blocker.Close()

	// Once headroom returns, BorrowNativeSemanticCut succeeds lock-free again.
	cut, err := db.BorrowNativeSemanticCut()
	if err != nil {
		t.Fatalf("post-release BorrowNativeSemanticCut: %v", err)
	}
	if err := cut.Release(); err != nil {
		t.Fatalf("post-release Release: %v", err)
	}
}
