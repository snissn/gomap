//go:build treedb_freelist_instrument

package freelist

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func validateAllocatorPageOperationCounts(pageID uint64, got, want allocatorPageOperationCounts) error {
	if got != want {
		return fmt.Errorf("page %d operations = %+v, want %+v", pageID, got, want)
	}
	return nil
}

func assertAllocatorPageOperationCounts(t *testing.T, a *Allocator, pageID uint64, want allocatorPageOperationCounts) {
	t.Helper()
	if err := validateAllocatorPageOperationCounts(pageID, a.pageOperationCounts(pageID), want); err != nil {
		t.Fatal(err)
	}
}

func performObservedPageCycle(t *testing.T, a *Allocator, pageID uint64) {
	t.Helper()
	data, err := a.batchGetForWrite(pageID)
	if err != nil {
		t.Fatalf("GetForWrite(%d): %v", pageID, err)
	}
	n := node.NewNode(data)
	if !a.batchVerifyChecksum(pageID, n) {
		t.Fatalf("VerifyChecksum(%d) = false", pageID)
	}
	a.batchUpdateChecksum(pageID, n)
}

func TestAllocator_PageOperationGateDetectsActualDuplicateCalls(t *testing.T) {
	a, _ := newAllocatorForFreeManyTest(t, 6)
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	headID := a.Head()
	a.resetPageOperationCounts()

	performObservedPageCycle(t, a, headID)
	performObservedPageCycle(t, a, headID)

	got := a.pageOperationCounts(headID)
	wantDuplicated := allocatorPageOperationCounts{gets: 2, verifies: 2, updates: 2}
	if err := validateAllocatorPageOperationCounts(headID, got, wantDuplicated); err != nil {
		t.Fatal(err)
	}
	if err := validateAllocatorPageOperationCounts(headID, got, allocatorPageOperationCounts{gets: 1, verifies: 1, updates: 1}); err == nil {
		t.Fatal("one-operation gate accepted deliberately duplicated real operations")
	}
}

func TestAllocator_FreeMany_ObservedOperationsMatchTouchedPages(t *testing.T) {
	a, _ := newAllocatorForFreeManyTest(t, page.MaxFreeIDs+8)
	seed := make([]uint64, page.MaxFreeIDs-1)
	for i := range seed {
		seed[i] = uint64(i + 1)
	}
	if err := a.FreeMany(seed); err != nil {
		t.Fatalf("seed FreeMany: %v", err)
	}
	ids := []uint64{uint64(page.MaxFreeIDs - 1), uint64(page.MaxFreeIDs), uint64(page.MaxFreeIDs + 1), uint64(page.MaxFreeIDs + 2)}
	a.resetPageOperationCounts()

	if err := a.FreeMany(ids); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}

	assertAllocatorPageOperationCounts(t, a, seed[0], allocatorPageOperationCounts{gets: 1, verifies: 1, updates: 1})
	assertAllocatorPageOperationCounts(t, a, ids[2], allocatorPageOperationCounts{gets: 1, updates: 1})
}

func TestAllocator_FreeMany_FullTransitionHeadObservedOnce(t *testing.T) {
	a, _ := newAllocatorForFreeManyTest(t, page.MaxFreeIDs+3)
	seed := make([]uint64, page.MaxFreeIDs+1)
	for i := range seed {
		seed[i] = uint64(i + 1)
	}
	if err := a.FreeMany(seed); err != nil {
		t.Fatalf("seed FreeMany: %v", err)
	}
	fullHead := a.Head()
	a.resetPageOperationCounts()

	if err := a.FreeMany([]uint64{uint64(page.MaxFreeIDs + 2)}); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}

	assertAllocatorPageOperationCounts(t, a, fullHead, allocatorPageOperationCounts{gets: 1, verifies: 1, updates: 1})
}

func TestAllocator_RegionAllocMany_DrainedAndEmptyHeadsObservedOnce(t *testing.T) {
	freed := make([]uint64, page.MaxFreeIDs+3)
	for i := range freed {
		freed[i] = uint64(i + 1)
	}
	a, p := newAllocatorForFreeManyTest(t, len(freed)+1)
	a.SetFreelistRegion(10, 1)
	if err := a.FreeMany(freed); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	newestHead := a.Head()
	olderHead := freelistNextPageID(mustAllocatorPage(t, p, newestHead))
	a.resetPageOperationCounts()

	got, err := a.AllocMany(3, freed[len(freed)-1])
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	if len(got) != 3 || got[1] != newestHead {
		t.Fatalf("AllocMany = %v, want body ID, recycled head %d, then older body ID", got, newestHead)
	}
	assertAllocatorPageOperationCounts(t, a, newestHead, allocatorPageOperationCounts{gets: 1, verifies: 1, updates: 1})
	assertAllocatorPageOperationCounts(t, a, olderHead, allocatorPageOperationCounts{gets: 1, verifies: 1, updates: 1})
}

func TestAllocator_InstrumentationDoesNotReplaceCorruptionResult(t *testing.T) {
	a, p := newAllocatorForFreeManyTest(t, 5)
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	headID := a.Head()
	data, err := p.GetForWrite(headID)
	if err != nil {
		t.Fatalf("GetForWrite: %v", err)
	}
	data[page.PageHeaderSize] ^= 0xff
	a.resetPageOperationCounts()

	err = a.FreeMany([]uint64{1, 3})
	var batchErr *FreeManyError
	if err == nil || !errors.As(err, &batchErr) {
		t.Fatal("FreeMany succeeded with a corrupt freelist head")
	}
	if batchErr.Processed != 0 || batchErr.Err == nil || batchErr.Err.Error() != "freelist head corrupted (FreeMany)" {
		t.Fatalf("corruption error = %+v, want unchanged corruption result with zero processed IDs", batchErr)
	}
	assertAllocatorPageOperationCounts(t, a, headID, allocatorPageOperationCounts{gets: 1, verifies: 1})
}

func TestAllocator_InstrumentationConcurrentObservation(t *testing.T) {
	a, _ := newAllocatorForFreeManyTest(t, 64)
	if err := a.FreeMany([]uint64{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatalf("seed FreeMany: %v", err)
	}
	a.SetFreelistRegion(4, 1)
	a.resetPageOperationCounts()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				ids, err := a.AllocMany(2, 4)
				if err != nil {
					t.Errorf("AllocMany: %v", err)
					return
				}
				if err := a.FreeMany(ids); err != nil {
					t.Errorf("FreeMany: %v", err)
					return
				}
			}
		}()
	}
	for range 1000 {
		_ = a.allPageOperationCounts()
	}
	wg.Wait()
}
