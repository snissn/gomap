package caching

import (
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Distinct captures share one exact basis, not a retained current Collection
// snapshot. A later physical handoff cannot replace the old capture's root.
func TestCOWNativeBasisWorkspaceLifetime(t *testing.T) {
	db := cowPointerFixture(t)
	db.valueLogThreshold = 64
	if err := db.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	a, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	basis, budget := a.cowCut.basis, db.cow.budget
	if b.cowCut.basis != basis {
		t.Fatal("fresh captures do not share immutable basis")
	}
	token, ok := basis.snapshot.StateToken()
	if !ok {
		t.Fatal("missing basis")
	}
	// Real same-budget preallocation refusal: no reusable partial is installed.
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = basis.nativeEntryAtRoot(budget, token.RootPageID, []byte("a"), true); !errors.Is(err, memtable.ErrCOWCapacity) || basis.reader != nil {
		t.Fatalf("admission refusal: %v", err)
	}
	pressure.Close()
	check := func() {
		t.Helper()
		entry, err := basis.nativeEntryAtRoot(budget, token.RootPageID, []byte("a"), true)
		if err != nil || string(entry.Value) != "old" || cap(entry.Value) != len(entry.Value) {
			t.Errorf("old physical read: %q %v", entry.Value, err)
			return
		}
		entry.Value[0] = '!'
	}
	check()
	workspace, point, receipt := basis.reader, basis.reader.point, basis.reader.lease
	var wg sync.WaitGroup
	entered := make(chan struct{}, 8)
	resume := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			check()
			entered <- struct{}{}
			<-resume
			for j := 0; j < 16; j++ {
				check()
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-entered
	}
	close(resume)
	// Close one capture while distinct retained captures continue shared reads.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if basis.reader != workspace || basis.reader.point != point {
		t.Fatal("fresh read reallocated physical workspace")
	}
	if err := db.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check()
	if db.cow.cut.basis == basis {
		t.Fatal("physical handoff reused old basis")
	}
	// Only b now retains old basis. Clear observer aliases before final release.
	workspace, point, basis = nil, nil, nil
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("last old-basis owner did not refund workspace")
	}
	receipt = nil
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 {
		t.Fatal("final original ownership", stats)
	}
}
