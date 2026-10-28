package caching

import (
	"runtime"
	"sync"
	"testing"
	"weak"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type rootSnapshotLifetimeWitness struct {
	memtable.Table
	padding [128]byte
}

//go:noinline
func detachedSnapshotLifetimeWitness(fused bool) ([]rootDomainSnapshot, weak.Pointer[rootSnapshotLifetimeWitness]) {
	mutable := &rootSnapshotLifetimeWitness{}
	if fused {
		// Negative control: GC still scans the point half of the shared array.
		shared := make([]rootDomainSnapshot, 2)
		shared[0].mutable = mutable
		return shared[1:2:2], weak.Make(mutable)
	}
	view := &memtableView{rootPointShards: []rootDomainSnapshot{{mutable: mutable}}}
	return view.snapshotPointShards(), weak.Make(mutable)
}

func TestRootSnapshotDetachedMutableLifetime(t *testing.T) {
	for _, fused := range []bool{false, true} {
		shards, mutable := detachedSnapshotLifetimeWitness(fused)
		runtime.GC()
		if retained := mutable.Value() != nil; retained != fused {
			t.Fatalf("fused=%v mutable retained=%v", fused, retained)
		}
		runtime.KeepAlive(shards)
	}
}

//go:noinline
func releasedViewSnapshotWitness(t *testing.T) (*Snapshot, weak.Pointer[rootSnapshotLifetimeWitness]) {
	t.Helper()
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	mutable := &rootSnapshotLifetimeWitness{Table: memtable.NewAppendOnlyWithCapacityHint(0, 96)}
	db := &DB{backend: backend, mutableShards: []memShard{{mem: mutable}}}
	db.mu.Lock()
	db.installPublishedRootSetLocked(&publishedRootSet{generation: 1, pointShards: []publishedRootRef{{
		lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "old"}), rootID: 101,
	}}})
	db.mu.Unlock()
	snap := db.AcquireSnapshot()
	if snap == nil || snap.view != nil {
		t.Fatal("snapshot did not release its empty-queue view")
	}
	db.mu.Lock()
	db.mutableShards[0].mem = nil
	db.installPublishedRootSetLocked(&publishedRootSet{generation: 2, pointShards: []publishedRootRef{{
		lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "new"}), rootID: 102,
	}}})
	db.mu.Unlock()
	return snap, weak.Make(mutable)
}

func TestAcquireSnapshotReleasedViewDoesNotRetainMutable(t *testing.T) {
	snap, mutable := releasedViewSnapshotWitness(t)
	defer snap.Close()
	runtime.GC()
	if mutable.Value() != nil {
		t.Fatal("released-view snapshot retained the retired mutable memtable")
	}
	assertRootDomainVisibleValue(t, rootDomainSnapshotFromCachedSnapshot(snap, []byte("k")), "k", "old")
	runtime.KeepAlive(snap)
}

func TestRootSnapshotConcurrentMaterialization(t *testing.T) {
	view := &memtableView{rootPointShards: []rootDomainSnapshot{{publishedRootID: 41}}}
	const readers = 64
	got := make([][]rootDomainSnapshot, readers)
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i] = view.snapshotPointShards()
		}(i)
	}
	wg.Wait()
	for i := range got {
		if len(got[i]) != 1 || &got[i][0] != &got[0][0] || got[i][0].publishedRootID != 41 {
			t.Fatalf("reader %d did not receive the same initialized immutable snapshot", i)
		}
	}
}

// A view that is never used for a cached snapshot should not pay for a
// snapshot-only shard array. The retained view makes all publication allocations
// escape, matching the lifetime of real views.
func TestRootSnapshotPublicationAvoidsUnusedSnapshotAllocation(t *testing.T) {
	db := &DB{mutableShards: make([]memShard, 4), rootPointStates: make([]rootDomainState, 4)}
	var retained *memtableView
	allocs := testing.AllocsPerRun(100, func() {
		view := &memtableView{}
		db.publishRootDomainSnapshotsLocked(view)
		retained = view
	})
	if retained == nil {
		t.Fatal("view was not retained")
	}
	t.Logf("publication allocations: %v", allocs)
	if allocs > 3 {
		t.Fatalf("publication allocated %v times; want at most 3 without snapshot acquisition", allocs)
	}
}
