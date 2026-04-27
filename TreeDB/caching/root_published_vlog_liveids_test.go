package caching

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func mustFrozenPointerRootTable(tb testing.TB, key string, ptr page.ValuePtr) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	mt.SetEntry([]byte(key), nil, ptr, node.FlagPointer)
	mt.Freeze()
	return mt
}

func mustBackendUserPointer(tb testing.TB, backend *backenddb.DB, key []byte) page.ValuePtr {
	tb.Helper()
	snap := backend.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected backend snapshot")
	}
	defer snap.Close()
	entry, err := snap.GetEntry(key)
	if err != nil {
		tb.Fatalf("GetEntry(%q): %v", key, err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		tb.Fatalf("entry %q is not a pointer", key)
	}
	return entry.ValuePtr
}

func TestCollectValueLogLiveIDsUntil_IncludesPublishedNonSystemRoots(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	skipRetainedPrune(db)

	value := bytes.Repeat([]byte("x"), 2<<10)
	if err := db.Set([]byte("k1"), value); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(k1): %v", err)
	}
	ptr := mustBackendUserPointer(t, backend, []byte("k1"))
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete(k1): %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(delete): %v", err)
	}

	liveBefore, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil(before): %v", err)
	}
	if _, ok := liveBefore[ptr.FileID]; ok {
		t.Fatalf("unexpected live id %d before published non-system root install", ptr.FileID)
	}

	rootTable := mustFrozenPointerRootTable(t, "root/p", ptr)
	db.rootPointStates = []rootDomainState{{
		immutables: []memtable.Table{rootTable},
	}}
	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{{
			lookup: rootTable,
			rootID: 0,
		}},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	liveAfter, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil(after): %v", err)
	}
	if _, ok := liveAfter[ptr.FileID]; !ok {
		t.Fatalf("expected published non-system root to keep file id %d live", ptr.FileID)
	}
}

func TestPruneRetainedValueLogs_PublishedNonSystemRootsKeepClosedSegment(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	skipRetainedPrune(db)

	value := bytes.Repeat([]byte("y"), 2<<10)
	if err := db.Set([]byte("k1"), value); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(k1): %v", err)
	}
	ptr := mustBackendUserPointer(t, backend, []byte("k1"))
	path := db.valueLogReader.SegmentPath(ptr.FileID)
	if path == "" {
		t.Fatalf("missing segment path for file id %d", ptr.FileID)
	}
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	db.markValueLogRetain(path)

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete(k1): %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(delete): %v", err)
	}

	rootTable := mustFrozenPointerRootTable(t, "root/p", ptr)
	db.rootPointStates = []rootDomainState{{
		immutables: []memtable.Table{rootTable},
	}}
	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{{
			lookup: rootTable,
			rootID: 0,
		}},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	db.pruneRetainedValueLogs(false)
	if !db.valueLogRetained(path) {
		t.Fatalf("expected retained path %q to remain pinned by published non-system root", path)
	}
}
