package caching

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
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

func mustFrozenInlineRootTable(tb testing.TB, prefix string, count int) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(count, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s/%06d", prefix, i)
		value := fmt.Sprintf("value-%06d", i)
		mt.Set([]byte(key), []byte(value))
	}
	mt.Freeze()
	return mt
}

func mustFrozenRawRootTable(tb testing.TB, kvs ...any) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	for i := 0; i+1 < len(kvs); i += 2 {
		key, ok := kvs[i].(string)
		if !ok {
			tb.Fatalf("key %d has type %T, want string", i, kvs[i])
		}
		var value []byte
		switch v := kvs[i+1].(type) {
		case string:
			value = []byte(v)
		case []byte:
			value = v
		default:
			tb.Fatalf("value %d has type %T, want string or []byte", i+1, kvs[i+1])
		}
		mt.Set([]byte(key), value)
	}
	mt.Freeze()
	return mt
}

func encodeRootDescriptorID(rootID uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, rootID)
	return out
}

func TestLeafGenerationGCOptionsIncludePublishedRoots(t *testing.T) {
	db := &DB{
		rootPublishedSet: &publishedRootSet{
			pointShards: []publishedRootRef{
				{rootID: 11},
				{rootID: 0},
				{rootID: 22},
				{rootID: 11},
			},
			system:   publishedRootRef{rootID: 33},
			iterator: publishedRootRef{rootID: 22},
		},
	}

	got := db.leafGenerationGCOptions().ProtectedRootIDs
	want := []uint64{11, 22, 33}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !uint64SlicesEqual(got, want) {
		t.Fatalf("ProtectedRootIDs=%v want %v", got, want)
	}
	gotSystem := db.leafGenerationGCOptions().ProtectedSystemRootIDs
	wantSystem := []uint64{33}
	if !uint64SlicesEqual(gotSystem, wantSystem) {
		t.Fatalf("ProtectedSystemRootIDs=%v want %v", gotSystem, wantSystem)
	}
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

func TestCollectValueLogLiveIDsUntil_IncludesPublishedSystemRootLeafRefs(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		ValueLog:                   backenddb.ValueLogOptions{PointerThreshold: 1},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:                           256 << 20,
		DisableWAL:                               true,
		RelaxedSync:                              true,
		AllowUnsafe:                              true,
		MemtableShards:                           1,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogMaxSegmentBytes:                  64 << 10,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationOff),
		ValueLogGenerationLeafSegmentTargetBytes: 64 << 10,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	skipRetainedPrune(db)

	rootTable := mustFrozenInlineRootTable(t, "sys/published", 1024)
	systemRootID, err := backend.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	if systemRootID == 0 {
		t.Fatal("expected non-zero published system root")
	}
	state := backend.State()
	if state.SystemRootPageID == systemRootID {
		t.Fatalf("test requires detached published system root, got state.SystemRootPageID=%d", state.SystemRootPageID)
	}
	leafRefs := collectLeafRefs(t, backend.Pager(), systemRootID)
	if len(leafRefs) == 0 {
		t.Fatalf("expected root %d to contain value-log leaf refs", systemRootID)
	}
	fileID := leafRefs[0].ValueLogFileID()

	liveBefore, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil(before): %v", err)
	}
	if _, ok := liveBefore[fileID]; ok {
		t.Fatalf("unexpected leaf-log file id %d before published system root install", fileID)
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 1,
		system: publishedRootRef{
			rootID: systemRootID,
		},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	liveAfter, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil(after): %v", err)
	}
	if _, ok := liveAfter[fileID]; !ok {
		t.Fatalf("expected published system root leaf ref to keep file id %d live", fileID)
	}
}

func TestCollectValueLogLiveIDsUntil_IncludesSystemDescriptorCollectionRootLeafRefs(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		ValueLog:                   backenddb.ValueLogOptions{PointerThreshold: 1},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:                           256 << 20,
		DisableWAL:                               true,
		RelaxedSync:                              true,
		AllowUnsafe:                              true,
		MemtableShards:                           1,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogMaxSegmentBytes:                  64 << 10,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationOff),
		ValueLogGenerationLeafSegmentTargetBytes: 64 << 10,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	skipRetainedPrune(db)

	_, rootIDs, err := backend.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenInlineRootTable(t, "collection/published", 1024).NewIterator(nil, nil),
		StoragePolicy: backenddb.OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenRawRootTable(t, "collections/root/users/primary", encodeRootDescriptorID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish descriptor-backed collection root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}
	leafRefs := collectLeafRefs(t, backend.Pager(), rootIDs[0])
	if len(leafRefs) == 0 {
		t.Fatalf("expected collection root %d to contain value-log leaf refs", rootIDs[0])
	}
	fileID := leafRefs[0].ValueLogFileID()
	if db.rootPublishedSet != nil {
		t.Fatalf("test requires descriptor-only reachability, rootPublishedSet=%+v", db.rootPublishedSet)
	}

	live, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil: %v", err)
	}
	if _, ok := live[fileID]; !ok {
		t.Fatalf("expected system descriptor collection root to keep leaf-log file id %d live", fileID)
	}
}

func TestCollectValueLogLiveIDsUntil_IncludesPublishedSystemDescriptorCollectionRootLeafRefs(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		ValueLog:                   backenddb.ValueLogOptions{PointerThreshold: 1},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:                           256 << 20,
		DisableWAL:                               true,
		RelaxedSync:                              true,
		AllowUnsafe:                              true,
		MemtableShards:                           1,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogMaxSegmentBytes:                  64 << 10,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationOff),
		ValueLogGenerationLeafSegmentTargetBytes: 64 << 10,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	skipRetainedPrune(db)

	collectionRootID, err := backend.PublishOrderedRootIterator(0, mustFrozenInlineRootTable(t, "collection/published", 1024).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish collection root: %v", err)
	}
	if collectionRootID == 0 {
		t.Fatal("expected non-zero collection root")
	}
	leafRefs := collectLeafRefs(t, backend.Pager(), collectionRootID)
	if len(leafRefs) == 0 {
		t.Fatalf("expected collection root %d to contain value-log leaf refs", collectionRootID)
	}
	fileID := leafRefs[0].ValueLogFileID()
	systemRootID, err := backend.PublishOrderedRootIterator(0, mustFrozenRawRootTable(t, "collections/root/users/published", encodeRootDescriptorID(collectionRootID)).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish detached system root: %v", err)
	}
	if systemRootID == 0 {
		t.Fatal("expected non-zero system root")
	}
	state := backend.State()
	if state.SystemRootPageID == systemRootID {
		t.Fatalf("test requires detached published system root, got state.SystemRootPageID=%d", state.SystemRootPageID)
	}

	liveBefore, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil(before): %v", err)
	}
	if _, ok := liveBefore[fileID]; ok {
		t.Fatalf("unexpected leaf-log file id %d before published system root install", fileID)
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 1,
		system: publishedRootRef{
			rootID: systemRootID,
		},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	liveAfter, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil(after): %v", err)
	}
	if _, ok := liveAfter[fileID]; !ok {
		t.Fatalf("expected published system descriptor collection root to keep leaf-log file id %d live", fileID)
	}
}

func TestCollectPublishedRootValueLogLiveIDsUntil_SkipsCurrentPublishedSystemDescriptorExpansion(t *testing.T) {
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

	collectionRootID, err := backend.PublishOrderedRootIterator(0, mustFrozenInlineRootTable(t, "collection/current", 4).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish collection root: %v", err)
	}
	systemRootID, err := backend.PublishSystemRootIterator(mustFrozenRawRootTable(t,
		"collections/root/users/current", encodeRootDescriptorID(collectionRootID),
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishSystemRootIterator: %v", err)
	}
	state := backend.State()
	if state == nil {
		t.Fatal("expected backend state")
	}
	if state.SystemRootPageID != systemRootID {
		t.Fatalf("SystemRootPageID=%d want %d", state.SystemRootPageID, systemRootID)
	}

	reader := newCachedLiveScanReader(valueReaderForBackendState(state), db.valueLogReader)
	var scanStats valueLogLiveIDScanStats
	err = db.collectPublishedRootValueLogLiveIDsUntil(context.Background(), backend.Pager(), reader, &publishedRootSet{
		system: publishedRootRef{rootID: systemRootID},
	}, state.RootPageID, state.SystemRootPageID, nil, map[uint32]struct{}{}, 0, &scanStats)
	if err != nil {
		t.Fatalf("collectPublishedRootValueLogLiveIDsUntil: %v", err)
	}
	if scanStats.Records != 0 {
		t.Fatalf("current published system descriptor expansion scanned %d records", scanStats.Records)
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
