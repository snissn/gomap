package caching

import (
	"encoding/binary"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const maxRootGroupTestShardKeySearchAttempts = 1 << 20

func rootGroupTestShardKey(t *testing.T, db *DB, shard int) []byte {
	t.Helper()
	if db == nil {
		t.Fatal("nil db")
	}
	var key [8]byte
	for i := uint64(0); i < maxRootGroupTestShardKeySearchAttempts; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) == shard {
			return append([]byte(nil), key[:]...)
		}
	}
	t.Fatalf("rootGroupTestShardKey: could not find key for shard %d after %d attempts", shard, maxRootGroupTestShardKeySearchAttempts)
	return nil
}

func TestRootDomainSnapshotFromCachedSnapshot_UsesPerRootPublishedView(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 2),
		mutableShardMask: 1,
	}

	key0 := rootGroupTestShardKey(t, db, 0)
	key1 := rootGroupTestShardKey(t, db, 1)
	pub0 := newRootDomainTestTable(t, rootDomainTestOp{key: string(key0), value: "published-0"})
	pub1 := newRootDomainTestTable(t, rootDomainTestOp{key: string(key1), value: "published-1"})

	snap := &Snapshot{
		db: db,
		publishedRoots: &publishedRootSet{
			generation: 44,
			pointShards: []publishedRootRef{
				{lookup: pub0, rootID: 101},
				{lookup: pub1, rootID: 202},
			},
		},
	}

	root0 := rootDomainSnapshotFromCachedSnapshot(snap, key0)
	if root0.publishedRootID != 101 {
		t.Fatalf("root0 published id=%d want 101", root0.publishedRootID)
	}
	assertRootDomainVisibleValue(t, root0, string(key0), "published-0")

	root1 := rootDomainSnapshotFromCachedSnapshot(snap, key1)
	if root1.publishedRootID != 202 {
		t.Fatalf("root1 published id=%d want 202", root1.publishedRootID)
	}
	assertRootDomainVisibleValue(t, root1, string(key1), "published-1")
}

func TestRootDomainIteratorSnapshotFromCachedSnapshot_UsesPinnedPublishedSet(t *testing.T) {
	snap := &Snapshot{
		publishedRoots: &publishedRootSet{
			generation: 77,
			iterator: publishedRootRef{
				lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "published-a"}),
				rootID: 303,
			},
		},
	}

	iterSnap := rootDomainIteratorSnapshotFromCachedSnapshot(snap)
	if iterSnap.publishedRootID != 303 {
		t.Fatalf("iterator published id=%d want 303", iterSnap.publishedRootID)
	}
	assertRootDomainVisibleValue(t, iterSnap, "iter/a", "published-a")
}

func TestRootDomainSnapshotFromCachedSnapshot_PreservesPinnedRootIDWithoutLookup(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	snap := &Snapshot{
		db: db,
		publishedRoots: &publishedRootSet{
			pointShards: []publishedRootRef{{rootID: 404}},
			iterator:    publishedRootRef{rootID: 405},
			system:      publishedRootRef{rootID: 406},
		},
	}

	point := rootDomainSnapshotFromCachedSnapshot(snap, []byte("k"))
	if point.publishedRootID != 404 {
		t.Fatalf("point published id=%d want 404", point.publishedRootID)
	}
	iter := rootDomainIteratorSnapshotFromCachedSnapshot(snap)
	if iter.publishedRootID != 405 {
		t.Fatalf("iterator published id=%d want 405", iter.publishedRootID)
	}
	system := rootDomainSystemSnapshotFromCachedSnapshot(snap)
	if system.publishedRootID != 406 {
		t.Fatalf("system published id=%d want 406", system.publishedRootID)
	}
}

func TestAcquireSnapshot_PinsInstalledPublishedRootSet(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 2),
		mutableShardMask: 1,
	}

	key0 := rootGroupTestShardKey(t, db, 0)
	key1 := rootGroupTestShardKey(t, db, 1)
	setA := &publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key0), value: "a0"}), rootID: 101},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key1), value: "a1"}), rootID: 202},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "aiter"}),
			rootID: 303,
		},
	}
	setB := &publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key0), value: "b0"}), rootID: 111},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key1), value: "b1"}), rootID: 222},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "biter"}),
			rootID: 333,
		},
	}

	db.mu.Lock()
	db.installPublishedRootSetLocked(setA)
	db.mu.Unlock()

	snapA := db.AcquireSnapshot()
	if snapA == nil {
		t.Fatal("expected snapshot A")
	}
	defer snapA.Close()
	if snapA.view != nil {
		t.Fatal("expected snapshot A to copy published roots without retaining empty memtable view")
	}

	db.mu.Lock()
	db.installPublishedRootSetLocked(setB)
	db.mu.Unlock()

	snapB := db.AcquireSnapshot()
	if snapB == nil {
		t.Fatal("expected snapshot B")
	}
	defer snapB.Close()
	if snapB.view != nil {
		t.Fatal("expected snapshot B to copy published roots without retaining empty memtable view")
	}

	rootA0 := rootDomainSnapshotFromCachedSnapshot(snapA, key0)
	rootA1 := rootDomainSnapshotFromCachedSnapshot(snapA, key1)
	iterA := rootDomainIteratorSnapshotFromCachedSnapshot(snapA)
	assertRootDomainVisibleValue(t, rootA0, string(key0), "a0")
	assertRootDomainVisibleValue(t, rootA1, string(key1), "a1")
	assertRootDomainVisibleValue(t, iterA, "iter/a", "aiter")
	if rootA0.publishedRootID != 101 || rootA1.publishedRootID != 202 || iterA.publishedRootID != 303 {
		t.Fatalf("unexpected snapshot A published ids: %d %d %d", rootA0.publishedRootID, rootA1.publishedRootID, iterA.publishedRootID)
	}

	rootB0 := rootDomainSnapshotFromCachedSnapshot(snapB, key0)
	rootB1 := rootDomainSnapshotFromCachedSnapshot(snapB, key1)
	iterB := rootDomainIteratorSnapshotFromCachedSnapshot(snapB)
	assertRootDomainVisibleValue(t, rootB0, string(key0), "b0")
	assertRootDomainVisibleValue(t, rootB1, string(key1), "b1")
	assertRootDomainVisibleValue(t, iterB, "iter/a", "biter")
	if rootB0.publishedRootID != 111 || rootB1.publishedRootID != 222 || iterB.publishedRootID != 333 {
		t.Fatalf("unexpected snapshot B published ids: %d %d %d", rootB0.publishedRootID, rootB1.publishedRootID, iterB.publishedRootID)
	}
}

func TestAcquireSnapshot_PinsInstalledPublishedRootSetIncludingSystem(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	setA := &publishedRootSet{
		generation: 31,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "a"}), rootID: 311},
		},
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/k", value: "sys-a"}), rootID: 312,
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "iter-a"}), rootID: 313,
		},
	}
	setB := &publishedRootSet{
		generation: 32,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "b"}), rootID: 321},
		},
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/k", value: "sys-b"}), rootID: 322,
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "iter-b"}), rootID: 323,
		},
	}

	db.mu.Lock()
	db.installPublishedRootSetLocked(setA)
	db.mu.Unlock()

	snapA := db.AcquireSnapshot()
	if snapA == nil {
		t.Fatal("expected snapshot A")
	}
	defer snapA.Close()

	db.mu.Lock()
	db.installPublishedRootSetLocked(setB)
	db.mu.Unlock()

	snapB := db.AcquireSnapshot()
	if snapB == nil {
		t.Fatal("expected snapshot B")
	}
	defer snapB.Close()

	systemA := rootDomainSystemSnapshotFromCachedSnapshot(snapA)
	systemB := rootDomainSystemSnapshotFromCachedSnapshot(snapB)
	assertRootDomainVisibleValue(t, systemA, "sys/k", "sys-a")
	assertRootDomainVisibleValue(t, systemB, "sys/k", "sys-b")
	if systemA.publishedRootID != 312 {
		t.Fatalf("systemA published id=%d want 312", systemA.publishedRootID)
	}
	if systemB.publishedRootID != 322 {
		t.Fatalf("systemB published id=%d want 322", systemB.publishedRootID)
	}
}

func TestAcquireSnapshot_PinsGroupedWarmPublishAcrossNonSystemRoots(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()
	oldPointTable := newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "old-p"})
	oldPointRootID, err := backend.PublishOrderedRootIterator(0, oldPointTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish old point root: %v", err)
	}
	oldIterTable := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "old-i"})
	oldIterRootID, err := backend.PublishOrderedRootIterator(0, oldIterTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish old iterator root: %v", err)
	}
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPointTable,
			publishedRootID: oldPointRootID,
		}},
		rootIteratorState: rootDomainState{
			published:       oldIterTable,
			publishedRootID: oldIterRootID,
		},
	}
	setA := &publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: oldPointTable, rootID: oldPointRootID},
		},
		iterator: publishedRootRef{
			lookup: oldIterTable, rootID: oldIterRootID,
		},
	}
	setB := &publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}), rootID: oldPointRootID},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}), rootID: oldIterRootID,
		},
	}

	db.mu.Lock()
	db.installPublishedRootSetLocked(setA)
	db.mu.Unlock()

	snapA := db.AcquireSnapshot()
	if snapA == nil {
		t.Fatal("expected snapshot A")
	}
	defer snapA.Close()

	db.mu.Lock()
	db.rootPointStates[0].immutables = []memtable.Table{
		newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}),
	}
	db.rootIteratorState.immutables = []memtable.Table{
		newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}),
	}
	db.mu.Unlock()

	if err := db.publishInstalledRootSet(setB); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	snapB := db.AcquireSnapshot()
	if snapB == nil {
		t.Fatal("expected snapshot B")
	}
	defer snapB.Close()

	rootA := rootDomainSnapshotFromCachedSnapshot(snapA, []byte("primary/doc"))
	iterA := rootDomainIteratorSnapshotFromCachedSnapshot(snapA)
	assertRootDomainVisibleValue(t, rootA, "primary/doc", "old-p")
	assertRootDomainVisibleValue(t, iterA, "iter/a", "old-i")

	rootB := rootDomainSnapshotFromCachedSnapshot(snapB, []byte("primary/doc"))
	iterB := rootDomainIteratorSnapshotFromCachedSnapshot(snapB)
	assertRootDomainVisibleValue(t, rootB, "primary/doc", "new-p")
	assertRootDomainVisibleValue(t, iterB, "iter/a", "new-i")
	if rootB.publishedRootID == rootA.publishedRootID {
		t.Fatal("expected point root id to advance")
	}
	if iterB.publishedRootID == iterA.publishedRootID {
		t.Fatal("expected iterator root id to advance")
	}
}

func TestAcquireSnapshot_PinsGroupedWarmPublishAcrossMixedSystemAndNonSystemRoots(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	oldSystemTable := newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "old-s"})
	oldSystemRootID, err := backend.PublishSystemRootIterator(oldSystemTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish old system root: %v", err)
	}
	oldPointTable := newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "old-p"})
	oldPointRootID, err := backend.PublishOrderedRootIterator(0, oldPointTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish old point root: %v", err)
	}
	oldIterTable := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "old-i"})
	oldIterRootID, err := backend.PublishOrderedRootIterator(0, oldIterTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish old iterator root: %v", err)
	}
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPointTable,
			publishedRootID: oldPointRootID,
		}},
		rootSystemState: rootDomainState{
			published:       oldSystemTable,
			publishedRootID: oldSystemRootID,
		},
		rootIteratorState: rootDomainState{
			published:       oldIterTable,
			publishedRootID: oldIterRootID,
		},
	}
	setA := &publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: oldPointTable, rootID: oldPointRootID},
		},
		system: publishedRootRef{
			lookup: oldSystemTable, rootID: oldSystemRootID,
		},
		iterator: publishedRootRef{
			lookup: oldIterTable, rootID: oldIterRootID,
		},
	}
	setB := &publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}), rootID: oldPointRootID},
		},
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}), rootID: oldSystemRootID,
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}), rootID: oldIterRootID,
		},
	}

	db.mu.Lock()
	db.installPublishedRootSetLocked(setA)
	db.mu.Unlock()

	snapA := db.AcquireSnapshot()
	if snapA == nil {
		t.Fatal("expected snapshot A")
	}
	defer snapA.Close()

	db.mu.Lock()
	db.rootPointStates[0].immutables = []memtable.Table{
		newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}),
	}
	db.rootSystemState.immutables = []memtable.Table{
		newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}),
	}
	db.rootIteratorState.immutables = []memtable.Table{
		newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}),
	}
	db.mu.Unlock()

	if err := db.publishInstalledRootSet(setB); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	snapB := db.AcquireSnapshot()
	if snapB == nil {
		t.Fatal("expected snapshot B")
	}
	defer snapB.Close()

	systemA := rootDomainSystemSnapshotFromCachedSnapshot(snapA)
	rootA := rootDomainSnapshotFromCachedSnapshot(snapA, []byte("primary/doc"))
	iterA := rootDomainIteratorSnapshotFromCachedSnapshot(snapA)
	assertRootDomainVisibleValue(t, systemA, "sys/catalog", "old-s")
	assertRootDomainVisibleValue(t, rootA, "primary/doc", "old-p")
	assertRootDomainVisibleValue(t, iterA, "iter/a", "old-i")

	systemB := rootDomainSystemSnapshotFromCachedSnapshot(snapB)
	rootB := rootDomainSnapshotFromCachedSnapshot(snapB, []byte("primary/doc"))
	iterB := rootDomainIteratorSnapshotFromCachedSnapshot(snapB)
	assertRootDomainVisibleValue(t, systemB, "sys/catalog", "new-s")
	assertRootDomainVisibleValue(t, rootB, "primary/doc", "new-p")
	assertRootDomainVisibleValue(t, iterB, "iter/a", "new-i")
	if systemB.publishedRootID == systemA.publishedRootID {
		t.Fatal("expected system root id to advance")
	}
	if rootB.publishedRootID == rootA.publishedRootID {
		t.Fatal("expected point root id to advance")
	}
	if iterB.publishedRootID == iterA.publishedRootID {
		t.Fatal("expected iterator root id to advance")
	}
}

func TestAcquireSnapshot_FallsBackToBackendPublishedSetWithoutInstalledGroup(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	if err := backend.SetSync([]byte("k"), []byte("backend-v")); err != nil {
		t.Fatalf("backend set: %v", err)
	}

	db := &DB{
		backend:       backend,
		mutableShards: make([]memShard, 1),
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	rootSnap := rootDomainSnapshotFromCachedSnapshot(snap, []byte("k"))
	assertRootDomainVisibleValue(t, rootSnap, "k", "backend-v")
	if rootSnap.publishedRootID == 0 {
		t.Fatal("expected backend published root id")
	}
	lookup, ok := rootSnap.published.(*backendSnapshotLookup)
	if !ok {
		t.Fatalf("published lookup type=%T, want *backendSnapshotLookup", rootSnap.published)
	}
	if lookup != &snap.backendRoot {
		t.Fatal("expected point fallback to reuse snapshot backendRoot lookup")
	}
	if stats := db.rootDomainPublishStatsSnapshot(); stats.backendFallbacks != 1 {
		t.Fatalf("backendFallbacks=%d want 1", stats.backendFallbacks)
	}
}

func TestAcquireSnapshot_BackendFallbackPinsSystemRootPageID(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db := &DB{
		backend:       backend,
		mutableShards: make([]memShard, 1),
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	systemSnap := rootDomainSystemSnapshotFromCachedSnapshot(snap)
	if got, want := systemSnap.publishedRootID, backend.State().SystemRootPageID; got != want {
		t.Fatalf("system published root id=%d want %d", got, want)
	}
	lookup, ok := systemSnap.published.(*backendSnapshotLookup)
	if !ok {
		t.Fatalf("system published lookup type=%T, want *backendSnapshotLookup", systemSnap.published)
	}
	if lookup != &snap.backendSystem {
		t.Fatal("expected system fallback to reuse snapshot backendSystem lookup")
	}
}

func TestAcquireSnapshot_InstallsBackendLookupForPublishedPointRoots(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	pointTable := newRootDomainTestTable(t, rootDomainTestOp{key: "published/k", value: "published-v"})
	pointRootID, err := backend.PublishOrderedRootIterator(0, pointTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish point root: %v", err)
	}
	if pointRootID == backend.State().RootPageID {
		t.Fatalf("test point root unexpectedly matches default root %d", pointRootID)
	}

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	view := &memtableView{
		rootSnapshotShards: []rootDomainSnapshot{{}},
		publishedRoots: &publishedRootSet{
			pointShards: []publishedRootRef{{rootID: pointRootID}},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	if snap.publishedRoots == view.publishedRoots {
		t.Fatal("expected snapshot to clone published root set before installing lookups")
	}
	if got := len(snap.backendPublishedLookups); got != 1 {
		t.Fatalf("backendPublishedLookups len=%d want 1", got)
	}

	rootSnap := rootDomainSnapshotFromCachedSnapshot(snap, []byte("published/k"))
	lookup, ok := rootSnap.published.(*backendSnapshotLookup)
	if !ok {
		t.Fatalf("published lookup type=%T, want *backendSnapshotLookup", rootSnap.published)
	}
	if lookup != &snap.backendPublishedLookups[0] {
		t.Fatal("expected point published ref to use snapshot-owned backend lookup")
	}
	if lookup.rootID != pointRootID {
		t.Fatalf("lookup rootID=%d want %d", lookup.rootID, pointRootID)
	}
	if lookup.snapshot != snap.backend {
		t.Fatal("expected published lookup to use acquired backend snapshot")
	}
	if lookup.db != db {
		t.Fatal("expected published lookup to retain parent db")
	}
}

func TestAcquireSnapshot_InstallsBackendLookupForPublishedSystemAndIteratorRoots(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	systemTable := newRootDomainTestTable(t, rootDomainTestOp{key: "system/k", value: "system-v"})
	systemRootID, err := backend.PublishOrderedRootIterator(0, systemTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish system root: %v", err)
	}
	iteratorTable := newRootDomainTestTable(t, rootDomainTestOp{key: "iterator/k", value: "iterator-v"})
	iteratorRootID, err := backend.PublishOrderedRootIterator(0, iteratorTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish iterator root: %v", err)
	}
	if state := backend.State(); state != nil {
		if systemRootID == state.RootPageID || systemRootID == state.SystemRootPageID {
			t.Fatalf("test system root unexpectedly matches static root state: %d", systemRootID)
		}
		if iteratorRootID == state.RootPageID || iteratorRootID == state.SystemRootPageID {
			t.Fatalf("test iterator root unexpectedly matches static root state: %d", iteratorRootID)
		}
	}

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	view := &memtableView{
		publishedRoots: &publishedRootSet{
			system:   publishedRootRef{rootID: systemRootID},
			iterator: publishedRootRef{rootID: iteratorRootID},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	if got := len(snap.backendPublishedLookups); got != 2 {
		t.Fatalf("backendPublishedLookups len=%d want 2", got)
	}
	systemLookup, ok := snap.publishedRoots.system.lookup.(*backendSnapshotLookup)
	if !ok {
		t.Fatalf("system lookup type=%T, want *backendSnapshotLookup", snap.publishedRoots.system.lookup)
	}
	iteratorLookup, ok := snap.publishedRoots.iterator.lookup.(*backendSnapshotLookup)
	if !ok {
		t.Fatalf("iterator lookup type=%T, want *backendSnapshotLookup", snap.publishedRoots.iterator.lookup)
	}
	if systemLookup.rootID != systemRootID {
		t.Fatalf("system lookup rootID=%d want %d", systemLookup.rootID, systemRootID)
	}
	if iteratorLookup.rootID != iteratorRootID {
		t.Fatalf("iterator lookup rootID=%d want %d", iteratorLookup.rootID, iteratorRootID)
	}
	if systemLookup == iteratorLookup {
		t.Fatal("expected distinct system and iterator lookup slots")
	}
}

func TestAcquireSnapshot_InstallsBackendLookupForDirectPublishedPointRootID(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	pointTable := newRootDomainTestTable(t, rootDomainTestOp{key: "published/k", value: "published-v"})
	pointRootID, err := backend.PublishOrderedRootIterator(0, pointTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish point root: %v", err)
	}
	if pointRootID == backend.State().RootPageID {
		t.Fatalf("test point root unexpectedly matches default root %d", pointRootID)
	}

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	view := &memtableView{
		queue:              []memtable.Table{newRootDomainTestTable(t, rootDomainTestOp{key: "queued/k", value: "queued-v"})},
		rootSnapshotShards: []rootDomainSnapshot{{publishedRootID: pointRootID}},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	if got := len(snap.backendPublishedLookups); got != 1 {
		t.Fatalf("backendPublishedLookups len=%d want 1", got)
	}
	if snap.publishedRoots != nil {
		t.Fatal("expected direct published root ID to avoid synthesizing a published root set")
	}
	if view.rootSnapshotShards[0].published != nil {
		t.Fatal("expected snapshot acquisition not to mutate retained memtable view root snapshot")
	}
	lookup, ok := snap.rootPointShards[0].published.(*backendSnapshotLookup)
	if !ok {
		t.Fatalf("published lookup type=%T, want *backendSnapshotLookup", snap.rootPointShards[0].published)
	}
	if lookup != &snap.backendPublishedLookups[0] {
		t.Fatal("expected direct point root to use snapshot-owned backend lookup")
	}
	if lookup.rootID != pointRootID {
		t.Fatalf("lookup rootID=%d want %d", lookup.rootID, pointRootID)
	}
}

func TestAcquireSnapshot_InstalledPublishedPointRootAllocsBounded(t *testing.T) {
	if testRaceEnabled {
		t.Skip("AllocsPerRun is not stable under -race")
	}
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	pointTable := newRootDomainTestTable(t, rootDomainTestOp{key: "published/k", value: "published-v"})
	pointRootID, err := backend.PublishOrderedRootIterator(0, pointTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish point root: %v", err)
	}
	if pointRootID == backend.State().RootPageID {
		t.Fatalf("test point root unexpectedly matches default root %d", pointRootID)
	}

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	view := &memtableView{
		rootSnapshotShards: []rootDomainSnapshot{{}},
		publishedRoots: &publishedRootSet{
			pointShards: []publishedRootRef{{rootID: pointRootID}},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	warm := db.AcquireSnapshot()
	if warm == nil {
		t.Fatal("warm AcquireSnapshot=nil")
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("warm Close: %v", err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot=nil")
		}
		if len(snap.backendPublishedLookups) != 1 {
			t.Fatalf("backendPublishedLookups len=%d want 1", len(snap.backendPublishedLookups))
		}
		if err := snap.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	if allocs > 5.1 {
		t.Fatalf("AcquireSnapshot allocs/run=%f, want <= 5.1 with installed published point root", allocs)
	}
}
