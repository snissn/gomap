package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type snapshotPublishedValueLookup struct {
	value []byte

	getEntryCalls       int
	getValueAppendCalls int
	getValueUnsafeCalls int
}

func snapshotGetAppendTestKey(key []byte) bool {
	return len(key) == 1 && key[0] == 'k'
}

func (l *snapshotPublishedValueLookup) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	l.getEntryCalls++
	if !snapshotGetAppendTestKey(key) {
		return nil, page.ValuePtr{}, 0, false
	}
	return l.value, page.ValuePtr{}, node.FlagInline, true
}

func (l *snapshotPublishedValueLookup) GetValueAppend(key, dst []byte) ([]byte, error) {
	l.getValueAppendCalls++
	if !snapshotGetAppendTestKey(key) {
		return dst, tree.ErrKeyNotFound
	}
	return append(dst, l.value...), nil
}

func (l *snapshotPublishedValueLookup) GetValueUnsafe(key []byte) ([]byte, error) {
	l.getValueUnsafeCalls++
	if !snapshotGetAppendTestKey(key) {
		return nil, tree.ErrKeyNotFound
	}
	return l.value, nil
}

type snapshotPublishedEntryOnlyLookup struct {
	value         []byte
	flags         byte
	getEntryCalls int
}

func (l *snapshotPublishedEntryOnlyLookup) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	l.getEntryCalls++
	if !snapshotGetAppendTestKey(key) {
		return nil, page.ValuePtr{}, 0, false
	}
	flags = l.flags
	if flags == 0 {
		flags = node.FlagInline
	}
	return l.value, page.ValuePtr{}, flags, true
}

type snapshotPublishedAppendMissLookup struct {
	value []byte
	flags byte

	getEntryCalls       int
	getValueAppendCalls int
	getValueUnsafeCalls int
}

func (l *snapshotPublishedAppendMissLookup) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	l.getEntryCalls++
	if !snapshotGetAppendTestKey(key) {
		return nil, page.ValuePtr{}, 0, false
	}
	flags = l.flags
	if flags == 0 {
		flags = node.FlagInline
	}
	return l.value, page.ValuePtr{}, flags, true
}

func (l *snapshotPublishedAppendMissLookup) GetValueAppend(_ []byte, dst []byte) ([]byte, error) {
	l.getValueAppendCalls++
	return dst, tree.ErrKeyNotFound
}

func (l *snapshotPublishedAppendMissLookup) GetValueUnsafe(_ []byte) ([]byte, error) {
	l.getValueUnsafeCalls++
	return nil, tree.ErrKeyNotFound
}

func TestSnapshotGetAppendPublishedUsesValueAppendDirectly(t *testing.T) {
	lookup := &snapshotPublishedValueLookup{value: []byte("published")}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       lookup,
			publishedRootID: 1,
		}},
	}

	got, err := snap.GetAppend([]byte("k"), []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(got) != "p:published" {
		t.Fatalf("value=%q, want p:published", got)
	}
	if lookup.getValueAppendCalls != 1 {
		t.Fatalf("GetValueAppend calls=%d, want 1", lookup.getValueAppendCalls)
	}
	if lookup.getEntryCalls != 0 {
		t.Fatalf("GetEntry calls=%d, want 0", lookup.getEntryCalls)
	}
	if lookup.getValueUnsafeCalls != 0 {
		t.Fatalf("GetValueUnsafe calls=%d, want 0", lookup.getValueUnsafeCalls)
	}
}

func TestSnapshotGetAppendPublishedValueAppendAllocs(t *testing.T) {
	if testRaceEnabled {
		t.Skip("AllocsPerRun is not stable under -race")
	}
	lookup := &snapshotPublishedValueLookup{value: []byte("published")}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       lookup,
			publishedRootID: 1,
		}},
	}
	key := []byte("k")
	prefix := []byte("p:")
	wantLen := len(prefix) + len(lookup.value)
	buf := make([]byte, len(prefix), wantLen)
	copy(buf, prefix)

	allocs := testing.AllocsPerRun(1000, func() {
		dst := buf[:len(prefix)]
		got, err := snap.GetAppend(key, dst)
		if err != nil {
			t.Fatalf("GetAppend: %v", err)
		}
		if len(got) != wantLen || got[0] != 'p' || got[1] != ':' || got[2] != 'p' {
			t.Fatalf("unexpected GetAppend value %q", got)
		}
	})
	if allocs > 0.5 {
		t.Fatalf("GetAppend allocs/run=%f, want 0", allocs)
	}
	if lookup.getEntryCalls != 0 {
		t.Fatalf("GetEntry calls=%d, want 0", lookup.getEntryCalls)
	}
	if lookup.getValueUnsafeCalls != 0 {
		t.Fatalf("GetValueUnsafe calls=%d, want 0", lookup.getValueUnsafeCalls)
	}
}

func TestSnapshotGetAppendPublishedFallsBackToEntryLookup(t *testing.T) {
	lookup := &snapshotPublishedEntryOnlyLookup{value: []byte("published")}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       lookup,
			publishedRootID: 1,
		}},
	}

	got, err := snap.GetAppend([]byte("k"), []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(got) != "p:published" {
		t.Fatalf("value=%q, want p:published", got)
	}
	if lookup.getEntryCalls != 1 {
		t.Fatalf("GetEntry calls=%d, want 1", lookup.getEntryCalls)
	}
}

func TestSnapshotGetAppendPublishedAppendMissFallsBackToEntryLookup(t *testing.T) {
	lookup := &snapshotPublishedAppendMissLookup{value: []byte("published")}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       lookup,
			publishedRootID: 1,
		}},
	}

	got, err := snap.GetAppend([]byte("k"), []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(got) != "p:published" {
		t.Fatalf("value=%q, want p:published", got)
	}
	if lookup.getValueAppendCalls != 1 {
		t.Fatalf("GetValueAppend calls=%d, want 1", lookup.getValueAppendCalls)
	}
	if lookup.getEntryCalls != 1 {
		t.Fatalf("GetEntry calls=%d, want 1", lookup.getEntryCalls)
	}
	if lookup.getValueUnsafeCalls != 0 {
		t.Fatalf("GetValueUnsafe calls=%d, want 0", lookup.getValueUnsafeCalls)
	}
}

func TestSnapshotGetAppendPublishedAppendMissPreservesTombstone(t *testing.T) {
	lookup := &snapshotPublishedAppendMissLookup{flags: node.FlagTombstone}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       lookup,
			publishedRootID: 1,
		}},
	}

	got, err := snap.GetAppend([]byte("k"), []byte("p:"))
	if !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend err=%v, want ErrKeyNotFound", err)
	}
	if string(got) != "p:" {
		t.Fatalf("value=%q, want unchanged prefix", got)
	}
	if lookup.getValueAppendCalls != 1 {
		t.Fatalf("GetValueAppend calls=%d, want 1", lookup.getValueAppendCalls)
	}
	if lookup.getEntryCalls != 1 {
		t.Fatalf("GetEntry calls=%d, want 1", lookup.getEntryCalls)
	}
	if lookup.getValueUnsafeCalls != 0 {
		t.Fatalf("GetValueUnsafe calls=%d, want 0", lookup.getValueUnsafeCalls)
	}
}

func TestSnapshotGetAppendBackendPublishedMissDoesNotFallBackToDefaultRoot(t *testing.T) {
	snap := newSnapshotWithBackendPublishedPointRootMissingKey(t)

	got, err := snap.GetAppend([]byte("k"), []byte("p:"))
	if !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend err=%v, want ErrKeyNotFound", err)
	}
	if string(got) != "p:" {
		t.Fatalf("value=%q, want unchanged prefix", got)
	}
}

func TestSnapshotBackendPublishedMissConsistentAcrossReadAPIs(t *testing.T) {
	snap := newSnapshotWithBackendPublishedPointRootMissingKey(t)

	if _, err := snap.Get([]byte("k")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("Get err=%v, want ErrKeyNotFound", err)
	}
	if _, err := snap.GetUnsafe([]byte("k")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetUnsafe err=%v, want ErrKeyNotFound", err)
	}
	ok, err := snap.Has([]byte("k"))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if ok {
		t.Fatal("Has=true, want false")
	}
	if _, err := snap.GetEntry([]byte("k")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetEntry err=%v, want ErrKeyNotFound", err)
	}
	if _, err := snap.GetEntryExact([]byte("k")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetEntryExact err=%v, want ErrKeyNotFound", err)
	}
}

func TestSnapshotBackendPublishedReadErrorsPropagate(t *testing.T) {
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       backendSnapshotLookup{},
			publishedRootID: 1,
		}},
	}
	if _, err := snap.Get([]byte("k")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("Get err=%v, want ErrClosed", err)
	}
	if _, err := snap.GetUnsafe([]byte("k")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("GetUnsafe err=%v, want ErrClosed", err)
	}
	if _, err := snap.GetAppend([]byte("k"), nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("GetAppend err=%v, want ErrClosed", err)
	}
	if _, err := snap.Has([]byte("k")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("Has err=%v, want ErrClosed", err)
	}
	if _, err := snap.GetEntry([]byte("k")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("GetEntry err=%v, want ErrClosed", err)
	}
	if _, err := snap.GetEntryExact([]byte("k")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("GetEntryExact err=%v, want ErrClosed", err)
	}
}

func TestSnapshotGetAppendBackendPublishedHitViaInstalledLookup(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	if err := backend.SetSync([]byte("k"), []byte("default-root")); err != nil {
		t.Fatalf("backend set: %v", err)
	}

	// Publish a backend root that contains the key we want to read.
	pubTable := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "from-published-root"})
	pointRootID, err := backend.PublishOrderedRootIterator(0, pubTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish root: %v", err)
	}
	if pointRootID == backend.State().RootPageID {
		t.Fatalf("test point root unexpectedly matches default root %d", pointRootID)
	}

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	// Install a published root set with only a rootID (no lookup object).
	// AcquireSnapshot must wire the backend lookup via installBackendPublishedRootLookups.
	db.mu.Lock()
	db.installPublishedRootSetLocked(&publishedRootSet{
		generation:  1,
		pointShards: []publishedRootRef{{rootID: pointRootID}},
	})
	db.mu.Unlock()

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	t.Cleanup(func() { _ = snap.Close() })

	// Verify the backend lookup was wired by installBackendPublishedRootLookups.
	if len(snap.backendPublishedLookups) == 0 {
		t.Fatal("expected backendPublishedLookups to be populated")
	}

	// GetAppend must read through the wired backend lookup without falling through
	// to the default-root backend fallback.
	got, err := snap.GetAppend([]byte("k"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(got) != "prefix:from-published-root" {
		t.Fatalf("GetAppend value=%q, want prefix:from-published-root", got)
	}
	entry, err := snap.GetEntry([]byte("k"))
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if string(entry.Value) != "from-published-root" {
		t.Fatalf("GetEntry value=%q, want from-published-root", entry.Value)
	}
}

func newSnapshotWithBackendPublishedPointRootMissingKey(t *testing.T) *Snapshot {
	t.Helper()

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	if err := backend.SetSync([]byte("k"), []byte("default")); err != nil {
		t.Fatalf("backend set: %v", err)
	}
	otherRoot := newRootDomainTestTable(t, rootDomainTestOp{key: "other", value: "published"})
	pointRootID, err := backend.PublishOrderedRootIterator(0, otherRoot.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish point root: %v", err)
	}
	if pointRootID == backend.State().RootPageID {
		t.Fatalf("test point root unexpectedly matches default root %d", pointRootID)
	}

	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		t.Fatal("expected backend snapshot")
	}
	t.Cleanup(func() { _ = backendSnap.Close() })

	db := &DB{backend: backend, mutableShards: make([]memShard, 1)}
	return &Snapshot{
		db:              db,
		backend:         backendSnap,
		rootPointShards: []rootDomainSnapshot{{publishedRootID: pointRootID}},
		backendRoot:     backendSnapshotLookup{db: db, snapshot: backendSnap, rootID: backendSnap.State().RootPageID},
		backendRootOK:   true,
	}
}
