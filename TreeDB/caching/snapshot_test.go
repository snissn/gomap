package caching

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type publishedAppendMissWithPrefix struct {
	prefix []byte
}

func (p publishedAppendMissWithPrefix) GetEntry(_ []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	return nil, page.ValuePtr{}, 0, false
}

func (p publishedAppendMissWithPrefix) GetValueAppend(_ []byte, dst []byte) ([]byte, error) {
	return append(dst, p.prefix...), tree.ErrKeyNotFound
}

func (p publishedAppendMissWithPrefix) GetValueUnsafe(_ []byte) ([]byte, error) {
	return nil, tree.ErrKeyNotFound
}

type publishedAppendMissNilOutput struct{}

func (publishedAppendMissNilOutput) GetEntry(_ []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	return nil, page.ValuePtr{}, 0, false
}

func (publishedAppendMissNilOutput) GetValueAppend(_ []byte, _ []byte) ([]byte, error) {
	return nil, tree.ErrKeyNotFound
}

func (publishedAppendMissNilOutput) GetValueUnsafe(_ []byte) ([]byte, error) {
	return nil, tree.ErrKeyNotFound
}

type publishedAppendHit struct {
	value       []byte
	appendCalls int
}

func (p *publishedAppendHit) GetEntry(_ []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	return nil, page.ValuePtr{}, 0, false
}

func (p *publishedAppendHit) GetValueAppend(_ []byte, dst []byte) ([]byte, error) {
	p.appendCalls++
	return append(dst, p.value...), nil
}

func (p *publishedAppendHit) GetValueUnsafe(_ []byte) ([]byte, error) {
	return append([]byte(nil), p.value...), nil
}

type publishedBackendLookupMissCounter struct {
	appendCalls int
	entryCalls  int
}

func (publishedBackendLookupMissCounter) rootDomainPublishedBackendLookupMarker() {}

func (p *publishedBackendLookupMissCounter) GetEntry(_ []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	p.entryCalls++
	return nil, page.ValuePtr{}, 0, false
}

func (p *publishedBackendLookupMissCounter) GetValueAppend(_ []byte, dst []byte) ([]byte, error) {
	p.appendCalls++
	return dst, tree.ErrKeyNotFound
}

func (p *publishedBackendLookupMissCounter) GetValueUnsafe(_ []byte) ([]byte, error) {
	return nil, tree.ErrKeyNotFound
}

func newSnapshotGetAppendBackendFixture(t *testing.T, seedKey, seedValue []byte) (*Snapshot, func()) {
	t.Helper()
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := Open(dir, backend, Options{FlushThreshold: 1024 * 1024})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if seedKey != nil {
		if err := cached.SetSync(seedKey, seedValue); err != nil {
			_ = cached.Close()
			_ = backend.Close()
			t.Fatalf("SetSync: %v", err)
		}
		if err := cached.Checkpoint(); err != nil {
			_ = cached.Close()
			_ = backend.Close()
			t.Fatalf("Checkpoint: %v", err)
		}
	}
	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		_ = cached.Close()
		_ = backend.Close()
		t.Fatal("AcquireSnapshot=nil")
	}
	snap := &Snapshot{
		db:      cached,
		backend: backendSnap,
	}
	cleanup := func() {
		_ = snap.Close()
		_ = cached.Close()
		_ = backend.Close()
	}
	return snap, cleanup
}

func TestAcquireSnapshot_NotifyErrorOnRotateFailure(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb-snapshot-rotate-fail-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	errCh := make(chan error, 1)
	cached, err := Open(dir, backend, Options{
		FlushThreshold: 1024 * 1024,
		NotifyError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	if err := cached.SetSync([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	// Force the snapshot rotation path to fail inside newMutableMemtableWithCapacityMode.
	cached.storeMemtableMode(memtable.Mode(255))

	if snap := cached.AcquireSnapshot(); snap != nil {
		_ = snap.Close()
		t.Fatalf("AcquireSnapshot()=%v want nil on rotate failure", snap)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected non-nil notify error")
		}
	default:
		t.Fatalf("expected NotifyError to be called")
	}
}

func TestIteratorSnapshotIsolation(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb-snapshot-iso-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	// Use a large threshold so we rely on the mutable memtable
	cached, err := Open(dir, backend, Options{FlushThreshold: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	// 1. Write "a"
	if err := cached.SetSync([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	// 2. Open Iterator (should capture snapshot)
	it, err := cached.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	// 3. Write "b" (should not be seen by 'it')
	if err := cached.SetSync([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	// 4. Verify Iterator
	foundA := false
	foundB := false

	for it.Valid() {
		k := string(it.Key())
		if k == "a" {
			foundA = true
		}
		if k == "b" {
			foundB = true
		}
		it.Next()
	}

	if !foundA {
		t.Error("Iterator missed pre-existing key 'a'")
	}
	if foundB {
		t.Error("Iterator saw key 'b' written AFTER iterator creation (Snapshot Isolation violation)")
	}
}

func TestAcquireSnapshot_CapturesPublishedRootDomainVersion(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb-root-domain-version-")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 2,
	})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	if err := cached.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	snap1 := cached.AcquireSnapshot()
	if snap1 == nil {
		t.Fatal("expected snapshot")
	}
	defer snap1.Close()
	if snap1.rootVersion == 0 {
		t.Fatal("expected non-zero rootVersion")
	}

	if err := cached.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("set b: %v", err)
	}
	snap2 := cached.AcquireSnapshot()
	if snap2 == nil {
		t.Fatal("expected second snapshot")
	}
	defer snap2.Close()

	if snap2.rootVersion <= snap1.rootVersion {
		t.Fatalf("rootVersion did not advance: snap1=%d snap2=%d", snap1.rootVersion, snap2.rootVersion)
	}
}

func TestAcquireSnapshot_AllocsBoundedAfterWarmPath(t *testing.T) {
	if testRaceEnabled {
		t.Skip("AllocsPerRun is not stable under -race")
	}
	dir, err := os.MkdirTemp("", "treedb-snapshot-allocs-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{FlushThreshold: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	warm := cached.AcquireSnapshot()
	if warm == nil {
		t.Fatal("warm AcquireSnapshot=nil")
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("warm Close: %v", err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		snap := cached.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot=nil")
		}
		if err := snap.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	// Distinct exported cached and backend handles are the two intentional
	// allocations. Their addresses cannot be reused safely while callers may
	// retain a closed pointer.
	if allocs > 2.1 {
		t.Fatalf("AcquireSnapshot allocs/run=%f, want <= 2.1 after warm path", allocs)
	}
}

func TestAcquireBackendSnapshotFastPath_AvailableAfterCheckpoint(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	key := []byte("fast-path-key")
	if err := cached.Set(key, []byte("persisted")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	snap := cached.AcquireBackendSnapshotFastPath()
	if snap == nil {
		t.Fatal("AcquireBackendSnapshotFastPath=nil after checkpointed empty cached state")
	}
	defer snap.Close()
	got, err := snap.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("backend fast-path GetAppend: %v", err)
	}
	if string(got) != "persisted" {
		t.Fatalf("backend fast-path value=%q want persisted", string(got))
	}
}

func TestAcquireBackendSnapshotFastPath_RejectsMutableCachedWrites(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	key := []byte("mutable-key")
	if err := cached.Set(key, []byte("cached")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if snap := cached.AcquireBackendSnapshotFastPath(); snap != nil {
		_ = snap.Close()
		t.Fatal("AcquireBackendSnapshotFastPath returned snapshot with mutable cached write")
	}

	snap := cached.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	got, err := snap.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("cached snapshot GetAppend: %v", err)
	}
	if string(got) != "cached" {
		t.Fatalf("cached snapshot value=%q want cached", string(got))
	}
}

func TestAcquireBackendSnapshotFastPath_RejectsDirtyBackendValueLogState(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	cached.backendReadVlogFlushedSeq.Store(1)
	cached.backendReadVlogDirtySeq.Store(2)
	if snap := cached.AcquireBackendSnapshotFastPath(); snap != nil {
		_ = snap.Close()
		t.Fatal("AcquireBackendSnapshotFastPath returned snapshot with dirty backend value-log state")
	}

	cached.backendReadVlogFlushedSeq.Store(2)
	snap := cached.AcquireBackendSnapshotFastPath()
	if snap == nil {
		t.Fatal("AcquireBackendSnapshotFastPath=nil after dirty value-log state marked flushed")
	}
	defer snap.Close()
}

type dirtyDuringAcquireBackend struct {
	BackendDB
	snapper   backendSnapshotProvider
	onAcquire func()
}

func (b *dirtyDuringAcquireBackend) AcquireSnapshot() *db.Snapshot {
	if b.onAcquire != nil {
		b.onAcquire()
	}
	return b.snapper.AcquireSnapshot()
}

func TestAcquireBackendSnapshotFastPath_RejectsValueLogDirtyRace(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	cached.backendReadVlogDirtySeq.Store(1)
	cached.backendReadVlogFlushedSeq.Store(1)
	wrapped := &dirtyDuringAcquireBackend{
		BackendDB: cached.backend,
		snapper:   backend,
		onAcquire: func() {
			cached.backendReadVlogDirtySeq.Add(1)
		},
	}
	cached.backend = wrapped
	if snap := cached.AcquireBackendSnapshotFastPath(); snap != nil {
		_ = snap.Close()
		t.Fatal("AcquireBackendSnapshotFastPath returned snapshot after value-log state became dirty during backend acquire")
	}

	cached.backendReadVlogFlushedSeq.Store(cached.backendReadVlogDirtySeq.Load())
	wrapped.onAcquire = nil
	snap := cached.AcquireBackendSnapshotFastPath()
	if snap == nil {
		t.Fatal("AcquireBackendSnapshotFastPath=nil after dirty race was marked flushed")
	}
	defer snap.Close()
}

func TestAcquireBackendSnapshotFastPath_RejectsPublishedRootState(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableShards: 1})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	published := newRootDomainTestTable(t, rootDomainTestOp{key: "root-key", value: "published"})
	cached.mu.Lock()
	ok := cached.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: published, rootID: 101},
		},
	})
	cached.mu.Unlock()
	if !ok {
		t.Fatal("installPublishedRootSetLocked=false")
	}

	if snap := cached.AcquireBackendSnapshotFastPath(); snap != nil {
		_ = snap.Close()
		t.Fatal("AcquireBackendSnapshotFastPath returned snapshot with published root state")
	}

	snap := cached.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	if snap.view != nil {
		t.Fatal("published-root snapshot should not retain empty memtable view")
	}
	rootSnap := rootDomainSnapshotFromCachedSnapshot(snap, []byte("root-key"))
	assertRootDomainVisibleValue(t, rootSnap, "root-key", "published")
}

func TestSnapshotClose_Idempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb-snapshot-close-idempotent-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{FlushThreshold: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	snap := cached.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	next := cached.AcquireSnapshot()
	if next == nil {
		t.Fatal("second AcquireSnapshot=nil")
	}
	if err := next.Close(); err != nil {
		t.Fatalf("close second snapshot: %v", err)
	}
}

func TestSnapshotGetAppend_PublishedLookupWithoutAppendSupport(t *testing.T) {
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{
			{
				published: newRootDomainTestTable(t,
					rootDomainTestOp{key: "k1", value: "published-v1"},
					rootDomainTestOp{key: "k2", tombstone: true},
				),
			},
		},
	}

	got, err := snap.GetAppend([]byte("k1"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend(k1): %v", err)
	}
	if want := "prefix:published-v1"; string(got) != want {
		t.Fatalf("GetAppend(k1)=%q want %q", string(got), want)
	}

	_, err = snap.GetAppend([]byte("k2"), nil)
	if !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend(k2) err=%v want ErrKeyNotFound", err)
	}
}

func TestSnapshotGetAppend_PublishedAppendHitReturnsWithoutFallback(t *testing.T) {
	published := &publishedAppendHit{value: []byte("published-v")}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{
			{published: published},
		},
	}

	got, err := snap.GetAppend([]byte("k"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend(k): %v", err)
	}
	if want := "prefix:published-v"; string(got) != want {
		t.Fatalf("GetAppend(k)=%q want %q", string(got), want)
	}
	if got, want := published.appendCalls, 1; got != want {
		t.Fatalf("published append calls=%d want %d", got, want)
	}
}

func TestSnapshotGetAppend_PublishedMissFallsBackToBackend(t *testing.T) {
	snap, cleanup := newSnapshotGetAppendBackendFixture(t, []byte("k"), []byte("backend-v"))
	defer cleanup()
	published := newRootDomainTestTable(t, rootDomainTestOp{key: "other", value: "v"})

	snap.rootPointShards = []rootDomainSnapshot{
		{publishedRootID: 1, published: published},
	}
	snap.publishedRoots = &publishedRootSet{
		pointShards: []publishedRootRef{{lookup: published, rootID: 1}},
	}
	got, err := snap.GetAppend([]byte("k"), nil)
	if err != nil {
		t.Fatalf("GetAppend(k): %v", err)
	}
	if want := "backend-v"; string(got) != want {
		t.Fatalf("GetAppend(k)=%q want %q", string(got), want)
	}
}

func TestSnapshotGetAppend_PublishedMissFallbackTruncatesDst(t *testing.T) {
	snap, cleanup := newSnapshotGetAppendBackendFixture(t, []byte("k"), []byte("backend-v"))
	defer cleanup()
	published := publishedAppendMissWithPrefix{prefix: []byte("bad:")}

	snap.rootPointShards = []rootDomainSnapshot{
		{publishedRootID: 1, published: published},
	}
	snap.publishedRoots = &publishedRootSet{
		pointShards: []publishedRootRef{{lookup: published, rootID: 1}},
	}
	got, err := snap.GetAppend([]byte("k"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend(k): %v", err)
	}
	if want := "prefix:backend-v"; string(got) != want {
		t.Fatalf("GetAppend(k)=%q want %q", string(got), want)
	}
}

func TestSnapshotGetAppend_PublishedMissFallbackHandlesShortErrorOutput(t *testing.T) {
	snap, cleanup := newSnapshotGetAppendBackendFixture(t, []byte("k"), []byte("backend-v"))
	defer cleanup()
	published := publishedAppendMissNilOutput{}

	snap.rootPointShards = []rootDomainSnapshot{
		{publishedRootID: 1, published: published},
	}
	snap.publishedRoots = &publishedRootSet{
		pointShards: []publishedRootRef{{lookup: published, rootID: 1}},
	}
	got, err := snap.GetAppend([]byte("k"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend(k): %v", err)
	}
	if want := "prefix:backend-v"; string(got) != want {
		t.Fatalf("GetAppend(k)=%q want %q", string(got), want)
	}
}

func TestSnapshotGetAppend_BackendLookupMissWithoutPublishedRootsReturnsNotFound(t *testing.T) {
	snap, cleanup := newSnapshotGetAppendBackendFixture(t, []byte("present"), []byte("value"))
	defer cleanup()
	_, err := snap.GetAppend([]byte("missing"), []byte("prefix:"))
	if !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend(missing) err=%v want ErrKeyNotFound", err)
	}
}

func TestShouldShortCircuitPublishedAppendMiss_BackendLookupOnly(t *testing.T) {
	if !shouldShortCircuitPublishedAppendMiss(true, nil, rootDomainSnapshot{published: backendSnapshotLookup{}}) {
		t.Fatal("backend lookup miss should short-circuit duplicate backend probes")
	}
	if shouldShortCircuitPublishedAppendMiss(true, nil, rootDomainSnapshot{published: publishedAppendMissWithPrefix{prefix: []byte("bad:")}}) {
		t.Fatal("non-backend published lookup should not short-circuit backend fallback")
	}
	if shouldShortCircuitPublishedAppendMiss(true, &publishedRootSet{}, rootDomainSnapshot{published: backendSnapshotLookup{}}) {
		t.Fatal("pinned published roots should not use nil-root short-circuit")
	}
	if shouldShortCircuitPublishedAppendMiss(false, nil, rootDomainSnapshot{published: backendSnapshotLookup{}}) {
		t.Fatal("unchecked published misses should not short-circuit")
	}
}

func TestSnapshotGetAppend_BackendPublishedMissSkipsEntryProbe(t *testing.T) {
	lookup := &publishedBackendLookupMissCounter{}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{
			{published: lookup},
		},
	}

	_, err := snap.GetAppend([]byte("missing"), []byte("prefix:"))
	if !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend(missing) err=%v want ErrKeyNotFound", err)
	}
	if got, want := lookup.appendCalls, 1; got != want {
		t.Fatalf("published GetValueAppend calls=%d want %d", got, want)
	}
	if got := lookup.entryCalls; got != 0 {
		t.Fatalf("published GetEntry calls=%d want 0", got)
	}
}

func TestSnapshotGetAppend_NilReceiverMissReturnsNotFound(t *testing.T) {
	var snap *Snapshot

	dst := []byte("prefix:")
	got, err := snap.GetAppend([]byte("missing"), dst)
	if !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend(missing) err=%v want ErrKeyNotFound", err)
	}
	if string(got) != "prefix:" {
		t.Fatalf("GetAppend(missing) dst=%q want %q", string(got), "prefix:")
	}
}

func TestSnapshotGet_PublishedEmptyInlineValueReturnsNonNilEmpty(t *testing.T) {
	published := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: ""})
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{
			{publishedRootID: 1, published: published},
		},
		publishedRoots: &publishedRootSet{
			pointShards: []publishedRootRef{{lookup: published, rootID: 1}},
		},
	}

	got, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("Get(k)=%#v want non-nil empty value", got)
	}
}

func TestSnapshotGetAppend_RootBoundPublishedMissDoesNotFallbackToDefaultRoot(t *testing.T) {
	dir := t.TempDir()

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{FlushThreshold: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	if err := cached.SetSync([]byte("other"), []byte("v1")); err != nil {
		t.Fatalf("SetSync(other): %v", err)
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(other): %v", err)
	}
	rootMiss := backend.State().RootPageID
	if rootMiss == 0 {
		t.Fatal("rootMiss=0")
	}
	if err := cached.SetSync([]byte("k"), []byte("backend-v")); err != nil {
		t.Fatalf("SetSync(k): %v", err)
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(k): %v", err)
	}
	if backend.State().RootPageID == rootMiss {
		t.Fatal("expected new default root after checkpoint")
	}

	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	publishedLookup := backendSnapshotLookup{db: cached, snapshot: backendSnap, rootID: rootMiss}
	snap := &Snapshot{
		db:      cached,
		backend: backendSnap,
		rootPointShards: []rootDomainSnapshot{
			{publishedRootID: rootMiss, published: publishedLookup},
		},
		publishedRoots: &publishedRootSet{
			pointShards: []publishedRootRef{{lookup: publishedLookup, rootID: rootMiss}},
		},
	}
	defer func() { _ = snap.Close() }()

	_, err = snap.GetAppend([]byte("k"), nil)
	if !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend(k) err=%v want ErrKeyNotFound", err)
	}
}
