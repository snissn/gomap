package caching

import (
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestSnapshotIterator_QueueValueOverridesPublishedAndTombstoneHidesPublished(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	if err := backend.SetSync([]byte("a"), []byte("backend_a")); err != nil {
		t.Fatalf("backend set a: %v", err)
	}
	if err := backend.SetSync([]byte("b"), []byte("backend_b")); err != nil {
		t.Fatalf("backend set b: %v", err)
	}
	if err := backend.SetSync([]byte("c"), []byte("backend_c")); err != nil {
		t.Fatalf("backend set c: %v", err)
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open caching db: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("b"), []byte("queue_b")); err != nil {
		t.Fatalf("set queued b: %v", err)
	}
	if err := db.Delete([]byte("c")); err != nil {
		t.Fatalf("delete queued c: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("snapshot iterator: %v", err)
	}

	if err := db.Set([]byte("d"), []byte("post_open_queue")); err != nil {
		t.Fatalf("post-open queued set: %v", err)
	}
	if err := backend.SetSync([]byte("e"), []byte("post_open_backend")); err != nil {
		t.Fatalf("post-open backend set: %v", err)
	}

	var gotKeys []string
	values := make(map[string]string)
	for it.Valid() {
		k := string(it.Key())
		gotKeys = append(gotKeys, k)
		values[k] = string(it.Value())
		it.Next()
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	wantKeys := []string{"a", "b"}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("keys: got=%v want=%v", gotKeys, wantKeys)
	}
	if values["a"] != "backend_a" {
		t.Fatalf("a value: got=%q want=%q", values["a"], "backend_a")
	}
	if values["b"] != "queue_b" {
		t.Fatalf("b value: got=%q want=%q", values["b"], "queue_b")
	}
	if _, ok := values["c"]; ok {
		t.Fatal("unexpected tombstoned key c")
	}
	if _, ok := values["d"]; ok {
		t.Fatal("unexpected post-open queued key d")
	}
	if _, ok := values["e"]; ok {
		t.Fatal("unexpected post-open backend key e")
	}
}

func TestSnapshotReverseIterator_QueueValueOverridesPublishedAndTombstoneHidesPublished(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	if err := backend.SetSync([]byte("a"), []byte("backend_a")); err != nil {
		t.Fatalf("backend set a: %v", err)
	}
	if err := backend.SetSync([]byte("b"), []byte("backend_b")); err != nil {
		t.Fatalf("backend set b: %v", err)
	}
	if err := backend.SetSync([]byte("c"), []byte("backend_c")); err != nil {
		t.Fatalf("backend set c: %v", err)
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open caching db: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("b"), []byte("queue_b")); err != nil {
		t.Fatalf("set queued b: %v", err)
	}
	if err := db.Delete([]byte("c")); err != nil {
		t.Fatalf("delete queued c: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	it, err := snap.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("snapshot reverse iterator: %v", err)
	}

	if err := db.Set([]byte("d"), []byte("post_open_queue")); err != nil {
		t.Fatalf("post-open queued set: %v", err)
	}
	if err := backend.SetSync([]byte("e"), []byte("post_open_backend")); err != nil {
		t.Fatalf("post-open backend set: %v", err)
	}

	var gotKeys []string
	values := make(map[string]string)
	for it.Valid() {
		k := string(it.Key())
		gotKeys = append(gotKeys, k)
		values[k] = string(it.Value())
		it.Next()
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	wantKeys := []string{"b", "a"}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("keys: got=%v want=%v", gotKeys, wantKeys)
	}
	if values["a"] != "backend_a" {
		t.Fatalf("a value: got=%q want=%q", values["a"], "backend_a")
	}
	if values["b"] != "queue_b" {
		t.Fatalf("b value: got=%q want=%q", values["b"], "queue_b")
	}
	if _, ok := values["c"]; ok {
		t.Fatal("unexpected tombstoned key c")
	}
	if _, ok := values["d"]; ok {
		t.Fatal("unexpected post-open queued key d")
	}
	if _, ok := values["e"]; ok {
		t.Fatal("unexpected post-open backend key e")
	}
}

func TestAcquireSnapshot_CapturesRootDomainStateAcrossLaterPublishes(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	if err := backend.SetSync([]byte("a"), []byte("backend_a")); err != nil {
		t.Fatalf("backend set a: %v", err)
	}
	if err := backend.SetSync([]byte("b"), []byte("backend_b")); err != nil {
		t.Fatalf("backend set b: %v", err)
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 8,
	})
	if err != nil {
		t.Fatalf("open caching db: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("a"), []byte("queue_a")); err != nil {
		t.Fatalf("set queued a: %v", err)
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("delete queued b: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	if snap.rootVersion == 0 {
		t.Fatal("expected captured rootVersion")
	}
	if len(snap.rootPointShards) != len(db.mutableShards) {
		t.Fatalf("captured point shards=%d want %d", len(snap.rootPointShards), len(db.mutableShards))
	}
	if len(snap.rootIterator.immutables) == 0 {
		t.Fatal("expected captured iterator immutables")
	}

	firstVersion := snap.rootVersion
	if got, err := snap.Get([]byte("a")); err != nil || string(got) != "queue_a" {
		t.Fatalf("snapshot get a: got=%q err=%v", string(got), err)
	}
	if ok, err := snap.Has([]byte("b")); err != nil || ok {
		t.Fatalf("snapshot has b: ok=%v err=%v", ok, err)
	}

	if err := db.Set([]byte("a"), []byte("post_publish_queue_a")); err != nil {
		t.Fatalf("set later queued a: %v", err)
	}
	if err := db.Set([]byte("c"), []byte("post_publish_queue_c")); err != nil {
		t.Fatalf("set later queued c: %v", err)
	}
	if err := backend.SetSync([]byte("d"), []byte("backend_d")); err != nil {
		t.Fatalf("backend set d: %v", err)
	}
	next := db.AcquireSnapshot()
	if next == nil {
		t.Fatal("expected later snapshot")
	}
	defer next.Close()
	if next.rootVersion <= firstVersion {
		t.Fatalf("later rootVersion=%d want > %d", next.rootVersion, firstVersion)
	}

	if got, err := snap.Get([]byte("a")); err != nil || string(got) != "queue_a" {
		t.Fatalf("stale snapshot get a: got=%q err=%v", string(got), err)
	}
	if ok, err := snap.Has([]byte("b")); err != nil || ok {
		t.Fatalf("stale snapshot has b: ok=%v err=%v", ok, err)
	}

	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("stale snapshot iterator: %v", err)
	}
	defer it.Close()
	var gotKeys []string
	for it.Valid() {
		gotKeys = append(gotKeys, string(it.Key()))
		it.Next()
	}
	if !reflect.DeepEqual(gotKeys, []string{"a"}) {
		t.Fatalf("stale snapshot keys=%v want [a]", gotKeys)
	}

	rit, err := snap.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("stale snapshot reverse iterator: %v", err)
	}
	defer rit.Close()
	gotKeys = gotKeys[:0]
	for rit.Valid() {
		gotKeys = append(gotKeys, string(rit.Key()))
		rit.Next()
	}
	if !reflect.DeepEqual(gotKeys, []string{"a"}) {
		t.Fatalf("stale snapshot reverse keys=%v want [a]", gotKeys)
	}
}

func TestSnapshot_PointReadsUseCapturedRootDomainStateAsAuthority(t *testing.T) {
	snap := &Snapshot{
		db: &DB{
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		},
		rootPointShards: []rootDomainSnapshot{
			{
				immutables: []memtable.Table{
					newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "captured"}),
				},
			},
		},
		view: &memtableView{
			rootSnapshotShards: []rootDomainSnapshot{
				{
					immutables: []memtable.Table{
						newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong"}),
					},
				},
			},
			queue: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong-queue"}),
			},
			queueShardIDs: []uint16{0},
		},
	}

	got, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snapshot get: %v", err)
	}
	if string(got) != "captured" {
		t.Fatalf("snapshot value=%q want %q", string(got), "captured")
	}
}

func TestSnapshot_IteratorUsesCapturedRootDomainStateAsAuthority(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		t.Fatal("expected backend snapshot")
	}
	defer backendSnap.Close()

	snap := &Snapshot{
		db: &DB{
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		},
		backend: backendSnap,
		rootIterator: rootDomainSnapshot{
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "captured-a"}),
			},
		},
		view: &memtableView{
			rootIterator: rootDomainSnapshot{
				immutables: []memtable.Table{
					newRootDomainTestTable(t, rootDomainTestOp{key: "b", value: "wrong-b"}),
				},
			},
			queue: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "c", value: "wrong-c"}),
			},
		},
	}

	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("snapshot iterator: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("keys=%v want [a]", got)
	}
}

func TestSnapshot_HasDoesNotFallBackToViewWhenNoCapturedRuns(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		t.Fatal("expected backend snapshot")
	}
	defer backendSnap.Close()

	snap := &Snapshot{
		db: &DB{
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		},
		backend: backendSnap,
		publishedRoots: &publishedRootSet{
			pointShards: []publishedRootRef{{
				lookup: backendSnapshotLookup{snapshot: backendSnap},
				rootID: backendSnap.State().RootPageID,
			}},
			iterator: publishedRootRef{
				lookup: backendSnapshotLookup{snapshot: backendSnap},
				rootID: backendSnap.State().RootPageID,
			},
		},
		view: &memtableView{
			rootSnapshotShards: []rootDomainSnapshot{
				{
					immutables: []memtable.Table{
						newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong"}),
					},
				},
			},
			queue: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong-queue"}),
			},
			queueShardIDs: []uint16{0},
		},
	}

	ok, err := snap.Has([]byte("k"))
	if err != nil {
		t.Fatalf("snapshot has: %v", err)
	}
	if ok {
		t.Fatal("expected snapshot Has to ignore uncaptured view state")
	}
}

func TestSnapshot_IteratorDoesNotFallBackToViewWhenNoCapturedRuns(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		t.Fatal("expected backend snapshot")
	}
	defer backendSnap.Close()

	snap := &Snapshot{
		db: &DB{
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		},
		backend: backendSnap,
		publishedRoots: &publishedRootSet{
			pointShards: []publishedRootRef{{
				lookup: backendSnapshotLookup{snapshot: backendSnap},
				rootID: backendSnap.State().RootPageID,
			}},
			iterator: publishedRootRef{
				lookup: backendSnapshotLookup{snapshot: backendSnap},
				rootID: backendSnap.State().RootPageID,
			},
		},
		view: &memtableView{
			rootIterator: rootDomainSnapshot{
				immutables: []memtable.Table{
					newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong"}),
				},
			},
			queue: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong-queue"}),
			},
		},
	}

	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("snapshot iterator: %v", err)
	}
	defer it.Close()
	if it.Valid() {
		t.Fatalf("expected iterator to ignore uncaptured view state; first key=%q", string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}
