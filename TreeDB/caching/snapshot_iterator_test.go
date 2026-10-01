package caching

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
)

func TestSingleSourceIteratorForwardSeekClampsToStart(t *testing.T) {
	newIterator := func(t *testing.T, keys ...string) merging.Iterator {
		t.Helper()
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeSkiplist)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		for _, key := range keys {
			mt.Set([]byte(key), []byte("value/"+key))
		}
		mt.Freeze()
		start := []byte("m")
		return newSingleSourceIterator(mt.NewIterator(start, nil), start, nil, false)
	}

	t.Run("mixed domain", func(t *testing.T) {
		it := newIterator(t, "a", "c", "m", "z")
		defer it.Close()
		for _, test := range []struct {
			name   string
			target []byte
			want   []byte
		}{
			{name: "nil", target: nil, want: []byte("m")},
			{name: "below", target: []byte("a"), want: []byte("m")},
			{name: "start", target: []byte("m"), want: []byte("m")},
			{name: "inside", target: []byte("n"), want: []byte("z")},
			{name: "above", target: []byte{0xff}},
		} {
			t.Run(test.name, func(t *testing.T) {
				it.Seek(test.target)
				if test.want == nil {
					if it.Valid() {
						t.Fatalf("Seek(%x) key=%q want invalid", test.target, it.Key())
					}
				} else if !it.Valid() || !bytes.Equal(it.Key(), test.want) {
					t.Fatalf("Seek(%x) valid=%v key=%q want %q", test.target, it.Valid(), it.Key(), test.want)
				}
				if err := it.Error(); err != nil {
					t.Fatalf("Seek(%x) error=%v", test.target, err)
				}
			})
		}
	})

	t.Run("all data below start", func(t *testing.T) {
		it := newIterator(t, "a", "c")
		defer it.Close()
		for _, target := range [][]byte{nil, []byte("a"), []byte("m"), {0xff}} {
			it.Seek(target)
			if it.Valid() {
				t.Fatalf("Seek(%x) key=%q want invalid", target, it.Key())
			}
			if err := it.Error(); err != nil {
				t.Fatalf("Seek(%x) error=%v", target, err)
			}
		}
	})
}

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

// Direct snapshots must be counted at their own entry point, including cuts
// of populated shards unrelated to the iterator's eventual range.
func TestAcquireSnapshotDebugAccounting(t *testing.T) {
	previous := iteratorDebugEnabled.Load()
	SetIteratorDebug(true)
	t.Cleanup(func() { SetIteratorDebug(previous) })
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableShards: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keys := make([][]byte, 8)
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("snapshot-accounting-%d", i))
		shard := db.shardIndex(key)
		keys[shard] = key
		complete := true
		for _, key := range keys {
			complete = complete && key != nil
		}
		if complete {
			break
		}
	}
	for _, key := range keys {
		if err := db.Set(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	var bytes uint64
	for i := range db.mutableShards {
		bytes += uint64(db.mutableShards[i].mem.Size())
	}
	stat := func(key string) uint64 {
		t.Helper()
		n, err := strconv.ParseUint(db.Stats()["treedb.cache."+key], 10, 64)
		if err != nil {
			t.Fatalf("stat %s: %v", key, err)
		}
		return n
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	for key, want := range map[string]uint64{
		"snapshot.calls_total": 1, "snapshot.rotations_total": 1,
		"snapshot.rotated_shards_total": 8, "snapshot.enqueued_records_total": 8,
		"snapshot.enqueued_bytes_total": bytes, "iterator.snapshot_rotations_total": 0,
	} {
		if got := stat(key); got != want {
			t.Fatalf("%s=%d want %d", key, got, want)
		}
	}
	it, err := snap.Iterator(keys[0], append(append([]byte(nil), keys[0]...), 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if got := stat("snapshot.iterator_calls_total"); got != 1 {
		t.Fatalf("snapshot iterator calls=%d", got)
	}
	if got := stat("iterator.sources_total"); got != 9 {
		t.Fatalf("snapshot sources=%d want 9", got)
	}
	second := db.AcquireSnapshot()
	if second == nil {
		t.Fatal("second AcquireSnapshot=nil")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if got := stat("snapshot.calls_total"); got != 2 {
		t.Fatalf("calls=%d want 2", got)
	}
	if got := stat("snapshot.rotations_total"); got != 1 {
		t.Fatalf("empty cut rotations=%d want 1", got)
	}
	// A DB.Iterator cut is attributed to that entry point, not AcquireSnapshot.
	if err := db.Set(keys[0], []byte("later")); err != nil {
		t.Fatal(err)
	}
	ordinary, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ordinary.Close(); err != nil {
		t.Fatal(err)
	}
	if got := stat("iterator.snapshot_rotations_total"); got != 1 {
		t.Fatalf("DB.Iterator rotations=%d", got)
	}
	if got := stat("snapshot.rotations_total"); got != 1 {
		t.Fatalf("direct rotations=%d", got)
	}
	SetIteratorDebug(false)
	if err := db.Set(keys[0], []byte("debug-off")); err != nil {
		t.Fatal(err)
	}
	unobserved := db.AcquireSnapshot()
	if unobserved == nil {
		t.Fatal("disabled AcquireSnapshot=nil")
	}
	if err := unobserved.Close(); err != nil {
		t.Fatal(err)
	}
	if got := stat("snapshot.calls_total"); got != 2 {
		t.Fatalf("disabled calls=%d", got)
	}
	if got := stat("snapshot.rotations_total"); got != 1 {
		t.Fatalf("disabled rotations=%d", got)
	}
}
