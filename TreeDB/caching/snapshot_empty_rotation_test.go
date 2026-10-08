package caching

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

func emptyRotationFixture(t testing.TB, shards int) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 20, MemtableMode: "hash_sorted", MemtableShards: shards, ValueLogPointerThreshold: 1})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cached.Close(); err != nil {
			t.Error(err)
		}
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	return cached
}

// Variable generic keys selected only to distribute real writes over shards.
func emptyRotationKeys(t testing.TB, db *DB, perShard int) [][][]byte {
	t.Helper()
	keys := make([][][]byte, len(db.mutableShards))
	left := len(keys) * perShard
	for i := 0; left != 0 && i < 1<<20; i++ {
		key := []byte(fmt.Sprintf("tenant/%x/%s/item-%09d", uint64(i)*0x9e3779b97f4a7c15, string(bytes.Repeat([]byte{'x'}, i%29)), i))
		shard := db.shardIndex(key)
		if len(keys[shard]) < perShard {
			keys[shard] = append(keys[shard], key)
			left--
		}
	}
	if left != 0 {
		t.Fatal("could not distribute generic keys")
	}
	return keys
}

func TestEmptySnapshotRotationMixedShards(t *testing.T) {
	for _, route := range []string{"snapshot", "forward", "reverse"} {
		t.Run(route, func(t *testing.T) {
			db := emptyRotationFixture(t, 8)
			keys := emptyRotationKeys(t, db, 1)
			value := bytes.Repeat([]byte("original-pointer/"), 32)
			if err := db.Set(keys[0][0], value); err != nil {
				t.Fatal(err)
			}
			before := make([]memtable.Table, len(keys))
			db.mu.Lock()
			for i := range before {
				before[i] = db.mutableShards[i].mem
			}
			db.mu.Unlock()
			_, ptr, flags, found := before[0].GetEntry(keys[0][0])
			if !found || flags&node.FlagPointer == 0 || ptr.FileID == 0 {
				t.Fatal("fixture did not create a persistent value pointer")
			}
			var snap *Snapshot
			var held interface {
				Valid() bool
				Next()
				Key() []byte
				Value() []byte
				Error() error
				Close() error
			}
			switch route {
			case "snapshot":
				snap = db.AcquireSnapshot()
				if snap == nil {
					t.Fatal("nil snapshot")
				}
				defer snap.Close()
			case "forward":
				it, err := db.Iterator(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				held = it
			case "reverse":
				it, err := db.ReverseIterator(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				held = it
			}
			if held != nil {
				defer held.Close()
			}
			db.mu.Lock()
			var mismatch []int
			for i := range before {
				if (db.mutableShards[i].mem == before[i]) != (i != 0) {
					mismatch = append(mismatch, i)
				}
			}
			view := db.retainMemtableView()
			db.mu.Unlock()
			if len(mismatch) != 0 {
				db.releaseMemtableView(view)
				t.Fatalf("rotation identities wrong for shards %v", mismatch)
			}
			defer func() {
				if view != nil {
					db.releaseMemtableView(view)
				}
			}()
			// Populated old table is frozen/pinned; retained siblings must stay writable.
			for i := range keys {
				if err := db.Set(keys[i][0], []byte(fmt.Sprintf("later-%d", i))); err != nil {
					t.Fatal(err)
				}
			}
			for i := range keys {
				got, err := db.Get(keys[i][0])
				if err != nil || !bytes.Equal(got, []byte(fmt.Sprintf("later-%d", i))) {
					t.Fatalf("live shard%d=%q,%v", i, got, err)
				}
			}
			current := db.AcquireSnapshot()
			if current == nil {
				t.Fatal("nil new snapshot")
			}
			defer current.Close()
			for i := range keys {
				ok, err := current.Has(keys[i][0])
				if err != nil || !ok {
					t.Fatalf("new snapshot shard%d=%t,%v", i, ok, err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if snap != nil {
				for pass := 0; pass < 2; pass++ {
					for i := range keys {
						ok, err := snap.Has(keys[i][0])
						if err != nil || ok != (i == 0) {
							t.Fatalf("old Has shard%d=%t,%v", i, ok, err)
						}
						got, err := snap.Get(keys[i][0])
						if i == 0 {
							if err != nil || !bytes.Equal(got, value) {
								t.Fatalf("old value=%q,%v", got, err)
							}
						} else if err != tree.ErrKeyNotFound {
							t.Fatalf("old Get shard%d=%q,%v", i, got, err)
						}
					}
					flat := make([][]byte, len(keys))
					for i := range keys {
						flat[i] = keys[i][0]
					}
					seen := 0
					if err := snap.GetManyView(flat, func(i int, key, got []byte, found bool) error {
						seen++
						if found != (i == 0) || (found && !bytes.Equal(got, value)) {
							return fmt.Errorf("old batch shard%d=%q,%t", i, got, found)
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					prefixes := make([][]byte, len(keys))
					for i := range keys {
						prefixes[i] = keys[i][0]
					}
					exists, err := snap.HasPrefixes(prefixes)
					if err != nil {
						t.Fatal(err)
					}
					for i := range exists {
						if exists[i] != (i == 0) {
							t.Fatalf("old prefix leaked shard%d", i)
						}
					}
					if seen != len(keys) {
						t.Fatalf("batch callbacks=%d", seen)
					}
					for _, reverse := range []bool{false, true} {
						var it interface {
							Valid() bool
							Next()
							Key() []byte
							Value() []byte
							Error() error
							Close() error
						}
						if reverse {
							x, e := snap.ReverseIterator(nil, nil)
							if e != nil {
								t.Fatal(e)
							}
							it = x
						} else {
							x, e := snap.Iterator(nil, nil)
							if e != nil {
								t.Fatal(e)
							}
							it = x
						}
						if !it.Valid() || !bytes.Equal(it.Key(), keys[0][0]) || !bytes.Equal(it.Value(), value) {
							t.Fatal("old iterator lost original pointer")
						}
						it.Next()
						if it.Valid() {
							t.Fatalf("old iterator leaked later key %q", it.Key())
						}
						if err := it.Error(); err != nil {
							t.Fatal(err)
						}
						_ = it.Close()
						for i := 1; i < len(keys); i++ {
							end := append(append([]byte(nil), keys[i][0]...), 0)
							x, e := snap.Iterator(keys[i][0], end)
							if e != nil {
								t.Fatal(e)
							}
							if x.Valid() {
								t.Fatalf("old range leaked shard%d", i)
							}
							_ = x.Close()
						}
					}
					if err := db.Set(keys[1][0], []byte("next-generation")); err != nil {
						t.Fatal(err)
					}
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				if !held.Valid() || !bytes.Equal(held.Key(), keys[0][0]) || !bytes.Equal(held.Value(), value) {
					t.Fatal("held iterator lost old entry")
				}
				held.Next()
				if held.Valid() {
					t.Fatalf("held iterator leaked %q", held.Key())
				}
				if err := held.Error(); err != nil {
					t.Fatal(err)
				}
			}
			// Releasing an older view must not recycle a reused mutable.
			db.releaseMemtableView(view)
			view = nil
			if err := db.Set(keys[2][0], []byte("after-view-release")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEmptySnapshotRotationSemanticEntries(t *testing.T) {
	for _, op := range []string{"empty-value", "delete", "empty-key-delete"} {
		t.Run(op, func(t *testing.T) {
			db := emptyRotationFixture(t, 4)
			key := emptyRotationKeys(t, db, 1)[0][0]
			if op == "empty-key-delete" {
				key = []byte{}
			}
			if op != "empty-value" {
				if err := db.Set(key, []byte("backend-value")); err != nil {
					t.Fatal(err)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if op == "empty-value" {
				if err := db.Set(key, []byte{}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := db.Delete(key); err != nil {
					t.Fatal(err)
				}
			}
			shard := db.shardIndex(key)
			old := db.mutableShards[shard].mem
			snap := db.AcquireSnapshot()
			if snap == nil {
				t.Fatal("nil snapshot")
			}
			defer snap.Close()
			if db.mutableShards[shard].mem == old {
				t.Fatal("semantic entry did not rotate")
			}
			if err := db.Set(key, []byte("later")); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			ok, err := snap.Has(key)
			if err != nil || ok != (op == "empty-value") {
				t.Fatalf("snapshot semantic entry=%t,%v", ok, err)
			}
		})
	}
}

func TestEmptySnapshotRotationFallbackAndAccounting(t *testing.T) {
	previous := iteratorDebugEnabled.Load()
	SetIteratorDebug(true)
	defer SetIteratorDebug(previous)
	for _, policy := range []string{"empty", "sparse", "generic", "wal", "warmup", "mode-change", "adaptive-change", "inconsistent-bytes"} {
		t.Run(policy, func(t *testing.T) {
			db := emptyRotationFixture(t, 4)
			keys := emptyRotationKeys(t, db, 1)
			before := make([]memtable.Table, 4)
			for i := range before {
				before[i] = db.mutableShards[i].mem
			}
			if policy != "empty" {
				if err := db.Set(keys[0][0], []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			if policy == "warmup" {
				db.mu.Lock()
				db.memtableWarmupActive = true
				db.mu.Unlock()
			}
			if policy == "mode-change" {
				db.storeMemtableMode(memtable.ModeSkiplist)
			}
			if policy == "adaptive-change" {
				db.mu.Lock()
				db.memtableAdaptive = true
				db.memtableStats.writes.Store(adaptiveMinWrites)
				db.memtableStats.seqWrites.Store(adaptiveMinWrites)
				db.mu.Unlock()
			}
			if policy == "inconsistent-bytes" {
				db.mu.Lock()
				db.mutableShards[1].mu.Lock()
				db.mutableShards[1].bytes = 1
				db.mutableBytes.Add(1)
				db.mutableShards[1].mu.Unlock()
				db.mu.Unlock()
			}
			if policy == "generic" || policy == "wal" {
				db.mu.Lock()
				var err error
				if policy == "generic" {
					err = db.rotateMutableShardsLocked(minMemtablePrealloc, false)
				} else {
					err = db.rotateMemtableLocked(false)
				}
				db.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			} else {
				snap := db.AcquireSnapshot()
				if snap == nil {
					t.Fatal("nil snapshot")
				}
				_ = snap.Close()
			}
			want := 4
			if policy == "empty" {
				want = 0
			}
			if policy == "sparse" {
				want = 1
			}
			if policy == "inconsistent-bytes" {
				want = 2
			}
			replaced := 0
			for i := range before {
				if before[i] != db.mutableShards[i].mem {
					replaced++
				}
			}
			if replaced != want {
				t.Fatalf("replaced=%d want%d", replaced, want)
			}
			if policy != "generic" && policy != "wal" && db.snapshotRotatedShardsTotal.Load() != uint64(want) {
				t.Fatalf("counter=%d want%d", db.snapshotRotatedShardsTotal.Load(), want)
			}
			if policy == "adaptive-change" && db.currentMemtableMode() != memtable.ModeAppendOnly {
				t.Fatal("adaptive transition not exercised")
			}
			if policy == "warmup" && db.memtableWarmupActive {
				t.Fatal("warmup not completed")
			}
		})
	}
}

func TestEmptySnapshotRotationConcurrentWrites(t *testing.T) {
	db := emptyRotationFixture(t, 8)
	keys := emptyRotationKeys(t, db, 2)
	if err := db.Set(keys[0][0], []byte("seed")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			if err := db.Set(keys[(i%7)+1][0], []byte(fmt.Sprintf("v%d", i))); err != nil {
				errs <- err
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("nil concurrent snapshot")
		}
		ok, e := snap.Has(keys[0][0])
		if e != nil || !ok {
			t.Fatalf("seed=%t,%v", ok, e)
		}
		_ = snap.Close()
	}
	wg.Wait()
	select {
	case e := <-errs:
		t.Fatal(e)
	default:
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptySnapshotRotationSiblingLifetime(t *testing.T) {
	// This case closes the owner explicitly while a snapshot is still held.
	// Its cleanup must not call Close a second time.
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 20, MemtableMode: "hash_sorted", MemtableShards: 4, ValueLogPointerThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	}()
	keys := emptyRotationKeys(t, db, 1)
	if err := db.Set(keys[0][0], []byte("first")); err != nil {
		t.Fatal(err)
	}
	sibling := db.mutableShards[1].mem
	first := db.AcquireSnapshot()
	if first == nil {
		t.Fatal("nil first snapshot")
	}
	if err := db.Set(keys[0][0], []byte("second")); err != nil {
		t.Fatal(err)
	}
	second := db.AcquireSnapshot()
	if second == nil {
		t.Fatal("nil second snapshot")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if db.mutableShards[1].mem != sibling {
		t.Fatal("empty sibling replaced on publication")
	}
	if err := db.Set(keys[1][0], []byte("still-writable")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if found, err := second.Has(keys[1][0]); err != nil || found {
		t.Fatalf("held cut leaked sibling after release: %t,%v", found, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if db.memtableViewReaders.Load() != 0 {
		t.Fatal("held view reader survived owner/snapshot close")
	}
}

func BenchmarkSnapshotMixedShardRotation(b *testing.B) {
	for _, dirty := range []int{1, 4, 16} {
		for _, count := range []int{1, 64} {
			b.Run(fmt.Sprintf("dirty_%d/keys_%d", dirty, count), func(b *testing.B) {
				db := emptyRotationFixture(b, 16)
				keys := emptyRotationKeys(b, db, count)
				value := bytes.Repeat([]byte("generic-value/"), 16)
				flat := make([][]byte, 0, dirty*count)
				for i := 0; i < dirty; i++ {
					flat = append(flat, keys[i]...)
				}
				sort.Slice(flat, func(i, j int) bool { return bytes.Compare(flat[i], flat[j]) < 0 })
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, k := range flat {
						if err := db.Set(k, value); err != nil {
							b.Fatal(err)
						}
					}
					snap := db.AcquireSnapshot()
					if snap == nil {
						b.Fatal("nil snapshot")
					}
					ok, err := snap.Has(flat[len(flat)-1])
					if err != nil || !ok {
						b.Fatalf("snapshot=%t,%v", ok, err)
					}
					if err := snap.Close(); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					if err := db.Checkpoint(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}
