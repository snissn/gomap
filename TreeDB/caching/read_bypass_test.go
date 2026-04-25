package caching

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func readBypassBEKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(i))
	return key
}

func TestCanBypassMemtableReadRequiresMutableEntriesEmpty(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold: 1 << 20,
		MemtableMode:   "btree",
		MemtableShards: 4,
		DisableWAL:     true,
		AllowUnsafe:    true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := []byte("route-safety-key")
	for i := byte(0); db.hashShardIndex(key) == 0; i++ {
		key = []byte{'r', 'o', 'u', 't', 'e', '-', i}
	}
	// Simulate the safety condition this guard is meant to cover: a mutable
	// entry exists outside the hash-selected shard while byte accounting alone
	// is not sufficient to prove the mutable layer is empty.
	shard := &db.mutableShards[0]
	shard.mu.Lock()
	shard.mem.Delete(key)
	shard.rng.add(key)
	db.updateMutableShardAccountingLocked(shard, shard.mem.Size(), shard.mem.Len())
	shard.mu.Unlock()
	db.mutableBytes.Store(0)

	view := db.retainMemtableView()
	if view == nil {
		t.Fatal("missing memtable view")
	}
	if db.canBypassMemtableRead(view, key) {
		db.releaseMemtableView(view)
		t.Fatal("canBypassMemtableRead returned true with mutable tombstone present")
	}
	db.releaseMemtableView(view)
}

func TestRoutedQueueKeepsFutureWritesOnRoute(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:          1 << 30,
		MemtableMode:            "btree",
		MemtableShards:          4,
		MaxQueuedMemtables:      -1,
		WriterFlushMaxMemtables: 0,
		DisableWAL:              true,
		AllowUnsafe:             true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const keys = 300
	initial := db.NewBatch()
	for i := 0; i < keys; i++ {
		if err := initial.Set(readBypassBEKey(i), []byte("older-ranged-queue-value")); err != nil {
			t.Fatalf("initial set i=%d: %v", i, err)
		}
	}
	if err := initial.Write(); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("initial close: %v", err)
	}
	if db.currentMutableRoute.Load() == nil {
		t.Fatal("initial sorted batch did not create a mutable route")
	}

	db.mu.Lock()
	if err := db.rotateMutableShardsLocked(-1, false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	db.mu.RLock()
	hasRangedQueue := false
	for _, mode := range db.queueRouteModes {
		if mode == memtableRouteRanged {
			hasRangedQueue = true
			break
		}
	}
	queueLen := len(db.queue)
	db.mu.RUnlock()
	if queueLen == 0 || !hasRangedQueue {
		t.Fatalf("rotate produced queue_len=%d ranged_queue=%t, want non-empty ranged queue", queueLen, hasRangedQueue)
	}

	route := db.currentMutableRoute.Load()
	if route == nil {
		t.Fatal("mutable route was not retained while ranged queue is pending")
	}
	var deleteKey, updateKey []byte
	for _, entry := range route.normalizedEntries() {
		for i := 0; i < keys; i++ {
			key := readBypassBEKey(i)
			if !keyInRange(entry.rng, key) || db.hashShardIndex(key) == int(entry.shardID) {
				continue
			}
			if deleteKey == nil {
				deleteKey = key
				continue
			}
			updateKey = key
			break
		}
		if updateKey != nil {
			break
		}
	}
	if deleteKey == nil || updateKey == nil {
		t.Fatal("expected routed keys whose hash shard differs from the active route shard")
	}
	updated := []byte("newer-current-routed-mutable-value")
	churn := db.NewBatch()
	if err := churn.Delete(deleteKey); err != nil {
		t.Fatalf("churn delete: %v", err)
	}
	if err := churn.Set(updateKey, updated); err != nil {
		t.Fatalf("churn set: %v", err)
	}
	if err := churn.Write(); err != nil {
		t.Fatalf("churn write: %v", err)
	}
	if err := churn.Close(); err != nil {
		t.Fatalf("churn close: %v", err)
	}

	got, err := db.Get(deleteKey)
	if err != nil {
		t.Fatalf("get deleted key: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted key returned len=%d from older ranged queue", len(got))
	}
	has, err := db.Has(deleteKey)
	if err != nil {
		t.Fatalf("has deleted key: %v", err)
	}
	if has {
		t.Fatal("Has returned true for key tombstoned in newer routed mutable")
	}

	got, err = db.Get(updateKey)
	if err != nil {
		t.Fatalf("get updated key: %v", err)
	}
	if !bytes.Equal(got, updated) {
		t.Fatalf("updated key got %q want %q", got, updated)
	}
	out, err := db.GetAppend(updateKey, []byte("prefix:"))
	if err != nil {
		t.Fatalf("get append updated key: %v", err)
	}
	if want := append([]byte("prefix:"), updated...); !bytes.Equal(out, want) {
		t.Fatalf("GetAppend got %q want %q", out, want)
	}
}
