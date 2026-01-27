package caching

import (
	"fmt"
	"testing"
)

func TestCachingDB_FlushCombinedLargeMemtablesPersists(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Force cached mode but keep the memtable small enough that tests don't
	// preallocate huge buffers.
	db, err := Open(dir, backend, Options{
		FlushThreshold:     1 << 20, // 1MiB
		MemtableShards:     1,
		MemtableMode:       "hash_sorted",
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Block background flush while building up multiple queued memtables.
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	type kv struct {
		k string
		v string
	}
	written := make([]kv, 0, 9000)

	writeMany := func(prefix byte, n int) {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("%c%08d", prefix, i)
			v := fmt.Sprintf("v-%c-%08d", prefix, i)
			if err := db.Set([]byte(k), []byte(v)); err != nil {
				t.Fatalf("Set: %v", err)
			}
			written = append(written, kv{k: k, v: v})
		}
		db.mu.Lock()
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMemtableLocked: %v", err)
		}
		db.mu.Unlock()
	}

	// Create multiple queued memtables so flushCombinedLocked has to stitch
	// together a large combined flush (totalLen > 2000) across units.
	writeMany('a', 3000)
	writeMany('b', 4000)
	writeMany('c', 2000)

	db.mu.RLock()
	queued := len(db.queue)
	db.mu.RUnlock()
	if queued < 2 {
		t.Fatalf("expected multiple queued memtables, got %d", queued)
	}

	for db.flushCombinedLocked(false) {
	}

	db.mu.RLock()
	queued = len(db.queue)
	db.mu.RUnlock()
	if queued != 0 {
		t.Fatalf("expected queue to be empty after flush, got %d", queued)
	}

	for _, item := range written {
		got, err := backend.Get([]byte(item.k))
		if err != nil {
			t.Fatalf("backend.Get: %v", err)
		}
		if string(got) != item.v {
			t.Fatalf("backend value mismatch for %q: got %q want %q", item.k, got, item.v)
		}
	}
}
