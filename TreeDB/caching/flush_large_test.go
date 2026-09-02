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

	for db.flushOneLocked(false) {
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

func TestCachingDB_FlushCombinedLargeMemtablesParallelBuildPreservesLastWrite(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Enable parallel build so the combined flush uses the worker path.
	db, err := Open(dir, backend, Options{
		FlushThreshold:          1 << 20, // 1MiB
		MemtableShards:          1,
		MemtableMode:            "hash_sorted",
		MaxQueuedMemtables:      -1,
		AllowUnsafe:             true,
		FlushBuildConcurrency:   4,
		WriterFlushMaxMemtables: 0,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Block background flush while building up multiple queued memtables.
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	writeAndRotate := func(prefix byte, n int, overlapKey string, overlapVal string) {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("%c%08d", prefix, i)
			v := fmt.Sprintf("v-%c-%08d", prefix, i)
			if err := db.Set([]byte(k), []byte(v)); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		if overlapKey != "" {
			if err := db.Set([]byte(overlapKey), []byte(overlapVal)); err != nil {
				t.Fatalf("Set overlap: %v", err)
			}
		}
		db.mu.Lock()
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMemtableLocked: %v", err)
		}
		db.mu.Unlock()
	}

	overlapKey := "zzzz-overlap"
	writeAndRotate('a', 3000, overlapKey, "old")
	writeAndRotate('b', 3000, overlapKey, "new")

	db.mu.RLock()
	queued := len(db.queue)
	db.mu.RUnlock()
	if queued < 2 {
		t.Fatalf("expected multiple queued memtables, got %d", queued)
	}

	for db.flushOneLocked(false) {
	}

	got, err := backend.Get([]byte(overlapKey))
	if err != nil {
		t.Fatalf("backend.Get overlap: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("overlap value mismatch: got %q want %q", got, "new")
	}
}

func TestCachingDB_FlushAllParallelBuildCombinesMemtables(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Force combined parallel build for small inputs.
	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 60,
		MemtableShards:        1,
		MemtableMode:          "hash_sorted",
		MaxQueuedMemtables:    -1,
		AllowUnsafe:           true,
		FlushBuildConcurrency: 4,
		FlushBuildMinEntries:  1,
		FlushBuildMinUnits:    2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("v1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k"), []byte("v2"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k2"), []byte("v3"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()
	if writeCalls != 1 {
		t.Fatalf("expected 1 backend batch commit (combined flush), got %d", writeCalls)
	}

	got, err := db.backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("backend.Get(k): got %q want %q", got, "v2")
	}

	got, err = db.backend.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("backend.Get(k2): got %q want %q", got, "v3")
	}
}

func TestCachingDB_FlushAllParallelBuildDeletesNewestWins(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 60,
		MemtableShards:        1,
		MemtableMode:          "hash_sorted",
		MaxQueuedMemtables:    -1,
		AllowUnsafe:           true,
		FlushBuildConcurrency: 4,
		FlushBuildMinEntries:  1,
		FlushBuildMinUnits:    2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("v1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	setMutable(db, []byte("k"), []byte("v2"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	deleteMutable(db, []byte("k"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	got, err := db.backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if got != nil {
		t.Fatalf("expected k deleted, got %q", got)
	}
}

func TestCachingDB_FlushAllParallelBuildChunkedRuns(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 60,
		MemtableShards:        1,
		MemtableMode:          "hash_sorted",
		MaxQueuedMemtables:    -1,
		AllowUnsafe:           true,
		FlushBuildConcurrency: 4,
		FlushBuildMinEntries:  1,
		FlushBuildMinUnits:    2,
		FlushBuildChunkCap:    2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.Lock()
	for i := 0; i < 5; i++ {
		setMutable(db, []byte(fmt.Sprintf("a%02d", i)), []byte("old"))
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	for i := 0; i < 5; i++ {
		setMutable(db, []byte(fmt.Sprintf("a%02d", i)), []byte("new"))
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()
	if writeCalls != 1 {
		t.Fatalf("expected 1 backend batch commit (combined flush), got %d", writeCalls)
	}

	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("a%02d", i))
		got, err := db.backend.Get(k)
		if err != nil {
			t.Fatalf("backend.Get: %v", err)
		}
		if string(got) != "new" {
			t.Fatalf("backend.Get(%s): got %q want %q", k, got, "new")
		}
	}
}
