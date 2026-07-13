package caching

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func openPointSuccessorTestDB(t *testing.T, backend *MockBackend) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     4,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func requireSeekGE(t *testing.T, db *DB, start, end []byte, wantKey, wantValue []byte) {
	t.Helper()
	key, value, found, err := db.SeekGE(start, end)
	if err != nil {
		t.Fatalf("SeekGE(%q,%q): %v", start, end, err)
	}
	if !found || !bytes.Equal(key, wantKey) || !bytes.Equal(value, wantValue) {
		t.Fatalf("SeekGE(%q,%q) = (%q,%q,%t), want (%q,%q,true)", start, end, key, value, found, wantKey, wantValue)
	}
}

func rotatePointSuccessorMemtables(t *testing.T, db *DB) {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.rotateMemtableLocked(false); err != nil {
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
}

func TestSeekGE_SourcePrecedenceTombstonesAndBounds(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("backend-b"))
	backend.Set([]byte("c"), []byte("backend-c"))
	backend.Set([]byte("f"), []byte("backend-f"))
	db := openPointSuccessorTestDB(t, backend)

	if err := db.Set([]byte("d"), []byte("queued-d")); err != nil {
		t.Fatalf("Set queued d: %v", err)
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete queued b: %v", err)
	}
	rotatePointSuccessorMemtables(t, db)
	if err := db.Set([]byte("c"), []byte("mutable-c")); err != nil {
		t.Fatalf("Set mutable c: %v", err)
	}

	requireSeekGE(t, db, []byte("b"), []byte("e"), []byte("c"), []byte("mutable-c"))
	requireSeekGE(t, db, []byte("d"), []byte("e"), []byte("d"), []byte("queued-d"))
	if key, value, found, err := db.SeekGE([]byte("e"), []byte("f")); err != nil || found || key != nil || value != nil {
		t.Fatalf("bounded miss = (%q,%q,%t,%v), want nil,nil,false,nil", key, value, found, err)
	}
}

func TestSeekGE_NewerRangeSpanMasksOlderCandidate(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("masked"))
	backend.Set([]byte("d"), []byte("visible"))
	db := openPointSuccessorTestDB(t, backend)

	if err := db.Set([]byte("z"), []byte("span carrier")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rotatePointSuccessorMemtables(t, db)
	db.mu.Lock()
	db.queueRangeSpans[len(db.queueRangeSpans)-1] = []batch.DeleteRange{{Start: []byte("a"), End: []byte("c")}}
	db.publishMemtablesLocked()
	db.mu.Unlock()

	requireSeekGE(t, db, []byte("a"), []byte("e"), []byte("d"), []byte("visible"))
}

func TestSeekGE_DoesNotRotateOrCreateGeneralIterator(t *testing.T) {
	SetIteratorDebug(true)
	t.Cleanup(func() { SetIteratorDebug(false) })
	backend := NewMockBackend()
	db := openPointSuccessorTestDB(t, backend)
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	before := db.Stats()
	requireSeekGE(t, db, []byte("k"), []byte("l"), []byte("k"), []byte("v"))
	after := db.Stats()
	for _, name := range []string{
		"treedb.cache.iterator.calls_total",
		"treedb.cache.iterator.snapshot_rotations_total",
	} {
		if before[name] != after[name] {
			t.Fatalf("%s changed across SeekGE: %q -> %q", name, before[name], after[name])
		}
	}
	if before["treedb.cache.queue_len"] != after["treedb.cache.queue_len"] {
		t.Fatalf("queue length changed across SeekGE: %q -> %q", before["treedb.cache.queue_len"], after["treedb.cache.queue_len"])
	}
	if calls, err := strconv.ParseUint(after["treedb.cache.point_successor.calls_total"], 10, 64); err != nil || calls != 1 {
		t.Fatalf("point successor calls = %q (%v), want 1", after["treedb.cache.point_successor.calls_total"], err)
	}
}

func TestSeekGE_ReturnsOwnedBytes(t *testing.T) {
	backend := NewMockBackend()
	db := openPointSuccessorTestDB(t, backend)
	if err := db.Set([]byte("k"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	key, value, found, err := db.SeekGE([]byte("k"), nil)
	if err != nil || !found {
		t.Fatalf("SeekGE: found=%t err=%v", found, err)
	}
	key[0], value[0] = 'x', 'X'
	requireSeekGE(t, db, []byte("k"), nil, []byte("k"), []byte("value"))
}
