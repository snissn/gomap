package caching

import (
	"bytes"
	"testing"
)

func TestVisibilityDuringCommitPipeline(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	opts := Options{
		FlushThreshold: 1,
		AllowUnsafe:    true,
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Rotate mutable to queue.
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	// Manually select the queued unit for a flush job and mark it as flushing.
	db.mu.Lock()
	_, ids, _, _ := db.collectFlushUnitsLocked(0, 1, 0)
	if len(ids) == 0 {
		db.mu.Unlock()
		t.Fatalf("expected queued unit")
	}
	for _, id := range ids {
		db.flushingIDs[id] = struct{}{}
	}
	db.mu.Unlock()

	// During flushing, reads should still see the value.
	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get mismatch: %q", got)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("iterator invalid")
	}
	if !bytes.Equal(it.Key(), []byte("k1")) {
		t.Fatalf("iterator key mismatch: %q", it.Key())
	}
	if !bytes.Equal(it.Value(), []byte("v1")) {
		t.Fatalf("iterator value mismatch: %q", it.Value())
	}

	// Cleanup: remove flushing marker so other tests aren't affected.
	db.mu.Lock()
	for _, id := range ids {
		delete(db.flushingIDs, id)
	}
	db.mu.Unlock()
}
