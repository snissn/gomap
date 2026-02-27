package caching

import (
	"bytes"
	"testing"
)

func openMutableCounterDriftDB(t *testing.T) (*DB, *MockBackend) {
	t.Helper()
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		FlushThreshold: 1 << 30,
		MemtableShards: 4,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db, backend
}

func seedMutableWithStaleByteCounter(t *testing.T, db *DB, backend *MockBackend, key, value []byte) {
	t.Helper()
	if err := db.Set(key, value); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
	got, err := backend.Get(key)
	if err != nil {
		t.Fatalf("backend.Get(%q) pre-boundary: %v", key, err)
	}
	if got != nil {
		t.Fatalf("backend already contains key before boundary: key=%q val=%q", key, got)
	}
	// Simulate accounting drift where mutable bytes under-reports while shards
	// still contain entries.
	db.mutableBytes.Store(0)
}

func requireBackendValue(t *testing.T, backend *MockBackend, key, want []byte) {
	t.Helper()
	got, err := backend.Get(key)
	if err != nil {
		t.Fatalf("backend.Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("backend value mismatch for %q: got=%q want=%q", key, got, want)
	}
}

func TestCheckpoint_RotatesMutableWhenByteCounterStale(t *testing.T) {
	db, backend := openMutableCounterDriftDB(t)
	defer func() { _ = db.Close() }()

	key := []byte("checkpoint/stale-counter")
	val := []byte("v-checkpoint")
	seedMutableWithStaleByteCounter(t, db, backend, key, val)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	requireBackendValue(t, backend, key, val)
}

func TestFlushAllMemtablesForSync_RotatesMutableWhenByteCounterStale(t *testing.T) {
	db, backend := openMutableCounterDriftDB(t)
	defer func() { _ = db.Close() }()

	key := []byte("flush/stale-counter")
	val := []byte("v-flush")
	seedMutableWithStaleByteCounter(t, db, backend, key, val)

	if err := db.flushAllMemtablesForSync(true); err != nil {
		t.Fatalf("flushAllMemtablesForSync: %v", err)
	}
	requireBackendValue(t, backend, key, val)
}

func TestDrain_RotatesMutableWhenByteCounterStale(t *testing.T) {
	db, backend := openMutableCounterDriftDB(t)
	defer func() { _ = db.Close() }()

	key := []byte("drain/stale-counter")
	val := []byte("v-drain")
	seedMutableWithStaleByteCounter(t, db, backend, key, val)

	if err := db.Drain(); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	requireBackendValue(t, backend, key, val)
}

func TestClose_FlushesMutableWhenByteCounterStale(t *testing.T) {
	db, backend := openMutableCounterDriftDB(t)

	key := []byte("close/stale-counter")
	val := []byte("v-close")
	seedMutableWithStaleByteCounter(t, db, backend, key, val)

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	requireBackendValue(t, backend, key, val)
}
