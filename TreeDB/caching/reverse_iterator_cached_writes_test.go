package caching

import (
	"reflect"
	"testing"
)

func TestReverseIterator_IncludesCachedWrites_SnapshotIsolated(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("backend_b"))

	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("b"), []byte("mem_b")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := db.Set([]byte("c"), []byte("mem_c")); err != nil {
		t.Fatalf("Set(c): %v", err)
	}

	it, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}

	// Subsequent writes must not be visible to the iterator snapshot.
	if err := db.Set([]byte("d"), []byte("mem_d")); err != nil {
		t.Fatalf("Set(d): %v", err)
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
		t.Fatalf("Close: %v", err)
	}

	wantKeys := []string{"c", "b"}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("keys: got=%v want=%v", gotKeys, wantKeys)
	}
	if values["b"] != "mem_b" {
		t.Fatalf("b value: got=%q want=%q", values["b"], "mem_b")
	}
	if values["c"] != "mem_c" {
		t.Fatalf("c value: got=%q want=%q", values["c"], "mem_c")
	}
	if _, ok := values["d"]; ok {
		t.Fatalf("unexpected key d in iterator")
	}

	// ReverseIterator must not flush buffered writes into the backend as a side effect.
	backend.mu.RLock()
	_, hasC := backend.data["c"]
	backendB := backend.data["b"]
	backend.mu.RUnlock()
	if hasC {
		t.Fatalf("backend unexpectedly has key c")
	}
	if string(backendB) != "backend_b" {
		t.Fatalf("backend b mutated: got=%q want=%q", string(backendB), "backend_b")
	}
}
