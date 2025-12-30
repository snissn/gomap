package caching

import "testing"

func TestMemtableMode_AdaptiveDefaultsToHashSorted(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1024 * 1024, MemtableMode: "adaptive"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if got := db.Stats()["treedb.cache.memtable_mode"]; got != "hash_sorted" {
		t.Fatalf("memtable_mode=%q, want hash_sorted", got)
	}
}

func TestMemtableMode_AdaptiveWithSkiplistSeed(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1024 * 1024, MemtableMode: "adaptive:"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if got := db.Stats()["treedb.cache.memtable_mode"]; got != "skiplist" {
		t.Fatalf("memtable_mode=%q, want skiplist", got)
	}
}
