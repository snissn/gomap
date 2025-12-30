package caching

import "testing"

func TestCachingBatch_ResetDoesNotCorruptHashSortedMemtable(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:        true,
		DisableWAL:         true,
		FlushThreshold:     1 << 30,
		MemtableMode:       "hash_sorted",
		MemtableShards:     1,
		MaxQueuedMemtables: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	b := db.NewBatch()
	if err := b.Set([]byte("aaaa"), []byte("1111")); err != nil {
		t.Fatalf("Set(aaaa): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write(aaaa): %v", err)
	}
	// Mimic callers (e.g. geth) that reuse a batch via Reset after Write.
	b.Reset()

	if err := b.Set([]byte("bbbb"), []byte("2222")); err != nil {
		t.Fatalf("Set(bbbb): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write(bbbb): %v", err)
	}

	gotA, err := db.Get([]byte("aaaa"))
	if err != nil {
		t.Fatalf("Get(aaaa): %v", err)
	}
	if string(gotA) != "1111" {
		t.Fatalf("Get(aaaa)=%q, want %q", gotA, "1111")
	}
	gotB, err := db.Get([]byte("bbbb"))
	if err != nil {
		t.Fatalf("Get(bbbb): %v", err)
	}
	if string(gotB) != "2222" {
		t.Fatalf("Get(bbbb)=%q, want %q", gotB, "2222")
	}
}
