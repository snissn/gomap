package caching

import (
	"encoding/binary"
	"testing"
)

func TestCachingDB_ViewBatchBypassesMemtableWhenWALDisabled(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		FlushThreshold: 1 << 60,
		MemtableShards: 1,
		MemtableMode:   "hash_sorted",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	b := db.NewBatch()
	var keyBuf [8]byte
	for i := 0; i < viewBatchBypassMinEntries; i++ {
		binary.BigEndian.PutUint64(keyBuf[:], uint64(i))
		key := append([]byte(nil), keyBuf[:]...)
		value := []byte("v")
		if err := b.SetView(key, value); err != nil {
			t.Fatalf("SetView(%d): %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if got := db.mutableBytes.Load(); got != 0 {
		t.Fatalf("expected mutableBytes=0 after bypass write, got %d", got)
	}

	backend.mu.RLock()
	writeCalls := backend.writeCalls
	backend.mu.RUnlock()
	if writeCalls == 0 {
		t.Fatalf("expected backend write calls")
	}

	binary.BigEndian.PutUint64(keyBuf[:], 0)
	got, err := backend.Get(keyBuf[:])
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("backend.Get=%q, want %q", got, "v")
	}
}
