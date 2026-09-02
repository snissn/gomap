package caching

import (
	"bytes"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openOwnedReadResultTestDB(t *testing.T) *DB {
	t.Helper()

	backendDir := filepath.Join(t.TempDir(), "backend")
	backend, err := backenddb.Open(backenddb.Options{Dir: backendDir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 30,
		MemtableShards:           1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestDBGet_PointerValueRemainsCallerOwned(t *testing.T) {
	db := openOwnedReadResultTestDB(t)

	key := []byte("k")
	want := bytes.Repeat([]byte("v"), 8<<10)
	if err := db.Set(key, want); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got1, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get #1: %v", err)
	}
	if !bytes.Equal(got1, want) {
		t.Fatalf("Get #1 mismatch")
	}
	got1[0] ^= 0xff

	got2, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get #2: %v", err)
	}
	if !bytes.Equal(got2, want) {
		t.Fatalf("Get #2 mismatch after caller mutation")
	}
}

func TestSnapshotGet_QueuedPointerValueRemainsCallerOwned(t *testing.T) {
	db := openOwnedReadResultTestDB(t)

	key := []byte("k")
	want := bytes.Repeat([]byte("q"), 8<<10)
	if err := db.Set(key, want); err != nil {
		t.Fatalf("Set: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	got1, err := snap.Get(key)
	if err != nil {
		t.Fatalf("Get #1: %v", err)
	}
	if !bytes.Equal(got1, want) {
		t.Fatalf("Get #1 mismatch")
	}
	got1[0] ^= 0xff

	got2, err := snap.Get(key)
	if err != nil {
		t.Fatalf("Get #2: %v", err)
	}
	if !bytes.Equal(got2, want) {
		t.Fatalf("Get #2 mismatch after caller mutation")
	}
}
