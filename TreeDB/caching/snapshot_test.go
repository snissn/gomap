package caching

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestAcquireSnapshot_NotifyErrorOnRotateFailure(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb-snapshot-rotate-fail-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	errCh := make(chan error, 1)
	cached, err := Open(dir, backend, Options{
		FlushThreshold: 1024 * 1024,
		NotifyError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	if err := cached.SetSync([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	// Force the snapshot rotation path to fail inside newMutableMemtableWithCapacityMode.
	cached.storeMemtableMode(memtable.Mode(255))

	if snap := cached.AcquireSnapshot(); snap != nil {
		_ = snap.Close()
		t.Fatalf("AcquireSnapshot()=%v want nil on rotate failure", snap)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected non-nil notify error")
		}
	default:
		t.Fatalf("expected NotifyError to be called")
	}
}

func TestIteratorSnapshotIsolation(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb-snapshot-iso-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	// Use a large threshold so we rely on the mutable memtable
	cached, err := Open(dir, backend, Options{FlushThreshold: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	// 1. Write "a"
	if err := cached.SetSync([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	// 2. Open Iterator (should capture snapshot)
	it, err := cached.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	// 3. Write "b" (should not be seen by 'it')
	if err := cached.SetSync([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	// 4. Verify Iterator
	foundA := false
	foundB := false

	for it.Valid() {
		k := string(it.Key())
		if k == "a" {
			foundA = true
		}
		if k == "b" {
			foundB = true
		}
		it.Next()
	}

	if !foundA {
		t.Error("Iterator missed pre-existing key 'a'")
	}
	if foundB {
		t.Error("Iterator saw key 'b' written AFTER iterator creation (Snapshot Isolation violation)")
	}
}
