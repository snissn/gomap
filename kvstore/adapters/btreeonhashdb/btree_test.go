package btreeonhashdbadapter

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	hashdb "github.com/snissn/gomap/HashDB"
	btreeonhashdb "github.com/snissn/gomap/HashDB/BTreeOnHashDB"
)

func TestBTreeAdapter(t *testing.T) {
	dir, err := os.MkdirTemp("", "btree_adapter_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	hdb, err := hashdb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer hdb.Close()

	tree, err := btreeonhashdb.NewTreeOnHashDB(hdb, "test_tree", nil)
	if err != nil {
		t.Fatal(err)
	}

	db := Wrap(hdb, tree)

	// Test Set/Get
	if err := db.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("got %q, want val1", val)
	}

	// Test Delete
	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	val, err = db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil value, got %q", val)
	}

	// Test Batch
	batch, err := db.NewBatch()
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}

	val, _ = db.Get([]byte("k1"))
	if !bytes.Equal(val, []byte("v1")) {
		t.Errorf("batch k1: got %q", val)
	}
	val, _ = db.Get([]byte("k2"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("batch k2: got %q", val)
	}
}

func TestBatchArenaResizing(t *testing.T) {
	dir, err := os.MkdirTemp("", "btree_arena_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	hdb, err := hashdb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer hdb.Close()

	tree, err := btreeonhashdb.NewTreeOnHashDB(hdb, "arena_tree", nil)
	if err != nil {
		t.Fatal(err)
	}

	db := Wrap(hdb, tree)
	batch, err := db.NewBatch()
	if err != nil {
		t.Fatal(err)
	}

	// Write enough data to force multiple chunk allocations (default chunk is 64KB)
	// 1000 items * ~100 bytes = ~100KB
	n := 1000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("val-%04d-data-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i))
		if err := batch.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}

	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify all items
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		want := []byte(fmt.Sprintf("val-%04d-data-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i))
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%q) = %q, want %q", k, got, want)
		}
	}
}
