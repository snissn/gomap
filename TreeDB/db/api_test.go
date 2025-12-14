package db

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestCRUD(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Set
	if err := db.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// 2. Get
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Get mismatch: %s", val)
	}

	// 3. Has
	has, err := db.Has([]byte("key1"))
	if err != nil || !has {
		t.Errorf("Has failed")
	}

	// 4. Iterator
	// Insert more
	db.Set([]byte("key2"), []byte("val2"))

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	count := 0
	for ; it.Valid(); it.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Iterator expected 2 items, got %d", count)
	}

	// 5. Delete
	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatal(err)
	}

	// 6. Get (Deleted)
	val, err = db.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Get deleted key returned error: %v", err)
	}
	if val != nil {
		t.Errorf("Get deleted key should return nil")
	}
	// Has
	has, _ = db.Has([]byte("key1"))
	if has {
		t.Error("Has deleted key should be false")
	}
}

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Initial Data
	for i := 0; i < 100; i++ {
		db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v1"))
	}

	var wg sync.WaitGroup

	// Acquire snapshot BEFORE writer starts to ensure we see v1
	snap := db.AcquireSnapshot()

	// Reader 1 (Long running snapshot)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer snap.Close()

		// This snapshot should see "v1" forever
		for i := 0; i < 100; i++ {
			val, _ := snap.Get([]byte(fmt.Sprintf("k%d", i)))
			if !bytes.Equal(val, []byte("v1")) {
				t.Errorf("Snapshot isolation failed for k%d: got %s, want v1", i, val)
			}
		}
	}()

	// Writer (Updates keys to "v2")
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v2"))
		}
	}()

	wg.Wait()

	// Check final state
	val, _ := db.Get([]byte("k0"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("Final state should be v2")
	}
}
