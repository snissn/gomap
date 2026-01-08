package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
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

func TestIterator_ValueIDResolutionError(t *testing.T) {
	// This test verifies that DBIterator.Value() correctly handles errors during
	// ValueID resolution (e.g. if the ValueID points to a missing entry).
	dir := t.TempDir()
	opts := Options{Dir: dir, EnableValueIndex: true, ForceValuePointers: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("v")
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	// Corrupt the System Tree by deleting the ValueID mapping manually.
	// We need to find the ValueID first.
	snap := db.AcquireSnapshot()
	entry, err := snap.GetEntry(key)
	if err != nil {
		t.Fatal(err)
	}
	snap.Close()

	if entry.Flags&node.FlagValueID == 0 {
		t.Fatal("expected ValueID entry")
	}

	// Delete the mapping from the System Tree
	b2 := db.NewBatch().(*Batch)
	sysKey := encodeValueIndexKey(ValueID(binary.BigEndian.Uint64(entry.Value)))
	if err := b2.batch.Delete(sysKey); err != nil {
		t.Fatal(err)
	}
	// Important: we must use the low-level writeSerialized to apply sysOps
	// because direct User Tree deletes of sysKeys are not handled by normal Write.
	if err := b2.writeSerialized(true, []batch.Entry{{Type: batch.OpDelete, Key: sysKey}}); err != nil {
		t.Fatal(err)
	}
	b2.Close()

	// Reopen DB to ensure clean state
	db.Close()
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Verify it's really gone from the system tree first
	snap2 := db.AcquireSnapshot()
	sysTree := tree.New(snap2.idx.pager, ValueReaderForState(snap2.state), snap2.state.SystemRootPageID)
	_, err = sysTree.Get(sysKey)
	if err == nil {
		t.Fatal("System Tree mapping should be deleted")
	}
	snap2.Close()

	// Now iterate. Resolution should fail.
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if !it.Valid() {
		t.Fatal("iterator should be valid initially")
	}

	// Value() triggers the resolution.
	v := it.Value()
	if v != nil {
		t.Errorf("Expected nil value due to resolution error, got %v", v)
	}
	if it.Error() == nil {
		t.Error("Expected non-nil error after failed resolution")
	}
}
