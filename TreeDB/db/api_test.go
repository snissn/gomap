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

	if err := db.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Get mismatch: %s", val)
	}

	has, err := db.Has([]byte("key1"))
	if err != nil || !has {
		t.Fatalf("Has failed: err=%v has=%v", err, has)
	}

	if err := db.Set([]byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

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
		t.Fatalf("Iterator expected 2 items, got %d", count)
	}

	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatal(err)
	}

	val, err = db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get deleted key returned error: %v", err)
	}
	if val != nil {
		t.Fatalf("Get deleted key should return nil")
	}

	has, err = db.Has([]byte("key1"))
	if err != nil {
		t.Fatalf("Has deleted key returned error: %v", err)
	}
	if has {
		t.Fatalf("Has deleted key should be false")
	}
}

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v1")); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup

	snap := db.AcquireSnapshot()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer snap.Close()

		for i := 0; i < 100; i++ {
			val, err := snap.Get([]byte(fmt.Sprintf("k%d", i)))
			if err != nil {
				t.Errorf("Snapshot.Get failed: %v", err)
				return
			}
			if !bytes.Equal(val, []byte("v1")) {
				t.Errorf("Snapshot isolation failed for k%d: got %q, want v1", i, val)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v2"))
		}
	}()

	wg.Wait()

	val, err := db.Get([]byte("k0"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("Final state should be v2, got %q", val)
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
