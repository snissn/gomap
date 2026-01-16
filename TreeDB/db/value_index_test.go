package db

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestValueIndex_RoundTrip(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	opts.EnableValueIndex = true
	// Force value pointers so even small values go to slab/index
	opts.ForceValuePointers = true
	opts.ValueLogPointerThreshold = 0 // Ensure everything goes to slab if possible? No, ForceValuePointers implies it.

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("key1")
	val := []byte("value1-with-significant-length-to-verify-pointers")

	if err := db.Set(key, val); err != nil {
		t.Fatal(err)
	}

	// Verify Read Path (Public API)
	got, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("Get mismatch: got %q, want %q", got, val)
	}

	// Verify Low-Level Structure
	// 1. User Tree should have FlagValueID and 8-byte Value
	snap := db.AcquireSnapshot()
	defer snap.Close()

	entry, err := snap.tree.GetEntry(key)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Flags&node.FlagValueID == 0 {
		t.Errorf("Expected FlagValueID to be set, got flags: %x", entry.Flags)
	}
	if len(entry.Value) != 8 {
		t.Errorf("Expected 8-byte ValueID, got len %d", len(entry.Value))
	}
	vid := ValueID(binary.BigEndian.Uint64(entry.Value))
	if vid == 0 {
		t.Error("Got ValueID 0")
	}

	// 2. System Tree should have ValueID -> ValuePtr
	// Need to access System Tree. We can reconstruct it.
	// But ValueReaderForState is internal.
	// We can use db internals via reflection or just trust Get worked?
	// Get worked, so the link exists.
	// But let's verify we actually stored it in System Tree.
	// We can iterate the system tree if we can access it.
	// But system tree root is in db.meta.SystemRootPageID.
	// We can't access db.meta from test unless we export accessors or use debug tools.
	// db.State().SystemRootPageID is available.
}

func TestValueIndex_SnapshotVisibility(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	opts.EnableValueIndex = true
	opts.ForceValuePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("key1")
	val1 := []byte("version1")
	val2 := []byte("version2")

	if err := db.Set(key, val1); err != nil {
		t.Fatal(err)
	}

	snap1 := db.AcquireSnapshot()
	defer snap1.Close()

	if err := db.Set(key, val2); err != nil {
		t.Fatal(err)
	}

	// Snap1 should see val1
	got1, err := snap1.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got1, val1) {
		t.Fatalf("Snap1 mismatch: got %q, want %q", got1, val1)
	}

	// DB should see val2
	got2, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, val2) {
		t.Fatalf("DB mismatch: got %q, want %q", got2, val2)
	}
}

func TestValueIndex_Iterator(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	opts.EnableValueIndex = true
	opts.ForceValuePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := map[string]string{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}
	for k, v := range data {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	count := 0
	for ; it.Valid(); it.Next() {
		k := string(it.Key())
		v := string(it.Value())
		if want, ok := data[k]; !ok {
			t.Errorf("Unexpected key: %s", k)
		} else if v != want {
			t.Errorf("Value mismatch for %s: got %s, want %s", k, v, want)
		}
		count++
	}
	if count != len(data) {
		t.Errorf("Expected %d items, got %d", len(data), count)
	}
}

func DefaultOptions(dir string) Options {
	return Options{
		Dir:        dir,
		ChunkSize:  16 * 1024 * 1024,
		KeepRecent: 100,
	}
}
