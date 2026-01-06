package db

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestGC_ReclaimsUnusedVlog(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                dir,
		EnableValueIndex:   true,
		ForceValuePointers: true,
		ChunkSize:          64 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Write a large value. It should go to a vlog segment.
	key := []byte("key1")
	val1 := bytes.Repeat([]byte("a"), 1024)
	if err := db.Set(key, val1); err != nil {
		t.Fatal(err)
	}

	// To test GC of SEGMENTS, we need multiple segments.
	// Let's force slab rotation.
	
	// Fill slab 0
	largeVal := make([]byte, 1024)
	for i := 0; i < 100; i++ {
		if err := db.Set([]byte(fmt.Sprintf("fill%d", i)), largeVal); err != nil {
			t.Fatal(err)
		}
	}
	
	activeIDBefore := db.slabManager.ActiveSlabID()
	
	// Rotate slab manually to ensure the old one is not active.
	if _, err := db.slabManager.Rotate(); err != nil {
		t.Fatal(err)
	}
	
	activeIDAfter := db.slabManager.ActiveSlabID()
	if activeIDAfter <= activeIDBefore {
		t.Errorf("Expected slab rotation, got %d -> %d", activeIDBefore, activeIDAfter)
	}

	// Overwrite key1 with a new value (goes to new slab) to make the old ValueID unreachable.
	val2 := bytes.Repeat([]byte("b"), 1024)
	if err := db.Set(key, val2); err != nil {
		t.Fatal(err)
	}

	// Check if old slab exists
	oldSlabPath := db.slabManager.GetSlabPath(activeIDBefore)
	if _, err := os.Stat(oldSlabPath); err != nil {
		t.Fatalf("Expected old slab to exist at %s", oldSlabPath)
	}

	// 2. Perform GC.
	// It should find that key1 now points to a new ValueID.
	// The old ValueID is no longer in User Tree.
	// GC should prune old ValueID from System Tree.
	// Then it should find that old slab has no more live refs.
	
	// Wait, we filled the slab with 'fillN' keys too. They are still live!
	// So old slab won't be deleted yet.
	// Let's delete them.
	for i := 0; i < 100; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("fill%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	
	// Now only key1 is live, and it points to the NEW slab.
	// Old slab should have 0 refs.
	
	reclaimed, err := db.GC()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("GC reclaimed: %d\n", reclaimed)

	// 3. Verify old slab is gone.
	if _, err := os.Stat(oldSlabPath); err == nil {
		t.Error("Expected old slab to be deleted by GC")
	} else if !os.IsNotExist(err) {
		t.Errorf("Unexpected error stating old slab: %v", err)
	}
	
	// 4. Verify val2 is still readable.
	got, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, val2) {
		t.Errorf("Value mismatch after GC: got %q, want %q", got, val2)
	}
	
db.Close()
}

func TestGC_ValueLog(t *testing.T) {
	// Similar test for vlog segments if possible in backend mode.
	// Vlog segments are usually created by CachingDB.
	// But we can manually place one.
}
