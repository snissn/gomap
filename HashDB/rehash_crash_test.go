package hashdb

import (
	"fmt"
	"testing"
)

// TestRehashMigrationSafety verifies that keys migrated during an incremental rehash
// are correctly marked as Tombstones in the old table, preventing probes from
// reading invalid slab offsets (which previously caused a panic).
func TestRehashMigrationSafety(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenSingle(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// 1. Fill DB to near capacity to prepare for resize
	// Default capacity is 32*1024.
	// Resize threshold default is 65%.
	count := int(DefaultCapacity * 60 / 100)
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte("v")
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// 2. Trigger Resize
	// We use an internal method or just push over threshold.
	// Pushing over threshold is safer.
	// db.SetResizeThreshold(1) // Force resize on next put
	db.SetResizeThreshold(1)

	// Trigger rehash
	if err := db.Put([]byte("trigger"), []byte("v")); err != nil {
		t.Fatalf("Put trigger: %v", err)
	}

	if !db.rehashInProgress {
		t.Fatalf("Expected rehash to be in progress")
	}

	// 3. Force migration of some keys
	// Put calls rehashStep(bucketsPerWrite).
	// We want to migrate enough keys to ensure we hit the "migrated" state.
	// But we also want to probe the OLD table.
	// probeIndexWithHash probes old table if rehashInProgress.

	// We will read (Get) all keys. Get probes old table if not found in new.
	// Migrated keys are in new.
	// Non-migrated keys are in old.
	//
	// The panic happens if we probe a slot in OLD table that WAS migrated (so it's in New),
	// but `probe` logic in Old table thinks it's still there (occupied control) but finds offset 0.
	//
	// To hit this, we need a key that:
	// 1. Was migrated.
	// 2. We search for a DIFFERENT key that collides with the migrated key's slot in the OLD table.
	//
	// We can try to induce collisions by generating many keys.

	// Generate random lookups/inserts to stress the probe logic
	for i := 0; i < count*2; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		// Mix of Get (Probe) and Put (Trigger rehash steps)
		if i%2 == 0 {
			_, _ = db.Get(k)
		} else {
			_ = db.Put(k, []byte("v2"))
		}
	}

	// Ensure we finish
	for db.rehashInProgress {
		if err := db.rehashStep(100); err != nil {
			t.Fatal(err)
		}
	}
}
