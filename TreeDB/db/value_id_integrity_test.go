package db

import (
	"fmt"
	"os"
	"testing"
)

func TestValueID_Integrity(t *testing.T) {
	// This test verifies that ValueID handling is robust under high churn, splits,
	// and compaction/restarts, ensuring we don't encounter "invalid value id length" errors.
	dir, err := os.MkdirTemp("", "treedb-integrity")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.EnableValueIndex = true
	opts.ForceValuePointers = true    // Force values to be treated as pointers initially
	opts.LeafPrefixCompression = true // Enable prefix compression

	// Open DB
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write many keys to trigger splits
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if i%2 == 0 {
			val = make([]byte, 1024) // Large value
		}
		if err := db.SetSync(key, val); err != nil {
			t.Fatal(err)
		}
	}

	// Update some keys
	for i := 0; i < 2000; i += 10 {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-update-%04d", i))
		if err := db.SetSync(key, val); err != nil {
			t.Fatal(err)
		}
	}

	// Read all
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		_, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
	}

	// Close and Reopen
	db.Close()
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Read all again
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		_, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for %s after reopen: %v", key, err)
		}
	}
}

func TestVacuum_ForceValuePointers_PreservesValueIDs(t *testing.T) {
	// This test reproduces the bug where TREEDB_FORCE_VALUE_POINTERS would cause
	// vacuum to treat existing ValueIDs as inline values and attempt to move them,
	// corrupting the entry (0-byte ValueID) and causing vacuum failure.
	dir, err := os.MkdirTemp("", "treedb-vacuum-regression")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.EnableValueIndex = true
	// Start with ForceValuePointers to generate some ValueIDs (for large values)
	// and some inline values (if we set threshold high enough or mixed).
	// Actually, we want to ensure we HAVE ValueIDs.
	opts.ForceValuePointers = true
	opts.ValueLogPointerThreshold = 10 // Make small values use ValueIndex

	// Open DB
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write keys that will become ValueIDs
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := make([]byte, 50) // > 10, so uses ValueIndex
		val[0] = byte(i)
		if err := db.SetSync(key, val); err != nil {
			t.Fatal(err)
		}
	}

	// Trigger Offline Vacuum
	// Vacuum uses internal/bulk.BuildWithOptions.
	// If the bug exists, vacuum will fail with "invalid value id length"
	// because it tries to migrate the ValueID (8 bytes) as if it were a user value.
	db.Close()

	// Reopen for Vacuum (VacuumIndexOffline opens it)
	if err := VacuumIndexOffline(opts); err != nil {
		t.Fatalf("Vacuum failed (likely regression of ForceValuePointers corruption): %v", err)
	}

	// Verify Data
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if val == nil || val[0] != byte(i) {
			t.Errorf("Data corruption for %s: got %v", key, val)
		}
	}
}
