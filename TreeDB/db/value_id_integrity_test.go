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
	opts.ForceValuePointers = true // Force values to be treated as pointers initially
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
