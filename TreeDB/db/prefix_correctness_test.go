package db

import (
	"fmt"
	"testing"
)

func TestLeafPrefixCompression_Correctness(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.LeafPrefixCompression = true
	opts.AllowUnsafe = true

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Write sequential keys (high prefix overlap)
	batch := db.NewBatch().(*Batch)
	count := 10000
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("common/prefix/%06d", i))
		batch.Set(key, []byte("value"))
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	// 2. Read back
	snap := db.AcquireSnapshot()
	defer snap.Close()

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("common/prefix/%06d", i))
		val, err := snap.Get(key)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if string(val) != "value" {
			t.Fatalf("Value mismatch for %s: got %q", key, val)
		}
	}

	// 3. Test non-existent keys
	_, err = snap.Get([]byte("common/prefix/999999"))
	if err == nil {
		t.Fatal("Expected error for missing key")
	}

	// 4. Test partial prefix match
	_, err = snap.Get([]byte("common/prefix"))
	if err == nil {
		t.Fatal("Expected error for missing key")
	}
}
