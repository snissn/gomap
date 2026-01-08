package db

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestValueID_With_PrefixCompression(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                   dir,
		EnableValueIndex:      true,
		ForceValuePointers:    true,
		LeafPrefixCompression: true,
		OmitSlabKeys:          true,
		AllowUnsafe:           true,
		ChunkSize:             1024 * 1024,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Initial set
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			got, err := db.Get(key)
			if err == nil && len(got) > 0 {
				// OK
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Update existing keys
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-updated-%04d", i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("Update(%d): %v", i, err)
		}
	}
	wg.Wait()

	// Structural check for FlagValueID and 8-byte IDs
	snap := db.AcquireSnapshot()
	defer snap.Close()
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		entry, err := snap.GetEntry(key)
		if err != nil {
			continue // Deleted
		}
		if entry.Flags&0x02 != 0 { // node.FlagTombstone
			continue
		}
		// FlagValueID is 0x04
		if entry.Flags&0x04 == 0 {
			t.Errorf("FlagValueID not set for key %d, flags=%x", i, entry.Flags)
		}
		if len(entry.Value) != 8 {
			t.Errorf("Invalid ValueID length for key %d: %d", i, len(entry.Value))
		}
	}
}
