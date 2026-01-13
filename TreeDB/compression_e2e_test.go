package treedb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCompression_E2E_FullRecord(t *testing.T) {
	dir := t.TempDir()

	// Set env vars to enable full record compression
	os.Setenv("TREEDB_SLAB_COMPRESSION", "zstd")
	os.Setenv("TREEDB_SLAB_COMPRESSION_MIN_BYTES", "1")
	os.Setenv("TREEDB_SLAB_COMPRESSION_MIN_SAVINGS", "0")
	os.Setenv("TREEDB_FORCE_VALUE_POINTERS", "1")
	os.Setenv("TREEDB_DISABLE_VALUE_LOG", "1")
	defer func() {
		os.Unsetenv("TREEDB_SLAB_COMPRESSION")
		os.Unsetenv("TREEDB_SLAB_COMPRESSION_MIN_BYTES")
		os.Unsetenv("TREEDB_SLAB_COMPRESSION_MIN_SAVINGS")
		os.Unsetenv("TREEDB_FORCE_VALUE_POINTERS")
		os.Unsetenv("TREEDB_DISABLE_VALUE_LOG")
	}()

	opts := Options{Dir: dir}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Mock a compression profile to force immediate dictionary compression.
	// This avoids waiting for background training which might not finish in time
	// for the plain-text check.
	dict := make([]byte, 32768)
	copy(dict, "ibc/facks/ports/transfer/channels/channel-2/sequences/")
	db.SlabManager().ForceAcceptProfileForTesting(&slab.ActiveCompressionProfile{
		Dict: dict,
		K:    1,
	})

	// Write redundant data with long keys and small values
	keyBase := "s/k:ibc/facks/ports/transfer/channels/channel-2/sequences/"
	valBase := bytes.Repeat([]byte("highly_compressible_value_with_lots_of_redundancy_"), 10) // ~500 bytes

	batch := db.NewBatch()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyBase, i))
		batch.Set(key, valBase)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	db.Close()

	// Find slab file
	matches, _ := filepath.Glob(dir + "/data-*.slab")
	if len(matches) == 0 {
		t.Fatalf("No slab files found")
	}
	slabPath := matches[0]
	content, err := os.ReadFile(slabPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Verify that the keyBase does NOT appear in plain text many times.
	// Since we wrote 100 entries, if it's NOT compressed, it should appear 100 times.
	// If it IS compressed (Full Record), it should NOT appear in plain text at all (except maybe once if zstd didn't compress one).
	count := bytes.Count(content, []byte("ibc/facks/ports/transfer"))
	t.Logf("Found %d occurrences of key prefix in slab", count)

	if count > 10 { // Allow a few for safety/edge cases, but 100 would definitely be uncompressed
		t.Errorf("Too many plain text key prefixes found in slab (%d), full record compression might be failing", count)
	}

	// Re-open and verify data integrity
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("%s%d", keyBase, i))
		val := valBase
		got, err := db2.Get(key)
		if err != nil {
			t.Errorf("Get(%s): %v", string(key), err)
			continue
		}
		if !bytes.Equal(got, val) {
			t.Errorf("Data mismatch for %s", string(key))
		}
	}
}

func TestCompression_E2E_LeafPrefix(t *testing.T) {
	dir := t.TempDir()

	// Enable leaf prefix compression
	os.Setenv("TREEDB_LEAF_PREFIX_COMPRESSION", "1")
	defer os.Unsetenv("TREEDB_LEAF_PREFIX_COMPRESSION")

	opts := Options{Dir: dir}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write many keys with a common long prefix
	prefix := "common_prefix_for_testing_leaf_prefix_compression_with_many_keys_"
	batch := db.NewBatch()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%s%04d", prefix, i))
		val := []byte("value")
		batch.Set(key, val)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	db.Close()

	// Re-open and verify data
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%s%04d", prefix, i))
		got, err := db2.Get(key)
		if err != nil {
			t.Errorf("Get(%s): %v", string(key), err)
			continue
		}
		if string(got) != "value" {
			t.Errorf("Data mismatch")
		}
	}
}

func TestCompression_E2E_LeafPrefix_Transition(t *testing.T) {
	dir := t.TempDir()

	// 1. Open WITHOUT leaf prefix compression
	os.Setenv("TREEDB_LEAF_PREFIX_COMPRESSION", "0")
	opts := Options{Dir: dir}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}

	prefix1 := "prefix_one_without_compression_"
	batch1 := db.NewBatch()
	for i := 0; i < 100; i++ {
		batch1.Set([]byte(fmt.Sprintf("%s%04d", prefix1, i)), []byte("val1"))
	}
	if err := batch1.Write(); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	db.Close()

	// 2. Re-open WITH leaf prefix compression
	os.Setenv("TREEDB_LEAF_PREFIX_COMPRESSION", "1")
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}

	prefix2 := "prefix_two_WITH_compression_"
	batch2 := db2.NewBatch()
	for i := 0; i < 100; i++ {
		batch2.Set([]byte(fmt.Sprintf("%s%04d", prefix2, i)), []byte("val2"))
	}
	if err := batch2.Write(); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	db2.Close()

	// 3. Verify both sets of data
	db3, err := Open(opts)
	if err != nil {
		t.Fatalf("Open 3: %v", err)
	}

	for i := 0; i < 100; i++ {
		k1 := []byte(fmt.Sprintf("%s%04d", prefix1, i))
		v1, err := db3.Get(k1)
		if err != nil || string(v1) != "val1" {
			t.Errorf("Mismatch for set 1 key %d: %v", i, err)
		}

		k2 := []byte(fmt.Sprintf("%s%04d", prefix2, i))
		v2, err := db3.Get(k2)
		if err != nil || string(v2) != "val2" {
			t.Errorf("Mismatch for set 2 key %d: %v", i, err)
		}
	}

	// 4. Compact index (should apply prefix compression to EVERYTHING)
	if err := db3.CompactIndex(); err != nil {
		t.Fatalf("CompactIndex: %v", err)
	}

	// 5. Verify again after compaction
	for i := 0; i < 100; i++ {
		k1 := []byte(fmt.Sprintf("%s%04d", prefix1, i))
		v1, err := db3.Get(k1)
		if err != nil || string(v1) != "val1" {
			t.Errorf("Mismatch post-compact for set 1 key %d: %v", i, err)
		}

		k2 := []byte(fmt.Sprintf("%s%04d", prefix2, i))
		v2, err := db3.Get(k2)
		if err != nil || string(v2) != "val2" {
			t.Errorf("Mismatch post-compact for set 2 key %d: %v", i, err)
		}
	}
	db3.Close()
}

func TestCompression_E2E_LeafPrefix_Reverse(t *testing.T) {
	dir := t.TempDir()

	os.Setenv("TREEDB_LEAF_PREFIX_COMPRESSION", "1")
	defer os.Unsetenv("TREEDB_LEAF_PREFIX_COMPRESSION")

	opts := Options{Dir: dir}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	prefix := "common_prefix_"
	batch := db.NewBatch()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("%s%04d", prefix, i))
		batch.Set(key, []byte("val"))
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify reverse iteration
	it, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	defer it.Close()

	count := 0
	for i := 99; i >= 0; i-- {
		if !it.Valid() {
			t.Fatalf("Iterator exhausted prematurely at i=%d", i)
		}
		expectedKey := []byte(fmt.Sprintf("%s%04d", prefix, i))
		if !bytes.Equal(it.Key(), expectedKey) {
			t.Errorf("Key mismatch at %d: got %s, want %s", i, string(it.Key()), string(expectedKey))
		}
		it.Next()
		count++
	}
	if count != 100 {
		t.Errorf("Expected 100 entries, got %d", count)
	}
	db.Close()
}
