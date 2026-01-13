package db

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/slab"
	"os"
	"path/filepath"
	"testing"
)

// TestCompressionEnabled verifies that setting the options
// enables compression on Slabs and Leaf Prefixes.
func TestCompressionEnabled(t *testing.T) {
	dir, err := os.MkdirTemp("", "compression-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		Dir:                   dir,
		ChunkSize:             64 * 1024 * 1024,
		LeafPrefixCompression: true,
		SlabCompression: slab.CompressionOptions{
			Kind: slab.CompressionZSTD,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Mock a compression profile to force immediate dictionary compression.
	// This avoids waiting for background training.
	db.SlabManager().ForceAcceptProfileForTesting(&slab.ActiveCompressionProfile{
		Dict: bytes.Repeat([]byte("A"), 1024),
		K:    1,
	})

	// 2. Write Compressible Data (Slab Compression)
	// Write a large, repetitive value that should compress well.
	largeVal := bytes.Repeat([]byte("A"), 1024*10) // 10KB of 'A's
	key1 := []byte("prefix/key1")

	batch := db.NewBatch()
	if err := batch.Set(key1, largeVal); err != nil {
		t.Fatal(err)
	}

	// 3. Write Prefix-Compressible Keys (Leaf Compression)
	// Write many keys sharing the same prefix to trigger prefix compression in B-Tree leaves.
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("common/prefix/very/long/key-%04d", i))
		v := []byte("val")
		if err := batch.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}

	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}

	// Force a sync to flush slabs to disk
	// (Note: SlabManager might buffer, but WriteSync/Close should flush active slab)
	// Re-opening DB ensures everything is flushed.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 4. Verify Slab Compression
	// Find slab file in data/ directory
	slabDir := filepath.Join(dir, "data")
	files, err := os.ReadDir(slabDir)
	if err != nil {
		t.Fatal(err)
	}

	foundSlab := false
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".slab" {
			foundSlab = true
			content, err := os.ReadFile(filepath.Join(slabDir, f.Name()))
			if err != nil {
				t.Fatal(err)
			}

			// Simple check: The raw data "AAAA..." (10KB) should NOT appear in plain text
			// if compressed. It might appear if uncompressed.
			if bytes.Contains(content, bytes.Repeat([]byte("A"), 100)) { // Check for a run of 100 As
				t.Errorf("Slab file %s appears uncompressed (found plaintext data)", f.Name())
			} else {
				t.Logf("Slab file %s appears compressed (plaintext data not found)", f.Name())
			}
		}
	}
	if !foundSlab {
		t.Fatal("No slab files found")
	}

	// 5. Verify Read-Back works
	// Re-open and read the data to ensure we can decompress it.
	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, err := db2.Get(key1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, largeVal) {
		t.Errorf("Read-back value mismatch. Got len %d, want %d", len(val), len(largeVal))
	}

	val2, err := db2.Get([]byte("common/prefix/very/long/key-0000"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val2) != "val" {
		t.Errorf("Read-back prefix key mismatch")
	}
}
