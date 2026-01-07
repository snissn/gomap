package db

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/slab"
)

func TestSlabCompression_Verify(t *testing.T) {
	dir := t.TempDir()
	
	// Create compressible data (repeated pattern)
	// 10KB of 'A'
	data := bytes.Repeat([]byte{'A'}, 1024*10)
	
	opts := Options{
		Dir: dir,
		DisableWAL: true,
		AllowUnsafe: true,
		SlabCompression: slab.CompressionOptions{
			Kind: slab.CompressionZSTD,
			MinBytes: 100, // Compress everything > 100 bytes
			MinSavingsBytes: 10,
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("compress_me")
	if err := db.Set(key, data); err != nil {
		t.Fatal(err)
	}

	// Force flush to backend slab (Set in Backend mode writes to slab immediately? 
	// Yes, commitBatch writes to slab).
	
	// Verify we can read it back
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Data mismatch")
	}

	// Check slab file size
	// It should be significantly smaller than 10KB + overhead.
	// ZSTD should compress 10KB of 'A' to < 100 bytes.
	
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	
	var slabPath string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "data-") && strings.HasSuffix(e.Name(), ".slab") {
			slabPath = filepath.Join(dir, e.Name())
			break
		}
	}
	
	if slabPath == "" {
		t.Fatal("No slab file found")
	}

	info, err := os.Stat(slabPath)
	if err != nil {
		t.Fatal(err)
	}
	
	size := info.Size()
	t.Logf("Slab size: %d bytes (Original data: %d)", size, len(data))
	
	if size > 1000 {
		t.Errorf("Slab size %d is too large, compression failed?", size)
	} else {
		t.Log("Compression confirmed working.")
	}
}
