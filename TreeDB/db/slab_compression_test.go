package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/slab"
)

func TestSlab_Compression_Effectiveness(t *testing.T) {
	dir := t.TempDir()
	
	// Create highly compressible data (zeros)
	val := make([]byte, 1024) // 1KB of zeros
	count := 1000 // 1MB total
	
	opts := DefaultOptions(dir)
	opts.ForceValuePointers = true
	// Enable Compression
	opts.SlabCompression = slab.CompressionOptions{
		Kind:            slab.CompressionZSTD,
		MinBytes:        1,
		MinSavingsBytes: 0,
	}
	
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	
	batch := db.NewBatch()
	for i := 0; i < count; i++ {
		// Key must be unique
		// key len 8 + val len 1024 = 1032.
		// Total logical ~1MB.
		if err := batch.Set([]byte(fmt.Sprintf("%08d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	db.Close() // Flush
	
	// Verify Slab Size
	// Logical size: 1MB.
	// Zstd on zeros should be tiny. Headers overhead dominates.
	// Header: 10 bytes + Key 8 bytes = 18 bytes.
	// 1000 * 18 = 18KB.
	// Compressed value: ~10 bytes?
	// Total should be < 50KB.
	
	entries, _ := os.ReadDir(dir)
	var slabSize int64
	for _, e := range entries {
		if len(e.Name()) > 5 && e.Name()[:5] == "data-" {
			info, _ := e.Info()
			slabSize += info.Size()
		}
	}
	
	t.Logf("Total Slab Size: %d bytes (Logical: ~1MB)", slabSize)
	
	if slabSize > 200 * 1024 { // 200KB limit (generous)
		t.Errorf("Compression failed! Slab size %d is too large for 1MB of zeros", slabSize)
	}
}
