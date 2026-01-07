package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func prefixTestOptions(dir string) Options {
	return Options{
		Dir:            dir,
		FlushThreshold: 4 * 1024 * 1024,
		ChunkSize:      1024 * 1024, // 1MB chunks
		Mode:           ModeCached,
	}
}

func TestIndex_PrefixCompression_Effectiveness(t *testing.T) {
	dir := t.TempDir()
	
	// Write keys with shared prefixes
	// user/alice/post/0001 ... 1000
	prefix := "user/alice/post/"
	count := 50000
	
	// Control: No Compression
	var sizeUncompressed int64
	{
		d := t.TempDir()
		opts := prefixTestOptions(d)
		opts.LeafPrefixCompression = false
		opts.ForceValuePointers = true // Keep values out of index to measure keys
		
		db, _ := Open(opts)
		batch := db.NewBatch()
		val := []byte{1}
		for i := 0; i < count; i++ {
			batch.Set([]byte(fmt.Sprintf("%s%04d", prefix, i)), val)
		}
		batch.Write()
		db.Close()
		
		info, _ := os.Stat(filepath.Join(d, "index.db"))
		sizeUncompressed = info.Size()
	}
	
	// Test: Compression Enabled
	var sizeCompressed int64
	{
		opts := prefixTestOptions(dir)
		opts.LeafPrefixCompression = true
		opts.ForceValuePointers = true
		
		db, _ := Open(opts)
		batch := db.NewBatch()
		val := []byte{1}
		for i := 0; i < count; i++ {
			batch.Set([]byte(fmt.Sprintf("%s%04d", prefix, i)), val)
		}
		batch.Write()
		db.Close()
		
		info, _ := os.Stat(filepath.Join(dir, "index.db"))
		sizeCompressed = info.Size()
	}
	
	t.Logf("Index Size: Uncompressed=%d, Compressed=%d", sizeUncompressed, sizeCompressed)
	
	if sizeCompressed >= sizeUncompressed {
		t.Errorf("Prefix compression failed! Size did not decrease.")
	}
	
	// Expect at least 30% reduction (prefix is 16 bytes, suffix 4 bytes)
	// Entry overhead exists, but prefix saving is significant.
	ratio := float64(sizeCompressed) / float64(sizeUncompressed)
	if ratio > 0.7 {
		t.Errorf("Prefix compression weak. Ratio %.2f (expected < 0.7)", ratio)
	}
}
