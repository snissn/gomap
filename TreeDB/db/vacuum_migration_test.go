package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVacuum_Migration_InlineToSlab(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write Inline Values (ForceValuePointers=false)
	{
		opts := testOptions(dir)
		opts.ForceValuePointers = false
		opts.LeafPrefixCompression = true // Match prod
		db, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}

		batch := db.NewBatch()
		// 100 bytes is < 256 default threshold, so it goes INLINE.
		valInline := bytes.Repeat([]byte{0xBB}, 100)

		for i := 0; i < 5000; i++ {
			if err := batch.Set([]byte(fmt.Sprintf("key-%06d", i)), valInline); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	// Verify Index Size (Should be moderate, containing values)
	info1, _ := os.Stat(filepath.Join(dir, "index.db"))
	t.Logf("Pre-Vacuum Index Size: %d", info1.Size())

	// Phase 2: Open with ForceValuePointers=true and Vacuum
	// Expectation: Vacuum should MOVE inline values to Slabs, reducing Index size.
	{
		opts := testOptions(dir)
		opts.ForceValuePointers = true
		opts.LeafPrefixCompression = true
		// We need to enable Value Index if we want to migrate?
		// ForceValuePointers implies using Slabs.

		db, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if err := db.VacuumIndexOnline(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	info2, _ := os.Stat(filepath.Join(dir, "index.db"))
	t.Logf("Post-Vacuum Index Size: %d", info2.Size())

	// If Migration worked, Index should be SMALLER (pointers are 16 bytes vs 100 bytes value).
	// 5000 * 100 bytes = 500KB data.
	// 5000 * 16 bytes = 80KB pointers.
	// Index size difference should be noticeable.

	if info2.Size() > info1.Size() {
		t.Errorf("Vacuum failed to migrate inline values to slabs! Size grew from %d to %d", info1.Size(), info2.Size())
	} else if float64(info2.Size()) > float64(info1.Size())*0.8 {
		t.Errorf("Vacuum migration insufficient. Size %d -> %d (expected significant reduction)", info1.Size(), info2.Size())
	}
}

func testOptions(dir string) Options {
	return Options{
		Dir:            dir,
		FlushThreshold: 4 * 1024 * 1024,
		ChunkSize:      1024 * 1024, // 1MB chunks
		Mode:           ModeCached,
	}
}
