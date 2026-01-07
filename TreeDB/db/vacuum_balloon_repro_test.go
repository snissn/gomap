package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVacuum_Ballooning_Repro(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ForceValuePointers = true   // Force data to slabs
	opts.ChunkSize = 4 * 1024 * 1024 // Small chunks to see file growth

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Write Data
	// Write 10MB of data. With pointers, Index should be small. Slabs should be 10MB.
	val := bytes.Repeat([]byte{0xAA}, 1000) // 1KB value
	count := 10000                          // 10MB total
	batch := db.NewBatch().(*Batch)
	for i := 0; i < count; i++ {
		batch.Set([]byte(fmt.Sprintf("k-%06d", i)), val)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	// 2. Verify Initial Size
	// Index should be small (Keys + Pointers).
	// Key=8, Ptr=16. Entry=~30. 10k entries = 300KB.
	// Index file will be ChunkSize (4MB).
	// Slabs should be ~10MB.

	idxInfo, _ := os.Stat(filepath.Join(dir, "index.db"))
	// slabInfo, _ := os.Stat(filepath.Join(dir, "data-0001.slab"))

	t.Logf("Initial Index Size: %d", idxInfo.Size())
	// t.Logf("Initial Slab Size: %d", slabInfo.Size())

	// 3. Trigger Vacuum
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	// 4. Verify New Index Size
	// If Vacuum works correctly, new index should be similar size (compacted).
	// If Vacuum INLINES values, new index will be ~10MB + overhead.
	// 10MB > 4MB (ChunkSize). So it will grow.

	// Vacuum swaps to index.db.
	newIdxInfo, _ := os.Stat(filepath.Join(dir, "index.db"))
	t.Logf("Post-Vacuum Index Size: %d", newIdxInfo.Size())

	if newIdxInfo.Size() > idxInfo.Size()*2 {
		t.Errorf("Vacuum caused massive ballooning! Old: %d, New: %d. Implies inlining of slab values.", idxInfo.Size(), newIdxInfo.Size())
	}
}
