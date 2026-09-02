package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestMaintenance_RepeatedOverwrite_DoesNotExplodePageCount(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                   dir,
		KeepRecent:            1,
		LeafFillTargetPPM:     850_000,
		InternalFillTargetPPM: 900_000,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 5000
	valA := bytes.Repeat([]byte("a"), 32)
	valB := bytes.Repeat([]byte("b"), 32)

	// Seed dataset in a few batches.
	{
		const batchSize = 512
		for base := 0; base < keys; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := base + batchSize
			if limit > keys {
				limit = keys
			}
			for i := base; i < limit; i++ {
				k := []byte{byte(i >> 8), byte(i)}
				if err := b.Set(k, valA); err != nil {
					t.Fatalf("seed set: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("seed write: %v", err)
			}
			_ = b.Close()
		}
	}

	basePages := d.Pager().PageCount()
	if basePages == 0 {
		t.Fatalf("expected non-zero base page count")
	}

	baseRep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport base: %v", err)
	}
	baseUserPagesStr := baseRep["treedb.user.pages"]
	if baseUserPagesStr == "" {
		t.Fatalf("missing treedb.user.pages")
	}
	baseUserPages, err := strconv.ParseUint(baseUserPagesStr, 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages: %v", err)
	}

	// Repeated overwrites should recycle pages via pruning/freelist reuse and not
	// cause unbounded index growth.
	for round := 0; round < 30; round++ {
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valB); err != nil {
				t.Fatalf("overwrite set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("overwrite write: %v", err)
		}
		_ = b.Close()
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	finalUserPagesStr := rep["treedb.user.pages"]
	if finalUserPagesStr == "" {
		t.Fatalf("missing treedb.user.pages (final)")
	}
	finalUserPages, err := strconv.ParseUint(finalUserPagesStr, 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages (final): %v", err)
	}
	// Live page count should remain roughly stable across overwrites.
	// (Note: the file page count may still grow due to append-only behavior and
	// internal bookkeeping pages.)
	if finalUserPages > baseUserPages*2 {
		t.Fatalf("expected bounded live page count, base=%d final=%d (file pages base=%d final=%d)",
			baseUserPages, finalUserPages, basePages, d.Pager().PageCount())
	}

	avgStr := rep["treedb.user.leaf_fill_ppm_avg"]
	if avgStr == "" {
		t.Fatalf("missing leaf_fill_ppm_avg")
	}
	avg, err := strconv.ParseUint(avgStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf_fill_ppm_avg: %v", err)
	}
	// After many overwrites, leaf fill should remain reasonably high with the
	// soft-full targets enabled.
	if avg < 500_000 {
		t.Fatalf("expected leaf fill avg >= 500k ppm, got %d", avg)
	}
}
