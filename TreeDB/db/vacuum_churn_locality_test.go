package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestVacuumThenModerateChurn_PreferAppendAlloc_PreservesLocality(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                   dir,
		KeepRecent:            1,
		PreferAppendAlloc:     true,
		LeafFillTargetPPM:     850_000,
		InternalFillTargetPPM: 900_000,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	min := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}

	writeBatched := func(start, end, step int, val []byte) {
		const batchSize = 512
		for base := start; base < end; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := min(end, base+batchSize)
			for i := base; i < limit; i += step {
				k := []byte{byte(i >> 8), byte(i)}
				if err := b.Set(k, val); err != nil {
					t.Fatalf("batch set: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("batch write: %v", err)
			}
			_ = b.Close()
		}
	}

	deleteBatched := func(start, end, step int) {
		const batchSize = 512
		for base := start; base < end; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := min(end, base+batchSize)
			for i := base; i < limit; i += step {
				k := []byte{byte(i >> 8), byte(i)}
				if err := b.Delete(k); err != nil {
					t.Fatalf("batch del: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("batch del write: %v", err)
			}
			_ = b.Close()
		}
	}

	val := bytes.Repeat([]byte("x"), 24)
	writeBatched(0, 5000, 1, val)

	if err := d.CompactIndex(); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	// Moderate churn: overwrite and delete some keys, then add new keys.
	val2 := bytes.Repeat([]byte("y"), 24)
	writeBatched(0, 5000, 3, val2)
	deleteBatched(0, 5000, 5)
	writeBatched(5000, 7500, 1, val)

	// Advance commit seq enough for KeepRecent=1 pruning to kick in for vacuum-retired pages.
	if err := d.SetSync([]byte{0xFF, 0xFE}, val); err != nil {
		t.Fatalf("set3: %v", err)
	}
	if err := d.SetSync([]byte{0xFF, 0xFD}, val); err != nil {
		t.Fatalf("set4: %v", err)
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	ratioStr := rep["treedb.user.pages.span_ratio_ppm"]
	if ratioStr == "" {
		t.Fatalf("missing span_ratio_ppm in report")
	}
	ratio, err := strconv.ParseUint(ratioStr, 10, 64)
	if err != nil {
		t.Fatalf("parse span_ratio_ppm: %v", err)
	}

	// With PreferAppendAlloc enabled, churn mostly appends new pages instead of
	// reusing scattered freelist pages. Note that underfull leaf coalescing may
	// retire pages (creating "holes" within the live span), so the span density
	// can drop even when physical order remains sequential.
	if ratio > 3_000_000 {
		t.Fatalf("expected acceptable locality under churn, span_ratio_ppm=%d", ratio)
	}
}
