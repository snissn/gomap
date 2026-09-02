package db

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestFillPercentiles(t *testing.T) {
	values := make([]uint32, 0, 100)
	for i := uint32(0); i < 100; i++ {
		values = append(values, i)
	}

	stats := fillPercentiles(values)
	if !stats.valid {
		t.Fatalf("expected valid stats")
	}
	if stats.min != 0 || stats.max != 99 {
		t.Fatalf("min/max mismatch: got min=%d max=%d", stats.min, stats.max)
	}
	if stats.p10 != 9 {
		t.Fatalf("p10 mismatch: got %d", stats.p10)
	}
	if stats.p50 != 49 {
		t.Fatalf("p50 mismatch: got %d", stats.p50)
	}
	if stats.p90 != 89 {
		t.Fatalf("p90 mismatch: got %d", stats.p90)
	}
	if stats.p99 != 98 {
		t.Fatalf("p99 mismatch: got %d", stats.p99)
	}
}

func TestReadFreelistStats(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	// Allocate 3 pages: 0, 1, 2.
	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := freelist.New(p, 0)
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	if err := a.Free(1); err != nil {
		t.Fatalf("Free(1): %v", err)
	}

	got, err := a.Stats(p.PageCount())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got.Pages != 1 {
		t.Fatalf("pages mismatch: got %d", got.Pages)
	}
	if got.FreeIDs != 1 {
		t.Fatalf("freeIDs mismatch: got %d", got.FreeIDs)
	}
	if got.ReclaimablePages() != 2 {
		t.Fatalf("reclaimable mismatch: got %d", got.ReclaimablePages())
	}
}
