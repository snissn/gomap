package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_FreeCreatesHead(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(2); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := New(p, 0)
	if a.Head() != 0 {
		t.Fatalf("expected initial head=0, got %d", a.Head())
	}
	if err := a.Free(1); err != nil {
		t.Fatalf("Free: %v", err)
	}
	if a.Head() == 0 {
		t.Fatalf("expected freelist head to become non-zero after first free")
	}
}

func TestAllocator_AllocManyUsesFreelistBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	// Create some pages to recycle.
	if _, err := p.Alloc(10); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := New(p, 0)
	// Free a few pages (>=3).
	if err := a.Free(9); err != nil {
		t.Fatalf("Free(9): %v", err)
	}
	if err := a.Free(8); err != nil {
		t.Fatalf("Free(8): %v", err)
	}
	if err := a.Free(7); err != nil {
		t.Fatalf("Free(7): %v", err)
	}

	f0, a0 := a.AllocCounters()
	ids, err := a.AllocMany(3, 0)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 ids, got %d", len(ids))
	}
	f1, a1 := a.AllocCounters()
	if f1 <= f0 {
		t.Fatalf("expected freelist alloc counter to increase (before=%d after=%d)", f0, f1)
	}
	if a1 != a0 {
		t.Fatalf("expected no append allocs while freelist satisfies request (before=%d after=%d)", a0, a1)
	}
}
