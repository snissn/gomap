package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_AllocClearsVerifiedBit(t *testing.T) {
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

	a := New(p, 0)

	// Make page 2 the freelist head, then add page 1 to the freelist body.
	p.MarkVerified(1)
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	if err := a.Free(1); err != nil {
		t.Fatalf("Free(1): %v", err)
	}
	if !p.IsVerified(1) {
		t.Fatalf("expected page 1 to remain verified until reused")
	}

	got, err := a.Alloc(0)
	if err != nil {
		t.Fatalf("Alloc(): %v", err)
	}
	if got != 1 {
		t.Fatalf("expected to reuse page 1, got %d", got)
	}
	if p.IsVerified(1) {
		t.Fatalf("expected page 1 to be unverified when reused")
	}
}
