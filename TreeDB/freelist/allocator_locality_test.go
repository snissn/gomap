package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_RegionBiasedAlloc_SelectsNearby(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(32); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := New(p, 0)
	a.SetFreelistRegion(4, 1)

	if err := a.Free(1); err != nil {
		t.Fatalf("Free(1): %v", err)
	}
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	if err := a.Free(9); err != nil {
		t.Fatalf("Free(9): %v", err)
	}
	if err := a.Free(29); err != nil {
		t.Fatalf("Free(29): %v", err)
	}

	a.mu.Lock()
	a.lastAlloc = 9
	a.mu.Unlock()

	got, err := a.Alloc(0)
	if err != nil {
		t.Fatalf("Alloc(): %v", err)
	}
	if got != 9 {
		t.Fatalf("expected region-biased alloc to return page 9, got %d", got)
	}

	head := a.Head()
	data, err := p.ReadPage(head)
	if err != nil {
		t.Fatalf("ReadPage(%d): %v", head, err)
	}
	if !page.VerifyChecksumNonMutating(data) {
		t.Fatalf("expected freelist head checksum to remain valid")
	}
}

func TestAllocator_RegionBiasedAlloc_FallsBackToLIFO(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(32); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := New(p, 0)
	a.SetFreelistRegion(4, 1)

	if err := a.Free(1); err != nil {
		t.Fatalf("Free(1): %v", err)
	}
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	if err := a.Free(6); err != nil {
		t.Fatalf("Free(6): %v", err)
	}
	if err := a.Free(9); err != nil {
		t.Fatalf("Free(9): %v", err)
	}

	a.mu.Lock()
	a.lastAlloc = 52
	a.mu.Unlock()

	got, err := a.Alloc(0)
	if err != nil {
		t.Fatalf("Alloc(): %v", err)
	}
	if got != 9 {
		t.Fatalf("expected fallback LIFO alloc to return page 9, got %d", got)
	}
}
