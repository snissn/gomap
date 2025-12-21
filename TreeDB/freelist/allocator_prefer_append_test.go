package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_PreferAppendAlloc_IgnoresFreelist(t *testing.T) {
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
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	if err := a.Free(1); err != nil {
		t.Fatalf("Free(1): %v", err)
	}

	a.SetPreferAppend(true)

	got, err := a.Alloc(0)
	if err != nil {
		t.Fatalf("Alloc(): %v", err)
	}
	if got != 3 {
		t.Fatalf("expected appended page 3, got %d", got)
	}
}

func TestAllocator_PreferAppendAlloc_CanToggleBack(t *testing.T) {
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
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	if err := a.Free(1); err != nil {
		t.Fatalf("Free(1): %v", err)
	}

	a.SetPreferAppend(true)
	if got, err := a.Alloc(0); err != nil {
		t.Fatalf("Alloc(prefer): %v", err)
	} else if got != 3 {
		t.Fatalf("expected appended page 3, got %d", got)
	}

	a.SetPreferAppend(false)
	got, err := a.Alloc(0)
	if err != nil {
		t.Fatalf("Alloc(freelist): %v", err)
	}
	if got != 1 {
		t.Fatalf("expected freelist reuse page 1, got %d", got)
	}
}
