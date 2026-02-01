package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_FreeMany_EmptyHeadCreatesFreelist(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 4*1024*1024)
	if err != nil {
		t.Fatalf("pager open: %v", err)
	}
	defer p.Close()

	a := New(p, 0)

	// Reserve page 0 so allocated IDs are non-zero.
	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc reserve: %v", err)
	}

	base, err := p.Alloc(3)
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	ids := []uint64{base, base + 1, base + 2}

	n, err := a.FreeMany(ids)
	if err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected freed=3, got %d", n)
	}
	if h := a.Head(); h == 0 {
		t.Fatalf("expected non-zero head after FreeMany")
	}
}

func TestAllocator_FreeMany_ThenAllocManyReusesAll(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 4*1024*1024)
	if err != nil {
		t.Fatalf("pager open: %v", err)
	}
	defer p.Close()

	a := New(p, 0)

	// Reserve page 0 so allocated IDs are non-zero.
	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc reserve: %v", err)
	}

	const count = 4096
	base, err := p.Alloc(count)
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	ids := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		ids = append(ids, base+uint64(i))
	}

	n, err := a.FreeMany(ids)
	if err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	if n != len(ids) {
		t.Fatalf("expected freed=%d, got %d", len(ids), n)
	}

	out, err := a.AllocMany(len(ids), 0)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	if len(out) != len(ids) {
		t.Fatalf("expected alloc=%d, got %d", len(ids), len(out))
	}
}

func TestAllocator_FreeMany_MatchesFreeLoopBehavior(t *testing.T) {
	dir1 := t.TempDir()
	p1, err := pager.Open(filepath.Join(dir1, "index.db"), 4*1024*1024)
	if err != nil {
		t.Fatalf("pager open: %v", err)
	}
	defer p1.Close()
	a1 := New(p1, 0)

	dir2 := t.TempDir()
	p2, err := pager.Open(filepath.Join(dir2, "index.db"), 4*1024*1024)
	if err != nil {
		t.Fatalf("pager open: %v", err)
	}
	defer p2.Close()
	a2 := New(p2, 0)

	const count = 6000
	if _, err := p1.Alloc(1); err != nil {
		t.Fatalf("Alloc reserve p1: %v", err)
	}
	base1, err := p1.Alloc(count)
	if err != nil {
		t.Fatalf("Alloc p1: %v", err)
	}
	ids1 := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		ids1 = append(ids1, base1+uint64(i))
	}

	if _, err := p2.Alloc(1); err != nil {
		t.Fatalf("Alloc reserve p2: %v", err)
	}
	base2, err := p2.Alloc(count)
	if err != nil {
		t.Fatalf("Alloc p2: %v", err)
	}
	ids2 := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		ids2 = append(ids2, base2+uint64(i))
	}

	if _, err := a1.FreeMany(ids1); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	for _, id := range ids2 {
		if err := a2.Free(id); err != nil {
			t.Fatalf("Free loop: %v", err)
		}
	}

	s1, err := a1.Stats(1 << 20)
	if err != nil {
		t.Fatalf("Stats a1: %v", err)
	}
	s2, err := a2.Stats(1 << 20)
	if err != nil {
		t.Fatalf("Stats a2: %v", err)
	}
	if s1.ReclaimablePages() != s2.ReclaimablePages() {
		t.Fatalf("expected reclaimable match: a1=%d a2=%d", s1.ReclaimablePages(), s2.ReclaimablePages())
	}
}
