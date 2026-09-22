package freelist

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_NoDuplicateAllocations(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	const pages = 64
	if _, err := p.Alloc(pages); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := New(p, 0)
	ids := make([]uint64, 0, pages-1)
	for i := 1; i < pages; i++ {
		ids = append(ids, uint64(i))
	}

	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

	for _, id := range ids {
		if err := a.Free(id); err != nil {
			t.Fatalf("Free(%d): %v", id, err)
		}
	}

	seen := make(map[uint64]struct{}, len(ids))
	for range ids {
		got, err := a.Alloc(0)
		if err != nil {
			t.Fatalf("Alloc: %v", err)
		}
		if got == 0 || got >= pages {
			t.Fatalf("expected freelist id in [1,%d), got %d", pages, got)
		}
		if _, ok := seen[got]; ok {
			t.Fatalf("duplicate allocation of page %d", got)
		}
		seen[got] = struct{}{}
	}

	got, err := a.Alloc(0)
	if err != nil {
		t.Fatalf("Alloc after exhaustion: %v", err)
	}
	if got < pages {
		t.Fatalf("expected allocator to extend pager, got %d", got)
	}
}
