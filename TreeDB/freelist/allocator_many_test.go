package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_AllocMany(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(5); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := New(p, 0)
	if err := a.Free(4); err != nil {
		t.Fatalf("Free(4): %v", err)
	}
	if err := a.Free(3); err != nil {
		t.Fatalf("Free(3): %v", err)
	}
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}

	ids, err := a.AllocMany(2, 0)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
	want := map[uint64]bool{2: true, 3: true}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected id %d", id)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Fatalf("missing ids: %v", want)
	}

	data, err := p.Get(4)
	if err != nil {
		t.Fatalf("Get(freelist): %v", err)
	}
	n := node.NewNode(data)
	if n.Type() != page.PageTypeFreelist {
		t.Fatalf("expected freelist page, got %d", n.Type())
	}
	if n.Count() != 0 {
		t.Fatalf("expected freelist count 0, got %d", n.Count())
	}
}
