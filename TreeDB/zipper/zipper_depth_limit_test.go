package zipper

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestZipperDepthLimit(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	sm, err := slab.NewSlabManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	makeLeaf := func(key, value []byte) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := node.NewBuilder(data, page.PageTypeLeaf)
		b.SetPageID(id)
		if err := b.AddLeafEntry(key, value, node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("add leaf: %v", err)
		}
		b.Finish()
		return id
	}

	targetKey := []byte("a")
	targetVal := []byte("value")
	leafID := makeLeaf(targetKey, targetVal)

	// Build a chain of single-child internal nodes; these should collapse on write.
	currentID := leafID
	for i := 0; i < 60; i++ {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc internal: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get internal: %v", err)
		}
		b := node.NewBuilder(data, page.PageTypeInternal)
		b.SetPageID(id)
		if err := b.AddInternalChild([]byte{}, currentID); err != nil {
			t.Fatalf("add child: %v", err)
		}
		b.Finish()
		currentID = id
	}

	b := batch.New(sm, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	if err := b.Set(targetKey, []byte("next")); err != nil {
		t.Fatalf("set: %v", err)
	}

	_, _, _, err = z.Apply(currentID, b)
	if err != nil {
		t.Fatalf("unexpected depth error: %v", err)
	}
}
