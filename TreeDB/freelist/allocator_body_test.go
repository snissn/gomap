package freelist

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_AllocClearsFreelistBodySlot(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	// Allocate pages 0, 1, 2.
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

	if _, err := a.Alloc(0); err != nil {
		t.Fatalf("Alloc(): %v", err)
	}

	data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get(freelist): %v", err)
	}
	n := node.NewNode(data)
	if n.Count() != 0 {
		t.Fatalf("expected freelist count 0, got %d", n.Count())
	}

	slotOff := page.PageHeaderSize + 8
	slot := binary.LittleEndian.Uint64(data[slotOff : slotOff+8])
	if slot != 0 {
		t.Fatalf("expected cleared freelist slot, got %d", slot)
	}
}
