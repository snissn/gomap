package tree

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestTreeGet(t *testing.T) {
	// Setup Pager
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536) // 64KB chunk (safe for 16KB pages)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	vr := newMapValueReader()

	// Alloc Pages
	// 0: Internal (Root)
	// 1: Leaf (Left)
	// 2: Leaf (Right)
	p0, _ := p.Alloc(1)
	p1, _ := p.Alloc(1)
	p2, _ := p.Alloc(1)

	if p0 != 0 || p1 != 1 || p2 != 2 {
		t.Fatalf("Unexpected page IDs: %d, %d, %d", p0, p1, p2)
	}

	// Build Leaf 1 (Keys "10", "40")
	data1, _ := p.Get(1)
	n1 := node.NewNode(data1)
	n1.SetType(page.PageTypeLeaf)
	n1.SetPageID(1)
	n1.AddLeafEntry([]byte("10"), []byte("val10"), node.FlagInline, page.ValuePtr{})
	n1.AddLeafEntry([]byte("40"), []byte("val40"), node.FlagInline, page.ValuePtr{})
	n1.UpdateChecksum()

	// Build Leaf 2 (Key "60", "huge")
	data2, _ := p.Get(2)
	n2 := node.NewNode(data2)
	n2.SetType(page.PageTypeLeaf)
	n2.SetPageID(2)
	n2.AddLeafEntry([]byte("60"), []byte("val60"), node.FlagInline, page.ValuePtr{})

	// Add Huge Value
	hugeVal := bytes.Repeat([]byte("A"), 1000)
	ptr := vr.Add(hugeVal)
	n2.AddLeafEntry([]byte("huge"), nil, node.FlagPointer, ptr)
	n2.UpdateChecksum()

	// Build Root (Internal)
	// Children:
	// Key "00" -> Page 1 (Covers < "50")
	// Key "50" -> Page 2 (Covers >= "50")
	// Note: Internal Entry[i].Child covers keys >= Entry[i].Key (in my impl?)
	// Wait, let's re-verify Internal Logic in node/internal.go
	// SearchInternal: "Find largest i such that Entry[i].Key <= Key"
	// So if we have:
	// Entry 0: Key="00", Child=1
	// Entry 1: Key="50", Child=2

	// Query "10":
	// "00" <= "10" (True).
	// "50" <= "10" (False).
	// Returns index 0 -> Child 1. Correct.

	// Query "60":
	// "00" <= "60" (True)
	// "50" <= "60" (True)
	// Returns index 1 -> Child 2. Correct.

	data0, _ := p.Get(0)
	n0 := node.NewNode(data0)
	n0.SetType(page.PageTypeInternal)
	n0.SetPageID(0)
	n0.AddInternalChild([]byte("00"), 1)
	n0.AddInternalChild([]byte("50"), 2)
	n0.UpdateChecksum()

	// Init Tree
	tr := New(p, vr, 0)

	// Tests
	cases := []struct {
		Key string
		Val []byte
		Err error
	}{
		{"10", []byte("val10"), nil},
		{"40", []byte("val40"), nil},
		{"60", []byte("val60"), nil},
		{"99", nil, ErrKeyNotFound},
		{"huge", hugeVal, nil},
	}

	for _, c := range cases {
		val, err := tr.Get([]byte(c.Key))
		if err != c.Err {
			t.Errorf("Get(%s): expected error %v, got %v", c.Key, c.Err, err)
		}
		if c.Err == nil && !bytes.Equal(val, c.Val) {
			t.Errorf("Get(%s): value mismatch", c.Key) // Don't print huge val
		}
	}
}
