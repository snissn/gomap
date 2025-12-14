package tree

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/GeminiTreeDB/node"
	"github.com/snissn/gomap/GeminiTreeDB/page"
	"github.com/snissn/gomap/GeminiTreeDB/pager"
	"github.com/snissn/gomap/GeminiTreeDB/slab"
)

func TestIterator(t *testing.T) {
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

	// Build Tree
	// Root (0) -> Internal
	// Child 1 (Left) -> "A", "C"
	// Child 2 (Right) -> "E", "G"
	
	p.Alloc(3) // 0, 1, 2
	
	// Leaf 1
	d1, _ := p.Get(1)
	n1 := node.NewNode(d1)
	n1.SetPageID(1)
	n1.SetType(page.PageTypeLeaf)
	n1.AddLeafEntry([]byte("A"), []byte("valA"), node.FlagInline, page.ValuePtr{})
	n1.AddLeafEntry([]byte("C"), []byte("valC"), node.FlagInline, page.ValuePtr{})
	n1.UpdateChecksum()
	
	// Leaf 2
	d2, _ := p.Get(2)
	n2 := node.NewNode(d2)
	n2.SetPageID(2)
	n2.SetType(page.PageTypeLeaf)
	n2.AddLeafEntry([]byte("E"), []byte("valE"), node.FlagInline, page.ValuePtr{})
	n2.AddLeafEntry([]byte("G"), []byte("valG"), node.FlagInline, page.ValuePtr{})
	n2.UpdateChecksum()
	
	// Root
	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeInternal)
	n0.AddInternalChild([]byte(""), 1) // Min key
	n0.AddInternalChild([]byte("D"), 2) // Split key
	n0.UpdateChecksum()
	
	tr := New(p, sm, 0)
	
	// 1. Full Forward
	t.Run("Forward", func(t *testing.T) {
		it := tr.Iterator(nil, nil)
		expected := []string{"A", "C", "E", "G"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})
	
	// 2. Full Reverse
	t.Run("Reverse", func(t *testing.T) {
		it := tr.ReverseIterator(nil, nil)
		expected := []string{"G", "E", "C", "A"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})
	
	// 3. Range Forward [B, F) -> C, E
	t.Run("RangeForward", func(t *testing.T) {
		it := tr.Iterator([]byte("B"), []byte("F"))
		expected := []string{"C", "E"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})
	
	// 4. Range Reverse [B, F) -> E, C
	t.Run("RangeReverse", func(t *testing.T) {
		it := tr.ReverseIterator([]byte("B"), []byte("F"))
		expected := []string{"E", "C"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})
}
