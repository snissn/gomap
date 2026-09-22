package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestNodeSplit_Leaf(t *testing.T) {
	// Setup Node with 4 items: "A", "B", "C", "D"
	n1 := NewNode(make([]byte, page.PageSize))
	n1.SetType(page.PageTypeLeaf)
	n1.SetPageID(1)

	keys := []string{"A", "B", "C", "D"}
	for _, k := range keys {
		n1.AddLeafEntry([]byte(k), []byte("val"+k), FlagInline, page.ValuePtr{})
	}

	// Setup New Node
	n2 := NewNode(make([]byte, page.PageSize))
	// Page ID 2
	n2.SetPageID(2)
	// Type is set by Split

	// Split
	pivot, err := n1.Split(n2)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Verify Pivot
	// Split at index 2 (4/2=2).
	// n1 keeps 0, 1 ("A", "B").
	// n2 gets 2, 3 ("C", "D").
	// Pivot should be "C".
	if string(pivot) != "C" {
		t.Errorf("Expected pivot 'C', got '%s'", pivot)
	}

	// Verify n1
	if n1.Count() != 2 {
		t.Errorf("n1 count: expected 2, got %d", n1.Count())
	}
	// Check keys in n1
	for i := uint16(0); i < 2; i++ {
		entry, _ := n1.GetLeafEntry(i)
		if string(entry.Key) != keys[i] {
			t.Errorf("n1 index %d: expected %s, got %s", i, keys[i], entry.Key)
		}
	}

	// Verify n2
	if n2.Count() != 2 {
		t.Errorf("n2 count: expected 2, got %d", n2.Count())
	}
	if n2.Type() != page.PageTypeLeaf {
		t.Error("n2 type not set to Leaf")
	}
	// Check keys in n2
	expectedN2 := []string{"C", "D"}
	for i := uint16(0); i < 2; i++ {
		entry, _ := n2.GetLeafEntry(i)
		if string(entry.Key) != expectedN2[i] {
			t.Errorf("n2 index %d: expected %s, got %s", i, expectedN2[i], entry.Key)
		}
	}
}

func TestNodeSplit_Internal(t *testing.T) {
	// Setup Internal Node with 4 items: "10", "20", "30", "40"
	n1 := NewNode(make([]byte, page.PageSize))
	n1.SetType(page.PageTypeInternal)
	n1.SetPageID(1)

	keys := []string{"10", "20", "30", "40"}
	for _, k := range keys {
		n1.AddInternalChild([]byte(k), 999) // Child ID dummy
	}

	n2 := NewNode(make([]byte, page.PageSize))
	n2.SetPageID(2)

	pivot, err := n1.Split(n2)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Verify Pivot "30"
	if string(pivot) != "30" {
		t.Errorf("Expected pivot '30', got '%s'", pivot)
	}

	// Verify counts
	if n1.Count() != 2 {
		t.Errorf("n1 count: expected 2, got %d", n1.Count())
	}
	if n2.Count() != 2 {
		t.Errorf("n2 count: expected 2, got %d", n2.Count())
	}
	if n2.Type() != page.PageTypeInternal {
		t.Error("n2 type not Internal")
	}

	// Verify content
	// n1: 10, 20
	checkKey(t, n1, 0, "10")
	checkKey(t, n1, 1, "20")

	// n2: 30, 40
	checkKey(t, n2, 0, "30")
	checkKey(t, n2, 1, "40")
}

func TestNodeSplit_Leaf_ColumnarV2(t *testing.T) {
	opts := BuilderOptions{LeafColumnar: true}
	leftBuilder := NewBuilderWithOptions(make([]byte, page.PageSize), page.PageTypeLeaf, opts)
	leftBuilder.SetPageID(11)

	for i := 0; i < 32; i++ {
		key := []byte{byte('a' + (i / 8)), byte('0' + (i % 8)), byte(i)}
		var val []byte
		flags := byte(FlagInline)
		var ptr page.ValuePtr
		if i%3 == 0 {
			flags = FlagPointer
			ptr = page.ValuePtr{Offset: uint64(100 + i), Length: uint32(64 + i), FileID: 3}
		} else {
			val = []byte{byte(i), byte(i + 1), byte(i + 2)}
		}
		if err := leftBuilder.AddLeafEntry(key, val, flags, ptr); err != nil {
			t.Fatalf("add leaf entry %d: %v", i, err)
		}
	}

	n1 := leftBuilder.Finish()
	n2 := NewNode(make([]byte, page.PageSize))
	n2.SetPageID(12)

	pivot, err := n1.Split(n2)
	if err != nil {
		t.Fatalf("split failed: %v", err)
	}
	if len(pivot) == 0 {
		t.Fatalf("expected non-empty pivot")
	}
	if !n1.leafColumnarV2() || !n2.leafColumnarV2() {
		t.Fatalf("expected both nodes to remain columnar v2")
	}

	total := int(n1.Count() + n2.Count())
	if total != 32 {
		t.Fatalf("unexpected total entries after split: got=%d want=32", total)
	}

	var last []byte
	for i := uint16(0); i < n1.Count(); i++ {
		k, _, _, _, err := n1.GetLeafEntryView(i)
		if err != nil {
			t.Fatalf("n1 decode idx=%d: %v", i, err)
		}
		if len(last) > 0 && bytes.Compare(last, k) >= 0 {
			t.Fatalf("n1 not strictly sorted at idx=%d", i)
		}
		last = append(last[:0], k...)
	}
	for i := uint16(0); i < n2.Count(); i++ {
		k, _, _, _, err := n2.GetLeafEntryView(i)
		if err != nil {
			t.Fatalf("n2 decode idx=%d: %v", i, err)
		}
		if len(last) > 0 && bytes.Compare(last, k) >= 0 {
			t.Fatalf("cross-node ordering violated at idx=%d", i)
		}
		last = append(last[:0], k...)
	}
}

func checkKey(t *testing.T, n *Node, idx uint16, expected string) {
	entry, _ := n.GetInternalEntry(idx)
	if !bytes.Equal(entry.Key, []byte(expected)) {
		t.Errorf("Index %d: expected %s, got %s", idx, expected, entry.Key)
	}
}
