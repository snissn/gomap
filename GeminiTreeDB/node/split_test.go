package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap-gemini/TreeDB/page"
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

func checkKey(t *testing.T, n *Node, idx uint16, expected string) {
	entry, _ := n.GetInternalEntry(idx)
	if !bytes.Equal(entry.Key, []byte(expected)) {
		t.Errorf("Index %d: expected %s, got %s", idx, expected, entry.Key)
	}
}
