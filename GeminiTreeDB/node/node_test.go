package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/GeminiTreeDB/page"
)

func TestLeafNode(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(1)

	// Add entries out of order
	keys := [][]byte{
		[]byte("key3"),
		[]byte("key1"),
		[]byte("key2"),
	}
	vals := [][]byte{
		[]byte("val3"),
		[]byte("val1"),
		[]byte("val2"),
	}

	for i, k := range keys {
		err := n.AddLeafEntry(k, vals[i], FlagInline, page.ValuePtr{})
		if err != nil {
			t.Fatalf("AddLeafEntry failed: %v", err)
		}
	}

	if n.Count() != 3 {
		t.Errorf("Expected 3 items, got %d", n.Count())
	}

	// Verify Sorted Order
	expectedKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for i := uint16(0); i < 3; i++ {
		entry, err := n.GetLeafEntry(i)
		if err != nil {
			t.Fatalf("GetLeafEntry(%d) failed: %v", i, err)
		}
		if !bytes.Equal(entry.Key, expectedKeys[i]) {
			t.Errorf("Index %d: expected key %s, got %s", i, expectedKeys[i], entry.Key)
		}
	}

	// Verify Search
	idx, found := n.SearchLeaf([]byte("key2"))
	if !found || idx != 1 {
		t.Errorf("Search key2: expected found=true idx=1, got found=%v idx=%d", found, idx)
	}

	idx, found = n.SearchLeaf([]byte("key1.5"))
	if found || idx != 1 {
		t.Errorf("Search key1.5: expected found=false idx=1, got found=%v idx=%d", found, idx)
	}
}

func TestInternalNode(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetType(page.PageTypeInternal)

	// Add keys: 10, 30, 20
	// Sorted: 10, 20, 30
	inputs := []struct {
		Key []byte
		ID  uint64
	}{
		{[]byte("10"), 100},
		{[]byte("30"), 300},
		{[]byte("20"), 200},
	}

	for _, in := range inputs {
		err := n.AddInternalChild(in.Key, in.ID)
		if err != nil {
			t.Fatalf("AddInternalChild failed: %v", err)
		}
	}

	// Verify Order
	expected := []struct {
		Key []byte
		ID  uint64
	}{
		{[]byte("10"), 100},
		{[]byte("20"), 200},
		{[]byte("30"), 300},
	}

	for i := uint16(0); i < 3; i++ {
		entry, err := n.GetInternalEntry(i)
		if err != nil {
			t.Fatalf("GetInternalEntry(%d) failed: %v", i, err)
		}
		if !bytes.Equal(entry.Key, expected[i].Key) {
			t.Errorf("Index %d: expected key %s, got %s", i, expected[i].Key, entry.Key)
		}
		if entry.ChildPageID != expected[i].ID {
			t.Errorf("Index %d: expected ID %d, got %d", i, expected[i].ID, entry.ChildPageID)
		}
	}

	// Test Search
	// Keys: 10, 20, 30
	cases := []struct {
		Key      string
		Expected uint16
	}{
		{"05", 0}, // < 10 -> Child 0
		{"10", 0}, // == 10 -> Child 0 (since key<=key found?) 
		           // Wait, SearchInternal: "Find largest i such that Entry[i].Key <= Key"
		           // Entry[0]=10. If Key=10, 10<=10 is true. Entry[1]=20. 20<=10 is false.
		           // So returns 0. Correct.
		{"15", 0}, // 10 <= 15. 20 <= 15 (False). -> 0.
		{"20", 1}, // 20 <= 20. -> 1.
		{"25", 1}, // 20 <= 25. -> 1.
		{"35", 2}, // 30 <= 35. -> 2.
	}

	for _, c := range cases {
		idx, _ := n.SearchInternal([]byte(c.Key))
		if idx != c.Expected {
			t.Errorf("Search(%s): expected %d, got %d", c.Key, c.Expected, idx)
		}
	}
}

func TestSpaceAccounting(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetType(page.PageTypeLeaf)

	initialFree := n.FreeSpace()
	// Header(16). Free = 4096 - 16 = 4080.
	if initialFree != 4080 {
		t.Errorf("Initial free space: expected 4080, got %d", initialFree)
	}

	// Add one entry
	// Key=1 (1 byte), Val=1 (1 byte).
	// Entry Overhead: 7 bytes. Total Entry Data: 9 bytes.
	// Directory: 2 bytes.
	// Total consumed: 11 bytes.
	n.AddLeafEntry([]byte("k"), []byte("v"), FlagInline, page.ValuePtr{})
	
	newFree := n.FreeSpace()
	if newFree != 4080-11 {
		t.Errorf("Free space after insert: expected %d, got %d", 4080-11, newFree)
	}
}
