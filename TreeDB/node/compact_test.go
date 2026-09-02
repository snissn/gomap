package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafOverwriteTriggersCompactionAndReusesSpace(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetPageID(123)
	n.SetType(page.PageTypeLeaf)
	n.SetCount(0)
	n.UpdateChecksum()

	key := []byte("k")
	val := bytes.Repeat([]byte("v"), 100)

	if err := n.AddLeafEntry(key, val, FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry initial: %v", err)
	}

	// Without in-page compaction, repeated overwrites quickly exhaust heap space
	// due to dead entries. With compaction, this should succeed.
	for i := 0; i < 500; i++ {
		if err := n.AddLeafEntry(key, val, FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry overwrite #%d: %v", i, err)
		}
	}

	got, err := n.GetLeafEntry(0)
	if err != nil {
		t.Fatalf("GetLeafEntry: %v", err)
	}
	if !bytes.Equal(got.Key, key) {
		t.Fatalf("key mismatch: got %q want %q", got.Key, key)
	}
	if !bytes.Equal(got.Value, val) {
		t.Fatalf("value mismatch: got len=%d want len=%d", len(got.Value), len(val))
	}
}

func TestCompactPreservesLeafOrdering(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetPageID(1)
	n.SetType(page.PageTypeLeaf)
	n.SetCount(0)
	n.UpdateChecksum()

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, k := range keys {
		if err := n.AddLeafEntry(k, []byte("x"), FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry %q: %v", k, err)
		}
	}

	// Create some holes.
	for i := 0; i < 50; i++ {
		if err := n.AddLeafEntry([]byte("b"), []byte("x"), FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("overwrite b: %v", err)
		}
	}

	if err := n.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for i, want := range keys {
		got, err := n.GetLeafEntry(uint16(i))
		if err != nil {
			t.Fatalf("GetLeafEntry[%d]: %v", i, err)
		}
		if !bytes.Equal(got.Key, want) {
			t.Fatalf("key[%d] mismatch: got %q want %q", i, got.Key, want)
		}
	}
}
