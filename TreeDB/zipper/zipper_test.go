package zipper

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/tree"
)

type MockAllocator struct {
	p *pager.Pager
}

func (m *MockAllocator) Alloc(hint uint64) (uint64, error) {
	return m.p.Alloc(1)
}

func TestZipperInsertSplit(t *testing.T) {
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

	// Create initial root (Leaf)
	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	// Batch 1: Insert enough to cause split
	// PageSize = 4096. Entry overhead ~10 bytes.
	// If key=10 bytes, val=10 bytes -> 30 bytes/entry.
	// 4000 / 30 = ~133 entries.

	b := batch.New(sm, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	// Insert 200 items
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		b.Set(key, val)
	}

	newRootID, _, _, err := z.Apply(rootID, b)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if newRootID == rootID {
		t.Error("Expected new root ID (COW)")
	}

	// Verify Data using Tree
	tr := tree.New(p, sm, newRootID)

	val, err := tr.Get([]byte("key-000"))
	if err != nil {
		t.Error("Failed to get key-000")
	}
	if !bytes.Equal(val, []byte("val-000")) {
		t.Errorf("Value mismatch: %s", val)
	}

	val, err = tr.Get([]byte("key-199"))
	if err != nil {
		t.Error("Failed to get key-199")
	}
	if !bytes.Equal(val, []byte("val-199")) {
		t.Errorf("Value mismatch: %s", val)
	}

	// Check Old Root untouched (basic check)
	// We can't easily check content without parsing, but ID check passed.
}

func TestZipperUpdates(t *testing.T) {
	// Setup same as above...
	dir := t.TempDir()
	p, _ := pager.Open(filepath.Join(dir, "index.db"), 65536)
	defer p.Close()
	sm, _ := slab.NewSlabManager(dir)
	defer sm.Close()
	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	// Init Root
	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	// Batch 1: Insert A, B
	b1 := batch.New(sm, page.DefaultInlineThreshold)
	defer func() { _ = b1.Close() }()
	b1.Set([]byte("A"), []byte("valA"))
	b1.Set([]byte("B"), []byte("valB"))

	root2, _, _, err := z.Apply(rootID, b1)
	if err != nil {
		t.Fatal(err)
	}

	// Batch 2: Update A, Delete B, Insert C
	b2 := batch.New(sm, page.DefaultInlineThreshold)
	defer func() { _ = b2.Close() }()
	b2.Set([]byte("A"), []byte("valA2"))
	b2.Delete([]byte("B"))
	b2.Set([]byte("C"), []byte("valC"))

	root3, _, _, err := z.Apply(root2, b2)
	if err != nil {
		t.Fatal(err)
	}

	tr := tree.New(p, sm, root3)

	// Check A updated
	val, err := tr.Get([]byte("A"))
	if !bytes.Equal(val, []byte("valA2")) {
		t.Errorf("A mismatch: %s", val)
	}

	// Check B deleted
	_, err = tr.Get([]byte("B"))
	if err != tree.ErrKeyNotFound {
		t.Errorf("B should be deleted, got %v", err)
	}

	// Check C inserted
	val, err = tr.Get([]byte("C"))
	if !bytes.Equal(val, []byte("valC")) {
		t.Errorf("C mismatch: %s", val)
	}
}

func TestCoalesceLeafChildrenPrefixCompression(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetLeafPrefixCompression(true)

	buildLeaf := func(keys []string, valSize int) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true})
		b.SetPageID(id)
		val := bytes.Repeat([]byte("v"), valSize)
		for _, k := range keys {
			if err := b.AddLeafEntry([]byte(k), val, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry %q: %v", k, err)
			}
		}
		b.Finish()
		return id
	}

	leftID := buildLeaf([]string{"a0"}, 100)
	rightID := buildLeaf([]string{"b0", "b1", "b2", "b3"}, 1000)

	entries := []internalEntry{
		{key: []byte{}, child: leftID},
		{key: []byte("b0"), child: rightID},
	}

	out, _, err := z.coalesceLeafChildren(entries, &adaptive.Metrics{})
	if err != nil {
		t.Fatalf("coalesceLeafChildren: %v", err)
	}

	var got []string
	for _, e := range out {
		data, err := p.Get(e.child)
		if err != nil {
			t.Fatalf("load leaf %d: %v", e.child, err)
		}
		n := node.NewNode(data)
		for i := uint16(0); i < n.Count(); i++ {
			entry, err := n.GetLeafEntry(i)
			if err != nil {
				t.Fatalf("leaf entry %d: %v", i, err)
			}
			got = append(got, string(entry.Key))
		}
	}

	expected := []string{"a0", "b0", "b1", "b2", "b3"}
	if len(got) != len(expected) {
		t.Fatalf("expected %d keys, got %d (%v)", len(expected), len(got), got)
	}
	for i, k := range expected {
		if got[i] != k {
			t.Fatalf("key[%d] = %q, want %q (all=%v)", i, got[i], k, got)
		}
	}
}
