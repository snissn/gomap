package tree

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestOwnedPointerProjectionFixedScratch(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	n := node.NewNode(data)
	n.SetPageID(root)
	n.SetType(page.PageTypeLeaf)
	ptr := page.ValuePtr{FileID: 9, Offset: 20, Length: 40}
	n.AddLeafEntry([]byte("a"), nil, node.FlagPointer, ptr)
	n.AddLeafEntry([]byte("b"), []byte("inline"), node.FlagInline, page.ValuePtr{})
	n.AddLeafEntry([]byte("c"), nil, node.FlagTombstone, page.ValuePtr{})
	n.UpdateChecksum()
	reader := newCountingValueReader()
	tr := New(p, reader, root)
	it := tr.OwnedPointerProjectionIterator(nil, nil, nil)
	defer it.Close()
	key := []byte("a")
	var keyScratch, leafScratch [page.PageSize]byte
	if got := testing.AllocsPerRun(100, func() {
		it.Seek(key)
		if !it.Valid() || !bytes.Equal(it.Key(), key) {
			t.Fatal("seek")
		}
		_, gotPtr, flags := it.UnsafeEntry()
		if gotPtr != ptr || flags&node.FlagPointer == 0 {
			t.Fatal("pointer projection")
		}
		it.Next()
		if !it.Valid() || string(it.Key()) != "b" || string(it.Value()) != "inline" {
			t.Fatal("inline projection")
		}
		it.Next()
		_, _, flags = it.UnsafeEntry()
		if !it.Valid() || flags&node.FlagTombstone == 0 {
			t.Fatal("tombstone precedence")
		}
		it.Next()
		if it.Valid() || it.Error() != nil {
			t.Fatal("end")
		}
		entry, err := tr.GetEntryWithFixedScratch(key, keyScratch[:], leafScratch[:], nil)
		if err != nil || entry.ValuePtr != ptr {
			t.Fatal("fixed point lookup")
		}
	}); got != 0 {
		t.Fatalf("owned operations allocated %g", got)
	}
	if reader.reads != 0 {
		t.Fatal("projection decoded pointer")
	}
}

func TestOwnedPointerProjectionExternalLeafRequiresBoundedReader(t *testing.T) {
	ptr := page.LeafLogPtr{FileID: 1, Offset: 8}
	data := make([]byte, page.PageSize)
	n := node.NewNode(data)
	n.SetType(page.PageTypeLeaf)
	n.AddLeafEntry([]byte("key"), []byte("value"), node.FlagInline, page.ValuePtr{})
	n.UpdateChecksum()
	reader := &trackedValueReader{mapValueReader: newMapValueReader()}
	reader.values[ptr.ValuePtr()] = data
	tr, closeTree := newTreeWithLeafLogRoot(t, reader, nil, ptr)
	defer closeTree()
	denied := tr.OwnedPointerProjectionIterator(nil, nil, nil)
	if denied.Valid() || denied.Error() != ErrOwnedIteratorLeafReader {
		t.Fatalf("unbounded fallback: %v", denied.Error())
	}
	denied.Close()
	if reader.readUnsafeCalls != 0 {
		t.Fatal("fallback decoded before refusal")
	}
	readLeaf := func(got page.LeafLogPtr, dst []byte) ([]byte, error) {
		if got != ptr || cap(dst) < page.PageSize {
			t.Fatal("wrong leaf owner/scratch")
		}
		copy(dst[:page.PageSize], data)
		return dst[:page.PageSize], nil
	}
	it := tr.OwnedPointerProjectionIterator(nil, nil, readLeaf)
	defer it.Close()
	var keyScratch, leafScratch [page.PageSize]byte
	key := []byte("key")
	if got := testing.AllocsPerRun(100, func() {
		it.Seek(key)
		if !it.Valid() || string(it.Value()) != "value" {
			t.Fatalf("bounded iterator: %v", it.Error())
		}
		entry, err := tr.GetEntryWithFixedScratch(key, keyScratch[:], leafScratch[:], readLeaf)
		if err != nil || string(entry.Value) != "value" {
			t.Fatalf("bounded lookup: %v", err)
		}
	}); got != 0 {
		t.Fatalf("external owned operations allocated %g", got)
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatal("bounded path used global reader")
	}
}

func TestOwnedPointerProjectionAtRootClearsInlineTree(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "projection-root.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	first, err := p.Alloc(2)
	if err != nil {
		t.Fatal(err)
	}
	for offset, key := range []string{"first", "second"} {
		id := first + uint64(offset)
		raw, err := p.GetForWrite(id)
		if err != nil {
			t.Fatal(err)
		}
		n := node.NewNode(raw)
		n.SetPageID(id)
		n.SetType(page.PageTypeLeaf)
		n.AddLeafEntry([]byte(key), []byte("inline"), node.FlagInline, page.ValuePtr{})
		n.UpdateChecksum()
	}
	original := New(p, newCountingValueReader(), first)
	projected := original.OwnedPointerProjectionIteratorAtRoot(first+1, nil, nil, nil).(*Iterator)
	if !projected.Valid() || string(projected.Key()) != "second" || projected.tree == original || projected.tree.slabReader != nil {
		t.Fatal("projection inherited wrong or generic operational tree")
	}
	bound := projected.tree
	if err := projected.Close(); err != nil {
		t.Fatal(err)
	}
	if bound.pager != nil || bound.rootPageID != 0 || projected.tree != nil || original.rootPageID != first {
		t.Fatal("projection Close retained pager or mutated original root")
	}
}
