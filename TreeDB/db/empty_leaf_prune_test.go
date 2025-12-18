package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestDeleteAllKeys_PrunesEmptyLeafChildren(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 32)

	// Insert enough keys to ensure multiple leaf pages.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < 5000; i++ {
			var k [8]byte
			k[0] = byte(i >> 24)
			k[1] = byte(i >> 16)
			k[2] = byte(i >> 8)
			k[3] = byte(i)
			if err := b.Set(k[:], val); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	before, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(before): %v", err)
	}
	beforeLeafStr := before["treedb.user.pages.leaf"]
	if beforeLeafStr == "" {
		t.Fatalf("missing treedb.user.pages.leaf")
	}
	beforeLeaf, err := strconv.ParseUint(beforeLeafStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf(before): %v", err)
	}
	if beforeLeaf <= 1 {
		t.Fatalf("expected multiple leaf pages before deletes, got %d", beforeLeaf)
	}

	// Delete all keys; internal nodes should drop empty leaf children rather than
	// keeping a forest of empty pages.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < 5000; i++ {
			var k [8]byte
			k[0] = byte(i >> 24)
			k[1] = byte(i >> 16)
			k[2] = byte(i >> 8)
			k[3] = byte(i)
			if err := b.Delete(k[:]); err != nil {
				t.Fatalf("del: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("del write: %v", err)
		}
		_ = b.Close()
	}

	after, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(after): %v", err)
	}
	afterLeafStr := after["treedb.user.pages.leaf"]
	if afterLeafStr == "" {
		t.Fatalf("missing treedb.user.pages.leaf (after)")
	}
	afterLeaf, err := strconv.ParseUint(afterLeafStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf(after): %v", err)
	}
	if afterLeaf != 1 {
		t.Fatalf("expected exactly 1 empty leaf page after deletes, got %d", afterLeaf)
	}

	it, err := d.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()
	if it.Valid() {
		t.Fatalf("expected empty iterator after deletes")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
}
