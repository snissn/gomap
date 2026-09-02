package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestDeleteMostKeys_MergesUnderfullLeafSiblings(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                   dir,
		KeepRecent:            1,
		LeafFillTargetPPM:     850_000,
		InternalFillTargetPPM: 900_000,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 32)
	const total = 5000

	// Insert keys.
	{
		const batchSize = 512
		for base := 0; base < total; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := base + batchSize
			if limit > total {
				limit = total
			}
			for i := base; i < limit; i++ {
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
	}

	before, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(before): %v", err)
	}
	beforeLeafStr := before["treedb.user.pages.leaf"]
	if beforeLeafStr == "" {
		t.Fatalf("missing treedb.user.pages.leaf (before)")
	}
	beforeLeaf, err := strconv.ParseUint(beforeLeafStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf(before): %v", err)
	}
	if beforeLeaf < 10 {
		t.Fatalf("expected substantial leaf fanout before deletes, got %d", beforeLeaf)
	}

	// Delete most keys: keep only the last 100 keys.
	{
		const keep = 100
		b := d.NewBatch().(*Batch)
		for i := 0; i < total-keep; i++ {
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

	// Underfull sibling merge/rebalance should collapse many sparse leaves.
	if afterLeaf > 10 {
		t.Fatalf("expected leaf pages to collapse, before=%d after=%d", beforeLeaf, afterLeaf)
	}

	// Sanity: remaining keys are present.
	for i := total - 100; i < total; i++ {
		var k [8]byte
		k[0] = byte(i >> 24)
		k[1] = byte(i >> 16)
		k[2] = byte(i >> 8)
		k[3] = byte(i)
		got, err := d.Get(k[:])
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got == nil {
			t.Fatalf("expected key %d to exist", i)
		}
	}
}
