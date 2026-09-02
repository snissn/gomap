package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestDeleteMostKeys_MergesUnderfullInternalSiblings(t *testing.T) {
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

	val := bytes.Repeat([]byte("x"), 24)
	const total = 50_000

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
	beforeInternalStr := before["treedb.user.pages.internal"]
	if beforeInternalStr == "" {
		t.Fatalf("missing treedb.user.pages.internal (before)")
	}
	beforeInternal, err := strconv.ParseUint(beforeInternalStr, 10, 64)
	if err != nil {
		t.Fatalf("parse internal(before): %v", err)
	}
	if beforeInternal < 3 {
		t.Fatalf("expected internal fanout before deletes, got %d", beforeInternal)
	}

	// Delete most keys: keep only the last 200 keys.
	{
		const keep = 200
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
	afterInternalStr := after["treedb.user.pages.internal"]
	if afterInternalStr == "" {
		t.Fatalf("missing treedb.user.pages.internal (after)")
	}
	afterInternal, err := strconv.ParseUint(afterInternalStr, 10, 64)
	if err != nil {
		t.Fatalf("parse internal(after): %v", err)
	}

	// Underfull sibling merge/rebalance should collapse sparse internal levels.
	if afterInternal >= beforeInternal {
		t.Fatalf("expected internal pages to decrease, before=%d after=%d", beforeInternal, afterInternal)
	}
	if afterInternal > 3 {
		t.Fatalf("expected internal fanout to collapse, before=%d after=%d", beforeInternal, afterInternal)
	}

	// Sanity: remaining keys are present.
	for i := total - 200; i < total; i++ {
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
