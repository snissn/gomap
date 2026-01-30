package db

import (
	"bytes"
	"testing"
)

func TestFragmentationReportFreelistKeysPresentWhenHeadNonZero(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 8000
	valA := bytes.Repeat([]byte("a"), 64)
	valB := bytes.Repeat([]byte("b"), 64)

	writeAll := func(val []byte) {
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, val); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	deleteAll := func() {
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Delete(k); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("delete write: %v", err)
		}
		_ = b.Close()
	}

	// Force enough commits so keepRecent doesn't block freeing.
	writeAll(valA)
	writeAll(valB) // overwrite -> retire
	deleteAll()    // retire
	writeAll(valA) // advance seq
	d.Prune()

	// Ensure head is non-zero before requiring freelist keys.
	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	head := idx.allocator.Head()
	if head == 0 {
		// If head is 0, freelist keys are expected to be absent; the test is about
		// correctness when the freelist exists.
		t.Skip("freelist head still 0; cannot validate freelist keys")
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	if err := ValidateFragmentationReport(rep); err != nil {
		t.Fatalf("ValidateFragmentationReport: %v", err)
	}

	// Require freelist keys when head != 0.
	wantKeys := []string{
		"treedb.freelist.pages",
		"treedb.freelist.free_ids",
		"treedb.freelist.reclaimable_pages",
		"treedb.freelist.reclaimable_ratio_ppm",
	}
	for _, k := range wantKeys {
		if _, ok := rep[k]; !ok {
			t.Fatalf("missing fragmentation key %q when freelist head != 0", k)
		}
	}
}
