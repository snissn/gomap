package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestCompactIndex_AllocatesNewPagesByAppending(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 16)
	for i := 0; i < 5000; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		// Use non-sync writes here: the test only needs enough pages to exist in
		// the file for CompactIndex to rebuild them append-only. Forcing an fsync
		// per key (SetSync) can be extremely slow on some environments and makes
		// the unit test flaky/time out.
		if err := d.Set(k, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	beforeTotal := d.Pager().PageCount()

	if err := d.CompactIndex(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	minStr := rep["treedb.user.pages.min"]
	if minStr == "" {
		t.Fatalf("missing pages.min in report")
	}
	minID, err := strconv.ParseUint(minStr, 10, 64)
	if err != nil {
		t.Fatalf("parse pages.min: %v", err)
	}

	// The vacuum rebuild uses pager.Alloc directly, which is append-only, so the
	// rebuilt tree should live at or beyond the previous end-of-file page count.
	if minID < beforeTotal {
		t.Fatalf("expected appended allocations, got minID=%d beforeTotal=%d", minID, beforeTotal)
	}
}
