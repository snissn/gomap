package db

import (
	"bytes"
	"testing"
)

// Regression: pages retired at seq S are unreachable from state S and should
// not be blocked by a reader pinned at seq S. Historically we required
// retiredAtSeq < minPinnedSeq, which unnecessarily delayed reuse and could
// force file growth under churn when KeepRecent is small.
func TestPrune_AllowsReclaimAtMinPinnedSeqBoundary(t *testing.T) {
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

	const keys = 20000
	valA := bytes.Repeat([]byte("a"), 128)
	valB := bytes.Repeat([]byte("b"), 128)

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

	// Commit 1: build a working set.
	writeAll(valA)

	// Commit 2: overwrite the working set; this retires a large number of pages
	// at seq==2.
	writeAll(valB)

	// Pin a reader at seq==2.
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	// Commit 3: advance currentSeq so KeepRecent=1 allows reclaiming seq==2.
	writeAll(valA)

	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil || idx.registry == nil {
		t.Fatalf("missing index internals")
	}

	minPinned := idx.registry.MinPinnedSeq()
	if minPinned != 2 {
		t.Fatalf("expected minPinned==2 (snapshot at seq 2), got %d", minPinned)
	}

	// Prune should be able to reclaim pages retired at seq==2 even though the
	// reader is pinned at seq==2.
	d.Prune()

	fl, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats: %v", err)
	}
	if fl.ReclaimablePages() == 0 {
		t.Fatalf("expected some reclaimable pages after prune, got 0 (head=%d)", fl.Head)
	}
}
