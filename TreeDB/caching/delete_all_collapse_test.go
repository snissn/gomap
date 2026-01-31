package caching

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Expected status today: FAIL.
//
// After deleting the entire keyspace, the live tree should collapse to a very
// small structure (close to an empty tree). If we still have ~O(N) empty leaf
// pages referenced, coalesce/pruning is not removing empty children.
func TestCachedDeleteAll_CollapsesToSmallTree(t *testing.T) {
	const keys = 20000

	dir := t.TempDir()
	backend, err := db.Open(db.Options{
		Dir:               dir,
		PreferAppendAlloc: false,
		KeepRecent:        1,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cached, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cached open: %v", err)
	}

	val := bytes.Repeat([]byte("a"), 128)
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after write: %v", err)
	}

	// Delete everything.
	{
		b := cached.NewBatch()
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
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after delete: %v", err)
	}

	rep, err := backend.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	userPages := rep["treedb.user.pages"]
	leafPages := rep["treedb.user.pages.leaf"]
	leafAvg := rep["treedb.user.leaf_fill_ppm_avg"]
	leafP50 := rep["treedb.user.leaf_fill_ppm_p50"]
	t.Logf("after delete: user.pages=%s leaf.pages=%s leaf.avg=%s leaf.p50=%s freelist.head=%s",
		userPages, leafPages, leafAvg, leafP50, rep["treedb.freelist.head"])

	_ = cached.Close()
	_ = backend.Close()

	// For a truly empty tree we'd expect only a handful of pages (root/internal
	// scaffolding). Using a generous bound keeps this robust across layout
	// changes while still catching the current pathological case (~1500 pages).
	//
	// This assertion is expected to FAIL until empty-leaf pruning/collapse works.
	if userPages == "" {
		t.Fatalf("missing treedb.user.pages")
	}
	userN, err := strconv.ParseUint(userPages, 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages: %v", err)
	}

	// Fail if the tree still references hundreds of pages after deleting all keys.
	// (The observed value is ~1500.)
	if userN > 50 {
		t.Fatalf("expected tree to collapse after delete-all (user.pages=%d)", userN)
	}
}
