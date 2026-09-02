package caching

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// This is a more structural form of the bloat regression: instead of comparing
// on-disk file sizes, compare live-tree page counts before/after a vacuum
// rebuild for the same logical dataset.
func TestCachedBenchBloat_PageCountRatioVsVacuum(t *testing.T) {
	requireTreeDBStress(t)

	keys := 20000
	if v := os.Getenv("TREEDB_TEST_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keys = n
		}
	}

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
		t.Fatalf("checkpoint after batch write: %v", err)
	}

	applyRandomUpdates(t, cached, keys, val, 1)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after random write: %v", err)
	}

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

	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after rewrite: %v", err)
	}

	repBefore, err := backend.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport before close: %v", err)
	}
	userBefore, err := strconv.ParseUint(repBefore["treedb.user.pages"], 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages before: %v", err)
	}

	_ = cached.Close()
	_ = backend.Close()

	if err := db.VacuumIndexOffline(db.Options{Dir: dir, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum offline: %v", err)
	}

	backend2, err := db.Open(db.Options{
		Dir:               dir,
		PreferAppendAlloc: false,
		KeepRecent:        1,
	})
	if err != nil {
		t.Fatalf("backend reopen after vacuum: %v", err)
	}
	defer backend2.Close()

	repAfter, err := backend2.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport after vacuum: %v", err)
	}
	userAfter, err := strconv.ParseUint(repAfter["treedb.user.pages"], 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages after: %v", err)
	}

	if userAfter == 0 {
		t.Fatalf("expected non-zero user pages after vacuum")
	}

	// If the live tree needs >2x as many pages as a vacuum rebuild, we're
	// retaining too many underfilled pages.
	if userBefore > userAfter*2 {
		t.Fatalf("live tree uses too many pages vs vacuum: before=%d after=%d ratio=%.2f",
			userBefore, userAfter, float64(userBefore)/float64(userAfter))
	}
}
