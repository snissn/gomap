package caching

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Diagnostic: capture fragmentation after each unified-bench-like phase to
// pinpoint when the live-tree page count / fill factor diverges from a vacuum
// rebuild.
func TestCachedBenchBloat_PhaseFragmentationTimeline(t *testing.T) {
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

	logFrag := func(label string) {
		t.Helper()
		rep, err := backend.FragmentationReport()
		if err != nil {
			t.Fatalf("%s: FragmentationReport: %v", label, err)
		}
		t.Logf("%s: pages.total=%s user.pages=%s leaf.pages=%s internal.pages=%s leaf.avg=%s leaf.p50=%s leaf.min=%s span_ratio_ppm=%s",
			label,
			rep["treedb.pages.total"],
			rep["treedb.user.pages"],
			rep["treedb.user.pages.leaf"],
			rep["treedb.user.pages.internal"],
			rep["treedb.user.leaf_fill_ppm_avg"],
			rep["treedb.user.leaf_fill_ppm_p50"],
			rep["treedb.user.leaf_fill_ppm_min"],
			rep["treedb.user.pages.span_ratio_ppm"],
		)
	}

	// Phase 1: batch write
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after batch write: %v", err)
	}
	logFrag("after_batch_write")

	// Phase 2: random write
	applyRandomUpdates(t, cached, keys, val, 1)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after random write: %v", err)
	}
	logFrag("after_random_write")

	// Phase 3: batch delete
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
	logFrag("after_batch_delete")

	// Phase 4: rewrite
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after rewrite: %v", err)
	}
	logFrag("after_rewrite")

	// Optional: show what vacuum would produce for the same data.
	_ = cached.Close()
	_ = backend.Close()

	if err := db.VacuumIndexOffline(db.Options{Dir: dir, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum: %v", err)
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
	rep, err := backend2.FragmentationReport()
	if err != nil {
		t.Fatalf("vacuum FragmentationReport: %v", err)
	}
	t.Logf("after_vacuum: pages.total=%s user.pages=%s leaf.pages=%s internal.pages=%s leaf.avg=%s leaf.p50=%s leaf.min=%s span_ratio_ppm=%s",
		rep["treedb.pages.total"],
		rep["treedb.user.pages"],
		rep["treedb.user.pages.leaf"],
		rep["treedb.user.pages.internal"],
		rep["treedb.user.leaf_fill_ppm_avg"],
		rep["treedb.user.leaf_fill_ppm_p50"],
		rep["treedb.user.leaf_fill_ppm_min"],
		rep["treedb.user.pages.span_ratio_ppm"],
	)
}
