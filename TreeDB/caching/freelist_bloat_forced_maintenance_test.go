package caching

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Diagnostic experiment: force "maintenance=true" even for put-only batches by
// setting LeafFillTargetPPM just low enough to reserve at least 1 byte (so the
// zipper treats the workload as maintenance-eligible).
//
// This helps answer: does coalesce/rebalance *work* for the underfilled-page
// issue, but simply not run by default for put-only batches?
func TestCachedBenchBloat_PageCountRatioVsVacuum_WithForcedMaintenance(t *testing.T) {
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
		// Reserve ~1 byte so maintenance is enabled for put-only batches.
		// For 8KB pages: reserveBytes ~= PageSize*(1_000_000-ppm)/1_000_000.
		LeafFillTargetPPM: 999_800,
		// Disable budget throttling so we can see the "full coalesce" effect.
		MaintenanceOpsPerCoalesce: 0,
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

	// Rewrite once more to mirror the bloat tests' end state.
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after rewrite: %v", err)
	}

	repBefore, err := backend.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport before: %v", err)
	}
	userBefore, err := strconv.ParseUint(repBefore["treedb.user.pages"], 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages before: %v", err)
	}

	_ = cached.Close()
	_ = backend.Close()

	if err := db.VacuumIndexOffline(db.Options{Dir: dir, KeepRecent: 1, LeafFillTargetPPM: 999_800}); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	backend2, err := db.Open(db.Options{
		Dir:               dir,
		PreferAppendAlloc: false,
		KeepRecent:        1,
		LeafFillTargetPPM: 999_800,
	})
	if err != nil {
		t.Fatalf("backend reopen: %v", err)
	}
	defer backend2.Close()

	repAfter, err := backend2.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport after: %v", err)
	}
	userAfter, err := strconv.ParseUint(repAfter["treedb.user.pages"], 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages after: %v", err)
	}

	if userAfter == 0 {
		t.Fatalf("expected non-zero user pages after vacuum")
	}
	t.Logf("forced-maintenance pagecount ratio: before=%d after=%d ratio=%.2f", userBefore, userAfter, float64(userBefore)/float64(userAfter))
}
