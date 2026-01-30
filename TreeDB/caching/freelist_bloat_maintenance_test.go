package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Sanity: if we force a very aggressive maintenance budget, churn should not
// leave the index far larger than a vacuum rebuild.
//
// This test should stay passing even while the default-budget bloat regression
// remains failing.
func TestCachedBenchBloatVacuum_WithAggressiveMaintenanceBudget(t *testing.T) {
	const keys = 20000
	dir := t.TempDir()

	backend, err := db.Open(db.Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
		// Force coalesce to run even on small tests.
		MaintenanceOpsPerCoalesce: 1000,
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
		t.Fatalf("checkpoint write: %v", err)
	}
	applyRandomUpdates(t, cached, keys, val, 1)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint overwrite: %v", err)
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
		t.Fatalf("checkpoint delete: %v", err)
	}

	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint rewrite: %v", err)
	}

	// Close and vacuum; this should not shrink massively if maintenance is effective.
	_ = cached.Close()
	_ = backend.Close()

	// Reuse the existing bloat test logic by calling it directly would be messy;
	// just re-run the vacuum and compare sizes here.
	indexPath := filepath.Join(dir, "index.db")
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}
	before := beforeInfo.Size()
	if err := db.VacuumIndexOffline(db.Options{Dir: dir, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	after := afterInfo.Size()
	if before > after*2 {
		t.Fatalf("unexpected bloat even with aggressive maintenance: before=%d after=%d ratio=%.2f", before, after, float64(before)/float64(after))
	}
}
