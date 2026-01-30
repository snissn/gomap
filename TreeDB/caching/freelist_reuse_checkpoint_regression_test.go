package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Regression: after churn + checkpoints, we expect the backend to eventually
// allocate from the freelist (not append-only forever). This is a stronger and
// more direct signal than file size bloat.
func TestCheckpointPruneEnablesFreelistReuse(t *testing.T) {
	dir := t.TempDir()

	backend, err := db.Open(db.Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("cached open: %v", err)
	}
	defer cached.Close()

	const keys = 20000
	valA := bytes.Repeat([]byte("a"), 128)
	valB := bytes.Repeat([]byte("b"), 128)

	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint write: %v", err)
	}

	applyRandomUpdatesReuse(t, cached, keys, valB, 1)
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

	// Rewrite should trigger reuse if pruning is working.
	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint rewrite: %v", err)
	}

	stats := backend.Stats()
	reused := parseReportUintReuse(t, stats, "treedb.alloc.freelist")
	if reused == 0 {
		t.Fatalf("expected some freelist allocations after churn, got 0 (alloc.append=%s)", stats["treedb.alloc.append"])
	}
}
