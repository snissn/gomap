package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestFlushCheckpointPrunesReclaimables(t *testing.T) {
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

	const keys = 4000
	valA := bytes.Repeat([]byte("a"), 64)
	valB := bytes.Repeat([]byte("b"), 64)

	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after seed: %v", err)
	}

	seedBatchesReuse(t, cached, keys, valB)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after overwrite: %v", err)
	}

	// Delete all keys, then checkpoint; prune should happen in Checkpoint.
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

	stats, err := backend.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	reclaimable := parseReportUintReuse(t, stats, "treedb.freelist.reclaimable_pages")
	if reclaimable == 0 {
		t.Fatalf("expected reclaimable pages after checkpoint prune")
	}

	before := backend.Stats()
	freelistBefore := parseReportUintReuse(t, before, "treedb.alloc.freelist")

	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after rewrite: %v", err)
	}
	after := backend.Stats()
	freelistAfter := parseReportUintReuse(t, after, "treedb.alloc.freelist")
	if freelistAfter <= freelistBefore {
		t.Fatalf("expected freelist reuse after checkpoint (before=%d after=%d)", freelistBefore, freelistAfter)
	}
}

func TestCheckpointTriggersPruneWithEmptyQueue(t *testing.T) {
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

	const keys = 2000
	valA := bytes.Repeat([]byte("a"), 32)
	valB := bytes.Repeat([]byte("b"), 32)

	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after seed: %v", err)
	}
	seedBatchesReuse(t, cached, keys, valB)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after overwrite: %v", err)
	}

	// Delete all keys to create retired pages.
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

	// Checkpoint should prune even when queue is empty after flush.
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after delete: %v", err)
	}

	stats, err := backend.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	reclaimable := parseReportUintReuse(t, stats, "treedb.freelist.reclaimable_pages")
	if reclaimable == 0 {
		t.Fatalf("expected reclaimable pages after checkpoint prune")
	}
}
