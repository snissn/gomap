package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCachedPhaseAllocTimeline(t *testing.T) {
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

	type snap struct {
		phase       string
		pages       uint64
		reclaimable uint64
		freelist    uint64
		appendAlloc uint64
	}

	var snaps []snap

	record := func(phase string) {
		stats, err := backend.FragmentationReport()
		if err != nil {
			t.Fatalf("FragmentationReport: %v", err)
		}
		reclaimable := parseReportUintReuse(t, stats, "treedb.freelist.reclaimable_pages")
		statMap := backend.Stats()
		freelist := parseReportUintReuse(t, statMap, "treedb.alloc.freelist")
		appendAlloc := parseReportUintReuse(t, statMap, "treedb.alloc.append")
		pages := backend.Pager().PageCount()
		snaps = append(snaps, snap{
			phase:       phase,
			pages:       pages,
			reclaimable: reclaimable,
			freelist:    freelist,
			appendAlloc: appendAlloc,
		})
		t.Logf("phase=%s pages=%d reclaimable=%d alloc.freelist=%d alloc.append=%d", phase, pages, reclaimable, freelist, appendAlloc)
	}

	// Phase 1: batch write
	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint batch write: %v", err)
	}
	record("batch_write")

	// Phase 2: random write
	applyRandomUpdatesReuse(t, cached, keys, valB, 1)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint random write: %v", err)
	}
	record("random_write")

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
		t.Fatalf("checkpoint delete: %v", err)
	}
	record("batch_delete")

	// Phase 4: rewrite
	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint rewrite: %v", err)
	}
	record("rewrite")

	if len(snaps) < 4 {
		t.Fatalf("expected 4 phases, got %d", len(snaps))
	}
}
