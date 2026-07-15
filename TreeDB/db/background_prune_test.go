package db

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestBackgroundPruneMakesProgress(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:               dir,
		KeepRecent:        1,
		PruneInterval:     5 * time.Millisecond,
		PruneMaxPages:     1_000_000,
		PruneMaxDuration:  25 * time.Millisecond,
		PreferAppendAlloc: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	initialStats := d.Stats()
	initialFreePages, err := strconv.ParseUint(initialStats["treedb.freelist.free_pages_total"], 10, 64)
	if err != nil {
		t.Fatalf("parse initial freelist free pages: %v", err)
	}

	const (
		commits = 6
		keys    = 200
	)

	for c := 0; c < commits; c++ {
		b := d.NewBatch()
		for i := 0; i < keys; i++ {
			k := []byte(fmt.Sprintf("k%08d", i))
			v := []byte(fmt.Sprintf("v%08d-%08d", c, i))
			if err := b.Set(k, v); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := d.Stats()
		if stats["treedb.prune.enabled"] == "true" {
			pruned, pruneErr := strconv.ParseUint(stats["treedb.prune.pages_freed"], 10, 64)
			freePages, freeErr := strconv.ParseUint(stats["treedb.freelist.free_pages_total"], 10, 64)
			// Durable-root COW generations seal retired pages into the freelist
			// directly; legacy graveyard work still increments prune.pages_freed.
			if pruneErr == nil && freeErr == nil && (pruned > 0 || freePages > initialFreePages) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats := d.Stats()
	t.Fatalf("expected retired-page reclamation progress; prune_enabled=%q prune_pages_freed=%q freelist_free_pages=%q initial_freelist_free_pages=%d", stats["treedb.prune.enabled"], stats["treedb.prune.pages_freed"], stats["treedb.freelist.free_pages_total"], initialFreePages)
}

func TestBackgroundPruneStopsOnClose(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:           dir,
		KeepRecent:    1,
		PruneInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	b := d.NewBatch()
	_ = b.Set([]byte("k"), []byte("v"))
	_ = b.Write()
	_ = b.Close()

	done := make(chan error, 1)
	go func() {
		done <- d.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Close() did not return; background pruner likely stuck")
	}
}
