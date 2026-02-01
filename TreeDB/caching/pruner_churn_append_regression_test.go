package caching_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

// This regression captures the "index.db grows wildly under churn" failure mode:
// if the pruner doesn't move eligible retired pages to the freelist fast enough,
// the allocator is forced to append new pages (pager.Alloc), growing index.db.
//
// The test is intentionally end-to-end: it seeds keys, checkpoints (to establish
// a durable boundary), then performs overwrites (no new keys) under KeepRecent=1
// and asserts that append allocations stay bounded.
func TestPrunerKeepsUpAvoidsAppendGrowthUnderOverwriteChurn(t *testing.T) {
	dir := t.TempDir()

	opts := treedb.Options{
		Dir:            dir,
		KeepRecent:     1,
		FlushThreshold: 1 << 20, // force frequent flushes to stress prune/reuse

		// Make this deterministic and reduce background noise unrelated to pruning.
		Durability:             treedb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: false, // regression is about the background pruner
		PreferAppendAlloc:      false,

		// Keep the run bounded.
		MaxQueuedMemtables:     8,
		SlowdownBacklogSeconds: 0,
		StopBacklogSeconds:     0,
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const (
		keys       = 20000
		overwrites = 50000
	)

	seed := func(start, n int) error {
		b := db.NewBatch()
		defer func() { _ = b.Close() }()
		for i := start; i < start+n; i++ {
			k := []byte(fmt.Sprintf("k%08d", i))
			v := []byte("v")
			if err := b.Set(k, v); err != nil {
				return err
			}
			// Commit in small batches to create many commits (stresses KeepRecent gating).
			if (i-start+1)%250 == 0 {
				if err := b.Write(); err != nil {
					return err
				}
				_ = b.Close()
				b = db.NewBatch()
			}
		}
		return b.Write()
	}

	// Initial load: creates pages (append is expected here).
	if err := seed(0, keys); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(seed): %v", err)
	}

	baseStats := db.Stats()
	baseAppend := parseUint(t, baseStats, "treedb.alloc.append")
	baseFreelist := parseUint(t, baseStats, "treedb.alloc.freelist")
	t.Logf("after seed: alloc.append=%d alloc.freelist=%d pages.total=%s prune.pages_freed=%s",
		baseAppend, baseFreelist, baseStats["treedb.pages.total"], baseStats["treedb.prune.pages_freed"])

	// Overwrite churn (no new keys).
	r := rand.New(rand.NewSource(1))
	b := db.NewBatch()
	for i := 0; i < overwrites; i++ {
		kid := r.Intn(keys)
		k := []byte(fmt.Sprintf("k%08d", kid))
		v := []byte("vv") // small overwrite
		if err := b.Set(k, v); err != nil {
			_ = b.Close()
			t.Fatalf("Set: %v", err)
		}
		if (i+1)%250 == 0 {
			if err := b.Write(); err != nil {
				_ = b.Close()
				t.Fatalf("Write: %v", err)
			}
			_ = b.Close()
			b = db.NewBatch()
		}
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write(final): %v", err)
	}
	_ = b.Close()

	// Give the background pruner a brief chance to catch up before measuring.
	time.Sleep(100 * time.Millisecond)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(churn): %v", err)
	}

	afterStats := db.Stats()
	afterAppend := parseUint(t, afterStats, "treedb.alloc.append")
	afterFreelist := parseUint(t, afterStats, "treedb.alloc.freelist")
	deltaAppend := afterAppend - baseAppend
	t.Logf("after churn: alloc.append=%d (+%d) alloc.freelist=%d pages.total=%s prune.pages_freed=%s",
		afterAppend, deltaAppend, afterFreelist, afterStats["treedb.pages.total"], afterStats["treedb.prune.pages_freed"])

	// Writes after the initial build should primarily reuse pages; a small amount of
	// append is tolerated for transient starvation, but it should be bounded.
	if deltaAppend > 4096 {
		indexPath := filepath.Join(dir, "index.db")
		t.Fatalf("unexpected index growth under overwrite churn: alloc.append increased by %d (base=%d after=%d) index=%s",
			deltaAppend, baseAppend, afterAppend, indexPath)
	}
}

func parseUint(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	s, ok := stats[key]
	if !ok {
		t.Fatalf("missing stats key %q", key)
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("parse %q=%q: %v", key, s, err)
	}
	return n
}
