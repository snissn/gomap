package treedb

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"treedb/internal/adaptive"
)

func TestAdaptiveLatchSemanticsPerCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: 128, AdaptiveEnabled: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	cfg := adaptive.DefaultConfig()
	cfg.Enabled = true
	cfg.K = 1
	cfg.Step = 64
	cfg.Alpha = 1
	cfg.W1, cfg.W2, cfg.W3 = 0, 0, 0
	cfg.V1, cfg.V2, cfg.V3 = 0, 0, 0
	db.adaptive = adaptive.New(cfg, 128)

	var (
		mu       sync.Mutex
		observed []int
	)
	db.hooks = &dbHooks{
		thresholdObserved: func(th int) {
			mu.Lock()
			observed = append(observed, th)
			if len(observed) == 1 {
				db.adaptive.SetThreshold(256) // concurrent update mid-commit
			}
			mu.Unlock()
		},
	}

	b := db.NewBatch().(*Batch)
	val := bytes.Repeat([]byte("v"), 100)
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		if err := b.Set(key, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	mu.Lock()
	obs := append([]int(nil), observed...)
	mu.Unlock()
	if len(obs) == 0 {
		t.Fatalf("no observed thresholds")
	}
	first := obs[0]
	for i, v := range obs {
		if v != first {
			t.Fatalf("op %d saw threshold %d, want %d", i, v, first)
		}
	}
	if first != 128 {
		t.Fatalf("expected latched threshold 128, got %d", first)
	}
}

func TestAdaptiveStatsKeysExposed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, AdaptiveEnabled: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	req := []string{
		"treedb.inline_threshold.current",
		"treedb.inline_threshold.hard_min",
		"treedb.inline_threshold.hard_max",
		"treedb.inline_threshold.leaf_fill_avg",
		"treedb.inline_threshold.split_rate",
		"treedb.inline_threshold.slab_dead_ratio",
		"treedb.inline_threshold.slab_write_bytes",
		"treedb.inline_threshold.compaction_io_bps",
	}
	for _, k := range req {
		if _, ok := stats[k]; !ok {
			t.Fatalf("missing stats key %q", k)
		}
	}
}
