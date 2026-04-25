package treedb_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestLeafGenerationGC_CachedModeCheckpointsBeforeDryRun(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:                        dir,
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	metaBeforeWrites := readMainMeta(t, dir)
	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("leaf-gc-%04d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 32)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	metaBeforeGC := readMainMeta(t, dir)
	if metaBeforeGC.CommitSeq != metaBeforeWrites.CommitSeq {
		t.Fatalf("expected cached writes to remain uncheckpointed before LeafGenerationGC, got before=%d after writes=%d", metaBeforeWrites.CommitSeq, metaBeforeGC.CommitSeq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stats, err := db.LeafGenerationGC(ctx, treedb.LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if stats.GenerationsTotal < 1 {
		t.Fatalf("GenerationsTotal=%d, want >= 1", stats.GenerationsTotal)
	}

	metaAfterGC := readMainMeta(t, dir)
	if metaAfterGC.CommitSeq <= metaBeforeGC.CommitSeq {
		t.Fatalf("expected LeafGenerationGC to checkpoint cached state, got before=%d after=%d", metaBeforeGC.CommitSeq, metaAfterGC.CommitSeq)
	}

	statsMap := db.Stats()
	if got := statsMap["treedb.maintenance.full_scan.leaf_gc_runs"]; got != "1" {
		t.Fatalf("leaf_gc_runs=%q want 1", got)
	}
	if got := statsMap["treedb.maintenance.full_scan.gc_runs"]; got != "0" {
		t.Fatalf("gc_runs=%q want 0", got)
	}
	if got := statsMap["treedb.maintenance.full_scan.active"]; got != "" {
		t.Fatalf("active=%q want empty after completion", got)
	}
}
