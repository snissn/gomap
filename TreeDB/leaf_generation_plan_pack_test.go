package treedb_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestLeafGenerationPlan_CachedModeCheckpointsBeforePlan(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:                              dir,
		Durability:                       treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog:       true,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
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
		key := []byte(fmt.Sprintf("leaf-plan-%04d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 32)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	metaBeforePlan := readMainMeta(t, dir)
	if metaBeforePlan.CommitSeq != metaBeforeWrites.CommitSeq {
		t.Fatalf("expected cached writes to remain uncheckpointed before LeafGenerationPlan, got before=%d after writes=%d", metaBeforeWrites.CommitSeq, metaBeforePlan.CommitSeq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	plan, err := db.LeafGenerationPlan(ctx, treedb.LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	if len(plan.Generations) < 1 {
		t.Fatalf("len(Generations)=%d, want >= 1", len(plan.Generations))
	}

	metaAfterPlan := readMainMeta(t, dir)
	if metaAfterPlan.CommitSeq <= metaBeforePlan.CommitSeq {
		t.Fatalf("expected LeafGenerationPlan to checkpoint cached state, got before=%d after=%d", metaBeforePlan.CommitSeq, metaAfterPlan.CommitSeq)
	}
}

func TestLeafGenerationPack_CachedModeCheckpointsBeforeNoOpPack(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:                              dir,
		Durability:                       treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog:       true,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
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
		key := []byte(fmt.Sprintf("leaf-pack-%04d", i))
		val := bytes.Repeat([]byte{byte((i + 7) % 251)}, 32)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	metaBeforePack := readMainMeta(t, dir)
	if metaBeforePack.CommitSeq != metaBeforeWrites.CommitSeq {
		t.Fatalf("expected cached writes to remain uncheckpointed before LeafGenerationPack, got before=%d after writes=%d", metaBeforeWrites.CommitSeq, metaBeforePack.CommitSeq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stats, err := db.LeafGenerationPack(ctx, treedb.LeafGenerationPackOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if stats.GenerationsRequested != 0 {
		t.Fatalf("GenerationsRequested=%d, want 0", stats.GenerationsRequested)
	}

	metaAfterPack := readMainMeta(t, dir)
	if metaAfterPack.CommitSeq <= metaBeforePack.CommitSeq {
		t.Fatalf("expected LeafGenerationPack to checkpoint cached state, got before=%d after=%d", metaBeforePack.CommitSeq, metaAfterPack.CommitSeq)
	}
}

func TestLeafGenerationPackFromPlan_CachedModeCheckpointsBeforeSelectionError(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:                              dir,
		Durability:                       treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog:       true,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
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
		key := []byte(fmt.Sprintf("leaf-pack-from-plan-%04d", i))
		val := bytes.Repeat([]byte{byte((i + 17) % 251)}, 32)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	metaBefore := readMainMeta(t, dir)
	if metaBefore.CommitSeq != metaBeforeWrites.CommitSeq {
		t.Fatalf("expected cached writes to remain uncheckpointed before LeafGenerationPackFromPlan, got before=%d after writes=%d", metaBeforeWrites.CommitSeq, metaBefore.CommitSeq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = db.LeafGenerationPackFromPlan(ctx, treedb.LeafGenerationPackFromPlanOptions{MaxBytesToCopy: 1})
	if err == nil {
		t.Fatalf("expected bounded from-plan pack to fail on oversize first candidate")
	}

	metaAfter := readMainMeta(t, dir)
	if metaAfter.CommitSeq <= metaBefore.CommitSeq {
		t.Fatalf("expected LeafGenerationPackFromPlan to checkpoint cached state, got before=%d after=%d", metaBefore.CommitSeq, metaAfter.CommitSeq)
	}
}

func TestLeafGenerationPackRunOnce_CachedModeCheckpointsBeforeSkip(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:                              dir,
		Durability:                       treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog:       true,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
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
		key := []byte(fmt.Sprintf("leaf-pack-run-once-%04d", i))
		val := bytes.Repeat([]byte{byte((i + 23) % 251)}, 32)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	metaBefore := readMainMeta(t, dir)
	if metaBefore.CommitSeq != metaBeforeWrites.CommitSeq {
		t.Fatalf("expected cached writes to remain uncheckpointed before LeafGenerationPackRunOnce, got before=%d after writes=%d", metaBeforeWrites.CommitSeq, metaBefore.CommitSeq)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stats, err := db.LeafGenerationPackRunOnce(ctx, treedb.LeafGenerationPackFromPlanOptions{MaxBytesToCopy: 1})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if stats.Ran {
		t.Fatalf("expected LeafGenerationPackRunOnce to skip, got pack=%+v", stats.Pack)
	}
	if stats.SkipReason == "" {
		t.Fatalf("expected non-empty skip reason")
	}

	metaAfter := readMainMeta(t, dir)
	if metaAfter.CommitSeq <= metaBefore.CommitSeq {
		t.Fatalf("expected LeafGenerationPackRunOnce to checkpoint cached state, got before=%d after=%d", metaBefore.CommitSeq, metaAfter.CommitSeq)
	}
}
