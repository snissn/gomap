package caching

import (
	"fmt"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVlogGenerationChunkHarness_StaleRatioPassLeavesExecutableChunkDebt(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               true,
		JournalLanes:                             1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerStaleRatioPPM:      1,
		ValueLogRewriteBudgetBytesPerSec:         16 << 10,
		ValueLogRewriteMinSegmentAge:             time.Millisecond,
		ForceValueLogPointers:                    true,
		IndexOuterLeavesInValueLog:               false,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)

	writeSubset := func(tag byte, keep func(int) bool) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < 128; i++ {
			if !keep(i) {
				continue
			}
			key := []byte(fmt.Sprintf("k%03d", i))
			value := make([]byte, 4096)
			seed := uint32((i+1)*131) ^ uint32(tag)
			for j := range value {
				seed = seed*1664525 + 1013904223
				value[j] = byte(seed >> 24)
			}
			if err := b.Set(key, value); err != nil {
				_ = b.Close()
				t.Fatalf("set key %d: %v", i, err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write batch tag=%d: %v", tag, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("close batch tag=%d: %v", tag, err)
		}
	}

	writeSubset(1, func(int) bool { return true })
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint initial: %v", err)
	}
	writeSubset(2, func(i int) bool { return i%4 != 0 })
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint overwrite: %v", err)
	}

	time.Sleep(5 * time.Millisecond)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(16 << 10)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	remainingChunks, chunkBytes, err := db.currentVlogGenerationRewriteChunkLedger()
	if err != nil {
		t.Fatalf("load chunk ledger after stale-ratio pass: %v", err)
	}
	if len(remainingChunks) == 0 || chunkBytes == 0 {
		stats := db.Stats()
		t.Fatalf("expected remaining chunk debt after bounded stale-ratio pass; ledger_chunks=%s stage_pending=%s runs=%s source_chunks_total=%s queued_rewrite_started=%s",
			stats["treedb.cache.vlog_generation.rewrite.ledger_chunks"],
			stats["treedb.cache.vlog_generation.rewrite.stage_pending"],
			stats["treedb.cache.vlog_generation.rewrite.runs"],
			stats["treedb.cache.vlog_generation.rewrite.exec.source_chunks_total"],
			stats["treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"],
		)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("load rewrite queue after stale-ratio pass: %v", err)
	}
	if len(queue) == 0 {
		t.Fatalf("rewrite queue after stale-ratio pass=%v want remaining executable debt", queue)
	}
	stagePending, stageObservedAt, err := db.currentVlogGenerationRewriteStage()
	if err != nil {
		t.Fatalf("load rewrite stage after stale-ratio pass: %v", err)
	}
	if stagePending || stageObservedAt != 0 {
		t.Fatalf("rewrite stage after stale-ratio pass pending=%t observed_at=%d want false/0", stagePending, stageObservedAt)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected_chunks_total"]; got == "0" {
		t.Fatalf("rewrite plan selected chunks total=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.runs"]; got == "0" {
		t.Fatalf("rewrite runs=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_chunks_total"]; got == "0" {
		t.Fatalf("rewrite exec source chunks total=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_pending"]; got != "false" {
		t.Fatalf("rewrite stage pending=%q want false", got)
	}
}

func TestVlogGenerationChunkHarness_GenericPassLeavesExecutableChunkDebt(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               true,
		JournalLanes:                             1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		ValueLogRewriteTriggerStaleRatioPPM:      0,
		ValueLogRewriteBudgetBytesPerSec:         16 << 10,
		ValueLogRewriteMinSegmentAge:             time.Millisecond,
		ForceValueLogPointers:                    true,
		IndexOuterLeavesInValueLog:               false,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)

	writeSubset := func(tag byte, keep func(int) bool) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < 128; i++ {
			if !keep(i) {
				continue
			}
			key := []byte(fmt.Sprintf("kr%03d", i))
			value := make([]byte, 4096)
			seed := uint32((i+1)*131) ^ uint32(tag)
			for j := range value {
				seed = seed*1664525 + 1013904223
				value[j] = byte(seed >> 24)
			}
			if err := b.Set(key, value); err != nil {
				_ = b.Close()
				t.Fatalf("set key %d: %v", i, err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write batch tag=%d: %v", tag, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("close batch tag=%d: %v", tag, err)
		}
	}

	writeSubset(1, func(int) bool { return true })
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint initial: %v", err)
	}
	writeSubset(2, func(i int) bool { return i%4 != 0 })
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint overwrite: %v", err)
	}

	time.Sleep(5 * time.Millisecond)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(16 << 10)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	remainingChunks, chunkBytes, err := db.currentVlogGenerationRewriteChunkLedger()
	if err != nil {
		t.Fatalf("load chunk ledger after generic pass: %v", err)
	}
	if len(remainingChunks) == 0 || chunkBytes == 0 {
		stats := db.Stats()
		t.Fatalf("expected remaining chunk debt after bounded generic pass; ledger_chunks=%s stage_pending=%s runs=%s source_chunks_total=%s",
			stats["treedb.cache.vlog_generation.rewrite.ledger_chunks"],
			stats["treedb.cache.vlog_generation.rewrite.stage_pending"],
			stats["treedb.cache.vlog_generation.rewrite.runs"],
			stats["treedb.cache.vlog_generation.rewrite.exec.source_chunks_total"],
		)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("load rewrite queue after generic pass: %v", err)
	}
	if len(queue) == 0 {
		t.Fatalf("rewrite queue after generic pass=%v want remaining executable debt", queue)
	}
	stagePending, stageObservedAt, err := db.currentVlogGenerationRewriteStage()
	if err != nil {
		t.Fatalf("load rewrite stage after generic pass: %v", err)
	}
	if stagePending || stageObservedAt != 0 {
		t.Fatalf("rewrite stage after generic pass pending=%t observed_at=%d want false/0", stagePending, stageObservedAt)
	}
}

func TestVlogGenerationChunkHarness_GenericPassUsesChunkRewrite(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               true,
		JournalLanes:                             1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		ValueLogRewriteTriggerStaleRatioPPM:      0,
		ValueLogRewriteBudgetBytesPerSec:         64 << 10,
		ValueLogRewriteMinSegmentAge:             time.Millisecond,
		ForceValueLogPointers:                    true,
		IndexOuterLeavesInValueLog:               false,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)

	writeSubset := func(tag byte, keep func(int) bool) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < 128; i++ {
			if !keep(i) {
				continue
			}
			key := []byte(fmt.Sprintf("kg%03d", i))
			value := make([]byte, 4096)
			seed := uint32((i+1)*131) ^ uint32(tag)
			for j := range value {
				seed = seed*1664525 + 1013904223
				value[j] = byte(seed >> 24)
			}
			if err := b.Set(key, value); err != nil {
				_ = b.Close()
				t.Fatalf("set key %d: %v", i, err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write batch tag=%d: %v", tag, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("close batch tag=%d: %v", tag, err)
		}
	}

	writeSubset(1, func(int) bool { return true })
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint initial: %v", err)
	}
	writeSubset(2, func(i int) bool { return i%4 != 0 })
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint overwrite: %v", err)
	}

	time.Sleep(5 * time.Millisecond)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(64 << 10)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_runs"]; got == "0" {
		t.Fatalf("rewrite plan runs=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected_chunks_total"]; got == "0" {
		t.Fatalf("rewrite plan selected chunks total=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.runs"]; got == "0" {
		t.Fatalf("rewrite runs=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_chunks_total"]; got == "0" {
		t.Fatalf("rewrite exec source chunks total=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_pending"]; got != "false" {
		t.Fatalf("rewrite stage pending=%q want false", got)
	}
}
