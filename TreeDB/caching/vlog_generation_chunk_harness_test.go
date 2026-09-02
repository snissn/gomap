package caching

import (
	"fmt"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVlogGenerationChunkHarness_StagesAndExecutesRealChunkDebt(t *testing.T) {
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

	initialChunks, chunkBytes, err := db.currentVlogGenerationRewriteChunkLedger()
	if err != nil {
		t.Fatalf("load chunk ledger after stage: %v", err)
	}
	if len(initialChunks) == 0 {
		stats := db.Stats()
		t.Fatalf("expected staged chunk debt after first maintenance pass; plan_runs=%s plan_empty=%s plan_empty_no_selection=%s plan_selected=%s plan_selected_chunks=%s ledger_chunks=%s stage_pending=%s age_blocked_remaining_ms=%s total_bytes=%s stale_total=%s live_total=%s",
			stats["treedb.cache.vlog_generation.rewrite.plan_runs"],
			stats["treedb.cache.vlog_generation.rewrite.plan_empty"],
			stats["treedb.cache.vlog_generation.rewrite.plan_empty.no_selection"],
			stats["treedb.cache.vlog_generation.rewrite.plan_selected"],
			stats["treedb.cache.vlog_generation.rewrite.plan_selected_chunks_total"],
			stats["treedb.cache.vlog_generation.rewrite.ledger_chunks"],
			stats["treedb.cache.vlog_generation.rewrite.stage_pending"],
			stats["treedb.cache.vlog_generation.rewrite.age_blocked_remaining_ms"],
			stats["treedb.cache.vlog_generation.bytes.total.total"],
			stats["treedb.cache.vlog_generation.bytes.stale.total"],
			stats["treedb.cache.vlog_generation.bytes.live.total"],
		)
	}
	if chunkBytes == 0 {
		t.Fatalf("expected non-zero chunk bytes after first maintenance pass")
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_pending"]; got != "true" {
		t.Fatalf("rewrite stage pending=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected"]; got == "0" {
		t.Fatalf("rewrite plan selected=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected_chunks_total"]; got == "0" {
		t.Fatalf("rewrite plan selected chunks total=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ledger_chunks"]; got == "0" {
		t.Fatalf("rewrite ledger chunks=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.runs"]; got != "0" {
		t.Fatalf("rewrite runs after staging=%q want 0", got)
	}

	db.vlogGenerationLastRewritePlanUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	forceRewriteStageConfirmDue(t, db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(16 << 10)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	})

	remainingChunks, _, err := db.currentVlogGenerationRewriteChunkLedger()
	if err != nil {
		t.Fatalf("load chunk ledger after confirm: %v", err)
	}
	initialLive := int64(0)
	for i := range initialChunks {
		initialLive += initialChunks[i].BytesLive
	}
	if len(remainingChunks) > 0 {
		remainingLive := int64(0)
		for i := range remainingChunks {
			remainingLive += remainingChunks[i].BytesLive
		}
		if remainingLive >= initialLive {
			t.Fatalf("remaining chunk live bytes=%d want < initial %d (chunks before=%d after=%d)", remainingLive, initialLive, len(initialChunks), len(remainingChunks))
		}
	}

	stats = db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_stage_confirm"]; got == "0" {
		t.Fatalf("maintenance acquired source rewrite_stage_confirm=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.runs"]; got == "0" {
		t.Fatalf("rewrite runs=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_chunks_total"]; got == "0" {
		t.Fatalf("rewrite exec source chunks total=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"]; got == "0" {
		t.Fatalf("rewrite queued debt rewrite started=%q want >0", got)
	}
}
