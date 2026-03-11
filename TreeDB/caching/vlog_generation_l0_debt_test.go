package caching

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func retainedLaneValueLogStats(t *testing.T, walDir string, lane int) (segments int, bytes int64) {
	t.Helper()
	all, _ := listNonEmptyLogSegments(walDir)
	for _, seg := range all {
		if !seg.valueLog || seg.lane != lane || seg.size <= 0 {
			continue
		}
		segments++
		bytes += seg.size
	}
	return segments, bytes
}

func TestVlogGenerationMaintenance_ReducesPathologicalL0DebtForLargeOverwrites(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	db, err := Open(dir, backend, Options{
		FlushThreshold:                           256 << 20,
		DisableWAL:                               true,
		RelaxedSync:                              true,
		AllowUnsafe:                              true,
		MemtableShards:                           1,
		JournalLanes:                             1,
		ForceValueLogPointers:                    true,
		ValueLogPointerThreshold:                 1,
		ValueLogMaxSegmentBytes:                  4 << 10,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogGenerationHotSegmentTargetBytes:  4 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 4 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 4 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		ValueLogRewriteTriggerStaleRatioPPM:      1,
		ValueLogRewriteBudgetBytesPerSec:         1 << 30,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	t.Cleanup(func() { _ = db.Close() })
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1 << 30)

	writeRound := func(round int) {
		t.Helper()
		b := db.NewBatch()
		uniqueKey := []byte(fmt.Sprintf("live-%02d", round))
		uniqueVal := bytes.Repeat([]byte(fmt.Sprintf("live-round-%02d-", round)), 256)
		if err := b.Set(uniqueKey, uniqueVal); err != nil {
			_ = b.Close()
			t.Fatalf("set unique round=%d: %v", round, err)
		}
		for i := 0; i < 6; i++ {
			key := []byte(fmt.Sprintf("hot-%02d", i))
			val := bytes.Repeat([]byte(fmt.Sprintf("round-%02d-", round)), 1024)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set round=%d key=%d: %v", round, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write round=%d: %v", round, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("close batch round=%d: %v", round, err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("checkpoint round=%d: %v", round, err)
		}
	}

	for round := 0; round < 12; round++ {
		writeRound(round)
	}

	walDir := filepath.Join(dir, "wal")
	beforeSegments, beforeBytes := retainedLaneValueLogStats(t, walDir, 0)
	if beforeSegments < 4 {
		t.Fatalf("expected pathological l0 buildup before maintenance, got segments=%d bytes=%d", beforeSegments, beforeBytes)
	}
	plan, err := backend.ValueLogRewritePlan(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
		MaxSourceBytes: beforeBytes,
	})
	if err != nil {
		t.Fatalf("pre-maintenance rewrite plan: %v", err)
	}
	if len(plan.SourceFileIDs) == 0 || plan.SelectedBytesStale == 0 || plan.SelectedBytesLive == 0 {
		t.Fatalf("expected mixed live/stale l0 rewrite debt before maintenance, got %+v", plan)
	}

	for i := 0; i < 6; i++ {
		forceVlogMaintenanceIdle(db)
		db.vlogGenerationLastRewriteUnixNano.Store(0)
		db.maybeRunVlogGenerationMaintenance(false)
	}

	afterSegments, afterBytes := retainedLaneValueLogStats(t, walDir, 0)
	stats := db.Stats()
	retained := db.valueLogRetainedStatsDetailed()
	if afterSegments >= beforeSegments {
		t.Fatalf("expected maintenance to shrink l0 segment count, before=%d after=%d bytes_before=%d bytes_after=%d rewrite_runs=%s queue=%v reason=%s scheduler=%s retained=%+v plan=%+v",
			beforeSegments, afterSegments, beforeBytes, afterBytes,
			stats["treedb.cache.vlog_generation.rewrite.runs"],
			stats["treedb.cache.vlog_generation.rewrite.queue"],
			stats["treedb.cache.vlog_generation.last_reason"],
			stats["treedb.cache.vlog_generation.scheduler.state"],
			retained,
			plan,
		)
	}
	if afterBytes >= beforeBytes {
		t.Fatalf("expected maintenance to shrink l0 bytes, before=%d after=%d segments_before=%d segments_after=%d rewrite_runs=%s queue=%v reason=%s scheduler=%s retained=%+v plan=%+v",
			beforeBytes, afterBytes, beforeSegments, afterSegments,
			stats["treedb.cache.vlog_generation.rewrite.runs"],
			stats["treedb.cache.vlog_generation.rewrite.queue"],
			stats["treedb.cache.vlog_generation.last_reason"],
			stats["treedb.cache.vlog_generation.scheduler.state"],
			retained,
			plan,
		)
	}
}
