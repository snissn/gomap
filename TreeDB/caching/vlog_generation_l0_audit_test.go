package caching

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func retainedValueLogLaneStats(t *testing.T, walDir string, lane int) (segments int, bytes int64) {
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

func TestVlogGenerationMaintenance_ReducesL0BytesAndEligibleDebt(t *testing.T) {
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
		// Keep some segment-local data live across rounds so the shape is mixed
		// live/stale debt rather than pure GC-only garbage.
		uniqueKey := []byte(fmt.Sprintf("live-%02d", round))
		uniqueVal := bytes.Repeat([]byte(fmt.Sprintf("live-round-%02d-", round)), 256)
		if err := b.Set(uniqueKey, uniqueVal); err != nil {
			_ = b.Close()
			t.Fatalf("set unique round=%d: %v", round, err)
		}
		// Repeatedly overwrite the hot keys to build stale l0 debt on one lane.
		for i := 0; i < 6; i++ {
			key := []byte(fmt.Sprintf("hot-%02d", i))
			val := bytes.Repeat([]byte(fmt.Sprintf("round-%02d-", round)), 1024)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set round=%d key=%d: %v", round, i, err)
			}
		}
		// Delete older live-only keys so some segments become fully dead and show
		// up in the scheduler's GC-eligible debt estimate.
		if round >= 3 {
			oldKey := []byte(fmt.Sprintf("live-%02d", round-3))
			if err := b.Delete(oldKey); err != nil {
				_ = b.Close()
				t.Fatalf("delete old live key round=%d: %v", round, err)
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
	beforeSegments, beforeBytes := retainedValueLogLaneStats(t, walDir, 0)
	if beforeSegments < 4 {
		t.Fatalf("expected pathological l0 buildup before maintenance, got segments=%d bytes=%d", beforeSegments, beforeBytes)
	}

	gcer, ok := any(backend).(backendValueLogGCer)
	if !ok {
		t.Fatalf("backend does not implement ValueLogGC")
	}
	beforeEligible, err := db.estimateVlogGenerationGCEligible(gcer)
	if err != nil {
		t.Fatalf("gc eligible before maintenance: %v", err)
	}
	if beforeEligible.BytesEligible <= 0 || beforeEligible.SegmentsEligible <= 0 {
		t.Fatalf("expected non-zero GC-eligible debt before maintenance, got %+v", beforeEligible)
	}

	for i := 0; i < 6; i++ {
		forceVlogMaintenanceIdle(db)
		db.vlogGenerationLastRewriteUnixNano.Store(0)
		db.vlogGenerationLastGCUnixNano.Store(0)
		db.maybeRunVlogGenerationMaintenance(true)
	}

	afterSegments, afterBytes := retainedValueLogLaneStats(t, walDir, 0)
	afterEligible, err := db.estimateVlogGenerationGCEligible(gcer)
	if err != nil {
		t.Fatalf("gc eligible after maintenance: %v", err)
	}
	if afterSegments >= beforeSegments {
		t.Fatalf("expected l0 segment count to drop, before=%d after=%d bytes_before=%d bytes_after=%d eligible_before=%+v eligible_after=%+v",
			beforeSegments, afterSegments, beforeBytes, afterBytes, beforeEligible, afterEligible)
	}
	if afterBytes >= beforeBytes {
		t.Fatalf("expected l0 bytes to drop, before=%d after=%d segments_before=%d segments_after=%d eligible_before=%+v eligible_after=%+v",
			beforeBytes, afterBytes, beforeSegments, afterSegments, beforeEligible, afterEligible)
	}
	if afterEligible.BytesEligible >= beforeEligible.BytesEligible {
		t.Fatalf("expected GC-eligible bytes to drop, before=%+v after=%+v", beforeEligible, afterEligible)
	}
	if afterEligible.SegmentsEligible >= beforeEligible.SegmentsEligible {
		t.Fatalf("expected GC-eligible segments to drop, before=%+v after=%+v", beforeEligible, afterEligible)
	}
}
