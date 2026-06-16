package caching

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type coalescingPressureBackend struct {
	*MockBackend
	snapshot backenddb.FlushApplyPressureSnapshot
}

func (b *coalescingPressureBackend) FlushApplyPressureSnapshot() backenddb.FlushApplyPressureSnapshot {
	return b.snapshot
}

func newCoalescingTestDB(t *testing.T, opts Options, snap backenddb.FlushApplyPressureSnapshot) (*DB, *coalescingPressureBackend) {
	t.Helper()
	backend := &coalescingPressureBackend{MockBackend: NewMockBackend(), snapshot: snap}
	if opts.FlushThreshold == 0 {
		opts.FlushThreshold = 1 << 60
	}
	if opts.MemtableShards == 0 {
		opts.MemtableShards = 1
	}
	if opts.JournalLanes == 0 {
		opts.JournalLanes = 1
	}
	if opts.FlushBuildConcurrency == 0 {
		opts.FlushBuildConcurrency = 1
	}
	db, err := Open(t.TempDir(), backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, backend
}

func highSingleOpCoalescingSnapshot() backenddb.FlushApplyPressureSnapshot {
	return backenddb.FlushApplyPressureSnapshot{
		ApplyOps:                     16,
		ReadOnlyPrepareSpans:         16,
		ReadOnlyPrepareSingleOpSpans: 16,
		ReadOnlyPrepareSpanOps:       16,
		OldLeafReadDecodeBytes:       16 << 12,
	}
}

func enqueuePointMemtables(t *testing.T, db *DB, n int, keyPrefix string) {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	for i := 0; i < n; i++ {
		setMutable(db, []byte(fmt.Sprintf("%s-%03d", keyPrefix, i)), []byte("value"))
		if err := db.rotateMemtableLocked(false); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
}

func coalescingStatUint64(t *testing.T, db *DB, key string) uint64 {
	t.Helper()
	stats := db.Stats()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return v
}

func TestFlushBacklogCoalescingAdmitsUnderSingleOpPressure(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing:                     true,
		FlushBacklogCoalescingMaxMemtables:         4,
		FlushBacklogCoalescingMaxOps:               16,
		FlushBacklogCoalescingSingleOpSpanRatio:    0.5,
		FlushBacklogCoalescingMaxOpsPerSpan:        2,
		FlushBacklogCoalescingMinOldLeafBytesPerOp: 1,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 4, "admit")

	db.flushAll(false)

	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.admitted_runs_total"); got == 0 {
		t.Fatalf("admitted_runs_total=%d want >0", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.admitted_extra_memtables_total"); got != 3 {
		t.Fatalf("admitted_extra_memtables_total=%d want 3", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.selected_memtables_max"); got != 4 {
		t.Fatalf("selected_memtables_max=%d want 4", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_span_run.source_memtables_max"); got != 4 {
		t.Fatalf("source_memtables_max=%d want 4", got)
	}
}

func TestFlushBacklogCoalescingSkipsWithoutPressure(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing:                  true,
		FlushBacklogCoalescingMaxMemtables:      4,
		FlushBacklogCoalescingMaxOps:            16,
		FlushBacklogCoalescingSingleOpSpanRatio: 0.9,
	}, backenddb.FlushApplyPressureSnapshot{
		ApplyOps:                     16,
		ReadOnlyPrepareSpans:         16,
		ReadOnlyPrepareSingleOpSpans: 1,
		ReadOnlyPrepareSpanOps:       32,
		OldLeafReadDecodeBytes:       16 << 12,
	})
	enqueuePointMemtables(t, db, 4, "nopressure")

	db.flushAll(false)

	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.admitted_runs_total"); got != 0 {
		t.Fatalf("admitted_runs_total=%d want 0", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.no_pressure_total"); got == 0 {
		t.Fatalf("no_pressure skip=%d want >0", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_span_run.source_memtables_max"); got != 1 {
		t.Fatalf("source_memtables_max=%d want base flush of 1", got)
	}
}

func TestFlushBacklogCoalescingBudgetSkipCounters(t *testing.T) {
	t.Run("ops budget", func(t *testing.T) {
		db, _ := newCoalescingTestDB(t, Options{
			FlushBacklogCoalescing:                  true,
			FlushBacklogCoalescingMaxMemtables:      8,
			FlushBacklogCoalescingMaxOps:            2,
			FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
			FlushBacklogCoalescingMaxOpsPerSpan:     2,
		}, highSingleOpCoalescingSnapshot())
		enqueuePointMemtables(t, db, 5, "opsbudget")
		db.flushAll(false)
		if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.selected_ops_max"); got != 2 {
			t.Fatalf("selected_ops_max=%d want 2", got)
		}
		if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.ops_budget_total"); got == 0 {
			t.Fatalf("ops budget skip=%d want >0", got)
		}
	})

	t.Run("memory budget", func(t *testing.T) {
		db, _ := newCoalescingTestDB(t, Options{
			FlushBacklogCoalescing:                  true,
			FlushBacklogCoalescingMaxMemtables:      8,
			FlushBacklogCoalescingMaxBytes:          1,
			FlushBacklogCoalescingMaxOps:            16,
			FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
			FlushBacklogCoalescingMaxOpsPerSpan:     2,
		}, highSingleOpCoalescingSnapshot())
		enqueuePointMemtables(t, db, 3, "membudget")
		db.flushAll(false)
		if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.admitted_runs_total"); got != 0 {
			t.Fatalf("admitted_runs_total=%d want 0 under tiny memory budget", got)
		}
		if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.memory_budget_total"); got == 0 {
			t.Fatalf("memory budget skip=%d want >0", got)
		}
	})
}

func TestFlushBacklogCoalescingPreservesLaneAndRangeBarriers(t *testing.T) {
	t.Run("lane barrier", func(t *testing.T) {
		db, _ := newCoalescingTestDB(t, Options{
			MemtableShards:                          2,
			JournalLanes:                            2,
			FlushBacklogCoalescing:                  true,
			FlushBacklogCoalescingMaxMemtables:      8,
			FlushBacklogCoalescingMaxOps:            16,
			FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
			FlushBacklogCoalescingMaxOpsPerSpan:     2,
		}, highSingleOpCoalescingSnapshot())
		keyForLane := func(lane int) []byte {
			for i := 0; i < 10000; i++ {
				key := []byte(fmt.Sprintf("lane-%d-key-%d", lane, i))
				if db.laneForShardIndex(db.shardIndex(key)) == lane {
					return key
				}
			}
			t.Fatalf("no key for lane %d", lane)
			return nil
		}
		db.mu.Lock()
		setMutable(db, keyForLane(0), []byte("l0"))
		if err := db.rotateMemtableLocked(false); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotate lane0: %v", err)
		}
		setMutable(db, keyForLane(1), []byte("l1"))
		if err := db.rotateMemtableLocked(false); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotate lane1: %v", err)
		}
		db.mu.Unlock()
		db.flushAll(false)
		if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.lane_barrier_total"); got == 0 {
			t.Fatalf("lane barrier skip=%d want >0", got)
		}
		if got := coalescingStatUint64(t, db, "treedb.cache.flush_span_run.source_memtables_max"); got != 1 {
			t.Fatalf("source_memtables_max=%d want 1 across lane barrier", got)
		}
	})

	t.Run("range barrier", func(t *testing.T) {
		db, _ := newCoalescingTestDB(t, Options{
			DisableWAL:                              true,
			AllowUnsafe:                             true,
			FlushBacklogCoalescing:                  true,
			FlushBacklogCoalescingMaxMemtables:      8,
			FlushBacklogCoalescingMaxOps:            16,
			FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
			FlushBacklogCoalescingMaxOpsPerSpan:     2,
		}, highSingleOpCoalescingSnapshot())
		if err := db.Set([]byte("point-a"), []byte("old")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := db.DeleteRangeAfterCommandWALAppend([]byte("point-a"), []byte("point-z"), func() error { return nil }); err != nil {
			t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
		}
		db.flushAll(false)
		if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.range_barrier_total"); got == 0 {
			t.Fatalf("range barrier skip=%d want >0", got)
		}
	})
}

func TestFlushBacklogCoalescingDrainModesBypassAdaptiveAdmission(t *testing.T) {
	cases := []struct {
		name string
		mode flushCollectionMode
		mark func(*DB)
		stat string
	}{
		{name: "checkpoint", mode: flushCollectionBackground, mark: func(db *DB) { db.checkpointing.Store(true) }, stat: "treedb.cache.flush_backlog_coalescing.skip.reason.checkpoint_total"},
		{name: "close", mode: flushCollectionBackground, mark: func(db *DB) { db.closing.Store(true) }, stat: "treedb.cache.flush_backlog_coalescing.skip.reason.close_total"},
		{name: "stop", mode: flushCollectionStop, stat: "treedb.cache.flush_backlog_coalescing.skip.reason.stop_pressure_total"},
		{name: "foreground", mode: flushCollectionForeground, stat: "treedb.cache.flush_backlog_coalescing.skip.reason.writer_stall_budget_total"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := newCoalescingTestDB(t, Options{
				FlushBacklogCoalescing:                  true,
				FlushBacklogCoalescingMaxMemtables:      4,
				FlushBacklogCoalescingMaxOps:            16,
				FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
				FlushBacklogCoalescingMaxOpsPerSpan:     2,
			}, highSingleOpCoalescingSnapshot())
			enqueuePointMemtables(t, db, 4, "drain"+tc.name)
			if tc.mark != nil {
				tc.mark(db)
			}
			if !db.flushLaneOnceWithCollectionMode(false, 0, nil, tc.mode) {
				t.Fatalf("flushLaneOnceWithCollectionMode returned false")
			}
			db.checkpointing.Store(false)
			db.closing.Store(false)
			if got := coalescingStatUint64(t, db, tc.stat); got == 0 {
				t.Fatalf("%s=%d want >0", tc.stat, got)
			}
			if got := coalescingStatUint64(t, db, "treedb.cache.flush_span_run.source_memtables_max"); got != 1 {
				t.Fatalf("source_memtables_max=%d want base flush of 1", got)
			}
		})
	}
}

func TestFlushBacklogCoalescingCoalescedRunPreservesShadowing(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing:                  true,
		FlushBacklogCoalescingMaxMemtables:      2,
		FlushBacklogCoalescingMaxOps:            4,
		FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
		FlushBacklogCoalescingMaxOpsPerSpan:     2,
	}, highSingleOpCoalescingSnapshot())
	db.mu.Lock()
	setMutable(db, []byte("dup"), []byte("old"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate old: %v", err)
	}
	setMutable(db, []byte("dup"), []byte("new"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate new: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)
	got, err := db.Get([]byte("dup"))
	if err != nil {
		t.Fatalf("Get dup: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("Get dup=%q want new", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_span_run.shadowed_ops_total"); got != 1 {
		t.Fatalf("shadowed_ops_total=%d want 1", got)
	}
}

func TestFlushBacklogCoalescingAgeGate(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing:                  true,
		FlushBacklogCoalescingMaxMemtables:      4,
		FlushBacklogCoalescingMaxOps:            16,
		FlushBacklogCoalescingMinAge:            time.Hour,
		FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
		FlushBacklogCoalescingMaxOpsPerSpan:     2,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 4, "age")
	db.flushAll(false)
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.admitted_runs_total"); got != 0 {
		t.Fatalf("admitted_runs_total=%d want 0 under age gate", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.queue_age_total"); got == 0 {
		t.Fatalf("queue age skip=%d want >0", got)
	}
}
