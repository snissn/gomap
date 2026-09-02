package caching

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
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
	enqueuePointMemtablesWithOps(t, db, repeatInt(n, 1), keyPrefix)
}

func repeatInt(n, value int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = value
	}
	return out
}

func enqueuePointMemtablesWithOps(t *testing.T, db *DB, opsPerMemtable []int, keyPrefix string) {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	for memIndex, ops := range opsPerMemtable {
		for opIndex := 0; opIndex < ops; opIndex++ {
			setMutable(db, []byte(fmt.Sprintf("%s-%03d-%03d", keyPrefix, memIndex, opIndex)), []byte("value"))
		}
		if err := db.rotateMemtableLocked(false); err != nil {
			t.Fatalf("rotate %d: %v", memIndex, err)
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

func TestFlushBacklogCoalescingDefaultPressureGatesAdmitObservedRandomShape(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing: true,
	}, backenddb.FlushApplyPressureSnapshot{
		ApplyOps:                     100,
		ReadOnlyPrepareSpans:         100,
		ReadOnlyPrepareSingleOpSpans: 61,
		ReadOnlyPrepareSpanOps:       341,
		OldLeafReadDecodeBytes:       140_000,
	})
	enqueuePointMemtables(t, db, 4, "defaultpressure")

	db.flushAll(false)

	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.admitted_runs_total"); got == 0 {
		t.Fatalf("admitted_runs_total=%d want >0 for observed random-write pressure shape", got)
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

func TestFlushBacklogCoalescingOpsBudgetAllowsOneWholeMemtableOvershoot(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing:                  true,
		FlushBacklogCoalescingMaxMemtables:      8,
		FlushBacklogCoalescingMaxOps:            2,
		FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
		FlushBacklogCoalescingMaxOpsPerSpan:     2,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtablesWithOps(t, db, []int{1, 3, 1}, "opsoft")

	db.flushAll(false)

	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.selected_ops_max"); got != 4 {
		t.Fatalf("selected_ops_max=%d want 4; max-ops is a soft pre-next-memtable budget", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.ops_budget_total"); got == 0 {
		t.Fatalf("ops budget skip=%d want >0", got)
	}
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
			ExternalCommandWAL:                      true,
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

func TestFlushBacklogCoalescingCheckpointUsesFrontierOwnedAdmission(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing:                  true,
		FlushBacklogCoalescingMaxMemtables:      4,
		FlushBacklogCoalescingMaxOps:            16,
		FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
		FlushBacklogCoalescingMaxOpsPerSpan:     2,
	}, backenddb.FlushApplyPressureSnapshot{})
	enqueuePointMemtables(t, db, 4, "checkpoint")
	db.checkpointing.Store(true)
	if !db.flushLaneOnceWithCollectionMode(false, 0, nil, flushCollectionBackground) {
		t.Fatalf("flushLaneOnceWithCollectionMode returned false")
	}
	db.checkpointing.Store(false)
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.admitted_runs_total"); got != 1 {
		t.Fatalf("checkpoint admitted runs=%d want 1", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.selected_memtables_max"); got != 4 {
		t.Fatalf("checkpoint selected_memtables_max=%d want 4", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.skip.reason.no_pressure_total"); got != 0 {
		t.Fatalf("checkpoint no_pressure skip=%d want 0", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_span_run.source_memtables_max"); got != 4 {
		t.Fatalf("source_memtables_max=%d want checkpoint coalesced flush of 4", got)
	}
}

func TestFlushBacklogCoalescingCheckpointAdmissionAccountsForBaseByteBudget(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushThreshold:         1,
		FlushBacklogCoalescing: true,
		FlushBuildConcurrency:  2,
	}, highSingleOpCoalescingSnapshot())
	units := []flushUnit{
		{memBytes: 40 << 20, memLen: 1},
		{memBytes: 40 << 20, memLen: 1},
		{memBytes: 40 << 20, memLen: 1},
	}
	db.observeCheckpointFlushBacklogCoalescingDrain(units, 120<<20, 3)
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.admitted_runs_total"); got != 1 {
		t.Fatalf("checkpoint admitted runs=%d want 1", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.base_budget_covered_total"); got != 0 {
		t.Fatalf("checkpoint base_budget_covered=%d want 0", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.selected_memtables_max"); got != 3 {
		t.Fatalf("checkpoint selected_memtables_max=%d want 3", got)
	}
}

func TestFlushBacklogCoalescingCheckpointRangeSpanBaseBudgetCovered(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushThreshold:         1,
		FlushBacklogCoalescing: true,
		FlushBuildConcurrency:  2,
	}, highSingleOpCoalescingSnapshot())
	units := []flushUnit{
		{spans: []batch.DeleteRange{{Start: []byte("a"), End: []byte("b")}}},
		{spans: []batch.DeleteRange{{Start: []byte("c"), End: []byte("d")}}},
	}
	db.observeCheckpointFlushBacklogCoalescingDrain(units, 0, 2)
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.admitted_runs_total"); got != 0 {
		t.Fatalf("checkpoint admitted runs=%d want 0", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.base_budget_covered_total"); got != 1 {
		t.Fatalf("checkpoint base_budget_covered=%d want 1", got)
	}
}

func TestFlushBacklogCoalescingDrainModesBypassAdaptiveAdmission(t *testing.T) {
	cases := []struct {
		name string
		mode flushCollectionMode
		mark func(*DB)
		stat string
	}{
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

func TestFlushCollectionModeSpanNativeFallbackOnlyForClose(t *testing.T) {
	cases := []struct {
		name string
		mode flushCollectionMode
		mark func(*DB)
		want backenddb.FlushSpanRunFallbackReason
	}{
		{name: "checkpoint", mode: flushCollectionBackground, mark: func(db *DB) { db.checkpointing.Store(true) }, want: backenddb.FlushSpanRunFallbackUnknown},
		{name: "close", mode: flushCollectionBackground, mark: func(db *DB) { db.closing.Store(true) }, want: backenddb.FlushSpanRunFallbackCloseOrCheckpoint},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, backend := newCoalescingTestDB(t, Options{
				FlushBacklogCoalescing: true,
			}, highSingleOpCoalescingSnapshot())
			enqueuePointMemtables(t, db, 1, "fallback"+tc.name)
			if tc.mark != nil {
				tc.mark(db)
			}
			if !db.flushLaneOnceWithCollectionMode(false, 0, nil, tc.mode) {
				t.Fatalf("flushLaneOnceWithCollectionMode returned false")
			}
			db.checkpointing.Store(false)
			db.closing.Store(false)
			backend.mu.RLock()
			got := backend.lastSpanNativeFallbackReason
			backend.mu.RUnlock()
			if got != tc.want {
				t.Fatalf("fallback reason=%s want %s", got, tc.want)
			}
		})
	}
}

func TestFlushCollectionModeCheckpointCommandWALPublishKeepsSpanNativeEligible(t *testing.T) {
	db, backend := newCoalescingTestDB(t, Options{
		FlushApplySpanNative:  true,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 1, "commandwal")
	db.checkpointing.Store(true)
	publish := &checkpointCommandWALPublish{appliedLSN: 7, ranges: []backenddb.CommandWALLSNRange{{First: 1, Last: 7}}}
	if !db.flushLaneOnceWithCollectionMode(false, 0, publish, flushCollectionBackground) {
		t.Fatalf("flushLaneOnceWithCollectionMode returned false")
	}
	db.checkpointing.Store(false)
	if !publish.consumed {
		t.Fatalf("command WAL publish was not consumed")
	}
	backend.mu.RLock()
	got := backend.lastSpanNativeFallbackReason
	backend.mu.RUnlock()
	if got.Valid() && got != backenddb.FlushSpanRunFallbackUnknown {
		t.Fatalf("command WAL checkpoint fallback reason=%s want none", got)
	}
}

func TestFlushCollectionModeCheckpointCommandWALPublishStagesOneFinalPublication(t *testing.T) {
	db, backend := newCoalescingTestDB(t, Options{
		FlushApplySpanNative:       true,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushBackendMaxEntries:     2,
		FlushBackendMaxBatches:     -1,
		FlushSpanRunTargetPlanning: true,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtablesWithOps(t, db, []int{5}, "commandwal-chunk")
	db.checkpointing.Store(true)
	publish := &checkpointCommandWALPublish{appliedLSN: 7, ranges: []backenddb.CommandWALLSNRange{{First: 1, Last: 7}}}
	if !db.flushLaneOnceWithCollectionMode(false, 0, publish, flushCollectionBackground) {
		t.Fatalf("flushLaneOnceWithCollectionMode returned false")
	}
	db.checkpointing.Store(false)
	if !publish.consumed {
		t.Fatalf("command WAL publish was not consumed")
	}
	backend.mu.RLock()
	writeCalls := backend.writeCalls
	reasons := append([]backenddb.FlushSpanRunFallbackReason(nil), backend.spanNativeFallbackReasons...)
	groups := backend.rootPublicationBuildGroups
	groupBatches := backend.rootPublicationGroupBatches
	groupFinals := backend.rootPublicationGroupFinals
	backend.mu.RUnlock()
	if writeCalls < 2 {
		t.Fatalf("backend write calls=%d want multi-chunk flush", writeCalls)
	}
	if len(reasons) != 0 {
		t.Fatalf("explicit fallback reasons=%v want none for command WAL publish chunk", reasons)
	}
	if groups != 1 || groupBatches != writeCalls || groupFinals != 1 {
		t.Fatalf("root publication grouping groups=%d batches=%d finals=%d writeCalls=%d, want one group covering every chunk with one final", groups, groupBatches, groupFinals, writeCalls)
	}
}

func TestFlushCollectionModeRangeOnlyCheckpointFallbackReason(t *testing.T) {
	db, backend := newCoalescingTestDB(t, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		FlushBacklogCoalescing:     true,
		FlushApplySpanNative:       true,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushSpanRunTargetPlanning: true,
	}, highSingleOpCoalescingSnapshot())
	db.mu.Lock()
	if err := db.enqueueRangeSpanLayerLocked([]batch.DeleteRange{{Start: []byte("point-a"), End: []byte("point-z")}}); err != nil {
		db.mu.Unlock()
		t.Fatalf("enqueueRangeSpanLayerLocked: %v", err)
	}
	db.publishMemtablesLocked()
	db.mu.Unlock()
	db.checkpointing.Store(true)
	if !db.flushLaneOnceWithCollectionMode(false, 0, nil, flushCollectionBackground) {
		t.Fatalf("flushLaneOnceWithCollectionMode returned false")
	}
	db.checkpointing.Store(false)
	backend.mu.RLock()
	got := backend.lastSpanNativeFallbackReason
	backend.mu.RUnlock()
	if got != backenddb.FlushSpanRunFallbackRangeDeleteBarrier {
		t.Fatalf("range-only checkpoint fallback reason=%s want %s", got, backenddb.FlushSpanRunFallbackRangeDeleteBarrier)
	}
}

func TestCheckpointFrontierDrainLeavesPostFrontierQueued(t *testing.T) {
	db, backend := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing:                  true,
		FlushBacklogCoalescingMaxMemtables:      4,
		FlushBacklogCoalescingMaxOps:            16,
		FlushBacklogCoalescingSingleOpSpanRatio: 0.5,
		FlushBacklogCoalescingMaxOpsPerSpan:     2,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 2, "frontier")
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	db.observeCheckpointFrontierCapture(frontier)
	db.mu.Unlock()
	enqueuePointMemtables(t, db, 1, "postfrontier")

	db.checkpointing.Store(true)
	db.flushMu.Lock()
	db.flushCheckpointFrontierLocked(false, nil, frontier)
	db.observeCheckpointFrontierDrain(frontier)
	db.flushMu.Unlock()
	db.checkpointing.Store(false)

	backend.mu.RLock()
	_, firstOK := backend.data["frontier-000-000"]
	_, secondOK := backend.data["frontier-001-000"]
	_, postOK := backend.data["postfrontier-000-000"]
	backend.mu.RUnlock()
	if !firstOK || !secondOK {
		t.Fatalf("frontier keys flushed to backend first=%t second=%t", firstOK, secondOK)
	}
	if postOK {
		t.Fatalf("post-frontier key was flushed to backend")
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.frontier.units_last"); got != 2 {
		t.Fatalf("frontier units_last=%d want 2", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.frontier.drained_units_last"); got != 2 {
		t.Fatalf("frontier drained_units_last=%d want 2", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.frontier.post_units_last"); got != 1 {
		t.Fatalf("frontier post_units_last=%d want 1", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_backlog_coalescing.checkpoint.selected_memtables_max"); got != 2 {
		t.Fatalf("checkpoint selected_memtables_max=%d want frontier-bound 2", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_span_run.source_memtables_max"); got != 2 {
		t.Fatalf("source_memtables_max=%d want frontier-bound 2", got)
	}
	if got, err := db.Get([]byte("postfrontier-000-000")); err != nil || string(got) != "value" {
		t.Fatalf("post-frontier key should remain readable from cache, got=%q err=%v", got, err)
	}
}

func TestCheckpointEmptyFrontierLeavesPostFrontierQueued(t *testing.T) {
	db, backend := newCoalescingTestDB(t, Options{}, highSingleOpCoalescingSnapshot())
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	db.observeCheckpointFrontierCapture(frontier)
	db.mu.Unlock()
	enqueuePointMemtables(t, db, 1, "emptyfrontier")

	db.checkpointing.Store(true)
	db.flushMu.Lock()
	db.flushCheckpointFrontierLocked(false, nil, frontier)
	db.observeCheckpointFrontierDrain(frontier)
	db.flushMu.Unlock()
	db.checkpointing.Store(false)

	backend.mu.RLock()
	_, postOK := backend.data["emptyfrontier-000-000"]
	backend.mu.RUnlock()
	if postOK {
		t.Fatalf("empty-frontier post key was flushed to backend")
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.frontier.units_last"); got != 0 {
		t.Fatalf("frontier units_last=%d want 0", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.frontier.post_units_last"); got != 1 {
		t.Fatalf("frontier post_units_last=%d want 1", got)
	}
	if got, err := db.Get([]byte("emptyfrontier-000-000")); err != nil || string(got) != "value" {
		t.Fatalf("empty-frontier post key should remain readable from cache, got=%q err=%v", got, err)
	}
}

func TestCheckpointActiveFrontierFiltersBackgroundFlush(t *testing.T) {
	db, backend := newCoalescingTestDB(t, Options{}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 1, "activefrontier")
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	db.observeCheckpointFrontierCapture(frontier)
	db.mu.Unlock()
	enqueuePointMemtables(t, db, 1, "activepost")

	db.setActiveCheckpointFrontier(frontier)
	db.checkpointing.Store(true)
	db.flushAll(false)
	db.checkpointing.Store(false)
	db.clearActiveCheckpointFrontier()
	db.observeCheckpointFrontierDrain(frontier)

	backend.mu.RLock()
	_, frontierOK := backend.data["activefrontier-000-000"]
	_, postOK := backend.data["activepost-000-000"]
	backend.mu.RUnlock()
	if !frontierOK {
		t.Fatalf("frontier key was not flushed by active frontier background drain")
	}
	if postOK {
		t.Fatalf("post-frontier key was flushed by background drain")
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.shared_drain.background_units_total"); got != 1 {
		t.Fatalf("background shared drain units=%d want 1", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.frontier.post_units_last"); got != 1 {
		t.Fatalf("frontier post_units_last=%d want 1", got)
	}
	if got, err := db.Get([]byte("activepost-000-000")); err != nil || string(got) != "value" {
		t.Fatalf("post-frontier key should remain readable from cache, got=%q err=%v", got, err)
	}
}

func TestCheckpointParallelDrainSkipsPreflushKick(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushApplyConcurrency: 4,
		FlushApplySpanNative:  true,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 2, "preflushskip")

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.preflush_kick_skips_total"); got == 0 {
		t.Fatalf("preflush kick skips=%d want >0", got)
	}
}

func TestBackgroundFlushYieldsToPendingCheckpointBetweenPasses(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{
		FlushBacklogCoalescing: false,
		FlushBuildConcurrency:  1,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 3, "preempt")

	db.checkpointFlushPreemptActive.Add(1)
	db.flushMu.Lock()
	db.flushAllLocked(false, nil)
	db.flushMu.Unlock()
	addAtomicInt64FloorZero(&db.checkpointFlushPreemptActive, -1)

	db.mu.RLock()
	remaining := len(db.queue)
	db.mu.RUnlock()
	if remaining != 2 {
		t.Fatalf("remaining queue after preempted background flush=%d want 2", remaining)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_apply.coordinator.checkpoint_preemptions_total"); got != 1 {
		t.Fatalf("checkpoint preemptions=%d want 1", got)
	}

	db.checkpointFlushPreemptActive.Add(1)
	db.flushAll(true)
	addAtomicInt64FloorZero(&db.checkpointFlushPreemptActive, -1)
	db.mu.RLock()
	remaining = len(db.queue)
	db.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("remaining queue after sync flush=%d want 0", remaining)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.flush_apply.coordinator.checkpoint_preemptions_total"); got != 1 {
		t.Fatalf("checkpoint preemptions after sync flush=%d want still 1", got)
	}
}

func TestCheckpointSharedFrontierRecordsBackgroundAndCheckpointWork(t *testing.T) {
	db, backend := newCoalescingTestDB(t, Options{}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 3, "sharedfrontier")
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	db.observeCheckpointFrontierCapture(frontier)
	db.mu.Unlock()

	db.setActiveCheckpointFrontier(frontier)
	db.checkpointing.Store(true)
	db.flushAll(false)
	db.flushMu.Lock()
	db.flushCheckpointFrontierLocked(false, nil, frontier)
	db.observeCheckpointFrontierDrain(frontier)
	db.flushMu.Unlock()
	db.checkpointing.Store(false)
	db.clearActiveCheckpointFrontier()

	backend.mu.RLock()
	_, firstOK := backend.data["sharedfrontier-000-000"]
	_, secondOK := backend.data["sharedfrontier-001-000"]
	_, thirdOK := backend.data["sharedfrontier-002-000"]
	backend.mu.RUnlock()
	if !firstOK || !secondOK || !thirdOK {
		t.Fatalf("frontier keys flushed first=%t second=%t third=%t", firstOK, secondOK, thirdOK)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.shared_drain.background_units_total"); got != 1 {
		t.Fatalf("background shared drain units=%d want 1", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.shared_drain.checkpoint_units_total"); got != 2 {
		t.Fatalf("checkpoint shared drain units=%d want 2", got)
	}
	if got := coalescingStatUint64(t, db, "treedb.cache.checkpoint.frontier.drained_units_last"); got != 3 {
		t.Fatalf("frontier drained units=%d want 3", got)
	}
}

func TestCheckpointFrontierMissingQueueIDMarkedIncomplete(t *testing.T) {
	db, _ := newCoalescingTestDB(t, Options{}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 1, "missingid")
	db.mu.Lock()
	if len(db.queueIDs) == 0 {
		db.mu.Unlock()
		t.Fatalf("expected queued unit id")
	}
	db.queueIDs[0] = 0
	frontier := db.captureCheckpointFrontierLocked()
	db.mu.Unlock()
	if !frontier.captured {
		t.Fatalf("frontier not marked captured")
	}
	if !frontier.missingQueueID {
		t.Fatalf("frontier missingQueueID=false want true")
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
