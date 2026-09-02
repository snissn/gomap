package caching

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func requireStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
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

type reducerPublishStatsBackend struct {
	*MockBackend
	statsCalls       int
	reducerPublishNs uint64
}

func (b *reducerPublishStatsBackend) Stats() map[string]string {
	b.statsCalls++
	return nil
}

func (b *reducerPublishStatsBackend) FlushApplyReducerPublishNs() uint64 {
	return b.reducerPublishNs
}

func TestBackendFlushApplyReducerPublishNsUsesLightweightAccessor(t *testing.T) {
	backend := &reducerPublishStatsBackend{MockBackend: NewMockBackend(), reducerPublishNs: 42}
	db := &DB{backend: backend}
	if got := db.backendFlushApplyReducerPublishNs(); got != 42 {
		t.Fatalf("reducer/publish ns=%d want 42", got)
	}
	if backend.statsCalls != 0 {
		t.Fatalf("Stats calls=%d want 0", backend.statsCalls)
	}
}

func TestFlushApplyStatsExposeStageCounters(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:             1 << 20,
		MemtableShards:             1,
		IndexOuterLeavesInValueLog: true,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = db.Close() }()

	value := bytes.Repeat([]byte("v"), 96)
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	stats := db.Stats()
	for _, key := range []string{
		"treedb.cache.flush_apply.planning_ns_total",
		"treedb.cache.flush_apply.build_ns_total",
		"treedb.cache.flush_apply.deferred_vlog_pointer_materialize_ns_total",
		"treedb.cache.flush_apply.vlog_flush_ns_total",
		"treedb.cache.flush_apply.vlog_sync_ns_total",
		"treedb.cache.flush_apply.leaf_log_encode_compress_ns_total",
		"treedb.flush_apply.apply_ns_total",
		"treedb.cache.checkpoint.active_background_flush_wait_ns_total",
		"treedb.cache.write.wait_for_checkpoint.ns_total",
		"treedb.cache.write.wait_for_checkpoint.count_total",
		"treedb.cache.checkpoint.flush_preempt_requests_total",
		"treedb.cache.flush_apply.coordinator.checkpoint_preemptions_total",
		"treedb.cache.checkpoint.debt_memtables_last",
		"treedb.cache.checkpoint.debt_bytes_last",
		"treedb.cache.checkpoint.barrier_wait_ns_total",
		"treedb.cache.checkpoint.flush_all.worker_passes_total",
		"treedb.cache.checkpoint.stage.command_wal_publish.samples",
		"treedb.cache.checkpoint.stage.backend_boundary.samples",
		"treedb.cache.checkpoint.stage.leaf_value_log_sync.samples",
		"treedb.cache.checkpoint.stage.reducer_publish.samples",
	} {
		// Tiny stage timers can round to zero on low-resolution platforms; the
		// plumbing requirement is that these counters are present in DB.Stats().
		_ = requireStatUint64(t, stats, key)
	}
	if got := stats["treedb.cache.flush_apply.leaf_log_append_frames_per_op"]; got == "" {
		t.Fatalf("missing leaf_log_append_frames_per_op")
	}
	for _, key := range []string{
		"treedb.cache.flush_apply.batches_total",
		"treedb.cache.flush_apply.bytes_total",
		"treedb.cache.flush_apply.backend_write_ns_total",
		"treedb.cache.flush_apply.backend_batch_write_ns_total",
		"treedb.cache.flush_span_run.runs_total",
		"treedb.cache.flush_span_run.source_point_ops_total",
		"treedb.cache.flush_span_run.planned_ops_total",
		"treedb.cache.flush_span_run.planned_point_ops_total",
		"treedb.cache.flush_span_run.source_memtables_total",
		"treedb.cache.flush_span_run.backend_chunks_total",
		"treedb.flush_apply.apply_calls_total",
		"treedb.flush_apply.old_leaf_read_decode.node_loads_total",
		"treedb.flush_apply.merge_build.leaf_merges_total",
		"treedb.flush_apply.prepared_output.leaf_log_pages_installed_total",
		"treedb.flush_apply.guarded_publish.calls_total",
		"treedb.cache.checkpoint.stage.cutover.samples",
		"treedb.cache.checkpoint.stage.wal_rotate.samples",
		"treedb.cache.checkpoint.stage.value_log_flush.samples",
		"treedb.cache.checkpoint.stage.flush_all.samples",
		"treedb.cache.checkpoint.stage.flush_all.total_ns",
		"treedb.cache.checkpoint.debt_memtables_max",
		"treedb.cache.checkpoint.debt_bytes_max",
		"treedb.cache.checkpoint.barrier_wait_samples",
		"treedb.cache.checkpoint.flush_all.workers_total",
		"treedb.cache.checkpoint.flush_all.workers_max",
		"treedb.cache.checkpoint.stage.wal_cleanup.samples",
		"treedb.cache.checkpoint.stage.post_maintenance.samples",
	} {
		if got := requireStatUint64(t, stats, key); got == 0 {
			t.Fatalf("%s=%d want >0", key, got)
		}
	}
	// Outer leaves are persisted through the persistent leaf log when enabled;
	// the M0 counters expose that append path separately from backend apply.
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.leaf_log_append_records_total"); got == 0 {
		t.Fatalf("leaf log append records = 0, want >0")
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.leaf_log_append_frames_total"); got == 0 {
		t.Fatalf("leaf log append frames = 0, want >0")
	}
}

func TestWriteWaitForCheckpointStatsIncrement(t *testing.T) {
	db := &DB{}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)

	stats := map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	for _, key := range []string{
		"treedb.cache.write.wait_for_checkpoint.ns_total",
		"treedb.cache.write.wait_for_checkpoint.ns_max",
		"treedb.cache.write.wait_for_checkpoint.ns_last",
		"treedb.cache.write.wait_for_checkpoint.count_total",
		"treedb.cache.write.wait_for_checkpoint.active",
	} {
		if got := requireStatUint64(t, stats, key); got != 0 {
			t.Fatalf("%s before wait=%d, want 0 (stats=%#v)", key, got, stats)
		}
	}

	db.checkpointing.Store(true)
	done := make(chan struct{})
	go func() {
		db.waitForCheckpointForWrite()
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for db.writeWaitForCheckpointActive.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("waitForCheckpointForWrite did not enter checkpoint wait")
		}
		runtime.Gosched()
	}
	db.checkpointMu.Lock()
	db.checkpointing.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForCheckpointForWrite did not unblock")
	}

	stats = map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.count_total"); got != 1 {
		t.Fatalf("wait_for_checkpoint.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.active"); got != 0 {
		t.Fatalf("wait_for_checkpoint.active=%d, want 0 (stats=%#v)", got, stats)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait.checkpoint_drain.count_total"); got != 1 {
		t.Fatalf("checkpoint_drain.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
	for _, key := range []string{
		"treedb.cache.write.wait_for_checkpoint.ns_total",
		"treedb.cache.write.wait_for_checkpoint.ns_max",
		"treedb.cache.write.wait_for_checkpoint.ns_last",
	} {
		if got := requireStatUint64(t, stats, key); got == 0 {
			t.Fatalf("%s=0, want >0 (stats=%#v)", key, stats)
		}
	}
}

func TestWriteWaitReasonStatsDistribution(t *testing.T) {
	db := &DB{}
	for _, wait := range []time.Duration{
		5 * time.Microsecond,
		75 * time.Microsecond,
		2 * time.Millisecond,
		75 * time.Millisecond,
		2 * time.Second,
	} {
		db.observeWriteWaitReason(writeWaitReasonFrontierCutover, wait)
	}
	db.observeWriteWaitReason(writeWaitReasonCheckpointDrain, 250*time.Millisecond)
	db.observeWriteWaitReason(writeWaitReasonMaintenance, 0)

	stats := map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	assertStat := func(key string, want uint64) {
		t.Helper()
		if got := requireStatUint64(t, stats, key); got != want {
			t.Fatalf("%s=%d, want %d", key, got, want)
		}
	}
	assertStat("treedb.cache.write.wait.frontier_cutover.count_total", 5)
	assertStat("treedb.cache.write.wait.frontier_cutover.bucket_le_10us.count_total", 1)
	assertStat("treedb.cache.write.wait.frontier_cutover.bucket_le_100us.count_total", 2)
	assertStat("treedb.cache.write.wait.frontier_cutover.bucket_le_1ms.count_total", 2)
	assertStat("treedb.cache.write.wait.frontier_cutover.bucket_le_10ms.count_total", 3)
	assertStat("treedb.cache.write.wait.frontier_cutover.bucket_le_100ms.count_total", 4)
	assertStat("treedb.cache.write.wait.frontier_cutover.bucket_le_5s.count_total", 5)
	assertStat("treedb.cache.write.wait.frontier_cutover.bucket_le_inf.count_total", 5)
	assertStat("treedb.cache.write.wait.frontier_cutover.p50_upper_ns", uint64((10 * time.Millisecond).Nanoseconds()))
	assertStat("treedb.cache.write.wait.frontier_cutover.p95_upper_ns", uint64((5 * time.Second).Nanoseconds()))
	assertStat("treedb.cache.write.wait.frontier_cutover.p99_upper_ns", uint64((5 * time.Second).Nanoseconds()))
	assertStat("treedb.cache.write.wait.checkpoint_drain.count_total", 1)
	assertStat("treedb.cache.write.wait.maintenance.count_total", 1)
	assertStat("treedb.cache.write.wait.maintenance.ns_total", 1)
}

func TestWriteWaitReasonStatsConcurrentSnapshotsNeverPublishIncompleteHistogram(t *testing.T) {
	var reasonStats writeWaitReasonStats
	const (
		writers          = 8
		samplesPerWriter = 25_000
	)

	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < writers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for sample := 0; sample < samplesPerWriter; sample++ {
				reasonStats.observe(time.Duration((worker+sample)%9+1) * time.Microsecond)
			}
		}(worker)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	close(start)

	var consistencyErr error
	finished := false
	for !finished && consistencyErr == nil {
		stats := map[string]string{}
		appendWriteWaitReasonStats(stats, "checkpoint_drain", &reasonStats)
		count := requireStatUint64(t, stats, "treedb.cache.write.wait.checkpoint_drain.count_total")
		bucketTotal := requireStatUint64(t, stats, "treedb.cache.write.wait.checkpoint_drain.bucket_le_inf.count_total")
		if bucketTotal < count {
			consistencyErr = fmt.Errorf("published samples=%d before histogram total=%d", count, bucketTotal)
			break
		}
		if count > 0 {
			for _, percentile := range []string{"p50", "p95", "p99"} {
				key := "treedb.cache.write.wait.checkpoint_drain." + percentile + "_upper_ns"
				if got := requireStatUint64(t, stats, key); got == ^uint64(0) {
					consistencyErr = fmt.Errorf("%s reported +Inf with samples=%d histogram_total=%d", key, count, bucketTotal)
					break
				}
			}
		}
		select {
		case <-done:
			finished = true
		default:
			runtime.Gosched()
		}
	}
	if !finished {
		<-done
	}
	if consistencyErr != nil {
		t.Fatal(consistencyErr)
	}

	stats := map[string]string{}
	appendWriteWaitReasonStats(stats, "checkpoint_drain", &reasonStats)
	want := uint64(writers * samplesPerWriter)
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait.checkpoint_drain.count_total"); got != want {
		t.Fatalf("final samples=%d, want %d", got, want)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait.checkpoint_drain.bucket_le_inf.count_total"); got != want {
		t.Fatalf("final histogram total=%d, want %d", got, want)
	}
}

func TestObserveWriteWaitForCheckpointPreservesZeroDurationSample(t *testing.T) {
	db := &DB{}
	db.observeWriteWaitForCheckpoint(0)

	stats := map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.count_total"); got != 1 {
		t.Fatalf("wait_for_checkpoint.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
	for _, key := range []string{
		"treedb.cache.write.wait_for_checkpoint.ns_total",
		"treedb.cache.write.wait_for_checkpoint.ns_max",
		"treedb.cache.write.wait_for_checkpoint.ns_last",
	} {
		if got := requireStatUint64(t, stats, key); got != 1 {
			t.Fatalf("%s=%d, want 1 (stats=%#v)", key, got, stats)
		}
	}
}

func TestBeginDirectWriteRecordsCheckpointWait(t *testing.T) {
	db := &DB{}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	db.checkpointing.Store(true)

	done := make(chan error, 1)
	go func() {
		err := db.beginDirectWrite()
		if err == nil {
			db.writeMu.RUnlock()
		}
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	db.checkpointMu.Lock()
	db.checkpointing.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("beginDirectWrite: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("beginDirectWrite did not unblock")
	}

	stats := map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.count_total"); got != 1 {
		t.Fatalf("wait_for_checkpoint.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
}

func TestBeginDirectWriteRecordsCheckpointWaitQueuedOnWriteMu(t *testing.T) {
	db := &DB{}
	db.checkpointing.Store(true)
	db.writeMu.Lock()

	done := make(chan error, 1)
	go func() {
		err := db.beginDirectWrite()
		if err == nil {
			db.writeMu.RUnlock()
		}
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	db.checkpointing.Store(false)
	db.writeMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("beginDirectWrite: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("beginDirectWrite did not unblock")
	}

	stats := map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.count_total"); got != 1 {
		t.Fatalf("wait_for_checkpoint.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.ns_total"); got == 0 {
		t.Fatalf("wait_for_checkpoint.ns_total=0, want >0 (stats=%#v)", stats)
	}
}

func TestBeginExclusiveWriteRecordsCheckpointWait(t *testing.T) {
	db := &DB{}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	db.checkpointing.Store(true)

	done := make(chan struct{})
	go func() {
		db.beginExclusiveWrite()
		db.writeMu.Unlock()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	db.checkpointMu.Lock()
	db.checkpointing.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("beginExclusiveWrite did not unblock")
	}

	stats := map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.count_total"); got != 1 {
		t.Fatalf("wait_for_checkpoint.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
}

func TestBeginExclusiveWriteRecordsCheckpointWaitQueuedOnWriteMu(t *testing.T) {
	db := &DB{}
	db.checkpointing.Store(true)
	db.writeMu.Lock()

	done := make(chan struct{})
	go func() {
		db.beginExclusiveWrite()
		db.writeMu.Unlock()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	db.checkpointing.Store(false)
	db.writeMu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("beginExclusiveWrite did not unblock")
	}

	stats := map[string]string{}
	db.appendWriteWaitForCheckpointStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.count_total"); got != 1 {
		t.Fatalf("wait_for_checkpoint.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait_for_checkpoint.ns_total"); got == 0 {
		t.Fatalf("wait_for_checkpoint.ns_total=0, want >0 (stats=%#v)", stats)
	}
}

func TestFlushAdmissionPropagationThroughCachedOpen(t *testing.T) {
	prev := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	dir := t.TempDir()
	backendOpts := backenddb.Options{
		Dir:                   dir,
		FlushAdmissionPolicy:  backenddb.FlushAdmissionPolicyAuto,
		FlushApplyConcurrency: 16,
	}
	decision := backenddb.NormalizeFlushAdmissionOptions(&backendOpts)
	if decision.FlushApplyConcurrencyConfigured != 16 || decision.FlushApplyConcurrency != 8 {
		t.Fatalf("normalized configured/selected=%d/%d want 16/8", decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrency)
	}

	backend, err := backenddb.Open(backendOpts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	db, err := Open(dir, backend, Options{
		FlushThreshold:         1 << 20,
		MemtableShards:         1,
		JournalLanes:           1,
		FlushApplyConcurrency:  backendOpts.FlushApplyConcurrency,
		FlushApplyMinEntries:   backendOpts.FlushApplyMinEntries,
		FlushApplyMinSpans:     backendOpts.FlushApplyMinSpans,
		FlushApplyMinBytes:     backendOpts.FlushApplyMinBytes,
		FlushApplySpanNative:   backendOpts.FlushApplySpanNative,
		FlushBacklogCoalescing: backendOpts.FlushBacklogCoalescing,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open cache: %v", err)
	}
	defer func() { _ = db.Close() }()

	stats := db.Stats()
	for key, want := range map[string]string{
		"treedb.flush_admission.policy":                             "auto",
		"treedb.flush_admission.admitted":                           "true",
		"treedb.flush_admission.reason":                             backenddb.FlushAdmissionReasonAutoAdmittedHardwareAware,
		"treedb.flush_admission.flush_apply_concurrency_configured": "16",
		"treedb.flush_admission.flush_apply_concurrency":            "8",
		"treedb.flush_admission.flush_apply_concurrency_cap_reason": backenddb.FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS,
		"treedb.flush_admission.flush_apply_concurrency_defaulted":  "false",
		"treedb.flush_admission.gomaxprocs":                         "8",
		"treedb.cache.flush_apply.concurrency":                      "8",
		"treedb.cache.flush_apply.span_native":                      "true",
		"treedb.cache.flush_backlog_coalescing.enabled":             "true",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%s]=%q want %q", key, got, want)
		}
	}
	if got := stats["treedb.flush_admission.physical_cores"]; got == "" {
		t.Fatal("missing physical core admission stat")
	}
}

func TestLeafLogLaneStatsExposeConfiguredActiveAndPerLaneCounters(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true}
	lane0 := &lane{id: leafLogLaneID, vlogPath: "leaf-0", vlogSeq: 1, vlogClosedSizes: map[string]int64{"old-0": 1}}
	lane1 := &lane{id: leafLogLaneID, vlogPath: "leaf-1", vlogSeq: 2}
	db.leafLogAppendLanes = []*lane{lane0, lane1}
	db.observeLeafLogLaneAppend(lane0, 3*time.Nanosecond, 5*time.Nanosecond, 128, 1, nil)
	db.observeLeafLogLaneAppend(lane1, 7*time.Nanosecond, 11*time.Nanosecond, 256, 2, nil)
	lane1.vlogRotateTotal.Add(1)
	lane1.vlogRotateIdleTotal.Add(1)

	stats := map[string]string{}
	db.appendCacheFlushApplyStats(stats)
	for key, want := range map[string]uint64{
		"treedb.cache.leaf_log_lanes.configured":                           2,
		"treedb.cache.leaf_log_lanes.active":                               2,
		"treedb.cache.leaf_log_lanes.append_lanes_used":                    2,
		"treedb.cache.leaf_log_lanes.append_calls_total":                   2,
		"treedb.cache.leaf_log_lanes.append_pages_total":                   3,
		"treedb.cache.leaf_log_lanes.append_bytes_total":                   384,
		"treedb.cache.leaf_log_lanes.append_lock_wait_ns_total":            10,
		"treedb.cache.leaf_log_lanes.append_lock_hold_ns_total":            16,
		"treedb.cache.leaf_log_lanes.segment_rotations_total":              1,
		"treedb.cache.leaf_log_lanes.segment_rotations_idle_total":         1,
		"treedb.cache.leaf_log_lanes.lane.00.append_calls_total":           1,
		"treedb.cache.leaf_log_lanes.lane.00.append_pages_total":           1,
		"treedb.cache.leaf_log_lanes.lane.00.segments_active":              1,
		"treedb.cache.leaf_log_lanes.lane.00.segments_closed":              1,
		"treedb.cache.leaf_log_lanes.lane.01.append_calls_total":           1,
		"treedb.cache.leaf_log_lanes.lane.01.append_pages_total":           2,
		"treedb.cache.leaf_log_lanes.lane.01.segment_rotations_total":      1,
		"treedb.cache.leaf_log_lanes.lane.01.segment_rotations_idle_total": 1,
	} {
		if got := requireStatUint64(t, stats, key); got != want {
			t.Fatalf("%s=%d want %d", key, got, want)
		}
	}
}

func TestObserveLeafLogLaneAppendHasNoAllocations(t *testing.T) {
	db := &DB{}
	l := &lane{}
	allocs := testing.AllocsPerRun(1000, func() {
		db.observeLeafLogLaneAppend(l, time.Nanosecond, time.Nanosecond, 128, 1, nil)
	})
	if allocs != 0 {
		t.Fatalf("observeLeafLogLaneAppend allocations/run=%f want 0", allocs)
	}
}

func TestFlushSpanRunStatsSeparateSourceShadowedAndPlannedOps(t *testing.T) {
	db := &DB{}
	db.observeFlushSpanRunSource(2, 3, 0, 0)
	db.observeFlushSpanRunShadowedOps(1)
	db.observeFlushSpanRunPlannedOps(2, 0)
	stats := map[string]string{}
	db.appendCacheFlushApplyStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.source_point_ops_total"); got != 3 {
		t.Fatalf("source point ops=%d want 3", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.shadowed_ops_total"); got != 1 {
		t.Fatalf("shadowed ops=%d want 1", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.planned_point_ops_total"); got != 2 {
		t.Fatalf("planned point ops=%d want 2", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.planned_ops_total"); got != 2 {
		t.Fatalf("planned ops=%d want 2", got)
	}
}

func TestFlushSpanRunStatsCombinedFlushCountsPostShadowPlannedOps(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 60,
		FlushBuildConcurrency: 2,
		FlushBuildMinEntries:  1,
		FlushBuildMinUnits:    2,
		MemtableShards:        1,
		JournalLanes:          1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("old"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate old memtable: %v", err)
	}
	setMutable(db, []byte("k"), []byte("new"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate new memtable: %v", err)
	}
	setMutable(db, []byte("other"), []byte("kept"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate other memtable: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.source_point_ops_total"); got != 3 {
		t.Fatalf("source point ops=%d want 3", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.shadowed_ops_total"); got != 1 {
		t.Fatalf("shadowed ops=%d want 1", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.planned_point_ops_total"); got != 2 {
		t.Fatalf("planned point ops=%d want 2", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.planned_ops_total"); got != 2 {
		t.Fatalf("planned ops=%d want 2", got)
	}
}

func TestCanonicalFlushRunShadowsOlderOverlappingMemtables(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold:        1 << 60,
		FlushBuildConcurrency: 2,
		FlushBuildMinEntries:  1,
		FlushBuildMinUnits:    2,
		MemtableShards:        1,
		JournalLanes:          1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.Lock()
	setMutable(db, []byte("dup"), []byte("old"))
	setMutable(db, []byte("old-only"), []byte("kept-old"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate old: %v", err)
	}
	setMutable(db, []byte("dup"), []byte("new"))
	setMutable(db, []byte("new-only"), []byte("kept-new"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate new: %v", err)
	}
	units, _, _, totalLen := db.collectFlushUnitsLocked(0, flushCombineMaxMemtables, 0)
	db.mu.Unlock()

	run, err := db.buildCanonicalFlushRun(units, totalLen, 0)
	if err != nil {
		t.Fatalf("buildCanonicalFlushRun: %v", err)
	}
	defer run.release()
	if got, want := run.sourcePointOps, 4; got != want {
		t.Fatalf("source point ops=%d want %d", got, want)
	}
	if got, want := run.shadowedPointOps, 1; got != want {
		t.Fatalf("shadowed point ops=%d want %d", got, want)
	}
	if got, want := run.plannedPointOps, 3; got != want {
		t.Fatalf("planned point ops=%d want %d", got, want)
	}
	got := map[string]string{}
	for _, op := range run.pointOps {
		got[string(op.Key)] = string(op.Value)
	}
	want := map[string]string{"dup": "new", "new-only": "kept-new", "old-only": "kept-old"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical point ops=%v want %v", got, want)
	}
}

func TestCollectFlushUnitsStopsAtLaneBarrier(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold: 1 << 60,
		MemtableShards: 2,
		JournalLanes:   2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	keyForLane := func(lane int) []byte {
		t.Helper()
		for i := 0; i < 10000; i++ {
			key := []byte(fmt.Sprintf("lane-%d-key-%d", lane, i))
			if db.laneForShardIndex(db.shardIndex(key)) == lane {
				return key
			}
		}
		t.Fatalf("no key for lane %d", lane)
		return nil
	}
	lane0Key := keyForLane(0)
	lane1Key := keyForLane(1)

	db.mu.Lock()
	setMutable(db, lane0Key, []byte("l0-old"))
	setMutable(db, lane1Key, []byte("l1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate first: %v", err)
	}
	setMutable(db, lane0Key, []byte("l0-new"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate second: %v", err)
	}
	units, ids, _, _ := db.collectFlushUnitsLocked(0, flushCombineMaxMemtables, 0)
	db.mu.Unlock()

	if got, want := len(units), 1; got != want {
		t.Fatalf("collected units=%d want %d (ids=%v)", got, want, ids)
	}
	if got := units[0].laneID; got != 0 {
		t.Fatalf("collected lane=%d want 0", got)
	}
}

func TestFlushSpanRunLeafAwareChunksDoNotSplitTargetLeaves(t *testing.T) {
	ops := make([]batch.Entry, 6)
	for i := range ops {
		ops[i] = batch.Entry{Type: batch.OpPut, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}
	}
	spans := []backenddb.FlushSpanRunTargetLeafSpan{
		{SpanIndex: 0, PointOpStart: 0, PointOpEnd: 3, OpCount: 3, ByteCount: 30},
		{SpanIndex: 1, PointOpStart: 3, PointOpEnd: 6, OpCount: 3, ByteCount: 30},
	}
	entryChunks := buildEntryCountFlushSpanRunChunks(ops, 4)
	if got := backenddb.SummarizeFlushSpanRunChunkSplits(spans, entryChunks).TargetLeavesSplitAcrossChunks; got == 0 {
		t.Fatalf("entry-count chunks did not split fixture target leaves")
	}
	leafChunks, exact := buildLeafAwareFlushSpanRunChunks(ops, spans, 4)
	if !exact {
		t.Fatalf("leaf-aware chunks reported inexact")
	}
	if got := backenddb.SummarizeFlushSpanRunChunkSplits(spans, leafChunks).TargetLeavesSplitAcrossChunks; got != 0 {
		t.Fatalf("leaf-aware split target leaves=%d want 0 (chunks=%+v)", got, leafChunks)
	}
}

func TestFlushSpanRunLeafAwareChunksExposeEmergencySplit(t *testing.T) {
	ops := make([]batch.Entry, 10)
	for i := range ops {
		ops[i] = batch.Entry{Type: batch.OpPut, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}
	}
	spans := []backenddb.FlushSpanRunTargetLeafSpan{
		{SpanIndex: 0, PointOpStart: 0, PointOpEnd: 10, OpCount: 10, ByteCount: 100},
	}
	chunks, exact := buildLeafAwareFlushSpanRunChunks(ops, spans, 4)
	if !exact {
		t.Fatalf("leaf-aware chunks reported inexact")
	}
	summary := backenddb.SummarizeFlushSpanRunChunkSplits(spans, chunks)
	if got := summary.TargetLeavesSplitAcrossChunks; got != 1 {
		t.Fatalf("emergency split target leaves=%d want 1 (summary=%+v chunks=%+v)", got, summary, chunks)
	}
}

func TestFlushSpanRunRuntimeTargetLeafPlanningDefaultOff(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), FlushAdmissionPolicy: backenddb.FlushAdmissionPolicyExplicit})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:         1 << 60,
		FlushBuildConcurrency:  2,
		FlushBuildMinEntries:   1,
		FlushBuildMinUnits:     2,
		FlushBackendMaxEntries: 2,
		FlushBackendMaxBatches: -1,
		MemtableShards:         1,
		JournalLanes:           1,
	})
	if err != nil {
		t.Fatalf("Open cache: %v", err)
	}
	defer func() { _ = db.Close() }()

	queueTwoPointMemtables(t, db)
	if !db.flushLaneOnce(true, 0) {
		t.Fatal("flushLaneOnce returned false")
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.target_leaf_spans_total"); got != 0 {
		t.Fatalf("default target leaf spans=%d want 0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.flush_apply.read_only_prepare.calls_total"); got != 0 {
		t.Fatalf("default read-only prepare calls=%d want 0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.backend_chunks_total"); got != 2 {
		t.Fatalf("backend chunks=%d want entry-count chunks", got)
	}
}

type malformedChunkPlannerBackend struct {
	*MockBackend
	chunks []backenddb.FlushSpanRunBackendChunk
}

func (b *malformedChunkPlannerBackend) PlanFlushSpanRunChunks(req backenddb.FlushSpanRunPlanRequest, maxPointOpsPerChunk int) (backenddb.FlushSpanRunChunkPlan, error) {
	meta := backenddb.FlushSpanRunMetadata{
		SourceMemtables:  req.SourceMemtables,
		SourcePointOps:   req.SourcePointOps,
		PlannedPointOps:  req.PlannedPointOps,
		ShadowedPointOps: req.ShadowedPointOps,
		RangeBarriers:    req.RangeBarriers,
		LaneBarriers:     req.LaneBarriers,
	}
	chunks := append([]backenddb.FlushSpanRunBackendChunk(nil), b.chunks...)
	meta.BackendChunks = chunks
	return backenddb.FlushSpanRunChunkPlan{Metadata: meta, BackendChunks: chunks}, nil
}

func TestFlushSpanRunPlannedBackendChunksFallbackOnInvalidCoverage(t *testing.T) {
	ops := []batch.Entry{
		{Key: []byte("k0"), Value: []byte("v")},
		{Key: []byte("k1"), Value: []byte("v")},
		{Key: []byte("k2"), Value: []byte("v")},
		{Key: []byte("k3"), Value: []byte("v")},
	}
	for _, tc := range []struct {
		name   string
		chunks []backenddb.FlushSpanRunBackendChunk
	}{
		{
			name: "gapped",
			chunks: []backenddb.FlushSpanRunBackendChunk{
				{ChunkIndex: 0, PointOpStart: 0, PointOpEnd: 1, ByteCount: 1},
				{ChunkIndex: 1, PointOpStart: 2, PointOpEnd: 4, ByteCount: 2},
			},
		},
		{
			name:   "tail-truncated",
			chunks: []backenddb.FlushSpanRunBackendChunk{{ChunkIndex: 0, PointOpStart: 0, PointOpEnd: 2, ByteCount: 2}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &malformedChunkPlannerBackend{MockBackend: NewMockBackend(), chunks: tc.chunks}
			db := &DB{backend: backend, flushSpanRunTargetPlanning: true}
			run := &canonicalFlushRun{sourceMemtables: 2, sourcePointOps: len(ops), plannedPointOps: len(ops), pointOps: ops}

			meta, chunks, _ := db.planCanonicalFlushRunMetadata(run, 2)
			want := []backenddb.FlushSpanRunBackendChunk{
				{ChunkIndex: 0, PointOpStart: 0, PointOpEnd: 2, ByteCount: 6},
				{ChunkIndex: 1, PointOpStart: 2, PointOpEnd: 4, ByteCount: 6},
			}
			if !reflect.DeepEqual(chunks, want) {
				t.Fatalf("chunks=%+v want fallback %+v", chunks, want)
			}
			if !reflect.DeepEqual(meta.BackendChunks, want) {
				t.Fatalf("metadata chunks=%+v want fallback %+v", meta.BackendChunks, want)
			}
		})
	}
}

func TestFlushCanonicalStreamedDisabledChunkingUsesRunSizedBuffer(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:         1 << 60,
		FlushBuildConcurrency:  2,
		FlushBuildMinEntries:   1,
		FlushBuildMinUnits:     2,
		FlushBackendMaxEntries: -1,
		MemtableShards:         1,
		JournalLanes:           1,
	})
	if err != nil {
		t.Fatalf("Open cache: %v", err)
	}
	defer func() { _ = db.Close() }()

	queueTwoPointMemtables(t, db)
	if !db.flushLaneOnce(true, 0) {
		t.Fatal("flushLaneOnce returned false")
	}
	if got := requireStatUint64(t, db.Stats(), "treedb.cache.flush_span_run.backend_chunks_total"); got != 1 {
		t.Fatalf("backend chunks=%d want one unchunked write", got)
	}
}

func TestFlushSpanRunRuntimeTargetLeafSplitCounterOptIn(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:             1 << 60,
		FlushBuildConcurrency:      2,
		FlushBuildMinEntries:       1,
		FlushBuildMinUnits:         2,
		FlushBackendMaxEntries:     2,
		FlushBackendMaxBatches:     -1,
		FlushSpanRunTargetPlanning: true,
		MemtableShards:             1,
		JournalLanes:               1,
	})
	if err != nil {
		t.Fatalf("Open cache: %v", err)
	}
	defer func() { _ = db.Close() }()

	queueTwoPointMemtables(t, db)
	if !db.flushLaneOnce(true, 0) {
		t.Fatal("flushLaneOnce returned false")
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.target_leaf_spans_total"); got == 0 {
		t.Fatalf("target leaf spans = 0, want runtime metadata")
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.target_leaves_split_across_chunks_total"); got == 0 {
		t.Fatalf("target leaf split counter = 0, want emergency split evidence")
	}
	if got := requireStatUint64(t, stats, "treedb.flush_apply.read_only_prepare.calls_total"); got == 0 {
		t.Fatalf("read-only prepare calls = 0, want opt-in planning")
	}
}

func queueTwoPointMemtables(t *testing.T, db *DB) {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	setMutable(db, []byte("k0"), []byte("v0"))
	setMutable(db, []byte("k1"), []byte("v1"))
	if err := db.rotateMemtableLocked(false); err != nil {
		t.Fatalf("rotate first: %v", err)
	}
	setMutable(db, []byte("k2"), []byte("v2"))
	setMutable(db, []byte("k3"), []byte("v3"))
	if err := db.rotateMemtableLocked(false); err != nil {
		t.Fatalf("rotate second: %v", err)
	}
}

func TestFlushSpanRunBackendChunksExcludeEmptySyncBoundary(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	for _, tc := range []struct {
		name     string
		parallel bool
	}{
		{name: "parallel", parallel: true},
		{name: "sequential", parallel: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := NewMockBackend()
			opts := Options{
				FlushThreshold:         1 << 60,
				FlushBackendMaxEntries: 2,
				FlushBackendMaxBatches: -1,
				MemtableShards:         1,
				JournalLanes:           1,
			}
			if tc.parallel {
				opts.FlushBuildConcurrency = 2
				opts.FlushBuildMinEntries = 1
				opts.FlushBuildMinUnits = 2
			} else {
				opts.FlushBuildConcurrency = 1
			}
			db, err := Open(t.TempDir(), backend, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			if tc.parallel {
				for unit := 0; unit < 2; unit++ {
					for i := 0; i < 2; i++ {
						setMutable(db, []byte(fmt.Sprintf("k%d%d", unit, i)), []byte("v"))
					}
					db.mu.Lock()
					if err := db.rotateMemtableLocked(false); err != nil {
						db.mu.Unlock()
						t.Fatalf("rotate memtable %d: %v", unit, err)
					}
					db.mu.Unlock()
				}
			} else {
				for i := 0; i < 4; i++ {
					setMutable(db, []byte(fmt.Sprintf("k%d", i)), []byte("v"))
				}
				db.mu.Lock()
				if err := db.rotateMemtableLocked(false); err != nil {
					db.mu.Unlock()
					t.Fatalf("rotate memtable: %v", err)
				}
				db.mu.Unlock()
			}

			if !db.flushLaneOnce(true, 0) {
				t.Fatal("flushLaneOnce returned false")
			}

			stats := db.Stats()
			if got := requireStatUint64(t, stats, "treedb.cache.flush_span_run.backend_chunks_total"); got != 2 {
				t.Fatalf("backend chunks=%d want 2", got)
			}
			backend.mu.RLock()
			writeCalls := backend.writeCalls
			writeSyncs := backend.writeSyncs
			backend.mu.RUnlock()
			if writeCalls != 2 || writeSyncs != 1 {
				t.Fatalf("backend writeCalls/writeSyncs=%d/%d want 2/1", writeCalls, writeSyncs)
			}
		})
	}
}

func TestFlushApplyStatsHelpersExposeForegroundAssist(t *testing.T) {
	db := &DB{}
	db.observeForegroundFlushAssist(2*time.Millisecond, 3)
	stats := map[string]string{}
	db.appendCacheFlushApplyStats(stats)
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.foreground_assist_calls_total"); got != 1 {
		t.Fatalf("foreground assist calls=%d want 1", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.foreground_assist_flushes_total"); got != 3 {
		t.Fatalf("foreground assist flushes=%d want 3", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.flush_apply.foreground_assist_wait_ns_total"); got == 0 {
		t.Fatalf("foreground assist wait ns=%d want >0", got)
	}
}
