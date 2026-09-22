package caching

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
)

func requireStatFloat64(t *testing.T, stats map[string]string, key string) float64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return v
}

func TestCheckpointWaitProductivityStatsNoopCheckpoint(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold: 1 << 60,
		MemtableShards: 1,
		JournalLanes:   1,
		DisableWAL:     true,
		AllowUnsafe:    true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.checkpointFlushAllWorkersLast.Store(2)
	db.checkpointFlushAllFrontierLanesLast.Store(2)
	db.checkpointOwnedDrainUnitsLast.Store(3)
	db.checkpointOwnedDrainOpsLast.Store(4)
	db.checkpointOwnedDrainBytesLast.Store(5)
	db.checkpointOwnedDrainNsLast.Store(6)
	db.checkpointBackgroundDrainUnitsLast.Store(7)
	db.checkpointBackgroundDrainOpsLast.Store(8)
	db.checkpointBackgroundDrainBytesLast.Store(9)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	stats := db.Stats()
	for _, key := range []string{
		"treedb.cache.checkpoint.debt.queue_units_last",
		"treedb.cache.checkpoint.debt.queue_bytes_last",
		"treedb.cache.checkpoint.debt.mutable_bytes_last",
		"treedb.cache.checkpoint.debt.active_in_flight_bytes_last",
		"treedb.cache.checkpoint.debt.active_workers_last",
		"treedb.cache.checkpoint.wait.frontier_units_at_request_last",
		"treedb.cache.checkpoint.wait.remaining_frontier_units_last",
		"treedb.cache.checkpoint.wait.drained_frontier_units_last",
		"treedb.cache.checkpoint.flush_all.workers_last",
		"treedb.cache.checkpoint.flush_all.frontier_lanes_last",
		"treedb.cache.checkpoint.flush_all.owned_drain_units_last",
		"treedb.cache.checkpoint.flush_all.owned_drain_ops_last",
		"treedb.cache.checkpoint.flush_all.owned_drain_bytes_last",
		"treedb.cache.checkpoint.flush_all.owned_drain_ns_last",
		"treedb.cache.checkpoint.flush_all.background_drain_units_last",
		"treedb.cache.checkpoint.flush_all.background_drain_ops_last",
		"treedb.cache.checkpoint.flush_all.background_drain_bytes_last",
		"treedb.cache.checkpoint.stage.command_wal_cleanup.samples",
		"treedb.cache.checkpoint.stage.command_wal_cleanup.last_ns",
	} {
		if got := requireStatUint64(t, stats, key); got != 0 {
			t.Fatalf("%s=%d want 0", key, got)
		}
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.noop_skips"); got != 1 {
		t.Fatalf("noop skips=%d want 1", got)
	}
	if got := requireStatFloat64(t, stats, "treedb.cache.checkpoint.productive_wait_ratio_last"); got != 0 {
		t.Fatalf("productive wait ratio=%f want 0", got)
	}
}

func TestCheckpointWaitProductivityStatsLateDrainUsesCurrentWaitDuration(t *testing.T) {
	frontier := checkpointFrontier{
		ids: map[uint64]checkpointFrontierUnit{
			1: {bytes: 4096, laneID: 0},
		},
		laneUnits: map[uint16]uint64{0: 1},
		units:     1,
		bytes:     4096,
		captured:  true,
	}

	t.Run("late-drained frontier records current wait", func(t *testing.T) {
		db := &DB{}
		staleWaitNs := uint64(time.Hour.Nanoseconds())
		db.checkpointActiveBackgroundFlushWaitLastNs.Store(staleWaitNs)

		wait := 25 * time.Millisecond
		residual := db.observeCheckpointFlushMuWaitAfterLock(frontier, wait, false)
		if residual.drainedUnits != 1 || residual.drainedBytes != 4096 {
			t.Fatalf("drained frontier = (%d units, %d bytes), want (1, 4096)", residual.drainedUnits, residual.drainedBytes)
		}
		waitNs := uint64(wait.Nanoseconds())
		if got := db.checkpointActiveBackgroundFlushWaitLastNs.Load(); got != waitNs {
			t.Fatalf("active wait last ns=%d want current wait %d, stale was %d", got, waitNs, staleWaitNs)
		}
		if got := db.checkpointActiveBackgroundFlushWaitSamples.Load(); got != 1 {
			t.Fatalf("active wait samples=%d want 1", got)
		}
		currentRate := checkpointRatePerSecond(db.checkpointWaitFrontierDrainedBytesLast.Load(), db.checkpointActiveBackgroundFlushWaitLastNs.Load())
		staleRate := checkpointRatePerSecond(db.checkpointWaitFrontierDrainedBytesLast.Load(), staleWaitNs)
		if currentRate <= staleRate {
			t.Fatalf("wait drain rate=%f should use current wait and exceed stale-rate %f", currentRate, staleRate)
		}
	})

	t.Run("no active or drained frontier clears stale wait", func(t *testing.T) {
		db := &DB{}
		db.queue = append(db.queue, nil)
		db.queueIDs = []uint64{1}
		db.queueLaneIDs = []uint16{0}
		db.checkpointActiveBackgroundFlushWaitLastNs.Store(uint64(time.Hour.Nanoseconds()))

		residual := db.observeCheckpointFlushMuWaitAfterLock(frontier, 25*time.Millisecond, false)
		if residual.drainedUnits != 0 || residual.frontierUnits != 1 {
			t.Fatalf("residual frontier = drained %d remaining %d, want drained 0 remaining 1", residual.drainedUnits, residual.frontierUnits)
		}
		if got := db.checkpointActiveBackgroundFlushWaitLastNs.Load(); got != 0 {
			t.Fatalf("active wait last ns=%d want 0", got)
		}
		if got := db.checkpointActiveBackgroundFlushWaitSamples.Load(); got != 0 {
			t.Fatalf("active wait samples=%d want 0", got)
		}
		if got := checkpointRatePerSecond(db.checkpointWaitFrontierDrainedBytesLast.Load(), db.checkpointActiveBackgroundFlushWaitLastNs.Load()); got != 0 {
			t.Fatalf("wait drain rate=%f want 0", got)
		}
	})
}

func TestCheckpointWaitProductivityStatsQueuedFrontierNoActiveBackground(t *testing.T) {
	// Keep the queued frontier stable until Checkpoint owns flushMu; legacy queue
	// backpressure can otherwise race this no-active-background assertion.
	db, _ := newCoalescingTestDB(t, Options{
		DisableWAL:         true,
		AllowUnsafe:        true,
		MaxQueuedMemtables: -1,
	}, highSingleOpCoalescingSnapshot())
	enqueuePointMemtables(t, db, 2, "queuedfrontier")
	if err := db.Set([]byte("queuedfrontier-mutable"), []byte("value")); err != nil {
		t.Fatalf("Set mutable: %v", err)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.debt.queue_units_last"); got != 2 {
		t.Fatalf("debt queue units=%d want 2", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.debt.queue_bytes_last"); got == 0 {
		t.Fatalf("debt queue bytes=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.debt.mutable_bytes_last"); got == 0 {
		t.Fatalf("mutable bytes=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.debt.active_in_flight_bytes_last"); got != 0 {
		t.Fatalf("active in-flight bytes=%d want 0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.active_in_flight_bytes_at_request_last"); got != 0 {
		t.Fatalf("wait active in-flight bytes=%d want 0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.frontier_units_at_request_last"); got != 2 {
		t.Fatalf("wait frontier at request=%d want 2", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.remaining_frontier_units_last"); got != 2 {
		t.Fatalf("wait remaining frontier=%d want 2", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.drained_frontier_units_last"); got != 0 {
		t.Fatalf("wait drained frontier=%d want 0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.frontier.drained_units_last"); got != 3 {
		t.Fatalf("checkpoint frontier drained=%d want 3", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.flush_all.owned_drain_units_last"); got != 3 {
		t.Fatalf("owned drain units=%d want 3", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.flush_all.owned_drain_bytes_last"); got == 0 {
		t.Fatalf("owned drain bytes=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.active_background_flush_wait_samples"); got != 0 {
		t.Fatalf("active background wait samples=%d want 0", got)
	}
}

type checkpointWaitBlockingBackend struct {
	*MockBackend
	once        sync.Once
	releaseOnce sync.Once
	started     chan struct{}
	release     chan struct{}
}

func newCheckpointWaitBlockingBackend() *checkpointWaitBlockingBackend {
	return &checkpointWaitBlockingBackend{
		MockBackend: NewMockBackend(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (b *checkpointWaitBlockingBackend) NewBatch() batch.Interface {
	return &checkpointWaitBlockingBatch{Interface: b.MockBackend.NewBatch(), backend: b}
}

func (b *checkpointWaitBlockingBackend) blockOnce() {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
}

func (b *checkpointWaitBlockingBackend) releaseBlock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

type checkpointWaitBlockingBatch struct {
	batch.Interface
	backend *checkpointWaitBlockingBackend
}

func (b *checkpointWaitBlockingBatch) Write() error {
	b.backend.blockOnce()
	return b.Interface.Write()
}

func (b *checkpointWaitBlockingBatch) WriteSync() error {
	b.backend.blockOnce()
	return b.Interface.WriteSync()
}

func TestCheckpointWaitProductivityStatsActiveBackgroundDrainsFrontier(t *testing.T) {
	backend := newCheckpointWaitBlockingBackend()
	defer backend.releaseBlock()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:        1 << 60,
		FlushBuildConcurrency: 1,
		MemtableShards:        1,
		JournalLanes:          1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	enqueuePointMemtables(t, db, 2, "activewait")

	bgDone := make(chan struct{})
	go func() {
		db.flushAll(false)
		close(bgDone)
	}()

	select {
	case <-backend.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background flush did not reach blocking backend write")
	}
	if got := db.flushCoordinatorInFlightBytes.Load(); got <= 0 {
		t.Fatalf("in-flight bytes before checkpoint=%d want >0", got)
	}

	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- db.Checkpoint() }()
	deadline := time.After(5 * time.Second)
	for db.checkpointFlushPreemptRequests.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("checkpoint did not reach flush wait")
		default:
			runtimeGosched()
		}
	}

	backend.releaseBlock()
	select {
	case <-bgDone:
	case <-time.After(5 * time.Second):
		t.Fatal("background flush did not finish")
	}
	select {
	case err := <-checkpointDone:
		if err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not finish")
	}

	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.debt.queue_units_last"); got != 2 {
		t.Fatalf("debt queue units=%d want 2", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.debt.active_in_flight_bytes_last"); got == 0 {
		t.Fatalf("active in-flight bytes=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.debt.active_workers_last"); got == 0 {
		t.Fatalf("active workers=%d want >0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.active_workers_at_request_last"); got == 0 {
		t.Fatalf("wait active workers=%d want >0", got)
	}
	activeWaitSamples := requireStatUint64(t, stats, "treedb.cache.checkpoint.active_background_flush_wait_samples")
	activeWaitLastNs := requireStatUint64(t, stats, "treedb.cache.checkpoint.active_background_flush_wait_ns_last")
	if activeWaitSamples == 0 {
		if activeWaitLastNs != 0 {
			t.Fatalf("active background wait last ns=%d want 0 when samples=0", activeWaitLastNs)
		}
	} else if activeWaitLastNs == 0 {
		t.Fatalf("active background wait last ns=%d want >0 when samples=%d", activeWaitLastNs, activeWaitSamples)
	}
	frontierAtRequest := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.frontier_units_at_request_last")
	if frontierAtRequest != 2 {
		t.Fatalf("wait frontier at request=%d want 2", frontierAtRequest)
	}
	remainingFrontier := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.remaining_frontier_units_last")
	drainedFrontier := requireStatUint64(t, stats, "treedb.cache.checkpoint.wait.drained_frontier_units_last")
	if remainingFrontier > frontierAtRequest || drainedFrontier+remainingFrontier != frontierAtRequest {
		t.Fatalf("wait frontier accounting: requested=%d drained=%d remaining=%d", frontierAtRequest, drainedFrontier, remainingFrontier)
	}
	if drainedFrontier == 0 {
		t.Fatalf("wait drained frontier=%d want >0", drainedFrontier)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.frontier.units_last"); got != remainingFrontier {
		t.Fatalf("checkpoint-owned frontier units=%d want remaining %d", got, remainingFrontier)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.checkpoint.flush_all.owned_drain_units_last"); got != remainingFrontier {
		t.Fatalf("owned drain units=%d want remaining %d", got, remainingFrontier)
	}
	if activeWaitLastNs != 0 {
		if got := requireStatFloat64(t, stats, "treedb.cache.checkpoint.wait.frontier_drain_bytes_per_sec_last"); got == 0 {
			t.Fatalf("wait drain bytes/sec=%f want >0", got)
		}
	} else if got := requireStatFloat64(t, stats, "treedb.cache.checkpoint.wait.frontier_drain_bytes_per_sec_last"); got != 0 {
		t.Fatalf("wait drain bytes/sec=%f want 0 when active wait last ns=0", got)
	}
	_ = requireStatFloat64(t, stats, "treedb.cache.checkpoint.productive_wait_ratio_last")
}

func runtimeGosched() {
	runtime.Gosched()
}
