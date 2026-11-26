package mongogateway

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoUpdateCoalescerBatchHysteresis(t *testing.T) {
	t.Run("W1W2ZeroSyscallInlineFastPath", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 8)
		defer func() { _ = s.Close() }()

		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}
		if coalescer.batchHysteresis != defaultUpdateCoalescingBatchHysteresis {
			t.Fatalf("batchHysteresis=%v want %v", coalescer.batchHysteresis, defaultUpdateCoalescingBatchHysteresis)
		}

		var fakeNow atomic.Int64
		fakeNow.Store(1_000_000_000)
		var clockCalls atomic.Int64
		coalescer.nowNano = func() int64 {
			clockCalls.Add(1)
			return fakeNow.Load()
		}

		ctx := context.Background()
		// w1: active == 1 must execute 100% inline with zero clock calls.
		for i := 0; i < 4; i++ {
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(i), int64(10+i)), nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				t.Fatalf("w1 update[%d]: resp=%s err=%v", i, resp, err)
			}
		}
		if got := coalescer.inlineExecs.Load(); got != 4 {
			t.Fatalf("w1 inlineExecs=%d want 4", got)
		}
		if got := coalescer.workerBatches.Load(); got != 0 {
			t.Fatalf("w1 workerBatches=%d want 0", got)
		}
		if got := coalescer.batchModeUntil.Load(); got != 0 {
			t.Fatalf("w1 batchModeUntil=%d want 0", got)
		}
		if got := clockCalls.Load(); got != 0 {
			t.Fatalf("w1 nowNano calls=%d want 0", got)
		}

		// w2..w4: active <= 4 (never > 4) must still execute 100% inline with zero clock calls.
		coalescer.inFlight.Add(3)
		for i := 4; i < 8; i++ {
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(i), int64(10+i)), nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				coalescer.inFlight.Add(-3)
				t.Fatalf("w4 update[%d]: resp=%s err=%v", i, resp, err)
			}
		}
		coalescer.inFlight.Add(-3)

		if got := coalescer.inlineExecs.Load(); got != 8 {
			t.Fatalf("w4 inlineExecs=%d want 8", got)
		}
		if got := coalescer.workerBatches.Load(); got != 0 {
			t.Fatalf("w4 workerBatches=%d want 0", got)
		}
		if got := coalescer.batchModeUntil.Load(); got != 0 {
			t.Fatalf("w4 batchModeUntil=%d want 0", got)
		}
		if got := clockCalls.Load(); got != 0 {
			t.Fatalf("w4 nowNano calls=%d want 0", got)
		}

		// maxDelay > 0 must not latch or invoke clock even when active > 4.
		sDelay, collDelay := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 4)
		defer func() { _ = sDelay.Close() }()
		sDelay.Server.UpdateCoalescingMaxDelay = 2 * time.Millisecond
		cDelay := sDelay.Server.mongoUpdateCoalescer("app." + collDelay)
		if cDelay == nil {
			t.Fatal("expected delayed coalescer")
		}
		var delayClockCalls atomic.Int64
		cDelay.nowNano = func() int64 {
			delayClockCalls.Add(1)
			return 1_000_000_000
		}
		cDelay.inFlight.Add(4)
		resp, err := sDelay.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collDelay, 0, 55), nil, 0)
		cDelay.inFlight.Add(-4)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("maxDelay>0 update: resp=%s err=%v", resp, err)
		}
		if got := delayClockCalls.Load(); got != 0 {
			t.Fatalf("maxDelay>0 nowNano calls=%d want 0", got)
		}
		if got := cDelay.batchModeUntil.Load(); got != 0 {
			t.Fatalf("maxDelay>0 batchModeUntil=%d want 0", got)
		}
	})

	t.Run("LatchActiveOver4WorkerBatchRefreshAndExpiry", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 16)
		defer func() { _ = s.Close() }()

		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}

		const t0 int64 = 1_000_000_000
		hyst := int64(coalescer.effectiveBatchHysteresis())
		if hyst != int64(defaultUpdateCoalescingBatchHysteresis) {
			t.Fatalf("effectiveBatchHysteresis=%d want %d", hyst, int64(defaultUpdateCoalescingBatchHysteresis))
		}

		var fakeNow atomic.Int64
		fakeNow.Store(t0)
		var clockCalls atomic.Int64
		coalescer.nowNano = func() int64 {
			clockCalls.Add(1)
			return fakeNow.Load()
		}

		syncWorkerIdle := func() {
			coalescer.execMu.Lock()
			coalescer.execMu.Unlock()
		}

		ctx := context.Background()

		// Step 1: Observe active == 5 -> latches batchModeUntil = t0 + hyst.
		coalescer.inFlight.Add(4)
		resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, 100), nil, 0)
		coalescer.inFlight.Add(-4)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("active=5 update: resp=%s err=%v", resp, err)
		}
		syncWorkerIdle()

		wantUntil := t0 + hyst
		if got := coalescer.batchModeUntil.Load(); got != wantUntil {
			t.Fatalf("batchModeUntil=%d want %d", got, wantUntil)
		}
		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("inlineExecs=%d want 0 after active=5", got)
		}
		if got := coalescer.workerBatches.Load(); got != 1 {
			t.Fatalf("workerBatches=%d want 1 after active=5", got)
		}

		// Step 2: While now < batchModeUntil, active == 1 and active == 4 stay on worker path.
		fakeNow.Store(wantUntil - 1)
		resp, err = s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 1, 101), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("latched active=1 update: resp=%s err=%v", resp, err)
		}
		syncWorkerIdle()

		coalescer.inFlight.Add(3)
		resp, err = s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 2, 102), nil, 0)
		coalescer.inFlight.Add(-3)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("latched active=4 update: resp=%s err=%v", resp, err)
		}
		syncWorkerIdle()

		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("inlineExecs=%d want 0 while batchModeUntil active", got)
		}
		if got := coalescer.workerBatches.Load(); got != 3 {
			t.Fatalf("workerBatches=%d want 3 while batchModeUntil active", got)
		}

		// Step 3: Worker batch with len(batch) >= 5 refreshes batchModeUntil even
		// if the clock advanced after callers entered runMongoUpdateCoalesced.
		tWorker := wantUntil + int64(5*time.Millisecond)
		coalescer.execMu.Lock()
		const batchSize = 5
		errCh := make(chan error, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for i := 0; i < batchSize; i++ {
			id := int64(3 + i)
			go func() {
				defer wg.Done()
				r, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, id, 200+id), nil, 0)
				if err != nil {
					errCh <- err
					return
				}
				if !mongoCommandResponseOK(r) {
					errCh <- fmt.Errorf("id=%d not ok: %s", id, r)
				}
			}()
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && len(coalescer.requests) < batchSize {
			time.Sleep(time.Millisecond)
		}
		if got := len(coalescer.requests); got != batchSize {
			coalescer.execMu.Unlock()
			t.Fatalf("queued=%d want %d", got, batchSize)
		}
		// Advance clock after all 5 writers already enqueued, so only the worker
		// latch refresh inside runBatchStartingWith sees tWorker.
		fakeNow.Store(tWorker)
		coalescer.execMu.Unlock()
		coalescer.signalWake()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}
		syncWorkerIdle()

		wantRefreshedUntil := tWorker + hyst
		if got := coalescer.batchModeUntil.Load(); got != wantRefreshedUntil {
			t.Fatalf("worker-refreshed batchModeUntil=%d want %d", got, wantRefreshedUntil)
		}
		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("inlineExecs=%d want 0 after worker batch refresh", got)
		}

		// Still latched right before refreshed expiry.
		fakeNow.Store(wantRefreshedUntil - 1)
		resp, err = s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 8, 308), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("pre-expiry update: resp=%s err=%v", resp, err)
		}
		syncWorkerIdle()
		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("inlineExecs=%d want 0 before refreshed expiry", got)
		}

		// Step 4: Advance clock past batchHysteresis -> batchModeUntil clears to 0
		// and inline execution resumes cleanly with zero subsequent clock calls.
		fakeNow.Store(wantRefreshedUntil)
		resp, err = s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 9, 309), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("post-expiry update 1: resp=%s err=%v", resp, err)
		}
		if got := coalescer.batchModeUntil.Load(); got != 0 {
			t.Fatalf("batchModeUntil=%d want 0 after expiry", got)
		}
		if got := coalescer.inlineExecs.Load(); got != 1 {
			t.Fatalf("inlineExecs=%d want 1 after expiry", got)
		}

		callsAfterClear := clockCalls.Load()
		resp, err = s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 9, 310), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("post-expiry update 2: resp=%s err=%v", resp, err)
		}
		if got := coalescer.inlineExecs.Load(); got != 2 {
			t.Fatalf("inlineExecs=%d want 2 after resuming inline", got)
		}
		if got := clockCalls.Load(); got != callsAfterClear {
			t.Fatalf("nowNano calls after clear=%d want %d (zero overhead)", got, callsAfterClear)
		}
		if got := findDocN(t, s.Server, collName, 9); got != 310 {
			t.Fatalf("doc 9 n=%d want 310", got)
		}
	})

	t.Run("ConcurrentCASRefreshRaceNotBypassed", func(t *testing.T) {
		const initialUntil int64 = 1_000_000_000 + int64(25*time.Millisecond)
		const nowAtExpiry int64 = initialUntil + 100
		const refreshedUntil int64 = nowAtExpiry + int64(25*time.Millisecond)

		// Case A: A concurrent writer refreshes batchModeUntil before our expiry CAS.
		c := &mongoUpdateCoalescer{
			batchHysteresis: defaultUpdateCoalescingBatchHysteresis,
		}
		c.batchModeUntil.Store(initialUntil)
		c.nowNano = func() int64 {
			// Simulate concurrent noteBatchConcurrency() arriving after initial Load().
			c.batchModeUntil.Store(refreshedUntil)
			return nowAtExpiry
		}
		if !c.batchModeActive() {
			t.Fatal("batchModeActive()=false want true when concurrent writer refreshed batchModeUntil")
		}
		if got := c.batchModeUntil.Load(); got != refreshedUntil {
			t.Fatalf("batchModeUntil=%d want %d (fresh latch must not be overwritten)", got, refreshedUntil)
		}

		// Case B: A concurrent caller already cleared batchModeUntil to 0 before our expiry CAS.
		c2 := &mongoUpdateCoalescer{
			batchHysteresis: defaultUpdateCoalescingBatchHysteresis,
		}
		c2.batchModeUntil.Store(initialUntil)
		c2.nowNano = func() int64 {
			c2.batchModeUntil.Store(0)
			return nowAtExpiry
		}
		if c2.batchModeActive() {
			t.Fatal("batchModeActive()=true want false when concurrent caller already cleared batchModeUntil")
		}
		if got := c2.batchModeUntil.Load(); got != 0 {
			t.Fatalf("batchModeUntil=%d want 0", got)
		}
	})

	t.Run("MonotonicLatchAndPostBatchRefreshWhileWorkerBusy", func(t *testing.T) {
		const t0 int64 = 1_000_000_000
		const hyst = defaultUpdateCoalescingBatchHysteresis

		// Case A: Stale timestamp in noteBatchConcurrency() must not regress a
		// fresher batchModeUntil deadline backwards.
		cMonotonic := &mongoUpdateCoalescer{
			batchHysteresis: hyst,
		}
		newerUntil := t0 + int64(4*hyst)
		cMonotonic.batchModeUntil.Store(newerUntil)
		var nowMonotonic atomic.Int64
		nowMonotonic.Store(t0)
		cMonotonic.nowNano = nowMonotonic.Load
		cMonotonic.noteBatchConcurrency()
		if got := cMonotonic.batchModeUntil.Load(); got != newerUntil {
			t.Fatalf("stale noteBatchConcurrency regressed batchModeUntil=%d want %d", got, newerUntil)
		}
		nowMonotonic.Store(t0 + int64(5*hyst))
		cMonotonic.noteBatchConcurrency()
		if got, want := cMonotonic.batchModeUntil.Load(), t0+int64(6*hyst); got != want {
			t.Fatalf("fresh noteBatchConcurrency batchModeUntil=%d want %d", got, want)
		}

		// Case B: While workerBusy is true, an active<=2 caller must skip
		// batchModeActive() (avoiding clock reads and premature expiry clear
		// before the worker's post-batch refresh runs).
		sBusy, collBusy := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 4)
		defer func() { _ = sBusy.Close() }()
		cBusy := sBusy.Server.mongoUpdateCoalescer("app." + collBusy)
		if cBusy == nil {
			t.Fatal("expected cached coalescer")
		}
		var busyClockCalls atomic.Int64
		expiredUntil := t0 + int64(hyst)
		cBusy.batchModeUntil.Store(expiredUntil)
		cBusy.nowNano = func() int64 {
			busyClockCalls.Add(1)
			return expiredUntil + int64(hyst)
		}
		cBusy.execMu.Lock()
		cBusy.workerBusy.Store(true)
		busyErrCh := make(chan error, 1)
		go func() {
			r, err := sBusy.Server.commandResponse(context.Background(), "update", buildUpdateOneCommand(t, collBusy, 0, 77), nil, 0)
			if err == nil && !mongoCommandResponseOK(r) {
				err = fmt.Errorf("busy update not ok: %s", r)
			}
			busyErrCh <- err
		}()
		busyDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(busyDeadline) && len(cBusy.requests) < 1 {
			time.Sleep(time.Millisecond)
		}
		queuedBusy := len(cBusy.requests)
		gotClockCalls := busyClockCalls.Load()
		gotUntilWhileBusy := cBusy.batchModeUntil.Load()
		cBusy.workerBusy.Store(false)
		cBusy.execMu.Unlock()
		cBusy.signalWake()
		if err := <-busyErrCh; err != nil {
			t.Fatalf("workerBusy update err: %v", err)
		}
		if queuedBusy != 1 {
			t.Fatalf("workerBusy queued=%d want 1", queuedBusy)
		}
		if gotClockCalls != 0 {
			t.Fatalf("workerBusy nowNano calls=%d want 0", gotClockCalls)
		}
		if gotUntilWhileBusy != expiredUntil {
			t.Fatalf("workerBusy cleared batchModeUntil=%d want %d", gotUntilWhileBusy, expiredUntil)
		}

		// Case C: A slow worker batch (clock advances > batchHysteresis during
		// runBatch) refreshes batchModeUntil at batch completion so the full
		// hysteresis window protects post-batch turnaround.
		sSlow, collSlow := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 8)
		defer func() { _ = sSlow.Close() }()
		colSlow, err := sSlow.Collections.OpenCollection("app." + collSlow)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		makeUnbufferedReq := func(id, val int64) mongoUpdateCoalescerRequest {
			item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: val}}}}},
			}))
			if err != nil {
				t.Fatalf("parseMongoUpdateItem: %v", err)
			}
			return mongoUpdateCoalescerRequest{
				col:  colSlow,
				item: item,
				done: make(chan mongoUpdateCoalescerResult),
			}
		}
		var slowNow atomic.Int64
		slowNow.Store(t0)
		cSlow := &mongoUpdateCoalescer{
			maxBatch:        8,
			batchHysteresis: hyst,
			requests:        make(chan mongoUpdateCoalescerRequest, 8),
			stoppedCh:       make(chan struct{}),
			nowNano:         slowNow.Load,
		}
		req0 := makeUnbufferedReq(0, 10)
		req1 := makeUnbufferedReq(1, 20)
		req2 := makeUnbufferedReq(2, 30)
		req3 := makeUnbufferedReq(3, 40)
		req4 := makeUnbufferedReq(4, 50)
		cSlow.requests <- req1
		cSlow.requests <- req2
		cSlow.requests <- req3
		cSlow.requests <- req4

		doneBatch := make(chan struct{})
		go func() {
			defer close(doneBatch)
			cSlow.runBatchStartingWith(req0)
		}()

		if res := <-req0.done; res.err != nil || !res.matched || !res.modified {
			t.Fatalf("slow req0=%+v want matched,modified,nil", res)
		}
		// Advance clock > batchHysteresis while runBatch is still completing
		// results (blocked sending req1.done).
		tAfterBatch := t0 + int64(2*hyst)
		slowNow.Store(tAfterBatch)
		for i, req := range []mongoUpdateCoalescerRequest{req1, req2, req3, req4} {
			if res := <-req.done; res.err != nil || !res.matched || !res.modified {
				t.Fatalf("slow req%d=%+v want matched,modified,nil", i+1, res)
			}
		}
		<-doneBatch

		wantAfterSlow := tAfterBatch + int64(hyst)
		if got := cSlow.batchModeUntil.Load(); got != wantAfterSlow {
			t.Fatalf("post-slow-batch batchModeUntil=%d want %d (refreshed after runBatch)", got, wantAfterSlow)
		}
		if !cSlow.batchModeActive() {
			t.Fatal("batchModeActive()=false immediately after slow batch completion; want true")
		}
	})

	t.Run("ConcurrentW8StandaloneCOW", func(t *testing.T) {
		const (
			writers      = 8
			opsPerWriter = 40
			docCount     = writers
		)
		dir := t.TempDir()
		s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALRelaxed, docCount)

		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			_ = s.Close()
			t.Fatal("expected cached coalescer")
		}

		ctx := context.Background()
		errCh := make(chan error, writers)
		finalSteps := make([]int64, writers)
		var stop atomic.Bool
		var minOpsWG sync.WaitGroup
		minOpsWG.Add(writers)
		var wg sync.WaitGroup
		wg.Add(writers)

		// Hold execMu during wave 1 so all 8 writers genuinely enter the coalescer
		// concurrently and engage batch-mode hysteresis through the public command
		// path, rather than letting a single early-scheduled goroutine loop inline
		// before other writers reach runMongoUpdateCoalesced under -race / low CPU.
		coalescer.execMu.Lock()
		for w := 0; w < writers; w++ {
			w := w
			go func() {
				defer wg.Done()
				reachedMin := false
				defer func() {
					if !reachedMin {
						minOpsWG.Done()
					}
				}()
				for step := int64(1); ; step++ {
					resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), step), nil, 0)
					if err != nil {
						errCh <- fmt.Errorf("writer %d step %d: %w", w, step, err)
						return
					}
					if !mongoCommandResponseOK(resp) {
						errCh <- fmt.Errorf("writer %d step %d not ok: %s", w, step, resp)
						return
					}
					finalSteps[w] = step
					if !reachedMin && step >= opsPerWriter {
						reachedMin = true
						minOpsWG.Done()
					}
					if reachedMin && stop.Load() {
						return
					}
				}
			}()
		}

		queueDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(queueDeadline) && len(coalescer.requests) < writers && len(errCh) == 0 {
			time.Sleep(time.Millisecond)
		}
		queued := len(coalescer.requests)
		coalescer.execMu.Unlock()
		coalescer.signalWake()

		// Snapshot hysteresis counters while all 8 writers are still concurrently
		// active (before stopping writers and entering <=2 straggler tail drain).
		minOpsWG.Wait()
		inlineCount := coalescer.inlineExecs.Load()
		batchCount := coalescer.workerBatches.Load()
		stop.Store(true)
		wg.Wait()
		close(errCh)
		for err := range errCh {
			_ = s.Close()
			t.Fatal(err)
		}
		if queued != writers {
			_ = s.Close()
			t.Fatalf("wave1 queued=%d want %d while execMu held", queued, writers)
		}

		if runtime.GOMAXPROCS(0) > 1 {
			if batchCount == 0 {
				_ = s.Close()
				t.Fatalf("w8 workerBatches=0 want > 0 (inlineExecs=%d)", inlineCount)
			}
			if inlineCount > writers {
				_ = s.Close()
				t.Fatalf("w8 inlineExecs=%d too high vs workerBatches=%d (expected hysteresis to prevent inline stealing)", inlineCount, batchCount)
			}
		}

		for w := 0; w < writers; w++ {
			if finalSteps[w] < opsPerWriter {
				_ = s.Close()
				t.Fatalf("doc[%d] finalSteps=%d want >= %d", w, finalSteps[w], opsPerWriter)
			}
			if got := findDocN(t, s.Server, collName, int64(w)); got != finalSteps[w] {
				_ = s.Close()
				t.Fatalf("doc[%d] n=%d want %d", w, got, finalSteps[w])
			}
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	t.Run("BatchBufClearedAndZeroMaxBatchSafe", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 4)
		defer func() { _ = s.Close() }()

		col, err := s.Collections.OpenCollection("app." + collName)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}

		makeReq := func(id, val int64, expired bool) mongoUpdateCoalescerRequest {
			item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: val}}}}},
			}))
			if err != nil {
				t.Fatalf("parseMongoUpdateItem: %v", err)
			}
			if expired {
				item.budget = newMongoWriteBudget(1)
				item.budget.deadline = time.Now().Add(-time.Second)
			}
			return mongoUpdateCoalescerRequest{
				col:  col,
				item: item,
				done: make(chan mongoUpdateCoalescerResult, 1),
			}
		}

		// Direct &mongoUpdateCoalescer{} without batchHysteresis keeps hysteresis disabled (0).
		c := &mongoUpdateCoalescer{
			maxBatch:  8,
			requests:  make(chan mongoUpdateCoalescerRequest, 8),
			stoppedCh: make(chan struct{}),
		}
		if got := c.effectiveBatchHysteresis(); got != 0 {
			t.Fatalf("zero-value effectiveBatchHysteresis=%v want 0", got)
		}

		req0 := makeReq(0, 11, false)
		req1 := makeReq(1, 22, true) // expired: compacted out inside runBatch
		req2 := makeReq(2, 33, false)
		c.requests <- req1
		c.requests <- req2

		c.runBatchStartingWith(req0)

		if res := <-req0.done; res.err != nil || !res.matched || !res.modified {
			t.Fatalf("req0 result=%+v want matched,modified,nil", res)
		}
		if res := <-req1.done; res.err == nil || !strings.Contains(res.err.Error(), "exceeded its execution-time budget") {
			t.Fatalf("req1 result=%+v want budget error", res)
		}
		if res := <-req2.done; res.err != nil || !res.matched || !res.modified {
			t.Fatalf("req2 result=%+v want matched,modified,nil", res)
		}
		if got := c.batchModeUntil.Load(); got != 0 {
			t.Fatalf("zero-hysteresis batchModeUntil=%d want 0", got)
		}
		if len(c.batchBuf) != 0 {
			t.Fatalf("len(batchBuf)=%d want 0", len(c.batchBuf))
		}
		if cap(c.batchBuf) != 8 {
			t.Fatalf("cap(batchBuf)=%d want 8", cap(c.batchBuf))
		}
		// Inspect every backing slot, including tail slots vacated by expired filtering.
		backing := c.batchBuf[:cap(c.batchBuf)]
		for i, slot := range backing {
			if slot.col != nil || slot.done != nil || slot.item.key != nil || slot.item.budget != nil || len(slot.item.updateDoc) != 0 {
				t.Fatalf("batchBuf backing slot[%d] not cleared: %+v", i, slot)
			}
		}
		firstBackingPtr := &backing[0]

		// Second batch reuses the exact same backing array without reallocating.
		req3 := makeReq(0, 44, false)
		req4 := makeReq(2, 55, false)
		c.requests <- req4
		c.runBatchStartingWith(req3)
		if res := <-req3.done; res.err != nil {
			t.Fatalf("req3 err=%v", res.err)
		}
		if res := <-req4.done; res.err != nil {
			t.Fatalf("req4 err=%v", res.err)
		}
		if &c.batchBuf[:1][0] != firstBackingPtr {
			t.Fatal("batchBuf backing array was reallocated instead of reused")
		}
		for i, slot := range c.batchBuf[:cap(c.batchBuf)] {
			if slot.col != nil || slot.done != nil || slot.item.key != nil || slot.item.budget != nil || len(slot.item.updateDoc) != 0 {
				t.Fatalf("batchBuf backing slot[%d] not cleared after reuse: %+v", i, slot)
			}
		}

		// maxBatch == 0 on direct &mongoUpdateCoalescer{} must not panic.
		cZero := &mongoUpdateCoalescer{
			maxBatch:  0,
			requests:  make(chan mongoUpdateCoalescerRequest, 1),
			stoppedCh: make(chan struct{}),
		}
		expiredOnly := makeReq(3, 99, true)
		cZero.runBatchStartingWith(expiredOnly)
		if res := <-expiredOnly.done; res.err == nil || !strings.Contains(res.err.Error(), "exceeded its execution-time budget") {
			t.Fatalf("maxBatch=0 result=%+v want budget error", res)
		}
		if cap(cZero.batchBuf) != 1 || len(cZero.batchBuf) != 0 {
			t.Fatalf("maxBatch=0 batchBuf len=%d cap=%d want 0,1", len(cZero.batchBuf), cap(cZero.batchBuf))
		}
		if slot := cZero.batchBuf[:1][0]; slot.col != nil || slot.done != nil || slot.item.budget != nil {
			t.Fatalf("maxBatch=0 batchBuf[0] not cleared: %+v", slot)
		}
	})
}

func BenchmarkMongoUpdateCoalescerW1W2W8StandaloneCOW(b *testing.B) {
	for _, writers := range []int{1, 2, 8} {
		writers := writers
		b.Run(fmt.Sprintf("w%d", writers), func(b *testing.B) {
			const docsPerWriter = 16
			docCount := writers * docsPerWriter
			if docCount < 64 {
				docCount = 64
			}
			s, collName := openUpdateCoalescerBenchServer(b, b.TempDir(), treedb.ProfileCommandWALRelaxed, docCount)
			defer func() { _ = s.Close() }()

			perWriterCmds := make([][]wire.Document, writers)
			for w := 0; w < writers; w++ {
				cmds := make([]wire.Document, docsPerWriter*2)
				base := w * docsPerWriter
				for i := 0; i < docsPerWriter; i++ {
					cmds[i] = buildUpdateOneCommand(b, collName, int64(base+i), 1)
					cmds[docsPerWriter+i] = buildUpdateOneCommand(b, collName, int64(base+i), 2)
				}
				perWriterCmds[w] = cmds
			}

			ctx := context.Background()
			resp, err := s.Server.commandResponse(ctx, "update", perWriterCmds[0][0], nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				b.Fatalf("warm update: resp=%s err=%v", resp, err)
			}
			coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
			if coalescer == nil {
				b.Fatal("expected cached coalescer")
			}

			inlineBefore := coalescer.inlineExecs.Load()
			batchesBefore := coalescer.workerBatches.Load()

			b.ReportAllocs()
			b.ResetTimer()
			if writers == 1 {
				cmds := perWriterCmds[0]
				for i := 0; i < b.N; i++ {
					r, err := s.Server.commandResponse(ctx, "update", cmds[i%len(cmds)], nil, 0)
					if err != nil {
						b.Fatalf("update[%d]: %v", i, err)
					}
					if !mongoCommandResponseOK(r) {
						b.Fatalf("update[%d] not ok: %s", i, r)
					}
					updateCoalescerBenchSink = r
				}
			} else {
				var nextOp atomic.Int64
				var firstErr atomic.Pointer[error]
				var wg sync.WaitGroup
				wg.Add(writers)
				for w := 0; w < writers; w++ {
					cmds := perWriterCmds[w]
					go func() {
						defer wg.Done()
						localIdx := 0
						for {
							idx := int(nextOp.Add(1) - 1)
							if idx >= b.N {
								return
							}
							cmd := cmds[localIdx%len(cmds)]
							localIdx++
							r, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
							if err != nil {
								e := fmt.Errorf("update[%d]: %w", idx, err)
								firstErr.CompareAndSwap(nil, &e)
								return
							}
							if !mongoCommandResponseOK(r) {
								e := fmt.Errorf("update[%d] not ok: %s", idx, r)
								firstErr.CompareAndSwap(nil, &e)
								return
							}
						}
					}()
				}
				wg.Wait()
				if errPtr := firstErr.Load(); errPtr != nil {
					b.Fatal(*errPtr)
				}
			}
			b.StopTimer()

			inlineDelta := coalescer.inlineExecs.Load() - inlineBefore
			batchesDelta := coalescer.workerBatches.Load() - batchesBefore
			b.ReportMetric(float64(inlineDelta)/float64(b.N), "inline/op")
			b.ReportMetric(float64(batchesDelta)/float64(b.N), "worker_batches/op")
		})
	}

	b.Run("w8_tcp", func(b *testing.B) {
		const (
			writers       = 8
			docsPerWriter = 16
			docCount      = writers * docsPerWriter
		)
		s, collName := openUpdateCoalescerBenchServer(b, b.TempDir(), treedb.ProfileCommandWALRelaxed, docCount)

		serveCtx, serveCancel := context.WithCancel(context.Background())
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			serveCancel()
			_ = s.Close()
			b.Fatalf("listen: %v", err)
		}
		serveErr := make(chan error, 1)
		go func() {
			serveErr <- s.Serve(serveCtx, ln)
		}()
		client, err := mongo.Connect(options.Client().
			ApplyURI("mongodb://" + ln.Addr().String()).
			SetDirect(true).
			SetMaxPoolSize(16).
			SetServerSelectionTimeout(2 * time.Second))
		if err != nil {
			serveCancel()
			_ = ln.Close()
			_ = s.Close()
			b.Fatalf("mongo connect: %v", err)
		}
		defer func() {
			_ = client.Disconnect(context.Background())
			serveCancel()
			_ = ln.Close()
			<-serveErr
			_ = s.Close()
		}()

		ctx := context.Background()
		if err := client.Ping(ctx, nil); err != nil {
			b.Fatalf("ping: %v", err)
		}
		coll := client.Database("app").Collection(collName)
		if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: int64(0)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(1)}}}}); err != nil {
			b.Fatalf("warm UpdateOne: %v", err)
		}
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			b.Fatal("expected cached coalescer")
		}

		type updatePair struct {
			filter bson.D
			update bson.D
		}
		perWriterOps := make([][]updatePair, writers)
		for w := 0; w < writers; w++ {
			ops := make([]updatePair, docsPerWriter*2)
			base := w * docsPerWriter
			for i := 0; i < docsPerWriter; i++ {
				id := int64(base + i)
				ops[i] = updatePair{
					filter: bson.D{{Key: "_id", Value: id}},
					update: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(1)}}}},
				}
				ops[docsPerWriter+i] = updatePair{
					filter: bson.D{{Key: "_id", Value: id}},
					update: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(2)}}}},
				}
			}
			perWriterOps[w] = ops
		}

		inlineBefore := coalescer.inlineExecs.Load()
		batchesBefore := coalescer.workerBatches.Load()

		var nextOp atomic.Int64
		var firstErr atomic.Pointer[error]
		b.ReportAllocs()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(writers)
		for w := 0; w < writers; w++ {
			ops := perWriterOps[w]
			go func() {
				defer wg.Done()
				localIdx := 0
				for {
					idx := int(nextOp.Add(1) - 1)
					if idx >= b.N {
						return
					}
					op := ops[localIdx%len(ops)]
					localIdx++
					res, err := coll.UpdateOne(ctx, op.filter, op.update)
					if err != nil {
						e := fmt.Errorf("UpdateOne[%d]: %w", idx, err)
						firstErr.CompareAndSwap(nil, &e)
						return
					}
					if res.MatchedCount != 1 {
						e := fmt.Errorf("UpdateOne[%d] matched=%d want 1", idx, res.MatchedCount)
						firstErr.CompareAndSwap(nil, &e)
						return
					}
				}
			}()
		}
		wg.Wait()
		b.StopTimer()
		if errPtr := firstErr.Load(); errPtr != nil {
			b.Fatal(*errPtr)
		}

		inlineDelta := coalescer.inlineExecs.Load() - inlineBefore
		batchesDelta := coalescer.workerBatches.Load() - batchesBefore
		b.ReportMetric(float64(inlineDelta)/float64(b.N), "inline/op")
		b.ReportMetric(float64(batchesDelta)/float64(b.N), "worker_batches/op")
	})
}
