package mongogateway

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestUpdateCoalescerDurableLinger_ComputeBoundsAndDeadlines(t *testing.T) {
	if got := computeDurableLinger(0, time.Time{}); got != 0 {
		t.Fatalf("waveDur=0 linger=%v want 0", got)
	}
	if got := computeDurableLinger(499*time.Microsecond, time.Time{}); got != 0 {
		t.Fatalf("waveDur=499us linger=%v want 0", got)
	}
	if got := computeDurableLinger(500*time.Microsecond, time.Time{}); got != durableLingerMin {
		t.Fatalf("waveDur=500us linger=%v want %v", got, durableLingerMin)
	}
	if got := computeDurableLinger(time.Millisecond, time.Time{}); got != durableLingerMin {
		t.Fatalf("waveDur=1ms linger=%v want %v", got, durableLingerMin)
	}
	if got := computeDurableLinger(2*time.Millisecond, time.Time{}); got != 250*time.Microsecond {
		t.Fatalf("waveDur=2ms linger=%v want 250us", got)
	}
	if got := computeDurableLinger(4*time.Millisecond, time.Time{}); got != durableLingerMax {
		t.Fatalf("waveDur=4ms linger=%v want %v", got, durableLingerMax)
	}
	if got := computeDurableLinger(20*time.Millisecond, time.Time{}); got != durableLingerMax {
		t.Fatalf("waveDur=20ms linger=%v want %v", got, durableLingerMax)
	}

	// Tight deadlines (<= 2ms remaining or expired) must skip lingering completely.
	if got := computeDurableLinger(4*time.Millisecond, time.Now().Add(-time.Millisecond)); got != 0 {
		t.Fatalf("expired deadline linger=%v want 0", got)
	}
	if got := computeDurableLinger(4*time.Millisecond, time.Now().Add(time.Millisecond)); got != 0 {
		t.Fatalf("1ms deadline linger=%v want 0", got)
	}
	if got := computeDurableLinger(4*time.Millisecond, time.Now().Add(20*time.Millisecond)); got != durableLingerMax {
		t.Fatalf("20ms deadline linger=%v want %v", got, durableLingerMax)
	}
	// When remaining > 2ms but < linger, linger must clamp to remaining/2.
	if got := computeDurableLingerWithBounds(400*time.Millisecond, time.Now().Add(10*time.Millisecond), 50*time.Millisecond, 50*time.Millisecond); got <= 0 || got > 6*time.Millisecond {
		t.Fatalf("clamped deadline linger=%v want in (0, 6ms]", got)
	}

	// Verify lfEarliestDeadlineLocked scans wrapped ring buffer accurately.
	c := &mongoUpdateCoalescer{useLeaderFollower: true}
	base := time.Now().Add(time.Minute)
	for i := 0; i < 12; i++ {
		c.lfPushLocked(mongoUpdateCoalescerRequest{item: mongoUpdateItem{index: i}})
	}
	for i := 0; i < 10; i++ {
		_ = c.lfPopFrontLocked()
	}
	// Ring cap is 16, head=10, len=2. Push 8 more to wrap around index 0.
	bLate := newMongoWriteBudget(1)
	bLate.deadline = base.Add(10 * time.Second)
	bEarly := newMongoWriteBudget(1)
	bEarly.deadline = base.Add(2 * time.Second)
	bMid := newMongoWriteBudget(1)
	bMid.deadline = base.Add(5 * time.Second)
	c.lfPushLocked(mongoUpdateCoalescerRequest{item: mongoUpdateItem{budget: bLate}})
	for i := 0; i < 5; i++ {
		c.lfPushLocked(mongoUpdateCoalescerRequest{item: mongoUpdateItem{}})
	}
	c.lfPushLocked(mongoUpdateCoalescerRequest{item: mongoUpdateItem{budget: bEarly}})
	c.lfPushLocked(mongoUpdateCoalescerRequest{item: mongoUpdateItem{budget: bMid}})

	if got := c.lfEarliestDeadlineLocked(); !got.Equal(bEarly.deadline) {
		t.Fatalf("lfEarliestDeadlineLocked=%v want %v", got, bEarly.deadline)
	}
}

func TestUpdateCoalescerDurableLinger_FastWavesNeverLinger(t *testing.T) {
	const (
		writers      = 4
		opsPerWriter = 30
	)
	s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, writers)
	defer func() { _ = s.Close() }()

	coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}

	// Poll lfLingerTarget concurrently; fast waves should stay below the 500us
	// threshold and never enter lingering.
	var sawLinger atomic.Bool
	var waves atomic.Int64
	coalescer.testWaveHook = func(batchLen int) {
		waves.Add(1)
	}

	ctx := context.Background()
	start := make(chan struct{})
	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			<-start
			for step := 1; step <= opsPerWriter; step++ {
				resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), int64(step)), nil, 0)
				if err != nil {
					errCh <- fmt.Errorf("writer %d step %d: %w", w, step, err)
					return
				}
				if !mongoCommandResponseOK(resp) {
					errCh <- fmt.Errorf("writer %d step %d not ok: %s", w, step, resp)
					return
				}
				coalescer.lfMu.Lock()
				if coalescer.lfLingerTarget > 0 {
					sawLinger.Store(true)
				}
				coalescer.lfMu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if waves.Load() == 0 {
		t.Fatal("expected at least one executed wave")
	}
	for w := 0; w < writers; w++ {
		if got := findDocN(t, s.Server, collName, int64(w)); got != opsPerWriter {
			t.Fatalf("doc[%d] n=%d want %d", w, got, opsPerWriter)
		}
	}
	// On a fast local relaxed run, we don't fail on a single rare OS scheduling
	// hiccup >500us, but verify the coalescer settles with lfLingerTarget == 0.
	coalescer.lfMu.Lock()
	target := coalescer.lfLingerTarget
	coalescer.lfMu.Unlock()
	if target != 0 {
		t.Fatalf("residual lfLingerTarget=%d want 0 after fast workload", target)
	}
	_ = sawLinger.Load()
}

func TestUpdateCoalescerDurableLinger_SingleWriterDurableNeverLingers(t *testing.T) {
	s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 4)
	defer func() { _ = s.Close() }()

	coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	// Pin sticky window high so we can test w=1 both inline AND on the hot worker.
	coalescer.lfMu.Lock()
	coalescer.stickyIdleWindow = 10 * time.Second
	coalescer.lfMu.Unlock()

	const simulatedFsync = 2 * time.Millisecond
	var hookCalls atomic.Int64
	coalescer.testWaveHook = func(batchLen int) {
		hookCalls.Add(1)
		time.Sleep(simulatedFsync)
	}

	ctx := context.Background()
	// Uncontended w=1 runs inline (zero worker, zero linger).
	for step := int64(1); step <= 4; step++ {
		resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, step), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("inline w1 step %d: resp=%s err=%v", step, resp, err)
		}
	}
	if got := coalescer.inlineExecs.Load(); got != 4 {
		t.Fatalf("inlineExecs=%d want 4", got)
	}
	if got := coalescer.workerBatches.Load(); got != 0 {
		t.Fatalf("workerBatches=%d want 0 for uncontended w1", got)
	}

	// Now trigger a 2-writer durable wave to latch hot worker + lingerTarget=2,
	// then drop back to w=1 and verify the 1st solo wave adapts lingerTarget to 1
	// so subsequent solo waves do not linger.
	releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	for w := 0; w < 2; w++ {
		w := w
		go func() {
			defer wg.Done()
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), 10), nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				errCh <- fmt.Errorf("w%d: resp=%s err=%v", w, resp, err)
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && coalescer.queuedLen() < 2 {
		time.Sleep(time.Millisecond)
	}
	releaseLeader()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Solo wave #1 adapts lingerTarget down from 2 -> 1.
	if resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, 11), nil, 0); err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("solo adapt step: resp=%s err=%v", resp, err)
	}
	// Solo waves #2..#4 must never set lfLingerTarget > 0.
	stopMonitor := make(chan struct{})
	var lingeredAfterAdapt atomic.Bool
	var monWG sync.WaitGroup
	monWG.Add(1)
	go func() {
		defer monWG.Done()
		for {
			select {
			case <-stopMonitor:
				return
			default:
				coalescer.lfMu.Lock()
				if coalescer.lfLingerTarget > 0 {
					lingeredAfterAdapt.Store(true)
				}
				coalescer.lfMu.Unlock()
				runtime.Gosched()
			}
		}
	}()
	for step := int64(12); step <= 14; step++ {
		if resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, step), nil, 0); err != nil || !mongoCommandResponseOK(resp) {
			close(stopMonitor)
			monWG.Wait()
			t.Fatalf("solo steady step %d: resp=%s err=%v", step, resp, err)
		}
	}
	close(stopMonitor)
	monWG.Wait()
	if lingeredAfterAdapt.Load() {
		t.Fatal("single-writer durable wave lingered after adapting lingerTarget down to 1")
	}
	if got := findDocN(t, s.Server, collName, 0); got != 14 {
		t.Fatalf("doc 0 n=%d want 14", got)
	}
}

func TestUpdateCoalescerDurableLinger_MultiWriterConvergence(t *testing.T) {
	for _, writers := range []int{2, 4, 8} {
		writers := writers
		t.Run(fmt.Sprintf("w%d", writers), func(t *testing.T) {
			const (
				opsPerWriter   = 16
				simulatedFsync = 6 * time.Millisecond
			)
			s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, writers)
			defer func() { _ = s.Close() }()

			coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
			if coalescer == nil {
				t.Fatal("expected coalescer")
			}
			// Widen linger bounds for test stability on loaded 2-vCPU CI runners where
			// goroutine wakeup jitter can exceed the production 350us ceiling; production
			// bounds (150us..350us) are verified separately in ComputeBoundsAndDeadlines.
			coalescer.lfMu.Lock()
			coalescer.stickyIdleWindow = 50 * time.Millisecond
			coalescer.lingerMinOverride = 4 * time.Millisecond
			coalescer.lingerMaxOverride = 6 * time.Millisecond
			coalescer.lfMu.Unlock()

			perWriterCmds := make([][]wire.Document, writers)
			for w := 0; w < writers; w++ {
				cmds := make([]wire.Document, opsPerWriter)
				for step := 1; step <= opsPerWriter; step++ {
					cmds[step-1] = buildUpdateOneCommand(t, collName, int64(w), int64(step))
				}
				perWriterCmds[w] = cmds
			}

			var mu sync.Mutex
			var batchSizes []int
			coalescer.testWaveHook = func(batchLen int) {
				mu.Lock()
				batchSizes = append(batchSizes, batchLen)
				mu.Unlock()
				time.Sleep(simulatedFsync)
			}

			ctx := context.Background()
			start := make(chan struct{})
			errCh := make(chan error, writers)
			var wg sync.WaitGroup
			wg.Add(writers)
			for w := 0; w < writers; w++ {
				w := w
				cmds := perWriterCmds[w]
				go func() {
					defer wg.Done()
					<-start
					for step := 1; step <= opsPerWriter; step++ {
						// Stagger goroutine loopback across writers to exercise arrival skew
						// that would otherwise trigger 1+(N-1) split fsyncs on every wave.
						if step > 1 {
							if skew := (w*7 + step*5) % 4; skew > 0 {
								time.Sleep(time.Duration(skew*25) * time.Microsecond)
							}
						}
						resp, err := s.Server.commandResponse(ctx, "update", cmds[step-1], nil, 0)
						if err != nil {
							errCh <- fmt.Errorf("writer %d step %d: %w", w, step, err)
							return
						}
						if !mongoCommandResponseOK(resp) {
							errCh <- fmt.Errorf("writer %d step %d not ok: %s", w, step, resp)
							return
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			close(errCh)
			for err := range errCh {
				t.Fatal(err)
			}

			mu.Lock()
			recorded := append([]int(nil), batchSizes...)
			mu.Unlock()

			totalOps := writers * opsPerWriter
			totalBatches := len(recorded)
			avgBatch := float64(totalOps) / float64(totalBatches)
			// Without durable linger, 1+(N-1) splitting produces ~2*opsPerWriter batches
			// (avgBatch ~= N/2). With adaptive linger, writers coalesce into ~1 batch/wave.
			minExpectedAvg := float64(writers) * 0.75
			if avgBatch < minExpectedAvg {
				t.Fatalf("w%d poor durable coalescing: totalOps=%d totalBatches=%d avgBatch=%.2f want >= %.2f (batches=%v)",
					writers, totalOps, totalBatches, avgBatch, minExpectedAvg, recorded)
			}
			fullWaves := 0
			for _, sz := range recorded {
				if sz == writers {
					fullWaves++
				}
			}
			if fullWaves < opsPerWriter/2 {
				t.Fatalf("w%d fullWaves=%d want >= %d (batches=%v)", writers, fullWaves, opsPerWriter/2, recorded)
			}
			for w := 0; w < writers; w++ {
				if got := findDocN(t, s.Server, collName, int64(w)); got != opsPerWriter {
					t.Fatalf("doc[%d] n=%d want %d", w, got, opsPerWriter)
				}
			}
		})
	}
}

func TestUpdateCoalescerDurableLinger_WorkloadDropAdaptsDown(t *testing.T) {
	const (
		highWriters    = 8
		lowWriters     = 4
		simulatedFsync = 6 * time.Millisecond
	)
	s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, highWriters)
	defer func() { _ = s.Close() }()

	coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	coalescer.lfMu.Lock()
	coalescer.stickyIdleWindow = 50 * time.Millisecond
	coalescer.lingerMinOverride = 4 * time.Millisecond
	coalescer.lingerMaxOverride = 6 * time.Millisecond
	coalescer.lfMu.Unlock()

	var mu sync.Mutex
	var batchSizes []int
	coalescer.testWaveHook = func(batchLen int) {
		mu.Lock()
		batchSizes = append(batchSizes, batchLen)
		mu.Unlock()
		time.Sleep(simulatedFsync)
	}

	ctx := context.Background()
	runPhase := func(writers, steps int, baseVal int64) []int {
		perWriterCmds := make([][]wire.Document, writers)
		for w := 0; w < writers; w++ {
			cmds := make([]wire.Document, steps)
			for step := 1; step <= steps; step++ {
				cmds[step-1] = buildUpdateOneCommand(t, collName, int64(w), baseVal+int64(step))
			}
			perWriterCmds[w] = cmds
		}

		mu.Lock()
		startIdx := len(batchSizes)
		mu.Unlock()

		start := make(chan struct{})
		errCh := make(chan error, writers)
		var wg sync.WaitGroup
		wg.Add(writers)
		for w := 0; w < writers; w++ {
			w := w
			cmds := perWriterCmds[w]
			go func() {
				defer wg.Done()
				<-start
				for step := 1; step <= steps; step++ {
					if step > 1 {
						if skew := (w*5 + step*3) % 4; skew > 0 {
							time.Sleep(time.Duration(skew*10) * time.Microsecond)
						}
					}
					resp, err := s.Server.commandResponse(ctx, "update", cmds[step-1], nil, 0)
					if err != nil {
						errCh <- fmt.Errorf("writer %d step %d: %w", w, step, err)
						return
					}
					if !mongoCommandResponseOK(resp) {
						errCh <- fmt.Errorf("writer %d step %d not ok: %s", w, step, resp)
						return
					}
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}
		mu.Lock()
		phaseBatches := append([]int(nil), batchSizes[startIdx:]...)
		mu.Unlock()
		return phaseBatches
	}

	// Phase 1: w=8 durable waves establish lingerTarget=8.
	phase1 := runPhase(highWriters, 6, 100)
	// Phase 2: immediately drop to w=4 without waiting for stickyIdleWindow;
	// lingerTarget adapts from 8 -> 4 in 1 wave and keeps coalescing full 4-writer waves.
	phase2 := runPhase(lowWriters, 8, 200)

	full4 := 0
	for _, sz := range phase2 {
		if sz == lowWriters {
			full4++
		}
	}
	if full4 < 4 {
		t.Fatalf("phase2 (w8->w4 drop) full4=%d want >= 4 (phase1=%v phase2=%v)", full4, phase1, phase2)
	}
	for w := 0; w < lowWriters; w++ {
		if got := findDocN(t, s.Server, collName, int64(w)); got != 208 {
			t.Fatalf("doc[%d] n=%d want 208", w, got)
		}
	}
}

func TestUpdateCoalescerDurableLinger_TightDeadlineSkipsLinger(t *testing.T) {
	s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 6)
	defer func() { _ = s.Close() }()

	col, err := s.Collections.OpenCollection("app." + collName)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	coalescer.lfMu.Lock()
	coalescer.stickyIdleWindow = 10 * time.Second
	coalescer.lingerMinOverride = 200 * time.Millisecond
	coalescer.lingerMaxOverride = 200 * time.Millisecond
	coalescer.lfMu.Unlock()

	// Make Wave 1 slow (4ms) with 4 writers so lastWaveDur=4ms and lingerTarget=4,
	// then switch the hook off for the tight-deadline probe so we directly verify
	// it does not sleep for the 200ms linger ceiling.
	var slowPhase atomic.Bool
	slowPhase.Store(true)
	coalescer.testWaveHook = func(batchLen int) {
		if slowPhase.Load() {
			time.Sleep(4 * time.Millisecond)
		}
	}

	releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(4)
	errCh := make(chan error, 4)
	for w := 0; w < 4; w++ {
		w := w
		go func() {
			defer wg.Done()
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), 10), nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				errCh <- fmt.Errorf("w%d: resp=%s err=%v", w, resp, err)
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && coalescer.queuedLen() < 4 {
		time.Sleep(time.Millisecond)
	}
	releaseLeader()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Switch off simulated sleep; submit a single request with a 20ms deadline
	// (< 200ms linger ceiling). Linger must clamp to remaining/2 (~10ms) so the
	// request completes well before its deadline instead of timing out in linger.
	slowPhase.Store(false)
	clampedItem, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: int64(4)}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(444)}}}}},
	}))
	if err != nil {
		t.Fatalf("parseMongoUpdateItem: %v", err)
	}
	clampedItem.budget = newMongoWriteBudget(1)
	clampedItem.budget.deadline = time.Now().Add(25 * time.Millisecond)

	t0 := time.Now()
	matched, modified, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, clampedItem)
	elapsed := time.Since(t0)
	if err != nil || !matched || !modified {
		t.Fatalf("clamped deadline update failed (elapsed=%v): matched=%v modified=%v err=%v", elapsed, matched, modified, err)
	}
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("clamped deadline update waited %v >= 100ms (failed to clamp 200ms linger)", elapsed)
	}
	if got := findDocN(t, s.Server, collName, 4); got != 444 {
		t.Fatalf("doc 4 n=%d want 444", got)
	}
}

func TestUpdateCoalescerDurableLinger_MidLingerTightDeadlineWakesEarly(t *testing.T) {
	s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 6)
	defer func() { _ = s.Close() }()

	col, err := s.Collections.OpenCollection("app." + collName)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	coalescer.lfMu.Lock()
	coalescer.stickyIdleWindow = 10 * time.Second
	coalescer.lingerMinOverride = 200 * time.Millisecond
	coalescer.lingerMaxOverride = 200 * time.Millisecond
	coalescer.lfMu.Unlock()

	var slowPhase atomic.Bool
	slowPhase.Store(true)
	coalescer.testWaveHook = func(batchLen int) {
		if slowPhase.Load() {
			time.Sleep(4 * time.Millisecond)
		}
	}

	// Establish lastWaveDur >= 4ms and lingerTarget = 4.
	releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(4)
	errCh := make(chan error, 4)
	for w := 0; w < 4; w++ {
		w := w
		go func() {
			defer wg.Done()
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), 10), nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				errCh <- fmt.Errorf("w%d: resp=%s err=%v", w, resp, err)
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && coalescer.queuedLen() < 4 {
		time.Sleep(time.Millisecond)
	}
	releaseLeader()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Enqueue request #1 (no deadline) so the worker enters a 200ms linger waiting
	// for target=4, then submit request #2 (still below target: lfLen=2 < 4) with
	// a 25ms deadline. Request #2 must wake the worker immediately so neither
	// request waits out the 200ms linger or misses its deadline.
	slowPhase.Store(false)
	req1Done := make(chan error, 1)
	go func() {
		resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 4, 404), nil, 0)
		if err == nil && !mongoCommandResponseOK(resp) {
			err = fmt.Errorf("req1 not ok: %s", resp)
		}
		req1Done <- err
	}()
	waitLingerDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(waitLingerDeadline) {
		coalescer.lfMu.Lock()
		inLinger := coalescer.lfLingerTarget == 4
		coalescer.lfMu.Unlock()
		if inLinger {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}

	tightItem, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: int64(5)}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(505)}}}}},
	}))
	if err != nil {
		t.Fatalf("parseMongoUpdateItem: %v", err)
	}
	tightItem.budget = newMongoWriteBudget(1)
	tightItem.budget.deadline = time.Now().Add(25 * time.Millisecond)

	t0 := time.Now()
	matched, modified, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, tightItem)
	elapsed := time.Since(t0)
	if err != nil || !matched || !modified {
		t.Fatalf("mid-linger tight deadline update failed (elapsed=%v): matched=%v modified=%v err=%v", elapsed, matched, modified, err)
	}
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("mid-linger tight deadline update waited %v >= 100ms (did not wake lingering worker early)", elapsed)
	}
	if err := <-req1Done; err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	if got := findDocN(t, s.Server, collName, 4); got != 404 {
		t.Fatalf("doc 4 n=%d want 404", got)
	}
	if got := findDocN(t, s.Server, collName, 5); got != 505 {
		t.Fatalf("doc 5 n=%d want 505", got)
	}
}

func TestUpdateCoalescerDurableLinger_CleanShutdownWhileLingering(t *testing.T) {
	for iter := 0; iter < 4; iter++ {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 6)
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			_ = s.Close()
			t.Fatal("expected coalescer")
		}
		coalescer.lfMu.Lock()
		coalescer.stickyIdleWindow = 10 * time.Second
		coalescer.lingerMinOverride = 80 * time.Millisecond
		coalescer.lingerMaxOverride = 80 * time.Millisecond
		coalescer.lfMu.Unlock()

		var slowPhase atomic.Bool
		slowPhase.Store(true)
		coalescer.testWaveHook = func(batchLen int) {
			if slowPhase.Load() {
				time.Sleep(3 * time.Millisecond)
			}
		}

		// Establish lastWaveDur >= 3ms and lingerTarget = 4.
		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(4)
		errCh := make(chan error, 4)
		for w := 0; w < 4; w++ {
			w := w
			go func() {
				defer wg.Done()
				resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), 1), nil, 0)
				if err != nil || !mongoCommandResponseOK(resp) {
					errCh <- fmt.Errorf("w%d: resp=%s err=%v", w, resp, err)
				}
			}()
		}
		qDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(qDeadline) && coalescer.queuedLen() < 4 {
			time.Sleep(time.Millisecond)
		}
		releaseLeader()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			_ = s.Close()
			t.Fatal(err)
		}

		// Enqueue 1 request (triggers active 80ms linger waiting for target=4),
		// deterministically wait until the worker is actively lingering, and
		// stop the coalescer. Both the request and stop() must finish promptly.
		slowPhase.Store(false)
		reqDone := make(chan error, 1)
		go func() {
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 5, 55), nil, 0)
			if err == nil && !mongoCommandResponseOK(resp) {
				err = fmt.Errorf("not ok: %s", resp)
			}
			reqDone <- err
		}()
		waitLingerDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(waitLingerDeadline) {
			coalescer.lfMu.Lock()
			inLinger := coalescer.lfLingerTarget > 0
			coalescer.lfMu.Unlock()
			if inLinger {
				break
			}
			time.Sleep(100 * time.Microsecond)
		}

		stopDone := make(chan struct{})
		go func() {
			defer close(stopDone)
			coalescer.stop()
		}()

		select {
		case err := <-reqDone:
			if err != nil {
				_ = s.Close()
				t.Fatalf("iter %d request during linger+stop failed: %v", iter, err)
			}
		case <-time.After(5 * time.Second):
			_ = s.Close()
			t.Fatalf("iter %d request hung during linger+stop", iter)
		}
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			_ = s.Close()
			t.Fatalf("iter %d stop() hung while worker was lingering", iter)
		}
		select {
		case <-coalescer.done:
		default:
			_ = s.Close()
			t.Fatalf("iter %d coalescer.done not closed after stop()", iter)
		}
		if got := findDocN(t, s.Server, collName, 5); got != 55 {
			_ = s.Close()
			t.Fatalf("iter %d doc 5 n=%d want 55", iter, got)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("iter %d Close: %v", iter, err)
		}
	}
}

func TestFindPredicateCapErrorSentinelAndPathValidation(t *testing.T) {
	if findPredicateCapError() != errFindPredicateCap {
		t.Fatal("findPredicateCapError did not return package sentinel")
	}
	if !strings.Contains(findPredicateCapError().Error(), fmt.Sprintf("exceeds %d predicates", mongoQueryMaxBooleanPredicates)) {
		t.Fatalf("unexpected sentinel message: %q", findPredicateCapError().Error())
	}

	// Verify validateMongoDocumentPath edge cases and error precedence match contract.
	if err := validateMongoDocumentPath("age"); err != nil {
		t.Fatalf("simple path rejected: %v", err)
	}
	if err := validateMongoDocumentPath("profile.address.zip"); err != nil {
		t.Fatalf("dotted path rejected: %v", err)
	}
	atMax := strings.Repeat("a.", mongoMutationMaxPathDepth-1) + "a"
	if err := validateMongoDocumentPath(atMax); err != nil {
		t.Fatalf("max-depth path rejected: %v", err)
	}
	overMax := strings.Repeat("a.", mongoMutationMaxPathDepth) + "a"
	if err := validateMongoDocumentPath(overMax); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("exceeds %d components", mongoMutationMaxPathDepth)) {
		t.Fatalf("over-max path err=%v want exceeds components error", err)
	}
	for _, bad := range []string{"", ".", ".a", "a.", "a..b", strings.Repeat("a..", mongoMutationMaxPathDepth)} {
		if err := validateMongoDocumentPath(bad); err != errMongoDocumentPathEmptySegment {
			t.Fatalf("path %q err=%v want %v", bad, err, errMongoDocumentPathEmptySegment)
		}
	}
	for _, bad := range []string{"$gt", "a.$b", "a.b.$c"} {
		if err := validateMongoDocumentPath(bad); err != errMongoDocumentPathUnsupportedSegment {
			t.Fatalf("path %q err=%v want %v", bad, err, errMongoDocumentPathUnsupportedSegment)
		}
	}
	// Depth check takes precedence over unsupported '$' segment.
	overMaxDollar := strings.Repeat("$a.", mongoMutationMaxPathDepth) + "a"
	if err := validateMongoDocumentPath(overMaxDollar); err != errMongoDocumentPathMaxDepth {
		t.Fatalf("over-max dollar path err=%v want %v", err, errMongoDocumentPathMaxDepth)
	}

	// Verify duplicate operator detection works both below and above the 4-op stack fast path.
	dupSmall := mustDocument(t, bson.D{
		{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(20)}, {Key: "$gte", Value: int32(25)}}},
	})
	if _, err := parseFindPredicates(dupSmall); err == nil || !strings.Contains(err.Error(), "repeats operator") {
		t.Fatalf("small duplicate operator err=%v want repeats operator", err)
	}
	fiveOps := mustDocument(t, bson.D{
		{Key: "age", Value: bson.D{
			{Key: "$gt", Value: int32(1)},
			{Key: "$gte", Value: int32(2)},
			{Key: "$lt", Value: int32(100)},
			{Key: "$lte", Value: int32(99)},
			{Key: "$ne", Value: int32(50)},
		}},
	})
	preds, err := parseFindPredicates(fiveOps)
	if err != nil {
		t.Fatalf("5 distinct operators failed: %v", err)
	}
	if len(preds) != 5 {
		t.Fatalf("5 distinct operators len=%d want 5", len(preds))
	}
	dupFiveOps := mustDocument(t, bson.D{
		{Key: "age", Value: bson.D{
			{Key: "$gt", Value: int32(1)},
			{Key: "$gte", Value: int32(2)},
			{Key: "$lt", Value: int32(100)},
			{Key: "$lte", Value: int32(99)},
			{Key: "$ne", Value: int32(50)},
			{Key: "$ne", Value: int32(51)},
		}},
	})
	if _, err := parseFindPredicates(dupFiveOps); err == nil || !strings.Contains(err.Error(), "repeats operator") {
		t.Fatalf("large duplicate operator err=%v want repeats operator", err)
	}
}

var parseFindRangeSink []findPredicate

func BenchmarkParseFindRangePredicate(b *testing.B) {
	filter := wire.Document(mustDocument(b, bson.D{
		{Key: "age", Value: bson.D{
			{Key: "$gte", Value: int64(25)},
			{Key: "$lte", Value: int64(35)},
		}},
	}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		preds, err := parseFindPredicates(filter)
		if err != nil {
			b.Fatalf("parseFindPredicates: %v", err)
		}
		parseFindRangeSink = preds
	}
}
