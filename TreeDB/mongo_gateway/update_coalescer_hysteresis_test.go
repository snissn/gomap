package mongogateway

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"slices"
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

func TestMongoUpdateCoalescerLeaderFollower(t *testing.T) {
	t.Run("W1DirectImmediateLeaderZeroWorker", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 8)
		defer func() { _ = s.Close() }()

		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}
		if !coalescer.useLeaderFollower {
			t.Fatal("expected useLeaderFollower=true when UpdateCoalescingMaxDelay == 0")
		}
		if coalescer.requests != nil {
			t.Fatal("expected nil requests channel in leader-follower mode")
		}

		ctx := context.Background()
		for i := 0; i < 8; i++ {
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(i), int64(10+i)), nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				t.Fatalf("w1 update[%d]: resp=%s err=%v", i, resp, err)
			}
		}
		if got := coalescer.inlineExecs.Load(); got != 8 {
			t.Fatalf("w1 inlineExecs=%d want 8", got)
		}
		if got := coalescer.workerBatches.Load(); got != 0 {
			t.Fatalf("w1 workerBatches=%d want 0", got)
		}
		if coalescer.hasLeaderWork() {
			t.Fatal("leader work still active after w1 updates completed")
		}
		coalescer.lfMu.Lock()
		queueAllocated := coalescer.lfQueue != nil
		workerStarted := coalescer.lfWorkerStarted
		coalescer.lfMu.Unlock()
		if queueAllocated {
			t.Fatal("uncontended w1 should not allocate follower queue")
		}
		if workerStarted {
			t.Fatal("uncontended w1 should not spawn hot worker goroutine")
		}
		for i := 0; i < 8; i++ {
			if got := findDocN(t, s.Server, collName, int64(i)); got != int64(10+i) {
				t.Fatalf("doc[%d] n=%d want %d", i, got, 10+i)
			}
		}
	})

	t.Run("DeadlineExpiryPromotedLeaderAndFollowers", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 8)
		defer func() { _ = s.Close() }()

		col, err := s.Collections.OpenCollection("app." + collName)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}

		makeItem := func(id, val int64, budget *mongoWriteBudget) mongoUpdateItem {
			item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: val}}}}},
			}))
			if err != nil {
				t.Fatalf("parseMongoUpdateItem: %v", err)
			}
			item.budget = budget
			return item
		}

		// Case A: Promoted leader expires while waiting in queue, while two followers remain valid.
		// Promoted leader must return its deadline error, still execute valid followers, and release token.
		releaseA := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseA()

		expiredLeaderBudget := newMongoWriteBudget(1)
		expiredLeaderBudget.deadline = time.Now().Add(time.Hour)
		item0 := makeItem(0, 100, expiredLeaderBudget)
		item1 := makeItem(1, 101, newMongoWriteBudget(1))
		item2 := makeItem(2, 102, newMongoWriteBudget(1))

		resCh0 := make(chan mongoUpdateCoalescerResult, 1)
		resCh1 := make(chan mongoUpdateCoalescerResult, 1)
		resCh2 := make(chan mongoUpdateCoalescerResult, 1)

		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, item0)
			resCh0 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && coalescer.queuedLen() < 1 {
			time.Sleep(time.Millisecond)
		}
		if coalescer.queuedLen() != 1 {
			t.Fatalf("queued=%d want 1 for item0", coalescer.queuedLen())
		}

		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, item1)
			resCh1 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, item2)
			resCh2 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		for time.Now().Before(deadline) && coalescer.queuedLen() < 3 {
			time.Sleep(time.Millisecond)
		}
		if coalescer.queuedLen() != 3 {
			t.Fatalf("queued=%d want 3", coalescer.queuedLen())
		}

		// Expire item0's budget while queued before leadership handoff.
		expiredLeaderBudget.deadline = time.Now().Add(-time.Second)
		releaseA()

		r0 := <-resCh0
		r1 := <-resCh1
		r2 := <-resCh2
		if r0.err == nil || !strings.Contains(r0.err.Error(), "exceeded its execution-time budget") {
			t.Fatalf("expired promoted leader res=%+v want budget error", r0)
		}
		if r1.err != nil || !r1.matched || !r1.modified {
			t.Fatalf("follower1 res=%+v want matched,modified,nil", r1)
		}
		if r2.err != nil || !r2.matched || !r2.modified {
			t.Fatalf("follower2 res=%+v want matched,modified,nil", r2)
		}
		if got := findDocN(t, s.Server, collName, 0); got != 0 {
			t.Fatalf("expired doc 0 mutated: n=%d want 0", got)
		}
		if got := findDocN(t, s.Server, collName, 1); got != 101 {
			t.Fatalf("doc 1 n=%d want 101", got)
		}
		if got := findDocN(t, s.Server, collName, 2); got != 102 {
			t.Fatalf("doc 2 n=%d want 102", got)
		}

		// Case B: Single promoted leader with no followers whose deadline expires while queued.
		releaseB := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseB()

		soloBudget := newMongoWriteBudget(1)
		soloBudget.deadline = time.Now().Add(time.Hour)
		soloItem := makeItem(3, 303, soloBudget)
		soloCh := make(chan mongoUpdateCoalescerResult, 1)
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, soloItem)
			soloCh <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		soloQueueDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(soloQueueDeadline) && coalescer.queuedLen() < 1 {
			time.Sleep(time.Millisecond)
		}
		if got := coalescer.queuedLen(); got != 1 {
			t.Fatalf("solo queued=%d want 1 before expiring deadline", got)
		}
		soloBudget.deadline = time.Now().Add(-time.Second)
		releaseB()
		soloRes := <-soloCh
		if soloRes.err == nil || !strings.Contains(soloRes.err.Error(), "exceeded its execution-time budget") {
			t.Fatalf("solo expired promoted leader res=%+v want budget error", soloRes)
		}
		idleDeadline := time.Now().Add(time.Second)
		for time.Now().Before(idleDeadline) && coalescer.hasLeaderWork() {
			time.Sleep(time.Millisecond)
		}
		if coalescer.hasLeaderWork() {
			t.Fatal("hot worker remained busy after expired solo request")
		}

		// Subsequent update must succeed immediately.
		resp, err := s.Server.commandResponse(context.Background(), "update", buildUpdateOneCommand(t, collName, 3, 304), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("post-expiry update: resp=%s err=%v", resp, err)
		}
		if got := findDocN(t, s.Server, collName, 3); got != 304 {
			t.Fatalf("doc 3 n=%d want 304", got)
		}
	})

	t.Run("StopAndCloseDuringActiveHandoff", func(t *testing.T) {
		const (
			maxBatch = 3
			writers  = 8
		)
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, writers+1)
		defer func() { _ = s.Close() }()

		s.Server.UpdateCoalescingMaxBatch = maxBatch
		s.Server.UpdateCoalescingMaxDelay = 0
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}

		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()

		ctx := context.Background()
		errCh := make(chan error, writers)
		var wg sync.WaitGroup
		wg.Add(writers)
		for w := 0; w < writers; w++ {
			w := w
			go func() {
				defer wg.Done()
				resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), int64(500+w)), nil, 0)
				if err != nil {
					errCh <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
				if !mongoCommandResponseOK(resp) {
					errCh <- fmt.Errorf("writer %d not ok: %s", w, resp)
				}
			}()
		}

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && coalescer.queuedLen() < writers {
			time.Sleep(time.Millisecond)
		}
		if got := coalescer.queuedLen(); got != writers {
			t.Fatalf("queued=%d want %d", got, writers)
		}

		// Start stop() while leader is active and multiple handoff waves are queued.
		stopDone := make(chan struct{})
		go func() {
			defer close(stopDone)
			coalescer.stop()
		}()

		// Wait until stop() has marked lfStopped; done MUST remain open while leader is held.
		for time.Now().Before(deadline) && !coalescer.isStopped() {
			time.Sleep(time.Millisecond)
		}
		if !coalescer.isStopped() {
			t.Fatal("stop() did not mark coalescer stopped")
		}
		select {
		case <-stopDone:
			t.Fatal("stop() returned while leader was still active")
		case <-coalescer.done:
			t.Fatal("coalescer.done closed while leader was still active")
		case <-time.After(15 * time.Millisecond):
		}

		// Releasing the leader must drain all queued followers across multi-batch handoffs
		// and only then close coalescer.done so stop() returns.
		releaseLeader()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			t.Fatal("stop() did not finish after leader drained queued followers")
		}
		select {
		case <-coalescer.done:
		default:
			t.Fatal("coalescer.done not closed after stop()")
		}
		for w := 0; w < writers; w++ {
			if got := findDocN(t, s.Server, collName, int64(w)); got != int64(500+w) {
				t.Fatalf("doc[%d] n=%d want %d", w, got, 500+w)
			}
		}

		// Subsequent update after coalescer stopped must fall back cleanly to runMongoUpdateOne.
		col, err := s.Collections.OpenCollection("app." + collName)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		fallbackItem, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: int64(writers)}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(999)}}}}},
		}))
		if err != nil {
			t.Fatalf("parse fallback update: %v", err)
		}
		matched, modified, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, fallbackItem)
		if err != nil || !matched || !modified {
			t.Fatalf("fallback after stop: matched=%v modified=%v err=%v", matched, modified, err)
		}
		if got := findDocN(t, s.Server, collName, int64(writers)); got != 999 {
			t.Fatalf("fallback doc n=%d want 999", got)
		}
	})

	t.Run("IdleTimerAndStopRace", func(t *testing.T) {
		for iter := 0; iter < 12; iter++ {
			server := NewServer()
			server.UpdateCoalescingIdleTTL = time.Millisecond
			coalescer := server.mongoUpdateCoalescer("app.users")
			if coalescer == nil {
				t.Fatal("expected coalescer")
			}
			// Let the 1ms AfterFunc fire right around concurrent stop()/Close().
			time.Sleep(time.Duration(iter%3) * time.Millisecond)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				coalescer.stop()
			}()
			go func() {
				defer wg.Done()
				_ = server.Close()
			}()
			wg.Wait()
			if !coalescer.isStopped() {
				t.Fatalf("iter %d: coalescer not stopped", iter)
			}
			select {
			case <-coalescer.done:
			default:
				t.Fatalf("iter %d: coalescer.done not closed", iter)
			}
		}
	})

	t.Run("PanicRecoveryPreservesLeaderToken", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 6)
		defer func() { _ = s.Close() }()

		col, err := s.Collections.OpenCollection("app." + collName)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}

		// Panic inside hot worker's runBatch hook must recover cleanly, abort all waiting
		// requests in that batch (never hang or crash the worker), clear batchBuf, and
		// keep serving subsequent waves cleanly.
		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()

		panicBudget := newMongoWriteBudget(1)
		panicBudget.beforeNativeUpdateBatchHook = func() {
			panic("injected batch panic")
		}
		item0, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: int64(0)}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(11)}}}}},
		}))
		if err != nil {
			t.Fatalf("parse item0: %v", err)
		}
		item0.budget = panicBudget
		item1, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: int64(1)}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(22)}}}}},
		}))
		if err != nil {
			t.Fatalf("parse item1: %v", err)
		}

		resCh0 := make(chan mongoUpdateCoalescerResult, 1)
		resCh1 := make(chan mongoUpdateCoalescerResult, 1)
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, item0)
			resCh0 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && coalescer.queuedLen() < 1 {
			time.Sleep(time.Millisecond)
		}
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, item1)
			resCh1 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		for time.Now().Before(deadline) && coalescer.queuedLen() < 2 {
			time.Sleep(time.Millisecond)
		}
		releaseLeader()

		for idx, ch := range []chan mongoUpdateCoalescerResult{resCh0, resCh1} {
			select {
			case res := <-ch:
				if res.err == nil || !strings.Contains(res.err.Error(), "batch aborted") {
					t.Fatalf("caller[%d] result=%+v want batch aborted error", idx, res)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("caller[%d] hung after hot worker batch panicked", idx)
			}
		}

		idleDeadline := time.Now().Add(time.Second)
		for time.Now().Before(idleDeadline) && coalescer.hasLeaderWork() {
			time.Sleep(time.Millisecond)
		}
		if coalescer.hasLeaderWork() {
			t.Fatal("hot worker remained busy after recovering from batch panic")
		}

		// Subsequent update must execute cleanly without wedging.
		resp, err := s.Server.commandResponse(context.Background(), "update", buildUpdateOneCommand(t, collName, 2, 222), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("post-panic update: resp=%s err=%v", resp, err)
		}
		if got := findDocN(t, s.Server, collName, 2); got != 222 {
			t.Fatalf("doc 2 n=%d want 222", got)
		}
	})

	t.Run("PanicRecoveryNeverDoubleCompletesExpiredOrSequentialRequests", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 8)
		defer func() { _ = s.Close() }()

		col, err := s.Collections.OpenCollection("app." + collName)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}

		makeItem := func(id, val int64, budget *mongoWriteBudget) mongoUpdateItem {
			item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: val}}}}},
			}))
			if err != nil {
				t.Fatalf("parseMongoUpdateItem: %v", err)
			}
			item.budget = budget
			return item
		}

		// Case 1: [Live0(panic hook), Live1, Expired2] and [Live0(panic hook), Expired1, Live2]
		// Using cap-2 channels ensures any duplicate send from panic fan-out is caught immediately.
		for _, tc := range []struct {
			name        string
			expiredSlot int
		}{
			{name: "TrailingExpired", expiredSlot: 2},
			{name: "MiddleExpiredCompacted", expiredSlot: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := &mongoUpdateCoalescer{
					maxBatch:          8,
					useLeaderFollower: true,
					lfWakeCh:          make(chan struct{}, 1),
					stoppedCh:         make(chan struct{}),
					done:              make(chan struct{}),
				}
				panicBudget := newMongoWriteBudget(1)
				panicBudget.beforeNativeUpdateBatchHook = func() {
					panic("injected batch hook panic")
				}
				expiredBudget := newMongoWriteBudget(1)
				expiredBudget.deadline = time.Now().Add(-time.Second)

				done0 := make(chan mongoUpdateCoalescerResult, 2)
				done1 := make(chan mongoUpdateCoalescerResult, 2)
				done2 := make(chan mongoUpdateCoalescerResult, 2)
				var batch []mongoUpdateCoalescerRequest
				if tc.expiredSlot == 2 {
					batch = []mongoUpdateCoalescerRequest{
						{col: col, item: makeItem(0, 10, panicBudget), done: done0},
						{col: col, item: makeItem(1, 11, newMongoWriteBudget(1)), done: done1},
						{col: col, item: makeItem(2, 12, expiredBudget), done: done2},
					}
				} else {
					batch = []mongoUpdateCoalescerRequest{
						{col: col, item: makeItem(0, 10, panicBudget), done: done0},
						{col: col, item: makeItem(1, 11, expiredBudget), done: done1},
						{col: col, item: makeItem(2, 12, newMongoWriteBudget(1)), done: done2},
					}
				}
				c.runHotWorkerBatch(batch)

				chans := []chan mongoUpdateCoalescerResult{done0, done1, done2}
				for i, ch := range chans {
					if got := len(ch); got != 1 {
						t.Fatalf("slot[%d] received %d results want exactly 1", i, got)
					}
					res := <-ch
					if i == tc.expiredSlot {
						if res.err == nil || !strings.Contains(res.err.Error(), "exceeded its execution-time budget") {
							t.Fatalf("expired slot[%d] res=%+v want budget error", i, res)
						}
					} else {
						if res.err == nil || !strings.Contains(res.err.Error(), "batch aborted") {
							t.Fatalf("live slot[%d] res=%+v want batch aborted error", i, res)
						}
					}
				}
			})
		}

		// Case 2: End-to-end pooled-channel regression with [Live0(panic), Live1, Expired2]
		// through runMongoUpdateCoalesced: recycled channels in mongoUpdateResultChanPool
		// must never be poisoned by a late abort send.
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}
		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()

		e2ePanicBudget := newMongoWriteBudget(1)
		e2ePanicBudget.beforeNativeUpdateBatchHook = func() {
			panic("injected e2e batch panic")
		}
		e2eExpBudget := newMongoWriteBudget(1)
		e2eExpBudget.deadline = time.Now().Add(time.Hour)

		e2eRes0 := make(chan mongoUpdateCoalescerResult, 1)
		e2eRes1 := make(chan mongoUpdateCoalescerResult, 1)
		e2eRes2 := make(chan mongoUpdateCoalescerResult, 1)
		waitQueued := func(want int) {
			t.Helper()
			qDeadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(qDeadline) && coalescer.queuedLen() < want {
				time.Sleep(time.Millisecond)
			}
			if got := coalescer.queuedLen(); got != want {
				t.Fatalf("e2e queued=%d want %d before advancing", got, want)
			}
		}
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, makeItem(0, 20, e2ePanicBudget))
			e2eRes0 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		waitQueued(1)
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, makeItem(1, 21, newMongoWriteBudget(1)))
			e2eRes1 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		waitQueued(2)
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, makeItem(2, 22, e2eExpBudget))
			e2eRes2 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		waitQueued(3)
		e2eExpBudget.deadline = time.Now().Add(-time.Second)
		releaseLeader()

		if r0 := <-e2eRes0; r0.err == nil || !strings.Contains(r0.err.Error(), "batch aborted") {
			t.Fatalf("e2e live0 res=%+v want batch aborted error", r0)
		}
		if r1 := <-e2eRes1; r1.err == nil || !strings.Contains(r1.err.Error(), "batch aborted") {
			t.Fatalf("e2e live1 res=%+v want batch aborted error", r1)
		}
		if r2 := <-e2eRes2; r2.err == nil || !strings.Contains(r2.err.Error(), "exceeded its execution-time budget") {
			t.Fatalf("e2e expired2 res=%+v want budget error", r2)
		}
		for i := 0; i < 64; i++ {
			ch := getMongoUpdateResultChan()
			select {
			case leaked := <-ch:
				t.Fatalf("pooled channel %d poisoned with stale result: %+v", i, leaked)
			default:
			}
			putMongoUpdateResultChan(ch)
		}

		// Case 3: Sequential fallback where item 0 completes and item 1 panics.
		// Item 0 must receive only its original success result (no second abort send),
		// while items 1 and 2 each receive exactly one abort error.
		cSeq := &mongoUpdateCoalescer{
			maxBatch:          8,
			useLeaderFollower: true,
			lfWakeCh:          make(chan struct{}, 1),
			stoppedCh:         make(chan struct{}),
			done:              make(chan struct{}),
		}
		seqPanicItem, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "tag", Value: "tag-04"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(444)}}}}},
		}))
		if err != nil {
			t.Fatalf("parse sequential filter update: %v", err)
		}
		seqPanicItem.selector = s.Server
		seqPanicItem.budget = newMongoWriteBudget(64)
		s.Server.filterWriteSelectedHook = func() {
			panic("injected sequential panic")
		}
		defer func() {
			s.Server.filterWriteSelectedHook = nil
		}()

		seqDone0 := make(chan mongoUpdateCoalescerResult, 2)
		seqDone1 := make(chan mongoUpdateCoalescerResult, 2)
		seqDone2 := make(chan mongoUpdateCoalescerResult, 2)
		seqBatch := []mongoUpdateCoalescerRequest{
			{col: col, item: makeItem(3, 333, newMongoWriteBudget(1)), done: seqDone0},
			{col: col, item: seqPanicItem, done: seqDone1},
			{col: col, item: makeItem(5, 555, newMongoWriteBudget(1)), done: seqDone2},
		}
		cSeq.runHotWorkerBatch(seqBatch)
		s.Server.filterWriteSelectedHook = nil

		if got := len(seqDone0); got != 1 {
			t.Fatalf("sequential completed item0 received %d results want exactly 1", got)
		}
		if r0 := <-seqDone0; r0.err != nil || !r0.matched || !r0.modified {
			t.Fatalf("sequential item0 res=%+v want matched,modified,nil", r0)
		}
		if got := len(seqDone1); got != 1 {
			t.Fatalf("sequential panicking item1 received %d results want exactly 1", got)
		}
		if r1 := <-seqDone1; r1.err == nil || !strings.Contains(r1.err.Error(), "batch aborted") {
			t.Fatalf("sequential item1 res=%+v want batch aborted error", r1)
		}
		if got := len(seqDone2); got != 1 {
			t.Fatalf("sequential trailing item2 received %d results want exactly 1", got)
		}
		if r2 := <-seqDone2; r2.err == nil || !strings.Contains(r2.err.Error(), "batch aborted") {
			t.Fatalf("sequential item2 res=%+v want batch aborted error", r2)
		}
		if got := findDocN(t, s.Server, collName, 3); got != 333 {
			t.Fatalf("doc 3 n=%d want 333", got)
		}

		// Subsequent update through the live coalescer must succeed cleanly.
		resp, err := s.Server.commandResponse(context.Background(), "update", buildUpdateOneCommand(t, collName, 6, 666), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("post-recovery update: resp=%s err=%v", resp, err)
		}
		if got := findDocN(t, s.Server, collName, 6); got != 666 {
			t.Fatalf("doc 6 n=%d want 666", got)
		}
	})

	t.Run("SoloStreakClearsStickyMode", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 4)
		defer func() { _ = s.Close() }()

		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}
		// Disable timer-based sticky expiry during this test so only soloStreak
		// can clear lfSticky (eliminating false passes under CI jitter).
		coalescer.lfMu.Lock()
		coalescer.stickyIdleWindow = 10 * time.Second
		coalescer.lfMu.Unlock()

		// Trigger a 2-writer coalesced batch to latch lfSticky = true.
		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		errCh := make(chan error, 2)
		for w := 0; w < 2; w++ {
			w := w
			go func() {
				defer wg.Done()
				resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), 1), nil, 0)
				if err != nil || !mongoCommandResponseOK(resp) {
					errCh <- fmt.Errorf("w%d: resp=%s err=%v", w, resp, err)
				}
			}()
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && coalescer.queuedLen() < 2 {
			time.Sleep(time.Millisecond)
		}
		if got := coalescer.queuedLen(); got != 2 {
			t.Fatalf("queued=%d want 2", got)
		}
		releaseLeader()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}

		coalescer.lfMu.Lock()
		enteredSticky := coalescer.lfSticky
		coalescer.lfMu.Unlock()
		if !enteredSticky {
			t.Fatal("expected lfSticky=true immediately after 2-writer coalesced batch")
		}

		// First solo update stays pinned on hot worker (inlineExecs remains 0).
		resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, 2), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("first solo update: resp=%s err=%v", resp, err)
		}
		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("first solo update inlineExecs=%d want 0 while sticky", got)
		}

		// Issue remaining solo updates through defaultUpdateCoalescingStickySoloLimit:
		// soloStreak must clear lfSticky well before the 10s stickyIdleWindow and
		// resume inline execution cleanly.
		for step := int64(3); step <= int64(2+defaultUpdateCoalescingStickySoloLimit+4); step++ {
			resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, step), nil, 0)
			if err != nil || !mongoCommandResponseOK(resp) {
				t.Fatalf("solo step %d: resp=%s err=%v", step, resp, err)
			}
		}
		coalescer.lfMu.Lock()
		stillSticky := coalescer.lfSticky
		coalescer.lfMu.Unlock()
		if stillSticky {
			t.Fatal("lfSticky remained true after soloStreak exceeded limit")
		}
		if got := coalescer.inlineExecs.Load(); got == 0 {
			t.Fatalf("expected soloStreak to clear sticky mode and resume inline execution; inlineExecs=0 workerBatches=%d", coalescer.workerBatches.Load())
		}
	})

	t.Run("MaxQueueCapBackpressureAndDeadline", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 8)
		defer func() { _ = s.Close() }()

		col, err := s.Collections.OpenCollection("app." + collName)
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected cached coalescer")
		}
		coalescer.lfMu.Lock()
		coalescer.maxQueueCap = 2
		coalescer.lfMu.Unlock()

		makeItem := func(id, val int64, budget *mongoWriteBudget) mongoUpdateItem {
			item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: val}}}}},
			}))
			if err != nil {
				t.Fatalf("parseMongoUpdateItem: %v", err)
			}
			item.budget = budget
			return item
		}

		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()

		res0 := make(chan mongoUpdateCoalescerResult, 1)
		res1 := make(chan mongoUpdateCoalescerResult, 1)
		resLive := make(chan mongoUpdateCoalescerResult, 1)
		resExpired := make(chan mongoUpdateCoalescerResult, 1)

		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, makeItem(0, 100, newMongoWriteBudget(1)))
			res0 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, makeItem(1, 101, newMongoWriteBudget(1)))
			res1 <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		qDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(qDeadline) && coalescer.queuedLen() < 2 {
			time.Sleep(time.Millisecond)
		}
		if got := coalescer.queuedLen(); got != 2 {
			t.Fatalf("queued=%d want 2 (full maxQueueCap)", got)
		}

		// Queue is now full (len == maxQueueCap == 2). Start a live waiter (must block
		// outside lfMu without exceeding cap) and a short-deadline waiter (must fail
		// with execution-time budget error while waiting).
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, makeItem(2, 102, newMongoWriteBudget(1)))
			resLive <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()
		shortBudget := newMongoWriteBudget(1)
		shortBudget.deadline = time.Now().Add(25 * time.Millisecond)
		go func() {
			m, mod, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, makeItem(3, 103, shortBudget))
			resExpired <- mongoUpdateCoalescerResult{matched: m, modified: mod, err: err}
		}()

		select {
		case rExp := <-resExpired:
			if rExp.err == nil || !strings.Contains(rExp.err.Error(), "exceeded its execution-time budget") {
				t.Fatalf("expired backpressure waiter res=%+v want budget error", rExp)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("short-deadline waiter blocked past deadline on full queue")
		}
		if got := coalescer.queuedLen(); got != 2 {
			t.Fatalf("queued=%d exceeded maxQueueCap=2 while leader held", got)
		}
		select {
		case r := <-resLive:
			t.Fatalf("live waiter bypassed full queue early: %+v", r)
		default:
		}

		// Releasing leader drains queue, wakes live waiter, and completes all 3 live updates.
		releaseLeader()
		for idx, ch := range []chan mongoUpdateCoalescerResult{res0, res1, resLive} {
			select {
			case r := <-ch:
				if r.err != nil || !r.matched || !r.modified {
					t.Fatalf("live[%d] res=%+v want matched,modified,nil", idx, r)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("live[%d] did not complete after drain", idx)
			}
		}
		if got := findDocN(t, s.Server, collName, 2); got != 102 {
			t.Fatalf("doc 2 n=%d want 102", got)
		}
		if got := findDocN(t, s.Server, collName, 3); got != 0 {
			t.Fatalf("expired doc 3 mutated: n=%d want 0", got)
		}
	})

	t.Run("RetireIdleSkipsWhenLeaderActive", func(t *testing.T) {
		server := NewServer()
		defer func() { _ = server.Close() }()
		coalescer := server.mongoUpdateCoalescer("app.users")
		if coalescer == nil {
			t.Fatal("expected coalescer")
		}

		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()
		// Even with inFlight == 0, retireIdle must atomically observe lfInlineRunning
		// under lfMu, refuse to stop, and keep the coalescer registered in updateCoalescers.
		if coalescer.retireIdle() {
			t.Fatal("retireIdle retired coalescer while lfInlineRunning was true")
		}
		if coalescer.isStopped() {
			t.Fatal("coalescer marked stopped while lfInlineRunning was true")
		}
		server.updateMu.RLock()
		stillCached := server.updateCoalescers["app.users"] == coalescer
		server.updateMu.RUnlock()
		if !stillCached {
			t.Fatal("retireIdle uncached active coalescer from server.updateCoalescers")
		}
		releaseLeader()
		if !coalescer.retireIdle() {
			t.Fatal("retireIdle failed after leader became idle")
		}
		if !coalescer.isStopped() {
			t.Fatal("coalescer not stopped after idle retirement")
		}
	})

	t.Run("RingBufferWrapAroundAndShrink", func(t *testing.T) {
		c := &mongoUpdateCoalescer{useLeaderFollower: true}
		mkReq := func(id int) mongoUpdateCoalescerRequest {
			return mongoUpdateCoalescerRequest{
				item: mongoUpdateItem{index: id},
				done: make(chan mongoUpdateCoalescerResult, 1),
			}
		}

		// Push 12, pop 10 -> lfHead = 10, lfLen = 2 inside cap 16.
		for i := 0; i < 12; i++ {
			c.lfPushLocked(mkReq(i))
		}
		for i := 0; i < 10; i++ {
			got := c.lfPopFrontLocked()
			if got.item.index != i {
				t.Fatalf("pop[%d]=%d want %d", i, got.item.index, i)
			}
		}
		if c.lfHead != 10 || c.lfLen != 2 {
			t.Fatalf("after pop head=%d len=%d want 10,2", c.lfHead, c.lfLen)
		}

		// Push 20 more -> wraps around 16 and forces growth to 32 preserving strict FIFO order.
		for i := 12; i < 32; i++ {
			c.lfPushLocked(mkReq(i))
		}
		if c.lfLen != 22 || len(c.lfQueue) != 32 {
			t.Fatalf("after wrap growth len=%d cap=%d want 22,32", c.lfLen, len(c.lfQueue))
		}

		// Advance head near end of 32-slot ring, then drain across wrap boundary.
		for i := 10; i < 30; i++ {
			got := c.lfPopFrontLocked()
			if got.item.index != i {
				t.Fatalf("fifo pop[%d]=%d want %d", i, got.item.index, i)
			}
		}
		// Remaining: 30, 31 at indices 20, 21. Push 14 more (32..45) -> wraps around index 0!
		for i := 32; i < 46; i++ {
			c.lfPushLocked(mkReq(i))
		}
		var drained []mongoUpdateCoalescerRequest
		drained = c.lfDrainIntoLocked(drained, 14)
		if len(drained) != 14 {
			t.Fatalf("drained len=%d want 14", len(drained))
		}
		for idx, req := range drained {
			if req.item.index != 30+idx {
				t.Fatalf("drained[%d]=%d want %d", idx, req.item.index, 30+idx)
			}
		}
		if c.lfLen != 2 {
			t.Fatalf("remaining lfLen=%d want 2", c.lfLen)
		}
		rest := c.lfDrainIntoLocked(nil, 10)
		if len(rest) != 2 || rest[0].item.index != 44 || rest[1].item.index != 45 {
			t.Fatalf("rest=%+v want indices 44,45", rest)
		}
		if c.lfLen != 0 || c.lfHead != 0 {
			t.Fatalf("empty queue head=%d len=%d want 0,0", c.lfHead, c.lfLen)
		}
		for i, slot := range c.lfQueue {
			if slot.done != nil || slot.col != nil {
				t.Fatalf("lfQueue[%d] retained pointer after drain: %+v", i, slot)
			}
		}

		// Burst above maxRetainedMongoUpdateLFQueueCap and verify shrink back to nil on empty.
		burst := maxRetainedMongoUpdateLFQueueCap + 32
		for i := 0; i < burst; i++ {
			c.lfPushLocked(mkReq(i))
		}
		if len(c.lfQueue) <= maxRetainedMongoUpdateLFQueueCap {
			t.Fatalf("burst queue cap=%d want > %d", len(c.lfQueue), maxRetainedMongoUpdateLFQueueCap)
		}
		_ = c.lfDrainIntoLocked(nil, burst)
		if c.lfQueue != nil || c.lfLen != 0 || c.lfHead != 0 {
			t.Fatalf("post-burst queue not shrunk: queue=%v len=%d head=%d", c.lfQueue != nil, c.lfLen, c.lfHead)
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

		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()
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
		for time.Now().Before(queueDeadline) && coalescer.queuedLen() < writers && len(errCh) == 0 {
			time.Sleep(time.Millisecond)
		}
		queued := coalescer.queuedLen()
		releaseLeader()

		minOpsWG.Wait()
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
			t.Fatalf("wave1 queued=%d want %d while leader held", queued, writers)
		}

		if runtime.GOMAXPROCS(0) > 1 && batchCount == 0 {
			_ = s.Close()
			t.Fatalf("w8 workerBatches=0 want > 0")
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

	t.Run("ConcurrentStressW1W2W4W8W16W32Reopen", func(t *testing.T) {
		for _, writers := range []int{1, 2, 4, 8, 16, 32} {
			writers := writers
			t.Run(fmt.Sprintf("w%d", writers), func(t *testing.T) {
				const opsPerWriter = 16
				dir := t.TempDir()
				s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALRelaxed, writers)

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
								errCh <- fmt.Errorf("w%d writer %d step %d: %w", writers, w, step, err)
								return
							}
							if !mongoCommandResponseOK(resp) {
								errCh <- fmt.Errorf("w%d writer %d step %d not ok: %s", writers, w, step, resp)
								return
							}
						}
					}()
				}
				close(start)
				wg.Wait()
				close(errCh)
				for err := range errCh {
					_ = s.Close()
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}

				reopened, err := OpenStandaloneServer(StandaloneOptions{
					Dir:     dir,
					Profile: treedb.ProfileCommandWALRelaxed,
				})
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				defer func() { _ = reopened.Close() }()
				for w := 0; w < writers; w++ {
					if got := findDocN(t, reopened.Server, collName, int64(w)); got != opsPerWriter {
						t.Fatalf("reopen doc[%d] n=%d want %d", w, got, opsPerWriter)
					}
				}
			})
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

		// Worker mode runBatchStartingWith buffer clearing and reuse.
		c := &mongoUpdateCoalescer{
			maxBatch:  8,
			requests:  make(chan mongoUpdateCoalescerRequest, 8),
			stoppedCh: make(chan struct{}),
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
		if len(c.batchBuf) != 0 {
			t.Fatalf("len(batchBuf)=%d want 0", len(c.batchBuf))
		}
		if cap(c.batchBuf) != 8 {
			t.Fatalf("cap(batchBuf)=%d want 8", cap(c.batchBuf))
		}
		backing := c.batchBuf[:cap(c.batchBuf)]
		for i, slot := range backing {
			if slot.col != nil || slot.done != nil || slot.item.key != nil || slot.item.budget != nil || len(slot.item.updateDoc) != 0 {
				t.Fatalf("batchBuf backing slot[%d] not cleared: %+v", i, slot)
			}
		}
		firstBackingPtr := &backing[0]

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

		// Hot worker mode runHotWorkerBatch buffer clearing and reuse.
		cLF := &mongoUpdateCoalescer{
			maxBatch:          8,
			useLeaderFollower: true,
			lfWakeCh:          make(chan struct{}, 1),
			stoppedCh:         make(chan struct{}),
			done:              make(chan struct{}),
		}
		lfReq0 := makeReq(0, 66, false)
		lfReq1 := makeReq(1, 77, true)
		lfReq2 := makeReq(2, 88, false)
		cLF.batchBuf = make([]mongoUpdateCoalescerRequest, 0, 8)
		cLF.lfPushLocked(lfReq0)
		cLF.lfPushLocked(lfReq1)
		cLF.lfPushLocked(lfReq2)
		batch1 := cLF.lfDrainIntoLocked(cLF.batchBuf[:0], 8)
		cLF.runHotWorkerBatch(batch1)
		if res := <-lfReq0.done; res.err != nil || !res.matched || !res.modified {
			t.Fatalf("lfReq0 result=%+v want matched,modified,nil", res)
		}
		if res := <-lfReq1.done; res.err == nil || !strings.Contains(res.err.Error(), "exceeded its execution-time budget") {
			t.Fatalf("lfReq1 result=%+v want budget error", res)
		}
		if res := <-lfReq2.done; res.err != nil || !res.matched || !res.modified {
			t.Fatalf("lfReq2 result=%+v want matched,modified,nil", res)
		}
		if len(cLF.batchBuf) != 0 || cap(cLF.batchBuf) != 8 {
			t.Fatalf("cLF batchBuf len=%d cap=%d want 0,8", len(cLF.batchBuf), cap(cLF.batchBuf))
		}
		lfBackingPtr := &cLF.batchBuf[:cap(cLF.batchBuf)][0]
		for i, slot := range cLF.batchBuf[:cap(cLF.batchBuf)] {
			if slot.col != nil || slot.done != nil || slot.item.key != nil || slot.item.budget != nil || len(slot.item.updateDoc) != 0 {
				t.Fatalf("cLF batchBuf slot[%d] not cleared: %+v", i, slot)
			}
		}
		lfReq3 := makeReq(0, 99, false)
		lfReq4 := makeReq(2, 100, false)
		cLF.lfPushLocked(lfReq3)
		cLF.lfPushLocked(lfReq4)
		batch2 := cLF.lfDrainIntoLocked(cLF.batchBuf[:0], 8)
		cLF.runHotWorkerBatch(batch2)
		if res := <-lfReq3.done; res.err != nil || !res.matched || !res.modified {
			t.Fatalf("lfReq3 result=%+v want matched,modified,nil", res)
		}
		if res := <-lfReq4.done; res.err != nil || !res.matched || !res.modified {
			t.Fatalf("lfReq4 result=%+v want matched,modified,nil", res)
		}
		if &cLF.batchBuf[:1][0] != lfBackingPtr {
			t.Fatal("cLF batchBuf backing array was reallocated instead of reused")
		}

		// maxBatch == 0 on direct &mongoUpdateCoalescer{} must not panic in either mode.
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

		cLFZero := &mongoUpdateCoalescer{
			maxBatch:          0,
			useLeaderFollower: true,
			lfWakeCh:          make(chan struct{}, 1),
			stoppedCh:         make(chan struct{}),
			done:              make(chan struct{}),
		}
		expiredLF := makeReq(3, 101, true)
		cLFZero.runHotWorkerSolo(expiredLF)
		if res := <-expiredLF.done; res.err == nil || !strings.Contains(res.err.Error(), "exceeded its execution-time budget") {
			t.Fatalf("cLFZero maxBatch=0 result=%+v want budget error", res)
		}
		if cLFZero.hasLeaderWork() {
			t.Fatal("cLFZero leaked leader work")
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

func BenchmarkMongoUpdateCoalescerScalingStandaloneCOW(b *testing.B) {
	runInProcess := func(b *testing.B, writers int) {
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
		if resp, err := s.Server.commandResponse(ctx, "update", perWriterCmds[0][0], nil, 0); err != nil || !mongoCommandResponseOK(resp) {
			b.Fatalf("warm update: resp=%s err=%v", resp, err)
		}
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			b.Fatal("expected cached coalescer")
		}

		inlineBefore := coalescer.inlineExecs.Load()
		batchesBefore := coalescer.workerBatches.Load()
		perWriterLat := make([][]int64, writers)
		for w := range perWriterLat {
			perWriterLat[w] = make([]int64, 0, b.N/writers+64)
		}

		var nextOp atomic.Int64
		var firstErr atomic.Pointer[error]
		b.ReportAllocs()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(writers)
		for w := 0; w < writers; w++ {
			w := w
			cmds := perWriterCmds[w]
			go func() {
				defer wg.Done()
				lats := perWriterLat[w]
				localIdx := 0
				for {
					idx := int(nextOp.Add(1) - 1)
					if idx >= b.N {
						perWriterLat[w] = lats
						return
					}
					cmd := cmds[localIdx%len(cmds)]
					localIdx++
					t0 := time.Now()
					r, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
					lats = append(lats, time.Since(t0).Nanoseconds())
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
					raw := bson.Raw(r)
					if n, ok := raw.Lookup("n").Int32OK(); !ok || n != 1 {
						e := fmt.Errorf("update[%d] n=%d ok=%v want 1: %s", idx, n, ok, r)
						firstErr.CompareAndSwap(nil, &e)
						return
					}
					if !raw.Lookup("writeErrors").IsZero() {
						e := fmt.Errorf("update[%d] writeErrors present: %s", idx, r)
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

		all := make([]int64, 0, b.N)
		for _, lats := range perWriterLat {
			all = append(all, lats...)
		}
		slices.Sort(all)
		if len(all) > 0 {
			b.ReportMetric(float64(all[len(all)/2])/1e3, "p50_us")
			b.ReportMetric(float64(all[(len(all)*99)/100])/1e3, "p99_us")
		}
		inlineDelta := coalescer.inlineExecs.Load() - inlineBefore
		batchesDelta := coalescer.workerBatches.Load() - batchesBefore
		b.ReportMetric(float64(inlineDelta)/float64(b.N), "inline/op")
		b.ReportMetric(float64(batchesDelta)/float64(b.N), "worker_batches/op")
	}

	runTCP := func(b *testing.B, writers int) {
		const docsPerWriter = 16
		docCount := writers * docsPerWriter
		if docCount < 64 {
			docCount = 64
		}
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
		poolSize := uint64(max(16, writers*2))
		client, err := mongo.Connect(options.Client().
			ApplyURI("mongodb://" + ln.Addr().String()).
			SetDirect(true).
			SetMaxPoolSize(poolSize).
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
		perWriterLat := make([][]int64, writers)
		for w := range perWriterLat {
			perWriterLat[w] = make([]int64, 0, b.N/writers+64)
		}

		var nextOp atomic.Int64
		var firstErr atomic.Pointer[error]
		b.ReportAllocs()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(writers)
		for w := 0; w < writers; w++ {
			w := w
			ops := perWriterOps[w]
			go func() {
				defer wg.Done()
				lats := perWriterLat[w]
				localIdx := 0
				for {
					idx := int(nextOp.Add(1) - 1)
					if idx >= b.N {
						perWriterLat[w] = lats
						return
					}
					op := ops[localIdx%len(ops)]
					localIdx++
					t0 := time.Now()
					res, err := coll.UpdateOne(ctx, op.filter, op.update)
					lats = append(lats, time.Since(t0).Nanoseconds())
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

		all := make([]int64, 0, b.N)
		for _, lats := range perWriterLat {
			all = append(all, lats...)
		}
		slices.Sort(all)
		if len(all) > 0 {
			b.ReportMetric(float64(all[len(all)/2])/1e3, "p50_us")
			b.ReportMetric(float64(all[(len(all)*99)/100])/1e3, "p99_us")
		}
		inlineDelta := coalescer.inlineExecs.Load() - inlineBefore
		batchesDelta := coalescer.workerBatches.Load() - batchesBefore
		b.ReportMetric(float64(inlineDelta)/float64(b.N), "inline/op")
		b.ReportMetric(float64(batchesDelta)/float64(b.N), "worker_batches/op")
	}

	for _, writers := range []int{1, 2, 4, 8, 16, 32} {
		writers := writers
		b.Run(fmt.Sprintf("w%d", writers), func(b *testing.B) {
			runInProcess(b, writers)
		})
	}
	for _, writers := range []int{1, 2, 4, 8, 16} {
		writers := writers
		b.Run(fmt.Sprintf("w%d_tcp", writers), func(b *testing.B) {
			runTCP(b, writers)
		})
	}
}
