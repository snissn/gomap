package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func openUpdateCoalescerBenchServer(tb testing.TB, dir string, profile treedb.Profile, docCount int) (*StandaloneServer, string) {
	tb.Helper()
	s, err := OpenStandaloneServer(StandaloneOptions{
		Dir:     dir,
		Profile: profile,
	})
	if err != nil {
		tb.Fatalf("OpenStandaloneServer: %v", err)
	}
	if !s.Backend.HasNativeSemanticBridge() || s.Stats()["treedb.cache.memtable_mode"] != "cow_btree" {
		_ = s.Close()
		tb.Fatalf("expected standalone cow_btree native semantic bridge, mode=%q", s.Stats()["treedb.cache.memtable_mode"])
	}

	const dbName = "app"
	const collName = "items"
	ns := dbName + "." + collName
	_, err = s.Collections.CreateCollection(&collections.CollectionMeta{
		Name:    ns,
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
		Indexes: []collections.IndexDefinition{
			{
				Name:      "n_1",
				Field:     "n",
				ValueType: collections.IndexValueInt64,
			},
			{
				Name:      "tag_1",
				Field:     "tag",
				ValueType: collections.IndexValueString,
			},
		},
	})
	if err != nil {
		_ = s.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}

	ctx := context.Background()
	batch := make(bson.A, 0, 128)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		cmd := mustDocument(tb, bson.D{
			{Key: "insert", Value: collName},
			{Key: "$db", Value: dbName},
			{Key: "documents", Value: batch},
		})
		resp, err := s.Server.insertResponse(ctx, cmd, nil)
		if err != nil {
			_ = s.Close()
			tb.Fatalf("insertResponse: %v", err)
		}
		if !mongoCommandResponseOK(resp) {
			_ = s.Close()
			tb.Fatalf("insertResponse not ok: %s", resp)
		}
		batch = batch[:0]
	}
	for i := 0; i < docCount; i++ {
		batch = append(batch, bson.D{
			{Key: "_id", Value: int64(i)},
			{Key: "n", Value: int64(0)},
			{Key: "tag", Value: fmt.Sprintf("tag-%02d", i%16)},
			{Key: "payload", Value: "value-initial"},
		})
		if len(batch) == 128 {
			flush()
		}
	}
	flush()
	return s, collName
}

func buildUpdateOneCommand(tb testing.TB, collName string, id int64, val int64) wire.Document {
	tb.Helper()
	return mustDocument(tb, bson.D{
		{Key: "update", Value: collName},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: val}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
}

func findDocN(tb testing.TB, server *Server, collName string, id int64) int64 {
	tb.Helper()
	resp := serveCommand(tb, server, 100, bson.D{
		{Key: "find", Value: collName},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(tb, resp)
	if len(firstBatch) != 1 {
		tb.Fatalf("find id=%d firstBatch len=%d want 1", id, len(firstBatch))
	}
	n, ok := firstBatch[0].Lookup("n").Int64OK()
	if !ok {
		tb.Fatalf("find id=%d missing int64 n: %s", id, firstBatch[0])
	}
	return n
}

func TestUpdateCoalescerFastPathSingleWriterDurableReopen(t *testing.T) {
	dir := t.TempDir()
	const docCount = 16
	s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALDurable, docCount)

	if s.Server.UpdateCoalescingMaxDelay != 0 {
		t.Fatalf("expected default UpdateCoalescingMaxDelay=0, got %v", s.Server.UpdateCoalescingMaxDelay)
	}

	ctx := context.Background()
	for i := 0; i < docCount; i++ {
		cmd := buildUpdateOneCommand(t, collName, int64(i), int64(i+100))
		resp, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("update[%d]: resp=%s err=%v", i, resp, err)
		}
	}

	s.Server.updateMu.RLock()
	coalescer := s.Server.updateCoalescers["app."+collName]
	s.Server.updateMu.RUnlock()
	if coalescer == nil {
		t.Fatal("expected cached coalescer for app.items")
	}
	if got := coalescer.inlineExecs.Load(); got != docCount {
		t.Fatalf("inlineExecs=%d want %d", got, docCount)
	}
	if got := coalescer.workerBatches.Load(); got != 0 {
		t.Fatalf("workerBatches=%d want 0 on uncontended single-writer path", got)
	}

	// Verify expired budget deadline fails before mutation without touching the document.
	col, err := s.Collections.OpenCollection("app." + collName)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	expiredUpdate, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: int64(0)}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(9999)}}}}},
	}))
	if err != nil {
		t.Fatalf("parseMongoUpdateItem: %v", err)
	}
	expiredUpdate.budget = newMongoWriteBudget(1)
	expiredUpdate.budget.deadline = time.Now().Add(-time.Second)
	matched, modified, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, expiredUpdate)
	if err == nil || !strings.Contains(err.Error(), "exceeded its execution-time budget") {
		t.Fatalf("expired deadline err=%v want execution-time budget error", err)
	}
	if matched || modified {
		t.Fatalf("expired deadline matched=%v modified=%v want false,false", matched, modified)
	}
	if got := findDocN(t, s.Server, collName, 0); got != 100 {
		t.Fatalf("expired update mutated doc 0: got n=%d want 100", got)
	}

	// Verify runtime error propagation inside runMongoUpdateOne (typed index mismatch)
	// does not wedge execMu or break subsequent inline updates.
	badTypeUpdate, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: int64(1)}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: "not-an-int64"}}}}},
	}))
	if err != nil {
		t.Fatalf("parse bad type update: %v", err)
	}
	inlineBeforeErr := coalescer.inlineExecs.Load()
	if _, _, err := s.Server.runMongoUpdateCoalesced("app."+collName, col, badTypeUpdate); err == nil {
		t.Fatal("expected error when setting string on int64-indexed field")
	}
	if got := coalescer.inlineExecs.Load(); got != inlineBeforeErr+1 {
		t.Fatalf("expected error update to execute on inline fast path: inlineExecs=%d want %d", got, inlineBeforeErr+1)
	}
	nextCmd := buildUpdateOneCommand(t, collName, 1, 201)
	if resp, err := s.Server.commandResponse(ctx, "update", nextCmd, nil, 0); err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("post-error update: resp=%s err=%v", resp, err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify all inline-executed updates persisted durably.
	reopened, err := OpenStandaloneServer(StandaloneOptions{
		Dir:     dir,
		Profile: treedb.ProfileCommandWALDurable,
	})
	if err != nil {
		t.Fatalf("reopen OpenStandaloneServer: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	for i := 0; i < docCount; i++ {
		want := int64(i + 100)
		if i == 1 {
			want = 201
		}
		if got := findDocN(t, reopened.Server, collName, int64(i)); got != want {
			t.Fatalf("reopen doc[%d] n=%d want %d", i, got, want)
		}
	}
}

func TestUpdateCoalescerFastPathDelayCoalescingPreserved(t *testing.T) {
	s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALDurable, 4)
	defer func() { _ = s.Close() }()

	s.Server.UpdateCoalescingMaxDelay = 40 * time.Millisecond
	s.Server.UpdateCoalescingMaxBatch = 16

	beforeStats := s.Backend.Stats()
	statU64 := func(stats map[string]string, key string) uint64 {
		v, err := strconv.ParseUint(stats[key], 10, 64)
		if err != nil {
			t.Fatalf("stat %s=%q: %v", key, stats[key], err)
		}
		return v
	}

	ctx := context.Background()
	cmd0 := buildUpdateOneCommand(t, collName, 0, 11)
	cmd1 := buildUpdateOneCommand(t, collName, 1, 22)

	errs := make(chan error, 2)
	go func() {
		resp, err := s.Server.commandResponse(ctx, "update", cmd0, nil, 0)
		if err == nil && !mongoCommandResponseOK(resp) {
			err = fmt.Errorf("cmd0 not ok: %s", resp)
		}
		errs <- err
	}()

	// Wait until the first writer has created the coalescer and entered the worker delay window.
	deadline := time.Now().Add(2 * time.Second)
	var coalescer *mongoUpdateCoalescer
	for time.Now().Before(deadline) {
		s.Server.updateMu.RLock()
		coalescer = s.Server.updateCoalescers["app."+collName]
		s.Server.updateMu.RUnlock()
		if coalescer != nil && coalescer.workerBatches.Load() == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if coalescer == nil || coalescer.workerBatches.Load() != 1 {
		t.Fatalf("expected writer 0 to enter worker delay window (coalescer=%v)", coalescer)
	}

	go func() {
		resp, err := s.Server.commandResponse(ctx, "update", cmd1, nil, 0)
		if err == nil && !mongoCommandResponseOK(resp) {
			err = fmt.Errorf("cmd1 not ok: %s", resp)
		}
		errs <- err
	}()

	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("delayed update failed: %v", err)
		}
	}

	if got := coalescer.inlineExecs.Load(); got != 0 {
		t.Fatalf("inlineExecs=%d want 0 when UpdateCoalescingMaxDelay > 0", got)
	}
	if got := coalescer.workerBatches.Load(); got != 1 {
		t.Fatalf("workerBatches=%d want 1 (both writers coalesced within maxDelay)", got)
	}
	afterStats := s.Backend.Stats()
	syncs := statU64(afterStats, "treedb.command_wal.file_sync.calls_total") - statU64(beforeStats, "treedb.command_wal.file_sync.calls_total")
	if syncs != 1 {
		t.Fatalf("WAL file syncs=%d want 1 for delay-coalesced pair", syncs)
	}
	if got := findDocN(t, s.Server, collName, 0); got != 11 {
		t.Fatalf("doc 0 n=%d want 11", got)
	}
	if got := findDocN(t, s.Server, collName, 1); got != 22 {
		t.Fatalf("doc 1 n=%d want 22", got)
	}
}

func holdUpdateCoalescerLeaderForTest(t *testing.T, c *mongoUpdateCoalescer) func() {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.lfMu.Lock()
		if !c.lfInlineRunning && !c.lfWorkerBusy && c.lfLen == 0 {
			c.lfInlineRunning = true
			c.lfMu.Unlock()
			break
		}
		c.lfMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for coalescer idle to hold inline leader")
		}
		time.Sleep(time.Millisecond)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			c.finishInlineFastPath()
		})
	}
}

func TestUpdateCoalescerFastPathConcurrentCoalescingAndClose(t *testing.T) {
	const (
		writers       = 16
		opsPerWriter  = 25
		docCount      = writers
		expectedFinal = int64(opsPerWriter)
	)
	dir := t.TempDir()
	s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALRelaxed, docCount)

	ctx := context.Background()
	// Warm the coalescer so we can deterministically prove concurrent writers
	// coalesce into a single leader-follower batch even under GOMAXPROCS=1.
	coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}

	releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
	defer releaseLeader()
	wave1Errs := make(chan error, writers)
	var wave1WG sync.WaitGroup
	wave1WG.Add(writers)
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wave1WG.Done()
			cmd := buildUpdateOneCommand(t, collName, int64(w), 1)
			resp, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
			if err != nil {
				wave1Errs <- fmt.Errorf("wave1 writer %d: %w", w, err)
				return
			}
			if !mongoCommandResponseOK(resp) {
				wave1Errs <- fmt.Errorf("wave1 writer %d not ok: %s", w, resp)
				return
			}
		}()
	}
	queueDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(queueDeadline) && coalescer.queuedLen() < writers {
		time.Sleep(time.Millisecond)
	}
	queued := coalescer.queuedLen()
	releaseLeader()
	wave1WG.Wait()
	close(wave1Errs)
	for err := range wave1Errs {
		t.Fatal(err)
	}
	if queued != writers {
		t.Fatalf("wave1 queued=%d want %d while leader held", queued, writers)
	}
	if got := coalescer.workerBatches.Load(); got != 1 {
		t.Fatalf("wave1 workerBatches=%d want 1 coalesced batch", got)
	}

	// Now run free-running concurrent updates across all writers.
	start := make(chan struct{})
	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			<-start
			for step := 2; step <= opsPerWriter; step++ {
				cmd := buildUpdateOneCommand(t, collName, int64(w), int64(step))
				resp, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
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

	if got := coalescer.workerBatches.Load(); got == 0 {
		t.Fatalf("expected concurrent coalescing batches > 0 under w%d, got 0 (inlineExecs=%d)", writers, coalescer.inlineExecs.Load())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-coalescer.done:
	default:
		t.Fatal("Close returned before coalescer worker exited")
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
		if got := findDocN(t, reopened.Server, collName, int64(w)); got != expectedFinal {
			t.Fatalf("reopen doc[%d] n=%d want %d", w, got, expectedFinal)
		}
	}
}

func TestUpdateCoalescerFastPathActiveStreamSkipsIdleRetire(t *testing.T) {
	s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 4)
	defer func() { _ = s.Close() }()

	s.Server.UpdateCoalescingIdleTTL = 3 * time.Millisecond

	ctx := context.Background()
	resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, 1), nil, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("initial update: resp=%s err=%v", resp, err)
	}
	s.Server.updateMu.RLock()
	coalescer := s.Server.updateCoalescers["app."+collName]
	s.Server.updateMu.RUnlock()
	if coalescer == nil {
		t.Fatal("expected cached coalescer")
	}

	// Simulate an active inline caller spanning multiple idle TTL intervals.
	coalescer.inFlight.Add(1)
	time.Sleep(15 * time.Millisecond)
	if coalescer.isStopped() {
		coalescer.inFlight.Add(-1)
		t.Fatal("coalescer retired while inFlight > 0")
	}
	// Inline updates continue to work cleanly while inFlight > 0.
	resp, err = s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 0, 2), nil, 0)
	coalescer.inFlight.Add(-1)
	if err != nil || !mongoCommandResponseOK(resp) {
		t.Fatalf("active stream update: resp=%s err=%v", resp, err)
	}
	if coalescer.inlineExecs.Load() < 2 {
		t.Fatalf("inlineExecs=%d want >= 2", coalescer.inlineExecs.Load())
	}

	// Once idle, the coalescer should cleanly retire and evict itself.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.Server.updateMu.RLock()
		_, cached := s.Server.updateCoalescers["app."+collName]
		s.Server.updateMu.RUnlock()
		if !cached && coalescer.isStopped() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("coalescer did not retire once active inline stream stopped")
}

func TestUpdateCoalescerFastPathWaitEarlyExitDoesNotRecycleChannel(t *testing.T) {
	coalescer := &mongoUpdateCoalescer{done: make(chan struct{})}
	done := getMongoUpdateResultChan()
	close(coalescer.done)

	result, received := coalescer.waitForUpdateResultReceived(done)
	if received {
		t.Fatal("expected received=false when worker stopped before sending result")
	}
	if result.err == nil || !strings.Contains(result.err.Error(), "stopped before completing request") {
		t.Fatalf("result err=%v want stopped error", result.err)
	}

	// Simulate a late sender writing to the abandoned channel.
	// Because done was NOT returned to the pool, subsequent pooled channels must be completely clean.
	sentinelErr := errors.New("late sender poisoned result")
	done <- mongoUpdateCoalescerResult{err: sentinelErr}

	for i := 0; i < 64; i++ {
		ch := getMongoUpdateResultChan()
		select {
		case leaked := <-ch:
			t.Fatalf("pooled channel %d contained stale/leaked result: %+v", i, leaked)
		default:
		}
		putMongoUpdateResultChan(ch)
	}
}

func TestUpdateCoalescerFastPathZeroValueStructSafe(t *testing.T) {
	// Zero-value wakeCh == nil with run() should cleanly process enqueued requests and stop.
	coalescer := &mongoUpdateCoalescer{
		maxBatch:  4,
		requests:  make(chan mongoUpdateCoalescerRequest, 4),
		stoppedCh: make(chan struct{}),
		done:      make(chan struct{}),
	}
	go coalescer.run()
	defer coalescer.stop()

	done := make(chan mongoUpdateCoalescerResult, 1)
	budget := newMongoWriteBudget(1)
	budget.deadline = time.Now().Add(-time.Second)
	req := mongoUpdateCoalescerRequest{
		item: mongoUpdateItem{budget: budget},
		done: done,
	}
	if !coalescer.enqueue(req) {
		t.Fatal("enqueue failed on wakeCh==nil coalescer")
	}
	res := coalescer.waitForUpdateResult(done)
	if res.err == nil || !strings.Contains(res.err.Error(), "exceeded its execution-time budget") {
		t.Fatalf("result err=%v want execution-time budget error", res.err)
	}
	coalescer.stop()
	if !coalescer.stoppedFlag.Load() {
		t.Fatal("stoppedFlag was not set after stop()")
	}
}

func TestUpdateCoalescerHighConcurrencyAvoidsInlineStealing(t *testing.T) {
	t.Run("DeterministicNonStealing", func(t *testing.T) {
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 8)
		defer func() { _ = s.Close() }()

		if s.Server.UpdateCoalescingMaxDelay != 0 {
			t.Fatalf("expected default UpdateCoalescingMaxDelay=0, got %v", s.Server.UpdateCoalescingMaxDelay)
		}
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected coalescer")
		}
		if !coalescer.useLeaderFollower {
			t.Fatal("expected leader-follower mode for maxDelay == 0")
		}
		// Use a long sticky window so CI scheduling jitter cannot prematurely
		// expire sticky hot-worker mode before the follow-up check below.
		coalescer.lfMu.Lock()
		coalescer.stickyIdleWindow = 10 * time.Second
		coalescer.lfMu.Unlock()
		ctx := context.Background()

		// While a leader holds execution, even w=4 writers must deterministically queue
		// as followers (zero inline stealing) and execute together in one coalesced batch
		// upon direct handoff.
		const writers = 4
		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()

		errCh := make(chan error, writers)
		var wg sync.WaitGroup
		wg.Add(writers)
		for w := 0; w < writers; w++ {
			w := w
			go func() {
				defer wg.Done()
				resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), int64(100+w)), nil, 0)
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
			t.Fatalf("queued=%d want %d while leader held", got, writers)
		}
		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("inlineExecs=%d want 0 while leader held", got)
		}
		releaseLeader()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}

		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("w4 coalesced inlineExecs=%d want 0", got)
		}
		if got := coalescer.workerBatches.Load(); got != 1 {
			t.Fatalf("w4 coalesced workerBatches=%d want 1", got)
		}
		coalescer.lfMu.Lock()
		stickyAfterWave := coalescer.lfSticky
		coalescer.lfMu.Unlock()
		if !stickyAfterWave {
			t.Fatal("expected lfSticky=true after coalesced w=4 wave")
		}
		for w := 0; w < writers; w++ {
			if got := findDocN(t, s.Server, collName, int64(w)); got != int64(100+w) {
				t.Fatalf("doc[%d] n=%d want %d", w, got, 100+w)
			}
		}

		// With stickyIdleWindow=10s, a follow-up update stays deterministically pinned
		// on the same hot worker (preventing cross-core stealing).
		resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, 4, 404), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("post-wave sticky update: resp=%s err=%v", resp, err)
		}
		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("post-wave sticky inlineExecs=%d want 0", got)
		}
		if got := coalescer.workerBatches.Load(); got != 2 {
			t.Fatalf("post-wave sticky workerBatches=%d want 2", got)
		}
		if got := findDocN(t, s.Server, collName, 4); got != 404 {
			t.Fatalf("doc 4 n=%d want 404", got)
		}

		// Verify separately with a short sticky window that quiet timer expiry
		// cleanly clears sticky mode and resumes uncontended w=1 inline execution.
		sExpiry, collExpiry := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, 4)
		defer func() { _ = sExpiry.Close() }()
		cExpiry := sExpiry.Server.mongoUpdateCoalescer("app." + collExpiry)
		if cExpiry == nil {
			t.Fatal("expected expiry coalescer")
		}
		cExpiry.lfMu.Lock()
		cExpiry.stickyIdleWindow = 5 * time.Millisecond
		cExpiry.lfMu.Unlock()

		releaseExpiry := holdUpdateCoalescerLeaderForTest(t, cExpiry)
		defer releaseExpiry()
		var expWG sync.WaitGroup
		expWG.Add(2)
		expErrs := make(chan error, 2)
		for w := 0; w < 2; w++ {
			w := w
			go func() {
				defer expWG.Done()
				r, err := sExpiry.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collExpiry, int64(w), int64(200+w)), nil, 0)
				if err != nil || !mongoCommandResponseOK(r) {
					expErrs <- fmt.Errorf("expiry writer %d: resp=%s err=%v", w, r, err)
				}
			}()
		}
		expQueueDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(expQueueDeadline) && cExpiry.queuedLen() < 2 {
			time.Sleep(time.Millisecond)
		}
		if got := cExpiry.queuedLen(); got != 2 {
			t.Fatalf("expiry queued=%d want 2", got)
		}
		releaseExpiry()
		expWG.Wait()
		close(expErrs)
		for err := range expErrs {
			t.Fatal(err)
		}

		stickyDeadline := time.Now().Add(time.Second)
		for time.Now().Before(stickyDeadline) {
			cExpiry.lfMu.Lock()
			sticky := cExpiry.lfSticky
			busy := cExpiry.lfWorkerBusy
			cExpiry.lfMu.Unlock()
			if !sticky && !busy {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if cExpiry.hasLeaderWork() {
			t.Fatal("leader work remained active after sticky window expired")
		}
		cExpiry.lfMu.Lock()
		stillSticky := cExpiry.lfSticky
		cExpiry.lfMu.Unlock()
		if stillSticky {
			t.Fatal("lfSticky remained true after sticky window expired")
		}
		resp, err = sExpiry.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collExpiry, 2, 505), nil, 0)
		if err != nil || !mongoCommandResponseOK(resp) {
			t.Fatalf("post-sticky single update: resp=%s err=%v", resp, err)
		}
		if got := cExpiry.inlineExecs.Load(); got != 1 {
			t.Fatalf("post-sticky inlineExecs=%d want 1", got)
		}
		if got := findDocN(t, sExpiry.Server, collExpiry, 2); got != 505 {
			t.Fatalf("doc 2 n=%d want 505", got)
		}
	})

	t.Run("ContinuousMultiBatchDrainUnderExecMu", func(t *testing.T) {
		const (
			maxBatch = 4
			writers  = maxBatch*2 + 2
		)
		s, collName := openUpdateCoalescerBenchServer(t, t.TempDir(), treedb.ProfileCommandWALRelaxed, writers)
		defer func() { _ = s.Close() }()

		s.Server.UpdateCoalescingMaxBatch = maxBatch
		s.Server.UpdateCoalescingMaxDelay = 0
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			t.Fatal("expected coalescer")
		}

		ctx := context.Background()
		releaseLeader := holdUpdateCoalescerLeaderForTest(t, coalescer)
		defer releaseLeader()
		errCh := make(chan error, writers)
		var wg sync.WaitGroup
		wg.Add(writers)
		for w := 0; w < writers; w++ {
			w := w
			go func() {
				defer wg.Done()
				resp, err := s.Server.commandResponse(ctx, "update", buildUpdateOneCommand(t, collName, int64(w), int64(300+w)), nil, 0)
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
		queued := coalescer.queuedLen()
		releaseLeader()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}
		if queued != writers {
			t.Fatalf("queued=%d want %d while leader held", queued, writers)
		}
		if got := coalescer.inlineExecs.Load(); got != 0 {
			t.Fatalf("inlineExecs=%d want 0 during queued multi-batch drain", got)
		}
		if got := coalescer.workerBatches.Load(); got != 3 {
			t.Fatalf("workerBatches=%d want 3 for %d queued updates with maxBatch=%d", got, writers, maxBatch)
		}
		for w := 0; w < writers; w++ {
			if got := findDocN(t, s.Server, collName, int64(w)); got != int64(300+w) {
				t.Fatalf("doc[%d] n=%d want %d", w, got, 300+w)
			}
		}
	})

	t.Run("FreeRunningW16Reopen", func(t *testing.T) {
		const (
			writers      = 16
			opsPerWriter = 64
			docCount     = writers
		)
		dir := t.TempDir()
		s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALRelaxed, docCount)

		if s.Server.UpdateCoalescingMaxDelay != 0 {
			_ = s.Close()
			t.Fatalf("expected default UpdateCoalescingMaxDelay=0, got %v", s.Server.UpdateCoalescingMaxDelay)
		}
		coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
		if coalescer == nil {
			_ = s.Close()
			t.Fatal("expected coalescer")
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
					cmd := buildUpdateOneCommand(t, collName, int64(w), int64(step))
					resp, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
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
			_ = s.Close()
			t.Fatal(err)
		}

		if runtime.GOMAXPROCS(0) > 1 {
			if got := coalescer.workerBatches.Load(); got == 0 {
				_ = s.Close()
				t.Fatalf("expected workerBatches > 0 under free-running w%d, got 0 (inlineExecs=%d)", writers, coalescer.inlineExecs.Load())
			}
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

var updateCoalescerBenchSink wire.Document

func BenchmarkUpdateOneW1StandaloneCOW(b *testing.B) {
	const docCount = 256
	s, collName := openUpdateCoalescerBenchServer(b, b.TempDir(), treedb.ProfileCommandWALRelaxed, docCount)
	defer func() { _ = s.Close() }()

	cmds := make([]wire.Document, docCount*2)
	for i := 0; i < docCount; i++ {
		cmds[i] = buildUpdateOneCommand(b, collName, int64(i), 1)
		cmds[docCount+i] = buildUpdateOneCommand(b, collName, int64(i), 2)
	}
	ctx := context.Background()
	resp, err := s.Server.commandResponse(ctx, "update", cmds[0], nil, 0)
	if err != nil || !mongoCommandResponseOK(resp) {
		b.Fatalf("warm update: resp=%s err=%v", resp, err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := s.Server.commandResponse(ctx, "update", cmds[i%len(cmds)], nil, 0)
		if err != nil {
			b.Fatalf("update[%d]: %v", i, err)
		}
		if !mongoCommandResponseOK(resp) {
			b.Fatalf("update[%d] not ok: %s", i, resp)
		}
		updateCoalescerBenchSink = resp
	}
	b.StopTimer()
}

func BenchmarkUpdateOneW16StandaloneCOW(b *testing.B) {
	const (
		writers       = 16
		docsPerWriter = 16
		docCount      = writers * docsPerWriter
	)
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

	var nextOp atomic.Int64
	var firstErr atomic.Pointer[error]
	b.ReportAllocs()
	b.ResetTimer()
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
	b.StopTimer()
	if errPtr := firstErr.Load(); errPtr != nil {
		b.Fatal(*errPtr)
	}
}
