package db

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func combinedTestReq(key, value string, del, sync bool) *commitCombineReq {
	return &commitCombineReq{key: []byte(key), value: []byte(value), del: del, sync: sync, result: make(chan error, 1)}
}

func combinedTestResult(t *testing.T, req *commitCombineReq) error {
	t.Helper()
	select {
	case err := <-req.result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("combiner result did not arrive")
		return nil
	}
}

// No clock threshold: collection returns synchronously at empty or at the exact
// cap. The queue contains the actual request pointers, including their inputs.
func TestCommitCombinerCollectAlreadyQueued(t *testing.T) {
	for _, queued := range []int{0, 1, 63, 64, 129} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			queue := make(chan *commitCombineReq, queued)
			stop := make(chan struct{})
			first := combinedTestReq("first", "first", false, false)
			want := []*commitCombineReq{first}
			for i := 0; i < queued; i++ {
				req := combinedTestReq(fmt.Sprint(i), "value", i%2 == 0, i%3 == 0)
				queue <- req
				want = append(want, req)
			}
			var scratch [commitCombineMaxBatch]*commitCombineReq
			scratch[0] = first
			batch, stopped := collectCombinedBatch(scratch[:1], queue, stop)
			n := min(len(want), commitCombineMaxBatch)
			if stopped || len(batch) != n || len(queue) != len(want)-n {
				t.Fatalf("batch=%d stopped=%v leftovers=%d; want %d/false/%d", len(batch), stopped, len(queue), n, len(want)-n)
			}
			for i, req := range batch {
				if req != want[i] {
					t.Fatalf("FIFO[%d]=%p want %p", i, req, want[i])
				}
			}
			for i := n; i < len(want); i++ {
				if req := <-queue; req != want[i] {
					t.Fatalf("leftover[%d]=%p want %p", i, req, want[i])
				}
			}
		})
	}
}

// Invoke the same stop-observed collection/apply/finish/drain sequence as the
// worker, with the stop selection deterministic (no competing ready request).
func TestCommitCombinerCollectedStopAppliesAndReleases(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.stopCommitCombiner()
	if err := d.SetSync([]byte("delete"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	var scratch [commitCombineMaxBatch]*commitCombineReq
	collected := []*commitCombineReq{
		combinedTestReq("put", "retained", false, false),
		combinedTestReq("delete", "", true, false),
		combinedTestReq("sync", "durable", false, true),
	}
	copy(scratch[:], collected)
	queue := make(chan *commitCombineReq, 2)
	stop := make(chan struct{})
	close(stop)
	batch, stopped := collectCombinedBatch(scratch[:len(collected)], queue, stop)
	if !stopped || len(batch) != len(collected) {
		t.Fatalf("stop lost collected batch: stopped=%v batch=%d", stopped, len(batch))
	}
	// These requests arrive after stop was observed; they must receive closed,
	// and must not enter the already collected batch.
	pending := []*commitCombineReq{combinedTestReq("queued1", "no", false, false), combinedTestReq("queued2", "no", false, true)}
	for _, req := range pending {
		queue <- req
	}
	err = d.applyCombinedBatch(batch)
	d.finishCombined(batch, err)
	d.drainCombined(queue, errCommitCombinerClosed)
	for _, req := range collected {
		if err := combinedTestResult(t, req); err != nil {
			t.Fatal(err)
		}
	}
	for _, req := range pending {
		if err := combinedTestResult(t, req); !errors.Is(err, errCommitCombinerClosed) {
			t.Fatalf("queued result=%v", err)
		}
		if got, err := d.Get(req.key); err != nil || got != nil {
			t.Fatalf("queued request installed: value=%q error=%v", got, err)
		}
	}
	for i, req := range scratch {
		if req != nil {
			t.Fatalf("completed scratch[%d] retains request", i)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for key, want := range map[string][]byte{"put": []byte("retained"), "delete": nil, "sync": []byte("durable")} {
		if got, err := reopened.Get([]byte(key)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopen %q=%q error=%v want %q", key, got, err, want)
		}
	}
}

// Buffered backlog is deterministic. Blocking only the final result delivery of
// each full batch exposes the applied cut without a production observer. No new
// request is needed to fill either batch, and no third-batch mutation may leak.
func TestCommitCombinerBacklogFIFOAndBound(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	d.stopCommitCombiner()
	queue := make(chan *commitCombineReq, 130)
	stop, done := make(chan struct{}), make(chan struct{})
	requests := make([]*commitCombineReq, 130)
	for i := range requests {
		requests[i] = combinedTestReq("ordered", fmt.Sprint(i), i == 63 || i == 128, i == 17)
		if i == 0 {
			requests[i].key = []byte("anchor")
		}
		queue <- requests[i]
	}
	gate1, gate2 := make(chan error), make(chan error)
	requests[63].result, requests[127].result = gate1, gate2
	var stopOnce sync.Once
	go d.commitCombinerLoop(queue, stop, done)
	defer func() {
		// Also release delivery gates on every assertion/error path before join.
		stopOnce.Do(func() { close(stop) })
		go func() {
			for {
				select {
				case <-gate1:
				case <-gate2:
				case <-done:
					return
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("combiner did not join")
		}
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := combinedTestResult(t, requests[0]); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Get([]byte("ordered")); err != nil || got != nil || len(queue) != 66 {
		t.Fatalf("first cut value=%q err=%v queued=%d; want deleted and 66 leftovers", got, err, len(queue))
	}
	state := d.rootPublication.coordinator.Stats()
	if state.DurableCommitSeq == 0 || state.DurableCommitSeq != state.VisibleCommitSeq {
		t.Fatalf("mixed sync did not finish durable cut: %+v", state)
	}
	if err := combinedTestResult(t, requests[63]); err != nil {
		t.Fatal(err)
	}
	if err := combinedTestResult(t, requests[64]); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Get([]byte("ordered")); err != nil || string(got) != "127" || len(queue) != 2 {
		t.Fatalf("second cut value=%q err=%v queued=%d; want 127 and 2 leftovers", got, err, len(queue))
	}
	if err := combinedTestResult(t, requests[127]); err != nil {
		t.Fatal(err)
	}
	if err := combinedTestResult(t, requests[129]); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Get([]byte("ordered")); err != nil || string(got) != "129" {
		t.Fatalf("final FIFO put-after-delete=%q error=%v", got, err)
	}
	for i, req := range requests {
		if i == 0 || i == 63 || i == 64 || i == 127 || i == 129 {
			continue
		}
		if err := combinedTestResult(t, req); err != nil {
			t.Fatalf("result[%d]=%v", i, err)
		}
	}
}

func TestCommitCombinerSyncFailureFansOutAndPoisons(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.stopCommitCombiner()
	queue := make(chan *commitCombineReq, 3)
	stop, done := make(chan struct{}), make(chan struct{})
	requests := []*commitCombineReq{
		combinedTestReq("one", "1", false, false),
		combinedTestReq("two", "2", false, true),
		combinedTestReq("three", "3", false, false),
	}
	for _, req := range requests {
		queue <- req
	}
	// Existing physical meta-sync failpoint, after candidate installation.
	d.testFailSyncMeta.Store(true)
	go d.commitCombinerLoop(queue, stop, done)
	defer func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("failed combiner did not join")
		}
		d.testFailSyncMeta.Store(false)
	}()
	var firstErr error
	for i, req := range requests {
		err := combinedTestResult(t, req)
		if !errors.Is(err, errTestSyncMetaFailpoint) {
			t.Fatalf("result[%d]=%v lost physical failure", i, err)
		}
		if i == 0 {
			firstErr = err
		} else if err != firstErr {
			t.Fatalf("result[%d]=%v was not common error %v", i, err, firstErr)
		}
	}
	if !d.publicationPoisoned.Load() {
		t.Fatal("post-install sync failure did not poison publication")
	}
	if handled, err := d.writeViaCommitCombiner([]byte("later"), []byte("no"), false, false); !handled || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("later write handled=%v error=%v", handled, err)
	}
}

// Delay starting the worker, rather than delaying collection. All public callers
// are then observably queued with independent owned copies before one real apply.
func TestCommitCombinerConcurrentQueuedCopies(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	d.stopCommitCombiner()
	queue := make(chan *commitCombineReq, 32)
	stop, done := make(chan struct{}), make(chan struct{})
	d.combineMu.Lock()
	d.combineReqCh, d.combineStopCh = queue, stop
	d.combineMu.Unlock()
	var startOnce sync.Once
	startWorker := func() {
		startOnce.Do(func() {
			d.combineMu.Lock()
			d.combineDoneCh = done
			d.combineMu.Unlock()
			go d.commitCombinerLoop(queue, stop, done)
		})
	}
	const count = 16
	keys, values := make([][]byte, count), make([][]byte, count)
	errorsCh := make(chan error, count)
	var callers sync.WaitGroup
	callers.Add(count)
	for i := 0; i < count; i++ {
		keys[i], values[i] = []byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i))
		go func(i int) {
			defer callers.Done()
			handled, err := d.writeViaCommitCombiner(keys[i], values[i], false, i == count/2)
			if !handled && err == nil {
				err = errors.New("queued public request unexpectedly fell back")
			}
			errorsCh <- err
		}(i)
	}
	callersDone := make(chan struct{})
	go func() { callers.Wait(); close(callersDone) }()
	var held []*commitCombineReq
	defer func() {
		for _, req := range held {
			queue <- req
		}
		startWorker()
		d.stopCommitCombiner()
		select {
		case <-callersDone:
		case <-time.After(5 * time.Second):
			t.Error("queued callers did not join")
		}
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Actual receives establish the ownership transfer before caller buffers are
	// mutated (observing channel length alone is not that synchronization).
	for i := 0; i < count; i++ {
		select {
		case req := <-queue:
			held = append(held, req)
		case <-time.After(5 * time.Second):
			t.Fatalf("public backlog captured=%d want %d", len(held), count)
		}
	}
	for i := range keys {
		clear(keys[i])
		clear(values[i])
	}
	for _, req := range held {
		queue <- req
	}
	held = nil
	before := d.State().CommitSeq
	startWorker()
	for i := 0; i < count; i++ {
		select {
		case err := <-errorsCh:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("public queued write did not finish")
		}
	}
	if after := d.State().CommitSeq; after != before+1 {
		t.Fatalf("one prequeued cohort published %d cuts, want 1", after-before)
	}
	for i := range keys {
		key, want := []byte(fmt.Sprintf("key-%02d", i)), fmt.Sprintf("value-%02d", i)
		if got, err := d.Get(key); err != nil || string(got) != want {
			t.Fatalf("owned %q=%q error=%v want %q", key, got, err, want)
		}
	}
}
