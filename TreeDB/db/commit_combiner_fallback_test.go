package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCommitCombinerFallback_CombinerStopped_UsesDirectWritePath(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Force API writes through the direct single-key path.
	d.stopCommitCombiner()

	if err := d.Set([]byte("k1"), []byte("v1")); err != nil {
		_ = d.Close()
		t.Fatalf("Set k1: %v", err)
	}
	if err := d.Set([]byte("k2"), []byte("v2")); err != nil {
		_ = d.Close()
		t.Fatalf("Set k2: %v", err)
	}
	if err := d.Delete([]byte("k2")); err != nil {
		_ = d.Close()
		t.Fatalf("Delete k2: %v", err)
	}
	if err := d.SetSync([]byte("dur"), []byte("ok")); err != nil {
		_ = d.Close()
		t.Fatalf("SetSync dur: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close writer DB: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	got, err := reopened.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get k1 = %q, want %q", got, []byte("v1"))
	}

	got, err = reopened.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get k2: %v", err)
	}
	if got != nil {
		t.Fatalf("Get k2 = %q, want nil", got)
	}

	got, err = reopened.Get([]byte("dur"))
	if err != nil {
		t.Fatalf("Get dur: %v", err)
	}
	if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("Get dur = %q, want %q", got, []byte("ok"))
	}
}

func TestCommitCombinerFallback_QueueFullFastProbe(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	d.stopCommitCombiner()

	reqCh := make(chan *commitCombineReq, 1)
	stopCh := make(chan struct{})
	sentinel := &commitCombineReq{result: make(chan error, 1)}
	reqCh <- sentinel

	d.combineMu.Lock()
	d.combineReqCh = reqCh
	d.combineStopCh = stopCh
	d.combineDoneCh = nil
	d.combineMu.Unlock()

	if err := d.Set([]byte("fast"), []byte("path")); err != nil {
		t.Fatalf("Set fast path fallback: %v", err)
	}

	select {
	case got := <-reqCh:
		if got != sentinel {
			t.Fatalf("queue head changed after fallback; got %p want %p", got, sentinel)
		}
	default:
		t.Fatalf("expected sentinel to remain queued after fast-probe fallback")
	}

	got, err := d.Get([]byte("fast"))
	if err != nil {
		t.Fatalf("Get fast: %v", err)
	}
	if !bytes.Equal(got, []byte("path")) {
		t.Fatalf("Get fast = %q, want %q", got, []byte("path"))
	}
}

func TestWriteViaCommitCombiner_UnbufferedQueue_DefaultFallback(t *testing.T) {
	d := &DB{}
	reqCh := make(chan *commitCombineReq)
	stopCh := make(chan struct{})

	d.combineMu.Lock()
	d.combineReqCh = reqCh
	d.combineStopCh = stopCh
	d.combineDoneCh = nil
	d.combineMu.Unlock()

	handled, err := d.writeViaCommitCombiner([]byte("k"), []byte("v"), false, false)
	if err != nil {
		t.Fatalf("writeViaCommitCombiner err = %v, want nil", err)
	}
	if handled {
		t.Fatalf("writeViaCommitCombiner handled = true, want false")
	}
}

func TestWriteViaCommitCombiner_StopClosedBeforeEnqueue(t *testing.T) {
	d := &DB{}
	reqCh := make(chan *commitCombineReq)
	stopCh := make(chan struct{})
	close(stopCh)

	d.combineMu.Lock()
	d.combineReqCh = reqCh
	d.combineStopCh = stopCh
	d.combineDoneCh = nil
	d.combineMu.Unlock()

	handled, err := d.writeViaCommitCombiner([]byte("k"), []byte("v"), false, false)
	if !handled {
		t.Fatalf("writeViaCommitCombiner handled = false, want true")
	}
	if !errors.Is(err, errCommitCombinerClosed) {
		t.Fatalf("writeViaCommitCombiner err = %v, want %v", err, errCommitCombinerClosed)
	}
}

func TestWriteViaCommitCombiner_StopClosedDuringFastProbe(t *testing.T) {
	d := &DB{}
	reqCh := make(chan *commitCombineReq, 1)
	stopCh := make(chan struct{})
	reqCh <- &commitCombineReq{result: make(chan error, 1)}
	close(stopCh)

	d.combineMu.Lock()
	d.combineReqCh = reqCh
	d.combineStopCh = stopCh
	d.combineDoneCh = nil
	d.combineMu.Unlock()

	handled, err := d.writeViaCommitCombiner([]byte("k"), []byte("v"), false, false)
	if !handled {
		t.Fatalf("writeViaCommitCombiner handled = false, want true")
	}
	if !errors.Is(err, errCommitCombinerClosed) {
		t.Fatalf("writeViaCommitCombiner err = %v, want %v", err, errCommitCombinerClosed)
	}
}

func TestWriteViaCommitCombiner_StopClosedWhileWaitingResult(t *testing.T) {
	d := &DB{}
	reqCh := make(chan *commitCombineReq, 1)
	stopCh := make(chan struct{})
	gotReq := make(chan *commitCombineReq, 1)

	d.combineMu.Lock()
	d.combineReqCh = reqCh
	d.combineStopCh = stopCh
	d.combineDoneCh = nil
	d.combineMu.Unlock()

	go func() {
		req := <-reqCh
		gotReq <- req
		close(stopCh)
	}()

	type writeResult struct {
		handled bool
		err     error
	}
	resCh := make(chan writeResult, 1)
	go func() {
		handled, err := d.writeViaCommitCombiner([]byte("k"), []byte("v"), false, false)
		resCh <- writeResult{handled: handled, err: err}
	}()

	select {
	case req := <-gotReq:
		if req == nil {
			t.Fatalf("received nil request")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for queued request")
	}

	select {
	case res := <-resCh:
		if !res.handled {
			t.Fatalf("writeViaCommitCombiner handled = false, want true")
		}
		if !errors.Is(res.err, errCommitCombinerClosed) {
			t.Fatalf("writeViaCommitCombiner err = %v, want %v", res.err, errCommitCombinerClosed)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for writeViaCommitCombiner result")
	}
}

func TestDrainCombined_PropagatesErrorToPendingRequests(t *testing.T) {
	d := &DB{}
	reqCh := make(chan *commitCombineReq, 2)
	wantErr := errors.New("drain error")
	req1 := &commitCombineReq{result: make(chan error, 1)}
	req2 := &commitCombineReq{result: make(chan error, 1)}
	reqCh <- req1
	reqCh <- req2

	d.drainCombined(reqCh, wantErr)

	select {
	case err := <-req1.result:
		if !errors.Is(err, wantErr) {
			t.Fatalf("req1 err = %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for req1 result")
	}

	select {
	case err := <-req2.result:
		if !errors.Is(err, wantErr) {
			t.Fatalf("req2 err = %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for req2 result")
	}
}

func TestCommitCombinerFallback_PointerCommitDurableAfterReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	d.stopCommitCombiner()

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		_ = d.Close()
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		_ = d.Close()
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewWriter: %v", err)
	}
	value := bytes.Repeat([]byte("p"), 128)
	ptr, err := w.Append(0, nil, 1, value)
	if err != nil {
		_ = w.Close()
		_ = d.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("Close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)

	b := d.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("kp"), ptr); err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("Close batch: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close writer DB: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	got, err := reopened.Get([]byte("kp"))
	if err != nil {
		t.Fatalf("Get pointer value: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get pointer value mismatch: got %d bytes, want %d", len(got), len(value))
	}
}
