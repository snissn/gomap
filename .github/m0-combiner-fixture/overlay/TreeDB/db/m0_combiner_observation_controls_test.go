package db

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

func TestM0CombinerOriginalProducerBranches(t *testing.T) {
	for _, name := range []string{"no_channel", "full", "unbuffered", "stopped", "saturated_stop", "read_only", "missing_db"} {
		t.Run(name, func(t *testing.T) {
			d := &DB{}
			switch name {
			case "full", "saturated_stop":
				d.combineReqCh = make(chan *commitCombineReq, 1)
				d.combineReqCh <- combinedTestReq("sentinel", "unchanged", false, false)
				d.combineStopCh = make(chan struct{})
			case "unbuffered", "stopped":
				d.combineReqCh = make(chan *commitCombineReq)
				d.combineStopCh = make(chan struct{})
			case "read_only":
				d.readOnly = true
			case "missing_db":
				d = nil
			}
			if name == "stopped" || name == "saturated_stop" {
				close(d.combineStopCh)
			}
			o := newM0CombinerOwner()
			s := o.slots[0].enter()
			handled, err := d.writeViaCommitCombinerWithM0Observation([]byte("key"), []byte("value"), false, false, mvccadmission.Input{}, s)
			if !handled {
				s.fallback() // Original API would now enter its direct write path.
			}
			s.returned(err)
			wantStatus, wantEnqueue := "DIRECT_FALLBACK", "NO_CHANNEL"
			switch name {
			case "full":
				wantEnqueue = "SATURATED_FALLBACK"
			case "unbuffered":
				wantEnqueue = "SEND_FALLBACK"
			case "stopped":
				wantStatus, wantEnqueue = "ORIGINAL_ERROR", "SEND_STOP"
			case "saturated_stop":
				wantStatus, wantEnqueue = "ORIGINAL_ERROR", "SATURATED_STOP"
			case "read_only", "missing_db":
				wantStatus, wantEnqueue = "ORIGINAL_ERROR", ""
			}
			status, validationErr := m0ValidateServiceSlot(s) // No worker exists.
			if validationErr != nil || status != wantStatus || s.Caller.EnqueueStatus != wantEnqueue || handled != (wantStatus == "ORIGINAL_ERROR") {
				t.Fatalf("handled=%v error=%v status=%s enqueue=%s validate=%v", handled, err, status, s.Caller.EnqueueStatus, validationErr)
			}
			if (name == "stopped" || name == "saturated_stop") && !errors.Is(err, errCommitCombinerClosed) {
				t.Fatalf("original closed error changed: %v", err)
			}
			if name == "read_only" && !errors.Is(err, ErrReadOnly) {
				t.Fatalf("original read-only error changed: %v", err)
			}
			if name == "full" || name == "saturated_stop" {
				if got := <-d.combineReqCh; string(got.key) != "sentinel" || len(d.combineReqCh) != 0 {
					t.Fatal("original queue sentinel changed")
				}
			}
		})
	}
}

func TestM0CombinerObservedSetCheckedLifecycle(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	o := newM0CombinerOwner()
	for i, value := range []string{"first", "last"} {
		if err := d.setWithM0CombinerObservation([]byte("observed"), []byte(value), &o.slots[2*i]); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := d.Get([]byte("observed")); err != nil || string(got) != "last" {
		t.Fatalf("acknowledged value=%q err=%v", got, err)
	}
	// Nil-slot SetSync and Delete retain their original shared producer paths.
	if err := d.SetSync([]byte("ordinary"), []byte("sync")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete([]byte("ordinary")); err != nil {
		t.Fatal(err)
	}
	if o.originalDoneObserved(false) {
		t.Fatal("missing original foreground join was accepted")
	}
	closed = true // Never retry an original Close error during cleanup.
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if !o.originalDoneObserved(true) {
		t.Fatal("original checked DB.Close did not publish borrowed worker done")
	}
	for _, ordinal := range []int{0, 2} {
		if status, err := m0ValidateServiceSlot(&o.slots[ordinal]); err != nil || status != "APPLIED_RESULT_RECEIVED" {
			t.Fatalf("ordinal=%d status=%s err=%v", ordinal, status, err)
		}
	}
	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if got, err := reopened.Get([]byte("observed")); err != nil || string(got) != "last" {
		t.Fatalf("reopen acknowledged value=%q err=%v", got, err)
	}
	if got, err := reopened.Get([]byte("ordinary")); err != nil || got != nil {
		t.Fatalf("reopen ordinary delete value=%q err=%v", got, err)
	}
}

func TestM0CombinerActualCollectApplyFinishDrain(t *testing.T) {
	for _, queued := range []int{0, 1, 63, 64, 129} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			d.stopCommitCombiner()
			defer func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}()
			o := newM0CombinerOwner()
			queue := make(chan *commitCombineReq, queued+1)
			stop := make(chan struct{})
			requests := make([]*commitCombineReq, queued+1)
			for i := range requests {
				s := o.slots[i*2].enter()
				s.enqueueBefore()
				req := combinedTestReq(fmt.Sprint(i), "value", false, i == 1)
				req.m0 = s
				queue <- req
				s.enqueueOutcome("ENQUEUED")
				s.selectBefore()
				requests[i] = req
			}
			var scratch [commitCombineMaxBatch]*commitCombineReq
			first := <-queue
			first.m0.dequeued()
			scratch[0] = first
			batch, stopped := collectCombinedBatch(scratch[:1], queue, stop)
			n := min(len(requests), commitCombineMaxBatch)
			if stopped || len(batch) != n || len(queue) != len(requests)-n {
				t.Fatalf("actual ready-only collector len=%d stopped=%v queued=%d", len(batch), stopped, len(queue))
			}
			for i, req := range batch {
				if req != requests[i] {
					t.Fatalf("actual FIFO position %d changed", i)
				}
			}
			m0BindBatch(batch, 1)
			d.finishCombined(batch, d.applyCombinedBatch(batch))
			d.drainCombined(queue, errCommitCombinerClosed)
			// These helpers execute synchronously on this test owner, so no live
			// worker fields are read. Real worker publication is tested separately.
			for i, req := range requests {
				err := combinedTestResult(t, req)
				req.m0.selected("RESULT")
				req.m0.returned(err)
				status, validationErr := m0ValidateServiceSlot(req.m0)
				want := "APPLIED_RESULT_RECEIVED"
				if i >= n {
					want = "DRAINED_NOT_APPLIED"
					if !errors.Is(err, errCommitCombinerClosed) {
						t.Fatal("original drain error changed", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if validationErr != nil || status != want {
					t.Fatalf("request=%d status=%s validate=%v", i, status, validationErr)
				}
				if i < n && (req.m0.Worker.BatchIndex != i || req.m0.Worker.BatchSize != n || req.m0.Worker.Selected != n) {
					t.Fatal("actual batch membership not preserved")
				}
			}
			for i, req := range scratch {
				if req != nil {
					t.Fatalf("original finish retained scratch[%d]", i)
				}
			}
			if len(queue) != 0 {
				t.Fatal("original drain left queued requests")
			}
		})
	}
}

func TestM0CombinerStopReturnSlotLifetime(t *testing.T) {
	queue := make(chan *commitCombineReq, 1)
	stop, done := make(chan struct{}), make(chan struct{})
	d := &DB{combineReqCh: queue, combineStopCh: stop, combineDoneCh: done}
	o := newM0CombinerOwner()
	s := o.slots[0].enter()
	allowDrain := make(chan struct{})
	workerErrors := make(chan error, 1)
	key, value := []byte("key"), []byte("value")
	go func() {
		defer close(done)
		// Existing fake-worker boundary: observe original owned request, stop
		// caller, then let the real drain helper own it. No production seam.
		req := <-queue
		close(stop)
		<-allowDrain
		if !bytes.Equal(req.key, []byte("key")) || !bytes.Equal(req.value, []byte("value")) || cap(req.result) != 1 || req.m0 != s {
			workerErrors <- errors.New("original owned copies/result buffer/slot binding changed")
		}
		queue <- req
		d.drainCombined(queue, errCommitCombinerClosed)
	}()
	handled, err := d.writeViaCommitCombinerWithM0Observation(key, value, false, false, mvccadmission.Input{}, s)
	s.returned(err)
	if !handled || !errors.Is(err, errCommitCombinerClosed) || s.Caller.Branch != "STOP" || o.originalDoneObserved(true) {
		// Always release the controlled worker before an assertion can unwind.
		close(allowDrain)
		<-done
		t.Fatalf("original stop behavior: handled=%v err=%v branch=%s", handled, err, s.Caller.Branch)
	}
	// Caller owns these originals again, while the worker still owns copies.
	key[0], value[0] = 'X', 'Y'
	close(allowDrain)
	<-done
	select {
	case err := <-workerErrors:
		t.Fatal(err)
	default:
	}
	if !o.originalDoneObserved(true) {
		t.Fatal("original done publication unavailable")
	}
	if status, err := m0ValidateServiceSlot(s); err != nil || status != "DRAINED_NOT_APPLIED" {
		t.Fatalf("post-stop service status=%s error=%v", status, err)
	}
}

func m0ServiceModelSlot() *m0CombinerSlot {
	o := newM0CombinerOwner()
	s := &o.slots[0]
	stamp := func(ns int64) m0CombinerStamp { return m0CombinerStamp{NS: ns, Valid: true} }
	s.Caller = m0CombinerCaller{
		Entry: stamp(1), EnqueueBefore: stamp(2), EnqueueAfter: stamp(10),
		SelectBefore: stamp(11), SelectAfter: stamp(13), Return: stamp(14),
		EnqueueStatus: "ENQUEUED", Branch: "RESULT", Error: "NIL", Entered: true,
	}
	s.Worker = m0CombinerWorker{
		Dequeue: stamp(3), ApplyBefore: stamp(4), WriteBefore: stamp(5), WriteAfter: stamp(6),
		CloseBefore: stamp(7), CloseAfter: stamp(8), ApplyAfter: stamp(9),
		SendBefore: stamp(12), SendAfter: stamp(15), BatchSerial: 1,
		BatchSize: 1, Selected: 1, Dispatch: "APPLIED", WriteKind: "WRITE",
		WriteError: "NIL", CloseError: "NIL", Terminal: true,
	}
	return s
}

func TestM0CombinerOverlapAndUnavailableControls(t *testing.T) {
	// Explicit MODEL of legal cross-owner ordering, not measured worker proof.
	if status, err := m0ValidateServiceSlot(m0ServiceModelSlot()); err != nil || status != "APPLIED_RESULT_RECEIVED" {
		t.Fatalf("legal E1>D / receive<S1 rejected: %s %v", status, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*m0CombinerSlot)
	}{
		{"caller_invalid", func(s *m0CombinerSlot) { s.Caller.Invalid = true }},
		{"worker_invalid", func(s *m0CombinerSlot) { s.Worker.Invalid = true }},
		{"missing_terminal", func(s *m0CombinerSlot) { s.Worker.Terminal = false }},
		{"absent_clock", func(s *m0CombinerSlot) { s.Worker.Dequeue.Valid = false }},
		{"clock_regression", func(s *m0CombinerSlot) { s.Worker.WriteAfter.NS = 0 }},
		{"serial_overflow", func(s *m0CombinerSlot) { s.Worker.BatchSerial = 0 }},
		{"batch_bound", func(s *m0CombinerSlot) { s.Worker.BatchSize = 65 }},
		{"unknown_dispatch", func(s *m0CombinerSlot) { s.Worker.Dispatch = "unknown" }},
		{"unknown_write", func(s *m0CombinerSlot) { s.Worker.WriteKind = "unknown" }},
		{"unknown_enqueue", func(s *m0CombinerSlot) { s.Caller.EnqueueStatus = "unknown" }},
		{"unknown_error", func(s *m0CombinerSlot) { s.Caller.Error = "unknown" }},
		{"ordinal_bound", func(s *m0CombinerSlot) { s.Ordinal = 4096 }},
		{"odd_binding", func(s *m0CombinerSlot) { s.Ordinal = 1 }},
		{"duplicate_binding", func(s *m0CombinerSlot) { s.enter() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := m0ServiceModelSlot()
			tc.change(s)
			if _, err := m0ValidateServiceSlot(s); err == nil {
				t.Fatal("invalid diagnostic model accepted")
			}
		})
	}
}

func TestM0CombinerDoneClosureIsNotSuccess(t *testing.T) {
	for _, goexit := range []bool{false, true} {
		t.Run(fmt.Sprintf("goexit_%t", goexit), func(t *testing.T) {
			s := m0ServiceModelSlot()
			s.Worker.Terminal = false
			done := make(chan struct{})
			s.owner.done, s.owner.doneBound = done, true
			go func() {
				defer close(done)
				// Outer TEST boundary only. Product combiner has no recovery or
				// fault hook; actual inner panic/Close injection is not qualified.
				defer func() { _ = recover() }()
				if goexit {
					runtime.Goexit()
				}
				panic("test boundary unwind")
			}()
			<-done
			if !s.owner.originalDoneObserved(true) {
				t.Fatal("test boundary closure not observed")
			}
			if _, err := m0ValidateServiceSlot(s); err == nil {
				t.Fatal("done closure fabricated terminal service success")
			}
		})
	}
}

func m0ServicePopulationFixture() (*m0CombinerOwner, *m0OrdinalObserver) {
	o := newM0CombinerOwner()
	ordinals := m0CompleteOrdinalFixture()
	model := m0ServiceModelSlot()
	for i := range o.slots {
		if i%2 == 0 {
			o.slots[i].Caller = model.Caller
			o.slots[i].Worker = model.Worker
			o.slots[i].Worker.BatchSerial = uint64(i/4 + 1)
			o.slots[i].Worker.BatchIndex = (i / 2) % 2
			o.slots[i].Worker.BatchSize, o.slots[i].Worker.Selected = 2, 2
		}
	}
	return o, ordinals
}

func TestM0CombinerPopulationAndSharedReceiptControls(t *testing.T) {
	o, ordinals := m0ServicePopulationFixture()
	rows, err := m0ServiceRows(o, ordinals)
	if err != nil || len(rows) != 4096 {
		t.Fatalf("full bounded population: rows=%d error=%v", len(rows), err)
	}
	points, ranges := 0, 0
	for i, row := range rows {
		if row.Ordinal != i || row.OriginalDurationNS != ordinals.records[i].DurationNS || row.OriginalRegion != ordinals.records[i].Region {
			t.Fatal("original ordinal/timing/region binding changed")
		}
		if i%2 == 0 && row.Status == "APPLIED_RESULT_RECEIVED" {
			points++
		} else if i%2 != 0 && row.Status == "NON_COMBINER" {
			ranges++
		} else {
			t.Fatalf("population status at %d=%s", i, row.Status)
		}
	}
	if points != 2048 || ranges != 2048 {
		t.Fatal("4096/2048/2048 population changed")
	}
	for _, tc := range []struct {
		name   string
		change func(*m0CombinerOwner, *m0OrdinalObserver)
	}{
		{"duplicate_position", func(o *m0CombinerOwner, _ *m0OrdinalObserver) { o.slots[2].Worker.BatchIndex = 0 }},
		{"conflicting_shared_time", func(o *m0CombinerOwner, _ *m0OrdinalObserver) { o.slots[2].Worker.ApplyBefore.NS++ }},
		{"missing_member", func(o *m0CombinerOwner, _ *m0OrdinalObserver) { o.slots[2].Worker.BatchSerial = 2049 }},
		{"odd_service", func(o *m0CombinerOwner, _ *m0OrdinalObserver) { o.slots[1].Caller.Entered = true }},
		{"mixed_owner", func(o *m0CombinerOwner, _ *m0OrdinalObserver) { o.slots[0].owner = newM0CombinerOwner() }},
		{"identity", func(o *m0CombinerOwner, _ *m0OrdinalObserver) { o.slots[0].Ordinal = 2 }},
		{"original_kind", func(_ *m0CombinerOwner, ordinals *m0OrdinalObserver) { ordinals.records[0].Kind = "delete-range" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, ordinals := m0ServicePopulationFixture()
			tc.change(o, ordinals)
			if _, err := m0ServiceRows(o, ordinals); err == nil {
				t.Fatal("invalid population/shared receipt accepted")
			}
		})
	}
}

func TestM0CombinerZeroValidClockAndDoneBorrowControls(t *testing.T) {
	s := m0ServiceModelSlot()
	for _, stamp := range []*m0CombinerStamp{
		&s.Caller.Entry, &s.Caller.EnqueueBefore, &s.Caller.EnqueueAfter,
		&s.Caller.SelectBefore, &s.Caller.SelectAfter, &s.Caller.Return,
		&s.Worker.Dequeue, &s.Worker.ApplyBefore, &s.Worker.ApplyAfter,
		&s.Worker.WriteBefore, &s.Worker.WriteAfter, &s.Worker.CloseBefore,
		&s.Worker.CloseAfter, &s.Worker.SendBefore, &s.Worker.SendAfter,
	} {
		stamp.NS, stamp.Valid = 0, true
	}
	if _, err := m0ValidateServiceSlot(s); err != nil {
		t.Fatal("valid zero observation treated as unavailable", err)
	}
	done := make(chan struct{})
	s.borrowDone(done)
	if s.owner.originalDoneObserved(true) {
		t.Fatal("open borrowed done accepted")
	}
	close(done)
	if s.owner.originalDoneObserved(false) || !s.owner.originalDoneObserved(true) {
		t.Fatal("original foreground/worker join conjunction changed")
	}
	other := make(chan struct{})
	close(other)
	s.borrowDone(other)
	if s.owner.originalDoneObserved(true) {
		t.Fatal("mixed original worker lifetimes accepted")
	}
}

func TestM0CombinerObservedOriginalSyncFailure(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	d.stopCommitCombiner()
	queue := make(chan *commitCombineReq, 3)
	stop, done := make(chan struct{}), make(chan struct{})
	o := newM0CombinerOwner()
	requests := make([]*commitCombineReq, 3)
	for i := range requests {
		s := o.slots[i*2].enter()
		s.borrowDone(done)
		s.enqueueBefore()
		req := combinedTestReq(fmt.Sprint(i), "value", false, i == 1)
		req.m0 = s
		queue <- req
		s.enqueueOutcome("ENQUEUED")
		s.selectBefore()
		requests[i] = req
	}
	d.testFailSyncMeta.Store(true) // Existing physical meta-sync failpoint only.
	go d.commitCombinerLoop(queue, stop, done)
	defer func() {
		close(stop)
		<-done
		d.testFailSyncMeta.Store(false)
		if !o.originalDoneObserved(true) {
			t.Error("original error-path worker join unavailable")
		}
		for _, req := range requests {
			if req.m0.Caller.Return.Valid {
				status, validationErr := m0ValidateServiceSlot(req.m0)
				if validationErr != nil || status != "APPLIED_RESULT_RECEIVED" || req.m0.Worker.WriteKind != "WRITE_SYNC" || req.m0.Worker.WriteError != "OTHER" || req.m0.Worker.CloseError != "NIL" {
					t.Errorf("actual error-path service: status=%s error=%v worker=%+v", status, validationErr, req.m0.Worker)
				}
			}
		}
		// Intentional postinstall poison may make Close fail. Retain its actual
		// return separately; this error-path control never claims clean Close.
		if closeErr := d.Close(); closeErr != nil {
			t.Logf("original checked Close after injected sync failure: %v", closeErr)
		}
	}()
	var firstErr error
	for _, req := range requests {
		err := combinedTestResult(t, req)
		req.m0.selected("RESULT")
		req.m0.returned(err)
		if !errors.Is(err, errTestSyncMetaFailpoint) {
			t.Fatalf("original physical sync failure changed: %v", err)
		}
		if firstErr == nil {
			firstErr = err
		} else if err != firstErr {
			t.Fatal("original batch error fanout changed")
		}
	}
	// Deferred validation observes the original worker join before any worker
	// fields, then records the original checked Close once. No live slot read.
	if !d.publicationPoisoned.Load() {
		t.Fatal("original postinstall publication poison missing")
	}
}
