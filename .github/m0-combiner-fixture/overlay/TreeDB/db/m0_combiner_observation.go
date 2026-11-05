package db

// DIAGNOSTIC OVERLAY ONLY. Remove this file AND the sealed source overlay to
// recover the original product. Even nil observations change this binary's
// code/layout; no same-ELF zero-cost or performance acceptance claim is made.

import (
	"errors"
	"time"
)

const m0CombinerOrdinalLimit = 4096

type m0CombinerStamp struct {
	NS    int64
	Valid bool
}

type m0CombinerCaller struct {
	Entry, EnqueueBefore, EnqueueAfter m0CombinerStamp
	SelectBefore, SelectAfter, Return  m0CombinerStamp
	EnqueueStatus, Branch, Error       string
	Entered, Invalid                   bool
}

type m0CombinerWorker struct {
	Dequeue, ApplyBefore, ApplyAfter m0CombinerStamp
	WriteBefore, WriteAfter          m0CombinerStamp
	CloseBefore, CloseAfter          m0CombinerStamp
	SendBefore, SendAfter            m0CombinerStamp
	BatchSerial                      uint64
	BatchIndex, BatchSize, Selected  int
	Dispatch, WriteKind              string
	WriteError, CloseError           string
	IgnoredCloseError                bool
	Terminal, Invalid                bool
}

type m0CombinerSlot struct {
	// Immutable before the original request send. The pointer is private and
	// never exported; no request/DB/key/value/error object is retained here.
	owner     *m0CombinerOwner
	Iteration int
	Ordinal   int
	Kind      string
	Caller    m0CombinerCaller // Foreground writer only, then original join.
	Worker    m0CombinerWorker // Original combiner only, then original done.
}

type m0CombinerOwner struct {
	origin time.Time
	slots  [m0CombinerOrdinalLimit]m0CombinerSlot
	// Foreground writer only until its original join. Worker code NEVER reads
	// these mutable fields. done is borrowed under original combineMu.RLock.
	done        <-chan struct{}
	doneBound   bool
	doneInvalid bool
}

func newM0CombinerOwner() *m0CombinerOwner {
	o := &m0CombinerOwner{origin: time.Now()}
	for i := range o.slots {
		kind := "set"
		if i%2 != 0 {
			kind = "delete-range"
		}
		o.slots[i] = m0CombinerSlot{owner: o, Iteration: 0, Ordinal: i, Kind: kind}
	}
	return o
}

func (o *m0CombinerOwner) stamp() m0CombinerStamp {
	return m0CombinerStamp{NS: time.Since(o.origin).Nanoseconds(), Valid: true}
}

func (s *m0CombinerSlot) enter() *m0CombinerSlot {
	if s == nil {
		return nil
	}
	if s.owner == nil || s.Iteration != 0 || s.Ordinal < 0 || s.Ordinal >= m0CombinerOrdinalLimit || s.Ordinal%2 != 0 || s.Kind != "set" || s.Caller.Entered {
		s.Caller.Invalid = true
		return nil // Diagnostic refusal NEVER refuses/retries the original Set.
	}
	if s != &s.owner.slots[s.Ordinal] {
		s.Caller.Invalid = true
		return nil
	}
	s.Caller.Entered = true
	s.Caller.Entry = s.owner.stamp()
	return s
}

func (s *m0CombinerSlot) borrowDone(done <-chan struct{}) {
	if s == nil {
		return
	}
	o := s.owner
	if !o.doneBound {
		o.done, o.doneBound = done, true
	}
	if done == nil || done != o.done {
		o.doneInvalid = true
	}
}

func (s *m0CombinerSlot) enqueueBefore() {
	if s != nil {
		s.Caller.EnqueueBefore = s.owner.stamp()
	}
}

func (s *m0CombinerSlot) enqueueOutcome(status string) {
	if s != nil {
		s.Caller.EnqueueStatus = status
		if s.Caller.EnqueueBefore.Valid {
			s.Caller.EnqueueAfter = s.owner.stamp()
		}
	}
}

func (s *m0CombinerSlot) selectBefore() {
	if s != nil {
		s.Caller.SelectBefore = s.owner.stamp()
	}
}

func (s *m0CombinerSlot) selected(branch string) {
	if s != nil {
		s.Caller.SelectAfter = s.owner.stamp()
		s.Caller.Branch = branch
	}
}

func (s *m0CombinerSlot) fallback() {
	if s != nil {
		s.Caller.Branch = "DIRECT_FALLBACK"
	}
}

func m0CombinerErrorCategory(err error) string {
	if err == nil {
		return "NIL"
	}
	if errors.Is(err, errCommitCombinerClosed) {
		return "COMBINER_CLOSED"
	}
	if errors.Is(err, ErrReadOnly) {
		return "READ_ONLY"
	}
	return "OTHER" // No Error() call or payload/error object retention.
}

func (s *m0CombinerSlot) returned(err error) {
	if s != nil {
		s.Caller.Return = s.owner.stamp()
		s.Caller.Error = m0CombinerErrorCategory(err)
		if s.Caller.Branch == "" {
			s.Caller.Branch = "EARLY_RETURN"
		}
	}
}

func (s *m0CombinerSlot) dequeued() {
	if s != nil {
		if s.Worker.Dequeue.Valid {
			s.Worker.Invalid = true
		}
		s.Worker.Dequeue = s.owner.stamp()
	}
}

func m0BindBatch(batch []*commitCombineReq, serial uint64) {
	selected := 0
	for _, req := range batch {
		if req != nil && req.m0 != nil {
			selected++
		}
	}
	for i, req := range batch {
		if req == nil || req.m0 == nil {
			continue
		}
		w := &req.m0.Worker
		if w.BatchSerial != 0 || serial == 0 || len(batch) > commitCombineMaxBatch {
			w.Invalid = true
		}
		w.BatchSerial, w.BatchIndex, w.BatchSize, w.Selected = serial, i, len(batch), selected
	}
}

// One shared observation per batch boundary, copied to selected members only.
// Scans are cap64; unselected requests are never retained in a receipt.
func m0StampBatch(batch []*commitCombineReq, boundary string) {
	var owner *m0CombinerOwner
	var stamp m0CombinerStamp
	for _, req := range batch {
		if req == nil || req.m0 == nil {
			continue
		}
		s := req.m0
		if owner == nil {
			owner = s.owner
			stamp = owner.stamp()
		}
		w := &s.Worker
		if s.owner != owner {
			w.Invalid = true
		}
		var target *m0CombinerStamp
		switch boundary {
		case "APPLY_BEFORE":
			target = &w.ApplyBefore
		case "APPLY_AFTER":
			target = &w.ApplyAfter
		case "WRITE_BEFORE":
			target = &w.WriteBefore
		case "WRITE_AFTER":
			target = &w.WriteAfter
		case "CLOSE_BEFORE":
			target = &w.CloseBefore
		case "CLOSE_AFTER":
			target = &w.CloseAfter
		default:
			w.Invalid = true
		}
		if target != nil {
			if target.Valid {
				w.Invalid = true
			}
			*target = stamp
		}
	}
}

func m0BatchErrors(batch []*commitCombineReq, kind string, writeErr, closeErr error, ignored bool) {
	for _, req := range batch {
		if req != nil && req.m0 != nil {
			w := &req.m0.Worker
			w.WriteKind = kind
			w.WriteError, w.CloseError = m0CombinerErrorCategory(writeErr), m0CombinerErrorCategory(closeErr)
			w.IgnoredCloseError = ignored
		}
	}
}

func (s *m0CombinerSlot) sendBefore(dispatch string) {
	if s != nil {
		if s.Worker.SendBefore.Valid {
			s.Worker.Invalid = true
		}
		s.Worker.Dispatch = dispatch
		s.Worker.SendBefore = s.owner.stamp()
	}
}

func (s *m0CombinerSlot) sendAfter() {
	if s != nil {
		s.Worker.SendAfter = s.owner.stamp()
		s.Worker.Terminal = true
	}
}

// Call ONLY after the original foreground join. No worker field is read here.
// This nonblocking observation is NOT an observer-induced stop/join or retry.
func (o *m0CombinerOwner) originalDoneObserved(writerJoined bool) bool {
	if !writerJoined || !o.doneBound || o.doneInvalid || o.done == nil {
		return false
	}
	select {
	case <-o.done:
		return true
	default:
		return false
	}
}
