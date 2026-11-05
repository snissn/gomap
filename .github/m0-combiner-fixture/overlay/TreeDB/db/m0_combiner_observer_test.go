package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime/trace"
	"testing"
)

// The maintained runner admits exactly one selected benchmark/count/iteration.
// This single root is assigned before ResetTimer and never cleared, including
// failed join/Close. It performs no request matching or producer notification.
var m0ServiceProcessOwner *m0CombinerOwner

type m0CombinerRow struct {
	Iteration          int
	Ordinal            int
	Kind               string
	OriginalRegion     string
	OriginalDurationNS int64
	Status             string
	Caller             m0CombinerCaller
	Worker             m0CombinerWorker
}

func m0StampOrder(stamps ...m0CombinerStamp) bool {
	var previous int64
	for i, stamp := range stamps {
		if !stamp.Valid || stamp.NS < 0 || (i != 0 && stamp.NS < previous) {
			return false
		}
		previous = stamp.NS
	}
	return true
}

func m0KnownError(category string) bool {
	return category == "NIL" || category == "COMBINER_CLOSED" || category == "READ_ONLY" || category == "OTHER"
}

// ONLY called after the original foreground join AND original worker done.
// E1>D and caller receive<S1 are legal; no cross-owner bracket ordering test.
func m0ValidateServiceSlot(s *m0CombinerSlot) (string, error) {
	if s.Ordinal < 0 || s.Ordinal >= m0CombinerOrdinalLimit || s.Iteration != 0 {
		return "INCOMPLETE", errors.New("M0 service identity outside fixed population")
	}
	c, w := s.Caller, s.Worker
	if s.Ordinal%2 != 0 {
		if s.Kind != "delete-range" || c != (m0CombinerCaller{}) || w != (m0CombinerWorker{}) {
			return "INCOMPLETE", errors.New("M0 non-combiner ordinal acquired service fields")
		}
		return "NON_COMBINER", nil
	}
	if s.Kind != "set" || !c.Entered || c.Invalid || w.Invalid || !m0StampOrder(c.Entry, c.Return) || !m0KnownError(c.Error) {
		return "INCOMPLETE", errors.New("M0 service caller incomplete/invalid")
	}
	if c.EnqueueBefore.Valid && !m0StampOrder(c.Entry, c.EnqueueBefore, c.EnqueueAfter, c.Return) {
		return "INCOMPLETE", errors.New("M0 caller enqueue observations regress or are absent")
	}
	if c.EnqueueStatus != "ENQUEUED" {
		if w != (m0CombinerWorker{}) || c.SelectBefore.Valid || c.SelectAfter.Valid {
			return "INCOMPLETE", errors.New("M0 non-enqueued request acquired worker/select fields")
		}
		switch c.EnqueueStatus {
		case "NO_CHANNEL", "SATURATED_FALLBACK", "SEND_FALLBACK":
			if c.Branch == "DIRECT_FALLBACK" && c.EnqueueBefore.Valid == (c.EnqueueStatus == "SEND_FALLBACK") && c.EnqueueAfter.Valid == c.EnqueueBefore.Valid {
				return "DIRECT_FALLBACK", nil
			}
		case "SATURATED_STOP", "SEND_STOP":
			if c.Branch == "EARLY_RETURN" && c.Error == "COMBINER_CLOSED" && c.EnqueueBefore.Valid == (c.EnqueueStatus == "SEND_STOP") && c.EnqueueAfter.Valid == c.EnqueueBefore.Valid {
				return "ORIGINAL_ERROR", nil
			}
		case "":
			if c.Branch == "EARLY_RETURN" && c.Error != "NIL" && !c.EnqueueBefore.Valid && !c.EnqueueAfter.Valid {
				return "ORIGINAL_ERROR", nil
			}
		}
		return "INCOMPLETE", errors.New("M0 unknown/inconsistent non-enqueued branch")
	}
	if !m0StampOrder(c.Entry, c.EnqueueBefore, c.EnqueueAfter, c.SelectBefore, c.SelectAfter, c.Return) || (c.Branch != "RESULT" && c.Branch != "STOP") {
		return "INCOMPLETE", errors.New("M0 enqueued caller select incomplete/invalid")
	}
	if c.Branch == "STOP" && c.Error != "COMBINER_CLOSED" {
		return "INCOMPLETE", errors.New("M0 stop branch changed original error")
	}
	if !w.Terminal || !m0StampOrder(w.Dequeue, w.SendBefore, w.SendAfter) {
		return "INCOMPLETE", errors.New("M0 worker terminal/result delivery absent")
	}
	switch w.Dispatch {
	case "APPLIED":
		if w.BatchSerial == 0 || w.BatchSize < 1 || w.BatchSize > commitCombineMaxBatch || w.BatchIndex < 0 || w.BatchIndex >= w.BatchSize || w.Selected < 1 || w.Selected > w.BatchSize {
			return "INCOMPLETE", errors.New("M0 batch identity/bounds invalid")
		}
		if !m0StampOrder(w.Dequeue, w.ApplyBefore, w.CloseBefore, w.CloseAfter, w.ApplyAfter, w.SendBefore, w.SendAfter) || !m0KnownError(w.WriteError) || !m0KnownError(w.CloseError) {
			return "INCOMPLETE", errors.New("M0 apply/Close/error observations incomplete")
		}
		if w.WriteKind == "SETUP" {
			if !w.IgnoredCloseError || w.WriteBefore.Valid || w.WriteAfter.Valid || w.WriteError == "NIL" {
				return "INCOMPLETE", errors.New("M0 setup-error original ignored Close contract changed")
			}
		} else if (w.WriteKind != "WRITE" && w.WriteKind != "WRITE_SYNC") || w.IgnoredCloseError || !m0StampOrder(w.ApplyBefore, w.WriteBefore, w.WriteAfter, w.CloseBefore) {
			return "INCOMPLETE", errors.New("M0 original Write/WriteSync observations incomplete")
		}
		if c.Branch == "STOP" {
			return "STOP_RETURN_PENDING_SERVICE", nil
		}
		wantError := w.WriteError
		if wantError == "NIL" {
			wantError = w.CloseError
		}
		if c.Error != wantError {
			return "INCOMPLETE", errors.New("M0 original result error precedence/category changed")
		}
		return "APPLIED_RESULT_RECEIVED", nil
	case "DRAINED":
		if w.BatchSerial != 0 || w.BatchIndex != 0 || w.BatchSize != 0 || w.Selected != 0 || w.ApplyBefore.Valid || w.ApplyAfter.Valid || w.WriteBefore.Valid || w.WriteAfter.Valid || w.CloseBefore.Valid || w.CloseAfter.Valid || w.WriteKind != "" || w.WriteError != "" || w.CloseError != "" || w.IgnoredCloseError || c.Error != "COMBINER_CLOSED" {
			return "INCOMPLETE", errors.New("M0 drained request acquired application evidence")
		}
		return "DRAINED_NOT_APPLIED", nil
	default:
		return "INCOMPLETE", errors.New("M0 unknown dispatch")
	}
}

func m0ServiceRows(o *m0CombinerOwner, ordinals *m0OrdinalObserver) ([]m0CombinerRow, error) {
	rows := make([]m0CombinerRow, m0CombinerOrdinalLimit)
	type batchEvidence struct {
		worker m0CombinerWorker
		seen   [commitCombineMaxBatch]bool
		count  int
	}
	batches := make(map[uint64]*batchEvidence)
	for i := range o.slots {
		s := &o.slots[i]
		if s.owner != o || s.Ordinal != i || ordinals.records[i].Ordinal != i || ordinals.records[i].Kind != s.Kind {
			return nil, fmt.Errorf("M0 ordinal/service binding mismatch at %d", i)
		}
		status, err := m0ValidateServiceSlot(s)
		if err != nil {
			return nil, fmt.Errorf("M0 service ordinal %d: %w", i, err)
		}
		if s.Worker.Dispatch == "APPLIED" {
			w := s.Worker
			evidence := batches[w.BatchSerial]
			if evidence == nil {
				evidence = &batchEvidence{worker: w}
				batches[w.BatchSerial] = evidence
			}
			same := evidence.worker
			if evidence.seen[w.BatchIndex] || same.BatchSize != w.BatchSize || same.Selected != w.Selected || same.ApplyBefore != w.ApplyBefore || same.ApplyAfter != w.ApplyAfter || same.WriteBefore != w.WriteBefore || same.WriteAfter != w.WriteAfter || same.CloseBefore != w.CloseBefore || same.CloseAfter != w.CloseAfter || same.WriteKind != w.WriteKind || same.WriteError != w.WriteError || same.CloseError != w.CloseError || same.IgnoredCloseError != w.IgnoredCloseError {
				return nil, errors.New("M0 duplicate batch position or conflicting shared service receipt")
			}
			evidence.seen[w.BatchIndex] = true
			evidence.count++
		}
		rows[i] = m0CombinerRow{s.Iteration, s.Ordinal, s.Kind, ordinals.records[i].Region, ordinals.records[i].DurationNS, status, s.Caller, s.Worker}
	}
	for _, evidence := range batches {
		if evidence.count != evidence.worker.Selected {
			return nil, errors.New("M0 batch selected-member population incomplete")
		}
	}
	return rows, nil
}

func (o *m0OrdinalObserver) finishCombinerService(b *testing.B, closeErr error) {
	if o.service == nil {
		return
	}
	if !o.timerStopped {
		b.Error("M0 service receipt withheld: original timer stop unreached")
		return // No diagnostic serialization/I/O inside the original timer.
	}
	value := struct {
		Schema, Status, Reason string
		DiagnosticOnly         bool
		OriginalWriterJoined   bool
		OriginalDoneObserved   bool
		OriginalCloseReturned  bool
		OriginalCloseError     string
		Rows                   []m0CombinerRow
		Interpretation         string
	}{Schema: "m0-combiner-service-observer-v1", Status: "INCOMPLETE", DiagnosticOnly: true,
		OriginalWriterJoined: o.joined, OriginalCloseReturned: true, OriginalCloseError: m0CombinerErrorCategory(closeErr),
		Interpretation: "Instrumented diagnostic only. One Go monotonic origin; observation brackets are not primitive transition timestamps. E1 may exceed dequeue and receive may precede send-after. Dequeue delay has no positive lower-bound interpretation. Batch service includes scheduling; nested Write/Close intervals and shared batch rows must not be summed as exclusive per-request service. Original closed trace/source/ELF binding remains external. No M0 CV acceptance or CPU/lock-owner causal claim."}
	value.OriginalDoneObserved = o.service.originalDoneObserved(o.joined)
	var validationErr error
	if !value.OriginalDoneObserved {
		// Do NOT inspect or serialize ANY worker fields. Process retention root
		// remains live even if original Close returned an error or failed to join.
		validationErr = errors.New("M0 original writer/worker joins unavailable; rows withheld and owner retained")
	} else if err := o.validate(closeErr, b.Failed(), trace.IsEnabled()); err != nil {
		validationErr = err
	} else {
		value.Rows, validationErr = m0ServiceRows(o.service, o)
		if validationErr == nil {
			value.Status = "SERVICE_ROWS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING"
		}
	}
	if validationErr != nil {
		value.Reason = validationErr.Error()
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		err = m0OrdinalWrite(filepath.Join(o.dir, "m0-combiner-service.json"), data)
	}
	if err != nil {
		b.Error("M0 service receipt write failed", err)
	}
	if validationErr != nil {
		b.Error(validationErr)
	}
}
