package collections

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These four requests enter from low pressure, then finish against a generation
// made pressured by a bounded disjoint suffix. Their earlier observations do
// not reserve space; original final admission must compose all four updates.
func TestNativeOrdinaryGenerationAdmittedCohort(t *testing.T) {
	const writers = 4
	owner, backend, c, leaveOpen := openNativeGenerationFixture(t)
	original := []byte(`{"tenant":"cohort","payload":"old"}`)
	var ids [writers][]byte
	var replacements [writers][]byte
	for i := range writers {
		ids[i] = []byte(fmt.Sprintf("cohort-%06d", i))
		replacements[i] = append(append([]byte(`{"tenant":"cohort","payload":"`), bytes.Repeat([]byte{byte('b' + i)}, 32<<10)...), []byte(`"}`)...)
		if got, err := c.Insert(ids[i], original); err != nil || !bytes.Equal(got, ids[i]) {
			t.Fatalf("initial row %d: id=%q err=%v", i, got, err)
		}
	}
	held, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	})
	initialBasis, valid := held.capture.Snapshot().StateToken()
	if !valid {
		t.Fatal("held original rows have no physical basis")
	}
	initial, err := backend.ObserveNativeProducer()
	if err != nil || initial.UnderPressure {
		t.Fatalf("cohort did not begin at low pressure: %+v err=%v", initial, err)
	}
	type completion struct {
		writer            int
		matched, modified bool
		err               error
	}
	entered := make(chan int, writers)
	done := make(chan completion, writers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	var calls [writers]atomic.Int32
	completed := 0
	// Release and join callbacks before the fixture closes manager/database,
	// including when a pressure or correctness assertion fails.
	defer func() {
		releaseAll()
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		for completed < writers {
			select {
			case result := <-done:
				completed++
				if result.err != nil {
					t.Errorf("cleanup join writer %d: %v", result.writer, result.err)
				}
			case <-deadline.C:
				leaveOpen()
				t.Error("admitted cohort did not finish within cleanup bound")
				return
			}
		}
	}()
	for i := range writers {
		go func(i int) {
			matched, modified, updateErr := c.Update(ids[i], func(current []byte) ([]byte, bool, error) {
				if n := calls[i].Add(1); n != 1 {
					return nil, false, fmt.Errorf("writer %d callback replayed: calls=%d", i, n)
				}
				if !bytes.Equal(current, original) {
					return nil, false, fmt.Errorf("writer %d callback received unexpected original bytes", i)
				}
				entered <- i
				<-release
				return replacements[i], true, nil
			})
			done <- completion{writer: i, matched: matched, modified: modified, err: updateErr}
		}(i)
	}
	var seen [writers]bool
	readyDeadline := time.NewTimer(10 * time.Second)
	defer readyDeadline.Stop()
	for n := 0; n < writers; n++ {
		select {
		case i := <-entered:
			if seen[i] {
				t.Fatalf("writer %d entered its callback twice", i)
			}
			seen[i] = true
		case result := <-done:
			completed++
			t.Fatalf("writer %d completed before cohort release: matched=%t modified=%t err=%v", result.writer, result.matched, result.modified, result.err)
		case <-readyDeadline.C:
			t.Fatalf("only %d/%d callbacks entered together", n, writers)
		}
	}
	// Fill only while observations are below the watermark. The insertion that
	// crosses it completes; the next producer's maintenance has not run yet.
	fill := append(append([]byte(`{"tenant":"fill","payload":"`), bytes.Repeat([]byte("a"), 32<<10)...), []byte(`"}`)...)
	var fillerIDs [][]byte
	pressured, err := backend.ObserveNativeProducer()
	if err != nil || pressured.UnderPressure {
		t.Fatalf("waiting cohort unexpectedly exhausted initial headroom: %+v err=%v", pressured, err)
	}
	for i := 0; i < 128 && !pressured.Semantic.GenerationUnderPressure; i++ {
		id := []byte(fmt.Sprintf("fill-%06d", i))
		if got, err := c.Insert(id, fill); err != nil || !bytes.Equal(got, id) {
			t.Fatalf("bounded disjoint fill %d: id=%q err=%v", i, got, err)
		}
		fillerIDs = append(fillerIDs, id)
		pressured, err = backend.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
	}
	if !pressured.Semantic.GenerationUnderPressure || !pressured.UnderPressure || len(fillerIDs) == 0 || pressured.LogicalSequence <= initial.LogicalSequence {
		t.Fatalf("bounded fill did not reach actual ordinary generation pressure: rows=%d initial=%+v current=%+v", len(fillerIDs), initial, pressured)
	}
	if pressured.Basis.CommitSeq != initialBasis.CommitSeq {
		t.Fatalf("fill unexpectedly maintained before releasing stale cohort: initial=%+v pressured=%+v", initialBasis, pressured.Basis)
	}
	releaseAll()
	finishDeadline := time.NewTimer(10 * time.Second)
	defer finishDeadline.Stop()
	for completed < writers {
		select {
		case result := <-done:
			completed++
			if result.err != nil || !result.matched || !result.modified {
				t.Errorf("admitted writer %d: matched=%t modified=%t err=%v", result.writer, result.matched, result.modified, result.err)
			}
		case <-finishDeadline.C:
			t.Fatal("released cohort did not complete within bound")
		}
	}
	for i := range writers {
		if n := calls[i].Load(); n != 1 {
			t.Errorf("writer %d callback count=%d want=1", i, n)
		}
	}
	verify := func() {
		t.Helper()
		for i := range writers {
			if got, err := c.Get(ids[i]); err != nil || !bytes.Equal(got, replacements[i]) {
				t.Errorf("writer %d exact value: bytes=%d err=%v", i, len(got), err)
			}
			if got, err := held.Get(ids[i]); err != nil || !bytes.Equal(got, original) {
				t.Errorf("held original %d changed: bytes=%d err=%v", i, len(got), err)
			}
		}
		for _, id := range fillerIDs {
			if got, err := c.Get(id); err != nil || !bytes.Equal(got, fill) {
				t.Errorf("filler ACK %q changed: bytes=%d err=%v", id, len(got), err)
			}
		}
	}
	verify()
	sentinelID := []byte("sentinel")
	sentinel := []byte(`{"tenant":"sentinel","payload":"after-cohort"}`)
	if got, err := c.Insert(sentinelID, sentinel); err != nil || !bytes.Equal(got, sentinelID) {
		t.Fatalf("later public sentinel: id=%q err=%v", got, err)
	}
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	afterBasis, valid := current.capture.Snapshot().StateToken()
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if !valid || afterBasis.CommitSeq <= pressured.Basis.CommitSeq {
		t.Errorf("later producer did not perform physical maintenance: pressured=%+v after=%+v", pressured.Basis, afterBasis)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("checked checkpoint after cohort/sentinel: %v", err)
	}
	verify()
	if got, err := c.Get(sentinelID); err != nil || !bytes.Equal(got, sentinel) {
		t.Errorf("sentinel changed across checkpoint: bytes=%d err=%v", len(got), err)
	}
	t.Logf("four original callbacks completed once after %d disjoint fill ACKs; physical basis %d -> %d", len(fillerIDs), pressured.Basis.CommitSeq, afterBasis.CommitSeq)
}
