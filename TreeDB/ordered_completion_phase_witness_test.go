package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const orderedACKWitnessLimit = 64 << 10

// This child-local observer reads only atomic phase state and goroutine stacks.
// It adds no persistence or storage/producer locks. A passing child stops and
// joins it after both normal closes; a stalled child emits at most one receipt.
type orderedACKPhaseWitness struct {
	scenario string
	started  time.Time
	phase    atomic.Pointer[string]
	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func startOrderedACKPhaseWitness(scenario string, at time.Time, out io.Writer) *orderedACKPhaseWitness {
	w := &orderedACKPhaseWitness{scenario: scenario, started: time.Now(), stopCh: make(chan struct{}), done: make(chan struct{})}
	w.mark("setup")
	go func() {
		defer close(w.done)
		timer := time.NewTimer(time.Until(at))
		defer timer.Stop()
		select {
		case <-w.stopCh:
			return
		case <-timer.C:
		}
		select {
		case <-w.stopCh:
			return
		default:
		}
		_, _ = out.Write(w.snapshot(runtime.Stack))
	}()
	return w
}

func (w *orderedACKPhaseWitness) mark(phase string) {
	if w != nil {
		w.phase.Store(&phase)
	}
}

func (w *orderedACKPhaseWitness) stopAndJoin() {
	w.stopOnce.Do(func() { close(w.stopCh) })
	<-w.done
}

func (w *orderedACKPhaseWitness) snapshot(stack func([]byte, bool) int) []byte {
	// Reserve ample header space; bounded labels keep the entire receipt, not
	// just its stack portion, below 64 KiB. A full buffer is conservatively
	// reported as truncated because runtime.Stack does not return a needed size.
	buf := make([]byte, orderedACKWitnessLimit-512)
	n := stack(buf, true)
	header := fmt.Sprintf("ordered-ack pre-deadline scenario=%.32s phase=%.64s elapsed=%s stack_truncated=%t\n", w.scenario, *w.phase.Load(), time.Since(w.started), n == len(buf))
	return append([]byte(header), buf[:n]...)
}

func closeOrderedACKImage(label string, closeImage func() error) error {
	if err := closeImage(); err != nil {
		return fmt.Errorf("%s close: %w", label, err)
	}
	return nil
}

func TestOrderedACKPhaseWitnessStop(t *testing.T) {
	var out bytes.Buffer
	w := startOrderedACKPhaseWitness("control", time.Now().Add(time.Hour), &out)
	w.stopAndJoin()
	w.stopAndJoin() // Joining is idempotent; it cannot emit after return.
	if out.Len() != 0 {
		t.Fatalf("stopped witness emitted %d bytes", out.Len())
	}
}

func TestOrderedACKPhaseWitnessSnapshot(t *testing.T) {
	var out bytes.Buffer
	w := startOrderedACKPhaseWitness("control", time.Now().Add(time.Hour), &out)
	defer w.stopAndJoin()
	w.mark("writable-open")
	for _, full := range []bool{false, true} {
		receipt := w.snapshot(func(buf []byte, all bool) int {
			if !all {
				t.Fatal("snapshot omitted other goroutines")
			}
			if full {
				for i := range buf {
					buf[i] = 'x'
				}
				return len(buf)
			}
			return copy(buf, "stack\n")
		})
		if len(receipt) > orderedACKWitnessLimit || !bytes.Contains(receipt, []byte("phase=writable-open")) || !bytes.Contains(receipt, []byte(fmt.Sprintf("stack_truncated=%t", full))) {
			t.Fatalf("invalid bounded phase receipt: bytes=%d", len(receipt))
		}
	}
}

func TestOrderedACKPhaseWitnessCloseReporting(t *testing.T) {
	failure := errors.New("close control")
	for _, want := range []error{nil, failure} {
		calls := 0
		err := closeOrderedACKImage("reopened", func() error { calls++; return want })
		if calls != 1 || !errors.Is(err, want) || (err != nil && !strings.Contains(err.Error(), "reopened close: close control")) {
			t.Fatalf("close result=%v calls=%d want=%v", err, calls, want)
		}
	}
}
