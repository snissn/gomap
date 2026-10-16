package db

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func TestOriginalCleanupWrappedSiblingReceiptsRetainGraph(t *testing.T) {
	originalA := originalRecoverySetWithPhysicalCloseFault(t)
	originalB := originalRecoverySetWithPhysicalCloseFault(t)
	a, b := originalA.ReleaseWithError(), originalB.ReleaseWithError()
	if !errors.Is(a, os.ErrClosed) || !errors.Is(b, os.ErrClosed) {
		t.Fatal("missing genuine original physical refusals", a, b)
	}
	historical := errors.New("already-linked original sibling")
	a.(interface{ LinkPriorCleanupError(error) }).LinkPriorCleanupError(historical)
	prior := errors.New("preexisting unrelated DB background error")
	d := &DB{bgErr: prior}
	incoming := fmt.Errorf("original incoming wrapper: %w", errors.Join(a, b))
	d.retainOriginalCleanupError(incoming)
	if d.backgroundError() != incoming {
		t.Fatal("incoming wrapper graph not retained")
	}
	for _, target := range []error{a, b, historical, prior, os.ErrClosed} {
		if !errors.Is(d.backgroundError(), target) {
			t.Fatalf("lost original sibling %v", target)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				d.retainOriginalCleanupError(incoming)
				retained := d.backgroundError()
				if retained != incoming || !errors.Is(retained, prior) || !errors.Is(retained, a) || !errors.Is(retained, b) {
					t.Error("repeat lost original graph")
				}
				_ = errors.Is(retained, errors.New("absent target"))
			}
		}()
	}
	wg.Wait()
	// A new physical original joins the same retained graph; already-retained
	// siblings cannot be linked backwards into a cycle.
	c := originalRecoverySetWithPhysicalCloseFault(t).ReleaseWithError()
	next := fmt.Errorf("next joined original: %w", errors.Join(incoming, c))
	d.retainOriginalCleanupError(next)
	for _, target := range []error{a, b, c, historical, prior, os.ErrClosed} {
		if !errors.Is(d.backgroundError(), target) {
			t.Fatalf("late sibling lost %v", target)
		}
	}
	if errors.Is(d.backgroundError(), errors.New("absent final target")) {
		t.Fatal("false error match")
	}
}

// Error methods may inspect or report DB state while an outer original receipt
// is being planned. They must run outside bgErrMu; no stale edge may be bound.
type originalReentrantCleanupError struct {
	db      *DB
	cause   error
	nested  error
	entered atomic.Bool
}

func (e *originalReentrantCleanupError) Error() string { return "original reentrant cleanup wrapper" }
func (e *originalReentrantCleanupError) Unwrap() error { return e.cause }
func (e *originalReentrantCleanupError) Is(error) bool {
	_ = e.db.backgroundError()
	if e.nested != nil && e.entered.CompareAndSwap(false, true) {
		e.db.retainOriginalCleanupError(e.nested)
	}
	return false
}
func TestOriginalCleanupReentrantTraversalAndConcurrentPublication(t *testing.T) {
	prior := errors.New("preexisting original background error")
	d := &DB{bgErr: prior}
	a := originalRecoverySetWithPhysicalCloseFault(t).ReleaseWithError()
	b := originalRecoverySetWithPhysicalCloseFault(t).ReleaseWithError()
	c := originalRecoverySetWithPhysicalCloseFault(t).ReleaseWithError()
	incoming := &originalReentrantCleanupError{db: d, cause: fmt.Errorf("physical original wrapper: %w", a), nested: b}
	d.reportError(incoming)
	if !incoming.entered.Load() {
		t.Fatal("reentrant traversal did not run")
	}
	for _, target := range []error{a, b, prior, os.ErrClosed} {
		if !errors.Is(d.backgroundError(), target) {
			t.Fatalf("reentrant original lost %v", target)
		}
	}
	d.notifyError = func(err error) {
		retained := d.backgroundError()
		if !errors.Is(retained, err) {
			t.Error("notification ran before original custody publication")
		}
		d.retainOriginalCleanupError(err)
	}
	d.reportError(fmt.Errorf("next physical original: %w", c))
	for _, target := range []error{a, b, c, prior, os.ErrClosed} {
		if !errors.Is(d.backgroundError(), target) {
			t.Fatalf("notification original lost %v", target)
		}
	}
	var wg sync.WaitGroup
	var originals [8]error
	for i := 0; i < 8; i++ {
		original := originalRecoverySetWithPhysicalCloseFault(t).ReleaseWithError()
		originals[i] = original
		wg.Add(1)
		go func() { defer wg.Done(); d.retainOriginalCleanupError(fmt.Errorf("concurrent original: %w", original)) }()
	}
	wg.Wait()
	for _, original := range originals {
		if !errors.Is(d.backgroundError(), original) {
			t.Fatal("concurrent original sibling lost")
		}
	}
	if !errors.Is(d.backgroundError(), prior) || errors.Is(d.backgroundError(), errors.New("absent original")) {
		t.Fatal("invalid original graph traversal")
	}
}
