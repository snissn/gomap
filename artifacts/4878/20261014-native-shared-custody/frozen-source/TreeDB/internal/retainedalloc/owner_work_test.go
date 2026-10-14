package retainedalloc

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
)

func TestRemovalWorkManyOriginalBindingsAndFutureGrowth(t *testing.T) {
	var o Owner
	o.Initialize(128)
	if err := o.Add(4096); err != nil {
		t.Fatal(err)
	}
	backing := make([]byte, 4096)
	budgets := make([]*testBudget, 80)
	captures := make([]*Enrollment, 80)
	for i := range budgets {
		budgets[i] = &testBudget{cap: 1 << 30}
		var err error
		captures[i], err = o.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	var receipt RemovalWork
	zero := &iterator.OrdinalScanWork{}
	before := o.Bytes()
	if done, e := o.RemoveWork(4096, &receipt, zero); done || e != nil || o.Bytes() != before {
		t.Fatal("refusal consumed storage")
	}
	// The actual producer alias is detached before its accounting delta.
	backing = nil
	_ = backing
	returns := 0
	for {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		done, e := o.RemoveWork(4096, &receipt, w)
		if e != nil {
			t.Fatal(e)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("budget exceeded")
		}
		o.mu.Lock()
		for b := o.head; b != nil; b = b.next {
			if b.lease.(*testLease).bytes < o.bytes {
				o.mu.Unlock()
				t.Fatal("cleanup under-admitted concurrent producer")
			}
		}
		o.mu.Unlock()
		if done {
			break
		}
		returns++
		if returns > 100 {
			t.Fatal("bounded refund did not finish")
		}
		if err := o.Add(64); err != nil {
			t.Fatal(err)
		}
		future := make([]byte, 64)
		_ = future
	}
	if returns < 2 {
		t.Fatal("many original bindings were drained synchronously")
	}
	if receipt.owner != nil || receipt.shrink.cursor != nil {
		t.Fatal("terminal receipt retained original owner")
	}
	for _, e := range captures {
		e.Close()
	}
	for _, b := range budgets {
		if b.bytes != 0 {
			t.Fatal("capture governor leaked")
		}
	}
}

func TestEnrollmentCloseWorkClearsOriginalAliasesBeforeRefund(t *testing.T) {
	var a, b Owner
	a.Initialize(256)
	b.Initialize(512)
	budget := &testBudget{cap: 1 << 20}
	e, err := EnrollPair(&a, &b, budget)
	if err != nil {
		t.Fatal(err)
	}
	var state EnrollmentReleaseWork
	before := budget.bytes
	if done, err := e.CloseWorkInto(&iterator.OrdinalScanWork{}, &state); done || err != nil || e.closed.Load() || budget.bytes != before {
		t.Fatal("zero work released capture")
	}
	bind := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := state.Bind(e, &bind); !ready || err != nil {
		t.Fatal("bind original enrollment pair", err)
	}
	// One record transfers the companion's exact remaining work to the external
	// receipt and scrubs that original Enrollment; it cannot traverse its leases.
	companion := e.companion
	w := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if done, err := state.ContinueWork(w); done || err != nil {
		t.Fatal("first bounded release", done, err)
	}
	if companion.owner != nil || companion.binding != nil || !companion.closed.Load() || state.companion.owner != &b {
		t.Fatal("refund remained in disposed enrollment")
	}
	if e.closed.Load() {
		t.Fatal("primary released before original companion cleanup")
	}
	first := budget.bytes
	e.Close()
	if budget.bytes != first {
		t.Fatal("ordinary close drained native-owned callback")
	}
	for calls := 0; ; calls++ {
		if calls > 100 {
			t.Fatal("capture did not finish")
		}
		done, err := state.ContinueWork(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if budget.bytes != 0 || e.owner != nil || e.binding != nil || e.companion != nil || state.primary.owner != nil || state.companion.owner != nil {
		t.Fatal("terminal aliases/refunds remain")
	}
	e.CloseAfterCleanup(errors.New("late duplicate cleanup report"))
	if err := e.ActivateProducer(); !errors.Is(err, ErrClosed) {
		t.Fatal("disposed enrollment reactivated", err)
	}
}

func TestRemovalWorkConcurrentOriginalGrowthAndEnrollment(t *testing.T) {
	var o Owner
	o.Initialize(128)
	if err := o.Add(4096); err != nil {
		t.Fatal(err)
	}
	captures := make([]*Enrollment, 80)
	for i := range captures {
		var err error
		captures[i], err = o.Enroll(&testBudget{cap: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
	}
	var receipt RemovalWork
	if done, err := o.RemoveWork(4096, &receipt, &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}); done || err != nil {
		t.Fatal("removal did not yield", done, err)
	}
	updates := make(chan error, 1)
	start := make(chan struct{})
	go func() {
		<-start
		for i := 0; i < 100; i++ {
			if err := o.Add(16); err != nil {
				updates <- err
				return
			}
			// A new ordinary capture shares the same original intrusive list while the
			// bounded cleanup cursor still borrows an older actual binding.
			e, err := o.Enroll(&testBudget{cap: 1 << 30})
			if err != nil {
				updates <- err
				return
			}
			e.Close()
			o.Remove(16)
		}
		updates <- nil
	}()
	close(start)
	for n := 0; ; n++ {
		if n > 1000 {
			t.Fatal("refund stalled")
		}
		done, err := o.RemoveWork(4096, &receipt, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if err := <-updates; err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	for b := o.head; b != nil; b = b.next {
		if b.cleanupRefs != 0 || b.lease.(*testLease).bytes < o.bytes {
			o.mu.Unlock()
			t.Fatal("original binding capture/admission was lost")
		}
	}
	o.mu.Unlock()
	for _, e := range captures {
		e.Close()
	}
}

func TestOrdinaryMetadataReleaseCreatesNoUnadmittedControl(t *testing.T) {
	var o Owner
	o.Initialize(128)
	e, err := o.Enroll(&testBudget{cap: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := o.Add(64); err != nil {
			panic(err)
		}
		o.Remove(64)
	})
	if allocs != 0 {
		t.Fatal("ordinary removal allocated cleanup control", allocs)
	}
	e.Close()
	captures := make([]*Enrollment, 101)
	for i := range captures {
		captures[i], err = o.Enroll(&testBudget{cap: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
	}
	i := 0
	allocs = testing.AllocsPerRun(100, func() { captures[i].Close(); i++ })
	if allocs != 0 {
		t.Fatal("ordinary enrollment close allocated cleanup control", allocs)
	}
}
