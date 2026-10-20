package retainedalloc

import (
	"errors"
	"testing"
)

func TestRemovalManyOriginalBindingsAndFutureGrowth(t *testing.T) {
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
	if err := o.Add(64); err != nil {
		t.Fatal(err)
	}
	backing = nil
	_ = backing
	o.Remove(4096)
	o.mu.Lock()
	for b := o.head; b != nil; b = b.next {
		if b.cleanupRefs != 0 || b.lease.(*testLease).bytes < o.bytes {
			o.mu.Unlock()
			t.Fatal("original governor lost admission")
		}
	}
	o.mu.Unlock()
	for _, e := range captures {
		e.Close()
	}
	for _, b := range budgets {
		if b.bytes != 0 {
			t.Fatal("capture governor leaked")
		}
	}
}
func TestEnrollmentCloseClearsOriginalAliasesBeforeRefund(t *testing.T) {
	var a, b Owner
	a.Initialize(256)
	b.Initialize(512)
	budget := &testBudget{cap: 1 << 20}
	e, err := EnrollPair(&a, &b, budget)
	if err != nil {
		t.Fatal(err)
	}
	companion := e.companion
	e.Close()
	if budget.bytes != 0 || e.owner != nil || e.binding != nil || e.companion != nil || companion.owner != nil || companion.binding != nil || !companion.closed.Load() {
		t.Fatal("terminal aliases/refunds remain")
	}
	e.CloseAfterCleanup(errors.New("late duplicate cleanup report"))
	if err := e.ActivateProducer(); !errors.Is(err, ErrClosed) {
		t.Fatal("disposed enrollment reactivated", err)
	}
}
func TestRemovalConcurrentOriginalGrowthAndEnrollment(t *testing.T) {
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
	updates := make(chan error, 1)
	start := make(chan struct{})
	go func() {
		<-start
		for i := 0; i < 100; i++ {
			if err := o.Add(16); err != nil {
				updates <- err
				return
			}
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
	o.Remove(4096)
	if err := <-updates; err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	for b := o.head; b != nil; b = b.next {
		if b.cleanupRefs != 0 || b.lease.(*testLease).bytes < o.bytes {
			o.mu.Unlock()
			t.Fatal("original binding admission lost")
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
