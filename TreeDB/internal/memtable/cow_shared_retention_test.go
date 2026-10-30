package memtable

import (
	"errors"
	"sync"
	"testing"
	"unsafe"
)

func TestCOWSharedRetentionSurvivesBudgetClose(t *testing.T) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	charge := COWAllocationCharge(4097)
	before := budget.Stats()
	owner, err := NewCOWSharedRetention(budget, charge)
	if err != nil {
		t.Fatal(err)
	}
	live := budget.Stats()
	want := COWAllocationCharge(charge+COWAllocationCharge(uint64(unsafe.Sizeof(COWSharedRetention{})))) + COWAllocationCharge(uint64(unsafe.Sizeof(COWExternalLease{})))
	if live.ExternalBytes-before.ExternalBytes != want || live.ExternalLeases != before.ExternalLeases+1 {
		t.Fatalf("unexpected admission before=%+v live=%+v want=%d", before, live, want)
	}
	if !owner.Retain() {
		t.Fatal("retain while live")
	}
	budget.Close()
	owner.Release()
	if got := budget.Stats(); got.ExternalBytes != want || got.ExternalLeases != 1 {
		t.Fatalf("first edge refunded shared charge: %+v", got)
	}
	owner.Release()
	if owner.Retain() {
		t.Fatal("resurrected terminal owner")
	}
	owner.Release()
	if got := budget.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("last edge did not refund: %+v", got)
	}
}

func TestCOWSharedRetentionRefusalDoesNotChangeBudget(t *testing.T) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	before := budget.Stats()
	for _, charge := range []uint64{^uint64(0), budget.Limits().MaxInFlightBytes} {
		owner, err := NewCOWSharedRetention(budget, charge)
		if owner != nil || !errors.Is(err, ErrCOWCapacity) {
			t.Fatalf("charge=%d owner=%v error=%v", charge, owner, err)
		}
		if got := budget.Stats(); got != before {
			t.Fatalf("refusal changed budget before=%+v after=%+v", before, got)
		}
	}
}

func TestCOWSharedRetentionConcurrentEdges(t *testing.T) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewCOWSharedRetention(budget, COWAllocationCharge(128))
	if err != nil {
		budget.Close()
		t.Fatal(err)
	}
	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if !owner.Retain() {
					t.Error("retain while root edge held")
					return
				}
				owner.Release()
			}
		}()
	}
	wg.Wait()
	budget.Close()
	owner.Release()
	if got := budget.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("remaining reservation: %+v", got)
	}
}

func TestCOWSharedRetentionMaintenanceCloseRace(t *testing.T) {
	parent, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.ReserveMaintenance(32 << 10)
	if err != nil {
		parent.Close()
		t.Fatal(err)
	}
	owner, err := NewCOWSharedRetention(child, COWAllocationCharge(256))
	if err != nil {
		parent.Close()
		t.Fatal(err)
	}
	if !owner.Retain() {
		parent.Close()
		owner.Release()
		t.Fatal("retain")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); parent.Close() }()
	go func() { defer wg.Done(); owner.Release() }()
	wg.Wait()
	if got := parent.Stats(); got.ExternalLeases != 1 {
		owner.Release()
		t.Fatalf("parent reservation lost while owner live: %+v", got)
	}
	owner.Release()
	if got := parent.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("parent reservation not released: %+v", got)
	}
	if got := child.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("child reservation not released: %+v", got)
	}
}

// A competing Retain can either win its own edge or observe terminal release.
// This test retains only the pointer, never dereferences any freed backing.
func TestCOWSharedRetentionRetainFinalReleaseRace(t *testing.T) {
	for i := 0; i < 100; i++ {
		budget, err := NewCOWBudget(DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		owner, err := NewCOWSharedRetention(budget, COWAllocationCharge(64))
		if err != nil {
			budget.Close()
			t.Fatal(err)
		}
		start := make(chan struct{})
		retained := make(chan bool, 1)
		released := make(chan struct{})
		go func() { <-start; retained <- owner.Retain() }()
		go func() { <-start; owner.Release(); close(released) }()
		close(start)
		keep := <-retained
		<-released
		if keep {
			owner.Release()
		}
		budget.Close()
		if owner.Retain() {
			t.Fatal("resurrected owner")
		}
		if got := budget.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
			t.Fatalf("remaining charge: %+v", got)
		}
	}
}
