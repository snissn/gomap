package collections

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNativePreparedSideOwnerAdmissionAndCompletion(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(budget.Close)
	registry := rootpublication.NewIdentityPinRegistry()
	t.Cleanup(registry.Close)
	probe, err := newNativePreparedSideOwner(budget, registry, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !probe.finished {
			if err := probe.finish(nil); err != nil {
				t.Errorf("finish probe: %v", err)
			}
		}
	})
	minimum := probe.idle
	if err := probe.finish(nil); err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	ownerCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(nativePreparedSideOwner{})))
	if candidate, e := newNativePreparedSideOwner(budget, registry, ownerCharge-1); candidate != nil || !errors.Is(e, retainedalloc.ErrCapacity) {
		t.Fatalf("unfunded containing owner: candidate=%v error=%v", candidate, e)
	}
	refused, err := newNativePreparedSideOwner(budget, registry, minimum-1)
	if refused != nil || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("short limit: owner=%v error=%v", refused, err)
	}
	if got := budget.Stats(); got != before {
		t.Fatalf("refusal changed COW budget: before=%+v after=%+v", before, got)
	}
	owner, err := newNativePreparedSideOwner(budget, registry, minimum)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !owner.finished {
			if err := owner.finish(nil); err != nil {
				t.Errorf("finish owner: %v", err)
			}
		}
	})
	live := budget.Stats()
	if err := owner.metadata.AddPending(1); err == nil {
		owner.metadata.RemovePending(1)
		t.Fatal("exact limit accepted additional metadata")
	} else if !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("exact limit accepted additional metadata: %v", err)
	}
	if got := budget.Stats(); got != live {
		t.Fatalf("fixed-cap refusal changed original COW budget: before=%+v after=%+v", live, got)
	}
	if err := owner.finish(nil); err != nil {
		t.Fatal(err)
	}
	if err := owner.finish(nil); err != nil {
		t.Fatalf("repeat completion: %v", err)
	}
	if got := budget.Stats(); got.TotalBytes != before.TotalBytes || got.ExternalLeases != before.ExternalLeases {
		t.Fatalf("completion retained reservation: before=%+v after=%+v", before, got)
	}
}

func TestNativePreparedSideOwnerRequiresActualCleanup(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(budget.Close)
	registry := rootpublication.NewIdentityPinRegistry()
	t.Cleanup(registry.Close)
	owner, err := newNativePreparedSideOwner(budget, registry, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	pending := false
	t.Cleanup(func() {
		if pending {
			owner.metadata.RemovePending(64)
		}
		if !owner.finished {
			if err := owner.finish(nil); err != nil {
				t.Errorf("finish owner: %v", err)
			}
		}
	})
	if err := owner.metadata.AddPending(64); err != nil {
		t.Fatal(err)
	}
	pending = true
	if err := owner.finish(nil); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("finish with pending charge: %v", err)
	}
	if owner.finished || owner.custody == nil || owner.retention == nil {
		t.Fatal("refused completion consumed ownership")
	}
	owner.metadata.RemovePending(64)
	pending = false
	if err := owner.finish(nil); err != nil {
		t.Fatal(err)
	}
}

// This injects a synthetic cleanup outcome to test custody accounting. It
// does not claim to exercise a real filesystem cleanup failure.
func TestNativePreparedSideOwnerFailureKeepsReservation(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	fault := errors.New("synthetic original cleanup failure")
	owner, err := newNativePreparedSideOwner(budget, registry, 1<<20)
	if err != nil {
		budget.Close()
		registry.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !owner.finished {
			if err := owner.finish(nil); err != nil {
				t.Errorf("finish owner: %v", err)
			}
		}
		budget.Close()
		registry.Close()
	})
	closed := make(chan struct{})
	go func() {
		budget.Close()
		close(closed)
	}()
	finishErr := owner.finish(fault)
	<-closed
	if !errors.Is(finishErr, fault) {
		t.Fatalf("failure=%v", finishErr)
	}
	if !errors.Is(registry.CleanupError(), fault) || owner.retention == nil || owner.enrollment == nil {
		t.Fatal("original registry lost the failure and its admitted owner")
	}
	if got := budget.Stats(); got.ExternalLeases == 0 || got.ExternalBytes == 0 {
		t.Fatalf("budget close refunded failed custody: %+v", got)
	}
	if err := owner.finish(nil); !errors.Is(err, fault) {
		t.Fatalf("terminal failure was retried or lost: %v", err)
	}
}
