package memtable

import (
	"errors"
	"testing"
)

func TestCOWNativeGrowthTransferRollbackAfterBudgetClose(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	source, err := b.AcquireExternal(1000)
	if err != nil {
		t.Fatal(err)
	}
	target, err := b.AcquireExternal(100)
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(); target.Close(); b.Close() })
	before := b.Stats()
	sourceBefore, targetBefore := source.charge, target.charge
	var x COWNativeGrowthTransfer
	if err := x.Begin(source, target, 200, 800); err != nil {
		t.Fatal(err)
	}
	if source.charge != sourceBefore-200 || target.charge != targetBefore+200 || b.Stats() != before {
		t.Fatal("transfer changed total admission or wrong lease", b.Stats(), before)
	}
	if err := x.Begin(source, target, 1, 0); !errors.Is(err, ErrCOWPending) {
		t.Fatal(err)
	}
	b.Close()
	closed := b.Stats()
	if err := x.Rollback(); err != nil {
		t.Fatal(err)
	}
	if source.charge != sourceBefore || target.charge != targetBefore || b.Stats() != closed {
		t.Fatal("rollback changed original admission", b.Stats(), closed)
	}
	if err := x.Rollback(); !errors.Is(err, ErrCOWPending) {
		t.Fatal(err)
	}
	if err := x.Begin(source, target, 1, 0); !errors.Is(err, ErrCOWPending) {
		t.Fatal(err)
	}
	source.Close()
	target.Close()
	if got := b.Stats(); got.ExternalLeases != 0 || got.TotalBytes != 0 {
		t.Fatal(got)
	}
}

func TestCOWNativeGrowthTransferRejectsWrongOwnerOrFloor(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		b.Close()
		t.Fatal(err)
	}
	source, err := b.AcquireExternal(1024)
	if err != nil {
		b.Close()
		other.Close()
		t.Fatal(err)
	}
	target, err := b.AcquireExternal(1)
	if err != nil {
		source.Close()
		b.Close()
		other.Close()
		t.Fatal(err)
	}
	foreign, err := other.AcquireExternal(1)
	if err != nil {
		source.Close()
		target.Close()
		b.Close()
		other.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(); target.Close(); foreign.Close(); b.Close(); other.Close() })
	before, otherBefore := b.Stats(), other.Stats()
	sourceBefore, targetBefore := source.charge, target.charge
	for _, request := range []struct {
		from, to     *COWExternalLease
		delta, floor uint64
	}{
		{source, source, 1, 0}, {source, foreign, 1, 0}, {source, target, 0, 0},
		{source, target, 1025, 0}, {source, target, 1, ^uint64(0)},
		{source, target, ^uint64(0), 0},
	} {
		var x COWNativeGrowthTransfer
		if err := x.Begin(request.from, request.to, request.delta, request.floor); !errors.Is(err, ErrCOWCapacity) {
			t.Fatalf("request=%+v err=%v", request, err)
		}
		if source.charge != sourceBefore || target.charge != targetBefore || b.Stats() != before || other.Stats() != otherBefore {
			t.Fatal("refusal changed accounting")
		}
	}
	var x COWNativeGrowthTransfer
	target.Close()
	if err := x.Begin(source, target, 1, 0); !errors.Is(err, ErrCOWClosed) {
		t.Fatal(err)
	}
}

func TestCOWNativeGrowthTransferCommitOwnsOnlyTarget(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	source, err := b.AcquireExternal(1024)
	if err != nil {
		b.Close()
		t.Fatal(err)
	}
	target, err := b.AcquireExternal(128)
	if err != nil {
		source.Close()
		b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(); target.Close(); b.Close() })
	oldTarget := target.charge
	var x COWNativeGrowthTransfer
	if err := x.Begin(source, target, 256, 768); err != nil {
		t.Fatal(err)
	}
	copyOfTransfer := x
	if err := copyOfTransfer.Commit(); !errors.Is(err, ErrCOWPending) {
		t.Fatal("copied transfer committed", err)
	}
	if err := copyOfTransfer.Rollback(); !errors.Is(err, ErrCOWPending) {
		t.Fatal("copied transfer rolled back", err)
	}
	if err := x.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := x.Rollback(); !errors.Is(err, ErrCOWPending) {
		t.Fatal(err)
	}
	if err := x.Commit(); !errors.Is(err, ErrCOWPending) {
		t.Fatal(err)
	}
	source.Close()
	b.Close()
	if target.charge != oldTarget+256 || b.Stats().ExternalLeases != 1 {
		t.Fatal("source completion refunded target")
	}
	target.Close()
	if got := b.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatal(got)
	}
}

// A caller must not mutate either lease during a transfer. This controlled
// misuse demonstrates that a visible charge change is refused, not that the
// primitive detects an ABA or can recover a closed lease.
func TestCOWNativeGrowthTransferRejectsChangedCharge(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	source, err := b.AcquireExternal(1024)
	if err != nil {
		b.Close()
		t.Fatal(err)
	}
	target, err := b.AcquireExternal(100)
	if err != nil {
		source.Close()
		b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(); target.Close(); b.Close() })
	var x COWNativeGrowthTransfer
	if err := x.Begin(source, target, 200, 0); err != nil {
		t.Fatal(err)
	}
	if err := target.Resize(301); err != nil {
		t.Fatal(err)
	}
	if err := x.Commit(); !errors.Is(err, ErrCOWPending) {
		t.Fatal(err)
	}
	if err := x.Rollback(); !errors.Is(err, ErrCOWPending) {
		t.Fatal(err)
	}
	if err := target.Resize(300); err != nil {
		t.Fatal(err)
	}
	if err := x.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWNativeGrowthTransferBeginAfterBudgetClose(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	source, err := b.AcquireExternal(1024)
	if err != nil {
		b.Close()
		t.Fatal(err)
	}
	target, err := b.AcquireExternal(100)
	if err != nil {
		source.Close()
		b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(); target.Close(); b.Close() })
	b.Close()
	before := b.Stats()
	var x COWNativeGrowthTransfer
	if err := x.Begin(source, target, 100, 0); err != nil {
		t.Fatal(err)
	}
	if err := x.Commit(); err != nil {
		t.Fatal(err)
	}
	if b.Stats() != before {
		t.Fatal("prepaid transfer changed accounting after close")
	}
}
