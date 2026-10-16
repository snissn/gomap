package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"testing"
)

func TestNativeMVCCSnapshotActualCaptureAndCancellation(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	var lease *memtable.COWExternalLease
	called := false
	admit := func(sizes SnapshotAllocationSizes) error {
		called = true
		var e error
		lease, e = budget.AcquireExternal(memtable.COWAllocationCharge(sizes.Wrapper))
		return e
	}
	beforeEpoch := d.snapshotAcquireEpoch.Load()
	if s, ready, err := d.BeginNativeMVCCSnapshotV6(admit, &iterator.OrdinalScanWork{}); s != nil || ready || err != nil || called || d.snapshotAcquireEpoch.Load() != beforeEpoch {
		t.Fatal("capture performed refused epoch/allocation effects", err)
	}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	s, ready, err := d.BeginNativeMVCCSnapshotV6(admit, &w)
	if !d.idx.Load().pager.SupportsBoundedPhysicalClose() {
		if s != nil || ready || !errors.Is(err, ErrNativeMVCCSnapshotWork) || called || lease != nil || d.snapshotAcquireEpoch.Load() != beforeEpoch {
			t.Fatalf("unsupported capture changed custody/admission/epoch: %v", err)
		}
		budget.Close()
		return
	}
	if s == nil || !ready || err != nil || lease == nil {
		t.Fatal("actual native Snapshot capture", err)
	}
	// Refuse/cancel the SAME partially captured object before registry readiness.
	if done, err := s.CloseNativeMVCCSnapshotStepV6(&iterator.OrdinalScanWork{}); done || err != nil || !s.nativeMVCC.epochLive {
		t.Fatal("zero work consumed partial capture", err)
	}
	for turns := 0; ; turns++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		done, e := s.CloseNativeMVCCSnapshotStepV6(&w)
		if e != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual capture cancellation", e)
		}
		if done {
			break
		}
		if turns > 100 {
			t.Fatal("cancellation did not complete")
		}
	}
	if s.nativeMVCC.epochLive || s.db != nil || s.idx != nil || s.state != nil || s.vlogManager != nil || !s.cleanupFinished {
		t.Fatal("partial capture left original aliases/epoch")
	}
	lease.Close()
	budget.Close()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Stats().TotalBytes != 0 {
		t.Fatal("completed actual Snapshot allocation not released")
	}
}

// Explicit discovery can publish an owned empty Set. Its map still belongs to
// that allocation; native capture must pin it before publication retires it.
func TestNativeMVCCSnapshotPinsOwnedEmptySet(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	original := d.state.Load().ValueLogSet
	sizes, known := original.RetentionSizes()
	if original == nil || !known || sizes.FileCount != 0 {
		t.Fatal("fixture did not publish owned empty Set")
	}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	beforeEpoch, beforeRefs := d.snapshotAcquireEpoch.Load(), original.RefCount.Load()
	admitted := false
	s, ready, err := d.BeginNativeMVCCSnapshotV6(func(SnapshotAllocationSizes) error { admitted = true; return nil }, &w)
	if !d.idx.Load().pager.SupportsBoundedPhysicalClose() {
		if s != nil || ready || !errors.Is(err, ErrNativeMVCCSnapshotWork) || admitted || d.snapshotAcquireEpoch.Load() != beforeEpoch || original.RefCount.Load() != beforeRefs {
			t.Fatalf("unsupported native capture changed custody/admission/epoch: ready=%v admitted=%v err=%v", ready, admitted, err)
		}
		return
	}
	if s != nil {
		defer func() {
			for turn := 0; turn < 100; turn++ {
				work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				done, e := s.CloseNativeMVCCSnapshotStepV6(&work)
				if e != nil {
					t.Error(e)
					return
				}
				if done {
					return
				}
			}
			t.Error("native cleanup did not complete")
		}()
	}
	if err != nil || !ready || s == nil || !s.vlogPinned || s.state.ValueLogSet != original {
		t.Fatalf("empty Set custody: ready=%v err=%v", ready, err)
	}
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	if d.state.Load().ValueLogSet == original {
		t.Fatal("fixture did not retire original publication")
	}
	if original.RefCount.Load() != 1 {
		t.Fatalf("native capture did not retain original Set: refs=%d", original.RefCount.Load())
	}
}
