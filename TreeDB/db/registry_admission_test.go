package db

import (
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/lifecycle"
)

func TestOriginalRegistrySnapshotGrowthRefusalRollsBackCapture(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	idx := d.idx.Load()
	metadata := idx.metadata
	budget := &primaryScratchBudget{capacity: math.MaxUint64}
	enrollment, err := metadata.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	var pins [lifecycle.FastReaderShardCount]int64
	for i := range pins {
		pins[i], _ = idx.registry.RegisterFastWithHint(uint64(100+i), -1)
		if pins[i] == 0 {
			t.Fatal("fixed cohort setup refused")
		}
	}
	defer func() {
		for _, id := range pins {
			idx.registry.Unregister(id)
		}
	}()
	before, state := metadata.Bytes(), d.state.Load()
	borrows := idx.wrapperBorrowCountV6()
	budget.capacity = before
	if snapshot := d.AcquireSnapshot(); snapshot != nil {
		_ = snapshot.Close()
		t.Fatal("original owner refusal returned an unpinned snapshot")
	}
	if metadata.Bytes() != before || d.state.Load() != state || idx.wrapperBorrowCountV6() != borrows || idx.registry.MinPinnedSeq() != 100 {
		t.Fatal("refused capture changed original charge, state, borrow, or pins")
	}
	budget.capacity = math.MaxUint64
	snapshot := d.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("unchanged capture did not recover after admission became available")
	}
	if snapshot.registryID <= 0 {
		t.Fatal("fixture did not exercise growable fallback")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if metadata.Bytes() <= before {
		t.Fatal("released snapshot lost retained fallback capacity charge")
	}
}
