package freelist

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAppendWorkRechecksCapacityAfterReusedAllocation(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err = a.EnableCOWV1(MustNewFreelistGenerationV1(1, 4, []uint64{2}, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	// Existing transaction capacity is fixture setup, not a production admission
	// exemption. The public probe uses only actual producer-issued capacity.
	a.cow.txn.allocated = make([]allocatedPage, 0, 1)
	var s AppendWork
	_, ready, err := s.Step(a, &iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: 1 << 20})
	if err != nil || ready || s.phase != 1 || s.growth != nil {
		t.Fatalf("not actual pre-growth pause: ready=%t err=%v phase=%d", ready, err, s.phase)
	}
	beforeWater, beforeCount := a.cow.txn.highWater, p.PageCount()
	reused, err := a.Alloc(0)
	if err != nil || reused != 2 || a.cow.txn.highWater != beforeWater || p.PageCount() != beforeCount {
		t.Fatal("reused allocation", reused, err)
	}
	_, ready, err = s.Step(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if ready || !errors.Is(err, ErrAppendCapacityContinuation) || s.growth != nil || a.cow.txn.highWater != beforeWater || p.PageCount() != beforeCount || len(a.cow.txn.allocated) != 1 {
		t.Fatal("resumed allocation escaped capacity admission", ready, err)
	}
	ready, err = s.Cancel(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatal("pre-storage cancellation", ready, err)
	}
}
