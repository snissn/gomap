package freelist

import (
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
	id, ready, err := s.Step(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if err != nil || !ready || id != beforeWater || len(a.cow.txn.allocated) != 2 || a.cow.txn.allocated[0].id != reused {
		t.Fatal("resumed allocation did not grow same live backing", id, ready, err)
	}
	ready, err = s.Cancel(nil)
	if !ready || err != nil {
		t.Fatal("checked diagnostic cancellation", ready, err)
	}
}

func TestAppendWorkAdmitsEmptyLiveBackingBeforeInstall(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err = a.EnableCOWV1(MustNewFreelistGenerationV1(1, 4, nil, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	var s AppendWork
	before := p.PageCount()
	_, ready, err := s.Step(a, &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20})
	if ready || err != nil || len(a.cow.txn.allocated) != 0 || cap(a.cow.txn.allocated) != 0 || p.PageCount() != before {
		t.Fatal("unadmitted backing effect", ready, err)
	}
	for i := 0; i < 32; i++ {
		id, done, e := s.Step(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if done {
			if id != before || len(a.cow.txn.allocated) != 1 {
				t.Fatal("wrong live install")
			}
			if ok, e := s.Cancel(nil); !ok || e != nil {
				t.Fatal(ok, e)
			}
			return
		}
	}
	t.Fatal("empty backing never installed")
}
