package primaryarena

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestOrdinaryPrimaryCountedClaimAndReleaseLeaseV6(t *testing.T) {
	a, err := OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	budget := &closeBudget{}
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	c, ready, err := a.PrepareClaim(Component, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatal("ordinary counted claim", ready, err)
	}
	if ready, err = c.Cancel(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatal("ordinary counted claim cancel", ready, err)
	}
	before, serial, counters := a.MetadataOwner().Bytes(), a.readSerial, a.Counters()
	r, ready, err := a.PrepareReadRootV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatal("ordinary counted root", ready, err)
	}
	if ready, err = a.Drop(r, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatal(ready, err)
	}
	for turn := 0; turn < 64; turn++ {
		ready, progress, e := a.ReleaseStep(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if !ready || e != nil {
			t.Fatal("ordinary counted release", ready, e)
		}
		if !progress {
			break
		}
		if turn == 63 {
			t.Fatal("ordinary release stalled")
		}
	}
	if a.MetadataOwner().Bytes() != before {
		t.Fatal("ordinary root refund lost", before, a.MetadataOwner().Bytes())
	}
	before, serial, counters = a.MetadataOwner().Bytes(), a.readSerial, a.Counters()
	if c, ready, e := a.PrepareNativeClaimV6(Component, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); c != nil || ready || !errors.Is(e, retainedalloc.ErrWorkRequired) || a.MetadataOwner().Bytes() != before || a.Counters() != counters {
		t.Fatal("native claim fell back", ready, e)
	}
	if r, ready, e := a.PrepareNativeConstructedReadRootV6(nil, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); r != (Ref{}) || ready || !errors.Is(e, retainedalloc.ErrWorkRequired) || a.MetadataOwner().Bytes() != before || a.readSerial != serial || a.Counters() != counters {
		t.Fatal("native root fell back", ready, e)
	}
}

func TestNativePrimaryAccountingOnlyQueueCannotBeAssistedV6(t *testing.T) {
	a, err := OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := a.MetadataOwner().Bytes()
	r, ready, err := a.PrepareNativeConstructedReadRootV6(nil, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	var q ReleaseQueue
	a.InitializeReleaseQueue(&q)
	if ready, err = a.DropToReleaseQueue(r, &q, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if ready, progress, e := a.ReleaseNativeQueueStepV6(&q, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || !progress || e != nil || q.refundOwner == nil || q.bankHead != 0 || q.groupHead != nil {
		t.Fatal("actual accounting-only queue not reached", ready, progress, e)
	}
	a.mu.Lock()
	empty := q.emptyLocked()
	a.mu.Unlock()
	if empty {
		t.Fatal("shared empty predicate discarded accounting-only debt")
	}
	bytes, charge, owner, counters := a.MetadataOwner().Bytes(), q.refundCharge, q.refundOwner, a.Counters()
	for _, w := range []*iterator.OrdinalScanWork{nil, {RecordLimit: 32, ByteLimit: 1 << 20}} {
		if ready, progress, e := a.ReleaseQueueStep(&q, w); ready || progress || !errors.Is(e, retainedalloc.ErrWorkRequired) || q.refundOwner != owner || q.refundCharge != charge || a.MetadataOwner().Bytes() != bytes || a.Counters() != counters {
			t.Fatal("ordinary stole native accounting", ready, progress, e)
		}
	}
	if e := a.PreserveOrdinaryReleaseQueue(&q); !errors.Is(e, retainedalloc.ErrWorkRequired) || q.refundOwner != owner || q.refundCharge != charge {
		t.Fatal("ordinary transfer lost debt", e)
	}
	for turn := 0; turn < 64; turn++ {
		ready, progress, e := a.ReleaseNativeQueueStepV6(&q, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if ready && !progress {
			if a.MetadataOwner().Bytes() != before || q.refundOwner != nil || q.refundCharge != 0 {
				t.Fatal("original refund incomplete")
			}
			return
		}
	}
	t.Fatal("native original accounting completion stalled")
}
