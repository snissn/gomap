package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

type containingUnlockBudget struct {
	actual  *memtable.COWBudget
	observe func()
}
type containingUnlockLease struct {
	actual  retainedalloc.Lease
	bytes   uint64
	observe func()
}

func (b *containingUnlockBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	l, e := b.actual.AcquireRetention(n)
	if e != nil {
		return nil, e
	}
	return &containingUnlockLease{actual: l, bytes: n, observe: b.observe}, nil
}
func (l *containingUnlockLease) Resize(n uint64) error {
	if n < l.bytes {
		l.observe()
	}
	e := l.actual.Resize(n)
	if e == nil {
		l.bytes = n
	}
	return e
}
func (l *containingUnlockLease) Close() { l.actual.Close() }

func TestFileProducerContainingRefundAfterOriginalCallUnlock(t *testing.T) {
	m, f, registry := originalTerminalFixture(t)
	set := m.CurrentSetNoRefresh()
	budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if e != nil {
		t.Fatal(e)
	}
	observed, locked := false, false
	wrapper := &containingUnlockBudget{actual: budget, observe: func() {
		if f.producerMetadata == nil {
			observed = true
			if !f.closeMu.TryLock() {
				locked = true
			} else {
				f.closeMu.Unlock()
			}
			if !f.deleteMu.TryLock() {
				locked = true
			} else {
				f.deleteMu.Unlock()
			}
		}
	}}
	capture, e := registry.MetadataOwner().Enroll(wrapper)
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Close(); e != nil {
		t.Fatal(e)
	}
	done := false
	for i := 0; i < 1000 && !done; i++ {
		done, e = m.ReleaseNativeMVCCSetStep(set, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
	}
	if !done || !observed || locked {
		t.Fatalf("actual containing Resize before original caller unlock: done=%t observed=%t locked=%t", done, observed, locked)
	}
	capture.Close()
	budget.Close()
}
