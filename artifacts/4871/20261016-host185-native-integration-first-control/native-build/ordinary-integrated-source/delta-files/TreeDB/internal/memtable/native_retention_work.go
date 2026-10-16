package memtable

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// These methods admit the concrete SAME budget/lease effects. Ordinary scalar
// operations and native continuations share their original lock and counters.
func (b *COWBudget) AcquireRetentionWork(bytes uint64, w *iterator.OrdinalScanWork) (retainedalloc.Lease, bool, error) {
	if w == nil {
		return nil, false, retainedalloc.ErrWorkRequired
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*b))+uint64(unsafe.Sizeof(COWExternalLease{}))) {
		return nil, false, nil
	}
	lease, err := b.AcquireExternal(bytes)
	return lease, err == nil, err
}
func (l *COWExternalLease) ResizeWork(bytes uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if !l.AdmitRetentionResize(w) {
		return false, nil
	}
	err := l.Resize(bytes)
	return err == nil, err
}

// AdmitRetentionClose admits the SAME concrete lease and budget operations
// before the caller detaches its private constructor alias and invokes Close.
func (l *COWExternalLease) AdmitRetentionClose(w *iterator.OrdinalScanWork) bool {
	return w != nil && w.Reserve(1, 2*uint64(unsafe.Sizeof(*l))+2*uint64(unsafe.Sizeof(COWBudget{})))
}

func COWRetentionAcquireWorkBytes() uint64 {
	return 2*uint64(unsafe.Sizeof(COWBudget{})) + uint64(unsafe.Sizeof(COWExternalLease{}))
}

// cowViewReleaseLease is the original view admission's external terminal
// receipt. It preserves the existing Views/Total policy and budget custody
// until all generation callbacks and view/cursor aliases have been consumed.
// No external-retention or in-flight limit is imposed on ordinary views.
type cowViewReleaseLease struct {
	budget *COWBudget
	charge uint64
	closed bool // original budget.mu
}

func (l *cowViewReleaseLease) Close() {
	b := l.budget
	b.mu.Lock()
	if l.closed {
		b.mu.Unlock()
		return
	}
	l.closed = true
	charge := l.charge
	l.charge = 0
	b.stats.DeferredBytes -= charge
	b.stats.TotalBytes -= charge
	b.releaseControlLocked()
	b.mu.Unlock()
}

// AdmitRetentionResize prices the SAME concrete Resize operation without
// changing its lease/budget. Original Owner atomic growth also reserves this
// operation for rollback before the first forward resize.
func (l *COWExternalLease) AdmitRetentionResize(w *iterator.OrdinalScanWork) bool {
	return w != nil && w.Reserve(1, 2*uint64(unsafe.Sizeof(*l))+2*uint64(unsafe.Sizeof(COWBudget{})))
}

// This observer takes the SAME original budget lock and performs no admission,
// acquisition, resize, close or statistic mutation. Called under Owner.mu, its
// order is the existing Owner -> concrete budget order used by Resize.
func (l *COWExternalLease) MaintenanceNativeRetentionGeometryForTestV6(expected retainedalloc.Budget, w *iterator.OrdinalScanWork) (g retainedalloc.NativeRetentionLeaseGeometryForTestV6, ready bool) {
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(COWExternalLease{}))+2*uint64(unsafe.Sizeof(COWBudget{}))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	if l == nil || l.budget == nil {
		return g, true
	}
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	g.RawHeader = uint64(unsafe.Sizeof(*l))
	g.HeaderCharge = cowAllocation(g.RawHeader)
	g.ResizeBytes = 2*g.RawHeader + 2*uint64(unsafe.Sizeof(*b))
	g.Charge, g.Closed, g.SameBudget, g.Concrete = l.charge, l.closed, expected == b, true
	if g.Charge >= g.HeaderCharge {
		g.Payload = g.Charge - g.HeaderCharge
	}
	return g, true
}
