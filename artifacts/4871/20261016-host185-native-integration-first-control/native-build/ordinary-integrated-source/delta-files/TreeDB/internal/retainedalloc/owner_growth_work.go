package retainedalloc

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// addWithWork admits the original atomic resize, including every possible
// rollback, under its original owner exclusion. Its first pass changes no
// owner, binding, lease or enrollment. Unsupported/oversized geometry refuses
// before the first resize. The second pass is the unchanged ordinary engine.
func (o *Owner) addWithWork(bytes uint64, pending bool, w *iterator.OrdinalScanWork) (bool, error) {
	if o == nil || w == nil {
		return false, ErrWorkRequired
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*o))+2*uint64(unsafe.Sizeof(EnrollmentWork{}))) {
		return false, nil
	}
	if active := o.enrollment; active != nil && active.pending != nil {
		if !w.Reserve(2, 4*uint64(unsafe.Sizeof(*active))) {
			return false, nil
		}
		lease, ok := active.pending.(WorkLease)
		if !ok {
			return false, ErrWorkRequired
		}
		if !lease.AdmitRetentionResize(w) || !lease.AdmitRetentionResize(w) {
			return false, nil
		}
	}
	for b := o.head; b != nil; {
		// Admission covers this discovery visit, the original forward visit and
		// the possible rollback link/control visit before observing this binding.
		if !w.Reserve(3, 6*uint64(unsafe.Sizeof(*b))) {
			return false, nil
		}
		lease, ok := b.lease.(WorkLease)
		if !ok {
			return false, ErrWorkRequired
		}
		if !lease.AdmitRetentionResize(w) || !lease.AdmitRetentionResize(w) {
			return false, nil
		}
		b = b.next
	}
	if err := o.resizeLocked(bytes, true); err != nil {
		return false, err
	}
	if pending {
		o.pending += bytes
	}
	return true, nil
}
func (o *Owner) AddWithWork(bytes uint64, w *iterator.OrdinalScanWork) (bool, error) {
	return o.addWithWork(bytes, false, w)
}
func (o *Owner) AddPendingWithWork(bytes uint64, w *iterator.OrdinalScanWork) (bool, error) {
	return o.addWithWork(bytes, true, w)
}

// ErrWorkBudget is used only by error-only original allocation helpers to
// propagate an effect-free work refusal to their typed continuation parent.
// It is distinct from unsupported work capability (ErrWorkRequired).
var ErrWorkBudget = errors.New("retained metadata: work vector exhausted")

// AddWithOrdinalWork preserves the ready result of the original atomic growth
// seam. Only genuine ordinary callers use the original nil-work operation.
func AddWithOrdinalWork(o *Owner, bytes uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		err := o.Add(bytes)
		return err == nil, err
	}
	return o.AddWithWork(bytes, w)
}
func AddPendingWithOrdinalWork(o *Owner, bytes uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		err := o.AddPending(bytes)
		return err == nil, err
	}
	return o.AddPendingWithWork(bytes, w)
}
