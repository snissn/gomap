package retainedalloc

import (
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

// AddWithOrdinalWork preserves genuine ordinary callers. A native caller
// always supplies its original vector; an unready atomic unit is an explicit
// refusal and never a synchronous nil-work retry.
func AddWithOrdinalWork(o *Owner, bytes uint64, w *iterator.OrdinalScanWork) error {
	if w == nil {
		return o.Add(bytes)
	}
	ready, err := o.AddWithWork(bytes, w)
	if !ready && err == nil {
		return ErrWorkRequired
	}
	return err
}
func AddPendingWithOrdinalWork(o *Owner, bytes uint64, w *iterator.OrdinalScanWork) error {
	if w == nil {
		return o.AddPending(bytes)
	}
	ready, err := o.AddPendingWithWork(bytes, w)
	if !ready && err == nil {
		return ErrWorkRequired
	}
	return err
}
