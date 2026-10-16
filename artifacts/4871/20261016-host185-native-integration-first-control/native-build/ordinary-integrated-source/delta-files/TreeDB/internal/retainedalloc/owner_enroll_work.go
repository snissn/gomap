package retainedalloc

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"math"
	"sync"
	"unsafe"
)

// WorkBudget/WorkLease are the SAME original budget and lease operations with
// their concrete containing/allocation admission. Native enrollment never
// calls an unbound constructor or guesses a foreign lease's physical geometry.
type WorkBudget interface {
	Budget
	AcquireRetentionWork(uint64, *iterator.OrdinalScanWork) (Lease, bool, error)
}
type WorkLease interface {
	Lease
	ResizeWork(uint64, *iterator.OrdinalScanWork) (bool, error)
	AdmitRetentionResize(*iterator.OrdinalScanWork) bool
	AdmitRetentionClose(*iterator.OrdinalScanWork) bool
}

// EnrollmentWork is embedded in the already admitted capture. Its intrusive
// borrows use the original binding cleanup edges; no registry or lease copy is
// created. Reserved controls remain unallocated until every original governor
// covers the CURRENT producer total. Ordinary growth/enrollment stays usable.
type EnrollmentWork struct {
	mu                  sync.Mutex
	owner               *Owner
	budget              Budget
	cursor, found       *binding // Owner.mu guards both borrowed nodes
	delta               uint64
	newBinding          bool
	phase               uint8
	failure             error
	result              *Enrollment
	pending             Lease
	pendingAdmitted     uint64
	removal, adjustment RemovalWork
	detachedRemoval     RemovalWork
	detachedAmount      uint64
}

func (o *Owner) EnrollWithWork(budget Budget, state *EnrollmentWork, w *iterator.OrdinalScanWork) (*Enrollment, bool, error) {
	if o == nil || state == nil || w == nil {
		return nil, false, ErrWorkRequired
	}
	if _, ok := budget.(WorkBudget); !ok {
		return nil, false, ErrWorkRequired
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return o.enrollStep(budget, state, w)
}

// BeginEnrollmentWithWork establishes the SAME original governing lease while
// the caller still owns the physical capture exclusion. A true result permits
// original physical borrows; enrollment itself is completed by EnrollWithWork.
// Neither the source header nor future producer growth is carried uncharged.
func (o *Owner) BeginEnrollmentWithWork(budget Budget, s *EnrollmentWork, w *iterator.OrdinalScanWork) (bool, error) {
	if o == nil || s == nil || w == nil {
		return false, ErrWorkRequired
	}
	wb, ok := budget.(WorkBudget)
	if !ok {
		return false, ErrWorkRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.beginEnrollmentLocked(wb, budget, s, w)
}
func (o *Owner) beginEnrollmentLocked(wb WorkBudget, budget Budget, s *EnrollmentWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase != 0 {
		if s.owner != o || s.budget != budget {
			return false, ErrCapacity
		}
		return s.failure == nil, s.failure
	}
	if o.enrollment != nil && o.enrollment != s {
		return false, ErrWorkRequired
	}
	if o.closed {
		return false, ErrClosed
	}
	if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*o))) {
		return false, nil
	}
	lease, ready, err := wb.AcquireRetentionWork(o.bytes, w)
	if !ready || err != nil {
		return false, err
	}
	s.pending, s.pendingAdmitted = lease, o.bytes
	s.owner, s.budget = o, budget
	o.enrollment = s
	s.cursor = o.head
	if s.cursor != nil {
		s.cursor.cleanupRefs++
	}
	s.phase = 1
	return true, nil
}
func (o *Owner) enrollStep(budget Budget, s *EnrollmentWork, w *iterator.OrdinalScanWork) (*Enrollment, bool, error) {
	if s.phase == 6 {
		return s.result, true, s.failure
	}
	o.mu.Lock()
	if s.owner != nil && (s.owner != o || s.budget != budget) {
		o.mu.Unlock()
		return nil, false, ErrCapacity
	}
	if s.phase == 0 {
		ready, err := o.beginEnrollmentLocked(budget.(WorkBudget), budget, s, w)
		if !ready || err != nil {
			o.mu.Unlock()
			return nil, err != nil, err
		}
	}
	if o.closed && s.failure == nil {
		s.failure = ErrClosed
	}
	if s.failure != nil {
		o.mu.Unlock()
		return o.unwindEnrollment(s, w)
	}
	for s.phase == 1 {
		b := s.cursor
		if b == nil {
			s.phase = 2
			break
		}
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(binding{}))+uint64(unsafe.Sizeof(*s))) {
			o.mu.Unlock()
			return nil, false, nil
		}
		if _, ok := b.lease.(WorkLease); !ok {
			s.failure = ErrWorkRequired
			o.mu.Unlock()
			return o.unwindEnrollment(s, w)
		}
		if b.budget == budget && s.found == nil {
			b.cleanupRefs++
			s.found = b
		}
		next := b.next
		if next != nil {
			next.cleanupRefs++
		}
		b.cleanupRefs--
		s.cursor = next
	}
	if s.phase == 2 {
		delta := AllocationCharge(uint64(unsafe.Sizeof(Enrollment{})))
		s.newBinding = s.found == nil
		if s.newBinding {
			delta += AllocationCharge(uint64(unsafe.Sizeof(binding{})))
		}
		if delta > math.MaxUint64-o.bytes {
			s.failure = ErrCapacity
			o.mu.Unlock()
			return o.unwindEnrollment(s, w)
		}
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*o))) {
			o.mu.Unlock()
			return nil, false, nil
		}
		// This is a reservation only: no allocation/physical capture has occurred.
		o.bytes += delta
		o.enrolling += delta
		s.delta = delta
		s.cursor = o.head
		if s.cursor != nil {
			s.cursor.cleanupRefs++
		}
		s.phase = 3
	}
	for s.phase == 3 {
		b := s.cursor
		if b == nil {
			s.phase = 4
			break
		}
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(binding{}))+uint64(unsafe.Sizeof(*s))) {
			o.mu.Unlock()
			return nil, false, nil
		}
		if b.admitted < o.bytes {
			lease, ok := b.lease.(WorkLease)
			if !ok {
				s.failure = ErrWorkRequired
				o.mu.Unlock()
				return o.unwindEnrollment(s, w)
			}
			ready, err := lease.ResizeWork(o.bytes, w)
			if err != nil {
				s.failure = err
				o.mu.Unlock()
				return o.unwindEnrollment(s, w)
			}
			if !ready {
				o.mu.Unlock()
				return nil, false, nil
			}
			b.admitted = o.bytes
		}
		next := b.next
		if next != nil {
			next.cleanupRefs++
		}
		b.cleanupRefs--
		s.cursor = next
	}
	if s.phase == 4 && s.newBinding && s.found != nil {
		// An ordinary capture installed the SAME budget binding during a return.
		// Release only the still-unallocated redundant binding reservation.
		amount := AllocationCharge(uint64(unsafe.Sizeof(binding{})))
		o.mu.Unlock()
		ready, err := o.removeStep(amount, false, true, &s.adjustment, w)
		if err != nil {
			return nil, false, err
		}
		if !ready {
			return nil, false, nil
		}
		o.mu.Lock()
		o.enrolling -= amount
		s.delta -= amount
		s.newBinding = false
	}
	if s.phase == 4 {
		if !reserveCleanup(w, 1, uint64(unsafe.Sizeof(Enrollment{}))+2*uint64(unsafe.Sizeof(binding{}))+2*uint64(unsafe.Sizeof(*o))+2*uint64(unsafe.Sizeof(*s))) {
			o.mu.Unlock()
			return nil, false, nil
		}
		b := s.found
		if b == nil {
			lease := s.pending
			ready, err := lease.(WorkLease).ResizeWork(o.bytes, w)
			if err != nil {
				s.failure = err
				o.mu.Unlock()
				return o.unwindEnrollment(s, w)
			}
			if !ready {
				o.mu.Unlock()
				return nil, false, nil
			}
			s.pending = nil
			s.pendingAdmitted = 0
			b = &binding{budget: budget, lease: lease, next: o.head, admitted: o.bytes}
			if o.head != nil {
				o.head.prev = b
			}
			o.head = b
		} else {
			// This private constructor overlapped an existing binding. Detach
			// the exact temporary lease before consuming its own refund edge.
			if s.pending != nil {
				lease := s.pending
				// Reserve the concrete lease closure while the original
				// constructor still owns it; effect runs after field detachment.
				if !lease.(WorkLease).AdmitRetentionClose(w) {
					o.mu.Unlock()
					return nil, false, nil
				}
				s.pending = nil
				s.pendingAdmitted = 0
				lease.Close()
			}
			b.cleanupRefs--
			s.found = nil
		}
		if b.refs == math.MaxUint64 {
			panic("retained enrollment reference overflow")
		}
		b.refs++
		o.controls += s.delta
		o.enrolling -= s.delta
		s.delta = 0
		s.result = &Enrollment{owner: o, binding: b}
		o.enrollment = nil
		s.owner = nil
		s.budget = nil
		s.phase = 6
		o.mu.Unlock()
		return s.result, true, nil
	}
	o.mu.Unlock()
	return nil, false, nil
}

func (o *Owner) unwindEnrollment(s *EnrollmentWork, w *iterator.OrdinalScanWork) (*Enrollment, bool, error) {
	o.mu.Lock()
	if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*o))+4*uint64(unsafe.Sizeof(binding{}))) {
		o.mu.Unlock()
		return nil, false, nil
	}
	if s.cursor != nil {
		s.cursor.cleanupRefs--
		s.cursor = nil
	}
	// Keep the SAME found binding edge through partial reservation unwind.
	// A legal ordinary last capture may close between these returns.
	amount := s.delta
	o.mu.Unlock()
	if amount != 0 {
		ready, err := o.removeStep(amount, false, true, &s.removal, w)
		if err != nil {
			return nil, false, err
		}
		if !ready {
			return nil, false, nil
		}
	}
	o.mu.Lock()
	if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*o))+2*uint64(unsafe.Sizeof(binding{}))) {
		o.mu.Unlock()
		return nil, false, nil
	}
	if b := s.found; b != nil && b.refs == 0 && b.cleanupRefs == 1 && !b.producer && ((!o.retired && !o.cleanupFailed) || o.closed && o.pending == 0) {
		// Ordinary capture promoted this SAME lease, then consumed its last
		// edge. Move its constructor custody back before unlinking its header.
		if s.pending == nil {
			s.pending = b.lease
			s.pendingAdmitted = b.admitted
		} else {
			// A distinct private constructor already preserves the source.
			if !b.lease.(WorkLease).AdmitRetentionClose(w) {
				o.mu.Unlock()
				return nil, false, nil
			}
			lease := b.lease
			b.lease = nil
			lease.Close()
		}
		o.unlinkBindingLocked(b)
		b.lease = nil
		b.budget = nil
		b.cleanupRefs = 0
		s.found = nil
		s.detachedAmount = AllocationCharge(uint64(unsafe.Sizeof(binding{})))
		o.controls -= s.detachedAmount
	}
	detached := s.detachedAmount
	o.mu.Unlock()
	if detached != 0 {
		ready, err := o.removeStep(detached, false, true, &s.detachedRemoval, w)
		if !ready || err != nil {
			return nil, false, err
		}
	}
	o.mu.Lock()
	if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*o))) {
		o.mu.Unlock()
		return nil, false, nil
	}
	lease := s.pending
	if lease != nil && !lease.(WorkLease).AdmitRetentionClose(w) {
		o.mu.Unlock()
		return nil, false, nil
	}
	if s.found != nil {
		s.found.cleanupRefs--
		s.found = nil
	}
	o.enrolling -= amount
	s.delta = 0
	s.detachedAmount = 0
	s.pending = nil
	s.pendingAdmitted = 0
	o.enrollment = nil
	if o.closed && o.head == nil && o.pending == 0 && o.enrolling == 0 {
		o.bytes = 0
		o.baseline = 0
	}
	s.owner = nil
	s.budget = nil
	s.phase = 6
	o.mu.Unlock()
	o = nil
	if lease != nil {
		lease.Close()
	}
	return nil, true, s.failure
}

// CancelWithWork consumes the SAME unallocated reservation and binding borrows.
// It never allocates a new cleanup receipt or invokes whole-list rollback.
func (s *EnrollmentWork) CancelWithWork(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil {
		return false, ErrWorkRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase == 6 {
		return true, s.failure
	}
	if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	if s.owner == nil {
		s.phase = 6
		return true, nil
	}
	if s.failure == nil {
		s.failure = ErrClosed
	}
	_, ready, err := s.owner.unwindEnrollment(s, w)
	return ready, err
}

// EnrollmentPairWork is the fixed original primary/registry pair in the
// caller's admitted capture. A refused second owner never hides first-owner
// disposal in an unlimited Close or allocates an unwind callback.
type EnrollmentPairWork struct {
	mu                        sync.Mutex
	firstOwner, secondOwner   *Owner
	budget                    Budget
	first, second             EnrollmentWork
	primary                   *Enrollment
	release                   EnrollmentReleaseWork
	phase                     uint8
	failure                   error
	cancelPhase               uint8
	cancelFirst, cancelSecond EnrollmentReleaseWork
}

func EnrollPairWithWork(first, second *Owner, budget Budget, s *EnrollmentPairWork, w *iterator.OrdinalScanWork) (*Enrollment, bool, error) {
	if s == nil || w == nil {
		return nil, false, ErrWorkRequired
	}
	if first == nil {
		if second != nil {
			return nil, true, ErrCapacity
		}
		return nil, true, nil
	}
	if first == second {
		return nil, true, ErrCapacity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase == 4 {
		return s.primary, true, s.failure
	}
	if s.firstOwner != nil && (s.firstOwner != first || s.secondOwner != second || s.budget != budget) {
		return nil, false, ErrCapacity
	}
	if s.firstOwner == nil {
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))) {
			return nil, false, nil
		}
		s.firstOwner, s.secondOwner, s.budget = first, second, budget
	}
	if s.phase == 0 {
		e, ready, err := first.EnrollWithWork(budget, &s.first, w)
		if !ready || err != nil {
			if ready {
				s.phase = 4
				s.failure = err
			}
			return nil, ready, err
		}
		s.primary = e
		s.phase = 1
	}
	if s.phase == 1 {
		if second == nil {
			s.phase = 4
			return s.primary, true, nil
		}
		e, ready, err := second.EnrollWithWork(budget, &s.second, w)
		if !ready {
			return nil, false, err
		}
		if err != nil {
			s.failure = err
			s.phase = 2
		} else {
			if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(Enrollment{}))+uint64(unsafe.Sizeof(*s))) {
				return nil, false, nil
			}
			s.primary.closeWork.mu.Lock()
			s.primary.companion = e
			s.primary.closeWork.mu.Unlock()
			s.first.result = nil
			s.second.result = nil
			s.phase = 4
			return s.primary, true, nil
		}
	}
	if s.phase == 2 {
		ready, err := s.release.Bind(s.primary, w)
		if !ready || err != nil {
			return nil, false, err
		}
		// Clear ALL caller aliases before external disposal can refund the handle.
		s.primary = nil
		s.first.result = nil
		// The external receipt is now the only original cleanup authority.
		// Clear source-header aliases before either owner can be refunded.
		s.firstOwner = nil
		s.secondOwner = nil
		s.budget = nil
		s.phase = 3
	}
	if s.phase == 3 {
		ready, err := s.release.ContinueWork(w)
		if !ready || err != nil {
			return nil, false, err
		}
		s.firstOwner = nil
		s.secondOwner = nil
		s.budget = nil
		s.phase = 4
		return nil, true, s.failure
	}
	return nil, false, nil
}

// EnrollmentConstructorWorkBytes describes only the fixed original header
// effects. The original budget admits its own concrete lease separately.
func EnrollmentConstructorWorkBytes() uint64 {
	return 2*uint64(unsafe.Sizeof(EnrollmentWork{})) + 2*uint64(unsafe.Sizeof(Owner{}))
}
func BeginEnrollmentPairWithWork(first, second *Owner, budget Budget, s *EnrollmentPairWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil || first == nil || first == second {
		return false, ErrCapacity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstOwner != nil && s.firstOwner != first || s.secondOwner != nil && s.secondOwner != second || s.budget != nil && s.budget != budget {
		return false, ErrCapacity
	}
	ready, err := first.BeginEnrollmentWithWork(budget, &s.first, w)
	if !ready || err != nil {
		return false, err
	}
	s.firstOwner, s.budget = first, budget
	if second != nil {
		ready, err = second.BeginEnrollmentWithWork(budget, &s.second, w)
		if !ready || err != nil {
			return false, err
		}
		s.secondOwner = second
	}
	return true, nil
}

// DetachResult transfers the exact pair to its original physical Snapshot.
// No completed constructor field continues borrowing the disposed headers.
func (s *EnrollmentPairWork) DetachResult() *Enrollment {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != 4 || s.failure != nil {
		return nil
	}
	e := s.primary
	s.primary = nil
	s.first.result = nil
	s.second.result = nil
	s.firstOwner = nil
	s.secondOwner = nil
	s.budget = nil
	return e
}

// CancelWithWork disposes both SAME original constructor/capture edges. Each
// outer receipt survives clearing the Enrollment's containing aliases.
func (s *EnrollmentPairWork) CancelWithWork(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil {
		return false, ErrWorkRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelPhase == 3 {
		return true, nil
	}
	if s.cancelPhase == 0 {
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		s.firstOwner = nil
		s.secondOwner = nil
		s.budget = nil
		if s.second.result != nil {
			ready, err := s.cancelSecond.Bind(s.second.result, w)
			if !ready || err != nil {
				return false, err
			}
			s.second.result = nil
		} else if s.second.phase != 6 {
			ready, err := s.second.CancelWithWork(w)
			if !ready || err != nil && err != ErrClosed {
				return false, err
			}
		}
		s.cancelPhase = 1
	}
	if s.cancelPhase == 1 {
		if s.cancelSecond.bound {
			ready, err := s.cancelSecond.ContinueWork(w)
			if !ready || err != nil {
				return false, err
			}
		}
		if s.primary != nil || s.first.result != nil {
			e := s.primary
			if e == nil {
				e = s.first.result
			}
			ready, err := s.cancelFirst.Bind(e, w)
			if !ready || err != nil {
				return false, err
			}
			s.primary = nil
			s.first.result = nil
		} else if s.first.phase != 6 {
			ready, err := s.first.CancelWithWork(w)
			if !ready || err != nil && err != ErrClosed {
				return false, err
			}
		}
		s.cancelPhase = 2
	}
	if s.cancelFirst.bound {
		ready, err := s.cancelFirst.ContinueWork(w)
		if !ready || err != nil {
			return false, err
		}
	}
	s.firstOwner = nil
	s.secondOwner = nil
	s.budget = nil
	s.cancelPhase = 3
	return true, nil
}
