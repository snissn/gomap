package retainedalloc

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"sync"
	"unsafe"
)

var ErrWorkRequired = errors.New("retained metadata: original bounded cleanup work required")

// A cursor borrows actual intrusive binding nodes. A borrowed node stays in the
// original list until the cursor advances; no second lease registry exists.
type shrinkWork struct {
	cursor           *binding
	phase            uint8
	started, restart bool
	failure          error
}
type ownerCloseWork struct {
	native        bool
	started, done bool
	shrink        shrinkWork
}

// RemovalWork belongs in the actual caller's admitted containing allocation.
// Repeated calls consume the same original accounting delta exactly once.
type RemovalWork struct {
	mu                                    sync.Mutex
	owner                                 *Owner
	amount                                uint64
	pending, native, bound, started, done bool
	shrink                                shrinkWork
}
type enrollmentCloseWork struct {
	mu     sync.Mutex
	native bool
	phase  uint8
}

// EnrollmentReleaseWork must be embedded in the capture being disposed. Its
// two receipts match the original fixed primary/registry pair, not a new list.
type EnrollmentReleaseWork struct {
	mu                 sync.Mutex
	primary, companion RemovalWork
	enrollment, paired *Enrollment
	bound              bool
	bindPhase          uint8
}

func reserveCleanup(w *iterator.OrdinalScanWork, r, b uint64) bool {
	return w == nil || w.Reserve(r, b)
}
func (o *Owner) unlinkBindingLocked(b *binding) {
	if b.prev != nil {
		b.prev.next = b.next
	} else {
		o.head = b.next
	}
	if b.next != nil {
		b.next.prev = b.prev
	}
	b.prev, b.next = nil, nil
}
func (o *Owner) disposableBindingLocked(b *binding) bool {
	return b.refs == 0 && b.cleanupRefs == 0 && !b.producer && (!o.closed && !o.retired && !o.cleanupFailed || o.closed && o.pending == 0)
}
func (o *Owner) shrinkStepLocked(s *shrinkWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s.failure != nil {
		return false, s.failure
	}
	if w != nil && (w.RecordLimit == 0 || w.ByteLimit == 0) {
		return false, nil
	}
	for {
		if !s.started {
			if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(Owner{}))+128) {
				return false, nil
			}
			s.cursor = o.head
			s.started = true
			if s.cursor != nil {
				s.cursor.cleanupRefs++
			}
		}
		b := s.cursor
		if b == nil {
			if s.restart {
				s.started = false
				s.restart = false
				continue
			}
			if o.closed && o.head == nil && o.pending == 0 {
				o.bytes = 0
				o.baseline = 0
			}
			return true, nil
		}
		if s.phase == 0 {
			if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(binding{}))+2*uint64(unsafe.Sizeof(Owner{}))+128) {
				return false, nil
			}
			// Original growth may have changed the target since a prior return. A
			// cleanup step never reinstates a stale, smaller producer admission.
			if err := b.lease.Resize(o.bytes); err != nil {
				s.failure = err
				o.cleanupFailed = true
				return false, err
			}
			if o.closed {
				b.producer = false
			}
			s.phase = 1
		}
		if !reserveCleanup(w, 3, 6*uint64(unsafe.Sizeof(binding{}))+128) {
			return false, nil
		}
		next := b.next
		if next != nil {
			next.cleanupRefs++
		}
		if b.cleanupRefs == 0 {
			panic("retained cleanup cursor underflow")
		}
		b.cleanupRefs--
		if o.disposableBindingLocked(b) {
			o.unlinkBindingLocked(b)
			b.lease.Close()
			b.lease = nil
			b.budget = nil
			delta := AllocationCharge(uint64(unsafe.Sizeof(binding{})))
			if delta > o.controls || delta > o.bytes {
				panic("retained binding accounting underflow")
			}
			o.controls -= delta
			o.bytes -= delta
			s.restart = true
		}
		s.cursor = next
		s.phase = 0
	}
}

// CloseWork follows real physical storage teardown. It withdraws producer
// growth once, then consumes actual original lease/binding disposal in phases.
func (o *Owner) CloseWork(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrWorkRequired
	}
	return o.closeStep(true, w)
}
func (o *Owner) closeStep(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := &o.closeWork
	if !native && s.native && !s.done {
		return false, ErrWorkRequired
	}
	if s.done {
		return true, nil
	}
	if !s.started {
		if o.closed {
			s.done = true
			return true, nil
		}
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(Owner{}))+128) {
			return false, nil
		}
		target := o.baseline + o.controls + o.pending
		if target > o.bytes {
			return false, ErrCapacity
		}
		o.bytes = target
		o.closed = true
		s.native = native
		s.started = true
	}
	ready, err := o.shrinkStepLocked(&s.shrink, w)
	if ready {
		s.done = true
	}
	return ready, err
}
func (o *Owner) removeStep(amount uint64, pending, native bool, s *RemovalWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil {
		return false, ErrCapacity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return o.removeUnserialized(amount, pending, native, s, w)
}
func (o *Owner) removeUnserialized(amount uint64, pending, native bool, s *RemovalWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil {
		return false, ErrCapacity
	}
	if !native && s.native && !s.done {
		return false, ErrWorkRequired
	}
	if s.done {
		return true, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !s.started {
		if s.bound && (s.owner != o || s.amount != amount || s.pending != pending) {
			return false, ErrCapacity
		}
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(Owner{}))+2*uint64(unsafe.Sizeof(RemovalWork{}))+128) {
			return false, nil
		}
		if amount > o.bytes || pending && amount > o.pending {
			return false, ErrCapacity
		}
		s.owner = o
		s.amount = amount
		s.pending = pending
		s.native = native
		s.started = true
		o.bytes -= amount
		if pending {
			o.pending -= amount
		}
	} else if s.owner != o || s.amount != amount || s.pending != pending {
		return false, ErrCapacity
	}
	ready, err := o.shrinkStepLocked(&s.shrink, w)
	if ready {
		s.owner = nil
		s.done = true
	}
	return ready, err
}
func (o *Owner) RemovePendingWork(amount uint64, s *RemovalWork, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrWorkRequired
	}
	return o.removeStep(amount, true, true, s, w)
}
func (o *Owner) RemoveWork(amount uint64, s *RemovalWork, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrWorkRequired
	}
	return o.removeStep(amount, false, true, s, w)
}

// BindPendingWork captures only the exact original producer and charge in an
// external admitted receipt. It does not deduct storage before alias teardown.
func (s *RemovalWork) BindPendingWork(o *Owner, amount uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || o == nil || w == nil {
		return false, ErrCapacity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound {
		if s.owner != o || s.amount != amount || !s.pending {
			return false, ErrCapacity
		}
		return true, nil
	}
	if s.started || s.done {
		return false, ErrCapacity
	}
	if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(RemovalWork{}))+128) {
		return false, nil
	}
	s.owner = o
	s.amount = amount
	s.pending = true
	s.native = true
	s.bound = true
	return true, nil
}

// ContinueWork follows the disposing object's actual alias scrub.
func (s *RemovalWork) ContinueWork(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil {
		return false, ErrWorkRequired
	}
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return true, nil
	}
	o, amount, pending := s.owner, s.amount, s.pending
	s.mu.Unlock()
	if o == nil {
		return false, ErrCapacity
	}
	return o.removeStep(amount, pending, true, s, w)
}

// CloseWorkInto uses the disposing capture's preborn receipt. The Enrollment
// is scrubbed before its containing charge is removed; unfinished traversal
// remains charged in that capture, rather than in the disposed Enrollment.
func (state *EnrollmentReleaseWork) Bind(e *Enrollment, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrWorkRequired
	}
	if state == nil {
		return false, ErrCapacity
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.bindUnserialized(e, true, w)
}
func (state *EnrollmentReleaseWork) bindUnserialized(e *Enrollment, native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if state.bound {
		return false, ErrCapacity
	}
	if state.bindPhase == 0 {
		if e == nil {
			return false, ErrCapacity
		}
		e.closeWork.mu.Lock()
		if !native && e.closeWork.native && e.closeWork.phase != 3 {
			e.closeWork.mu.Unlock()
			return false, ErrWorkRequired
		}
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*state))+2*uint64(unsafe.Sizeof(*e))+128) {
			e.closeWork.mu.Unlock()
			return false, nil
		}
		if e.closed.Load() {
			state.bound = true
			e.closeWork.mu.Unlock()
			return true, nil
		}
		if native && e.closeWork.native {
			e.closeWork.mu.Unlock()
			return false, ErrWorkRequired
		}
		state.enrollment = e
		state.paired = e.companion
		e.closeWork.native = native
		state.bindPhase = 1
		e.closeWork.mu.Unlock()
	}
	if state.paired != nil {
		companion := state.paired
		companion.closeWork.mu.Lock()
		defer companion.closeWork.mu.Unlock()
		if companion.closeWork.native && companion.closeWork.phase != 3 {
			return false, ErrWorkRequired
		}
		if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(*state))+2*uint64(unsafe.Sizeof(*companion))+128) {
			return false, nil
		}
		companion.closeWork.native = native
	}
	state.bound = true
	return true, nil
}
func (state *EnrollmentReleaseWork) ContinueWork(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrWorkRequired
	}
	if state == nil {
		return false, ErrCapacity
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.bound {
		return false, ErrCapacity
	}
	return state.continuePairUnserialized(true, w)
}
func (state *EnrollmentReleaseWork) continuePairUnserialized(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	ready, err := state.paired.closeEnrollmentStepInto(native, w, &state.companion, &state.paired)
	if !ready || err != nil {
		return false, err
	}
	return state.enrollment.closeEnrollmentStepInto(native, w, &state.primary, &state.enrollment)
}
func (e *Enrollment) CloseWorkInto(w *iterator.OrdinalScanWork, state *EnrollmentReleaseWork) (bool, error) {
	if w == nil {
		return false, ErrWorkRequired
	}
	if state == nil {
		return false, ErrCapacity
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return e.closePairUnserialized(true, w, state)
}
func (e *Enrollment) closePairUnserialized(native bool, w *iterator.OrdinalScanWork, state *EnrollmentReleaseWork) (bool, error) {
	if state == nil {
		return false, ErrCapacity
	}
	if !state.bound {
		if e == nil {
			return true, nil
		}
		ready, err := state.bindUnserialized(e, native, w)
		if !ready || err != nil {
			return false, err
		}
	}
	return state.continuePairUnserialized(native, w)
}
func (e *Enrollment) closeEnrollmentStepInto(native bool, w *iterator.OrdinalScanWork, state *RemovalWork, alias **Enrollment) (bool, error) {
	if state.done {
		return true, nil
	}
	if state.started {
		*alias = nil
		return state.owner.removeUnserialized(state.amount, false, native, state, w)
	}
	if e == nil {
		return true, nil
	}
	e.closeWork.mu.Lock()

	if !native && e.closeWork.native {
		e.closeWork.mu.Unlock()
		return false, ErrWorkRequired
	}
	if e.closed.Load() {
		*alias = nil
		e.closeWork.mu.Unlock()
		return true, nil
	}
	o := e.owner
	o.mu.Lock()
	delta := AllocationCharge(uint64(unsafe.Sizeof(Enrollment{})))
	if !reserveCleanup(w, 1, 2*uint64(unsafe.Sizeof(Enrollment{}))+2*uint64(unsafe.Sizeof(binding{}))+2*uint64(unsafe.Sizeof(RemovalWork{}))+128) {
		o.mu.Unlock()
		e.closeWork.mu.Unlock()
		return false, nil
	}
	if e.binding.refs == 0 || delta > o.controls || delta > o.bytes {
		o.mu.Unlock()
		e.closeWork.mu.Unlock()
		panic("retained enrollment accounting underflow")
	}
	// Transfer the exact remaining cleanup to an already admitted containing
	// receipt before releasing the original capture edge or its physical aliases.
	state.owner = o
	state.amount = delta
	state.native = native
	state.started = true
	e.binding.refs--
	e.binding = nil
	e.owner = nil
	e.companion = nil
	e.closed.Store(true)
	e.closeWork.native = native
	e.closeWork.phase = 3
	o.controls -= delta
	o.bytes -= delta
	o.mu.Unlock()
	e.closeWork.mu.Unlock()
	*alias = nil
	e = nil
	return o.removeUnserialized(delta, false, native, state, w)
}
