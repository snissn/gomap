package retainedalloc

import (
	"errors"
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
	started, done bool
	shrink        shrinkWork
}

// removalState is the synchronous caller's local original accounting receipt.
// Repeated internal steps consume the exact delta once.
type removalState struct {
	owner                  *Owner
	amount                 uint64
	pending, started, done bool
	shrink                 shrinkWork
}
type enrollmentCloseWork struct {
	mu sync.Mutex
}

// The local pair receipt detaches both Enrollment headers before their refunds.
// It borrows only the original fixed primary/registry pair.
type enrollmentReleaseState struct {
	primary, companion removalState
	enrollment, paired *Enrollment
	bound              bool
	bindPhase          uint8
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
func (o *Owner) shrinkStepLocked(s *shrinkWork) (bool, error) {
	if s.failure != nil {
		return false, s.failure
	}
	for {
		if !s.started {
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
			// Original growth may have changed the target since a prior return. A
			// cleanup step never reinstates a stale, smaller producer admission.
			if err := b.lease.Resize(o.bytes); err != nil {
				s.failure = err
				o.cleanupFailed = true
				return false, err
			}
			b.admitted = o.bytes
			if o.closed {
				b.producer = false
			}
			s.phase = 1
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

func (o *Owner) closeStep() (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := &o.closeWork
	if s.done {
		return true, nil
	}
	if !s.started {
		if o.closed {
			s.done = true
			return true, nil
		}
		target := o.baseline + o.controls + o.pending
		if target > o.bytes {
			return false, ErrCapacity
		}
		o.bytes = target
		o.closed = true
		s.started = true
	}
	ready, err := o.shrinkStepLocked(&s.shrink)
	if ready {
		s.done = true
	}
	return ready, err
}
func (o *Owner) removeUnserialized(amount uint64, pending bool, s *removalState) (bool, error) {
	if s == nil {
		return false, ErrCapacity
	}
	if s.done {
		return true, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !s.started {
		if amount > o.bytes || pending && amount > o.pending {
			return false, ErrCapacity
		}
		s.owner = o
		s.amount = amount
		s.pending = pending
		s.started = true
		o.bytes -= amount
		if pending {
			o.pending -= amount
		}
	} else if s.owner != o || s.amount != amount || s.pending != pending {
		return false, ErrCapacity
	}
	ready, err := o.shrinkStepLocked(&s.shrink)
	if ready {
		s.owner = nil
		s.done = true
	}
	return ready, err
}
func (state *enrollmentReleaseState) bindUnserialized(e *Enrollment) (bool, error) {
	if state.bound {
		return false, ErrCapacity
	}
	if state.bindPhase == 0 {
		if e == nil {
			return false, ErrCapacity
		}
		e.closeWork.mu.Lock()
		if e.closed.Load() {
			state.bound = true
			e.closeWork.mu.Unlock()
			return true, nil
		}
		state.enrollment = e
		state.paired = e.companion
		state.bindPhase = 1
		e.closeWork.mu.Unlock()
	}
	if state.paired != nil {
		companion := state.paired
		companion.closeWork.mu.Lock()
		defer companion.closeWork.mu.Unlock()
	}
	state.bound = true
	return true, nil
}
func (state *enrollmentReleaseState) continuePairUnserialized() (bool, error) {
	ready, err := state.paired.closeEnrollmentStepInto(&state.companion, &state.paired)
	if !ready || err != nil {
		return false, err
	}
	return state.enrollment.closeEnrollmentStepInto(&state.primary, &state.enrollment)
}
func (e *Enrollment) closePairUnserialized(state *enrollmentReleaseState) (bool, error) {
	if state == nil {
		return false, ErrCapacity
	}
	if !state.bound {
		if e == nil {
			return true, nil
		}
		ready, err := state.bindUnserialized(e)
		if !ready || err != nil {
			return false, err
		}
	}
	return state.continuePairUnserialized()
}
func (e *Enrollment) closeEnrollmentStepInto(state *removalState, alias **Enrollment) (bool, error) {
	if state.done {
		return true, nil
	}
	if state.started {
		*alias = nil
		return state.owner.removeUnserialized(state.amount, false, state)
	}
	if e == nil {
		return true, nil
	}
	e.closeWork.mu.Lock()

	if e.closed.Load() {
		*alias = nil
		e.closeWork.mu.Unlock()
		return true, nil
	}
	o := e.owner
	o.mu.Lock()
	delta := AllocationCharge(uint64(unsafe.Sizeof(Enrollment{})))
	if e.binding.refs == 0 || delta > o.controls || delta > o.bytes {
		o.mu.Unlock()
		e.closeWork.mu.Unlock()
		panic("retained enrollment accounting underflow")
	}
	// Transfer the exact remaining cleanup to an already admitted containing
	// receipt before releasing the original capture edge or its physical aliases.
	state.owner = o
	state.amount = delta
	state.started = true
	e.binding.refs--
	e.binding = nil
	e.owner = nil
	e.companion = nil
	e.closed.Store(true)
	o.controls -= delta
	o.bytes -= delta
	o.mu.Unlock()
	e.closeWork.mu.Unlock()
	*alias = nil
	e = nil
	return o.removeUnserialized(delta, false, state)
}
