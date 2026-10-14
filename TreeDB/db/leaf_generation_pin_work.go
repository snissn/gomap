package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// This receipt belongs in the existing disposing Snapshot. It consumes only
// that holder edge and borrows the original PinSet's physical counter position.
// It is not a generation or reclamation authority.
type leafGenerationPinReleaseWork struct {
	consumed, done bool
	cycle          uint64
}

func leafPinReserve(w *iterator.OrdinalScanWork, records, bytes uint64) bool {
	return w == nil || w.Reserve(records, bytes)
}

// A new original holder repairs the exact prefix already decremented by the
// last-holder cleanup before it can become readable. The repair establishes
// this new holder's pins; it does not drain the earlier cleanup callback.
func (s *leafGenerationPinSet) repairUnpinPrefixLocked() bool {
	if !s.unpinning {
		return false
	}
	for i := 0; i < s.unpinPosition; i++ {
		if ref := s.refs[i]; ref != nil {
			ref.count.Add(1)
		}
	}
	s.unpinPosition = 0
	s.unpinning = false
	s.pinned = true
	return true
}

func (s *leafGenerationPinSet) releasePinWork(tracker *leafGenerationPinTracker, state *leafGenerationPinReleaseWork, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || tracker == nil {
		return true, nil
	}
	if state == nil {
		return false, ErrSnapshotCapacity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.done {
		return true, nil
	}
	if !state.consumed {
		if !leafPinReserve(w, 1, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*state))+64) {
			return false, nil
		}
		state.consumed = true
		if s.holders == 0 {
			state.done = true
			return true, nil
		}
		s.holders--
		if s.holders != 0 || !s.pinned {
			state.done = true
			return true, nil
		}
		s.unpinCycle++
		s.unpinPosition = 0
		s.unpinning = true
		state.cycle = s.unpinCycle
	}
	if !leafPinReserve(w, 1, 2*uint64(unsafe.Sizeof(*s))+uint64(unsafe.Sizeof(*state))) {
		return false, nil
	}
	// The new holder restored custody, or a subsequent original last holder
	// began another cycle. This receipt never consumes that holder's operation.
	if !s.unpinning || s.unpinCycle != state.cycle {
		state.done = true
		return true, nil
	}
	for s.unpinPosition < len(s.refs) {
		if !leafPinReserve(w, 2, 2*uint64(unsafe.Sizeof(leafGenerationPinRef{}))+16) {
			return false, nil
		}
		ref := s.refs[s.unpinPosition]
		if ref != nil {
			current := ref.count.Load()
			if current > 0 && !ref.count.CompareAndSwap(current, current-1) {
				return false, nil
			}
		}
		s.unpinPosition++
	}
	if !leafPinReserve(w, 1, 2*uint64(unsafe.Sizeof(*s))+uint64(unsafe.Sizeof(*state))) {
		return false, nil
	}
	s.pinned = false
	s.unpinning = false
	s.unpinPosition = 0
	state.done = true
	return true, nil
}
