package rootpublication

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// The original canonical table is traversed under its own mutex. No cursor or
// preflight right survives a yield; each actual probe is admitted before read.
func (registry *IdentityPinRegistry) captureStateLockedV6(identity StableIdentity, w *iterator.OrdinalScanWork) (*identityPinState, int, bool) {
	if w == nil {
		return registry.states.get(identity), -1, true
	}
	if !w.Reserve(1, uint64(len(identity.Platform)+len(identity.ObjectID))) {
		return nil, -1, false
	}
	t := &registry.states
	if len(t.cells) == 0 {
		return nil, -1, true
	}
	mask := len(t.cells) - 1
	for i := int(t.hash(identity)) & mask; ; i = (i + 1) & mask {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(t.cells[i]))+2*uint64(unsafe.Sizeof(identityPinState{}))) {
			return nil, -1, false
		}
		cell := &t.cells[i]
		if cell.status == 0 {
			return nil, -1, true
		}
		if cell.status == 1 && cell.key == identity {
			return cell.value, i, true
		}
	}
}

// PinWithWorkV6 binds the SAME original pin after an admitted canonical lookup.
func (registry *IdentityPinRegistry) PinWithWorkV6(identity StableIdentity, w *iterator.OrdinalScanWork) (*IdentityPin, bool, error) {
	identity, err := validateRegistryIdentity(identity)
	if err != nil || registry == nil || w == nil {
		if err == nil {
			err = retainedalloc.ErrWorkRequired
		}
		return nil, false, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state, _, ready := registry.captureStateLockedV6(identity, w)
	if !ready {
		return nil, false, nil
	}
	if state == nil || state.observers == 0 || state.retired {
		return nil, false, ErrResourceConflict
	}
	if state.deleting {
		return nil, false, ErrResourceDeletionInProgress
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(IdentityPin{}))+2*uint64(unsafe.Sizeof(*state))) {
		return nil, false, nil
	}
	return registry.pinStateLockedV6(identity, state), true, nil
}

// UnobserveWithWorkV6 consumes the exact temporary capture observation. Last
// idle state removal uses the already-admitted table cell, with no second scan.
func (registry *IdentityPinRegistry) UnobserveWithWorkV6(identity StableIdentity, w *iterator.OrdinalScanWork) (bool, error) {
	identity, err := validateRegistryIdentity(identity)
	if err != nil || registry == nil || w == nil {
		if err == nil {
			err = retainedalloc.ErrWorkRequired
		}
		return false, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state, position, ready := registry.captureStateLockedV6(identity, w)
	if !ready {
		return false, nil
	}
	if state == nil || state.observers == 0 {
		return false, ErrUnbalancedResourcePin
	}
	idle := state.observers == 1 && state.pins == 0 && !state.deleting
	if idle && registry.closing {
		return false, retainedalloc.ErrWorkRequired
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*state))+128) {
		return false, nil
	}
	state.observers--
	if idle {
		registry.states.cells[position] = registryCell[StableIdentity, *identityPinState]{status: 2}
		registry.states.count--
		registry.metadata.Remove(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*state))))
	}
	return true, nil
}
