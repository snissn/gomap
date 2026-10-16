package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// ObserveWithWorkV6 admits the original registry cell and any actual geometric
// backing replacement under the original lock. It creates no parallel table.
func (registry *IdentityPinRegistry) ObserveWithWorkV6(identity StableIdentity, w *iterator.OrdinalScanWork) (bool, error) {
	identity, err := validateRegistryIdentity(identity)
	if err != nil {
		return false, err
	}
	if registry == nil {
		return false, ErrUnresolvedResource
	}
	reserve := func(r, b uint64) (bool, error) {
		if w == nil {
			return true, nil
		}
		if r > w.RecordLimit || b > w.ByteLimit {
			return false, &iterator.OrdinalUnitTooLarge{Records: r, Bytes: b}
		}
		return w.Reserve(r, b), nil
	}
	if ok, e := reserve(1, 2*uint64(unsafe.Sizeof(identity))+uint64(len(identity.Platform))); !ok || e != nil {
		return false, e
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closing {
		return false, retainedalloc.ErrClosed
	}
	state, _, ready := registry.captureStateLockedV6(identity, w)
	if !ready {
		return false, nil
	}
	if state == nil {
		t := &registry.states
		capacity := len(t.cells)
		replace := capacity == 0 || (t.used+1)*2 >= capacity
		records := uint64(2)
		bytes := 2 * uint64(unsafe.Sizeof(identityPinState{}))
		if replace {
			if (t.count+1)*2 >= capacity {
				if capacity > int(^uint(0)>>1)/2 {
					return false, errors.New("identity registry capacity overflow")
				}
				capacity *= 2
			}
			if capacity == 0 {
				capacity = 16
			}
			records += uint64(len(t.cells))
			bytes += uint64(capacity+len(t.cells)) * uint64(unsafe.Sizeof(registryCell[StableIdentity, *identityPinState]{}))
		}
		if ok, e := reserve(records, bytes); !ok || e != nil {
			return false, e
		}
		state, err = registry.stateLocked(identity)
		if err != nil {
			return false, err
		}
	}
	if state.retired || state.deleting {
		return false, ErrResourceConflict
	}
	state.observers++
	return true, nil
}
