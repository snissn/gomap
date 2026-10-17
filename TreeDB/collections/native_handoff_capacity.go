package collections

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeHandoffCapacity bounds BOTH complete successors requested by the lower
// handoff: latest logical state and, when distinct, the original frozen state.
// Only captured touched collections/domains contribute mutations. Later catalog
// growth is covered by the maintained tree's maximum representable height;
// newer source positions are bounded by the SAME original source limit.
//
// The returned bytes must be reserved before physical acceptance, separately
// from the original build ledger floor. Each child includes its independent
// lease header. No child points back to that ledger or another EngineVersion.
func nativeHandoffCapacity(captured *nativeEngineVersion) (bytes uint64, collections, domains int, err error) {
	if captured == nil || captured.pending == nil || captured.allocator == nil {
		return 0, 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	maxSources := captured.allocator.Limits().MaxSources
	if maxSources <= 0 {
		return 0, 0, 0, memtable.ErrCOWCapacity
	}
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	add := func(charge uint64) bool {
		charge = memtable.COWAllocationCharge(charge)
		if charge > ^uint64(0)-header || charge+header > ^uint64(0)-bytes {
			err = memtable.ErrCOWCapacity
			return false
		}
		bytes += charge + header
		return true
	}
	captured.pending.Scan(func(_ uint64, c *nativeCollectionState) bool {
		touched := c.pending.Len()
		if touched == 0 {
			return true
		}
		if collections == int(^uint(0)>>1) || touched > int(^uint(0)>>1)-domains {
			err = memtable.ErrCOWCapacity
			return false
		}
		collections++
		domains += touched
		charge, e := cowbtree.OwnedMapBuilderMaximumCharge[string, *nativeDomainState](touched)
		if e != nil {
			err = e
			return false
		}
		if !add(charge) || !add(charge) || !add(nativeCollectionStateCharge()) {
			return false
		}
		c.pending.Scan(func(name string, _ *nativeDomainState) bool {
			if !add(nativeDomainStateCharge(name)) {
				return false
			}
			// prepareNativeSourceSuffix admits one bounded borrowed-pointer
			// scratch vector/refund, then one position/refund per newer source.
			ptrSize := uint64(unsafe.Sizeof(uintptr(0)))
			if uint64(maxSources) > ^uint64(0)/ptrSize {
				err = memtable.ErrCOWCapacity
				return false
			}
			ptrs := memtable.COWAllocationCharge(uint64(maxSources) * ptrSize)
			refund := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
			if ptrs > ^uint64(0)-refund || !add(ptrs+refund) {
				err = memtable.ErrCOWCapacity
				return false
			}
			position := memtable.COWAllocationCharge(nativePendingSourcesCharge())
			if position > ^uint64(0)-header || uint64(maxSources) > ^uint64(0)/(position+header) || uint64(maxSources)*(position+header) > ^uint64(0)-bytes {
				err = memtable.ErrCOWCapacity
				return false
			}
			bytes += uint64(maxSources) * (position + header)
			return true
		})
		return err == nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if domains == 0 {
		return 0, 0, 0, nil
	}
	charge, err := nativeEnginePreparationCharge(collections, domains)
	if err != nil {
		return 0, 0, 0, err
	}
	if !add(charge) {
		return 0, 0, 0, memtable.ErrCOWCapacity
	}
	nameMap, err := cowbtree.OwnedMapBuilderMaximumCharge[string, *nativeCollectionState](collections)
	if err != nil {
		return 0, 0, 0, err
	}
	idMap, err := cowbtree.OwnedMapBuilderMaximumCharge[uint64, *nativeCollectionState](collections)
	if err != nil {
		return 0, 0, 0, err
	}
	if !add(nameMap) || !add(idMap) || !add(idMap) || bytes > ^uint64(0)/2 {
		return 0, 0, 0, memtable.ErrCOWCapacity
	}
	return bytes * 2, collections, domains, nil
}
