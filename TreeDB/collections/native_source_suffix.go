package collections

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// prepareNativeSourceSuffix drops boundary and its captured older prefix.
// Only newer chain-position headers are cloned. Physical source owners and
// their finite source-slot receipts are shared, never duplicated. admit must
// spend original preadmitted handoff construction capacity, not producer
// headroom. On error result is an owned PRIVATE partial list for original Work.
// scratch contains independent original receipts, with every borrowed scratch
// alias cleared before transfer; complete it only through the caller's worker.
func prepareNativeSourceSuffix(latest *nativePendingSources, boundary *memtable.COWRoot, admit cowbtree.OwnedMapAdmit) (result *nativePendingSources, scratch cowbtree.OwnedMapCompletion, err error) {
	if boundary == nil {
		latest.retain()
		return latest, scratch, nil
	}
	count := 0
	cursor := latest
	for cursor != nil && cursor.source.root != boundary {
		count++
		cursor = cursor.older
	}
	if cursor == nil {
		return nil, scratch, backenddb.ErrRootPublicationBasisMismatch
	}
	if count == 0 {
		return nil, scratch, nil
	}
	if uint64(count) > ^uint64(0)/uint64(unsafe.Sizeof(uintptr(0))) {
		return nil, scratch, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(count) * uint64(unsafe.Sizeof(uintptr(0))))
	metadata := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	if metadata > ^uint64(0)-charge {
		return nil, scratch, memtable.ErrCOWCapacity
	}
	charge += metadata
	lease, e := admit(charge)
	if e != nil {
		return nil, scratch, e
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	sources := make([]*nativePendingSource, count)
	cursor = latest
	for i := range sources {
		sources[i] = cursor.source
		cursor = cursor.older
	}
	cursor = nil
	for i := len(sources) - 1; i >= 0; i-- {
		node, e := newNativePendingSources(sources[i], result, admit)
		if e != nil {
			err = e
			break
		}
		// The opening source remains owned during construction. Attach the actual
		// infallible shared edge immediately, before another fallible admission.
		sources[i].retain()
		result = node
	}
	clear(sources)
	sources = nil
	scratch = refund.Completion()
	return result, scratch, err
}
