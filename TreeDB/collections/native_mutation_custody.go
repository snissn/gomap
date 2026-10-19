package collections

import (
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// nativeMutationCustody is the original sparse planner ledger, independent of
// the complete EngineVersion. Each private descriptor is adopted immediately
// after successful construction, before another fallible stage can begin.
// The accepted prefix transfers this exact work; no close/error path rebuilds
// its inventory from current metadata or retains an ancestor engine.
type nativeMutationCustody struct {
	released        atomic.Bool
	cleanup         rootpublication.SemanticCleanup
	domains         []*nativeDomainState
	collections     []*nativeCollectionState
	refund          *nativeEngineRefund
	assignedFailure error
}

func newNativeMutationCustody(domainCount, collectionCount int, allocator backenddb.NativeSemanticAllocator) (*nativeMutationCustody, error) {
	if allocator == nil || domainCount < 0 || collectionCount < 0 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	slots := uint64(domainCount) + uint64(collectionCount)
	if slots > ^uint64(0)/uint64(unsafe.Sizeof(uintptr(0))) {
		return nil, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeMutationCustody{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{})))
	domainBytes := memtable.COWAllocationCharge(uint64(domainCount) * uint64(unsafe.Sizeof(uintptr(0))))
	collectionBytes := memtable.COWAllocationCharge(uint64(collectionCount) * uint64(unsafe.Sizeof(uintptr(0))))
	if domainBytes > ^uint64(0)-charge || collectionBytes > ^uint64(0)-charge-domainBytes {
		return nil, memtable.ErrCOWCapacity
	}
	lease, err := allocator.AdmitNativeBytes(charge + domainBytes + collectionBytes)
	if err != nil {
		return nil, err
	}
	w := &nativeMutationCustody{domains: make([]*nativeDomainState, 0, domainCount), collections: make([]*nativeCollectionState, 0, collectionCount), refund: &nativeEngineRefund{lease: lease}}
	w.cleanup.Work = w
	return w, nil
}

func (w *nativeMutationCustody) adoptDomain(domain *nativeDomainState) {
	if w == nil || w.released.Load() || domain == nil || len(w.domains) == cap(w.domains) {
		panic("collections: native sparse domain admission mismatch")
	}
	w.domains = append(w.domains, domain)
}
func (w *nativeMutationCustody) adoptCollection(collection *nativeCollectionState) {
	if w == nil || w.released.Load() || collection == nil || len(w.collections) == cap(w.collections) {
		panic("collections: native sparse collection admission mismatch")
	}
	w.collections = append(w.collections, collection)
}
func (w *nativeMutationCustody) ReleaseNativeCustody() *rootpublication.SemanticCleanup {
	if w == nil {
		return nil
	}
	if !w.released.CompareAndSwap(false, true) {
		panic("collections: native sparse custody released twice")
	}
	return &w.cleanup
}

// The lower owner retains all original assigned custody before invoking this
// notification. The complete State is only borrowed and is never kept here.
func (w *nativeMutationCustody) RetainAssignedNativeFailure(_ backenddb.NativeFinalizedPublication, err error) {
	if w.assignedFailure == nil {
		w.assignedFailure = err
	}
}
func (w *nativeMutationCustody) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	if w.assignedFailure != nil {
		return rootpublication.SemanticCleanupCompletion{}, w.assignedFailure
	}
	var failure error
	// Collections may also own the adopted domain descriptors. Release their
	// separate sparse planner edges before the domain edge, exactly once each.
	for i, collection := range w.collections {
		completion, err := (nativeCollectionLifetime{}).Release(collection)
		if err == nil {
			w.collections[i] = nil
		}
		collection = nil
		w.refund.children.Append(completion)
		if failure == nil {
			failure = err
		}
	}
	for i, domain := range w.domains {
		completion, err := (nativeDomainLifetime{}).Release(domain)
		if err == nil {
			w.domains[i] = nil
		}
		domain = nil
		w.refund.children.Append(completion)
		if failure == nil {
			failure = err
		}
	}
	if failure != nil {
		completion := w.refund.children
		w.refund.children = cowbtree.OwnedMapCompletion{}
		completion.Complete()
		return rootpublication.SemanticCleanupCompletion{}, failure
	}
	w.collections, w.domains = nil, nil
	refund := w.refund
	w.refund = nil
	return rootpublication.SemanticCleanupCompletion{Refund: refund}, nil
}
