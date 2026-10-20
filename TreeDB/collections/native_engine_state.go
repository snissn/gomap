package collections

import (
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// nativeEngineVersion owns the complete logical catalog. Header retention and
// last-reference release remain constant work regardless of catalog size.
// pending contains only collections with physical work, distinct from the full
// names/incarnation directories. Structural sharing owns actual node edges.
type nativeEngineVersion struct {
	refs        atomic.Int64
	names       *cowbtree.OwnedMap[string, *nativeCollectionState]
	collections *cowbtree.OwnedMap[uint64, *nativeCollectionState]
	pending     *cowbtree.OwnedMap[uint64, *nativeCollectionState]
	// catalog contains actual pending system-tree mutations, independently of
	// touched physical collection domains. Its native generation consumes the
	// same active+captured Sources bound before the first catalog mutation.
	catalog         *nativeDomainState
	inventory       *nativeResourceInventory
	nextIncarnation uint64
	allocator       backenddb.NativeSemanticAllocator
	maintenance     backenddb.NativeSemanticAllocator
	preparation     *nativeEnginePreparation
	build           *nativeEngineBuild
	cleanup         rootpublication.SemanticCleanup
	refund          *nativeEngineRefund
	// Set only by a complete physical import. No Snapshot/capture or storage
	// owner is retained; the allocator already identifies the original DB owner.
	physicalImport nativeEngineImportBasis
}

type nativeEngineImportBasis struct {
	state    backenddb.StateToken
	sequence uint64
	complete bool
}

func (e *nativeEngineVersion) NativeSemanticImportEvidence() transactionowner.NativeImportEvidence {
	if e == nil || !e.physicalImport.complete || e.allocator == nil || e.inventory != nil || e.build != nil || e.catalog != nil || e.pending == nil || e.pending.Len() != 0 {
		return transactionowner.NativeImportEvidence{}
	}
	return transactionowner.NewNativeImportEvidence(e, e.allocator, backenddb.NativeSemanticImportBasis(e.physicalImport.state), e.physicalImport.sequence)
}

// This independently preborn original accounting receipt owns no engine,
// embedded cleanup record, collection, resource or destructor backpointer.
type nativeEngineRefund struct {
	control   cowbtree.OwnedMapLease
	lease     cowbtree.OwnedMapLease
	children  cowbtree.OwnedMapCompletion
	inventory rootpublication.SemanticCleanupCompletion
}

func (r *nativeEngineRefund) Close() {
	lease, children, inventory, control := r.lease, r.children, r.inventory, r.control
	r.lease, r.children, r.inventory = nil, cowbtree.OwnedMapCompletion{}, rootpublication.SemanticCleanupCompletion{}
	r.control = nil
	r = nil
	children.Complete()
	inventory.Complete()
	if control != nil {
		control.Close()
	}
	if lease != nil {
		lease.Close()
	}
}

// newNativeEngineVersion adopts directory references only on success. The
// allocator is the original budget capability, never a bound capture method.
func newNativeEngineVersion(names *cowbtree.OwnedMap[string, *nativeCollectionState], collections, pending *cowbtree.OwnedMap[uint64, *nativeCollectionState], nextIncarnation uint64, allocator, maintenance backenddb.NativeSemanticAllocator) (*nativeEngineVersion, error) {
	if names == nil || collections == nil || pending == nil || nextIncarnation == 0 || allocator == nil || maintenance == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineVersion{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{})))
	lease, err := allocator.AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	refund := &nativeEngineRefund{lease: lease}
	e := &nativeEngineVersion{names: names, collections: collections, pending: pending, nextIncarnation: nextIncarnation, allocator: allocator, maintenance: maintenance, refund: refund}
	e.refs.Store(1)
	e.cleanup.Work = e
	return e, nil
}
func (e *nativeEngineVersion) Retain() {
	if e == nil || e.refs.Add(1) <= 1 {
		panic("collections: retain released native engine")
	}
}
func (e *nativeEngineVersion) Release() *rootpublication.SemanticCleanup {
	if e == nil {
		return nil
	}
	refs := e.refs.Add(-1)
	if refs < 0 {
		panic("collections: native engine released twice")
	}
	if refs != 0 {
		return nil
	}
	return &e.cleanup
}

// FinishSemanticCleanup is invoked once by the existing worker. It detaches
// successful fields and original independent receipts; failed originals remain
// on this same work. Final accounting follows the worker's record/Work detach.
func (e *nativeEngineVersion) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	if e.build != nil {
		child, err := e.build.finish()
		e.refund.children.Append(child)
		if err != nil {
			// This original ledger still borrows exact directory/source edges.
			// Park them together; no sibling cleanup may drop those aliases.
			done := e.refund.children
			e.refund.children = cowbtree.OwnedMapCompletion{}
			child = cowbtree.OwnedMapCompletion{}
			done.Complete()
			return rootpublication.SemanticCleanupCompletion{}, err
		}
		e.build = nil
	}
	var failure error
	child, err := (nativeDomainLifetime{}).Release(e.catalog)
	e.refund.children.Append(child)
	if err == nil {
		e.catalog = nil
	}
	failure = err
	child, err = finishNativeCollectionDirectory(&e.pending)
	e.refund.children.Append(child)
	if failure == nil {
		failure = err
	}
	child, err = finishNativeCollectionDirectory(&e.collections)
	e.refund.children.Append(child)
	if failure == nil {
		failure = err
	}
	child, err = finishNativeNameDirectory(&e.names)
	e.refund.children.Append(child)
	if failure == nil {
		failure = err
	}
	child, err = e.preparation.finish()
	e.refund.children.Append(child)
	if err == nil {
		e.preparation = nil
	}
	if failure == nil {
		failure = err
	}
	child = cowbtree.OwnedMapCompletion{}
	if failure != nil {
		// Independent successful children may finish even while the exact
		// failed graph and this containing allocation remain parked forever.
		done := e.refund.children
		e.refund.children = cowbtree.OwnedMapCompletion{}
		done.Complete()
		return rootpublication.SemanticCleanupCompletion{}, failure
	}
	if e.inventory != nil {
		work := e.inventory.release()
		if work != nil {
			done, err := work.Finish()
			if err != nil {
				children := e.refund.children
				e.refund.children = cowbtree.OwnedMapCompletion{}
				children.Complete()
				return rootpublication.SemanticCleanupCompletion{}, err
			}
			e.refund.inventory = done
		}
		e.inventory = nil
		work = nil
	}
	e.allocator, e.maintenance = nil, nil
	e.nextIncarnation = 0
	refund := e.refund
	e.refund = nil
	return rootpublication.SemanticCleanupCompletion{Refund: refund}, nil
}
func finishNativeCollectionDirectory(slot **cowbtree.OwnedMap[uint64, *nativeCollectionState]) (completion cowbtree.OwnedMapCompletion, err error) {
	m := *slot
	if m == nil {
		return completion, nil
	}
	if m.Release() {
		completion, err = m.Finish()
		if err != nil {
			return completion, err
		}
	}
	*slot = nil
	m = nil
	return completion, nil
}
func finishNativeNameDirectory(slot **cowbtree.OwnedMap[string, *nativeCollectionState]) (completion cowbtree.OwnedMapCompletion, err error) {
	m := *slot
	if m == nil {
		return completion, nil
	}
	if m.Release() {
		completion, err = m.Finish()
		if err != nil {
			return completion, err
		}
	}
	*slot = nil
	m = nil
	return completion, nil
}
