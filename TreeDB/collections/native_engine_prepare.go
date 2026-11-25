package collections

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeEnginePreparation is the original finite private successor ledger.
// Every fallible constructor attaches its result here before the next stage.
// Unsealed builders borrow the lower opening cut only during construction.
// Before returning any private partial, sealPrivateBuilders attaches its exact
// child/value edges, including on error. The original failed Work can then stay
// parked after the opening/latest cut leaves. No engine ancestor is retained.
type nativeEnginePreparation struct {
	names                               *cowbtree.OwnedMapBuilder[string, *nativeCollectionState]
	collections, pending                *cowbtree.OwnedMapBuilder[uint64, *nativeCollectionState]
	domainBuilder, pendingDomainBuilder *cowbtree.OwnedMapBuilder[string, *nativeDomainState]
	domains, pendingDomains             *cowbtree.OwnedMap[string, *nativeDomainState]
	schema                              *nativeSchemaState
	ownedDomains                        []*nativeDomainState
	ownedCollections                    []*nativeCollectionState
	inlineDomains                       [1]*nativeDomainState
	inlineCollections                   [1]*nativeCollectionState
}

// nativeEngineVersionWithPrep fuses nativeEngineVersion and
// nativeEnginePreparation into a single heap allocation for prepared
// successors. nativeEngineRefund remains strictly independent with zero
// backpointer to this composite allocation.
type nativeEngineVersionWithPrep struct {
	version nativeEngineVersion
	prep    nativeEnginePreparation
}

// sealPrivateBuilders completes ownership, not publication. Seal is infallible
// from each original admitted mutation cohort. No fallible child admission is
// added while converting the private partial's borrowed tree edges to owned
// edges. Both complete and partial successors use the same cleanup path.
func (p *nativeEnginePreparation) sealPrivateBuilders(e *nativeEngineVersion) {
	if p.names != nil {
		e.names = p.names.Seal()
		p.names = nil
	}
	if p.collections != nil {
		e.collections = p.collections.Seal()
		p.collections = nil
	}
	if p.pending != nil {
		e.pending = p.pending.Seal()
		p.pending = nil
	}
	if p.domainBuilder != nil {
		p.domains = p.domainBuilder.Seal()
		p.domainBuilder = nil
	}
	if p.pendingDomainBuilder != nil {
		p.pendingDomains = p.pendingDomainBuilder.Seal()
		p.pendingDomainBuilder = nil
	}
}

func newNativeEnginePreparation(base *nativeEngineVersion, collectionCount, domainCount int, allocation backenddb.NativeSemanticAllocator) (*nativeEngineVersion, error) {
	if base == nil || collectionCount < 0 || domainCount < 0 || allocation == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge, err := nativeEnginePreparationCharge(collectionCount, domainCount)
	if err != nil {
		return nil, err
	}
	lease, err := allocation.AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	holder := new(nativeEngineVersionWithPrep)
	e := &holder.version
	p := &holder.prep
	e.nextIncarnation = base.nextIncarnation
	e.allocator = base.allocator
	e.maintenance = base.maintenance
	e.refund = &nativeEngineRefund{lease: lease}
	e.preparation = p
	if domainCount <= len(p.inlineDomains) {
		p.ownedDomains = p.inlineDomains[:0]
	} else {
		p.ownedDomains = make([]*nativeDomainState, 0, domainCount)
	}
	if collectionCount <= len(p.inlineCollections) {
		p.ownedCollections = p.inlineCollections[:0]
	} else {
		p.ownedCollections = make([]*nativeCollectionState, 0, collectionCount)
	}
	e.refs.Store(1)
	e.cleanup.Work = e
	if base.inventory != nil {
		base.inventory.retain()
		e.inventory = base.inventory
	}
	return e, nil
}

func nativeEnginePreparationCharge(collectionCount, domainCount int) (uint64, error) {
	if collectionCount < 0 || domainCount < 0 {
		return 0, memtable.ErrCOWCapacity
	}
	fused := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineVersionWithPrep{})))
	separate := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineVersion{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEnginePreparation{})))
	header := max(fused, separate)
	charge := header + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{})))
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	if collectionCount > len(nativeEnginePreparation{}.inlineCollections) {
		if uint64(collectionCount) > ^uint64(0)/ptr {
			return 0, memtable.ErrCOWCapacity
		}
		backing := memtable.COWAllocationCharge(uint64(collectionCount) * ptr)
		if backing > ^uint64(0)-charge {
			return 0, memtable.ErrCOWCapacity
		}
		charge += backing
	}
	if domainCount > len(nativeEnginePreparation{}.inlineDomains) {
		if uint64(domainCount) > ^uint64(0)/ptr {
			return 0, memtable.ErrCOWCapacity
		}
		backing := memtable.COWAllocationCharge(uint64(domainCount) * ptr)
		if backing > ^uint64(0)-charge {
			return 0, memtable.ErrCOWCapacity
		}
		charge += backing
	}
	return charge, nil
}

func (p *nativeEnginePreparation) finish() (done cowbtree.OwnedMapCompletion, failure error) {
	if p == nil {
		return done, nil
	}
	if p.names != nil {
		done.Append(p.names.Cancel())
		p.names = nil
	}
	if p.collections != nil {
		done.Append(p.collections.Cancel())
		p.collections = nil
	}
	if p.pending != nil {
		done.Append(p.pending.Cancel())
		p.pending = nil
	}
	if p.domainBuilder != nil {
		done.Append(p.domainBuilder.Cancel())
		p.domainBuilder = nil
	}
	if p.pendingDomainBuilder != nil {
		done.Append(p.pendingDomainBuilder.Cancel())
		p.pendingDomainBuilder = nil
	}
	child, err := finishNativeDomainDirectory(&p.domains)
	done.Append(child)
	failure = err
	child, err = finishNativeDomainDirectory(&p.pendingDomains)
	done.Append(child)
	if failure == nil {
		failure = err
	}
	for i, c := range p.ownedCollections {
		child, err = (nativeCollectionLifetime{}).Release(c)
		done.Append(child)
		if err == nil {
			p.ownedCollections[i] = nil
		}
		c = nil
		if failure == nil {
			failure = err
		}
	}
	for i, d := range p.ownedDomains {
		child, err = (nativeDomainLifetime{}).Release(d)
		done.Append(child)
		if err == nil {
			p.ownedDomains[i] = nil
		}
		d = nil
		if failure == nil {
			failure = err
		}
	}
	if failure != nil {
		return done, failure
	}
	done.Append(p.schema.release())
	p.schema = nil
	clear(p.inlineDomains[:])
	clear(p.inlineCollections[:])
	p.ownedDomains, p.ownedCollections = nil, nil
	return done, nil
}
