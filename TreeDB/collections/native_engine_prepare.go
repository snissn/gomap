package collections

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeEnginePreparation is the original finite private successor ledger.
// Every fallible constructor attaches its result here before the next stage.
// Unsealed builders borrow the lower opening cut; error rollback queues this
// Work before withdrawing that cut, preserving single-worker cleanup order.
// Successful sealing owns real child edges and clears every builder alias.
type nativeEnginePreparation struct {
	names                               *cowbtree.OwnedMapBuilder[string, *nativeCollectionState]
	collections, pending                *cowbtree.OwnedMapBuilder[uint64, *nativeCollectionState]
	domainBuilder, pendingDomainBuilder *cowbtree.OwnedMapBuilder[string, *nativeDomainState]
	domains, pendingDomains             *cowbtree.OwnedMap[string, *nativeDomainState]
	schema                              *nativeSchemaState
	ownedDomains                        []*nativeDomainState
	ownedCollections                    []*nativeCollectionState
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
	e := &nativeEngineVersion{nextIncarnation: base.nextIncarnation, allocator: base.allocator, maintenance: base.maintenance, refund: &nativeEngineRefund{lease: lease}, preparation: &nativeEnginePreparation{ownedDomains: make([]*nativeDomainState, 0, domainCount), ownedCollections: make([]*nativeCollectionState, 0, collectionCount)}}
	e.refs.Store(1)
	e.cleanup.Work = e
	return e, nil
}

func nativeEnginePreparationCharge(collectionCount, domainCount int) (uint64, error) {
	if collectionCount < 0 || domainCount < 0 {
		return 0, memtable.ErrCOWCapacity
	}
	count := uint64(collectionCount) + uint64(domainCount)
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	if count > ^uint64(0)/ptr {
		return 0, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineVersion{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEnginePreparation{})))
	for _, bytes := range [2]uint64{uint64(collectionCount) * ptr, uint64(domainCount) * ptr} {
		backing := memtable.COWAllocationCharge(bytes)
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
	p.ownedDomains, p.ownedCollections = nil, nil
	return done, nil
}
