package collections

import (
	"encoding/binary"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeBuildAllocation spends only the original accepted construction
// promise. Children contain the existing budget/charge, with no parent/Work
// backpointer. Constructors borrow this capability only while preparing private
// headers; published successors retain the original bare budget capabilities.
type nativeBuildAllocation struct {
	backenddb.NativeSemanticAllocator
	parent *memtable.COWExternalLease
	floor  uint64
}

func (a *nativeBuildAllocation) AdmitNativeBytes(n uint64) (*memtable.COWExternalLease, error) {
	return a.parent.SplitNativeBytes(n, a.floor)
}

// nativeEngineBuild is the captured state's original sparse physical ledger.
// It owns exact domain edges, never an EngineVersion, Snapshot, group, or cache
// ancestor. Failed partials and the first failure stay here without replay.
// This construction helper is not the complete NativeSemanticState build: the
// original system-catalog and durable-resource closure must join the same group
// before any caller may activate the higher-level interface.
type nativeEngineBuild struct {
	domains      []*nativeDomainBuild
	built        []nativeBuiltDomain
	catalog      *nativeDomainBuild
	catalogBuilt nativeBuiltDomain
	allocation   nativeBuildAllocation
	collections  int
	handoffs     int
	partial      *nativeEngineVersion
	failure      error
	started      bool
	complete     bool
	refund       *cowbtree.OwnedMapRefund
}

// prepareNativeEngineBuild reserves all domain scratch and BOTH sparse
// successors before changing the original ledger. Only the initial capacity
// refusal proves that no allocation/ledger mutation has started. Every later
// fallible constructor is preceded by adoption into captured.build.
func prepareNativeEngineBuild(captured *nativeEngineVersion) (*nativeEngineBuild, error) {
	if captured == nil || captured.maintenance == nil || captured.allocator == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if captured.build != nil {
		if captured.build.failure != nil {
			return captured.build, captured.build.failure
		}
		return captured.build, backenddb.ErrNativeSemanticUnsupported
	}
	promise, collections, domains, err := nativeHandoffCapacity(captured)
	if err != nil {
		return nil, err
	}
	bytes, floor, err := nativeEngineBuildCapacity(domains, captured.allocator.Limits().MaxSources, promise)
	if err != nil {
		return nil, err
	}
	keys, err := nativeEngineRootDescriptorKeyCharge(captured)
	if err != nil || keys > ^uint64(0)-bytes {
		return nil, memtable.ErrCOWCapacity
	}
	bytes, floor = bytes+keys, floor+keys
	lease, err := captured.maintenance.AdmitNativeBytes(bytes)
	if err != nil {
		if errors.Is(err, memtable.ErrCOWCapacity) {
			return nil, backenddb.NativeSemanticBuildRetryableRefusal{Cause: err}
		}
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	b := &nativeEngineBuild{
		domains: make([]*nativeDomainBuild, 0, domains),
		built:   make([]nativeBuiltDomain, 0, domains), collections: collections,
		allocation: nativeBuildAllocation{NativeSemanticAllocator: captured.maintenance, parent: lease, floor: floor},
		refund:     refund,
	}
	captured.build = b
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return b.allocation.AdmitNativeBytes(n) }
	if nativeCatalogHasPending(captured.catalog) {
		source := captured.catalog
		if source.name != nativeCatalogDomainName || source.baseRoot != 0 || source.interpretation != 1 || source.storagePolicy != 0 {
			b.failure = backenddb.ErrNativeSemanticUnsupported
			return b, b.failure
		}
		b.catalog, err = newNativeDomainBuild(source, captured.allocator.Limits().MaxSources, admit)
		if err != nil {
			b.failure = err
			return b, err
		}
		b.catalog.catalog = true
		boundary := source.pending
		if boundary == nil {
			boundary = source.captured.source.root
		}
		b.catalogBuilt = nativeBuiltDomain{name: nativeCatalogDomainName, interpretation: 1, boundary: boundary}
	}
	captured.pending.Scan(func(id uint64, c *nativeCollectionState) bool {
		if c.incarnation != id {
			b.failure = backenddb.ErrNativeSemanticUnsupported
			return false
		}
		c.pending.Scan(func(name string, d *nativeDomainState) bool {
			if len(b.domains) == cap(b.domains) || name != d.name {
				b.failure = backenddb.ErrNativeSemanticUnsupported
				return false
			}
			work, err := newNativeDomainBuild(d, captured.allocator.Limits().MaxSources, admit)
			if work != nil {
				b.domains = append(b.domains, work)
			}
			if err != nil {
				b.failure = err
				return false
			}
			boundary := d.pending
			if boundary == nil && d.captured != nil {
				boundary = d.captured.source.root
			}
			b.built = append(b.built, nativeBuiltDomain{
				collection: id, interpretation: d.interpretation, name: d.name,
				oldBaseRoot: d.baseRoot, root: d.baseRoot, boundary: boundary,
				empty:   boundary == nil,
				rootKey: systemCollectionRootPrefix + c.schema.meta.Name + "/" + name,
			})
			return true
		})
		return b.failure == nil
	})
	return b, b.failure
}

// nativeEngineBuildCapacity uses the SAME constructor charges and independent
// lease headers. floor is retained original ledger storage, not capacity that
// child constructors can consume. The enclosing parent adds its own lease
// header once through AdmitNativeBytes.
func nativeEngineBuildCapacity(domains, maxSources int, handoff uint64) (total, floor uint64, err error) {
	if domains < 0 || maxSources <= 0 {
		return 0, 0, memtable.ErrCOWCapacity
	}
	floor = memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineBuild{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	for _, item := range [2]uint64{uint64(unsafe.Sizeof((*nativeDomainBuild)(nil))), uint64(unsafe.Sizeof(nativeBuiltDomain{}))} {
		if uint64(domains) > ^uint64(0)/item {
			return 0, 0, memtable.ErrCOWCapacity
		}
		bytes := memtable.COWAllocationCharge(uint64(domains) * item)
		if bytes > ^uint64(0)-floor {
			return 0, 0, memtable.ErrCOWCapacity
		}
		floor += bytes
	}
	child, err := nativeDomainBuildCharge(maxSources)
	if err != nil {
		return 0, 0, err
	}
	child = memtable.COWAllocationCharge(child)
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if child > ^uint64(0)-header || uint64(domains) > (^uint64(0)-floor)/(child+header) {
		return 0, 0, memtable.ErrCOWCapacity
	}
	total = floor + uint64(domains)*(child+header)
	if handoff > ^uint64(0)-total {
		return 0, 0, memtable.ErrCOWCapacity
	}
	return total + handoff, floor, nil
}

func (b *nativeEngineBuild) materializeDomains(group nativeOrderedRootBuilder) error {
	if b.failure != nil {
		return b.failure
	}
	if b.started || group == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	b.started = true
	if len(b.domains) != 0 {
		if _, ok := group.(nativeSystemRootBuilder); !ok {
			b.failure = backenddb.ErrNativeSemanticUnsupported
			return b.failure
		}
	}
	if b.catalog != nil {
		if _, err := b.catalog.materialize(group); err != nil {
			b.failure = err
			return err
		}
	}
	for i, work := range b.domains {
		root, err := work.materialize(group)
		if err != nil {
			b.failure = err
			return err
		}
		b.built[i].root = root
		binary.BigEndian.PutUint64(b.built[i].rootValue[:], root)
	}
	if err := b.materializeRootDescriptors(group); err != nil {
		b.failure = err
		return err
	}
	b.complete = true
	return nil
}

func (b *nativeEngineBuild) prepareHandoff(latest *nativeEngineVersion) (*nativeEngineVersion, error) {
	if b.failure != nil {
		return nil, b.failure
	}
	if !b.complete || b.handoffs == 2 || latest == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	b.handoffs++
	var catalog *nativeBuiltDomain
	if b.catalog != nil {
		catalog = &b.catalogBuilt
	}
	next, err := prepareNativeEngineHandoffWithCatalog(latest, b.built, b.collections, catalog, &b.allocation)
	if err != nil {
		b.partial, b.failure = next, err
		return nil, err // partial belongs to this original, never the caller
	}
	return next, nil
}

// finish runs before the containing Engine's directories can drop any source
// edge. Returned receipts cannot refund until its caller clears e.build and
// every containing work slot. A failed original ledger is never destructed.
func (b *nativeEngineBuild) finish() (done cowbtree.OwnedMapCompletion, err error) {
	if b.failure != nil {
		return done, b.failure
	}
	clear(b.built)
	b.built = nil
	b.catalogBuilt = nativeBuiltDomain{}
	if b.catalog != nil {
		child, err := b.catalog.finish()
		done.Append(child)
		if err != nil {
			b.failure = err
			return done, err
		}
		b.catalog = nil
	}
	for i, work := range b.domains {
		child, err := work.finish()
		done.Append(child)
		if err != nil {
			b.failure = err
			return done, err
		}
		b.domains[i] = nil
		work = nil
	}
	b.domains = nil
	b.allocation = nativeBuildAllocation{}
	refund := b.refund
	b.refund = nil
	done.Append(refund.Completion())
	return done, nil
}
