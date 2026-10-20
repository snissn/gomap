package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
	lease, err := a.parent.SplitNativeBytes(n, a.floor)
	if err != nil {
		return nil, fmt.Errorf("collections: native build split bytes=%d floor=%d: %w", n, a.floor, err)
	}
	return lease, nil
}

// nativeEngineBuild is the captured state's original sparse physical ledger.
// It owns exact domain edges, never an EngineVersion, Snapshot, group, or cache
// ancestor. Failed partials and the first failure stay here without replay.
// This construction helper is not the complete NativeSemanticState build: the
// original system-catalog and durable-resource closure must join the same group
// before any caller may activate the higher-level interface.
type nativeEngineBuild struct {
	typed             []nativeTypedBuildWork
	typedResources    *rootpublication.StableResourceSetBuilder
	typedClosure      *rootpublication.StableResourceSet
	typedRequirements rootpublication.StableLogicalObligationRequirements
	domains           []*nativeDomainBuild
	built             []nativeBuiltDomain
	catalog           *nativeDomainBuild
	catalogBuilt      nativeBuiltDomain
	allocation        nativeBuildAllocation
	collections       int
	handoffs          int
	partial           *nativeEngineVersion
	failure           error
	started           bool
	complete          bool
	refund            *cowbtree.OwnedMapRefund
}

// prepareNativeEngineBuild reserves all domain scratch and BOTH sparse
// successors before changing the original ledger. Only the initial capacity
// refusal proves that no allocation/ledger mutation has started. Every later
// fallible constructor is preceded by adoption into captured.build.
func prepareNativeEngineBuild(captured *nativeEngineVersion) (*nativeEngineBuild, error) {
	return prepareNativeEngineBuildAt(captured, nil, nil, nil)
}
func prepareNativeEngineBuildAt(captured *nativeEngineVersion, basis *backenddb.Snapshot, capture *backenddb.StableResourceCaptureLease, group *backenddb.RootPublicationBuildGroup) (*nativeEngineBuild, error) {
	return prepareNativeEngineBuildForCompaction(captured, basis, capture, group, nil)
}
func prepareNativeEngineBuildForCompaction(captured *nativeEngineVersion, basis *backenddb.Snapshot, capture *backenddb.StableResourceCaptureLease, group *backenddb.RootPublicationBuildGroup, operation *nativeColumnCompaction) (*nativeEngineBuild, error) {
	var target uint64
	if operation != nil {
		target = operation.target
	}

	if captured == nil || captured.maintenance == nil || captured.allocator == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if captured.build != nil {
		if captured.build.failure != nil {
			return captured.build, captured.build.failure
		}
		return captured.build, backenddb.ErrNativeSemanticUnsupported
	}
	promise, collections, domains, err := nativeHandoffCapacityForCompaction(captured, target)
	if err != nil {
		return nil, err
	}
	bytes, floor, err := nativeEngineBuildCapacity(domains, captured.allocator.Limits().MaxSources, promise)
	if err != nil {
		return nil, err
	}
	keys, err := nativeEngineRootDescriptorKeyChargeForCompaction(captured, target)
	if err != nil || keys > ^uint64(0)-bytes {
		return nil, memtable.ErrCOWCapacity
	}
	bytes, floor = bytes+keys, floor+keys
	typedBytes, typedFloor, typedCollections, _, err := nativeTypedBuildCapacityForCompaction(captured, basis, target)
	if err != nil {
		return nil, err
	}
	if typedBytes > ^uint64(0)-bytes {
		return nil, memtable.ErrCOWCapacity
	}
	bytes += typedBytes
	if err = nativeTypedCheckedAdd(&floor, typedFloor); err != nil {
		return nil, err
	}
	lease, err := captured.maintenance.AdmitNativeBytes(bytes)
	if err != nil {
		err = fmt.Errorf("collections: native build reservation bytes=%d floor=%d typed_bytes=%d typed_floor=%d: %w", bytes, floor, typedBytes, typedFloor, err)
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
	scanNativeBuildCollections(captured, target, func(id uint64, c *nativeCollectionState) bool {
		if c.incarnation != id {
			b.failure = backenddb.ErrNativeSemanticUnsupported
			return false
		}
		domainSet := c.pending
		if id == target {
			domainSet = c.domains
		}
		domainSet.Scan(func(name string, d *nativeDomainState) bool {
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
				oldBaseRoot: d.baseRoot, root: d.baseRoot, boundary: boundary, typedBoundary: d.typed,
				empty: boundary == nil, physicalOnly: id == target && nativeTypedPhysicalDomain(name),
				rootKey: systemCollectionRootPrefix + c.schema.meta.Name + "/" + name,
			})
			return true
		})
		return b.failure == nil
	})
	if b.failure == nil {
		err = b.prepareTypedForCompaction(captured, typedCollections, basis, capture, group, operation)
		if err != nil {
			return b, err
		}
	}
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
	defer func() {
		// Catalog or ordinary-domain failure can precede all typed work. Drop
		// every borrowed input while preserving the original output custody.
		for _, input := range b.typed {
			if input.work != nil {
				input.work.input = nativeTypedMaterializeInput{}
			}
		}
	}()
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
		if b.hasTypedCollection(b.built[i].collection) && nativeTypedPhysicalDomain(b.built[i].name) {
			continue
		}
		root, err := work.materialize(group)
		if err != nil {
			b.failure = err
			return err
		}
		b.built[i].root = root
		binary.BigEndian.PutUint64(b.built[i].rootValue[:], root)
	}
	if len(b.typed) != 0 {
		physical, ok := group.(*backenddb.RootPublicationBuildGroup)
		if !ok {
			b.failure = backenddb.ErrNativeSemanticUnsupported
			return b.failure
		}
		if err := b.materializeTyped(physical); err != nil {
			b.failure = err
			return err
		}
	}
	if err := b.materializeRootDescriptors(group); err != nil {
		b.failure = err
		return err
	}
	if len(b.typed) != 0 {
		// All synchronous materialization, codec Close, resource validation and
		// root construction have ended. Only retained outputs and the original
		// two handoff promises survive. Do not keep the operation peak charged
		// until this captured engine's asynchronous last-reference cleanup: a
		// following checkpoint needs the SAME sequential workspace again.
		workspace, err := nativeTypedBuildWorkspaceCapacity()
		if err != nil || workspace > b.allocation.floor {
			b.failure = memtable.ErrCOWCapacity
			return b.failure
		}
		for _, input := range b.typed {
			input.work.input = nativeTypedMaterializeInput{}
		}
		floor := b.allocation.floor - workspace
		if err = b.allocation.parent.ShrinkNativeBytes(workspace, floor); err != nil {
			b.failure = err
			return err
		}
		b.allocation.floor = floor
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
	if len(b.typed) != 0 {
		for i := range b.built {
			b.built[i].typedMeta = nil
		}
		for i := range b.typed {
			work := b.typed[i].work
			child, e := work.release()
			done.Append(child)
			if e != nil {
				b.failure = e
				return done, e
			}
			b.typed[i] = nativeTypedBuildWork{}
			work = nil
		}
		b.typed = nil
	}
	if b.typedResources != nil {
		if err = b.typedResources.AbandonWithError(); err != nil {
			b.failure = err
			return done, err
		}
		b.typedResources = nil
	}
	if b.typedClosure != nil {
		if err = b.typedClosure.ReleaseWithError(); err != nil {
			b.failure = err
			return done, err
		}
		b.typedClosure = nil
	}
	b.typedRequirements = rootpublication.StableLogicalObligationRequirements{}
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
