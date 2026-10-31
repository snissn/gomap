package collections

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// nativeTypedBuildCapacity inventories every captured typed prefix. Source,
// manifest and output custody sum across collections. The largest applicable
// operation peak stays in the original parent because materialization is sequential.
func nativeTypedBuildCapacity(e *nativeEngineVersion, basis *backenddb.Snapshot) (uint64, uint64, int, int, error) {
	return nativeTypedBuildCapacityForCompaction(e, basis, 0)
}
func nativeTypedBuildCapacityForCompaction(e *nativeEngineVersion, basis *backenddb.Snapshot, target uint64) (uint64, uint64, int, int, error) {
	inventory, err := nativeTypedBuildInventoryForCompaction(e, basis, target)
	return inventory.charge, inventory.floor, inventory.collections, inventory.count, err
}

// nativeTypedBuildInventory exposes the once-per-build maximum from the same
// checked scan that includes it in charge. It owns no state or allocation.
type nativeTypedBuildInventory struct {
	charge, floor, workspace uint64
	collections, count       int
}

func nativeTypedBuildInventoryForCompaction(e *nativeEngineVersion, basis *backenddb.Snapshot, target uint64) (inventory nativeTypedBuildInventory, err error) {
	scanNativeBuildCollections(e, target, func(id uint64, c *nativeCollectionState) bool {
		primary, ok := c.domains.Get("primary")
		if !ok || (primary.typed == nil && id != target) {
			return true
		}
		if basis == nil {
			err = backenddb.ErrNativeSemanticUnsupported
			return false
		}
		localWorkspace, workspaceErr := nativeTypedOperationWorkspaceCapacity(*c.schema.meta, id == target, nativeTypedGraphOnly(primary.typed))
		if workspaceErr != nil {
			err = workspaceErr
			return false
		}
		inventory.workspace = max(inventory.workspace, localWorkspace)
		inventory.collections++
		var source uint64
		members := 0
		for d := primary.typed; d != nil; d = d.previous {
			if err = nativeTypedCheckedAdd(&source, d.buildCapacity); err != nil {
				return false
			}
			members++
		}
		if members > e.allocator.Limits().MaxSources || members > int(^uint(0)>>1)-inventory.count {
			err = memtable.ErrCOWCapacity
			return false
		}
		inventory.count += members
		owned, buildErr := nativeTypedCollectionRetainedCapacity(*c.schema.meta, source)
		if buildErr != nil {
			err = buildErr
			return false
		}
		manifest, _ := c.domains.Get("column/manifest")
		var root uint64
		if manifest != nil {
			root = manifest.baseRoot
		}
		base, e := nativeTypedBasisCapacity(*c.schema.meta, basis, root)
		if e != nil {
			err = e
			return false
		}
		if id == target {
			read, readErr := nativeTypedCompactionReadRetainedCapacity(*c.schema.meta, basis, root)
			if readErr != nil {
				err = readErr
				return false
			}
			if err = nativeTypedCheckedAdd(&owned, read); err != nil {
				return false
			}
		}
		if err = nativeTypedCheckedAdd(&owned, base); err != nil {
			return false
		}
		// Each retained work owns its original rounded child and receipt.
		if err = nativeTypedCheckedAdd(&inventory.charge, memtable.COWAllocationCharge(owned)); err != nil {
			return false
		}
		if err = nativeTypedCheckedAdd(&inventory.charge, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))); err != nil {
			return false
		}
		pointers, e := nativeTypedCheckedMul(uint64(members), uint64(unsafe.Sizeof((*nativeTypedDelta)(nil))))
		if e != nil {
			err = e
			return false
		}
		err = nativeTypedCheckedAdd(&inventory.floor, memtable.COWAllocationCharge(pointers))
		return err == nil
	})
	if err != nil || inventory.collections == 0 {
		return
	}
	works, worksErr := nativeTypedCheckedMul(uint64(inventory.collections), uint64(unsafe.Sizeof(nativeTypedBuildWork{})))
	if worksErr != nil {
		err = worksErr
		return
	}
	for _, n := range []uint64{inventory.workspace, memtable.COWAllocationCharge(works), memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.StableResourceSetBuilder{}))), memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.StableResourceSet{})))} {
		if err = nativeTypedCheckedAdd(&inventory.floor, n); err != nil {
			return
		}
	}
	err = nativeTypedCheckedAdd(&inventory.charge, inventory.floor)
	return
}

type nativeTypedBuildWork struct {
	workspace  uint64 // Captured operation peak; work.input is cleared after materialization.
	compaction bool
	collection uint64
	work       *nativeTypedMaterialization
}

// prepareTyped adopts every original partial before observing its error. The
// same parent retains shared workspace through materialization; pointer arrays
// and retained outputs remain covered until final finish.
func (b *nativeEngineBuild) prepareTyped(e *nativeEngineVersion, collections int, basis *backenddb.Snapshot, capture *backenddb.StableResourceCaptureLease, group *backenddb.RootPublicationBuildGroup) error {
	return b.prepareTypedForCompaction(e, collections, basis, capture, group, nil)
}
func (b *nativeEngineBuild) prepareTypedForCompaction(e *nativeEngineVersion, collections int, basis *backenddb.Snapshot, capture *backenddb.StableResourceCaptureLease, group *backenddb.RootPublicationBuildGroup, operation *nativeColumnCompaction) error {
	var target uint64
	if operation != nil {
		target = operation.target
	}

	if collections == 0 {
		return nil
	}
	b.typed = make([]nativeTypedBuildWork, 0, collections)
	defer func() {
		if b.failure != nil {
			for _, input := range b.typed {
				if input.work != nil {
					input.work.input = nativeTypedMaterializeInput{}
				}
			}
		}
	}()
	scanNativeBuildCollections(e, target, func(id uint64, c *nativeCollectionState) bool {
		primary, ok := c.domains.Get("primary")
		if !ok || (primary.typed == nil && id != target) {
			return true
		}
		count := 0
		for d := primary.typed; d != nil; d = d.previous {
			count++
		}
		deltas := make([]*nativeTypedDelta, count)
		i := count
		for d := primary.typed; d != nil; d = d.previous {
			i--
			deltas[i] = d
		}
		var root uint64
		if manifest, ok := c.domains.Get("column/manifest"); ok {
			root = manifest.baseRoot
		}
		var compaction *nativeColumnCompaction
		var capturedCollection *nativeCollectionState
		if id == target {
			compaction, capturedCollection = operation, c
		}
		sharedWorkspace, err := nativeTypedOperationWorkspaceCapacity(*c.schema.meta, compaction != nil, nativeTypedGraphOnly(primary.typed))
		if err != nil {
			b.failure = err
			return false
		}
		work, err := prepareNativeTypedMaterialization(nativeTypedMaterializeInput{compaction: compaction, capturedCollection: capturedCollection, meta: *c.schema.meta, baseManifestRoot: root, deltas: deltas, snapshot: basis, capture: capture, allocator: &b.allocation, group: group, sharedWorkspace: sharedWorkspace})
		b.typed = append(b.typed, nativeTypedBuildWork{workspace: sharedWorkspace, compaction: compaction != nil, collection: c.incarnation, work: work})
		b.failure = err
		return err == nil
	})
	return b.failure
}

func (b *nativeEngineBuild) hasTypedCollection(id uint64) bool {
	for _, input := range b.typed {
		if input.collection == id {
			return true
		}
	}
	return false
}

// Only these roots are produced by the maintained physical typed builder.
// Scalar index/state roots use the original ordinary domain builder.
func nativeTypedPhysicalDomain(name string) bool {
	switch name {
	case "primary", "column/manifest", "column/row-locator", "retained/semantic-stream-v1":
		return true
	}
	return false
}

func (b *nativeEngineBuild) materializeTyped(group *backenddb.RootPublicationBuildGroup) error {
	if len(b.typed) == 0 {
		return nil
	}
	// The original group borrows the final requirements synchronously. Once
	// transferred (or failed), the retained build must not keep captured readers
	// alive past group cleanup. Resource custody remains in its original cells.
	defer func() {
		b.typedRequirements = rootpublication.StableLogicalObligationRequirements{}
		for _, input := range b.typed {
			input.work.requirements = rootpublication.StableLogicalObligationRequirements{}
			input.work.fallback = nil
			input.work.sourceDirectory = nil
		}
	}()
	// Keep aggregate custody in the SAME accepted ledger before any merge or
	// later collection can fail. Transfer only the final complete closure.
	b.typedResources = rootpublication.NewStableResourceSetBuilder()
	var appendMutation rootpublication.StableLogicalObligationMutation
	var requirementWork rootpublication.StableResourceClosureWork
	hasAppend := false
	for _, input := range b.typed {
		work := input.work
		if err := work.materialize(); err != nil {
			return fmt.Errorf("native typed collection=%d: %w", input.collection, err)
		}
		for i := range b.built {
			result := &b.built[i]
			if result.collection != input.collection {
				continue
			}
			// The original handoff reads metadata from the first sorted domain;
			// secondary indexes can sort before primary. Every result carries
			// the same immutable physical metadata, including ordinary indexes.
			result.typedMeta = &work.meta
			result.ordinaryGraphSourceCodecs = work.ordinaryGraphSourceCodecs
			if !nativeTypedPhysicalDomain(result.name) {
				continue
			}
			found := false
			for _, root := range work.roots {
				if root.name == result.name {
					result.root = root.root
					found = true
					break
				}
			}
			if !found {
				return backenddb.ErrNativeSemanticUnsupported
			}
			binary.BigEndian.PutUint64(result.rootValue[:], result.root)
		}
		requirements, err := rootpublication.MergeStableLogicalObligationRequirements(b.typedRequirements, work.requirements)
		if err != nil {
			return err
		}
		if input.compaction {
			// Pending assets may have been superseded inside this SAME group. The
			// candidate producer must contain only the final scoped obligations. Keep
			// the original unfiltered producer in its original work until release.
			selected, selectErr := rootpublication.CloneStableResourceSetForLogicalObligations(work.resources, work.requirements)
			if selectErr != nil {
				return selectErr
			}
			b.typedClosure = selected // original ledger adopts before fallible merge
			if selected != nil {
				if err = b.typedResources.Merge(selected); err != nil {
					return err
				}
			}
			b.typedClosure = nil
		} else {
			if err = b.typedResources.Merge(work.resources); err != nil {
				return err
			}
			work.resources = nil
		}
		b.typedRequirements = requirements
		if work.fallback != nil {
			appendMutation, err = rootpublication.MergeStableLogicalObligationMutations(appendMutation, work.appendMutation)
			if err != nil {
				return err
			}
			requirementWork.Add(work.requirementWork)
			hasAppend = true
		}
		encoded, err := encodeCollectionMeta(work.meta)
		if err != nil {
			return err
		}
		delta := b.domains[0].delta
		if err = delta.Set([]byte(systemCollectionMetaKey(work.meta.Name)), encoded); err == nil {
			err = group.BuildNativeSystemDelta(delta)
		}
		delta.Reset()
		if err != nil {
			return err
		}
	}
	resources, err := b.typedResources.Freeze()
	if err != nil {
		return err
	}
	b.typedResources = nil
	b.typedClosure = resources
	if hasAppend {
		requirements := b.typedRequirements
		err = group.SetNativeDurableResourceAppend(resources, requirements, appendMutation, requirementWork, func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
			return requirements, rootpublication.StableResourceClosureWork{}, nil
		})
	} else {
		err = group.SetNativeDurableResources(resources, b.typedRequirements)
	}
	if err != nil {
		return err
	}
	b.typedClosure = nil // exact complete closure now belongs to original group
	return nil
}

// Native schema replacements and newer typed header suffixes are part of BOTH
// handoff promises, not an allocation discovered after physical acceptance.
func nativeTypedHandoffCharge(c *nativeCollectionState, maxSources int) (uint64, error) {
	return nativeTypedHandoffChargeForCompaction(c, maxSources, false)
}
func nativeTypedHandoffChargeForCompaction(c *nativeCollectionState, maxSources int, physicalOnly bool) (uint64, error) {
	primary, ok := c.domains.Get("primary")
	if !ok || (primary.typed == nil && !physicalOnly) {
		return 0, nil
	}
	schema, err := nativeSchemaBackingCharge(reflect.ValueOf(c.schema.meta), 0)
	if err != nil {
		return 0, err
	}
	schema += memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeSchemaState{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	node := nativeTypedDeltaHeaderCharge() + header
	if maxSources < 0 || uint64(maxSources) > (^uint64(0)-schema-header)/node {
		return 0, memtable.ErrCOWCapacity
	}
	return schema + header + uint64(maxSources)*node, nil
}

// nativeTypedConstructionInventory is the shared complete preaccept inventory.
// It owns no allocation receipt or state edge. In particular the producer
// observer must not substitute its watermark for exact final admission.
type nativeTypedConstructionInventory struct {
	total, typed, keys, catalog uint64
	workspace                   uint64
	domains, sources            int
}

func nativeTypedConstructionCapacity(e *nativeEngineVersion, basis *backenddb.Snapshot) (inventory nativeTypedConstructionInventory, err error) {
	typed, err := nativeTypedBuildInventoryForCompaction(e, basis, 0)
	inventory.typed, inventory.sources, inventory.workspace = typed.charge, typed.count, typed.workspace
	if err != nil || inventory.typed == 0 {
		return inventory, err
	}
	if inventory.sources > e.allocator.Limits().MaxSources {
		return inventory, memtable.ErrCOWCapacity
	}
	var promise uint64
	promise, _, inventory.domains, err = nativeHandoffCapacity(e)
	if err != nil {
		return inventory, err
	}
	inventory.total, _, err = nativeEngineBuildCapacity(inventory.domains, e.allocator.Limits().MaxSources, promise)
	if err != nil {
		return inventory, err
	}
	inventory.keys, err = nativeEngineRootDescriptorKeyCharge(e)
	if err != nil {
		return inventory, err
	}
	for _, n := range []uint64{inventory.typed, inventory.keys} {
		if n > ^uint64(0)-inventory.total {
			return inventory, memtable.ErrCOWCapacity
		}
		inventory.total += n
	}
	inventory.catalog, err = nativeCatalogProjectionCapacity(e)
	if err != nil {
		return inventory, err
	}
	if err = nativeTypedCheckedAdd(&inventory.total, inventory.catalog); err != nil {
		return inventory, err
	}
	return inventory, nil
}

// nativeGenerationUnderPressure uses the existing three-quarter scheduling
// policy without overflowing history+pending. This is not request admission.
func nativeGenerationUnderPressure(usage memtable.COWGenerationUsage) bool {
	watermark := usage.MaxGenerationBytes - usage.MaxGenerationBytes/4
	return usage.HistoryBytes >= watermark || usage.PendingBytes >= watermark-usage.HistoryBytes
}

// nativePendingGenerationPressure borrows only active pending roots in this
// retained engine. Captured old sources remain owned but aren't current tails.
// The sparse pending directories avoid a catalog-wide or manifest scan.
func (e *nativeEngineVersion) nativePendingGenerationPressure() (pressure bool, err error) {
	observe := func(d *nativeDomainState) bool {
		if d == nil || d.pending == nil {
			return true
		}
		var usage memtable.COWGenerationUsage
		usage, err = d.pending.GenerationUsage()
		pressure = err == nil && nativeGenerationUnderPressure(usage)
		return err == nil && !pressure
	}
	if !observe(e.catalog) {
		return pressure, err
	}
	e.pending.Scan(func(_ uint64, c *nativeCollectionState) bool {
		c.pending.Scan(func(_ string, d *nativeDomainState) bool { return observe(d) })
		return err == nil && !pressure
	})
	return pressure, err
}

// NativeProducerPressure scans this retained state's active generation tails
// and pending typed prefixes against its paired basis. The zero-typed fast path
// still avoids manifest iterators and the typed handoff/build inventory.
// No result reserves or discounts capacity.
func (e *nativeEngineVersion) NativeProducerPressure(basis *backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) {
	pressure, err := e.nativePendingGenerationPressure()
	if err != nil {
		return backenddb.NativeSemanticProducerPressure{}, err
	}
	inventory, err := nativeTypedConstructionCapacity(e, basis)
	if err != nil || inventory.typed == 0 {
		return backenddb.NativeSemanticProducerPressure{GenerationUnderPressure: pressure}, err
	}
	limits := e.maintenance.Limits()
	return backenddb.NativeSemanticProducerPressure{
		GenerationUnderPressure: pressure,
		ConstructionBytes:       inventory.total,
		FixedConstructionBytes:  inventory.workspace,
		MaxInFlightBytes:        limits.MaxInFlightBytes,
		MaxTotalBytes:           limits.MaxTotalBytes,
		Sources:                 inventory.sources,
		MaxSources:              e.allocator.Limits().MaxSources,
	}, nil
}

// Validate the complete next finite cohort BEFORE canonical assignment. This
// uses the identical inventory and original exact limits, independently of the
// off-gate producer's advisory maintenance watermark.
func nativeTypedEnsureConstructible(e *nativeEngineVersion, basis *backenddb.Snapshot) error {
	inventory, err := nativeTypedConstructionCapacity(e, basis)
	if err != nil || inventory.typed == 0 {
		return err
	}
	limits := e.maintenance.Limits()
	if inventory.total > limits.MaxInFlightBytes || inventory.total > limits.MaxTotalBytes {
		return fmt.Errorf("collections: native typed construction total=%d typed=%d keys=%d catalog=%d domains=%d in_flight_limit=%d total_limit=%d: %w", inventory.total, inventory.typed, inventory.keys, inventory.catalog, inventory.domains, limits.MaxInFlightBytes, limits.MaxTotalBytes, memtable.ErrCOWCapacity)
	}
	return nil
}
