package collections

import (
	"encoding/binary"
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
func nativeTypedBuildCapacityForCompaction(e *nativeEngineVersion, basis *backenddb.Snapshot, target uint64) (charge, floor uint64, collections, count int, err error) {
	var workspace uint64
	scanNativeBuildCollections(e, target, func(id uint64, c *nativeCollectionState) bool {
		primary, ok := c.domains.Get("primary")
		if !ok || (primary.typed == nil && id != target) {
			return true
		}
		if basis == nil {
			err = backenddb.ErrNativeSemanticUnsupported
			return false
		}
		localWorkspace, workspaceErr := nativeTypedOperationWorkspaceCapacity(*c.schema.meta, id == target)
		if workspaceErr != nil {
			err = workspaceErr
			return false
		}
		workspace = max(workspace, localWorkspace)
		collections++
		var source uint64
		members := 0
		for d := primary.typed; d != nil; d = d.previous {
			if err = nativeTypedCheckedAdd(&source, d.buildCapacity); err != nil {
				return false
			}
			members++
		}
		if members > e.allocator.Limits().MaxSources || members > int(^uint(0)>>1)-count {
			err = memtable.ErrCOWCapacity
			return false
		}
		count += members
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
		if err = nativeTypedCheckedAdd(&charge, memtable.COWAllocationCharge(owned)); err != nil {
			return false
		}
		if err = nativeTypedCheckedAdd(&charge, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))); err != nil {
			return false
		}
		pointers, e := nativeTypedCheckedMul(uint64(members), uint64(unsafe.Sizeof((*nativeTypedDelta)(nil))))
		if e != nil {
			err = e
			return false
		}
		err = nativeTypedCheckedAdd(&floor, memtable.COWAllocationCharge(pointers))
		return err == nil
	})
	if err != nil || collections == 0 {
		return
	}
	works, worksErr := nativeTypedCheckedMul(uint64(collections), uint64(unsafe.Sizeof(nativeTypedBuildWork{})))
	if worksErr != nil {
		err = worksErr
		return
	}
	for _, n := range []uint64{workspace, memtable.COWAllocationCharge(works), memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.StableResourceSetBuilder{}))), memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.StableResourceSet{})))} {
		if err = nativeTypedCheckedAdd(&floor, n); err != nil {
			return
		}
	}
	err = nativeTypedCheckedAdd(&charge, floor)
	return
}

type nativeTypedBuildWork struct {
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
		sharedWorkspace, err := nativeTypedOperationWorkspaceCapacity(*c.schema.meta, compaction != nil)
		if err != nil {
			b.failure = err
			return false
		}
		work, err := prepareNativeTypedMaterialization(nativeTypedMaterializeInput{compaction: compaction, capturedCollection: capturedCollection, meta: *c.schema.meta, baseManifestRoot: root, deltas: deltas, snapshot: basis, capture: capture, allocator: &b.allocation, group: group, sharedWorkspace: sharedWorkspace})
		b.typed = append(b.typed, nativeTypedBuildWork{compaction: compaction != nil, collection: c.incarnation, work: work})
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
			return err
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

// Validate the complete next finite cohort BEFORE canonical assignment. This
// is the same construction inventory used by PrepareBuild, not a second budget.
func nativeTypedEnsureConstructible(e *nativeEngineVersion, basis *backenddb.Snapshot) error {
	typed, _, _, count, err := nativeTypedBuildCapacity(e, basis)
	if err != nil {
		return err
	}
	if typed == 0 {
		return nil
	}
	if count > e.allocator.Limits().MaxSources {
		return memtable.ErrCOWCapacity
	}
	promise, _, domains, err := nativeHandoffCapacity(e)
	if err != nil {
		return err
	}
	total, _, err := nativeEngineBuildCapacity(domains, e.allocator.Limits().MaxSources, promise)
	if err != nil {
		return err
	}
	keys, err := nativeEngineRootDescriptorKeyCharge(e)
	if err != nil {
		return err
	}
	for _, n := range []uint64{typed, keys} {
		if n > ^uint64(0)-total {
			return memtable.ErrCOWCapacity
		}
		total += n
	}
	limits := e.maintenance.Limits()
	if total > limits.MaxInFlightBytes || total > limits.MaxTotalBytes {
		return memtable.ErrCOWCapacity
	}
	return nil
}
