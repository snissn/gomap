package collections

import (
	"encoding/binary"
	"reflect"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeTypedBuildCapacity inventories the exact captured cohort without
// allocating a historical row projection. All deltas retain command identity.
// The first producer slice has one physical typed owner in an atomic command.
func nativeTypedBuildCapacity(e *nativeEngineVersion, basis *backenddb.Snapshot) (charge uint64, collection *nativeCollectionState, count int, err error) {
	e.pending.Scan(func(_ uint64, c *nativeCollectionState) bool {
		primary, ok := c.domains.Get("primary")
		if !ok || primary.typed == nil {
			return true
		}
		if collection != nil || basis == nil {
			err = backenddb.ErrNativeSemanticUnsupported
			return false
		}
		collection = c
		var source uint64
		for d := primary.typed; d != nil; d = d.previous {
			if d.buildCapacity > ^uint64(0)-source {
				err = memtable.ErrCOWCapacity
				return false
			}
			source += d.buildCapacity
			count++
		}
		charge, err = nativeTypedCollectionBuildCapacity(*c.schema.meta, source)
		if err != nil {
			return false
		}
		manifest, _ := c.domains.Get("column/manifest")
		var root uint64
		if manifest != nil {
			root = manifest.baseRoot
		}
		base, e := nativeTypedManifestCapacity(basis, root)
		if e != nil {
			err = e
			return false
		}
		if err = nativeTypedCheckedAdd(&charge, base); err != nil {
			return false
		}
		// SplitNativeBytes rounds the child aggregate and owns one receipt.
		charge = memtable.COWAllocationCharge(charge)
		pointers := memtable.COWAllocationCharge(uint64(count) * uint64(unsafe.Sizeof((*nativeTypedDelta)(nil))))
		for _, n := range []uint64{memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{}))), pointers} {
			if n > ^uint64(0)-charge {
				err = memtable.ErrCOWCapacity
				return false
			}
			charge += n
		}
		return true
	})
	return
}

// prepareTyped adopts the materializer's original partial before observing its
// error. Only this original accepted Work owns its rollback/failure closure.
func (b *nativeEngineBuild) prepareTyped(c *nativeCollectionState, count int, basis *backenddb.Snapshot, capture *backenddb.StableResourceCaptureLease, group *backenddb.RootPublicationBuildGroup) error {
	if c == nil {
		return nil
	}
	primary, _ := c.domains.Get("primary")
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
	b.typedCollection = c.incarnation
	work, err := prepareNativeTypedMaterialization(nativeTypedMaterializeInput{meta: *c.schema.meta, baseManifestRoot: root, deltas: deltas, snapshot: basis, capture: capture, allocator: &b.allocation, group: group})
	b.typed = work
	if err != nil {
		b.failure = err
	}
	return err
}

func (b *nativeEngineBuild) materializeTyped(group *backenddb.RootPublicationBuildGroup) error {
	if b.typed == nil {
		return nil
	}
	if err := b.typed.materialize(); err != nil {
		return err
	}
	for i := range b.built {
		result := &b.built[i]
		if result.collection != b.typedCollection {
			continue
		}
		found := false
		for _, root := range b.typed.roots {
			if root.name == result.name {
				result.root = root.root
				found = true
				break
			}
		}
		if !found {
			return backenddb.ErrNativeSemanticUnsupported
		}
		result.typedMeta = &b.typed.meta
		binary.BigEndian.PutUint64(result.rootValue[:], result.root)
	}
	if err := group.SetNativeDurableResources(b.typed.resources, b.typed.requirements); err != nil {
		return err
	}
	b.typed.resources = nil // exact complete closure now belongs to original group
	encoded, err := encodeCollectionMeta(b.typed.meta)
	if err != nil {
		return err
	}
	delta := b.domains[0].delta
	defer delta.Reset()
	if err = delta.Set([]byte(systemCollectionMetaKey(b.typed.meta.Name)), encoded); err != nil {
		return err
	}
	return group.BuildNativeSystemDelta(delta)
}

// Native schema replacements and newer typed header suffixes are part of BOTH
// handoff promises, not an allocation discovered after physical acceptance.
func nativeTypedHandoffCharge(c *nativeCollectionState, maxSources int) (uint64, error) {
	primary, ok := c.domains.Get("primary")
	if !ok || primary.typed == nil {
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
	typed, _, count, err := nativeTypedBuildCapacity(e, basis)
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
