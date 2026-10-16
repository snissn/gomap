package db

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/zipper"
	"unsafe"
)

// One fixed allocation/accounting row. Charge is the original constructor's
// charge; it is never added to an Owner.Bytes envelope. Raw/capacity operands
// describe that allocation, not another independently chargeable copy.
type NativeRetainedRowForTestV6 struct {
	Present, SamePrimaryOwner, SameRegistryOwner, Embedded, Borrowed, OpaqueBacking bool
	Charge, Raw, Capacity, SecondaryCharge, Phase                                   uint64
	IteratorsMapPresent                                                             bool
	RootTreesEntryRaw                                                               uint64
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeRetainedRowForTestV6(role uint8, w *iterator.OrdinalScanWork) (g NativeRetainedRowForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(3, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(rootPublicationSealV1{}))+2*uint64(unsafe.Sizeof(rootPublicationVisibleInstallV1{}))+2*uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}))+2*uint64(unsafe.Sizeof(Snapshot{}))) {
		return g, false
	}
	if c.db == nil || c.db.rootPublication == nil || c.db.rootPublication.idx == nil || c.db.rootPublication.idx.primary == nil {
		return g, false
	}
	runtime := c.db.rootPublication
	primary := runtime.idx.primary.MetadataOwner()
	registry := c.db.valueLogIdentityPins.MetadataOwner()
	var owner *retainedalloc.Owner
	member, seal := c.candidatePreparation.member, c.sealPreparation.seal
	switch role {
	case 0:
		if member != nil {
			g.Present, g.Raw = true, uint64(unsafe.Sizeof(*member))
			g.Charge = retainedalloc.AllocationCharge(g.Raw)
			owner = primary
		}
	case 1:
		if member != nil && member.install != nil {
			v := member.install
			g.Present, g.Raw, g.Charge = true, uint64(unsafe.Sizeof(*v)), v.metadataCharge
			owner = v.metadata
		}
	case 2:
		if seal != nil {
			g.Present, g.Raw, g.Charge, g.Capacity = true, uint64(unsafe.Sizeof(*seal)), seal.metadataCharge, uint64(cap(seal.prefix))
			owner = seal.metadata
		}
	case 3:
		if seal != nil && seal.commitRuntime != nil {
			v := seal.commitRuntime
			g.Present, g.Raw, g.Charge = true, uint64(unsafe.Sizeof(*v)), v.metadataCharge
			owner = v.metadata
			g.Capacity = uint64(cap(seal.commitRetired))
			g.SecondaryCharge = seal.commitRetiredCharge
		}
	case 4:
		runtime.mu.Lock()
		g.Present, g.Raw, g.Charge = true, uint64(unsafe.Sizeof(*runtime)), runtime.metadataCharge
		owner = runtime.metadata
		capacity := runtime.debtBackingCapacity
		if capacity == 0 {
			capacity = cap(runtime.debt)
		}
		g.Capacity = uint64(capacity)
		g.OpaqueBacking = capacity < cap(runtime.debt) || capacity < len(runtime.debt)
		runtime.mu.Unlock()
	case 5:
		g.Present, g.Embedded, g.Raw, g.Charge, g.Phase = true, true, uint64(unsafe.Sizeof(c.allocatorPreparation)), c.allocatorPreparation.charge, uint64(c.allocatorPreparation.phase)
		owner = c.allocatorPreparation.owner
	case 6:
		g.Present, g.Embedded, g.Raw, g.Phase = true, true, uint64(unsafe.Sizeof(c.rootPreparation)), uint64(c.rootPreparation.phase)
		g.OpaqueBacking = true // claim/group/image provenance is a distinct bank row
	case 7, 8, 9:
		snapshot := c.snapshot
		if role != 7 {
			snapshot = c.sealPreparation.indexCaptures[role-8].snapshot
		}
		if snapshot != nil {
			snapshot.iteratorMu.Lock()
			g.Present, g.Raw, g.Borrowed = true, uint64(unsafe.Sizeof(*snapshot)), true
			g.IteratorsMapPresent = snapshot.iterators != nil
			g.OpaqueBacking = g.IteratorsMapPresent
			snapshot.iteratorMu.Unlock()
			snapshot.rootTreesMu.Lock()
			g.Capacity = uint64(cap(snapshot.rootTrees))
			g.RootTreesEntryRaw = uint64(unsafe.Sizeof(snapshotRootTree{}))
			snapshot.rootTreesMu.Unlock()
		}
	case 10:
		g.Present, g.Embedded, g.Raw, g.Borrowed = true, true, uint64(unsafe.Sizeof(c.publication)), true
	case 11:
		g.Present, g.Embedded, g.Raw, g.Charge = true, true, uint64(unsafe.Sizeof(c.candidatePreparation)), c.candidatePreparation.charge
		owner = c.candidatePreparation.metadata
	case 12:
		g.Present, g.Embedded, g.Raw, g.Charge = true, true, uint64(unsafe.Sizeof(c.sealPreparation)), c.sealPreparation.claimRefundCharge
		owner = c.sealPreparation.claimRefundOwner
	default:
		return g, false
	}
	g.SamePrimaryOwner, g.SameRegistryOwner = owner != nil && owner == primary, owner != nil && owner == registry
	return g, true
}

// Owners are original physical containers, not per-row allocations. Repeated
// roles that compare equal must be counted once; physical rows remain separate.
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeOwnerGeometryForTestV6(role uint8, b retainedalloc.Budget, w *iterator.OrdinalScanWork) (g retainedalloc.NativeOwnerGeometryForTestV6, samePrimary bool, ready bool) {
	if c == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))) {
		return g, false, false
	}
	if c.db == nil || c.db.rootPublication == nil || c.db.rootPublication.idx == nil || c.db.rootPublication.idx.primary == nil {
		return g, false, false
	}
	idx := c.db.rootPublication.idx
	owner := idx.primary.MetadataOwner()
	switch role {
	case 0:
	case 1:
		owner = c.db.valueLogIdentityPins.MetadataOwner()
	case 2:
		g, samePrimary, ready = c.output.MaintenanceNativeOwnerGeometryForTestV6(b, idx.primary.MetadataOwner(), w)
		return g, samePrimary, ready
	default:
		return g, false, false
	}
	samePrimary = owner == idx.primary.MetadataOwner()
	g, ready = owner.MaintenanceNativeOwnerGeometryForTestV6(b, w)
	return
}
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeBankGeometryForTestV6(role uint8, w *iterator.OrdinalScanWork) (g primaryarena.NativeBankGeometryForTestV6, sameSource, sameParent bool, ready bool) {
	if c == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))) {
		return g, false, false, false
	}
	root := &c.rootPreparation
	ref := root.source
	switch role {
	case 0:
	case 1:
		ref = c.sealPreparation.parentRoot
	case 2:
		if c.candidatePreparation.member != nil && c.candidatePreparation.member.install != nil && c.candidatePreparation.member.install.primaryRoot != nil {
			ref = c.candidatePreparation.member.install.primaryRoot.ref
		}
	default:
		return g, false, false, false
	}
	sameSource, sameParent = ref == root.source, ref == c.sealPreparation.parentRoot
	g, ready = root.arena.MaintenanceNativeBankGeometryForTestV6(ref, w)
	return
}
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeFrameGeometryForTestV6(ordinal uint64, a freelist.NativeCOWGeometryForTestV6, w *iterator.OrdinalScanWork) (zipper.NativeMaterializedGeometryForTestV6, bool) {
	if c == nil || w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) {
		return zipper.NativeMaterializedGeometryForTestV6{}, false
	}
	return c.output.MaintenanceNativeFrameGeometryForTestV6(ordinal, a, c.inputWork, w)
}

// Observe the canonical immutable prefix while holding the SAME runtime ->
// allocator -> ledger order. The pure remainder calculator does no admission,
// callback, pager count install, file access or publication.
type NativeRemainderGeometryForTestV6 struct {
	Bound                           freelist.NativePublicationBoundV6
	Records, Bytes, Seals           uint64
	Complete, Unsupported, Overflow bool
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeRemainderGeometryForTestV6(w *iterator.OrdinalScanWork) (g NativeRemainderGeometryForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(3, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}))) {
		return g, false
	}
	if c.db == nil || c.db.rootPublication == nil || c.sealPreparation.seal == nil {
		return g, false
	}
	runtime := c.db.rootPublication
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	var err error
	g.Bound, ready, err = c.allocatorPreparation.preparation.NativePublicationBoundWithWorkV6(c.sealPreparation.seal.prefix, w)
	if err != nil {
		g.Unsupported = true
		return g, true
	}
	if !ready {
		return g, false
	}
	g.Seals = uint64(len(runtime.seals))
	if g.Seals == ^uint64(0) {
		g.Overflow = true
		return g, true
	}
	g.Seals++
	p, ok, e := nativePublicationRemainderForGeometryV6(g.Bound, g.Seals)
	g.Complete, g.Unsupported = ok && e == nil, !ok || e != nil
	g.Records, g.Bytes = p.records, p.bytes
	return g, true
}

type NativeKeyGeometryForTestV6 struct {
	Count, KeyBytes, Floor, RequestValidateBytes uint64
	RequestMatches, SameRequest, End             bool
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeKeyGeometryForTestV6(ordinal uint64, expected mvccadmission.Request, w *iterator.OrdinalScanWork) (g NativeKeyGeometryForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	g.SameRequest = c.request == expected
	var ok bool
	g.Floor, g.RequestValidateBytes, g.RequestMatches, ok = c.request.MaintenanceNativeRequestGeometryForTestV6(w)
	if !ok {
		return g, false
	}
	g.Count = uint64(len(c.records))
	if g.Count == 0 {
		g.Count = 1
	}
	if ordinal >= g.Count {
		g.End = true
		return g, true
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(c.record))) {
		return g, false
	}
	if len(c.records) == 0 {
		g.KeyBytes = uint64(len(c.record.Key))
	} else {
		g.KeyBytes = uint64(len(c.records[ordinal].Key))
	}
	return g, true
}

type NativeSnapshotPriceForTestV6 struct {
	Records, Bytes, OldRefs, NewIDs, TrackerRefs, Acquiring  uint64
	OldDifferent, Stale, Pinned, Holders, Complete, Overflow bool
}

// Observes the same leaf operand used by activation after N1. No temporary
// DBState, stale transition, pin mutation or tracker pruning is constructed.
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeSnapshotPriceForTestV6(w *iterator.OrdinalScanWork) (g NativeSnapshotPriceForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(3, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(leafGenerationPinSet{}))+2*uint64(unsafe.Sizeof(leafGenerationPinTracker{}))+2*uint64(unsafe.Sizeof(snapshotView{}))) {
		return g, false
	}
	if c.db == nil || c.candidatePreparation.member == nil || c.candidatePreparation.member.install == nil {
		return g, false
	}
	db := c.db
	db.rootReuseMu.RLock()
	defer db.rootReuseMu.RUnlock()
	db.mu.RLock()
	defer db.mu.RUnlock()
	next := c.candidatePreparation.member.install.leafGenerationView
	old := db.snapshotViewRO.Load()
	if old != nil && old.state != nil && old.state.LeafGenerations != nil && old.state.LeafGenerations != next {
		g.OldDifferent = true
		s := old.state.LeafGenerations.PinSet
		if s != nil {
			s.mu.Lock()
			g.Stale, g.Pinned, g.Holders = s.stale, s.pinned, s.holders > 0
			g.OldRefs = uint64(len(s.refs))
			s.mu.Unlock()
			if !g.Stale {
				pins := uint64(0)
				if g.Holders && !g.Pinned {
					pins = g.OldRefs
				}
				if pins > ^uint64(0)-1 || pins > (^uint64(0)-uint64(unsafe.Sizeof(*s)))/(uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil)))+16) {
					g.Overflow = true
					return g, true
				}
				g.Records = 1 + pins
				g.Bytes = uint64(unsafe.Sizeof(*s)) + pins*(uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil)))+16)
			}
		}
	}
	if next == nil {
		g.Complete = true
		return g, true
	}
	// snapshotAcquireInFlight reads the actual fixed shard set. Admit each
	// atomic shard load before visiting; never price after an uncharged scan.
	for i := range db.snapshotAcquireRO {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(db.snapshotAcquireRO[i]))) {
			return g, false
		}
		n := db.snapshotAcquireRO[i].Load()
		if n > 0 {
			if uint64(n) > ^uint64(0)-g.Acquiring {
				g.Overflow = true
				return g, true
			}
			g.Acquiring += uint64(n)
		}
	}
	if next != nil && g.Acquiring == 0 {
		tracker := &db.leafGenerationPins
		tracker.mu.Lock()
		g.TrackerRefs, g.NewIDs = uint64(len(tracker.refs)), uint64(len(next.GenerationOrder))
		tracker.mu.Unlock()
		p := nativePublicationRemainderV6{records: g.Records, bytes: g.Bytes}
		if g.NewIDs > (^uint64(0)-uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil))))/8-1 || !p.addN(g.TrackerRefs, 1+g.NewIDs, uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil)))+8*(1+g.NewIDs)) {
			g.Overflow = true
			return g, true
		}
		g.Records, g.Bytes = p.records, p.bytes
	}
	g.Complete = true
	return g, true
}

// All roles borrow original private operands only for this call. Equality is
// evaluated inside the original package/lock; neither addresses nor handles
// become result identities.
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeSetRowForTestV6(role uint8, w *iterator.OrdinalScanWork) (g rootpublication.NativeSetAllocationForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) {
		return g, false
	}
	member := c.candidatePreparation.member
	if member == nil {
		return g, false
	}
	set := c.candidatePreparation.resources
	switch role {
	case 0:
	case 1:
		set = member.resources
	case 2:
		set = c.sealPreparation.resources
	case 3:
		set = c.sealPreparation.proofResources
	case 4:
		if c.sealPreparation.seal != nil {
			set = c.sealPreparation.seal.resources
		} else {
			return g, false
		}
	default:
		return g, false
	}
	return set.MaintenanceNativeSetAllocationForTestV6(c.candidatePreparation.resources, w)
}
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeManifestRowForTestV6(w *iterator.OrdinalScanWork) (g rootpublication.NativeManifestAllocationForTestV6, sameSeal bool, ready bool) {
	if c == nil || w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) {
		return g, false, false
	}
	sameSeal = c.sealPreparation.seal != nil && c.sealPreparation.seal.manifest == c.sealPreparation.manifest
	g, ready = c.sealPreparation.manifest.MaintenanceNativeManifestAllocationForTestV6(w)
	return
}
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeTransactionRowForTestV6(w *iterator.OrdinalScanWork) (g rootpublication.NativeTransactionAllocationForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) {
		return g, false
	}
	var promotion *rootpublication.PrimaryPromotionV6
	if c.sealPreparation.seal != nil && c.sealPreparation.seal.primary != nil {
		promotion = c.sealPreparation.seal.primary.promotion
	}
	return c.transaction.MaintenanceNativeTransactionAllocationForTestV6(c.candidatePreparation.resources, promotion, c.allocatorPreparation.primary, w)
}
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeRegistryTableForTestV6(role uint8, w *iterator.OrdinalScanWork) (g rootpublication.NativeRegistryTableForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) || c.db == nil {
		return g, false
	}
	return c.db.valueLogIdentityPins.MaintenanceNativeTableForTestV6(role, w)
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeSinkGeometryForTestV6(w *iterator.OrdinalScanWork) (g freelist.NativeSinkGeometryForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) {
		return g, false
	}
	return c.allocatorPreparation.sink.MaintenanceNativeSinkGeometryForTestV6(w)
}
