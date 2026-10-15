package rootpublication

import (
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// EmptyDependencyManifestWithWorkV6 enters the original selected manifest
// constructor only after proving its actual fixed empty representation. The
// constructor's allocation remains under the SAME selected resource owner.
func EmptyDependencyManifestWithWorkV6(set *StableResourceSet, w *iterator.OrdinalScanWork) (*DependencyManifestV1, bool, error) {
	if w == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(4, 2*uint64(unsafe.Sizeof(DependencyManifestV1{}))+4*uint64(unsafe.Sizeof(resourceAllocation{}))+2*uint64(unsafe.Sizeof(dependencyManifestCursorV1{}))+128) {
		return nil, false, nil
	}
	if set == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if !set.hasEmptyAdmittedDependencyClosureLockedV6() || set.emptyDependencyDirectoryLocked() != nil || len(set.physicalDependencyDirectoriesLocked()) != 0 || set.kindViews == nil || set.kindViews.allocation == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	owner := set.metadata.owner
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DependencyManifestV1{})))
	allocation, err := newResourceAllocationWithWorkV6(owner, charge, w)
	if err != nil {
		if err == retainedalloc.ErrWorkBudget {
			return nil, false, nil
		}
		return nil, false, err
	}
	// Zero encoded entries share the canonical nonfallible empty codec. No
	// borrowed kind edge, encoded pointer backing or hidden failure refund.
	manifest, _ := newDependencyManifestV1FromEncoded(nil, true)
	manifest.allocation, manifest.selected = allocation, true
	return manifest, true, nil
}

// HasEmptyAdmittedDependencyClosureV6 checks the actual immutable representation,
// not a caller-supplied resource maximum. Nonempty closures require the original
// retained resource derivation/validation and are deliberately excluded here.
func (set *StableResourceSet) HasEmptyAdmittedDependencyClosureV6() bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	return set.hasEmptyAdmittedDependencyClosureLockedV6()
}
func (set *StableResourceSet) hasEmptyAdmittedDependencyClosureLockedV6() bool {
	owner := set.Owner()
	if !set.selected || set.metadata == nil || set.physicalOnly || owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred || len(set.entries) != 0 || len(set.pinHighWater) != 0 || set.kindViews != nil && len(set.kindViews.cells) != 0 {
		return false
	}
	if set.extras != nil {
		if len(set.extras.physical) != 0 || len(set.extras.logicalMembershipEvidence) != 0 {
			return false
		}
		if d := set.extras.empty; d != nil && (d.ref.PhysicalCount != 0 || d.ref.LogicalCount != 0) {
			return false
		}
	}
	return true
}

// A genuinely absent ordinary closure has no resource edge to import. Allocate
// its selected empty descriptor with the SAME maintained builder/import path;
// the caller owns the resulting original Set until binding or finite abort.
func PrepareEmptyStableResourceSetWithOrdinalWorkV6(owner *retainedalloc.Owner, source *StableResourceSet, w *iterator.OrdinalScanWork) (*StableResourceSet, bool, error) {
	if w == nil || owner == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceSet{}))) {
		return nil, false, nil
	}
	if source != nil {
		if !source.HasEmptyAdmittedDependencyClosureV6() {
			return nil, false, ErrNativeMVCCRootWorkRequiredV6
		}
		return CloneAdmittedStableResourceSetWithOrdinalWorkV6(source, w)
	}
	bytes := 2*uint64(unsafe.Sizeof(StableResourceSetBuilder{})) + 2*uint64(unsafe.Sizeof(StableResourceSet{})) + 2*uint64(unsafe.Sizeof(resourceKindBacking{})) + 6*uint64(unsafe.Sizeof(resourceAllocation{}))
	if !w.Reserve(6, bytes) {
		return nil, false, nil
	}
	// The original empty import's exact immutable output consists of these
	// two containing allocations and their two original reference headers.
	// Aggregate Add precedes all four allocations; no later fallible step or
	// temporary builder/abandonment callback is needed for zero input edges.
	header := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceAllocation{})))
	kindCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceKindBacking{}))) + header
	setCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceSet{}))) + header
	if ready, err := retainedalloc.AddPendingWithOrdinalWork(owner, kindCharge+setCharge, w); !ready || err != nil {
		return nil, false, err
	}
	views := &resourceKindBacking{allocation: resourceAllocationAdmittedV6(owner, kindCharge)}
	set := &StableResourceSet{metadata: resourceAllocationAdmittedV6(owner, setCharge), selected: true, kindViews: views}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	return set, true, nil
}

func (set *StableResourceSet) EmptyClosureRefusalV6() error {
	if set == nil {
		return fmt.Errorf("%w: absent original resource descriptor", ErrNativeMVCCRootWorkRequiredV6)
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	cells, physical, evidence := 0, 0, 0
	if set.kindViews != nil {
		cells = len(set.kindViews.cells)
	}
	if set.extras != nil {
		physical = len(set.extras.physical)
		evidence = len(set.extras.logicalMembershipEvidence)
	}
	return fmt.Errorf("%w: resource closure selected=%v admitted=%v entries=%d kinds=%d pins=%d physicalDirectories=%d evidenceKinds=%d", ErrNativeMVCCRootWorkRequiredV6, set.selected, set.metadata != nil, len(set.entries), cells, len(set.pinHighWater), physical, evidence)
}

// NewPreparedPrimaryEmptyCandidateWithWorkV6 enters the SAME maintained
// constructor after admitting its real fixed geometry. No opaque namespace/file
// validation is reachable for this actual empty closure. Nonempty resource and
// explicit-obligation shapes need their retained validator before this seam.
func NewPreparedPrimaryEmptyCandidateWithWorkV6(spec CandidateSpec, w *iterator.OrdinalScanWork) (*PreparedRootCandidate, bool, error) {
	if w == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(3, 2*uint64(unsafe.Sizeof(spec))+2*uint64(unsafe.Sizeof(PreparedRootCandidate{}))+2*uint64(unsafe.Sizeof(DurableRootTransaction{}))) {
		return nil, false, nil
	}
	if spec.DurableRoot == nil || !spec.DurableRoot.RequiresNativeCallbackWorkV6() || len(spec.Obligations) != 0 || !spec.ResourceSet.HasEmptyAdmittedDependencyClosureV6() {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	candidate, err := NewPreparedRootCandidate(spec)
	return candidate, err == nil, err
}

type NativeCoordinatorGeometryForTestV6 struct {
	Pending, PendingCapacity, Builders, Dispatch, NativeDispatch, Receipts, Waiters, PinKinds, HighWaterKinds uint64
	Publishing, Preparing, Timer, RealClock                                                                   bool
	BuilderBytes, ReceiptBytes, PendingEntryBytes                                                             uint64
	ChannelBackingOpaque, TimerBackingOpaque                                                                  bool
}

func (c *Coordinator) MaintenanceNativeGeometryForTestV6(w *iterator.OrdinalScanWork) (g NativeCoordinatorGeometryForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g.Pending, g.PendingCapacity, g.Builders = uint64(len(c.pending)), uint64(cap(c.pending)), c.activeBuilders
	g.Dispatch, g.NativeDispatch, g.Waiters = c.activeDispatch, c.nativeDispatchV6, uint64(len(c.waiters))
	g.PinKinds, g.HighWaterKinds = uint64(len(c.resourceActivePins)), uint64(len(c.resourcePinHighWater))
	g.Publishing, g.Preparing, g.Timer = c.publishing, c.preparing, c.timer != nil
	_, g.RealClock = c.clock.(realClock)
	for receipt := c.nativePublicationsV6; receipt != nil; {
		if !w.Reserve(1, uint64(unsafe.Sizeof(*receipt))) {
			return g, false
		}
		g.Receipts++
		receipt = receipt.next
	}
	g.BuilderBytes = uint64(unsafe.Sizeof(builderLease{})) + uint64(unsafe.Sizeof(builderHandle{})) + uint64(unsafe.Sizeof(BuilderToken{}))
	g.ReceiptBytes, g.PendingEntryBytes = uint64(unsafe.Sizeof(BuilderHandoffReceipt{})), uint64(unsafe.Sizeof(pendingEntry{}))
	// No risky mirrors of unexported runtime layouts. The final report must
	// price each actual notify/timer call and leave these retained sizes opaque.
	g.ChannelBackingOpaque, g.TimerBackingOpaque = true, true
	return g, true
}

type NativeEmptySetGeometryForTestV6 struct {
	Entries, Kinds, Directories, MetadataRefs, BackingRefs, EmptyRefs uint64
	VisibleEntries, VisibleKinds, VisibleDirectories                  uint64
	Selected, SameBacking, SameEmpty, Empty, MetadataOwnerMatches     bool
	ConsumeRecords, ConsumeBytes                                      uint64
	ConsumeBoundComplete                                              bool
}

func (set *StableResourceSet) MaintenanceNativeEmptyGeometryForTestV6(visible *StableResourceSet, w *iterator.OrdinalScanWork) (g NativeEmptySetGeometryForTestV6, ready bool) {
	if set == nil || visible == nil || set == visible || w == nil || !w.Reserve(3, 2*uint64(unsafe.Sizeof(*set))+2*uint64(unsafe.Sizeof(*visible))+2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(resourceKindBacking{}))+2*uint64(unsafe.Sizeof(resourceAllocation{}))+2*uint64(unsafe.Sizeof(DependencyDirectoryV2{}))) {
		return g, false
	}
	first, second := set, visible
	if uintptr(unsafe.Pointer(first)) > uintptr(unsafe.Pointer(second)) {
		first, second = second, first
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()
	g.Entries, g.VisibleEntries = uint64(len(set.entries)), uint64(len(visible.entries))
	g.Selected, g.SameBacking = set.selected, set.kindViews == visible.kindViews
	if set.metadata != nil {
		g.MetadataRefs = uint64(set.metadata.refs.Load())
		g.MetadataOwnerMatches = visible.metadata != nil && set.metadata.owner == visible.metadata.owner
	}
	if set.kindViews != nil {
		g.Kinds = uint64(len(set.kindViews.cells))
		if set.kindViews.allocation != nil {
			g.BackingRefs = uint64(set.kindViews.allocation.refs.Load())
		}
	}
	if visible.kindViews != nil {
		g.VisibleKinds = uint64(len(visible.kindViews.cells))
	}
	g.Directories, g.VisibleDirectories = uint64(len(set.physicalDependencyDirectoriesLocked())), uint64(len(visible.physicalDependencyDirectoriesLocked()))
	empty := set.emptyDependencyDirectoryLocked()
	g.Empty, g.SameEmpty = empty != nil, empty == visible.emptyDependencyDirectoryLocked()
	if empty != nil {
		g.EmptyRefs = uint64(empty.refs.Load())
	}
	// Exact original narrow empty branch in consumeSharedWithVisibleV6.
	if g.Entries == 0 && g.Kinds == 0 && g.Directories == 0 && g.VisibleEntries == 0 && g.VisibleKinds == 0 && g.VisibleDirectories == 0 && g.SameBacking && g.SameEmpty {
		g.ConsumeRecords = 1 + 1 + 1 + 2 // outer, header, nil-or-empty slot, fixed effects
		g.ConsumeBytes = 2*uint64(unsafe.Sizeof(NativePublicationV6{})) + 4*uint64(unsafe.Sizeof(*set)) + 4*uint64(unsafe.Sizeof(StableResourceSetReleaseWorkV6{})) + 2*uint64(unsafe.Sizeof(*visible)) + uint64(unsafe.Sizeof((*DependencyDirectoryV2)(nil)))
		if empty != nil {
			g.ConsumeRecords++
		}
		if set.kindViews != nil {
			g.ConsumeRecords++
			g.ConsumeBytes += 2*uint64(unsafe.Sizeof(*set.kindViews)) + 2*uint64(unsafe.Sizeof(resourceAllocation{}))
		}
		g.ConsumeBoundComplete = true
	}
	return g, true
}

type NativeIndexTokenGeometryForTestV6 struct {
	Released, Owned, Imported, NativeNamespace, StableNamespace, Linked bool
	PathBytes, RequiredRIDs, StableRIDs                                 uint64
}

func (token *StableResourceToken) MaintenanceNativeIndexGeometryForTestV6(w *iterator.OrdinalScanWork) (g NativeIndexTokenGeometryForTestV6, ready bool) {
	if token == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*token))+2*uint64(unsafe.Sizeof(StableNamespaceToken{}))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	g.Released, g.Owned, g.Imported = token.released.Load(), token.ownedOperations != nil, token.importedSource != nil
	if token.pinned != nil {
		g.PathBytes = uint64(len(token.pinned.Name()))
	}
	if token.frontier.exactRIDs != nil {
		g.RequiredRIDs = uint64(len(token.frontier.exactRIDs.values))
	}
	if token.syncedFrontier.exactRIDs != nil {
		g.StableRIDs = uint64(len(token.syncedFrontier.exactRIDs.values))
	}
	if ns := token.namespace; ns != nil {
		ns.mu.Lock()
		g.StableNamespace, g.Linked = ns.state.Load() == namespaceStable, ns.hasLinkedResource
		_, g.NativeNamespace = ns.adapter.(nativeNamespaceAdapter)
		ns.mu.Unlock()
	}
	return g, true
}
