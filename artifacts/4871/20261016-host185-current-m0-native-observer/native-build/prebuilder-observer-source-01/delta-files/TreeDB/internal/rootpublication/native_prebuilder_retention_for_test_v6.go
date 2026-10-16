package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// Original constructor envelopes, not a recursively summed physical union.
// Comparing allocations under the original pair of set locks permits exact
// deduplication against a named original role without exporting an identity.
type NativeSetAllocationForTestV6 struct {
	Present, SameSet, SameMetadata, SameKinds, SameEmpty, Selected, OpaqueLogicalGraph             bool
	SetCharge, KindCharge, EmptyCharge, EntriesCapacity, KindCapacity, ExtrasRaw, PhysicalCapacity uint64
}

func (s *StableResourceSet) MaintenanceNativeSetAllocationForTestV6(other *StableResourceSet, w *iterator.OrdinalScanWork) (g NativeSetAllocationForTestV6, ready bool) {
	if w == nil || !w.Reserve(3, 4*uint64(unsafe.Sizeof(StableResourceSet{}))+2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(stableResourceSetExtras{}))+2*uint64(unsafe.Sizeof(resourceKindBacking{}))+2*uint64(unsafe.Sizeof(DependencyDirectoryV2{}))) {
		return g, false
	}
	if s == nil {
		return g, true
	}
	first, second := s, other
	if second != nil && uintptr(unsafe.Pointer(first)) > uintptr(unsafe.Pointer(second)) {
		first, second = second, first
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	if second != nil && second != first {
		second.mu.Lock()
		defer second.mu.Unlock()
	}
	g.Present, g.Selected, g.SameSet = true, s.selected, s == other
	g.EntriesCapacity = uint64(cap(s.entries))
	g.OpaqueLogicalGraph = len(s.entries) != 0 || s.pinHighWater != nil
	if s.metadata != nil {
		g.SetCharge = s.metadata.charge
	}
	if s.kindViews != nil {
		g.KindCapacity = uint64(cap(s.kindViews.cells))
		if s.kindViews.allocation != nil {
			g.KindCharge = s.kindViews.allocation.charge
		}
		g.OpaqueLogicalGraph = g.OpaqueLogicalGraph || len(s.kindViews.cells) != 0
	}
	if s.extras != nil {
		g.ExtrasRaw = uint64(unsafe.Sizeof(*s.extras))
		g.PhysicalCapacity = uint64(cap(s.extras.physical))
		g.OpaqueLogicalGraph = g.OpaqueLogicalGraph || s.extras.logicalMembershipEvidence != nil || len(s.extras.physical) != 0
		if s.extras.empty != nil {
			g.EmptyCharge = s.extras.empty.metadataCharge
		}
	}
	if other != nil {
		g.SameMetadata = s.metadata != nil && s.metadata == other.metadata
		g.SameKinds = s.kindViews != nil && s.kindViews == other.kindViews
		g.SameEmpty = s.emptyDependencyDirectoryLocked() != nil && s.emptyDependencyDirectoryLocked() == other.emptyDependencyDirectoryLocked()
	}
	return g, true
}

type NativeManifestAllocationForTestV6 struct {
	Present, Selected, Released, OpaqueEntryGraph                   bool
	Charge, PayloadCapacity, EntryCapacity, LoadedCapacity, Entries uint64
}

func (m *DependencyManifestV1) MaintenanceNativeManifestAllocationForTestV6(w *iterator.OrdinalScanWork) (g NativeManifestAllocationForTestV6, ready bool) {
	if w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(DependencyManifestV1{}))+2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(resourceAllocation{}))) {
		return g, false
	}
	if m == nil {
		return g, true
	}
	m.metadataMu.Lock()
	defer m.metadataMu.Unlock()
	g.Present, g.Selected, g.Released = true, m.selected, m.released
	if m.allocation != nil {
		g.Charge = m.allocation.charge
	}
	g.PayloadCapacity, g.EntryCapacity, g.LoadedCapacity, g.Entries = uint64(cap(m.payload)), uint64(cap(m.entries)), uint64(cap(m.loadedEntries)), uint64(len(m.entries))
	g.OpaqueEntryGraph = len(m.entries) != 0 || len(m.loadedEntries) != 0
	return g, true
}

type NativeTransactionAllocationForTestV6 struct {
	Present, Native, CandidateEmbedded, SameResources, SamePromotion, SamePrimaryCertificate                                   bool
	Charge, TransactionRaw, ConstructionRaw, TerminalRaw, PrimaryRaw, DataCertificateRaw, PromotionRaw, PromotionImageCapacity uint64
}

func (t *DurableRootTransaction) MaintenanceNativeTransactionAllocationForTestV6(resources *StableResourceSet, promotion *PrimaryPromotionV6, primary *PreparedPrimaryRootV5, w *iterator.OrdinalScanWork) (g NativeTransactionAllocationForTestV6, ready bool) {
	if w == nil || !w.Reserve(3, 2*uint64(unsafe.Sizeof(DurableRootTransaction{}))+2*uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))+2*uint64(unsafe.Sizeof(PreparedPrimaryRootV5{}))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	if t == nil {
		return g, true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil {
		return g, true
	}
	g.Present, g.Native, g.CandidateEmbedded = true, c.nativeWorkOwned, true
	g.Charge, g.TransactionRaw, g.ConstructionRaw = c.metadataCharge, uint64(unsafe.Sizeof(*t)), uint64(unsafe.Sizeof(*c))
	if c.terminalCompletion != nil {
		g.TerminalRaw = uint64(unsafe.Sizeof(*c.terminalCompletion))
	}
	g.SameResources, g.SamePromotion = c.resources == resources, c.promotion == promotion
	if c.promotion != nil {
		g.PromotionRaw = uint64(unsafe.Sizeof(*c.promotion))
		g.PromotionImageCapacity = uint64(cap(c.promotion.Image))
	}
	if primary != nil {
		g.PrimaryRaw = uint64(unsafe.Sizeof(*primary))
		if primary.dataCertificate != nil {
			g.DataCertificateRaw = uint64(unsafe.Sizeof(*primary.dataCertificate))
		}
		g.SamePrimaryCertificate = primary.readCertificate == c.certificate
	}
	return g, true
}

type NativeRegistryTableForTestV6 struct {
	Present, SameOwner                     bool
	Charge, Capacity, CellRaw, Count, Used uint64
}

// Each role is one existing table, not a walk through registry payload owners.
// Charge describes the actual current backing; payload envelopes stay separate.
func (r *IdentityPinRegistry) MaintenanceNativeTableForTestV6(role uint8, w *iterator.OrdinalScanWork) (g NativeRegistryTableForTestV6, ready bool) {
	if r == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*r))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	g.Present = true
	switch role {
	case 0:
		t := &r.states
		g.Charge, g.Capacity, g.CellRaw, g.Count, g.Used, g.SameOwner = t.charge, uint64(cap(t.cells)), uint64(unsafe.Sizeof(registryCell[StableIdentity, *identityPinState]{})), uint64(t.count), uint64(t.used), t.owner == &r.metadata
	case 1:
		t := &r.namespaces
		g.Charge, g.Capacity, g.CellRaw, g.Count, g.Used, g.SameOwner = t.charge, uint64(cap(t.cells)), uint64(unsafe.Sizeof(registryCell[string, bool]{})), uint64(t.count), uint64(t.used), t.owner == &r.metadata
	case 2:
		t := &r.stableLinks
		g.Charge, g.Capacity, g.CellRaw, g.Count, g.Used, g.SameOwner = t.charge, uint64(cap(t.cells)), uint64(unsafe.Sizeof(registryCell[stableNamespaceLink, struct{}]{})), uint64(t.count), uint64(t.used), t.owner == &r.metadata
	case 3:
		t := &r.stableDirectoryLinks
		g.Charge, g.Capacity, g.CellRaw, g.Count, g.Used, g.SameOwner = t.charge, uint64(cap(t.cells)), uint64(unsafe.Sizeof(registryCell[stableNamespaceLink, stableDirectoryLinkAuthority]{})), uint64(t.count), uint64(t.used), t.owner == &r.metadata
	default:
		return g, false
	}
	return g, true
}
