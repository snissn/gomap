package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"strings"
	"unsafe"
)

// An imported descriptor owns new admitted metadata, while this retained exact
// source-kind rope remains the original operation, callback and deletion owner.
// Import never reopens a handle, Observes an identity, copies an opaque callback
// or changes the foreign producer's MetadataOwner.
func newImportedResourceToken(owner *retainedalloc.Owner, source *StableResourceToken, root *stableResourceEntryNode, lane, id, path string, frontier DurableFrontier, field ReachabilityField, values []StableLogicalObligation) (*StableResourceToken, error) {
	if owner == nil || source == nil || root == nil || source.released.Load() || source.pinned == nil {
		return nil, ErrResourceOwnership
	}
	spec := StableResourceSpec{Kind: source.kind, LogicalLane: lane, ResourceID: id, DiagnosticPath: path, Frontier: frontier, Reachability: field, StableIdentityOverride: source.identity}
	charge, err := ownedTokenMetadataCharge(spec)
	if err != nil {
		return nil, err
	}
	if source.syncedFrontier.exactRIDs != nil {
		count := uint64(len(source.syncedFrontier.exactRIDs.values))
		if count > ^uint64(0)/8 {
			return nil, retainedalloc.ErrCapacity
		}
		bytes := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(exactRIDMembership{}))) + retainedalloc.AllocationCharge(count*8)
		if bytes > ^uint64(0)-charge {
			return nil, retainedalloc.ErrCapacity
		}
		charge += bytes
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	obligations, err := newResourceObligationBacking(owner, values, "")
	if err != nil {
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	physicalCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourcePinnedBacking{})))
	if err = owner.AddPending(physicalCharge); err != nil {
		obligations.release()
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	if !root.retain() {
		owner.RemovePending(physicalCharge)
		obligations.release()
		allocation.drop()
		allocation.refund()
		return nil, ErrResourceOwnership
	}
	physical := &resourcePinnedBacking{sourceRoot: root, file: source.pinned, owner: owner, charge: physicalCharge}
	physical.refs.Store(1)
	token := &StableResourceToken{metadata: allocation, obligationBacking: obligations, physicalBacking: physical, importedSource: source,
		kind: ResourceKind(strings.Clone(string(source.kind))), logicalLane: strings.Clone(lane), resourceID: strings.Clone(id), diagnosticPath: strings.Clone(path), generation: source.generation,
		identity: source.identity, frontier: cloneDurableFrontier(frontier), digest: source.digest, reachability: ReachabilityField(strings.Clone(string(field))), logicalObligations: obligations.values,
		stability: source.stability, namespace: source.namespace, pinned: source.pinned, pinnedRefs: &physical.refs, syncedFrontier: cloneDurableFrontier(source.syncedFrontier), hasSyncedFrontier: source.hasSyncedFrontier}
	token.identity.Platform = strings.Clone(source.identity.Platform)
	token.owner.Store(uint32(ResourceOwnerToken))
	token.metrics.registeredNanos = source.metrics.registeredNanos
	return token, nil
}
