package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"unsafe"
)

// This is the existing shared handle counter's constructor-owned backing.
// Immutable descriptor references are separate token allocation edges.
type resourcePinnedBacking struct {
	sourceRoot       *stableResourceEntryNode
	refs             atomic.Int64
	file             *os.File
	owner            *retainedalloc.Owner
	charge           uint64
	registry         *IdentityPinRegistry
	failure          error
	causes           ownedNamespaceCleanupFailure
	namespace        *StableNamespaceToken
	failedNext       *resourcePinnedBacking
	custody          any
	custodyCompleted atomic.Bool
}

func newResourcePinnedBacking(owner *retainedalloc.Owner, registry *IdentityPinRegistry, source *os.File, defaultSync bool) (*resourcePinnedBacking, error) {
	if owner == nil || registry == nil || source == nil {
		return nil, ErrResourceOwnership
	}
	suffix := len("#stable-pin")
	if runtime.GOOS == "windows" && defaultSync {
		suffix = len("#stable-sync-pin")
	}
	fileCharge, err := StableFileMetadataCharge(uint64(len(source.Name()) + suffix))
	if err != nil {
		return nil, err
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourcePinnedBacking{})))
	if fileCharge > ^uint64(0)-charge {
		return nil, retainedalloc.ErrCapacity
	}
	charge += fileCharge
	if err = owner.AddPending(charge); err != nil {
		return nil, err
	}
	backing := &resourcePinnedBacking{owner: owner, charge: charge, registry: registry}
	backing.refs.Store(1)
	return backing, nil
}
func (backing *resourcePinnedBacking) release() {
	if backing == nil || backing.refs.Add(-1) != 0 {
		return
	}
	if backing.sourceRoot != nil {
		root := backing.sourceRoot
		backing.sourceRoot, backing.file = nil, nil
		root.release()
	} else if backing.file != nil {
		if err := backing.file.Close(); err != nil {
			backing.failure = err
			backing.owner.CleanupFailed()
			backing.registry.retainFailedPinnedBacking(backing)
			return
		}
	}
	backing.file = nil
	owner, charge := backing.owner, backing.charge
	backing.owner, backing.charge, backing.registry = nil, 0, nil
	owner.RemovePending(charge)
}

func ownedTokenMetadataCharge(spec StableResourceSpec) (uint64, error) {
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceToken{})))
	// Every descriptor string is copied into its own actual immutable storage.
	for _, text := range [...]string{string(spec.Kind), spec.LogicalLane, spec.ResourceID, spec.DiagnosticPath, string(spec.Reachability), spec.StableIdentityOverride.Platform} {
		n := retainedalloc.AllocationCharge(uint64(len(text)))
		if n > ^uint64(0)-charge {
			return 0, retainedalloc.ErrCapacity
		}
		charge += n
	}
	if spec.PinRegistry != nil {
		n := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(IdentityPin{})))
		if n > ^uint64(0)-charge {
			return 0, retainedalloc.ErrCapacity
		}
		charge += n
	}
	if spec.Frontier.exactRIDs != nil {
		count := uint64(len(spec.Frontier.exactRIDs.values))
		if count > ^uint64(0)/8 {
			return 0, retainedalloc.ErrCapacity
		}
		n := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(exactRIDMembership{}))) + retainedalloc.AllocationCharge(count*8)
		copies := uint64(1)
		if spec.ContentSynced {
			copies++
		}
		if n > (^uint64(0)-charge)/copies {
			return 0, retainedalloc.ErrCapacity
		}
		charge += copies * n
	}
	return charge, nil
}

// The selected constructor accepts only static default operations here.
// Producers with retained callback state must supply the actual admitted
// callback owner through their typed producer route, never an opaque estimate.
func newOwnedStableResourceToken(spec StableResourceSpec, owner *retainedalloc.Owner) (*StableResourceToken, error) {
	if spec.OnRelease != nil || spec.FlushThrough != nil || spec.SyncThrough != nil || spec.MetadataOwner != owner || (spec.OwnedOperations != nil && spec.OwnedOperations.MetadataOwner() != owner) {
		return nil, ErrResourceOwnership
	}
	if spec.Namespace != nil && (!spec.Namespace.metadataScoped || spec.Namespace.metadata != owner) {
		return nil, ErrResourceOwnership
	}
	if spec.Namespace != nil {
		if err := spec.Namespace.BindCleanupRegistry(spec.PinRegistry); err != nil {
			return nil, err
		}
	}
	if err := validateDurableFrontier(spec.Frontier); err != nil {
		return nil, err
	}
	charge, err := ownedTokenMetadataCharge(spec)
	if err != nil {
		return nil, err
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	obligations, err := newResourceObligationBacking(owner, spec.LogicalObligations, spec.Reachability)
	if err != nil {
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	physical, err := newResourcePinnedBacking(owner, spec.PinRegistry, spec.File, spec.OwnedOperations == nil)
	if err != nil {
		obligations.release()
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	operationsRetained := false
	if spec.OwnedOperations != nil {
		if err = spec.OwnedOperations.Retain(); err != nil {
			physical.release()
			obligations.release()
			allocation.drop()
			allocation.refund()
			return nil, err
		}
		operationsRetained = true
	}
	transferred := false
	defer func() {
		if operationsRetained && !transferred {
			spec.OwnedOperations.Release()
		}
	}()
	spec.Kind = ResourceKind(strings.Clone(string(spec.Kind)))
	spec.LogicalLane = strings.Clone(spec.LogicalLane)
	spec.ResourceID = strings.Clone(spec.ResourceID)
	spec.DiagnosticPath = strings.Clone(spec.DiagnosticPath)
	spec.Reachability = ReachabilityField(strings.Clone(string(spec.Reachability)))
	spec.StableIdentityOverride.Platform = strings.Clone(spec.StableIdentityOverride.Platform)
	spec.MetadataOwner = nil
	spec.physicalBacking = physical
	token, err := newStableResourceToken(spec, obligations.values)
	if err != nil {
		obligations.release()
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	token.metadata, token.obligationBacking = allocation, obligations
	transferred = true
	return token, nil
}

func (token *StableResourceToken) retainMetadata() bool {
	return token != nil && token.metadata != nil && token.metadata.retain()
}
func (token *StableResourceToken) releaseMetadata() {
	if token == nil || token.metadata == nil || !token.metadata.drop() {
		return
	}
	allocation := token.metadata
	operations := token.ownedOperations
	token.importedSource = nil
	token.importedFields = nil
	token.ownedOperations = nil
	token.obligationBacking.release()
	token.obligationBacking = nil
	token.kind, token.logicalLane, token.resourceID, token.diagnosticPath, token.reachability = "", "", "", "", ""
	token.identity = StableIdentity{}
	token.frontier, token.syncedFrontier = DurableFrontier{}, DurableFrontier{}
	token.logicalObligations = nil
	token.flush, token.sync, token.onRelease, token.identityPin, token.releaseObservation = nil, nil, nil, nil, nil
	token.metadata = nil
	if operations != nil {
		operations.Release()
	}
	allocation.refund()
}

func (token *StableResourceToken) cloneOwnedPinnedDirectory(lane, id, path string, frontier DurableFrontier, field ReachabilityField, values []StableLogicalObligation, directory *DependencyDirectoryV2, onRelease func()) (*StableResourceToken, error) {
	if token != nil && token.importedSource != nil {
		if onRelease != nil || directory != nil {
			return nil, ErrResourceOwnership
		}
		return newImportedResourceToken(token.metadata.owner, token.importedSource, token.physicalBacking.sourceRoot, lane, id, path, frontier, field, values)
	}
	if onRelease != nil || token.metadata == nil || token.physicalBacking == nil || token.released.Load() {
		return nil, ErrResourceOwnership
	}
	owner := token.metadata.owner
	spec := StableResourceSpec{Kind: token.kind, LogicalLane: lane, ResourceID: id, DiagnosticPath: path, Reachability: field, Frontier: frontier, PinRegistry: token.releaseObservation, StableIdentityOverride: token.identity}
	// Pin registry is independently allocated for every cloned physical edge.
	if token.identityPin != nil {
		spec.PinRegistry = token.identityPin.registry
	}
	charge, err := ownedTokenMetadataCharge(spec)
	if err != nil {
		return nil, err
	}
	if token.syncedFrontier.exactRIDs != nil {
		n := uint64(len(token.syncedFrontier.exactRIDs.values))
		if n > ^uint64(0)/8 {
			return nil, retainedalloc.ErrCapacity
		}
		size := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(exactRIDMembership{}))) + retainedalloc.AllocationCharge(n*8)
		if size > ^uint64(0)-charge {
			return nil, retainedalloc.ErrCapacity
		}
		charge += size
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	obligations, err := newResourceObligationBacking(owner, values, field)
	if err != nil {
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	fail := func(err error) (*StableResourceToken, error) {
		obligations.release()
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	if err = token.retainPinned(); err != nil {
		return fail(err)
	}
	retained := true
	defer func() {
		if retained {
			token.releasePinnedReference()
		}
	}()
	if directory != nil {
		if err = directory.Retain(); err != nil {
			return fail(err)
		}
	}
	var identityPin *IdentityPin
	if spec.PinRegistry != nil {
		identityPin, err = spec.PinRegistry.Pin(token.identity)
		if err != nil {
			if directory != nil {
				directory.Release()
			}
			return fail(err)
		}
	}
	if token.namespace != nil {
		if err = token.namespace.retain(); err != nil {
			identityPin.Release()
			if directory != nil {
				directory.Release()
			}
			return fail(err)
		}
	}
	if token.ownedOperations != nil {
		if err = token.ownedOperations.Retain(); err != nil {
			if token.namespace != nil {
				token.namespace.release()
			}
			identityPin.Release()
			if directory != nil {
				directory.Release()
			}
			return fail(err)
		}
	}
	cloned := &StableResourceToken{metadata: allocation, obligationBacking: obligations, physicalBacking: token.physicalBacking, ownedOperations: token.ownedOperations,
		kind: ResourceKind(strings.Clone(string(token.kind))), logicalLane: strings.Clone(lane), resourceID: strings.Clone(id), diagnosticPath: strings.Clone(path),
		generation: token.generation, identity: token.identity, frontier: cloneDurableFrontier(frontier), digest: token.digest, reachability: ReachabilityField(strings.Clone(string(field))), logicalObligations: obligations.values,
		directory: directory, stability: token.stability, namespace: token.namespace, pinned: token.pinned, pinnedRefs: token.pinnedRefs, flush: token.flush, sync: token.sync,
		syncedFrontier: cloneDurableFrontier(token.syncedFrontier), hasSyncedFrontier: token.hasSyncedFrontier, identityPin: identityPin}
	cloned.identity.Platform = strings.Clone(token.identity.Platform)
	cloned.owner.Store(uint32(ResourceOwnerToken))
	cloned.metrics.registeredNanos = token.metrics.registeredNanos
	if cloned.hasSyncedFrontier {
		cloned.metrics.physicalFileSyncs.Store(1)
	}
	retained = false
	return cloned, nil
}
