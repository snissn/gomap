package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"strings"
	"unsafe"
)

// This preborn record owns one exact original observation action. It is also
// its intrusive failed-custody node at the original identity registry; the
// imported metadata adapter owns neither this action nor its cleanup outcome.
func newOriginalObservationCleanup(registry *IdentityPinRegistry) (*resourcePinnedBacking, error) {
	if registry == nil {
		return nil, ErrResourceOwnership
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closing {
		return nil, retainedalloc.ErrClosed
	}
	owner := &registry.metadata
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourcePinnedBacking{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	cleanup := &resourcePinnedBacking{owner: owner, charge: charge, registry: registry}
	cleanup.refs.Store(1)
	return cleanup, nil
}

func newOriginalObservedStableResourceToken(spec StableResourceSpec) (*StableResourceToken, error) {
	return newOriginalObservedStableResourceTokenNormalized(spec, nil)
}
func newOriginalObservedStableResourceTokenNormalized(spec StableResourceSpec, normalized []StableLogicalObligation) (*StableResourceToken, error) {
	registry := spec.OriginalObservation
	if registry == nil || registry != spec.PinRegistry || spec.OnRelease != nil || spec.releaseObservation != nil || spec.physicalBacking != nil || spec.File == nil {
		return nil, ErrResourceOwnership
	}
	observation, err := newOriginalObservationCleanup(registry)
	if err != nil {
		return nil, err
	}
	physical, err := newResourcePinnedBacking(registry.MetadataOwner(), registry, spec.File, spec.SyncThrough == nil)
	if err != nil {
		observation.release()
		return nil, err
	}
	if spec.Namespace != nil {
		if err = spec.Namespace.BindCleanupRegistry(registry); err != nil {
			physical.release()
			observation.release()
			return nil, err
		}
	}
	spec.physicalBacking = physical
	spec.releaseObservation = registry
	spec.OriginalObservation = nil
	token, err := newStableResourceToken(spec, normalized)
	if err != nil {
		observation.release()
		return nil, err
	}
	token.observationCleanup = observation
	return token, nil
}

func (cleanup *resourcePinnedBacking) finishOriginalObservation(registry *IdentityPinRegistry, identity StableIdentity, namespace *StableNamespaceToken) error {
	// The token's once-only release consumed its physical and namespace edges
	// before arriving here. Observe/Unobserve is independent of handle sharing.
	if registry != nil {
		cleanup.causes.add(registry.Unobserve(identity))
	}
	if cleanup.causes.count != 0 {
		cleanup.failure = &cleanup.causes
		cleanup.refs.Store(0)
		cleanup.owner.CleanupFailed()
		cleanup.registry.retainFailedPinnedBacking(cleanup)
		return cleanup.failure
	}
	return cleanup.release()
}

// OriginalCleanupCustody is a preborn failure edge in the existing physical
// identity registry. Its independent metadata owner is supplied before effects;
// it never changes the original action or physical deletion authority.
type OriginalCleanupCustody resourcePinnedBacking

func NewOriginalCleanupCustody(registry *IdentityPinRegistry, owner *retainedalloc.Owner) (*OriginalCleanupCustody, error) {
	if registry == nil || owner == nil {
		return nil, ErrResourceOwnership
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closing || registry.originalCleanupEdges == ^uint64(0) {
		return nil, retainedalloc.ErrClosed
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourcePinnedBacking{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	registry.originalCleanupEdges++
	backing := &resourcePinnedBacking{owner: owner, charge: charge, registry: registry}
	backing.refs.Store(1)
	return (*OriginalCleanupCustody)(backing), nil
}

// Complete consumes the original action once. A real failure retains the
// exact payload and admitted backing at its original registry, without retry.
func (custody *OriginalCleanupCustody) Complete(payload any, err error) {
	if custody == nil {
		return
	}
	backing := (*resourcePinnedBacking)(custody)
	if !backing.custodyCompleted.CompareAndSwap(false, true) {
		return
	}
	registry := backing.registry
	if err != nil {
		backing.custody, backing.failure = payload, err
		backing.refs.Store(0)
		backing.owner.CleanupFailed()
	} else {
		backing.release()
	}
	// Failure was linked before consuming the original edge; success consumed
	// the original payload before permitting physical registry disposal.
	registry.mu.Lock()
	if registry.originalCleanupEdges == 0 {
		registry.mu.Unlock()
		panic("unbalanced original cleanup custody")
	}
	if err != nil {
		backing.failedNext = registry.failedPinned
		registry.failedPinned = backing
	}
	registry.originalCleanupEdges--
	registry.closeIfIdleLocked()
	registry.mu.Unlock()
}

// CaptureStableNamespaceWithMetadata creates an independent, admitted namespace
// descriptor for an already-held child. It never changes a generation's cached
// proof owner. Every cleanup operand is preborn at the original registry.
func CaptureStableNamespaceWithMetadata(owner *retainedalloc.Owner, registry *IdentityPinRegistry, directory string, child *os.File, name, diagnostic string) (*StableNamespaceToken, error) {
	return captureStableNamespaceWithMetadata(owner, registry, directory, child, name, diagnostic, false)
}

// CaptureKnownStableNamespaceWithMetadata preserves the original manager's
// exact identity-keyed stable-link reuse through the same checked producer.
func CaptureKnownStableNamespaceWithMetadata(owner *retainedalloc.Owner, registry *IdentityPinRegistry, directory string, child *os.File, name, diagnostic string) (*StableNamespaceToken, error) {
	return captureStableNamespaceWithMetadata(owner, registry, directory, child, name, diagnostic, true)
}
func captureStableNamespaceWithMetadata(owner *retainedalloc.Owner, registry *IdentityPinRegistry, directory string, child *os.File, name, diagnostic string, reuseKnown bool) (token *StableNamespaceToken, err error) {
	if owner == nil || registry == nil || child == nil {
		return nil, ErrResourceOwnership
	}
	custody, err := NewOriginalCleanupCustody(registry, owner)
	if err != nil {
		return nil, err
	}
	backing := (*resourcePinnedBacking)(custody)
	charge, err := StableFileMetadataCharge(uint64(len(directory)))
	if err != nil {
		custody.Complete(nil, nil)
		return nil, err
	}
	if err = owner.AddPending(charge); err != nil {
		custody.Complete(nil, nil)
		return nil, err
	}
	backing.charge += charge
	parent, err := os.Open(strings.Clone(directory))
	if err != nil {
		custody.Complete(nil, nil)
		return nil, err
	}
	var proof *StableNamespaceCreationProof
	defer func() {
		if proof != nil {
			backing.causes.add(proof.ReleaseWithError())
		}
		backing.causes.add(parent.Close())
		if backing.causes.count != 0 {
			if token != nil {
				backing.causes.add(token.ReleaseWithError())
				token = nil
			}
			backing.file = parent
			backing.causes.add(err)
			err = &backing.causes
			custody.Complete(proof, err)
		} else {
			custody.Complete(nil, nil)
		}
	}()
	generation, err := StableNamespaceParentGeneration(parent)
	if err != nil {
		return nil, err
	}
	if reuseKnown {
		token, err = registry.newStableNamespaceTokenForKnownLinkWithMetadata(StableNamespaceSpec{
			Parent: parent, LinkedResource: child, ParentGeneration: generation,
			Operation: NamespaceCreate, NewName: name, DiagnosticPath: diagnostic}, owner)
		if err == nil {
			err = token.BindCleanupRegistry(registry)
			if err != nil {
				backing.causes.add(token.ReleaseWithError())
				token = nil
			}
			return token, err
		}
		if !errors.Is(err, ErrNamespaceUnstable) {
			return nil, err
		}
	}
	proof, err = NewOwnedStableNamespaceCreationProof(parent, child, name, owner)
	if err != nil {
		return nil, err
	}
	if reuseKnown {
		if err = registry.RememberStableNamespaceLink(parent, child, name); err != nil {
			return nil, err
		}
	}
	token, err = proof.Bind(parent, generation, name, diagnostic)
	if err == nil {
		err = token.BindCleanupRegistry(registry)
		if err != nil {
			backing.causes.add(token.ReleaseWithError())
			token = nil
		}
	}
	return token, err
}

// The selected descriptor and its original observed edge have independent
// admitted owners. Only this exact producer-origin action is transferred.
func newOwnedOriginalObservedStableResourceToken(spec StableResourceSpec) (*StableResourceToken, error) {
	registry := spec.OriginalObservation
	if registry == nil || registry != spec.PinRegistry || spec.OnRelease != nil || spec.releaseObservation != nil || spec.physicalBacking != nil || spec.File == nil {
		return nil, ErrResourceOwnership
	}
	observation, err := newOriginalObservationCleanup(registry)
	if err != nil {
		return nil, err
	}
	spec.OriginalObservation = nil
	spec.releaseObservation = registry
	token, err := newOwnedStableResourceToken(spec, spec.MetadataOwner)
	if err != nil {
		observation.release()
		return nil, err
	}
	token.observationCleanup = observation
	return token, nil
}
