package db

import (
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ErrLeafGenerationManifestStablePreparedClosureConsumed reports a second
// ownership transfer from an already-consumed or abandoned prepared closure.
var ErrLeafGenerationManifestStablePreparedClosureConsumed = errors.New("treedb: leaf generation manifest stable prepared closure consumed")

// LeafGenerationManifestStableObservations records the production durability
// work performed by the exact replacement store.
type LeafGenerationManifestStableObservations struct {
	ContentSyncs   uint64
	NamespaceSyncs uint64
}

// LeafGenerationManifestStablePreparedClosure owns the exact replacement
// manifest token returned by the production store. The replacement itself is
// already persistent; no tree root, system root, or alternate meta is
// published by this seam.
type LeafGenerationManifestStablePreparedClosure struct {
	mu           sync.Mutex
	resources    *rootpublication.StableResourceSet
	revision     uint64
	digest       [32]byte
	observations LeafGenerationManifestStableObservations
	consumed     bool
}

// Revision returns the durable replacement manifest revision.
func (closure *LeafGenerationManifestStablePreparedClosure) Revision() uint64 {
	if closure == nil {
		return 0
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	return closure.revision
}

// Digest returns the immutable replacement manifest digest.
func (closure *LeafGenerationManifestStablePreparedClosure) Digest() [32]byte {
	if closure == nil {
		return [32]byte{}
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	return closure.digest
}

// Observations returns the producer durability counters for this closure.
func (closure *LeafGenerationManifestStablePreparedClosure) Observations() LeafGenerationManifestStableObservations {
	if closure == nil {
		return LeafGenerationManifestStableObservations{}
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	return closure.observations
}

// TakeStableResources transfers exact-handle authority exactly once.
func (closure *LeafGenerationManifestStablePreparedClosure) TakeStableResources() (*rootpublication.StableResourceSet, error) {
	if closure == nil {
		return nil, ErrLeafGenerationManifestStablePreparedClosureConsumed
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	if closure.consumed || closure.resources == nil {
		return nil, ErrLeafGenerationManifestStablePreparedClosureConsumed
	}
	resources := closure.resources
	closure.resources = nil
	closure.consumed = true
	return resources, nil
}

// Release abandons untransferred authority and is idempotent.
func (closure *LeafGenerationManifestStablePreparedClosure) Release() {
	if closure == nil {
		return
	}
	closure.mu.Lock()
	resources := closure.resources
	closure.resources = nil
	closure.consumed = true
	closure.mu.Unlock()
	if resources != nil {
		resources.Release()
	}
}

// Abandon is an explicit alias for Release on pre-visibility failure.
func (closure *LeafGenerationManifestStablePreparedClosure) Abandon() { closure.Release() }

// PrepareLeafGenerationManifestStableClosure clones the DB-owned manifest and
// runs the real replacement store. It returns the replacement's exact stable
// token without publishing a B-tree/system-root/meta candidate.
func (db *DB) PrepareLeafGenerationManifestStableClosure() (*LeafGenerationManifestStablePreparedClosure, error) {
	if db == nil || db.closing.Load() {
		return nil, ErrClosed
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if !rootpublication.StableRelativeNamespaceSupported() {
		return nil, fmt.Errorf("%w: retained-parent manifest replacement unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	// Manifest maintenance and Close serialize in maintenanceMu -> writeMu
	// order. Commit post-work persists a manifest under commitMu, sometimes
	// after its optimistic publisher has released writeMu. Hold all three in
	// that established order so the clone, exact replacement, and DB-owned view
	// advance form one serialized manifest transition.
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return nil, err
	}
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	db.mu.RLock()
	if db.leafGenerationManifest == nil || db.leafGenerationManifestStore == nil {
		db.mu.RUnlock()
		return nil, fmt.Errorf("%w: DB has no outer-leaf generation manifest producer", rootpublication.ErrUnresolvedResource)
	}
	store := db.leafGenerationManifestStore
	candidate := db.leafGenerationManifest.clone()
	db.mu.RUnlock()
	if store.mode != leafGenerationManifestStable {
		return nil, fmt.Errorf("%w: stable manifest replacement mode unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	beforeContent := store.durabilityCounters.ContentSyncs.Load()
	beforeNamespace := store.durabilityCounters.NamespaceSyncs.Load()
	token, err := store.Replace(candidate)
	if err != nil {
		if token != nil {
			token.Release()
		}
		return nil, err
	}
	if token == nil {
		return nil, fmt.Errorf("%w: stable manifest replacement returned no exact token", rootpublication.ErrUnresolvedResource)
	}
	// Replace has committed the new manifest namespace and revision. Advance the
	// DB-owned view before seam-only validation so a later diagnostic failure
	// cannot leave memory stale relative to the durable manifest.
	db.mu.Lock()
	db.leafGenerationManifest = candidate
	db.mu.Unlock()
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafGeneration)
	if err := builder.Add(token); err != nil {
		token.Release()
		builder.Abandon()
		return nil, err
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Kind() != rootpublication.ResourceOuterLeafManifest || descriptors[0].Generation() != candidate.ManifestRevision || descriptors[0].Digest() == ([32]byte{}) {
		resources.Release()
		return nil, fmt.Errorf("%w: manifest replacement token does not match persisted revision %d", rootpublication.ErrResourceConflict, candidate.ManifestRevision)
	}
	contentSyncs := store.durabilityCounters.ContentSyncs.Load() - beforeContent
	namespaceSyncs := store.durabilityCounters.NamespaceSyncs.Load() - beforeNamespace
	if contentSyncs == 0 || namespaceSyncs == 0 {
		resources.Release()
		return nil, fmt.Errorf("%w: manifest replacement lacks producer durability observations", rootpublication.ErrUnresolvedResource)
	}
	return &LeafGenerationManifestStablePreparedClosure{
		resources: resources,
		revision:  candidate.ManifestRevision,
		digest:    descriptors[0].Digest(),
		observations: LeafGenerationManifestStableObservations{
			ContentSyncs: contentSyncs, NamespaceSyncs: namespaceSyncs,
		},
	}, nil
}
