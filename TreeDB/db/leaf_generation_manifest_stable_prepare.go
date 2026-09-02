package db

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// loadSelectedDurableLeafGenerationManifest reads the exact immutable
// revision retained by the selected recovery slot. manifest.json is only a
// compatibility view and may legitimately be ahead of an older fallback slot.
func (db *DB) loadSelectedDurableLeafGenerationManifest() (*leafGenerationManifest, bool, error) {
	if db == nil || db.durableRoot.slot > 1 {
		return nil, false, nil
	}
	resources := db.durableRoot.slotResources[db.durableRoot.slot]
	if resources == nil {
		return nil, false, nil
	}
	var selected *rootpublication.StableResourceToken
	for _, token := range resources.Tokens() {
		if token == nil || token.Kind() != rootpublication.ResourceOuterLeafManifest {
			continue
		}
		if selected != nil {
			return nil, false, fmt.Errorf("%w: selected durable root has multiple outer-leaf manifests", rootpublication.ErrResourceConflict)
		}
		selected = token
	}
	if selected == nil {
		return nil, false, nil
	}
	frontier := selected.Frontier()
	if frontier.Bytes == 0 || frontier.Bytes > math.MaxInt64 {
		return nil, false, fmt.Errorf("%w: selected outer-leaf manifest has invalid frontier", rootpublication.ErrFrontierBeyondResource)
	}
	var data []byte
	err := selected.WithPinnedFile(func(file *os.File) error {
		var readErr error
		data, readErr = io.ReadAll(io.NewSectionReader(file, 0, int64(frontier.Bytes)))
		return readErr
	})
	if err != nil {
		return nil, false, err
	}
	manifest, err := decodeLeafGenerationManifest(data, selected.ResourceID())
	if err != nil {
		return nil, false, err
	}
	if manifest.ManifestRevision != selected.Generation() {
		return nil, false, fmt.Errorf("%w: selected outer-leaf manifest revision=%d token=%d", rootpublication.ErrResourceConflict, manifest.ManifestRevision, selected.Generation())
	}
	return manifest, true, nil
}

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
	store        *leafGenerationManifestStore
	resourceID   string
	identity     rootpublication.StableIdentity
	previousView []byte
	revision     uint64
	digest       [32]byte
	observations LeafGenerationManifestStableObservations
	consumed     bool
	abandoned    bool
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

// abandonUnpublished removes the exact immutable revision and rolls back the
// compatibility view only when it still names that revision. It is deliberately
// separate from Release: the public producer seam may publish the operational
// manifest view without constructing a durable-root candidate.
func (closure *LeafGenerationManifestStablePreparedClosure) abandonUnpublished() error {
	if closure == nil {
		return nil
	}
	closure.mu.Lock()
	if closure.abandoned {
		closure.mu.Unlock()
		return nil
	}
	store := closure.store
	resourceID := closure.resourceID
	identity := closure.identity
	revision := closure.revision
	previousView := append([]byte(nil), closure.previousView...)
	closure.mu.Unlock()
	if store == nil || resourceID == "" || identity == (rootpublication.StableIdentity{}) || revision == 0 {
		return fmt.Errorf("%w: unpublished manifest cleanup lacks exact authority", rootpublication.ErrResourceOwnership)
	}
	if err := store.abandonPreparedStableRevision(resourceID, identity, revision, previousView); err != nil {
		return err
	}
	closure.mu.Lock()
	closure.abandoned = true
	closure.previousView = nil
	closure.mu.Unlock()
	return nil
}

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
	candidate := db.leafGenerationManifest.clone()
	db.mu.RUnlock()
	closure, candidate, err := db.prepareLeafGenerationManifestStableCandidate(candidate)
	if err != nil {
		return nil, err
	}
	// The public seam intentionally publishes only the DB-owned operational
	// manifest view. Durable-root callers use the internal candidate helper and
	// transfer its exact immutable revision into their root transaction instead.
	db.mu.Lock()
	db.leafGenerationManifest = candidate
	db.mu.Unlock()
	return closure, nil
}

// prepareLeafGenerationManifestStableCandidate persists one immutable
// candidate revision without changing the DB-visible manifest. The caller
// must run it outside DB/write/commit/root-build locks, then either transfer
// the returned closure into a durable-root candidate or abandon it.
func (db *DB) prepareLeafGenerationManifestStableCandidate(candidate *leafGenerationManifest) (*LeafGenerationManifestStablePreparedClosure, *leafGenerationManifest, error) {
	if db == nil || candidate == nil || db.leafGenerationManifestStore == nil {
		return nil, nil, fmt.Errorf("%w: DB has no outer-leaf generation manifest producer", rootpublication.ErrUnresolvedResource)
	}
	store := db.leafGenerationManifestStore
	if store.mode != leafGenerationManifestStable {
		return nil, nil, fmt.Errorf("%w: stable manifest replacement mode unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	candidate = candidate.clone()
	beforeContent := store.durabilityCounters.ContentSyncs.Load()
	beforeNamespace := store.durabilityCounters.NamespaceSyncs.Load()
	token, previousView, err := store.replaceForPreparedClosure(candidate)
	if err != nil {
		if token != nil {
			token.Release()
		}
		return nil, nil, err
	}
	if token == nil {
		return nil, nil, fmt.Errorf("%w: stable manifest replacement returned no exact token", rootpublication.ErrUnresolvedResource)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafGeneration)
	if err := builder.Add(token); err != nil {
		token.Release()
		builder.Abandon()
		return nil, nil, err
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, nil, err
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Kind() != rootpublication.ResourceOuterLeafManifest || descriptors[0].Generation() != candidate.ManifestRevision || descriptors[0].Digest() == ([32]byte{}) {
		resources.Release()
		return nil, nil, fmt.Errorf("%w: manifest replacement token does not match persisted revision %d", rootpublication.ErrResourceConflict, candidate.ManifestRevision)
	}
	contentSyncs := store.durabilityCounters.ContentSyncs.Load() - beforeContent
	namespaceSyncs := store.durabilityCounters.NamespaceSyncs.Load() - beforeNamespace
	if contentSyncs == 0 || namespaceSyncs == 0 {
		resources.Release()
		return nil, nil, fmt.Errorf("%w: manifest replacement lacks producer durability observations", rootpublication.ErrUnresolvedResource)
	}
	return &LeafGenerationManifestStablePreparedClosure{
		resources:    resources,
		store:        store,
		resourceID:   descriptors[0].ResourceID(),
		identity:     descriptors[0].Identity(),
		previousView: append([]byte(nil), previousView...),
		revision:     candidate.ManifestRevision,
		digest:       descriptors[0].Digest(),
		observations: LeafGenerationManifestStableObservations{
			ContentSyncs: contentSyncs, NamespaceSyncs: namespaceSyncs,
		},
	}, candidate, nil
}
