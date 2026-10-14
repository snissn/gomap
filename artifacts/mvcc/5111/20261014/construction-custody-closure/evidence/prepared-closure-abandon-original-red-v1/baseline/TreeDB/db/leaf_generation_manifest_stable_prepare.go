package db

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"

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
	var manifest *leafGenerationManifest
	var found bool
	err := resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
		var err error
		manifest, found, err = loadSelectedDurableLeafGenerationManifestTokens(tokens)
		return err
	})
	return manifest, found, err
}
func loadSelectedDurableLeafGenerationManifestTokens(tokens []*rootpublication.StableResourceToken) (*leafGenerationManifest, bool, error) {
	var selected *rootpublication.StableResourceToken
	for _, token := range tokens {
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
	mu                     sync.Mutex
	resources              *rootpublication.StableResourceSet
	store                  *leafGenerationManifestStore
	resourceID             string
	identity               rootpublication.StableIdentity
	previousView           []byte
	revision               uint64
	digest                 [32]byte
	observations           LeafGenerationManifestStableObservations
	consumed               bool
	abandoned              bool
	db                     *DB
	metadata               *retainedalloc.Owner
	metadataCharge         uint64
	previousFile           *os.File
	previousFileCharge     uint64
	previousCloseAttempted bool
	previousCloseErr       error
	cleanupAttempted       bool
	cleanupDone            bool
	cleanupErr             error
	abandonAttempted       bool
	abandonErr             error
	replacementFiles       [2]*os.File
	replacementCharges     [2]uint64
	replacementAttempted   [2]bool
	replacementErrors      [2]error
	failure                originalDBCleanupFailure
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
	if closure.consumed || closure.cleanupAttempted || closure.abandonAttempted || closure.resources == nil {
		return nil, ErrLeafGenerationManifestStablePreparedClosureConsumed
	}
	resources := closure.resources
	closure.resources = nil
	closure.consumed = true
	return resources, nil
}

// ReleaseWithError ends the original closure lifetime. Take transfers only the
// Set: rollback bytes remain owned here until abandonment or this final release.
func (closure *LeafGenerationManifestStablePreparedClosure) ReleaseWithError() error {
	if closure == nil {
		return nil
	}
	closure.mu.Lock()
	if closure.cleanupAttempted {
		err, done := closure.cleanupErr, closure.cleanupDone
		closure.mu.Unlock()
		if !done {
			return rootpublication.ErrResourceOwnership
		}
		return err
	}
	if closure.abandonAttempted && !closure.abandoned && closure.abandonErr == nil {
		closure.mu.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	closure.cleanupAttempted, closure.consumed = true, true
	resources := closure.resources
	closure.mu.Unlock()
	var err error
	if resources != nil {
		err = resources.ReleaseWithError()
	}
	if err == nil {
		err = closure.closePreviousFile()
	}
	if err == nil {
		err = closure.closeReplacementFiles()
	}
	if err != nil {
		return closure.retainCleanupFailure(err)
	}
	closure.mu.Lock()
	clear(closure.previousView)
	closure.previousView, closure.previousFile, closure.resources, closure.store = nil, nil, nil, nil
	owner, charge := closure.metadata, closure.metadataCharge
	closure.metadata, closure.metadataCharge, closure.db = nil, 0, nil
	closure.resourceID, closure.identity = "", rootpublication.StableIdentity{}
	closure.cleanupDone = true
	closure.mu.Unlock()
	if owner != nil {
		owner.RemovePending(charge)
	}
	return nil
}

// Release preserves the original compatibility surface; selected callers check
// ReleaseWithError, and a refusal is rooted in the existing DB error graph.
func (closure *LeafGenerationManifestStablePreparedClosure) Release() { _ = closure.ReleaseWithError() }
func (closure *LeafGenerationManifestStablePreparedClosure) Abandon() { closure.Release() }

func (closure *LeafGenerationManifestStablePreparedClosure) retainCleanupFailure(cause error) error {
	closure.failure.retainCause(0, cause)
	closure.mu.Lock()
	closure.cleanupAttempted, closure.consumed = true, true
	closure.cleanupErr, closure.cleanupDone = &closure.failure, true
	owner, db := closure.metadata, closure.db
	closure.mu.Unlock()
	if owner != nil {
		owner.CleanupFailed()
	}
	if db != nil {
		db.retainOriginalCleanupError(&closure.failure)
	}
	return &closure.failure
}

func (closure *LeafGenerationManifestStablePreparedClosure) openReplacementFile(index int, store *leafGenerationManifestStore, name string, flags int, mode os.FileMode) (*os.File, error) {
	charge, err := rootpublication.StableFileMetadataCharge(uint64(len(store.parent.Name()) + 1 + len(name)))
	if err != nil {
		return nil, err
	}
	if err = closure.metadata.AddPending(charge); err != nil {
		return nil, err
	}
	file, err := rootpublication.OpenStableChildFile(store.parent, name, flags, mode)
	if err != nil {
		closure.metadata.RemovePending(charge)
		return nil, err
	}
	closure.replacementFiles[index], closure.replacementCharges[index] = file, charge
	closure.metadataCharge += charge
	return file, nil
}
func (closure *LeafGenerationManifestStablePreparedClosure) closeReplacementFiles() error {
	var first error
	for i, file := range closure.replacementFiles {
		if file == nil {
			continue
		}
		if !closure.replacementAttempted[i] {
			closure.replacementAttempted[i] = true
			closure.replacementErrors[i] = file.Close()
			if closure.replacementErrors[i] == nil {
				closure.replacementFiles[i] = nil
				closure.metadata.RemovePending(closure.replacementCharges[i])
				closure.metadataCharge -= closure.replacementCharges[i]
				closure.replacementCharges[i] = 0
			}
		}
		if first == nil {
			first = closure.replacementErrors[i]
		}
	}
	return first
}
func (closure *LeafGenerationManifestStablePreparedClosure) closePreviousFile() error {
	// Producer construction is unpublished; final Release owns the claimed
	// completion. Actual file Close runs outside the closure and store locks.
	if closure.previousFile == nil {
		return nil
	}
	if closure.previousCloseAttempted {
		return closure.previousCloseErr
	}
	closure.previousCloseAttempted = true
	closure.previousCloseErr = closure.previousFile.Close()
	if closure.previousCloseErr == nil {
		closure.previousFile = nil
		closure.metadata.RemovePending(closure.previousFileCharge)
		closure.metadataCharge -= closure.previousFileCharge
		closure.previousFileCharge = 0
	}
	return closure.previousCloseErr
}

// The exact previous-view handle and full buffer are admitted before opening
// and allocating. The returned bytes are transferred once, without a clone.
func (closure *LeafGenerationManifestStablePreparedClosure) readPreviousViewLocked(store *leafGenerationManifestStore) ([]byte, error) {
	if store.parent == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	charge, err := rootpublication.StableFileMetadataCharge(uint64(len(store.parent.Name()) + 1 + len(leafGenerationManifestFileName)))
	if err != nil {
		return nil, err
	}
	if err := closure.metadata.AddPending(charge); err != nil {
		return nil, err
	}
	file, err := rootpublication.OpenStableChildFile(store.parent, leafGenerationManifestFileName, os.O_RDONLY, 0)
	if err != nil {
		closure.metadata.RemovePending(charge)
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	closure.previousFile, closure.previousFileCharge = file, charge
	closure.metadataCharge += charge
	before, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 0 || uint64(before.Size()) > uint64(math.MaxInt) {
		return nil, retainedalloc.ErrCapacity
	}
	bytesCharge := retainedalloc.AllocationCharge(uint64(before.Size()))
	if err := closure.metadata.AddPending(bytesCharge); err != nil {
		return nil, err
	}
	closure.metadataCharge += bytesCharge
	closure.previousView = make([]byte, int(before.Size()))
	if _, err := io.ReadFull(file, closure.previousView); err != nil {
		return nil, err
	}
	var tail [1]byte
	if n, err := file.Read(tail[:]); n != 0 || err != io.EOF {
		return nil, errors.Join(err, rootpublication.ErrResourceConflict)
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || after.Size() != before.Size() {
		return nil, errors.Join(err, rootpublication.ErrResourceConflict)
	}
	if _, err := decodeLeafGenerationManifest(closure.previousView, leafGenerationManifestFileName); err != nil {
		return nil, err
	}
	return closure.previousView, nil
}

// Abandon preserves the separate unpublished namespace action after Set
// transfer. It claims that action once; refused cleanup is never replayed.
func (closure *LeafGenerationManifestStablePreparedClosure) abandonUnpublished() error {
	if closure == nil {
		return nil
	}
	closure.mu.Lock()
	if closure.abandonAttempted {
		err, done := closure.abandonErr, closure.abandoned
		closure.mu.Unlock()
		if err == nil && !done {
			return rootpublication.ErrResourceOwnership
		}
		return err
	}
	if closure.cleanupAttempted {
		err := closure.cleanupErr
		closure.mu.Unlock()
		return errors.Join(err, rootpublication.ErrResourceOwnership)
	}
	closure.abandonAttempted = true
	store, resourceID, identity, revision, previousView := closure.store, closure.resourceID, closure.identity, closure.revision, closure.previousView
	closure.mu.Unlock()
	var err error
	if store == nil || resourceID == "" || identity == (rootpublication.StableIdentity{}) || revision == 0 {
		err = fmt.Errorf("%w: unpublished manifest cleanup lacks exact authority", rootpublication.ErrResourceOwnership)
	} else {
		err = store.abandonPreparedStableRevision(resourceID, identity, revision, previousView)
	}
	if err != nil {
		failure := closure.retainCleanupFailure(err)
		closure.mu.Lock()
		closure.abandonErr = failure
		closure.mu.Unlock()
		return failure
	}
	closure.mu.Lock()
	closure.abandoned = true
	closure.mu.Unlock()
	return closure.ReleaseWithError()
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
func (db *DB) prepareLeafGenerationManifestStableCandidate(candidate *leafGenerationManifest) (closure *LeafGenerationManifestStablePreparedClosure, persisted *leafGenerationManifest, retErr error) {
	if db == nil || candidate == nil || db.leafGenerationManifestStore == nil {
		return nil, nil, fmt.Errorf("%w: DB has no outer-leaf generation manifest producer", rootpublication.ErrUnresolvedResource)
	}
	store := db.leafGenerationManifestStore
	if store.mode != leafGenerationManifestStable {
		return nil, nil, fmt.Errorf("%w: stable manifest replacement mode unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	owner := db.valueLogIdentityPins.MetadataOwner()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(LeafGenerationManifestStablePreparedClosure{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, nil, err
	}
	original := &LeafGenerationManifestStablePreparedClosure{db: db, store: store, metadata: owner, metadataCharge: charge}
	original.failure.payload = original
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, original.ReleaseWithError())
		}
	}()
	candidate = candidate.clone()
	beforeContent := store.durabilityCounters.ContentSyncs.Load()
	beforeNamespace := store.durabilityCounters.NamespaceSyncs.Load()
	token, _, err := store.replaceForPreparedClosure(candidate, original)
	err = errors.Join(err, original.closePreviousFile(), original.closeReplacementFiles())
	if err != nil {
		if token != nil {
			err = errors.Join(err, token.ReleaseWithError())
		}
		return nil, nil, err
	}
	if token == nil {
		return nil, nil, fmt.Errorf("%w: stable manifest replacement returned no exact token", rootpublication.ErrUnresolvedResource)
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(owner, rootpublication.ReachabilityOuterLeafGeneration)
	if err != nil {
		return nil, nil, errors.Join(err, token.ReleaseWithError())
	}
	if err := builder.Add(token); err != nil {
		return nil, nil, errors.Join(err, token.ReleaseWithError(), builder.AbandonWithError())
	}
	resources, err := builder.Freeze()
	if err != nil {
		return nil, nil, errors.Join(err, builder.AbandonWithError())
	}
	original.resources = resources
	diagnostics, captureErr := resources.AcquirePhysicalDiagnostics()
	if captureErr != nil {
		return nil, nil, errors.Join(captureErr, resources.ReleaseWithError())
	}

	descriptors := diagnostics.Physical()
	if len(descriptors) != 1 || descriptors[0].Kind != rootpublication.ResourceOuterLeafManifest || descriptors[0].Generation != candidate.ManifestRevision || descriptors[0].Digest() == ([32]byte{}) {
		closeErr := diagnostics.CloseWithError()
		return nil, nil, errors.Join(closeErr, resources.ReleaseWithError(), fmt.Errorf("%w: manifest replacement token does not match persisted revision %d", rootpublication.ErrResourceConflict, candidate.ManifestRevision))
	}
	descriptor := descriptors[0]
	resourceID, identity, digest := descriptor.ResourceID(), descriptor.Identity(), descriptor.Digest()
	// The source Set still owns the same physical edge. End the diagnostic
	// borrow before returning a closure or disposing that source edge.
	if err := diagnostics.CloseWithError(); err != nil {
		return nil, nil, errors.Join(err, resources.ReleaseWithError())
	}
	contentSyncs := store.durabilityCounters.ContentSyncs.Load() - beforeContent
	namespaceSyncs := store.durabilityCounters.NamespaceSyncs.Load() - beforeNamespace
	if contentSyncs == 0 || namespaceSyncs == 0 {
		return nil, nil, errors.Join(resources.ReleaseWithError(), fmt.Errorf("%w: manifest replacement lacks producer durability observations", rootpublication.ErrUnresolvedResource))
	}
	stringCharge := retainedalloc.AllocationCharge(uint64(len(resourceID))) + retainedalloc.AllocationCharge(uint64(len(identity.Platform)))
	if err := owner.AddPending(stringCharge); err != nil {
		return nil, nil, err
	}
	original.metadataCharge += stringCharge
	resourceID, identity.Platform = strings.Clone(resourceID), strings.Clone(identity.Platform)
	original.resourceID, original.identity, original.revision, original.digest = resourceID, identity, candidate.ManifestRevision, digest
	original.observations = LeafGenerationManifestStableObservations{ContentSyncs: contentSyncs, NamespaceSyncs: namespaceSyncs}
	return original, candidate, nil
}
