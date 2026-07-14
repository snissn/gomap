package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// ErrLeafGenerationPackStablePreparedClosureConsumed reports a second
// ownership transfer from an already-consumed or abandoned prepared closure.
var ErrLeafGenerationPackStablePreparedClosureConsumed = errors.New("treedb: leaf generation pack stable prepared closure consumed")

var (
	leafGenerationPackStableBeforeCaptureAdmissionTestHookMu sync.RWMutex
	leafGenerationPackStableBeforeCaptureAdmissionTestHook   func()
	leafGenerationPackStablePreparedClosureTestHookMu        sync.RWMutex
	leafGenerationPackStablePreparedClosureTestHook          func()
	leafGenerationPackStableClosureValidationTestHookMu      sync.RWMutex
	leafGenerationPackStableClosureValidationTestHook        func() error
)

func setLeafGenerationPackStableBeforeCaptureAdmissionTestHook(hook func()) func() {
	leafGenerationPackStableBeforeCaptureAdmissionTestHookMu.Lock()
	previous := leafGenerationPackStableBeforeCaptureAdmissionTestHook
	leafGenerationPackStableBeforeCaptureAdmissionTestHook = hook
	leafGenerationPackStableBeforeCaptureAdmissionTestHookMu.Unlock()
	return func() {
		leafGenerationPackStableBeforeCaptureAdmissionTestHookMu.Lock()
		leafGenerationPackStableBeforeCaptureAdmissionTestHook = previous
		leafGenerationPackStableBeforeCaptureAdmissionTestHookMu.Unlock()
	}
}

func runLeafGenerationPackStableBeforeCaptureAdmissionTestHook() {
	leafGenerationPackStableBeforeCaptureAdmissionTestHookMu.RLock()
	hook := leafGenerationPackStableBeforeCaptureAdmissionTestHook
	leafGenerationPackStableBeforeCaptureAdmissionTestHookMu.RUnlock()
	if hook != nil {
		hook()
	}
}

func setLeafGenerationPackStablePreparedClosureTestHook(hook func()) func() {
	leafGenerationPackStablePreparedClosureTestHookMu.Lock()
	previous := leafGenerationPackStablePreparedClosureTestHook
	leafGenerationPackStablePreparedClosureTestHook = hook
	leafGenerationPackStablePreparedClosureTestHookMu.Unlock()
	return func() {
		leafGenerationPackStablePreparedClosureTestHookMu.Lock()
		leafGenerationPackStablePreparedClosureTestHook = previous
		leafGenerationPackStablePreparedClosureTestHookMu.Unlock()
	}
}

func runLeafGenerationPackStablePreparedClosureTestHook() {
	leafGenerationPackStablePreparedClosureTestHookMu.RLock()
	hook := leafGenerationPackStablePreparedClosureTestHook
	leafGenerationPackStablePreparedClosureTestHookMu.RUnlock()
	if hook != nil {
		hook()
	}
}

func setLeafGenerationPackStableClosureValidationTestHook(hook func() error) func() {
	leafGenerationPackStableClosureValidationTestHookMu.Lock()
	previous := leafGenerationPackStableClosureValidationTestHook
	leafGenerationPackStableClosureValidationTestHook = hook
	leafGenerationPackStableClosureValidationTestHookMu.Unlock()
	return func() {
		leafGenerationPackStableClosureValidationTestHookMu.Lock()
		leafGenerationPackStableClosureValidationTestHook = previous
		leafGenerationPackStableClosureValidationTestHookMu.Unlock()
	}
}

func runLeafGenerationPackStableClosureValidationTestHook() error {
	leafGenerationPackStableClosureValidationTestHookMu.RLock()
	hook := leafGenerationPackStableClosureValidationTestHook
	leafGenerationPackStableClosureValidationTestHookMu.RUnlock()
	if hook == nil {
		return nil
	}
	return hook()
}

// LeafGenerationPackStableObservations records the production durability work
// performed before packed authority is transferred to publication.
type LeafGenerationPackStableObservations struct {
	Segments             uint64
	ContentSyncs         uint64
	NamespaceSyncs       uint64
	NamespaceObligations uint64
}

// LeafGenerationPackStablePreparedClosure owns exact packed outer-leaf
// physical children after the production staging promotion, but before any
// root or alternate-meta publication makes their pointers reachable.
type LeafGenerationPackStablePreparedClosure struct {
	mu           sync.Mutex
	resources    *rootpublication.StableResourceSet
	segments     []LeafPageLogSegment
	promoted     []rewriteCreatedSegment
	cleanupDir   *os.File
	cleanupPins  *rootpublication.IdentityPinRegistry
	pointers     []page.LeafLogPtr
	observations LeafGenerationPackStableObservations
	consumed     bool
}

// Pointers returns a copy of the packed pointers covered by the closure.
func (closure *LeafGenerationPackStablePreparedClosure) Pointers() []page.LeafLogPtr {
	if closure == nil {
		return nil
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	return append([]page.LeafLogPtr(nil), closure.pointers...)
}

// Segments returns a copy of the promoted physical segment identities.
func (closure *LeafGenerationPackStablePreparedClosure) Segments() []LeafPageLogSegment {
	if closure == nil {
		return nil
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	return append([]LeafPageLogSegment(nil), closure.segments...)
}

// Observations returns the production durability counters for this closure.
func (closure *LeafGenerationPackStablePreparedClosure) Observations() LeafGenerationPackStableObservations {
	if closure == nil {
		return LeafGenerationPackStableObservations{}
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	return closure.observations
}

// TakeStableResources transfers exact-handle authority exactly once.
func (closure *LeafGenerationPackStablePreparedClosure) TakeStableResources() (*rootpublication.StableResourceSet, error) {
	if closure == nil {
		return nil, ErrLeafGenerationPackStablePreparedClosureConsumed
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	if closure.consumed || closure.resources == nil {
		return nil, ErrLeafGenerationPackStablePreparedClosureConsumed
	}
	resources := closure.resources
	closure.resources = nil
	cleanupDir := closure.cleanupDir
	closure.cleanupDir = nil
	closure.cleanupPins = nil
	closure.promoted = nil
	closure.consumed = true
	if cleanupDir != nil {
		// The retained directory handle exists only so pre-transfer abandonment
		// can remove exact promoted children. After ownership transfer, cleanup
		// belongs to the recipient of the stable resources.
		_ = cleanupDir.Close()
	}
	return resources, nil
}

// Release abandons untransferred authority, removes its promoted but still
// unreachable children, and is idempotent. Once TakeStableResources succeeds,
// the recipient owns both the authority and the promoted files, so Release no
// longer removes them.
func (closure *LeafGenerationPackStablePreparedClosure) Release() error {
	if closure == nil {
		return nil
	}
	closure.mu.Lock()
	resources := closure.resources
	closure.resources = nil
	promoted := closure.promoted
	closure.promoted = nil
	cleanupDir := closure.cleanupDir
	closure.cleanupDir = nil
	cleanupPins := closure.cleanupPins
	closure.cleanupPins = nil
	closure.consumed = true
	closure.mu.Unlock()
	if resources != nil {
		resources.Release()
	}
	return cleanupLeafGenerationPackStablePreparedSegments(cleanupDir, promoted, cleanupPins)
}

// Abandon is an explicit alias for Release on pre-visibility failure.
func (closure *LeafGenerationPackStablePreparedClosure) Abandon() error { return closure.Release() }

func cleanupLeafGenerationPackStablePreparedSegments(parent *os.File, promoted []rewriteCreatedSegment, registry *rootpublication.IdentityPinRegistry) error {
	err := cleanupLeafGenerationPackStablePreparedSegmentsRetainingParent(parent, promoted, registry)
	if parent != nil {
		err = errors.Join(err, parent.Close())
	}
	if err != nil {
		return errors.Join(err, ErrRecoveryRequired)
	}
	return nil
}

// cleanupLeafGenerationPackStablePreparedSegmentsRetainingParent is
// intentionally idempotent and never consumes parent. A caller that must
// preserve cleanup authority after an ambiguous attempt can retain the same
// exact directory handle and retry during DB teardown.
func cleanupLeafGenerationPackStablePreparedSegmentsRetainingParent(parent *os.File, promoted []rewriteCreatedSegment, registry *rootpublication.IdentityPinRegistry) error {
	if parent == nil {
		if len(promoted) == 0 {
			return nil
		}
		return errors.Join(
			fmt.Errorf("%w: packed abandonment lacks exact destination parent", rootpublication.ErrResourceOwnership),
			ErrRecoveryRequired,
		)
	}
	var errs []error
	diagnosticDir := parent.Name()
	parentIdentity, parentIdentityErr := rootpublication.StableIdentityFromFile(parent)
	if parentIdentityErr != nil {
		return errors.Join(parentIdentityErr, ErrRecoveryRequired)
	}
	forgetProof := func(segment rewriteCreatedSegment, name string) {
		if registry == nil {
			return
		}
		if err := registry.ForgetStableNamespaceLinkIdentity(parentIdentity, segment.identity, name); err != nil {
			errs = append(errs, err)
		}
	}
	type namespaceProof struct {
		segment rewriteCreatedSegment
		name    string
	}
	proofsPendingDeletionSync := make([]namespaceProof, 0, len(promoted))
	queueProofAfterDeletionSync := func(segment rewriteCreatedSegment, name string) {
		proofsPendingDeletionSync = append(proofsPendingDeletionSync, namespaceProof{segment: segment, name: name})
	}
	for _, segment := range promoted {
		name := filepath.Base(segment.path)
		if name == "." || name == "" || segment.identity == (rootpublication.StableIdentity{}) {
			errs = append(errs, fmt.Errorf("%w: incomplete promoted packed segment", rootpublication.ErrUnresolvedResource))
			continue
		}
		child, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
		if err != nil {
			if os.IsNotExist(err) {
				queueProofAfterDeletionSync(segment, name)
				continue
			}
			errs = append(errs, fmt.Errorf("open promoted packed segment %q for abandonment: %w", name, err))
			continue
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(child)
		linkErr := rootpublication.ValidateStableChildLink(parent, child, name)
		if identityErr != nil || linkErr != nil || !rootpublication.SamePhysicalIdentity(identity, segment.identity) {
			_ = child.Close()
			if identityErr == nil && linkErr == nil {
				forgetProof(segment, name)
				identityErr = fmt.Errorf("%w: promoted packed path %q was rebound", rootpublication.ErrResourceConflict, name)
			}
			errs = append(errs, errors.Join(identityErr, linkErr, ErrRecoveryRequired))
			continue
		}
		if err := rootpublication.RemoveStableChildFile(parent, name); err != nil {
			_ = child.Close()
			errs = append(errs, fmt.Errorf("unlink promoted packed segment %q: %w", name, err))
			continue
		}
		queueProofAfterDeletionSync(segment, name)
		if err := observeNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceOuterLeaf, diagnosticDir, segment.path, ""); err != nil {
			errs = append(errs, err)
		}
		if err := child.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	deletionNamespaceSynced := false
	if len(promoted) != 0 {
		// Sync even when every child is already absent. This makes a retry after
		// a partial unlink or an interrupted prior directory sync prove the same
		// deletion namespace durably and keeps cleanup idempotent.
		if err := durabilitycut.EmitPath(durabilitycut.BeforeDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, diagnosticDir, diagnosticDir); err != nil {
			errs = append(errs, errors.Join(err, ErrRecoveryRequired))
		} else if err := parent.Sync(); err != nil {
			errs = append(errs, errors.Join(err, ErrRecoveryRequired))
		} else if err := durabilitycut.EmitPath(durabilitycut.AfterDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, diagnosticDir, diagnosticDir); err != nil {
			errs = append(errs, errors.Join(err, ErrRecoveryRequired))
		} else {
			deletionNamespaceSynced = true
		}
	}
	if deletionNamespaceSynced {
		// The positive-link proof remains recovery authority until the exact
		// deletion namespace is durably established. Retiring it immediately
		// after unlink would create an authority gap if this sync were ambiguous.
		for _, proof := range proofsPendingDeletionSync {
			forgetProof(proof.segment, proof.name)
		}
	}
	if err := errors.Join(errs...); err != nil {
		// At this boundary the children have already been promoted into their
		// final namespace. Any failure to prove their exact unlink and the
		// deletion-directory sync is recovery-significant.
		return errors.Join(err, ErrRecoveryRequired)
	}
	return nil
}

func (authority *leafGenerationPackPromotionAuthority) takeStablePreparedClosure(promoted []rewriteCreatedSegment, pointers []page.LeafLogPtr, contentSyncs uint64, stagingRoot string) (*LeafGenerationPackStablePreparedClosure, error) {
	if authority == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	fail := func(validationErr error) (*LeafGenerationPackStablePreparedClosure, error) {
		// Promotion has completed, but no root or alternate meta page can yet
		// reference these children. Use the retained destination parent for
		// exact identity-verified abandonment before releasing producer
		// authority. If abandonment is ambiguous, preserve that same parent,
		// every exact child handle, and the pins for a teardown retry.
		cleanupErr := cleanupLeafGenerationPackStablePreparedSegmentsRetainingParent(
			authority.destinationParent, promoted, authority.db.valueLogIdentityPins,
		)
		if cleanupErr != nil {
			authority.retainPromotedCleanupForRecovery(promoted, stagingRoot)
			return nil, errors.Join(validationErr, cleanupErr, ErrRecoveryRequired)
		}
		return nil, validationErr
	}
	if authority.resources == nil || authority.released {
		return fail(rootpublication.ErrResourceOwnership)
	}
	if err := runLeafGenerationPackStableClosureValidationTestHook(); err != nil {
		return fail(errors.Join(
			fmt.Errorf("%w: packed closure validation fault", rootpublication.ErrUnresolvedResource),
			err,
		))
	}
	// Token construction binds the producer's logical file generation into the
	// captured physical identity. The authority's pre-token identity came
	// directly from fstat and therefore has generation zero. Key only by physical
	// identity here, then validate the descriptor's logical generation below.
	want := make(map[rootpublication.StableIdentity]uint32, len(authority.segments))
	for i := range authority.segments {
		segment := &authority.segments[i]
		identity := segment.identity
		identity.Generation = 0
		want[identity] = segment.created.fileID
	}
	seen := make(map[rootpublication.StableIdentity]struct{}, len(want))
	frontiers := make(map[uint32]uint64, len(want))
	var namespaceSyncs uint64
	var namespaceObligations uint64
	for _, descriptor := range authority.resources.Descriptors() {
		if descriptor.Kind() != rootpublication.ResourceOuterLeafPack {
			continue
		}
		if fields := descriptor.ReachabilityFields(); len(fields) != 1 || fields[0] != rootpublication.ReachabilityOuterLeafPackedPointer {
			return fail(fmt.Errorf("%w: packed descriptor reachability=%q", rootpublication.ErrUnresolvedResource, fields))
		}
		identity := descriptor.Identity()
		identity.Generation = 0
		fileID, ok := want[identity]
		if !ok || descriptor.Generation() != uint64(fileID) || descriptor.Frontier().Bytes == 0 {
			return fail(fmt.Errorf("%w: packed descriptor does not match promoted segment", rootpublication.ErrResourceConflict))
		}
		seen[identity] = struct{}{}
		frontiers[page.ValueLogSegmentID(fileID)] = descriptor.Frontier().Bytes
		namespaceObligations++
	}
	if len(seen) != len(want) {
		return fail(fmt.Errorf("%w: packed closure descriptors=%d promoted_segments=%d", rootpublication.ErrUnresolvedResource, len(seen), len(want)))
	}
	if len(pointers) == 0 {
		return fail(fmt.Errorf("%w: packed closure has no reachable pointers", rootpublication.ErrUnresolvedResource))
	}
	referenced := make(map[uint32]struct{}, len(frontiers))
	for _, pointer := range pointers {
		frontier, ok := frontiers[pointer.FileID]
		length := uint64(pointer.RecordLength())
		if !ok || pointer.FileID == 0 || length == 0 {
			return fail(fmt.Errorf("%w: packed closure pointer references foreign or empty segment %d", rootpublication.ErrResourceConflict, pointer.FileID))
		}
		if pointer.Offset > ^uint64(0)-length || pointer.Offset+length > frontier {
			return fail(fmt.Errorf("%w: packed closure pointer exceeds immutable frontier", rootpublication.ErrFrontierBeyondResource))
		}
		referenced[pointer.FileID] = struct{}{}
	}
	if len(referenced) != len(frontiers) {
		return fail(fmt.Errorf("%w: packed closure pointers cover %d of %d promoted segments", rootpublication.ErrUnresolvedResource, len(referenced), len(frontiers)))
	}
	for _, stats := range authority.resources.Stats(time.Now()) {
		if stats.Kind == rootpublication.ResourceOuterLeafPack {
			namespaceSyncs += stats.NamespaceSyncs
		}
	}
	if contentSyncs == 0 || namespaceSyncs == 0 {
		return fail(fmt.Errorf("%w: packed closure lacks producer durability observations", rootpublication.ErrUnresolvedResource))
	}
	segments := make([]LeafPageLogSegment, len(promoted))
	for i := range promoted {
		segments[i] = LeafPageLogSegment{Path: promoted[i].path, FileID: promoted[i].fileID}
	}
	closure := &LeafGenerationPackStablePreparedClosure{
		resources:   authority.resources,
		segments:    segments,
		promoted:    append([]rewriteCreatedSegment(nil), promoted...),
		cleanupDir:  authority.destinationParent,
		cleanupPins: authority.db.valueLogIdentityPins,
		pointers:    append([]page.LeafLogPtr(nil), pointers...),
		observations: LeafGenerationPackStableObservations{
			Segments: uint64(len(promoted)), ContentSyncs: contentSyncs, NamespaceSyncs: namespaceSyncs,
			NamespaceObligations: namespaceObligations,
		},
	}
	authority.resources = nil
	authority.destinationParent = nil
	return closure, nil
}

// PrepareLeafGenerationPackStableClosure uses the production rewrite writer
// and packed promotion authority to prepare physical packed outer-leaf
// children. It deliberately stops before manager registration and root/meta
// publication.
func (db *DB) PrepareLeafGenerationPackStableClosure(ctx context.Context, leafPages [][]byte) (closure *LeafGenerationPackStablePreparedClosure, retErr error) {
	if db == nil || db.closing.Load() {
		return nil, ErrClosed
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if len(leafPages) == 0 {
		return nil, errors.New("treedb: leaf generation pack stable prepare requires leaf pages")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runLeafGenerationPackStableBeforeCaptureAdmissionTestHook()
	captureLease, err := db.AcquireStableResourceCaptureLease()
	if err != nil {
		return nil, err
	}
	// Admission precedes the allocator's writeMu read lock. Close drops writeMu
	// before waiting on teardownMu, so this ordering cannot deadlock shutdown.
	// Keep the lease until all deferred staging/authority cleanup has completed.
	defer captureLease.Release()
	layout := resolveStorageLayout(db.dir)
	stagingRoot, err := os.MkdirTemp(layout.leafVLogDir, ".leaf-pack-stable-prepare-")
	if err != nil {
		return nil, err
	}
	var writer *rewriteWriter
	var authority *leafGenerationPackPromotionAuthority
	defer func() {
		var cleanupErr error
		retainedForRecovery := authority != nil && authority.retainedForRecovery
		if authority != nil && !retainedForRecovery {
			cleanupErr = errors.Join(cleanupErr, authority.release())
		}
		if writer != nil {
			cleanupErr = errors.Join(cleanupErr, writer.Close())
		}
		if !retainedForRecovery {
			cleanupErr = errors.Join(cleanupErr, removeLeafGenerationPackStagingDirFn(stagingRoot))
		}
		if retErr != nil || cleanupErr != nil {
			if closure != nil {
				cleanupErr = errors.Join(cleanupErr, closure.Release())
				closure = nil
			}
			retErr = errors.Join(retErr, cleanupErr)
		}
	}()
	stagingLeafDir := filepath.Join(stagingRoot, filepath.Base(layout.leafVLogDir))
	if err := os.MkdirAll(stagingLeafDir, 0o700); err != nil {
		return nil, err
	}
	seqAlloc, ridAlloc := db.leafGenerationPackAllocators(1, 1, nil)
	writer = newRewriteWriter(layout.valueVLogDir, 0, 0, 0)
	writer.ConfigureLeafLog(stagingLeafDir, rewriteLeafLogLaneID, 1)
	writer.configureLeafStaging(stagingRoot)
	writer.setLeafPageLogSeqAllocator(seqAlloc)
	writer.setLeafPageLogRIDAllocator(ridAlloc)
	authority, err = newLeafGenerationPackPromotionAuthority(db, stagingRoot, stagingLeafDir, layout.leafVLogDir)
	if err != nil {
		return nil, err
	}
	if err := authority.captureDictionary(ctx, 0, nil); err != nil {
		return nil, err
	}
	pointers, err := writer.AppendLeafPages(leafPages)
	if err != nil {
		return nil, err
	}
	if err := writer.Sync(); err != nil {
		return nil, err
	}
	if writer.leafW == nil {
		return nil, fmt.Errorf("%w: packed writer has no physical leaf-log producer", rootpublication.ErrUnresolvedResource)
	}
	contentSyncs := writer.leafW.DurabilityStats().FileSyncCalls
	created, err := writer.createdSegmentsSnapshot()
	if err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if err := authority.capture(created, pointers); err != nil {
		return nil, err
	}
	promoted, mutated, err := authority.promote()
	if err != nil {
		return nil, err
	}
	if !mutated || len(promoted) == 0 {
		return nil, fmt.Errorf("%w: packed promotion prepared no physical children", rootpublication.ErrUnresolvedResource)
	}
	closure, retErr = authority.takeStablePreparedClosure(promoted, pointers, contentSyncs, stagingRoot)
	if retErr == nil {
		runLeafGenerationPackStablePreparedClosureTestHook()
	}
	return closure, retErr
}
