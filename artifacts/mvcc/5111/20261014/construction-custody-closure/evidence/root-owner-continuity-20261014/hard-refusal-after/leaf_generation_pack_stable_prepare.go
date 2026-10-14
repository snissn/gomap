package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"

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
	mu                   sync.Mutex
	completion           sync.Cond
	resources            *rootpublication.StableResourceSet
	segments             []LeafPageLogSegment
	promoted             []rewriteCreatedSegment
	cleanupDir           *os.File
	cleanupPins          *rootpublication.IdentityPinRegistry
	pointers             []page.LeafLogPtr
	observations         LeafGenerationPackStableObservations
	consumed             bool
	db                   *DB
	metadata             *retainedalloc.Owner
	metadataCharge       uint64
	attempted            bool
	done                 bool
	cleanupErr           error
	failure              originalDBCleanupFailure
	cleanupChild         *os.File
	cleanupChildCharge   uint64
	parentCloseAttempted bool
	parentCloseErr       error
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

// Take transfers only after the SAME retained cleanup parent closes successfully.
// Failure preserves the original Set, parent, pins and arrays; no action retries.
func (closure *LeafGenerationPackStablePreparedClosure) TakeStableResources() (*rootpublication.StableResourceSet, error) {
	if closure == nil {
		return nil, ErrLeafGenerationPackStablePreparedClosureConsumed
	}
	closure.mu.Lock()
	if closure.attempted || closure.consumed || closure.resources == nil {
		err := closure.cleanupErr
		closure.mu.Unlock()
		return nil, errors.Join(err, ErrLeafGenerationPackStablePreparedClosureConsumed)
	}
	closure.attempted = true
	closure.mu.Unlock()
	if err := closure.closeCleanupParent(); err != nil {
		return nil, closure.retainCleanupFailure(err)
	}
	closure.mu.Lock()
	resources := closure.resources
	closure.resources, closure.promoted, closure.cleanupPins = nil, nil, nil
	closure.consumed, closure.attempted, closure.done = true, false, false
	closure.completion.Broadcast()
	closure.mu.Unlock()
	return resources, nil
}

// Release consumes only original cleanup edges. A refusal retains this SAME
// preborn containing object in DB.bgErr before any metadata contribution refund.
func (closure *LeafGenerationPackStablePreparedClosure) Release() error {
	if closure == nil {
		return nil
	}
	closure.mu.Lock()
	for closure.attempted && !closure.done {
		// The containing condition uses the original mutex; no second cleanup
		// owner or terminal refusal is created while physical Close is active.
		if closure.completion.L == nil {
			closure.completion.L = &closure.mu
		}
		closure.completion.Wait()
	}
	if closure.attempted {
		err := closure.cleanupErr
		closure.mu.Unlock()
		return err
	}
	closure.attempted = true
	resources, consumed := closure.resources, closure.consumed
	closure.mu.Unlock()
	if resources != nil {
		if err := resources.ReleaseWithError(); err != nil {
			return closure.retainCleanupFailure(err)
		}
		closure.mu.Lock()
		closure.resources = nil
		closure.mu.Unlock()
	}
	if !consumed {
		if err := cleanupLeafGenerationPackStablePreparedSegmentsWithCustody(closure.cleanupDir, closure.promoted, closure.cleanupPins, closure); err != nil {
			return closure.retainCleanupFailure(err)
		}
	}
	if err := closure.closeCleanupParent(); err != nil {
		return closure.retainCleanupFailure(err)
	}
	closure.mu.Lock()
	clear(closure.segments)
	clear(closure.promoted)
	clear(closure.pointers)
	closure.resources, closure.segments, closure.promoted, closure.pointers, closure.cleanupPins = nil, nil, nil, nil, nil
	owner, charge := closure.metadata, closure.metadataCharge
	closure.metadata, closure.metadataCharge, closure.db = nil, 0, nil
	closure.consumed = true
	closure.mu.Unlock()
	if owner != nil {
		owner.RemovePending(charge)
	}
	closure.mu.Lock()
	closure.done = true
	closure.completion.Broadcast()
	closure.mu.Unlock()
	return nil
}
func (closure *LeafGenerationPackStablePreparedClosure) Abandon() error { return closure.Release() }

func (closure *LeafGenerationPackStablePreparedClosure) retainCleanupFailure(cause error) error {
	closure.failure.retainCause(0, cause)
	closure.mu.Lock()
	closure.cleanupErr = &closure.failure
	db, owner := closure.db, closure.metadata
	closure.mu.Unlock()
	if owner != nil {
		owner.CleanupFailed()
	}
	if db != nil {
		db.retainOriginalCleanupError(&closure.failure)
	}
	closure.mu.Lock()
	closure.done = true
	closure.completion.Broadcast()
	closure.mu.Unlock()
	return &closure.failure
}
func (closure *LeafGenerationPackStablePreparedClosure) closeCleanupParent() error {
	if closure.cleanupDir == nil {
		return nil
	}
	if closure.parentCloseAttempted {
		return closure.parentCloseErr
	}
	closure.parentCloseAttempted = true
	closure.parentCloseErr = leafGenerationPackAuthorityCloseFile(closure.cleanupDir)
	if closure.parentCloseErr == nil {
		closure.cleanupDir = nil
	}
	return closure.parentCloseErr
}

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
	return cleanupLeafGenerationPackStablePreparedSegmentsWithCustody(parent, promoted, registry, nil)
}
func cleanupLeafGenerationPackStablePreparedSegmentsWithCustody(parent *os.File, promoted []rewriteCreatedSegment, registry *rootpublication.IdentityPinRegistry, custody *LeafGenerationPackStablePreparedClosure) error {
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
	var proofsPendingDeletionSync []namespaceProof
	if custody == nil {
		proofsPendingDeletionSync = make([]namespaceProof, 0, len(promoted))
	}
	queueProofAfterDeletionSync := func(segment rewriteCreatedSegment, name string) {
		if custody == nil {
			proofsPendingDeletionSync = append(proofsPendingDeletionSync, namespaceProof{segment: segment, name: name})
		}
	}
	for _, segment := range promoted {
		name := filepath.Base(segment.path)
		if name == "." || name == "" || segment.identity == (rootpublication.StableIdentity{}) {
			errs = append(errs, fmt.Errorf("%w: incomplete promoted packed segment", rootpublication.ErrUnresolvedResource))
			continue
		}
		var childCharge uint64
		if custody != nil {
			var err error
			childCharge, err = rootpublication.StableFileMetadataCharge(uint64(len(parent.Name()) + 1 + len(name)))
			if err != nil {
				return err
			}
			if err := custody.metadata.AddPending(childCharge); err != nil {
				return err
			}
			custody.metadataCharge += childCharge
		}
		child, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
		if custody != nil && err != nil {
			custody.metadata.RemovePending(childCharge)
			custody.metadataCharge -= childCharge
		}
		if custody != nil && child != nil {
			custody.cleanupChild, custody.cleanupChildCharge = child, childCharge
		}
		closeChild := func() error {
			if child == nil {
				return nil
			}
			err := child.Close()
			if err == nil && custody != nil {
				custody.cleanupChild = nil
				custody.metadata.RemovePending(custody.cleanupChildCharge)
				custody.metadataCharge -= custody.cleanupChildCharge
				custody.cleanupChildCharge = 0
			}
			return err
		}
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
			closeErr := closeChild()
			if custody != nil && closeErr != nil {
				return errors.Join(identityErr, linkErr, closeErr, ErrRecoveryRequired)
			}
			if identityErr == nil && linkErr == nil {
				forgetProof(segment, name)
				identityErr = fmt.Errorf("%w: promoted packed path %q was rebound", rootpublication.ErrResourceConflict, name)
			}
			errs = append(errs, errors.Join(identityErr, linkErr, closeErr, ErrRecoveryRequired))
			continue
		}
		if err := rootpublication.RemoveStableChildFile(parent, name); err != nil {
			closeErr := closeChild()
			if custody != nil {
				return errors.Join(err, closeErr, ErrRecoveryRequired)
			}
			errs = append(errs, errors.Join(closeErr, fmt.Errorf("unlink promoted packed segment %q: %w", name, err)))
			continue
		}
		queueProofAfterDeletionSync(segment, name)
		if err := observeNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceOuterLeaf, diagnosticDir, segment.path, ""); err != nil {
			errs = append(errs, err)
		}
		if err := closeChild(); err != nil {
			if custody != nil {
				return errors.Join(err, ErrRecoveryRequired)
			}
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
	if deletionNamespaceSynced && (custody == nil || len(errs) == 0) {
		// The positive-link proof remains recovery authority until the exact
		// deletion namespace is durably established. Retiring it immediately
		// after unlink would create an authority gap if this sync were ambiguous.
		if custody != nil {
			for _, segment := range promoted {
				forgetProof(segment, filepath.Base(segment.path))
			}
		} else {
			for _, proof := range proofsPendingDeletionSync {
				forgetProof(proof.segment, proof.name)
			}
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

func (authority *leafGenerationPackPromotionAuthority) takeStablePreparedClosure(original *LeafGenerationPackStablePreparedClosure, promoted []rewriteCreatedSegment, pointers []page.LeafLogPtr, contentSyncs uint64, stagingRoot string) (closure *LeafGenerationPackStablePreparedClosure, retErr error) {
	if authority == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	fail := func(validationErr error) (*LeafGenerationPackStablePreparedClosure, error) {
		// Promotion has completed, but no root or alternate meta page can yet
		// reference these children. Use the retained destination parent for
		// exact identity-verified abandonment before releasing producer
		// authority. If abandonment is ambiguous, preserve that same parent,
		// every exact child handle, and the pins for a teardown retry.
		cleanupErr := authority.rollbackPromoted()
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
	if len(promoted) != len(authority.segments) {
		return fail(fmt.Errorf("%w: promoted segment count differs from original authority", rootpublication.ErrResourceConflict))
	}
	for i := range authority.segments {
		authority.segments[i].descriptorSeen = false
		authority.segments[i].referenced = false
	}
	for _, promotedSegment := range promoted {
		position := authority.segmentIndex.get(promotedSegment.fileID)
		if position == 0 {
			return fail(rootpublication.ErrResourceConflict)
		}
		segment := &authority.segments[position-1]
		if promotedSegment.path != segment.created.path || !rootpublication.SamePhysicalIdentity(promotedSegment.identity, segment.identity) {
			return fail(rootpublication.ErrResourceConflict)
		}
	}
	var namespaceSyncs, namespaceObligations uint64
	diagnostics, captureErr := authority.resources.AcquirePhysicalDiagnostics()
	if captureErr != nil {
		return fail(captureErr)
	}
	defer func() { retErr = errors.Join(retErr, diagnostics.CloseWithError()) }()
	for _, descriptor := range diagnostics.Physical() {
		if descriptor.Kind != rootpublication.ResourceOuterLeafPack {
			continue
		}
		if fields := descriptor.ReachabilityFields(); len(fields) != 1 || fields[0] != rootpublication.ReachabilityOuterLeafPackedPointer {
			return fail(fmt.Errorf("%w: packed descriptor reachability=%q", rootpublication.ErrUnresolvedResource, fields))
		}
		if descriptor.Generation > math.MaxUint32 {
			return fail(rootpublication.ErrResourceConflict)
		}
		position := authority.segmentIndex.get(uint32(descriptor.Generation))
		if position == 0 {
			return fail(rootpublication.ErrResourceConflict)
		}
		segment := &authority.segments[position-1]
		identity := descriptor.Identity()
		identity.Generation = 0
		if segment.descriptorSeen || identity != segment.identity || descriptor.Frontier().Bytes != segment.frontierBytes || segment.frontierBytes == 0 {
			return fail(fmt.Errorf("%w: packed descriptor differs from captured original segment", rootpublication.ErrResourceConflict))
		}
		segment.descriptorSeen = true
		namespaceObligations++
	}
	if len(pointers) == 0 {
		return fail(fmt.Errorf("%w: packed closure has no reachable pointers", rootpublication.ErrUnresolvedResource))
	}
	for _, pointer := range pointers {
		position := authority.segmentIndex.get(page.ValueLogFileID(pointer.FileID))
		length := uint64(pointer.RecordLength())
		if position == 0 || pointer.FileID == 0 || length == 0 {
			return fail(fmt.Errorf("%w: packed closure pointer references foreign or empty segment %d", rootpublication.ErrResourceConflict, pointer.FileID))
		}
		segment := &authority.segments[position-1]
		if pointer.Offset > ^uint64(0)-length || pointer.Offset+length > segment.frontierBytes {
			return fail(fmt.Errorf("%w: packed closure pointer exceeds immutable frontier", rootpublication.ErrFrontierBeyondResource))
		}
		segment.referenced = true
	}
	for _, segment := range authority.segments {
		if !segment.descriptorSeen || !segment.referenced {
			return fail(fmt.Errorf("%w: packed closure lacks exact descriptor or pointer coverage", rootpublication.ErrUnresolvedResource))
		}
	}
	statistics, statsErr := authority.resources.AcquireStats(time.Now())
	if statsErr != nil {
		return fail(statsErr)
	}
	defer func() { retErr = errors.Join(retErr, statistics.CloseWithError()) }()
	for _, stats := range statistics.Stats() {
		if stats.Kind == rootpublication.ResourceOuterLeafPack {
			namespaceSyncs += stats.NamespaceSyncs
		}
	}
	if contentSyncs == 0 || namespaceSyncs == 0 {
		return fail(fmt.Errorf("%w: packed closure lacks producer durability observations", rootpublication.ErrUnresolvedResource))
	}
	if err := statistics.CloseWithError(); err != nil {
		return fail(err)
	}
	if err := diagnostics.CloseWithError(); err != nil {
		return fail(err)
	}
	charge, chargeErr := packPreparedPayloadCharge(promoted, pointers)
	if chargeErr != nil {
		return fail(chargeErr)
	}
	if err := original.metadata.AddPending(charge); err != nil {
		return fail(err)
	}
	original.metadataCharge += charge
	segments := make([]LeafPageLogSegment, len(promoted))
	ownedPromoted := make([]rewriteCreatedSegment, len(promoted))
	for i := range promoted {
		ownedPromoted[i] = promoted[i]
		ownedPromoted[i].path = strings.Clone(promoted[i].path)
		segments[i] = LeafPageLogSegment{Path: ownedPromoted[i].path, FileID: promoted[i].fileID}
	}
	original.resources, original.segments, original.promoted = authority.resources, segments, ownedPromoted
	original.cleanupDir, original.cleanupPins = authority.destinationParent, authority.db.valueLogIdentityPins
	original.pointers = make([]page.LeafLogPtr, len(pointers))
	copy(original.pointers, pointers)
	original.observations = LeafGenerationPackStableObservations{
		Segments: uint64(len(promoted)), ContentSyncs: contentSyncs, NamespaceSyncs: namespaceSyncs,
		NamespaceObligations: namespaceObligations,
	}
	closure = original
	authority.resources = nil
	authority.destinationParent = nil
	return closure, nil
}

func packPreparedPayloadCharge(promoted []rewriteCreatedSegment, pointers []page.LeafLogPtr) (uint64, error) {
	var total uint64
	add := func(count uint64, size uint64) error {
		return checkedRetainedPayloadChargeAdd(&total, count, size)
	}
	if err := add(uint64(len(promoted)), uint64(unsafe.Sizeof(LeafPageLogSegment{}))); err != nil {
		return 0, err
	}
	if err := add(uint64(len(promoted)), uint64(unsafe.Sizeof(rewriteCreatedSegment{}))); err != nil {
		return 0, err
	}
	if err := add(uint64(len(pointers)), uint64(unsafe.Sizeof(page.LeafLogPtr{}))); err != nil {
		return 0, err
	}
	for _, segment := range promoted {
		if err := add(uint64(len(segment.path)), 1); err != nil {
			return 0, err
		}
	}
	return total, nil
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
	owner := db.valueLogIdentityPins.MetadataOwner()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(LeafGenerationPackStablePreparedClosure{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	original := &LeafGenerationPackStablePreparedClosure{db: db, metadata: owner, metadataCharge: charge}
	original.failure.payload = original
	defer func() {
		if closure == nil {
			retErr = errors.Join(retErr, original.Release())
		}
	}()
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
	segments, err := listValueLogSegments(db.dir)
	if err != nil {
		return nil, err
	}
	leafStartSeq := maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
	seqAlloc, ridAlloc, err := db.leafGenerationPackAllocators(leafStartSeq, 1, nil)
	if err != nil {
		return nil, err
	}
	stagingRoot, err := makeLeafGenerationPackStagingDirFn(layout.leafVLogDir, ".leaf-pack-stable-prepare-")
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
	writer = newRewriteWriter(layout.valueVLogDir, 0, 0, 0, db.valueLogIdentityPins)
	writer.ConfigureLeafLog(stagingLeafDir, rewriteLeafLogLaneID, leafStartSeq)
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
	closure, retErr = authority.takeStablePreparedClosure(original, promoted, pointers, contentSyncs, stagingRoot)
	if retErr == nil {
		runLeafGenerationPackStablePreparedClosureTestHook()
	}
	return closure, retErr
}

// checkedRetainedPayloadChargeAdd admits explicit producer geometry and overlap;
// arithmetic refusal happens before allocation or physical effects.
func checkedRetainedPayloadChargeAdd(total *uint64, count, width uint64) error {
	if width == 0 || count > math.MaxUint64/width {
		return retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(count * width)
	if charge > math.MaxUint64-*total {
		return retainedalloc.ErrCapacity
	}
	*total += charge
	return nil
}
func checkedRetainedCapacity(base int, counts ...int) (int, error) {
	for _, count := range counts {
		if count < 0 || count > int(^uint(0)>>1)-base {
			return 0, retainedalloc.ErrCapacity
		}
		base += count
	}
	return base, nil
}
