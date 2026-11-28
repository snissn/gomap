package collections

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var (
	errColumnPublishPlanLeaseConsumed           = errors.New("collections: column publish plan lease already consumed")
	errColumnPublishPlanLeaseNotInstalling      = errors.New("collections: column publish plan lease is not installing")
	errColumnPublishPlanLeaseCleanupQuarantined = errors.New("collections: column publish plan cleanup retained quarantined assets")
)

type columnPublishPlanLeaseState uint8

const (
	columnPublishPlanLeasePrepared columnPublishPlanLeaseState = iota + 1
	columnPublishPlanLeaseInstalling
	columnPublishPlanLeaseCommitted
	columnPublishPlanLeaseAbandoned
	columnPublishPlanLeaseQuarantined
)

// columnPublishPlanLease is the sole owner of one unpublished plan's physical
// assets and stable resources until publication takes them or cleanup proves
// that the unpublished tail can be removed.
type columnPublishPlanLease struct {
	mu sync.Mutex

	collection               *Collection
	plan                     ColumnPublishPlan
	state                    columnPublishPlanLeaseState
	transferred              bool
	verifiedStableIdentities map[string]stableColumnAssetCleanupIdentity

	preparedLease   *ColumnAssetLifecycleRegistryLease
	pendingLease    *ColumnAssetLifecycleRegistryLease
	quarantineLease *ColumnAssetLifecycleRegistryLease
}

type columnPublishPlanQuarantineError struct {
	err   error
	lease *ColumnAssetLifecycleRegistryLease
}

func (e *columnPublishPlanQuarantineError) Error() string {
	if e == nil || e.err == nil {
		return errColumnPublishPlanLeaseCleanupQuarantined.Error()
	}
	return e.err.Error()
}

func (e *columnPublishPlanQuarantineError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func newColumnPublishPlanLease(collection *Collection, plan ColumnPublishPlan) (_ *columnPublishPlanLease, retErr error) {
	if collection == nil || collection.db == nil {
		plan.releaseStableResources()
		return nil, errCollectionDBNil
	}
	if !plan.Enabled || plan.Collection == "" || plan.AppliedCommandLSN == 0 {
		plan.releaseStableResources()
		return nil, errors.New("collections: column publish plan lease requires an enabled bound plan")
	}
	if collection.meta.Name != "" && plan.Collection != collection.meta.Name {
		plan.releaseStableResources()
		return nil, fmt.Errorf("collections: column publish plan lease collection %q does not match %q", plan.Collection, collection.meta.Name)
	}

	var verifiedIdentities map[string]stableColumnAssetCleanupIdentity
	if plan.stablePreparedAssets && plan.stableResources != nil {
		if captured, captureErr := captureStableColumnAssetCleanupIdentities(collection.db.ColumnAssetRootDir(), plan.stableResources); captureErr == nil {
			verifiedIdentities = captured
		}
	}
	lease := &columnPublishPlanLease{
		collection:               collection,
		plan:                     plan,
		state:                    columnPublishPlanLeasePrepared,
		verifiedStableIdentities: verifiedIdentities,
	}
	refs := columnPublishPlanPreparedRefs(plan)
	if len(refs) == 0 {
		return lease, nil
	}
	preparedLease, err := collection.RegisterColumnAssetPreparedAssets(ColumnAssetPreparedAssetRegistrationOptions{
		Owner:  lease.owner(),
		Source: "column_publish_plan",
		Reason: "unpublished column publish plan",
		Refs:   refs,
	})
	if err == nil {
		lease.preparedLease = preparedLease
		return lease, nil
	}
	var retained []ColumnAssetRef
	var cleanupErr error
	if plan.stablePreparedAssets {
		retained, cleanupErr = cleanupUnpublishedStableColumnPublishAssets(
			collection.db.ColumnAssetRootDir(),
			collection.db.StableResourceIdentityPinRegistry(),
			plan.PreparedAssets,
			&plan.stableResources,
			verifiedIdentities,
		)
	} else {
		retained, cleanupErr = cleanupUnpublishedColumnPublishAssets(collection.db.ColumnAssetRootDir(), plan.PreparedAssets)
		plan.releaseStableResources()
	}
	if len(retained) != 0 {
		quarantineLease, quarantineErr := collection.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
			Owner: lease.owner(), Source: "column_publish_plan", Reason: "prepared-plan registration failed", Refs: retained,
		})
		lease.quarantineLease = quarantineLease
		joined := errors.Join(cleanupErr, errColumnPublishPlanLeaseCleanupQuarantined, quarantineErr)
		if quarantineLease != nil {
			joined = &columnPublishPlanQuarantineError{err: joined, lease: quarantineLease}
		}
		return nil, errors.Join(err, joined)
	}
	return nil, errors.Join(err, cleanupErr)
}

func (l *columnPublishPlanLease) owner() string {
	return fmt.Sprintf("column-publish-plan-%s-%d", l.plan.Collection, l.plan.AppliedCommandLSN)
}

func (l *columnPublishPlanLease) beginInstall(collection string, appliedCommandLSN, baseManifestRootID uint64) (ColumnPublishPlan, error) {
	if l == nil {
		return ColumnPublishPlan{}, errors.New("collections: nil column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != columnPublishPlanLeasePrepared {
		return ColumnPublishPlan{}, errColumnPublishPlanLeaseConsumed
	}
	if l.plan.Collection != collection || l.plan.AppliedCommandLSN != appliedCommandLSN ||
		l.plan.ManifestRootBaseID != baseManifestRootID || l.plan.RootDelta.BaseRootID != baseManifestRootID {
		return ColumnPublishPlan{}, fmt.Errorf("collections: prebuilt column publish plan binding mismatch collection=%q lsn=%d base=%d", collection, appliedCommandLSN, baseManifestRootID)
	}
	refs := columnPublishPlanPreparedRefs(l.plan)
	if len(refs) != 0 {
		pendingLease, err := l.collection.RegisterColumnAssetPendingPublish(ColumnAssetPendingPublishRegistrationOptions{
			Owner: l.owner(), Source: "column_publish_plan", Reason: "atomic column publish in progress", Refs: refs,
		})
		if err != nil {
			return ColumnPublishPlan{}, err
		}
		l.pendingLease = pendingLease
		if l.preparedLease != nil {
			if err := l.preparedLease.Close(); err != nil {
				_ = pendingLease.Close()
				l.pendingLease = nil
				return ColumnPublishPlan{}, err
			}
			l.preparedLease = nil
		}
	}
	l.state = columnPublishPlanLeaseInstalling
	return cloneColumnPublishPlanForHook(l.plan), nil
}

func (l *columnPublishPlanLease) transferStableResources(ctx backenddb.CommandWALPublishContext) error {
	if l == nil {
		return errors.New("collections: nil column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.transferStableResourcesLocked(ctx)
}

func (l *columnPublishPlanLease) transferStableResourcesLocked(ctx backenddb.CommandWALPublishContext) error {
	if l.state != columnPublishPlanLeaseInstalling {
		return errColumnPublishPlanLeaseNotInstalling
	}
	if l.transferred {
		return errColumnPublishPlanLeaseConsumed
	}
	resources := l.plan.takeStableResources()
	l.transferred = true
	return ctx.RegisterDurableResources(resources)
}

func (l *columnPublishPlanLease) installDurability(ctx backenddb.CommandWALPublishContext) error {
	if l == nil {
		return errors.New("collections: nil prebuilt column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != columnPublishPlanLeaseInstalling {
		return errColumnPublishPlanLeaseNotInstalling
	}
	if l.transferred {
		return errColumnPublishPlanLeaseConsumed
	}
	plan := l.plan
	if plan.durableResourceRequirementsFallback != nil {
		if err := ctx.RegisterDurableLogicalObligationAppendMutation(plan.durableResourceMutation, plan.durableResourceRequirementWork, plan.durableResourceRequirementsFallback); err != nil {
			return fmt.Errorf("collections: register column publish durable append mutation: %w", err)
		}
	} else {
		if err := ctx.RegisterDurableLogicalObligationRequirements(plan.durableResourceRequirements); err != nil {
			return fmt.Errorf("collections: register column publish durable requirements: %w", err)
		}
		if err := ctx.RegisterDurableLogicalObligationMutation(plan.durableResourceMutation); err != nil {
			return fmt.Errorf("collections: register column publish durable mutation: %w", err)
		}
		if err := ctx.RecordDurableLogicalObligationRequirementWork(plan.durableResourceRequirementWork); err != nil {
			return fmt.Errorf("collections: record column publish durable requirement work: %w", err)
		}
	}
	if err := l.transferStableResourcesLocked(ctx); err != nil {
		return fmt.Errorf("collections: register column publish durable resources: %w", err)
	}
	return nil
}

func (l *columnPublishPlanLease) finishCommit() error {
	if l == nil {
		return errors.New("collections: nil column publish plan lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != columnPublishPlanLeaseInstalling || !l.transferred {
		return errColumnPublishPlanLeaseConsumed
	}
	l.state = columnPublishPlanLeaseCommitted
	l.plan.releaseStableResources()
	if l.pendingLease == nil {
		return nil
	}
	err := l.pendingLease.Close()
	l.pendingLease = nil
	return err
}

func (l *columnPublishPlanLease) finishFailure(cause error) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case columnPublishPlanLeasePrepared:
		return l.abandonLocked()
	case columnPublishPlanLeaseInstalling:
		if !l.transferred || (!backenddb.CommitPublicationAccepted(cause) && !errors.Is(cause, backenddb.ErrRecoveryRequired)) {
			return l.abandonLocked()
		}
		return l.quarantineLocked(fmt.Sprintf("ambiguous column publish: %v", cause), columnPublishPlanPreparedRefs(l.plan))
	case columnPublishPlanLeaseQuarantined, columnPublishPlanLeaseAbandoned:
		return nil
	default:
		return errColumnPublishPlanLeaseConsumed
	}
}

func (l *columnPublishPlanLease) Abandon() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == columnPublishPlanLeaseAbandoned {
		return nil
	}
	if l.state != columnPublishPlanLeasePrepared {
		return errColumnPublishPlanLeaseConsumed
	}
	return l.abandonLocked()
}

func (l *columnPublishPlanLease) abandonLocked() error {
	if l.plan.stablePreparedAssets {
		retained, cleanupErr := cleanupUnpublishedStableColumnPublishAssets(
			l.collection.db.ColumnAssetRootDir(),
			l.collection.db.StableResourceIdentityPinRegistry(),
			l.plan.PreparedAssets,
			&l.plan.stableResources,
			l.verifiedStableIdentities,
		)
		if len(retained) != 0 {
			return errors.Join(
				cleanupErr,
				l.quarantineLocked("stable unpublished column assets require reachability cleanup", retained),
				errColumnPublishPlanLeaseCleanupQuarantined,
			)
		}
		l.plan.releaseStableResources()
		l.state = columnPublishPlanLeaseAbandoned
		return errors.Join(cleanupErr, l.closeProtectionLeasesLocked())
	}
	retained, cleanupErr := cleanupUnpublishedColumnPublishAssets(l.collection.db.ColumnAssetRootDir(), l.plan.PreparedAssets)
	if len(retained) != 0 {
		return errors.Join(cleanupErr, l.quarantineLocked("unpublished column publish cleanup was not provably safe", retained), errColumnPublishPlanLeaseCleanupQuarantined)
	}
	l.plan.releaseStableResources()
	l.state = columnPublishPlanLeaseAbandoned
	return errors.Join(cleanupErr, l.closeProtectionLeasesLocked())
}

func (l *columnPublishPlanLease) quarantineLocked(reason string, refs []ColumnAssetRef) error {
	if len(refs) != 0 && l.quarantineLease == nil {
		quarantineLease, err := l.collection.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
			Owner: l.owner(), Source: "column_publish_plan", Reason: reason, Refs: refs,
		})
		if err != nil {
			l.state = columnPublishPlanLeaseQuarantined
			l.plan.releaseStableResources()
			return err
		}
		l.quarantineLease = quarantineLease
	}
	l.state = columnPublishPlanLeaseQuarantined
	l.plan.releaseStableResources()
	return l.closeProtectionLeasesLocked()
}

func (l *columnPublishPlanLease) closeProtectionLeasesLocked() error {
	var errs []error
	if l.preparedLease != nil {
		errs = append(errs, l.preparedLease.Close())
		l.preparedLease = nil
	}
	if l.pendingLease != nil {
		errs = append(errs, l.pendingLease.Close())
		l.pendingLease = nil
	}
	return errors.Join(errs...)
}

func (l *columnPublishPlanLease) stateValue() columnPublishPlanLeaseState {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

func columnPublishPlanPreparedRefs(plan ColumnPublishPlan) []ColumnAssetRef {
	refs := make([]ColumnAssetRef, len(plan.PreparedAssets))
	for i := range plan.PreparedAssets {
		refs[i] = plan.PreparedAssets[i].Ref
	}
	return refs
}

// cleanupUnpublishedColumnPublishAssets reports every ref whose containing
// segment did not end at the proven pre-plan offset after cleanup. Such refs
// must remain protected rather than guessing around a later append.
func cleanupUnpublishedColumnPublishAssets(rootDir string, assets []ColumnPreparedAsset) ([]ColumnAssetRef, error) {
	if len(assets) == 0 {
		return nil, nil
	}
	cleanupErr := cleanupColumnPreparedAssets(rootDir, assets)
	if cleanupErr != nil {
		return columnPublishPlanPreparedRefs(ColumnPublishPlan{PreparedAssets: assets}), cleanupErr
	}
	type target struct {
		truncateTo int64
		refs       []ColumnAssetRef
	}
	targets := make(map[string]*target)
	var inspectErrs []error
	for _, asset := range assets {
		path, err := columnAssetSegmentPath(rootDir, asset.Ref)
		if err != nil {
			inspectErrs = append(inspectErrs, err)
			continue
		}
		entry := targets[path]
		if entry == nil {
			entry = &target{truncateTo: asset.Ref.Offset}
			targets[path] = entry
		}
		if asset.Ref.Offset < entry.truncateTo {
			entry.truncateTo = asset.Ref.Offset
		}
		entry.refs = append(entry.refs, asset.Ref)
	}
	var retained []ColumnAssetRef
	for path, target := range targets {
		info, err := os.Stat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case err != nil:
			inspectErrs = append(inspectErrs, err)
			retained = append(retained, target.refs...)
		case info.Size() != target.truncateTo:
			retained = append(retained, target.refs...)
		}
	}
	return retained, errors.Join(inspectErrs...)
}

type stableColumnAssetCleanupIdentity struct {
	parentIdentity rootpublication.StableIdentity
	childIdentity  rootpublication.StableIdentity
	name           string
}

func captureStableColumnAssetCleanupIdentities(rootDir string, resources *rootpublication.StableResourceSet) (map[string]stableColumnAssetCleanupIdentity, error) {
	if resources == nil {
		return nil, nil
	}
	diagnostics, err := resources.AcquirePhysicalDiagnostics()
	if err != nil {
		return nil, err
	}
	if diagnostics == nil {
		return nil, nil
	}
	defer diagnostics.Close()
	physical := diagnostics.Physical()
	identities := make(map[string]stableColumnAssetCleanupIdentity, len(physical))
	for _, descriptor := range physical {
		lane := descriptor.LogicalLane()
		gen := descriptor.Generation
		if lane == "" || gen == 0 || gen > math.MaxUint32 {
			continue
		}
		fileID := uint32(gen)
		ns, ok := descriptor.Namespace()
		if !ok {
			return nil, fmt.Errorf("%w: stable column asset %q file_id=%d missing namespace identity", rootpublication.ErrUnresolvedResource, lane, fileID)
		}
		expectedName := columnAssetSegmentFileName(fileID)
		if ns.NewName != expectedName {
			return nil, fmt.Errorf("%w: stable column asset name %q does not match %q", rootpublication.ErrResourceConflict, ns.NewName, expectedName)
		}
		childIdentity := descriptor.Identity()
		if !rootpublication.SamePhysicalIdentity(ns.ParentIdentity, ns.ParentIdentity) ||
			!rootpublication.SamePhysicalIdentity(childIdentity, childIdentity) {
			return nil, fmt.Errorf("%w: stable column asset %q file_id=%d missing exact physical identity", rootpublication.ErrUnresolvedResource, lane, fileID)
		}
		assetPath, err := columnAssetSegmentPath(rootDir, ColumnAssetRef{Namespace: lane, FileID: fileID})
		if err != nil {
			return nil, err
		}
		assetPath = filepath.Clean(assetPath)
		candidate := stableColumnAssetCleanupIdentity{
			parentIdentity: ns.ParentIdentity,
			childIdentity:  childIdentity,
			name:           ns.NewName,
		}
		if existing, exists := identities[assetPath]; exists {
			if !rootpublication.SamePhysicalIdentity(existing.parentIdentity, candidate.parentIdentity) ||
				!rootpublication.SamePhysicalIdentity(existing.childIdentity, candidate.childIdentity) ||
				existing.name != candidate.name {
				return nil, fmt.Errorf("%w: conflicting stable identities for %q", rootpublication.ErrResourceConflict, assetPath)
			}
			continue
		}
		identities[assetPath] = candidate
	}
	return identities, nil
}

func extractStableColumnAssetCleanupAssets(resources *rootpublication.StableResourceSet) ([]ColumnPreparedAsset, error) {
	if resources == nil {
		return nil, nil
	}
	var assets []ColumnPreparedAsset
	err := resources.WalkLogicalObligations(func(_ rootpublication.StableResourcePhysicalDescriptor, obligation rootpublication.StableLogicalObligation) error {
		if obligation.Namespace == "" || obligation.FileID == 0 || obligation.FileID > math.MaxUint32 || obligation.Offset < 0 || obligation.Length <= 0 {
			return fmt.Errorf("%w: invalid stable logical obligation %+v", rootpublication.ErrUnresolvedResource, obligation)
		}
		ref := ColumnAssetRef{
			Kind:       ColumnAssetKind(obligation.Kind),
			Namespace:  obligation.Namespace,
			Generation: obligation.Generation,
			PartID:     obligation.PartID,
			FileID:     uint32(obligation.FileID),
			Offset:     obligation.Offset,
			Length:     obligation.Length,
			Checksum:   obligation.Checksum,
		}
		assets = append(assets, ColumnPreparedAsset{
			Ref:          ref,
			Bytes:        ref.Length,
			GenerationID: ref.Generation,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return assets, nil
}

// columnAssetKindMayHavePrefixPadding reports whether columnAssetSegmentPayloadAlignment
// can write deterministic zero padding before an appended asset of this kind.
// Only kinds whose alignment is unconditionally zero can safely prove
// appendStart == ref.Offset on a non-zero shared-segment offset.
func columnAssetKindMayHavePrefixPadding(kind ColumnAssetKind) bool {
	switch kind {
	case ColumnAssetKindTCS1PartImage, ColumnAssetKindTCS1AggregateMetadata:
		return false
	default:
		return true
	}
}

// cleanupUnpublishedStableColumnPublishAssets performs identity-verified
// pre-publish cleanup for stable-authoritative unpublished column assets.
//
// Invariants:
//   - Requires rootpublication.StableRelativeNamespaceSupported(), a non-nil
//     IdentityPinRegistry, and verified (parentIdentity, childIdentity) from a
//     non-nil StableResourceSet (either still held in *stableResources or
//     captured prior to transfer).
//   - Shared-segment tail truncations (truncateTo > 0) run via fd-based
//     OpenStableParent + OpenStableChildFile + SamePhysicalIdentity +
//     ValidateStableChildLink + exact EOF verification BEFORE releasing
//     *stableResources, and refuse to truncate when prefix alignment padding
//     could precede min(ref.Offset).
//   - Fresh whole-segment removals (truncateTo == 0) coordinate with
//     IdentityPinRegistry.BeginDeleteAt, re-verify exact (parentIdentity,
//     childIdentity), exact EOF, and ValidateStableChildLink, unlink via
//     RemoveStableChildFile, forget stable namespace link state, clear
//     directory-sync caches, sync the parent directory, and commit the delete
//     lease.
//   - Any segment failing identity, link, pin, EOF, or pre-padding checks is
//     left untouched and returned in retained so callers quarantine it.
func cleanupUnpublishedStableColumnPublishAssets(
	rootDir string,
	registry *rootpublication.IdentityPinRegistry,
	assets []ColumnPreparedAsset,
	stableResources **rootpublication.StableResourceSet,
	verifiedIdentities map[string]stableColumnAssetCleanupIdentity,
) ([]ColumnAssetRef, error) {
	releaseStable := func() {
		if stableResources != nil && *stableResources != nil {
			(*stableResources).Release()
			*stableResources = nil
		}
	}
	if len(assets) == 0 {
		releaseStable()
		return nil, nil
	}
	allRefs := columnPublishPlanPreparedRefs(ColumnPublishPlan{PreparedAssets: assets})
	if !rootpublication.StableRelativeNamespaceSupported() || registry == nil {
		releaseStable()
		return allRefs, nil
	}

	var identities map[string]stableColumnAssetCleanupIdentity
	if stableResources != nil && *stableResources != nil {
		var err error
		identities, err = captureStableColumnAssetCleanupIdentities(rootDir, *stableResources)
		if err != nil {
			releaseStable()
			return allRefs, err
		}
	} else if len(verifiedIdentities) != 0 {
		identities = verifiedIdentities
	} else {
		// Fail closed: never touch disk for stablePreparedAssets without
		// verified physical identities captured from a non-nil StableResourceSet.
		return allRefs, nil
	}

	type cleanupTarget struct {
		path          string
		segmentDir    string
		name          string
		truncateTo    int64
		minOffsetKind ColumnAssetKind
		maxEnd        int64
		identity      stableColumnAssetCleanupIdentity
		ineligible    bool
		refs          []ColumnAssetRef
	}

	targets := make(map[string]*cleanupTarget)
	var cleanupErrs []error
	var retained []ColumnAssetRef
	for _, asset := range assets {
		ref := asset.Ref
		if ref.Namespace == "" || ref.FileID == 0 || ref.Offset < 0 || ref.Length <= 0 || ref.Offset > math.MaxInt64-ref.Length {
			retained = append(retained, ref)
			continue
		}
		assetPath, err := columnAssetSegmentPath(rootDir, ref)
		if err != nil {
			cleanupErrs = append(cleanupErrs, err)
			retained = append(retained, ref)
			continue
		}
		assetPath = filepath.Clean(assetPath)
		target := targets[assetPath]
		if target == nil {
			expectedName := columnAssetSegmentFileName(ref.FileID)
			verified, ok := identities[assetPath]
			ineligible := !ok || verified.name != expectedName ||
				!rootpublication.SamePhysicalIdentity(verified.parentIdentity, verified.parentIdentity) ||
				!rootpublication.SamePhysicalIdentity(verified.childIdentity, verified.childIdentity)
			target = &cleanupTarget{
				path:          assetPath,
				segmentDir:    filepath.Dir(assetPath),
				name:          expectedName,
				truncateTo:    ref.Offset,
				minOffsetKind: ref.Kind,
				maxEnd:        ref.Offset + ref.Length,
				identity:      verified,
				ineligible:    ineligible,
			}
			targets[assetPath] = target
		}
		if ref.Offset < target.truncateTo {
			target.truncateTo = ref.Offset
			target.minOffsetKind = ref.Kind
		} else if ref.Offset == target.truncateTo && columnAssetKindMayHavePrefixPadding(ref.Kind) {
			target.minOffsetKind = ref.Kind
		}
		if end := ref.Offset + ref.Length; end > target.maxEnd {
			target.maxEnd = end
		}
		target.refs = append(target.refs, ref)
	}

	paths := make([]string, 0, len(targets))
	for path := range targets {
		paths = append(paths, path)
	}
	slices.Sort(paths)

	var wholeTargets []*cleanupTarget
	for _, path := range paths {
		target := targets[path]
		if target.ineligible {
			retained = append(retained, target.refs...)
			continue
		}
		if target.truncateTo == 0 {
			wholeTargets = append(wholeTargets, target)
			continue
		}
		if columnAssetKindMayHavePrefixPadding(target.minOffsetKind) {
			// Cannot prove exact pre-padding appendStart from ref.Offset alone.
			retained = append(retained, target.refs...)
			continue
		}
		// Shared-segment tail truncation runs while *stableResources pins are
		// still held so GC cannot unlink or retire the segment concurrently.
		keep, err := truncateStableUnpublishedColumnSegmentTail(
			target.path,
			target.segmentDir,
			target.name,
			target.identity,
			target.truncateTo,
			target.maxEnd,
			registry,
		)
		if err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}
		if keep {
			retained = append(retained, target.refs...)
		}
	}

	// Release our own StableResourceSet pins before BeginDeleteAt on fresh
	// whole segments so only external pins can block deletion. Every whole
	// target still re-verifies exact (parentIdentity, childIdentity), exact
	// EOF, ValidateStableChildLink, and BeginDeleteAt under the segment write
	// lock before unlinking.
	releaseStable()

	for _, target := range wholeTargets {
		keep, err := removeStableUnpublishedWholeColumnSegment(
			target.path,
			target.segmentDir,
			target.name,
			target.identity,
			target.maxEnd,
			registry,
		)
		if err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}
		if keep {
			retained = append(retained, target.refs...)
		}
	}

	return retained, errors.Join(cleanupErrs...)
}

func truncateStableUnpublishedColumnSegmentTail(
	assetPath, segmentDir, name string,
	expected stableColumnAssetCleanupIdentity,
	truncateTo, expectedEnd int64,
	registry *rootpublication.IdentityPinRegistry,
) (retain bool, retErr error) {
	lock := columnAssetSegmentWriteLock(assetPath)
	lock.Lock()
	defer lock.Unlock()

	if registry.IsRetired(expected.childIdentity) {
		return true, nil
	}
	parent, err := rootpublication.OpenStableParent(segmentDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return true, err
	}
	defer func() { retErr = errors.Join(retErr, parent.Close()) }()

	parentID, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return true, err
	}
	if !rootpublication.SamePhysicalIdentity(parentID, expected.parentIdentity) {
		return true, nil
	}
	child, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return true, err
	}
	defer func() { retErr = errors.Join(retErr, child.Close()) }()

	childID, err := rootpublication.StableIdentityFromFile(child)
	if err != nil {
		return true, err
	}
	if !rootpublication.SamePhysicalIdentity(childID, expected.childIdentity) {
		return true, nil
	}
	if err := rootpublication.ValidateStableChildLink(parent, child, name); err != nil {
		return true, nil
	}
	info, err := child.Stat()
	if err != nil {
		return true, err
	}
	if !info.Mode().IsRegular() || info.Size() != expectedEnd {
		return true, nil
	}
	if err := child.Truncate(truncateTo); err != nil {
		return true, err
	}
	if err := rootpublication.SyncStableFile(child); err != nil {
		return true, err
	}
	if err := rootpublication.SyncStableNamespace(parent); err != nil {
		return true, err
	}
	return false, nil
}

func removeStableUnpublishedWholeColumnSegment(
	assetPath, segmentDir, name string,
	expected stableColumnAssetCleanupIdentity,
	expectedEnd int64,
	registry *rootpublication.IdentityPinRegistry,
) (retain bool, retErr error) {
	lock := columnAssetSegmentWriteLock(assetPath)
	lock.Lock()
	defer lock.Unlock()

	if registry.IsRetired(expected.childIdentity) {
		return false, nil
	}
	parent, err := rootpublication.OpenStableParent(segmentDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return true, err
	}
	defer func() { retErr = errors.Join(retErr, parent.Close()) }()

	parentID, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return true, err
	}
	if !rootpublication.SamePhysicalIdentity(parentID, expected.parentIdentity) {
		return true, nil
	}
	child, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return true, err
	}
	defer func() { retErr = errors.Join(retErr, child.Close()) }()

	childID, err := rootpublication.StableIdentityFromFile(child)
	if err != nil {
		return true, err
	}
	if !rootpublication.SamePhysicalIdentity(childID, expected.childIdentity) {
		return true, nil
	}
	namespace := filepath.ToSlash(filepath.Join(filepath.Clean(segmentDir), name))
	deleteLease, err := registry.BeginDeleteAt(childID, namespace)
	if err != nil {
		if errors.Is(err, rootpublication.ErrResourcePinned) {
			return true, nil
		}
		if errors.Is(err, rootpublication.ErrResourceConflict) && registry.IsRetired(childID) {
			return false, nil
		}
		return true, err
	}
	committed := false
	defer func() {
		if !committed {
			deleteLease.Abort()
		}
	}()

	info, err := child.Stat()
	if err != nil {
		return true, err
	}
	if !info.Mode().IsRegular() || info.Size() != expectedEnd {
		return true, nil
	}
	if err := rootpublication.ValidateStableChildLink(parent, child, name); err != nil {
		return true, nil
	}
	if err := rootpublication.RemoveStableChildFile(parent, name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, err
	}
	clearColumnAssetSegmentDirSyncKnown(assetPath)
	forgetErr := registry.ForgetStableNamespaceLinkIdentity(expected.parentIdentity, expected.childIdentity, name)
	syncErr := rootpublication.SyncStableNamespace(parent)
	deleteLease.Commit()
	committed = true
	return false, errors.Join(forgetErr, syncErr)
}
