package collections

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// ColumnAssetGCOptions controls safe M15B column asset segment reclamation.
type ColumnAssetGCOptions struct {
	DryRun bool
	// Detailed keeps detailed ref and segment entries in the returned plan.
	Detailed bool
	// SegmentDetails keeps segment-level entries in the returned plan without
	// retaining per-ref entries.
	SegmentDetails                   bool
	CandidateRefs                    []ColumnAssetRef
	PendingRefs                      []ColumnAssetRef
	PreparedRefs                     []ColumnAssetRef
	PreparedQueryRefs                []ColumnAssetRef
	QuarantineRefs                   []ColumnAssetRef
	QuarantineSegments               []ColumnAssetQuarantineSegment
	PinnedRefs                       []ColumnAssetRef
	releaseVectorPartitionReclaimIDs map[string]struct{}
}

// ColumnAssetGCStats summarizes safe whole-segment column asset reclamation.
type ColumnAssetGCStats struct {
	DryRun bool

	Plan ColumnAssetReachabilityPlan

	SegmentsEligible int
	SegmentsDeleted  int
	SegmentsRetained int
	BytesEligible    int64
	BytesDeleted     int64
	BytesRetained    int64
}

// ErrColumnAssetGCPlanStale reports that a parent directory, candidate file,
// exact byte frontier, or committed-root witness no longer matches GC
// planning. Callers may retry from a fresh reachability plan; changed storage
// is never touched.
var ErrColumnAssetGCPlanStale = errors.New("collections: column asset GC plan identity changed")

var (
	columnAssetGCTestHookMu             sync.RWMutex
	removeColumnAssetGCSegment          func(string) error
	syncColumnAssetGCDeletedSegmentsDir func(string) error
	columnAssetStableDeleteBeforeUnlink func()
	columnAssetStableDeleteAfterPlan    func()
)

type columnAssetGCPlannedSegment struct {
	entry          ColumnAssetReachabilitySegmentEntry
	parentIdentity rootpublication.StableIdentity
	childIdentity  rootpublication.StableIdentity
}

type columnAssetStableSegmentDeleter struct {
	segmentDir     string
	parent         *os.File
	registry       *rootpublication.IdentityPinRegistry
	parentIdentity rootpublication.StableIdentity
	leases         []*rootpublication.IdentityDeleteLease
	removed        bool
}

func newColumnAssetStableSegmentDeleter(segmentDir string, registry *rootpublication.IdentityPinRegistry) (*columnAssetStableSegmentDeleter, error) {
	if registry == nil {
		return nil, errors.New("collections: stable column segment delete requires identity pin registry")
	}
	parent, err := os.Open(segmentDir)
	if err != nil {
		return nil, err
	}
	parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	return &columnAssetStableSegmentDeleter{segmentDir: segmentDir, parent: parent, registry: registry, parentIdentity: parentIdentity}, nil
}

func (deleter *columnAssetStableSegmentDeleter) delete(planned columnAssetGCPlannedSegment, validatePlanCurrent func() error) (bool, error) {
	fileID := planned.entry.FileID
	if deleter == nil || deleter.parent == nil || fileID == 0 {
		return false, errors.New("collections: incomplete stable column segment delete")
	}
	if !rootpublication.SamePhysicalIdentity(deleter.parentIdentity, planned.parentIdentity) {
		return false, fmt.Errorf("%w: parent directory rebound for file_id=%d", ErrColumnAssetGCPlanStale, fileID)
	}
	name := columnAssetSegmentFileName(fileID)
	resource, err := rootpublication.OpenStableChildFile(deleter.parent, name, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer resource.Close()
	identity, err := rootpublication.StableIdentityFromFile(resource)
	if err != nil {
		return false, err
	}
	if !rootpublication.SamePhysicalIdentity(identity, planned.childIdentity) {
		return false, fmt.Errorf("%w: candidate rebound for file_id=%d", ErrColumnAssetGCPlanStale, fileID)
	}
	// Detect publications that completed after planning before consulting the
	// pin registry. Such a publication may itself pin this identity; returning a
	// generic retained result would hide that the destructive plan is stale.
	if validatePlanCurrent != nil {
		if err := validatePlanCurrent(); err != nil {
			return false, err
		}
	}
	namespace := filepath.ToSlash(filepath.Join(filepath.Clean(deleter.segmentDir), name))
	lease, err := deleter.registry.BeginDeleteAt(identity, namespace)
	if err != nil {
		return false, err
	}
	abort := true
	defer func() {
		if abort {
			lease.Abort()
		}
	}()
	removeHook, _, beforeUnlink := columnAssetStableDeleteHooks()
	if beforeUnlink != nil {
		beforeUnlink()
	}
	info, err := resource.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() != planned.entry.Bytes {
		return false, fmt.Errorf("%w: candidate frontier changed for file_id=%d planned_bytes=%d current_bytes=%d",
			ErrColumnAssetGCPlanStale, fileID, planned.entry.Bytes, info.Size())
	}
	if err := rootpublication.ValidateStableChildLink(deleter.parent, resource, name); err != nil {
		return false, err
	}
	if validatePlanCurrent != nil {
		if err := validatePlanCurrent(); err != nil {
			return false, err
		}
	}
	if removeHook != nil {
		err = removeHook(filepath.Join(deleter.segmentDir, name))
	} else {
		err = rootpublication.RemoveStableChildFile(deleter.parent, name)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := deleter.registry.ForgetStableNamespaceLink(deleter.parent, resource, name); err != nil {
		return false, err
	}
	abort = false
	deleter.leases = append(deleter.leases, lease)
	deleter.removed = true
	return true, nil
}

func (deleter *columnAssetStableSegmentDeleter) finish(cause error) error {
	if deleter == nil {
		return cause
	}
	var syncErr error
	if deleter.removed {
		_, syncHook, _ := columnAssetStableDeleteHooks()
		if syncHook != nil {
			syncErr = syncHook(deleter.segmentDir)
		} else if deleter.parent != nil {
			syncErr = deleter.parent.Sync()
		}
	}
	for _, lease := range deleter.leases {
		lease.Commit()
	}
	deleter.leases = nil
	var closeErr error
	if deleter.parent != nil {
		closeErr = deleter.parent.Close()
		deleter.parent = nil
	}
	return errors.Join(cause, syncErr, closeErr)
}

func deleteColumnAssetSegmentWithStableLease(segmentDir string, fileID uint32, registry *rootpublication.IdentityPinRegistry) (bool, error) {
	planned, found, err := planColumnAssetStableSegmentDelete(segmentDir, fileID)
	if err != nil || !found {
		return false, err
	}
	deleter, err := newColumnAssetStableSegmentDeleter(segmentDir, registry)
	if err != nil {
		return false, err
	}
	deleted, err := deleter.delete(planned, nil)
	return deleted, deleter.finish(err)
}

func planColumnAssetStableSegmentDelete(segmentDir string, fileID uint32) (columnAssetGCPlannedSegment, bool, error) {
	var planned columnAssetGCPlannedSegment
	parent, err := os.Open(segmentDir)
	if err != nil {
		return planned, false, err
	}
	defer parent.Close()
	planned.parentIdentity, err = rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return planned, false, err
	}
	resource, err := rootpublication.OpenStableChildFile(parent, columnAssetSegmentFileName(fileID), os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return planned, false, nil
	}
	if err != nil {
		return planned, false, err
	}
	defer resource.Close()
	planned.childIdentity, err = rootpublication.StableIdentityFromFile(resource)
	if err != nil {
		return planned, false, err
	}
	info, err := resource.Stat()
	if err != nil {
		return planned, false, err
	}
	planned.entry.FileID = fileID
	planned.entry.Bytes = info.Size()
	return planned, true, nil
}

func columnAssetStableDeleteHooks() (func(string) error, func(string) error, func()) {
	columnAssetGCTestHookMu.RLock()
	defer columnAssetGCTestHookMu.RUnlock()
	return removeColumnAssetGCSegment, syncColumnAssetGCDeletedSegmentsDir, columnAssetStableDeleteBeforeUnlink
}

func setColumnAssetStableDeleteBeforeUnlinkTestHook(hook func()) func() {
	columnAssetGCTestHookMu.Lock()
	previous := columnAssetStableDeleteBeforeUnlink
	columnAssetStableDeleteBeforeUnlink = hook
	columnAssetGCTestHookMu.Unlock()
	return func() {
		columnAssetGCTestHookMu.Lock()
		columnAssetStableDeleteBeforeUnlink = previous
		columnAssetGCTestHookMu.Unlock()
	}
}

func columnAssetStableDeleteAfterPlanHook() func() {
	columnAssetGCTestHookMu.RLock()
	defer columnAssetGCTestHookMu.RUnlock()
	return columnAssetStableDeleteAfterPlan
}

func setColumnAssetStableDeleteAfterPlanTestHook(hook func()) func() {
	columnAssetGCTestHookMu.Lock()
	previous := columnAssetStableDeleteAfterPlan
	columnAssetStableDeleteAfterPlan = hook
	columnAssetGCTestHookMu.Unlock()
	return func() {
		columnAssetGCTestHookMu.Lock()
		columnAssetStableDeleteAfterPlan = previous
		columnAssetGCTestHookMu.Unlock()
	}
}

// ColumnAssetGC reclaims only complete, canonical column asset segments that
// M15A reachability proves wholly reclaimable. Mixed segments remain rewrite
// debt for M15C; incomplete reachability fails closed before deletion.
func (c *Collection) ColumnAssetGC(ctx context.Context, opts ColumnAssetGCOptions) (ColumnAssetGCStats, error) {
	var stats ColumnAssetGCStats
	if c == nil {
		return stats, errCollectionNil
	}
	if c.db == nil {
		return stats, errCollectionDBNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if !opts.DryRun {
		if err := c.db.CheckStorageMaintenanceReady(); err != nil {
			return stats, err
		}
		// Destructive GC and durable vector-partition publication both protect
		// column assets. Keep their shared order storage barrier -> collection
		// mutation, including planning and recoverable-root revalidation. Taking
		// only the mutation lock here would let publication reserve the storage
		// barrier and then advance root-publication state between the GC plan and
		// its delete witness.
		var mutationErr error
		mutationErr = c.withVectorPartitionStorageMutationV1("column_asset_gc", func() error {
			stats, mutationErr = c.columnAssetGC(ctx, opts)
			return mutationErr
		})
		return stats, mutationErr
	}
	return c.columnAssetGC(ctx, opts)
}

func (c *Collection) columnAssetGC(ctx context.Context, opts ColumnAssetGCOptions) (stats ColumnAssetGCStats, err error) {
	defer func() {
		stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed, opts.SegmentDetails)
	}()
	needSegmentEntries := !opts.DryRun || opts.Detailed || opts.SegmentDetails
	planOpts, err := c.columnAssetLifecycleAugmentReachabilityOptions(ColumnAssetReachabilityOptions{
		Detailed:                              opts.Detailed,
		SegmentDetails:                        needSegmentEntries,
		ProtectCandidateRefsForOlderSnapshots: true,
		CandidateRefs:                         opts.CandidateRefs,
		PendingRefs:                           opts.PendingRefs,
		PreparedRefs:                          opts.PreparedRefs,
		PreparedQueryRefs:                     opts.PreparedQueryRefs,
		QuarantineRefs:                        opts.QuarantineRefs,
		QuarantineSegments:                    opts.QuarantineSegments,
		PinnedRefs:                            opts.PinnedRefs,
		releaseVectorPartitionReclaimIDs:      opts.releaseVectorPartitionReclaimIDs,
	})
	if err != nil {
		return ColumnAssetGCStats{}, err
	}
	plan, _, err := c.planColumnAssetReachability(ctx, columnAssetReachabilityOptionsInternal{
		ColumnAssetReachabilityOptions: planOpts,
	})
	stats = ColumnAssetGCStats{
		DryRun:           opts.DryRun,
		Plan:             plan,
		SegmentsRetained: columnAssetGCExistingSegmentCount(plan),
		BytesRetained:    plan.Segments.BytesTotal,
	}
	if err != nil {
		return stats, c.columnAssetGCNormalizeMaintenanceRaceError(err)
	}
	if !plan.Complete {
		if opts.DryRun {
			return stats, nil
		}
		return stats, fmt.Errorf("%w: collection=%q namespace=%q uncertain_refs=%d unknown_segments=%d missing_segments=%d out_of_bounds_refs=%d quarantine_segment_mismatches=%d unconvertible_pins=%d",
			ErrColumnAssetReachabilityIncomplete,
			plan.Collection,
			plan.Namespace,
			plan.Refs.Uncertain,
			plan.Segments.Unknown,
			plan.Segments.Missing,
			plan.Segments.OutOfBoundsRefs,
			plan.Segments.QuarantineSegmentMismatches,
			plan.MappedResources.UnconvertiblePins,
		)
	}
	if opts.DryRun && !needSegmentEntries {
		stats.SegmentsEligible = plan.Segments.Reclaimable
		stats.BytesEligible = plan.Segments.BytesWholeReclaimable
		return stats, nil
	}

	if opts.DryRun {
		if len(plan.SegmentEntries) == 0 {
			// Non-detailed dry-runs keep the allocation profile bounded by using
			// the canonical segment summary instead of materializing entries.
			stats.SegmentsEligible = plan.Segments.Reclaimable
			stats.BytesEligible = plan.Segments.BytesWholeReclaimable
			return stats, nil
		}
		namespace, err := columnAssetManagerNamespaceForRoot(c.db.ColumnAssetRootDir(), plan.Namespace)
		if err != nil {
			return stats, err
		}
		for _, entry := range plan.SegmentEntries {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			if !columnAssetGCSegmentEligibleForDelete(namespace.SegmentDir, entry) {
				continue
			}
			stats.SegmentsEligible++
			stats.BytesEligible += entry.Bytes
		}
		return stats, nil
	}

	namespace, err := columnAssetManagerNamespaceForRoot(c.db.ColumnAssetRootDir(), plan.Namespace)
	if err != nil {
		return stats, err
	}
	eligible := make([]columnAssetGCPlannedSegment, 0, plan.Segments.Reclaimable)
	exactDeleteSupported := rootpublication.StableRelativeNamespaceSupported()
	legacyEligible := 0
	for _, entry := range plan.SegmentEntries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !columnAssetGCSegmentEligibleForDelete(namespace.SegmentDir, entry) {
			continue
		}
		if exactDeleteSupported {
			if !rootpublication.SamePhysicalIdentity(entry.plannedParentIdentity, entry.plannedParentIdentity) ||
				!rootpublication.SamePhysicalIdentity(entry.plannedChildIdentity, entry.plannedChildIdentity) {
				continue
			}
		} else {
			legacyEligible++
		}
		stats.SegmentsEligible++
		stats.BytesEligible = addColumnAssetReachabilityBytes(stats.BytesEligible, entry.ReclaimableBytes)
		if exactDeleteSupported {
			eligible = append(eligible, columnAssetGCPlannedSegment{
				entry: entry, parentIdentity: entry.plannedParentIdentity, childIdentity: entry.plannedChildIdentity,
			})
		}
	}
	// Re-check after planning while still under the mutation lock so destructive
	// deletion cannot proceed if the handle became closed, read-only, or
	// command-WAL-poisoned while reachability was being computed.
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	if !exactDeleteSupported && legacyEligible != 0 {
		return stats, fmt.Errorf("%w: destructive column asset GC requires exact relative namespace authority", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if len(eligible) == 0 {
		return stats, nil
	}
	recoverableRoots, err := c.db.CaptureRecoverableRootSet(ctx)
	if err != nil {
		return stats, err
	}
	defer recoverableRoots.Release()
	if err := c.pinRecoverableColumnAssetSegments(ctx, recoverableRoots, opts.CandidateRefs); err != nil {
		return stats, err
	}
	if hook := columnAssetStableDeleteAfterPlanHook(); hook != nil {
		hook()
	}
	deleter, err := newColumnAssetStableSegmentDeleter(namespace.SegmentDir, c.db.StableResourceIdentityPinRegistry())
	if err != nil {
		return stats, err
	}

	for _, planned := range eligible {
		entry := planned.entry
		if err := ctx.Err(); err != nil {
			return stats, deleter.finish(err)
		}
		deleted, err := deleter.delete(planned, func() error {
			if err := recoverableRoots.Revalidate(); err != nil {
				return errors.Join(ErrColumnAssetGCPlanStale, err)
			}
			currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
			if currentCommitSeq != plan.PlanCommitSeq || currentSystemRoot != plan.SystemRoot {
				return fmt.Errorf("%w: committed root changed for collection=%q planned_commit_seq=%d current_commit_seq=%d planned_system_root=%d current_system_root=%d",
					ErrColumnAssetGCPlanStale,
					plan.Collection,
					plan.PlanCommitSeq,
					currentCommitSeq,
					plan.SystemRoot,
					currentSystemRoot,
				)
			}
			return nil
		})
		if err != nil {
			// A publication owner can pin an otherwise unreachable segment
			// between planning and deletion. Retain it for a later GC pass.
			if errors.Is(err, rootpublication.ErrResourcePinned) {
				continue
			}
			return stats, deleter.finish(err)
		}
		if !deleted {
			continue
		}
		stats.SegmentsDeleted++
		stats.BytesDeleted = addColumnAssetReachabilityBytes(stats.BytesDeleted, entry.ReclaimableBytes)
		stats.SegmentsRetained--
		stats.BytesRetained = subColumnAssetReachabilityBytesFloor(stats.BytesRetained, entry.ReclaimableBytes)
	}
	if err := deleter.finish(nil); err != nil {
		return stats, err
	}
	return stats, nil
}

type recoverableColumnAssetReplayBasis struct {
	appliedCommandLSN  uint64
	manifestGeneration uint64
	collection         string
	config             ColumnStoreConfig
}

type recoverableColumnAssetReplayClassifier func(ColumnAssetRef, recoverableColumnAssetReplayBasis) (bool, error)

func recoverableColumnAssetReplayRefs(
	candidateRefs []ColumnAssetRef,
	bases []recoverableColumnAssetReplayBasis,
	requiresReplayBefore func(uint64) bool,
	classify recoverableColumnAssetReplayClassifier,
) ([]ColumnAssetRef, error) {
	if len(candidateRefs) == 0 || len(bases) == 0 || requiresReplayBefore == nil || classify == nil {
		return nil, nil
	}
	replayRefs := make([]ColumnAssetRef, 0, len(candidateRefs))
	for _, ref := range candidateRefs {
		if ref.Generation == 0 {
			continue
		}
		for _, basis := range bases {
			if basis.appliedCommandLSN == 0 || basis.manifestGeneration == 0 ||
				ref.Generation > basis.manifestGeneration || !requiresReplayBefore(basis.appliedCommandLSN) {
				continue
			}
			replayable, err := classify(ref, basis)
			if err != nil {
				return nil, err
			}
			if replayable {
				replayRefs = append(replayRefs, ref)
				break
			}
		}
	}
	return replayRefs, nil
}

func (c *Collection) pinRecoverableColumnAssetSegments(ctx context.Context, roots *backenddb.RecoverableRootSet, candidateRefs []ColumnAssetRef) error {
	if c == nil || c.db == nil || roots == nil {
		return backenddb.ErrRecoverableRootSetStale
	}
	seenPaths := make(map[string]struct{})
	visibleReplayBases := make([]recoverableColumnAssetReplayBasis, 0, 2)
	pinRefs := func(refs []ColumnAssetRef) error {
		for _, ref := range refs {
			path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), ref)
			if err != nil {
				return err
			}
			path = filepath.Clean(path)
			if _, ok := seenPaths[path]; ok {
				continue
			}
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("collections: open recoverable column asset %q: %w", path, err)
			}
			pinErr := roots.PinStableFile(file)
			closeErr := file.Close()
			if pinErr != nil {
				return fmt.Errorf("collections: pin recoverable column asset %q: %w", path, pinErr)
			}
			if closeErr != nil {
				return closeErr
			}
			seenPaths[path] = struct{}{}
		}
		return nil
	}
	for _, root := range roots.Roots() {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot := roots.AcquireSnapshotForRoot(root)
		if snapshot == nil {
			return backenddb.ErrRecoverableRootSetStale
		}
		// Recoverable roots may intentionally be older than the collection's
		// current catalog. Loading one is an audit operation: it must not replace
		// the handle-local latest catalog cache with historical state.
		catalog, err := loadCollectionCatalog(snapshot, c.collectionName())
		if err != nil {
			_ = snapshot.Close()
			if errors.Is(err, errCollectionNotFound) {
				continue
			}
			return fmt.Errorf("collections: capture recoverable column catalog at commit_seq=%d: %w", root.CommitSeq, err)
		}
		if catalog == nil {
			_ = snapshot.Close()
			continue
		}
		cfgPtr := catalog.meta.Options.ColumnStore
		if cfgPtr == nil || !cfgPtr.Enabled {
			_ = snapshot.Close()
			continue
		}
		cfg := *cfgPtr
		if cfg.ActiveManifest == nil {
			if cfg.RecoveryAuthoritativeManifest != nil {
				_ = snapshot.Close()
				return fmt.Errorf("collections: recoverable column catalog at commit_seq=%d has recovery manifest without active manifest", root.CommitSeq)
			}
			_ = snapshot.Close()
			continue
		}
		rootName := catalog.columnManifestRootName
		if rootName == "" && cfg.ManifestRoot != nil {
			rootName = cfg.ManifestRoot.Name
		}
		if rootName == "" {
			rootName = collectionColumnManifestRootName(catalog.meta.Name)
		}
		view, viewErr := c.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(
			snapshot,
			catalog,
			catalog.meta.Name,
			catalog.rootID(rootName),
			cfg,
			true,
			columnManifestScanAllSidecars(),
		)
		closeErr := snapshot.Close()
		if viewErr != nil {
			return fmt.Errorf("collections: capture recoverable column assets at commit_seq=%d: %w", root.CommitSeq, viewErr)
		}
		if closeErr != nil {
			return closeErr
		}
		if root.Visible {
			if identity := view.FullConfig.RecoveryAuthoritativeManifest; identity != nil &&
				identity.Generation != 0 && view.FullConfig.RecoveryAuthoritativeAppliedCommandLSN != 0 {
				visibleReplayBases = append(visibleReplayBases, recoverableColumnAssetReplayBasis{
					appliedCommandLSN:  view.FullConfig.RecoveryAuthoritativeAppliedCommandLSN,
					manifestGeneration: identity.Generation,
					collection:         catalog.meta.Name,
					config:             view.FullConfig,
				})
			}
		}
		if err := pinRefs(columnPhysicalScanSnapshotViewAssetRefs(view)); err != nil {
			return err
		}
	}
	// A durable fallback behind any visible command-WAL frontier can replay
	// authoritative generations that the corresponding visible manifest has
	// since superseded. Preserve each root's LSN, generation, collection, and
	// configuration as one basis so candidate parsing never mixes authorities.
	replayRefs, err := recoverableColumnAssetReplayRefs(
		candidateRefs,
		visibleReplayBases,
		roots.RequiresReplayBefore,
		func(ref ColumnAssetRef, basis recoverableColumnAssetReplayBasis) (bool, error) {
			return recoverableColumnAssetReplayCandidate(
				c.db.ColumnAssetRootDir(), ref, basis.collection, basis.config, basis.appliedCommandLSN,
			)
		},
	)
	if err != nil {
		return err
	}
	if err := pinRefs(replayRefs); err != nil {
		return err
	}
	return roots.Revalidate()
}

func recoverableColumnAssetReplayCandidate(rootDir string, ref ColumnAssetRef, collection string, cfg ColumnStoreConfig, visibleRecoveryLSN uint64) (bool, error) {
	if visibleRecoveryLSN == 0 {
		return false, nil
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, ref)
	if err != nil {
		// An unreadable candidate is ambiguous until exact namespace/identity
		// validation immediately before deletion. Fail closed so a transient I/O
		// error cannot silently drop a required replay input.
		return false, fmt.Errorf("collections: read recoverable column asset kind=%q namespace=%q file_id=%d: %w", ref.Kind, ref.Namespace, ref.FileID, err)
	}
	switch ref.Kind {
	case ColumnAssetKindTCS1PartImage:
		header, _, _, err := parseColumnPhysicalAssetScanHeader(raw, ref, collection, &cfg, "")
		if err != nil {
			return false, nil
		}
		return header.AppliedCommandLSN != 0 && header.AppliedCommandLSN <= visibleRecoveryLSN, nil
	case ColumnAssetKindTCS1TypedColumnPart:
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			return false, nil
		}
		return image.PartID == ref.PartID, nil
	default:
		// Aggregate metadata, dictionary/int64 sidecars, HNSW packs, and
		// query-ready assets are rebuildable derived accelerators.
		return false, nil
	}
}

func columnAssetGCExistingSegmentCount(plan ColumnAssetReachabilityPlan) int {
	segments := plan.Segments.Total - plan.Segments.Missing
	if segments < 0 {
		return 0
	}
	return segments
}

func columnAssetGCSegmentEligibleForDelete(segmentDir string, entry ColumnAssetReachabilitySegmentEntry) bool {
	if entry.Status != ColumnAssetReachabilitySegmentReclaimable ||
		entry.FileID == 0 ||
		entry.Path == "" ||
		entry.Bytes <= 0 ||
		entry.ProtectedBytes != 0 ||
		entry.UnknownBytes != 0 ||
		entry.ReclaimableBytes != entry.Bytes {
		return false
	}
	expected := columnAssetReachabilitySegmentPath(segmentDir, columnAssetSegmentFileName(entry.FileID))
	if entry.Path != expected {
		return false
	}
	return true
}

func (c *Collection) columnAssetGCNormalizeMaintenanceRaceError(err error) error {
	if err == nil || c == nil || c.db == nil {
		return err
	}
	maintenanceErr := c.db.CheckStorageMaintenanceReady()
	if maintenanceErr != nil {
		return errors.Join(err, maintenanceErr)
	}
	return err
}

func columnAssetGCPlanForDetail(plan ColumnAssetReachabilityPlan, detailed, segmentDetails bool) ColumnAssetReachabilityPlan {
	if detailed {
		return plan
	}
	plan.Entries = nil
	if !segmentDetails {
		plan.SegmentEntries = nil
	}
	return plan
}

func setColumnAssetGCTestHooks(remove func(string) error, syncDeletedDir func(string) error) func() {
	columnAssetGCTestHookMu.Lock()
	prevRemove := removeColumnAssetGCSegment
	prevSync := syncColumnAssetGCDeletedSegmentsDir
	if remove != nil {
		removeColumnAssetGCSegment = remove
	}
	if syncDeletedDir != nil {
		syncColumnAssetGCDeletedSegmentsDir = syncDeletedDir
	}
	columnAssetGCTestHookMu.Unlock()
	return func() {
		columnAssetGCTestHookMu.Lock()
		removeColumnAssetGCSegment = prevRemove
		syncColumnAssetGCDeletedSegmentsDir = prevSync
		columnAssetGCTestHookMu.Unlock()
	}
}

func subColumnAssetReachabilityBytesFloor(total, delta int64) int64 {
	if delta <= 0 {
		return total
	}
	if total <= delta {
		return 0
	}
	return total - delta
}
