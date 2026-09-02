package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

var errColumnAssetRewritePublishPreflightFailed = errors.New("collections: column asset rewrite publish preflight failed")

// ColumnAssetRewriteOptions controls M15C mixed-segment rewrite/remap.
type ColumnAssetRewriteOptions struct {
	DryRun bool
	// Detailed keeps detailed ref and segment entries in the returned plan.
	Detailed           bool
	CandidateRefs      []ColumnAssetRef
	PendingRefs        []ColumnAssetRef
	PreparedRefs       []ColumnAssetRef
	PreparedQueryRefs  []ColumnAssetRef
	QuarantineRefs     []ColumnAssetRef
	QuarantineSegments []ColumnAssetQuarantineSegment
	PinnedRefs         []ColumnAssetRef
}

type columnAssetRewriteOptions struct {
	ColumnAssetRewriteOptions
	afterCopyHookForTest       func() error
	afterPrePublishHookForTest func() error
	// beforeRemapPublish receives the complete old-ref set after copying and
	// preflight, while an error can still guarantee that no remap is published.
	beforeRemapPublish               func([]ColumnAssetRef) error
	releaseVectorPartitionReclaimIDs map[string]struct{}
}

// ColumnAssetRewriteStats summarizes mixed-segment rewrite/remap.
type ColumnAssetRewriteStats struct {
	DryRun bool

	Plan ColumnAssetReachabilityPlan

	SegmentsEligible  int
	SegmentsRewritten int
	SegmentsRetained  int
	RefsEligible      int
	RefsRemapped      int
	RefsRetained      int
	BytesEligible     int64
	// BytesCopied is committed copied bytes for destructive rewrites. Dry-run
	// reports the bytes that would be copied.
	BytesCopied      int64
	BytesReclaimable int64
	// BytesRetained is the pre-GC physical bytes in segments observed by the
	// reachability plan. Successful rewrite remaps protected refs, but the old
	// mixed segment remains on disk until ColumnAssetGC reclaims SupersededRefs.
	BytesRetained int64

	SupersededRefs     []ColumnAssetRef
	RemappedRefs       []ColumnAssetRef
	RemapManifestRoot  uint64
	RemapSystemRoot    uint64
	RemapSegmentFileID uint32
}

// ColumnAssetRewrite copies protected manifest refs out of complete mixed
// segments and remaps the column manifest to the copied refs. It never rewrites
// logical rows and never deletes the old mixed segment; M15B GC can reclaim the
// old segment once callers present the superseded refs as reclaimable candidates.
func (c *Collection) ColumnAssetRewrite(ctx context.Context, opts ColumnAssetRewriteOptions) (ColumnAssetRewriteStats, error) {
	return c.columnAssetRewriteWithOptions(ctx, columnAssetRewriteOptions{
		ColumnAssetRewriteOptions: opts,
	})
}

func (c *Collection) columnAssetRewriteWithOptions(ctx context.Context, opts columnAssetRewriteOptions) (ColumnAssetRewriteStats, error) {
	var stats ColumnAssetRewriteStats
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
		unlock := c.lockMutation()
		defer unlock.Unlock()
	}
	return c.columnAssetRewrite(ctx, opts)
}

func (c *Collection) columnAssetRewrite(ctx context.Context, opts columnAssetRewriteOptions) (ColumnAssetRewriteStats, error) {
	planOpts, err := c.columnAssetLifecycleAugmentReachabilityOptions(ColumnAssetReachabilityOptions{
		Detailed:                         opts.Detailed,
		SegmentDetails:                   true,
		CandidateRefs:                    opts.CandidateRefs,
		PendingRefs:                      opts.PendingRefs,
		PreparedRefs:                     opts.PreparedRefs,
		PreparedQueryRefs:                opts.PreparedQueryRefs,
		QuarantineRefs:                   opts.QuarantineRefs,
		QuarantineSegments:               opts.QuarantineSegments,
		PinnedRefs:                       opts.PinnedRefs,
		releaseVectorPartitionReclaimIDs: opts.releaseVectorPartitionReclaimIDs,
	})
	if err != nil {
		return ColumnAssetRewriteStats{}, err
	}
	plan, sourceMasks, err := c.planColumnAssetReachability(ctx, columnAssetReachabilityOptionsInternal{
		ColumnAssetReachabilityOptions: planOpts,
		omitDetailedEntrySources:       !opts.Detailed,
		omitDetailedEntrySort:          !opts.Detailed,
	})
	stats := ColumnAssetRewriteStats{
		DryRun:           opts.DryRun,
		Plan:             plan,
		SegmentsRetained: plan.Segments.Total,
		RefsRetained:     plan.Refs.Total,
		BytesRetained:    plan.Segments.BytesTotal,
	}
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if !plan.Complete {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
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

	namespace, err := columnAssetManagerNamespaceForRoot(c.db.ColumnAssetRootDir(), plan.Namespace)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	segments := columnAssetRewriteEligibleSegments(namespace.SegmentDir, plan, sourceMasks)
	refs := columnAssetRewriteEligibleRefs(plan, sourceMasks, segments)
	for _, entry := range segments {
		stats.SegmentsEligible++
		stats.BytesEligible += entry.ProtectedBytes
		stats.BytesReclaimable += entry.ReclaimableBytes
	}
	stats.RefsEligible = len(refs)
	var bytesToCopy int64
	for _, ref := range refs {
		bytesToCopy += ref.Length
	}
	if opts.DryRun {
		stats.BytesCopied = bytesToCopy
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, nil
	}
	// Re-check after reachability planning and immediately before writing copied
	// assets; DB maintenance readiness can change independently of this
	// collection's mutation lock.
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if len(segments) == 0 || len(refs) == 0 {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, nil
	}

	state, err := c.loadColumnAssetRewriteManifestState()
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if err := c.columnAssetRewriteRootDescriptorPreflight(state)(); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	// Validate publish-only manifest root state before copying assets so local
	// descriptor corruption cannot leave behind an otherwise avoidable segment.
	if _, err := columnAssetRewriteManifestRootStoragePolicy(state); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	remap, err := c.copyColumnAssetRewriteRefs(ctx, state.cfg, refs)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if len(remap.oldRefs) == 0 {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, nil
	}
	// Copied segments remain persistent GC orphans on every pre-visibility
	// failure. Authority release must never pathname-delete a segment that may
	// have been rebound after its exact handle was captured.
	defer remap.releaseStableResources()
	if opts.afterCopyHookForTest != nil {
		if err := opts.afterCopyHookForTest(); err != nil {
			stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
	}
	if err := ctx.Err(); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	// This preflight still runs before the publish attempt. A stale copy remains
	// a persistent GC orphan because pathname cleanup cannot prove that the
	// captured inode is still bound to its original name.
	if err := c.columnAssetRewriteRootDescriptorPreflight(state)(); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if opts.afterPrePublishHookForTest != nil {
		if err := opts.afterPrePublishHookForTest(); err != nil {
			stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
	}
	if opts.beforeRemapPublish != nil {
		if err := opts.beforeRemapPublish(slices.Clone(remap.oldRefs)); err != nil {
			stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
	}
	if err := ctx.Err(); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}

	patchedRecords, patched, err := patchColumnAssetRewriteManifestRecordsInPlace(state.records, remap.byOldRef, state.cfg.AssetManager.Namespace)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if patched != len(remap.oldRefs) {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, fmt.Errorf("collections: column asset rewrite patched %d manifest refs, want %d", patched, len(remap.oldRefs))
	}
	updatedIdentity, err := columnAssetRewriteUpdatedIdentity(state, patchedRecords)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	updatedMeta, err := columnAssetRewriteUpdatedMeta(state.meta, updatedIdentity)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	durableRequirements, err := stableColumnManifestDurableRequirements(patchedRecords, state.manifest.Generation, state.cfg.AssetManager.Namespace)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, fmt.Errorf("collections: column asset rewrite durable requirements: %w", err)
	}
	durableResources := remap.stableResources
	remap.stableResources = nil
	newSystemRoot, rootIDs, err := c.publishColumnAssetRewriteManifestStateWithDurableClosure(state, updatedMeta, updatedIdentity, patchedRecords, durableResources, durableRequirements)
	if err != nil {
		if columnAssetRewritePublishFailedBeforeApply(err) {
			stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
		// Publish errors can be ambiguous after root/system application starts;
		// retain the copied segment as fail-closed maintenance debt and make it a
		// process-local logical quarantine root until DB close/recovery.
		quarantineErr := c.columnAssetRewriteRegisterAmbiguousPublishQuarantine(remap.newRefs, err)
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, errors.Join(err, quarantineErr)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, unexpectedOrderedRootCountError(state.meta.Name, 1, len(rootIDs))
	}

	stats.SegmentsRewritten = stats.SegmentsEligible
	stats.RefsRemapped = len(remap.oldRefs)
	for _, ref := range remap.newRefs {
		stats.BytesCopied += ref.Length
	}
	stats.SupersededRefs = remap.oldRefs
	stats.RemappedRefs = remap.newRefs
	stats.RemapManifestRoot = rootIDs[0]
	stats.RemapSystemRoot = newSystemRoot
	stats.RemapSegmentFileID = remap.segmentFileID
	nextCatalog := cloneCatalogWithRootUpdates(state.catalog, updatedMeta, []string{state.rootName}, rootIDs)
	c.meta = updatedMeta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
	return stats, nil
}

func columnAssetRewritePublishFailedBeforeApply(err error) bool {
	return errors.Is(err, errColumnAssetRewritePublishPreflightFailed) ||
		errors.Is(err, backenddb.ErrStorageMaintenancePublishPreApplyFailed)
}

func (c *Collection) columnAssetRewriteRegisterAmbiguousPublishQuarantine(refs []ColumnAssetRef, publishErr error) error {
	if len(refs) == 0 {
		return nil
	}
	reason := "ambiguous column asset rewrite publish"
	if publishErr != nil {
		reason = fmt.Sprintf("%s: %v", reason, publishErr)
	}
	_, err := c.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
		Owner:  "column_asset_rewrite",
		Source: "ambiguous_publish",
		Reason: reason,
		Refs:   refs,
	})
	return err
}

type columnAssetRewriteManifestState struct {
	catalog        *collectionCatalog
	meta           CollectionMeta
	cfg            ColumnStoreConfig
	rootName       string
	baseRoot       uint64
	baseCommitSeq  uint64
	baseSystemRoot uint64
	manifest       columnManifestSnapshot
	records        []columnManifestRecord
}

type columnAssetRewriteCopyResult struct {
	oldRefs              []ColumnAssetRef
	newRefs              []ColumnAssetRef
	byOldRef             map[ColumnAssetRef]ColumnAssetRef
	segmentFileID        uint32
	stableResources      *rootpublication.StableResourceSet
	stableSegments       uint64
	stableDescriptors    uint64
	stableContentSyncs   uint64
	stableNamespaceSyncs uint64
	stablePinHighWater   uint64
}

func (result *columnAssetRewriteCopyResult) releaseStableResources() {
	if result == nil || result.stableResources == nil {
		return
	}
	result.stableResources.Release()
	result.stableResources = nil
}

func (c *Collection) loadColumnAssetRewriteManifestState() (columnAssetRewriteManifestState, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return columnAssetRewriteManifestState{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return columnAssetRewriteManifestState{}, err
	}
	if catalog == nil {
		return columnAssetRewriteManifestState{}, errCollectionNotFound
	}
	meta := catalog.meta
	if meta.Options.ColumnStore == nil || !meta.Options.ColumnStore.Enabled {
		return columnAssetRewriteManifestState{}, errors.New("collections: column asset rewrite requires enabled column_store")
	}
	cfg := meta.Options.ColumnStore.copy()
	if cfg.ActiveManifest == nil {
		return columnAssetRewriteManifestState{}, errors.New("collections: column asset rewrite requires active column manifest")
	}
	if cfg.RecoveryAuthoritativeManifest == nil {
		return columnAssetRewriteManifestState{}, errors.New("collections: column asset rewrite requires recovery-authoritative column manifest")
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return columnAssetRewriteManifestState{}, errors.New("collections: column asset rewrite requires active recovery-authoritative manifest")
	}
	if cfg.AssetManager == nil {
		return columnAssetRewriteManifestState{}, errors.New("collections: column asset rewrite requires column asset manager metadata")
	}
	rootName := collectionColumnManifestRootName(meta.Name)
	baseRoot := catalog.rootID(rootName)
	if baseRoot == 0 {
		return columnAssetRewriteManifestState{}, fmt.Errorf("collections: column asset rewrite missing manifest root %q", rootName)
	}
	if err := validateColumnManifestIdentityAtRoot(snap, baseRoot, *cfg.ActiveManifest); err != nil {
		return columnAssetRewriteManifestState{}, err
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, baseRoot)
	if err != nil {
		return columnAssetRewriteManifestState{}, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		return columnAssetRewriteManifestState{}, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, cfg, *cfg.ActiveManifest, meta.Name, "column asset rewrite"); err != nil {
		return columnAssetRewriteManifestState{}, err
	}
	return columnAssetRewriteManifestState{
		catalog:        catalog,
		meta:           meta,
		cfg:            cfg,
		rootName:       rootName,
		baseRoot:       baseRoot,
		baseCommitSeq:  snapshotCommitSeq(snap),
		baseSystemRoot: snapshotSystemRoot(snap),
		manifest:       manifest,
		records:        records,
	}, nil
}

func (c *Collection) copyColumnAssetRewriteRefs(ctx context.Context, cfg ColumnStoreConfig, refs []ColumnAssetRef) (out columnAssetRewriteCopyResult, retErr error) {
	if err := validateColumnAssetRewriteRefKinds(refs); err != nil {
		return columnAssetRewriteCopyResult{}, err
	}
	readCache, err := newColumnPhysicalAssetReadCache(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		return columnAssetRewriteCopyResult{}, err
	}
	defer func() { _ = readCache.close() }()
	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(
		c.db.ColumnAssetRootDir(), cfg, c.db.StableResourceIdentityPinRegistry(),
	)
	if err != nil {
		return columnAssetRewriteCopyResult{}, err
	}
	closed := false
	defer func() {
		if closed {
			return
		}
		retErr = errors.Join(retErr, cleanupColumnAssetRewriteOpenAppender(appender))
	}()
	out = columnAssetRewriteCopyResult{
		oldRefs:       make([]ColumnAssetRef, 0, len(refs)),
		newRefs:       make([]ColumnAssetRef, 0, len(refs)),
		byOldRef:      make(map[ColumnAssetRef]ColumnAssetRef, len(refs)),
		segmentFileID: appender.fileID,
	}
	var rawScratch []byte
	for _, oldRef := range refs {
		if err := ctx.Err(); err != nil {
			return columnAssetRewriteCopyResult{}, err
		}
		raw, err := readCache.read(oldRef, rawScratch)
		if err != nil {
			return columnAssetRewriteCopyResult{}, fmt.Errorf("collections: column asset rewrite read generation=%d part_id=%d: %w", oldRef.Generation, oldRef.PartID, err)
		}
		rawScratch = raw
		alignment := columnAssetSegmentPayloadAlignment(oldRef.Kind, cfg)
		if oldRef.Kind == ColumnAssetKindTCS1TypedColumnPart {
			switch {
			case oldRef.Offset%columnVectorGraphScalarU8CodesAlignment == 0:
				alignment = columnVectorGraphScalarU8CodesAlignment
			case oldRef.Offset%int64(typedColumnPartDirectViewAssetAlignment) == 0:
				alignment = typedColumnPartDirectViewAssetAlignment
			}
		}
		newRef, err := appender.appendKindWithAlignment(raw, oldRef.Kind, oldRef.Generation, oldRef.PartID, alignment)
		if err != nil {
			return columnAssetRewriteCopyResult{}, err
		}
		if !columnAssetRewriteSameLogicalRef(oldRef, newRef) {
			return columnAssetRewriteCopyResult{}, fmt.Errorf("collections: column asset rewrite copied ref %+v to non-equivalent ref %+v", oldRef, newRef)
		}
		out.oldRefs = append(out.oldRefs, oldRef)
		out.newRefs = append(out.newRefs, newRef)
		out.byOldRef[oldRef] = newRef
	}
	if err := appender.close(); err != nil {
		return columnAssetRewriteCopyResult{}, err
	}
	out.stableResources = appender.stableResources
	appender.stableResources = nil
	if out.stableResources == nil {
		return columnAssetRewriteCopyResult{}, errors.New("collections: column asset rewrite copy returned no stable authority")
	}
	descriptors := out.stableResources.Descriptors()
	out.stableDescriptors = uint64(len(descriptors))
	identities := make(map[rootpublication.StableIdentity]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		identities[descriptor.Identity()] = struct{}{}
	}
	out.stableSegments = uint64(len(identities))
	out.stableContentSyncs = uint64(appender.closeStats.FileSyncCount)
	for _, stats := range out.stableResources.Stats(time.Now()) {
		out.stableNamespaceSyncs += stats.NamespaceSyncs
		out.stablePinHighWater += stats.PinHighWater
	}
	// This rewrite transaction owns one newly-created physical segment. A mixed
	// segment may require multiple kind-scoped descriptors and pins while still
	// sharing one exact file identity and one content/namespace sync epoch. The
	// stable inventory counts that shared physical namespace barrier once.
	if out.stableSegments != 1 || out.stableDescriptors == 0 || out.stableContentSyncs != 1 || out.stableNamespaceSyncs != 1 || out.stablePinHighWater != out.stableDescriptors {
		out.releaseStableResources()
		return columnAssetRewriteCopyResult{}, fmt.Errorf("%w: column asset rewrite stable counters segments=%d descriptors=%d content_syncs=%d namespace_syncs=%d pin_high_water=%d want segments/content/namespace=1 and pins=descriptors", rootpublication.ErrUnresolvedResource, out.stableSegments, out.stableDescriptors, out.stableContentSyncs, out.stableNamespaceSyncs, out.stablePinHighWater)
	}
	assets := make([]ColumnPreparedAsset, len(out.newRefs))
	for i, ref := range out.newRefs {
		assets[i] = ColumnPreparedAsset{Ref: ref}
	}
	if err := validateStableColumnResourcesMatchPrepared(assets, out.stableResources); err != nil {
		out.releaseStableResources()
		return columnAssetRewriteCopyResult{}, fmt.Errorf("%w: column asset rewrite stable closure: %v", rootpublication.ErrUnresolvedResource, err)
	}
	closed = true
	return out, nil
}

func cleanupColumnAssetRewriteOpenAppender(appender *columnPhysicalAssetSegmentAppender) error {
	if appender == nil {
		return nil
	}
	// Abandoned partial copies remain unreachable persistent orphans. Abort
	// closes only the exact file handle; later reachability GC may reclaim them.
	return appender.abort()
}

func validateColumnAssetRewriteRefKinds(refs []ColumnAssetRef) error {
	for idx, ref := range refs {
		if ref.Kind != ColumnAssetKindTCS1PartImage && ref.Kind != ColumnAssetKindTCS1TypedColumnPart && ref.Kind != ColumnAssetKindTCS1AggregateMetadata && ref.Kind != ColumnAssetKindTCS1DictionaryCodes && ref.Kind != ColumnAssetKindTCS1Int64Values && ref.Kind != ColumnAssetKindTCS1HNSWSearchPack {
			return fmt.Errorf("collections: column asset rewrite supports only physical part, typed-column part, aggregate metadata, dictionary code, int64 value, or hnsw search pack refs: ref %d kind %q", idx, ref.Kind)
		}
	}
	return nil
}

func patchColumnAssetRewriteManifestRecords(records []columnManifestRecord, byOldRef map[ColumnAssetRef]ColumnAssetRef, expectedNamespace string) ([]columnManifestRecord, int, error) {
	return patchColumnAssetRewriteManifestRecordsWithMode(records, byOldRef, expectedNamespace, false)
}

func patchColumnAssetRewriteManifestRecordsInPlace(records []columnManifestRecord, byOldRef map[ColumnAssetRef]ColumnAssetRef, expectedNamespace string) ([]columnManifestRecord, int, error) {
	return patchColumnAssetRewriteManifestRecordsWithMode(records, byOldRef, expectedNamespace, true)
}

// patchColumnAssetRewriteManifestRecordsWithMode mutates part values only when
// the caller owns records, as loadColumnAssetRewriteManifestState does.
func patchColumnAssetRewriteManifestRecordsWithMode(records []columnManifestRecord, byOldRef map[ColumnAssetRef]ColumnAssetRef, expectedNamespace string, inPlace bool) ([]columnManifestRecord, int, error) {
	if len(records) == 0 {
		return nil, 0, nil
	}
	patched := records
	if !inPlace {
		patched = make([]columnManifestRecord, len(records))
	}
	count := 0
	for i, record := range records {
		if !inPlace {
			patched[i] = record
		}
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			graph, err := decodeColumnVectorGraphManifestRecord(record.value)
			if err != nil {
				return nil, 0, err
			}
			if _, err := columnVectorGraphManifestAssetRefsForScan(graph, graph.BaseManifestGeneration, expectedNamespace); err != nil {
				return nil, 0, err
			}
			changed := false
			if newRef, ok := byOldRef[graph.AssetRef]; ok {
				if !columnAssetRewriteSameLogicalRef(graph.AssetRef, newRef) {
					return nil, 0, fmt.Errorf("collections: column asset rewrite cannot remap non-equivalent vector graph ref %+v to %+v", graph.AssetRef, newRef)
				}
				if err := validateColumnAssetRefForPlan(newRef); err != nil {
					return nil, 0, err
				}
				graph.AssetRef = newRef
				graph.AssetBytes = newRef.Length
				patchColumnVectorGraphAdjacencySourceGraphIdentity(&graph.Layer0AdjacencySource, newRef)
				for idx := range graph.AdjacencyLayerSources {
					patchColumnVectorGraphAdjacencySourceGraphIdentity(&graph.AdjacencyLayerSources[idx], newRef)
				}
				changed = true
				count++
			}
			if len(graph.AdjacencyLayerSources) > 0 {
				for idx := range graph.AdjacencyLayerSources {
					sourceRef := graph.AdjacencyLayerSources[idx].Ref
					if newRef, ok := byOldRef[sourceRef]; ok {
						if !columnAssetRewriteSameLogicalRef(sourceRef, newRef) {
							return nil, 0, fmt.Errorf("collections: column asset rewrite cannot remap non-equivalent vector graph adjacency layer %d source ref %+v to %+v", graph.AdjacencyLayerSources[idx].Layer, sourceRef, newRef)
						}
						if err := validateColumnAssetRefForPlan(newRef); err != nil {
							return nil, 0, err
						}
						graph.AdjacencyLayerSources[idx].Ref = newRef
						graph.AdjacencyLayerSources[idx].AssetBytes = newRef.Length
						changed = true
						count++
					}
				}
				if len(graph.AdjacencyLayerSources) > 0 {
					graph.Layer0AdjacencySource = graph.AdjacencyLayerSources[0]
				}
			} else if graph.Layer0AdjacencySource.Present {
				sourceRef := graph.Layer0AdjacencySource.Ref
				if newRef, ok := byOldRef[sourceRef]; ok {
					if !columnAssetRewriteSameLogicalRef(sourceRef, newRef) {
						return nil, 0, fmt.Errorf("collections: column asset rewrite cannot remap non-equivalent vector graph layer-0 adjacency source ref %+v to %+v", sourceRef, newRef)
					}
					if err := validateColumnAssetRefForPlan(newRef); err != nil {
						return nil, 0, err
					}
					graph.Layer0AdjacencySource.Ref = newRef
					graph.Layer0AdjacencySource.AssetBytes = newRef.Length
					changed = true
					count++
				}
			}
			if changed {
				value, err := encodeColumnVectorGraphManifestRecord(graph)
				if err != nil {
					return nil, 0, err
				}
				patched[i].value = value
			}
			continue
		}
		if bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
			state, err := decodeColumnVectorIndexStateRecord(record.value)
			if err != nil {
				return nil, 0, err
			}
			if _, err := columnVectorIndexStateManifestAssetRefsForScan(state, state.BaseManifestGeneration, expectedNamespace); err != nil {
				return nil, 0, err
			}
			changed := false
			for idx := range state.Assets {
				oldRef := state.Assets[idx].Ref
				newRef, ok := byOldRef[oldRef]
				if !ok {
					continue
				}
				if !columnAssetRewriteSameLogicalRef(oldRef, newRef) {
					return nil, 0, fmt.Errorf("collections: column asset rewrite cannot remap non-equivalent vector-index state role=%q id=%q ref %+v to %+v", state.Assets[idx].Role, state.Assets[idx].AssetID, oldRef, newRef)
				}
				if err := validateColumnAssetRefForPlan(newRef); err != nil {
					return nil, 0, err
				}
				state.Assets[idx].Ref = newRef
				state.Assets[idx].AssetBytes = newRef.Length
				changed = true
				count++
			}
			if changed {
				value, err := encodeColumnVectorIndexStateRecord(state)
				if err != nil {
					return nil, 0, err
				}
				patched[i].value = value
			}
			continue
		}
		isPart := bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes)
		isMetadata := bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes)
		isDictionary := bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes)
		isInt64Values := bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes)
		if !isPart && !isMetadata && !isDictionary && !isInt64Values {
			continue
		}
		oldRef, offsets, err := columnAssetRewriteManifestPartRefForPatch(record.value, expectedNamespace)
		if err != nil {
			return nil, 0, err
		}
		var keyGeneration, keyPartID uint64
		if isPart {
			keyGeneration, keyPartID, err = columnManifestPartKeyFromRecordKeyForScan(record.key)
			if err != nil {
				return nil, 0, err
			}
		} else {
			if isMetadata {
				keyGeneration, keyPartID, _, err = columnManifestAggregateMetadataKeyFromRecordKey(record.key)
			} else if isDictionary {
				keyGeneration, keyPartID, _, err = columnManifestDictionaryCodesKeyFromRecordKey(record.key)
			} else {
				keyGeneration, keyPartID, _, err = columnManifestInt64ValuesKeyFromRecordKey(record.key)
			}
			if err != nil {
				return nil, 0, err
			}
		}
		if oldRef.Generation != keyGeneration {
			return nil, 0, fmt.Errorf("collections: column asset rewrite part key generation=%d does not match ref generation=%d", keyGeneration, oldRef.Generation)
		}
		if oldRef.PartID != keyPartID {
			return nil, 0, fmt.Errorf("collections: column asset rewrite part key part_id=%d does not match ref part_id=%d", keyPartID, oldRef.PartID)
		}
		newRef, ok := byOldRef[oldRef]
		if !ok {
			continue
		}
		if !columnAssetRewriteSameLogicalRef(oldRef, newRef) {
			return nil, 0, fmt.Errorf("collections: column asset rewrite cannot remap non-equivalent manifest ref %+v to %+v", oldRef, newRef)
		}
		if err := validateColumnAssetRefForPlan(newRef); err != nil {
			return nil, 0, err
		}
		value := record.value
		if !inPlace {
			value = bytes.Clone(record.value)
		}
		binary.BigEndian.PutUint64(value[offsets.fileID:], uint64(newRef.FileID))
		binary.BigEndian.PutUint64(value[offsets.offset:], uint64(newRef.Offset))
		binary.BigEndian.PutUint64(value[offsets.length:], uint64(newRef.Length))
		binary.BigEndian.PutUint64(value[offsets.checksum:], uint64(newRef.Checksum))
		patched[i].value = value
		count++
	}
	return patched, count, nil
}

func patchColumnVectorGraphAdjacencySourceGraphIdentity(source *columnVectorGraphLayer0AdjacencySourceSnapshot, graphRef ColumnAssetRef) {
	if source == nil || !source.Present {
		return
	}
	source.GraphAssetGeneration = graphRef.Generation
	source.GraphAssetPartID = graphRef.PartID
	source.GraphAssetFileID = graphRef.FileID
	source.GraphAssetOffset = graphRef.Offset
	source.GraphAssetLength = graphRef.Length
	source.GraphAssetChecksum = graphRef.Checksum
}

type columnAssetRewriteManifestPartPatchOffsets struct {
	fileID   int
	offset   int
	length   int
	checksum int
}

func columnAssetRewriteManifestPartRefForPatch(raw []byte, expectedNamespace string) (ColumnAssetRef, columnAssetRewriteManifestPartPatchOffsets, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestPartMagic {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, fmt.Errorf("collections: bad column manifest part magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnManifestRecordVersion(version) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, fmt.Errorf("collections: unsupported column manifest part version=%d", version)
	}
	kindBytes := cur.stringBytes()
	namespaceBytes := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	offsets := columnAssetRewriteManifestPartPatchOffsets{fileID: cur.pos}
	fileID64 := cur.u64()
	offsets.offset = cur.pos
	offset64 := cur.u64()
	offsets.length = cur.pos
	length64 := cur.u64()
	offsets.checksum = cur.pos
	checksum64 := cur.u64()
	rows64 := uint64(0)
	if version >= columnManifestRecordVersionV2 {
		rows64 = cur.u64()
	}
	bytes64 := cur.u64()
	_ = cur.u64() // publish_id; rewrite preserves the original field bytes.
	_ = cur.u64() // generation_id; rewrite preserves the original field bytes.
	reason := cur.stringBytes()
	role := ColumnManifestPartRole("")
	if version >= columnManifestRecordVersionV3 {
		role = ColumnManifestPartRole(string(cur.stringBytes()))
	}
	var sortKey []ColumnSortKey
	if version >= columnManifestRecordVersionV4 {
		sortKey = readColumnManifestSortKey(&cur)
	}
	if err := cur.err; err != nil {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, err
	}
	if cur.pos != len(raw) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: trailing bytes in column manifest part record")
	}
	kind := ColumnAssetKind(string(kindBytes))
	if kind != ColumnAssetKindTCS1PartImage && kind != ColumnAssetKindTCS1TypedColumnPart && kind != ColumnAssetKindTCS1AggregateMetadata && kind != ColumnAssetKindTCS1DictionaryCodes && kind != ColumnAssetKindTCS1Int64Values {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, fmt.Errorf("collections: unsupported column manifest part asset kind %q", string(kindBytes))
	}
	if role == "" {
		if version >= columnManifestRecordVersionV3 && (kind == ColumnAssetKindTCS1PartImage || kind == ColumnAssetKindTCS1TypedColumnPart) {
			return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: column manifest part role is required for v3 typed-storage part record")
		}
		role = inferColumnManifestPartRole(kind, string(reason))
	}
	if !columnPhysicalBytesEqualString(namespaceBytes, expectedNamespace) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, fmt.Errorf("collections: column manifest part namespace=%q want %q", string(namespaceBytes), expectedNamespace)
	}
	if fileID64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: column manifest part file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: column manifest part checksum overflows uint32")
	}
	if rows64 > uint64(maxCollectionInt) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: column manifest part rows overflows int")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || bytes64 > uint64(math.MaxInt64) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: column manifest part offsets or byte counts overflow int64")
	}
	ref := ColumnAssetRef{
		Kind:       kind,
		Namespace:  expectedNamespace,
		Generation: generation,
		PartID:     partID,
		FileID:     uint32(fileID64),
		Offset:     int64(offset64),
		Length:     int64(length64),
		Checksum:   uint32(checksum64),
	}
	if err := validateColumnPreparedAssetForPlan(ColumnPreparedAsset{Ref: ref, Rows: int(rows64), Bytes: int64(bytes64), Reason: string(reason), PartRole: role, SortKey: columnSortKeyMatchString(sortKey)}); err != nil {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, err
	}
	return ref, offsets, nil
}

type columnVectorGraphManifestRefPatchOffsets struct {
	fileID   int
	offset   int
	length   int
	checksum int
}

func columnVectorGraphManifestAssetRefForPatch(raw []byte, expectedNamespace string) (ColumnAssetRef, columnVectorGraphManifestRefPatchOffsets, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestVectorGraphMagic {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, fmt.Errorf("collections: bad column vector graph manifest magic=0x%08x", magic)
	}
	if version := cur.u16(); !isSupportedColumnVectorGraphManifestRecordVersion(version) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, fmt.Errorf("collections: unsupported column vector graph manifest version=%d", version)
	}
	_ = cur.stringBytes() // index_name
	_ = cur.stringBytes() // field
	_ = cur.stringBytes() // metric
	_ = cur.stringBytes() // encoding
	_ = cur.u64()         // dimensions
	_ = cur.u64()         // M
	_ = cur.u64()         // ef_construction
	_ = cur.u64()         // ef_search
	generation := cur.u64()
	_ = cur.u64() // base manifest checksum
	_ = cur.u64() // base schema hash
	_ = cur.u64() // graph schema hash
	rowCount64 := cur.u64()
	kindBytes := cur.stringBytes()
	namespaceBytes := cur.stringBytes()
	refGeneration := cur.u64()
	partID := cur.u64()
	offsets := columnVectorGraphManifestRefPatchOffsets{fileID: cur.pos}
	fileID64 := cur.u64()
	offsets.offset = cur.pos
	offset64 := cur.u64()
	offsets.length = cur.pos
	length64 := cur.u64()
	offsets.checksum = cur.pos
	checksum64 := cur.u64()
	assetBytes64 := cur.u64()
	if err := cur.err; err != nil {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, err
	}
	if cur.pos != len(raw) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, errors.New("collections: trailing bytes in column vector graph manifest record")
	}
	if generation != refGeneration {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, fmt.Errorf("collections: column vector graph manifest generation=%d does not match ref generation=%d", generation, refGeneration)
	}
	if !columnPhysicalBytesEqualString(kindBytes, string(ColumnAssetKindTCS1PartImage)) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, fmt.Errorf("collections: unsupported column vector graph manifest asset kind %q", string(kindBytes))
	}
	if !columnPhysicalBytesEqualString(namespaceBytes, expectedNamespace) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, fmt.Errorf("collections: column vector graph manifest namespace=%q want %q", string(namespaceBytes), expectedNamespace)
	}
	if fileID64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, errors.New("collections: column vector graph manifest file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, errors.New("collections: column vector graph manifest checksum overflows uint32")
	}
	if rowCount64 > uint64(maxCollectionInt) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, errors.New("collections: column vector graph manifest row count overflows int")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || assetBytes64 > uint64(math.MaxInt64) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, errors.New("collections: column vector graph manifest offsets or byte counts overflow int64")
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  expectedNamespace,
		Generation: refGeneration,
		PartID:     partID,
		FileID:     uint32(fileID64),
		Offset:     int64(offset64),
		Length:     int64(length64),
		Checksum:   uint32(checksum64),
	}
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, err
	}
	if assetBytes64 == 0 {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, errors.New("collections: column vector graph manifest asset bytes=0 must be positive")
	}
	if ref.Length != int64(assetBytes64) {
		return ColumnAssetRef{}, columnVectorGraphManifestRefPatchOffsets{}, fmt.Errorf("collections: column vector graph manifest asset bytes=%d does not match ref length=%d", assetBytes64, ref.Length)
	}
	return ref, offsets, nil
}

func columnAssetRewriteUpdatedIdentity(state columnAssetRewriteManifestState, records []columnManifestRecord) (ColumnManifestIdentity, error) {
	active, err := activeColumnManifestRecordsForScan(records, state.manifest.Generation)
	if err != nil {
		return ColumnManifestIdentity{}, err
	}
	checksum := checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
		Collection:        state.manifest.Collection,
		ColumnStore:       state.cfg,
		Operation:         state.manifest.Operation,
		AppliedCommandLSN: state.manifest.AppliedCommandLSN,
	}, state.manifest.Generation, active)
	identity := *state.cfg.ActiveManifest
	identity.Checksum = checksum
	normalizeColumnManifestIdentityDefaults(&identity)
	if err := validateColumnManifestIdentity(identity); err != nil {
		return ColumnManifestIdentity{}, err
	}
	return identity, nil
}

func columnAssetRewriteUpdatedMeta(base CollectionMeta, identity ColumnManifestIdentity) (CollectionMeta, error) {
	if base.Options.ColumnStore == nil || !base.Options.ColumnStore.Enabled {
		return CollectionMeta{}, errors.New("collections: column asset rewrite requires enabled column_store")
	}
	updated := copyCollectionMeta(base)
	cfg := updated.Options.ColumnStore.copy()
	active := identity
	recovery := identity
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	updated.Options.ColumnStore = &cfg
	return normalizeCollectionMeta(updated)
}

func columnAssetRewriteManifestRootStoragePolicy(state columnAssetRewriteManifestState) (backenddb.OrderedRootStoragePolicy, error) {
	if state.cfg.ManifestRoot == nil {
		return backenddb.OrderedRootStorageDefault, errors.New("collections: column asset rewrite requires manifest root descriptor")
	}
	return backendRootStoragePolicy(state.cfg.ManifestRoot.StoragePolicy)
}

func (c *Collection) publishColumnAssetRewriteManifestState(state columnAssetRewriteManifestState, updatedMeta CollectionMeta, updatedIdentity ColumnManifestIdentity, records []columnManifestRecord) (uint64, []uint64, error) {
	return c.publishColumnAssetRewriteManifestStateWithDurableClosure(state, updatedMeta, updatedIdentity, records, nil, rootpublication.StableLogicalObligationRequirements{})
}

func (c *Collection) publishColumnAssetRewriteManifestStateWithDurableClosure(state columnAssetRewriteManifestState, updatedMeta CollectionMeta, updatedIdentity ColumnManifestIdentity, records []columnManifestRecord, durableResources *rootpublication.StableResourceSet, durableRequirements rootpublication.StableLogicalObligationRequirements) (uint64, []uint64, error) {
	defer durableResources.Release()
	policy, err := columnAssetRewriteManifestRootStoragePolicy(state)
	if err != nil {
		return 0, nil, err
	}
	identityRecord := encodeColumnManifestIdentityRecordArray(updatedIdentity)
	ordered := []backenddb.StorageMaintenanceRootDeltaPublishInput{{
		BaseRoot:                    state.baseRoot,
		Iter:                        columnManifestRootRecordIteratorOwned(identityRecord, records),
		StoragePolicy:               policy,
		DurableResources:            durableResources,
		DurableResourceRequirements: durableRequirements,
	}}
	return c.db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		ordered,
		c.columnAssetRewriteRootDescriptorPreflight(state),
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			rootNames := []string{state.rootName}
			baseRootIDs := map[string]uint64{state.rootName: state.baseRoot}
			return c.buildColumnAssetRewriteSystemDeltaIteratorForMeta(state.meta, updatedMeta, state.baseCommitSeq, state.baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		},
	)
}

func (c *Collection) columnAssetRewriteRootDescriptorPreflight(state columnAssetRewriteManifestState) backenddb.OrderedRootGroupPreflight {
	rootNames := []string{state.rootName}
	baseRootIDs := map[string]uint64{state.rootName: state.baseRoot}
	return func() error {
		if err := c.validateRootDescriptorSystemDeltaForMeta(state.meta, state.baseCommitSeq, state.baseSystemRoot, rootNames, baseRootIDs); err != nil {
			return errors.Join(errColumnAssetRewritePublishPreflightFailed, err)
		}
		return nil
	}
}

func (c *Collection) buildColumnAssetRewriteSystemDeltaIteratorForMeta(baseMeta, updatedMeta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(baseMeta.Name, len(rootNames), len(rootIDs))
	}
	if err := c.validateRootDescriptorSystemDeltaForMeta(baseMeta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, err
	}
	encodedMeta, err := encodeNormalizedCollectionMeta(updatedMeta)
	if err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames)+1)
	updates[systemCollectionMetaKey(updatedMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		if rootIDs[i] == 0 {
			return nil, fmt.Errorf("collections: ordered root publish returned zero root for %q", rootName)
		}
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemDeltaIterator(updates)
}

func columnAssetRewriteEligibleSegments(segmentDir string, plan ColumnAssetReachabilityPlan, sourceMasks map[ColumnAssetRef]columnAssetReachabilitySourceMask) map[uint32]ColumnAssetReachabilitySegmentEntry {
	eligible := make(map[uint32]ColumnAssetReachabilitySegmentEntry, plan.Segments.Mixed)
	for _, entry := range plan.SegmentEntries {
		if columnAssetRewriteSegmentEligible(segmentDir, entry) {
			eligible[entry.FileID] = entry
		}
	}
	for ref, sourceMask := range sourceMasks {
		if !columnAssetRewriteSourceMaskIsProtectedNonManifest(sourceMask) {
			continue
		}
		if _, ok := eligible[ref.FileID]; !ok {
			continue
		}
		if !columnAssetReachabilityRefCanContributeRange(ref, plan.Namespace) {
			continue
		}
		delete(eligible, ref.FileID)
	}
	return eligible
}

func columnAssetRewriteEligibleRefs(plan ColumnAssetReachabilityPlan, sourceMasks map[ColumnAssetRef]columnAssetReachabilitySourceMask, segments map[uint32]ColumnAssetReachabilitySegmentEntry) []ColumnAssetRef {
	refs := make([]ColumnAssetRef, 0, len(sourceMasks))
	for ref, sourceMask := range sourceMasks {
		if !columnAssetRewriteSourceMaskIncludesManifest(sourceMask) {
			continue
		}
		if _, ok := segments[ref.FileID]; !ok {
			continue
		}
		if !columnAssetReachabilityRefCanContributeRange(ref, plan.Namespace) {
			continue
		}
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, compareColumnAssetRefs)
	return refs
}

func columnAssetRewriteSegmentEligible(segmentDir string, entry ColumnAssetReachabilitySegmentEntry) bool {
	if entry.Status != ColumnAssetReachabilitySegmentMixed ||
		entry.FileID == 0 ||
		entry.Path == "" ||
		entry.Bytes <= 0 ||
		entry.RefCount == 0 ||
		entry.ProtectedBytes <= 0 ||
		entry.ReclaimableBytes <= 0 ||
		entry.UnknownBytes != 0 {
		return false
	}
	cleanSegmentDir := filepath.Clean(segmentDir)
	cleanPath := filepath.Clean(entry.Path)
	expected := filepath.Clean(filepath.Join(cleanSegmentDir, columnAssetSegmentFileName(entry.FileID)))
	return cleanPath == expected
}

func columnAssetRewriteSourceMaskIncludesManifest(sourceMask columnAssetReachabilitySourceMask) bool {
	return sourceMask&columnAssetRewriteManifestSourceMask() != 0
}

func columnAssetRewriteSourceMaskIsProtectedNonManifest(sourceMask columnAssetReachabilitySourceMask) bool {
	return sourceMask&columnAssetRewriteProtectedNonManifestSourceMask() != 0
}

func columnAssetRewriteManifestSourceMask() columnAssetReachabilitySourceMask {
	return columnAssetReachabilitySourceActiveManifestMask | columnAssetReachabilitySourceRecoveryManifestMask
}

func columnAssetRewriteProtectedNonManifestSourceMask() columnAssetReachabilitySourceMask {
	return columnAssetReachabilityProtectedSourceMask &^ columnAssetRewriteManifestSourceMask()
}

func columnAssetRewriteSameLogicalRef(left, right ColumnAssetRef) bool {
	return left.Kind == right.Kind &&
		left.Namespace == right.Namespace &&
		left.Generation == right.Generation &&
		left.PartID == right.PartID &&
		left.Length == right.Length &&
		left.Checksum == right.Checksum
}

func columnAssetRewritePlanForDetail(plan ColumnAssetReachabilityPlan, detailed bool) ColumnAssetReachabilityPlan {
	if detailed {
		return plan
	}
	plan.Entries = nil
	plan.SegmentEntries = nil
	return plan
}
