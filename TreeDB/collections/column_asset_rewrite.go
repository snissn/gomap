package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

// ColumnAssetRewriteOptions controls M15C mixed-segment rewrite/remap.
type ColumnAssetRewriteOptions struct {
	DryRun bool
	// Detailed keeps detailed ref and segment entries in the returned plan.
	Detailed      bool
	CandidateRefs []ColumnAssetRef
	PendingRefs   []ColumnAssetRef
	PreparedRefs  []ColumnAssetRef
	PinnedRefs    []ColumnAssetRef

	afterCopyHookForTest func() error
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
	BytesCopied        int64
	BytesReclaimable   int64
	BytesRetained      int64
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

func (c *Collection) columnAssetRewrite(ctx context.Context, opts ColumnAssetRewriteOptions) (ColumnAssetRewriteStats, error) {
	plan, err := c.PlanColumnAssetReachability(ctx, ColumnAssetReachabilityOptions{
		Detailed:       true,
		SegmentDetails: true,
		CandidateRefs:  opts.CandidateRefs,
		PendingRefs:    opts.PendingRefs,
		PreparedRefs:   opts.PreparedRefs,
		PinnedRefs:     opts.PinnedRefs,
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
		return stats, fmt.Errorf("%w: collection=%q namespace=%q unknown_segments=%d missing_segments=%d out_of_bounds_refs=%d",
			ErrColumnAssetReachabilityIncomplete,
			plan.Collection,
			plan.Namespace,
			plan.Segments.Unknown,
			plan.Segments.Missing,
			plan.Segments.OutOfBoundsRefs,
		)
	}

	namespace, err := columnAssetManagerNamespaceForRoot(c.db.ColumnAssetRootDir(), plan.Namespace)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	segments := columnAssetRewriteEligibleSegments(namespace.SegmentDir, plan)
	refs := columnAssetRewriteEligibleRefs(plan, segments)
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
	remap, err := c.copyColumnAssetRewriteRefs(ctx, state.cfg, refs)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if len(remap.oldRefs) == 0 {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, nil
	}
	cleanupRemap := func(baseErr error) error {
		if cleanupErr := cleanupColumnAssetRewriteCopiedSegment(c.db.ColumnAssetRootDir(), remap); cleanupErr != nil {
			return errors.Join(baseErr, cleanupErr)
		}
		return baseErr
	}
	if opts.afterCopyHookForTest != nil {
		if err := opts.afterCopyHookForTest(); err != nil {
			stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
			return stats, cleanupRemap(err)
		}
	}
	if err := ctx.Err(); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, cleanupRemap(err)
	}
	// This preflight still runs before the publish attempt, so copied segments
	// can be safely removed when the root descriptor has already moved.
	if err := c.columnAssetRewriteRootDescriptorPreflight(state)(); err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, cleanupRemap(err)
	}

	patchedRecords, patched, err := patchColumnAssetRewriteManifestRecords(state.records, remap.byOldRef)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, cleanupRemap(err)
	}
	if patched != len(remap.oldRefs) {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, cleanupRemap(fmt.Errorf("collections: column asset rewrite patched %d manifest refs, want %d", patched, len(remap.oldRefs)))
	}
	updatedIdentity, err := columnAssetRewriteUpdatedIdentity(state, patchedRecords)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, cleanupRemap(err)
	}
	updatedMeta, err := columnAssetRewriteUpdatedMeta(state.meta, updatedIdentity)
	if err != nil {
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, cleanupRemap(err)
	}
	newSystemRoot, rootIDs, err := c.publishColumnAssetRewriteManifestState(state, updatedMeta, updatedIdentity, patchedRecords)
	if err != nil {
		// Publish errors can be ambiguous after root/system application starts;
		// retain the copied segment as fail-closed maintenance debt.
		stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
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
	stats.SupersededRefs = append(stats.SupersededRefs, remap.oldRefs...)
	stats.RemappedRefs = append(stats.RemappedRefs, remap.newRefs...)
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
	oldRefs       []ColumnAssetRef
	newRefs       []ColumnAssetRef
	byOldRef      map[ColumnAssetRef]ColumnAssetRef
	segmentFileID uint32
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
		records:        cloneColumnManifestRecords(records),
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
	appender, err := newNextColumnPhysicalAssetSegmentAppender(c.db.ColumnAssetRootDir(), cfg)
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
		newRef, err := appender.append(raw, oldRef.Generation, oldRef.PartID)
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
	closed = true
	return out, nil
}

func cleanupColumnAssetRewriteOpenAppender(appender *columnPhysicalAssetSegmentAppender) error {
	if appender == nil {
		return nil
	}
	// Abandoned partial copies are removed immediately; syncing the file before
	// deletion would add write amplification without strengthening recovery.
	closeErr := appender.abort()
	removeErr := os.Remove(appender.assetPath)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	syncErr := syncColumnAssetDir(appender.namespace.SegmentDir)
	return errors.Join(closeErr, removeErr, syncErr)
}

func cleanupColumnAssetRewriteCopiedSegment(rootDir string, remap columnAssetRewriteCopyResult) error {
	if len(remap.newRefs) == 0 {
		return nil
	}
	segmentPath, err := columnAssetSegmentPath(rootDir, remap.newRefs[0])
	if err != nil {
		return err
	}
	removeErr := os.Remove(segmentPath)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	syncErr := syncColumnAssetDir(filepath.Dir(segmentPath))
	return errors.Join(removeErr, syncErr)
}

func validateColumnAssetRewriteRefKinds(refs []ColumnAssetRef) error {
	for idx, ref := range refs {
		if ref.Kind != ColumnAssetKindTCS1PartImage {
			return fmt.Errorf("collections: column asset rewrite supports only %s refs: ref %d kind %q", ColumnAssetKindTCS1PartImage, idx, ref.Kind)
		}
	}
	return nil
}

func patchColumnAssetRewriteManifestRecords(records []columnManifestRecord, byOldRef map[ColumnAssetRef]ColumnAssetRef) ([]columnManifestRecord, int, error) {
	patched := cloneColumnManifestRecords(records)
	count := 0
	for i := range patched {
		if !bytes.HasPrefix(patched[i].key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(patched[i].value)
		if err != nil {
			return nil, 0, err
		}
		newRef, ok := byOldRef[part.AssetRef]
		if !ok {
			continue
		}
		value, err := encodeColumnManifestPartRecord(ColumnPreparedAsset{
			Ref:          newRef,
			Bytes:        part.Bytes,
			PublishID:    part.PublishID,
			GenerationID: part.GenerationID,
			Reason:       part.Reason,
		})
		if err != nil {
			return nil, 0, err
		}
		patched[i].value = value
		count++
	}
	sortColumnManifestRecords(patched)
	return patched, count, nil
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

func (c *Collection) publishColumnAssetRewriteManifestState(state columnAssetRewriteManifestState, updatedMeta CollectionMeta, updatedIdentity ColumnManifestIdentity, records []columnManifestRecord) (uint64, []uint64, error) {
	if state.cfg.ManifestRoot == nil {
		return 0, nil, errors.New("collections: column asset rewrite requires manifest root descriptor")
	}
	policy, err := backendRootStoragePolicy(state.cfg.ManifestRoot.StoragePolicy)
	if err != nil {
		return 0, nil, err
	}
	identityRecord := encodeColumnManifestIdentityRecordArray(updatedIdentity)
	ordered := []backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:                  state.baseRoot,
		Iter:                      columnManifestRootRecordIterator(identityRecord, records),
		StoragePolicy:             policy,
		StorageMaintenanceRewrite: true,
	}}
	return c.db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewrite(),
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
		return c.validateRootDescriptorSystemDeltaForMeta(state.meta, state.baseCommitSeq, state.baseSystemRoot, rootNames, baseRootIDs)
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

func columnAssetRewriteEligibleSegments(segmentDir string, plan ColumnAssetReachabilityPlan) map[uint32]ColumnAssetReachabilitySegmentEntry {
	eligible := make(map[uint32]ColumnAssetReachabilitySegmentEntry, plan.Segments.Mixed)
	for _, entry := range plan.SegmentEntries {
		if columnAssetRewriteSegmentEligible(segmentDir, entry) {
			eligible[entry.FileID] = entry
		}
	}
	for _, entry := range plan.Entries {
		if entry.Status != ColumnAssetReachabilityProtected {
			continue
		}
		if _, ok := eligible[entry.Ref.FileID]; !ok {
			continue
		}
		if columnAssetRewriteSourcesIncludeManifest(entry.Sources) {
			continue
		}
		delete(eligible, entry.Ref.FileID)
	}
	return eligible
}

func columnAssetRewriteEligibleRefs(plan ColumnAssetReachabilityPlan, segments map[uint32]ColumnAssetReachabilitySegmentEntry) []ColumnAssetRef {
	refs := make([]ColumnAssetRef, 0, len(plan.Entries))
	for _, entry := range plan.Entries {
		if entry.Status != ColumnAssetReachabilityProtected {
			continue
		}
		if _, ok := segments[entry.Ref.FileID]; !ok {
			continue
		}
		if !columnAssetRewriteSourcesIncludeManifest(entry.Sources) {
			continue
		}
		refs = append(refs, entry.Ref)
	}
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

func columnAssetRewriteSourcesIncludeManifest(sources []ColumnAssetReachabilitySource) bool {
	for _, source := range sources {
		if source == ColumnAssetReachabilitySourceActiveManifest || source == ColumnAssetReachabilitySourceRecoveryManifest {
			return true
		}
	}
	return false
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
