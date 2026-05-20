package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

var errColumnAssetRewritePublishPreflightFailed = errors.New("collections: column asset rewrite publish preflight failed")

// ColumnAssetRewriteOptions controls M15C mixed-segment rewrite/remap.
type ColumnAssetRewriteOptions struct {
	DryRun bool
	// Detailed keeps detailed ref and segment entries in the returned plan.
	Detailed      bool
	CandidateRefs []ColumnAssetRef
	PendingRefs   []ColumnAssetRef
	PreparedRefs  []ColumnAssetRef
	PinnedRefs    []ColumnAssetRef
}

type columnAssetRewriteOptions struct {
	ColumnAssetRewriteOptions
	afterCopyHookForTest       func() error
	afterPrePublishHookForTest func() error
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
	plan, sourceMasks, err := c.planColumnAssetReachability(ctx, columnAssetReachabilityOptionsInternal{
		ColumnAssetReachabilityOptions: ColumnAssetReachabilityOptions{
			Detailed:       opts.Detailed,
			SegmentDetails: true,
			CandidateRefs:  opts.CandidateRefs,
			PendingRefs:    opts.PendingRefs,
			PreparedRefs:   opts.PreparedRefs,
			PinnedRefs:     opts.PinnedRefs,
		},
		omitDetailedEntrySources: !opts.Detailed,
		omitDetailedEntrySort:    !opts.Detailed,
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
	if opts.afterPrePublishHookForTest != nil {
		if err := opts.afterPrePublishHookForTest(); err != nil {
			stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
			return stats, cleanupRemap(err)
		}
	}

	patchedRecords, patched, err := patchColumnAssetRewriteManifestRecordsInPlace(state.records, remap.byOldRef, state.cfg.AssetManager.Namespace)
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
		if columnAssetRewritePublishFailedBeforeApply(err) {
			stats.Plan = columnAssetRewritePlanForDetail(stats.Plan, opts.Detailed)
			return stats, cleanupRemap(err)
		}
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

func columnAssetRewritePublishFailedBeforeApply(err error) bool {
	return errors.Is(err, errColumnAssetRewritePublishPreflightFailed) ||
		errors.Is(err, backenddb.ErrStorageMaintenancePublishPreApplyFailed)
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
	return appender.abort()
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
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		oldRef, offsets, err := columnAssetRewriteManifestPartRefForPatch(record.value, expectedNamespace)
		if err != nil {
			return nil, 0, err
		}
		keyGeneration, keyPartID, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
		if err != nil {
			return nil, 0, err
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
	if version := cur.u16(); version != columnManifestRecordVersion {
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
	bytes64 := cur.u64()
	_ = cur.u64() // publish_id; rewrite preserves the original field bytes.
	_ = cur.u64() // generation_id; rewrite preserves the original field bytes.
	_ = cur.stringBytes()
	if err := cur.err; err != nil {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, err
	}
	if cur.pos != len(raw) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: trailing bytes in column manifest part record")
	}
	if !columnPhysicalBytesEqualString(kindBytes, string(ColumnAssetKindTCS1PartImage)) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, fmt.Errorf("collections: unsupported column manifest part asset kind %q", string(kindBytes))
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
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || bytes64 > uint64(math.MaxInt64) {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, errors.New("collections: column manifest part offsets or byte counts overflow int64")
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  expectedNamespace,
		Generation: generation,
		PartID:     partID,
		FileID:     uint32(fileID64),
		Offset:     int64(offset64),
		Length:     int64(length64),
		Checksum:   uint32(checksum64),
	}
	if err := validateColumnPreparedAssetForPlan(ColumnPreparedAsset{Ref: ref, Bytes: int64(bytes64)}); err != nil {
		return ColumnAssetRef{}, columnAssetRewriteManifestPartPatchOffsets{}, err
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
	policy, err := columnAssetRewriteManifestRootStoragePolicy(state)
	if err != nil {
		return 0, nil, err
	}
	identityRecord := encodeColumnManifestIdentityRecordArray(updatedIdentity)
	ordered := []backenddb.StorageMaintenanceRootDeltaPublishInput{{
		BaseRoot:      state.baseRoot,
		Iter:          columnManifestRootRecordIteratorOwned(identityRecord, records),
		StoragePolicy: policy,
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
