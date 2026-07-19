package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
	"github.com/snissn/gomap/TreeDB/node"
)

// ColumnStoreCompactOptions controls logical column-store compaction.
type ColumnStoreCompactOptions struct {
	// ReadIntegrity controls validation while reading existing column assets to
	// materialize the latest-visible row set. The zero value uses the default
	// column-asset read policy.
	ReadIntegrity ColumnAssetReadIntegrity
}

var (
	columnStoreCompactionAfterPrepareHookMu sync.RWMutex
	columnStoreCompactionAfterPrepareHook   func(ColumnPublishPreparedAssets) error
)

func runColumnStoreCompactionAfterPrepareHook(prepared ColumnPublishPreparedAssets) error {
	columnStoreCompactionAfterPrepareHookMu.RLock()
	hook := columnStoreCompactionAfterPrepareHook
	columnStoreCompactionAfterPrepareHookMu.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(prepared)
}

func setColumnStoreCompactionAfterPrepareTestHook(hook func(ColumnPublishPreparedAssets) error) func() {
	columnStoreCompactionAfterPrepareHookMu.Lock()
	previous := columnStoreCompactionAfterPrepareHook
	columnStoreCompactionAfterPrepareHook = hook
	columnStoreCompactionAfterPrepareHookMu.Unlock()
	return func() {
		columnStoreCompactionAfterPrepareHookMu.Lock()
		columnStoreCompactionAfterPrepareHook = previous
		columnStoreCompactionAfterPrepareHookMu.Unlock()
	}
}

// ColumnStoreCompactStats summarizes a logical column-store compaction.
type ColumnStoreCompactStats struct {
	Compacted bool

	PreviousGeneration uint64
	NewGeneration      uint64
	ManifestRoot       uint64
	SystemRoot         uint64

	ManifestRecordsBefore int
	ManifestRecordsAfter  int
	MutationPartsBefore   int
	MutationPartsAfter    int

	RowsScanned       int
	DeletedRows       int
	RowsCompacted     int
	PhysicalBytesRead int64

	AssetsPublished            int
	AggregateMetadataPublished int
	PublishedRefs              []ColumnAssetRef
	SupersededRefs             []ColumnAssetRef
}

type columnStoreCompactionState struct {
	snap           *backenddb.Snapshot
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

// ColumnStoreCompact collapses the current mutation-bearing column manifest
// lineage into one insert-only generation containing exactly latest-visible live
// rows. It rewrites only column assets/manifest metadata; primary document and
// index roots are unchanged.
func (c *Collection) ColumnStoreCompact(ctx context.Context, opts ColumnStoreCompactOptions) (ColumnStoreCompactStats, error) {
	var stats ColumnStoreCompactStats
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
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	unlock := c.lockMutation()
	defer unlock.Unlock()
	return c.columnStoreCompact(ctx, opts)
}

func (c *Collection) columnStoreCompact(ctx context.Context, opts ColumnStoreCompactOptions) (ColumnStoreCompactStats, error) {
	state, closeState, err := c.loadColumnStoreCompactionState(ctx)
	if closeState != nil {
		defer closeState()
	}
	stats := ColumnStoreCompactStats{}
	if err != nil {
		return stats, err
	}
	stats.PreviousGeneration = state.manifest.Generation
	stats.ManifestRecordsBefore = len(state.records)

	if err := columnStoreCompactionRejectUnsupportedManifestRecords(state.records); err != nil {
		return stats, err
	}

	rows, visibilityDiag, err := c.materializeColumnStoreCompactionRows(ctx, state, opts.ReadIntegrity)
	if err != nil {
		return stats, err
	}
	stats.MutationPartsBefore = visibilityDiag.MutationParts
	stats.RowsScanned = visibilityDiag.RowsScanned
	stats.DeletedRows = visibilityDiag.DeletedRows
	stats.PhysicalBytesRead = visibilityDiag.PhysicalBytesScanned
	if visibilityDiag.MutationParts == 0 && state.cfg.PhysicalMutationParts == 0 {
		return stats, nil
	}
	stats.RowsCompacted = len(rows)
	supersededRefs := columnStoreCompactionRefsFromManifestRecords(state.records)
	if err := ctx.Err(); err != nil {
		return stats, err
	}

	prepared, err := c.prepareColumnStoreCompactionAssets(state, rows)
	if err != nil {
		return stats, err
	}
	releasePrepared := func() {
		if prepared.stableResources != nil {
			prepared.stableResources.Release()
			prepared.stableResources = nil
		}
	}
	defer releasePrepared()
	cleanupPrepared := func(baseErr error) error {
		releasePrepared()
		if !prepared.stableResourcesRequired {
			if cleanupErr := cleanupColumnPreparedAssets(c.db.ColumnAssetRootDir(), prepared.Assets); cleanupErr != nil {
				return errors.Join(baseErr, cleanupErr)
			}
		}
		// Stable-authoritative compaction assets remain as reachability-GC
		// orphans. After releasing authority a pathname truncate could hit a
		// rebound segment. The legacy pre-cutover path keeps its old cleanup.
		return baseErr
	}
	if prepared.stableResourcesRequired || prepared.stableResources != nil {
		if err := validateStableColumnResourcesMatchPrepared(prepared.Assets, prepared.stableResources); err != nil {
			return stats, cleanupPrepared(err)
		}
	}
	if err := runColumnStoreCompactionAfterPrepareHook(prepared); err != nil {
		return stats, cleanupPrepared(err)
	}
	if err := ctx.Err(); err != nil {
		return stats, cleanupPrepared(err)
	}
	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        state.meta.Name,
		ColumnStore:       state.cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: state.cfg.RecoveryAuthoritativeAppliedCommandLSN,
		CurrentManifest:   state.cfg.ActiveManifest,
		Prepared:          prepared,
	})
	if err != nil {
		return stats, cleanupPrepared(fmt.Errorf("collections: column store compaction manifest encode failed: %w", err))
	}
	stats.NewGeneration = manifest.Identity.Generation
	stats.ManifestRecordsAfter = len(manifest.Records)

	updatedMeta, err := columnStoreCompactionUpdatedMeta(state.meta, manifest.Identity)
	if err != nil {
		return stats, cleanupPrepared(err)
	}
	policy, err := columnStoreCompactionManifestRootStoragePolicy(state)
	if err != nil {
		return stats, cleanupPrepared(err)
	}
	if err := ctx.Err(); err != nil {
		return stats, cleanupPrepared(err)
	}
	identityRecord := encodeColumnManifestIdentityRecordArray(manifest.Identity)
	deltaIter := columnStoreCompactionManifestDeltaIterator(identityRecord, state.records, manifest.Records)
	durableRequirements, err := stableColumnManifestDurableRequirements(manifest.Records, manifest.Identity.Generation, state.cfg.AssetManager.Namespace)
	if err != nil {
		_ = deltaIter.Close()
		return stats, cleanupPrepared(fmt.Errorf("collections: compaction durable requirements: %w", err))
	}
	rootNames := []string{state.rootName}
	baseRootIDs := map[string]uint64{state.rootName: state.baseRoot}
	var locatorBaseRoot uint64
	var locatorPolicy backenddb.OrderedRootStoragePolicy
	var locatorIter iterator.UnsafeIterator
	if len(rows) > 0 {
		locatorRootName := collectionColumnRowLocatorRootName(state.meta.Name)
		locatorBaseRoot = state.catalog.rootID(locatorRootName)
		locatorPolicy, err = collectionRootStoragePolicyForDB(c.db, state.meta, locatorRootName)
		if err != nil {
			_ = deltaIter.Close()
			return stats, cleanupPrepared(err)
		}
		locatorDocuments := make([]columnWriteDocument, len(rows))
		for i := range rows {
			locatorDocuments[i].ID = rows[i].ID
		}
		locatorTable, err := buildColumnPrimaryRowLocatorTable(ColumnPublishPlan{
			Operation:             ColumnPublishOperationInsert,
			AppliedCommandLSN:     state.cfg.RecoveryAuthoritativeAppliedCommandLSN,
			UpdatedActiveManifest: manifest.Identity,
			Rows:                  len(rows),
		}, locatorDocuments)
		if err != nil {
			_ = deltaIter.Close()
			return stats, cleanupPrepared(err)
		}
		locatorIter = locatorTable.NewIterator(nil, nil)
		rootNames = append(rootNames, locatorRootName)
		baseRootIDs[locatorRootName] = locatorBaseRoot
	}
	preflight := c.columnStoreCompactionRootDescriptorPreflight(state, rootNames, baseRootIDs)
	// The maintenance publisher owns and releases the producer closure once it
	// is attached to the candidate durable slot.
	durableResources := prepared.stableResources
	prepared.stableResources = nil
	orderedInputs := []backenddb.StorageMaintenanceRootDeltaPublishInput{{
		BaseRoot:                    state.baseRoot,
		Iter:                        deltaIter,
		StoragePolicy:               policy,
		DurableResources:            durableResources,
		DurableResourceRequirements: durableRequirements,
	}}
	if locatorIter != nil {
		orderedInputs = append(orderedInputs, backenddb.StorageMaintenanceRootDeltaPublishInput{
			BaseRoot:      locatorBaseRoot,
			Iter:          locatorIter,
			StoragePolicy: locatorPolicy,
		})
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		orderedInputs,
		preflight,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildColumnAssetRewriteSystemDeltaIteratorForMeta(state.meta, updatedMeta, state.baseCommitSeq, state.baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		},
	)
	if err != nil {
		if columnAssetRewritePublishFailedBeforeApply(err) {
			return stats, cleanupPrepared(err)
		}
		return stats, err
	}
	if len(rootIDs) != len(rootNames) {
		return stats, unexpectedOrderedRootCountError(state.meta.Name, len(rootNames), len(rootIDs))
	}
	for _, rootID := range rootIDs {
		if rootID == 0 {
			return stats, unexpectedOrderedRootCountError(state.meta.Name, len(rootNames), len(rootIDs))
		}
	}
	stats.AssetsPublished = len(prepared.Assets)
	stats.AggregateMetadataPublished = columnStoreCompactionAggregateMetadataAssets(prepared.Assets)
	stats.PublishedRefs = columnStoreCompactionPreparedRefs(prepared.Assets)
	stats.SupersededRefs = supersededRefs
	stats.Compacted = true
	stats.ManifestRoot = rootIDs[0]
	stats.SystemRoot = newSystemRoot
	stats.MutationPartsAfter = 0
	nextCatalog := cloneCatalogWithRootUpdates(state.catalog, updatedMeta, rootNames, rootIDs)
	c.meta = updatedMeta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return stats, nil
}

func (c *Collection) loadColumnStoreCompactionState(ctx context.Context) (columnStoreCompactionState, func(), error) {
	if err := ctx.Err(); err != nil {
		return columnStoreCompactionState{}, nil, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return columnStoreCompactionState{}, nil, backenddb.ErrClosed
	}
	closeState := func() { _ = snap.Close() }
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		closeState()
		return columnStoreCompactionState{}, nil, err
	}
	if catalog == nil {
		closeState()
		return columnStoreCompactionState{}, nil, errCollectionNotFound
	}
	meta := catalog.meta
	if meta.Options.ColumnStore == nil || !meta.Options.ColumnStore.Enabled {
		closeState()
		return columnStoreCompactionState{}, nil, errors.New("collections: column store compaction requires enabled column_store")
	}
	cfg := meta.Options.ColumnStore.copy()
	if cfg.ActiveManifest == nil {
		closeState()
		return columnStoreCompactionState{}, nil, errors.New("collections: column store compaction requires active column manifest")
	}
	if cfg.RecoveryAuthoritativeManifest == nil {
		closeState()
		return columnStoreCompactionState{}, nil, errors.New("collections: column store compaction requires recovery-authoritative column manifest")
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		closeState()
		return columnStoreCompactionState{}, nil, errors.New("collections: column store compaction requires active recovery-authoritative manifest")
	}
	if cfg.AssetManager == nil {
		closeState()
		return columnStoreCompactionState{}, nil, errors.New("collections: column store compaction requires column asset manager metadata")
	}
	rootName := collectionColumnManifestRootName(meta.Name)
	if cfg.ManifestRoot != nil && cfg.ManifestRoot.Name != "" {
		rootName = cfg.ManifestRoot.Name
	}
	baseRoot := catalog.rootID(rootName)
	if baseRoot == 0 {
		closeState()
		return columnStoreCompactionState{}, nil, fmt.Errorf("collections: column store compaction missing manifest root %q", rootName)
	}
	if err := validateColumnManifestIdentityAtRoot(snap, baseRoot, *cfg.ActiveManifest); err != nil {
		closeState()
		return columnStoreCompactionState{}, nil, err
	}
	if cfg.RecoveryAuthoritativeAppliedCommandLSN == 0 {
		closeState()
		return columnStoreCompactionState{}, nil, errors.New("collections: column store compaction requires recovery-authoritative AppliedCommandLSN")
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, baseRoot)
	if err != nil {
		closeState()
		return columnStoreCompactionState{}, nil, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		closeState()
		return columnStoreCompactionState{}, nil, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, cfg, *cfg.ActiveManifest, meta.Name, "column store compaction"); err != nil {
		closeState()
		return columnStoreCompactionState{}, nil, err
	}
	return columnStoreCompactionState{
		snap:           snap,
		catalog:        catalog,
		meta:           meta,
		cfg:            cfg,
		rootName:       rootName,
		baseRoot:       baseRoot,
		baseCommitSeq:  snapshotCommitSeq(snap),
		baseSystemRoot: snapshotSystemRoot(snap),
		manifest:       manifest,
		records:        records,
	}, closeState, nil
}

func (c *Collection) materializeColumnStoreCompactionRows(ctx context.Context, state columnStoreCompactionState, readIntegrity ColumnAssetReadIntegrity) ([]columnDeclaredRow, columnPhysicalScanDiagnostics, error) {
	visible, err := c.scanColumnPhysicalVisibleRowsAtSnapshotWithReadIntegrity(state.snap, state.catalog, state.meta.Name, state.baseRoot, state.cfg, true, nil, readIntegrity)
	if err != nil {
		return nil, visible.Diagnostics, err
	}
	if err := ctx.Err(); err != nil {
		return nil, visible.Diagnostics, err
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), state.cfg.AssetManager.Namespace, readIntegrity)
	if err != nil {
		return nil, visible.Diagnostics, err
	}
	defer func() { _ = readCache.close() }()
	typedCache := typedColumnPartReconstructionCache{ReadCache: &readCache}
	rows := make([]columnDeclaredRow, 0, len(visible.Rows))
	var typedScratch []columnDeclaredValue
	var fullScratch []columnDeclaredValue
	for _, row := range visible.Rows {
		if err := ctx.Err(); err != nil {
			return nil, visible.Diagnostics, err
		}
		if row.Deleted {
			continue
		}
		typedValues, err := c.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(state.snap, state.baseRoot, state.cfg, row, &typedCache, typedScratch)
		if err != nil {
			return nil, visible.Diagnostics, err
		}
		typedScratch = typedValues.Values
		fullValues, err := mergeColumnReconstructionValuesInto(state.cfg, row.Values, typedValues.Values, fullScratch)
		if err != nil {
			return nil, visible.Diagnostics, err
		}
		fullScratch = fullValues
		rows = append(rows, columnDeclaredRow{
			ID:     bytes.Clone(row.ID),
			Values: cloneColumnDeclaredValues(fullValues),
		})
	}
	return rows, visible.Diagnostics, nil
}

func (c *Collection) prepareColumnStoreCompactionAssets(state columnStoreCompactionState, rows []columnDeclaredRow) (ColumnPublishPreparedAssets, error) {
	prepared := ColumnPublishPreparedAssets{RowCount: len(rows)}
	if len(rows) == 0 {
		return prepared, nil
	}
	input := columnWritePublishInput{
		meta:              state.meta,
		operation:         ColumnPublishOperationInsert,
		rows:              len(rows),
		declaredRows:      rows,
		declaredRowsReady: true,
	}
	return c.prepareColumnPhysicalAssetRowsForCommand(prepared, input, ColumnPublishAssetPrepareInput{
		Collection:        state.meta.Name,
		ColumnStore:       state.cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: state.cfg.RecoveryAuthoritativeAppliedCommandLSN,
		CurrentManifest:   state.cfg.ActiveManifest,
	}, rows)
}

func columnStoreCompactionUpdatedMeta(base CollectionMeta, identity ColumnManifestIdentity) (CollectionMeta, error) {
	if base.Options.ColumnStore == nil || !base.Options.ColumnStore.Enabled {
		return CollectionMeta{}, errors.New("collections: column store compaction requires enabled column_store")
	}
	updated := copyCollectionMeta(base)
	cfg := updated.Options.ColumnStore.copy()
	active := identity
	recovery := identity
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	cfg.PhysicalMutationParts = 0
	updated.Options.ColumnStore = &cfg
	return normalizeCollectionMeta(updated)
}

func columnStoreCompactionManifestRootStoragePolicy(state columnStoreCompactionState) (backenddb.OrderedRootStoragePolicy, error) {
	if state.cfg.ManifestRoot == nil {
		return backenddb.OrderedRootStorageDefault, nil
	}
	return backendRootStoragePolicy(state.cfg.ManifestRoot.StoragePolicy)
}

func cleanupColumnPreparedAssets(rootDir string, assets []ColumnPreparedAsset) error {
	type cleanupTarget struct {
		path       string
		truncateTo int64
		maxEnd     int64
		remove     bool
	}
	targets := make(map[string]*cleanupTarget)
	var cleanupErrs []error
	for _, asset := range assets {
		ref := asset.Ref
		if ref.Namespace == "" || ref.FileID == 0 {
			continue
		}
		assetPath, err := columnAssetSegmentPath(rootDir, ref)
		if err != nil {
			cleanupErrs = append(cleanupErrs, err)
			continue
		}
		target := targets[assetPath]
		if target == nil {
			target = &cleanupTarget{path: assetPath, truncateTo: ref.Offset, maxEnd: ref.Offset + ref.Length}
			targets[assetPath] = target
		}
		if ref.Offset < target.truncateTo {
			target.truncateTo = ref.Offset
		}
		if end := ref.Offset + ref.Length; end > target.maxEnd {
			target.maxEnd = end
		}
	}
	for _, target := range targets {
		lock := columnAssetSegmentWriteLock(target.path)
		lock.Lock()
		info, err := os.Stat(target.path)
		if err != nil {
			lock.Unlock()
			if os.IsNotExist(err) {
				continue
			}
			cleanupErrs = append(cleanupErrs, err)
			continue
		}
		if info.Size() != target.maxEnd {
			lock.Unlock()
			// Another writer appended to the shared segment after these assets were
			// prepared. Leave the orphaned bytes for reachability/GC rather than
			// truncating potentially live refs from the winning manifest.
			continue
		}
		if target.remove {
			if err := os.Remove(target.path); err != nil && !os.IsNotExist(err) {
				cleanupErrs = append(cleanupErrs, err)
				lock.Unlock()
				continue
			}
		} else if err := os.Truncate(target.path, target.truncateTo); err != nil && !os.IsNotExist(err) {
			cleanupErrs = append(cleanupErrs, err)
			lock.Unlock()
			continue
		}
		lock.Unlock()
		if err := syncColumnAssetDir(filepath.Dir(target.path)); err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}
	}
	return errors.Join(cleanupErrs...)
}

func (c *Collection) columnStoreCompactionRootDescriptorPreflight(state columnStoreCompactionState, rootNames []string, baseRootIDs map[string]uint64) backenddb.OrderedRootGroupPreflight {
	return func() error {
		if err := c.validateRootDescriptorSystemDeltaForMeta(state.meta, state.baseCommitSeq, state.baseSystemRoot, rootNames, baseRootIDs); err != nil {
			return errors.Join(errColumnAssetRewritePublishPreflightFailed, err)
		}
		return nil
	}
}

func columnStoreCompactionManifestDeltaIterator(identityRecord [columnManifestIdentityRecordSize]byte, oldRecords, newRecords []columnManifestRecord) *systemTargetIterator {
	entries := make([]systemTargetEntry, 0, 1+len(newRecords)+len(oldRecords))
	identityValue := make([]byte, len(identityRecord))
	copy(identityValue, identityRecord[:])
	entries = append(entries, systemTargetEntry{key: newColumnManifestIdentityRecordKey(), value: identityValue})
	newKeys := make(map[string]struct{}, len(newRecords))
	for _, record := range newRecords {
		newKeys[string(record.key)] = struct{}{}
		entries = append(entries, systemTargetEntry{key: bytes.Clone(record.key), value: bytes.Clone(record.value)})
	}
	for _, record := range oldRecords {
		if _, keep := newKeys[string(record.key)]; keep {
			continue
		}
		entries = append(entries, systemTargetEntry{key: bytes.Clone(record.key), flags: node.FlagTombstone})
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	return &systemTargetIterator{entries: entries}
}

func columnStoreCompactionRejectUnsupportedManifestRecords(records []columnManifestRecord) error {
	for _, record := range records {
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) || bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
			return fmt.Errorf("%w: column store compaction with vector graph/index state records is not supported in C5", ErrColumnQueryPlanUnsupported)
		}
	}
	return nil
}

func columnStoreCompactionRefsFromManifestRecords(records []columnManifestRecord) []ColumnAssetRef {
	refs := make([]ColumnAssetRef, 0, len(records))
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) &&
			!bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes) &&
			!bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes) &&
			!bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			continue
		}
		refs = append(refs, part.AssetRef)
	}
	return refs
}

func columnStoreCompactionPreparedRefs(assets []ColumnPreparedAsset) []ColumnAssetRef {
	if len(assets) == 0 {
		return nil
	}
	refs := make([]ColumnAssetRef, len(assets))
	for i, asset := range assets {
		refs[i] = asset.Ref
	}
	return refs
}

func columnStoreCompactionAggregateMetadataAssets(assets []ColumnPreparedAsset) int {
	count := 0
	for _, asset := range assets {
		if asset.Ref.Kind == ColumnAssetKindTCS1AggregateMetadata {
			count++
		}
	}
	return count
}
