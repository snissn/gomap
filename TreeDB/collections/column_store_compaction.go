package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

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
	stats.RowsCompacted = len(rows)
	stats.PhysicalBytesRead = visibilityDiag.PhysicalBytesScanned
	if visibilityDiag.MutationParts == 0 && state.cfg.PhysicalMutationParts == 0 {
		return stats, nil
	}
	stats.SupersededRefs = columnStoreCompactionRefsFromManifestRecords(state.records)
	if state.cfg.RecoveryAuthoritativeAppliedCommandLSN == 0 {
		return stats, errors.New("collections: column store compaction requires recovery-authoritative AppliedCommandLSN")
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}

	prepared, err := c.prepareColumnStoreCompactionAssets(state, rows)
	if err != nil {
		return stats, err
	}
	stats.AssetsPublished = len(prepared.Assets)
	stats.AggregateMetadataPublished = columnStoreCompactionAggregateMetadataAssets(prepared.Assets)
	stats.PublishedRefs = columnStoreCompactionPreparedRefs(prepared.Assets)

	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        state.meta.Name,
		ColumnStore:       state.cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: state.cfg.RecoveryAuthoritativeAppliedCommandLSN,
		CurrentManifest:   state.cfg.ActiveManifest,
		Prepared:          prepared,
	})
	if err != nil {
		return stats, fmt.Errorf("collections: column store compaction manifest encode failed: %w", err)
	}
	stats.NewGeneration = manifest.Identity.Generation
	stats.ManifestRecordsAfter = len(manifest.Records)

	updatedMeta, err := columnStoreCompactionUpdatedMeta(state.meta, manifest.Identity)
	if err != nil {
		return stats, err
	}
	policy, err := columnStoreCompactionManifestRootStoragePolicy(state)
	if err != nil {
		return stats, err
	}
	identityRecord := encodeColumnManifestIdentityRecordArray(manifest.Identity)
	deltaIter := columnStoreCompactionManifestDeltaIterator(identityRecord, state.records, manifest.Records)
	rootNames := []string{state.rootName}
	baseRootIDs := map[string]uint64{state.rootName: state.baseRoot}
	preflight := c.columnStoreCompactionRootDescriptorPreflight(state, rootNames, baseRootIDs)
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]backenddb.StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot:      state.baseRoot,
			Iter:          deltaIter,
			StoragePolicy: policy,
		}},
		preflight,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildColumnAssetRewriteSystemDeltaIteratorForMeta(state.meta, updatedMeta, state.baseCommitSeq, state.baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		},
	)
	if err != nil {
		return stats, err
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return stats, unexpectedOrderedRootCountError(state.meta.Name, 1, len(rootIDs))
	}
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
		return backenddb.OrderedRootStorageDefault, errors.New("collections: column store compaction requires manifest root descriptor")
	}
	return backendRootStoragePolicy(state.cfg.ManifestRoot.StoragePolicy)
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
