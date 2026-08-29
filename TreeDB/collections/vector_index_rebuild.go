package collections

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	columnVectorGraphNeighborInsertionLimit = 32
	maxColumnVectorGraphAdjacencyOrdinal    = uint64(^uint32(0))
	columnVectorGraphLayeredAdjacencyMagic  = ^uint32(0)
)

const vectorPartitionConstructionCandidateSampleLimitV1 = 32
const vectorPartitionConstructionCandidateCaptureLimitV1 = 33

func vectorPartitionConstructionSampleIDsV1(rows []columnVectorGraphAssetRow) map[string]struct{} {
	type item struct {
		id   string
		hash [32]byte
	}
	items := make([]item, len(rows))
	for i := range rows {
		items[i] = item{id: string(rows[i].ID), hash: vectorPartitionConstructionSampleHashV1(rows[i].ID)}
	}
	sort.Slice(items, func(i, j int) bool {
		return vectorPartitionConstructionSampleLessV1(items[i].hash, []byte(items[i].id), items[j].hash, []byte(items[j].id))
	})
	if len(items) > vectorPartitionConstructionCandidateCaptureLimitV1 {
		items = items[:vectorPartitionConstructionCandidateCaptureLimitV1]
	}
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		out[item.id] = struct{}{}
	}
	return out
}

func vectorPartitionConstructionSampleHashV1(id []byte) [32]byte {
	return sha256.Sum256(append([]byte("treedb-4170-candidate-sample-v1/"), id...))
}

func vectorPartitionConstructionSampleLessV1(leftHash [32]byte, leftID []byte, rightHash [32]byte, rightID []byte) bool {
	if cmp := bytes.Compare(leftHash[:], rightHash[:]); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(leftID, rightID) < 0
}

var (
	errColumnVectorGraphInvNormEmpty          = errors.New("collections: column_graph vector is empty")
	errColumnVectorGraphInvNormValueNotFinite = errors.New("collections: column_graph vector value is not finite")
	errColumnVectorGraphInvNormNormInvalid    = errors.New("collections: column_graph vector norm must be finite and non-zero")
	errColumnVectorGraphInvNormOutOfRange     = errors.New("collections: column_graph vector inverse norm must be finite and fit float32")
)

var columnVectorGraphCanonicalRowsTestHook struct {
	sync.RWMutex
	hook func()
}

var columnVectorGraphRebuildBeforeBuildTestHook struct {
	sync.RWMutex
	hook func()
}

func setColumnVectorGraphCanonicalRowsTestHook(hook func()) func() {
	columnVectorGraphCanonicalRowsTestHook.Lock()
	previous := columnVectorGraphCanonicalRowsTestHook.hook
	columnVectorGraphCanonicalRowsTestHook.hook = hook
	columnVectorGraphCanonicalRowsTestHook.Unlock()
	return func() {
		columnVectorGraphCanonicalRowsTestHook.Lock()
		columnVectorGraphCanonicalRowsTestHook.hook = previous
		columnVectorGraphCanonicalRowsTestHook.Unlock()
	}
}

func runColumnVectorGraphCanonicalRowsTestHook() {
	columnVectorGraphCanonicalRowsTestHook.RLock()
	hook := columnVectorGraphCanonicalRowsTestHook.hook
	columnVectorGraphCanonicalRowsTestHook.RUnlock()
	if hook != nil {
		hook()
	}
}

func setColumnVectorGraphRebuildBeforeBuildTestHook(hook func()) func() {
	columnVectorGraphRebuildBeforeBuildTestHook.Lock()
	previous := columnVectorGraphRebuildBeforeBuildTestHook.hook
	columnVectorGraphRebuildBeforeBuildTestHook.hook = hook
	columnVectorGraphRebuildBeforeBuildTestHook.Unlock()
	return func() {
		columnVectorGraphRebuildBeforeBuildTestHook.Lock()
		columnVectorGraphRebuildBeforeBuildTestHook.hook = previous
		columnVectorGraphRebuildBeforeBuildTestHook.Unlock()
	}
}

func runColumnVectorGraphRebuildBeforeBuildTestHook() {
	columnVectorGraphRebuildBeforeBuildTestHook.RLock()
	hook := columnVectorGraphRebuildBeforeBuildTestHook.hook
	columnVectorGraphRebuildBeforeBuildTestHook.RUnlock()
	if hook != nil {
		hook()
	}
}

func takeColumnVectorGraphDurablePublication(prepared *columnVectorGraphPreparedPhysicalAsset, records []columnManifestRecord, activeGeneration uint64, namespace string) (*rootpublication.StableResourceSet, rootpublication.StableLogicalObligationRequirements, error) {
	if err := runColumnVectorGraphStablePublishTestHook(prepared); err != nil {
		return nil, rootpublication.StableLogicalObligationRequirements{}, err
	}
	requirements, err := stableColumnManifestDurableRequirements(records, activeGeneration, namespace)
	if err != nil {
		return nil, rootpublication.StableLogicalObligationRequirements{}, err
	}
	if prepared == nil || prepared.stableResources == nil {
		return nil, rootpublication.StableLogicalObligationRequirements{}, fmt.Errorf("%w: column_graph rebuild produced no exact durable resources", rootpublication.ErrUnresolvedResource)
	}
	resources := prepared.stableResources
	prepared.stableResources = nil
	return resources, requirements, nil
}

func registerColumnVectorGraphDurablePublication(ctx backenddb.CommandWALPublishContext, prepared *columnVectorGraphPreparedPhysicalAsset, records []columnManifestRecord, activeGeneration uint64, namespace string) error {
	resources, requirements, err := takeColumnVectorGraphDurablePublication(prepared, records, activeGeneration, namespace)
	if err != nil {
		return err
	}
	if err := ctx.RegisterDurableLogicalObligationRequirements(requirements); err != nil {
		resources.Release()
		return err
	}
	return ctx.RegisterDurableResources(resources)
}

func reconcileColumnGraphBuildTiming(timing *ColumnGraphBuildTiming) {
	timing.AssetPreparation = max(timing.AssetPreparation, timing.InvNormPreparation+timing.AdjacencyStatePreparation+timing.RowRefPreparation+timing.DocumentIDPreparation+timing.QuantizedPreparation+timing.SearchPackPreparation+timing.ManifestFinalization)
	timing.Publication = max(timing.Publication, timing.AssetPreparation)
	timing.Total = max(timing.Total, timing.Snapshot+timing.RowExtraction+timing.AdjacencyBuild+timing.LocalityRemap+timing.Publication)
}

func (c *Collection) rebuildVectorIndexWithCommandWALIntent(name string, replay *backenddb.CommandWALIntent) (VectorIndexStatus, error) {
	started := time.Now()
	var timing ColumnGraphBuildTiming
	if err := ValidateIndexName(name); err != nil {
		return VectorIndexStatus{}, err
	}
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return VectorIndexStatus{}, err
	}

	snapshotStarted := time.Now()
	snap := c.db.AcquireStableSnapshot()
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if catalog == nil {
		return VectorIndexStatus{}, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		return VectorIndexStatus{}, err
	}
	baseMeta := catalog.meta
	c.meta = baseMeta
	def, ok := findVectorIndex(baseMeta.VectorIndexes, name)
	if !ok {
		return VectorIndexStatus{}, ErrIndexNotFound
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		return c.rebuildNativeVectorIndexPrepared(def, catalog, replay)
	}
	cfg := baseMeta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		status, statusErr := c.columnGraphVectorIndexStatus(def.Name)
		return c.finishRebuildVectorIndexNoopStatus(name, status, statusErr, replay)
	}
	if normalizedDocumentFormat(baseMeta.Options.DocumentFormat) != DocumentFormatJSON {
		return VectorIndexStatus{}, fmt.Errorf("collections: column_graph rebuild for %q requires JSON documents, got %q", name, baseMeta.Options.DocumentFormat)
	}

	state, ok := snap.StateToken()
	if !ok {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	baseCommitSeq := state.CommitSeq
	baseSystemRoot := state.SystemRootPageID
	rootName := collectionColumnManifestRootName(baseMeta.Name)
	baseManifestRootID := catalog.rootID(rootName)
	if baseManifestRootID == 0 {
		timing.Snapshot = collectionObservedElapsedSince(snapshotStarted)
		rowsStarted := time.Now()
		rows, err := c.columnVectorGraphRowsFromCatalogSnapshot(snap, catalog, def)
		timing.RowExtraction = collectionObservedElapsedSince(rowsStarted)
		if err != nil {
			return VectorIndexStatus{}, err
		}
		if len(rows) == 0 {
			return c.rebuildEmptyColumnGraphVectorIndexWithoutBaseManifestRoot(name, catalog, baseMeta, def, *cfg, baseCommitSeq, baseSystemRoot, rootName, replay, started, &timing)
		}
		return VectorIndexStatus{}, fmt.Errorf("collections: column_graph rebuild for %q requires an initial physical column manifest root before rebuilding %d documents", name, len(rows))
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		status, statusErr := c.columnGraphVectorIndexStatus(def.Name)
		return c.finishRebuildVectorIndexNoopStatus(name, status, statusErr, replay)
	}
	if err := validateColumnManifestIdentityAtRoot(snap, baseManifestRootID, *cfg.ActiveManifest); err != nil {
		status, statusErr := c.columnGraphVectorIndexStatus(def.Name)
		return c.finishRebuildVectorIndexNoopStatus(name, status, statusErr, replay)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, baseManifestRootID)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, baseMeta.Name, "column vector graph rebuild"); err != nil {
		return VectorIndexStatus{}, err
	}
	timing.Snapshot = collectionObservedElapsedSince(snapshotStarted)
	rowsStarted := time.Now()
	rows, usedTypedColumns, err := c.columnVectorGraphRowsFromTypedColumnCatalogSnapshot(snap, catalog, *cfg, records, manifest, def)
	if err == nil && !usedTypedColumns {
		rows, err = c.columnVectorGraphRowsFromCatalogSnapshot(snap, catalog, def)
	}
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if !usedTypedColumns {
		if err := c.assignColumnVectorGraphRowRefsFromBaseManifest(baseMeta.Name, *cfg, records, manifest.Generation, rows); err != nil {
			return VectorIndexStatus{}, err
		}
	}
	timing.RowExtraction = collectionObservedElapsedSince(rowsStarted)
	runColumnVectorGraphRebuildBeforeBuildTestHook()

	if err := buildColumnVectorGraphAdjacencyTimed(rows, def, &timing); err != nil {
		return VectorIndexStatus{}, err
	}
	rootNames := []string{rootName}
	baseRootIDs := map[string]uint64{rootName: baseManifestRootID}
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(name, replay)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	if intent != nil {
		var prepared columnVectorGraphPreparedPhysicalAsset
		defer func() { prepared.releaseStableResources() }()
		var updatedMeta CollectionMeta
		buildContextDeltas := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
			var deltaRecords []columnManifestRecord
			var nextIdentity ColumnManifestIdentity
			prepareStarted := time.Now()
			preparedAsset, preparedRecords, preparedIdentity, prepareErr := prepareColumnVectorGraphRebuildManifestForPublicationTimed(baseMeta.Name, *cfg, baseMeta.VectorIndexes, def, manifest, records, ctx.AppliedCommandLSN, rows, c.db.ColumnAssetRootDir(), c.db.StableResourceIdentityPinRegistry(), &timing)
			timing.AssetPreparation = collectionObservedElapsedSince(prepareStarted)
			if prepareErr != nil {
				return nil, prepareErr
			}
			timing.FileSync = preparedAsset.stableFileSync
			timing.FileSyncCount = preparedAsset.stableContentSyncs
			timing.NamespaceSync = preparedAsset.stableNamespaceSync
			timing.NamespaceSyncCount = preparedAsset.stableNamespaceSyncs
			replaceColumnVectorGraphPreparedPhysicalAsset(&prepared, preparedAsset)
			deltaRecords, nextIdentity = preparedRecords, preparedIdentity
			if prepared.RowCount != len(rows) {
				return nil, fmt.Errorf("collections: column_graph rebuild row count changed rows=%d prepared=%d", len(rows), prepared.RowCount)
			}
			delta := ColumnManifestRootDelta{
				RootName:       rootName,
				BaseRootID:     baseManifestRootID,
				StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
				Identity:       nextIdentity,
				IdentityRecord: encodeColumnManifestIdentityRecordArray(nextIdentity),
				Records:        deltaRecords,
			}
			ordered, err := delta.OrderedRootDeltaPublishInput()
			if err != nil {
				return nil, err
			}
			updated, metaErr := columnGraphRebuildUpdatedMeta(baseMeta, nextIdentity, ctx.AppliedCommandLSN)
			if metaErr != nil {
				return nil, metaErr
			}
			updatedMeta = updated
			if err := registerColumnVectorGraphDurablePublication(ctx, &prepared, deltaRecords, nextIdentity.Generation, cfg.AssetManager.Namespace); err != nil {
				return nil, err
			}
			return []backenddb.OrderedRootDeltaPublishInput{ordered}, nil
		}
		buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if prepared.RowCount != len(rows) {
				return nil, errors.New("collections: column_graph rebuild did not prepare physical graph asset")
			}
			if updatedMeta.Name == "" {
				return nil, errors.New("collections: column_graph rebuild did not prepare updated metadata")
			}
			return c.buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		}
		publicationStarted := time.Now()
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(nil, intent, buildContextDeltas, buildSystemDelta)
		timing.Publication = collectionObservedElapsedSince(publicationStarted)
		if err != nil {
			return VectorIndexStatus{}, err
		}
		c.meta = updatedMeta
	} else {
		return VectorIndexStatus{}, fmt.Errorf("%w: column_graph rebuild for %q requires command WAL to publish exact durable resources", backenddb.ErrCommandWALRejected, name)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return VectorIndexStatus{}, unexpectedOrderedRootCountError(baseMeta.Name, 1, len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	status, err := c.columnGraphVectorIndexStatus(def.Name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	timing.Total = collectionObservedElapsedSince(started)
	reconcileColumnGraphBuildTiming(&timing)
	status.Duration = timing.Total
	status.ColumnGraphBuild = timing
	return status, nil
}

func (c *Collection) rebuildNativeVectorIndexPrepared(def VectorIndexDefinition, catalog *collectionCatalog, replay *backenddb.CommandWALIntent) (VectorIndexStatus, error) {
	start := time.Now()
	index, err := c.buildVectorIndexPrepared(vectorIndexOptionsFromDefinition(def), false, false, false, false)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	index.setNativePersistent(true)
	index.recordFullSnapshotBaseEpoch(catalog.rootID(collectionVectorIndexRootName(catalog.meta.Name, def.Name)))
	native, err := index.saveNativeSnapshotPreparedWithCommandWALIntent(replay)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	index.recordNativeDefinition(def)
	if c.manager != nil {
		c.manager.registerCollectionHandle(c)
	}
	duration := collectionObservedElapsedSince(start)
	index.mu.Lock()
	index.lastRebuildDuration = duration
	index.mu.Unlock()
	stats := index.Stats()
	stats.BytesDisk = native.BytesDisk
	return VectorIndexStatus{
		Definition:          def,
		Name:                def.Name,
		Strategy:            def.Strategy,
		State:               VectorIndexStateNativeRuntime,
		Reason:              VectorIndexReasonNativeRuntime,
		Loaded:              native.Loaded,
		RootName:            collectionVectorIndexRootName(catalog.meta.Name, def.Name),
		RootID:              native.RootID,
		NativeRootLoaded:    native.Loaded,
		NativeRootBytes:     native.BytesDisk,
		ExactFallbackReason: native.ExactFallbackReason,
		Registered:          true,
		Stats:               stats,
		RebuildNeeded:       native.ExactFallbackReason != "" || stats.RebuildNeeded || stats.SnapshotDirty,
		Duration:            duration,
	}, nil
}

// columnVectorGraphRowsFromTypedColumnCatalogSnapshot is the narrow rebuild
// source fast path for the currently certified publication shape. It retains
// manifest validation at its caller and falls back only for shapes the typed
// lifecycle explicitly does not support; bad or mismatched assets fail closed.
func (c *Collection) columnVectorGraphRowsFromTypedColumnCatalogSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, cfg ColumnStoreConfig, records []columnManifestRecord, manifest columnManifestSnapshot, def VectorIndexDefinition) ([]columnVectorGraphAssetRow, bool, error) {
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, false, errCollectionNotFound
	}
	field, adapterColumn, ok, err := columnVectorGraphTypedColumnVectorField(cfg, def.Field, def.Dimensions)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	physicalRefs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, manifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, false, err
	}
	if mutationParts != 0 {
		return nil, false, nil
	}
	typedRefs, err := typedColumnPartRefsByGenerationFromManifestRecords(records, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, false, err
	}
	if len(typedRefs) == 0 {
		if manifest.RowCount == 0 {
			return nil, false, nil
		}
		return nil, false, errors.New("collections: column_graph rebuild missing typed_column_part refs")
	}
	if typedColumnRefsHaveSortKey(typedRefs) {
		return nil, false, nil
	}
	physicalRowsByGeneration, physicalPartByGeneration, err := columnVectorGraphTypedColumnPhysicalRowsByGenerationFromRefs(physicalRefs)
	if errors.Is(err, errColumnVectorGraphTypedColumnMultipartDeferred) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	physicalLocations, scannedRowsByGeneration, err := c.columnVectorGraphTypedColumnPhysicalLocations(catalog.meta.Name, cfg, physicalRefs)
	if errors.Is(err, errColumnVectorGraphTypedColumnMultipartDeferred) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	for generation, rows := range scannedRowsByGeneration {
		if physicalRowsByGeneration[generation] != rows {
			return nil, false, fmt.Errorf("collections: column_graph rebuild physical row count generation=%d scanned=%d manifest=%d", generation, rows, physicalRowsByGeneration[generation])
		}
	}

	source := &columnVectorGraphTypedColumnVectorSource{field: field, column: adapterColumn, dims: def.Dimensions, manager: mappedresource.NewManager()}
	defer func() { _ = source.Close() }()
	parts := make(map[uint64]*columnVectorGraphTypedColumnVectorPart, len(typedRefs))
	for generation, typedRef := range typedRefs {
		physicalRows, exists := physicalRowsByGeneration[generation]
		if !exists {
			return nil, false, fmt.Errorf("collections: column_graph rebuild typed_column_part generation=%d has no physical rows", generation)
		}
		part, _, loadErr := c.loadColumnVectorGraphTypedColumnVectorPart(catalog.meta.Name, cfg, typedRef, physicalRows, field, adapterColumn, source.manager)
		if loadErr != nil {
			return nil, false, fmt.Errorf("collections: column_graph rebuild load typed_column_part generation=%d: %w", generation, loadErr)
		}
		source.parts = append(source.parts, part)
		parts[generation] = part
	}
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
	if err != nil {
		return nil, false, err
	}
	if it == nil {
		return nil, true, nil
	}
	defer func() { _ = it.Close() }()
	rows := make([]columnVectorGraphAssetRow, 0, len(physicalLocations))
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		id := bytes.Clone(it.UnsafeKey())
		location, exists := physicalLocations[string(id)]
		if !exists {
			return nil, false, fmt.Errorf("collections: column_graph rebuild missing physical row for document id %q", string(id))
		}
		if physicalPartByGeneration[location.generation] != location.partID {
			return nil, false, fmt.Errorf("collections: column_graph rebuild physical row document id %q generation=%d part mismatch", string(id), location.generation)
		}
		part := parts[location.generation]
		if part == nil || location.rowIndex < 0 || location.rowIndex >= part.rows {
			return nil, false, fmt.Errorf("collections: column_graph rebuild typed row document id %q generation=%d row_index=%d unavailable", string(id), location.generation, location.rowIndex)
		}
		start := location.rowIndex * def.Dimensions
		end := start + def.Dimensions
		if start < 0 || end < start || end > len(part.values) {
			return nil, false, fmt.Errorf("collections: column_graph rebuild typed row document id %q vector bounds", string(id))
		}
		vector := append([]float32(nil), part.values[start:end]...)
		invNorm, normErr := columnVectorGraphInvNorm(vector)
		if normErr != nil {
			return nil, false, fmt.Errorf("collections: column_graph rebuild document id %q: %w", string(id), normErr)
		}
		baseRowRef := DocumentRowRef{
			Generation:        location.generation,
			PartID:            location.partID,
			RowIndex:          location.rowIndex,
			AppliedCommandLSN: location.appliedCommandLSN,
		}
		if err := validateColumnVectorGraphRowRefForState(len(rows), baseRowRef, manifest.Generation); err != nil {
			return nil, false, err
		}
		rows = append(rows, columnVectorGraphAssetRow{ID: id, Vector: vector, InvNorm: invNorm, BaseRowRef: baseRowRef})
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, false, err
	}
	if len(rows) != len(physicalLocations) {
		return nil, false, fmt.Errorf("collections: column_graph rebuild physical rows=%d primary documents=%d", len(physicalLocations), len(rows))
	}
	return rows, true, nil
}

func (c *Collection) rebuildEmptyColumnGraphVectorIndexWithoutBaseManifestRoot(name string, catalog *collectionCatalog, baseMeta CollectionMeta, def VectorIndexDefinition, cfg ColumnStoreConfig, baseCommitSeq, baseSystemRoot uint64, rootName string, replay *backenddb.CommandWALIntent, started time.Time, timing *ColumnGraphBuildTiming) (VectorIndexStatus, error) {
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(name, replay)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if intent == nil {
		return VectorIndexStatus{}, fmt.Errorf("%w: empty column_graph rebuild for %q requires command WAL to publish the initial physical column manifest root", backenddb.ErrCommandWALRejected, name)
	}

	rootNames := []string{rootName}
	baseRootIDs := map[string]uint64{rootName: 0}
	var updatedMeta CollectionMeta
	var prepared columnVectorGraphPreparedPhysicalAsset
	defer func() { prepared.releaseStableResources() }()
	buildContextDeltas := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
		manifest, records, err := initialColumnVectorGraphBaseManifestForRebuild(baseMeta.Name, cfg, ctx.AppliedCommandLSN)
		if err != nil {
			return nil, err
		}
		prepareStarted := time.Now()
		preparedAsset, deltaRecords, nextIdentity, err := prepareColumnVectorGraphRebuildManifestForPublicationTimed(baseMeta.Name, cfg, baseMeta.VectorIndexes, def, manifest, records, ctx.AppliedCommandLSN, nil, c.db.ColumnAssetRootDir(), c.db.StableResourceIdentityPinRegistry(), timing)
		if timing != nil {
			timing.AssetPreparation = collectionObservedElapsedSince(prepareStarted)
		}
		if err != nil {
			return nil, err
		}
		if timing != nil {
			timing.FileSync = preparedAsset.stableFileSync
			timing.FileSyncCount = preparedAsset.stableContentSyncs
			timing.NamespaceSync = preparedAsset.stableNamespaceSync
			timing.NamespaceSyncCount = preparedAsset.stableNamespaceSyncs
		}
		replaceColumnVectorGraphPreparedPhysicalAsset(&prepared, preparedAsset)
		if prepared.RowCount != 0 {
			return nil, fmt.Errorf("collections: empty column_graph rebuild prepared rows=%d want 0", prepared.RowCount)
		}
		delta := ColumnManifestRootDelta{
			RootName:       rootName,
			BaseRootID:     0,
			StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
			Identity:       nextIdentity,
			IdentityRecord: encodeColumnManifestIdentityRecordArray(nextIdentity),
			Records:        deltaRecords,
		}
		ordered, err := delta.OrderedRootDeltaPublishInput()
		if err != nil {
			return nil, err
		}
		updatedMeta, err = columnGraphRebuildUpdatedMeta(baseMeta, nextIdentity, ctx.AppliedCommandLSN)
		if err != nil {
			return nil, err
		}
		if err := registerColumnVectorGraphDurablePublication(ctx, &prepared, deltaRecords, nextIdentity.Generation, cfg.AssetManager.Namespace); err != nil {
			return nil, err
		}
		return []backenddb.OrderedRootDeltaPublishInput{ordered}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if updatedMeta.Name == "" {
			return nil, errors.New("collections: empty column_graph rebuild did not prepare updated metadata")
		}
		return c.buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	}
	publicationStarted := time.Now()
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(nil, intent, buildContextDeltas, buildSystemDelta)
	if timing != nil {
		timing.Publication = collectionObservedElapsedSince(publicationStarted)
	}
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return VectorIndexStatus{}, unexpectedOrderedRootCountError(baseMeta.Name, 1, len(rootIDs))
	}
	c.meta = updatedMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, updatedMeta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	status, err := c.columnGraphVectorIndexStatus(def.Name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if timing != nil {
		timing.Total = collectionObservedElapsedSince(started)
		reconcileColumnGraphBuildTiming(timing)
		status.Duration = timing.Total
		status.ColumnGraphBuild = *timing
	}
	return status, nil
}

func (c *Collection) nativeVectorIndexRebuildStatus(def VectorIndexDefinition) VectorIndexStatus {
	return VectorIndexStatus{
		Name:     def.Name,
		Strategy: def.Strategy,
		State:    VectorIndexStateNativeRuntime,
		Reason:   VectorIndexReasonNativeRuntime,
	}
}

func (c *Collection) finishRebuildVectorIndexNoopStatus(name string, status VectorIndexStatus, statusErr error, replay *backenddb.CommandWALIntent) (VectorIndexStatus, error) {
	if statusErr != nil {
		return VectorIndexStatus{}, statusErr
	}
	intent, err := c.newCollectionRebuildVectorIndexCommandWALIntent(name, replay)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if intent != nil {
		if err := c.db.PublishCommandWALNoop(intent, false); err != nil {
			return VectorIndexStatus{}, err
		}
	}
	return status, nil
}

func (c *Collection) columnVectorGraphRowsFromCatalogSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, def VectorIndexDefinition) ([]columnVectorGraphAssetRow, error) {
	runColumnVectorGraphCanonicalRowsTestHook()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	vectorCfg, err := normalizeColumnStoreConfig(catalog.meta.Name, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:       columnVectorGraphVectorColumnName,
			Path:       def.Field,
			ValueType:  ColumnStoreValueFloat32Vector,
			VectorDims: def.Dimensions,
		}},
	})
	if err != nil {
		return nil, err
	}
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
	if err != nil {
		return nil, err
	}
	if it == nil {
		return nil, nil
	}
	defer func() { _ = it.Close() }()

	rows := make([]columnVectorGraphAssetRow, 0)
	visit := func(record DocumentRecord) (bool, error) {
		declared, err := extractColumnDeclaredRowsFromJSONDocuments(*vectorCfg, []columnWriteDocument{{
			ID:       record.ID,
			Document: record.Document,
		}})
		if err != nil {
			return false, err
		}
		if len(declared) != 1 || len(declared[0].Values) != 1 {
			values := 0
			if len(declared) != 0 {
				values = len(declared[0].Values)
			}
			return false, fmt.Errorf("collections: column_graph rebuild vector extraction returned rows=%d values=%d", len(declared), values)
		}
		value := declared[0].Values[0]
		if !value.Present || value.Null {
			return false, fmt.Errorf("collections: column_graph rebuild missing vector for document id %q", string(record.ID))
		}
		vector := append([]float32(nil), value.Float32Vector...)
		invNorm, err := columnVectorGraphInvNorm(vector)
		if err != nil {
			return false, fmt.Errorf("collections: column_graph rebuild document id %q: %w", string(record.ID), err)
		}
		// The scan producers clone iterator keys before calling visit; do
		// not add another per-row ID copy on the rebuild hot path.
		rows = append(rows, columnVectorGraphAssetRow{
			ID:      record.ID,
			Vector:  vector,
			InvNorm: invNorm,
		})
		return true, nil
	}
	if columnStoreCanReconstructDocument(catalog.meta) {
		_, err = c.scanDocumentsFuncWithColumnReconstruction(snap, catalog, it, maxCollectionInt, visit, nil)
		return rows, err
	}
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		record := DocumentRecord{
			ID:       bytes.Clone(it.UnsafeKey()),
			Document: it.ValueCopy(nil),
		}
		next, err := visit(record)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return rows, nil
}

func columnVectorGraphInvNorm(vector []float32) (float32, error) {
	if len(vector) == 0 {
		return 0, errColumnVectorGraphInvNormEmpty
	}
	var sum float64
	for i, v := range vector {
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("vector[%d] is not finite: %w", i, errColumnVectorGraphInvNormValueNotFinite)
		}
		sum += f * f
	}
	if sum == 0 || math.IsNaN(sum) || math.IsInf(sum, 0) {
		return 0, errColumnVectorGraphInvNormNormInvalid
	}
	invNorm := 1 / math.Sqrt(sum)
	if invNorm > math.MaxFloat32 || math.IsNaN(invNorm) || math.IsInf(invNorm, 0) {
		return 0, errColumnVectorGraphInvNormOutOfRange
	}
	return float32(invNorm), nil
}

func buildColumnVectorGraphAdjacency(rows []columnVectorGraphAssetRow, def VectorIndexDefinition) error {
	return buildColumnVectorGraphAdjacencyV1(rows, def, nil, true, nil, true, nil)
}

func buildColumnVectorGraphAdjacencyTimed(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, timing *ColumnGraphBuildTiming) error {
	return buildColumnVectorGraphAdjacencyV1(rows, def, nil, true, nil, true, timing)
}

func buildColumnVectorGraphAdjacencyWithConstructionTraceV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, trace *vectorIndexConstructionTraceV1) error {
	return buildColumnVectorGraphAdjacencyWithConstructionTraceFinalV1(rows, def, trace, true)
}

// buildColumnVectorGraphAdjacencyWithConstructionTraceFinalV1 lets a
// partition variant defer final survivors until after its own adjacency rewrite.
// Generic graph construction finalizes immediately.
func buildColumnVectorGraphAdjacencyWithConstructionTraceFinalV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, trace *vectorIndexConstructionTraceV1, recordFinal bool) error {
	return buildColumnVectorGraphAdjacencyWithConstructionPolicyV1(rows, def, trace, recordFinal, nil)
}

func buildColumnVectorGraphAdjacencyWithConstructionPolicyV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, trace *vectorIndexConstructionTraceV1, recordFinal bool, policy *vectorIndexLayer0ConstructionPolicyV1) error {
	return buildColumnVectorGraphAdjacencyV1(rows, def, trace, recordFinal, policy, false, nil)
}

func buildColumnVectorGraphAdjacencyV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, trace *vectorIndexConstructionTraceV1, recordFinal bool, policy *vectorIndexLayer0ConstructionPolicyV1, parallelReciprocalLinks bool, timing *ColumnGraphBuildTiming) error {
	if uint64(len(rows)) > maxColumnVectorGraphAdjacencyOrdinal {
		return fmt.Errorf("collections: column vector graph row count=%d exceeds uint32 adjacency encoding", len(rows))
	}
	if policy != nil && (def.Metric != VectorMetricCosine || (policy.initialSelectionFactor != 1 && policy.initialSelectionFactor != 2) || policy.qualityPostfill && policy.robustPruneRefinement) {
		return errors.New("collections: invalid offline layer-0 construction policy")
	}
	if trace != nil {
		trace.sampleIDs = vectorPartitionConstructionSampleIDsV1(rows)
	}
	for i := range rows {
		if len(rows[i].Vector) != def.Dimensions {
			return fmt.Errorf("collections: column vector graph row[%d] vector dims=%d want %d", i, len(rows[i].Vector), def.Dimensions)
		}
	}

	adjacencyStarted := time.Now()
	index, err := newVectorIndex(nil, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return err
	}
	index.constructionTrace = trace
	index.layer0ConstructionPolicy = policy
	index.parallelReciprocalLinks = parallelReciprocalLinks
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := insertColumnVectorGraphRowsLocked(index, rows); err != nil {
		return err
	}
	if policy != nil && policy.qualityPostfill {
		if err := index.applyQualityPostfillLocked(trace, def.M*2); err != nil {
			return err
		}
	}
	if policy != nil && policy.robustPruneRefinement {
		if err := index.applyRobustPruneRefinementLocked(trace, def.M*2); err != nil {
			return err
		}
	}
	// The final refinement stages have consumed this bounded offline pool.
	// Release it before locality remapping allocates its working buffers.
	index.qualityPostfillCandidates = nil
	if timing != nil {
		timing.AdjacencyBuild = collectionObservedElapsedSince(adjacencyStarted)
	}

	localityStarted := time.Now()
	inputOrdinalByNode := make([]int, len(index.nodes))
	for i := range inputOrdinalByNode {
		inputOrdinalByNode[i] = -1
	}
	for i := range rows {
		nodeID, ok := index.currentNode[string(rows[i].ID)]
		if !ok || nodeID < 0 || nodeID >= len(index.nodes) || index.nodes[nodeID].deleted {
			return fmt.Errorf("collections: column vector graph row[%d] missing native graph node", i)
		}
		inputOrdinalByNode[nodeID] = i
	}
	order := columnVectorGraphNativeLocalityOrder(index)
	if len(order) != len(rows) {
		return fmt.Errorf("collections: column vector graph locality order rows=%d want %d", len(order), len(rows))
	}
	orderedRows := make([]columnVectorGraphAssetRow, len(rows))
	nodeOrdinal := make([]int, len(index.nodes))
	for i := range nodeOrdinal {
		nodeOrdinal[i] = -1
	}
	for ordinal, nodeID := range order {
		inputOrdinal := -1
		if nodeID >= 0 && nodeID < len(inputOrdinalByNode) {
			inputOrdinal = inputOrdinalByNode[nodeID]
		}
		if inputOrdinal < 0 {
			return fmt.Errorf("collections: column vector graph locality node=%d missing input row", nodeID)
		}
		orderedRows[ordinal] = rows[inputOrdinal]
		nodeOrdinal[nodeID] = ordinal
	}
	if err := trace.remap(index, nodeOrdinal); err != nil {
		return err
	}
	copy(rows, orderedRows)
	nodeIDByOrdinal := order
	for i := range rows {
		nodeID := nodeIDByOrdinal[i]
		adjacency, err := columnVectorGraphLayeredAdjacencyFromNativeNode(&index.nodes[nodeID], nodeOrdinal)
		if err != nil {
			return err
		}
		rows[i].Adjacency = adjacency
	}
	if err := trace.reconcileNativeAdjacency(rows); err != nil {
		return err
	}
	if recordFinal {
		if err := trace.recordFinalSurvivors(rows); err != nil {
			return err
		}
	}
	if timing != nil {
		timing.LocalityRemap = collectionObservedElapsedSince(localityStarted)
	}
	return nil
}

func insertColumnVectorGraphRowsLocked(index *VectorIndex, rows []columnVectorGraphAssetRow) error {
	ids := make([][]byte, len(rows))
	vectors := make([][]float32, len(rows))
	for i := range rows {
		ids[i], vectors[i] = rows[i].ID, rows[i].Vector
	}
	if err := index.insertVectorBatchLocked(ids, vectors); err != nil {
		return fmt.Errorf("collections: build column vector graph: %w", err)
	}
	return nil
}

// remap converts temporary native node IDs to the exact BFS-locality ordinals
// written into the pack. InsertionOrdinal intentionally remains in native
// insertion order: it is causal age, not a persisted graph ordinal. Final
// survivors are deliberately recorded later, after a variant has made every
// native-adjacency mutation (overlay or reciprocity repair).
func (t *vectorIndexConstructionTraceV1) remap(index *VectorIndex, nodeOrdinal []int) error {
	if t == nil {
		return nil
	}
	remap := func(nodeID int) (int, error) {
		if nodeID < 0 || nodeID >= len(nodeOrdinal) || nodeOrdinal[nodeID] < 0 {
			return 0, fmt.Errorf("collections: construction trace node=%d has no locality ordinal", nodeID)
		}
		return nodeOrdinal[nodeID], nil
	}
	// Only the lowest domain-separated eligible L0 selections survive as
	// samples. We captured one extra ID because the entry has no selection.
	type sampleSelection struct {
		index int
		hash  [32]byte
		id    []byte
	}
	var samples []sampleSelection
	for i := range t.selections {
		s := &t.selections[i]
		if s.Layer == 0 && s.Sampled {
			id := index.nodes[s.Node].documentID
			samples = append(samples, sampleSelection{index: i, id: id, hash: vectorPartitionConstructionSampleHashV1(id)})
		}
	}
	sort.Slice(samples, func(i, j int) bool {
		return vectorPartitionConstructionSampleLessV1(samples[i].hash, samples[i].id, samples[j].hash, samples[j].id)
	})
	for i := vectorPartitionConstructionCandidateSampleLimitV1; i < len(samples); i++ {
		t.selections[samples[i].index].Sampled = false
		t.selections[samples[i].index].CandidateNodes = nil
	}
	t.nativeInsertionOrdinals = make([]int, len(nodeOrdinal))
	for native, bfs := range nodeOrdinal {
		if bfs < 0 || bfs >= len(t.nativeInsertionOrdinals) {
			return fmt.Errorf("collections: construction trace native ordinal")
		}
		t.nativeInsertionOrdinals[bfs] = native
	}
	for i := range t.selections {
		ordinal, err := remap(t.selections[i].Node)
		if err != nil {
			return err
		}
		t.selections[i].Node = ordinal
		for j, candidate := range t.selections[i].CandidateNodes {
			mapped, err := remap(candidate)
			if err != nil {
				return err
			}
			t.selections[i].CandidateNodes[j] = mapped
		}
		// The evidence contract stores sampled candidates in canonical packed
		// ordinal order, independent of the transient HNSW score ordering.
		sort.Ints(t.selections[i].CandidateNodes)
		if t.selections[i].Sampled {
			unique := t.selections[i].CandidateNodes[:0]
			for _, candidate := range t.selections[i].CandidateNodes {
				if len(unique) == 0 || unique[len(unique)-1] != candidate {
					unique = append(unique, candidate)
				}
			}
			t.selections[i].CandidateNodes = unique
			// Candidate-pool evidence is a set for sampled and unsampled
			// selections alike; selectLayerNeighborsLocked recorded that count
			// before this BFS ordinal remap.
			if len(unique) != t.selections[i].Candidates {
				return fmt.Errorf("collections: construction trace sampled candidate count")
			}
		}
	}
	for i := range t.events {
		from, err := remap(t.events[i].From)
		if err != nil {
			return err
		}
		to, err := remap(t.events[i].To)
		if err != nil {
			return err
		}
		t.events[i].From, t.events[i].To = from, to
	}
	remapOrigins := make(map[vectorIndexConstructionEdgeKeyV1]string, len(t.origins))
	for key, origin := range t.origins {
		from, err := remap(key.From)
		if err != nil {
			return err
		}
		to, err := remap(key.To)
		if err != nil {
			return err
		}
		remapOrigins[vectorIndexConstructionEdgeKeyV1{From: from, To: to, Layer: key.Layer}] = origin
	}
	t.origins = remapOrigins
	if len(t.pending) != 0 {
		return fmt.Errorf("collections: construction trace has pending selected edges")
	}
	return nil
}

func (t *vectorIndexConstructionTraceV1) recordFinalSurvivors(rows []columnVectorGraphAssetRow) error {
	if t == nil {
		return nil
	}
	// Compact evidence exports the already-live origin map separately. Do not
	// turn its final adjacency into a full historical event slice.
	if !t.detailed {
		return nil
	}
	for from := range rows {
		layers, err := vectorPartitionConstructionAdjacencyLayersV1(rows[from].Adjacency)
		if err != nil {
			return err
		}
		for layer, neighbors := range layers {
			for _, neighbor := range neighbors {
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: int(neighbor), Layer: layer}
				origin, ok := t.origins[key]
				if !ok {
					return fmt.Errorf("collections: construction trace missing final edge origin from=%d to=%d layer=%d", from, neighbor, layer)
				}
				insertionOrdinal := t.nativeInsertionOrdinals[from]
				if t.nativeInsertionOrdinals[int(neighbor)] > insertionOrdinal {
					insertionOrdinal = t.nativeInsertionOrdinals[int(neighbor)]
				}
				t.events = append(t.events, vectorIndexConstructionEdgeEventV1{From: from, To: int(neighbor), Layer: layer, InsertionOrdinal: insertionOrdinal, Origin: origin, Action: "final_survivor"})
			}
		}
	}
	return nil
}

// reconcileNativeAdjacency makes the traced live edge map agree with the
// serialized native graph before a partition variant mutates layer 0. The
// builder can prune a reciprocal edge while maintaining a node; record that
// loss explicitly rather than allowing a stale in-memory trace to masquerade
// as a variant rewrite. Every packed native edge must already have an origin.
func (t *vectorIndexConstructionTraceV1) reconcileNativeAdjacency(rows []columnVectorGraphAssetRow) error {
	if t == nil {
		return nil
	}
	final := make(map[vectorIndexConstructionEdgeKeyV1]struct{})
	for from := range rows {
		layers, err := vectorPartitionConstructionAdjacencyLayersV1(rows[from].Adjacency)
		if err != nil {
			return err
		}
		for layer, neighbors := range layers {
			for _, to := range neighbors {
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: int(to), Layer: layer}
				if _, ok := t.origins[key]; !ok {
					return fmt.Errorf("collections: construction trace missing native edge origin from=%d to=%d layer=%d", from, to, layer)
				}
				final[key] = struct{}{}
			}
		}
	}
	drops := make([]vectorIndexConstructionEdgeKeyV1, 0)
	for key := range t.origins {
		if _, ok := final[key]; !ok {
			drops = append(drops, key)
		}
	}
	sort.Slice(drops, func(i, j int) bool {
		if drops[i].Layer != drops[j].Layer {
			return drops[i].Layer < drops[j].Layer
		}
		if drops[i].From != drops[j].From {
			return drops[i].From < drops[j].From
		}
		return drops[i].To < drops[j].To
	})
	for _, key := range drops {
		t.record(key.From, key.To, key.Layer, t.origins[key], "reciprocal_prune_drop")
		delete(t.origins, key)
	}
	return nil
}

func vectorPartitionConstructionAdjacencyLayersV1(adjacency []uint32) ([][]uint32, error) {
	maxLayer, err := columnVectorGraphAdjacencyMaxLayer(adjacency)
	if err != nil {
		return nil, err
	}
	if maxLayer < 0 {
		return nil, nil
	}
	layers := make([][]uint32, maxLayer+1)
	for layer := range layers {
		layers[layer], err = columnVectorGraphAdjacencyLayer(adjacency, layer)
		if err != nil {
			return nil, err
		}
	}
	return layers, nil
}

func columnVectorGraphNativeLocalityOrder(index *VectorIndex) []int {
	if index == nil || len(index.nodes) == 0 {
		return nil
	}
	order := make([]int, 0, len(index.nodes))
	visited := make([]bool, len(index.nodes))
	queue := make([]int, 0, len(index.nodes))
	if index.entry >= 0 && index.entry < len(index.nodes) && !index.nodes[index.entry].deleted {
		visited[index.entry] = true
		queue = append(queue, index.entry)
	}
	for head := 0; head < len(queue); head++ {
		nodeID := queue[head]
		order = append(order, nodeID)
		node := &index.nodes[nodeID]
		for layer := len(node.neighbors) - 1; layer >= 0; layer-- {
			for _, neighbor := range node.neighbors[layer] {
				neighborID := neighbor.nodeID
				if neighborID < 0 || neighborID >= len(index.nodes) || visited[neighborID] || index.nodes[neighborID].deleted {
					continue
				}
				visited[neighborID] = true
				queue = append(queue, neighborID)
			}
		}
	}
	for nodeID := range index.nodes {
		if visited[nodeID] || index.nodes[nodeID].deleted {
			continue
		}
		order = append(order, nodeID)
	}
	return order
}

func columnVectorGraphLayeredAdjacencyFromNativeNode(node *vectorIndexNode, nodeOrdinal []int) ([]uint32, error) {
	if node == nil {
		return nil, nil
	}
	maxLayer := len(node.neighbors) - 1
	for maxLayer >= 0 && len(node.neighbors[maxLayer]) == 0 {
		maxLayer--
	}
	if maxLayer < 0 {
		return nil, nil
	}
	layers := make([][]uint32, maxLayer+1)
	for layer := 0; layer <= maxLayer; layer++ {
		if len(node.neighbors[layer]) == 0 {
			continue
		}
		layers[layer] = make([]uint32, 0, len(node.neighbors[layer]))
		for _, neighbor := range node.neighbors[layer] {
			neighborID := neighbor.nodeID
			if neighborID < 0 || neighborID >= len(nodeOrdinal) {
				return nil, fmt.Errorf("collections: column vector graph neighbor node=%d out of range", neighborID)
			}
			ordinal := nodeOrdinal[neighborID]
			if ordinal < 0 || uint64(ordinal) > maxColumnVectorGraphAdjacencyOrdinal {
				return nil, fmt.Errorf("collections: column vector graph neighbor ordinal=%d exceeds uint32 adjacency encoding", ordinal)
			}
			layers[layer] = append(layers[layer], uint32(ordinal))
		}
	}
	if maxLayer == 0 {
		return layers[0], nil
	}
	total := 2
	for layer := 0; layer <= maxLayer; layer++ {
		total += 1 + len(layers[layer])
	}
	encoded := make([]uint32, 0, total)
	encoded = append(encoded, columnVectorGraphLayeredAdjacencyMagic, uint32(maxLayer))
	for layer := 0; layer <= maxLayer; layer++ {
		encoded = append(encoded, uint32(len(layers[layer])))
		encoded = append(encoded, layers[layer]...)
	}
	return encoded, nil
}

func topColumnVectorGraphNeighbors(rows []columnVectorGraphAssetRow, row, degree int) ([]columnVectorGraphNeighbor, error) {
	if degree <= columnVectorGraphNeighborInsertionLimit {
		candidates := make([]columnVectorGraphNeighbor, 0, degree)
		for j := range rows {
			if row == j {
				continue
			}
			score := columnVectorGraphCosine(rows[row], rows[j])
			if math.IsNaN(score) {
				return nil, fmt.Errorf("collections: column vector graph cosine row[%d,%d] is NaN", row, j)
			}
			candidates = insertColumnVectorGraphTopNeighbor(candidates, degree, columnVectorGraphNeighbor{ordinal: j, score: score})
		}
		return candidates, nil
	}

	candidates := make(columnVectorGraphNeighborHeap, 0, degree)
	for j := range rows {
		if row == j {
			continue
		}
		score := columnVectorGraphCosine(rows[row], rows[j])
		if math.IsNaN(score) {
			return nil, fmt.Errorf("collections: column vector graph cosine row[%d,%d] is NaN", row, j)
		}
		candidates.pushTop(degree, columnVectorGraphNeighbor{ordinal: j, score: score})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return columnVectorGraphNeighborLess(candidates[i], candidates[j])
	})
	return candidates, nil
}

type columnVectorGraphNeighbor struct {
	ordinal int
	score   float64
}

type columnVectorGraphNeighborHeap []columnVectorGraphNeighbor

func insertColumnVectorGraphTopNeighbor(top []columnVectorGraphNeighbor, limit int, candidate columnVectorGraphNeighbor) []columnVectorGraphNeighbor {
	if limit <= 0 {
		return top
	}
	pos := len(top)
	for pos > 0 && columnVectorGraphNeighborLess(candidate, top[pos-1]) {
		pos--
	}
	if pos >= limit {
		return top
	}
	if len(top) < limit {
		top = append(top, columnVectorGraphNeighbor{})
	}
	copy(top[pos+1:], top[pos:len(top)-1])
	top[pos] = candidate
	return top
}

func (h *columnVectorGraphNeighborHeap) pushTop(limit int, candidate columnVectorGraphNeighbor) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.siftUp(len(*h) - 1)
		return
	}
	if !columnVectorGraphNeighborLess(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.siftDown(0)
}

func (h columnVectorGraphNeighborHeap) siftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !columnVectorGraphNeighborWorse(h[idx], h[parent]) {
			return
		}
		h[idx], h[parent] = h[parent], h[idx]
		idx = parent
	}
}

func (h columnVectorGraphNeighborHeap) siftDown(idx int) {
	for {
		left := idx*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		if right := left + 1; right < len(h) && columnVectorGraphNeighborWorse(h[right], h[left]) {
			child = right
		}
		if !columnVectorGraphNeighborWorse(h[child], h[idx]) {
			return
		}
		h[idx], h[child] = h[child], h[idx]
		idx = child
	}
}

func columnVectorGraphNeighborLess(left, right columnVectorGraphNeighbor) bool {
	if left.score == right.score {
		return left.ordinal < right.ordinal
	}
	return left.score > right.score
}

func columnVectorGraphNeighborWorse(left, right columnVectorGraphNeighbor) bool {
	if left.score == right.score {
		return left.ordinal > right.ordinal
	}
	return left.score < right.score
}

func columnVectorGraphCosine(left, right columnVectorGraphAssetRow) float64 {
	var dot float64
	for i, v := range left.Vector {
		dot += float64(v) * float64(right.Vector[i])
	}
	return dot * float64(left.InvNorm) * float64(right.InvNorm)
}

func prepareColumnVectorGraphRebuildManifest(collection string, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, def VectorIndexDefinition, manifest columnManifestSnapshot, records []columnManifestRecord, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow, assetRootDir string) (columnVectorGraphPreparedPhysicalAsset, []columnManifestRecord, ColumnManifestIdentity, error) {
	return prepareColumnVectorGraphRebuildManifestWithAuthority(collection, cfg, activeVectorIndexes, def, manifest, records, appliedCommandLSN, rows, assetRootDir, nil)
}

func prepareColumnVectorGraphRebuildManifestWithStableResources(collection string, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, def VectorIndexDefinition, manifest columnManifestSnapshot, records []columnManifestRecord, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow, assetRootDir string, registry *rootpublication.IdentityPinRegistry) (columnVectorGraphPreparedPhysicalAsset, []columnManifestRecord, ColumnManifestIdentity, error) {
	authority, err := newColumnVectorGraphStableResourceAccumulator(registry)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	prepared, nextRecords, identity, err := prepareColumnVectorGraphRebuildManifestWithAuthority(collection, cfg, activeVectorIndexes, def, manifest, records, appliedCommandLSN, rows, assetRootDir, authority)
	if err != nil {
		authority.abandon()
		prepared.releaseStableResources()
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	return prepared, nextRecords, identity, nil
}

func prepareColumnVectorGraphRebuildManifestForPublication(collection string, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, def VectorIndexDefinition, manifest columnManifestSnapshot, records []columnManifestRecord, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow, assetRootDir string, registry *rootpublication.IdentityPinRegistry) (columnVectorGraphPreparedPhysicalAsset, []columnManifestRecord, ColumnManifestIdentity, error) {
	return prepareColumnVectorGraphRebuildManifestForPublicationTimed(collection, cfg, activeVectorIndexes, def, manifest, records, appliedCommandLSN, rows, assetRootDir, registry, nil)
}

func prepareColumnVectorGraphRebuildManifestForPublicationTimed(collection string, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, def VectorIndexDefinition, manifest columnManifestSnapshot, records []columnManifestRecord, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow, assetRootDir string, registry *rootpublication.IdentityPinRegistry, timing *ColumnGraphBuildTiming) (columnVectorGraphPreparedPhysicalAsset, []columnManifestRecord, ColumnManifestIdentity, error) {
	if ordinaryColumnStableAuthorityEnabled() {
		authority, err := newColumnVectorGraphStableResourceAccumulator(registry)
		if err != nil {
			return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
		}
		prepared, nextRecords, identity, err := prepareColumnVectorGraphRebuildManifestWithAuthorityTimed(collection, cfg, activeVectorIndexes, def, manifest, records, appliedCommandLSN, rows, assetRootDir, authority, timing)
		if err != nil {
			authority.abandon()
			prepared.releaseStableResources()
			return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
		}
		return prepared, nextRecords, identity, nil
	}
	return prepareColumnVectorGraphRebuildManifestWithAuthorityTimed(collection, cfg, activeVectorIndexes, def, manifest, records, appliedCommandLSN, rows, assetRootDir, nil, timing)
}

func prepareColumnVectorGraphRebuildManifestWithAuthority(collection string, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, def VectorIndexDefinition, manifest columnManifestSnapshot, records []columnManifestRecord, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow, assetRootDir string, authority *columnVectorGraphStableResourceAccumulator) (columnVectorGraphPreparedPhysicalAsset, []columnManifestRecord, ColumnManifestIdentity, error) {
	return prepareColumnVectorGraphRebuildManifestWithAuthorityTimed(collection, cfg, activeVectorIndexes, def, manifest, records, appliedCommandLSN, rows, assetRootDir, authority, nil)
}

func prepareColumnVectorGraphRebuildManifestWithAuthorityTimed(collection string, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, def VectorIndexDefinition, manifest columnManifestSnapshot, records []columnManifestRecord, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow, assetRootDir string, authority *columnVectorGraphStableResourceAccumulator, timing *ColumnGraphBuildTiming) (columnVectorGraphPreparedPhysicalAsset, []columnManifestRecord, ColumnManifestIdentity, error) {
	if appliedCommandLSN == 0 {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, errors.New("collections: column_graph rebuild requires non-zero AppliedCommandLSN")
	}
	recordsForLSN, err := columnVectorGraphManifestRecordsWithAppliedCommandLSN(manifest, records, cfg, activeVectorIndexes, appliedCommandLSN)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	manifestForLSN := manifest
	manifestForLSN.AppliedCommandLSN = appliedCommandLSN
	baseChecksum, err := columnVectorGraphBaseManifestChecksum(manifestForLSN, recordsForLSN, cfg)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	if len(rows) > 0 {
		if _, _, typedVectorOwner, ownerErr := columnVectorGraphTypedColumnVectorField(cfg, def.Field, def.Dimensions); ownerErr != nil {
			return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, ownerErr
		} else if !typedVectorOwner {
			return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, fmt.Errorf("collections: column_graph rebuild for %q requires base float32_vector typed-column owner for field %q; rebuild collection metadata before rebuilding the vector index", def.Name, def.Field)
		}
	}
	partID := nextColumnVectorGraphPartID(recordsForLSN, graphCfg.AssetManager.Namespace)
	prepared := columnVectorGraphPreparedPhysicalAsset{
		AssetRootDir: assetRootDir,
		Config:       graphCfg,
		RowCount:     len(rows),
	}
	invNormPartID := partID
	stageStarted := time.Now()
	preparedInvNorm, err := prepareColumnVectorGraphInvNormStateAssetWithStableAuthority(assetRootDir, collection, cfg, def, manifest.Generation, invNormPartID, rows, authority)
	if timing != nil {
		timing.InvNormPreparation = collectionObservedElapsedSince(stageStarted)
	}
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	graph := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: manifest.Generation,
		BaseManifestChecksum:   baseChecksum,
		BaseSchemaHash:         cfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               prepared.RowCount,
	}
	statePartID := invNormPartID
	if preparedInvNorm.Present {
		statePartID = nextColumnVectorGraphPartIDAfter(statePartID, preparedInvNorm.Ref.PartID)
	}
	stageStarted = time.Now()
	stateAdjacencyAssets, err := prepareColumnVectorIndexStateAdjacencyAssetsWithStableAuthority(assetRootDir, collection, cfg, def, manifest.Generation, statePartID, rows, authority)
	if timing != nil {
		timing.AdjacencyStatePreparation = collectionObservedElapsedSince(stageStarted)
	}
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	rowRefPartID := statePartID
	if len(stateAdjacencyAssets) > 0 {
		rowRefPartID = nextColumnVectorGraphPartIDAfter(rowRefPartID, stateAdjacencyAssets[len(stateAdjacencyAssets)-1].Ref.PartID)
	}
	stageStarted = time.Now()
	preparedRowRefs, err := prepareColumnVectorGraphRowRefStateAssetsWithStableAuthority(assetRootDir, collection, cfg, def, manifest.Generation, rowRefPartID, rows, authority)
	if timing != nil {
		timing.RowRefPreparation = collectionObservedElapsedSince(stageStarted)
	}
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	documentIDPartID := rowRefPartID
	if len(preparedRowRefs) > 0 {
		documentIDPartID = nextColumnVectorGraphPartIDAfter(documentIDPartID, preparedRowRefs[len(preparedRowRefs)-1].Ref.PartID)
	}
	stageStarted = time.Now()
	preparedDocumentIDs, err := prepareColumnVectorGraphDocumentIDStateAssetWithStableAuthority(assetRootDir, collection, cfg, def, manifest.Generation, documentIDPartID, rows, authority)
	if timing != nil {
		timing.DocumentIDPreparation = collectionObservedElapsedSince(stageStarted)
	}
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	quantizedPartID := documentIDPartID
	if preparedDocumentIDs.Present {
		quantizedPartID = nextColumnVectorGraphPartIDAfter(quantizedPartID, preparedDocumentIDs.Ref.PartID)
	}
	stageStarted = time.Now()
	preparedQuantizedAssets, err := prepareColumnVectorGraphQuantizedAssetsWithStableAuthority(assetRootDir, collection, cfg, def, graph, manifest.Generation, quantizedPartID, rows, authority)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	if timing != nil && len(preparedQuantizedAssets) > 0 {
		timing.QuantizedPreparation = collectionObservedElapsedSince(stageStarted)
	}
	searchPackPartID := quantizedPartID
	if len(preparedQuantizedAssets) > 0 {
		searchPackPartID = nextColumnVectorGraphPartIDAfter(searchPackPartID, preparedQuantizedAssets[len(preparedQuantizedAssets)-1].Ref.PartID)
	}
	stageStarted = time.Now()
	preparedSearchPack, err := prepareColumnHNSWSearchPackAssetWithStableAuthority(assetRootDir, cfg, def, graph, manifest.Generation, searchPackPartID, rows, authority)
	if timing != nil {
		timing.SearchPackPreparation = collectionObservedElapsedSince(stageStarted)
	}
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	stageStarted = time.Now()
	raw, err := encodeColumnVectorGraphManifestRecord(graph)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	record := columnManifestRecord{key: columnVectorGraphManifestRecordKey(def.Name), value: raw}
	nextRecords, err := replaceColumnVectorGraphManifestRecord(recordsForLSN, manifest.Generation, record)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	state := columnVectorIndexStateSnapshotFromGraph(graph)
	state.AdjacencyLayerCount = len(stateAdjacencyAssets)
	state.Assets = columnVectorIndexStateAdjacencyAssetsFromPrepared(stateAdjacencyAssets)
	if invNormAsset, ok := columnVectorGraphInvNormStateAssetSnapshot(preparedInvNorm); ok {
		state.Assets = append(state.Assets, invNormAsset)
	}
	state.Assets = append(state.Assets, columnVectorGraphRowRefStateAssetSnapshots(preparedRowRefs)...)
	if documentIDAsset, ok := columnVectorGraphDocumentIDStateAssetSnapshot(preparedDocumentIDs); ok {
		state.Assets = append(state.Assets, documentIDAsset)
	}
	state.Assets = append(state.Assets, columnVectorGraphQuantizedAssetSnapshotsFromPrepared(preparedQuantizedAssets)...)
	if searchPackAsset, ok := columnHNSWSearchPackStateAssetSnapshot(preparedSearchPack); ok {
		state.Assets = append(state.Assets, searchPackAsset)
	}
	stateRaw, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	stateRecord := columnManifestRecord{key: columnVectorIndexStateRecordKey(def.Name), value: stateRaw}
	nextRecords, err = replaceColumnVectorGraphManifestRecord(nextRecords, manifest.Generation, stateRecord)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
	}
	identity := ColumnManifestIdentity{
		Generation: manifest.Generation,
		Format:     columnManifestFormatTCS1,
		Version:    columnManifestIdentityVersion,
		Checksum: checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
			Collection:        manifest.Collection,
			ColumnStore:       cfg,
			Operation:         manifest.Operation,
			AppliedCommandLSN: appliedCommandLSN,
		}, manifest.Generation, nextRecords),
	}
	normalizeColumnManifestIdentityDefaults(&identity)
	if authority != nil {
		resources, err := authority.freeze(state.Assets)
		if err != nil {
			return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
		}
		prepared.stableResources = resources
		prepared.stableSegments = authority.segments
		prepared.stableContentSyncs = authority.contentSyncs
		prepared.stableNamespaceSyncs = authority.namespaceSyncs
		prepared.stableFileSync = authority.fileSync
		prepared.stableNamespaceSync = authority.namespaceSync
		if err := runColumnVectorGraphStableAuthorityTestHook(resources, state.Assets); err != nil {
			prepared.releaseStableResources()
			return columnVectorGraphPreparedPhysicalAsset{}, nil, ColumnManifestIdentity{}, err
		}
	}
	if timing != nil {
		timing.ManifestFinalization = collectionObservedElapsedSince(stageStarted)
	}
	return prepared, nextRecords, identity, nil
}

func columnVectorGraphManifestRecordsWithAppliedCommandLSN(manifest columnManifestSnapshot, records []columnManifestRecord, cfg ColumnStoreConfig, activeVectorIndexes []VectorIndexDefinition, appliedCommandLSN uint64) ([]columnManifestRecord, error) {
	active, err := activeColumnManifestRecordsForScan(records, manifest.Generation)
	if err != nil {
		return nil, err
	}
	out := make([]columnManifestRecord, 0, len(active))
	activePartCount := uint64(0)
	for _, record := range active {
		if bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			partGeneration, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
			if err != nil {
				return nil, err
			}
			if partGeneration == manifest.Generation {
				activePartCount++
			}
		}
	}
	header := encodeColumnVectorGraphRebuildHeaderRecord(manifest, cfg, appliedCommandLSN, activePartCount)
	replacedHeader := false
	for _, record := range active {
		if bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes) {
			out = append(out, columnManifestRecord{key: bytes.Clone(record.key), value: header})
			replacedHeader = true
			continue
		}
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) &&
			!retainColumnManifestVectorGraphRecordForWrite(record.key, true, activeVectorIndexes) {
			continue
		}
		if bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) &&
			!retainColumnVectorIndexStateRecordForWrite(record.key, true, activeVectorIndexes) {
			continue
		}
		out = append(out, columnManifestRecord{key: bytes.Clone(record.key), value: bytes.Clone(record.value)})
	}
	if !replacedHeader {
		return nil, errors.New("collections: column_graph rebuild missing column manifest header")
	}
	sortColumnManifestRecords(out)
	return out, nil
}

func encodeColumnVectorGraphRebuildHeaderRecord(manifest columnManifestSnapshot, cfg ColumnStoreConfig, appliedCommandLSN, activePartCount uint64) []byte {
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestHeaderMagic)
	writeManifestUint16(&b, columnManifestRecordVersion)
	writeManifestString(&b, manifest.Collection)
	writeManifestString(&b, string(manifest.Operation))
	writeManifestUint64(&b, manifest.Generation)
	writeManifestUint64(&b, appliedCommandLSN)
	writeManifestUint64(&b, cfg.SchemaHash)
	writeManifestUint64(&b, uint64(manifest.RowCount))
	writeManifestUint64(&b, uint64(manifest.CommandBytes))
	writeManifestUint64(&b, uint64(manifest.RowRemainderBytes))
	writeManifestUint64(&b, uint64(manifest.ColumnPayloadBytes))
	writeManifestUint64(&b, activePartCount)
	return b.Bytes()
}

func initialColumnVectorGraphBaseManifestForRebuild(collection string, cfg ColumnStoreConfig, appliedCommandLSN uint64) (columnManifestSnapshot, []columnManifestRecord, error) {
	encoded, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        collection,
		ColumnStore:       cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: appliedCommandLSN,
		Prepared: ColumnPublishPreparedAssets{
			RowCount: 0,
		},
	})
	if err != nil {
		return columnManifestSnapshot{}, nil, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(encoded.Records)
	if err != nil {
		return columnManifestSnapshot{}, nil, err
	}
	return manifest, encoded.Records, nil
}

func replaceColumnVectorGraphManifestRecord(records []columnManifestRecord, generation uint64, replacement columnManifestRecord) ([]columnManifestRecord, error) {
	active, err := activeColumnManifestRecordsForScan(records, generation)
	if err != nil {
		return nil, err
	}
	out := make([]columnManifestRecord, 0, len(active)+1)
	replaced := false
	for _, record := range active {
		if bytes.Equal(record.key, replacement.key) {
			out = append(out, columnManifestRecord{key: bytes.Clone(replacement.key), value: bytes.Clone(replacement.value)})
			replaced = true
			continue
		}
		out = append(out, columnManifestRecord{key: bytes.Clone(record.key), value: bytes.Clone(record.value)})
	}
	if !replaced {
		out = append(out, columnManifestRecord{key: bytes.Clone(replacement.key), value: bytes.Clone(replacement.value)})
	}
	sortColumnManifestRecords(out)
	return out, nil
}

func nextColumnVectorGraphPartID(records []columnManifestRecord, namespace string) uint64 {
	next := uint64(1)
	for _, record := range records {
		if bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			_, partID, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
			if err != nil {
				continue
			}
			next = nextColumnVectorGraphPartIDAfter(next, partID)
			continue
		}
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			graph, err := decodeColumnVectorGraphManifestRecord(record.value)
			if err != nil {
				continue
			}
			if graph.AssetRef.Namespace == namespace {
				next = nextColumnVectorGraphPartIDAfter(next, graph.AssetRef.PartID)
			}
			if len(graph.AdjacencyLayerSources) > 0 {
				for _, source := range graph.AdjacencyLayerSources {
					if source.Present && source.Ref.Namespace == namespace {
						next = nextColumnVectorGraphPartIDAfter(next, source.Ref.PartID)
					}
				}
			} else if graph.Layer0AdjacencySource.Present && graph.Layer0AdjacencySource.Ref.Namespace == namespace {
				next = nextColumnVectorGraphPartIDAfter(next, graph.Layer0AdjacencySource.Ref.PartID)
			}
			continue
		}
		if bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
			state, err := decodeColumnVectorIndexStateRecord(record.value)
			if err != nil {
				continue
			}
			for _, asset := range state.Assets {
				if asset.Ref.Namespace == namespace {
					next = nextColumnVectorGraphPartIDAfter(next, asset.Ref.PartID)
				}
			}
		}
	}
	return next
}

func nextColumnVectorGraphPartIDAfter(next, observed uint64) uint64 {
	if observed < next || observed == ^uint64(0) {
		return next
	}
	return observed + 1
}

func columnGraphRebuildUpdatedMeta(base CollectionMeta, identity ColumnManifestIdentity, appliedCommandLSN uint64) (CollectionMeta, error) {
	updated := copyCollectionMeta(base)
	if updated.Options.ColumnStore == nil || !updated.Options.ColumnStore.Enabled {
		return CollectionMeta{}, errColumnPublishPlanRequiresEnabledColumnStore
	}
	cfg := updated.Options.ColumnStore.copy()
	active := identity
	recovery := identity
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	cfg.RecoveryAuthoritativeAppliedCommandLSN = appliedCommandLSN
	updated.Options.ColumnStore = &cfg
	return normalizeCollectionMeta(updated)
}

func (c *Collection) buildColumnGraphRebuildSystemDeltaIterator(baseMeta, updatedMeta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
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
	updates[systemCollectionMetaKey(baseMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		if rootIDs[i] == 0 {
			return nil, fmt.Errorf("collections: ordered root publish returned zero root for %q", rootName)
		}
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemDeltaIterator(updates)
}
