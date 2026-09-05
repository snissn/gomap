package collections

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

// foldTypedGraph is explicit internal maintenance. The callback is an internal
// observation seam after capture admission is released, not a publication hook.
// Cold limits bound individual input/decoder terms; they are not a disk quota.
func (c *Collection) foldTypedGraph(ctx context.Context, cold typedGraphColdLimits, maxRows int, afterCapture func() error) (err error) {
	return c.foldTypedGraphTimed(ctx, cold, maxRows, afterCapture, nil)
}

func (c *Collection) foldTypedGraphTimed(ctx context.Context, cold typedGraphColdLimits, maxRows int, afterCapture func() error, timing *ColumnGraphBuildTiming) (err error) {
	started := time.Now()
	defer func() {
		if timing != nil {
			timing.Total = time.Since(started)
		}
	}()
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() || maxRows <= 0 || cold.ManifestRecords <= 0 || cold.ManifestBytes <= 0 || cold.AssetBytes <= 0 || cold.DecodedTermBytes <= 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil || !coord.typedGraphFoldActive.CompareAndSwap(false, true) {
		return ErrConcurrentMutation
	}
	defer coord.typedGraphFoldActive.Store(false)
	var captured columnStoreCompactionState
	var lease *ColumnAssetLifecyclePinSet
	var copy *typedGraphBaseCopy
	defer func() {
		if lease != nil {
			err = errors.Join(err, lease.Close())
		}
		if captured.snap != nil {
			err = errors.Join(err, captured.snap.Close())
		}
	}()
	unlock := c.lockCollectionSchemaWrite()
	err = c.flushCollectionWriteDomainsForSchemaMutation()
	if err == nil {
		err = WithVectorPartitionStorageBarrierV1(c.db.Dir(), func() error {
			var e error
			captured, _, e = c.loadColumnStoreCompactionStateWithBudget(ctx, &cold)
			if e != nil {
				return e
			}
			if captured.catalog.typedGraphBase == nil || len(captured.meta.VectorIndexes) != 1 || captured.meta.VectorIndexes[0].Strategy != VectorIndexStrategyColumnGraph {
				return ErrHybridSearchUnsupported
			}
			view, e := c.prepareColumnPhysicalScanSnapshotViewAtSnapshot(captured.snap, captured.catalog, captured.meta.Name, captured.baseRoot, captured.cfg, true)
			if e != nil {
				return e
			}
			refs, e := validateTypedGraphFoldView(view, cold, maxRows)
			if e != nil {
				return e
			}
			lease, e = c.acquireColumnAssetLifecyclePinSetOwned(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "typed_graph_fold", Refs: refs})
			if e != nil {
				return e
			}
			copy, e = prepareTypedGraphBaseCopy(captured.snap, captured.catalog, typedGraphCaptureBudget{records: typedGraphCaptureMaxRecords, bytes: typedGraphCaptureMaxBytes})
			return e
		})
	}
	unlock()
	if timing != nil {
		timing.Snapshot = time.Since(started)
	}
	if err != nil {
		return err
	}
	if afterCapture != nil {
		if err = afterCapture(); err != nil {
			return err
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	stage := time.Now()
	rows, _, err := c.materializeColumnStoreCompactionRows(ctx, captured, "")
	if timing != nil {
		timing.RowExtraction = time.Since(stage)
	}
	if err != nil {
		return err
	}
	stage = time.Now()
	prepared, err := c.prepareTypedGraphCapturedAssets(captured, rows)
	if timing != nil {
		timing.AssetPreparation = time.Since(stage)
	}
	if err != nil {
		return err
	}
	defer func() {
		if prepared.stableResources != nil {
			prepared.stableResources.Release()
		}
	}()
	baseManifest, err := encodeColumnManifestAtGeneration(ColumnPublishManifestEncodeInput{Collection: captured.meta.Name, ColumnStore: captured.cfg, Operation: ColumnPublishOperationInsert, AppliedCommandLSN: captured.manifest.AppliedCommandLSN, Prepared: prepared}, captured.manifest.Generation)
	if err != nil {
		return err
	}
	baseHeader, err := decodeColumnManifestSnapshotForScan(baseManifest.Records)
	if err != nil {
		return err
	}
	def := captured.meta.VectorIndexes[0]
	vectorColumn := -1
	for i, column := range captured.cfg.Columns {
		if column.Path == def.Field {
			vectorColumn = i
		}
	}
	if vectorColumn < 0 {
		return ErrHybridSearchUnsupported
	}
	var rowPart uint64
	for _, asset := range prepared.Assets {
		if asset.Ref.Kind == ColumnAssetKindTCS1PartImage {
			rowPart = asset.Ref.PartID
		}
	}
	graphRows := make([]columnVectorGraphAssetRow, len(rows))
	locators := make([]systemTargetEntry, len(rows))
	for i, row := range rows {
		ref := DocumentRowRef{DocumentID: row.ID, Generation: captured.manifest.Generation, PartID: rowPart, RowIndex: i, AppliedCommandLSN: captured.manifest.AppliedCommandLSN}
		vector := row.Values[vectorColumn].Float32Vector
		norm, e := columnVectorGraphInvNorm(vector)
		if e != nil {
			return e
		}
		graphRows[i] = columnVectorGraphAssetRow{ID: row.ID, Vector: vector, InvNorm: norm, BaseRowRef: ref}
		locators[i] = systemTargetEntry{key: row.ID, value: encodeColumnPrimaryRowLocator(ref)}
	}
	sort.Slice(locators, func(i, j int) bool { return bytes.Compare(locators[i].key, locators[j].key) < 0 })
	if err = buildColumnVectorGraphAdjacencyTimed(graphRows, def, timing); err != nil {
		return err
	}
	if err = copy.reserveManifest(def, graphRows); err != nil {
		return err
	}
	stage = time.Now()
	graph, records, identity, err := prepareColumnVectorGraphRebuildManifestForPublicationTimed(captured.meta.Name, captured.cfg, captured.meta.VectorIndexes, def, baseHeader, baseManifest.Records, captured.manifest.AppliedCommandLSN, graphRows, c.db.ColumnAssetRootDir(), c.db.StableResourceIdentityPinRegistry(), timing)
	if timing != nil {
		timing.AssetPreparation += time.Since(stage)
	}
	if err != nil {
		return err
	}
	defer graph.releaseStableResources()
	baseMeta, err := columnStoreCompactionUpdatedMeta(captured.meta, identity)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	unlock = c.lockCollectionSchemaWrite()
	err = c.flushCollectionWriteDomainsForSchemaMutation()
	if err == nil {
		err = WithVectorPartitionStorageBarrierV1(c.db.Dir(), func() error {
			return c.installTypedGraphFold(ctx, captured, copy, cold, maxRows, records, identity, baseMeta, locators, &prepared, &graph, timing)
		})
	}
	unlock()
	if err != nil {
		return err
	}
	// Seal once for explicit maintenance, never under collection admission.
	if err = c.db.Checkpoint(); err != nil {
		return err
	}
	if state := coord.typedPublication.Load(); state != nil {
		return c.reconcileTypedGraphPublication(state.limits, cold)
	}
	return nil
}

func (c *Collection) installTypedGraphFold(ctx context.Context, captured columnStoreCompactionState, copy *typedGraphBaseCopy, cold typedGraphColdLimits, maxRows int, baseRecords []columnManifestRecord, baseIdentity ColumnManifestIdentity, baseMeta CollectionMeta, locators []systemTargetEntry, prepared *ColumnPublishPreparedAssets, graph *columnVectorGraphPreparedPhysicalAsset, timing *ColumnGraphBuildTiming) (err error) {
	latest, _, err := c.loadColumnStoreCompactionStateWithBudget(ctx, &cold)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, latest.snap.Close()) }()
	old, now := captured.catalog.typedGraphBase, latest.catalog.typedGraphBase
	if latest.catalog.pager != captured.catalog.pager || !typedGraphBaseSchemaMatches(captured.meta, latest.meta) || old == nil || now == nil || !sameCollectionMeta(old.meta, now.meta) || !reflect.DeepEqual(old.roots, now.roots) {
		return ErrConcurrentMutation
	}
	view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshot(latest.snap, latest.catalog, latest.meta.Name, latest.baseRoot, latest.cfg, true)
	if err != nil {
		return err
	}
	if _, err := validateTypedGraphFoldView(view, cold, maxRows); err != nil {
		return err
	}
	// The lock-held current locator stream is independently charged. A valid
	// captured budget does not bound post-capture writes or malformed roots.
	if _, err := scanTypedGraphCaptureRoot(latest.snap, latest.catalog.rootID(collectionColumnRowLocatorRootName(latest.meta.Name)), &copy.remaining); err != nil {
		return err
	}
	currentRecords := make([]columnManifestRecord, 0, len(baseRecords)+len(latest.records))
	for _, record := range baseRecords {
		if !bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes) || latest.manifest.Generation == captured.manifest.Generation {
			currentRecords = append(currentRecords, record)
		}
	}
	if latest.manifest.Generation > captured.manifest.Generation {
		for _, record := range latest.records {
			if bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes) {
				currentRecords = append(currentRecords, record)
				continue
			}
			if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) || bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
				continue
			}
			part, e := decodeColumnManifestPartRecord(record.value)
			if e != nil {
				return e
			}
			if part.AssetRef.Generation > captured.manifest.Generation {
				currentRecords = append(currentRecords, record)
			}
		}
	}
	sortColumnManifestRecords(currentRecords)
	header, err := decodeColumnManifestSnapshotForScan(currentRecords)
	if err != nil {
		return err
	}
	identity := ColumnManifestIdentity{Generation: header.Generation, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{Collection: latest.meta.Name, ColumnStore: latest.cfg, Operation: header.Operation, AppliedCommandLSN: header.AppliedCommandLSN}, header.Generation, currentRecords)}
	updated, err := columnGraphRebuildUpdatedMeta(latest.meta, identity, header.AppliedCommandLSN)
	if err != nil {
		return err
	}
	_, _, mutationParts, err := decodeColumnManifestSnapshotViewForScan(currentRecords, latest.cfg.AssetManager.Namespace)
	if err != nil {
		return err
	}
	updated.Options.ColumnStore.PhysicalMutationParts = uint64(mutationParts)
	aliasInputs, err := copy.inputsWithLocator(encodeColumnManifestIdentityRecordArray(baseIdentity), baseRecords, &systemTargetIterator{entries: locators})
	if err != nil {
		return err
	}
	inputs := make([]backenddb.StorageMaintenanceRootDeltaPublishInput, 0, len(aliasInputs)+2)
	handedOff := false
	currentStart := 0
	defer func() {
		if handedOff {
			return
		}
		for _, input := range aliasInputs {
			if input.Iter != nil {
				err = errors.Join(err, input.Iter.Close())
			}
		}
		for _, input := range inputs[currentStart:] {
			err = errors.Join(err, input.Iter.Close())
			if input.DurableResources != nil {
				input.DurableResources.Release()
			}
		}
	}()
	aliasPositions := make([]int, len(aliasInputs))
	aliasRoots := make([]uint64, len(aliasInputs))
	for i, input := range aliasInputs {
		aliasPositions[i] = -1
		if !input.Iter.Valid() && input.BaseRoot != 0 {
			// Empty unchanged roots need no maintenance input. Retain exactly
			// the old independently owned root, verified by install preflight.
			e := errors.Join(input.Iter.Error(), input.Iter.Close())
			aliasInputs[i].Iter = nil
			if e != nil {
				currentStart = len(inputs)
				return e
			}
			aliasRoots[i] = input.BaseRoot
			continue
		}
		aliasPositions[i] = len(inputs)
		inputs = append(inputs, backenddb.StorageMaintenanceRootDeltaPublishInput{BaseRoot: input.BaseRoot, Iter: input.Iter, StoragePolicy: input.StoragePolicy})
	}
	currentStart = len(inputs)
	locatorName := collectionColumnRowLocatorRootName(latest.meta.Name)
	requirements, err := stableColumnManifestDurableRequirements(currentRecords, identity.Generation, latest.cfg.AssetManager.Namespace)
	if err != nil {
		return err
	}
	var locator iterator.UnsafeIterator = &systemTargetIterator{}
	if root := latest.catalog.rootID(locatorName); root != 0 {
		locator, err = latest.snap.IteratorAtRoot(root, nil, nil)
		if err != nil {
			return err
		}
	}
	remapped, err := typedGraphFoldLocatorDelta(locator, locators, captured.manifest.Generation)
	if err != nil {
		return err
	}
	// Every captured asset is also in currentRecords. Thus this exact closure
	// covers current U and captured T without consulting retired base aliases.
	inputs = append(inputs, backenddb.StorageMaintenanceRootDeltaPublishInput{BaseRoot: latest.baseRoot, Iter: columnStoreCompactionManifestDeltaIterator(encodeColumnManifestIdentityRecordArray(identity), latest.records, currentRecords), StoragePolicy: backenddb.OrderedRootStoragePagerLeaves, DurableResources: prepared.stableResources, DurableResourceRequirements: requirements})
	prepared.stableResources = nil
	rootNames := []string{latest.rootName}
	oldRoots := map[string]uint64{latest.rootName: latest.baseRoot, locatorName: latest.catalog.rootID(locatorName)}
	if remapped.Valid() || latest.catalog.rootID(locatorName) == 0 {
		inputs = append(inputs, backenddb.StorageMaintenanceRootDeltaPublishInput{BaseRoot: latest.catalog.rootID(locatorName), Iter: remapped, StoragePolicy: backenddb.OrderedRootStoragePagerLeaves})
		rootNames = append(rootNames, locatorName)
	} else if e := errors.Join(remapped.Error(), remapped.Close()); e != nil {
		return e
	}
	inputs[0].DurableResources = graph.stableResources
	graph.stableResources = nil
	var installed *typedGraphBaseAlias
	handedOff = true // publisher owns all iterators and both producer sets
	publicationStarted := time.Now()
	system, roots, err := c.db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(storagemaintenance.ColumnAssetRewritePlan(), inputs, func() error {
		if e := ctx.Err(); e != nil {
			return e
		}
		return c.validateColumnGraphRebuildSource(latest.catalog, latest.baseCommitSeq, latest.baseSystemRoot)
	}, func(roots []uint64) (iterator.UnsafeIterator, error) {
		if len(roots) != len(inputs) {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		var e error
		for i, position := range aliasPositions {
			if position >= 0 {
				aliasRoots[i] = roots[position]
			}
		}
		installed, e = copy.captured(baseMeta, aliasRoots)
		if e != nil {
			return nil, e
		}
		return c.buildColumnGraphRebuildSystemDeltaIterator(latest.meta, updated, latest.baseCommitSeq, latest.baseSystemRoot, rootNames, oldRoots, roots[currentStart:], installed)
	})
	if timing != nil {
		timing.Publication = time.Since(publicationStarted)
	}
	// A changed physical frontier fences derived state, including an ambiguous
	// accepted result. It never releases attempted encoded-output debt.
	coord := c.collectionSchemaCoordinator()
	if err == nil || !columnAssetRewritePublishFailedBeforeApply(err) {
		if state := coord.typedPublication.Load(); state != nil {
			invalid := *state
			invalid.invalid = true
			coord.typedPublication.CompareAndSwap(state, &invalid)
		}
	}
	if err != nil {
		return err
	}
	next := cloneCatalogWithRootUpdates(latest.catalog, updated, rootNames, roots[currentStart:])
	next.typedGraphBase = installed
	c.meta = updated
	c.rememberCatalogAtSystemRoot(system, next)
	c.noteWriteDomainCatalog(system, next)
	return nil
}

func validateTypedGraphFoldView(view columnPhysicalScanSnapshotView, cold typedGraphColdLimits, maxRows int) ([]ColumnAssetRef, error) {
	if err := validateTypedGraphOverlayVectorOwners(view.FullConfig); err != nil {
		return nil, err
	}
	physicalRows := 0
	for _, ref := range view.AssetRefs {
		if ref.Rows < 0 || ref.Rows > maxRows-physicalRows {
			return nil, errTypedGraphOverlayFoldNeeded
		}
		physicalRows += ref.Rows
	}
	if int64(physicalRows) > cold.DecodedTermBytes/int64(reflect.TypeFor[columnPhysicalVisibleRow]().Size()) || int64(len(view.FullConfig.Columns)) > cold.DecodedTermBytes/int64(typedGraphDeclaredValueHeaderBytes())/int64(max(physicalRows, 1)) {
		return nil, errTypedGraphOverlayFoldNeeded
	}
	for _, column := range view.FullConfig.Columns {
		if column.ValueType == ColumnStoreValueFloat32Vector && (column.VectorDims <= 0 || int64(column.VectorDims) > cold.DecodedTermBytes/4/int64(max(physicalRows, 1))) {
			return nil, errTypedGraphOverlayFoldNeeded
		}
	}
	refs := columnPhysicalScanSnapshotViewAssetRefs(view)
	remaining := cold.AssetBytes
	for _, ref := range refs {
		if ref.Length <= 0 || ref.Length > remaining {
			return nil, errTypedGraphOverlayFoldNeeded
		}
		remaining -= ref.Length
	}
	return refs, nil
}

// Walk ordered current locators once. Only surviving pre-T coordinates change;
// post-T replacements and reinserts retain their actual published coordinates.
func typedGraphFoldLocatorDelta(current iterator.UnsafeIterator, captured []systemTargetEntry, generation uint64) (_ iterator.UnsafeIterator, err error) {
	defer func() { err = errors.Join(err, current.Close()) }()
	entries := make([]systemTargetEntry, 0, len(captured))
	index := 0
	for ; current.Valid(); current.Next() {
		id := current.UnsafeKey()
		ref, e := decodeColumnPrimaryRowLocatorBorrowedID(id, current.UnsafeValue())
		if e != nil {
			return nil, e
		}
		if ref.Generation > generation {
			continue
		}
		for index < len(captured) && bytes.Compare(captured[index].key, id) < 0 {
			index++
		}
		if index == len(captured) || !bytes.Equal(captured[index].key, id) {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		entries = append(entries, captured[index])
	}
	return &systemTargetIterator{entries: entries}, current.Error()
}
