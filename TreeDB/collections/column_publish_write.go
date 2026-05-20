package collections

import (
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type columnWritePublishInput struct {
	meta               CollectionMeta
	catalog            *collectionCatalog
	baseCommitSeq      uint64
	baseSystemRoot     uint64
	rootNames          []string
	baseRootIDs        map[string]uint64
	commandWALIntent   *backenddb.CommandWALIntent
	operation          ColumnPublishOperation
	documents          []columnWriteDocument
	rows               int
	declaredRows       []columnDeclaredRow
	declaredRowsReady  bool
	commandBytes       int64
	rowRemainderBytes  int64
	columnPayloadBytes int64
}

func columnStoreWriteEnabled(meta CollectionMeta) bool {
	return meta.Options.ColumnStore != nil && meta.Options.ColumnStore.Enabled
}

// requireColumnStoreCommandWAL is an internal write-path guard. It requires a
// live collection handle because durability mode and command-WAL state are DB
// properties, not catalog metadata.
func (c *Collection) requireColumnStoreCommandWAL(meta CollectionMeta, commandWALIntent *backenddb.CommandWALIntent) error {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	profileSupport := cfg.ProfileSupport
	if profileSupport == "" {
		profileSupport = ColumnStoreProfileDurableOnly
	}
	if c == nil || c.db == nil {
		return errCollectionDBNil
	}
	if _, replay := commandWALIntent.ReplayAssignedLSN(); replay {
		return nil
	}
	durabilityMode := c.db.DurabilityMode()
	if !columnStoreProfileAllowsForegroundCommandWAL(profileSupport, durabilityMode) {
		return fmt.Errorf("%w: column-store writes require durable DB durability mode with command WAL enabled; relaxed durability modes are unsupported for column-store writes (collection=%q durability=%s command_wal=%t profile=%s)",
			backenddb.ErrCommandWALRejected,
			meta.Name,
			columnStoreDurabilityModeName(durabilityMode),
			c.db.CommandWALEnabled(),
			profileSupport,
		)
	}
	if c.commandWALActive(commandWALIntent) {
		return nil
	}
	return fmt.Errorf("%w: column-store writes require command WAL collection=%q", backenddb.ErrCommandWALRejected, meta.Name)
}

func requireColumnStoreWriteOperationSupported(meta CollectionMeta, operation ColumnPublishOperation) error {
	if !columnStoreWriteEnabled(meta) {
		return nil
	}
	if len(meta.Indexes) != 0 && columnStoreNeedsRetainedPayloadTransform(meta) {
		return fmt.Errorf("%w: unsupported column-store write operation: M13C retained payload reconstruction is not wired to secondary indexes yet collection=%q operation=%s indexes=%d",
			backenddb.ErrCommandWALRejected,
			meta.Name,
			operation,
			len(meta.Indexes),
		)
	}
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON {
		return fmt.Errorf("%w: M12C unsupported column-store write collection=%q operation=%s document_format=%q",
			backenddb.ErrCommandWALRejected,
			meta.Name,
			operation,
			meta.Options.DocumentFormat,
		)
	}
	switch operation {
	case ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete:
		return nil
	default:
		return fmt.Errorf("%w: unsupported column-store write operation collection=%q operation=%q",
			backenddb.ErrCommandWALRejected,
			meta.Name,
			operation,
		)
	}
}

func columnStoreProfileAllowsForegroundCommandWAL(profileSupport ColumnStoreProfileSupport, durabilityMode backenddb.DurabilityMode) bool {
	switch profileSupport {
	case ColumnStoreProfileDurableOnly, ColumnStoreProfileBenchmarkRelaxed:
		return durabilityMode == backenddb.DurabilityDurable
	default:
		return false
	}
}

func (c *Collection) publishRootDeltaGroupMaybeColumn(ordered []backenddb.OrderedRootDeltaPublishInput, input columnWritePublishInput) (uint64, []uint64, CollectionMeta, []string, error) {
	if err := c.requireColumnStoreCommandWAL(input.meta, input.commandWALIntent); err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(input.meta, input.operation); err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if !columnStoreWriteEnabled(input.meta) {
		newSystemRoot, rootIDs, err := c.publishRootDeltaGroupWithoutColumn(ordered, input)
		return newSystemRoot, rootIDs, input.meta, input.rootNames, err
	}
	if input.commandWALIntent == nil {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("%w: column-store publish requires command WAL intent", backenddb.ErrCommandWALContextMissingFrame)
	}
	preparedInput, err := prepareColumnWritePublishInputBeforeCommandWAL(input)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	input = preparedInput
	columnRootName := collectionColumnManifestRootName(input.meta.Name)
	columnBaseRoot := uint64(0)
	if input.catalog != nil {
		columnBaseRoot = input.catalog.rootID(columnRootName)
	}
	rootNames, baseRootIDs, appendErr := appendColumnManifestRootPublishBase(input.rootNames, input.baseRootIDs, columnRootName, columnBaseRoot)
	if appendErr != nil {
		return 0, nil, CollectionMeta{}, nil, appendErr
	}
	preflight := c.columnPublishRootDescriptorPreflight(input, rootNames, baseRootIDs)
	var plan ColumnPublishPlan
	var updatedMeta CollectionMeta
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(
		ordered,
		preflight,
		input.commandWALIntent,
		func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
			nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
			if err != nil {
				return nil, err
			}
			plan = nextPlan
			columnDelta, err := plan.RootDelta.OrderedRootDeltaPublishInput()
			if err != nil {
				return nil, err
			}
			return []backenddb.OrderedRootDeltaPublishInput{columnDelta}, nil
		},
		func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if plan.AppliedCommandLSN != ctx.AppliedCommandLSN {
				return nil, columnPublishPlanLSNMismatchError(input.meta, ctx.AppliedCommandLSN, plan.AppliedCommandLSN)
			}
			iter, nextMeta, err := c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
			if err == nil {
				updatedMeta = nextMeta
			}
			return iter, err
		},
	)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if updatedMeta.Name == "" {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("collections: column publish did not prepare updated metadata collection=%q operation=%s", input.meta.Name, input.operation)
	}
	return newSystemRoot, rootIDs, updatedMeta, rootNames, nil
}

func (c *Collection) publishRootDeltaBatchGroupMaybeColumn(ordered []backenddb.OrderedRootDeltaBatchPublishInput, preflight backenddb.OrderedRootGroupPreflight, input columnWritePublishInput) (uint64, []uint64, CollectionMeta, []string, error) {
	if err := c.requireColumnStoreCommandWAL(input.meta, input.commandWALIntent); err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(input.meta, input.operation); err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if !columnStoreWriteEnabled(input.meta) {
		newSystemRoot, rootIDs, err := c.publishRootDeltaBatchGroupWithoutColumn(ordered, preflight, input)
		return newSystemRoot, rootIDs, input.meta, input.rootNames, err
	}
	if input.commandWALIntent == nil {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("%w: column-store publish requires command WAL intent", backenddb.ErrCommandWALContextMissingFrame)
	}
	preparedInput, err := prepareColumnWritePublishInputBeforeCommandWAL(input)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	input = preparedInput
	columnRootName := collectionColumnManifestRootName(input.meta.Name)
	columnBaseRoot := uint64(0)
	if input.catalog != nil {
		columnBaseRoot = input.catalog.rootID(columnRootName)
	}
	rootNames, baseRootIDs, appendErr := appendColumnManifestRootPublishBase(input.rootNames, input.baseRootIDs, columnRootName, columnBaseRoot)
	if appendErr != nil {
		return 0, nil, CollectionMeta{}, nil, appendErr
	}
	preflight = combineOrderedRootGroupPreflight(preflight, c.columnPublishRootDescriptorPreflight(input, rootNames, baseRootIDs))
	var plan ColumnPublishPlan
	var updatedMeta CollectionMeta
	var cleanupColumnDelta func()
	buildColumnDelta := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaBatchPublishInput, error) {
		nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
		if err != nil {
			return nil, err
		}
		plan = nextPlan
		columnDelta, cleanup, err := plan.RootDelta.OrderedRootDeltaBatchPublishInput()
		if err != nil {
			return nil, err
		}
		cleanupColumnDelta = cleanup
		return []backenddb.OrderedRootDeltaBatchPublishInput{columnDelta}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if plan.AppliedCommandLSN != ctx.AppliedCommandLSN {
			return nil, columnPublishPlanLSNMismatchError(input.meta, ctx.AppliedCommandLSN, plan.AppliedCommandLSN)
		}
		iter, nextMeta, err := c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
		if err == nil {
			updatedMeta = nextMeta
		}
		return iter, err
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, buildColumnDelta, buildSystemDelta)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if cleanupColumnDelta != nil {
		cleanupColumnDelta()
	}
	if updatedMeta.Name == "" {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("collections: column publish did not prepare updated metadata collection=%q operation=%s", input.meta.Name, input.operation)
	}
	return newSystemRoot, rootIDs, updatedMeta, rootNames, nil
}

func (c *Collection) columnPublishRootDescriptorPreflight(input columnWritePublishInput, rootNames []string, baseRootIDs map[string]uint64) backenddb.OrderedRootGroupPreflight {
	return func() error {
		return c.validateColumnPublishRootDescriptorPreflight(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs)
	}
}

func (c *Collection) validateColumnPublishRootDescriptorPreflight(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64) error {
	if c == nil || c.db == nil {
		return backenddb.ErrClosed
	}
	for _, rootName := range rootNames {
		if _, ok := baseRootIDs[rootName]; !ok {
			return fmt.Errorf("collections: missing base root for collection %q root %q", meta.Name, rootName)
		}
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if currentSystemRoot == expectedSystemRoot && currentCommitSeq == expectedCommitSeq {
		return nil
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, meta.Name)
	if err != nil {
		return err
	}
	if catalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMetaIgnoringColumnManifestProgress(catalog.meta, meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
	}
	for _, rootName := range rootNames {
		want := baseRootIDs[rootName]
		if got := catalog.rootID(rootName); got != want {
			return errConcurrentRootModification(meta.Name, rootName)
		}
	}
	return nil
}

func sameCollectionMetaIgnoringColumnManifestProgress(a, b CollectionMeta) bool {
	return sameCollectionMeta(clearColumnManifestProgress(a), clearColumnManifestProgress(b))
}

func clearColumnManifestProgress(meta CollectionMeta) CollectionMeta {
	copied := copyCollectionMeta(meta)
	if copied.Options.ColumnStore == nil {
		return copied
	}
	cfg := *copied.Options.ColumnStore
	cfg.ActiveManifest = nil
	cfg.RecoveryAuthoritativeManifest = nil
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 0
	cfg.PhysicalMutationParts = 0
	copied.Options.ColumnStore = &cfg
	return copied
}

func combineOrderedRootGroupPreflight(first, second backenddb.OrderedRootGroupPreflight) backenddb.OrderedRootGroupPreflight {
	switch {
	case first == nil:
		return second
	case second == nil:
		return first
	default:
		return func() error {
			if err := first(); err != nil {
				return err
			}
			return second()
		}
	}
}

func prepareColumnWritePublishInputBeforeCommandWAL(input columnWritePublishInput) (columnWritePublishInput, error) {
	switch input.operation {
	case ColumnPublishOperationInsert, ColumnPublishOperationUpdate:
		if input.rows == 0 {
			input.declaredRowsReady = true
			return input, nil
		}
		if len(input.documents) != input.rows {
			return columnWritePublishInput{}, fmt.Errorf("collections: column physical asset %s documents=%d rows=%d", input.operation, len(input.documents), input.rows)
		}
		if normalizedDocumentFormat(input.meta.Options.DocumentFormat) != DocumentFormatJSON {
			return columnWritePublishInput{}, fmt.Errorf("collections: column physical asset %s requires JSON document format in M12A, got %q", input.operation, input.meta.Options.DocumentFormat)
		}
		if input.meta.Options.ColumnStore == nil {
			return columnWritePublishInput{}, fmt.Errorf("collections: column physical asset %s missing column-store config", input.operation)
		}
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(*input.meta.Options.ColumnStore, input.documents)
		if err != nil {
			return columnWritePublishInput{}, err
		}
		input.declaredRows = rows
		input.declaredRowsReady = true
		return input, nil
	case ColumnPublishOperationDelete:
		if len(input.documents) != input.rows {
			return columnWritePublishInput{}, fmt.Errorf("collections: column physical asset delete documents=%d rows=%d", len(input.documents), input.rows)
		}
		input.declaredRowsReady = true
		return input, nil
	default:
		return columnWritePublishInput{}, fmt.Errorf("collections: unsupported column publish operation %q", input.operation)
	}
}

func (c *Collection) publishRootDeltaGroupWithoutColumn(ordered []backenddb.OrderedRootDeltaPublishInput, input columnWritePublishInput) (uint64, []uint64, error) {
	if input.commandWALIntent != nil {
		return c.db.PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(ordered, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
		})
	}
	return c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
	})
}

func (c *Collection) publishRootDeltaBatchGroupWithoutColumn(ordered []backenddb.OrderedRootDeltaBatchPublishInput, preflight backenddb.OrderedRootGroupPreflight, input columnWritePublishInput) (uint64, []uint64, error) {
	if input.commandWALIntent != nil {
		if preflight != nil {
			return c.db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
			})
		}
		return c.db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
		})
	}
	if preflight != nil {
		return c.db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
		})
	}
	return c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
	})
}

func (c *Collection) buildColumnPublishPlanForCommandWALContext(ctx backenddb.CommandWALPublishContext, input columnWritePublishInput, baseManifestRootID uint64) (ColumnPublishPlan, error) {
	currentRecords, err := c.loadColumnManifestRecordsForPublish(baseManifestRootID, input.meta.Name, *input.meta.Options.ColumnStore)
	if err != nil {
		return ColumnPublishPlan{}, err
	}
	return BuildColumnPublishPlan(ColumnPublishPlanInput{
		Collection:             input.meta.Name,
		ColumnStore:            input.meta.Options.ColumnStore,
		ColumnStoreNormalized:  true,
		Operation:              input.operation,
		CurrentManifest:        input.meta.Options.ColumnStore.ActiveManifest,
		CurrentManifestRecords: currentRecords,
		AppliedCommandLSN:      ctx.AppliedCommandLSN,
		BaseManifestRootID:     baseManifestRootID,
		Hooks: ColumnPublishPlanHooks{
			PrepareAssets: func(hookInput ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
				return c.prepareColumnPhysicalAssetsForCommand(input, hookInput)
			},
			EncodeManifest: encodeColumnManifestIdentityForWrite,
		},
	})
}

func (c *Collection) loadColumnManifestRecordsForPublish(rootID uint64, collectionName string, cfg ColumnStoreConfig) ([]columnManifestRecord, error) {
	if rootID == 0 {
		return nil, nil
	}
	if cfg.ActiveManifest == nil {
		return nil, errors.New("collections: column publish existing manifest root requires active manifest identity")
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, errCollectionDBNil
	}
	defer func() { _ = snap.Close() }()
	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		return nil, fmt.Errorf("collections: validate existing column manifest identity for publish: %w", err)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return nil, fmt.Errorf("collections: load existing column manifest records: %w", err)
	}
	manifest, err := decodeColumnManifestRecords(records)
	if err != nil {
		return nil, fmt.Errorf("collections: decode existing column manifest records for publish: %w", err)
	}
	if err := validateColumnManifestSnapshot(manifest, records, cfg, *cfg.ActiveManifest, collectionName, "column publish"); err != nil {
		return nil, err
	}
	return records, nil
}

func (c *Collection) prepareColumnPhysicalAssetsForCommand(input columnWritePublishInput, hookInput ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
	prepared := ColumnPublishPreparedAssets{
		RowCount:           input.rows,
		CommandBytes:       input.commandBytes,
		RowRemainderBytes:  input.rowRemainderBytes,
		ColumnPayloadBytes: input.columnPayloadBytes,
	}
	switch input.operation {
	case ColumnPublishOperationInsert, ColumnPublishOperationUpdate:
		if input.rows == 0 {
			return prepared, nil
		}
		rows := input.declaredRows
		if !input.declaredRowsReady {
			var err error
			fallback, err := prepareColumnWritePublishInputBeforeCommandWAL(input)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			rows = fallback.declaredRows
		}
		if len(rows) != input.rows {
			return ColumnPublishPreparedAssets{}, fmt.Errorf("collections: column physical asset %s prepared rows=%d rows=%d", input.operation, len(rows), input.rows)
		}
		return c.prepareColumnPhysicalAssetRowsForCommand(prepared, input, hookInput, rows)
	case ColumnPublishOperationDelete:
		if len(input.documents) != input.rows {
			return ColumnPublishPreparedAssets{}, fmt.Errorf("collections: column physical asset delete documents=%d rows=%d", len(input.documents), input.rows)
		}
		if input.rows == 0 {
			return prepared, nil
		}
		rows := make([]columnDeclaredRow, len(input.documents))
		for i, doc := range input.documents {
			rows[i] = columnDeclaredRow{
				ID:      append([]byte(nil), doc.ID...),
				Deleted: true,
			}
		}
		return c.prepareColumnPhysicalAssetRowsForCommand(prepared, input, hookInput, rows)
	default:
		return ColumnPublishPreparedAssets{}, fmt.Errorf("collections: unsupported column publish operation %q", input.operation)
	}
}

func (c *Collection) prepareColumnPhysicalAssetRowsForCommand(prepared ColumnPublishPreparedAssets, input columnWritePublishInput, hookInput ColumnPublishAssetPrepareInput, rows []columnDeclaredRow) (ColumnPublishPreparedAssets, error) {
	generation := uint64(1)
	if hookInput.CurrentManifest != nil {
		generation = hookInput.CurrentManifest.Generation + 1
	}
	const partID = uint64(1)
	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        hookInput.Collection,
		Namespace:         hookInput.ColumnStore.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: hookInput.AppliedCommandLSN,
		Operation:         hookInput.Operation,
		SchemaHash:        hookInput.ColumnStore.SchemaHash,
		Columns:           hookInput.ColumnStore.Columns,
		Rows:              rows,
	})
	if err != nil {
		return ColumnPublishPreparedAssets{}, err
	}
	ref, err := writeColumnPhysicalAssetToManager(c.db.ColumnAssetRootDir(), hookInput.ColumnStore, encoded, generation, partID)
	if err != nil {
		return ColumnPublishPreparedAssets{}, err
	}
	if err := validateColumnPhysicalAssetPreparedRefForManifest(ref, hookInput.ColumnStore, generation, partID, len(encoded)); err != nil {
		return ColumnPublishPreparedAssets{}, err
	}
	if prepared.CommandBytes == 0 {
		prepared.CommandBytes = columnWriteDocumentsBytes(input.documents)
	}
	prepared.RowCount = summary.RowCount
	prepared.ColumnPayloadBytes = summary.PayloadBytes
	prepared.Assets = []ColumnPreparedAsset{{
		Ref:          ref,
		Bytes:        ref.Length,
		PublishID:    hookInput.AppliedCommandLSN,
		GenerationID: generation,
		Reason:       string(input.operation),
	}}
	if hookInput.Operation == ColumnPublishOperationInsert {
		for _, aggregate := range hookInput.ColumnStore.AggregateMetadata {
			metadata, ok, err := buildColumnAggregateMetadataAsset(hookInput.ColumnStore, rows, aggregate, hookInput.Collection, hookInput.ColumnStore.AssetManager.Namespace, generation, partID, hookInput.AppliedCommandLSN)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			if !ok {
				continue
			}
			encodedMetadata, err := encodeColumnAggregateMetadataAsset(metadata)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			metadataRef, err := writeColumnAggregateMetadataAssetToManager(c.db.ColumnAssetRootDir(), hookInput.ColumnStore, encodedMetadata, generation, partID)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			if metadataRef.Namespace != hookInput.ColumnStore.AssetManager.Namespace || metadataRef.Kind != ColumnAssetKindTCS1AggregateMetadata ||
				metadataRef.Generation != generation || metadataRef.PartID != partID || metadataRef.Length != int64(len(encodedMetadata)) {
				return ColumnPublishPreparedAssets{}, fmt.Errorf("collections: invalid aggregate metadata asset ref %+v", metadataRef)
			}
			prepared.Assets = append(prepared.Assets, ColumnPreparedAsset{
				Ref:          metadataRef,
				Bytes:        metadataRef.Length,
				PublishID:    hookInput.AppliedCommandLSN,
				GenerationID: generation,
				Reason:       aggregate.Name,
			})
		}
	}
	return prepared, nil
}

func validateColumnPhysicalAssetPreparedRefForManifest(ref ColumnAssetRef, cfg ColumnStoreConfig, generation, partID uint64, payloadLen int) error {
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return err
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: column physical asset manifest validation requires asset manager")
	}
	if ref.Kind != ColumnAssetKindTCS1PartImage {
		return fmt.Errorf("collections: column physical asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1PartImage)
	}
	if ref.Namespace != cfg.AssetManager.Namespace {
		return fmt.Errorf("collections: column physical asset namespace=%q want %q", ref.Namespace, cfg.AssetManager.Namespace)
	}
	if ref.Generation != generation {
		return fmt.Errorf("collections: column physical asset generation=%d want %d", ref.Generation, generation)
	}
	if ref.PartID != partID {
		return fmt.Errorf("collections: column physical asset part_id=%d want %d", ref.PartID, partID)
	}
	if ref.Length != int64(payloadLen) {
		return fmt.Errorf("collections: column physical asset length=%d want %d", ref.Length, payloadLen)
	}
	return nil
}

func columnWriteDocumentsBytes(docs []columnWriteDocument) int64 {
	var total int64
	for _, doc := range docs {
		total += int64(len(doc.ID) + len(doc.Document))
	}
	return total
}

func encodeColumnManifestIdentityForWrite(input ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
	return encodeColumnManifestForWrite(input)
}

func appendColumnManifestRootPublishBase(rootNames []string, baseRootIDs map[string]uint64, columnRootName string, columnBaseRoot uint64) ([]string, map[string]uint64, error) {
	for _, rootName := range rootNames {
		if rootName == columnRootName {
			return nil, nil, fmt.Errorf("collections: column manifest root %q must be published by the column context delta, not the row root group", columnRootName)
		}
	}
	if baseRootIDs == nil {
		baseRootIDs = make(map[string]uint64, 1)
	}
	baseRootIDs[columnRootName] = columnBaseRoot
	return append(rootNames, columnRootName), baseRootIDs, nil
}

func cloneColumnPublishRootNames(rootNames []string) []string {
	if rootNames == nil {
		return nil
	}
	out := make([]string, 0, len(rootNames)+1)
	return append(out, rootNames...)
}

func cloneColumnPublishBaseRootIDs(baseRootIDs map[string]uint64) map[string]uint64 {
	if baseRootIDs == nil {
		return nil
	}
	out := make(map[string]uint64, len(baseRootIDs)+1)
	for name, rootID := range baseRootIDs {
		out[name] = rootID
	}
	return out
}

func columnPublishPlanLSNMismatchError(meta CollectionMeta, ctxLSN, planLSN uint64) error {
	return fmt.Errorf("collections: column publish plan LSN mismatch collection=%q ctx_lsn=%d plan_lsn=%d", meta.Name, ctxLSN, planLSN)
}

func validateColumnPublishPlanRootForMeta(meta CollectionMeta, plan ColumnPublishPlan) (string, error) {
	rootName := plan.ManifestRootName
	if rootName == "" {
		rootName = plan.RootDelta.RootName
	}
	if rootName == "" {
		return "", errors.New("collections: column publish plan missing manifest root name")
	}
	if plan.Collection != meta.Name {
		return "", fmt.Errorf("collections: column publish plan collection %q does not match collection %q", plan.Collection, meta.Name)
	}
	expectedRootName := collectionColumnManifestRootName(meta.Name)
	if rootName != expectedRootName {
		return "", fmt.Errorf("collections: column publish plan root %q does not match collection root %q", rootName, expectedRootName)
	}
	if plan.RootDelta.RootName != "" && plan.RootDelta.RootName != rootName {
		return "", fmt.Errorf("collections: column publish plan root %q does not match root delta %q", rootName, plan.RootDelta.RootName)
	}
	rootIdentity := plan.RootDelta.Identity
	normalizeColumnManifestIdentityDefaults(&rootIdentity)
	if err := validateColumnManifestIdentity(rootIdentity); err != nil {
		return "", err
	}
	if plan.RootDelta.IdentityRecord != encodeColumnManifestIdentityRecordArray(rootIdentity) {
		return "", errors.New("collections: column publish plan root identity record does not match root identity")
	}
	activeIdentity := plan.UpdatedActiveManifest
	normalizeColumnManifestIdentityDefaults(&activeIdentity)
	if activeIdentity != rootIdentity {
		return "", errors.New("collections: column publish plan active manifest identity does not match root delta identity")
	}
	recoveryIdentity := plan.RecoveryAuthoritativeManifest
	normalizeColumnManifestIdentityDefaults(&recoveryIdentity)
	if recoveryIdentity != rootIdentity {
		return "", errors.New("collections: column publish plan recovery-authoritative manifest identity does not match root delta identity")
	}
	return rootName, nil
}

func columnPublishUpdatedMeta(base CollectionMeta, plan ColumnPublishPlan) (CollectionMeta, error) {
	if !plan.Enabled {
		return base, nil
	}
	updated := copyCollectionMeta(base)
	if updated.Options.ColumnStore == nil || !updated.Options.ColumnStore.Enabled {
		return CollectionMeta{}, errColumnPublishPlanRequiresEnabledColumnStore
	}
	cfg := updated.Options.ColumnStore.copy()
	active := plan.UpdatedActiveManifest
	recovery := plan.RecoveryAuthoritativeManifest
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	cfg.RecoveryAuthoritativeAppliedCommandLSN = plan.RecoveryAuthoritativeAppliedCommandLSN
	mutationParts, err := columnPublishPhysicalMutationParts(base.Options.ColumnStore, plan)
	if err != nil {
		return CollectionMeta{}, err
	}
	cfg.PhysicalMutationParts = mutationParts
	updated.Options.ColumnStore = &cfg
	return normalizeCollectionMeta(updated)
}

func columnPublishPhysicalMutationParts(base *ColumnStoreConfig, plan ColumnPublishPlan) (uint64, error) {
	var parts uint64
	if base != nil {
		parts = base.PhysicalMutationParts
	}
	if plan.Operation != ColumnPublishOperationInsert {
		add := uint64(len(plan.PreparedAssets))
		if ^uint64(0)-parts < add {
			return 0, errors.New("collections: physical mutation part count overflow")
		}
		parts += add
	}
	return parts, nil
}

func (c *Collection) buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64, plan ColumnPublishPlan) (iterator.UnsafeIterator, error) {
	iter, _, err := c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
	return iter, err
}

func (c *Collection) buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64, plan ColumnPublishPlan) (iterator.UnsafeIterator, CollectionMeta, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, CollectionMeta{}, unexpectedOrderedRootCountError(meta.Name, len(rootNames), len(rootIDs))
	}
	if !plan.Enabled {
		return nil, CollectionMeta{}, errors.New("collections: disabled column publish plan cannot build system delta")
	}
	columnRootName, err := validateColumnPublishPlanRootForMeta(meta, plan)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	columnRootIndex := -1
	for i, rootName := range rootNames {
		if rootName == columnRootName {
			columnRootIndex = i
			break
		}
	}
	if columnRootIndex < 0 {
		return nil, CollectionMeta{}, fmt.Errorf("collections: column manifest root %q missing from published root descriptors", columnRootName)
	}
	columnBaseRoot, ok := baseRootIDs[columnRootName]
	if !ok {
		return nil, CollectionMeta{}, fmt.Errorf("collections: missing base root for collection %q root %q", meta.Name, columnRootName)
	}
	if plan.ManifestRootBaseID != columnBaseRoot || plan.RootDelta.BaseRootID != columnBaseRoot {
		return nil, CollectionMeta{}, errConcurrentRootModification(meta.Name, columnRootName)
	}
	if err := c.validateRootDescriptorSystemDeltaForMeta(meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, CollectionMeta{}, err
	}
	if rootIDs[columnRootIndex] == 0 {
		return nil, CollectionMeta{}, errors.New("collections: column manifest root publish returned zero root")
	}
	// The ordered-root group just built this root. Avoid reading it through the
	// pre-commit snapshot because compressed roots may depend on value-log
	// segment visibility published by the enclosing commit.
	updatedMeta, err := columnPublishUpdatedMeta(meta, plan)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	encodedMeta, err := encodeNormalizedCollectionMeta(updatedMeta)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	updates := make(map[string][]byte, len(rootNames)+1)
	updates[systemCollectionMetaKey(updatedMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		if rootIDs[i] == 0 {
			return nil, CollectionMeta{}, fmt.Errorf("collections: ordered root publish returned zero root for %q", rootName)
		}
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	iter, err := buildSystemDeltaIterator(updates)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	return iter, updatedMeta, nil
}
