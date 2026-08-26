package collections

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type columnPhysicalAssetPreparationAfterPrepareHookState struct {
	hook func(ColumnPublishPreparedAssets) error
}

var columnPhysicalAssetPreparationAfterPrepareHook atomic.Pointer[columnPhysicalAssetPreparationAfterPrepareHookState]

func runColumnPhysicalAssetPreparationAfterPrepareHook(prepared ColumnPublishPreparedAssets) error {
	state := columnPhysicalAssetPreparationAfterPrepareHook.Load()
	if state == nil || state.hook == nil {
		return nil
	}
	return state.hook(prepared)
}

func setColumnPhysicalAssetPreparationAfterPrepareTestHook(hook func(ColumnPublishPreparedAssets) error) func() {
	previous := columnPhysicalAssetPreparationAfterPrepareHook.Load()
	columnPhysicalAssetPreparationAfterPrepareHook.Store(&columnPhysicalAssetPreparationAfterPrepareHookState{hook: hook})
	return func() {
		columnPhysicalAssetPreparationAfterPrepareHook.Store(previous)
	}
}

type columnWritePublishInput struct {
	meta                  CollectionMeta
	catalog               *collectionCatalog
	baseCommitSeq         uint64
	baseSystemRoot        uint64
	rootNames             []string
	baseRootIDs           map[string]uint64
	commandWALIntent      *backenddb.CommandWALIntent
	rawPublishLocked      bool
	operation             ColumnPublishOperation
	documents             []columnWriteDocument
	sourceDeleteDocuments []columnWriteDocument
	rows                  int
	declaredRows          []columnDeclaredRow
	declaredRowsReady     bool
	partIDOffset          uint64
	documentExtraction    time.Duration
	commandBytes          int64
	rowRemainderBytes     int64
	columnPayloadBytes    int64
	insertStats           *CollectionInsertStats
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
		return fmt.Errorf("%w: unsupported column-store write operation: retained payload reconstruction is not wired to secondary indexes yet collection=%q operation=%q indexes=%d",
			backenddb.ErrCommandWALRejected,
			meta.Name,
			operation,
			len(meta.Indexes),
		)
	}
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON {
		return fmt.Errorf("%w: unsupported column-store write collection=%q operation=%q document_format=%q",
			backenddb.ErrCommandWALRejected,
			meta.Name,
			operation,
			meta.Options.DocumentFormat,
		)
	}
	layout, err := ResolveTypedStorageLayout(meta)
	if err != nil {
		return fmt.Errorf("%w: column-store write collection=%q operation=%q: %v", backenddb.ErrCommandWALRejected, meta.Name, operation, err)
	}
	if err := layout.EnsurePublicationSupported(); err != nil {
		return fmt.Errorf("%w: column-store write collection=%q operation=%q: %v", backenddb.ErrCommandWALRejected, meta.Name, operation, err)
	}
	switch operation {
	case ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete:
		return nil
	default:
		return fmt.Errorf("%w: unsupported column-store write collection=%q operation=%q",
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
	locatorRootName := collectionColumnRowLocatorRootName(input.meta.Name)
	locatorBaseRoot := uint64(0)
	if input.catalog != nil {
		locatorBaseRoot = input.catalog.rootID(locatorRootName)
	}
	rootNames, baseRootIDs, appendErr = appendColumnManifestRootPublishBase(rootNames, baseRootIDs, locatorRootName, locatorBaseRoot)
	if appendErr != nil {
		return 0, nil, CollectionMeta{}, nil, appendErr
	}
	preflight := c.columnPublishRootDescriptorPreflight(input, rootNames, baseRootIDs)
	var plan ColumnPublishPlan
	defer func() { plan.releaseStableResources() }()
	var updatedMeta CollectionMeta
	var newSystemRoot uint64
	var rootIDs []uint64
	buildColumnDelta := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
		stageStart := time.Now()
		defer func() {
			if input.insertStats != nil {
				input.insertStats.ColumnPublishBuildColumnDelta += time.Since(stageStart)
			}
		}()
		nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
		if err != nil {
			return nil, err
		}
		plan.releaseStableResources()
		plan = nextPlan
		recordColumnPublishPlanStats(input.insertStats, plan)
		materializeStart := time.Now()
		columnDelta, err := plan.RootDelta.OrderedRootDeltaPublishInput()
		recordColumnPublishRootDeltaMaterialization(input.insertStats, time.Since(materializeStart))
		if err != nil {
			return nil, err
		}
		if plan.durableResourceRequirementsFallback != nil {
			if err := ctx.RegisterDurableLogicalObligationAppendMutation(plan.durableResourceMutation, plan.durableResourceRequirementWork, plan.durableResourceRequirementsFallback); err != nil {
				_ = columnDelta.Iter.Close()
				return nil, fmt.Errorf("collections: register column publish durable append mutation: %w", err)
			}
		} else {
			if err := ctx.RegisterDurableLogicalObligationRequirements(plan.durableResourceRequirements); err != nil {
				_ = columnDelta.Iter.Close()
				return nil, fmt.Errorf("collections: register column publish durable requirements: %w", err)
			}
			if err := ctx.RegisterDurableLogicalObligationMutation(plan.durableResourceMutation); err != nil {
				_ = columnDelta.Iter.Close()
				return nil, fmt.Errorf("collections: register column publish durable mutation: %w", err)
			}
			if err := ctx.RecordDurableLogicalObligationRequirementWork(plan.durableResourceRequirementWork); err != nil {
				_ = columnDelta.Iter.Close()
				return nil, fmt.Errorf("collections: record column publish durable requirement work: %w", err)
			}
		}
		if err := ctx.RegisterDurableResources(plan.takeStableResources()); err != nil {
			_ = columnDelta.Iter.Close()
			return nil, fmt.Errorf("collections: register column publish durable resources: %w", err)
		}
		locatorPolicy, err := collectionRootStoragePolicyForDB(c.db, input.meta, locatorRootName)
		if err != nil {
			_ = columnDelta.Iter.Close()
			return nil, err
		}
		locatorDelta, err := buildColumnPrimaryRowLocatorDelta(plan, input.documents, locatorBaseRoot, locatorPolicy)
		if err != nil {
			_ = columnDelta.Iter.Close()
			return nil, err
		}
		return []backenddb.OrderedRootDeltaPublishInput{columnDelta, locatorDelta}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		stageStart := time.Now()
		defer func() {
			if input.insertStats != nil {
				input.insertStats.ColumnPublishBuildSystemDelta += time.Since(stageStart)
			}
		}()
		if plan.AppliedCommandLSN != ctx.AppliedCommandLSN {
			return nil, columnPublishPlanLSNMismatchError(input.meta, ctx.AppliedCommandLSN, plan.AppliedCommandLSN)
		}
		iter, nextMeta, err := c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
		if err == nil {
			updatedMeta = nextMeta
		}
		return iter, err
	}
	var publishTiming backenddb.CommandWALPublishTiming
	previousTiming := input.commandWALIntent.SetPublishTiming(&publishTiming)
	defer func() {
		previousTiming.Add(publishTiming)
		input.commandWALIntent.SetPublishTiming(previousTiming)
	}()
	commitStart := time.Now()
	if input.rawPublishLocked {
		newSystemRoot, rootIDs, err = c.db.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(
			ordered,
			preflight,
			input.commandWALIntent,
			buildColumnDelta,
			buildSystemDelta,
		)
	} else {
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(
			ordered,
			preflight,
			input.commandWALIntent,
			buildColumnDelta,
			buildSystemDelta,
		)
	}
	recordColumnPublishCommit(input.insertStats, time.Since(commitStart))
	recordColumnPublishTiming(input.insertStats, publishTiming)
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
	locatorRootName := collectionColumnRowLocatorRootName(input.meta.Name)
	locatorBaseRoot := uint64(0)
	if input.catalog != nil {
		locatorBaseRoot = input.catalog.rootID(locatorRootName)
	}
	rootNames, baseRootIDs, appendErr = appendColumnManifestRootPublishBase(rootNames, baseRootIDs, locatorRootName, locatorBaseRoot)
	if appendErr != nil {
		return 0, nil, CollectionMeta{}, nil, appendErr
	}
	preflight = combineOrderedRootGroupPreflight(preflight, c.columnPublishRootDescriptorPreflight(input, rootNames, baseRootIDs))
	var plan ColumnPublishPlan
	defer func() { plan.releaseStableResources() }()
	var updatedMeta CollectionMeta
	var cleanupColumnDelta func()
	buildColumnDelta := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaBatchPublishInput, error) {
		stageStart := time.Now()
		defer func() {
			if input.insertStats != nil {
				input.insertStats.ColumnPublishBuildColumnDelta += time.Since(stageStart)
			}
		}()
		nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
		if err != nil {
			return nil, err
		}
		plan.releaseStableResources()
		plan = nextPlan
		recordColumnPublishPlanStats(input.insertStats, plan)
		materializeStart := time.Now()
		columnDelta, cleanup, err := plan.RootDelta.OrderedRootDeltaBatchPublishInput()
		recordColumnPublishRootDeltaMaterialization(input.insertStats, time.Since(materializeStart))
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			return nil, err
		}
		cleanupColumnDelta = cleanup
		if plan.durableResourceRequirementsFallback != nil {
			if err := ctx.RegisterDurableLogicalObligationAppendMutation(plan.durableResourceMutation, plan.durableResourceRequirementWork, plan.durableResourceRequirementsFallback); err != nil {
				if cleanupColumnDelta != nil {
					cleanupColumnDelta()
					cleanupColumnDelta = nil
				}
				return nil, fmt.Errorf("collections: register column publish durable append mutation: %w", err)
			}
		} else {
			if err := ctx.RegisterDurableLogicalObligationRequirements(plan.durableResourceRequirements); err != nil {
				if cleanupColumnDelta != nil {
					cleanupColumnDelta()
					cleanupColumnDelta = nil
				}
				return nil, fmt.Errorf("collections: register column publish durable requirements: %w", err)
			}
			if err := ctx.RegisterDurableLogicalObligationMutation(plan.durableResourceMutation); err != nil {
				if cleanupColumnDelta != nil {
					cleanupColumnDelta()
					cleanupColumnDelta = nil
				}
				return nil, fmt.Errorf("collections: register column publish durable mutation: %w", err)
			}
			if err := ctx.RecordDurableLogicalObligationRequirementWork(plan.durableResourceRequirementWork); err != nil {
				if cleanupColumnDelta != nil {
					cleanupColumnDelta()
					cleanupColumnDelta = nil
				}
				return nil, fmt.Errorf("collections: record column publish durable requirement work: %w", err)
			}
		}
		if err := ctx.RegisterDurableResources(plan.takeStableResources()); err != nil {
			if cleanupColumnDelta != nil {
				cleanupColumnDelta()
				cleanupColumnDelta = nil
			}
			return nil, fmt.Errorf("collections: register column publish durable resources: %w", err)
		}
		locatorPolicy, err := collectionRootStoragePolicyForDB(c.db, input.meta, locatorRootName)
		if err != nil {
			if cleanupColumnDelta != nil {
				cleanupColumnDelta()
				cleanupColumnDelta = nil
			}
			return nil, err
		}
		var locatorDelta backenddb.OrderedRootDeltaBatchPublishInput
		var locatorCleanup func()
		if len(input.sourceDeleteDocuments) != 0 {
			locatorDelta, locatorCleanup, err = buildColumnSourceReplacementRowLocatorDeltaBatch(plan, input.sourceDeleteDocuments, input.documents, locatorBaseRoot, locatorPolicy)
		} else {
			locatorDelta, locatorCleanup, err = buildColumnPrimaryRowLocatorDeltaBatch(plan, input.documents, locatorBaseRoot, locatorPolicy)
		}
		if err != nil {
			if cleanupColumnDelta != nil {
				cleanupColumnDelta()
				cleanupColumnDelta = nil
			}
			return nil, err
		}
		columnCleanup := cleanupColumnDelta
		cleanupColumnDelta = func() {
			columnCleanup()
			locatorCleanup()
		}
		return []backenddb.OrderedRootDeltaBatchPublishInput{columnDelta, locatorDelta}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		stageStart := time.Now()
		defer func() {
			if input.insertStats != nil {
				input.insertStats.ColumnPublishBuildSystemDelta += time.Since(stageStart)
			}
		}()
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
	var publishTiming backenddb.CommandWALPublishTiming
	previousTiming := input.commandWALIntent.SetPublishTiming(&publishTiming)
	defer func() {
		previousTiming.Add(publishTiming)
		input.commandWALIntent.SetPublishTiming(previousTiming)
	}()
	commitStart := time.Now()
	if input.rawPublishLocked {
		newSystemRoot, rootIDs, err = c.db.PublishStagedOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, buildColumnDelta, buildSystemDelta)
	} else {
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, buildColumnDelta, buildSystemDelta)
	}
	recordColumnPublishCommit(input.insertStats, time.Since(commitStart))
	recordColumnPublishTiming(input.insertStats, publishTiming)
	if err != nil {
		// The DB publish helper owns context-built batch deltas on publish errors.
		return 0, nil, CollectionMeta{}, nil, err
	}
	if cleanupColumnDelta != nil {
		cleanupColumnDelta()
		cleanupColumnDelta = nil
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
		if input.declaredRowsReady {
			if len(input.declaredRows) != input.rows {
				return columnWritePublishInput{}, fmt.Errorf("collections: column physical asset %s prepared rows=%d rows=%d", input.operation, len(input.declaredRows), input.rows)
			}
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
		start := time.Now()
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(*input.meta.Options.ColumnStore, input.documents)
		input.documentExtraction = time.Since(start)
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

func recordColumnPublishPlanStats(stats *CollectionInsertStats, plan ColumnPublishPlan) {
	if stats == nil || !plan.Enabled {
		return
	}
	metrics := plan.StageMetrics
	stats.ColumnPublishDocumentExtraction += metrics.DocumentExtraction
	stats.ColumnPublishDeclaredColumnEncoding += metrics.DeclaredColumnEncoding
	stats.ColumnPublishAssetPreparation += metrics.AssetPreparation
	stats.ColumnPublishRowAssetPreparation += metrics.AssetMetrics.RowAssetDuration
	stats.ColumnPublishTypedColumnPreparation += metrics.AssetMetrics.TypedColumnPartDuration
	stats.ColumnPublishTypedColumnDictionaryBuild += metrics.AssetMetrics.TypedColumnDictionaryBuild
	stats.ColumnPublishTypedColumnRowMaterialization += metrics.AssetMetrics.TypedColumnRowMaterialization
	stats.ColumnPublishTypedColumnPartBuild += metrics.AssetMetrics.TypedColumnPartBuild
	stats.ColumnPublishTypedColumnImageBuild += metrics.AssetMetrics.TypedColumnImageBuild
	stats.ColumnPublishDictionaryPreparation += metrics.AssetMetrics.DictionarySidecarDuration
	stats.ColumnPublishInt64Preparation += metrics.AssetMetrics.Int64SidecarDuration
	stats.ColumnPublishAggregateMetadataPrepare += metrics.AssetMetrics.AggregateMetadataDuration
	stats.ColumnPublishRowSidecarSharedBuild += metrics.AssetMetrics.RowSidecarSharedBuildDuration
	stats.ColumnPublishAssetAppend += metrics.AssetMetrics.SharedAppendDuration
	stats.ColumnPublishAssetAppendOpen += metrics.AssetMetrics.SharedAppendOpenDuration
	stats.ColumnPublishAssetAppendWrite += metrics.AssetMetrics.SharedAppendWriteDuration
	stats.ColumnPublishAssetAppendClose += metrics.AssetMetrics.SharedAppendCloseDuration
	stats.ColumnPublishAssetAppendFileSync += metrics.AssetMetrics.SharedAppendFileSyncDuration
	stats.ColumnPublishAssetAppendFileClose += metrics.AssetMetrics.SharedAppendFileCloseDuration
	stats.ColumnPublishAssetAppendDirSync += metrics.AssetMetrics.SharedAppendDirSyncDuration
	stats.ColumnPublishAssetAppendCleanup += metrics.AssetMetrics.SharedAppendCleanupDuration
	stats.ColumnPublishAssetAppenderCloseCount += metrics.AssetMetrics.SharedAppendCloseCount
	stats.ColumnPublishAssetAppendFileSyncCount += metrics.AssetMetrics.SharedAppendFileSyncCount
	stats.ColumnPublishAssetSyncEpochCount += metrics.AssetMetrics.SharedAppendSyncEpochCount
	stats.ColumnPublishSharedSegmentAppenderCloseCount += metrics.AssetMetrics.SharedSegmentAppendCloseCount
	stats.ColumnPublishSharedSegmentAppendFileSyncCount += metrics.AssetMetrics.SharedSegmentAppendFileSyncCount
	stats.ColumnPublishSharedSegmentAppendSyncEpochCount += metrics.AssetMetrics.SharedSegmentAppendSyncEpochCount
	stats.ColumnPublishDirectViewSegmentAppenderCloseCount += metrics.AssetMetrics.DirectViewSegmentAppendCloseCount
	stats.ColumnPublishDirectViewSegmentAppendFileSyncCount += metrics.AssetMetrics.DirectViewSegmentAppendFileSyncCount
	stats.ColumnPublishDirectViewSegmentAppendSyncEpochCount += metrics.AssetMetrics.DirectViewSegmentAppendSyncEpochCount
	stats.ColumnPublishManifestEncode += metrics.ManifestEncode
	stats.ColumnPublishAssetClosureValidation += metrics.AssetClosureValidation
	stats.ColumnPublishRootDeltaConstruction += metrics.RootDeltaConstruction
	stats.ColumnPublishSystemDeltaConstruction += metrics.SystemDeltaConstruction
	stats.ColumnPublishRows += plan.Rows
	stats.ColumnPublishPreparedAssets += len(plan.PreparedAssets)
	stats.ColumnPublishRowAssetBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishRowAssetBytes, metrics.AssetMetrics.RowAssetBytes)
	stats.ColumnPublishRowAssetCount += metrics.AssetMetrics.RowAssetCount
	stats.ColumnPublishTypedColumnBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishTypedColumnBytes, metrics.AssetMetrics.TypedColumnPartBytes)
	stats.ColumnPublishTypedColumnCount += metrics.AssetMetrics.TypedColumnPartCount
	stats.ColumnPublishDictionaryBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishDictionaryBytes, metrics.AssetMetrics.DictionarySidecarBytes)
	stats.ColumnPublishDictionaryCount += metrics.AssetMetrics.DictionarySidecarCount
	stats.ColumnPublishInt64Bytes = saturatingAddNonNegativeInt64(stats.ColumnPublishInt64Bytes, metrics.AssetMetrics.Int64SidecarBytes)
	stats.ColumnPublishInt64Count += metrics.AssetMetrics.Int64SidecarCount
	stats.ColumnPublishAggregateMetadataBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishAggregateMetadataBytes, metrics.AssetMetrics.AggregateMetadataBytes)
	stats.ColumnPublishAggregateMetadataCount += metrics.AssetMetrics.AggregateMetadataCount
	stats.ColumnPublishSharedAppendBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishSharedAppendBytes, metrics.AssetMetrics.SharedAppendBytes)
	stats.ColumnPublishSharedAppendCount += metrics.AssetMetrics.SharedAppendCount
	stats.ColumnPublishSharedSegmentAppendBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishSharedSegmentAppendBytes, metrics.AssetMetrics.SharedSegmentAppendBytes)
	stats.ColumnPublishSharedSegmentAppendCount += metrics.AssetMetrics.SharedSegmentAppendCount
	stats.ColumnPublishDirectViewSegmentAppendBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishDirectViewSegmentAppendBytes, metrics.AssetMetrics.DirectViewSegmentAppendBytes)
	stats.ColumnPublishDirectViewSegmentAppendCount += metrics.AssetMetrics.DirectViewSegmentAppendCount
	stats.ColumnPublishRequiredAssetBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishRequiredAssetBytes, plan.RequiredAssetBytes)
	stats.ColumnPublishManifestBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishManifestBytes, plan.ManifestBytes)
	stats.ColumnPublishManifestMutationRecords += plan.ManifestMutationRecords
	stats.ColumnPublishManifestMutationBytes = saturatingAddNonNegativeInt64(stats.ColumnPublishManifestMutationBytes, plan.ManifestMutationBytes)
}

func recordColumnPublishRootDeltaMaterialization(stats *CollectionInsertStats, elapsed time.Duration) {
	if stats == nil {
		return
	}
	stats.ColumnPublishRootDeltaMaterialization += elapsed
}

func recordColumnPublishCommit(stats *CollectionInsertStats, elapsed time.Duration) {
	if stats == nil {
		return
	}
	stats.ColumnPublishCommit += elapsed
}

func recordColumnPublishTiming(stats *CollectionInsertStats, timing backenddb.CommandWALPublishTiming) {
	if stats == nil {
		return
	}
	stats.ColumnPublishWriteLockWait += timing.WriteLockWait
	stats.ColumnPublishPreflight += timing.Preflight
	stats.ColumnPublishCommandWALAppend += timing.Append
	stats.ColumnPublishOrderedRootApply += timing.RootApply
	stats.ColumnPublishSystemRootApply += timing.SystemApply
	stats.ColumnPublishFinalize += timing.Finalize
	stats.ColumnPublishFinalizePrepareDurability += timing.FinalizePrepareDurability
	stats.ColumnPublishFinalizeCandidateBuild += timing.FinalizeCandidateBuild
	stats.ColumnPublishFinalizeCandidateVisibleBaseClone += timing.FinalizeCandidateVisibleBaseClone
	stats.ColumnPublishFinalizeCandidateInheritedFilter += timing.FinalizeCandidateInheritedFilter
	stats.ColumnPublishFinalizeCandidateFreshCapture += timing.FinalizeCandidateFreshCapture
	stats.ColumnPublishFinalizeCandidateClosureAssemble += timing.FinalizeCandidateClosureAssemble
	stats.ColumnPublishFinalizeCandidateVisibleClone += timing.FinalizeCandidateVisibleClone
	stats.ColumnPublishFinalizeCandidateCOWPrepare += timing.FinalizeCandidateCOWPrepare
	stats.ColumnPublishFinalizeCandidateOther += timing.FinalizeCandidateOther
	work := timing.FinalizeCandidateResourceWork
	stats.ColumnPublishFinalizeCandidateResourceWork.Add(ColumnPublishCandidateResourceWork{
		CloneOperations: work.CloneOperations, FreezeOperations: work.FreezeOperations,
		RequirementFieldsInspected: work.RequirementFieldsInspected, RequirementObligationsInspected: work.RequirementObligationsInspected,
		SourceEntriesInspected: work.SourceEntriesInspected, SourceObligationsInspected: work.SourceObligationsInspected,
		RetainedEntries: work.RetainedEntries, RetainedObligations: work.RetainedObligations,
		DroppedEntries: work.DroppedEntries, DroppedObligations: work.DroppedObligations,
		CopiedEntries: work.CopiedEntries, CopiedObligations: work.CopiedObligations,
		PhysicalHandleCopies: work.PhysicalHandleCopies, PhysicalHandleShares: work.PhysicalHandleShares,
		PhysicalRootShares:              work.PhysicalRootShares,
		LogicalObligationNormalizations: work.LogicalObligationNormalizations,
		RetainedIndexNodeVisits:         work.RetainedIndexNodeVisits, RetainedIndexNodeCopies: work.RetainedIndexNodeCopies,
		LogicalIndexNodesAdmitted: work.LogicalIndexNodesAdmitted,
		AggregateMembershipProbes: work.AggregateMembershipProbes, AggregateMembershipNodeVisits: work.AggregateMembershipNodeVisits,
		AggregateMembershipNodeCopies: work.AggregateMembershipNodeCopies, AggregateMembershipAdmissions: work.AggregateMembershipAdmissions,
		PhysicalEntryLookupProbes: work.PhysicalEntryLookupProbes, PhysicalEntryLookupComparisons: work.PhysicalEntryLookupComparisons,
		PhysicalEntryLookupAdmissions: work.PhysicalEntryLookupAdmissions, NewlyAdmittedEntries: work.NewlyAdmittedEntries,
		NewlyAdmittedObligations: work.NewlyAdmittedObligations, RemovedObligations: work.RemovedObligations,
		AppendOnlyFastPath:  work.AppendOnlyFastPath,
		AppendOnlyFallbacks: work.AppendOnlyFallbacks, DestructiveFallbacks: work.DestructiveFallbacks,
		FullClosureValidations:        work.FullClosureValidations,
		FinalRequirementProofFastPath: work.FinalRequirementProofFastPath, FinalRequirementProofFallbacks: work.FinalRequirementProofFallbacks,
		FinalRequirementRecordsDecoded: work.FinalRequirementRecordsDecoded, FinalRequirementObligationsMaterialized: work.FinalRequirementObligationsMaterialized,
	})
	stats.ColumnPublishFinalizeEnqueueActivation += timing.FinalizeEnqueueActivation
	stats.ColumnPublishFinalizeAdmissionWait += timing.FinalizeAdmissionWait
	stats.ColumnPublishFinalizeDurabilityWait += timing.FinalizeDurabilityWait
	stats.ColumnPublishPostFinalize += timing.PostFinalize
}

func (c *Collection) publishRootDeltaGroupWithoutColumn(ordered []backenddb.OrderedRootDeltaPublishInput, input columnWritePublishInput) (uint64, []uint64, error) {
	if input.commandWALIntent != nil {
		if input.rawPublishLocked {
			return c.db.PublishStagedOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(ordered, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
			})
		}
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
			if input.rawPublishLocked {
				return c.db.PublishStagedOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
					return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
				})
			}
			return c.db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
			})
		}
		if input.rawPublishLocked {
			return c.db.PublishStagedOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
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
	cfg := input.meta.Options.ColumnStore
	if cfg == nil {
		return ColumnPublishPlan{}, errors.New("collections: column publish requires column store config")
	}
	currentRecords, err := c.loadColumnManifestRecordsForPublish(baseManifestRootID, input.meta.Name, *cfg)
	if err != nil {
		return ColumnPublishPlan{}, err
	}
	plan, err := BuildColumnPublishPlan(ColumnPublishPlanInput{
		Collection:               input.meta.Name,
		ColumnStore:              cfg,
		ColumnStoreNormalized:    true,
		ActiveVectorIndexes:      append([]VectorIndexDefinition(nil), input.meta.VectorIndexes...),
		ActiveVectorIndexesKnown: true,
		Operation:                input.operation,
		CurrentManifest:          cfg.ActiveManifest,
		CurrentManifestRecords:   currentRecords,
		AppliedCommandLSN:        ctx.AppliedCommandLSN,
		BaseManifestRootID:       baseManifestRootID,
		Hooks: ColumnPublishPlanHooks{
			PrepareAssets: func(hookInput ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
				return c.prepareColumnPhysicalAssetsForCommand(input, hookInput)
			},
			EncodeManifest: encodeColumnManifestIdentityForWrite,
		},
	})
	if err != nil {
		return ColumnPublishPlan{}, err
	}
	plan.StageMetrics.DocumentExtraction += input.documentExtraction
	return plan, nil
}

func (c *Collection) loadColumnManifestRecordsForPublish(rootID uint64, collectionName string, cfg ColumnStoreConfig) ([]columnManifestRecord, error) {
	if rootID == 0 {
		if cfg.ActiveManifest != nil {
			return nil, fmt.Errorf("collections: active column manifest generation %d for %q is missing manifest root", cfg.ActiveManifest.Generation, collectionName)
		}
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
	if len(input.sourceDeleteDocuments) != 0 {
		deleteInput := input
		deleteInput.operation = ColumnPublishOperationDelete
		deleteInput.documents = input.sourceDeleteDocuments
		deleteInput.sourceDeleteDocuments = nil
		deleteInput.rows = len(deleteInput.documents)
		deleteInput.partIDOffset = 0
		deleteHook := hookInput
		deleteHook.Operation = ColumnPublishOperationDelete
		deleted, err := c.prepareColumnPhysicalAssetsForCommand(deleteInput, deleteHook)
		if err != nil {
			return ColumnPublishPreparedAssets{}, err
		}
		insertInput := input
		insertInput.operation = ColumnPublishOperationInsert
		insertInput.sourceDeleteDocuments = nil
		insertInput.partIDOffset = 1 << 32
		insertHook := hookInput
		insertHook.Operation = ColumnPublishOperationInsert
		inserted, err := c.prepareColumnPhysicalAssetsForCommand(insertInput, insertHook)
		if err != nil {
			if deleted.stableResources != nil {
				deleted.stableResources.Release()
			}
			return ColumnPublishPreparedAssets{}, err
		}
		return mergeSourceColumnPreparedAssets(deleted, inserted)
	}
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

func mergeSourceColumnPreparedAssets(deleted, inserted ColumnPublishPreparedAssets) (ColumnPublishPreparedAssets, error) {
	merged := inserted
	merged.Assets = append(append([]ColumnPreparedAsset(nil), deleted.Assets...), inserted.Assets...)
	merged.RowCount = deleted.RowCount + inserted.RowCount
	merged.CommandBytes = saturatingAddNonNegativeInt64(deleted.CommandBytes, inserted.CommandBytes)
	merged.RowRemainderBytes = saturatingAddNonNegativeInt64(deleted.RowRemainderBytes, inserted.RowRemainderBytes)
	merged.ColumnPayloadBytes = saturatingAddNonNegativeInt64(deleted.ColumnPayloadBytes, inserted.ColumnPayloadBytes)
	merged.AssetMetrics.RowAssetDuration += deleted.AssetMetrics.RowAssetDuration
	merged.AssetMetrics.RowAssetBytes = saturatingAddNonNegativeInt64(merged.AssetMetrics.RowAssetBytes, deleted.AssetMetrics.RowAssetBytes)
	merged.AssetMetrics.RowAssetCount += deleted.AssetMetrics.RowAssetCount
	merged.stableResourcesRequired = deleted.stableResourcesRequired || inserted.stableResourcesRequired
	if deleted.stableResources == nil {
		return merged, nil
	}
	if inserted.stableResources == nil {
		merged.stableResources = deleted.stableResources
		return merged, nil
	}
	builder := &rootpublication.StableResourceSetBuilder{}
	if err := builder.Merge(deleted.stableResources); err != nil {
		inserted.stableResources.Release()
		return ColumnPublishPreparedAssets{}, err
	}
	if err := builder.Merge(inserted.stableResources); err != nil {
		builder.Abandon()
		return ColumnPublishPreparedAssets{}, err
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return ColumnPublishPreparedAssets{}, err
	}
	merged.stableResources = resources
	return merged, nil
}

func (c *Collection) prepareColumnPhysicalAssetRowsForCommand(prepared ColumnPublishPreparedAssets, input columnWritePublishInput, hookInput ColumnPublishAssetPrepareInput, rows []columnDeclaredRow) (_ ColumnPublishPreparedAssets, retErr error) {
	cleanupAssets := make([]ColumnPreparedAsset, 0, 8)
	defer func() {
		if retErr != nil {
			if prepared.stableResources != nil {
				prepared.stableResources.Release()
				prepared.stableResources = nil
			}
			retainStableOrphans := prepared.stableResourcesRequired && columnPreparedAssetsRequireOrphanRetention(cleanupAssets)
			if len(cleanupAssets) != 0 && !retainStableOrphans {
				if cleanupErr := cleanupColumnPreparedAssets(c.db.ColumnAssetRootDir(), cleanupAssets); cleanupErr != nil {
					retErr = errors.Join(retErr, cleanupErr)
				}
			}
		}
	}()
	trackCleanupAsset := func(ref ColumnAssetRef) {
		cleanupAssets = append(cleanupAssets, ColumnPreparedAsset{Ref: ref})
	}
	generation := uint64(1)
	if hookInput.CurrentManifest != nil {
		generation = hookInput.CurrentManifest.Generation + 1
	}
	rowPartID := columnPhysicalRowAssetPartID + input.partIDOffset
	typedPartID := uint64(typedColumnPartAssetPartID)
	role := columnManifestPartRoleForPublish(hookInput.Operation)
	type pendingColumnAsset struct {
		payload  []byte
		ref      ColumnAssetRef
		hasRef   bool
		fileID   uint32
		kind     ColumnAssetKind
		partID   uint64
		rows     int
		reason   string
		partRole ColumnManifestPartRole
		sortKey  string
		validate func(ColumnAssetRef) error
	}
	pendingAssets := make([]pendingColumnAsset, 0, 8)
	queueRegularAsset := func(payload []byte, kind ColumnAssetKind, partID uint64, rows int, reason string) {
		pendingAssets = append(pendingAssets, pendingColumnAsset{
			payload: payload,
			fileID:  columnAssetM12ASegmentFileID,
			kind:    kind,
			partID:  partID,
			rows:    rows,
			reason:  reason,
		})
	}
	queueRegularManifestAssetToFile := func(payload []byte, kind ColumnAssetKind, partID uint64, rows int, reason string, partRole ColumnManifestPartRole, sortKey string, fileID uint32, validate func(ColumnAssetRef) error) {
		pendingAssets = append(pendingAssets, pendingColumnAsset{
			payload:  payload,
			fileID:   fileID,
			kind:     kind,
			partID:   partID,
			rows:     rows,
			reason:   reason,
			partRole: partRole,
			sortKey:  sortKey,
			validate: validate,
		})
	}
	queueRegularManifestAsset := func(payload []byte, kind ColumnAssetKind, partID uint64, rows int, reason string, partRole ColumnManifestPartRole, sortKey string, validate func(ColumnAssetRef) error) {
		queueRegularManifestAssetToFile(payload, kind, partID, rows, reason, partRole, sortKey, columnAssetM12ASegmentFileID, validate)
	}
	flushPendingAssets := func() (retErr error) {
		if len(pendingAssets) == 0 {
			return nil
		}
		needsAppender := false
		for _, asset := range pendingAssets {
			if !asset.hasRef {
				needsAppender = true
				break
			}
		}
		var session *columnPhysicalAssetAppendSession
		closed := false
		var appendOpenDuration time.Duration
		var appendWriteDuration time.Duration
		var appendCloseDuration time.Duration
		var appendFileSyncDuration time.Duration
		var appendFileCloseDuration time.Duration
		var appendDirSyncDuration time.Duration
		var appendCleanupDuration time.Duration
		var appendCloseCount int
		var appendFileSyncCount int
		var appendSyncEpochCount int
		if needsAppender {
			appendStart := time.Now()
			if ordinaryColumnStableAuthorityEnabled() {
				prepared.stableResourcesRequired = true
				session = newColumnPhysicalAssetAppendSessionWithStableResources(
					c.db.ColumnAssetRootDir(), hookInput.ColumnStore, c.db.StableResourceIdentityPinRegistry(),
				)
			} else {
				// Platforms without complete retained-parent persistence keep the
				// legacy pre-activation path and do not claim stable authority.
				session = newColumnPhysicalAssetAppendSession(c.db.ColumnAssetRootDir(), hookInput.ColumnStore)
			}
			appendOpenDuration += time.Since(appendStart)
			defer func() {
				if retErr != nil && !closed {
					retErr = errors.Join(retErr, session.abort())
				}
			}()
		}
		var appendedBytes int64
		var appendedCount int
		var sharedSegmentAppendBytes int64
		var sharedSegmentAppendCount int
		var directViewSegmentAppendBytes int64
		var directViewSegmentAppendCount int
		appendedRefs := make([]ColumnAssetRef, len(pendingAssets))
		appendedRefSet := make([]bool, len(pendingAssets))
		if needsAppender {
			type appendGroup struct {
				fileID  uint32
				indexes []int
				items   []columnPhysicalAssetAppendItem
			}
			appendGroups := make(map[uint32]*appendGroup)
			appendGroupOrder := make([]uint32, 0, 2)
			for i, asset := range pendingAssets {
				if asset.hasRef {
					continue
				}
				fileID := asset.fileID
				if fileID == 0 {
					fileID = columnAssetM12ASegmentFileID
				}
				group := appendGroups[fileID]
				if group == nil {
					group = &appendGroup{fileID: fileID}
					appendGroups[fileID] = group
					appendGroupOrder = append(appendGroupOrder, fileID)
				}
				group.indexes = append(group.indexes, i)
				group.items = append(group.items, columnPhysicalAssetAppendItem{
					payload:    asset.payload,
					kind:       asset.kind,
					generation: generation,
					partID:     asset.partID,
				})
			}
			for _, fileID := range appendGroupOrder {
				group := appendGroups[fileID]
				directViewSegment := columnAssetSegmentFileIDIsDirectView(group.fileID)
				refs, openDuration, writeDuration, err := session.appendKindsMeasured(group.fileID, group.items)
				appendOpenDuration += openDuration
				appendWriteDuration += writeDuration
				if err != nil {
					return err
				}
				if len(refs) != len(group.indexes) {
					return fmt.Errorf("collections: column physical asset append refs=%d want %d for file_id=%d", len(refs), len(group.indexes), group.fileID)
				}
				for _, ref := range refs {
					trackCleanupAsset(ref)
				}
				for i, ref := range refs {
					assetIndex := group.indexes[i]
					asset := pendingAssets[assetIndex]
					appendedRefs[assetIndex] = ref
					appendedRefSet[assetIndex] = true
					payloadBytes := int64(len(asset.payload))
					appendedBytes = saturatingAddNonNegativeInt64(appendedBytes, payloadBytes)
					appendedCount++
					if directViewSegment {
						directViewSegmentAppendBytes = saturatingAddNonNegativeInt64(directViewSegmentAppendBytes, payloadBytes)
						directViewSegmentAppendCount++
					} else {
						sharedSegmentAppendBytes = saturatingAddNonNegativeInt64(sharedSegmentAppendBytes, payloadBytes)
						sharedSegmentAppendCount++
					}
					if ref.Namespace != hookInput.ColumnStore.AssetManager.Namespace || ref.Kind != asset.kind ||
						ref.Generation != generation || ref.PartID != asset.partID || ref.Length != int64(len(asset.payload)) || ref.FileID != group.fileID {
						return fmt.Errorf("collections: invalid %s asset ref %+v", asset.kind, ref)
					}
				}
			}
		}
		for i, asset := range pendingAssets {
			ref := asset.ref
			if !asset.hasRef {
				if !appendedRefSet[i] {
					return fmt.Errorf("collections: missing appended ref for %s asset part_id=%d", asset.kind, asset.partID)
				}
				ref = appendedRefs[i]
			}
			if asset.validate != nil {
				if err := asset.validate(ref); err != nil {
					return err
				}
			}
			prepared.Assets = append(prepared.Assets, ColumnPreparedAsset{
				Ref:          ref,
				Rows:         asset.rows,
				Bytes:        ref.Length,
				PublishID:    hookInput.AppliedCommandLSN,
				GenerationID: generation,
				Reason:       asset.reason,
				PartRole:     asset.partRole,
				SortKey:      asset.sortKey,
			})
		}
		if session != nil {
			appendStart := time.Now()
			var closeStats columnPhysicalAssetSegmentCloseStats
			if prepared.stableResourcesRequired {
				var resources *rootpublication.StableResourceSet
				var err error
				closeStats, resources, err = session.closeWithStableResources()
				if err != nil {
					return err
				}
				if prepared.stableResources != nil {
					resources.Release()
					return errors.New("collections: column physical asset preparation produced more than one stable resource set")
				}
				prepared.stableResources = resources
			} else {
				var err error
				closeStats, err = session.close()
				if err != nil {
					return err
				}
			}
			closed = true
			appendCloseDuration += time.Since(appendStart)
			appendFileSyncDuration += closeStats.FileSync
			appendFileCloseDuration += closeStats.FileClose
			appendDirSyncDuration += closeStats.DirSync
			appendCleanupDuration += closeStats.CleanupDuration()
			appendCloseCount += closeStats.CloseCount
			appendFileSyncCount += closeStats.FileSyncCount
			appendSyncEpochCount += closeStats.SyncEpochCount
			prepared.AssetMetrics.SharedSegmentAppendCloseCount += closeStats.SharedSegment.CloseCount
			prepared.AssetMetrics.SharedSegmentAppendFileSyncCount += closeStats.SharedSegment.FileSyncCount
			prepared.AssetMetrics.SharedSegmentAppendSyncEpochCount += closeStats.SharedSegment.SyncEpochCount
			prepared.AssetMetrics.DirectViewSegmentAppendCloseCount += closeStats.DirectViewSegment.CloseCount
			prepared.AssetMetrics.DirectViewSegmentAppendFileSyncCount += closeStats.DirectViewSegment.FileSyncCount
			prepared.AssetMetrics.DirectViewSegmentAppendSyncEpochCount += closeStats.DirectViewSegment.SyncEpochCount
		}
		if needsAppender {
			appendDuration := appendOpenDuration + appendWriteDuration + appendCloseDuration
			prepared.AssetMetrics.SharedAppendDuration += appendDuration
			prepared.AssetMetrics.SharedAppendOpenDuration += appendOpenDuration
			prepared.AssetMetrics.SharedAppendWriteDuration += appendWriteDuration
			prepared.AssetMetrics.SharedAppendCloseDuration += appendCloseDuration
			prepared.AssetMetrics.SharedAppendFileSyncDuration += appendFileSyncDuration
			prepared.AssetMetrics.SharedAppendFileCloseDuration += appendFileCloseDuration
			prepared.AssetMetrics.SharedAppendDirSyncDuration += appendDirSyncDuration
			prepared.AssetMetrics.SharedAppendCleanupDuration += appendCleanupDuration
			prepared.AssetMetrics.SharedAppendCloseCount += appendCloseCount
			prepared.AssetMetrics.SharedAppendFileSyncCount += appendFileSyncCount
			prepared.AssetMetrics.SharedAppendSyncEpochCount += appendSyncEpochCount
			prepared.AssetMetrics.SharedAppendBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.SharedAppendBytes, appendedBytes)
			prepared.AssetMetrics.SharedAppendCount += appendedCount
			prepared.AssetMetrics.SharedSegmentAppendBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.SharedSegmentAppendBytes, sharedSegmentAppendBytes)
			prepared.AssetMetrics.SharedSegmentAppendCount += sharedSegmentAppendCount
			prepared.AssetMetrics.DirectViewSegmentAppendBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.DirectViewSegmentAppendBytes, directViewSegmentAppendBytes)
			prepared.AssetMetrics.DirectViewSegmentAppendCount += directViewSegmentAppendCount
		}
		return nil
	}
	type rowAssetPrepareResult struct {
		config   ColumnStoreConfig
		rows     []columnDeclaredRow
		encoded  []byte
		summary  columnPhysicalAssetSummary
		duration time.Duration
	}
	prepareRowAsset := func() (rowAssetPrepareResult, error) {
		start := time.Now()
		rowAssetConfig := columnStoreRowAssetConfig(hookInput.ColumnStore)
		rowAssetRows, err := projectColumnDeclaredRowsForColumns(hookInput.ColumnStore.Columns, rowAssetConfig.Columns, rows)
		if err != nil {
			return rowAssetPrepareResult{}, err
		}
		encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
			Collection:        hookInput.Collection,
			Namespace:         hookInput.ColumnStore.AssetManager.Namespace,
			Generation:        generation,
			PartID:            rowPartID,
			AppliedCommandLSN: hookInput.AppliedCommandLSN,
			Operation:         hookInput.Operation,
			SchemaHash:        hookInput.ColumnStore.SchemaHash,
			Columns:           rowAssetConfig.Columns,
			Rows:              rowAssetRows,
		})
		if err != nil {
			return rowAssetPrepareResult{}, err
		}
		return rowAssetPrepareResult{
			config:   rowAssetConfig,
			rows:     rowAssetRows,
			encoded:  encoded,
			summary:  summary,
			duration: time.Since(start),
		}, nil
	}
	type typedColumnPrepareResult struct {
		build    typedColumnPartImageBuildResult
		duration time.Duration
		err      error
	}
	var typedColumnDone chan typedColumnPrepareResult
	if (hookInput.Operation == ColumnPublishOperationInsert || hookInput.Operation == ColumnPublishOperationUpdate) && columnStoreHasTypedColumnPartOwners(hookInput.ColumnStore) {
		typedColumnDone = make(chan typedColumnPrepareResult, 1)
		go func(done chan<- typedColumnPrepareResult) {
			start := time.Now()
			build, err := buildTypedColumnPartImageForDeclaredRowsWithResult(hookInput.ColumnStore, generation, typedPartID, rows)
			done <- typedColumnPrepareResult{
				build:    build,
				duration: time.Since(start),
				err:      err,
			}
		}(typedColumnDone)
	}
	waitTypedColumn := func() (typedColumnPrepareResult, error) {
		if typedColumnDone == nil {
			return typedColumnPrepareResult{}, nil
		}
		result := <-typedColumnDone
		typedColumnDone = nil
		return result, result.err
	}
	rowAsset, err := prepareRowAsset()
	if err != nil {
		_, _ = waitTypedColumn()
		return ColumnPublishPreparedAssets{}, err
	}
	typedColumn, err := waitTypedColumn()
	if err != nil {
		return ColumnPublishPreparedAssets{}, err
	}
	if prepared.CommandBytes == 0 {
		prepared.CommandBytes = columnWriteDocumentsBytes(input.documents)
	}
	prepared.RowCount = rowAsset.summary.RowCount
	prepared.ColumnPayloadBytes = rowAsset.summary.PayloadBytes
	prepared.AssetMetrics.RowAssetDuration += rowAsset.duration
	prepared.AssetMetrics.RowAssetBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.RowAssetBytes, int64(len(rowAsset.encoded)))
	prepared.AssetMetrics.RowAssetCount++
	queueRegularManifestAsset(rowAsset.encoded, ColumnAssetKindTCS1PartImage, rowPartID, rowAsset.summary.RowCount, string(input.operation), role, "", func(ref ColumnAssetRef) error {
		return validateColumnPhysicalAssetPreparedRefForManifest(ref, rowAsset.config, generation, rowPartID, len(rowAsset.encoded))
	})
	typedGranuleRowOrder := typedColumn.build.TypedGranuleRowOrder
	if hookInput.Operation == ColumnPublishOperationInsert || hookInput.Operation == ColumnPublishOperationUpdate {
		typedColumnImage := typedColumn.build.Bytes
		typedColumnRows := typedColumn.build.Rows
		if len(typedColumnImage) != 0 {
			typedColumnPostStart := time.Now()
			typedColumnSortKey, err := typedColumnPartPublicationSortKey(hookInput.ColumnStore, columnStoreTypedColumnPartFields(hookInput.ColumnStore))
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			validateTypedColumnRef := func(ref ColumnAssetRef) error {
				if ref.Namespace != hookInput.ColumnStore.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart ||
					ref.Generation != generation || ref.PartID != typedPartID || ref.Length != int64(len(typedColumnImage)) {
					return fmt.Errorf("collections: invalid typed-column part asset ref %+v", ref)
				}
				return nil
			}
			if columnStoreConfigNeedsDirectViewTypedColumnAlignment(hookInput.ColumnStore) {
				directFileID, err := directViewTypedColumnSegmentFileID(generation)
				if err != nil {
					return ColumnPublishPreparedAssets{}, err
				}
				queueRegularManifestAssetToFile(typedColumnImage, ColumnAssetKindTCS1TypedColumnPart, typedPartID, typedColumnRows, string(input.operation), role, columnSortKeyMatchString(typedColumnSortKey), directFileID, func(ref ColumnAssetRef) error {
					if err := validateTypedColumnRef(ref); err != nil {
						return err
					}
					if ref.FileID != directFileID {
						return fmt.Errorf("collections: invalid direct-view typed-column part asset file_id=%d want %d", ref.FileID, directFileID)
					}
					if ref.Offset%typedColumnPartDirectViewAssetAlignment != 0 {
						return fmt.Errorf("collections: invalid direct-view typed-column part asset offset=%d want %d-byte alignment", ref.Offset, typedColumnPartDirectViewAssetAlignment)
					}
					return nil
				})
			} else {
				queueRegularManifestAsset(typedColumnImage, ColumnAssetKindTCS1TypedColumnPart, typedPartID, typedColumnRows, string(input.operation), role, columnSortKeyMatchString(typedColumnSortKey), validateTypedColumnRef)
			}
			prepared.AssetMetrics.TypedColumnPartDuration += typedColumn.duration + time.Since(typedColumnPostStart)
			prepared.AssetMetrics.TypedColumnDictionaryBuild += typedColumn.build.Metrics.DictionaryBuild
			prepared.AssetMetrics.TypedColumnRowMaterialization += typedColumn.build.Metrics.RowMaterialization
			prepared.AssetMetrics.TypedColumnPartBuild += typedColumn.build.Metrics.PartBuild
			prepared.AssetMetrics.TypedColumnImageBuild += typedColumn.build.Metrics.ImageBuild
			prepared.AssetMetrics.TypedColumnPartBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.TypedColumnPartBytes, int64(len(typedColumnImage)))
			prepared.AssetMetrics.TypedColumnPartCount++
		}
	}
	if hookInput.Operation == ColumnPublishOperationInsert {
		typedMetadataStart := time.Now()
		typedMetadataAssets, err := buildColumnAggregateMetadataAssetsWithOptions(hookInput.ColumnStore, rows, columnStoreTypedColumnPartAggregateMetadata(hookInput.ColumnStore), hookInput.Collection, hookInput.ColumnStore.AssetManager.Namespace, generation, typedPartID, hookInput.AppliedCommandLSN, columnAggregateMetadataAssetBuildOptions{
			TypedGranuleRowOrder: typedGranuleRowOrder,
		})
		if err != nil {
			return ColumnPublishPreparedAssets{}, err
		}
		var typedMetadataBytes int64
		for _, metadata := range typedMetadataAssets {
			encodedMetadata, err := encodeColumnAggregateMetadataAsset(metadata)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			typedMetadataBytes = saturatingAddNonNegativeInt64(typedMetadataBytes, int64(len(encodedMetadata)))
			queueRegularAsset(encodedMetadata, ColumnAssetKindTCS1AggregateMetadata, typedPartID, len(rows), metadata.AggregateName)
		}
		if typedMetadataBytes > 0 {
			prepared.AssetMetrics.AggregateMetadataDuration += time.Since(typedMetadataStart)
			prepared.AssetMetrics.AggregateMetadataBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.AggregateMetadataBytes, typedMetadataBytes)
			prepared.AssetMetrics.AggregateMetadataCount += len(typedMetadataAssets)
		}
		rowSidecarStart := time.Now()
		rowSidecarAssets, fusedRowSidecars, err := buildColumnRowSidecarAssets(rowAsset.config, rowAsset.rows, rowAsset.config.AggregateMetadata, hookInput.Collection, hookInput.ColumnStore.AssetManager.Namespace, generation, rowPartID, hookInput.AppliedCommandLSN)
		rowSidecarBuildDuration := time.Since(rowSidecarStart)
		if err != nil {
			rowSidecarAssets = columnRowSidecarAssets{}
			fusedRowSidecars = false
			err = nil
		}
		if fusedRowSidecars {
			prepared.AssetMetrics.RowSidecarSharedBuildDuration += rowSidecarBuildDuration
		}
		dictionaryStart := time.Now()
		dictionaryAssets := rowSidecarAssets.DictionaryCodes
		if !fusedRowSidecars {
			dictionaryAssets, err = buildColumnDictionaryCodesAssets(rowAsset.config, rowAsset.rows, hookInput.Collection, hookInput.ColumnStore.AssetManager.Namespace, generation, rowPartID, hookInput.AppliedCommandLSN)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
		}
		var dictionaryBytes int64
		for _, dictionary := range dictionaryAssets {
			encodedDictionary, err := encodeColumnDictionaryCodesAsset(dictionary)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			dictionaryBytes = saturatingAddNonNegativeInt64(dictionaryBytes, int64(len(encodedDictionary)))
			queueRegularAsset(encodedDictionary, ColumnAssetKindTCS1DictionaryCodes, rowPartID, rowAsset.summary.RowCount, dictionary.ColumnName)
		}
		if dictionaryBytes > 0 {
			prepared.AssetMetrics.DictionarySidecarDuration += time.Since(dictionaryStart)
			prepared.AssetMetrics.DictionarySidecarBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.DictionarySidecarBytes, dictionaryBytes)
			prepared.AssetMetrics.DictionarySidecarCount += len(dictionaryAssets)
		}
		int64Start := time.Now()
		int64Assets := rowSidecarAssets.Int64Values
		if !fusedRowSidecars {
			int64Assets, err = buildColumnInt64ValuesAssets(rowAsset.config, rowAsset.rows, hookInput.Collection, hookInput.ColumnStore.AssetManager.Namespace, generation, rowPartID, hookInput.AppliedCommandLSN)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
		}
		var int64Bytes int64
		for _, values := range int64Assets {
			encodedValues, err := encodeColumnInt64ValuesAsset(values)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			int64Bytes = saturatingAddNonNegativeInt64(int64Bytes, int64(len(encodedValues)))
			queueRegularAsset(encodedValues, ColumnAssetKindTCS1Int64Values, rowPartID, rowAsset.summary.RowCount, values.ColumnName)
		}
		if int64Bytes > 0 {
			prepared.AssetMetrics.Int64SidecarDuration += time.Since(int64Start)
			prepared.AssetMetrics.Int64SidecarBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.Int64SidecarBytes, int64Bytes)
			prepared.AssetMetrics.Int64SidecarCount += len(int64Assets)
		}
		rowMetadataStart := time.Now()
		rowMetadataAssets := rowSidecarAssets.AggregateMetadata
		if !fusedRowSidecars {
			rowMetadataAssets, err = buildColumnAggregateMetadataAssets(rowAsset.config, rowAsset.rows, rowAsset.config.AggregateMetadata, hookInput.Collection, hookInput.ColumnStore.AssetManager.Namespace, generation, rowPartID, hookInput.AppliedCommandLSN)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
		}
		var rowMetadataBytes int64
		for _, metadata := range rowMetadataAssets {
			encodedMetadata, err := encodeColumnAggregateMetadataAsset(metadata)
			if err != nil {
				return ColumnPublishPreparedAssets{}, err
			}
			rowMetadataBytes = saturatingAddNonNegativeInt64(rowMetadataBytes, int64(len(encodedMetadata)))
			queueRegularAsset(encodedMetadata, ColumnAssetKindTCS1AggregateMetadata, rowPartID, rowAsset.summary.RowCount, metadata.AggregateName)
		}
		if rowMetadataBytes > 0 {
			prepared.AssetMetrics.AggregateMetadataDuration += time.Since(rowMetadataStart)
			prepared.AssetMetrics.AggregateMetadataBytes = saturatingAddNonNegativeInt64(prepared.AssetMetrics.AggregateMetadataBytes, rowMetadataBytes)
			prepared.AssetMetrics.AggregateMetadataCount += len(rowMetadataAssets)
		}
		if fusedRowSidecars {
			totalFusedBytes := saturatingAddNonNegativeInt64(saturatingAddNonNegativeInt64(dictionaryBytes, int64Bytes), rowMetadataBytes)
			prepared.AssetMetrics.DictionarySidecarDuration += columnPublishDurationShare(rowSidecarBuildDuration, dictionaryBytes, totalFusedBytes)
			prepared.AssetMetrics.Int64SidecarDuration += columnPublishDurationShare(rowSidecarBuildDuration, int64Bytes, totalFusedBytes)
			prepared.AssetMetrics.AggregateMetadataDuration += columnPublishDurationShare(rowSidecarBuildDuration, rowMetadataBytes, totalFusedBytes)
		}
	}
	if err := flushPendingAssets(); err != nil {
		return ColumnPublishPreparedAssets{}, err
	}
	if err := runColumnPhysicalAssetPreparationAfterPrepareHook(prepared); err != nil {
		return ColumnPublishPreparedAssets{}, err
	}
	cleanupAssets = nil
	return prepared, nil
}

func ordinaryColumnStableAuthorityEnabled() bool {
	return rootpublication.StableNamespaceCreationSupported()
}

// Stable-authoritative bytes remain persistent storage even when publication
// fails. Once their exact identity authority is released, pathname cleanup
// could target a same-name replacement, so retain them for reachability GC.
// Query-ready assets remain rebuildable and keep their existing cleanup path.
func columnPreparedAssetsRequireOrphanRetention(assets []ColumnPreparedAsset) bool {
	for _, asset := range assets {
		_, _, classification, err := stableColumnAssetResourceClassification(asset.Ref.Kind)
		if err != nil || classification != "rebuildable-non-authoritative" {
			return true
		}
	}
	return false
}

func columnPublishDurationShare(total time.Duration, partBytes, totalBytes int64) time.Duration {
	if total <= 0 || partBytes <= 0 || totalBytes <= 0 {
		return 0
	}
	return time.Duration(float64(total) * (float64(partBytes) / float64(totalBytes)))
}

func columnManifestPartRoleForPublish(operation ColumnPublishOperation) ColumnManifestPartRole {
	switch operation {
	case ColumnPublishOperationDelete:
		return ColumnManifestPartRoleTombstone
	case ColumnPublishOperationInsert:
		return ColumnManifestPartRoleBase
	case ColumnPublishOperationUpdate:
		return ColumnManifestPartRoleDelta
	default:
		return ColumnManifestPartRoleDelta
	}
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
	if err := c.addDocumentMutationGenerationUpdate(updates, updatedMeta, rootNames); err != nil {
		return nil, CollectionMeta{}, err
	}
	iter, err := buildSystemDeltaIterator(updates)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	return iter, updatedMeta, nil
}
