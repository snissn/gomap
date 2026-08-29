package collections

import (
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// CreateTextIndex adds a collection-native text index and backfills persistent
// roots from the current documents. Explicit v1 indexes backfill postings,
// text-state, and text-stats roots. V2 indexes (the default for new text index
// declarations) backfill document ordinals, docmap, term/stat shells, posting
// blocks, packed norms, optional positions, and status/format roots for v2 BM25F
// serving.
func (c *Collection) CreateTextIndex(def TextIndexDefinition) (*CollectionMeta, TextIndexBackfillStats, error) {
	var emptyStats TextIndexBackfillStats
	if c == nil {
		return nil, emptyStats, errCollectionNil
	}
	if c.db == nil {
		return nil, emptyStats, errCollectionDBNil
	}
	if c.db.CommandWALEnabled() {
		return nil, emptyStats, fmt.Errorf("%w: collection catalog text index mutation is rejected under command_wal_v2 until catalog text index commands are supported", backenddb.ErrCommandWALRejected)
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, emptyStats, err
	}
	return c.createTextIndexWithRetry(def, c.createTextIndexOnce)
}

func (c *Collection) createTextIndexWithRetry(def TextIndexDefinition, run func(TextIndexDefinition) (*CollectionMeta, TextIndexBackfillStats, error)) (*CollectionMeta, TextIndexBackfillStats, error) {
	var emptyStats TextIndexBackfillStats
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		meta, stats, err := run(def)
		if err == nil {
			return meta, stats, nil
		}
		if backenddb.CommitPublicationAccepted(err) {
			if !isRetriableOrderedRootPublishConflict(err) {
				return nil, emptyStats, err
			}
			return c.reconcileAcceptedTextIndexCreate(def.Name, meta, stats, err)
		}
		if !isRetriableTextIndexMutationError(err) {
			return nil, emptyStats, err
		}
		lastErr = err
		waitBeforeCollectionMutationRetry(attempt)
	}
	return nil, emptyStats, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) createTextIndexOnce(def TextIndexDefinition) (*CollectionMeta, TextIndexBackfillStats, error) {
	var emptyStats TextIndexBackfillStats
	unlockSchema := c.lockCollectionSchemaWrite()
	defer unlockSchema()
	if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
		return nil, emptyStats, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, emptyStats, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return nil, emptyStats, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, emptyStats, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return nil, emptyStats, err
	}
	baseMeta := catalog.meta
	c.meta = baseMeta
	baseOptions, err := collectionPlannerOptionsForDB(c.db, baseMeta)
	if err != nil {
		_ = snap.Close()
		return nil, emptyStats, err
	}
	baseOptions = collectionOptionsWithTemplateV1Resolver(baseOptions, snap, catalog)
	newMeta, normalizedDef, err := addTextIndexToCollectionMeta(baseMeta, def)
	if err != nil {
		_ = snap.Close()
		return nil, emptyStats, err
	}
	if err := rejectCreateTextIndexOnRetainedColumnField(baseMeta, normalizedDef); err != nil {
		_ = snap.Close()
		return nil, emptyStats, err
	}
	plan, err := buildCreateTextIndexBackfillPlan(snap, catalog, normalizedDef, baseOptions)
	if err != nil {
		_ = snap.Close()
		return nil, emptyStats, err
	}
	defer func() { _ = snap.Close() }()
	publishTables, cleanupPointerized, err := pointerizeCollectionRootDeltaTables(c.db, newMeta, plan.rootNames, plan.tables)
	if err != nil {
		resetCollectionTables(plan.tables)
		return nil, emptyStats, err
	}
	defer cleanupPointerized()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionTables(plan.tables)
	}()
	for i, rootName := range plan.rootNames {
		iter := publishTables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      plan.baseRootIDs[rootName],
			Iter:          iter,
			StoragePolicy: plan.policies[i],
		})
	}

	if hook := testBeforeSchemaBackfillPublishHook.ptr.Load(); hook != nil && hook.fn != nil {
		hook.fn("text_index")
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(snap.Pager(), snapshotUserRoot(snap), snapshotSystemRoot(snap), snapshotCommitSeq(snap))
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaAndRootDescriptorSystemIterator(baseMeta, newMeta, plan.rootNames, plan.baseRootIDs, rootIDs)
	})
	if err != nil {
		return newMeta.copy(), plan.stats, err
	}
	if len(rootIDs) != len(plan.rootNames) {
		return nil, emptyStats, unexpectedOrderedRootCountError(newMeta.Name, len(plan.rootNames), len(rootIDs))
	}
	c.meta = newMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, plan.rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), plan.stats, nil
}

// DropTextIndex removes text index metadata and clears the persistent v1 and
// v2 text root descriptors for the index.
func (c *Collection) DropTextIndex(name string) (*CollectionMeta, error) {
	if err := ValidateIndexName(name); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if c.db.CommandWALEnabled() {
		return nil, fmt.Errorf("%w: collection catalog text index mutation is rejected under command_wal_v2 until catalog text index commands are supported", backenddb.ErrCommandWALRejected)
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	return c.dropTextIndexWithRetry(name, c.dropTextIndexOnce)
}

func (c *Collection) dropTextIndexWithRetry(name string, run func(string) (*CollectionMeta, error)) (*CollectionMeta, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		meta, err := run(name)
		if err == nil {
			return meta, nil
		}
		if backenddb.CommitPublicationAccepted(err) {
			if !isRetriableOrderedRootPublishConflict(err) {
				return nil, err
			}
			return c.reconcileAcceptedTextIndexDrop(name, err)
		}
		if !isRetriableTextIndexMutationError(err) {
			return nil, err
		}
		lastErr = err
		waitBeforeCollectionMutationRetry(attempt)
	}
	return nil, collectionMutationRetryExhausted(lastErr)
}

func isRetriableTextIndexMutationError(err error) bool {
	if backenddb.CommitPublicationAccepted(err) {
		return false
	}
	if isRetriableCollectionMutationError(err) {
		return true
	}
	return errors.Is(err, errConcurrentSchemaModification)
}

func (c *Collection) dropTextIndexOnce(name string) (*CollectionMeta, error) {
	unlockSchema := c.lockCollectionSchemaWrite()
	defer unlockSchema()
	if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return nil, err
	}
	baseMeta := catalog.meta
	c.meta = baseMeta
	_ = snap.Close()

	nextTextIndexes := make([]TextIndexDefinition, 0, len(baseMeta.TextIndexes))
	dropped := false
	for _, idx := range baseMeta.TextIndexes {
		if idx.Name == name {
			dropped = true
			continue
		}
		nextTextIndexes = append(nextTextIndexes, idx)
	}
	if !dropped {
		return nil, ErrIndexNotFound
	}
	newMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name:          baseMeta.Name,
		Options:       baseMeta.Options,
		Indexes:       baseMeta.Indexes,
		VectorIndexes: baseMeta.VectorIndexes,
		TextIndexes:   nextTextIndexes,
	})
	if err != nil {
		return nil, err
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	clearedRootNames := collectionTextAllRootNames(baseMeta.Name, name)
	newSystemRoot, _, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaOnlySystemDeltaIterator(baseMeta, encodedMeta, clearedRootNames)
	})
	if err != nil {
		return newMeta.copy(), err
	}
	c.meta = newMeta
	clearedRootIDs := make([]uint64, len(clearedRootNames))
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, clearedRootNames, clearedRootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), nil
}

func (c *Collection) reconcileAcceptedTextIndexCreate(name string, desired *CollectionMeta, stats TextIndexBackfillStats, publicationErr error) (*CollectionMeta, TextIndexBackfillStats, error) {
	var emptyStats TextIndexBackfillStats
	if desired == nil {
		return nil, emptyStats, publicationErr
	}
	desiredDef, ok := findTextIndex(desired.TextIndexes, name)
	if !ok {
		return nil, emptyStats, publicationErr
	}
	unlockSchema := c.lockCollectionSchemaWrite()
	defer unlockSchema()
	if err := c.db.Checkpoint(); err != nil {
		return nil, emptyStats, errors.Join(publicationErr, fmt.Errorf("collections: settle accepted text index create: %w", err))
	}
	catalog, err := c.loadCurrentCatalogForAcceptedTextIndexMutation()
	if err != nil {
		return nil, emptyStats, errors.Join(publicationErr, err)
	}
	got, ok := findTextIndex(catalog.meta.TextIndexes, desiredDef.Name)
	if !ok || !textIndexDefinitionValuesEqual(got, desiredDef) {
		return nil, emptyStats, errors.Join(publicationErr, fmt.Errorf("collections: accepted text index create did not reconcile exact index %q", desiredDef.Name))
	}
	return catalog.meta.copy(), stats, nil
}

func (c *Collection) reconcileAcceptedTextIndexDrop(name string, publicationErr error) (*CollectionMeta, error) {
	unlockSchema := c.lockCollectionSchemaWrite()
	defer unlockSchema()
	if err := c.db.Checkpoint(); err != nil {
		return nil, errors.Join(publicationErr, fmt.Errorf("collections: settle accepted text index drop: %w", err))
	}
	catalog, err := c.loadCurrentCatalogForAcceptedTextIndexMutation()
	if err != nil {
		return nil, errors.Join(publicationErr, err)
	}
	if _, ok := findTextIndex(catalog.meta.TextIndexes, name); ok {
		return nil, errors.Join(publicationErr, fmt.Errorf("collections: accepted text index drop left index %q present", name))
	}
	return catalog.meta.copy(), nil
}

func (c *Collection) loadCurrentCatalogForAcceptedTextIndexMutation() (*collectionCatalog, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	c.meta = catalog.meta
	c.rememberCatalog(snap, catalog)
	c.noteWriteDomainCatalog(snapshotSystemRoot(snap), catalog)
	return catalog, nil
}

// TextIndexStorageStats validates and summarizes persistent text roots for a
// declared text index. It scans durable text roots and is intended for
// maintenance/accounting/benchmark use; serving health paths should prefer the
// lightweight TextIndexStatus.
func (c *Collection) TextIndexStorageStats(indexName string) (TextIndexStorageStats, error) {
	var stats TextIndexStorageStats
	if err := ValidateIndexName(indexName); err != nil {
		return stats, err
	}
	if c == nil {
		return stats, errCollectionNil
	}
	if c.db == nil {
		return stats, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return stats, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return stats, err
	}
	if catalog == nil {
		return stats, errCollectionNotFound
	}
	def, ok := findTextIndex(catalog.meta.TextIndexes, indexName)
	if !ok {
		return stats, ErrIndexNotFound
	}
	return inspectTextIndexStorage(snap, catalog, def)
}

// TextIndexStorageAccounting returns exact encoded byte totals for each durable
// text-index lane. Unlike TextIndexStorageStats, it does not decode and validate
// every posting or position entry, so it is suitable for large-scale accounting.
func (c *Collection) TextIndexStorageAccounting(indexName string) (TextIndexStorageAccounting, error) {
	var stats TextIndexStorageAccounting
	if err := ValidateIndexName(indexName); err != nil {
		return stats, err
	}
	if c == nil {
		return stats, errCollectionNil
	}
	if c.db == nil {
		return stats, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return stats, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return stats, err
	}
	if catalog == nil {
		return stats, errCollectionNotFound
	}
	def, ok := findTextIndex(catalog.meta.TextIndexes, indexName)
	if !ok {
		return stats, ErrIndexNotFound
	}
	return accountTextIndexStorage(snap, catalog, def)
}

func addTextIndexToCollectionMeta(meta CollectionMeta, def TextIndexDefinition) (CollectionMeta, TextIndexDefinition, error) {
	if _, ok := findIndex(meta.Indexes, def.Name); ok {
		return CollectionMeta{}, TextIndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	if _, ok := findVectorIndex(meta.VectorIndexes, def.Name); ok {
		return CollectionMeta{}, TextIndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	if _, ok := findTextIndex(meta.TextIndexes, def.Name); ok {
		return CollectionMeta{}, TextIndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	candidate := CollectionMeta{
		Name:          meta.Name,
		Options:       meta.Options,
		Indexes:       append([]IndexDefinition(nil), meta.Indexes...),
		VectorIndexes: copyVectorIndexDefinitions(meta.VectorIndexes),
		TextIndexes:   append(copyTextIndexDefinitions(meta.TextIndexes), def),
	}
	normalized, err := normalizeCollectionMeta(candidate)
	if err != nil {
		return CollectionMeta{}, TextIndexDefinition{}, err
	}
	normalizedDef, ok := findTextIndex(normalized.TextIndexes, def.Name)
	if !ok {
		return CollectionMeta{}, TextIndexDefinition{}, fmt.Errorf("collections: normalized text index %q not found", def.Name)
	}
	return normalized, normalizedDef, nil
}

func rejectCreateTextIndexOnRetainedColumnField(meta CollectionMeta, def TextIndexDefinition) error {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload == ColumnRetainedPayloadFull {
		return nil
	}
	if cfg.RetainedPayload == ColumnRetainedPayloadNone {
		return fmt.Errorf("collections: CreateTextIndex on retained-payload-none collection is unsupported because primary rows retain no JSON payload for text index backfill")
	}
	for _, field := range def.Fields {
		fieldPath := field.Field
		for _, col := range cfg.Columns {
			columnPath := col.Path
			if columnRetainedPayloadPathOverlaps(fieldPath, columnPath) {
				return fmt.Errorf("collections: CreateTextIndex on retained-payload column field %q is unsupported because primary rows omit declared column payloads", fieldPath)
			}
		}
	}
	return nil
}
