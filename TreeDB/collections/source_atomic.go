package collections

import (
	"bytes"
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// sourcePublicationHooks are package-test observation/fault points. Every hook
// except afterPublish runs before the durable publication boundary.
type sourcePublicationHooks struct {
	afterDeletePlan func() error
	afterInsertPlan func() error
	afterParentPlan func() error
	beforePublish   func() error
	afterPublish    func() error
}

type sourceReplacementPlan struct {
	meta             CollectionMeta
	catalog          *collectionCatalog
	snap             *backenddb.Snapshot
	baseUserRoot     uint64
	baseSystemRoot   uint64
	baseCommitSeq    uint64
	rootNames        []string
	baseRootIDs      map[string]uint64
	policies         []backenddb.OrderedRootStoragePolicy
	deltaTables      []memtable.Table
	deleteCount      int
	oldTextDocuments map[string][]byte
	deleteColumnDocs []columnWriteDocument
	insertColumnDocs []columnWriteDocument
	commandWAL       *backenddb.CommandWALIntent
}

type sourceReplacementDeletePlanner func(*backenddb.Snapshot, *collectionCatalog) ([][]byte, error)

func (plan *sourceReplacementPlan) close() {
	if plan == nil {
		return
	}
	if plan.snap != nil {
		_ = plan.snap.Close()
		plan.snap = nil
	}
	resetCollectionTables(plan.deltaTables)
	plan.deltaTables = nil
}

// replaceChunkSourceDocuments publishes one source's old parent/children
// removal and complete replacement as one dependency-closed collection root
// group. Insert state wins for deterministic child IDs present in both sets.

func (c *Collection) replaceChunkSourceDocuments(parentID []byte, insertIDs, insertDocs [][]byte, hooks *sourcePublicationHooks) (int, error) {
	return c.replaceSourceDocumentsAtomic(parentID, nil, insertIDs, insertDocs, nil, hooks)
}

func (c *Collection) replaceSourceDocumentsWithCommandWALIntent(deleteIDs, insertIDs, insertDocs [][]byte, replay *backenddb.CommandWALIntent, hooks *sourcePublicationHooks) (int, error) {
	return c.replaceSourceDocumentsAtomic(nil, deleteIDs, insertIDs, insertDocs, replay, hooks)
}

func (c *Collection) replaceSourceDocumentsAtomic(parentID []byte, deleteIDs, insertIDs, insertDocs [][]byte, replay *backenddb.CommandWALIntent, hooks *sourcePublicationHooks) (int, error) {
	if c == nil {
		return 0, errCollectionNil
	}
	if c.db == nil {
		return 0, errCollectionDBNil
	}
	if len(insertIDs) != len(insertDocs) {
		return 0, fmt.Errorf("collections: source replacement ids=%d documents=%d", len(insertIDs), len(insertDocs))
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return 0, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
		return 0, err
	}
	replacedChildren := -1
	if len(parentID) != 0 {
		oldChildren, _, err := c.chunkChildrenUnlocked(parentID)
		if err != nil {
			return 0, err
		}
		replacedChildren = len(oldChildren)
		deleteIDs = make([][]byte, 0, len(oldChildren)+1)
		deleteIDs = append(deleteIDs, oldChildren...)
		deleteIDs = append(deleteIDs, bytes.Clone(parentID))
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		plan, err := c.buildSourceReplacementPlan(deleteIDs, insertIDs, insertDocs, nil, replay, hooks)
		if err != nil {
			if isRetriableCollectionMutationError(err) {
				lastErr = err
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			return 0, err
		}
		deleted := plan.deleteCount
		publishErr := c.publishSourceReplacementPlan(plan, hooks)
		plan.close()
		if isRetriableCollectionMutationError(publishErr) {
			lastErr = publishErr
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		published := publishErr == nil || backenddb.CommitPublicationAccepted(publishErr) || errors.Is(publishErr, ErrCommitAmbiguous)
		if !published {
			return 0, publishErr
		}
		reconcileIDs := make([][]byte, 0, len(deleteIDs)+len(insertIDs))
		seen := make(map[string]struct{}, len(deleteIDs)+len(insertIDs))
		for _, ids := range [][][]byte{deleteIDs, insertIDs} {
			for _, id := range ids {
				if _, ok := seen[string(id)]; ok {
					continue
				}
				seen[string(id)] = struct{}{}
				reconcileIDs = append(reconcileIDs, id)
			}
		}
		notifyErr := c.reconcileVectorIndexes(reconcileIDs)
		if notifyErr != nil {
			c.invalidateRegisteredVectorIndexDocumentCoverage()
			notifyErr = commitAmbiguousError("atomic source replacement vector maintenance", notifyErr)
		}
		if publishErr != nil || notifyErr != nil {
			return deleted, errors.Join(publishErr, notifyErr)
		}
		if replacedChildren >= 0 {
			return replacedChildren, nil
		}
		return deleted, nil
	}
	return 0, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) buildSourceReplacementPlan(deleteIDs, insertIDs, insertDocs [][]byte, deletePlanner sourceReplacementDeletePlanner, replay *backenddb.CommandWALIntent, hooks *sourcePublicationHooks) (*sourceReplacementPlan, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	plan := &sourceReplacementPlan{snap: snap}
	fail := func(err error) (*sourceReplacementPlan, error) {
		plan.close()
		return nil, err
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return fail(err)
	}
	if catalog == nil {
		return fail(errCollectionNotFound)
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		return fail(err)
	}
	meta := catalog.meta
	c.meta = meta
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, meta)
	if err != nil {
		return fail(err)
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	if err := requireColumnStoreWriteOperationSupported(meta, ColumnPublishOperationDelete); err != nil {
		return fail(err)
	}
	if err := requireColumnStoreWriteOperationSupported(meta, ColumnPublishOperationInsert); err != nil {
		return fail(err)
	}
	plan.meta = meta
	plan.catalog = catalog
	plan.baseUserRoot = snapshotUserRoot(snap)
	plan.baseSystemRoot = snapshotSystemRoot(snap)
	plan.baseCommitSeq = snapshotCommitSeq(snap)
	plan.baseRootIDs = make(map[string]uint64)

	if deletePlanner != nil {
		deleteIDs, err = deletePlanner(snap, catalog)
		if err != nil {
			return fail(err)
		}
	}

	if err := c.appendSourceDeleteDeltas(plan, deleteIDs, plannerOptions); err != nil {
		return fail(err)
	}
	if hooks != nil && hooks.afterDeletePlan != nil {
		if err := hooks.afterDeletePlan(); err != nil {
			return fail(err)
		}
	}

	indexRuntimes, indexRuntimesErr := catalog.cachedIndexRuntimes()
	insertPlanner := insertBatchPlanner{
		collection:             meta.Name,
		primaryRoot:            catalog.primaryRootName,
		templateRoot:           catalog.templateRootName,
		indexStateRoot:         catalog.indexStateRootName,
		cachedIndexRuntimes:    indexRuntimes,
		cachedIndexRuntimesErr: indexRuntimesErr,
		options:                plannerOptions,
	}
	insertPlan, err := insertPlanner.planInsertBatch(insertIDs, insertDocs)
	if err != nil {
		return fail(err)
	}
	defer resetCollectionRunTables(insertPlan.runs)
	if len(meta.TextIndexes) > 0 {
		if err := c.appendSourceTextDeltas(plan, insertIDs, insertDocs, plannerOptions); err != nil {
			return fail(err)
		}
	}
	replacing := make(map[string]struct{}, len(deleteIDs))
	for _, id := range deleteIDs {
		replacing[string(id)] = struct{}{}
	}
	if err := insertPlan.checkPersistedConflictsReplacing(snap, catalog, replacing); err != nil {
		return fail(err)
	}
	if hooks != nil && hooks.afterInsertPlan != nil {
		if err := hooks.afterInsertPlan(); err != nil {
			return fail(err)
		}
	}
	if hooks != nil && hooks.afterParentPlan != nil {
		if err := hooks.afterParentPlan(); err != nil {
			return fail(err)
		}
	}
	for i := range insertPlan.runs {
		run := &insertPlan.runs[i]
		plan.rootNames = append(plan.rootNames, run.name)
		plan.baseRootIDs[run.name] = catalog.rootID(run.name)
		plan.policies = append(plan.policies, run.storagePolicy)
		plan.deltaTables = append(plan.deltaTables, run.table)
		run.table = nil
	}
	docs, err := collectionDocumentsFromBatchInput(insertIDs, insertDocs)
	if err != nil {
		return fail(err)
	}
	plan.insertColumnDocs = columnWriteDocumentsFromCommitLog(docs)
	if replay != nil {
		plan.commandWAL = replay
	} else if c.commandWALActive(nil) {
		plan.commandWAL, err = c.newCollectionReplaceSourceCommandWALIntent(deleteIDs, docs, nil)
		if err != nil {
			return fail(err)
		}
	}
	if err := c.requireColumnStoreCommandWAL(meta, plan.commandWAL); err != nil {
		return fail(err)
	}
	return plan, nil
}

func (c *Collection) appendSourceDeleteDeltas(plan *sourceReplacementPlan, documentIDs [][]byte, plannerOptions collectionOptions) error {
	if plan == nil || plan.snap == nil || plan.catalog == nil || len(documentIDs) == 0 {
		return nil
	}
	primaryRootName := collectionPrimaryRootName(plan.meta.Name)
	primaryRoot := plan.catalog.rootID(primaryRootName)
	if primaryRoot == 0 {
		return nil
	}
	runtimes, err := plan.catalog.cachedIndexRuntimes()
	if err != nil {
		return err
	}
	type existingDelete struct {
		id           []byte
		state        documentIndexState
		document     []byte
		primaryValue []byte
	}
	existing := make([]existingDelete, 0, len(documentIDs))
	for _, documentID := range documentIDs {
		entry, _, err := collectionGetEntryAtCatalogRoot(plan.snap, plan.catalog, primaryRootName, documentID)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			continue
		}
		item := existingDelete{id: documentID}
		if columnStoreRetainedPayloadUsesSemanticStreamV1(plan.meta.Options.ColumnStore) {
			primaryValue, ok, err := columnRetainedSemanticStreamV1PrimaryValueForReclaim(plan.snap, plan.catalog, primaryRootName, documentID, entry)
			if err != nil {
				return err
			}
			if ok {
				item.primaryValue = primaryValue
			}
		}
		if len(plan.meta.TextIndexes) > 0 {
			document, found, err := collectionGetAppendAtCatalogRoot(plan.snap, plan.catalog, primaryRootName, documentID, nil)
			if err != nil {
				return err
			}
			if found {
				item.document = document
			}
		}
		if len(runtimes) > 0 {
			item.state, err = loadDeleteIndexState(plan.snap, plan.catalog, documentID, entry.Value, runtimes, plannerOptions)
			if err != nil {
				return err
			}
		}
		existing = append(existing, item)
	}
	plan.deleteCount = len(existing)
	if len(existing) == 0 {
		return nil
	}
	deleteIDs := make([][]byte, len(existing))
	for i := range existing {
		deleteIDs[i] = existing[i].id
	}
	plan.deleteColumnDocs = columnWriteDocumentsFromIDs(deleteIDs)
	if len(plan.meta.TextIndexes) > 0 {
		plan.oldTextDocuments = make(map[string][]byte, len(existing))
		for _, item := range existing {
			plan.oldTextDocuments[string(item.id)] = item.document
		}
	}
	plan.rootNames = append(plan.rootNames, primaryRootName)
	plan.baseRootIDs[primaryRootName] = primaryRoot
	plan.policies = append(plan.policies, plannerOptions.dataStoragePolicy)
	plan.deltaTables = append(plan.deltaTables, buildDeleteRootDeltaTable(deleteIDs))

	if len(runtimes) > 0 {
		if persistIndexStateForOptions(plannerOptions) {
			stateRootName := collectionIndexStateRootName(plan.meta.Name)
			if stateRootID := plan.catalog.rootID(stateRootName); stateRootID != 0 {
				plan.rootNames = append(plan.rootNames, stateRootName)
				plan.baseRootIDs[stateRootName] = stateRootID
				plan.policies = append(plan.policies, plannerOptions.indexStateStoragePolicy)
				plan.deltaTables = append(plan.deltaTables, buildDeleteRootDeltaTable(deleteIDs))
			}
		}
		for _, runtime := range runtimes {
			rootName := collectionSecondaryRootName(plan.meta.Name, runtime.def.name)
			rootID := plan.catalog.rootID(rootName)
			if rootID == 0 {
				continue
			}
			var table memtable.Table
			for _, item := range existing {
				if len(item.state[runtime.def.name]) == 0 {
					continue
				}
				if table == nil {
					table = newCollectionRunTable(len(existing))
				}
				if err := deleteSecondaryEntriesForDocument(table, runtime, item.state, item.id); err != nil {
					resetCollectionRunTable(table)
					return err
				}
			}
			if table == nil {
				continue
			}
			table.Freeze()
			plan.rootNames = append(plan.rootNames, rootName)
			plan.baseRootIDs[rootName] = rootID
			plan.policies = append(plan.policies, runtime.def.storagePolicy)
			plan.deltaTables = append(plan.deltaTables, table)
		}
	}
	var semanticReclaimValues [][]byte
	for _, item := range existing {
		if len(item.primaryValue) != 0 {
			semanticReclaimValues = append(semanticReclaimValues, item.primaryValue)
		}
	}
	return appendColumnRetainedSemanticStreamV1ReclaimDeltas(c.db, plan.snap, plan.catalog, plan.meta, deleteIDs, semanticReclaimValues, nil, &plan.rootNames, plan.baseRootIDs, &plan.policies, &plan.deltaTables)
}

func (c *Collection) appendSourceTextDeltas(plan *sourceReplacementPlan, insertIDs, insertDocs [][]byte, plannerOptions collectionOptions) error {
	if plan == nil || len(plan.meta.TextIndexes) == 0 {
		return nil
	}
	mutations := make([]textDocumentMutation, 0, len(plan.oldTextDocuments)+len(insertIDs))
	for i, id := range insertIDs {
		mutation := textDocumentMutation{documentID: id, newDocument: insertDocs[i], setNew: true}
		if old, ok := plan.oldTextDocuments[string(id)]; ok {
			mutation.oldDocument = old
			mutation.deleteOld = true
			delete(plan.oldTextDocuments, string(id))
		}
		mutations = append(mutations, mutation)
	}
	for id, old := range plan.oldTextDocuments {
		mutations = append(mutations, textDocumentMutation{
			documentID: []byte(id), oldDocument: old, deleteOld: true,
		})
	}
	if len(mutations) == 0 {
		return nil
	}
	return appendTextIndexMutationDeltas(plan.snap, plan.catalog, plannerOptions, mutations, &plan.rootNames, plan.baseRootIDs, &plan.policies, &plan.deltaTables)
}

func (plan *insertBatchPlan) checkPersistedConflictsReplacing(snap *backenddb.Snapshot, catalog *collectionCatalog, replacing map[string]struct{}) error {
	if plan == nil || snap == nil || catalog == nil {
		return errors.New("collections: source replacement conflict check missing plan state")
	}
	if len(plan.primaryKeys) != len(plan.resultIDs) {
		return fmt.Errorf("collections: source replacement conflict check missing primary keys")
	}
	rootName := collectionPrimaryRootName(catalog.meta.Name)
	for _, key := range plan.primaryKeys {
		entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, rootName, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if entry.Flags&node.FlagTombstone == 0 {
			if _, ok := replacing[string(key)]; !ok {
				return ErrDocumentExists
			}
		}
	}
	uniqueNames := uniqueIndexNamesWithDataOrOverlays(catalog)
	runs, err := plan.uniqueProbeRunsForPersistedConflictIndexes(func(indexName string) bool {
		_, ok := uniqueNames[indexName]
		return ok
	})
	if err != nil {
		return err
	}
	for _, run := range runs {
		rootName := collectionSecondaryRootName(catalog.meta.Name, run.indexName)
		indexDef, ok := findIndex(catalog.meta.Indexes, run.indexName)
		if !ok {
			return fmt.Errorf("collections: source replacement missing unique index definition %q", run.indexName)
		}
		for _, prefix := range run.prefixes {
			it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, prefixEnd(prefix), false)
			if err != nil {
				return err
			}
			for it != nil && it.Valid() {
				key := it.UnsafeKey()
				if len(key) < len(prefix) {
					_ = it.Close()
					return errors.New("collections: malformed unique index entry during source replacement")
				}
				documentID := key[len(prefix):]
				if indexDef.ValueType == IndexValueBSONOrderedV2 {
					documentID, err = bsonIndexKeyDocumentIDV2(key)
					if err != nil {
						_ = it.Close()
						return err
					}
				}
				if _, ok := replacing[string(documentID)]; !ok {
					_ = it.Close()
					return fmt.Errorf("%w %q", ErrUniqueIndexConflict, run.indexName)
				}
				it.Next()
			}
			if it != nil {
				iterErr := it.Error()
				closeErr := it.Close()
				if iterErr != nil {
					return iterErr
				}
				if closeErr != nil {
					return closeErr
				}
			}
		}
	}
	return nil
}

func (c *Collection) publishSourceReplacementPlan(plan *sourceReplacementPlan, hooks *sourcePublicationHooks) error {
	if plan == nil {
		return errors.New("collections: missing source replacement plan")
	}
	rootNames, tables, policies, cleanupCoalesced, err := coalesceCollectionRootDeltaTables(plan.meta.Name, plan.rootNames, plan.deltaTables, plan.policies)
	if err != nil {
		return err
	}
	defer cleanupCoalesced()
	tables, cleanupPointerized, err := pointerizeCollectionRootDeltaTables(c.db, plan.meta, rootNames, tables)
	if err != nil {
		return err
	}
	defer cleanupPointerized()
	ordered, cleanupDeltas, err := buildRootDeltaBatchPublishInputsFromTables(plan.meta.Name, rootNames, tables, plan.baseRootIDs, policies)
	if err != nil {
		return err
	}
	defer cleanupDeltas()
	if hooks != nil && hooks.beforePublish != nil {
		if err := hooks.beforePublish(); err != nil {
			return err
		}
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(plan.snap.Pager(), plan.baseUserRoot, plan.baseSystemRoot, plan.baseCommitSeq)
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	var publishMeta = plan.meta
	var publishRootNames = rootNames
	publish := func() error {
		if columnStoreWriteEnabled(plan.meta) {
			columnOperation := ColumnPublishOperationUpdate
			if len(plan.deleteColumnDocs) == 0 {
				columnOperation = ColumnPublishOperationInsert
			}
			newSystemRoot, rootIDs, publishMeta, publishRootNames, err = c.publishRootDeltaBatchGroupMaybeColumn(ordered, preflight, columnWritePublishInput{
				meta:                  plan.meta,
				catalog:               plan.catalog,
				baseCommitSeq:         plan.baseCommitSeq,
				baseSystemRoot:        plan.baseSystemRoot,
				rootNames:             cloneColumnPublishRootNames(rootNames),
				baseRootIDs:           cloneColumnPublishBaseRootIDs(plan.baseRootIDs),
				commandWALIntent:      plan.commandWAL,
				rawPublishLocked:      true,
				operation:             columnOperation,
				documents:             plan.insertColumnDocs,
				sourceDeleteDocuments: plan.deleteColumnDocs,
				rows:                  len(plan.insertColumnDocs),
			})
			return err
		}
		input := columnWritePublishInput{
			meta: plan.meta, baseCommitSeq: plan.baseCommitSeq, baseSystemRoot: plan.baseSystemRoot,
			rootNames: cloneColumnPublishRootNames(rootNames), baseRootIDs: cloneColumnPublishBaseRootIDs(plan.baseRootIDs),
			commandWALIntent: plan.commandWAL, rawPublishLocked: plan.commandWAL != nil,
		}
		newSystemRoot, rootIDs, err = c.publishRootDeltaBatchGroupWithoutColumn(ordered, preflight, input)
		return err
	}
	if err = c.withCommandWALPublishCoordinatorForIntent(plan.commandWAL, publish); err != nil {
		return err
	}
	if len(rootIDs) != len(publishRootNames) {
		return &CommitAmbiguousError{
			Operation: "atomic source replacement root publication",
			Err:       unexpectedOrderedRootCountError(plan.meta.Name, len(publishRootNames), len(rootIDs)),
		}
	}
	nextCatalog := cloneCatalogWithRootUpdates(plan.catalog, publishMeta, publishRootNames, rootIDs)
	c.meta = publishMeta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.clearWriteDomainPrimaryDocumentCache()
	if hooks != nil && hooks.afterPublish != nil {
		if err := hooks.afterPublish(); err != nil {
			return &CommitAmbiguousError{Operation: "atomic source replacement after publication", Err: err}
		}
	}
	return nil
}

func buildColumnSourceReplacementRowLocatorDeltaBatch(plan ColumnPublishPlan, deleted, inserted []columnWriteDocument, baseRoot uint64, policy backenddb.OrderedRootStoragePolicy) (backenddb.OrderedRootDeltaBatchPublishInput, func(), error) {
	table := newCollectionRunTable(len(deleted) + len(inserted))
	for _, document := range deleted {
		if len(document.ID) == 0 {
			resetCollectionRunTable(table)
			return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, errors.New("collections: source replacement row locator delete missing document id")
		}
		table.DeleteSteal(bytes.Clone(document.ID))
	}
	for row, document := range inserted {
		if len(document.ID) == 0 {
			resetCollectionRunTable(table)
			return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, errors.New("collections: source replacement row locator insert missing document id")
		}
		setCollectionRunValue(table, bytes.Clone(document.ID), encodeColumnPrimaryRowLocator(DocumentRowRef{
			Generation:        plan.UpdatedActiveManifest.Generation,
			PartID:            columnPhysicalRowAssetPartID + (1 << 32),
			RowIndex:          row,
			AppliedCommandLSN: plan.AppliedCommandLSN,
		}))
	}
	table.Freeze()
	it := table.NewIterator(nil, nil)
	delta, err := backenddb.OrderedRootDeltaBatchFromIterator(it)
	if err != nil {
		_ = it.Close()
		resetCollectionRunTable(table)
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, err
	}
	cleanup := func() {
		_ = delta.Close()
		_ = it.Close()
		resetCollectionRunTable(table)
	}
	return backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: baseRoot, Delta: delta, StoragePolicy: policy}, cleanup, nil
}
