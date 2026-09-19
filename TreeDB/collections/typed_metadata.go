package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// TypedMetadataUpdateResult counts existing IDs and actual changes separately.
type TypedMetadataUpdateResult struct{ MatchedCount, ModifiedCount int }

// UpdateTypedMetadataByID changes only meta.* fields, skipping missing IDs.
// expectedGeneration is the schema generation, not a document version. Required
// declared scalar fields cannot be unset. The batch is one atomic publication;
// unchanged content/vectors remain in their existing physical rows and WAL
// records contain only metadata after-images. Ambiguous errors must not be retried.
func (c *Collection) UpdateTypedMetadataByID(ids [][]byte, set map[string]any, unset []string, expectedGeneration uint64) (TypedMetadataUpdateResult, error) {
	if expectedGeneration == 0 {
		return TypedMetadataUpdateResult{}, errors.New("collections: expected_generation must be positive")
	}
	ownedIDs := make([][]byte, len(ids))
	seen := make(map[string]bool, len(ids))
	for i, id := range ids {
		if len(id) == 0 || !utf8.Valid(id) {
			return TypedMetadataUpdateResult{}, errors.New("collections: metadata ID must be nonempty UTF-8")
		}
		if seen[string(id)] {
			return TypedMetadataUpdateResult{}, ErrDuplicateDocumentID
		}
		seen[string(id)] = true
		ownedIDs[i] = bytes.Clone(id)
	}
	slices.SortFunc(ownedIDs, bytes.Compare)
	// Freeze JSON values before planning, reject non-JSON/NaN input, and keep
	// numbers lossless when comparing existing retained JSON for no-op admission.
	raw, err := json.Marshal(set)
	if err != nil {
		return TypedMetadataUpdateResult{}, err
	}
	var ownedSet map[string]any
	if err := decodeTypedMetadataJSON(raw, &ownedSet); err != nil {
		return TypedMetadataUpdateResult{}, err
	}
	return c.updateTypedMetadataByID(ownedIDs, ownedSet, slices.Clone(unset), expectedGeneration, nil, nil)
}

func decodeTypedMetadataJSON(raw []byte, out any) error {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return errors.New("collections: metadata must be valid UTF-8 JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(out)
}

func typedMetadataPathsOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+".") || strings.HasPrefix(b, a+".")
}

func validateTypedMetadataMutation(meta CollectionMeta, set map[string]any, unset []string, generation uint64) error {
	cfg := meta.Options.ColumnStore
	if cfg == nil {
		return ErrHybridSearchUnsupported
	}
	if err := validateTypedProjectionMeta(meta, &trustedFloat32Projection{columns: cfg.Columns, schemaHash: cfg.SchemaHash}); err != nil {
		return err
	}
	var actual uint64
	for _, def := range meta.VectorIndexes {
		actual = max(actual, def.SchemaGeneration)
	}
	for _, def := range meta.TextIndexes {
		actual = max(actual, def.SchemaGeneration)
	}
	actual = max(actual, 1)
	if generation == 0 || generation != actual {
		return fmt.Errorf("%w: metadata expected_generation=%d current=%d", ErrHybridSearchStaleIndex, generation, actual)
	}
	paths := make([]string, 0, len(set)+len(unset))
	for path := range set {
		paths = append(paths, path)
	}
	paths = append(paths, unset...)
	slices.Sort(paths)
	seen := make(map[string]bool, len(paths))
	for _, path := range paths {
		if !utf8.ValidString(path) || !strings.HasPrefix(path, "meta.") || strings.ContainsAny(path, "\x00\\*?[]#") {
			return fmt.Errorf("collections: unsupported metadata path %q", path)
		}
		if seen[path] {
			return errors.New("collections: overlapping metadata paths")
		}
		parts := strings.Split(path, ".")
		for i, part := range parts {
			if part == "" {
				return errors.New("collections: metadata path has empty component")
			}
			if seen[strings.Join(parts[:i], ".")] {
				return errors.New("collections: overlapping metadata paths")
			}
		}
		seen[path] = true
		for _, reserved := range []string{chunking.MetaFieldParent, chunking.MetaFieldOrdinal, chunking.MetaFieldKind} {
			if typedMetadataPathsOverlap(path, "meta."+reserved) {
				return errors.New("collections: chunk linkage metadata is immutable")
			}
		}
		for _, index := range meta.TextIndexes {
			for _, field := range index.Fields {
				if typedMetadataPathsOverlap(path, field.Field) {
					return errors.New("collections: text-indexed metadata mutation is unsupported")
				}
			}
		}
		for _, col := range cfg.Columns {
			if !typedMetadataPathsOverlap(path, col.Path) {
				continue
			}
			value, setting := set[path]
			text, stringValue := value.(string)
			if path != col.Path || !columnMetadataStoredColumn(col) || !setting || !stringValue || !utf8.ValidString(text) {
				return fmt.Errorf("collections: metadata column %q requires a non-null string set at its exact path", col.Name)
			}
		}
	}
	return nil
}

func (c *Collection) updateTypedMetadataByID(ids [][]byte, set map[string]any, unset []string, generation uint64, replayPayload *commitlog.CollectionTypedMetadataPayload, replay *backenddb.CommandWALIntent) (TypedMetadataUpdateResult, error) {
	if c == nil || c.db == nil {
		return TypedMetadataUpdateResult{}, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return TypedMetadataUpdateResult{}, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	if replayPayload == nil {
		if err := validateTypedMetadataMutation(c.Meta(), set, unset, generation); err != nil {
			return TypedMetadataUpdateResult{}, err
		}
	}
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return TypedMetadataUpdateResult{}, err
	}
	if err := c.requireColumnStoreCommandWAL(c.Meta(), replay); err != nil {
		return TypedMetadataUpdateResult{}, err
	}
	if replay == nil {
		if err := c.db.CheckCommandWALPublishReady(); err != nil {
			return TypedMetadataUpdateResult{}, err
		}
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
		return TypedMetadataUpdateResult{}, err
	}
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		plan, payload, result, err := c.buildTypedMetadataPlan(ids, set, unset, replayPayload)
		if err != nil {
			return TypedMetadataUpdateResult{}, err
		}
		if result.ModifiedCount == 0 {
			plan.close()
			if replay != nil {
				return result, c.publishCommandWALNoop(replay, false)
			}
			return result, nil
		}
		intent := replay
		if intent == nil {
			raw, e := commitlog.EncodeCollectionTypedMetadataPayload(payload)
			if e == nil {
				intent, e = c.db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionTypedMetadataByIDV1, raw)
			}
			if e != nil {
				plan.close()
				return TypedMetadataUpdateResult{}, e
			}
		}
		_, err = c.publishUpdateBatchPlanLocked(plan, intent)
		plan.close()
		if isRetriableCollectionMutationError(err) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return result, err
	}
	return TypedMetadataUpdateResult{}, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) buildTypedMetadataPlan(ids [][]byte, set map[string]any, unset []string, replay *commitlog.CollectionTypedMetadataPayload) (_ *updateBatchPlan, payload commitlog.CollectionTypedMetadataPayload, result TypedMetadataUpdateResult, retErr error) {
	plan := newUpdateBatchPlan()
	defer func() {
		if retErr != nil {
			plan.close()
		}
	}()
	plan.snap = c.db.AcquireSnapshot()
	if plan.snap == nil {
		return nil, payload, result, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(plan.snap)
	if err != nil {
		return nil, payload, result, err
	}
	if catalog == nil {
		return nil, payload, result, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		return nil, payload, result, err
	}
	plan.catalog, plan.meta = catalog, catalog.meta
	plan.baseUserRoot, plan.baseSystemRoot, plan.baseCommitSeq = snapshotUserRoot(plan.snap), snapshotSystemRoot(plan.snap), snapshotCommitSeq(plan.snap)
	plan.baseRootIDs = make(map[string]uint64)
	plan.results = make([]UpdateBatchResult, len(ids))
	cfg := *plan.meta.Options.ColumnStore
	if err := validateTypedProjectionMeta(plan.meta, &trustedFloat32Projection{columns: cfg.Columns, schemaHash: cfg.SchemaHash}); err != nil {
		return nil, payload, result, err
	}
	columns := make([]int, 0, len(cfg.Columns))
	for i, col := range cfg.Columns {
		if columnMetadataStoredColumn(col) {
			columns = append(columns, i)
		}
	}
	slices.SortFunc(columns, func(a, b int) int { return strings.Compare(cfg.Columns[a].Name, cfg.Columns[b].Name) })
	payload.Collection, payload.SchemaHash = plan.meta.Name, cfg.SchemaHash
	for _, i := range columns {
		payload.Columns = append(payload.Columns, cfg.Columns[i].Name)
	}
	if replay != nil && (replay.Collection != payload.Collection || replay.SchemaHash != payload.SchemaHash || !slices.Equal(replay.Columns, payload.Columns) || len(replay.Documents) != len(ids)) {
		return nil, payload, result, errors.New("collections: metadata replay schema mismatch")
	}
	opts, err := collectionPlannerOptionsForDB(c.db, plan.meta)
	if err != nil {
		return nil, payload, result, err
	}
	runtimes, err := catalog.cachedIndexRuntimes()
	if err != nil {
		return nil, payload, result, err
	}
	view := newCollectionReadViewAtSnapshot(c, plan.snap, catalog, false, "")
	defer view.Close()
	if err := view.ensureAssetReadCaches(cfg, ColumnAssetReadIntegrityVerify); err != nil {
		return nil, payload, result, err
	}
	physical, err := view.materializerColumnSnapshotView(cfg)
	if err != nil {
		return nil, payload, result, err
	}
	projection, err := view.pointRowScanProjection(physical, nil)
	if err != nil {
		return nil, payload, result, err
	}
	var scratch columnPhysicalRowReaderScratch
	var indexArena indexEncodeArena
	var changed []preparedBatchUpdate
	for pos, id := range ids {
		raw, found, err := collectionGetAppendAtCatalogRoot(plan.snap, catalog, collectionPrimaryRootName(plan.meta.Name), id, nil)
		if err != nil {
			return nil, payload, result, err
		}
		if !found {
			if replay != nil {
				return nil, payload, result, errors.New("collections: metadata replay missing document")
			}
			continue
		}
		result.MatchedCount++
		plan.results[pos].Matched = true
		locator, found, err := collectionGetAppendAtCatalogRoot(plan.snap, catalog, collectionColumnRowLocatorRootName(plan.meta.Name), id, nil)
		if err != nil || !found {
			return nil, payload, result, errors.Join(err, errors.New("collections: metadata update missing row locator"))
		}
		latest, err := decodeColumnPrimaryRowLocatorBorrowedID(id, locator)
		if err != nil {
			return nil, payload, result, err
		}
		scoring, err := decodeColumnScoringRowLocatorBorrowedID(id, locator)
		if err != nil {
			return nil, payload, result, err
		}
		row, err := view.fetchDocumentPointRow(physical, latest, projection, &scratch, nil)
		if err != nil {
			return nil, payload, result, err
		}
		oldValues := make([]columnDeclaredValue, len(cfg.Columns))
		for j, col := range physical.Config.Columns {
			for k, fullCol := range cfg.Columns {
				if col.Name == fullCol.Name {
					oldValues[k] = row.Values[j]
					if oldValues[k].StringBytes != nil {
						oldValues[k].String = string(oldValues[k].StringBytes)
						oldValues[k].StringBytes = nil
					}
					break
				}
			}
		}
		newValues := slices.Clone(oldValues)
		var object map[string]any
		if err := decodeTypedMetadataJSON(raw, &object); err != nil || object == nil {
			return nil, payload, result, errors.Join(err, errors.New("collections: invalid retained metadata object"))
		}
		before := bytes.Clone(raw)
		metadataChanged := false
		if replay != nil {
			d := replay.Documents[pos]
			if !bytes.Equal(d.ID, id) || len(d.Values) != len(columns) {
				return nil, payload, result, errors.New("collections: metadata replay row mismatch")
			}
			var after map[string]any
			if err := decodeTypedMetadataJSON(d.Retained, &after); err != nil || after == nil {
				return nil, payload, result, errors.New("collections: invalid metadata replay retained JSON")
			}
			oldMeta, oldHasMeta := object["meta"]
			newMeta, newHasMeta := after["meta"]
			delete(object, "meta")
			delete(after, "meta")
			if !reflect.DeepEqual(object, after) {
				return nil, payload, result, errors.New("collections: metadata replay changes non-metadata fields")
			}
			if newHasMeta {
				after["meta"] = newMeta
			}
			if oldHasMeta {
				object["meta"] = oldMeta
			}
			metadataChanged = !reflect.DeepEqual(object, after)
			object = after
			for j, k := range columns {
				newValues[k] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: d.Values[j]}
			}
		} else {
			for path, value := range set {
				if j := typedStringColumnIndex(cfg.Columns, path); j >= 0 {
					newValues[j].String = value.(string)
					continue
				}
				changed, err := applyTypedMetadataPath(object, path, value, false)
				if err != nil {
					return nil, payload, result, err
				}
				metadataChanged = metadataChanged || changed
			}
			for _, path := range unset {
				changed, err := applyTypedMetadataPath(object, path, nil, true)
				if err != nil {
					return nil, payload, result, err
				}
				metadataChanged = metadataChanged || changed
			}
		}
		scalarChanged := false
		for _, j := range columns {
			scalarChanged = scalarChanged || oldValues[j].String != newValues[j].String
		}
		if !metadataChanged && !scalarChanged {
			continue
		}
		retained := before
		if metadataChanged {
			retained, err = json.Marshal(object)
			if err != nil {
				return nil, payload, result, err
			}
		}
		p := &trustedFloat32Projection{columns: cfg.Columns, typedRows: map[string][]columnDeclaredValue{string(id): nil}, retainedJSON: [][]byte{retained}}
		if _, err := validateTrustedFloat32ProjectionRetainedJSONWithOwnership([][]byte{id}, p, false); err != nil {
			return nil, payload, result, err
		}
		oldState, err := typedIndexState(oldValues, cfg.Columns, runtimes, &indexArena)
		if err != nil {
			return nil, payload, result, err
		}
		newState, err := typedIndexState(newValues, cfg.Columns, runtimes, &indexArena)
		if err != nil {
			return nil, payload, result, err
		}
		update := preparedBatchUpdate{itemIndex: pos, documentID: id, document: retained, oldState: oldState, newState: newState}
		for j := range runtimes {
			if orderedDocumentIndexRuntimeChanged(oldState, newState, j) {
				update.indexStateChanged = true
				if bit, ok := updateIndexChangedMaskBit(j); ok {
					update.changedIndexes |= bit
				}
			}
		}
		changed = append(changed, update)
		values := make([]columnDeclaredValue, len(cfg.Columns))
		wal := commitlog.CollectionTypedMetadataDocument{ID: bytes.Clone(id), Retained: retained}
		for _, j := range columns {
			values[j] = newValues[j]
			wal.Values = append(wal.Values, newValues[j].String)
		}
		preserved := columnCoordinates(scoring)
		plan.metadataDocuments = append(plan.metadataDocuments, columnWriteDocument{ID: bytes.Clone(id), Document: retained, preserved: &preserved, declaredValues: values, declaredValuesReady: true})
		payload.Documents = append(payload.Documents, wal)
		plan.results[pos].Modified = true
		result.ModifiedCount++
	}
	plan.stats.Items, plan.stats.Matched, plan.stats.Modified = len(ids), result.MatchedCount, result.ModifiedCount
	if len(changed) == 0 {
		return plan, payload, result, nil
	}
	replacements := batchUniqueReplacementOwners(runtimes, changed)
	for _, update := range changed {
		if err := rejectReplaceUniqueConflictsOrdered(plan.snap, catalog, runtimes, update, replacements); err != nil {
			return nil, payload, result, err
		}
	}
	if err := rejectBatchUniqueConflicts(runtimes, changed); err != nil {
		return nil, payload, result, err
	}
	add := func(name string, policy backenddb.OrderedRootStoragePolicy, table memtable.Table) {
		plan.rootNames = append(plan.rootNames, name)
		plan.baseRootIDs[name] = catalog.rootID(name)
		plan.policies = append(plan.policies, policy)
		plan.deltaTables = append(plan.deltaTables, table)
	}
	primary := newCollectionRunTable(len(changed))
	for _, update := range changed {
		setCollectionRunCopiedValue(primary, update.documentID, update.document)
	}
	primary.Freeze()
	add(collectionPrimaryRootName(plan.meta.Name), opts.dataStoragePolicy, primary)
	if len(runtimes) > 0 && persistIndexStateForOptions(opts) {
		stateTable := newCollectionRunTable(len(changed))
		add(collectionIndexStateRootName(plan.meta.Name), opts.indexStateStoragePolicy, stateTable)
		for _, update := range changed {
			raw, err := encodeRuntimeOrderedDocumentIndexState(update.newState, runtimes)
			if err != nil {
				return nil, payload, result, err
			}
			stateTable.SetSteal(bytes.Clone(update.documentID), raw)
		}
		stateTable.Freeze()
	}
	for j, runtime := range runtimes {
		table := newCollectionRunTable(0)
		// Register ownership before fallible entry encoding.
		plan.rootNames = append(plan.rootNames, runtimeSecondaryRootName(plan.meta.Name, runtime))
		plan.baseRootIDs[plan.rootNames[len(plan.rootNames)-1]] = catalog.rootID(plan.rootNames[len(plan.rootNames)-1])
		plan.policies = append(plan.policies, runtime.def.storagePolicy)
		plan.deltaTables = append(plan.deltaTables, table)
		for _, update := range changed {
			if !orderedDocumentIndexRuntimeChanged(update.oldState, update.newState, j) {
				continue
			}
			for _, value := range update.oldState.valuesAt(j) {
				if _, err := deleteCollectionSecondaryIndexEntryForValueType(table, runtime.def.valueType, value, update.documentID); err != nil {
					return nil, payload, result, err
				}
			}
			for _, value := range update.newState.valuesAt(j) {
				if _, err := setCollectionSecondaryIndexEntryForValueType(table, runtime.def.valueType, value, update.documentID); err != nil {
					return nil, payload, result, err
				}
			}
		}
		table.Freeze()
	}
	return plan, payload, result, nil
}

func applyTypedMetadataPath(object map[string]any, path string, value any, unset bool) (bool, error) {
	parts := strings.Split(path, ".")
	for _, part := range parts[:len(parts)-1] {
		child, exists := object[part]
		if !exists {
			if unset {
				return false, nil
			}
			next := make(map[string]any)
			object[part] = next
			object = next
			continue
		}
		next, ok := child.(map[string]any)
		if !ok {
			return false, fmt.Errorf("collections: metadata path %q traverses non-object", path)
		}
		object = next
	}
	key := parts[len(parts)-1]
	old, exists := object[key]
	if unset {
		if !exists {
			return false, nil
		}
		delete(object, key)
		return true, nil
	}
	if exists && reflect.DeepEqual(old, value) {
		return false, nil
	}
	object[key] = value
	return true, nil
}
