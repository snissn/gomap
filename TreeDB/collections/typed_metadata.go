package collections

import (
	"bytes"
	"encoding"
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
	"github.com/snissn/gomap/TreeDB/internal/strictjson"
)

// TypedMetadataUpdateResult counts existing IDs and actual changes separately.
type TypedMetadataUpdateResult struct{ MatchedCount, ModifiedCount int }

// ErrTypedMetadataInvalid distinguishes rejected input from storage corruption
// or an ambiguous durable publication at the public service boundary.
var ErrTypedMetadataInvalid = errors.New("collections: invalid typed metadata update")

// UpdateTypedMetadataByID changes only meta.* fields, skipping missing IDs.
// expectedGeneration is the schema generation, not a document version. Required
// declared scalar fields cannot be unset. The batch is one atomic publication;
// unchanged content/vectors remain in their existing physical rows and WAL
// records contain only metadata after-images. Ambiguous errors must not be retried.
// Custom MarshalJSON output is validated as JSON; MarshalText-dependent values
// and map keys must instead be supplied as explicit UTF-8 strings. Serializers
// must not mutate the request. All potentially serialized Go fields are checked.
func (c *Collection) UpdateTypedMetadataByID(ids [][]byte, set map[string]any, unset []string, expectedGeneration uint64) (TypedMetadataUpdateResult, error) {
	if len(ids) == 0 {
		return TypedMetadataUpdateResult{}, fmt.Errorf("%w: at least one ID is required", ErrTypedMetadataInvalid)
	}
	if expectedGeneration == 0 {
		return TypedMetadataUpdateResult{}, fmt.Errorf("%w: expected_generation must be positive", ErrTypedMetadataInvalid)
	}
	ownedIDs := make([][]byte, len(ids))
	seen := make(map[string]bool, len(ids))
	for i, id := range ids {
		if len(id) == 0 || !utf8.Valid(id) {
			return TypedMetadataUpdateResult{}, fmt.Errorf("%w: metadata ID must be nonempty UTF-8", ErrTypedMetadataInvalid)
		}
		if seen[string(id)] {
			return TypedMetadataUpdateResult{}, ErrDuplicateDocumentID
		}
		seen[string(id)] = true
		ownedIDs[i] = bytes.Clone(id)
	}
	slices.SortFunc(ownedIDs, bytes.Compare)
	// Validate before serialization: encoding/json replaces invalid UTF-8 and
	// MarshalText output lossily. Do not invoke unsupported text callbacks.
	if !typedMetadataValidJSONInput(reflect.ValueOf(set), 0) {
		return TypedMetadataUpdateResult{}, fmt.Errorf("%w: metadata requires valid UTF-8 strings/keys and no MarshalText-dependent encoding", ErrTypedMetadataInvalid)
	}
	// Freeze JSON values before planning, reject non-JSON/NaN input, and keep
	// numbers lossless when comparing existing retained JSON for no-op admission.
	raw, err := json.Marshal(set)
	if err != nil {
		return TypedMetadataUpdateResult{}, errors.Join(ErrTypedMetadataInvalid, err)
	}
	var ownedSet map[string]any
	if err := decodeTypedMetadataJSON(raw, &ownedSet); err != nil {
		return TypedMetadataUpdateResult{}, errors.Join(ErrTypedMetadataInvalid, err)
	}
	return c.updateTypedMetadataByID(ownedIDs, ownedSet, slices.Clone(unset), expectedGeneration, nil, nil)
}

func typedMetadataValidJSONInput(value reflect.Value, depth int) bool {
	// JSON cannot exceed encoding/json's 10,000-level decoding limit.
	if depth > 10000 {
		return false
	}
	if !value.IsValid() || ((value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil()) {
		return true
	}
	// Match encoding/json's method precedence and addressability without
	// invoking user code twice. MarshalJSON owns its representation; strictjson
	// checks its output. MarshalText would undergo lossy string conversion, so
	// callers must supply an explicit UTF-8 string instead. Check the static
	// interface type before unwrapping it, just as encoding/json does.
	if value.Kind() != reflect.Pointer && value.CanAddr() && value.Addr().Type().Implements(reflect.TypeFor[json.Marshaler]()) {
		return true
	}
	if value.Type().Implements(reflect.TypeFor[json.Marshaler]()) {
		return true
	}
	if (value.Kind() != reflect.Pointer && value.CanAddr() && value.Addr().Type().Implements(reflect.TypeFor[encoding.TextMarshaler]())) || value.Type().Implements(reflect.TypeFor[encoding.TextMarshaler]()) {
		return false
	}
	switch value.Kind() {
	case reflect.String:
		return utf8.ValidString(value.String())
	case reflect.Interface, reflect.Pointer:
		return typedMetadataValidJSONInput(value.Elem(), depth+1)
	case reflect.Map:
		it := value.MapRange()
		for it.Next() {
			// Map keys ignore MarshalJSON; string kinds also ignore MarshalText.
			key := it.Key()
			if key.Kind() == reflect.String {
				if !utf8.ValidString(key.String()) {
					return false
				}
			} else if key.Type().Implements(reflect.TypeFor[encoding.TextMarshaler]()) && !(key.Kind() == reflect.Pointer && key.IsNil()) {
				return false
			}
			if !typedMetadataValidJSONInput(it.Value(), depth+1) {
				return false
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if !typedMetadataValidJSONInput(value.Index(i), depth+1) {
				return false
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			fieldType := field.Type
			if fieldType.Kind() == reflect.Pointer {
				fieldType = fieldType.Elem()
			}
			visible := field.IsExported() || (field.Anonymous && fieldType.Kind() == reflect.Struct)
			if visible && field.Tag.Get("json") != "-" && !typedMetadataValidJSONInput(value.Field(i), depth+1) {
				return false
			}
		}
	}
	return true
}

func decodeTypedMetadataJSON(raw []byte, out any) error {
	if !strictjson.Valid(raw) {
		return errors.New("collections: metadata must be lossless UTF-8 JSON")
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
	for i, path := range paths {
		if !utf8.ValidString(path) || !strings.HasPrefix(path, "meta.") || strings.ContainsAny(path, "\x00\\*?[]#") {
			return fmt.Errorf("collections: unsupported metadata path %q", path)
		}
		if i > 0 && paths[i-1] == path {
			return errors.New("collections: overlapping metadata paths")
		}
		if strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
			return errors.New("collections: metadata path has empty component")
		}
		// Avoid both all-pairs comparisons and rebuilding every ancestor of
		// a deep path. Punctuation may separate ancestors in lexical order.
		prefix := path + "."
		j, _ := slices.BinarySearch(paths, prefix)
		if j < len(paths) && strings.HasPrefix(paths[j], prefix) {
			return errors.New("collections: overlapping metadata paths")
		}
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
			return TypedMetadataUpdateResult{}, errors.Join(ErrTypedMetadataInvalid, err)
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
	if plan.meta.Options.ColumnStore == nil {
		return nil, payload, result, fmt.Errorf("%w: metadata update requires a typed column schema", ErrHybridSearchUnsupported)
	}
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
			if err := validateTypedMetadataReplayChanges(plan.meta, object, after, oldValues, columns, d.Values); err != nil {
				return nil, payload, result, err
			}
			object = after
			for j, k := range columns {
				newValues[k] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: d.Values[j]}
			}
		} else {
			for path, value := range set {
				if j := typedStringColumnIndex(cfg.Columns, path); j >= 0 {
					text, ok := value.(string)
					if !ok {
						return nil, payload, result, fmt.Errorf("%w: metadata column %q requires a string value", ErrTypedMetadataInvalid, cfg.Columns[j].Name)
					}
					newValues[j] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: text}
					continue
				}
				changed, err := applyTypedMetadataPath(object, path, value, false)
				if err != nil {
					return nil, payload, result, errors.Join(ErrTypedMetadataInvalid, err)
				}
				metadataChanged = metadataChanged || changed
			}
			for _, path := range unset {
				changed, err := applyTypedMetadataPath(object, path, nil, true)
				if err != nil {
					return nil, payload, result, errors.Join(ErrTypedMetadataInvalid, err)
				}
				metadataChanged = metadataChanged || changed
			}
		}
		scalarChanged := false
		for _, j := range columns {
			scalarChanged = scalarChanged || oldValues[j].Type != newValues[j].Type ||
				oldValues[j].Present != newValues[j].Present || oldValues[j].Null != newValues[j].Null ||
				oldValues[j].String != newValues[j].String
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
		var stateTable memtable.Table
		for _, update := range changed {
			if !update.indexStateChanged {
				continue
			}
			if stateTable == nil {
				stateTable = newCollectionRunTable(len(changed))
				add(collectionIndexStateRootName(plan.meta.Name), opts.indexStateStoragePolicy, stateTable)
			}
			raw, err := encodeRuntimeOrderedDocumentIndexState(update.newState, runtimes)
			if err != nil {
				return nil, payload, result, err
			}
			stateTable.SetSteal(bytes.Clone(update.documentID), raw)
		}
		if stateTable != nil {
			stateTable.Freeze()
		}
	}
	for j, runtime := range runtimes {
		var table memtable.Table
		for _, update := range changed {
			if !orderedDocumentIndexRuntimeChanged(update.oldState, update.newState, j) {
				continue
			}
			if table == nil {
				table = newCollectionRunTable(0)
				add(runtimeSecondaryRootName(plan.meta.Name, runtime), runtime.def.storagePolicy, table)
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
		if table != nil {
			table.Freeze()
		}
	}
	return plan, payload, result, nil
}

// The wire codec validates bytes; this layer checks the after-image against
// the preceding schema and row. Recovery cannot use format 13 to rewrite a
// protected metadata/text/linkage field that the public mutation would reject.
func validateTypedMetadataReplayChanges(meta CollectionMeta, before, after map[string]any, oldValues []columnDeclaredValue, columns []int, values []string) error {
	var generation uint64
	for _, def := range meta.VectorIndexes {
		generation = max(generation, def.SchemaGeneration)
	}
	for _, def := range meta.TextIndexes {
		generation = max(generation, def.SchemaGeneration)
	}
	generation = max(generation, 1)
	set := make(map[string]any)
	candidate := make(map[string]any, 1)
	var unset []string
	var diff func(string, any, bool, any, bool, bool) error
	diff = func(path string, old any, oldExists bool, next any, nextExists, addressable bool) error {
		if oldExists == nextExists && reflect.DeepEqual(old, next) {
			return nil
		}
		if !addressable {
			return errors.New("collections: metadata replay changes an unaddressable object key")
		}
		// Public set values are whole JSON values: their literal keys are not
		// dotted mutation paths. Coalesce at the highest admitted path before
		// inspecting children; descend only around protected ancestors.
		var admission error
		if nextExists {
			candidate[path] = next
			admission = validateTypedMetadataMutation(meta, candidate, nil, generation)
			delete(candidate, path)
		} else {
			admission = validateTypedMetadataMutation(meta, nil, []string{path}, generation)
		}
		if admission == nil {
			if nextExists {
				set[path] = next
			} else {
				unset = append(unset, path)
			}
			return nil
		}
		oldMap, oldObject := old.(map[string]any)
		nextMap, nextObject := next.(map[string]any)
		if (!oldExists || oldObject) && nextObject && len(oldMap)+len(nextMap) != 0 {
			child := func(key string, old any, oldExists bool, next any, nextExists bool) error {
				valid := key != "" && utf8.ValidString(key) && !strings.ContainsAny(key, ".\x00\\*?[]#")
				return diff(path+"."+key, old, oldExists, next, nextExists, valid)
			}
			for key, value := range oldMap {
				n, ok := nextMap[key]
				if err := child(key, value, true, n, ok); err != nil {
					return err
				}
			}
			for key, value := range nextMap {
				if _, ok := oldMap[key]; !ok {
					if err := child(key, nil, false, value, true); err != nil {
						return err
					}
				}
			}
			return nil
		}
		return admission
	}
	old, oldExists := before["meta"]
	next, nextExists := after["meta"]
	if err := diff("meta", old, oldExists, next, nextExists, true); err != nil {
		return err
	}
	for i, j := range columns {
		if oldValues[j].String != values[i] {
			set[meta.Options.ColumnStore.Columns[j].Path] = values[i]
		}
	}
	return validateTypedMetadataMutation(meta, set, unset, generation)
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
