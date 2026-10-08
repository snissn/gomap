package collections

import (
	"bytes"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
	"slices"
)

var ErrTypedStringPatchInvalid = errors.New("collections: invalid typed string patch")
var ErrTypedStringPatchConflict = errors.New("collections: typed string patch revision conflict")

type TypedStringEdit struct{ Column, Value string }
type TypedStringResidualMode uint8

const (
	TypedStringResidualPreserve TypedStringResidualMode = iota
	TypedStringResidualReplace
)

type TypedStringPatch struct {
	ID           []byte
	Edits        []TypedStringEdit
	Expected     *DocumentRowRef
	ResidualMode TypedStringResidualMode
	Residual     []byte
}
type TypedStringPatchResult struct{ MatchedCount, ModifiedCount int }

// PatchTypedStringsBatch atomically changes explicit declared string slots.
// Missing IDs are skipped. Absent edits and Preserve leave existing physical
// coordinates and the primary ValuePtr untouched; Replace replaces the entire
// non-column JSON residual. Empty strings are present values. Null/deletion and
// unsupported schemas are rejected before WAL. An ambiguous ACK is not retryable.
func (c *Collection) PatchTypedStringsBatch(requests []TypedStringPatch, expectedSchemaHash uint64) (TypedStringPatchResult, error) {
	if c == nil || c.db == nil {
		return TypedStringPatchResult{}, errCollectionNil
	}
	if len(requests) == 0 || expectedSchemaHash == 0 {
		return TypedStringPatchResult{}, fmt.Errorf("%w: nonempty batch and schema hash required", ErrTypedStringPatchInvalid)
	}
	// Borrow stable schema under its existing admission lock. Deep-copying the
	// full manifest here would duplicate owned metadata before request credit.
	unlockSchema := c.lockCollectionSchemaRead()
	if err := validateTypedStringPatchInput(requests, c.MetaView(), expectedSchemaHash); err != nil {
		unlockSchema()
		return TypedStringPatchResult{}, err
	}
	unlockSchema()
	var admission nativeStringPatchAdmission
	if c.db.ResolvedProfile() == backenddb.ProfileCommandWALDurable {
		var err error
		admission, err = c.acquireNativeStringPatchAdmission()
		if err != nil {
			return TypedStringPatchResult{}, err
		}
		defer admission.release()
	}
	if admission.coord != nil {
		limits, err := nativeStringPatchInputCreditLimits(requests)
		if err != nil {
			return TypedStringPatchResult{}, err
		}
		prepared, err := prepareNativeStringPatchInput(requests, limits)
		if err != nil {
			return TypedStringPatchResult{}, err
		}
		if !prepared.consume() {
			panic("collections: fresh native input already consumed")
		}
		defer prepared.finish()
		if hook := nativeStringPatchInputCopiedTestHook.Load(); hook != nil {
			(*hook)()
		}
		return c.joinNativeStringPatchGroup(&admission, prepared, expectedSchemaHash)
	}
	owned, err := cloneTypedStringPatchInput(requests)
	if err != nil {
		return TypedStringPatchResult{}, err
	}
	if hook := nativeStringPatchInputCopiedTestHook.Load(); hook != nil {
		(*hook)()
	}
	return c.patchTypedStringsBatch(owned, expectedSchemaHash, nil, nil)
}

func validateTypedStringPatchMeta(meta CollectionMeta, schema uint64) error {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.SchemaHash != schema || len(cfg.Columns) == 0 || uint64(len(cfg.Columns)) > uint64(^uint32(0)) || normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON || cfg.RetainedPayload != ColumnRetainedPayloadNonColumn || columnRetainedPayloadEffectiveEncoding(cfg) != ColumnRetainedPayloadEncodingJSON || cfg.ActiveManifest == nil || cfg.ActiveManifest.Format == columnSourceDirectoryFormatV2 || len(meta.TextIndexes) != 0 || len(meta.VectorIndexes) != 0 {
		return fmt.Errorf("%w: unsupported schema/retention/manifest/index", ErrTypedStringPatchInvalid)
	}
	for _, col := range cfg.Columns {
		owner, err := columnStoreColumnOwner(col)
		if err != nil {
			return err
		}
		if col.ValueType != ColumnStoreValueString || owner != TypedStorageOwnerRowAsset {
			return ErrTypedStringPatchInvalid
		}
	}
	if len(meta.Indexes) != 0 && !columnStoreTypedScalarIndexesSupported(meta) {
		return fmt.Errorf("%w: index source state is not supported", ErrTypedStringPatchInvalid)
	}
	for _, index := range meta.Indexes {
		if index.ValueType != IndexValueString || index.MultiKey || len(index.Components) != 0 || typedStringColumnIndex(cfg.Columns, index.Field) < 0 {
			return ErrTypedStringPatchInvalid
		}
	}
	return nil
}

func (c *Collection) patchTypedStringsBatch(requests []TypedStringPatch, schema uint64, replayPayload *commitlog.CollectionTypedStringsPayload, replay *backenddb.CommandWALIntent) (TypedStringPatchResult, error) {
	if c == nil || c.db == nil {
		return TypedStringPatchResult{}, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return TypedStringPatchResult{}, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	admissionState := c.lockCollectionCommandWALAdmission()
	admission := &admissionState
	defer admission.unlock()
	if err := validateTypedStringPatchMeta(c.MetaView(), schema); err != nil {
		return TypedStringPatchResult{}, err
	}
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return TypedStringPatchResult{}, err
	}
	if err := c.requireColumnStoreCommandWAL(c.MetaView(), replay); err != nil {
		return TypedStringPatchResult{}, err
	}
	if replay == nil {
		if err := c.db.CheckCommandWALPublishReady(); err != nil {
			return TypedStringPatchResult{}, err
		}
	}
	unlockMutation := c.lockMutation()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			unlockMutation.Unlock()
		}
	}()
	previousMutation, previousHeld := admission.bindMutation(&unlockMutation, &mutationLocked)
	defer admission.restoreMutation(previousMutation, previousHeld)
	if err := c.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
		return TypedStringPatchResult{}, err
	}
	// Prepare against an immutable cut outside serialized mutation ownership.
	// Publication retains the existing root preflight and zero-LSN retry rule.
	unlockMutation.Unlock()
	mutationLocked = false
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		plan, payload, result, err := c.buildTypedStringPatchPlan(requests, schema, replayPayload)
		if err != nil {
			return TypedStringPatchResult{}, err
		}
		if result.ModifiedCount == 0 {
			plan.close()
			if replay != nil {
				unlockMutation = c.lockMutation()
				mutationLocked = true
				return result, c.publishCommandWALNoop(replay, false)
			}
			return result, nil
		}
		intent := replay
		if intent == nil {
			raw, e := commitlog.EncodeCollectionTypedStringsPayload(payload)
			if e == nil {
				intent, e = c.db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionTypedStringsPatchV1, raw)
			}
			if e != nil {
				plan.close()
				return TypedStringPatchResult{}, e
			}
		}
		if hook := nativeStringPatchAfterPrepareTestHook.Load(); hook != nil {
			(*hook)()
		}
		unlockMutation = c.lockMutation()
		mutationLocked = true
		_, err = c.publishUpdateBatchPlanLocked(plan, intent, admission)
		plan.close()
		if mutationLocked {
			unlockMutation.Unlock()
			mutationLocked = false
		}
		if intent.AssignedLSN() == 0 && isRetriableCollectionMutationError(err) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return result, err
	}
	return TypedStringPatchResult{}, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) buildTypedStringPatchPlan(requests []TypedStringPatch, schema uint64, replay *commitlog.CollectionTypedStringsPayload) (*updateBatchPlan, commitlog.CollectionTypedStringsPayload, TypedStringPatchResult, error) {
	return c.buildTypedStringPatchPlanWithBoundaries(requests, schema, replay, nil)
}

func (c *Collection) buildTypedStringPatchPlanForGroup(requests []TypedStringPatch, schema uint64, group *nativeStringPatchGroup) (*updateBatchPlan, commitlog.CollectionTypedStringsPayload, TypedStringPatchResult, error) {
	return c.buildTypedStringPatchPlanWithBoundaries(requests, schema, nil, group)
}

func (c *Collection) buildTypedStringPatchPlanWithBoundaries(requests []TypedStringPatch, schema uint64, replay *commitlog.CollectionTypedStringsPayload, group *nativeStringPatchGroup) (_ *updateBatchPlan, payload commitlog.CollectionTypedStringsPayload, result TypedStringPatchResult, retErr error) {
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
	plan.results = make([]UpdateBatchResult, len(requests))
	if err := validateTypedStringPatchMeta(plan.meta, schema); err != nil {
		return nil, payload, result, err
	}
	cfg := *plan.meta.Options.ColumnStore
	payload.Collection, payload.SchemaHash, payload.SchemaCount = plan.meta.Name, cfg.SchemaHash, uint32(len(cfg.Columns))
	if replay != nil && (replay.Collection != payload.Collection || replay.SchemaHash != schema || replay.SchemaCount != payload.SchemaCount || len(replay.Documents) != len(requests)) {
		return nil, payload, result, errors.New("collections: string patch replay schema mismatch")
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
	if err := view.preparePointRowCredit(physical); err != nil {
		return nil, payload, result, err
	}
	var scratch columnPhysicalRowReaderScratch
	var indexArena indexEncodeArena
	var changed []preparedBatchUpdate
	// Validate every explicit edit, including unmatched IDs, before any WAL.
	for _, request := range requests {
		if request.ResidualMode == TypedStringResidualReplace {
			retainedProjection := &trustedFloat32Projection{columns: cfg.Columns, typedRows: map[string][]columnDeclaredValue{string(request.ID): nil}, retainedJSON: [][]byte{request.Residual}}
			if _, err := validateTrustedFloat32ProjectionRetainedJSONWithOwnership([][]byte{request.ID}, retainedProjection, false); err != nil {
				return nil, payload, result, err
			}
		}
		seen := make([]bool, len(cfg.Columns))
		for _, edit := range request.Edits {
			ordinal := -1
			for j, col := range cfg.Columns {
				if col.Name == edit.Column {
					ordinal = j
					break
				}
			}
			if ordinal < 0 || seen[ordinal] {
				return nil, payload, result, ErrTypedStringPatchInvalid
			}
			seen[ordinal] = true
		}
	}
	for pos, request := range requests {
		primary, _, err := collectionGetEntryAtCatalogRoot(plan.snap, catalog, collectionPrimaryRootName(plan.meta.Name), request.ID)
		if errors.Is(err, tree.ErrKeyNotFound) {
			if replay != nil {
				return nil, payload, result, errors.New("collections: string patch replay missing document")
			}
			continue
		}
		if err != nil {
			return nil, payload, result, err
		}
		if primary.Flags&node.FlagTombstone != 0 {
			if replay != nil {
				return nil, payload, result, errors.New("collections: string patch replay tombstoned document")
			}
			continue
		}
		if request.ResidualMode == TypedStringResidualPreserve && primary.Flags&node.FlagPointer == 0 && len(primary.Value) != 0 && !bytes.Equal(primary.Value, []byte("{}")) {
			return nil, payload, result, fmt.Errorf("%w: noncanonical inline residual", ErrTypedStringPatchInvalid)
		}
		result.MatchedCount++
		plan.results[pos].Matched = true
		locator, found, err := collectionGetAppendAtCatalogRoot(plan.snap, catalog, collectionColumnRowLocatorRootName(plan.meta.Name), request.ID, nil)
		if err != nil || !found {
			return nil, payload, result, errors.Join(err, errors.New("collections: string patch missing locator"))
		}
		latest, err := decodeColumnPrimaryRowLocatorBorrowedID(request.ID, locator)
		if err != nil {
			return nil, payload, result, err
		}
		if request.Expected != nil {
			if err := validateDocumentRowRefMatchesRowRef(*request.Expected, latest); err != nil {
				return nil, payload, result, errors.Join(ErrTypedStringPatchConflict, err)
			}
		}
		sources, err := columnFieldSourcesForLocator(request.ID, locator, cfg.Columns)
		if err != nil {
			return nil, payload, result, err
		}
		edits := make([]commitlog.CollectionTypedStringEdit, 0, len(request.Edits))
		seen := make([]bool, len(cfg.Columns))
		names := make([]string, 0, len(request.Edits))
		for _, edit := range request.Edits {
			j := -1
			for k, col := range cfg.Columns {
				if col.Name == edit.Column {
					j = k
					break
				}
			}
			if j < 0 || seen[j] {
				return nil, payload, result, ErrTypedStringPatchInvalid
			}
			seen[j] = true
			edits = append(edits, commitlog.CollectionTypedStringEdit{Column: uint32(j), Value: edit.Value})
			names = append(names, cfg.Columns[j].Name)
		}
		slices.SortFunc(edits, func(a, b commitlog.CollectionTypedStringEdit) int { return int(a.Column) - int(b.Column) })
		projection, err := view.pointRowScanProjection(physical, names)
		if err != nil {
			return nil, payload, result, err
		}
		var oldRow columnPhysicalVisibleRow
		if len(edits) != 0 {
			// The previous row has been compared into owned edits/state. Release
			// borrowed aliases before any cache eviction between requests.
			clear(scratch.Values[:cap(scratch.Values)])
			if err := view.preparePointRowEmission(cfg, ColumnAssetReadIntegrityVerify); err != nil {
				return nil, payload, result, err
			}
			oldRow, err = view.fetchDocumentPointRow(physical, latest, projection, &scratch, nil)
			if err != nil {
				return nil, payload, result, err
			}
		}
		values := make([]columnDeclaredValue, len(cfg.Columns))
		stored := make([]bool, len(cfg.Columns))
		changedEdits := edits[:0]
		for _, edit := range edits {
			j := int(edit.Column)
			old := oldRow.Values[projection.outputByColumn[j]]
			equal := old.Present && !old.Null && old.Type == ColumnStoreValueString
			if old.StringBytes != nil {
				equal = equal && bytes.Equal(old.StringBytes, []byte(edit.Value))
			} else {
				equal = equal && old.String == edit.Value
			}
			if equal {
				continue
			}
			values[j] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: edit.Value}
			stored[j] = true
			sources[j] = columnRowCoordinates{}
			changedEdits = append(changedEdits, edit)
		}
		residualChanged := false
		if request.ResidualMode == TypedStringResidualReplace {
			old, _, err := collectionGetAppendAtCatalogRoot(plan.snap, catalog, collectionPrimaryRootName(plan.meta.Name), request.ID, nil)
			if err != nil {
				return nil, payload, result, err
			}
			residualChanged = !bytes.Equal(old, request.Residual)
		}
		if len(changedEdits) == 0 && !residualChanged {
			if replay != nil {
				return nil, payload, result, errors.New("collections: string patch replay is not a change")
			}
			continue
		}
		oldMap, err := loadDeleteIndexState(plan.snap, catalog, request.ID, nil, runtimes, opts)
		if err != nil {
			return nil, payload, result, err
		}
		oldState := indexArena.appendState(len(runtimes))
		newState := indexArena.appendState(len(runtimes))
		for i, runtime := range runtimes {
			oldState[i] = oldMap[runtime.def.name]
			newState[i] = oldState[i]
			j := typedStringColumnIndex(cfg.Columns, runtime.def.field)
			if stored[j] {
				start := len(indexArena.buf)
				indexArena.buf = appendIndexStringComponent(indexArena.buf, []byte(values[j].String))
				newState[i] = indexArena.appendSingleValueRef(indexArena.buf[start:])
			}
		}
		update := preparedBatchUpdate{itemIndex: pos, documentID: request.ID, document: request.Residual, hasPrimaryDocument: residualChanged, oldState: oldState, newState: newState}
		for j := range runtimes {
			if orderedDocumentIndexRuntimeChanged(oldState, newState, j) {
				update.indexStateChanged = true
				if bit, ok := updateIndexChangedMaskBit(j); ok {
					update.changedIndexes |= bit
				}
			}
		}
		changed = append(changed, update)
		plan.nativeStringDocuments = append(plan.nativeStringDocuments, columnWriteDocument{ID: request.ID, fieldSources: sources, storedColumns: stored, declaredValues: values, declaredValuesReady: true})
		d := commitlog.CollectionTypedStringPatch{ID: request.ID, Generation: latest.Generation, PartID: latest.PartID, RowIndex: uint64(latest.RowIndex), AppliedCommandLSN: latest.AppliedCommandLSN, Edits: changedEdits, ReplaceResidual: residualChanged}
		if residualChanged {
			d.Residual = request.Residual
		}
		payload.Documents = append(payload.Documents, d)
		plan.results[pos].Modified = true
		result.ModifiedCount++
	}
	plan.stats.Items, plan.stats.Matched, plan.stats.Modified = len(requests), result.MatchedCount, result.ModifiedCount
	if group != nil {
		if err := group.checkUniqueBoundaries(plan, changed, runtimes); err != nil {
			return nil, payload, result, err
		}
	}
	if err := buildTypedMutationRootDeltas(plan, changed, runtimes, opts, func(u preparedBatchUpdate) bool { return u.hasPrimaryDocument }); err != nil {
		return nil, payload, result, err
	}
	return plan, payload, result, nil
}

func columnFieldSourcesForLocator(id, raw []byte, columns []ColumnStoreColumn) ([]columnRowCoordinates, error) {
	if len(raw) >= 4 && string(raw[:4]) == "CRL3" {
		return decodeColumnFieldSources(id, raw, len(columns))
	}
	latest, err := decodeColumnPrimaryRowLocatorBorrowedID(id, raw)
	if err != nil {
		return nil, err
	}
	sources := make([]columnRowCoordinates, len(columns))
	for i := range sources {
		sources[i] = columnCoordinates(latest)
	}
	if string(raw[:4]) == "CRL2" {
		preserved, err := decodeColumnRowCoordinates(id, raw[36:])
		if err != nil {
			return nil, err
		}
		for i, col := range columns {
			if !columnMetadataStoredColumn(col) {
				sources[i] = columnCoordinates(preserved)
			}
		}
	}
	return sources, nil
}
