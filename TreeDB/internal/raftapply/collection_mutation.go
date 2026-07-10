package raftapply

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const deterministicCollectionRefTagName = 1

type collectionMutationV1 struct {
	command          nativewire.CommandID
	collection       string
	documentFormat   collections.DocumentFormat
	trustedValidBSON bool
	ids              [][]byte
	documents        [][]byte
	bsonSetItems     []collections.BSONSetUpdateBatchItem
	bsonSetMatched   int
	frameDocuments   []commitlog.CollectionDocument
}

func (h *Harness) applyCollectionMutationV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (raftentry.ApplyResultV1, error) {
	expectedCatalogVersion, err := decodeExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if err := checkCatalogVersionGuardV1(meta, expectedCatalogVersion); err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	mutation, err := lowerCollectionMutationV1(entry, h.opts.DecodeLimits)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	collection, err := h.preflightCollectionMutationV1(&mutation)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	frame, err := mutation.loweredFrame()
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	var handle commandwalapply.Handle
	handleAppended := false
	handleFinalized := false
	defer func() {
		if handleAppended && !handleFinalized {
			h.walApply.Abort(h.db, handle)
		}
	}()
	handle, _, err = h.walApply.Append(h.db, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL})
	if err != nil {
		return raftentry.ApplyResultV1{}, codeCommandWALApplyError(err)
	}
	handleAppended = true
	if err := h.injectFault(FaultAfterLocalWALAppendBeforeVisibleV1, meta.EntryID, entry.Digest); err != nil {
		return commandWALPostAppendRecoveryRequired(entry, err)
	}
	intent := handle.CommandWALIntent()
	if intent == nil || handle.LSN() == 0 {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: command WAL append did not return a usable intent")
	}
	finalizeHandle := func() error {
		if _, err := h.walApply.Finalize(h.db, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL}); err != nil {
			return codeCommandWALApplyError(err)
		}
		handleFinalized = true
		return nil
	}

	var affected int64
	var matched int64
	switch mutation.command {
	case nativewire.CommandInsertBatch:
		if len(mutation.documents) == 0 {
			if err := finalizeHandle(); err != nil {
				return commandWALFinalizeRecoveryRequired(entry, err)
			}
			break
		}
		resultIDs, err := collection.InsertBatchWithCommandWALIntent(mutation.ids, mutation.documents, mutation.trustedValidBSON, intent)
		if err != nil {
			return h.collectionMutationApplyError(entry, handle, err)
		}
		affected = int64(len(resultIDs))
	case nativewire.CommandReplaceBatch:
		matchedCount, modified, err := collection.ReplaceBatchWithCommandWALIntent(mutation.ids, mutation.documents, intent)
		if err != nil {
			return h.collectionMutationApplyError(entry, handle, err)
		}
		matched = int64(matchedCount)
		affected = int64(modified)
	case nativewire.CommandUpdateBSONSet:
		results, err := collection.UpdateBSONSetBatchWithCommandWALIntent(mutation.bsonSetItems, mutation.frameDocuments, intent)
		if err != nil {
			return h.collectionMutationApplyError(entry, handle, err)
		}
		matched = int64(mutation.bsonSetMatched)
		affected = int64(countModifiedUpdateBatchResultsV1(results))
	case nativewire.CommandDeleteBatch:
		deleted, err := collection.DeleteBatchWithCommandWALIntent(mutation.ids, intent)
		if err != nil {
			return h.collectionMutationApplyError(entry, handle, err)
		}
		affected = int64(deleted)
	default:
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorUnsupportedCommandV1, "raftapply: unsupported mutation command %d", mutation.command)
	}
	if !handleFinalized {
		if err := finalizeHandle(); err != nil {
			return commandWALFinalizeRecoveryRequired(entry, err)
		}
	}

	logical, err := h.logicalDigestV1(LogicalDigestOptionsV1{
		ScopeRule:     meta.ScopeRule,
		DatabaseScope: meta.DatabaseScope,
		CatalogScope:  meta.CatalogScope,
	})
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	return raftentry.ApplyResultV1{
		Status:                 raftentry.ApplyStatusApplied,
		CommandDigest:          entry.Digest,
		DeterministicErrorCode: raftentry.ErrorNoneV1,
		AffectedCount:          affected,
		MatchedCount:           matched,
		ResultDigest:           raftentry.CommandDigestV1(logical),
	}, nil
}

func commandWALFinalizeRecoveryRequired(entry raftentry.CommandEntryV1, err error) (raftentry.ApplyResultV1, error) {
	code, _ := ErrorCodeOf(err)
	return recoveryRequired(entry.Digest, code, err)
}

func commandWALPostAppendRecoveryRequired(entry raftentry.CommandEntryV1, err error) (raftentry.ApplyResultV1, error) {
	code, _ := ErrorCodeOf(err)
	return recoveryRequired(entry.Digest, code, err)
}

func (h *Harness) collectionMutationApplyError(entry raftentry.CommandEntryV1, handle commandwalapply.Handle, err error) (raftentry.ApplyResultV1, error) {
	if h.commandWALHandleCovered(handle) && collectionMutationRequiresRecovery(err) {
		return recoveryRequired(entry.Digest, raftentry.ErrorUnsafeDurabilityModeV1, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "%w", err))
	}
	return raftentry.ApplyResultV1{}, codeCollectionApplyError(err)
}

func collectionMutationRequiresRecovery(err error) bool {
	if code, ok := ErrorCodeOf(err); ok && code == raftentry.ErrorUnsafeDurabilityModeV1 {
		return true
	}
	return errors.Is(err, collections.ErrCommitAmbiguous) ||
		errors.Is(err, backenddb.ErrRecoveryRequired) ||
		errors.Is(err, backenddb.ErrCommandWALRejected) ||
		errors.Is(err, backenddb.ErrCommandWALAppliedLSNNonContig) ||
		errors.Is(err, backenddb.ErrCommandWALAppliedLSNRegression) ||
		errors.Is(err, backenddb.ErrCommandWALSplitPublish)
}

func (h *Harness) commandWALHandleCovered(handle commandwalapply.Handle) bool {
	if h == nil || h.db == nil || handle.LSN() == 0 {
		return false
	}
	state, ok := h.db.StateToken()
	return ok && state.AppliedCommandLSN >= handle.LSN()
}

func lowerCollectionMutationV1(entry raftentry.CommandEntryV1, limits nativewire.Limits) (collectionMutationV1, error) {
	collectionName, err := lowerCollectionNameV1(entry)
	if err != nil {
		return collectionMutationV1{}, err
	}
	mutation := collectionMutationV1{
		command:    entry.Target.CommandID,
		collection: collectionName,
	}
	switch entry.Target.CommandID {
	case nativewire.CommandInsertBatch, nativewire.CommandReplaceBatch:
		format, err := lowerDocumentFormatV1(entry)
		if err != nil {
			return collectionMutationV1{}, err
		}
		if format == collections.DocumentFormatTemplateV1 {
			return collectionMutationV1{}, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: template-v1 mutation lowering is not accepted by R3a v1")
		}
		ids, err := lowerByteVectorSectionV1(entry, nativewire.SectionDocumentIDs, limits, "document_ids")
		if err != nil {
			return collectionMutationV1{}, err
		}
		documents, err := lowerByteVectorSectionV1(entry, nativewire.SectionDocuments, limits, "documents")
		if err != nil {
			return collectionMutationV1{}, err
		}
		if len(ids) != len(documents) {
			return collectionMutationV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: document_ids length %d does not match documents length %d", len(ids), len(documents))
		}
		if err := validateMutationDocumentIDsV1(ids); err != nil {
			return collectionMutationV1{}, err
		}
		if entry.Target.CommandID == nativewire.CommandReplaceBatch {
			if err := requireExistingReplacementModeV1(entry); err != nil {
				return collectionMutationV1{}, err
			}
		}
		mutation.documentFormat = format
		mutation.trustedValidBSON = format == collections.DocumentFormatBSON
		mutation.ids = ids
		mutation.documents = documents
	case nativewire.CommandUpdateBSONSet:
		ids, err := lowerByteVectorSectionV1(entry, nativewire.SectionDocumentIDs, limits, "document_ids")
		if err != nil {
			return collectionMutationV1{}, err
		}
		if len(ids) != 1 {
			return collectionMutationV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: update_bson_set requires exactly one document_id, got %d", len(ids))
		}
		if err := validateMutationDocumentIDsV1(ids); err != nil {
			return collectionMutationV1{}, err
		}
		fieldNames, err := lowerByteVectorSectionV1(entry, nativewire.SectionUpdateFieldNames, limits, "update_field_names")
		if err != nil {
			return collectionMutationV1{}, err
		}
		fieldValues, err := lowerByteVectorSectionV1(entry, nativewire.SectionUpdateFieldValues, limits, "update_field_values")
		if err != nil {
			return collectionMutationV1{}, err
		}
		fields, err := lowerBSONSetFieldsV1(fieldNames, fieldValues)
		if err != nil {
			return collectionMutationV1{}, err
		}
		mutation.documentFormat = collections.DocumentFormatBSON
		mutation.ids = ids
		mutation.bsonSetItems = []collections.BSONSetUpdateBatchItem{{
			DocumentID: bytes.Clone(ids[0]),
			Fields:     fields,
		}}
	case nativewire.CommandDeleteBatch:
		ids, err := lowerByteVectorSectionV1(entry, nativewire.SectionDocumentIDs, limits, "document_ids")
		if err != nil {
			return collectionMutationV1{}, err
		}
		if err := validateMutationDocumentIDsV1(ids); err != nil {
			return collectionMutationV1{}, err
		}
		mutation.ids = ids
	default:
		return collectionMutationV1{}, codedError(raftentry.ErrorUnsupportedCommandV1, "raftapply: unsupported mutation command %d", entry.Target.CommandID)
	}
	return mutation, nil
}

func (h *Harness) preflightCollectionMutationV1(mutation *collectionMutationV1) (*collections.Collection, error) {
	return h.preflightCollectionMutationWithOptionsV1(mutation, true)
}

func (h *Harness) preflightCollectionMutationOnlyV1(mutation *collectionMutationV1) error {
	_, err := h.preflightCollectionMutationWithOptionsV1(mutation, false)
	return err
}

func (h *Harness) preflightCollectionMutationWithOptionsV1(mutation *collectionMutationV1, prepareFrameDocuments bool) (*collections.Collection, error) {
	if h == nil || h.db == nil {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil DB cannot preflight collection mutation")
	}
	if mutation == nil {
		return nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: nil collection mutation")
	}
	manager := h.replayCollectionManager()
	if manager == nil {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil collection manager cannot preflight collection mutation")
	}
	collection, err := manager.OpenCollection(mutation.collection)
	if err != nil {
		return nil, codeCollectionApplyError(err)
	}
	if mutation.command == nativewire.CommandInsertBatch || mutation.command == nativewire.CommandReplaceBatch || mutation.command == nativewire.CommandUpdateBSONSet {
		format, err := normalizeApplyDocumentFormat(collection.Meta().Options.DocumentFormat)
		if err != nil {
			return nil, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: collection %q has unsupported document format: %v", mutation.collection, err)
		}
		if format != mutation.documentFormat {
			return nil, codedError(raftentry.ErrorRejectedConflictV1, "raftapply: deterministic document format %q does not match collection %q format %q", mutation.documentFormat, mutation.collection, format)
		}
		if mutation.command != nativewire.CommandUpdateBSONSet {
			if err := validateMutationDocumentsV1(format, mutation.documents); err != nil {
				return nil, err
			}
		}
	}
	switch mutation.command {
	case nativewire.CommandInsertBatch:
		if err := collection.PreflightCommandWALMutation(collections.ColumnPublishOperationInsert); err != nil {
			return nil, codeCollectionApplyError(err)
		}
		if prepareFrameDocuments {
			mutation.frameDocuments = collectionDocumentsFromMutationV1(mutation.ids, mutation.documents)
		}
		if err := collection.PreflightInsertBatchConflicts(mutation.ids, mutation.documents, mutation.trustedValidBSON); err != nil {
			return nil, codeCollectionApplyError(err)
		}
	case nativewire.CommandReplaceBatch:
		if err := collection.PreflightCommandWALMutation(collections.ColumnPublishOperationUpdate); err != nil {
			return nil, codeCollectionApplyError(err)
		}
		var changed []commitlog.CollectionDocument
		if prepareFrameDocuments {
			changed = make([]commitlog.CollectionDocument, 0, len(mutation.ids))
		}
		for i, id := range mutation.ids {
			current, err := collection.Get(id)
			if err != nil {
				return nil, codeCollectionApplyError(err)
			}
			if current == nil {
				continue
			}
			if mutation.documentFormat == collections.DocumentFormatBSON {
				if err := validateBSONReplacementPreservesIDV1(current, mutation.documents[i]); err != nil {
					return nil, err
				}
			}
			if prepareFrameDocuments && !bytes.Equal(current, mutation.documents[i]) {
				changed = append(changed, commitlog.CollectionDocument{
					ID:       mutation.ids[i],
					Document: mutation.documents[i],
				})
			}
		}
		mutation.frameDocuments = changed
		if err := collection.PreflightReplaceBatchConflicts(mutation.ids, mutation.documents); err != nil {
			return nil, codeCollectionApplyError(err)
		}
	case nativewire.CommandUpdateBSONSet:
		if err := collection.PreflightCommandWALMutation(collections.ColumnPublishOperationUpdate); err != nil {
			return nil, codeCollectionApplyError(err)
		}
		results, docs, err := collection.PrepareBSONSetUpdateBatchCommandWAL(mutation.bsonSetItems)
		if err != nil {
			return nil, codeCollectionApplyError(err)
		}
		// No-index preflight can skip retaining command-WAL frame documents,
		// but it must still run the BSON-set prepare validation above.
		if !prepareFrameDocuments && collectionHasNoSecondaryIndexesV1(collection) {
			return collection, nil
		}
		mutation.bsonSetMatched = countMatchedUpdateBatchResultsV1(results)
		mutation.frameDocuments = docs
	case nativewire.CommandDeleteBatch:
		if err := collection.PreflightCommandWALMutation(collections.ColumnPublishOperationDelete); err != nil {
			return nil, codeCollectionApplyError(err)
		}
	default:
		return nil, codedError(raftentry.ErrorUnsupportedCommandV1, "raftapply: unsupported mutation command %d", mutation.command)
	}
	return collection, nil
}

func collectionHasNoSecondaryIndexesV1(collection *collections.Collection) bool {
	meta := collection.MetaView()
	return len(meta.Indexes) == 0 && len(meta.VectorIndexes) == 0 && len(meta.TextIndexes) == 0
}

func (m collectionMutationV1) loweredFrame() (commandwalapply.LoweredFrame, error) {
	switch m.command {
	case nativewire.CommandInsertBatch:
		payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(m.collection, m.frameDocuments)
		if err != nil {
			return commandwalapply.LoweredFrame{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode collection insert payload: %v", err)
		}
		frame, err := commandwalapply.CollectionInsertBatchByIDFrame(payload)
		if err != nil {
			return commandwalapply.LoweredFrame{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: lower collection insert: %v", err)
		}
		return frame, nil
	case nativewire.CommandReplaceBatch, nativewire.CommandUpdateBSONSet:
		payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload(m.collection, m.frameDocuments)
		if err != nil {
			return commandwalapply.LoweredFrame{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode collection update payload: %v", err)
		}
		frame, err := commandwalapply.CollectionUpdateBatchByIDFrame(payload)
		if err != nil {
			return commandwalapply.LoweredFrame{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: lower collection update: %v", err)
		}
		return frame, nil
	case nativewire.CommandDeleteBatch:
		payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload(m.collection, m.ids)
		if err != nil {
			return commandwalapply.LoweredFrame{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode collection delete payload: %v", err)
		}
		frame, err := commandwalapply.CollectionDeleteBatchByIDFrame(payload)
		if err != nil {
			return commandwalapply.LoweredFrame{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: lower collection delete: %v", err)
		}
		return frame, nil
	default:
		return commandwalapply.LoweredFrame{}, codedError(raftentry.ErrorUnsupportedCommandV1, "raftapply: unsupported mutation command %d", m.command)
	}
}

func lowerCollectionNameV1(entry raftentry.CommandEntryV1) (string, error) {
	raw, err := requiredDeterministicSectionV1(entry, nativewire.SectionCollectionRef, "collection_ref")
	if err != nil {
		return "", err
	}
	if len(raw) == 0 || raw[0] != deterministicCollectionRefTagName {
		return "", codedError(raftentry.ErrorMalformedEntryV1, "raftapply: collection_ref must use deterministic collection name")
	}
	name := string(raw[1:])
	if err := collections.ValidateCollectionName(name); err != nil {
		return "", codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid collection_ref: %v", err)
	}
	return name, nil
}

func lowerBSONSetFieldsV1(names, values [][]byte) ([]collections.BSONSetField, error) {
	if len(names) == 0 {
		return nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: update_bson_set requires at least one field")
	}
	if len(names) != len(values) {
		return nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: update_field_names length %d does not match update_field_values length %d", len(names), len(values))
	}
	fields := make([]collections.BSONSetField, len(names))
	seen := make(map[string]struct{}, len(names))
	for i := range names {
		key := string(names[i])
		if err := validateBSONSetFieldKeyV1(key); err != nil {
			return nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: BSON $set field %q key: %v", key, err)
		}
		if _, ok := seen[key]; ok {
			return nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: duplicate BSON $set field %q", key)
		}
		seen[key] = struct{}{}
		value, err := lowerBSONSetRawValueV1(values[i])
		if err != nil {
			return nil, err
		}
		fields[i] = collections.BSONSetField{
			Key:   key,
			Value: value,
		}
	}
	return fields, nil
}

func validateBSONSetFieldKeyV1(key string) error {
	if key == "" {
		return errors.New("field name cannot be empty")
	}
	if !utf8.ValidString(key) {
		return errors.New("field name must be valid UTF-8")
	}
	if key == "_id" {
		return errors.New("cannot modify _id")
	}
	if strings.Contains(key, ".") {
		return errors.New("currently supports top-level fields only")
	}
	if strings.HasPrefix(key, "$") {
		return errors.New("field names cannot start with $")
	}
	if strings.Contains(key, "\x00") {
		return errors.New("field names cannot contain NUL")
	}
	return nil
}

func lowerBSONSetRawValueV1(raw []byte) (bson.RawValue, error) {
	if len(raw) == 0 {
		return bson.RawValue{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: BSON $set value missing type byte")
	}
	value := bson.RawValue{
		Type:  bson.Type(raw[0]),
		Value: bytes.Clone(raw[1:]),
	}
	_, rem, ok := bsoncore.ReadValue(value.Value, bsoncore.Type(value.Type))
	if !ok || len(rem) != 0 {
		return bson.RawValue{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid BSON $set raw value")
	}
	return value, nil
}

func countModifiedUpdateBatchResultsV1(results []collections.UpdateBatchResult) int {
	modified := 0
	for _, result := range results {
		if result.Modified {
			modified++
		}
	}
	return modified
}

func countMatchedUpdateBatchResultsV1(results []collections.UpdateBatchResult) int {
	matched := 0
	for _, result := range results {
		if result.Matched {
			matched++
		}
	}
	return matched
}

func lowerDocumentFormatV1(entry raftentry.CommandEntryV1) (collections.DocumentFormat, error) {
	raw, err := requiredDeterministicSectionV1(entry, nativewire.SectionDocumentFormat, "document_format")
	if err != nil {
		return collections.DocumentFormatDefault, err
	}
	value, n := binary.Uvarint(raw)
	if n <= 0 {
		return collections.DocumentFormatDefault, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid document_format")
	}
	if n != len(raw) {
		return collections.DocumentFormatDefault, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: document_format has %d trailing bytes", len(raw)-n)
	}
	switch nativewire.DocumentFormat(value) {
	case nativewire.DocumentFormatDefault, nativewire.DocumentFormatJSON:
		return collections.DocumentFormatJSON, nil
	case nativewire.DocumentFormatBSON:
		return collections.DocumentFormatBSON, nil
	case nativewire.DocumentFormatTemplateV1:
		return collections.DocumentFormatTemplateV1, nil
	default:
		return collections.DocumentFormatDefault, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: unsupported document_format %d", value)
	}
}

func requireExistingReplacementModeV1(entry raftentry.CommandEntryV1) error {
	raw, err := requiredDeterministicSectionV1(entry, nativewire.SectionReplacementMode, "replacement_mode")
	if err != nil {
		return err
	}
	value, n := binary.Uvarint(raw)
	if n <= 0 {
		return codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid replacement_mode")
	}
	if n != len(raw) {
		return codedError(raftentry.ErrorMalformedEntryV1, "raftapply: replacement_mode has %d trailing bytes", len(raw)-n)
	}
	if value != 1 {
		return codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: unsupported replacement_mode %d", value)
	}
	return nil
}

func lowerByteVectorSectionV1(entry raftentry.CommandEntryV1, id nativewire.SectionID, limits nativewire.Limits, name string) ([][]byte, error) {
	raw, err := requiredDeterministicSectionV1(entry, id, name)
	if err != nil {
		return nil, err
	}
	items, err := nativewire.DecodeByteVectorItems(raw, limits)
	if err != nil {
		return nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: decode %s: %v", name, err)
	}
	out := make([][]byte, len(items))
	for i := range items {
		if len(items[i]) == 0 {
			out[i] = []byte{}
		} else {
			out[i] = bytes.Clone(items[i])
		}
	}
	return out, nil
}

func requiredDeterministicSectionV1(entry raftentry.CommandEntryV1, id nativewire.SectionID, name string) ([]byte, error) {
	for _, section := range entry.Decoded.Sections {
		if section.ID == id {
			return section.Bytes, nil
		}
	}
	return nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: missing deterministic section %s", name)
}

func normalizeApplyDocumentFormat(format collections.DocumentFormat) (collections.DocumentFormat, error) {
	switch format {
	case collections.DocumentFormatDefault, collections.DocumentFormatJSON:
		return collections.DocumentFormatJSON, nil
	case collections.DocumentFormatBSON:
		return collections.DocumentFormatBSON, nil
	case collections.DocumentFormatTemplateV1:
		return collections.DocumentFormatTemplateV1, nil
	default:
		return collections.DocumentFormatDefault, fmt.Errorf("unsupported document format %q", format)
	}
}

func validateMutationDocumentsV1(format collections.DocumentFormat, documents [][]byte) error {
	switch format {
	case collections.DocumentFormatJSON:
		for i, doc := range documents {
			if !json.Valid(doc) {
				return codedError(raftentry.ErrorMalformedEntryV1, "raftapply: JSON document at index %d is invalid JSON", i)
			}
			if !mutationJSONDocumentIsObject(doc) {
				return codedError(raftentry.ErrorMalformedEntryV1, "raftapply: JSON document at index %d must be an object", i)
			}
		}
	case collections.DocumentFormatBSON:
		for i, doc := range documents {
			if err := bson.Raw(doc).Validate(); err != nil {
				return codedError(raftentry.ErrorMalformedEntryV1, "raftapply: BSON document at index %d: %v", i, err)
			}
		}
	}
	return nil
}

func mutationJSONDocumentIsObject(doc []byte) bool {
	doc = bytes.TrimSpace(doc)
	return len(doc) > 0 && doc[0] == '{'
}

func validateMutationDocumentIDsV1(ids [][]byte) error {
	seen := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		if len(id) == 0 {
			return codedError(raftentry.ErrorMalformedEntryV1, "raftapply: document_id at index %d cannot be empty", i)
		}
		key := string(id)
		if _, ok := seen[key]; ok {
			return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: duplicate document_id %q at index %d", string(id), i)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateBSONReplacementPreservesIDV1(current, replacement []byte) error {
	currentRaw := bson.Raw(current)
	if err := currentRaw.Validate(); err != nil {
		return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: current BSON document: %v", err)
	}
	replacementRaw := bson.Raw(replacement)
	if err := replacementRaw.Validate(); err != nil {
		return codedError(raftentry.ErrorMalformedEntryV1, "raftapply: replacement BSON document: %v", err)
	}
	currentID := currentRaw.Lookup("_id")
	replacementID := replacementRaw.Lookup("_id")
	currentMissing := currentID.IsZero()
	replacementMissing := replacementID.IsZero()
	if currentMissing && replacementMissing {
		return nil
	}
	if currentMissing || replacementMissing || currentID.Type != replacementID.Type || !bytes.Equal(currentID.Value, replacementID.Value) {
		return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: replacement BSON document would mutate _id")
	}
	return nil
}

func collectionDocumentsFromMutationV1(ids, documents [][]byte) []commitlog.CollectionDocument {
	docs := make([]commitlog.CollectionDocument, len(ids))
	for i := range ids {
		docs[i] = commitlog.CollectionDocument{
			ID:       ids[i],
			Document: documents[i],
		}
	}
	return docs
}
