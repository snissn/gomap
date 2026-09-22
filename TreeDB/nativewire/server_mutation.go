package nativewire

import (
	"bytes"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type insertBatchFastRequest struct {
	collectionName   string
	collection       *collections.Collection
	sections         []iwire.Section
	format           collections.DocumentFormat
	ids              [][]byte
	docs             [][]byte
	ack              AckPolicy
	includeResultIDs bool
	includeMeta      bool
}

func (s *Server) handleInsertBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	resultIDs, insertedCount, actualAck, catalogVersion, hasCatalogVersion, err := s.insertBatch(state, sections, true)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, resultIDs...)},
		ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion, responseMetaCount{key: "inserted_count", value: insertedCount}),
	}, nil
}

func (s *Server) handleInsertBatchBody(state *connState, sections []iwire.Section, dst []byte, includeResultIDs, includeMeta bool) ([]byte, error) {
	resultIDs, insertedCount, actualAck, catalogVersion, hasCatalogVersion, err := s.insertBatch(state, sections, includeResultIDs)
	if err != nil {
		return nil, err
	}
	return appendInsertBatchResponseBody(dst, resultIDs, insertedCount, actualAck, catalogVersion, hasCatalogVersion, includeResultIDs, includeMeta)
}

func (s *Server) handleInsertBatchFastBody(req insertBatchFastRequest, dst []byte) ([]byte, error) {
	if err := s.rejectClusterLocalMutation("insert_batch fast path"); err != nil {
		return nil, err
	}
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	if err := s.checkCatalogGuard(req.sections); err != nil {
		return nil, err
	}
	if result, combined := s.insertBatchCombiner.run(s, req); combined {
		if result.err != nil {
			return nil, result.err
		}
		return appendInsertBatchResponseBody(dst, nil, len(req.ids), result.actualAck, result.catalogVersion, result.hasCatalogVersion, false, req.includeMeta)
	}
	resultIDs, insertedCount, actualAck, catalogVersion, hasCatalogVersion, err := s.insertBatchDecoded(req.collection, req.format, req.ids, req.docs, req.ack, req.includeResultIDs)
	if err != nil {
		return nil, err
	}
	return appendInsertBatchResponseBody(dst, resultIDs, insertedCount, actualAck, catalogVersion, hasCatalogVersion, req.includeResultIDs, req.includeMeta)
}

func appendInsertBatchResponseBody(dst []byte, resultIDs [][]byte, insertedCount int, actualAck iwire.AckPolicy, catalogVersion uint64, hasCatalogVersion, includeResultIDs, includeMeta bool) ([]byte, error) {
	if !includeResultIDs {
		if !includeMeta {
			return dst[:0], nil
		}
		return appendAckMetaSectionVersion(dst, actualAck, catalogVersion, hasCatalogVersion, responseMetaCount{key: "inserted_count", value: insertedCount})
	}
	idsLen := iwire.ByteVectorEncodedLen(resultIDs)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionDocumentIDs, 0, idsLen)
	if err != nil {
		return nil, err
	}
	body = iwire.AppendByteVectorWithEncodedLen(body, idsLen, resultIDs...)
	if !includeMeta {
		return body, nil
	}
	return appendAckMetaSectionVersion(body, actualAck, catalogVersion, hasCatalogVersion, responseMetaCount{key: "inserted_count", value: insertedCount})
}

func (s *Server) insertBatch(state *connState, sections []iwire.Section, includeResultIDs bool) ([][]byte, int, iwire.AckPolicy, uint64, bool, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, 0, 0, 0, false, err
	}
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	if err := s.checkCatalogGuard(sections); err != nil {
		return nil, 0, 0, 0, false, err
	}
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	format, err := decodeDocumentFormatSection(sections)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	var ids, docs [][]byte
	if state != nil {
		ids, docs, err = decodeIDsAndDocumentsInto(state.idsScratch, state.docsScratch, sections, s.limits)
		state.idsScratch = ids
		state.docsScratch = docs
	} else {
		ids, docs, err = decodeIDsAndDocuments(sections, s.limits)
	}
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	docs, err = applyTemplateRecords(format, sections, docs, s.limits)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	if err := s.admitMutationAck(ack); err != nil {
		return nil, 0, 0, 0, false, err
	}
	return s.insertBatchDecoded(collection, format, ids, docs, ack, includeResultIDs)
}

func (s *Server) insertBatchDecoded(collection *collections.Collection, format collections.DocumentFormat, ids, docs [][]byte, ack AckPolicy, includeResultIDs bool) ([][]byte, int, iwire.AckPolicy, uint64, bool, error) {
	if err := s.rejectClusterLocalMutation("insert_batch"); err != nil {
		return nil, 0, 0, 0, false, err
	}
	var err error
	var resultIDs [][]byte
	if format == collections.DocumentFormatBSON {
		if err := validateBSONDocuments(docs); err != nil {
			return nil, 0, 0, 0, false, err
		}
		if includeResultIDs {
			resultIDs, err = collection.InsertBatchValidatedBSON(ids, docs)
		} else {
			err = collection.NativewireInsertBatchNoResultIDs(ids, docs, true)
		}
	} else {
		if includeResultIDs {
			resultIDs, err = collection.InsertBatch(ids, docs)
		} else {
			err = collection.NativewireInsertBatchNoResultIDs(ids, docs, false)
		}
	}
	if err != nil {
		return nil, 0, 0, 0, false, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	catalogVersion, hasCatalogVersion := s.mutationCatalogVersion()
	return resultIDs, len(ids), actualAck, catalogVersion, hasCatalogVersion, nil
}

func (s *Server) mutationCatalogVersion() (uint64, bool) {
	version, err := s.currentCatalogVersion()
	return version, err == nil
}

func (s *Server) decodeInsertBatchFastRequest(state *connState, sections []iwire.Section) (insertBatchFastRequest, bool, error) {
	if len(sections) == 0 || sections[0].ID != iwire.SectionCommandHeader {
		return insertBatchFastRequest{}, false, nil
	}
	header, err := iwire.DecodeCommandHeader(sections[0].Bytes)
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	if header.ID != iwire.CommandInsertBatch {
		return insertBatchFastRequest{}, false, nil
	}
	if header.Version != 1 {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrUnsupportedVersion, "unsupported command %d version %d", header.ID, header.Version)
	}
	if unsupported := header.Flags &^ (iwire.CommandFlagOmitResultIDs | iwire.CommandFlagOmitResponseMeta); unsupported != 0 {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrUnsupportedFeature, "unsupported insert_batch command flags 0x%x", unsupported)
	}

	var seen insertBatchFastSections
	var rawCollection, rawFormat, rawIDs, rawDocs, rawAck []byte
	for _, section := range sections {
		switch section.ID {
		case iwire.SectionCommandHeader,
			iwire.SectionDeadline,
			iwire.SectionTraceContext,
			iwire.SectionAckPolicy,
			iwire.SectionConsistencyPolicy,
			iwire.SectionIdempotencyKey,
			iwire.SectionChecksum,
			iwire.SectionCompression,
			iwire.SectionCollectionRef,
			iwire.SectionDocumentFormat,
			iwire.SectionDocumentIDs,
			iwire.SectionDocuments,
			iwire.SectionTemplateRecords,
			iwire.SectionExpectedCatalogVersion:
			if err := seen.mark(section.ID); err != nil {
				return insertBatchFastRequest{}, true, err
			}
		default:
			if section.Critical() {
				return insertBatchFastRequest{}, true, protocolError(iwire.ErrUnsupportedFeature, "unknown critical section %d", section.ID)
			}
			continue
		}

		switch section.ID {
		case iwire.SectionCollectionRef:
			rawCollection = section.Bytes
		case iwire.SectionDocumentFormat:
			rawFormat = section.Bytes
		case iwire.SectionDocumentIDs:
			rawIDs = section.Bytes
		case iwire.SectionDocuments:
			rawDocs = section.Bytes
		case iwire.SectionAckPolicy:
			rawAck = section.Bytes
		}
	}
	if !seen.has(iwire.SectionCommandHeader) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionCommandHeader)
	}
	if !seen.has(iwire.SectionCollectionRef) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionCollectionRef)
	}
	if !seen.has(iwire.SectionDocumentFormat) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionDocumentFormat)
	}
	if !seen.has(iwire.SectionDocumentIDs) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionDocumentIDs)
	}
	if !seen.has(iwire.SectionDocuments) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionDocuments)
	}
	if !seen.has(iwire.SectionIdempotencyKey) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionIdempotencyKey)
	}
	if !seen.has(iwire.SectionExpectedCatalogVersion) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "missing required section %d", iwire.SectionExpectedCatalogVersion)
	}
	collectionName, collection, err := s.openCollectionRawRef(state, rawCollection)
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	format, err := decodeDocumentFormatPayload(rawFormat)
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	var ids, docs [][]byte
	if state != nil {
		ids, err = decodeByteVectorBorrowedInto(state.idsScratch, rawIDs, s.limits)
		state.idsScratch = ids
	} else {
		ids, err = decodeByteVectorBorrowed(rawIDs, s.limits)
	}
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	if state != nil {
		docs, err = decodeByteVectorBorrowedInto(state.docsScratch, rawDocs, s.limits)
		state.docsScratch = docs
	} else {
		docs, err = decodeByteVectorBorrowed(rawDocs, s.limits)
	}
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	if len(ids) != len(docs) {
		return insertBatchFastRequest{}, true, protocolError(iwire.ErrInvalidCommand, "document_ids length %d does not match documents length %d", len(ids), len(docs))
	}
	if err := rejectDuplicateIDs(ids); err != nil {
		return insertBatchFastRequest{}, true, err
	}
	docs, err = applyTemplateRecords(format, sections, docs, s.limits)
	if err != nil {
		return insertBatchFastRequest{}, true, err
	}
	ack := s.defaultAckPolicy
	if seen.has(iwire.SectionAckPolicy) {
		ack, err = ackPolicyFromPayload(rawAck, s.defaultAckPolicy)
		if err != nil {
			return insertBatchFastRequest{}, true, err
		}
	}
	if err := s.admitMutationAck(ack); err != nil {
		return insertBatchFastRequest{}, true, err
	}
	return insertBatchFastRequest{
		collectionName:   collectionName,
		collection:       collection,
		sections:         sections,
		format:           format,
		ids:              ids,
		docs:             docs,
		ack:              ack,
		includeResultIDs: header.Flags&iwire.CommandFlagOmitResultIDs == 0,
		includeMeta:      header.Flags&iwire.CommandFlagOmitResponseMeta == 0,
	}, true, nil
}

type insertBatchFastSections struct {
	small    [128]bool
	overflow map[iwire.SectionID]struct{}
}

func (s *insertBatchFastSections) mark(id iwire.SectionID) error {
	if s.has(id) {
		return protocolError(iwire.ErrInvalidCommand, "duplicate singleton section %d", id)
	}
	if id < iwire.SectionID(len(s.small)) {
		s.small[id] = true
		return nil
	}
	if s.overflow == nil {
		s.overflow = make(map[iwire.SectionID]struct{}, 4)
	}
	s.overflow[id] = struct{}{}
	return nil
}

func (s *insertBatchFastSections) has(id iwire.SectionID) bool {
	if id < iwire.SectionID(len(s.small)) {
		return s.small[id]
	}
	_, ok := s.overflow[id]
	return ok
}

func (s *Server) handleReplaceBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	if err := s.rejectClusterLocalMutation("replace_batch"); err != nil {
		return nil, err
	}
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	if err := s.checkCatalogGuard(sections); err != nil {
		return nil, err
	}
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	format, err := decodeDocumentFormatSection(sections)
	if err != nil {
		return nil, err
	}
	if err := validateReplacementMode(sections); err != nil {
		return nil, err
	}
	var ids, docs [][]byte
	if state != nil {
		ids, docs, err = decodeIDsAndDocumentsInto(state.idsScratch, state.docsScratch, sections, s.limits)
		state.idsScratch = ids
		state.docsScratch = docs
	} else {
		ids, docs, err = decodeIDsAndDocuments(sections, s.limits)
	}
	if err != nil {
		return nil, err
	}
	docs, err = applyTemplateRecords(format, sections, docs, s.limits)
	if err != nil {
		return nil, err
	}
	if format == collections.DocumentFormatBSON {
		if err := validateBSONDocuments(docs); err != nil {
			return nil, err
		}
	}
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
		return nil, err
	}
	if err := s.admitMutationAck(ack); err != nil {
		return nil, err
	}
	matched, modified, err := replaceBatchDocuments(collection, ids, docs)
	if err != nil {
		return nil, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, err
	}
	catalogVersion, hasCatalogVersion := s.mutationCatalogVersion()
	return []iwire.Section{ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion,
		responseMetaCount{key: "matched_count", value: matched},
		responseMetaCount{key: "modified_count", value: modified},
	)}, nil
}

func replaceBatchDocuments(collection *collections.Collection, ids, docs [][]byte) (int, int, error) {
	if len(ids) == 1 && len(docs) == 1 {
		doc := docs[0]
		matched, modified, err := collection.Update(ids[0], func(current []byte) ([]byte, bool, error) {
			if current == nil {
				return nil, false, nil
			}
			if bytes.Equal(current, doc) {
				return current, false, nil
			}
			return doc, true, nil
		})
		if err != nil {
			return 0, 0, err
		}
		return boolCount(matched), boolCount(modified), nil
	}
	results, err := collection.UpdateBatch(updateBatchItems(ids, docs))
	if err != nil {
		return 0, 0, err
	}
	matched, modified := 0, 0
	for _, result := range results {
		if result.Matched {
			matched++
		}
		if result.Modified {
			modified++
		}
	}
	return matched, modified, nil
}

func (s *Server) handleUpdateBSONSet(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	if err := s.rejectClusterLocalMutation("update_bson_set"); err != nil {
		return nil, err
	}
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	if err := s.checkCatalogGuard(sections); err != nil {
		return nil, err
	}
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	if collection.Meta().Options.DocumentFormat != collections.DocumentFormatBSON {
		return nil, protocolError(iwire.ErrInvalidCommand, "update_bson_set requires a BSON collection")
	}
	var ids [][]byte
	if state != nil {
		ids, err = decodeIDVectorInto(state.idsScratch, sections, s.limits)
		state.idsScratch = ids
	} else {
		ids, err = decodeIDVector(sections, s.limits)
	}
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, protocolError(iwire.ErrInvalidCommand, "update_bson_set requires exactly one document id, got %d", len(ids))
	}
	var fields []collections.BSONSetField
	if state != nil {
		state.updateNames, state.updateValues, fields, err = decodeBSONSetFieldSectionsInto(state.updateNames, state.updateValues, state.updateFields, sections, s.limits)
		state.updateFields = fields
	} else {
		_, _, fields, err = decodeBSONSetFieldSectionsInto(nil, nil, nil, sections, s.limits)
	}
	if err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
		return nil, err
	}
	if err := s.admitMutationAck(ack); err != nil {
		return nil, err
	}
	matched, modified, err := collection.UpdateBSONSet(ids[0], fields)
	if err != nil {
		return nil, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, err
	}
	catalogVersion, hasCatalogVersion := s.mutationCatalogVersion()
	return []iwire.Section{ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion,
		responseMetaCount{key: "matched_count", value: boolCount(matched)},
		responseMetaCount{key: "modified_count", value: boolCount(modified)},
	)}, nil
}

func decodeBSONSetFieldSectionsInto(nameDst, valueDst [][]byte, fieldDst []collections.BSONSetField, sections []iwire.Section, limits iwire.Limits) ([][]byte, [][]byte, []collections.BSONSetField, error) {
	rawNames, err := metadataSection(sections, iwire.SectionUpdateFieldNames)
	if err != nil {
		return nameDst, valueDst, fieldDst, err
	}
	names, err := decodeByteVectorBorrowedInto(nameDst, rawNames, limits)
	if err != nil {
		return names, valueDst, fieldDst, err
	}
	rawValues, err := metadataSection(sections, iwire.SectionUpdateFieldValues)
	if err != nil {
		return names, valueDst, fieldDst, err
	}
	values, err := decodeByteVectorBorrowedInto(valueDst, rawValues, limits)
	if err != nil {
		return names, values, fieldDst, err
	}
	if len(names) == 0 {
		return names, values, fieldDst, protocolError(iwire.ErrInvalidCommand, "update_bson_set requires at least one field")
	}
	if len(names) != len(values) {
		return names, values, fieldDst, protocolError(iwire.ErrInvalidCommand, "update_field_names length %d does not match update_field_values length %d", len(names), len(values))
	}
	fields := fieldDst[:0]
	for i := range names {
		rawValue := values[i]
		if len(rawValue) == 0 {
			return names, values, fields, protocolError(iwire.ErrInvalidCommand, "update_field_values[%d] missing BSON type", i)
		}
		fields = append(fields, collections.BSONSetField{
			Key: string(names[i]),
			Value: bson.RawValue{
				Type:  bson.Type(rawValue[0]),
				Value: rawValue[1:],
			},
		})
	}
	if err := validateNativewireBSONSetFields(fields); err != nil {
		return names, values, fields, err
	}
	return names, values, fields, nil
}

func validateNativewireBSONSetFields(fields []collections.BSONSetField) error {
	if len(fields) == 0 {
		return protocolError(iwire.ErrInvalidCommand, "update_bson_set requires at least one field")
	}
	var seen map[string]struct{}
	if len(fields) > 1 {
		seen = make(map[string]struct{}, len(fields))
	}
	for i, field := range fields {
		if err := validateNativewireBSONSetFieldKey(field.Key); err != nil {
			return protocolError(iwire.ErrInvalidCommand, "update_field_names[%d] %q: %v", i, field.Key, err)
		}
		if err := field.Value.Validate(); err != nil {
			return protocolError(iwire.ErrInvalidCommand, "update_field_values[%d] invalid BSON raw value: %v", i, err)
		}
		if len(fields) == 1 {
			continue
		}
		if _, ok := seen[field.Key]; ok {
			return protocolError(iwire.ErrInvalidCommand, "duplicate update field %q", field.Key)
		}
		seen[field.Key] = struct{}{}
	}
	return nil
}

func validateNativewireBSONSetFieldKey(key string) error {
	if key == "" {
		return errInvalidBSONSetField("field name cannot be empty")
	}
	if key == "_id" {
		return errInvalidBSONSetField("cannot modify _id")
	}
	if strings.Contains(key, ".") {
		return errInvalidBSONSetField("currently supports top-level fields only")
	}
	if strings.HasPrefix(key, "$") {
		return errInvalidBSONSetField("field names cannot start with $")
	}
	if strings.Contains(key, "\x00") {
		return errInvalidBSONSetField("field names cannot contain NUL")
	}
	if !utf8.ValidString(key) {
		return errInvalidBSONSetField("field name must be valid UTF-8")
	}
	return nil
}

func errInvalidBSONSetField(message string) error {
	return errors.New(message)
}

func boolCount(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Server) handleDeleteBatch(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	if err := s.rejectClusterLocalMutation("delete_batch"); err != nil {
		return nil, err
	}
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	if err := s.checkCatalogGuard(sections); err != nil {
		return nil, err
	}
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	var ids [][]byte
	if state != nil {
		ids, err = decodeIDVectorInto(state.idsScratch, sections, s.limits)
		state.idsScratch = ids
	} else {
		ids, err = decodeIDVector(sections, s.limits)
	}
	if err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultAckPolicy)
	if err != nil {
		return nil, err
	}
	if err := s.admitMutationAck(ack); err != nil {
		return nil, err
	}
	deleted, err := collection.DeleteBatch(ids)
	if err != nil {
		return nil, metadataWrap(err)
	}
	actualAck, err := s.satisfyAck(collection, ack)
	if err != nil {
		return nil, err
	}
	catalogVersion, hasCatalogVersion := s.mutationCatalogVersion()
	return []iwire.Section{ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion, responseMetaCount{key: "deleted_count", value: deleted})}, nil
}

func (s *Server) handleFlushCollection(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultBarrierAck(iwire.AckFlushed))
	if err != nil {
		return nil, err
	}
	if err := s.admitBarrierAck(iwire.AckFlushed, ack); err != nil {
		return nil, err
	}
	if err := collection.Flush(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{s.ackMeta(iwire.AckFlushed)}, nil
}

func (s *Server) handleFlushAll(sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	ack, err := ackPolicyFromSections(sections, s.defaultBarrierAck(iwire.AckFlushed))
	if err != nil {
		return nil, err
	}
	if err := s.admitBarrierAck(iwire.AckFlushed, ack); err != nil {
		return nil, err
	}
	if err := s.collections.FlushAll(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{s.ackMeta(iwire.AckFlushed)}, nil
}

func (s *Server) handleCheckpoint(sections []iwire.Section) ([]iwire.Section, error) {
	if s.backend == nil {
		return nil, protocolError(iwire.ErrDurabilityUnavailable, "checkpoint requires a backend DB")
	}
	actualAck := iwire.AckSynced
	ack, err := ackPolicyFromSections(sections, s.defaultBarrierAck(actualAck))
	if err != nil {
		return nil, err
	}
	if err := s.admitBarrierAck(actualAck, ack); err != nil {
		return nil, err
	}
	if s.collections != nil {
		if err := s.collections.FlushAll(); err != nil {
			return nil, metadataWrap(err)
		}
	}
	if err := s.backend.Checkpoint(); err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{s.ackMeta(actualAck)}, nil
}

func (s *Server) admitBarrierAck(actual, requested iwire.AckPolicy) error {
	switch requested {
	case 0, iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced:
	case iwire.AckRaftCommitted:
		return protocolError(iwire.ErrDurabilityUnavailable, "raft_committed ack is unavailable in single-node nativewire R1")
	default:
		return protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", requested)
	}
	if requested == 0 || requested <= actual {
		return nil
	}
	return protocolError(iwire.ErrDurabilityUnavailable, "requested ack policy %d cannot be satisfied by barrier ack policy %d", requested, actual)
}

func (s *Server) defaultBarrierAck(minimum iwire.AckPolicy) iwire.AckPolicy {
	if s != nil && s.defaultAckPolicy > minimum {
		return s.defaultAckPolicy
	}
	return minimum
}

func (s *Server) admitMutationAck(requested iwire.AckPolicy) error {
	switch requested {
	case 0, iwire.AckVisible, iwire.AckFlushed:
		return nil
	case iwire.AckSynced:
		return s.admitSyncedAck()
	case iwire.AckRaftCommitted:
		return protocolError(iwire.ErrDurabilityUnavailable, "raft_committed ack is unavailable in single-node nativewire R1")
	default:
		return protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", requested)
	}
}

func validateBSONDocuments(docs [][]byte) error {
	for i, doc := range docs {
		if err := bson.Raw(doc).Validate(); err != nil {
			return protocolError(iwire.ErrInvalidCommand, "invalid BSON document at index %d: %v", i, err)
		}
	}
	return nil
}

func (s *Server) satisfyAck(collection interface{ Flush() error }, requested iwire.AckPolicy) (iwire.AckPolicy, error) {
	switch requested {
	case 0, iwire.AckVisible:
		return iwire.AckVisible, nil
	case iwire.AckFlushed:
		if collection != nil {
			if err := collection.Flush(); err != nil {
				return 0, metadataWrap(err)
			}
		}
		return iwire.AckFlushed, nil
	case iwire.AckSynced:
		if collection != nil {
			if err := collection.Flush(); err != nil {
				return 0, metadataWrap(err)
			}
		}
		if err := s.admitSyncedAck(); err != nil {
			return 0, err
		}
		if err := s.backend.Checkpoint(); err != nil {
			return 0, metadataWrap(err)
		}
		return iwire.AckSynced, nil
	case iwire.AckRaftCommitted:
		return 0, protocolError(iwire.ErrDurabilityUnavailable, "raft_committed ack is unavailable in single-node nativewire R1")
	default:
		return 0, protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", requested)
	}
}

func (s *Server) admitSyncedAck() error {
	if s.backend == nil {
		return protocolError(iwire.ErrDurabilityUnavailable, "synced ack requires a backend DB")
	}
	return nil
}
