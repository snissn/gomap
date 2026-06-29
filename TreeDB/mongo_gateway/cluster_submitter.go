package mongogateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	treenativewire "github.com/snissn/gomap/TreeDB/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	clusterCollectionRefNameTag = byte(1)
	clusterSyntheticCatalogV1   = uint64(0)
)

func (s *Server) clusterSubmitterConfigured() bool {
	return s != nil && s.ClusterSubmitter != nil
}

func (s *Server) clusterInsertResponse(command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if err := rejectClusterWriteConcern(command); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "insert")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	name, err := gatewayCollectionName(db, collection)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	documents, err := commandDocuments(command, sequences, "documents")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	format := s.clusterCollectionFormat(name)
	ids, stored, err := prepareInsertDocuments(documents, format)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	inserted, err := s.submitClusterInsert(name, format, ids, stored)
	if err != nil {
		return mongoClusterMutationCommandError(err)
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: inserted},
	})
}

func (s *Server) clusterUpdateResponse(command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if err := rejectClusterWriteConcern(command); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "update")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	name, err := gatewayCollectionName(db, collection)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	updates, err := commandDocuments(command, sequences, "updates")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	parsed := make([]mongoUpdateItem, 0, len(updates))
	for i, update := range updates {
		item, err := parseMongoUpdateItem(i, update)
		if err != nil {
			return mongoUpdateParseCommandError(err)
		}
		if !item.bsonSetFieldsOK {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: cluster Mongo gateway currently supports only top-level BSON $set updateOne by _id", i))
		}
		parsed = append(parsed, item)
	}
	var matched, modified int32
	for _, item := range parsed {
		matchedOne, modifiedOne, err := s.submitClusterUpdateBSONSet(name, item)
		if err != nil {
			return mongoClusterMutationCommandError(mongoUpdateErrorWithIndex(item.index, err))
		}
		matched += matchedOne
		modified += modifiedOne
	}
	return marshalUpdateResponse(matched, modified)
}

func (s *Server) clusterDeleteResponse(command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if err := rejectClusterWriteConcern(command); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "delete")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	name, err := gatewayCollectionName(db, collection)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	deletes, err := commandDocuments(command, sequences, "deletes")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	ids := make([][]byte, 0, len(deletes))
	for i, deleteItem := range deletes {
		filter, err := requiredDocumentField(deleteItem, "q")
		if err != nil {
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("deletes[%d]: %v", i, err))
		}
		id, err := idEqualityFilterValue(filter, "delete")
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("deletes[%d]: %v", i, err))
		}
		if limit, err := optionalInt32Field(deleteItem, "limit"); err != nil {
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("deletes[%d]: %v", i, err))
		} else if limit != 0 && limit != 1 {
			return commandError(commandCodeBadValue, "BadValue", "Mongo gateway delete limit must be 0 or 1")
		}
		key, err := encodePrimaryKey(id)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("deletes[%d]: %v", i, err))
		}
		ids = append(ids, key)
	}
	deleted, err := s.submitClusterDelete(name, ids)
	if err != nil {
		return mongoClusterMutationCommandError(err)
	}
	return marshalDeleteResponse(deleted)
}

func (s *Server) clusterCollectionFormat(name string) collections.DocumentFormat {
	format := s.DefaultCollectionOptions.DocumentFormat
	if s != nil && s.Collections != nil {
		if col, err := s.Collections.OpenCollection(name); err == nil && col != nil {
			format = col.MetaView().Options.DocumentFormat
		}
	}
	return format
}

func (s *Server) submitClusterInsert(name string, format collections.DocumentFormat, ids, docs [][]byte) (int32, error) {
	sections, seq := s.clusterMutationSections(iwire.CommandInsertBatch, "insert_batch", name,
		clusterDocumentFormatSection(format),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)},
	)
	response, err := s.submitClusterMutation(iwire.CommandInsertBatch, sections, seq)
	if err != nil {
		return 0, err
	}
	inserted, ok, err := clusterResponseMetaInt32(response, "inserted_count")
	if err != nil {
		return 0, err
	}
	if !ok {
		return int32(len(ids)), nil
	}
	return inserted, nil
}

func (s *Server) submitClusterUpdateBSONSet(name string, item mongoUpdateItem) (int32, int32, error) {
	sections, seq := s.clusterMutationSections(iwire.CommandUpdateBSONSet, "update_bson_set", name,
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, item.key)},
		clusterBSONSetFieldNamesSection(item.bsonSetFields),
		clusterBSONSetFieldValuesSection(item.bsonSetFields),
	)
	response, err := s.submitClusterMutation(iwire.CommandUpdateBSONSet, sections, seq)
	if err != nil {
		return 0, 0, err
	}
	matched, ok, err := clusterResponseMetaInt32(response, "matched_count")
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, errors.New("cluster submitter response_meta missing matched_count")
	}
	modified, ok, err := clusterResponseMetaInt32(response, "modified_count")
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, errors.New("cluster submitter response_meta missing modified_count")
	}
	return matched, modified, nil
}

func (s *Server) submitClusterDelete(name string, ids [][]byte) (int32, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	sections, seq := s.clusterMutationSections(iwire.CommandDeleteBatch, "delete_batch", name,
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
	)
	response, err := s.submitClusterMutation(iwire.CommandDeleteBatch, sections, seq)
	if err != nil {
		return 0, err
	}
	deleted, ok, err := clusterResponseMetaInt32(response, "deleted_count")
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("cluster submitter response_meta missing deleted_count")
	}
	return deleted, nil
}

func (s *Server) clusterMutationSections(command iwire.CommandID, commandName, collection string, payload ...iwire.Section) ([]iwire.Section, uint64) {
	seq := s.nextClusterSubmit.Add(1)
	sections := make([]iwire.Section, 0, 5+len(payload))
	sections = append(sections,
		iwire.Section{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})},
		iwire.Section{ID: iwire.SectionIdempotencyKey, Bytes: []byte("mongo-gateway/" + commandName + "/" + strconv.FormatUint(seq, 10))},
		iwire.Section{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, clusterSyntheticCatalogV1)},
		clusterCollectionNameRefSection(collection),
	)
	return append(sections, payload...), seq
}

func (s *Server) submitClusterMutation(command iwire.CommandID, sections []iwire.Section, seq uint64) ([]iwire.Section, error) {
	if s == nil || s.ClusterSubmitter == nil {
		return nil, errors.New("Mongo gateway cluster submitter is not configured")
	}
	if row := raftentry.ClassifyNativeWireCommandV1(command); !row.Known || row.Decision != raftentry.DecisionAccepted {
		return nil, fmt.Errorf("cluster submitter command %d is not accepted by R3a v1", command)
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, err
	}
	entry, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		return nil, err
	}
	metadata := treenativewire.ClusterRequestMetadata{
		RequestID: uint64(seq),
		AckPolicy: iwire.AckVisible,
	}
	if _, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata}); err != nil {
		return nil, err
	}
	result, err := s.ClusterSubmitter.SubmitCommandEntryV1(context.Background(), entry, metadata)
	if err != nil {
		return nil, err
	}
	if err := validateMongoClusterSubmitResult(result); err != nil {
		return nil, err
	}
	return result.ResponseSections, nil
}

func validateMongoClusterSubmitResult(result treenativewire.ClusterSubmitResult) error {
	switch result.ActualAck {
	case iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced:
		return nil
	case iwire.AckRaftCommitted:
		if result.CommittedRecoverable {
			return nil
		}
		return errors.New("cluster submitter returned raft_committed without committed recoverability")
	default:
		return fmt.Errorf("cluster submitter returned unsupported ack policy %d", result.ActualAck)
	}
}

func clusterCollectionNameRefSection(name string) iwire.Section {
	payload := make([]byte, 0, 1+len(name))
	payload = append(payload, clusterCollectionRefNameTag)
	payload = append(payload, name...)
	return iwire.Section{ID: iwire.SectionCollectionRef, Bytes: payload}
}

func clusterDocumentFormatSection(format collections.DocumentFormat) iwire.Section {
	return iwire.Section{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(clusterDocumentFormat(format)))}
}

func clusterDocumentFormat(format collections.DocumentFormat) iwire.DocumentFormat {
	switch format {
	case collections.DocumentFormatJSON:
		return iwire.DocumentFormatJSON
	case collections.DocumentFormatBSON:
		return iwire.DocumentFormatBSON
	case collections.DocumentFormatTemplateV1:
		return iwire.DocumentFormatTemplateV1
	default:
		return iwire.DocumentFormatDefault
	}
}

func clusterBSONSetFieldNamesSection(fields []collections.BSONSetField) iwire.Section {
	return iwire.Section{ID: iwire.SectionUpdateFieldNames, Bytes: clusterAppendByteVectorStrings(nil, fields)}
}

func clusterAppendByteVectorStrings(dst []byte, fields []collections.BSONSetField) []byte {
	values := make([][]byte, len(fields))
	for i := range fields {
		values[i] = []byte(fields[i].Key)
	}
	return iwire.AppendByteVector(dst, values...)
}

func clusterBSONSetFieldValuesSection(fields []collections.BSONSetField) iwire.Section {
	values := make([][]byte, len(fields))
	for i := range fields {
		value := fields[i].Value
		raw := make([]byte, 0, 1+len(value.Value))
		raw = append(raw, byte(value.Type))
		raw = append(raw, value.Value...)
		values[i] = raw
	}
	return iwire.Section{ID: iwire.SectionUpdateFieldValues, Bytes: iwire.AppendByteVector(nil, values...)}
}

func clusterResponseMetaInt32(sections []iwire.Section, key string) (int32, bool, error) {
	values, ok, err := clusterResponseMetaMap(sections)
	if err != nil || !ok {
		return 0, false, err
	}
	raw, ok := values[key]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || n < 0 {
		return 0, true, fmt.Errorf("cluster submitter response_meta %s is not a non-negative int32", key)
	}
	return int32(n), true, nil
}

func clusterResponseMetaMap(sections []iwire.Section) (map[string]string, bool, error) {
	var raw []byte
	found := false
	for _, section := range sections {
		if section.ID != iwire.SectionResponseMeta {
			continue
		}
		if found {
			return nil, false, errors.New("cluster submitter returned duplicate response_meta sections")
		}
		raw = section.Bytes
		found = true
	}
	if !found {
		return nil, false, nil
	}
	values, err := clusterDecodeStringMap(raw)
	return values, true, err
}

func clusterDecodeStringMap(src []byte) (map[string]string, error) {
	count, off, err := clusterReadUvarint(src)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, int(count))
	for i := uint64(0); i < count; i++ {
		key, err := clusterReadString(src, &off)
		if err != nil {
			return nil, err
		}
		value, err := clusterReadString(src, &off)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	if off != len(src) {
		return nil, errors.New("cluster submitter response_meta has trailing bytes")
	}
	return out, nil
}

func clusterReadString(src []byte, off *int) (string, error) {
	n, read, err := clusterReadUvarint(src[*off:])
	if err != nil {
		return "", err
	}
	*off += read
	if n > uint64(len(src)-*off) {
		return "", errors.New("cluster submitter response_meta string exceeds remaining bytes")
	}
	out := string(src[*off : *off+int(n)])
	*off += int(n)
	return out, nil
}

func clusterReadUvarint(src []byte) (uint64, int, error) {
	value, n := binary.Uvarint(src)
	switch {
	case n > 0:
		return value, n, nil
	case n == 0:
		return 0, 0, errors.New("cluster submitter response_meta contains invalid uvarint")
	default:
		return 0, 0, errors.New("cluster submitter response_meta contains uvarint overflow")
	}
}

func rejectClusterWriteConcern(command wire.Document) error {
	if bson.Raw(command).Lookup("writeConcern").IsZero() {
		return nil
	}
	return errors.New("Mongo gateway cluster submitter mode does not implement Mongo writeConcern semantics")
}

func mongoClusterMutationCommandError(err error) (wire.Document, error) {
	code, codeName := commandCodeBadValue, "BadValue"
	if nativeCode, ok := iwire.ErrorCodeOf(err); ok {
		switch nativeCode {
		case iwire.ErrUnsupportedFeature:
			code, codeName = commandCodeBadValue, "BadValue"
		case iwire.ErrDurabilityUnavailable, iwire.ErrReadOnly, iwire.ErrCatalogVersionMismatch:
			code, codeName = commandCodeBadValue, "BadValue"
		case iwire.ErrDuplicateDocumentID, iwire.ErrDocumentExists, iwire.ErrUniqueIndexConflict:
			code, codeName = commandCodeDuplicateKey, "DuplicateKey"
		}
	}
	if collections.IsDuplicateKeyError(err) {
		code, codeName = commandCodeDuplicateKey, "DuplicateKey"
	}
	return commandError(code, codeName, err.Error())
}
