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
	clusterCollectionMetaV5     = uint64(5)
)

func (s *Server) clusterSubmitterConfigured() bool {
	return s != nil && s.ClusterSubmitter != nil
}

func (s *Server) currentClusterCatalogVersion(ctx context.Context) (uint64, error) {
	if s == nil || s.ClusterCatalogVersion == nil {
		return 0, errors.New("Mongo gateway cluster catalog version provider is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.ClusterCatalogVersion(ctx)
}

func (s *Server) clusterCreateCollectionResponse(ctx context.Context, command wire.Document) (wire.Document, error) {
	if err := rejectClusterWriteConcern(command); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	collection, err := commandString(command, "create")
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
	if err := validateCreateCollectionCommand(command); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if err := s.submitClusterCreateCollection(ctx, *s.defaultCollectionMeta(name)); err != nil {
		return mongoClusterMutationCommandError(err)
	}
	return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})
}

func (s *Server) clusterInsertResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
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
	format, exists, err := s.clusterCollectionFormat(name)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	ids, stored, err := prepareInsertDocuments(documents, format)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if !exists {
		if err := s.submitClusterCreateCollection(ctx, *s.defaultCollectionMeta(name)); err != nil {
			return mongoClusterMutationCommandError(err)
		}
	}
	inserted, err := s.submitClusterInsert(ctx, name, format, ids, stored)
	if err != nil {
		return mongoClusterMutationCommandError(err)
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: inserted},
	})
}

func (s *Server) clusterUpdateResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
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
	var matched, modified int32
	for i, update := range updates {
		item, err := parseMongoUpdateItem(i, update)
		if err != nil {
			return mongoUpdateParseCommandError(err)
		}
		if !item.bsonSetFieldsOK {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: cluster Mongo gateway currently supports only top-level BSON $set updateOne by _id", i))
		}
		matchedOne, modifiedOne, err := s.submitClusterUpdateBSONSet(ctx, name, item)
		if err != nil {
			return mongoClusterMutationCommandError(mongoUpdateErrorWithIndex(item.index, err))
		}
		matched += matchedOne
		modified += modifiedOne
	}
	return marshalUpdateResponse(matched, modified)
}

func (s *Server) clusterDeleteResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
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
	seenIDs := make(map[string]struct{}, len(deletes))
	submitPendingBeforeError := func() error {
		if len(ids) == 0 {
			return nil
		}
		_, err := s.submitClusterDelete(ctx, name, ids)
		return err
	}
	for i, deleteItem := range deletes {
		filter, err := requiredDocumentField(deleteItem, "q")
		if err != nil {
			if submitErr := submitPendingBeforeError(); submitErr != nil {
				return mongoClusterMutationCommandError(submitErr)
			}
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("deletes[%d]: %v", i, err))
		}
		id, err := idEqualityFilterValue(filter, "delete")
		if err != nil {
			if submitErr := submitPendingBeforeError(); submitErr != nil {
				return mongoClusterMutationCommandError(submitErr)
			}
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("deletes[%d]: %v", i, err))
		}
		if limit, err := optionalInt32Field(deleteItem, "limit"); err != nil {
			if submitErr := submitPendingBeforeError(); submitErr != nil {
				return mongoClusterMutationCommandError(submitErr)
			}
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("deletes[%d]: %v", i, err))
		} else if limit != 0 && limit != 1 {
			if submitErr := submitPendingBeforeError(); submitErr != nil {
				return mongoClusterMutationCommandError(submitErr)
			}
			return commandError(commandCodeBadValue, "BadValue", "Mongo gateway delete limit must be 0 or 1")
		}
		key, err := encodePrimaryKey(id)
		if err != nil {
			if submitErr := submitPendingBeforeError(); submitErr != nil {
				return mongoClusterMutationCommandError(submitErr)
			}
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("deletes[%d]: %v", i, err))
		}
		keyString := string(key)
		if _, ok := seenIDs[keyString]; ok {
			continue
		}
		seenIDs[keyString] = struct{}{}
		ids = append(ids, key)
	}
	deleted, err := s.submitClusterDelete(ctx, name, ids)
	if err != nil {
		return mongoClusterMutationCommandError(err)
	}
	return marshalDeleteResponse(deleted)
}

func (s *Server) clusterCollectionFormat(name string) (collections.DocumentFormat, bool, error) {
	format := s.DefaultCollectionOptions.DocumentFormat
	if s != nil && s.Collections != nil {
		col, err := s.Collections.OpenCollection(name)
		if err == nil && col != nil {
			return col.MetaView().Options.DocumentFormat, true, nil
		}
		if err != nil && !errors.Is(err, collections.ErrCollectionNotFound) {
			return format, false, err
		}
	}
	return format, false, nil
}

func (s *Server) submitClusterCreateCollection(ctx context.Context, meta collections.CollectionMeta) error {
	if len(meta.Indexes) != 0 || len(meta.VectorIndexes) != 0 || len(meta.TextIndexes) != 0 {
		return errors.New("Mongo gateway cluster create currently supports only collection metadata without indexes")
	}
	catalogVersion, err := s.currentClusterCatalogVersion(ctx)
	if err != nil {
		return err
	}
	sections, seq, err := s.clusterCreateCollectionSections(meta, catalogVersion)
	if err != nil {
		return err
	}
	_, err = s.submitClusterMutation(ctx, iwire.CommandCreateCollection, sections, seq)
	return err
}

func (s *Server) submitClusterInsert(ctx context.Context, name string, format collections.DocumentFormat, ids, docs [][]byte) (int32, error) {
	catalogVersion, err := s.currentClusterCatalogVersion(ctx)
	if err != nil {
		return 0, err
	}
	sections, seq, err := s.clusterMutationSections(iwire.CommandInsertBatch, "insert_batch", name, catalogVersion,
		clusterDocumentFormatSection(format),
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
		iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)},
	)
	if err != nil {
		return 0, err
	}
	response, err := s.submitClusterMutation(ctx, iwire.CommandInsertBatch, sections, seq)
	if err != nil {
		return 0, err
	}
	inserted, ok, err := clusterResponseMetaInt32(response, "inserted_count")
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("cluster submitter response_meta missing inserted_count")
	}
	return inserted, nil
}

func (s *Server) submitClusterUpdateBSONSet(ctx context.Context, name string, item mongoUpdateItem) (int32, int32, error) {
	catalogVersion, err := s.currentClusterCatalogVersion(ctx)
	if err != nil {
		return 0, 0, err
	}
	sections, seq, err := s.clusterMutationSections(iwire.CommandUpdateBSONSet, "update_bson_set", name, catalogVersion,
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, item.key)},
		clusterBSONSetFieldNamesSection(item.bsonSetFields),
		clusterBSONSetFieldValuesSection(item.bsonSetFields),
	)
	if err != nil {
		return 0, 0, err
	}
	response, err := s.submitClusterMutation(ctx, iwire.CommandUpdateBSONSet, sections, seq)
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

func (s *Server) submitClusterDelete(ctx context.Context, name string, ids [][]byte) (int32, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	catalogVersion, err := s.currentClusterCatalogVersion(ctx)
	if err != nil {
		return 0, err
	}
	sections, seq, err := s.clusterMutationSections(iwire.CommandDeleteBatch, "delete_batch", name, catalogVersion,
		iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)},
	)
	if err != nil {
		return 0, err
	}
	response, err := s.submitClusterMutation(ctx, iwire.CommandDeleteBatch, sections, seq)
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

func (s *Server) clusterCreateCollectionSections(meta collections.CollectionMeta, catalogVersion uint64) ([]iwire.Section, uint64, error) {
	seq := s.nextClusterSubmit.Add(1)
	idempotencyKey, err := s.clusterIdempotencyKey("create_collection", seq)
	if err != nil {
		return nil, 0, err
	}
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: idempotencyKey},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		clusterCollectionMetaSection(meta),
	}
	return sections, seq, nil
}

func (s *Server) clusterMutationSections(command iwire.CommandID, commandName, collection string, catalogVersion uint64, payload ...iwire.Section) ([]iwire.Section, uint64, error) {
	seq := s.nextClusterSubmit.Add(1)
	idempotencyKey, err := s.clusterIdempotencyKey(commandName, seq)
	if err != nil {
		return nil, 0, err
	}
	sections := make([]iwire.Section, 0, 5+len(payload))
	sections = append(sections,
		iwire.Section{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})},
		iwire.Section{ID: iwire.SectionIdempotencyKey, Bytes: idempotencyKey},
		iwire.Section{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		clusterCollectionNameRefSection(collection),
	)
	return append(sections, payload...), seq, nil
}

func (s *Server) clusterIdempotencyKey(commandName string, seq uint64) ([]byte, error) {
	if s == nil || s.ClusterIdempotencyNonce == "" {
		return nil, errors.New("Mongo gateway cluster idempotency nonce is not configured")
	}
	key := make([]byte, 0, len("mongo-gateway/")+len(s.ClusterIdempotencyNonce)+1+len(commandName)+1+20)
	key = append(key, "mongo-gateway/"...)
	key = append(key, s.ClusterIdempotencyNonce...)
	key = append(key, '/')
	key = append(key, commandName...)
	key = append(key, '/')
	key = strconv.AppendUint(key, seq, 10)
	if len(key) > raftentry.MaxIdempotencyKeyBytesV1 {
		return nil, fmt.Errorf("Mongo gateway cluster idempotency key length %d exceeds %d", len(key), raftentry.MaxIdempotencyKeyBytesV1)
	}
	return key, nil
}

func (s *Server) submitClusterMutation(ctx context.Context, command iwire.CommandID, sections []iwire.Section, seq uint64) ([]iwire.Section, error) {
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
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := s.ClusterSubmitter.SubmitCommandEntryV1(ctx, entry, metadata)
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
	case iwire.AckRaftCommitted:
		return errors.New("cluster submitter returned raft_committed without proving requested local visibility")
	default:
		return fmt.Errorf("cluster submitter returned unsupported ack policy %d", result.ActualAck)
	}
	ack, ok, err := clusterResponseMetaAckPolicy(result.ResponseSections)
	if err != nil || !ok {
		return err
	}
	if ack != result.ActualAck {
		return fmt.Errorf("cluster submitter response_meta actual_ack_policy %d does not match submit result ack policy %d", ack, result.ActualAck)
	}
	return nil
}

func clusterCollectionNameRefSection(name string) iwire.Section {
	payload := make([]byte, 0, 1+len(name))
	payload = append(payload, clusterCollectionRefNameTag)
	payload = append(payload, name...)
	return iwire.Section{ID: iwire.SectionCollectionRef, Bytes: payload}
}

func clusterCollectionMetaSection(meta collections.CollectionMeta) iwire.Section {
	return iwire.Section{ID: iwire.SectionCollectionMeta, Bytes: clusterEncodeCollectionMeta(meta)}
}

func clusterEncodeCollectionMeta(meta collections.CollectionMeta) []byte {
	dst := binary.AppendUvarint(nil, clusterCollectionMetaV5)
	dst = clusterAppendString(dst, meta.Name)
	dst = binary.AppendUvarint(dst, uint64(clusterDocumentFormat(meta.Options.DocumentFormat)))
	dst = binary.AppendUvarint(dst, clusterRootStorage(meta.Options.DataRootStoragePolicy))
	dst = binary.AppendUvarint(dst, clusterRootStorage(meta.Options.IndexStateStoragePolicy))
	dst = clusterAppendBool(dst, meta.Options.AllowArrayValuesInIndex)
	dst = clusterAppendBool(dst, meta.Options.DisableIndexedWriteMemtables)
	dst = clusterAppendBool(dst, meta.Options.BufferedIndexedWrites)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedWriteMaxDocuments))
	dst = binary.AppendVarint(dst, meta.Options.BufferedIndexedWriteMaxBytes)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedWriteMaxRootRuns))
	dst = clusterAppendBool(dst, meta.Options.BufferedIndexedAsyncFlush)
	dst = clusterAppendBool(dst, meta.Options.BufferedIndexedOverlayRoots)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits))
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	return dst
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

func clusterRootStorage(policy collections.RootStoragePolicy) uint64 {
	switch policy {
	case collections.RootStorageFast:
		return 1
	case collections.RootStorageCompressed:
		return 2
	default:
		return 0
	}
}

func clusterAppendBool(dst []byte, value bool) []byte {
	if value {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func clusterAppendString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
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

func clusterResponseMetaAckPolicy(sections []iwire.Section) (iwire.AckPolicy, bool, error) {
	values, ok, err := clusterResponseMetaMap(sections)
	if err != nil || !ok {
		return 0, ok, err
	}
	raw, ok := values["actual_ack_policy"]
	if !ok {
		return 0, true, errors.New("cluster submitter response_meta missing actual_ack_policy")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, true, errors.New("cluster submitter response_meta actual_ack_policy is not a uint64")
	}
	return iwire.AckPolicy(value), true, nil
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

func mongoClusterUnsupportedLocalMutation(command string) (wire.Document, error) {
	return commandError(
		commandCodeBadValue,
		"BadValue",
		"Mongo gateway cluster submitter mode does not support local "+command+" mutation",
	)
}
