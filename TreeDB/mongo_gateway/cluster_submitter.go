package mongogateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	treenativewire "github.com/snissn/gomap/TreeDB/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	clusterCollectionRefNameTag = byte(1)
	clusterCollectionMetaV5     = uint64(5)

	commandCodeWriteConcernFailed int32 = 64
	commandCodeShutdownInProgress int32 = 91
	commandCodeNotWritablePrimary int32 = 10107
)

func (s *Server) clusterSubmitterConfigured() bool {
	return s != nil && s.ClusterSubmitter != nil
}

func (s *Server) clusterRouteProviderConfigured() bool {
	if s == nil || s.ClusterSubmitter == nil {
		return false
	}
	_, ok := s.ClusterSubmitter.(treenativewire.ClusterRouteProvider)
	return ok
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
	ack, err := parseClusterWriteConcern(command, "create")
	if err != nil {
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
	if err := s.submitClusterCreateCollection(ctx, *s.defaultCollectionMeta(name), ack, mongoClusterRouteRequest(db, collection, iwire.CommandCreateCollection, "create_collection")); err != nil {
		return mongoClusterMutationCommandError(err)
	}
	return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})
}

func (s *Server) clusterInsertResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	ack, err := parseClusterWriteConcern(command, "insert")
	if err != nil {
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
		if err := s.submitClusterCreateCollection(ctx, *s.defaultCollectionMeta(name), ack, mongoClusterRouteRequest(db, collection, iwire.CommandCreateCollection, "create_collection")); err != nil {
			return mongoClusterMutationCommandError(err)
		}
	}
	inserted, err := s.submitClusterInsert(ctx, name, format, ids, stored, ack, mongoClusterDocumentIDsRouteRequest(db, collection, iwire.CommandInsertBatch, "insert_batch", ids))
	if err != nil {
		return mongoClusterMutationCommandError(err)
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: inserted},
	})
}

func (s *Server) clusterUpdateResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	ack, err := parseClusterWriteConcern(command, "update")
	if err != nil {
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
	// Routed collection writes have no cross-token atomicity.  Refuse a batch
	// before even consulting the local collection cache so it cannot be routed
	// as a sequence of single-item submissions and leave a prefix committed.
	if len(updates) != 1 {
		return mongoClusterMutationCommandError(errors.New("cluster Mongo gateway currently does not support multi-item update commands"))
	}
	if err := s.admitClusterMutation(ctx); err != nil {
		return mongoClusterMutationCommandError(err)
	}
	exists, err := s.clusterCollectionExists(name)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if !exists {
		for i, update := range updates {
			item, err := parseMongoUpdateItem(i, update)
			if err != nil {
				return mongoUpdateParseCommandError(err)
			}
			if item.upsert {
				return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: cluster Mongo gateway currently does not support upsert", i))
			}
			if response, err := clusterUpdateItemAdmission(i, item); response != nil || err != nil {
				return response, err
			}
		}
		if s.clusterRouteProviderConfigured() {
			return mongoClusterMutationCommandError(mongoClusterMissingCollectionMetadataError())
		}
		if err := rejectClusterMissingCollectionMajorityNoop(ack); err != nil {
			return mongoClusterMutationCommandError(err)
		}
		return marshalUpdateResponse(0, 0)
	}
	var matched, modified int32
	for i, update := range updates {
		item, err := parseMongoUpdateItem(i, update)
		if err != nil {
			return mongoUpdateParseCommandError(err)
		}
		if item.upsert {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: cluster Mongo gateway currently does not support upsert", i))
		}
		if response, err := clusterUpdateItemAdmission(i, item); response != nil || err != nil {
			return response, err
		}
		route := mongoClusterRouteRequest(db, collection, iwire.CommandUpdateBSONSet, "update_bson_set")
		if len(updates) == 1 {
			route = mongoClusterDocumentIDRouteRequest(db, collection, iwire.CommandUpdateBSONSet, "update_bson_set", item.key)
		}
		matchedOne, modifiedOne, err := s.submitClusterUpdateBSONSet(ctx, name, item, ack, route)
		if err != nil {
			return mongoClusterMutationCommandError(mongoUpdateErrorWithIndex(item.index, err))
		}
		matched += matchedOne
		modified += modifiedOne
	}
	return marshalUpdateResponse(matched, modified)
}

func clusterUpdateItemAdmission(index int, item mongoUpdateItem) (wire.Document, error) {
	if !item.multi && item.exactID && item.bsonSetFieldsOK && !mongoBSONSetFieldsNeedNestingValidation(item.bsonSetFields) {
		return nil, nil
	}
	return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: cluster Mongo gateway currently supports only top-level BSON $set updateOne by _id", index))
}

func (s *Server) clusterDeleteResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	ack, err := parseClusterWriteConcern(command, "delete")
	if err != nil {
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
	// See clusterUpdateResponse: routed batches are deliberately fail-closed
	// until the cluster protocol owns their all-item outcome and error indices.
	if len(deletes) != 1 {
		return mongoClusterMutationCommandError(errors.New("cluster Mongo gateway currently does not support multi-item delete commands"))
	}
	// Keep raw-command validation aligned with standalone before any cluster
	// admission, catalog lookup, routing, or submit side effect.
	if limit, limitSet, err := optionalInt32FieldWithPresence(deletes[0], "limit"); err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("deletes[0]: %v", err))
	} else if !limitSet {
		return commandError(commandCodeFailedToParse, "FailedToParse", "deletes[0]: Mongo command missing \"limit\"")
	} else if limit != 1 {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway cluster delete requires limit: 1")
	}
	if err := s.admitClusterMutation(ctx); err != nil {
		return mongoClusterMutationCommandError(err)
	}
	exists, err := s.clusterCollectionExists(name)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if !exists {
		if s.clusterRouteProviderConfigured() {
			return mongoClusterMutationCommandError(mongoClusterMissingCollectionMetadataError())
		}
		if err := rejectClusterMissingCollectionMajorityNoop(ack); err != nil {
			return mongoClusterMutationCommandError(err)
		}
		return marshalDeleteResponse(0)
	}
	ids := make([][]byte, 0, len(deletes))
	seenIDs := make(map[string]struct{}, len(deletes))
	submitPendingBeforeError := func() error {
		if len(ids) == 0 {
			return nil
		}
		_, err := s.submitClusterDelete(ctx, name, ids, ack, mongoClusterDocumentIDsRouteRequest(db, collection, iwire.CommandDeleteBatch, "delete_batch", ids))
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
		if limit, limitSet, err := optionalInt32FieldWithPresence(deleteItem, "limit"); err != nil {
			if submitErr := submitPendingBeforeError(); submitErr != nil {
				return mongoClusterMutationCommandError(submitErr)
			}
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("deletes[%d]: %v", i, err))
		} else if !limitSet {
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("deletes[%d]: Mongo command missing \"limit\"", i))
		} else if limit != 1 {
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
	deleted, err := s.submitClusterDelete(ctx, name, ids, ack, mongoClusterDocumentIDsRouteRequest(db, collection, iwire.CommandDeleteBatch, "delete_batch", ids))
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

func (s *Server) clusterCollectionExists(name string) (bool, error) {
	if s != nil && s.clusterCollectionLookupHook != nil {
		s.clusterCollectionLookupHook()
	}
	if s == nil || s.Collections == nil {
		return true, nil
	}
	col, err := s.Collections.OpenCollection(name)
	if err == nil && col != nil {
		return true, nil
	}
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return false, nil
	}
	return false, err
}

func (s *Server) submitClusterCreateCollection(ctx context.Context, meta collections.CollectionMeta, ack iwire.AckPolicy, route *treenativewire.ClusterRouteRequest) error {
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
	_, err = s.submitClusterMutation(ctx, iwire.CommandCreateCollection, sections, seq, ack, route)
	return err
}

func (s *Server) submitClusterInsert(ctx context.Context, name string, format collections.DocumentFormat, ids, docs [][]byte, ack iwire.AckPolicy, route *treenativewire.ClusterRouteRequest) (int32, error) {
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
	response, err := s.submitClusterMutation(ctx, iwire.CommandInsertBatch, sections, seq, ack, route)
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

func (s *Server) submitClusterUpdateBSONSet(ctx context.Context, name string, item mongoUpdateItem, ack iwire.AckPolicy, route *treenativewire.ClusterRouteRequest) (int32, int32, error) {
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
	response, err := s.submitClusterMutation(ctx, iwire.CommandUpdateBSONSet, sections, seq, ack, route)
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

func (s *Server) submitClusterDelete(ctx context.Context, name string, ids [][]byte, ack iwire.AckPolicy, route *treenativewire.ClusterRouteRequest) (int32, error) {
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
	response, err := s.submitClusterMutation(ctx, iwire.CommandDeleteBatch, sections, seq, ack, route)
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

func rejectClusterMissingCollectionMajorityNoop(ack iwire.AckPolicy) error {
	if ack != iwire.AckRaftCommitted {
		return nil
	}
	return mongoWriteConcernFailedError("cluster submitter cannot prove raft_committed durability for missing collection no-op")
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

func (s *Server) submitClusterMutation(ctx context.Context, command iwire.CommandID, sections []iwire.Section, seq uint64, ack iwire.AckPolicy, routeReq *treenativewire.ClusterRouteRequest) ([]iwire.Section, error) {
	if s == nil || s.ClusterSubmitter == nil {
		return nil, errors.New("Mongo gateway cluster submitter is not configured")
	}
	if err := s.admitClusterMutation(ctx); err != nil {
		return nil, err
	}
	var route treenativewire.ClusterRouteTarget
	var routed bool
	if routeReq != nil {
		var err error
		route, routed, err = treenativewire.PreflightClusterRoute(ctx, s.ClusterSubmitter, *routeReq)
		if err != nil {
			return nil, err
		}
		if routed {
			if err := s.rejectClusterTokenRouteIndexedMutation(command, *routeReq, route); err != nil {
				return nil, err
			}
		}
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
		AckPolicy: ack,
	}
	if routed {
		treenativewire.ApplyClusterRouteMetadata(&metadata, *routeReq, route)
	}
	if _, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata}); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result, submitErr := treenativewire.SubmitCommandEntryWithVectorPartitionAdmissionV1(ctx, s.ClusterSubmitter, command, cmd.Known, entry, metadata)
	if result.CommittedApplied {
		if err := treenativewire.ConfirmCommittedVectorPartitionMutationV1(ctx, s.ClusterSubmitter, command, cmd.Known); err != nil {
			return nil, errors.Join(submitErr, err)
		}
	}
	if submitErr != nil {
		return nil, submitErr
	}
	if err := validateMongoClusterSubmitResult(ack, result); err != nil {
		return nil, err
	}
	return result.ResponseSections, nil
}

func (s *Server) admitClusterMutation(ctx context.Context) error {
	if s == nil || s.ClusterSubmitter == nil {
		return errors.New("Mongo gateway cluster submitter is not configured")
	}
	return treenativewire.AdmitClusterMutation(ctx, s.ClusterSubmitter)
}

func (s *Server) preflightClusterRoute(ctx context.Context, routeReq *treenativewire.ClusterRouteRequest) error {
	if routeReq == nil || s == nil || s.ClusterSubmitter == nil {
		return nil
	}
	_, _, err := treenativewire.PreflightClusterRoute(ctx, s.ClusterSubmitter, *routeReq)
	return err
}

func (s *Server) preflightClusterFindRoute(ctx context.Context, db, collection string, plan findPlan) error {
	if !s.clusterRouteProviderConfigured() {
		return nil
	}
	routeReq, err := mongoClusterFindRouteRequest(db, collection, plan)
	if err != nil {
		return err
	}
	_, routed, err := treenativewire.PreflightClusterRoute(ctx, s.ClusterSubmitter, *routeReq)
	if err != nil || !routed {
		return err
	}
	return &iwire.ProtocolError{
		Code: iwire.ErrReadOnly,
		Reason: "Mongo gateway cluster route target for _id find is disabled until the serving collection-store identity " +
			"is bound to the owner Raft proof; refusing an unbarriered local read",
	}
}

func mongoClusterFindRouteRequest(db, collection string, plan findPlan) (*treenativewire.ClusterRouteRequest, error) {
	value, ok := simplePrimaryEqualityFindValue(plan)
	if !ok {
		return mongoClusterQueryRouteRequest(db, collection, 0, "find"), nil
	}
	key, err := encodePrimaryKey(value)
	if err != nil {
		return nil, err
	}
	return mongoClusterDocumentIDRouteRequest(db, collection, 0, "find", key), nil
}

func (s *Server) rejectClusterTokenRouteIndexedMutation(command iwire.CommandID, _ treenativewire.ClusterRouteRequest, target treenativewire.ClusterRouteTarget) error {
	switch target.PlacementMode {
	case string(raftplacement.PlacementModeTokenV1), string(raftplacement.PlacementModeRingV1):
	default:
		return nil
	}
	switch command {
	case iwire.CommandInsertBatch, iwire.CommandUpdateBSONSet, iwire.CommandDeleteBatch:
	default:
		return nil
	}
	return &iwire.ProtocolError{
		Code: iwire.ErrReadOnly,
		Reason: "Mongo gateway token/ring mutation is disabled until authoritative collection and index metadata " +
			"is bound to the owner route proof",
	}
}

func mongoClusterRouteRequest(db, collection string, command iwire.CommandID, commandName string) *treenativewire.ClusterRouteRequest {
	return &treenativewire.ClusterRouteRequest{
		Database:    db,
		Catalog:     "default",
		Collection:  collection,
		CommandID:   command,
		CommandName: commandName,
		Shape:       treenativewire.ClusterRouteShapeCollection,
	}
}

func mongoClusterQueryRouteRequest(db, collection string, command iwire.CommandID, commandName string) *treenativewire.ClusterRouteRequest {
	req := mongoClusterRouteRequest(db, collection, command, commandName)
	req.Shape = treenativewire.ClusterRouteShapeQuery
	return req
}

func mongoClusterDocumentIDRouteRequest(db, collection string, command iwire.CommandID, commandName string, id []byte) *treenativewire.ClusterRouteRequest {
	req := mongoClusterRouteRequest(db, collection, command, commandName)
	req.Shape = treenativewire.ClusterRouteShapeToken
	req.TokenKnown = true
	req.Token = raftplacement.DocumentIDTokenV1(id)
	return req
}

func mongoClusterDocumentIDsRouteRequest(db, collection string, command iwire.CommandID, commandName string, ids [][]byte) *treenativewire.ClusterRouteRequest {
	if len(ids) == 1 {
		return mongoClusterDocumentIDRouteRequest(db, collection, command, commandName, ids[0])
	}
	req := mongoClusterRouteRequest(db, collection, command, commandName)
	if len(ids) > 1 {
		req.Shape = treenativewire.ClusterRouteShapeTokenBatch
		req.Tokens = make([]uint64, len(ids))
		for i, id := range ids {
			req.Tokens[i] = raftplacement.DocumentIDTokenV1(id)
		}
	}
	return req
}

func mongoClusterUpdateBatchRouteRequest(db, collection string, updates []wire.Document) *treenativewire.ClusterRouteRequest {
	ids := make([][]byte, 0, len(updates))
	for i, update := range updates {
		filter, err := requiredDocumentField(update, "q")
		if err != nil {
			return mongoClusterRouteRequest(db, collection, iwire.CommandUpdateBSONSet, "update_bson_set")
		}
		id, err := idEqualityFilterValue(filter, "update")
		if err != nil {
			return mongoClusterQueryRouteRequest(db, collection, iwire.CommandUpdateBSONSet, "update_bson_set")
		}
		key, err := encodePrimaryKey(id)
		if err != nil {
			return mongoClusterRouteRequest(db, collection, iwire.CommandUpdateBSONSet, "update_bson_set")
		}
		item, err := parseMongoUpdateItem(i, update)
		if err != nil || !item.exactID || !item.bsonSetFieldsOK {
			return mongoClusterRouteRequest(db, collection, iwire.CommandUpdateBSONSet, "update_bson_set")
		}
		ids = append(ids, key)
	}
	return mongoClusterDocumentIDsRouteRequest(db, collection, iwire.CommandUpdateBSONSet, "update_bson_set", ids)
}

func mongoClusterDeleteBatchRouteRequest(db, collection string, deletes []wire.Document) *treenativewire.ClusterRouteRequest {
	ids := make([][]byte, 0, len(deletes))
	seenIDs := make(map[string]struct{}, len(deletes))
	for _, deleteItem := range deletes {
		filter, err := requiredDocumentField(deleteItem, "q")
		if err != nil {
			return mongoClusterRouteRequest(db, collection, iwire.CommandDeleteBatch, "delete_batch")
		}
		id, err := idEqualityFilterValue(filter, "delete")
		if err != nil {
			return mongoClusterQueryRouteRequest(db, collection, iwire.CommandDeleteBatch, "delete_batch")
		}
		if limit, limitSet, err := optionalInt32FieldWithPresence(deleteItem, "limit"); err != nil || !limitSet || limit != 1 {
			return mongoClusterRouteRequest(db, collection, iwire.CommandDeleteBatch, "delete_batch")
		}
		key, err := encodePrimaryKey(id)
		if err != nil {
			return mongoClusterRouteRequest(db, collection, iwire.CommandDeleteBatch, "delete_batch")
		}
		keyString := string(key)
		if _, ok := seenIDs[keyString]; ok {
			continue
		}
		seenIDs[keyString] = struct{}{}
		ids = append(ids, key)
	}
	return mongoClusterDocumentIDsRouteRequest(db, collection, iwire.CommandDeleteBatch, "delete_batch", ids)
}

func validateMongoClusterSubmitResult(requested iwire.AckPolicy, result treenativewire.ClusterSubmitResult) error {
	switch result.ActualAck {
	case iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced, iwire.AckRaftCommitted:
	default:
		return fmt.Errorf("cluster submitter returned unsupported ack policy %d", result.ActualAck)
	}
	if requested == iwire.AckRaftCommitted {
		if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable {
			return mongoWriteConcernFailedError("cluster submitter did not prove raft_committed durability")
		}
	} else if result.ActualAck == iwire.AckRaftCommitted {
		return mongoWriteConcernFailedError(fmt.Sprintf("cluster submitter returned raft_committed without proving requested local ack policy %d", requested))
	} else if requested != 0 && result.ActualAck < requested {
		return mongoWriteConcernFailedError(fmt.Sprintf("cluster submitter actual ack policy %d is below requested policy %d", result.ActualAck, requested))
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

func parseClusterWriteConcern(command wire.Document, commandName string) (iwire.AckPolicy, error) {
	raw := bson.Raw(command)
	if !raw.Lookup("startTransaction").IsZero() || !raw.Lookup("autocommit").IsZero() || !raw.Lookup("txnNumber").IsZero() {
		return 0, fmt.Errorf("Mongo gateway %s does not support transactions or retryable writes", commandName)
	}
	value := raw.Lookup("writeConcern")
	if value.IsZero() {
		return iwire.AckVisible, nil
	}
	writeConcern, ok := value.DocumentOK()
	if !ok {
		return 0, errors.New("Mongo command field \"writeConcern\" must be a document")
	}
	elements, err := writeConcern.Elements()
	if err != nil {
		return 0, fmt.Errorf("Mongo command field \"writeConcern\" is malformed: %v", err)
	}
	ack := iwire.AckVisible
	seenW := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return 0, fmt.Errorf("Mongo command field \"writeConcern\" is malformed: %v", err)
		}
		switch key {
		case "w":
			if seenW {
				return 0, errors.New("Mongo writeConcern field \"w\" is duplicated")
			}
			seenW = true
			ack, err = parseClusterWriteConcernW(elem.Value())
			if err != nil {
				return 0, err
			}
		case "j", "wtimeout", "wtimeoutMS":
			return 0, fmt.Errorf("Mongo gateway cluster writeConcern does not support %q", key)
		default:
			return 0, fmt.Errorf("Mongo gateway cluster writeConcern does not support %q", key)
		}
	}
	return ack, nil
}

func parseClusterWriteConcernW(value bson.RawValue) (iwire.AckPolicy, error) {
	if w, ok := strictBSONInt64(value); ok {
		switch {
		case w == 1:
			return iwire.AckVisible, nil
		case w == 0:
			return 0, errors.New("Mongo gateway cluster writeConcern does not support unacknowledged w:0 writes")
		case w > 1:
			return 0, errors.New("Mongo gateway cluster writeConcern does not support numeric w greater than 1")
		default:
			return 0, errors.New("Mongo gateway cluster writeConcern does not support negative numeric w")
		}
	}
	if w, ok := value.StringValueOK(); ok {
		if w == "majority" {
			return iwire.AckRaftCommitted, nil
		}
		return 0, fmt.Errorf("Mongo gateway cluster writeConcern does not support tag string w:%q", w)
	}
	return 0, errors.New("Mongo command field \"writeConcern.w\" must be integer 1 or string \"majority\"")
}

func mongoClusterRouteCommandError(err error) (wire.Document, error) {
	code, codeName := commandCodeBadValue, "BadValue"
	var nativeCode iwire.ErrorCode
	var mongoErr mongoCommandError
	if errors.As(err, &mongoErr) {
		code, codeName = mongoErr.code, mongoErr.codeName
	} else if errors.Is(err, collections.ErrCommitAmbiguous) {
		nativeCode = iwire.ErrCommitAmbiguous
		code, codeName = commandCodeShutdownInProgress, "ShutdownInProgress"
	} else if parsedNativeCode, ok := mongoClusterNativeErrorCodeOf(err); ok {
		nativeCode = parsedNativeCode
		switch nativeCode {
		case iwire.ErrUnsupportedFeature:
			code, codeName = commandCodeBadValue, "BadValue"
		case iwire.ErrReadOnly:
			code, codeName = commandCodeNotWritablePrimary, "NotWritablePrimary"
		case iwire.ErrDurabilityUnavailable:
			code, codeName = commandCodeShutdownInProgress, "ShutdownInProgress"
		case iwire.ErrCommitAmbiguous:
			code, codeName = commandCodeShutdownInProgress, "ShutdownInProgress"
		case iwire.ErrCatalogVersionMismatch:
			code, codeName = commandCodeBadValue, "BadValue"
		case iwire.ErrDuplicateDocumentID, iwire.ErrDocumentExists, iwire.ErrUniqueIndexConflict:
			code, codeName = commandCodeDuplicateKey, "DuplicateKey"
		}
	}
	if collections.IsDuplicateKeyError(err) && nativeCode != iwire.ErrCommitAmbiguous {
		code, codeName = commandCodeDuplicateKey, "DuplicateKey"
	}
	message := err.Error()
	if _, routed := treenativewire.ClusterRouteErrorMetadataOf(err); routed {
		message = "Mongo gateway cluster route rejected"
	}
	return commandErrorWithFields(code, codeName, message, mongoClusterErrorFields(err, nativeCode, codeName))
}

func mongoClusterNativeErrorCodeOf(err error) (iwire.ErrorCode, bool) {
	if code, ok := iwire.ErrorCodeOf(err); ok {
		return code, true
	}
	var wireErr *treenativewire.WireError
	if errors.As(err, &wireErr) {
		return wireErr.Code, true
	}
	return 0, false
}

func mongoClusterErrorFields(err error, nativeCode iwire.ErrorCode, codeName string) bson.D {
	if err == nil || !mongoClusterErrorMetadataApplies(err, nativeCode, codeName) {
		return nil
	}
	fields := bson.D{
		{Key: "treedbClusterError", Value: true},
	}
	if class := mongoClusterErrorClass(err, nativeCode, codeName); class != "" {
		fields = append(fields, bson.E{Key: "treedbErrorClass", Value: class})
	}
	if leaderHint := mongoClusterLeaderHint(err); leaderHint != "" {
		fields = append(fields, bson.E{Key: "treedbLeaderHint", Value: leaderHint})
	}
	if route, ok := treenativewire.ClusterRouteErrorMetadataOf(err); ok {
		fields = appendMongoClusterRouteErrorFields(fields, route)
	}
	return fields
}

func mongoClusterErrorMetadataApplies(err error, nativeCode iwire.ErrorCode, codeName string) bool {
	if err == nil {
		return false
	}
	if nativeCode != 0 ||
		errors.Is(err, collections.ErrCommitAmbiguous) ||
		collections.IsDuplicateKeyError(err) {
		return true
	}
	if route, ok := treenativewire.ClusterRouteErrorMetadataOf(err); ok && route.Class != "" {
		return true
	}
	return codeName == "WriteConcernFailed"
}

func mongoClusterErrorClass(err error, nativeCode iwire.ErrorCode, codeName string) string {
	if route, ok := treenativewire.ClusterRouteErrorMetadataOf(err); ok && route.Class != "" {
		return route.Class
	}
	message := strings.ToLower(err.Error())
	switch nativeCode {
	case iwire.ErrReadOnly:
		if mongoClusterRouteRejectedMessage(message) {
			return "route_rejected"
		}
		if mongoClusterLeaderHint(err) != "" ||
			strings.Contains(message, "not leader") ||
			strings.Contains(message, "not cluster leader") {
			return "not_leader"
		}
		return "read_only"
	case iwire.ErrDurabilityUnavailable:
		return "durability_unavailable"
	case iwire.ErrCommitAmbiguous:
		return "commit_ambiguous"
	case iwire.ErrCatalogVersionMismatch:
		return "catalog_version_mismatch"
	case iwire.ErrDuplicateDocumentID, iwire.ErrDocumentExists, iwire.ErrUniqueIndexConflict:
		return "write_conflict"
	}
	switch codeName {
	case "WriteConcernFailed":
		return "write_concern_failed"
	case "NotWritablePrimary":
		return "not_leader"
	case "ShutdownInProgress":
		return "durability_unavailable"
	case "DuplicateKey":
		return "write_conflict"
	default:
		return "cluster_error"
	}
}

func appendMongoClusterRouteErrorFields(fields bson.D, route treenativewire.ClusterRouteErrorMetadata) bson.D {
	appendString := func(key, value string) {
		if value != "" {
			fields = append(fields, bson.E{Key: key, Value: value})
		}
	}
	appendString("treedbRouteGroup", route.GroupID)
	appendString("treedbRouteLeaderHint", route.LeaderHint)
	appendString("treedbRouteShape", route.Shape)
	appendString("treedbRoutePlacementMode", route.PlacementMode)
	appendString("treedbRouteKey", route.RouteKey)
	appendString("treedbRoutePartitionId", route.PartitionID)
	appendString("treedbRouteLocalGroup", route.LocalGroupID)
	if len(route.Members) != 0 {
		members := make(bson.A, len(route.Members))
		for i, member := range route.Members {
			members[i] = member
		}
		fields = append(fields, bson.E{Key: "treedbRouteMembers", Value: members})
	}
	if route.TokenKnown {
		fields = append(fields, bson.E{Key: "treedbRouteTokenKnown", Value: true})
		fields = append(fields, bson.E{Key: "treedbRouteToken", Value: strconv.FormatUint(route.Token, 10)})
	}
	return fields
}

func mongoClusterRouteRejectedMessage(message string) bool {
	return strings.Contains(message, "route group") ||
		strings.Contains(message, "route_error_class=") ||
		strings.Contains(message, "route_class=") ||
		strings.Contains(message, "cluster route rejected") ||
		strings.Contains(message, "cluster query route shape") ||
		strings.Contains(message, "cluster route shape") ||
		strings.Contains(message, "cluster token route") ||
		strings.Contains(message, "cluster token batch route") ||
		strings.Contains(message, "routed-cluster") ||
		strings.Contains(message, "cluster route target") ||
		strings.Contains(message, "route target") ||
		strings.Contains(message, "owner route proof") ||
		strings.Contains(message, "route request") ||
		strings.Contains(message, "requires fanout before submit") ||
		strings.Contains(message, "requires command split before submit")
}

func mongoClusterLeaderHint(err error) string {
	if err == nil {
		return ""
	}
	if route, ok := treenativewire.ClusterRouteErrorMetadataOf(err); ok {
		if leaderHint := strings.TrimSpace(route.LeaderHint); leaderHint != "" {
			return leaderHint
		}
	}
	var hinted interface {
		ClusterLeaderHint() string
	}
	if errors.As(err, &hinted) {
		if leaderHint := strings.TrimSpace(hinted.ClusterLeaderHint()); leaderHint != "" {
			return leaderHint
		}
	}
	const marker = "leader_hint="
	message := err.Error()
	start := strings.Index(message, marker)
	if start < 0 {
		return ""
	}
	start += len(marker)
	end := start
	for end < len(message) {
		switch message[end] {
		case ';', ',', ' ', '\t', '\n', '\r':
			return strings.TrimSpace(message[start:end])
		default:
			end++
		}
	}
	return strings.TrimSpace(message[start:end])
}

func mongoClusterMutationCommandError(err error) (wire.Document, error) {
	return mongoClusterRouteCommandError(err)
}

type mongoCommandError struct {
	code     int32
	codeName string
	message  string
}

func (e mongoCommandError) Error() string {
	return e.message
}

func mongoWriteConcernFailedError(message string) error {
	return mongoCommandError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: message}
}

func mongoClusterUnsupportedLocalMutation(command string) (wire.Document, error) {
	return commandError(
		commandCodeBadValue,
		"BadValue",
		"Mongo gateway cluster submitter mode does not support local "+command+" mutation",
	)
}

func mongoClusterUnsupportedIndexDDL() (wire.Document, error) {
	return commandError(
		commandCodeBadValue,
		"BadValue",
		"Mongo gateway cluster mode does not support secondary or global unique index DDL; "+
			"shard-local index ownership and global unique coordination are not implemented",
	)
}

func mongoClusterMissingCollectionMetadataError() error {
	return &iwire.ProtocolError{
		Code: iwire.ErrReadOnly,
		Reason: "Mongo gateway token/ring mutation is disabled until authoritative collection and index metadata " +
			"is bound to the owner route proof",
	}
}

func (s *Server) rejectClusterRoutedLocalMetadataRead(command string) (wire.Document, error, bool) {
	if !s.clusterRouteProviderConfigured() {
		return nil, nil, false
	}
	doc, err := mongoClusterRouteCommandError(&iwire.ProtocolError{
		Code: iwire.ErrReadOnly,
		Reason: "Mongo gateway routed-cluster " + command + " is disabled until authoritative catalog metadata " +
			"is bound to the route provider",
	})
	return doc, err, true
}
