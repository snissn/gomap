package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (s *Server) findAndModifyResponse(ctx context.Context, command wire.Document) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if s.clusterSubmitterConfigured() {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway cluster findAndModify is not supported")
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "findAndModify"); rejected {
		return doc, err
	}
	if err := validateFindAndModifyCommand(command); err != nil {
		return mongoClusterRouteCommandError(err)
	}
	collection, err := commandString(command, "findAndModify")
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
	query, err := requiredDocumentField(command, "query")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	update, err := requiredDocumentField(command, "update")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	upsert, err := optionalBoolField(command, "upsert")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	newImage, err := optionalBoolField(command, "new")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	fields, err := commandOptionalDocument(command, "fields")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	projection, err := compileProjection(fields)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	itemDoc, err := marshalDocument(bson.D{{Key: "q", Value: bson.Raw(query)}, {Key: "u", Value: bson.Raw(update)}, {Key: "upsert", Value: upsert}})
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	item, err := parseMongoUpdateItem(0, itemDoc)
	if err != nil {
		return mongoUpdateParseCommandError(err)
	}
	item.selector = s
	col, err := s.openCollectionForMutation(name)
	var releaseColdCollection func()
	defer func() {
		if releaseColdCollection != nil {
			releaseColdCollection()
		}
	}()
	if errors.Is(err, collections.ErrCollectionNotFound) {
		if !upsert {
			return marshalFindAndModifyResponse(nil, false, false, bson.RawValue{}, projection)
		}
		if err := s.validateMongoMissingCollectionFirstUpsert([]mongoUpdateItem{item}); err != nil {
			return mongoUpdateWriteCommandError(err)
		}
		if err := preflightFindAndModifyUpsertResponse(item, newImage, projection, s.maxMessageLength()); err != nil {
			return mongoUpdateWriteCommandError(err)
		}
		col, releaseColdCollection, err = s.openOrCreateCollectionForFirstWrite(name)
	}
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	before, after, matched, err := s.findAndModifyExisting(col, item, newImage, projection)
	if err != nil {
		return mongoUpdateWriteCommandError(err)
	}
	if !matched && upsert {
		doc, err := mongoUpsertDocument(item)
		if err != nil {
			return mongoUpdateWriteCommandError(err)
		}
		key, stored, err := prepareInsertDocument(doc, col.MetaView().Options.DocumentFormat)
		if err != nil {
			return mongoUpdateWriteCommandError(err)
		}
		if !bytes.Equal(key, item.key) {
			return mongoUpdateWriteCommandError(errors.New("Mongo gateway update cannot modify _id"))
		}
		if err := validateFindAndModifyResponse(func() wire.Document {
			if newImage {
				return doc
			}
			return nil
		}(), false, true, item.id, projection, s.maxMessageLength()); err != nil {
			return mongoUpdateWriteCommandError(err)
		}
		if s.findAndModifyBeforeUpsertInsertHook != nil {
			s.findAndModifyBeforeUpsertInsertHook()
		}
		if _, err := col.Insert(key, stored); err != nil {
			if !errors.Is(err, collections.ErrDocumentExists) {
				return mongoUpdateWriteCommandError(err)
			}
			response, err := s.findAndModifyAfterInsertConflict(col, item, newImage, projection)
			if err != nil {
				return mongoUpdateWriteCommandError(err)
			}
			return response, nil
		}
		if !newImage {
			doc = nil
		}
		return marshalFindAndModifyResponse(doc, false, true, item.id, projection)
	}
	if !matched {
		return marshalFindAndModifyResponse(nil, false, false, bson.RawValue{}, projection)
	}
	if newImage {
		before = after
	}
	return marshalFindAndModifyResponse(before, true, false, bson.RawValue{}, projection)
}

// preflightFindAndModifyUpsertResponse is the only response admission that
// may run before catalog creation: an absent exact-ID upsert's response image
// is deterministic from the already parsed command. Existing-document images
// are admitted inside Collection.Update so the checked image and publication
// share one atomic callback.
func preflightFindAndModifyUpsertResponse(item mongoUpdateItem, newImage bool, projection compiledProjection, maxMessageLength int32) error {
	doc, err := mongoUpsertDocument(item)
	if err != nil {
		return err
	}
	if !newImage {
		doc = nil
	}
	return validateFindAndModifyResponse(doc, false, true, item.id, projection, maxMessageLength)
}

func validateFindAndModifyResponse(value wire.Document, updatedExisting, upserted bool, id bson.RawValue, projection compiledProjection, maxMessageLength int32) error {
	response, err := marshalFindAndModifyResponse(value, updatedExisting, upserted, id, projection)
	if err != nil {
		return err
	}
	// OP_MSG is a fixed 16-byte header, four flag bytes, one kind-0 section
	// discriminator, and the already validated BSON response. Avoid allocating
	// a potentially huge temporary message merely to reject it by size.
	if wire.HeaderLen+4+1+len(response) > int(maxMessageLength) {
		// Keep the known pre-mutation rejection representable at the gateway's
		// minimum response envelope as well.
		return errors.New("findAndModify response too large")
	}
	return nil
}

func findAndModifyUpdatedImage(col *collections.Collection, item mongoUpdateItem, before wire.Document) (wire.Document, error) {
	var (
		updated wire.Document
		err     error
	)
	if item.pureSet && normalizedMongoUpdateDocumentFormat(col) == collections.DocumentFormatBSON && item.bsonSetFieldsOK {
		updated, _, err = applyBSONSetUpdate(before, item.bsonSetFields)
	} else if item.pureSet {
		updated, _, err = applySetUpdate(before, item.updateDoc)
	} else {
		updated, _, err = applyMongoMutation(before, item.mutation)
	}
	if err != nil {
		return nil, err
	}
	updatedKey, _, err := prepareInsertDocument(updated, col.MetaView().Options.DocumentFormat)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(updatedKey, item.key) {
		return nil, errors.New("Mongo gateway update cannot modify _id")
	}
	return updated, nil
}

func (s *Server) findAndModifyAfterInsertConflict(col *collections.Collection, item mongoUpdateItem, newImage bool, projection compiledProjection) (wire.Document, error) {
	before, after, matched, err := s.findAndModifyExisting(col, item, newImage, projection)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, errors.New("Mongo gateway findAndModify upsert lost concurrent insert")
	}
	if newImage {
		before = after
	}
	return marshalFindAndModifyResponse(before, true, false, bson.RawValue{}, projection)
}

func validateFindAndModifyCommand(command wire.Document) error {
	elements, err := bson.Raw(command).Elements()
	if err != nil {
		return err
	}
	for _, elem := range elements {
		key, _ := elem.KeyErr()
		if isMongoCommandMetadataField(key) {
			continue
		}
		switch key {
		case "findAndModify", "query", "update", "new", "upsert", "fields", "remove":
		case "sort", "arrayFilters", "hint", "collation":
			return fmt.Errorf("Mongo gateway findAndModify does not support option %q", key)
		default:
			return fmt.Errorf("Mongo gateway findAndModify does not support option %q", key)
		}
	}
	if remove, err := optionalBoolField(command, "remove"); err != nil {
		return err
	} else if remove {
		return errors.New("Mongo gateway findAndModify does not support remove: true")
	}
	return nil
}

func (s *Server) findAndModifyExisting(col *collections.Collection, item mongoUpdateItem, newImage bool, projection compiledProjection) (before, after wire.Document, matched bool, err error) {
	if !item.exactID {
		if item.selector == nil {
			return nil, nil, false, errors.New("Mongo gateway filter findAndModify has no selector")
		}
		return item.selector.findAndModifyFilterExisting(col, item, newImage, projection)
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return nil, nil, false, err
	}
	if materializer != nil {
		defer materializer.Close()
	}
	matched, _, err = col.Update(item.key, func(stored []byte) ([]byte, bool, error) {
		raw, err := storedDocumentToBSON(col, materializer, stored)
		if err != nil {
			return nil, false, err
		}
		matched, before = true, bytes.Clone(raw)
		var updated wire.Document
		var changed bool
		if item.pureSet && normalizedMongoUpdateDocumentFormat(col) == collections.DocumentFormatBSON && item.bsonSetFieldsOK {
			updated, changed, err = applyBSONSetUpdate(raw, item.bsonSetFields)
		} else if item.pureSet {
			updated, changed, err = applySetUpdate(raw, item.updateDoc)
		} else {
			updated, changed, err = applyMongoMutation(raw, item.mutation)
		}
		if err != nil {
			return nil, false, err
		}
		updatedKey, encoded, err := prepareInsertDocument(updated, col.MetaView().Options.DocumentFormat)
		if err != nil {
			return nil, false, err
		}
		if !bytes.Equal(updatedKey, item.key) {
			return nil, false, errors.New("Mongo gateway update cannot modify _id")
		}
		after = bytes.Clone(updated)
		value := before
		if newImage {
			value = after
		}
		if s.findAndModifyExactAdmissionHook != nil {
			s.findAndModifyExactAdmissionHook()
		}
		// This runs in the same conditional mutation callback as the actual
		// materialization. A concurrent larger before/after image therefore
		// fails before changed=true can publish a document or command-WAL root.
		if err := validateFindAndModifyResponse(value, true, false, bson.RawValue{}, projection, s.maxMessageLength()); err != nil {
			return nil, false, err
		}
		if !changed {
			return nil, false, nil
		}
		return encoded, true, nil
	})
	return finalizeFindAndModifyImages(before, after, matched, err)
}

func finalizeFindAndModifyImages(before, after wire.Document, matched bool, err error) (wire.Document, wire.Document, bool, error) {
	if err != nil || !matched {
		return nil, nil, matched, err
	}
	return before, after, matched, nil
}

func marshalFindAndModifyResponse(value wire.Document, updatedExisting, upserted bool, id bson.RawValue, projection compiledProjection) (wire.Document, error) {
	var output any
	if value != nil {
		var err error
		output, err = projectDocumentWithProjection(value, projection)
		if err != nil {
			return nil, err
		}
	}
	last := bson.D{{Key: "n", Value: int32(0)}, {Key: "updatedExisting", Value: updatedExisting}}
	if updatedExisting || upserted {
		last[0].Value = int32(1)
	}
	if upserted {
		last = append(last, bson.E{Key: "upserted", Value: id})
	}
	return marshalDocument(bson.D{{Key: "lastErrorObject", Value: last}, {Key: "value", Value: output}, {Key: "ok", Value: 1.0}})
}
