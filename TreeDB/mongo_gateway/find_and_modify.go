package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (s *Server) findAndModifyResponse(ctx context.Context, command wire.Document) (wire.Document, error) {
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
	if err := validateFindAndModifyIDQuery(query); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
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
	col, err := s.Collections.OpenCollection(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		if !upsert {
			return marshalFindAndModifyResponse(nil, false, false, bson.RawValue{}, projection)
		}
		if err := s.validateMongoMissingCollectionFirstUpsert([]mongoUpdateItem{item}); err != nil {
			return mongoUpdateWriteCommandError(err)
		}
		col, err = s.openOrCreateCollection(name)
	}
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	before, after, matched, err := findAndModifyExisting(col, item)
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
		if _, err := col.Insert(key, stored); err != nil {
			if !errors.Is(err, collections.ErrDocumentExists) {
				return mongoUpdateWriteCommandError(err)
			}
			response, err := findAndModifyAfterInsertConflict(col, item, newImage, projection)
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

func findAndModifyAfterInsertConflict(col *collections.Collection, item mongoUpdateItem, newImage bool, projection compiledProjection) (wire.Document, error) {
	before, after, matched, err := findAndModifyExisting(col, item)
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

func validateFindAndModifyIDQuery(query wire.Document) error {
	id, err := idEqualityFilterValue(query, "findAndModify")
	if err != nil {
		return err
	}
	if doc, ok := id.DocumentOK(); ok {
		elements, err := doc.Elements()
		if err != nil {
			return err
		}
		for _, elem := range elements {
			key, err := elem.KeyErr()
			if err != nil {
				return err
			}
			if len(key) > 0 && key[0] == '$' {
				return errors.New("Mongo gateway findAndModify currently requires an _id equality filter")
			}
		}
	}
	return nil
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
	if ack, err := parseClusterWriteConcern(command, "findAndModify"); err != nil {
		return mongoWriteConcernFailedError(err.Error())
	} else if ack != iwire.AckVisible {
		return mongoWriteConcernFailedError("Mongo gateway findAndModify cannot satisfy writeConcern majority")
	}
	return nil
}

func findAndModifyExisting(col *collections.Collection, item mongoUpdateItem) (before, after wire.Document, matched bool, err error) {
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
