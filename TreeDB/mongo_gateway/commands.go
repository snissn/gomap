package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	commandCodeBadValue      int32 = 2
	commandCodeFailedToParse int32 = 9
	commandCodeDuplicateKey  int32 = 11000
)

const primaryKeyPrefixBSONValue byte = 1

func (s *Server) insertResponse(command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
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

	ids := make([][]byte, 0, len(documents))
	stored := make([][]byte, 0, len(documents))
	for _, doc := range documents {
		key, encoded, err := prepareInsertDocument(doc)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		ids = append(ids, key)
		stored = append(stored, encoded)
	}

	col, err := s.openOrCreateCollection(name)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if _, err := col.InsertBatch(ids, stored); err != nil {
		code, codeName := commandCodeBadValue, "BadValue"
		if collections.IsDuplicateKeyError(err) {
			code, codeName = commandCodeDuplicateKey, "DuplicateKey"
		}
		return commandError(code, codeName, err.Error())
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: int32(len(documents))},
	})
}

func (s *Server) findResponse(command wire.Document) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	collection, err := commandString(command, "find")
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
	filter, err := commandOptionalDocument(command, "filter")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if filter == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway find currently requires an _id equality filter")
	}
	id, err := idEqualityFilterValue(filter, "find")
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	key, err := encodePrimaryKey(id)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	firstBatch := bson.A{}
	col, err := s.Collections.OpenCollection(name)
	if err == nil {
		stored, err := col.Get(key)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if len(stored) > 0 {
			doc, err := storedDocumentToBSON(stored)
			if err != nil {
				return commandError(commandCodeBadValue, "BadValue", err.Error())
			}
			firstBatch = append(firstBatch, bson.Raw(doc))
		}
	} else if !errors.Is(err, collections.ErrCollectionNotFound) {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	return marshalDocument(bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: db + "." + collection},
			{Key: "firstBatch", Value: firstBatch},
		}},
		{Key: "ok", Value: 1.0},
	})
}

func (s *Server) updateResponse(command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
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

	var matched int32
	var modified int32
	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return marshalUpdateResponse(0, 0)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	for i, update := range updates {
		filter, err := requiredDocumentField(update, "q")
		if err != nil {
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("updates[%d]: %v", i, err))
		}
		id, err := idEqualityFilterValue(filter, "update")
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: %v", i, err))
		}
		key, err := encodePrimaryKey(id)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: %v", i, err))
		}
		if multi, err := optionalBoolField(update, "multi"); err != nil {
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("updates[%d]: %v", i, err))
		} else if multi {
			return commandError(commandCodeBadValue, "BadValue", "Mongo gateway update currently supports updateOne only")
		}
		if upsert, err := optionalBoolField(update, "upsert"); err != nil {
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("updates[%d]: %v", i, err))
		} else if upsert {
			return commandError(commandCodeBadValue, "BadValue", "Mongo gateway update currently does not support upsert")
		}
		updateDoc, err := requiredDocumentField(update, "u")
		if err != nil {
			return commandError(commandCodeFailedToParse, "FailedToParse", fmt.Sprintf("updates[%d]: %v", i, err))
		}

		stored, err := col.Get(key)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if len(stored) == 0 {
			continue
		}
		raw, err := storedDocumentToBSON(stored)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		updated, changed, err := applySetUpdate(raw, updateDoc)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: %v", i, err))
		}
		updatedKey, encoded, err := prepareInsertDocument(updated)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("updates[%d]: %v", i, err))
		}
		if !bytes.Equal(updatedKey, key) {
			return commandError(commandCodeBadValue, "BadValue", "Mongo gateway update cannot modify _id")
		}
		matched++
		if !changed {
			continue
		}
		replaced, err := col.Replace(key, encoded)
		if err != nil {
			code, codeName := commandCodeBadValue, "BadValue"
			if collections.IsDuplicateKeyError(err) {
				code, codeName = commandCodeDuplicateKey, "DuplicateKey"
			}
			return commandError(code, codeName, err.Error())
		}
		if replaced {
			modified++
		}
	}
	return marshalUpdateResponse(matched, modified)
}

func (s *Server) deleteResponse(command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
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

	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return marshalDeleteResponse(0)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	var deleted int32
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
		stored, err := col.Get(key)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if len(stored) == 0 {
			continue
		}
		if err := col.Delete(key); err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		deleted++
	}
	return marshalDeleteResponse(deleted)
}

func marshalUpdateResponse(matched, modified int32) (wire.Document, error) {
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: matched},
		{Key: "nModified", Value: modified},
	})
}

func marshalDeleteResponse(deleted int32) (wire.Document, error) {
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: deleted},
	})
}

func (s *Server) openOrCreateCollection(name string) (*collections.Collection, error) {
	col, err := s.Collections.OpenCollection(name)
	if err == nil {
		return col, nil
	}
	if _, createErr := s.Collections.CreateCollection(&collections.CollectionMeta{Name: name}); createErr != nil {
		return nil, createErr
	}
	return s.Collections.OpenCollection(name)
}

func commandString(doc wire.Document, key string) (string, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return "", fmt.Errorf("Mongo command missing %q", key)
	}
	out, ok := value.StringValueOK()
	if !ok {
		return "", fmt.Errorf("Mongo command field %q must be a string", key)
	}
	if out == "" {
		return "", fmt.Errorf("Mongo command field %q cannot be empty", key)
	}
	return out, nil
}

func commandOptionalDocument(doc wire.Document, key string) (wire.Document, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return nil, nil
	}
	out, ok := value.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("Mongo command field %q must be a document", key)
	}
	return wire.Document(out), nil
}

func requiredDocumentField(doc wire.Document, key string) (wire.Document, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return nil, fmt.Errorf("Mongo command missing %q", key)
	}
	out, ok := value.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("Mongo command field %q must be a document", key)
	}
	return wire.Document(out), nil
}

func optionalBoolField(doc wire.Document, key string) (bool, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return false, nil
	}
	out, ok := value.BooleanOK()
	if !ok {
		return false, fmt.Errorf("Mongo command field %q must be a boolean", key)
	}
	return out, nil
}

func optionalInt32Field(doc wire.Document, key string) (int32, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return 0, nil
	}
	if out, ok := value.Int32OK(); ok {
		return out, nil
	}
	if out, ok := value.Int64OK(); ok {
		if out < 0 || out > int64(^uint32(0)>>1) {
			return 0, fmt.Errorf("Mongo command field %q is out of int32 range", key)
		}
		return int32(out), nil
	}
	return 0, fmt.Errorf("Mongo command field %q must be an integer", key)
}

func commandDocumentArray(doc wire.Document, key string) ([]wire.Document, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return nil, fmt.Errorf("Mongo command missing %q", key)
	}
	array, ok := value.ArrayOK()
	if !ok {
		return nil, fmt.Errorf("Mongo command field %q must be an array", key)
	}
	values, err := array.Values()
	if err != nil {
		return nil, err
	}
	out := make([]wire.Document, 0, len(values))
	for i, value := range values {
		doc, ok := value.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("Mongo command field %q[%d] must be a document", key, i)
		}
		out = append(out, wire.Document(doc))
	}
	return out, nil
}

func commandDocuments(doc wire.Document, sequences []wire.DocumentSequence, key string) ([]wire.Document, error) {
	var sequenceDocs []wire.Document
	for _, seq := range sequences {
		if seq.Identifier != key {
			continue
		}
		if sequenceDocs != nil {
			return nil, fmt.Errorf("Mongo command contains multiple %q document sequences", key)
		}
		sequenceDocs = append([]wire.Document(nil), seq.Documents...)
	}
	arrayValue := bson.Raw(doc).Lookup(key)
	if sequenceDocs != nil {
		if !arrayValue.IsZero() {
			return nil, fmt.Errorf("Mongo command contains both %q array and document sequence", key)
		}
		return sequenceDocs, nil
	}
	return commandDocumentArray(doc, key)
}

func idEqualityFilterValue(filter wire.Document, commandName string) (bson.RawValue, error) {
	elements, err := bson.Raw(filter).Elements()
	if err != nil {
		return bson.RawValue{}, err
	}
	if len(elements) != 1 {
		return bson.RawValue{}, fmt.Errorf("Mongo gateway %s currently supports exactly one _id equality predicate", commandName)
	}
	key, err := elements[0].KeyErr()
	if err != nil {
		return bson.RawValue{}, err
	}
	if key != "_id" {
		return bson.RawValue{}, fmt.Errorf("Mongo gateway %s currently requires an _id equality filter", commandName)
	}
	return elements[0].Value(), nil
}

func gatewayCollectionName(db, collection string) (string, error) {
	if db == "" {
		return "", errors.New("Mongo database name cannot be empty")
	}
	if collection == "" {
		return "", errors.New("Mongo collection name cannot be empty")
	}
	if strings.ContainsAny(db, "\x00/:") {
		return "", errors.New("Mongo database name contains reserved punctuation")
	}
	name := db + "." + collection
	if err := collections.ValidateCollectionName(name); err != nil {
		return "", err
	}
	return name, nil
}

// prepareInsertDocument uses canonical Extended JSON as the temporary collection
// storage bridge so BSON types can round-trip before a native BSON format exists.
func prepareInsertDocument(doc wire.Document) ([]byte, []byte, error) {
	if err := wire.ValidateDocument(doc); err != nil {
		return nil, nil, err
	}
	raw := bson.Raw(doc)
	if err := validateSupportedDocument(raw); err != nil {
		return nil, nil, err
	}
	if raw.Lookup("_id").IsZero() {
		var decoded bson.D
		if err := bson.Unmarshal(raw, &decoded); err != nil {
			return nil, nil, err
		}
		decoded = append(bson.D{{Key: "_id", Value: bson.NewObjectID()}}, decoded...)
		encoded, err := bson.Marshal(decoded)
		if err != nil {
			return nil, nil, err
		}
		raw = bson.Raw(encoded)
	}
	key, err := encodePrimaryKey(raw.Lookup("_id"))
	if err != nil {
		return nil, nil, err
	}
	stored, err := bson.MarshalExtJSON(raw, true, false)
	if err != nil {
		return nil, nil, err
	}
	return key, stored, nil
}

func storedDocumentToBSON(stored []byte) (wire.Document, error) {
	var raw bson.Raw
	if err := bson.UnmarshalExtJSON(stored, true, &raw); err != nil {
		return nil, err
	}
	return wire.Document(raw), nil
}

func applySetUpdate(doc wire.Document, update wire.Document) (wire.Document, bool, error) {
	updateElements, err := bson.Raw(update).Elements()
	if err != nil {
		return nil, false, err
	}
	if len(updateElements) != 1 {
		return nil, false, errors.New("Mongo gateway update currently supports exactly one $set operator")
	}
	operator, err := updateElements[0].KeyErr()
	if err != nil {
		return nil, false, err
	}
	if operator != "$set" {
		return nil, false, errors.New("Mongo gateway update currently supports $set only")
	}
	setDoc, ok := updateElements[0].Value().DocumentOK()
	if !ok {
		return nil, false, errors.New("Mongo gateway $set value must be a document")
	}
	sets, setOrder, err := parseSetDocument(setDoc)
	if err != nil {
		return nil, false, err
	}
	if len(sets) == 0 {
		return doc, false, nil
	}

	elements, err := bson.Raw(doc).Elements()
	if err != nil {
		return nil, false, err
	}
	out := make(bson.D, 0, len(elements)+len(sets))
	used := make(map[string]struct{}, len(sets))
	changed := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, false, err
		}
		value := elem.Value()
		if replacement, ok := sets[key]; ok {
			if !replacement.Equal(value) {
				changed = true
			}
			value = replacement
			used[key] = struct{}{}
		}
		out = append(out, bson.E{Key: key, Value: value})
	}
	for _, key := range setOrder {
		if _, ok := used[key]; ok {
			continue
		}
		out = append(out, bson.E{Key: key, Value: sets[key]})
		changed = true
	}
	raw, err := bson.Marshal(out)
	if err != nil {
		return nil, false, err
	}
	return wire.Document(raw), changed, nil
}

func parseSetDocument(doc bson.Raw) (map[string]bson.RawValue, []string, error) {
	elements, err := doc.Elements()
	if err != nil {
		return nil, nil, err
	}
	sets := make(map[string]bson.RawValue, len(elements))
	order := make([]string, 0, len(elements))
	seen := make(map[string]struct{}, len(elements))
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, nil, err
		}
		if key == "" {
			return nil, nil, errors.New("Mongo gateway $set field name cannot be empty")
		}
		if key == "_id" {
			return nil, nil, errors.New("Mongo gateway update cannot modify _id")
		}
		if strings.Contains(key, ".") {
			return nil, nil, errors.New("Mongo gateway $set currently supports top-level fields only")
		}
		if strings.HasPrefix(key, "$") {
			return nil, nil, errors.New("Mongo gateway $set field names cannot start with $")
		}
		value := elem.Value()
		if err := validateSupportedValue(key, value); err != nil {
			return nil, nil, err
		}
		if _, ok := seen[key]; !ok {
			order = append(order, key)
			seen[key] = struct{}{}
		}
		sets[key] = value
	}
	sort.Strings(order)
	return sets, order, nil
}

func encodePrimaryKey(value bson.RawValue) ([]byte, error) {
	if value.IsZero() {
		return nil, errors.New("Mongo document _id is required")
	}
	if value.Type == bson.TypeArray {
		return nil, errors.New("Mongo document _id cannot be an array")
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	key := make([]byte, 0, 2+len(value.Value))
	key = append(key, primaryKeyPrefixBSONValue, byte(value.Type))
	key = append(key, value.Value...)
	return key, nil
}

func validateSupportedDocument(doc bson.Raw) error {
	return validateSupportedDocumentAt("", doc)
}

func validateSupportedDocumentAt(prefix string, doc bson.Raw) error {
	elements, err := doc.Elements()
	if err != nil {
		return err
	}
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return err
		}
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if err := validateSupportedValue(path, elem.Value()); err != nil {
			return err
		}
	}
	return nil
}

func validateSupportedValue(path string, value bson.RawValue) error {
	switch value.Type {
	case bson.TypeDouble, bson.TypeString, bson.TypeObjectID, bson.TypeBoolean, bson.TypeNull, bson.TypeInt32, bson.TypeInt64:
		return value.Validate()
	case bson.TypeEmbeddedDocument:
		doc, ok := value.DocumentOK()
		if !ok {
			return fmt.Errorf("Mongo document field %q is not a valid embedded document", path)
		}
		return validateSupportedDocumentAt(path, doc)
	case bson.TypeArray:
		array, ok := value.ArrayOK()
		if !ok {
			return fmt.Errorf("Mongo document field %q is not a valid array", path)
		}
		values, err := array.Values()
		if err != nil {
			return err
		}
		for i, item := range values {
			if err := validateSupportedValue(fmt.Sprintf("%s.%d", path, i), item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("Mongo document field %q uses unsupported BSON type %s", path, value.Type)
	}
}

func commandError(code int32, codeName, message string) (wire.Document, error) {
	message = strings.TrimSpace(message)
	if message == "" {
		message = codeName
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: message},
		{Key: "code", Value: code},
		{Key: "codeName", Value: codeName},
	})
}
