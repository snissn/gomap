package mongogateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const (
	commandCodeBadValue            int32 = 2
	commandCodeFailedToParse       int32 = 9
	commandCodeNamespaceNotFound   int32 = 26
	commandCodeIndexNotFound       int32 = 27
	commandCodeCursorNotFound      int32 = 43
	commandCodeDuplicateKey        int32 = 11000
	maxWireMessageLengthInt32Limit       = int64(1<<31 - 1)
)

const primaryKeyPrefixBSONValue byte = 1

var maxInt = int(^uint(0) >> 1)

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

	var col *collections.Collection
	format := s.DefaultCollectionOptions.DocumentFormat
	if existing, err := s.Collections.OpenCollection(name); err == nil {
		col = existing
		format = existing.Meta().Options.DocumentFormat
	} else if !errors.Is(err, collections.ErrCollectionNotFound) {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	ids, stored, err := prepareInsertDocuments(documents, format)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if col == nil {
		col, err = s.openOrCreateCollection(name)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if actualFormat := col.Meta().Options.DocumentFormat; actualFormat != format {
			ids, stored, err = prepareInsertDocuments(documents, actualFormat)
			if err != nil {
				return commandError(commandCodeBadValue, "BadValue", err.Error())
			}
			format = actualFormat
		}
	}
	if format == collections.DocumentFormatBSON {
		_, err = col.InsertBatchValidatedBSON(ids, stored)
	} else {
		_, err = col.InsertBatch(ids, stored)
	}
	if err != nil {
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

func prepareInsertDocuments(documents []wire.Document, format collections.DocumentFormat) ([][]byte, [][]byte, error) {
	ids := make([][]byte, len(documents))
	stored := make([][]byte, len(documents))
	for i, doc := range documents {
		key, encoded, err := prepareInsertDocument(doc, format)
		if err != nil {
			return nil, nil, err
		}
		ids[i] = key
		stored[i] = encoded
	}
	return ids, stored, nil
}

type rawCursorDocumentsResponse struct {
	ns       string
	cursorID int64
	batchKey string
	batch    []wire.Document
}

type findResponsePayloadKind uint8

const (
	findResponsePayloadDocument findResponsePayloadKind = iota
	findResponsePayloadRaw
	findResponsePayloadIndexedRange
)

type findResponsePayload struct {
	kind         findResponsePayloadKind
	document     wire.Document
	raw          rawCursorDocumentsResponse
	indexedRange indexedRangeCursorResponse
}

func (p findResponsePayload) marshalDocument() (wire.Document, error) {
	switch p.kind {
	case findResponsePayloadRaw:
		return marshalCursorDocumentsResponseWithID(p.raw.ns, p.raw.cursorID, p.raw.batchKey, p.raw.batch)
	case findResponsePayloadIndexedRange:
		return p.indexedRange.marshalDocument()
	default:
		return p.documentPayload()
	}
}

func (p findResponsePayload) marshalMsg(requestID, responseTo int32) ([]byte, error) {
	return p.marshalMsgIntoWithMaxLength(nil, requestID, responseTo, wire.DefaultMaxMessageLength)
}

func (p findResponsePayload) marshalMsgInto(dst []byte, requestID, responseTo int32) ([]byte, error) {
	return p.marshalMsgIntoWithMaxLength(dst, requestID, responseTo, wire.DefaultMaxMessageLength)
}

func (p findResponsePayload) marshalMsgIntoWithMaxLength(dst []byte, requestID, responseTo int32, maxMessageLength int) ([]byte, error) {
	switch p.kind {
	case findResponsePayloadRaw:
		return marshalCursorDocumentsMsgResponseWithIDInto(dst, requestID, responseTo, p.raw.ns, p.raw.cursorID, p.raw.batchKey, p.raw.batch, maxMessageLength)
	case findResponsePayloadIndexedRange:
		return p.indexedRange.marshalMsgIntoWithMaxLength(dst, requestID, responseTo, maxMessageLength)
	default:
		if _, err := p.documentPayload(); err != nil {
			return dst, err
		}
		if maxMessageLength <= 0 || maxMessageLength > wire.DefaultMaxMessageLength {
			maxMessageLength = wire.DefaultMaxMessageLength
		}
		base := len(dst)
		msg, err := wire.AppendMsgMessage(dst, requestID, responseTo, 0, p.document)
		if err != nil {
			return dst[:base], err
		}
		messageLength := len(msg) - base
		if messageLength > maxMessageLength {
			return msg[:base], fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, messageLength, maxMessageLength)
		}
		return msg, nil
	}
}

func (p findResponsePayload) documentPayload() (wire.Document, error) {
	if p.document == nil {
		return nil, errors.New("mongo gateway: find response payload missing document")
	}
	if p.raw.ns != "" || p.raw.cursorID != 0 || p.raw.batchKey != "" || p.raw.batch != nil ||
		p.indexedRange.col != nil || p.indexedRange.server != nil || p.indexedRange.ns != "" ||
		p.indexedRange.indexName != "" || p.indexedRange.batchKey != "" || p.indexedRange.maxBatchBytes != 0 ||
		p.indexedRange.cursorOwner != 0 || p.indexedRange.singleBatch || !zeroIndexRangeOptions(p.indexedRange.opts) {
		return nil, fmt.Errorf("mongo gateway: find response payload kind mismatch: kind=%d raw.ns=%q raw.cursorID=%d raw.batchKey=%q raw.batch=%t indexedRange.ns=%q indexedRange.indexName=%q indexedRange.batchKey=%q indexedRange.maxBatchBytes=%d indexedRange.cursorOwner=%d indexedRange.singleBatch=%t indexedRange.optsSet=%t indexedRange.col=%t indexedRange.server=%t",
			p.kind, p.raw.ns, p.raw.cursorID, p.raw.batchKey, p.raw.batch != nil, p.indexedRange.ns, p.indexedRange.indexName, p.indexedRange.batchKey, p.indexedRange.maxBatchBytes, p.indexedRange.cursorOwner, p.indexedRange.singleBatch, !zeroIndexRangeOptions(p.indexedRange.opts), p.indexedRange.col != nil, p.indexedRange.server != nil)
	}
	return p.document, nil
}

func zeroIndexRangeOptions(opts collections.IndexRangeOptions) bool {
	return opts.Limit == 0 && !opts.Desc && opts.Lower.Value == nil && !opts.Lower.Inclusive && !opts.Lower.Unbounded &&
		opts.Upper.Value == nil && !opts.Upper.Inclusive && !opts.Upper.Unbounded
}

func (s *Server) findResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	payload, err := s.findResponsePayload(command, cursorOwner)
	if err != nil {
		return nil, err
	}
	doc, err := payload.marshalDocument()
	if err != nil && payload.kind == findResponsePayloadIndexedRange {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return doc, err
}

func (s *Server) findMsgResponse(command wire.Document, requestID, responseTo int32, cursorOwner int64) ([]byte, error) {
	return s.findMsgResponseInto(nil, command, requestID, responseTo, cursorOwner)
}

func (s *Server) findMsgResponseInto(dst []byte, command wire.Document, requestID, responseTo int32, cursorOwner int64) ([]byte, error) {
	payload, err := s.findResponsePayload(command, cursorOwner)
	if err != nil {
		return nil, err
	}
	base := len(dst)
	msg, err := payload.marshalMsgIntoWithMaxLength(dst, requestID, responseTo, int(s.maxMessageLength()))
	if err != nil && payload.kind == findResponsePayloadIndexedRange {
		doc, docErr := commandError(commandCodeBadValue, "BadValue", err.Error())
		if docErr != nil {
			return nil, docErr
		}
		return wire.AppendMsgMessage(dst[:base], requestID, responseTo, 0, doc)
	}
	return msg, err
}

func (s *Server) findResponsePayload(command wire.Document, cursorOwner int64) (findResponsePayload, error) {
	if s.Collections == nil {
		doc, err := commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
		return findResponsePayload{document: doc}, err
	}
	collection, err := commandString(command, "find")
	if err != nil {
		doc, err := commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return findResponsePayload{document: doc}, err
	}
	db, err := commandString(command, "$db")
	if err != nil {
		doc, err := commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return findResponsePayload{document: doc}, err
	}
	name, err := gatewayCollectionName(db, collection)
	if err != nil {
		doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
		return findResponsePayload{document: doc}, err
	}
	filter, err := commandOptionalDocument(command, "filter")
	if err != nil {
		doc, err := commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return findResponsePayload{document: doc}, err
	}
	plan, err := parseFindPlan(command, filter)
	if err != nil {
		doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
		return findResponsePayload{document: doc}, err
	}
	col, err := s.Collections.OpenCollection(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		doc, err := marshalCursorResponse(db, collection, bson.A{})
		return findResponsePayload{document: doc}, err
	}
	if err != nil {
		doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
		return findResponsePayload{document: doc}, err
	}
	batchSize, batchSizeSet, err := optionalInt32FieldWithPresence(command, "batchSize")
	if err != nil {
		doc, err := commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return findResponsePayload{document: doc}, err
	}
	singleBatch, err := optionalBoolField(command, "singleBatch")
	if err != nil {
		doc, err := commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return findResponsePayload{document: doc}, err
	}
	ns := db + "." + collection
	if !plan.projection.present {
		idx, opts, limit, ok, empty, err := pureIndexedRangeLimitPlan(col.Meta(), plan, s.maxFindScanDocuments())
		if err != nil {
			doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
			return findResponsePayload{document: doc}, err
		}
		if ok {
			normalizedBatchSize, err := normalizeBatchSize(int(batchSize), batchSizeSet, defaultCursorBatchSize)
			if err != nil {
				doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
				return findResponsePayload{document: doc}, err
			}
			if empty {
				return findResponsePayload{
					kind: findResponsePayloadRaw,
					raw:  rawCursorDocumentsResponse{ns: ns, cursorID: 0, batchKey: "firstBatch"},
				}, nil
			}
			if normalizedBatchSize >= limit {
				opts.Limit = limit
				return findResponsePayload{
					kind: findResponsePayloadIndexedRange,
					indexedRange: indexedRangeCursorResponse{
						col:           col,
						server:        s,
						ns:            ns,
						indexName:     idx.Name,
						opts:          opts,
						batchKey:      "firstBatch",
						maxBatchBytes: s.maxFindBatchBytes(),
						cursorOwner:   cursorOwner,
						singleBatch:   singleBatch,
					},
				}, nil
			}
		}
	}
	results, err := s.executeFind(col, plan)
	if err != nil {
		doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
		return findResponsePayload{document: doc}, err
	}
	if singleBatch {
		normalizedBatchSize, err := normalizeBatchSize(int(batchSize), batchSizeSet, defaultCursorBatchSize)
		if err != nil {
			doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
			return findResponsePayload{document: doc}, err
		}
		if !results.projection.present {
			consumed, err := rawDocumentsBatchLimit(results.docs, normalizedBatchSize, s.maxFindBatchBytes())
			if err != nil {
				doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
				return findResponsePayload{document: doc}, err
			}
			return findResponsePayload{
				kind: findResponsePayloadRaw,
				raw:  rawCursorDocumentsResponse{ns: ns, cursorID: 0, batchKey: "firstBatch", batch: results.docs[:consumed]},
			}, nil
		}
		firstBatch, _, err := documentsBatchWithLimit(results.docs, results.projection, normalizedBatchSize, s.maxFindBatchBytes())
		if err != nil {
			doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
			return findResponsePayload{document: doc}, err
		}
		doc, err := marshalCursorResponseWithID(ns, 0, "firstBatch", firstBatch)
		return findResponsePayload{document: doc}, err
	}
	if !results.projection.present {
		normalizedBatchSize, err := normalizeBatchSize(int(batchSize), batchSizeSet, defaultCursorBatchSize)
		if err != nil {
			doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
			return findResponsePayload{document: doc}, err
		}
		if normalizedBatchSize >= len(results.docs) {
			consumed, err := rawDocumentsBatchLimit(results.docs, normalizedBatchSize, s.maxFindBatchBytes())
			if err != nil {
				doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
				return findResponsePayload{document: doc}, err
			}
			if consumed >= len(results.docs) {
				return findResponsePayload{
					kind: findResponsePayloadRaw,
					raw:  rawCursorDocumentsResponse{ns: ns, cursorID: 0, batchKey: "firstBatch", batch: results.docs},
				}, nil
			}
		}
	}
	cursorID, firstBatch, err := s.openCursor(ns, results.docs, results.projection, int(batchSize), batchSizeSet, defaultCursorBatchSize, cursorOwner)
	if err != nil {
		doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
		return findResponsePayload{document: doc}, err
	}
	doc, err := marshalCursorResponseWithID(ns, cursorID, "firstBatch", firstBatch)
	return findResponsePayload{document: doc}, err
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

	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return marshalUpdateResponse(0, 0)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	parsed := make([]mongoUpdateItem, 0, len(updates))
	seenKeys := make(map[string]struct{}, len(updates))
	hasDuplicateKey := false
	for i, update := range updates {
		item, err := parseMongoUpdateItem(i, update)
		if err != nil {
			if len(parsed) > 0 {
				if _, _, runErr := runMongoUpdatesSequential(col, parsed); runErr != nil {
					return mongoUpdateWriteCommandError(runErr)
				}
			}
			return mongoUpdateParseCommandError(err)
		}
		keyString := string(item.key)
		if _, ok := seenKeys[keyString]; ok {
			hasDuplicateKey = true
		}
		seenKeys[keyString] = struct{}{}
		parsed = append(parsed, item)
	}
	var matched, modified int32
	if len(parsed) == 1 {
		var matchedOne, modifiedOne bool
		matchedOne, modifiedOne, err = s.runMongoUpdateCoalesced(name, col, parsed[0])
		if matchedOne {
			matched = 1
		}
		if modifiedOne {
			modified = 1
		}
	} else if len(parsed) > 1 && !hasDuplicateKey {
		var batched bool
		matched, modified, batched, err = runMongoUpdateBatch(col, parsed)
		if err != nil || !batched {
			matched, modified, err = runMongoUpdatesSequential(col, parsed)
		}
	} else {
		matched, modified, err = runMongoUpdatesSequential(col, parsed)
	}
	if err != nil {
		return mongoUpdateWriteCommandError(err)
	}
	return marshalUpdateResponse(matched, modified)
}

type mongoUpdateItem struct {
	index       int
	key         []byte
	updateDoc   wire.Document
	setFields   map[string]struct{}
	setFieldsOK bool
}

type mongoUpdateParseError struct {
	code     int32
	codeName string
	message  string
}

func (e mongoUpdateParseError) Error() string {
	return e.message
}

func parseMongoUpdateItem(index int, update wire.Document) (mongoUpdateItem, error) {
	filter, err := requiredDocumentField(update, "q")
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	id, err := idEqualityFilterValue(filter, "update")
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	key, err := encodePrimaryKey(id)
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	if multi, err := optionalBoolField(update, "multi"); err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	} else if multi {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: Mongo gateway update currently supports updateOne only", index)}
	}
	if upsert, err := optionalBoolField(update, "upsert"); err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	} else if upsert {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: Mongo gateway update currently does not support upsert", index)}
	}
	updateDoc, err := requiredDocumentField(update, "u")
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	setFields, setFieldsOK := mongoSetUpdateFields(updateDoc)
	return mongoUpdateItem{index: index, key: key, updateDoc: updateDoc, setFields: setFields, setFieldsOK: setFieldsOK}, nil
}

func mongoUpdateParseCommandError(err error) (wire.Document, error) {
	var parseErr mongoUpdateParseError
	if errors.As(err, &parseErr) {
		return commandError(parseErr.code, parseErr.codeName, parseErr.message)
	}
	return commandError(commandCodeBadValue, "BadValue", err.Error())
}

func mongoUpdateWriteCommandError(err error) (wire.Document, error) {
	code, codeName := commandCodeBadValue, "BadValue"
	if collections.IsDuplicateKeyError(err) {
		code, codeName = commandCodeDuplicateKey, "DuplicateKey"
	}
	return commandError(code, codeName, err.Error())
}

func runMongoUpdatesSequential(col *collections.Collection, updates []mongoUpdateItem) (int32, int32, error) {
	var matched int32
	var modified int32
	for _, update := range updates {
		matchedOne, modifiedOne, err := runMongoUpdateOne(col, update)
		if err != nil {
			return 0, 0, mongoUpdateErrorWithIndex(update.index, err)
		}
		if matchedOne {
			matched++
		}
		if modifiedOne {
			modified++
		}
	}
	return matched, modified, nil
}

func mongoUpdateErrorWithIndex(index int, err error) error {
	if err == nil {
		return nil
	}
	prefix := fmt.Sprintf("updates[%d]:", index)
	if strings.HasPrefix(err.Error(), prefix) {
		return err
	}
	return fmt.Errorf("updates[%d]: %w", index, err)
}

func runMongoUpdateOne(col *collections.Collection, update mongoUpdateItem) (bool, bool, error) {
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return false, false, fmt.Errorf("updates[%d]: %w", update.index, err)
	}
	if materializer != nil {
		defer func() { _ = materializer.Close() }()
	}
	return col.Update(update.key, func(stored []byte) ([]byte, bool, error) {
		return applyMongoUpdateToStoredDocument(col, materializer, update, stored)
	})
}

func runMongoUpdateBatch(col *collections.Collection, updates []mongoUpdateItem) (int32, int32, bool, error) {
	results, batched, err := runMongoUpdateBatchResults(col, updates)
	var matched int32
	var modified int32
	if err != nil || !batched {
		return matched, modified, batched, err
	}
	for _, result := range results {
		if result.Matched {
			matched++
		}
		if result.Modified {
			modified++
		}
	}
	return matched, modified, true, nil
}

func runMongoUpdateBatchResults(col *collections.Collection, updates []mongoUpdateItem) ([]collections.UpdateBatchResult, bool, error) {
	if !mongoUpdateItemsCanUseBatch(col, updates) {
		return make([]collections.UpdateBatchResult, len(updates)), false, nil
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return nil, false, err
	}
	if materializer != nil {
		defer func() { _ = materializer.Close() }()
	}
	items := make([]collections.UpdateBatchItem, len(updates))
	for i, update := range updates {
		update := update
		items[i] = collections.UpdateBatchItem{
			DocumentID: update.key,
			Update: func(stored []byte) ([]byte, bool, error) {
				return applyMongoUpdateToStoredDocument(col, materializer, update, stored)
			},
		}
	}
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges(items)
	if !batched {
		if results == nil {
			results = make([]collections.UpdateBatchResult, len(updates))
		}
		return results, false, err
	}
	if err != nil {
		return nil, true, err
	}
	return results, true, nil
}

func applyMongoUpdateToStoredDocument(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, update mongoUpdateItem, stored []byte) ([]byte, bool, error) {
	raw, err := storedDocumentToBSON(col, materializer, stored)
	if err != nil {
		return nil, false, fmt.Errorf("updates[%d]: %w", update.index, err)
	}
	updated, changed, err := applySetUpdate(raw, update.updateDoc)
	if err != nil {
		return nil, false, fmt.Errorf("updates[%d]: %w", update.index, err)
	}
	updatedKey, encoded, err := prepareInsertDocument(updated, col.Meta().Options.DocumentFormat)
	if err != nil {
		return nil, false, fmt.Errorf("updates[%d]: %w", update.index, err)
	}
	if !bytes.Equal(updatedKey, update.key) {
		return nil, false, fmt.Errorf("updates[%d]: Mongo gateway update cannot modify _id", update.index)
	}
	if !changed {
		return nil, false, nil
	}
	return encoded, true, nil
}

type mongoUpdateCoalescer struct {
	maxDelay  time.Duration
	maxBatch  int
	idleTTL   time.Duration
	requests  chan mongoUpdateCoalescerRequest
	stoppedCh chan struct{}
	done      chan struct{}
	server    *Server
	name      string
	mu        sync.RWMutex
	stopped   bool
	enqueueMu sync.Mutex
}

type mongoUpdateCoalescerRequest struct {
	col  *collections.Collection
	item mongoUpdateItem
	done chan mongoUpdateCoalescerResult
}

type mongoUpdateCoalescerResult struct {
	matched  bool
	modified bool
	err      error
}

func (s *Server) runMongoUpdateCoalesced(name string, col *collections.Collection, update mongoUpdateItem) (bool, bool, error) {
	if !mongoUpdateCanUseBatch(col, update) {
		return runMongoUpdateOne(col, update)
	}
	coalescer := s.mongoUpdateCoalescer(name)
	if coalescer == nil {
		return runMongoUpdateOne(col, update)
	}
	done := make(chan mongoUpdateCoalescerResult, 1)
	if !coalescer.enqueue(mongoUpdateCoalescerRequest{col: col, item: update, done: done}) {
		return runMongoUpdateOne(col, update)
	}
	result := coalescer.waitForUpdateResult(done)
	return result.matched, result.modified, result.err
}

func (c *mongoUpdateCoalescer) waitForUpdateResult(done chan mongoUpdateCoalescerResult) mongoUpdateCoalescerResult {
	select {
	case result := <-done:
		return result
	default:
	}
	if c == nil || c.done == nil {
		return <-done
	}
	select {
	case result := <-done:
		return result
	case <-c.done:
		select {
		case result := <-done:
			return result
		default:
			return mongoUpdateCoalescerResult{err: errors.New("mongo gateway update coalescer stopped before completing request")}
		}
	}
}

func (s *Server) mongoUpdateCoalescer(name string) *mongoUpdateCoalescer {
	if s == nil || s.UpdateCoalescingMaxBatch <= 1 || s.UpdateCoalescingMaxDelay < 0 {
		return nil
	}
	maxBatch := clampUpdateCoalescingMaxBatch(s.UpdateCoalescingMaxBatch)
	if maxBatch <= 1 {
		return nil
	}
	maxDelay := s.UpdateCoalescingMaxDelay
	idleTTL := s.UpdateCoalescingIdleTTL
	if idleTTL == 0 {
		idleTTL = defaultUpdateCoalescingIdleTTL
	}
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	if s.closed.Load() {
		return nil
	}
	if s.updateCoalescers == nil {
		s.updateCoalescers = make(map[string]*mongoUpdateCoalescer)
	}
	if coalescer := s.updateCoalescers[name]; coalescer != nil {
		return coalescer
	}
	coalescer := &mongoUpdateCoalescer{
		maxDelay:  maxDelay,
		maxBatch:  maxBatch,
		idleTTL:   idleTTL,
		requests:  make(chan mongoUpdateCoalescerRequest, maxBatch*4),
		stoppedCh: make(chan struct{}),
		done:      make(chan struct{}),
		server:    s,
		name:      name,
	}
	s.updateCoalescers[name] = coalescer
	go coalescer.run()
	return coalescer
}

func clampUpdateCoalescingMaxBatch(maxBatch int) int {
	if maxBatch > maxUpdateCoalescingBatch {
		return maxUpdateCoalescingBatch
	}
	return maxBatch
}

func (c *mongoUpdateCoalescer) enqueue(req mongoUpdateCoalescerRequest) bool {
	if c == nil {
		return false
	}
	for {
		c.enqueueMu.Lock()
		c.mu.RLock()
		if c.stopped || c.requests == nil {
			c.mu.RUnlock()
			c.enqueueMu.Unlock()
			return false
		}
		requests := c.requests
		stoppedCh := c.stoppedCh
		select {
		case requests <- req:
			c.mu.RUnlock()
			c.enqueueMu.Unlock()
			return true
		default:
		}
		c.mu.RUnlock()
		c.enqueueMu.Unlock()

		select {
		case <-stoppedCh:
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *mongoUpdateCoalescer) stop() {
	if c == nil {
		return
	}
	_ = c.closeRequests()
	if c.done != nil {
		<-c.done
	}
}

func (c *mongoUpdateCoalescer) closeRequests() bool {
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return false
	}
	if c.stoppedCh == nil {
		c.stoppedCh = make(chan struct{})
	}
	c.stopped = true
	close(c.stoppedCh)
	close(c.requests)
	c.mu.Unlock()
	return true
}

func (c *mongoUpdateCoalescer) retireIdle() bool {
	if c == nil {
		return false
	}
	stopped := false
	if c.server != nil {
		c.server.updateMu.Lock()
		if c.server.updateCoalescers != nil && c.server.updateCoalescers[c.name] == c {
			stopped = c.closeRequests()
			delete(c.server.updateCoalescers, c.name)
		}
		c.server.updateMu.Unlock()
	} else {
		stopped = c.closeRequests()
	}
	if !stopped {
		if c.isStopped() {
			c.drainRequestsDirect()
			return true
		}
		return false
	}
	c.drainRequestsDirect()
	return true
}

func (c *mongoUpdateCoalescer) isStopped() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopped
}

func (c *mongoUpdateCoalescer) markStopped() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
}

func (c *mongoUpdateCoalescer) drainRequestsDirect() {
	for req := range c.requests {
		matched, modified, err := runMongoUpdateOne(req.col, req.item)
		req.done <- mongoUpdateCoalescerResult{matched: matched, modified: modified, err: err}
	}
}

func (c *mongoUpdateCoalescer) run() {
	defer func() {
		c.markStopped()
		if c.done != nil {
			close(c.done)
		}
	}()
	var idle <-chan time.Time
	var timer *time.Timer
	if c.idleTTL > 0 {
		timer = time.NewTimer(c.idleTTL)
		idle = timer.C
		defer timer.Stop()
	}
	resetIdle := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(c.idleTTL)
	}
	for {
		select {
		case first, ok := <-c.requests:
			if !ok {
				return
			}
			c.runBatchStartingWith(first)
			resetIdle()
		case <-idle:
			if c.retireIdle() {
				return
			}
			resetIdle()
		case <-c.stoppedCh:
			c.drainRequestsDirect()
			return
		}
	}
}

func (c *mongoUpdateCoalescer) runBatchStartingWith(first mongoUpdateCoalescerRequest) {
	batch := []mongoUpdateCoalescerRequest{first}
	if c.maxDelay > 0 {
		timer := time.NewTimer(c.maxDelay)
	collect:
		for len(batch) < c.maxBatch {
			select {
			case req, ok := <-c.requests:
				if !ok {
					break collect
				}
				batch = append(batch, req)
			case <-timer.C:
				break collect
			case <-c.stoppedCh:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	for len(batch) < c.maxBatch {
		select {
		case req, ok := <-c.requests:
			if !ok {
				goto drained
			}
			batch = append(batch, req)
		case <-c.stoppedCh:
			goto drained
		default:
			goto drained
		}
	}
drained:
	c.runBatch(batch)
}

func (c *mongoUpdateCoalescer) runBatch(batch []mongoUpdateCoalescerRequest) {
	if len(batch) == 0 {
		return
	}
	if len(batch) == 1 ||
		mongoUpdateCoalescerHasDuplicateKeys(batch) ||
		!mongoUpdateCoalescerUsesSingleCollection(batch) ||
		!mongoUpdateCoalescerBatchCanUseBatch(batch) {
		runMongoUpdateCoalescerSequential(batch)
		return
	}
	updates := make([]mongoUpdateItem, len(batch))
	for i, req := range batch {
		updates[i] = req.item
	}
	results, batched, err := runMongoUpdateBatchResults(batch[0].col, updates)
	if !batched {
		runMongoUpdateCoalescerSequential(batch)
		return
	}
	if err != nil {
		if index, ok := collectionUpdateBatchErrorIndex(err); ok && index >= 0 && index < len(batch) {
			completeMongoUpdateCoalescerBatchExcept(batch, index)
			batch[index].done <- mongoUpdateCoalescerResult{err: collectionUpdateBatchErrorForRequest(err, batch[index].item.index)}
			return
		}
		completeMongoUpdateCoalescerBatch(batch, mongoUpdateCoalescerResult{err: err})
		return
	}
	if len(results) != len(batch) {
		completeMongoUpdateCoalescerBatch(batch, mongoUpdateCoalescerResult{err: fmt.Errorf("mongo gateway update coalescer batch results=%d want %d", len(results), len(batch))})
		return
	}
	for i, req := range batch {
		result := results[i]
		req.done <- mongoUpdateCoalescerResult{matched: result.Matched, modified: result.Modified}
	}
}

func completeMongoUpdateCoalescerBatch(batch []mongoUpdateCoalescerRequest, result mongoUpdateCoalescerResult) {
	for _, req := range batch {
		req.done <- result
	}
}

func completeMongoUpdateCoalescerBatchExcept(batch []mongoUpdateCoalescerRequest, skip int) {
	for i, req := range batch {
		if i == skip {
			continue
		}
		matched, modified, err := runMongoUpdateOne(req.col, req.item)
		req.done <- mongoUpdateCoalescerResult{matched: matched, modified: modified, err: err}
	}
}

func collectionUpdateBatchErrorIndex(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	var itemErr *collections.UpdateBatchItemError
	if errors.As(err, &itemErr) {
		return itemErr.Index, true
	}
	return 0, false
}

func collectionUpdateBatchErrorForRequest(err error, updateIndex int) error {
	var itemErr *collections.UpdateBatchItemError
	if errors.As(err, &itemErr) {
		return mongoUpdateErrorWithIndex(updateIndex, itemErr.Err)
	}
	return err
}

func mongoUpdateCoalescerHasDuplicateKeys(batch []mongoUpdateCoalescerRequest) bool {
	seen := make(map[string]struct{}, len(batch))
	for _, req := range batch {
		key := string(req.item.key)
		if _, ok := seen[key]; ok {
			return true
		}
		seen[key] = struct{}{}
	}
	return false
}

func mongoUpdateCoalescerUsesSingleCollection(batch []mongoUpdateCoalescerRequest) bool {
	if len(batch) == 0 {
		return true
	}
	col := batch[0].col
	if col == nil {
		return false
	}
	for _, req := range batch[1:] {
		if !col.SameCachedCatalog(req.col) {
			return false
		}
	}
	return true
}

func mongoUpdateCoalescerBatchCanUseBatch(batch []mongoUpdateCoalescerRequest) bool {
	if len(batch) == 0 || batch[0].col == nil {
		return false
	}
	meta := batch[0].col.Meta()
	for _, req := range batch {
		if !mongoUpdateCanUseBatchMeta(meta, req.item) {
			return false
		}
	}
	return true
}

func runMongoUpdateCoalescerSequential(batch []mongoUpdateCoalescerRequest) {
	for _, req := range batch {
		matched, modified, err := runMongoUpdateOne(req.col, req.item)
		req.done <- mongoUpdateCoalescerResult{matched: matched, modified: modified, err: err}
	}
}

func mongoUpdateItemsCanUseBatch(col *collections.Collection, updates []mongoUpdateItem) bool {
	if col == nil {
		return false
	}
	meta := col.Meta()
	for _, update := range updates {
		if !mongoUpdateCanUseBatchMeta(meta, update) {
			return false
		}
	}
	return true
}

func mongoUpdateCanUseBatch(col *collections.Collection, update mongoUpdateItem) bool {
	if col == nil {
		return false
	}
	return mongoUpdateCanUseBatchMeta(col.Meta(), update)
}

func mongoUpdateCanUseBatchMeta(meta collections.CollectionMeta, update mongoUpdateItem) bool {
	if !update.setFieldsOK {
		return false
	}
	return !mongoUpdateSetFieldsTouchSecondaryUniqueIndexMeta(meta, update)
}

func mongoUpdateSetFieldsTouchSecondaryUniqueIndexMeta(meta collections.CollectionMeta, update mongoUpdateItem) bool {
	if !update.setFieldsOK {
		return true
	}
	for _, idx := range meta.Indexes {
		if !idx.Unique {
			continue
		}
		if _, ok := update.setFields[idx.Field]; ok {
			return true
		}
	}
	return false
}

func mongoSetUpdateFields(updateDoc wire.Document) (map[string]struct{}, bool) {
	updateElements, err := bson.Raw(updateDoc).Elements()
	if err != nil || len(updateElements) != 1 {
		return nil, false
	}
	operator, err := updateElements[0].KeyErr()
	if err != nil || operator != "$set" {
		return nil, false
	}
	setDoc, ok := updateElements[0].Value().DocumentOK()
	if !ok {
		return nil, false
	}
	order, err := parseSetFieldNames(setDoc)
	if err != nil {
		return nil, false
	}
	out := make(map[string]struct{}, len(order))
	for _, field := range order {
		out[field] = struct{}{}
	}
	return out, true
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
		deletedOne, err := col.DeleteDocument(key)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if deletedOne {
			deleted++
		}
	}
	return marshalDeleteResponse(deleted)
}

func (s *Server) listCollectionsResponse(command wire.Document) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if err := validateMongoDatabaseName(db); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	nameOnly, err := optionalBoolField(command, "nameOnly")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	filter, err := commandOptionalDocument(command, "filter")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	nameFilter, err := collectionNameFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	metas, err := s.Collections.ListCollections()
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	prefix := db + "."
	firstBatch := bson.A{}
	for _, meta := range metas {
		collectionName, ok := strings.CutPrefix(meta.Name, prefix)
		if !ok {
			continue
		}
		if nameFilter != "" && collectionName != nameFilter {
			continue
		}
		firstBatch = append(firstBatch, mongoCollectionDocument(collectionName, nameOnly))
	}
	return marshalCursorResponse(db, "$cmd.listCollections", firstBatch)
}

func (s *Server) createIndexesResponse(command wire.Document) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	collection, err := commandString(command, "createIndexes")
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
	indexDocs, err := commandDocumentArray(command, "indexes")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if len(indexDocs) == 0 {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway createIndexes requires at least one index")
	}
	defs := make([]collections.IndexDefinition, 0, len(indexDocs))
	for i, indexDoc := range indexDocs {
		def, err := parseCreateIndexDefinition(indexDoc)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("indexes[%d]: %v", i, err))
		}
		defs = append(defs, s.applyDefaultIndexOptions(def))
	}

	createdAutomatically := false
	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if !errors.Is(err, collections.ErrCollectionNotFound) {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		meta := s.defaultCollectionMeta(name)
		meta.Indexes = dedupeIdenticalIndexDefinitions(defs)
		created, err := s.Collections.CreateCollection(meta)
		if err != nil {
			// If another request created the collection after our miss, fall
			// through to the idempotent add-index path below.
			createErr := err
			col, err = s.Collections.OpenCollection(name)
			if err != nil {
				return commandError(commandCodeBadValue, "BadValue", createErr.Error())
			}
		} else {
			return marshalDocument(bson.D{
				{Key: "ok", Value: 1.0},
				{Key: "createdCollectionAutomatically", Value: true},
				{Key: "numIndexesBefore", Value: int32(1)},
				{Key: "numIndexesAfter", Value: int32(1 + len(created.Indexes))},
			})
		}
	}
	numBefore := int32(1 + len(col.Meta().Indexes))
	meta := col.Meta()
	for _, def := range defs {
		if existing, ok := findIndexDefinition(meta.Indexes, def.Name); ok && sameIndexDefinition(existing, def) {
			continue
		}
		next, err := col.CreateIndex(def)
		if err != nil {
			code, codeName := commandCodeBadValue, "BadValue"
			if collections.IsDuplicateKeyError(err) {
				code, codeName = commandCodeDuplicateKey, "DuplicateKey"
			}
			return commandError(code, codeName, err.Error())
		}
		meta = *next
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "createdCollectionAutomatically", Value: createdAutomatically},
		{Key: "numIndexesBefore", Value: numBefore},
		{Key: "numIndexesAfter", Value: int32(1 + len(meta.Indexes))},
	})
}

func (s *Server) listIndexesResponse(command wire.Document) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	collection, err := commandString(command, "listIndexes")
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
	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return commandError(commandCodeNamespaceNotFound, "NamespaceNotFound", "collection not found: "+db+"."+collection)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return marshalCursorResponse(db, collection, mongoIndexDocuments(col.Meta()))
}

func (s *Server) dropIndexesResponse(command wire.Document) (wire.Document, error) {
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	collection, err := commandString(command, "dropIndexes")
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
	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return commandError(commandCodeNamespaceNotFound, "NamespaceNotFound", "collection not found: "+db+"."+collection)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	before := int32(1 + len(col.Meta().Indexes))
	names, all, err := dropIndexNames(command)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if all {
		if _, err := col.DropAllIndexes(); err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
	} else {
		for _, indexName := range names {
			if indexName == "_id_" {
				return commandError(commandCodeBadValue, "BadValue", "cannot drop _id index")
			}
		}
		if _, err := col.DropIndexes(names); err != nil {
			if errors.Is(err, collections.ErrIndexNotFound) {
				return commandError(commandCodeIndexNotFound, "IndexNotFound", "index not found")
			}
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "nIndexesWas", Value: before},
	})
}

func (s *Server) getMoreResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	cursorID, err := requiredInt64Field(command, "getMore")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	collection, err := commandString(command, "collection")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	batchSize, batchSizeSet, err := optionalInt32FieldWithPresence(command, "batchSize")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	batchSizeValue, batchSizeSet := normalizeGetMoreBatchSize(batchSize, batchSizeSet)
	ns := db + "." + collection
	nextID, nextBatch, ok, err := s.getMore(cursorID, ns, cursorOwner, batchSizeValue, batchSizeSet, maxInt)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if !ok {
		return commandError(commandCodeCursorNotFound, "CursorNotFound", fmt.Sprintf("cursor not found: %d", cursorID))
	}
	return marshalCursorResponseWithID(ns, nextID, "nextBatch", nextBatch)
}

func (s *Server) killCursorsResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	collection, err := commandString(command, "killCursors")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	cursorIDs, err := requiredInt64ArrayField(command, "cursors")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	ns := db + "." + collection
	killed, notFound := s.killCursors(ns, cursorOwner, cursorIDs)
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "cursorsKilled", Value: int64Array(killed)},
		{Key: "cursorsNotFound", Value: int64Array(notFound)},
		{Key: "cursorsAlive", Value: bson.A{}},
		{Key: "cursorsUnknown", Value: bson.A{}},
	})
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

func marshalCursorResponse(db, collection string, firstBatch bson.A) (wire.Document, error) {
	return marshalCursorResponseWithID(db+"."+collection, 0, "firstBatch", firstBatch)
}

func marshalCursorResponseWithID(ns string, cursorID int64, batchKey string, batch bson.A) (wire.Document, error) {
	return marshalDocument(bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: cursorID},
			{Key: "ns", Value: ns},
			{Key: batchKey, Value: batch},
		}},
		{Key: "ok", Value: 1.0},
	})
}

func marshalCursorDocumentsResponseWithID(ns string, cursorID int64, batchKey string, batch []wire.Document) (wire.Document, error) {
	batchBytes := findBatchOverheadBytes
	for i, doc := range batch {
		batchBytes += findBatchDocumentBytes(doc, i)
	}
	docIdx, doc := bsoncore.AppendDocumentStart(make([]byte, 0, len(ns)+batchBytes+96))
	cursorIdx, doc := bsoncore.AppendDocumentElementStart(doc, "cursor")
	doc = bsoncore.AppendInt64Element(doc, "id", cursorID)
	doc = bsoncore.AppendStringElement(doc, "ns", ns)
	batchIdx, doc := bsoncore.AppendArrayElementStart(doc, batchKey)
	for i, batchDoc := range batch {
		doc = bsoncore.AppendDocumentElement(doc, bsonArrayIndexKey(i), batchDoc)
	}
	var err error
	doc, err = bsoncore.AppendArrayEnd(doc, batchIdx)
	if err != nil {
		return nil, err
	}
	doc, err = bsoncore.AppendDocumentEnd(doc, cursorIdx)
	if err != nil {
		return nil, err
	}
	doc = bsoncore.AppendDoubleElement(doc, "ok", 1.0)
	doc, err = bsoncore.AppendDocumentEnd(doc, docIdx)
	if err != nil {
		return nil, err
	}
	return wire.Document(doc), nil
}

func marshalCursorDocumentsMsgResponseWithID(requestID, responseTo int32, ns string, cursorID int64, batchKey string, batch []wire.Document) ([]byte, error) {
	return marshalCursorDocumentsMsgResponseWithIDInto(nil, requestID, responseTo, ns, cursorID, batchKey, batch, wire.DefaultMaxMessageLength)
}

func marshalCursorDocumentsMsgResponseWithIDInto(dst []byte, requestID, responseTo int32, ns string, cursorID int64, batchKey string, batch []wire.Document, maxMessageLength int) ([]byte, error) {
	if maxMessageLength <= 0 || maxMessageLength > wire.DefaultMaxMessageLength {
		maxMessageLength = wire.DefaultMaxMessageLength
	}
	batchBytes := findBatchOverheadBytes
	for i, doc := range batch {
		batchBytes += findBatchDocumentBytes(doc, i)
	}
	need := 16 + 5 + len(ns) + batchBytes + 96
	dst = ensureWireAppendCapacity(dst, need)
	msg := dst
	base := len(msg)
	msg = appendWireInt32(msg, 0)
	msg = appendWireInt32(msg, requestID)
	msg = appendWireInt32(msg, responseTo)
	msg = appendWireInt32(msg, int32(wire.OpMsg))
	msg = appendWireInt32(msg, 0)
	msg = append(msg, wire.MsgSectionBody)
	docIdx, msg := bsoncore.AppendDocumentStart(msg)
	cursorIdx, msg := bsoncore.AppendDocumentElementStart(msg, "cursor")
	msg = bsoncore.AppendInt64Element(msg, "id", cursorID)
	msg = bsoncore.AppendStringElement(msg, "ns", ns)
	batchIdx, msg := bsoncore.AppendArrayElementStart(msg, batchKey)
	for i, batchDoc := range batch {
		msg = bsoncore.AppendDocumentElement(msg, bsonArrayIndexKey(i), batchDoc)
	}
	var err error
	msg, err = bsoncore.AppendArrayEnd(msg, batchIdx)
	if err != nil {
		return nil, err
	}
	msg, err = bsoncore.AppendDocumentEnd(msg, cursorIdx)
	if err != nil {
		return nil, err
	}
	msg = bsoncore.AppendDoubleElement(msg, "ok", 1.0)
	msg, err = bsoncore.AppendDocumentEnd(msg, docIdx)
	if err != nil {
		return nil, err
	}
	messageLength := len(msg) - base
	if int64(messageLength) > maxWireMessageLengthInt32Limit {
		return nil, fmt.Errorf("%w: length=%d", wire.ErrMessageTooLarge, messageLength)
	}
	if messageLength > maxMessageLength {
		return nil, fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, messageLength, maxMessageLength)
	}
	binary.LittleEndian.PutUint32(msg[base:base+4], uint32(messageLength))
	return msg, nil
}

type indexedRangeCursorResponse struct {
	col           *collections.Collection
	server        *Server
	ns            string
	indexName     string
	opts          collections.IndexRangeOptions
	batchKey      string
	maxBatchBytes int
	cursorOwner   int64
	singleBatch   bool
}

var errBorrowedRangeMaterialization = errors.New("mongo gateway: borrowed range materialization")

func (r *indexedRangeCursorResponse) marshalDocument() (wire.Document, error) {
	materializer, err := storedDocumentMaterializerForCollection(r.col)
	if err != nil {
		return nil, err
	}
	doc, err := marshalIndexedRangeCursorDocument(r.server, r.cursorOwner, r.singleBatch, r.col, materializer, r.ns, r.indexName, r.opts, r.batchKey, r.maxBatchBytes)
	retry := shouldRetryBorrowedRangeMaterialization(materializer, err)
	_ = materializer.Close()
	if !retry {
		return doc, err
	}
	materializer, materializerErr := storedDocumentMaterializerForCollection(r.col)
	if materializerErr != nil {
		return nil, materializerErr
	}
	defer func() { _ = materializer.Close() }()
	return marshalIndexedRangeCursorDocument(r.server, r.cursorOwner, r.singleBatch, r.col, materializer, r.ns, r.indexName, r.opts, r.batchKey, r.maxBatchBytes)
}

func (r *indexedRangeCursorResponse) marshalMsgIntoWithMaxLength(dst []byte, requestID, responseTo int32, maxMessageLength int) ([]byte, error) {
	materializer, err := storedDocumentMaterializerForCollection(r.col)
	if err != nil {
		return nil, err
	}
	msg, err := marshalIndexedRangeCursorMsgInto(dst, requestID, responseTo, r.server, r.cursorOwner, r.singleBatch, r.col, materializer, r.ns, r.indexName, r.opts, r.batchKey, r.maxBatchBytes, maxMessageLength)
	retry := shouldRetryBorrowedRangeMaterialization(materializer, err)
	_ = materializer.Close()
	if !retry {
		return msg, err
	}
	materializer, materializerErr := storedDocumentMaterializerForCollection(r.col)
	if materializerErr != nil {
		return nil, materializerErr
	}
	defer func() { _ = materializer.Close() }()
	return marshalIndexedRangeCursorMsgInto(dst, requestID, responseTo, r.server, r.cursorOwner, r.singleBatch, r.col, materializer, r.ns, r.indexName, r.opts, r.batchKey, r.maxBatchBytes, maxMessageLength)
}

func shouldRetryBorrowedRangeMaterialization(materializer *collections.StoredDocumentJSONMaterializer, err error) bool {
	return err != nil &&
		errors.Is(err, errBorrowedRangeMaterialization) &&
		materializer != nil &&
		materializer.DocumentFormat() == collections.DocumentFormatTemplateV1
}

func marshalIndexedRangeCursorDocument(server *Server, cursorOwner int64, singleBatch bool, col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, ns, indexName string, opts collections.IndexRangeOptions, batchKey string, maxBatchBytes int) (wire.Document, error) {
	builder := newRawCursorDocumentBuilder(make([]byte, 0, rawCursorResponseCapacityHint(ns, opts.Limit, maxBatchBytes)), ns, batchKey, maxBatchBytes)
	retainedDocs, err := collectIndexedRangeCursorDocs(col, materializer, indexName, opts, &builder, singleBatch)
	if err != nil {
		return nil, err
	}
	doc, err := builder.finish()
	if err != nil {
		return nil, err
	}
	if len(retainedDocs) > 0 {
		cursorID, err := server.openRetainedCursor(ns, retainedDocs, compiledProjection{}, cursorOwner)
		if err != nil {
			return nil, err
		}
		builder.setCursorID(cursorID)
	}
	return wire.Document(doc), nil
}

func marshalIndexedRangeCursorMsgInto(dst []byte, requestID, responseTo int32, server *Server, cursorOwner int64, singleBatch bool, col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, ns, indexName string, opts collections.IndexRangeOptions, batchKey string, maxBatchBytes int, maxMessageLength int) ([]byte, error) {
	if maxMessageLength <= 0 || maxMessageLength > wire.DefaultMaxMessageLength {
		maxMessageLength = int(server.maxMessageLength())
	}
	need := wire.HeaderLen + 5 + rawCursorResponseCapacityHint(ns, opts.Limit, maxBatchBytes)
	if need > maxMessageLength {
		return dst, fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, need, maxMessageLength)
	}
	dst = ensureWireAppendCapacity(dst, need)
	msg := dst
	base := len(msg)
	msg = appendWireInt32(msg, 0)
	msg = appendWireInt32(msg, requestID)
	msg = appendWireInt32(msg, responseTo)
	msg = appendWireInt32(msg, int32(wire.OpMsg))
	msg = appendWireInt32(msg, 0)
	msg = append(msg, wire.MsgSectionBody)
	builder := newRawCursorDocumentBuilder(msg, ns, batchKey, maxBatchBytes)
	retainedDocs, err := collectIndexedRangeCursorDocs(col, materializer, indexName, opts, &builder, singleBatch)
	if err != nil {
		return msg[:base], err
	}
	msg, err = builder.finish()
	if err != nil {
		return msg[:base], err
	}
	if len(retainedDocs) > 0 {
		cursorID, err := server.openRetainedCursor(ns, retainedDocs, compiledProjection{}, cursorOwner)
		if err != nil {
			return msg[:base], err
		}
		builder.setCursorID(cursorID)
	}
	messageLength := len(msg) - base
	if int64(messageLength) > maxWireMessageLengthInt32Limit {
		return msg[:base], fmt.Errorf("%w: length=%d", wire.ErrMessageTooLarge, messageLength)
	}
	if messageLength > maxMessageLength {
		return msg[:base], fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, messageLength, maxMessageLength)
	}
	binary.LittleEndian.PutUint32(msg[base:base+4], uint32(messageLength))
	return msg, nil
}

func collectIndexedRangeCursorDocs(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, indexName string, opts collections.IndexRangeOptions, builder *rawCursorDocumentBuilder, singleBatch bool) ([]wire.Document, error) {
	var retainedDocs []wire.Document
	retainOnly := false
	_, err := col.ScanBorrowedDocumentsByIndexRange(indexName, opts, func(record collections.BorrowedDocumentRecord) (bool, error) {
		if len(record.Document) == 0 {
			return true, nil
		}
		doc, err := borrowedStoredDocumentToBSON(materializer, record.Document)
		if err != nil {
			return false, fmt.Errorf("%w: %w", errBorrowedRangeMaterialization, err)
		}
		if retainOnly {
			retainedDocs = append(retainedDocs, append(wire.Document(nil), doc...))
			return true, nil
		}
		appended, err := builder.appendDocument(doc)
		if err != nil || appended {
			return appended, err
		}
		if singleBatch {
			return false, nil
		}
		retainOnly = true
		retainedDocs = append(retainedDocs, append(wire.Document(nil), doc...))
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return retainedDocs, nil
}

type rawCursorDocumentBuilder struct {
	buf           []byte
	docIdx        int32
	cursorIdx     int32
	cursorIDIdx   int
	batchIdx      int32
	batchBytes    int
	count         int
	maxBatchBytes int
}

func newRawCursorDocumentBuilder(dst []byte, ns, batchKey string, maxBatchBytes int) rawCursorDocumentBuilder {
	docIdx, dst := bsoncore.AppendDocumentStart(dst)
	cursorIdx, dst := bsoncore.AppendDocumentElementStart(dst, "cursor")
	cursorIDIdx := len(dst) + 1 + len("id") + 1
	dst = bsoncore.AppendInt64Element(dst, "id", 0)
	dst = bsoncore.AppendStringElement(dst, "ns", ns)
	batchIdx, dst := bsoncore.AppendArrayElementStart(dst, batchKey)
	return rawCursorDocumentBuilder{
		buf:           dst,
		docIdx:        docIdx,
		cursorIdx:     cursorIdx,
		cursorIDIdx:   cursorIDIdx,
		batchIdx:      batchIdx,
		maxBatchBytes: maxBatchBytes,
	}
}

func (b *rawCursorDocumentBuilder) appendDocument(doc wire.Document) (bool, error) {
	docBytes := findBatchDocumentBytes(doc, b.count)
	if findBatchOverheadBytes+docBytes > b.maxBatchBytes {
		return false, fmt.Errorf("mongo gateway cursor document exceeds max batch bytes: docBytes=%d maxBatchBytes=%d", docBytes, b.maxBatchBytes)
	}
	if b.count > 0 && findBatchOverheadBytes+b.batchBytes+docBytes > b.maxBatchBytes {
		return false, nil
	}
	b.buf = bsoncore.AppendDocumentElement(b.buf, bsonArrayIndexKey(b.count), doc)
	b.batchBytes += docBytes
	b.count++
	return true, nil
}

func (b *rawCursorDocumentBuilder) setCursorID(cursorID int64) {
	if b.cursorIDIdx < 0 || b.cursorIDIdx+8 > len(b.buf) {
		return
	}
	binary.LittleEndian.PutUint64(b.buf[b.cursorIDIdx:b.cursorIDIdx+8], uint64(cursorID))
}

func (b *rawCursorDocumentBuilder) finish() ([]byte, error) {
	var err error
	b.buf, err = bsoncore.AppendArrayEnd(b.buf, b.batchIdx)
	if err != nil {
		return nil, err
	}
	b.buf, err = bsoncore.AppendDocumentEnd(b.buf, b.cursorIdx)
	if err != nil {
		return nil, err
	}
	b.buf = bsoncore.AppendDoubleElement(b.buf, "ok", 1.0)
	b.buf, err = bsoncore.AppendDocumentEnd(b.buf, b.docIdx)
	if err != nil {
		return nil, err
	}
	return b.buf, nil
}

func rawCursorResponseCapacityHint(ns string, limit int, maxBatchBytes int) int {
	const estimatedIndexedRangeDocumentBytes = 1024
	need := len(ns) + 96 + findBatchOverheadBytes
	if limit > 0 {
		need += limit * estimatedIndexedRangeDocumentBytes
	}
	if maxBatchBytes > 0 && need > maxBatchBytes+len(ns)+96 {
		need = maxBatchBytes + len(ns) + 96
	}
	if need > maxRetainedWireWriteBuffer {
		return maxRetainedWireWriteBuffer
	}
	return need
}

func appendWireInt32(dst []byte, v int32) []byte {
	n := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(dst[n:], uint32(v))
	return dst
}

func ensureWireAppendCapacity(dst []byte, need int) []byte {
	if need <= 0 || cap(dst)-len(dst) >= need {
		return dst
	}
	minCap := len(dst) + need
	newCap := cap(dst) * 2
	if newCap < minCap {
		newCap = minCap
	}
	if minCap <= maxRetainedWireWriteBuffer && newCap > maxRetainedWireWriteBuffer {
		newCap = maxRetainedWireWriteBuffer
	}
	grown := make([]byte, len(dst), newCap)
	copy(grown, dst)
	return grown
}

var bsonArrayIndexKeyCache = func() [128]string {
	var keys [128]string
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return keys
}()

func bsonArrayIndexKey(index int) string {
	if index >= 0 && index < len(bsonArrayIndexKeyCache) {
		return bsonArrayIndexKeyCache[index]
	}
	return strconv.Itoa(index)
}

func (s *Server) openCursor(ns string, docs []wire.Document, projection compiledProjection, batchSize int, explicitBatchSize bool, defaultBatchSize int, owner int64) (int64, bson.A, error) {
	if s.isClosed() {
		return 0, nil, errServerClosed
	}
	batchSize, err := normalizeBatchSize(batchSize, explicitBatchSize, defaultBatchSize)
	if err != nil {
		return 0, nil, err
	}
	firstBatch, consumed, err := documentsBatchWithLimit(docs, projection, batchSize, s.maxFindBatchBytes())
	if err != nil {
		return 0, nil, err
	}
	if consumed >= len(docs) {
		return 0, firstBatch, nil
	}
	retainedDocs := append([]wire.Document(nil), docs[consumed:]...)
	cursorID, err := s.openRetainedCursor(ns, retainedDocs, projection, owner)
	if err != nil {
		return 0, nil, err
	}
	return cursorID, firstBatch, nil
}

func (s *Server) openRetainedCursor(ns string, docs []wire.Document, projection compiledProjection, owner int64) (int64, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	retainedBytes := documentsBytes(docs)
	if maxBytes := s.maxCursorRetainedBytes(); retainedBytes > maxBytes {
		return 0, fmt.Errorf("mongo gateway cursor retained bytes exceeds limit: retainedBytes=%d maxBytes=%d", retainedBytes, maxBytes)
	}
	cursorID := s.nextCursorID.Add(1)
	if cursorID == 0 {
		cursorID = s.nextCursorID.Add(1)
	}
	now := time.Now()
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	if s.isClosed() {
		return 0, errServerClosed
	}
	s.reapExpiredCursorsLocked(now)
	if s.cursors == nil {
		s.cursors = make(map[int64]*serverCursor)
	}
	if len(s.cursors) >= s.maxOpenCursors() {
		return 0, errors.New("Mongo gateway cursor limit exceeded")
	}
	s.addCursorLocked(cursorID, &serverCursor{ns: ns, owner: owner, docs: docs, projection: projection, pos: 0, lastUsed: now})
	return cursorID, nil
}

func (s *Server) getMore(cursorID int64, ns string, owner int64, batchSize int, explicitBatchSize bool, defaultBatchSize int) (int64, bson.A, bool, error) {
	if cursorID == 0 {
		return 0, nil, false, nil
	}
	if explicitBatchSize {
		var err error
		batchSize, err = normalizeBatchSize(batchSize, true, defaultBatchSize)
		if err != nil {
			return 0, nil, false, err
		}
	}
	for {
		s.cursorMu.Lock()
		s.reapExpiredCursorsLocked(time.Now())
		cursor := s.cursors[cursorID]
		if cursor == nil || cursor.ns != ns || cursor.owner != owner {
			s.cursorMu.Unlock()
			return 0, nil, false, nil
		}
		startPos := cursor.pos
		remaining := cursor.docs[startPos:]
		projection := cursor.projection
		effectiveBatchSize := batchSize
		if !explicitBatchSize {
			effectiveBatchSize = defaultBatchSize
		}
		s.cursorMu.Unlock()

		batch, consumed, err := documentsBatchWithLimit(remaining, projection, effectiveBatchSize, s.maxFindBatchBytes())
		if err != nil {
			s.cursorMu.Lock()
			current := s.cursors[cursorID]
			if current == cursor && current.ns == ns && current.owner == owner && current.pos == startPos {
				s.deleteCursorLocked(cursorID)
			}
			s.cursorMu.Unlock()
			return 0, nil, false, err
		}

		s.cursorMu.Lock()
		current := s.cursors[cursorID]
		if current == nil || current != cursor || current.ns != ns || current.owner != owner {
			s.cursorMu.Unlock()
			return 0, nil, false, nil
		}
		if current.pos != startPos {
			s.cursorMu.Unlock()
			continue
		}
		current.pos += consumed
		current.lastUsed = time.Now()
		if current.pos >= len(current.docs) {
			s.deleteCursorLocked(cursorID)
			s.cursorMu.Unlock()
			return 0, batch, true, nil
		}
		s.cursorMu.Unlock()
		return cursorID, batch, true, nil
	}
}

func (s *Server) killCursorsForOwner(owner int64) {
	if owner == 0 {
		return
	}
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	for cursorID, cursor := range s.cursors {
		if cursor.owner == owner {
			s.deleteCursorLocked(cursorID)
		}
	}
}

func (s *Server) killCursors(ns string, owner int64, cursorIDs []int64) ([]int64, []int64) {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	s.reapExpiredCursorsLocked(time.Now())
	killed := make([]int64, 0, len(cursorIDs))
	notFound := make([]int64, 0)
	for _, cursorID := range cursorIDs {
		cursor := s.cursors[cursorID]
		if cursor == nil || cursor.ns != ns || cursor.owner != owner {
			notFound = append(notFound, cursorID)
			continue
		}
		s.deleteCursorLocked(cursorID)
		killed = append(killed, cursorID)
	}
	return killed, notFound
}

func (s *Server) reapExpiredCursors() {
	timeout := s.cursorIdleTimeout()
	if timeout <= 0 || s.cursorCount.Load() == 0 {
		return
	}
	now := time.Now()
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	if !s.lastCursorReap.IsZero() && now.Sub(s.lastCursorReap) < defaultCursorReapInterval {
		return
	}
	s.lastCursorReap = now
	s.reapExpiredCursorsLocked(now)
}

func (s *Server) reapExpiredCursorsLocked(now time.Time) {
	timeout := s.cursorIdleTimeout()
	if timeout <= 0 {
		return
	}
	cutoff := now.Add(-timeout)
	for cursorID, cursor := range s.cursors {
		if !cursor.lastUsed.IsZero() && !cursor.lastUsed.After(cutoff) {
			s.deleteCursorLocked(cursorID)
		}
	}
}

func (s *Server) addCursorLocked(cursorID int64, cursor *serverCursor) {
	if cursor == nil {
		return
	}
	if s.cursors == nil {
		s.cursors = make(map[int64]*serverCursor)
	}
	if _, ok := s.cursors[cursorID]; !ok {
		s.cursorCount.Add(1)
	}
	s.cursors[cursorID] = cursor
}

func (s *Server) deleteCursorLocked(cursorID int64) bool {
	if s.cursors == nil {
		return false
	}
	if _, ok := s.cursors[cursorID]; !ok {
		return false
	}
	delete(s.cursors, cursorID)
	s.cursorCount.Add(-1)
	return true
}

func normalizeBatchSize(batchSize int, explicit bool, defaultBatchSize int) (int, error) {
	if !explicit {
		return defaultBatchSize, nil
	}
	if batchSize < 0 {
		return 0, errors.New("Mongo gateway cursor batchSize must be non-negative")
	}
	return batchSize, nil
}

func normalizeGetMoreBatchSize(batchSize int32, explicit bool) (int, bool) {
	// MongoDB treats getMore batchSize: 0 like an omitted count limit; find
	// batchSize: 0 is intentionally different and opens an empty first batch.
	if explicit && batchSize == 0 {
		return 0, false
	}
	return int(batchSize), explicit
}

func documentsBatch(docs []wire.Document) bson.A {
	out, _, _ := documentsBatchWithLimit(docs, compiledProjection{}, len(docs), maxInt)
	return out
}

func documentsBytes(docs []wire.Document) int {
	total := 0
	for _, doc := range docs {
		if len(doc) > maxInt-total {
			return maxInt
		}
		total += len(doc)
	}
	return total
}

func documentsBatchWithLimit(docs []wire.Document, projection compiledProjection, maxDocs int, maxBytes int) (bson.A, int, error) {
	if maxDocs < 0 || maxDocs > len(docs) {
		maxDocs = len(docs)
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	out := make(bson.A, 0, maxDocs)
	batchBytes := 0
	consumed := 0
	for consumed < maxDocs {
		doc, err := projectDocumentWithProjection(docs[consumed], projection)
		if err != nil {
			return nil, 0, err
		}
		docBytes := findBatchDocumentBytes(doc, len(out))
		if findBatchOverheadBytes+docBytes > maxBytes {
			return nil, 0, fmt.Errorf("mongo gateway cursor document exceeds max batch bytes: docBytes=%d maxBatchBytes=%d", docBytes, maxBytes)
		}
		if len(out) > 0 && findBatchOverheadBytes+batchBytes+docBytes > maxBytes {
			break
		}
		out = append(out, bson.Raw(doc))
		batchBytes += docBytes
		consumed++
	}
	return out, consumed, nil
}

func rawDocumentsBatchLimit(docs []wire.Document, maxDocs int, maxBytes int) (int, error) {
	if maxDocs < 0 || maxDocs > len(docs) {
		maxDocs = len(docs)
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	batchBytes := 0
	consumed := 0
	for consumed < maxDocs {
		docBytes := findBatchDocumentBytes(docs[consumed], consumed)
		if findBatchOverheadBytes+docBytes > maxBytes {
			return 0, fmt.Errorf("mongo gateway cursor document exceeds max batch bytes: docBytes=%d maxBatchBytes=%d", docBytes, maxBytes)
		}
		if consumed > 0 && findBatchOverheadBytes+batchBytes+docBytes > maxBytes {
			break
		}
		batchBytes += docBytes
		consumed++
	}
	return consumed, nil
}

func (s *Server) openOrCreateCollection(name string) (*collections.Collection, error) {
	col, err := s.Collections.OpenCollection(name)
	if err == nil {
		return col, nil
	}
	if _, createErr := s.Collections.CreateCollection(s.defaultCollectionMeta(name)); createErr != nil {
		return nil, createErr
	}
	return s.Collections.OpenCollection(name)
}

func (s *Server) defaultCollectionMeta(name string) *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name:    name,
		Options: s.DefaultCollectionOptions,
	}
}

func (s *Server) applyDefaultIndexOptions(def collections.IndexDefinition) collections.IndexDefinition {
	if def.StoragePolicy == collections.RootStorageDefault {
		def.StoragePolicy = s.DefaultIndexStoragePolicy
	}
	return def
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
	out, _, err := optionalInt32FieldWithPresence(doc, key)
	return out, err
}

func optionalInt32FieldWithPresence(doc wire.Document, key string) (int32, bool, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return 0, false, nil
	}
	if out, ok := value.Int32OK(); ok {
		return out, true, nil
	}
	if out, ok := value.Int64OK(); ok {
		if out < 0 || out > int64(^uint32(0)>>1) {
			return 0, true, fmt.Errorf("Mongo command field %q is out of int32 range", key)
		}
		return int32(out), true, nil
	}
	return 0, true, fmt.Errorf("Mongo command field %q must be an integer", key)
}

func requiredInt64Field(doc wire.Document, key string) (int64, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return 0, fmt.Errorf("Mongo command missing %q", key)
	}
	if out, ok := strictBSONInt64(value); ok {
		return out, nil
	}
	return 0, fmt.Errorf("Mongo command field %q must be an integer", key)
}

func requiredInt64ArrayField(doc wire.Document, key string) ([]int64, error) {
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
	out := make([]int64, 0, len(values))
	for i, value := range values {
		item, ok := strictBSONInt64(value)
		if !ok {
			return nil, fmt.Errorf("Mongo command field %q[%d] must be an integer", key, i)
		}
		out = append(out, item)
	}
	return out, nil
}

func strictBSONInt64(value bson.RawValue) (int64, bool) {
	if out, ok := value.Int64OK(); ok {
		return out, true
	}
	if out, ok := value.Int32OK(); ok {
		return int64(out), true
	}
	return 0, false
}

func int64Array(values []int64) bson.A {
	out := make(bson.A, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func collectionNameFilter(filter wire.Document) (string, error) {
	if filter == nil {
		return "", nil
	}
	elements, err := bson.Raw(filter).Elements()
	if err != nil {
		return "", err
	}
	if len(elements) == 0 {
		return "", nil
	}
	if len(elements) != 1 {
		return "", errors.New("Mongo gateway listCollections filter currently supports name equality only")
	}
	key, err := elements[0].KeyErr()
	if err != nil {
		return "", err
	}
	if key != "name" {
		return "", errors.New("Mongo gateway listCollections filter currently supports name equality only")
	}
	name, ok := elements[0].Value().StringValueOK()
	if !ok {
		return "", errors.New("Mongo gateway listCollections name filter must be a string")
	}
	return name, nil
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
		sequenceDocs = seq.Documents
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
	if collection == "" {
		return "", errors.New("Mongo collection name cannot be empty")
	}
	if err := validateMongoDatabaseName(db); err != nil {
		return "", err
	}
	name := db + "." + collection
	if err := collections.ValidateCollectionName(name); err != nil {
		return "", err
	}
	return name, nil
}

func validateMongoDatabaseName(db string) error {
	if db == "" {
		return errors.New("Mongo database name cannot be empty")
	}
	if strings.ContainsAny(db, "\x00/:") {
		return errors.New("Mongo database name contains reserved punctuation")
	}
	return nil
}

// prepareInsertDocument converts one wire BSON document into the collection's
// configured storage format. Callers pass documents parsed from validated wire
// messages or produced by gateway update materialization.
func prepareInsertDocument(doc wire.Document, format collections.DocumentFormat) ([]byte, []byte, error) {
	raw := bson.Raw(doc)
	id := raw.Lookup("_id")
	if id.IsZero() {
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
		id = raw.Lookup("_id")
	}
	key, err := encodePrimaryKey(id)
	if err != nil {
		return nil, nil, err
	}
	if format == collections.DocumentFormatBSON {
		return key, raw, nil
	}
	if err := validateSupportedDocument(raw); err != nil {
		return nil, nil, err
	}
	stored, err := bson.MarshalExtJSON(raw, true, false)
	if err != nil {
		return nil, nil, err
	}
	if format == collections.DocumentFormatTemplateV1 {
		stored, err = collections.EncodeTemplateV1DocumentJSON(stored)
		if err != nil {
			return nil, nil, err
		}
	}
	return key, stored, nil
}

func storedDocumentMaterializerForCollection(col *collections.Collection) (*collections.StoredDocumentJSONMaterializer, error) {
	if col == nil {
		return nil, nil
	}
	switch col.Meta().Options.DocumentFormat {
	case collections.DocumentFormatDefault, collections.DocumentFormatJSON:
		return nil, nil
	default:
		return col.NewStoredDocumentJSONMaterializer()
	}
}

func borrowedStoredDocumentToBSON(materializer *collections.StoredDocumentJSONMaterializer, stored []byte) (wire.Document, error) {
	// Borrowed scan callbacks already run under the collection's captured read
	// state. Do not call back into Collection here; doing so can block behind a
	// writer while the callback still holds the borrowed scan lock.
	return storedDocumentToBSON(nil, materializer, stored)
}

func storedDocumentToBSON(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, stored []byte) (wire.Document, error) {
	if materializer != nil {
		if materializer.DocumentFormat() == collections.DocumentFormatBSON {
			// Stored BSON bytes are validated when the gateway accepts or builds
			// the document. Keep a cheap frame check but avoid repeating full BSON
			// validation on every hot read response.
			if err := validateStoredBSONFrame(stored); err != nil {
				return nil, err
			}
			return wire.Document(stored), nil
		}
		materialized, err := materializer.StoredDocumentJSON(stored)
		if err != nil {
			// A reused template-v1 resolver can lag a concurrently fetched document.
			// Retry once with a fresh snapshot before surfacing the original error.
			if col != nil {
				if fresh, freshErr := col.StoredDocumentJSON(stored); freshErr == nil {
					materialized = fresh
					err = nil
				}
			}
			if err != nil {
				return nil, err
			}
		}
		stored = materialized
	}
	var raw bson.Raw
	if err := bson.UnmarshalExtJSON(stored, true, &raw); err != nil {
		return nil, err
	}
	return wire.Document(raw), nil
}

func validateStoredBSONFrame(doc []byte) error {
	if len(doc) < 5 {
		return fmt.Errorf("stored BSON document too short: %d", len(doc))
	}
	size := int(int32(binary.LittleEndian.Uint32(doc[:4])))
	if size < 0 {
		return fmt.Errorf("stored BSON document has negative length header: %d", size)
	}
	if size < 5 {
		return fmt.Errorf("stored BSON document length=%d below minimum", size)
	}
	if size != len(doc) {
		return fmt.Errorf("stored BSON document length=%d available=%d", size, len(doc))
	}
	if doc[len(doc)-1] != 0 {
		return errors.New("stored BSON document missing terminator")
	}
	return nil
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

func parseCreateIndexDefinition(doc wire.Document) (collections.IndexDefinition, error) {
	keyDoc, err := requiredDocumentField(doc, "key")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	elements, err := bson.Raw(keyDoc).Elements()
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	if len(elements) != 1 {
		return collections.IndexDefinition{}, errors.New("Mongo gateway createIndexes currently supports single-field indexes only")
	}
	field, err := elements[0].KeyErr()
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	if field == "_id" {
		return collections.IndexDefinition{}, errors.New("Mongo gateway cannot create the built-in _id index")
	}
	if !isAscendingIndexKey(elements[0].Value()) {
		return collections.IndexDefinition{}, errors.New("Mongo gateway createIndexes currently supports ascending indexes only")
	}
	name, namePresent, err := optionalStringFieldWithPresence(doc, "name")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	if namePresent && name == "" {
		return collections.IndexDefinition{}, errors.New("Mongo gateway createIndexes index name cannot be empty")
	}
	if !namePresent {
		name = field + "_1"
	}
	unique, err := optionalBoolField(doc, "unique")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	valueTypeRaw, valueTypePresent, err := optionalStringFieldWithPresence(doc, "treedbValueType")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	if !valueTypePresent {
		return collections.IndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes index %q on field %q requires treedbValueType", name, field)
	}
	valueType := collections.IndexValueType(valueTypeRaw)
	switch valueType {
	case collections.IndexValueString, collections.IndexValueBool, collections.IndexValueInt64, collections.IndexValueDouble:
	default:
		return collections.IndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes index %q on field %q has unsupported treedbValueType %q; supported values are string, bool, int64, double", name, field, valueTypeRaw)
	}
	return collections.IndexDefinition{
		Name:      name,
		Field:     field,
		ValueType: valueType,
		Unique:    unique,
	}, nil
}

func optionalStringField(doc wire.Document, key string) (string, error) {
	out, _, err := optionalStringFieldWithPresence(doc, key)
	return out, err
}

func optionalStringFieldWithPresence(doc wire.Document, key string) (string, bool, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return "", false, nil
	}
	out, ok := value.StringValueOK()
	if !ok {
		return "", true, fmt.Errorf("Mongo command field %q must be a string", key)
	}
	return out, true, nil
}

func isAscendingIndexKey(value bson.RawValue) bool {
	if v, ok := value.Int32OK(); ok {
		return v == 1
	}
	if v, ok := value.Int64OK(); ok {
		return v == 1
	}
	if v, ok := value.DoubleOK(); ok {
		return v == 1
	}
	return false
}

func mongoCollectionDocument(name string, nameOnly bool) bson.D {
	doc := bson.D{
		{Key: "name", Value: name},
		{Key: "type", Value: "collection"},
	}
	if nameOnly {
		return doc
	}
	return append(doc,
		bson.E{Key: "options", Value: bson.D{}},
		bson.E{Key: "info", Value: bson.D{{Key: "readOnly", Value: false}}},
		bson.E{Key: "idIndex", Value: bson.D{
			{Key: "v", Value: int32(2)},
			{Key: "key", Value: bson.D{{Key: "_id", Value: int32(1)}}},
			{Key: "name", Value: "_id_"},
		}},
	)
}

func mongoIndexDocuments(meta collections.CollectionMeta) bson.A {
	out := bson.A{bson.D{
		{Key: "v", Value: int32(2)},
		{Key: "key", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "name", Value: "_id_"},
	}}
	for _, idx := range meta.Indexes {
		doc := bson.D{
			{Key: "v", Value: int32(2)},
			{Key: "key", Value: bson.D{{Key: idx.Field, Value: int32(1)}}},
			{Key: "name", Value: idx.Name},
			{Key: "treedbValueType", Value: string(idx.ValueType)},
		}
		if idx.Unique {
			doc = append(doc, bson.E{Key: "unique", Value: true})
		}
		out = append(out, doc)
	}
	return out
}

func findIndexDefinition(indexes []collections.IndexDefinition, name string) (collections.IndexDefinition, bool) {
	for _, index := range indexes {
		if index.Name == name {
			return index, true
		}
	}
	return collections.IndexDefinition{}, false
}

func dedupeIdenticalIndexDefinitions(defs []collections.IndexDefinition) []collections.IndexDefinition {
	out := make([]collections.IndexDefinition, 0, len(defs))
	for _, def := range defs {
		if existing, ok := findIndexDefinition(out, def.Name); ok && sameIndexDefinition(existing, def) {
			continue
		}
		out = append(out, def)
	}
	return out
}

func sameIndexDefinition(left, right collections.IndexDefinition) bool {
	return left.Name == right.Name &&
		left.Field == right.Field &&
		left.ValueType == right.ValueType &&
		left.Unique == right.Unique &&
		left.MultiKey == right.MultiKey &&
		left.StoragePolicy == right.StoragePolicy
}

func dropIndexNames(command wire.Document) ([]string, bool, error) {
	value := bson.Raw(command).Lookup("index")
	if value.IsZero() {
		return nil, false, errors.New("Mongo command missing \"index\"")
	}
	if name, ok := value.StringValueOK(); ok {
		if name == "*" {
			return nil, true, nil
		}
		return []string{name}, false, nil
	}
	array, ok := value.ArrayOK()
	if !ok {
		return nil, false, errors.New("Mongo command field \"index\" must be a string or array")
	}
	values, err := array.Values()
	if err != nil {
		return nil, false, err
	}
	if len(values) == 0 {
		return nil, false, errors.New("Mongo command field \"index\" must not be empty")
	}
	names := make([]string, 0, len(values))
	for i, value := range values {
		name, ok := value.StringValueOK()
		if !ok {
			return nil, false, fmt.Errorf("Mongo command field \"index\"[%d] must be a string", i)
		}
		if name == "*" {
			return nil, true, nil
		}
		names = append(names, name)
	}
	return names, false, nil
}

func parseSetDocument(doc bson.Raw) (map[string]bson.RawValue, []string, error) {
	elements, err := doc.Elements()
	if err != nil {
		return nil, nil, err
	}
	order := make([]string, 0, len(elements))
	seen := make(map[string]struct{}, len(elements))
	sets := make(map[string]bson.RawValue, len(elements))
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, nil, err
		}
		if err := validateSetFieldName(key); err != nil {
			return nil, nil, err
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
	return sets, order, nil
}

func parseSetFieldNames(doc bson.Raw) ([]string, error) {
	elements, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	return parseSetFieldNamesFromElements(elements)
}

func parseSetFieldNamesFromElements(elements []bson.RawElement) ([]string, error) {
	order := make([]string, 0, len(elements))
	seen := make(map[string]struct{}, len(elements))
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, err
		}
		if err := validateSetFieldName(key); err != nil {
			return nil, err
		}
		if _, ok := seen[key]; !ok {
			order = append(order, key)
			seen[key] = struct{}{}
		}
	}
	return order, nil
}

func validateSetFieldName(key string) error {
	if key == "" {
		return errors.New("Mongo gateway $set field name cannot be empty")
	}
	if key == "_id" {
		return errors.New("Mongo gateway update cannot modify _id")
	}
	if strings.Contains(key, ".") {
		return errors.New("Mongo gateway $set currently supports top-level fields only")
	}
	if strings.HasPrefix(key, "$") {
		return errors.New("Mongo gateway $set field names cannot start with $")
	}
	return nil
}

// EncodePrimaryKey encodes a MongoDB _id value into the canonical TreeDB
// collection primary key used by the Mongo gateway.
func EncodePrimaryKey(value bson.RawValue) ([]byte, error) {
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

func encodePrimaryKey(value bson.RawValue) ([]byte, error) {
	return EncodePrimaryKey(value)
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
