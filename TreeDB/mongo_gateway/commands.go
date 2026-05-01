package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	commandCodeBadValue          int32 = 2
	commandCodeFailedToParse     int32 = 9
	commandCodeNamespaceNotFound int32 = 26
	commandCodeIndexNotFound     int32 = 27
	commandCodeCursorNotFound    int32 = 43
	commandCodeDuplicateKey      int32 = 11000
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

func (s *Server) findResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
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
	plan, err := parseFindPlan(command, filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	col, err := s.Collections.OpenCollection(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return marshalCursorResponse(db, collection, bson.A{})
	}
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	results, err := s.executeFind(col, plan)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	batchSize, batchSizeSet, err := optionalInt32FieldWithPresence(command, "batchSize")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	singleBatch, err := optionalBoolField(command, "singleBatch")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	ns := db + "." + collection
	if singleBatch {
		normalizedBatchSize, err := normalizeBatchSize(int(batchSize), batchSizeSet, defaultCursorBatchSize)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		firstBatch, _, err := documentsBatchWithLimit(results.docs, results.projection, normalizedBatchSize, s.maxFindBatchBytes())
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		return marshalCursorResponseWithID(ns, 0, "firstBatch", firstBatch)
	}
	cursorID, firstBatch, err := s.openCursor(ns, results.docs, results.projection, int(batchSize), batchSizeSet, defaultCursorBatchSize, cursorOwner)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return marshalCursorResponseWithID(ns, cursorID, "firstBatch", firstBatch)
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
	} else if len(parsed) > 1 && !hasDuplicateKey && !collectionHasSecondaryUniqueIndexes(col) {
		matched, modified, err = runMongoUpdateBatch(col, parsed)
		if err != nil {
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
	index     int
	key       []byte
	keyString string
	updateDoc wire.Document
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
	return mongoUpdateItem{index: index, key: key, keyString: string(key), updateDoc: updateDoc}, nil
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

func runMongoUpdateBatch(col *collections.Collection, updates []mongoUpdateItem) (int32, int32, error) {
	results, err := runMongoUpdateBatchResults(col, updates)
	if err != nil {
		return 0, 0, err
	}
	var matched int32
	var modified int32
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

func runMongoUpdateBatchResults(col *collections.Collection, updates []mongoUpdateItem) ([]collections.UpdateBatchResult, error) {
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return nil, err
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
	results, err := col.UpdateBatch(items)
	if err != nil {
		return nil, err
	}
	return results, nil
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
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	c.mu.RLock()
	stopped := c.stopped
	requests := c.requests
	c.mu.RUnlock()
	if stopped {
		return false
	}
	select {
	case requests <- req:
		return true
	default:
		return false
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
		shouldStop := false
		c.server.updateMu.Lock()
		if c.server.updateCoalescers != nil && c.server.updateCoalescers[c.name] == c {
			delete(c.server.updateCoalescers, c.name)
			shouldStop = true
		}
		c.server.updateMu.Unlock()
		if shouldStop {
			stopped = c.closeRequests()
		}
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
		collectionHasSecondaryUniqueIndexes(batch[0].col) {
		runMongoUpdateCoalescerSequential(batch)
		return
	}
	updates := make([]mongoUpdateItem, len(batch))
	for i, req := range batch {
		updates[i] = req.item
	}
	results, err := runMongoUpdateBatchResults(batch[0].col, updates)
	if err != nil || len(results) != len(batch) {
		runMongoUpdateCoalescerSequential(batch)
		return
	}
	for i, req := range batch {
		result := results[i]
		req.done <- mongoUpdateCoalescerResult{matched: result.Matched, modified: result.Modified}
	}
}

func mongoUpdateCoalescerHasDuplicateKeys(batch []mongoUpdateCoalescerRequest) bool {
	seen := make(map[string]struct{}, len(batch))
	for _, req := range batch {
		key := req.item.keyString
		if key == "" {
			key = string(req.item.key)
		}
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

func runMongoUpdateCoalescerSequential(batch []mongoUpdateCoalescerRequest) {
	for _, req := range batch {
		matched, modified, err := runMongoUpdateOne(req.col, req.item)
		req.done <- mongoUpdateCoalescerResult{matched: matched, modified: modified, err: err}
	}
}

func collectionHasSecondaryUniqueIndexes(col *collections.Collection) bool {
	if col == nil {
		return false
	}
	for _, idx := range col.Meta().Indexes {
		if idx.Unique {
			return true
		}
	}
	return false
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
		if _, err := s.Collections.CreateCollection(s.defaultCollectionMeta(name)); err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		createdAutomatically = true
		col, err = s.Collections.OpenCollection(name)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
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
	retainedBytes := documentsBytes(retainedDocs)
	if maxBytes := s.maxCursorRetainedBytes(); retainedBytes > maxBytes {
		return 0, nil, fmt.Errorf("Mongo gateway cursor retained bytes exceeds limit: retainedBytes=%d maxBytes=%d", retainedBytes, maxBytes)
	}
	cursorID := s.nextCursorID.Add(1)
	if cursorID == 0 {
		cursorID = s.nextCursorID.Add(1)
	}
	now := time.Now()
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	if s.isClosed() {
		return 0, nil, errServerClosed
	}
	s.reapExpiredCursorsLocked(now)
	if s.cursors == nil {
		s.cursors = make(map[int64]*serverCursor)
	}
	if len(s.cursors) >= s.maxOpenCursors() {
		return 0, nil, errors.New("Mongo gateway cursor limit exceeded")
	}
	s.cursors[cursorID] = &serverCursor{ns: ns, owner: owner, docs: retainedDocs, projection: projection, pos: 0, lastUsed: now}
	return cursorID, firstBatch, nil
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
				delete(s.cursors, cursorID)
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
			delete(s.cursors, cursorID)
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
			delete(s.cursors, cursorID)
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
		delete(s.cursors, cursorID)
		killed = append(killed, cursorID)
	}
	return killed, notFound
}

func (s *Server) reapExpiredCursors() {
	timeout := s.cursorIdleTimeout()
	if timeout <= 0 {
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
			delete(s.cursors, cursorID)
		}
	}
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
			return nil, 0, fmt.Errorf("Mongo gateway cursor document exceeds max message size: docBytes=%d maxBytes=%d", docBytes, maxBytes)
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

func storedDocumentToBSON(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, stored []byte) (wire.Document, error) {
	if materializer != nil {
		if materializer.DocumentFormat() == collections.DocumentFormatBSON {
			raw := bson.Raw(stored)
			if err := raw.Validate(); err != nil {
				return nil, err
			}
			return wire.Document(raw), nil
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
	return collections.IndexDefinition{
		Name:   name,
		Field:  field,
		Unique: unique,
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

func sameIndexDefinition(left, right collections.IndexDefinition) bool {
	return left.Name == right.Name &&
		left.Field == right.Field &&
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
