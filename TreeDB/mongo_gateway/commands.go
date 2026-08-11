package mongogateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
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

	treeDBIndexTypeField     = "treedbIndexType"
	treeDBIndexTypeVector    = "vector"
	treeDBVectorOptionsField = "treedbVector"

	mongoDefaultVectorIndexM              = 16
	mongoDefaultVectorIndexEfConstruction = 128
	mongoDefaultVectorIndexEfSearch       = 64
)

const primaryKeyPrefixBSONValue byte = 1

var maxInt = int(^uint(0) >> 1)

func (s *Server) insertResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if s.clusterSubmitterConfigured() {
		return s.clusterInsertResponse(ctx, command, sequences)
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "insert"); rejected {
		return doc, err
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
	ordered, _, err := mongoCommandOrdered(command)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if len(documents) > defaultMaxWriteBatchSize {
		return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("Mongo gateway insert exceeds maxWriteBatchSize %d", defaultMaxWriteBatchSize))
	}
	budget, err := s.newMongoWriteBudgetForCommand(ctx, command)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if err := budget.ensureMinimumResponse(); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	var col *collections.Collection
	format := s.DefaultCollectionOptions.DocumentFormat
	if existing, err := s.openCollectionForMutation(name); err == nil {
		col = existing
		format = existing.MetaView().Options.DocumentFormat
	} else if !errors.Is(err, collections.ErrCollectionNotFound) {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	ids, stored, err := prepareInsertDocuments(documents, format)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	// A BSON multi-document insert takes the native atomic batch granule.  Its
	// whole target reservation is knowable after parse, before a first-write
	// collection would be created, so reject an over-cap command without that
	// otherwise observable catalog side effect.
	if len(ids) > 1 && format == collections.DocumentFormatBSON && len(ids) > s.maxMongoWriteTargets() {
		return marshalInsertResponseWithWriteErrors(0, []mongoWriteError{{index: 0, err: errors.New("Mongo gateway multi-write command exceeded its retained-target budget")}})
	}
	var releaseColdCollection func()
	defer func() {
		if releaseColdCollection != nil {
			releaseColdCollection()
		}
	}()
	if col == nil {
		if s.mongoWriteBeforeFirstCreateHook != nil {
			s.mongoWriteBeforeFirstCreateHook(budget)
		}
		if err := budget.checkDeadline(); err != nil {
			return mongoInsertPreMutationWriteErrorResponse(budget, 0, err)
		}
		col, releaseColdCollection, err = s.openOrCreateCollectionForFirstWrite(name)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if actualFormat := col.MetaView().Options.DocumentFormat; actualFormat != format {
			ids, stored, err = prepareInsertDocuments(documents, actualFormat)
			if err != nil {
				return commandError(commandCodeBadValue, "BadValue", err.Error())
			}
			format = actualFormat
		}
	}
	return s.runMongoInsertCommand(name, col, format, ids, stored, ordered, budget)
}

// runMongoInsertCommand applies a fully parsed insert command.  A native
// InsertBatchValidatedBSON call is an intentionally non-interruptible atomic
// granule: its deadline is checked immediately before entry.  The duplicate
// fallback is item-at-a-time, so it reserves target capacity and rechecks the
// deadline before every possible side effect.
func (s *Server) runMongoInsertCommand(name string, col *collections.Collection, format collections.DocumentFormat, ids, stored [][]byte, ordered bool, budget *mongoWriteBudget) (wire.Document, error) {
	// Preserve the native batch fast path for the normal success case.  Its
	// planner rejects duplicate conflicts before publishing, so a duplicate can
	// safely fall back to the per-item path that supplies Mongo's indexed
	// ordered/unordered error envelope.
	if len(ids) > 1 && format == collections.DocumentFormatBSON {
		if err := budget.reserveTargets(len(ids)); err != nil {
			if reserveErr := budget.reserveError(); reserveErr == nil {
				return marshalInsertResponseWithWriteErrors(0, []mongoWriteError{{index: 0, err: err}})
			}
			// ensureMinimumResponse reserves this terminal slot before any
			// mutation is admitted. Never turn a runtime rejection into a
			// successful-looking n:0 response merely because ordinary error
			// capacity has already been exhausted.
			if reserveErr := budget.reserveTerminalError(); reserveErr != nil {
				return nil, reserveErr
			}
			return marshalInsertResponseWithWriteErrors(0, []mongoWriteError{{index: 0, err: err}})
		}
		if _, batchErr := col.InsertBatchValidatedBSON(ids, stored); batchErr == nil {
			return marshalInsertResponseWithWriteErrors(int32(len(ids)), nil)
		} else if errors.Is(batchErr, collections.ErrCommitAmbiguous) {
			return mongoInsertCommandError(batchErr)
		} else {
			// A non-ambiguous native batch failure is pre-publication. This includes
			// duplicate conflicts and item/planning failures (for example an
			// oversized secondary-index key). Give the atomic reservation back and
			// preserve Mongo's indexed ordered/unordered semantics per item.
			budget.refundTargets(len(ids))
		}
	}
	inserted := int32(0)
	writeErrors := make([]mongoWriteError, 0)
	for i := range ids {
		if err := budget.reserveTarget(); err != nil {
			if reserveErr := budget.reserveError(); reserveErr == nil {
				writeErrors = append(writeErrors, mongoWriteError{index: i, err: err})
			} else {
				if terminalErr := budget.reserveTerminalError(); terminalErr != nil {
					return nil, terminalErr
				}
				writeErrors = append(writeErrors, mongoWriteError{index: i, err: err})
			}
			break
		}
		var err error
		if format == collections.DocumentFormatBSON {
			if len(ids) == 1 {
				err = s.runMongoInsertCoalesced(name, col, ids[i], stored[i], budget)
			} else {
				err = runMongoInsertOne(col, ids[i], stored[i])
			}
		} else {
			_, err = col.InsertBatch(ids[i:i+1], stored[i:i+1])
		}
		if err == nil {
			inserted++
			continue
		}
		// A collection can report an error after publishing a document (for
		// example while maintaining an auxiliary index).  That outcome is not a
		// normal per-item write failure: continuing an unordered command would
		// make the caller unable to tell which writes may have committed.
		if errors.Is(err, collections.ErrCommitAmbiguous) {
			return mongoCommitAmbiguousCommandError(err)
		}
		if reserveErr := budget.reserveError(); reserveErr != nil {
			if terminalErr := budget.reserveTerminalError(); terminalErr != nil {
				return nil, terminalErr
			}
			writeErrors = append(writeErrors, mongoWriteError{index: i, err: reserveErr})
			break
		}
		writeErrors = append(writeErrors, mongoWriteError{index: i, err: err})
		if ordered {
			break
		}
	}
	return marshalInsertResponseWithWriteErrors(inserted, writeErrors)
}

// mongoInsertPreMutationWriteErrorResponse reports an indexed runtime stop
// discovered after full parsing but before a missing namespace is created.
// The terminal response reservation is retained even at the smallest valid
// envelope, so a deadline never becomes a misleading successful n:0 result.
func mongoInsertPreMutationWriteErrorResponse(budget *mongoWriteBudget, index int, runErr error) (wire.Document, error) {
	if reserveErr := budget.reserveError(); reserveErr == nil {
		return marshalInsertResponseWithWriteErrors(0, []mongoWriteError{{index: index, err: runErr}})
	}
	if terminalErr := budget.reserveTerminalError(); terminalErr != nil {
		return nil, terminalErr
	}
	return marshalInsertResponseWithWriteErrors(0, []mongoWriteError{{index: index, err: runErr}})
}

// mongoUpdatePreMutationWriteErrorsResponse is the update counterpart used
// for a runtime deadline discovered after full parse/admission but before
// first catalog creation. Missing-collection preview errors have not consumed
// the real budget yet, so reserve them in stable order before adding the
// terminal deadline error. This preserves unordered outcomes without a
// catalog side effect.
func mongoUpdatePreMutationWriteErrorsResponse(budget *mongoWriteBudget, previewErrors []mongoWriteError, index int, runErr error) (wire.Document, error) {
	writeErrors := make([]mongoWriteError, 0, len(previewErrors)+1)
	for _, writeErr := range append(previewErrors, mongoWriteError{index: index, err: runErr}) {
		if reserveErr := budget.reserveError(); reserveErr == nil {
			writeErrors = append(writeErrors, writeErr)
			continue
		}
		if terminalErr := budget.reserveTerminalError(); terminalErr != nil {
			return nil, terminalErr
		}
		writeErrors = append(writeErrors, writeErr)
		break
	}
	return marshalUpdateResponseWithWriteErrors(0, 0, nil, writeErrors)
}

func mongoInsertCommandError(err error) (wire.Document, error) {
	if errors.Is(err, collections.ErrCommitAmbiguous) {
		return mongoCommitAmbiguousCommandError(err)
	}
	code, codeName := commandCodeBadValue, "BadValue"
	if collections.IsDuplicateKeyError(err) {
		code, codeName = commandCodeDuplicateKey, "DuplicateKey"
	}
	return commandError(code, codeName, err.Error())
}

func mongoCommitAmbiguousCommandError(err error) (wire.Document, error) {
	return commandError(commandCodeShutdownInProgress, "ShutdownInProgress", err.Error())
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

type mongoInsertCoalescer struct {
	maxDelay  time.Duration
	maxBatch  int
	idleTTL   time.Duration
	requests  chan mongoInsertCoalescerRequest
	stoppedCh chan struct{}
	done      chan struct{}
	server    *Server
	name      string

	mu        sync.RWMutex
	stopped   bool
	enqueueMu sync.Mutex
}

type mongoInsertCoalescerRequest struct {
	col      *collections.Collection
	id       []byte
	stored   []byte
	deadline time.Time
	done     chan mongoInsertCoalescerResult
}

type mongoInsertCoalescerResult struct {
	err error
}

func (s *Server) runMongoInsertCoalesced(name string, col *collections.Collection, id, stored []byte, budget *mongoWriteBudget) error {
	if err := budget.checkDeadline(); err != nil {
		return err
	}
	if col == nil {
		return runMongoInsertOne(col, id, stored)
	}
	coalescer := s.mongoInsertCoalescer(name)
	if coalescer == nil {
		return runMongoInsertOne(col, id, stored)
	}
	done := make(chan mongoInsertCoalescerResult, 1)
	// The handler waits for completion before returning, so request-body-backed
	// BSON remains live while the worker builds and applies the coalesced batch.
	if !coalescer.enqueue(mongoInsertCoalescerRequest{col: col, id: id, stored: stored, deadline: budget.deadline, done: done}) {
		if err := budget.checkDeadline(); err != nil {
			return err
		}
		return runMongoInsertOne(col, id, stored)
	}
	return coalescer.waitForInsertResult(done).err
}

func runMongoInsertOne(col *collections.Collection, id, stored []byte) error {
	if col == nil {
		return errCollectionMissingForInsert()
	}
	_, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{stored})
	return err
}

func errCollectionMissingForInsert() error {
	return errors.New("Mongo gateway insert collection handle is not configured")
}

func (c *mongoInsertCoalescer) waitForInsertResult(done chan mongoInsertCoalescerResult) mongoInsertCoalescerResult {
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
			return mongoInsertCoalescerResult{err: errors.New("mongo gateway insert coalescer stopped before completing request")}
		}
	}
}

func (s *Server) mongoInsertCoalescer(name string) *mongoInsertCoalescer {
	if s == nil || s.InsertCoalescingMaxBatch <= 1 || s.InsertCoalescingMaxDelay < 0 {
		return nil
	}
	maxBatch := clampInsertCoalescingMaxBatch(s.InsertCoalescingMaxBatch)
	if maxBatch <= 1 {
		return nil
	}
	maxDelay := s.InsertCoalescingMaxDelay
	idleTTL := s.InsertCoalescingIdleTTL
	if idleTTL == 0 {
		idleTTL = defaultInsertCoalescingIdleTTL
	}
	s.insertMu.Lock()
	defer s.insertMu.Unlock()
	if s.closed.Load() {
		return nil
	}
	if s.insertCoalescers == nil {
		s.insertCoalescers = make(map[string]*mongoInsertCoalescer)
	}
	if coalescer := s.insertCoalescers[name]; coalescer != nil {
		return coalescer
	}
	coalescer := &mongoInsertCoalescer{
		maxDelay:  maxDelay,
		maxBatch:  maxBatch,
		idleTTL:   idleTTL,
		requests:  make(chan mongoInsertCoalescerRequest, maxBatch*4),
		stoppedCh: make(chan struct{}),
		done:      make(chan struct{}),
		server:    s,
		name:      name,
	}
	s.insertCoalescers[name] = coalescer
	go coalescer.run()
	return coalescer
}

func clampInsertCoalescingMaxBatch(maxBatch int) int {
	if maxBatch > maxInsertCoalescingBatch {
		return maxInsertCoalescingBatch
	}
	return maxBatch
}

func (c *mongoInsertCoalescer) enqueue(req mongoInsertCoalescerRequest) bool {
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

func (c *mongoInsertCoalescer) stop() {
	if c == nil {
		return
	}
	_ = c.closeRequests()
	if c.done != nil {
		<-c.done
	}
}

func (c *mongoInsertCoalescer) closeRequests() bool {
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

func (c *mongoInsertCoalescer) retireIdle() bool {
	if c == nil {
		return false
	}
	stopped := false
	if c.server != nil {
		c.server.insertMu.Lock()
		if c.server.insertCoalescers != nil && c.server.insertCoalescers[c.name] == c {
			stopped = c.closeRequests()
			delete(c.server.insertCoalescers, c.name)
		}
		c.server.insertMu.Unlock()
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

func (c *mongoInsertCoalescer) isStopped() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopped
}

func (c *mongoInsertCoalescer) markStopped() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
}

func (c *mongoInsertCoalescer) drainRequestsDirect() {
	for req := range c.requests {
		runMongoInsertCoalescerSequential([]mongoInsertCoalescerRequest{req})
	}
}

func (c *mongoInsertCoalescer) run() {
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

func (c *mongoInsertCoalescer) runBatchStartingWith(first mongoInsertCoalescerRequest) {
	batch := []mongoInsertCoalescerRequest{first}
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

func (c *mongoInsertCoalescer) runBatch(batch []mongoInsertCoalescerRequest) {
	batch = filterExpiredMongoInsertCoalescerRequests(batch)
	if len(batch) == 0 {
		return
	}
	if len(batch) == 1 ||
		mongoInsertCoalescerHasDuplicateKeys(batch) ||
		!mongoInsertCoalescerUsesSingleCollection(batch) {
		runMongoInsertCoalescerSequential(batch)
		return
	}
	ids := make([][]byte, len(batch))
	stored := make([][]byte, len(batch))
	for i, req := range batch {
		ids[i] = req.id
		stored[i] = req.stored
	}
	_, err := batch[0].col.InsertBatchValidatedBSON(ids, stored)
	if err != nil {
		if collections.IsDuplicateKeyError(err) && !errors.Is(err, collections.ErrCommitAmbiguous) {
			runMongoInsertCoalescerSequential(batch)
			return
		}
		completeMongoInsertCoalescerBatch(batch, mongoInsertCoalescerResult{err: err})
		return
	}
	completeMongoInsertCoalescerBatch(batch, mongoInsertCoalescerResult{})
}

// A coalescer can wait behind unrelated traffic.  Drop requests whose command
// deadline elapsed while waiting before the worker enters any mutation granule.
func filterExpiredMongoInsertCoalescerRequests(batch []mongoInsertCoalescerRequest) []mongoInsertCoalescerRequest {
	kept := batch[:0]
	for _, req := range batch {
		if !req.deadline.IsZero() && time.Now().After(req.deadline) {
			req.done <- mongoInsertCoalescerResult{err: errors.New("Mongo gateway multi-write command exceeded its execution-time budget")}
			continue
		}
		kept = append(kept, req)
	}
	return kept
}

func completeMongoInsertCoalescerBatch(batch []mongoInsertCoalescerRequest, result mongoInsertCoalescerResult) {
	for _, req := range batch {
		req.done <- result
	}
}

func mongoInsertCoalescerHasDuplicateKeys(batch []mongoInsertCoalescerRequest) bool {
	seen := make(map[string]struct{}, len(batch))
	for _, req := range batch {
		key := string(req.id)
		if _, ok := seen[key]; ok {
			return true
		}
		seen[key] = struct{}{}
	}
	return false
}

func mongoInsertCoalescerUsesSingleCollection(batch []mongoInsertCoalescerRequest) bool {
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

func runMongoInsertCoalescerSequential(batch []mongoInsertCoalescerRequest) {
	for _, req := range batch {
		if !req.deadline.IsZero() && time.Now().After(req.deadline) {
			req.done <- mongoInsertCoalescerResult{err: errors.New("Mongo gateway multi-write command exceeded its execution-time budget")}
			continue
		}
		req.done <- mongoInsertCoalescerResult{err: runMongoInsertOne(req.col, req.id, req.stored)}
	}
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

func (s *Server) findResponse(ctx context.Context, command wire.Document, cursorOwner int64) (wire.Document, error) {
	payload, err := s.findResponsePayload(ctx, command, cursorOwner)
	if err != nil {
		return nil, err
	}
	doc, err := payload.marshalDocument()
	if err != nil && payload.kind == findResponsePayloadIndexedRange {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return doc, err
}

func (s *Server) findMsgResponse(ctx context.Context, command wire.Document, requestID, responseTo int32, cursorOwner int64) ([]byte, error) {
	return s.findMsgResponseInto(ctx, nil, command, requestID, responseTo, cursorOwner)
}

func (s *Server) findMsgResponseInto(ctx context.Context, dst []byte, command wire.Document, requestID, responseTo int32, cursorOwner int64) ([]byte, error) {
	// handleMsgInto performs admission before selecting this optimized encoder.
	// Keep the check here too so a future direct caller cannot turn this helper
	// into a wire-level authentication bypass.
	if s.authenticationRequired() && !s.authenticated(cursorOwner) {
		doc, err := commandError(13, "Unauthorized", "Authentication required")
		if err != nil {
			return nil, err
		}
		return wire.AppendMsgMessage(dst, requestID, responseTo, 0, doc)
	}
	if s.authenticationRequired() {
		if _, doc, err, allowed := s.authorizeCommand("find", command, cursorOwner); !allowed {
			if err != nil {
				return nil, err
			}
			return wire.AppendMsgMessage(dst, requestID, responseTo, 0, doc)
		}
	}
	payload, err := s.findResponsePayload(ctx, command, cursorOwner)
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

func (s *Server) findResponsePayload(ctx context.Context, command wire.Document, cursorOwner int64) (findResponsePayload, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return findResponsePayload{document: doc}, err
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "find"); rejected {
		return findResponsePayload{document: doc}, err
	}
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
	if err := s.preflightClusterFindRoute(ctx, db, collection, plan); err != nil {
		doc, err := mongoClusterRouteCommandError(err)
		return findResponsePayload{document: doc}, err
	}
	col, err := s.openCollectionCached(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		if plan.hint.present {
			doc, err := commandError(commandCodeBadValue, "BadValue", "Mongo gateway find hint does not name an existing index")
			return findResponsePayload{document: doc}, err
		}
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
	if !plan.hint.present {
		if payload, ok, err := s.findSimpleProjectedPrimaryEqualityPayload(col, ns, plan, int(batchSize), batchSizeSet); ok || err != nil {
			if err != nil {
				doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
				return findResponsePayload{document: doc}, err
			}
			return payload, nil
		}
	}
	if !plan.projection.present && !plan.hint.present {
		idx, opts, limit, ok, empty, err := pureIndexedRangeLimitPlan(col.MetaView(), plan, s.maxFindScanDocuments())
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
	// The V2 compound path retains primary keys, not BSON documents. It is used
	// only when every predicate is encoded by the index and its order satisfies
	// the requested sort; all other shapes use the conservative executor below.
	if !singleBatch && compoundIDCursorEligible(col.MetaView(), plan) {
		cursorIDBudget := s.maxCursorRetainedBytes() - findPlanCursorRetainedBytes(plan)
		if cursorIDBudget <= 0 {
			doc, err := commandError(commandCodeBadValue, "BadValue", fmt.Errorf("%w: Mongo gateway compound cursor plan exceeds retained-byte cap", errMongoFindScanCapExceeded).Error())
			return findResponsePayload{document: doc}, err
		}
		if ids, compound, ok, err := s.compoundIndexPlanIDs(col, plan, cursorIDBudget); ok || err != nil {
			if err != nil {
				doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
				return findResponsePayload{document: doc}, err
			}
			if compound.residualFilters == 0 && (plan.sort.field == "" || compound.sortSatisfied) && compoundPrefixCombinationCount(compound.prefixChoices) == 1 {
				if plan.skip > 0 {
					if int(plan.skip) >= len(ids) {
						ids = nil
					} else {
						ids = ids[plan.skip:]
					}
				}
				if plan.limit > 0 && int(plan.limit) < len(ids) {
					ids = ids[:plan.limit]
				}
				cursorID, firstBatch, err := s.openCompoundIDCursor(ns, col, ids, plan, int(batchSize), batchSizeSet, defaultCursorBatchSize, cursorOwner)
				if err != nil {
					doc, err := commandError(commandCodeBadValue, "BadValue", err.Error())
					return findResponsePayload{document: doc}, err
				}
				doc, err := marshalCursorResponseWithID(ns, cursorID, "firstBatch", firstBatch)
				return findResponsePayload{document: doc}, err
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

func (s *Server) findSimpleProjectedPrimaryEqualityPayload(col *collections.Collection, ns string, plan findPlan, batchSize int, batchSizeSet bool) (findResponsePayload, bool, error) {
	if !plan.projection.present {
		return findResponsePayload{}, false, nil
	}
	value, ok := simplePrimaryEqualityFindValue(plan)
	if !ok {
		return findResponsePayload{}, false, nil
	}
	normalizedBatchSize, err := normalizeBatchSize(batchSize, batchSizeSet, defaultCursorBatchSize)
	if err != nil {
		return findResponsePayload{}, true, err
	}
	if normalizedBatchSize < 1 {
		return findResponsePayload{}, false, nil
	}
	key, err := encodePrimaryKey(value)
	if err != nil {
		return findResponsePayload{}, true, err
	}
	stored, err := col.Get(key)
	if err != nil {
		return findResponsePayload{}, true, err
	}
	if len(stored) == 0 {
		return findResponsePayload{
			kind: findResponsePayloadRaw,
			raw:  rawCursorDocumentsResponse{ns: ns, cursorID: 0, batchKey: "firstBatch"},
		}, true, nil
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return findResponsePayload{}, true, err
	}
	defer func() { _ = materializer.Close() }()
	doc, err := storedDocumentToBSON(col, materializer, stored)
	if err != nil {
		return findResponsePayload{}, true, err
	}
	projected, err := projectDocumentWithProjection(doc, plan.projection)
	if err != nil {
		return findResponsePayload{}, true, err
	}
	docBytes := findBatchDocumentBytes(projected, 0)
	if findBatchOverheadBytes+docBytes > s.maxFindBatchBytes() {
		return findResponsePayload{}, true, fmt.Errorf("mongo gateway cursor document exceeds max batch bytes: docBytes=%d maxBatchBytes=%d", docBytes, s.maxFindBatchBytes())
	}
	return findResponsePayload{
		kind: findResponsePayloadRaw,
		raw:  rawCursorDocumentsResponse{ns: ns, cursorID: 0, batchKey: "firstBatch", batch: []wire.Document{projected}},
	}, true, nil
}

func (s *Server) updateResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if s.clusterSubmitterConfigured() {
		return s.clusterUpdateResponse(ctx, command, sequences)
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "update"); rejected {
		return doc, err
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
	ordered, _, err := mongoCommandOrdered(command)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if len(updates) > defaultMaxWriteBatchSize {
		return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("Mongo gateway update exceeds maxWriteBatchSize %d", defaultMaxWriteBatchSize))
	}
	// The complete BSON command (including this array) has already passed the
	// OP_MSG MaxMessageLength check before dispatch; this count cap separately
	// bounds result/error entries and per-spec bookkeeping.
	// Parse the entire command before opening/creating a collection or applying a
	// mutation.  This is deliberately stricter than the legacy streaming parser:
	// malformed later specifications never leave a partial command behind.
	parsed := make([]mongoUpdateItem, len(updates))
	budget, err := s.newMongoWriteBudgetForCommand(ctx, command)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	for i, update := range updates {
		item, parseErr := parseMongoUpdateItem(i, update)
		if parseErr != nil {
			return mongoUpdateParseCommandError(parseErr)
		}
		item.selector = s
		item.budget = budget
		parsed[i] = item
	}
	if err := budget.ensureMinimumResponse(); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	col, err := s.openCollectionForMutation(name)
	missingCollection := errors.Is(err, collections.ErrCollectionNotFound)
	if err != nil && !missingCollection {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	var releaseColdCollection func()
	defer func() {
		if releaseColdCollection != nil {
			releaseColdCollection()
		}
	}()
	if missingCollection {
		hasUpsert := false
		for _, item := range parsed {
			hasUpsert = hasUpsert || item.upsert
		}
		if !hasUpsert {
			return marshalUpdateResponse(0, 0, nil)
		}
		// Ordered missing-collection upserts retain the established preflight
		// rejection contract. Unordered commands instead validate every actual
		// candidate in the preview below so an indexed runtime error can continue
		// to a later valid upsert without creating an empty collection.
		if ordered {
			if err := s.validateMongoMissingCollectionFirstUpsert(parsed); err != nil {
				return mongoUpdateWriteCommandError(err)
			}
		}
		// A successful first upsert creates this collection. Preview response
		// admission before opening the catalog: otherwise a response-budget
		// rejection would leave an empty collection behind. For unordered writes,
		// an oversized earlier upsert must not hide a later admissible upsert; the
		// normal command loop records the same indexed errors after the collection
		// is opened. The preview never consumes the real budget.
		createIndex, create, writeErrors, err := s.mongoMissingCollectionFirstUpsertResponseAdmission(parsed, budget, ordered)
		if err != nil {
			return nil, err
		}
		if !create {
			return marshalUpdateResponseWithWriteErrors(0, 0, nil, writeErrors)
		}
		if s.mongoWriteBeforeFirstCreateHook != nil {
			s.mongoWriteBeforeFirstCreateHook(budget)
		}
		if err := budget.checkDeadline(); err != nil {
			return mongoUpdatePreMutationWriteErrorsResponse(budget, writeErrors, createIndex, err)
		}
		col, releaseColdCollection, err = s.openOrCreateCollectionForFirstWrite(name)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
	}
	return s.runMongoUpdateCommand(name, col, parsed, ordered)
}

type mongoUpdateItem struct {
	index           int
	key             []byte
	updateDoc       wire.Document
	setFields       map[string]struct{}
	bsonSetFields   []collections.BSONSetField
	setFieldsOK     bool
	bsonSetFieldsOK bool
	pureSet         bool
	mutation        mongoMutation
	upsert          bool
	multi           bool
	id              bson.RawValue
	plan            findPlan
	exactID         bool
	selector        *Server
	budget          *mongoWriteBudget
}

// mongoWriteBudget is local to one command. It prevents a batch of otherwise
// bounded filter specifications from multiplying the configured scan budget.
type mongoWriteBudget struct {
	examinedRemaining         int
	targetsRemaining          int
	retainedKeyBytesRemaining int
	errorsRemaining           int
	deadline                  time.Time
	responseBytesRemaining    int
	minimumResponseFits       bool
	// beforeUpsertInsertHook is test-only coordination between successful
	// response admission and the non-interruptible document publication.
	beforeUpsertInsertHook func()
	// beforeNativeUpdateBatchHook is test-only coordination after native batch
	// planning/materialization and immediately before its atomic publication.
	beforeNativeUpdateBatchHook func()
}

const (
	mongoWriteCommandMaxDuration         = 5 * time.Second
	mongoWriteCommandMaxRetainedKeyBytes = 16 << 20
	mongoWriteCommandMaxErrorEntries     = 10_000
	mongoWriteErrorMessageMaxRunes       = 128
	// A write error has a bounded 128-rune message (at most 512 UTF-8 bytes)
	// plus BSON element framing. Reserve a deliberately conservative amount so
	// a command never executes more errors than its advertised OP_MSG response
	// can carry.
	mongoWriteErrorResponseReserveBytes = 1024
	mongoWriteResponseMinimumBytes      = 128
)

func newMongoWriteBudget(limit int) *mongoWriteBudget {
	return &mongoWriteBudget{examinedRemaining: limit, targetsRemaining: limit, retainedKeyBytesRemaining: mongoWriteCommandMaxRetainedKeyBytes, errorsRemaining: mongoWriteCommandMaxErrorEntries - 1, responseBytesRemaining: int(wire.DefaultMaxMessageLength) - mongoWriteResponseMinimumBytes - mongoWriteErrorResponseReserveBytes, minimumResponseFits: true, deadline: time.Now().Add(mongoWriteCommandMaxDuration)}
}

func (s *Server) newMongoWriteBudget() *mongoWriteBudget {
	budget := newMongoWriteBudget(s.maxFindScanDocuments())
	budget.targetsRemaining = s.maxMongoWriteTargets()
	if s != nil && s.mongoWriteRetainedKeyBytesLimit > 0 {
		budget.retainedKeyBytesRemaining = s.mongoWriteRetainedKeyBytesLimit
	}
	budget.minimumResponseFits = int(s.maxMessageLength()) >= mongoWriteResponseMinimumBytes+mongoWriteErrorResponseReserveBytes
	budget.responseBytesRemaining = int(s.maxMessageLength()) - mongoWriteResponseMinimumBytes - mongoWriteErrorResponseReserveBytes
	if budget.responseBytesRemaining < 0 {
		budget.responseBytesRemaining = 0
	}
	return budget
}

// newMongoWriteBudgetForCommand applies the gateway ceiling together with a
// valid client maxTimeMS and any caller context deadline.  Atomic collection
// calls remain non-interruptible granules, but every selector and mutation
// boundary observes this shared deadline before it enters the granule.
func (s *Server) newMongoWriteBudgetForCommand(ctx context.Context, command wire.Document) (*mongoWriteBudget, error) {
	budget := s.newMongoWriteBudget()
	maxTimeMS, present, err := optionalPositiveIntFieldWithPresence(command, "maxTimeMS")
	if err != nil {
		return nil, err
	}
	if present {
		deadline := time.Now().Add(time.Duration(maxTimeMS) * time.Millisecond)
		if deadline.Before(budget.deadline) {
			budget.deadline = deadline
		}
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok && deadline.Before(budget.deadline) {
			budget.deadline = deadline
		}
	}
	return budget, nil
}

func (s *Server) maxMongoWriteTargets() int {
	if s != nil && s.mongoWriteTargetLimit > 0 {
		return s.mongoWriteTargetLimit
	}
	return defaultMaxWriteBatchSize
}
func (b *mongoWriteBudget) charge() error {
	if b == nil {
		return nil
	}
	if time.Now().After(b.deadline) {
		return errors.New("Mongo gateway multi-write command exceeded its execution-time budget")
	}
	if b.examinedRemaining <= 0 {
		return errors.New("Mongo gateway multi-write command exceeded its shared document-work budget")
	}
	b.examinedRemaining--
	return nil
}

func (b *mongoWriteBudget) reserveTarget() error {
	if b == nil {
		return nil
	}
	if time.Now().After(b.deadline) {
		return errors.New("Mongo gateway multi-write command exceeded its execution-time budget")
	}
	if b.targetsRemaining <= 0 {
		return errors.New("Mongo gateway multi-write command exceeded its retained-target budget")
	}
	b.targetsRemaining--
	return nil
}

// reserveTargetKey admits a key retained between the natural-order scan and
// its later conditional mutation.  Counting targets alone is not enough: BSON
// _id values can be large, so the retained key bytes have their own command
// ceiling.
func (b *mongoWriteBudget) reserveTargetKey(keyBytes int) error {
	if b == nil {
		return nil
	}
	if keyBytes < 0 {
		return errors.New("Mongo gateway multi-write command has an invalid retained key size")
	}
	if time.Now().After(b.deadline) {
		return errors.New("Mongo gateway multi-write command exceeded its execution-time budget")
	}
	if b.targetsRemaining <= 0 {
		return errors.New("Mongo gateway multi-write command exceeded its retained-target budget")
	}
	if b.retainedKeyBytesRemaining < keyBytes {
		return errors.New("Mongo gateway multi-write command exceeded its retained-key byte budget")
	}
	b.targetsRemaining--
	b.retainedKeyBytesRemaining -= keyBytes
	return nil
}

// reserveTargets reserves an entire non-interruptible mutation granule.  It
// deliberately checks capacity before decrementing so an over-cap native batch
// is rejected without consuming budget or applying any prefix.
func (b *mongoWriteBudget) reserveTargets(count int) error {
	if b == nil || count == 0 {
		return nil
	}
	if count < 0 {
		return errors.New("Mongo gateway multi-write command has an invalid target reservation")
	}
	if time.Now().After(b.deadline) {
		return errors.New("Mongo gateway multi-write command exceeded its execution-time budget")
	}
	if b.targetsRemaining < count {
		return errors.New("Mongo gateway multi-write command exceeded its retained-target budget")
	}
	b.targetsRemaining -= count
	return nil
}

func (b *mongoWriteBudget) refundTargets(count int) {
	if b != nil && count > 0 {
		b.targetsRemaining += count
	}
}

func (b *mongoWriteBudget) reserveError() error {
	if b == nil {
		return nil
	}
	if b.errorsRemaining <= 0 {
		return errors.New("Mongo gateway multi-write command exceeded its write-error budget")
	}
	if b.responseBytesRemaining < mongoWriteErrorResponseReserveBytes {
		return errors.New("Mongo gateway multi-write command exceeded its write-error response budget")
	}
	b.errorsRemaining--
	b.responseBytesRemaining -= mongoWriteErrorResponseReserveBytes
	return nil
}

// reserveUpsertResponse admits the exact BSON entry that a successful upsert
// adds to the command response.  It runs before Insert: unlike write errors,
// an upsert is a successful side effect and cannot honestly be discarded after
// publication merely because the response envelope has filled up.
func (b *mongoWriteBudget) reserveUpsertResponse(item mongoUpdateUpserted) (int, error) {
	if b == nil {
		return 0, nil
	}
	bytes, err := b.upsertResponseBytesAvailable(item)
	if err != nil {
		return 0, err
	}
	b.responseBytesRemaining -= bytes
	return bytes, nil
}

// upsertResponseBytesAvailable checks the exact successful-upsert response
// entry without consuming it. Missing-collection writes use this before
// opening the catalog; the side-effecting insert still reserves the same
// amount immediately before publication.
func (b *mongoWriteBudget) upsertResponseBytesAvailable(item mongoUpdateUpserted) (int, error) {
	if b == nil {
		return 0, nil
	}
	bytes, err := mongoUpdateUpsertResponseBytes(item)
	if err != nil {
		return 0, err
	}
	if b.responseBytesRemaining < bytes {
		return 0, errors.New("Mongo gateway multi-write command exceeded its upsert response budget")
	}
	return bytes, nil
}

func mongoUpdateUpsertResponseBytes(item mongoUpdateUpserted) (int, error) {
	entry, err := bson.Marshal(bson.D{{Key: "index", Value: int32(item.index)}, {Key: "_id", Value: item.id}})
	if err != nil {
		return 0, fmt.Errorf("Mongo gateway cannot size upsert response entry: %w", err)
	}
	// Account for the array element type/key and the upserted field/array
	// framing. The fixed allowance deliberately exceeds the decimal array-key
	// and BSON container overhead at the supported write batch size.
	const responseFramingReserve = 64
	return len(entry) + responseFramingReserve, nil
}

func (b *mongoWriteBudget) refundResponseBytes(bytes int) {
	if b != nil && bytes > 0 {
		b.responseBytesRemaining += bytes
	}
}

// reserveTerminalError consumes the response slot held back by
// ensureMinimumResponse. It is used only when execution has already stopped:
// the final indexed error must remain observable even at the minimum accepted
// response envelope.
func (b *mongoWriteBudget) reserveTerminalError() error {
	if b == nil {
		return nil
	}
	if !b.minimumResponseFits {
		return errors.New("Mongo gateway maxMessageSizeBytes is too small for a multi-write response")
	}
	if b.errorsRemaining < 0 {
		return errors.New("Mongo gateway multi-write command terminal write-error slot is already consumed")
	}
	b.errorsRemaining = -1
	return nil
}

func (b *mongoWriteBudget) ensureMinimumResponse() error {
	if b != nil && !b.minimumResponseFits {
		return errors.New("Mongo gateway maxMessageSizeBytes is too small for a multi-write response")
	}
	return nil
}

func (b *mongoWriteBudget) checkDeadline() error {
	if b != nil && time.Now().After(b.deadline) {
		return errors.New("Mongo gateway multi-write command exceeded its execution-time budget")
	}
	return nil
}

type mongoWriteError struct {
	index int
	err   error
}

func mongoCommandOrdered(command wire.Document) (bool, bool, error) {
	value := bson.Raw(command).Lookup("ordered")
	if value.IsZero() {
		return true, false, nil
	}
	ordered, ok := value.BooleanOK()
	if !ok {
		return false, true, errors.New("Mongo command field \"ordered\" must be a boolean")
	}
	return ordered, true, nil
}

// runMongoUpdateCommand has intentionally per-document atomicity only. A
// multi-write command is not a transaction: ordered commands stop on the first
// write error, while unordered commands continue and retain original indices.
func (s *Server) runMongoUpdateCommand(name string, col *collections.Collection, updates []mongoUpdateItem, ordered bool) (wire.Document, error) {
	// Restore the command-local native batch only for BSON $set exact-id
	// updates. This subset cannot hit a secondary-unique runtime conflict and
	// has already completed all state-independent validation, so the atomic
	// collection result preserves the command's per-item counts. All other
	// shapes retain the indexed-error sequential path below.
	batchable := len(updates) > 1 && mongoUpdateItemsCanUseBatch(col, updates)
	if batchable {
		seen := make(map[string]struct{}, len(updates))
		for _, update := range updates {
			if _, duplicate := seen[string(update.key)]; duplicate {
				batchable = false
				break
			}
			seen[string(update.key)] = struct{}{}
		}
	}
	if batchable {
		budget := updates[0].budget
		if err := budget.reserveTargets(len(updates)); err == nil {
			results, batched, batchErr := runMongoUpdateBatchResults(col, updates)
			if batchErr != nil {
				if errors.Is(batchErr, collections.ErrCommitAmbiguous) {
					return mongoCommitAmbiguousCommandError(batchErr)
				}
				// UpdateBatchItemError and other planner failures are known to have
				// occurred before publication. Fall back to the indexed sequential
				// path so unordered commands can continue at later original indices.
				batched = false
			}
			if batched {
				var matched, modified int32
				for _, result := range results {
					matched += boolToInt32(result.Matched)
					modified += boolToInt32(result.Modified)
				}
				return marshalUpdateResponseWithWriteErrors(matched, modified, nil, nil)
			}
			budget.refundTargets(len(updates))
		}
	}
	var matched, modified int32
	upserted := make([]mongoUpdateUpserted, 0)
	writeErrors := make([]mongoWriteError, 0)
	for _, update := range updates {
		matchedOne, modifiedOne, inserted, err := s.runMongoUpdateItem(name, col, update)
		if err != nil {
			if errors.Is(err, collections.ErrCommitAmbiguous) {
				return mongoCommitAmbiguousCommandError(err)
			}
			matched += matchedOne
			modified += modifiedOne
			if reserveErr := update.budget.reserveError(); reserveErr != nil {
				if terminalErr := update.budget.reserveTerminalError(); terminalErr != nil {
					return nil, terminalErr
				}
				writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: reserveErr})
				return marshalUpdateResponseWithWriteErrors(matched, modified, upserted, writeErrors)
			}
			writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: err})
			if ordered {
				break
			}
			continue
		}
		matched += matchedOne
		modified += modifiedOne
		if inserted {
			upserted = append(upserted, mongoUpdateUpserted{index: update.index, id: update.id})
		}
	}
	return marshalUpdateResponseWithWriteErrors(matched, modified, upserted, writeErrors)
}

func (s *Server) runMongoUpdateItem(name string, col *collections.Collection, update mongoUpdateItem) (int32, int32, bool, error) {
	if update.exactID {
		if err := update.budget.reserveTarget(); err != nil {
			return 0, 0, false, err
		}
	}
	if !update.multi {
		if !update.upsert {
			matched, modified, err := s.runMongoUpdateCoalesced(name, col, update)
			return boolToInt32(matched), boolToInt32(modified), false, err
		}
		matched, modified, inserted, err := runMongoUpdateOneWithUpsert(col, update)
		return boolToInt32(matched || inserted), boolToInt32(modified), inserted, err
	}
	if update.upsert && !update.exactID {
		return 0, 0, false, errors.New("Mongo gateway multi update upsert requires an exact _id equality filter")
	}
	if update.exactID {
		matched, modified, inserted, err := runMongoUpdateOneWithUpsert(col, update)
		return boolToInt32(matched || inserted), boolToInt32(modified), inserted, err
	}
	return s.runMongoFilterUpdateMany(col, update)
}

func boolToInt32(value bool) int32 {
	if value {
		return 1
	}
	return 0
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
	if !bson.Raw(update).Lookup("arrayFilters").IsZero() {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: Mongo gateway update does not support arrayFilters", index)}
	}
	for _, option := range []string{"collation", "hint"} {
		if !bson.Raw(update).Lookup(option).IsZero() {
			return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: Mongo gateway update does not support %s", index, option)}
		}
	}
	filter, err := requiredDocumentField(update, "q")
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	// Keep the long-standing scalar _id path allocation-light. Operator documents
	// deliberately fall through to the find parser: {_id: {$eq: ...}} is an
	// operator predicate, not an embedded-document _id.
	id, directErr := idEqualityFilterValue(filter, "update")
	directID := directErr == nil && !mongoIDOperatorDocument(id)
	var plan findPlan
	if !directID {
		plan, err = parseFindPlan(nil, filter)
		if err != nil {
			return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
		}
		if err := validateMongoWritePlan(plan); err != nil {
			return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
		}
		id, directID = simplePrimaryEqualityFindValue(plan)
	}
	multi, err := optionalBoolField(update, "multi")
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	upsert, err := optionalBoolField(update, "upsert")
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	if upsert && !directID {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: Mongo gateway update upsert requires an exact _id equality filter", index)}
	}
	var key []byte
	if directID {
		key, err = encodePrimaryKey(id)
		if err != nil {
			return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
		}
	}
	updateDoc, err := requiredDocumentField(update, "u")
	if err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	if err := validateMongoMutationTargetCount(updateDoc); err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	if err := validateMongoMutationOperandsNesting(updateDoc); err != nil {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
	}
	setFields, bsonSetFields, setFieldsOK := mongoSetUpdateFields(updateDoc)
	bsonSetFields, bsonSetFieldNames, bsonSetFieldsOK := mongoBSONSetUpdateFields(updateDoc)
	if !setFieldsOK && bsonSetFieldsOK {
		setFields = bsonSetFieldNames
	}
	var mutation mongoMutation
	pureSet := (setFieldsOK && len(setFields) != 0) || (bsonSetFieldsOK && len(bsonSetFields) != 0)
	if pureSet && len(setFields) > mongoMutationMaxTargets {
		return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: Mongo gateway update exceeds %d target fields", index, mongoMutationMaxTargets)}
	}
	if !pureSet {
		mutation, err = parseMongoMutation(updateDoc)
		if err != nil {
			return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: %v", index, err)}
		}
		// MongoDB only permits replacement-style updates for a single target.
		// Detect this while parsing the full command, so a later invalid item
		// cannot leave an earlier mutation behind.
		if multi && mutation.replace != nil {
			return mongoUpdateItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("updates[%d]: Mongo gateway multi update does not support replacement-style updates", index)}
		}
	}
	return mongoUpdateItem{
		index:           index,
		key:             key,
		updateDoc:       updateDoc,
		setFields:       setFields,
		bsonSetFields:   bsonSetFields,
		setFieldsOK:     setFieldsOK,
		bsonSetFieldsOK: bsonSetFieldsOK,
		pureSet:         pureSet,
		mutation:        mutation,
		upsert:          upsert,
		multi:           multi,
		id:              id,
		plan:            plan,
		exactID:         directID,
	}, nil
}

func mongoIDOperatorDocument(id bson.RawValue) bool {
	if id.Type != bson.TypeEmbeddedDocument {
		return false
	}
	elements, err := id.Document().Elements()
	if err != nil {
		return true
	}
	for _, element := range elements {
		key, err := element.KeyErr()
		if err != nil || strings.HasPrefix(key, "$") {
			return true
		}
	}
	return false
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
	matched, modified, _, err := runMongoUpdatesSequentialWithUpserts(col, updates)
	return matched, modified, err
}

type mongoUpdateUpserted struct {
	index int
	id    bson.RawValue
}

func runMongoUpdatesSequentialWithUpserts(col *collections.Collection, updates []mongoUpdateItem) (int32, int32, []mongoUpdateUpserted, error) {
	var matched int32
	var modified int32
	var upserted []mongoUpdateUpserted
	for _, update := range updates {
		matchedOne, modifiedOne, inserted, err := runMongoUpdateOneWithUpsert(col, update)
		if err != nil {
			return 0, 0, nil, mongoUpdateErrorWithIndex(update.index, err)
		}
		if matchedOne {
			matched++
		}
		if modifiedOne {
			modified++
		}
		if inserted {
			matched++
			upserted = append(upserted, mongoUpdateUpserted{index: update.index, id: update.id})
		}
	}
	return matched, modified, upserted, nil
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
	matched, modified, _, err := runMongoUpdateOneWithUpsert(col, update)
	return matched, modified, err
}

func runMongoUpdateOneWithUpsert(col *collections.Collection, update mongoUpdateItem) (bool, bool, bool, error) {
	if !update.exactID {
		if update.selector == nil {
			return false, false, false, errors.New("Mongo gateway filter update has no selector")
		}
		matched, modified, err := update.selector.runMongoFilterUpdateOne(col, update)
		return matched, modified, false, err
	}
	if mongoUpdateCanUseBSONSet(col, update) {
		matched, modified, err := col.UpdateBSONSet(update.key, update.bsonSetFields)
		if err != nil || matched || !update.upsert {
			return matched, modified, false, mongoUpdateErrorWithIndex(update.index, err)
		}
		if err := update.budget.checkDeadline(); err != nil {
			return matched, modified, false, err
		}
		return mongoInsertUpsert(col, update)
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return false, false, false, fmt.Errorf("updates[%d]: %w", update.index, err)
	}
	if materializer != nil {
		defer func() { _ = materializer.Close() }()
	}
	// Materializer setup can acquire a snapshot and refresh buffered template
	// state. Recheck immediately before the non-interruptible collection update
	// so a short command deadline cannot publish after that setup work.
	if err := update.budget.checkDeadline(); err != nil {
		return false, false, false, err
	}
	matched, modified, err := col.Update(update.key, func(stored []byte) ([]byte, bool, error) {
		return applyMongoUpdateToStoredDocument(col, materializer, update, stored)
	})
	if err != nil || matched || !update.upsert {
		return matched, modified, false, err
	}
	if err := update.budget.checkDeadline(); err != nil {
		return matched, modified, false, err
	}
	return mongoInsertUpsert(col, update)
}

func mongoInsertUpsert(col *collections.Collection, update mongoUpdateItem) (bool, bool, bool, error) {
	doc, err := mongoUpsertDocument(update)
	if err != nil {
		return false, false, false, err
	}
	key, stored, err := prepareInsertDocument(doc, col.MetaView().Options.DocumentFormat)
	if err != nil {
		return false, false, false, err
	}
	if !bytes.Equal(key, update.key) {
		return false, false, false, errors.New("Mongo gateway update cannot modify _id")
	}
	responseReservation, err := update.budget.reserveUpsertResponse(mongoUpdateUpserted{index: update.index, id: update.id})
	if err != nil {
		return false, false, false, err
	}
	if update.budget != nil && update.budget.beforeUpsertInsertHook != nil {
		update.budget.beforeUpsertInsertHook()
	}
	if err := update.budget.checkDeadline(); err != nil {
		update.budget.refundResponseBytes(responseReservation)
		return false, false, false, err
	}
	if _, err := col.Insert(key, stored); err != nil {
		update.budget.refundResponseBytes(responseReservation)
		return false, false, false, err
	}
	return false, false, true, nil
}

func (s *Server) validateMongoMissingCollectionUpsert(update mongoUpdateItem) error {
	doc, err := mongoUpsertDocument(update)
	if err != nil {
		return mongoUpdateErrorWithIndex(update.index, err)
	}
	key, _, err := prepareInsertDocument(doc, s.DefaultCollectionOptions.DocumentFormat)
	if err != nil {
		return mongoUpdateErrorWithIndex(update.index, err)
	}
	if !bytes.Equal(key, update.key) {
		return mongoUpdateErrorWithIndex(update.index, errors.New("Mongo gateway update cannot modify _id"))
	}
	return nil
}

// validateMongoMissingCollectionFirstUpsert remains the single-item helper
// used by findAndModify. Multi-update admission validates the actual selected
// candidate below because unordered response admission may skip earlier items.
func (s *Server) validateMongoMissingCollectionFirstUpsert(updates []mongoUpdateItem) error {
	for _, update := range updates {
		if update.upsert {
			return s.validateMongoMissingCollectionUpsert(update)
		}
	}
	return nil
}

// mongoMissingCollectionFirstUpsertResponseAdmission previews upsert response
// entries until one can create an otherwise missing collection. The copied
// budget makes this pre-catalog check side-effect free; runMongoUpdateCommand
// later consumes the same reservations and preserves indexed ordered/unordered
// behavior. If no upsert can be admitted, its complete bounded response is
// returned without creating a catalog entry.
func (s *Server) mongoMissingCollectionFirstUpsertResponseAdmission(updates []mongoUpdateItem, budget *mongoWriteBudget, ordered bool) (int, bool, []mongoWriteError, error) {
	preview := *budget
	writeErrors := make([]mongoWriteError, 0)
	for _, update := range updates {
		// Replay exact-ID target admission in command order. The actual command
		// reserves one target before each exact-ID update, including a no-match
		// non-upsert. Without this, a later upsert could appear admissible,
		// create the catalog, then fail after an earlier item consumes the last
		// target reservation.
		if update.exactID {
			if targetErr := preview.reserveTarget(); targetErr != nil {
				if reserveErr := preview.reserveError(); reserveErr != nil {
					if terminalErr := preview.reserveTerminalError(); terminalErr != nil {
						return -1, false, nil, terminalErr
					}
					writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: reserveErr})
					return -1, false, writeErrors, nil
				}
				writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: targetErr})
				if ordered {
					return -1, false, writeErrors, nil
				}
				continue
			}
		}
		if !update.upsert {
			continue
		}
		if _, err := preview.upsertResponseBytesAvailable(mongoUpdateUpserted{index: update.index, id: update.id}); err == nil {
			// Validate exactly the upsert that would cause a first-write catalog
			// creation. An unordered oversized earlier item can be skipped, so
			// validating only the first upsert would otherwise create an empty
			// collection for a later semantically invalid candidate.
			if validateErr := s.validateMongoMissingCollectionUpsert(update); validateErr == nil {
				return update.index, true, writeErrors, nil
			} else if reserveErr := preview.reserveError(); reserveErr != nil {
				if terminalErr := preview.reserveTerminalError(); terminalErr != nil {
					return -1, false, nil, terminalErr
				}
				writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: reserveErr})
				return -1, false, writeErrors, nil
			} else {
				writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: validateErr})
				if ordered {
					return -1, false, writeErrors, nil
				}
				continue
			}
		} else if reserveErr := preview.reserveError(); reserveErr != nil {
			if terminalErr := preview.reserveTerminalError(); terminalErr != nil {
				return -1, false, nil, terminalErr
			}
			writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: reserveErr})
			return -1, false, writeErrors, nil
		} else {
			writeErrors = append(writeErrors, mongoWriteError{index: update.index, err: err})
			if ordered {
				return -1, false, writeErrors, nil
			}
		}
	}
	return -1, false, writeErrors, nil
}

func mongoUpsertDocument(update mongoUpdateItem) (wire.Document, error) {
	base, err := marshalDocument(bson.D{{Key: "_id", Value: update.id}})
	if err != nil {
		return nil, err
	}
	var doc wire.Document
	if update.pureSet {
		if update.bsonSetFieldsOK && !update.setFieldsOK {
			doc, _, err = applyBSONSetUpdate(base, update.bsonSetFields)
		} else {
			doc, _, err = applySetUpdate(base, update.updateDoc)
		}
	} else {
		doc, _, err = applyMongoMutationWithOptions(base, update.mutation, true)
	}
	if err != nil {
		return nil, err
	}
	return doc, nil
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
	if mongoUpdateItemsCanUseBSONSet(col, updates) {
		items := make([]collections.BSONSetUpdateBatchItem, len(updates))
		for i, update := range updates {
			items[i] = collections.BSONSetUpdateBatchItem{
				DocumentID: update.key,
				Fields:     update.bsonSetFields,
			}
		}
		if len(updates) > 0 && updates[0].budget != nil && updates[0].budget.beforeNativeUpdateBatchHook != nil {
			updates[0].budget.beforeNativeUpdateBatchHook()
		}
		if len(updates) > 0 && updates[0].budget != nil {
			if err := updates[0].budget.checkDeadline(); err != nil {
				return nil, false, err
			}
		}
		return col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(items)
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
	if len(updates) > 0 && updates[0].budget != nil && updates[0].budget.beforeNativeUpdateBatchHook != nil {
		updates[0].budget.beforeNativeUpdateBatchHook()
	}
	if len(updates) > 0 && updates[0].budget != nil {
		if err := updates[0].budget.checkDeadline(); err != nil {
			return nil, false, err
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
	var updated wire.Document
	var changed bool
	if update.pureSet {
		if normalizedMongoUpdateDocumentFormat(col) == collections.DocumentFormatBSON && update.bsonSetFieldsOK {
			updated, changed, err = applyBSONSetUpdate(raw, update.bsonSetFields)
		} else {
			updated, changed, err = applySetUpdate(raw, update.updateDoc)
		}
	} else {
		updated, changed, err = applyMongoMutation(raw, update.mutation)
	}
	if err != nil {
		return nil, false, fmt.Errorf("updates[%d]: %w", update.index, err)
	}
	updatedKey, encoded, err := prepareInsertDocument(updated, col.MetaView().Options.DocumentFormat)
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
	if err := update.budget.checkDeadline(); err != nil {
		return false, false, err
	}
	if !mongoUpdateCanUseBatch(col, update) {
		return runMongoUpdateOne(col, update)
	}
	coalescer := s.mongoUpdateCoalescer(name)
	if coalescer == nil {
		return runMongoUpdateOne(col, update)
	}
	done := make(chan mongoUpdateCoalescerResult, 1)
	if !coalescer.enqueue(mongoUpdateCoalescerRequest{col: col, item: update, done: done}) {
		if err := update.budget.checkDeadline(); err != nil {
			return false, false, err
		}
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
		runMongoUpdateCoalescerSequential([]mongoUpdateCoalescerRequest{req})
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
	batch = filterExpiredMongoUpdateCoalescerRequests(batch)
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

func filterExpiredMongoUpdateCoalescerRequests(batch []mongoUpdateCoalescerRequest) []mongoUpdateCoalescerRequest {
	kept := batch[:0]
	for _, req := range batch {
		if err := req.item.budget.checkDeadline(); err != nil {
			req.done <- mongoUpdateCoalescerResult{err: err}
			continue
		}
		kept = append(kept, req)
	}
	return kept
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
		if err := req.item.budget.checkDeadline(); err != nil {
			req.done <- mongoUpdateCoalescerResult{err: err}
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
	meta := batch[0].col.MetaView()
	for _, req := range batch {
		if !mongoUpdateCanUseBatchMeta(meta, req.item) {
			return false
		}
	}
	return true
}

func runMongoUpdateCoalescerSequential(batch []mongoUpdateCoalescerRequest) {
	for _, req := range batch {
		if err := req.item.budget.checkDeadline(); err != nil {
			req.done <- mongoUpdateCoalescerResult{err: err}
			continue
		}
		matched, modified, err := runMongoUpdateOne(req.col, req.item)
		req.done <- mongoUpdateCoalescerResult{matched: matched, modified: modified, err: err}
	}
}

func mongoUpdateItemsCanUseBatch(col *collections.Collection, updates []mongoUpdateItem) bool {
	if col == nil {
		return false
	}
	meta := col.MetaView()
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
	return mongoUpdateCanUseBatchMeta(col.MetaView(), update)
}

func mongoUpdateItemsCanUseBSONSet(col *collections.Collection, updates []mongoUpdateItem) bool {
	if col == nil || normalizedMongoUpdateDocumentFormat(col) != collections.DocumentFormatBSON {
		return false
	}
	for _, update := range updates {
		if !update.exactID || !update.bsonSetFieldsOK || mongoBSONSetFieldsNeedNestingValidation(update.bsonSetFields) {
			return false
		}
	}
	return true
}

func mongoUpdateCanUseBSONSet(col *collections.Collection, update mongoUpdateItem) bool {
	return col != nil && update.exactID && !update.upsert && normalizedMongoUpdateDocumentFormat(col) == collections.DocumentFormatBSON && update.bsonSetFieldsOK && !mongoBSONSetFieldsNeedNestingValidation(update.bsonSetFields)
}

func mongoBSONSetFieldsNeedNestingValidation(fields []collections.BSONSetField) bool {
	for _, field := range fields {
		if !mongoBSONSetFieldFitsResultNesting(field.Value) {
			return true
		}
	}
	return false
}

// mongoBSONSetFieldFitsResultNesting accounts for the document root added by
// the structured BSON-set path without routing ordinary shallow containers.
func mongoBSONSetFieldFitsResultNesting(value bson.RawValue) bool {
	if _, container := mongoMutationDecodeContainer(value); !container {
		return true
	}
	index, doc := bsoncore.AppendDocumentStart(nil)
	doc = bsoncore.AppendValueElement(doc, "v", bsoncore.Value{Type: bsoncore.Type(value.Type), Data: value.Value})
	doc, _ = bsoncore.AppendDocumentEnd(doc, index)
	return validateMongoMutationRawNesting(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: doc}) == nil
}

func normalizedMongoUpdateDocumentFormat(col *collections.Collection) collections.DocumentFormat {
	if col == nil {
		return collections.DocumentFormatDefault
	}
	format := col.MetaView().Options.DocumentFormat
	if format == collections.DocumentFormatDefault {
		return collections.DocumentFormatJSON
	}
	return format
}

func mongoUpdateCanUseBatchMeta(meta collections.CollectionMeta, update mongoUpdateItem) bool {
	if !update.exactID || update.upsert {
		return false
	}
	format := meta.Options.DocumentFormat
	if format == collections.DocumentFormatDefault {
		format = collections.DocumentFormatJSON
	}
	if format == collections.DocumentFormatBSON {
		if !update.bsonSetFieldsOK {
			return false
		}
	} else if !update.setFieldsOK {
		return false
	}
	return !mongoUpdateSetFieldsTouchSecondaryUniqueIndexMeta(meta, update)
}

func mongoUpdateSetFieldsTouchSecondaryUniqueIndexMeta(meta collections.CollectionMeta, update mongoUpdateItem) bool {
	if update.setFields == nil {
		return true
	}
	updatedTopLevel := make(map[string]struct{}, len(update.setFields))
	for field := range update.setFields {
		topLevel, _, _ := strings.Cut(field, ".")
		updatedTopLevel[topLevel] = struct{}{}
	}
	for _, idx := range meta.Indexes {
		if !idx.Unique {
			continue
		}
		if len(idx.Components) == 0 {
			// Legacy indexes keep their compact Field-only metadata.
			topLevel, _, _ := strings.Cut(idx.Field, ".")
			if _, ok := updatedTopLevel[topLevel]; ok {
				return true
			}
			continue
		}
		for _, component := range idx.Components {
			topLevel, _, _ := strings.Cut(component.Field, ".")
			if _, ok := updatedTopLevel[topLevel]; ok {
				return true
			}
		}
	}
	return false
}

func mongoSetUpdateFields(updateDoc wire.Document) (map[string]struct{}, []collections.BSONSetField, bool) {
	updateElements, err := bson.Raw(updateDoc).Elements()
	if err != nil || len(updateElements) != 1 {
		return nil, nil, false
	}
	operator, err := updateElements[0].KeyErr()
	if err != nil || operator != "$set" {
		return nil, nil, false
	}
	setDoc, ok := updateElements[0].Value().DocumentOK()
	if !ok {
		return nil, nil, false
	}
	sets, order, err := parseSetDocument(setDoc)
	if err != nil {
		return nil, nil, false
	}
	out := make(map[string]struct{}, len(order))
	fields := make([]collections.BSONSetField, 0, len(order))
	for _, field := range order {
		out[field] = struct{}{}
		fields = append(fields, collections.BSONSetField{
			Key:   field,
			Value: sets[field],
		})
	}
	return out, fields, true
}

func mongoBSONSetUpdateFields(updateDoc wire.Document) ([]collections.BSONSetField, map[string]struct{}, bool) {
	updateElements, err := bson.Raw(updateDoc).Elements()
	if err != nil || len(updateElements) != 1 {
		return nil, nil, false
	}
	operator, err := updateElements[0].KeyErr()
	if err != nil || operator != "$set" {
		return nil, nil, false
	}
	setDoc, ok := updateElements[0].Value().DocumentOK()
	if !ok {
		return nil, nil, false
	}
	sets, order, err := parseBSONSetDocument(setDoc)
	if err != nil {
		return nil, nil, false
	}
	fields := make([]collections.BSONSetField, 0, len(order))
	fieldNames := make(map[string]struct{}, len(order))
	for _, field := range order {
		fieldNames[field] = struct{}{}
		fields = append(fields, collections.BSONSetField{
			Key:   field,
			Value: sets[field],
		})
	}
	return fields, fieldNames, true
}

func (s *Server) deleteResponse(ctx context.Context, command wire.Document, sequences []wire.DocumentSequence) (wire.Document, error) {
	if s.clusterSubmitterConfigured() {
		return s.clusterDeleteResponse(ctx, command, sequences)
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "delete"); rejected {
		return doc, err
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
	ordered, _, err := mongoCommandOrdered(command)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if len(deletes) > defaultMaxWriteBatchSize {
		return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("Mongo gateway delete exceeds maxWriteBatchSize %d", defaultMaxWriteBatchSize))
	}
	// OP_MSG MaxMessageLength bounds BSON bytes before dispatch; this cap bounds
	// per-spec result/error bookkeeping independently of command byte size.
	parsed := make([]mongoDeleteItem, len(deletes))
	budget, err := s.newMongoWriteBudgetForCommand(ctx, command)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	for i, deleteItem := range deletes {
		item, parseErr := parseMongoDeleteItem(i, deleteItem)
		if parseErr != nil {
			return mongoUpdateParseCommandError(parseErr)
		}
		parsed[i] = item
		parsed[i].budget = budget
	}
	if err := budget.ensureMinimumResponse(); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	col, err := s.openCollectionForMutation(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return marshalDeleteResponse(0)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	var deleted int32
	writeErrors := make([]mongoWriteError, 0)
	for _, item := range parsed {
		count, runErr := s.runMongoDeleteItem(col, item)
		if runErr != nil {
			if errors.Is(runErr, collections.ErrCommitAmbiguous) {
				return mongoCommitAmbiguousCommandError(runErr)
			}
			deleted += count
			if reserveErr := item.budget.reserveError(); reserveErr != nil {
				if terminalErr := item.budget.reserveTerminalError(); terminalErr != nil {
					return nil, terminalErr
				}
				writeErrors = append(writeErrors, mongoWriteError{index: item.index, err: reserveErr})
				return marshalDeleteResponseWithWriteErrors(deleted, writeErrors)
			}
			writeErrors = append(writeErrors, mongoWriteError{index: item.index, err: runErr})
			if ordered {
				break
			}
			continue
		}
		deleted += count
	}
	return marshalDeleteResponseWithWriteErrors(deleted, writeErrors)
}

type mongoDeleteItem struct {
	index   int
	plan    findPlan
	limit   int32
	exactID bool
	key     []byte
	budget  *mongoWriteBudget
}

func parseMongoDeleteItem(index int, deleteItem wire.Document) (mongoDeleteItem, error) {
	for _, option := range []string{"collation", "hint"} {
		if !bson.Raw(deleteItem).Lookup(option).IsZero() {
			return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("deletes[%d]: Mongo gateway delete does not support %s", index, option)}
		}
	}
	filter, err := requiredDocumentField(deleteItem, "q")
	if err != nil {
		return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("deletes[%d]: %v", index, err)}
	}
	plan, err := parseFindPlan(nil, filter)
	if err != nil {
		return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("deletes[%d]: %v", index, err)}
	}
	if err := validateMongoWritePlan(plan); err != nil {
		return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("deletes[%d]: %v", index, err)}
	}
	limit, limitSet, err := optionalInt32FieldWithPresence(deleteItem, "limit")
	if err != nil {
		return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("deletes[%d]: %v", index, err)}
	}
	if !limitSet {
		return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("deletes[%d]: Mongo command missing \"limit\"", index)}
	}
	if limit != 0 && limit != 1 {
		return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("deletes[%d]: Mongo gateway delete limit must be 0 or 1", index)}
	}
	item := mongoDeleteItem{index: index, plan: plan, limit: limit}
	if id, exact := simplePrimaryEqualityFindValue(plan); exact {
		key, err := encodePrimaryKey(id)
		if err != nil {
			return mongoDeleteItem{}, mongoUpdateParseError{code: commandCodeBadValue, codeName: "BadValue", message: fmt.Sprintf("deletes[%d]: %v", index, err)}
		}
		item.exactID, item.key = true, key
	}
	return item, nil
}

func (s *Server) runMongoDeleteItem(col *collections.Collection, item mongoDeleteItem) (int32, error) {
	if item.exactID {
		if err := item.budget.reserveTarget(); err != nil {
			return 0, err
		}
		deleted, err := col.DeleteDocument(item.key)
		return boolToInt32(deleted), err
	}
	if item.limit == 0 {
		return s.deleteMongoFilterMany(col, item.plan, item.budget)
	}
	deleted, err := s.deleteMongoFilterOneWithBudget(col, item.plan, item.budget)
	return boolToInt32(deleted), err
}

func (s *Server) listCollectionsResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if doc, err, rejected := s.rejectClusterRoutedLocalMetadataRead("listCollections"); rejected {
		return doc, err
	}
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
		if s.authenticationRequired() && !s.authorizedResource(cursorOwner, db, collectionName, authorizationMetadataRead) {
			continue
		}
		firstBatch = append(firstBatch, mongoCollectionDocument(collectionName, nameOnly))
	}
	return marshalCursorResponse(db, "$cmd.listCollections", firstBatch)
}

func (s *Server) listDatabasesResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if doc, err, rejected := s.rejectClusterRoutedLocalMetadataRead("listDatabases"); rejected {
		return doc, err
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	filter, err := commandOptionalDocument(command, "filter")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	nameFilter, err := databaseNameFilter(filter)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}

	metas, err := s.Collections.ListCollections()
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	names := make(map[string]struct{})
	for _, meta := range metas {
		db, _, ok := strings.Cut(meta.Name, ".")
		if !ok || db == "" {
			continue
		}
		if nameFilter != "" && db != nameFilter {
			continue
		}
		if s.authenticationRequired() && !s.authorizedResource(cursorOwner, db, "", authorizationListDatabases) {
			continue
		}
		names[db] = struct{}{}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	databases := make(bson.A, 0, len(ordered))
	for _, name := range ordered {
		databases = append(databases, bson.D{
			{Key: "name", Value: name},
			{Key: "sizeOnDisk", Value: int64(0)},
			{Key: "empty", Value: false},
		})
	}
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "databases", Value: databases},
		{Key: "totalSize", Value: int64(0)},
		{Key: "totalSizeMb", Value: int64(0)},
	})
}

func (s *Server) createCollectionResponse(ctx context.Context, command wire.Document) (wire.Document, error) {
	if s.clusterSubmitterConfigured() {
		return s.clusterCreateCollectionResponse(ctx, command)
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "create"); rejected {
		return doc, err
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
	if _, err := s.Collections.OpenCollection(name); err == nil {
		return marshalDocument(bson.D{
			{Key: "ok", Value: 1.0},
			{Key: "note", Value: "TreeDB Mongo gateway treats duplicate create as idempotent success"},
		})
	} else if !errors.Is(err, collections.ErrCollectionNotFound) {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if _, err := s.Collections.CreateCollection(s.defaultCollectionMeta(name)); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	s.invalidateCollectionCache(name)
	return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})
}

func validateCreateCollectionCommand(command wire.Document) error {
	capped, err := optionalBoolField(command, "capped")
	if err != nil {
		return err
	}
	if capped {
		return errors.New("Mongo gateway create does not support capped collections")
	}
	elements, err := bson.Raw(command).Elements()
	if err != nil {
		return err
	}
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return err
		}
		if key == "create" || isMongoCommandMetadataField(key) {
			continue
		}
		switch key {
		case "capped":
		default:
			return fmt.Errorf("Mongo gateway create does not support option %q", key)
		}
	}
	return nil
}

func isMongoCommandMetadataField(key string) bool {
	switch key {
	case "$db", "$clusterTime", "$readPreference", "lsid", "comment", "writeConcern", "readConcern", "readPreference", "maxTimeMS", "apiVersion", "apiStrict", "apiDeprecationErrors":
		return true
	default:
		return false
	}
}

func endSessionsResponse(command wire.Document) (wire.Document, error) {
	if err := validateEndSessionsCommand(command); err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})
}

func validateEndSessionsCommand(command wire.Document) error {
	sessions, err := commandDocumentArray(command, "endSessions")
	if err != nil {
		return err
	}
	for i, session := range sessions {
		id := bson.Raw(session).Lookup("id")
		if id.IsZero() {
			return fmt.Errorf("Mongo command field \"endSessions[%d].id\" is required", i)
		}
		subtype, data, ok := id.BinaryOK()
		if !ok {
			return fmt.Errorf("Mongo command field \"endSessions[%d].id\" must be binary UUID subtype 4", i)
		}
		if subtype != 4 || len(data) != 16 {
			return fmt.Errorf("Mongo command field \"endSessions[%d].id\" must be binary UUID subtype 4 with 16 bytes", i)
		}
	}
	return nil
}

func rejectTransactionalCommand(command wire.Document, commandName string) (wire.Document, bool, error) {
	raw := bson.Raw(command)
	hasTransactionMarker := !raw.Lookup("startTransaction").IsZero() || !raw.Lookup("autocommit").IsZero()
	hasRetryableWriteMarker := !raw.Lookup("txnNumber").IsZero()
	if !hasTransactionMarker && !hasRetryableWriteMarker {
		return nil, false, nil
	}
	// The gateway advertises logical sessions for driver compatibility but not a
	// replica-set setName. If a client forces retryable-write txnNumber anyway,
	// reject it rather than pretending idempotency bookkeeping exists.
	doc, err := commandError(
		commandCodeBadValue,
		"BadValue",
		"Mongo gateway "+commandName+" does not support transactions or retryable writes",
	)
	return doc, true, err
}

func rejectUnsupportedReadConcern(command wire.Document) (wire.Document, bool, error) {
	if err := parseMongoReadConcern(command); err != nil {
		doc, docErr := mongoReadConcernCommandError(err)
		return doc, true, docErr
	}
	return nil, false, nil
}

type mongoReadConcernParseError struct {
	code     int32
	codeName string
	message  string
}

func (e mongoReadConcernParseError) Error() string {
	return e.message
}

func mongoReadConcernFailedToParse(format string, args ...any) error {
	return mongoReadConcernParseError{
		code:     commandCodeFailedToParse,
		codeName: "FailedToParse",
		message:  fmt.Sprintf(format, args...),
	}
}

func mongoReadConcernBadValue(format string, args ...any) error {
	return mongoReadConcernParseError{
		code:     commandCodeBadValue,
		codeName: "BadValue",
		message:  fmt.Sprintf(format, args...),
	}
}

func mongoReadConcernCommandError(err error) (wire.Document, error) {
	var parseErr mongoReadConcernParseError
	if errors.As(err, &parseErr) {
		return commandError(parseErr.code, parseErr.codeName, parseErr.message)
	}
	return commandError(commandCodeBadValue, "BadValue", err.Error())
}

func parseMongoReadConcern(command wire.Document) error {
	elements, err := bson.Raw(command).Elements()
	if err != nil {
		return mongoReadConcernFailedToParse("Mongo command is malformed: %v", err)
	}
	seenReadConcern := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return mongoReadConcernFailedToParse("Mongo command is malformed: %v", err)
		}
		if key != "readConcern" {
			continue
		}
		if seenReadConcern {
			return mongoReadConcernBadValue("Mongo command field \"readConcern\" is duplicated")
		}
		seenReadConcern = true
		if err := parseMongoReadConcernValue(elem.Value()); err != nil {
			return err
		}
	}
	return nil
}

func parseMongoReadConcernValue(value bson.RawValue) error {
	readConcern, ok := value.DocumentOK()
	if !ok {
		return mongoReadConcernFailedToParse("Mongo command field \"readConcern\" must be a document")
	}
	elements, err := readConcern.Elements()
	if err != nil {
		return mongoReadConcernFailedToParse("Mongo command field \"readConcern\" is malformed: %v", err)
	}
	seenLevel := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return mongoReadConcernFailedToParse("Mongo command field \"readConcern\" is malformed: %v", err)
		}
		switch key {
		case "level":
			if seenLevel {
				return mongoReadConcernBadValue("Mongo readConcern field \"level\" is duplicated")
			}
			seenLevel = true
			level, ok := elem.Value().StringValueOK()
			if !ok {
				return mongoReadConcernFailedToParse("Mongo command field \"readConcern.level\" must be a string")
			}
			if !mongoReadConcernLevelIsLocalStale(level) {
				return mongoReadConcernBadValue("Mongo gateway readConcern level %q is not supported; only local-stale reads are supported", level)
			}
		case "afterClusterTime", "atClusterTime":
			return mongoReadConcernBadValue("Mongo gateway readConcern does not support %q", key)
		default:
			return mongoReadConcernBadValue("Mongo gateway readConcern does not support %q", key)
		}
	}
	return nil
}

func mongoReadConcernLevelIsLocalStale(level string) bool {
	switch level {
	case "local", "available":
		return true
	default:
		return false
	}
}

func (s *Server) createIndexesResponse(command wire.Document) (wire.Document, error) {
	if s.clusterSubmitterConfigured() {
		return mongoClusterUnsupportedIndexDDL()
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "createIndexes"); rejected {
		return doc, err
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
	scalarDefs := make([]collections.IndexDefinition, 0, len(indexDocs))
	var vectorDefs []collections.VectorIndexDefinition
	for i, indexDoc := range indexDocs {
		def, err := parseCreateIndexDefinition(indexDoc)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("indexes[%d]: %v", i, err))
		}
		if def.vector {
			vectorDefs = append(vectorDefs, def.vectorDef)
			continue
		}
		scalarDefs = append(scalarDefs, s.applyDefaultIndexOptions(def.scalarDef))
	}
	if err := validateCreateIndexesRequestDuplicates(scalarDefs, vectorDefs); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	scalarDefs = dedupeIdenticalIndexDefinitions(scalarDefs)
	vectorDefs = dedupeIdenticalVectorIndexDefinitions(vectorDefs)
	if err := validateCreateIndexesCrossKindNames(collections.CollectionMeta{}, scalarDefs, vectorDefs); err != nil {
		return commandError(commandCodeDuplicateKey, "DuplicateKey", err.Error())
	}

	createdAutomatically := false
	s.invalidateCollectionCache(name)
	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if !errors.Is(err, collections.ErrCollectionNotFound) {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		meta := s.defaultCollectionMeta(name)
		meta.Indexes = scalarDefs
		meta.VectorIndexes = vectorDefs
		created, err := s.Collections.CreateCollection(meta)
		s.invalidateCollectionCache(name)
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
				{Key: "numIndexesAfter", Value: int32(1 + len(created.Indexes) + len(created.VectorIndexes))},
			})
		}
	}
	numBefore := int32(1 + len(col.MetaView().Indexes) + len(col.MetaView().VectorIndexes))
	meta := col.MetaView()
	if err := validateCreateIndexesCrossKindNames(meta, scalarDefs, vectorDefs); err != nil {
		return commandError(commandCodeDuplicateKey, "DuplicateKey", err.Error())
	}
	if err := validateCreateIndexesExistingVectorDefinitions(meta, vectorDefs); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	for _, def := range scalarDefs {
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
	for _, def := range vectorDefs {
		if existing, ok := findVectorIndexDefinition(meta.VectorIndexes, def.Name); ok && sameVectorIndexDefinition(existing, def) {
			continue
		}
		next, err := col.CreateVectorIndex(def)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		meta = *next
	}
	s.invalidateCollectionCache(name)
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "createdCollectionAutomatically", Value: createdAutomatically},
		{Key: "numIndexesBefore", Value: numBefore},
		{Key: "numIndexesAfter", Value: int32(1 + len(meta.Indexes) + len(meta.VectorIndexes))},
	})
}

func (s *Server) listIndexesResponse(command wire.Document) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if doc, err, rejected := s.rejectClusterRoutedLocalMetadataRead("listIndexes"); rejected {
		return doc, err
	}
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
	col, err := s.openCollectionCached(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return commandError(commandCodeNamespaceNotFound, "NamespaceNotFound", "collection not found: "+db+"."+collection)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	return marshalCursorResponse(db, collection, mongoIndexDocuments(col.MetaView()))
}

func (s *Server) dropIndexesResponse(command wire.Document) (wire.Document, error) {
	if s.clusterSubmitterConfigured() {
		return mongoClusterUnsupportedLocalMutation("dropIndexes")
	}
	if s.Collections == nil {
		return commandError(commandCodeBadValue, "BadValue", "Mongo gateway collection manager is not configured")
	}
	if doc, rejected, err := rejectTransactionalCommand(command, "dropIndexes"); rejected {
		return doc, err
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
	s.invalidateCollectionCache(name)
	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		if errors.Is(err, collections.ErrCollectionNotFound) {
			return commandError(commandCodeNamespaceNotFound, "NamespaceNotFound", "collection not found: "+db+"."+collection)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	metaBefore := col.MetaView()
	before := int32(1 + len(metaBefore.Indexes) + len(metaBefore.VectorIndexes))
	names, all, err := dropIndexNames(command)
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if all {
		if _, err := col.DropAllIndexes(); err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		for _, index := range metaBefore.VectorIndexes {
			if _, err := col.DropVectorIndex(index.Name); err != nil {
				return commandError(commandCodeBadValue, "BadValue", fmt.Sprintf("dropIndexes partially applied before vector index %q failed: %v", index.Name, err))
			}
		}
	} else {
		scalarNames, vectorNames, err := classifyDropIndexNames(metaBefore, names)
		if err != nil {
			if errors.Is(err, collections.ErrIndexNotFound) {
				return commandError(commandCodeIndexNotFound, "IndexNotFound", "index not found")
			}
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if len(scalarNames) > 0 {
			if _, err := col.DropIndexes(scalarNames); err != nil {
				if errors.Is(err, collections.ErrIndexNotFound) {
					return commandError(commandCodeIndexNotFound, "IndexNotFound", "index not found")
				}
				return commandError(commandCodeBadValue, "BadValue", err.Error())
			}
		}
		for _, indexName := range vectorNames {
			if _, err := col.DropVectorIndex(indexName); err != nil {
				if errors.Is(err, collections.ErrIndexNotFound) {
					return commandError(commandCodeIndexNotFound, "IndexNotFound", "index not found")
				}
				return commandError(commandCodeBadValue, "BadValue", err.Error())
			}
		}
	}
	s.invalidateCollectionCache(name)
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "nIndexesWas", Value: before},
	})
}

func (s *Server) getMoreResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
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

func marshalUpdateResponse(matched, modified int32, upserted ...[]mongoUpdateUpserted) (wire.Document, error) {
	response := bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: matched},
		{Key: "nModified", Value: modified},
	}
	if len(upserted) > 0 && len(upserted[0]) > 0 {
		items := make(bson.A, len(upserted[0]))
		for i, item := range upserted[0] {
			items[i] = bson.D{{Key: "index", Value: int32(item.index)}, {Key: "_id", Value: item.id}}
		}
		response = append(response, bson.E{Key: "upserted", Value: items})
	}
	return marshalDocument(response)
}

func marshalUpdateResponseWithWriteErrors(matched, modified int32, upserted []mongoUpdateUpserted, writeErrors []mongoWriteError) (wire.Document, error) {
	response := bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: matched}, {Key: "nModified", Value: modified}}
	if len(upserted) != 0 {
		items := make(bson.A, len(upserted))
		for i, item := range upserted {
			items[i] = bson.D{{Key: "index", Value: int32(item.index)}, {Key: "_id", Value: item.id}}
		}
		response = append(response, bson.E{Key: "upserted", Value: items})
	}
	return marshalWriteErrorsResponse(response, writeErrors)
}

func marshalDeleteResponse(deleted int32) (wire.Document, error) {
	return marshalDocument(bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "n", Value: deleted},
	})
}

func marshalInsertResponseWithWriteErrors(inserted int32, writeErrors []mongoWriteError) (wire.Document, error) {
	return marshalWriteErrorsResponse(bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: inserted}}, writeErrors)
}

func marshalDeleteResponseWithWriteErrors(deleted int32, writeErrors []mongoWriteError) (wire.Document, error) {
	return marshalWriteErrorsResponse(bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: deleted}}, writeErrors)
}

func marshalWriteErrorsResponse(response bson.D, writeErrors []mongoWriteError) (wire.Document, error) {
	if len(writeErrors) != 0 {
		items := make(bson.A, len(writeErrors))
		for i, item := range writeErrors {
			code, codeName := commandCodeBadValue, "BadValue"
			if collections.IsDuplicateKeyError(item.err) {
				code, codeName = commandCodeDuplicateKey, "DuplicateKey"
			}
			items[i] = bson.D{{Key: "index", Value: int32(item.index)}, {Key: "code", Value: code}, {Key: "codeName", Value: codeName}, {Key: "errmsg", Value: boundedMongoWriteErrorMessage(item.err)}}
		}
		response = append(response, bson.E{Key: "writeErrors", Value: items})
	}
	return marshalDocument(response)
}

func boundedMongoWriteErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToValidUTF8(err.Error(), "?")
	runes := []rune(message)
	if len(runes) <= mongoWriteErrorMessageMaxRunes {
		return message
	}
	return string(runes[:mongoWriteErrorMessageMaxRunes]) + "..."
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
	need := cursorDocumentsMsgResponseLength(ns, batchKey, batchBytes)
	if int64(need) > maxWireMessageLengthInt32Limit {
		return nil, fmt.Errorf("%w: length=%d", wire.ErrMessageTooLarge, need)
	}
	if need > maxMessageLength {
		return nil, fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, need, maxMessageLength)
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
	if maxBatchLimit := maxMessageLength - findBatchResponseReserveBytes; maxBatchLimit < maxBatchBytes {
		if maxBatchLimit < 0 {
			maxBatchLimit = 0
		}
		maxBatchBytes = maxBatchLimit
	}
	need := wire.HeaderLen + 5 + rawCursorResponseCapacityHint(ns, opts.Limit, maxBatchBytes)
	if need > maxMessageLength {
		need = maxMessageLength
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

func cursorDocumentsMsgResponseLength(ns, batchKey string, batchBytes int) int {
	const (
		messageHeaderAndMsgBodyBytes = wire.HeaderLen + 5
		responseDocumentFixedBytes   = 53
	)
	return messageHeaderAndMsgBodyBytes + responseDocumentFixedBytes + len(ns) + len(batchKey) + batchBytes
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
	// A zero-sized first batch would otherwise defer dotted-path validation to
	// getMore after publishing a cursor. Validate all already-bounded result
	// documents before that externally visible state transition.
	if projectionHasDottedPath(projection) {
		for _, doc := range docs {
			if _, err := projectDocumentWithProjection(doc, projection); err != nil {
				return 0, nil, err
			}
		}
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
	principal := AuthUser{}
	if user := s.authUser(owner); user != nil {
		principal = *user
	}
	s.addCursorLocked(cursorID, &serverCursor{ns: ns, owner: owner, principal: principal, docs: docs, projection: projection, pos: 0, lastUsed: now})
	return cursorID, nil
}

// openCompoundIDCursor retains only ordered primary keys from the bounded
// compound index walk. BSON decoding is intentionally deferred to the first
// batch/getMore path so a large cursor is not fully materialized up front.
func (s *Server) openCompoundIDCursor(ns string, col *collections.Collection, ids [][]byte, plan findPlan, batchSize int, explicitBatchSize bool, defaultBatchSize int, owner int64) (int64, bson.A, error) {
	if s.isClosed() {
		return 0, nil, errServerClosed
	}
	batchSize, err := normalizeBatchSize(batchSize, explicitBatchSize, defaultBatchSize)
	if err != nil {
		return 0, nil, err
	}
	if len(ids) == 0 {
		return 0, bson.A{}, nil
	}
	retained := 0
	for _, id := range ids {
		if len(id) > s.maxCursorRetainedBytes()-retained {
			return 0, nil, fmt.Errorf("%w: Mongo gateway compound cursor IDs exceed retained-byte cap", errMongoFindScanCapExceeded)
		}
		retained += len(id)
	}
	planBytes := findPlanCursorRetainedBytes(plan)
	if planBytes > s.maxCursorRetainedBytes()-retained {
		return 0, nil, fmt.Errorf("%w: Mongo gateway compound cursor plan exceeds retained-byte cap", errMongoFindScanCapExceeded)
	}
	retained += planBytes
	retainedPlan := cloneFindPlanForCursor(plan)
	// Compound cursors retain IDs, so their normal batch decoder has not yet
	// observed every result when batchSize is zero (or when a later getMore
	// would reach it). Validate dotted projections before publishing the cursor
	// just as openCursor does for retained BSON documents. This walks one
	// bounded ID at a time and retains no decoded BSON.
	if err := s.preflightCompoundCursorProjection(col, ids, &retainedPlan, plan.projection); err != nil {
		return 0, nil, err
	}
	cursor := &serverCursor{ns: ns, owner: owner, compoundIDs: ids, compoundCollection: col, compoundPlan: &retainedPlan, projection: plan.projection, lastUsed: time.Now()}
	batch, consumed, materialized, err := s.compoundCursorBatch(col, ids, &retainedPlan, plan.projection, 0, batchSize, 0)
	if err != nil {
		return 0, nil, err
	}
	cursor.pos, cursor.materializedBytes = consumed, materialized
	if consumed >= len(ids) {
		return 0, batch, nil
	}
	cursorID := s.nextCursorID.Add(1)
	if cursorID == 0 {
		cursorID = s.nextCursorID.Add(1)
	}
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	if s.isClosed() {
		return 0, nil, errServerClosed
	}
	s.reapExpiredCursorsLocked(time.Now())
	if s.cursors == nil {
		s.cursors = make(map[int64]*serverCursor)
	}
	if len(s.cursors) >= s.maxOpenCursors() {
		return 0, nil, errors.New("Mongo gateway cursor limit exceeded")
	}
	if user := s.authUser(owner); user != nil {
		cursor.principal = *user
	}
	s.addCursorLocked(cursorID, cursor)
	return cursorID, batch, nil
}

func (s *Server) preflightCompoundCursorProjection(col *collections.Collection, ids [][]byte, plan *findPlan, projection compiledProjection) error {
	if !projectionHasDottedPath(projection) {
		return nil
	}
	if col == nil {
		return errors.New("mongo gateway compound cursor has no collection")
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return err
	}
	defer func() { _ = materializer.Close() }()
	materialized := 0
	for _, id := range ids {
		stored, err := col.Get(id)
		if err != nil {
			return err
		}
		if len(stored) == 0 {
			continue
		}
		doc, err := storedDocumentToBSON(col, materializer, stored)
		if err != nil {
			return err
		}
		if len(doc) > s.maxCursorRetainedBytes()-materialized {
			return fmt.Errorf("%w: Mongo gateway compound cursor projection preflight exceeded %d bytes", errMongoFindScanCapExceeded, s.maxCursorRetainedBytes())
		}
		materialized += len(doc)
		if plan != nil {
			match, err := documentMatchesPlan(doc, *plan)
			if err != nil {
				return err
			}
			if !match {
				continue
			}
		}
		if _, err := projectDocumentWithProjection(doc, projection); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) compoundCursorBatch(col *collections.Collection, ids [][]byte, plan *findPlan, projection compiledProjection, start, maxDocs, committedMaterialized int) (bson.A, int, int, error) {
	if s.compoundCursorBatchHook != nil {
		s.compoundCursorBatchHook()
	}
	if col == nil {
		return nil, 0, 0, errors.New("mongo gateway compound cursor has no collection")
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return nil, 0, 0, err
	}
	defer func() { _ = materializer.Close() }()
	if maxDocs < 0 || maxDocs > len(ids)-start {
		maxDocs = len(ids) - start
	}
	out := make(bson.A, 0, maxDocs)
	batchBytes, materializedBytes, added, decoded := 0, 0, 0, 0
	for start+decoded < len(ids) && added < maxDocs {
		stored, err := col.Get(ids[start+decoded])
		if err != nil {
			return nil, 0, 0, err
		}
		decoded++
		if len(stored) == 0 {
			continue
		}
		doc, err := storedDocumentToBSON(col, materializer, stored)
		if err != nil {
			return nil, 0, 0, err
		}
		// Decoding is materialization even if a document updated between batches
		// no longer satisfies the retained predicate.  The ID is consumed below,
		// so charge it before the predicate recheck.
		if len(doc) > s.maxCursorRetainedBytes()-committedMaterialized-materializedBytes {
			return nil, 0, 0, fmt.Errorf("%w: Mongo gateway compound cursor materialization exceeded %d bytes", errMongoFindScanCapExceeded, s.maxCursorRetainedBytes())
		}
		materializedBytes += len(doc)
		if plan != nil {
			match, err := documentMatchesPlan(doc, *plan)
			if err != nil {
				return nil, 0, 0, err
			}
			if !match {
				continue
			}
		}
		projected, err := projectDocumentWithProjection(doc, projection)
		if err != nil {
			return nil, 0, 0, err
		}
		docBytes := findBatchDocumentBytes(projected, len(out))
		if findBatchOverheadBytes+docBytes > s.maxFindBatchBytes() {
			return nil, 0, 0, fmt.Errorf("mongo gateway cursor document exceeds max batch bytes: docBytes=%d maxBatchBytes=%d", docBytes, s.maxFindBatchBytes())
		}
		if len(out) > 0 && findBatchOverheadBytes+batchBytes+docBytes > s.maxFindBatchBytes() {
			decoded--
			materializedBytes -= len(doc)
			break
		}
		out = append(out, bson.Raw(projected))
		batchBytes += docBytes
		added++
	}
	return out, decoded, materializedBytes, nil
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
		principal := s.authUser(owner)
		if cursor == nil || cursor.ns != ns || cursor.owner != owner || !cursorPrincipalMatches(cursor, principal) {
			s.cursorMu.Unlock()
			return 0, nil, false, nil
		}
		if cursor.compoundCollection != nil {
			s.cursorMu.Unlock()
			// Do not allow concurrent getMore requests to decode the same IDs while
			// they race optimistically to commit cursor.pos. This per-cursor lock is
			// deliberately acquired after cursorMu so the winner can still publish
			// progress without a lock-order cycle.
			cursor.compoundBatchMu.Lock()
			s.cursorMu.Lock()
			current := s.cursors[cursorID]
			if current == nil || current != cursor || current.ns != ns || current.owner != owner || !cursorPrincipalMatches(current, s.authUser(owner)) {
				s.cursorMu.Unlock()
				cursor.compoundBatchMu.Unlock()
				return 0, nil, false, nil
			}
			startPos := current.pos
			committedMaterialized := current.materializedBytes
			compoundCollection := current.compoundCollection
			compoundIDs := current.compoundIDs
			compoundPlan := current.compoundPlan
			projection := current.projection
			effectiveBatchSize := batchSize
			if !explicitBatchSize {
				effectiveBatchSize = defaultBatchSize
			}
			s.cursorMu.Unlock()
			batch, consumed, materialized, err := s.compoundCursorBatch(compoundCollection, compoundIDs, compoundPlan, projection, startPos, effectiveBatchSize, committedMaterialized)
			if err != nil {
				s.cursorMu.Lock()
				if current := s.cursors[cursorID]; current == cursor && current.pos == startPos {
					s.deleteCursorLocked(cursorID)
				}
				s.cursorMu.Unlock()
				cursor.compoundBatchMu.Unlock()
				return 0, nil, false, err
			}
			s.cursorMu.Lock()
			current = s.cursors[cursorID]
			if current == nil || current != cursor || current.ns != ns || current.owner != owner || !cursorPrincipalMatches(current, s.authUser(owner)) {
				s.cursorMu.Unlock()
				cursor.compoundBatchMu.Unlock()
				return 0, nil, false, nil
			}
			current.pos += consumed
			current.materializedBytes += materialized
			current.lastUsed = time.Now()
			if current.pos >= len(current.compoundIDs) {
				s.deleteCursorLocked(cursorID)
				s.cursorMu.Unlock()
				cursor.compoundBatchMu.Unlock()
				return 0, batch, true, nil
			}
			s.cursorMu.Unlock()
			cursor.compoundBatchMu.Unlock()
			return cursorID, batch, true, nil
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
		if current == nil || current != cursor || current.ns != ns || current.owner != owner || !cursorPrincipalMatches(current, s.authUser(owner)) {
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
		if cursor == nil || cursor.ns != ns || cursor.owner != owner || !cursorPrincipalMatches(cursor, s.authUser(owner)) {
			notFound = append(notFound, cursorID)
			continue
		}
		s.deleteCursorLocked(cursorID)
		killed = append(killed, cursorID)
	}
	return killed, notFound
}

func cursorPrincipalMatches(cursor *serverCursor, principal *AuthUser) bool {
	if cursor == nil {
		return false
	}
	if cursor.principal == (AuthUser{}) {
		return principal == nil
	}
	return principal != nil && cursor.principal == *principal
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
	if !errors.Is(err, collections.ErrCollectionNotFound) {
		return nil, err
	}
	col, release, err := s.openOrCreateCollectionForFirstWrite(name)
	if release != nil {
		release()
	}
	return col, err
}

func (s *Server) openOrCreateCollectionForFirstWrite(name string) (*collections.Collection, func(), error) {
	// Register every caller that observed this namespace as missing, then
	// serialize only those callers through their first mutation. CollectionManager
	// still owns any global schema coordination needed by CreateCollection.
	s.collectionCreateMu.Lock()
	registry := s.collectionFirstWrites.Load()
	var pending *collectionFirstWritePending
	if registry != nil {
		pending = registry.byName[name]
	}
	if pending == nil {
		pending = &collectionFirstWritePending{name: name, done: make(chan struct{})}
		byName := make(map[string]*collectionFirstWritePending, 1)
		if registry != nil {
			byName = make(map[string]*collectionFirstWritePending, len(registry.byName)+1)
			for registeredName, registered := range registry.byName {
				byName[registeredName] = registered
			}
		}
		byName[name] = pending
		s.collectionFirstWrites.Store(&collectionFirstWriteRegistry{byName: byName})
	}
	pending.coldRefs++
	s.collectionCreateMu.Unlock()
	pending.mutationMu.Lock()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			pending.mutationMu.Unlock()
			s.collectionCreateMu.Lock()
			pending.coldRefs--
			if pending.coldRefs == 0 {
				registry := s.collectionFirstWrites.Load()
				if registry != nil && registry.byName[name] == pending {
					if len(registry.byName) == 1 {
						s.collectionFirstWrites.Store(nil)
					} else {
						byName := make(map[string]*collectionFirstWritePending, len(registry.byName)-1)
						for registeredName, registered := range registry.byName {
							if registeredName != name {
								byName[registeredName] = registered
							}
						}
						s.collectionFirstWrites.Store(&collectionFirstWriteRegistry{byName: byName})
					}
				}
				close(pending.done)
			}
			s.collectionCreateMu.Unlock()
		})
	}
	col, err := s.Collections.OpenCollection(name)
	if err == nil {
		// This caller observed the collection as missing before it entered the
		// cold path. Keep it serialized through its mutation too: otherwise a
		// group of waiters can all miss the same document and race their upserts.
		return col, release, nil
	} else if !errors.Is(err, collections.ErrCollectionNotFound) {
		release()
		return nil, nil, err
	}
	if _, createErr := s.Collections.CreateCollection(s.defaultCollectionMeta(name)); createErr != nil {
		release()
		return nil, nil, createErr
	}
	s.invalidateCollectionCache(name)
	col, err = s.Collections.OpenCollection(name)
	if err != nil {
		release()
		return nil, nil, err
	}
	if s.firstWriteAfterCreateHook != nil {
		s.firstWriteAfterCreateHook(name)
	}
	return col, release, nil
}

func (s *Server) openCollectionForMutation(name string) (*collections.Collection, error) {
	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		return col, err
	}
	registry := s.collectionFirstWrites.Load()
	if registry == nil {
		return col, nil
	}
	waited := false
	for {
		pending := registry.byName[name]
		if pending == nil {
			if waited {
				return s.Collections.OpenCollection(name)
			}
			return col, nil
		}
		if s.firstWriteBeforeWaitHook != nil {
			s.firstWriteBeforeWaitHook(pending)
		}
		<-pending.done
		waited = true
		registry = s.collectionFirstWrites.Load()
		if registry == nil {
			return s.Collections.OpenCollection(name)
		}
	}
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

func databaseNameFilter(filter wire.Document) (string, error) {
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
		return "", errors.New("Mongo gateway listDatabases filter currently supports name equality only")
	}
	key, err := elements[0].KeyErr()
	if err != nil {
		return "", err
	}
	if key != "name" {
		return "", errors.New("Mongo gateway listDatabases filter currently supports name equality only")
	}
	name, ok := elements[0].Value().StringValueOK()
	if !ok {
		return "", errors.New("Mongo gateway listDatabases name filter must be a string")
	}
	if err := validateMongoDatabaseName(name); err != nil {
		return "", err
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
	id := elements[0].Value()
	if id.Type == bson.TypeRegex {
		return bson.RawValue{}, fmt.Errorf("Mongo gateway %s currently requires an _id equality filter", commandName)
	}
	return id, nil
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
	switch col.MetaView().Options.DocumentFormat {
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
	if err == nil {
		return applySetFields(doc, sets, setOrder)
	}
	mutation, mutationErr := parseMongoMutation(update)
	if mutationErr != nil {
		return nil, false, mutationErr
	}
	return applyMongoMutation(doc, mutation)
}

func applyBSONSetUpdate(doc wire.Document, fields []collections.BSONSetField) (wire.Document, bool, error) {
	sets := make(map[string]bson.RawValue, len(fields))
	setOrder := make([]string, 0, len(fields))
	for _, field := range fields {
		sets[field.Key] = field.Value
		setOrder = append(setOrder, field.Key)
	}
	return applySetFields(doc, sets, setOrder)
}

func applySetFields(doc wire.Document, sets map[string]bson.RawValue, setOrder []string) (wire.Document, bool, error) {
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
	return finalizeMongoMutationResult(wire.Document(raw), changed, nil)
}

type mongoMutationField struct {
	name  string
	value bson.RawValue
}

type mongoMutationArrayField struct {
	name   string
	values []bson.RawValue
}

type mongoMutation struct {
	set, setOnInsert, inc []mongoMutationField
	unset                 []string
	push, addToSet        []mongoMutationArrayField
	replace               wire.Document
}

const mongoMutationMaxEachValues = 256
const mongoMutationMaxTargets = 256

// Bound $addToSet duplicate checks so one update cannot monopolize a mutation
// callback by comparing every candidate with an arbitrarily large stored array.
const mongoMutationMaxAddToSetComparisons = 65536

// Decimal128 comparison currently normalizes finite values through big.Rat.
// Keep the expensive subset small even when the raw BSON byte budget admits
// many compact Decimal128 operands with very large exponents.
const mongoMutationMaxAddToSetDecimalComparisons = 1024

// Also bound the raw BSON value bytes examined by duplicate comparisons. This
// applies before mutation, including when this modifier is combined with others.
const mongoMutationMaxAddToSetComparisonBytes uint64 = 8 << 20

// MongoDB limits update paths to 100 components; enforce it before recursive mutation.
const mongoMutationMaxPathDepth = 100

// MongoDB limits BSON nesting to 100 levels. Validate raw containers before a
// modifier reaches BSON decoding, which recursively materializes the document.
const mongoMutationMaxBSONNesting = 100

// Slow nested mutation decoding is bounded independently from BSON nesting.
const mongoMutationMaxDecodedElements = 65536

// MongoDB BSON objects are capped at 16 MiB. Pair that byte ceiling with the
// element cap above before slow-path decoding can build an unbounded Go graph.
const mongoMutationMaxDecodedBSONBytes = 16 << 20

// parseMongoMutation validates the shared modifier subset before any document is changed.
func parseMongoMutation(update wire.Document) (mongoMutation, error) {
	updateValue := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: update}
	first, _, err := mongoMutationFirstRawElement(updateValue)
	if err != nil {
		return mongoMutation{}, errors.New("Mongo gateway update must be a non-empty document")
	}
	if first == nil {
		return mongoMutation{replace: update}, nil
	}
	if !mongoMutationElementHasDollarKey(first) {
		if err := mongoMutationForEachRawElement(update, func(element bsoncore.Element) error {
			if mongoMutationElementHasDollarKey(element) {
				return errors.New("Mongo gateway update cannot mix replacement fields and operators")
			}
			return nil
		}); err != nil {
			return mongoMutation{}, err
		}
		if _, err := validateMongoReplacement(update); err != nil {
			return mongoMutation{}, err
		}
		return mongoMutation{replace: update}, nil
	}
	mutation := mongoMutation{}
	seen := make(map[string]struct{})
	err = mongoMutationForEachRawElement(updateValue.Value, func(elem bsoncore.Element) error {
		op := string(elem.KeyBytes())
		if op != "$set" && op != "$setOnInsert" && op != "$inc" && op != "$unset" && op != "$push" && op != "$addToSet" {
			return fmt.Errorf("Mongo gateway unsupported update operator %q", op)
		}
		opValue, err := elem.ValueErr()
		if err != nil {
			return err
		}
		if bson.Type(opValue.Type) != bson.TypeEmbeddedDocument {
			return fmt.Errorf("Mongo gateway %s value must be a document", op)
		}
		return mongoMutationForEachRawElement(opValue.Data, func(item bsoncore.Element) error {
			name := string(item.KeyBytes())
			if err := validateMongoMutationPath(name); err != nil {
				return err
			}
			if _, ok := seen[name]; ok {
				return fmt.Errorf("Mongo gateway update operators cannot target field %q more than once", name)
			}
			if len(seen) == mongoMutationMaxTargets {
				return fmt.Errorf("Mongo gateway update exceeds %d target fields", mongoMutationMaxTargets)
			}
			seen[name] = struct{}{}
			itemValue, err := item.ValueErr()
			if err != nil {
				return err
			}
			value := bson.RawValue{Type: bson.Type(itemValue.Type), Value: itemValue.Data}
			if err := validateMongoMutationOperandNesting(op, name, value); err != nil {
				return err
			}
			if err := value.Validate(); err != nil {
				return err
			}
			switch op {
			case "$set":
				mutation.set = append(mutation.set, mongoMutationField{name, value})
			case "$setOnInsert":
				mutation.setOnInsert = append(mutation.setOnInsert, mongoMutationField{name, value})
			case "$inc":
				if !mongoMutationNumeric(value) {
					return fmt.Errorf("Mongo gateway $inc field %q must be numeric", name)
				}
				mutation.inc = append(mutation.inc, mongoMutationField{name, value})
			case "$unset":
				mutation.unset = append(mutation.unset, name)
			case "$push", "$addToSet":
				values, err := mongoMutationArrayValues(op, name, value)
				if err != nil {
					return err
				}
				field := mongoMutationArrayField{name: name, values: values}
				if op == "$push" {
					mutation.push = append(mutation.push, field)
				} else {
					mutation.addToSet = append(mutation.addToSet, field)
				}
			}
			return nil
		})
	})
	if err != nil {
		return mongoMutation{}, err
	}
	if err := validateMongoMutationPathConflicts(seen); err != nil {
		return mongoMutation{}, err
	}
	return mutation, nil
}

func applyMongoMutation(doc wire.Document, mutation mongoMutation) (wire.Document, bool, error) {
	return applyMongoMutationWithOptions(doc, mutation, false)
}

func applyMongoMutationWithOptions(doc wire.Document, mutation mongoMutation, upsertInsert bool) (wire.Document, bool, error) {
	if len(mutation.replace) != 0 {
		updated, changed, err := applyMongoReplacement(doc, mutation.replace)
		return finalizeMongoMutationResult(updated, changed, err)
	}
	if !upsertInsert && mongoMutationUsesTopLevelFields(mutation) {
		updated, changed, err := applyMongoMutationTopLevel(doc, mutation)
		return finalizeMongoMutationResult(updated, changed, err)
	}
	if err := validateMongoMutationDecodeInputs(doc, mutation, upsertInsert); err != nil {
		return nil, false, err
	}
	var out bson.D
	if err := bson.Unmarshal(doc, &out); err != nil {
		return nil, false, err
	}
	if err := validateMongoMutationAddToSetBudget(bson.Raw(doc), mutation.addToSet); err != nil {
		return nil, false, err
	}
	changed := false
	applyFields := func(fields []mongoMutationField, operation string) error {
		for _, field := range fields {
			var fieldChanged bool
			var err error
			switch operation {
			case "$set":
				value, decodeErr := mongoMutationDecodeValue(field.value)
				if decodeErr != nil {
					err = decodeErr
				} else {
					out, fieldChanged, err = mongoMutationSetPath(out, strings.Split(field.name, "."), value)
				}
			case "$inc":
				out, fieldChanged, err = mongoMutationIncrementPath(out, strings.Split(field.name, "."), field.value)
			}
			if err != nil {
				return err
			}
			changed = changed || fieldChanged
		}
		return nil
	}
	if err := applyFields(mutation.set, "$set"); err != nil {
		return nil, false, err
	}
	if upsertInsert {
		if err := applyFields(mutation.setOnInsert, "$set"); err != nil {
			return nil, false, err
		}
	}
	if err := applyFields(mutation.inc, "$inc"); err != nil {
		return nil, false, err
	}
	for _, name := range mutation.unset {
		var fieldChanged bool
		var err error
		out, fieldChanged, err = mongoMutationUnsetPath(out, strings.Split(name, "."))
		if err != nil {
			return nil, false, err
		}
		changed = changed || fieldChanged
	}
	for _, field := range mutation.push {
		var fieldChanged bool
		var err error
		out, fieldChanged, err = mongoMutationArrayPath(out, strings.Split(field.name, "."), field.values, false)
		if err != nil {
			return nil, false, err
		}
		changed = changed || fieldChanged
	}
	for _, field := range mutation.addToSet {
		var fieldChanged bool
		var err error
		out, fieldChanged, err = mongoMutationArrayPath(out, strings.Split(field.name, "."), field.values, true)
		if err != nil {
			return nil, false, err
		}
		changed = changed || fieldChanged
	}
	if !changed {
		return doc, false, nil
	}
	raw, err := bson.Marshal(out)
	return finalizeMongoMutationResult(wire.Document(raw), true, err)
}

// finalizeMongoMutationResult rejects a result that would exceed MongoDB's
// container-nesting limit before the caller can commit it.
func finalizeMongoMutationResult(doc wire.Document, changed bool, err error) (wire.Document, bool, error) {
	if err != nil || !changed {
		return doc, changed, err
	}
	if err := validateMongoMutationRawNesting(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: doc}); err != nil {
		return nil, false, err
	}
	return doc, true, nil
}

func mongoMutationUsesTopLevelFields(mutation mongoMutation) bool {
	if len(mutation.setOnInsert) != 0 || len(mutation.push) != 0 || len(mutation.addToSet) != 0 {
		return false
	}
	for _, field := range mutation.set {
		if strings.Contains(field.name, ".") {
			return false
		}
	}
	for _, field := range mutation.inc {
		if strings.Contains(field.name, ".") {
			return false
		}
	}
	for _, name := range mutation.unset {
		if strings.Contains(name, ".") {
			return false
		}
	}
	return true
}

func applyMongoMutationTopLevel(doc wire.Document, mutation mongoMutation) (wire.Document, bool, error) {
	set := make(map[string]bson.RawValue, len(mutation.set))
	inc := make(map[string]bson.RawValue, len(mutation.inc))
	unset := make(map[string]struct{}, len(mutation.unset))
	for _, field := range mutation.set {
		set[field.name] = field.value
	}
	for _, field := range mutation.inc {
		inc[field.name] = field.value
	}
	for _, name := range mutation.unset {
		unset[name] = struct{}{}
	}
	elements, err := bson.Raw(doc).Elements()
	if err != nil {
		return nil, false, err
	}
	out := make(bson.D, 0, len(elements)+len(set)+len(inc))
	seen := make(map[string]struct{}, len(elements))
	changed := false
	for _, elem := range elements {
		name, _ := elem.KeyErr()
		value := elem.Value()
		seen[name] = struct{}{}
		if _, ok := unset[name]; ok {
			changed = true
			continue
		}
		if next, ok := set[name]; ok {
			if !next.Equal(value) {
				changed = true
			}
			value = next
		}
		if delta, ok := inc[name]; ok {
			next, err := mongoMutationIncrement(value, delta)
			if err != nil {
				return nil, false, err
			}
			if !next.Equal(value) {
				changed = true
			}
			value = next
		}
		out = append(out, bson.E{Key: name, Value: value})
	}
	for _, field := range mutation.set {
		if _, ok := seen[field.name]; !ok {
			out = append(out, bson.E{Key: field.name, Value: field.value})
			changed = true
		}
	}
	for _, field := range mutation.inc {
		if _, ok := seen[field.name]; !ok {
			out = append(out, bson.E{Key: field.name, Value: field.value})
			changed = true
		}
	}
	if !changed {
		return doc, false, nil
	}
	raw, err := bson.Marshal(out)
	return wire.Document(raw), true, err
}

func mongoMutationArrayValues(op, name string, value bson.RawValue) ([]bson.RawValue, error) {
	if value.Type != bson.TypeEmbeddedDocument {
		return []bson.RawValue{value}, nil
	}
	first, remaining, err := mongoMutationFirstRawElement(value)
	if err != nil {
		return nil, err
	}
	if first == nil {
		return []bson.RawValue{value}, nil
	}
	if !mongoMutationElementHasDollarKey(first) {
		return []bson.RawValue{value}, nil
	}
	if string(first.KeyBytes()) != "$each" || len(remaining) != 0 {
		return nil, fmt.Errorf("Mongo gateway %s field %q only supports a scalar or $each", op, name)
	}
	array, err := first.ValueErr()
	if err != nil {
		return nil, err
	}
	if bson.Type(array.Type) != bson.TypeArray {
		return nil, fmt.Errorf("Mongo gateway %s field %q $each must be an array", op, name)
	}
	values := make([]bson.RawValue, 0, mongoMutationMaxEachValues)
	err = mongoMutationForEachRawElement(array.Data, func(element bsoncore.Element) error {
		if len(values) == mongoMutationMaxEachValues {
			return fmt.Errorf("Mongo gateway %s field %q $each exceeds %d values", op, name, mongoMutationMaxEachValues)
		}
		item, err := element.ValueErr()
		if err != nil {
			return err
		}
		values = append(values, bson.RawValue{Type: bson.Type(item.Type), Value: item.Data})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

func validateMongoMutationPath(path string) error {
	if path == "" || strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
		return errors.New("Mongo gateway update path must contain non-empty segments")
	}
	segments := strings.Split(path, ".")
	if len(segments) > mongoMutationMaxPathDepth {
		return fmt.Errorf("Mongo gateway update path exceeds %d components", mongoMutationMaxPathDepth)
	}
	for index, segment := range segments {
		if index == 0 && segment == "_id" {
			return errors.New("Mongo gateway update cannot modify _id")
		}
		if index > 0 && mongoMutationDecimalPathSegment(segment) {
			return errors.New("Mongo gateway update does not support numeric array-index paths")
		}
		if strings.HasPrefix(segment, "$") {
			return errors.New("Mongo gateway update does not support positional paths or array filters")
		}
	}
	return nil
}

func validateMongoMutationRawNesting(value bson.RawValue) error {
	type frame struct {
		remaining []byte
		depth     int
	}
	container, ok := mongoMutationDecodeContainer(value)
	if !ok {
		return nil
	}
	remaining, ok := rawBSONContainerContents(container)
	if !ok {
		return errors.New("Mongo gateway invalid BSON container")
	}
	// BSON documents include their root container in the nesting limit.
	stack := []frame{{remaining: remaining, depth: 1}}
	for len(stack) != 0 {
		last := len(stack) - 1
		current := &stack[last]
		if len(current.remaining) == 0 {
			stack = stack[:last]
			continue
		}
		element, next, ok := bsoncore.ReadElement(current.remaining)
		if !ok {
			return errors.New("Mongo gateway invalid BSON container")
		}
		if err := element.Validate(); err != nil {
			return err
		}
		current.remaining = next
		childValue, err := element.ValueErr()
		if err != nil {
			return err
		}
		child, isContainer := mongoMutationDecodeContainer(bson.RawValue{Type: bson.Type(childValue.Type), Value: childValue.Data})
		if isContainer {
			if current.depth == mongoMutationMaxBSONNesting {
				return fmt.Errorf("Mongo gateway BSON nesting exceeds %d levels", mongoMutationMaxBSONNesting)
			}
			childRemaining, ok := rawBSONContainerContents(child)
			if !ok {
				return errors.New("Mongo gateway invalid BSON container")
			}
			stack = append(stack, frame{remaining: childRemaining, depth: current.depth + 1})
		}
	}
	return nil
}

// mongoMutationDecodeContainer returns a raw BSON document/array that a slow
// mutation decode will recursively materialize. CodeWithScope carries a scope
// document and must therefore follow the same bounds as explicit containers.
func mongoMutationDecodeContainer(value bson.RawValue) (bson.RawValue, bool) {
	if value.Type == bson.TypeEmbeddedDocument || value.Type == bson.TypeArray {
		return value, true
	}
	if value.Type != bson.TypeCodeWithScope {
		return bson.RawValue{}, false
	}
	_, scope, remaining, ok := bsoncore.ReadCodeWithScope(value.Value)
	if !ok || len(remaining) != 0 {
		return bson.RawValue{}, false
	}
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: scope}, true
}

func validateMongoMutationOperandsNesting(update wire.Document) error {
	return mongoMutationForEachRawElement(update, func(element bsoncore.Element) error {
		if !mongoMutationElementHasDollarKey(element) {
			return nil
		}
		value, err := element.ValueErr()
		if err != nil || bson.Type(value.Type) != bson.TypeEmbeddedDocument {
			return err
		}
		operator := string(element.KeyBytes())
		return mongoMutationForEachRawElement(value.Data, func(item bsoncore.Element) error {
			operand, err := item.ValueErr()
			if err != nil {
				return err
			}
			return validateMongoMutationOperandNesting(operator, string(item.KeyBytes()), bson.RawValue{Type: bson.Type(operand.Type), Value: operand.Data})
		})
	})
}

// validateMongoMutationOperandNesting keeps $each admission bounded before
// walking nested values. Other operands use the shared raw nesting walk.
func validateMongoMutationOperandNesting(operator, name string, value bson.RawValue) error {
	if operator != "$push" && operator != "$addToSet" || value.Type != bson.TypeEmbeddedDocument {
		return validateMongoMutationRawNesting(value)
	}
	first, remaining, err := mongoMutationFirstRawElement(value)
	if err != nil || first == nil || !mongoMutationElementHasDollarKey(first) {
		return validateMongoMutationRawNesting(value)
	}
	if string(first.KeyBytes()) != "$each" || len(remaining) != 0 {
		// parseMongoMutation reports unsupported modifier syntax without walking it.
		return nil
	}
	array, err := first.ValueErr()
	if err != nil || bson.Type(array.Type) != bson.TypeArray {
		return err
	}
	count := 0
	return mongoMutationForEachRawElement(array.Data, func(element bsoncore.Element) error {
		if count == mongoMutationMaxEachValues {
			return fmt.Errorf("Mongo gateway %s field %q $each exceeds %d values", operator, name, mongoMutationMaxEachValues)
		}
		count++
		item, err := element.ValueErr()
		if err != nil {
			return err
		}
		return validateMongoMutationRawNesting(bson.RawValue{Type: bson.Type(item.Type), Value: item.Data})
	})
}

func validateMongoMutationTargetCount(update wire.Document) error {
	first, _, err := mongoMutationFirstRawElement(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: update})
	if err != nil || first == nil || !mongoMutationElementHasDollarKey(first) {
		return err
	}
	count := 0
	return mongoMutationForEachRawElement(update, func(element bsoncore.Element) error {
		value, err := element.ValueErr()
		if err != nil || bson.Type(value.Type) != bson.TypeEmbeddedDocument {
			return err
		}
		return mongoMutationForEachRawElement(value.Data, func(bsoncore.Element) error {
			count++
			if count > mongoMutationMaxTargets {
				return fmt.Errorf("Mongo gateway update exceeds %d target fields", mongoMutationMaxTargets)
			}
			return nil
		})
	})
}

func mongoMutationForEachRawElement(raw []byte, visit func(bsoncore.Element) error) error {
	contents, ok := rawBSONContainerContents(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: raw})
	if !ok {
		return errors.New("Mongo gateway invalid BSON document")
	}
	for len(contents) != 0 {
		element, next, ok := bsoncore.ReadElement(contents)
		if !ok {
			return errors.New("Mongo gateway invalid BSON document")
		}
		if err := element.Validate(); err != nil {
			return err
		}
		if err := visit(element); err != nil {
			return err
		}
		contents = next
	}
	return nil
}

func mongoMutationFirstRawElement(value bson.RawValue) (bsoncore.Element, []byte, error) {
	contents, ok := rawBSONContainerContents(value)
	if !ok {
		return nil, nil, errors.New("Mongo gateway invalid BSON container")
	}
	if len(contents) == 0 {
		return nil, nil, nil
	}
	element, remaining, ok := bsoncore.ReadElement(contents)
	if !ok {
		return nil, nil, errors.New("Mongo gateway invalid BSON container")
	}
	if err := element.Validate(); err != nil {
		return nil, nil, err
	}
	return element, remaining, nil
}

func mongoMutationElementHasDollarKey(element bsoncore.Element) bool {
	key := element.KeyBytes()
	return len(key) != 0 && key[0] == '$'
}

func mongoMutationDecimalPathSegment(segment string) bool {
	for _, r := range segment {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func validateMongoMutationAddToSetBudget(doc bson.Raw, fields []mongoMutationArrayField) error {
	remaining := mongoMutationMaxAddToSetComparisons
	remainingBytes := mongoMutationMaxAddToSetComparisonBytes
	remainingDecimalComparisons := mongoMutationMaxAddToSetDecimalComparisons
	for _, field := range fields {
		existingValues, err := mongoMutationRawArrayPathValues(doc, strings.Split(field.name, "."))
		if err != nil {
			return err
		}
		existing := len(existingValues)
		values := len(field.values)
		candidateComparisons := values * (values - 1) / 2
		if candidateComparisons > remaining || (existing != 0 && values > (remaining-candidateComparisons)/existing) {
			return fmt.Errorf("Mongo gateway $addToSet exceeds %d duplicate comparisons", mongoMutationMaxAddToSetComparisons)
		}
		comparisonBytes, ok := mongoMutationAddToSetComparisonBytes(existingValues, field.values, remainingBytes)
		if !ok {
			return fmt.Errorf("Mongo gateway $addToSet exceeds %d comparison bytes", mongoMutationMaxAddToSetComparisonBytes)
		}
		decimalComparisons, ok := mongoMutationAddToSetDecimalComparisonWork(existingValues, field.values, remainingDecimalComparisons)
		if !ok {
			return fmt.Errorf("Mongo gateway $addToSet exceeds %d Decimal128 comparisons", mongoMutationMaxAddToSetDecimalComparisons)
		}
		remaining -= candidateComparisons + existing*values
		remainingBytes -= comparisonBytes
		remainingDecimalComparisons -= decimalComparisons
	}
	return nil
}

// mongoMutationAddToSetDecimalComparisonWork mirrors the equality traversal so
// byte-identical nested numeric leaves do not consume the Decimal128 budget.
// It also mirrors membership short-circuiting and compares later candidates
// only with values that would actually be admitted. Validation remains separate
// because equality treats malformed containers as unequal while mutation
// admission must fail closed.
func mongoMutationAddToSetDecimalComparisonWork(existing, candidates []bson.RawValue, limit int) (int, bool) {
	for _, values := range [][]bson.RawValue{existing, candidates} {
		for _, value := range values {
			if mongoMutationRawValueDecimal128NormalizationLeaves(value) == maxInt {
				return 0, false
			}
		}
	}
	remaining := limit
	compare := func(left, right bson.RawValue) (bool, bool) {
		if left.Type == right.Type && bytes.Equal(left.Value, right.Value) {
			return true, true
		}
		equal, err := rawValuesEqualModeBudget(left, right, true, &remaining)
		return equal, err == nil
	}
	if len(candidates) > mongoMutationMaxEachValues {
		return limit - remaining, false
	}
	var admitted [mongoMutationMaxEachValues]int
	admittedCount := 0
	for candidateIndex, candidate := range candidates {
		duplicate := false
		for _, stored := range existing {
			equal, ok := compare(candidate, stored)
			if !ok {
				return limit - remaining, false
			}
			if equal {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		for _, earlierIndex := range admitted[:admittedCount] {
			equal, ok := compare(candidate, candidates[earlierIndex])
			if !ok {
				return limit - remaining, false
			}
			if equal {
				duplicate = true
				break
			}
		}
		if !duplicate {
			admitted[admittedCount] = candidateIndex
			admittedCount++
		}
	}
	return limit - remaining, true
}

func mongoMutationRawValueDecimal128NormalizationLeaves(value bson.RawValue) int {
	values := []bson.RawValue{value}
	count := 0
	for len(values) != 0 {
		last := len(values) - 1
		current := values[last]
		values = values[:last]
		switch current.Type {
		case bson.TypeDecimal128:
			count += decimal128NormalizationCount(current)
		case bson.TypeEmbeddedDocument, bson.TypeArray:
			contents, ok := rawBSONContainerContents(current)
			if !ok {
				return maxInt
			}
			for len(contents) != 0 {
				element, remaining, ok := bsoncore.ReadElement(contents)
				if !ok || element.Validate() != nil {
					return maxInt
				}
				raw, err := element.ValueErr()
				if err != nil {
					return maxInt
				}
				values = append(values, bson.RawValue{Type: bson.Type(raw.Type), Value: raw.Data})
				contents = remaining
			}
		case bson.TypeCodeWithScope:
			_, scope, remaining, ok := bsoncore.ReadCodeWithScope(current.Value)
			if !ok || len(remaining) != 0 {
				return maxInt
			}
			values = append(values, bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: scope})
		}
	}
	return count
}

func mongoMutationRawArrayPathValues(doc bson.Raw, path []string) ([]bson.RawValue, error) {
	for index, segment := range path {
		value := doc.Lookup(segment)
		if value.IsZero() {
			return nil, nil
		}
		if index == len(path)-1 {
			if value.Type != bson.TypeArray {
				return nil, nil
			}
			return value.Array().Values()
		}
		if value.Type != bson.TypeEmbeddedDocument {
			return nil, nil
		}
		doc = value.Document()
	}
	return nil, nil
}

func mongoMutationAddToSetComparisonBytes(existing, candidates []bson.RawValue, limit uint64) (uint64, bool) {
	if len(candidates) == 0 {
		return 0, true
	}
	var existingBytes, candidateBytes uint64
	for _, value := range existing {
		existingBytes += uint64(len(value.Value) + 1)
	}
	for _, value := range candidates {
		candidateBytes += uint64(len(value.Value) + 1)
	}
	candidateCount := uint64(len(candidates))
	if existingBytes > limit/candidateCount {
		return 0, false
	}
	used := candidateCount * existingBytes
	multiplier := uint64(len(existing) + len(candidates) - 1)
	if multiplier == 0 {
		return used, true
	}
	if candidateBytes > (limit-used)/multiplier {
		return 0, false
	}
	return used + multiplier*candidateBytes, true
}

func validateMongoMutationPathConflicts(paths map[string]struct{}) error {
	for path := range paths {
		segments := strings.Split(path, ".")
		for i := len(segments) - 1; i > 0; i-- {
			ancestor := strings.Join(segments[:i], ".")
			if _, ok := paths[ancestor]; ok {
				return fmt.Errorf("Mongo gateway update paths %q and %q conflict", ancestor, path)
			}
		}
	}
	return nil
}

func mongoMutationDecodeValue(value bson.RawValue) (any, error) {
	var decoded any
	if err := value.Unmarshal(&decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

type mongoMutationDecodeBudget struct {
	bytes    uint64
	elements int
}

// validateMongoMutationDecodeInputs admits every raw value that slow mutation
// decoding can unmarshal against one shared budget before it decodes any of
// them. This prevents a many-field mutation from multiplying per-value limits.
func validateMongoMutationDecodeInputs(doc wire.Document, mutation mongoMutation, upsertInsert bool) error {
	budget := mongoMutationDecodeBudget{}
	if err := budget.validate(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: doc}); err != nil {
		return err
	}
	validateFields := func(fields []mongoMutationField) error {
		for _, field := range fields {
			if err := budget.validate(field.value); err != nil {
				return err
			}
		}
		return nil
	}
	if err := validateFields(mutation.set); err != nil {
		return err
	}
	if upsertInsert {
		if err := validateFields(mutation.setOnInsert); err != nil {
			return err
		}
	}
	if err := validateFields(mutation.inc); err != nil {
		return err
	}
	validateArrays := func(fields []mongoMutationArrayField) error {
		for _, field := range fields {
			for _, value := range field.values {
				if err := budget.validate(value); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := validateArrays(mutation.push); err != nil {
		return err
	}
	return validateArrays(mutation.addToSet)
}

// validate streams raw BSON before a slow mutation calls bson.Unmarshal. The
// caller shares this budget across the stored document and every decoded
// mutation operand, bounding both retained bytes and decoded Go values.
func (budget *mongoMutationDecodeBudget) validate(value bson.RawValue) error {
	valueBytes := uint64(len(value.Value))
	if valueBytes > mongoMutationMaxDecodedBSONBytes-budget.bytes {
		return fmt.Errorf("Mongo gateway mutation BSON exceeds %d decoded bytes", mongoMutationMaxDecodedBSONBytes)
	}
	budget.bytes += valueBytes
	container, ok := mongoMutationDecodeContainer(value)
	if !ok {
		return value.Validate()
	}
	type frame struct {
		remaining []byte
		depth     int
	}
	contents, ok := rawBSONContainerContents(container)
	if !ok {
		return errors.New("Mongo gateway invalid BSON container")
	}
	stack := []frame{{remaining: contents, depth: 1}}
	for len(stack) != 0 {
		last := len(stack) - 1
		current := &stack[last]
		if len(current.remaining) == 0 {
			stack = stack[:last]
			continue
		}
		element, next, ok := bsoncore.ReadElement(current.remaining)
		if !ok {
			return errors.New("Mongo gateway invalid BSON container")
		}
		if err := element.Validate(); err != nil {
			return err
		}
		current.remaining = next
		budget.elements++
		if budget.elements > mongoMutationMaxDecodedElements {
			return fmt.Errorf("Mongo gateway mutation BSON exceeds %d decoded elements", mongoMutationMaxDecodedElements)
		}
		childValue, err := element.ValueErr()
		if err != nil {
			return err
		}
		child, isContainer := mongoMutationDecodeContainer(bson.RawValue{Type: bson.Type(childValue.Type), Value: childValue.Data})
		if !isContainer {
			continue
		}
		if current.depth == mongoMutationMaxBSONNesting {
			return fmt.Errorf("Mongo gateway BSON nesting exceeds %d levels", mongoMutationMaxBSONNesting)
		}
		childContents, ok := rawBSONContainerContents(child)
		if !ok {
			return errors.New("Mongo gateway invalid BSON container")
		}
		stack = append(stack, frame{remaining: childContents, depth: current.depth + 1})
	}
	return nil
}

func mongoMutationPathIndex(doc bson.D, key string) int {
	for i := range doc {
		if doc[i].Key == key {
			return i
		}
	}
	return -1
}

func mongoMutationNestedDocument(value any, path string) (bson.D, error) {
	doc, ok := value.(bson.D)
	if !ok {
		return nil, fmt.Errorf("Mongo gateway cannot traverse non-document field %q", path)
	}
	return doc, nil
}

func mongoMutationSetPath(doc bson.D, path []string, value any) (bson.D, bool, error) {
	idx := mongoMutationPathIndex(doc, path[0])
	if len(path) == 1 {
		if idx < 0 {
			return append(doc, bson.E{Key: path[0], Value: value}), true, nil
		}
		current, err := mongoMutationRaw(doc[idx].Value)
		if err != nil {
			return nil, false, err
		}
		next, err := mongoMutationRaw(value)
		if err != nil {
			return nil, false, err
		}
		if current.Equal(next) {
			return doc, false, nil
		}
		doc[idx].Value = value
		return doc, true, nil
	}
	if idx < 0 {
		nested, changed, err := mongoMutationSetPath(bson.D{}, path[1:], value)
		if err != nil {
			return nil, false, err
		}
		return append(doc, bson.E{Key: path[0], Value: nested}), changed, nil
	}
	nested, err := mongoMutationNestedDocument(doc[idx].Value, path[0])
	if err != nil {
		return nil, false, err
	}
	nested, changed, err := mongoMutationSetPath(nested, path[1:], value)
	if err != nil || !changed {
		return doc, changed, err
	}
	doc[idx].Value = nested
	return doc, true, nil
}

func mongoMutationUnsetPath(doc bson.D, path []string) (bson.D, bool, error) {
	idx := mongoMutationPathIndex(doc, path[0])
	if idx < 0 {
		return doc, false, nil
	}
	if len(path) == 1 {
		return append(doc[:idx:idx], doc[idx+1:]...), true, nil
	}
	nested, err := mongoMutationNestedDocument(doc[idx].Value, path[0])
	if err != nil {
		return nil, false, err
	}
	nested, changed, err := mongoMutationUnsetPath(nested, path[1:])
	if err != nil || !changed {
		return doc, changed, err
	}
	doc[idx].Value = nested
	return doc, true, nil
}

func mongoMutationIncrementPath(doc bson.D, path []string, delta bson.RawValue) (bson.D, bool, error) {
	idx := mongoMutationPathIndex(doc, path[0])
	if len(path) > 1 {
		if idx < 0 {
			nested, changed, err := mongoMutationIncrementPath(bson.D{}, path[1:], delta)
			if err != nil {
				return nil, false, err
			}
			return append(doc, bson.E{Key: path[0], Value: nested}), changed, nil
		}
		nested, err := mongoMutationNestedDocument(doc[idx].Value, path[0])
		if err != nil {
			return nil, false, err
		}
		nested, changed, err := mongoMutationIncrementPath(nested, path[1:], delta)
		if err != nil || !changed {
			return doc, changed, err
		}
		doc[idx].Value = nested
		return doc, true, nil
	}
	if idx < 0 {
		value, err := mongoMutationDecodeValue(delta)
		if err != nil {
			return nil, false, err
		}
		return append(doc, bson.E{Key: path[0], Value: value}), true, nil
	}
	current, err := mongoMutationRaw(doc[idx].Value)
	if err != nil {
		return nil, false, err
	}
	next, err := mongoMutationIncrement(current, delta)
	if err != nil {
		return nil, false, err
	}
	if current.Equal(next) {
		return doc, false, nil
	}
	value, err := mongoMutationDecodeValue(next)
	if err != nil {
		return nil, false, err
	}
	doc[idx].Value = value
	return doc, true, nil
}

func mongoMutationArrayPath(doc bson.D, path []string, values []bson.RawValue, unique bool) (bson.D, bool, error) {
	idx := mongoMutationPathIndex(doc, path[0])
	if len(path) > 1 {
		if idx < 0 {
			nested, changed, err := mongoMutationArrayPath(bson.D{}, path[1:], values, unique)
			if err != nil {
				return nil, false, err
			}
			if !changed {
				return doc, false, nil
			}
			return append(doc, bson.E{Key: path[0], Value: nested}), changed, nil
		}
		nested, err := mongoMutationNestedDocument(doc[idx].Value, path[0])
		if err != nil {
			return nil, false, err
		}
		nested, changed, err := mongoMutationArrayPath(nested, path[1:], values, unique)
		if err != nil || !changed {
			return doc, changed, err
		}
		doc[idx].Value = nested
		return doc, true, nil
	}
	var array bson.A
	if idx >= 0 {
		var ok bool
		array, ok = doc[idx].Value.(bson.A)
		if !ok {
			return nil, false, fmt.Errorf("Mongo gateway %s target must be an array", map[bool]string{true: "$addToSet", false: "$push"}[unique])
		}
	}
	changed := false
	var existingValues []bson.RawValue
	if unique {
		existingValues = make([]bson.RawValue, 0, len(array)+len(values))
		for _, existing := range array {
			existingRaw, err := mongoMutationRaw(existing)
			if err != nil {
				return nil, false, err
			}
			existingValues = append(existingValues, existingRaw)
		}
	}
	for _, raw := range values {
		if unique {
			duplicate := false
			for _, existing := range existingValues {
				if mongoMutationValuesEqual(existing, raw) {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
		}
		value, err := mongoMutationDecodeValue(raw)
		if err != nil {
			return nil, false, err
		}
		array = append(array, value)
		if unique {
			existingValues = append(existingValues, raw)
		}
		changed = true
	}
	if !changed {
		return doc, false, nil
	}
	if idx < 0 {
		return append(doc, bson.E{Key: path[0], Value: array}), true, nil
	}
	doc[idx].Value = array
	return doc, true, nil
}

func applyMongoReplacement(doc, replacement wire.Document) (wire.Document, bool, error) {
	elements, err := validateMongoReplacement(replacement)
	if err != nil {
		return nil, false, err
	}
	oldID, newID := bson.Raw(doc).Lookup("_id"), bson.Raw(replacement).Lookup("_id")
	if !newID.IsZero() && !newID.Equal(oldID) {
		return nil, false, errors.New("Mongo gateway update cannot modify _id")
	}
	if !newID.IsZero() {
		return replacement, !bytes.Equal(doc, replacement), nil
	}
	out := bson.D{{Key: "_id", Value: oldID}}
	for _, elem := range elements {
		key, _ := elem.KeyErr()
		out = append(out, bson.E{Key: key, Value: elem.Value()})
	}
	raw, err := bson.Marshal(out)
	return wire.Document(raw), !bytes.Equal(doc, raw), err
}

func validateMongoReplacement(replacement wire.Document) ([]bson.RawElement, error) {
	elements, err := bson.Raw(replacement).Elements()
	if err != nil {
		return nil, err
	}
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, err
		}
		if key != "_id" {
			if err := validateSetFieldName(key); err != nil {
				return nil, err
			}
		}
		if err := elem.Value().Validate(); err != nil {
			return nil, err
		}
	}
	return elements, nil
}

func mongoMutationNumeric(v bson.RawValue) bool {
	return v.Type == bson.TypeInt32 || v.Type == bson.TypeInt64 || v.Type == bson.TypeDouble
}
func mongoMutationInt64(v bson.RawValue) int64 {
	if n, ok := v.Int64OK(); ok {
		return n
	}
	n, _ := v.Int32OK()
	return int64(n)
}
func mongoMutationRaw(v any) (bson.RawValue, error) {
	typ, raw, err := bson.MarshalValue(v)
	return bson.RawValue{Type: typ, Value: raw}, err
}

func mongoMutationIncrement(value, delta bson.RawValue) (bson.RawValue, error) {
	if !mongoMutationNumeric(value) {
		return bson.RawValue{}, errors.New("Mongo gateway $inc target must be numeric and not null")
	}
	if value.Type == bson.TypeDouble || delta.Type == bson.TypeDouble {
		a, _ := value.DoubleOK()
		if value.Type != bson.TypeDouble {
			a = float64(mongoMutationInt64(value))
		}
		b, _ := delta.DoubleOK()
		if delta.Type != bson.TypeDouble {
			b = float64(mongoMutationInt64(delta))
		}
		return mongoMutationRaw(a + b)
	}
	a, b := mongoMutationInt64(value), mongoMutationInt64(delta)
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return bson.RawValue{}, errors.New("Mongo gateway $inc overflow")
	}
	sum := a + b
	if value.Type == bson.TypeInt32 && delta.Type == bson.TypeInt32 && sum >= math.MinInt32 && sum <= math.MaxInt32 {
		return mongoMutationRaw(int32(sum))
	}
	return mongoMutationRaw(sum)
}

type createIndexDefinition struct {
	scalarDef collections.IndexDefinition
	vectorDef collections.VectorIndexDefinition
	vector    bool
}

func parseCreateIndexDefinition(doc wire.Document) (createIndexDefinition, error) {
	// These options alter index membership, TTL behaviour, comparison, or
	// visibility. Accepting then ignoring any of them would silently create a
	// different index, so reject them before the catalog can be mutated.
	for _, option := range []string{"sparse", "partialFilterExpression", "expireAfterSeconds", "collation", "hidden"} {
		if !bson.Raw(doc).Lookup(option).IsZero() {
			return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes does not support option %q", option)
		}
	}
	keyDoc, err := requiredDocumentField(doc, "key")
	if err != nil {
		return createIndexDefinition{}, err
	}
	elements, err := bson.Raw(keyDoc).Elements()
	if err != nil {
		return createIndexDefinition{}, err
	}
	if len(elements) == 0 || len(elements) > 4 {
		return createIndexDefinition{}, errors.New("Mongo gateway createIndexes supports between one and four scalar key components")
	}
	field, err := elements[0].KeyErr()
	if err != nil {
		return createIndexDefinition{}, err
	}
	if len(elements) == 1 && field == "_id" {
		return createIndexDefinition{}, errors.New("Mongo gateway cannot create the built-in _id index")
	}
	name, namePresent, err := optionalStringFieldWithPresence(doc, "name")
	if err != nil {
		return createIndexDefinition{}, err
	}
	if namePresent && name == "" {
		return createIndexDefinition{}, errors.New("Mongo gateway createIndexes index name cannot be empty")
	}
	indexType, indexTypePresent, err := optionalStringFieldWithPresence(doc, treeDBIndexTypeField)
	if err != nil {
		return createIndexDefinition{}, err
	}
	if indexTypePresent {
		if len(elements) != 1 {
			return createIndexDefinition{}, errors.New("Mongo gateway vector indexes require exactly one key component")
		}
		if indexType != treeDBIndexTypeVector {
			return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes index %q on field %q has unsupported %s %q", indexNameOrDefault(name, namePresent, field, ""), field, treeDBIndexTypeField, indexType)
		}
		return parseCreateVectorIndexDefinition(doc, field, name, namePresent, elements[0].Value())
	}

	if isVectorIndexKey(elements[0].Value()) || !bson.Raw(doc).Lookup(treeDBVectorOptionsField).IsZero() {
		return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes vector index on field %q requires %s %q", field, treeDBIndexTypeField, treeDBIndexTypeVector)
	}
	components := make([]collections.IndexComponent, 0, len(elements))
	for _, element := range elements {
		componentField, err := element.KeyErr()
		if err != nil {
			return createIndexDefinition{}, err
		}
		direction, ok := mongoIndexDirection(element.Value())
		if !ok {
			return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes index key %q must be 1 or -1", componentField)
		}
		components = append(components, collections.IndexComponent{Field: componentField, Direction: direction})
	}
	if !namePresent {
		name = mongoIndexDefaultName(components)
	}
	unique, err := optionalBoolField(doc, "unique")
	if err != nil {
		return createIndexDefinition{}, err
	}
	valueTypeRaw, valueTypePresent, err := optionalStringFieldWithPresence(doc, "treedbValueType")
	if err != nil {
		return createIndexDefinition{}, err
	}
	if !valueTypePresent {
		return createIndexDefinition{scalarDef: collections.IndexDefinition{Name: name, Field: field, Components: components, ValueType: collections.IndexValueBSONOrderedV2, Unique: unique}}, nil
	}
	if len(components) != 1 || components[0].Direction != collections.IndexDirectionAscending {
		return createIndexDefinition{}, errors.New("Mongo gateway createIndexes treedbValueType indexes require exactly one ascending key component")
	}
	valueType := collections.IndexValueType(valueTypeRaw)
	switch valueType {
	case collections.IndexValueString, collections.IndexValueBool, collections.IndexValueInt64, collections.IndexValueDouble:
	default:
		return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes index %q on field %q has unsupported treedbValueType %q; supported values are string, bool, int64, double", name, field, valueTypeRaw)
	}
	return createIndexDefinition{scalarDef: collections.IndexDefinition{
		Name:      name,
		Field:     field,
		ValueType: valueType,
		Unique:    unique,
	}}, nil
}

func parseCreateVectorIndexDefinition(doc wire.Document, field, name string, namePresent bool, keyValue bson.RawValue) (createIndexDefinition, error) {
	if !isVectorIndexKey(keyValue) {
		return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes vector index on field %q requires key value %q", field, treeDBIndexTypeVector)
	}
	if !namePresent {
		name = field + "_vector"
	}
	if unique, err := optionalBoolField(doc, "unique"); err != nil {
		return createIndexDefinition{}, err
	} else if unique {
		return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes vector index %q on field %q does not support unique", name, field)
	}
	if _, present, err := optionalStringFieldWithPresence(doc, "treedbValueType"); err != nil {
		return createIndexDefinition{}, err
	} else if present {
		return createIndexDefinition{}, fmt.Errorf("Mongo gateway createIndexes vector index %q on field %q does not support treedbValueType", name, field)
	}
	options, err := requiredDocumentField(doc, treeDBVectorOptionsField)
	if err != nil {
		return createIndexDefinition{}, err
	}
	dimensions, err := requiredPositiveIntField(options, "dimensions")
	if err != nil {
		return createIndexDefinition{}, err
	}
	metric, err := optionalVectorMetricField(options, "metric")
	if err != nil {
		return createIndexDefinition{}, err
	}
	m, err := optionalPositiveIntField(options, "m")
	if err != nil {
		return createIndexDefinition{}, err
	}
	efConstruction, efConstructionPresent, err := optionalPositiveIntFieldWithPresence(options, "efConstruction")
	if err != nil {
		return createIndexDefinition{}, err
	}
	efSearch, err := optionalPositiveIntField(options, "efSearch")
	if err != nil {
		return createIndexDefinition{}, err
	}
	encoding, err := optionalVectorEncodingField(options, "encoding")
	if err != nil {
		return createIndexDefinition{}, err
	}
	if m == 0 {
		m = mongoDefaultVectorIndexM
	}
	if efConstruction == 0 {
		efConstruction = mongoDefaultVectorIndexEfConstruction
	}
	if efConstruction < m {
		if efConstructionPresent {
			return createIndexDefinition{}, errors.New("Mongo command field \"efConstruction\" must be >= \"m\"")
		}
		efConstruction = m
	}
	if efSearch == 0 {
		efSearch = mongoDefaultVectorIndexEfSearch
	}
	return createIndexDefinition{
		vector: true,
		vectorDef: collections.VectorIndexDefinition{
			Name:           name,
			Field:          field,
			Metric:         metric,
			Dimensions:     dimensions,
			M:              m,
			EfConstruction: efConstruction,
			EfSearch:       efSearch,
			Encoding:       encoding,
		},
	}, nil
}

func indexNameOrDefault(name string, present bool, field string, suffix string) string {
	if present {
		return name
	}
	return field + suffix
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

func mongoIndexDirection(value bson.RawValue) (collections.IndexDirection, bool) {
	if isAscendingIndexKey(value) {
		return collections.IndexDirectionAscending, true
	}
	if v, ok := value.Int32OK(); ok && v == -1 {
		return collections.IndexDirectionDescending, true
	}
	if v, ok := value.Int64OK(); ok && v == -1 {
		return collections.IndexDirectionDescending, true
	}
	if v, ok := value.DoubleOK(); ok && v == -1 {
		return collections.IndexDirectionDescending, true
	}
	return 0, false
}

func mongoIndexDefaultName(components []collections.IndexComponent) string {
	parts := make([]string, 0, len(components))
	for _, component := range components {
		parts = append(parts, component.Field+map[collections.IndexDirection]string{collections.IndexDirectionAscending: "_1", collections.IndexDirectionDescending: "_-1"}[component.Direction])
	}
	return strings.Join(parts, "_")
}

func isVectorIndexKey(value bson.RawValue) bool {
	key, ok := value.StringValueOK()
	return ok && key == treeDBIndexTypeVector
}

func requiredPositiveIntField(doc wire.Document, key string) (int, error) {
	value, present, err := optionalPositiveIntFieldWithPresence(doc, key)
	if err != nil {
		return 0, err
	}
	if !present {
		return 0, fmt.Errorf("Mongo command missing %q", key)
	}
	return value, nil
}

func optionalPositiveIntField(doc wire.Document, key string) (int, error) {
	value, _, err := optionalPositiveIntFieldWithPresence(doc, key)
	return value, err
}

func optionalPositiveIntFieldWithPresence(doc wire.Document, key string) (int, bool, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return 0, false, nil
	}
	var out int64
	if v, ok := value.Int32OK(); ok {
		out = int64(v)
	} else if v, ok := value.Int64OK(); ok {
		out = v
	} else if v, ok := value.DoubleOK(); ok {
		var integral bool
		out, integral = exactInt64FromFloat64(v)
		if !integral {
			return 0, true, fmt.Errorf("Mongo command field %q must be an integer", key)
		}
	} else {
		return 0, true, fmt.Errorf("Mongo command field %q must be an integer", key)
	}
	if out <= 0 {
		return 0, true, fmt.Errorf("Mongo command field %q must be positive", key)
	}
	if out > math.MaxInt32 {
		return 0, true, fmt.Errorf("Mongo command field %q is out of int32 range", key)
	}
	return int(out), true, nil
}

func optionalVectorMetricField(doc wire.Document, key string) (collections.VectorMetric, error) {
	value, present, err := optionalStringFieldWithPresence(doc, key)
	if err != nil {
		return 0, err
	}
	if !present || value == collections.VectorMetricCosine.String() {
		return collections.VectorMetricCosine, nil
	}
	if value == "" {
		return 0, fmt.Errorf("Mongo command field %q cannot be empty", key)
	}
	switch value {
	case collections.VectorMetricL2.String():
		return collections.VectorMetricL2, nil
	case collections.VectorMetricInnerProduct.String():
		return collections.VectorMetricInnerProduct, nil
	default:
		return 0, fmt.Errorf("Mongo command field %q has unsupported vector metric %q; supported values are cosine, l2, inner_product", key, value)
	}
}

func optionalVectorEncodingField(doc wire.Document, key string) (collections.VectorIndexEncoding, error) {
	value, present, err := optionalStringFieldWithPresence(doc, key)
	if err != nil {
		return 0, err
	}
	if !present || value == collections.VectorIndexEncodingFloat32.String() {
		return collections.VectorIndexEncodingFloat32, nil
	}
	if value == "" {
		return 0, fmt.Errorf("Mongo command field %q cannot be empty", key)
	}
	switch value {
	case collections.VectorIndexEncodingInt8.String():
		return collections.VectorIndexEncodingInt8, nil
	default:
		return 0, fmt.Errorf("Mongo command field %q has unsupported vector encoding %q; supported values are float32, int8", key, value)
	}
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
		components := idx.Components
		if len(components) == 0 {
			components = []collections.IndexComponent{{Field: idx.Field, Direction: collections.IndexDirectionAscending}}
		}
		key := make(bson.D, 0, len(components))
		for _, component := range components {
			key = append(key, bson.E{Key: component.Field, Value: int32(component.Direction)})
		}
		doc := bson.D{
			{Key: "v", Value: int32(2)},
			{Key: "key", Value: key},
			{Key: "name", Value: idx.Name},
		}
		if idx.ValueType == collections.IndexValueBSONOrderedV2 {
			doc = append(doc, bson.E{Key: "treedbIndexKeyFormat", Value: "bson-ordered-v2"}, bson.E{Key: "treedbIndexKeyVersion", Value: int32(2)})
		} else {
			doc = append(doc, bson.E{Key: "treedbValueType", Value: string(idx.ValueType)})
		}
		if idx.Unique {
			doc = append(doc, bson.E{Key: "unique", Value: true})
		}
		out = append(out, doc)
	}
	for _, idx := range meta.VectorIndexes {
		doc := bson.D{
			{Key: "v", Value: int32(2)},
			{Key: "key", Value: bson.D{{Key: idx.Field, Value: treeDBIndexTypeVector}}},
			{Key: "name", Value: idx.Name},
			{Key: treeDBIndexTypeField, Value: treeDBIndexTypeVector},
			{Key: treeDBVectorOptionsField, Value: bson.D{
				{Key: "dimensions", Value: int32(idx.Dimensions)},
				{Key: "metric", Value: idx.Metric.String()},
				{Key: "m", Value: int32(idx.M)},
				{Key: "efConstruction", Value: int32(idx.EfConstruction)},
				{Key: "efSearch", Value: int32(idx.EfSearch)},
				{Key: "encoding", Value: idx.Encoding.String()},
			}},
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

func findVectorIndexDefinition(indexes []collections.VectorIndexDefinition, name string) (collections.VectorIndexDefinition, bool) {
	for _, index := range indexes {
		if index.Name == name {
			return index, true
		}
	}
	return collections.VectorIndexDefinition{}, false
}

func dedupeIdenticalVectorIndexDefinitions(defs []collections.VectorIndexDefinition) []collections.VectorIndexDefinition {
	out := make([]collections.VectorIndexDefinition, 0, len(defs))
	for _, def := range defs {
		if existing, ok := findVectorIndexDefinition(out, def.Name); ok && sameVectorIndexDefinition(existing, def) {
			continue
		}
		out = append(out, def)
	}
	return out
}

func sameIndexDefinition(left, right collections.IndexDefinition) bool {
	if left.Name != right.Name ||
		left.ValueType != right.ValueType ||
		left.Unique != right.Unique ||
		left.MultiKey != right.MultiKey ||
		left.StoragePolicy != right.StoragePolicy {
		return false
	}
	leftComponents := indexDefinitionComponents(left)
	rightComponents := indexDefinitionComponents(right)
	if len(leftComponents) != len(rightComponents) {
		return false
	}
	for i := range leftComponents {
		if leftComponents[i] != rightComponents[i] {
			return false
		}
	}
	return true
}

// indexDefinitionComponents normalizes the legacy single-field spelling for
// comparisons at the gateway boundary. Collection metadata stores the
// canonical component form, but a repeated createIndexes command can still
// present either representation while a catalog is being upgraded.
func indexDefinitionComponents(def collections.IndexDefinition) []collections.IndexComponent {
	if len(def.Components) != 0 {
		return def.Components
	}
	return []collections.IndexComponent{{Field: def.Field, Direction: collections.IndexDirectionAscending}}
}

func sameVectorIndexDefinition(left, right collections.VectorIndexDefinition) bool {
	return left.Name == right.Name &&
		left.Field == right.Field &&
		left.Metric == right.Metric &&
		left.Dimensions == right.Dimensions &&
		left.M == right.M &&
		left.EfConstruction == right.EfConstruction &&
		left.EfSearch == right.EfSearch &&
		left.Encoding == right.Encoding
}

func validateCreateIndexesRequestDuplicates(scalarDefs []collections.IndexDefinition, vectorDefs []collections.VectorIndexDefinition) error {
	scalarSeen := make(map[string]collections.IndexDefinition, len(scalarDefs))
	for _, def := range scalarDefs {
		if existing, ok := scalarSeen[def.Name]; ok {
			if !sameIndexDefinition(existing, def) {
				return fmt.Errorf("duplicate index %q has conflicting definitions", def.Name)
			}
			continue
		}
		scalarSeen[def.Name] = def
	}
	vectorSeen := make(map[string]collections.VectorIndexDefinition, len(vectorDefs))
	for _, def := range vectorDefs {
		if existing, ok := vectorSeen[def.Name]; ok {
			if !sameVectorIndexDefinition(existing, def) {
				return fmt.Errorf("duplicate vector index %q has conflicting definitions", def.Name)
			}
			continue
		}
		vectorSeen[def.Name] = def
	}
	return nil
}

func validateCreateIndexesCrossKindNames(meta collections.CollectionMeta, scalarDefs []collections.IndexDefinition, vectorDefs []collections.VectorIndexDefinition) error {
	scalarNames := make(map[string]struct{}, len(meta.Indexes)+len(scalarDefs))
	for _, def := range meta.Indexes {
		scalarNames[def.Name] = struct{}{}
	}
	for _, def := range scalarDefs {
		scalarNames[def.Name] = struct{}{}
	}
	for _, def := range meta.VectorIndexes {
		if _, ok := scalarNames[def.Name]; ok {
			return fmt.Errorf("index name %q conflicts between scalar and vector indexes", def.Name)
		}
	}
	for _, def := range vectorDefs {
		if _, ok := scalarNames[def.Name]; ok {
			return fmt.Errorf("index name %q conflicts between scalar and vector indexes", def.Name)
		}
	}
	return nil
}

func validateCreateIndexesExistingVectorDefinitions(meta collections.CollectionMeta, vectorDefs []collections.VectorIndexDefinition) error {
	for _, def := range vectorDefs {
		existing, ok := findVectorIndexDefinition(meta.VectorIndexes, def.Name)
		if ok && !sameVectorIndexDefinition(existing, def) {
			return fmt.Errorf("vector index %q already exists with a different definition", def.Name)
		}
	}
	return nil
}

func classifyDropIndexNames(meta collections.CollectionMeta, names []string) ([]string, []string, error) {
	scalarNames := make([]string, 0, len(names))
	vectorNames := make([]string, 0, len(names))
	for _, indexName := range names {
		if indexName == "_id_" {
			return nil, nil, errors.New("cannot drop _id index")
		}
		if _, ok := findIndexDefinition(meta.Indexes, indexName); ok {
			scalarNames = append(scalarNames, indexName)
			continue
		}
		if _, ok := findVectorIndexDefinition(meta.VectorIndexes, indexName); ok {
			vectorNames = append(vectorNames, indexName)
			continue
		}
		return nil, nil, collections.ErrIndexNotFound
	}
	return scalarNames, vectorNames, nil
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
	seen := make(map[string]struct{}, len(values))
	for i, value := range values {
		name, ok := value.StringValueOK()
		if !ok {
			return nil, false, fmt.Errorf("Mongo command field \"index\"[%d] must be a string", i)
		}
		if name == "*" {
			return nil, true, nil
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names, false, nil
}

func parseSetDocument(doc bson.Raw) (map[string]bson.RawValue, []string, error) {
	return parseSetDocumentWithValueValidator(doc, validateSupportedValue)
}

func parseBSONSetDocument(doc bson.Raw) (map[string]bson.RawValue, []string, error) {
	return parseSetDocumentWithValueValidator(doc, func(path string, value bson.RawValue) error {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("Mongo document field %q is not a valid BSON value: %w", path, err)
		}
		return nil
	})
}

func parseSetDocumentWithValueValidator(doc bson.Raw, validateValue func(string, bson.RawValue) error) (map[string]bson.RawValue, []string, error) {
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
		if err := validateValue(key, value); err != nil {
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
	return commandErrorWithFields(code, codeName, message, nil)
}

func commandErrorWithFields(code int32, codeName, message string, fields bson.D) (wire.Document, error) {
	message = strings.TrimSpace(message)
	if message == "" {
		message = codeName
	}
	doc := bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: message},
		{Key: "code", Value: code},
		{Key: "codeName", Value: codeName},
	}
	for _, field := range fields {
		if field.Key != "" {
			doc = append(doc, field)
		}
	}
	return marshalDocument(doc)
}
