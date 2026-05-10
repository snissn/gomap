package mongogateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	defaultMaxBSONObjectSize       = 16 * 1024 * 1024
	defaultMaxWriteBatchSize       = 100_000
	defaultMaxFindScanDocuments    = 10_000
	defaultMaxCursorRetainedBytes  = 64 * 1024 * 1024
	defaultMaxOpenCursors          = 1_024
	defaultCursorBatchSize         = 101
	defaultCursorIdleTimeout       = 10 * time.Minute
	defaultCursorReapInterval      = time.Second
	defaultUpdateCoalescingDelay   = 0
	defaultUpdateCoalescingBatch   = 256
	maxUpdateCoalescingBatch       = 4096
	defaultUpdateCoalescingIdleTTL = 30 * time.Second
	maxRetainedWireReadBuffer      = 1 << 20
	maxRetainedWireWriteBuffer     = 1 << 20
	defaultWireReadBufferSize      = 32 * 1024
	maxCoalescedWireResponses      = 64
)

var errServerClosed = errors.New("mongo gateway server is closed")

type Server struct {
	MaxMessageLength       int32
	MaxFindScanDocuments   int
	MaxCursorRetainedBytes int
	MaxOpenCursors         int
	CursorIdleTimeout      time.Duration
	// UpdateCoalescingMaxDelay waits for additional same-collection update
	// commands before publishing a batch. Zero means only coalesce already-queued
	// work; negative disables coalescing.
	UpdateCoalescingMaxDelay time.Duration
	// UpdateCoalescingMaxBatch caps one coalesced same-collection update publish.
	// Values above maxUpdateCoalescingBatch are clamped.
	UpdateCoalescingMaxBatch int
	// UpdateCoalescingIdleTTL removes an idle per-collection coalescer after this
	// duration. Zero uses the default; negative disables idle removal.
	UpdateCoalescingIdleTTL   time.Duration
	Collections               *collections.CollectionManager
	DefaultCollectionOptions  collections.CollectionOptions
	DefaultIndexStoragePolicy collections.RootStoragePolicy

	nextResponseID   atomic.Int32
	nextConnectionID atomic.Int64
	nextCursorID     atomic.Int64
	cursorCount      atomic.Int64
	connMu           sync.Mutex
	conns            map[net.Conn]struct{}
	listenerMu       sync.Mutex
	listeners        map[net.Listener]struct{}
	cursorMu         sync.Mutex
	cursors          map[int64]*serverCursor
	lastCursorReap   time.Time
	updateMu         sync.Mutex
	updateCoalescers map[string]*mongoUpdateCoalescer
	closed           atomic.Bool
}

type serverCursor struct {
	ns         string
	owner      int64
	docs       []wire.Document
	projection compiledProjection
	pos        int
	lastUsed   time.Time
}

func NewServer() *Server {
	s := &Server{
		MaxMessageLength:         wire.DefaultMaxMessageLength,
		UpdateCoalescingMaxDelay: defaultUpdateCoalescingDelay,
		UpdateCoalescingMaxBatch: defaultUpdateCoalescingBatch,
		UpdateCoalescingIdleTTL:  defaultUpdateCoalescingIdleTTL,
	}
	s.nextResponseID.Store(0)
	return s
}

func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.closed.Store(true)
	s.closeActiveListeners()
	s.closeActiveConns()
	s.closeUpdateCoalescers()
	s.cursorMu.Lock()
	s.cursors = nil
	s.cursorCount.Store(0)
	s.cursorMu.Unlock()
	return nil
}

func (s *Server) closeUpdateCoalescers() {
	if s == nil {
		return
	}
	s.updateMu.Lock()
	coalescers := s.updateCoalescers
	s.updateCoalescers = nil
	s.updateMu.Unlock()
	for _, coalescer := range coalescers {
		coalescer.stop()
	}
}

func (s *Server) registerListener(ln net.Listener) bool {
	if s == nil || ln == nil {
		return false
	}
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	if s.closed.Load() {
		return false
	}
	if s.listeners == nil {
		s.listeners = make(map[net.Listener]struct{})
	}
	s.listeners[ln] = struct{}{}
	return true
}

func (s *Server) unregisterListener(ln net.Listener) {
	if s == nil || ln == nil {
		return
	}
	s.listenerMu.Lock()
	delete(s.listeners, ln)
	s.listenerMu.Unlock()
}

func (s *Server) closeActiveListeners() {
	if s == nil {
		return
	}
	s.listenerMu.Lock()
	listeners := make([]net.Listener, 0, len(s.listeners))
	for ln := range s.listeners {
		listeners = append(listeners, ln)
	}
	s.listeners = nil
	s.listenerMu.Unlock()
	for _, ln := range listeners {
		_ = ln.Close()
	}
}

func (s *Server) registerConn(conn net.Conn) bool {
	if s == nil || conn == nil {
		return false
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closed.Load() {
		return false
	}
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) unregisterConn(conn net.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func (s *Server) closeActiveConns() {
	if s == nil {
		return
	}
	s.connMu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.conns = nil
	s.connMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (s *Server) isClosed() bool {
	if s == nil {
		return true
	}
	return s.closed.Load()
}

func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	if !s.registerConn(conn) {
		if conn != nil {
			_ = conn.Close()
		}
		return errServerClosed
	}
	defer s.unregisterConn(conn)
	defer conn.Close()
	owner := s.nextConnectionID.Add(1)
	defer s.killCursorsForOwner(owner)
	rw := bufferedConnReadWriter{
		reader: bufio.NewReaderSize(conn, defaultWireReadBufferSize),
		writer: conn,
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	var readBuf []byte
	var writeBuf []byte
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var err error
		readBuf, writeBuf, err = s.appendOneWithOwner(rw, owner, readBuf, writeBuf[:0])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctxErr := serveConnContextError(ctx, err); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		for coalesced := 1; coalesced < maxCoalescedWireResponses && len(writeBuf) < maxRetainedWireWriteBuffer; coalesced++ {
			var appended bool
			writeBuf, appended, err = s.appendBufferedMessageWithOwner(rw.reader, owner, writeBuf)
			if err != nil {
				if len(writeBuf) > 0 {
					if flushErr := writeFull(rw, writeBuf); flushErr != nil {
						if ctxErr := serveConnContextError(ctx, flushErr); ctxErr != nil {
							return ctxErr
						}
						return flushErr
					}
				}
				if ctxErr := serveConnContextError(ctx, err); ctxErr != nil {
					return ctxErr
				}
				return err
			}
			if !appended {
				break
			}
		}
		if len(writeBuf) == 0 {
			continue
		}
		if err := writeFull(rw, writeBuf); err != nil {
			if ctxErr := serveConnContextError(ctx, err); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		if cap(writeBuf) <= maxRetainedWireWriteBuffer {
			writeBuf = writeBuf[:0]
		} else {
			writeBuf = nil
		}
	}
}

type bufferedConnReadWriter struct {
	reader *bufio.Reader
	writer io.Writer
}

func (rw bufferedConnReadWriter) Read(p []byte) (int, error) {
	return rw.reader.Read(p)
}

func (rw bufferedConnReadWriter) Write(p []byte) (int, error) {
	return rw.writer.Write(p)
}

// ServeOne serves a single MongoDB wire message with a one-shot cursor owner.
// Callers serving a real long-lived connection should use ServeOneWithOwner or
// ServeConn so cursors survive across getMore/killCursors commands on the same
// connection owner.
func (s *Server) ServeOne(rw io.ReadWriter) error {
	owner := s.nextConnectionID.Add(1)
	defer s.killCursorsForOwner(owner)
	return s.ServeOneWithOwner(rw, owner)
}

func (s *Server) ServeOneWithOwner(rw io.ReadWriter, cursorOwner int64) error {
	_, _, err := s.serveOneWithOwner(rw, cursorOwner, nil, nil)
	return err
}

// ServeBuffers holds reusable per-connection buffers for callers that dispatch
// individual wire messages without using ServeConn.
//
// ServeBuffers is not safe for concurrent use. Use one instance per logical
// connection/worker.
type ServeBuffers struct {
	readBuf  []byte
	writeBuf []byte
}

// ServeOneWithOwnerBuffered serves one MongoDB wire message with caller-owned
// reusable buffers. It is intended for in-process dispatchers and benchmarks
// that need the same buffer reuse behavior as ServeConn.
func (s *Server) ServeOneWithOwnerBuffered(rw io.ReadWriter, cursorOwner int64, buffers *ServeBuffers) error {
	if buffers == nil {
		return s.ServeOneWithOwner(rw, cursorOwner)
	}
	readBuf, writeBuf, err := s.serveOneWithOwner(rw, cursorOwner, buffers.readBuf, buffers.writeBuf)
	buffers.readBuf = readBuf
	buffers.writeBuf = writeBuf
	return err
}

func (s *Server) serveOneWithOwner(rw io.ReadWriter, cursorOwner int64, readBuf, writeBuf []byte) ([]byte, []byte, error) {
	readBuf, writeBuf, err := s.appendOneWithOwner(rw, cursorOwner, readBuf, writeBuf[:0])
	if err != nil {
		return readBuf, writeBuf, err
	}
	if len(writeBuf) == 0 {
		return readBuf, writeBuf, nil
	}
	if err := writeFull(rw, writeBuf); err != nil {
		return readBuf, writeBuf, err
	}
	if cap(writeBuf) <= maxRetainedWireWriteBuffer {
		writeBuf = writeBuf[:0]
	} else {
		writeBuf = nil
	}
	return readBuf, writeBuf, nil
}

func (s *Server) appendOneWithOwner(rw io.Reader, cursorOwner int64, readBuf, writeBuf []byte) ([]byte, []byte, error) {
	if s.isClosed() {
		return readBuf, writeBuf, errServerClosed
	}
	h, body, err := wire.ReadMessageInto(rw, readBuf, s.maxMessageLength())
	if err != nil {
		if s.isClosed() {
			return readBuf, writeBuf, errServerClosed
		}
		return readBuf, writeBuf, err
	}
	response, retainRequestBody, err := s.handleMessageInto(writeBuf[:0], h, body, cursorOwner)
	if err != nil {
		return nil, writeBuf, err
	}
	if retainRequestBody && cap(body) <= maxRetainedWireReadBuffer {
		readBuf = body
	} else {
		readBuf = nil
	}
	if response == nil {
		return readBuf, writeBuf, nil
	}
	return readBuf, response, nil
}

func (s *Server) appendBufferedMessageWithOwner(reader *bufio.Reader, cursorOwner int64, writeBuf []byte) ([]byte, bool, error) {
	if s.isClosed() {
		return writeBuf, false, errServerClosed
	}
	if reader == nil || reader.Buffered() < wire.HeaderLen {
		return writeBuf, false, nil
	}
	headerBytes, err := reader.Peek(wire.HeaderLen)
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return writeBuf, false, nil
		}
		return writeBuf, false, err
	}
	h, err := wire.ParseHeader(headerBytes)
	if err != nil {
		return writeBuf, false, err
	}
	if h.MessageLength < wire.HeaderLen {
		return writeBuf, false, fmt.Errorf("%w: message length %d below header length", wire.ErrMalformed, h.MessageLength)
	}
	if h.MessageLength > s.maxMessageLength() {
		return writeBuf, false, fmt.Errorf("%w: length=%d max=%d", wire.ErrMessageTooLarge, h.MessageLength, s.maxMessageLength())
	}
	messageLength := int(h.MessageLength)
	if reader.Buffered() < messageLength {
		return writeBuf, false, nil
	}
	message, err := reader.Peek(messageLength)
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return writeBuf, false, nil
		}
		return writeBuf, false, err
	}
	body := message[wire.HeaderLen:messageLength]
	if !bufferedMessageCanRetainRequestBody(h, body) {
		return writeBuf, false, nil
	}
	response, _, err := s.handleMessageInto(writeBuf, h, body, cursorOwner)
	if err != nil {
		return writeBuf, false, err
	}
	if _, err := reader.Discard(messageLength); err != nil {
		return writeBuf, false, err
	}
	if response != nil {
		writeBuf = response
	}
	return writeBuf, true, nil
}

func (s *Server) handleMessage(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	response, _, err := s.handleMessageInto(nil, h, body, cursorOwner)
	return response, err
}

func bufferedMessageCanRetainRequestBody(h wire.Header, body []byte) bool {
	name, ok := bufferedMessageCommandName(h.OpCode, body)
	if !ok {
		return false
	}
	switch name {
	case "find", "getMore", "hello", "isMaster", "ismaster", "killCursors", "listCollections", "listIndexes", "ping":
		return true
	default:
		return false
	}
}

func bufferedMessageCommandName(op wire.OpCode, body []byte) (string, bool) {
	switch op {
	case wire.OpMsg:
		if len(body) < 5 {
			return "", false
		}
		rem := body[4:]
		for len(rem) > 0 {
			kind := rem[0]
			rem = rem[1:]
			switch kind {
			case wire.MsgSectionBody:
				return bsonDocumentCommandName(rem)
			case wire.MsgSectionDocumentSequence:
				if len(rem) < 4 {
					return "", false
				}
				size := int(int32(binary.LittleEndian.Uint32(rem[:4])))
				if size <= 4 || size > len(rem) || bytes.IndexByte(rem[4:size], 0) < 0 {
					return "", false
				}
				rem = rem[size:]
			default:
				return "", false
			}
		}
		return "", false
	case wire.OpQuery:
		if len(body) < 4 {
			return "", false
		}
		rem := body[4:]
		i := 0
		for i < len(rem) && rem[i] != 0 {
			i++
		}
		if i == len(rem) {
			return "", false
		}
		rem = rem[i+1:]
		if len(rem) < 8 {
			return "", false
		}
		return bsonDocumentCommandName(rem[8:])
	default:
		return "", false
	}
}

func bsonDocumentCommandName(doc []byte) (string, bool) {
	if len(doc) < 5 {
		return "", false
	}
	size := int(int32(binary.LittleEndian.Uint32(doc[:4])))
	if size < 5 || size > len(doc) || doc[size-1] != 0 {
		return "", false
	}
	if size == 5 || doc[4] == 0 {
		return "", false
	}
	key := doc[5:size]
	for i, b := range key {
		if b == 0 {
			if i == 0 {
				return "", false
			}
			return string(key[:i]), true
		}
	}
	return "", false
}

func (s *Server) handleMessageInto(dst []byte, h wire.Header, body []byte, cursorOwner int64) ([]byte, bool, error) {
	if s.isClosed() {
		return nil, false, errServerClosed
	}
	s.reapExpiredCursors()
	switch h.OpCode {
	case wire.OpQuery:
		return s.handleQueryInto(dst, h, body, cursorOwner)
	case wire.OpMsg:
		return s.handleMsgInto(dst, h, body, cursorOwner)
	case wire.OpCompressed:
		return nil, false, fmt.Errorf("%w: OP_COMPRESSED", wire.ErrUnsupported)
	default:
		return nil, false, fmt.Errorf("%w: opcode %d", wire.ErrUnsupported, h.OpCode)
	}
}

func (s *Server) handleQuery(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	response, _, err := s.handleQueryInto(nil, h, body, cursorOwner)
	return response, err
}

func (s *Server) handleQueryInto(dst []byte, h wire.Header, body []byte, cursorOwner int64) ([]byte, bool, error) {
	q, err := wire.ParseQuery(body)
	if err != nil {
		return nil, false, err
	}
	name, err := wire.CommandNameFromValidatedDocument(q.Query)
	if err != nil {
		return nil, false, err
	}

	response, err := s.commandResponse(name, q.Query, nil, cursorOwner)
	if err != nil {
		return nil, name != "insert", err
	}
	reply, err := wire.AppendReplyMessage(dst, s.nextID(), h.RequestID, 0, 0, 0, response)
	return reply, name != "insert", err
}

func (s *Server) handleMsg(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	response, _, err := s.handleMsgInto(nil, h, body, cursorOwner)
	return response, err
}

func (s *Server) handleMsgInto(dst []byte, h wire.Header, body []byte, cursorOwner int64) ([]byte, bool, error) {
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return nil, false, err
	}
	name, err := wire.CommandNameFromValidatedDocument(msg.Body)
	if err != nil {
		return nil, false, err
	}
	retainRequestBody := name != "insert"

	if name == "find" {
		// The find path builds a raw OP_MSG response directly, so reject OP_MSG
		// features it does not preserve. Other commands go through
		// commandResponse with parsed document sequences.
		if msg.Flags&wire.MsgFlagMoreToCome != 0 {
			return nil, retainRequestBody, fmt.Errorf("%w: find with moreToCome flag", wire.ErrUnsupported)
		}
		if len(msg.Sequences) > 0 {
			return nil, retainRequestBody, fmt.Errorf("%w: find with document sequences", wire.ErrUnsupported)
		}
		responseID := s.nextID()
		response, err := s.findMsgResponseInto(dst, msg.Body, responseID, h.RequestID, cursorOwner)
		if err != nil {
			return nil, retainRequestBody, err
		}
		return response, retainRequestBody, nil
	}

	response, err := s.commandResponse(name, msg.Body, msg.Sequences, cursorOwner)
	if err != nil {
		return nil, retainRequestBody, err
	}
	if msg.Flags&wire.MsgFlagMoreToCome != 0 {
		return nil, retainRequestBody, nil
	}
	msgResponse, err := wire.AppendMsgMessage(dst, s.nextID(), h.RequestID, 0, response)
	return msgResponse, retainRequestBody, err
}

func (s *Server) commandResponse(name string, command wire.Document, sequences []wire.DocumentSequence, cursorOwner int64) (wire.Document, error) {
	switch name {
	case "hello", "isMaster", "ismaster":
		return marshalDocument(helloResponse(s.maxMessageLength()))
	case "ping":
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})
	case "insert":
		return s.insertResponse(command, sequences)
	case "find":
		return s.findResponse(command, cursorOwner)
	case "getMore":
		return s.getMoreResponse(command, cursorOwner)
	case "killCursors":
		return s.killCursorsResponse(command, cursorOwner)
	case "update":
		return s.updateResponse(command, sequences)
	case "delete":
		return s.deleteResponse(command, sequences)
	case "listCollections":
		return s.listCollectionsResponse(command)
	case "createIndexes":
		return s.createIndexesResponse(command)
	case "listIndexes":
		return s.listIndexesResponse(command)
	case "dropIndexes":
		return s.dropIndexesResponse(command)
	default:
		return commandError(59, "CommandNotFound", "unsupported MongoDB gateway command: "+name)
	}
}

func helloResponse(maxMessageLength int32) bson.D {
	return bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "isWritablePrimary", Value: true},
		{Key: "ismaster", Value: true},
		{Key: "secondary", Value: false},
		{Key: "helloOk", Value: true},
		{Key: "minWireVersion", Value: int32(0)},
		{Key: "maxWireVersion", Value: int32(21)},
		{Key: "maxBsonObjectSize", Value: int32(defaultMaxBSONObjectSize)},
		{Key: "maxMessageSizeBytes", Value: maxMessageLength},
		{Key: "maxWriteBatchSize", Value: int32(defaultMaxWriteBatchSize)},
		{Key: "localTime", Value: time.Now().UTC()},
	}
}

func marshalDocument(doc bson.D) (wire.Document, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return wire.Document(raw), nil
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func serveConnContextError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	ctxErr := ctx.Err()
	if ctxErr == nil {
		return nil
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
		return ctxErr
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ctxErr
	}
	return nil
}

func (s *Server) maxMessageLength() int32 {
	if s.MaxMessageLength <= 0 {
		return wire.DefaultMaxMessageLength
	}
	if s.MaxMessageLength > wire.DefaultMaxMessageLength {
		return wire.DefaultMaxMessageLength
	}
	return s.MaxMessageLength
}

func (s *Server) maxFindScanDocuments() int {
	if s.MaxFindScanDocuments <= 0 {
		return defaultMaxFindScanDocuments
	}
	return s.MaxFindScanDocuments
}

func (s *Server) maxOpenCursors() int {
	if s.MaxOpenCursors <= 0 {
		return defaultMaxOpenCursors
	}
	return s.MaxOpenCursors
}

func (s *Server) maxCursorRetainedBytes() int {
	if s.MaxCursorRetainedBytes <= 0 {
		return defaultMaxCursorRetainedBytes
	}
	return s.MaxCursorRetainedBytes
}

func (s *Server) cursorIdleTimeout() time.Duration {
	if s.CursorIdleTimeout < 0 {
		return 0
	}
	if s.CursorIdleTimeout == 0 {
		return defaultCursorIdleTimeout
	}
	return s.CursorIdleTimeout
}

func (s *Server) nextID() int32 {
	return s.nextResponseID.Add(1)
}
