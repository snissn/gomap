package nativewire

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const (
	defaultMaxInFlight            = 1024
	defaultMaxCollectionHandles   = 1024
	defaultMaxOpenCursors         = 1024
	defaultMaxCursorRetainedBytes = 64 << 20
	defaultMaxScanDocuments       = 10000
	defaultCursorBatchSize        = 101
	defaultCursorIdleTimeout      = 10 * time.Minute
)

type ServerOptions struct {
	Limits                 iwire.Limits
	MaxInFlight            int
	MaxCollectionHandles   int
	MaxOpenCursors         int
	MaxCursorRetainedBytes int
	MaxScanDocuments       int
	DefaultCursorBatchSize int
	CursorIdleTimeout      time.Duration
	DefaultAckPolicy       iwire.AckPolicy
	Collections            *collections.CollectionManager
	Backend                *backenddb.DB
}

type Server struct {
	limits                 iwire.Limits
	maxInFlight            int
	maxCollectionHandles   int
	maxOpenCursors         int
	maxCursorRetainedBytes int
	maxScanDocuments       int
	defaultCursorBatchSize int
	cursorIdleTimeout      time.Duration
	defaultAckPolicy       iwire.AckPolicy
	registry               *iwire.Registry
	collections            *collections.CollectionManager
	backend                *backenddb.DB

	closed    atomic.Bool
	connMu    sync.Mutex
	conns     map[net.Conn]struct{}
	listeners map[net.Listener]struct{}

	inFlight       atomic.Int64
	nextConn       atomic.Int64
	nextCursor     atomic.Uint64
	cursorCount    atomic.Int64
	cursorReapMu   sync.Mutex
	nextCursorReap time.Time
	cursorMu       sync.Mutex
	cursors        map[uint64]*serverCursor
	counters       counters
}

type serverCursor struct {
	owner     uint64
	records   []collections.DocumentRecord
	pos       int
	lastUsed  time.Time
	bytes     int
	truncated bool
}

type connState struct {
	id          uint64
	hello       bool
	mu          sync.Mutex
	nextHandle  uint64
	handles     map[CollectionHandle]string
	handleCols  map[CollectionHandle]*collections.Collection
	collections map[string]*collections.Collection

	readBody       []byte
	writeBody      []byte
	sections       []iwire.Section
	commandScratch iwire.CommandScratch
	idsScratch     [][]byte
	docsScratch    [][]byte
}

func (s *connState) addCollectionHandle(name string, collection *collections.Collection, limit int) (CollectionHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handles == nil {
		s.handles = make(map[CollectionHandle]string)
	}
	if limit > 0 && len(s.handles) >= limit {
		return 0, protocolError(iwire.ErrResourceExhausted, "collection handle limit %d reached", limit)
	}
	s.nextHandle++
	if s.nextHandle == 0 {
		return 0, protocolError(iwire.ErrResourceExhausted, "collection handle space exhausted")
	}
	handle := CollectionHandle(s.nextHandle)
	s.handles[handle] = name
	if collection != nil {
		if s.handleCols == nil {
			s.handleCols = make(map[CollectionHandle]*collections.Collection)
		}
		s.handleCols[handle] = collection
		if s.collections == nil {
			s.collections = make(map[string]*collections.Collection)
		}
		s.collections[name] = collection
	}
	return handle, nil
}

func (s *connState) collectionForHandle(handle CollectionHandle) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.handles[handle]
	return name, ok
}

func (s *connState) collectionForHandleRef(handle CollectionHandle) (string, *collections.Collection, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.handles[handle]
	if !ok {
		return "", nil, false
	}
	var collection *collections.Collection
	if s.handleCols != nil {
		collection = s.handleCols[handle]
	}
	if collection == nil && s.collections != nil {
		collection = s.collections[name]
	}
	return name, collection, true
}

func (s *connState) closeCollectionHandle(handle CollectionHandle) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.handles[handle]; !ok {
		return false
	}
	delete(s.handles, handle)
	delete(s.handleCols, handle)
	return true
}

func (s *connState) cachedCollection(name string) (*collections.Collection, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	collection, ok := s.collections[name]
	return collection, ok
}

func (s *connState) cacheCollection(name string, collection *collections.Collection) {
	if collection == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.collections == nil {
		s.collections = make(map[string]*collections.Collection)
	}
	s.collections[name] = collection
}

func NewServer(opts ServerOptions) *Server {
	limits := opts.Limits
	defaultLimits := iwire.DefaultLimits()
	if limits.MaxFrameSize == 0 {
		limits.MaxFrameSize = defaultLimits.MaxFrameSize
	}
	if limits.MaxHeaderLen == 0 {
		limits.MaxHeaderLen = defaultLimits.MaxHeaderLen
	}
	if limits.MaxSections == 0 {
		limits.MaxSections = defaultLimits.MaxSections
	}
	if limits.MaxSectionLen == 0 {
		limits.MaxSectionLen = defaultLimits.MaxSectionLen
	}
	if limits.MaxByteVectorItems == 0 {
		limits.MaxByteVectorItems = defaultLimits.MaxByteVectorItems
	}
	if limits.MaxByteVectorBytes == 0 {
		limits.MaxByteVectorBytes = defaultLimits.MaxByteVectorBytes
	}
	maxInFlight := opts.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = defaultMaxInFlight
	}
	maxCollectionHandles := opts.MaxCollectionHandles
	if maxCollectionHandles <= 0 {
		maxCollectionHandles = defaultMaxCollectionHandles
	}
	maxOpenCursors := opts.MaxOpenCursors
	if maxOpenCursors <= 0 {
		maxOpenCursors = defaultMaxOpenCursors
	}
	maxCursorRetainedBytes := opts.MaxCursorRetainedBytes
	if maxCursorRetainedBytes <= 0 {
		maxCursorRetainedBytes = defaultMaxCursorRetainedBytes
	}
	maxScanDocuments := opts.MaxScanDocuments
	if maxScanDocuments <= 0 {
		maxScanDocuments = defaultMaxScanDocuments
	}
	cursorBatchSize := opts.DefaultCursorBatchSize
	if cursorBatchSize <= 0 {
		cursorBatchSize = defaultCursorBatchSize
	}
	cursorIdleTimeout := opts.CursorIdleTimeout
	if cursorIdleTimeout == 0 {
		cursorIdleTimeout = defaultCursorIdleTimeout
	}
	if cursorIdleTimeout < 0 {
		cursorIdleTimeout = 0
	}
	defaultAck := opts.DefaultAckPolicy
	if defaultAck == 0 {
		defaultAck = iwire.AckVisible
	}
	return &Server{
		limits:                 limits,
		maxInFlight:            maxInFlight,
		maxCollectionHandles:   maxCollectionHandles,
		maxOpenCursors:         maxOpenCursors,
		maxCursorRetainedBytes: maxCursorRetainedBytes,
		maxScanDocuments:       maxScanDocuments,
		defaultCursorBatchSize: cursorBatchSize,
		cursorIdleTimeout:      cursorIdleTimeout,
		defaultAckPolicy:       defaultAck,
		registry:               iwire.MustV1Registry(),
		collections:            opts.Collections,
		backend:                opts.Backend,
	}
}

func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.closed.Store(true)
	s.connMu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	listeners := make([]net.Listener, 0, len(s.listeners))
	for ln := range s.listeners {
		listeners = append(listeners, ln)
	}
	s.conns = nil
	s.listeners = nil
	s.connMu.Unlock()
	for _, ln := range listeners {
		_ = ln.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	return nil
}

func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	if s == nil || conn == nil {
		return ErrServerClosed
	}
	if !s.registerConn(conn) {
		_ = conn.Close()
		return ErrServerClosed
	}
	defer s.unregisterConn(conn)
	defer conn.Close()

	state := &connState{id: uint64(s.nextConn.Add(1))}
	defer s.killCursorsForOwner(state.id)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	for {
		if s.closed.Load() {
			return ErrServerClosed
		}
		header, body, err := readFrameInto(conn, s.limits, state.readBody)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctx.Err() != nil && (errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)) {
				return ctx.Err()
			}
			s.counters.inc("malformed_frames_total")
			return err
		}
		if err := s.handleFrame(ctx, conn, state, header, body); err != nil {
			if errors.Is(err, errGoaway) {
				return nil
			}
			if ctx.Err() != nil && (errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)) {
				return ctx.Err()
			}
			return err
		}
		if body != nil {
			state.readBody = body[:0]
		}
	}
}

var errGoaway = errors.New("nativewire: goaway")

func (s *Server) registerConn(conn net.Conn) bool {
	if s == nil || conn == nil || s.closed.Load() {
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
	s.counters.inc("connections.opened_total")
	return true
}

func (s *Server) unregisterConn(conn net.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
	s.counters.inc("connections.closed_total")
}

func (s *Server) registerListener(ln net.Listener) bool {
	if s == nil || ln == nil || s.closed.Load() {
		return false
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
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
	s.connMu.Lock()
	delete(s.listeners, ln)
	s.connMu.Unlock()
}

func (s *Server) handleFrame(ctx context.Context, w io.Writer, state *connState, header iwire.Header, body []byte) error {
	s.counters.inc("frames.in_total")
	s.counters.add("bytes.in_total", uint64(iwire.FrameHeaderLenV1)+uint64(len(body)))
	if err := iwire.ValidateHeaderVersion(header, iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}); err != nil {
		if writeErr := s.writeError(w, header, err); writeErr != nil {
			return writeErr
		}
		if state != nil && state.hello {
			return errGoaway
		}
		return nil
	}
	switch header.Type {
	case iwire.FrameHello:
		return s.writeHelloOK(w, header, state)
	case iwire.FramePing:
		return s.writeSimpleFrame(w, iwire.Header{Type: iwire.FramePong, StreamID: header.StreamID, RequestID: header.RequestID}, nil)
	case iwire.FrameGoaway:
		body, err := appendGoawayBody(nil, header.RequestID)
		if err != nil {
			return err
		}
		if err := s.writeSimpleFrame(w, iwire.Header{Type: iwire.FrameGoaway, StreamID: header.StreamID, RequestID: header.RequestID}, body); err != nil {
			return err
		}
		return errGoaway
	case iwire.FrameCancel:
		s.counters.inc("requests.canceled_total")
		return nil
	case iwire.FrameRequest:
		if state != nil && !state.hello {
			return s.writeError(w, header, protocolError(iwire.ErrInvalidCommand, "hello required before request"))
		}
		return s.handleRequest(ctx, w, state, header, body)
	default:
		return s.writeError(w, header, protocolError(iwire.ErrInvalidCommand, "unexpected frame type %d", header.Type))
	}
}

func (s *Server) handleRequest(ctx context.Context, w io.Writer, state *connState, header iwire.Header, body []byte) error {
	if ctx.Err() != nil {
		return s.writeError(w, header, ctx.Err())
	}
	s.reapExpiredCursors()
	if s.inFlight.Add(1) > int64(s.maxInFlight) {
		s.inFlight.Add(-1)
		return s.writeError(w, header, protocolError(iwire.ErrResourceExhausted, "too many in-flight requests"))
	}
	defer s.inFlight.Add(-1)

	s.counters.inc("requests.started_total")
	start := time.Now()
	var sections []iwire.Section
	var err error
	if state != nil {
		sections, err = iwire.DecodeSectionsInto(state.sections, body, s.limits)
		state.sections = sections
	} else {
		sections, err = iwire.DecodeSections(body, s.limits)
	}
	if err != nil {
		s.counters.inc("requests.failed_total")
		if writeErr := s.writeError(w, header, err); writeErr != nil {
			return writeErr
		}
		if isMalformedProtocolError(err) {
			return errGoaway
		}
		return nil
	}
	if req, ok, err := s.decodeInsertBatchFastRequest(state, sections); ok {
		if err != nil {
			s.counters.inc("requests.failed_total")
			s.counters.incCommandError(iwire.CommandInsertBatch, "insert_batch")
			return s.writeError(w, header, err)
		}
		s.counters.incCommandRequest(iwire.CommandInsertBatch, "insert_batch")
		responseBody, err := s.handleInsertBatchFastBody(req, body[:0])
		s.counters.add("dispatch_nanos_total", uint64(time.Since(start).Nanoseconds()))
		if err != nil {
			s.counters.inc("requests.failed_total")
			s.counters.incCommandError(iwire.CommandInsertBatch, "insert_batch")
			return s.writeError(w, header, err)
		}
		s.counters.inc("requests.completed_total")
		return s.writeSimpleFrameBuffered(w, state, iwire.Header{Type: iwire.FrameResponse, StreamID: header.StreamID, RequestID: header.RequestID}, responseBody)
	} else if err != nil {
		s.counters.inc("requests.failed_total")
		return s.writeError(w, header, err)
	}
	var cmd iwire.ValidatedCommand
	if state != nil {
		cmd, err = s.registry.ValidateRequestSectionsInto(sections, &state.commandScratch)
	} else {
		cmd, err = s.registry.ValidateRequestSections(sections)
	}
	if err != nil {
		s.counters.inc("requests.failed_total")
		return s.writeError(w, header, err)
	}
	s.counters.incCommandRequest(cmd.Header.ID, cmd.Schema.Name)
	var responseSections []iwire.Section
	var responseBody []byte
	responseBodySet := false
	switch cmd.Header.ID {
	case iwire.CommandCreateCollection:
		responseSections, err = s.handleCreateCollection(cmd.Known)
	case iwire.CommandListCollections:
		responseSections, err = s.handleListCollections()
	case iwire.CommandCreateIndex:
		responseSections, err = s.handleCreateIndex(state, cmd.Known)
	case iwire.CommandListIndexes:
		responseSections, err = s.handleListIndexes(state, cmd.Known)
	case iwire.CommandDropIndex:
		responseSections, err = s.handleDropIndex(state, cmd.Known)
	case iwire.CommandOpenCollection:
		responseSections, err = s.handleOpenCollection(state, cmd.Known)
	case iwire.CommandCloseCollection:
		responseSections, err = s.handleCloseCollection(state, cmd.Known)
	case iwire.CommandDropCollection:
		err = unsupportedDropCollection()
	case iwire.CommandInsertBatch:
		includeResultIDs := cmd.Header.Flags&iwire.CommandFlagOmitResultIDs == 0
		includeMeta := cmd.Header.Flags&iwire.CommandFlagOmitResponseMeta == 0
		responseBody, err = s.handleInsertBatchBody(state, cmd.Known, body[:0], includeResultIDs, includeMeta)
		responseBodySet = true
	case iwire.CommandReplaceBatch:
		responseSections, err = s.handleReplaceBatch(state, cmd.Known)
	case iwire.CommandDeleteBatch:
		responseSections, err = s.handleDeleteBatch(state, cmd.Known)
	case iwire.CommandFlushCollection:
		responseSections, err = s.handleFlushCollection(state, cmd.Known)
	case iwire.CommandFlushAll:
		responseSections, err = s.handleFlushAll(cmd.Known)
	case iwire.CommandCheckpoint:
		responseSections, err = s.handleCheckpoint(cmd.Known)
	case iwire.CommandGetMany:
		responseBody, err = s.handleGetManyBody(state, cmd.Known, body[:0])
		responseBodySet = true
	case iwire.CommandIndexLookup:
		responseSections, err = s.handleIndexLookup(state, cmd.Known)
	case iwire.CommandIndexRange:
		responseSections, err = s.handleIndexRange(state, cmd.Known)
	case iwire.CommandOpenScan:
		responseSections, err = s.handleOpenScan(state, cmd.Known)
	case iwire.CommandCursorNext:
		responseSections, err = s.handleCursorNext(state, header.StreamID, cmd.Known)
	case iwire.CommandCursorClose:
		responseSections, err = s.handleCursorClose(state, header.StreamID, cmd.Known)
	case iwire.CommandStats:
		responseSections = []iwire.Section{{ID: iwire.SectionResponseMeta, Bytes: appendStringMap(nil, s.Stats())}}
	default:
		err = protocolError(iwire.ErrUnsupportedFeature, "command %s is not implemented", cmd.Schema.Name)
	}
	s.counters.add("dispatch_nanos_total", uint64(time.Since(start).Nanoseconds()))
	if err != nil {
		s.counters.inc("requests.failed_total")
		s.counters.incCommandError(cmd.Header.ID, cmd.Schema.Name)
		return s.writeError(w, header, err)
	}
	if responseBodySet {
		body = responseBody
	} else {
		body = body[:0]
		for _, section := range responseSections {
			body, err = iwire.AppendSection(body, section)
			if err != nil {
				return err
			}
		}
	}
	s.counters.inc("requests.completed_total")
	return s.writeSimpleFrameBuffered(w, state, iwire.Header{Type: iwire.FrameResponse, StreamID: header.StreamID, RequestID: header.RequestID}, body)
}

func (s *Server) writeHelloOK(w io.Writer, header iwire.Header, state *connState) error {
	var connectionID uint64
	if state != nil {
		state.hello = true
		connectionID = state.id
	}
	caps := map[string]string{
		"protocol":           "treedb-native-wire",
		"protocol_major":     strconv.Itoa(int(iwire.ProtocolMajorV1)),
		"protocol_minor":     strconv.Itoa(int(iwire.ProtocolMinorV0)),
		"connection_id":      strconv.FormatUint(connectionID, 10),
		"default_ack_policy": strconv.FormatUint(uint64(s.defaultAckPolicy), 10),
	}
	body, err := iwire.AppendSection(nil, iwire.Section{ID: iwire.SectionCapabilitySet, Bytes: appendStringMap(nil, caps)})
	if err != nil {
		return err
	}
	return s.writeSimpleFrame(w, iwire.Header{Type: iwire.FrameHelloOK, StreamID: header.StreamID, RequestID: header.RequestID}, body)
}

func appendGoawayBody(dst []byte, lastAcceptedRequestID uint64) ([]byte, error) {
	return iwire.AppendSection(dst, iwire.Section{
		ID: iwire.SectionResponseMeta,
		Bytes: appendStringMap(nil, map[string]string{
			"last_accepted_request_id": strconv.FormatUint(lastAcceptedRequestID, 10),
		}),
	})
}

func (s *Server) writeError(w io.Writer, request iwire.Header, err error) error {
	code := errorCodeFor(err)
	if code == 0 {
		code = iwire.ErrInternal
	}
	message := err.Error()
	body, sectionErr := iwire.AppendSection(nil, iwire.Section{
		ID:    iwire.SectionError,
		Bytes: appendErrorPayload(nil, code, retryableError(code), message),
	})
	if sectionErr != nil {
		return sectionErr
	}
	s.counters.inc("errors.total")
	s.counters.inc("errors.code." + strconv.FormatUint(uint64(code), 10))
	return s.writeSimpleFrame(w, iwire.Header{Type: iwire.FrameError, StreamID: request.StreamID, RequestID: request.RequestID}, body)
}

func (s *Server) writeSimpleFrame(w io.Writer, header iwire.Header, body []byte) error {
	if err := writeFrame(w, header, body); err != nil {
		return err
	}
	s.counters.inc("frames.out_total")
	s.counters.add("bytes.out_total", uint64(iwire.FrameHeaderLenV1)+uint64(len(body)))
	return nil
}

func (s *Server) writeSimpleFrameBuffered(w io.Writer, state *connState, header iwire.Header, body []byte) error {
	var err error
	if state != nil {
		state.writeBody, err = writeFrameBuffered(w, header, body, state.writeBody)
	} else {
		err = writeFrame(w, header, body)
	}
	if err != nil {
		return err
	}
	s.counters.inc("frames.out_total")
	s.counters.add("bytes.out_total", uint64(iwire.FrameHeaderLenV1)+uint64(len(body)))
	return nil
}

func (s *Server) Stats() map[string]string {
	out := make(map[string]string)
	for _, key := range []string{
		"connections.opened_total",
		"connections.closed_total",
		"frames.in_total",
		"frames.out_total",
		"bytes.in_total",
		"bytes.out_total",
		"malformed_frames_total",
		"requests.started_total",
		"requests.completed_total",
		"requests.failed_total",
		"requests.canceled_total",
		"errors.total",
		"dispatch_nanos_total",
	} {
		out[nativeStatsPrefix+key] = "0"
	}
	for key, value := range s.counters.snapshot() {
		out[nativeStatsPrefix+key] = strconv.FormatUint(value, 10)
	}
	out[nativeStatsPrefix+"requests.in_flight"] = strconv.FormatInt(s.inFlight.Load(), 10)
	out[nativeStatsPrefix+"cursors.open"] = strconv.Itoa(s.openCursorCount())
	if s.collections != nil {
		for key, value := range s.collections.Stats() {
			out[key] = value
		}
	}
	if s.backend != nil {
		for key, value := range s.backend.Stats() {
			out[key] = value
		}
	}
	return out
}
