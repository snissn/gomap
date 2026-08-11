package nativewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

var debugLogger *log.Logger
var slowRequestThreshold time.Duration

func init() {
	if raw := os.Getenv("TREEDB_NATIVE_SLOW_REQUEST"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			slowRequestThreshold = d
		}
	}
	path := os.Getenv("TREEDB_NATIVE_DEBUG_LOG")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "DEBUG: Failed to open log file: %v\n", err)
	} else {
		debugLogger = log.New(f, "DEBUG: ", log.LstdFlags)
	}
}

func debugLoggingEnabled() bool {
	return debugLogger != nil
}

func logDebug(format string, v ...interface{}) {
	if debugLoggingEnabled() {
		debugLogger.Printf(format, v...)
	}
}

func logSlowRequest(command string, requestID uint64, dispatch, total time.Duration, err error) {
	if slowRequestThreshold <= 0 || total < slowRequestThreshold {
		return
	}
	log.Printf("nativewire slow request command=%s request_id=%d dispatch=%s total=%s err=%v", command, requestID, dispatch, total, err)
}

const (
	defaultMaxInFlight                   = 1024
	defaultMaxConnections                = 1024
	defaultMaxCollectionHandles          = 1024
	defaultMaxCachedCollections          = 1024
	defaultMaxOpenCursors                = 1024
	defaultMaxCursorRetainedBytes        = 64 << 20
	defaultMaxScanDocuments              = 10000
	defaultCursorBatchSize               = 101
	defaultCursorIdleTimeout             = 10 * time.Minute
	defaultMaxMetadataIdempotencyEntries = 1024
	defaultInsertBatchCombineMaxBatch    = 256
	defaultInsertBatchCombineDrainYields = 2
)

// ServerOptions configures a native-wire server.
type ServerOptions struct {
	Limits                          iwire.Limits
	MaxFrameSize                    uint64
	MaxInFlight                     int
	MaxConnections                  int
	MaxCollectionHandles            int
	MaxCachedCollections            int
	MaxOpenCursors                  int
	MaxCursorRetainedBytes          int
	MaxScanDocuments                int
	DefaultCursorBatchSize          int
	CursorIdleTimeout               time.Duration
	DefaultAckPolicy                iwire.AckPolicy
	MaxMetadataIdempotencyEntries   int
	InsertBatchCombineMaxBatch      int
	InsertBatchCombineDrainYields   int
	Collections                     *collections.CollectionManager
	Backend                         *backenddb.DB
	ClusterSubmitter                ClusterSubmitter
	ClusterReadCoordinator          ClusterReadCoordinator
	VectorPartitionOperations       *public.OperationsV1
	VectorPartitionNodeConfigSHA256 string
	ConnectionIdleTimeout           time.Duration
}

// Server serves native-wire control and command frames for TreeDB.
type Server struct {
	limits                          iwire.Limits
	maxInFlight                     int
	maxConnections                  int
	maxCollectionHandles            int
	maxCachedCollections            int
	maxOpenCursors                  int
	maxCursorRetainedBytes          int
	maxScanDocuments                int
	defaultCursorBatchSize          int
	cursorIdleTimeout               time.Duration
	defaultAckPolicy                iwire.AckPolicy
	maxMetadataIdempotencyEntries   int
	insertBatchCombineMaxBatch      int
	insertBatchCombineDrainYields   int
	registry                        *iwire.Registry
	collections                     *collections.CollectionManager
	backend                         *backenddb.DB
	clusterSubmitter                ClusterSubmitter
	clusterReadCoordinator          ClusterReadCoordinator
	vectorPartitionOperations       *public.OperationsV1
	vectorPartitionNodeConfigSHA256 string
	connectionIdleTimeout           time.Duration
	catalogVersion                  atomic.Uint64
	insertBatchCombiner             nativewireInsertBatchCombiner

	closed              atomic.Bool
	connMu              sync.Mutex
	connWG              sync.WaitGroup
	conns               map[net.Conn]struct{}
	listeners           map[net.Listener]struct{}
	locals              map[*localEndpoint]struct{}
	metadataMu          sync.RWMutex
	metadataIdempotency map[string]metadataIdempotencyEntry
	metadataIdemOrder   []string

	inFlight    atomic.Int64
	nextConn    atomic.Int64
	nextCursor  atomic.Uint64
	cursorCount atomic.Int64
	reapMu      sync.Mutex
	nextReap    time.Time
	cursorMu    sync.Mutex
	cursors     map[uint64]*serverCursor
	counters    counters

	cursorReaperOnce     sync.Once
	cursorReaperStopOnce sync.Once
	cursorReaperDone     chan struct{}
}

type serverCursor struct {
	owner     uint64
	records   []collections.DocumentRecord
	pos       int
	lastUsed  time.Time
	bytes     int
	truncated bool
	readMeta  ReadMetadata
}

type connState struct {
	id          uint64
	hello       bool
	mu          sync.Mutex
	nextHandle  uint64
	handles     map[CollectionHandle]string
	handleCols  map[CollectionHandle]*collections.Collection
	collections map[string]*collections.Collection

	readBody        []byte
	writeBody       []byte
	responseBody    []byte
	sections        []iwire.Section
	commandScratch  iwire.CommandScratch
	idsScratch      [][]byte
	docsScratch     [][]byte
	getManyLengths  []int
	getManyPresence []byte
	getManyPayload  []byte
	updateNames     [][]byte
	updateValues    [][]byte
	updateFields    []collections.BSONSetField
	vectorQuery     []float32
	vectorPinned    *public.PinnedSearchSnapshotV1
}

func (s *connState) closeVectorPinned() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeVectorPinnedLocked()
}

func (s *connState) closeVectorPinnedLocked() error {
	if s == nil || s.vectorPinned == nil {
		return nil
	}
	err := s.vectorPinned.Close()
	s.vectorPinned = nil
	return err
}

func (s *connState) addCollectionHandle(name string, collection *collections.Collection, handleLimit, cacheLimit int) (CollectionHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handles == nil {
		s.handles = make(map[CollectionHandle]string)
	}
	if handleLimit > 0 && len(s.handles) >= handleLimit {
		return 0, protocolError(iwire.ErrResourceExhausted, "collection handle limit %d reached", handleLimit)
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
		if cacheLimit <= 0 || len(s.collections) < cacheLimit {
			s.collections[name] = collection
		}
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

func (s *connState) cacheCollection(name string, collection *collections.Collection, limit int) {
	if collection == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.collections == nil {
		s.collections = make(map[string]*collections.Collection)
	}
	if limit > 0 && len(s.collections) >= limit {
		if _, ok := s.collections[name]; !ok {
			return
		}
	}
	s.collections[name] = collection
}

func (s *connState) responseScratch() []byte {
	if s == nil {
		return nil
	}
	return s.responseBody[:0]
}

// NewServer creates a native-wire server with defaulted limits and policies.
func NewServer(opts ServerOptions) *Server {
	limits := opts.Limits
	if opts.MaxFrameSize != 0 {
		limits.MaxFrameSize = opts.MaxFrameSize
	}
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
	maxConnections := opts.MaxConnections
	if maxConnections <= 0 {
		maxConnections = defaultMaxConnections
	}
	maxCollectionHandles := opts.MaxCollectionHandles
	if maxCollectionHandles == 0 {
		maxCollectionHandles = defaultMaxCollectionHandles
	}
	maxCachedCollections := opts.MaxCachedCollections
	if maxCachedCollections <= 0 {
		maxCachedCollections = defaultMaxCachedCollections
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
	maxMetadataIdempotencyEntries := opts.MaxMetadataIdempotencyEntries
	if maxMetadataIdempotencyEntries == 0 {
		maxMetadataIdempotencyEntries = defaultMaxMetadataIdempotencyEntries
	}
	insertBatchCombineMaxBatch := opts.InsertBatchCombineMaxBatch
	if insertBatchCombineMaxBatch <= 0 {
		insertBatchCombineMaxBatch = defaultInsertBatchCombineMaxBatch
	}
	insertBatchCombineDrainYields := opts.InsertBatchCombineDrainYields
	if insertBatchCombineDrainYields <= 0 {
		insertBatchCombineDrainYields = defaultInsertBatchCombineDrainYields
	}
	server := &Server{
		limits:                          limits,
		maxInFlight:                     maxInFlight,
		maxConnections:                  maxConnections,
		maxCollectionHandles:            maxCollectionHandles,
		maxCachedCollections:            maxCachedCollections,
		maxOpenCursors:                  maxOpenCursors,
		maxCursorRetainedBytes:          maxCursorRetainedBytes,
		maxScanDocuments:                maxScanDocuments,
		defaultCursorBatchSize:          cursorBatchSize,
		cursorIdleTimeout:               cursorIdleTimeout,
		defaultAckPolicy:                defaultAck,
		maxMetadataIdempotencyEntries:   maxMetadataIdempotencyEntries,
		insertBatchCombineMaxBatch:      insertBatchCombineMaxBatch,
		insertBatchCombineDrainYields:   insertBatchCombineDrainYields,
		registry:                        iwire.MustV1Registry(),
		collections:                     opts.Collections,
		backend:                         opts.Backend,
		clusterSubmitter:                opts.ClusterSubmitter,
		clusterReadCoordinator:          opts.ClusterReadCoordinator,
		vectorPartitionOperations:       opts.VectorPartitionOperations,
		vectorPartitionNodeConfigSHA256: opts.VectorPartitionNodeConfigSHA256,
		connectionIdleTimeout:           max(opts.ConnectionIdleTimeout, 0),
		cursorReaperDone:                make(chan struct{}),
	}
	server.catalogVersion.Store(initialCatalogVersion(opts.Backend))
	return server
}

// Close closes all active server connections and rejects new ones.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.closed.Store(true)
	s.stopCursorReaper()
	s.connMu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	listeners := make([]net.Listener, 0, len(s.listeners))
	for ln := range s.listeners {
		listeners = append(listeners, ln)
	}
	locals := make([]*localEndpoint, 0, len(s.locals))
	for local := range s.locals {
		locals = append(locals, local)
	}
	s.conns = nil
	s.listeners = nil
	s.locals = nil
	s.connMu.Unlock()
	for _, ln := range listeners {
		_ = ln.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	for _, local := range locals {
		_ = local.close()
	}
	s.connWG.Wait()
	return nil
}

// ServeConn serves native-wire frames on conn until shutdown, goaway, or error.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	logDebug("New connection")
	if s == nil || conn == nil {
		return ErrServerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.registerConn(conn) {
		_ = conn.Close()
		return ErrServerClosed
	}
	return s.serveRegisteredConn(ctx, conn)
}

func (s *Server) serveRegisteredConn(ctx context.Context, conn net.Conn) error {
	logDebug("serveRegisteredConn")
	defer s.unregisterConn(conn)
	defer conn.Close()

	state := &connState{id: uint64(s.nextConn.Add(1))}
	defer s.killCursorsForOwner(state.id)
	defer state.closeVectorPinned()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	s.startCursorReaper()

	for {
		if s.closed.Load() {
			return ErrServerClosed
		}
		if s.connectionIdleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(s.connectionIdleTimeout)); err != nil {
				return err
			}
		}
		header, body, err := readFrameInto(conn, s.limits, state.readBody)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctx.Err() != nil && (errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)) {
				return ctx.Err()
			}
			if _, ok := iwire.ErrorCodeOf(err); ok {
				s.counters.inc("malformed_frames_total")
			} else {
				s.counters.inc("transport_errors_total")
			}
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
	active := len(s.conns) + len(s.locals)
	if s.maxConnections > 0 && active >= s.maxConnections {
		s.counters.inc("connections.rejected_total")
		return false
	}
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[conn] = struct{}{}
	s.connWG.Add(1)
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
	s.connWG.Done()
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

func (s *Server) registerLocalEndpoint(endpoint *localEndpoint) bool {
	if s == nil || endpoint == nil || s.closed.Load() {
		return false
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closed.Load() {
		return false
	}
	active := len(s.conns) + len(s.locals)
	if s.maxConnections > 0 && active >= s.maxConnections {
		s.counters.inc("connections.rejected_total")
		return false
	}
	if s.locals == nil {
		s.locals = make(map[*localEndpoint]struct{})
	}
	s.locals[endpoint] = struct{}{}
	s.counters.inc("connections.opened_total")
	return true
}

func (s *Server) unregisterLocalEndpoint(endpoint *localEndpoint) {
	if s == nil || endpoint == nil {
		return
	}
	s.connMu.Lock()
	delete(s.locals, endpoint)
	s.connMu.Unlock()
	s.counters.inc("connections.closed_total")
}

func (s *Server) handleFrame(ctx context.Context, w io.Writer, state *connState, header iwire.Header, body []byte) error {
	s.counters.incFramesIn()
	s.counters.addBytesIn(uint64(iwire.FrameHeaderLenV1) + uint64(len(body)))
	if err := iwire.ValidateHeaderVersion(header, iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}); err != nil {
		if writeErr := s.writeError(w, header, err); writeErr != nil {
			return writeErr
		}
		return errGoaway
	}
	switch header.Type {
	case iwire.FrameHello:
		if err := s.validateHelloBody(body); err != nil {
			return s.writeError(w, header, err)
		}
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
		s.counters.incRequestsCanceled()
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
	if debugLoggingEnabled() {
		logDebug("handleRequest: bodyLen=%d", len(body))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return s.writeError(w, header, ctx.Err())
	}
	s.reapExpiredCursors()
	if s.inFlight.Add(1) > int64(s.maxInFlight) {
		s.inFlight.Add(-1)
		return s.writeError(w, header, protocolError(iwire.ErrResourceExhausted, "too many in-flight requests"))
	}
	defer s.inFlight.Add(-1)

	s.counters.incRequestsStarted()
	start := time.Now()
	dispatchStart := start
	var dispatchDone time.Time
	commandName := "decode"
	var requestErr error
	defer func() {
		end := time.Now()
		if dispatchDone.IsZero() {
			dispatchDone = end
		}
		logSlowRequest(commandName, header.RequestID, dispatchDone.Sub(dispatchStart), end.Sub(start), requestErr)
	}()
	var sections []iwire.Section
	var err error
	if state != nil {
		sections, err = iwire.DecodeSectionsInto(state.sections, body, s.limits)
		state.sections = sections
	} else {
		sections, err = iwire.DecodeSections(body, s.limits)
	}
	if err != nil {
		s.counters.incRequestsFailed()
		if writeErr := s.writeError(w, header, err); writeErr != nil {
			return writeErr
		}
		if isMalformedProtocolError(err) {
			return errGoaway
		}
		return nil
	}
	if s.clusterSubmitter == nil {
		if req, ok, err := s.decodeInsertBatchFastRequest(state, sections); ok {
			commandName = "insert_batch_fast"
			s.counters.incCommandRequest(iwire.CommandInsertBatch, "insert_batch")
			if err != nil {
				s.counters.addDispatchNanos(uint64(time.Since(start).Nanoseconds()))
				s.counters.incRequestsFailed()
				s.counters.incCommandError(iwire.CommandInsertBatch, "insert_batch")
				return s.writeError(w, header, err)
			}
			responseBody, err := s.handleInsertBatchFastBody(req, state.responseScratch())
			dispatchDone = time.Now()
			s.counters.addDispatchNanos(uint64(dispatchDone.Sub(start).Nanoseconds()))
			if err != nil {
				requestErr = err
				s.counters.incRequestsFailed()
				s.counters.incCommandError(iwire.CommandInsertBatch, "insert_batch")
				return s.writeError(w, header, err)
			}
			s.counters.incRequestsCompleted()
			if state != nil {
				state.responseBody = responseBody[:0]
			}
			requestErr = s.writeSimpleFrameBuffered(w, state, iwire.Header{Type: iwire.FrameResponse, StreamID: header.StreamID, RequestID: header.RequestID}, responseBody)
			return requestErr
		} else if err != nil {
			s.counters.incRequestsFailed()
			return s.writeError(w, header, err)
		}
	}
	var cmd iwire.ValidatedCommand
	if state != nil {
		cmd, err = s.registry.ValidateRequestSectionsInto(sections, &state.commandScratch)
	} else {
		cmd, err = s.registry.ValidateRequestSections(sections)
	}
	if err != nil {
		s.counters.incRequestsFailed()
		return s.writeError(w, header, err)
	}
	commandName = cmd.Schema.Name
	s.counters.incCommandRequest(cmd.Header.ID, cmd.Schema.Name)
	var responseSections []iwire.Section
	var responseBody []byte
	responseBodySet := false
	if err = s.rejectClusterRoutedLocalMetadataRead(cmd.Header.ID); err != nil {
		// The common error path below records command/request counters.
	} else if s.clusterSubmitter != nil && cmd.Schema.Kind == iwire.CommandKindMutation {
		responseSections, err = s.handleClusterMutation(ctx, header, cmd)
	} else {
		if cmd.Schema.Kind == iwire.CommandKindRead && !coordinatedReadCommand(cmd.Header.ID) {
			if err := rejectUnsupportedReadConsistencyPolicy(cmd); err != nil {
				s.counters.incRequestsFailed()
				s.counters.incCommandError(cmd.Header.ID, cmd.Schema.Name)
				return s.writeError(w, header, err)
			}
		}
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
			responseBody, err = s.handleInsertBatchBody(state, cmd.Known, state.responseScratch(), includeResultIDs, includeMeta)
			responseBodySet = true
		case iwire.CommandReplaceBatch:
			responseSections, err = s.handleReplaceBatch(state, cmd.Known)
		case iwire.CommandDeleteBatch:
			responseSections, err = s.handleDeleteBatch(state, cmd.Known)
		case iwire.CommandUpdateBSONSet:
			responseSections, err = s.handleUpdateBSONSet(state, cmd.Known)
		case iwire.CommandFlushCollection:
			responseSections, err = s.handleFlushCollection(state, cmd.Known)
		case iwire.CommandFlushAll:
			responseSections, err = s.handleFlushAll(cmd.Known)
		case iwire.CommandCheckpoint:
			responseSections, err = s.handleCheckpoint(cmd.Known)
		case iwire.CommandGetMany,
			iwire.CommandIndexLookup,
			iwire.CommandIndexRange,
			iwire.CommandOpenScan,
			iwire.CommandCursorNext,
			iwire.CommandCursorClose:
			responseSections, responseBody, responseBodySet, err = s.handleRead(ctx, header, state, cmd)
		case iwire.CommandStats:
			responseSections = []iwire.Section{{ID: iwire.SectionResponseMeta, Bytes: appendStringMap(nil, s.Stats())}}
		case iwire.CommandVectorStatus,
			iwire.CommandVectorSearchStrict,
			iwire.CommandVectorSearchFast,
			iwire.CommandVectorPinSearchSnapshot,
			iwire.CommandVectorSearchPinned,
			iwire.CommandVectorClosePinnedSnapshot:
			responseBody, err = s.handleVectorPartitionCommandV1(ctx, state, cmd, state.responseScratch())
			responseBodySet = true
		default:
			err = protocolError(iwire.ErrUnsupportedFeature, "command %s is not implemented", cmd.Schema.Name)
		}
	}
	dispatchDone = time.Now()
	s.counters.addDispatchNanos(uint64(dispatchDone.Sub(start).Nanoseconds()))
	if err != nil {
		requestErr = err
		s.counters.incRequestsFailed()
		s.counters.incCommandError(cmd.Header.ID, cmd.Schema.Name)
		return s.writeError(w, header, err)
	}
	if !responseBodySet {
		responseBody = state.responseScratch()
		for _, section := range responseSections {
			responseBody, err = iwire.AppendSection(responseBody, section)
			if err != nil {
				return err
			}
		}
	}
	s.counters.incRequestsCompleted()
	if state != nil {
		state.responseBody = responseBody[:0]
	}
	requestErr = s.writeSimpleFrameBuffered(w, state, iwire.Header{Type: iwire.FrameResponse, StreamID: header.StreamID, RequestID: header.RequestID}, responseBody)
	return requestErr
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

func (s *Server) validateHelloBody(body []byte) error {
	if len(body) == 0 {
		return nil
	}
	sections, err := iwire.DecodeSections(body, s.limits)
	if err != nil {
		return err
	}
	for _, section := range sections {
		switch section.ID {
		case iwire.SectionCapabilitySet,
			iwire.SectionTraceContext,
			iwire.SectionCompression:
			continue
		default:
			if section.Critical() {
				return protocolError(iwire.ErrUnsupportedFeature, "unknown critical hello section %d", section.ID)
			}
		}
	}
	return nil
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
	logDebug("writeError: code=%d err=%v", code, err)
	if code == 0 {
		code = iwire.ErrInternal
	}
	message := wireErrorMessage(code, err)
	body, sectionErr := iwire.AppendSection(nil, iwire.Section{
		ID:    iwire.SectionError,
		Bytes: appendErrorPayload(nil, code, retryableError(code), message),
	})
	if sectionErr != nil {
		return sectionErr
	}
	s.counters.incErrorsTotal()
	s.counters.incErrorCode(code)
	return s.writeSimpleFrame(w, iwire.Header{Type: iwire.FrameError, StreamID: request.StreamID, RequestID: request.RequestID}, body)
}

func wireErrorMessage(code iwire.ErrorCode, err error) string {
	var protocolErr *iwire.ProtocolError
	if errors.As(err, &protocolErr) && protocolErr.Reason != "" {
		return protocolErr.Reason
	}
	switch code {
	case iwire.ErrMalformedFrame:
		return "malformed frame"
	case iwire.ErrUnsupportedVersion:
		return "unsupported version"
	case iwire.ErrUnsupportedFeature:
		return "unsupported feature"
	case iwire.ErrAuthRequired:
		return "auth required"
	case iwire.ErrPermissionDenied:
		return "permission denied"
	case iwire.ErrInvalidCommand:
		return "invalid command"
	case iwire.ErrCollectionNotFound:
		return "collection not found"
	case iwire.ErrIndexNotFound:
		return "index not found"
	case iwire.ErrDuplicateDocumentID:
		return "duplicate document id"
	case iwire.ErrDocumentExists:
		return "document exists"
	case iwire.ErrUniqueIndexConflict:
		return "unique index conflict"
	case iwire.ErrCatalogVersionMismatch:
		return "catalog version mismatch"
	case iwire.ErrReadOnly:
		return "read only"
	case iwire.ErrTimeout:
		return "timeout"
	case iwire.ErrCanceled:
		return "canceled"
	case iwire.ErrResourceExhausted:
		return "resource exhausted"
	case iwire.ErrDurabilityUnavailable:
		return "durability unavailable"
	case iwire.ErrConsistencyUnavailable:
		return "consistency unavailable"
	case iwire.ErrCursorNotFound:
		return "cursor not found"
	case iwire.ErrCatalogChanged:
		return "catalog changed"
	case iwire.ErrIdempotencyConflict:
		return "idempotency conflict"
	case iwire.ErrCommitAmbiguous:
		return "commit ambiguous"
	default:
		return "internal error"
	}
}

func (s *Server) writeSimpleFrame(w io.Writer, header iwire.Header, body []byte) error {
	if s.connectionIdleTimeout > 0 {
		if conn, ok := w.(net.Conn); ok {
			if err := conn.SetWriteDeadline(time.Now().Add(s.connectionIdleTimeout)); err != nil {
				return err
			}
		}
	}
	header.Version = iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}
	if err := writeFrame(w, header, body); err != nil {
		return err
	}
	s.counters.incFramesOut()
	s.counters.addBytesOut(uint64(iwire.FrameHeaderLenV1) + uint64(len(body)))
	return nil
}

func (s *Server) writeSimpleFrameBuffered(w io.Writer, state *connState, header iwire.Header, body []byte) error {
	if s.connectionIdleTimeout > 0 {
		if conn, ok := w.(net.Conn); ok {
			if err := conn.SetWriteDeadline(time.Now().Add(s.connectionIdleTimeout)); err != nil {
				return err
			}
		}
	}
	var err error
	if state != nil {
		state.writeBody, err = writeFrameBuffered(w, header, body, state.writeBody)
	} else {
		err = writeFrame(w, header, body)
	}
	if err != nil {
		return err
	}
	s.counters.incFramesOut()
	s.counters.addBytesOut(uint64(iwire.FrameHeaderLenV1) + uint64(len(body)))
	return nil
}

// Stats returns the server's native-wire counters and backend stats.
func (s *Server) Stats() map[string]string {
	out := make(map[string]string)
	for _, key := range []string{
		"connections.opened_total",
		"connections.closed_total",
		"connections.rejected_total",
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
		"transport_errors_total",
		"dispatch_nanos_total",
		"cursors.opened_total",
		"cursors.closed_total",
		"cursors.timeouts_total",
		"insert_batch_combiner.batches_total",
		"insert_batch_combiner.requests_total",
		"insert_batch_combiner.single_requests_total",
		"insert_batch_combiner.fallback_requests_total",
		"cluster_submit.requests_total",
		"cluster_submit.success_total",
		"cluster_submit.errors_total",
		"cluster_submit.read_only_total",
		"cluster_submit.durability_unavailable_total",
		"cluster_submit.commit_ambiguous_total",
		"cluster_submit.ack_visible_total",
		"cluster_submit.ack_flushed_total",
		"cluster_submit.ack_synced_total",
		"cluster_submit.ack_raft_committed_total",
		"cluster_submit.nanos_total",
		"cluster_read_route.requests_total",
		"cluster_read_route.errors_total",
		"cluster_read_route.unsupported_total",
		"cluster_read_route.owner_store_unbound_total",
	} {
		out[nativeStatsPrefix+key] = "0"
	}
	if s.registry != nil {
		for _, schema := range s.registry.Schemas() {
			out[nativeStatsPrefix+"commands."+schema.Name+".requests_total"] = "0"
			out[nativeStatsPrefix+"commands."+schema.Name+".errors_total"] = "0"
		}
	}
	for code := iwire.ErrorCode(1); code <= iwire.MaxErrorCode; code++ {
		out[nativeStatsPrefix+"errors.code."+strconv.FormatUint(uint64(code), 10)] = "0"
	}
	for key, value := range s.counters.snapshot() {
		out[nativeStatsPrefix+key] = strconv.FormatUint(value, 10)
	}
	out[nativeStatsPrefix+"requests.in_flight"] = strconv.FormatInt(s.inFlight.Load(), 10)
	out[nativeStatsPrefix+"cursors.open"] = strconv.Itoa(s.openCursorCount())
	if version, err := s.currentCatalogVersion(); err == nil {
		out[nativeStatsPrefix+"catalog.version"] = strconv.FormatUint(version, 10)
	}
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
