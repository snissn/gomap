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
	defaultMaxOpenCursors         = 1024
	defaultMaxCursorRetainedBytes = 64 << 20
	defaultMaxScanDocuments       = 10000
	defaultCursorBatchSize        = 101
	defaultCursorIdleTimeout      = 10 * time.Minute
)

type ServerOptions struct {
	Limits                 iwire.Limits
	MaxInFlight            int
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
	maxOpenCursors         int
	maxCursorRetainedBytes int
	maxScanDocuments       int
	defaultCursorBatchSize int
	cursorIdleTimeout      time.Duration
	defaultAckPolicy       iwire.AckPolicy
	registry               *iwire.Registry
	collections            *collections.CollectionManager
	backend                *backenddb.DB

	closed atomic.Bool
	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	inFlight   atomic.Int64
	nextConn   atomic.Int64
	nextCursor atomic.Uint64
	cursorMu   sync.Mutex
	cursors    map[uint64]*serverCursor
	counters   counters
}

type serverCursor struct {
	owner    uint64
	records  []collections.DocumentRecord
	pos      int
	lastUsed time.Time
	bytes    int
}

type connState struct {
	id         uint64
	mu         sync.Mutex
	nextHandle uint64
	handles    map[CollectionHandle]string
}

func (s *connState) addCollectionHandle(name string) CollectionHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextHandle++
	handle := CollectionHandle(s.nextHandle)
	if s.handles == nil {
		s.handles = make(map[CollectionHandle]string)
	}
	s.handles[handle] = name
	return handle
}

func (s *connState) collectionForHandle(handle CollectionHandle) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.handles[handle]
	return name, ok
}

func (s *connState) closeCollectionHandle(handle CollectionHandle) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.handles[handle]; !ok {
		return false
	}
	delete(s.handles, handle)
	return true
}

func NewServer(opts ServerOptions) *Server {
	limits := opts.Limits
	if limits.MaxFrameSize == 0 {
		limits = iwire.DefaultLimits()
	}
	maxInFlight := opts.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = defaultMaxInFlight
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
	s.conns = nil
	s.connMu.Unlock()
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
		header, body, err := readFrame(conn, s.limits)
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

func (s *Server) handleFrame(ctx context.Context, w io.Writer, state *connState, header iwire.Header, body []byte) error {
	s.counters.inc("frames.in_total")
	s.counters.add("bytes.in_total", uint64(iwire.FrameHeaderLenV1)+uint64(len(body)))
	if err := iwire.ValidateHeaderVersion(header, iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}); err != nil {
		return s.writeError(w, header, err)
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
	sections, err := iwire.DecodeSections(body, s.limits)
	if err != nil {
		s.counters.inc("requests.failed_total")
		return s.writeError(w, header, err)
	}
	cmd, err := s.registry.ValidateRequestSections(sections)
	if err != nil {
		s.counters.inc("requests.failed_total")
		return s.writeError(w, header, err)
	}
	s.counters.inc("commands." + cmd.Schema.Name + ".requests_total")
	var responseSections []iwire.Section
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
	case iwire.CommandGetMany:
		responseSections, err = s.handleGetMany(state, cmd.Known)
	case iwire.CommandIndexLookup:
		responseSections, err = s.handleIndexLookup(state, cmd.Known)
	case iwire.CommandIndexRange:
		responseSections, err = s.handleIndexRange(state, cmd.Known)
	case iwire.CommandOpenScan:
		responseSections, err = s.handleOpenScan(state, cmd.Known)
	case iwire.CommandCursorNext:
		responseSections, err = s.handleCursorNext(state, cmd.Known)
	case iwire.CommandCursorClose:
		responseSections, err = s.handleCursorClose(state, cmd.Known)
	case iwire.CommandStats:
		responseSections = []iwire.Section{{ID: iwire.SectionResponseMeta, Bytes: appendStringMap(nil, s.Stats())}}
	default:
		err = protocolError(iwire.ErrUnsupportedFeature, "command %s is not implemented", cmd.Schema.Name)
	}
	s.counters.add("dispatch_nanos_total", uint64(time.Since(start).Nanoseconds()))
	if err != nil {
		s.counters.inc("requests.failed_total")
		s.counters.inc("commands." + cmd.Schema.Name + ".errors_total")
		return s.writeError(w, header, err)
	}
	body = body[:0]
	for _, section := range responseSections {
		body, err = iwire.AppendSection(body, section)
		if err != nil {
			return err
		}
	}
	s.counters.inc("requests.completed_total")
	return s.writeSimpleFrame(w, iwire.Header{Type: iwire.FrameResponse, StreamID: header.StreamID, RequestID: header.RequestID}, body)
}

func (s *Server) writeHelloOK(w io.Writer, header iwire.Header, state *connState) error {
	caps := map[string]string{
		"protocol":           "treedb-native-wire",
		"protocol_major":     strconv.Itoa(int(iwire.ProtocolMajorV1)),
		"protocol_minor":     strconv.Itoa(int(iwire.ProtocolMinorV0)),
		"connection_id":      strconv.FormatUint(state.id, 10),
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
