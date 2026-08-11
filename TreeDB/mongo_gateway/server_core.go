package mongogateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	treenativewire "github.com/snissn/gomap/TreeDB/nativewire"
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
	defaultInsertCoalescingDelay   = 0
	defaultInsertCoalescingBatch   = 256
	maxInsertCoalescingBatch       = 4096
	defaultInsertCoalescingIdleTTL = 30 * time.Second
	maxRetainedWireReadBuffer      = 1 << 20
	maxRetainedWireWriteBuffer     = 1 << 20
	defaultWireReadBufferSize      = 32 * 1024
	maxCoalescedWireResponses      = 64
)

var (
	errServerClosed       = errors.New("mongo gateway server is closed")
	errInvalidCursorOwner = errors.New("mongo gateway cursor owner must be nonzero")
)

type ClusterCatalogVersionProvider func(context.Context) (uint64, error)

var clusterIdempotencyNonceFallback atomic.Uint64

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
	UpdateCoalescingIdleTTL time.Duration
	// InsertCoalescingMaxDelay waits for additional same-collection single-document
	// BSON insert commands before publishing a batch. Zero means only coalesce
	// already-queued work; negative disables coalescing.
	InsertCoalescingMaxDelay time.Duration
	// InsertCoalescingMaxBatch caps one coalesced same-collection insert publish.
	// Values above maxInsertCoalescingBatch are clamped.
	InsertCoalescingMaxBatch int
	// InsertCoalescingIdleTTL removes an idle per-collection coalescer after this
	// duration. Zero uses the default; negative disables idle removal.
	InsertCoalescingIdleTTL   time.Duration
	Collections               *collections.CollectionManager
	DefaultCollectionOptions  collections.CollectionOptions
	DefaultIndexStoragePolicy collections.RootStoragePolicy
	ClusterSubmitter          treenativewire.ClusterSubmitter
	ClusterCatalogVersion     ClusterCatalogVersionProvider
	// clusterCollectionLookupHook is a test seam for proving routed command
	// preflight fails before touching the local collection catalog.
	clusterCollectionLookupHook func()
	// compoundIndexPlanScanHook is a test seam for proving a non-streaming
	// compound plan is walked exactly once by its eventual executor.
	compoundIndexPlanScanHook func()
	// compoundCursorBatchHook is a test seam for proving a cursor serializes
	// deferred compound batch materialization before it advances its position.
	compoundCursorBatchHook func()
	// ClusterIdempotencyNonce scopes generated cluster mutation idempotency
	// keys to one gateway process epoch. NewServer initializes a random nonce
	// so sequence-based keys are not reused after restart.
	ClusterIdempotencyNonce string
	// AuthenticationEnabled gates ordinary commands until one SCRAM identity
	// has been established for this connection. Authorization remains #4059.
	AuthenticationEnabled bool
	AuthCatalog           *AuthCatalog
	// diagnosticCommandWALEnabled is set by the standalone owner. It is a
	// read-only inventory seam so diagnostics never infer persistence state by
	// inspecting WAL files.
	diagnosticCommandWALEnabled func() bool

	nextResponseID            atomic.Int32
	nextConnectionID          atomic.Int64
	nextClusterSubmit         atomic.Uint64
	nextCursorID              atomic.Int64
	cursorCount               atomic.Int64
	connMu                    sync.Mutex
	conns                     map[net.Conn]struct{}
	listenerMu                sync.Mutex
	listeners                 map[net.Listener]struct{}
	cursorMu                  sync.Mutex
	cursors                   map[int64]*serverCursor
	lastCursorReap            time.Time
	collectionCacheMu         sync.RWMutex
	collectionCache           map[string]*collections.Collection
	collectionCreateMu        sync.Mutex
	collectionFirstWrites     atomic.Pointer[collectionFirstWriteRegistry]
	firstWriteAfterCreateHook func(string)
	firstWriteBeforeWaitHook  func(*collectionFirstWritePending)
	// filterWriteSelectedHook is test-only coordination between natural-order
	// selection and the mutation-boundary predicate recheck.
	filterWriteSelectedHook func()
	// filterWriteAfterMaterializerHook is test-only coordination immediately
	// after a conditional filter-write materializer is acquired and before its
	// deadline-checked mutation boundary.
	filterWriteAfterMaterializerHook func()
	// findAndModifyExactAdmissionHook and findAndModifyBeforeUpsertInsertHook
	// are test-only race seams for exact-image response admission.
	findAndModifyExactAdmissionHook     func()
	findAndModifyBeforeUpsertInsertHook func()
	// mongoWriteRetainedKeyBytesLimit is a test-only override of the command
	// retained-key ceiling; zero uses mongoWriteCommandMaxRetainedKeyBytes.
	mongoWriteRetainedKeyBytesLimit int
	// mongoWriteTargetLimit is a test-only override of the command-wide target
	// ceiling. It is distinct from MaxFindScanDocuments because inserts and
	// exact-ID writes do not consume scan work.
	mongoWriteTargetLimit int
	// mongoWriteBeforeFirstCreateHook is a test-only seam between command
	// preparation/admission and a missing-namespace catalog creation.
	mongoWriteBeforeFirstCreateHook func(*mongoWriteBudget)
	updateMu                        sync.Mutex
	updateCoalescers                map[string]*mongoUpdateCoalescer
	insertMu                        sync.Mutex
	insertCoalescers                map[string]*mongoInsertCoalescer
	// standaloneWriteBoundaryMu lets ordinary standalone writes overlap while
	// making a journal request exclusive from dispatch through its durable
	// boundary. That exclusion prevents a concurrent collection mutation from
	// holding a domain lock while waiting on the command-WAL publish lock that
	// the journal boundary owns while draining collection barriers.
	standaloneWriteBoundaryMu    sync.RWMutex
	standaloneWriteConcernSync   func() (bool, error)
	writeConcernStats            standaloneWriteConcernStats
	authConnections              sync.Map // map[int64]*authConnectionState
	nextSASLConversation         atomic.Int32
	authFailures                 atomic.Uint64
	authorizationAllowed         atomic.Uint64
	authorizationDenied          atomic.Uint64
	diagnosticsStartedAt         time.Time
	diagnosticsMu                sync.Mutex
	diagnosticsCommands          map[string]mongoCommandDiagnostic
	diagnosticsNamespaces        map[string]mongoNamespaceDiagnostic
	diagnosticsDroppedCommands   int64
	diagnosticsDroppedNamespaces int64
	// beforeSCRAMIdentityStore is test-only coordination for owner release.
	beforeSCRAMIdentityStore func()
	closed                   atomic.Bool
}

type collectionFirstWritePending struct {
	name       string
	done       chan struct{}
	mutationMu sync.Mutex
	coldRefs   int
}

type collectionFirstWriteRegistry struct {
	byName map[string]*collectionFirstWritePending
}

type serverCursor struct {
	ns        string
	owner     int64
	principal AuthUser
	docs      []wire.Document
	// compoundIDs is an ordered, bounded index result. Unlike docs it contains
	// no decoded BSON: getMore fetches and projects only the requested batch.
	compoundIDs        [][]byte
	compoundCollection *collections.Collection
	compoundPlan       *findPlan
	// cursorMu guards compoundIDs, compoundPlan, materializedBytes, and pos.
	// Every getMore reader/writer must hold it while observing cursor progress.
	// compoundBatchMu serializes deferred BSON materialization for this cursor;
	// it is never acquired while cursorMu is held.
	compoundBatchMu   sync.Mutex
	materializedBytes int
	projection        compiledProjection
	pos               int
	lastUsed          time.Time
}

func NewServer() *Server {
	s := &Server{
		MaxMessageLength:         wire.DefaultMaxMessageLength,
		UpdateCoalescingMaxDelay: defaultUpdateCoalescingDelay,
		UpdateCoalescingMaxBatch: defaultUpdateCoalescingBatch,
		UpdateCoalescingIdleTTL:  defaultUpdateCoalescingIdleTTL,
		InsertCoalescingMaxDelay: defaultInsertCoalescingDelay,
		InsertCoalescingMaxBatch: defaultInsertCoalescingBatch,
		InsertCoalescingIdleTTL:  defaultInsertCoalescingIdleTTL,
		ClusterIdempotencyNonce:  newClusterIdempotencyNonce(),
		diagnosticsStartedAt:     time.Now(),
		diagnosticsCommands:      make(map[string]mongoCommandDiagnostic),
		diagnosticsNamespaces:    make(map[string]mongoNamespaceDiagnostic),
	}
	s.nextResponseID.Store(0)
	return s
}

func newClusterIdempotencyNonce() string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err == nil {
		return hex.EncodeToString(nonce[:])
	}
	return fmt.Sprintf("fallback-%d-%d", time.Now().UnixNano(), clusterIdempotencyNonceFallback.Add(1))
}

func (s *Server) openCollectionCached(name string) (*collections.Collection, error) {
	if s == nil || s.Collections == nil {
		return nil, collections.ErrCollectionNotFound
	}
	s.collectionCacheMu.RLock()
	if col := s.collectionCache[name]; col != nil {
		s.collectionCacheMu.RUnlock()
		return col, nil
	}
	s.collectionCacheMu.RUnlock()

	col, err := s.Collections.OpenCollection(name)
	if err != nil {
		return nil, err
	}
	s.collectionCacheMu.Lock()
	if s.collectionCache == nil {
		s.collectionCache = make(map[string]*collections.Collection)
	}
	if cached := s.collectionCache[name]; cached != nil {
		col = cached
	} else {
		s.collectionCache[name] = col
	}
	s.collectionCacheMu.Unlock()
	return col, nil
}

func (s *Server) invalidateCollectionCache(name string) {
	if s == nil || name == "" {
		return
	}
	s.collectionCacheMu.Lock()
	delete(s.collectionCache, name)
	s.collectionCacheMu.Unlock()
}

func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.closed.Store(true)
	s.collectionCacheMu.Lock()
	clear(s.collectionCache)
	s.collectionCacheMu.Unlock()
	s.closeActiveListeners()
	s.closeActiveConns()
	s.closeUpdateCoalescers()
	s.closeInsertCoalescers()
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

func (s *Server) closeInsertCoalescers() {
	if s == nil {
		return
	}
	s.insertMu.Lock()
	coalescers := s.insertCoalescers
	s.insertCoalescers = nil
	s.insertMu.Unlock()
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
	defer s.ReleaseOwner(owner)
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
		readBuf, writeBuf, err = s.appendOneWithOwner(ctx, rw, owner, readBuf, writeBuf[:0])
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
			writeBuf, appended, err = s.appendBufferedMessageWithOwner(ctx, rw.reader, owner, writeBuf)
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
	defer s.ReleaseOwner(owner)
	return s.ServeOneWithOwner(rw, owner)
}

// ServeOneWithOwner serves one wire message for a caller-owned logical
// connection. cursorOwner must be nonzero. The caller must call ReleaseOwner
// when that connection closes and before reusing cursorOwner for another
// connection.
func (s *Server) ServeOneWithOwner(rw io.ReadWriter, cursorOwner int64) error {
	if cursorOwner == 0 {
		return errInvalidCursorOwner
	}
	_, _, err := s.serveOneWithOwner(context.Background(), rw, cursorOwner, nil, nil)
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
// that need the same buffer reuse behavior as ServeConn. The caller must call
// ReleaseOwner when the logical connection closes or before reusing cursorOwner.
// cursorOwner must be nonzero.
func (s *Server) ServeOneWithOwnerBuffered(rw io.ReadWriter, cursorOwner int64, buffers *ServeBuffers) error {
	if cursorOwner == 0 {
		return errInvalidCursorOwner
	}
	if buffers == nil {
		return s.ServeOneWithOwner(rw, cursorOwner)
	}
	readBuf, writeBuf, err := s.serveOneWithOwner(context.Background(), rw, cursorOwner, buffers.readBuf, buffers.writeBuf)
	buffers.readBuf = readBuf
	buffers.writeBuf = writeBuf
	return err
}

// ReleaseOwner closes one caller-owned logical connection. It removes all
// cursors and authentication state bound to cursorOwner. It is safe to call
// concurrently and more than once.
func (s *Server) ReleaseOwner(cursorOwner int64) {
	if s == nil || cursorOwner == 0 {
		return
	}
	s.killCursorsForOwner(cursorOwner)
	s.clearAuthState(cursorOwner)
}

func (s *Server) serveOneWithOwner(ctx context.Context, rw io.ReadWriter, cursorOwner int64, readBuf, writeBuf []byte) ([]byte, []byte, error) {
	readBuf, writeBuf, err := s.appendOneWithOwner(ctx, rw, cursorOwner, readBuf, writeBuf[:0])
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

func (s *Server) appendOneWithOwner(ctx context.Context, rw io.Reader, cursorOwner int64, readBuf, writeBuf []byte) ([]byte, []byte, error) {
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
	response, retainRequestBody, err := s.handleMessageInto(ctx, writeBuf[:0], h, body, cursorOwner)
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

func (s *Server) appendBufferedMessageWithOwner(ctx context.Context, reader *bufio.Reader, cursorOwner int64, writeBuf []byte) ([]byte, bool, error) {
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
	response, _, err := s.handleMessageInto(ctx, writeBuf, h, body, cursorOwner)
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
	response, _, err := s.handleMessageInto(context.Background(), nil, h, body, cursorOwner)
	return response, err
}

func bufferedMessageCanRetainRequestBody(h wire.Header, body []byte) bool {
	name, ok := bufferedMessageCommandName(h.OpCode, body)
	if !ok {
		return false
	}
	switch name {
	case "buildInfo", "connectionStatus", "endSessions", "find", "getMore", "hello", "hostInfo", "isMaster", "ismaster", "killCursors", "listCollections", "listDatabases", "listIndexes", "ping":
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

func (s *Server) handleMessageInto(ctx context.Context, dst []byte, h wire.Header, body []byte, cursorOwner int64) ([]byte, bool, error) {
	if s.isClosed() {
		return nil, false, errServerClosed
	}
	s.reapExpiredCursors()
	switch h.OpCode {
	case wire.OpQuery:
		return s.handleQueryInto(ctx, dst, h, body, cursorOwner)
	case wire.OpMsg:
		return s.handleMsgInto(ctx, dst, h, body, cursorOwner)
	case wire.OpCompressed:
		return nil, false, fmt.Errorf("%w: OP_COMPRESSED", wire.ErrUnsupported)
	default:
		return nil, false, fmt.Errorf("%w: opcode %d", wire.ErrUnsupported, h.OpCode)
	}
}

func (s *Server) handleQuery(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	response, _, err := s.handleQueryInto(context.Background(), nil, h, body, cursorOwner)
	return response, err
}

func (s *Server) handleQueryInto(ctx context.Context, dst []byte, h wire.Header, body []byte, cursorOwner int64) (response []byte, retainRequestBody bool, err error) {
	q, err := wire.ParseQuery(body)
	if err != nil {
		return nil, false, err
	}
	name, err := wire.CommandNameFromValidatedDocument(q.Query)
	if err != nil {
		return nil, false, err
	}
	started := time.Now()
	diagnosticFailed := false
	defer func() {
		s.noteDiagnosticCommand(name, q.Query, time.Since(started), diagnosticFailed || err != nil)
	}()

	response, err = s.commandResponse(ctx, name, q.Query, nil, cursorOwner)
	diagnosticFailed = err != nil || !commandResponseOK(response)
	if err != nil {
		return nil, name != "insert", err
	}
	reply, err := wire.AppendReplyMessage(dst, s.nextID(), h.RequestID, 0, 0, 0, response)
	return reply, name != "insert", err
}

func (s *Server) handleMsg(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	response, _, err := s.handleMsgInto(context.Background(), nil, h, body, cursorOwner)
	return response, err
}

func (s *Server) handleMsgInto(ctx context.Context, dst []byte, h wire.Header, body []byte, cursorOwner int64) (response []byte, retainRequestBody bool, err error) {
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return nil, false, err
	}
	name, err := wire.CommandNameFromValidatedDocument(msg.Body)
	if err != nil {
		return nil, false, err
	}
	started := time.Now()
	diagnosticFailed := false
	defer func() {
		s.noteDiagnosticCommand(name, msg.Body, time.Since(started), diagnosticFailed || err != nil)
	}()
	retainRequestBody = name != "insert"

	if commandRejectsReadOPMsgFeatures(name) {
		// These read paths neither consume document sequences nor support
		// unacknowledged execution. Reject both before a command can retain a
		// cursor or otherwise perform work whose response would be suppressed.
		if msg.Flags&wire.MsgFlagMoreToCome != 0 {
			return nil, retainRequestBody, fmt.Errorf("%w: %s with moreToCome flag", wire.ErrUnsupported, name)
		}
		if len(msg.Sequences) > 0 {
			return nil, retainRequestBody, fmt.Errorf("%w: %s with document sequences", wire.ErrUnsupported, name)
		}
	}
	if msg.Flags&wire.MsgFlagMoreToCome != 0 && standaloneWriteCommand(name) && !s.clusterSubmitterConfigured() {
		// moreToCome is the wire signal for an unacknowledged OP_MSG write. Reject
		// the flag itself before dispatch, even when a crafted command omits w:0
		// or claims w:1, and suppress the response so the connection remains usable.
		s.recordStandaloneMoreToComeRejection()
		diagnosticFailed = true
		return nil, retainRequestBody, nil
	}

	if name == "find" {
		// The find path builds a raw OP_MSG response directly.
		base := len(dst)
		responseID := s.nextID()
		response, err = s.findMsgResponseInto(ctx, dst, msg.Body, responseID, h.RequestID, cursorOwner)
		diagnosticFailed = err != nil || !opMsgCommandResponseOK(response[base:])
		if err != nil {
			return nil, retainRequestBody, err
		}
		return response, retainRequestBody, nil
	}

	response, err = s.commandResponse(ctx, name, msg.Body, msg.Sequences, cursorOwner)
	diagnosticFailed = err != nil || !commandResponseOK(response)
	if err != nil {
		return nil, retainRequestBody, err
	}
	if msg.Flags&wire.MsgFlagMoreToCome != 0 {
		return nil, retainRequestBody, nil
	}
	base := len(dst)
	msgResponse, err := wire.AppendMsgMessage(dst, s.nextID(), h.RequestID, 0, response)
	if err != nil || len(msgResponse)-base <= int(s.maxMessageLength()) {
		return msgResponse, retainRequestBody, err
	}
	// Keep every ordinary command response within the same advertised OP_MSG
	// limit as requests and find batches.  In particular, a bounded input can
	// otherwise expand into a large writeErrors envelope.
	tooLargeCode, tooLargeName := commandCodeBadValue, "BadValue"
	tooLargeMessage := "Mongo gateway command response exceeds maxMessageSizeBytes"
	if name == "findAndModify" {
		// Exact-ID findAndModify pre-admits its deterministic response image
		// before mutation. A concurrent filter selection can still make an
		// already-published image overflow, so never rewrite that outcome as a
		// retry-safe BadValue/no-op response.
		tooLargeCode, tooLargeName = commandCodeShutdownInProgress, "ShutdownInProgress"
		// This stays within the gateway's 128-byte minimum response envelope
		// when encoded as OP_MSG. A client must not treat it as a retry-safe
		// BadValue/no-op after a findAndModify outcome may have published.
		tooLargeMessage = "findAndModify outcome uncertain"
	}
	tooLarge, marshalErr := commandError(tooLargeCode, tooLargeName, tooLargeMessage)
	if marshalErr != nil {
		return nil, retainRequestBody, marshalErr
	}
	msgResponse, err = wire.AppendMsgMessage(dst[:base], s.nextID(), h.RequestID, 0, tooLarge)
	if err != nil {
		return nil, retainRequestBody, err
	}
	if len(msgResponse)-base > int(s.maxMessageLength()) {
		return nil, retainRequestBody, fmt.Errorf("%w: response length=%d max=%d", wire.ErrMessageTooLarge, len(msgResponse)-base, s.maxMessageLength())
	}
	return msgResponse, retainRequestBody, nil
}

// opMsgCommandResponseOK extracts the command body from one complete OP_MSG
// response. The fast find encoder returns wire bytes rather than a BSON command
// document, so dispatch uses this to preserve the same failed-command metrics
// as the ordinary commandResponse path.
func opMsgCommandResponseOK(response []byte) bool {
	header, err := wire.ParseHeader(response)
	if err != nil || header.OpCode != wire.OpMsg || int(header.MessageLength) != len(response) {
		return false
	}
	msg, err := wire.ParseMsg(response[wire.HeaderLen:])
	return err == nil && commandResponseOK(msg.Body)
}

func commandRejectsReadOPMsgFeatures(name string) bool {
	switch name {
	case "aggregate", "count", "distinct", "explain", "find", "serverStatus", "dbStats", "collStats", "top":
		return true
	default:
		return false
	}
}

func (s *Server) commandResponse(ctx context.Context, name string, command wire.Document, sequences []wire.DocumentSequence, cursorOwner int64) (response wire.Document, err error) {
	if s.authenticationRequired() && !s.authenticated(cursorOwner) && !authUnauthenticatedCommand(name) {
		return commandError(13, "Unauthorized", "Authentication required")
	}
	if s.authenticationRequired() {
		if _, response, err, allowed := s.authorizeCommand(name, command, cursorOwner); !allowed {
			return response, err
		}
	}
	if commandRejectsTransactionMarkers(name) {
		if doc, rejected, err := rejectTransactionalCommand(command, name); rejected {
			return doc, err
		}
	}
	if standaloneWriteCommand(name) && !s.clusterSubmitterConfigured() {
		return s.standaloneWriteCommandResponse(ctx, name, command, sequences, cursorOwner)
	}
	return s.dispatchCommandResponse(ctx, name, command, sequences, cursorOwner)
}

// mongoGatewaySupportedCommands is the dispatcher admission registry. Keeping
// this gate separate from the handler switch makes a newly added switch case
// unavailable until it is also added to the executable authorization matrix.
var mongoGatewaySupportedCommands = map[string]struct{}{
	"aggregate": {}, "buildInfo": {}, "connectionStatus": {}, "count": {},
	"collStats": {}, "dbStats": {},
	"create": {}, "createIndexes": {}, "createUser": {}, "delete": {},
	"distinct": {}, "dropIndexes": {}, "dropUser": {}, "endSessions": {}, "explain": {},
	"find": {}, "findAndModify": {}, "getMore": {}, "hello": {},
	"hostInfo": {}, "insert": {}, "isMaster": {}, "ismaster": {},
	"killCursors": {}, "listCollections": {}, "listDatabases": {}, "listIndexes": {},
	"ping": {}, "saslContinue": {}, "saslStart": {}, "update": {}, "updateUser": {},
	"usersInfo": {}, "serverStatus": {}, "top": {},
}

func (s *Server) dispatchCommandResponse(ctx context.Context, name string, command wire.Document, sequences []wire.DocumentSequence, cursorOwner int64) (wire.Document, error) {
	if _, supported := mongoGatewaySupportedCommands[name]; !supported {
		return commandError(59, "CommandNotFound", "unsupported MongoDB gateway command: "+name)
	}
	switch name {
	case "hello", "isMaster", "ismaster":
		return marshalDocument(s.helloResponse(ctx, command))
	case "buildInfo":
		return marshalDocument(buildInfoResponse())
	case "connectionStatus":
		return marshalDocument(s.connectionStatusResponse(cursorOwner))
	case "serverStatus":
		return s.serverStatusResponse(command)
	case "dbStats":
		return s.dbStatsResponse(command, cursorOwner)
	case "collStats":
		return s.collStatsResponse(command, cursorOwner)
	case "top":
		return s.topResponse(command)
	case "saslStart":
		return s.saslStartResponse(command, cursorOwner)
	case "saslContinue":
		return s.saslContinueResponse(command, cursorOwner)
	case "create":
		return s.createCollectionResponse(ctx, command)
	case "endSessions":
		return endSessionsResponse(command)
	case "hostInfo":
		return marshalDocument(hostInfoResponse())
	case "ping":
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})
	case "insert":
		return s.insertResponse(ctx, command, sequences)
	case "find":
		return s.findResponse(ctx, command, cursorOwner)
	case "explain":
		return s.explainResponse(ctx, command, cursorOwner)
	case "aggregate":
		return s.aggregateResponse(ctx, command, cursorOwner)
	case "count":
		return s.countResponse(ctx, command)
	case "distinct":
		return s.distinctResponse(ctx, command)
	case "findAndModify":
		return s.findAndModifyResponse(ctx, command)
	case "getMore":
		return s.getMoreResponse(command, cursorOwner)
	case "killCursors":
		return s.killCursorsResponse(command, cursorOwner)
	case "update":
		return s.updateResponse(ctx, command, sequences)
	case "delete":
		return s.deleteResponse(ctx, command, sequences)
	case "listCollections":
		return s.listCollectionsResponse(command, cursorOwner)
	case "listDatabases":
		return s.listDatabasesResponse(command, cursorOwner)
	case "createIndexes":
		return s.createIndexesResponse(command)
	case "listIndexes":
		return s.listIndexesResponse(command)
	case "dropIndexes":
		return s.dropIndexesResponse(command)
	case "createUser", "updateUser", "dropUser", "usersInfo":
		return s.userManagementResponse(name, command, cursorOwner)
	default:
		return commandError(59, "CommandNotFound", "unsupported MongoDB gateway command: "+name)
	}
}

func commandRejectsTransactionMarkers(name string) bool {
	switch name {
	case "aggregate", "count", "create", "createIndexes", "createUser", "delete", "distinct", "dropIndexes", "dropUser", "find", "findAndModify", "getMore", "insert", "killCursors", "listCollections", "listDatabases", "listIndexes", "update", "updateUser", "usersInfo":
		return true
	default:
		return false
	}
}

func (s *Server) helloResponse(ctx context.Context, command wire.Document) bson.D {
	writablePrimary := true
	if s != nil && s.clusterSubmitterConfigured() {
		writablePrimary = false
		if provider, ok := s.ClusterSubmitter.(treenativewire.ClusterAdmissionProvider); ok {
			status, err := provider.ClusterAdmissionStatus(ctx)
			writablePrimary = err == nil && status.Leader && !status.Unavailable
		}
	}
	response := helloResponse(s.maxMessageLength(), writablePrimary)
	// MongoDB drivers request this field as "<authDB>.<username>" while
	// selecting a default authenticator. Advertise only the implemented
	// mechanism and only when authentication is configured.
	if s.authenticationRequired() && s.AuthCatalog != nil {
		if _, ok := bson.Raw(command).Lookup("saslSupportedMechs").StringValueOK(); ok {
			response = append(response, bson.E{Key: "saslSupportedMechs", Value: bson.A{"SCRAM-SHA-256"}})
		}
	}
	return response
}

func helloResponse(maxMessageLength int32, writablePrimary bool) bson.D {
	return bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "isWritablePrimary", Value: writablePrimary},
		{Key: "ismaster", Value: writablePrimary},
		{Key: "secondary", Value: false},
		{Key: "helloOk", Value: true},
		{Key: "minWireVersion", Value: mongoGatewayCapabilityManifest.Advertised.MinWireVersion},
		{Key: "maxWireVersion", Value: mongoGatewayCapabilityManifest.Advertised.MaxWireVersion},
		{Key: "maxBsonObjectSize", Value: int32(defaultMaxBSONObjectSize)},
		{Key: "maxMessageSizeBytes", Value: maxMessageLength},
		{Key: "maxWriteBatchSize", Value: int32(defaultMaxWriteBatchSize)},
		{Key: "logicalSessionTimeoutMinutes", Value: mongoGatewayCapabilityManifest.Advertised.LogicalSessionTimeoutMinutes},
		{Key: "localTime", Value: time.Now().UTC()},
	}
}

func buildInfoResponse() bson.D {
	return bson.D{
		{Key: "version", Value: mongoGatewayCapabilityManifest.Advertised.MongoVersion},
		{Key: "gitVersion", Value: mongoGatewayCapabilityManifest.Advertised.GitVersion},
		{Key: "modules", Value: bson.A{}},
		{Key: "allocator", Value: "go"},
		{Key: "javascriptEngine", Value: ""},
		{Key: "sysInfo", Value: runtime.GOOS + "/" + runtime.GOARCH},
		{Key: "versionArray", Value: bson.A{
			mongoGatewayCapabilityManifest.Advertised.MongoVersionArray[0],
			mongoGatewayCapabilityManifest.Advertised.MongoVersionArray[1],
			mongoGatewayCapabilityManifest.Advertised.MongoVersionArray[2],
			mongoGatewayCapabilityManifest.Advertised.MongoVersionArray[3],
		}},
		{Key: "bits", Value: runtimePointerSizeBits()},
		{Key: "debug", Value: false},
		{Key: "maxBsonObjectSize", Value: int32(defaultMaxBSONObjectSize)},
		{Key: "storageEngines", Value: bson.A{"treedb"}},
		{Key: "treedbCapabilityManifest", Value: bson.D{
			{Key: "schema", Value: MongoGatewayCapabilitySchema},
			{Key: "version", Value: int32(MongoGatewayCapabilityVersion)},
			{Key: "identity", Value: MongoGatewayCapabilityIdentity()},
			{Key: "deploymentMode", Value: mongoGatewayCapabilityManifest.Advertised.DeploymentMode},
		}},
		{Key: "ok", Value: 1.0},
	}
}

func connectionStatusResponse() bson.D {
	return connectionStatusResponseForUser(nil)
}

func (s *Server) connectionStatusResponse(owner int64) bson.D {
	user := s.authUser(owner)
	if user == nil || s.AuthCatalog == nil {
		return connectionStatusResponseForUser(user)
	}
	roles, err := s.AuthCatalog.effectiveRolesForUser(*user)
	if err != nil {
		return connectionStatusResponseForUser(nil)
	}
	return connectionStatusResponseForUserAndRoles(user, roles)
}
func connectionStatusResponseForUser(user *AuthUser) bson.D {
	return connectionStatusResponseForUserAndRoles(user, nil)
}
func connectionStatusResponseForUserAndRoles(user *AuthUser, roles []AuthRoleGrant) bson.D {
	users := bson.A{}
	if user != nil {
		users = append(users, bson.D{{Key: "user", Value: user.Username}, {Key: "db", Value: user.AuthDB}})
	}
	return bson.D{
		{Key: "authInfo", Value: bson.D{
			{Key: "authenticatedUsers", Value: users},
			{Key: "authenticatedUserRoles", Value: authRoleDocuments(roles)},
			{Key: "authenticatedUserPrivileges", Value: authPrivilegeDocuments(roles)},
		}},
		{Key: "ok", Value: 1.0},
	}
}

func hostInfoResponse() bson.D {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}
	return bson.D{
		{Key: "system", Value: bson.D{
			{Key: "currentTime", Value: time.Now().UTC()},
			{Key: "hostname", Value: hostname},
			{Key: "cpuAddrSize", Value: runtimePointerSizeBits()},
			{Key: "numCores", Value: int32(runtime.NumCPU())},
			{Key: "cpuArch", Value: runtime.GOARCH},
		}},
		{Key: "os", Value: bson.D{
			{Key: "type", Value: runtime.GOOS},
			{Key: "name", Value: runtime.GOOS},
		}},
		{Key: "ok", Value: 1.0},
	}
}

func runtimePointerSizeBits() int32 {
	return int32(32 << (^uint(0) >> 63))
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
