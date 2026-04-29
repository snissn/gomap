package mongogateway

import (
	"context"
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
	defaultMaxBSONObjectSize    = 16 * 1024 * 1024
	defaultMaxWriteBatchSize    = 100_000
	defaultMaxFindScanDocuments = 10_000
	defaultMaxOpenCursors       = 1_024
	defaultCursorBatchSize      = 101
)

type Server struct {
	MaxMessageLength     int32
	MaxFindScanDocuments int
	MaxOpenCursors       int
	Collections          *collections.CollectionManager

	nextResponseID   atomic.Int32
	nextConnectionID atomic.Int64
	nextCursorID     atomic.Int64
	cursorMu         sync.Mutex
	cursors          map[int64]*serverCursor
}

type serverCursor struct {
	ns         string
	owner      int64
	docs       []wire.Document
	projection compiledProjection
	pos        int
}

func NewServer() *Server {
	s := &Server{MaxMessageLength: wire.DefaultMaxMessageLength}
	s.nextResponseID.Store(0)
	return s
}

func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	owner := s.nextConnectionID.Add(1)
	defer s.killCursorsForOwner(owner)

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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := s.ServeOneWithOwner(conn, owner); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctx.Err() != nil && (errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)) {
				return ctx.Err()
			}
			return err
		}
	}
}

// ServeOne serves a single MongoDB wire message using cursor owner 0.
// Callers serving a real long-lived connection should use ServeOneWithOwner or
// ServeConn so opened cursors participate in owner-based cleanup.
func (s *Server) ServeOne(rw io.ReadWriter) error {
	return s.ServeOneWithOwner(rw, 0)
}

func (s *Server) ServeOneWithOwner(rw io.ReadWriter, cursorOwner int64) error {
	h, body, err := wire.ReadMessage(rw, s.maxMessageLength())
	if err != nil {
		return err
	}
	response, err := s.handleMessage(h, body, cursorOwner)
	if err != nil {
		return err
	}
	return writeFull(rw, response)
}

func (s *Server) handleMessage(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	switch h.OpCode {
	case wire.OpQuery:
		return s.handleQuery(h, body, cursorOwner)
	case wire.OpMsg:
		return s.handleMsg(h, body, cursorOwner)
	case wire.OpCompressed:
		return nil, fmt.Errorf("%w: OP_COMPRESSED", wire.ErrUnsupported)
	default:
		return nil, fmt.Errorf("%w: opcode %d", wire.ErrUnsupported, h.OpCode)
	}
}

func (s *Server) handleQuery(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	q, err := wire.ParseQuery(body)
	if err != nil {
		return nil, err
	}
	name, err := wire.CommandName(q.Query)
	if err != nil {
		return nil, err
	}

	response, err := s.commandResponse(name, q.Query, nil, cursorOwner)
	if err != nil {
		return nil, err
	}
	return wire.AppendReplyMessage(nil, s.nextID(), h.RequestID, 0, 0, 0, response)
}

func (s *Server) handleMsg(h wire.Header, body []byte, cursorOwner int64) ([]byte, error) {
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return nil, err
	}
	name, err := wire.CommandName(msg.Body)
	if err != nil {
		return nil, err
	}

	response, err := s.commandResponse(name, msg.Body, msg.Sequences, cursorOwner)
	if err != nil {
		return nil, err
	}
	return wire.AppendMsgMessage(nil, s.nextID(), h.RequestID, 0, response)
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

func (s *Server) nextID() int32 {
	return s.nextResponseID.Add(1)
}
