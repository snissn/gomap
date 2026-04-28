package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	defaultMaxBSONObjectSize = 16 * 1024 * 1024
	defaultMaxWriteBatchSize = 100_000
)

type Server struct {
	MaxMessageLength int32

	nextResponseID atomic.Int32
}

func NewServer() *Server {
	s := &Server{MaxMessageLength: wire.DefaultMaxMessageLength}
	s.nextResponseID.Store(1)
	return s
}

func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := s.ServeOne(conn); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (s *Server) ServeOne(rw io.ReadWriter) error {
	h, body, err := wire.ReadMessage(rw, s.maxMessageLength())
	if err != nil {
		return err
	}
	response, err := s.handleMessage(h, body)
	if err != nil {
		return err
	}
	_, err = rw.Write(response)
	return err
}

func (s *Server) handleMessage(h wire.Header, body []byte) ([]byte, error) {
	switch h.OpCode {
	case wire.OpQuery:
		return s.handleQuery(h, body)
	case wire.OpMsg:
		return s.handleMsg(h, body)
	case wire.OpCompressed:
		return nil, fmt.Errorf("%w: OP_COMPRESSED", wire.ErrUnsupported)
	default:
		return nil, fmt.Errorf("%w: opcode %d", wire.ErrUnsupported, h.OpCode)
	}
}

func (s *Server) handleQuery(h wire.Header, body []byte) ([]byte, error) {
	q, err := wire.ParseQuery(body)
	if err != nil {
		return nil, err
	}
	name, err := wire.CommandName(q.Query)
	if err != nil {
		return nil, err
	}

	response, err := s.commandResponse(name)
	if err != nil {
		return nil, err
	}
	return wire.AppendReplyMessage(nil, s.nextID(), h.RequestID, 0, 0, 0, response)
}

func (s *Server) handleMsg(h wire.Header, body []byte) ([]byte, error) {
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return nil, err
	}
	name, err := wire.CommandName(msg.Body)
	if err != nil {
		return nil, err
	}

	response, err := s.commandResponse(name)
	if err != nil {
		return nil, err
	}
	return wire.AppendMsgMessage(nil, s.nextID(), h.RequestID, 0, response)
}

func (s *Server) commandResponse(name string) (wire.Document, error) {
	switch name {
	case "hello", "isMaster", "ismaster":
		return marshalDocument(helloResponse(s.maxMessageLength()))
	case "ping":
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})
	default:
		return marshalDocument(bson.D{
			{Key: "ok", Value: 0.0},
			{Key: "errmsg", Value: "unsupported MongoDB gateway command: " + name},
			{Key: "code", Value: int32(59)},
			{Key: "codeName", Value: "CommandNotFound"},
		})
	}
}

func helloResponse(maxMessageLength int32) bson.D {
	return bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "isWritablePrimary", Value: true},
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

func (s *Server) maxMessageLength() int32 {
	if s.MaxMessageLength <= 0 {
		return wire.DefaultMaxMessageLength
	}
	return s.MaxMessageLength
}

func (s *Server) nextID() int32 {
	return s.nextResponseID.Add(1)
}
