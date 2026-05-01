package fastclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Client struct {
	conn          net.Conn
	maxMessageLen int32
	nextRequestID atomic.Int32
	mu            sync.Mutex
}

func Connect(ctx context.Context, address string) (*Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return New(conn), nil
}

func New(conn net.Conn) *Client {
	return &Client{conn: conn, maxMessageLen: wire.DefaultMaxMessageLength}
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) InsertManyRawBSON(ctx context.Context, database, collection string, docs []bson.Raw) (int, error) {
	if c == nil || c.conn == nil {
		return 0, errors.New("mongo gateway fast client is closed")
	}
	if database == "" {
		return 0, errors.New("database is required")
	}
	if collection == "" {
		return 0, errors.New("collection is required")
	}
	if len(docs) == 0 {
		return 0, errors.New("InsertManyRawBSON requires at least one document")
	}
	commandDoc, err := bson.Marshal(bson.D{
		{Key: "insert", Value: collection},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: database},
	})
	if err != nil {
		return 0, err
	}
	seqDocs := make([]wire.Document, len(docs))
	for i := range docs {
		if err := wire.ValidateDocument(docs[i]); err != nil {
			return 0, fmt.Errorf("documents[%d]: %w", i, err)
		}
		seqDocs[i] = wire.Document(docs[i])
	}
	msg, err := wire.AppendMsgMessageWithSequences(nil, c.nextRequestID.Add(1), 0, 0, wire.Document(commandDoc), []wire.DocumentSequence{{
		Identifier: "documents",
		Documents:  seqDocs,
	}})
	if err != nil {
		return 0, err
	}
	return c.roundTripInsert(ctx, msg, len(docs))
}

func (c *Client) roundTripInsert(ctx context.Context, msg []byte, wantN int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return 0, err
		}
		defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	}
	if err := writeFull(c.conn, msg); err != nil {
		return 0, err
	}
	header, body, err := wire.ReadMessage(c.conn, c.maxMessageLen)
	if err != nil {
		return 0, err
	}
	return parseInsertResponse(header, body, wantN)
}

func parseInsertResponse(header wire.Header, body []byte, wantN int) (int, error) {
	if header.OpCode != wire.OpMsg {
		return 0, fmt.Errorf("insert response opcode=%d want %d", header.OpCode, wire.OpMsg)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return 0, err
	}
	raw := bson.Raw(msg.Body)
	if !rawOK(raw) {
		return 0, fmt.Errorf("insert failed: %s", commandErrorMessage(raw))
	}
	n, ok := rawInt(raw, "n")
	if !ok {
		return 0, errors.New("insert response missing n")
	}
	if n != wantN {
		return 0, fmt.Errorf("insert response n=%d want %d", n, wantN)
	}
	return n, nil
}

func rawOK(raw bson.Raw) bool {
	v := raw.Lookup("ok")
	if ok, okType := v.DoubleOK(); okType {
		return ok == 1.0
	}
	if ok, okType := v.Int32OK(); okType {
		return ok == 1
	}
	if ok, okType := v.Int64OK(); okType {
		return ok == 1
	}
	if ok, okType := v.BooleanOK(); okType {
		return ok
	}
	return false
}

func rawInt(raw bson.Raw, key string) (int, bool) {
	v := raw.Lookup(key)
	if n, ok := v.Int32OK(); ok {
		return int(n), true
	}
	if n, ok := v.Int64OK(); ok {
		return int(n), true
	}
	return 0, false
}

func commandErrorMessage(raw bson.Raw) string {
	if msg, ok := raw.Lookup("errmsg").StringValueOK(); ok && msg != "" {
		return msg
	}
	var buf bytes.Buffer
	_, _ = buf.Write(raw)
	if buf.Len() == 0 {
		return "missing ok field"
	}
	return buf.String()
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}
