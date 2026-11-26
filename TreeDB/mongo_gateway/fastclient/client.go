package fastclient

import (
	"bufio"
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
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

type Client struct {
	conn          net.Conn
	rd            *bufio.Reader
	maxMessageLen int32
	nextRequestID atomic.Int32
	readBuf       []byte
	writeBuf      []byte
	docsBuf       []bson.Raw
	mu            sync.Mutex

	closed        atomic.Bool
	watchInFlight atomic.Bool
	watchFired    atomic.Bool
	watchMu       sync.Mutex
	watchGen      uint64
	watchCtx      context.Context // guarded by mu
	watchStop     func() bool     // guarded by watchMu
}

const (
	defaultReadBufferSize  = 32 * 1024
	maxRetainedReadBuffer  = 1 << 20
	maxRetainedWriteBuffer = 1 << 20
	maxRetainedDocsBuffer  = 1024
)

func Connect(ctx context.Context, address string) (*Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return New(conn), nil
}

func New(conn net.Conn) *Client {
	c := &Client{conn: conn, maxMessageLen: wire.DefaultMaxMessageLength}
	if conn != nil {
		c.rd = bufio.NewReaderSize(conn, defaultReadBufferSize)
	}
	return c
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.closed.Store(true)
	err := c.conn.Close()
	c.watchMu.Lock()
	c.watchGen++
	stop := c.watchStop
	c.watchStop = nil
	c.watchFired.Store(false)
	c.watchMu.Unlock()
	if stop != nil {
		stop()
	}
	return err
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

// FindRawBSON sends a find command and returns the cursor.firstBatch documents
// from the response. Non-find commands are rejected because this helper only
// understands find-style cursor replies.
func (c *Client) FindRawBSON(ctx context.Context, command bson.Raw) ([]bson.Raw, error) {
	if c == nil || c.conn == nil {
		return nil, errors.New("mongo gateway fast client is closed")
	}
	if len(command) == 0 {
		return nil, errors.New("find raw bson requires a command document")
	}
	if _, ok := command.Lookup("find").StringValueOK(); !ok {
		return nil, errors.New("FindRawBSON requires a find command document")
	}
	return c.roundTripFindCommand(ctx, command)
}

// FindRawBSONBorrowed is like FindRawBSON, but the returned BSON documents are
// borrowed and valid only for the duration of fn. The callback runs while the
// client mutex is held, so it must not call back into the same client and must
// not retain the batch or any bson.Raw after returning. It is intended for
// benchmark hot paths that validate a response without retaining it.
func (c *Client) FindRawBSONBorrowed(ctx context.Context, command bson.Raw, fn func([]bson.Raw) error) error {
	if fn == nil {
		return errors.New("find raw bson borrowed requires a callback")
	}
	if c == nil || c.conn == nil {
		return errors.New("mongo gateway fast client is closed")
	}
	if len(command) == 0 {
		return errors.New("find raw bson borrowed requires a command document")
	}
	if _, ok := command.Lookup("find").StringValueOK(); !ok {
		return errors.New("FindRawBSONBorrowed requires a find command document")
	}
	return c.roundTripFindCommandBorrowed(ctx, command, fn)
}

func (c *Client) roundTripInsert(ctx context.Context, msg []byte, wantN int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if err := c.beginContextWatchLocked(ctx); err != nil {
		return 0, err
	}
	defer c.endContextWatchLocked()
	if err := writeFull(c.conn, msg); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, err
	}
	header, body, err := c.readMessageLocked(true)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, err
	}
	return parseInsertResponse(header, body, wantN)
}

func (c *Client) roundTripFindCommand(ctx context.Context, command bson.Raw) ([]bson.Raw, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	msg, err := wire.AppendMsgMessage(c.writeBuf[:0], c.nextRequestID.Add(1), 0, 0, wire.Document(command))
	if err != nil {
		return nil, err
	}
	if cap(msg) <= maxRetainedWriteBuffer {
		c.writeBuf = msg[:0]
	} else {
		c.writeBuf = nil
	}

	if err := c.beginContextWatchLocked(ctx); err != nil {
		return nil, err
	}
	defer c.endContextWatchLocked()
	if err := writeFull(c.conn, msg); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	header, body, err := c.readMessageLocked(false)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return parseFindResponseInto(header, body, nil)
}

func (c *Client) roundTripFindCommandBorrowed(ctx context.Context, command bson.Raw, fn func([]bson.Raw) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	msg, err := wire.AppendMsgMessage(c.writeBuf[:0], c.nextRequestID.Add(1), 0, 0, wire.Document(command))
	if err != nil {
		return err
	}
	if cap(msg) <= maxRetainedWriteBuffer {
		c.writeBuf = msg[:0]
	} else {
		c.writeBuf = nil
	}

	if err := c.beginContextWatchLocked(ctx); err != nil {
		return err
	}
	defer c.endContextWatchLocked()
	if err := writeFull(c.conn, msg); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	header, body, err := c.readMessageLocked(true)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	docs, err := parseFindResponseInto(header, body, c.docsBuf[:0])
	if cap(docs) <= maxRetainedDocsBuffer {
		c.docsBuf = docs[:0]
	} else {
		c.docsBuf = nil
	}
	if err != nil {
		return err
	}
	return fn(docs)
}

func (c *Client) readMessageLocked(retain bool) (wire.Header, []byte, error) {
	var dst []byte
	if retain {
		dst = c.readBuf
	}
	reader := io.Reader(c.conn)
	if c.rd != nil {
		reader = c.rd
	}
	header, body, err := wire.ReadMessageInto(reader, dst, c.maxMessageLen)
	if err != nil {
		return wire.Header{}, nil, err
	}
	if retain && cap(body) <= maxRetainedReadBuffer {
		c.readBuf = body
	} else if retain {
		c.readBuf = nil
	}
	return header, body, nil
}

func (c *Client) retireContextWatchLocked() {
	c.watchMu.Lock()
	c.watchGen++
	oldStop := c.watchStop
	c.watchStop = nil
	c.watchFired.Store(false)
	c.watchMu.Unlock()
	c.watchCtx = nil
	if oldStop != nil {
		oldStop()
	}
}

func (c *Client) beginContextWatchLocked(ctx context.Context) error {
	if c.closed.Load() {
		c.retireContextWatchLocked()
		return errors.New("mongo gateway fast client is closed")
	}
	if ctx.Done() == nil {
		if c.watchCtx != nil {
			c.retireContextWatchLocked()
		}
		return nil
	}
	if ctx != c.watchCtx {
		c.retireContextWatchLocked()
		if err := ctx.Err(); err != nil {
			return err
		}
		c.watchMu.Lock()
		if c.closed.Load() {
			c.watchMu.Unlock()
			return errors.New("mongo gateway fast client is closed")
		}
		c.watchGen++
		gen := c.watchGen
		c.watchFired.Store(false)
		c.watchStop = context.AfterFunc(ctx, func() {
			c.watchMu.Lock()
			defer c.watchMu.Unlock()
			if c.watchGen != gen || c.closed.Load() {
				return
			}
			c.watchFired.Store(true)
			if c.watchInFlight.Load() {
				_ = c.conn.SetDeadline(time.Now())
			}
		})
		c.watchMu.Unlock()
		c.watchCtx = ctx
	}
	c.watchInFlight.Store(true)
	if c.watchFired.Load() {
		c.endContextWatchLocked()
		if err := ctx.Err(); err != nil {
			return err
		}
		return context.Canceled
	}
	return nil
}

func (c *Client) endContextWatchLocked() {
	if !c.watchInFlight.Swap(false) {
		return
	}
	if c.watchFired.Load() {
		// Synchronize with any in-flight AfterFunc callback and clear the
		// deadline so the connection is not left permanently expired if a
		// subsequent request uses a fresh non-cancelled context.
		c.watchMu.Lock()
		_ = c.conn.SetDeadline(time.Time{})
		c.watchGen++
		oldStop := c.watchStop
		c.watchStop = nil
		c.watchFired.Store(false)
		c.watchMu.Unlock()
		c.watchCtx = nil
		if oldStop != nil {
			oldStop()
		}
	}
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

func parseFindResponseInto(header wire.Header, body []byte, dst []bson.Raw) ([]bson.Raw, error) {
	if header.OpCode != wire.OpMsg {
		return nil, fmt.Errorf("find response opcode=%d want %d", header.OpCode, wire.OpMsg)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return nil, err
	}
	raw := bson.Raw(msg.Body)
	if !rawOK(raw) {
		return nil, fmt.Errorf("find failed: %s", commandErrorMessage(raw))
	}
	cursor, ok := raw.Lookup("cursor").DocumentOK()
	if !ok {
		return nil, errors.New("find response missing cursor")
	}
	batch, ok := cursor.Lookup("firstBatch").ArrayOK()
	if !ok {
		return nil, errors.New("find response missing cursor.firstBatch")
	}
	return rawDocumentsFromArrayInto(batch, dst)
}

func rawDocumentsFromArrayInto(batch bson.RawArray, dst []bson.Raw) ([]bson.Raw, error) {
	length, rem, ok := bsoncore.ReadLength(bsoncore.Array(batch))
	if !ok || length < 5 || int(length) > len(batch) {
		return nil, errors.New("malformed find cursor.firstBatch")
	}
	rem = rem[:int(length)-4]
	if len(rem) == 0 || rem[len(rem)-1] != 0x00 {
		return nil, errors.New("malformed find cursor.firstBatch")
	}
	rem = rem[:len(rem)-1]

	docs := dst[:0]
	if cap(docs) == 0 {
		docs = make([]bson.Raw, 0, rawArrayDocumentCapacityHint(len(rem)))
	}
	for len(rem) > 0 {
		elem, next, ok := bsoncore.ReadElement(rem)
		if !ok {
			return nil, errors.New("malformed find cursor.firstBatch")
		}
		value, err := elem.ValueErr()
		if err != nil {
			return nil, err
		}
		doc, ok := value.DocumentOK()
		if !ok {
			return nil, errors.New("find firstBatch entry is not a document")
		}
		docs = append(docs, bson.Raw(doc))
		rem = next
	}
	return docs, nil
}

func rawArrayDocumentCapacityHint(payloadBytes int) int {
	if payloadBytes <= 0 {
		return 0
	}
	const minElementOverhead = 8
	const maxInitialCapacity = 1024
	hint := payloadBytes / minElementOverhead
	if hint < 1 {
		return 1
	}
	if hint > maxInitialCapacity {
		return maxInitialCapacity
	}
	return hint
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
