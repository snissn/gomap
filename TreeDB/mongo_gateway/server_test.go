package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type readWriter struct {
	r io.Reader
	w bytes.Buffer
}

func (rw *readWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *readWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

type partialReadWriter struct {
	r        io.Reader
	w        bytes.Buffer
	maxWrite int
}

func (rw *partialReadWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *partialReadWriter) Write(p []byte) (int, error) {
	if len(p) > rw.maxWrite {
		p = p[:rw.maxWrite]
	}
	return rw.w.Write(p)
}

func TestServerHandlesQueryHello(t *testing.T) {
	queryDoc := mustDocument(t, bson.D{
		{Key: "isMaster", Value: int32(1)},
		{Key: "helloOk", Value: true},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendQueryMessage(nil, 100, 0, 0, "admin.$cmd", 0, -1, queryDoc, nil)
	if err != nil {
		t.Fatalf("AppendQueryMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	h, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatalf("ReadMessage response: %v", err)
	}
	if h.OpCode != wire.OpReply || h.RequestID != 1 || h.ResponseTo != 100 {
		t.Fatalf("response header=%+v", h)
	}
	reply, err := wire.ParseReply(body)
	if err != nil {
		t.Fatalf("ParseReply: %v", err)
	}
	if len(reply.Documents) != 1 {
		t.Fatalf("reply document count=%d want 1", len(reply.Documents))
	}
	assertOK(t, reply.Documents[0])
	assertBool(t, reply.Documents[0], "helloOk", true)
	assertBool(t, reply.Documents[0], "ismaster", true)
	assertBool(t, reply.Documents[0], "secondary", false)
}

func TestServerHandlesMsgPing(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 200, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	h, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatalf("ReadMessage response: %v", err)
	}
	if h.OpCode != wire.OpMsg || h.RequestID != 1 || h.ResponseTo != 200 {
		t.Fatalf("response header=%+v", h)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	assertOK(t, msg.Body)
}

func TestServerInsertAndFindByID(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	id := bson.NewObjectID()
	insertDoc := mustDocument(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	insertPayload := mustDocument(t, bson.D{
		{Key: "_id", Value: id},
		{Key: "name", Value: "ada"},
		{Key: "age", Value: int64(37)},
		{Key: "active", Value: true},
	})
	insertReq, err := wire.AppendMsgMessageWithSequences(nil, 210, 0, 0, insertDoc, []wire.DocumentSequence{{
		Identifier: "documents",
		Documents:  []wire.Document{insertPayload},
	}})
	if err != nil {
		t.Fatalf("AppendMsgMessage insert: %v", err)
	}
	insertRW := &readWriter{r: bytes.NewReader(insertReq)}

	if err := server.ServeOne(insertRW); err != nil {
		t.Fatalf("ServeOne insert: %v", err)
	}
	insertResp := readMsgResponse(t, insertRW.w.Bytes(), 210)
	assertOK(t, insertResp)
	assertInt32(t, insertResp, "n", 1)

	findDoc := mustDocument(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	findReq, err := wire.AppendMsgMessage(nil, 211, 0, 0, findDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage find: %v", err)
	}
	findRW := &readWriter{r: bytes.NewReader(findReq)}

	if err := server.ServeOne(findRW); err != nil {
		t.Fatalf("ServeOne find: %v", err)
	}
	findResp := readMsgResponse(t, findRW.w.Bytes(), 211)
	assertOK(t, findResp)
	firstBatch := cursorFirstBatch(t, findResp)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotID, ok := firstBatch[0].Lookup("_id").ObjectIDOK()
	if !ok || gotID != id {
		t.Fatalf("firstBatch _id=%v ok=%v want %v", gotID, ok, id)
	}
	gotAge, ok := firstBatch[0].Lookup("age").Int64OK()
	if !ok || gotAge != 37 {
		t.Fatalf("firstBatch age=%d ok=%v want 37", gotAge, ok)
	}
	assertBool(t, firstBatch[0], "active", true)
}

func TestServerUpdateAndDeleteByID(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	id := bson.NewObjectID()
	insertResponse := serveCommand(t, server, 220, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: id},
			{Key: "name", Value: "ada"},
			{Key: "age", Value: int64(37)},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, insertResponse)

	updateResponse := serveCommand(t, server, 221, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "age", Value: int64(38)},
				{Key: "city", Value: "London"},
			}}}},
			{Key: "multi", Value: false},
			{Key: "upsert", Value: false},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, updateResponse)
	assertInt32(t, updateResponse, "n", 1)
	assertInt32(t, updateResponse, "nModified", 1)

	findResponse := serveCommand(t, server, 222, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotAge, ok := firstBatch[0].Lookup("age").Int64OK()
	if !ok || gotAge != 38 {
		t.Fatalf("firstBatch age=%d ok=%v want 38", gotAge, ok)
	}
	gotCity, ok := firstBatch[0].Lookup("city").StringValueOK()
	if !ok || gotCity != "London" {
		t.Fatalf("firstBatch city=%q ok=%v want London", gotCity, ok)
	}

	deleteResponse := serveCommand(t, server, 223, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "limit", Value: int32(1)},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, deleteResponse)
	assertInt32(t, deleteResponse, "n", 1)

	afterDelete := serveCommand(t, server, 224, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	if firstBatch := cursorFirstBatch(t, afterDelete); len(firstBatch) != 0 {
		t.Fatalf("firstBatch len=%d want 0", len(firstBatch))
	}
}

func TestServerUpdateRejectsIDMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	id := bson.NewObjectID()
	insertResponse := serveCommand(t, server, 225, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: id},
			{Key: "name", Value: "ada"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, insertResponse)

	updateResponse := serveCommand(t, server, 226, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "_id", Value: bson.NewObjectID()},
			}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "BadValue")
}

func TestServerIndexMetadataCommands(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	createResponse := serveCommand(t, server, 227, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
			{Key: "unique", Value: true},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, createResponse)
	assertInt32(t, createResponse, "numIndexesBefore", 1)
	assertInt32(t, createResponse, "numIndexesAfter", 2)

	collectionsResponse := serveCommand(t, server, 228, bson.D{
		{Key: "listCollections", Value: int32(1)},
		{Key: "nameOnly", Value: true},
		{Key: "$db", Value: "app"},
	})
	collectionBatch := cursorFirstBatch(t, collectionsResponse)
	if len(collectionBatch) != 1 {
		t.Fatalf("collection batch len=%d want 1", len(collectionBatch))
	}
	if got, ok := collectionBatch[0].Lookup("name").StringValueOK(); !ok || got != "users" {
		t.Fatalf("collection name=%q ok=%v want users", got, ok)
	}

	indexesResponse := serveCommand(t, server, 229, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	indexBatch := cursorFirstBatch(t, indexesResponse)
	if got, want := len(indexBatch), 2; got != want {
		t.Fatalf("index batch len=%d want %d", got, want)
	}
	assertIndexName(t, indexBatch[0], "_id_")
	assertIndexName(t, indexBatch[1], "email_1")
	assertBool(t, wire.Document(indexBatch[1]), "unique", true)

	dropResponse := serveCommand(t, server, 230, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: "email_1"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, dropResponse)
	assertInt32(t, dropResponse, "nIndexesWas", 2)

	afterDrop := serveCommand(t, server, 231, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	indexBatch = cursorFirstBatch(t, afterDrop)
	if got, want := len(indexBatch), 1; got != want {
		t.Fatalf("index batch after drop len=%d want %d", got, want)
	}
	assertIndexName(t, indexBatch[0], "_id_")
}

func TestServerFindByIDMissingCollectionReturnsEmptyBatch(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	findDoc := mustDocument(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "missing"}}},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessage(nil, 212, 0, 0, findDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}
	resp := readMsgResponse(t, rw.w.Bytes(), 212)
	assertOK(t, resp)
	if firstBatch := cursorFirstBatch(t, resp); len(firstBatch) != 0 {
		t.Fatalf("firstBatch len=%d want 0", len(firstBatch))
	}
}

func TestServerInsertRejectsUnsupportedBSONTypes(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	insertDoc := mustDocument(t, bson.D{
		{Key: "insert", Value: "events"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: "e1"},
			{Key: "at", Value: time.Now()},
		}}},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessage(nil, 213, 0, 0, insertDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}
	resp := readMsgResponse(t, rw.w.Bytes(), 213)
	assertCommandError(t, resp, "BadValue")
}

func TestServerHandlesPartialWrites(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 201, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &partialReadWriter{r: bytes.NewReader(req), maxWrite: 5}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	h, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatalf("ReadMessage response: %v", err)
	}
	if h.OpCode != wire.OpMsg || h.ResponseTo != 201 {
		t.Fatalf("response header=%+v", h)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	assertOK(t, msg.Body)
}

func TestServerRejectsCompressedMessages(t *testing.T) {
	req, err := wire.AppendMessage(nil, 300, 0, wire.OpCompressed, nil)
	if err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	err = NewServer().ServeOne(rw)
	if !errors.Is(err, wire.ErrUnsupported) {
		t.Fatalf("ServeOne err=%v want ErrUnsupported", err)
	}
	if rw.w.Len() != 0 {
		t.Fatalf("unexpected response bytes=%d", rw.w.Len())
	}
}

func TestServeConnCancellationInterruptsRead(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewServer().ServeConn(ctx, serverConn)
	}()

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ServeConn err=%v want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not return after context cancellation")
	}
}

func TestServerMaxMessageLengthClampsToWireLimit(t *testing.T) {
	server := &Server{MaxMessageLength: wire.DefaultMaxMessageLength + 1}
	if got := server.maxMessageLength(); got != wire.DefaultMaxMessageLength {
		t.Fatalf("maxMessageLength=%d want %d", got, wire.DefaultMaxMessageLength)
	}
}

func mustDocument(tb testing.TB, doc bson.D) wire.Document {
	tb.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal BSON document: %v", err)
	}
	return wire.Document(raw)
}

func serveCommand(tb testing.TB, server *Server, requestID int32, doc bson.D) wire.Document {
	tb.Helper()
	commandDoc := mustDocument(tb, doc)
	req, err := wire.AppendMsgMessage(nil, requestID, 0, 0, commandDoc)
	if err != nil {
		tb.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}
	if err := server.ServeOne(rw); err != nil {
		tb.Fatalf("ServeOne: %v", err)
	}
	return readMsgResponse(tb, rw.w.Bytes(), requestID)
}

func assertOK(tb testing.TB, doc wire.Document) {
	tb.Helper()
	raw := bson.Raw(doc)
	value := raw.Lookup("ok")
	ok, okType := value.DoubleOK()
	if !okType || ok != 1.0 {
		tb.Fatalf("ok=%v typeOK=%v want 1.0", ok, okType)
	}
}

func assertBool(tb testing.TB, doc wire.Document, key string, want bool) {
	tb.Helper()
	raw := bson.Raw(doc)
	value := raw.Lookup(key)
	got, ok := value.BooleanOK()
	if !ok || got != want {
		tb.Fatalf("%s=%v typeOK=%v want %v", key, got, ok, want)
	}
}

func readMsgResponse(tb testing.TB, response []byte, responseTo int32) wire.Document {
	tb.Helper()
	h, body, err := wire.ReadMessage(bytes.NewReader(response), 0)
	if err != nil {
		tb.Fatalf("ReadMessage response: %v", err)
	}
	if h.OpCode != wire.OpMsg || h.ResponseTo != responseTo {
		tb.Fatalf("response header=%+v want OP_MSG responseTo=%d", h, responseTo)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		tb.Fatalf("ParseMsg response: %v", err)
	}
	return msg.Body
}

func cursorFirstBatch(tb testing.TB, doc wire.Document) []bson.Raw {
	tb.Helper()
	cursor, ok := bson.Raw(doc).Lookup("cursor").DocumentOK()
	if !ok {
		tb.Fatalf("cursor missing or not document in %v", bson.Raw(doc))
	}
	batch, ok := cursor.Lookup("firstBatch").ArrayOK()
	if !ok {
		tb.Fatalf("cursor.firstBatch missing or not array in %v", cursor)
	}
	values, err := batch.Values()
	if err != nil {
		tb.Fatalf("firstBatch values: %v", err)
	}
	out := make([]bson.Raw, 0, len(values))
	for i, value := range values {
		doc, ok := value.DocumentOK()
		if !ok {
			tb.Fatalf("firstBatch[%d] is not a document", i)
		}
		out = append(out, doc)
	}
	return out
}

func assertInt32(tb testing.TB, doc wire.Document, key string, want int32) {
	tb.Helper()
	value := bson.Raw(doc).Lookup(key)
	got, ok := value.Int32OK()
	if !ok || got != want {
		tb.Fatalf("%s=%v typeOK=%v want %v", key, got, ok, want)
	}
}

func assertCommandError(tb testing.TB, doc wire.Document, codeName string) {
	tb.Helper()
	raw := bson.Raw(doc)
	value := raw.Lookup("ok")
	ok, okType := value.DoubleOK()
	if !okType || ok != 0.0 {
		tb.Fatalf("ok=%v typeOK=%v want 0.0", ok, okType)
	}
	got, gotOK := raw.Lookup("codeName").StringValueOK()
	if !gotOK || got != codeName {
		tb.Fatalf("codeName=%q typeOK=%v want %q", got, gotOK, codeName)
	}
}

func assertIndexName(tb testing.TB, doc bson.Raw, want string) {
	tb.Helper()
	got, ok := doc.Lookup("name").StringValueOK()
	if !ok || got != want {
		tb.Fatalf("index name=%q typeOK=%v want %q", got, ok, want)
	}
}
