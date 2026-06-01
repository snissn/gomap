package mongogateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
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

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

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
	assertInt32(t, reply.Documents[0], "logicalSessionTimeoutMinutes", defaultLogicalSessionTimeout)
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

func TestServerHandlesConnectionStatus(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "connectionStatus", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 201, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	resp := readMsgResponse(t, rw.w.Bytes(), 201)
	assertOK(t, resp)
	authInfo, ok := resp.Lookup("authInfo").DocumentOK()
	if !ok {
		t.Fatalf("authInfo missing or non-document in %s", resp)
	}
	if _, ok := authInfo.Lookup("authenticatedUsers").ArrayOK(); !ok {
		t.Fatalf("authenticatedUsers missing or non-array in %s", authInfo)
	}
	if _, ok := authInfo.Lookup("authenticatedUserRoles").ArrayOK(); !ok {
		t.Fatalf("authenticatedUserRoles missing or non-array in %s", authInfo)
	}
	if _, ok := authInfo.Lookup("authenticatedUserPrivileges").ArrayOK(); !ok {
		t.Fatalf("authenticatedUserPrivileges missing or non-array in %s", authInfo)
	}
}

func TestServerHandlesHostInfo(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "hostInfo", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 202, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	resp := readMsgResponse(t, rw.w.Bytes(), 202)
	assertOK(t, resp)
	system, ok := resp.Lookup("system").DocumentOK()
	if !ok {
		t.Fatalf("system missing or non-document in %s", resp)
	}
	if _, ok := system.Lookup("hostname").StringValueOK(); !ok {
		t.Fatalf("hostname missing or non-string in %s", system)
	}
	if _, ok := system.Lookup("numCores").Int32OK(); !ok {
		t.Fatalf("numCores missing or non-int32 in %s", system)
	}
	osInfo, ok := resp.Lookup("os").DocumentOK()
	if !ok {
		t.Fatalf("os missing or non-document in %s", resp)
	}
	if _, ok := osInfo.Lookup("type").StringValueOK(); !ok {
		t.Fatalf("os.type missing or non-string in %s", osInfo)
	}
}

func TestServerHandlesBuildInfo(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "buildInfo", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 203, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	resp := readMsgResponse(t, rw.w.Bytes(), 203)
	assertOK(t, resp)
	if version, ok := resp.Lookup("version").StringValueOK(); !ok || version != "7.0.0" {
		t.Fatalf("version=%q ok=%v want 7.0.0", version, ok)
	}
	if _, ok := resp.Lookup("versionArray").ArrayOK(); !ok {
		t.Fatalf("versionArray missing or non-array in %s", resp)
	}
	if bits, ok := resp.Lookup("bits").Int32OK(); !ok || bits != runtimePointerSizeBits() {
		t.Fatalf("bits=%d ok=%v want %d", bits, ok, runtimePointerSizeBits())
	}
	if _, ok := resp.Lookup("maxBsonObjectSize").Int32OK(); !ok {
		t.Fatalf("maxBsonObjectSize missing or non-int32 in %s", resp)
	}
	if _, ok := resp.Lookup("storageEngines").ArrayOK(); !ok {
		t.Fatalf("storageEngines missing or non-array in %s", resp)
	}
}

func TestServerHandlesEndSessions(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "endSessions", Value: bson.A{bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}}},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 204, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	assertOK(t, readMsgResponse(t, rw.w.Bytes(), 204))

	badType := serveCommand(t, NewServer(), 205, bson.D{{Key: "endSessions", Value: "not-array"}, {Key: "$db", Value: "admin"}})
	assertCommandError(t, badType, "FailedToParse")

	badID := serveCommand(t, NewServer(), 206, bson.D{
		{Key: "endSessions", Value: bson.A{bson.D{{Key: "id", Value: bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}}}}},
		{Key: "$db", Value: "admin"},
	})
	assertCommandError(t, badID, "FailedToParse")
}

func TestServerRetainsReadBufferForSafeCommands(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 22001, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	server := NewServer()
	readBuf, _, err := server.serveOneWithOwner(&readWriter{r: bytes.NewReader(req)}, 1, make([]byte, 0, len(req)), nil)
	if err != nil {
		t.Fatalf("serveOneWithOwner: %v", err)
	}
	if readBuf == nil {
		t.Fatal("read buffer was not retained for ping")
	}
}

func TestServerDoesNotRetainReadBufferAfterBSONInsert(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	commandDoc := mustDocument(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "payload", Value: strings.Repeat("x", 128)},
		}}},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessage(nil, 22002, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}
	readBuf, _, err := server.serveOneWithOwner(rw, 1, make([]byte, 0, len(req)), nil)
	if err != nil {
		t.Fatalf("serveOneWithOwner: %v", err)
	}
	if readBuf != nil {
		t.Fatal("read buffer was retained after BSON insert")
	}
	assertOK(t, readMsgResponse(t, rw.w.Bytes(), 22002))
}

func TestBufferedMessageCanRetainRequestBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  bson.D
		want bool
	}{
		{name: "ping", doc: bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}}, want: true},
		{name: "buildInfo", doc: bson.D{{Key: "buildInfo", Value: int32(1)}, {Key: "$db", Value: "admin"}}, want: true},
		{name: "connectionStatus", doc: bson.D{{Key: "connectionStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}}, want: true},
		{name: "create", doc: bson.D{{Key: "create", Value: "users"}, {Key: "$db", Value: "app"}}, want: false},
		{name: "endSessions", doc: bson.D{{Key: "endSessions", Value: bson.A{}}, {Key: "$db", Value: "admin"}}, want: true},
		{name: "hostInfo", doc: bson.D{{Key: "hostInfo", Value: int32(1)}, {Key: "$db", Value: "admin"}}, want: true},
		{name: "find", doc: bson.D{{Key: "find", Value: "users"}, {Key: "$db", Value: "app"}}, want: true},
		{name: "update", doc: bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{}}, {Key: "$db", Value: "app"}}, want: false},
		{name: "insert", doc: bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}}, {Key: "$db", Value: "app"}}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commandDoc := mustDocument(t, tc.doc)
			req, err := wire.AppendMsgMessage(nil, 22003, 0, 0, commandDoc)
			if err != nil {
				t.Fatalf("AppendMsgMessage: %v", err)
			}
			header, err := wire.ParseHeader(req[:wire.HeaderLen])
			if err != nil {
				t.Fatalf("ParseHeader: %v", err)
			}
			if got := bufferedMessageCanRetainRequestBody(header, req[wire.HeaderLen:]); got != tc.want {
				t.Fatalf("retain=%v want %v", got, tc.want)
			}
		})
	}
}

func TestServerRejectsFindWithMoreToCome(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessage(nil, 201, 0, wire.MsgFlagMoreToCome, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
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

func TestServerRejectsFindWithDocumentSequences(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessageWithSequences(nil, 202, 0, 0, commandDoc, []wire.DocumentSequence{{
		Identifier: "ignored",
		Documents:  []wire.Document{mustDocument(t, bson.D{{Key: "x", Value: 1}})},
	}})
	if err != nil {
		t.Fatalf("AppendMsgMessageWithSequences: %v", err)
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

	noopResponse := serveCommand(t, server, 222, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "age", Value: int64(38)},
			}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, noopResponse)
	assertInt32(t, noopResponse, "n", 1)
	assertInt32(t, noopResponse, "nModified", 0)

	findResponse := serveCommand(t, server, 223, bson.D{
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

	deleteResponse := serveCommand(t, server, 224, bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "limit", Value: int32(1)},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, deleteResponse)
	assertInt32(t, deleteResponse, "n", 1)

	afterDelete := serveCommand(t, server, 225, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	if firstBatch := cursorFirstBatch(t, afterDelete); len(firstBatch) != 0 {
		t.Fatalf("firstBatch len=%d want 0", len(firstBatch))
	}
}

func TestRunMongoUpdateOneUsesBSONSetFastPath(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := collections.NewCollectionManager(db)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "app.users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	key, stored, err := prepareInsertDocument(mustDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
	}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare insert: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{key}, [][]byte{stored}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	update, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}},
	}))
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	matched, modified, err := runMongoUpdateOne(col, update)
	if err != nil {
		t.Fatalf("run update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true/true", matched, modified)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.StructuredUpdateApplications, 1; got != want {
		t.Fatalf("structured update applications=%d want %d", got, want)
	}
	if stats.Callback != 0 {
		t.Fatalf("callback duration=%s want zero for BSON set gateway update", stats.Callback)
	}
}

func TestServerUpdateTemplateV1DefaultAddsFields(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatTemplateV1,
	}
	id := bson.NewObjectID()
	insertResponse := serveCommand(t, server, 2251, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: id},
			{Key: "name", Value: "ada"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, insertResponse)

	updateResponse := serveCommand(t, server, 2252, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "city", Value: "London"},
				{Key: "updated", Value: true},
			}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, updateResponse)
	assertInt32(t, updateResponse, "n", 1)
	assertInt32(t, updateResponse, "nModified", 1)

	findResponse := serveCommand(t, server, 2253, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotCity, ok := firstBatch[0].Lookup("city").StringValueOK()
	if !ok || gotCity != "London" {
		t.Fatalf("firstBatch city=%q ok=%v want London", gotCity, ok)
	}
	assertBool(t, firstBatch[0], "updated", true)

	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: id}}), collections.DocumentFormatJSON)
	if err != nil {
		t.Fatalf("encode id key: %v", err)
	}
	stored, err := col.Get(key)
	if err != nil {
		t.Fatalf("get stored doc: %v", err)
	}
	if _, err := col.StoredDocumentJSON(stored); err != nil {
		t.Fatalf("materialize stored doc: %v", err)
	}
}

func TestServerBSONDefaultStoresNativeBSONAndUpdatesIndexes(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	id := bson.NewObjectID()
	assertOK(t, serveCommand(t, server, 2261, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
			bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}}, {Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 2262, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: id},
			{Key: "email", Value: "ada@example.com"},
			{Key: "city", Value: "hnl"},
			{Key: "age", Value: int64(37)},
		}}},
		{Key: "$db", Value: "app"},
	}))

	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: id}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("encode id key: %v", err)
	}
	stored, err := col.Get(key)
	if err != nil {
		t.Fatalf("get stored BSON: %v", err)
	}
	if err := bson.Raw(stored).Validate(); err != nil {
		t.Fatalf("stored native BSON failed validation: %v", err)
	}
	if json.Valid(stored) {
		t.Fatalf("stored BSON is also valid JSON: %q", stored[:min(len(stored), 32)])
	}
	if got, ok := bson.Raw(stored).Lookup("email").StringValueOK(); !ok || got != "ada@example.com" {
		t.Fatalf("stored email=%q ok=%v want ada@example.com", got, ok)
	}
	emailIDs, err := col.FindByIndexValue("email_1", "ada@example.com")
	if err != nil {
		t.Fatalf("find email index: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], key) {
		t.Fatalf("email ids=%q want %q", emailIDs, key)
	}

	assertOK(t, serveCommand(t, server, 2263, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "city", Value: "sea"},
				{Key: "active", Value: true},
			}}}},
		}}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 2264, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: "sea"}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	assertBool(t, firstBatch[0], "active", true)
	cityIDs, err := col.FindByIndexValue("city_1", "sea")
	if err != nil {
		t.Fatalf("find updated city index: %v", err)
	}
	if len(cityIDs) != 1 || !bytes.Equal(cityIDs[0], key) {
		t.Fatalf("city ids=%q want %q", cityIDs, key)
	}
	oldCityIDs, err := col.FindByIndexValue("city_1", "hnl")
	if err != nil {
		t.Fatalf("find old city index: %v", err)
	}
	if len(oldCityIDs) != 0 {
		t.Fatalf("old city ids=%q want none", oldCityIDs)
	}
}

func TestPrepareInsertDocumentBSONAllowsNativeUnindexedTypes(t *testing.T) {
	doc := mustDocument(t, bson.D{
		{Key: "_id", Value: "native"},
		{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte{1, 2, 3}}},
	})
	_, stored, err := prepareInsertDocument(doc, collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare BSON insert document: %v", err)
	}
	if err := bson.Raw(stored).Validate(); err != nil {
		t.Fatalf("stored BSON invalid: %v", err)
	}
	subtype, payload := bson.Raw(stored).Lookup("payload").Binary()
	if subtype != 0x00 || !bytes.Equal(payload, []byte{1, 2, 3}) {
		t.Fatalf("payload subtype/data=%#x/%v", subtype, payload)
	}

	_, _, err = prepareInsertDocument(doc, collections.DocumentFormatJSON)
	if err == nil || !strings.Contains(err.Error(), "unsupported BSON type binary") {
		t.Fatalf("prepare JSON err=%v want unsupported binary", err)
	}
}

func TestServerUpdateBSONSetAllowsNativeBinaryValues(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}

	assertOK(t, serveCommand(t, server, 22540, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: "binary-set"},
			{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte{1}}},
		}}},
		{Key: "$db", Value: "app"},
	}))
	updateResponse := serveCommand(t, server, 22541, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "binary-set"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte{2, 3, 4}}},
			}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, updateResponse)
	assertInt32(t, updateResponse, "n", 1)
	assertInt32(t, updateResponse, "nModified", 1)

	findResponse := serveCommand(t, server, 22542, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "binary-set"}}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, findResponse)
	if len(batch) != 1 {
		t.Fatalf("batch len=%d want 1", len(batch))
	}
	subtype, payload := batch[0].Lookup("payload").Binary()
	if subtype != 0x00 || !bytes.Equal(payload, []byte{2, 3, 4}) {
		t.Fatalf("payload subtype/data=%#x/%v want 0/[2 3 4]", subtype, payload)
	}
}

func TestServerUpdateTemplateV1RefreshesMaterializerBetweenStatements(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatTemplateV1,
	}
	id := bson.NewObjectID()
	assertOK(t, serveCommand(t, server, 2254, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: id},
			{Key: "name", Value: "ada"},
		}}},
		{Key: "$db", Value: "app"},
	}))

	updateResponse := serveCommand(t, server, 2255, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
					{Key: "city", Value: "London"},
				}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
					{Key: "score", Value: int32(7)},
				}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, updateResponse)
	assertInt32(t, updateResponse, "n", 2)
	assertInt32(t, updateResponse, "nModified", 2)

	findResponse := serveCommand(t, server, 2256, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotCity, ok := firstBatch[0].Lookup("city").StringValueOK()
	if !ok || gotCity != "London" {
		t.Fatalf("firstBatch city=%q ok=%v want London", gotCity, ok)
	}
	gotScore, ok := firstBatch[0].Lookup("score").Int32OK()
	if !ok || gotScore != 7 {
		t.Fatalf("firstBatch score=%d ok=%v want 7", gotScore, ok)
	}
}

func TestServerUpdateBatchesDistinctIDs(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 2257, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}, {Key: "score", Value: int32(0)}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}, {Key: "score", Value: int32(0)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	updateResponse := serveCommand(t, server, 2258, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(2)}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, updateResponse)
	assertInt32(t, updateResponse, "n", 2)
	assertInt32(t, updateResponse, "nModified", 2)

	for i, tc := range []struct {
		id    string
		score int32
	}{
		{id: "u1", score: 1},
		{id: "u2", score: 2},
	} {
		findResponse := serveCommand(t, server, int32(2259+i), bson.D{
			{Key: "find", Value: "users"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: tc.id}}},
			{Key: "$db", Value: "app"},
		})
		firstBatch := cursorFirstBatch(t, findResponse)
		if len(firstBatch) != 1 {
			t.Fatalf("%s firstBatch len=%d want 1", tc.id, len(firstBatch))
		}
		gotScore, ok := firstBatch[0].Lookup("score").Int32OK()
		if !ok || gotScore != tc.score {
			t.Fatalf("%s score=%d ok=%v want %d", tc.id, gotScore, ok, tc.score)
		}
	}
}

func TestServerUpdateBatchTemplateV1UpdatesDistinctIDs(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatTemplateV1,
	}
	assertOK(t, serveCommand(t, server, 22591, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}, {Key: "score", Value: int32(0)}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}, {Key: "score", Value: int32(0)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	updateResponse := serveCommand(t, server, 22592, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(11)}, {Key: "city", Value: "hnl"}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(12)}, {Key: "city", Value: "sea"}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, updateResponse)
	assertInt32(t, updateResponse, "n", 2)
	assertInt32(t, updateResponse, "nModified", 2)

	for i, tc := range []struct {
		id    string
		score int32
		city  string
	}{
		{id: "u1", score: 11, city: "hnl"},
		{id: "u2", score: 12, city: "sea"},
	} {
		findResponse := serveCommand(t, server, int32(22593+i), bson.D{
			{Key: "find", Value: "users"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: tc.id}}},
			{Key: "$db", Value: "app"},
		})
		firstBatch := cursorFirstBatch(t, findResponse)
		if len(firstBatch) != 1 {
			t.Fatalf("%s firstBatch len=%d want 1", tc.id, len(firstBatch))
		}
		gotScore, ok := firstBatch[0].Lookup("score").Int32OK()
		if !ok || gotScore != tc.score {
			t.Fatalf("%s score=%d ok=%v want %d", tc.id, gotScore, ok, tc.score)
		}
		gotCity, ok := firstBatch[0].Lookup("city").StringValueOK()
		if !ok || gotCity != tc.city {
			t.Fatalf("%s city=%q ok=%v want %q", tc.id, gotCity, ok, tc.city)
		}
	}
}

func TestRunMongoUpdateBatchDeclinesFreshSecondaryUniqueIndex(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "app.users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	stale, err := mgr.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	id1, err := encodePrimaryKey(mustRawValue(t, "u1"))
	if err != nil {
		t.Fatalf("encode u1: %v", err)
	}
	id2, err := encodePrimaryKey(mustRawValue(t, "u2"))
	if err != nil {
		t.Fatalf("encode u2: %v", err)
	}
	doc1 := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}})
	doc2 := mustDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}})
	if _, err := stale.InsertBatchValidatedBSON([][]byte{id1, id2}, [][]byte{doc1, doc2}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	fresh, err := mgr.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := fresh.CreateIndex(collections.IndexDefinition{Name: "email_1", Field: "email", ValueType: collections.IndexValueString, Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	buildUpdateItem := func(index int, id string, update bson.D) mongoUpdateItem {
		t.Helper()
		item, err := parseMongoUpdateItem(index, mustDocument(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: update},
		}))
		if err != nil {
			t.Fatalf("parse update item: %v", err)
		}
		return item
	}

	matched, modified, batched, err := runMongoUpdateBatch(stale, []mongoUpdateItem{
		buildUpdateItem(0, "u1", bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "c@example.com"}}}}),
		buildUpdateItem(1, "u2", bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "a@example.com"}}}}),
	})
	if err != nil {
		t.Fatalf("runMongoUpdateBatch: %v", err)
	}
	if batched || matched != 0 || modified != 0 {
		t.Fatalf("matched=%d modified=%d batched=%v want declined", matched, modified, batched)
	}
}

func TestRunMongoUpdateBatchResultsDeclineReturnsZeroResults(t *testing.T) {
	results, batched, err := runMongoUpdateBatchResults(nil, []mongoUpdateItem{
		{index: 0, key: []byte("u1")},
		{index: 1, key: []byte("u2")},
	})
	if err != nil {
		t.Fatalf("runMongoUpdateBatchResults: %v", err)
	}
	if batched {
		t.Fatal("runMongoUpdateBatchResults batched with nil collection")
	}
	if len(results) != 2 {
		t.Fatalf("results len=%d want 2", len(results))
	}
	for i, result := range results {
		if result.Matched || result.Modified {
			t.Fatalf("result[%d]=%+v want zero value", i, result)
		}
	}
}

func TestRunMongoUpdateBatchBatchesNonUniqueFieldWithSecondaryUniqueIndex(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := collections.NewCollectionManager(db)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "app.users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
		Indexes: []collections.IndexDefinition{
			{Name: "email_1", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_1", Field: "city", ValueType: collections.IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	id1, err := encodePrimaryKey(mustRawValue(t, "u1"))
	if err != nil {
		t.Fatalf("encode u1: %v", err)
	}
	id2, err := encodePrimaryKey(mustRawValue(t, "u2"))
	if err != nil {
		t.Fatalf("encode u2: %v", err)
	}
	doc1 := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}, {Key: "city", Value: "hnl"}})
	doc2 := mustDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}, {Key: "city", Value: "hnl"}})
	if _, err := col.InsertBatchValidatedBSON([][]byte{id1, id2}, [][]byte{doc1, doc2}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := db.State()
	buildUpdateItem := func(index int, id string, update bson.D) mongoUpdateItem {
		t.Helper()
		item, err := parseMongoUpdateItem(index, mustDocument(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "u", Value: update},
		}))
		if err != nil {
			t.Fatalf("parse update item: %v", err)
		}
		return item
	}

	matched, modified, batched, err := runMongoUpdateBatch(col, []mongoUpdateItem{
		buildUpdateItem(0, "u1", bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}),
		buildUpdateItem(1, "u2", bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sfo"}}}}),
	})
	if err != nil {
		t.Fatalf("runMongoUpdateBatch: %v", err)
	}
	if !batched || matched != 2 || modified != 2 {
		t.Fatalf("matched=%d modified=%d batched=%v want 2,2,true", matched, modified, batched)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.StructuredUpdateApplications, 2; got != want {
		t.Fatalf("structured update applications=%d want %d", got, want)
	}
	if stats.Callback != 0 {
		t.Fatalf("callback duration=%s want zero for BSON set gateway batch", stats.Callback)
	}
	after := db.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered batch advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}
	ids, err := col.FindByIndex("city_1", "sea")
	if err != nil {
		t.Fatalf("find city sea: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], id1) {
		t.Fatalf("sea ids=%q want [%q]", ids, id1)
	}
	ids, err = col.FindByIndex("city_1", "hnl")
	if err != nil {
		t.Fatalf("find city hnl: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("hnl ids=%q want none after buffered updates", ids)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered mongo update batch: %v", err)
	}
	flushed := db.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flushed batch advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
	ids, err = col.FindByIndex("city_1", "sfo")
	if err != nil {
		t.Fatalf("find city sfo after flush: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], id2) {
		t.Fatalf("flushed sfo ids=%q want [%q]", ids, id2)
	}
}

func TestServerUpdateAppliesEarlierOrderedUpdatesBeforeLaterParseError(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 22594, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(0)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	updateResponse := serveCommand(t, server, 22595, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "multi", Value: true},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(2)}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "BadValue")
	errmsg, ok := bson.Raw(updateResponse).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(errmsg, "updates[1]") {
		t.Fatalf("errmsg=%q ok=%v want updates[1]", errmsg, ok)
	}

	findResponse := serveCommand(t, server, 22596, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotScore, ok := firstBatch[0].Lookup("score").Int32OK()
	if !ok || gotScore != 1 {
		t.Fatalf("u1 score=%d ok=%v want 1", gotScore, ok)
	}
}

func TestParseMongoUpdateItemUnsupportedFlagsIncludeIndex(t *testing.T) {
	tests := []struct {
		name string
		flag bson.E
		want string
	}{
		{name: "multi", flag: bson.E{Key: "multi", Value: true}, want: "updateOne only"},
		{name: "upsert", flag: bson.E{Key: "upsert", Value: true}, want: "does not support upsert"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := mustDocument(t, bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				tt.flag,
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
			})
			_, err := parseMongoUpdateItem(3, doc)
			if err == nil {
				t.Fatal("parseMongoUpdateItem accepted unsupported flag")
			}
			if !strings.Contains(err.Error(), "updates[3]") || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err=%v want index and %q", err, tt.want)
			}
		})
	}
}

func TestServerUpdateAppliesEarlierOrderedUpdatesBeforeLaterWriteError(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 22597, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 22598, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}, {Key: "city", Value: "hnl"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}, {Key: "city", Value: "hnl"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	updateResponse := serveCommand(t, server, 22599, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "a@example.com"}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "DuplicateKey")

	findResponse := serveCommand(t, server, 22600, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotCity, ok := firstBatch[0].Lookup("city").StringValueOK()
	if !ok || gotCity != "sea" {
		t.Fatalf("u1 city=%q ok=%v want sea", gotCity, ok)
	}
}

func TestServerInsertCoalescesConcurrentDistinctIDs(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	server.InsertCoalescingMaxDelay = 5 * time.Second
	server.InsertCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 22520, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	before := server.Collections.StatsSnapshot()
	start := make(chan struct{})
	responses := make(chan commandResult, 2)
	var wg sync.WaitGroup
	for i, id := range []string{"u1", "u2"} {
		i, id := i, id
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			doc, err := serveCommandResult(server, int32(22521+i), bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{
					{Key: "_id", Value: id},
					{Key: "email", Value: fmt.Sprintf("%s@example.com", id)},
				}}},
				{Key: "$db", Value: "app"},
			})
			responses <- commandResult{doc: doc, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(responses)
	for response := range responses {
		if response.err != nil {
			t.Fatalf("ServeOneWithOwner: %v", response.err)
		}
		assertOK(t, response.doc)
		assertInt32(t, response.doc, "n", 1)
	}
	after := server.Collections.StatsSnapshot()
	if got := after.IndexedStageBatches - before.IndexedStageBatches; got != 1 {
		t.Fatalf("indexed stage batches delta=%d want 1", got)
	}
	if got := after.IndexedStageDocs - before.IndexedStageDocs; got != 2 {
		t.Fatalf("indexed stage docs delta=%d want 2", got)
	}

	for i, id := range []string{"u1", "u2"} {
		findResponse := serveCommand(t, server, int32(22523+i), bson.D{
			{Key: "find", Value: "users"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
			{Key: "$db", Value: "app"},
		})
		firstBatch := cursorFirstBatch(t, findResponse)
		if len(firstBatch) != 1 {
			t.Fatalf("%s firstBatch len=%d want 1", id, len(firstBatch))
		}
	}
}

func TestMongoInsertCoalescerDuplicateIDsFallBackToOrderedSingles(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "app.users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := manager.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	id1, doc1, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(1)}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare doc1: %v", err)
	}
	id2, doc2, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(2)}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare doc2: %v", err)
	}

	done1 := make(chan mongoInsertCoalescerResult, 1)
	done2 := make(chan mongoInsertCoalescerResult, 1)
	(&mongoInsertCoalescer{}).runBatch([]mongoInsertCoalescerRequest{
		{col: col, id: id1, stored: doc1, done: done1},
		{col: col, id: id2, stored: doc2, done: done2},
	})
	first := <-done1
	second := <-done2
	if first.err != nil {
		t.Fatalf("first insert err=%v", first.err)
	}
	if !collections.IsDuplicateKeyError(second.err) {
		t.Fatalf("second insert err=%v want duplicate key", second.err)
	}
}

func TestMongoInsertCoalescerUniqueIndexErrorFallsBackToOrderedSingles(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 22530, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 22531, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u0"}, {Key: "email", Value: "a@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	duplicateID, duplicateDoc, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare duplicate doc: %v", err)
	}
	validID, validDoc, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare valid doc: %v", err)
	}

	doneDuplicate := make(chan mongoInsertCoalescerResult, 1)
	doneValid := make(chan mongoInsertCoalescerResult, 1)
	(&mongoInsertCoalescer{}).runBatch([]mongoInsertCoalescerRequest{
		{col: col, id: duplicateID, stored: duplicateDoc, done: doneDuplicate},
		{col: col, id: validID, stored: validDoc, done: doneValid},
	})
	duplicateResult := <-doneDuplicate
	validResult := <-doneValid
	if !collections.IsDuplicateKeyError(duplicateResult.err) {
		t.Fatalf("duplicate err=%v want duplicate key", duplicateResult.err)
	}
	if validResult.err != nil {
		t.Fatalf("valid err=%v", validResult.err)
	}

	findResponse := serveCommand(t, server, 22532, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("u2 firstBatch len=%d want 1", len(firstBatch))
	}
	gotEmail, ok := firstBatch[0].Lookup("email").StringValueOK()
	if !ok || gotEmail != "b@example.com" {
		t.Fatalf("u2 email=%q ok=%v want b@example.com", gotEmail, ok)
	}
}

func TestServerInsertCoalescedSkipsCoalescerForNonBSONAndMultiDocument(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatJSON,
	}
	server.InsertCoalescingMaxDelay = 5 * time.Second
	server.InsertCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 22540, bson.D{
		{Key: "insert", Value: "json_users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	}))

	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 22541, bson.D{
		{Key: "insert", Value: "bson_users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(2)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	server.insertMu.Lock()
	coalescers := len(server.insertCoalescers)
	server.insertMu.Unlock()
	if coalescers != 0 {
		t.Fatalf("created %d insert coalescers for non-BSON or multi-document insert", coalescers)
	}
}

func TestServerCloseStopsInsertCoalescers(t *testing.T) {
	server := NewServer()
	coalescer := server.mongoInsertCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	coalescer.mu.RLock()
	stopped := coalescer.stopped
	coalescer.mu.RUnlock()
	if !stopped {
		t.Fatal("coalescer was not stopped")
	}
	select {
	case <-coalescer.done:
	default:
		t.Fatal("Close returned before insert coalescer worker exited")
	}
	if got := server.mongoInsertCoalescer("app.users"); got != nil {
		t.Fatal("closed server created a new insert coalescer")
	}
}

func TestMongoInsertCoalescerClampsConfiguredMaxBatch(t *testing.T) {
	server := NewServer()
	server.InsertCoalescingMaxBatch = maxInsertCoalescingBatch + 1
	coalescer := server.mongoInsertCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	defer func() { _ = server.Close() }()
	if coalescer.maxBatch != maxInsertCoalescingBatch {
		t.Fatalf("maxBatch=%d want %d", coalescer.maxBatch, maxInsertCoalescingBatch)
	}
	if got, want := cap(coalescer.requests), maxInsertCoalescingBatch*4; got != want {
		t.Fatalf("request queue cap=%d want %d", got, want)
	}
}

func TestServerUpdateCoalescesConcurrentDistinctIDs(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	server.UpdateCoalescingMaxDelay = 5 * time.Second
	server.UpdateCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 2260, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(0)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush setup insert: %v", err)
	}

	before := db.State()
	start := make(chan struct{})
	responses := make(chan commandResult, 2)
	var wg sync.WaitGroup
	for i, id := range []string{"u1", "u2"} {
		i, id := i, id
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			doc, err := serveCommandResult(server, int32(2261+i), bson.D{
				{Key: "update", Value: "users"},
				{Key: "updates", Value: bson.A{bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: id}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(10 + i)}}}}},
				}}},
				{Key: "$db", Value: "app"},
			})
			responses <- commandResult{doc: doc, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(responses)
	for response := range responses {
		if response.err != nil {
			t.Fatalf("ServeOneWithOwner: %v", response.err)
		}
		assertOK(t, response.doc)
		assertInt32(t, response.doc, "n", 1)
		assertInt32(t, response.doc, "nModified", 1)
	}
	after := db.State()
	if after.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("coalesced updates advanced commit seq by %d, want 1 (synchronous publish in WAL-on mode)", after.CommitSeq-before.CommitSeq)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush coalesced updates: %v", err)
	}
	flushed := db.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush should be idempotent after synchronous publish, commit seq delta=%d want 1", flushed.CommitSeq-before.CommitSeq)
	}
}

func TestServerUpdateCoalescedBatchIsolatesItemErrors(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	server.UpdateCoalescingMaxDelay = 5 * time.Second
	server.UpdateCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 2262, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(0)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	type updateResponse struct {
		name string
		doc  wire.Document
		err  error
	}
	start := make(chan struct{})
	responses := make(chan updateResponse, 2)
	var wg sync.WaitGroup
	for i, tc := range []struct {
		name string
		id   string
		set  bson.D
	}{
		{name: "invalid", id: "u1", set: bson.D{{Key: "_id", Value: "moved"}}},
		{name: "valid", id: "u2", set: bson.D{{Key: "score", Value: int32(12)}}},
	} {
		i, tc := i, tc
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			doc, err := serveCommandResult(server, int32(2263+i), bson.D{
				{Key: "update", Value: "users"},
				{Key: "updates", Value: bson.A{bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: tc.id}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: tc.set}}},
				}}},
				{Key: "$db", Value: "app"},
			})
			responses <- updateResponse{name: tc.name, doc: doc, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(responses)

	got := make(map[string]wire.Document, 2)
	for response := range responses {
		if response.err != nil {
			t.Fatalf("%s ServeOneWithOwner: %v", response.name, response.err)
		}
		got[response.name] = response.doc
	}
	assertCommandError(t, got["invalid"], "BadValue")
	assertOK(t, got["valid"])
	assertInt32(t, got["valid"], "n", 1)
	assertInt32(t, got["valid"], "nModified", 1)

	findResponse := serveCommand(t, server, 2265, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotScore, ok := firstBatch[0].Lookup("score").Int32OK()
	if !ok || gotScore != 12 {
		t.Fatalf("u2 score=%d ok=%v want 12", gotScore, ok)
	}
}

func TestMongoUpdateCoalescerUniqueIndexFallsBackToOrderedSingles(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 2266, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 2267, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	acquire, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "a@example.com"}}}}},
	}))
	if err != nil {
		t.Fatalf("parse acquire: %v", err)
	}
	release, err := parseMongoUpdateItem(1, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "c@example.com"}}}}},
	}))
	if err != nil {
		t.Fatalf("parse release: %v", err)
	}

	doneAcquire := make(chan mongoUpdateCoalescerResult, 1)
	doneRelease := make(chan mongoUpdateCoalescerResult, 1)
	(&mongoUpdateCoalescer{}).runBatch([]mongoUpdateCoalescerRequest{
		{col: col, item: acquire, done: doneAcquire},
		{col: col, item: release, done: doneRelease},
	})
	acquireResult := <-doneAcquire
	releaseResult := <-doneRelease
	if !collections.IsDuplicateKeyError(acquireResult.err) {
		t.Fatalf("acquire err=%v want duplicate key", acquireResult.err)
	}
	if releaseResult.err != nil {
		t.Fatalf("release err=%v", releaseResult.err)
	}
	if !releaseResult.matched || !releaseResult.modified {
		t.Fatalf("release matched=%v modified=%v want true,true", releaseResult.matched, releaseResult.modified)
	}
}

func TestServerUpdateCoalescedSkipsCoalescerForSecondaryUniqueIndex(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.UpdateCoalescingMaxDelay = 5 * time.Second
	server.UpdateCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 2270, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 2271, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	update, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "b@example.com"}}}}},
	}))
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	matched, modified, err := server.runMongoUpdateCoalesced("app.users", col, update)
	if err != nil {
		t.Fatalf("runMongoUpdateCoalesced: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true,true", matched, modified)
	}
	server.updateMu.Lock()
	_, cached := server.updateCoalescers["app.users"]
	server.updateMu.Unlock()
	if cached {
		t.Fatal("secondary unique update created a coalescer")
	}
}

func TestServerUpdateCoalescedSkipsCoalescerForUnrecognizedUpdateShape(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.UpdateCoalescingMaxDelay = 5 * time.Second
	server.UpdateCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 2280, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(1)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	update, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
	}))
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	if update.setFieldsOK {
		t.Fatal("test update unexpectedly parsed as $set")
	}
	matched, modified, err := server.runMongoUpdateCoalesced("app.users", col, update)
	if err == nil {
		t.Fatal("runMongoUpdateCoalesced succeeded for unsupported $inc update")
	}
	if matched || modified {
		t.Fatalf("matched=%v modified=%v want false,false", matched, modified)
	}
	server.updateMu.Lock()
	_, cached := server.updateCoalescers["app.users"]
	server.updateMu.Unlock()
	if cached {
		t.Fatal("unrecognized update shape created a coalescer")
	}
}

func TestCollectionUpdateBatchErrorIndexUsesTypedError(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &collections.UpdateBatchItemError{
		Index: 3,
		Err:   errors.New("bad replacement"),
	})
	index, ok := collectionUpdateBatchErrorIndex(err)
	if !ok || index != 3 {
		t.Fatalf("index=%d ok=%v want 3,true", index, ok)
	}
}

func TestCollectionUpdateBatchErrorForRequestUsesCommandIndex(t *testing.T) {
	err := collectionUpdateBatchErrorForRequest(&collections.UpdateBatchItemError{
		Index: 2,
		Err:   errors.New("bad replacement"),
	}, 0)
	if err == nil || strings.Contains(err.Error(), "update batch index") {
		t.Fatalf("request err=%v should not expose coalesced batch index", err)
	}
	if !strings.Contains(err.Error(), "updates[0]") || !strings.Contains(err.Error(), "bad replacement") {
		t.Fatalf("request err=%v want command update index", err)
	}
}

func TestMongoUpdateCoalescerUsesSingleCollection(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "app.users"}); err != nil {
		t.Fatalf("create users: %v", err)
	}
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "app.posts"}); err != nil {
		t.Fatalf("create posts: %v", err)
	}
	usersA, err := manager.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open users A: %v", err)
	}
	usersB, err := manager.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open users B: %v", err)
	}
	posts, err := manager.OpenCollection("app.posts")
	if err != nil {
		t.Fatalf("open posts: %v", err)
	}
	if !mongoUpdateCoalescerUsesSingleCollection([]mongoUpdateCoalescerRequest{{col: usersA}, {col: usersB}}) {
		t.Fatal("same collection reported as mixed")
	}
	if mongoUpdateCoalescerUsesSingleCollection([]mongoUpdateCoalescerRequest{{col: usersA}, {col: posts}}) {
		t.Fatal("mixed collections reported as single collection")
	}
	schemaCol, err := manager.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open users for schema change: %v", err)
	}
	if _, err := schemaCol.CreateIndex(collections.IndexDefinition{Name: "email", Field: "email", ValueType: collections.IndexValueString}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	usersAfterSchemaChange, err := manager.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open users after schema change: %v", err)
	}
	if mongoUpdateCoalescerUsesSingleCollection([]mongoUpdateCoalescerRequest{{col: usersA}, {col: usersAfterSchemaChange}}) {
		t.Fatal("different collection catalog states reported as single collection")
	}
}

func TestServerCloseStopsUpdateCoalescers(t *testing.T) {
	server := NewServer()
	coalescer := server.mongoUpdateCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	coalescer.mu.RLock()
	stopped := coalescer.stopped
	coalescer.mu.RUnlock()
	if !stopped {
		t.Fatal("coalescer was not stopped")
	}
	select {
	case <-coalescer.done:
	default:
		t.Fatal("Close returned before coalescer worker exited")
	}
	if got := server.mongoUpdateCoalescer("app.users"); got != nil {
		t.Fatal("closed server created a new coalescer")
	}
	if err := server.ServeOne(&readWriter{r: bytes.NewReader(nil)}); !errors.Is(err, errServerClosed) {
		t.Fatalf("ServeOne after Close err=%v want %v", err, errServerClosed)
	}
	if _, _, err := server.openCursor("app.users", []wire.Document{mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})}, compiledProjection{}, 1, true, defaultCursorBatchSize, 1); !errors.Is(err, errServerClosed) {
		t.Fatalf("openCursor after Close err=%v want %v", err, errServerClosed)
	}
}

func TestMongoUpdateCoalescerRejectsEnqueueAfterStop(t *testing.T) {
	server := NewServer()
	coalescer := server.mongoUpdateCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	coalescer.stop()
	if coalescer.enqueue(mongoUpdateCoalescerRequest{done: make(chan mongoUpdateCoalescerResult, 1)}) {
		t.Fatal("stopped coalescer accepted enqueue")
	}
}

func TestMongoUpdateCoalescerCloseDoesNotBlockBehindFullQueue(t *testing.T) {
	coalescer := &mongoUpdateCoalescer{
		requests:  make(chan mongoUpdateCoalescerRequest, 1),
		stoppedCh: make(chan struct{}),
	}
	coalescer.requests <- mongoUpdateCoalescerRequest{done: make(chan mongoUpdateCoalescerResult, 1)}
	enqueueDone := make(chan bool, 1)
	go func() {
		enqueueDone <- coalescer.enqueue(mongoUpdateCoalescerRequest{done: make(chan mongoUpdateCoalescerResult, 1)})
	}()

	time.Sleep(10 * time.Millisecond)
	closed := make(chan bool, 1)
	go func() {
		closed <- coalescer.closeRequests()
	}()
	select {
	case ok := <-closed:
		if !ok {
			t.Fatal("closeRequests reported false")
		}
	case <-time.After(time.Second):
		t.Fatal("closeRequests blocked behind a full enqueue")
	}
	select {
	case ok := <-enqueueDone:
		if ok {
			t.Fatal("enqueue succeeded after close")
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue did not observe close")
	}
}

func TestServerUpdateCoalescedFallsBackWhenCoalescerStopsBeforeEnqueue(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	server.UpdateCoalescingMaxDelay = 5 * time.Second
	server.UpdateCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 2280, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	coalescer := server.mongoUpdateCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	coalescer.stop()

	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	update, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(7)}}}}},
	}))
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}

	matched, modified, err := server.runMongoUpdateCoalesced("app.users", col, update)
	if err != nil {
		t.Fatalf("runMongoUpdateCoalesced: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true,true", matched, modified)
	}

	findResponse := serveCommand(t, server, 2281, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	gotScore, ok := firstBatch[0].Lookup("score").Int32OK()
	if !ok || gotScore != 7 {
		t.Fatalf("u1 score=%d ok=%v want 7", gotScore, ok)
	}
}

func TestMongoUpdateCoalescerWaitReturnsWhenWorkerStops(t *testing.T) {
	coalescer := &mongoUpdateCoalescer{done: make(chan struct{})}
	done := make(chan mongoUpdateCoalescerResult, 1)
	close(coalescer.done)
	result := coalescer.waitForUpdateResult(done)
	if result.err == nil || !strings.Contains(result.err.Error(), "stopped before completing request") {
		t.Fatalf("result err=%v want stopped error", result.err)
	}
}

func TestMongoUpdateCoalescerClampsConfiguredMaxBatch(t *testing.T) {
	server := NewServer()
	server.UpdateCoalescingMaxBatch = maxUpdateCoalescingBatch + 1
	coalescer := server.mongoUpdateCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	defer func() { _ = server.Close() }()
	if coalescer.maxBatch != maxUpdateCoalescingBatch {
		t.Fatalf("maxBatch=%d want %d", coalescer.maxBatch, maxUpdateCoalescingBatch)
	}
	if got, want := cap(coalescer.requests), maxUpdateCoalescingBatch*4; got != want {
		t.Fatalf("request queue cap=%d want %d", got, want)
	}
}

func TestServerUpdateCoalescerEvictsWhenIdle(t *testing.T) {
	server := NewServer()
	server.UpdateCoalescingIdleTTL = time.Millisecond
	coalescer := server.mongoUpdateCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(time.Second)
	for {
		server.updateMu.Lock()
		_, stillCached := server.updateCoalescers["app.users"]
		server.updateMu.Unlock()
		coalescer.mu.RLock()
		stopped := coalescer.stopped
		coalescer.mu.RUnlock()
		if !stillCached && stopped {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("coalescer was not evicted after idle timeout")
		}
		<-ticker.C
	}
}

func TestMongoUpdateCoalescerRetireIdleStopsBeforeUncache(t *testing.T) {
	server := NewServer()
	coalescer := server.mongoUpdateCoalescer("app.users")
	if coalescer == nil {
		t.Fatal("expected coalescer")
	}
	coalescer.enqueueMu.Lock()
	retired := make(chan bool, 1)
	go func() {
		retired <- coalescer.retireIdle()
	}()

	deadline := time.Now().Add(time.Second)
	for {
		if !server.updateMu.TryLock() {
			break
		}
		server.updateMu.Unlock()
		if time.Now().After(deadline) {
			coalescer.enqueueMu.Unlock()
			t.Fatal("retireIdle did not hold server update lock while stopping")
		}
		time.Sleep(time.Millisecond)
	}

	replacement := make(chan *mongoUpdateCoalescer, 1)
	go func() {
		replacement <- server.mongoUpdateCoalescer("app.users")
	}()
	select {
	case got := <-replacement:
		coalescer.enqueueMu.Unlock()
		if got != coalescer {
			t.Fatal("created replacement coalescer before old coalescer stopped")
		}
		t.Fatal("returned old coalescer while idle retirement was stopping it")
	case <-time.After(25 * time.Millisecond):
	}

	coalescer.enqueueMu.Unlock()
	select {
	case ok := <-retired:
		if !ok {
			t.Fatal("retireIdle reported false")
		}
	case <-time.After(time.Second):
		t.Fatal("retireIdle did not finish")
	}
	select {
	case got := <-replacement:
		if got == nil || got == coalescer {
			t.Fatalf("replacement=%p old=%p want new coalescer", got, coalescer)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement coalescer was not created after idle retirement")
	}
	_ = server.Close()
}

func TestServerUpdateWithUniqueIndexKeepsAcquireBeforeReleaseOrdered(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 22601, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 22602, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	updateResponse := serveCommand(t, server, 22603, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "a@example.com"}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "c@example.com"}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, updateResponse, "DuplicateKey")

	for i, tc := range []struct {
		id    string
		email string
	}{
		{id: "u1", email: "a@example.com"},
		{id: "u2", email: "b@example.com"},
	} {
		findResponse := serveCommand(t, server, int32(22604+i), bson.D{
			{Key: "find", Value: "users"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: tc.id}}},
			{Key: "$db", Value: "app"},
		})
		firstBatch := cursorFirstBatch(t, findResponse)
		if len(firstBatch) != 1 {
			t.Fatalf("%s firstBatch len=%d want 1", tc.id, len(firstBatch))
		}
		gotEmail, ok := firstBatch[0].Lookup("email").StringValueOK()
		if !ok || gotEmail != tc.email {
			t.Fatalf("%s email=%q ok=%v want %q", tc.id, gotEmail, ok, tc.email)
		}
	}
}

func TestMongoUpdateErrorWithIndexAddsMissingContext(t *testing.T) {
	err := mongoUpdateErrorWithIndex(3, errors.New("write failed"))
	if err == nil || !strings.Contains(err.Error(), "updates[3]: write failed") {
		t.Fatalf("err=%v want indexed context", err)
	}
	alreadyIndexed := errors.New("updates[3]: bad bson")
	if got := mongoUpdateErrorWithIndex(3, alreadyIndexed); got != alreadyIndexed {
		t.Fatalf("already indexed err changed: %v", got)
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
	errmsg, ok := bson.Raw(updateResponse).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(errmsg, "updates[0]") {
		t.Fatalf("errmsg=%q ok=%v want updates[0]", errmsg, ok)
	}
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
			{Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"},
			{Key: "unique", Value: true},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, createResponse)
	assertInt32(t, createResponse, "numIndexesBefore", 1)
	assertInt32(t, createResponse, "numIndexesAfter", 2)

	idempotentResponse := serveCommand(t, server, 2271, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"},
			{Key: "unique", Value: true},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, idempotentResponse)
	assertInt32(t, idempotentResponse, "numIndexesBefore", 2)
	assertInt32(t, idempotentResponse, "numIndexesAfter", 2)

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
	valueType, valueTypeOK := indexBatch[1].Lookup("treedbValueType").StringValueOK()
	if !valueTypeOK || valueType != "string" {
		t.Fatalf("listIndexes treedbValueType=%q ok=%v want string", valueType, valueTypeOK)
	}
	assertBool(t, wire.Document(indexBatch[1]), "unique", true)

	replayResponse := serveCommand(t, server, 2291, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{indexBatch[1]}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, replayResponse)
	assertInt32(t, replayResponse, "numIndexesBefore", 2)
	assertInt32(t, replayResponse, "numIndexesAfter", 2)

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

func TestServerVectorIndexMetadataExtension(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	vectorIndex := bson.D{
		{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
		{Key: "name", Value: "embedding_vector"},
		{Key: "treedbIndexType", Value: "vector"},
		{Key: "treedbVector", Value: bson.D{
			{Key: "dimensions", Value: int32(64)},
			{Key: "metric", Value: "cosine"},
			{Key: "m", Value: int32(16)},
			{Key: "efConstruction", Value: int32(128)},
			{Key: "efSearch", Value: int32(64)},
			{Key: "encoding", Value: "float32"},
		}},
	}
	createResponse := serveCommand(t, server, 23101, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
				{Key: "name", Value: "email_1"},
				{Key: "treedbValueType", Value: "string"},
			},
			vectorIndex,
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, createResponse)
	assertInt32(t, createResponse, "numIndexesBefore", 1)
	assertInt32(t, createResponse, "numIndexesAfter", 3)

	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if got := len(col.Meta().VectorIndexes); got != 1 {
		t.Fatalf("vector indexes=%d want 1", got)
	}
	stored := col.Meta().VectorIndexes[0]
	if stored.Name != "embedding_vector" || stored.Field != "embedding" || stored.Dimensions != 64 || stored.Metric != collections.VectorMetricCosine || stored.Encoding != collections.VectorIndexEncodingFloat32 {
		t.Fatalf("stored vector index=%+v", stored)
	}

	indexesResponse := serveCommand(t, server, 23102, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	indexBatch := cursorFirstBatch(t, indexesResponse)
	if got, want := len(indexBatch), 3; got != want {
		t.Fatalf("index batch len=%d want %d", got, want)
	}
	assertIndexName(t, indexBatch[0], "_id_")
	assertIndexName(t, indexBatch[1], "email_1")
	assertIndexName(t, indexBatch[2], "embedding_vector")
	keyDoc, ok := indexBatch[2].Lookup("key").DocumentOK()
	if !ok {
		t.Fatalf("vector index key missing: %v", indexBatch[2])
	}
	if got, ok := keyDoc.Lookup("embedding").StringValueOK(); !ok || got != "vector" {
		t.Fatalf("vector key embedding=%q ok=%v want vector", got, ok)
	}
	if got, ok := indexBatch[2].Lookup("treedbIndexType").StringValueOK(); !ok || got != "vector" {
		t.Fatalf("treedbIndexType=%q ok=%v want vector", got, ok)
	}
	options, ok := indexBatch[2].Lookup("treedbVector").DocumentOK()
	if !ok {
		t.Fatalf("treedbVector missing: %v", indexBatch[2])
	}
	if got, ok := options.Lookup("dimensions").Int32OK(); !ok || got != 64 {
		t.Fatalf("dimensions=%d ok=%v want 64", got, ok)
	}
	if got, ok := options.Lookup("metric").StringValueOK(); !ok || got != "cosine" {
		t.Fatalf("metric=%q ok=%v want cosine", got, ok)
	}
	if got, ok := options.Lookup("encoding").StringValueOK(); !ok || got != "float32" {
		t.Fatalf("encoding=%q ok=%v want float32", got, ok)
	}

	doubleOptionsResponse := serveCommand(t, server, 231021, bson.D{
		{Key: "createIndexes", Value: "users_double_vector_options"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{
				{Key: "dimensions", Value: float64(64)},
				{Key: "m", Value: float64(16)},
				{Key: "efConstruction", Value: float64(128)},
				{Key: "efSearch", Value: float64(64)},
			}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, doubleOptionsResponse)
	doubleOptionsCol, err := server.Collections.OpenCollection("app.users_double_vector_options")
	if err != nil {
		t.Fatalf("open double-options collection: %v", err)
	}
	if got := doubleOptionsCol.Meta().VectorIndexes[0]; got.Dimensions != 64 || got.M != 16 || got.EfConstruction != 128 || got.EfSearch != 64 {
		t.Fatalf("double vector options stored=%+v", got)
	}

	replayResponse := serveCommand(t, server, 23103, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{indexBatch[2]}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, replayResponse)
	assertInt32(t, replayResponse, "numIndexesBefore", 3)
	assertInt32(t, replayResponse, "numIndexesAfter", 3)

	assertOK(t, serveCommand(t, server, 23104, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "ada@example.com"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "grace@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 23105, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "email", Value: "ada@example.com"}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, findResponse), []string{"u1"})

	dropResponse := serveCommand(t, server, 23106, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: "embedding_vector"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, dropResponse)
	assertInt32(t, dropResponse, "nIndexesWas", 3)
	afterDrop := cursorFirstBatch(t, serveCommand(t, server, 23107, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	}))
	if got, want := len(afterDrop), 2; got != want {
		t.Fatalf("index batch after vector drop len=%d want %d", got, want)
	}
	assertIndexName(t, afterDrop[0], "_id_")
	assertIndexName(t, afterDrop[1], "email_1")

	assertOK(t, serveCommand(t, server, 23108, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{vectorIndex}},
		{Key: "$db", Value: "app"},
	}))
	duplicateVectorDropResponse := serveCommand(t, server, 23109, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: bson.A{"embedding_vector", "embedding_vector"}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, duplicateVectorDropResponse)
	assertInt32(t, duplicateVectorDropResponse, "nIndexesWas", 3)
	afterDuplicateVectorDrop := cursorFirstBatch(t, serveCommand(t, server, 23110, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	}))
	if got, want := len(afterDuplicateVectorDrop), 2; got != want {
		t.Fatalf("index batch after duplicate vector drop len=%d want %d", got, want)
	}
	assertOK(t, serveCommand(t, server, 23111, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{vectorIndex}},
		{Key: "$db", Value: "app"},
	}))
	dropAllResponse := serveCommand(t, server, 23112, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: "*"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, dropAllResponse)
	assertInt32(t, dropAllResponse, "nIndexesWas", 3)
	afterDropAll := cursorFirstBatch(t, serveCommand(t, server, 23113, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	}))
	if got, want := len(afterDropAll), 1; got != want {
		t.Fatalf("index batch after drop all len=%d want %d", got, want)
	}
	assertIndexName(t, afterDropAll[0], "_id_")
}

func TestServerCreateIndexesConflictingExistingVectorDoesNotCreateScalar(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	vectorIndex := bson.D{
		{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
		{Key: "name", Value: "embedding_vector"},
		{Key: "treedbIndexType", Value: "vector"},
		{Key: "treedbVector", Value: bson.D{
			{Key: "dimensions", Value: int32(64)},
			{Key: "metric", Value: "cosine"},
		}},
	}
	assertOK(t, serveCommand(t, server, 23114, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{vectorIndex}},
		{Key: "$db", Value: "app"},
	}))

	conflictingVector := bson.D{
		{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
		{Key: "name", Value: "embedding_vector"},
		{Key: "treedbIndexType", Value: "vector"},
		{Key: "treedbVector", Value: bson.D{
			{Key: "dimensions", Value: int32(32)},
			{Key: "metric", Value: "cosine"},
		}},
	}
	response := serveCommand(t, server, 23115, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
				{Key: "name", Value: "city_1"},
				{Key: "treedbValueType", Value: "string"},
			},
			conflictingVector,
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")

	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, ok := findIndexDefinition(col.Meta().Indexes, "city_1"); ok {
		t.Fatalf("conflicting vector request created scalar index: %+v", col.Meta().Indexes)
	}
	stored, ok := findVectorIndexDefinition(col.Meta().VectorIndexes, "embedding_vector")
	if !ok || stored.Dimensions != 64 {
		t.Fatalf("vector index changed after conflicting request: %+v ok=%v", stored, ok)
	}
}

func TestServerCreateCollectionCommand(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}

	createResponse := serveCommand(t, server, 2311, bson.D{
		{Key: "create", Value: "created"},
		{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
		{Key: "$clusterTime", Value: bson.D{}},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "primaryPreferred"}}},
		{Key: "apiVersion", Value: "1"},
		{Key: "maxTimeMS", Value: int64(1000)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, createResponse)

	col, err := server.Collections.OpenCollection("app.created")
	if err != nil {
		t.Fatalf("open created collection: %v", err)
	}
	if col.Meta().Options.DocumentFormat != collections.DocumentFormatBSON {
		t.Fatalf("document format=%q want bson", col.Meta().Options.DocumentFormat)
	}

	collectionsResponse := serveCommand(t, server, 2312, bson.D{
		{Key: "listCollections", Value: int32(1)},
		{Key: "filter", Value: bson.D{{Key: "name", Value: "created"}}},
		{Key: "nameOnly", Value: true},
		{Key: "$db", Value: "app"},
	})
	collectionBatch := cursorFirstBatch(t, collectionsResponse)
	if len(collectionBatch) != 1 {
		t.Fatalf("collection batch len=%d want 1", len(collectionBatch))
	}
	if got, ok := collectionBatch[0].Lookup("name").StringValueOK(); !ok || got != "created" {
		t.Fatalf("collection name=%q ok=%v want created", got, ok)
	}

	indexesResponse := serveCommand(t, server, 2313, bson.D{
		{Key: "listIndexes", Value: "created"},
		{Key: "$db", Value: "app"},
	})
	indexBatch := cursorFirstBatch(t, indexesResponse)
	if got, want := len(indexBatch), 1; got != want {
		t.Fatalf("index batch len=%d want %d", got, want)
	}
	assertIndexName(t, indexBatch[0], "_id_")

	duplicateResponse := serveCommand(t, server, 2314, bson.D{
		{Key: "create", Value: "created"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, duplicateResponse)
	if note, ok := duplicateResponse.Lookup("note").StringValueOK(); !ok || !strings.Contains(note, "idempotent") {
		t.Fatalf("duplicate create note=%q ok=%v want idempotent note", note, ok)
	}

	cappedFalseResponse := serveCommand(t, server, 2315, bson.D{
		{Key: "create", Value: "plain"},
		{Key: "capped", Value: false},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, cappedFalseResponse)

	cappedTrueResponse := serveCommand(t, server, 2316, bson.D{
		{Key: "create", Value: "capped"},
		{Key: "capped", Value: true},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, cappedTrueResponse, "BadValue")

	validatorResponse := serveCommand(t, server, 2317, bson.D{
		{Key: "create", Value: "validated"},
		{Key: "validator", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, validatorResponse, "BadValue")
}

func TestServerRejectsTransactionalMutations(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 2318, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}}}},
		{Key: "$db", Value: "app"},
	}))
	transactionFields := bson.D{
		{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
		{Key: "txnNumber", Value: int64(1)},
		{Key: "startTransaction", Value: true},
		{Key: "autocommit", Value: false},
	}
	retryableWriteFields := bson.D{
		{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
		{Key: "txnNumber", Value: int64(2)},
	}

	assertOK(t, serveCommand(t, server, 2319, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}}}},
		{Key: "$db", Value: "app"},
	}))
	transactionalCreateIndexes := append(bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}}, {Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"}}}},
	}, transactionFields...)
	transactionalCreateIndexes = append(transactionalCreateIndexes, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2320, transactionalCreateIndexes), "BadValue")
	indexes := cursorFirstBatch(t, serveCommand(t, server, 2321, bson.D{{Key: "listIndexes", Value: "users"}, {Key: "$db", Value: "app"}}))
	if len(indexes) != 2 {
		t.Fatalf("indexes after rejected transactional createIndexes len=%d want 2", len(indexes))
	}
	assertIndexName(t, indexes[0], "_id_")
	assertIndexName(t, indexes[1], "email_1")
	transactionalDropIndexes := append(bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: "email_1"},
	}, transactionFields...)
	transactionalDropIndexes = append(transactionalDropIndexes, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2322, transactionalDropIndexes), "BadValue")
	indexes = cursorFirstBatch(t, serveCommand(t, server, 2323, bson.D{{Key: "listIndexes", Value: "users"}, {Key: "$db", Value: "app"}}))
	if len(indexes) != 2 {
		t.Fatalf("indexes after rejected transactional dropIndexes len=%d want 2", len(indexes))
	}
	assertIndexName(t, indexes[0], "_id_")
	assertIndexName(t, indexes[1], "email_1")

	transactionalFind := append(bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
	}, transactionFields...)
	transactionalFind = append(transactionalFind, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2330, transactionalFind), "BadValue")
	retryableFind := append(bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
	}, retryableWriteFields...)
	retryableFind = append(retryableFind, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2331, retryableFind), "BadValue")
	transactionalListCollections := append(bson.D{
		{Key: "listCollections", Value: int32(1)},
	}, transactionFields...)
	transactionalListCollections = append(transactionalListCollections, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2332, transactionalListCollections), "BadValue")
	transactionalListIndexes := append(bson.D{
		{Key: "listIndexes", Value: "users"},
	}, transactionFields...)
	transactionalListIndexes = append(transactionalListIndexes, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2333, transactionalListIndexes), "BadValue")

	transactionalInsert := append(bson.D{
		{Key: "insert", Value: "tx_insert"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u2"}}}},
	}, transactionFields...)
	transactionalInsert = append(transactionalInsert, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2324, transactionalInsert), "BadValue")
	if _, err := server.Collections.OpenCollection("app.tx_insert"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("transactional insert collection err=%v, want collection not found", err)
	}

	retryableInsert := append(bson.D{
		{Key: "insert", Value: "retryable_insert"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u3"}}}},
	}, retryableWriteFields...)
	retryableInsert = append(retryableInsert, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2329, retryableInsert), "BadValue")
	if _, err := server.Collections.OpenCollection("app.retryable_insert"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("retryable insert collection err=%v, want collection not found", err)
	}

	transactionalUpdate := append(bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "changed"}}}}},
		}}},
	}, transactionFields...)
	transactionalUpdate = append(transactionalUpdate, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2325, transactionalUpdate), "BadValue")
	found := serveCommand(t, server, 2326, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, found)
	if got, ok := batch[0].Lookup("name").StringValueOK(); !ok || got != "ada" {
		t.Fatalf("name after rejected transactional update=%q ok=%v want ada", got, ok)
	}

	transactionalDelete := append(bson.D{
		{Key: "delete", Value: "users"},
		{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
	}, transactionFields...)
	transactionalDelete = append(transactionalDelete, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveCommand(t, server, 2327, transactionalDelete), "BadValue")
	found = serveCommand(t, server, 2328, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, found), []string{"u1"})
}

func TestServerCreateIndexesAutoCreateDedupesIdenticalDefinitions(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	createResponse := serveCommand(t, server, 2272, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
				{Key: "name", Value: "email_1"},
				{Key: "treedbValueType", Value: "string"},
				{Key: "unique", Value: true},
			},
			bson.D{
				{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
				{Key: "name", Value: "email_1"},
				{Key: "treedbValueType", Value: "string"},
				{Key: "unique", Value: true},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, createResponse)
	assertBool(t, createResponse, "createdCollectionAutomatically", true)
	assertInt32(t, createResponse, "numIndexesBefore", 1)
	assertInt32(t, createResponse, "numIndexesAfter", 2)

	indexesResponse := serveCommand(t, server, 2273, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	indexBatch := cursorFirstBatch(t, indexesResponse)
	if got, want := len(indexBatch), 2; got != want {
		t.Fatalf("index batch len=%d want %d", got, want)
	}
	assertIndexName(t, indexBatch[0], "_id_")
	assertIndexName(t, indexBatch[1], "email_1")
}

func TestServerIndexMetadataRejectsInvalidCommands(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	emptyCreate := serveCommand(t, server, 232, bson.D{
		{Key: "createIndexes", Value: "empty"},
		{Key: "indexes", Value: bson.A{}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, emptyCreate, "BadValue")

	emptyName := serveCommand(t, server, 233, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: ""},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, emptyName, "BadValue")

	missingValueType := serveCommand(t, server, 2331, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, missingValueType, "BadValue")
	errmsg, ok := bson.Raw(missingValueType).Lookup("errmsg").StringValueOK()
	for _, want := range []string{"treedbValueType", "email_1", "email"} {
		if !ok || !strings.Contains(errmsg, want) {
			t.Fatalf("missing value type errmsg=%q ok=%v want %q", errmsg, ok, want)
		}
	}

	unsupportedValueType := serveCommand(t, server, 2332, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
			{Key: "treedbValueType", Value: "decimal"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, unsupportedValueType, "BadValue")
	errmsg, ok = bson.Raw(unsupportedValueType).Lookup("errmsg").StringValueOK()
	for _, want := range []string{"email_1", "email", "decimal", "string", "bool", "int64", "double"} {
		if !ok || !strings.Contains(errmsg, want) {
			t.Fatalf("unsupported value type errmsg=%q ok=%v want %q", errmsg, ok, want)
		}
	}

	missingVectorType := serveCommand(t, server, 23321, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(64)}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, missingVectorType, "BadValue")
	errmsg, ok = bson.Raw(missingVectorType).Lookup("errmsg").StringValueOK()
	for _, want := range []string{"treedbIndexType", "vector", "embedding"} {
		if !ok || !strings.Contains(errmsg, want) {
			t.Fatalf("missing vector type errmsg=%q ok=%v want %q", errmsg, ok, want)
		}
	}

	missingVectorOptions := serveCommand(t, server, 23322, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, missingVectorOptions, "BadValue")
	errmsg, ok = bson.Raw(missingVectorOptions).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(errmsg, "treedbVector") {
		t.Fatalf("missing vector options errmsg=%q ok=%v want treedbVector", errmsg, ok)
	}

	fractionalVectorOption := serveCommand(t, server, 233221, bson.D{
		{Key: "createIndexes", Value: "users_fractional_vector_option"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: float64(64.5)}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, fractionalVectorOption, "BadValue")
	errmsg, ok = bson.Raw(fractionalVectorOption).Lookup("errmsg").StringValueOK()
	for _, want := range []string{"dimensions", "integer"} {
		if !ok || !strings.Contains(errmsg, want) {
			t.Fatalf("fractional vector option errmsg=%q ok=%v want %q", errmsg, ok, want)
		}
	}

	unsupportedVectorMetric := serveCommand(t, server, 23323, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{
				{Key: "dimensions", Value: int32(64)},
				{Key: "metric", Value: "angular"},
			}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, unsupportedVectorMetric, "BadValue")
	errmsg, ok = bson.Raw(unsupportedVectorMetric).Lookup("errmsg").StringValueOK()
	for _, want := range []string{"metric", "angular", "cosine", "l2", "inner_product"} {
		if !ok || !strings.Contains(errmsg, want) {
			t.Fatalf("unsupported vector metric errmsg=%q ok=%v want %q", errmsg, ok, want)
		}
	}

	emptyVectorMetric := serveCommand(t, server, 23325, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{
				{Key: "dimensions", Value: int32(64)},
				{Key: "metric", Value: ""},
			}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, emptyVectorMetric, "BadValue")

	emptyVectorEncoding := serveCommand(t, server, 23326, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{
				{Key: "dimensions", Value: int32(64)},
				{Key: "encoding", Value: ""},
			}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, emptyVectorEncoding, "BadValue")

	efConstructionBelowM := serveCommand(t, server, 23327, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{
				{Key: "dimensions", Value: int32(64)},
				{Key: "m", Value: int32(32)},
				{Key: "efConstruction", Value: int32(16)},
			}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, efConstructionBelowM, "BadValue")

	unsupportedVectorType := serveCommand(t, server, 23328, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "treedbIndexType", Value: "hnsw"},
			{Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(64)}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, unsupportedVectorType, "BadValue")
	errmsg, ok = bson.Raw(unsupportedVectorType).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(errmsg, `"embedding"`) || strings.Contains(errmsg, "embedding_1") {
		t.Fatalf("unsupported vector type errmsg=%q ok=%v want neutral default name", errmsg, ok)
	}

	oversizedVectorOption := serveCommand(t, server, 23324, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "embedding_vector"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{
				{Key: "dimensions", Value: int64(1) << 40},
			}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, oversizedVectorOption, "BadValue")
	errmsg, ok = bson.Raw(oversizedVectorOption).Lookup("errmsg").StringValueOK()
	for _, want := range []string{"dimensions", "int32"} {
		if !ok || !strings.Contains(errmsg, want) {
			t.Fatalf("oversized vector option errmsg=%q ok=%v want %q", errmsg, ok, want)
		}
	}

	conflictingDuplicate := serveCommand(t, server, 2333, bson.D{
		{Key: "createIndexes", Value: "conflicting_dup"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
				{Key: "name", Value: "email_1"},
				{Key: "treedbValueType", Value: "string"},
				{Key: "unique", Value: true},
			},
			bson.D{
				{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
				{Key: "name", Value: "email_1"},
				{Key: "treedbValueType", Value: "string"},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, conflictingDuplicate, "BadValue")
	errmsg, ok = bson.Raw(conflictingDuplicate).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(errmsg, `duplicate index "email_1"`) {
		t.Fatalf("errmsg=%q ok=%v want duplicate index", errmsg, ok)
	}
	if _, err := server.Collections.OpenCollection("app.conflicting_dup"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("conflicting duplicate collection err=%v, want collection not found", err)
	}

	crossKindDuplicate := serveCommand(t, server, 23329, bson.D{
		{Key: "createIndexes", Value: "cross_kind_dup"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
				{Key: "name", Value: "shared_name"},
				{Key: "treedbValueType", Value: "string"},
			},
			bson.D{
				{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
				{Key: "name", Value: "shared_name"},
				{Key: "treedbIndexType", Value: "vector"},
				{Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(64)}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, crossKindDuplicate, "DuplicateKey")

	assertOK(t, serveCommand(t, server, 23330, bson.D{
		{Key: "insert", Value: "vector_dup"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "$db", Value: "app"},
	}))
	conflictingVectorDuplicate := serveCommand(t, server, 23331, bson.D{
		{Key: "createIndexes", Value: "vector_dup"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
				{Key: "name", Value: "embedding_vector"},
				{Key: "treedbIndexType", Value: "vector"},
				{Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(64)}}},
			},
			bson.D{
				{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
				{Key: "name", Value: "embedding_vector"},
				{Key: "treedbIndexType", Value: "vector"},
				{Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(128)}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, conflictingVectorDuplicate, "BadValue")
	errmsg, ok = bson.Raw(conflictingVectorDuplicate).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(errmsg, `duplicate vector index "embedding_vector"`) {
		t.Fatalf("conflicting vector duplicate errmsg=%q ok=%v want duplicate vector index", errmsg, ok)
	}
	vectorDupCol, err := server.Collections.OpenCollection("app.vector_dup")
	if err != nil {
		t.Fatalf("open vector_dup collection: %v", err)
	}
	if got := len(vectorDupCol.Meta().VectorIndexes); got != 0 {
		t.Fatalf("vector indexes after rejected duplicate=%d want 0", got)
	}
	vectorDupIndexes := cursorFirstBatch(t, serveCommand(t, server, 23332, bson.D{
		{Key: "listIndexes", Value: "vector_dup"},
		{Key: "$db", Value: "app"},
	}))
	if got, want := len(vectorDupIndexes), 1; got != want {
		t.Fatalf("vector_dup indexes after rejected duplicate len=%d want %d", got, want)
	}
	assertIndexName(t, vectorDupIndexes[0], "_id_")

	invalidListCollectionsDB := serveCommand(t, server, 234, bson.D{
		{Key: "listCollections", Value: int32(1)},
		{Key: "$db", Value: "bad/name"},
	})
	assertCommandError(t, invalidListCollectionsDB, "BadValue")

	assertOK(t, serveCommand(t, server, 235, bson.D{
		{Key: "insert", Value: "dupes"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "same@example.com"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "same@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	uniqueConflict := serveCommand(t, server, 236, bson.D{
		{Key: "createIndexes", Value: "dupes"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"},
			{Key: "unique", Value: true},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, uniqueConflict, "DuplicateKey")

	assertOK(t, serveCommand(t, server, 237, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 238, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	existingCrossKindDuplicate := serveCommand(t, server, 242, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}},
			{Key: "name", Value: "city_1"},
			{Key: "treedbIndexType", Value: "vector"},
			{Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(64)}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, existingCrossKindDuplicate, "DuplicateKey")
	emptyDrop := serveCommand(t, server, 239, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: bson.A{}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, emptyDrop, "FailedToParse")

	partialDrop := serveCommand(t, server, 240, bson.D{
		{Key: "dropIndexes", Value: "users"},
		{Key: "index", Value: bson.A{"city_1", "missing_1"}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, partialDrop, "IndexNotFound")
	afterPartialDrop := serveCommand(t, server, 241, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	indexBatch := cursorFirstBatch(t, afterPartialDrop)
	if got, want := len(indexBatch), 3; got != want {
		t.Fatalf("index batch after failed drop len=%d want %d", got, want)
	}
	assertIndexName(t, indexBatch[0], "_id_")
	assertIndexName(t, indexBatch[1], "city_1")
	assertIndexName(t, indexBatch[2], "email_1")
}

func TestServerFindPlannerIndexedAndBoundedPredicates(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatTemplateV1}
	assertOK(t, serveCommand(t, server, 232, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
				{Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"},
			},
			bson.D{
				{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
				{Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"},
			},
			bson.D{
				{Key: "key", Value: bson.D{{Key: "score", Value: int32(1)}}},
				{Key: "name", Value: "score_1"}, {Key: "treedbValueType", Value: "double"},
			},
		}},
		{Key: "$db", Value: "app"},
	}))
	id1 := bson.NewObjectID()
	id2 := bson.NewObjectID()
	id3 := bson.NewObjectID()
	assertOK(t, serveCommand(t, server, 233, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: id1}, {Key: "name", Value: "ada"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(37)}, {Key: "score", Value: 1.25}},
			bson.D{{Key: "_id", Value: id2}, {Key: "name", Value: "grace"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(42)}, {Key: "score", Value: 2.5}},
			bson.D{{Key: "_id", Value: id3}, {Key: "name", Value: "katherine"}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int64(36)}, {Key: "score", Value: 3.75}},
		}},
		{Key: "$db", Value: "app"},
	}))

	indexedFind := serveCommand(t, server, 234, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "city", Value: "hnl"}},
			bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(40)}}}},
		}}}},
		{Key: "projection", Value: bson.D{{Key: "name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, indexedFind)
	if len(firstBatch) != 1 {
		t.Fatalf("indexed firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "grace" {
		t.Fatalf("projected name=%q ok=%v want grace", got, ok)
	}
	if !firstBatch[0].Lookup("_id").IsZero() {
		t.Fatalf("projected document unexpectedly includes _id: %v", firstBatch[0])
	}
	if !firstBatch[0].Lookup("age").IsZero() {
		t.Fatalf("projected document unexpectedly includes age: %v", firstBatch[0])
	}

	oldMaxFindScanDocuments := server.MaxFindScanDocuments
	server.MaxFindScanDocuments = 1
	ageRangeFind := serveCommand(t, server, 23401, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(37)}}}},
			bson.D{{Key: "age", Value: bson.D{{Key: "$lt", Value: int64(43)}}}},
		}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, ageRangeFind)
	firstBatch = cursorFirstBatch(t, ageRangeFind)
	if len(firstBatch) != 1 {
		t.Fatalf("indexed age range firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "grace" {
		t.Fatalf("indexed age range matched name=%q ok=%v want grace", got, ok)
	}

	doubleInt64EqualityFind := serveCommand(t, server, 234011, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: 37.0}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, doubleInt64EqualityFind)
	firstBatch = cursorFirstBatch(t, doubleInt64EqualityFind)
	if len(firstBatch) != 1 {
		t.Fatalf("indexed int64 equality via double firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "ada" {
		t.Fatalf("indexed int64 equality via double matched name=%q ok=%v want ada", got, ok)
	}

	fractionalDoubleInt64EqualityFind := serveCommand(t, server, 234012, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: 37.5}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, fractionalDoubleInt64EqualityFind)
	firstBatch = cursorFirstBatch(t, fractionalDoubleInt64EqualityFind)
	if len(firstBatch) != 0 {
		t.Fatalf("fractional double int64 equality firstBatch len=%d want 0", len(firstBatch))
	}

	stringRangeFind := serveCommand(t, server, 23402, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: bson.D{{Key: "$gte", Value: "s"}, {Key: "$lt", Value: "t"}}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, stringRangeFind)
	firstBatch = cursorFirstBatch(t, stringRangeFind)
	if len(firstBatch) != 1 {
		t.Fatalf("indexed string range firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "katherine" {
		t.Fatalf("indexed string range matched name=%q ok=%v want katherine", got, ok)
	}

	doubleRangeFind := serveCommand(t, server, 23403, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "score", Value: bson.D{{Key: "$gt", Value: 2.0}, {Key: "$lte", Value: 3.0}}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, doubleRangeFind)
	firstBatch = cursorFirstBatch(t, doubleRangeFind)
	if len(firstBatch) != 1 {
		t.Fatalf("indexed double range firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "grace" {
		t.Fatalf("indexed double range matched name=%q ok=%v want grace", got, ok)
	}

	wrongTypeIndexedRange := serveCommand(t, server, 23404, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: "40"}}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, wrongTypeIndexedRange)
	firstBatch = cursorFirstBatch(t, wrongTypeIndexedRange)
	if len(firstBatch) != 0 {
		t.Fatalf("wrong-type indexed range firstBatch len=%d want 0", len(firstBatch))
	}
	server.MaxFindScanDocuments = oldMaxFindScanDocuments

	limitedAgeRangeFind := serveCommand(t, server, 234041, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(36)}}}}},
		{Key: "limit", Value: int32(2)},
		{Key: "batchSize", Value: int32(2)},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, limitedAgeRangeFind)
	firstBatch = cursorFirstBatch(t, limitedAgeRangeFind)
	if len(firstBatch) != 2 {
		t.Fatalf("limited indexed age range firstBatch len=%d want 2", len(firstBatch))
	}
	if cursorID := cursorIDFromResponse(t, limitedAgeRangeFind); cursorID != 0 {
		t.Fatalf("limited indexed age range cursor id=%d want 0", cursorID)
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "katherine" {
		t.Fatalf("limited indexed age range first name=%q ok=%v want katherine", got, ok)
	}
	if got, ok := firstBatch[1].Lookup("name").StringValueOK(); !ok || got != "ada" {
		t.Fatalf("limited indexed age range second name=%q ok=%v want ada", got, ok)
	}

	wrongTypeIndexedFind := serveCommand(t, server, 2341, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: int32(5)}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, wrongTypeIndexedFind)
	firstBatch = cursorFirstBatch(t, wrongTypeIndexedFind)
	if len(firstBatch) != 0 {
		t.Fatalf("wrong-type indexed firstBatch len=%d want 0", len(firstBatch))
	}

	inFind := serveCommand(t, server, 235, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{id3, id1}}}}}},
		{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
		{Key: "skip", Value: int32(1)},
		{Key: "limit", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	firstBatch = cursorFirstBatch(t, inFind)
	if len(firstBatch) != 1 {
		t.Fatalf("$in firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "katherine" {
		t.Fatalf("$in sorted/skipped name=%q ok=%v want katherine", got, ok)
	}

	withoutID := serveCommand(t, server, 236, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id1}}},
		{Key: "projection", Value: bson.D{{Key: "_id", Value: int32(0)}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch = cursorFirstBatch(t, withoutID)
	if len(firstBatch) != 1 {
		t.Fatalf("_id exclude firstBatch len=%d want 1", len(firstBatch))
	}
	if !firstBatch[0].Lookup("_id").IsZero() {
		t.Fatalf("_id exclude projection returned _id: %v", firstBatch[0])
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "ada" {
		t.Fatalf("_id exclude name=%q ok=%v want ada", got, ok)
	}

	onlyID := serveCommand(t, server, 237, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id1}}},
		{Key: "projection", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch = cursorFirstBatch(t, onlyID)
	if len(firstBatch) != 1 {
		t.Fatalf("_id include firstBatch len=%d want 1", len(firstBatch))
	}
	elements, err := firstBatch[0].Elements()
	if err != nil {
		t.Fatalf("_id include elements: %v", err)
	}
	if len(elements) != 1 || firstBatch[0].Lookup("_id").IsZero() {
		t.Fatalf("_id include projection=%v want only _id", firstBatch[0])
	}

	missingProjected := serveCommand(t, server, 2371, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: bson.NewObjectID()}}},
		{Key: "projection", Value: bson.D{{Key: "name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}},
		{Key: "limit", Value: int32(1)},
		{Key: "singleBatch", Value: true},
		{Key: "$db", Value: "app"},
	})
	firstBatch = cursorFirstBatch(t, missingProjected)
	if len(firstBatch) != 0 {
		t.Fatalf("missing projected primary find firstBatch len=%d want 0", len(firstBatch))
	}
	if cursorID := cursorIDFromResponse(t, missingProjected); cursorID != 0 {
		t.Fatalf("missing projected primary find cursor id=%d want 0", cursorID)
	}

	id4 := bson.NewObjectID()
	id5 := bson.NewObjectID()
	assertOK(t, serveCommand(t, server, 238, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: id4}, {Key: "name", Value: "large-a"}, {Key: "big", Value: int64(9007199254740992)}},
			bson.D{{Key: "_id", Value: id5}, {Key: "name", Value: "large-b"}, {Key: "big", Value: int64(9007199254740993)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	largeFind := serveCommand(t, server, 239, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "big", Value: int64(9007199254740993)}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch = cursorFirstBatch(t, largeFind)
	if len(firstBatch) != 1 {
		t.Fatalf("large int firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("name").StringValueOK(); !ok || got != "large-b" {
		t.Fatalf("large int matched name=%q ok=%v want large-b", got, ok)
	}

	negativeSkipMissingCollection := serveCommand(t, server, 240, bson.D{
		{Key: "find", Value: "missing"},
		{Key: "filter", Value: bson.D{}},
		{Key: "skip", Value: int32(-1)},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, negativeSkipMissingCollection, "BadValue")

	emptyAnd := serveCommand(t, server, 241, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, emptyAnd, "BadValue")

	mixedOperator := serveCommand(t, server, 242, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "x", Value: int32(1)}, {Key: "$gte", Value: int64(40)}}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, mixedOperator, "BadValue")

	unsupportedSort := serveCommand(t, server, 243, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "$natural", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, unsupportedSort, "BadValue")

	dottedSort := serveCommand(t, server, 244, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "profile.name", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, dottedSort, "BadValue")

	nonIndexableValue := serveCommand(t, server, 245, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: bson.D{{Key: "nested", Value: "hnl"}}}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, nonIndexableValue)
	if batch := cursorFirstBatch(t, nonIndexableValue); len(batch) != 0 {
		t.Fatalf("non-indexable value firstBatch len=%d want 0", len(batch))
	}
}

func TestServerPureIndexedRangeKeepsCursorWhenByteCapped(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 260, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{
				{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
				{Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"},
			},
		}},
		{Key: "$db", Value: "app"},
	}))

	docA := bson.D{{Key: "_id", Value: "a"}, {Key: "age", Value: int64(10)}, {Key: "payload", Value: strings.Repeat("a", 24)}}
	docB := bson.D{{Key: "_id", Value: "b"}, {Key: "age", Value: int64(11)}, {Key: "payload", Value: strings.Repeat("b", 24)}}
	docC := bson.D{{Key: "_id", Value: "c"}, {Key: "age", Value: int64(12)}, {Key: "payload", Value: strings.Repeat("c", 24)}}
	assertOK(t, serveCommand(t, server, 261, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{docA, docB, docC}},
		{Key: "$db", Value: "app"},
	}))

	rawA, err := bson.Marshal(docA)
	if err != nil {
		t.Fatalf("marshal docA: %v", err)
	}
	oneDocBatchBytes := findBatchOverheadBytes + findBatchDocumentBytes(rawA, 0)
	server.MaxMessageLength = int32(findBatchResponseReserveBytes + oneDocBatchBytes)
	findResponse := serveCommand(t, server, 262, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(10)}}}}},
		{Key: "limit", Value: int32(3)},
		{Key: "batchSize", Value: int32(3)},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, findResponse)
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("_id").StringValueOK(); !ok || got != "a" {
		t.Fatalf("firstBatch[0]._id=%q ok=%t want a", got, ok)
	}
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id is 0 after byte-capped first batch, want retained cursor")
	}

	getMoreResponse := serveCommand(t, server, 263, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, getMoreResponse)
	nextBatch := cursorNextBatch(t, getMoreResponse)
	if len(nextBatch) != 1 {
		t.Fatalf("nextBatch len=%d want 1", len(nextBatch))
	}
	if got, ok := nextBatch[0].Lookup("_id").StringValueOK(); !ok || got != "b" {
		t.Fatalf("nextBatch[0]._id=%q ok=%t want b", got, ok)
	}
	nextID := cursorIDFromResponse(t, getMoreResponse)
	if nextID != cursorID {
		t.Fatalf("cursor id after first getMore=%d want %d", nextID, cursorID)
	}

	finalResponse := serveCommand(t, server, 264, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, finalResponse)
	finalBatch := cursorNextBatch(t, finalResponse)
	if len(finalBatch) != 1 {
		t.Fatalf("final nextBatch len=%d want 1", len(finalBatch))
	}
	if got, ok := finalBatch[0].Lookup("_id").StringValueOK(); !ok || got != "c" {
		t.Fatalf("finalBatch[0]._id=%q ok=%t want c", got, ok)
	}
	if finalID := cursorIDFromResponse(t, finalResponse); finalID != 0 {
		t.Fatalf("cursor id after final getMore=%d want 0", finalID)
	}
}

func TestServerFindGetMoreAndKillCursors(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 236, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}},
			bson.D{{Key: "_id", Value: "u3"}, {Key: "name", Value: "katherine"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	findResponse := serveCommand(t, server, 237, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}

	getMoreResponse := serveCommand(t, server, 238, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	nextBatch := cursorNextBatch(t, getMoreResponse)
	if len(nextBatch) != 1 {
		t.Fatalf("nextBatch len=%d want 1", len(nextBatch))
	}
	if nextID := cursorIDFromResponse(t, getMoreResponse); nextID != cursorID {
		t.Fatalf("cursor id after first getMore=%d want %d", nextID, cursorID)
	}

	killResponse := serveCommand(t, server, 239, bson.D{
		{Key: "killCursors", Value: "users"},
		{Key: "cursors", Value: bson.A{cursorID}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, killResponse)
	killed, ok := bson.Raw(killResponse).Lookup("cursorsKilled").ArrayOK()
	if !ok {
		t.Fatal("cursorsKilled missing")
	}
	if values, err := killed.Values(); err != nil || len(values) != 1 {
		t.Fatalf("cursorsKilled values len/err=%d/%v want 1/nil", len(values), err)
	}

	missingResponse := serveCommand(t, server, 240, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, missingResponse, "CursorNotFound")
}

func TestServerFindBatchSizeZeroKeepsCursorEmpty(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 241, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	findResponse := serveCommand(t, server, 242, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(0)},
		{Key: "$db", Value: "app"},
	})
	if firstBatch := cursorFirstBatch(t, findResponse); len(firstBatch) != 0 {
		t.Fatalf("firstBatch len=%d want 0", len(firstBatch))
	}
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}

	zeroGetMore := serveCommand(t, server, 243, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "batchSize", Value: int32(0)},
		{Key: "$db", Value: "app"},
	})
	nextBatch := cursorNextBatch(t, zeroGetMore)
	if len(nextBatch) != 2 {
		t.Fatalf("zero getMore nextBatch len=%d want 2", len(nextBatch))
	}
	if nextID := cursorIDFromResponse(t, zeroGetMore); nextID != 0 {
		t.Fatalf("cursor id after zero getMore=%d want 0", nextID)
	}
	name, ok := nextBatch[0].Lookup("name").StringValueOK()
	if !ok || name != "ada" {
		t.Fatalf("nextBatch[0].name=%q ok=%v want ada", name, ok)
	}

	projectedFindResponse := serveCommand(t, server, 2431, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "projection", Value: bson.D{{Key: "name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}},
		{Key: "batchSize", Value: int32(0)},
		{Key: "$db", Value: "app"},
	})
	if firstBatch := cursorFirstBatch(t, projectedFindResponse); len(firstBatch) != 0 {
		t.Fatalf("projected primary firstBatch len=%d want 0", len(firstBatch))
	}
	projectedCursorID := cursorIDFromResponse(t, projectedFindResponse)
	if projectedCursorID == 0 {
		t.Fatal("projected primary cursor id=0 want open cursor")
	}
	projectedGetMore := serveCommand(t, server, 2432, bson.D{
		{Key: "getMore", Value: projectedCursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	projectedNextBatch := cursorNextBatch(t, projectedGetMore)
	if len(projectedNextBatch) != 1 {
		t.Fatalf("projected primary nextBatch len=%d want 1", len(projectedNextBatch))
	}
	if got, ok := projectedNextBatch[0].Lookup("name").StringValueOK(); !ok || got != "ada" {
		t.Fatalf("projected primary nextBatch name=%q ok=%v want ada", got, ok)
	}
	if !projectedNextBatch[0].Lookup("_id").IsZero() {
		t.Fatalf("projected primary nextBatch unexpectedly includes _id: %v", projectedNextBatch[0])
	}
}

func TestServerFindSingleBatchClosesCursor(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 244, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	findResponse := serveCommand(t, server, 245, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(1)},
		{Key: "singleBatch", Value: true},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	if cursorID := cursorIDFromResponse(t, findResponse); cursorID != 0 {
		t.Fatalf("singleBatch cursor id=%d want 0", cursorID)
	}
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("open cursors=%d want 0", len(server.cursors))
	}
}

func TestMarshalCursorDocumentsResponseWithRawBatch(t *testing.T) {
	rawDoc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}})
	response, err := marshalCursorDocumentsResponseWithID("app.users", 0, "firstBatch", []wire.Document{rawDoc})
	if err != nil {
		t.Fatalf("marshal raw cursor response: %v", err)
	}
	assertOK(t, response)
	if cursorID := cursorIDFromResponse(t, response); cursorID != 0 {
		t.Fatalf("cursor id=%d want 0", cursorID)
	}
	batch := cursorFirstBatch(t, response)
	if len(batch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(batch))
	}
	if !bytes.Equal(batch[0], bson.Raw(rawDoc)) {
		t.Fatalf("firstBatch[0]=%v want raw doc %v", batch[0], bson.Raw(rawDoc))
	}
}

func TestServerFindRawBSONOPMsgResponse(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 249, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int64(41)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	findDoc := mustDocument(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(40)}}}}},
		{Key: "limit", Value: int32(1)},
		{Key: "singleBatch", Value: true},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessage(nil, 250, 0, 0, findDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}
	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}
	h, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatalf("ReadMessage response: %v", err)
	}
	if h.OpCode != wire.OpMsg || h.ResponseTo != 250 || h.MessageLength != int32(len(rw.w.Bytes())) {
		t.Fatalf("response header=%+v len=%d", h, len(rw.w.Bytes()))
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		t.Fatalf("ParseMsg response: %v", err)
	}
	assertOK(t, msg.Body)
	if cursorID := cursorIDFromResponse(t, msg.Body); cursorID != 0 {
		t.Fatalf("cursor id=%d want 0", cursorID)
	}
	batch := cursorFirstBatch(t, msg.Body)
	if len(batch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(batch))
	}
	if age, ok := batch[0].Lookup("age").Int64OK(); !ok || age != 41 {
		t.Fatalf("firstBatch age=%d ok=%v want 41", age, ok)
	}
}

func TestServerFindRawBSONOPMsgResponseHonorsMaxMessageLength(t *testing.T) {
	rawDoc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "payload", Value: strings.Repeat("x", 256)}})
	if _, err := marshalCursorDocumentsMsgResponseWithIDInto(nil, 1, 250, "app.users", 0, "firstBatch", []wire.Document{rawDoc}, wire.DefaultMaxMessageLength); err != nil {
		t.Fatalf("marshal default max response: %v", err)
	}
	_, err := marshalCursorDocumentsMsgResponseWithIDInto(nil, 1, 250, "app.users", 0, "firstBatch", []wire.Document{rawDoc}, 128)
	if !errors.Is(err, wire.ErrMessageTooLarge) {
		t.Fatalf("marshal small max err=%v want ErrMessageTooLarge", err)
	}
}

func TestFindResponsePayloadOPMsgHonorsMaxMessageLength(t *testing.T) {
	payload := findResponsePayload{
		document: mustDocument(t, bson.D{{Key: "ok", Value: 1.0}, {Key: "payload", Value: strings.Repeat("x", 256)}}),
	}
	if _, err := payload.marshalMsgIntoWithMaxLength(nil, 1, 250, wire.DefaultMaxMessageLength); err != nil {
		t.Fatalf("marshal default max response: %v", err)
	}
	_, err := payload.marshalMsgIntoWithMaxLength(nil, 1, 250, 128)
	if !errors.Is(err, wire.ErrMessageTooLarge) {
		t.Fatalf("marshal small max err=%v want ErrMessageTooLarge", err)
	}
}

func TestRawDocumentsBatchLimit(t *testing.T) {
	docs := []wire.Document{
		mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u2"}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u3"}}),
	}
	consumed, err := rawDocumentsBatchLimit(docs, 2, maxInt)
	if err != nil {
		t.Fatalf("raw batch limit: %v", err)
	}
	if consumed != 2 {
		t.Fatalf("consumed=%d want 2", consumed)
	}
	tooSmall := findBatchOverheadBytes + findBatchDocumentBytes(docs[0], 0) - 1
	if _, err := rawDocumentsBatchLimit(docs[:1], 1, tooSmall); err == nil {
		t.Fatal("raw batch limit accepted oversized single document")
	}
}

func TestValidateStoredBSONFrame(t *testing.T) {
	valid := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	if err := validateStoredBSONFrame(valid); err != nil {
		t.Fatalf("valid frame: %v", err)
	}
	for name, doc := range map[string][]byte{
		"too_short":            {1, 0, 0, 0},
		"declared_too_small":   {4, 0, 0, 0, 0},
		"length_mismatch":      {6, 0, 0, 0, 0},
		"missing_terminator":   {5, 0, 0, 0, 1},
		"negative_length":      {0xff, 0xff, 0xff, 0xff, 0},
		"zero_declared_length": {0, 0, 0, 0, 0},
	} {
		if err := validateStoredBSONFrame(doc); err == nil {
			t.Fatalf("%s accepted invalid frame %v", name, doc)
		}
	}
}

func TestServeOneCleansUpOneShotCursors(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 246, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}},
			bson.D{{Key: "_id", Value: "u2"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	commandDoc := mustDocument(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: int32(0)},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessage(nil, 247, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}
	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}
	findResponse := readMsgResponse(t, rw.w.Bytes(), 247)
	if cursorID := cursorIDFromResponse(t, findResponse); cursorID == 0 {
		t.Fatal("one-shot find cursor id=0 want returned cursor id")
	}
	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if len(server.cursors) != 0 {
		t.Fatalf("open cursors=%d want 0", len(server.cursors))
	}
}

func TestServerGetMoreWithoutBatchSizeUsesMessageLimit(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	documents := make(bson.A, 0, 4)
	for _, name := range []string{"ada", "grace", "katherine", "mary"} {
		documents = append(documents, bson.D{{Key: "_id", Value: name}, {Key: "name", Value: name}})
	}
	assertOK(t, serveCommand(t, server, 245, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: documents},
		{Key: "$db", Value: "app"},
	}))

	findResponse := serveCommand(t, server, 246, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	if firstBatch := cursorFirstBatch(t, findResponse); len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}

	getMoreResponse := serveCommand(t, server, 247, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	nextBatch := cursorNextBatch(t, getMoreResponse)
	if len(nextBatch) != 3 {
		t.Fatalf("nextBatch len=%d want 3", len(nextBatch))
	}
	if nextID := cursorIDFromResponse(t, getMoreResponse); nextID != 0 {
		t.Fatalf("cursor id after getMore=%d want 0", nextID)
	}
}

func TestServerCursorRejectsNegativeBatchSize(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 248, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}},
			bson.D{{Key: "_id", Value: "u2"}},
		}},
		{Key: "$db", Value: "app"},
	}))

	negativeFind := serveCommand(t, server, 249, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: int32(-1)},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, negativeFind, "BadValue")

	findResponse := serveCommand(t, server, 250, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}
	negativeGetMore := serveCommand(t, server, 251, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "batchSize", Value: int32(-1)},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, negativeGetMore, "BadValue")
}

func TestRequiredInt64FieldsRejectNonIntegerNumerics(t *testing.T) {
	doc := mustDocument(t, bson.D{
		{Key: "getMore", Value: 1.5},
		{Key: "cursors", Value: bson.A{int32(1), int64(2), 3.5}},
	})
	if _, err := requiredInt64Field(doc, "getMore"); err == nil {
		t.Fatal("requiredInt64Field accepted double cursor id")
	}
	values, err := requiredInt64ArrayField(doc, "cursors")
	if err == nil {
		t.Fatalf("requiredInt64ArrayField values=%v err=nil want double rejection", values)
	}

	okDoc := mustDocument(t, bson.D{
		{Key: "getMore", Value: int32(7)},
		{Key: "cursors", Value: bson.A{int32(8), int64(9)}},
	})
	if value, err := requiredInt64Field(okDoc, "getMore"); err != nil || value != 7 {
		t.Fatalf("requiredInt64Field int32 value/err=%d/%v want 7/nil", value, err)
	}
	values, err = requiredInt64ArrayField(okDoc, "cursors")
	if err != nil || len(values) != 2 || values[0] != 8 || values[1] != 9 {
		t.Fatalf("requiredInt64ArrayField values/err=%v/%v want [8 9]/nil", values, err)
	}
}

func TestServerCursorOwnerCleanup(t *testing.T) {
	server := NewServer()
	docs := []wire.Document{
		mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u2"}}),
	}
	cursorID, firstBatch, err := server.openCursor("app.users", docs, compiledProjection{}, 0, true, defaultCursorBatchSize, 99)
	if err != nil {
		t.Fatalf("openCursor: %v", err)
	}
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}
	if len(firstBatch) != 0 {
		t.Fatalf("firstBatch len=%d want 0", len(firstBatch))
	}

	server.killCursorsForOwner(99)
	if _, _, ok, err := server.getMore(cursorID, "app.users", 99, 1, true, defaultCursorBatchSize); err != nil || ok {
		t.Fatalf("getMore after owner cleanup ok/err=%v/%v want false/nil", ok, err)
	}
}

func TestServerCursorOwnerIsolation(t *testing.T) {
	server := NewServer()
	docs := []wire.Document{
		mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u2"}}),
	}
	cursorID, firstBatch, err := server.openCursor("app.users", docs, compiledProjection{}, 1, true, defaultCursorBatchSize, 1)
	if err != nil {
		t.Fatalf("openCursor: %v", err)
	}
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	if _, _, ok, err := server.getMore(cursorID, "app.users", 2, 1, true, defaultCursorBatchSize); err != nil || ok {
		t.Fatalf("wrong-owner getMore ok/err=%v/%v want false/nil", ok, err)
	}
	if killed, notFound := server.killCursors("app.users", 2, []int64{cursorID}); len(killed) != 0 || len(notFound) != 1 {
		t.Fatalf("wrong-owner kill killed=%v notFound=%v want none/one", killed, notFound)
	}
	if nextID, batch, ok, err := server.getMore(cursorID, "app.users", 1, 1, true, defaultCursorBatchSize); err != nil || !ok || nextID != 0 || len(batch) != 1 {
		t.Fatalf("owner getMore id=%d len=%d ok/err=%v/%v want 0/1 true/nil", nextID, len(batch), ok, err)
	}
}

func TestServerCursorLimit(t *testing.T) {
	server := NewServer()
	server.MaxOpenCursors = 1
	docs := []wire.Document{
		mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}),
	}

	cursorID, _, err := server.openCursor("app.users", docs, compiledProjection{}, 0, true, defaultCursorBatchSize, 1)
	if err != nil {
		t.Fatalf("openCursor first: %v", err)
	}
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}
	if _, _, err := server.openCursor("app.users", docs, compiledProjection{}, 0, true, defaultCursorBatchSize, 2); err == nil {
		t.Fatal("second openCursor err=nil want cursor limit error")
	}
}

func TestServerCursorRetainedBytesLimit(t *testing.T) {
	server := NewServer()
	server.MaxCursorRetainedBytes = 1
	docs := []wire.Document{
		mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u2"}}),
	}

	cursorID, firstBatch, err := server.openCursor("app.users", docs, compiledProjection{}, 1, true, defaultCursorBatchSize, 1)
	if err == nil {
		t.Fatalf("openCursor id=%d firstBatch=%d err=nil want retained bytes error", cursorID, len(firstBatch))
	}
	if cursorID != 0 || firstBatch != nil {
		t.Fatalf("openCursor id/firstBatch=%d/%v want zero/nil on retained bytes error", cursorID, firstBatch)
	}
}

func TestServerCursorIdleExpiry(t *testing.T) {
	server := NewServer()
	server.CursorIdleTimeout = time.Minute
	docs := []wire.Document{
		mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u2"}}),
	}
	cursorID, _, err := server.openCursor("app.users", docs, compiledProjection{}, 1, true, defaultCursorBatchSize, 1)
	if err != nil {
		t.Fatalf("openCursor: %v", err)
	}
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}

	server.cursorMu.Lock()
	server.cursors[cursorID].lastUsed = time.Now().Add(-2 * time.Minute)
	server.cursorMu.Unlock()

	if _, _, ok, err := server.getMore(cursorID, "app.users", 1, 1, true, defaultCursorBatchSize); err != nil || ok {
		t.Fatalf("expired getMore ok/err=%v/%v want false/nil", ok, err)
	}
}

func TestServerBackgroundCursorReapIsThrottled(t *testing.T) {
	server := NewServer()
	server.CursorIdleTimeout = time.Nanosecond
	server.cursorMu.Lock()
	server.addCursorLocked(1, &serverCursor{ns: "app.users", owner: 1, lastUsed: time.Now().Add(-time.Minute)})
	server.lastCursorReap = time.Now()
	server.cursorMu.Unlock()

	server.reapExpiredCursors()
	server.cursorMu.Lock()
	_, stillPresent := server.cursors[1]
	server.lastCursorReap = time.Now().Add(-2 * defaultCursorReapInterval)
	server.cursorMu.Unlock()
	if !stillPresent {
		t.Fatal("cursor reaped despite throttle interval")
	}

	server.reapExpiredCursors()
	server.cursorMu.Lock()
	_, stillPresent = server.cursors[1]
	server.cursorMu.Unlock()
	if stillPresent {
		t.Fatal("cursor not reaped after throttle interval")
	}
}

func TestServerBackgroundCursorReapSkipsEmptyCursorMap(t *testing.T) {
	server := NewServer()
	server.CursorIdleTimeout = time.Nanosecond
	lastReap := time.Now().Add(-2 * defaultCursorReapInterval)
	server.cursorMu.Lock()
	server.lastCursorReap = lastReap
	server.cursorMu.Unlock()

	server.reapExpiredCursors()

	server.cursorMu.Lock()
	defer server.cursorMu.Unlock()
	if !server.lastCursorReap.Equal(lastReap) {
		t.Fatalf("lastCursorReap changed to %v want %v", server.lastCursorReap, lastReap)
	}
}

func TestServerCursorBatchesRespectMessageSize(t *testing.T) {
	server := NewServer()
	server.MaxMessageLength = 5 * 1024
	docs := []wire.Document{
		mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "payload", Value: strings.Repeat("a", 900)}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "payload", Value: strings.Repeat("b", 900)}}),
		mustDocument(t, bson.D{{Key: "_id", Value: "u3"}, {Key: "payload", Value: strings.Repeat("c", 900)}}),
	}

	cursorID, firstBatch, err := server.openCursor("app.users", docs, compiledProjection{}, 10, true, defaultCursorBatchSize, 0)
	if err != nil {
		t.Fatalf("openCursor: %v", err)
	}
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}

	nextID, nextBatch, ok, err := server.getMore(cursorID, "app.users", 0, 10, true, defaultCursorBatchSize)
	if err != nil || !ok {
		t.Fatalf("getMore ok/err=%v/%v want true/nil", ok, err)
	}
	if len(nextBatch) != 1 {
		t.Fatalf("nextBatch len=%d want 1", len(nextBatch))
	}
	if nextID != cursorID {
		t.Fatalf("cursor id after getMore=%d want %d", nextID, cursorID)
	}

	finalID, finalBatch, ok, err := server.getMore(cursorID, "app.users", 0, 10, true, defaultCursorBatchSize)
	if err != nil || !ok {
		t.Fatalf("final getMore ok/err=%v/%v want true/nil", ok, err)
	}
	if len(finalBatch) != 1 {
		t.Fatalf("finalBatch len=%d want 1", len(finalBatch))
	}
	if finalID != 0 {
		t.Fatalf("final cursor id=%d want 0", finalID)
	}
}

func TestServerFindNullEqualityMatchesMissingWithIndex(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 248, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "null"}, {Key: "city", Value: nil}},
			bson.D{{Key: "_id", Value: "missing"}, {Key: "name", Value: "no city"}},
			bson.D{{Key: "_id", Value: "hnl"}, {Key: "city", Value: "hnl"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 249, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))

	nullFind := serveCommand(t, server, 250, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: nil}}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, nullFind), []string{"missing", "null"})

	inFind := serveCommand(t, server, 251, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: bson.D{{Key: "$in", Value: bson.A{nil, "hnl"}}}}}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, inFind), []string{"hnl", "missing", "null"})
}

func TestServerFindRejectsOversizedResultDocument(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxMessageLength = 4500
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 252, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "large"}, {Key: "payload", Value: strings.Repeat("x", 600)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	tooLarge := serveCommand(t, server, 253, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "large"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, tooLarge, "BadValue")
}

func TestServerFindFirstBatchOverflowOpensCursor(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxMessageLength = 5200
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 250, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "payload", Value: strings.Repeat("a", 700)}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "payload", Value: strings.Repeat("b", 700)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 251, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}
	getMoreResponse := serveCommand(t, server, 252, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	nextBatch := cursorNextBatch(t, getMoreResponse)
	if len(nextBatch) != 1 {
		t.Fatalf("nextBatch len=%d want 1", len(nextBatch))
	}
	if nextID := cursorIDFromResponse(t, getMoreResponse); nextID != 0 {
		t.Fatalf("cursor id after getMore=%d want 0", nextID)
	}
}

func TestServerFindIndexedRangeStreamingFirstBatchOverflowOpensCursor(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxMessageLength = 6 * 1024
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 2521, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "name", Value: "age_1"},
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "treedbValueType", Value: "int64"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	largeValue := strings.Repeat("x", 1400)
	assertOK(t, serveCommand(t, server, 2522, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "age", Value: int64(37)}, {Key: "payload", Value: largeValue}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "age", Value: int64(42)}, {Key: "payload", Value: largeValue}},
		}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 2523, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(37)}}}}},
		{Key: "limit", Value: int32(2)},
		{Key: "batchSize", Value: int32(2)},
		{Key: "$db", Value: "app"},
	})
	firstBatch := cursorFirstBatch(t, findResponse)
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("_id").StringValueOK(); !ok || got != "a" {
		t.Fatalf("firstBatch _id=%q ok=%v want a", got, ok)
	}
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}
	getMoreResponse := serveCommand(t, server, 2524, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	nextBatch := cursorNextBatch(t, getMoreResponse)
	if len(nextBatch) != 1 {
		t.Fatalf("nextBatch len=%d want 1", len(nextBatch))
	}
	if got, ok := nextBatch[0].Lookup("_id").StringValueOK(); !ok || got != "b" {
		t.Fatalf("nextBatch _id=%q ok=%v want b", got, ok)
	}
	if nextID := cursorIDFromResponse(t, getMoreResponse); nextID != 0 {
		t.Fatalf("cursor id after getMore=%d want 0", nextID)
	}
}

func TestMarshalIndexedRangeCursorMsgDoesNotRejectCapacityHint(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(t, serveCommand(t, server, 2525, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "name", Value: "age_1"},
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "treedbValueType", Value: "int64"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 2526, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "age", Value: int64(37)}, {Key: "payload", Value: strings.Repeat("x", 64)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush collection: %v", err)
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		t.Fatalf("open materializer: %v", err)
	}
	defer func() { _ = materializer.Close() }()

	opts := collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Value: int64(37), Inclusive: true},
		Upper: collections.IndexRangeBound{Unbounded: true},
		Limit: 10_000,
	}
	msg, err := marshalIndexedRangeCursorMsgInto(nil, 1, 2526, server, 1, false, col, materializer, "app.users", "age_1", opts, "firstBatch", server.maxFindBatchBytes(), 64*1024)
	if err != nil {
		t.Fatalf("marshal indexed range cursor msg: %v", err)
	}
	firstBatch := cursorFirstBatch(t, readMsgResponse(t, msg, 2526))
	if len(firstBatch) != 1 {
		t.Fatalf("firstBatch len=%d want 1", len(firstBatch))
	}
	if got, ok := firstBatch[0].Lookup("_id").StringValueOK(); !ok || got != "a" {
		t.Fatalf("firstBatch _id=%q ok=%v want a", got, ok)
	}
}

func TestServerGetMoreDropsCursorOnOversizedNextDocument(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxMessageLength = 4500
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 253, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "payload", Value: "small"}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "payload", Value: strings.Repeat("x", 600)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 254, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	cursorID := cursorIDFromResponse(t, findResponse)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}

	tooLarge := serveCommand(t, server, 255, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, tooLarge, "BadValue")
	missing := serveCommand(t, server, 256, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, missing, "CursorNotFound")
}

func TestServerFindCapsIndexedCandidates(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 254, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 255, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	tooMany := serveCommand(t, server, 256, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: "hnl"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, tooMany, "BadValue")
}

func TestServerFindUnindexedLimitStopsBeforeScanCap(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 257, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 258, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: "hnl"}}},
		{Key: "limit", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, findResponse), []string{"u1"})
}

func TestStoredDocumentMatchesPredicatesExtendedJSONScalars(t *testing.T) {
	_, stored, err := prepareInsertDocument(mustDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
		{Key: "age", Value: int64(42)},
		{Key: "active", Value: true},
	}), collections.DocumentFormatJSON)
	if err != nil {
		t.Fatalf("prepare insert document: %v", err)
	}
	predicates := []findPredicate{
		{field: "city", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, "hnl")}},
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
		{field: "active", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, true)}},
	}
	match, ok, err := storedDocumentMatchesPredicates(stored, predicates)
	if err != nil {
		t.Fatalf("match stored predicates: %v", err)
	}
	if !ok || !match {
		t.Fatalf("match=%v ok=%v want true/true", match, ok)
	}
	match, ok, err = storedDocumentMatchesPredicates(stored, []findPredicate{
		{field: "age", op: findPredicateLT, values: []bson.RawValue{mustRawValue(t, int64(40))}},
	})
	if err != nil {
		t.Fatalf("match stored miss predicate: %v", err)
	}
	if !ok || match {
		t.Fatalf("miss match=%v ok=%v want false/true", match, ok)
	}
	_, ok, err = storedDocumentMatchesPredicates(stored, []findPredicate{
		{field: "profile.rank", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, int32(1))}},
	})
	if err != nil {
		t.Fatalf("match unsupported dotted predicate: %v", err)
	}
	if ok {
		t.Fatal("dotted predicate should fall back to BSON evaluation")
	}
}

func TestServerFindChoosesNarrowestIndexedPredicate(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 253, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}, {Key: "email", Value: "one@example.com"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}, {Key: "email", Value: "target@example.com"}},
			bson.D{{Key: "_id", Value: "u3"}, {Key: "city", Value: "hnl"}, {Key: "email", Value: "three@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 254, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}}, {Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 255, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "city", Value: "hnl"}},
			bson.D{{Key: "email", Value: "target@example.com"}},
		}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, findResponse), []string{"u2"})
}

func TestServerFindChoosesNarrowestSameFieldIndexedPredicate(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 253, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}},
			bson.D{{Key: "_id", Value: "u3"}, {Key: "city", Value: "sfo"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 254, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 255, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "city", Value: bson.D{{Key: "$in", Value: bson.A{"hnl", "sfo"}}}}},
			bson.D{{Key: "city", Value: "sfo"}},
		}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, findResponse), []string{"u3"})
}

func TestServerFindChoosesIndexedPredicateBeforeOversizedPrimaryCandidates(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 256, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "one@example.com"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "target@example.com"}},
			bson.D{{Key: "_id", Value: "u3"}, {Key: "email", Value: "three@example.com"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 257, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	findResponse := serveCommand(t, server, 258, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"u1", "u2", "u3"}}}}},
			bson.D{{Key: "email", Value: "target@example.com"}},
		}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, findResponse), []string{"u2"})
}

func TestServerFindDottedArrayPredicates(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 256, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "match"}, {Key: "tags", Value: bson.A{"a", "b"}}, {Key: "items", Value: bson.A{bson.D{{Key: "sku", Value: "sku-1"}}}}},
			bson.D{{Key: "_id", Value: "miss"}, {Key: "tags", Value: bson.A{"b"}}, {Key: "items", Value: bson.A{bson.D{{Key: "sku", Value: "sku-2"}}}}},
		}},
		{Key: "$db", Value: "app"},
	}))
	tagFind := serveCommand(t, server, 257, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "tags.0", Value: "a"}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, tagFind), []string{"match"})

	scalarTagFind := serveCommand(t, server, 258, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "tags", Value: "a"}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, scalarTagFind), []string{"match"})

	inTagFind := serveCommand(t, server, 259, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "tags", Value: bson.D{{Key: "$in", Value: bson.A{"a"}}}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, inTagFind), []string{"match"})

	itemFind := serveCommand(t, server, 260, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "items.sku", Value: "sku-1"}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, itemFind), []string{"match"})
}

func TestServerFindArrayRangePredicatesCanUseDifferentElements(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 261, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "no"}, {Key: "scores", Value: bson.A{int32(1), int32(10)}}},
			bson.D{{Key: "_id", Value: "yes"}, {Key: "scores", Value: bson.A{int32(6), int32(7)}}},
		}},
		{Key: "$db", Value: "app"},
	}))
	rangeFind := serveCommand(t, server, 262, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "scores", Value: bson.D{
			{Key: "$gt", Value: int32(5)},
			{Key: "$lt", Value: int32(8)},
		}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, rangeFind), []string{"no", "yes"})
}

func TestServerFindRangePredicatesUseTypeBrackets(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 263, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "num"}, {Key: "age", Value: int32(10)}},
			bson.D{{Key: "_id", Value: "string"}, {Key: "age", Value: "old"}},
			bson.D{{Key: "_id", Value: "object"}, {Key: "age", Value: bson.D{{Key: "nested", Value: true}}}},
		}},
		{Key: "$db", Value: "app"},
	}))
	rangeFind := serveCommand(t, server, 264, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int32(5)}}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, rangeFind), []string{"num"})
}

func TestServerFindIndexedRangeSkipsNullMissingAndWrongTypePredicates(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 265, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 266, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "num"}, {Key: "age", Value: int64(10)}},
			bson.D{{Key: "_id", Value: "null"}, {Key: "age", Value: nil}},
			bson.D{{Key: "_id", Value: "missing"}, {Key: "name", Value: "no age"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	rangeFind := serveCommand(t, server, 267, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(5)}}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, rangeFind), []string{"num"})

	decimalTen, err := bson.ParseDecimal128("10")
	if err != nil {
		t.Fatalf("parse decimal: %v", err)
	}
	decimalRangeFind := serveCommand(t, server, 268, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: decimalTen}}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, decimalRangeFind), []string{"num"})

	wrongTypeRangeFind := serveCommand(t, server, 268, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: "5"}}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, wrongTypeRangeFind), nil)
}

func TestServerFindIndexedRangeDecimal128FractionFallsBackToScan(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 10
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 269, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 270, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "ten"}, {Key: "age", Value: int64(10)}},
			bson.D{{Key: "_id", Value: "eleven"}, {Key: "age", Value: int64(11)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	decimal, err := bson.ParseDecimal128("10.5")
	if err != nil {
		t.Fatalf("parse decimal: %v", err)
	}
	rangeFind := serveCommand(t, server, 271, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: decimal}}}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, rangeFind), []string{"eleven"})
}

func TestServerFindIndexedRangeUnusableBoundUsesUnsortedEarlyStop(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 272, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 273, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a-match"}, {Key: "age", Value: int64(11)}},
			bson.D{{Key: "_id", Value: "z-other"}, {Key: "age", Value: int64(10)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	decimal, err := bson.ParseDecimal128("10.5")
	if err != nil {
		t.Fatalf("parse decimal: %v", err)
	}
	rangeFind := serveCommand(t, server, 274, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: decimal}}}}},
		{Key: "limit", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, rangeFind), []string{"a-match"})
}

func TestServerFindIndexedRangeNullFallsBackToScan(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.MaxFindScanDocuments = 10
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 272, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}},
			{Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 273, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "null"}, {Key: "age", Value: nil}},
			bson.D{{Key: "_id", Value: "num"}, {Key: "age", Value: int64(10)}},
			bson.D{{Key: "_id", Value: "missing"}, {Key: "name", Value: "no age"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	rangeFind := serveCommand(t, server, 274, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: nil}}}}},
		{Key: "sort", Value: bson.D{{Key: "age", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, rangeFind), []string{"null"})
}

func TestCompareRawNumbersHandlesNonFiniteDoubles(t *testing.T) {
	decimal, err := bson.ParseDecimal128("1.50")
	if err != nil {
		t.Fatalf("parse decimal: %v", err)
	}
	decimalInt, err := bson.ParseDecimal128("37")
	if err != nil {
		t.Fatalf("parse decimal int: %v", err)
	}
	decimalTenth, err := bson.ParseDecimal128("0.1")
	if err != nil {
		t.Fatalf("parse decimal tenth: %v", err)
	}
	raw := bson.Raw(mustDocument(t, bson.D{
		{Key: "nan", Value: math.NaN()},
		{Key: "pos_inf", Value: math.Inf(1)},
		{Key: "neg_inf", Value: math.Inf(-1)},
		{Key: "finite", Value: 1.5},
		{Key: "decimal", Value: decimal},
		{Key: "decimal_int", Value: decimalInt},
		{Key: "decimal_tenth", Value: decimalTenth},
		{Key: "large_int", Value: int64(9007199254740993)},
		{Key: "double_int", Value: 37.0},
		{Key: "double_fraction", Value: 37.5},
	}))
	nanValue := raw.Lookup("nan")
	posInf := raw.Lookup("pos_inf")
	negInf := raw.Lookup("neg_inf")
	finite := raw.Lookup("finite")
	decimalValue := raw.Lookup("decimal")
	decimalIntValue := raw.Lookup("decimal_int")
	decimalTenthValue := raw.Lookup("decimal_tenth")
	largeInt := raw.Lookup("large_int")
	doubleInt := raw.Lookup("double_int")
	doubleFraction := raw.Lookup("double_fraction")

	if rawValuesEqual(nanValue, finite) {
		t.Fatal("NaN compared equal to finite number")
	}
	if match, err := valueMatchesPredicate(nanValue, findPredicate{op: findPredicateGT, values: []bson.RawValue{finite}}); err != nil || match {
		t.Fatalf("NaN range match/err=%v/%v want false/nil", match, err)
	}
	if scalar, ok := indexScalarForBSONValue(nanValue, collections.IndexValueDouble); ok {
		t.Fatalf("NaN double scalar=%v ok=%v want not indexable", scalar, ok)
	}
	if cmp := compareRawValues(posInf, finite); cmp <= 0 {
		t.Fatalf("+Inf vs finite cmp=%d want >0", cmp)
	}
	if cmp := compareRawValues(negInf, finite); cmp >= 0 {
		t.Fatalf("-Inf vs finite cmp=%d want <0", cmp)
	}
	if cmp := compareRawValues(decimalValue, finite); cmp != 0 {
		t.Fatalf("Decimal128 vs double cmp=%d want 0", cmp)
	}
	scalar, ok := indexScalarForBSONValue(largeInt, collections.IndexValueInt64)
	if !ok || scalar != int64(9007199254740993) {
		t.Fatalf("large int scalar=%v ok=%v want int64", scalar, ok)
	}
	scalar, ok = indexScalarForBSONValue(doubleInt, collections.IndexValueInt64)
	if !ok || scalar != int64(37) {
		t.Fatalf("double int scalar=%v ok=%v want int64(37)", scalar, ok)
	}
	if scalar, ok = indexScalarForBSONValue(doubleFraction, collections.IndexValueInt64); ok {
		t.Fatalf("fractional double int64 scalar=%v ok=%v want not indexable", scalar, ok)
	}
	if scalar, ok = indexScalarForBSONValue(posInf, collections.IndexValueInt64); ok {
		t.Fatalf("+Inf int64 scalar=%v ok=%v want not indexable", scalar, ok)
	}
	scalar, ok = indexScalarForBSONValue(decimalIntValue, collections.IndexValueInt64)
	if !ok || scalar != int64(37) {
		t.Fatalf("decimal int64 scalar=%v ok=%v want int64(37)", scalar, ok)
	}
	scalar, ok = indexScalarForBSONValue(decimalValue, collections.IndexValueDouble)
	if !ok || scalar != float64(1.5) {
		t.Fatalf("decimal double scalar=%v ok=%v want float64(1.5)", scalar, ok)
	}
	if scalar, ok = indexScalarForBSONValue(decimalTenthValue, collections.IndexValueDouble); ok {
		t.Fatalf("non-exact decimal double scalar=%v ok=%v want not indexable", scalar, ok)
	}
}

func TestIndexRangeOptionsForPredicatesCombinesTypedBounds(t *testing.T) {
	idx := collections.IndexDefinition{Name: "age_1", Field: "age", ValueType: collections.IndexValueInt64}
	decimalFifteen, err := bson.ParseDecimal128("15")
	if err != nil {
		t.Fatalf("parse decimal: %v", err)
	}
	decimalFraction, err := bson.ParseDecimal128("15.5")
	if err != nil {
		t.Fatalf("parse decimal fraction: %v", err)
	}
	opts, ok, empty, err := indexRangeOptionsForPredicates([]findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int32(10))}},
		{field: "age", op: findPredicateGT, values: []bson.RawValue{mustRawValue(t, int64(10))}},
		{field: "age", op: findPredicateLT, values: []bson.RawValue{mustRawValue(t, int64(30))}},
		{field: "age", op: findPredicateLTE, values: []bson.RawValue{mustRawValue(t, int64(20))}},
	}, idx)
	if err != nil {
		t.Fatalf("range options: %v", err)
	}
	if !ok || empty {
		t.Fatalf("range options ok=%v empty=%v want true/false", ok, empty)
	}
	if opts.Lower.Value != int64(10) || opts.Lower.Inclusive || opts.Lower.Unbounded {
		t.Fatalf("lower bound=%+v want exclusive int64(10)", opts.Lower)
	}
	if opts.Upper.Value != int64(20) || !opts.Upper.Inclusive || opts.Upper.Unbounded {
		t.Fatalf("upper bound=%+v want inclusive int64(20)", opts.Upper)
	}

	opts, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, decimalFifteen)}},
	}, idx)
	if err != nil {
		t.Fatalf("decimal int64 range options: %v", err)
	}
	if !ok || empty {
		t.Fatalf("decimal int64 range ok=%v empty=%v want true/false", ok, empty)
	}
	if opts.Lower.Value != int64(15) || !opts.Lower.Inclusive || opts.Lower.Unbounded {
		t.Fatalf("decimal int64 lower bound=%+v want inclusive int64(15)", opts.Lower)
	}

	_, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, decimalFraction)}},
	}, idx)
	if err != nil {
		t.Fatalf("fractional decimal int64 range options: %v", err)
	}
	if ok || empty {
		t.Fatalf("fractional decimal int64 range ok=%v empty=%v want fallback false/false", ok, empty)
	}

	_, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{{Type: bson.TypeNull}}},
	}, idx)
	if err != nil {
		t.Fatalf("null int64 range options: %v", err)
	}
	if ok || empty {
		t.Fatalf("null int64 range ok=%v empty=%v want fallback false/false", ok, empty)
	}

	_, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "age", op: findPredicateGT, values: []bson.RawValue{mustRawValue(t, int64(10))}},
		{field: "age", op: findPredicateLTE, values: []bson.RawValue{mustRawValue(t, int64(10))}},
	}, idx)
	if err != nil {
		t.Fatalf("contradictory range options: %v", err)
	}
	if !ok || !empty {
		t.Fatalf("contradictory range ok=%v empty=%v want true/true", ok, empty)
	}

	_, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, "40")}},
	}, idx)
	if err != nil {
		t.Fatalf("wrong-type range options: %v", err)
	}
	if !ok || !empty {
		t.Fatalf("wrong-type range ok=%v empty=%v want true/true", ok, empty)
	}
}

func TestFindPlanHasDirectCandidateRequiresUsableRangeIndex(t *testing.T) {
	decimalFraction, err := bson.ParseDecimal128("15.5")
	if err != nil {
		t.Fatalf("parse decimal fraction: %v", err)
	}
	meta := collections.CollectionMeta{Indexes: []collections.IndexDefinition{
		{Name: "age_1", Field: "age", ValueType: collections.IndexValueInt64},
	}}
	if !findPlanHasDirectCandidate(meta, []findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(10))}},
	}) {
		t.Fatal("usable int64 range should be a direct candidate")
	}
	if findPlanHasDirectCandidate(meta, []findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, decimalFraction)}},
	}) {
		t.Fatal("fractional decimal int64 range should use scan fallback, not direct candidate mode")
	}
	if findPlanHasDirectCandidate(meta, []findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{{Type: bson.TypeNull}}},
	}) {
		t.Fatal("null int64 range should use scan fallback, not direct candidate mode")
	}
	if !findPlanHasDirectCandidate(meta, []findPredicate{
		{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, "10")}},
	}) {
		t.Fatal("wrong-type int64 range should use direct empty-candidate mode")
	}
}

func TestSimplePrimaryEqualityFindValue(t *testing.T) {
	value := mustRawValue(t, "user1")
	got, ok := simplePrimaryEqualityFindValue(findPlan{
		predicates: []findPredicate{{field: "_id", op: findPredicateEq, values: []bson.RawValue{value}}},
		projection: compiledProjection{present: true},
		limit:      1,
	})
	if !ok || got.Type != value.Type || !bytes.Equal(got.Value, value.Value) {
		t.Fatalf("simple primary value ok=%v got=%v want %v", ok, got, value)
	}
	if _, ok := simplePrimaryEqualityFindValue(findPlan{
		predicates: []findPredicate{{field: "_id", op: findPredicateEq, values: []bson.RawValue{value}}},
		sort:       findSort{field: "name"},
		limit:      1,
	}); ok {
		t.Fatal("sorted primary find should not use simple path")
	}
	if _, ok := simplePrimaryEqualityFindValue(findPlan{
		predicates: []findPredicate{{field: "_id", op: findPredicateEq, values: []bson.RawValue{value}}},
		skip:       1,
		limit:      1,
	}); ok {
		t.Fatal("skipped primary find should not use simple path")
	}
	if _, ok := simplePrimaryEqualityFindValue(findPlan{
		predicates: []findPredicate{{field: "_id", op: findPredicateEq, values: []bson.RawValue{value}}},
		limit:      2,
	}); ok {
		t.Fatal("multi-limit primary find should not use simple path")
	}
	if _, ok := simplePrimaryEqualityFindValue(findPlan{
		predicates: []findPredicate{{field: "_id", op: findPredicateIn, values: []bson.RawValue{value}}},
		limit:      1,
	}); ok {
		t.Fatal("$in primary find should not use simple path")
	}
	if _, ok := simplePrimaryEqualityFindValue(findPlan{
		predicates: []findPredicate{
			{field: "_id", op: findPredicateEq, values: []bson.RawValue{value}},
			{field: "field0", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, []byte("v"))}},
		},
		limit: 1,
	}); ok {
		t.Fatal("compound predicate primary find should not use simple path")
	}
}

func TestIndexRangeOptionsForPredicatesHandlesDoubleBounds(t *testing.T) {
	idx := collections.IndexDefinition{Name: "score_1", Field: "score", ValueType: collections.IndexValueDouble}
	decimalOnePointFive, err := bson.ParseDecimal128("1.5")
	if err != nil {
		t.Fatalf("parse decimal: %v", err)
	}
	decimalTenth, err := bson.ParseDecimal128("0.1")
	if err != nil {
		t.Fatalf("parse decimal tenth: %v", err)
	}
	opts, ok, empty, err := indexRangeOptionsForPredicates([]findPredicate{
		{field: "score", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(4))}},
		{field: "score", op: findPredicateLT, values: []bson.RawValue{mustRawValue(t, 5.5)}},
	}, idx)
	if err != nil {
		t.Fatalf("double range options: %v", err)
	}
	if !ok || empty {
		t.Fatalf("double range ok=%v empty=%v want true/false", ok, empty)
	}
	if opts.Lower.Value != float64(4) || !opts.Lower.Inclusive || opts.Lower.Unbounded {
		t.Fatalf("double lower bound=%+v want inclusive float64(4)", opts.Lower)
	}
	if opts.Upper.Value != float64(5.5) || opts.Upper.Inclusive || opts.Upper.Unbounded {
		t.Fatalf("double upper bound=%+v want exclusive float64(5.5)", opts.Upper)
	}

	opts, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "score", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, decimalOnePointFive)}},
	}, idx)
	if err != nil {
		t.Fatalf("decimal double range options: %v", err)
	}
	if !ok || empty {
		t.Fatalf("decimal double range ok=%v empty=%v want true/false", ok, empty)
	}
	if opts.Lower.Value != float64(1.5) || !opts.Lower.Inclusive || opts.Lower.Unbounded {
		t.Fatalf("decimal double lower bound=%+v want inclusive float64(1.5)", opts.Lower)
	}

	_, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "score", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, decimalTenth)}},
	}, idx)
	if err != nil {
		t.Fatalf("non-exact decimal double range options: %v", err)
	}
	if ok || empty {
		t.Fatalf("non-exact decimal double range ok=%v empty=%v want fallback false/false", ok, empty)
	}

	_, ok, empty, err = indexRangeOptionsForPredicates([]findPredicate{
		{field: "score", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, math.NaN())}},
	}, idx)
	if err != nil {
		t.Fatalf("NaN double range options: %v", err)
	}
	if !ok || !empty {
		t.Fatalf("NaN double range ok=%v empty=%v want true/true", ok, empty)
	}
}

func TestIndexedRangeCandidateLimitOnlyForPureSameFieldRange(t *testing.T) {
	idx := collections.IndexDefinition{Name: "age_1", Field: "age", ValueType: collections.IndexValueInt64}
	limit, ok := indexedRangeCandidateLimit(findPlan{
		predicates: []findPredicate{
			{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
			{field: "age", op: findPredicateLT, values: []bson.RawValue{mustRawValue(t, int64(50))}},
		},
		skip:  3,
		limit: 10,
	}, idx, 100)
	if !ok || limit != 13 {
		t.Fatalf("pure range candidate limit=%d ok=%v want 13,true", limit, ok)
	}

	_, ok = indexedRangeCandidateLimit(findPlan{
		predicates: []findPredicate{
			{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
			{field: "city", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, "hnl")}},
		},
		limit: 10,
	}, idx, 100)
	if ok {
		t.Fatal("mixed predicate range candidate should not use page limit")
	}

	_, ok = indexedRangeCandidateLimit(findPlan{
		predicates: []findPredicate{
			{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
		},
		sort:  findSort{field: "name"},
		limit: 10,
	}, idx, 100)
	if ok {
		t.Fatal("other-field sort range candidate should not use page limit")
	}

	const maxInt32 = int32(1<<31 - 1)
	limit, ok = indexedRangeCandidateLimit(findPlan{
		predicates: []findPredicate{
			{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
		},
		skip:  maxInt32,
		limit: maxInt32,
	}, idx, 100)
	if !ok || limit != 101 {
		t.Fatalf("overflow-prone range candidate limit=%d ok=%v want 101,true", limit, ok)
	}

	maxInt := int(^uint(0) >> 1)
	if got := candidateLimitWithOverflowSlot(maxInt); got != maxInt {
		t.Fatalf("candidateLimitWithOverflowSlot(maxInt)=%d want %d", got, maxInt)
	}
	if got := candidateLimitWithOverflowSlot(100); got != 101 {
		t.Fatalf("candidateLimitWithOverflowSlot(100)=%d want 101", got)
	}
}

func TestPureIndexedRangeLimitPlanOnlyForSingleRangeNoSkip(t *testing.T) {
	meta := collections.CollectionMeta{Indexes: []collections.IndexDefinition{
		{Name: "age_1", Field: "age", ValueType: collections.IndexValueInt64},
	}}
	_, opts, limit, ok, empty, err := pureIndexedRangeLimitPlan(meta, findPlan{
		predicates: []findPredicate{
			{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
		},
		limit: 10,
	}, 100)
	if err != nil {
		t.Fatalf("pure indexed range plan: %v", err)
	}
	if !ok || empty || limit != 10 || opts.Limit != 0 {
		t.Fatalf("pure indexed range plan ok=%v empty=%v limit=%d opts.Limit=%d want true,false,10,0", ok, empty, limit, opts.Limit)
	}

	_, _, _, ok, _, err = pureIndexedRangeLimitPlan(meta, findPlan{
		predicates: []findPredicate{
			{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
		},
		skip:  1,
		limit: 10,
	}, 100)
	if err != nil {
		t.Fatalf("skipped pure indexed range plan: %v", err)
	}
	if ok {
		t.Fatal("pure indexed range plan accepted skip")
	}

	_, _, _, ok, _, err = pureIndexedRangeLimitPlan(meta, findPlan{
		predicates: []findPredicate{
			{field: "age", op: findPredicateGTE, values: []bson.RawValue{mustRawValue(t, int64(40))}},
			{field: "city", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, "hnl")}},
		},
		limit: 10,
	}, 100)
	if err != nil {
		t.Fatalf("mixed pure indexed range plan: %v", err)
	}
	if ok {
		t.Fatal("pure indexed range plan accepted mixed predicates")
	}
}

func TestApplySetUpdateAppendsNewFieldsInSetOrder(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "name", Value: "ada"},
	})
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	update, err := bson.Marshal(bson.D{{Key: "$set", Value: bson.D{
		{Key: "zeta", Value: int32(1)},
		{Key: "alpha", Value: int32(2)},
	}}})
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}
	updated, changed, err := applySetUpdate(wire.Document(doc), wire.Document(update))
	if err != nil {
		t.Fatalf("apply set: %v", err)
	}
	if !changed {
		t.Fatal("apply set changed=false want true")
	}
	elements, err := bson.Raw(updated).Elements()
	if err != nil {
		t.Fatalf("updated elements: %v", err)
	}
	keys := make([]string, 0, len(elements))
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			t.Fatalf("element key: %v", err)
		}
		keys = append(keys, key)
	}
	want := []string{"_id", "name", "zeta", "alpha"}
	if len(keys) != len(want) {
		t.Fatalf("keys=%v want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys=%v want %v", keys, want)
		}
	}
}

func TestCompareDocumentFieldTreatsMissingAsNull(t *testing.T) {
	missing := mustDocument(t, bson.D{{Key: "_id", Value: "missing"}})
	nullValue := mustDocument(t, bson.D{{Key: "_id", Value: "null"}, {Key: "rank", Value: nil}})
	numberValue := mustDocument(t, bson.D{{Key: "_id", Value: "number"}, {Key: "rank", Value: int64(1)}})
	if cmp := compareDocumentField(missing, nullValue, "rank"); cmp != 0 {
		t.Fatalf("missing vs null cmp=%d want 0", cmp)
	}
	if cmp := compareDocumentField(missing, numberValue, "rank"); cmp >= 0 {
		t.Fatalf("missing vs number cmp=%d want <0", cmp)
	}
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
	if _, err := server.Collections.OpenCollection("app.events"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("failed insert collection err=%v, want collection not found", err)
	}
}

func TestServerAppliesDefaultCollectionAndIndexOptions(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat:                   collections.DocumentFormatTemplateV1,
		DataRootStoragePolicy:            collections.RootStorageCompressed,
		IndexStateStoragePolicy:          collections.RootStorageCompressed,
		BufferedIndexedWriteMaxDocuments: 1234,
		BufferedIndexedWriteMaxBytes:     5678,
		BufferedIndexedWriteMaxRootRuns:  90,
	}
	server.DefaultIndexStoragePolicy = collections.RootStorageCompressed

	col, err := server.openOrCreateCollection("app.inserted")
	if err != nil {
		t.Fatalf("openOrCreateCollection: %v", err)
	}
	meta := col.Meta()
	if meta.Options.DocumentFormat != collections.DocumentFormatTemplateV1 {
		t.Fatalf("document format=%q want %q", meta.Options.DocumentFormat, collections.DocumentFormatTemplateV1)
	}
	if meta.Options.DataRootStoragePolicy != collections.RootStorageCompressed {
		t.Fatalf("data root storage=%q want %q", meta.Options.DataRootStoragePolicy, collections.RootStorageCompressed)
	}
	if meta.Options.IndexStateStoragePolicy != collections.RootStorageCompressed {
		t.Fatalf("index state storage=%q want %q", meta.Options.IndexStateStoragePolicy, collections.RootStorageCompressed)
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments != 1234 || meta.Options.BufferedIndexedWriteMaxBytes != 5678 || meta.Options.BufferedIndexedWriteMaxRootRuns != 90 {
		t.Fatalf("no-index defaults docs=%d bytes=%d rootRuns=%d want 1234/5678/90",
			meta.Options.BufferedIndexedWriteMaxDocuments, meta.Options.BufferedIndexedWriteMaxBytes, meta.Options.BufferedIndexedWriteMaxRootRuns)
	}

	commandDoc := mustDocument(t, bson.D{
		{Key: "createIndexes", Value: "indexed"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"},
			{Key: "unique", Value: true},
		}}},
		{Key: "$db", Value: "app"},
	})
	req, err := wire.AppendMsgMessage(nil, 214, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}
	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}
	resp := readMsgResponse(t, rw.w.Bytes(), 214)
	assertOK(t, resp)

	indexed, err := server.Collections.OpenCollection("app.indexed")
	if err != nil {
		t.Fatalf("open indexed collection: %v", err)
	}
	indexedMeta := indexed.Meta()
	if indexedMeta.Options.DocumentFormat != collections.DocumentFormatTemplateV1 {
		t.Fatalf("auto-created document format=%q want %q", indexedMeta.Options.DocumentFormat, collections.DocumentFormatTemplateV1)
	}
	if indexedMeta.Options.BufferedIndexedWriteMaxDocuments != 1234 {
		t.Fatalf("auto-created buffered indexed max documents=%d want 1234", indexedMeta.Options.BufferedIndexedWriteMaxDocuments)
	}
	if indexedMeta.Options.BufferedIndexedWriteMaxBytes != 5678 {
		t.Fatalf("auto-created buffered indexed max bytes=%d want 5678", indexedMeta.Options.BufferedIndexedWriteMaxBytes)
	}
	if indexedMeta.Options.BufferedIndexedWriteMaxRootRuns != 90 {
		t.Fatalf("auto-created buffered indexed max root runs=%d want 90", indexedMeta.Options.BufferedIndexedWriteMaxRootRuns)
	}
	def, ok := findIndexDefinition(indexedMeta.Indexes, "email_1")
	if !ok {
		t.Fatalf("email_1 index missing from %+v", indexedMeta.Indexes)
	}
	if def.StoragePolicy != collections.RootStorageCompressed {
		t.Fatalf("index storage=%q want %q", def.StoragePolicy, collections.RootStorageCompressed)
	}

	existingCreateResponse := serveCommand(t, server, 215, bson.D{
		{Key: "createIndexes", Value: "inserted"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"},
			{Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, existingCreateResponse)
	inserted, err := server.Collections.OpenCollection("app.inserted")
	if err != nil {
		t.Fatalf("open inserted collection after createIndexes: %v", err)
	}
	insertedMeta := inserted.Meta()
	if !insertedMeta.Options.BufferedIndexedWrites {
		t.Fatal("existing collection did not enable indexed writes after createIndexes")
	}
	if insertedMeta.Options.BufferedIndexedWriteMaxDocuments != 1234 || insertedMeta.Options.BufferedIndexedWriteMaxBytes != 5678 || insertedMeta.Options.BufferedIndexedWriteMaxRootRuns != 90 {
		t.Fatalf("existing collection buffered indexed limits docs=%d bytes=%d rootRuns=%d want 1234/5678/90",
			insertedMeta.Options.BufferedIndexedWriteMaxDocuments, insertedMeta.Options.BufferedIndexedWriteMaxBytes, insertedMeta.Options.BufferedIndexedWriteMaxRootRuns)
	}
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

func TestServeConnBufferedMessageCoalescingAppendsResponses(t *testing.T) {
	firstCommand := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	firstReq, err := wire.AppendMsgMessage(nil, 401, 0, 0, firstCommand)
	if err != nil {
		t.Fatalf("AppendMsgMessage first: %v", err)
	}
	secondCommand := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	secondReq, err := wire.AppendMsgMessage(nil, 402, 0, 0, secondCommand)
	if err != nil {
		t.Fatalf("AppendMsgMessage second: %v", err)
	}
	requests := append(firstReq, secondReq...)
	reader := bufio.NewReaderSize(bytes.NewReader(requests), len(requests))
	if _, err := reader.Peek(len(requests)); err != nil {
		t.Fatalf("prime buffered reader: %v", err)
	}

	server := NewServer()
	var writeBuf []byte
	writeBuf, appended, err := server.appendBufferedMessageWithOwner(reader, 1, writeBuf)
	if err != nil {
		t.Fatalf("append first buffered message: %v", err)
	}
	if !appended {
		t.Fatal("first buffered message was not appended")
	}
	writeBuf, appended, err = server.appendBufferedMessageWithOwner(reader, 1, writeBuf)
	if err != nil {
		t.Fatalf("append second buffered message: %v", err)
	}
	if !appended {
		t.Fatal("second buffered message was not appended")
	}
	if reader.Buffered() != 0 {
		t.Fatalf("reader buffered bytes=%d want 0", reader.Buffered())
	}

	responseReader := bytes.NewReader(writeBuf)
	firstHeader, firstBody, err := wire.ReadMessage(responseReader, 0)
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if firstHeader.OpCode != wire.OpMsg || firstHeader.ResponseTo != 401 {
		t.Fatalf("first response header=%+v", firstHeader)
	}
	firstMsg, err := wire.ParseMsg(firstBody)
	if err != nil {
		t.Fatalf("parse first response: %v", err)
	}
	assertOK(t, firstMsg.Body)
	secondHeader, secondBody, err := wire.ReadMessage(responseReader, 0)
	if err != nil {
		t.Fatalf("read second response: %v", err)
	}
	if secondHeader.OpCode != wire.OpMsg || secondHeader.ResponseTo != 402 {
		t.Fatalf("second response header=%+v", secondHeader)
	}
	secondMsg, err := wire.ParseMsg(secondBody)
	if err != nil {
		t.Fatalf("parse second response: %v", err)
	}
	assertOK(t, secondMsg.Body)
	if responseReader.Len() != 0 {
		t.Fatalf("trailing response bytes=%d", responseReader.Len())
	}
}

func TestServeConnBufferedMessageRejectsShortMessageLength(t *testing.T) {
	req := wire.AppendHeader(nil, wire.Header{
		MessageLength: wire.HeaderLen - 1,
		RequestID:     401,
		OpCode:        wire.OpMsg,
	})
	reader := bufio.NewReaderSize(bytes.NewReader(req), len(req))
	if _, err := reader.Peek(len(req)); err != nil {
		t.Fatalf("prime buffered reader: %v", err)
	}

	_, appended, err := NewServer().appendBufferedMessageWithOwner(reader, 1, nil)
	if !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("append buffered message err=%v want ErrMalformed", err)
	}
	if appended {
		t.Fatal("malformed buffered message was appended")
	}
}

func TestServeConnFlushesBufferedResponseBeforeCoalescedError(t *testing.T) {
	firstCommand := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	firstReq, err := wire.AppendMsgMessage(nil, 501, 0, 0, firstCommand)
	if err != nil {
		t.Fatalf("AppendMsgMessage first: %v", err)
	}
	badReq, err := wire.AppendMessage(nil, 502, 0, wire.OpCompressed, nil)
	if err != nil {
		t.Fatalf("AppendMessage bad: %v", err)
	}
	requests := append(firstReq, badReq...)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	if err := clientConn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- NewServer().ServeConn(context.Background(), serverConn)
	}()
	writeErrCh := make(chan error, 1)
	go func() {
		writeErrCh <- writeFull(clientConn, requests)
	}()

	header, body, err := wire.ReadMessage(clientConn, 0)
	if err != nil {
		t.Fatalf("read flushed response: %v", err)
	}
	if header.OpCode != wire.OpMsg || header.ResponseTo != 501 {
		t.Fatalf("response header=%+v", header)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	assertOK(t, msg.Body)

	select {
	case err := <-writeErrCh:
		if err != nil {
			t.Fatalf("write requests: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request write did not finish")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, wire.ErrUnsupported) {
			t.Fatalf("ServeConn err=%v want ErrUnsupported", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not return after coalesced error")
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

func TestServeConnContextErrorMapsCanceledTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := serveConnContextError(ctx, timeoutNetError{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("serveConnContextError canceled timeout=%v want context.Canceled", err)
	}
	if err := serveConnContextError(context.Background(), timeoutNetError{}); err != nil {
		t.Fatalf("serveConnContextError active context=%v want nil", err)
	}
}

func TestServerCloseInterruptsServeConnRead(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	server := NewServer()
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeConn(context.Background(), serverConn)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		server.connMu.Lock()
		registered := len(server.conns) == 1
		server.connMu.Unlock()
		if registered {
			break
		}
		time.Sleep(time.Millisecond)
	}
	server.connMu.Lock()
	registered := len(server.conns) == 1
	server.connMu.Unlock()
	if !registered {
		t.Fatal("ServeConn did not register connection")
	}

	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, errServerClosed) {
			t.Fatalf("ServeConn err=%v want %v", err, errServerClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not return after server close")
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

func mustRawValue(tb testing.TB, value any) bson.RawValue {
	tb.Helper()
	valueType, raw, err := bson.MarshalValue(value)
	if err != nil {
		tb.Fatalf("marshal BSON value: %v", err)
	}
	return bson.RawValue{Type: valueType, Value: raw}
}

type commandResult struct {
	doc wire.Document
	err error
}

func serveCommand(tb testing.TB, server *Server, requestID int32, doc bson.D) wire.Document {
	tb.Helper()
	response, err := serveCommandResult(server, requestID, doc)
	if err != nil {
		tb.Fatalf("serve command: %v", err)
	}
	return response
}

func serveCommandResult(server *Server, requestID int32, doc bson.D) (wire.Document, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal BSON document: %w", err)
	}
	commandDoc := wire.Document(raw)
	req, err := wire.AppendMsgMessage(nil, requestID, 0, 0, commandDoc)
	if err != nil {
		return nil, fmt.Errorf("AppendMsgMessage: %w", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}
	if err := server.ServeOneWithOwner(rw, 1); err != nil {
		return nil, fmt.Errorf("ServeOneWithOwner: %w", err)
	}
	return readMsgResponseResult(rw.w.Bytes(), requestID)
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
	doc, err := readMsgResponseResult(response, responseTo)
	if err != nil {
		tb.Fatalf("read msg response: %v", err)
	}
	return doc
}

func readMsgResponseResult(response []byte, responseTo int32) (wire.Document, error) {
	h, body, err := wire.ReadMessage(bytes.NewReader(response), 0)
	if err != nil {
		return nil, fmt.Errorf("ReadMessage response: %w", err)
	}
	if h.OpCode != wire.OpMsg || h.ResponseTo != responseTo {
		return nil, fmt.Errorf("response header=%+v want OP_MSG responseTo=%d", h, responseTo)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return nil, fmt.Errorf("ParseMsg response: %w", err)
	}
	return msg.Body, nil
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

func cursorNextBatch(tb testing.TB, doc wire.Document) []bson.Raw {
	tb.Helper()
	cursor, ok := bson.Raw(doc).Lookup("cursor").DocumentOK()
	if !ok {
		tb.Fatalf("cursor missing or not document in %v", bson.Raw(doc))
	}
	batch, ok := cursor.Lookup("nextBatch").ArrayOK()
	if !ok {
		tb.Fatalf("cursor.nextBatch missing or not array in %v", cursor)
	}
	values, err := batch.Values()
	if err != nil {
		tb.Fatalf("nextBatch values: %v", err)
	}
	out := make([]bson.Raw, 0, len(values))
	for i, value := range values {
		doc, ok := value.DocumentOK()
		if !ok {
			tb.Fatalf("nextBatch[%d] is not a document", i)
		}
		out = append(out, doc)
	}
	return out
}

func cursorIDFromResponse(tb testing.TB, doc wire.Document) int64 {
	tb.Helper()
	cursor, ok := bson.Raw(doc).Lookup("cursor").DocumentOK()
	if !ok {
		tb.Fatalf("cursor missing or not document in %v", bson.Raw(doc))
	}
	id, ok := cursor.Lookup("id").Int64OK()
	if !ok {
		tb.Fatalf("cursor.id missing or not int64 in %v", cursor)
	}
	return id
}

func assertBatchIDs(tb testing.TB, batch []bson.Raw, want []string) {
	tb.Helper()
	if len(batch) != len(want) {
		tb.Fatalf("batch len=%d want %d", len(batch), len(want))
	}
	for i, doc := range batch {
		got, ok := doc.Lookup("_id").StringValueOK()
		if !ok || got != want[i] {
			tb.Fatalf("batch[%d]._id=%q ok=%v want %q", i, got, ok, want[i])
		}
	}
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
