package mongogateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
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
	assertInt32(t, reply.Documents[0], "logicalSessionTimeoutMinutes", mongoGatewayCapabilityManifest.Advertised.LogicalSessionTimeoutMinutes)
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
	readBuf, _, err := server.serveOneWithOwner(context.Background(), &readWriter{r: bytes.NewReader(req)}, 1, make([]byte, 0, len(req)), nil)
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
	readBuf, _, err := server.serveOneWithOwner(context.Background(), rw, 1, make([]byte, 0, len(req)), nil)
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

func TestMongoReadConcernAcceptsLocalStaleReadSurfaces(t *testing.T) {
	server := newMongoReadConcernTestServer(t)

	findCases := []struct {
		name        string
		present     bool
		readConcern bson.D
	}{
		{name: "absent"},
		{name: "empty", present: true, readConcern: bson.D{}},
		{name: "local", present: true, readConcern: bson.D{{Key: "level", Value: "local"}}},
		{name: "available", present: true, readConcern: bson.D{{Key: "level", Value: "available"}}},
	}
	for _, tc := range findCases {
		t.Run(tc.name, func(t *testing.T) {
			command := bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
			}
			if tc.present {
				command = append(command, bson.E{Key: "readConcern", Value: tc.readConcern})
			}
			command = append(command, bson.E{Key: "$db", Value: "app"})
			assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 330400, command)), []string{"u1"})
		})
	}

	listCollections := serveCommand(t, server, 330401, bson.D{
		{Key: "listCollections", Value: int32(1)},
		{Key: "nameOnly", Value: true},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}},
		{Key: "$db", Value: "app"},
	})
	if batch := cursorFirstBatch(t, listCollections); len(batch) != 1 {
		t.Fatalf("listCollections batch len=%d want 1", len(batch))
	}

	listDatabases := serveCommand(t, server, 330402, bson.D{
		{Key: "listDatabases", Value: int32(1)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "available"}}},
		{Key: "$db", Value: "admin"},
	})
	assertOK(t, listDatabases)

	listIndexes := serveCommand(t, server, 330403, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}},
		{Key: "$db", Value: "app"},
	})
	assertIndexNameSet(t, cursorFirstBatch(t, listIndexes), []string{"_id_", "city_1"})

	find := serveCommand(t, server, 330404, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	cursorID := cursorIDFromResponse(t, find)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}
	getMore := serveCommand(t, server, 330405, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "batchSize", Value: int32(1)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "available"}}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorNextBatch(t, getMore), []string{"u2"})
}

func TestMongoReadConcernRejectsStrongLevelsBeforeServingData(t *testing.T) {
	server := newMongoReadConcernTestServer(t)

	for _, level := range []string{"majority", "linearizable", "snapshot"} {
		t.Run("find_"+level, func(t *testing.T) {
			resp := serveCommand(t, server, 330410, bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "readConcern", Value: bson.D{{Key: "level", Value: level}}},
				{Key: "$db", Value: "app"},
			})
			assertCommandError(t, resp, "BadValue")
		})
	}

	listCollections := serveCommand(t, server, 330411, bson.D{
		{Key: "listCollections", Value: int32(1)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, listCollections, "BadValue")

	listDatabases := serveCommand(t, server, 330412, bson.D{
		{Key: "listDatabases", Value: int32(1)},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "snapshot"}}},
		{Key: "$db", Value: "admin"},
	})
	assertCommandError(t, listDatabases, "BadValue")

	listIndexes := serveCommand(t, server, 330413, bson.D{
		{Key: "listIndexes", Value: "users"},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "linearizable"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, listIndexes, "BadValue")

	find := serveCommand(t, server, 330414, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	cursorID := cursorIDFromResponse(t, find)
	if cursorID == 0 {
		t.Fatal("cursor id=0 want open cursor")
	}
	rejectedGetMore := serveCommand(t, server, 330415, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, rejectedGetMore, "BadValue")

	next := serveCommand(t, server, 330416, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "batchSize", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorNextBatch(t, next), []string{"u2"})
}

func TestMongoReadConcernRejectsMalformedDocumentsAndUnsupportedOptions(t *testing.T) {
	server := newMongoReadConcernTestServer(t)

	cases := []struct {
		name        string
		readConcern any
		codeName    string
	}{
		{name: "non_document", readConcern: "local", codeName: "FailedToParse"},
		{name: "bad_level_type", readConcern: bson.D{{Key: "level", Value: int32(1)}}, codeName: "FailedToParse"},
		{name: "unknown_option", readConcern: bson.D{{Key: "level", Value: "local"}, {Key: "foo", Value: true}}, codeName: "BadValue"},
		{name: "after_cluster_time", readConcern: bson.D{{Key: "afterClusterTime", Value: int64(42)}}, codeName: "BadValue"},
		{name: "at_cluster_time", readConcern: bson.D{{Key: "atClusterTime", Value: int64(42)}}, codeName: "BadValue"},
		{name: "duplicate_level", readConcern: bson.D{{Key: "level", Value: "local"}, {Key: "level", Value: "local"}}, codeName: "BadValue"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := serveCommand(t, server, 330420, bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "readConcern", Value: tc.readConcern},
				{Key: "$db", Value: "app"},
			})
			assertCommandError(t, resp, tc.codeName)
		})
	}

	duplicateTopLevel := serveCommand(t, server, 330421, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, duplicateTopLevel, "BadValue")
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

func TestServerUpdateBSONSetRejectsCodeWithScopeAtResultNestingLimit(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 22550, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "scope-limit"}, {Key: "stable", Value: true}}}},
		{Key: "$db", Value: "app"},
	}))

	response := serveCommand(t, server, 22551, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "scope-limit"}}},
			{Key: "u", Value: bson.D{
				{Key: "$set", Value: bson.D{
					{Key: "code", Value: deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting - 1)},
				}},
			}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertIndexedWriteError(t, response, 0)

	find := serveCommand(t, server, 22552, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "scope-limit"}}}, {Key: "$db", Value: "app"}})
	batch := cursorFirstBatch(t, find)
	if len(batch) != 1 || !batch[0].Lookup("code").IsZero() || !batch[0].Lookup("stable").Boolean() {
		t.Fatalf("rejected scoped-code update changed document: %v", batch)
	}
}

func TestMongoBSONSetFieldsNeedNestingValidationCodeWithScope(t *testing.T) {
	if !mongoBSONSetFieldsNeedNestingValidation([]collections.BSONSetField{{
		Key:   "code",
		Value: deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting - 1),
	}}) {
		t.Fatal("CodeWithScope field bypasses nesting validation")
	}
}

func TestMongoMutationValuesEqualCodeWithScopeScopes(t *testing.T) {
	value := func(number any) bson.RawValue {
		scope := mustDocument(t, bson.D{{Key: "n", Value: number}})
		return bson.RawValue{Type: bson.TypeCodeWithScope, Value: bsoncore.AppendCodeWithScope(nil, "return n", scope)}
	}
	if !mongoMutationValuesEqual(value(int32(1)), value(int64(1))) {
		t.Fatal("CodeWithScope numeric-equivalent scopes differ")
	}
	if mongoMutationValuesEqual(value(int32(1)), bson.RawValue{Type: bson.TypeCodeWithScope, Value: bsoncore.AppendCodeWithScope(nil, "return other", mustDocument(t, bson.D{{Key: "n", Value: int32(1)}}))}) {
		t.Fatal("different code strings compare equal")
	}
}

func TestMongoMutationValuesEqualCountsCodeWithScopeWrapperDepth(t *testing.T) {
	withinLimit := deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting - 1)
	if !mongoMutationValuesEqual(withinLimit, withinLimit) {
		t.Fatal("equal CodeWithScope values at the nesting limit differ")
	}
	overLimit := deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting)
	if mongoMutationValuesEqual(overLimit, overLimit) {
		t.Fatal("CodeWithScope scope depth bypasses the nesting limit")
	}
}

func TestMongoMutationAddToSetDeduplicatesCodeWithScopeBeyondFiftyLevels(t *testing.T) {
	value := deeplyNestedCodeWithScopeValue(50)
	doc := mustDocument(t, bson.D{{Key: "items", Value: bson.A{value}}})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: value}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || changed || !bytes.Equal(updated, doc) {
		t.Fatalf("CodeWithScope duplicate changed=%v err=%v", changed, err)
	}
}

func TestServerBSONSetUpsertAllowsNativeBinaryValues(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			server := NewServer()
			server.Collections = collections.NewCollectionManager(db)
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}
			response := serveCommand(t, server, 22543, bson.D{
				{Key: "update", Value: "users"},
				{Key: "updates", Value: bson.A{bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: "binary-upsert"}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte{2, 3, 4}}}}}}},
					{Key: "upsert", Value: true},
				}}},
				{Key: "$db", Value: "app"},
			})
			if format != collections.DocumentFormatBSON {
				assertCommandError(t, response, "BadValue")
				if _, err := server.Collections.OpenCollection("app.users"); !errors.Is(err, collections.ErrCollectionNotFound) {
					t.Fatalf("unsupported BSON upsert created collection: %v", err)
				}
				return
			}
			assertOK(t, response)
			assertInt32(t, response, "n", 1)
			assertInt32(t, response, "nModified", 0)
			found := serveCommand(t, server, 22544, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "binary-upsert"}}}, {Key: "$db", Value: "app"}})
			batch := cursorFirstBatch(t, found)
			if len(batch) != 1 {
				t.Fatalf("batch len=%d want 1", len(batch))
			}
			subtype, payload := batch[0].Lookup("payload").Binary()
			if subtype != 0x00 || !bytes.Equal(payload, []byte{2, 3, 4}) {
				t.Fatalf("payload subtype/data=%#x/%v want 0/[2 3 4]", subtype, payload)
			}
			response = serveCommand(t, server, 22545, bson.D{
				{Key: "update", Value: "users"},
				{Key: "updates", Value: bson.A{bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: "binary-upsert"}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte{5, 6}}}}}}},
					{Key: "upsert", Value: true},
				}}},
				{Key: "$db", Value: "app"},
			})
			assertOK(t, response)
			assertInt32(t, response, "n", 1)
			assertInt32(t, response, "nModified", 1)
			found = serveCommand(t, server, 22546, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "binary-upsert"}}}, {Key: "$db", Value: "app"}})
			subtype, payload = cursorFirstBatch(t, found)[0].Lookup("payload").Binary()
			if subtype != 0x00 || !bytes.Equal(payload, []byte{5, 6}) {
				t.Fatalf("matched payload subtype/data=%#x/%v want 0/[5 6]", subtype, payload)
			}
		})
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

// A repeated exact ID must remain on the sequential path: each statement
// observes the prior statement's result, so the second $set matches but is a
// no-op.  This is deliberately BSON (not TemplateV1) because the native BSON
// batch path is the optimization guarded by mongoUpdateItemsCanUseBatch.
func TestServerUpdateBatchDuplicateBSONIDRemainsSequential(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 2280, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}}}},
		{Key: "$db", Value: "app"},
	}))

	update := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(1)}}}}}}
	response := serveCommand(t, server, 2281, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{update, update}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	assertInt32(t, response, "n", 2)
	assertInt32(t, response, "nModified", 1)
	find := serveCommand(t, server, 2282, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	batch := cursorFirstBatch(t, find)
	if len(batch) != 1 {
		t.Fatalf("find firstBatch len=%d want 1", len(batch))
	}
	got, ok := batch[0].Lookup("score").Int32OK()
	if !ok || got != 1 {
		t.Fatalf("score=%d typeOK=%v want 1", got, ok)
	}
}

func TestServerUpdateBatchVectorMaintenanceIsCommitAmbiguous(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	index := bson.D{{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}}, {Key: "name", Value: "embedding_vector"}, {Key: "treedbIndexType", Value: "vector"}, {Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(2)}, {Key: "metric", Value: "cosine"}, {Key: "m", Value: int32(16)}, {Key: "efConstruction", Value: int32(128)}, {Key: "efSearch", Value: int32(64)}, {Key: "encoding", Value: "float32"}}}}
	assertOK(t, serveCommand(t, s, 2290, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{index}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, s, 2291, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "embedding", Value: bson.A{1.0, 2.0}}}, bson.D{{Key: "_id", Value: "u2"}, {Key: "embedding", Value: bson.A{1.0, 2.0}}}}}, {Key: "$db", Value: "app"}}))
	updates := bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "embedding", Value: bson.A{1.0, 2.0, 3.0}}}}}}}, bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "embedding", Value: bson.A{1.0, 2.0, 3.0}}}}}}}}
	response := serveCommand(t, s, 2292, bson.D{{Key: "update", Value: "users"}, {Key: "ordered", Value: false}, {Key: "updates", Value: updates}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "ShutdownInProgress")
	assertInt32(t, response, "code", 91)
	if raw := bson.Raw(response); !raw.Lookup("n").IsZero() || !raw.Lookup("nModified").IsZero() {
		t.Fatalf("ambiguous batch leaked success counts: %s", response)
	}
	if !bson.Raw(response).Lookup("writeErrors").IsZero() {
		t.Fatalf("ambiguous batch returned indexed errors: %s", response)
	}
	for _, id := range []string{"u1", "u2"} {
		find := serveCommand(t, s, 2293, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "app"}})
		values, _ := cursorFirstBatch(t, find)[0].Lookup("embedding").Array().Values()
		if len(values) != 3 {
			t.Fatalf("%s vector len=%d", id, len(values))
		}
	}
}

func TestServerCreateIndexesRejectsUnsupportedOptionsBeforeCatalogMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	for _, option := range []bson.E{
		{Key: "sparse", Value: true},
		{Key: "partialFilterExpression", Value: bson.D{{Key: "score", Value: bson.D{{Key: "$gt", Value: 0}}}}},
		{Key: "expireAfterSeconds", Value: int32(60)},
		{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}}},
		{Key: "hidden", Value: true},
	} {
		index := bson.D{{Key: "key", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "name", Value: "score_1"}, option}
		assertCommandError(t, serveCommand(t, s, 2400, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{index}}, {Key: "$db", Value: "app"}}), "BadValue")
		if _, err := s.Collections.OpenCollection("app.users"); !errors.Is(err, collections.ErrCollectionNotFound) {
			t.Fatalf("option %q mutated catalog, open err=%v", option.Key, err)
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
				{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
			},
			bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}},
				{Key: "u", Value: bson.D{{Key: "$push", Value: bson.D{{Key: "score", Value: int32(2)}}}}},
			},
		}},
		{Key: "$db", Value: "app"},
	})
	assertIndexedWriteError(t, updateResponse, 1)

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
		{name: "multi", flag: bson.E{Key: "multi", Value: true}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := mustDocument(t, bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				tt.flag,
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
			})
			item, err := parseMongoUpdateItem(3, doc)
			if err != nil {
				t.Fatalf("parseMongoUpdateItem: %v", err)
			}
			if !item.multi {
				t.Fatal("parseMongoUpdateItem did not retain multi")
			}
		})
	}
}

func TestParseMongoUpdateItemRejectsRegexIDFilter(t *testing.T) {
	_, err := parseMongoUpdateItem(3, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: bson.Regex{Pattern: "^u", Options: ""}}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
	}))
	if err == nil || !strings.Contains(err.Error(), "updates[3]") || !strings.Contains(err.Error(), "_id equality") {
		t.Fatalf("err=%v want indexed _id equality rejection", err)
	}
}

func TestParseMongoUpdateItemIDOperatorFiltersUseFindPlan(t *testing.T) {
	for _, tt := range []struct {
		name    string
		query   bson.D
		upsert  bool
		exactID bool
	}{
		{name: "eq rejected", query: bson.D{{Key: "_id", Value: bson.D{{Key: "$eq", Value: "u1"}}}}, exactID: false},
		{name: "in generic", query: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"u1", "u2"}}}}}, exactID: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
				{Key: "q", Value: tt.query},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "ok", Value: true}}}}},
				{Key: "upsert", Value: tt.upsert},
			}))
			if tt.name == "eq rejected" {
				if err == nil || !strings.Contains(err.Error(), "unsupported find operator") {
					t.Fatalf("err=%v want unsupported $eq", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if item.exactID != tt.exactID {
				t.Fatalf("exactID=%v want %v", item.exactID, tt.exactID)
			}
			if tt.exactID && len(item.key) == 0 {
				t.Fatal("$eq did not encode the scalar _id key")
			}
			if !tt.exactID && len(item.key) != 0 {
				t.Fatal("$in incorrectly used the direct _id key path")
			}
		})
	}
}

func TestParseMongoUpdateItemPureSetSkipsGenericMutation(t *testing.T) {
	item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "grace"}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !item.pureSet || !item.bsonSetFieldsOK || len(item.mutation.set) != 0 || len(item.mutation.inc) != 0 || len(item.mutation.unset) != 0 || item.mutation.replace != nil {
		t.Fatalf("pure set item=%+v", item)
	}
}

func TestParseMongoUpdateItemBSONOnlyPureSetSkipsGenericMutation(t *testing.T) {
	item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte{1}}}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !item.pureSet || item.setFieldsOK || !item.bsonSetFieldsOK || len(item.mutation.set) != 0 || len(item.mutation.inc) != 0 || len(item.mutation.unset) != 0 || item.mutation.replace != nil {
		t.Fatalf("BSON-only pure set item=%+v", item)
	}
}

func TestParseMongoUpdateItemRejectsPureSetOverTargetLimit(t *testing.T) {
	fields := bson.D{}
	for i := range mongoMutationMaxTargets + 1 {
		fields = append(fields, bson.E{Key: fmt.Sprintf("f%d", i), Value: int32(i)})
	}
	_, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: fields}}},
	}))
	if err == nil {
		t.Fatalf("accepted %d pure $set targets", mongoMutationMaxTargets+1)
	}
}

func TestServerUpdateMissingCollectionExecutesEarlierUpsertBeforeParseError(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	response := serveCommand(t, s, 22597, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: int32(1)}}}}}, {Key: "upsert", Value: true}},
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "$pull", Value: bson.D{{Key: "score", Value: int32(2)}}}}}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	found := serveCommand(t, s, 22598, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	batch := cursorFirstBatch(t, found)
	if len(batch) != 0 {
		t.Fatalf("firstBatch len=%d want 0 after preflight rejection", len(batch))
	}
	response = serveCommand(t, s, 22599, bson.D{
		{Key: "update", Value: "replacement-users"},
		{Key: "updates", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: int32(1)}}}}}, {Key: "upsert", Value: true}},
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "_id", Value: "different"}}}, {Key: "upsert", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	})
	assertIndexedWriteError(t, response, 1)
	found = serveCommand(t, s, 22600, bson.D{{Key: "find", Value: "replacement-users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	batch = cursorFirstBatch(t, found)
	if len(batch) != 1 {
		t.Fatalf("replacement prefix batch len=%d want 1 after first-item upsert", len(batch))
	}
	if score, _ := batch[0].Lookup("score").Int32OK(); score != 1 {
		t.Fatalf("replacement prefix score=%d want 1", score)
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
	assertIndexedWriteError(t, updateResponse, 1)

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
	// Keep this below the command-wide execution budget: queued coalesced
	// mutations are now rejected rather than running after their deadline.
	server.UpdateCoalescingMaxDelay = 50 * time.Millisecond
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
	// Keep this below the command-wide execution budget: queued coalesced
	// mutations are now rejected rather than running after their deadline.
	server.UpdateCoalescingMaxDelay = 50 * time.Millisecond
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

func TestServerUpdateCoalescedSkipsBatchForNonLeadingCompoundUniqueComponent(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	defer func() { _ = server.Close() }()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	// A batch-eligible update would wait for this coalescing delay. Keep the
	// command budget much smaller to prove the non-leading unique component is
	// excluded before batch planning.
	server.UpdateCoalescingMaxDelay = time.Second
	server.UpdateCoalescingMaxBatch = 2
	assertOK(t, serveCommand(t, server, 2272, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "email", Value: int32(1)}}}, {Key: "name", Value: "tenant_email_1"}, {Key: "unique", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 2273, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "tenant", Value: "acme"}, {Key: "email", Value: "a@example.com"}},
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
	update.budget = newMongoWriteBudget(1)
	deadline := time.Now().Add(500 * time.Millisecond)
	update.budget.deadline = deadline
	if mongoUpdateCanUseBatch(col, update) {
		t.Fatal("non-leading compound unique component was admitted to native update batch")
	}
	matched, modified, err := server.runMongoUpdateCoalesced("app.users", col, update)
	if err != nil {
		t.Fatalf("runMongoUpdateCoalesced: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true,true", matched, modified)
	}
	if time.Now().After(deadline) {
		t.Fatal("non-leading compound unique update waited for batch planning past its command deadline")
	}
	server.updateMu.Lock()
	_, cached := server.updateCoalescers["app.users"]
	server.updateMu.Unlock()
	if cached {
		t.Fatal("non-leading compound unique update created a coalescer")
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
	if err != nil || !matched || !modified {
		t.Fatalf("matched=%v modified=%v err=%v want true,true,nil", matched, modified, err)
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
	assertIndexedWriteError(t, updateResponse, 0)

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
	for _, want := range []string{"BSON", "email_1"} {
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
	assertOK(t, dottedSort)

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

func TestServerFindTopLevelOrAcrossDocumentFormats(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = db.Close() }()
			server := NewServer()
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}
			server.Collections = collections.NewCollectionManager(db)
			assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
				bson.D{{Key: "_id", Value: "both"}, {Key: "active", Value: true}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(45)}},
				bson.D{{Key: "_id", Value: "city"}, {Key: "active", Value: true}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(30)}},
				bson.D{{Key: "_id", Value: "age"}, {Key: "active", Value: true}, {Key: "city", Value: "lax"}, {Key: "age", Value: int64(45)}},
				bson.D{{Key: "_id", Value: "inactive"}, {Key: "active", Value: false}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(45)}},
			}}, {Key: "$db", Value: "app"}}))
			assertOK(t, serveCommand(t, server, 11, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{
				{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}}, {Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"},
			}}}, {Key: "$db", Value: "app"}}))
			response := serveCommand(t, server, 2, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{
				{Key: "active", Value: true},
				{Key: "$or", Value: bson.A{
					bson.D{{Key: "city", Value: "hnl"}},
					bson.D{{Key: "$and", Value: bson.A{bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(40)}}}}}}},
				}},
			}}, {Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}}, {Key: "$db", Value: "app"}})
			assertBatchIDs(t, cursorFirstBatch(t, response), []string{"age", "both", "city"})
			byID := serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{
				{Key: "_id", Value: "both"}, {Key: "$or", Value: bson.A{bson.D{{Key: "city", Value: "missing"}}}},
			}}, {Key: "projection", Value: bson.D{{Key: "_id", Value: int32(1)}}}, {Key: "$db", Value: "app"}})
			assertBatchIDs(t, cursorFirstBatch(t, byID), nil)
			rangeLimit := serveCommand(t, server, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{
				{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(0)}}},
				{Key: "$or", Value: bson.A{bson.D{{Key: "city", Value: "missing"}}}},
			}}, {Key: "limit", Value: int32(1)}, {Key: "$db", Value: "app"}})
			assertBatchIDs(t, cursorFirstBatch(t, rangeLimit), nil)
		})
	}
}

func TestServerFindTopLevelOrRejectsMalformedAndRespectsScanCap(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	server := NewServer()
	server.MaxFindScanDocuments = 1
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "one"}, {Key: "city", Value: "lax"}},
		bson.D{{Key: "_id", Value: "two"}, {Key: "city", Value: "hnl"}},
	}}, {Key: "$db", Value: "app"}}))
	for _, filter := range []bson.D{
		{{Key: "$or", Value: bson.A{}}},
		{{Key: "$or", Value: bson.A{"not-a-document"}}},
		{{Key: "$or", Value: bson.A{bson.D{{Key: "city", Value: bson.D{{Key: "$regex", Value: "hnl"}}}}}}},
	} {
		assertCommandError(t, serveCommand(t, server, 2, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: filter}, {Key: "$db", Value: "app"}}), "BadValue")
	}
	assertCommandError(t, serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "$or", Value: bson.A{bson.D{{Key: "city", Value: "hnl"}}}}}}, {Key: "$db", Value: "app"}}), "BadValue")
}

func BenchmarkDocumentMatchesPlanTopLevelOr(b *testing.B) {
	plan, err := parseFindPlan(nil, mustDocument(b, bson.D{{Key: "active", Value: true}, {Key: "$or", Value: bson.A{
		bson.D{{Key: "city", Value: "hnl"}},
		bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(40)}}}},
	}}}))
	if err != nil {
		b.Fatalf("parse plan: %v", err)
	}
	doc := mustDocument(b, bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(45)}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match, err := documentMatchesPlan(doc, plan)
		if err != nil || !match {
			b.Fatalf("match=%v err=%v", match, err)
		}
	}
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
	decimalNaN, err := bson.ParseDecimal128("NaN")
	if err != nil {
		t.Fatalf("parse decimal NaN: %v", err)
	}
	decimalPosInf, err := bson.ParseDecimal128("Infinity")
	if err != nil {
		t.Fatalf("parse decimal +Infinity: %v", err)
	}
	decimalNegInf, err := bson.ParseDecimal128("-Infinity")
	if err != nil {
		t.Fatalf("parse decimal -Infinity: %v", err)
	}
	raw := bson.Raw(mustDocument(t, bson.D{
		{Key: "nan", Value: math.NaN()},
		{Key: "pos_inf", Value: math.Inf(1)},
		{Key: "neg_inf", Value: math.Inf(-1)},
		{Key: "finite", Value: 1.5},
		{Key: "decimal", Value: decimal},
		{Key: "decimal_int", Value: decimalInt},
		{Key: "decimal_tenth", Value: decimalTenth},
		{Key: "decimal_nan", Value: decimalNaN},
		{Key: "decimal_pos_inf", Value: decimalPosInf},
		{Key: "decimal_neg_inf", Value: decimalNegInf},
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
	decimalNaNValue := raw.Lookup("decimal_nan")
	decimalPosInfValue := raw.Lookup("decimal_pos_inf")
	decimalNegInfValue := raw.Lookup("decimal_neg_inf")
	largeInt := raw.Lookup("large_int")
	doubleInt := raw.Lookup("double_int")
	doubleFraction := raw.Lookup("double_fraction")

	if rawValuesEqual(nanValue, finite) {
		t.Fatal("NaN compared equal to finite number")
	}
	if rawValuesEqual(decimalNaNValue, decimalNaNValue) {
		t.Fatal("Decimal128 NaN compared equal to itself")
	}
	if !rawValuesEqual(decimalPosInfValue, posInf) || !rawValuesEqual(decimalNegInfValue, negInf) || rawValuesEqual(decimalPosInfValue, decimalNegInfValue) {
		t.Fatalf("Decimal128 infinity equality +/double=%v -/double=%v +/-=%v", rawValuesEqual(decimalPosInfValue, posInf), rawValuesEqual(decimalNegInfValue, negInf), rawValuesEqual(decimalPosInfValue, decimalNegInfValue))
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

func TestDocumentMatchesPlanBoundsDecimal128EqualityWork(t *testing.T) {
	leftDecimal, err := bson.ParseDecimal128("1E+6000")
	if err != nil {
		t.Fatal(err)
	}
	rightDecimal, err := bson.ParseDecimal128("10E+5999")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		count   int
		wantErr bool
	}{
		{name: "boundary", count: mongoQueryMaxDecimal128Normalizations / 2},
		{name: "over_budget", count: mongoQueryMaxDecimal128Normalizations/2 + 1, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			left, right := make(bson.A, test.count), make(bson.A, test.count)
			for i := range test.count {
				left[i], right[i] = leftDecimal, rightDecimal
			}
			doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "items", Value: left}})
			plan := findPlan{predicates: []findPredicate{{field: "items", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, right)}}}}
			match, err := documentMatchesPlan(doc, plan)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "1024 Decimal128 normalizations") || match {
					t.Fatalf("match=%v err=%v want Decimal128 budget error", match, err)
				}
				return
			}
			if err != nil || !match {
				t.Fatalf("match=%v err=%v want true/nil", match, err)
			}
		})
	}

	makeArray := func(value any, count int) bson.A {
		values := make(bson.A, count)
		for i := range count {
			values[i] = value
		}
		return values
	}
	t.Run("shared_across_or_branches", func(t *testing.T) {
		left, right := makeArray(leftDecimal, 300), makeArray(rightDecimal, 300)
		doc := mustDocument(t, bson.D{{Key: "items", Value: left}})
		pred := findPredicate{field: "items", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, right)}}
		match, err := documentMatchesPlan(doc, findPlan{predicates: []findPredicate{pred}, orBranches: [][]findPredicate{{pred}}})
		if err == nil || !strings.Contains(err.Error(), "1024 Decimal128 normalizations") || match {
			t.Fatalf("match=%v err=%v want shared Decimal128 budget error", match, err)
		}
	})
	t.Run("shared_across_in_candidates", func(t *testing.T) {
		left, right := makeArray(leftDecimal, 300), makeArray(rightDecimal, 300)
		miss := append(bson.A(nil), right...)
		miss[len(miss)-1] = int32(0)
		doc := mustDocument(t, bson.D{{Key: "items", Value: left}})
		pred := findPredicate{field: "items", op: findPredicateIn, values: []bson.RawValue{mustRawValue(t, miss), mustRawValue(t, right)}}
		match, err := documentMatchesPlan(doc, findPlan{predicates: []findPredicate{pred}})
		if err == nil || !strings.Contains(err.Error(), "1024 Decimal128 normalizations") || match {
			t.Fatalf("match=%v err=%v want shared Decimal128 budget error", match, err)
		}
	})
	t.Run("identical_encoding_fast_path", func(t *testing.T) {
		values := makeArray(leftDecimal, mongoQueryMaxDecimal128Normalizations+1)
		doc := mustDocument(t, bson.D{{Key: "items", Value: values}})
		pred := findPredicate{field: "items", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, values)}}
		match, err := documentMatchesPlan(doc, findPlan{predicates: []findPredicate{pred}})
		if err != nil || !match {
			t.Fatalf("match=%v err=%v want identical Decimal128 fast-path match", match, err)
		}
	})
}

func TestServerQueryAndFilterWriteRejectOverBudgetDecimal128Equality(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() { _ = closeBackend() }()
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	server.Collections = collections.NewCollectionManager(backend)

	leftDecimal, err := bson.ParseDecimal128("1E+6000")
	if err != nil {
		t.Fatal(err)
	}
	rightDecimal, err := bson.ParseDecimal128("10E+5999")
	if err != nil {
		t.Fatal(err)
	}
	count := mongoQueryMaxDecimal128Normalizations/2 + 1
	left, right := make(bson.A, count), make(bson.A, count)
	for i := range count {
		left[i], right[i] = leftDecimal, rightDecimal
	}
	assertOK(t, serveCommand(t, server, 330500, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "items", Value: left}}}},
		{Key: "$db", Value: "app"},
	}))
	response := serveCommand(t, server, 330501, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "items", Value: right}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "BadValue")
	message, ok := bson.Raw(response).Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(message, "1024 Decimal128 normalizations") {
		t.Fatalf("errmsg=%q ok=%v want Decimal128 budget error", message, ok)
	}
	updateResponse := serveCommand(t, server, 330502, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "items", Value: right}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertIndexedWriteError(t, updateResponse, 0)
	message, ok = bson.Raw(updateResponse).Lookup("writeErrors").Array().Index(0).Document().Lookup("errmsg").StringValueOK()
	if !ok || !strings.Contains(message, "1024 Decimal128 normalizations") {
		t.Fatalf("update errmsg=%q ok=%v want Decimal128 budget error", message, ok)
	}
	stored := cursorFirstBatch(t, serveCommand(t, server, 330503, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "$db", Value: "app"},
	}))
	if len(stored) != 1 || !stored[0].Lookup("marker").IsZero() {
		t.Fatalf("over-budget filter write mutated document: %v", stored)
	}
}

func TestRawValuesEqualHandlesDeepNestedBSON(t *testing.T) {
	within := deeplyNestedRawDocumentValue(mongoMutationMaxBSONNesting-1, int32(1))
	if !rawValuesEqual(within, within) {
		t.Fatal("bounded deep BSON equality result was incorrect")
	}
	excess := deeplyNestedRawDocumentValue(mongoMutationMaxBSONNesting, int32(1))
	if rawValuesEqual(excess, excess) {
		t.Fatal("accepted BSON equality beyond the nesting limit")
	}
}

func TestRawValuesEqualHandlesWideBSON(t *testing.T) {
	left := wideRawDocumentValue(4096, true)
	right := wideRawDocumentValue(4096, true)
	different := wideRawDocumentValue(4096, false)
	if !rawValuesEqual(left, right) || rawValuesEqual(left, different) {
		t.Fatal("wide BSON equality result was incorrect")
	}
	smallLeft := wideRawDocumentValue(64, true)
	smallRight := wideRawDocumentValue(64, true)
	smallAllocs := testing.AllocsPerRun(100, func() {
		if !rawValuesEqual(smallLeft, smallRight) {
			t.Fatal("small BSON equality result was incorrect")
		}
	})
	wideAllocs := testing.AllocsPerRun(100, func() {
		if !rawValuesEqual(left, right) {
			t.Fatal("wide BSON equality result was incorrect")
		}
	})
	if wideAllocs > smallAllocs+1 {
		t.Fatalf("wide BSON equality allocations=%f small=%f want bounded", wideAllocs, smallAllocs)
	}
}

func TestMongoMutationAddToSetDeduplicatesWideDocument(t *testing.T) {
	value := wideRawDocumentValue(4096, true)
	doc := rawDocumentWithValue("items", rawArrayWithValue(value))
	mutation := mongoMutation{addToSet: []mongoMutationArrayField{{name: "items", values: []bson.RawValue{value}}}}
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || changed || !bytes.Equal(updated, doc) {
		t.Fatalf("wide $addToSet updated=%v changed=%v err=%v", updated, changed, err)
	}
}

func wideRawDocumentValue(width int, final bool) bson.RawValue {
	index, doc := bsoncore.AppendDocumentStart(nil)
	for i := range width {
		value := true
		if i == width-1 {
			value = final
		}
		doc = bsoncore.AppendBooleanElement(doc, strconv.Itoa(i), value)
	}
	doc, _ = bsoncore.AppendDocumentEnd(doc, index)
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: doc}
}

func rawArrayWithValue(value bson.RawValue) bson.RawValue {
	index, array := bsoncore.AppendArrayStart(nil)
	array = bsoncore.AppendValueElement(array, "0", bsoncore.Value{Type: bsoncore.Type(value.Type), Data: value.Value})
	array, _ = bsoncore.AppendArrayEnd(array, index)
	return bson.RawValue{Type: bson.TypeArray, Value: array}
}

func wideRawArrayValue(width int) bson.RawValue {
	index, array := bsoncore.AppendArrayStart(nil)
	for i := range width {
		array = bsoncore.AppendBooleanElement(array, strconv.Itoa(i), true)
	}
	array, _ = bsoncore.AppendArrayEnd(array, index)
	return bson.RawValue{Type: bson.TypeArray, Value: array}
}

func deeplyNestedRawDocumentValue(depth int, leaf int32) bson.RawValue {
	value := make([]byte, 12)
	binary.LittleEndian.PutUint32(value, uint32(len(value)))
	value[4] = byte(bson.TypeInt32)
	value[5] = 'v'
	binary.LittleEndian.PutUint32(value[7:], uint32(leaf))
	for range depth {
		nested := make([]byte, 8+len(value))
		binary.LittleEndian.PutUint32(nested, uint32(len(nested)))
		nested[4] = byte(bson.TypeEmbeddedDocument)
		nested[5] = 'v'
		copy(nested[7:], value)
		value = nested
	}
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: value}
}

func deeplyNestedCodeWithScopeValue(depth int) bson.RawValue {
	scope := []byte{5, 0, 0, 0, 0}
	value := bson.RawValue{Type: bson.TypeCodeWithScope, Value: bsoncore.AppendCodeWithScope(nil, "", scope)}
	for range depth {
		index, scopeDocument := bsoncore.AppendDocumentStart(nil)
		scopeDocument = bsoncore.AppendValueElement(scopeDocument, "scope", bsoncore.Value{Type: bsoncore.Type(value.Type), Data: value.Value})
		scopeDocument, _ = bsoncore.AppendDocumentEnd(scopeDocument, index)
		value = bson.RawValue{Type: bson.TypeCodeWithScope, Value: bsoncore.AppendCodeWithScope(nil, "", scopeDocument)}
	}
	return value
}

func rawDocumentWithValue(key string, value bson.RawValue) wire.Document {
	doc := make([]byte, 4+1+len(key)+1+len(value.Value)+1)
	binary.LittleEndian.PutUint32(doc, uint32(len(doc)))
	doc[4] = byte(value.Type)
	copy(doc[5:], key)
	copy(doc[5+len(key)+1:], value.Value)
	return wire.Document(doc)
}

func rawDocumentWithIDAndValue(id, key string, value bson.RawValue) wire.Document {
	index, doc := bsoncore.AppendDocumentStart(nil)
	doc = bsoncore.AppendStringElement(doc, "_id", id)
	doc = bsoncore.AppendValueElement(doc, key, bsoncore.Value{Type: bsoncore.Type(value.Type), Data: value.Value})
	doc, _ = bsoncore.AppendDocumentEnd(doc, index)
	return wire.Document(doc)
}

func TestMongoMutationRejectsDeepStoredBSONBeforeDecode(t *testing.T) {
	doc := rawDocumentWithValue("deep", deeplyNestedRawDocumentValue(10000, int32(1)))
	before := append(wire.Document(nil), doc...)
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$push", Value: bson.D{{Key: "tags", Value: "x"}}}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || changed || !bytes.Equal(doc, before) {
		t.Fatalf("deep stored mutation changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationRejectsDeepCodeWithScopeBeforeDecode(t *testing.T) {
	doc := rawDocumentWithValue("code", deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting))
	before := append(wire.Document(nil), doc...)
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: "nested.marker", Value: true}}}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || changed || !bytes.Equal(doc, before) {
		t.Fatalf("deep CodeWithScope changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationRejectsDeepCodeWithScopeOperand(t *testing.T) {
	_, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: "nested.code", Value: deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting)}}}}))
	if err == nil {
		t.Fatal("accepted deeply nested CodeWithScope operand")
	}
}

func TestServerUpdateRejectsDeepCodeWithScopeBeforeDecode(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeBackend() }()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(backend)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	doc := rawDocumentWithIDAndValue("u1", "code", deeplyNestedCodeWithScopeValue(mongoMutationMaxBSONNesting))
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.Raw(doc)}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 2, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "nested.marker", Value: true}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertIndexedWriteError(t, response, 0)
	find := serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	if got := cursorFirstBatch(t, find)[0]; !got.Lookup("nested").IsZero() {
		t.Fatalf("rejected deep CodeWithScope update changed document: %v", got)
	}
}

func TestMongoMutationRejectsDeepOperandBeforeApplication(t *testing.T) {
	doc := mustDocument(t, bson.D{{Key: "existing", Value: int32(1)}})
	before := append(wire.Document(nil), doc...)
	_, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{
		{Key: "marker", Value: true},
		{Key: "deep", Value: deeplyNestedRawDocumentValue(10000, int32(1))},
	}}}))
	if err == nil || !bytes.Equal(doc, before) {
		t.Fatalf("deep operand err=%v document changed=%v", err, !bytes.Equal(doc, before))
	}
}

func TestMongoMutationBSONNestingLimit(t *testing.T) {
	for _, test := range []struct {
		name  string
		depth int
		want  bool
	}{
		{name: "boundary", depth: mongoMutationMaxBSONNesting - 1, want: true},
		{name: "excess", depth: mongoMutationMaxBSONNesting},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: "deep", Value: deeplyNestedRawDocumentValue(test.depth, int32(1))}}}}))
			if (err == nil) != test.want {
				t.Fatalf("depth=%d err=%v", test.depth, err)
			}
		})
	}
}

func TestMongoMutationRejectsResultBeyondBSONNestingLimit(t *testing.T) {
	doc := mustDocument(t, bson.D{{Key: "existing", Value: int32(1)}})
	before := append(wire.Document(nil), doc...)
	boundary, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: "deep", Value: deeplyNestedRawDocumentValue(mongoMutationMaxBSONNesting-2, int32(1))}}}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, boundary); err != nil || !changed {
		t.Fatalf("boundary result changed=%v err=%v", changed, err)
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{
		{Key: "marker", Value: true},
		{Key: "deep", Value: deeplyNestedRawDocumentValue(mongoMutationMaxBSONNesting-1, int32(1))},
	}}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || changed || !bytes.Equal(doc, before) {
		t.Fatalf("over-limit result changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationRejectsWideEachBeforeMaterialization(t *testing.T) {
	small := mongoMutationEachUpdate(t, mongoMutationMaxEachValues+1)
	wide := mongoMutationEachUpdate(t, 4096)
	if _, err := parseMongoMutation(wide); err == nil {
		t.Fatal("accepted wide $each")
	}
	smallAllocs := testing.AllocsPerRun(100, func() {
		if _, err := parseMongoMutation(small); err == nil {
			t.Fatal("accepted over-limit $each")
		}
	})
	wideAllocs := testing.AllocsPerRun(100, func() {
		if _, err := parseMongoMutation(wide); err == nil {
			t.Fatal("accepted wide $each")
		}
	})
	if wideAllocs > smallAllocs+8 {
		t.Fatalf("wide $each allocations=%f small=%f want bounded", wideAllocs, smallAllocs)
	}
}

func TestMongoMutationRejectsWideStoredBSONBeforeDecode(t *testing.T) {
	doc := rawDocumentWithIDAndValue("u1", "items", wideRawArrayValue(mongoMutationMaxDecodedElements+1))
	before := append(wire.Document(nil), doc...)
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$push", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: bson.A{}}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || changed || !bytes.Equal(doc, before) {
		t.Fatalf("wide stored BSON changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationSharesDecodeBudgetAcrossOperands(t *testing.T) {
	doc := rawDocumentWithIDAndValue("u1", "stable", bson.RawValue{Type: bson.TypeBoolean, Value: []byte{1}})
	before := append(wire.Document(nil), doc...)
	items := mongoMutationMaxDecodedElements/2 + 1
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{
		{Key: "nested.items", Value: wideRawArrayValue(items)},
		{Key: "other", Value: wideRawArrayValue(items)},
	}}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || changed || !bytes.Equal(doc, before) {
		t.Fatalf("split operands bypassed shared decode budget: changed=%v err=%v", changed, err)
	}
}

func TestServerUpdateRejectsWideStoredBSONBeforeDecode(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeBackend() }()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(backend)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	doc := rawDocumentWithIDAndValue("u1", "items", wideRawArrayValue(mongoMutationMaxDecodedElements+1))
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.Raw(doc)}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 2, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}}, {Key: "$push", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: bson.A{}}}}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertIndexedWriteError(t, response, 0)
	find := serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	if got := cursorFirstBatch(t, find)[0]; !got.Lookup("marker").IsZero() {
		t.Fatalf("rejected wide stored update changed document: %v", got)
	}
}

func TestServerUpdateSharesDecodeBudgetAcrossOperands(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeBackend() }()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(backend)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.Raw(rawDocumentWithIDAndValue("u1", "stable", bson.RawValue{Type: bson.TypeBoolean, Value: []byte{1}}))}}, {Key: "$db", Value: "app"}}))
	items := mongoMutationMaxDecodedElements/2 + 1
	response := serveCommand(t, server, 2, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "nested.items", Value: wideRawArrayValue(items)}, {Key: "other", Value: wideRawArrayValue(items)}}}}},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertIndexedWriteError(t, response, 0)
	find := serveCommand(t, server, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	got := cursorFirstBatch(t, find)[0]
	if !got.Lookup("nested").IsZero() || !got.Lookup("other").IsZero() {
		t.Fatalf("rejected split operand update changed document: %v", got)
	}
}

func TestMongoUpdateItemRejectsWideTargetsBeforeMaterialization(t *testing.T) {
	fields := bson.D{}
	for i := 0; i < 4096; i++ {
		fields = append(fields, bson.E{Key: fmt.Sprintf("field%d", i), Value: true})
	}
	item := mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: fields}}},
	})
	if _, err := parseMongoUpdateItem(0, item); err == nil {
		t.Fatal("accepted wide target set")
	}
	nearLimit := bson.D{}
	for i := 0; i < mongoMutationMaxTargets+1; i++ {
		nearLimit = append(nearLimit, bson.E{Key: fmt.Sprintf("field%d", i), Value: true})
	}
	nearItem := mustDocument(t, bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: nearLimit}}}})
	nearAllocs := testing.AllocsPerRun(100, func() {
		if _, err := parseMongoUpdateItem(0, nearItem); err == nil {
			t.Fatal("accepted over-limit target set")
		}
	})
	wideAllocs := testing.AllocsPerRun(100, func() {
		if _, err := parseMongoUpdateItem(0, item); err == nil {
			t.Fatal("accepted wide target set")
		}
	})
	if wideAllocs > nearAllocs+8 {
		t.Fatalf("wide target allocations=%f near=%f want bounded", wideAllocs, nearAllocs)
	}
}

func TestServerUpdateRejectsWideMutationOperandsBeforeAnyChange(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(t, serveCommand(t, server, 1, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "items", Value: bson.A{"old"}}}}},
		{Key: "$db", Value: "app"},
	}))
	wideEach := make(bson.A, 4096)
	for i := range wideEach {
		wideEach[i] = true
	}
	deep := deeplyNestedRawDocumentValue(mongoMutationMaxBSONNesting-1, int32(1))
	for requestID, update := range []bson.D{
		{{Key: "$set", Value: bson.D{{Key: "marker", Value: "wide"}}}, {Key: "$push", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: wideEach}}}}}},
		{{Key: "$set", Value: bson.D{{Key: "marker", Value: "deep"}, {Key: "deep", Value: deep}}}},
	} {
		response := serveCommand(t, server, int32(requestID+2), bson.D{
			{Key: "update", Value: "users"},
			{Key: "updates", Value: bson.A{bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: update},
			}}},
			{Key: "$db", Value: "app"},
		})
		if requestID == 0 {
			assertCommandError(t, response, "BadValue")
		} else {
			assertIndexedWriteError(t, response, 0)
		}
	}
	find := serveCommand(t, server, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	doc := cursorFirstBatch(t, find)[0]
	if !doc.Lookup("marker").IsZero() || !doc.Lookup("deep").IsZero() {
		t.Fatalf("rejected update changed document: %v", doc)
	}
	items, err := doc.Lookup("items").Array().Values()
	if err != nil || len(items) != 1 || items[0].StringValue() != "old" {
		t.Fatalf("rejected update changed array: values=%v err=%v", items, err)
	}
}

func mongoMutationEachUpdate(t *testing.T, count int) wire.Document {
	t.Helper()
	values := make(bson.A, count)
	for i := range values {
		values[i] = true
	}
	return mustDocument(t, bson.D{{Key: "$push", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}}})
}

func TestMongoUpdateItemRejectsDeepPureSetBeforeApplication(t *testing.T) {
	doc := mustDocument(t, bson.D{{Key: "existing", Value: int32(1)}})
	before := append(wire.Document(nil), doc...)
	_, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "deep", Value: deeplyNestedRawDocumentValue(mongoMutationMaxBSONNesting, int32(1))}}}}},
	}))
	if err == nil || !bytes.Equal(doc, before) {
		t.Fatalf("deep pure set err=%v document changed=%v", err, !bytes.Equal(doc, before))
	}
}

func TestMongoMutationAddToSetDistinguishesNonFiniteDecimal128(t *testing.T) {
	positive, err := bson.ParseDecimal128("Infinity")
	if err != nil {
		t.Fatal(err)
	}
	negative, err := bson.ParseDecimal128("-Infinity")
	if err != nil {
		t.Fatal(err)
	}
	nan, err := bson.ParseDecimal128("NaN")
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$addToSet", Value: bson.D{
			{Key: "items", Value: bson.D{{Key: "$each", Value: bson.A{negative, positive, nan, nan}}}},
		}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	updated, changed, err := applyMongoMutation(mustDocument(t, bson.D{{Key: "items", Value: bson.A{positive}}}), mutation)
	values, valuesErr := bson.Raw(updated).Lookup("items").Array().Values()
	if err != nil || !changed || valuesErr != nil || len(values) != 3 || values[0].Decimal128().String() != "Infinity" || values[1].Decimal128().String() != "-Infinity" || values[2].Decimal128().String() != "NaN" {
		t.Fatalf("non-finite Decimal128 $addToSet changed=%v err=%v values=%v valuesErr=%v", changed, err, values, valuesErr)
	}
}

func TestMongoMutationAddToSetDeduplicatesNestedNaN(t *testing.T) {
	decimalNaN, err := bson.ParseDecimal128("NaN")
	if err != nil {
		t.Fatal(err)
	}
	doc := mustDocument(t, bson.D{
		{Key: "documents", Value: bson.A{bson.D{{Key: "n", Value: math.NaN()}}}},
		{Key: "arrays", Value: bson.A{bson.A{decimalNaN}}},
	})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$addToSet", Value: bson.D{
		{Key: "documents", Value: bson.D{{Key: "$each", Value: bson.A{bson.D{{Key: "n", Value: decimalNaN}}}}}},
		{Key: "arrays", Value: bson.D{{Key: "$each", Value: bson.A{bson.A{math.NaN()}}}}},
	}}}))
	if err != nil {
		t.Fatal(err)
	}
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || changed || !bytes.Equal(updated, doc) {
		t.Fatalf("nested NaN $addToSet changed=%v err=%v", changed, err)
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

func TestIndexedEqualityCandidateLimitOnlyForPureSingleEquality(t *testing.T) {
	idx := collections.IndexDefinition{Name: "city_1", Field: "city", ValueType: collections.IndexValueString}
	limit, ok := indexedEqualityCandidateLimit(findPlan{
		predicates: []findPredicate{{field: "city", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, "hnl")}}},
		limit:      1,
	}, idx, 1)
	if !ok || limit != 1 {
		t.Fatalf("pure equality candidate limit=%d ok=%v want 1,true", limit, ok)
	}

	limit, ok = indexedEqualityCandidateLimit(findPlan{
		predicates: []findPredicate{{field: "city", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, "hnl")}}},
		skip:       1,
		limit:      1,
	}, idx, 1)
	if !ok || limit != 2 {
		t.Fatalf("over-cap equality candidate limit=%d ok=%v want overflow slot 2,true", limit, ok)
	}

	_, ok = indexedEqualityCandidateLimit(findPlan{
		predicates: []findPredicate{
			{field: "city", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, "hnl")}},
			{field: "active", op: findPredicateEq, values: []bson.RawValue{mustRawValue(t, true)}},
		},
		limit: 1,
	}, idx, 1)
	if ok {
		t.Fatal("mixed equality predicates should not use page candidate limit")
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

func TestMongoMutationApplyOperatorsAndReplacement(t *testing.T) {
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "count", Value: int32(1)}, {Key: "old", Value: true}})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "ada"}}}, {Key: "$inc", Value: bson.D{{Key: "count", Value: int32(2)}, {Key: "new", Value: int64(-1)}}}, {Key: "$unset", Value: bson.D{{Key: "old", Value: true}}}}))
	if err != nil {
		t.Fatalf("parse mutation: %v", err)
	}
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || !changed {
		t.Fatalf("apply changed=%v err=%v", changed, err)
	}
	if got, _ := bson.Raw(updated).Lookup("count").Int32OK(); got != 3 {
		t.Fatalf("count=%d want 3", got)
	}
	if got, _ := bson.Raw(updated).Lookup("new").Int64OK(); got != -1 {
		t.Fatalf("new=%d want -1", got)
	}
	if !bson.Raw(updated).Lookup("old").IsZero() {
		t.Fatal("unset field remains")
	}
	replacement, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "name", Value: "grace"}}))
	if err != nil {
		t.Fatal(err)
	}
	updated, _, err = applyMongoMutation(doc, replacement)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := bson.Raw(updated).Lookup("_id").StringValueOK(); got != "u1" {
		t.Fatalf("_id=%q", got)
	}
}

func TestMongoMutationNestedOperators(t *testing.T) {
	decimalOne, err := bson.ParseDecimal128("1")
	if err != nil {
		t.Fatal(err)
	}
	doc := mustDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "profile", Value: bson.D{{Key: "name", Value: "ada"}, {Key: "count", Value: int32(1)}, {Key: "old", Value: true}}},
		{Key: "tags", Value: bson.A{"go"}},
		{Key: "labels", Value: bson.A{"go"}},
		{Key: "scalarLabels", Value: bson.A{"go"}},
		{Key: "numbers", Value: bson.A{int32(1)}},
		{Key: "documents", Value: bson.A{bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}}},
	})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "profile.name", Value: "grace"}, {Key: "profile.city", Value: "london"}}},
		{Key: "$set", Value: bson.D{{Key: "profile._id", Value: "nested"}}},
		{Key: "$set", Value: bson.D{{Key: "profile.binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{1, 2}}}}},
		{Key: "$inc", Value: bson.D{{Key: "profile.count", Value: int32(2)}}},
		{Key: "$push", Value: bson.D{{Key: "tags", Value: bson.D{{Key: "$each", Value: bson.A{"db", "go"}}}}}},
		{Key: "$push", Value: bson.D{{Key: "events", Value: bson.D{{Key: "kind", Value: "login"}}}}},
		{Key: "$addToSet", Value: bson.D{{Key: "empty", Value: bson.D{{Key: "$each", Value: bson.A{}}}}}},
		{Key: "$addToSet", Value: bson.D{{Key: "labels", Value: bson.D{{Key: "$each", Value: bson.A{"go", "db", "go"}}}}}},
		{Key: "$addToSet", Value: bson.D{{Key: "scalarLabels", Value: "db"}}},
		{Key: "$addToSet", Value: bson.D{{Key: "numbers", Value: bson.D{{Key: "$each", Value: bson.A{int64(1), float64(1), decimalOne, int32(2)}}}}}},
		{Key: "$addToSet", Value: bson.D{
			{Key: "documents", Value: bson.D{
				{Key: "$each", Value: bson.A{
					bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}},
					bson.D{{Key: "b", Value: int32(2)}, {Key: "a", Value: int32(1)}},
				}},
			}},
		}},
		{Key: "$unset", Value: bson.D{{Key: "profile.old", Value: true}}},
	}))
	if err != nil {
		t.Fatalf("parse nested mutation: %v", err)
	}
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || !changed {
		t.Fatalf("apply nested mutation changed=%v err=%v", changed, err)
	}
	profile := bson.Raw(updated).Lookup("profile").Document()
	if got, _ := profile.Lookup("name").StringValueOK(); got != "grace" {
		t.Fatalf("profile.name=%q", got)
	}
	if got, _ := profile.Lookup("_id").StringValueOK(); got != "nested" {
		t.Fatalf("profile._id=%q", got)
	}
	if got, _ := profile.Lookup("count").Int32OK(); got != 3 {
		t.Fatalf("profile.count=%d", got)
	}
	if subtype, value := profile.Lookup("binary").Binary(); subtype != 0x80 || !bytes.Equal(value, []byte{1, 2}) {
		t.Fatalf("profile.binary=%#x/%v", subtype, value)
	}
	if !profile.Lookup("old").IsZero() {
		t.Fatal("profile.old remains")
	}
	values, err := bson.Raw(updated).Lookup("tags").Array().Values()
	if err != nil || len(values) != 3 {
		t.Fatalf("tags=%v err=%v", values, err)
	}
	values, err = bson.Raw(updated).Lookup("labels").Array().Values()
	if err != nil || len(values) != 2 {
		t.Fatalf("labels=%v err=%v", values, err)
	}
	values, err = bson.Raw(updated).Lookup("scalarLabels").Array().Values()
	if err != nil || len(values) != 2 || values[0].StringValue() != "go" || values[1].StringValue() != "db" {
		t.Fatalf("scalarLabels=%v err=%v", values, err)
	}
	values, err = bson.Raw(updated).Lookup("numbers").Array().Values()
	if err != nil || len(values) != 2 || values[0].Type != bson.TypeInt32 || values[1].Type != bson.TypeInt32 || values[1].Int32() != 2 {
		t.Fatalf("numbers=%v err=%v", values, err)
	}
	values, err = bson.Raw(updated).Lookup("documents").Array().Values()
	if err != nil || len(values) != 2 {
		t.Fatalf("documents=%v err=%v", values, err)
	}
	first, firstErr := values[0].Document().Elements()
	second, secondErr := values[1].Document().Elements()
	if firstErr != nil || secondErr != nil || len(first) != 2 || len(second) != 2 || first[0].Key() != "a" || first[1].Key() != "b" || second[0].Key() != "b" || second[1].Key() != "a" {
		t.Fatalf("documents preserve BSON field order: %v", values)
	}
	events, err := bson.Raw(updated).Lookup("events").Array().Values()
	if err != nil || len(events) != 1 || events[0].Document().Lookup("kind").StringValue() != "login" {
		t.Fatalf("events=%v err=%v", events, err)
	}
	if !bson.Raw(updated).Lookup("empty").IsZero() {
		t.Fatal("empty $each created an array")
	}
}

func TestMongoMutationAddToSetUsesNestedNumericEquality(t *testing.T) {
	decimalOne, err := bson.ParseDecimal128("1")
	if err != nil {
		t.Fatal(err)
	}
	doc := mustDocument(t, bson.D{
		{Key: "items", Value: bson.A{bson.D{{Key: "n", Value: int32(1)}}}},
		{Key: "arrays", Value: bson.A{bson.A{int32(1), bson.D{{Key: "n", Value: int32(1)}}}}},
	})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$addToSet", Value: bson.D{
			{Key: "items", Value: bson.D{
				{Key: "$each", Value: bson.A{
					bson.D{{Key: "n", Value: int64(1)}},
					bson.D{{Key: "n", Value: float64(1)}},
					bson.D{{Key: "n", Value: decimalOne}},
					bson.D{{Key: "n", Value: int32(2)}},
				}},
			}},
			{Key: "arrays", Value: bson.D{
				{Key: "$each", Value: bson.A{
					bson.A{int64(1), bson.D{{Key: "n", Value: decimalOne}}},
				}},
			}},
		}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	updated, changed, err := applyMongoMutation(doc, mutation)
	values, valuesErr := bson.Raw(updated).Lookup("items").Array().Values()
	if err != nil || !changed || valuesErr != nil || len(values) != 2 {
		t.Fatalf("nested numeric $addToSet changed=%v err=%v values=%v valuesErr=%v", changed, err, values, valuesErr)
	}
	arrays, arraysErr := bson.Raw(updated).Lookup("arrays").Array().Values()
	if arraysErr != nil || len(arrays) != 1 {
		t.Fatalf("nested numeric array $addToSet values=%v err=%v", arrays, arraysErr)
	}
}

func TestMongoMutationAddToSetRejectsLargeComparisonBytesBeforeMutation(t *testing.T) {
	payload := strings.Repeat("x", 512<<10)
	values := bson.A{}
	for i := range 9 {
		values = append(values, bson.D{{Key: "payload", Value: payload + fmt.Sprintf("-%d", i)}})
	}
	doc := mustDocument(t, bson.D{{Key: "items", Value: bson.A{bson.D{{Key: "payload", Value: payload + "-existing"}}}}})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || changed || !bson.Raw(doc).Lookup("marker").IsZero() {
		t.Fatalf("large comparison changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationAddToSetAcceptsRepeatedIdenticalDecimal128Values(t *testing.T) {
	decimal, err := bson.ParseDecimal128("1E+6000")
	if err != nil {
		t.Fatal(err)
	}
	values := make(bson.A, mongoMutationMaxEachValues)
	for i := range values {
		values[i] = decimal
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || !changed {
		t.Fatalf("identical Decimal128 $addToSet changed=%v err=%v", changed, err)
	}
	items, itemsErr := bson.Raw(updated).Lookup("items").Array().Values()
	if itemsErr != nil || len(items) != 1 || !mongoMutationValuesEqual(items[0], mustRawValue(t, decimal)) || !bson.Raw(updated).Lookup("marker").Boolean() {
		t.Fatalf("identical Decimal128 $addToSet changed=%v err=%v items=%v itemsErr=%v marker=%v", changed, err, items, itemsErr, bson.Raw(updated).Lookup("marker"))
	}
}

func TestMongoMutationAddToSetAcceptsAlternatingEquivalentDecimal128Values(t *testing.T) {
	left, err := bson.ParseDecimal128("1E+6000")
	if err != nil {
		t.Fatal(err)
	}
	right, err := bson.ParseDecimal128("10E+5999")
	if err != nil {
		t.Fatal(err)
	}
	leftRaw := mustRawValue(t, left)
	rightRaw := mustRawValue(t, right)
	if bytes.Equal(leftRaw.Value, rightRaw.Value) || !mongoMutationValuesEqual(leftRaw, rightRaw) {
		t.Fatal("alternating Decimal128 operands must be numerically equal with distinct encodings")
	}
	values := make(bson.A, 46)
	for i := range values {
		if i%2 == 0 {
			values[i] = left
		} else {
			values[i] = right
		}
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	work, ok := mongoMutationAddToSetDecimalComparisonWork(nil, mutation.addToSet[0].values, mongoMutationMaxAddToSetDecimalComparisons)
	if !ok || work != len(values) {
		t.Fatalf("alternating equivalent Decimal128 work=%d ok=%v want=%d", work, ok, len(values))
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || !changed {
		t.Fatalf("alternating equivalent Decimal128 $addToSet changed=%v err=%v", changed, err)
	}
	items, itemsErr := bson.Raw(updated).Lookup("items").Array().Values()
	if itemsErr != nil || len(items) != 1 || !mongoMutationValuesEqual(items[0], leftRaw) || !bson.Raw(updated).Lookup("marker").Boolean() {
		t.Fatalf("alternating equivalent Decimal128 $addToSet changed=%v err=%v items=%v itemsErr=%v marker=%v", changed, err, items, itemsErr, bson.Raw(updated).Lookup("marker"))
	}
}

func TestMongoMutationAddToSetAcceptsDocumentsWithIdenticalDecimal128Leaves(t *testing.T) {
	decimal, err := bson.ParseDecimal128("1E+6000")
	if err != nil {
		t.Fatal(err)
	}
	values := make(bson.A, 33)
	for i := range values {
		values[i] = bson.D{{Key: "n", Value: decimal}, {Key: "i", Value: int32(i)}}
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	first := mutation.addToSet[0].values[0]
	last := mutation.addToSet[0].values[len(mutation.addToSet[0].values)-1]
	if bytes.Equal(first.Value, last.Value) || !bytes.Equal(first.Document().Lookup("n").Value, last.Document().Lookup("n").Value) {
		t.Fatal("nested Decimal128 documents must differ while their Decimal128 leaves remain byte-identical")
	}
	doc := mustDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "items", Value: bson.A{values[0]}},
	})
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || !changed {
		t.Fatalf("identical nested Decimal128 $addToSet changed=%v err=%v", changed, err)
	}
	items, itemsErr := bson.Raw(updated).Lookup("items").Array().Values()
	if itemsErr != nil || len(items) != len(values) || !bson.Raw(updated).Lookup("marker").Boolean() {
		t.Fatalf("identical nested Decimal128 $addToSet changed=%v err=%v items=%d itemsErr=%v marker=%v", changed, err, len(items), itemsErr, bson.Raw(updated).Lookup("marker"))
	}
}

func TestMongoMutationAddToSetAcceptsDistinctDecimal128NaNPayloads(t *testing.T) {
	values := make(bson.A, mongoMutationMaxEachValues)
	for i := range values {
		value := bson.NewDecimal128(0x1f<<58, uint64(i+1))
		if !value.IsNaN() {
			t.Fatalf("value %d is not NaN", i)
		}
		values[i] = value
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(mutation.addToSet) != 1 || len(mutation.addToSet[0].values) != mongoMutationMaxEachValues || bytes.Equal(mutation.addToSet[0].values[0].Value, mutation.addToSet[0].values[len(mutation.addToSet[0].values)-1].Value) {
		t.Fatal("Decimal128 NaN payloads did not remain byte-distinct")
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || !changed {
		t.Fatalf("distinct Decimal128 NaN $addToSet changed=%v err=%v", changed, err)
	}
	items, itemsErr := bson.Raw(updated).Lookup("items").Array().Values()
	if itemsErr != nil || len(items) != 1 || !rawValueIsNaN(items[0]) || !bson.Raw(updated).Lookup("marker").Boolean() {
		t.Fatalf("distinct Decimal128 NaN $addToSet changed=%v err=%v items=%v itemsErr=%v marker=%v", changed, err, items, itemsErr, bson.Raw(updated).Lookup("marker"))
	}
}

func TestMongoMutationAddToSetAcceptsNestedDistinctDecimal128NaNPayloads(t *testing.T) {
	values := make(bson.A, mongoMutationMaxEachValues)
	for i := range values {
		value := bson.NewDecimal128(0x1f<<58, uint64(i+1))
		if !value.IsNaN() {
			t.Fatalf("value %d is not NaN", i)
		}
		values[i] = bson.D{{Key: "n", Value: value}}
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(mutation.addToSet) != 1 || len(mutation.addToSet[0].values) != mongoMutationMaxEachValues || bytes.Equal(mutation.addToSet[0].values[0].Value, mutation.addToSet[0].values[len(mutation.addToSet[0].values)-1].Value) {
		t.Fatal("nested Decimal128 NaN payloads did not remain byte-distinct")
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	updated, changed, err := applyMongoMutation(doc, mutation)
	if err != nil || !changed {
		t.Fatalf("nested distinct Decimal128 NaN $addToSet changed=%v err=%v", changed, err)
	}
	items, itemsErr := bson.Raw(updated).Lookup("items").Array().Values()
	if itemsErr != nil || len(items) != 1 || !rawValueIsNaN(items[0].Document().Lookup("n")) || !bson.Raw(updated).Lookup("marker").Boolean() {
		t.Fatalf("nested distinct Decimal128 NaN $addToSet changed=%v err=%v items=%v itemsErr=%v marker=%v", changed, err, items, itemsErr, bson.Raw(updated).Lookup("marker"))
	}
}

func TestMongoMutationAddToSetRejectsExpensiveDecimal128ComparisonsBeforeMutation(t *testing.T) {
	existing := bson.A{}
	candidates := bson.A{}
	for i := range 128 {
		value, err := bson.ParseDecimal128(fmt.Sprintf("%dE+6000", i+1))
		if err != nil {
			t.Fatal(err)
		}
		existing = append(existing, value)
	}
	for i := range mongoMutationMaxEachValues {
		value, err := bson.ParseDecimal128(fmt.Sprintf("%dE+6000", i+129))
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, value)
	}
	doc := mustDocument(t, bson.D{{Key: "items", Value: existing}})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: candidates}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || !strings.Contains(err.Error(), "1024 Decimal128 comparisons") || changed || !bson.Raw(doc).Lookup("marker").IsZero() {
		t.Fatalf("expensive Decimal128 comparison changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationAddToSetChargesNestedDecimal128LeavesBeforeMutation(t *testing.T) {
	existing := bson.A{}
	candidate := bson.A{}
	left, err := bson.ParseDecimal128("1E+6000")
	if err != nil {
		t.Fatal(err)
	}
	right, err := bson.ParseDecimal128("10E+5999")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(mustRawValue(t, left).Value, mustRawValue(t, right).Value) || !mongoMutationValuesEqual(mustRawValue(t, left), mustRawValue(t, right)) {
		t.Fatal("nested Decimal128 operands must be numerically equal with distinct encodings")
	}
	if decimal128NormalizationCount(mustRawValue(t, left)) != 1 || decimal128NormalizationCount(mustRawValue(t, right)) != 1 {
		t.Fatal("nested Decimal128 operands must both require finite normalization")
	}
	// The 513th equal-but-differently-encoded leaf would require the 1,025th
	// and 1,026th finite Decimal128 normalizations.
	for range mongoMutationMaxAddToSetDecimalComparisons/2 + 1 {
		existing = append(existing, left)
		candidate = append(candidate, right)
	}
	work, ok := mongoMutationAddToSetDecimalComparisonWork(
		[]bson.RawValue{mustRawValue(t, existing)},
		[]bson.RawValue{mustRawValue(t, candidate)},
		mongoMutationMaxAddToSetDecimalComparisons,
	)
	if ok || work != mongoMutationMaxAddToSetDecimalComparisons {
		t.Fatalf("nested Decimal128 work=%d ok=%v", work, ok)
	}
	doc := mustDocument(t, bson.D{{Key: "items", Value: bson.A{existing}}})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: candidate}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	storedValues, err := mongoMutationRawArrayPathValues(bson.Raw(doc), []string{"items"})
	if err != nil {
		t.Fatal(err)
	}
	work, ok = mongoMutationAddToSetDecimalComparisonWork(storedValues, mutation.addToSet[0].values, mongoMutationMaxAddToSetDecimalComparisons)
	if ok || work != mongoMutationMaxAddToSetDecimalComparisons {
		t.Fatalf("parsed nested Decimal128 work=%d ok=%v stored=%d candidates=%d", work, ok, len(storedValues), len(mutation.addToSet[0].values))
	}
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || !strings.Contains(err.Error(), "Decimal128 comparisons") || changed || !bson.Raw(doc).Lookup("marker").IsZero() {
		t.Fatalf("nested Decimal128 comparison changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationAddToSetSharesDecimal128BudgetAcrossTargets(t *testing.T) {
	values := bson.A{}
	for i := range 33 {
		value, err := bson.ParseDecimal128(fmt.Sprintf("%dE+6000", i+1))
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{
			{Key: "first", Value: bson.D{{Key: "$each", Value: values}}},
			{Key: "second", Value: bson.D{{Key: "$each", Value: values}}},
		}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || !strings.Contains(err.Error(), "Decimal128 comparisons") || changed || !bson.Raw(doc).Lookup("marker").IsZero() {
		t.Fatalf("multi-target Decimal128 comparison changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationAddToSetChargesDecimal128LeavesOnBothSides(t *testing.T) {
	left := bson.A{}
	right := bson.A{}
	for i := range 2048 {
		decimal, err := bson.ParseDecimal128("0E+6000")
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			left = append(left, decimal)
			right = append(right, int32(0))
		} else {
			left = append(left, int32(0))
			right = append(right, decimal)
		}
	}
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: bson.A{left, right}}}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	if _, changed, err := applyMongoMutation(doc, mutation); err == nil || !strings.Contains(err.Error(), "Decimal128 comparisons") || changed || !bson.Raw(doc).Lookup("marker").IsZero() {
		t.Fatalf("opposite Decimal128 leaves changed=%v err=%v", changed, err)
	}
}

func TestMongoMutationAddToSetRejectsMalformedDecimalLeafCountWithoutOverflow(t *testing.T) {
	decimal, err := bson.ParseDecimal128("1E+6000")
	if err != nil {
		t.Fatal(err)
	}
	work, ok := mongoMutationAddToSetDecimalComparisonWork(
		[]bson.RawValue{{Type: bson.TypeEmbeddedDocument, Value: []byte{0xff}}},
		[]bson.RawValue{mustRawValue(t, decimal)},
		mongoMutationMaxAddToSetDecimalComparisons,
	)
	if ok || work < 0 || work > mongoMutationMaxAddToSetDecimalComparisons {
		t.Fatalf("malformed Decimal128 work=%d ok=%v", work, ok)
	}
	work, ok = mongoMutationAddToSetDecimalComparisonWork(
		[]bson.RawValue{mustRawValue(t, decimal)},
		[]bson.RawValue{{Type: bson.TypeEmbeddedDocument, Value: []byte{0xff}}},
		mongoMutationMaxAddToSetDecimalComparisons,
	)
	if ok || work < 0 || work > mongoMutationMaxAddToSetDecimalComparisons {
		t.Fatalf("malformed right Decimal128 work=%d ok=%v", work, ok)
	}
}

func TestMongoMutationEmptyNestedArrayEachDoesNotCreateParents(t *testing.T) {
	for _, operator := range []string{"$push", "$addToSet"} {
		t.Run(operator, func(t *testing.T) {
			mutation, err := parseMongoMutation(mustDocument(t, bson.D{
				{Key: "$set", Value: bson.D{{Key: "changed", Value: true}}},
				{Key: operator, Value: bson.D{{Key: "parent.items", Value: bson.D{{Key: "$each", Value: bson.A{}}}}}},
			}))
			if err != nil {
				t.Fatal(err)
			}
			updated, changed, err := applyMongoMutation(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), mutation)
			if err != nil || !changed || !bson.Raw(updated).Lookup("changed").Boolean() || !bson.Raw(updated).Lookup("parent").IsZero() {
				t.Fatalf("updated=%v changed=%v err=%v", updated, changed, err)
			}
		})
	}
}

func TestMongoMutationArrayDocumentWithLaterEachIsScalar(t *testing.T) {
	for _, operator := range []string{"$push", "$addToSet"} {
		t.Run(operator, func(t *testing.T) {
			literal := bson.D{{Key: "kind", Value: "login"}, {Key: "$each", Value: "metadata"}}
			mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: operator, Value: bson.D{{Key: "events", Value: literal}}}}))
			if err != nil {
				t.Fatal(err)
			}
			updated, changed, err := applyMongoMutation(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), mutation)
			values, valuesErr := bson.Raw(updated).Lookup("events").Array().Values()
			if err != nil || !changed || valuesErr != nil || len(values) != 1 || values[0].Document().Lookup("kind").StringValue() != "login" || values[0].Document().Lookup("$each").StringValue() != "metadata" {
				t.Fatalf("updated=%v changed=%v err=%v values=%v valuesErr=%v", updated, changed, err, values, valuesErr)
			}
		})
	}
}

func TestMongoMutationSetOnInsertOnlyAppliesToInsertion(t *testing.T) {
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "state", Value: "matched"}})
	mutation, err := parseMongoMutation(mustDocument(t, bson.D{
		{Key: "$set", Value: bson.D{{Key: "state", Value: "updated"}}},
		{Key: "$setOnInsert", Value: bson.D{{Key: "created.by", Value: "gateway"}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	matched, _, err := applyMongoMutation(doc, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if !bson.Raw(matched).Lookup("created").IsZero() {
		t.Fatal("matched update applied $setOnInsert")
	}
	inserted, _, err := applyMongoMutationWithOptions(mustDocument(t, bson.D{{Key: "_id", Value: "u2"}}), mutation, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := bson.Raw(inserted).Lookup("created").Document().Lookup("by").StringValue(); got != "gateway" {
		t.Fatalf("created.by=%q", got)
	}
}

func TestMongoMutationEmptyOperatorSpecificationsAreNoops(t *testing.T) {
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	for _, operator := range []string{"$set", "$unset", "$inc", "$push", "$addToSet", "$setOnInsert"} {
		t.Run(operator, func(t *testing.T) {
			mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: operator, Value: bson.D{}}}))
			if err != nil {
				t.Fatalf("parse %s: %v", operator, err)
			}
			updated, changed, err := applyMongoMutation(doc, mutation)
			if err != nil || changed || !bytes.Equal(updated, doc) {
				t.Fatalf("apply %s updated=%v changed=%v err=%v", operator, updated, changed, err)
			}
		})
	}
}

func TestMongoMutationRejectsInvalidShapesAndOverflow(t *testing.T) {
	for _, update := range []bson.D{
		{{Key: "$set", Value: bson.D{{Key: "a", Value: 1}, {Key: "a.b", Value: 2}}}},
		{{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}}, {Key: "$inc", Value: bson.D{{Key: "x", Value: 1}}}},
		{{Key: "$push", Value: bson.D{{Key: "x", Value: bson.D{{Key: "$each", Value: "bad"}}}}}},
	} {
		if _, err := parseMongoMutation(mustDocument(t, update)); err == nil {
			t.Fatalf("accepted %v", update)
		}
	}
	for _, path := range []string{"", ".a", "a.", "a..b", "$", "$[]", "$[x]", "tags.0"} {
		if _, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: path, Value: int32(1)}}}})); err == nil {
			t.Fatalf("accepted invalid update path %q", path)
		}
	}
	if _, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: "2026", Value: int32(1)}}}})); err != nil {
		t.Fatalf("rejected top-level numeric field: %v", err)
	}
	pathAtLimit := strings.Repeat("a.", mongoMutationMaxPathDepth-1) + "a"
	if _, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: pathAtLimit, Value: int32(1)}}}})); err != nil {
		t.Fatalf("rejected %d-component path: %v", mongoMutationMaxPathDepth, err)
	}
	pathOverLimit := pathAtLimit + ".a"
	if _, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: pathOverLimit, Value: int32(1)}}}})); err == nil {
		t.Fatalf("accepted %d-component path", mongoMutationMaxPathDepth+1)
	}
	disjoint := make(map[string]struct{}, 2048)
	for i := range 2048 {
		disjoint[fmt.Sprintf("field%d", i)] = struct{}{}
	}
	if err := validateMongoMutationPathConflicts(disjoint); err != nil {
		t.Fatalf("disjoint paths conflict: %v", err)
	}
	disjoint["field1.child"] = struct{}{}
	if err := validateMongoMutationPathConflicts(disjoint); err == nil {
		t.Fatal("ancestor conflict accepted")
	}
	if err := validateMongoMutationPathConflicts(map[string]struct{}{"a": {}, "a-foo": {}, "a.b": {}}); err == nil {
		t.Fatal("intervening sibling hid ancestor conflict")
	}
	overLimit := bson.A{}
	for range mongoMutationMaxEachValues + 1 {
		overLimit = append(overLimit, int32(1))
	}
	if _, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$push", Value: bson.D{
		{Key: "tooMany", Value: bson.D{{Key: "$each", Value: overLimit}}},
	}}})); err == nil {
		t.Fatal("accepted over-limit $each")
	}
	targets := bson.D{}
	for i := range mongoMutationMaxTargets {
		targets = append(targets, bson.E{Key: fmt.Sprintf("f%d", i), Value: int32(i)})
	}
	if _, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: targets}})); err != nil {
		t.Fatalf("rejected %d targets: %v", mongoMutationMaxTargets, err)
	}
	targets = append(targets, bson.E{Key: "overflow", Value: int32(1)})
	if _, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: targets}})); err == nil {
		t.Fatalf("accepted %d targets", mongoMutationMaxTargets+1)
	}
	comparisonValues := bson.A{}
	for i := range mongoMutationMaxEachValues {
		comparisonValues = append(comparisonValues, fmt.Sprintf("new-%d", i))
	}
	normalMutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: comparisonValues}}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	updated, changed, err := applyMongoMutation(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), normalMutation)
	if values, valuesErr := bson.Raw(updated).Lookup("items").Array().Values(); err != nil || !changed || valuesErr != nil || len(values) != mongoMutationMaxEachValues {
		t.Fatalf("empty-array $each changed=%v err=%v values=%d valuesErr=%v", changed, err, len(values), valuesErr)
	}
	overBudgetArray := make(bson.A, 129)
	for i := range overBudgetArray {
		overBudgetArray[i] = int32(i)
	}
	comparisonMutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}}, {Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: comparisonValues}}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	overBudgetDocument := mustDocument(t, bson.D{{Key: "items", Value: overBudgetArray}})
	if _, changed, err := applyMongoMutation(overBudgetDocument, comparisonMutation); err == nil || changed || !bson.Raw(overBudgetDocument).Lookup("marker").IsZero() {
		t.Fatalf("over-budget comparison changed=%v err=%v", changed, err)
	}
	for _, test := range []struct {
		doc  wire.Document
		path string
	}{
		{mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "scalar", Value: true}}), "scalar.child"},
		{mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "array", Value: bson.A{int32(1)}}}), "array.child"},
	} {
		mutation, err := parseMongoMutation(mustDocument(t, bson.D{{Key: "$set", Value: bson.D{{Key: test.path, Value: int32(1)}}}}))
		if err != nil {
			t.Fatal(err)
		}
		if _, changed, applyErr := applyMongoMutation(test.doc, mutation); applyErr == nil || changed {
			t.Fatalf("%s traversal changed=%v err=%v", test.path, changed, applyErr)
		}
	}
	_, err = mongoMutationIncrement(mustRawValue(t, int64(math.MaxInt64)), mustRawValue(t, int64(1)))
	if err == nil {
		t.Fatal("overflow accepted")
	}
	_, err = mongoMutationIncrement(bson.RawValue{Type: bson.TypeNull}, mustRawValue(t, int32(1)))
	if err == nil {
		t.Fatal("null accepted")
	}
	changedID := mustDocument(t, bson.D{{Key: "_id", Value: "u2"}})
	if _, _, err := applyMongoReplacement(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), changedID); err == nil {
		t.Fatal("changed _id accepted")
	}
	doc := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}})
	invalidReplacement := mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}, {Key: "profile.name", Value: "bad"}})
	updated, changed, err = applyMongoReplacement(doc, invalidReplacement)
	if err == nil || updated != nil || changed || !bytes.Equal(doc, mustDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}})) {
		t.Fatalf("invalid replacement updated=%v changed=%v err=%v", updated, changed, err)
	}
}

func TestServerUpdateRejectsNumericArrayPathsAndAddToSetComparisonOverflow(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	items := bson.A{}
	for i := 0; i < 129; i++ {
		items = append(items, int32(i))
	}
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "tags", Value: bson.A{"old"}}, {Key: "items", Value: items}}}}, {Key: "$db", Value: "app"}}))
	update := func(requestID int32, value bson.D) bson.Raw {
		return serveCommand(t, s, requestID, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: value}}}}, {Key: "$db", Value: "app"}})
	}
	assertCommandError(t, update(2, bson.D{{Key: "$set", Value: bson.D{{Key: "tags.0", Value: "new"}}}}), "BadValue")
	values := bson.A{}
	for i := range mongoMutationMaxEachValues {
		values = append(values, fmt.Sprintf("new-%d", i))
	}
	assertIndexedWriteError(t, update(3, bson.D{{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}}, {Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}}}), 0)
	find := serveCommand(t, s, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	got := cursorFirstBatch(t, find)[0]
	if tags, tagsErr := got.Lookup("tags").Array().Values(); tagsErr != nil || len(tags) != 1 || tags[0].StringValue() != "old" || !got.Lookup("marker").IsZero() {
		t.Fatalf("numeric path changed document: tags=%v err=%v marker=%v", tags, tagsErr, got.Lookup("marker"))
	}
	if gotItems, itemsErr := got.Lookup("items").Array().Values(); itemsErr != nil || len(gotItems) != len(items) {
		t.Fatalf("over-budget $addToSet changed items=%d err=%v", len(gotItems), itemsErr)
	}
}

func TestServerUpdateRejectsExpensiveDecimal128AddToSetBeforeMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	s.Collections = collections.NewCollectionManager(db)
	existing := bson.A{}
	candidates := bson.A{}
	for i := range 128 {
		value, parseErr := bson.ParseDecimal128(fmt.Sprintf("%dE+6000", i+1))
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		existing = append(existing, value)
	}
	for i := range mongoMutationMaxEachValues {
		value, parseErr := bson.ParseDecimal128(fmt.Sprintf("%dE+6000", i+129))
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		candidates = append(candidates, value)
	}
	assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "items", Value: existing}}}}, {Key: "$db", Value: "app"}}))
	update := bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "u", Value: bson.D{
				{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
				{Key: "$addToSet", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: candidates}}}}},
			}},
		}}},
		{Key: "$db", Value: "app"},
	}
	assertIndexedWriteError(t, serveCommand(t, s, 2, update), 0)
	got := cursorFirstBatch(t, serveCommand(t, s, 3, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}}))[0]
	items, itemsErr := got.Lookup("items").Array().Values()
	if itemsErr != nil || len(items) != len(existing) || !got.Lookup("marker").IsZero() {
		t.Fatalf("expensive Decimal128 mutation changed document: items=%d err=%v marker=%v", len(items), itemsErr, got.Lookup("marker"))
	}
}

func TestServerUpdateGenericMutationsAcrossDocumentFormats(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := NewServer()
			s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}
			s.Collections = collections.NewCollectionManager(db)
			assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(2147483647)}, {Key: "old", Value: true}, {Key: "nullValue", Value: nil}, {Key: "textValue", Value: "bad"}}}}, {Key: "$db", Value: "app"}}))

			requestID := int32(2)
			nextRequestID := func() int32 {
				id := requestID
				requestID++
				return id
			}
			update := func(value bson.D) bson.Raw {
				return serveCommand(t, s, nextRequestID(), bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: value}}}}, {Key: "$db", Value: "app"}})
			}
			resp := update(bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "ada"}}}, {Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}, {Key: "missing", Value: int64(-2)}}}, {Key: "$unset", Value: bson.D{{Key: "old", Value: true}, {Key: "absent", Value: true}}}})
			assertOK(t, resp)
			assertInt32(t, resp, "n", 1)
			assertInt32(t, resp, "nModified", 1)
			find := serveCommand(t, s, nextRequestID(), bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
			got := cursorFirstBatch(t, find)[0]
			if n, ok := got.Lookup("n").Int64OK(); !ok || n != 2147483648 {
				t.Fatalf("n=%d ok=%v", n, ok)
			}
			if n, _ := got.Lookup("missing").Int64OK(); n != -2 {
				t.Fatalf("missing=%d", n)
			}
			if !got.Lookup("old").IsZero() {
				t.Fatal("old remains")
			}
			binaryUpdate := update(bson.D{{Key: "$set", Value: bson.D{{Key: "profile.binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{1, 2}}}}}})
			if format == collections.DocumentFormatBSON {
				assertOK(t, binaryUpdate)
			} else {
				assertIndexedWriteError(t, binaryUpdate, 0)
				find = serveCommand(t, s, nextRequestID(), bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
				if !cursorFirstBatch(t, find)[0].Lookup("profile").IsZero() {
					t.Fatalf("failed BSON nested update changed %s document", format)
				}
			}
			noop := update(bson.D{{Key: "$unset", Value: bson.D{{Key: "stillAbsent", Value: true}}}})
			assertOK(t, noop)
			assertInt32(t, noop, "nModified", 0)
			// int64 plus double is a double.
			assertOK(t, update(bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 0.5}}}}))
			find = serveCommand(t, s, nextRequestID(), bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
			if n, ok := cursorFirstBatch(t, find)[0].Lookup("n").DoubleOK(); !ok || n != 2147483648.5 {
				t.Fatalf("double n=%v ok=%v", n, ok)
			}
			for _, tc := range []struct {
				update  bson.D
				runtime bool
			}{
				{bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: "bad"}}}}, false},
				{bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: nil}}}}, false},
				{bson.D{{Key: "$inc", Value: bson.D{{Key: "nullValue", Value: int32(1)}}}}, true},
				{bson.D{{Key: "$inc", Value: bson.D{{Key: "textValue", Value: int32(1)}}}}, true},
				{bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: 1}}}, {Key: "$unset", Value: bson.D{{Key: "n", Value: true}}}}, false},
				{bson.D{{Key: "$set", Value: bson.D{{Key: "_id", Value: "u2"}}}}, false},
				{bson.D{{Key: "$push", Value: bson.D{{Key: "n", Value: 1}}}}, true},
			} {
				if tc.runtime {
					assertIndexedWriteError(t, update(tc.update), 0)
				} else {
					assertCommandError(t, update(tc.update), "BadValue")
				}
			}
			arrayFilters := serveCommand(t, s, nextRequestID(), bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "blocked", Value: true}}}}}, {Key: "arrayFilters", Value: bson.A{bson.D{{Key: "x", Value: int32(1)}}}}}}}, {Key: "$db", Value: "app"}})
			assertCommandError(t, arrayFilters, "BadValue")
			find = serveCommand(t, s, nextRequestID(), bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
			if !cursorFirstBatch(t, find)[0].Lookup("blocked").IsZero() {
				t.Fatalf("arrayFilters update changed %s document", format)
			}
			overLimit := bson.A{}
			for range mongoMutationMaxEachValues + 1 {
				overLimit = append(overLimit, int32(1))
			}
			assertCommandError(t, update(bson.D{{Key: "$push", Value: bson.D{{Key: "tooMany", Value: bson.D{{Key: "$each", Value: overLimit}}}}}}), "BadValue")
			find = serveCommand(t, s, nextRequestID(), bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
			if !cursorFirstBatch(t, find)[0].Lookup("tooMany").IsZero() {
				t.Fatalf("over-limit $each changed %s document", format)
			}
			find = serveCommand(t, s, nextRequestID(), bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
			if n, _ := cursorFirstBatch(t, find)[0].Lookup("n").DoubleOK(); n != 2147483648.5 {
				t.Fatalf("failed item changed n=%v", n)
			}
			assertOK(t, update(bson.D{{Key: "$inc", Value: bson.D{{Key: "largest", Value: int64(math.MaxInt64)}}}}))
			assertIndexedWriteError(t, update(bson.D{{Key: "$inc", Value: bson.D{{Key: "largest", Value: int64(1)}}}}), 0)
			assertOK(t, update(bson.D{{Key: "name", Value: "grace"}})) // omitted _id is preserved
			noop = update(bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "grace"}})
			assertOK(t, noop)
			assertInt32(t, noop, "nModified", 0)
			assertIndexedWriteError(t, update(bson.D{{Key: "_id", Value: "u2"}}), 0)
			assertOK(t, update(bson.D{})) // an empty replacement retains _id
		})
	}
}

func TestServerUpdateUpsertAcrossDocumentFormats(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := NewServer()
			s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}
			s.Collections = collections.NewCollectionManager(db)
			response := serveCommand(t, s, 7001, bson.D{
				{Key: "update", Value: "users"},
				{Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(-2)}}}}}, {Key: "upsert", Value: true}}}},
				{Key: "$db", Value: "app"},
			})
			assertOK(t, response)
			assertInt32(t, response, "n", 1)
			assertInt32(t, response, "nModified", 0)
			upserted, ok := bson.Raw(response).Lookup("upserted").ArrayOK()
			if !ok {
				t.Fatalf("missing upserted: %v", response)
			}
			values, err := upserted.Values()
			if err != nil || len(values) != 1 {
				t.Fatalf("upserted=%v err=%v", values, err)
			}
			if id, _ := values[0].Document().Lookup("_id").StringValueOK(); id != "u1" {
				t.Fatalf("upserted id=%q", id)
			}
			response = serveCommand(t, s, 7002, bson.D{
				{Key: "update", Value: "users"},
				{Key: "updates", Value: bson.A{
					bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}}, {Key: "upsert", Value: true}},
					bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: int64(2)}}}, {Key: "u", Value: bson.D{{Key: "name", Value: "grace"}}}, {Key: "upsert", Value: true}},
				}},
				{Key: "$db", Value: "app"},
			})
			assertOK(t, response)
			assertInt32(t, response, "n", 2)
			assertInt32(t, response, "nModified", 1)
			upserted, ok = bson.Raw(response).Lookup("upserted").ArrayOK()
			if !ok {
				t.Fatalf("missing mixed upserted: %v", response)
			}
			values, err = upserted.Values()
			if err != nil || len(values) != 1 || values[0].Document().Lookup("index").Int32() != 1 {
				t.Fatalf("mixed upserted=%v err=%v", values, err)
			}
			if id, ok := values[0].Document().Lookup("_id").Int64OK(); !ok || id != 2 {
				t.Fatalf("upserted typed id=%d ok=%v", id, ok)
			}
			assertIndexedWriteError(t, serveCommand(t, s, 7003, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "bad"}}}, {Key: "u", Value: bson.D{{Key: "_id", Value: "other"}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}}), 0)
		})
	}
}

func TestServerUpdateInvalidUpsertDoesNotCreateCollection(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	assertCommandError(t, serveCommand(t, s, 7010, bson.D{{Key: "update", Value: "missing"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: "bad"}}}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}}), "BadValue")
	if _, err := s.Collections.OpenCollection("app.missing"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("invalid upsert created collection: %v", err)
	}
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			server := NewServer()
			server.Collections = collections.NewCollectionManager(db)
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}
			response := serveCommand(t, server, 7011, bson.D{{Key: "update", Value: "replacement-mismatch"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "wrong"}}}, {Key: "upsert", Value: true}}}}, {Key: "$db", Value: "app"}})
			assertCommandError(t, response, "BadValue")
			if _, err := server.Collections.OpenCollection("app.replacement-mismatch"); !errors.Is(err, collections.ErrCollectionNotFound) {
				t.Fatalf("replacement mismatch created collection: %v", err)
			}
		})
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
	writeBuf, appended, err := server.appendBufferedMessageWithOwner(context.Background(), reader, 1, writeBuf)
	if err != nil {
		t.Fatalf("append first buffered message: %v", err)
	}
	if !appended {
		t.Fatal("first buffered message was not appended")
	}
	writeBuf, appended, err = server.appendBufferedMessageWithOwner(context.Background(), reader, 1, writeBuf)
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

	_, appended, err := NewServer().appendBufferedMessageWithOwner(context.Background(), reader, 1, nil)
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

func newMongoReadConcernTestServer(tb testing.TB) *Server {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(tb, serveCommand(tb, server, 330490, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}}, {Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(tb, serveCommand(tb, server, 330491, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "sfo"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	return server
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

func assertIndexedWriteError(tb testing.TB, doc wire.Document, wantIndex int32) {
	tb.Helper()
	assertOK(tb, doc)
	values, err := bson.Raw(doc).Lookup("writeErrors").Array().Values()
	if err != nil || len(values) == 0 {
		tb.Fatalf("writeErrors=%s err=%v", doc, err)
	}
	got, ok := values[0].Document().Lookup("index").Int32OK()
	if !ok || got != wantIndex {
		tb.Fatalf("writeErrors[0].index=%d ok=%v want %d: %s", got, ok, wantIndex, doc)
	}
}

func assertIndexName(tb testing.TB, doc bson.Raw, want string) {
	tb.Helper()
	got, ok := doc.Lookup("name").StringValueOK()
	if !ok || got != want {
		tb.Fatalf("index name=%q typeOK=%v want %q", got, ok, want)
	}
}
