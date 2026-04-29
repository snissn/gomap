package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"strings"
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

	idempotentResponse := serveCommand(t, server, 2271, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
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
			{Key: "name", Value: "email_1"},
			{Key: "unique", Value: true},
		}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, uniqueConflict, "DuplicateKey")

	assertOK(t, serveCommand(t, server, 237, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 238, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
		}}},
		{Key: "$db", Value: "app"},
	}))
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
	assertOK(t, serveCommand(t, server, 232, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	id1 := bson.NewObjectID()
	id2 := bson.NewObjectID()
	id3 := bson.NewObjectID()
	assertOK(t, serveCommand(t, server, 233, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: id1}, {Key: "name", Value: "ada"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(37)}},
			bson.D{{Key: "_id", Value: id2}, {Key: "name", Value: "grace"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(42)}},
			bson.D{{Key: "_id", Value: id3}, {Key: "name", Value: "katherine"}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int64(36)}},
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

	nonIndexableValue := serveCommand(t, server, 244, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "city", Value: bson.D{{Key: "nested", Value: "hnl"}}}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, nonIndexableValue, "BadValue")
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
	if _, _, ok, err := server.getMore(cursorID, "app.users", 1, true, defaultCursorBatchSize); err != nil || ok {
		t.Fatalf("getMore after owner cleanup ok/err=%v/%v want false/nil", ok, err)
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

	nextID, nextBatch, ok, err := server.getMore(cursorID, "app.users", 10, true, defaultCursorBatchSize)
	if err != nil || !ok {
		t.Fatalf("getMore ok/err=%v/%v want true/nil", ok, err)
	}
	if len(nextBatch) != 1 {
		t.Fatalf("nextBatch len=%d want 1", len(nextBatch))
	}
	if nextID != cursorID {
		t.Fatalf("cursor id after getMore=%d want %d", nextID, cursorID)
	}

	finalID, finalBatch, ok, err := server.getMore(cursorID, "app.users", 10, true, defaultCursorBatchSize)
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
			{Key: "name", Value: "city_1"},
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
			{Key: "name", Value: "city_1"},
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
			bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}}, {Key: "name", Value: "city_1"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}},
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

func TestCompareRawNumbersHandlesNonFiniteDoubles(t *testing.T) {
	raw := bson.Raw(mustDocument(t, bson.D{
		{Key: "nan", Value: math.NaN()},
		{Key: "pos_inf", Value: math.Inf(1)},
		{Key: "neg_inf", Value: math.Inf(-1)},
		{Key: "finite", Value: 1.5},
		{Key: "large_int", Value: int64(9007199254740993)},
	}))
	nanValue := raw.Lookup("nan")
	posInf := raw.Lookup("pos_inf")
	negInf := raw.Lookup("neg_inf")
	finite := raw.Lookup("finite")
	largeInt := raw.Lookup("large_int")

	if rawValuesEqual(nanValue, finite) {
		t.Fatal("NaN compared equal to finite number")
	}
	if match, err := valueMatchesPredicate(nanValue, findPredicate{op: findPredicateGT, values: []bson.RawValue{finite}}); err != nil || match {
		t.Fatalf("NaN range match/err=%v/%v want false/nil", match, err)
	}
	if cmp := compareRawValues(posInf, finite); cmp <= 0 {
		t.Fatalf("+Inf vs finite cmp=%d want >0", cmp)
	}
	if cmp := compareRawValues(negInf, finite); cmp >= 0 {
		t.Fatalf("-Inf vs finite cmp=%d want <0", cmp)
	}
	scalar, ok := indexScalarForBSONValue(largeInt)
	if !ok || scalar != int64(9007199254740993) {
		t.Fatalf("large int scalar=%v ok=%v want int64", scalar, ok)
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
