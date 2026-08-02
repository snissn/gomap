package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestFindAndModifyReturnsAtomicBeforeAndAfterImages(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(1)}, {Key: "name", Value: "ada"}}}}, {Key: "$db", Value: "app"}}))
	before := serveCommand(t, server, 2, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, before)
	if n, ok := bson.Raw(before).Lookup("value").Document().Lookup("n").Int32OK(); !ok || n != 1 {
		t.Fatalf("before n=%d ok=%v want 1", n, ok)
	}
	after := serveCommand(t, server, 3, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "grace"}}}}}, {Key: "new", Value: true}, {Key: "fields", Value: bson.D{{Key: "name", Value: int32(1)}}}, {Key: "$db", Value: "app"}})
	assertOK(t, after)
	value := bson.Raw(after).Lookup("value").Document()
	if name, ok := value.Lookup("name").StringValueOK(); !ok || name != "grace" {
		t.Fatalf("after name=%q ok=%v", name, ok)
	}
	if !value.Lookup("n").IsZero() {
		t.Fatalf("projection retained n")
	}
}

func TestFindAndModifyNoMatchAndUpsert(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	miss := serveCommand(t, server, 4, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "none"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(1)}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, miss)
	if bson.Raw(miss).Lookup("value").Type != bson.TypeNull {
		t.Fatalf("no-match value=%v want null", bson.Raw(miss).Lookup("value").Type)
	}
	upsert := serveCommand(t, server, 5, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "update", Value: bson.D{{Key: "name", Value: "new"}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
	assertOK(t, upsert)
	if id, ok := bson.Raw(upsert).Lookup("lastErrorObject").Document().Lookup("upserted").StringValueOK(); !ok || id != "u2" {
		t.Fatalf("upserted=%q ok=%v", id, ok)
	}
	if name, ok := bson.Raw(upsert).Lookup("value").Document().Lookup("name").StringValueOK(); !ok || name != "new" {
		t.Fatalf("upsert value name=%q ok=%v", name, ok)
	}
}
