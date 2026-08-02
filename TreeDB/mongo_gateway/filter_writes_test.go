package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoFilterWritesSelectOneAcrossUpdateDeleteAndFindAndModify(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(30)}, {Key: "active", Value: true}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "age", Value: int32(40)}, {Key: "active", Value: true}},
	}}, {Key: "$db", Value: "app"}}))
	update := serveCommand(t, server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(30)}}}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, update)
	if n, _ := bson.Raw(update).Lookup("n").Int32OK(); n != 1 {
		t.Fatalf("update n=%d want 1", n)
	}
	modify := serveCommand(t, server, 3, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "modified", Value: true}}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, modify)
	if id, _ := bson.Raw(modify).Lookup("value").Document().Lookup("_id").StringValueOK(); id != "u1" {
		t.Fatalf("findAndModify id=%q want u1", id)
	}
	deleted := serveCommand(t, server, 4, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(30)}}}}}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, deleted)
	if n, _ := bson.Raw(deleted).Lookup("n").Int32OK(); n != 1 {
		t.Fatalf("delete n=%d want 1", n)
	}
	limitZero := serveCommand(t, server, 5, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: int32(40)}}}, {Key: "limit", Value: int32(0)}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, limitZero, "BadValue")
	remaining := serveCommand(t, server, 6, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, remaining)
	if values, err := bson.Raw(remaining).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 1 {
		t.Fatalf("limit:0 mutated document: values=%v err=%v", values, err)
	}
}
