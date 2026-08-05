package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This is the public adoption contract: ordinary Mongo index DDL must not
// require a TreeDB-specific homogeneous type declaration.
func TestCreateIndexesDefaultsToBSONOrderedV2WithoutTreeDBValueType(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 4062, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "value", Value: int32(1)}}}, {Key: "name", Value: "value_1"}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 4064, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "n1"}, {Key: "value", Value: int32(7)}},
			bson.D{{Key: "_id", Value: "n2"}, {Key: "value", Value: int64(7)}},
			bson.D{{Key: "_id", Value: "n3"}, {Key: "value", Value: "seven"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 4065, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "value", Value: int64(7)}}},
		{Key: "$db", Value: "app"},
	})), []string{"n1", "n2"})
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 4066, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "value", Value: bson.D{{Key: "$in", Value: bson.A{int64(7)}}}}}},
		{Key: "$db", Value: "app"},
	})), []string{"n1", "n2"})
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 4067, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "value", Value: bson.D{{Key: "$gte", Value: int32(7)}}}},
			bson.D{{Key: "value", Value: bson.D{{Key: "$lt", Value: int32(8)}}}},
		}}}},
		{Key: "$db", Value: "app"},
	})), []string{"n1", "n2"})
	indexes := cursorFirstBatch(t, serveCommand(t, server, 4063, bson.D{{Key: "listIndexes", Value: "users"}, {Key: "$db", Value: "app"}}))
	if got := indexes[1].Lookup("treedbIndexKeyFormat"); got.StringValue() != "bson-ordered-v2" {
		t.Fatalf("listIndexes format=%q want bson-ordered-v2", got.StringValue())
	}
	if got := indexes[1].Lookup("treedbValueType"); !got.IsZero() {
		t.Fatalf("listIndexes exposed legacy treedbValueType=%q", got.StringValue())
	}
	if got, ok := indexes[1].Lookup("treedbIndexKeyVersion").Int32OK(); !ok || got != 2 {
		t.Fatalf("listIndexes v2 version=%d ok=%v want 2", got, ok)
	}
}
