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

func TestServerExactIDBSONSetMaintainsNonLeadingCompoundComponent(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 40630, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}, {Key: "unique", Value: true}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 40631, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 40632, bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "createdAt", Value: int32(2)}}}}}}}},
		{Key: "$db", Value: "app"},
	}))
	col, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	queryRaw, err := bson.Marshal(bson.D{{Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	prefix := bson.Raw(queryRaw).Lookup("tenant")
	old := bson.Raw(queryRaw).Lookup("createdAt")
	oldIDs, truncated, err := col.FindByCompoundIndexRange("tenant_created", collections.CompoundIndexRangeOptions{Prefix: []bson.RawValue{prefix}, Lower: collections.IndexRangeBound{Value: old, Inclusive: true}, Upper: collections.IndexRangeBound{Value: old, Inclusive: true}, Limit: 2})
	if err != nil || truncated || len(oldIDs) != 0 {
		t.Fatalf("old key ids=%d truncated=%v err=%v want 0,false,nil", len(oldIDs), truncated, err)
	}
	queryRaw, err = bson.Marshal(bson.D{{Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}})
	if err != nil {
		t.Fatal(err)
	}
	newValue := bson.Raw(queryRaw).Lookup("createdAt")
	newIDs, truncated, err := col.FindByCompoundIndexRange("tenant_created", collections.CompoundIndexRangeOptions{Prefix: []bson.RawValue{prefix}, Lower: collections.IndexRangeBound{Value: newValue, Inclusive: true}, Upper: collections.IndexRangeBound{Value: newValue, Inclusive: true}, Limit: 2})
	if err != nil || truncated || len(newIDs) != 1 {
		t.Fatalf("new key ids=%d truncated=%v err=%v want 1,false,nil", len(newIDs), truncated, err)
	}
	assertIndexedWriteError(t, serveCommand(t, server, 40633, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u2"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}}}},
		{Key: "$db", Value: "app"},
	}), 0)
}

func TestServerFindPlansCompoundAndExplicitDescendingIndexes(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 40640, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}}}, {Key: "name", Value: "tenant_created"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "createdAt", Value: int32(-1)}}}, {Key: "name", Value: "created_desc"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, server, 40641, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(2)}},
			bson.D{{Key: "_id", Value: "u3"}, {Key: "tenant", Value: "other"}, {Key: "createdAt", Value: int32(3)}},
		}},
		{Key: "$db", Value: "app"},
	}))

	// #4065 owns planner selection for the direct BSON-v2 compound primitive.
	// The compound index's declared descending suffix defines the no-sort scan
	// order; callers that need a particular order must request sort explicitly.
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40642, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}},
		{Key: "$db", Value: "app"},
	})), []string{"u2", "u1"})
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40643, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "createdAt", Value: int32(2)}}},
		{Key: "$db", Value: "app"},
	})), []string{"u2"})
	// Explicit descending BSON-v2 indexes must use the compound primitive,
	// rather than the legacy ascending single-field range API.
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40644, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: int32(2)}}}}},
		{Key: "limit", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})), []string{"u3"})
	assertBatchIDs(t, cursorFirstBatch(t, serveCommand(t, server, 40645, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "tenant", Value: bson.D{{Key: "$gte", Value: "acme"}}}}},
		{Key: "limit", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})), []string{"u2"})
	// Explain shares the executor's compound eligibility.
	for _, tc := range []struct {
		requestID int32
		filter    bson.D
	}{
		{requestID: 40646, filter: bson.D{{Key: "tenant", Value: "acme"}}},
		{requestID: 40647, filter: bson.D{{Key: "createdAt", Value: int32(2)}}},
	} {
		resp := serveCommand(t, server, tc.requestID, bson.D{
			{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: tc.filter}}},
			{Key: "$db", Value: "app"},
		})
		assertOK(t, resp)
		stage, ok := bson.Raw(resp).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValueOK()
		if !ok || stage != "compound_index_scan" {
			t.Fatalf("explain stage=%q ok=%v want compound_index_scan: %s", stage, ok, resp)
		}
	}
}
