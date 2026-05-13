package mongogateway

import (
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoCompatibilityProbe func(*testing.T, *Server)

func TestMongoCompatibilityMatrix(t *testing.T) {
	rows := []struct {
		category string
		feature  string
		status   string
		probe    mongoCompatibilityProbe
	}{
		{
			category: "wire",
			feature:  "hello command",
			status:   "supported",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 1, bson.D{{Key: "hello", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, resp)
				assertBool(t, resp, "helloOk", true)
			},
		},
		{
			category: "wire",
			feature:  "ping command",
			status:   "supported",
			probe: func(t *testing.T, server *Server) {
				assertOK(t, serveCommand(t, server, 2, bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}}))
			},
		},
		{
			category: "wire",
			feature:  "connectionStatus command (#1473)",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 24, bson.D{{Key: "connectionStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}})
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
			},
		},
		{
			category: "crud",
			feature:  "insert explicit _id",
			status:   "supported",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 3, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u4"}, {Key: "city", Value: "lax"}, {Key: "age", Value: int64(29)}}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
			},
		},
		{
			category: "crud",
			feature:  "find by _id equality",
			status:   "supported",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 4, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u1"})
			},
		},
		{
			category: "query",
			feature:  "indexed equality and range predicates",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 5, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
						bson.D{{Key: "city", Value: "hnl"}},
						bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(40)}}}},
					}}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u2"})
			},
		},
		{
			category: "query",
			feature:  "$in on indexed scalar fields",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 6, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "city", Value: bson.D{{Key: "$in", Value: bson.A{"sfo", "missing"}}}}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u3"})
			},
		},
		{
			category: "query",
			feature:  "projection, sort, skip, and limit",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 7, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{}},
					{Key: "sort", Value: bson.D{{Key: "age", Value: int32(1)}}},
					{Key: "skip", Value: int32(1)},
					{Key: "limit", Value: int32(1)},
					{Key: "projection", Value: bson.D{{Key: "_id", Value: int32(1)}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u1"})
			},
		},
		{
			category: "cursor",
			feature:  "getMore and killCursors",
			status:   "supported",
			probe: func(t *testing.T, server *Server) {
				find := serveCommand(t, server, 8, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{}},
					{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
					{Key: "batchSize", Value: int32(1)},
					{Key: "$db", Value: "app"},
				})
				if first := cursorFirstBatch(t, find); len(first) != 1 {
					t.Fatalf("first batch len=%d want 1", len(first))
				}
				cursorID := cursorIDFromResponse(t, find)
				if cursorID == 0 {
					t.Fatal("cursor id=0 want open cursor")
				}
				next := serveCommand(t, server, 9, bson.D{
					{Key: "getMore", Value: cursorID},
					{Key: "collection", Value: "users"},
					{Key: "batchSize", Value: int32(1)},
					{Key: "$db", Value: "app"},
				})
				if batch := cursorNextBatch(t, next); len(batch) != 1 {
					t.Fatalf("next batch len=%d want 1", len(batch))
				}
				kill := serveCommand(t, server, 10, bson.D{
					{Key: "killCursors", Value: "users"},
					{Key: "cursors", Value: bson.A{cursorID}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, kill)
			},
		},
		{
			category: "crud",
			feature:  "updateOne $set by _id",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 11, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
				assertInt32(t, resp, "nModified", 1)
			},
		},
		{
			category: "crud",
			feature:  "delete by _id",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 12, bson.D{
					{Key: "delete", Value: "users"},
					{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
			},
		},
		{
			category: "metadata",
			feature:  "listCollections",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 13, bson.D{
					{Key: "listCollections", Value: int32(1)},
					{Key: "nameOnly", Value: true},
					{Key: "$db", Value: "app"},
				})
				batch := cursorFirstBatch(t, resp)
				if len(batch) != 1 {
					t.Fatalf("collection batch len=%d want 1", len(batch))
				}
			},
		},
		{
			category: "metadata",
			feature:  "createIndexes, listIndexes, and dropIndexes",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				list := serveCommand(t, server, 14, bson.D{{Key: "listIndexes", Value: "users"}, {Key: "$db", Value: "app"}})
				if batch := cursorFirstBatch(t, list); len(batch) != 4 {
					t.Fatalf("index batch len=%d want 4", len(batch))
				}
				drop := serveCommand(t, server, 15, bson.D{{Key: "dropIndexes", Value: "users"}, {Key: "index", Value: "score_1"}, {Key: "$db", Value: "app"}})
				assertOK(t, drop)
			},
		},
		{
			category: "document",
			feature:  "native BSON storage mode",
			status:   "supported subset",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 16, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "binary"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}}}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
			},
		},
		{
			category: "query gap",
			feature:  "$or",
			status:   "rejected",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 17, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "$or", Value: bson.A{bson.D{{Key: "city", Value: "hnl"}}}}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			category: "query gap",
			feature:  "dotted projection",
			status:   "rejected",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 18, bson.D{
					{Key: "find", Value: "users"},
					{Key: "projection", Value: bson.D{{Key: "profile.name", Value: int32(1)}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			category: "update gap",
			feature:  "upsert",
			status:   "rejected",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 19, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}},
						{Key: "upsert", Value: true},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			category: "update gap",
			feature:  "multi update",
			status:   "rejected",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 20, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}},
						{Key: "multi", Value: true},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			category: "update gap",
			feature:  "$inc",
			status:   "rejected",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 21, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			category: "index gap",
			feature:  "compound index",
			status:   "rejected",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 22, bson.D{
					{Key: "createIndexes", Value: "users"},
					{Key: "indexes", Value: bson.A{bson.D{
						{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}, {Key: "age", Value: int32(1)}}},
						{Key: "name", Value: "city_age_1"},
						{Key: "treedbValueType", Value: "string"},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			category: "index gap",
			feature:  "index without treedbValueType",
			status:   "rejected",
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 23, bson.D{
					{Key: "createIndexes", Value: "users"},
					{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			category: "command gap",
			feature:  "aggregate",
			status:   "not implemented",
			probe:    expectCommandNotFound(bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{}}, {Key: "$db", Value: "app"}}),
		},
		{
			category: "command gap",
			feature:  "count",
			status:   "not implemented",
			probe:    expectCommandNotFound(bson.D{{Key: "count", Value: "users"}, {Key: "$db", Value: "app"}}),
		},
		{
			category: "command gap",
			feature:  "findAndModify",
			status:   "not implemented",
			probe:    expectCommandNotFound(bson.D{{Key: "findAndModify", Value: "users"}, {Key: "$db", Value: "app"}}),
		},
		{
			category: "transaction gap",
			feature:  "sessions and transactions",
			status:   "not implemented",
			probe:    expectCommandNotFound(bson.D{{Key: "startSession", Value: int32(1)}, {Key: "$db", Value: "admin"}}),
		},
	}

	for i, row := range rows {
		row := row
		t.Run(fmt.Sprintf("%02d_%s_%s", i, compatibilityTestSlug(row.category), compatibilityTestSlug(row.feature)), func(t *testing.T) {
			if row.category == "" || row.feature == "" || row.status == "" {
				t.Fatalf("incomplete compatibility row: %+v", row)
			}
			if row.probe == nil {
				t.Fatalf("%s/%s has no probe", row.category, row.feature)
			}
			row.probe(t, newMongoCompatibilityMatrixServer(t))
		})
	}
}

func newMongoCompatibilityMatrixServer(tb testing.TB) *Server {
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
	assertOK(tb, serveCommand(tb, server, 1001, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}}, {Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}}, {Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "name", Value: "score_1"}, {Key: "treedbValueType", Value: "double"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(tb, serveCommand(tb, server, 1002, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(37)}, {Key: "score", Value: 1.25}, {Key: "active", Value: true}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(42)}, {Key: "score", Value: 2.5}, {Key: "active", Value: false}},
			bson.D{{Key: "_id", Value: "u3"}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int64(36)}, {Key: "score", Value: 3.75}, {Key: "active", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	return server
}

func expectCommandNotFound(command bson.D) mongoCompatibilityProbe {
	return func(t *testing.T, server *Server) {
		resp := serveCommand(t, server, 2000, command)
		assertCommandError(t, resp, "CommandNotFound")
	}
}

func compatibilityTestSlug(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
