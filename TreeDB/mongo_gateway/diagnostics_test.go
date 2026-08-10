package mongogateway

import (
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDiagnosticsCommandsBoundedStandaloneShape(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)

	serverStatus := serveCommand(t, server, 6101, bson.D{{Key: "serverStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}})
	assertOK(t, serverStatus)
	if _, ok := serverStatus.Lookup("uptime").Int64OK(); !ok {
		t.Fatalf("serverStatus uptime missing or non-int64: %s", serverStatus)
	}
	connections, ok := serverStatus.Lookup("connections").DocumentOK()
	if !ok {
		t.Fatalf("serverStatus connections missing or non-document: %s", serverStatus)
	}
	if _, ok := connections.Lookup("current").Int64OK(); !ok {
		t.Fatalf("serverStatus connections.current missing or non-int64: %s", connections)
	}
	opcounters, ok := serverStatus.Lookup("opcounters").DocumentOK()
	if !ok {
		t.Fatalf("serverStatus opcounters missing or non-document: %s", serverStatus)
	}
	for _, name := range []string{"insert", "query", "update", "delete", "getmore", "command"} {
		if _, ok := opcounters.Lookup(name).Int64OK(); !ok {
			t.Fatalf("serverStatus opcounters.%s missing or non-int64: %s", name, opcounters)
		}
	}
	metrics, ok := serverStatus.Lookup("metrics").DocumentOK()
	if !ok {
		t.Fatalf("serverStatus metrics missing or non-document: %s", serverStatus)
	}
	treedb, ok := metrics.Lookup("treedb").DocumentOK()
	if !ok {
		t.Fatalf("serverStatus metrics.treedb missing or non-document: %s", metrics)
	}
	commands, ok := treedb.Lookup("commands").DocumentOK()
	if !ok {
		t.Fatalf("serverStatus metrics.treedb.commands missing or non-document: %s", treedb)
	}
	if commands.Lookup("serverStatus").Type != 0 {
		t.Fatalf("serverStatus must snapshot counters before counting itself: %s", commands)
	}

	dbStats := serveCommand(t, server, 6102, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertOK(t, dbStats)
	if got, ok := dbStats.Lookup("collections").Int64OK(); !ok || got != 1 {
		t.Fatalf("dbStats collections=%d ok=%v want 1: %s", got, ok, dbStats)
	}
	if got, ok := dbStats.Lookup("objects").Int64OK(); !ok || got != 3 {
		t.Fatalf("dbStats objects=%d ok=%v want 3: %s", got, ok, dbStats)
	}
	if dbStats.Lookup("dataSize").Type != 0 || dbStats.Lookup("storageSize").Type != 0 {
		t.Fatalf("dbStats reported non-authoritative byte totals: %s", dbStats)
	}

	collStats := serveCommand(t, server, 6103, bson.D{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}})
	assertOK(t, collStats)
	if got, ok := collStats.Lookup("count").Int64OK(); !ok || got != 3 {
		t.Fatalf("collStats count=%d ok=%v want 3: %s", got, ok, collStats)
	}
	if got, ok := collStats.Lookup("nindexes").Int64OK(); !ok || got != 4 {
		t.Fatalf("collStats nindexes=%d ok=%v want 4: %s", got, ok, collStats)
	}

	top := serveCommand(t, server, 6104, bson.D{{Key: "top", Value: int32(1)}, {Key: "$db", Value: "admin"}})
	assertOK(t, top)
	totals, ok := top.Lookup("totals").DocumentOK()
	if !ok || totals.Lookup("app.users").Type == 0 {
		t.Fatalf("top totals missing app.users command aggregate: %s", top)
	}
	namespace, ok := totals.Lookup("app.users").DocumentOK()
	if !ok {
		t.Fatalf("top app.users is not a document: %s", totals)
	}
	total, ok := namespace.Lookup("total").DocumentOK()
	if !ok {
		t.Fatalf("top app.users.total missing: %s", namespace)
	}
	if _, ok := total.Lookup("time").Int64OK(); !ok {
		t.Fatalf("top app.users.total.time missing or non-int64: %s", total)
	}
	if _, ok := total.Lookup("count").Int64OK(); !ok {
		t.Fatalf("top app.users.total.count missing or non-int64: %s", total)
	}
	assertCommandError(t, serveCommand(t, server, 6105, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "scale", Value: int32(1)}, {Key: "$db", Value: "app"}}), "FailedToParse")
}

func TestDiagnosticsRejectRoutedBeforeLocalObservation(t *testing.T) {
	server := newMongoRaftBridgeTestServerWithRoute(t, raftcluster.LeaderAdmission(), &mongoStaticClusterRouteProvider{})
	// A nil catalog makes any accidental metadata read fail distinctly. Each
	// diagnostic must return the routed-cluster fence first.
	server.Collections = nil
	for requestID, command := range []bson.D{
		{{Key: "serverStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}},
		{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}},
		{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}},
		{{Key: "top", Value: int32(1)}, {Key: "$db", Value: "admin"}},
	} {
		assertCommandError(t, serveCommand(t, server, int32(6200+requestID), command), "NotWritablePrimary")
	}
}

func TestDiagnosticsZeroValueServerInitializesNamespaceCounters(t *testing.T) {
	server := &Server{}
	server.noteDiagnosticCommand("insert", mustDocument(t, bson.D{{Key: "insert", Value: "users"}, {Key: "$db", Value: "app"}}), time.Millisecond, false)
	top, err := server.topResponse(mustDocument(t, bson.D{{Key: "top", Value: int32(1)}, {Key: "$db", Value: "admin"}}))
	if err != nil {
		t.Fatalf("top response: %v", err)
	}
	totals, ok := bson.Raw(top).Lookup("totals").DocumentOK()
	if !ok || totals.Lookup("app.users").Type == 0 {
		t.Fatalf("zero-value server lost namespace diagnostic: %s", top)
	}
}

func TestDiagnosticsCommandArgumentValidation(t *testing.T) {
	server := NewServer()
	for requestID, command := range []bson.D{
		{{Key: "serverStatus", Value: "1"}, {Key: "$db", Value: "admin"}},
		{{Key: "dbStats", Value: int32(2)}, {Key: "$db", Value: "app"}},
		{{Key: "top", Value: true}, {Key: "$db", Value: "admin"}},
		{{Key: "collStats", Value: ""}, {Key: "$db", Value: "app"}},
		{{Key: "serverStatus", Value: int32(1)}, {Key: "lsid", Value: "bad"}, {Key: "$db", Value: "admin"}},
		{{Key: "serverStatus", Value: int32(1)}, {Key: "comment", Value: int32(1)}, {Key: "$db", Value: "admin"}},
	} {
		assertCommandError(t, serveCommand(t, server, int32(6250+requestID), command), "FailedToParse")
	}
}

func TestDiagnosticsMetricsBoundedCardinality(t *testing.T) {
	server := &Server{MaxFindScanDocuments: 2}
	for i := 0; i < 8; i++ {
		server.noteDiagnosticCommand(fmt.Sprintf("unknown%d", i), mustDocument(t, bson.D{{Key: fmt.Sprintf("unknown%d", i), Value: "users"}, {Key: "$db", Value: "app"}}), time.Millisecond, true)
		server.noteDiagnosticCommand("insert", mustDocument(t, bson.D{{Key: "insert", Value: fmt.Sprintf("users%d", i)}, {Key: "$db", Value: "app"}}), time.Millisecond, false)
	}
	_, commands, namespaces, droppedCommands, droppedNamespaces := server.diagnosticsSnapshot()
	if len(commands) != 1 || commands["insert"].Count != 8 || droppedCommands != 8 {
		t.Fatalf("command metric cardinality=%d insert=%+v dropped=%d", len(commands), commands["insert"], droppedCommands)
	}
	if len(namespaces) != 2 || droppedNamespaces != 6 {
		t.Fatalf("namespace metric cardinality=%d dropped=%d", len(namespaces), droppedNamespaces)
	}
}

func TestDBStatsSharesGlobalDocumentScanBudget(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 3
	assertOK(t, serveCommand(t, server, 6270, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "e1"}}}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveCommand(t, server, 6271, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}}), "BadValue")
}

func TestDiagnosticsAuthorizationScopes(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "reader", []byte("reader password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "dbreader", []byte("dbreader password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "users"}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "dbreader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app"}}); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	setAuthorizationTestUser(server, 2, "admin", "reader")
	setAuthorizationTestUser(server, 3, "admin", "dbreader")
	assertOK(t, serveAuthorizationCommand(t, server, 1, 6301, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveAuthorizationCommand(t, server, 2, 6302, bson.D{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 6303, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertOK(t, serveAuthorizationCommand(t, server, 3, 6304, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 6305, bson.D{{Key: "collStats", Value: "other"}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 6306, bson.D{{Key: "serverStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}}), "Unauthorized")
}
