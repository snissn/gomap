package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
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

func TestDiagnosticsCountsTextIndexes(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	col, err := server.openCollectionCached("app.users")
	if err != nil {
		t.Fatalf("open users: %v", err)
	}
	if _, _, err := col.CreateTextIndex(collections.TextIndexDefinition{
		Name:   "body_text",
		Fields: []collections.TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("create text index: %v", err)
	}

	dbStats := serveCommand(t, server, 6110, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertOK(t, dbStats)
	if got, ok := dbStats.Lookup("indexes").Int64OK(); !ok || got != 5 {
		t.Fatalf("dbStats indexes=%d ok=%v want 5 with primary, three scalar, and one text index: %s", got, ok, dbStats)
	}

	collStats := serveCommand(t, server, 6111, bson.D{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}})
	assertOK(t, collStats)
	if got, ok := collStats.Lookup("nindexes").Int64OK(); !ok || got != 5 {
		t.Fatalf("collStats nindexes=%d ok=%v want 5 with primary, three scalar, and one text index: %s", got, ok, collStats)
	}
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

func TestDiagnosticsRejectOPMsgFeaturesBeforeObservation(t *testing.T) {
	commands := []bson.D{
		{{Key: "serverStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}},
		{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}},
		{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}},
		{{Key: "top", Value: int32(1)}, {Key: "$db", Value: "admin"}},
	}
	for _, command := range commands {
		name := command[0].Key
		for _, tc := range []struct {
			name  string
			build func(wire.Document) ([]byte, error)
		}{
			{name: "moreToCome", build: func(doc wire.Document) ([]byte, error) {
				return wire.AppendMsgMessage(nil, 6290, 0, wire.MsgFlagMoreToCome, doc)
			}},
			{name: "documentSequence", build: func(doc wire.Document) ([]byte, error) {
				return wire.AppendMsgMessageWithSequences(nil, 6291, 0, 0, doc, []wire.DocumentSequence{{Identifier: "ignored", Documents: []wire.Document{mustDocument(t, bson.D{{Key: "x", Value: int32(1)}})}}})
			}},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				request, err := tc.build(mustDocument(t, command))
				if err != nil {
					t.Fatal(err)
				}
				// No catalog is installed: reaching a diagnostic implementation would
				// yield a catalog error instead of this wire-admission fence.
				rw := &readWriter{r: bytes.NewReader(request)}
				err = NewServer().ServeOne(rw)
				if !errors.Is(err, wire.ErrUnsupported) || rw.w.Len() != 0 {
					t.Fatalf("ServeOne err=%v responseBytes=%d want ErrUnsupported/0", err, rw.w.Len())
				}
			})
		}
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

func TestDiagnosticsFastFindErrorCountsAsFailedWithoutNamespace(t *testing.T) {
	server := NewServer()
	response := serveCommand(t, server, 6240, bson.D{{Key: "find", Value: "users"}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	_, commands, namespaces, _, _ := server.diagnosticsSnapshot()
	metric, ok := commands["find"]
	if !ok || metric.Count != 1 || metric.Errors != 1 {
		t.Fatalf("find metric=%+v present=%v", metric, ok)
	}
	if len(namespaces) != 0 {
		t.Fatalf("failed find recorded namespace metrics: %+v", namespaces)
	}
}

func TestDiagnosticsRejectedMoreToComeWriteCountsAsFailedWithoutNamespace(t *testing.T) {
	server := NewServer()
	request, err := wire.AppendMsgMessage(nil, 6245, 0, wire.MsgFlagMoreToCome, mustDocument(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "rejected"}}}},
		{Key: "$db", Value: "app"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	rw := &readWriter{r: bytes.NewReader(request)}
	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne rejected moreToCome write: %v", err)
	}
	if rw.w.Len() != 0 {
		t.Fatalf("rejected moreToCome response bytes=%d want 0", rw.w.Len())
	}
	_, commands, namespaces, _, _ := server.diagnosticsSnapshot()
	metric, ok := commands["insert"]
	if !ok || metric.Count != 1 || metric.Errors != 1 {
		t.Fatalf("insert metric=%+v present=%v", metric, ok)
	}
	if len(namespaces) != 0 {
		t.Fatalf("rejected moreToCome write recorded namespace metrics: %+v", namespaces)
	}
}

func TestDiagnosticCommandNamespaceExcludesUserManagementArguments(t *testing.T) {
	for _, name := range []string{"createUser", "updateUser", "dropUser", "usersInfo"} {
		command := mustDocument(t, bson.D{{Key: name, Value: "alice"}, {Key: "$db", Value: "admin"}})
		if namespace := diagnosticCommandNamespace(name, command); namespace != "" {
			t.Fatalf("%s namespace=%q want empty", name, namespace)
		}
	}
	if namespace := diagnosticCommandNamespace("insert", mustDocument(t, bson.D{{Key: "insert", Value: "users"}, {Key: "$db", Value: "app"}})); namespace != "app.users" {
		t.Fatalf("insert namespace=%q want app.users", namespace)
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

func TestDBStatsExactPhysicalCapAllowsTrailingMetadataProvenEmptyCollection(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	// MaxFindScanDocuments is the public live-document limit: exactly three
	// app.users documents succeed. app.zzempty sorts after it and has no primary
	// root, so it remains provably empty after the live budget is exhausted.
	assertOK(t, serveCommand(t, server, 6275, bson.D{{Key: "create", Value: "zzempty"}, {Key: "$db", Value: "app"}}))
	server.MaxFindScanDocuments = 3
	response := serveCommand(t, server, 6276, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	if collections, ok := response.Lookup("collections").Int64OK(); !ok || collections != 2 {
		t.Fatalf("dbStats collections=%s", response)
	}
	if objects, ok := response.Lookup("objects").Int64OK(); !ok || objects != 3 {
		t.Fatalf("dbStats objects=%s", response)
	}
}

func TestCollStatsExactLiveDocumentCapSucceeds(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	// The public cap is live documents, not iterator positioning work. Exactly
	// three live IDs must complete; a fourth live ID would reject.
	server.MaxFindScanDocuments = 3
	response := serveCommand(t, server, 6277, bson.D{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	if count, ok := response.Lookup("count").Int64OK(); !ok || count != 3 {
		t.Fatalf("collStats count=%s", response)
	}
}

func TestDiagnosticsCountsRejectPrimaryWorkBeforeLiveCap(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	if _, err := server.Collections.CreateCollection(&collections.CollectionMeta{
		Name: "work.items",
		Options: collections.CollectionOptions{
			DocumentFormat:                   collections.DocumentFormatBSON,
			BufferedIndexedOverlayRoots:      true,
			BufferedIndexedWriteMaxDocuments: 16,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []collections.IndexDefinition{{Name: "kind", Field: "kind", ValueType: collections.IndexValueString}},
	}); err != nil {
		t.Fatalf("create overlay collection: %v", err)
	}
	// Leave exactly two live records while retaining three physical primary
	// overlay tombstones. The public live-document cap alone permits this
	// collection; its separate 2*N primary-source-work cap must still reject it.
	col, err := server.Collections.OpenCollection("work.items")
	if err != nil {
		t.Fatalf("open work items: %v", err)
	}
	ids := make([][]byte, 0, 5)
	documents := make([][]byte, 0, 5)
	for _, id := range []string{"i1", "i2", "i3", "i4", "i5"} {
		document, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "kind", Value: "diagnostic"}})
		if err != nil {
			t.Fatalf("marshal %s: %v", id, err)
		}
		ids = append(ids, []byte(id))
		documents = append(documents, document)
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		t.Fatalf("insert work items: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush work-item inserts: %v", err)
	}
	if _, err := col.CompactRootOverlays(context.Background()); err != nil {
		t.Fatalf("compact work-item insert overlays: %v", err)
	}
	for _, id := range []string{"i1", "i2", "i3"} {
		deleted, err := col.DeleteDocument([]byte(id))
		if err != nil || !deleted {
			t.Fatalf("delete %s deleted=%v err=%v", id, deleted, err)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush work-item tombstones: %v", err)
	}
	for _, id := range []string{"i1", "i2", "i3"} {
		if value, err := col.Get([]byte(id)); err != nil || value != nil {
			t.Fatalf("deleted %s value=%q err=%v", id, value, err)
		}
	}
	for _, id := range []string{"i4", "i5"} {
		if value, err := col.Get([]byte(id)); err != nil || value == nil {
			t.Fatalf("live %s value=%q err=%v", id, value, err)
		}
	}

	const liveCap = 2
	physicalCap := diagnosticPhysicalWorkBudget(liveCap)
	count, inspected, truncated, err := server.diagnosticCollectionCountWithin("work.items", liveCap, physicalCap*32)
	if err != nil || truncated || count != liveCap || inspected <= physicalCap {
		t.Fatalf("generous primary-work count=%d inspected=%d truncated=%v err=%v; want %d live and >%d physical work", count, inspected, truncated, err, liveCap, physicalCap)
	}
	count, inspected, truncated, err = server.diagnosticCollectionCountWithin("work.items", liveCap, physicalCap)
	if err != nil || !truncated || count > liveCap || inspected > physicalCap {
		t.Fatalf("capped primary-work count=%d inspected=%d truncated=%v err=%v", count, inspected, truncated, err)
	}

	server.MaxFindScanDocuments = liveCap
	assertCommandError(t, serveCommand(t, server, 6285, bson.D{{Key: "collStats", Value: "items"}, {Key: "$db", Value: "work"}}), "BadValue")
	assertCommandError(t, serveCommand(t, server, 6286, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "work"}}), "BadValue")
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
