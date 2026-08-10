package mongogateway

import (
	"testing"

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
}
