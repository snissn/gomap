package mongogateway

import (
	"context"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// This uses the same RunCommand BSON envelopes issued by Compass' server-status
// and database/collection statistics panels, while retaining a direct official
// driver assertion instead of requiring a desktop Compass installation in CI.
func TestStandaloneServerOfficialDriverCompassDiagnosticsSmoke(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{
		Dir:     t.TempDir(),
		Profile: treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	if _, err := client.Database("app").Collection("users").InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: "u1"}},
		bson.D{{Key: "_id", Value: "u2"}},
	}); err != nil {
		t.Fatalf("seed diagnostics collection: %v", err)
	}

	var status bson.Raw
	if err := client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "serverStatus", Value: int32(1)}}).Decode(&status); err != nil {
		t.Fatalf("driver serverStatus: %v", err)
	}
	opcounters, ok := status.Lookup("opcounters").DocumentOK()
	if !ok {
		t.Fatalf("serverStatus opcounters=%s", status)
	}
	for _, name := range []string{"insert", "query", "update", "delete", "getmore", "command"} {
		if _, ok := opcounters.Lookup(name).Int64OK(); !ok {
			t.Fatalf("serverStatus opcounters.%s=%s", name, opcounters)
		}
	}
	metrics, ok := status.Lookup("metrics").DocumentOK()
	if !ok || metrics.Lookup("treedb").Type == 0 {
		t.Fatalf("serverStatus gateway metrics=%s", status)
	}
	storage, ok := status.Lookup("storage").DocumentOK()
	treedbStorage, storageOK := storage.Lookup("treedb").DocumentOK()
	commandWAL, walOK := treedbStorage.Lookup("commandWAL").BooleanOK()
	if !ok || !storageOK || !walOK || !commandWAL {
		t.Fatalf("serverStatus command-WAL inventory=%s", status)
	}

	var dbStats bson.Raw
	if err := client.Database("app").RunCommand(context.Background(), bson.D{{Key: "dbStats", Value: int32(1)}}).Decode(&dbStats); err != nil {
		t.Fatalf("driver dbStats: %v", err)
	}
	if collections, ok := dbStats.Lookup("collections").Int64OK(); !ok || collections != 1 {
		t.Fatalf("dbStats collections=%s", dbStats)
	}
	var collStats bson.Raw
	if err := client.Database("app").RunCommand(context.Background(), bson.D{{Key: "collStats", Value: "users"}}).Decode(&collStats); err != nil {
		t.Fatalf("driver collStats: %v", err)
	}
	if count, ok := collStats.Lookup("count").Int64OK(); !ok || count != 2 {
		t.Fatalf("collStats count=%s", collStats)
	}
	var top bson.Raw
	if err := client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "top", Value: int32(1)}}).Decode(&top); err != nil {
		t.Fatalf("driver top: %v", err)
	}
	totals, ok := top.Lookup("totals").DocumentOK()
	namespace, namespaceOK := totals.Lookup("app.users").DocumentOK()
	total, totalOK := namespace.Lookup("total").DocumentOK()
	if !ok || !namespaceOK || !totalOK {
		t.Fatalf("top totals=%s", top)
	}
	if _, ok := total.Lookup("time").Int64OK(); !ok {
		t.Fatalf("top app.users.total=%s", total)
	}
}

func TestStandaloneServerOfficialDriverDiagnosticsCountsFindAndGetMoreOnce(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{
		Dir:                      t.TempDir(),
		Profile:                  treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	coll := client.Database("app").Collection("users")
	if _, err := coll.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: "u1"}},
		bson.D{{Key: "_id", Value: "u2"}},
		bson.D{{Key: "_id", Value: "u3"}},
	}); err != nil {
		t.Fatalf("seed users: %v", err)
	}
	beforeTop := diagnosticsDriverTop(t, client)
	beforeCount := diagnosticsDriverTopCount(t, beforeTop, "app.users")
	beforeStatus := diagnosticsDriverStatus(t, client)
	beforeOpcounters, _ := beforeStatus.Lookup("opcounters").DocumentOK()
	beforeQuery, _ := beforeOpcounters.Lookup("query").Int64OK()
	beforeGetMore, _ := beforeOpcounters.Lookup("getmore").Int64OK()

	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetBatchSize(1))
	if err != nil {
		t.Fatalf("driver find: %v", err)
	}
	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil {
		t.Fatalf("driver cursor: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("driver docs=%d want 3", len(docs))
	}

	afterStatus := diagnosticsDriverStatus(t, client)
	afterOpcounters, _ := afterStatus.Lookup("opcounters").DocumentOK()
	afterQuery, _ := afterOpcounters.Lookup("query").Int64OK()
	afterGetMore, _ := afterOpcounters.Lookup("getmore").Int64OK()
	if afterQuery != beforeQuery+1 || afterGetMore != beforeGetMore+2 {
		t.Fatalf("opcounters query/getmore=%d/%d want %d/%d: %s", afterQuery, afterGetMore, beforeQuery+1, beforeGetMore+2, afterOpcounters)
	}
	afterTop := diagnosticsDriverTop(t, client)
	if got := diagnosticsDriverTopCount(t, afterTop, "app.users"); got != beforeCount+3 {
		t.Fatalf("top app.users.total.count=%d want %d (find plus two getMore exactly once): %s", got, beforeCount+3, afterTop)
	}
}

func diagnosticsDriverStatus(t *testing.T, client *mongo.Client) bson.Raw {
	t.Helper()
	var status bson.Raw
	if err := client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "serverStatus", Value: int32(1)}}).Decode(&status); err != nil {
		t.Fatalf("driver serverStatus: %v", err)
	}
	return status
}

func diagnosticsDriverTop(t *testing.T, client *mongo.Client) bson.Raw {
	t.Helper()
	var top bson.Raw
	if err := client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "top", Value: int32(1)}}).Decode(&top); err != nil {
		t.Fatalf("driver top: %v", err)
	}
	return top
}

func diagnosticsDriverTopCount(t *testing.T, top bson.Raw, namespace string) int64 {
	t.Helper()
	totals, ok := top.Lookup("totals").DocumentOK()
	metric, metricOK := totals.Lookup(namespace).DocumentOK()
	total, totalOK := metric.Lookup("total").DocumentOK()
	count, countOK := total.Lookup("count").Int64OK()
	if !ok || !metricOK || !totalOK || !countOK {
		t.Fatalf("top namespace %s=%s", namespace, top)
	}
	return count
}
