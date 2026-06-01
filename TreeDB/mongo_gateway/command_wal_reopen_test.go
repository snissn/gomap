package mongogateway

import (
	"context"
	"net"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestStandaloneServerCommandWALCRUDReopens(t *testing.T) {
	profiles := []treedb.Profile{
		treedb.ProfileCommandWALRelaxed,
		treedb.ProfileCommandWALDurable,
	}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			standalone, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     dir,
				Profile: profile,
				DefaultCollectionOptions: collections.CollectionOptions{
					DocumentFormat: collections.DocumentFormatBSON,
				},
			})
			if err != nil {
				t.Fatalf("OpenStandaloneServer: %v", err)
			}
			client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
			opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer opCancel()

			coll := client.Database("app").Collection("users")
			if _, err := coll.InsertOne(opCtx, bson.D{
				{Key: "_id", Value: "u1"},
				{Key: "name", Value: "ada"},
				{Key: "generation", Value: int32(1)},
			}); err != nil {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("InsertOne u1: %v", err)
			}
			if _, err := coll.InsertOne(opCtx, bson.D{
				{Key: "_id", Value: "u2"},
				{Key: "name", Value: "grace"},
				{Key: "generation", Value: int32(1)},
			}); err != nil {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("InsertOne u2: %v", err)
			}
			updateResult, err := coll.UpdateOne(opCtx,
				bson.D{{Key: "_id", Value: "u1"}},
				bson.D{{Key: "$set", Value: bson.D{
					{Key: "name", Value: "ada2"},
					{Key: "generation", Value: int32(2)},
				}}},
			)
			if err != nil {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("UpdateOne u1: %v", err)
			}
			if updateResult.MatchedCount != 1 || updateResult.ModifiedCount != 1 {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("UpdateOne matched=%d modified=%d, want 1/1", updateResult.MatchedCount, updateResult.ModifiedCount)
			}
			deleteResult, err := coll.DeleteOne(opCtx, bson.D{{Key: "_id", Value: "u2"}})
			if err != nil {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("DeleteOne u2: %v", err)
			}
			if deleteResult.DeletedCount != 1 {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("DeleteOne deleted=%d, want 1", deleteResult.DeletedCount)
			}
			if got := standalone.Backend.Stats()["treedb.command_wal.required_feature"]; got != "true" {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("required_feature=%q, want true", got)
			}
			if got := standalone.Backend.State().AppliedCommandLSN; got < 4 {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("AppliedCommandLSN=%d, want at least 4", got)
			}
			stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)

			reopened, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     dir,
				Profile: profile,
			})
			if err != nil {
				t.Fatalf("reopen standalone: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			if got := reopened.Backend.Stats()["treedb.command_wal.required_feature"]; got != "true" {
				t.Fatalf("reopened required_feature=%q, want true", got)
			}
			reopenedCollection, err := reopened.Collections.OpenCollection("app.users")
			if err != nil {
				t.Fatalf("open collection after reopen: %v", err)
			}
			keyU1, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), collections.DocumentFormatBSON)
			if err != nil {
				t.Fatalf("prepare u1 key: %v", err)
			}
			stored, err := reopenedCollection.Get(keyU1)
			if err != nil {
				t.Fatalf("get u1 after reopen: %v", err)
			}
			raw := bson.Raw(stored)
			if err := raw.Validate(); err != nil {
				t.Fatalf("stored BSON validation: %v", err)
			}
			name, ok := raw.Lookup("name").StringValueOK()
			if !ok || name != "ada2" {
				t.Fatalf("stored name=%q ok=%v, want ada2", name, ok)
			}
			generation, ok := raw.Lookup("generation").Int32OK()
			if !ok || generation != 2 {
				t.Fatalf("stored generation=%d ok=%v, want 2", generation, ok)
			}
			keyU2, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u2"}}), collections.DocumentFormatBSON)
			if err != nil {
				t.Fatalf("prepare u2 key: %v", err)
			}
			missing, err := reopenedCollection.Get(keyU2)
			if err != nil {
				t.Fatalf("get u2 after reopen: %v", err)
			}
			if missing != nil {
				t.Fatalf("u2 exists after command-WAL delete replay: %s", missing)
			}
			if got := reopened.Backend.State().AppliedCommandLSN; got < 4 {
				t.Fatalf("reopened AppliedCommandLSN=%d, want at least 4", got)
			}
		})
	}
}

func startStandaloneMongoClientForTest(t *testing.T, standalone *StandaloneServer) (*mongo.Client, context.CancelFunc, net.Listener, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		_ = standalone.Close()
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- standalone.Serve(ctx, ln)
	}()
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("mongo connect: %v", err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("driver ping: %v", err)
	}
	return client, cancel, ln, serveErr
}

func stopStandaloneMongoClientForTest(t *testing.T, client *mongo.Client, cancel context.CancelFunc, ln net.Listener, serveErr chan error, standalone *StandaloneServer) {
	t.Helper()
	if client != nil {
		if err := client.Disconnect(context.Background()); err != nil {
			cancel()
			_ = ln.Close()
			_ = standalone.Close()
			t.Fatalf("disconnect: %v", err)
		}
	}
	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			_ = standalone.Close()
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(standaloneShutdownTimeout):
		_ = standalone.Close()
		t.Fatal("Serve did not stop")
	}
	if err := standalone.Close(); err != nil {
		t.Fatalf("standalone close: %v", err)
	}
}
