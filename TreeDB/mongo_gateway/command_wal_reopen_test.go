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
			mutationResult, err := coll.UpdateOne(opCtx,
				bson.D{{Key: "_id", Value: "u1"}},
				bson.D{{Key: "$inc", Value: bson.D{{Key: "generation", Value: int32(3)}}}, {Key: "$unset", Value: bson.D{{Key: "name", Value: true}}}},
			)
			if err != nil || mutationResult.MatchedCount != 1 || mutationResult.ModifiedCount != 1 {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("generic UpdateOne result=%+v err=%v want 1/1", mutationResult, err)
			}
			replaceResult, err := coll.ReplaceOne(opCtx, bson.D{{Key: "_id", Value: "u1"}}, bson.D{{Key: "generation", Value: int32(6)}, {Key: "name", Value: "replaced"}})
			if err != nil || replaceResult.MatchedCount != 1 || replaceResult.ModifiedCount != 1 {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("ReplaceOne result=%+v err=%v want 1/1", replaceResult, err)
			}
			modifierUpsert, err := coll.UpdateOne(opCtx, bson.D{{Key: "_id", Value: "u3"}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "generation", Value: int32(1)}}}}, options.UpdateOne().SetUpsert(true))
			if err != nil || modifierUpsert.UpsertedCount != 1 || modifierUpsert.UpsertedID != "u3" {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("modifier upsert result=%+v err=%v", modifierUpsert, err)
			}
			replacementUpsert, err := coll.ReplaceOne(opCtx, bson.D{{Key: "_id", Value: "u4"}}, bson.D{{Key: "name", Value: "upserted"}}, options.Replace().SetUpsert(true))
			if err != nil || replacementUpsert.UpsertedCount != 1 || replacementUpsert.UpsertedID != "u4" {
				stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				t.Fatalf("replacement upsert result=%+v err=%v", replacementUpsert, err)
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
			if !ok || name != "replaced" {
				t.Fatalf("stored name=%q ok=%v, want replaced", name, ok)
			}
			generation, ok := raw.Lookup("generation").Int32OK()
			if !ok || generation != 6 {
				t.Fatalf("stored generation=%d ok=%v, want 6", generation, ok)
			}
			for _, id := range []string{"u3", "u4"} {
				key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: id}}), collections.DocumentFormatBSON)
				if err != nil {
					t.Fatalf("prepare %s key: %v", id, err)
				}
				stored, err := reopenedCollection.Get(key)
				if err != nil || stored == nil {
					t.Fatalf("get %s after reopen: stored=%v err=%v", id, stored, err)
				}
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

func TestMongoMutationCommandWALValueLogPointersReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(backend)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "generation", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "u3"}, {Key: "generation", Value: int32(3)}, {Key: "payload", Value: string(make([]byte, 256))}},
	}}, {Key: "$db", Value: "app"}}))
	nestedMutation := bson.D{
		{Key: "$inc", Value: bson.D{{Key: "generation", Value: int32(5)}}},
		{Key: "$set", Value: bson.D{{Key: "profile.name", Value: "durable"}}},
		{Key: "$addToSet", Value: bson.D{{Key: "labels", Value: bson.D{{Key: "$each", Value: bson.A{"wal", "wal"}}}}}},
	}
	upsertMutation := bson.D{
		{Key: "$set", Value: bson.D{{Key: "name", Value: "upserted"}}},
		{Key: "$setOnInsert", Value: bson.D{{Key: "created.by", Value: "wal"}}},
	}
	assertOK(t, serveCommand(t, server, 2, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "generation", Value: int32(1)}}}, {Key: "u", Value: nestedMutation}},
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: upsertMutation}, {Key: "upsert", Value: true}},
	}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 3, bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "generation", Value: int32(3)}}}, {Key: "limit", Value: int32(1)}},
	}}, {Key: "$db", Value: "app"}}))
	if err := server.Collections.FlushAll(); err != nil {
		_ = closeBackend()
		t.Fatalf("flush collections: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		_ = closeBackend()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := closeBackend(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	reopened, closeReopened, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	defer func() { _ = closeReopened() }()
	collection, err := collections.NewCollectionManager(reopened).OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	for _, want := range []struct {
		id         string
		generation int32
		name       string
	}{{id: "u1", generation: 6}, {id: "u2", name: "upserted"}} {
		key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: want.id}}), collections.DocumentFormatBSON)
		if err != nil {
			t.Fatalf("prepare %s key: %v", want.id, err)
		}
		stored, err := collection.Get(key)
		if err != nil {
			t.Fatalf("get %s after reopen: %v", want.id, err)
		}
		raw := bson.Raw(stored)
		if want.generation != 0 {
			if got, ok := raw.Lookup("generation").Int32OK(); !ok || got != want.generation {
				t.Fatalf("%s generation=%d ok=%v, want %d", want.id, got, ok, want.generation)
			}
		}
		if want.name != "" {
			if got, ok := raw.Lookup("name").StringValueOK(); !ok || got != want.name {
				t.Fatalf("%s name=%q ok=%v, want %q", want.id, got, ok, want.name)
			}
		}
		if want.id == "u1" {
			if name, ok := raw.Lookup("profile", "name").StringValueOK(); !ok || name != "durable" {
				t.Fatalf("u1 profile after reopen=%v", raw.Lookup("profile"))
			}
			labels, labelsErr := raw.Lookup("labels").Array().Values()
			label, labelOK := "", false
			if len(labels) == 1 {
				label, labelOK = labels[0].StringValueOK()
			}
			if labelsErr != nil || len(labels) != 1 || !labelOK || label != "wal" {
				t.Fatalf("u1 labels after reopen=%v err=%v", labels, labelsErr)
			}
		}
		if want.id == "u2" {
			if by, ok := raw.Lookup("created", "by").StringValueOK(); !ok || by != "wal" {
				t.Fatalf("u2 created after reopen=%v", raw.Lookup("created"))
			}
		}
	}
	reopenedServer := NewServer()
	reopenedServer.Collections = collections.NewCollectionManager(reopened)
	reopenedServer.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	deleted := serveCommand(t, reopenedServer, 4, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u3"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, deleted)
	if values, err := bson.Raw(deleted).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 0 {
		t.Fatalf("deleted pointer document after reopen: values=%v err=%v", values, err)
	}
	surviving := serveCommand(t, reopenedServer, 5, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, surviving)
	if values, err := bson.Raw(surviving).Lookup("cursor").Document().Lookup("firstBatch").Array().Values(); err != nil || len(values) != 1 {
		t.Fatalf("surviving pointer document after reopen: values=%v err=%v", values, err)
	}
}

// A command can durably apply a prefix before a later item hits a normal write
// error.  That prefix is observable in the ok:1 reply and must survive a
// checkpoint/close/reopen even when every document is stored through the
// persistent value log.
func TestMongoMultiUpdatePartialWriteErrorCommandWALValueLogPointersReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	server := NewServer()
	server.Collections = collections.NewCollectionManager(backend)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 1, bson.D{{Key: "createIndexes", Value: "users"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 2, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "email", Value: "a@example.com"}, {Key: "payload", Value: string(make([]byte, 256))}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "email", Value: "b@example.com"}, {Key: "payload", Value: string(make([]byte, 256))}},
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 3, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: "committed"}}}}}},
		bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "b"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "email", Value: "a@example.com"}}}}}},
	}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	assertInt32(t, response, "n", 1)
	assertInt32(t, response, "nModified", 1)
	writeErrors, ok := bson.Raw(response).Lookup("writeErrors").ArrayOK()
	values, valuesErr := writeErrors.Values()
	if !ok || valuesErr != nil || len(values) != 1 {
		t.Fatalf("partial update writeErrors=%s", response)
	}
	if index, indexOK := values[0].Document().Lookup("index").Int32OK(); !indexOK || index != 1 {
		t.Fatalf("partial update writeErrors index=%d ok=%v, want 1", index, indexOK)
	}
	if err := server.Collections.FlushAll(); err != nil {
		_ = closeBackend()
		t.Fatalf("flush collections: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		_ = closeBackend()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := closeBackend(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	reopened, closeReopened, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	defer func() { _ = closeReopened() }()
	collection, err := collections.NewCollectionManager(reopened).OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	for _, want := range []struct{ id, state, email string }{
		{id: "a", state: "committed", email: "a@example.com"},
		{id: "b", state: "", email: "b@example.com"},
	} {
		key, _, keyErr := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: want.id}}), collections.DocumentFormatBSON)
		if keyErr != nil {
			t.Fatalf("prepare %s key: %v", want.id, keyErr)
		}
		stored, getErr := collection.Get(key)
		if getErr != nil || stored == nil {
			t.Fatalf("get %s after reopen: stored=%v err=%v", want.id, stored, getErr)
		}
		raw := bson.Raw(stored)
		if email, emailOK := raw.Lookup("email").StringValueOK(); !emailOK || email != want.email {
			t.Fatalf("%s email=%q ok=%v, want %q", want.id, email, emailOK, want.email)
		}
		if want.state == "" {
			if !raw.Lookup("state").IsZero() {
				t.Fatalf("failed update became durable for %s: %s", want.id, raw)
			}
		} else if state, stateOK := raw.Lookup("state").StringValueOK(); !stateOK || state != want.state {
			t.Fatalf("%s state=%q ok=%v, want %q", want.id, state, stateOK, want.state)
		}
		if payload, payloadOK := raw.Lookup("payload").StringValueOK(); !payloadOK || len(payload) != 256 {
			t.Fatalf("%s persistent value-log payload len=%d ok=%v", want.id, len(payload), payloadOK)
		}
	}
}

func TestFindAndModifyCommandWALValueLogPointersReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	closed := false
	closeOriginal := func() error {
		if closed {
			return nil
		}
		closed = true
		return closeBackend()
	}
	defer func() {
		if err := closeOriginal(); err != nil {
			t.Error(err)
		}
	}()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(backend)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 401, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: string(make([]byte, 256))}, {Key: "n", Value: int32(1)}}}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 402, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	if n, ok := bson.Raw(response).Lookup("value").Document().Lookup("n").Int32OK(); !ok || n != 2 {
		t.Fatalf("returned n=%d ok=%v", n, ok)
	}
	assertOK(t, serveCommand(t, server, 403, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "update", Value: bson.D{{Key: "payload", Value: string(make([]byte, 256))}}}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "$db", Value: "app"}}))
	if err := server.Collections.FlushAll(); err != nil {
		if closeErr := closeOriginal(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	if err := backend.Checkpoint(); err != nil {
		if closeErr := closeOriginal(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	if err := closeOriginal(); err != nil {
		t.Fatal(err)
	}
	reopened, closeReopened, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := closeReopened(); err != nil {
			t.Error(err)
		}
	}()
	collection, err := collections.NewCollectionManager(reopened).OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		id string
		n  int32
	}{{"u1", 2}, {"u2", 0}} {
		key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: want.id}}), collections.DocumentFormatBSON)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := collection.Get(key)
		if err != nil || stored == nil {
			t.Fatalf("get %s stored=%v err=%v", want.id, stored, err)
		}
		raw := bson.Raw(stored)
		if _, ok := raw.Lookup("payload").StringValueOK(); !ok {
			t.Fatalf("%s payload pointer did not resolve", want.id)
		}
		if want.n != 0 {
			if n, ok := raw.Lookup("n").Int32OK(); !ok || n != want.n {
				t.Fatalf("%s n=%d ok=%v", want.id, n, ok)
			}
		}
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
