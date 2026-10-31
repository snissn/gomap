package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// This is a support/lifecycle control, not a benchmark. Mongo wire BSON binary
// is intentionally unsupported by JSON-backed collections. Exercise the real
// official driver's error instead of stopping at a local fixture preflight.
func TestStandaloneOfficialDriverJSONBinaryRefusalNoEffectReopen(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			serverOptions := StandaloneOptions{Dir: dir, Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}
			open := func() *StandaloneServer {
				t.Helper()
				s, err := OpenStandaloneServer(serverOptions)
				if err != nil {
					t.Fatalf("open standalone: %v", err)
				}
				if got := s.Stats()["treedb.cache.memtable_mode"]; got != "cow_btree" || !s.Collections.UsesNativeCommandWALApply() {
					_ = s.Close()
					t.Fatalf("owner mode=%q native=%t, want COW native", got, s.Collections.UsesNativeCommandWALApply())
				}
				return s
			}
			ctx, cancelContext := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelContext()
			s := open()
			client, cancel, listener, served := startStandaloneMongoClientForTest(t, s)
			active := true
			defer func() {
				if active {
					stopStandaloneMongoClientForTest(t, client, cancel, listener, served, s)
				}
			}()
			db := client.Database("app")
			coll := db.Collection("sentinel", options.Collection().SetWriteConcern(writeconcern.W1()))
			if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "safe"}, {Key: "field", Value: "unchanged"}}); err != nil {
				t.Fatalf("insert sentinel: %v", err)
			}
			checkState := func(c *mongo.Client) {
				t.Helper()
				database := c.Database("app")
				names, err := database.ListCollectionNames(ctx, bson.D{})
				if err != nil {
					t.Fatalf("list collections: %v", err)
				}
				sort.Strings(names)
				if !reflect.DeepEqual(names, []string{"sentinel"}) {
					t.Fatalf("collections=%v, want sentinel only", names)
				}
				cursor, err := database.Collection("sentinel").Find(ctx, bson.D{})
				if err != nil {
					t.Fatalf("find sentinel: %v", err)
				}
				var docs []bson.M
				if err := cursor.All(ctx, &docs); err != nil {
					t.Fatalf("read all sentinel rows: %v", err)
				}
				want := []bson.M{{"_id": "safe", "field": "unchanged"}}
				if !reflect.DeepEqual(docs, want) {
					t.Fatalf("rows=%v want %v", docs, want)
				}
			}
			checkState(client)
			payload := bytes.Repeat([]byte{0x41}, 100)
			for _, name := range []string{"sentinel", "must_not_be_created"} {
				target := db.Collection(name, options.Collection().SetWriteConcern(writeconcern.W1()))
				result, err := target.InsertOne(ctx, bson.D{{Key: "_id", Value: "must-not-exist"}, {Key: "field", Value: bson.Binary{Subtype: 0, Data: payload}}})
				var commandErr mongo.CommandError
				if !errors.As(err, &commandErr) || commandErr.Code != commandCodeBadValue || commandErr.Name != "BadValue" || commandErr.Message != `Mongo document field "field" uses unsupported BSON type binary` {
					t.Fatalf("insert %s result=%v error=%#v; want precise server BadValue binary refusal", name, result, err)
				}
				rawMessage, rawOK := commandErr.Raw.Lookup("errmsg").StringValueOK()
				if len(commandErr.Raw) == 0 || !rawOK || rawMessage != commandErr.Message {
					t.Fatalf("insert %s missing matching actual server response: %s", name, commandErr.Raw)
				}
				t.Logf("official driver refusal collection=%s configured_w=1 code=%d name=%s response=%s", name, commandErr.Code, commandErr.Name, commandErr.Raw)
				checkState(client)
			}
			// This is the same owner's complete accepted-prefix checkpoint. It is
			// not a public Mongo checkpoint API or an ACK durability assertion.
			if err := s.Collections.FlushAll(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if err := s.Collections.CheckpointStorage(); err != nil {
				t.Fatalf("owner checkpoint: %v", err)
			}
			active = false
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, s)

			s = open()
			client, cancel, listener, served = startStandaloneMongoClientForTest(t, s)
			active = true
			checkState(client)
			active = false
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, s)
		})
	}
}
