package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestStandaloneServerOfficialGoDriverBSONSetBinaryUpsert(t *testing.T) {
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
	opCtx, opCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer opCancel()
	result, err := client.Database("app").Collection("users").UpdateOne(opCtx,
		bson.D{{Key: "_id", Value: "binary-upsert"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte{1, 2, 3}}}}}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		t.Fatalf("driver BSON binary upsert: %v", err)
	}
	if result.MatchedCount != 0 || result.ModifiedCount != 0 || result.UpsertedCount != 1 || result.UpsertedID != "binary-upsert" {
		t.Fatalf("binary upsert result=%+v", result)
	}
	result, err = client.Database("app").Collection("users").UpdateOne(opCtx,
		bson.D{{Key: "_id", Value: "binary-upsert"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: bson.Binary{Subtype: 0x00, Data: []byte{4, 5}}}}}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		t.Fatalf("driver BSON binary matched upsert: %v", err)
	}
	if result.MatchedCount != 1 || result.ModifiedCount != 1 || result.UpsertedCount != 0 {
		t.Fatalf("matched binary upsert result=%+v", result)
	}
	var stored bson.Raw
	if err := client.Database("app").Collection("users").FindOne(opCtx, bson.D{{Key: "_id", Value: "binary-upsert"}}).Decode(&stored); err != nil {
		t.Fatalf("driver find binary upsert: %v", err)
	}
	subtype, payload := stored.Lookup("payload").Binary()
	if subtype != 0x00 || string(payload) != string([]byte{4, 5}) {
		t.Fatalf("driver binary payload subtype/data=%#x/%v", subtype, payload)
	}
}

func TestStandaloneServerOfficialGoDriverExplainReadPlans(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	db := client.Database("app")
	if _, err := db.Collection("users").InsertMany(ctx, []any{bson.D{{Key: "_id", Value: "a"}, {Key: "city", Value: "hnl"}}, bson.D{{Key: "_id", Value: "b"}, {Key: "city", Value: "sea"}}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var response bson.Raw
	// The official driver converts a deadline to maxTimeMS, which this
	// deliberately fail-closed explain surface rejects.
	err = db.RunCommand(context.Background(), bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}}}, {Key: "verbosity", Value: "executionStats"}}).Decode(&response)
	if err != nil {
		t.Fatalf("driver explain: %v", err)
	}
	planner, ok := response.Lookup("queryPlanner").DocumentOK()
	stage, stageOK := planner.Lookup("winningPlan").Document().Lookup("stage").StringValueOK()
	if !ok || !stageOK || stage != "primary_lookup" {
		t.Fatalf("explain response=%s", response)
	}
	stats, ok := response.Lookup("executionStats").DocumentOK()
	if returned, returnedOK := stats.Lookup("nReturned").Int64OK(); !ok || !returnedOK || returned != 1 {
		t.Fatalf("explain executionStats=%s", response)
	}
}

func TestStandaloneServerOfficialGoDriverNegativeDottedQueries(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	ctx := context.Background()
	coll := client.Database("app").Collection("events")
	if _, err := coll.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: "missing"}, {Key: "state", Value: "missing"}},
		bson.D{{Key: "_id", Value: "null"}, {Key: "score", Value: nil}, {Key: "state", Value: "null"}},
		bson.D{{Key: "_id", Value: "live"}, {Key: "score", Value: int32(7)}, {Key: "state", Value: "live"}, {Key: "profile", Value: bson.D{{Key: "name", Value: "Ada"}, {Key: "age", Value: int32(37)}}}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	nonNull := bson.D{{Key: "score", Value: bson.D{{Key: "$ne", Value: nil}}}}
	cursor, err := coll.Find(ctx, nonNull, options.Find().SetProjection(bson.D{{Key: "profile.name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}).SetSort(bson.D{{Key: "profile.name", Value: int32(1)}}))
	if err != nil {
		t.Fatalf("negative dotted find: %v", err)
	}
	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil || len(docs) != 1 || docs[0].Lookup("profile", "name").StringValue() != "Ada" || docs[0].Lookup("_id").Type != 0 {
		t.Fatalf("negative dotted find docs=%v err=%v", docs, err)
	}
	if count, err := coll.CountDocuments(ctx, nonNull); err != nil || count != 1 {
		t.Fatalf("negative count=%d err=%v", count, err)
	}
	values, err := coll.Distinct(ctx, "state", nonNull).Raw()
	if err != nil {
		t.Fatalf("negative distinct: %v", err)
	}
	valuesRaw, err := values.Values()
	if err != nil || len(valuesRaw) != 1 || valuesRaw[0].StringValue() != "live" {
		t.Fatalf("negative distinct values=%v err=%v", valuesRaw, err)
	}
	result, err := coll.UpdateOne(ctx, bson.D{{Key: "$nor", Value: bson.A{
		bson.D{{Key: "score", Value: bson.D{{Key: "$exists", Value: false}}}}, bson.D{{Key: "score", Value: nil}},
	}}}, bson.D{{Key: "$set", Value: bson.D{{Key: "updated", Value: true}}}})
	if err != nil || result.MatchedCount != 1 || result.ModifiedCount != 1 {
		t.Fatalf("negative update result=%+v err=%v", result, err)
	}
	var before bson.Raw
	if err := coll.FindOneAndUpdate(ctx, nonNull, bson.D{{Key: "$set", Value: bson.D{{Key: "modified", Value: true}}}}, options.FindOneAndUpdate().SetProjection(bson.D{{Key: "profile.name", Value: int32(1)}, {Key: "_id", Value: int32(0)}})).Decode(&before); err != nil || before.Lookup("profile", "name").StringValue() != "Ada" || before.Lookup("_id").Type != 0 {
		t.Fatalf("negative findAndModify before=%s err=%v", before, err)
	}
}

func TestStandaloneServerOfficialGoDriverCompoundPlanner(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatalf("open standalone: %v", err)
			}
			client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
			defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
			ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelCtx()
			coll := client.Database("app").Collection("events")
			if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(-1)}}}); err != nil {
				t.Fatalf("create compound index: %v", err)
			}
			if _, err := coll.InsertMany(ctx, []any{
				bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}},
				bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(2)}},
				bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: "other"}, {Key: "score", Value: int32(3)}},
			}); err != nil {
				t.Fatalf("seed compound planner: %v", err)
			}
			cursor, err := coll.Find(ctx, bson.D{{Key: "tenant", Value: "acme"}}, options.Find().SetHint("tenant_1_score_-1").SetSort(bson.D{{Key: "score", Value: int32(-1)}}).SetBatchSize(1))
			if err != nil {
				t.Fatalf("driver compound hinted find: %v", err)
			}
			var docs []bson.M
			if err := cursor.All(ctx, &docs); err != nil {
				t.Fatalf("driver compound hinted cursor: %v", err)
			}
			if len(docs) != 2 || docs[0]["_id"] != "b" || docs[1]["_id"] != "a" {
				t.Fatalf("driver compound docs=%v want b,a", docs)
			}
			// These commands deliberately reuse the same supported match/sort shape;
			// they are driver-level coverage of planner fallback/reuse, not a claim that
			// MongoDB exposes the same physical plan for count/distinct/aggregate.
			// Count/distinct/aggregate currently reject driver-generated maxTimeMS, so
			// use an explicitly unbounded test context for these supported commands.
			queryCtx := context.Background()
			count, err := coll.CountDocuments(queryCtx, bson.D{{Key: "tenant", Value: "acme"}})
			if err != nil || count != 2 {
				t.Fatalf("driver compound count=%d err=%v want 2", count, err)
			}
			values, err := coll.Distinct(queryCtx, "score", bson.D{{Key: "tenant", Value: "acme"}}).Raw()
			distinctValues, valuesErr := values.Values()
			if err != nil || valuesErr != nil || len(distinctValues) != 2 {
				t.Fatalf("driver compound distinct=%v err=%v valuesErr=%v", values, err, valuesErr)
			}
			agg, err := coll.Aggregate(queryCtx, mongo.Pipeline{
				bson.D{{Key: "$match", Value: bson.D{{Key: "tenant", Value: "acme"}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "score", Value: int32(-1)}}}},
			})
			if err != nil {
				t.Fatalf("driver compound aggregate: %v", err)
			}
			var aggregateDocs []bson.M
			if err := agg.All(queryCtx, &aggregateDocs); err != nil || len(aggregateDocs) != 2 || aggregateDocs[0]["_id"] != "b" {
				t.Fatalf("driver compound aggregate docs=%v err=%v", aggregateDocs, err)
			}

		})
	}
}

func TestStandaloneServerOfficialGoDriverBoundedMultiWrites(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	coll := client.Database("app").Collection("users")
	if _, err := coll.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: "a"}, {Key: "city", Value: "hnl"}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "city", Value: "hnl"}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "city", Value: "sea"}},
	}); err != nil {
		t.Fatalf("seed InsertMany: %v", err)
	}
	updated, err := coll.UpdateMany(ctx, bson.D{{Key: "city", Value: "hnl"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}})
	if err != nil || updated.MatchedCount != 2 || updated.ModifiedCount != 2 {
		t.Fatalf("UpdateMany result=%+v err=%v, want matched/modified 2/2", updated, err)
	}
	deleted, err := coll.DeleteMany(ctx, bson.D{{Key: "city", Value: "sea"}})
	if err != nil || deleted.DeletedCount != 1 {
		t.Fatalf("DeleteMany result=%+v err=%v, want 1", deleted, err)
	}
	bulk, err := coll.BulkWrite(ctx, []mongo.WriteModel{
		mongo.NewUpdateOneModel().SetFilter(bson.D{{Key: "_id", Value: "a"}}).SetUpdate(bson.D{{Key: "$set", Value: bson.D{{Key: "bulk", Value: int32(1)}}}}),
		mongo.NewUpdateOneModel().SetFilter(bson.D{{Key: "_id", Value: "b"}}).SetUpdate(bson.D{{Key: "$set", Value: bson.D{{Key: "bulk", Value: int32(2)}}}}),
	})
	if err != nil || bulk.MatchedCount != 2 || bulk.ModifiedCount != 2 {
		t.Fatalf("BulkWrite update result=%+v err=%v, want matched/modified 2/2", bulk, err)
	}
}

func TestStandaloneServerOfficialGoDriverFindOneAndModify(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	coll := client.Database("app").Collection("users")
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "u1"}, {Key: "n", Value: int32(1)}, {Key: "name", Value: "ada"}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var before bson.M
	if err := coll.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: "u1"}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}).Decode(&before); err != nil {
		t.Fatalf("FindOneAndUpdate before: %v", err)
	}
	if before["n"] != int32(1) {
		t.Fatalf("before=%v", before)
	}
	var after bson.M
	if err := coll.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: "u1"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "grace"}, {Key: "profile.name", Value: "grace"}}}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&after); err != nil {
		t.Fatalf("FindOneAndUpdate after: %v", err)
	}
	afterRaw, err := bson.Marshal(after)
	if err != nil || after["name"] != "grace" || bson.Raw(afterRaw).Lookup("profile", "name").StringValue() != "grace" {
		t.Fatalf("after=%v", after)
	}
	var replacementBefore bson.M
	if err := coll.FindOneAndReplace(ctx, bson.D{{Key: "_id", Value: "u1"}}, bson.D{{Key: "name", Value: "replace-before"}}).Decode(&replacementBefore); err != nil {
		t.Fatalf("FindOneAndReplace before: %v", err)
	}
	if replacementBefore["name"] != "grace" {
		t.Fatalf("replacement before=%v", replacementBefore)
	}
	var replaced bson.M
	if err := coll.FindOneAndReplace(ctx, bson.D{{Key: "_id", Value: "u1"}}, bson.D{{Key: "name", Value: "replace"}}, options.FindOneAndReplace().SetReturnDocument(options.After)).Decode(&replaced); err != nil {
		t.Fatalf("FindOneAndReplace: %v", err)
	}
	if replaced["name"] != "replace" || replaced["_id"] != "u1" {
		t.Fatalf("replaced=%v", replaced)
	}
	var upserted bson.M
	if err := coll.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: "u2"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "upsert"}}}, {Key: "$setOnInsert", Value: bson.D{{Key: "profile.created", Value: true}}}}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&upserted); err != nil {
		t.Fatalf("FindOneAndUpdate upsert: %v", err)
	}
	upsertedRaw, err := bson.Marshal(upserted)
	if err != nil || upserted["_id"] != "u2" || upserted["name"] != "upsert" || !bson.Raw(upsertedRaw).Lookup("profile", "created").Boolean() {
		t.Fatalf("upserted=%v", upserted)
	}
	var replacementUpsert bson.M
	if err := coll.FindOneAndReplace(ctx, bson.D{{Key: "_id", Value: "u3"}}, bson.D{{Key: "name", Value: "replacement upsert"}}, options.FindOneAndReplace().SetUpsert(true).SetReturnDocument(options.After)).Decode(&replacementUpsert); err != nil {
		t.Fatalf("FindOneAndReplace upsert: %v", err)
	}
	if replacementUpsert["_id"] != "u3" || replacementUpsert["name"] != "replacement upsert" {
		t.Fatalf("replacement upsert=%v", replacementUpsert)
	}
	for _, test := range []struct {
		id, name string
		replace  bool
	}{{"u4", "default update upsert", false}, {"u5", "default replacement upsert", true}} {
		var value bson.M
		var err error
		if test.replace {
			err = coll.FindOneAndReplace(ctx, bson.D{{Key: "_id", Value: test.id}}, bson.D{{Key: "name", Value: test.name}}, options.FindOneAndReplace().SetUpsert(true)).Decode(&value)
		} else {
			err = coll.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: test.id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: test.name}}}}, options.FindOneAndUpdate().SetUpsert(true)).Decode(&value)
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			t.Fatalf("default upsert %s err=%v want ErrNoDocuments", test.id, err)
		}
		if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: test.id}}).Decode(&value); err != nil || value["name"] != test.name {
			t.Fatalf("default upsert %s persisted value=%v err=%v", test.id, value, err)
		}
	}
}

func TestStandaloneServerOfficialGoDriverFilterWrites(t *testing.T) {
	decimalOne, err := bson.ParseDecimal128("1")
	if err != nil {
		t.Fatal(err)
	}
	decimalPositiveInfinity, err := bson.ParseDecimal128("Infinity")
	if err != nil {
		t.Fatal(err)
	}
	decimalNegativeInfinity, err := bson.ParseDecimal128("-Infinity")
	if err != nil {
		t.Fatal(err)
	}
	decimalNaN, err := bson.ParseDecimal128("NaN")
	if err != nil {
		t.Fatal(err)
	}
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	coll := client.Database("app").Collection("users")
	if _, err := coll.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(20)}, {Key: "active", Value: true}, {Key: "numbers", Value: bson.A{int32(1)}}, {Key: "nestedNumbers", Value: bson.A{bson.D{{Key: "n", Value: int32(1)}}}}, {Key: "nonFinite", Value: bson.A{decimalPositiveInfinity, decimalNaN}}}, bson.D{{Key: "_id", Value: "u2"}, {Key: "age", Value: int32(30)}, {Key: "active", Value: true}}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	updated, err := coll.UpdateOne(ctx, bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(20)}}}}, bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}})
	if err != nil || updated.MatchedCount != 1 || updated.ModifiedCount != 1 {
		t.Fatalf("filter UpdateOne result=%+v err=%v", updated, err)
	}
	updated, err = coll.UpdateOne(ctx, bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(20)}}}}, bson.D{
		{Key: "$set", Value: bson.D{{Key: "profile.name", Value: "ada"}, {Key: "profile.blob", Value: bson.Binary{Subtype: 0x80, Data: []byte{1, 2}}}}},
		{Key: "$inc", Value: bson.D{{Key: "profile.logins", Value: int32(1)}}},
		{Key: "$push", Value: bson.D{{Key: "events", Value: bson.D{{Key: "kind", Value: "login"}}}}},
		{Key: "$addToSet", Value: bson.D{{Key: "labels", Value: bson.D{{Key: "$each", Value: bson.A{"staff", "staff"}}}}}},
		{Key: "$addToSet", Value: bson.D{{Key: "numbers", Value: bson.D{{Key: "$each", Value: bson.A{int64(1), float64(1), decimalOne, int32(2)}}}}}},
		{Key: "$addToSet", Value: bson.D{{Key: "nestedNumbers", Value: bson.D{{Key: "$each", Value: bson.A{bson.D{{Key: "n", Value: int64(1)}}, bson.D{{Key: "n", Value: decimalOne}}, bson.D{{Key: "n", Value: int32(2)}}}}}}}},
		{Key: "$addToSet", Value: bson.D{{Key: "nonFinite", Value: bson.D{{Key: "$each", Value: bson.A{decimalNegativeInfinity, decimalPositiveInfinity, decimalNaN, decimalNaN}}}}}},
	})
	if err != nil || updated.MatchedCount != 1 || updated.ModifiedCount != 1 {
		t.Fatalf("nested filter UpdateOne result=%+v err=%v", updated, err)
	}
	var nested bson.Raw
	if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: "u1"}}).Decode(&nested); err != nil {
		t.Fatalf("find nested update: %v", err)
	}
	profile := nested.Lookup("profile").Document()
	events, eventsErr := nested.Lookup("events").Array().Values()
	labels, labelsErr := nested.Lookup("labels").Array().Values()
	numbers, numbersErr := nested.Lookup("numbers").Array().Values()
	nestedNumbers, nestedNumbersErr := nested.Lookup("nestedNumbers").Array().Values()
	nonFinite, nonFiniteErr := nested.Lookup("nonFinite").Array().Values()
	if subtype, value := profile.Lookup("blob").Binary(); profile.Lookup("name").StringValue() != "ada" || profile.Lookup("logins").Int32() != 1 || subtype != 0x80 || string(value) != string([]byte{1, 2}) || eventsErr != nil || len(events) != 1 || events[0].Document().Lookup("kind").StringValue() != "login" || labelsErr != nil || len(labels) != 1 || labels[0].StringValue() != "staff" || numbersErr != nil || len(numbers) != 2 || numbers[0].Type != bson.TypeInt32 || numbers[1].Int32() != 2 || nestedNumbersErr != nil || len(nestedNumbers) != 2 || nestedNumbers[0].Document().Lookup("n").Int32() != 1 || nestedNumbers[1].Document().Lookup("n").Int32() != 2 || nonFiniteErr != nil || len(nonFinite) != 3 || nonFinite[0].Decimal128().String() != "Infinity" || nonFinite[1].Decimal128().String() != "NaN" || nonFinite[2].Decimal128().String() != "-Infinity" {
		t.Fatalf("nested update document=%v", nested)
	}
	var before bson.M
	if err := coll.FindOneAndUpdate(ctx, bson.D{{Key: "active", Value: true}}, bson.D{{Key: "$set", Value: bson.D{{Key: "modified", Value: true}}}, {Key: "$unset", Value: bson.D{{Key: "profile.name", Value: true}}}, {Key: "$setOnInsert", Value: bson.D{{Key: "created", Value: true}}}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&before); err != nil {
		t.Fatalf("filter FindOneAndUpdate: %v", err)
	}
	if before["_id"] != "u1" {
		t.Fatalf("filter FindOneAndUpdate selected %v want u1", before)
	}
	if _, ok := before["created"]; ok {
		t.Fatalf("matched FindOneAndUpdate applied $setOnInsert: %v", before)
	}
	replaced, err := coll.ReplaceOne(ctx, bson.D{{Key: "modified", Value: true}}, bson.D{{Key: "name", Value: "replacement"}, {Key: "age", Value: int32(20)}})
	if err != nil || replaced.MatchedCount != 1 || replaced.ModifiedCount != 1 {
		t.Fatalf("filter ReplaceOne result=%+v err=%v", replaced, err)
	}
	var replaceBefore bson.M
	if err := coll.FindOneAndReplace(ctx, bson.D{{Key: "active", Value: true}}, bson.D{{Key: "name", Value: "find-replacement"}}).Decode(&replaceBefore); err != nil {
		t.Fatalf("filter FindOneAndReplace: %v", err)
	}
	if replaceBefore["_id"] != "u2" {
		t.Fatalf("filter FindOneAndReplace selected %v want u2", replaceBefore)
	}
	deleted, err := coll.DeleteOne(ctx, bson.D{{Key: "age", Value: int32(20)}})
	if err != nil || deleted.DeletedCount != 1 {
		t.Fatalf("filter DeleteOne result=%+v err=%v", deleted, err)
	}
}

func TestStandaloneServerOfficialDriverConcurrentFirstWriteFindAndModifyUpserts(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
	const workers = 16
	// This checks atomic first-write concurrency, not the latency of durable syncs.
	ctx, cancelOperations := context.WithTimeout(t.Context(), time.Minute)
	defer cancelOperations()
	var images [workers]int32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	coll := client.Database("app").Collection("users")
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var value bson.M
			err := coll.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: "u1"}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&value)
			if err != nil {
				errs <- err
				return
			}
			if n, ok := value["n"].(int32); !ok || n < 1 || n > workers {
				errs <- fmt.Errorf("returned value=%v", value)
			} else {
				images[i] = n
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var seen [workers + 1]bool
	for _, n := range images {
		if n < 1 || n > workers || seen[n] {
			t.Fatalf("returned increments=%v want each of 1..%d once", images, workers)
		}
		seen[n] = true
	}
	var stored bson.M
	if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: "u1"}}).Decode(&stored); err != nil {
		t.Fatal(err)
	}
	if n, ok := stored["n"].(int32); !ok || n != workers {
		t.Fatalf("stored n=%v want %d", stored["n"], workers)
	}
}

func TestServerOfficialGoDriverBasicCRUD(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	if err := client.Ping(opCtx, nil); err != nil {
		t.Fatalf("driver ping: %v", err)
	}

	coll := client.Database("app").Collection("users")
	id := bson.NewObjectID()
	if _, err := coll.InsertOne(opCtx, bson.D{
		{Key: "_id", Value: id},
		{Key: "name", Value: "ada"},
		{Key: "age", Value: int64(37)},
	}); err != nil {
		t.Fatalf("driver insert one: %v", err)
	}
	names, err := client.ListDatabaseNames(opCtx, bson.D{{Key: "name", Value: "app"}})
	if err != nil {
		t.Fatalf("driver list database names: %v", err)
	}
	if len(names) != 1 || names[0] != "app" {
		t.Fatalf("driver list database names=%v want [app]", names)
	}

	var got bson.M
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: id}}).Decode(&got); err != nil {
		t.Fatalf("driver find one: %v", err)
	}
	if got["_id"] != id {
		t.Fatalf("decoded _id=%v want %v", got["_id"], id)
	}
	if got["name"] != "ada" {
		t.Fatalf("decoded name=%v want ada", got["name"])
	}
	if got["age"] != int64(37) {
		t.Fatalf("decoded age=%v want 37", got["age"])
	}
	if _, err := coll.InsertMany(opCtx, []any{
		bson.D{{Key: "_id", Value: "or-city"}, {Key: "active", Value: true}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(30)}},
		bson.D{{Key: "_id", Value: "or-age"}, {Key: "active", Value: true}, {Key: "city", Value: "lax"}, {Key: "age", Value: int64(45)}},
		bson.D{{Key: "_id", Value: "or-inactive"}, {Key: "active", Value: false}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(45)}},
	}); err != nil {
		t.Fatalf("driver insert many for $or: %v", err)
	}
	cursor, err := coll.Find(opCtx, bson.D{{Key: "active", Value: true}, {Key: "$or", Value: bson.A{
		bson.D{{Key: "city", Value: "hnl"}},
		bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(40)}}}},
	}}})
	if err != nil {
		t.Fatalf("driver find $or: %v", err)
	}
	defer func() { _ = cursor.Close(opCtx) }()
	var orDocs []bson.M
	if err := cursor.All(opCtx, &orDocs); err != nil {
		t.Fatalf("driver decode $or: %v", err)
	}
	if len(orDocs) != 2 {
		t.Fatalf("driver $or documents=%d want 2", len(orDocs))
	}

	updateResult, err := coll.UpdateOne(opCtx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "age", Value: int64(38)},
			{Key: "city", Value: "London"},
		}}},
	)
	if err != nil {
		t.Fatalf("driver update one: %v", err)
	}
	if updateResult.MatchedCount != 1 || updateResult.ModifiedCount != 1 {
		t.Fatalf("update result matched=%d modified=%d want 1/1", updateResult.MatchedCount, updateResult.ModifiedCount)
	}

	got = nil
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: id}}).Decode(&got); err != nil {
		t.Fatalf("driver find one after update: %v", err)
	}
	if got["age"] != int64(38) {
		t.Fatalf("decoded updated age=%v want 38", got["age"])
	}
	if got["city"] != "London" {
		t.Fatalf("decoded city=%v want London", got["city"])
	}

	mutationResult, err := coll.UpdateOne(opCtx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(2)}}}, {Key: "$unset", Value: bson.D{{Key: "city", Value: true}}}},
	)
	if err != nil {
		t.Fatalf("driver generic update one: %v", err)
	}
	if mutationResult.MatchedCount != 1 || mutationResult.ModifiedCount != 1 {
		t.Fatalf("generic update matched=%d modified=%d want 1/1", mutationResult.MatchedCount, mutationResult.ModifiedCount)
	}
	replaceResult, err := coll.ReplaceOne(opCtx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "name", Value: "grace"}, {Key: "age", Value: int64(40)}})
	if err != nil {
		t.Fatalf("driver replace one: %v", err)
	}
	if replaceResult.MatchedCount != 1 || replaceResult.ModifiedCount != 1 {
		t.Fatalf("replace result matched=%d modified=%d want 1/1", replaceResult.MatchedCount, replaceResult.ModifiedCount)
	}
	got = nil
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: id}}).Decode(&got); err != nil {
		t.Fatalf("driver find one after replacement: %v", err)
	}
	if got["name"] != "grace" || got["age"] != int64(40) {
		t.Fatalf("replacement document=%v", got)
	}
	if _, ok := got["city"]; ok {
		t.Fatalf("replacement retained city: %v", got)
	}

	upsertID := bson.NewObjectID()
	upsertResult, err := coll.UpdateOne(opCtx, bson.D{{Key: "_id", Value: upsertID}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		t.Fatalf("driver modifier upsert: %v", err)
	}
	if upsertResult.MatchedCount != 0 || upsertResult.ModifiedCount != 0 || upsertResult.UpsertedCount != 1 || upsertResult.UpsertedID != upsertID {
		t.Fatalf("modifier upsert result=%+v want inserted %v", upsertResult, upsertID)
	}
	replacementID := bson.NewObjectID()
	replacementUpsert, err := coll.ReplaceOne(opCtx, bson.D{{Key: "_id", Value: replacementID}}, bson.D{{Key: "name", Value: "upserted"}}, options.Replace().SetUpsert(true))
	if err != nil {
		t.Fatalf("driver replacement upsert: %v", err)
	}
	if replacementUpsert.MatchedCount != 0 || replacementUpsert.ModifiedCount != 0 || replacementUpsert.UpsertedCount != 1 || replacementUpsert.UpsertedID != replacementID {
		t.Fatalf("replacement upsert result=%+v want inserted %v", replacementUpsert, replacementID)
	}

	deleteResult, err := coll.DeleteOne(opCtx, bson.D{{Key: "_id", Value: id}})
	if err != nil {
		t.Fatalf("driver delete one: %v", err)
	}
	if deleteResult.DeletedCount != 1 {
		t.Fatalf("delete result deleted=%d want 1", deleteResult.DeletedCount)
	}
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: id}}).Decode(&got); !errors.Is(err, mongo.ErrNoDocuments) {
		t.Fatalf("driver find one after delete err=%v want ErrNoDocuments", err)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve loop did not stop")
	}
}

func TestServerOfficialGoDriverLogicalSession(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	if err := client.Ping(opCtx, nil); err != nil {
		t.Fatalf("driver ping: %v", err)
	}

	session, err := client.StartSession()
	if err != nil {
		t.Fatalf("driver start session: %v", err)
	}
	defer session.EndSession(context.Background())

	if err := mongo.WithSession(opCtx, session, func(sessionCtx context.Context) error {
		return client.Database("admin").RunCommand(sessionCtx, bson.D{{Key: "ping", Value: int32(1)}}).Err()
	}); err != nil {
		t.Fatalf("driver session ping: %v", err)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve loop did not stop")
	}
}

func TestServerOfficialGoDriverUnacknowledgedInsertMany(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetMaxPoolSize(1).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	if err := client.Ping(opCtx, nil); err != nil {
		t.Fatalf("driver ping: %v", err)
	}

	ackColl := client.Database("app").Collection("users")
	unackColl := client.Database("app").Collection("users", options.Collection().SetWriteConcern(writeconcern.Unacknowledged()))
	result, err := unackColl.InsertMany(opCtx, []any{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}},
	})
	if err != nil {
		t.Fatalf("unacknowledged insert many: %v", err)
	}
	if result.Acknowledged {
		t.Fatal("unacknowledged insert reported Acknowledged=true")
	}
	deadline := time.Now().Add(time.Second)
	for server.StandaloneWriteConcernStats().PreMutationRejections == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	var got bson.M
	err = ackColl.FindOne(opCtx, bson.D{{Key: "_id", Value: "u2"}}).Decode(&got)
	if !errors.Is(err, mongo.ErrNoDocuments) {
		t.Fatalf("rejected unacknowledged write find err=%v, want no documents", err)
	}
	if err := client.Ping(opCtx, nil); err != nil {
		t.Fatalf("connection after rejected moreToCome write: %v", err)
	}
	stats := server.StandaloneWriteConcernStats()
	if stats.PreMutationRejections != 1 || stats.LogicalWrites != 0 {
		t.Fatalf("unacknowledged write stats=%+v", stats)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve err: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

// TestStandaloneServerOfficialDriverJournalFenceWithoutCheckpoint first checks
// the acknowledged native route and complete checked lifecycle. Checkpoints are
// the selected baseline side effect: that assertion must precede candidate-only
// receipt force-count checks so a missing new route cannot obscure that RED.
func TestStandaloneServerOfficialDriverJournalFenceWithoutCheckpoint(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			result := characterizeStandaloneNativeJournalDriver(t, profile)
			if result.checkpoints != 0 {
				t.Fatalf("explicit journal ACK constructed cache-owner checkpoints: delta=%d, want 0", result.checkpoints)
			}
			if result.forcedGroups != 0 {
				t.Fatalf("covered operation issued redundant forced groups=%d for journal ACK=%d", result.forcedGroups, result.acknowledged)
			}
		})
	}
}

type standaloneNativeJournalCharacterization struct {
	acknowledged, checkpoints, forcedGroups uint64
}

func requireStandaloneNativeJournalOwner(tb testing.TB, standalone *StandaloneServer) {
	tb.Helper()
	if standalone.Backend == nil || standalone.Collections == nil || standalone.ownerStats == nil || standalone.cleanup == nil ||
		!standalone.Backend.CommandWALEnabled() || !standalone.Collections.UsesNativeCommandWALApply() {
		tb.Fatal("standalone must retain its original public CommandWAL/native owner and cleanup")
	}
	stats := standalone.Stats()
	if mode := stats["treedb.cache.memtable_mode"]; mode != "cow_btree" {
		tb.Fatalf("original cache-owner mode=%q want cow_btree", mode)
	}
	// These are original public completion-group and cache-owner counters, not
	// backend aliases or the public wrapper's Checkpoint API call count.
	for _, key := range []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.group_commit.forced_total", "treedb.command_wal.sync.count_total", "treedb.command_wal.file_sync.calls_total"} {
		writeConcernBenchmarkStat(tb, stats, key)
	}
}

func standaloneNativeJournalDocument(id string, generation int) bson.D {
	doc := bson.D{{Key: "_id", Value: id}}
	for field := 0; field < 10; field++ {
		payload := make([]byte, 100)
		for i := range payload {
			payload[i] = byte(generation*31 + field*7 + i)
		}
		doc = append(doc, bson.E{Key: fmt.Sprintf("field%d", field), Value: bson.Binary{Subtype: 0, Data: payload}})
	}
	return doc
}

func requireStandaloneNativeJournalDocument(tb testing.TB, raw bson.Raw, id string, generation int) {
	tb.Helper()
	if got, ok := raw.Lookup("_id").StringValueOK(); !ok || got != id {
		tb.Fatalf("document _id=%q string=%t want %q", got, ok, id)
	}
	fields, err := raw.Elements()
	if err != nil || len(fields) != 11 {
		tb.Fatalf("document fields=%d err=%v want _id plus ten binary fields", len(fields), err)
	}
	for _, field := range standaloneNativeJournalDocument(id, generation)[1:] {
		want := field.Value.(bson.Binary)
		subtype, payload, ok := raw.Lookup(field.Key).BinaryOK()
		if !ok || subtype != want.Subtype || !bytes.Equal(payload, want.Data) {
			tb.Fatalf("document %s field %s: binary=%t subtype=%d bytes=%v want subtype=%d bytes=%v", id, field.Key, ok, subtype, payload, want.Subtype, want.Data)
		}
	}
}

func characterizeStandaloneNativeJournalDriver(t *testing.T, profile treedb.Profile) standaloneNativeJournalCharacterization {
	t.Helper()
	const count = 8 // Fresh ~10 KiB fixture; default product maintenance stays enabled.
	opts := StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
	open := func() *StandaloneServer {
		t.Helper()
		s, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatalf("open standalone: %v", err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("checked standalone cleanup: %v", err)
			}
		})
		requireStandaloneNativeJournalOwner(t, s)
		return s
	}
	standalone := open()
	client, cancel, listener, served := startStandaloneMongoClientForTest(t, standalone)
	active := true
	defer func() {
		if active {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
		}
	}()
	ctx, cancelContext := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelContext()
	// Precreate before the observed request interval; inserts exercise the real
	// official driver's explicit w:1,j:true, including wire dispatch and ACK.
	if err := client.Database("app").CreateCollection(ctx, "journal_fence"); err != nil {
		t.Fatalf("precreate BSON collection: %v", err)
	}
	journal := true
	collection := client.Database("app").Collection("journal_fence", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
	before := standalone.Stats()
	beforeACK := standalone.Server.StandaloneWriteConcernStats()
	var acknowledged uint64
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("journal-%02d", i)
		result, err := collection.InsertOne(ctx, standaloneNativeJournalDocument(id, i))
		if err != nil || result == nil || result.InsertedID != id {
			t.Fatalf("journal insert %s result=%+v err=%v", id, result, err)
		}
		acknowledged++
	}
	after := standalone.Stats()
	afterACK := standalone.Server.StandaloneWriteConcernStats()
	t.Logf("native original owner profile=%s mode=%s acknowledged=%d checkpoints=%s->%s forced_groups=%s->%s WAL_sync_operations=%s->%s WAL_file_sync_calls=%s->%s before_ack=%+v after_ack=%+v", profile, after["treedb.cache.memtable_mode"], acknowledged,
		before["treedb.cache.checkpoint.runs"], after["treedb.cache.checkpoint.runs"],
		before["treedb.command_wal.group_commit.forced_total"], after["treedb.command_wal.group_commit.forced_total"],
		before["treedb.command_wal.sync.count_total"], after["treedb.command_wal.sync.count_total"],
		before["treedb.command_wal.file_sync.calls_total"], after["treedb.command_wal.file_sync.calls_total"], beforeACK, afterACK)
	if acknowledged != count || afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != count ||
		afterACK.SyncAttempts-beforeACK.SyncAttempts != count || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != count ||
		afterACK.SyncFailures != beforeACK.SyncFailures || afterACK.UncertainOutcomes != beforeACK.UncertainOutcomes {
		t.Fatalf("journal ACK/boundary mismatch acknowledged=%d before=%+v after=%+v", acknowledged, beforeACK, afterACK)
	}
	delta := func(key string) uint64 {
		t.Helper()
		first, last := writeConcernBenchmarkStat(t, before, key), writeConcernBenchmarkStat(t, after, key)
		if last < first {
			t.Fatalf("counter %s decreased %d -> %d", key, first, last)
		}
		return last - first
	}
	if operations, files := delta("treedb.command_wal.sync.count_total"), delta("treedb.command_wal.file_sync.calls_total"); operations < acknowledged || files < acknowledged {
		t.Fatalf("ACKs=%d command-WAL sync operations=%d physical file-sync calls=%d; want complete physical boundaries", acknowledged, operations, files)
	}
	check := func(c *mongo.Client) {
		t.Helper()
		coll := c.Database("app").Collection("journal_fence")
		cursor, err := coll.Find(ctx, bson.D{})
		if err != nil {
			t.Fatalf("readback find: %v", err)
		}
		var documents []bson.Raw
		if err := cursor.All(ctx, &documents); err != nil || len(documents) != count {
			t.Fatalf("readback documents=%d want %d err=%v", len(documents), count, err)
		}
		seen := make(map[string]bool, count)
		for _, document := range documents {
			id, ok := document.Lookup("_id").StringValueOK()
			if !ok || seen[id] {
				t.Fatalf("invalid/duplicate _id=%q string=%t", id, ok)
			}
			seen[id] = true
		}
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("journal-%02d", i)
			var document bson.Raw
			if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&document); err != nil {
				t.Fatalf("readback %s: %v", id, err)
			}
			requireStandaloneNativeJournalDocument(t, document, id, i)
		}
	}
	check(client)
	active = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
	t.Log("original driver disconnect, Serve shutdown, standalone manager/DB close: PASS")
	standalone = open()
	client, cancel, listener, served = startStandaloneMongoClientForTest(t, standalone)
	active = true
	check(client)
	active = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
	t.Log("fresh native owner typed reopen census and checked close: PASS (graceful reopen, not crash proof)")
	return standaloneNativeJournalCharacterization{acknowledged: acknowledged, checkpoints: delta("treedb.cache.checkpoint.runs"), forcedGroups: delta("treedb.command_wal.group_commit.forced_total")}
}

func TestStandaloneServerOfficialGoDriverWriteConcernW1AndJournaled(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{
		Dir:     t.TempDir(),
		Profile: treedb.ProfileCommandWALRelaxed,
		DefaultCollectionOptions: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)

	opCtx, opCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer opCancel()
	db := client.Database("app")
	w1 := db.Collection("users", options.Collection().SetWriteConcern(writeconcern.W1()))
	journaled := db.Collection("users", options.Collection().SetWriteConcern(writeconcern.Journaled()))
	if _, err := w1.InsertOne(opCtx, bson.D{{Key: "_id", Value: "w1"}}); err != nil {
		t.Fatalf("w:1 insert: %v", err)
	}
	if _, err := journaled.InsertOne(opCtx, bson.D{{Key: "_id", Value: "journaled"}, {Key: "payload", Value: strings.Repeat("p", 4096)}}); err != nil {
		t.Fatalf("journaled insert: %v", err)
	}
	stats := standalone.Server.StandaloneWriteConcernStats()
	if stats.VisibleAcknowledgements != 1 || stats.JournalAcknowledgements != 1 || stats.PhysicalSyncBoundaries != 1 {
		t.Fatalf("driver writeConcern stats=%+v", stats)
	}
	var got bson.M
	if err := db.Collection("users").FindOne(opCtx, bson.D{{Key: "_id", Value: "journaled"}}).Decode(&got); err != nil {
		t.Fatalf("find journaled document: %v", err)
	}
}

func TestServerOfficialGoDriverIndexMetadata(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	mongoDB := client.Database("app")
	coll := mongoDB.Collection("users")
	if err := mongoDB.RunCommand(opCtx, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
			{Key: "treedbValueType", Value: "string"},
			{Key: "unique", Value: true},
		}}},
	}).Err(); err != nil {
		t.Fatalf("driver create index: %v", err)
	}

	names, err := mongoDB.ListCollectionNames(opCtx, bson.D{})
	if err != nil {
		t.Fatalf("driver list collection names: %v", err)
	}
	if len(names) != 1 || names[0] != "users" {
		t.Fatalf("collection names=%q want [users]", names)
	}

	specs, err := coll.Indexes().ListSpecifications(opCtx)
	if err != nil {
		t.Fatalf("driver list index specs: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("index spec len=%d want 2: %+v", len(specs), specs)
	}
	if specs[0].Name != "_id_" || specs[1].Name != "email_1" {
		t.Fatalf("index spec names=%q,%q want _id_,email_1", specs[0].Name, specs[1].Name)
	}
	if specs[1].Unique == nil || !*specs[1].Unique {
		t.Fatalf("email_1 unique=%v want true", specs[1].Unique)
	}

	if err := coll.Indexes().DropOne(opCtx, "email_1"); err != nil {
		t.Fatalf("driver drop index: %v", err)
	}
	specs, err = coll.Indexes().ListSpecifications(opCtx)
	if err != nil {
		t.Fatalf("driver list after drop: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "_id_" {
		t.Fatalf("index specs after drop=%+v want only _id_", specs)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve loop did not stop")
	}
}

func TestServerOfficialGoDriverFindPlanner(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	mongoDB := client.Database("app")
	coll := mongoDB.Collection("users")
	if err := mongoDB.RunCommand(opCtx, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"},
			{Key: "treedbValueType", Value: "string"},
		}}},
	}).Err(); err != nil {
		t.Fatalf("driver create city index: %v", err)
	}
	docs := []any{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(37)}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(42)}},
		bson.D{{Key: "_id", Value: "u3"}, {Key: "name", Value: "katherine"}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int64(36)}},
	}
	if _, err := coll.InsertMany(opCtx, docs); err != nil {
		t.Fatalf("driver insert many: %v", err)
	}

	cursor, err := coll.Find(opCtx,
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "city", Value: "hnl"}},
			bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(40)}}}},
		}}},
		options.Find().
			SetProjection(bson.D{{Key: "name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}))
	if err != nil {
		t.Fatalf("driver indexed find: %v", err)
	}
	var results []bson.M
	if err := cursor.All(opCtx, &results); err != nil {
		t.Fatalf("driver indexed find all: %v", err)
	}
	if len(results) != 1 || results[0]["name"] != "grace" {
		t.Fatalf("indexed find results=%v want grace", results)
	}
	if _, ok := results[0]["_id"]; ok {
		t.Fatalf("indexed find projection included _id: %v", results[0])
	}

	cursor, err = coll.Find(opCtx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"u3", "u1"}}}}},
		options.Find().SetSort(bson.D{{Key: "name", Value: int32(1)}}).SetSkip(1).SetLimit(1))
	if err != nil {
		t.Fatalf("driver _id in find: %v", err)
	}
	results = nil
	if err := cursor.All(opCtx, &results); err != nil {
		t.Fatalf("driver _id in find all: %v", err)
	}
	if len(results) != 1 || results[0]["name"] != "katherine" {
		t.Fatalf("_id in find results=%v want katherine", results)
	}

	cursor, err = coll.Find(opCtx,
		bson.D{},
		options.Find().SetSort(bson.D{{Key: "name", Value: int32(1)}}).SetBatchSize(1))
	if err != nil {
		t.Fatalf("driver batched find: %v", err)
	}
	results = nil
	if err := cursor.All(opCtx, &results); err != nil {
		t.Fatalf("driver batched find all: %v", err)
	}
	if len(results) != 3 || results[0]["name"] != "ada" || results[2]["name"] != "katherine" {
		t.Fatalf("batched find results=%v want ada..katherine", results)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve loop did not stop")
	}
}
