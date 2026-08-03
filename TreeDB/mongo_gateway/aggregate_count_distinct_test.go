package mongogateway

import (
	"context"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoReadCommandsAggregateCountDistinct(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)

	count := serveCommand(t, server, 1, bson.D{
		{Key: "count", Value: "users"},
		{Key: "query", Value: bson.D{{Key: "active", Value: true}}},
		{Key: "skip", Value: int32(1)},
		{Key: "limit", Value: int32(1)},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, count)
	if got, ok := bson.Raw(count).Lookup("n").Int64OK(); !ok || got != 1 {
		t.Fatalf("count n=%v ok=%v want 1", got, ok)
	}

	distinct := serveCommand(t, server, 2, bson.D{
		{Key: "distinct", Value: "users"},
		{Key: "key", Value: "city"},
		{Key: "query", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, distinct)
	values, err := bson.Raw(distinct).Lookup("values").Array().Values()
	if err != nil {
		t.Fatalf("distinct values: %v", err)
	}
	if len(values) != 2 || values[0].StringValue() != "hnl" || values[1].StringValue() != "sfo" {
		t.Fatalf("distinct values=%v want [hnl sfo]", values)
	}

	aggregate := serveCommand(t, server, 3, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "age", Value: int32(-1)}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: int32(1)}}}},
			bson.D{{Key: "$skip", Value: int32(1)}},
			bson.D{{Key: "$limit", Value: int32(1)}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, aggregate), []string{"u3"})

	countAggregate := serveCommand(t, server, 4, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, countAggregate)
	if len(batch) != 1 {
		t.Fatalf("count aggregate batch len=%d want 1", len(batch))
	}
	if got, ok := batch[0].Lookup("n").Int64OK(); !ok || got != 2 {
		t.Fatalf("count aggregate n=%v ok=%v want 2", got, ok)
	}
}

func TestStandaloneServerOfficialGoDriverAggregateCountDistinct(t *testing.T) {
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
	coll := client.Database("app").Collection("users")
	if _, err := coll.InsertMany(ctx, []any{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int32(20)}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "active", Value: true}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int32(30)}},
		bson.D{{Key: "_id", Value: "u3"}, {Key: "active", Value: false}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int32(40)}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got, err := coll.CountDocuments(ctx, bson.D{{Key: "active", Value: true}}); err != nil || got != 2 {
		t.Fatalf("CountDocuments=%d err=%v want 2", got, err)
	}
	if got, err := coll.EstimatedDocumentCount(ctx); err != nil || got != 3 {
		t.Fatalf("EstimatedDocumentCount=%d err=%v want 3", got, err)
	}
	values, err := coll.Distinct(ctx, "city", bson.D{}).Raw()
	if err != nil {
		t.Fatalf("Distinct: %v", err)
	}
	distinctValues, err := values.Values()
	if err != nil || len(distinctValues) != 2 || distinctValues[0].StringValue() != "hnl" || distinctValues[1].StringValue() != "sfo" {
		t.Fatalf("Distinct values=%v err=%v want [hnl sfo]", distinctValues, err)
	}
	cursor, err := coll.Aggregate(ctx, bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
		bson.D{{Key: "$count", Value: "n"}},
	})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var result []bson.M
	if err := cursor.All(ctx, &result); err != nil || len(result) != 1 || result[0]["n"] != int64(2) {
		t.Fatalf("Aggregate result=%v err=%v want n=2", result, err)
	}
}
