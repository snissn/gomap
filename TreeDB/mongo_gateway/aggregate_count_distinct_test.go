package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
	limitThenSkip := serveCommand(t, server, 31, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$limit", Value: int32(2)}},
			bson.D{{Key: "$skip", Value: int32(1)}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, limitThenSkip), []string{"u2"})

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

func TestExecuteAggregateStagesMultiFieldSortUsesStableIDTie(t *testing.T) {
	doc := func(id string, a, b int32) wire.Document {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "a", Value: a}, {Key: "b", Value: b}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	stages := []aggregateStage{{name: "$sort", plan: findPlan{sort: findSort{field: "a", terms: []findSortTerm{{field: "a"}, {field: "b", desc: true}}}}}}
	got, err := executeAggregateStages([]wire.Document{doc("b", 1, 2), doc("a", 1, 2), doc("c", 1, 3)}, stages)
	if err != nil {
		t.Fatal(err)
	}
	batch := make([]bson.Raw, len(got))
	for i := range got {
		batch[i] = bson.Raw(got[i])
	}
	assertBatchIDs(t, batch, []string{"c", "a", "b"})
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
	// A deadline makes the v2 driver send maxTimeMS, which this bounded subset
	// rejects until server-side deadline enforcement is implemented.
	readCtx := context.Background()
	if got, err := coll.CountDocuments(readCtx, bson.D{{Key: "active", Value: true}}); err != nil || got != 2 {
		t.Fatalf("CountDocuments=%d err=%v want 2", got, err)
	}
	if got, err := coll.CountDocuments(readCtx, bson.D{}, options.Count().SetSkip(1).SetLimit(1)); err != nil || got != 1 {
		t.Fatalf("CountDocuments skip/limit=%d err=%v want 1", got, err)
	}
	if got, err := coll.EstimatedDocumentCount(readCtx); err != nil || got != 3 {
		t.Fatalf("EstimatedDocumentCount=%d err=%v want 3", got, err)
	}
	values, err := coll.Distinct(readCtx, "city", bson.D{}).Raw()
	if err != nil {
		t.Fatalf("Distinct: %v", err)
	}
	distinctValues, err := values.Values()
	if err != nil || len(distinctValues) != 2 || distinctValues[0].StringValue() != "hnl" || distinctValues[1].StringValue() != "sfo" {
		t.Fatalf("Distinct values=%v err=%v want [hnl sfo]", distinctValues, err)
	}
	cursor, err := coll.Aggregate(readCtx, bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
		bson.D{{Key: "$count", Value: "n"}},
	})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	defer func() { _ = cursor.Close(readCtx) }()
	var result []bson.M
	if err := cursor.All(readCtx, &result); err != nil || len(result) != 1 || result[0]["n"] != int64(2) {
		t.Fatalf("Aggregate result=%v err=%v want n=2", result, err)
	}
}

func TestMongoDistinctTopLevelArrayNumericEqualityAndOrder(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	decimalOne, err := bson.ParseDecimal128("1.00")
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, serveCommand(t, server, 10, bson.D{
		{Key: "insert", Value: "distinct_values"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: bson.A{int32(1), int64(1), "first", "first"}}},
			bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: 1.0}},
			bson.D{{Key: "_id", Value: "c"}, {Key: "v", Value: decimalOne}},
			bson.D{{Key: "_id", Value: "d"}, {Key: "v", Value: nil}},
			bson.D{{Key: "_id", Value: "e"}},
			bson.D{{Key: "_id", Value: "f"}, {Key: "v", Value: "last"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	response := serveCommand(t, server, 11, bson.D{
		{Key: "distinct", Value: "distinct_values"},
		{Key: "key", Value: "v"},
		{Key: "query", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	values, err := bson.Raw(response).Lookup("values").Array().Values()
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 4 || values[0].Type != bson.TypeInt32 || values[0].Int32() != 1 ||
		values[1].StringValue() != "first" || values[2].Type != bson.TypeNull || values[3].StringValue() != "last" {
		t.Fatalf("distinct values=%v want [int32(1) first null last]", values)
	}
	filtered := serveCommand(t, server, 12, bson.D{
		{Key: "distinct", Value: "distinct_values"},
		{Key: "key", Value: "v"},
		{Key: "query", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"d", "f"}}}}}},
		{Key: "$db", Value: "app"},
	})
	filteredValues, err := bson.Raw(filtered).Lookup("values").Array().Values()
	if err != nil || len(filteredValues) != 2 || filteredValues[0].Type != bson.TypeNull || filteredValues[1].StringValue() != "last" {
		t.Fatalf("filtered distinct=%v err=%v", filteredValues, err)
	}
}

func TestMongoAggregateCursorAndEmptyCounts(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	response := serveCommand(t, server, 20, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: int32(1)}}}}}},
		{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	if batch := cursorFirstBatch(t, response); len(batch) != 1 || batch[0].Lookup("_id").StringValue() != "u1" {
		t.Fatalf("first batch=%v", batch)
	}
	cursorID := cursorIDFromResponse(t, response)
	if cursorID == 0 {
		t.Fatal("aggregate cursor id=0 want retained cursor")
	}
	next := serveCommand(t, server, 21, bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "users"},
		{Key: "batchSize", Value: int32(2)},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorNextBatch(t, next), []string{"u2", "u3"})

	for requestID, stage := range []bson.D{
		{{Key: "$count", Value: "n"}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
	} {
		empty := serveCommand(t, server, int32(22+requestID), bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: "missing"}}}},
				stage,
			}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "$db", Value: "app"},
		})
		if batch := cursorFirstBatch(t, empty); len(batch) != 0 {
			t.Fatalf("empty count stage %v batch=%v", stage, batch)
		}
	}
}

func TestMongoAggregateCountDistinctRejectUnsupportedSurface(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	tests := []struct {
		name    string
		command bson.D
		want    string
	}{
		{name: "aggregate output", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{bson.D{{Key: "$out", Value: "copy"}}}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "$db", Value: "app"},
		}},
		{name: "aggregate merge", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{bson.D{{Key: "$merge", Value: "copy"}}}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "$db", Value: "app"},
		}},
		{name: "aggregate expression", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{bson.D{{Key: "$project", Value: bson.D{{Key: "copy", Value: "$age"}}}}}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "$db", Value: "app"},
		}},
		{name: "aggregate option", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "allowDiskUse", Value: true},
			{Key: "$db", Value: "app"},
		}},
		{name: "aggregate cursor option", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{}},
			{Key: "cursor", Value: bson.D{{Key: "unknown", Value: true}}},
			{Key: "$db", Value: "app"},
		}},
		{name: "aggregate missing collection negative batch size", command: bson.D{
			{Key: "aggregate", Value: "missing"},
			{Key: "pipeline", Value: bson.A{}},
			{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: int32(-1)}}},
			{Key: "$db", Value: "app"},
		}},
		{name: "aggregate group", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$group", Value: bson.D{
					{Key: "_id", Value: "$city"},
					{Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
				}}},
			}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "$db", Value: "app"},
		}},
		{name: "aggregate zero limit", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{bson.D{{Key: "$limit", Value: int32(0)}}}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "$db", Value: "app"},
		}},
		{name: "count option", command: bson.D{{Key: "count", Value: "users"}, {Key: "hint", Value: "_id_"}, {Key: "$db", Value: "app"}}},
		{name: "count max time", command: bson.D{{Key: "count", Value: "users"}, {Key: "maxTimeMS", Value: int64(1)}, {Key: "$db", Value: "app"}}},
		{name: "distinct empty", command: bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: ""}, {Key: "$db", Value: "app"}}, want: "FailedToParse"},
		{name: "distinct dotted", command: bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "profile.city"}, {Key: "$db", Value: "app"}}},
		{name: "distinct option", command: bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "collation", Value: bson.D{}}, {Key: "$db", Value: "app"}}},
		{name: "distinct max time", command: bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "maxTimeMS", Value: int64(1)}, {Key: "$db", Value: "app"}}},
		{name: "aggregate max time", command: bson.D{
			{Key: "aggregate", Value: "users"},
			{Key: "pipeline", Value: bson.A{}},
			{Key: "cursor", Value: bson.D{}},
			{Key: "maxTimeMS", Value: int64(1)},
			{Key: "$db", Value: "app"},
		}},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveCommand(t, server, int32(100+i), test.command)
			want := test.want
			if want == "" {
				want = "BadValue"
			}
			assertCommandError(t, response, want)
		})
	}
	for i, name := range []string{"aggregate", "count", "distinct"} {
		command := bson.D{{Key: name, Value: "users"}}
		switch name {
		case "aggregate":
			command = append(command, bson.E{Key: "pipeline", Value: bson.A{}}, bson.E{Key: "cursor", Value: bson.D{}})
		case "distinct":
			command = append(command, bson.E{Key: "key", Value: "city"})
		}
		command = append(command, bson.E{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}}, bson.E{Key: "$db", Value: "app"})
		response := serveCommand(t, server, int32(120+i), command)
		assertCommandError(t, response, "BadValue")
	}
}

func TestMongoAggregateCountDistinctEnforceScanBounds(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	server.MaxFindScanDocuments = 2
	for i, command := range []bson.D{
		{{Key: "count", Value: "users"}, {Key: "query", Value: bson.D{{Key: "active", Value: false}}}, {Key: "$db", Value: "app"}},
		{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "$db", Value: "app"}},
		{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{}}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}},
	} {
		response := serveCommand(t, server, int32(200+i), command)
		assertCommandError(t, response, "BadValue")
	}
	selective := serveCommand(t, server, 209, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: "u1"}}}}}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, selective), []string{"u1"})
	limited := serveCommand(t, server, 210, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$limit", Value: int32(1)}}}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, limited), []string{"u1"})
	matchedLimited := serveCommand(t, server, 211, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
			bson.D{{Key: "$limit", Value: int32(1)}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, matchedLimited), []string{"u1"})
	indexed := newMongoCompatibilityMatrixServer(t)
	indexed.MaxFindScanDocuments = 1
	indexedMatchedLimited := serveCommand(t, indexed, 2111, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "city", Value: "hnl"}}}},
			bson.D{{Key: "$limit", Value: int32(1)}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, indexedMatchedLimited), []string{"u1"})
	skippedLimited := serveCommand(t, server, 212, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$skip", Value: int32(1)}},
			bson.D{{Key: "$limit", Value: int32(1)}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, skippedLimited), []string{"u2"})
	assertOK(t, serveCommand(t, server, 213, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u4"}, {Key: "active", Value: false}}}},
		{Key: "$db", Value: "app"},
	}))
	server.MaxFindScanDocuments = 3
	matchedSkippedLimited := serveCommand(t, server, 214, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
			bson.D{{Key: "$skip", Value: int32(1)}},
			bson.D{{Key: "$limit", Value: int32(1)}},
		}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertBatchIDs(t, cursorFirstBatch(t, matchedSkippedLimited), []string{"u3"})
	skipOnly := serveCommand(t, server, 215, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$skip", Value: int32(1)}}}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, skipOnly, "BadValue")

	oneDoc := newMongoCompatibilityMatrixServer(t)
	oneDoc.MaxFindScanDocuments = 2
	assertOK(t, serveCommand(t, oneDoc, 210, bson.D{{Key: "insert", Value: "wide"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "wide"}, {Key: "v", Value: bson.A{1, 2, 3}}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, oneDoc, 211, bson.D{{Key: "distinct", Value: "wide"}, {Key: "key", Value: "v"}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")

	stages := make(bson.A, mongoAggregateMaxStages+1)
	for i := range stages {
		stages[i] = bson.D{{Key: "$skip", Value: int32(0)}}
	}
	response = serveCommand(t, oneDoc, 212, bson.D{{Key: "aggregate", Value: "wide"}, {Key: "pipeline", Value: stages}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
}

func TestForEachRawArrayValueStopsWithoutMaterializingRemainder(t *testing.T) {
	items := make(bson.A, 4096)
	for i := range items {
		items[i] = int32(i)
	}
	doc := mustDocument(t, bson.D{{Key: "values", Value: items}})
	visits := 0
	err := forEachRawArrayValue(bson.Raw(doc).Lookup("values"), func(bson.RawValue) error {
		visits++
		if visits == 3 {
			return errors.New("stop")
		}
		return nil
	})
	if err == nil || err.Error() != "stop" || visits != 3 {
		t.Fatalf("streamed array err=%v visits=%d want stop/3", err, visits)
	}
}

func TestServerRejectsAggregateCountDistinctOPMsgFeatures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command bson.D
	}{
		{name: "aggregate", command: bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{}}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}}},
		{name: "count", command: bson.D{{Key: "count", Value: "users"}, {Key: "$db", Value: "app"}}},
		{name: "distinct", command: bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "$db", Value: "app"}}},
	} {
		t.Run(tc.name+"_moreToCome", func(t *testing.T) {
			request, err := wire.AppendMsgMessage(nil, 501, 0, wire.MsgFlagMoreToCome, mustDocument(t, tc.command))
			if err != nil {
				t.Fatal(err)
			}
			rw := &readWriter{r: bytes.NewReader(request)}
			err = NewServer().ServeOne(rw)
			if !errors.Is(err, wire.ErrUnsupported) || rw.w.Len() != 0 {
				t.Fatalf("ServeOne err=%v responseBytes=%d want ErrUnsupported/0", err, rw.w.Len())
			}
		})
		t.Run(tc.name+"_documentSequences", func(t *testing.T) {
			request, err := wire.AppendMsgMessageWithSequences(nil, 502, 0, 0, mustDocument(t, tc.command), []wire.DocumentSequence{{
				Identifier: "ignored",
				Documents:  []wire.Document{mustDocument(t, bson.D{{Key: "x", Value: int32(1)}})},
			}})
			if err != nil {
				t.Fatal(err)
			}
			rw := &readWriter{r: bytes.NewReader(request)}
			err = NewServer().ServeOne(rw)
			if !errors.Is(err, wire.ErrUnsupported) || rw.w.Len() != 0 {
				t.Fatalf("ServeOne err=%v responseBytes=%d want ErrUnsupported/0", err, rw.w.Len())
			}
		})
	}
}

func TestMongoDistinctEqualityWorkBounds(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	comparisonDocs := make([]wire.Document, 363)
	for i := range comparisonDocs {
		comparisonDocs[i] = mustDocument(t, bson.D{{Key: "v", Value: bson.D{{Key: "n", Value: int32(i)}}}})
	}
	if _, err := server.distinctValues(comparisonDocs, "v"); err == nil || !strings.Contains(err.Error(), "65536 comparisons") {
		t.Fatalf("comparison bound err=%v", err)
	}

	decimalDocs := make([]wire.Document, 33)
	for i := range decimalDocs {
		value, err := bson.ParseDecimal128(fmt.Sprintf("%dE+6000", i+1))
		if err != nil {
			t.Fatal(err)
		}
		decimalDocs[i] = mustDocument(t, bson.D{{Key: "v", Value: value}})
	}
	if _, err := server.distinctValues(decimalDocs, "v"); err == nil || !strings.Contains(err.Error(), "1024 Decimal128 normalizations") {
		t.Fatalf("Decimal128 bound err=%v", err)
	}
}

func TestMongoAggregateCountDistinctClusterFailsClosed(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	submitter := &mongoClusterFakeSubmitter{}
	server.ClusterSubmitter = submitter
	for i, command := range []bson.D{
		{{Key: "count", Value: "users"}, {Key: "$db", Value: "app"}},
		{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "$db", Value: "app"}},
		{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{bson.D{{Key: "$out", Value: "copy"}}}}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}},
	} {
		response := serveCommand(t, server, int32(300+i), command)
		assertCommandError(t, response, "BadValue")
	}
	submitter.mu.Lock()
	calls := len(submitter.calls)
	submitter.mu.Unlock()
	if calls != 0 {
		t.Fatalf("cluster submit calls=%d want 0", calls)
	}
	server.ClusterSubmitter = nil
	find := serveCommand(t, server, 310, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}})
	if batch := cursorFirstBatch(t, find); len(batch) != 3 {
		t.Fatalf("documents changed after rejected cluster reads: %v", batch)
	}
	missing := serveCommand(t, server, 311, bson.D{{Key: "find", Value: "copy"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}})
	if batch := cursorFirstBatch(t, missing); len(batch) != 0 {
		t.Fatalf("rejected $out created documents: %v", batch)
	}
}

func TestMongoAggregateCountDistinctCommandWALValueLogPointersReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.ValueLog.PointerThreshold = 1
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOpen := true
	closeInitial := func() error {
		if !backendOpen {
			return nil
		}
		backendOpen = false
		return closeBackend()
	}
	t.Cleanup(func() { _ = closeInitial() })
	manager := collections.NewCollectionManager(backend)
	server := NewServer()
	server.Collections = manager
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 400, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "active", Value: true}, {Key: "city", Value: "hnl"}, {Key: "payload", Value: string(make([]byte, 256))}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "active", Value: false}, {Key: "city", Value: "sfo"}, {Key: "payload", Value: string(make([]byte, 256))}},
		}},
		{Key: "$db", Value: "app"},
	}))
	if err := manager.FlushAll(); err != nil {
		_ = closeInitial()
		t.Fatalf("flush: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		_ = closeInitial()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := closeInitial(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, closeReopened, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = closeReopened() }()
	reopenedServer := NewServer()
	reopenedServer.Collections = collections.NewCollectionManager(reopened)
	reopenedServer.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	count := serveCommand(t, reopenedServer, 401, bson.D{{Key: "count", Value: "users"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "$db", Value: "app"}})
	if n, ok := bson.Raw(count).Lookup("n").Int64OK(); !ok || n != 1 {
		t.Fatalf("reopened count n=%d ok=%v", n, ok)
	}
	distinct := serveCommand(t, reopenedServer, 402, bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "$db", Value: "app"}})
	values, err := bson.Raw(distinct).Lookup("values").Array().Values()
	if err != nil || len(values) != 2 || values[0].StringValue() != "hnl" || values[1].StringValue() != "sfo" {
		t.Fatalf("reopened distinct=%v err=%v", values, err)
	}
	aggregate := serveCommand(t, reopenedServer, 403, bson.D{
		{Key: "aggregate", Value: "users"},
		{Key: "pipeline", Value: bson.A{bson.D{{Key: "$count", Value: "n"}}}},
		{Key: "cursor", Value: bson.D{}},
		{Key: "$db", Value: "app"},
	})
	batch := cursorFirstBatch(t, aggregate)
	if len(batch) != 1 {
		t.Fatalf("reopened aggregate batch=%v", batch)
	}
	if n, ok := batch[0].Lookup("n").Int64OK(); !ok || n != 2 {
		t.Fatalf("reopened aggregate n=%d ok=%v", n, ok)
	}
}
