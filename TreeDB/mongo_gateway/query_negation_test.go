package mongogateway

import (
	"bytes"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func TestMongoNegativeAndExistsPredicatesRespectMissingNullAndDottedValues(t *testing.T) {
	tests := []struct {
		name   string
		filter bson.D
		doc    bson.D
		want   bool
	}{
		{"ne matches missing", bson.D{{Key: "a", Value: bson.D{{Key: "$ne", Value: int32(1)}}}}, bson.D{{Key: "_id", Value: 1}}, true},
		{"ne null rejects missing", bson.D{{Key: "a", Value: bson.D{{Key: "$ne", Value: nil}}}}, bson.D{{Key: "_id", Value: 1}}, false},
		{"ne null rejects explicit null", bson.D{{Key: "a", Value: bson.D{{Key: "$ne", Value: nil}}}}, bson.D{{Key: "a", Value: nil}}, false},
		{"ne null matches scalar", bson.D{{Key: "a", Value: bson.D{{Key: "$ne", Value: nil}}}}, bson.D{{Key: "a", Value: int32(1)}}, true},
		{"ne rejects numeric equivalent", bson.D{{Key: "a", Value: bson.D{{Key: "$ne", Value: int32(1)}}}}, bson.D{{Key: "a", Value: int64(1)}}, false},
		{"nin matches null", bson.D{{Key: "a", Value: bson.D{{Key: "$nin", Value: bson.A{int32(1)}}}}}, bson.D{{Key: "a", Value: nil}}, true},
		{"exists distinguishes null from missing", bson.D{{Key: "a", Value: bson.D{{Key: "$exists", Value: true}}}}, bson.D{{Key: "a", Value: nil}}, true},
		{"exists false matches missing", bson.D{{Key: "a", Value: bson.D{{Key: "$exists", Value: false}}}}, bson.D{{Key: "_id", Value: 1}}, true},
		{"not matches missing", bson.D{{Key: "a", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: int32(3)}}}}}}, bson.D{{Key: "_id", Value: 1}}, true},
		{"dotted ne observes nested value", bson.D{{Key: "profile.age", Value: bson.D{{Key: "$ne", Value: int32(4)}}}}, bson.D{{Key: "profile", Value: bson.D{{Key: "age", Value: int32(4)}}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := bson.Marshal(tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			doc, err := bson.Marshal(tt.doc)
			if err != nil {
				t.Fatal(err)
			}
			predicates, err := parseFindPredicates(wire.Document(filter))
			if err != nil {
				t.Fatal(err)
			}
			got, err := documentMatchesPredicates(wire.Document(doc), predicates)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("match=%v want %v", got, tt.want)
			}
		})
	}
}

func TestMongoDottedProjectionRebuildsParentsAndFailsClosedOnArrays(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "x"}, {Key: "profile", Value: bson.D{{Key: "_id", Value: "nested"}, {Key: "name", Value: "Ada"}, {Key: "age", Value: int32(37)}}}, {Key: "state", Value: "active"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		projection bson.D
		want       bson.D
	}{
		{"include nested", bson.D{{Key: "profile.name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}, bson.D{{Key: "profile", Value: bson.D{{Key: "name", Value: "Ada"}}}}},
		{"exclude nested", bson.D{{Key: "profile.age", Value: int32(0)}}, bson.D{{Key: "_id", Value: "x"}, {Key: "profile", Value: bson.D{{Key: "_id", Value: "nested"}, {Key: "name", Value: "Ada"}}}, {Key: "state", Value: "active"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			projection, err := bson.Marshal(test.projection)
			if err != nil {
				t.Fatal(err)
			}
			want, err := bson.Marshal(test.want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := projectDocument(wire.Document(doc), wire.Document(projection))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("projected=%s want %s", got, want)
			}
		})
	}
	arrayDoc, err := bson.Marshal(bson.D{{Key: "profile", Value: bson.A{bson.D{{Key: "name", Value: "Ada"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := bson.Marshal(bson.D{{Key: "profile.name", Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projectDocument(wire.Document(arrayDoc), wire.Document(projection)); err == nil {
		t.Fatal("array traversal unexpectedly accepted")
	}
	deep := strings.Repeat("a.", mongoMutationMaxPathDepth) + "a"
	deepProjection, err := bson.Marshal(bson.D{{Key: deep, Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compileProjection(wire.Document(deepProjection)); err == nil {
		t.Fatal("over-depth projection unexpectedly accepted")
	}
}

func TestMongoDottedProjectionArrayFailureDoesNotPublishCursor(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406604, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "x"}, {Key: "profile", Value: bson.A{bson.D{{Key: "name", Value: "Ada"}}}}},
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 406605, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{}}, {Key: "projection", Value: bson.D{{Key: "profile.name", Value: int32(1)}}}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	if len(server.cursors) != 0 {
		t.Fatalf("published cursor despite dotted projection rejection: %d", len(server.cursors))
	}
}

func TestMongoNegativeArrayPathsRejectBeforeResponseOrMutation(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406606, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "first"}, {Key: "tags", Value: "safe"}},
		bson.D{{Key: "_id", Value: "x"}, {Key: "tags", Value: bson.A{"a"}}, {Key: "profile", Value: bson.A{bson.D{{Key: "name", Value: "Ada"}}}}},
	}}, {Key: "$db", Value: "app"}}))
	for _, filter := range []bson.D{
		bson.D{{Key: "tags", Value: bson.D{{Key: "$ne", Value: "a"}}}},
		bson.D{{Key: "tags", Value: bson.D{{Key: "$nin", Value: bson.A{"a"}}}}},
		bson.D{{Key: "tags", Value: bson.D{{Key: "$exists", Value: true}}}},
		bson.D{{Key: "tags", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: "a"}}}}}},
	} {
		response := serveCommand(t, server, 406607, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: filter}, {Key: "$db", Value: "app"}})
		assertCommandError(t, response, "BadValue")
	}
	for _, filter := range []bson.D{
		{{Key: "$or", Value: bson.A{bson.D{{Key: "tags", Value: bson.D{{Key: "$ne", Value: "a"}}}}}}},
		{{Key: "$nor", Value: bson.A{bson.D{{Key: "tags", Value: bson.D{{Key: "$nin", Value: bson.A{"a"}}}}}}}},
	} {
		assertCommandError(t, serveCommand(t, server, 406607, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: filter}, {Key: "$db", Value: "app"}}), "BadValue")
	}
	sortResponse := serveCommand(t, server, 406608, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{}}, {Key: "sort", Value: bson.D{{Key: "profile.name", Value: int32(1)}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, sortResponse, "BadValue")
	writeFilter := bson.D{{Key: "tags", Value: bson.D{{Key: "$ne", Value: "a"}}}}
	updateSpec := bson.D{{Key: "q", Value: writeFilter}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "mutated", Value: true}}}}}, {Key: "multi", Value: false}}
	write := serveCommand(t, server, 406609, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{updateSpec}}, {Key: "$db", Value: "app"}})
	assertIndexedWriteError(t, write, 0)
	multiSpec := bson.D{{Key: "q", Value: writeFilter}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "allMutated", Value: true}}}}}, {Key: "multi", Value: true}}
	assertIndexedWriteError(t, serveCommand(t, server, 406611, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{multiSpec}}, {Key: "$db", Value: "app"}}), 0)
	check := serveCommand(t, server, 406610, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "first"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, check)
	if bson.Raw(cursorFirstBatch(t, check)[0]).Lookup("mutated").Type != 0 || bson.Raw(cursorFirstBatch(t, check)[0]).Lookup("allMutated").Type != 0 {
		t.Fatal("array-rejected filter write mutated document")
	}
}

func TestMongoNegativePredicateRejectsMalformedOperands(t *testing.T) {
	for _, filter := range []bson.D{
		{{Key: "a", Value: bson.D{{Key: "$exists", Value: int32(1)}}}},
		{{Key: "a", Value: bson.D{{Key: "$not", Value: int32(1)}}}},
	} {
		raw, err := bson.Marshal(filter)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseFindPredicates(wire.Document(raw)); err == nil {
			t.Fatalf("filter %v unexpectedly accepted", filter)
		}
	}
}

func TestMongoNegativePredicateRejectsNestedAndDuplicateOperators(t *testing.T) {
	nestedNot, err := bson.Marshal(bson.D{{Key: "a", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: int32(1)}}}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(nestedNot)); err == nil {
		t.Fatal("nested $not unexpectedly accepted")
	}

	operatorStart, operatorDoc := bsoncore.AppendDocumentStart(nil)
	operatorDoc = bsoncore.AppendInt32Element(operatorDoc, "$exists", 1)
	operatorDoc = bsoncore.AppendInt32Element(operatorDoc, "$exists", 0)
	operatorDoc, _ = bsoncore.AppendDocumentEnd(operatorDoc, operatorStart)
	filterStart, filter := bsoncore.AppendDocumentStart(nil)
	filter = bsoncore.AppendDocumentElement(filter, "a", operatorDoc)
	filter, _ = bsoncore.AppendDocumentEnd(filter, filterStart)
	if _, err := parseFindPredicates(wire.Document(filter)); err == nil {
		t.Fatal("duplicate $exists unexpectedly accepted")
	}

	notStart, notDoc := bsoncore.AppendDocumentStart(nil)
	notDoc = bsoncore.AppendInt32Element(notDoc, "$gt", 1)
	notDoc = bsoncore.AppendInt32Element(notDoc, "$gt", 2)
	notDoc, _ = bsoncore.AppendDocumentEnd(notDoc, notStart)
	operatorStart, operatorDoc = bsoncore.AppendDocumentStart(nil)
	operatorDoc = bsoncore.AppendDocumentElement(operatorDoc, "$not", notDoc)
	operatorDoc, _ = bsoncore.AppendDocumentEnd(operatorDoc, operatorStart)
	filterStart, filter = bsoncore.AppendDocumentStart(nil)
	filter = bsoncore.AppendDocumentElement(filter, "a", operatorDoc)
	filter, _ = bsoncore.AppendDocumentEnd(filter, filterStart)
	if _, err := parseFindPredicates(wire.Document(filter)); err == nil {
		t.Fatal("duplicate $not inner operator unexpectedly accepted")
	}
}

func TestMongoQuerySortAndProjectionPathsRejectMalformedOrOverDepth(t *testing.T) {
	deep := strings.Repeat("a.", mongoMutationMaxPathDepth) + "a"
	for _, filter := range []bson.D{
		{{Key: "a..b", Value: int32(1)}},
		{{Key: deep, Value: int32(1)}},
	} {
		raw, err := bson.Marshal(filter)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseFindPredicates(wire.Document(raw)); err == nil {
			t.Fatalf("filter %v unexpectedly accepted", filter)
		}
	}
	for _, sortDoc := range []bson.D{
		{{Key: "a..b", Value: int32(1)}},
		{{Key: deep, Value: int32(1)}},
	} {
		raw, err := bson.Marshal(sortDoc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseFindSortDocument(wire.Document(raw)); err == nil {
			t.Fatalf("sort %v unexpectedly accepted", sortDoc)
		}
	}
}

func TestMongoTopLevelNorCombinesWithSiblingPredicates(t *testing.T) {
	filter, err := bson.Marshal(bson.D{{Key: "tenant", Value: "a"}, {Key: "$nor", Value: bson.A{
		bson.D{{Key: "score", Value: int32(1)}},
		bson.D{{Key: "state", Value: "disabled"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := parseFindPlan(nil, wire.Document(filter))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		doc  bson.D
		want bool
	}{
		{bson.D{{Key: "tenant", Value: "a"}, {Key: "score", Value: int32(2)}}, true},
		{bson.D{{Key: "tenant", Value: "a"}, {Key: "state", Value: "disabled"}}, false},
		{bson.D{{Key: "tenant", Value: "b"}, {Key: "score", Value: int32(2)}}, false},
	} {
		raw, err := bson.Marshal(test.doc)
		if err != nil {
			t.Fatal(err)
		}
		got, err := documentMatchesPlan(wire.Document(raw), plan)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("doc %v match=%v want %v", test.doc, got, test.want)
		}
	}
}

func TestMongoTopLevelOrAndNorCoexist(t *testing.T) {
	filter, err := bson.Marshal(bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "tenant", Value: "a"}}, bson.D{{Key: "tenant", Value: "b"}},
	}}, {Key: "$nor", Value: bson.A{bson.D{{Key: "state", Value: "disabled"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := parseFindPlan(nil, wire.Document(filter))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		doc  bson.D
		want bool
	}{
		{bson.D{{Key: "tenant", Value: "a"}}, true},
		{bson.D{{Key: "tenant", Value: "a"}, {Key: "state", Value: "disabled"}}, false},
		{bson.D{{Key: "tenant", Value: "c"}}, false},
	} {
		raw, err := bson.Marshal(test.doc)
		if err != nil {
			t.Fatal(err)
		}
		got, err := documentMatchesPlan(wire.Document(raw), plan)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("doc %v match=%v want %v", test.doc, got, test.want)
		}
	}
}

func TestMongoDottedSortIsStableAndReportsBoundedMaterialization(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406601, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "z"}, {Key: "profile", Value: bson.D{{Key: "name", Value: "bob"}}}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "profile", Value: bson.D{{Key: "name", Value: "alice"}}}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "profile", Value: bson.D{{Key: "name", Value: "alice"}}}},
	}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{}}, {Key: "sort", Value: bson.D{{Key: "profile.name", Value: int32(1)}}}, {Key: "skip", Value: int32(1)}, {Key: "limit", Value: int32(2)}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 406602, command)
	assertOK(t, response)
	batch := cursorFirstBatch(t, response)
	if len(batch) != 2 {
		t.Fatalf("batch len=%d want 2", len(batch))
	}
	for i, want := range []string{"c", "z"} {
		if got, ok := batch[i].Lookup("_id").StringValueOK(); !ok || got != want {
			t.Fatalf("result[%d]._id=%q ok=%v want %q", i, got, ok, want)
		}
	}
	explain := serveCommand(t, server, 406603, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	stats := bson.Raw(explain).Lookup("executionStats").Document()
	if got, ok := stats.Lookup("candidateDocumentsMaterialized").Int64OK(); !ok || got != 3 {
		t.Fatalf("materialized=%d ok=%v want 3: %s", got, ok, explain)
	}
}
