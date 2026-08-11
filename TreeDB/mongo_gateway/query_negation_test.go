package mongogateway

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
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
		{"nin empty matches missing", bson.D{{Key: "a", Value: bson.D{{Key: "$nin", Value: bson.A{}}}}}, bson.D{{Key: "_id", Value: 1}}, true},
		{"nin empty matches scalar", bson.D{{Key: "a", Value: bson.D{{Key: "$nin", Value: bson.A{}}}}}, bson.D{{Key: "a", Value: int32(1)}}, true},
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
		{"include nested id", bson.D{{Key: "profile._id", Value: int32(1)}, {Key: "_id", Value: int32(0)}}, bson.D{{Key: "profile", Value: bson.D{{Key: "_id", Value: "nested"}}}}},
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
	server.cursorMu.Lock()
	cursors := len(server.cursors)
	server.cursorMu.Unlock()
	if cursors != 0 {
		t.Fatalf("published cursor despite dotted projection rejection: %d", cursors)
	}
}

func TestMongoFilterFindAndModifyProjectionRejectsBeforeMutation(t *testing.T) {
	for _, newImage := range []bool{false, true} {
		t.Run(fmt.Sprintf("new=%v", newImage), func(t *testing.T) {
			server := newMongoCompatibilityMatrixServer(t)
			assertOK(t, serveCommand(t, server, 406640, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
				bson.D{{Key: "_id", Value: "array-path"}, {Key: "active", Value: true}, {Key: "n", Value: int32(0)}, {Key: "profile", Value: bson.A{bson.D{{Key: "name", Value: "Ada"}}}}},
			}}, {Key: "$db", Value: "app"}}))

			response := serveCommand(t, server, 406641, bson.D{
				{Key: "findAndModify", Value: "events"},
				{Key: "query", Value: bson.D{{Key: "active", Value: true}}},
				{Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}},
				{Key: "new", Value: newImage},
				{Key: "fields", Value: bson.D{{Key: "profile.name", Value: int32(1)}}},
				{Key: "$db", Value: "app"},
			})
			assertCommandError(t, response, "BadValue")
			if !bson.Raw(response).Lookup("value").IsZero() || !bson.Raw(response).Lookup("lastErrorObject").IsZero() {
				t.Fatalf("findAndModify emitted partial success response: %s", response)
			}
			server.cursorMu.Lock()
			cursors := len(server.cursors)
			server.cursorMu.Unlock()
			if cursors != 0 {
				t.Fatalf("findAndModify published cursor despite response rejection: %d", cursors)
			}
			check := serveCommand(t, server, 406642, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "array-path"}}}, {Key: "$db", Value: "app"}})
			assertOK(t, check)
			doc := bson.Raw(cursorFirstBatch(t, check)[0])
			if got, ok := doc.Lookup("n").Int32OK(); !ok || got != 0 {
				t.Fatalf("array-rejected filter findAndModify mutated n=%d ok=%v want 0", got, ok)
			}
		})
	}
}

func TestMongoNorResidualBypassesDirectPrimaryProjectionShortcut(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406643, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "rejected"}, {Key: "state", Value: "disabled"}},
	}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 406644, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{
		{Key: "_id", Value: "rejected"},
		{Key: "$nor", Value: bson.A{bson.D{{Key: "state", Value: "disabled"}}}},
	}}, {Key: "projection", Value: bson.D{{Key: "_id", Value: int32(1)}}}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	if batch := cursorFirstBatch(t, response); len(batch) != 0 {
		t.Fatalf("$nor-rejected primary document escaped projected shortcut: %v", batch)
	}
}

func TestMongoNorResidualGuardsFindWritesAndExplain(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406645, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "rejected"}, {Key: "state", Value: "disabled"}, {Key: "n", Value: int32(0)}},
	}}, {Key: "$db", Value: "app"}}))
	filter := bson.D{{Key: "_id", Value: "rejected"}, {Key: "$nor", Value: bson.A{bson.D{{Key: "state", Value: "disabled"}}}}}
	update := serveCommand(t, server, 406646, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: filter}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(1)}}}}},
	}}}, {Key: "$db", Value: "app"}})
	assertOK(t, update)
	assertInt32(t, update, "n", 0)
	deleted := serveCommand(t, server, 406647, bson.D{{Key: "delete", Value: "events"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: filter}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}})
	assertOK(t, deleted)
	assertInt32(t, deleted, "n", 0)
	check := serveCommand(t, server, 406648, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "rejected"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, check)
	if batch := cursorFirstBatch(t, check); len(batch) != 1 || batch[0].Lookup("n").Int32() != 0 {
		t.Fatalf("$nor residual write changed document: %v", batch)
	}
	explain := serveCommand(t, server, 406649, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: filter}, {Key: "$db", Value: "app"}}}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, explain)
	winning := bson.Raw(explain).Lookup("queryPlanner", "winningPlan").Document()
	if residual, ok := winning.Lookup("residualFilter").BooleanOK(); !ok || !residual {
		t.Fatalf("$nor explain residualFilter=%v ok=%v: %s", residual, ok, explain)
	}
	if returned, ok := bson.Raw(explain).Lookup("executionStats", "nReturned").Int64OK(); !ok || returned != 0 {
		t.Fatalf("$nor explain nReturned=%d ok=%v want 0: %s", returned, ok, explain)
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

func TestMongoNegativeQueryWorkCapsRejectBeforeCursorOrMutation(t *testing.T) {
	choices := make(bson.A, 0, mongoQueryMaxNegativeChoices+1)
	for i := 0; i < mongoQueryMaxNegativeChoices; i++ {
		choices = append(choices, int32(i))
	}
	nearCap, err := bson.Marshal(bson.D{{Key: "score", Value: bson.D{{Key: "$nin", Value: choices}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(nearCap)); err != nil {
		t.Fatalf("near-cap $nin rejected: %v", err)
	}
	choices = append(choices, int32(mongoQueryMaxNegativeChoices))
	overCap := bson.D{{Key: "score", Value: bson.D{{Key: "$nin", Value: choices}}}}
	raw, err := bson.Marshal(overCap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(raw)); err == nil {
		t.Fatal("over-cap $nin unexpectedly accepted")
	}
	branches := make(bson.A, mongoQueryMaxBooleanBranches+1)
	for i := range branches {
		branches[i] = bson.D{{Key: "score", Value: int32(i)}}
	}
	raw, err = bson.Marshal(bson.D{{Key: "$nor", Value: branches}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(raw)); err == nil {
		t.Fatal("over-cap $nor unexpectedly accepted")
	}

	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406628, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "x"}, {Key: "score", Value: int32(7)}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 406629, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: overCap}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	server.cursorMu.Lock()
	cursors := len(server.cursors)
	server.cursorMu.Unlock()
	if cursors != 0 {
		t.Fatalf("published cursor for over-cap filter: %d", cursors)
	}
	write := serveCommand(t, server, 406630, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: overCap}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "mutated", Value: true}}}}},
	}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, write, "BadValue")
	check := serveCommand(t, server, 406631, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "x"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, check)
	if bson.Raw(cursorFirstBatch(t, check)[0]).Lookup("mutated").Type != 0 {
		t.Fatal("over-cap filter write mutated document")
	}
}

func TestMongoNegativeQueryRawArrayCapsRejectBeforeFullMaterialization(t *testing.T) {
	choices := make(bson.A, mongoQueryMaxNegativeChoices*16)
	for i := range choices {
		choices[i] = int32(7)
	}
	filter := bson.D{{Key: "score", Value: bson.D{{Key: "$nin", Value: choices}}}}
	raw, err := bson.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(raw)); err == nil || !strings.Contains(err.Error(), "$nin exceeds") {
		t.Fatalf("duplicate-heavy $nin error=%v want bounded rejection", err)
	}
	raw, err = bson.Marshal(bson.D{{Key: "score", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$in", Value: choices}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(raw)); err == nil || !strings.Contains(err.Error(), "$in exceeds") {
		t.Fatalf("duplicate-heavy $not:$in error=%v want bounded rejection", err)
	}
	branches := make(bson.A, mongoQueryMaxBooleanBranches*16)
	for i := range branches {
		branches[i] = bson.D{{Key: "score", Value: int32(7)}}
	}
	for _, operator := range []string{"$or", "$nor"} {
		raw, err := bson.Marshal(bson.D{{Key: operator, Value: branches}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseFindPredicates(wire.Document(raw)); err == nil || !strings.Contains(err.Error(), operator+" exceeds") {
			t.Fatalf("duplicate-heavy %s error=%v want bounded rejection", operator, err)
		}
	}
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406650, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "x"}, {Key: "score", Value: int32(7)}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 406651, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: filter}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	server.cursorMu.Lock()
	cursors := len(server.cursors)
	server.cursorMu.Unlock()
	if cursors != 0 {
		t.Fatalf("published cursor for duplicate-heavy over-cap filter: %d", cursors)
	}
	write := serveCommand(t, server, 406652, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: filter}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "mutated", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, write, "BadValue")
	check := serveCommand(t, server, 406653, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "x"}}}, {Key: "$db", Value: "app"}})
	if bson.Raw(cursorFirstBatch(t, check)[0]).Lookup("mutated").Type != 0 {
		t.Fatal("duplicate-heavy over-cap filter mutated document")
	}
}

func TestMongoWideNorBranchRejectsBeforeDocumentMaterializationOrMutation(t *testing.T) {
	nearCapBranch := make(bson.D, 0, mongoQueryMaxBooleanPredicates)
	for i := 0; i < mongoQueryMaxBooleanPredicates; i++ {
		nearCapBranch = append(nearCapBranch, bson.E{Key: fmt.Sprintf("score_%03d", i), Value: int32(i)})
	}
	nearCap, err := bson.Marshal(bson.D{{Key: "$nor", Value: bson.A{nearCapBranch}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(nearCap)); err != nil {
		t.Fatalf("near-cap single $nor branch rejected: %v", err)
	}

	// Use a raw, several-megabyte branch rather than a compact high-level
	// shape. The parser must stop at predicate 257 without allocating a raw
	// element slice proportional to the remaining wire document.
	wideBranch := append(bson.D(nil), nearCapBranch...)
	filler := strings.Repeat("x", 1024)
	for i := mongoQueryMaxBooleanPredicates; i < mongoQueryMaxBooleanPredicates*16; i++ {
		wideBranch = append(wideBranch, bson.E{Key: fmt.Sprintf("score_%04d", i), Value: filler})
	}
	filter := bson.D{{Key: "$nor", Value: bson.A{wideBranch}}}
	raw, err := bson.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(raw)); err == nil || !strings.Contains(err.Error(), "exceeds "+fmt.Sprint(mongoQueryMaxBooleanPredicates)+" predicates") {
		t.Fatalf("wide $nor branch error=%v want bounded predicate rejection", err)
	}
	// The same document walker is reached through every supported nesting
	// route; no branch may regain an eager Raw.Elements allocation.
	nested, err := bson.Marshal(bson.D{{Key: "$or", Value: bson.A{bson.D{{Key: "$and", Value: bson.A{wideBranch}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseFindPredicates(wire.Document(nested)); err == nil || !strings.Contains(err.Error(), "exceeds "+fmt.Sprint(mongoQueryMaxBooleanPredicates)+" predicates") {
		t.Fatalf("nested $or/$and wide branch error=%v want bounded predicate rejection", err)
	}

	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406654, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "x"}, {Key: "score_000", Value: int32(0)}}}}, {Key: "$db", Value: "app"}}))
	response := serveCommand(t, server, 406655, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: filter}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	server.cursorMu.Lock()
	cursors := len(server.cursors)
	server.cursorMu.Unlock()
	if cursors != 0 {
		t.Fatalf("published cursor for wide $nor branch: %d", cursors)
	}
	write := serveCommand(t, server, 406656, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: filter}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "mutated", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, write, "BadValue")
	check := serveCommand(t, server, 406657, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "x"}}}, {Key: "$db", Value: "app"}})
	assertOK(t, check)
	if bson.Raw(cursorFirstBatch(t, check)[0]).Lookup("mutated").Type != 0 {
		t.Fatal("wide $nor branch mutated document")
	}
}

func TestMongoMalformedNegativeFilterRejectsBeforeRoutedObservation(t *testing.T) {
	server, submitter := newMongoPlacementRouteTestServer(t, raftplacement.PlacementModeRingV1)
	lookups := 0
	server.clusterCollectionLookupHook = func() { lookups++ }
	choices := make(bson.A, mongoQueryMaxNegativeChoices+1)
	for i := range choices {
		choices[i] = int32(i)
	}
	response := serveCommand(t, server, 406632, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "score", Value: bson.D{{Key: "$nin", Value: choices}}}}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "BadValue")
	if lookups != 0 {
		t.Fatalf("local collection lookups=%d want 0", lookups)
	}
	if routes := submitter.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("route calls=%d want 0", len(routes))
	}
}

func TestMongoNegativeExplainReportsResidualCandidatesAndMaterialization(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406633, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}}}, {Key: "name", Value: "tenant_1"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406634, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a1"}, {Key: "tenant", Value: "a"}, {Key: "score", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "a2"}, {Key: "tenant", Value: "a"}, {Key: "score", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "b1"}, {Key: "tenant", Value: "b"}, {Key: "score", Value: int32(2)}},
	}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "a"}, {Key: "score", Value: bson.D{{Key: "$ne", Value: int32(1)}}}}}, {Key: "$db", Value: "app"}}
	response := serveCommand(t, server, 406635, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(t, response)
	winning := bson.Raw(response).Lookup("queryPlanner", "winningPlan").Document()
	if residual, ok := winning.Lookup("residualFilter").BooleanOK(); !ok || !residual {
		t.Fatalf("residualFilter=%v ok=%v want true: %s", residual, ok, response)
	}
	stats := bson.Raw(response).Lookup("executionStats").Document()
	if got, ok := stats.Lookup("nReturned").Int64OK(); !ok || got != 1 {
		t.Fatalf("nReturned=%d ok=%v want 1: %s", got, ok, response)
	}
	if got, ok := stats.Lookup("candidateDocumentsExamined").Int64OK(); !ok || got != 2 {
		t.Fatalf("candidateDocumentsExamined=%d ok=%v want 2: %s", got, ok, response)
	}
	if got, ok := stats.Lookup("candidateDocumentsMaterialized").Int64OK(); !ok || got != 2 {
		t.Fatalf("candidateDocumentsMaterialized=%d ok=%v want 2: %s", got, ok, response)
	}
	if got, ok := stats.Lookup("candidateMaterializedBytes").Int64OK(); !ok || got <= 0 {
		t.Fatalf("candidateMaterializedBytes=%d ok=%v want >0: %s", got, ok, response)
	}
}

func TestMongoSortedNorExplainPreservesPositiveIndexedResidualWinner(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406636, bson.D{{Key: "createIndexes", Value: "events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}}}, {Key: "name", Value: "tenant_1"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, server, 406637, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "a1"}, {Key: "tenant", Value: "a"}, {Key: "score", Value: int32(1)}, {Key: "rank", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "a2"}, {Key: "tenant", Value: "a"}, {Key: "score", Value: int32(2)}, {Key: "rank", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "b1"}, {Key: "tenant", Value: "b"}, {Key: "score", Value: int32(2)}, {Key: "rank", Value: int32(0)}},
	}}, {Key: "$db", Value: "app"}}))
	command := bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "a"}, {Key: "$nor", Value: bson.A{bson.D{{Key: "score", Value: int32(1)}}}}}}, {Key: "sort", Value: bson.D{{Key: "rank", Value: int32(1)}}}, {Key: "$db", Value: "app"}}
	for _, verbosity := range []string{"queryPlanner", "executionStats"} {
		t.Run(verbosity, func(t *testing.T) {
			response := serveCommand(t, server, 406638, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: verbosity}, {Key: "$db", Value: "app"}})
			assertOK(t, response)
			winning := bson.Raw(response).Lookup("queryPlanner", "winningPlan").Document()
			if stage, ok := winning.Lookup("stage").StringValueOK(); !ok || stage != "secondary_equality_lookup" {
				t.Fatalf("%s stage=%q ok=%v want secondary_equality_lookup: %s", verbosity, stage, ok, response)
			}
			if residual, ok := winning.Lookup("residualFilter").BooleanOK(); !ok || !residual {
				t.Fatalf("%s residualFilter=%v ok=%v want true: %s", verbosity, residual, ok, response)
			}
			if inMemory, ok := winning.Lookup("inMemorySort").BooleanOK(); !ok || !inMemory {
				t.Fatalf("%s inMemorySort=%v ok=%v want true: %s", verbosity, inMemory, ok, response)
			}
			if verbosity == "executionStats" {
				stats := bson.Raw(response).Lookup("executionStats").Document()
				if got, ok := stats.Lookup("nReturned").Int64OK(); !ok || got != 1 {
					t.Fatalf("nReturned=%d ok=%v want 1: %s", got, ok, response)
				}
				if got, ok := stats.Lookup("candidateDocumentsExamined").Int64OK(); !ok || got != 2 {
					t.Fatalf("candidateDocumentsExamined=%d ok=%v want 2: %s", got, ok, response)
				}
			}
		})
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

func TestMongoNegativePredicatesShareReadAndFilterWriteConsumers(t *testing.T) {
	server := newMongoCompatibilityMatrixServer(t)
	assertOK(t, serveCommand(t, server, 406620, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "missing"}, {Key: "state", Value: "missing"}},
		bson.D{{Key: "_id", Value: "null"}, {Key: "score", Value: nil}, {Key: "state", Value: "null"}},
		bson.D{{Key: "_id", Value: "live"}, {Key: "score", Value: int32(7)}, {Key: "state", Value: "live"}, {Key: "profile", Value: bson.D{{Key: "name", Value: "Ada"}}}},
	}}, {Key: "$db", Value: "app"}}))
	nonNull := bson.D{{Key: "score", Value: bson.D{{Key: "$ne", Value: nil}}}}
	find := serveCommand(t, server, 406621, bson.D{{Key: "find", Value: "events"}, {Key: "filter", Value: nonNull}, {Key: "$db", Value: "app"}})
	assertOK(t, find)
	if batch := cursorFirstBatch(t, find); len(batch) != 1 || batch[0].Lookup("_id").StringValue() != "live" {
		t.Fatalf("non-null find batch=%v want live", batch)
	}
	count := serveCommand(t, server, 406622, bson.D{{Key: "count", Value: "events"}, {Key: "query", Value: nonNull}, {Key: "$db", Value: "app"}})
	assertOK(t, count)
	if n, ok := bson.Raw(count).Lookup("n").Int64OK(); !ok || n != 1 {
		t.Fatalf("count n=%d ok=%v want 1", n, ok)
	}
	distinct := serveCommand(t, server, 406623, bson.D{{Key: "distinct", Value: "events"}, {Key: "key", Value: "state"}, {Key: "query", Value: nonNull}, {Key: "$db", Value: "app"}})
	assertOK(t, distinct)
	values, err := bson.Raw(distinct).Lookup("values").Array().Values()
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].StringValue() != "live" {
		t.Fatalf("non-null distinct values=%v want [live]", values)
	}
	aggregate := serveCommand(t, server, 406624, bson.D{{Key: "aggregate", Value: "events"}, {Key: "pipeline", Value: bson.A{
		bson.D{{Key: "$match", Value: nonNull}}, bson.D{{Key: "$count", Value: "n"}},
	}}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}})
	assertOK(t, aggregate)
	if batch := cursorFirstBatch(t, aggregate); len(batch) != 1 || batch[0].Lookup("n").Int64() != 1 {
		t.Fatalf("non-null aggregate batch=%v want count 1", batch)
	}

	update := serveCommand(t, server, 406625, bson.D{{Key: "update", Value: "events"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: nonNull}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "updated", Value: true}}}}},
	}}}, {Key: "$db", Value: "app"}})
	assertOK(t, update)
	assertInt32(t, update, "n", 1)
	famQuery := bson.D{{Key: "$nor", Value: bson.A{
		bson.D{{Key: "score", Value: bson.D{{Key: "$exists", Value: false}}}},
		bson.D{{Key: "score", Value: nil}},
	}}}
	fam := serveCommand(t, server, 406626, bson.D{{Key: "findAndModify", Value: "events"}, {Key: "query", Value: famQuery}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "modified", Value: true}}}}}, {Key: "fields", Value: bson.D{{Key: "profile.name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}}, {Key: "$db", Value: "app"}})
	assertOK(t, fam)
	if value := bson.Raw(fam).Lookup("value").Document(); value.Lookup("profile", "name").StringValue() != "Ada" || value.Lookup("_id").Type != 0 {
		t.Fatalf("findAndModify dotted projection=%s want profile.name without _id", value)
	}
	deleteResponse := serveCommand(t, server, 406627, bson.D{{Key: "delete", Value: "events"}, {Key: "deletes", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "score", Value: bson.D{{Key: "$exists", Value: false}}}}}, {Key: "limit", Value: int32(1)},
	}}}, {Key: "$db", Value: "app"}})
	assertOK(t, deleteResponse)
	assertInt32(t, deleteResponse, "n", 1)
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
