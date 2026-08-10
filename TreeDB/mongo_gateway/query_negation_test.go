package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoNegativeAndExistsPredicatesRespectMissingNullAndDottedValues(t *testing.T) {
	tests := []struct {
		name   string
		filter bson.D
		doc    bson.D
		want   bool
	}{
		{"ne matches missing", bson.D{{Key: "a", Value: bson.D{{Key: "$ne", Value: int32(1)}}}}, bson.D{{Key: "_id", Value: 1}}, true},
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
