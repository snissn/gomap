package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var benchmarkMongoMutationDocumentSink wire.Document
var benchmarkRawValuesEqualSink bool

func BenchmarkRawValuesEqualWideDocument(b *testing.B) {
	left := wideRawDocumentValue(4096, true)
	right := wideRawDocumentValue(4096, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkRawValuesEqualSink = rawValuesEqual(left, right)
	}
}

func BenchmarkParseMongoMutationWideEachReject(b *testing.B) {
	values := make(bson.A, 4096)
	for i := range values {
		values[i] = true
	}
	update, err := bson.Marshal(bson.D{{Key: "$push", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: values}}}}}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := parseMongoMutation(update); err == nil {
			b.Fatal("accepted over-limit $each")
		}
	}
}

func BenchmarkApplyMongoMutationWideStoredReject(b *testing.B) {
	doc := rawDocumentWithIDAndValue("benchmark-user", "items", wideRawArrayValue(mongoMutationMaxDecodedElements+1))
	update, err := bson.Marshal(bson.D{
		{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}},
		{Key: "$push", Value: bson.D{{Key: "items", Value: bson.D{{Key: "$each", Value: bson.A{}}}}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	mutation, err := parseMongoMutation(update)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := applyMongoMutation(doc, mutation); err == nil {
			b.Fatal("accepted wide stored BSON")
		}
	}
}

func BenchmarkApplyMongoMutationInc(b *testing.B) {
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "benchmark-user"}, {Key: "count", Value: int64(41)}})
	if err != nil {
		b.Fatal(err)
	}
	update, err := bson.Marshal(bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: int32(1)}}}})
	if err != nil {
		b.Fatal(err)
	}
	mutation, err := parseMongoMutation(update)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		updated, _, err := applyMongoMutation(doc, mutation)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMongoMutationDocumentSink = updated
	}
}

func BenchmarkApplyMongoMutationNestedArray(b *testing.B) {
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "benchmark-user"},
		{Key: "profile", Value: bson.D{{Key: "count", Value: int64(41)}}},
		{Key: "tags", Value: bson.A{"go", "db"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	update, err := bson.Marshal(bson.D{
		{Key: "$set", Value: bson.D{{Key: "profile.name", Value: "ada"}}},
		{Key: "$inc", Value: bson.D{{Key: "profile.count", Value: int32(1)}}},
		{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: bson.D{{Key: "$each", Value: bson.A{"go", "gateway"}}}}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	mutation, err := parseMongoMutation(update)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		updated, _, err := applyMongoMutation(doc, mutation)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMongoMutationDocumentSink = updated
	}
}
