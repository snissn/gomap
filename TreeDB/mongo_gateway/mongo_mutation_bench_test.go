package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var benchmarkMongoMutationDocumentSink wire.Document

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
