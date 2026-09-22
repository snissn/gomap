package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var benchmarkFindAndModifyItemSink mongoUpdateItem

func BenchmarkFindAndModifyUpdateParse(b *testing.B) {
	command, err := marshalDocument(bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "benchmark-user"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: int32(1)}}}}}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, err := parseMongoUpdateItem(0, wire.Document(command))
		if err != nil {
			b.Fatal(err)
		}
		benchmarkFindAndModifyItemSink = item
	}
}
