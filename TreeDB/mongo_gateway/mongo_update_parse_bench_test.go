package mongogateway

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var benchmarkMongoUpdateItemSink mongoUpdateItem

func BenchmarkParseMongoUpdateItemPureSet(b *testing.B) {
	raw, err := bson.Marshal(bson.D{
		{Key: "q", Value: bson.D{{Key: "_id", Value: "benchmark-user"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "grace"}, {Key: "age", Value: int32(42)}}}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, err := parseMongoUpdateItem(0, raw)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMongoUpdateItemSink = item
	}
}
