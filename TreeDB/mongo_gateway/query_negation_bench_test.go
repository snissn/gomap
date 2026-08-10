package mongogateway

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BenchmarkMongoNegativeDottedQueryShapes records the four bounded #4066
// shapes. Go reports ns/op, B/op and allocs/op; explain tests pin candidates
// and materialized bytes independently of timing noise.
func BenchmarkMongoNegativeDottedQueryShapes(b *testing.B) {
	server := newMongoCompatibilityMatrixServer(b)
	server.MaxFindScanDocuments = 256
	docs := make(bson.A, 256)
	for i := range docs {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "tenant", Value: "a"}, {Key: "score", Value: int32(i % 3)}, {Key: "profile", Value: bson.D{{Key: "name", Value: string(rune('a' + i%26))}}}}
	}
	assertOK(b, serveCommand(b, server, 1, bson.D{{Key: "insert", Value: "bench_negative"}, {Key: "documents", Value: docs}, {Key: "$db", Value: "app"}}))
	assertOK(b, serveCommand(b, server, 2, bson.D{{Key: "createIndexes", Value: "bench_negative"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}}}, {Key: "name", Value: "tenant_1"}}}}, {Key: "$db", Value: "app"}}))
	shapes := map[string]bson.D{
		"indexed_positive_residual": {{Key: "find", Value: "bench_negative"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "a"}, {Key: "score", Value: bson.D{{Key: "$ne", Value: int32(1)}}}}}, {Key: "limit", Value: int32(20)}, {Key: "$db", Value: "app"}},
		"bounded_negative_scan":     {{Key: "find", Value: "bench_negative"}, {Key: "filter", Value: bson.D{{Key: "score", Value: bson.D{{Key: "$nin", Value: bson.A{int32(1), int32(2)}}}}}}, {Key: "$db", Value: "app"}},
		"dotted_projection":         {{Key: "find", Value: "bench_negative"}, {Key: "projection", Value: bson.D{{Key: "profile.name", Value: int32(1)}}}, {Key: "$db", Value: "app"}},
		"dotted_sort_materialized":  {{Key: "find", Value: "bench_negative"}, {Key: "sort", Value: bson.D{{Key: "profile.name", Value: int32(1)}}}, {Key: "$db", Value: "app"}},
	}
	for name, spec := range shapes {
		b.Run(name, func(b *testing.B) {
			cmd := mustDocument(b, spec)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := server.commandResponse(context.Background(), "find", cmd, nil, 0)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkMongoReadCommandSink = wire.Document(out)
			}
			b.ReportMetric(256, "fixture_docs")
		})
	}
}
