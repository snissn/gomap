package mongogateway

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BenchmarkMongoNegativeDottedQueryShapes records the four bounded #4066
// shapes. Each sub-benchmark first captures the exact explain counters it
// asserts, then reports them beside Go's ns/op, B/op and allocs/op metrics.
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
			metrics := benchmarkMongoExplainMetrics(b, server, spec)
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
			b.ReportMetric(float64(metrics.returned), "returned_docs")
			b.ReportMetric(float64(metrics.examined), "candidate_docs")
			b.ReportMetric(float64(metrics.materialized), "materialized_docs")
			b.ReportMetric(float64(metrics.materializedBytes), "materialized_bytes")
		})
	}
}

type mongoBenchmarkExplainMetrics struct {
	returned, examined, materialized, materializedBytes int64
}

func benchmarkMongoExplainMetrics(b *testing.B, server *Server, command bson.D) mongoBenchmarkExplainMetrics {
	b.Helper()
	response := serveCommand(b, server, 3, bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"}})
	assertOK(b, response)
	stats := bson.Raw(response).Lookup("executionStats").Document()
	lookup := func(name string) int64 {
		value, ok := stats.Lookup(name).Int64OK()
		if !ok || value < 0 {
			b.Fatalf("explain %s=%d ok=%v: %s", name, value, ok, response)
		}
		return value
	}
	return mongoBenchmarkExplainMetrics{
		returned:          lookup("nReturned"),
		examined:          lookup("candidateDocumentsExamined"),
		materialized:      lookup("candidateDocumentsMaterialized"),
		materializedBytes: lookup("candidateMaterializedBytes"),
	}
}
