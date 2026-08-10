package mongogateway

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var benchmarkMongoReadCommandSink wire.Document

func BenchmarkMongoAggregateCountDistinctBoundedScan(b *testing.B) {
	const fixtureDocuments = 1000
	server := newMongoCompatibilityMatrixServer(b)
	server.MaxFindScanDocuments = fixtureDocuments
	documents := make(bson.A, fixtureDocuments)
	for i := range documents {
		documents[i] = bson.D{
			{Key: "_id", Value: i},
			{Key: "active", Value: i%2 == 0},
			{Key: "bucket", Value: int32(i % 10)},
		}
	}
	assertOK(b, serveCommand(b, server, 1, bson.D{
		{Key: "insert", Value: "bench_reads"},
		{Key: "documents", Value: documents},
		{Key: "$db", Value: "app"},
	}))

	commands := []struct {
		name        string
		commandName string
		command     bson.D
	}{
		{"find", "find", bson.D{{Key: "find", Value: "bench_reads"}, {Key: "filter", Value: bson.D{{Key: "active", Value: true}}}, {Key: "limit", Value: int32(10)}, {Key: "$db", Value: "app"}}},
		{"count", "count", bson.D{{Key: "count", Value: "bench_reads"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "$db", Value: "app"}}},
		{"distinct_low_cardinality", "distinct", bson.D{{Key: "distinct", Value: "bench_reads"}, {Key: "key", Value: "bucket"}, {Key: "$db", Value: "app"}}},
		{"aggregate_count", "aggregate", bson.D{{Key: "aggregate", Value: "bench_reads"}, {Key: "pipeline", Value: bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}}, bson.D{{Key: "$count", Value: "n"}}}}, {Key: "cursor", Value: bson.D{}}, {Key: "$db", Value: "app"}}},
	}
	for _, benchmark := range commands {
		b.Run(benchmark.name, func(b *testing.B) {
			command := mustDocument(b, benchmark.command)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err := server.commandResponse(context.Background(), benchmark.commandName, command, nil, 0)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkMongoReadCommandSink = response
			}
			b.StopTimer()
			b.ReportMetric(fixtureDocuments, "fixture_docs")
			b.ReportMetric(fixtureDocuments, "scan_bound_docs")
		})
	}
}

// BenchmarkMongoFindExplainDisabled is the ordinary find hot path after the
// optional explain accounting hooks. Keep this separate from explain itself:
// callers that do not request explain must not pay material diagnostic cost.
func BenchmarkMongoFindExplainDisabled(b *testing.B) {
	server := newMongoCompatibilityMatrixServer(b)
	command := mustDocument(b, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "active", Value: true}}}, {Key: "$db", Value: "app"}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := server.commandResponse(context.Background(), "find", command, nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMongoReadCommandSink = response
	}
}
