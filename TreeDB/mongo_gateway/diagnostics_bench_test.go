package mongogateway

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkDiagnosticsServerStatus(b *testing.B) {
	server := NewServer()
	command := bson.D{{Key: "serverStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}}
	if response := serveCommand(b, server, 1, command); response == nil {
		b.Fatal("warmup serverStatus response is nil")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = serveCommand(b, server, int32(i+2), command)
	}
}

func BenchmarkDiagnosticsTop(b *testing.B) {
	server := NewServer()
	for i := 0; i < 64; i++ {
		server.noteDiagnosticCommand("insert", mustDocument(b, bson.D{{Key: "insert", Value: "users"}, {Key: "$db", Value: "app"}}), 0, false)
	}
	command := bson.D{{Key: "top", Value: int32(1)}, {Key: "$db", Value: "admin"}}
	if response := serveCommand(b, server, 1, command); response == nil {
		b.Fatal("warmup top response is nil")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = serveCommand(b, server, int32(i+2), command)
	}
}

func BenchmarkDiagnosticsCollectionStats(b *testing.B) {
	for _, tc := range []struct {
		name string
		cap  int
	}{
		{name: "small_fixture", cap: 8},
		// The seeded fixture has three live primary IDs and one merged-source
		// inspection. This near-cap witness therefore makes four charged work
		// units explicit for both dbStats and collStats.
		{name: "near_cap_four_physical_units", cap: 4},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for _, command := range []struct {
				name  string
				field string
				value any
			}{
				{name: "dbStats", field: "objects", value: int32(1)},
				{name: "collStats", field: "count", value: "users"},
			} {
				b.Run(command.name, func(b *testing.B) {
					server := newMongoCompatibilityMatrixServer(b)
					server.MaxFindScanDocuments = tc.cap
					warmup := serveCommand(b, server, 1, bson.D{{Key: command.name, Value: command.value}, {Key: "$db", Value: "app"}})
					if value, ok := warmup.Lookup(command.field).Int64OK(); !ok || value != 3 {
						b.Fatalf("warmup %s=%s", command.name, warmup)
					}
					_, inspected, truncated, err := server.diagnosticCollectionCountWithin("app.users", tc.cap)
					if err != nil || truncated || inspected != 4 {
						b.Fatalf("physical work inspected=%d truncated=%v err=%v want 4/false/nil", inspected, truncated, err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_ = serveCommand(b, server, int32(i+2), bson.D{{Key: command.name, Value: command.value}, {Key: "$db", Value: "app"}})
					}
					b.ReportMetric(float64(inspected), "physical_ids/op")
				})
			}
		})
	}
}
