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
	server := newMongoCompatibilityMatrixServer(b)
	server.MaxFindScanDocuments = 8 // fixture has three IDs; no scan truncation.
	for _, tc := range []struct {
		name    string
		command bson.D
		field   string
		want    int64
	}{
		{name: "dbStats", command: bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}}, field: "objects", want: 3},
		{name: "collStats", command: bson.D{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}}, field: "count", want: 3},
	} {
		b.Run(tc.name, func(b *testing.B) {
			warmup := serveCommand(b, server, 1, tc.command)
			if value, ok := warmup.Lookup(tc.field).Int64OK(); !ok || value != tc.want {
				b.Fatalf("warmup %s=%s", tc.name, warmup)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = serveCommand(b, server, int32(i+2), tc.command)
			}
		})
	}
}
