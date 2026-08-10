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
