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
		name      string
		cap       int
		documents int
	}{
		{name: "small_fixture", cap: 8, documents: 3},
		{name: "representative_ten_live_documents", cap: 10, documents: 10},
		// The seeded fixture has three live primary IDs and one merged-source
		// inspection. The public cap is three live documents; the separate
		// diagnostics source-work budget is six units, of which this fixture uses
		// four (positioning plus three primary IDs).
		{name: "near_cap_three_live_documents", cap: 3, documents: 3},
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
					if tc.documents > 3 {
						documents := make(bson.A, 0, tc.documents-3)
						for id := 3; id < tc.documents; id++ {
							documents = append(documents, bson.D{{Key: "_id", Value: id}})
						}
						response := serveCommand(b, server, 0, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: documents}, {Key: "$db", Value: "app"}})
						if ok, valid := response.Lookup("ok").DoubleOK(); !valid || ok != 1 {
							b.Fatalf("seed representative documents: %s", response)
						}
					}
					server.MaxFindScanDocuments = tc.cap
					warmup := serveCommand(b, server, 1, bson.D{{Key: command.name, Value: command.value}, {Key: "$db", Value: "app"}})
					if value, ok := warmup.Lookup(command.field).Int64OK(); !ok || value != int64(tc.documents) {
						b.Fatalf("warmup %s=%s", command.name, warmup)
					}
					_, inspected, truncated, err := server.diagnosticCollectionCountWithin("app.users", tc.cap, diagnosticPhysicalWorkBudget(tc.cap))
					if err != nil || truncated || inspected != tc.documents+1 {
						b.Fatalf("physical work inspected=%d truncated=%v err=%v want %d/false/nil", inspected, truncated, err, tc.documents+1)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_ = serveCommand(b, server, int32(i+2), bson.D{{Key: command.name, Value: command.value}, {Key: "$db", Value: "app"}})
					}
					b.ReportMetric(float64(tc.documents), "live_documents/op")
					b.ReportMetric(float64(inspected), "physical_source_work/op")
				})
			}
		})
	}
}
