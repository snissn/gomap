package mongogateway

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var benchmarkStandaloneWriteConcernSink wire.Document

func BenchmarkStandaloneWriteConcernAcknowledgement(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		concern bson.D
	}{
		{name: "visible_default"},
		{name: "journal_sync", concern: bson.D{{Key: "j", Value: true}}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, b.TempDir())
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			opts.DisableBackgroundPrune = true
			backend, cleanup, err := treedb.OpenBackend(opts)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = cleanup() }()
			server := NewServer()
			server.Collections = collections.NewCollectionManager(backend)
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
			assertOK(b, serveCommand(b, server, 1, bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "bench"}, {Key: "n", Value: int32(0)}}}},
				{Key: "$db", Value: "app"},
			}))
			commandDocument := bson.D{
				{Key: "update", Value: "users"},
				{Key: "updates", Value: bson.A{bson.D{
					{Key: "q", Value: bson.D{{Key: "_id", Value: "bench"}}},
					{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(1)}}}}},
				}}},
			}
			if benchmark.concern != nil {
				commandDocument = append(commandDocument, bson.E{Key: "writeConcern", Value: benchmark.concern})
			}
			commandDocument = append(commandDocument, bson.E{Key: "$db", Value: "app"})
			command := mustDocument(b, commandDocument)
			before := server.StandaloneWriteConcernStats()
			beforeBackend := writeConcernBenchmarkStat(b, backend.Stats(), "treedb.command_wal.file_sync.calls_total")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err := server.commandResponse(context.Background(), "update", command, nil, 0)
				if err != nil {
					b.Fatal(err)
				}
				if !mongoCommandResponseOK(response) {
					b.Fatalf("update response: %s", response)
				}
				benchmarkStandaloneWriteConcernSink = response
			}
			b.StopTimer()
			after := server.StandaloneWriteConcernStats()
			afterBackend := writeConcernBenchmarkStat(b, backend.Stats(), "treedb.command_wal.file_sync.calls_total")
			operations := float64(b.N)
			ackNanosPerOperation := float64(after.AcknowledgementNanos-before.AcknowledgementNanos) / operations
			b.ReportMetric(ackNanosPerOperation, "ack_ns/op")
			b.ReportMetric(1e9/ackNanosPerOperation, "acks/s")
			b.ReportMetric(float64(after.PhysicalSyncBoundaries-before.PhysicalSyncBoundaries)/operations, "sync_boundaries/op")
			b.ReportMetric(float64(afterBackend-beforeBackend)/operations, "wal_file_syncs/op")
		})
	}
}

func BenchmarkStandaloneWriteConcernConcurrentAcknowledgement(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		concern bson.D
	}{
		{name: "visible_default"},
		{name: "journal_sync", concern: bson.D{{Key: "j", Value: true}}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, b.TempDir())
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			opts.DisableBackgroundPrune = true
			backend, cleanup, err := treedb.OpenBackend(opts)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = cleanup() }()
			server := NewServer()
			server.Collections = collections.NewCollectionManager(backend)
			server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}

			commands := make([]wire.Document, b.N)
			for i := range commands {
				commandDocument := bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: fmt.Sprintf("bench-%d", i)}}}},
				}
				if benchmark.concern != nil {
					commandDocument = append(commandDocument, bson.E{Key: "writeConcern", Value: benchmark.concern})
				}
				commandDocument = append(commandDocument, bson.E{Key: "$db", Value: "app"})
				commands[i] = mustDocument(b, commandDocument)
			}

			before := server.StandaloneWriteConcernStats()
			beforeBackend := writeConcernBenchmarkStat(b, backend.Stats(), "treedb.command_wal.file_sync.calls_total")
			var next atomic.Uint64
			var firstErr string
			var firstErrOnce sync.Once
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := int(next.Add(1) - 1)
					response, err := server.commandResponse(context.Background(), "insert", commands[i], nil, 0)
					if err != nil || !mongoCommandResponseOK(response) {
						firstErrOnce.Do(func() { firstErr = fmt.Sprintf("insert err=%v response=%s", err, response) })
					}
				}
			})
			b.StopTimer()
			elapsed := b.Elapsed()
			if firstErr != "" {
				b.Fatal(firstErr)
			}
			after := server.StandaloneWriteConcernStats()
			afterBackend := writeConcernBenchmarkStat(b, backend.Stats(), "treedb.command_wal.file_sync.calls_total")
			operations := float64(b.N)
			ackNanosPerOperation := float64(after.AcknowledgementNanos-before.AcknowledgementNanos) / operations
			b.ReportMetric(ackNanosPerOperation, "ack_ns/op")
			b.ReportMetric(1e9/ackNanosPerOperation, "acks/s")
			b.ReportMetric(operations/elapsed.Seconds(), "throughput_ops/s")
			b.ReportMetric(float64(after.PhysicalSyncBoundaries-before.PhysicalSyncBoundaries)/operations, "sync_boundaries/op")
			b.ReportMetric(float64(afterBackend-beforeBackend)/operations, "wal_file_syncs/op")
		})
	}
}

func writeConcernBenchmarkStat(tb testing.TB, stats map[string]string, key string) uint64 {
	tb.Helper()
	value, ok := stats[key]
	if !ok {
		tb.Fatalf("backend stat %q is unavailable", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		tb.Fatalf("parse backend stat %q=%q: %v", key, value, err)
	}
	return parsed
}
