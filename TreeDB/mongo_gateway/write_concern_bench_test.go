package mongogateway

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// BenchmarkStandaloneNativeJournalAcknowledgement measures complete direct
// native gateway dispatch plus explicit journal ACK, excluding client/wire I/O.
// The legacy OpenBackend benchmark above remains a separate compatibility control.
// Use the fixed 64x recipe for the bounded default-maintenance characterization.
func BenchmarkStandaloneNativeJournalAcknowledgement(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		b.Run(string(profile), func(b *testing.B) {
			b.StopTimer()
			opts := StandaloneOptions{Dir: b.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
			open := func() *StandaloneServer {
				s, err := OpenStandaloneServer(opts)
				if err != nil {
					b.Fatalf("open standalone: %v", err)
				}
				b.Cleanup(func() {
					if err := s.Close(); err != nil {
						b.Errorf("checked standalone cleanup: %v", err)
					}
				})
				requireStandaloneNativeJournalOwner(b, s)
				return s
			}
			standalone := open()
			server := standalone.Server
			assertOK(b, serveCommand(b, server, 1, bson.D{{Key: "create", Value: "journal_bench"}, {Key: "$db", Value: "app"}}))
			assertOK(b, serveCommand(b, server, 2, bson.D{
				{Key: "insert", Value: "journal_bench"},
				{Key: "documents", Value: bson.A{standaloneNativeJournalDocument("bench", 0)}},
				{Key: "$db", Value: "app"},
			}))
			var commands [2]wire.Document
			for i := range commands {
				commands[i] = mustDocument(b, bson.D{
					{Key: "update", Value: "journal_bench"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "bench"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: standaloneNativeJournalDocument("bench", i+1)[1:]}}},
					}}},
					{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}, {Key: "j", Value: true}}},
					{Key: "$db", Value: "app"},
				})
			}
			b.Logf("native benchmark runtime version=%s GOMAXPROCS=%d NumCPU=%d", runtime.Version(), runtime.GOMAXPROCS(0), runtime.NumCPU())
			beforeOwner := standalone.Stats()
			beforeACK := server.StandaloneWriteConcernStats()
			var allocationStart, allocationEnd runtime.MemStats
			var successful uint64
			b.ReportAllocs()
			b.ResetTimer()
			// These process allocation counters bracket only StartTimer/loop/
			// StopTimer; setup, stats, readback, reopen and Close are excluded.
			// Go's B/op and allocs/op independently use the benchmark timer.
			runtime.ReadMemStats(&allocationStart)
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				response, err := server.commandResponse(context.Background(), "update", commands[i&1], nil, 0)
				if err != nil || !mongoCommandResponseOK(response) {
					b.Fatalf("native journal update %d response=%s err=%v", i, response, err)
				}
				raw := bson.Raw(response)
				matched, matchedOK := raw.Lookup("n").AsInt64OK()
				modified, modifiedOK := raw.Lookup("nModified").AsInt64OK()
				if !matchedOK || !modifiedOK || matched != 1 || modified != 1 || raw.Lookup("writeConcernError").Type != 0 || raw.Lookup("writeErrors").Type != 0 {
					b.Fatalf("update %d must actually mutate once with certain ACK: %s", i, response)
				}
				successful++
				benchmarkStandaloneWriteConcernSink = response
			}
			b.StopTimer()
			runtime.ReadMemStats(&allocationEnd)
			afterOwner := standalone.Stats()
			afterACK := server.StandaloneWriteConcernStats()
			if successful != uint64(b.N) || afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != successful ||
				afterACK.SyncAttempts-beforeACK.SyncAttempts != successful || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != successful ||
				afterACK.SyncFailures != beforeACK.SyncFailures || afterACK.UncertainOutcomes != beforeACK.UncertainOutcomes {
				b.Fatalf("native ACK count=%d N=%d before=%+v after=%+v", successful, b.N, beforeACK, afterACK)
			}
			delta := func(key string) uint64 {
				first, last := writeConcernBenchmarkStat(b, beforeOwner, key), writeConcernBenchmarkStat(b, afterOwner, key)
				if last < first {
					b.Fatalf("counter %s decreased %d -> %d", key, first, last)
				}
				return last - first
			}
			walSyncs := delta("treedb.command_wal.sync.count_total")
			fileSyncs := delta("treedb.command_wal.file_sync.calls_total")
			if walSyncs < successful || fileSyncs < successful {
				b.Fatalf("incomplete native physical boundary: ACKs=%d WAL sync operations=%d physical file-sync calls=%d", successful, walSyncs, fileSyncs)
			}
			operations := float64(successful)
			b.ReportMetric(operations/b.Elapsed().Seconds(), "operations/s")
			b.ReportMetric(float64(afterACK.AcknowledgementNanos-beforeACK.AcknowledgementNanos)/operations, "ack_ns/op")
			b.ReportMetric(float64(delta("treedb.cache.checkpoint.runs"))/operations, "cache_checkpoints/op")
			b.ReportMetric(float64(delta("treedb.command_wal.group_commit.forced_total"))/operations, "forced_groups/op")
			b.ReportMetric(float64(walSyncs)/operations, "wal_sync_operations/op")
			b.ReportMetric(float64(fileSyncs)/operations, "wal_file_syncs/op")
			b.Logf("native allocation bracket successful=%d total_alloc_bytes=%d mallocs=%d; per-operation denominator=%d; setup/readback/reopen/Close excluded; process counters include concurrent default maintenance", successful, allocationEnd.TotalAlloc-allocationStart.TotalAlloc, allocationEnd.Mallocs-allocationStart.Mallocs, successful)
			b.Logf("native original owner mode=%s successful=%d cache_checkpoint_delta=%d forced_group_delta=%d WAL_sync_operations_delta=%d WAL_file_sync_calls_delta=%d before_ack=%+v after_ack=%+v", afterOwner["treedb.cache.memtable_mode"], successful, delta("treedb.cache.checkpoint.runs"), delta("treedb.command_wal.group_commit.forced_total"), walSyncs, fileSyncs, beforeACK, afterACK)
			generation := 0
			if successful != 0 {
				generation = int((successful-1)&1) + 1
			}
			check := func(s *StandaloneServer) {
				response := serveCommand(b, s.Server, 3, bson.D{{Key: "find", Value: "journal_bench"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}})
				assertOK(b, response)
				documents := cursorFirstBatch(b, response)
				if len(documents) != 1 {
					b.Fatalf("native readback count=%d want 1", len(documents))
				}
				requireStandaloneNativeJournalDocument(b, documents[0], "bench", generation)
			}
			check(standalone)
			originalCloseStarted := time.Now()
			if err := standalone.Close(); err != nil {
				b.Fatalf("original standalone close: %v", err)
			}
			originalCloseElapsed := time.Since(originalCloseStarted)
			freshReopenStarted := time.Now()
			fresh := open()
			check(fresh)
			freshReadbackElapsed := time.Since(freshReopenStarted)
			freshCloseStarted := time.Now()
			if err := fresh.Close(); err != nil {
				b.Fatalf("fresh standalone close: %v", err)
			}
			freshCloseElapsed := time.Since(freshCloseStarted)
			b.Logf("untimed lifecycle original_Close_ns=%d fresh_open_readback_ns=%d fresh_Close_ns=%d; report fixed lifecycle debt separately from timed mutation+ACK", originalCloseElapsed.Nanoseconds(), freshReadbackElapsed.Nanoseconds(), freshCloseElapsed.Nanoseconds())
			b.Log("original server/manager/DB Close, fresh native owner typed reopen/readback and Close: PASS; graceful reopen is not crash proof")
		})
	}
}

// Exercise the actual default queue/done-channel seam for both plain and strict
// single-document inserts. Each operation inserts a fresh key. Setup, full exact
// typed readback, original Close and fresh-owner verification are untimed.
func BenchmarkStandaloneNativeInsertReceiptAllocation(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, strict := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/strict=%t", profile, strict), func(b *testing.B) {
				b.StopTimer()
				opts := StandaloneOptions{Dir: b.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
				open := func() *StandaloneServer {
					s, err := OpenStandaloneServer(opts)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() {
						if err := s.Close(); err != nil {
							b.Error(err)
						}
					})
					requireStandaloneNativeJournalOwner(b, s)
					return s
				}
				s := open()
				assertOK(b, serveCommand(b, s.Server, 1, bson.D{{Key: "create", Value: "receipt_alloc"}, {Key: "$db", Value: "app"}}))
				if s.Server.InsertCoalescingMaxBatch < 2 {
					b.Fatal("default queue inactive")
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.StartTimer()
				for i := 0; i < b.N; i++ {
					command := bson.D{{Key: "insert", Value: "receipt_alloc"}, {Key: "documents", Value: bson.A{standaloneNativeJournalDocument(strconv.Itoa(i), 1)}}}
					if strict {
						command = append(command, bson.E{Key: "writeConcern", Value: bson.D{{Key: "w", Value: 1}, {Key: "j", Value: true}}})
					}
					command = append(command, bson.E{Key: "$db", Value: "app"})
					response, err := s.Server.commandResponse(context.Background(), "insert", mustDocument(b, command), nil, 0)
					if err != nil || !mongoCommandResponseOK(response) || bson.Raw(response).Lookup("writeConcernError").Type != 0 || bson.Raw(response).Lookup("writeErrors").Type != 0 {
						b.Fatalf("insert%d response=%s err=%v", i, response, err)
					}
					benchmarkStandaloneWriteConcernSink = response
				}
				b.StopTimer()
				check := func(owner *StandaloneServer) {
					for _, i := range []int{0, b.N - 1} {
						if i < 0 {
							continue
						}
						response := serveCommand(b, owner.Server, 2, bson.D{{Key: "find", Value: "receipt_alloc"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: strconv.Itoa(i)}}}, {Key: "$db", Value: "app"}})
						assertOK(b, response)
						docs := cursorFirstBatch(b, response)
						if len(docs) != 1 {
							b.Fatalf("readback%d count=%d", i, len(docs))
						}
						requireStandaloneNativeJournalDocument(b, docs[0], strconv.Itoa(i), 1)
					}
				}
				check(s)
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
				fresh := open()
				check(fresh)
				if err := fresh.Close(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
