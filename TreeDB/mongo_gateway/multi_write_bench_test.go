package mongogateway

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BenchmarkMongoInsertCommandBatchSizes compares one insert command with N
// documents against N one-document commands. It reports both command ns/op and
// normalized per-document ns/doc; commands are built before timing so BSON
// construction does not hide gateway execution work. Run with a fixed short
// benchtime (for example -benchtime=1x) because prebuilt fixtures scale with
// b.N * batchSize.
func BenchmarkMongoInsertCommandBatchSizes(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			benchmarkMongoInsertCommandShape(b, size, true)
		})
		b.Run(fmt.Sprintf("singles_%d", size), func(b *testing.B) {
			benchmarkMongoInsertCommandShape(b, size, false)
		})
	}
}

func BenchmarkMongoUpdateManyCommandBatchSizes(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("many_%d", size), func(b *testing.B) { benchmarkMongoFilterWriteShape(b, size, false, true) })
		b.Run(fmt.Sprintf("singles_%d", size), func(b *testing.B) { benchmarkMongoFilterWriteShape(b, size, false, false) })
	}
}

// BenchmarkMongoUpdateCommandBatchSizes measures the BulkWrite-style exact-id
// shape separately from UpdateMany: one update command containing N operator
// updates versus N single-update commands.
func BenchmarkMongoUpdateCommandBatchSizes(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) { benchmarkMongoExactUpdateShape(b, size, true) })
		b.Run(fmt.Sprintf("singles_%d", size), func(b *testing.B) { benchmarkMongoExactUpdateShape(b, size, false) })
	}
}

func BenchmarkMongoDeleteManyCommandBatchSizes(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("many_%d", size), func(b *testing.B) { benchmarkMongoFilterWriteShape(b, size, true, true) })
		b.Run(fmt.Sprintf("singles_%d", size), func(b *testing.B) { benchmarkMongoFilterWriteShape(b, size, true, false) })
	}
}

func benchmarkMongoInsertCommandShape(b *testing.B, batchSize int, batched bool) {
	db, cleanup, err := openMongoWriteBenchmarkBackend(b)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cleanup() }()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	commands := make([]bson.D, 0, b.N)
	for op := 0; op < b.N; op++ {
		if batched {
			commands = append(commands, benchmarkMongoInsertCommand(op, 0, batchSize))
			continue
		}
		for item := 0; item < batchSize; item++ {
			commands = append(commands, benchmarkMongoInsertCommand(op, item, 1))
		}
	}
	b.ReportAllocs()
	beforeBackend := db.Stats()
	beforeCollections := s.Collections.Stats()
	b.ResetTimer()
	started := time.Now()
	for i, command := range commands {
		response := serveCommand(b, s, int32(i+1), command)
		want := int32(1)
		if batched {
			want = int32(batchSize)
		}
		assertMongoBenchmarkWriteResponse(b, response, want, "insert")
	}
	elapsed := time.Since(started)
	b.StopTimer()
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N*batchSize), "ns/doc")
	b.ReportMetric(float64(b.N*batchSize)*float64(time.Second)/float64(elapsed), "docs/s")
	b.ReportMetric(float64(batchSize), "docs/op")
	reportMongoWriteBenchmarkAccounting(b, db, s.Collections, beforeBackend, beforeCollections, b.N*batchSize)
}

// benchmarkMongoFilterWriteShape uses a multi:true/limit:0 natural-order
// command for the batch side and exact-_id commands for the equivalent
// repeated-single side. Documents and BSON command shapes are made before the
// timer starts, leaving only gateway/storage write work in the measurement.
func benchmarkMongoFilterWriteShape(b *testing.B, batchSize int, deleting, batched bool) {
	db, cleanup, err := openMongoWriteBenchmarkBackend(b)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cleanup() }()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	seed := make(bson.A, 0, b.N*batchSize)
	for op := 0; op < b.N; op++ {
		for item := 0; item < batchSize; item++ {
			seed = append(seed, bson.D{{Key: "_id", Value: fmt.Sprintf("filter-%d-%d", op, item)}, {Key: "group", Value: fmt.Sprintf("g-%d", op)}, {Key: "seen", Value: false}})
		}
	}
	assertMongoBenchmarkWriteResponse(b, serveCommand(b, s, 1, bson.D{{Key: "insert", Value: "bench"}, {Key: "documents", Value: seed}, {Key: "$db", Value: "app"}}), int32(len(seed)), "filter seed")
	commands := make([]bson.D, 0, b.N)
	for op := 0; op < b.N; op++ {
		if batched {
			commands = append(commands, benchmarkMongoFilterCommand(op, 0, batchSize, deleting, true))
			continue
		}
		for item := 0; item < batchSize; item++ {
			commands = append(commands, benchmarkMongoFilterCommand(op, item, 1, deleting, false))
		}
	}
	b.ReportAllocs()
	beforeBackend := db.Stats()
	beforeCollections := s.Collections.Stats()
	b.ResetTimer()
	started := time.Now()
	for i, command := range commands {
		response := serveCommand(b, s, int32(i+2), command)
		want := int32(1)
		if batched {
			want = int32(batchSize)
		}
		assertMongoBenchmarkWriteResponse(b, response, want, "filter write")
	}
	elapsed := time.Since(started)
	b.StopTimer()
	docs := b.N * batchSize
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
	b.ReportMetric(float64(docs)*float64(time.Second)/float64(elapsed), "docs/s")
	b.ReportMetric(float64(batchSize), "docs/op")
	reportMongoWriteBenchmarkAccounting(b, db, s.Collections, beforeBackend, beforeCollections, docs)
}

func benchmarkMongoExactUpdateShape(b *testing.B, batchSize int, batched bool) {
	db, cleanup, err := openMongoWriteBenchmarkBackend(b)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cleanup() }()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	seed := make(bson.A, 0, b.N*batchSize)
	for op := 0; op < b.N; op++ {
		for item := 0; item < batchSize; item++ {
			seed = append(seed, bson.D{{Key: "_id", Value: fmt.Sprintf("exact-%d-%d", op, item)}, {Key: "seen", Value: false}})
		}
	}
	assertMongoBenchmarkWriteResponse(b, serveCommand(b, s, 1, bson.D{{Key: "insert", Value: "bench"}, {Key: "documents", Value: seed}, {Key: "$db", Value: "app"}}), int32(len(seed)), "exact update seed")
	commands := make([]bson.D, 0, b.N)
	for op := 0; op < b.N; op++ {
		if batched {
			updates := make(bson.A, 0, batchSize)
			for item := 0; item < batchSize; item++ {
				updates = append(updates, bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("exact-%d-%d", op, item)}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}})
			}
			commands = append(commands, bson.D{{Key: "update", Value: "bench"}, {Key: "updates", Value: updates}, {Key: "$db", Value: "app"}})
			continue
		}
		for item := 0; item < batchSize; item++ {
			commands = append(commands, bson.D{{Key: "update", Value: "bench"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("exact-%d-%d", op, item)}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
		}
	}
	b.ReportAllocs()
	beforeBackend := db.Stats()
	beforeCollections := s.Collections.Stats()
	b.ResetTimer()
	started := time.Now()
	for i, command := range commands {
		response := serveCommand(b, s, int32(i+2), command)
		want := int32(1)
		if batched {
			want = int32(batchSize)
		}
		assertMongoBenchmarkWriteResponse(b, response, want, "exact update")
	}
	elapsed := time.Since(started)
	b.StopTimer()
	docs := b.N * batchSize
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
	b.ReportMetric(float64(docs)*float64(time.Second)/float64(elapsed), "docs/s")
	b.ReportMetric(float64(batchSize), "docs/op")
	reportMongoWriteBenchmarkAccounting(b, db, s.Collections, beforeBackend, beforeCollections, docs)
}

// assertMongoBenchmarkWriteResponse keeps timing-loop samples meaningful: a
// command that merely returns ok:1 with an indexed error is not a successful
// batch/single execution and must not enter the performance comparison.
func assertMongoBenchmarkWriteResponse(b testing.TB, response wire.Document, wantN int32, operation string) {
	b.Helper()
	assertOK(b, response)
	assertInt32(b, response, "n", wantN)
	if !bson.Raw(response).Lookup("writeErrors").IsZero() {
		b.Fatalf("%s response has writeErrors: %s", operation, response)
	}
}

// openMongoWriteBenchmarkBackend makes the command-WAL byte counter visible to
// every shape. Fixture construction and the initial seed are outside the timer;
// each reported counter is a delta over the timed command loop only.
func openMongoWriteBenchmarkBackend(b *testing.B) (*backenddb.DB, func() error, error) {
	b.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, b.TempDir())
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.DisableBackgroundPrune = true
	return treedb.OpenBackend(opts)
}

func reportMongoWriteBenchmarkAccounting(b *testing.B, db *backenddb.DB, manager *collections.CollectionManager, beforeBackend, beforeCollections map[string]string, documents int) {
	b.Helper()
	if documents <= 0 {
		b.Fatal("benchmark documents must be positive")
	}
	afterBackend := db.Stats()
	afterCollections := manager.Stats()
	denominator := float64(documents)
	b.ReportMetric(float64(mongoWriteBenchmarkStat(b, afterBackend, "treedb.command_wal.write.bytes_total")-mongoWriteBenchmarkStat(b, beforeBackend, "treedb.command_wal.write.bytes_total"))/denominator, "wal_bytes/doc")
	// Native BSON insert has no indexed write-domain counters. Report each root
	// counter only when the backend exposes it rather than treating unavailable
	// accounting as a measured zero.
	for _, counter := range []struct {
		key    string
		metric string
	}{
		{"treedb.collections.write_domain.indexed_stage.root_runs_total", "indexed_stage_root_runs/doc"},
		{"treedb.collections.write_domain.indexed_flush.root_runs_total", "indexed_flush_root_runs/doc"},
		{"treedb.collections.write_domain.indexed_flush.roots_total", "indexed_flush_roots/doc"},
	} {
		after, afterOK := mongoWriteBenchmarkStatOK(afterCollections, counter.key)
		before, beforeOK := mongoWriteBenchmarkStatOK(beforeCollections, counter.key)
		if afterOK && beforeOK {
			b.ReportMetric(float64(after-before)/denominator, counter.metric)
		}
	}
}

func mongoWriteBenchmarkStat(b *testing.B, stats map[string]string, key string) uint64 {
	b.Helper()
	value, ok := stats[key]
	if !ok {
		b.Fatalf("benchmark stat %q unavailable", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		b.Fatalf("parse benchmark stat %q=%q: %v", key, value, err)
	}
	return parsed
}

func mongoWriteBenchmarkStatOK(stats map[string]string, key string) (uint64, bool) {
	value, ok := stats[key]
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil
}

func benchmarkMongoFilterCommand(op, first, count int, deleting, multi bool) bson.D {
	var query bson.D
	if multi {
		query = bson.D{{Key: "group", Value: fmt.Sprintf("g-%d", op)}}
	} else {
		query = bson.D{{Key: "_id", Value: fmt.Sprintf("filter-%d-%d", op, first)}}
	}
	if deleting {
		limit := int32(1)
		if multi {
			limit = 0
		}
		return bson.D{{Key: "delete", Value: "bench"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: query}, {Key: "limit", Value: limit}}}}, {Key: "$db", Value: "app"}}
	}
	return bson.D{{Key: "update", Value: "bench"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: query}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: true}}}}}, {Key: "multi", Value: multi}}}}, {Key: "$db", Value: "app"}}
}

func benchmarkMongoInsertCommand(op, first, count int) bson.D {
	documents := make(bson.A, 0, count)
	for item := first; item < first+count; item++ {
		documents = append(documents, bson.D{{Key: "_id", Value: fmt.Sprintf("bench-%d-%d", op, item)}, {Key: "value", Value: int32(item)}})
	}
	return bson.D{{Key: "insert", Value: "bench"}, {Key: "documents", Value: documents}, {Key: "$db", Value: "app"}}
}
