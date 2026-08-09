package mongogateway

import (
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	db, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
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
	b.ResetTimer()
	started := time.Now()
	for i, command := range commands {
		response := serveCommand(b, s, int32(i+1), command)
		if ok, okOK := bson.Raw(response).Lookup("ok").DoubleOK(); !okOK || ok != 1 {
			b.Fatalf("insert response=%s", response)
		}
	}
	elapsed := time.Since(started)
	b.StopTimer()
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N*batchSize), "ns/doc")
	b.ReportMetric(float64(batchSize), "docs/op")
}

// benchmarkMongoFilterWriteShape uses a multi:true/limit:0 natural-order
// command for the batch side and exact-_id commands for the equivalent
// repeated-single side. Documents and BSON command shapes are made before the
// timer starts, leaving only gateway/storage write work in the measurement.
func benchmarkMongoFilterWriteShape(b *testing.B, batchSize int, deleting, batched bool) {
	db, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	seed := make(bson.A, 0, b.N*batchSize)
	for op := 0; op < b.N; op++ {
		for item := 0; item < batchSize; item++ {
			seed = append(seed, bson.D{{Key: "_id", Value: fmt.Sprintf("filter-%d-%d", op, item)}, {Key: "group", Value: fmt.Sprintf("g-%d", op)}, {Key: "seen", Value: false}})
		}
	}
	assertOK(b, serveCommand(b, s, 1, bson.D{{Key: "insert", Value: "bench"}, {Key: "documents", Value: seed}, {Key: "$db", Value: "app"}}))
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
	b.ResetTimer()
	started := time.Now()
	for i, command := range commands {
		response := serveCommand(b, s, int32(i+2), command)
		if ok, okOK := bson.Raw(response).Lookup("ok").DoubleOK(); !okOK || ok != 1 {
			b.Fatalf("filter-write response=%s", response)
		}
	}
	elapsed := time.Since(started)
	b.StopTimer()
	docs := b.N * batchSize
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
	b.ReportMetric(float64(docs)*float64(time.Second)/float64(elapsed), "docs/s")
	b.ReportMetric(float64(batchSize), "docs/op")
}

func benchmarkMongoExactUpdateShape(b *testing.B, batchSize int, batched bool) {
	db, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	seed := make(bson.A, 0, b.N*batchSize)
	for op := 0; op < b.N; op++ {
		for item := 0; item < batchSize; item++ {
			seed = append(seed, bson.D{{Key: "_id", Value: fmt.Sprintf("exact-%d-%d", op, item)}, {Key: "seen", Value: false}})
		}
	}
	assertOK(b, serveCommand(b, s, 1, bson.D{{Key: "insert", Value: "bench"}, {Key: "documents", Value: seed}, {Key: "$db", Value: "app"}}))
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
	b.ResetTimer()
	started := time.Now()
	for i, command := range commands {
		response := serveCommand(b, s, int32(i+2), command)
		if ok, yes := bson.Raw(response).Lookup("ok").DoubleOK(); !yes || ok != 1 {
			b.Fatalf("update response=%s", response)
		}
	}
	elapsed := time.Since(started)
	b.StopTimer()
	docs := b.N * batchSize
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
	b.ReportMetric(float64(docs)*float64(time.Second)/float64(elapsed), "docs/s")
	b.ReportMetric(float64(batchSize), "docs/op")
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
