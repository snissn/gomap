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
// construction does not hide gateway execution work.
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

func benchmarkMongoInsertCommand(op, first, count int) bson.D {
	documents := make(bson.A, 0, count)
	for item := first; item < first+count; item++ {
		documents = append(documents, bson.D{{Key: "_id", Value: fmt.Sprintf("bench-%d-%d", op, item)}, {Key: "value", Value: int32(item)}})
	}
	return bson.D{{Key: "insert", Value: "bench"}, {Key: "documents", Value: documents}, {Key: "$db", Value: "app"}}
}
