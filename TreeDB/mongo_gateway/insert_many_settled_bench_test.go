package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestPrepareInsertDocumentsFastPathParity(t *testing.T) {
	makeDoc := func(d bson.D) wire.Document {
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return wire.Document(raw)
	}

	// Mix first-element _id, non-first-element _id, and missing _id.
	docs := []wire.Document{
		makeDoc(bson.D{{Key: "_id", Value: "user-000001"}, {Key: "age", Value: int32(21)}}),
		makeDoc(bson.D{{Key: "name", Value: "bob"}, {Key: "_id", Value: int64(42)}}),
		makeDoc(bson.D{{Key: "name", Value: "auto-id"}}),
		makeDoc(bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "city", Value: "austin"}}),
	}

	ids, stored, err := prepareInsertDocuments(docs, collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepareInsertDocuments: %v", err)
	}
	if len(ids) != len(docs) || len(stored) != len(docs) {
		t.Fatalf("len mismatch: ids=%d stored=%d want %d", len(ids), len(stored), len(docs))
	}
	for i := range docs {
		if i == 2 {
			// Missing _id auto-generated an ObjectID.
			if len(ids[i]) != 2+12 || ids[i][0] != primaryKeyPrefixBSONValue || ids[i][1] != byte(bson.TypeObjectID) {
				t.Fatalf("auto-generated id[%d]=%x unexpected", i, ids[i])
			}
			continue
		}
		wantKey, wantDoc, err := prepareInsertDocument(docs[i], collections.DocumentFormatBSON)
		if err != nil {
			t.Fatalf("reference prepareInsertDocument(%d): %v", i, err)
		}
		if !bytes.Equal(ids[i], wantKey) {
			t.Fatalf("id[%d]=%x want %x", i, ids[i], wantKey)
		}
		if !bytes.Equal(stored[i], wantDoc) {
			t.Fatalf("stored[%d] mismatch", i)
		}
	}

	// Array _id must be rejected.
	badDocs := []wire.Document{
		makeDoc(bson.D{{Key: "_id", Value: "ok"}}),
		makeDoc(bson.D{{Key: "_id", Value: bson.A{1, 2}}}),
	}
	if _, _, err := prepareInsertDocuments(badDocs, collections.DocumentFormatBSON); err == nil {
		t.Fatal("expected error for array _id")
	}
}

func BenchmarkMongoGatewayInsertMany10kBSONSettled(b *testing.B) {
	const (
		totalDocs = 100000
		batchSize = 10000
		producers = 8
	)
	cities := []string{"austin", "boston", "chicago", "denver", "miami", "seattle", "newyork", "atlanta"}
	numBatches := totalDocs / batchSize
	batchDocs := make([][]wire.Document, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		docs := make([]wire.Document, batchSize)
		for j := 0; j < batchSize; j++ {
			id := batchIdx*batchSize + j
			raw, err := bson.Marshal(bson.D{
				{Key: "_id", Value: fmt.Sprintf("user-%06d", id)},
				{Key: "email", Value: fmt.Sprintf("user-%06d@example.com", id)},
				{Key: "city", Value: cities[id%len(cities)]},
				{Key: "age", Value: int32(18 + (id % 65))},
				{Key: "active", Value: id%2 == 0},
				{Key: "score", Value: float64(id%1000) + 0.5},
				{Key: "bio", Value: "benchmark user profile payload with steady length"},
			})
			if err != nil {
				b.Fatalf("marshal doc %d: %v", id, err)
			}
			docs[j] = wire.Document(raw)
		}
		batchDocs[batchIdx] = docs
	}
	// Pre-encode OP_MSG frames for each 10k batch.
	frames := make([][]byte, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		body, err := bson.Marshal(bson.D{
			{Key: "insert", Value: "users"},
			{Key: "$db", Value: "bench"},
			{Key: "ordered", Value: false},
		})
		if err != nil {
			b.Fatalf("marshal insert cmd: %v", err)
		}
		msg, err := wire.AppendMsgMessageWithSequences(nil, int32(batchIdx+100), 0, 0, wire.Document(body), []wire.DocumentSequence{
			{Identifier: "documents", Documents: batchDocs[batchIdx]},
		})
		if err != nil {
			b.Fatalf("AppendMsgBodyWithSequences: %v", err)
		}
		frames[batchIdx] = msg
	}

	for _, idx := range []int{0, 1, 2} {
		b.Run(fmt.Sprintf("idx=%d", idx), func(b *testing.B) {
			b.ReportAllocs()
			var totalInsertNs, totalCheckpointNs int64
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				s, err := OpenStandaloneServer(StandaloneOptions{
					Dir:                      b.TempDir(),
					Profile:                  treedb.ProfileCommandWALRelaxed,
					DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
				})
				if err != nil {
					b.Fatalf("OpenStandaloneServer: %v", err)
				}
				// Create collection & indexes matching idx=0/1/2 + age_1 range index.
				defs := []collections.IndexDefinition{
					{Name: "age_1", Field: "age", ValueType: collections.IndexValueBSONOrderedV2},
				}
				if idx >= 1 {
					defs = append(defs, collections.IndexDefinition{Name: "email_1", Field: "email", Unique: true, ValueType: collections.IndexValueBSONOrderedV2})
				}
				if idx >= 2 {
					defs = append(defs, collections.IndexDefinition{Name: "city_1", Field: "city", ValueType: collections.IndexValueBSONOrderedV2})
				}
				if _, err := s.Collections.CreateCollection(&collections.CollectionMeta{
					Name:    "bench.users",
					Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
					Indexes: defs,
				}); err != nil {
					_ = s.Close()
					b.Fatalf("CreateCollection: %v", err)
				}
				b.StartTimer()

				t0 := time.Now()
				batchCh := make(chan int, numBatches)
				for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
					batchCh <- batchIdx
				}
				close(batchCh)
				var wg sync.WaitGroup
				errCh := make(chan error, producers)
				for p := 0; p < producers; p++ {
					wg.Add(1)
					go func(pid int) {
						defer wg.Done()
						var input bytes.Reader
						rw := &readWriter{r: &input}
						var readBuf, writeBuf []byte
						for batchIdx := range batchCh {
							input.Reset(frames[batchIdx])
							rw.w.Reset()
							var err error
							readBuf, writeBuf, err = s.Server.serveOneWithOwner(context.Background(), rw, int64(pid+1), readBuf, writeBuf)
							if err != nil {
								errCh <- err
								return
							}
						}
					}(p)
				}
				wg.Wait()
				close(errCh)
				for err := range errCh {
					if err != nil {
						b.Fatalf("insert wire batch: %v", err)
					}
				}
				t1 := time.Now()
				if err := s.Collections.FlushAll(); err != nil {
					b.Fatalf("FlushAll: %v", err)
				}
				if err := s.Collections.CheckpointStorage(); err != nil {
					b.Fatalf("CheckpointStorage: %v", err)
				}
				t2 := time.Now()

				b.StopTimer()
				totalInsertNs += t1.Sub(t0).Nanoseconds()
				totalCheckpointNs += t2.Sub(t1).Nanoseconds()
				if err := s.Close(); err != nil {
					b.Fatalf("Close: %v", err)
				}
			}
			totalNs := totalInsertNs + totalCheckpointNs
			if totalNs > 0 {
				docsPerSec := float64(totalDocs*b.N) / (float64(totalNs) / 1e9)
				b.ReportMetric(docsPerSec, "docs/s")
			}
			b.ReportMetric(float64(totalInsertNs)/float64(b.N)/1e6, "insert_ms/op")
			b.ReportMetric(float64(totalCheckpointNs)/float64(b.N)/1e6, "checkpoint_ms/op")
		})
	}
}
