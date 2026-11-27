package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
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

func TestMongoGatewayInsertManyDurableUsesSharedJournalReceipt(t *testing.T) {
	const (
		producers  = 8
		numBatches = 24
		batchSize  = 200
	)
	s, err := OpenStandaloneServer(StandaloneOptions{
		Dir:                      t.TempDir(),
		Profile:                  treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if _, err := s.Collections.CreateCollection(&collections.CollectionMeta{
		Name:    "bench.users",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
		Indexes: []collections.IndexDefinition{
			{Name: "age_1", Field: "age", ValueType: collections.IndexValueBSONOrderedV2},
			{Name: "email_1", Field: "email", Unique: true, ValueType: collections.IndexValueBSONOrderedV2},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	frames := make([][]byte, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		docs := make([]wire.Document, batchSize)
		for j := 0; j < batchSize; j++ {
			id := batchIdx*batchSize + j
			raw, err := bson.Marshal(bson.D{
				{Key: "_id", Value: fmt.Sprintf("user-%06d", id)},
				{Key: "email", Value: fmt.Sprintf("user-%06d@example.com", id)},
				{Key: "age", Value: int32(18 + (id % 65))},
			})
			if err != nil {
				t.Fatalf("marshal doc %d: %v", id, err)
			}
			docs[j] = wire.Document(raw)
		}
		body, err := bson.Marshal(bson.D{
			{Key: "insert", Value: "users"},
			{Key: "$db", Value: "bench"},
			{Key: "ordered", Value: false},
			{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}, {Key: "j", Value: true}}},
		})
		if err != nil {
			t.Fatalf("marshal insert cmd: %v", err)
		}
		msg, err := wire.AppendMsgMessageWithSequences(nil, int32(batchIdx+100), 0, 0, wire.Document(body), []wire.DocumentSequence{
			{Identifier: "documents", Documents: docs},
		})
		if err != nil {
			t.Fatalf("AppendMsgMessageWithSequences: %v", err)
		}
		frames[batchIdx] = msg
	}

	parseStat := func(key string) uint64 {
		raw := s.ownerStats()[key]
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("parse stat %q=%q: %v", key, raw, err)
		}
		return v
	}
	syncsBefore := parseStat("treedb.command_wal.group_commit.syncs_total")
	forcedBefore := parseStat("treedb.command_wal.group_commit.forced_total")

	// Deterministically overlap concurrent InsertMany batches at the CommandWAL
	// sync boundary: hold the first physical WAL sync until at least 3 other
	// concurrent batches have appended while it is in flight (bounded by 200ms
	// so a lock-hold regression cannot deadlock). This proves workers release
	// nativeBulkInsertMu before awaiting durable sync and coalesce into a shared
	// group-commit wave even under GOMAXPROCS=1 or slow runners.
	var (
		firstSyncHeld atomic.Bool
		appendsInSync atomic.Int32
		releaseOnce   sync.Once
		releaseSync   = make(chan struct{})
	)
	restoreCut := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		switch e.Point {
		case durabilitycut.BeforeDependencyFileSync:
			if firstSyncHeld.CompareAndSwap(false, true) {
				timer := time.NewTimer(200 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-releaseSync:
				case <-timer.C:
				}
			}
		case durabilitycut.AfterDependencyAppend:
			if firstSyncHeld.Load() && appendsInSync.Add(1) >= 3 {
				releaseOnce.Do(func() { close(releaseSync) })
			}
		}
		return nil
	}))
	defer restoreCut()

	batchCh := make(chan int, numBatches)
	for i := 0; i < numBatches; i++ {
		batchCh <- i
	}
	close(batchCh)
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			<-start
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
				header, replyBody, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
				if err != nil {
					errCh <- fmt.Errorf("batch %d read wire reply: %w", batchIdx, err)
					return
				}
				if header.OpCode != wire.OpMsg {
					errCh <- fmt.Errorf("batch %d unexpected reply opcode=%d", batchIdx, header.OpCode)
					return
				}
				msg, err := wire.ParseMsg(replyBody)
				if err != nil {
					errCh <- fmt.Errorf("batch %d parse OP_MSG reply: %w", batchIdx, err)
					return
				}
				raw := bson.Raw(msg.Body)
				if !mongoCommandResponseOK(msg.Body) {
					errCh <- fmt.Errorf("batch %d reply not ok: %s", batchIdx, raw)
					return
				}
				if n, ok := raw.Lookup("n").Int32OK(); !ok || n != batchSize {
					errCh <- fmt.Errorf("batch %d reply n=%d (ok=%t), want %d: %s", batchIdx, n, ok, batchSize, raw)
					return
				}
				if raw.Lookup("writeErrors").Type != 0 {
					errCh <- fmt.Errorf("batch %d unexpected writeErrors: %s", batchIdx, raw)
					return
				}
				if raw.Lookup("writeConcernError").Type != 0 {
					errCh <- fmt.Errorf("batch %d unexpected writeConcernError: %s", batchIdx, raw)
					return
				}
			}
		}(p)
	}
	close(start)
	wg.Wait()
	restoreCut()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("durable insert wire batch: %v", err)
		}
	}

	syncDelta := parseStat("treedb.command_wal.group_commit.syncs_total") - syncsBefore
	forcedDelta := parseStat("treedb.command_wal.group_commit.forced_total") - forcedBefore
	maxGroup := parseStat("treedb.command_wal.group_commit.group_size_max")
	wcStats := s.Server.StandaloneWriteConcernStats()
	if forcedDelta != 0 {
		t.Fatalf("expected multi-doc InsertMany w1_j to use shared journal receipt with 0 forced syncs, got forced=%d", forcedDelta)
	}
	if syncDelta >= numBatches || maxGroup < 2 {
		t.Fatalf("expected concurrent durable gateway InsertMany to group-commit: batches=%d syncs=%d max_group=%d", numBatches, syncDelta, maxGroup)
	}
	if wcStats.JournalAcknowledgements != numBatches || wcStats.SyncFailures != 0 || wcStats.UncertainOutcomes != 0 {
		t.Fatalf("unexpected writeConcern stats: %+v", wcStats)
	}
}
