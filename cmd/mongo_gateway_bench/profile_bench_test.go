package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func BenchmarkTreeDBGatewayLoadBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoad(b, collections.DocumentFormatBSON, 2, false)
}

func BenchmarkTreeDBGatewayLoadGeneratedIDBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoadWithDocument(b, collections.DocumentFormatBSON, 2, false, benchmarkDocumentWithoutID)
}

func BenchmarkTreeDBGatewayLoadObjectIDBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoadWithDocument(b, collections.DocumentFormatBSON, 2, false, benchmarkDocumentWithObjectID)
}

func BenchmarkTreeDBGatewayLoadUnackBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoadWithDocument(b, collections.DocumentFormatBSON, 2, false, benchmarkDocument,
		options.Collection().SetWriteConcern(writeconcern.Unacknowledged()))
}

func BenchmarkTreeDBGatewayLoadRawBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayLoad(b, collections.DocumentFormatBSON, 2, true)
}

func BenchmarkTreeDBGatewayRunCommandLoadBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayRunCommandLoad(b, collections.DocumentFormatBSON, 2)
}

func BenchmarkTreeDBGatewayRunRawCommandLoadBSONIndexes2(b *testing.B) {
	benchmarkTreeDBGatewayRunRawCommandLoad(b, collections.DocumentFormatBSON, 2)
}

func BenchmarkTreeDBGatewayRawWireLoadBSONIndexes2(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "treedb")
	opts := treedb.OptionsFor(treedb.ProfileWALOnFast, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			b.Fatalf("close backend: %v", err)
		}
	}()
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "bench.docs",
		Options: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormatBSON,
			DataRootStoragePolicy:   collections.RootStorageCompressed,
			IndexStateStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench.docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "email_1",
		Field:         "email",
		Unique:        true,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create email index: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "city_1",
		Field:         "city",
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create city index: %v", err)
	}
	server := mongogateway.NewServer()
	server.Collections = manager
	server.MaxFindScanDocuments = b.N
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat:          collections.DocumentFormatBSON,
		DataRootStoragePolicy:   collections.RootStorageCompressed,
		IndexStateStoragePolicy: collections.RootStorageCompressed,
	}
	server.DefaultIndexStoragePolicy = collections.RootStorageCompressed
	commandDoc := mustProfileBenchDocument(b, bson.D{
		{Key: "insert", Value: "docs"},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: "bench"},
	})

	batchSize := profileBenchBatchSize(b)
	b.ReportAllocs()
	var timedElapsed time.Duration
	var requestID int32
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := batchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make([]wire.Document, count)
		for i := 0; i < count; i++ {
			raw, err := bson.Marshal(benchmarkDocument(inserted + i))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			docs[i] = wire.Document(raw)
		}
		requestID++
		b.StartTimer()
		batchStart := time.Now()
		msg, err := wire.AppendMsgMessageWithSequences(nil, requestID, 0, 0, commandDoc, []wire.DocumentSequence{{
			Identifier: "documents",
			Documents:  docs,
		}})
		if err != nil {
			b.Fatalf("append raw wire insert message: %v", err)
		}
		rw := profileBenchReadWriter{r: bytes.NewReader(msg)}
		if err := server.ServeOneWithOwner(&rw, 1); err != nil {
			b.Fatalf("serve raw wire insert: %v", err)
		}
		if err := assertRawWireInsertOK(rw.w.Bytes(), count); err != nil {
			b.Fatalf("raw wire insert response: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkTreeDBGatewayRawWireTCPLoadBSONIndexes2(b *testing.B) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("/private/tmp", "mongo-gateway-profile-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatalf("remove temp dir: %v", err)
		}
	}()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            2,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        collections.DocumentFormatBSON,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection)
	if err := createIndexes(ctx, coll, cfg.SecondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}
	commandDoc := mustProfileBenchDocument(b, bson.D{
		{Key: "insert", Value: cfg.Collection},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: cfg.Database},
	})
	conn, err := net.Dial("tcp", target.mongoAddr)
	if err != nil {
		b.Fatalf("dial raw-wire tcp: %v", err)
	}
	defer conn.Close()

	batchSize := profileBenchBatchSize(b)
	b.ReportAllocs()
	var timedElapsed time.Duration
	var requestID int32
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := batchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make([]wire.Document, count)
		for i := 0; i < count; i++ {
			raw, err := bson.Marshal(benchmarkDocument(inserted + i))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			docs[i] = wire.Document(raw)
		}
		requestID++
		b.StartTimer()
		batchStart := time.Now()
		if err := serveRawWireTCPInsert(conn, requestID, commandDoc, docs); err != nil {
			b.Fatalf("serve raw wire tcp insert: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkDirectCollectionLoadBSONIndexes2(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "treedb")
	opts := treedb.OptionsFor(treedb.ProfileWALOnFast, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			b.Fatalf("close backend: %v", err)
		}
	}()
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "bench.docs",
		Options: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormatBSON,
			DataRootStoragePolicy:   collections.RootStorageCompressed,
			IndexStateStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench.docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "email_1",
		Field:         "email",
		Unique:        true,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create email index: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "city_1",
		Field:         "city",
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create city index: %v", err)
	}

	batchSize := profileBenchBatchSize(b)
	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := batchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		ids := make([][]byte, count)
		docs := make([][]byte, count)
		for i := 0; i < count; i++ {
			docNum := inserted + i
			ids[i] = []byte(benchmarkID(docNum))
			raw, err := bson.Marshal(benchmarkDocument(docNum))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			docs[i] = raw
		}
		b.StartTimer()
		batchStart := time.Now()
		if _, err := collection.InsertBatchValidatedBSON(ids, docs); err != nil {
			b.Fatalf("insert batch: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush collections: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint backend: %v", err)
	}
	reportDocsPerSecond(b, b.N, timedElapsed)
}

type profileBenchReadWriter struct {
	r *bytes.Reader
	w bytes.Buffer
}

func (rw *profileBenchReadWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *profileBenchReadWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

func mustProfileBenchDocument(tb testing.TB, doc bson.D) wire.Document {
	tb.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal profile benchmark document: %v", err)
	}
	return wire.Document(raw)
}

func BenchmarkClientBSONBatchEncode(b *testing.B) {
	docs := make([]bson.D, profileBenchBatchSize(b))
	for i := range docs {
		docs[i] = benchmarkDocument(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bson.Marshal(docs[i%len(docs)]); err != nil {
			b.Fatalf("marshal BSON document: %v", err)
		}
	}
}

func benchmarkTreeDBGatewayLoad(b *testing.B, format collections.DocumentFormat, secondaryIndexes int, rawDocs bool) {
	benchmarkTreeDBGatewayLoadWithDocument(b, format, secondaryIndexes, rawDocs, benchmarkDocument)
}

func benchmarkTreeDBGatewayLoadWithDocument(b *testing.B, format collections.DocumentFormat, secondaryIndexes int, rawDocs bool, document func(int) bson.D, collectionOptions ...options.Lister[options.CollectionOptions]) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("/private/tmp", "mongo-gateway-profile-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatalf("remove temp dir: %v", err)
		}
	}()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            secondaryIndexes,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        format,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection, collectionOptions...)
	if err := createIndexes(ctx, coll, secondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}

	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := cfg.BatchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make([]any, 0, count)
		for i := 0; i < count; i++ {
			doc := document(inserted + i)
			if rawDocs {
				raw, err := bson.Marshal(doc)
				if err != nil {
					b.Fatalf("marshal BSON document: %v", err)
				}
				docs = append(docs, bson.Raw(raw))
			} else {
				docs = append(docs, doc)
			}
		}
		b.StartTimer()
		batchStart := time.Now()
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			b.Fatalf("insert many: %v", err)
		}
		timedElapsed += time.Since(batchStart)
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func benchmarkDocumentWithoutID(i int) bson.D {
	doc := benchmarkDocument(i)
	return doc[1:]
}

func benchmarkDocumentWithObjectID(i int) bson.D {
	doc := benchmarkDocument(i)
	doc[0].Value = benchmarkObjectID(i)
	return doc
}

func benchmarkObjectID(i int) bson.ObjectID {
	var id bson.ObjectID
	binary.BigEndian.PutUint32(id[0:4], 1)
	binary.BigEndian.PutUint64(id[4:12], uint64(i))
	return id
}

func benchmarkTreeDBGatewayRunCommandLoad(b *testing.B, format collections.DocumentFormat, secondaryIndexes int) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("/private/tmp", "mongo-gateway-profile-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatalf("remove temp dir: %v", err)
		}
	}()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            secondaryIndexes,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        format,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection)
	if err := createIndexes(ctx, coll, secondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}
	db := target.client.Database(cfg.Database)

	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := cfg.BatchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		docs := make(bson.A, 0, count)
		for i := 0; i < count; i++ {
			docs = append(docs, benchmarkDocument(inserted+i))
		}
		b.StartTimer()
		batchStart := time.Now()
		err := db.RunCommand(ctx, bson.D{
			{Key: "insert", Value: cfg.Collection},
			{Key: "documents", Value: docs},
			{Key: "ordered", Value: true},
		}).Err()
		timedElapsed += time.Since(batchStart)
		if err != nil {
			b.Fatalf("run insert command: %v", err)
		}
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func benchmarkTreeDBGatewayRunRawCommandLoad(b *testing.B, format collections.DocumentFormat, secondaryIndexes int) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("/private/tmp", "mongo-gateway-profile-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatalf("remove temp dir: %v", err)
		}
	}()
	cfg := config{
		Target:                      "treedb",
		TreeDBDir:                   dir,
		Database:                    "bench",
		Collection:                  "docs",
		BatchSize:                   profileBenchBatchSize(b),
		SecondaryIndexes:            secondaryIndexes,
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        format,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceNone,
		Timeout:                     0,
	}
	target, err := openTreeDBTarget(ctx, cfg)
	if err != nil {
		b.Fatalf("open target: %v", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			b.Fatalf("close target: %v", err)
		}
	}()
	coll := target.client.Database(cfg.Database).Collection(cfg.Collection)
	if err := createIndexes(ctx, coll, secondaryIndexes); err != nil {
		b.Fatalf("create indexes: %v", err)
	}
	db := target.client.Database(cfg.Database)

	b.ReportAllocs()
	var timedElapsed time.Duration
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		count := cfg.BatchSize
		if remaining := b.N - inserted; remaining < count {
			count = remaining
		}
		rawDocs := make([]bson.Raw, count)
		for i := 0; i < count; i++ {
			raw, err := bson.Marshal(benchmarkDocument(inserted + i))
			if err != nil {
				b.Fatalf("marshal BSON document: %v", err)
			}
			rawDocs[i] = raw
		}
		command, err := rawInsertCommand(cfg.Collection, 0, count, nil, rawDocs)
		if err != nil {
			b.Fatalf("build raw insert command: %v", err)
		}
		b.StartTimer()
		batchStart := time.Now()
		err = db.RunCommand(ctx, command).Err()
		timedElapsed += time.Since(batchStart)
		if err != nil {
			b.Fatalf("run raw insert command: %v", err)
		}
		inserted += count
	}
	b.StopTimer()
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func profileBenchBatchSize(tb testing.TB) int {
	tb.Helper()
	const defaultBatchSize = 10000
	raw := os.Getenv("MONGO_GATEWAY_PROFILE_BENCH_BATCH_SIZE")
	if raw == "" {
		return defaultBatchSize
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		tb.Fatalf("invalid MONGO_GATEWAY_PROFILE_BENCH_BATCH_SIZE=%q", raw)
	}
	return value
}

func reportDocsPerSecond(b *testing.B, docs int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(docs)/elapsed.Seconds(), "docs/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
}
