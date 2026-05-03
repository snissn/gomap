package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

var (
	errUpdatedPrimaryKey      = errors.New("profile benchmark update changed _id")
	errProfileBenchUpdateMiss = errors.New("profile benchmark update missed document")
)

const (
	// Keep update payloads bounded so the benchmark stresses collection update
	// concurrency instead of BSON allocation variety.
	profileBenchUpdateDocPoolSize = 4096
	// Preferred deterministic stride for scrambling sequential operation
	// numbers across the preloaded ID set.
	profileBenchPreferredUpdateIDStride = 37
)

func profileBenchUpdateIDStride(documentCount int) int {
	if documentCount <= 1 {
		return 1
	}
	stride := profileBenchPreferredUpdateIDStride
	if stride >= documentCount {
		stride %= documentCount
	}
	if stride <= 0 {
		stride = 1
	}
	for profileBenchGCD(stride, documentCount) != 1 {
		stride++
		if stride >= documentCount {
			stride = 1
		}
	}
	return stride
}

func profileBenchGCD(a, b int) int {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func TestProfileBenchUpdateIDStrideCoversDocumentSet(t *testing.T) {
	for _, documentCount := range []int{1, 2, 37, 74, 1000} {
		stride := profileBenchUpdateIDStride(documentCount)
		seen := make(map[int]struct{}, documentCount)
		for op := 0; op < documentCount; op++ {
			seen[(op*stride)%documentCount] = struct{}{}
		}
		if len(seen) != documentCount {
			t.Fatalf("documentCount=%d stride=%d covered=%d", documentCount, stride, len(seen))
		}
	}
}

func TestProfileBenchBufferedIndexedAsyncFlushEnv(t *testing.T) {
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_DOCUMENTS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_BYTES", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_ROOT_RUNS", "")
	if profileBenchBufferedIndexedAsyncFlush(t) {
		t.Fatal("async flush default=true want false")
	}
	if got := profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(t); got != 0 {
		t.Fatalf("async max queued units default=%d want 0", got)
	}
	if got := profileBenchBufferedIndexedWriteMaxDocuments(t); got != 0 {
		t.Fatalf("buffered max docs default=%d want 0", got)
	}
	if got := profileBenchBufferedIndexedWriteMaxBytes(t); got != 0 {
		t.Fatalf("buffered max bytes default=%d want 0", got)
	}
	if got := profileBenchBufferedIndexedWriteMaxRootRuns(t); got != 0 {
		t.Fatalf("buffered max root runs default=%d want 0", got)
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH", "true")
	if !profileBenchBufferedIndexedAsyncFlush(t) {
		t.Fatal("async flush env=true want true")
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS", "6")
	if got := profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(t); got != 6 {
		t.Fatalf("async max queued units=%d want 6", got)
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_DOCUMENTS", "123")
	if got := profileBenchBufferedIndexedWriteMaxDocuments(t); got != 123 {
		t.Fatalf("buffered max docs=%d want 123", got)
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_BYTES", "1099511627776")
	if got := profileBenchBufferedIndexedWriteMaxBytes(t); got != 1099511627776 {
		t.Fatalf("buffered max bytes=%d want 1099511627776", got)
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_ROOT_RUNS", "789")
	if got := profileBenchBufferedIndexedWriteMaxRootRuns(t); got != 789 {
		t.Fatalf("buffered max root runs=%d want 789", got)
	}
}

func TestProfileBenchParsedUpdateDocsUsesDistinctCityPhases(t *testing.T) {
	warmup := profileBenchParsedUpdateDocs(t, true, "warmup")
	timed := profileBenchParsedUpdateDocs(t, true, "timed")
	if len(warmup) == 0 || len(timed) == 0 {
		t.Fatal("expected parsed update docs")
	}
	warmupCity := profileBenchSetUpdateFieldString(t, warmup[0], "city")
	timedCity := profileBenchSetUpdateFieldString(t, timed[0], "city")
	if warmupCity == timedCity {
		t.Fatalf("city update phases both produced %q; timed city updates must differ from warmup", warmupCity)
	}
}

func TestProfileBenchParsedUpdateDocsChangesCityAcrossFullSweep(t *testing.T) {
	updateDocs := profileBenchParsedUpdateDocs(t, true, "timed")
	if len(updateDocs) == 0 {
		t.Fatal("expected parsed update docs")
	}
	documentCount := profileBenchUpdateDocPoolSize
	idStride := profileBenchUpdateIDStride(documentCount)
	operation := 7
	documentOrdinal := (operation * idStride) % documentCount
	nextOperation := operation + documentCount
	nextDocumentOrdinal := (nextOperation * idStride) % documentCount
	if nextDocumentOrdinal != documentOrdinal {
		t.Fatalf("test setup document ordinals differ: %d vs %d", documentOrdinal, nextDocumentOrdinal)
	}
	firstCity := profileBenchSetUpdateFieldStringAt(t, updateDocs[operation%len(updateDocs)], "city", operation, documentOrdinal, documentCount)
	nextCity := profileBenchSetUpdateFieldStringAt(t, updateDocs[nextOperation%len(updateDocs)], "city", nextOperation, nextDocumentOrdinal, documentCount)
	if firstCity == nextCity {
		t.Fatalf("city repeated across full sweep: %q", firstCity)
	}
}

func profileBenchSetUpdateFieldString(t *testing.T, update profileBenchSetUpdate, key string) string {
	t.Helper()
	return profileBenchSetUpdateFieldStringAt(t, update, key, -1, 0, 0)
}

func profileBenchSetUpdateFieldStringAt(t *testing.T, update profileBenchSetUpdate, key string, operation, documentOrdinal, documentCount int) string {
	t.Helper()
	for _, field := range update.fields {
		if field.key != key {
			continue
		}
		value := profileBenchSetFieldValueForOperation(field, operation, documentOrdinal, documentCount)
		got, ok := value.StringValueOK()
		if !ok {
			t.Fatalf("field %q raw value=%v is not a string", key, value.Type)
		}
		return got
	}
	t.Fatalf("missing update field %q in %+v", key, update.fields)
	return ""
}

func TestProfileBenchDeltaUintStat(t *testing.T) {
	before := map[string]string{"x": "10"}
	after := map[string]string{"x": "17", "bad": "not-a-number"}
	if got := profileBenchDeltaUintStat(after, before, "x"); got != 7 {
		t.Fatalf("delta x=%d want 7", got)
	}
	if got := profileBenchDeltaUintStat(after, before, "missing"); got != 0 {
		t.Fatalf("delta missing=%d want 0", got)
	}
	if got := profileBenchDeltaUintStat(after, before, "bad"); got != 0 {
		t.Fatalf("delta bad=%d want 0", got)
	}
	if got := profileBenchDeltaUintStat(map[string]string{"x": "9"}, before, "x"); got != 0 {
		t.Fatalf("delta underflow=%d want 0", got)
	}
	if got := profileBenchSignedDeltaUintStat(after, before, "x"); got != 7 {
		t.Fatalf("signed delta x=%d want 7", got)
	}
	if got := profileBenchSignedDeltaUintStat(map[string]string{"x": "9"}, before, "x"); got != -1 {
		t.Fatalf("signed delta underflow=%d want -1", got)
	}
	if got := profileBenchUintStat(after, "x"); got != 17 {
		t.Fatalf("value x=%d want 17", got)
	}
	if got := profileBenchUintStat(after, "bad"); got != 0 {
		t.Fatalf("value bad=%d want 0", got)
	}
}

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
		Name:    "bench.docs",
		Options: profileBenchCollectionOptions(b, collections.DocumentFormatBSON),
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
		ValueType:     collections.IndexValueString,
		Unique:        true,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create email index: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "city_1",
		Field:         "city",
		ValueType:     collections.IndexValueString,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create city index: %v", err)
	}
	server := mongogateway.NewServer()
	server.Collections = manager
	server.MaxFindScanDocuments = b.N
	server.DefaultCollectionOptions = profileBenchCollectionOptions(b, collections.DocumentFormatBSON)
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
	flushStart := time.Now()
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush raw-wire collections: %v", err)
	}
	timedElapsed += time.Since(flushStart)
	b.StopTimer()
	reportProfileBenchBufferedIndexedWriteOptions(b, collection.Meta().Options)
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkTreeDBGatewayRawWireTCPLoadBSONIndexes2(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
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
	db := target.client.Database(cfg.Database)
	coll := db.Collection(cfg.Collection)
	if err := createIndexes(ctx, db, coll, cfg.SecondaryIndexes, cfg.RangeIndex, true); err != nil {
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
		Name:    "bench.docs",
		Options: profileBenchCollectionOptions(b, collections.DocumentFormatBSON),
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
		ValueType:     collections.IndexValueString,
		Unique:        true,
		StoragePolicy: collections.RootStorageCompressed,
	}); err != nil {
		b.Fatalf("create email index: %v", err)
	}
	if _, err := collection.CreateIndex(collections.IndexDefinition{
		Name:          "city_1",
		Field:         "city",
		ValueType:     collections.IndexValueString,
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
	flushStart := time.Now()
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush collections: %v", err)
	}
	timedElapsed += time.Since(flushStart)
	b.StopTimer()
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint backend: %v", err)
	}
	reportProfileBenchBufferedIndexedWriteOptions(b, collection.Meta().Options)
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2(b *testing.B) {
	benchmarkDirectCollectionConcurrentUpdateBSON(b, []collections.IndexDefinition{
		{
			Name:          "email_1",
			Field:         "email",
			ValueType:     collections.IndexValueString,
			Unique:        true,
			StoragePolicy: collections.RootStorageCompressed,
		},
		{
			Name:          "city_1",
			Field:         "city",
			ValueType:     collections.IndexValueString,
			StoragePolicy: collections.RootStorageCompressed,
		},
	}, false)
}

func BenchmarkDirectCollectionConcurrentUpdateBSONCityIndex1(b *testing.B) {
	benchmarkDirectCollectionConcurrentUpdateBSON(b, []collections.IndexDefinition{
		{
			Name:          "city_1",
			Field:         "city",
			ValueType:     collections.IndexValueString,
			StoragePolicy: collections.RootStorageCompressed,
		},
	}, true)
}

func BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2CityUpdate(b *testing.B) {
	benchmarkDirectCollectionConcurrentUpdateBSON(b, []collections.IndexDefinition{
		{
			Name:          "email_1",
			Field:         "email",
			ValueType:     collections.IndexValueString,
			Unique:        true,
			StoragePolicy: collections.RootStorageCompressed,
		},
		{
			Name:          "city_1",
			Field:         "city",
			ValueType:     collections.IndexValueString,
			StoragePolicy: collections.RootStorageCompressed,
		},
	}, true)
}

func BenchmarkDirectCollectionConcurrentUpdateBSONIndexes3CityUpdate(b *testing.B) {
	benchmarkDirectCollectionConcurrentUpdateBSON(b, []collections.IndexDefinition{
		{
			Name:          "email_1",
			Field:         "email",
			ValueType:     collections.IndexValueString,
			Unique:        true,
			StoragePolicy: collections.RootStorageCompressed,
		},
		{
			Name:          "city_1",
			Field:         "city",
			ValueType:     collections.IndexValueString,
			StoragePolicy: collections.RootStorageCompressed,
		},
		{
			Name:          "active_1",
			Field:         "active",
			ValueType:     collections.IndexValueBool,
			StoragePolicy: collections.RootStorageCompressed,
		},
	}, true)
}

func benchmarkDirectCollectionConcurrentUpdateBSON(b *testing.B, indexes []collections.IndexDefinition, updateCity bool) {
	b.Helper()
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
		Name:    "bench.docs",
		Options: profileBenchCollectionOptions(b, collections.DocumentFormatBSON),
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench.docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	for _, idx := range indexes {
		if _, err := collection.CreateIndex(idx); err != nil {
			b.Fatalf("create index %s: %v", idx.Name, err)
		}
	}

	documentCount := profileBenchUpdateDocumentCount(b)
	batchSize := profileBenchBatchSize(b)
	for inserted := 0; inserted < documentCount; {
		count := batchSize
		if remaining := documentCount - inserted; remaining < count {
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
		if _, err := collection.InsertBatchValidatedBSON(ids, docs); err != nil {
			b.Fatalf("insert preload batch: %v", err)
		}
		inserted += count
	}
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush preload: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint preload: %v", err)
	}
	warmupUpdateDocs := profileBenchParsedUpdateDocs(b, updateCity, "warmup")
	updateDocs := profileBenchParsedUpdateDocs(b, updateCity, "timed")
	ids := make([][]byte, documentCount)
	for i := range ids {
		ids[i] = []byte(benchmarkID(i))
	}
	idStride := profileBenchUpdateIDStride(documentCount)

	writers := profileBenchConcurrentWriters(b)
	warmupOps := documentCount
	if warmupOps > 100000 {
		warmupOps = 100000
	}
	if err := runProfileBenchDirectCollectionConcurrentUpdates(context.Background(), writers, warmupOps, documentCount, idStride, ids, warmupUpdateDocs, collection); err != nil {
		b.Fatalf("warm up concurrent updates: %v", err)
	}
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush warm up: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint warm up: %v", err)
	}

	b.ReportAllocs()
	manager.SetUpdateBatchDetailedStatsEnabled(true)
	statsBefore := manager.StatsSnapshot()
	backendStatsBefore := backend.Stats()
	b.ResetTimer()
	started := time.Now()
	err = runProfileBenchDirectCollectionConcurrentUpdates(context.Background(), writers, b.N, documentCount, idStride, ids, updateDocs, collection)
	if err == nil {
		// Keep async indexed-flush rows comparable with synchronous rows: the
		// timed update phase includes the final drain of deferred publish work.
		err = manager.FlushAll()
	}
	timedElapsed := time.Since(started)
	b.StopTimer()
	if err != nil {
		b.Fatalf("run concurrent updates: %v", err)
	}
	b.ReportMetric(float64(writers), "writers")
	reportProfileBenchBufferedIndexedWriteOptions(b, collection.Meta().Options)
	reportCollectionManagerUpdateStats(b, deltaCollectionManagerUpdateStats(manager.StatsSnapshot(), statsBefore), b.N)
	backendStatsAfter := backend.Stats()
	reportProfileBenchOrderedRootPublishStats(b, backendStatsAfter, backendStatsBefore, b.N)
	reportProfileBenchBackendVlogMmapStats(b, backendStatsAfter, backendStatsBefore, b.N)
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func profileBenchParsedUpdateDocs(tb testing.TB, updateCity bool, cityPhase string) []profileBenchSetUpdate {
	tb.Helper()
	updateDocs := make([]profileBenchSetUpdate, profileBenchUpdateDocPoolSize)
	var dynamicCityValues []bson.RawValue
	if updateCity {
		dynamicCityValues = profileBenchUpdatedCityRawValues(tb, cityPhase)
	}
	for i := range updateDocs {
		set := bson.D{
			{Key: "concurrent_updated", Value: true},
			{Key: "concurrent_update_seq", Value: int64(i)},
		}
		if updateCity {
			set = append(set, bson.E{Key: "city", Value: cityPhase + "-" + benchmarkUpdatedCity(i, i, profileBenchUpdateDocPoolSize)})
		}
		updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: set}})
		if err != nil {
			tb.Fatalf("marshal update document: %v", err)
		}
		parsed, err := parseProfileBenchSetUpdate(bson.Raw(updateRaw))
		if err != nil {
			tb.Fatalf("parse update document: %v", err)
		}
		if len(dynamicCityValues) > 0 {
			for fieldIdx := range parsed.fields {
				if parsed.fields[fieldIdx].key == "city" {
					parsed.fields[fieldIdx].dynamicCityValues = dynamicCityValues
					break
				}
			}
		}
		updateDocs[i] = parsed
	}
	return updateDocs
}

func profileBenchUpdatedCityRawValues(tb testing.TB, cityPhase string) []bson.RawValue {
	tb.Helper()
	values := make([]bson.RawValue, benchmarkUpdatedCityValueCount)
	for i := range values {
		values[i] = bson.RawValue{
			Type:  bson.TypeString,
			Value: bsoncore.AppendString(nil, cityPhase+"-"+benchmarkUpdatedCityValue(i)),
		}
	}
	return values
}

func profileBenchDeltaUintStat(after, before map[string]string, key string) uint64 {
	afterValue := profileBenchUintStat(after, key)
	if afterValue == 0 {
		return 0
	}
	beforeValue := profileBenchUintStat(before, key)
	if afterValue < beforeValue {
		return 0
	}
	return afterValue - beforeValue
}

func profileBenchUintStat(stats map[string]string, key string) uint64 {
	v, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func profileBenchSignedDeltaUintStat(after, before map[string]string, key string) int64 {
	afterValue := profileBenchUintStat(after, key)
	beforeValue := profileBenchUintStat(before, key)
	if afterValue >= beforeValue {
		delta := afterValue - beforeValue
		if delta > uint64(math.MaxInt64) {
			return math.MaxInt64
		}
		return int64(delta)
	}
	delta := beforeValue - afterValue
	if delta > uint64(math.MaxInt64) {
		return math.MinInt64
	}
	return -int64(delta)
}

func reportProfileBenchBackendVlogMmapStats(b *testing.B, after, before map[string]string, docs int) {
	b.Helper()
	if docs <= 0 {
		return
	}
	reportPerDoc := func(metric, key string) {
		delta := profileBenchDeltaUintStat(after, before, key)
		b.ReportMetric(float64(delta)/float64(docs), metric)
	}
	reportPerDoc("backend_vlog_mmap_hits/doc", "treedb.vlog.mmap_read.hits")
	reportPerDoc("backend_vlog_mmap_miss_out_of_range/doc", "treedb.vlog.mmap_read.miss_out_of_range")
	reportPerDoc("backend_vlog_mmap_miss_no_mapping/doc", "treedb.vlog.mmap_read.miss_no_mapping")
	reportPerDoc("backend_vlog_mmap_miss_dead_mapping_cap/doc", "treedb.vlog.mmap_read.miss_dead_mapping_cap")
	reportPerDoc("backend_vlog_mmap_fallback_readat/doc", "treedb.vlog.mmap_read.fallback_readat")
	reportPerDoc("backend_vlog_mmap_sealed_denied_count_cap/doc", "treedb.vlog.mmap_sealed_map_denied.count_cap")
	reportPerDoc("backend_vlog_mmap_sealed_denied_bytes_cap/doc", "treedb.vlog.mmap_sealed_map_denied.bytes_cap")
	reportPerDoc("backend_tree_get_append_inline_hits/doc", "treedb.process.read_path.backend_tree.get_append_inline_hits_total")
	reportPerDoc("backend_tree_get_append_inline_bytes/doc", "treedb.process.read_path.backend_tree.get_append_inline_bytes_total")
	reportPerDoc("backend_tree_get_append_pointer_hits/doc", "treedb.process.read_path.backend_tree.get_append_pointer_hits_total")
	reportPerDoc("backend_tree_get_append_pointer_bytes/doc", "treedb.process.read_path.backend_tree.get_append_pointer_bytes_total")
	reportPerDoc("backend_outer_leaf_loads/doc", "treedb.process.read_path.outer_leaf.loads_total")
	reportPerDoc("backend_outer_leaf_point_loads/doc", "treedb.process.read_path.outer_leaf.point_loads_total")
	reportPerDoc("backend_outer_leaf_iterator_loads/doc", "treedb.process.read_path.outer_leaf.iterator_loads_total")
	reportPerDoc("backend_outer_leaf_bytes/doc", "treedb.process.read_path.outer_leaf.bytes_total")
	reportPerDoc("backend_outer_leaf_samples/doc", "treedb.process.read_path.outer_leaf.samples_total")
	reportPerDoc("backend_outer_leaf_cache_hits/doc", "treedb.process.read_path.outer_leaf.cache.hits")
	reportPerDoc("backend_outer_leaf_cache_misses/doc", "treedb.process.read_path.outer_leaf.cache.misses")
	reportPerDoc("backend_outer_leaf_cache_stores/doc", "treedb.process.read_path.outer_leaf.cache.stores")
	reportPerDoc("backend_outer_leaf_cache_evictions/doc", "treedb.process.read_path.outer_leaf.cache.evictions")
	reportPerDoc("backend_outer_leaf_cache_read_miss_admission_skips/doc", "treedb.process.read_path.outer_leaf.cache.read_miss_admission_skips")
	reportPerDoc("backend_outer_leaf_cache_read_miss_admission_stores/doc", "treedb.process.read_path.outer_leaf.cache.read_miss_admission_stores")
	reportProfileBenchBackendReadPathRatios(b, after, before)
	hits := profileBenchDeltaUintStat(after, before, "treedb.vlog.mmap_read.hits")
	fallbacks := profileBenchDeltaUintStat(after, before, "treedb.vlog.mmap_read.fallback_readat")
	if total := hits + fallbacks; total > 0 {
		b.ReportMetric(float64(hits)/float64(total), "backend_vlog_mmap_hit_ratio")
	}
	outerLeafCacheHits := profileBenchDeltaUintStat(after, before, "treedb.process.read_path.outer_leaf.cache.hits")
	outerLeafCacheMisses := profileBenchDeltaUintStat(after, before, "treedb.process.read_path.outer_leaf.cache.misses")
	if total := outerLeafCacheHits + outerLeafCacheMisses; total > 0 {
		b.ReportMetric(float64(outerLeafCacheHits)/float64(total), "backend_outer_leaf_cache_hit_ratio")
	}
	deadDelta := profileBenchSignedDeltaUintStat(after, before, "treedb.vlog.mmap_dead_mappings")
	b.ReportMetric(float64(deadDelta), "backend_vlog_mmap_dead_mappings_delta")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_dead_mappings")), "backend_vlog_mmap_dead_mappings")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_sealed_segments")), "backend_vlog_mmap_sealed_segments")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_sealed_bytes")), "backend_vlog_mmap_sealed_bytes")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_active_segments")), "backend_vlog_mmap_active_segments")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_active_bytes")), "backend_vlog_mmap_active_bytes")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.process.read_path.outer_leaf.cache.entries")), "backend_outer_leaf_cache_entries")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.process.read_path.outer_leaf.cache.capacity")), "backend_outer_leaf_cache_capacity")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.process.read_path.outer_leaf.cache.bytes")), "backend_outer_leaf_cache_bytes")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.process.read_path.outer_leaf.sample_mod")), "backend_outer_leaf_sample_mod")
}

func reportProfileBenchBackendReadPathRatios(b *testing.B, after, before map[string]string) {
	b.Helper()
	reportPerHit := func(metric, bytesKey, hitsKey string) {
		bytes := profileBenchDeltaUintStat(after, before, bytesKey)
		hits := profileBenchDeltaUintStat(after, before, hitsKey)
		if hits > 0 {
			b.ReportMetric(float64(bytes)/float64(hits), metric)
		}
	}
	reportPerHit(
		"backend_tree_get_append_inline_bytes/hit",
		"treedb.process.read_path.backend_tree.get_append_inline_bytes_total",
		"treedb.process.read_path.backend_tree.get_append_inline_hits_total",
	)
	reportPerHit(
		"backend_tree_get_append_pointer_bytes/hit",
		"treedb.process.read_path.backend_tree.get_append_pointer_bytes_total",
		"treedb.process.read_path.backend_tree.get_append_pointer_hits_total",
	)
	outerLeafLoads := profileBenchDeltaUintStat(after, before, "treedb.process.read_path.outer_leaf.loads_total")
	outerLeafBytes := profileBenchDeltaUintStat(after, before, "treedb.process.read_path.outer_leaf.bytes_total")
	if outerLeafLoads > 0 {
		b.ReportMetric(float64(outerLeafBytes)/float64(outerLeafLoads), "backend_outer_leaf_bytes/load")
	}
	outerLeafSamples := profileBenchDeltaUintStat(after, before, "treedb.process.read_path.outer_leaf.samples_total")
	if outerLeafSamples == 0 {
		return
	}
	reportPotential := func(metric, key string) {
		hits := profileBenchDeltaUintStat(after, before, key)
		b.ReportMetric(float64(hits)/float64(outerLeafSamples), metric)
	}
	reportPotential("backend_outer_leaf_cache_potential_64_hit_ratio", "treedb.process.read_path.outer_leaf.cache_potential.capacity_64_hits_total")
	reportPotential("backend_outer_leaf_cache_potential_256_hit_ratio", "treedb.process.read_path.outer_leaf.cache_potential.capacity_256_hits_total")
	reportPotential("backend_outer_leaf_cache_potential_1024_hit_ratio", "treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hits_total")
	reportPotential("backend_outer_leaf_cache_potential_4096_hit_ratio", "treedb.process.read_path.outer_leaf.cache_potential.capacity_4096_hits_total")
}

func reportProfileBenchOrderedRootPublishStats(b *testing.B, after, before map[string]string, docs int) {
	b.Helper()
	if docs <= 0 {
		return
	}
	const prefix = "treedb.publish.ordered_root_delta_group."
	calls := profileBenchDeltaUintStat(after, before, prefix+"calls_total")
	if calls == 0 {
		return
	}
	roots := profileBenchDeltaUintStat(after, before, prefix+"roots_total")
	holdNs := profileBenchDeltaUintStat(after, before, prefix+"write_lock_hold_ns_total")
	waitNs := profileBenchDeltaUintStat(after, before, prefix+"write_lock_wait_ns_total")
	preflightNs := profileBenchDeltaUintStat(after, before, prefix+"preflight_ns_total")
	rootApplyNs := profileBenchDeltaUintStat(after, before, prefix+"root_apply_ns_total")
	rootApplyCalls := profileBenchDeltaUintStat(after, before, prefix+"root_apply_calls_total")
	rootApplyOps := profileBenchDeltaUintStat(after, before, prefix+"root_apply_ops_total")
	rootApplyNodeLoads := profileBenchDeltaUintStat(after, before, prefix+"root_apply_node_loads_total")
	rootApplyPagerNodeLoads := profileBenchDeltaUintStat(after, before, prefix+"root_apply_pager_node_loads_total")
	rootApplyLeafLogNodeLoads := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_node_loads_total")
	rootApplyLeafLogCacheHits := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_cache_hits_total")
	rootApplyLeafLogReaderCalls := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_reader_calls_total")
	rootApplyLeafLogViewReads := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_view_reads_total")
	rootApplyLeafLogScratchReads := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_scratch_reads_total")
	rootApplyPagerNodeBytesRead := profileBenchDeltaUintStat(after, before, prefix+"root_apply_pager_node_bytes_read_total")
	rootApplyLeafLogNodeBytesRead := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_node_bytes_read_total")
	rootApplyLeafLogRecordHintBytesRead := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_record_hint_bytes_read_total")
	rootApplyLeafMerges := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_merges_total")
	rootApplyInternalMerges := profileBenchDeltaUintStat(after, before, prefix+"root_apply_internal_merges_total")
	rootApplyLeafPagesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_pages_written_total")
	rootApplyPagerLeafPagesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_pager_leaf_pages_written_total")
	rootApplyLeafLogPagesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_pages_written_total")
	rootApplyLeafPageBytesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_page_bytes_written_total")
	rootApplyPagerLeafPageBytesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_pager_leaf_page_bytes_written_total")
	rootApplyLeafLogPageBytesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_page_bytes_written_total")
	rootApplyLeafLogRecordHintBytesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_leaf_log_record_hint_bytes_written_total")
	rootApplyInternalPagesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_internal_pages_written_total")
	rootApplyInternalPageBytesWritten := profileBenchDeltaUintStat(after, before, prefix+"root_apply_internal_page_bytes_written_total")
	rootApplyInternalChildRefs := profileBenchDeltaUintStat(after, before, prefix+"root_apply_internal_child_refs_total")
	rootApplyInternalPageChildRefs := profileBenchDeltaUintStat(after, before, prefix+"root_apply_internal_page_child_refs_total")
	rootApplyInternalLeafLogRefs := profileBenchDeltaUintStat(after, before, prefix+"root_apply_internal_leaf_log_refs_total")
	rootApplyInternalLeafLogRefCopies := profileBenchDeltaUintStat(after, before, prefix+"root_apply_internal_leaf_log_ref_copies_total")
	rootApplyRootSplitLevels := profileBenchDeltaUintStat(after, before, prefix+"root_apply_root_split_levels_total")
	systemBuildNs := profileBenchDeltaUintStat(after, before, prefix+"system_build_ns_total")
	systemApplyNs := profileBenchDeltaUintStat(after, before, prefix+"system_apply_ns_total")
	systemApplyCalls := profileBenchDeltaUintStat(after, before, prefix+"system_apply_calls_total")
	systemApplyOps := profileBenchDeltaUintStat(after, before, prefix+"system_apply_ops_total")
	systemApplyNodeLoads := profileBenchDeltaUintStat(after, before, prefix+"system_apply_node_loads_total")
	finalizeNs := profileBenchDeltaUintStat(after, before, prefix+"finalize_ns_total")
	finalizeCalls := profileBenchDeltaUintStat(after, before, prefix+"finalize_calls_total")
	b.ReportMetric(float64(calls), "publish_delta_group_calls")
	b.ReportMetric(float64(calls)/float64(docs), "publish_delta_group_calls/doc")
	b.ReportMetric(float64(roots)/float64(calls), "publish_delta_group_roots/call")
	b.ReportMetric(float64(holdNs)/float64(docs), "publish_delta_group_lock_hold_ns/doc")
	b.ReportMetric(float64(waitNs)/float64(docs), "publish_delta_group_lock_wait_ns/doc")
	b.ReportMetric(float64(holdNs)/float64(calls), "publish_delta_group_lock_hold_ns/call")
	b.ReportMetric(float64(waitNs)/float64(calls), "publish_delta_group_lock_wait_ns/call")
	b.ReportMetric(float64(preflightNs)/float64(docs), "publish_delta_group_preflight_ns/doc")
	b.ReportMetric(float64(rootApplyNs)/float64(docs), "publish_delta_group_root_apply_ns/doc")
	b.ReportMetric(float64(rootApplyOps)/float64(docs), "publish_delta_group_root_apply_ops/doc")
	b.ReportMetric(float64(rootApplyNodeLoads)/float64(docs), "publish_delta_group_root_apply_node_loads/doc")
	b.ReportMetric(float64(rootApplyPagerNodeLoads)/float64(docs), "publish_delta_group_root_apply_pager_node_loads/doc")
	b.ReportMetric(float64(rootApplyLeafLogNodeLoads)/float64(docs), "publish_delta_group_root_apply_leaf_log_node_loads/doc")
	b.ReportMetric(float64(rootApplyLeafLogCacheHits)/float64(docs), "publish_delta_group_root_apply_leaf_log_cache_hits/doc")
	b.ReportMetric(float64(rootApplyLeafLogReaderCalls)/float64(docs), "publish_delta_group_root_apply_leaf_log_reader_calls/doc")
	b.ReportMetric(float64(rootApplyLeafLogViewReads)/float64(docs), "publish_delta_group_root_apply_leaf_log_view_reads/doc")
	b.ReportMetric(float64(rootApplyLeafLogScratchReads)/float64(docs), "publish_delta_group_root_apply_leaf_log_scratch_reads/doc")
	b.ReportMetric(float64(rootApplyPagerNodeBytesRead)/float64(docs), "publish_delta_group_root_apply_pager_node_read_bytes/doc")
	b.ReportMetric(float64(rootApplyLeafLogNodeBytesRead)/float64(docs), "publish_delta_group_root_apply_leaf_log_node_read_bytes/doc")
	b.ReportMetric(float64(rootApplyLeafLogRecordHintBytesRead)/float64(docs), "publish_delta_group_root_apply_leaf_log_record_hint_read_bytes/doc")
	b.ReportMetric(float64(rootApplyLeafMerges)/float64(docs), "publish_delta_group_root_apply_leaf_merges/doc")
	b.ReportMetric(float64(rootApplyInternalMerges)/float64(docs), "publish_delta_group_root_apply_internal_merges/doc")
	b.ReportMetric(float64(rootApplyLeafPagesWritten)/float64(docs), "publish_delta_group_root_apply_leaf_pages_written/doc")
	b.ReportMetric(float64(rootApplyPagerLeafPagesWritten)/float64(docs), "publish_delta_group_root_apply_pager_leaf_pages_written/doc")
	b.ReportMetric(float64(rootApplyLeafLogPagesWritten)/float64(docs), "publish_delta_group_root_apply_leaf_log_pages_written/doc")
	b.ReportMetric(float64(rootApplyLeafPageBytesWritten)/float64(docs), "publish_delta_group_root_apply_leaf_page_write_bytes/doc")
	b.ReportMetric(float64(rootApplyPagerLeafPageBytesWritten)/float64(docs), "publish_delta_group_root_apply_pager_leaf_page_write_bytes/doc")
	b.ReportMetric(float64(rootApplyLeafLogPageBytesWritten)/float64(docs), "publish_delta_group_root_apply_leaf_log_page_write_bytes/doc")
	b.ReportMetric(float64(rootApplyLeafLogRecordHintBytesWritten)/float64(docs), "publish_delta_group_root_apply_leaf_log_record_hint_write_bytes/doc")
	b.ReportMetric(float64(rootApplyInternalPagesWritten)/float64(docs), "publish_delta_group_root_apply_internal_pages_written/doc")
	b.ReportMetric(float64(rootApplyInternalPageBytesWritten)/float64(docs), "publish_delta_group_root_apply_internal_page_write_bytes/doc")
	b.ReportMetric(float64(rootApplyInternalChildRefs)/float64(docs), "publish_delta_group_root_apply_internal_child_refs/doc")
	b.ReportMetric(float64(rootApplyInternalPageChildRefs)/float64(docs), "publish_delta_group_root_apply_internal_page_child_refs/doc")
	b.ReportMetric(float64(rootApplyInternalLeafLogRefs)/float64(docs), "publish_delta_group_root_apply_internal_leaf_log_refs/doc")
	b.ReportMetric(float64(rootApplyInternalLeafLogRefCopies)/float64(docs), "publish_delta_group_root_apply_internal_leaf_log_ref_copies/doc")
	b.ReportMetric(float64(rootApplyRootSplitLevels)/float64(docs), "publish_delta_group_root_apply_root_split_levels/doc")
	b.ReportMetric(float64(systemBuildNs)/float64(docs), "publish_delta_group_system_build_ns/doc")
	b.ReportMetric(float64(systemApplyNs)/float64(docs), "publish_delta_group_system_apply_ns/doc")
	b.ReportMetric(float64(systemApplyOps)/float64(docs), "publish_delta_group_system_apply_ops/doc")
	b.ReportMetric(float64(systemApplyNodeLoads)/float64(docs), "publish_delta_group_system_apply_node_loads/doc")
	b.ReportMetric(float64(finalizeNs)/float64(docs), "publish_delta_group_finalize_ns/doc")
	if rootApplyCalls > 0 {
		b.ReportMetric(float64(rootApplyNs)/float64(rootApplyCalls), "publish_delta_group_root_apply_ns/call")
	}
	if rootApplyLeafLogNodeLoads > 0 {
		b.ReportMetric(float64(rootApplyLeafLogRecordHintBytesRead)/float64(rootApplyLeafLogNodeLoads), "publish_delta_group_root_apply_leaf_log_record_hint_read_bytes/load")
	}
	if rootApplyLeafLogPagesWritten > 0 {
		b.ReportMetric(float64(rootApplyLeafLogRecordHintBytesWritten)/float64(rootApplyLeafLogPagesWritten), "publish_delta_group_root_apply_leaf_log_record_hint_write_bytes/page")
	}
	if rootApplyLeafLogPageBytesWritten > 0 {
		b.ReportMetric(float64(rootApplyLeafLogRecordHintBytesWritten)/float64(rootApplyLeafLogPageBytesWritten), "publish_delta_group_root_apply_leaf_log_record_hint_write_ratio")
	}
	if systemApplyCalls > 0 {
		b.ReportMetric(float64(systemApplyNs)/float64(systemApplyCalls), "publish_delta_group_system_apply_ns/call")
	}
	if finalizeCalls > 0 {
		b.ReportMetric(float64(finalizeNs)/float64(finalizeCalls), "publish_delta_group_finalize_ns/call")
	}
}

func deltaCollectionManagerUpdateStats(after, before collections.CollectionManagerStats) collections.CollectionManagerStats {
	delta := collections.CollectionManagerStats{
		UpdateBatchCalls:               after.UpdateBatchCalls - before.UpdateBatchCalls,
		UpdateBatchItems:               after.UpdateBatchItems - before.UpdateBatchItems,
		UpdateBatchMatched:             after.UpdateBatchMatched - before.UpdateBatchMatched,
		UpdateBatchModified:            after.UpdateBatchModified - before.UpdateBatchModified,
		UpdateBatchRuns:                after.UpdateBatchRuns - before.UpdateBatchRuns,
		UpdateBatchBufferedBatches:     after.UpdateBatchBufferedBatches - before.UpdateBatchBufferedBatches,
		UpdateBatchCurrentRead:         after.UpdateBatchCurrentRead - before.UpdateBatchCurrentRead,
		UpdateBatchCallback:            after.UpdateBatchCallback - before.UpdateBatchCallback,
		UpdateBatchPrepareDocuments:    after.UpdateBatchPrepareDocuments - before.UpdateBatchPrepareDocuments,
		UpdateBatchIndexStateExtract:   after.UpdateBatchIndexStateExtract - before.UpdateBatchIndexStateExtract,
		UpdateBatchUniquePreflight:     after.UpdateBatchUniquePreflight - before.UpdateBatchUniquePreflight,
		UpdateBatchTemplateRunBuild:    after.UpdateBatchTemplateRunBuild - before.UpdateBatchTemplateRunBuild,
		UpdateBatchPrimaryRunBuild:     after.UpdateBatchPrimaryRunBuild - before.UpdateBatchPrimaryRunBuild,
		UpdateBatchIndexStateRunBuild:  after.UpdateBatchIndexStateRunBuild - before.UpdateBatchIndexStateRunBuild,
		UpdateBatchSecondaryRunBuild:   after.UpdateBatchSecondaryRunBuild - before.UpdateBatchSecondaryRunBuild,
		UpdateBatchBufferStage:         after.UpdateBatchBufferStage - before.UpdateBatchBufferStage,
		UpdateBatchBufferPrecheck:      after.UpdateBatchBufferPrecheck - before.UpdateBatchBufferPrecheck,
		UpdateBatchBufferLockWait:      after.UpdateBatchBufferLockWait - before.UpdateBatchBufferLockWait,
		UpdateBatchBufferLockHold:      after.UpdateBatchBufferLockHold - before.UpdateBatchBufferLockHold,
		UpdateBatchBufferValidation:    after.UpdateBatchBufferValidation - before.UpdateBatchBufferValidation,
		UpdateBatchBufferRootScan:      after.UpdateBatchBufferRootScan - before.UpdateBatchBufferRootScan,
		UpdateBatchBufferDomainPrepare: after.UpdateBatchBufferDomainPrepare - before.UpdateBatchBufferDomainPrepare,
		UpdateBatchBufferPrimaryIdx:    after.UpdateBatchBufferPrimaryIdx - before.UpdateBatchBufferPrimaryIdx,
		UpdateBatchBufferUniqueIdx:     after.UpdateBatchBufferUniqueIdx - before.UpdateBatchBufferUniqueIdx,
		UpdateBatchBufferRootAppend:    after.UpdateBatchBufferRootAppend - before.UpdateBatchBufferRootAppend,
		UpdateBatchBufferFlush:         after.UpdateBatchBufferFlush - before.UpdateBatchBufferFlush,
		UpdateBatchPublish:             after.UpdateBatchPublish - before.UpdateBatchPublish,
		UpdateBatchSecondaryDeletes:    after.UpdateBatchSecondaryDeletes - before.UpdateBatchSecondaryDeletes,
		UpdateBatchSecondarySets:       after.UpdateBatchSecondarySets - before.UpdateBatchSecondarySets,
		UpdateBatchSecondaryKeyBytes:   after.UpdateBatchSecondaryKeyBytes - before.UpdateBatchSecondaryKeyBytes,
		UpdateBatchIndexValueChanges:   after.UpdateBatchIndexValueChanges - before.UpdateBatchIndexValueChanges,
		UpdateBatchIndexValueUnchanged: after.UpdateBatchIndexValueUnchanged - before.UpdateBatchIndexValueUnchanged,
		UpdateBatchUniqueChecks:        after.UpdateBatchUniqueChecks - before.UpdateBatchUniqueChecks,
		UpdateBatchUniqueCheckSkips:    after.UpdateBatchUniqueCheckSkips - before.UpdateBatchUniqueCheckSkips,
		UpdateCombineRequests:          after.UpdateCombineRequests - before.UpdateCombineRequests,
		UpdateCombineBatches:           after.UpdateCombineBatches - before.UpdateCombineBatches,
		UpdateCombineBatchedRequests:   after.UpdateCombineBatchedRequests - before.UpdateCombineBatchedRequests,
		UpdateCombineFallbackRequests:  after.UpdateCombineFallbackRequests - before.UpdateCombineFallbackRequests,
		IndexedFlushCalls:              after.IndexedFlushCalls - before.IndexedFlushCalls,
		IndexedFlushErrors:             after.IndexedFlushErrors - before.IndexedFlushErrors,
		IndexedFlushDocs:               after.IndexedFlushDocs - before.IndexedFlushDocs,
		IndexedFlushBytes:              after.IndexedFlushBytes - before.IndexedFlushBytes,
		IndexedFlushRootRuns:           after.IndexedFlushRootRuns - before.IndexedFlushRootRuns,
		IndexedFlushRoots:              after.IndexedFlushRoots - before.IndexedFlushRoots,
		IndexedFlushDuration:           after.IndexedFlushDuration - before.IndexedFlushDuration,
	}
	for i := 0; i < after.UpdateBatchIndexStatsCount && i < len(after.UpdateBatchIndexStats); i++ {
		next := after.UpdateBatchIndexStats[i]
		if next.IndexName == "" {
			continue
		}
		if previous, ok := collectionManagerUpdateIndexStat(before, next); ok {
			next.Changed -= previous.Changed
			next.Unchanged -= previous.Unchanged
			next.UniqueChecks -= previous.UniqueChecks
			next.UniqueCheckSkips -= previous.UniqueCheckSkips
			next.SecondaryRuns -= previous.SecondaryRuns
			next.SecondaryDeletes -= previous.SecondaryDeletes
			next.SecondarySets -= previous.SecondarySets
			next.SecondaryKeyBytes -= previous.SecondaryKeyBytes
		}
		if delta.UpdateBatchIndexStatsCount < len(delta.UpdateBatchIndexStats) {
			delta.UpdateBatchIndexStats[delta.UpdateBatchIndexStatsCount] = next
			delta.UpdateBatchIndexStatsCount++
		}
	}
	return delta
}

func collectionManagerUpdateIndexStatByName(stats collections.CollectionManagerStats, name string) (collections.CollectionUpdateIndexStats, bool) {
	for i := 0; i < stats.UpdateBatchIndexStatsCount && i < len(stats.UpdateBatchIndexStats); i++ {
		if stats.UpdateBatchIndexStats[i].IndexName == name {
			return stats.UpdateBatchIndexStats[i], true
		}
	}
	return collections.CollectionUpdateIndexStats{}, false
}

func collectionManagerUpdateIndexStat(stats collections.CollectionManagerStats, target collections.CollectionUpdateIndexStats) (collections.CollectionUpdateIndexStats, bool) {
	for i := 0; i < stats.UpdateBatchIndexStatsCount && i < len(stats.UpdateBatchIndexStats); i++ {
		candidate := stats.UpdateBatchIndexStats[i]
		if candidate.CollectionName == target.CollectionName &&
			candidate.IndexOrdinal == target.IndexOrdinal &&
			candidate.IndexName == target.IndexName {
			return candidate, true
		}
	}
	return collections.CollectionUpdateIndexStats{}, false
}

func TestDeltaCollectionManagerUpdateStatsIncludesCombinerStats(t *testing.T) {
	before := collections.CollectionManagerStats{
		UpdateCombineRequests:          10,
		UpdateCombineBatches:           3,
		UpdateCombineBatchedRequests:   8,
		UpdateCombineFallbackRequests:  1,
		UpdateBatchIndexValueChanges:   20,
		UpdateBatchIndexValueUnchanged: 30,
		UpdateBatchUniqueChecks:        4,
		UpdateBatchUniqueCheckSkips:    10,
		IndexedFlushCalls:              3,
		IndexedFlushErrors:             1,
		IndexedFlushDocs:               300,
		IndexedFlushBytes:              9000,
		IndexedFlushRootRuns:           90,
		IndexedFlushRoots:              9,
		IndexedFlushDuration:           30 * time.Millisecond,
		UpdateBatchIndexStatsCount:     2,
		UpdateBatchIndexStats: [8]collections.CollectionUpdateIndexStats{
			{CollectionName: "users", IndexName: "email", IndexOrdinal: 0, Unique: true, Changed: 1, Unchanged: 9, UniqueChecks: 1, UniqueCheckSkips: 8},
			{CollectionName: "users", IndexName: "city", IndexOrdinal: 1, Changed: 10, SecondaryRuns: 10, SecondaryDeletes: 10, SecondarySets: 10, SecondaryKeyBytes: 1000},
		},
	}
	after := collections.CollectionManagerStats{
		UpdateCombineRequests:          17,
		UpdateCombineBatches:           5,
		UpdateCombineBatchedRequests:   14,
		UpdateCombineFallbackRequests:  2,
		UpdateBatchIndexValueChanges:   27,
		UpdateBatchIndexValueUnchanged: 43,
		UpdateBatchUniqueChecks:        6,
		UpdateBatchUniqueCheckSkips:    19,
		IndexedFlushCalls:              8,
		IndexedFlushErrors:             2,
		IndexedFlushDocs:               900,
		IndexedFlushBytes:              27000,
		IndexedFlushRootRuns:           270,
		IndexedFlushRoots:              24,
		IndexedFlushDuration:           90 * time.Millisecond,
		UpdateBatchIndexStatsCount:     2,
		UpdateBatchIndexStats: [8]collections.CollectionUpdateIndexStats{
			{CollectionName: "users", IndexName: "email", IndexOrdinal: 0, Unique: true, Changed: 1, Unchanged: 22, UniqueChecks: 1, UniqueCheckSkips: 17},
			{CollectionName: "users", IndexName: "city", IndexOrdinal: 1, Changed: 17, SecondaryRuns: 17, SecondaryDeletes: 17, SecondarySets: 17, SecondaryKeyBytes: 1700},
		},
	}
	got := deltaCollectionManagerUpdateStats(after, before)
	if got.UpdateCombineRequests != 7 {
		t.Fatalf("UpdateCombineRequests=%d want 7", got.UpdateCombineRequests)
	}
	if got.UpdateCombineBatches != 2 {
		t.Fatalf("UpdateCombineBatches=%d want 2", got.UpdateCombineBatches)
	}
	if got.UpdateCombineBatchedRequests != 6 {
		t.Fatalf("UpdateCombineBatchedRequests=%d want 6", got.UpdateCombineBatchedRequests)
	}
	if got.UpdateCombineFallbackRequests != 1 {
		t.Fatalf("UpdateCombineFallbackRequests=%d want 1", got.UpdateCombineFallbackRequests)
	}
	if got.UpdateBatchIndexValueChanges != 7 {
		t.Fatalf("UpdateBatchIndexValueChanges=%d want 7", got.UpdateBatchIndexValueChanges)
	}
	if got.UpdateBatchIndexValueUnchanged != 13 {
		t.Fatalf("UpdateBatchIndexValueUnchanged=%d want 13", got.UpdateBatchIndexValueUnchanged)
	}
	if got.UpdateBatchUniqueChecks != 2 {
		t.Fatalf("UpdateBatchUniqueChecks=%d want 2", got.UpdateBatchUniqueChecks)
	}
	if got.UpdateBatchUniqueCheckSkips != 9 {
		t.Fatalf("UpdateBatchUniqueCheckSkips=%d want 9", got.UpdateBatchUniqueCheckSkips)
	}
	if got.IndexedFlushCalls != 5 || got.IndexedFlushErrors != 1 || got.IndexedFlushDocs != 600 || got.IndexedFlushBytes != 18000 || got.IndexedFlushRootRuns != 180 || got.IndexedFlushRoots != 15 || got.IndexedFlushDuration != 60*time.Millisecond {
		t.Fatalf("indexed flush delta calls/errors/docs/bytes/rootRuns/roots/duration=%d/%d/%d/%d/%d/%d/%s want 5/1/600/18000/180/15/60ms",
			got.IndexedFlushCalls,
			got.IndexedFlushErrors,
			got.IndexedFlushDocs,
			got.IndexedFlushBytes,
			got.IndexedFlushRootRuns,
			got.IndexedFlushRoots,
			got.IndexedFlushDuration,
		)
	}
	if got.UpdateBatchIndexStatsCount != 2 {
		t.Fatalf("UpdateBatchIndexStatsCount=%d want 2", got.UpdateBatchIndexStatsCount)
	}
	email, ok := collectionManagerUpdateIndexStatByName(got, "email")
	if !ok {
		t.Fatalf("missing email index stat in %+v", got.UpdateBatchIndexStats)
	}
	if !email.Unique || email.Changed != 0 || email.Unchanged != 13 || email.UniqueChecks != 0 || email.UniqueCheckSkips != 9 {
		t.Fatalf("email delta=%+v want unchanged=13 unique_skips=9", email)
	}
	city, ok := collectionManagerUpdateIndexStatByName(got, "city")
	if !ok {
		t.Fatalf("missing city index stat in %+v", got.UpdateBatchIndexStats)
	}
	if city.Changed != 7 || city.SecondaryRuns != 7 || city.SecondaryDeletes != 7 || city.SecondarySets != 7 || city.SecondaryKeyBytes != 700 {
		t.Fatalf("city delta=%+v want changed/run/delete/set/key-bytes 7/7/7/7/700", city)
	}
}

func reportCollectionManagerUpdateStats(b *testing.B, stats collections.CollectionManagerStats, docs int) {
	b.Helper()
	if docs <= 0 {
		return
	}
	if stats.IndexedFlushCalls > 0 {
		b.ReportMetric(float64(stats.IndexedFlushCalls), "indexed_flush_calls")
		if stats.IndexedFlushDocs > 0 {
			b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(stats.IndexedFlushCalls), "indexed_flush_docs/call")
			b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(docs), "indexed_flush_docs/doc")
		}
		if stats.IndexedFlushBytes > 0 {
			b.ReportMetric(float64(stats.IndexedFlushBytes)/float64(stats.IndexedFlushCalls), "indexed_flush_bytes/call")
			b.ReportMetric(float64(stats.IndexedFlushBytes)/float64(docs), "indexed_flush_bytes/doc")
		}
		if stats.IndexedFlushRootRuns > 0 {
			b.ReportMetric(float64(stats.IndexedFlushRootRuns)/float64(stats.IndexedFlushCalls), "indexed_flush_root_runs/call")
			b.ReportMetric(float64(stats.IndexedFlushRootRuns)/float64(docs), "indexed_flush_root_runs/doc")
		}
		if stats.IndexedFlushRoots > 0 {
			b.ReportMetric(float64(stats.IndexedFlushRoots)/float64(stats.IndexedFlushCalls), "indexed_flush_roots/call")
			b.ReportMetric(float64(stats.IndexedFlushRoots)/float64(docs), "indexed_flush_roots/doc")
		}
		if stats.IndexedFlushErrors > 0 {
			b.ReportMetric(float64(stats.IndexedFlushErrors), "indexed_flush_errors")
		}
		if stats.IndexedFlushDuration > 0 {
			b.ReportMetric(float64(stats.IndexedFlushDuration.Nanoseconds())/float64(stats.IndexedFlushCalls), "indexed_flush_ns/call")
			if stats.IndexedFlushDocs > 0 {
				b.ReportMetric(float64(stats.IndexedFlushDuration.Nanoseconds())/float64(stats.IndexedFlushDocs), "indexed_flush_ns/doc")
			}
		}
	}
	if stats.UpdateBatchCalls > 0 {
		b.ReportMetric(float64(stats.UpdateBatchCalls), "update_batches")
		b.ReportMetric(float64(stats.UpdateBatchItems)/float64(stats.UpdateBatchCalls), "update_items/batch")
	}
	if stats.UpdateBatchRuns > 0 && stats.UpdateBatchCalls > 0 {
		b.ReportMetric(float64(stats.UpdateBatchRuns)/float64(stats.UpdateBatchCalls), "update_roots/batch")
	}
	if stats.UpdateBatchMatched > 0 {
		b.ReportMetric(float64(stats.UpdateBatchMatched)/float64(docs), "update_matched/doc")
	}
	if stats.UpdateBatchModified > 0 {
		b.ReportMetric(float64(stats.UpdateBatchModified)/float64(docs), "update_modified/doc")
	}
	if stats.UpdateBatchBufferedBatches > 0 {
		b.ReportMetric(float64(stats.UpdateBatchBufferedBatches), "update_buffered_batches")
	}
	if stats.UpdateCombineRequests > 0 {
		b.ReportMetric(float64(stats.UpdateCombineRequests), "update_combine_requests")
		b.ReportMetric(float64(stats.UpdateCombineRequests)/float64(docs), "update_combine_requests/doc")
	}
	if stats.UpdateCombineBatches > 0 {
		b.ReportMetric(float64(stats.UpdateCombineBatches), "update_combine_batches")
		b.ReportMetric(float64(stats.UpdateCombineBatchedRequests)/float64(stats.UpdateCombineBatches), "update_combine_items/batch")
	}
	if stats.UpdateCombineBatchedRequests > 0 {
		b.ReportMetric(float64(stats.UpdateCombineBatchedRequests), "update_combine_batched_requests")
		b.ReportMetric(float64(stats.UpdateCombineBatchedRequests)/float64(docs), "update_combine_batched_requests/doc")
	}
	if stats.UpdateCombineFallbackRequests > 0 {
		b.ReportMetric(float64(stats.UpdateCombineFallbackRequests), "update_combine_fallback_requests")
		b.ReportMetric(float64(stats.UpdateCombineFallbackRequests)/float64(docs), "update_combine_fallback_requests/doc")
	}
	if stats.UpdateBatchSecondaryDeletes > 0 {
		b.ReportMetric(float64(stats.UpdateBatchSecondaryDeletes)/float64(docs), "update_secondary_deletes/doc")
	}
	if stats.UpdateBatchSecondarySets > 0 {
		b.ReportMetric(float64(stats.UpdateBatchSecondarySets)/float64(docs), "update_secondary_sets/doc")
	}
	if stats.UpdateBatchSecondaryKeyBytes > 0 {
		b.ReportMetric(float64(stats.UpdateBatchSecondaryKeyBytes)/float64(docs), "update_secondary_key_bytes/doc")
	}
	if stats.UpdateBatchIndexValueChanges > 0 {
		b.ReportMetric(float64(stats.UpdateBatchIndexValueChanges)/float64(docs), "update_index_value_changes/doc")
	}
	if stats.UpdateBatchIndexValueUnchanged > 0 {
		b.ReportMetric(float64(stats.UpdateBatchIndexValueUnchanged)/float64(docs), "update_index_value_unchanged/doc")
	}
	if stats.UpdateBatchUniqueChecks > 0 {
		b.ReportMetric(float64(stats.UpdateBatchUniqueChecks)/float64(docs), "update_unique_checks/doc")
	}
	if stats.UpdateBatchUniqueCheckSkips > 0 {
		b.ReportMetric(float64(stats.UpdateBatchUniqueCheckSkips)/float64(docs), "update_unique_check_skips/doc")
	}
	for i := 0; i < stats.UpdateBatchIndexStatsCount && i < len(stats.UpdateBatchIndexStats); i++ {
		indexStats := stats.UpdateBatchIndexStats[i]
		if indexStats.IndexName == "" {
			continue
		}
		prefix := "update_collection_" + sanitizeProfileName(indexStats.CollectionName) +
			"_index_" + strconv.Itoa(indexStats.IndexOrdinal) + "_" + sanitizeProfileName(indexStats.IndexName) + "_"
		if indexStats.Changed > 0 {
			b.ReportMetric(float64(indexStats.Changed)/float64(docs), prefix+"changed/doc")
		}
		if indexStats.Unchanged > 0 {
			b.ReportMetric(float64(indexStats.Unchanged)/float64(docs), prefix+"unchanged/doc")
		}
		if indexStats.UniqueChecks > 0 {
			b.ReportMetric(float64(indexStats.UniqueChecks)/float64(docs), prefix+"unique_checks/doc")
		}
		if indexStats.UniqueCheckSkips > 0 {
			b.ReportMetric(float64(indexStats.UniqueCheckSkips)/float64(docs), prefix+"unique_check_skips/doc")
		}
		if indexStats.SecondaryRuns > 0 {
			b.ReportMetric(float64(indexStats.SecondaryRuns)/float64(docs), prefix+"secondary_runs/doc")
		}
		if indexStats.SecondaryDeletes > 0 {
			b.ReportMetric(float64(indexStats.SecondaryDeletes)/float64(docs), prefix+"secondary_deletes/doc")
		}
		if indexStats.SecondarySets > 0 {
			b.ReportMetric(float64(indexStats.SecondarySets)/float64(docs), prefix+"secondary_sets/doc")
		}
		if indexStats.SecondaryKeyBytes > 0 {
			b.ReportMetric(float64(indexStats.SecondaryKeyBytes)/float64(docs), prefix+"secondary_key_bytes/doc")
		}
	}
	reportDuration := func(name string, d time.Duration) {
		if d > 0 {
			b.ReportMetric(float64(d.Nanoseconds())/float64(docs), name)
		}
	}
	reportDuration("update_current_read_ns/doc", stats.UpdateBatchCurrentRead)
	reportDuration("update_callback_ns/doc", stats.UpdateBatchCallback)
	reportDuration("update_prepare_ns/doc", stats.UpdateBatchPrepareDocuments)
	reportDuration("update_index_state_extract_ns/doc", stats.UpdateBatchIndexStateExtract)
	reportDuration("update_unique_preflight_ns/doc", stats.UpdateBatchUniquePreflight)
	reportDuration("update_template_run_ns/doc", stats.UpdateBatchTemplateRunBuild)
	reportDuration("update_primary_run_ns/doc", stats.UpdateBatchPrimaryRunBuild)
	reportDuration("update_index_state_run_ns/doc", stats.UpdateBatchIndexStateRunBuild)
	reportDuration("update_secondary_runs_ns/doc", stats.UpdateBatchSecondaryRunBuild)
	reportDuration("update_buffer_stage_ns/doc", stats.UpdateBatchBufferStage)
	reportDuration("update_buffer_precheck_ns/doc", stats.UpdateBatchBufferPrecheck)
	reportDuration("update_buffer_lock_wait_ns/doc", stats.UpdateBatchBufferLockWait)
	reportDuration("update_buffer_lock_hold_ns/doc", stats.UpdateBatchBufferLockHold)
	reportDuration("update_buffer_validation_ns/doc", stats.UpdateBatchBufferValidation)
	reportDuration("update_buffer_root_scan_ns/doc", stats.UpdateBatchBufferRootScan)
	reportDuration("update_buffer_domain_prepare_ns/doc", stats.UpdateBatchBufferDomainPrepare)
	reportDuration("update_buffer_primary_index_ns/doc", stats.UpdateBatchBufferPrimaryIdx)
	reportDuration("update_buffer_unique_index_ns/doc", stats.UpdateBatchBufferUniqueIdx)
	reportDuration("update_buffer_root_append_ns/doc", stats.UpdateBatchBufferRootAppend)
	reportDuration("update_buffer_flush_ns/doc", stats.UpdateBatchBufferFlush)
	reportDuration("update_publish_ns/doc", stats.UpdateBatchPublish)
}

func runProfileBenchDirectCollectionConcurrentUpdates(
	ctx context.Context,
	workers, operations, documentCount, idStride int,
	ids [][]byte,
	updateDocs []profileBenchSetUpdate,
	collection *collections.Collection,
) error {
	if workers <= 0 || operations <= 0 {
		return nil
	}
	if workers > operations {
		workers = operations
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var next atomic.Int64
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updateScratch := make([]byte, 0, 512)
			for {
				if err := runCtx.Err(); err != nil {
					return
				}
				op := int(next.Add(1) - 1)
				if op >= operations {
					return
				}
				documentOrdinal := (op * idStride) % documentCount
				id := ids[documentOrdinal]
				updateDoc := updateDocs[op%len(updateDocs)]
				matched, _, err := collection.Update(id, func(stored []byte) ([]byte, bool, error) {
					raw := bson.Raw(stored)
					originalID := raw.Lookup("_id")
					updated, nextScratch, shouldWrite, err := profileBenchApplyParsedSetUpdateToOperation(updateScratch[:0], raw, updateDoc, op, documentOrdinal, documentCount)
					updateScratch = nextScratch
					if err != nil {
						return nil, false, err
					}
					if !updated.Lookup("_id").Equal(originalID) {
						return nil, false, errUpdatedPrimaryKey
					}
					return []byte(updated), shouldWrite, nil
				})
				if err != nil {
					recordErr(err)
					return
				}
				if !matched {
					recordErr(errProfileBenchUpdateMiss)
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

type profileBenchSetField struct {
	key               string
	keyBytes          []byte
	value             bson.RawValue
	dynamicCityValues []bson.RawValue
}

type profileBenchSetUpdate struct {
	fields []profileBenchSetField
}

func profileBenchApplySetUpdate(doc bson.Raw, update bson.Raw) (bson.Raw, bool, error) {
	updateElements, err := update.Elements()
	if err != nil {
		return nil, false, err
	}
	if len(updateElements) != 1 {
		return nil, false, errors.New("profile benchmark update supports exactly one $set operator")
	}
	operator, err := updateElements[0].KeyErr()
	if err != nil {
		return nil, false, err
	}
	if operator != "$set" {
		return nil, false, errors.New("profile benchmark update supports $set only")
	}
	setDoc, ok := updateElements[0].Value().DocumentOK()
	if !ok {
		return nil, false, errors.New("profile benchmark $set value must be a document")
	}
	setElements, err := setDoc.Elements()
	if err != nil {
		return nil, false, err
	}
	if len(setElements) == 0 {
		return doc, false, nil
	}
	sets := make(map[string]bson.RawValue, len(setElements))
	setOrder := make([]string, 0, len(setElements))
	for _, elem := range setElements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, false, err
		}
		if key == "" {
			return nil, false, errors.New("profile benchmark $set field name cannot be empty")
		}
		if key == "_id" {
			return nil, false, errors.New("profile benchmark update cannot modify _id")
		}
		if strings.Contains(key, ".") {
			return nil, false, errors.New("profile benchmark $set currently supports top-level fields only")
		}
		if strings.HasPrefix(key, "$") {
			return nil, false, errors.New("profile benchmark $set field names cannot start with $")
		}
		if _, exists := sets[key]; !exists {
			setOrder = append(setOrder, key)
		}
		sets[key] = elem.Value()
	}
	parsed := profileBenchSetUpdate{fields: make([]profileBenchSetField, 0, len(setOrder))}
	for _, key := range setOrder {
		parsed.fields = append(parsed.fields, profileBenchSetField{
			key:      key,
			keyBytes: []byte(key),
			value:    sets[key],
		})
	}
	return profileBenchApplyParsedSetUpdate(doc, parsed)
}

func parseProfileBenchSetUpdate(update bson.Raw) (profileBenchSetUpdate, error) {
	updateElements, err := update.Elements()
	if err != nil {
		return profileBenchSetUpdate{}, err
	}
	if len(updateElements) != 1 {
		return profileBenchSetUpdate{}, errors.New("profile benchmark update supports exactly one $set operator")
	}
	operator, err := updateElements[0].KeyErr()
	if err != nil {
		return profileBenchSetUpdate{}, err
	}
	if operator != "$set" {
		return profileBenchSetUpdate{}, errors.New("profile benchmark update supports $set only")
	}
	setDoc, ok := updateElements[0].Value().DocumentOK()
	if !ok {
		return profileBenchSetUpdate{}, errors.New("profile benchmark $set value must be a document")
	}
	setElements, err := setDoc.Elements()
	if err != nil {
		return profileBenchSetUpdate{}, err
	}
	sets := make(map[string]bson.RawValue, len(setElements))
	setOrder := make([]string, 0, len(setElements))
	for _, elem := range setElements {
		key, err := elem.KeyErr()
		if err != nil {
			return profileBenchSetUpdate{}, err
		}
		if err := validateProfileBenchSetKey(key); err != nil {
			return profileBenchSetUpdate{}, err
		}
		if _, exists := sets[key]; !exists {
			setOrder = append(setOrder, key)
		}
		sets[key] = elem.Value()
	}
	parsed := profileBenchSetUpdate{fields: make([]profileBenchSetField, 0, len(setOrder))}
	for _, key := range setOrder {
		parsed.fields = append(parsed.fields, profileBenchSetField{
			key:      key,
			keyBytes: []byte(key),
			value:    sets[key],
		})
	}
	return parsed, nil
}

func validateProfileBenchSetKey(key string) error {
	if key == "" {
		return errors.New("profile benchmark $set field name cannot be empty")
	}
	if key == "_id" {
		return errors.New("profile benchmark update cannot modify _id")
	}
	if strings.Contains(key, ".") {
		return errors.New("profile benchmark $set currently supports top-level fields only")
	}
	if strings.HasPrefix(key, "$") {
		return errors.New("profile benchmark $set field names cannot start with $")
	}
	return nil
}

func profileBenchApplyParsedSetUpdate(doc bson.Raw, update profileBenchSetUpdate) (bson.Raw, bool, error) {
	raw, _, changed, err := profileBenchApplyParsedSetUpdateTo(nil, doc, update)
	return raw, changed, err
}

func profileBenchApplyParsedSetUpdateTo(dst []byte, doc bson.Raw, update profileBenchSetUpdate) (bson.Raw, []byte, bool, error) {
	return profileBenchApplyParsedSetUpdateToOperation(dst, doc, update, -1, 0, 0)
}

func profileBenchApplyParsedSetUpdateToOperation(dst []byte, doc bson.Raw, update profileBenchSetUpdate, operation, documentOrdinal, documentCount int) (bson.Raw, []byte, bool, error) {
	if len(update.fields) == 0 {
		return doc, dst, false, nil
	}
	length, rem, ok := bsoncore.ReadLength(doc)
	if !ok {
		return nil, dst, false, bsoncore.NewInsufficientBytesError(doc, rem)
	}
	length -= 4
	out := dst[:0]
	if cap(out) < len(doc)+64 {
		out = make([]byte, 0, len(doc)+64)
	}
	idx, out := bsoncore.AppendDocumentStart(out)
	var usedInline [8]bool
	used := usedInline[:]
	if len(update.fields) > len(usedInline) {
		used = make([]bool, len(update.fields))
	} else {
		used = used[:len(update.fields)]
	}
	var elem bsoncore.Element
	for length > 1 {
		var elemOK bool
		elem, rem, elemOK = bsoncore.ReadElement(rem)
		length -= int32(len(elem))
		if !elemOK {
			return nil, out, false, bsoncore.NewInsufficientBytesError(doc, rem)
		}
		replacement := -1
		keyBytes := elem.KeyBytes()
		for i := range update.fields {
			if bytes.Equal(keyBytes, update.fields[i].keyBytes) {
				replacement = i
				break
			}
		}
		if replacement >= 0 {
			field := update.fields[replacement]
			value := profileBenchSetFieldValueForOperation(field, operation, documentOrdinal, documentCount)
			out = bsoncore.AppendValueElement(out, field.key, bsoncore.Value{
				Type: bsoncore.Type(value.Type),
				Data: value.Value,
			})
			used[replacement] = true
			continue
		}
		out = append(out, elem...)
	}
	for i, field := range update.fields {
		if used[i] {
			continue
		}
		value := profileBenchSetFieldValueForOperation(field, operation, documentOrdinal, documentCount)
		out = bsoncore.AppendValueElement(out, field.key, bsoncore.Value{
			Type: bsoncore.Type(value.Type),
			Data: value.Value,
		})
	}
	raw, err := bsoncore.AppendDocumentEnd(out, idx)
	if err != nil {
		return nil, out, false, err
	}
	return bson.Raw(raw), raw, true, nil
}

func profileBenchSetFieldValueForOperation(field profileBenchSetField, operation, documentOrdinal, documentCount int) bson.RawValue {
	if len(field.dynamicCityValues) == 0 || operation < 0 {
		return field.value
	}
	index := benchmarkUpdatedCityIndex(operation, documentOrdinal, documentCount, len(field.dynamicCityValues))
	if documentCount > 0 && operation >= documentCount {
		previousIndex := benchmarkUpdatedCityIndex(operation-documentCount, documentOrdinal, documentCount, len(field.dynamicCityValues))
		index = avoidBenchmarkUpdatedCityRepeat(index, previousIndex, len(field.dynamicCityValues))
	}
	return field.dynamicCityValues[index]
}

func TestProfileBenchApplySetUpdateHappyPath(t *testing.T) {
	docRaw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "name", Value: "ada"},
		{Key: "count", Value: int32(1)},
	})
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: bson.D{
		{Key: "name", Value: "grace"},
		{Key: "city", Value: "hnl"},
		{Key: "count", Value: int32(1)},
	}}})
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}
	updated, changed, err := profileBenchApplySetUpdate(bson.Raw(docRaw), bson.Raw(updateRaw))
	if err != nil {
		t.Fatalf("apply update: %v", err)
	}
	if !changed {
		t.Fatal("changed=false want true for non-empty $set")
	}
	if got, ok := updated.Lookup("_id").StringValueOK(); !ok || got != "u1" {
		t.Fatalf("_id=%q ok=%v want u1", got, ok)
	}
	if got, ok := updated.Lookup("name").StringValueOK(); !ok || got != "grace" {
		t.Fatalf("name=%q ok=%v want grace", got, ok)
	}
	if got, ok := updated.Lookup("city").StringValueOK(); !ok || got != "hnl" {
		t.Fatalf("city=%q ok=%v want hnl", got, ok)
	}
	if got, ok := updated.Lookup("count").Int32OK(); !ok || got != 1 {
		t.Fatalf("count=%d ok=%v want 1", got, ok)
	}
	_, changed, err = profileBenchApplySetUpdate(updated, bson.Raw(updateRaw))
	if err != nil {
		t.Fatalf("reapply update: %v", err)
	}
	if !changed {
		t.Fatal("reapply changed=false want true so benchmark exercises write path")
	}
}

func TestProfileBenchApplySetUpdateRejectsGatewayInvalidSetFields(t *testing.T) {
	docRaw, err := bson.Marshal(bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}})
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	tests := []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "id", key: "_id"},
		{name: "dotted", key: "profile.name"},
		{name: "dollar", key: "$name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: bson.D{{Key: tt.key, Value: "grace"}}}})
			if err != nil {
				t.Fatalf("marshal update: %v", err)
			}
			if _, _, err := profileBenchApplySetUpdate(bson.Raw(docRaw), bson.Raw(updateRaw)); err == nil {
				t.Fatalf("profileBenchApplySetUpdate accepted key %q", tt.key)
			}
		})
	}
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
	dir := b.TempDir()
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
	db := target.client.Database(cfg.Database)
	coll := db.Collection(cfg.Collection, collectionOptions...)
	if err := createIndexes(ctx, db, coll, secondaryIndexes, false, true); err != nil {
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
	dir := b.TempDir()
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
	db := target.client.Database(cfg.Database)
	coll := db.Collection(cfg.Collection)
	if err := createIndexes(ctx, db, coll, secondaryIndexes, false, true); err != nil {
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
	dir := b.TempDir()
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
	db := target.client.Database(cfg.Database)
	coll := db.Collection(cfg.Collection)
	if err := createIndexes(ctx, db, coll, secondaryIndexes, false, true); err != nil {
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
	return profileBenchPositiveEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_BATCH_SIZE", 10000)
}

func profileBenchUpdateDocumentCount(tb testing.TB) int {
	return profileBenchPositiveEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_UPDATE_DOCUMENTS", 100000)
}

func profileBenchConcurrentWriters(tb testing.TB) int {
	return profileBenchPositiveEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_WRITERS", 8)
}

func profileBenchBufferedIndexedAsyncFlush(tb testing.TB) bool {
	return profileBenchBoolEnv(tb, "MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH", false)
}

func profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(tb testing.TB) int {
	return profileBenchNonNegativeEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS", 0)
}

func profileBenchBufferedIndexedWriteMaxDocuments(tb testing.TB) int {
	return profileBenchNonNegativeEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_DOCUMENTS", 0)
}

func profileBenchBufferedIndexedWriteMaxBytes(tb testing.TB) int64 {
	return profileBenchNonNegativeEnvInt64(tb, "MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_BYTES", 0)
}

func profileBenchBufferedIndexedWriteMaxRootRuns(tb testing.TB) int {
	return profileBenchNonNegativeEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_ROOT_RUNS", 0)
}

func profileBenchCollectionOptions(tb testing.TB, format collections.DocumentFormat) collections.CollectionOptions {
	tb.Helper()
	return collections.CollectionOptions{
		DocumentFormat:                          format,
		DataRootStoragePolicy:                   collections.RootStorageCompressed,
		IndexStateStoragePolicy:                 collections.RootStorageCompressed,
		BufferedIndexedWriteMaxDocuments:        profileBenchBufferedIndexedWriteMaxDocuments(tb),
		BufferedIndexedWriteMaxBytes:            profileBenchBufferedIndexedWriteMaxBytes(tb),
		BufferedIndexedWriteMaxRootRuns:         profileBenchBufferedIndexedWriteMaxRootRuns(tb),
		BufferedIndexedAsyncFlush:               profileBenchBufferedIndexedAsyncFlush(tb),
		BufferedIndexedAsyncFlushMaxQueuedUnits: profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(tb),
	}
}

func profileBenchBoolEnv(tb testing.TB, name string, defaultValue bool) bool {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		tb.Fatalf("invalid %s=%q", name, raw)
	}
	return value
}

func profileBenchPositiveEnvInt(tb testing.TB, name string, defaultValue int) int {
	tb.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		tb.Fatalf("invalid %s=%q", name, raw)
	}
	return value
}

func profileBenchNonNegativeEnvInt(tb testing.TB, name string, defaultValue int) int {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		tb.Fatalf("invalid %s=%q", name, raw)
	}
	return value
}

func profileBenchNonNegativeEnvInt64(tb testing.TB, name string, defaultValue int64) int64 {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		tb.Fatalf("invalid %s=%q", name, raw)
	}
	return value
}

func reportProfileBenchBufferedIndexedWriteOptions(b *testing.B, opts collections.CollectionOptions) {
	b.Helper()
	if opts.BufferedIndexedWriteMaxDocuments > 0 {
		b.ReportMetric(float64(opts.BufferedIndexedWriteMaxDocuments), "buffered_max_docs")
	}
	if opts.BufferedIndexedWriteMaxBytes > 0 {
		b.ReportMetric(float64(opts.BufferedIndexedWriteMaxBytes), "buffered_max_bytes")
	}
	if opts.BufferedIndexedWriteMaxRootRuns > 0 {
		b.ReportMetric(float64(opts.BufferedIndexedWriteMaxRootRuns), "buffered_max_root_runs")
	}
	if opts.BufferedIndexedAsyncFlush {
		b.ReportMetric(1, "buffered_async_flush")
		if opts.BufferedIndexedAsyncFlushMaxQueuedUnits > 0 {
			b.ReportMetric(float64(opts.BufferedIndexedAsyncFlushMaxQueuedUnits), "buffered_async_max_units")
		}
	}
}

func reportDocsPerSecond(b *testing.B, docs int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(docs)/elapsed.Seconds(), "docs/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
}
