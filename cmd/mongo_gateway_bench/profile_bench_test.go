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
	if profileBenchBufferedIndexedAsyncFlush(t) {
		t.Fatal("async flush default=true want false")
	}
	if got := profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(t); got != 0 {
		t.Fatalf("async max queued units default=%d want 0", got)
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH", "true")
	if !profileBenchBufferedIndexedAsyncFlush(t) {
		t.Fatal("async flush env=true want true")
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS", "6")
	if got := profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(t); got != 6 {
		t.Fatalf("async max queued units=%d want 6", got)
	}
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
		Name: "bench.docs",
		Options: collections.CollectionOptions{
			DocumentFormat:                          collections.DocumentFormatBSON,
			DataRootStoragePolicy:                   collections.RootStorageCompressed,
			IndexStateStoragePolicy:                 collections.RootStorageCompressed,
			BufferedIndexedAsyncFlush:               profileBenchBufferedIndexedAsyncFlush(b),
			BufferedIndexedAsyncFlushMaxQueuedUnits: profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(b),
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
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat:                          collections.DocumentFormatBSON,
		DataRootStoragePolicy:                   collections.RootStorageCompressed,
		IndexStateStoragePolicy:                 collections.RootStorageCompressed,
		BufferedIndexedAsyncFlush:               profileBenchBufferedIndexedAsyncFlush(b),
		BufferedIndexedAsyncFlushMaxQueuedUnits: profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(b),
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
	flushStart := time.Now()
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush raw-wire collections: %v", err)
	}
	timedElapsed += time.Since(flushStart)
	b.StopTimer()
	reportProfileBenchBufferedIndexedAsyncFlush(b, collection.Meta().Options)
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
		Name: "bench.docs",
		Options: collections.CollectionOptions{
			DocumentFormat:                          collections.DocumentFormatBSON,
			DataRootStoragePolicy:                   collections.RootStorageCompressed,
			IndexStateStoragePolicy:                 collections.RootStorageCompressed,
			BufferedIndexedAsyncFlush:               profileBenchBufferedIndexedAsyncFlush(b),
			BufferedIndexedAsyncFlushMaxQueuedUnits: profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(b),
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
	reportProfileBenchBufferedIndexedAsyncFlush(b, collection.Meta().Options)
	reportDocsPerSecond(b, b.N, timedElapsed)
}

func BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2(b *testing.B) {
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
			DocumentFormat:                          collections.DocumentFormatBSON,
			DataRootStoragePolicy:                   collections.RootStorageCompressed,
			IndexStateStoragePolicy:                 collections.RootStorageCompressed,
			BufferedIndexedAsyncFlush:               profileBenchBufferedIndexedAsyncFlush(b),
			BufferedIndexedAsyncFlushMaxQueuedUnits: profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(b),
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
	updateDocs := make([]profileBenchSetUpdate, profileBenchUpdateDocPoolSize)
	for i := range updateDocs {
		updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: bson.D{
			{Key: "concurrent_updated", Value: true},
			{Key: "concurrent_update_seq", Value: int64(i)},
		}}})
		if err != nil {
			b.Fatalf("marshal update document: %v", err)
		}
		parsed, err := parseProfileBenchSetUpdate(bson.Raw(updateRaw))
		if err != nil {
			b.Fatalf("parse update document: %v", err)
		}
		updateDocs[i] = parsed
	}
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
	if err := runProfileBenchDirectCollectionConcurrentUpdates(context.Background(), writers, warmupOps, documentCount, idStride, ids, updateDocs, collection); err != nil {
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
	reportProfileBenchBufferedIndexedAsyncFlush(b, collection.Meta().Options)
	reportCollectionManagerUpdateStats(b, deltaCollectionManagerUpdateStats(manager.StatsSnapshot(), statsBefore), b.N)
	reportProfileBenchBackendVlogMmapStats(b, backend.Stats(), backendStatsBefore, b.N)
	reportDocsPerSecond(b, b.N, timedElapsed)
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
	hits := profileBenchDeltaUintStat(after, before, "treedb.vlog.mmap_read.hits")
	fallbacks := profileBenchDeltaUintStat(after, before, "treedb.vlog.mmap_read.fallback_readat")
	if total := hits + fallbacks; total > 0 {
		b.ReportMetric(float64(hits)/float64(total), "backend_vlog_mmap_hit_ratio")
	}
	deadDelta := profileBenchSignedDeltaUintStat(after, before, "treedb.vlog.mmap_dead_mappings")
	b.ReportMetric(float64(deadDelta), "backend_vlog_mmap_dead_mappings_delta")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_dead_mappings")), "backend_vlog_mmap_dead_mappings")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_sealed_segments")), "backend_vlog_mmap_sealed_segments")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_sealed_bytes")), "backend_vlog_mmap_sealed_bytes")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_active_segments")), "backend_vlog_mmap_active_segments")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.mmap_active_bytes")), "backend_vlog_mmap_active_bytes")
}

func deltaCollectionManagerUpdateStats(after, before collections.CollectionManagerStats) collections.CollectionManagerStats {
	return collections.CollectionManagerStats{
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
	}
}

func reportCollectionManagerUpdateStats(b *testing.B, stats collections.CollectionManagerStats, docs int) {
	b.Helper()
	if docs <= 0 {
		return
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
	if stats.UpdateBatchSecondaryDeletes > 0 {
		b.ReportMetric(float64(stats.UpdateBatchSecondaryDeletes)/float64(docs), "update_secondary_deletes/doc")
	}
	if stats.UpdateBatchSecondarySets > 0 {
		b.ReportMetric(float64(stats.UpdateBatchSecondarySets)/float64(docs), "update_secondary_sets/doc")
	}
	if stats.UpdateBatchSecondaryKeyBytes > 0 {
		b.ReportMetric(float64(stats.UpdateBatchSecondaryKeyBytes)/float64(docs), "update_secondary_key_bytes/doc")
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
				id := ids[(op*idStride)%documentCount]
				updateDoc := updateDocs[op%len(updateDocs)]
				matched, _, err := collection.Update(id, func(stored []byte) ([]byte, bool, error) {
					raw := bson.Raw(stored)
					originalID := raw.Lookup("_id")
					updated, nextScratch, shouldWrite, err := profileBenchApplyParsedSetUpdateTo(updateScratch[:0], raw, updateDoc)
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
	key      string
	keyBytes []byte
	value    bson.RawValue
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
			out = bsoncore.AppendValueElement(out, field.key, bsoncore.Value{
				Type: bsoncore.Type(field.value.Type),
				Data: field.value.Value,
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
		out = bsoncore.AppendValueElement(out, field.key, bsoncore.Value{
			Type: bsoncore.Type(field.value.Type),
			Data: field.value.Value,
		})
	}
	raw, err := bsoncore.AppendDocumentEnd(out, idx)
	if err != nil {
		return nil, out, false, err
	}
	return bson.Raw(raw), raw, true, nil
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

func reportProfileBenchBufferedIndexedAsyncFlush(b *testing.B, opts collections.CollectionOptions) {
	b.Helper()
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
