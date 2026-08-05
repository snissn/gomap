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
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

var (
	errProfileBenchUpdateMiss = errors.New("profile benchmark update missed document")
)

const (
	// Keep update payloads bounded so the benchmark stresses collection update
	// concurrency instead of BSON allocation variety.
	profileBenchUpdateDocPoolSize = 4096
	// Preferred deterministic stride for scrambling sequential operation
	// numbers across the preloaded ID set.
	profileBenchPreferredUpdateIDStride = 37
	profileBenchLabelBenchmarkKey       = "gomap_benchmark"
	profileBenchLabelPhaseKey           = "gomap_benchmark_phase"
	profileBenchDirectUpdateBenchmark   = "direct_collection_concurrent_update_bson"
	profileBenchTimedUpdatePhase        = "timed_update_and_flush"
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
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_DISABLE_BUFFERED_INDEXED_ASYNC_FLUSH", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_DOCUMENTS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_BYTES", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_WRITE_MAX_ROOT_RUNS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_SHARDS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_LANE_WORKERS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_OVERLAY_ROOTS", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_COMPACT_OVERLAY_ROOTS_AFTER_FLUSH", "")
	if profileBenchBufferedIndexedAsyncFlush(t) {
		t.Fatal("async flush default=true want false")
	}
	if profileBenchDisableBufferedIndexedAsyncFlush(t) {
		t.Fatal("disable async flush default=true want false")
	}
	if profileBenchBufferedIndexedOverlayRoots(t) {
		t.Fatal("overlay roots default=true want false")
	}
	if profileBenchCompactOverlayRootsAfterFlush(t) {
		t.Fatal("compact overlay roots after flush default=true want false")
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
	if got := profileBenchUpdateCombineShards(t); got != 1 {
		t.Fatalf("update combine shards default=%d want 1", got)
	}
	if profileBenchUpdateCombineLaneWorkers(t) {
		t.Fatal("update combine lane workers default=true want false")
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH", "true")
	if !profileBenchBufferedIndexedAsyncFlush(t) {
		t.Fatal("async flush env=true want true")
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH", "")
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_DISABLE_BUFFERED_INDEXED_ASYNC_FLUSH", "true")
	if !profileBenchDisableBufferedIndexedAsyncFlush(t) {
		t.Fatal("disable async flush env=true want true")
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
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_SHARDS", "4")
	if got := profileBenchUpdateCombineShards(t); got != 4 {
		t.Fatalf("update combine shards=%d want 4", got)
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_LANE_WORKERS", "true")
	if !profileBenchUpdateCombineLaneWorkers(t) {
		t.Fatal("update combine lane workers env=true want true")
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_OVERLAY_ROOTS", "true")
	if !profileBenchBufferedIndexedOverlayRoots(t) {
		t.Fatal("overlay roots env=true want true")
	}
	t.Setenv("MONGO_GATEWAY_PROFILE_BENCH_COMPACT_OVERLAY_ROOTS_AFTER_FLUSH", "true")
	if !profileBenchCompactOverlayRootsAfterFlush(t) {
		t.Fatal("compact overlay roots after flush env=true want true")
	}
}

func TestProfileBenchEffectiveUpdateCombineLaneWorkers(t *testing.T) {
	for _, tc := range []struct {
		name        string
		shards      int
		laneWorkers bool
		want        bool
	}{
		{name: "disabled", shards: 4, laneWorkers: false, want: false},
		{name: "single shard", shards: 1, laneWorkers: true, want: false},
		{name: "sharded", shards: 4, laneWorkers: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := profileBenchEffectiveUpdateCombineLaneWorkers(tc.shards, tc.laneWorkers); got != tc.want {
				t.Fatalf("effective lane workers=%v want %v", got, tc.want)
			}
		})
	}
}

func TestProfileBenchTimedUpdatePhaseLabels(t *testing.T) {
	called := false
	err := runProfileBenchTimedUpdatePhase(context.Background(), func(ctx context.Context) error {
		called = true
		if got, ok := pprof.Label(ctx, profileBenchLabelBenchmarkKey); !ok || got != profileBenchDirectUpdateBenchmark {
			t.Fatalf("benchmark label=(%q,%v), want (%q,true)", got, ok, profileBenchDirectUpdateBenchmark)
		}
		if got, ok := pprof.Label(ctx, profileBenchLabelPhaseKey); !ok || got != profileBenchTimedUpdatePhase {
			t.Fatalf("phase label=(%q,%v), want (%q,true)", got, ok, profileBenchTimedUpdatePhase)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run timed phase: %v", err)
	}
	if !called {
		t.Fatal("timed phase callback was not called")
	}
	if err := runProfileBenchTimedUpdatePhase(context.Background(), nil); err == nil {
		t.Fatal("nil timed phase callback returned nil error")
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

func TestProfileBenchParsedUpdateDocsUsesDistinctSequencePhases(t *testing.T) {
	warmup := profileBenchParsedUpdateDocs(t, false, "warmup")
	timed := profileBenchParsedUpdateDocs(t, false, "timed")
	if len(warmup) == 0 || len(timed) == 0 {
		t.Fatal("expected parsed update docs")
	}
	warmupSeq := profileBenchSetUpdateFieldInt64(t, warmup[0], "concurrent_update_seq")
	timedSeq := profileBenchSetUpdateFieldInt64(t, timed[0], "concurrent_update_seq")
	if warmupSeq == timedSeq {
		t.Fatalf("sequence update phases both produced %d; timed sequence updates must differ from warmup", warmupSeq)
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

func profileBenchSetUpdateFieldInt64(t *testing.T, update profileBenchSetUpdate, key string) int64 {
	t.Helper()
	for _, field := range update.fields {
		if field.key != key {
			continue
		}
		value := profileBenchSetFieldValueForOperation(field, -1, 0, 0)
		got, ok := value.Int64OK()
		if !ok {
			t.Fatalf("field %q raw value=%v is not an int64", key, value.Type)
		}
		return got
	}
	t.Fatalf("missing update field %q in %+v", key, update.fields)
	return 0
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

func TestReportProfileBenchOrderedRootPublishStatsIncludesPrepareError(t *testing.T) {
	result := testing.Benchmark(func(b *testing.B) {
		before := map[string]string{
			"treedb.publish.ordered_root_delta_group.calls_total": "1",
		}
		after := map[string]string{
			"treedb.publish.ordered_root_delta_group.calls_total":                                                                   "2",
			"treedb.publish.ordered_root_delta_group.roots_total":                                                                   "1",
			"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.ops_total":                           "6",
			"treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.fallback.reason.prepare_error.ops_total": "4",
		}
		reportProfileBenchOrderedRootPublishStats(b, after, before, 20)
	})
	got, ok := result.Extra["publish_delta_group_span_native_fallback_prepare_error_ops/doc"]
	if !ok {
		t.Fatalf("missing prepare_error profile metric: %v", result.Extra)
	}
	if want := 0.3; math.Abs(got-want) > 1e-9 {
		t.Fatalf("prepare_error profile metric=%v want %v", got, want)
	}
	got, ok = result.Extra["publish_delta_group_span_native_route_command_wal_publish_fallback_prepare_error_ops/doc"]
	if !ok {
		t.Fatalf("missing command_wal_publish prepare_error profile metric: %v", result.Extra)
	}
	if want := 0.2; math.Abs(got-want) > 1e-9 {
		t.Fatalf("command_wal_publish prepare_error profile metric=%v want %v", got, want)
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
	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dir)
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
		TreeDBProfile:               treedb.ProfileBenchUnsafe,
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
	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dir)
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
	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dir)
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
	preloadCompactStats := compactProfileBenchOverlayRootsAfterFlush(b, collection, "preload")
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
	warmupCompactStats := compactProfileBenchOverlayRootsAfterFlush(b, collection, "warmup")
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint warm up: %v", err)
	}

	b.ReportAllocs()
	manager.SetUpdateBatchDetailedStatsEnabled(true)
	updateCombineShards := profileBenchUpdateCombineShards(b)
	manager.SetUpdateCombineShardsForProfiling(updateCombineShards)
	updateCombineLaneWorkers := profileBenchUpdateCombineLaneWorkers(b)
	manager.SetUpdateCombineLaneWorkersForProfiling(updateCombineLaneWorkers)
	manager.ResetUpdateCombinersForProfiling()
	manager.ResetUpdateCombineQueueDepthMax()
	statsBefore := manager.StatsSnapshot()
	backendStatsBefore := backend.Stats()
	b.ResetTimer()
	started := time.Now()
	var logicalUpdateElapsed time.Duration
	var finalFlushElapsed time.Duration
	err = runProfileBenchTimedUpdatePhase(context.Background(), func(ctx context.Context) error {
		updateStarted := time.Now()
		if err := runProfileBenchDirectCollectionConcurrentUpdates(ctx, writers, b.N, documentCount, idStride, ids, updateDocs, collection); err != nil {
			return err
		}
		logicalUpdateElapsed = time.Since(updateStarted)
		// Keep async indexed-flush rows comparable with synchronous rows: the
		// timed update phase includes the final drain of deferred publish work.
		flushStarted := time.Now()
		err := manager.FlushAll()
		finalFlushElapsed = time.Since(flushStarted)
		return err
	})
	timedElapsed := time.Since(started)
	b.StopTimer()
	if err != nil {
		b.Fatalf("run concurrent updates: %v", err)
	}
	b.ReportMetric(float64(writers), "writers")
	b.ReportMetric(float64(updateCombineShards), "update_combine_shards")
	if profileBenchEffectiveUpdateCombineLaneWorkers(updateCombineShards, updateCombineLaneWorkers) {
		b.ReportMetric(1, "update_combine_lane_workers")
	}
	reportProfileBenchBufferedIndexedWriteOptions(b, collection.Meta().Options)
	reportProfileBenchOverlayCompactionStats(b, "preload", preloadCompactStats)
	reportProfileBenchOverlayCompactionStats(b, "warmup", warmupCompactStats)
	reportCollectionManagerUpdateStats(b, deltaCollectionManagerUpdateStats(manager.StatsSnapshot(), statsBefore), b.N)
	backendStatsAfter := backend.Stats()
	reportProfileBenchOrderedRootPublishStats(b, backendStatsAfter, backendStatsBefore, b.N)
	reportProfileBenchBackendVlogMmapStats(b, backendStatsAfter, backendStatsBefore, b.N)
	reportDocsPerSecond(b, b.N, timedElapsed)
	if logicalUpdateElapsed > 0 {
		b.ReportMetric(float64(b.N)/logicalUpdateElapsed.Seconds(), "logical_update_docs/sec")
	}
	if finalFlushElapsed > 0 {
		b.ReportMetric(float64(finalFlushElapsed.Nanoseconds())/float64(b.N), "final_flush_ns/doc")
	}
}

func runProfileBenchTimedUpdatePhase(ctx context.Context, run func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if run == nil {
		return errors.New("mongo gateway profile benchmark timed update phase callback is nil")
	}
	var err error
	pprof.Do(ctx, pprof.Labels(
		profileBenchLabelBenchmarkKey, profileBenchDirectUpdateBenchmark,
		profileBenchLabelPhaseKey, profileBenchTimedUpdatePhase,
	), func(ctx context.Context) {
		err = run(ctx)
	})
	return err
}

func compactProfileBenchOverlayRootsAfterFlush(tb testing.TB, collection *collections.Collection, phase string) collections.CollectionRootOverlayCompactionStats {
	tb.Helper()
	if !profileBenchCompactOverlayRootsAfterFlush(tb) {
		return collections.CollectionRootOverlayCompactionStats{}
	}
	if collection == nil {
		tb.Fatalf("compact overlay roots after %s: collection is nil", phase)
	}
	stats, err := collection.CompactRootOverlays(context.Background())
	if err != nil {
		tb.Fatalf("compact overlay roots after %s: %v", phase, err)
	}
	return stats
}

func profileBenchParsedUpdateDocs(tb testing.TB, updateCity bool, updatePhase string) []profileBenchSetUpdate {
	tb.Helper()
	updateDocs := make([]profileBenchSetUpdate, profileBenchUpdateDocPoolSize)
	dynamicSequenceValues := profileBenchUpdatedSequenceRawValues(updatePhase)
	var dynamicCityValues []bson.RawValue
	if updateCity {
		dynamicCityValues = profileBenchUpdatedCityRawValues(tb, updatePhase)
	}
	for i := range updateDocs {
		set := bson.D{
			{Key: "concurrent_updated", Value: true},
			{Key: "concurrent_update_seq", Value: profileBenchUpdatedSequenceValue(updatePhase, i)},
		}
		if updateCity {
			set = append(set, bson.E{Key: "city", Value: updatePhase + "-" + benchmarkUpdatedCity(i, i, profileBenchUpdateDocPoolSize)})
		}
		updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: set}})
		if err != nil {
			tb.Fatalf("marshal update document: %v", err)
		}
		parsed, err := parseProfileBenchSetUpdate(bson.Raw(updateRaw))
		if err != nil {
			tb.Fatalf("parse update document: %v", err)
		}
		for fieldIdx := range parsed.fields {
			switch parsed.fields[fieldIdx].key {
			case "city":
				if len(dynamicCityValues) > 0 {
					parsed.fields[fieldIdx].dynamicValues = dynamicCityValues
				}
			case "concurrent_update_seq":
				parsed.fields[fieldIdx].dynamicValues = dynamicSequenceValues
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

func profileBenchUpdatedSequenceRawValues(updatePhase string) []bson.RawValue {
	values := make([]bson.RawValue, profileBenchUpdatedSequenceValueCount)
	for i := range values {
		values[i] = bson.RawValue{
			Type:  bson.TypeInt64,
			Value: bsoncore.AppendInt64(nil, profileBenchUpdatedSequenceValue(updatePhase, i)),
		}
	}
	return values
}

// Keep the sequence cardinality large enough to avoid revisiting replacement
// values during the profile sweep without coupling it to city value generation.
const profileBenchUpdatedSequenceValueCount = 65521

func profileBenchUpdatedSequenceValue(updatePhase string, i int) int64 {
	switch updatePhase {
	case "warmup":
		return int64(i)
	case "timed":
		return int64(i) + 1_000_000_000
	default:
		return int64(i) + 2_000_000_000
	}
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
	reportPerDoc("backend_vlog_grouped_frame_cache_hits/doc", "treedb.vlog.grouped_frame_cache.hits")
	reportPerDoc("backend_vlog_grouped_frame_cache_misses/doc", "treedb.vlog.grouped_frame_cache.misses")
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
	groupedFrameCacheHits := profileBenchDeltaUintStat(after, before, "treedb.vlog.grouped_frame_cache.hits")
	groupedFrameCacheMisses := profileBenchDeltaUintStat(after, before, "treedb.vlog.grouped_frame_cache.misses")
	if total := groupedFrameCacheHits + groupedFrameCacheMisses; total > 0 {
		b.ReportMetric(float64(groupedFrameCacheHits)/float64(total), "backend_vlog_grouped_frame_cache_hit_ratio")
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
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.grouped_frame_cache.entries")), "backend_vlog_grouped_frame_cache_entries")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.grouped_frame_cache.capacity")), "backend_vlog_grouped_frame_cache_capacity")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.grouped_frame_cache.allocated_shards")), "backend_vlog_grouped_frame_cache_allocated_shards")
	b.ReportMetric(float64(profileBenchUintStat(after, "treedb.vlog.grouped_frame_cache.allocated_slots")), "backend_vlog_grouped_frame_cache_allocated_slots")
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
	rootApplyParallelGroups := profileBenchDeltaUintStat(after, before, prefix+"root_apply_parallel_groups_total")
	rootApplyParallelRoots := profileBenchDeltaUintStat(after, before, prefix+"root_apply_parallel_roots_total")
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
	spanNativeCandidateOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.candidate_ops_total")
	spanNativeEligibleOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.eligible_ops_total")
	spanNativeUsedOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.used_ops_total")
	spanNativeIneligibleOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.ineligible_ops_total")
	spanNativeFallbacks := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallbacks_total")
	spanNativeNotImplementedOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallback.reason.span_native_not_implemented.ops_total")
	spanNativePrepareErrorOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallback.reason.prepare_error.ops_total")
	spanNativeRouteIneligibleOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallback.reason.route_ineligible.ops_total")
	spanNativeDisabledOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallback.reason.disabled.ops_total")
	spanNativeAdmissionDeclineOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallback.reason.admission_policy_decline.ops_total")
	spanNativeColdBuildOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallback.reason.cold_build.ops_total")
	spanNativeUnknownOps := profileBenchDeltaUintStat(after, before, prefix+"span_native.fallback.reason.unknown.ops_total")
	spanNativeRoutePrepareErrorOps := []struct {
		route string
		ops   uint64
	}{}
	for _, route := range []string{
		string(backenddb.OrderedRootSpanNativeRouteDirectPublish),
		string(backenddb.OrderedRootSpanNativeRouteGroupedPublish),
		string(backenddb.OrderedRootSpanNativeRouteSystemDeltaBuilderPublish),
		string(backenddb.OrderedRootSpanNativeRouteCommandWALPublish),
		string(backenddb.OrderedRootSpanNativeRouteCollectionBufferedRoots),
		string(backenddb.OrderedRootSpanNativeRouteOverlayColdBuild),
		string(backenddb.OrderedRootSpanNativeRouteMultiIndexGroupPublish),
		string(backenddb.OrderedRootSpanNativeRouteDeltaBatchPublish),
		string(backenddb.OrderedRootSpanNativeRouteReadOnlyPrepare),
	} {
		spanNativeRoutePrepareErrorOps = append(spanNativeRoutePrepareErrorOps, struct {
			route string
			ops   uint64
		}{
			route: route,
			ops:   profileBenchDeltaUintStat(after, before, prefix+"span_native.route."+route+".fallback.reason.prepare_error.ops_total"),
		})
	}
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
	b.ReportMetric(float64(rootApplyParallelGroups), "publish_delta_group_root_apply_parallel_groups")
	b.ReportMetric(float64(rootApplyParallelRoots), "publish_delta_group_root_apply_parallel_roots")
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
	b.ReportMetric(float64(spanNativeCandidateOps)/float64(docs), "publish_delta_group_span_native_candidate_ops/doc")
	b.ReportMetric(float64(spanNativeEligibleOps)/float64(docs), "publish_delta_group_span_native_eligible_ops/doc")
	b.ReportMetric(float64(spanNativeUsedOps)/float64(docs), "publish_delta_group_span_native_used_ops/doc")
	b.ReportMetric(float64(spanNativeIneligibleOps)/float64(docs), "publish_delta_group_span_native_ineligible_ops/doc")
	b.ReportMetric(float64(spanNativeFallbacks), "publish_delta_group_span_native_fallbacks")
	b.ReportMetric(float64(spanNativeNotImplementedOps)/float64(docs), "publish_delta_group_span_native_fallback_not_implemented_ops/doc")
	b.ReportMetric(float64(spanNativePrepareErrorOps)/float64(docs), "publish_delta_group_span_native_fallback_prepare_error_ops/doc")
	for _, routeMetric := range spanNativeRoutePrepareErrorOps {
		b.ReportMetric(float64(routeMetric.ops)/float64(docs), "publish_delta_group_span_native_route_"+routeMetric.route+"_fallback_prepare_error_ops/doc")
	}
	b.ReportMetric(float64(spanNativeRouteIneligibleOps)/float64(docs), "publish_delta_group_span_native_fallback_route_ineligible_ops/doc")
	b.ReportMetric(float64(spanNativeDisabledOps)/float64(docs), "publish_delta_group_span_native_fallback_disabled_ops/doc")
	b.ReportMetric(float64(spanNativeAdmissionDeclineOps)/float64(docs), "publish_delta_group_span_native_fallback_admission_policy_decline_ops/doc")
	b.ReportMetric(float64(spanNativeColdBuildOps)/float64(docs), "publish_delta_group_span_native_fallback_cold_build_ops/doc")
	b.ReportMetric(float64(spanNativeUnknownOps)/float64(docs), "publish_delta_group_span_native_fallback_unknown_ops/doc")
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
	if spanNativeCandidateOps > 0 {
		b.ReportMetric(float64(spanNativeUsedOps)/float64(spanNativeCandidateOps), "publish_delta_group_span_native_used_ops/candidate_op")
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
		UpdateBatchCalls:                 after.UpdateBatchCalls - before.UpdateBatchCalls,
		UpdateBatchItems:                 after.UpdateBatchItems - before.UpdateBatchItems,
		UpdateBatchMatched:               after.UpdateBatchMatched - before.UpdateBatchMatched,
		UpdateBatchModified:              after.UpdateBatchModified - before.UpdateBatchModified,
		UpdateBatchRuns:                  after.UpdateBatchRuns - before.UpdateBatchRuns,
		UpdateBatchBufferedBatches:       after.UpdateBatchBufferedBatches - before.UpdateBatchBufferedBatches,
		UpdateBatchCurrentRead:           after.UpdateBatchCurrentRead - before.UpdateBatchCurrentRead,
		UpdateBatchCallback:              after.UpdateBatchCallback - before.UpdateBatchCallback,
		UpdateBatchStructuredApply:       after.UpdateBatchStructuredApply - before.UpdateBatchStructuredApply,
		UpdateBatchPrepareDocuments:      after.UpdateBatchPrepareDocuments - before.UpdateBatchPrepareDocuments,
		UpdateBatchIndexStateExtract:     after.UpdateBatchIndexStateExtract - before.UpdateBatchIndexStateExtract,
		UpdateBatchOldIndexStateExtract:  after.UpdateBatchOldIndexStateExtract - before.UpdateBatchOldIndexStateExtract,
		UpdateBatchNewIndexStateExtract:  after.UpdateBatchNewIndexStateExtract - before.UpdateBatchNewIndexStateExtract,
		UpdateBatchUniquePreflight:       after.UpdateBatchUniquePreflight - before.UpdateBatchUniquePreflight,
		UpdateBatchTemplateRunBuild:      after.UpdateBatchTemplateRunBuild - before.UpdateBatchTemplateRunBuild,
		UpdateBatchPrimaryRunBuild:       after.UpdateBatchPrimaryRunBuild - before.UpdateBatchPrimaryRunBuild,
		UpdateBatchIndexStateRunBuild:    after.UpdateBatchIndexStateRunBuild - before.UpdateBatchIndexStateRunBuild,
		UpdateBatchSecondaryRunBuild:     after.UpdateBatchSecondaryRunBuild - before.UpdateBatchSecondaryRunBuild,
		UpdateBatchBufferStage:           after.UpdateBatchBufferStage - before.UpdateBatchBufferStage,
		UpdateBatchBufferPrecheck:        after.UpdateBatchBufferPrecheck - before.UpdateBatchBufferPrecheck,
		UpdateBatchBufferLockWait:        after.UpdateBatchBufferLockWait - before.UpdateBatchBufferLockWait,
		UpdateBatchBufferLockHold:        after.UpdateBatchBufferLockHold - before.UpdateBatchBufferLockHold,
		UpdateBatchBufferValidation:      after.UpdateBatchBufferValidation - before.UpdateBatchBufferValidation,
		UpdateBatchBufferRootScan:        after.UpdateBatchBufferRootScan - before.UpdateBatchBufferRootScan,
		UpdateBatchBufferDomainPrepare:   after.UpdateBatchBufferDomainPrepare - before.UpdateBatchBufferDomainPrepare,
		UpdateBatchBufferFreeze:          after.UpdateBatchBufferFreeze - before.UpdateBatchBufferFreeze,
		UpdateBatchBufferRootTable:       after.UpdateBatchBufferRootTable - before.UpdateBatchBufferRootTable,
		UpdateBatchBufferPrimaryIdx:      after.UpdateBatchBufferPrimaryIdx - before.UpdateBatchBufferPrimaryIdx,
		UpdateBatchBufferUniqueIdx:       after.UpdateBatchBufferUniqueIdx - before.UpdateBatchBufferUniqueIdx,
		UpdateBatchBufferPrimaryAppend:   after.UpdateBatchBufferPrimaryAppend - before.UpdateBatchBufferPrimaryAppend,
		UpdateBatchBufferSecondaryAppend: after.UpdateBatchBufferSecondaryAppend - before.UpdateBatchBufferSecondaryAppend,
		UpdateBatchBufferRootAppend:      after.UpdateBatchBufferRootAppend - before.UpdateBatchBufferRootAppend,
		UpdateBatchBufferFlush:           after.UpdateBatchBufferFlush - before.UpdateBatchBufferFlush,
		UpdateBatchPublish:               after.UpdateBatchPublish - before.UpdateBatchPublish,
		UpdateBatchSecondaryDeletes:      after.UpdateBatchSecondaryDeletes - before.UpdateBatchSecondaryDeletes,
		UpdateBatchSecondarySets:         after.UpdateBatchSecondarySets - before.UpdateBatchSecondarySets,
		UpdateBatchSecondaryKeyBytes:     after.UpdateBatchSecondaryKeyBytes - before.UpdateBatchSecondaryKeyBytes,
		UpdateBatchIndexValueChanges:     after.UpdateBatchIndexValueChanges - before.UpdateBatchIndexValueChanges,
		UpdateBatchIndexValueUnchanged:   after.UpdateBatchIndexValueUnchanged - before.UpdateBatchIndexValueUnchanged,
		UpdateBatchMaskFallbacks:         after.UpdateBatchMaskFallbacks - before.UpdateBatchMaskFallbacks,
		UpdateBatchUniqueChecks:          after.UpdateBatchUniqueChecks - before.UpdateBatchUniqueChecks,
		UpdateBatchUniqueCheckSkips:      after.UpdateBatchUniqueCheckSkips - before.UpdateBatchUniqueCheckSkips,
		UpdateCombineRequests:            after.UpdateCombineRequests - before.UpdateCombineRequests,
		UpdateCombineBatches:             after.UpdateCombineBatches - before.UpdateCombineBatches,
		UpdateCombineBatchedRequests:     after.UpdateCombineBatchedRequests - before.UpdateCombineBatchedRequests,
		UpdateCombineFallbackRequests:    after.UpdateCombineFallbackRequests - before.UpdateCombineFallbackRequests,
		UpdateCombineInlineRequests:      after.UpdateCombineInlineRequests - before.UpdateCombineInlineRequests,
		UpdateCombineEnqueue:             after.UpdateCombineEnqueue - before.UpdateCombineEnqueue,
		UpdateCombineWait:                after.UpdateCombineWait - before.UpdateCombineWait,
		UpdateCombineQueueWait:           after.UpdateCombineQueueWait - before.UpdateCombineQueueWait,
		UpdateCombineDrain:               after.UpdateCombineDrain - before.UpdateCombineDrain,
		UpdateCombineRun:                 after.UpdateCombineRun - before.UpdateCombineRun,
		UpdateCombineResultDelivery:      after.UpdateCombineResultDelivery - before.UpdateCombineResultDelivery,
		IndexedFlushCalls:                after.IndexedFlushCalls - before.IndexedFlushCalls,
		IndexedFlushErrors:               after.IndexedFlushErrors - before.IndexedFlushErrors,
		IndexedFlushForcedDrains:         after.IndexedFlushForcedDrains - before.IndexedFlushForcedDrains,
		IndexedFlushUnits:                after.IndexedFlushUnits - before.IndexedFlushUnits,
		IndexedFlushDocs:                 after.IndexedFlushDocs - before.IndexedFlushDocs,
		IndexedFlushBytes:                after.IndexedFlushBytes - before.IndexedFlushBytes,
		IndexedFlushRootRuns:             after.IndexedFlushRootRuns - before.IndexedFlushRootRuns,
		IndexedFlushRoots:                after.IndexedFlushRoots - before.IndexedFlushRoots,
		IndexedFlushDuration:             after.IndexedFlushDuration - before.IndexedFlushDuration,
		IndexedFlushMaterialize:          after.IndexedFlushMaterialize - before.IndexedFlushMaterialize,
		IndexedFlushPublish:              after.IndexedFlushPublish - before.IndexedFlushPublish,
		IndexedAsyncFlushWait:            after.IndexedAsyncFlushWait - before.IndexedAsyncFlushWait,
		RootDeltaPlanPrimaryRoots:        after.RootDeltaPlanPrimaryRoots - before.RootDeltaPlanPrimaryRoots,
		RootDeltaPlanTemplateRoots:       after.RootDeltaPlanTemplateRoots - before.RootDeltaPlanTemplateRoots,
		RootDeltaPlanIndexStateRoots:     after.RootDeltaPlanIndexStateRoots - before.RootDeltaPlanIndexStateRoots,
		RootDeltaPlanSecondaryRoots:      after.RootDeltaPlanSecondaryRoots - before.RootDeltaPlanSecondaryRoots,
		RootDeltaPlanEntries:             after.RootDeltaPlanEntries - before.RootDeltaPlanEntries,
		RootDeltaPlanKeyBytes:            after.RootDeltaPlanKeyBytes - before.RootDeltaPlanKeyBytes,
		RootDeltaPlanValueBytes:          after.RootDeltaPlanValueBytes - before.RootDeltaPlanValueBytes,
		RootDeltaPlanTombstones:          after.RootDeltaPlanTombstones - before.RootDeltaPlanTombstones,
		PrimaryOnlyUpdateCalls:           after.PrimaryOnlyUpdateCalls - before.PrimaryOnlyUpdateCalls,
		PrimaryOnlyMatched:               after.PrimaryOnlyMatched - before.PrimaryOnlyMatched,
		PrimaryOnlyModified:              after.PrimaryOnlyModified - before.PrimaryOnlyModified,
		PrimaryOnlyBufferedCalls:         after.PrimaryOnlyBufferedCalls - before.PrimaryOnlyBufferedCalls,
		PrimaryOnlyRootPublishes:         after.PrimaryOnlyRootPublishes - before.PrimaryOnlyRootPublishes,
		PrimaryOnlyRootDeltaEntries:      after.PrimaryOnlyRootDeltaEntries - before.PrimaryOnlyRootDeltaEntries,
		PrimaryOnlyRootDeltaKeyBytes:     after.PrimaryOnlyRootDeltaKeyBytes - before.PrimaryOnlyRootDeltaKeyBytes,
		PrimaryOnlyRootDeltaValueBytes:   after.PrimaryOnlyRootDeltaValueBytes - before.PrimaryOnlyRootDeltaValueBytes,
		PrimaryOnlyCoalescedDocs:         after.PrimaryOnlyCoalescedDocs - before.PrimaryOnlyCoalescedDocs,
	}
	delta.OverlayMutableDocuments = after.OverlayMutableDocuments
	delta.OverlayQueuedIndexedFlushUnits = after.OverlayQueuedIndexedFlushUnits
	delta.OverlayActiveIndexedFlushUnits = after.OverlayActiveIndexedFlushUnits
	delta.OverlayVisibleDepth = after.OverlayVisibleDepth
	if after.UpdateCombineQueueDepthMax > before.UpdateCombineQueueDepthMax {
		delta.UpdateCombineQueueDepthMax = after.UpdateCombineQueueDepthMax
	}
	for i := range delta.UpdateCombineQueueDepthBuckets {
		delta.UpdateCombineQueueDepthBuckets[i] = after.UpdateCombineQueueDepthBuckets[i] - before.UpdateCombineQueueDepthBuckets[i]
		delta.UpdateCombineBatchSizeBuckets[i] = after.UpdateCombineBatchSizeBuckets[i] - before.UpdateCombineBatchSizeBuckets[i]
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
		UpdateCombineRequests:            10,
		UpdateCombineBatches:             3,
		UpdateCombineBatchedRequests:     8,
		UpdateCombineFallbackRequests:    1,
		UpdateCombineQueueDepthMax:       12,
		UpdateCombineInlineRequests:      4,
		UpdateCombineEnqueue:             100 * time.Nanosecond,
		UpdateCombineWait:                200 * time.Nanosecond,
		UpdateCombineQueueWait:           250 * time.Nanosecond,
		UpdateCombineDrain:               300 * time.Nanosecond,
		UpdateCombineRun:                 400 * time.Nanosecond,
		UpdateCombineResultDelivery:      500 * time.Nanosecond,
		UpdateBatchStructuredApply:       50 * time.Nanosecond,
		UpdateBatchIndexStateExtract:     70 * time.Nanosecond,
		UpdateBatchOldIndexStateExtract:  30 * time.Nanosecond,
		UpdateBatchNewIndexStateExtract:  40 * time.Nanosecond,
		UpdateBatchBufferFreeze:          11 * time.Nanosecond,
		UpdateBatchBufferRootTable:       13 * time.Nanosecond,
		UpdateBatchBufferPrimaryAppend:   17 * time.Nanosecond,
		UpdateBatchBufferSecondaryAppend: 19 * time.Nanosecond,
		UpdateBatchIndexValueChanges:     20,
		UpdateBatchIndexValueUnchanged:   30,
		UpdateBatchMaskFallbacks:         5,
		UpdateBatchUniqueChecks:          4,
		UpdateBatchUniqueCheckSkips:      10,
		IndexedFlushCalls:                3,
		IndexedFlushErrors:               1,
		IndexedFlushForcedDrains:         2,
		IndexedFlushUnits:                6,
		IndexedFlushDocs:                 300,
		IndexedFlushBytes:                9000,
		IndexedFlushRootRuns:             90,
		IndexedFlushRoots:                9,
		IndexedFlushDuration:             30 * time.Millisecond,
		IndexedFlushMaterialize:          12 * time.Millisecond,
		IndexedFlushPublish:              18 * time.Millisecond,
		IndexedAsyncFlushWait:            2 * time.Millisecond,
		RootDeltaPlanPrimaryRoots:        3,
		RootDeltaPlanTemplateRoots:       2,
		RootDeltaPlanIndexStateRoots:     1,
		RootDeltaPlanSecondaryRoots:      6,
		RootDeltaPlanEntries:             300,
		RootDeltaPlanKeyBytes:            1200,
		RootDeltaPlanValueBytes:          2400,
		RootDeltaPlanTombstones:          30,
		PrimaryOnlyUpdateCalls:           10,
		PrimaryOnlyMatched:               9,
		PrimaryOnlyModified:              8,
		PrimaryOnlyBufferedCalls:         1,
		PrimaryOnlyRootPublishes:         8,
		PrimaryOnlyRootDeltaEntries:      8,
		PrimaryOnlyRootDeltaKeyBytes:     16,
		PrimaryOnlyRootDeltaValueBytes:   160,
		PrimaryOnlyCoalescedDocs:         8,
		UpdateBatchIndexStatsCount:       2,
		UpdateBatchIndexStats: [8]collections.CollectionUpdateIndexStats{
			{CollectionName: "users", IndexName: "email", IndexOrdinal: 0, Unique: true, Changed: 1, Unchanged: 9, UniqueChecks: 1, UniqueCheckSkips: 8},
			{CollectionName: "users", IndexName: "city", IndexOrdinal: 1, Changed: 10, SecondaryRuns: 10, SecondaryDeletes: 10, SecondarySets: 10, SecondaryKeyBytes: 1000},
		},
	}
	after := collections.CollectionManagerStats{
		UpdateCombineRequests:            17,
		UpdateCombineBatches:             5,
		UpdateCombineBatchedRequests:     14,
		UpdateCombineFallbackRequests:    2,
		UpdateCombineQueueDepthMax:       31,
		UpdateCombineInlineRequests:      15,
		UpdateCombineEnqueue:             700 * time.Nanosecond,
		UpdateCombineWait:                1400 * time.Nanosecond,
		UpdateCombineQueueWait:           1750 * time.Nanosecond,
		UpdateCombineDrain:               2100 * time.Nanosecond,
		UpdateCombineRun:                 2800 * time.Nanosecond,
		UpdateCombineResultDelivery:      3500 * time.Nanosecond,
		UpdateBatchStructuredApply:       500 * time.Nanosecond,
		UpdateBatchIndexStateExtract:     700 * time.Nanosecond,
		UpdateBatchOldIndexStateExtract:  300 * time.Nanosecond,
		UpdateBatchNewIndexStateExtract:  400 * time.Nanosecond,
		UpdateBatchBufferFreeze:          110 * time.Nanosecond,
		UpdateBatchBufferRootTable:       130 * time.Nanosecond,
		UpdateBatchBufferPrimaryAppend:   170 * time.Nanosecond,
		UpdateBatchBufferSecondaryAppend: 190 * time.Nanosecond,
		UpdateBatchIndexValueChanges:     27,
		UpdateBatchIndexValueUnchanged:   43,
		UpdateBatchMaskFallbacks:         12,
		UpdateBatchUniqueChecks:          6,
		UpdateBatchUniqueCheckSkips:      19,
		IndexedFlushCalls:                8,
		IndexedFlushErrors:               2,
		IndexedFlushForcedDrains:         5,
		IndexedFlushUnits:                16,
		IndexedFlushDocs:                 900,
		IndexedFlushBytes:                27000,
		IndexedFlushRootRuns:             270,
		IndexedFlushRoots:                24,
		IndexedFlushDuration:             90 * time.Millisecond,
		IndexedFlushMaterialize:          30 * time.Millisecond,
		IndexedFlushPublish:              60 * time.Millisecond,
		IndexedAsyncFlushWait:            11 * time.Millisecond,
		RootDeltaPlanPrimaryRoots:        8,
		RootDeltaPlanTemplateRoots:       5,
		RootDeltaPlanIndexStateRoots:     4,
		RootDeltaPlanSecondaryRoots:      16,
		RootDeltaPlanEntries:             900,
		RootDeltaPlanKeyBytes:            3600,
		RootDeltaPlanValueBytes:          7200,
		RootDeltaPlanTombstones:          90,
		PrimaryOnlyUpdateCalls:           17,
		PrimaryOnlyMatched:               16,
		PrimaryOnlyModified:              15,
		PrimaryOnlyBufferedCalls:         3,
		PrimaryOnlyRootPublishes:         15,
		PrimaryOnlyRootDeltaEntries:      15,
		PrimaryOnlyRootDeltaKeyBytes:     30,
		PrimaryOnlyRootDeltaValueBytes:   300,
		PrimaryOnlyCoalescedDocs:         15,
		UpdateBatchIndexStatsCount:       2,
		UpdateBatchIndexStats: [8]collections.CollectionUpdateIndexStats{
			{CollectionName: "users", IndexName: "email", IndexOrdinal: 0, Unique: true, Changed: 1, Unchanged: 22, UniqueChecks: 1, UniqueCheckSkips: 17},
			{CollectionName: "users", IndexName: "city", IndexOrdinal: 1, Changed: 17, SecondaryRuns: 17, SecondaryDeletes: 17, SecondarySets: 17, SecondaryKeyBytes: 1700},
		},
	}
	before.UpdateCombineQueueDepthBuckets[0] = 1
	before.UpdateCombineQueueDepthBuckets[3] = 2
	before.UpdateCombineBatchSizeBuckets[1] = 3
	before.UpdateCombineBatchSizeBuckets[4] = 4
	after.UpdateCombineQueueDepthBuckets[0] = 3
	after.UpdateCombineQueueDepthBuckets[3] = 7
	after.UpdateCombineBatchSizeBuckets[1] = 11
	after.UpdateCombineBatchSizeBuckets[4] = 13
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
	if got.UpdateCombineQueueDepthMax != 31 {
		t.Fatalf("UpdateCombineQueueDepthMax=%d want 31", got.UpdateCombineQueueDepthMax)
	}
	if got.UpdateCombineInlineRequests != 11 {
		t.Fatalf("UpdateCombineInlineRequests=%d want 11", got.UpdateCombineInlineRequests)
	}
	if got.UpdateCombineEnqueue != 600*time.Nanosecond || got.UpdateCombineWait != 1200*time.Nanosecond || got.UpdateCombineQueueWait != 1500*time.Nanosecond || got.UpdateCombineDrain != 1800*time.Nanosecond || got.UpdateCombineRun != 2400*time.Nanosecond || got.UpdateCombineResultDelivery != 3000*time.Nanosecond {
		t.Fatalf("update combine timing delta enqueue/wait/queue-wait/drain/run/result-delivery=%s/%s/%s/%s/%s/%s want 600ns/1200ns/1500ns/1800ns/2400ns/3000ns",
			got.UpdateCombineEnqueue,
			got.UpdateCombineWait,
			got.UpdateCombineQueueWait,
			got.UpdateCombineDrain,
			got.UpdateCombineRun,
			got.UpdateCombineResultDelivery,
		)
	}
	if got.UpdateCombineQueueDepthBuckets[0] != 2 || got.UpdateCombineQueueDepthBuckets[3] != 5 {
		t.Fatalf("update combine queue-depth bucket deltas=%v want index 0=2 index 3=5", got.UpdateCombineQueueDepthBuckets)
	}
	if got.UpdateCombineBatchSizeBuckets[1] != 8 || got.UpdateCombineBatchSizeBuckets[4] != 9 {
		t.Fatalf("update combine batch-size bucket deltas=%v want index 1=8 index 4=9", got.UpdateCombineBatchSizeBuckets)
	}
	if got.UpdateBatchStructuredApply != 450*time.Nanosecond {
		t.Fatalf("UpdateBatchStructuredApply=%s want 450ns", got.UpdateBatchStructuredApply)
	}
	if got.UpdateBatchIndexStateExtract != 630*time.Nanosecond || got.UpdateBatchOldIndexStateExtract != 270*time.Nanosecond || got.UpdateBatchNewIndexStateExtract != 360*time.Nanosecond {
		t.Fatalf("index state timing delta total/old/new=%s/%s/%s want 630ns/270ns/360ns",
			got.UpdateBatchIndexStateExtract,
			got.UpdateBatchOldIndexStateExtract,
			got.UpdateBatchNewIndexStateExtract,
		)
	}
	if got.UpdateBatchBufferFreeze != 99*time.Nanosecond || got.UpdateBatchBufferRootTable != 117*time.Nanosecond || got.UpdateBatchBufferPrimaryAppend != 153*time.Nanosecond || got.UpdateBatchBufferSecondaryAppend != 171*time.Nanosecond {
		t.Fatalf("buffer detail timing delta freeze/root-table/primary/secondary=%s/%s/%s/%s want 99ns/117ns/153ns/171ns",
			got.UpdateBatchBufferFreeze,
			got.UpdateBatchBufferRootTable,
			got.UpdateBatchBufferPrimaryAppend,
			got.UpdateBatchBufferSecondaryAppend,
		)
	}
	staleMax := deltaCollectionManagerUpdateStats(
		collections.CollectionManagerStats{UpdateCombineQueueDepthMax: 31},
		collections.CollectionManagerStats{UpdateCombineQueueDepthMax: 31},
	)
	if staleMax.UpdateCombineQueueDepthMax != 0 {
		t.Fatalf("stale UpdateCombineQueueDepthMax=%d want 0", staleMax.UpdateCombineQueueDepthMax)
	}
	if got.UpdateBatchIndexValueChanges != 7 {
		t.Fatalf("UpdateBatchIndexValueChanges=%d want 7", got.UpdateBatchIndexValueChanges)
	}
	if got.UpdateBatchIndexValueUnchanged != 13 {
		t.Fatalf("UpdateBatchIndexValueUnchanged=%d want 13", got.UpdateBatchIndexValueUnchanged)
	}
	if got.UpdateBatchMaskFallbacks != 7 {
		t.Fatalf("UpdateBatchMaskFallbacks=%d want 7", got.UpdateBatchMaskFallbacks)
	}
	if got.UpdateBatchUniqueChecks != 2 {
		t.Fatalf("UpdateBatchUniqueChecks=%d want 2", got.UpdateBatchUniqueChecks)
	}
	if got.UpdateBatchUniqueCheckSkips != 9 {
		t.Fatalf("UpdateBatchUniqueCheckSkips=%d want 9", got.UpdateBatchUniqueCheckSkips)
	}
	if got.IndexedFlushCalls != 5 || got.IndexedFlushErrors != 1 || got.IndexedFlushForcedDrains != 3 || got.IndexedFlushUnits != 10 || got.IndexedFlushDocs != 600 || got.IndexedFlushBytes != 18000 || got.IndexedFlushRootRuns != 180 || got.IndexedFlushRoots != 15 || got.IndexedFlushDuration != 60*time.Millisecond || got.IndexedFlushMaterialize != 18*time.Millisecond || got.IndexedFlushPublish != 42*time.Millisecond || got.IndexedAsyncFlushWait != 9*time.Millisecond {
		t.Fatalf("indexed flush delta calls/errors/forced/units/docs/bytes/rootRuns/roots/duration/materialize/publish/wait=%d/%d/%d/%d/%d/%d/%d/%d/%s/%s/%s/%s want 5/1/3/10/600/18000/180/15/60ms/18ms/42ms/9ms",
			got.IndexedFlushCalls,
			got.IndexedFlushErrors,
			got.IndexedFlushForcedDrains,
			got.IndexedFlushUnits,
			got.IndexedFlushDocs,
			got.IndexedFlushBytes,
			got.IndexedFlushRootRuns,
			got.IndexedFlushRoots,
			got.IndexedFlushDuration,
			got.IndexedFlushMaterialize,
			got.IndexedFlushPublish,
			got.IndexedAsyncFlushWait,
		)
	}
	if got.RootDeltaPlanPrimaryRoots != 5 || got.RootDeltaPlanTemplateRoots != 3 || got.RootDeltaPlanIndexStateRoots != 3 || got.RootDeltaPlanSecondaryRoots != 10 || got.RootDeltaPlanEntries != 600 || got.RootDeltaPlanKeyBytes != 2400 || got.RootDeltaPlanValueBytes != 4800 || got.RootDeltaPlanTombstones != 60 {
		t.Fatalf("root delta plan stats=%d/%d/%d/%d/%d/%d/%d/%d want 5/3/3/10/600/2400/4800/60",
			got.RootDeltaPlanPrimaryRoots,
			got.RootDeltaPlanTemplateRoots,
			got.RootDeltaPlanIndexStateRoots,
			got.RootDeltaPlanSecondaryRoots,
			got.RootDeltaPlanEntries,
			got.RootDeltaPlanKeyBytes,
			got.RootDeltaPlanValueBytes,
			got.RootDeltaPlanTombstones,
		)
	}
	if got.PrimaryOnlyUpdateCalls != 7 || got.PrimaryOnlyMatched != 7 || got.PrimaryOnlyModified != 7 || got.PrimaryOnlyBufferedCalls != 2 || got.PrimaryOnlyRootPublishes != 7 || got.PrimaryOnlyRootDeltaEntries != 7 || got.PrimaryOnlyRootDeltaKeyBytes != 14 || got.PrimaryOnlyRootDeltaValueBytes != 140 || got.PrimaryOnlyCoalescedDocs != 7 {
		t.Fatalf("primary-only stats calls/matched/modified/buffered/publishes/entries/key/value/coalesced=%d/%d/%d/%d/%d/%d/%d/%d/%d want 7/7/7/2/7/7/14/140/7",
			got.PrimaryOnlyUpdateCalls,
			got.PrimaryOnlyMatched,
			got.PrimaryOnlyModified,
			got.PrimaryOnlyBufferedCalls,
			got.PrimaryOnlyRootPublishes,
			got.PrimaryOnlyRootDeltaEntries,
			got.PrimaryOnlyRootDeltaKeyBytes,
			got.PrimaryOnlyRootDeltaValueBytes,
			got.PrimaryOnlyCoalescedDocs,
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

func TestReportCollectionManagerUpdateStatsIncludesIndexedFlushMetrics(t *testing.T) {
	stats := collections.CollectionManagerStats{
		IndexedFlushCalls:              5,
		IndexedFlushErrors:             1,
		IndexedFlushForcedDrains:       2,
		IndexedFlushUnits:              10,
		IndexedFlushDocs:               600,
		IndexedFlushBytes:              18000,
		IndexedFlushRootRuns:           180,
		IndexedFlushRoots:              15,
		IndexedFlushDuration:           60 * time.Millisecond,
		IndexedFlushMaterialize:        18 * time.Millisecond,
		IndexedFlushPublish:            42 * time.Millisecond,
		IndexedAsyncFlushWait:          6 * time.Millisecond,
		RootDeltaPlanPrimaryRoots:      5,
		RootDeltaPlanTemplateRoots:     2,
		RootDeltaPlanIndexStateRoots:   3,
		RootDeltaPlanSecondaryRoots:    10,
		RootDeltaPlanEntries:           600,
		RootDeltaPlanKeyBytes:          2400,
		RootDeltaPlanValueBytes:        4800,
		RootDeltaPlanTombstones:        60,
		PrimaryOnlyUpdateCalls:         600,
		PrimaryOnlyMatched:             600,
		PrimaryOnlyModified:            600,
		PrimaryOnlyBufferedCalls:       30,
		PrimaryOnlyRootPublishes:       600,
		PrimaryOnlyRootDeltaEntries:    600,
		PrimaryOnlyRootDeltaKeyBytes:   1200,
		PrimaryOnlyRootDeltaValueBytes: 12000,
		PrimaryOnlyCoalescedDocs:       600,
		UpdateBatchStructuredApply:     1800 * time.Nanosecond,
		UpdateBatchIndexValueChanges:   300,
		UpdateBatchIndexValueUnchanged: 900,
		UpdateBatchMaskFallbacks:       60,
	}
	result := testing.Benchmark(func(b *testing.B) {
		reportCollectionManagerUpdateStats(b, stats, 600)
	})
	want := map[string]float64{
		"indexed_flush_calls":                   5,
		"indexed_flush_units/call":              2,
		"indexed_flush_units/batch":             2,
		"indexed_flush_units/doc":               1.0 / 60.0,
		"indexed_flush_docs/call":               120,
		"indexed_flush_docs/batch":              120,
		"indexed_flush_docs/doc":                1,
		"indexed_flush_docs/unit":               60,
		"indexed_flush_bytes/call":              3600,
		"indexed_flush_bytes/doc":               30,
		"indexed_flush_root_runs/call":          36,
		"indexed_flush_root_runs/doc":           0.3,
		"indexed_flush_roots/call":              3,
		"indexed_flush_roots/doc":               0.025,
		"indexed_flush_errors":                  1,
		"indexed_flush_forced_drains":           2,
		"indexed_flush_forced_drains/doc":       1.0 / 300.0,
		"indexed_flush_ns/call":                 12_000_000,
		"indexed_flush_ns/doc":                  100_000,
		"indexed_flush_materialize_ns/call":     3_600_000,
		"indexed_flush_materialize_ns/doc":      30_000,
		"indexed_flush_publish_ns/call":         8_400_000,
		"indexed_flush_publish_ns/doc":          70_000,
		"indexed_async_flush_wait_ns":           6_000_000,
		"indexed_async_flush_wait_ns/doc":       10_000,
		"root_delta_plan_entries/doc":           1,
		"root_delta_plan_key_bytes/doc":         4,
		"root_delta_plan_value_bytes/doc":       8,
		"root_delta_plan_tombstones/doc":        0.1,
		"affected_primary_roots/doc":            1.0 / 120.0,
		"affected_template_roots/doc":           1.0 / 300.0,
		"affected_index_state_roots/doc":        0.005,
		"affected_secondary_roots/doc":          1.0 / 60.0,
		"primary_only_update_calls/doc":         1,
		"primary_only_matched/doc":              1,
		"primary_only_modified/doc":             1,
		"primary_only_buffered_calls":           30,
		"primary_only_buffered_calls/doc":       0.05,
		"primary_root_publishes/doc":            1,
		"primary_root_delta_entries/doc":        1,
		"primary_root_delta_bytes/doc":          22,
		"primary_only_coalesced_docs/publish":   1,
		"update_structured_apply_ns/doc":        3,
		"update_index_value_changes/doc":        0.5,
		"update_index_value_unchanged/doc":      1.5,
		"changed_index_fast_mask_fallbacks/doc": 0.1,
	}
	for metric, wantValue := range want {
		gotValue, ok := result.Extra[metric]
		if !ok {
			t.Fatalf("missing benchmark metric %q in %v", metric, result.Extra)
		}
		if math.Abs(gotValue-wantValue) > 1e-9 {
			t.Fatalf("benchmark metric %s=%g want %g", metric, gotValue, wantValue)
		}
	}
}

func TestReportCollectionManagerUpdateStatsIncludesCombinerQueueDepth(t *testing.T) {
	stats := collections.CollectionManagerStats{
		UpdateCombineRequests:         600,
		UpdateCombineBatches:          30,
		UpdateCombineBatchedRequests:  600,
		UpdateCombineFallbackRequests: 6,
		UpdateCombineQueueDepthMax:    31,
		UpdateCombineInlineRequests:   300,
		UpdateCombineEnqueue:          600 * time.Nanosecond,
		UpdateCombineWait:             1200 * time.Nanosecond,
		UpdateCombineQueueWait:        3000 * time.Nanosecond,
		UpdateCombineDrain:            1800 * time.Nanosecond,
		UpdateCombineRun:              2400 * time.Nanosecond,
		UpdateCombineResultDelivery:   3600 * time.Nanosecond,
	}
	stats.UpdateCombineQueueDepthBuckets[0] = 60
	stats.UpdateCombineQueueDepthBuckets[4] = 540
	stats.UpdateCombineBatchSizeBuckets[2] = 6
	stats.UpdateCombineBatchSizeBuckets[5] = 24
	result := testing.Benchmark(func(b *testing.B) {
		reportCollectionManagerUpdateStats(b, stats, 600)
	})
	want := map[string]float64{
		"update_combine_requests":                         600,
		"update_combine_requests/doc":                     1,
		"update_combine_batches":                          30,
		"update_combine_items/batch":                      20,
		"update_combine_batched_requests":                 600,
		"update_combine_batched_requests/doc":             1,
		"update_combine_fallback_requests":                6,
		"update_combine_fallback_requests/doc":            0.01,
		"update_combine_queue_depth_max":                  31,
		"update_combine_queue_depth_bucket_le_1":          60,
		"update_combine_queue_depth_bucket_le_1/request":  0.1,
		"update_combine_queue_depth_bucket_le_16":         540,
		"update_combine_queue_depth_bucket_le_16/request": 0.9,
		"update_combine_batch_size_bucket_le_4":           6,
		"update_combine_batch_size_bucket_le_4/batch":     0.2,
		"update_combine_batch_size_bucket_le_32":          24,
		"update_combine_batch_size_bucket_le_32/batch":    0.8,
		"update_combine_inline_requests":                  300,
		"update_combine_inline_requests/doc":              0.5,
		"update_combine_enqueue_ns/doc":                   1,
		"update_combine_wait_ns/doc":                      2,
		"update_combine_queue_wait_ns/doc":                5,
		"update_combine_drain_ns/doc":                     3,
		"update_combine_run_ns/doc":                       4,
		"update_combine_result_delivery_ns/doc":           6,
	}
	for metric, wantValue := range want {
		gotValue, ok := result.Extra[metric]
		if !ok {
			t.Fatalf("missing benchmark metric %q in %v", metric, result.Extra)
		}
		if math.Abs(gotValue-wantValue) > 1e-9 {
			t.Fatalf("benchmark metric %s=%g want %g", metric, gotValue, wantValue)
		}
	}
}

func reportCollectionManagerUpdateStats(b *testing.B, stats collections.CollectionManagerStats, docs int) {
	b.Helper()
	if docs <= 0 {
		return
	}
	if stats.IndexedFlushCalls > 0 {
		b.ReportMetric(float64(stats.IndexedFlushCalls), "indexed_flush_calls")
		if stats.IndexedFlushUnits > 0 {
			b.ReportMetric(float64(stats.IndexedFlushUnits)/float64(stats.IndexedFlushCalls), "indexed_flush_units/call")
			b.ReportMetric(float64(stats.IndexedFlushUnits)/float64(stats.IndexedFlushCalls), "indexed_flush_units/batch")
			b.ReportMetric(float64(stats.IndexedFlushUnits)/float64(docs), "indexed_flush_units/doc")
		}
		if stats.IndexedFlushDocs > 0 {
			b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(stats.IndexedFlushCalls), "indexed_flush_docs/call")
			b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(stats.IndexedFlushCalls), "indexed_flush_docs/batch")
			b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(docs), "indexed_flush_docs/doc")
		}
		if stats.IndexedFlushUnits > 0 && stats.IndexedFlushDocs > 0 {
			b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(stats.IndexedFlushUnits), "indexed_flush_docs/unit")
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
		if stats.IndexedFlushForcedDrains > 0 {
			b.ReportMetric(float64(stats.IndexedFlushForcedDrains), "indexed_flush_forced_drains")
			b.ReportMetric(float64(stats.IndexedFlushForcedDrains)/float64(docs), "indexed_flush_forced_drains/doc")
		}
		if stats.IndexedFlushDuration > 0 {
			b.ReportMetric(float64(stats.IndexedFlushDuration.Nanoseconds())/float64(stats.IndexedFlushCalls), "indexed_flush_ns/call")
			if stats.IndexedFlushDocs > 0 {
				b.ReportMetric(float64(stats.IndexedFlushDuration.Nanoseconds())/float64(stats.IndexedFlushDocs), "indexed_flush_ns/doc")
			}
		}
		if stats.IndexedFlushMaterialize > 0 {
			b.ReportMetric(float64(stats.IndexedFlushMaterialize.Nanoseconds())/float64(stats.IndexedFlushCalls), "indexed_flush_materialize_ns/call")
			if stats.IndexedFlushDocs > 0 {
				b.ReportMetric(float64(stats.IndexedFlushMaterialize.Nanoseconds())/float64(stats.IndexedFlushDocs), "indexed_flush_materialize_ns/doc")
			}
		}
		if stats.IndexedFlushPublish > 0 {
			b.ReportMetric(float64(stats.IndexedFlushPublish.Nanoseconds())/float64(stats.IndexedFlushCalls), "indexed_flush_publish_ns/call")
			if stats.IndexedFlushDocs > 0 {
				b.ReportMetric(float64(stats.IndexedFlushPublish.Nanoseconds())/float64(stats.IndexedFlushDocs), "indexed_flush_publish_ns/doc")
			}
		}
	}
	if stats.IndexedAsyncFlushWait > 0 {
		b.ReportMetric(float64(stats.IndexedAsyncFlushWait.Nanoseconds()), "indexed_async_flush_wait_ns")
		b.ReportMetric(float64(stats.IndexedAsyncFlushWait.Nanoseconds())/float64(docs), "indexed_async_flush_wait_ns/doc")
	}
	if stats.OverlayMutableDocuments > 0 {
		b.ReportMetric(float64(stats.OverlayMutableDocuments), "overlay_mutable_docs")
	}
	if stats.OverlayQueuedIndexedFlushUnits > 0 {
		b.ReportMetric(float64(stats.OverlayQueuedIndexedFlushUnits), "overlay_queued_indexed_flush_units")
	}
	if stats.OverlayActiveIndexedFlushUnits > 0 {
		b.ReportMetric(float64(stats.OverlayActiveIndexedFlushUnits), "overlay_active_indexed_flush_units")
	}
	if stats.OverlayVisibleDepth > 0 {
		b.ReportMetric(float64(stats.OverlayVisibleDepth), "overlay_visible_depth")
	}
	if stats.RootDeltaPlanEntries > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanEntries)/float64(docs), "root_delta_plan_entries/doc")
	}
	if stats.RootDeltaPlanKeyBytes > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanKeyBytes)/float64(docs), "root_delta_plan_key_bytes/doc")
	}
	if stats.RootDeltaPlanValueBytes > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanValueBytes)/float64(docs), "root_delta_plan_value_bytes/doc")
	}
	if stats.RootDeltaPlanTombstones > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanTombstones)/float64(docs), "root_delta_plan_tombstones/doc")
	}
	if stats.RootDeltaPlanPrimaryRoots > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanPrimaryRoots)/float64(docs), "affected_primary_roots/doc")
	}
	if stats.RootDeltaPlanTemplateRoots > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanTemplateRoots)/float64(docs), "affected_template_roots/doc")
	}
	if stats.RootDeltaPlanIndexStateRoots > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanIndexStateRoots)/float64(docs), "affected_index_state_roots/doc")
	}
	if stats.RootDeltaPlanSecondaryRoots > 0 {
		b.ReportMetric(float64(stats.RootDeltaPlanSecondaryRoots)/float64(docs), "affected_secondary_roots/doc")
	}
	if stats.PrimaryOnlyUpdateCalls > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyUpdateCalls)/float64(docs), "primary_only_update_calls/doc")
	}
	if stats.PrimaryOnlyMatched > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyMatched)/float64(docs), "primary_only_matched/doc")
	}
	if stats.PrimaryOnlyModified > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyModified)/float64(docs), "primary_only_modified/doc")
	}
	if stats.PrimaryOnlyBufferedCalls > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyBufferedCalls), "primary_only_buffered_calls")
		b.ReportMetric(float64(stats.PrimaryOnlyBufferedCalls)/float64(docs), "primary_only_buffered_calls/doc")
	}
	if stats.PrimaryOnlyRootPublishes > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyRootPublishes)/float64(docs), "primary_root_publishes/doc")
	}
	if stats.PrimaryOnlyRootDeltaEntries > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyRootDeltaEntries)/float64(docs), "primary_root_delta_entries/doc")
	}
	if stats.PrimaryOnlyRootDeltaKeyBytes+stats.PrimaryOnlyRootDeltaValueBytes > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyRootDeltaKeyBytes+stats.PrimaryOnlyRootDeltaValueBytes)/float64(docs), "primary_root_delta_bytes/doc")
	}
	if stats.PrimaryOnlyRootPublishes > 0 {
		b.ReportMetric(float64(stats.PrimaryOnlyCoalescedDocs)/float64(stats.PrimaryOnlyRootPublishes), "primary_only_coalesced_docs/publish")
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
	if stats.UpdateCombineQueueDepthMax > 0 {
		b.ReportMetric(float64(stats.UpdateCombineQueueDepthMax), "update_combine_queue_depth_max")
	}
	for i, count := range stats.UpdateCombineQueueDepthBuckets {
		if count == 0 || stats.UpdateCombineRequests == 0 {
			continue
		}
		label := collections.CollectionUpdateCombineBucketLabel(i)
		if label == "" {
			continue
		}
		name := "update_combine_queue_depth_bucket_" + label
		b.ReportMetric(float64(count), name)
		b.ReportMetric(float64(count)/float64(stats.UpdateCombineRequests), name+"/request")
	}
	for i, count := range stats.UpdateCombineBatchSizeBuckets {
		if count == 0 || stats.UpdateCombineBatches == 0 {
			continue
		}
		label := collections.CollectionUpdateCombineBucketLabel(i)
		if label == "" {
			continue
		}
		name := "update_combine_batch_size_bucket_" + label
		b.ReportMetric(float64(count), name)
		b.ReportMetric(float64(count)/float64(stats.UpdateCombineBatches), name+"/batch")
	}
	if stats.UpdateCombineInlineRequests > 0 {
		b.ReportMetric(float64(stats.UpdateCombineInlineRequests), "update_combine_inline_requests")
		b.ReportMetric(float64(stats.UpdateCombineInlineRequests)/float64(docs), "update_combine_inline_requests/doc")
	}
	reportDuration := func(name string, d time.Duration) {
		if d > 0 {
			b.ReportMetric(float64(d.Nanoseconds())/float64(docs), name)
		}
	}
	reportDuration("update_combine_enqueue_ns/doc", stats.UpdateCombineEnqueue)
	reportDuration("update_combine_wait_ns/doc", stats.UpdateCombineWait)
	reportDuration("update_combine_queue_wait_ns/doc", stats.UpdateCombineQueueWait)
	reportDuration("update_combine_drain_ns/doc", stats.UpdateCombineDrain)
	reportDuration("update_combine_run_ns/doc", stats.UpdateCombineRun)
	reportDuration("update_combine_result_delivery_ns/doc", stats.UpdateCombineResultDelivery)
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
	if stats.UpdateBatchMaskFallbacks > 0 {
		b.ReportMetric(float64(stats.UpdateBatchMaskFallbacks)/float64(docs), "changed_index_fast_mask_fallbacks/doc")
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
	reportDuration("update_current_read_ns/doc", stats.UpdateBatchCurrentRead)
	reportDuration("update_callback_ns/doc", stats.UpdateBatchCallback)
	reportDuration("update_structured_apply_ns/doc", stats.UpdateBatchStructuredApply)
	reportDuration("update_prepare_ns/doc", stats.UpdateBatchPrepareDocuments)
	reportDuration("update_index_state_extract_ns/doc", stats.UpdateBatchIndexStateExtract)
	reportDuration("update_old_index_state_extract_ns/doc", stats.UpdateBatchOldIndexStateExtract)
	reportDuration("update_new_index_state_extract_ns/doc", stats.UpdateBatchNewIndexStateExtract)
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
	reportDuration("update_buffer_freeze_ns/doc", stats.UpdateBatchBufferFreeze)
	reportDuration("update_buffer_root_table_ns/doc", stats.UpdateBatchBufferRootTable)
	reportDuration("update_buffer_primary_index_ns/doc", stats.UpdateBatchBufferPrimaryIdx)
	reportDuration("update_buffer_unique_index_ns/doc", stats.UpdateBatchBufferUniqueIdx)
	reportDuration("update_buffer_primary_append_ns/doc", stats.UpdateBatchBufferPrimaryAppend)
	reportDuration("update_buffer_secondary_append_ns/doc", stats.UpdateBatchBufferSecondaryAppend)
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
			pprof.SetGoroutineLabels(runCtx)
			setFields := make([]collections.BSONSetField, 0, 8)
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
				setFields = profileBenchCollectionSetFieldsForOperation(setFields[:0], updateDoc, op, documentOrdinal, documentCount)
				matched, _, err := collection.UpdateBSONSet(id, setFields)
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

func profileBenchCollectionSetFieldsForOperation(dst []collections.BSONSetField, update profileBenchSetUpdate, operation, documentOrdinal, documentCount int) []collections.BSONSetField {
	for _, field := range update.fields {
		dst = append(dst, collections.BSONSetField{
			Key:   field.key,
			Value: profileBenchSetFieldValueForOperation(field, operation, documentOrdinal, documentCount),
		})
	}
	return dst
}

type profileBenchSetField struct {
	key           string
	keyBytes      []byte
	value         bson.RawValue
	dynamicValues []bson.RawValue
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
	if len(field.dynamicValues) == 0 || operation < 0 {
		return field.value
	}
	index := benchmarkUpdatedCityIndex(operation, documentOrdinal, documentCount, len(field.dynamicValues))
	if documentCount > 0 && operation >= documentCount {
		previousIndex := benchmarkUpdatedCityIndex(operation-documentCount, documentOrdinal, documentCount, len(field.dynamicValues))
		index = avoidBenchmarkUpdatedCityRepeat(index, previousIndex, len(field.dynamicValues))
	}
	return field.dynamicValues[index]
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
		TreeDBProfile:               treedb.ProfileBenchUnsafe,
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
		TreeDBProfile:               treedb.ProfileBenchUnsafe,
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
		TreeDBProfile:               treedb.ProfileBenchUnsafe,
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
		command, err := rawInsertCommand(cfg.Collection, 0, count, documentShapeGateway, nil, rawDocs)
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

func profileBenchUpdateCombineShards(tb testing.TB) int {
	return profileBenchPositiveEnvInt(tb, "MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_SHARDS", 1)
}

func profileBenchUpdateCombineLaneWorkers(tb testing.TB) bool {
	return profileBenchBoolEnv(tb, "MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_LANE_WORKERS", false)
}

func profileBenchEffectiveUpdateCombineLaneWorkers(shards int, laneWorkers bool) bool {
	return laneWorkers && shards > 1
}

func profileBenchBufferedIndexedAsyncFlush(tb testing.TB) bool {
	return profileBenchBoolEnv(tb, "MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH", false)
}

func profileBenchDisableBufferedIndexedAsyncFlush(tb testing.TB) bool {
	return profileBenchBoolEnv(tb, "MONGO_GATEWAY_PROFILE_BENCH_DISABLE_BUFFERED_INDEXED_ASYNC_FLUSH", false)
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

func profileBenchBufferedIndexedOverlayRoots(tb testing.TB) bool {
	return profileBenchBoolEnv(tb, "MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_OVERLAY_ROOTS", false)
}

func profileBenchCompactOverlayRootsAfterFlush(tb testing.TB) bool {
	return profileBenchBoolEnv(tb, "MONGO_GATEWAY_PROFILE_BENCH_COMPACT_OVERLAY_ROOTS_AFTER_FLUSH", false)
}

func profileBenchCollectionOptions(tb testing.TB, format collections.DocumentFormat) collections.CollectionOptions {
	tb.Helper()
	disableAsyncFlush := profileBenchDisableBufferedIndexedAsyncFlush(tb)
	asyncFlush := profileBenchBufferedIndexedAsyncFlush(tb)
	asyncFlushMaxQueuedUnits := profileBenchBufferedIndexedAsyncFlushMaxQueuedUnits(tb)
	if disableAsyncFlush && asyncFlush {
		tb.Fatalf("cannot set both MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH and MONGO_GATEWAY_PROFILE_BENCH_DISABLE_BUFFERED_INDEXED_ASYNC_FLUSH")
	}
	if disableAsyncFlush && asyncFlushMaxQueuedUnits != 0 {
		tb.Fatalf("cannot set MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS when MONGO_GATEWAY_PROFILE_BENCH_DISABLE_BUFFERED_INDEXED_ASYNC_FLUSH is set")
	}
	return collections.CollectionOptions{
		DocumentFormat:                          format,
		DataRootStoragePolicy:                   collections.RootStorageCompressed,
		IndexStateStoragePolicy:                 collections.RootStorageCompressed,
		BufferedIndexedWriteMaxDocuments:        profileBenchBufferedIndexedWriteMaxDocuments(tb),
		BufferedIndexedWriteMaxBytes:            profileBenchBufferedIndexedWriteMaxBytes(tb),
		BufferedIndexedWriteMaxRootRuns:         profileBenchBufferedIndexedWriteMaxRootRuns(tb),
		DisableBufferedIndexedAsyncFlush:        disableAsyncFlush,
		BufferedIndexedAsyncFlush:               asyncFlush,
		BufferedIndexedOverlayRoots:             profileBenchBufferedIndexedOverlayRoots(tb),
		BufferedIndexedAsyncFlushMaxQueuedUnits: asyncFlushMaxQueuedUnits,
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
	if opts.BufferedIndexedOverlayRoots {
		b.ReportMetric(1, "buffered_overlay_roots")
	}
	if profileBenchCompactOverlayRootsAfterFlush(b) {
		b.ReportMetric(1, "compact_overlay_roots_after_flush")
	}
}

func reportProfileBenchOverlayCompactionStats(b *testing.B, phase string, stats collections.CollectionRootOverlayCompactionStats) {
	b.Helper()
	if !profileBenchCompactOverlayRootsAfterFlush(b) {
		return
	}
	if phase == "" {
		phase = "unknown"
	}
	b.ReportMetric(float64(stats.Roots), phase+"_overlay_compact_roots")
	b.ReportMetric(float64(stats.OverlayRoots), phase+"_overlay_compact_overlay_roots")
}

func reportDocsPerSecond(b *testing.B, docs int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(docs)/elapsed.Seconds(), "docs/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
}
