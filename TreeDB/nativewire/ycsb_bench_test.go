package nativewire

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	ycsbBenchCollection = "usertable"
	ycsbBenchKeyField   = "_ycsb_key"
	ycsbBenchKeyIndex   = "_ycsb_key_1"
	ycsbBenchClients    = 16
	ycsbBenchFields     = 10
	ycsbBenchFieldLen   = 100

	ycsbBenchProfileEnv                = "TREEDB_NATIVEWIRE_YCSB_PROFILE"
	ycsbTimedCPUProfilePathEnv         = "TREEDB_NATIVEWIRE_YCSB_TIMED_CPU_PROFILE_PATH"
	maxYCSBPrecomputedRequests         = 500_000
	maxProfiledYCSBPrecomputedRequests = 1_000_000
)

var (
	ycsbBenchFieldNames       = ycsbFieldNames()
	ycsbBenchFieldValue       = ycsbFieldValue()
	ycsbBenchFieldValueString = base64.StdEncoding.EncodeToString(ycsbBenchFieldValue)
	ycsbTemplateFields        = append([]string{ycsbBenchKeyField}, ycsbBenchFieldNames...)
)

// BenchmarkNativewireYCSBLoad exercises the treedb-native load shape used by
// go-ycsb's default workload: one InsertBatch request per document, 16 client
// connections, a 10x100-byte row, AckVisible, and a unique _ycsb_key index.
//
// The JSON case intentionally mirrors the original go-ycsb treedb-native
// client: json.Marshal(map[string]any) over []byte field values, which stores
// those fields as base64 JSON strings. BSON and template-v1 cover alternate
// native encodings for the same logical row shape.
func BenchmarkNativewireYCSBLoad(b *testing.B) {
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(logOutput)

	for _, transport := range []string{"tcp", "inproc"} {
		transport := transport
		for _, tc := range []ycsbLoadFormat{
			{name: "json_go_ycsb", format: collections.DocumentFormatJSON, encode: encodeYCSBJSONDocument},
			{name: "bson_binary", format: collections.DocumentFormatBSON, encode: encodeYCSBBSONBinaryDocument},
			{name: "bson_base64_strings", format: collections.DocumentFormatBSON, encode: encodeYCSBBSONBase64StringDocument},
			{name: "template_v1_base64_strings", format: collections.DocumentFormatTemplateV1, encode: encodeYCSBTemplateV1Document},
		} {
			tc := tc
			b.Run(transport+"/"+tc.name, func(b *testing.B) {
				benchmarkNativewireYCSBLoad(b, transport, tc)
			})
		}
	}
}

// BenchmarkNativewireYCSBLoadServerPrecomputed keeps the YCSB load shape but
// moves client-side request construction and document encoding out of the timed
// region. It is intended for CPU, alloc, block, and mutex profiles of the
// server-side nativewire dispatch plus collection insert path.
func BenchmarkNativewireYCSBLoadServerPrecomputed(b *testing.B) {
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(logOutput)

	tc := ycsbLoadFormat{name: "bson_binary", format: collections.DocumentFormatBSON, encode: encodeYCSBBSONBinaryDocument}
	b.Run("inproc/"+tc.name, func(b *testing.B) {
		benchmarkNativewireYCSBLoadServerPrecomputed(b, tc)
	})
}

// BenchmarkNativewireYCSBUpdateServerPrecomputed exercises the YCSB update
// shape after load. The replace path models the current external client cost:
// a GetMany request followed by a full-document ReplaceBatch. The set path
// models the native structured BSON update command.
func BenchmarkNativewireYCSBUpdateServerPrecomputed(b *testing.B) {
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(logOutput)

	for _, mode := range []string{"get_many_replace_batch", "update_bson_set"} {
		mode := mode
		b.Run("inproc/bson_binary/"+mode, func(b *testing.B) {
			benchmarkNativewireYCSBUpdateServerPrecomputed(b, mode)
		})
	}
}

func TestYCSBBenchTreeDBProfileFromEnv(t *testing.T) {
	t.Setenv(ycsbBenchProfileEnv, "")
	if got := ycsbBenchTreeDBProfile(t); got != treedb.ProfileCommandWALRelaxed {
		t.Fatalf("default profile=%q want %q", got, treedb.ProfileCommandWALRelaxed)
	}

	t.Setenv(ycsbBenchProfileEnv, string(treedb.ProfileCommandWALRelaxed))
	if got := ycsbBenchTreeDBProfile(t); got != treedb.ProfileCommandWALRelaxed {
		t.Fatalf("env profile=%q want %q", got, treedb.ProfileCommandWALRelaxed)
	}
}

func TestYCSBBenchTreeDBProfileRejectsDeprecatedPublicNames(t *testing.T) {
	for _, raw := range []string{"fast", "wal_on_fast", "walonfast", "durable", "legacy_wal_durable", "legacy_wal_relaxed_fast"} {
		if got, ok := parseYCSBBenchTreeDBProfile(raw); ok {
			t.Fatalf("deprecated profile %q parsed as %q", raw, got)
		}
	}
}

type ycsbLoadFormat struct {
	name   string
	format collections.DocumentFormat
	encode func(key string) ([]byte, error)
}

type ycsbBenchEnv struct {
	server  *Server
	cleanup func() error
	address string
}

func benchmarkNativewireYCSBLoad(b *testing.B, transport string, format ycsbLoadFormat) {
	env := newYCSBBenchEnv(b, format.format, transport == "tcp")
	defer func() {
		if err := env.cleanup(); err != nil {
			b.Fatalf("cleanup: %v", err)
		}
	}()

	clients := make([]*Client, ycsbBenchClients)
	handles := make([]CollectionHandle, ycsbBenchClients)
	cleanups := make([]func() error, ycsbBenchClients)
	ctx := context.Background()
	for i := range clients {
		client, cleanup := newYCSBBenchClient(b, ctx, env, transport)
		handle, err := client.OpenCollection(ctx, ycsbBenchCollection)
		if err != nil {
			_ = cleanup()
			b.Fatalf("OpenCollection: %v", err)
		}
		clients[i] = client
		handles[i] = handle
		cleanups[i] = cleanup
	}
	defer func() {
		for _, cleanup := range cleanups {
			if cleanup != nil {
				_ = cleanup()
			}
		}
	}()

	sample, err := format.encode(ycsbBenchKey(0))
	if err != nil {
		b.Fatalf("encode sample: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(sample)))
	b.ResetTimer()

	var failed atomic.Bool
	var firstErr error
	var firstErrOnce sync.Once
	recordErr := func(err error) {
		if err == nil {
			return
		}
		failed.Store(true)
		firstErrOnce.Do(func() {
			firstErr = err
		})
	}

	var wg sync.WaitGroup
	base := 0
	for worker := 0; worker < ycsbBenchClients; worker++ {
		count := b.N / ycsbBenchClients
		if worker < b.N%ycsbBenchClients {
			count++
		}
		start := base
		base += count
		if count == 0 {
			continue
		}
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([][]byte, 1)
			docs := make([][]byte, 1)
			for i := 0; i < count && !failed.Load(); i++ {
				key := ycsbBenchKey(start + i)
				doc, err := format.encode(key)
				if err != nil {
					recordErr(fmt.Errorf("encode %s: %w", key, err))
					return
				}
				ids[0] = []byte(key)
				docs[0] = doc
				if err := ycsbInsertBatchNoResultIDs(ctx, clients[worker], handles[worker], format.format, ids, docs); err != nil {
					recordErr(fmt.Errorf("insert %s: %w", key, err))
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
	b.ReportMetric(float64(len(sample)), "document_bytes")
	b.ReportMetric(float64(ycsbBenchClients), "clients")
}

func benchmarkNativewireYCSBLoadServerPrecomputed(b *testing.B, format ycsbLoadFormat) {
	requireSafeYCSBPrecomputedRequests(b)
	env := newYCSBBenchEnv(b, format.format, false)
	defer func() {
		if err := env.cleanup(); err != nil {
			b.Fatalf("cleanup: %v", err)
		}
	}()

	col, err := env.server.collections.OpenCollection(ycsbBenchCollection)
	if err != nil {
		b.Fatalf("OpenCollection: %v", err)
	}
	workers := make([]ycsbPrecomputedNativewireWorker, ycsbBenchClients)
	base := 0
	for worker := 0; worker < ycsbBenchClients; worker++ {
		count := b.N / ycsbBenchClients
		if worker < b.N%ycsbBenchClients {
			count++
		}
		state := benchmarkConnState()
		handle := benchmarkAddCollectionHandle(b, state, env.server, ycsbBenchCollection, col)
		workers[worker] = ycsbPrecomputedNativewireWorker{
			state:  state,
			bodies: precomputeYCSBInsertRequests(b, env.server, handle, format, base, count),
		}
		base += count
	}

	sample, err := format.encode(ycsbBenchKey(0))
	if err != nil {
		b.Fatalf("encode sample: %v", err)
	}
	beforeStats := env.server.collections.StatsSnapshot()
	beforeNativeCounters := env.server.counters.snapshot()
	b.ReportAllocs()
	b.SetBytes(int64(len(sample)))
	b.ResetTimer()
	b.StopTimer()
	stopProfile := startTimedYCSBCPUProfile(b)
	profileActive := true
	defer func() {
		if profileActive {
			stopProfile()
		}
	}()

	var failed atomic.Bool
	var firstErr error
	var firstErrOnce sync.Once
	recordErr := func(err error) {
		if err == nil {
			return
		}
		failed.Store(true)
		firstErrOnce.Do(func() {
			firstErr = err
		})
	}

	var wg sync.WaitGroup
	b.StartTimer()
	for worker := range workers {
		if len(workers[worker].bodies) == 0 {
			continue
		}
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink := benchmarkFrameSink{}
			for _, body := range workers[worker].bodies {
				if failed.Load() {
					return
				}
				if _, err := benchmarkDispatchRequestError(env.server, workers[worker].state, &sink, body); err != nil {
					recordErr(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	profileActive = false
	stopProfile()
	afterStats := env.server.collections.StatsSnapshot()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
	b.ReportMetric(float64(len(sample)), "document_bytes")
	b.ReportMetric(float64(ycsbBenchClients), "clients")
	reportYCSBCollectionStatsDelta(b, beforeStats, afterStats, b.N)
	reportYCSBNativeCounterDelta(b, beforeNativeCounters, env.server.counters.snapshot(), b.N)
}

type ycsbPrecomputedNativewireWorker struct {
	state  *connState
	bodies [][]byte
}

type ycsbPrecomputedUpdateWorker struct {
	state    *connState
	requests []ycsbPrecomputedUpdateRequest
}

type ycsbPrecomputedUpdateRequest struct {
	read     []byte
	mutation []byte
}

func precomputeYCSBInsertRequests(b *testing.B, server *Server, handle CollectionHandle, format ycsbLoadFormat, start, count int) [][]byte {
	b.Helper()
	bodies := make([][]byte, count)
	ids := make([][]byte, 1)
	docs := make([][]byte, 1)
	for i := 0; i < count; i++ {
		key := ycsbBenchKey(start + i)
		doc, err := format.encode(key)
		if err != nil {
			b.Fatalf("encode %s: %v", key, err)
		}
		ids[0] = []byte(key)
		docs[0] = doc
		guard := benchmarkMutationGuard(b, server, "insert_batch_precomputed", start+i)
		body, err := appendInsertBatchRequestBodyRefFlags(nil, "", handle, true, format.format, ids, docs, AckVisible, iwire.CommandFlagOmitResultIDs|iwire.CommandFlagOmitResponseMeta, guard)
		if err != nil {
			b.Fatalf("append insert request: %v", err)
		}
		bodies[i] = body
	}
	return bodies
}

func benchmarkNativewireYCSBUpdateServerPrecomputed(b *testing.B, mode string) {
	requireSafeYCSBPrecomputedRequests(b)
	format := ycsbLoadFormat{name: "bson_binary", format: collections.DocumentFormatBSON, encode: encodeYCSBBSONBinaryDocument}
	env := newYCSBBenchEnv(b, format.format, false)
	defer func() {
		if err := env.cleanup(); err != nil {
			b.Fatalf("cleanup: %v", err)
		}
	}()

	col, err := env.server.collections.OpenCollection(ycsbBenchCollection)
	if err != nil {
		b.Fatalf("OpenCollection: %v", err)
	}
	preloadYCSBBSONDocuments(b, col, 10_000)

	workers := make([]ycsbPrecomputedUpdateWorker, ycsbBenchClients)
	base := 0
	for worker := 0; worker < ycsbBenchClients; worker++ {
		count := b.N / ycsbBenchClients
		if worker < b.N%ycsbBenchClients {
			count++
		}
		state := benchmarkConnState()
		handle := benchmarkAddCollectionHandle(b, state, env.server, ycsbBenchCollection, col)
		workers[worker] = ycsbPrecomputedUpdateWorker{
			state:    state,
			requests: precomputeYCSBUpdateRequests(b, env.server, handle, mode, base, count, 10_000),
		}
		base += count
	}

	sample, err := encodeYCSBBSONBinaryDocument(ycsbBenchKey(0))
	if err != nil {
		b.Fatalf("encode sample: %v", err)
	}
	beforeStats := env.server.collections.StatsSnapshot()
	beforeNativeCounters := env.server.counters.snapshot()
	b.ReportAllocs()
	b.SetBytes(int64(len(sample)))
	b.ResetTimer()
	b.StopTimer()
	stopProfile := startTimedYCSBCPUProfile(b)
	profileActive := true
	defer func() {
		if profileActive {
			stopProfile()
		}
	}()

	var failed atomic.Bool
	var firstErr error
	var firstErrOnce sync.Once
	recordErr := func(err error) {
		if err == nil {
			return
		}
		failed.Store(true)
		firstErrOnce.Do(func() {
			firstErr = err
		})
	}

	var wg sync.WaitGroup
	b.StartTimer()
	for worker := range workers {
		if len(workers[worker].requests) == 0 {
			continue
		}
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink := benchmarkFrameSink{}
			for _, request := range workers[worker].requests {
				if failed.Load() {
					return
				}
				if request.read != nil {
					if _, err := benchmarkDispatchRequestError(env.server, workers[worker].state, &sink, request.read); err != nil {
						recordErr(err)
						return
					}
				}
				if _, err := benchmarkDispatchRequestError(env.server, workers[worker].state, &sink, request.mutation); err != nil {
					recordErr(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	profileActive = false
	stopProfile()
	afterStats := env.server.collections.StatsSnapshot()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
	b.ReportMetric(float64(len(sample)), "document_bytes")
	b.ReportMetric(float64(ycsbBenchClients), "clients")
	reportYCSBCollectionStatsDelta(b, beforeStats, afterStats, b.N)
	reportYCSBUpdateNativeCounterDelta(b, beforeNativeCounters, env.server.counters.snapshot(), b.N)
}

func preloadYCSBBSONDocuments(b *testing.B, col *collections.Collection, count int) {
	b.Helper()
	const batchSize = 1000
	ids := make([][]byte, 0, batchSize)
	docs := make([][]byte, 0, batchSize)
	for i := 0; i < count; i++ {
		key := ycsbBenchKey(i)
		doc, err := encodeYCSBBSONBinaryDocument(key)
		if err != nil {
			b.Fatalf("encode preload %s: %v", key, err)
		}
		ids = append(ids, []byte(key))
		docs = append(docs, doc)
		if len(ids) == batchSize {
			if _, err := col.InsertBatchValidatedBSON(ids, docs); err != nil {
				b.Fatalf("preload InsertBatchValidatedBSON: %v", err)
			}
			ids = ids[:0]
			docs = docs[:0]
		}
	}
	if len(ids) > 0 {
		if _, err := col.InsertBatchValidatedBSON(ids, docs); err != nil {
			b.Fatalf("preload final InsertBatchValidatedBSON: %v", err)
		}
	}
	if err := col.Flush(); err != nil {
		b.Fatalf("preload Flush: %v", err)
	}
}

func precomputeYCSBUpdateRequests(b *testing.B, server *Server, handle CollectionHandle, mode string, start, count, docCount int) []ycsbPrecomputedUpdateRequest {
	b.Helper()
	requests := make([]ycsbPrecomputedUpdateRequest, count)
	for i := 0; i < count; i++ {
		operation := start + i
		key := ycsbBenchKey(operation % docCount)
		id := []byte(key)
		value := ycsbBenchUpdateValue(operation)
		switch mode {
		case "get_many_replace_batch":
			readBody, err := appendGetManyRequestBodyRef(nil, "", handle, true, [][]byte{id})
			if err != nil {
				b.Fatalf("append get_many request: %v", err)
			}
			doc, err := encodeYCSBBSONBinaryDocumentWithField0(key, value)
			if err != nil {
				b.Fatalf("encode replacement %s: %v", key, err)
			}
			guard := benchmarkMutationGuard(b, server, "replace_batch_precomputed", operation)
			replaceBody, err := appendCommandRequestBody(nil, iwire.CommandReplaceBatch,
				append(guard,
					collectionHandleRef(handle),
					documentFormatSection(collections.DocumentFormatBSON),
					iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, id)},
					iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, doc)},
					iwire.Section{ID: iwire.SectionReplacementMode, Bytes: []byte{byte(replacementModeExistingOnly)}},
				)...,
			)
			if err != nil {
				b.Fatalf("append replace request: %v", err)
			}
			requests[i] = ycsbPrecomputedUpdateRequest{read: readBody, mutation: replaceBody}
		case "update_bson_set":
			guard := benchmarkMutationGuard(b, server, "update_bson_set_precomputed", operation)
			fields := []collections.BSONSetField{{
				Key:   "field0",
				Value: ycsbBenchBSONBinaryRawValue(value),
			}}
			body, err := appendUpdateBSONSetRequestBodyRef(nil, "", handle, true, id, fields, AckVisible, guard)
			if err != nil {
				b.Fatalf("append update_bson_set request: %v", err)
			}
			requests[i] = ycsbPrecomputedUpdateRequest{mutation: body}
		default:
			b.Fatalf("unknown update mode %q", mode)
		}
	}
	return requests
}

func reportYCSBCollectionStatsDelta(b *testing.B, before, after collections.CollectionManagerStats, ops int) {
	b.Helper()
	if ops <= 0 {
		return
	}
	reportUint64 := func(name string, value uint64) {
		b.ReportMetric(float64(value)/float64(ops), name)
	}
	reportDuration := func(name string, value time.Duration) {
		if value < 0 {
			value = 0
		}
		b.ReportMetric(float64(value.Nanoseconds())/float64(ops), name)
	}
	reportUint64("mutation_lock_calls/op", uint64Delta(after.MutationLockCalls, before.MutationLockCalls))
	reportDuration("mutation_lock_wait_ns/op", after.MutationLockWait-before.MutationLockWait)
	reportDuration("mutation_lock_hold_ns/op", after.MutationLockHold-before.MutationLockHold)
	reportUint64("indexed_stage_batches/op", uint64Delta(after.IndexedStageBatches, before.IndexedStageBatches))
	reportUint64("indexed_stage_docs/op", uint64Delta(after.IndexedStageDocs, before.IndexedStageDocs))
	reportUint64("indexed_stage_root_runs/op", uint64Delta(after.IndexedStageRootRuns, before.IndexedStageRootRuns))
	reportUint64("validation_preflight_reused/op", uint64Delta(after.InsertValidationPreflightReused, before.InsertValidationPreflightReused))
	reportUint64("validation_preflight_rechecked/op", uint64Delta(after.InsertValidationPreflightRechecked, before.InsertValidationPreflightRechecked))
}

func reportYCSBNativeCounterDelta(b *testing.B, before, after map[string]uint64, ops int) {
	b.Helper()
	if ops <= 0 {
		return
	}
	report := func(metricName, counterName string) {
		b.ReportMetric(float64(uint64Delta(after[counterName], before[counterName]))/float64(ops), metricName)
	}
	report("combiner_batches/op", "insert_batch_combiner.batches_total")
	report("combiner_requests/op", "insert_batch_combiner.requests_total")
	report("combiner_single_requests/op", "insert_batch_combiner.single_requests_total")
	report("combiner_fallback_requests/op", "insert_batch_combiner.fallback_requests_total")
}

func reportYCSBUpdateNativeCounterDelta(b *testing.B, before, after map[string]uint64, ops int) {
	b.Helper()
	if ops <= 0 {
		return
	}
	report := func(metricName, counterName string) {
		b.ReportMetric(float64(uint64Delta(after[counterName], before[counterName]))/float64(ops), metricName)
	}
	report("get_many_requests/op", "commands.get_many.requests_total")
	report("replace_batch_requests/op", "commands.replace_batch.requests_total")
	report("update_bson_set_requests/op", "commands.update_bson_set.requests_total")
}

func uint64Delta(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func startTimedYCSBCPUProfile(b *testing.B) func() {
	b.Helper()

	profilePath := strings.TrimSpace(os.Getenv(ycsbTimedCPUProfilePathEnv))
	if profilePath == "" {
		return func() {}
	}
	if dir := filepath.Dir(profilePath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatalf("create timed cpu profile dir: %v", err)
		}
	}
	file, err := os.Create(profilePath)
	if err != nil {
		b.Fatalf("create timed cpu profile: %v", err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		b.Fatalf("start timed cpu profile: %v; do not also pass go test -cpuprofile", err)
	}

	stopped := false
	return func() {
		if stopped {
			return
		}
		pprof.StopCPUProfile()
		stopped = true
		if err := file.Close(); err != nil {
			b.Errorf("close timed cpu profile: %v", err)
		}
	}
}

func requireSafeYCSBPrecomputedRequests(b *testing.B) {
	b.Helper()

	if strings.TrimSpace(os.Getenv(ycsbTimedCPUProfilePathEnv)) != "" {
		if b.N <= maxProfiledYCSBPrecomputedRequests {
			return
		}
		b.Fatalf("precomputed nativewire YCSB profile would build %d requests outside the timed section; use a fixed -benchtime below %d ops", b.N, maxProfiledYCSBPrecomputedRequests)
	}
	if b.N <= maxYCSBPrecomputedRequests {
		return
	}
	b.Skipf("skipping precomputed nativewire YCSB benchmark with no %s: use fixed -benchtime <= %d ops", ycsbTimedCPUProfilePathEnv, maxYCSBPrecomputedRequests)
}

func newYCSBBenchEnv(tb testing.TB, format collections.DocumentFormat, tcp bool) *ycsbBenchEnv {
	tb.Helper()
	opts := treedb.OptionsFor(ycsbBenchTreeDBProfile(tb), tb.TempDir())
	db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		tb.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: ycsbBenchCollection,
		Options: collections.CollectionOptions{
			DocumentFormat: format,
		},
		Indexes: []collections.IndexDefinition{
			{
				Name:      ycsbBenchKeyIndex,
				Field:     ycsbBenchKeyField,
				ValueType: collections.IndexValueString,
				Unique:    true,
			},
		},
	}); err != nil {
		_ = cleanup()
		tb.Fatalf("CreateCollection: %v", err)
	}

	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	env := &ycsbBenchEnv{
		server: server,
		cleanup: func() error {
			return errors.Join(server.Close(), cleanup())
		},
	}
	if !tcp {
		return env
	}

	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		_ = env.cleanup()
		tb.Fatalf("Listen: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ctx, ln)
	}()
	baseCleanup := env.cleanup
	env.address = ln.Addr().String()
	env.cleanup = func() error {
		cancel()
		_ = ln.Close()
		err := baseCleanup()
		select {
		case serveErr := <-errCh:
			if serveErr != nil && !errors.Is(serveErr, context.Canceled) && !errors.Is(serveErr, net.ErrClosed) {
				err = errors.Join(err, serveErr)
			}
		default:
		}
		return err
	}
	return env
}

func ycsbBenchTreeDBProfile(tb testing.TB) treedb.Profile {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv(ycsbBenchProfileEnv))
	profile, ok := parseYCSBBenchTreeDBProfile(raw)
	if !ok {
		tb.Fatalf("unknown %s %q", ycsbBenchProfileEnv, raw)
	}
	return profile
}

func parseYCSBBenchTreeDBProfile(raw string) (treedb.Profile, bool) {
	return treedb.ParsePublicProfile(raw, treedb.ProfileCommandWALRelaxed)
}

func newYCSBBenchClient(tb testing.TB, ctx context.Context, env *ycsbBenchEnv, transport string) (*Client, func() error) {
	tb.Helper()
	if transport == "tcp" {
		client, err := DialContext(ctx, "tcp", env.address)
		if err != nil {
			tb.Fatalf("DialContext: %v", err)
		}
		return client, client.Close
	}
	client, cleanup, err := NewInProcessClient(ctx, env.server)
	if err != nil {
		tb.Fatalf("NewInProcessClient: %v", err)
	}
	return client, cleanup
}

func ycsbInsertBatchNoResultIDs(ctx context.Context, c *Client, handle CollectionHandle, format collections.DocumentFormat, ids, docs [][]byte) error {
	if c == nil {
		return errors.New("nativewire: nil client")
	}
	guard, err := c.replicatedMutationGuard(ctx, "insert_batch")
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	body, err := appendInsertBatchRequestBodyRefFlags(c.requestBody[:0], "", handle, true, format, ids, docs, AckVisible, iwire.CommandFlagOmitResultIDs, guard)
	if err != nil {
		return err
	}
	_, response, err := c.roundTripLocked(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	c.requestBody = body[:0]
	if err != nil {
		c.clearCatalogVersionOnMismatch(err)
		return err
	}
	var sectionBuf [4]iwire.Section
	sections, err := iwire.DecodeSectionsInto(sectionBuf[:0], response, c.limits)
	if err != nil {
		return err
	}
	c.updateCatalogVersionFromMutationResponse(sections)
	return nil
}

func encodeYCSBJSONDocument(key string) ([]byte, error) {
	row := make(map[string]any, ycsbBenchFields+1)
	for _, field := range ycsbBenchFieldNames {
		row[field] = ycsbBenchFieldValue
	}
	row[ycsbBenchKeyField] = key
	return json.Marshal(row)
}

func encodeYCSBBSONBinaryDocument(key string) ([]byte, error) {
	return encodeYCSBBSONBinaryDocumentWithField0(key, ycsbBenchFieldValue)
}

func encodeYCSBBSONBinaryDocumentWithField0(key string, field0 []byte) ([]byte, error) {
	row := make(bson.D, 0, ycsbBenchFields+1)
	row = append(row, bson.E{Key: ycsbBenchKeyField, Value: key})
	for i, field := range ycsbBenchFieldNames {
		value := ycsbBenchFieldValue
		if i == 0 {
			value = field0
		}
		row = append(row, bson.E{Key: field, Value: value})
	}
	return bson.Marshal(row)
}

func ycsbBenchBSONBinaryRawValue(value []byte) bson.RawValue {
	return bson.RawValue{Type: bson.TypeBinary, Value: appendYCSBBSONBinaryValue(nil, value)}
}

func appendYCSBBSONBinaryValue(dst []byte, value []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(value)))
	dst = append(dst, 0)
	return append(dst, value...)
}

func encodeYCSBBSONBase64StringDocument(key string) ([]byte, error) {
	row := make(bson.D, 0, ycsbBenchFields+1)
	row = append(row, bson.E{Key: ycsbBenchKeyField, Value: key})
	for _, field := range ycsbBenchFieldNames {
		row = append(row, bson.E{Key: field, Value: ycsbBenchFieldValueString})
	}
	return bson.Marshal(row)
}

func encodeYCSBTemplateV1Document(key string) ([]byte, error) {
	values := make([]any, 0, ycsbBenchFields+1)
	values = append(values, key)
	for range ycsbBenchFieldNames {
		values = append(values, ycsbBenchFieldValueString)
	}
	return collections.EncodeTemplateV1Document(ycsbTemplateFields, values)
}

func ycsbBenchKey(n int) string {
	return fmt.Sprintf("user%d", ycsbHash64(int64(n)))
}

func ycsbHash64(n int64) int64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	hash := fnv.New64a()
	_, _ = hash.Write(b[:])
	result := int64(hash.Sum64())
	if result < 0 {
		return -result
	}
	return result
}

func ycsbFieldNames() []string {
	fields := make([]string, ycsbBenchFields)
	for i := range fields {
		fields[i] = fmt.Sprintf("field%d", i)
	}
	return fields
}

func ycsbFieldValue() []byte {
	value := make([]byte, ycsbBenchFieldLen)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	return value
}

func ycsbBenchUpdateValue(operation int) []byte {
	value := make([]byte, ycsbBenchFieldLen)
	for i := range value {
		value[i] = byte('A' + (operation+i)%26)
	}
	return value
}
