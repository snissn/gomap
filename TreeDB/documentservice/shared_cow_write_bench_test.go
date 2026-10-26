package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	sharedCOWWriteRows      = 4096
	sharedCOWWriteBatchSize = 128
	sharedCOWWriteBatches   = sharedCOWWriteRows / sharedCOWWriteBatchSize
	sharedCOWWriteIndex     = "shared_cow_consumers"
)

// BenchmarkServiceSharedCOWWriteLoad measures one fresh 4096-document load per
// op: 32 public UpsertDocuments calls, each invoked once, on one Service/index.
// The fixture and owner/profile selection match BenchmarkServiceSharedCOWConsumers.
// Standard ns/op, B/op and allocs/op cover only the concurrent request-load
// interval (including dispatch/wait), not Create, fixture construction, response
// checks, Optimize, Close or reopen validation. ns/request is throughput
// normalization, not individual request latency; divide B/op and allocs/op by
// requests/op or records/op for those normalizations. Deferred graph work and
// lifecycle costs are reported separately and must succeed. This benchmark is
// a diagnostic entry point, not a numerical acceptance rule or crash proof.
func BenchmarkServiceSharedCOWWriteLoad(b *testing.B) {
	backend := os.Getenv("GOMAP_SERVICE_BENCH_BACKEND")
	if backend == "" {
		backend = "legacy"
	}
	if backend != "legacy" && backend != "native" {
		b.Fatalf("unsupported service benchmark backend %q", backend)
	}
	profile := treedb.Profile(os.Getenv("GOMAP_SERVICE_BENCH_PROFILE"))
	if profile == "" {
		profile = treedb.ProfileCommandWALDurable
	}
	switch profile {
	case treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast:
	default:
		b.Fatalf("unsupported service benchmark profile %q", profile)
	}
	for _, writers := range []int{1, 4} {
		b.Run(fmt.Sprintf("writers%d", writers), func(b *testing.B) {
			b.StopTimer()
			b.ReportAllocs()
			parent := b.TempDir()
			docs := make([]Document, sharedCOWWriteRows)
			for i := range docs {
				docs[i] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
			}
			var costs sharedCOWWriteCosts
			b.ResetTimer()
			for load := 0; load < b.N; load++ {
				benchmarkSharedCOWWriteLoad(b, parent, backend, profile, writers, load, docs, &costs)
			}
			loads := float64(b.N)
			ackNS := float64(b.Elapsed().Nanoseconds())
			b.ReportMetric(sharedCOWWriteBatches, "requests/op")
			b.ReportMetric(sharedCOWWriteRows, "records/op")
			b.ReportMetric(ackNS, "ack_total_ns")
			b.ReportMetric(ackNS/(loads*sharedCOWWriteBatches), "ns/request")
			b.ReportMetric(ackNS/(loads*sharedCOWWriteRows), "ns/record")
			b.ReportMetric(loads*sharedCOWWriteRows/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(costs.optimize.Nanoseconds())/loads, "optimize_ns/load")
			b.ReportMetric(float64(costs.close.Nanoseconds())/loads, "close_ns/load")
			b.ReportMetric(float64(costs.reopen.Nanoseconds())/loads, "reopen_ns/load")
			b.ReportMetric(float64(costs.reopenedClose.Nanoseconds())/loads, "reopened_close_ns/load")
			b.Logf("backend=%s profile=%s writers=%d loads=%d attempted=%d completed=%d request_retries=0 timed_scope=request_load_only B/op=bytes/load allocs/op=objects/load", backend, profile, writers, b.N, costs.attempted, costs.completed)
		})
	}
}

type sharedCOWWriteCosts struct {
	optimize      time.Duration
	close         time.Duration
	reopen        time.Duration
	reopenedClose time.Duration
	attempted     uint64
	completed     uint64
}

type sharedCOWWriteOwner struct {
	service *Service
	manager *collections.CollectionManager
	closeDB func() error
}

func openSharedCOWWriteOwner(opts treedb.Options, backend string) (*sharedCOWWriteOwner, error) {
	owner := &sharedCOWWriteOwner{}
	switch backend {
	case "legacy":
		db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			return nil, err
		}
		owner.closeDB = cleanup
		owner.manager = collections.NewCollectionManager(db)
	case "native":
		db, err := treedb.Open(opts)
		if err != nil {
			return nil, err
		}
		owner.closeDB = db.Close
		handle, err := db.NativeSemanticBackend()
		if err != nil {
			return nil, errors.Join(err, owner.close())
		}
		owner.manager = collections.NewCollectionManager(handle)
	default:
		return nil, fmt.Errorf("unsupported service benchmark backend %q", backend)
	}
	// This deliberately matches the maintained read fixture. Command-owner
	// maintenance wiring is a different constructor and is not substituted here.
	owner.service = New(owner.manager)
	return owner, nil
}

func (owner *sharedCOWWriteOwner) close() error {
	if owner == nil {
		return nil
	}
	var err error
	if owner.service != nil {
		err = errors.Join(err, owner.service.Close())
		owner.service = nil
	}
	if owner.manager != nil {
		err = errors.Join(err, owner.manager.Close())
		owner.manager = nil
	}
	if owner.closeDB != nil {
		err = errors.Join(err, owner.closeDB())
		owner.closeDB = nil
	}
	return err
}

func benchmarkSharedCOWWriteLoad(b *testing.B, parent, backend string, profile treedb.Profile, writers, load int, docs []Document, costs *sharedCOWWriteCosts) {
	b.Helper()
	dir, err := os.MkdirTemp(parent, "load-")
	if err != nil {
		b.Fatal(err)
	}
	var owner *sharedCOWWriteOwner
	defer func() {
		if err := owner.close(); err != nil {
			b.Errorf("load%d deferred cleanup: %v", load, err)
		}
		// Each b.N iteration owns only this directory; do not retain all loads
		// until benchmark cleanup or remove any caller-supplied directory.
		if err := os.RemoveAll(dir); err != nil {
			b.Errorf("load%d remove owned directory: %v", load, err)
		}
	}()
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err = openSharedCOWWriteOwner(opts, backend)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	info, err := owner.service.CreateIndex(ctx, CreateIndexRequest{Name: sharedCOWWriteIndex, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
	if err != nil {
		b.Fatal(err)
	}
	checkSharedCOWWriteIndex(b, info)
	if load == 0 {
		b.Logf("backend=%s profile=%s requested_memtable=cow_btree resolved_strategy=%s metric=%s rows=%d batch=%d writers=%d defer_rebuild=true", backend, profile, info.VectorStrategy, info.Metric, sharedCOWWriteRows, sharedCOWWriteBatchSize, writers)
	}
	type result struct {
		response  UpsertDocumentsResponse
		err       error
		attempted bool
	}
	var results [sharedCOWWriteBatches]result
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(writers)
	done.Add(writers)
	for worker := 0; worker < writers; worker++ {
		go func(worker int) {
			defer done.Done()
			ready.Done()
			<-start
			for batch := worker; batch < sharedCOWWriteBatches; batch += writers {
				first := batch * sharedCOWWriteBatchSize
				results[batch].attempted = true
				results[batch].response, results[batch].err = owner.service.UpsertDocuments(ctx, sharedCOWWriteIndex, UpsertDocumentsRequest{Documents: docs[first : first+sharedCOWWriteBatchSize], DeferVectorIndexRebuild: true})
			}
		}(worker)
	}
	ready.Wait()
	b.StartTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	failed := false
	for batch, got := range results {
		if got.attempted {
			costs.attempted++
		}
		if !got.attempted || got.err != nil {
			b.Logf("load%d batch%d attempted=%t error=%v cause=%v", load, batch, got.attempted, got.err, errors.Unwrap(got.err))
			failed = true
			continue
		}
		costs.completed++
		checkSharedCOWWriteIndex(b, got.response.Index)
		if got.response.Upserted != sharedCOWWriteBatchSize || got.response.Inserted != sharedCOWWriteBatchSize || got.response.Updated != 0 || len(got.response.IDs) != sharedCOWWriteBatchSize {
			b.Fatalf("load%d batch%d incorrect response: %+v", load, batch, got.response)
		}
		for i, id := range got.response.IDs {
			if want := docs[batch*sharedCOWWriteBatchSize+i].ID; id != want {
				b.Fatalf("load%d batch%d response ID%d=%q want%q", load, batch, i, id, want)
			}
		}
	}
	if failed {
		b.Fatalf("load%d public request failure: attempted=%d completed=%d; no caller retry", load, costs.attempted, costs.completed)
	}
	checkSharedCOWWriteDocuments(b, owner.service, docs)
	tick := time.Now()
	_, err = owner.service.OptimizeIndex(ctx, sharedCOWWriteIndex, OptimizeIndexRequest{})
	costs.optimize += time.Since(tick)
	if err != nil {
		b.Fatalf("load%d Optimize: %v", load, err)
	}
	b.Logf("load%d query_phase=post-optimize", load)
	before := checkSharedCOWWriteQueries(b, owner.service, backend)
	tick = time.Now()
	err = owner.close()
	costs.close += time.Since(tick)
	if err != nil {
		b.Fatalf("load%d Close: %v", load, err)
	}
	tick = time.Now()
	owner, err = openSharedCOWWriteOwner(opts, backend)
	costs.reopen += time.Since(tick)
	if err != nil {
		b.Fatalf("load%d reopen: %v", load, err)
	}
	info, err = owner.service.OpenIndex(ctx, sharedCOWWriteIndex)
	if err != nil {
		b.Fatal(err)
	}
	checkSharedCOWWriteIndex(b, info)
	checkSharedCOWWriteDocuments(b, owner.service, docs)
	b.Logf("load%d query_phase=post-reopen", load)
	after := checkSharedCOWWriteQueries(b, owner.service, backend)
	if !reflect.DeepEqual(before, after) {
		b.Fatalf("load%d public query results/route changed after clean reopen", load)
	}
	tick = time.Now()
	err = owner.close()
	costs.reopenedClose += time.Since(tick)
	if err != nil {
		b.Fatalf("load%d reopened Close: %v", load, err)
	}
}

func checkSharedCOWWriteIndex(b *testing.B, info IndexInfo) {
	b.Helper()
	if info.Name != sharedCOWWriteIndex || info.Dimension != 2 || info.Metric != MetricCosine || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.TypedInput || info.TextIndexName != defaultTextIndexName || len(info.ScalarFields) != 1 || info.ScalarFields[0].Field != "meta.tenant" || info.ScalarFields[0].ValueType != ScalarFieldString {
		b.Fatalf("incorrect ordinary omitted-strategy declaration: %+v", info)
	}
}

func checkSharedCOWWriteDocuments(b *testing.B, svc *Service, want []Document) {
	b.Helper()
	ctx := context.Background()
	count, err := svc.CountDocuments(ctx, sharedCOWWriteIndex, CountDocumentsRequest{})
	if err != nil || count.Count != sharedCOWWriteRows {
		b.Fatalf("count=%d want%d error=%v", count.Count, sharedCOWWriteRows, err)
	}
	all, err := svc.FilterDocuments(ctx, sharedCOWWriteIndex, FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil || all.Truncated || all.MatchedCount != len(want) || len(all.Documents) != len(want) {
		b.Fatalf("document verification: got%d matched%d truncated=%t error=%v", len(all.Documents), all.MatchedCount, all.Truncated, err)
	}
	for i, got := range all.Documents {
		if !reflect.DeepEqual(got, want[i]) {
			b.Fatalf("document%d mismatch: got%+v want%+v", i, got, want[i])
		}
	}
	for tenant := 0; tenant < 16; tenant++ {
		filter := &Filter{Field: "meta.tenant", Operator: "==", Value: fmt.Sprintf("tenant-%02d", tenant)}
		count, err := svc.CountDocuments(ctx, sharedCOWWriteIndex, CountDocumentsRequest{Filter: filter})
		if err != nil || count.Count != sharedCOWWriteRows/16 {
			b.Fatalf("tenant%d count=%d error=%v", tenant, count.Count, err)
		}
	}
}

type sharedCOWWriteQueryResults struct {
	keyword    []Document
	dense      []Document
	hybrid     []Document
	denseRoute Route
	denseExact bool
}

func checkSharedCOWWriteQueries(b *testing.B, svc *Service, backend string) sharedCOWWriteQueryResults {
	b.Helper()
	ctx := context.Background()
	filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "tenant-00"}
	check := func(label string, docs []Document) {
		b.Helper()
		if len(docs) != 10 {
			b.Fatalf("%s returned%d documents, want10", label, len(docs))
		}
		seen := make(map[string]bool, len(docs))
		for _, doc := range docs {
			if seen[doc.ID] || doc.Meta["tenant"] != "tenant-00" || len(doc.Embedding) != 0 || doc.Score == nil || math.IsNaN(*doc.Score) || math.IsInf(*doc.Score, 0) {
				b.Fatalf("%s incorrect filtered/projected result: %+v", label, doc)
			}
			seen[doc.ID] = true
		}
	}
	keyword, err := svc.SearchKeyword(ctx, sharedCOWWriteIndex, KeywordSearchRequest{Query: "content", TopK: 10, Filter: filter})
	if err != nil {
		b.Fatalf("Keyword: %v", err)
	}
	check("Keyword", keyword.Documents)
	if keyword.Stats.FullDocumentScanFallbacks != 0 {
		b.Fatal("keyword document scan fallback")
	}
	dense, err := svc.SearchDenseVector(ctx, sharedCOWWriteIndex, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Filter: filter})
	if err != nil {
		b.Fatalf("Dense: %v", err)
	}
	check("Dense", dense.Documents)
	hybrid, err := svc.SearchHybrid(ctx, sharedCOWWriteIndex, HybridSearchRequest{Query: "content", QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100, Filter: filter})
	if err != nil {
		b.Fatalf("Hybrid: %v; cause=%v; stats=%+v", err, errors.Unwrap(err), hybrid.Stats)
	}
	check("Hybrid", hybrid.Documents)
	encoded, err := json.Marshal(hybrid.Snapshot)
	if err != nil {
		b.Fatal(err)
	}
	var snapshot struct {
		Native bool `json:"native"`
	}
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		b.Fatal(err)
	}
	if snapshot.Native != (backend == "native") || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.FailClosed != 0 || hybrid.Stats.DocumentsFetched > 10 {
		b.Fatalf("incorrect owner/work: snapshot=%+v stats=%+v", hybrid.Snapshot, hybrid.Stats)
	}
	return sharedCOWWriteQueryResults{keyword: keyword.Documents, dense: dense.Documents, hybrid: hybrid.Documents, denseRoute: dense.Route, denseExact: dense.Exact}
}
