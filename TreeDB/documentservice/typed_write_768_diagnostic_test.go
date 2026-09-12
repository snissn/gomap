package documentservice

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

type serviceWrite768Result struct {
	Call   int
	WallNS int64
	err    error
}

// Opt-in measurement of the document-service method used by native Minima
// upserts. Wire/client encoding is excluded. The default buffered async
// publication policy is retained, so acknowledgement and drain are separate.
func TestDocumentServiceTypedWrite768Diagnostic(t *testing.T) {
	if os.Getenv("GOMAP_WRITE_768_DIAGNOSTIC") != "1" {
		t.Skip("set GOMAP_WRITE_768_DIAGNOSTIC=1 to run the bounded durable-write sweep")
	}
	requireTypedServiceServingTest(t)
	const calls = 16
	preload := serviceWrite768Preload(t)
	for _, rows := range []int{1, 64, 256, 1024} {
		for _, writers := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("rows%d/writers%d", rows, writers), func(t *testing.T) {
				dir := t.TempDir()
				opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
				opts.PublicBatchWriteSyncPhaseStats = true
				backend, cleanup, databaseStats, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
				if err != nil {
					t.Fatal(err)
				}
				manager := collections.NewCollectionManager(backend)
				svc := NewWithDeferredVectorBuildMaintenance(manager, maintenance)
				closed := false
				defer func() {
					if !closed {
						_ = svc.Close()
						_ = cleanup()
					}
				}()
				svc.DiagnosticsHandler(nil)
				ctx := context.Background()
				info, err := svc.CreateIndex(ctx, CreateIndexRequest{
					Name: "minima", Dimension: 768, Metric: MetricCosine, TypedInput: true,
					VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph},
					ScalarFields:       []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}, {Field: "meta.fpath", ValueType: ScalarFieldString}},
				})
				if err != nil {
					t.Fatal(err)
				}
				col, err := manager.OpenCollection(info.Name)
				if err != nil {
					t.Fatal(err)
				}
				if meta := col.Meta(); meta.Options.DisableBufferedIndexedAsyncFlush || !meta.Options.BufferedIndexedAsyncFlush {
					t.Fatalf("document-service collection is not using default async publication: %+v", meta.Options)
				}
				for offset := 0; offset < preload; offset += 1024 {
					input := serviceWrite768Input(info.Generation, "seed", offset, min(1024, preload-offset))
					if out, err := svc.UpsertTypedDocuments(ctx, info.Name, input); err != nil || out.Inserted != len(input.IDs) {
						t.Fatalf("preload offset=%d result=%+v err=%v", offset, out, err)
					}
				}
				serving := serviceWrite768Serving(preload + calls*rows)
				if _, err := svc.OptimizeIndex(ctx, info.Name, OptimizeIndexRequest{ColumnGraphAction: "build", ColumnGraphServing: &serving}); err != nil {
					t.Fatal(err)
				}

				inputs := make([]TypedDocumentsRequest, calls)
				for call := range inputs {
					inputs[call] = serviceWrite768Input(info.Generation, "write", call*rows, rows)
				}
				results := make([]serviceWrite768Result, calls)
				beforeDB, beforeManager := databaseStats(), manager.Stats()
				start := make(chan struct{})
				var ready, done sync.WaitGroup
				ready.Add(writers)
				done.Add(writers)
				for writer := range writers {
					go func() {
						defer done.Done()
						ready.Done()
						<-start
						for call := writer; call < calls; call += writers {
							began := time.Now()
							out, err := svc.UpsertTypedDocuments(ctx, info.Name, inputs[call])
							results[call] = serviceWrite768Result{Call: call, WallNS: time.Since(began).Nanoseconds(), err: err}
							if err == nil && (out.Inserted != rows || out.Updated != 0) {
								results[call].err = fmt.Errorf("inserted=%d updated=%d", out.Inserted, out.Updated)
							}
						}
					}()
				}
				ready.Wait()
				began := time.Now()
				close(start)
				done.Wait()
				elapsed := time.Since(began)
				afterDB, afterManager := databaseStats(), manager.Stats()
				lastInsert := svc.DiagnosticsSnapshot(nil).LastOpened
				for _, result := range results {
					if result.err != nil || result.WallNS == 0 {
						t.Fatalf("call %d: %v (wall=%d)", result.Call, result.err, result.WallNS)
					}
				}
				walls := make([]int64, len(results))
				for i, result := range results {
					walls[i] = result.WallNS
				}
				slices.Sort(walls)
				drainStarted := time.Now()
				if err := manager.FlushAll(); err != nil {
					t.Fatal(err)
				}
				drainElapsed := time.Since(drainStarted)
				drainedDB, drainedManager := databaseStats(), manager.Stats()
				summary := map[string]any{
					"phase": "write768", "layer": "documentservice_typed_no_wire", "preload": preload,
					"rows": rows, "writers": writers, "calls": calls, "wall_ns": elapsed.Nanoseconds(),
					"rows_per_second": float64(rows*calls) / elapsed.Seconds(), "call_p50_ns": walls[len(walls)/2], "call_p95_ns": walls[(len(walls)-1)*95/100],
					"ack_db_delta":      serviceWrite768CounterDelta(t, beforeDB, afterDB, serviceWrite768DBCounterKeys),
					"ack_manager_delta": serviceWrite768CounterDelta(t, beforeManager, afterManager, serviceWrite768ManagerCounterKeys),
					"ack_pending":       serviceWrite768Gauges(afterManager), "last_completed_insert": lastInsert,
					"drain_wall_ns":       drainElapsed.Nanoseconds(),
					"drain_db_delta":      serviceWrite768CounterDelta(t, afterDB, drainedDB, serviceWrite768DBCounterKeys),
					"drain_manager_delta": serviceWrite768CounterDelta(t, afterManager, drainedManager, serviceWrite768ManagerCounterKeys),
					"drained_pending":     serviceWrite768Gauges(drainedManager),
				}
				encoded, err := json.Marshal(summary)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("write768_service_summary=%s", encoded)

				if err := svc.Close(); err != nil {
					t.Fatal(err)
				}
				if err := cleanup(); err != nil {
					t.Fatal(err)
				}
				closed = true
				reopenedBackend, reopenedCleanup, _, reopenedMaintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer reopenedCleanup()
				reopenedManager := collections.NewCollectionManager(reopenedBackend)
				reopened := NewWithDeferredVectorBuildMaintenance(reopenedManager, reopenedMaintenance)
				defer reopened.Close()
				count, err := reopened.CountDocuments(ctx, info.Name, CountDocumentsRequest{})
				if err != nil || count.Count != preload+calls*rows {
					t.Fatalf("reopen count=%d want=%d err=%v", count.Count, preload+calls*rows, err)
				}
				reopenedCollection, err := reopenedManager.OpenCollection(info.Name)
				if err != nil {
					t.Fatal(err)
				}
				for _, call := range []int{0, len(inputs) - 1} {
					for _, id := range [][]byte{inputs[call].IDs[0], inputs[call].IDs[len(inputs[call].IDs)-1]} {
						if doc, err := reopenedCollection.Get(id); err != nil || len(doc) == 0 {
							t.Fatalf("reopen Get(%q): len=%d err=%v", id, len(doc), err)
						}
					}
				}
			})
		}
	}
}

func serviceWrite768Preload(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("GOMAP_WRITE_768_PRELOAD")
	if raw == "" {
		return 0
	}
	preload, err := strconv.Atoi(raw)
	if err != nil || preload < 0 || preload > 500000 {
		t.Fatal("GOMAP_WRITE_768_PRELOAD must be in [0,500000]")
	}
	return preload
}

func serviceWrite768Input(generation uint64, prefix string, offset, rows int) TypedDocumentsRequest {
	req := TypedDocumentsRequest{ExpectedGeneration: generation, IDs: make([][]byte, rows), Retained: make([][]byte, rows), Columns: []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, rows)}, {Name: "content", Strings: make([]string, rows)}, {Name: "meta.user_id", Strings: make([]string, rows)}, {Name: "meta.fpath", Strings: make([]string, rows)}}}
	for row := range rows {
		n := offset + row
		id := fmt.Sprintf("%s768-%08d", prefix, n)
		req.IDs[row] = []byte(id)
		req.Retained[row] = []byte(fmt.Sprintf(`{"id":%q,"meta":{"residual":"kept"}}`, id))
		vector := make([]float32, 768)
		for dimension := range vector {
			vector[dimension] = float32(1+((n+1)*(dimension+3))%997) / 997
		}
		req.Columns[0].Float32Vectors[row] = vector
		req.Columns[1].Strings[row] = "diagnostic content"
		req.Columns[2].Strings[row] = fmt.Sprintf("user-%d", n%4)
		req.Columns[3].Strings[row] = fmt.Sprintf("/path-%d", n)
	}
	return req
}

func serviceWrite768Serving(capacity int) collections.ColumnGraphServingOptions {
	opts := typedServiceTestOptions()
	opts.Publication = collections.ColumnGraphPublicationLimits{Rows: max(1024, capacity), Tombstones: max(1024, capacity), ValueSlots: max(4096, capacity*4), OwnedBytes: 512 << 20, EncodedOutputBytes: 1 << 30}
	opts.Owners.StateBytes, opts.Owners.AssetBytes = 512<<20, 1<<30
	opts.Owners.Cold.AssetBytes = 512 << 20
	opts.Filter.SourceIDs, opts.Filter.InspectedEntries = max(1024, capacity), max(4096, capacity*4)
	opts.Filter.SourceBytes, opts.Filter.RetainedBytes, opts.Filter.MappingWork = 64<<20, 64<<20, 128<<20
	opts.FoldRows, opts.SearchCandidates = max(1024, capacity), max(4096, capacity*2+128)
	return opts
}

var serviceWrite768DBCounterKeys = []string{
	"treedb.command_wal.append.count_total", "treedb.command_wal.append.ns_total",
	"treedb.command_wal.sync.count_total", "treedb.command_wal.sync.ns_total",
	"treedb.command_wal.file_sync.calls_total", "treedb.command_wal.file_sync.ns_total",
	"treedb.publish.ordered_root_delta_group.calls_total", "treedb.publish.ordered_root_delta_group.root_apply_ns_total",
	"treedb.publish.ordered_root_delta_group.root_apply_calls_total", "treedb.publish.ordered_root_delta_group.root_apply_ops_total",
	"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total", "treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total",
}

var serviceWrite768ManagerCounterKeys = []string{
	"treedb.collections.write_domain.mutation_lock.calls_total", "treedb.collections.write_domain.mutation_lock.wait_ns_total", "treedb.collections.write_domain.mutation_lock.hold_ns_total",
	"treedb.collections.write_domain.indexed_stage.batches_total", "treedb.collections.write_domain.indexed_stage.docs_total",
	"treedb.collections.write_domain.indexed_async_flush.scheduled_total", "treedb.collections.write_domain.indexed_async_flush.backpressure_sync_total", "treedb.collections.write_domain.indexed_async_flush.wait_ns_total",
	"treedb.collections.write_domain.indexed_flush.calls_total", "treedb.collections.write_domain.indexed_flush.docs_total", "treedb.collections.write_domain.indexed_flush.duration_ns_total", "treedb.collections.write_domain.indexed_flush.publish_ns_total",
	"treedb.collections.write_domain.root_delta_plan.entries_total",
}

func serviceWrite768CounterDelta(t *testing.T, before, after map[string]string, keys []string) map[string]uint64 {
	t.Helper()
	out := make(map[string]uint64, len(keys))
	for _, key := range keys {
		left, leftErr := strconv.ParseUint(before[key], 10, 64)
		right, rightErr := strconv.ParseUint(after[key], 10, 64)
		if leftErr != nil || rightErr != nil || right < left {
			t.Fatalf("invalid monotonic stat %s: before=%q after=%q", key, before[key], after[key])
		}
		out[key] = right - left
	}
	return out
}

func serviceWrite768Gauges(stats map[string]string) map[string]string {
	out := make(map[string]string, len(serviceWrite768GaugeKeys))
	for _, key := range serviceWrite768GaugeKeys {
		out[key] = stats[key]
	}
	return out
}

var serviceWrite768GaugeKeys = []string{
	"treedb.collections.write_domain.pending_docs", "treedb.collections.write_domain.pending_bytes", "treedb.collections.write_domain.pending_root_runs",
	"treedb.collections.write_domain.pending_indexed_flush_units", "treedb.collections.write_domain.pending_indexed_publication_bytes",
	"treedb.collections.write_domain.overlay.mutable_docs", "treedb.collections.write_domain.overlay.queued_indexed_flush_units", "treedb.collections.write_domain.overlay.active_indexed_flush_units", "treedb.collections.write_domain.overlay.visible_depth",
	"treedb.collections.write_domain.indexed_async_flush.running_domains",
}
