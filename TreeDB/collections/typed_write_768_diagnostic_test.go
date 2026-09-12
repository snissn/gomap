package collections

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

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type typedWrite768Result struct {
	Call   int                   `json:"call"`
	WallNS int64                 `json:"wall_ns"`
	Stats  CollectionInsertStats `json:"stats"`
	err    error
}

// Opt-in phase diagnostic, not a latency assertion or a vector-recall benchmark.
// Input generation, empty graph capture, stats collection, and clean Close are
// outside the measured interval. Every timed request is a durable public typed
// upsert of previously absent IDs with two scalar indexes and one text index.
func TestTypedWrite768Diagnostic(t *testing.T) {
	if os.Getenv("GOMAP_WRITE_768_DIAGNOSTIC") != "1" {
		t.Skip("set GOMAP_WRITE_768_DIAGNOSTIC=1 to run the bounded durable-write sweep")
	}
	const calls = 16
	for _, rows := range []int{1, 64, 256, 1024} {
		for _, writers := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("rows%d/writers%d", rows, writers), func(t *testing.T) {
				preload := typedWrite768Preload(t)
				dir, db, col := openTypedWrite768Diagnostic(t, preload, calls*rows)
				defer db.Close()
				type input struct {
					ids, retained [][]byte
					columns       []TypedColumnBatch
				}
				inputs := make([]input, calls)
				for call := range inputs {
					in := &inputs[call]
					in.ids, in.retained, in.columns = typedWrite768DiagnosticInput("write", call*rows, rows)
				}
				results := make([]typedWrite768Result, calls)
				before := db.Stats()
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
							in := inputs[call]
							began := time.Now()
							updated, stats, err := col.UpsertTypedBatchWithStats(in.ids, in.retained, in.columns)
							results[call] = typedWrite768Result{Call: call, WallNS: time.Since(began).Nanoseconds(), Stats: stats, err: err}
							if err == nil && updated != 0 {
								results[call].err = fmt.Errorf("updated=%d, expected inserts", updated)
							}
							if results[call].err != nil {
								return
							}
						}
					}()
				}
				ready.Wait()
				began := time.Now()
				close(start)
				done.Wait()
				elapsed := time.Since(began)
				after := db.Stats()
				for _, r := range results {
					if r.err != nil || r.WallNS == 0 {
						t.Fatalf("call %d: %v (wall=%d)", r.Call, r.err, r.WallNS)
					}
					encoded, err := json.Marshal(r)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("write768_request=%s", encoded)
				}
				delta := typedWrite768StatsDelta(t, before, after)
				t.Logf("write768_summary preload=%d rows=%d writers=%d calls=%d wall_ns=%d rows_per_second=%.3f db_delta=%s pending=%s", preload, rows, writers, calls, elapsed.Nanoseconds(), float64(rows*calls)/elapsed.Seconds(), typedWrite768JSON(t, delta), typedWrite768JSON(t, typedWrite768ManagerSnapshot(col.manager.StatsSnapshot())))
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				// This checks clean-reopen correctness, not simulated power loss.
				reopened := openTypedMinimaDB(t, dir)
				defer reopened.Close()
				got, err := NewCollectionManager(reopened).OpenCollection("minima")
				if err != nil {
					t.Fatal(err)
				}
				for _, in := range inputs {
					for _, id := range [][]byte{in.ids[0], in.ids[len(in.ids)-1]} {
						if doc, err := got.Get(id); err != nil || len(doc) == 0 {
							t.Fatalf("reopen Get(%q): len=%d err=%v", id, len(doc), err)
						}
					}
				}
			})
		}
	}
}

// Opt-in matched interaction diagnostic. Setup, graph construction, warmup,
// stats snapshots, close, and reopen are outside each measured cell.
func TestTypedWrite768ReadWriteInteractionDiagnostic(t *testing.T) {
	if os.Getenv("GOMAP_WRITE_768_DIAGNOSTIC") != "1" {
		t.Skip("set GOMAP_WRITE_768_DIAGNOSTIC=1 to run the bounded durable-write sweep")
	}
	const preload, calls, rows, readsPerReader = 5000, 16, 256, 64
	for _, cell := range []struct {
		readers, writers int
	}{{1, 0}, {4, 0}, {0, 1}, {4, 1}} {
		t.Run(fmt.Sprintf("readers%d/writers%d", cell.readers, cell.writers), func(t *testing.T) {
			dir, db, col := openTypedWrite768Diagnostic(t, preload, calls*rows)
			defer db.Close()
			query := vectorBenchmarkEmbedding(7, 768)
			for range 3 {
				if _, err := typedWrite768Search(col, query); err != nil {
					t.Fatal(err)
				}
			}

			type input struct {
				ids, retained [][]byte
				columns       []TypedColumnBatch
			}
			inputs := make([]input, calls)
			for call := range inputs {
				inputs[call].ids, inputs[call].retained, inputs[call].columns = typedWrite768DiagnosticInput("mixed", call*rows, rows)
			}
			results := make([]typedWrite768Result, calls)
			latencies := make([][]int64, cell.readers)
			beforeDB := db.Stats()
			beforeManager := col.manager.StatsSnapshot()
			start := make(chan struct{})
			writerDone := make(chan struct{})
			errs := make(chan error, cell.readers+cell.writers)
			var ready, readers, writers sync.WaitGroup
			ready.Add(cell.readers + cell.writers)
			for reader := range cell.readers {
				reader := reader
				readers.Add(1)
				go func() {
					defer readers.Done()
					ready.Done()
					<-start
					for operation := 0; ; operation++ {
						if cell.writers == 0 {
							if operation == readsPerReader {
								return
							}
						} else {
							select {
							case <-writerDone:
								return
							default:
							}
						}
						elapsed, err := typedWrite768Search(col, query)
						if err != nil {
							errs <- err
							return
						}
						latencies[reader] = append(latencies[reader], elapsed.Nanoseconds())
					}
				}()
			}
			var writerStarted, writerEnded time.Time
			if cell.writers == 0 {
				close(writerDone)
			} else {
				writers.Add(1)
				go func() {
					defer writers.Done()
					ready.Done()
					<-start
					writerStarted = time.Now()
					for call, in := range inputs {
						began := time.Now()
						updated, stats, err := col.UpsertTypedBatchWithStats(in.ids, in.retained, in.columns)
						results[call] = typedWrite768Result{Call: call, WallNS: time.Since(began).Nanoseconds(), Stats: stats, err: err}
						if err == nil && updated != 0 {
							results[call].err = fmt.Errorf("updated=%d, expected inserts", updated)
						}
						if results[call].err != nil {
							errs <- results[call].err
							return
						}
					}
					writerEnded = time.Now()
				}()
				go func() {
					writers.Wait()
					close(writerDone)
				}()
			}
			ready.Wait()
			cellStarted := time.Now()
			close(start)
			readers.Wait()
			<-writerDone
			cellElapsed := time.Since(cellStarted)
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}

			var all []int64
			for _, samples := range latencies {
				all = append(all, samples...)
			}
			if cell.readers > 0 && len(all) == 0 {
				t.Fatal("reader interval produced no samples")
			}
			slices.Sort(all)
			summary := map[string]any{
				"phase": "write768_interaction", "preload": preload,
				"readers": cell.readers, "writers": cell.writers,
				"cell_wall_ns": cellElapsed.Nanoseconds(), "read_samples": len(all),
				"db_delta":       typedWrite768StatsDelta(t, beforeDB, db.Stats()),
				"manager_before": typedWrite768ManagerSnapshot(beforeManager),
				"manager_after":  typedWrite768ManagerSnapshot(col.manager.StatsSnapshot()),
			}
			if len(all) > 0 {
				summary["read_p50_ns"] = all[len(all)/2]
				summary["read_p95_ns"] = all[(len(all)-1)*95/100]
			}
			if cell.writers > 0 {
				if writerStarted.IsZero() || writerEnded.IsZero() {
					t.Fatal("writer interval was not captured")
				}
				writeElapsed := writerEnded.Sub(writerStarted)
				summary["write_wall_ns"] = writeElapsed.Nanoseconds()
				summary["write_rows_per_second"] = float64(calls*rows) / writeElapsed.Seconds()
				for _, result := range results {
					t.Logf("write768_interaction_request=%s", typedWrite768JSON(t, result))
				}
			}
			t.Logf("write768_interaction_summary=%s", typedWrite768JSON(t, summary))

			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if cell.writers > 0 {
				reopened := openTypedMinimaDB(t, dir)
				defer reopened.Close()
				got, err := NewCollectionManager(reopened).OpenCollection("minima")
				if err != nil {
					t.Fatal(err)
				}
				for _, call := range []int{0, len(inputs) - 1} {
					for _, id := range [][]byte{inputs[call].ids[0], inputs[call].ids[len(inputs[call].ids)-1]} {
						if doc, err := got.Get(id); err != nil || len(doc) == 0 {
							t.Fatalf("reopen Get(%q): len=%d err=%v", id, len(doc), err)
						}
					}
				}
			}
		})
	}
}

func typedWrite768Preload(t *testing.T) int {
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

func openTypedWrite768Diagnostic(t *testing.T, preload, writeRows int) (string, *backenddb.DB, *Collection) {
	t.Helper()
	meta := typedMinimaCollectionMeta()
	meta.Options.ColumnStore.Columns[0].VectorDims = 768
	meta.VectorIndexes[0].Dimensions = 768
	meta.VectorIndexes[0].M = 16
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)
	for offset := 0; offset < preload; offset += 1024 {
		ids, retained, columns := typedWrite768DiagnosticInput("seed", offset, min(1024, preload-offset))
		if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	publication := typedGraphPublicationLimits{Rows: max(1, writeRows), Tombstones: max(1, writeRows), ValueSlots: max(4, writeRows*4), OwnedBytes: 512 << 20, EncodedOutputBytes: 1 << 30}
	cold := typedGraphColdLimits{ManifestRecords: 512, ManifestBytes: 1 << 20, AssetBytes: 512 << 20, DecodedTermBytes: 64 << 20}
	if err := col.reconcileTypedGraphPublication(publication, cold); err != nil {
		db.Close()
		t.Fatal(err)
	}
	capacity := max(1024, preload+writeRows)
	opts := typedGraphPublicTestOptions()
	opts.Publication = publication
	opts.Owners = typedGraphReadOwnerLimits{Owners: 16, States: 16, StateBytes: 512 << 20, AssetBytes: 1 << 30, Cold: cold}
	opts.Filter = typedGraphFilterLimits{SourceIDs: capacity, SourceBytes: 64 << 20, RetainedBytes: 64 << 20, MappingWork: 128 << 20, InspectedEntries: capacity * 4}
	opts.CandidateOutput = typedGraphFoldAssetLimits{Bytes: 1 << 30, AppenderAttempts: 4096}
	opts.Maintenance = typedGraphWorkEpochLimits{NativeEntries: 4096, ColumnSegments: 4096, ManifestRecords: 4096, LifecycleEntries: 4096, NativeBytes: 1 << 30, ColumnBytes: 1 << 30, ManifestBytes: 64 << 20, RetainedBytes: 512 << 20, PagerPages: 1 << 18}
	opts.FoldRows, opts.SearchCandidates = capacity, capacity*2+128
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", opts); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return dir, db, col
}

func typedWrite768Search(col *Collection, query []float32) (time.Duration, error) {
	var buffer VectorIndexSearchBuffer
	started := time.Now()
	response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: query, TopK: 10, EfSearch: 128, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
	elapsed := time.Since(started)
	if view != nil {
		if closeErr := view.Close(); err == nil {
			err = closeErr
		}
	}
	if err == nil && (len(response.Results) != 10 || !response.Stats.ColumnGraphWork.Completed) {
		err = fmt.Errorf("incomplete public column_graph search: results=%d work=%+v", len(response.Results), response.Stats.ColumnGraphWork)
	}
	return elapsed, err
}

func typedWrite768StatsDelta(t *testing.T, before, after map[string]string) map[string]uint64 {
	t.Helper()
	keys := []string{
		"treedb.command_wal.append.count_total",
		"treedb.command_wal.append.ns_total",
		"treedb.command_wal.sync.count_total",
		"treedb.command_wal.sync.ns_total",
		"treedb.command_wal.file_sync.calls_total",
		"treedb.command_wal.file_sync.ns_total",
		"treedb.command_wal.directory_sync.calls_total",
		"treedb.command_wal.directory_sync.ns_total",
		"treedb.publish.ordered_root_delta_group.calls_total",
		"treedb.publish.ordered_root_delta_group.write_lock_wait_ns_total",
		"treedb.publish.ordered_root_delta_group.write_lock_hold_ns_total",
		"treedb.publish.ordered_root_delta_group.root_apply_ns_total",
		"treedb.publish.ordered_root_delta_group.root_apply_calls_total",
		"treedb.publish.ordered_root_delta_group.root_apply_ops_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total",
		"treedb.publish.ordered_root_delta_group.system_apply_ns_total",
		"treedb.publish.ordered_root_delta_group.finalize_ns_total",
	}
	delta := make(map[string]uint64, len(keys))
	for _, key := range keys {
		left, leftErr := strconv.ParseUint(before[key], 10, 64)
		right, rightErr := strconv.ParseUint(after[key], 10, 64)
		if leftErr != nil || rightErr != nil || right < left {
			t.Fatalf("invalid monotonic DB stat %s: before=%q after=%q", key, before[key], after[key])
		}
		delta[key] = right - left
	}
	return delta
}

func typedWrite768ManagerSnapshot(stats CollectionManagerStats) map[string]any {
	return map[string]any{
		"pending_documents":            stats.PendingDocuments,
		"pending_root_runs":            stats.PendingRootRuns,
		"pending_indexed_flush_units":  stats.PendingIndexedFlushUnits,
		"overlay_queued_units":         stats.OverlayQueuedIndexedFlushUnits,
		"overlay_active_units":         stats.OverlayActiveIndexedFlushUnits,
		"indexed_async_flush_running":  stats.IndexedAsyncFlushRunning,
		"mutation_lock_calls":          stats.MutationLockCalls,
		"mutation_lock_wait_ns":        stats.MutationLockWait.Nanoseconds(),
		"mutation_lock_hold_ns":        stats.MutationLockHold.Nanoseconds(),
		"indexed_flush_calls":          stats.IndexedFlushCalls,
		"indexed_flush_docs":           stats.IndexedFlushDocs,
		"indexed_flush_duration_ns":    stats.IndexedFlushDuration.Nanoseconds(),
		"indexed_flush_materialize_ns": stats.IndexedFlushMaterialize.Nanoseconds(),
		"indexed_flush_pointerize_ns":  stats.IndexedFlushPointerize.Nanoseconds(),
		"indexed_flush_publish_ns":     stats.IndexedFlushPublish.Nanoseconds(),
		"root_delta_plan_entries":      stats.RootDeltaPlanEntries,
	}
}

func typedWrite768JSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func typedWrite768DiagnosticInput(prefix string, offset, rows int) (ids, retained [][]byte, columns []TypedColumnBatch) {
	ids, retained = make([][]byte, rows), make([][]byte, rows)
	columns = []TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, rows)}, {Name: "content", Strings: make([]string, rows)}, {Name: "user", Strings: make([]string, rows)}, {Name: "path", Strings: make([]string, rows)}}
	for row := range rows {
		id := fmt.Sprintf("%s768-%08d", prefix, offset+row)
		ids[row] = []byte(id)
		retained[row] = []byte(fmt.Sprintf(`{"id":%q}`, id))
		columns[0].Float32Vectors[row] = vectorBenchmarkEmbedding(offset+row, 768)
		columns[1].Strings[row] = "diagnostic content"
		columns[2].Strings[row] = fmt.Sprintf("user-%d", row%4)
		columns[3].Strings[row] = fmt.Sprintf("path-%d", row)
	}
	return
}
