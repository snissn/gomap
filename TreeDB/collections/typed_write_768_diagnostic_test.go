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

type typedWrite768SearchResult struct {
	WallNS        int64
	BaseANNScored uint64
	DeltaScored   uint64
	Started       time.Time
	Ended         time.Time
}

// Opt-in phase diagnostic, not a latency assertion or a vector-recall benchmark.
// Input generation, empty graph capture, stats collection, and clean Close are
// outside the measured interval. Every timed request is a durable public typed
// upsert of previously absent IDs with two scalar indexes and one text index.
func TestTypedWrite768Diagnostic(t *testing.T) {
	if os.Getenv("GOMAP_WRITE_768_DIAGNOSTIC") != "1" {
		t.Skip("set GOMAP_WRITE_768_DIAGNOSTIC=1 to run the bounded durable-write sweep")
	}
	requireTypedGraphPublicServingTest(t)
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
				beforeManager := col.manager.StatsSnapshot()
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
				afterManager := col.manager.StatsSnapshot()
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
				t.Logf("write768_summary layer=collections_sync preload=%d rows=%d writers=%d calls=%d wall_ns=%d rows_per_second=%.3f db_delta=%s manager_delta=%s pending=%s", preload, rows, writers, calls, elapsed.Nanoseconds(), float64(rows*calls)/elapsed.Seconds(), typedWrite768JSON(t, delta), typedWrite768JSON(t, typedWrite768ManagerDelta(t, beforeManager, afterManager)), typedWrite768JSON(t, typedWrite768ManagerGauges(afterManager)))
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
					for _, id := range in.ids {
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
	requireTypedGraphPublicServingTest(t)
	const preload, calls, rows, readsPerReader = 5000, 16, 256, 256
	for _, cell := range []struct {
		readers, writers int
		state            string
	}{{1, 0, "base"}, {4, 0, "base"}, {4, 0, "postwrite"}, {0, 1, "base"}, {4, 1, "base"}} {
		t.Run(fmt.Sprintf("readers%d/writers%d/state%s", cell.readers, cell.writers, cell.state), func(t *testing.T) {
			dir, db, col := openTypedWrite768Diagnostic(t, preload, calls*rows)
			defer db.Close()

			type input struct {
				ids, retained [][]byte
				columns       []TypedColumnBatch
			}
			inputs := make([]input, calls)
			for call := range inputs {
				inputs[call].ids, inputs[call].retained, inputs[call].columns = typedWrite768DiagnosticInput("mixed", call*rows, rows)
			}
			if cell.state == "postwrite" {
				for _, in := range inputs {
					if updated, _, err := col.UpsertTypedBatchWithStats(in.ids, in.retained, in.columns); err != nil || updated != 0 {
						t.Fatalf("prepare postwrite state: updated=%d err=%v", updated, err)
					}
				}
			}
			query := vectorBenchmarkEmbedding(7, 768)
			for range 3 {
				if _, err := typedWrite768Search(col, query); err != nil {
					t.Fatal(err)
				}
			}
			results := make([]typedWrite768Result, calls)
			searches := make([][]typedWrite768SearchResult, cell.readers)
			beforeDB := db.Stats()
			beforeManager := col.manager.StatsSnapshot()
			start := make(chan struct{})
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
					for range readsPerReader {
						result, err := typedWrite768Search(col, query)
						if err != nil {
							errs <- err
							return
						}
						searches[reader] = append(searches[reader], result)
					}
				}()
			}
			var writerStarted, writerEnded time.Time
			if cell.writers > 0 {
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
			}
			ready.Wait()
			cellStarted := time.Now()
			close(start)
			readers.Wait()
			writers.Wait()
			cellElapsed := time.Since(cellStarted)
			afterDB := db.Stats()
			afterManager := col.manager.StatsSnapshot()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}

			var all []int64
			var baseANNScored, deltaScored uint64
			for _, readerSearches := range searches {
				for _, search := range readerSearches {
					if cell.writers > 0 && (!search.Ended.After(writerStarted) || !search.Started.Before(writerEnded)) {
						continue
					}
					all = append(all, search.WallNS)
					baseANNScored += search.BaseANNScored
					deltaScored += search.DeltaScored
				}
			}
			if cell.readers > 0 && len(all) == 0 {
				t.Fatal("reader interval produced no samples")
			}
			slices.Sort(all)
			summary := map[string]any{
				"phase": "write768_interaction", "preload": preload,
				"layer": "collections_sync", "state": cell.state,
				"readers": cell.readers, "writers": cell.writers,
				"cell_wall_ns": cellElapsed.Nanoseconds(), "read_samples": len(all),
				"base_ann_scored": baseANNScored, "delta_scored": deltaScored,
				"db_delta":      typedWrite768StatsDelta(t, beforeDB, afterDB),
				"manager_delta": typedWrite768ManagerDelta(t, beforeManager, afterManager),
				"pending":       typedWrite768ManagerGauges(afterManager),
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
				for _, input := range inputs {
					for _, id := range input.ids {
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
	if err != nil || preload < 0 || preload > 10000 {
		t.Fatal("GOMAP_WRITE_768_PRELOAD must be in [0,10000]")
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
	opts.Owners = typedGraphReadOwnerLimits{Owners: 16, States: 16, StateBytes: 512 << 20, AssetBytes: 1 << 30, Cold: cold, Physical: typedGraphPhysicalResourceLimits{Segments: 4096, Descriptors: 4096, MappedBytes: 2 << 30, FallbackBytes: 2 << 30, InventoryBytes: 128 << 20}}
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

func typedWrite768Search(col *Collection, query []float32) (typedWrite768SearchResult, error) {
	var buffer VectorIndexSearchBuffer
	started := time.Now()
	response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: query, TopK: 10, EfSearch: 128, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if view != nil {
		if closeErr := view.Close(); err == nil {
			err = closeErr
		}
	}
	ended := time.Now()
	elapsed := ended.Sub(started)
	if err == nil && (len(response.Results) != 10 || !response.Stats.ColumnGraphWork.Completed) {
		err = fmt.Errorf("incomplete public column_graph search: results=%d work=%+v", len(response.Results), response.Stats.ColumnGraphWork)
	}
	work := response.Stats.ColumnGraphWork
	return typedWrite768SearchResult{WallNS: elapsed.Nanoseconds(), BaseANNScored: work.BaseANNScored, DeltaScored: work.DeltaScored, Started: started, Ended: ended}, err
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

func typedWrite768ManagerGauges(stats CollectionManagerStats) map[string]any {
	return map[string]any{
		"pending_bytes":               stats.PendingBytes,
		"pending_documents":           stats.PendingDocuments,
		"pending_root_runs":           stats.PendingRootRuns,
		"pending_indexed_flush_units": stats.PendingIndexedFlushUnits,
		"pending_publication_bytes":   stats.PendingIndexedPublicationBytes,
		"overlay_mutable_documents":   stats.OverlayMutableDocuments,
		"overlay_queued_units":        stats.OverlayQueuedIndexedFlushUnits,
		"overlay_active_units":        stats.OverlayActiveIndexedFlushUnits,
		"overlay_visible_depth":       stats.OverlayVisibleDepth,
		"indexed_async_flush_running": stats.IndexedAsyncFlushRunning,
	}
}

func typedWrite768ManagerDelta(t *testing.T, before, after CollectionManagerStats) map[string]uint64 {
	t.Helper()
	delta := func(name string, left, right uint64) uint64 {
		if right < left {
			t.Fatalf("invalid monotonic collection stat %s: before=%d after=%d", name, left, right)
		}
		return right - left
	}
	return map[string]uint64{
		"mutation_lock_calls":          delta("mutation_lock_calls", before.MutationLockCalls, after.MutationLockCalls),
		"mutation_lock_wait_ns":        delta("mutation_lock_wait_ns", uint64(before.MutationLockWait), uint64(after.MutationLockWait)),
		"mutation_lock_hold_ns":        delta("mutation_lock_hold_ns", uint64(before.MutationLockHold), uint64(after.MutationLockHold)),
		"indexed_stage_batches":        delta("indexed_stage_batches", before.IndexedStageBatches, after.IndexedStageBatches),
		"indexed_stage_docs":           delta("indexed_stage_docs", before.IndexedStageDocs, after.IndexedStageDocs),
		"indexed_flush_calls":          delta("indexed_flush_calls", before.IndexedFlushCalls, after.IndexedFlushCalls),
		"indexed_flush_docs":           delta("indexed_flush_docs", before.IndexedFlushDocs, after.IndexedFlushDocs),
		"indexed_flush_duration_ns":    delta("indexed_flush_duration_ns", uint64(before.IndexedFlushDuration), uint64(after.IndexedFlushDuration)),
		"indexed_flush_materialize_ns": delta("indexed_flush_materialize_ns", uint64(before.IndexedFlushMaterialize), uint64(after.IndexedFlushMaterialize)),
		"indexed_flush_pointerize_ns":  delta("indexed_flush_pointerize_ns", uint64(before.IndexedFlushPointerize), uint64(after.IndexedFlushPointerize)),
		"indexed_flush_publish_ns":     delta("indexed_flush_publish_ns", uint64(before.IndexedFlushPublish), uint64(after.IndexedFlushPublish)),
		"root_delta_plan_entries":      delta("root_delta_plan_entries", before.RootDeltaPlanEntries, after.RootDeltaPlanEntries),
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
