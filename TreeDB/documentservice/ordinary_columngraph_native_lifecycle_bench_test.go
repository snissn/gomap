package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// BenchmarkOrdinaryColumnGraphNativeLifecycle4096 measures the public durable
// 4096/128 sequential seed, Optimize and explicit Checkpoint phases. Each op
// owns a fresh directory. Standard ns/op, B/op and allocs/op sum those three
// phases; setup, observation, full-content/query checks, Close and reopen are
// outside the timer. Each lifecycle must still complete and recover correctly.
//
// Phase memory deltas are process-wide allocations during that interval, and
// HeapAlloc is an unforced phase-end sample, not a per-operation retained heap
// claim. COW gauges are the existing finite owner accounting. Measure process
// peak RSS externally in a fresh benchmark process; no sampler changes the
// producer path. This is an unaffected successful lifecycle cost control, not a
// deterministic captured-handoff witness or a speedup acceptance threshold.
func BenchmarkOrdinaryColumnGraphNativeLifecycle4096(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()
	ctx := context.Background()
	documents := make([]Document, sharedCOWWriteRows)
	for i := range documents {
		documents[i] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
	}
	var phaseNanos [3]int64
	var phaseBytes, phaseAllocs [3]uint64
	var peakCOW uint64
	parent := b.TempDir()
	b.ResetTimer()
	for load := 0; load < b.N; load++ {
		func() {
			dir, err := os.MkdirTemp(parent, "lifecycle-")
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err := os.RemoveAll(dir); err != nil {
					b.Error(err)
				}
			}()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			owner, err := treedb.Open(opts)
			if err != nil {
				b.Fatal(err)
			}
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				b.Fatal(errors.Join(err, owner.Close()))
			}
			manager := collections.NewCollectionManager(backend)
			svc := New(manager)
			closeAll := func() error {
				if owner == nil {
					return nil
				}
				err := errors.Join(svc.Close(), manager.Close(), owner.Close())
				owner = nil
				return err
			}
			defer func() {
				if err := closeAll(); err != nil {
					b.Errorf("load%d original owner cleanup: %v", load, err)
				}
			}()
			info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: sharedCOWWriteIndex, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
			if err != nil {
				b.Fatal(err)
			}
			checkSharedCOWWriteIndex(b, info)
			measure := func(phase int, name string, operation func() error) {
				b.Helper()
				beforeCounters := ordinaryNativeLifecycleCounters(b, owner.Stats())
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				b.StartTimer()
				start := time.Now()
				err := operation()
				elapsed := time.Since(start).Nanoseconds()
				b.StopTimer()
				runtime.ReadMemStats(&after)
				afterCounters := ordinaryNativeLifecycleCounters(b, owner.Stats())
				if err != nil {
					b.Fatalf("load%d %s: %v; cause: %v", load, name, err, errors.Unwrap(err))
				}
				phaseNanos[phase] += elapsed
				phaseBytes[phase] += after.TotalAlloc - before.TotalAlloc
				phaseAllocs[phase] += after.Mallocs - before.Mallocs
				peakCOW = max(peakCOW, afterCounters["peak_bytes"])
				encoded, err := json.Marshal(struct {
					Load             int               `json:"load"`
					Phase            string            `json:"phase"`
					Nanos            int64             `json:"nanos"`
					AllocatedBytes   uint64            `json:"allocated_bytes"`
					Allocations      uint64            `json:"allocations"`
					HeapAllocAtStart uint64            `json:"heap_alloc_at_start"`
					HeapAllocAtEnd   uint64            `json:"heap_alloc_at_end"`
					Before           map[string]uint64 `json:"cow_before"`
					After            map[string]uint64 `json:"cow_after"`
				}{load, name, elapsed, after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs, before.HeapAlloc, after.HeapAlloc, beforeCounters, afterCounters})
				if err != nil {
					b.Fatal(err)
				}
				b.Logf("LIFECYCLE_PHASE %s", encoded)
			}
			measure(0, "seed", func() error {
				for first := 0; first < len(documents); first += sharedCOWWriteBatchSize {
					out, err := svc.UpsertDocuments(ctx, sharedCOWWriteIndex, UpsertDocumentsRequest{Documents: documents[first : first+sharedCOWWriteBatchSize], DeferVectorIndexRebuild: true})
					if err != nil {
						return err
					}
					if out.Upserted != sharedCOWWriteBatchSize || out.Inserted != sharedCOWWriteBatchSize || out.Updated != 0 {
						return fmt.Errorf("incorrect acknowledged batch [%d:%d): %+v", first, first+sharedCOWWriteBatchSize, out)
					}
				}
				return nil
			})
			var optimized OptimizeIndexResponse
			measure(1, "optimize", func() error {
				var err error
				optimized, err = svc.OptimizeIndex(ctx, sharedCOWWriteIndex, OptimizeIndexRequest{})
				return err
			})
			if !optimized.Status.Loaded || optimized.Status.RebuildNeeded || optimized.Status.Strategy != collections.VectorIndexStrategyColumnGraph {
				b.Fatalf("load%d graph completion=%+v", load, optimized.Status)
			}
			measure(2, "checkpoint", owner.Checkpoint)
			col, err := manager.OpenCollection(sharedCOWWriteIndex)
			if err != nil {
				b.Fatal(err)
			}
			graph, err := col.VectorIndexStatus(defaultVectorIndexName)
			if err != nil || !graph.Loaded || graph.RebuildNeeded || graph.Stats.LiveDocs != sharedCOWWriteRows || graph.Stats.SnapshotDirty {
				b.Fatalf("load%d physical graph=%+v err=%v", load, graph, err)
			}
			b.Logf("load%d completed_graph_rows=%d optimize_calls=1 optimize_timing=%+v manifest=%+v", load, graph.Stats.LiveDocs, optimized.Timing, col.Meta().Options.ColumnStore.ActiveManifest)
			checkSharedCOWWriteDocuments(b, svc, documents)
			checkSharedCOWWriteQueries(b, svc, "native")
			if err := closeAll(); err != nil {
				b.Fatalf("load%d original Close: %v", load, err)
			}
			if stats := backend.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
				b.Fatalf("load%d original owner retained physical resources: %+v", load, stats)
			}
			// A fresh public owner validates all acknowledged data and serving.
			reopened, err := openSharedCOWWriteOwner(opts, "native")
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err := reopened.close(); err != nil {
					b.Errorf("load%d reopened Close: %v", load, err)
				}
			}()
			checkSharedCOWWriteDocuments(b, reopened.service, documents)
			checkSharedCOWWriteQueries(b, reopened.service, "native")
		}()
		// Both owners have closed. This untimed process-retention sample keeps
		// the expected-input fixture alive and includes shared/runtime caches.
		runtime.GC()
		var retained runtime.MemStats
		runtime.ReadMemStats(&retained)
		runtime.KeepAlive(documents)
		b.Logf("LIFECYCLE_CLOSED load=%d heap_alloc_after_gc=%d heap_sys=%d sys=%d", load, retained.HeapAlloc, retained.HeapSys, retained.Sys)
	}
	for i, phase := range []string{"seed", "optimize", "checkpoint"} {
		b.ReportMetric(float64(phaseNanos[i])/float64(b.N), phase+"_ns/load")
		b.ReportMetric(float64(phaseBytes[i])/float64(b.N), phase+"_B/load")
		b.ReportMetric(float64(phaseAllocs[i])/float64(b.N), phase+"_allocs/load")
	}
	b.ReportMetric(float64(peakCOW), "cow_peak_B")
	b.ReportMetric(sharedCOWWriteRows, "records/op")
	b.Log("native command_wal_durable; 4096 rows; 128 rows/request; one writer; no warmup; no request retry; phase observations and validation excluded from standard timer")
}

func ordinaryNativeLifecycleCounters(b *testing.B, stats map[string]string) map[string]uint64 {
	b.Helper()
	out := make(map[string]uint64, 9)
	for _, name := range []string{"total_bytes", "peak_bytes", "retired_bytes", "maintenance_used_bytes", "publications_total", "rollovers_total", "handoffs_total", "txn.logical_seq"} {
		value, err := strconv.ParseUint(stats["treedb.cache.cow."+name], 10, 64)
		if err != nil {
			b.Fatalf("missing COW counter %s: %v", name, err)
		}
		out[name] = value
	}
	value, err := strconv.ParseUint(stats["treedb.cache.checkpoint.runs"], 10, 64)
	if err != nil {
		b.Fatalf("missing checkpoint counter: %v", err)
	}
	out["checkpoint_runs"] = value
	return out
}
