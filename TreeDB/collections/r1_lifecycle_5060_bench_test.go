package collections

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This is standalone diagnostic tooling, not a comparator or a retained R1
// comparator packet. Freeze reviewed/landed tooling before retained capture.
// B/op and allocs/op use epochs of callsPerEpoch public calls.
func BenchmarkR1Lifecycle5060(b *testing.B) {
	requireStandaloneColumnProductionAuthorityTest(b)
	documents := r1LifecycleDimension5060(b, "DOCUMENTS", 4096, 32)
	callsPerEpoch := r1LifecycleDimension5060(b, "CALLS_PER_EPOCH", 1024, 8)
	result := r1LifecycleResult5060{Schema: "gomap-r1-lifecycle-result-v3", Epochs: b.N, Documents: documents, CallsPerEpoch: callsPerEpoch}
	result.PID, result.GOMAXPROCS = os.Getpid(), runtime.GOMAXPROCS(0)
	result.Schedule = r1LifecycleSchedule5060
	operations := []string{"ordinary_get", "prepared_get", "indexed_update", "typed_replace", "delete", "typed_insert", "typed_upsert", "post_upsert_get"}
	result.Operations = make(map[string]uint64, len(operations))
	dir, db, col, cleanup := r1LifecycleNew5060(b, true)
	result.FreshProfile = r1LifecycleProfile5060(b, db)
	result.BackendProfile = string(db.ResolvedProfile())
	var candidates []ColumnAssetRef
	defer func() {
		if err := cleanup(); err != nil {
			b.Error(err)
		}
	}()
	want, known := make(map[string]map[string]any, documents), r1MutationKnown5059()
	fixture := sha256.New()
	for start := 0; start < documents; start += 32 {
		rows := make([]map[string]any, 32)
		for i := range rows {
			rows[i] = r1MutationRow5059(start + i)
			fixture.Write(r1MutationJSON5059(b, rows[i]))
			fixture.Write([]byte{'\n'})
			want[rows[i]["id"].(string)] = rows[i]
		}
		ids, retained, columns := r1MutationBatch5059(b, rows...)
		if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
			b.Fatal(err)
		}
		r1MutationRemember5059(known, rows...)
	}
	r1LifecycleCurrent5060(b, col, want, known)
	held, err := col.OpenCollectionReadView()
	if err != nil {
		b.Fatal(err)
	}
	defer held.Close()
	captured := make(map[string]map[string]any, len(want))
	for id, row := range want {
		captured[id] = r1MutationCopy5059(row)
	}
	r1LifecycleAssert5060(b, held, captured, known)
	result.FixtureSHA256 = fmt.Sprintf("%x", fixture.Sum(nil))
	result.Census = append(result.Census, r1LifecycleCensus5060(b, dir, "ingest"))

	samples := make([]int64, 0, callsPerEpoch*b.N)
	seenIDs := make(map[string]int, min(documents, callsPerEpoch/2))
	var heapHigh, loopBytes, loopAllocs, apiElapsed, totalCalls uint64
	b.ReportAllocs()
	b.ResetTimer()
	for epoch := 0; epoch < b.N; epoch++ {
		b.StopTimer()
		epochIDs := make(map[string]struct{}, min(documents, callsPerEpoch/2))
		// Every two calls address one row; only index fields change on indexed
		// updates. The live population is restored before the next pair.
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		b.StartTimer()
		for call := range callsPerEpoch {
			// Repeat the same deterministic bounded working set every epoch.
			ordinal := ((call / 2) * 37) % documents
			id := fmt.Sprintf("row-%03d", ordinal)
			current := want[id]
			started := time.Now()
			switch (call / 2) % 4 {
			case 0: // ordinary public complete row, followed by prepared row
				var raw []byte
				if call%2 == 0 {
					raw, err = col.Get([]byte(id))
				} else {
					var view *CollectionReadView
					view, err = col.OpenCollectionReadView()
					if err == nil {
						var response DocumentFetchResponse
						response, err = view.FetchDocumentsByID([][]byte{[]byte(id)}, DocumentFetchOptions{})
						if err == nil && (len(response.Results) != 1 || !response.Results[0].Found) {
							b.Fatal("prepared point missing")
						}
						if err == nil {
							raw = response.Results[0].Document
						}
						err = errors.Join(err, view.Close())
					}
				}
				if err == nil {
					var got map[string]any
					if e := json.Unmarshal(raw, &got); e != nil || !reflect.DeepEqual(got, current) {
						b.Fatalf("ordinary/prepared full row id=%s got=%s want=%s err=%v", id, raw, r1MutationJSON5059(b, current), e)
					}
				}
			case 1: // indexed generic update and nonindexed typed replacement
				changed := r1MutationCopy5059(current)
				changed["revision"] = current["revision"].(float64) + 1
				if call%2 == 0 {
					changed["email"] = fmt.Sprintf("epoch-%d-call-%d@example.test", epoch, call)
					changed["city"] = fmt.Sprintf("city-%d", (ordinal+epoch+1)%8)
					var result []UpdateBatchResult
					result, err = col.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte(id), Update: func(raw []byte) ([]byte, bool, error) { return r1MutationJSON5059(b, changed), true, nil }}})
					if err == nil && !reflect.DeepEqual(result, []UpdateBatchResult{{true, true}}) {
						b.Fatalf("update result=%+v", result)
					}
				} else {
					changed["bio"] = fmt.Sprintf("epoch %d retained-row biography 雪", epoch)
					ids, retained, columns := r1MutationBatch5059(b, changed)
					var result []UpdateBatchResult
					result, err = col.ReplaceTypedBatch(ids, retained, columns)
					if err == nil && !reflect.DeepEqual(result, []UpdateBatchResult{{true, true}}) {
						b.Fatalf("replace result=%+v", result)
					}
				}
				want[id] = changed
				r1MutationRemember5059(known, changed)
			case 2: // delete then reinsert the same complete row
				if call%2 == 0 {
					var deleted int
					deleted, err = col.DeleteBatch([][]byte{[]byte(id)})
					if err == nil && deleted != 1 {
						b.Fatalf("delete=%d", deleted)
					}
				} else {
					ids, retained, columns := r1MutationBatch5059(b, current)
					_, _, err = col.InsertTypedBatchWithStats(ids, retained, columns)
				}
			case 3: // typed upsert existing row, followed by complete ordinary read
				if call%2 == 0 {
					changed := r1MutationCopy5059(current)
					changed["score"], changed["active"] = float64(epoch+call)+.25, !current["active"].(bool)
					ids, retained, columns := r1MutationBatch5059(b, changed)
					var matched int
					matched, err = col.UpsertTypedBatch(ids, retained, columns)
					if err == nil && matched != 1 {
						b.Fatalf("upsert matched=%d", matched)
					}
					want[id] = changed
				} else {
					var raw []byte
					raw, err = col.Get([]byte(id))
					if err == nil {
						var got map[string]any
						if e := json.Unmarshal(raw, &got); e != nil || !reflect.DeepEqual(got, current) {
							b.Fatalf("post-upsert complete row=%s err=%v", raw, e)
						}
					}
				}
			}
			elapsed := uint64(time.Since(started).Nanoseconds())
			if err != nil {
				b.Fatal(err)
			}
			samples = append(samples, int64(elapsed))
			apiElapsed += elapsed
			totalCalls++
			result.Operations[operations[(call/2)%4*2+call%2]]++
			epochIDs[id] = struct{}{}
		}
		b.StopTimer()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		loopBytes += after.TotalAlloc - before.TotalAlloc
		loopAllocs += after.Mallocs - before.Mallocs
		heapHigh = max(heapHigh, before.HeapAlloc, after.HeapAlloc)
		coverage := r1LifecycleIDCoverage5060{Epoch: epoch, DistinctIDs: len(epochIDs)}
		ids := make([]string, 0, len(epochIDs))
		for id := range epochIDs {
			ids = append(ids, id)
			if seenIDs[id] > 0 {
				coverage.RevisitedIDs++
			}
			seenIDs[id]++
		}
		slices.Sort(ids)
		coverage.IDsSHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(ids, "\n")+"\n")))
		coverage.NewIDs = coverage.DistinctIDs - coverage.RevisitedIDs
		coverage.CumulativeDistinctIDs = len(seenIDs)
		result.IDCoverage = append(result.IDCoverage, coverage)
		result.RevisitedIDs += coverage.RevisitedIDs
		r1LifecycleCurrent5060(b, col, want, known)
		if epoch == 0 {
			r1LifecycleAssert5060(b, held, captured, known)
		}
		result.Census = append(result.Census, r1LifecycleCensus5060(b, dir, fmt.Sprintf("churn-%d", epoch)))
		observe := func(phase string) {
			r1LifecycleCurrent5060(b, col, want, known)
			if epoch == 0 {
				r1LifecycleAssert5060(b, held, captured, known)
			}
			result.Census = append(result.Census, r1LifecycleCensus5060(b, dir, fmt.Sprintf("%s-%d", phase, epoch)))
		}
		result.Maintenance = append(result.Maintenance, r1LifecycleFold5060(b, db, col, epoch, &candidates, observe))
		if epoch == 0 {
			r1LifecycleAssert5060(b, held, captured, known)
			if err := held.Close(); err != nil {
				b.Fatal(err)
			}
			if stats := held.assetManager.Stats(); stats.ActiveHandles != 0 {
				b.Fatalf("held view release leaked handles: %+v", stats)
			}
			released := r1LifecycleReclaim5060(b, db, col, &candidates)
			result.AfterViewReleaseFinal = new(r1LifecycleFinalRecord5060)
			*result.AfterViewReleaseFinal = r1LifecycleFinal5060(b, db, col, &candidates)
			result.AfterViewReleaseGC = &released
			r1LifecycleCurrent5060(b, col, want, known)
			result.Census = append(result.Census, r1LifecycleCensus5060(b, dir, "after_view_release"))
		}
		b.StartTimer()
	}
	b.StopTimer()
	slices.Sort(samples)
	b.ReportMetric(float64(callsPerEpoch), "calls/op")
	b.ReportMetric(float64(apiElapsed)/float64(totalCalls), "loop-ns/call")
	b.ReportMetric(float64(totalCalls)*1e9/float64(apiElapsed), "mixed-calls/s")
	b.ReportMetric(float64(samples[(len(samples)-1)*95/100]), "mixed-p95-ns/call")
	b.ReportMetric(float64(samples[(len(samples)-1)*99/100]), "mixed-p99-ns/call")
	b.ReportMetric(float64(loopBytes)/float64(totalCalls), "loop-B/call")
	b.ReportMetric(float64(loopAllocs)/float64(totalCalls), "loop-allocs/call")
	b.ReportMetric(float64(heapHigh), "sampled-heap-high-B")
	if err := cleanup(); err != nil {
		b.Fatal(err)
	}
	db, cleanup, result.ReopenProfile = r1LifecycleOpen5060(b, dir)
	col, err = NewCollectionManager(db).OpenCollection("r1")
	if err != nil {
		b.Fatal(err)
	}
	r1LifecycleCurrent5060(b, col, want, known)
	result.Census = append(result.Census, r1LifecycleCensus5060(b, dir, "reopen"))
	runtime.GC()
	var retained runtime.MemStats
	runtime.ReadMemStats(&retained)
	runtime.KeepAlive(want)
	runtime.KeepAlive(known)
	runtime.KeepAlive(captured)
	runtime.KeepAlive(samples)
	runtime.KeepAlive(seenIDs)
	runtime.KeepAlive(candidates)
	b.ReportMetric(float64(retained.HeapAlloc), "process-retained-heap-B")
	result.TotalCalls, result.CallNS, result.LoopBytes, result.LoopAllocs = totalCalls, apiElapsed, loopBytes, loopAllocs
	result.P95NS, result.P99NS = uint64(samples[(len(samples)-1)*95/100]), uint64(samples[(len(samples)-1)*99/100])
	result.SampledHeapHigh, result.ProcessRetainedHeap = heapHigh, retained.HeapAlloc
	result.DistinctIDs = len(seenIDs)
	for _, epochsSeen := range seenIDs {
		if epochsSeen > 1 {
			result.RevisitedDistinctIDs++
		}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("R1_LIFECYCLE_RESULT %s", raw)
	b.Log("heap high is sampled after each call epoch, not an exact peak; retained heap includes benchmark oracle and latency samples; RSS and unsampled peak unavailable; timings/allocations include caller encoding and oracle bookkeeping; maintenance and phase oracle excluded from call timers")
}

// Logical lengths, not allocated blocks; the walk includes every regular file
// once and preserves unknown files as other rather than silently dropping them.
func r1LifecycleCensus5060(b testing.TB, dir, phase string) r1LifecycleCensusRecord5060 {
	b.Helper()
	bytes, files := make(map[string]int64), make(map[string]int)
	for _, component := range []string{"index", "persistent_vlog", "persistent_leaf_log", "typed_assets", "redo_wal", "dictionary_store", "template_store", "immutable_manifest_metadata", "other"} {
		bytes[component], files[component] = 0, 0
	}
	paths := make(map[string]r1LifecycleFileRecord5060)
	var totalBytes int64
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("census nonregular file: %s", path)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		component := r1LifecycleFileComponent5060(rel)
		bytes[component] += info.Size()
		files[component]++
		paths[rel] = r1LifecycleFileRecord5060{Component: component, Bytes: info.Size()}
		totalBytes += info.Size()
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("phase=%s logical_bytes=%v regular_files=%v", phase, bytes, files)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	b.Logf("phase=%s process_heap_alloc=%d heap_inuse=%d heap_objects=%d num_gc=%d (sampled, no forced GC)", phase, memory.HeapAlloc, memory.HeapInuse, memory.HeapObjects, memory.NumGC)
	return r1LifecycleCensusRecord5060{Scope: "full-profile-root", Files: paths, TotalBytes: totalBytes, TotalFiles: len(paths), Phase: phase, LogicalBytes: bytes, RegularFiles: files, HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse, HeapObjects: memory.HeapObjects, NumGC: memory.NumGC}
}

func r1LifecycleDimension5060(b *testing.B, suffix string, fallback, multiple int) int {
	b.Helper()
	key := "GOMAP_R1_LIFECYCLE_" + suffix
	if value := os.Getenv(key); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < multiple || n%multiple != 0 || n > 1<<20 {
			b.Fatalf("%s must be a multiple of %d in [%d,1048576]", key, multiple, multiple)
		}
		return n
	}
	return fallback
}

type r1LifecycleResult5060 struct {
	FreshProfile          r1LifecycleProfileRecord5060       `json:"fresh_profile"`
	ReopenProfile         r1LifecycleProfileRecord5060       `json:"reopen_profile"`
	AfterViewReleaseFinal *r1LifecycleFinalRecord5060        `json:"after_view_release_final"`
	Schedule              string                             `json:"schedule"`
	BackendProfile        string                             `json:"backend_profile"`
	DistinctIDs           int                                `json:"distinct_ids"`
	RevisitedIDs          int                                `json:"cross_epoch_revisited_ids"`
	RevisitedDistinctIDs  int                                `json:"revisited_distinct_ids"`
	IDCoverage            []r1LifecycleIDCoverage5060        `json:"id_coverage"`
	AfterViewReleaseGC    *r1LifecycleReclaimRecord5060      `json:"after_view_release_gc"`
	PID                   int                                `json:"pid"`
	GOMAXPROCS            int                                `json:"gomaxprocs"`
	Schema                string                             `json:"schema"`
	Epochs                int                                `json:"epochs"`
	Documents             int                                `json:"documents"`
	CallsPerEpoch         int                                `json:"calls_per_epoch"`
	FixtureSHA256         string                             `json:"fixture_sha256"`
	TotalCalls            uint64                             `json:"total_calls"`
	CallNS                uint64                             `json:"call_ns"`
	LoopBytes             uint64                             `json:"loop_bytes"`
	LoopAllocs            uint64                             `json:"loop_allocs"`
	P95NS                 uint64                             `json:"p95_ns"`
	P99NS                 uint64                             `json:"p99_ns"`
	SampledHeapHigh       uint64                             `json:"sampled_heap_high_bytes"`
	ProcessRetainedHeap   uint64                             `json:"process_retained_heap_bytes"`
	Census                []r1LifecycleCensusRecord5060      `json:"census"`
	Maintenance           []r1LifecycleMaintenanceRecord5060 `json:"maintenance"`
	Operations            map[string]uint64                  `json:"operations"`
}

type r1LifecycleFileRecord5060 struct {
	Component string `json:"component"`
	Bytes     int64  `json:"bytes"`
}

type r1LifecycleCensusRecord5060 struct {
	Scope        string                               `json:"scope"`
	Files        map[string]r1LifecycleFileRecord5060 `json:"files"`
	TotalBytes   int64                                `json:"total_bytes"`
	TotalFiles   int                                  `json:"total_files"`
	Phase        string                               `json:"phase"`
	LogicalBytes map[string]int64                     `json:"logical_bytes"`
	RegularFiles map[string]int                       `json:"regular_files"`
	HeapAlloc    uint64                               `json:"heap_alloc"`
	HeapInuse    uint64                               `json:"heap_inuse"`
	HeapObjects  uint64                               `json:"heap_objects"`
	NumGC        uint32                               `json:"num_gc"`
}

type r1LifecycleMaintenanceRecord5060 struct {
	Full                r1LifecycleFullRecord5060    `json:"full"`
	Final               r1LifecycleFinalRecord5060   `json:"final"`
	Epoch               int                          `json:"epoch"`
	FlushNS             int64                        `json:"flush_ns"`
	CheckpointNS        int64                        `json:"checkpoint_ns"`
	BeforeFoldGCNS      int64                        `json:"before_fold_gc_ns"`
	BeforeFoldGC        ColumnAssetGCStats           `json:"before_fold_gc"`
	FoldNS              int64                        `json:"fold_ns"`
	Fold                r1LifecycleFoldRecord5060    `json:"fold"`
	FoldCheckpointNS    int64                        `json:"fold_checkpoint_ns"`
	OverlayNS           int64                        `json:"overlay_ns"`
	Overlay             any                          `json:"overlay"`
	OverlayCheckpointNS int64                        `json:"overlay_checkpoint_ns"`
	Reclaim             r1LifecycleReclaimRecord5060 `json:"reclaim"`
	VlogGCNS            int64                        `json:"vlog_gc_ns"`
	ValueLogGC          backenddb.ValueLogGCStats    `json:"vlog_gc"`
	VacuumNS            int64                        `json:"vacuum_ns"`
	Vacuum              backenddb.VacuumOnlineStats  `json:"vacuum"`
	MaintenanceNS       int64                        `json:"maintenance_ns"`
}

type r1LifecycleIDCoverage5060 struct {
	Epoch                 int    `json:"epoch"`
	DistinctIDs           int    `json:"distinct_ids"`
	NewIDs                int    `json:"new_ids"`
	RevisitedIDs          int    `json:"revisited_ids"`
	CumulativeDistinctIDs int    `json:"cumulative_distinct_ids"`
	IDsSHA256             string `json:"ids_sha256"`
}

// Retain all aggregate attribution without per-reference/segment lists.
func r1LifecycleAggregateGC5060(stats ColumnAssetGCStats) ColumnAssetGCStats {
	stats.Plan.Entries, stats.Plan.SegmentEntries = nil, nil
	return stats
}

func r1LifecycleFileComponent5060(rel string) string {
	switch {
	case strings.Contains(rel, "/wal/"):
		return "redo_wal"
	case strings.HasPrefix(rel, "dictdb/"):
		return "dictionary_store"
	case strings.HasPrefix(rel, "templatedb/"):
		return "template_store"
	case strings.HasPrefix(filepath.Base(rel), "manifest.durable."):
		return "immutable_manifest_metadata"
	case strings.Contains(rel, "/value_vlog/"):
		return "persistent_vlog"
	case strings.Contains(rel, "leaf_vlog/"):
		return "persistent_leaf_log"
	case strings.Contains(rel, "column-assets/") || strings.Contains(rel, "column_assets/"):
		return "typed_assets"
	case strings.HasSuffix(rel, ".db") || strings.HasSuffix(rel, "index.db.bak") || strings.HasSuffix(rel, "index.db.new"):
		return "index"
	default:
		return "other"
	}
}
