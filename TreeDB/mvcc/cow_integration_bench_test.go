package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
)

// BenchmarkCOWIntegratedPublicMVCC follows P4's growing-history CommitAt/read
// workload through the production Store and public DB. Collect with fixed
// -benchtime=128x or 256x; this is a tiny diagnostic, not a steady-state load.
func BenchmarkCOWIntegratedPublicMVCC(b *testing.B) {
	caching.SetIteratorDebug(true)
	defer caching.SetIteratorDebug(false)
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, mode := range []string{"append_only", "btree", "cow_btree"} {
			for _, read := range []string{"point", "all_versions"} {
				b.Run(fmt.Sprintf("%s/%s/%s", profile, mode, read), func(b *testing.B) {
					if b.N > 256 {
						b.Fatal("use fixed -benchtime=128x or 256x for this growing-history diagnostic")
					}
					opts := treedb.OptionsFor(profile, b.TempDir())
					opts.DisableSideStores = true
					opts.BackgroundCheckpointInterval = -1
					opts.MemtableMode = mode
					opts.MemtableShards = 8
					opts.FlushThreshold = 16 << 20
					database, err := treedb.Open(opts)
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := database.Close(); err != nil {
							b.Error(err)
						}
					}()
					before := database.Stats()
					if database.ResolvedProfile() != profile || before["treedb.profile.resolved"] != string(profile) || before["treedb.cache.memtable_mode"] != mode {
						b.Fatalf("resolved profile/mode: %s, %q, %q; want %s/%s", database.ResolvedProfile(), before["treedb.profile.resolved"], before["treedb.cache.memtable_mode"], profile, mode)
					}
					store := New(database)
					keys := make([][]byte, 8)
					for n := range keys {
						keys[n] = []byte(fmt.Sprintf("key%d", n))
					}
					value := bytes.Repeat([]byte("v"), 128)
					latencies := make([]int64, b.N)
					var visits, output uint64
					var mem0, mem1 runtime.MemStats
					runtime.ReadMemStats(&mem0)
					before = database.Stats()
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						begin := time.Now()
						key, timestamp := keys[n%8], uint64(n+1)
						if err := store.CommitAt(timestamp, []Mutation{{Key: key, Value: value}}, CommitRelaxed); err != nil {
							b.Fatal(err)
						}
						if read == "point" {
							result, err := store.GetAt(key, timestamp)
							if err != nil || result.State != Present || result.Timestamp != timestamp || !bytes.Equal(result.Value, value) {
								b.Fatalf("GetAt: %+v, %v", result, err)
							}
							output++
						} else {
							stats, count, err := cowIntegrationHistory(store, key, value, timestamp)
							if err != nil {
								b.Fatal(err)
							}
							visits += stats.Visited
							output += count
						}
						latencies[n] = time.Since(begin).Nanoseconds()
					}
					b.StopTimer()
					sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
					for _, p := range []int{50, 95, 99} {
						b.ReportMetric(float64(latencies[(b.N-1)*p/100]), fmt.Sprintf("p%d_ns", p))
					}
					runtime.ReadMemStats(&mem1)
					after := database.Stats()
					for metric, stat := range map[string]string{
						"snapshot_rotations/op": "treedb.cache.snapshot.rotations_total",
						"rotated_shards/op":     "treedb.cache.snapshot.rotated_shards_total",
						"enqueued_records/op":   "treedb.cache.snapshot.enqueued_records_total",
						"wal_appends/op":        "treedb.command_wal.append.count_total",
						"wal_syncs/op":          "treedb.command_wal.file_sync.calls_total",
					} {
						if profile == treedb.ProfileNoWALFast && (metric == "wal_appends/op" || metric == "wal_syncs/op") {
							continue
						}
						x, y := cowIntegrationCounter(b, before, stat), cowIntegrationCounter(b, after, stat)
						if y < x {
							b.Fatalf("counter regressed: %s", stat)
						}
						b.ReportMetric(float64(y-x)/float64(b.N), metric)
					}
					b.ReportMetric(float64(output)/float64(b.N), "output/op")
					if read == "all_versions" {
						b.ReportMetric(float64(visits)/float64(b.N), "visited/op")
					}
					b.ReportMetric(float64(mem1.HeapAlloc), "heap_end_B")
					b.ReportMetric(float64(mem1.TotalAlloc-mem0.TotalAlloc)/float64(b.N), "process_alloc_B/op")
					if mode == "cow_btree" {
						for _, key := range []string{"total_bytes", "history_bytes", "reserved_bytes", "retired_bytes", "peak_bytes", "control_bytes", "deferred_bytes", "external_bytes", "views", "generations", "sources", "external_leases", "active_cuts", "capture_calls_total", "prepare_calls_total", "publications_total", "rollovers_total", "handoffs_total", "current_roots", "frozen_roots"} {
							b.ReportMetric(float64(cowIntegrationCounter(b, after, "treedb.cache.cow."+key)), "fixture_cow_"+key)
						}
					}
				})
			}
		}
	}
}

func cowIntegrationCounter(b *testing.B, stats map[string]string, key string) uint64 {
	b.Helper()
	value, ok := stats[key]
	if !ok {
		b.Fatalf("missing actual counter %s", key)
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		b.Fatalf("counter %s: %v", key, err)
	}
	return n
}

func cowIntegrationHistory(store *Store, key, value []byte, latest uint64) (stats VersionIteratorStats, output uint64, err error) {
	it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: key})
	if err != nil {
		return stats, output, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	want := (latest-1)/8 + 1
	for it.Valid() {
		entry := it.EntryView()
		if output >= want || entry.State != Present || !bytes.Equal(entry.Key, key) || entry.Timestamp != latest-8*output || !bytes.Equal(entry.Value, value) {
			return stats, output, fmt.Errorf("incorrect exact-key history at timestamp %d, entry %d: %+v", latest, output, entry)
		}
		output++
		it.Next()
	}
	stats = it.Stats()
	if err := it.Error(); err != nil {
		return stats, output, err
	}
	if output != want || stats.Visited != output || stats.Retained != output || stats.Skipped != 0 {
		return stats, output, fmt.Errorf("history at %d: output %d want %d, stats %+v", latest, output, want, stats)
	}
	return stats, output, nil
}
