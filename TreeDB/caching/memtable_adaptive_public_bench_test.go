package caching_test

import (
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// Each operation commits one MVCC posting value; every 112 writes acquires a
// public snapshot, reproducing rotation before the adaptive sample minimum.
// Use fixed iteration counts >= 1120 to include a measured mode decision.
func BenchmarkAdaptiveMVCCSnapshotCommandWAL(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, mode := range []string{"adaptive", "append_only", "hash_sorted", "skiplist", "btree"} {
			b.Run(fmt.Sprintf("%s/%s", profile, mode), func(b *testing.B) {
				opts := treedb.OptionsFor(profile, b.TempDir())
				opts.MemtableMode = mode
				opts.MemtableShards = 1
				opts.FlushThreshold = 256 << 20
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				opts.MaxWALBytes = -1
				opts.BackgroundIndexVacuumInterval = -1
				opts.DisableBackgroundPrune = true
				opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
				db, err := treedb.Open(opts)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				store := mvcc.New(db)
				commitMode := mvcc.CommitRelaxed
				if profile == treedb.ProfileCommandWALDurable {
					commitMode = mvcc.CommitDurable
				}
				keys := [][]byte{[]byte("posting-0"), []byte("posting-1"), []byte("posting-2"), []byte("posting-3")}
				value := make([]byte, 512)
				mutations := []mvcc.Mutation{{Value: value}}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					mutations[0].Key = keys[i%len(keys)]
					if err := store.CommitAt(uint64(i+1), mutations, commitMode); err != nil {
						b.Fatal(err)
					}
					if (i+1)%112 == 0 {
						snap := db.AcquireSnapshot()
						if snap == nil {
							b.Fatal("AcquireSnapshot returned nil")
						}
						if err := snap.Close(); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				stats := db.Stats()
				for _, key := range []string{
					"treedb.cache.memtable_stats.writes",
					"treedb.cache.memtable_adaptive.last_writes",
					"treedb.cache.memtable_adaptive.reason_low_data_total",
					"treedb.cache.memtable_adaptive.reason_hash_mixed_total",
					"treedb.cache.memtable_adaptive.reason_append_sequential_total",
					"treedb.command_wal.append.count_total",
					"treedb.command_wal.file_sync.calls_total",
				} {
					n, err := strconv.ParseFloat(stats[key], 64)
					if err != nil {
						b.Fatalf("stat %s=%q: %v", key, stats[key], err)
					}
					b.ReportMetric(n, key)
				}
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/s")
				b.Logf("profile=%s config=%s selected=%s reason=%s", db.ResolvedProfile(), mode,
					stats["treedb.cache.memtable_mode"], stats["treedb.cache.memtable_adaptive.last_reason"])
			})
		}
	}
}
