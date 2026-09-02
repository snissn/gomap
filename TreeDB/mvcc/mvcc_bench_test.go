package mvcc

import (
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func openBenchDB(b *testing.B) *treedb.DB {
	b.Helper()
	db := openTestDB(b, b.TempDir(), treedb.DurabilityWALOffRelaxed)
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func openBenchDBWithMemtableMode(b *testing.B, mode string) *treedb.DB {
	b.Helper()
	db, err := treedb.Open(treedb.Options{
		Dir:                          b.TempDir(),
		Durability:                   treedb.DurabilityWALOffRelaxed,
		CommandWAL:                   false,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
		MemtableMode:                 mode,
	})
	if err != nil {
		b.Fatalf("Open memtable mode %q: %v", mode, err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkCommitAt(b *testing.B) {
	for _, batchSize := range []int{1, 32} {
		b.Run(fmt.Sprintf("DirectTreeDB/%d", batchSize), func(b *testing.B) {
			db := openBenchDB(b)
			value := []byte("value")
			logicalKeys := make([][]byte, batchSize)
			for i := range logicalKeys {
				logicalKeys[i] = []byte(fmt.Sprintf("key-%03d", i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				write := db.NewBatchWithSize(batchSize)
				for keyIndex := 0; keyIndex < batchSize; keyIndex++ {
					key, err := mvcckey.Encode(logicalKeys[keyIndex], uint64(i+1))
					if err != nil {
						b.Fatal(err)
					}
					if err := write.Set(key, append([]byte{recordValueV1}, value...)); err != nil {
						b.Fatal(err)
					}
				}
				if err := write.Write(); err != nil {
					b.Fatal(err)
				}
				if err := write.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("MVCC/%d", batchSize), func(b *testing.B) {
			db := openBenchDB(b)
			store := New(db)
			mutations := make([]Mutation, batchSize)
			for i := range mutations {
				mutations[i] = Mutation{Key: []byte(fmt.Sprintf("key-%03d", i)), Value: []byte("value")}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.CommitAt(uint64(i+1), mutations, CommitRelaxed); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCommitAtCommandWALRelaxedSingleton exercises the production-shaped
// cached command-WAL path used by Dgraph. BenchmarkCommitAt deliberately uses
// the no-WAL profile and is a useful MVCC-front-end control, but it does not
// expose the asynchronous canonical-flush/root-publication work that dominates
// the live relaxed profile.
func BenchmarkCommitAtCommandWALRelaxedSingleton(b *testing.B) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, b.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	// Keep the reproduction bounded while forcing several background flushes in
	// a short benchmark. One shard makes source/flush accounting deterministic;
	// the live Dgraph gate retains its production defaults.
	opts.FlushThreshold = 64 << 10
	opts.MemtableShards = 1
	db, err := treedb.Open(opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	store := New(db)
	logicalKeys := [][]byte{
		[]byte("dgraph-posting-0"),
		[]byte("dgraph-posting-1"),
		[]byte("dgraph-posting-2"),
		[]byte("dgraph-posting-3"),
	}
	value := make([]byte, 512)
	before := db.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mutation := []Mutation{{Key: logicalKeys[i%len(logicalKeys)], Value: value}}
		if err := store.CommitAt(uint64(i+1), mutation, CommitRelaxed); err != nil {
			b.Fatalf("CommitAt(%d): %v", i+1, err)
		}
	}
	b.StopTimer()
	if err := db.Checkpoint(); err != nil {
		b.Fatalf("Checkpoint: %v", err)
	}
	after := db.Stats()
	commandAppends := benchmarkStatDelta(b, before, after, "treedb.command_wal.append.count_total")
	pointAppends := benchmarkStatDelta(b, before, after, "treedb.command_wal.append.point.count_total")
	payloadAppends := benchmarkStatDelta(b, before, after, "treedb.command_wal.append.payload.count_total")
	backendWrites := benchmarkStatDelta(b, before, after, "treedb.cache.flush_apply.batches_total")
	units := benchmarkStatDelta(b, before, after, "treedb.cache.flush_apply.units_total")
	entries := benchmarkStatDelta(b, before, after, "treedb.cache.flush_apply.entries_total")
	b.ReportMetric(commandAppends/float64(b.N), "command_wal_appends/op")
	b.ReportMetric(pointAppends/float64(b.N), "point_appends/op")
	b.ReportMetric(payloadAppends/float64(b.N), "payload_appends/op")
	b.ReportMetric(backendWrites/float64(b.N), "backend_writes/op")
	if backendWrites > 0 {
		b.ReportMetric(units/backendWrites, "memtables/backend_write")
		b.ReportMetric(entries/backendWrites, "entries/backend_write")
	}
}

func benchmarkStatDelta(b *testing.B, before, after map[string]string, key string) float64 {
	b.Helper()
	parse := func(stats map[string]string) uint64 {
		value, ok := stats[key]
		if !ok {
			b.Fatalf("missing stat %q", key)
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			b.Fatalf("parse stat %q=%q: %v", key, value, err)
		}
		return parsed
	}
	start, finish := parse(before), parse(after)
	if finish < start {
		b.Fatalf("stat %q regressed from %d to %d", key, start, finish)
	}
	return float64(finish - start)
}

// BenchmarkCommitGroupAtDgraphShape models the c4 external-MVCC publication
// wedge: four independently timestamped caller commits, each with a small
// mutation batch. The grouped row must issue one public TreeDB batch write per
// four commits, while the baseline preserves the existing CommitAt boundary.
func BenchmarkCommitGroupAtDgraphShape(b *testing.B) {
	const commitsPerPublication = 4
	for _, grouped := range []bool{false, true} {
		name := "CommitAt_x4"
		if grouped {
			name = "CommitGroupAt_x4"
		}
		b.Run(name, func(b *testing.B) {
			db := openBenchDB(b)
			countingDB := &benchmarkBatchCounterDB{DB: db}
			store := newStore(countingDB)
			groups := make([]CommitGroup, commitsPerPublication)
			for groupIndex := range groups {
				groups[groupIndex].Mutations = []Mutation{{
					Key:   []byte(fmt.Sprintf("dgraph-submission-%d", groupIndex)),
					Value: []byte("posting-list-delta"),
				}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				firstTimestamp := uint64(i*commitsPerPublication + 1)
				for groupIndex := range groups {
					groups[groupIndex].Timestamp = firstTimestamp + uint64(groupIndex)
				}
				if grouped {
					if err := store.CommitGroupAt(groups, CommitRelaxed); err != nil {
						b.Fatal(err)
					}
					continue
				}
				for groupIndex := range groups {
					if err := store.CommitAt(groups[groupIndex].Timestamp, groups[groupIndex].Mutations, CommitRelaxed); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(countingDB.pointWrites+countingDB.batchWrites)/float64(b.N), "public_writes/publication")
			b.ReportMetric(float64(countingDB.batchWrites)/float64(b.N), "public_batch_writes/publication")
		})
	}
}

// benchmarkBatchCounterDB observes calls at TreeDB's public Batch surface,
// rather than relying on optional runtime statistics in a benchmark profile.
type benchmarkBatchCounterDB struct {
	*treedb.DB
	pointWrites uint64
	batchWrites uint64
}

func (db *benchmarkBatchCounterDB) NewBatchWithSize(size int) treedb.Batch {
	return &benchmarkBatchCounter{Batch: db.DB.NewBatchWithSize(size), db: db}
}

func (db *benchmarkBatchCounterDB) Set(key, value []byte) error {
	db.pointWrites++
	return db.DB.Set(key, value)
}

func (db *benchmarkBatchCounterDB) SetSync(key, value []byte) error {
	db.pointWrites++
	return db.DB.SetSync(key, value)
}

type benchmarkBatchCounter struct {
	treedb.Batch
	db *benchmarkBatchCounterDB
}

func (b *benchmarkBatchCounter) Write() error {
	b.db.batchWrites++
	return b.Batch.Write()
}

func (b *benchmarkBatchCounter) WriteSync() error {
	b.db.batchWrites++
	return b.Batch.WriteSync()
}

var _ treeDB = (*benchmarkBatchCounterDB)(nil)
var _ treedb.Batch = (*benchmarkBatchCounter)(nil)

func BenchmarkGetAt(b *testing.B) {
	for _, depth := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("DirectSeek/%d", depth), func(b *testing.B) {
			db, key := prepareGetAtBench(b, depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, err := directSeek(db, key, uint64(depth+1))
				if err != nil || len(value) == 0 {
					b.Fatalf("directSeek value=%q err=%v", value, err)
				}
			}
		})
		b.Run(fmt.Sprintf("MVCC/%d", depth), func(b *testing.B) {
			db, key := prepareGetAtBench(b, depth)
			store := New(db)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := store.GetAt(key, uint64(depth+1))
				if err != nil || result.State != Present || len(result.Value) == 0 {
					b.Fatalf("GetAt result=%+v err=%v", result, err)
				}
			}
		})
	}
}

// BenchmarkCommitAtGetAtInterleaved complements the preloaded read-only depth
// rows above. Each iteration writes a new version and immediately performs a
// point lookup, exposing snapshot rotations and source accumulation.
func BenchmarkCommitAtGetAtInterleaved(b *testing.B) {
	caching.SetIteratorDebug(true)
	b.Cleanup(func() { caching.SetIteratorDebug(false) })
	db := openBenchDB(b)
	store := New(db)
	key := []byte("benchmark-key")
	mutation := []Mutation{{Key: key, Value: []byte("value")}}
	if err := store.CommitAt(1, mutation, CommitRelaxed); err != nil {
		b.Fatalf("seed CommitAt: %v", err)
	}
	before := db.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := uint64(i + 2)
		if err := store.CommitAt(timestamp, mutation, CommitRelaxed); err != nil {
			b.Fatalf("CommitAt(%d): %v", timestamp, err)
		}
		result, err := store.GetAt(key, timestamp)
		if err != nil || result.State != Present {
			b.Fatalf("GetAt(%d) result=%+v err=%v", timestamp, result, err)
		}
	}
	b.StopTimer()
	after := db.Stats()
	reportMVCCBenchCounter(b, before, after, "treedb.cache.iterator.snapshot_rotations_total", "iterator_snapshot_rotations/op")
	reportMVCCBenchCounter(b, before, after, "treedb.cache.iterator.sources_total", "iterator_sources/op")
	reportMVCCBenchGauge(b, after, "treedb.cache.iterator.sources_max", "iterator_sources_max")
	reportMVCCBenchGauge(b, after, "treedb.cache.iterator.queue_len_max", "iterator_queue_len_max")
	reportMVCCBenchGauge(b, after, "treedb.cache.queue_len", "iterator_queue_len_end")
	reportMVCCBenchCounter(b, before, after, "treedb.cache.point_successor.calls_total", "point_successor_calls/op")
	reportMVCCBenchCounter(b, before, after, "treedb.cache.point_successor.sources_total", "point_successor_sources/op")
	reportMVCCBenchCounter(b, before, after, "treedb.cache.point_successor.mutable_probes_total", "point_successor_mutable_probes/op")
	reportMVCCBenchCounter(b, before, after, "treedb.cache.point_successor.backend_probes_total", "point_successor_backend_probes/op")
	reportMVCCBenchGauge(b, after, "treedb.cache.point_successor.sources_max", "point_successor_sources_max")
	reportMVCCBenchGauge(b, after, "treedb.cache.point_successor.general_merge_iterators_total", "point_successor_general_merge_iterators")
}

// BenchmarkCommitAtGetAtInterleavedHashSorted guards the exact-successor hash
// lookup used by write-then-read workloads. New physical MVCC versions
// invalidate HashSorted's ordered index; exact GetAt must not rebuild it.
func BenchmarkCommitAtGetAtInterleavedHashSorted(b *testing.B) {
	db := openBenchDBWithMemtableMode(b, "hash_sorted")
	store := New(db)
	key := []byte("benchmark-key")
	mutation := []Mutation{{Key: key, Value: []byte("value")}}
	if err := store.CommitAt(1, mutation, CommitRelaxed); err != nil {
		b.Fatalf("seed CommitAt: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := uint64(i + 2)
		if err := store.CommitAt(timestamp, mutation, CommitRelaxed); err != nil {
			b.Fatalf("CommitAt(%d): %v", timestamp, err)
		}
		result, err := store.GetAt(key, timestamp)
		if err != nil || result.State != Present {
			b.Fatalf("GetAt(%d) result=%+v err=%v", timestamp, result, err)
		}
	}
}

func reportMVCCBenchCounter(b *testing.B, before, after map[string]string, key, name string) {
	var start, end uint64
	if _, err := fmt.Sscanf(before[key], "%d", &start); err != nil {
		return
	}
	if _, err := fmt.Sscanf(after[key], "%d", &end); err != nil || end < start || b.N == 0 {
		return
	}
	b.ReportMetric(float64(end-start)/float64(b.N), name)
}

func reportMVCCBenchGauge(b *testing.B, stats map[string]string, key, name string) {
	var value uint64
	if _, err := fmt.Sscanf(stats[key], "%d", &value); err == nil {
		b.ReportMetric(float64(value), name)
	}
}

func prepareGetAtBench(b *testing.B, depth int) (*treedb.DB, []byte) {
	b.Helper()
	db := openBenchDB(b)
	store := New(db)
	key := []byte("benchmark-key")
	for timestamp := 1; timestamp <= depth; timestamp++ {
		if err := store.CommitAt(uint64(timestamp), []Mutation{{Key: key, Value: []byte("value")}}, CommitRelaxed); err != nil {
			b.Fatalf("prepare CommitAt: %v", err)
		}
	}
	return db, key
}

func directSeek(db *treedb.DB, logical []byte, timestamp uint64) ([]byte, error) {
	lower, err := mvcckey.Encode(logical, timestamp)
	if err != nil {
		return nil, err
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		return nil, err
	}
	it, err := db.Iterator(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if !it.Valid() {
		return nil, it.Error()
	}
	return it.ValueCopy(nil), nil
}

func BenchmarkVersionIteration(b *testing.B) {
	for _, keys := range []int{64, 256} {
		for _, depth := range []int{1, 8, 32} {
			for _, reverse := range []bool{false, true} {
				name := fmt.Sprintf("keys=%d/depth=%d/reverse=%t", keys, depth, reverse)
				b.Run("Physical/"+name, func(b *testing.B) {
					db, _ := prepareVersionIterationBench(b, keys, depth)
					lower := mvcckey.AppendNamespaceLower(nil)
					upper := mvcckey.AppendNamespaceUpper(nil)
					versions := uint64(keys * depth)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var it treedb.Iterator
						var err error
						if reverse {
							it, err = db.ReverseIterator(lower, upper)
						} else {
							it, err = db.Iterator(lower, upper)
						}
						if err != nil {
							b.Fatal(err)
						}
						var seen uint64
						for it.Valid() {
							logical, _, err := mvcckey.Decode(it.Key())
							if err != nil {
								b.Fatal(err)
							}
							value := it.ValueCopy(nil)
							if len(logical) == 0 || len(value) == 0 {
								b.Fatal("empty benchmark record")
							}
							seen++
							it.Next()
						}
						iterErr := it.Error()
						if err := it.Close(); err != nil || iterErr != nil || seen != versions {
							b.Fatalf("seen=%d iterErr=%v closeErr=%v", seen, iterErr, err)
						}
					}
					b.ReportMetric(float64(versions*uint64(b.N))/b.Elapsed().Seconds(), "versions/s")
				})
				b.Run("MVCC/"+name, func(b *testing.B) {
					_, store := prepareVersionIterationBench(b, keys, depth)
					versions := uint64(keys * depth)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						it, err := store.IterateVersions(VersionIteratorOptions{Reverse: reverse})
						if err != nil {
							b.Fatal(err)
						}
						var seen uint64
						for it.Valid() {
							entry := it.Entry()
							if len(entry.Key) == 0 {
								b.Fatal("empty benchmark key")
							}
							seen++
							it.Next()
						}
						stats := it.Stats()
						iterErr := it.Error()
						if err := it.Close(); err != nil || iterErr != nil || seen != versions || stats.Skipped != 0 {
							b.Fatalf("seen=%d stats=%+v iterErr=%v closeErr=%v", seen, stats, iterErr, err)
						}
					}
					b.ReportMetric(float64(versions*uint64(b.N))/b.Elapsed().Seconds(), "versions/s")
				})
			}
		}
	}
}

func BenchmarkVersionIterationFiltered(b *testing.B) {
	const depth, readTimestamp = 16, 8
	cases := []struct {
		name    string
		keys    int
		prefix  []byte
		matches int
	}{
		{name: "all_of_128", keys: 128, matches: 128},
		{name: "all_of_512", keys: 512, matches: 512},
		{name: "100_of_512", keys: 512, prefix: []byte("key-0000"), matches: 100},
		{name: "10_of_512", keys: 512, prefix: []byte("key-00000"), matches: 10},
		{name: "1_of_512", keys: 512, prefix: []byte("key-000000"), matches: 1},
	}
	for _, benchmark := range cases {
		b.Run(fmt.Sprintf("keys=%d/selectivity=%s", benchmark.keys, benchmark.name), func(b *testing.B) {
			_, store := prepareVersionIterationBench(b, benchmark.keys, depth)
			useful := uint64(benchmark.matches * readTimestamp)
			visited := uint64(benchmark.matches * depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it, err := store.IterateVersions(VersionIteratorOptions{
					Prefix:        benchmark.prefix,
					ReadTimestamp: readTimestamp,
				})
				if err != nil {
					b.Fatal(err)
				}
				for it.Valid() {
					it.Next()
				}
				stats := it.Stats()
				if err := it.Close(); err != nil || stats.Retained != useful || stats.Skipped != visited-useful || stats.Visited != visited {
					b.Fatalf("stats=%+v err=%v", stats, err)
				}
			}
			b.ReportMetric(float64(useful*uint64(b.N))/b.Elapsed().Seconds(), "useful_versions/s")
			b.ReportMetric(float64((visited-useful)*uint64(b.N))/b.Elapsed().Seconds(), "skipped_versions/s")
		})
	}
}

func BenchmarkPruneVersions(b *testing.B) {
	const depth = 16
	cases := []struct {
		keys  int
		floor int
	}{
		{keys: 64, floor: 4},
		{keys: 64, floor: 12},
		{keys: 256, floor: 4},
		{keys: 256, floor: 8},
		{keys: 256, floor: 12},
	}
	for _, benchmark := range cases {
		discardedPerKey := benchmark.floor - 1
		name := fmt.Sprintf("keys=%d/depth=%d/discard=%d_of_%d", benchmark.keys, depth, discardedPerKey, depth)
		b.Run(name, func(b *testing.B) {
			var total PruneStats
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db, store := prepareVersionIterationBench(b, benchmark.keys, depth)
				if err := store.AdvanceDiscardFloor(uint64(benchmark.floor), CommitRelaxed); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				stats, err := store.PruneVersions(PruneOptions{BatchSize: 256, Mode: CommitRelaxed})
				if err != nil {
					b.Fatal(err)
				}
				wantPruned := uint64(benchmark.keys * discardedPerKey)
				wantRetained := uint64(benchmark.keys * (depth - benchmark.floor + 1))
				if stats.Pruned != wantPruned || stats.Retained != wantRetained {
					b.Fatalf("stats=%+v want pruned=%d retained=%d", stats, wantPruned, wantRetained)
				}
				total.Visited += stats.Visited
				total.Skipped += stats.Skipped
				total.Retained += stats.Retained
				total.Pruned += stats.Pruned
				total.RetainedBytes += stats.RetainedBytes
				total.PrunedBytes += stats.PrunedBytes
				total.DeleteWriteBytes += stats.DeleteWriteBytes
				b.StopTimer()
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(total.Pruned)/b.Elapsed().Seconds(), "pruned_versions/s")
			b.ReportMetric(float64(total.Retained)/float64(max(b.N, 1)), "retained_versions/op")
			b.ReportMetric(float64(total.RetainedBytes)/float64(max(b.N, 1)), "retained_physical_bytes/op")
			b.ReportMetric(float64(total.PrunedBytes)/float64(max(total.Pruned, 1)), "physical_bytes/pruned_version")
			b.ReportMetric(float64(total.DeleteWriteBytes)/float64(max(total.PrunedBytes, 1)), "delete_write_amplification")
		})
	}
}

func prepareVersionIterationBench(b *testing.B, keys, depth int) (*treedb.DB, *Store) {
	b.Helper()
	db := openBenchDB(b)
	store := New(db)
	mutations := make([]Mutation, keys)
	for key := range mutations {
		mutations[key] = Mutation{Key: []byte(fmt.Sprintf("key-%06d", key)), Value: []byte("value")}
	}
	for timestamp := 1; timestamp <= depth; timestamp++ {
		if err := store.CommitAt(uint64(timestamp), mutations, CommitRelaxed); err != nil {
			b.Fatalf("prepare CommitAt(%d): %v", timestamp, err)
		}
	}
	return db, store
}
