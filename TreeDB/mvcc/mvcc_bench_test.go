package mvcc

import (
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func openBenchDB(b *testing.B) *treedb.DB {
	b.Helper()
	db := openTestDB(b, b.TempDir(), treedb.DurabilityWALOffRelaxed)
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
