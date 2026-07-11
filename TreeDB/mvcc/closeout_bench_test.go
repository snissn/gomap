package mvcc

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

type closeoutProfile struct {
	name       string
	durability treedb.DurabilityMode
	mode       CommitMode
}

var closeoutProfiles = []closeoutProfile{
	{name: "durable_sync", durability: treedb.DurabilityDurable, mode: CommitDurable},
	{name: "wal_on_relaxed", durability: treedb.DurabilityWALOnRelaxed, mode: CommitRelaxed},
	{name: "wal_off_relaxed", durability: treedb.DurabilityWALOffRelaxed, mode: CommitRelaxed},
}

// BenchmarkDgraphMVCCCloseout is the pinned downstream-readiness matrix. Its
// durability classes are intentionally separate sub-benchmarks: the relaxed
// rows are not presented as equivalent to durable acknowledgement.
func BenchmarkDgraphMVCCCloseout(b *testing.B) {
	for _, profile := range closeoutProfiles {
		profile := profile
		for _, batchSize := range []int{1, 32} {
			batchSize := batchSize
			b.Run(fmt.Sprintf("CommitAt/%s/batch=%d", profile.name, batchSize), func(b *testing.B) {
				benchmarkCloseoutCommitAt(b, profile, batchSize)
			})
		}
		for _, depth := range []int{1, 64} {
			depth := depth
			b.Run(fmt.Sprintf("GetAt/%s/depth=%d", profile.name, depth), func(b *testing.B) {
				benchmarkCloseoutGetAt(b, profile, depth)
			})
		}
		for _, depth := range []int{1, 32} {
			depth := depth
			b.Run(fmt.Sprintf("AllVersions/%s/keys=64/depth=%d", profile.name, depth), func(b *testing.B) {
				benchmarkCloseoutAllVersions(b, profile, 64, depth)
			})
		}
		for _, floor := range []int{4, 12} {
			floor := floor
			b.Run(fmt.Sprintf("Prune/%s/keys=64/depth=16/floor=%d", profile.name, floor), func(b *testing.B) {
				benchmarkCloseoutPrune(b, profile, 64, 16, floor)
			})
		}
	}
}

func benchmarkCloseoutCommitAt(b *testing.B, profile closeoutProfile, batchSize int) {
	db, store, dir := openCloseoutBench(b, profile)
	mutations := make([]Mutation, batchSize)
	for i := range mutations {
		mutations[i] = Mutation{Key: []byte(fmt.Sprintf("key-%03d", i)), Value: []byte("value")}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.CommitAt(uint64(i+1), mutations, profile.mode); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	storageBytes := closeoutDirectoryBytes(b, dir)
	b.ReportMetric(float64(batchSize*b.N)/b.Elapsed().Seconds(), "mutations/s")
	b.ReportMetric(float64(storageBytes)/float64(max(b.N, 1)), "storage_bytes/op")
	if profile.mode == CommitDurable {
		b.ReportMetric(float64(storageBytes)/float64(max(b.N, 1)), "durable_footprint_bytes/op")
	}
}

func benchmarkCloseoutGetAt(b *testing.B, profile closeoutProfile, depth int) {
	db, store, _ := openCloseoutBench(b, profile)
	key := []byte("benchmark-key")
	for timestamp := 1; timestamp <= depth; timestamp++ {
		if err := store.CommitAt(uint64(timestamp), []Mutation{{Key: key, Value: []byte("value")}}, profile.mode); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := store.GetAt(key, uint64(depth+1))
		if err != nil || result.State != Present || len(result.Value) == 0 {
			b.Fatalf("GetAt result=%+v err=%v", result, err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lookups/s")
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkCloseoutAllVersions(b *testing.B, profile closeoutProfile, keys, depth int) {
	db, store, _ := openCloseoutBench(b, profile)
	closeoutPrepareHistory(b, store, keys, depth, profile.mode)
	versions := uint64(keys * depth)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := store.IterateVersions(VersionIteratorOptions{})
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
		iterErr := it.Error()
		closeErr := it.Close()
		if iterErr != nil || closeErr != nil || seen != versions {
			b.Fatalf("seen=%d iterErr=%v closeErr=%v", seen, iterErr, closeErr)
		}
	}
	b.ReportMetric(float64(versions*uint64(b.N))/b.Elapsed().Seconds(), "versions/s")
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkCloseoutPrune(b *testing.B, profile closeoutProfile, keys, depth, floor int) {
	b.StopTimer()
	var total PruneStats
	var storageBytes uint64
	parentDir := b.TempDir()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, err := os.MkdirTemp(parentDir, "prune-")
		if err != nil {
			b.Fatal(err)
		}
		db := openTestDB(b, dir, profile.durability)
		store := New(db)
		closeoutPrepareHistory(b, store, keys, depth, profile.mode)
		if err := store.AdvanceDiscardFloor(uint64(floor), profile.mode); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, err := store.PruneVersions(PruneOptions{BatchSize: 256, Mode: profile.mode})
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
		storageBytes += closeoutDirectoryBytes(b, dir)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		total.Visited += stats.Visited
		total.Skipped += stats.Skipped
		total.Retained += stats.Retained
		total.Pruned += stats.Pruned
		total.PrunedBytes += stats.PrunedBytes
		total.DeleteWriteBytes += stats.DeleteWriteBytes
		b.StartTimer()
	}
	b.StopTimer()
	wantPruned := uint64(keys * (floor - 1) * b.N)
	if total.Pruned != wantPruned || total.Visited != total.Retained+total.Pruned {
		b.Fatalf("stats=%+v want pruned=%d", total, wantPruned)
	}
	b.ReportMetric(float64(total.Pruned)/b.Elapsed().Seconds(), "pruned_versions/s")
	b.ReportMetric(float64(total.DeleteWriteBytes)/float64(max(total.PrunedBytes, 1)), "delete_write_amplification")
	b.ReportMetric(float64(storageBytes)/float64(max(b.N, 1)), "storage_bytes/op")
	if profile.mode == CommitDurable {
		b.ReportMetric(float64(storageBytes)/float64(max(b.N, 1)), "durable_footprint_bytes/op")
	}
}

func openCloseoutBench(b *testing.B, profile closeoutProfile) (*treedb.DB, *Store, string) {
	b.Helper()
	dir := b.TempDir()
	db := openTestDB(b, dir, profile.durability)
	return db, New(db), dir
}

func closeoutPrepareHistory(b *testing.B, store *Store, keys, depth int, mode CommitMode) {
	b.Helper()
	mutations := make([]Mutation, keys)
	for key := range mutations {
		mutations[key] = Mutation{Key: []byte(fmt.Sprintf("key-%06d", key)), Value: []byte("value")}
	}
	for timestamp := 1; timestamp <= depth; timestamp++ {
		if err := store.CommitAt(uint64(timestamp), mutations, mode); err != nil {
			b.Fatalf("prepare CommitAt(%d): %v", timestamp, err)
		}
	}
}

func closeoutDirectoryBytes(b *testing.B, root string) uint64 {
	b.Helper()
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += uint64(info.Size())
		return nil
	})
	if err != nil {
		b.Fatalf("measure %s: %v", root, err)
	}
	return total
}
