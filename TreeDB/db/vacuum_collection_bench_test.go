package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func BenchmarkVacuumIndexOnlineCollection(b *testing.B) {
	for _, tc := range []struct {
		name      string
		valueSize int
	}{
		{name: "bytes_1x", valueSize: 16},
		{name: "bytes_64x", valueSize: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db := openVacuumCollectionBenchmarkDB(b, tc.valueSize)
			defer func() { _ = db.Close() }()
			var total VacuumOnlineStats

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.VacuumIndexOnline(context.Background()); err != nil {
					b.Fatalf("vacuum: %v", err)
				}
				stats := db.vacuumOnlineStatsSnapshot()
				total.TotalDuration += stats.TotalDuration
				total.CutoverDuration += stats.CutoverDuration
				total.SystemTreeDuration += stats.SystemTreeDuration
				if stats.MaxWriterPause > total.MaxWriterPause {
					total.MaxWriterPause = stats.MaxWriterPause
				}
				total.PrecloneTraversalPages += stats.PrecloneTraversalPages
				total.RecloneTraversalPages += stats.RecloneTraversalPages
				total.CutoverCloneTraversalPages += stats.CutoverCloneTraversalPages
				total.DirtyDescriptors += stats.DirtyDescriptors
				total.UserTailMutations += stats.UserTailMutations
				total.DeferredCutovers += stats.DeferredCutovers
				total.ConcurrentMutationAborts += stats.ConcurrentMutationAborts
			}
			b.StopTimer()

			b.ReportMetric(float64(total.TotalDuration.Nanoseconds())/float64(b.N), "vacuum-total-ns/op")
			b.ReportMetric(float64(total.CutoverDuration.Nanoseconds())/float64(b.N), "cutover-ns/op")
			b.ReportMetric(float64(total.SystemTreeDuration.Nanoseconds())/float64(b.N), "system-tree-ns/op")
			b.ReportMetric(float64(total.MaxWriterPause.Nanoseconds()), "max-writer-pause-ns")
			b.ReportMetric(float64(total.PrecloneTraversalPages)/float64(b.N), "preclone-pages/op")
			b.ReportMetric(float64(total.RecloneTraversalPages)/float64(b.N), "reclone-pages/op")
			b.ReportMetric(float64(total.CutoverCloneTraversalPages)/float64(b.N), "cutover-clone-pages/op")
			b.ReportMetric(float64(total.DirtyDescriptors)/float64(b.N), "dirty-descriptors/op")
			b.ReportMetric(float64(total.UserTailMutations)/float64(b.N), "tail-mutations/op")
			b.ReportMetric(float64(total.DeferredCutovers)/float64(b.N), "deferred-cutovers/op")
			b.ReportMetric(float64(total.ConcurrentMutationAborts)/float64(b.N), "concurrent-aborts/op")
		})
	}
}

func openVacuumCollectionBenchmarkDB(b *testing.B, valueSize int) *DB {
	b.Helper()
	db, err := Open(Options{
		Dir:       b.TempDir(),
		ChunkSize: 1 << 20,
		ValueLog: ValueLogOptions{
			PointerThreshold: 4096,
		},
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	collection, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		_ = db.Close()
		b.Fatalf("new collection memtable: %v", err)
	}
	value := make([]byte, valueSize)
	for i := 0; i < 1024; i++ {
		value[0] = byte(i)
		collection.Set([]byte(fmt.Sprintf("doc/%06d", i)), value)
	}
	collection.Freeze()
	_, roots, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          collection.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		catalog, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			return nil, err
		}
		encoded := encodeCollectionRootDescriptorRootID(rootIDs[0])
		catalog.Set([]byte(vacuumSnapshotPrimaryKey), encoded)
		catalog.Set([]byte(vacuumSnapshotAliasKey), encoded)
		catalog.Set([]byte(vacuumSnapshotOverlayKey), encodeCollectionRootDescriptorRootIDs([]uint64{rootIDs[0], rootIDs[0]}))
		catalog.Set([]byte(vacuumSnapshotEmptyKey), nil)
		catalog.Freeze()
		return catalog.NewIterator(nil, nil), nil
	})
	if err != nil || len(roots) != 1 {
		_ = db.Close()
		b.Fatalf("publish collection roots=%v err=%v", roots, err)
	}
	return db
}
