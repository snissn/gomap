package db

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

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
			disableRootPublicationForLegacyVacuumBenchmark(b, db)
			var total VacuumOnlineStats

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.vacuumIndexOnlineLegacyForTest(context.Background()); err != nil {
					b.Fatalf("vacuum: %v", err)
				}
				stats := db.vacuumOnlineStatsSnapshot()
				total.TotalDuration += stats.TotalDuration
				total.UserTreeDuration += stats.UserTreeDuration
				total.SystemReserveDuration += stats.SystemReserveDuration
				total.CollectionBasisDuration += stats.CollectionBasisDuration
				total.PreflushDuration += stats.PreflushDuration
				total.CutoverDuration += stats.CutoverDuration
				total.SystemTreeDuration += stats.SystemTreeDuration
				total.FinalPagerSyncDuration += stats.FinalPagerSyncDuration
				total.SwapPublishDuration += stats.SwapPublishDuration
				if stats.MaxWriterPause > total.MaxWriterPause {
					total.MaxWriterPause = stats.MaxWriterPause
				}
				total.PrecloneTraversalPages += stats.PrecloneTraversalPages
				total.RecloneTraversalPages += stats.RecloneTraversalPages
				total.CutoverCloneTraversalPages += stats.CutoverCloneTraversalPages
				total.DirtyDescriptors += stats.DirtyDescriptors
				total.UserTailMutations += stats.UserTailMutations
				total.UserTailPointMutations += stats.UserTailPointMutations
				total.UserTailRangeMutations += stats.UserTailRangeMutations
				total.DeferredCutovers += stats.DeferredCutovers
				total.ConcurrentMutationAborts += stats.ConcurrentMutationAborts
			}
			b.StopTimer()

			b.ReportMetric(float64(total.TotalDuration.Nanoseconds())/float64(b.N), "vacuum-total-ns/op")
			b.ReportMetric(float64(total.UserTreeDuration.Nanoseconds())/float64(b.N), "user-tree-ns/op")
			b.ReportMetric(float64(total.SystemReserveDuration.Nanoseconds())/float64(b.N), "system-reserve-ns/op")
			b.ReportMetric(float64(total.CollectionBasisDuration.Nanoseconds())/float64(b.N), "collection-basis-ns/op")
			b.ReportMetric(float64(total.PreflushDuration.Nanoseconds())/float64(b.N), "preflush-ns/op")
			b.ReportMetric(float64(total.CutoverDuration.Nanoseconds())/float64(b.N), "cutover-ns/op")
			b.ReportMetric(float64(total.SystemTreeDuration.Nanoseconds())/float64(b.N), "system-tree-ns/op")
			b.ReportMetric(float64(total.FinalPagerSyncDuration.Nanoseconds())/float64(b.N), "final-pager-sync-ns/op")
			b.ReportMetric(float64(total.SwapPublishDuration.Nanoseconds())/float64(b.N), "swap-publish-ns/op")
			b.ReportMetric(float64(total.MaxWriterPause.Nanoseconds()), "max-writer-pause-ns")
			b.ReportMetric(float64(total.PrecloneTraversalPages)/float64(b.N), "preclone-pages/op")
			b.ReportMetric(float64(total.RecloneTraversalPages)/float64(b.N), "reclone-pages/op")
			b.ReportMetric(float64(total.CutoverCloneTraversalPages)/float64(b.N), "cutover-clone-pages/op")
			b.ReportMetric(float64(total.DirtyDescriptors)/float64(b.N), "dirty-descriptors/op")
			b.ReportMetric(float64(total.UserTailMutations)/float64(b.N), "tail-mutations/op")
			b.ReportMetric(float64(total.UserTailPointMutations)/float64(b.N), "tail-points/op")
			b.ReportMetric(float64(total.UserTailRangeMutations)/float64(b.N), "tail-ranges/op")
			b.ReportMetric(float64(total.DeferredCutovers)/float64(b.N), "deferred-cutovers/op")
			b.ReportMetric(float64(total.ConcurrentMutationAborts)/float64(b.N), "concurrent-aborts/op")
		})
	}
}

func BenchmarkVacuumIndexOnlineCollectionForegroundChurn(b *testing.B) {
	benchmarkVacuumIndexOnlineCollectionForegroundChurn(b, true)
}

func BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn(b *testing.B) {
	benchmarkVacuumIndexOnlineCollectionForegroundChurn(b, false)
}

func benchmarkVacuumIndexOnlineCollectionForegroundChurn(b *testing.B, legacy bool) {
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
			if legacy {
				disableRootPublicationForLegacyVacuumBenchmark(b, db)
			}
			var total VacuumOnlineStats
			var foregroundLatencies []time.Duration
			var foregroundPoints, foregroundRanges uint64
			var vacuumErrors uint64

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				start := make(chan struct{})
				stop := make(chan struct{})
				warmed := make(chan struct{})
				writerDone := make(chan vacuumCollectionForegroundResult, 1)
				var startOnce sync.Once
				go vacuumCollectionForegroundWriter(db, start, stop, warmed, writerDone)

				var hookOnce sync.Once
				db.vacuumPagerSyncHook = func(phase vacuumPagerSyncPhase) {
					if phase != vacuumPagerSyncPrecutover {
						return
					}
					hookOnce.Do(func() {
						startOnce.Do(func() { close(start) })
						<-warmed
					})
				}
				var vacuumErr error
				if legacy {
					vacuumErr = db.vacuumIndexOnlineLegacyForTest(context.Background())
				} else {
					vacuumErr = db.VacuumIndexOnline(context.Background())
				}
				db.vacuumPagerSyncHook = nil
				startOnce.Do(func() { close(start) })
				close(stop)
				foreground := <-writerDone
				if foreground.err != nil {
					b.Fatalf("foreground churn: %v", foreground.err)
				}
				if vacuumErr != nil {
					vacuumErrors++
					if !legacy || !errors.Is(vacuumErr, ErrVacuumConcurrentMutation) {
						b.Fatalf("vacuum: %v", vacuumErr)
					}
				}
				foregroundLatencies = append(foregroundLatencies, foreground.latencies...)
				foregroundPoints += foreground.points
				foregroundRanges += foreground.ranges

				stats := db.vacuumOnlineStatsSnapshot()
				total.TotalDuration += stats.TotalDuration
				total.UserTreeDuration += stats.UserTreeDuration
				total.SystemReserveDuration += stats.SystemReserveDuration
				total.CollectionBasisDuration += stats.CollectionBasisDuration
				total.PreflushDuration += stats.PreflushDuration
				total.CutoverDuration += stats.CutoverDuration
				total.SystemTreeDuration += stats.SystemTreeDuration
				total.FinalPagerSyncDuration += stats.FinalPagerSyncDuration
				total.SwapPublishDuration += stats.SwapPublishDuration
				if stats.MaxWriterPause > total.MaxWriterPause {
					total.MaxWriterPause = stats.MaxWriterPause
				}
				total.PrecloneTraversalPages += stats.PrecloneTraversalPages
				total.RecloneTraversalPages += stats.RecloneTraversalPages
				total.CutoverCloneTraversalPages += stats.CutoverCloneTraversalPages
				total.DirtyDescriptors += stats.DirtyDescriptors
				total.UserTailMutations += stats.UserTailMutations
				total.UserTailPointMutations += stats.UserTailPointMutations
				total.UserTailRangeMutations += stats.UserTailRangeMutations
				total.DeferredCutovers += stats.DeferredCutovers
				total.ConcurrentMutationAborts += stats.ConcurrentMutationAborts
			}
			b.StopTimer()
			exposureMisses := verifyVacuumCollectionForegroundPoints(b, db)

			b.ReportMetric(float64(total.TotalDuration.Nanoseconds())/float64(b.N), "vacuum-total-ns/op")
			b.ReportMetric(float64(total.UserTreeDuration.Nanoseconds())/float64(b.N), "user-tree-ns/op")
			b.ReportMetric(float64(total.SystemReserveDuration.Nanoseconds())/float64(b.N), "system-reserve-ns/op")
			b.ReportMetric(float64(total.CollectionBasisDuration.Nanoseconds())/float64(b.N), "collection-basis-ns/op")
			b.ReportMetric(float64(total.PreflushDuration.Nanoseconds())/float64(b.N), "preflush-ns/op")
			b.ReportMetric(float64(total.CutoverDuration.Nanoseconds())/float64(b.N), "cutover-ns/op")
			b.ReportMetric(float64(total.SystemTreeDuration.Nanoseconds())/float64(b.N), "system-tree-ns/op")
			b.ReportMetric(float64(total.FinalPagerSyncDuration.Nanoseconds())/float64(b.N), "final-pager-sync-ns/op")
			b.ReportMetric(float64(total.SwapPublishDuration.Nanoseconds())/float64(b.N), "swap-publish-ns/op")
			b.ReportMetric(float64(total.MaxWriterPause.Nanoseconds()), "max-writer-pause-ns")
			b.ReportMetric(float64(vacuumCollectionLatencyPercentile(foregroundLatencies, 95).Nanoseconds()), "foreground-p95-ns/op")
			b.ReportMetric(float64(vacuumCollectionLatencyPercentile(foregroundLatencies, 99).Nanoseconds()), "foreground-p99-ns/op")
			b.ReportMetric(float64(foregroundPoints)/float64(b.N), "foreground-points/op")
			b.ReportMetric(float64(foregroundRanges)/float64(b.N), "foreground-ranges/op")
			b.ReportMetric(float64(len(foregroundLatencies))/float64(b.N), "foreground-overlap-samples/op")
			b.ReportMetric(float64(exposureMisses)/float64(b.N), "foreground-exposure-misses/op")
			b.ReportMetric(float64(vacuumErrors)/float64(b.N), "vacuum-errors/op")
			b.ReportMetric(float64(total.PrecloneTraversalPages)/float64(b.N), "preclone-pages/op")
			b.ReportMetric(float64(total.RecloneTraversalPages)/float64(b.N), "reclone-pages/op")
			b.ReportMetric(float64(total.CutoverCloneTraversalPages)/float64(b.N), "cutover-clone-pages/op")
			b.ReportMetric(float64(total.DirtyDescriptors)/float64(b.N), "dirty-descriptors/op")
			b.ReportMetric(float64(total.UserTailMutations)/float64(b.N), "tail-mutations/op")
			b.ReportMetric(float64(total.UserTailPointMutations)/float64(b.N), "tail-points/op")
			b.ReportMetric(float64(total.UserTailRangeMutations)/float64(b.N), "tail-ranges/op")
			b.ReportMetric(float64(total.DeferredCutovers)/float64(b.N), "deferred-cutovers/op")
			b.ReportMetric(float64(total.ConcurrentMutationAborts)/float64(b.N), "concurrent-aborts/op")
		})
	}
}

// disableRootPublicationForLegacyVacuumBenchmark recreates the runtime mode
// that the legacy test-only swap was designed for. Production comparison must
// keep the coordinator enabled and prove coherent post-swap rebinding.
func disableRootPublicationForLegacyVacuumBenchmark(tb testing.TB, db *DB) {
	tb.Helper()
	runtime := db.rootPublication
	if runtime == nil || runtime.coordinator == nil {
		tb.Fatal("legacy vacuum baseline requires an active root-publication runtime to quiesce")
	}
	if err := runtime.coordinator.Drain(context.Background()); err != nil {
		tb.Fatalf("drain root publication for legacy baseline: %v", err)
	}
	if err := runtime.coordinator.Stop(context.Background()); err != nil {
		tb.Fatalf("stop root publication for legacy baseline: %v", err)
	}
	handoff, err := runtime.coordinator.TakeRecoveryHandoff()
	if err != nil {
		tb.Fatalf("take root-publication handoff for legacy baseline: %v", err)
	}
	db.rootPublication = nil
	runtime.release()
	handoff.Release()
}

type vacuumCollectionForegroundResult struct {
	latencies []time.Duration
	points    uint64
	ranges    uint64
	err       error
}

func vacuumCollectionForegroundWriter(db *DB, start, stop <-chan struct{}, warmed chan<- struct{}, done chan<- vacuumCollectionForegroundResult) {
	const warmOperations = 4096
	<-start
	result := vacuumCollectionForegroundResult{latencies: make([]time.Duration, 0, warmOperations*2)}
	var warmOnce sync.Once
	finish := func() {
		warmOnce.Do(func() { close(warmed) })
		done <- result
	}
	defer finish()
	for operation := 0; ; operation++ {
		select {
		case <-stop:
			return
		default:
		}
		started := time.Now()
		if operation%2 == 0 {
			key := []byte(fmt.Sprintf("foreground/point/%06d", operation%256))
			value := []byte(fmt.Sprintf("value/%06d", operation))
			result.err = db.Set(key, value)
			result.points++
		} else {
			batch := db.NewBatch()
			rangeID := operation % 64
			startKey := []byte(fmt.Sprintf("foreground/range/%03d/a", rangeID))
			endKey := []byte(fmt.Sprintf("foreground/range/%03d/z", rangeID))
			result.err = batch.DeleteRange(startKey, endKey)
			if result.err == nil {
				result.err = batch.Write()
			}
			if closeErr := batch.Close(); result.err == nil {
				result.err = closeErr
			}
			result.ranges++
		}
		result.latencies = append(result.latencies, time.Since(started))
		if len(result.latencies) >= warmOperations {
			warmOnce.Do(func() { close(warmed) })
			return
		}
		if result.err != nil {
			return
		}
	}
}

func verifyVacuumCollectionForegroundPoints(tb testing.TB, db *DB) uint64 {
	tb.Helper()
	var misses uint64
	for point := 0; point < 256; point += 2 {
		lastOperation := point
		for lastOperation+256 < 4096 {
			lastOperation += 256
		}
		key := []byte(fmt.Sprintf("foreground/point/%06d", point))
		want := fmt.Sprintf("value/%06d", lastOperation)
		got, err := db.Get(key)
		if err != nil || string(got) != want {
			misses++
		}
	}
	return misses
}

func vacuumCollectionLatencyPercentile(latencies []time.Duration, percentile int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(sorted) {
		index = len(sorted)
	}
	return sorted[index-1]
}

const vacuumCollectionBenchmarkCatalogEntries = 131072

func vacuumCollectionBenchmarkCatalog(rootIDs []uint64) (iterator.UnsafeIterator, error) {
	return vacuumCollectionCatalog(rootIDs, vacuumCollectionBenchmarkCatalogEntries)
}

func vacuumCollectionFixtureCatalog(rootIDs []uint64) (iterator.UnsafeIterator, error) {
	return vacuumCollectionCatalog(rootIDs, 0)
}

func vacuumCollectionCatalog(rootIDs []uint64, metadataEntries int) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		return nil, fmt.Errorf("unexpected collection roots %v", rootIDs)
	}
	catalog, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		return nil, err
	}
	encoded := encodeCollectionRootDescriptorRootID(rootIDs[0])
	catalog.Set([]byte(vacuumSnapshotPrimaryKey), encoded)
	catalog.Set([]byte(vacuumSnapshotAliasKey), encoded)
	for entry := 0; entry < metadataEntries; entry++ {
		catalog.Set([]byte(fmt.Sprintf("vacuum-benchmark/catalog/%06d", entry)), encoded)
	}
	catalog.Set([]byte(vacuumSnapshotOverlayKey), encodeCollectionRootDescriptorRootIDs([]uint64{rootIDs[0], rootIDs[0]}))
	catalog.Set([]byte(vacuumSnapshotEmptyKey), nil)
	catalog.Freeze()
	return catalog.NewIterator(nil, nil), nil
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
	}}, vacuumCollectionBenchmarkCatalog)
	if err != nil || len(roots) != 1 {
		_ = db.Close()
		b.Fatalf("publish collection roots=%v err=%v", roots, err)
	}
	return db
}
