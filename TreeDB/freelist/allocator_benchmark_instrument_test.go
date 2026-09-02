//go:build treedb_freelist_instrument

package freelist

import "testing"

type allocatorObservedMetrics struct {
	gets         int
	verifies     int
	updates      int
	pagesTouched int
}

func observedMetrics(a *Allocator) allocatorObservedMetrics {
	var out allocatorObservedMetrics
	for _, counts := range a.allPageOperationCounts() {
		out.pagesTouched++
		out.gets += counts.gets
		out.verifies += counts.verifies
		out.updates += counts.updates
	}
	return out
}

func (metrics *allocatorObservedMetrics) add(other allocatorObservedMetrics) {
	metrics.gets += other.gets
	metrics.verifies += other.verifies
	metrics.updates += other.updates
	metrics.pagesTouched += other.pagesTouched
}

func (metrics allocatorObservedMetrics) report(b *testing.B) {
	b.ReportMetric(float64(metrics.pagesTouched)/float64(b.N), "pages_touched/op")
	b.ReportMetric(float64(metrics.gets)/float64(b.N), "get_for_write/op")
	b.ReportMetric(float64(metrics.verifies)/float64(b.N), "checksum_verifications/op")
	b.ReportMetric(float64(metrics.updates)/float64(b.N), "checksum_updates/op")
}

func BenchmarkAllocator_FreeManyObservedOperations(b *testing.B) {
	ids := allocatorBenchmarkIDsSlice()
	root := b.TempDir()
	var metrics allocatorObservedMetrics
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, p, path := newAllocatorBenchmarkFixture(b, root, i)
		a.resetPageOperationCounts()
		b.StartTimer()
		freeAllocatorBenchmarkIDs(b, a, ids, true)
		b.StopTimer()
		metrics.add(observedMetrics(a))
		closeAllocatorBenchmarkFixture(b, p, path)
	}
	metrics.report(b)
}

func BenchmarkAllocator_AllocManyObservedOperations(b *testing.B) {
	benchmarkAllocatorObservedOperations(b, allocatorBenchmarkIDs)
}

func BenchmarkAllocator_AllocMany2ObservedOperations(b *testing.B) {
	benchmarkAllocatorObservedOperations(b, 2)
}

func benchmarkAllocatorObservedOperations(b *testing.B, count int) {
	ids := allocatorBenchmarkIDsSlice()
	root := b.TempDir()
	var metrics allocatorObservedMetrics
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, p, path := newAllocatorBenchmarkFixture(b, root, i)
		populateAllocatorBenchmarkFreelist(b, a, ids)
		a.SetFreelistRegion(allocatorBenchmarkRegion, 1)
		a.resetPageOperationCounts()
		b.StartTimer()
		allocAllocatorBenchmarkIDs(b, a, count, true)
		b.StopTimer()
		metrics.add(observedMetrics(a))
		closeAllocatorBenchmarkFixture(b, p, path)
	}
	metrics.report(b)
}
