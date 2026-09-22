package collections

import "testing"

func BenchmarkColumnVectorGraphFrontierHeapOperations2271(b *testing.B) {
	const heapSize = 128
	var scratch columnVectorGraphNativeSearchScratch
	scratch.frontier = make([]columnVectorGraphSearchCandidate, 0, heapSize)
	var checksum int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scratch.frontier = scratch.frontier[:0]
		seed := uint32(i + 1)
		for j := 0; j < heapSize; j++ {
			seed = seed*1664525 + 1013904223
			scratch.pushFrontier(columnVectorGraphSearchCandidate{ordinal: j, score: float64(seed & 0xffff)})
		}
		for {
			candidate, ok := scratch.popFrontier()
			if !ok {
				break
			}
			checksum += int64(candidate.ordinal)
		}
	}
	b.StopTimer()
	columnPhysicalScanBenchSum += checksum
	b.ReportMetric(2271, "traversal_microbench_issue")
	b.ReportMetric(float64(columnVectorGraphNativeFrontierHeapFanout), "frontier_heap_fanout")
	b.ReportMetric(float64(heapSize), "frontier_heap_items_per_iteration")
}

func BenchmarkColumnVectorGraphAdjacencyIteration2271(b *testing.B) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("adjacency iteration benchmark requires mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityProductionShape2091()
	rows := columnVectorGraphSearchTopologyParityRows2091(b, shape)
	closeFn, reader, _ := openColumnVectorGraphSearchTopologyParityReader2091(b, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	if reader.adjacencyLayerSources == nil {
		b.Fatalf("reader missing prepared adjacency layer sources")
	}
	var checksum uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ordinal := i % shape.rows
		neighbors, _, reason, ok := reader.directAdjacencyLayerForOrdinal(ordinal, 0)
		if !ok {
			b.Fatalf("directAdjacencyLayerForOrdinal ordinal=%d unavailable reason=%s", ordinal, reason)
		}
		for _, neighbor := range neighbors {
			checksum += uint64(neighbor)
		}
	}
	b.StopTimer()
	columnPhysicalScanBenchSum += int64(checksum)
	b.ReportMetric(2271, "traversal_microbench_issue")
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.degree), "degree")
}
