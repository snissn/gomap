package collections

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestVectorIndexSearcherSharedPreparedResourceCounters1735(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("shared prepared public-searcher resource test requires mmap_direct support")
	}
	rows := columnGraphRebuildSyntheticRowsV2A(128, 16)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 16, 8, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	const workers = 3
	searchers := make([]*VectorIndexSearcher, workers)
	defer func() {
		for _, searcher := range searchers {
			_ = searcher.Close()
		}
	}()
	for i := range searchers {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		searchers[i] = searcher
	}
	snap := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if snap.Entries != 1 || snap.Refs != workers || snap.CacheBuilds != 1 || snap.CacheMisses != 1 || snap.CacheHits != workers-1 || snap.ActiveHandles == 0 || snap.ActiveMappedBytes == 0 || snap.ActiveHeapCopyBytes != 0 {
		t.Fatalf("shared prepared public-searcher snapshot=%+v want one mmap-backed holder with %d refs", snap, workers)
	}
	query := append([]float32(nil), rows[11].vector...)
	opts := VectorIndexSearcherSearchOptions{Query: query, TopK: 8, EfSearch: 64, StatsMode: VectorIndexSearchStatsModeProduction}
	baseline, err := searchers[0].SearchWithBuffer(opts, &VectorIndexSearchBuffer{})
	if err != nil {
		t.Fatalf("baseline SearchWithBuffer: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for worker := range searchers {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buffer VectorIndexSearchBuffer
			for i := 0; i < 10; i++ {
				got, err := searchers[worker].SearchWithBuffer(opts, &buffer)
				if err != nil {
					errs <- fmt.Sprintf("worker %d SearchWithBuffer: %v", worker, err)
					return
				}
				if len(got.Results) != len(baseline.Results) || len(got.Results) == 0 || got.Results[0].Ordinal != baseline.Results[0].Ordinal {
					errs <- fmt.Sprintf("worker %d iteration %d top result=%v baseline=%v", worker, i, got.Results, baseline.Results)
					return
				}
				if got.Stats.SearchRouteHNSWSearchPack != 1 || got.Stats.HNSWSearchPackActive != 1 || got.Stats.HNSWSearchPackFallbacks != 0 || got.Stats.TypedColumnFallbacks != 0 || got.Stats.GraphRowFallbacks != 0 || got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.VectorScratchDecodes != 0 {
					errs <- fmt.Sprintf("worker %d stats=%+v want shared hnsw_search_pack_v1 path", worker, got.Stats)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if err := searchers[0].Close(); err != nil {
		t.Fatalf("close searcher 0: %v", err)
	}
	searchers[0] = nil
	afterFirstClose := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if afterFirstClose.Entries != 1 || afterFirstClose.Refs != workers-1 || afterFirstClose.ActiveHandles != snap.ActiveHandles || afterFirstClose.ActiveMappedBytes != snap.ActiveMappedBytes {
		t.Fatalf("shared prepared after first close=%+v initial=%+v want remaining refs to retain one holder", afterFirstClose, snap)
	}
	if _, err := searchers[1].SearchWithBuffer(opts, &VectorIndexSearchBuffer{}); err != nil {
		t.Fatalf("searcher 1 SearchWithBuffer after searcher 0 close: %v", err)
	}
	for i := range searchers {
		if searchers[i] != nil {
			if err := searchers[i].Close(); err != nil {
				t.Fatalf("close searcher %d: %v", i, err)
			}
			searchers[i] = nil
		}
	}
	afterAllClosed := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if afterAllClosed.Entries != 0 || afterAllClosed.Refs != 0 || afterAllClosed.ActiveHandles != 0 || afterAllClosed.ActiveMappedBytes != 0 {
		t.Fatalf("shared prepared after all searchers closed=%+v want no active shared resources", afterAllClosed)
	}
}

func TestColumnVectorGraphSharedPreparedSearchCachesNotEligible1735(t *testing.T) {
	col := &Collection{}
	builds := 0
	for i := 0; i < 2; i++ {
		_, err := col.acquireColumnVectorGraphSharedPreparedSearch("not-eligible", func() (*columnVectorGraphSharedPreparedSearch, error) {
			builds++
			return nil, errColumnVectorGraphSharedPreparedSearchNotEligible
		})
		if !errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
			t.Fatalf("acquire %d err=%v want not-eligible sentinel", i, err)
		}
	}
	if builds != 1 {
		t.Fatalf("not-eligible builds=%d want one negative-cached build", builds)
	}
	snap := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if snap.Entries != 0 || snap.CacheBuilds != 1 || snap.CacheMisses != 1 || snap.CacheHits != 0 {
		t.Fatalf("not-eligible snapshot=%+v want inactive negative cache entry with one build/miss", snap)
	}
}

func TestColumnVectorGraphSharedPreparedSearchCacheKeyCanonicalizesStateAssets1735(t *testing.T) {
	def := VectorIndexDefinition{Name: "vec", Field: "embedding", Strategy: VectorIndexStrategyColumnGraph, Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 4, M: 8, EfConstruction: 32, EfSearch: 64}
	graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, BaseManifestGeneration: 7, BaseManifestChecksum: 11, BaseSchemaHash: 13, GraphSchemaHash: 17, RowCount: 2, AdjacencyLayerCount: 1}
	assetA := columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleInverseNorm, AssetID: columnVectorGraphInvNormStateAssetID, LogicalType: columnVectorIndexStateLogicalTypeFloat32, PhysicalEncoding: columnVectorIndexStateEncodingRawFloat32, RowCount: 2, SourceSchemaHash: 101, AssetBytes: 16, Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "ns", Generation: 7, PartID: 1, FileID: 2, Offset: 3, Length: 16, Checksum: 5}}
	assetB := columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleAdjacency, AssetID: columnVectorIndexStateAdjacencyAssetID(0), LogicalType: columnVectorIndexStateLogicalTypeUint32List, PhysicalEncoding: columnVectorIndexStateEncodingRawUint32List, RowCount: 2, SourceSchemaHash: 103, AssetBytes: 24, Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "ns", Generation: 7, PartID: 2, FileID: 3, Offset: 4, Length: 24, Checksum: 6}}
	state := columnVectorIndexStateSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, RowCount: 2, BaseManifestGeneration: 7, BaseManifestChecksum: 11, BaseSchemaHash: 13, AdjacencyLayerCount: 1, Assets: []columnVectorIndexStateAssetSnapshot{assetA, assetB}}
	keyAB, err := columnVectorGraphSharedPreparedSearchCacheKey("docs", "ns", def, graph, state)
	if err != nil {
		t.Fatalf("cache key AB: %v", err)
	}
	state.Assets = []columnVectorIndexStateAssetSnapshot{assetB, assetA}
	keyBA, err := columnVectorGraphSharedPreparedSearchCacheKey("docs", "ns", def, graph, state)
	if err != nil {
		t.Fatalf("cache key BA: %v", err)
	}
	if keyAB != keyBA {
		t.Fatalf("cache keys differ after asset reordering\nAB=%s\nBA=%s", keyAB, keyBA)
	}
}

func BenchmarkVectorSearchSharedPreparedOpenWarmupResources1735(b *testing.B) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("shared prepared resource benchmark requires mmap_direct support")
	}
	for _, workers := range []int{1, 4, 8} {
		workers := workers
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			benchmarkVectorSearchSharedPreparedOpenWarmupResources1735(b, workers)
		})
	}
}

func benchmarkVectorSearchSharedPreparedOpenWarmupResources1735(b *testing.B, workers int) {
	b.Helper()
	const (
		rows     = 8192
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{Query: query, TopK: topK, EfSearch: efSearch, StatsMode: VectorIndexSearchStatsModeProduction}
	sample, err := openWarmCloseVectorSearchersSharedPrepared1735(col, def.Name, opts, workers, true)
	if err != nil {
		b.Fatalf("sample open/warm/close: %v", err)
	}
	if sample.snapshot.Entries != 1 || sample.snapshot.Refs != workers || sample.snapshot.CacheBuilds == 0 || sample.snapshot.ActiveHandles == 0 || sample.snapshot.ActiveMappedBytes == 0 || sample.snapshot.ActiveHeapCopyBytes != 0 {
		b.Fatalf("sample shared prepared snapshot=%+v want one mmap-backed holder with %d refs", sample.snapshot, workers)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := openWarmCloseVectorSearchersSharedPrepared1735(col, def.Name, opts, workers, false); err != nil {
			b.Fatalf("iteration %d open/warm/close: %v", i, err)
		}
	}
	b.StopTimer()
	reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, sample.snapshot, workers)
	b.ReportMetric(float64(rows), "graph_rows")
	b.ReportMetric(float64(topK), "top_k")
	b.ReportMetric(float64(efSearch), "ef_search")
	b.ReportMetric(float64(sample.openPrepareNanos)/float64(workers), "open_prepare_ns/worker")
	b.ReportMetric(float64(sample.warmupNanos)/float64(workers), "warmup_ns/worker")
	if sample.heapAllocBytes > 0 {
		b.ReportMetric(float64(sample.heapAllocBytes), "heap_alloc_B")
	}
	if sample.heapAllocDeltaBytes > 0 {
		b.ReportMetric(float64(sample.heapAllocDeltaBytes), "heap_alloc_delta_B")
	}
	if sample.maxRSSBytes > 0 {
		b.ReportMetric(float64(sample.maxRSSBytes), "process_maxrss_B")
	}
	if sample.maxRSSDeltaBytes > 0 {
		b.ReportMetric(float64(sample.maxRSSDeltaBytes), "process_maxrss_delta_B")
	}
}

type columnVectorGraphSharedPreparedOpenWarmupSample1735 struct {
	snapshot            columnVectorGraphSharedPreparedSearchCacheSnapshot
	openPrepareNanos    int64
	warmupNanos         int64
	heapAllocBytes      uint64
	heapAllocDeltaBytes uint64
	maxRSSBytes         uint64
	maxRSSDeltaBytes    uint64
}

func openWarmCloseVectorSearchersSharedPrepared1735(col *Collection, indexName string, opts VectorIndexSearcherSearchOptions, workers int, captureMemory bool) (columnVectorGraphSharedPreparedOpenWarmupSample1735, error) {
	var sample columnVectorGraphSharedPreparedOpenWarmupSample1735
	if workers <= 0 {
		return sample, fmt.Errorf("workers=%d", workers)
	}
	var heapBefore uint64
	var rssBefore uint64
	if captureMemory {
		heapBefore = runtimeHeapAllocBytes1735()
		rssBefore = processMaxRSSBytes1735()
	}
	searchers := make([]*VectorIndexSearcher, workers)
	defer func() {
		for _, searcher := range searchers {
			_ = searcher.Close()
		}
	}()
	openStart := time.Now()
	for i := range searchers {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: indexName, MaxDecodedBlocks: 1})
		if err != nil {
			return sample, fmt.Errorf("OpenVectorIndexSearcher worker %d: %w", i, err)
		}
		searchers[i] = searcher
	}
	sample.openPrepareNanos = time.Since(openStart).Nanoseconds()
	buffers := make([]VectorIndexSearchBuffer, workers)
	warmStart := time.Now()
	for i, searcher := range searchers {
		got, err := searcher.SearchWithBuffer(opts, &buffers[i])
		if err != nil {
			return sample, fmt.Errorf("warm SearchWithBuffer worker %d: %w", i, err)
		}
		if len(got.Results) == 0 {
			return sample, fmt.Errorf("warm SearchWithBuffer worker %d returned no results", i)
		}
		if got.Stats.SearchRouteHNSWSearchPack != 1 || got.Stats.HNSWSearchPackActive != 1 || got.Stats.HNSWSearchPackFallbacks != 0 || got.Stats.TypedColumnFallbacks != 0 || got.Stats.GraphRowFallbacks != 0 || got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.VectorScratchDecodes != 0 {
			return sample, fmt.Errorf("warm SearchWithBuffer worker %d stats=%+v want shared hnsw_search_pack_v1 path", i, got.Stats)
		}
	}
	sample.warmupNanos = time.Since(warmStart).Nanoseconds()
	sample.snapshot = col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if captureMemory {
		heapAfter := runtimeHeapAllocBytes1735()
		sample.heapAllocBytes = heapAfter
		if heapAfter > heapBefore {
			sample.heapAllocDeltaBytes = heapAfter - heapBefore
		}
		rssAfter := processMaxRSSBytes1735()
		sample.maxRSSBytes = rssAfter
		if rssAfter > rssBefore {
			sample.maxRSSDeltaBytes = rssAfter - rssBefore
		}
	}
	return sample, nil
}

func reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b *testing.B, snap columnVectorGraphSharedPreparedSearchCacheSnapshot, workers int) {
	b.Helper()
	b.ReportMetric(float64(snap.Entries), "shared_prepared_entries")
	b.ReportMetric(float64(snap.Refs), "shared_prepared_refs")
	b.ReportMetric(float64(snap.BuildingEntries), "shared_prepared_building_entries")
	b.ReportMetric(float64(snap.CacheBuilds), "shared_prepared_builds")
	b.ReportMetric(float64(snap.CacheMisses), "shared_prepared_cache_misses")
	b.ReportMetric(float64(snap.CacheHits), "shared_prepared_cache_hits")
	b.ReportMetric(float64(snap.CacheWaits), "shared_prepared_cache_waits")
	b.ReportMetric(float64(snap.ActiveHandles), "shared_prepared_active_handles")
	b.ReportMetric(float64(snap.ActiveMappedBytes), "shared_prepared_mapped_B")
	b.ReportMetric(float64(snap.ActiveHeapCopyBytes), "shared_prepared_heap_copy_B")
	b.ReportMetric(float64(snap.ActiveDerivedMetadataBytes), "shared_prepared_derived_metadata_B")
	b.ReportMetric(float64(snap.TotalAcquires), "shared_prepared_total_acquires")
	b.ReportMetric(float64(snap.TotalReleases), "shared_prepared_total_releases")
	b.ReportMetric(float64(snap.FallbackReads), "shared_prepared_fallback_reads")
	if workers > 0 {
		b.ReportMetric(float64(workers), "parallel_workers")
		b.ReportMetric(float64(snap.ActiveHandles)/float64(workers), "shared_prepared_active_handles/worker")
		b.ReportMetric(float64(snap.ActiveMappedBytes)/float64(workers), "shared_prepared_mapped_B/worker")
	}
}

func runtimeHeapAllocBytes1735() uint64 {
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return mem.HeapAlloc
}
