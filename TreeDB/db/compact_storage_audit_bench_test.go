package db

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkCompactStorageSharedAudit(b *testing.B) {
	db := openCompactStorageAuditBenchmarkFixture(b, 4096, 256)
	opts := CompactStorageOptions{
		Mode:                           CompactStorageFull,
		DisableZeroByteValueLogCleanup: true,
	}
	var refScans, liveScans, leafScans atomic.Uint64
	unregisterRefs := registerScanValueLogRefCountsHook(func() { refScans.Add(1) })
	unregisterLive := registerRewritePlanLiveEstimateHook(func() { liveScans.Add(1) })
	unregisterLeaf := registerLeafGenerationLiveScanHook(func() { leafScans.Add(1) })
	b.Cleanup(unregisterRefs)
	b.Cleanup(unregisterLive)
	b.Cleanup(unregisterLeaf)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.rewritePlanLiveBytesMu.Lock()
		db.rewritePlanLiveBytesCache = valueLogRewriteLiveBytesCache{}
		db.rewritePlanLiveBytesMu.Unlock()
		db.clearLeafGenerationReachabilityCaches()
		b.StartTimer()
		if _, err := db.CompactStoragePlan(context.Background(), opts); err != nil {
			b.Fatalf("CompactStoragePlan: %v", err)
		}
	}
	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(refScans.Load())/float64(b.N), "legacy_ref_scans/op")
		b.ReportMetric(float64(liveScans.Load())/float64(b.N), "legacy_live_scans/op")
		b.ReportMetric(float64(leafScans.Load())/float64(b.N), "legacy_leaf_scans/op")
	}
}

func openCompactStorageAuditBenchmarkFixture(b *testing.B, records, valueSize int) *DB {
	b.Helper()
	dir := b.TempDir()
	ptrs := appendPointersInNewSegmentBench(b, dir, 0, 1, 1, records, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i%23)}, valueSize)
	})
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	b.Cleanup(func() { benchmarkCloseNoErr(b, leafLog) })
	b.Cleanup(func() { benchmarkCloseNoErr(b, db) })

	batch := db.NewBatch().(*Batch)
	for i := 0; i < records; i++ {
		key := []byte(fmt.Sprintf("audit-%06d", i))
		if err := batch.SetPointer(key, ptrs[i]); err != nil {
			b.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		b.Fatalf("WriteSync: %v", err)
	}
	benchmarkCloseNoErr(b, batch)
	if err := db.RefreshValueLogSet(); err != nil {
		b.Fatalf("RefreshValueLogSet: %v", err)
	}
	return db
}
