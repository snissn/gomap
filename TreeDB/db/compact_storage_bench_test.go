package db

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func BenchmarkCompactStorageRewritePolicyMostlyLivePlan(b *testing.B) {
	db := openCompactStorageRewritePolicyBenchmarkFixture(b, 2047, 1, 1024)
	defer func() { _ = db.Close() }()

	opts := CompactStorageOptions{
		Mode:                           CompactStorageFull,
		DisableZeroByteValueLogCleanup: true,
	}
	var selectedBytes, copiedBytes, staleBytes, selectedStaleBytes int64
	var audit CompactStorageAuditStats
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, err := db.CompactStoragePlan(context.Background(), opts)
		if err != nil {
			b.Fatalf("CompactStoragePlan: %v", err)
		}
		selectedBytes += plan.ValueLogRewritePlan.SelectedBytesTotal
		copiedBytes += plan.ValueLogRewritePlan.SelectedBytesLive
		staleBytes += plan.ValueLogRewritePlan.BytesStale
		selectedStaleBytes += plan.ValueLogRewritePlan.SelectedBytesStale
		addCompactStorageAuditStats(&audit, plan.Audit)
	}
	if b.N > 0 {
		b.ReportMetric(float64(selectedBytes)/float64(b.N), "selected_bytes/op")
		b.ReportMetric(float64(copiedBytes)/float64(b.N), "copied_bytes/op")
		b.ReportMetric(float64(staleBytes)/float64(b.N), "stale_bytes/op")
		b.ReportMetric(float64(selectedStaleBytes)/float64(b.N), "selected_stale_bytes/op")
		b.ReportMetric(float64(audit.SharedScans)/float64(b.N), "shared_scans/op")
		b.ReportMetric(float64(audit.StructuralReuseHits)/float64(b.N), "reuse_hits/op")
		b.ReportMetric(float64(audit.RootSets)/float64(b.N), "root_sets/op")
		b.ReportMetric(float64(audit.PagesVisited)/float64(b.N), "pages/op")
		b.ReportMetric(float64(audit.MemoHits)/float64(b.N), "memo_hits/op")
		b.ReportMetric(float64(audit.PointerProjections)/float64(b.N), "pointer_projections/op")
		b.ReportMetric(float64(audit.GroupedRecordDedupeHits)/float64(b.N), "grouped_dedupe_hits/op")
		b.ReportMetric(float64(audit.PhysicalBytesRead)/float64(b.N), "physical_bytes_read/op")
	}
}

func BenchmarkCompactStorageRewritePolicyMostlyLiveApply(b *testing.B) {
	var selectedBytes, copiedBytes, staleBytes, selectedStaleBytes int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openCompactStorageRewritePolicyBenchmarkFixture(b, 2047, 1, 1024)
		beforePlan, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{
			Mode:                           CompactStorageFull,
			DisableZeroByteValueLogCleanup: true,
		})
		if err != nil {
			_ = db.Close()
			b.Fatalf("CompactStoragePlan before apply: %v", err)
		}
		b.StartTimer()

		stats, err := db.CompactStorage(context.Background(), CompactStorageOptions{
			Mode:                           CompactStorageFull,
			DisableZeroByteValueLogCleanup: true,
		})
		b.StopTimer()
		if err != nil {
			_ = db.Close()
			b.Fatalf("CompactStorage: %v", err)
		}
		selectedBytes += stats.ValueLogRewrite.SourceBytesRequested
		copiedBytes += stats.ValueLogRewrite.ValueBytesCopied
		staleBytes += beforePlan.ValueLogRewritePlan.BytesStale
		selectedStaleBytes += beforePlan.ValueLogRewritePlan.SelectedBytesStale
		if err := db.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
		b.StartTimer()
	}
	if b.N > 0 {
		b.ReportMetric(float64(selectedBytes)/float64(b.N), "selected_bytes/op")
		b.ReportMetric(float64(copiedBytes)/float64(b.N), "copied_bytes/op")
		b.ReportMetric(float64(staleBytes)/float64(b.N), "stale_bytes/op")
		b.ReportMetric(float64(selectedStaleBytes)/float64(b.N), "selected_stale_bytes/op")
	}
}

func openCompactStorageRewritePolicyBenchmarkFixture(tb testing.TB, liveRecords, staleRecords, valueSize int) *DB {
	tb.Helper()
	db, err := Open(Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	total := liveRecords + staleRecords
	ptrs := appendPointersInNewSegmentBench(tb, db.dir, 0, 1, 560_000, total, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i%23)}, valueSize)
	})
	activePtrs := appendPointersInNewSegmentBench(tb, db.dir, 0, 2, 660_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("z"), valueSize)
	})
	b, ok := db.NewBatch().(*Batch)
	if !ok {
		_ = db.Close()
		tb.Fatalf("NewBatch type assertion failed")
	}
	for i := 0; i < liveRecords; i++ {
		if err := b.SetPointer([]byte(fmt.Sprintf("source-live-%06d", i)), ptrs[i]); err != nil {
			_ = b.Close()
			_ = db.Close()
			tb.Fatalf("SetPointer source %d: %v", i, err)
		}
	}
	if err := b.SetPointer([]byte("active-live"), activePtrs[0]); err != nil {
		_ = b.Close()
		_ = db.Close()
		tb.Fatalf("SetPointer active: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		tb.Fatalf("batch write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		tb.Fatalf("batch close: %v", err)
	}
	if err := db.RefreshValueLogSet(); err != nil {
		_ = db.Close()
		tb.Fatalf("RefreshValueLogSet: %v", err)
	}
	return db
}
