package db

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkCompactStorageLeafPackMultiPass(b *testing.B) {
	var liveScans, packPasses atomic.Uint64
	var packPhaseNanos, packApplyNanos, packCopyNanos int64
	var packPublishWaitNanos, packPublishHoldNanos int64
	var packSetupNanos, packTreeRewriteNanos, packLeafSyncNanos, packCopyCloseNanos int64
	var packRevalidateNanos, packPromotionNanos, packRelocationNanos, packPageSyncNanos int64
	var packDirectorySyncNanos, packDirectorySyncWaitNanos, packRegistrationNanos int64
	var packCollectionPublishNanos, packFinalizeNanos, packPostWorkNanos, packCleanupNanos int64
	unregister := registerLeafGenerationLiveScanHook(func() { liveScans.Add(1) })
	defer unregister()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openCompactStorageLeafPackBenchmarkFixture(b)
		db.compactStorageAfterPhase = func(name string) {
			if len(name) >= len("leaf-generation-pack-") && name[:len("leaf-generation-pack-")] == "leaf-generation-pack-" {
				packPasses.Add(1)
			}
		}
		b.StartTimer()

		stats, err := db.CompactStorage(context.Background(), CompactStorageOptions{
			LeafPackMaxPasses:             4,
			LeafPackMaxGenerationsPerPass: 1,
			LeafPackMinReclaimPerCopyPPM:  1,
		})
		b.StopTimer()
		if err != nil {
			_ = db.Close()
			b.Fatalf("CompactStorage: %v", err)
		}
		ranPacks := 0
		for _, pack := range stats.LeafGenerationPacks {
			if pack.Ran {
				ranPacks++
			}
		}
		if ranPacks < 3 {
			_ = db.Close()
			b.Fatalf("ran leaf-pack passes=%d want at least 3", ranPacks)
		}
		for _, phase := range stats.Phases {
			if len(phase.Name) >= len("leaf-generation-pack-") && phase.Name[:len("leaf-generation-pack-")] == "leaf-generation-pack-" {
				packPhaseNanos += phase.WallTimeNanos
			}
		}
		for _, pack := range stats.LeafGenerationPacks {
			packApplyNanos += pack.Pack.WallTimeNanos
			packCopyNanos += pack.Pack.CopyTimeNanos
			packPublishWaitNanos += pack.Pack.PublishWaitNanos
			packPublishHoldNanos += pack.Pack.PublishHoldNanos
			stages := pack.Pack.ApplyStages
			packSetupNanos += stages.SetupTimeNanos
			packTreeRewriteNanos += stages.TreeRewriteTimeNanos
			packLeafSyncNanos += stages.LeafSyncTimeNanos
			packCopyCloseNanos += stages.CopyCloseTimeNanos
			packRevalidateNanos += stages.RevalidateTimeNanos
			packPromotionNanos += stages.PromotionTimeNanos
			packRelocationNanos += stages.RelocationTimeNanos
			packPageSyncNanos += stages.PageSyncTimeNanos
			packDirectorySyncNanos += stages.DirectorySyncTimeNanos
			packDirectorySyncWaitNanos += stages.DirectorySyncWaitTimeNanos
			packRegistrationNanos += stages.RegistrationTimeNanos
			packCollectionPublishNanos += stages.CollectionPublishTimeNanos
			packFinalizeNanos += stages.FinalizeTimeNanos
			packPostWorkNanos += stages.PostWorkTimeNanos
			packCleanupNanos += stages.CleanupTimeNanos
		}
		db.compactStorageAfterPhase = nil
		if err := db.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
		b.StartTimer()
	}
	if b.N > 0 {
		b.ReportMetric(float64(liveScans.Load())/float64(b.N), "live_scans/op")
		b.ReportMetric(float64(packPasses.Load())/float64(b.N), "pack_passes/op")
		b.ReportMetric(float64(packPhaseNanos)/float64(b.N), "pack_phase_ns/op")
		b.ReportMetric(float64(packApplyNanos)/float64(b.N), "pack_apply_ns/op")
		b.ReportMetric(float64(packCopyNanos)/float64(b.N), "pack_copy_ns/op")
		b.ReportMetric(float64(packPublishWaitNanos)/float64(b.N), "pack_publish_wait_ns/op")
		b.ReportMetric(float64(packPublishHoldNanos)/float64(b.N), "pack_publish_hold_ns/op")
		b.ReportMetric(float64(packSetupNanos)/float64(b.N), "pack_setup_ns/op")
		b.ReportMetric(float64(packTreeRewriteNanos)/float64(b.N), "pack_tree_rewrite_ns/op")
		b.ReportMetric(float64(packLeafSyncNanos)/float64(b.N), "pack_leaf_sync_ns/op")
		b.ReportMetric(float64(packCopyCloseNanos)/float64(b.N), "pack_copy_close_ns/op")
		b.ReportMetric(float64(packRevalidateNanos)/float64(b.N), "pack_revalidate_ns/op")
		b.ReportMetric(float64(packPromotionNanos)/float64(b.N), "pack_promotion_ns/op")
		b.ReportMetric(float64(packRelocationNanos)/float64(b.N), "pack_relocation_ns/op")
		b.ReportMetric(float64(packPageSyncNanos)/float64(b.N), "pack_page_sync_ns/op")
		b.ReportMetric(float64(packDirectorySyncNanos)/float64(b.N), "pack_directory_sync_ns/op")
		b.ReportMetric(float64(packDirectorySyncWaitNanos)/float64(b.N), "pack_directory_sync_wait_ns/op")
		b.ReportMetric(float64(packRegistrationNanos)/float64(b.N), "pack_registration_ns/op")
		b.ReportMetric(float64(packCollectionPublishNanos)/float64(b.N), "pack_collection_publish_ns/op")
		b.ReportMetric(float64(packFinalizeNanos)/float64(b.N), "pack_finalize_ns/op")
		b.ReportMetric(float64(packPostWorkNanos)/float64(b.N), "pack_post_work_ns/op")
		b.ReportMetric(float64(packCleanupNanos)/float64(b.N), "pack_cleanup_ns/op")
	}
}

func openCompactStorageLeafPackBenchmarkFixture(b *testing.B) *DB {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)

	for generation := 0; generation < 4; generation++ {
		writeLeafGenerationBenchKeyRange(b, db, "shared", 0, 4096, byte('a'+generation))
		writeLeafGenerationBenchKeyRange(b, db, fmt.Sprintf("keep-%d", generation), 0, 64, byte('k'+generation))
		if generation < 3 {
			if err := leafLog.rotateLeaf(); err != nil {
				_ = leafLog.Close()
				_ = db.Close()
				b.Fatalf("rotateLeaf generation %d: %v", generation, err)
			}
		}
	}
	db.SetLeafPageLog(nil)
	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		b.Fatalf("close leaf log: %v", err)
	}
	return db
}

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
	return openCompactStorageRewritePolicyBenchmarkFixtureWithThreshold(tb, liveRecords, staleRecords, valueSize, 1)
}

func openCompactStorageRewritePolicyBenchmarkFixtureWithThreshold(tb testing.TB, liveRecords, staleRecords, valueSize, pointerThreshold int) *DB {
	tb.Helper()
	db, err := Open(Options{Dir: tb.TempDir(), ValueLog: ValueLogOptions{PointerThreshold: pointerThreshold}})
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
