package db

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkCompactStorageM0 reports the stable M0 fixture name and adapter
// metrics. Setup is intentionally outside the timed interval.
func BenchmarkCompactStorageM0(b *testing.B) {
	for fixtureIndex := range compactStorageM0Fixtures {
		fixtureIndex := fixtureIndex
		b.Run(compactStorageM0Fixtures[fixtureIndex].Name, func(b *testing.B) {
			benchmarkCompactStorageM0Fixture(b, fixtureIndex)
		})
	}
}

func benchmarkCompactStorageM0Fixture(b *testing.B, fixtureIndex int) {
	fixture := compactStorageM0Fixtures[fixtureIndex]
	var totalWall, applyWall, reclaimedBytes, stableCalls int64
	var foregroundP95, idleP95 int64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openCompactStorageM0Fixture(b, fixtureIndex)
		plan, err := db.CompactStoragePlan(context.Background(), compactStorageM0Options(fixtureIndex))
		if err != nil {
			_ = db.Close()
			b.Fatalf("CompactStoragePlan: %v", err)
		}
		fixture = compactStorageM0FixtureMetadata(fixture, db, plan)
		recorder := newCompactStorageM0StableRecorder()
		db.compactStorageBeforePhase = recorder.beginPhase
		db.compactStorageAfterPhase = recorder.endPhase
		restore := installCompactStorageM0Recorder(recorder)
		checkpoint := compactStorageM0CheckpointBaseline(db, recorder)
		if fixtureIndex == 5 {
			idleP95 = int64(m0DurationPercentile(runCompactStorageM0IdleWrites(db, 64), 95))
		}
		b.StartTimer()
		started := time.Now()
		startForeground := make(chan struct{})
		foregroundDone := make(chan []time.Duration, 1)
		if fixtureIndex == 5 {
			go runCompactStorageM0ForegroundWrites(db, startForeground, foregroundDone)
			close(startForeground)
		}
		stats, err := db.CompactStorage(context.Background(), compactStorageM0Options(fixtureIndex))
		elapsed := time.Since(started)
		b.StopTimer()
		if fixtureIndex == 5 {
			foregroundP95 = int64(m0DurationPercentile(<-foregroundDone, 95))
		}
		restore()
		db.compactStorageBeforePhase = nil
		db.compactStorageAfterPhase = nil
		if err != nil {
			if fixtureIndex != 5 {
				_ = db.Close()
				b.Fatalf("CompactStorage: %v", err)
			}
			b.ReportMetric(1, "foreground_maintenance_error")
		}
		measurement := newCompactStorageMeasurement(fixture, elapsed.Nanoseconds(), stats)
		measurement.Checkpoint = checkpoint
		totalWall += measurement.TotalWallTimeNanos
		applyWall += measurement.ApplyWallTimeNanos
		reclaimedBytes += stats.LeafGenerationGC.BytesDeleted
		stableCalls += int64(recorder.totalCalls())
		if err := db.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalWall)/float64(b.N), "m0_total_wall_ns/op")
		b.ReportMetric(float64(applyWall)/float64(b.N), "m0_apply_wall_ns/op")
		b.ReportMetric(float64(reclaimedBytes)/float64(b.N), "m0_reclaimed_bytes/op")
		b.ReportMetric(float64(stableCalls)/float64(b.N), "m0_stable_calls/op")
		b.ReportMetric(float64(fixture.DatabaseBytes), "fixture_database_bytes")
		b.ReportMetric(float64(fixture.PageCount), "fixture_pages")
		b.ReportMetric(float64(fixture.GenerationCount), "fixture_generations")
		if fixtureIndex == 5 {
			b.ReportMetric(float64(foregroundP95), "foreground_p95_ns/op")
			b.ReportMetric(float64(idleP95), "idle_control_p95_ns/op")
		}
	}
}

func runCompactStorageM0IdleWrites(db *DB, count int) []time.Duration {
	latencies := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		started := time.Now()
		_ = db.Set([]byte(fmt.Sprintf("m0/idle/%03d", i)), []byte("idle"))
		latencies = append(latencies, time.Since(started))
	}
	return latencies
}

func runCompactStorageM0ForegroundWrites(db *DB, start <-chan struct{}, done chan<- []time.Duration) {
	<-start
	latencies := make([]time.Duration, 0, 64)
	for i := 0; i < 64; i++ {
		started := time.Now()
		if err := db.Set([]byte(fmt.Sprintf("m0/foreground/%03d", i)), []byte("foreground")); err != nil {
			break
		}
		latencies = append(latencies, time.Since(started))
	}
	done <- latencies
}

func m0DurationPercentile(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := (len(values)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(values) {
		index = len(values)
	}
	return values[index-1]
}

func compactStorageM0CheckpointBaseline(db *DB, recorder *compactStorageM0StableRecorder) compactStorageMeasurementCheckpoint {
	result := compactStorageMeasurementCheckpoint{Availability: compactStorageMeasurementUnavailable, StableCallCounter: compactStorageMeasurementUnavailable, CoverageReason: "root-publication-stats-unavailable"}
	if db == nil || db.rootPublication == nil {
		return result
	}
	before := db.rootPublication.coordinator.Stats()
	started := time.Now()
	if err := db.Checkpoint(); err != nil {
		result.CoverageReason = "checkpoint-error:" + err.Error()
		return result
	}
	after := db.rootPublication.coordinator.Stats()
	result.Availability = compactStorageMeasurementObserved
	result.StableCallCounter = compactStorageMeasurementObserved
	result.CoverageReason = "explicit-checkpoint-current-path"
	result.ExactCoverageObserved = after.DurableCommitSeq >= after.VisibleCommitSeq
	result.WallTimeNanos = time.Since(started).Nanoseconds()
	result.BeforeVisibleFrontier = before.VisibleCommitSeq
	result.AfterVisibleFrontier = after.VisibleCommitSeq
	result.BeforeDurableFrontier = before.DurableCommitSeq
	result.AfterDurableFrontier = after.DurableCommitSeq
	result.StableCalls = recorder.totalCalls()
	return result
}

func compactStorageM0Options(index int) CompactStorageOptions {
	switch index {
	case 0:
		return CompactStorageOptions{LeafPackMaxPasses: 4, LeafPackMaxGenerationsPerPass: 1, LeafPackMinReclaimPerCopyPPM: 1}
	case 5:
		return CompactStorageOptions{Mode: CompactStorageFull, LeafPackMaxPasses: 4, LeafPackMaxGenerationsPerPass: 1, LeafPackMinReclaimPerCopyPPM: 1}
	case 4:
		return CompactStorageOptions{Mode: CompactStorageExhaustive}
	default:
		return CompactStorageOptions{Mode: CompactStorageFull}
	}
}

func openCompactStorageM0Fixture(b *testing.B, index int) *DB {
	if index == 0 || index == 1 || index == 4 {
		return openCompactStorageLeafPackBenchmarkFixture(b)
	}
	if index == 5 {
		return openCompactStorageRewritePolicyBenchmarkFixture(b, 2047, 1, 1024)
	}
	if index == 2 {
		return openCompactStorageRewritePolicyBenchmarkFixture(b, 2047, 1, 1024)
	}
	return openCompactStorageRewritePolicyBenchmarkFixture(b, 1024, 2048, 1024)
}

func compactStorageM0FixtureMetadata(f compactStorageMeasurementFixture, db *DB, plan CompactStorageStats) compactStorageMeasurementFixture {
	f.DatabaseBytesAvailability, f.PageCountAvailability, f.DebtAvailability = "unavailable", "unavailable", "unavailable"
	for _, usage := range plan.Before {
		if usage.Name == "total" {
			f.DatabaseBytes, f.DatabaseBytesAvailability = usage.Bytes, "observed"
		}
	}
	if generation := db.idx.Load(); generation != nil && generation.pager != nil {
		f.PageCount, f.PageCountAvailability = int(generation.pager.PageCount()), "observed"
	}
	f.GenerationCount, f.DebtAvailability = len(plan.LeafGenerationPlan.Generations), "observed"
	f.LiveDebtBytes = plan.LeafGenerationPlan.CandidateBytesLive
	f.DeadDebtBytes = plan.LeafGenerationPlan.CandidateBytesDead
	return f
}

func compactStorageM0ArtifactName(fixture string, iteration int) string {
	return "compact-storage-m0/" + fixture + "/seed=" + strconv.Itoa(3733001+iteration)
}

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
		ValueLog:                   ValueLogOptions{PointerThreshold: 1},
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
	tb.Helper()
	db, err := Open(Options{Dir: tb.TempDir(), ValueLog: ValueLogOptions{PointerThreshold: 1}})
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
