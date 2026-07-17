package db

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestMaintenanceReachabilityCollectorsSharePageWalkAndMatchStandaloneConsumers(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	grouped := appendPointersInNewSegment(t, db.dir, 0, 1, 410_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("maintenance-reachability-grouped|"), 32)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	writeLeafGenerationKeys(t, db, "base", 1024, 'a')
	b := db.NewBatch().(*Batch)
	recordLen := page.ValuePtrRecordLength(grouped)
	for i := 0; i < 3; i++ {
		ptr := grouped
		ptr.Length = page.ValuePtrMarkGrouped(recordLen, uint8(i))
		if err := b.SetPointer([]byte(fmt.Sprintf("grouped/%d", i)), ptr); err != nil {
			t.Fatalf("SetPointer grouped %d: %v", i, err)
		}
	}
	if err := b.Set([]byte("inline"), []byte("value")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "current", 1, 'z')

	wantRefs, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("standalone ref counts: %v", err)
	}
	wantLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("standalone live bytes: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer closeNoErr(t, snap)
	wantLeaf, err := db.scanLeafGenerationLiveStats(context.Background(), snap)
	if err != nil {
		t.Fatalf("standalone leaf-generation totals: %v", err)
	}

	got, err := db.maintenanceReachabilityScan(context.Background(), snap, maintenanceReachabilityScanOptions{
		Collectors: maintenanceReachabilityValueLogRefCounts |
			maintenanceReachabilityValueLogLiveBytes |
			maintenanceReachabilityLeafGenerationTotals,
	})
	if err != nil {
		t.Fatalf("maintenanceReachabilityScan: %v", err)
	}
	if !reflect.DeepEqual(got.valueLogRefCounts, wantRefs) {
		t.Fatalf("ref counts mismatch: combined=%v standalone=%v", got.valueLogRefCounts, wantRefs)
	}
	if !reflect.DeepEqual(got.valueLogLiveBytesBySegment, wantLive) {
		t.Fatalf("live bytes mismatch: combined=%v standalone=%v", got.valueLogLiveBytesBySegment, wantLive)
	}
	if !reflect.DeepEqual(got.leafGenerationLive, wantLeaf) {
		t.Fatalf("leaf-generation totals mismatch: combined=%v standalone=%v", got.leafGenerationLive, wantLeaf)
	}
	if got.valueLogRefCounts[grouped.FileID] != 3 {
		t.Fatalf("grouped ref count=%d want 3", got.valueLogRefCounts[grouped.FileID])
	}
	if got.valueLogLiveBytesBySegment[grouped.FileID] != int64(recordLen) {
		t.Fatalf("grouped live bytes=%d want one record=%d", got.valueLogLiveBytesBySegment[grouped.FileID], recordLen)
	}
	if got.counters.SharedScans != 1 || got.counters.PagesVisited == 0 {
		t.Fatalf("shared traversal counters=%+v", got.counters)
	}
}

func TestMaintenanceReachabilityCollectorSelectionSkipsUnusedWork(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	ptr := appendPointersInNewSegment(t, db.dir, 0, 1, 420_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("maintenance-reachability-selection|"), 32)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("pointer"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
	writeLeafGenerationKeys(t, db, "leaf", 512, 'l')

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer closeNoErr(t, snap)
	got, err := db.maintenanceReachabilityScan(context.Background(), snap, maintenanceReachabilityScanOptions{
		Collectors: maintenanceReachabilityValueLogRefCounts,
	})
	if err != nil {
		t.Fatalf("maintenanceReachabilityScan ref-only: %v", err)
	}
	if got.valueLogRefCounts[ptr.FileID] != 1 {
		t.Fatalf("ref count=%d want 1", got.valueLogRefCounts[ptr.FileID])
	}
	if got.valueLogLiveBytesBySegment != nil {
		t.Fatalf("live-byte collector unexpectedly initialized: %v", got.valueLogLiveBytesBySegment)
	}
	if got.leafGenerationLive.Generations != nil {
		t.Fatalf("leaf collector unexpectedly initialized: %v", got.leafGenerationLive)
	}
	if got.recordLengthLookups != 0 || got.leafFrameProjections != 0 {
		t.Fatalf("unused collector work: record lengths=%d leaf frames=%d", got.recordLengthLookups, got.leafFrameProjections)
	}
}

func TestScanValueLogRefCountsUsesSharedCollectorAndPreservesHook(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	ptr := appendPointersInNewSegment(t, db.dir, 0, 1, 430_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("maintenance-reachability-ref-migration|"), 32)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("pointer"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
	ctx := context.Background()

	var hookCalls int
	restore := registerScanValueLogRefCountsHook(func() { hookCalls++ })
	defer restore()

	got, gotSeq, err := db.scanValueLogRefCounts(ctx)
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("scan hook calls=%d want 1", hookCalls)
	}
	if gotSeq != db.State().CommitSeq {
		t.Fatalf("commit sequence=%d want published %d", gotSeq, db.State().CommitSeq)
	}
	if got[ptr.FileID] != 1 {
		t.Fatalf("ref count for segment %d=%d want 1", ptr.FileID, got[ptr.FileID])
	}
}

func clearRewritePlanLiveBytesCacheForTest(db *DB) {
	db.rewritePlanLiveBytesMu.Lock()
	db.rewritePlanLiveBytesCache = valueLogRewriteLiveBytesCache{}
	db.rewritePlanLiveBytesMu.Unlock()
}

func TestEstimateValueLogLiveBytesBySegmentUsesSharedCollectorAndCaches(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	grouped := appendPointersInNewSegment(t, db.dir, 0, 1, 440_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("maintenance-reachability-live-migration|"), 32)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	recordLen := page.ValuePtrRecordLength(grouped)
	for i := 0; i < 3; i++ {
		ptr := grouped
		ptr.Length = page.ValuePtrMarkGrouped(recordLen, uint8(i))
		if err := b.SetPointer([]byte(fmt.Sprintf("grouped-live/%d", i)), ptr); err != nil {
			t.Fatalf("SetPointer grouped %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
	ctx := context.Background()

	clearRewritePlanLiveBytesCacheForTest(db)
	var hookCalls int
	restore := registerRewritePlanLiveEstimateHook(func() { hookCalls++ })
	defer restore()
	got, err := db.estimateValueLogLiveBytesBySegment(ctx)
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesBySegment: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("uncached live-estimate hook calls=%d want 1", hookCalls)
	}
	if got[grouped.FileID] != int64(recordLen) {
		t.Fatalf("grouped live bytes=%d want one record=%d", got[grouped.FileID], recordLen)
	}
	if _, err := db.estimateValueLogLiveBytesBySegment(ctx); err != nil {
		t.Fatalf("cached estimateValueLogLiveBytesBySegment: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("cached live estimate reran scanner: hook calls=%d want 1", hookCalls)
	}
}

func TestEstimateValueLogLiveBytesBySegmentRefreshesOnceOnMissingSnapshotFile(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		LeafPageReadCacheEntries:   -1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	t.Cleanup(func() { closeNoErr(t, leafLog) })
	t.Cleanup(func() { closeNoErr(t, db) })
	ptr := appendPointersInNewSegment(t, db.dir, 0, 1, 450_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("maintenance-reachability-live-refresh|"), 32)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("refresh-pointer"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
	writeLeafGenerationKeys(t, db, "refresh-outer-leaf", 1024, 'r')

	clearRewritePlanLiveBytesCacheForTest(db)
	forceStalePublishedValueLogSetForReadRetryTest(t, db)
	refreshBefore := db.valueLogManager.RefreshScanCount()
	var hookCalls int
	restore := registerRewritePlanLiveEstimateHook(func() { hookCalls++ })
	defer restore()

	got, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesBySegment after stale set: %v", err)
	}
	if gotRefreshes := db.valueLogManager.RefreshScanCount() - refreshBefore; gotRefreshes != 1 {
		t.Fatalf("refresh scans=%d want 1", gotRefreshes)
	}
	if hookCalls != 2 {
		t.Fatalf("uncached live-estimate hook calls=%d want initial attempt plus retry", hookCalls)
	}
	if want := int64(page.ValuePtrRecordLength(ptr)); got[ptr.FileID] != want {
		t.Fatalf("live bytes for refreshed segment=%d want %d", got[ptr.FileID], want)
	}
}

func TestMaintenanceReachabilityLeafSubtreeCachePolicy(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	writeLeafGenerationKeys(t, db, "cache", 1024, 'c')
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer closeNoErr(t, snap)

	scan := func(disable bool) maintenanceReachabilityResult {
		t.Helper()
		got, err := db.maintenanceReachabilityScan(context.Background(), snap, maintenanceReachabilityScanOptions{
			Collectors:              maintenanceReachabilityLeafGenerationTotals,
			DisableLeafSubtreeCache: disable,
		})
		if err != nil {
			t.Fatalf("maintenanceReachabilityScan disable=%v: %v", disable, err)
		}
		return got
	}

	db.clearLeafGenerationReachabilityCaches()
	if first := scan(false); first.counters.PagesVisited == 0 {
		t.Fatalf("cold cached scan visited no pages: %+v", first.counters)
	}
	if second := scan(false); second.counters.PagesVisited != 0 || second.counters.MemoHits == 0 {
		t.Fatalf("warm cached scan did not reuse root subtree: %+v", second.counters)
	}

	db.clearLeafGenerationReachabilityCaches()
	if first := scan(true); first.counters.PagesVisited == 0 {
		t.Fatalf("first uncached scan visited no pages: %+v", first.counters)
	}
	if second := scan(true); second.counters.PagesVisited == 0 {
		t.Fatalf("disabled cache unexpectedly reused subtree: %+v", second.counters)
	}
}

func TestScanCandidateExternalReferencesCompressedOuterLeavesDoNotPopulatePointReadCache(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		LeafPageReadCacheEntries:   -1,
		ValueLog: ValueLogOptions{
			Compression:   ValueLogCompressionAuto,
			ReadIntegrity: IntegritySkipChecksums,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		Compression: ValueLogCompressionAuto,
	})
	if err != nil {
		closeNoErr(t, db)
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	db.SetLeafPageLog(leafLog)
	defer closeNoErr(t, leafLog)
	defer closeNoErr(t, db)
	writeLeafGenerationKeys(t, db, "compressed-scan", 2048, 'c')

	// Reset any entries admitted while the write published its durable root. The
	// maintenance scan is a one-pass projection and must not displace the point-
	// read working set with every compressed outer-leaf frame it encounters.
	db.valueLogManager.SetGroupedFrameCacheEntries(0)
	db.valueLogManager.SetGroupedFrameCacheEntries(2048)
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer closeNoErr(t, snap)
	var grouped page.LeafLogPtr
	if err := leafrefscan.WalkRoots(context.Background(), []uint64{snap.state.RootPageID}, snap.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error {
		if grouped.FileID == 0 && page.ValuePtrIsGrouped(ptr.ValuePtr()) {
			grouped = ptr
		}
		return nil
	}); err != nil {
		t.Fatalf("walk outer-leaf refs: %v", err)
	}
	if grouped.FileID == 0 {
		t.Fatal("fixture produced no grouped outer-leaf pointer")
	}
	if data, _, err := db.valueLogManager.ReadUnsafeTo(grouped.ValuePtr(), make([]byte, 0, page.PageSize)); err != nil {
		t.Fatalf("probe compressed grouped outer leaf: %v", err)
	} else if len(data) != page.PageSize {
		t.Fatalf("probe outer leaf size=%d want %d", len(data), page.PageSize)
	}
	if probe := db.valueLogManager.GroupedFrameCacheDetailedStats(); probe.Stores == 0 {
		t.Fatalf("fixture grouped frame was not compressed/cacheable: %+v", probe)
	}
	db.valueLogManager.SetGroupedFrameCacheEntries(0)
	db.valueLogManager.SetGroupedFrameCacheEntries(2048)

	references, err := db.scanCandidateExternalReferencesV1(snap)
	if err != nil {
		t.Fatalf("scanCandidateExternalReferencesV1: %v", err)
	}
	if len(references) == 0 {
		t.Fatal("candidate scan reported no outer-leaf segment dependencies")
	}
	stats := db.valueLogManager.GroupedFrameCacheDetailedStats()
	if stats.Stores != 0 || stats.Entries != 0 || stats.RetainedBytes != 0 {
		t.Fatalf("one-pass maintenance scan polluted grouped point-read cache: %+v", stats)
	}
}

func TestScanLeafGenerationLiveStatsUsesPublishedViewWhenOptionDisabled(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	writeLeafGenerationKeys(t, db, "published-view", 512, 'p')

	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil || snap.state.LeafGenerations == nil {
		t.Fatal("expected snapshot with published leaf-generation state")
	}
	defer closeNoErr(t, snap)

	db.indexOuterLeavesInValueLog = false
	stats, err := db.scanLeafGenerationLiveStats(context.Background(), snap)
	if err != nil {
		t.Fatalf("scanLeafGenerationLiveStats: %v", err)
	}
	livePages := 0
	for _, totals := range stats.Generations {
		livePages += totals.LivePages
	}
	if livePages == 0 {
		t.Fatalf("published leaf-generation view was ignored: %+v", stats.Generations)
	}
}

func BenchmarkMaintenanceReachability(b *testing.B) {
	db := openCompactStorageAuditBenchmarkFixture(b, 4096, 256)
	ctx := context.Background()

	b.Run("ref_counts", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			snap := db.AcquireSnapshot()
			if snap == nil {
				b.Fatal("AcquireSnapshot returned nil")
			}
			_, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{
				Collectors: maintenanceReachabilityValueLogRefCounts,
			})
			if closeErr := snap.Close(); closeErr != nil {
				b.Fatalf("close snapshot: %v", closeErr)
			}
			if err != nil {
				b.Fatalf("maintenanceReachabilityScan: %v", err)
			}
		}
	})
	b.Run("live_bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			clearRewritePlanLiveBytesCacheForTest(db)
			b.StartTimer()
			if _, err := db.estimateValueLogLiveBytesBySegment(ctx); err != nil {
				b.Fatalf("estimateValueLogLiveBytesBySegment: %v", err)
			}
		}
	})
	b.Run("leaf_generation", func(b *testing.B) {
		snap := db.AcquireSnapshot()
		if snap == nil {
			b.Fatal("AcquireSnapshot returned nil")
		}
		defer func() { _ = snap.Close() }()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := db.scanLeafGenerationLiveStatsWithOptions(ctx, snap, leafGenerationLiveStatsScanOptions{DisableCache: true}); err != nil {
				b.Fatalf("scanLeafGenerationLiveStatsWithOptions: %v", err)
			}
		}
	})
	b.Run("all_collectors", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			in, err := db.acquireCompactStorageAuditInput(CompactStorageOptions{})
			if err != nil {
				b.Fatalf("acquireCompactStorageAuditInput: %v", err)
			}
			_, err = db.scanCompactStorageAudit(ctx, in)
			in.close()
			if err != nil {
				b.Fatalf("scanCompactStorageAudit: %v", err)
			}
		}
	})
}
