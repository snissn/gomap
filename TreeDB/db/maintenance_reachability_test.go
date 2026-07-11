package db

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestMaintenanceReachabilityCollectorsSharePageWalkAndMatchLegacy(t *testing.T) {
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

	wantRefs, _, err := db.scanValueLogRefCountsLegacy(context.Background())
	if err != nil {
		t.Fatalf("legacy ref counts: %v", err)
	}
	wantLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("legacy live bytes: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer closeNoErr(t, snap)
	wantLeaf, err := db.scanLeafGenerationLiveStats(context.Background(), snap)
	if err != nil {
		t.Fatalf("legacy leaf-generation totals: %v", err)
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
		t.Fatalf("ref counts mismatch: shared=%v legacy=%v", got.valueLogRefCounts, wantRefs)
	}
	if !reflect.DeepEqual(got.valueLogLiveBytesBySegment, wantLive) {
		t.Fatalf("live bytes mismatch: shared=%v legacy=%v", got.valueLogLiveBytesBySegment, wantLive)
	}
	if !reflect.DeepEqual(got.leafGenerationLive, wantLeaf) {
		t.Fatalf("leaf-generation totals mismatch: shared=%v legacy=%v", got.leafGenerationLive, wantLeaf)
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

func TestScanValueLogRefCountsUsesSharedCollectorAndMatchesLegacy(t *testing.T) {
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

	want, wantSeq, err := db.scanValueLogRefCountsLegacy(ctx)
	if err != nil {
		t.Fatalf("legacy scanValueLogRefCounts: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("legacy ref-count fixture produced no value-log references")
	}
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
	if gotSeq != wantSeq {
		t.Fatalf("commit sequence=%d want %d", gotSeq, wantSeq)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ref counts mismatch: shared=%v legacy=%v", got, want)
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

func BenchmarkMaintenanceReachability(b *testing.B) {
	db := openCompactStorageAuditBenchmarkFixture(b, 4096, 256)
	ctx := context.Background()

	b.Run("legacy_ref_counts", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := db.scanValueLogRefCountsLegacy(ctx); err != nil {
				b.Fatalf("scanValueLogRefCountsLegacy: %v", err)
			}
		}
	})
	b.Run("shared_ref_counts", func(b *testing.B) {
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
	b.Run("legacy_all_collectors", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			in, err := db.acquireCompactStorageAuditInput(CompactStorageOptions{})
			if err != nil {
				b.Fatalf("acquireCompactStorageAuditInput: %v", err)
			}
			_, err = db.scanCompactStorageAuditLegacy(ctx, in)
			in.close()
			if err != nil {
				b.Fatalf("scanCompactStorageAuditLegacy: %v", err)
			}
		}
	})
	b.Run("shared_all_collectors", func(b *testing.B) {
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
