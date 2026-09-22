package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func benchmarkLeafGenerationPlanFixtureDir(b *testing.B, envName string) string {
	b.Helper()
	fixture := os.Getenv(envName)
	if fixture == "" {
		b.Skipf("set %s to a saved application.db or maindb fixture", envName)
	}
	if _, err := os.Stat(fixture); err != nil {
		b.Fatalf("stat fixture %q: %v", fixture, err)
	}
	if _, err := os.Stat(fixture + "/index.db"); err == nil {
		return fixture
	}
	if _, err := os.Stat(fixture + "/maindb/index.db"); err == nil {
		return fixture + "/maindb"
	}
	b.Fatalf("fixture %q is neither a maindb nor an application.db directory", fixture)
	return ""
}

func benchmarkLeafGenerationPlanFixture(b *testing.B) string {
	b.Helper()
	return benchmarkLeafGenerationPlanFixtureDir(b, "TREEDB_LEAFGEN_PLAN_FIXTURE")
}

func benchmarkLeafGenerationPlanWorkFixture(b *testing.B) string {
	b.Helper()
	return benchmarkLeafGenerationPlanFixtureDir(b, "TREEDB_LEAFGEN_PLAN_WORK_FIXTURE")
}

func openLeafGenerationPlanDB(b *testing.B, fixture string, readOnly bool) *DB {
	b.Helper()
	db, err := Open(Options{
		Dir:                        fixture,
		ReadOnly:                   readOnly,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		b.Fatalf("open fixture: %v", err)
	}
	return db
}

func openLeafGenerationPlanFixtureDB(b *testing.B) *DB {
	b.Helper()
	db := openLeafGenerationPlanDB(b, benchmarkLeafGenerationPlanFixture(b), true)
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func benchmarkCloseNoErr(b *testing.B, c interface{ Close() error }) {
	b.Helper()
	if err := c.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
}

func benchmarkLeafGenerationStatInt64(b *testing.B, stats map[string]string, key string) int64 {
	b.Helper()
	raw, ok := stats[key]
	if !ok {
		b.Fatalf("missing stats key %q", key)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		b.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return v
}

func openLeafGenerationPlanRootChurnBenchDB(b *testing.B) (*DB, *rewriteWriter) {
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
	b.Cleanup(func() { benchmarkCloseNoErr(b, leafLog) })
	b.Cleanup(func() { benchmarkCloseNoErr(b, db) })
	return db, leafLog
}

func writeLeafGenerationBenchKeyRange(b *testing.B, db *DB, prefix string, start, count int, fill byte) {
	b.Helper()
	raw := db.NewBatch()
	batch, ok := raw.(*Batch)
	if !ok {
		_ = raw.Close()
		b.Fatalf("NewBatch type=%T, want *Batch", raw)
	}
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("%s-%04d", prefix, start+i))
		value := bytes.Repeat([]byte{fill}, 32)
		if err := batch.Set(key, value); err != nil {
			_ = batch.Close()
			b.Fatalf("Set(%q): %v", key, err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		b.Fatalf("WriteSync: %v", err)
	}
	benchmarkCloseNoErr(b, batch)
}

func withLeafGenerationSubtreeCacheMissCounterB(b *testing.B) *atomic.Uint64 {
	b.Helper()
	var counter atomic.Uint64
	unregister := registerLeafGenerationSubtreeCacheMissHook(func(uint64) {
		counter.Add(1)
	})
	b.Cleanup(unregister)
	return &counter
}

func BenchmarkLeafGenerationPlan_SavedHome(b *testing.B) {
	db := openLeafGenerationPlanFixtureDB(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.leafGenerationLiveStatsMu.Lock()
		db.leafGenerationLiveStatsCache = leafGenerationLiveStatsCache{}
		db.leafGenerationLiveStatsMu.Unlock()
		if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
			b.Fatalf("LeafGenerationPlan: %v", err)
		}
	}
}

func BenchmarkLeafGenerationPlanPersistedIndexReopen_SavedHome(b *testing.B) {
	fixture := benchmarkLeafGenerationPlanWorkFixture(b)
	materialize := openLeafGenerationPlanDB(b, fixture, false)
	if _, err := materialize.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		_ = materialize.Close()
		b.Fatalf("materialize LeafGenerationPlan: %v", err)
	}
	if err := materialize.Close(); err != nil {
		b.Fatalf("close materialize db: %v", err)
	}

	db := openLeafGenerationPlanDB(b, fixture, true)
	b.Cleanup(func() { _ = db.Close() })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.leafGenerationLiveStatsMu.Lock()
		db.leafGenerationLiveStatsCache = leafGenerationLiveStatsCache{}
		db.leafGenerationLiveStatsMu.Unlock()
		db.leafGenerationRecordLengthMu.Lock()
		db.leafGenerationRecordLengthByFile = nil
		db.leafGenerationRecordLengthMu.Unlock()
		if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
			b.Fatalf("LeafGenerationPlan persisted reopen: %v", err)
		}
	}
}

func BenchmarkLeafGenerationLiveStatsPersistedIndex_SavedHome(b *testing.B) {
	fixture := benchmarkLeafGenerationPlanWorkFixture(b)
	materialize := openLeafGenerationPlanDB(b, fixture, false)
	if _, err := materialize.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		_ = materialize.Close()
		b.Fatalf("materialize LeafGenerationPlan: %v", err)
	}
	if err := materialize.Close(); err != nil {
		b.Fatalf("close materialize db: %v", err)
	}

	db := openLeafGenerationPlanDB(b, fixture, true)
	b.Cleanup(func() { _ = db.Close() })

	db.writeMu.RLock()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		b.Fatal(ErrClosed)
	}
	b.Cleanup(func() { _ = snap.Close() })
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.leafGenerationRecordLengthMu.Lock()
		db.leafGenerationRecordLengthByFile = nil
		db.leafGenerationRecordLengthMu.Unlock()
		if _, err := db.scanLeafGenerationLiveStats(context.Background(), snap); err != nil {
			b.Fatalf("scanLeafGenerationLiveStats: %v", err)
		}
	}
}

func BenchmarkLeafGenerationLiveStatsVerifiedPagesPersistedIndex_SavedHome(b *testing.B) {
	fixture := benchmarkLeafGenerationPlanWorkFixture(b)
	materialize := openLeafGenerationPlanDB(b, fixture, false)
	if _, err := materialize.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		_ = materialize.Close()
		b.Fatalf("materialize LeafGenerationPlan: %v", err)
	}
	if err := materialize.Close(); err != nil {
		b.Fatalf("close materialize db: %v", err)
	}

	db := openLeafGenerationPlanDB(b, fixture, true)
	b.Cleanup(func() { _ = db.Close() })

	db.writeMu.RLock()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		b.Fatal(ErrClosed)
	}
	b.Cleanup(func() { _ = snap.Close() })
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	if _, err := db.scanLeafGenerationLiveStats(context.Background(), snap); err != nil {
		b.Fatalf("warm scanLeafGenerationLiveStats: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.leafGenerationRecordLengthMu.Lock()
		db.leafGenerationRecordLengthByFile = nil
		db.leafGenerationRecordLengthMu.Unlock()
		if _, err := db.scanLeafGenerationLiveStats(context.Background(), snap); err != nil {
			b.Fatalf("scanLeafGenerationLiveStats verified pages: %v", err)
		}
	}
}

func BenchmarkLeafGenerationLiveStatsWarmIndexesVerifiedPages_SavedHome(b *testing.B) {
	fixture := benchmarkLeafGenerationPlanWorkFixture(b)
	materialize := openLeafGenerationPlanDB(b, fixture, false)
	if _, err := materialize.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		_ = materialize.Close()
		b.Fatalf("materialize LeafGenerationPlan: %v", err)
	}
	if err := materialize.Close(); err != nil {
		b.Fatalf("close materialize db: %v", err)
	}

	db := openLeafGenerationPlanDB(b, fixture, true)
	b.Cleanup(func() { _ = db.Close() })

	db.writeMu.RLock()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		b.Fatal(ErrClosed)
	}
	b.Cleanup(func() { _ = snap.Close() })
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	if _, err := db.scanLeafGenerationLiveStats(context.Background(), snap); err != nil {
		b.Fatalf("warm scanLeafGenerationLiveStats: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.scanLeafGenerationLiveStats(context.Background(), snap); err != nil {
			b.Fatalf("scanLeafGenerationLiveStats warm indexes: %v", err)
		}
	}
}

func BenchmarkLeafGenerationPlanCached_SavedHome(b *testing.B) {

	db := openLeafGenerationPlanFixtureDB(b)

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		b.Fatalf("warmup LeafGenerationPlan: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
			b.Fatalf("LeafGenerationPlan cached: %v", err)
		}
	}
}

func BenchmarkLeafGenerationPlanAfterSingleKeyRootChange_Local(b *testing.B) {
	db, _ := openLeafGenerationPlanRootChurnBenchDB(b)
	writeLeafGenerationBenchKeyRange(b, db, "k", 0, 4096, 'a')
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		b.Fatalf("warmup LeafGenerationPlan: %v", err)
	}

	cacheMisses := withLeafGenerationSubtreeCacheMissCounterB(b)
	var totalMisses uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fill := byte('b' + byte(i%23))
		b.StopTimer()
		writeLeafGenerationBenchKeyRange(b, db, "k", 0, 1, fill)
		beforeMisses := cacheMisses.Load()
		b.StartTimer()
		if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
			b.Fatalf("LeafGenerationPlan root churn: %v", err)
		}
		totalMisses += cacheMisses.Load() - beforeMisses
	}
	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(totalMisses)/float64(b.N), "subtree_misses/op")
	}
	stats := db.Stats()
	b.ReportMetric(float64(benchmarkLeafGenerationStatInt64(b, stats, "treedb.leaf_generation.plan_cache.subtree_pages")), "subtree_pages")
}

func BenchmarkLeafGenerationPlanAfterSingleKeyRootChange_SavedHome(b *testing.B) {
	fixture := benchmarkLeafGenerationPlanWorkFixture(b)
	db := openLeafGenerationPlanDB(b, fixture, false)
	b.Cleanup(func() { _ = db.Close() })
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil {
		b.Fatal("missing value-log set")
	}
	leafStartSeq := maxRewriteLaneSeqFromSet(set, rewriteLeafLogLaneID)
	_ = db.valueLogManager.Release(set)
	leafLog := newRewriteWriter(ValueLogDirPath(fixture), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(fixture), rewriteLeafLogLaneID, leafStartSeq)
	db.SetLeafPageLog(leafLog)
	b.Cleanup(func() { benchmarkCloseNoErr(b, leafLog) })
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		b.Fatalf("warmup LeafGenerationPlan: %v", err)
	}

	key := []byte("~leafgen-bench-root-churn")
	value := []byte{0}
	if err := db.Set(key, value); err != nil {
		b.Fatalf("seed Set: %v", err)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		b.Fatalf("warmup LeafGenerationPlan after seed Set: %v", err)
	}

	cacheMisses := withLeafGenerationSubtreeCacheMissCounterB(b)
	var totalMisses uint64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		value = []byte{byte(i)}
		if err := db.Set(key, value); err != nil {
			b.Fatalf("Set: %v", err)
		}
		beforeMisses := cacheMisses.Load()
		b.StartTimer()
		if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
			b.Fatalf("LeafGenerationPlan saved-home root churn: %v", err)
		}
		totalMisses += cacheMisses.Load() - beforeMisses
	}
	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(totalMisses)/float64(b.N), "subtree_misses/op")
	}
	stats := db.Stats()
	b.ReportMetric(float64(benchmarkLeafGenerationStatInt64(b, stats, "treedb.leaf_generation.plan_cache.subtree_pages")), "subtree_pages")
}

func BenchmarkLeafGenerationPlanLeafRefStats_SavedHome(b *testing.B) {
	db := openLeafGenerationPlanFixtureDB(b)
	ctx := context.Background()
	var totalPtrs, uniquePtrs, duplicatePtrs int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.writeMu.RLock()
		snap := db.AcquireSnapshot()
		db.writeMu.RUnlock()
		if snap == nil {
			b.Fatal(ErrClosed)
		}
		if len(snap.leafGenerationIDs) > 0 {
			snap.releaseLeafGenerationPins()
		}

		seen := make(map[page.LeafLogPtr]struct{}, 1024)
		totalPtrs, uniquePtrs, duplicatePtrs = 0, 0, 0
		visit := func(ptr page.LeafLogPtr) error {
			totalPtrs++
			if _, ok := seen[ptr]; ok {
				duplicatePtrs++
				return nil
			}
			seen[ptr] = struct{}{}
			uniquePtrs++
			return nil
		}
		for _, rootID := range []uint64{snap.state.RootPageID, snap.state.SystemRootPageID} {
			if rootID == 0 {
				continue
			}
			if err := leafrefscan.Walk(ctx, rootID, snap.idx.pager.Get, nil, visit); err != nil {
				_ = snap.Close()
				b.Fatalf("leafref walk: %v", err)
			}
		}
		if err := snap.Close(); err != nil {
			b.Fatalf("close snapshot: %v", err)
		}
	}
	b.ReportMetric(float64(totalPtrs), "total_leafrefs")
	b.ReportMetric(float64(uniquePtrs), "unique_leafrefs")
	b.ReportMetric(float64(duplicatePtrs), "duplicate_leafrefs")
}

func BenchmarkLeafGenerationPlanLeafRefPageStats_SavedHome(b *testing.B) {
	db := openLeafGenerationPlanFixtureDB(b)
	var totalPages, uniquePages, duplicatePages, leafRefs int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.writeMu.RLock()
		snap := db.AcquireSnapshot()
		db.writeMu.RUnlock()
		if snap == nil {
			b.Fatal(ErrClosed)
		}
		if len(snap.leafGenerationIDs) > 0 {
			snap.releaseLeafGenerationPins()
		}

		seenPages := make(map[uint64]struct{}, 1024)
		stack := make([]uint64, 0, 128)
		totalPages, uniquePages, duplicatePages, leafRefs = 0, 0, 0, 0
		for _, rootID := range []uint64{snap.state.RootPageID, snap.state.SystemRootPageID} {
			if rootID == 0 {
				continue
			}
			stack = append(stack, rootID)
		}
		for len(stack) > 0 {
			pageID := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			totalPages++
			if _, ok := seenPages[pageID]; ok {
				duplicatePages++
				continue
			}
			seenPages[pageID] = struct{}{}
			uniquePages++
			data, err := snap.idx.pager.Get(pageID)
			if err != nil {
				_ = snap.Close()
				b.Fatalf("pager.Get(%d): %v", pageID, err)
			}
			n := node.NewNodeView(data)
			switch n.Type() {
			case page.PageTypeLeaf:
				continue
			case page.PageTypeInternal:
				count := n.Count()
				for j := uint16(0); j < count; j++ {
					childRef, err := n.GetInternalChildRef(j)
					if err != nil {
						_ = snap.Close()
						b.Fatalf("GetInternalChildRef(%d): %v", j, err)
					}
					if childRef.Kind == page.ChildRefLeafLog {
						leafRefs++
						continue
					}
					stack = append(stack, childRef.Page)
				}
			default:
				_ = snap.Close()
				b.Fatalf("invalid page type %d on %d", n.Type(), pageID)
			}
		}
		if err := snap.Close(); err != nil {
			b.Fatalf("close snapshot: %v", err)
		}
	}
	b.ReportMetric(float64(totalPages), "total_pages")
	b.ReportMetric(float64(uniquePages), "unique_pages")
	b.ReportMetric(float64(duplicatePages), "duplicate_pages")
	b.ReportMetric(float64(leafRefs), "leafrefs")
}
