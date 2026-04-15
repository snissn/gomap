package db

import (
	"context"
	"os"
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
		db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
		snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
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
			db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
			snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
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
			db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
			snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
		}

		seenPages := make(map[uint64]struct{}, 1024)
		stack := make([]uint64, 0, 128)
		totalPages, uniquePages, duplicatePages, leafRefs = 0, 0, 0, 0
		for _, rootID := range []uint64{snap.state.RootPageID, snap.state.SystemRootPageID} {
			if rootID == 0 {
				continue
			}
			if _, ok := page.DecodeLeafRef(rootID); ok {
				leafRefs++
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
					childID, err := n.GetInternalChildID(j)
					if err != nil {
						_ = snap.Close()
						b.Fatalf("GetInternalChildID(%d): %v", j, err)
					}
					if _, ok := page.DecodeLeafRef(childID); ok {
						leafRefs++
						continue
					}
					stack = append(stack, childID)
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
