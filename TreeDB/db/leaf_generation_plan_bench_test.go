package db

import (
	"context"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/page"
)

func benchmarkLeafGenerationPlanFixture(b *testing.B) string {
	b.Helper()
	fixture := os.Getenv("TREEDB_LEAFGEN_PLAN_FIXTURE")
	if fixture == "" {
		b.Skip("set TREEDB_LEAFGEN_PLAN_FIXTURE to a saved application.db or maindb fixture")
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

func openLeafGenerationPlanFixtureDB(b *testing.B) *DB {
	b.Helper()
	fixture := benchmarkLeafGenerationPlanFixture(b)
	db, err := Open(Options{
		Dir:                        fixture,
		ReadOnly:                   true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		b.Fatalf("open fixture: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkLeafGenerationPlan_SavedHome(b *testing.B) {
	db := openLeafGenerationPlanFixtureDB(b)

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		b.Fatalf("warmup LeafGenerationPlan: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
			b.Fatalf("LeafGenerationPlan: %v", err)
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
