package db

import (
	"context"
	"os"
	"testing"
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

func BenchmarkLeafGenerationPlan_SavedHome(b *testing.B) {
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
