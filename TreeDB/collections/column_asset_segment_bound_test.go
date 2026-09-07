package collections

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestColumnAssetReachabilitySegmentEntryBudget(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	db := openCollectionCommandWALDB(t, dir)
	defer db.Close()
	col := openColumnStoreCollectionM10B(t, db)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	ns, err := columnAssetManagerNamespaceForRoot(db.ColumnAssetRootDir(), col.Meta().Options.ColumnStore.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{columnAssetSegmentFileName(98), columnAssetSegmentFileName(99), "unknown-empty"} {
		if err := os.WriteFile(filepath.Join(ns.SegmentDir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(ns.SegmentDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1, -1, len(entries) - 1} {
		plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{MaxSegmentEntries: limit, SegmentDetails: true})
		if !errors.Is(err, ErrColumnAssetReachabilitySegmentLimit) || plan.Complete || !plan.ProtectOnly || len(plan.SegmentEntries) != 0 {
			t.Fatalf("limit%d: err=%v complete=%v entries=%d", limit, err, plan.Complete, len(plan.SegmentEntries))
		}
	}
	for _, limit := range []int{0, len(entries), len(entries) + 1} {
		plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{MaxSegmentEntries: limit, SegmentDetails: true})
		if err != nil || len(plan.SegmentEntries) != len(entries) {
			t.Fatalf("limit%d: err=%v entries=%d want%d", limit, err, len(plan.SegmentEntries), len(entries))
		}
		if plan.Complete { // Unknown entries still fail closed under an adequate budget.
			t.Fatal("budget bypassed unknown segment classification")
		}
	}
}

func TestColumnAssetSegmentDiscoveryBatchBoundary(t *testing.T) {
	dir := t.TempDir()
	for i := uint32(1); i <= 257; i++ {
		if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for name, list := range map[string]func(context.Context, string, int) ([]columnAssetReachabilitySegment, error){
		"native": listColumnAssetReachabilitySegmentsWithLimit,
		"legacy": listColumnAssetReachabilitySegmentsLegacy,
	} {
		t.Run(name, func(t *testing.T) {
			for _, limit := range []int{-1, 1, 255, 256, 257, 258, int(^uint(0) >> 1), 0} {
				segments, err := list(context.Background(), dir, limit)
				if limit != 0 && limit < 257 {
					if !errors.Is(err, ErrColumnAssetReachabilitySegmentLimit) || len(segments) != 0 {
						t.Fatalf("limit%d returned %d entries err=%v", limit, len(segments), err)
					}
				} else if err != nil || len(segments) != 257 || segments[0].fileID != 1 || segments[256].fileID != 257 {
					t.Fatalf("limit%d returned %d entries err=%v", limit, len(segments), err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if segments, err := list(ctx, dir, 257); !errors.Is(err, context.Canceled) || len(segments) != 0 {
				t.Fatalf("canceled discovery: entries%d err=%v", len(segments), err)
			}
		})
	}
}

func BenchmarkColumnAssetSegmentDiscovery(b *testing.B) {
	dir := b.TempDir()
	for i := uint32(1); i <= 257; i++ {
		if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(i)), nil, 0600); err != nil {
			b.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name  string
		limit int
	}{{"unbounded", 0}, {"bounded", 257}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				segments, err := listColumnAssetReachabilitySegmentsWithLimit(context.Background(), dir, tc.limit)
				if err != nil || len(segments) != 257 {
					b.Fatalf("entries%d err=%v", len(segments), err)
				}
			}
		})
	}
}
