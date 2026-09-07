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
		stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{MaxSegmentEntries: limit})
		if !errors.Is(err, ErrColumnAssetReachabilitySegmentLimit) || stats.SegmentsDeleted != 0 {
			t.Fatalf("GC segment budget: err=%v deleted=%d", err, stats.SegmentsDeleted)
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

func TestColumnAssetReachabilityManifestBudget(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	db := openCollectionCommandWALDB(t, dir)
	defer db.Close()
	col := openColumnStoreCollectionM10B(t, db)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatal(err)
	}
	for _, limits := range []struct {
		records int
		bytes   int64
	}{{1, 1 << 20}, {4096, 1}, {-1, 1024}, {0, 1024}, {4096, 0}} {
		plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{MaxManifestRecords: limits.records, MaxManifestBytes: limits.bytes})
		if !errors.Is(err, ErrColumnAssetReachabilityManifestLimit) || plan.Complete || len(plan.SegmentEntries) != 0 {
			t.Fatalf("manifest limit %+v returned err=%v complete=%v", limits, err, plan.Complete)
		}
		stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{MaxManifestRecords: limits.records, MaxManifestBytes: limits.bytes})
		if !errors.Is(err, ErrColumnAssetReachabilityManifestLimit) || stats.SegmentsDeleted != 0 || stats.BytesDeleted != 0 {
			t.Fatalf("GC manifest limit %+v: err=%v deleted=%d/%d", limits, err, stats.SegmentsDeleted, stats.BytesDeleted)
		}
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{MaxManifestRecords: 4096, MaxManifestBytes: 1 << 20})
	if err != nil || !plan.Complete {
		t.Fatalf("adequate manifest budget: err=%v complete=%v", err, plan.Complete)
	}
	// Publication happens during the preflight's first context check, after
	// snapshot/catalog acquisition. Decoding must still use the admitted root.
	beforeSeq, _ := dbCommitSeqAndSystemRoot(db)
	ctx := &columnAssetDiscoveryCheckContext{Context: context.Background(), at: 4, check: func() {
		if _, err := col.Insert([]byte("e2"), []byte(`{"time_us":2,"kind":"like","did":"d2"}`)); err != nil {
			t.Fatal(err)
		}
	}}
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecarsAndBudget(ctx, columnManifestScanAllSidecars(), 4096, 1<<20)
	if closeView != nil {
		defer closeView()
	}
	afterSeq, _ := dbCommitSeqAndSystemRoot(db)
	if err != nil || ctx.calls < ctx.at || afterSeq <= beforeSeq || view.CommitSeq != beforeSeq || view.Diagnostics.ManifestGeneration != plan.ActiveManifestGeneration {
		t.Fatalf("snapshot changed between preflight/decode: err=%v checks=%d before=%d view=%d after=%d", err, ctx.calls, beforeSeq, view.CommitSeq, afterSeq)
	}
}

type columnAssetDiscoveryCheckContext struct {
	context.Context
	calls, at int
	check     func()
}

func (c *columnAssetDiscoveryCheckContext) Err() error {
	c.calls++
	if c.calls == c.at {
		c.check()
	}
	return c.Context.Err()
}

func TestColumnAssetReachabilityCapturedManifestBudget(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	if refs, err := col.typedGraphBaseReachabilityRefsWithBudget(context.Background(), 1, 1<<20); !errors.Is(err, ErrColumnAssetReachabilityManifestLimit) || len(refs) != 0 {
		t.Fatalf("captured manifest escaped input budget: refs=%d err=%v", len(refs), err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if refs, err := col.typedGraphBaseReachabilityRefsWithBudget(ctx, 4096, 1<<20); !errors.Is(err, context.Canceled) || len(refs) != 0 {
		t.Fatalf("captured preflight ignored cancellation: refs=%d err=%v", len(refs), err)
	}
	if refs, err := col.typedGraphBaseReachabilityRefsWithBudget(context.Background(), 4096, 1<<20); err != nil || len(refs) == 0 {
		t.Fatalf("adequate captured budget: refs=%d err=%v", len(refs), err)
	}
}

func BenchmarkColumnAssetReachabilityDiscoveryBudget(b *testing.B) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(b, 1024)
	for _, tc := range []struct {
		name string
		opts ColumnAssetReachabilityOptions
	}{
		{"default", ColumnAssetReachabilityOptions{}},
		{"bounded", ColumnAssetReachabilityOptions{MaxSegmentEntries: 4096, MaxManifestRecords: 4096, MaxManifestBytes: 4 << 20, MaxLifecycleEntries: 4096}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				plan, err := col.PlanColumnAssetReachability(context.Background(), tc.opts)
				if err != nil || !plan.Complete {
					b.Fatalf("complete=%v err=%v", plan.Complete, err)
				}
			}
		})
	}
}

func TestColumnAssetReachabilityLifecycleCopyBudget(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	refs, err := col.typedGraphBaseReachabilityRefsWithBudget(context.Background(), 4096, 4<<20)
	if err != nil || len(refs) < 2 {
		t.Fatalf("refs%d err=%v", len(refs), err)
	}
	pin, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "copy-budget", Refs: refs[:2]})
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	registry, err := col.RegisterColumnAssetPreparedAsset(ColumnAssetPreparedAssetRegistrationOptions{Owner: "copy-budget", Source: "test", Refs: refs[:2]})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	for _, limit := range []int{-1, 1, 2} {
		if entries, err := col.columnAssetLifecyclePinSetSnapshotWithLimit(limit); !errors.Is(err, ErrColumnAssetReachabilityLifecycleLimit) || len(entries) != 0 {
			t.Fatalf("pin limit%d: entries%d err=%v", limit, len(entries), err)
		}
		if entries, err := col.columnAssetLifecycleRegistrySnapshotWithLimit(limit); !errors.Is(err, ErrColumnAssetReachabilityLifecycleLimit) || len(entries) != 0 {
			t.Fatalf("registry limit%d: entries%d err=%v", limit, len(entries), err)
		}
		stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{MaxLifecycleEntries: limit})
		if !errors.Is(err, ErrColumnAssetReachabilityLifecycleLimit) || stats.SegmentsDeleted != 0 {
			t.Fatalf("GC limit%d: deleted%d err=%v", limit, stats.SegmentsDeleted, err)
		}
	}
	for _, limit := range []int{0, 65536} {
		pins, err := col.columnAssetLifecyclePinSetSnapshotWithLimit(limit)
		if err != nil || len(pins) == 0 {
			t.Fatalf("pins limit%d: %v", limit, err)
		}
		records, err := col.columnAssetLifecycleRegistrySnapshotWithLimit(limit)
		if err != nil || len(records) == 0 {
			t.Fatalf("records limit%d: %v", limit, err)
		}
		plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{MaxLifecycleEntries: limit})
		if err != nil || !plan.Complete {
			t.Fatalf("plan limit%d complete%v err=%v", limit, plan.Complete, err)
		}
	}
}
