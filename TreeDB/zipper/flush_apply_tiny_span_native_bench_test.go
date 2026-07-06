package zipper

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

func newTinySpanNativePointBatch(tb testing.TB) *batch.Batch {
	tb.Helper()
	delta := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	for _, idx := range []int{0, 512, 1024, 1536} {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte("new-value")); err != nil {
			_ = delta.Close()
			tb.Fatalf("Set delta: %v", err)
		}
	}
	delta.SortedEntries()
	return delta
}

func requireTinyPointReadOnlyPlan(tb testing.TB, prepared ReadOnlyPrepareResult, wantOps int) ReadOnlyLeafSpanSummary {
	tb.Helper()
	requireValidReadOnlyPrepare(tb, prepared)
	summary := prepared.LeafSpanSummary()
	if prepared.ColdBuild || prepared.Maintenance || !prepared.ExactLeafSpans {
		tb.Fatalf("prepare flags cold=%v maintenance=%v exact=%v want warm exact point plan", prepared.ColdBuild, prepared.Maintenance, prepared.ExactLeafSpans)
	}
	if summary.Ops != wantOps || summary.PointOps != wantOps || summary.DeleteRanges != 0 {
		tb.Fatalf("prepare ops/point/ranges=%d/%d/%d want %d/%d/0", summary.Ops, summary.PointOps, summary.DeleteRanges, wantOps, wantOps)
	}
	if summary.Spans != wantOps || summary.SingleOpSpans != wantOps || summary.SpanOps != wantOps {
		tb.Fatalf("prepare spans/single/spanOps=%d/%d/%d want %d/%d/%d", summary.Spans, summary.SingleOpSpans, summary.SpanOps, wantOps, wantOps, wantOps)
	}
	return summary
}

func sameTinyPointPlanningShape(a, b ReadOnlyLeafSpanSummary) bool {
	return a.Ops == b.Ops &&
		a.PointOps == b.PointOps &&
		a.DeleteRanges == b.DeleteRanges &&
		a.SpanOps == b.SpanOps &&
		a.SpanBytes == b.SpanBytes &&
		a.Spans == b.Spans &&
		a.ExactLeafSpans == b.ExactLeafSpans &&
		a.ColdBuild == b.ColdBuild &&
		a.Maintenance == b.Maintenance &&
		a.MinSpanOps == b.MinSpanOps &&
		a.MaxSpanOps == b.MaxSpanOps &&
		a.MinSpanBytes == b.MinSpanBytes &&
		a.MaxSpanBytes == b.MaxSpanBytes &&
		a.SingleOpSpans == b.SingleOpSpans
}

func TestSpanNativeApplyForcesReadOnlyPrepareKeyCopiesForTinyPointPlan(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 2048)
	delta := newTinySpanNativePointBatch(t)
	defer func() { _ = delta.Close() }()

	omitKeys, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{OmitKeys: true})
	if err != nil {
		t.Fatalf("PrepareReadOnly omit keys: %v", err)
	}
	omitSummary := requireTinyPointReadOnlyPlan(t, omitKeys, 4)
	if !omitKeys.OmitKeys {
		t.Fatalf("omit-keys prepare reported OmitKeys=false")
	}
	if len(omitKeys.keyArena) != 0 || cap(omitKeys.keyArena) != 0 {
		t.Fatalf("omit-keys key arena len/cap=%d/%d want 0/0", len(omitKeys.keyArena), cap(omitKeys.keyArena))
	}

	withKeys, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly with keys: %v", err)
	}
	withKeySummary := requireTinyPointReadOnlyPlan(t, withKeys, 4)
	if withKeys.OmitKeys {
		t.Fatalf("with-keys prepare reported OmitKeys=true")
	}
	if len(withKeys.keyArena) == 0 {
		t.Fatalf("with-keys prepare did not copy span/op keys into keyArena")
	}
	if !sameTinyPointPlanningShape(withKeySummary, omitSummary) {
		t.Fatalf("with-keys summary=%+v differs from omit-keys summary=%+v", withKeySummary, omitSummary)
	}

	applied, err := z.ApplyWithOptions(rootID, delta, ApplyOptions{
		SpanNativeApply:          true,
		ParallelApplyConcurrency: 4,
		ReadOnlyPrepare: ReadOnlyPrepareOptions{
			OmitKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	applySummary := requireTinyPointReadOnlyPlan(t, applied.ReadOnlyPrepare, 4)
	if !applied.ReadOnlyPrepareRequested || !applied.SpanNativeEligible {
		t.Fatalf("span-native prepare/eligible flags requested=%v eligible=%v fallback=%q", applied.ReadOnlyPrepareRequested, applied.SpanNativeEligible, applied.SpanNativeFallbackReason)
	}
	if applied.ReadOnlyPrepare.OmitKeys {
		t.Fatalf("span-native ApplyWithOptions kept OmitKeys=true; exact leaf-span execution must force keys")
	}
	if len(applied.ReadOnlyPrepare.keyArena) == 0 {
		t.Fatalf("span-native ApplyWithOptions did not retain copied key arena for exact spans")
	}
	if !sameTinyPointPlanningShape(applySummary, withKeySummary) {
		t.Fatalf("span-native summary=%+v differs from direct with-keys summary=%+v", applySummary, withKeySummary)
	}
}

func BenchmarkZipperTinySpanNativeReadOnlyPrepareKeyCopies(b *testing.B) {
	_, z := newReadOnlyPrepareZipper(b)
	rootID := buildReadOnlyPrepareRootWithKeys(b, z, 2048)
	delta := newTinySpanNativePointBatch(b)
	defer func() { _ = delta.Close() }()
	ops := delta.SortedEntries()

	firstWithKeys, err := z.PrepareReadOnlyPlan(rootID, ops, nil, ReadOnlyPrepareOptions{})
	if err != nil {
		b.Fatalf("initial PrepareReadOnlyPlan with keys: %v", err)
	}
	withKeySummary := requireTinyPointReadOnlyPlan(b, firstWithKeys, len(ops))
	if len(firstWithKeys.keyArena) == 0 {
		b.Fatalf("initial with-keys prepare did not copy key bytes")
	}

	firstOmitKeys, err := z.PrepareReadOnlyPlan(rootID, ops, nil, ReadOnlyPrepareOptions{OmitKeys: true})
	if err != nil {
		b.Fatalf("initial PrepareReadOnlyPlan omit keys: %v", err)
	}
	omitSummary := requireTinyPointReadOnlyPlan(b, firstOmitKeys, len(ops))
	if !firstOmitKeys.OmitKeys || len(firstOmitKeys.keyArena) != 0 || cap(firstOmitKeys.keyArena) != 0 {
		b.Fatalf("initial omit-keys result OmitKeys=%v keyArena len/cap=%d/%d", firstOmitKeys.OmitKeys, len(firstOmitKeys.keyArena), cap(firstOmitKeys.keyArena))
	}
	if !sameTinyPointPlanningShape(withKeySummary, omitSummary) {
		b.Fatalf("with-keys summary=%+v differs from omit-keys summary=%+v", withKeySummary, omitSummary)
	}

	cases := []struct {
		name  string
		opts  ReadOnlyPrepareOptions
		reuse bool
	}{
		{name: "with_keys_fresh"},
		{name: "omit_keys_fresh", opts: ReadOnlyPrepareOptions{OmitKeys: true}},
		{name: "with_keys_reuse", opts: firstWithKeys.ReuseOptions(), reuse: true},
		{name: "omit_keys_reuse", opts: firstOmitKeys.ReuseOptions(), reuse: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			opts := tc.opts
			var keyArenaBytes int64
			var last ReadOnlyPrepareResult
			b.ReportAllocs()
			b.SetBytes(int64(withKeySummary.SpanBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				prepared, err := z.PrepareReadOnlyPlan(rootID, ops, nil, opts)
				if err != nil {
					b.Fatalf("PrepareReadOnlyPlan: %v", err)
				}
				keyArenaBytes += int64(len(prepared.keyArena))
				last = prepared
				if tc.reuse && i+1 < b.N {
					opts = prepared.ReuseOptions()
				}
			}
			b.StopTimer()
			summary := requireTinyPointReadOnlyPlan(b, last, len(ops))
			b.ReportMetric(float64(summary.Spans), "leaf_spans/op")
			b.ReportMetric(float64(summary.SingleOpSpans), "single_op_spans/op")
			if b.N > 0 {
				b.ReportMetric(float64(keyArenaBytes)/float64(b.N), "key_arena_bytes/op")
			}
		})
	}
}
