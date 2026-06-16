package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/zipper"
)

func TestFlushApplySpanNativeEligibleCountersDoNotMarkIneligible(t *testing.T) {
	db := &DB{flushApplyConcurrency: 4, flushApplySpanNative: true}
	result := zipper.ApplyResult{
		ReadOnlyPrepareRequested: true,
		ReadOnlyPrepare: zipper.ReadOnlyPrepareResult{
			Ops:            4,
			PointOps:       4,
			ExactLeafSpans: true,
			LeafSpans:      make([]zipper.ReadOnlyLeafSpan, 2),
		},
		SpanNativeEligible: true,
		SpanNativeWorkers:  2,
	}
	// LeafSpanSummary derives span count from LeafSpans and op count from Ops.
	db.observeFlushApplyPrepareResult(result, nil)

	if got := db.flushApplySpanNativeCandidateOps.Load(); got != 4 {
		t.Fatalf("candidate ops=%d want 4", got)
	}
	if got := db.flushApplySpanNativeCandidateSpans.Load(); got != 2 {
		t.Fatalf("candidate spans=%d want 2", got)
	}
	if got := db.flushApplySpanNativeEligibleOps.Load(); got != 4 {
		t.Fatalf("eligible ops=%d want 4", got)
	}
	if got := db.flushApplySpanNativeEligibleSpans.Load(); got != 2 {
		t.Fatalf("eligible spans=%d want 2", got)
	}
	if got := db.flushApplySpanNativeIneligibleOps.Load(); got != 0 {
		t.Fatalf("ineligible ops=%d want 0", got)
	}
	if got := db.flushApplySpanNativeUsedOps.Load(); got != 0 {
		t.Fatalf("used ops=%d want 0 for eligible fallback scaffold", got)
	}
	if got := db.flushApplySpanNativeFallbackOps[FlushSpanRunFallbackSpanNativeNotImplemented].Load(); got != 4 {
		t.Fatalf("not-implemented fallback ops=%d want 4", got)
	}
}

func TestFlushApplySpanNativeUsedCounters(t *testing.T) {
	db := &DB{flushApplyConcurrency: 4, flushApplySpanNative: true}
	result := zipper.ApplyResult{
		ReadOnlyPrepareRequested: true,
		ReadOnlyPrepare: zipper.ReadOnlyPrepareResult{
			Ops:            4,
			PointOps:       4,
			ExactLeafSpans: true,
			LeafSpans:      make([]zipper.ReadOnlyLeafSpan, 2),
		},
		SpanNativeEligible: true,
		SpanNativeWorkers:  2,
		SpanNativeUsed:     true,
	}
	db.observeFlushApplyPrepareResult(result, nil)
	if got := db.flushApplySpanNativeUsedOps.Load(); got != 4 {
		t.Fatalf("used ops=%d want 4", got)
	}
	if got := db.flushApplySpanNativeUsedSpans.Load(); got != 2 {
		t.Fatalf("used spans=%d want 2", got)
	}
	if got := db.flushApplySpanNativeFallbacks.Load(); got != 0 {
		t.Fatalf("fallbacks=%d want 0", got)
	}
}

func TestFlushApplyOptionsEnableSpanNativePrepareWithoutParallelWorkers(t *testing.T) {
	db := &DB{flushApplySpanNative: true}
	opts := db.flushApplyOptions()
	if !opts.SpanNativeApply {
		t.Fatalf("SpanNativeApply=false want true")
	}
	if !opts.PrepareReadOnly {
		t.Fatalf("PrepareReadOnly=false want true")
	}
	if opts.ParallelApplyConcurrency != 0 {
		t.Fatalf("ParallelApplyConcurrency=%d want 0", opts.ParallelApplyConcurrency)
	}
}
