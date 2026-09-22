package db

import (
	"errors"
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

func TestFlushApplySpanNativeUnsupportedFallbackReasonCounters(t *testing.T) {
	base := zipper.ReadOnlyPrepareResult{
		Ops:            4,
		PointOps:       4,
		ExactLeafSpans: true,
		LeafSpans:      make([]zipper.ReadOnlyLeafSpan, 2),
	}
	cases := []struct {
		name   string
		mutate func(*zipper.ReadOnlyPrepareResult)
		want   FlushSpanRunFallbackReason
	}{
		{
			name: "range delete barrier",
			mutate: func(r *zipper.ReadOnlyPrepareResult) {
				r.DeleteRanges = 1
				r.Ops = r.PointOps + r.DeleteRanges
				r.ExactLeafSpans = false
			},
			want: FlushSpanRunFallbackRangeDeleteBarrier,
		},
		{
			name: "maintenance rewrite",
			mutate: func(r *zipper.ReadOnlyPrepareResult) {
				r.Maintenance = true
				r.ExactLeafSpans = false
			},
			want: FlushSpanRunFallbackMaintenance,
		},
		{
			name: "inexact leaf spans",
			mutate: func(r *zipper.ReadOnlyPrepareResult) {
				r.ExactLeafSpans = false
			},
			want: FlushSpanRunFallbackInexactLeafSpans,
		},
		{
			name: "cold build",
			mutate: func(r *zipper.ReadOnlyPrepareResult) {
				r.ColdBuild = true
			},
			want: FlushSpanRunFallbackColdBuild,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prepared := base
			prepared.LeafSpans = append([]zipper.ReadOnlyLeafSpan(nil), base.LeafSpans...)
			tc.mutate(&prepared)
			db := &DB{flushApplySpanNative: true}
			db.observeFlushApplyPrepareResult(zipper.ApplyResult{ReadOnlyPrepareRequested: true, ReadOnlyPrepare: prepared}, nil)
			if got := db.flushApplySpanNativeFallbackOps[tc.want].Load(); got != uint64(prepared.Ops) {
				t.Fatalf("fallback ops for %s=%d want %d", tc.want, got, prepared.Ops)
			}
		})
	}
}

func TestFlushApplySpanNativeExplicitFallbackReasonCounters(t *testing.T) {
	summary := zipper.ReadOnlyPrepareResult{
		Ops:            8,
		PointOps:       8,
		ExactLeafSpans: true,
		LeafSpans:      make([]zipper.ReadOnlyLeafSpan, 2),
	}
	cases := []struct {
		name   string
		reason FlushSpanRunFallbackReason
		result zipper.ApplyResult
	}{
		{
			name:   "forced memory cap before output",
			reason: FlushSpanRunFallbackMemoryEmergencyCap,
			result: zipper.ApplyResult{ReadOnlyPrepareRequested: true, ReadOnlyPrepare: summary, SpanNativeFallbackReason: FlushSpanRunFallbackMemoryEmergencyCap.String()},
		},
		{
			name:   "close checkpoint drain override before output",
			reason: FlushSpanRunFallbackCloseOrCheckpoint,
			result: zipper.ApplyResult{ReadOnlyPrepareRequested: true, ReadOnlyPrepare: summary, SpanNativeFallbackReason: FlushSpanRunFallbackCloseOrCheckpoint.String()},
		},
		{
			name:   "reducer validation after candidate",
			reason: FlushSpanRunFallbackReducerValidationFailed,
			result: zipper.ApplyResult{ReadOnlyPrepareRequested: true, ReadOnlyPrepare: summary, SpanNativeEligible: true, SpanNativeFallbackReason: FlushSpanRunFallbackReducerValidationFailed.String()},
		},
		{
			name:   "output ownership after candidate",
			reason: FlushSpanRunFallbackOutputOwnershipFailure,
			result: zipper.ApplyResult{ReadOnlyPrepareRequested: true, ReadOnlyPrepare: summary, SpanNativeEligible: true, SpanNativeFallbackReason: FlushSpanRunFallbackOutputOwnershipFailure.String()},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{flushApplySpanNative: true}
			db.observeFlushApplyPrepareResult(tc.result, nil)
			if got := db.flushApplySpanNativeFallbackOps[tc.reason].Load(); got != 8 {
				t.Fatalf("fallback ops for %s=%d want 8", tc.reason, got)
			}
			if got := db.flushApplySpanNativeFallbackSpans[tc.reason].Load(); got != 2 {
				t.Fatalf("fallback spans for %s=%d want 2", tc.reason, got)
			}
			if got := db.flushApplySpanNativeFallbacks.Load(); got != 1 {
				t.Fatalf("fallbacks=%d want 1", got)
			}
		})
	}
}

func TestFlushApplySpanNativeValidationFailureCounter(t *testing.T) {
	db := &DB{flushApplySpanNative: true}
	result := zipper.ApplyResult{
		ReadOnlyPrepareRequested:        true,
		ReadOnlyPrepareValidationFailed: true,
		ReadOnlyPrepare: zipper.ReadOnlyPrepareResult{
			Ops:            3,
			PointOps:       3,
			ExactLeafSpans: true,
			LeafSpans:      make([]zipper.ReadOnlyLeafSpan, 1),
		},
	}
	db.observeFlushApplyPrepareResult(result, errors.New("invalid plan"))
	if got := db.flushApplySpanNativeFallbackOps[FlushSpanRunFallbackValidationFailed].Load(); got != 3 {
		t.Fatalf("validation fallback ops=%d want 3", got)
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

func TestFlushApplySpanNativeReducerValidationGuardForcesFutureFallback(t *testing.T) {
	db := &DB{flushApplyConcurrency: 4, flushApplySpanNative: true}
	prepared := zipper.ReadOnlyPrepareResult{
		Ops:            8,
		PointOps:       8,
		ExactLeafSpans: true,
		LeafSpans:      make([]zipper.ReadOnlyLeafSpan, 2),
	}
	failed := zipper.ApplyResult{
		ReadOnlyPrepareRequested: true,
		ReadOnlyPrepare:          prepared,
		SpanNativeEligible:       true,
		SpanNativeFallbackReason: FlushSpanRunFallbackReducerValidationFailed.String(),
	}
	db.observeFlushApplyPrepareResult(failed, zipper.ErrSpanNativeReducerValidation)
	if !db.flushApplySpanNativeReducerValidationGuard.Load() {
		t.Fatalf("reducer validation guard inactive after reducer validation failure")
	}
	if got := db.flushApplySpanNativeReducerValidationGuardTrips.Load(); got != 1 {
		t.Fatalf("guard trips=%d want 1", got)
	}

	opts := db.flushApplyOptions()
	if got := opts.SpanNativeForceFallbackReason; got != FlushSpanRunFallbackReducerValidationGuard.String() {
		t.Fatalf("forced fallback=%q want %q", got, FlushSpanRunFallbackReducerValidationGuard.String())
	}

	guarded := zipper.ApplyResult{
		ReadOnlyPrepareRequested: true,
		ReadOnlyPrepare:          prepared,
		SpanNativeFallbackReason: opts.SpanNativeForceFallbackReason,
	}
	db.observeFlushApplyPrepareResult(guarded, nil)
	if got := db.flushApplySpanNativeReducerValidationGuardTrips.Load(); got != 1 {
		t.Fatalf("guard trips after forced fallback=%d want 1", got)
	}
	if got := db.flushApplySpanNativeFallbackOps[FlushSpanRunFallbackReducerValidationFailed].Load(); got != 8 {
		t.Fatalf("reducer validation failed fallback ops=%d want 8", got)
	}
	if got := db.flushApplySpanNativeFallbackOps[FlushSpanRunFallbackReducerValidationGuard].Load(); got != 8 {
		t.Fatalf("reducer validation guard fallback ops=%d want 8", got)
	}
}
