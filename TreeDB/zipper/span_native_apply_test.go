package zipper

import "testing"

func TestSpanNativeApplyEligibilityStrictPointSpans(t *testing.T) {
	summary := ReadOnlyLeafSpanSummary{
		Ops:            4,
		PointOps:       4,
		Spans:          2,
		SpanOps:        4,
		ExactLeafSpans: true,
	}
	workers, reason := spanNativeApplyFallbackReason(ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 8}, summary, nil, false)
	if reason != "" {
		t.Fatalf("reason=%q want eligible", reason)
	}
	if workers != 2 {
		t.Fatalf("workers=%d want bounded by spans=2", workers)
	}
}

func TestSpanNativeApplyEligibilityFallbackReasons(t *testing.T) {
	base := ReadOnlyLeafSpanSummary{Ops: 1, PointOps: 1, Spans: 1, SpanOps: 1, ExactLeafSpans: true}
	cases := []struct {
		name              string
		opts              ApplyOptions
		summary           ReadOnlyLeafSpanSummary
		validationFailure bool
		want              string
	}{
		{name: "disabled", opts: ApplyOptions{}, summary: base, want: "disabled"},
		{name: "validation", opts: ApplyOptions{SpanNativeApply: true}, summary: base, validationFailure: true, want: "validation_failed"},
		{name: "forced-memory", opts: ApplyOptions{SpanNativeApply: true, SpanNativeForceFallbackReason: "memory_or_emergency_cap"}, summary: base, want: "memory_or_emergency_cap"},
		{name: "forced-close", opts: ApplyOptions{SpanNativeApply: true, SpanNativeForceFallbackReason: "close_or_checkpoint"}, summary: base, want: "close_or_checkpoint"},
		{name: "cold", opts: ApplyOptions{SpanNativeApply: true}, summary: withSpanNativeSummary(base, func(s *ReadOnlyLeafSpanSummary) { s.ColdBuild = true }), want: "cold_build"},
		{name: "point-delete-maintenance-bit-default", opts: ApplyOptions{SpanNativeApply: true}, summary: withSpanNativeSummary(base, func(s *ReadOnlyLeafSpanSummary) { s.Maintenance = true }), want: "maintenance"},
		{name: "point-delete-maintenance-bit-ordered-root-opt-in", opts: ApplyOptions{SpanNativeApply: true, SpanNativeAllowMaintenancePointOps: true}, summary: withSpanNativeSummary(base, func(s *ReadOnlyLeafSpanSummary) { s.Maintenance = true }), want: ""},
		{name: "maintenance-opt-in-without-point-ops", opts: ApplyOptions{SpanNativeApply: true, SpanNativeAllowMaintenancePointOps: true}, summary: withSpanNativeSummary(base, func(s *ReadOnlyLeafSpanSummary) {
			s.Maintenance = true
			s.PointOps = 0
		}), want: "maintenance"},
		{name: "range", opts: ApplyOptions{SpanNativeApply: true}, summary: withSpanNativeSummary(base, func(s *ReadOnlyLeafSpanSummary) { s.DeleteRanges = 1 }), want: "range_delete_barrier"},
		{name: "inexact", opts: ApplyOptions{SpanNativeApply: true}, summary: withSpanNativeSummary(base, func(s *ReadOnlyLeafSpanSummary) { s.ExactLeafSpans = false }), want: "inexact_leaf_spans"},
		{name: "empty", opts: ApplyOptions{SpanNativeApply: true}, summary: withSpanNativeSummary(base, func(s *ReadOnlyLeafSpanSummary) { s.PointOps = 0 }), want: "below_threshold"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, reason := spanNativeApplyFallbackReason(tc.opts, tc.summary, nil, tc.validationFailure)
			if reason != tc.want {
				t.Fatalf("reason=%q want %q", reason, tc.want)
			}
		})
	}
}

func withSpanNativeSummary(in ReadOnlyLeafSpanSummary, mutate func(*ReadOnlyLeafSpanSummary)) ReadOnlyLeafSpanSummary {
	mutate(&in)
	return in
}
