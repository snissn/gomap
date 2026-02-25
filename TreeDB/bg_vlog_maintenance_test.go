package treedb

import (
	"testing"
	"time"
)

func TestComputeVlogRewriteScore(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		totalBytes   int64
		staleBytes   int64
		targetTotal  int64
		targetStale  int64
		expectMinVal float64
		expectMaxVal float64
	}{
		{
			name:         "empty_total",
			totalBytes:   0,
			staleBytes:   0,
			targetTotal:  128 << 20,
			targetStale:  16 << 20,
			expectMinVal: 0,
			expectMaxVal: 0,
		},
		{
			name:         "stale_dominates",
			totalBytes:   128 << 20,
			staleBytes:   24 << 20,
			targetTotal:  256 << 20,
			targetStale:  16 << 20,
			expectMinVal: 1.49,
			expectMaxVal: 1.51,
		},
		{
			name:         "total_dominates",
			totalBytes:   256 << 20,
			staleBytes:   8 << 20,
			targetTotal:  128 << 20,
			targetStale:  16 << 20,
			expectMinVal: 1.99,
			expectMaxVal: 2.01,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			score := computeVlogRewriteScore(tc.totalBytes, tc.staleBytes, 0, tc.targetTotal, tc.targetStale, 0)
			if score < tc.expectMinVal || score > tc.expectMaxVal {
				t.Fatalf("score=%f outside expected range [%f,%f]", score, tc.expectMinVal, tc.expectMaxVal)
			}
		})
	}
}

func TestShouldBypassRewriteCooldown(t *testing.T) {
	t.Parallel()
	if shouldBypassRewriteCooldown(1.49, 1.5) {
		t.Fatalf("expected no bypass below threshold")
	}
	if !shouldBypassRewriteCooldown(1.5, 1.5) {
		t.Fatalf("expected bypass at threshold")
	}
	if shouldBypassRewriteCooldown(2.0, 0) {
		t.Fatalf("expected no bypass when threshold disabled")
	}
}

func TestEffectiveRewriteMaxSourceBytes(t *testing.T) {
	t.Parallel()
	got := effectiveRewriteMaxSourceBytes(64<<20, 8<<20, 5*time.Second)
	if want := int64(40 << 20); got != want {
		t.Fatalf("budget-capped max source bytes = %d, want %d", got, want)
	}
	got = effectiveRewriteMaxSourceBytes(16<<20, 8<<20, 5*time.Second)
	if want := int64(16 << 20); got != want {
		t.Fatalf("base max should win when tighter: got %d want %d", got, want)
	}
	got = effectiveRewriteMaxSourceBytes(0, 8<<20, 5*time.Second)
	if want := int64(40 << 20); got != want {
		t.Fatalf("budget should define max when base disabled: got %d want %d", got, want)
	}
}
