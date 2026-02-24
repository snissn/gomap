package treedb

import "testing"

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
			score := computeVlogRewriteScore(tc.totalBytes, tc.staleBytes, tc.targetTotal, tc.targetStale)
			if score < tc.expectMinVal || score > tc.expectMaxVal {
				t.Fatalf("score=%f outside expected range [%f,%f]", score, tc.expectMinVal, tc.expectMaxVal)
			}
		})
	}
}
