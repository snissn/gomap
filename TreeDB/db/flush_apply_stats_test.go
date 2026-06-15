package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/zipper"
)

func TestClassifyFlushApplySpanNativeFallbackPrioritizesRangeDeleteBarrier(t *testing.T) {
	summary := zipper.ReadOnlyLeafSpanSummary{
		Ops:            1,
		DeleteRanges:   1,
		ExactLeafSpans: false,
	}
	if got := classifyFlushApplySpanNativeFallback(summary, nil, false); got != FlushSpanRunFallbackRangeDeleteBarrier {
		t.Fatalf("fallback=%s want %s", got, FlushSpanRunFallbackRangeDeleteBarrier)
	}
}
