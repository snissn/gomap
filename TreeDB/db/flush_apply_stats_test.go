package db

import (
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/zipper"
)

func requireFlushApplyStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return got
}

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

func TestFlushApplyStatsExposeLeafLogOutputLaneTaskDistribution(t *testing.T) {
	db := &DB{}
	metrics := adaptive.Metrics{}
	metrics.ZipperLeafLogOutputLaneTaskTotal = 5
	metrics.ZipperLeafLogOutputLaneTasks[1] = 2
	metrics.ZipperLeafLogOutputLaneTasks[2] = 3
	metrics.ZipperLeafLogOutputLaneTaskOverflow = 1
	db.observeFlushApplyMetrics(metrics, 0, nil)

	stats := map[string]string{}
	db.appendFlushApplyStats(stats)
	for key, want := range map[string]uint64{
		"treedb.flush_apply.leaf_log_output.lane.01.tasks_total":       2,
		"treedb.flush_apply.leaf_log_output.lane.02.tasks_total":       3,
		"treedb.flush_apply.leaf_log_output.lane.tasks_total":          6,
		"treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used":     2,
		"treedb.flush_apply.leaf_log_output.lane.tasks_max":            3,
		"treedb.flush_apply.leaf_log_output.lane.tasks_overflow_total": 1,
	} {
		if got := requireFlushApplyStatUint64(t, stats, key); got != want {
			t.Fatalf("%s=%d want %d", key, got, want)
		}
	}
}
