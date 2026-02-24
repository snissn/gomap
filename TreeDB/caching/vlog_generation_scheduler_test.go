package caching

import "testing"

func TestShouldRunVlogGenerationRewrite_TotalBytes(t *testing.T) {
	db := &DB{valueLogRewriteTriggerBytes: 100}
	run, reason := db.shouldRunVlogGenerationRewrite(150, 0, 0)
	if !run {
		t.Fatalf("expected rewrite to trigger on total bytes")
	}
	if reason != vlogGenerationReasonTotalBytes {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonTotalBytes)
	}
}

func TestShouldRunVlogGenerationRewrite_StaleRatio(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 250000}
	run, reason := db.shouldRunVlogGenerationRewrite(0, 300000, 0)
	if !run {
		t.Fatalf("expected rewrite to trigger on stale ratio")
	}
	if reason != vlogGenerationReasonStaleRatio {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonStaleRatio)
	}
}

func TestShouldRunVlogGenerationRewrite_Churn(t *testing.T) {
	db := &DB{valueLogRewriteTriggerChurn: 1 << 20}
	run, reason := db.shouldRunVlogGenerationRewrite(0, 0, 2<<20)
	if !run {
		t.Fatalf("expected rewrite to trigger on churn")
	}
	if reason != vlogGenerationReasonChurn {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonChurn)
	}
}

func TestShouldRunVlogGenerationRewrite_NoTrigger(t *testing.T) {
	db := &DB{
		valueLogRewriteTriggerBytes:    100,
		valueLogRewriteTriggerRatioPPM: 200000,
		valueLogRewriteTriggerChurn:    1000,
	}
	run, reason := db.shouldRunVlogGenerationRewrite(50, 100000, 900)
	if run {
		t.Fatalf("expected no rewrite trigger")
	}
	if reason != vlogGenerationReasonNone {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonNone)
	}
}

func TestShouldRunVlogGenerationGC_ReclaimableBytes(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: 1}
	run := db.shouldRunVlogGenerationGC(valueLogRetainedGenerationStats{}, vlogGenerationGCMinBytes, 0)
	if !run {
		t.Fatalf("expected gc to trigger on reclaimable bytes")
	}
}

func TestShouldRunVlogGenerationGC_HotSegmentPressure(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: 1}
	run := db.shouldRunVlogGenerationGC(valueLogRetainedGenerationStats{
		SegmentsTotal: 4,
		SegmentsHot:   2,
	}, 0, 0)
	if !run {
		t.Fatalf("expected gc to trigger on hot segment pressure")
	}
}

func TestShouldRunVlogGenerationGC_ChurnDriven(t *testing.T) {
	db := &DB{
		valueLogGenerationPolicy:    1,
		valueLogRewriteTriggerChurn: 1 << 20,
	}
	run := db.shouldRunVlogGenerationGC(valueLogRetainedGenerationStats{}, 0, 1<<19)
	if !run {
		t.Fatalf("expected gc to trigger on churn threshold")
	}
}
