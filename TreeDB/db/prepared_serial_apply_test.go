package db

import "testing"

func TestPreparedOrderedRootApplyOptionsForceSerial(t *testing.T) {
	db := &DB{flushApplyConcurrency: 8, flushApplySpanNative: true}
	ordinary := db.orderedRootDeltaBatchApplyOptions(orderedRootPublishOptions{})
	if ordinary.ParallelApplyConcurrency != 8 || !ordinary.SpanNativeApply {
		t.Fatalf("ordinary apply options changed: %+v", ordinary)
	}
	prepared := db.orderedRootDeltaBatchApplyOptions(orderedRootPublishOptions{serialApply: true})
	if prepared.ParallelApplyConcurrency != 1 || prepared.SpanNativeApply || prepared.PrepareReadOnly || !flushApplyUseOptions(prepared) {
		t.Fatalf("prepared apply did not select serial ApplyWithOptions: %+v", prepared)
	}
}
