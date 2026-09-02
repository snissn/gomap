package raftcluster

import (
	"reflect"
	"strings"
	"testing"
)

func TestRecoveryStatusV1MetricsUseStableKeysAndLabels(t *testing.T) {
	manifest := validSnapshotManifestV1()
	status := NewRecoveryStatusV1("node-a", "group-a")
	status.Readiness = RecoveryReadinessReadyAppliedIndexV1
	status.SafeToServeReads = true
	status.SnapshotState = RecoverySnapshotStateManifestVerifiedV1
	status.TailState = RecoveryTailStateCompleteV1
	status.ReadSafetyState = RecoveryReadSafetyAppliedIndexSatisfiedV1
	status.AppliedProgress = AppliedProgress{
		NodeID:     "node-a",
		GroupID:    "group-a",
		Term:       7,
		Index:      12,
		HasApplied: true,
	}
	status.AppliedCommandLSN = 19
	status.HasAppliedCommandLSN = true
	status.SnapshotManifest = &manifest
	status.RequiredAppliedIndex = 12
	status.TailTargetIndex = 12

	metrics := status.MetricsV1()
	if metrics.Format != RecoveryMetricsFormatV1 || metrics.Version != RecoveryMetricsVersion1 {
		t.Fatalf("metrics contract=(%q,%d), want (%q,%d)", metrics.Format, metrics.Version, RecoveryMetricsFormatV1, RecoveryMetricsVersion1)
	}
	if metrics.StatusLabel != RecoveryReadinessReadyAppliedIndexV1 ||
		metrics.SnapshotLabel != RecoverySnapshotStateManifestVerifiedV1 ||
		metrics.TailLabel != RecoveryTailStateCompleteV1 ||
		metrics.ReadSafetyLabel != RecoveryReadSafetyAppliedIndexSatisfiedV1 {
		t.Fatalf("metric labels=%+v, want ready/verified/complete/applied-index", metrics)
	}
	gotKeys := make([]RecoveryMetricKeyV1, 0, len(metrics.Samples))
	for _, sample := range metrics.Samples {
		gotKeys = append(gotKeys, sample.Key)
	}
	wantKeys := []RecoveryMetricKeyV1{
		RecoveryMetricSafeToServeReadsV1,
		RecoveryMetricAppliedIndexV1,
		RecoveryMetricRequiredAppliedIndexV1,
		RecoveryMetricSnapshotIncludedIndexV1,
		RecoveryMetricTailTargetIndexV1,
		RecoveryMetricTailLagEntriesV1,
		RecoveryMetricAppliedCommandLSNV1,
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("metric keys=%+v, want %+v", gotKeys, wantKeys)
	}
	values := map[RecoveryMetricKeyV1]uint64{}
	for _, sample := range metrics.Samples {
		values[sample.Key] = sample.Value
	}
	if values[RecoveryMetricSafeToServeReadsV1] != 1 ||
		values[RecoveryMetricAppliedIndexV1] != 12 ||
		values[RecoveryMetricRequiredAppliedIndexV1] != 12 ||
		values[RecoveryMetricSnapshotIncludedIndexV1] != manifest.LastIncludedIndex ||
		values[RecoveryMetricTailTargetIndexV1] != 12 ||
		values[RecoveryMetricTailLagEntriesV1] != 0 ||
		values[RecoveryMetricAppliedCommandLSNV1] != 19 {
		t.Fatalf("metric values=%+v", values)
	}
}

func TestUnsupportedRecoveryStatusV1FailsClosed(t *testing.T) {
	status := UnsupportedRecoveryStatusV1("node-b", "group-a", RecoveryUnsupportedLogTruncationV1)
	if status.Format != RecoveryStatusFormatV1 || status.Version != RecoveryStatusVersion1 {
		t.Fatalf("status contract=(%q,%d), want (%q,%d)", status.Format, status.Version, RecoveryStatusFormatV1, RecoveryStatusVersion1)
	}
	if status.Readiness != RecoveryReadinessUnsupportedV1 || status.SafeToServeReads {
		t.Fatalf("status=%+v, want unsupported and unsafe", status)
	}
	if !reflect.DeepEqual(status.Unsupported, []RecoveryUnsupportedOperationV1{RecoveryUnsupportedLogTruncationV1}) {
		t.Fatalf("unsupported=%+v", status.Unsupported)
	}
	if len(status.Errors) != 1 || !strings.Contains(status.Errors[0], ErrRecoveryOperationUnsupported.Error()) {
		t.Fatalf("errors=%+v, want unsupported error", status.Errors)
	}
}
