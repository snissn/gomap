package raftcluster

import (
	"errors"
	"fmt"
)

const (
	RecoveryStatusFormatV1 = "treedb.raftcluster.recovery-status"
	RecoveryStatusVersion1 = uint16(1)

	RecoveryMetricsFormatV1 = "treedb.raftcluster.recovery-metrics"
	RecoveryMetricsVersion1 = uint16(1)
)

var ErrRecoveryOperationUnsupported = errors.New("raftcluster: recovery operation unsupported")

type RecoveryReadinessV1 string

const (
	RecoveryReadinessUnsafeNoSnapshotV1         RecoveryReadinessV1 = "unsafe_no_snapshot"
	RecoveryReadinessUnsafeManifestUnverifiedV1 RecoveryReadinessV1 = "unsafe_manifest_unverified"
	RecoveryReadinessTailPendingV1              RecoveryReadinessV1 = "tail_pending"
	RecoveryReadinessTailCompleteV1             RecoveryReadinessV1 = "tail_complete"
	RecoveryReadinessReadSafetyPendingV1        RecoveryReadinessV1 = "read_safety_pending"
	RecoveryReadinessReadyAppliedIndexV1        RecoveryReadinessV1 = "ready_applied_index"
	RecoveryReadinessUnsupportedV1              RecoveryReadinessV1 = "unsupported"
)

type RecoverySnapshotStateV1 string

const (
	RecoverySnapshotStateNoneV1             RecoverySnapshotStateV1 = "no_snapshot"
	RecoverySnapshotStateManifestVerifiedV1 RecoverySnapshotStateV1 = "manifest_verified"
	RecoverySnapshotStateManifestRejectedV1 RecoverySnapshotStateV1 = "manifest_rejected"
)

type RecoveryTailStateV1 string

const (
	RecoveryTailStateNoSnapshotV1 RecoveryTailStateV1 = "no_snapshot"
	RecoveryTailStatePendingV1    RecoveryTailStateV1 = "pending"
	RecoveryTailStateCompleteV1   RecoveryTailStateV1 = "complete"
	RecoveryTailStateUnknownV1    RecoveryTailStateV1 = "unknown"
)

type RecoveryReadSafetyStateV1 string

const (
	RecoveryReadSafetyNotRequestedV1          RecoveryReadSafetyStateV1 = "not_requested"
	RecoveryReadSafetyAppliedIndexSatisfiedV1 RecoveryReadSafetyStateV1 = "applied_index_satisfied"
	RecoveryReadSafetyAppliedIndexLaggingV1   RecoveryReadSafetyStateV1 = "applied_index_lagging"
	RecoveryReadSafetyTargetMismatchV1        RecoveryReadSafetyStateV1 = "target_mismatch"
)

type RecoveryUnsupportedOperationV1 string

const (
	RecoveryUnsupportedLogTruncationV1              RecoveryUnsupportedOperationV1 = "log_truncation"
	RecoveryUnsupportedProductionRejoinV1           RecoveryUnsupportedOperationV1 = "production_rejoin"
	RecoveryUnsupportedProductionSnapshotTransferV1 RecoveryUnsupportedOperationV1 = "production_snapshot_transfer"
)

type RecoveryMetricKeyV1 string

const (
	RecoveryMetricSafeToServeReadsV1      RecoveryMetricKeyV1 = "treedb.raftcluster.recovery.safe_to_serve_reads"
	RecoveryMetricAppliedIndexV1          RecoveryMetricKeyV1 = "treedb.raftcluster.recovery.applied_index"
	RecoveryMetricRequiredAppliedIndexV1  RecoveryMetricKeyV1 = "treedb.raftcluster.recovery.required_applied_index"
	RecoveryMetricSnapshotIncludedIndexV1 RecoveryMetricKeyV1 = "treedb.raftcluster.recovery.snapshot_last_included_index"
	RecoveryMetricTailTargetIndexV1       RecoveryMetricKeyV1 = "treedb.raftcluster.recovery.tail_target_index"
	RecoveryMetricTailLagEntriesV1        RecoveryMetricKeyV1 = "treedb.raftcluster.recovery.tail_lag_entries"
	RecoveryMetricAppliedCommandLSNV1     RecoveryMetricKeyV1 = "treedb.raftcluster.recovery.applied_command_lsn"
)

// RecoveryStatusV1 is a report-only status contract. It describes whether a
// local node can prove snapshot/tail/read safety from durable local evidence; it
// does not install snapshots, truncate Raft logs, or rejoin replicas.
type RecoveryStatusV1 struct {
	Format  string `json:"format"`
	Version uint16 `json:"version"`

	NodeID  NodeID  `json:"node_id"`
	GroupID GroupID `json:"group_id"`

	Readiness        RecoveryReadinessV1              `json:"readiness"`
	SafeToServeReads bool                             `json:"safe_to_serve_reads"`
	SnapshotState    RecoverySnapshotStateV1          `json:"snapshot_state"`
	TailState        RecoveryTailStateV1              `json:"tail_state"`
	ReadSafetyState  RecoveryReadSafetyStateV1        `json:"read_safety_state"`
	Unsupported      []RecoveryUnsupportedOperationV1 `json:"unsupported,omitempty"`

	AppliedProgress      AppliedProgress     `json:"applied_progress"`
	AppliedCommandLSN    uint64              `json:"applied_command_lsn"`
	HasAppliedCommandLSN bool                `json:"has_applied_command_lsn"`
	SnapshotManifest     *SnapshotManifestV1 `json:"snapshot_manifest,omitempty"`
	RequiredAppliedIndex uint64              `json:"required_applied_index"`
	TailTargetIndex      uint64              `json:"tail_target_index"`
	TailLagEntries       uint64              `json:"tail_lag_entries"`
	Errors               []string            `json:"errors,omitempty"`
}

type RecoveryMetricSampleV1 struct {
	Key   RecoveryMetricKeyV1 `json:"key"`
	Value uint64              `json:"value"`
}

// RecoveryMetricsV1 freezes the metric keys and low-cardinality status labels
// exported by RecoveryStatusV1. Callers may map samples into their metrics
// backend, but should not derive new keys from error strings.
type RecoveryMetricsV1 struct {
	Format  string `json:"format"`
	Version uint16 `json:"version"`

	StatusLabel     RecoveryReadinessV1       `json:"status_label"`
	SnapshotLabel   RecoverySnapshotStateV1   `json:"snapshot_label"`
	TailLabel       RecoveryTailStateV1       `json:"tail_label"`
	ReadSafetyLabel RecoveryReadSafetyStateV1 `json:"read_safety_label"`

	Samples []RecoveryMetricSampleV1 `json:"samples"`
}

func NewRecoveryStatusV1(nodeID NodeID, groupID GroupID) RecoveryStatusV1 {
	return RecoveryStatusV1{
		Format:          RecoveryStatusFormatV1,
		Version:         RecoveryStatusVersion1,
		NodeID:          nodeID,
		GroupID:         groupID,
		Readiness:       RecoveryReadinessUnsafeNoSnapshotV1,
		SnapshotState:   RecoverySnapshotStateNoneV1,
		TailState:       RecoveryTailStateNoSnapshotV1,
		ReadSafetyState: RecoveryReadSafetyNotRequestedV1,
	}
}

func UnsupportedRecoveryStatusV1(nodeID NodeID, groupID GroupID, operation RecoveryUnsupportedOperationV1) RecoveryStatusV1 {
	status := NewRecoveryStatusV1(nodeID, groupID)
	status.Readiness = RecoveryReadinessUnsupportedV1
	status.Unsupported = []RecoveryUnsupportedOperationV1{operation}
	status.Errors = []string{fmt.Sprintf("%s: %s", ErrRecoveryOperationUnsupported.Error(), operation)}
	return status
}

func (s RecoveryStatusV1) MetricsV1() RecoveryMetricsV1 {
	appliedIndex := uint64(0)
	if s.AppliedProgress.HasApplied {
		appliedIndex = s.AppliedProgress.Index
	}
	snapshotIndex := uint64(0)
	if s.SnapshotManifest != nil {
		snapshotIndex = s.SnapshotManifest.LastIncludedIndex
	}
	safe := uint64(0)
	if s.SafeToServeReads {
		safe = 1
	}
	return RecoveryMetricsV1{
		Format:          RecoveryMetricsFormatV1,
		Version:         RecoveryMetricsVersion1,
		StatusLabel:     s.Readiness,
		SnapshotLabel:   s.SnapshotState,
		TailLabel:       s.TailState,
		ReadSafetyLabel: s.ReadSafetyState,
		Samples: []RecoveryMetricSampleV1{
			{Key: RecoveryMetricSafeToServeReadsV1, Value: safe},
			{Key: RecoveryMetricAppliedIndexV1, Value: appliedIndex},
			{Key: RecoveryMetricRequiredAppliedIndexV1, Value: s.RequiredAppliedIndex},
			{Key: RecoveryMetricSnapshotIncludedIndexV1, Value: snapshotIndex},
			{Key: RecoveryMetricTailTargetIndexV1, Value: s.TailTargetIndex},
			{Key: RecoveryMetricTailLagEntriesV1, Value: s.TailLagEntries},
			{Key: RecoveryMetricAppliedCommandLSNV1, Value: s.AppliedCommandLSN},
		},
	}
}
