package raftfsm

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

type RecoveryStatusOptionsV1 struct {
	SnapshotManifest *raftcluster.SnapshotManifestV1
	TailTargetIndex  uint64

	RequireReadSafety bool
	ReadBarrier       raftcluster.AppliedIndexReadBarrier
}

// RecoveryStatusV1 derives local recovery readiness from durable FSM state. It
// is a report-only status path: it does not install snapshots, replay tails,
// truncate logs, rejoin nodes, or prove production snapshot transfer.
func (f *FSM) RecoveryStatusV1(ctx context.Context, opts RecoveryStatusOptionsV1) (raftcluster.RecoveryStatusV1, error) {
	ctx = readBarrierContext(ctx)
	status := raftcluster.NewRecoveryStatusV1("", "")
	if f != nil {
		status = raftcluster.NewRecoveryStatusV1(f.cluster.NodeID, f.cluster.GroupID)
	}
	if err := ctx.Err(); err != nil {
		return status, err
	}
	if f == nil {
		return status, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return status, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is closed")
	}
	if f.db == nil || f.progress == nil || f.results == nil {
		return status, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	progress, err := f.appliedProgressLocked()
	if err != nil {
		return status, err
	}
	status.AppliedProgress = progress
	localLSN, err := localAppliedCommandLSN(f.db)
	if err != nil {
		return status, err
	}
	status.AppliedCommandLSN = localLSN
	status.HasAppliedCommandLSN = localLSN != 0
	status.TailTargetIndex = opts.TailTargetIndex

	if opts.SnapshotManifest == nil {
		status = applyRecoveryReadSafety(status, opts, progress)
		return finalizeRecoveryStatus(status), nil
	}

	manifest := *opts.SnapshotManifest
	status.SnapshotManifest = &manifest
	if opts.TailTargetIndex == 0 || opts.TailTargetIndex < manifest.LastIncludedIndex {
		opts.TailTargetIndex = manifest.LastIncludedIndex
	}
	status.TailTargetIndex = opts.TailTargetIndex

	if err := f.verifyRecoverySnapshotManifestV1Locked(manifest); err != nil {
		status.SnapshotState = raftcluster.RecoverySnapshotStateManifestRejectedV1
		status.TailState = raftcluster.RecoveryTailStateUnknownV1
		status.Errors = append(status.Errors, err.Error())
		status = applyRecoveryReadSafety(status, opts, progress)
		return finalizeRecoveryStatus(status), nil
	}

	status.SnapshotState = raftcluster.RecoverySnapshotStateManifestVerifiedV1
	if !progress.HasApplied {
		status.TailState = raftcluster.RecoveryTailStatePendingV1
		status.TailLagEntries = opts.TailTargetIndex
	} else if progress.Index < opts.TailTargetIndex {
		status.TailState = raftcluster.RecoveryTailStatePendingV1
		status.TailLagEntries = opts.TailTargetIndex - progress.Index
	} else {
		status.TailState = raftcluster.RecoveryTailStateCompleteV1
	}

	status = applyRecoveryReadSafety(status, opts, progress)
	return finalizeRecoveryStatus(status), nil
}

func (f *FSM) verifyRecoverySnapshotManifestV1Locked(manifest raftcluster.SnapshotManifestV1) error {
	if err := f.verifyInstalledSnapshotManifestV1Locked(manifest); err == nil {
		return nil
	}
	if err := manifest.Validate(f.snapshotScopeIdentityV1()); err != nil {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest is not valid for FSM scope: %v", err)
	}
	if manifest.GroupID != f.cluster.GroupID {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest group %q does not match FSM group %q", manifest.GroupID, f.cluster.GroupID)
	}
	record, ok, err := f.lastAppliedProgressRecord()
	if err != nil {
		return err
	}
	if !ok {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM has no durable applied progress for recovery status")
	}
	if record.EntryID.Index < manifest.LastIncludedIndex {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest last included index %d is ahead of durable progress %d", manifest.LastIncludedIndex, record.EntryID.Index)
	}
	if record.AppliedCommandLSN < manifest.AppliedCommandLSN {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "snapshot manifest AppliedCommandLSN %d is ahead of durable progress coverage %d", manifest.AppliedCommandLSN, record.AppliedCommandLSN)
	}
	localLSN, err := localAppliedCommandLSN(f.db)
	if err != nil {
		return err
	}
	if localLSN != record.AppliedCommandLSN {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "local AppliedCommandLSN coverage %d does not match durable progress coverage %d for recovery status", localLSN, record.AppliedCommandLSN)
	}
	if localLSN < manifest.AppliedCommandLSN {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "snapshot manifest AppliedCommandLSN %d is ahead of local coverage %d", manifest.AppliedCommandLSN, localLSN)
	}
	boundary, ok, err := f.progress.LookupApplyProgress(raftentry.ApplyEntryID{
		Term:  manifest.LastIncludedTerm,
		Index: manifest.LastIncludedIndex,
	})
	if err != nil {
		return err
	}
	if !ok {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing durable progress for snapshot boundary %d/%d", manifest.LastIncludedTerm, manifest.LastIncludedIndex)
	}
	if boundary.AppliedCommandLSN != manifest.AppliedCommandLSN {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest AppliedCommandLSN %d does not match boundary progress coverage %d", manifest.AppliedCommandLSN, boundary.AppliedCommandLSN)
	}
	if boundary.LogicalDigestV1 == (raftapply.LogicalDigestV1{}) {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing durable logical digest for snapshot boundary %d/%d", manifest.LastIncludedTerm, manifest.LastIncludedIndex)
	}
	if boundary.LogicalDigestV1.Hex() != manifest.LogicalDigestV1 {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest logical digest %s does not match boundary logical digest %s", manifest.LogicalDigestV1, boundary.LogicalDigestV1.Hex())
	}
	return nil
}

func applyRecoveryReadSafety(status raftcluster.RecoveryStatusV1, opts RecoveryStatusOptionsV1, progress raftcluster.AppliedProgress) raftcluster.RecoveryStatusV1 {
	if !opts.RequireReadSafety && opts.ReadBarrier == (raftcluster.AppliedIndexReadBarrier{}) {
		return status
	}
	barrier := opts.ReadBarrier
	if barrier.NodeID == "" {
		barrier.NodeID = status.NodeID
	}
	if barrier.GroupID == "" {
		barrier.GroupID = status.GroupID
	}
	if barrier.MinAppliedIndex == 0 {
		barrier.MinAppliedIndex = status.TailTargetIndex
	}
	status.RequiredAppliedIndex = barrier.MinAppliedIndex
	if err := barrier.Check(progress); err != nil {
		if errors.Is(err, raftcluster.ErrReadBarrierTargetMismatch) {
			status.ReadSafetyState = raftcluster.RecoveryReadSafetyTargetMismatchV1
		} else {
			status.ReadSafetyState = raftcluster.RecoveryReadSafetyAppliedIndexLaggingV1
		}
		status.Errors = append(status.Errors, err.Error())
		return status
	}
	status.ReadSafetyState = raftcluster.RecoveryReadSafetyAppliedIndexSatisfiedV1
	return status
}

func finalizeRecoveryStatus(status raftcluster.RecoveryStatusV1) raftcluster.RecoveryStatusV1 {
	status.SafeToServeReads = false
	switch {
	case status.Readiness == raftcluster.RecoveryReadinessUnsupportedV1:
		return status
	case status.SnapshotState == raftcluster.RecoverySnapshotStateNoneV1:
		status.Readiness = raftcluster.RecoveryReadinessUnsafeNoSnapshotV1
	case status.SnapshotState == raftcluster.RecoverySnapshotStateManifestRejectedV1:
		status.Readiness = raftcluster.RecoveryReadinessUnsafeManifestUnverifiedV1
	case status.TailState == raftcluster.RecoveryTailStatePendingV1:
		status.Readiness = raftcluster.RecoveryReadinessTailPendingV1
	case status.TailState != raftcluster.RecoveryTailStateCompleteV1:
		status.Readiness = raftcluster.RecoveryReadinessUnsafeManifestUnverifiedV1
	case status.ReadSafetyState == raftcluster.RecoveryReadSafetyAppliedIndexSatisfiedV1:
		status.Readiness = raftcluster.RecoveryReadinessReadyAppliedIndexV1
		status.SafeToServeReads = true
	case status.ReadSafetyState == raftcluster.RecoveryReadSafetyNotRequestedV1:
		status.Readiness = raftcluster.RecoveryReadinessTailCompleteV1
	default:
		status.Readiness = raftcluster.RecoveryReadinessReadSafetyPendingV1
	}
	return status
}
