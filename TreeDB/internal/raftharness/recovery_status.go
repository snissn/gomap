package raftharness

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

type RecoveryStatusOptionsV1 struct {
	SnapshotManifest *raftcluster.SnapshotManifestV1
	TailTargetIndex  uint64

	RequireReadSafety bool
	ReadBarrier       raftcluster.AppliedIndexReadBarrier
}

func (h *Harness) RecoveryStatusV1(ctx context.Context, nodeID raftcluster.NodeID, opts RecoveryStatusOptionsV1) (raftcluster.RecoveryStatusV1, error) {
	ctx = readBarrierContext(ctx)
	if err := ctx.Err(); err != nil {
		return raftcluster.NewRecoveryStatusV1(nodeID, ""), err
	}
	if h == nil {
		return raftcluster.NewRecoveryStatusV1(nodeID, ""), ErrHarnessClosed
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return raftcluster.NewRecoveryStatusV1(nodeID, h.groupID), ErrHarnessClosed
	}
	node, ok := h.nodes[nodeID]
	groupID := h.groupID
	if !ok {
		h.mu.Unlock()
		return raftcluster.NewRecoveryStatusV1(nodeID, groupID), fmt.Errorf("%w: %s", ErrNodeNotFound, nodeID)
	}
	if opts.TailTargetIndex == 0 && len(h.committed) > 0 {
		opts.TailTargetIndex = h.committed[len(h.committed)-1].Index
	}
	committed := make([]raftfsm.CommittedEntryV1, 0, len(h.committed))
	for _, entry := range h.committed {
		committed = append(committed, cloneCommittedEntry(entry))
	}
	h.mu.Unlock()

	if opts.RequireReadSafety && opts.ReadBarrier.MinAppliedIndex == 0 {
		opts.ReadBarrier.MinAppliedIndex = opts.TailTargetIndex
	}
	status, err := node.fsm.RecoveryStatusV1(ctx, raftfsm.RecoveryStatusOptionsV1{
		SnapshotManifest:  opts.SnapshotManifest,
		TailTargetIndex:   opts.TailTargetIndex,
		RequireReadSafety: opts.RequireReadSafety,
		ReadBarrier:       opts.ReadBarrier,
	})
	if err != nil {
		return status, err
	}
	if harnessRecoveryReadSafetyRequestedV1(opts) && status.SafeToServeReads {
		status = validateHarnessReadSafetyPrefixV1(status, node, committed)
	}
	return status, nil
}

func harnessRecoveryReadSafetyRequestedV1(opts RecoveryStatusOptionsV1) bool {
	return opts.RequireReadSafety || opts.ReadBarrier != (raftcluster.AppliedIndexReadBarrier{})
}

func (h *Harness) LogTruncationRecoveryStatusV1(nodeID raftcluster.NodeID) (raftcluster.RecoveryStatusV1, error) {
	return h.unsupportedRecoveryStatusV1(nodeID, raftcluster.RecoveryUnsupportedLogTruncationV1)
}

func (h *Harness) ProductionRejoinRecoveryStatusV1(nodeID raftcluster.NodeID) (raftcluster.RecoveryStatusV1, error) {
	return h.unsupportedRecoveryStatusV1(nodeID, raftcluster.RecoveryUnsupportedProductionRejoinV1)
}

func (h *Harness) ProductionSnapshotTransferRecoveryStatusV1(nodeID raftcluster.NodeID) (raftcluster.RecoveryStatusV1, error) {
	return h.unsupportedRecoveryStatusV1(nodeID, raftcluster.RecoveryUnsupportedProductionSnapshotTransferV1)
}

func (h *Harness) unsupportedRecoveryStatusV1(nodeID raftcluster.NodeID, op raftcluster.RecoveryUnsupportedOperationV1) (raftcluster.RecoveryStatusV1, error) {
	groupID := raftcluster.GroupID("")
	if h != nil {
		h.mu.Lock()
		groupID = h.groupID
		h.mu.Unlock()
	}
	status := raftcluster.UnsupportedRecoveryStatusV1(nodeID, groupID, op)
	return status, fmt.Errorf("%w: %s", raftcluster.ErrRecoveryOperationUnsupported, op)
}

func validateHarnessReadSafetyPrefixV1(status raftcluster.RecoveryStatusV1, node *Node, committed []raftfsm.CommittedEntryV1) raftcluster.RecoveryStatusV1 {
	progress := status.AppliedProgress
	if node == nil || node.fsm == nil {
		return failHarnessReadSafetyPrefixV1(status, fmt.Errorf("%w: closed node", ErrNodeClosed))
	}
	if !progress.HasApplied || progress.Index == 0 {
		return failHarnessReadSafetyPrefixV1(status, fmt.Errorf("%w: node %s has no applied progress", ErrCommittedLogConflict, status.NodeID))
	}
	prefix, ok := committedPrefixThroughIndex(committed, progress.Index)
	if !ok {
		return failHarnessReadSafetyPrefixV1(status, fmt.Errorf("%w: node %s applied index %d is not committed", ErrCommittedLogConflict, status.NodeID, progress.Index))
	}
	if _, err := node.fsm.ValidateAppliedPrefixV1(prefix); err != nil {
		return failHarnessReadSafetyPrefixV1(status, err)
	}
	return status
}

func failHarnessReadSafetyPrefixV1(status raftcluster.RecoveryStatusV1, err error) raftcluster.RecoveryStatusV1 {
	status.SafeToServeReads = false
	status.Readiness = raftcluster.RecoveryReadinessReadSafetyPendingV1
	status.ReadSafetyState = raftcluster.RecoveryReadSafetyTargetMismatchV1
	status.Errors = append(status.Errors, fmt.Sprintf("raftharness: committed-prefix read safety validation failed: %v", err))
	return status
}
