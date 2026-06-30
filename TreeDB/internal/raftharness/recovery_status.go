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
	h.mu.Unlock()

	if opts.RequireReadSafety && opts.ReadBarrier.MinAppliedIndex == 0 {
		opts.ReadBarrier.MinAppliedIndex = opts.TailTargetIndex
	}
	return node.fsm.RecoveryStatusV1(ctx, raftfsm.RecoveryStatusOptionsV1{
		SnapshotManifest:  opts.SnapshotManifest,
		TailTargetIndex:   opts.TailTargetIndex,
		RequireReadSafety: opts.RequireReadSafety,
		ReadBarrier:       opts.ReadBarrier,
	})
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
