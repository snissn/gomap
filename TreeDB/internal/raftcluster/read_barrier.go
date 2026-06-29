package raftcluster

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrReadBarrierNotSatisfied   = errors.New("raftcluster: read barrier not satisfied")
	ErrReadBarrierTargetMismatch = errors.New("raftcluster: read barrier target mismatch")
)

// AppliedProgress is the local apply progress a node can prove before serving
// a read. It is intentionally an applied-index contract, not a Raft read-index
// or lease proof.
type AppliedProgress struct {
	NodeID     NodeID
	GroupID    GroupID
	Term       uint64
	Index      uint64
	HasApplied bool
}

// AppliedIndexReadBarrier requests proof that a node has applied through at
// least MinAppliedIndex before a read observes local state.
type AppliedIndexReadBarrier struct {
	NodeID          NodeID
	GroupID         GroupID
	MinAppliedIndex uint64
}

// AppliedProgressReader exposes current applied progress with errors.
type AppliedProgressReader interface {
	AppliedProgress(context.Context) (AppliedProgress, error)
}

// AppliedIndexReadBarrierWaiter waits until it can prove an applied-index read
// barrier or fails closed.
type AppliedIndexReadBarrierWaiter interface {
	WaitAppliedIndex(context.Context, AppliedIndexReadBarrier) (AppliedProgress, error)
}

func (b AppliedIndexReadBarrier) SatisfiedBy(progress AppliedProgress) bool {
	return b.Check(progress) == nil
}

func (b AppliedIndexReadBarrier) Check(progress AppliedProgress) error {
	if b.NodeID != "" && progress.NodeID != b.NodeID {
		return fmt.Errorf("%w: node %q progress came from node %q", ErrReadBarrierTargetMismatch, b.NodeID, progress.NodeID)
	}
	if b.GroupID != "" && progress.GroupID != b.GroupID {
		return fmt.Errorf("%w: group %q progress came from group %q", ErrReadBarrierTargetMismatch, b.GroupID, progress.GroupID)
	}
	if b.MinAppliedIndex == 0 {
		return nil
	}
	if !progress.HasApplied {
		return fmt.Errorf("%w: no applied progress for required index %d", ErrReadBarrierNotSatisfied, b.MinAppliedIndex)
	}
	if progress.Index < b.MinAppliedIndex {
		return fmt.Errorf("%w: applied index %d below required index %d", ErrReadBarrierNotSatisfied, progress.Index, b.MinAppliedIndex)
	}
	return nil
}
