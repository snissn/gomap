package raftcluster

import (
	"context"
	"fmt"
)

// ReadIndexBarrier requests a quorum-backed read-index proof for a node/group.
// Empty NodeID or GroupID fields do not constrain the proof target, but callers
// that know the intended target should set them so mismatches fail closed.
type ReadIndexBarrier struct {
	NodeID  NodeID
	GroupID GroupID
}

// ReadIndexProof is the minimal result a Raft adapter must provide before a
// linearizable local read can wait for local application.
type ReadIndexProof struct {
	NodeID    NodeID
	GroupID   GroupID
	Term      uint64
	Index     uint64
	HasQuorum bool
}

// ReadIndexProvider obtains a read-index proof from the selected Raft adapter.
type ReadIndexProvider interface {
	ReadIndex(context.Context, ReadIndexBarrier) (ReadIndexProof, error)
}

func (b ReadIndexBarrier) SatisfiedBy(proof ReadIndexProof) bool {
	return b.Check(proof) == nil
}

func (b ReadIndexBarrier) Check(proof ReadIndexProof) error {
	if b.NodeID != "" && proof.NodeID != b.NodeID {
		return fmt.Errorf("%w: node %q proof came from node %q", ErrReadBarrierTargetMismatch, b.NodeID, proof.NodeID)
	}
	if b.GroupID != "" && proof.GroupID != b.GroupID {
		return fmt.Errorf("%w: group %q proof came from group %q", ErrReadBarrierTargetMismatch, b.GroupID, proof.GroupID)
	}
	if proof.NodeID == "" {
		return fmt.Errorf("%w: read-index proof missing node id", ErrReadBarrierTargetMismatch)
	}
	if proof.GroupID == "" {
		return fmt.Errorf("%w: read-index proof missing group id", ErrReadBarrierTargetMismatch)
	}
	if !proof.HasQuorum {
		return fmt.Errorf("%w: read-index proof missing quorum", ErrReadBarrierNotSatisfied)
	}
	if proof.Index == 0 {
		return fmt.Errorf("%w: read-index proof missing index", ErrReadBarrierNotSatisfied)
	}
	return nil
}

func (p ReadIndexProof) AppliedIndexBarrier() AppliedIndexReadBarrier {
	return AppliedIndexReadBarrier{
		NodeID:          p.NodeID,
		GroupID:         p.GroupID,
		MinAppliedIndex: p.Index,
	}
}
