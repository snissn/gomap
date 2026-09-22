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

// ReadIndexEvidenceKind records where a read-index proof came from. Production
// linearizable reads only accept ReadIndexEvidenceProduction.
type ReadIndexEvidenceKind uint8

const (
	ReadIndexEvidenceUnknown ReadIndexEvidenceKind = iota
	ReadIndexEvidenceTestHarness
	ReadIndexEvidenceProduction
)

func (k ReadIndexEvidenceKind) String() string {
	switch k {
	case ReadIndexEvidenceTestHarness:
		return "test_harness"
	case ReadIndexEvidenceProduction:
		return "production"
	default:
		return "unknown"
	}
}

// ReadIndexProof is the minimal result a Raft adapter must provide before a
// linearizable local read can wait for local application. Production Raft
// adapters must set EvidenceKind to ReadIndexEvidenceProduction only after a
// real read-index or equivalent quorum proof. Harness and fake providers should
// use non-production evidence unless a narrow test deliberately exercises the
// production path.
type ReadIndexProof struct {
	NodeID       NodeID
	GroupID      GroupID
	Term         uint64
	Index        uint64
	HasQuorum    bool
	EvidenceKind ReadIndexEvidenceKind
}

// ReadIndexProvider obtains a read-index proof from the selected Raft adapter.
type ReadIndexProvider interface {
	ReadIndex(context.Context, ReadIndexBarrier) (ReadIndexProof, error)
}

func (b ReadIndexBarrier) SatisfiedBy(proof ReadIndexProof) bool {
	return b.Check(proof) == nil
}

// ReadIndexCheckOptions allows deterministic harnesses to validate target and
// quorum shape without pretending their evidence is production-consensus proof.
type ReadIndexCheckOptions struct {
	AllowNonProductionEvidence bool
}

func (b ReadIndexBarrier) Check(proof ReadIndexProof) error {
	return b.CheckWithOptions(proof, ReadIndexCheckOptions{})
}

func (b ReadIndexBarrier) CheckWithOptions(proof ReadIndexProof, opts ReadIndexCheckOptions) error {
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
	switch proof.EvidenceKind {
	case ReadIndexEvidenceProduction:
		return nil
	case ReadIndexEvidenceTestHarness:
		if opts.AllowNonProductionEvidence {
			return nil
		}
		return fmt.Errorf("%w: read-index proof evidence %s is not production", ErrReadBarrierNotSatisfied, proof.EvidenceKind)
	default:
		return fmt.Errorf("%w: read-index proof missing evidence kind", ErrReadBarrierNotSatisfied)
	}
}

func (p ReadIndexProof) AppliedIndexBarrier() AppliedIndexReadBarrier {
	return AppliedIndexReadBarrier{
		NodeID:          p.NodeID,
		GroupID:         p.GroupID,
		MinAppliedIndex: p.Index,
	}
}
