package raftcluster

import (
	"errors"
	"testing"
)

func TestReadIndexBarrierAcceptsQuorumProof(t *testing.T) {
	barrier := ReadIndexBarrier{NodeID: "node-a", GroupID: "group-a"}
	proof := ReadIndexProof{
		NodeID:    "node-a",
		GroupID:   "group-a",
		Term:      7,
		Index:     42,
		HasQuorum: true,
	}
	if err := barrier.Check(proof); err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !barrier.SatisfiedBy(proof) {
		t.Fatalf("SatisfiedBy=false want true")
	}

	applied := proof.AppliedIndexBarrier()
	if applied.NodeID != proof.NodeID ||
		applied.GroupID != proof.GroupID ||
		applied.MinAppliedIndex != proof.Index {
		t.Fatalf("AppliedIndexBarrier=%+v proof=%+v", applied, proof)
	}
}

func TestReadIndexBarrierRejectsMissingQuorumProof(t *testing.T) {
	barrier := ReadIndexBarrier{NodeID: "node-a", GroupID: "group-a"}
	proof := ReadIndexProof{
		NodeID:  "node-a",
		GroupID: "group-a",
		Term:    7,
		Index:   42,
	}
	err := barrier.Check(proof)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("Check err=%v want ErrReadBarrierNotSatisfied", err)
	}
	if barrier.SatisfiedBy(proof) {
		t.Fatalf("SatisfiedBy=true want false")
	}
}

func TestReadIndexBarrierRejectsMissingReadIndex(t *testing.T) {
	barrier := ReadIndexBarrier{NodeID: "node-a", GroupID: "group-a"}
	proof := ReadIndexProof{
		NodeID:    "node-a",
		GroupID:   "group-a",
		Term:      7,
		HasQuorum: true,
	}
	err := barrier.Check(proof)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("Check err=%v want ErrReadBarrierNotSatisfied", err)
	}
}

func TestReadIndexBarrierRejectsTargetMismatch(t *testing.T) {
	tests := []struct {
		name    string
		barrier ReadIndexBarrier
		proof   ReadIndexProof
	}{
		{
			name:    "node",
			barrier: ReadIndexBarrier{NodeID: "node-a", GroupID: "group-a"},
			proof: ReadIndexProof{
				NodeID:    "node-b",
				GroupID:   "group-a",
				Term:      7,
				Index:     42,
				HasQuorum: true,
			},
		},
		{
			name:    "group",
			barrier: ReadIndexBarrier{NodeID: "node-a", GroupID: "group-a"},
			proof: ReadIndexProof{
				NodeID:    "node-a",
				GroupID:   "group-b",
				Term:      7,
				Index:     42,
				HasQuorum: true,
			},
		},
		{
			name:    "missing-node",
			barrier: ReadIndexBarrier{GroupID: "group-a"},
			proof: ReadIndexProof{
				GroupID:   "group-a",
				Term:      7,
				Index:     42,
				HasQuorum: true,
			},
		},
		{
			name:    "missing-group",
			barrier: ReadIndexBarrier{NodeID: "node-a"},
			proof: ReadIndexProof{
				NodeID:    "node-a",
				Term:      7,
				Index:     42,
				HasQuorum: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.barrier.Check(tt.proof)
			if !errors.Is(err, ErrReadBarrierTargetMismatch) {
				t.Fatalf("Check err=%v want ErrReadBarrierTargetMismatch", err)
			}
		})
	}
}
