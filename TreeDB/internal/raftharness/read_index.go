package raftharness

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// ReadIndexProvider is a test-only adapter for injected committed-entry
// harnesses. It reports the latest injected committed entry as read-index
// evidence for a configured node/group, but it does not prove production
// consensus and it does not apply entries locally.
type ReadIndexProvider struct {
	h      *Harness
	nodeID raftcluster.NodeID
}

func (h *Harness) ReadIndexProvider(nodeID raftcluster.NodeID) *ReadIndexProvider {
	return &ReadIndexProvider{h: h, nodeID: nodeID}
}

func (p *ReadIndexProvider) ReadIndex(ctx context.Context, target raftcluster.ReadIndexBarrier) (raftcluster.ReadIndexProof, error) {
	ctx = readBarrierContext(ctx)
	if err := ctx.Err(); err != nil {
		return raftcluster.ReadIndexProof{}, err
	}
	if p == nil || p.h == nil {
		return raftcluster.ReadIndexProof{}, ErrHarnessClosed
	}

	p.h.mu.Lock()
	defer p.h.mu.Unlock()
	if p.h.closed {
		return raftcluster.ReadIndexProof{}, ErrHarnessClosed
	}
	node, ok := p.h.nodes[p.nodeID]
	if !ok {
		return raftcluster.ReadIndexProof{}, fmt.Errorf("%w: %s", ErrNodeNotFound, p.nodeID)
	}
	if node == nil || node.closed {
		return raftcluster.ReadIndexProof{}, fmt.Errorf("%w: %s", ErrNodeClosed, p.nodeID)
	}
	if len(p.h.committed) == 0 {
		return raftcluster.ReadIndexProof{}, fmt.Errorf("%w: read-index has no committed entries", raftcluster.ErrReadBarrierNotSatisfied)
	}
	last := p.h.committed[len(p.h.committed)-1]
	proof := raftcluster.ReadIndexProof{
		NodeID:       p.nodeID,
		GroupID:      p.h.groupID,
		Term:         last.Term,
		Index:        last.Index,
		HasQuorum:    true,
		EvidenceKind: raftcluster.ReadIndexEvidenceTestHarness,
	}
	if err := target.CheckWithOptions(proof, raftcluster.ReadIndexCheckOptions{AllowNonProductionEvidence: true}); err != nil {
		return raftcluster.ReadIndexProof{}, fmt.Errorf("raftharness: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return raftcluster.ReadIndexProof{}, err
	}
	return proof, nil
}
