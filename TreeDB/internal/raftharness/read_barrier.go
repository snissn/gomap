package raftharness

import (
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// ReadBarrier is a per-node test adapter for injected committed-entry harnesses.
// It may catch the target node up through the requested index before proving
// local applied progress. It does not prove production consensus.
type ReadBarrier struct {
	h      *Harness
	nodeID raftcluster.NodeID
}

func (h *Harness) ReadBarrier(nodeID raftcluster.NodeID) *ReadBarrier {
	return &ReadBarrier{h: h, nodeID: nodeID}
}

func (b *ReadBarrier) AppliedProgress(ctx context.Context) (raftcluster.AppliedProgress, error) {
	node, err := b.node()
	if err != nil {
		return raftcluster.AppliedProgress{}, err
	}
	return node.AppliedProgress(ctx)
}

func (b *ReadBarrier) WaitAppliedIndex(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedProgress, error) {
	ctx = readBarrierContext(ctx)
	if err := ctx.Err(); err != nil {
		return raftcluster.AppliedProgress{}, err
	}
	barrier, err := b.withDefaults(barrier)
	if err != nil {
		return raftcluster.AppliedProgress{}, err
	}
	progress, err := b.AppliedProgress(ctx)
	if err != nil {
		return progress, err
	}
	initialBarrierErr := barrier.Check(progress)
	if initialBarrierErr != nil && !errors.Is(initialBarrierErr, raftcluster.ErrReadBarrierNotSatisfied) {
		return progress, fmt.Errorf("raftharness: %w", initialBarrierErr)
	}
	if barrier.MinAppliedIndex == 0 {
		return progress, nil
	}
	if _, err := b.h.CatchUpNodeThrough(b.nodeID, barrier.MinAppliedIndex); err != nil {
		if refreshed, refreshErr := b.AppliedProgress(context.Background()); refreshErr == nil {
			progress = refreshed
		}
		if errors.Is(initialBarrierErr, raftcluster.ErrReadBarrierNotSatisfied) && barrier.Check(progress) == nil {
			return progress, nil
		}
		return progress, err
	}
	refreshed, refreshErr := b.AppliedProgress(context.Background())
	if refreshErr == nil {
		progress = refreshed
	}
	if err := ctx.Err(); err != nil {
		return progress, err
	}
	if refreshErr != nil {
		return progress, refreshErr
	}
	if err := barrier.Check(progress); err != nil {
		return progress, fmt.Errorf("raftharness: %w", err)
	}
	return progress, nil
}

func (n *Node) AppliedProgress(ctx context.Context) (raftcluster.AppliedProgress, error) {
	ctx = readBarrierContext(ctx)
	if err := ctx.Err(); err != nil {
		return raftcluster.AppliedProgress{}, err
	}
	if n == nil {
		return raftcluster.AppliedProgress{}, fmt.Errorf("%w: nil node", ErrNodeClosed)
	}
	if n.fsm == nil || n.closed {
		return raftcluster.AppliedProgress{}, fmt.Errorf("%w: closed node", ErrNodeClosed)
	}
	return n.fsm.AppliedProgress(ctx)
}

func (n *Node) WaitAppliedIndex(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedProgress, error) {
	ctx = readBarrierContext(ctx)
	if err := ctx.Err(); err != nil {
		return raftcluster.AppliedProgress{}, err
	}
	if n == nil {
		return raftcluster.AppliedProgress{}, fmt.Errorf("%w: nil node", ErrNodeClosed)
	}
	if n.fsm == nil || n.closed {
		return raftcluster.AppliedProgress{}, fmt.Errorf("%w: closed node", ErrNodeClosed)
	}
	return n.fsm.WaitAppliedIndex(ctx, barrier)
}

func (b *ReadBarrier) node() (*Node, error) {
	if b == nil || b.h == nil {
		return nil, ErrHarnessClosed
	}
	b.h.mu.Lock()
	defer b.h.mu.Unlock()
	if b.h.closed {
		return nil, ErrHarnessClosed
	}
	node, ok := b.h.nodes[b.nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, b.nodeID)
	}
	return node, nil
}

func (b *ReadBarrier) withDefaults(barrier raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedIndexReadBarrier, error) {
	if b == nil || b.h == nil {
		return barrier, ErrHarnessClosed
	}
	b.h.mu.Lock()
	defer b.h.mu.Unlock()
	if b.h.closed {
		return barrier, ErrHarnessClosed
	}
	if barrier.NodeID == "" {
		barrier.NodeID = b.nodeID
	}
	if barrier.GroupID == "" {
		barrier.GroupID = b.h.groupID
	}
	return barrier, nil
}

func readBarrierContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
