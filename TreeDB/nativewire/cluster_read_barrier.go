package nativewire

import (
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// AppliedIndexReadBarrierProvider supplies the applied-index target a
// leader_read must prove before nativewire serves local state.
type AppliedIndexReadBarrierProvider interface {
	LeaderReadBarrier(context.Context, ClusterReadRequest) (raftcluster.AppliedIndexReadBarrier, error)
}

// AppliedIndexReadCoordinator bridges nativewire leader_read requests to the
// raftcluster applied-index read barrier foundation. It intentionally does not
// claim linearizable or lease-read safety; those require a real read-index or
// lease proof outside this adapter.
type AppliedIndexReadCoordinator struct {
	BarrierProvider AppliedIndexReadBarrierProvider
	Waiter          raftcluster.AppliedIndexReadBarrierWaiter
}

func (c AppliedIndexReadCoordinator) CoordinateRead(ctx context.Context, request ClusterReadRequest) (ClusterReadResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch request.Policy {
	case ConsistencyLeaderRead:
	case ConsistencyLinearizable:
		return ClusterReadResult{}, errors.New("nativewire: linearizable reads require a read-index proof")
	case ConsistencyLeaseRead:
		return ClusterReadResult{}, errors.New("nativewire: lease reads require a lease proof")
	default:
		return ClusterReadResult{}, fmt.Errorf("nativewire: unsupported coordinated read policy %s", consistencyPolicyName(request.Policy))
	}
	if c.BarrierProvider == nil {
		return ClusterReadResult{}, errors.New("nativewire: applied-index read barrier provider is not configured")
	}
	if c.Waiter == nil {
		return ClusterReadResult{}, errors.New("nativewire: applied-index read barrier waiter is not configured")
	}
	barrier, err := c.BarrierProvider.LeaderReadBarrier(ctx, request)
	if err != nil {
		return ClusterReadResult{}, err
	}
	if err := validateAppliedIndexLeaderReadBarrier(barrier); err != nil {
		return ClusterReadResult{}, err
	}
	progress, err := c.Waiter.WaitAppliedIndex(ctx, barrier)
	if err != nil {
		return ClusterReadResult{}, err
	}
	if err := barrier.Check(progress); err != nil {
		return ClusterReadResult{}, err
	}
	if !progress.HasApplied {
		return ClusterReadResult{}, fmt.Errorf("%w: no applied progress for leader_read", raftcluster.ErrReadBarrierNotSatisfied)
	}
	return ClusterReadResult{
		ActualConsistency: ConsistencyLeaderRead,
		ServingNode:       string(progress.NodeID),
		LeaderNode:        string(barrier.NodeID),
		AppliedIndex:      progress.Index,
		HasAppliedIndex:   true,
	}, nil
}

func validateAppliedIndexLeaderReadBarrier(barrier raftcluster.AppliedIndexReadBarrier) error {
	if barrier.NodeID == "" {
		return fmt.Errorf("%w: leader_read barrier missing node id", raftcluster.ErrReadBarrierTargetMismatch)
	}
	if barrier.GroupID == "" {
		return fmt.Errorf("%w: leader_read barrier missing group id", raftcluster.ErrReadBarrierTargetMismatch)
	}
	if barrier.MinAppliedIndex == 0 {
		return fmt.Errorf("%w: leader_read barrier missing minimum applied index", raftcluster.ErrReadBarrierNotSatisfied)
	}
	return nil
}
