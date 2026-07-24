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
// raftcluster applied-index read barrier foundation. Linearizable reads require
// a configured read-index provider with production evidence and then wait for
// local apply through the proven read index. Lease reads remain unsupported
// until a lease proof exists.
type AppliedIndexReadCoordinator struct {
	BarrierProvider   AppliedIndexReadBarrierProvider
	Waiter            raftcluster.AppliedIndexReadBarrierWaiter
	ReadIndexTarget   raftcluster.ReadIndexBarrier
	ReadIndexProvider raftcluster.ReadIndexProvider
}

func (c AppliedIndexReadCoordinator) CoordinateRead(ctx context.Context, request ClusterReadRequest) (ClusterReadResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch request.Policy {
	case ConsistencyLeaderRead:
		return c.coordinateLeaderRead(ctx, request)
	case ConsistencyLinearizable:
		return c.coordinateLinearizableRead(ctx)
	case ConsistencyLeaseRead:
		return ClusterReadResult{}, errors.New("nativewire: lease reads require a lease proof")
	default:
		return ClusterReadResult{}, fmt.Errorf("nativewire: unsupported coordinated read policy %s", consistencyPolicyName(request.Policy))
	}
}

func (c AppliedIndexReadCoordinator) coordinateLeaderRead(ctx context.Context, request ClusterReadRequest) (ClusterReadResult, error) {
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

func (c AppliedIndexReadCoordinator) coordinateLinearizableRead(ctx context.Context) (ClusterReadResult, error) {
	if c.ReadIndexProvider == nil {
		return ClusterReadResult{}, errors.New("nativewire: read-index provider is not configured")
	}
	if c.Waiter == nil {
		return ClusterReadResult{}, errors.New("nativewire: applied-index read barrier waiter is not configured")
	}
	proof, err := c.ReadIndexProvider.ReadIndex(ctx, c.ReadIndexTarget)
	if err != nil {
		return ClusterReadResult{}, err
	}
	if err := c.ReadIndexTarget.Check(proof); err != nil {
		return ClusterReadResult{}, err
	}
	barrier := proof.AppliedIndexBarrier()
	progress, err := c.Waiter.WaitAppliedIndex(ctx, barrier)
	if err != nil {
		return ClusterReadResult{}, err
	}
	if err := barrier.Check(progress); err != nil {
		return ClusterReadResult{}, err
	}
	if !progress.HasApplied {
		return ClusterReadResult{}, fmt.Errorf("%w: no applied progress for linearizable read", raftcluster.ErrReadBarrierNotSatisfied)
	}
	return ClusterReadResult{
		ActualConsistency: ConsistencyLinearizable,
		ServingNode:       string(progress.NodeID),
		LeaderNode:        string(proof.NodeID),
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
