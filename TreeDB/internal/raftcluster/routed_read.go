package raftcluster

import (
	"context"
	"errors"
	"fmt"
)

// RoutedReadIndexCoordinator validates a read-index proof from the selected
// owner group and waits for that same owner to apply through it. This internal
// contract does not bind a collection store to the proof and therefore does
// not, by itself, authorize any public read path.
type RoutedReadIndexCoordinator interface {
	CoordinateRoutedReadIndex(context.Context, ReadIndexBarrier) (ReadIndexProof, AppliedProgress, error)
}

// GroupReadIndexCoordinatorV1 binds one statically registered owner group to
// its read-index provider and local apply waiter.
type GroupReadIndexCoordinatorV1 struct {
	GroupID            GroupID
	NodeID             NodeID
	ReadIndexProvider  ReadIndexProvider
	AppliedIndexWaiter AppliedIndexReadBarrierWaiter
}

type groupReadIndexCoordinatorV1 struct {
	nodeID             NodeID
	readIndexProvider  ReadIndexProvider
	appliedIndexWaiter AppliedIndexReadBarrierWaiter
}

// GroupRoutedReadIndexCoordinator is internal coordination scaffolding that
// dispatches a strong read barrier to exactly one statically registered owner
// group. It does not scatter, discover ownership, or bind the serving
// collection store; callers must supply a catalog-derived group target.
type GroupRoutedReadIndexCoordinator struct {
	byGroup map[GroupID]groupReadIndexCoordinatorV1
}

// NewGroupRoutedReadIndexCoordinator validates and copies a static owner
// registry for routed read-index/apply coordination.
func NewGroupRoutedReadIndexCoordinator(entries []GroupReadIndexCoordinatorV1) (*GroupRoutedReadIndexCoordinator, error) {
	if len(entries) == 0 {
		return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("at least one routed read coordinator is required"))
	}
	byGroup := make(map[GroupID]groupReadIndexCoordinatorV1, len(entries))
	for i, entry := range entries {
		if entry.GroupID == "" {
			return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("routed read coordinator[%d] missing group id", i))
		}
		if entry.NodeID == "" {
			return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("routed read coordinator[%d] for group %q missing node id", i, entry.GroupID))
		}
		if entry.ReadIndexProvider == nil {
			return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("routed read coordinator[%d] for group %q missing read-index provider", i, entry.GroupID))
		}
		if entry.AppliedIndexWaiter == nil {
			return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("routed read coordinator[%d] for group %q missing applied-index waiter", i, entry.GroupID))
		}
		if _, exists := byGroup[entry.GroupID]; exists {
			return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("routed read coordinator for group %q is duplicated", entry.GroupID))
		}
		byGroup[entry.GroupID] = groupReadIndexCoordinatorV1{
			nodeID:             entry.NodeID,
			readIndexProvider:  entry.ReadIndexProvider,
			appliedIndexWaiter: entry.AppliedIndexWaiter,
		}
	}
	return &GroupRoutedReadIndexCoordinator{byGroup: byGroup}, nil
}

// CoordinateRoutedReadIndex selects the requested owner before obtaining any
// proof, rejects stale leader targets, and enforces read-index-before-apply
// ordering for the selected owner.
func (c *GroupRoutedReadIndexCoordinator) CoordinateRoutedReadIndex(ctx context.Context, target ReadIndexBarrier) (ReadIndexProof, AppliedProgress, error) {
	if c == nil || len(c.byGroup) == 0 {
		return ReadIndexProof{}, AppliedProgress{}, ErrInvalidSubmitter
	}
	if target.GroupID == "" {
		return ReadIndexProof{}, AppliedProgress{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("routed read target missing group id"))
	}
	owner, ok := c.byGroup[target.GroupID]
	if !ok {
		return ReadIndexProof{}, AppliedProgress{}, errors.Join(ErrRouteTargetUnknown, fmt.Errorf("routed read group %q is not configured locally", target.GroupID))
	}
	if target.NodeID != "" && target.NodeID != owner.nodeID {
		return ReadIndexProof{}, AppliedProgress{}, fmt.Errorf("%w: routed read leader target %q does not match registered owner node %q for group %q", ErrReadBarrierTargetMismatch, target.NodeID, owner.nodeID, target.GroupID)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ownerTarget := ReadIndexBarrier{NodeID: owner.nodeID, GroupID: target.GroupID}
	proof, err := owner.readIndexProvider.ReadIndex(ctx, ownerTarget)
	if err != nil {
		return ReadIndexProof{}, AppliedProgress{}, err
	}
	if err := ownerTarget.Check(proof); err != nil {
		return ReadIndexProof{}, AppliedProgress{}, err
	}
	barrier := proof.AppliedIndexBarrier()
	progress, err := owner.appliedIndexWaiter.WaitAppliedIndex(ctx, barrier)
	if err != nil {
		return ReadIndexProof{}, AppliedProgress{}, err
	}
	if err := barrier.Check(progress); err != nil {
		return ReadIndexProof{}, AppliedProgress{}, err
	}
	return proof, progress, nil
}
