package raftfsm

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func (f *FSM) AppliedProgress(ctx context.Context) (raftcluster.AppliedProgress, error) {
	ctx = readBarrierContext(ctx)
	progress := f.appliedProgressIdentity()
	if err := ctx.Err(); err != nil {
		return progress, err
	}
	if f == nil {
		return progress, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.appliedProgressLocked()
}

func (f *FSM) appliedProgressLocked() (raftcluster.AppliedProgress, error) {
	progress := f.appliedProgressIdentity()
	if f == nil || f.db == nil || f.progress == nil {
		return progress, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if f.closed {
		return progress, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is closed")
	}
	record, ok, err := f.lastAppliedProgressRecord()
	if err != nil {
		return progress, err
	}
	if !ok {
		return progress, nil
	}
	progress.Term = record.EntryID.Term
	progress.Index = record.EntryID.Index
	progress.HasApplied = true
	return progress, nil
}

func (f *FSM) WaitAppliedIndex(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedProgress, error) {
	progress, err := f.AppliedProgress(ctx)
	if err != nil {
		return progress, err
	}
	if err := barrier.Check(progress); err != nil {
		return progress, fmt.Errorf("raftfsm: %w", err)
	}
	return progress, nil
}

func (f *FSM) appliedProgressIdentity() raftcluster.AppliedProgress {
	if f == nil {
		return raftcluster.AppliedProgress{}
	}
	return raftcluster.AppliedProgress{
		NodeID:  f.cluster.NodeID,
		GroupID: f.cluster.GroupID,
	}
}

func readBarrierContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
