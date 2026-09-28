package raftfsm

import (
	"context"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// ReplacementTailProgressV1 reads immutable progress/result records under the
// existing FSM lock and proves their local command-WAL coverage. The returned
// identity deliberately excludes replica-local LSNs.
func (f *FSM) ReplacementTailProgressV1(ctx context.Context, id raftentry.ApplyEntryID) (raftcluster.ReplacementTailProgressV1, error) {
	var proof raftcluster.ReplacementTailProgressV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	if f == nil {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed || f.db == nil || f.progress == nil || f.results == nil {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	latest, ok, err := f.lastAppliedProgressRecord()
	if err != nil {
		return proof, err
	}
	if !ok || latest.EntryID.Index < id.Index {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	localLSN, err := localAppliedCommandLSN(f.db)
	if err != nil {
		return proof, err
	}
	if localLSN != latest.AppliedCommandLSN {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	progress, ok, err := f.progress.LookupApplyProgress(id)
	if err != nil {
		return proof, err
	}
	if !ok || progress.EntryID != id || progress.AppliedCommandLSN > localLSN {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	result, ok, err := f.results.LookupApplyResult(id)
	if err != nil {
		return proof, err
	}
	if !ok || result.EntryID != id || result.CommandDigest != progress.CommandDigest || result.AppliedCommandLSN != progress.AppliedCommandLSN {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	digest := result.ProgressLogicalDigestV1
	if digest == (raftapply.LogicalDigestV1{}) {
		digest = raftapply.LogicalDigestV1(result.Result.ResultDigest)
	}
	if digest != progress.LogicalDigestV1 {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	proof = raftcluster.ReplacementTailProgressV1{EntryID: id, CommandDigest: progress.CommandDigest, ProgressDigest: raftentry.CommandDigestV1(progress.LogicalDigestV1), Result: result.Result}
	return proof, proof.Validate()
}
