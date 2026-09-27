package nativewire

import (
	"cmp"
	"context"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// VectorPartitionOwnerANNInputV2 is the trusted planner's complete intended
// owner placement stream, not a supplied digest or receipt. BUILD commits this
// intent. The local builder must resolve every row through its verified source
// pages bound to the same BUILD before graph construction or exposure.
type VectorPartitionOwnerANNInputV2 struct {
	GroupID raftcluster.GroupID
	Walk    func(context.Context, func(source.ANNRecordV2) error) error
}

func PrepareVectorPartitionANNV2(ctx context.Context, identity raftplacement.VectorPartitionLifecycleIdentityV1, owners []VectorPartitionOwnerANNInputV2) ([]raftplacement.VectorPartitionANNOwnerPreparationV2, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if identity.SourceFormat != 2 || len(owners) == 0 || len(owners) > raftplacement.MaxVectorPartitionLifecycleGroupsV1 {
		return nil, raftplacement.ErrVectorPartitionLifecycleGuard
	}
	out := make([]raftplacement.VectorPartitionANNOwnerPreparationV2, 0, len(owners))
	for _, owner := range owners {
		if owner.Walk == nil {
			return nil, raftplacement.ErrVectorPartitionLifecycleGuard
		}
		accumulator, err := source.NewANNOwnerAccumulatorV2(string(owner.GroupID))
		if err != nil {
			return nil, err
		}
		if err = owner.Walk(ctx, func(record source.ANNRecordV2) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return accumulator.AddRecord(record)
		}); err != nil {
			return nil, err
		}
		commitment, err := accumulator.Commitment()
		if err != nil {
			return nil, err
		}
		out = append(out, commitment)
	}
	slices.SortFunc(out, func(a, b raftplacement.VectorPartitionANNOwnerPreparationV2) int {
		return cmp.Compare(a.GroupID, b.GroupID)
	})
	if _, err := source.ANNOwnerSetDigestV2(out); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
