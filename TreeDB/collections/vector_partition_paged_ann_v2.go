package collections

import (
	"context"
	"fmt"
	"slices"

	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// verifyANNIntentV2 streams every local owner exactly once. The root is either
// the immutable private intent root under the builder's capture lease, or the
// root atomically bound by the pinned local VCP1 generation. No producer callback
// survives this boundary, and no owner-wide membership list is retained.
func (s *VectorPartitionPagedSourceSessionV2) verifyANNIntentV2(ctx context.Context, input VectorPartitionPreparedInputV2, root VectorPartitionAssetV1) error {
	if len(input.LocalANNOwners) == 0 {
		if root != (VectorPartitionAssetV1{}) {
			return fmt.Errorf("%w: unexpected ANN directory", ErrVectorPartitionManifestInvalid)
		}
		return nil
	}
	var current vectorPartitionSourceCurrentChunkV2
	var acc *source.ANNOwnerAccumulatorV2
	var expected source.ANNOwnerCommitmentV2
	var domain uint64
	ownerIndex := 0
	finish := func() error {
		if acc == nil {
			return nil
		}
		got, err := acc.Commitment()
		if err != nil {
			return err
		}
		if got != expected {
			return fmt.Errorf("%w: complete ANN owner commitment", ErrVectorPartitionManifestInvalid)
		}
		return nil
	}
	err := walkVectorPartitionDirectoryV2(ctx, root, "metadata", "", func(a VectorPartitionAssetV1) ([]byte, error) {
		if a.Ref.Generation != input.Generation || a.Ref.Namespace != s.collection.meta.Options.ColumnStore.AssetManager.Namespace {
			return nil, fmt.Errorf("%w: foreign ANN page", ErrVectorPartitionManifestInvalid)
		}
		return readVectorPartitionDirectoryAssetV2(s.collection.db.ColumnAssetRootDir(), a)
	}, nil, func(r VectorPartitionDirectoryRecordV2) error {
		if acc == nil || r.Owner != expected.GroupID {
			if err := finish(); err != nil {
				return err
			}
			if ownerIndex >= len(input.LocalANNOwners) || r.Owner != input.LocalANNOwners[ownerIndex] {
				return fmt.Errorf("%w: ANN owner coverage", ErrVectorPartitionManifestInvalid)
			}
			i, ok := slices.BinarySearchFunc(input.ANNOwners, r.Owner, func(a source.ANNOwnerCommitmentV2, b string) int {
				if a.GroupID < b {
					return -1
				}
				if a.GroupID > b {
					return 1
				}
				return 0
			})
			if !ok {
				return fmt.Errorf("%w: uncommitted ANN owner", ErrVectorPartitionManifestInvalid)
			}
			expected = input.ANNOwners[i]
			var err error
			acc, err = source.NewANNOwnerAccumulatorV2(r.Owner)
			if err != nil {
				return err
			}
			ownerIndex++
			if r.Domain == nil {
				return fmt.Errorf("%w: missing ANN domain declaration", ErrVectorPartitionManifestInvalid)
			}
		}
		if r.Domain != nil {
			domain = r.DomainID
			return acc.BeginDomain(*r.Domain)
		}
		if r.DomainID != domain {
			return fmt.Errorf("%w: undeclared ANN domain", ErrVectorPartitionManifestInvalid)
		}
		if r.Member != nil {
			if err := acc.AddMember(source.ANNMemberV2{Source: *r.Member, Kind: r.MembershipKind}); err != nil {
				return err
			}
			_, err := s.readSourceRowWithCurrentChunkV2(ctx, *r.Member, &current)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := finish(); err != nil {
		return err
	}
	if ownerIndex != len(input.LocalANNOwners) {
		return fmt.Errorf("%w: omitted ANN owner", ErrVectorPartitionManifestInvalid)
	}
	s.verifiedANNOwners = slices.Clone(input.LocalANNOwners)
	return nil
}
