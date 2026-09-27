package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// VectorPartitionOwnerSourceInputV2 is node-owned local preparation input.
// Walk visits immutable paged descriptors in strict shard-ID order. Descriptors
// select imports to open; they are not caller-supplied verification receipts.
type VectorPartitionOwnerSourceInputV2 struct {
	GroupID    raftcluster.GroupID
	Collection *collections.Collection
	Walk       func(context.Context, func(collections.VectorPartitionSourceSnapshotV2) error) error
}

// PrepareVectorPartitionOwnerSourceV2 derives one owner's aggregate from actual
// completed imports. Remote map owners need not be locally hosted. The walk
// must cover exactly this owner's assigned shards, in canonical shard-ID order.
// Only one source reader is retained and no shard descriptor corpus is copied.
func PrepareVectorPartitionOwnerSourceV2(ctx context.Context, identity raftplacement.VectorPartitionLifecycleIdentityV1, ownership raftplacement.ResolvedSourceShardMapV2, owner VectorPartitionOwnerSourceInputV2) (raftplacement.VectorPartitionSourceOwnerPreparationV2, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if identity.SourceFormat != 2 || identity.Source != (raftplacement.VectorPartitionLifecycleSourceIdentityV1{}) || identity.Index.Collection != ownership.Collection() || identity.SourceV2.SourceMapEpoch != ownership.Epoch() || identity.SourceV2.SourceMapDigest != ownership.Digest() {
		return raftplacement.VectorPartitionSourceOwnerPreparationV2{}, raftplacement.ErrVectorPartitionLifecycleIdentity
	}
	if owner.GroupID == "" || owner.Collection == nil || owner.Walk == nil {
		return raftplacement.VectorPartitionSourceOwnerPreparationV2{}, raftplacement.ErrVectorPartitionLifecycleGuard
	}
	sourceMap := ownership.SourceMapV2()
	var expected uint64
	if err := sourceMap.WalkShards(func(shard sourcepartition.SourceShardV2) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if shard.GroupID == string(owner.GroupID) {
			expected++
		}
		return nil
	}); err != nil {
		return raftplacement.VectorPartitionSourceOwnerPreparationV2{}, err
	}
	if expected == 0 {
		return raftplacement.VectorPartitionSourceOwnerPreparationV2{}, raftplacement.ErrVectorPartitionLifecycleIdentity
	}
	semantic, completion := source.NewOwnerSnapshotSetHashV2(string(owner.GroupID)), sha256.New()
	sourcePreparationHashStringV2(completion, "treedb/owner-local-completion/v2")
	sourcePreparationHashStringV2(completion, string(owner.GroupID))
	var previous string
	var count uint64
	err := owner.Walk(ctx, func(snapshot collections.VectorPartitionSourceSnapshotV2) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if snapshot.ShardID <= previous || count >= expected || hex.EncodeToString(snapshot.IndexDefinitionDigest[:]) != identity.Index.IndexDefinitionDigest {
			return raftplacement.ErrVectorPartitionLifecycleIdentity
		}
		reader, err := owner.Collection.OpenVectorPartitionSourceSnapshotV2(snapshot, identity.Index.IndexName)
		if err != nil {
			return err
		}
		if err = reader.ValidateOwnerV2(sourceMap, string(owner.GroupID)); err != nil {
			return errors.Join(err, reader.Close())
		}
		commitment := reader.Commitment()
		if err = reader.Close(); err != nil {
			return err
		}
		source.WriteOwnerSnapshotIdentityV2(semantic, snapshot)
		sourcePreparationHashStringV2(completion, snapshot.ShardID)
		var revision [8]byte
		binary.BigEndian.PutUint64(revision[:], snapshot.SnapshotRevision)
		completion.Write(revision[:])
		completion.Write(snapshot.Digest[:])
		binary.BigEndian.PutUint64(revision[:], commitment.DirectoryGeneration)
		completion.Write(revision[:])
		completion.Write(commitment.DirectoryDigest[:])
		previous = snapshot.ShardID
		count++
		return nil
	})
	if err != nil {
		return raftplacement.VectorPartitionSourceOwnerPreparationV2{}, err
	}
	if err := ctx.Err(); err != nil {
		return raftplacement.VectorPartitionSourceOwnerPreparationV2{}, err
	}
	if count != expected {
		return raftplacement.VectorPartitionSourceOwnerPreparationV2{}, raftplacement.ErrVectorPartitionLifecycleIdentity
	}
	return raftplacement.VectorPartitionSourceOwnerPreparationV2{GroupID: owner.GroupID, ShardCount: count, SnapshotSetDigest: hex.EncodeToString(semantic.Sum(nil)), CompletionEvidenceDigest: hex.EncodeToString(completion.Sum(nil))}, nil
}

// PrepareVectorPartitionSourcesV2 derives all available owner inputs and checks
// exact map coverage for a catalog BEGIN. This local convenience path explicitly
// refuses a map with unavailable remote owners. Local build/open calls the
// one-owner helper above; distributed preparation transport belongs to P3.
func PrepareVectorPartitionSourcesV2(ctx context.Context, identity raftplacement.VectorPartitionLifecycleIdentityV1, ownership raftplacement.ResolvedSourceShardMapV2, owners []VectorPartitionOwnerSourceInputV2) ([]raftplacement.VectorPartitionSourceOwnerPreparationV2, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(owners) == 0 || len(owners) > raftplacement.MaxVectorPartitionLifecycleGroupsV1 {
		return nil, raftplacement.ErrVectorPartitionLifecycleLimit
	}
	available := make(map[raftcluster.GroupID]bool, len(owners))
	for _, owner := range owners {
		if available[owner.GroupID] {
			return nil, raftplacement.ErrVectorPartitionLifecycleIdentity
		}
		available[owner.GroupID] = true
	}
	if err := ownership.SourceMapV2().WalkShards(func(shard sourcepartition.SourceShardV2) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !available[raftcluster.GroupID(shard.GroupID)] {
			return errors.Join(raftplacement.ErrVectorPartitionLifecycleGuard, errors.New("nativewire: source owner preparation unavailable"))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	out := make([]raftplacement.VectorPartitionSourceOwnerPreparationV2, 0, len(owners))
	for _, owner := range owners {
		prepared, err := PrepareVectorPartitionOwnerSourceV2(ctx, identity, ownership, owner)
		if err != nil {
			return nil, err
		}
		out = append(out, prepared)
	}
	return out, nil
}

// BeginPreparedVectorPartitionBuildV2 enters the existing replicated BUILD
// coordinator through concrete durable import preparation. The caller supplies
// descriptors, never verification receipts; missing owners fail closed before
// any catalog command is submitted. ANN required groups are independent of the
// source-map owner set. This does not activate distributed V2 reads.
func BeginPreparedVectorPartitionBuildV2(ctx context.Context, coordinator raftplacement.VectorPartitionLifecycleCoordinatorV1, identity raftplacement.VectorPartitionLifecycleIdentityV1, ownership raftplacement.ResolvedSourceShardMapV2, owners []VectorPartitionOwnerSourceInputV2, requiredGroups []raftcluster.GroupID, previousGeneration, mutationEpoch uint64) (raftplacement.VectorPartitionLifecycleRecordV1, error) {
	if identity.SourceFormat != 2 {
		return raftplacement.VectorPartitionLifecycleRecordV1{}, raftplacement.ErrVectorPartitionLifecycleIdentity
	}
	coordinator.PrepareSourceV2 = func(ctx context.Context, identity raftplacement.VectorPartitionLifecycleIdentityV1) ([]raftplacement.VectorPartitionSourceOwnerPreparationV2, error) {
		return PrepareVectorPartitionSourcesV2(ctx, identity, ownership, owners)
	}
	return coordinator.BeginBuildV1(ctx, identity, requiredGroups, previousGeneration, mutationEpoch)
}

func sourcePreparationHashStringV2(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	h.Write(size[:])
	h.Write([]byte(value))
}
