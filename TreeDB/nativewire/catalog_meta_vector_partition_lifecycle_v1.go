package nativewire

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// CatalogMetaLinearizableAppliedIndexProviderV1 performs the meta-Raft read
// fence and returns the last catalog command installed in the fenced local
// applied view.
type CatalogMetaLinearizableAppliedIndexProviderV1 interface {
	LinearizableCatalogMetaAppliedIndexV1(context.Context) (uint64, error)
}

type catalogMetaNoLogReadProofProviderV1 interface {
	CatalogMetaLinearizableAppliedIndexProviderV1
	LinearizableCatalogMetaReadProofV1(context.Context) (raftcluster.CatalogMetaReadProofV1, error)
	ValidateCatalogMetaReadProofLeaseV1(raftcluster.CatalogMetaReadProofV1) error
}

type vectorPartitionServingAuthorityProofV1 struct {
	read      raftcluster.CatalogMetaReadProofV1
	authority raftplacement.VectorPartitionServingAuthoritySnapshotV1
}

// LinearizableCatalogVectorPartitionLifecycleAuthorityV1 is the only adapter
// from the concrete catalog authority to the M7 serving interface. Every
// validation first performs a fresh meta-Raft quorum read and then proves that
// the local authority has caught up through the returned command index.
type LinearizableCatalogVectorPartitionLifecycleAuthorityV1 struct {
	authority *raftplacement.CatalogMetaAuthorityV1
	readFence CatalogMetaLinearizableAppliedIndexProviderV1
}

func NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(
	authority *raftplacement.CatalogMetaAuthorityV1,
	readFence CatalogMetaLinearizableAppliedIndexProviderV1,
) (*LinearizableCatalogVectorPartitionLifecycleAuthorityV1, error) {
	if authority == nil || readFence == nil {
		return nil, errors.New("nativewire: catalog lifecycle authority and linearizable read fence are required")
	}
	return &LinearizableCatalogVectorPartitionLifecycleAuthorityV1{authority: authority, readFence: readFence}, nil
}

func (a *LinearizableCatalogVectorPartitionLifecycleAuthorityV1) ValidateVectorPartitionGenerationSearchV1(
	ctx context.Context,
	collection raftplacement.CollectionRefV1,
	index string,
	generation uint64,
	indexDefinitionDigest string,
	sourceGeneration uint64,
	sourceChecksum uint64,
	sourceSchemaHash uint64,
	sourceRowCount uint64,
) (string, error) {
	if a == nil || a.authority == nil || a.readFence == nil {
		return "", ErrVectorPartitionShardSearchAssetsUnavailable
	}
	requiredAppliedIndex, err := a.readFence.LinearizableCatalogMetaAppliedIndexV1(ctx)
	if err != nil {
		return "", errors.Join(ErrVectorPartitionShardSearchAssetsUnavailable, err)
	}
	return a.authority.ValidateVectorPartitionGenerationSearchAtAppliedIndexV1(
		ctx,
		requiredAppliedIndex,
		collection,
		index,
		generation,
		indexDefinitionDigest,
		sourceGeneration,
		sourceChecksum,
		sourceSchemaHash,
		sourceRowCount,
	)
}

func (a *LinearizableCatalogVectorPartitionLifecycleAuthorityV1) captureVectorPartitionServingAuthorityV1(
	ctx context.Context,
	collection raftplacement.CollectionRefV1,
	index string,
	generation uint64,
	indexDefinitionDigest string,
	sourceGeneration uint64,
	sourceChecksum uint64,
	sourceSchemaHash uint64,
	sourceRowCount uint64,
) (vectorPartitionServingAuthorityProofV1, error) {
	if a == nil || a.authority == nil {
		return vectorPartitionServingAuthorityProofV1{}, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	proofs, ok := a.readFence.(catalogMetaNoLogReadProofProviderV1)
	if !ok {
		return vectorPartitionServingAuthorityProofV1{}, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	proof, err := proofs.LinearizableCatalogMetaReadProofV1(ctx)
	if err != nil {
		return vectorPartitionServingAuthorityProofV1{}, errors.Join(ErrVectorPartitionShardSearchAssetsUnavailable, err)
	}
	authority, err := a.authority.VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
		ctx, proof.CatalogAppliedIndex, collection, index, generation, indexDefinitionDigest,
		sourceGeneration, sourceChecksum, sourceSchemaHash, sourceRowCount,
	)
	if err != nil {
		return vectorPartitionServingAuthorityProofV1{}, err
	}
	return vectorPartitionServingAuthorityProofV1{read: proof, authority: authority}, nil
}

func (a *LinearizableCatalogVectorPartitionLifecycleAuthorityV1) validateVectorPartitionServingAuthorityV1(proof vectorPartitionServingAuthorityProofV1) error {
	if a == nil || a.authority == nil {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	proofs, ok := a.readFence.(catalogMetaNoLogReadProofProviderV1)
	if !ok {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	if err := proofs.ValidateCatalogMetaReadProofLeaseV1(proof.read); err != nil {
		return errors.Join(ErrVectorPartitionShardSearchAssetsUnavailable, err)
	}
	if err := a.authority.ValidateVectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
		context.Background(), proof.read.CatalogAppliedIndex, proof.authority,
	); err != nil {
		return errors.Join(ErrVectorPartitionShardSearchGenerationMismatch, err)
	}
	return nil
}

var _ VectorPartitionReplicatedLifecycleAuthorityV1 = (*LinearizableCatalogVectorPartitionLifecycleAuthorityV1)(nil)
