package nativewire

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// This server-owned scope carries no commit/count/visibility authority. Exact-ID
// source routing and all ANN placements must resolve to this one fixed group.
type VectorPartitionRoutedMutationV1 struct {
	VectorPartitionRoutedInsertV1
	Delete bool
}

type vectorPartitionColocatedSubmitterV1 interface {
	SubmitVectorPartitionColocatedMutationV1(context.Context, VectorPartitionRoutedMutationV1) (public.MutationResponseV1, error)
}

func (b *VectorPartitionPublicBackendV1) ReplaceVectorPartitionV1(ctx context.Context, request public.ReplaceRequestV1) (public.MutationResponseV1, error) {
	return b.submitColocatedMutationV1(ctx, VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: request}})
}
func (b *VectorPartitionPublicBackendV1) DeleteVectorPartitionV1(ctx context.Context, request public.DeleteRequestV1) (public.MutationResponseV1, error) {
	return b.submitColocatedMutationV1(ctx, VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{Version: request.Version, Generation: request.Generation, ID: request.ID, IdempotencyKey: request.IdempotencyKey, Deadline: request.Deadline}}, Delete: true})
}
func (b *VectorPartitionPublicBackendV1) submitColocatedMutationV1(ctx context.Context, request VectorPartitionRoutedMutationV1) (public.MutationResponseV1, error) {
	if b == nil || b.opts.Topology == nil {
		return public.MutationResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorUnavailableV1)
	}
	submitter, ok := b.opts.MutationSubmitter.(vectorPartitionColocatedSubmitterV1)
	if !ok || b.opts.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || b.opts.Identity.SourceFormat == 2 || len(b.opts.RequiredGroups) != 1 {
		return public.MutationResponseV1{}, publicBackendErrorV1(ErrFixedPeerVectorUnavailableV1)
	}
	if err := b.checkID(request.Request.Generation); err != nil {
		return public.MutationResponseV1{}, err
	}
	coordinator := b.opts.Topology.Coordinator()
	lease, err := coordinator.acquireRouterSessionV1(ctx, request.Request.Generation.Index, request.Request.Generation.Generation)
	if err != nil {
		return public.MutationResponseV1{}, publicBackendErrorV1(err)
	}
	defer lease.Close()
	status := lease.session.router.Status()
	ready, err := coordinator.validateReplicatedLifecycle(ctx, status)
	if err != nil {
		return public.MutationResponseV1{}, publicBackendErrorV1(err)
	}
	owner, err := vectorPartitionColocatedOwnerV1(status.Manifest, coordinator.placement)
	if err != nil {
		return public.MutationResponseV1{}, publicBackendErrorV1(err)
	}
	domain := uint32(0)
	if !request.Delete {
		selection, err := lease.session.router.SearchWithContextV1(ctx, request.Request.Vector, collections.VectorPartitionRouterSearchOptionsV3{Mode: collections.VectorPartitionRouterModeExactV1, ScoreBudget: int(status.Representatives), PartitionProbes: 1})
		if err != nil || len(selection.Partitions) != 1 {
			return public.MutationResponseV1{}, publicBackendErrorV1(errors.Join(ErrFixedPeerVectorWrongOwnerV1, err))
		}
		domain = selection.Partitions[0].PartitionID
	}
	partition, group, err := vectorPartitionMutationOwnerV1(status.Manifest, coordinator.placement, domain)
	if err != nil || group != owner {
		return public.MutationResponseV1{}, publicBackendErrorV1(errors.Join(ErrFixedPeerVectorWrongOwnerV1, err))
	}
	request.Identity, request.CatalogProof = b.opts.Identity, raftplacement.CatalogProofV1{Epoch: b.opts.Identity.Index.CatalogEpoch, Digest: b.opts.Identity.Index.CatalogDigest}
	request.ReadySetDigest, request.RouterModelDigest, request.OwnerGroup, request.PartitionID = ready, status.ModelDigest, owner, partition
	response, err := submitter.SubmitVectorPartitionColocatedMutationV1(ctx, request)
	if err != nil {
		return public.MutationResponseV1{}, fixedPeerVectorPublicErrorV1(err)
	}
	return response, nil
}

func vectorPartitionColocatedOwnerV1(manifest collections.VectorPartitionManifestV1, placement raftplacement.VectorPartitionPlacementRecordV1) (raftcluster.GroupID, error) {
	if len(placement.Partitions) == 0 || len(placement.Partitions) != int(manifest.PartitionCount) {
		return "", ErrFixedPeerVectorWrongOwnerV1
	}
	owner := placement.Partitions[0].GroupID
	if owner == "" {
		return "", ErrFixedPeerVectorWrongOwnerV1
	}
	for i, p := range placement.Partitions {
		if p.PartitionID != uint32(i) || p.GroupID != owner {
			return "", ErrFixedPeerVectorUnavailableV1
		}
	}
	return owner, nil
}

func (b *fixedPeerVectorBackendV1) ReplaceVectorPartitionV1(ctx context.Context, request public.ReplaceRequestV1) (public.MutationResponseV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.MutationResponseV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.MutationResponseV1{}, publicBackendErrorV1(err)
	}
	return backend.ReplaceVectorPartitionV1(ctx, request)
}
func (b *fixedPeerVectorBackendV1) DeleteVectorPartitionV1(ctx context.Context, request public.DeleteRequestV1) (public.MutationResponseV1, error) {
	if err := b.immutableMutationRefusalV1(); err != nil {
		return public.MutationResponseV1{}, err
	}
	backend, err := b.backend(ctx)
	if err != nil {
		return public.MutationResponseV1{}, publicBackendErrorV1(err)
	}
	return backend.DeleteVectorPartitionV1(ctx, request)
}
