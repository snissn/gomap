package raftplacement

import (
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// VectorPartitionLifecycleGroupBuilderV1 is the narrow integration seam for
// a real asset builder/uploader. It returns only the bounded readiness proof;
// transport, uploads, and local prepared-manifest staging remain owned by the
// caller's group implementation.
type VectorPartitionLifecycleGroupBuilderV1 interface {
	BuildAndStageVectorPartitionGroupV1(context.Context, VectorPartitionLifecycleIdentityV1, raftcluster.GroupID) (VectorPartitionLifecycleGroupReadyV1, error)
}

// BeginBuildV1 records a source- and catalog-bound candidate before any group
// assets can be accepted. Exact retry is handled by the catalog command digest.
func (c VectorPartitionLifecycleCoordinatorV1) BeginBuildV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, required []raftcluster.GroupID, previousGeneration, mutationEpoch uint64) (VectorPartitionLifecycleRecordV1, error) {
	return c.Submit(ctx, VectorPartitionLifecycleCommandV1{Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1, Identity: identity, RequiredGroups: required, PreviousActiveGeneration: previousGeneration, MutationEpoch: mutationEpoch})
}

func (c VectorPartitionLifecycleCoordinatorV1) RecordGroupReadyV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, ready VectorPartitionLifecycleGroupReadyV1) (VectorPartitionLifecycleRecordV1, error) {
	record, ok := c.Authority.VectorPartitionLifecycleRecordV1(identity)
	if !ok {
		return VectorPartitionLifecycleRecordV1{}, ErrVectorPartitionLifecycleGuard
	}
	return c.Submit(ctx, VectorPartitionLifecycleCommandV1{Kind: VectorPartitionLifecycleRecordGroupReadyV1, ExpectedRevision: record.Revision, ExpectedState: record.State, Identity: identity, GroupReady: ready})
}

// BuildAndRecordGroupReadyV1 invokes a caller-owned staging implementation and
// commits only its bounded readiness proof.
func (c VectorPartitionLifecycleCoordinatorV1) BuildAndRecordGroupReadyV1(ctx context.Context, builder VectorPartitionLifecycleGroupBuilderV1, identity VectorPartitionLifecycleIdentityV1, group raftcluster.GroupID) (VectorPartitionLifecycleRecordV1, error) {
	if builder == nil {
		return VectorPartitionLifecycleRecordV1{}, errors.New("raftplacement: lifecycle group builder is required")
	}
	ready, err := builder.BuildAndStageVectorPartitionGroupV1(ctx, identity, group)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	if ready.GroupID != group {
		return VectorPartitionLifecycleRecordV1{}, fmt.Errorf("raftplacement: group builder returned wrong group")
	}
	return c.RecordGroupReadyV1(ctx, identity, ready)
}

func (c VectorPartitionLifecycleCoordinatorV1) PrepareV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1) (VectorPartitionLifecycleRecordV1, error) {
	r, ok := c.Authority.VectorPartitionLifecycleRecordV1(identity)
	if !ok {
		return VectorPartitionLifecycleRecordV1{}, ErrVectorPartitionLifecycleGuard
	}
	digest, err := VectorPartitionLifecycleReadySetDigestV1(r.Identity, r.RequiredGroups, r.ReadyGroups)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	return c.Submit(ctx, VectorPartitionLifecycleCommandV1{Kind: VectorPartitionLifecyclePrepareV1, ExpectedRevision: r.Revision, ExpectedState: r.State, Identity: identity, ReadySetDigest: digest})
}

func (c VectorPartitionLifecycleCoordinatorV1) ActivateV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1) (VectorPartitionLifecycleRecordV1, error) {
	r, ok := c.Authority.VectorPartitionLifecycleRecordV1(identity)
	if !ok {
		return VectorPartitionLifecycleRecordV1{}, ErrVectorPartitionLifecycleGuard
	}
	cmd := VectorPartitionLifecycleCommandV1{Kind: VectorPartitionLifecycleActivateV1, ExpectedRevision: r.Revision, ExpectedState: r.State, Identity: identity, PreviousActiveGeneration: r.PreviousActiveGeneration, MutationEpoch: r.MutationEpoch}
	for _, status := range c.Authority.VectorPartitionLifecycleStatusesV1() {
		if status.Active && status.Identity.Index == identity.Index {
			cmd.PreviousActiveGeneration, cmd.PreviousActiveRevision = status.Identity.Generation, status.Revision
		}
	}
	return c.Submit(ctx, cmd)
}

func (c VectorPartitionLifecycleCoordinatorV1) transitionV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, kind VectorPartitionLifecycleCommandKindV1, mutate func(*VectorPartitionLifecycleCommandV1)) (VectorPartitionLifecycleRecordV1, error) {
	r, ok := c.Authority.VectorPartitionLifecycleRecordV1(identity)
	if !ok {
		return VectorPartitionLifecycleRecordV1{}, ErrVectorPartitionLifecycleGuard
	}
	cmd := VectorPartitionLifecycleCommandV1{Kind: kind, ExpectedRevision: r.Revision, ExpectedState: r.State, Identity: identity}
	if mutate != nil {
		mutate(&cmd)
	}
	return c.Submit(ctx, cmd)
}

func (c VectorPartitionLifecycleCoordinatorV1) AbortV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, reason string) (VectorPartitionLifecycleRecordV1, error) {
	return c.transitionV1(ctx, identity, VectorPartitionLifecycleAbortBuildV1, func(x *VectorPartitionLifecycleCommandV1) { x.Reason = reason })
}
func (c VectorPartitionLifecycleCoordinatorV1) RetireV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1) (VectorPartitionLifecycleRecordV1, error) {
	return c.transitionV1(ctx, identity, VectorPartitionLifecycleRetireV1, nil)
}
func (c VectorPartitionLifecycleCoordinatorV1) MarkCleanableV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, refs VectorPartitionLifecycleReferencesV1) (VectorPartitionLifecycleRecordV1, error) {
	return c.transitionV1(ctx, identity, VectorPartitionLifecycleMarkCleanableV1, func(x *VectorPartitionLifecycleCommandV1) { x.References = refs })
}
func (c VectorPartitionLifecycleCoordinatorV1) RecordGroupCleanupV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, group raftcluster.GroupID) (VectorPartitionLifecycleRecordV1, error) {
	return c.transitionV1(ctx, identity, VectorPartitionLifecycleRecordGroupCleanupV1, func(x *VectorPartitionLifecycleCommandV1) { x.GroupID = group })
}
func (c VectorPartitionLifecycleCoordinatorV1) CompleteCleanupV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1) (VectorPartitionLifecycleRecordV1, error) {
	return c.transitionV1(ctx, identity, VectorPartitionLifecycleCompleteCleanupV1, nil)
}

func (c VectorPartitionLifecycleCoordinatorV1) RecoveryStatusV1() ([]VectorPartitionLifecycleAuthorityStatusV1, []VectorPartitionLifecycleMutationFenceStatusV1, error) {
	if c.Authority == nil {
		return nil, nil, ErrCatalogMetaUnavailable
	}
	return c.Authority.VectorPartitionLifecycleStatusesV1(), c.Authority.VectorPartitionLifecycleMutationFencesV1(), nil
}
