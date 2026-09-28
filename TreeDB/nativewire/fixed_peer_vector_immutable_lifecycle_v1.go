package nativewire

import (
	"context"
	"errors"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// The control request names an action, never an identity, asset digest, or
// readiness receipt. All three are derived from committed catalog authority
// and locally verified bytes by the receiving fixed peer.
type fixedPeerVectorLifecycleActionV1 string

const (
	fixedPeerVectorLifecycleEnsureImmutableV1 fixedPeerVectorLifecycleActionV1 = "ensure-immutable"
	fixedPeerVectorLifecycleStageImmutableV1  fixedPeerVectorLifecycleActionV1 = "stage-immutable"
	fixedPeerVectorLifecycleWarmImmutableV1   fixedPeerVectorLifecycleActionV1 = "warm-immutable"
)

type fixedPeerVectorLifecycleRequestV1 struct {
	Action fixedPeerVectorLifecycleActionV1 `json:"action"`
}

// EnsureImmutableVectorLifecycleV1 asks the configured catalog leader to
// establish one source-verified BUILD, have each physical owner stage its own
// assets, and commit the complete ready set before ACTIVE. A lost response is
// commit-ambiguous; callers must inspect catalog state before any retry.
func (c *FixedPeerTCPClientV1) EnsureImmutableVectorLifecycleV1(ctx context.Context) (raftplacement.CatalogMetaStatusV1, error) {
	if c == nil || c.config.Vector == nil || c.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return raftplacement.CatalogMetaStatusV1{}, ErrFixedPeerVectorUnavailableV1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	leader, err := c.leader(ctx, c.config.Catalog)
	if err != nil {
		return raftplacement.CatalogMetaStatusV1{}, err
	}
	reply, err := c.call(ctx, leader, "vector-lifecycle", fixedPeerRequestV1{
		VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorLifecycleEnsureImmutableV1},
	}, true)
	return reply.Catalog, err
}

func (r *FixedPeerTCPRuntimeV1) ensureImmutableVectorLifecycleLeaderV1(ctx context.Context) (raftplacement.CatalogMetaStatusV1, error) {
	var zero raftplacement.CatalogMetaStatusV1
	if r == nil || r.closed.Load() || r.draining.Load() || r.meta == nil || r.vector == nil || r.config.Vector == nil ||
		r.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	status := r.meta.RuntimeStatusV1()
	if status.State != "Leader" || status.LeaderID != r.config.NodeID {
		return zero, raftcluster.ErrNotLeader
	}
	vector := r.config.Vector
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil {
		return zero, err
	}
	placement, ok := resolved.Placement(vector.Collection)
	if !ok || placement.Mode != raftplacement.PlacementModeCollectionV1 || r.vector.dataGroup != placement.GroupID {
		// The current V1 source callback requires colocated meta and source-data
		// leaders. A catalog-only leader cannot attest another node's source.
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	if err := r.vector.requireCurrentImmutableDBV1(); err != nil {
		return zero, err
	}
	collection, err := r.vector.manager.OpenCollection(vector.Collection.Collection)
	if err != nil {
		return zero, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if err := fixedPeerVectorImmutableDefinitionV1(collection.MetaView(), vector.Identity); err != nil {
		return zero, err
	}
	prepareSource, err := NewVectorPartitionImmutableSourceHolderPreparationV1(r, vector.Collection)
	if err != nil {
		return zero, err
	}
	lifecycle := raftplacement.VectorPartitionLifecycleCoordinatorV1{
		Authority: r.authority, Committer: r.meta, PrepareImmutableV1: prepareSource,
	}
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	mutationEpoch, err := lifecycle.BuildSourceMutationEpochV1(vector.Collection)
	if err != nil {
		return zero, err
	}
	record, err := lifecycle.BeginBuildV1(ctx, vector.Identity, owners, 0, mutationEpoch)
	if err != nil {
		return zero, err
	}
	if record.State == raftplacement.VectorPartitionLifecycleActiveV1 {
		if _, err := r.immutableActiveVectorRecordV1(ctx, owners); err != nil {
			return zero, err
		}
		if err := r.warmImmutableVectorNodesV1(ctx, resolved, owners); err != nil {
			return zero, err
		}
		result, ok := r.authority.Status()
		if !ok {
			return zero, raftplacement.ErrCatalogMetaUnavailable
		}
		return result, nil
	}
	// The router is a local prepared asset too, but it is not a data-group
	// readiness vote. Its stage must complete before activation.
	if _, err := r.stageImmutableVectorOnNodeV1(ctx, vector.RouterNodeID); err != nil {
		return zero, err
	}
	for _, owner := range owners {
		group, ok := resolved.Group(owner)
		if !ok || group.LeaderHint == "" {
			return zero, ErrFixedPeerVectorWrongOwnerV1
		}
		ready, err := r.stageImmutableVectorOnNodeV1(ctx, group.LeaderHint)
		if err != nil {
			return zero, err
		}
		if ready == nil || ready.GroupID != owner || ready.AppliedIndex == 0 ||
			ready.AssetSetDigest != vectorPartitionM8GroupAssetSetDigestV1(string(owner), vector.Manifest) {
			return zero, ErrFixedPeerVectorProofStaleV1
		}
		record, err = lifecycle.RecordGroupReadyV1(ctx, vector.Identity, *ready)
		if err != nil {
			return zero, err
		}
	}
	if record.State == raftplacement.VectorPartitionLifecycleStagedV1 {
		record, err = lifecycle.PrepareV1(ctx, vector.Identity)
		if err != nil {
			return zero, err
		}
	}
	if record.State == raftplacement.VectorPartitionLifecyclePreparedV1 {
		record, err = lifecycle.ActivateV1(ctx, vector.Identity)
		if err != nil {
			return zero, err
		}
	}
	if record.State != raftplacement.VectorPartitionLifecycleActiveV1 {
		return zero, raftplacement.ErrVectorPartitionLifecycleState
	}
	if _, err := r.immutableActiveVectorRecordV1(ctx, owners); err != nil {
		return zero, err
	}
	if err := r.warmImmutableVectorNodesV1(ctx, resolved, owners); err != nil {
		return zero, err
	}
	result, ok := r.authority.Status()
	if !ok {
		return zero, raftplacement.ErrCatalogMetaUnavailable
	}
	return result, nil
}

// ACTIVE is durable before listeners can be opened. Warming through the
// existing authenticated control path makes success mean that every owner can
// actually accept a shard request; an interrupted warm remains safe to retry.
func (r *FixedPeerTCPRuntimeV1) warmImmutableVectorNodesV1(ctx context.Context, resolved raftplacement.ResolvedCatalogV1, owners []raftcluster.GroupID) error {
	for _, owner := range owners {
		group, ok := resolved.Group(owner)
		if !ok || group.LeaderHint == "" {
			return ErrFixedPeerVectorWrongOwnerV1
		}
		if err := r.warmImmutableVectorOnNodeV1(ctx, group.LeaderHint); err != nil {
			return err
		}
	}
	// Ingress is reported ready only after each owner has opened its shard
	// listener. Topology construction itself does not connect to the owners.
	return r.warmImmutableVectorOnNodeV1(ctx, r.config.Vector.RouterNodeID)
}

func (r *FixedPeerTCPRuntimeV1) warmImmutableVectorOnNodeV1(ctx context.Context, node raftcluster.NodeID) error {
	if node == "" {
		return ErrFixedPeerVectorWrongOwnerV1
	}
	if node == r.config.NodeID {
		return r.warmImmutableVectorLocalV1(ctx)
	}
	_, err := r.client.call(ctx, node, "vector-lifecycle", fixedPeerRequestV1{
		VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorLifecycleWarmImmutableV1},
	}, true)
	return err
}

func (r *FixedPeerTCPRuntimeV1) warmImmutableVectorLocalV1(ctx context.Context) error {
	if r == nil || r.closed.Load() || r.draining.Load() || r.vector == nil || r.config.Vector == nil ||
		r.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return ErrFixedPeerVectorUnavailableV1
	}
	_, err := r.vector.ensureImmutableBackendV1(ctx)
	return err
}

func (r *FixedPeerTCPRuntimeV1) stageImmutableVectorOnNodeV1(ctx context.Context, node raftcluster.NodeID) (*raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	if node == "" {
		return nil, ErrFixedPeerVectorWrongOwnerV1
	}
	if node == r.config.NodeID {
		return r.stageImmutableVectorLocalV1(ctx)
	}
	reply, err := r.client.call(ctx, node, "vector-lifecycle", fixedPeerRequestV1{
		VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorLifecycleStageImmutableV1},
	}, true)
	return reply.VectorReady, err
}

func (r *FixedPeerTCPRuntimeV1) stageImmutableVectorLocalV1(ctx context.Context) (*raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	if r == nil || r.closed.Load() || r.draining.Load() || r.vector == nil || r.config.Vector == nil ||
		r.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	vector := r.config.Vector
	group := r.vector.dataGroup
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	owner := slices.Contains(owners, group)
	router := r.config.NodeID == vector.RouterNodeID
	if (!owner && !router) || (owner && router) {
		return nil, ErrFixedPeerVectorWrongOwnerV1
	}
	if err := r.vector.requireCurrentImmutableDBV1(); err != nil {
		return nil, err
	}
	collection, err := r.vector.manager.OpenCollection(vector.Collection.Collection)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if err := fixedPeerVectorImmutableDefinitionV1(collection.MetaView(), vector.Identity); err != nil {
		return nil, err
	}
	scope := collections.VectorPartitionLocalScopeV1{
		HostedGroup: string(group), Router: router,
		ManifestDigest:  vector.Identity.Immutable.ManifestDigest,
		PlacementDigest: vector.Identity.Immutable.PlacementDigest,
	}
	authority, err := newFixedPeerVectorScopedStageAuthorityV1(r, group)
	if err != nil {
		return nil, err
	}
	resources, err := collection.CaptureVectorPartitionScopedExistingAssetsV1(vector.Manifest, scope)
	if err != nil {
		return nil, err
	}
	if err := collection.StageVectorPartitionScopedManifestWithContextV1(ctx, vector.Manifest, scope, resources, authority); err != nil {
		return nil, err
	}
	if err := r.vector.requireCurrentImmutableDBV1(); err != nil {
		return nil, err
	}
	if router {
		return nil, nil
	}
	data := r.data[group]
	if data == nil || data.provider == nil || data.fsm == nil {
		return nil, ErrFixedPeerVectorWrongOwnerV1
	}
	reads, err := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{{
		GroupID: group, NodeID: r.config.NodeID, ReadIndexProvider: data.provider, AppliedIndexWaiter: data.fsm,
	}})
	if err != nil {
		return nil, err
	}
	proof, _, err := reads.CoordinateRoutedReadIndex(ctx, raftcluster.ReadIndexBarrier{NodeID: r.config.NodeID, GroupID: group})
	if err != nil || proof.Index == 0 {
		return nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	ready := &raftplacement.VectorPartitionLifecycleGroupReadyV1{
		GroupID: group, AppliedIndex: proof.Index,
		AssetSetDigest: vectorPartitionM8GroupAssetSetDigestV1(string(group), vector.Manifest),
	}
	return ready, nil
}
