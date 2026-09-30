package nativewire

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// The immutable branch is opened only after a replicated ACTIVE record covers
// every physical owner. A config digest or locally prepared scope alone is not
// serving authority. Mutable fixed-peer construction remains unchanged.
func (r *fixedPeerVectorRuntimeV1) requireCurrentImmutableDBV1() error {
	if r == nil || r.parent == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	data := r.parent.data[r.dataGroup]
	if data == nil || data.fsm == nil || !data.fsm.HasCurrentDBV1(data.db) {
		return ErrFixedPeerVectorProofStaleV1
	}
	return nil
}

// Observation must never warm durable assets or bind a shard listener.
func (r *fixedPeerVectorRuntimeV1) observeImmutableTopologyV1(ctx context.Context) (*VectorPartitionProductionTopologyV1, raftplacement.VectorPartitionLifecycleRecordV1, error) {
	var zero raftplacement.VectorPartitionLifecycleRecordV1
	if err := r.requireCurrentImmutableDBV1(); err != nil {
		return nil, zero, err
	}
	r.initMu.Lock()
	topology, source := r.topology, r.source
	r.initMu.Unlock()
	if topology == nil || source == nil {
		return nil, zero, ErrFixedPeerVectorUnavailableV1
	}
	record, err := r.parent.immutableActiveVectorRecordV1(ctx, fixedPeerVectorOwnerGroupsV1(r.parent.config.Vector.Placement))
	if err != nil {
		return nil, zero, err
	}
	if err := r.requireCurrentImmutableDBV1(); err != nil {
		return nil, zero, err
	}
	return topology, record, nil
}

func (r *fixedPeerVectorRuntimeV1) ensureImmutableTopologyV1(ctx context.Context) (*VectorPartitionProductionTopologyV1, error) {
	if r == nil || r.parent == nil || r.parent.config.Vector == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.requireCurrentImmutableDBV1(); err != nil {
		return nil, err
	}
	vector := r.parent.config.Vector
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	// A cached topology is not a cached serving grant. Every public entry must
	// still observe the current ACTIVE record and complete ready set.
	_, err := r.parent.immutableActiveVectorRecordV1(ctx, owners)
	if err != nil {
		return nil, err
	}
	r.initMu.Lock()
	defer r.initMu.Unlock()
	if r.topology != nil {
		if err := r.requireCurrentImmutableDBV1(); err != nil {
			return nil, err
		}
		return r.topology, nil
	}
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if err := resolved.ValidateVectorPartitionPlacementV1(vector.Placement); err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	collection, err := r.manager.OpenCollection(vector.Collection.Collection)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if err := fixedPeerVectorImmutableDefinitionV1(collection.MetaView(), vector.Identity); err != nil {
		return nil, err
	}
	manifest, scope, err := collection.PreparedVectorPartitionScopedManifestWithContextV1(ctx, vector.Manifest.IndexName, vector.Manifest.Generation)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if scope.HostedGroup != string(r.dataGroup) || scope.ManifestDigest != vector.Identity.Immutable.ManifestDigest ||
		scope.PlacementDigest != vector.Identity.Immutable.PlacementDigest {
		return nil, ErrFixedPeerVectorProofStaleV1
	}
	localOwner := slices.Contains(owners, r.dataGroup)
	if !scope.Router && !localOwner {
		return nil, ErrFixedPeerVectorWrongOwnerV1
	}
	routerAuthority := fixedPeerVectorImmutableRouterAuthorityV1{runtime: r.parent, hosted: r.dataGroup}
	if scope.Router {
		if err := routerAuthority.ValidateVectorPartitionScopedRouterV1(ctx, manifest, scope); err != nil {
			return nil, err
		}
	}

	var lifecycle VectorPartitionReplicatedLifecycleAuthorityV1 = r.parent
	if r.parent.meta != nil {
		// Keep voter search validation and its hot-path cost unchanged.
		lifecycle, err = NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(r.parent.authority, r.parent)
		if err != nil {
			return nil, err
		}
	}
	endpoints := make(map[raftcluster.GroupID]string, len(owners))
	nodeEndpoints := make(map[raftcluster.GroupID]map[raftcluster.NodeID]string, len(owners))
	for _, owner := range owners {
		group, ok := resolved.Group(owner)
		if !ok || group.LeaderHint == "" || vector.ShardAddresses[owner][group.LeaderHint] == "" {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, fmt.Errorf("owner group %q has no configured leader endpoint", owner))
		}
		endpoints[owner] = vector.ShardAddresses[owner][group.LeaderHint]
		nodeEndpoints[owner] = make(map[raftcluster.NodeID]string, len(vector.ShardAddresses[owner]))
		for node, endpoint := range vector.ShardAddresses[owner] {
			nodeEndpoints[owner][node] = endpoint
		}
		if r.dataGroup == owner {
			endpoints[owner] = vector.ShardAddresses[owner][r.parent.config.NodeID]
		}
	}
	peerTransport := r.parent.PeerTransportV1()
	if r.parent.config.Credentials != nil && peerTransport == nil {
		return nil, errPeerAuthenticationV1
	}
	topologyOptions := VectorPartitionProductionTopologyOptionsV1{
		ConstructionContext: ctx,
		Catalog:             resolved,
		Placement:           vector.Placement,
		RouterSource:        VectorPartitionImmutableCoordinatorRouterSourceV1{VectorPartitionCoordinatorRouterSourceV1: fixedPeerVectorScopedRouterSourceV1{collection: collection, authority: routerAuthority}},
		ReplicatedLifecycle: lifecycle,
		Endpoints:           endpoints,
		NodeEndpoints:       nodeEndpoints,
		PeerTransport:       peerTransport,
		transportLeaderResolver: func(ctx context.Context, group raftcluster.GroupID, _ raftcluster.NodeID) (raftcluster.NodeID, error) {
			leader, err := r.parent.immutableVectorOwnerLeaderV1(ctx, resolved, group)
			if err != nil {
				return "", err
			}
			if nodeEndpoints[group][leader] == "" {
				return "", ErrFixedPeerVectorWrongOwnerV1
			}
			return leader, nil
		},
		CoordinatorLimits: DefaultVectorPartitionCoordinatorLimitsV1(),
		ShardLimits:       DefaultVectorPartitionShardSearchLimitsV1(),
		ShardIdleTimeout:  r.parent.config.RequestTimeout,
	}
	var source *CollectionVectorPartitionGenerationSourceV1
	var shardListener net.Listener
	if localOwner {
		ownerAuthority, err := newFixedPeerVectorScopedOwnerAuthorityV1(r.parent, r.dataGroup)
		if err != nil {
			return nil, err
		}
		if err := ownerAuthority.ValidateVectorPartitionScopedOwnerV1(ctx, manifest, scope); err != nil {
			return nil, err
		}
		data := r.parent.data[r.dataGroup]
		readCoordinator, err := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{{
			GroupID: r.dataGroup, NodeID: r.parent.config.NodeID, ReadIndexProvider: data.provider, AppliedIndexWaiter: data.fsm,
		}})
		if err != nil {
			return nil, err
		}
		source, err = NewCollectionVectorPartitionGenerationSourceForScopedOwnerV1(collection, vector.Collection, lifecycle, ownerAuthority, r.dataGroup)
		if err != nil {
			return nil, err
		}
		shardService, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
			Catalog: resolved, Placement: vector.Placement, LocalNodeID: r.parent.config.NodeID, LocalGroupID: r.dataGroup,
			ReadCoordinator: readCoordinator, GenerationSource: source, Limits: DefaultVectorPartitionShardSearchLimitsV1(),
		})
		if err != nil {
			_ = source.Close()
			return nil, err
		}
		shardService.postSearchGuard = r.requireCurrentImmutableDBV1
		shardListener, err = net.Listen("tcp", vector.ShardAddresses[r.dataGroup][r.parent.config.NodeID])
		if err != nil {
			_ = source.Close()
			return nil, err
		}
		topologyOptions.Shards = []VectorPartitionProductionShardV1{{
			GroupID: r.dataGroup, Listener: shardListener, Service: shardService, EndpointIdentity: r.parent.client.digest,
		}}
	}
	topology, err := NewVectorPartitionProductionTopologyV1(topologyOptions)
	if err != nil {
		if shardListener != nil {
			_ = shardListener.Close()
		}
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	if err := r.requireCurrentImmutableDBV1(); err != nil {
		_ = topology.Close()
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	r.collection, r.source, r.topology = collection, source, topology
	return topology, nil
}

// Only the catalog-voter router needs a public search backend/mutation
// coordinator. Owners serve directly through the existing topology and source.
func (r *fixedPeerVectorRuntimeV1) ensureImmutableBackendV1(ctx context.Context) (*VectorPartitionPublicBackendV1, error) {
	if r == nil || r.parent == nil || r.parent.config.Vector == nil ||
		r.parent.config.NodeID != r.parent.config.Vector.RouterNodeID || r.parent.meta == nil || r.parent.authority == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	topology, err := r.ensureImmutableTopologyV1(ctx)
	if err != nil {
		return nil, err
	}
	r.initMu.Lock()
	defer r.initMu.Unlock()
	if r.backend != nil {
		if err := r.requireCurrentImmutableDBV1(); err != nil {
			return nil, err
		}
		return r.backend, nil
	}
	vector := r.parent.config.Vector
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	record, err := r.parent.immutableActiveVectorRecordV1(ctx, owners)
	if err != nil {
		return nil, err
	}
	backend, err := NewVectorPartitionPublicBackendV1(VectorPartitionPublicBackendOptionsV1{
		Topology: topology, RequestBase: vector.RequestBase,
		Lifecycle: raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: r.parent.authority, Committer: r.parent.meta},
		ReadFence: r.parent, Identity: vector.Identity, RequiredGroups: owners,
		Builder: fixedPeerVectorImmutableNoBuildV1{}, MutationEpoch: record.MutationEpoch,
	})
	if err != nil {
		return nil, err
	}
	if err := r.requireCurrentImmutableDBV1(); err != nil {
		return nil, err
	}
	r.backend = backend
	return backend, nil
}

func (r *FixedPeerTCPRuntimeV1) immutableActiveVectorRecordV1(ctx context.Context, owners []raftcluster.GroupID) (raftplacement.VectorPartitionLifecycleRecordV1, error) {
	var zero raftplacement.VectorPartitionLifecycleRecordV1
	if r == nil || r.config.Vector == nil || len(owners) == 0 {
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	if r.meta == nil {
		_, record, err := r.consumerImmutableVectorCatalogV1(ctx, fixedPeerVectorCatalogActiveV1)
		if err == nil && !slices.Equal(record.RequiredGroups, owners) {
			err = ErrFixedPeerVectorProofStaleV1
		}
		return record, err
	}
	fence, err := r.catalogFence(ctx)
	if err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	identity := r.config.Vector.Identity
	if fence.AppliedIndex == 0 || fence.Epoch != identity.Index.CatalogEpoch || fence.Digest != identity.Index.CatalogDigest {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	// The exact-index snapshot reads catalog and lifecycle under one lock;
	// an apply between the fence and snapshot fails closed.
	snapshot, err := r.authority.VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
		ctx, fence.AppliedIndex, identity.Index.Collection, identity.Index.IndexName, identity.Generation,
		identity.Index.IndexDefinitionDigest, identity.Source.Generation, identity.Source.Checksum,
		identity.Source.SchemaHash, identity.Source.RowCount,
	)
	if err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if !sameScopedStageCatalogStatusV1(fence, snapshot.Catalog) || snapshot.Identity != identity {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	if err := r.validateImmutableActiveVectorRecordV1(snapshot.Record, owners); err != nil {
		return zero, err
	}
	return snapshot.Record, nil
}

// Compute only configuration-derived expectations. Fresh catalog records and
// their readiness/applied indices are validated separately on every request.
func fixedPeerImmutableVectorAssetDigestsV1(vector *FixedPeerTCPVectorConfigV1) map[raftcluster.GroupID]string {
	if vector == nil || vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return nil
	}
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	digests := make(map[raftcluster.GroupID]string, len(owners))
	for _, owner := range owners {
		digests[owner] = vectorPartitionM8GroupAssetSetDigestV1(string(owner), vector.Manifest)
	}
	return digests
}

func (r *FixedPeerTCPRuntimeV1) validateImmutableActiveVectorRecordV1(record raftplacement.VectorPartitionLifecycleRecordV1, owners []raftcluster.GroupID) error {
	if r == nil || r.config.Vector == nil {
		return ErrFixedPeerVectorProofStaleV1
	}
	identity := r.config.Vector.Identity
	if record.Identity != identity || record.Aborted ||
		record.State != raftplacement.VectorPartitionLifecycleActiveV1 || record.InvalidationEpoch != 0 ||
		!slices.Equal(record.RequiredGroups, owners) || len(record.ReadyGroups) != len(owners) || record.ReadySetDigest == "" {
		return ErrFixedPeerVectorProofStaleV1
	}
	for i, owner := range owners {
		ready := record.ReadyGroups[i]
		expected, ok := r.immutableVectorAssetDigests[owner]
		if !ok || expected == "" || ready.GroupID != owner || ready.AppliedIndex == 0 ||
			ready.AssetSetDigest != expected {
			return ErrFixedPeerVectorProofStaleV1
		}
	}
	digest, err := raftplacement.VectorPartitionLifecycleReadySetDigestV1(identity, owners, record.ReadyGroups)
	if err != nil || digest != record.ReadySetDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	return nil
}

func fixedPeerVectorImmutableDefinitionV1(meta collections.CollectionMeta, identity raftplacement.VectorPartitionLifecycleIdentityV1) error {
	// This V1 transport has no DropCollection path. A noninitial incarnation
	// needs separate durable incarnation authority before it may serve.
	if identity.Index.CollectionIncarnation != 1 || meta.Name != identity.Index.Collection.Collection {
		return ErrFixedPeerVectorProofStaleV1
	}
	for _, definition := range meta.VectorIndexes {
		if definition.Name == identity.Index.IndexName && definition.SchemaGeneration != 0 &&
			definition.SchemaGeneration == identity.Index.IndexEpoch &&
			collections.VectorIndexDefinitionDigestV1(definition) == identity.Index.IndexDefinitionDigest {
			return nil
		}
	}
	return ErrFixedPeerVectorProofStaleV1
}

type fixedPeerVectorScopedRouterSourceV1 struct {
	collection *collections.Collection
	authority  fixedPeerVectorImmutableRouterAuthorityV1
}

func (s fixedPeerVectorScopedRouterSourceV1) OpenVectorPartitionCoordinatorRouterV1(ctx context.Context, index string, generation uint64) (VectorPartitionCoordinatorRouterV1, error) {
	if s.collection == nil {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	router, _, err := s.collection.OpenPreparedVectorPartitionScopedRouterWithContextV1(ctx, index, generation, s.authority)
	return router, err
}

type fixedPeerVectorImmutableRouterAuthorityV1 struct {
	runtime *FixedPeerTCPRuntimeV1
	hosted  raftcluster.GroupID
}

func (a fixedPeerVectorImmutableRouterAuthorityV1) ValidateVectorPartitionScopedRouterV1(ctx context.Context, manifest collections.VectorPartitionManifestV1, scope collections.VectorPartitionLocalScopeV1) error {
	if a.runtime == nil || a.runtime.config.Vector == nil || !scope.Router || scope.HostedGroup != string(a.hosted) {
		return ErrFixedPeerVectorProofStaleV1
	}
	identity := a.runtime.config.Vector.Identity
	raw, err := collections.EncodeVectorPartitionManifestV1(manifest)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != identity.Immutable.ManifestDigest ||
		scope.ManifestDigest != identity.Immutable.ManifestDigest || scope.PlacementDigest != identity.Immutable.PlacementDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	placementDigest, err := collections.VectorPartitionPlacementDigestV1(manifest)
	if err != nil || placementDigest != identity.Immutable.PlacementDigest ||
		manifest.Collection != identity.Index.Collection.Collection || manifest.IndexName != identity.Index.IndexName ||
		manifest.IndexDefinitionDigest != identity.Index.IndexDefinitionDigest || manifest.Generation != identity.Generation ||
		manifest.SourceGeneration != identity.Source.Generation || manifest.SourceChecksum != identity.Source.Checksum ||
		manifest.SourceSchemaHash != identity.Source.SchemaHash || manifest.SourceRowCount != identity.Source.RowCount {
		return ErrFixedPeerVectorProofStaleV1
	}
	owners := fixedPeerVectorOwnerGroupsV1(a.runtime.config.Vector.Placement)
	_, err = a.runtime.immutableActiveVectorRecordV1(ctx, owners)
	return err
}

type fixedPeerVectorImmutableNoBuildV1 struct{}

func (fixedPeerVectorImmutableNoBuildV1) BuildAndStageVectorPartitionGroupV1(context.Context, raftplacement.VectorPartitionLifecycleIdentityV1, raftcluster.GroupID) (raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	return raftplacement.VectorPartitionLifecycleGroupReadyV1{}, ErrFixedPeerVectorUnavailableV1
}
