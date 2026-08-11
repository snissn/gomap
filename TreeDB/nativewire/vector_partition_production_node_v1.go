package nativewire

// This bounded node adapter assembles the production topology and public
// backend from an already-open retained collection. It exists so executables
// outside TreeDB can run the real Raft/read-index and public service path
// without importing TreeDB's internal consensus packages.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type VectorPartitionProductionNodeShardV1 struct {
	GroupID  string
	Listener net.Listener
}

type VectorPartitionProductionNodeOptionsV1 struct {
	Collection                 *collections.Collection
	Manifest                   collections.VectorPartitionManifestV1
	RouterSource               VectorPartitionCoordinatorRouterSourceV1
	AssetSetDigests            map[string]string
	GroupAppliedIndexes        map[string]uint64
	Database                   string
	Catalog                    string
	Endpoints                  map[string]string
	LocalShards                []VectorPartitionProductionNodeShardV1
	RequestBase                VectorPartitionCoordinatorRequestV1
	NodeID                     string
	EndpointIdentity           string
	RuntimeStats               func() VectorPartitionProcessRuntimeStatsV1
	CoordinatorLimits          VectorPartitionCoordinatorLimitsV1
	ShardLimits                VectorPartitionShardSearchLimitsV1
	TopologyDigest             string
	AuthorizationOverlayDigest string
	IndexedThrough             uint64
	StrictCapabilityKey        []byte
	MaxPinnedSessions          int
	MaxPinnedSessionAge        time.Duration
	MaxRetainedSnapshots       int
}

type VectorPartitionProductionNodeGroupEvidenceV1 struct {
	GroupID, LeaderID string
	AppliedIndex      uint64
}

type VectorPartitionProductionNodeV1 struct {
	groups   map[raftcluster.GroupID]*raftcluster.ThreeNodeHarness
	meta     *raftplacement.CatalogMetaLifecycleHarnessV1
	sources  []*CollectionVectorPartitionGenerationSourceV1
	topology *VectorPartitionProductionTopologyV1
	backend  *VectorPartitionPublicBackendV1
	evidence []VectorPartitionProductionNodeGroupEvidenceV1
}

type vectorPartitionProductionNodeBuilderV1 struct {
	groups map[raftcluster.GroupID]raftplacement.VectorPartitionLifecycleGroupReadyV1
}

func (b vectorPartitionProductionNodeBuilderV1) BuildAndStageVectorPartitionGroupV1(_ context.Context, _ raftplacement.VectorPartitionLifecycleIdentityV1, group raftcluster.GroupID) (raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	ready, ok := b.groups[group]
	if !ok {
		return raftplacement.VectorPartitionLifecycleGroupReadyV1{}, fmt.Errorf("nativewire: production node group %q is not staged", group)
	}
	return ready, nil
}

type vectorPartitionProductionNodeGenerationSourceV1 struct {
	source   VectorPartitionGenerationSourceV1
	manifest VectorPartitionPinnedManifestV1
}

func (s vectorPartitionProductionNodeGenerationSourceV1) PinVectorPartitionGenerationV1(ctx context.Context, index string, generation uint64) (VectorPartitionPinnedGenerationV1, error) {
	pinned, err := s.source.PinVectorPartitionGenerationV1(ctx, index, generation)
	if err != nil {
		return nil, err
	}
	if s.manifest.IndexName != index || s.manifest.Generation != generation {
		_ = pinned.Close()
		return nil, ErrVectorPartitionShardSearchGenerationMismatch
	}
	manifest := pinned.Manifest()
	manifest.IntegrityDigest = s.manifest.IntegrityDigest
	manifest.ReadySetDigest = s.manifest.ReadySetDigest
	manifest.Placements = slices.Clone(s.manifest.Placements)
	return vectorPartitionProductionNodePinnedGenerationV1{VectorPartitionPinnedGenerationV1: pinned, manifest: manifest}, nil
}

type vectorPartitionProductionNodePinnedGenerationV1 struct {
	VectorPartitionPinnedGenerationV1
	manifest VectorPartitionPinnedManifestV1
}

func (p vectorPartitionProductionNodePinnedGenerationV1) Manifest() VectorPartitionPinnedManifestV1 {
	out := p.manifest
	out.Placements = slices.Clone(p.manifest.Placements)
	return out
}

func NewVectorPartitionProductionNodeV1(ctx context.Context, opts VectorPartitionProductionNodeOptionsV1) (_ *VectorPartitionProductionNodeV1, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Collection == nil || opts.RouterSource == nil || opts.Manifest.State != "ready" || len(opts.Endpoints) == 0 || len(opts.LocalShards) == 0 || opts.Database == "" || opts.Catalog == "" || opts.NodeID == "" ||
		!isVectorPartitionShardSearchDigestV1(opts.TopologyDigest) || !isVectorPartitionShardSearchDigestV1(opts.AuthorizationOverlayDigest) || opts.IndexedThrough == 0 || len(opts.StrictCapabilityKey) != sha256.Size {
		return nil, errors.New("nativewire: production node options are incomplete")
	}
	groups := make([]string, 0, len(opts.Endpoints))
	for group, endpoint := range opts.Endpoints {
		if group == "" || endpoint == "" || opts.AssetSetDigests[group] == "" {
			return nil, errors.New("nativewire: production node endpoint or asset binding is incomplete")
		}
		groups = append(groups, group)
	}
	assetGroups, assetErr := vectorPartitionValidateAssetBindingsV1(opts.Manifest, opts.AssetSetDigests)
	if assetErr != nil {
		return nil, fmt.Errorf("nativewire: production node asset binding: %w", assetErr)
	}
	if len(opts.GroupAppliedIndexes) != 0 && len(opts.GroupAppliedIndexes) != len(opts.Endpoints) {
		return nil, errors.New("nativewire: production node shared applied-index binding is incomplete")
	}
	for group, applied := range opts.GroupAppliedIndexes {
		if opts.Endpoints[group] == "" || applied == 0 {
			return nil, errors.New("nativewire: production node shared applied-index binding is invalid")
		}
	}
	sort.Strings(groups)
	if len(assetGroups) != len(groups) {
		return nil, errors.New("nativewire: production node asset ownership differs from endpoints")
	}
	for i := range groups {
		if string(assetGroups[i]) != groups[i] {
			return nil, errors.New("nativewire: production node asset ownership differs from endpoints")
		}
	}
	local := make(map[raftcluster.GroupID]net.Listener, len(opts.LocalShards))
	for _, shard := range opts.LocalShards {
		group := raftcluster.GroupID(shard.GroupID)
		if group == "" || shard.Listener == nil || local[group] != nil || opts.Endpoints[shard.GroupID] == "" {
			return nil, errors.New("nativewire: production node local shard binding is invalid")
		}
		local[group] = shard.Listener
	}
	node := &VectorPartitionProductionNodeV1{groups: make(map[raftcluster.GroupID]*raftcluster.ThreeNodeHarness)}
	defer func() {
		if err != nil {
			_ = node.Close()
		}
	}()
	features := raftplacement.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	catalog := raftplacement.CatalogV1{Features: features}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection:            raftplacement.CollectionRefV1{Database: opts.Database, Catalog: opts.Catalog, Collection: opts.Manifest.Collection},
		IndexName:             opts.Manifest.IndexName,
		IndexDefinitionDigest: opts.Manifest.IndexDefinitionDigest,
		SourceGeneration:      opts.Manifest.SourceGeneration,
		SourceChecksum:        opts.Manifest.SourceChecksum,
		SourceSchemaHash:      opts.Manifest.SourceSchemaHash,
		SourceRowCount:        opts.Manifest.SourceRowCount,
		PartitionGeneration:   opts.Manifest.Generation,
		PartitionCount:        opts.Manifest.PartitionCount,
	}
	members := []raftcluster.NodeID{"node-a", "node-b", "node-c"}
	localApplied := make(map[raftcluster.GroupID]uint64, len(local))
	for i, groupName := range groups {
		group := raftcluster.GroupID(groupName)
		preferred := members[i%len(members)]
		if local[group] != nil {
			harness, openErr := raftcluster.OpenThreeNodeHarnessWithOptions(ctx, group, raftcluster.ThreeNodeHarnessOptions{PreferredLeader: preferred})
			if openErr != nil {
				return nil, openErr
			}
			node.groups[group] = harness
			if _, proofErr := harness.CommitBenchmarkProofV1(ctx); proofErr != nil {
				return nil, proofErr
			}
			proof, progress, proofErr := harness.ReadCoordinator().CoordinateRoutedReadIndex(ctx, raftcluster.ReadIndexBarrier{NodeID: harness.LeaderID(), GroupID: group})
			if proofErr != nil {
				return nil, fmt.Errorf("nativewire: production node group %q read proof: %w", group, proofErr)
			}
			if proof.EvidenceKind != raftcluster.ReadIndexEvidenceProduction || !proof.HasQuorum || progress.Index == 0 {
				return nil, fmt.Errorf("nativewire: production node group %q lacks production read proof: kind=%q quorum=%t index=%d", group, proof.EvidenceKind, proof.HasQuorum, progress.Index)
			}
			localApplied[group] = progress.Index
			node.evidence = append(node.evidence, VectorPartitionProductionNodeGroupEvidenceV1{GroupID: string(group), LeaderID: string(harness.LeaderID()), AppliedIndex: progress.Index})
		}
		catalog.Groups = append(catalog.Groups, raftplacement.GroupV1{ID: group, Members: slices.Clone(members), LeaderHint: preferred})
	}
	catalog.Placements = []raftplacement.CollectionPlacementV1{{Collection: placement.Collection, GroupID: raftcluster.GroupID(groups[0]), Mode: raftplacement.PlacementModeCollectionV1}}
	for _, part := range opts.Manifest.Placements {
		placement.Partitions = append(placement.Partitions, raftplacement.VectorPartitionGroupV1{PartitionID: part.PartitionID, GroupID: raftcluster.GroupID(part.GroupID)})
	}
	resolved, err := raftplacement.Validate(catalog)
	if err != nil {
		return nil, err
	}
	node.meta, err = raftplacement.OpenCatalogMetaLifecycleHarnessV1(ctx, raftplacement.CatalogMetaLifecycleHarnessOptionsV1{Catalog: catalog, Prefix: "production-node-" + opts.NodeID})
	if err != nil {
		return nil, err
	}
	metaStatus, ok := node.meta.LeaderAuthority().Status()
	if !ok {
		return nil, errors.New("nativewire: production node catalog authority is unavailable")
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: placement.Collection, CollectionIncarnation: 1, IndexName: placement.IndexName,
			IndexDefinitionDigest: placement.IndexDefinitionDigest, IndexEpoch: 1, CatalogEpoch: metaStatus.Epoch, CatalogDigest: metaStatus.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: placement.SourceGeneration, Checksum: placement.SourceChecksum, SchemaHash: placement.SourceSchemaHash, RowCount: placement.SourceRowCount,
		},
		Generation: placement.PartitionGeneration,
	}
	lifecycle := node.meta.LifecycleCoordinator()
	requiredGroups := make([]raftcluster.GroupID, 0, len(groups))
	for _, group := range groups {
		requiredGroups = append(requiredGroups, raftcluster.GroupID(group))
	}
	if _, err = lifecycle.BeginBuildV1(ctx, identity, requiredGroups, 0, 1); err != nil {
		return nil, err
	}
	readyGroups := make(map[raftcluster.GroupID]raftplacement.VectorPartitionLifecycleGroupReadyV1, len(groups))
	if len(opts.GroupAppliedIndexes) == 0 && len(localApplied) == len(groups) {
		opts.GroupAppliedIndexes = make(map[string]uint64, len(groups))
		for group, applied := range localApplied {
			opts.GroupAppliedIndexes[string(group)] = applied
		}
	}
	for _, group := range requiredGroups {
		applied := opts.GroupAppliedIndexes[string(group)]
		if applied == 0 {
			return nil, fmt.Errorf("nativewire: production node group %q lacks a shared applied index", group)
		}
		if localIndex := localApplied[group]; localIndex != 0 && localIndex < applied {
			return nil, fmt.Errorf("nativewire: production node group %q applied index=%d is behind shared %d", group, localIndex, applied)
		}
		ready := raftplacement.VectorPartitionLifecycleGroupReadyV1{GroupID: group, AppliedIndex: applied, AssetSetDigest: opts.AssetSetDigests[string(group)]}
		readyGroups[group] = ready
		if _, err = lifecycle.RecordGroupReadyV1(ctx, identity, ready); err != nil {
			return nil, err
		}
	}
	if _, err = lifecycle.PrepareV1(ctx, identity); err != nil {
		return nil, err
	}
	active, err := lifecycle.ActivateV1(ctx, identity)
	if err != nil {
		return nil, err
	}
	replicated, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(node.meta.LeaderAuthority(), node.meta.LeaderFence())
	if err != nil {
		return nil, err
	}
	pinnedManifest := vectorPartitionProductionNodePinnedManifestV1(opts.Manifest, active.ReadySetDigest)
	shards := make([]VectorPartitionProductionShardV1, 0, len(local))
	generationSources := make(map[raftcluster.GroupID]VectorPartitionGenerationSourceV1, len(local))
	for group, listener := range local {
		group := group
		source, sourceErr := NewCollectionVectorPartitionGenerationSourceForReplicatedLifecycleV1(opts.Collection, placement.Collection, replicated)
		if sourceErr != nil {
			return nil, sourceErr
		}
		node.sources = append(node.sources, source)
		generationSource := vectorPartitionProductionNodeGenerationSourceV1{source: source, manifest: pinnedManifest}
		generationSources[group] = generationSource
		service, serviceErr := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
			Catalog: resolved, Placement: placement, LocalNodeID: node.groups[group].LeaderID(), LocalGroupID: group,
			ReadCoordinator: node.groups[group].ReadCoordinator(), GenerationSource: generationSource,
			Limits: opts.ShardLimits,
		})
		if serviceErr != nil {
			return nil, serviceErr
		}
		shards = append(shards, VectorPartitionProductionShardV1{
			GroupID: group, Listener: listener, Service: service, EndpointIdentity: opts.EndpointIdentity,
			EndpointIdentityProvider: func() VectorPartitionShardEndpointIdentityV1 {
				var runtimeStats VectorPartitionProcessRuntimeStatsV1
				if opts.RuntimeStats != nil {
					runtimeStats = opts.RuntimeStats()
				}
				return VectorPartitionShardEndpointIdentityV1{
					Version: 1, GroupID: string(group), InstanceIdentity: opts.EndpointIdentity,
					CatalogMetaReadStats: node.meta.LeaderFence().CatalogMetaLinearizableReadStatsV1(),
					ProcessRuntimeStats:  runtimeStats,
				}
			},
		})
	}
	endpoints := make(map[raftcluster.GroupID]string, len(opts.Endpoints))
	nodeEndpoints := make(map[raftcluster.GroupID]map[raftcluster.NodeID]string, len(opts.Endpoints))
	for group, endpoint := range opts.Endpoints {
		groupID := raftcluster.GroupID(group)
		endpoints[groupID] = endpoint
		_, port, splitErr := net.SplitHostPort(endpoint)
		if splitErr != nil {
			return nil, splitErr
		}
		preferred := members[slices.Index(groups, group)%len(members)]
		nodeEndpoints[groupID] = make(map[raftcluster.NodeID]string, len(members))
		for memberIndex, member := range members {
			memberEndpoint := net.JoinHostPort("192.0.2."+strconv.Itoa(1+slices.Index(groups, group)*len(members)+memberIndex), port)
			if member == preferred {
				memberEndpoint = endpoint
			}
			nodeEndpoints[groupID][member] = memberEndpoint
		}
	}
	node.topology, err = NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{
		ConstructionContext: ctx,
		Catalog:             resolved, Placement: placement, RouterSource: opts.RouterSource, ReplicatedLifecycle: replicated,
		Endpoints: endpoints, NodeEndpoints: nodeEndpoints, Shards: shards, CoordinatorLimits: opts.CoordinatorLimits, ShardLimits: opts.ShardLimits,
		ServingSnapshot: &VectorPartitionServingSnapshotPublisherOptionsV1{
			Authority: replicated, GenerationSources: generationSources, TopologyDigest: opts.TopologyDigest,
			AuthorizationOverlayDigest: opts.AuthorizationOverlayDigest, IndexedThrough: opts.IndexedThrough,
			MaxPinnedSessions: opts.MaxPinnedSessions, MaxPinnedSessionAge: opts.MaxPinnedSessionAge, MaxRetainedSnapshots: opts.MaxRetainedSnapshots,
		},
		StrictCapabilityKey: opts.StrictCapabilityKey,
	})
	if err != nil {
		return nil, err
	}
	requestBase := opts.RequestBase
	requestBase.Database, requestBase.Catalog, requestBase.Collection = opts.Database, opts.Catalog, opts.Manifest.Collection
	requestBase.IndexName, requestBase.IndexDefinitionDigest = opts.Manifest.IndexName, opts.Manifest.IndexDefinitionDigest
	if requestBase.RouterCandidateBudget > len(opts.Manifest.Representatives) {
		requestBase.RouterCandidateBudget = len(opts.Manifest.Representatives)
	}
	if requestBase.RouterMode == "" {
		requestBase.RouterMode = collections.VectorPartitionRouterModeApproxV1
	}
	if requestBase.StatsMode == "" {
		requestBase.StatsMode = VectorPartitionShardSearchStatsBasicV1
	}
	node.backend, err = NewVectorPartitionPublicBackendV1(VectorPartitionPublicBackendOptionsV1{
		Topology: node.topology, RequestBase: requestBase, Lifecycle: lifecycle, ReadFence: node.meta.LeaderFence(), Identity: identity,
		RequiredGroups: requiredGroups, Builder: vectorPartitionProductionNodeBuilderV1{groups: readyGroups}, MutationEpoch: 1,
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(node.evidence, func(i, j int) bool { return node.evidence[i].GroupID < node.evidence[j].GroupID })
	return node, nil
}

func (n *VectorPartitionProductionNodeV1) PublicBackendV1() *VectorPartitionPublicBackendV1 {
	if n == nil {
		return nil
	}
	return n.backend
}

func (n *VectorPartitionProductionNodeV1) GroupEvidenceV1() []VectorPartitionProductionNodeGroupEvidenceV1 {
	if n == nil {
		return nil
	}
	return slices.Clone(n.evidence)
}

// PublishSearchSnapshotV1 publishes one complete update watermark and returns
// the local read-your-writes token accepted by SearchFast.
func (n *VectorPartitionProductionNodeV1) PublishSearchSnapshotV1(ctx context.Context, indexedThrough uint64, authorizationDigest string, deniedDocumentIDs []string) (public.IndexedWriteTokenV1, error) {
	if n == nil || n.topology == nil {
		return public.IndexedWriteTokenV1{}, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	return n.topology.PublishSearchSnapshotV1(ctx, indexedThrough, authorizationDigest, deniedDocumentIDs)
}

func (n *VectorPartitionProductionNodeV1) Close() error {
	if n == nil {
		return nil
	}
	var errs []error
	if n.topology != nil {
		errs = append(errs, n.topology.Close())
		n.topology = nil
	}
	for _, source := range n.sources {
		errs = append(errs, source.Close())
	}
	for _, group := range n.groups {
		errs = append(errs, group.Close())
	}
	if n.meta != nil {
		errs = append(errs, n.meta.Close())
		n.meta = nil
	}
	return errors.Join(errs...)
}

func vectorPartitionProductionNodePinnedManifestV1(manifest collections.VectorPartitionManifestV1, readySetDigest string) VectorPartitionPinnedManifestV1 {
	return VectorPartitionPinnedManifestV1{
		State: manifest.State, Collection: manifest.Collection, IndexName: manifest.IndexName, IndexDefinitionDigest: manifest.IndexDefinitionDigest, IntegrityDigest: manifest.IntegrityDigest,
		SourceGeneration: manifest.SourceGeneration, SourceChecksum: manifest.SourceChecksum, SourceSchemaHash: manifest.SourceSchemaHash, SourceRowCount: manifest.SourceRowCount,
		Generation: manifest.Generation, RouterGeneration: manifest.RouterGeneration, ReadySetDigest: readySetDigest, PartitionCount: manifest.PartitionCount,
		Placements: slices.Clone(manifest.Placements),
	}
}
