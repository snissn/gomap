package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"slices"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

var (
	ErrFixedPeerVectorProofMissingV1 = errors.New("nativewire: fixed-peer vector mutation proof missing")
	ErrFixedPeerVectorProofStaleV1   = errors.New("nativewire: fixed-peer vector mutation proof stale")
	ErrFixedPeerVectorWrongOwnerV1   = errors.New("nativewire: fixed-peer vector mutation wrong owner")
	ErrFixedPeerVectorUnavailableV1  = errors.New("nativewire: fixed-peer vector runtime unavailable")
	ErrFixedPeerVectorDocumentV1     = errors.New("nativewire: fixed-peer vector document does not match routed vector")
)

// FixedPeerTCPVectorConfigV1 is shared byte-for-byte by every fixed-peer
// process. Per-process listeners are selected from the node-keyed maps so the
// fixed-peer configuration digest cannot silently diverge across daemons.
type FixedPeerTCPVectorConfigV1 struct {
	Collection                 raftplacement.CollectionRefV1
	Catalog                    raftplacement.CatalogV1
	Manifest                   collections.VectorPartitionManifestV1
	Placement                  raftplacement.VectorPartitionPlacementRecordV1
	Identity                   raftplacement.VectorPartitionLifecycleIdentityV1
	PublicAddresses            map[raftcluster.NodeID]string
	ShardAddresses             map[raftcluster.GroupID]map[raftcluster.NodeID]string
	RequestBase                VectorPartitionCoordinatorRequestV1
	TopologyDigest             string
	AuthorizationOverlayDigest string
	StrictCapabilityKey        []byte
	IndexedThrough             uint64
}

type fixedPeerVectorRuntimeV1 struct {
	parent     *FixedPeerTCPRuntimeV1
	manager    *collections.CollectionManager
	collection *collections.Collection
	dataGroup  raftcluster.GroupID
	listener   net.Listener
	server     *Server
	topology   *VectorPartitionProductionTopologyV1
	source     *CollectionVectorPartitionGenerationSourceV1
	backend    *VectorPartitionPublicBackendV1

	initMu     sync.Mutex
	mutationMu sync.Mutex
	closeOnce  sync.Once
	closeErr   error
}

type fixedPeerVectorBuilderV1 struct {
	ready map[raftcluster.GroupID]raftplacement.VectorPartitionLifecycleGroupReadyV1
}

func (b fixedPeerVectorBuilderV1) BuildAndStageVectorPartitionGroupV1(_ context.Context, _ raftplacement.VectorPartitionLifecycleIdentityV1, group raftcluster.GroupID) (raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	ready, ok := b.ready[group]
	if !ok {
		return raftplacement.VectorPartitionLifecycleGroupReadyV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	return ready, nil
}

type fixedPeerVectorBackendV1 struct{ runtime *fixedPeerVectorRuntimeV1 }

func (b *fixedPeerVectorBackendV1) backend(ctx context.Context) (*VectorPartitionPublicBackendV1, error) {
	if b == nil || b.runtime == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return b.runtime.ensureBackendV1(ctx)
}

func (b *fixedPeerVectorBackendV1) SearchVectorPartitionV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(err)
	}
	return backend.SearchVectorPartitionV1(ctx, request)
}

func (b *fixedPeerVectorBackendV1) SearchVectorPartitionFastV1(ctx context.Context, request public.SearchRequestV1, options public.FastSearchOptionsV1) (public.SearchResponseV1, public.FastSearchEvidenceV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, publicBackendErrorV1(err)
	}
	return backend.SearchVectorPartitionFastV1(ctx, request, options)
}

func (b *fixedPeerVectorBackendV1) PinVectorPartitionSearchSnapshotV1(ctx context.Context, options public.PinSearchSnapshotOptionsV1) (public.SearchSnapshotBackendV1, public.FastSearchEvidenceV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return nil, public.FastSearchEvidenceV1{}, publicBackendErrorV1(err)
	}
	return backend.PinVectorPartitionSearchSnapshotV1(ctx, options)
}

func (b *fixedPeerVectorBackendV1) InsertVectorPartitionV1(ctx context.Context, request public.InsertRequestV1) (public.InsertResponseV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.InsertResponseV1{}, publicBackendErrorV1(err)
	}
	return backend.InsertVectorPartitionV1(ctx, request)
}

func (b *fixedPeerVectorBackendV1) RegisterVectorPartitionV1(ctx context.Context, request public.GenerationRegistrationV1) (public.GenerationStatusV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.RegisterVectorPartitionV1(ctx, request)
}

func (b *fixedPeerVectorBackendV1) GenerationStatusV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.GenerationStatusV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) PrepareVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.PrepareVectorPartitionV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) ActivateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.ActivateVectorPartitionV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) InvalidateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1, reason string) (public.GenerationStatusV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.InvalidateVectorPartitionV1(ctx, id, reason)
}

func (b *fixedPeerVectorBackendV1) RetireVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.RetireVectorPartitionV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) RequestVectorPartitionRebuildV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.GenerationStatusV1{}, publicBackendErrorV1(err)
	}
	return backend.RequestVectorPartitionRebuildV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) VectorPartitionCleanupEligibilityV1(ctx context.Context, id public.GenerationIDV1) (public.CleanupEligibilityV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.CleanupEligibilityV1{}, publicBackendErrorV1(err)
	}
	return backend.VectorPartitionCleanupEligibilityV1(ctx, id)
}

func (b *fixedPeerVectorBackendV1) OperationsHealthV1(ctx context.Context) (public.OperationsHealthV1, error) {
	backend, err := b.backend(ctx)
	if err != nil {
		return public.OperationsHealthV1{Generation: public.GenerationIDV1{Index: b.runtime.parent.config.Vector.Manifest.IndexName, Generation: b.runtime.parent.config.Vector.Manifest.Generation}, Reason: "authority_unavailable"}, err
	}
	return backend.OperationsHealthV1(ctx)
}

func (r *fixedPeerVectorRuntimeV1) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		var errs []error
		// The accept loop is owned here rather than registered with Server, so
		// always close its listener before draining active public connections.
		if r.listener != nil {
			errs = append(errs, r.listener.Close())
		}
		if r.server != nil {
			errs = append(errs, r.server.Close())
		}
		if r.topology != nil {
			errs = append(errs, r.topology.Close())
		}
		if r.source != nil {
			errs = append(errs, r.source.Close())
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

func openFixedPeerVectorRuntimeV1(parent *FixedPeerTCPRuntimeV1) (*fixedPeerVectorRuntimeV1, error) {
	if parent == nil || parent.config.Vector == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	var group raftcluster.GroupID
	var data *fixedPeerDataV1
	for _, configured := range parent.config.Groups {
		if candidate := parent.data[configured.ID]; candidate != nil {
			if data != nil {
				return nil, errors.New("nativewire: fixed-peer vector runtime requires exactly one local data group")
			}
			group, data = configured.ID, candidate
		}
	}
	if data == nil || data.db == nil {
		return nil, errors.New("nativewire: fixed-peer vector runtime requires one local data store")
	}
	manager := collections.NewCollectionManager(data.db)
	collection, err := manager.OpenCollection(parent.config.Vector.Collection.Collection)
	if err != nil {
		return nil, fmt.Errorf("nativewire: open fixed-peer vector collection: %w", err)
	}
	prepared, err := collection.PreparedVectorPartitionManifestWithContextV1(context.Background(), parent.config.Vector.Manifest.IndexName, parent.config.Vector.Manifest.Generation)
	if err != nil || !vectorPartitionReplicatedLiveManifestMatchesV1(prepared, parent.config.Vector.Manifest) {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, fmt.Errorf("prepared vector manifest mismatch: %v", err))
	}
	owners := fixedPeerVectorOwnerGroupsV1(parent.config.Vector.Placement)
	if slices.Contains(owners, group) {
		// Replicated fixed-peer startup is validation-only: publishing a missing
		// binding here would create local command-WAL coverage outside Raft. The
		// prepared binding must already be durable before this process advertises
		// readiness.
		if _, err := collection.NewPreparedVectorPartitionGenerationReplicatedLiveSearchOpenPlanWithContextV1(context.Background(), prepared); err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, fmt.Errorf("validate vector live binding: %w", err))
		}
	}
	listener, err := net.Listen("tcp", parent.config.Vector.PublicAddresses[parent.config.NodeID])
	if err != nil {
		return nil, err
	}
	runtime := &fixedPeerVectorRuntimeV1{parent: parent, manager: manager, collection: collection, dataGroup: group, listener: listener}
	lazy := &fixedPeerVectorBackendV1{runtime: runtime}
	service, err := public.NewServiceV1(lazy)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	ops, err := public.NewOperationsV1(service, fixedPeerVectorOperationsConfigV1(), lazy.OperationsHealthV1)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	runtime.server = NewServer(ServerOptions{VectorPartitionOperations: ops, VectorPartitionNodeConfigSHA256: parent.client.digest, ConnectionIdleTimeout: parent.config.RequestTimeout})
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() { _ = runtime.server.ServeConn(context.Background(), conn) }()
		}
	}()
	return runtime, nil
}

func (r *fixedPeerVectorRuntimeV1) ensureBackendV1(ctx context.Context) (*VectorPartitionPublicBackendV1, error) {
	if r == nil || r.parent == nil || r.parent.config.Vector == nil || r.collection == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.initMu.Lock()
	defer r.initMu.Unlock()
	if r.backend != nil {
		return r.backend, nil
	}

	vector := r.parent.config.Vector
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if err := resolved.ValidateVectorPartitionPlacementV1(vector.Placement); err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	if len(owners) != 1 {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("fixed-peer routed vector mutation requires exactly one owner group"))
	}
	owner := owners[0]
	ready := map[raftcluster.GroupID]raftplacement.VectorPartitionLifecycleGroupReadyV1{
		owner: {
			GroupID:        owner,
			AppliedIndex:   vector.IndexedThrough,
			AssetSetDigest: vectorPartitionM8GroupAssetSetDigestV1(string(owner), vector.Manifest),
		},
	}
	lifecycle := raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: r.parent.authority, Committer: r.parent.meta}
	record, exists := r.parent.authority.VectorPartitionLifecycleRecordV1(vector.Identity)
	if !exists {
		metaStatus := r.parent.meta.RuntimeStatusV1()
		if metaStatus.State != "Leader" || metaStatus.LeaderID != r.parent.config.NodeID || r.parent.config.Catalog.BootstrapNode != r.parent.config.NodeID {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("vector lifecycle is not active on this catalog follower"))
		}
		if _, err := lifecycle.BeginBuildV1(ctx, vector.Identity, owners, 0, 1); err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
		}
		if _, err := lifecycle.RecordGroupReadyV1(ctx, vector.Identity, ready[owner]); err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
		}
		if _, err := lifecycle.PrepareV1(ctx, vector.Identity); err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
		}
		record, err = lifecycle.ActivateV1(ctx, vector.Identity)
		if err != nil {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
		}
	}
	if record.State != raftplacement.VectorPartitionLifecycleActiveV1 || record.Identity != vector.Identity ||
		!reflect.DeepEqual(record.RequiredGroups, owners) || len(record.ReadyGroups) != 1 || record.ReadyGroups[0] != ready[owner] || record.ReadySetDigest == "" {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("vector lifecycle identity or ready set mismatch"))
	}

	authority, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(r.parent.authority, r.parent)
	if err != nil {
		return nil, err
	}
	endpoints := make(map[raftcluster.GroupID]string, len(owners))
	nodeEndpoints := make(map[raftcluster.GroupID]map[raftcluster.NodeID]string, len(owners))
	for _, groupID := range owners {
		group, ok := resolved.Group(groupID)
		if !ok || group.LeaderHint == "" || vector.ShardAddresses[groupID][group.LeaderHint] == "" {
			return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, fmt.Errorf("owner group %q has no configured leader endpoint", groupID))
		}
		endpoints[groupID] = vector.ShardAddresses[groupID][group.LeaderHint]
		nodeEndpoints[groupID] = make(map[raftcluster.NodeID]string, len(vector.ShardAddresses[groupID]))
		for node, endpoint := range vector.ShardAddresses[groupID] {
			nodeEndpoints[groupID][node] = endpoint
		}
	}

	topologyOptions := VectorPartitionProductionTopologyOptionsV1{
		ConstructionContext: ctx,
		Catalog:             resolved,
		Placement:           vector.Placement,
		RouterSource:        CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: r.collection},
		ReplicatedLifecycle: authority,
		Endpoints:           endpoints,
		NodeEndpoints:       nodeEndpoints,
		CoordinatorLimits:   DefaultVectorPartitionCoordinatorLimitsV1(),
		ShardLimits:         DefaultVectorPartitionShardSearchLimitsV1(),
		ShardIdleTimeout:    r.parent.config.RequestTimeout,
	}
	var source *CollectionVectorPartitionGenerationSourceV1
	var shardListener net.Listener
	localOwnerLeader := false
	if group, ok := resolved.Group(owner); ok {
		localOwnerLeader = r.dataGroup == owner && group.LeaderHint == r.parent.config.NodeID
	}
	if localOwnerLeader {
		readCoordinator, readErr := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{{
			GroupID: owner, NodeID: r.parent.config.NodeID, ReadIndexProvider: r.parent.data[owner].provider, AppliedIndexWaiter: r.parent.data[owner].fsm,
		}})
		if readErr != nil {
			return nil, readErr
		}
		source, err = NewCollectionVectorPartitionGenerationSourceForReplicatedLiveLifecycleV1(r.collection, vector.Collection, authority, vector.Manifest, record.ReadySetDigest)
		if err != nil {
			return nil, err
		}
		shardService, serviceErr := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
			Catalog: resolved, Placement: vector.Placement, LocalNodeID: r.parent.config.NodeID, LocalGroupID: owner,
			ReadCoordinator: readCoordinator, GenerationSource: source, Limits: DefaultVectorPartitionShardSearchLimitsV1(),
		})
		if serviceErr != nil {
			_ = source.Close()
			return nil, serviceErr
		}
		shardListener, err = net.Listen("tcp", vector.ShardAddresses[owner][r.parent.config.NodeID])
		if err != nil {
			_ = source.Close()
			return nil, err
		}
		topologyOptions.Shards = []VectorPartitionProductionShardV1{{
			GroupID: owner, Listener: shardListener, Service: shardService, EndpointIdentity: r.parent.client.digest,
		}}
		topologyOptions.ServingSnapshot = &VectorPartitionServingSnapshotPublisherOptionsV1{
			Authority: authority, GenerationSources: map[raftcluster.GroupID]VectorPartitionGenerationSourceV1{owner: source},
			TopologyDigest: vector.TopologyDigest, AuthorizationOverlayDigest: vector.AuthorizationOverlayDigest, IndexedThrough: vector.IndexedThrough,
		}
		topologyOptions.StrictCapabilityKey = slices.Clone(vector.StrictCapabilityKey)
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
	backend, err := NewVectorPartitionPublicBackendV1(VectorPartitionPublicBackendOptionsV1{
		Topology: topology, RequestBase: vector.RequestBase, Lifecycle: lifecycle, ReadFence: r.parent,
		Identity: vector.Identity, RequiredGroups: owners, Builder: fixedPeerVectorBuilderV1{ready: ready}, MutationEpoch: record.MutationEpoch,
		MutationSubmitter: r.parent,
	})
	if err != nil {
		_ = topology.Close()
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	r.source, r.topology, r.backend = source, topology, backend
	return backend, nil
}

func fixedPeerVectorOwnerGroupsV1(placement raftplacement.VectorPartitionPlacementRecordV1) []raftcluster.GroupID {
	seen := make(map[raftcluster.GroupID]bool)
	for _, partition := range placement.Partitions {
		seen[partition.GroupID] = true
	}
	out := make([]raftcluster.GroupID, 0, len(seen))
	for group := range seen {
		out = append(out, group)
	}
	slices.Sort(out)
	return out
}

func fixedPeerVectorOperationsConfigV1() public.OperationsConfigV1 {
	config := public.ConservativeOperationsConfigV1()
	config.Enabled = true
	return config
}

// LinearizableCatalogMetaAppliedIndexV1 makes remote catalog fencing available
// to ingress nodes without pretending they can mint an owner-local no-log
// proof. Strict snapshots are therefore constructed only on the meta leader.
func (r *FixedPeerTCPRuntimeV1) LinearizableCatalogMetaAppliedIndexV1(ctx context.Context) (uint64, error) {
	status, err := r.catalogFence(ctx)
	return status.AppliedIndex, err
}

func validateFixedPeerVectorConfigV1(config FixedPeerTCPConfigV1, localGroups map[raftcluster.GroupID]bool) error {
	vector := config.Vector
	if vector == nil {
		return nil
	}
	if vector.Collection.Database == "" || vector.Collection.Catalog == "" || vector.Collection.Collection == "" ||
		vector.Manifest.State != "ready" || vector.Manifest.Collection != vector.Collection.Collection ||
		vector.Manifest.IndexName == "" || vector.Manifest.Generation == 0 || vector.Manifest.IntegrityDigest == "" ||
		vector.Placement.Collection != vector.Collection || vector.Placement.IndexName != vector.Manifest.IndexName ||
		vector.Placement.PartitionGeneration != vector.Manifest.Generation || vector.Identity.Index.Collection != vector.Collection ||
		vector.Identity.Index.IndexName != vector.Manifest.IndexName || vector.Identity.Generation != vector.Manifest.Generation ||
		vector.TopologyDigest == "" || vector.AuthorizationOverlayDigest == "" || len(vector.StrictCapabilityKey) != sha256.Size || vector.IndexedThrough == 0 {
		return errors.New("incomplete vector runtime identity")
	}
	if len(vector.PublicAddresses) != len(config.Nodes) {
		return errors.New("vector public address coverage differs from fixed peers")
	}
	for _, node := range config.Nodes {
		address := vector.PublicAddresses[node.ID]
		resolved, err := net.ResolveTCPAddr("tcp", address)
		if err != nil || resolved.IP == nil || resolved.IP.IsUnspecified() || resolved.Port == 0 {
			return fmt.Errorf("invalid vector public address for node %q", node.ID)
		}
	}
	owners := make(map[raftcluster.GroupID]bool)
	for _, partition := range vector.Placement.Partitions {
		owners[partition.GroupID] = true
	}
	for group := range owners {
		var fixed *FixedPeerTCPGroupV1
		for i := range config.Groups {
			if config.Groups[i].ID == group {
				fixed = &config.Groups[i]
				break
			}
		}
		if fixed == nil || len(vector.ShardAddresses[group]) != len(fixed.Peers) {
			return fmt.Errorf("vector shard address coverage is incomplete for group %q", group)
		}
		for _, peer := range fixed.Peers {
			if vector.ShardAddresses[group][peer.ID] == "" {
				return fmt.Errorf("vector shard address is missing for node %q", peer.ID)
			}
		}
		if localGroups[group] && vector.ShardAddresses[group][config.NodeID] == "" {
			return fmt.Errorf("vector shard address is missing for local owner group %q", group)
		}
	}
	return nil
}

// SubmitVectorPartitionInsertV1 performs exactly one fixed-peer hop to the
// configured owner leader. The receiving owner repeats the complete proof
// validation inside its serialized preflight-to-consensus boundary.
func (r *FixedPeerTCPRuntimeV1) SubmitVectorPartitionInsertV1(ctx context.Context, request VectorPartitionRoutedInsertV1) (public.InsertResponseV1, error) {
	if r == nil || r.config.Vector == nil || request.OwnerGroup == "" {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	var group *FixedPeerTCPGroupV1
	for i := range r.config.Groups {
		if r.config.Groups[i].ID == request.OwnerGroup {
			group = &r.config.Groups[i]
			break
		}
	}
	if group == nil {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	leader, err := r.client.leader(ctx, *group)
	if err != nil {
		return public.InsertResponseV1{}, err
	}
	request.Forwarded = leader != r.config.NodeID
	reply, err := r.client.call(ctx, leader, "vector-forward", fixedPeerRequestV1{VectorInsert: &request}, true)
	if err != nil {
		return public.InsertResponseV1{}, err
	}
	if reply.VectorInsert == nil {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	return *reply.VectorInsert, nil
}

func (r *FixedPeerTCPRuntimeV1) applyVectorInsertV1(ctx context.Context, request VectorPartitionRoutedInsertV1) (public.InsertResponseV1, error) {
	if r == nil || r.vector == nil || r.config.Vector == nil {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	r.vector.mutationMu.Lock()
	defer r.vector.mutationMu.Unlock()
	if err := r.validateVectorInsertOwnerV1(ctx, request); err != nil {
		return public.InsertResponseV1{}, err
	}
	data := r.data[request.OwnerGroup]
	if data == nil || data.fsm == nil {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	catalogVersion, ok, err := data.fsm.CurrentCatalogVersion(ctx)
	if err != nil || !ok {
		return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	entry, key, err := fixedPeerVectorInsertEntryV1(r.config.Vector.Collection.Collection, catalogVersion, request)
	if err != nil {
		return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorDocumentV1, err)
	}
	base, ok := r.localRegistry.Lookup(request.OwnerGroup)
	if !ok {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	submitter, ok := base.(raftcluster.CommandSubmitterWithPreCommitV1)
	if !ok {
		return public.InsertResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	resolved, err := raftplacement.Validate(r.config.Vector.Catalog)
	if err != nil {
		return public.InsertResponseV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	group, ok := resolved.Group(request.OwnerGroup)
	if !ok {
		return public.InsertResponseV1{}, ErrFixedPeerVectorWrongOwnerV1
	}
	members := make([]string, len(group.Members))
	for i := range group.Members {
		members[i] = string(group.Members[i])
	}
	metadata := raftentry.RequestMetadataV1{
		RequestID:                 binary.BigEndian.Uint64(key[:8]),
		AckPolicy:                 iwire.AckRaftCommitted,
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      r.config.Vector.Collection.Database,
		ClusterRouteCatalog:       r.config.Vector.Collection.Catalog,
		ClusterRouteCollection:    r.config.Vector.Collection.Collection,
		ClusterRouteShape:         "vector_partition_exact_id",
		ClusterRouteGroupID:       string(request.OwnerGroup),
		ClusterRouteMembers:       members,
		ClusterRouteLeaderHint:    string(group.LeaderHint),
		ClusterRoutePlacementMode: "vector_partition",
		ClusterRouteKey:           "_id",
		ClusterRoutePartitionID:   fmt.Sprint(request.PartitionID),
		CatalogMetaEpoch:          request.CatalogProof.Epoch,
		CatalogMetaDigest:         request.CatalogProof.Digest,
	}
	if !request.Request.Deadline.IsZero() {
		metadata.DeadlineUnixNanos = request.Request.Deadline.UnixNano()
	}
	result, err := submitter.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, func(commitCtx context.Context) error {
		return r.validateVectorInsertOwnerV1(commitCtx, request)
	})
	if err != nil {
		return public.InsertResponseV1{}, err
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable || !result.CommittedApplied ||
		!result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != request.OwnerGroup ||
		result.Evidence.NodeID != r.config.NodeID || result.Evidence.LeaderID != r.config.NodeID ||
		result.Evidence.Term != result.CommittedEntry.Term || result.Evidence.Index != result.CommittedEntry.Index ||
		result.CommittedEntry.Term == 0 || result.CommittedEntry.Index == 0 ||
		(result.ApplyResult.Status != raftentry.ApplyStatusApplied && result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied) {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(raftcluster.ErrCommitNotProven)
	}
	ownerStatus, err := data.provider.RuntimeStatusV1(ctx)
	if err != nil || ownerStatus.State != "Leader" || ownerStatus.LeaderID != r.config.NodeID || ownerStatus.RaftAppliedIndex < result.CommittedEntry.Index {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err, errors.New("owner apply proof is incomplete")))
	}
	if err := r.vector.topology.PublishServingSnapshotV1(ctx); err != nil {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	pin, err := r.vector.collection.AcquireVectorPartitionLiveSearchPinV1(r.config.Vector.Manifest)
	if err != nil {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	live := pin.StatusV1()
	pin.Release()
	if live.Generation != request.Identity.Generation || live.Revision == 0 || live.Coverage == 0 {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("live visibility proof is incomplete")))
	}
	backend, err := r.vector.ensureBackendV1(ctx)
	if err != nil {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	requestBase := r.config.Vector.RequestBase
	visibility, err := backend.SearchVectorPartitionV1(ctx, public.SearchRequestV1{
		Version: request.Request.Version, Generation: request.Request.Generation, Query: slices.Clone(request.Request.Vector),
		Metric: public.MetricCosineV1, TopK: requestBase.TopK, Probes: requestBase.PartitionProbes, EfSearch: requestBase.EfSearch,
		Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits: public.SearchLimitsV1{
			RequestBytes: requestBase.RequestBytesLimit, CandidateBytes: requestBase.CandidateBytesLimit,
			ResponseBytes: requestBase.ResponseBytesLimit, MergeEntries: requestBase.MergeEntriesLimit,
		},
		Deadline: request.Request.Deadline,
	})
	if err != nil {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	visibleID := string(request.Request.ID)
	visible := false
	for _, neighbor := range visibility.Neighbors {
		if neighbor.ID == visibleID {
			visible = true
			break
		}
	}
	if !visible {
		return public.InsertResponseV1{}, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, errors.New("owner strict search did not return inserted id")))
	}
	forwards := uint64(0)
	if request.Forwarded {
		forwards = 1
	}
	return public.InsertResponseV1{
		Generation: request.Request.Generation, PartitionID: request.PartitionID, OwnerGroup: string(request.OwnerGroup),
		CommitTerm: result.CommittedEntry.Term, CommitIndex: result.CommittedEntry.Index, AppliedIndex: ownerStatus.RaftAppliedIndex,
		ProductionConsensus: true, LiveRevision: live.Revision, VisibilityGeneration: request.Request.Generation, VisibleID: visibleID,
		Counters: public.MutationCountersV1{Routes: 1, Forwards: forwards, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1},
	}, nil
}

func fixedPeerVectorPostCommitAmbiguousV1(err error) error {
	if err == nil {
		err = errors.New("post-commit mutation proof is incomplete")
	}
	return errors.Join(raftcluster.ErrCommitAmbiguous, err)
}

func (r *FixedPeerTCPRuntimeV1) validateVectorInsertOwnerV1(ctx context.Context, request VectorPartitionRoutedInsertV1) error {
	if request.CatalogProof.Epoch == 0 || request.CatalogProof.Digest == "" || request.ReadySetDigest == "" || request.RouterModelDigest == "" {
		return ErrFixedPeerVectorProofMissingV1
	}
	vector := r.config.Vector
	if vector == nil || r.vector == nil || request.Identity != vector.Identity ||
		request.Request.Generation.Index != vector.Identity.Index.IndexName || request.Request.Generation.Generation != vector.Identity.Generation ||
		request.CatalogProof.Epoch != vector.Identity.Index.CatalogEpoch || request.CatalogProof.Digest != vector.Identity.Index.CatalogDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if r.vector.dataGroup != request.OwnerGroup || r.data[request.OwnerGroup] == nil {
		return ErrFixedPeerVectorWrongOwnerV1
	}
	status, err := r.data[request.OwnerGroup].provider.RuntimeStatusV1(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if status.State != "Leader" || status.LeaderID != r.config.NodeID {
		return errors.Join(ErrFixedPeerVectorUnavailableV1, raftcluster.ErrNotLeader)
	}
	catalog, err := r.catalogFence(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if catalog.Epoch != request.CatalogProof.Epoch || catalog.Digest != request.CatalogProof.Digest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if err := r.authority.ValidateCatalogMetaProof(ctx, request.CatalogProof.Epoch, request.CatalogProof.Digest); err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	record, exists := r.authority.VectorPartitionLifecycleRecordV1(request.Identity)
	if !exists || record.ReadySetDigest != request.ReadySetDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if err := record.CanSearch(raftplacement.VectorPartitionLifecycleSearchProofV1{Identity: request.Identity, ReadySetDigest: request.ReadySetDigest}); err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	decoded, err := r.vector.collection.ValidatedVectorFromDocumentV1(vector.Manifest.IndexName, collections.DocumentFormatJSON, request.Request.Document)
	if err != nil || !sameFloat32BitsV1(decoded, request.Request.Vector) {
		return errors.Join(ErrFixedPeerVectorDocumentV1, err)
	}
	backend, err := r.vector.ensureBackendV1(ctx)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	coordinator := backend.opts.Topology.Coordinator()
	lease, err := coordinator.acquireRouterSessionV1(ctx, request.Request.Generation.Index, request.Request.Generation.Generation)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	defer lease.Close()
	routerStatus := lease.session.router.Status()
	readySetDigest, err := coordinator.validateReplicatedLifecycle(ctx, routerStatus)
	if err != nil || readySetDigest != request.ReadySetDigest || routerStatus.ModelDigest != request.RouterModelDigest {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	selection, err := lease.session.router.SearchWithContextV1(ctx, decoded, collections.VectorPartitionRouterSearchOptionsV1{
		Mode: collections.VectorPartitionRouterModeExactV1, CandidateBudget: int(routerStatus.Representatives), PartitionProbes: 1,
	})
	if err != nil || len(selection.Partitions) != 1 {
		return errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	partitionID := selection.Partitions[0].PartitionID
	owner := raftcluster.GroupID("")
	for _, partition := range coordinator.placement.Partitions {
		if partition.PartitionID == partitionID {
			owner = partition.GroupID
			break
		}
	}
	if partitionID != request.PartitionID || owner == "" || owner != request.OwnerGroup {
		return ErrFixedPeerVectorWrongOwnerV1
	}
	return nil
}

func fixedPeerVectorInsertEntryV1(collection string, catalogVersion uint64, request VectorPartitionRoutedInsertV1) ([]byte, [sha256.Size]byte, error) {
	h := sha256.New()
	_, _ = h.Write([]byte("TreeDB/fixed-peer-vector-insert/v1\x00"))
	_, _ = h.Write([]byte(request.Identity.Index.IndexName))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], request.Identity.Generation)
	_, _ = h.Write(generation[:])
	_, _ = h.Write(request.Request.ID)
	_, _ = h.Write(request.Request.Document)
	var key [sha256.Size]byte
	copy(key[:], h.Sum(nil))
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: key[:]},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		collectionNameRef(collection),
		documentFormatSection(collections.DocumentFormatJSON),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, request.Request.ID)},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, request.Request.Document)},
	}
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, key, err
	}
	entry, err := iwire.AppendDeterministicEntry(nil, validated)
	return entry, key, err
}

func sameFloat32BitsV1(left, right []float32) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if math.Float32bits(left[i]) != math.Float32bits(right[i]) {
			return false
		}
	}
	return true
}
