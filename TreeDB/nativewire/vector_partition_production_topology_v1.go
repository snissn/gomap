package nativewire

// Production vector-partition topology composes already-authoritative catalog,
// lifecycle, read-coordinator, and TCP service dependencies.  It deliberately
// does not create Raft groups, catalogs, or vector assets.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type VectorPartitionProductionShardV1 struct {
	GroupID                  raftcluster.GroupID
	Listener                 net.Listener
	Service                  *VectorPartitionShardSearchServiceV1
	EndpointIdentity         string
	EndpointIdentityProvider func() VectorPartitionShardEndpointIdentityV1
}

type VectorPartitionProductionTopologyOptionsV1 struct {
	ConstructionContext context.Context
	Catalog             raftplacement.ResolvedCatalogV1
	Placement           raftplacement.VectorPartitionPlacementRecordV1
	RouterSource        VectorPartitionCoordinatorRouterSourceV1
	ReplicatedLifecycle VectorPartitionReplicatedLifecycleAuthorityV1
	Endpoints           map[raftcluster.GroupID]string
	NodeEndpoints       map[raftcluster.GroupID]map[raftcluster.NodeID]string
	Shards              []VectorPartitionProductionShardV1
	CoordinatorLimits   VectorPartitionCoordinatorLimitsV1
	ShardLimits         VectorPartitionShardSearchLimitsV1
	ShardIdleTimeout    time.Duration
	ServingSnapshot     *VectorPartitionServingSnapshotPublisherOptionsV1
	StrictCapabilityKey []byte
}

type VectorPartitionProductionTopologyStatusV1 struct {
	Ready       bool
	Closed      bool
	Endpoints   map[raftcluster.GroupID]string
	ShardGroups []raftcluster.GroupID
}

// VectorPartitionProductionTopologyV1 owns only listeners and the coordinator.
// Callers retain ownership of Raft, catalog, lifecycle, and local asset sources.
type VectorPartitionProductionTopologyV1 struct {
	coordinator       *VectorPartitionCoordinatorV1
	servingSnapshot   *VectorPartitionServingSnapshotPublisherV1
	authorization     *vectorPartitionAuthorizationOverlayV1
	searchPublication sync.Mutex
	strictKey         []byte
	dispatcher        *VectorPartitionShardSearchTCPDispatcherV1
	listeners         map[raftcluster.GroupID]net.Listener
	endpoints         map[raftcluster.GroupID]string
	services          map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1
	identities        map[raftcluster.GroupID]string
	identityProviders map[raftcluster.GroupID]func() VectorPartitionShardEndpointIdentityV1
	serving           map[raftcluster.GroupID]bool
	mu                sync.Mutex
	conns             map[net.Conn]struct{}
	wg                sync.WaitGroup
	closed            bool
	closeOnce         sync.Once
	closeErr          error
}

func NewVectorPartitionProductionTopologyV1(opts VectorPartitionProductionTopologyOptionsV1) (_ *VectorPartitionProductionTopologyV1, err error) {
	if opts.RouterSource == nil || opts.ReplicatedLifecycle == nil {
		return nil, errors.New("nativewire: production vector topology requires router and replicated lifecycle")
	}
	coordinatorLimits, err := normalizeVectorPartitionCoordinatorLimitsV1(opts.CoordinatorLimits)
	if err != nil {
		return nil, err
	}
	shardLimits, err := normalizeVectorPartitionShardSearchLimitsV1(opts.ShardLimits)
	if err != nil {
		return nil, err
	}
	opts.CoordinatorLimits, opts.ShardLimits = coordinatorLimits, shardLimits
	if err := opts.Catalog.ValidateVectorPartitionPlacementV1(opts.Placement); err != nil {
		return nil, fmt.Errorf("nativewire: production vector topology placement: %w", err)
	}
	if len(opts.Endpoints) == 0 {
		return nil, errors.New("nativewire: production vector topology requires owner endpoints")
	}
	if opts.ShardIdleTimeout < 0 {
		return nil, errors.New("nativewire: production vector topology shard idle timeout is negative")
	}
	if opts.ShardIdleTimeout == 0 {
		opts.ShardIdleTimeout = 30 * time.Second
	}
	h := &VectorPartitionProductionTopologyV1{listeners: make(map[raftcluster.GroupID]net.Listener), endpoints: make(map[raftcluster.GroupID]string, len(opts.Endpoints)), services: make(map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1), identities: make(map[raftcluster.GroupID]string), identityProviders: make(map[raftcluster.GroupID]func() VectorPartitionShardEndpointIdentityV1), serving: make(map[raftcluster.GroupID]bool), conns: make(map[net.Conn]struct{})}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	owners := make(map[raftcluster.GroupID]bool, len(opts.Placement.Partitions))
	for _, part := range opts.Placement.Partitions {
		owners[part.GroupID] = true
	}
	for group, endpoint := range opts.Endpoints {
		if !owners[group] || endpoint == "" {
			return nil, fmt.Errorf("nativewire: production vector topology endpoint %q is not a partition owner", group)
		}
		h.endpoints[group] = endpoint
	}
	if len(h.endpoints) != len(owners) {
		return nil, errors.New("nativewire: production vector topology has incomplete owner endpoint coverage")
	}
	for group, nodes := range opts.NodeEndpoints {
		resolved, ok := opts.Catalog.Group(group)
		if !ok || !owners[group] {
			return nil, fmt.Errorf("nativewire: production vector topology node endpoint owner %q is invalid", group)
		}
		groupEndpointNodes := make(map[string]raftcluster.NodeID, len(nodes))
		for node, endpoint := range nodes {
			if !slices.Contains(resolved.Members, node) {
				return nil, fmt.Errorf("nativewire: production vector topology node %q is not a member of %q", node, group)
			}
			keys, err := vectorPartitionProductionEndpointKeysV1(endpoint)
			if err != nil {
				return nil, fmt.Errorf("nativewire: production vector topology node %q endpoint: %w", node, err)
			}
			for _, key := range keys {
				if owner := groupEndpointNodes[key]; owner != "" && owner != node {
					return nil, fmt.Errorf("nativewire: production vector topology nodes %q and %q in %q share an endpoint", owner, node, group)
				}
				groupEndpointNodes[key] = node
			}
		}
	}
	endpointOwners := make(map[string]raftcluster.GroupID, len(opts.Endpoints)+len(opts.NodeEndpoints))
	recordEndpoint := func(group raftcluster.GroupID, endpoint string) error {
		keys, err := vectorPartitionProductionEndpointKeysV1(endpoint)
		if err != nil {
			return fmt.Errorf("nativewire: production vector topology group %q endpoint: %w", group, err)
		}
		for _, key := range keys {
			if owner := endpointOwners[key]; owner != "" && owner != group {
				return fmt.Errorf("nativewire: production vector topology groups %q and %q share an endpoint", owner, group)
			}
			endpointOwners[key] = group
		}
		return nil
	}
	for group, endpoint := range h.endpoints {
		if err := recordEndpoint(group, endpoint); err != nil {
			return nil, err
		}
	}
	for group, nodes := range opts.NodeEndpoints {
		for _, endpoint := range nodes {
			if err := recordEndpoint(group, endpoint); err != nil {
				return nil, err
			}
		}
	}
	localListeners := make(map[string]raftcluster.GroupID, len(opts.Shards))
	catalogHints := make(map[raftcluster.GroupID]raftcluster.NodeID, len(opts.Catalog.Groups))
	for _, group := range opts.Catalog.Groups {
		catalogHints[group.ID] = group.LeaderHint
	}
	for _, shard := range opts.Shards {
		if shard.GroupID == "" || shard.Listener == nil || shard.Service == nil || !owners[shard.GroupID] || h.listeners[shard.GroupID] != nil {
			return nil, fmt.Errorf("nativewire: production vector topology shard %q is invalid", shard.GroupID)
		}
		group, _ := opts.Catalog.Group(shard.GroupID)
		if shard.Service.localGroup != shard.GroupID || !slices.Contains(group.Members, shard.Service.localNodeID) ||
			!reflect.DeepEqual(shard.Service.route.placement, opts.Placement) || !reflect.DeepEqual(shard.Service.route.hints, catalogHints) ||
			shard.Service.limits != shardLimits {
			return nil, fmt.Errorf("nativewire: production vector topology shard %q service does not match topology", shard.GroupID)
		}
		if endpoint := h.endpoints[shard.GroupID]; endpoint == "" || !vectorPartitionProductionEndpointMatchesListenerV1(endpoint, shard.Listener) {
			return nil, fmt.Errorf("nativewire: production vector topology shard %q endpoint does not match listener", shard.GroupID)
		}
		if endpoint := opts.NodeEndpoints[shard.GroupID][shard.Service.localNodeID]; endpoint != "" && !vectorPartitionProductionEndpointMatchesListenerV1(endpoint, shard.Listener) {
			return nil, fmt.Errorf("nativewire: production vector topology shard %q local node endpoint does not match listener", shard.GroupID)
		}
		for group, endpoint := range h.endpoints {
			if group == shard.GroupID {
				continue
			}
			usesLocalListener, err := vectorPartitionProductionEndpointUsesListenerV1(endpoint, shard.Listener)
			if err != nil {
				return nil, fmt.Errorf("nativewire: production vector topology group %q endpoint: %w", group, err)
			}
			if usesLocalListener {
				return nil, fmt.Errorf("nativewire: production vector topology group %q uses shard %q listener", group, shard.GroupID)
			}
		}
		for group, nodes := range opts.NodeEndpoints {
			if group == shard.GroupID {
				continue
			}
			for node, endpoint := range nodes {
				usesLocalListener, err := vectorPartitionProductionEndpointUsesListenerV1(endpoint, shard.Listener)
				if err != nil {
					return nil, fmt.Errorf("nativewire: production vector topology node %q endpoint: %w", node, err)
				}
				if usesLocalListener {
					return nil, fmt.Errorf("nativewire: production vector topology node %q in %q uses shard %q listener", node, group, shard.GroupID)
				}
			}
		}
		for _, member := range group.Members {
			if member == shard.Service.localNodeID {
				continue
			}
			endpoint := opts.NodeEndpoints[shard.GroupID][member]
			if endpoint == "" {
				return nil, fmt.Errorf("nativewire: production vector topology shard %q cannot route to member %q", shard.GroupID, member)
			}
			usesLocalListener, err := vectorPartitionProductionEndpointUsesListenerV1(endpoint, shard.Listener)
			if err != nil {
				return nil, fmt.Errorf("nativewire: production vector topology shard %q remote member %q endpoint: %w", shard.GroupID, member, err)
			}
			if usesLocalListener {
				return nil, fmt.Errorf("nativewire: production vector topology shard %q remote member %q uses the local fallback", shard.GroupID, member)
			}
		}
		listenerKey := shard.Listener.Addr().Network() + "\x00" + shard.Listener.Addr().String()
		if owner := localListeners[listenerKey]; owner != "" && owner != shard.GroupID {
			return nil, fmt.Errorf("nativewire: production vector topology shard groups %q and %q share a listener", owner, shard.GroupID)
		}
		localListeners[listenerKey] = shard.GroupID
		h.listeners[shard.GroupID], h.services[shard.GroupID], h.identities[shard.GroupID], h.identityProviders[shard.GroupID] = shard.Listener, shard.Service, shard.EndpointIdentity, shard.EndpointIdentityProvider
	}
	maxPoolConnections := coordinatorLimits.MaxConcurrentRequests
	dispatcher, err := newVectorPartitionShardSearchTCPDispatcherV1(h.endpoints, opts.NodeEndpoints, maxPoolConnections, shardLimits)
	if err != nil {
		return nil, err
	}
	h.dispatcher = dispatcher
	h.coordinator, err = NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1{Catalog: opts.Catalog, Placement: opts.Placement, RouterSource: opts.RouterSource, Dispatcher: dispatcher, ReplicatedLifecycle: opts.ReplicatedLifecycle, RequireReplicatedLifecycle: true, Limits: opts.CoordinatorLimits, ShardLimits: opts.ShardLimits})
	if err != nil {
		return nil, err
	}
	if opts.ServingSnapshot != nil || len(opts.StrictCapabilityKey) != 0 {
		if opts.ServingSnapshot == nil || len(opts.StrictCapabilityKey) != sha256.Size {
			return nil, errors.New("nativewire: production vector topology strict serving options are incomplete")
		}
		snapshotOpts := *opts.ServingSnapshot
		snapshotOpts.Coordinator = h.coordinator
		h.servingSnapshot, err = NewVectorPartitionServingSnapshotPublisherV1(snapshotOpts)
		if err != nil {
			return nil, err
		}
		h.authorization, err = newVectorPartitionAuthorizationOverlayV1(snapshotOpts.AuthorizationOverlayDigest, nil, coordinatorLimits.MaxStableIDBytes)
		if err != nil {
			return nil, err
		}
		constructionContext := opts.ConstructionContext
		if constructionContext == nil {
			constructionContext = context.Background()
		}
		publishContext, cancel := context.WithTimeout(constructionContext, coordinatorLimits.MaxWallClock)
		defer cancel()
		if err = h.servingSnapshot.PublishV1(publishContext); err != nil {
			return nil, err
		}
		h.strictKey = slices.Clone(opts.StrictCapabilityKey)
		for _, service := range h.services {
			if err = service.bindServingSnapshotV1(h.servingSnapshot, h.strictKey); err != nil {
				return nil, err
			}
		}
	}
	for group, listener := range h.listeners {
		// Keep accepted sockets bounded without letting one dispatcher pool consume
		// the whole listener while its keep-alives are idle.
		h.serve(group, listener, h.services[group], h.identities[group], opts.ShardIdleTimeout, coordinatorLimits.MaxRequests, dispatcher.maxRequestFrame, dispatcher.maxResponseFrame)
	}
	return h, nil
}

func (h *VectorPartitionProductionTopologyV1) serve(group raftcluster.GroupID, listener net.Listener, service *VectorPartitionShardSearchServiceV1, identity string, idleTimeout time.Duration, maxConnections int, maxRequestFrame, maxResponseFrame uint32) {
	connectionSlots := make(chan struct{}, maxConnections)
	h.mu.Lock()
	h.serving[group] = true
	h.mu.Unlock()
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer func() { h.mu.Lock(); h.serving[group] = false; h.mu.Unlock() }()
		var retryDelay time.Duration
		for {
			conn, err := listener.Accept()
			if err != nil {
				h.mu.Lock()
				closed := h.closed
				h.mu.Unlock()
				var netErr net.Error
				if !closed && !errors.Is(err, net.ErrClosed) && errors.As(err, &netErr) && netErr.Temporary() {
					if retryDelay == 0 {
						retryDelay = 5 * time.Millisecond
					} else {
						retryDelay = min(2*retryDelay, time.Second)
					}
					time.Sleep(retryDelay)
					continue
				}
				return
			}
			retryDelay = 0
			select {
			case connectionSlots <- struct{}{}:
			default:
				_ = conn.Close()
				continue
			}
			h.mu.Lock()
			if h.closed {
				h.mu.Unlock()
				<-connectionSlots
				_ = conn.Close()
				return
			}
			h.conns[conn] = struct{}{}
			h.wg.Add(1)
			h.mu.Unlock()
			go func() {
				defer h.wg.Done()
				defer func() { <-connectionSlots }()
				defer func() { h.mu.Lock(); delete(h.conns, conn); h.mu.Unlock() }()
				(VectorPartitionShardSearchTCPServerV1{Service: service, EndpointIdentity: VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: string(group), InstanceIdentity: identity}, EndpointIdentityProvider: h.identityProviders[group], MaxFrame: maxRequestFrame, MaxResponseFrame: maxResponseFrame, InitialTimeout: idleTimeout}).ServeConn(context.Background(), conn)
			}()
		}
	}()
}

func (h *VectorPartitionProductionTopologyV1) Coordinator() *VectorPartitionCoordinatorV1 {
	if h == nil {
		return nil
	}
	return h.coordinator
}

func (h *VectorPartitionProductionTopologyV1) searchStrictV1(ctx context.Context, request VectorPartitionCoordinatorRequestV1) (VectorPartitionCoordinatorResponseV1, error) {
	if h == nil || h.coordinator == nil {
		return VectorPartitionCoordinatorResponseV1{}, ErrVectorPartitionCoordinatorUnavailable
	}
	if h.servingSnapshot == nil {
		return h.coordinator.Search(ctx, request)
	}
	snapshot, err := h.servingSnapshot.AcquireStrictV1(ctx)
	if err != nil {
		return VectorPartitionCoordinatorResponseV1{}, err
	}
	defer snapshot.Close()
	response, err := h.coordinator.searchStrictV1(ctx, request, snapshot, snapshot.proof, h.strictKey)
	if err == nil {
		response.Counters.SnapshotPins = 1
		err = h.authorization.filterV1(snapshot.IdentityV1().AuthorizationOverlayDigest, &response)
	}
	return response, err
}

type vectorPartitionFastSearchEvidenceV1 struct {
	Identity VectorPartitionServingSnapshotIdentityV1
	Age      time.Duration
}

func (h *VectorPartitionProductionTopologyV1) searchFastV1(ctx context.Context, request VectorPartitionCoordinatorRequestV1, maxAge time.Duration, minIndexedThrough uint64) (VectorPartitionCoordinatorResponseV1, vectorPartitionFastSearchEvidenceV1, error) {
	if h == nil || h.coordinator == nil || h.servingSnapshot == nil {
		return VectorPartitionCoordinatorResponseV1{}, vectorPartitionFastSearchEvidenceV1{}, ErrVectorPartitionCoordinatorUnavailable
	}
	snapshot, err := h.servingSnapshot.AcquireFastV1(ctx, maxAge, minIndexedThrough)
	if err != nil {
		return VectorPartitionCoordinatorResponseV1{}, vectorPartitionFastSearchEvidenceV1{}, err
	}
	defer snapshot.Close()
	return h.searchSnapshotV1(ctx, request, snapshot, snapshot.proof, true)
}

func (h *VectorPartitionProductionTopologyV1) searchSnapshotV1(ctx context.Context, request VectorPartitionCoordinatorRequestV1, snapshot *VectorPartitionServingSnapshotLeaseV1, proof vectorPartitionServingAuthorityProofV1, countPin bool) (VectorPartitionCoordinatorResponseV1, vectorPartitionFastSearchEvidenceV1, error) {
	identity := snapshot.IdentityV1()
	age := time.Since(time.Unix(0, identity.PublishedAtUnixNano))
	if age < 0 {
		age = 0
	}
	response, err := h.coordinator.searchStrictV1(ctx, request, snapshot, proof, h.strictKey)
	if err == nil {
		if countPin {
			response.Counters.SnapshotPins = 1
		}
		err = h.authorization.filterV1(identity.AuthorizationOverlayDigest, &response)
	}
	return response, vectorPartitionFastSearchEvidenceV1{Identity: identity, Age: age}, err
}

type vectorPartitionPinnedTopologySearchV1 struct {
	topology *VectorPartitionProductionTopologyV1
	lease    *VectorPartitionServingSnapshotLeaseV1
}

func (h *VectorPartitionProductionTopologyV1) pinSearchSnapshotV1(ctx context.Context, maxAge time.Duration, minIndexedThrough uint64, maxSessionAge time.Duration) (*vectorPartitionPinnedTopologySearchV1, vectorPartitionFastSearchEvidenceV1, error) {
	if h == nil || h.coordinator == nil || h.servingSnapshot == nil {
		return nil, vectorPartitionFastSearchEvidenceV1{}, ErrVectorPartitionCoordinatorUnavailable
	}
	lease, err := h.servingSnapshot.AcquirePinnedV1(ctx, maxAge, minIndexedThrough, maxSessionAge)
	if err != nil {
		return nil, vectorPartitionFastSearchEvidenceV1{}, err
	}
	identity := lease.IdentityV1()
	age := time.Since(time.Unix(0, identity.PublishedAtUnixNano))
	if age < 0 {
		age = 0
	}
	return &vectorPartitionPinnedTopologySearchV1{topology: h, lease: lease}, vectorPartitionFastSearchEvidenceV1{Identity: identity, Age: age}, nil
}

func (p *vectorPartitionPinnedTopologySearchV1) searchV1(ctx context.Context, request VectorPartitionCoordinatorRequestV1) (VectorPartitionCoordinatorResponseV1, error) {
	if p == nil || p.topology == nil || p.lease == nil {
		return VectorPartitionCoordinatorResponseV1{}, ErrVectorPartitionCoordinatorUnavailable
	}
	proof, err := p.lease.lockPinnedProofV1()
	if err != nil {
		return VectorPartitionCoordinatorResponseV1{}, err
	}
	defer p.lease.useMu.RUnlock()
	response, _, err := p.topology.searchSnapshotV1(ctx, request, p.lease, proof, false)
	return response, err
}

func (p *vectorPartitionPinnedTopologySearchV1) Close() error {
	if p == nil || p.lease == nil {
		return nil
	}
	return p.lease.Close()
}

// PublishSearchSnapshotV1 atomically advances the source watermark and current
// authorization overlay before making the new immutable snapshot available.
// Its token is the local read-your-writes floor accepted by SearchFast.
func (h *VectorPartitionProductionTopologyV1) PublishSearchSnapshotV1(ctx context.Context, indexedThrough uint64, authorizationDigest string, deniedDocumentIDs []string) (public.IndexedWriteTokenV1, error) {
	if h == nil || h.servingSnapshot == nil || h.authorization == nil {
		return public.IndexedWriteTokenV1{}, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	h.searchPublication.Lock()
	defer h.searchPublication.Unlock()
	nextAuthorization, err := newVectorPartitionAuthorizationOverlayStateV1(authorizationDigest, deniedDocumentIDs, h.coordinator.limits.MaxStableIDBytes)
	if err != nil {
		return public.IndexedWriteTokenV1{}, err
	}
	if err := h.servingSnapshot.publishStateV1(ctx, indexedThrough, authorizationDigest, func() { h.authorization.state.Store(nextAuthorization) }); err != nil {
		return public.IndexedWriteTokenV1{}, err
	}
	return public.IndexedWriteTokenV1{Sequence: indexedThrough}, nil
}

func (h *VectorPartitionProductionTopologyV1) InvalidateServingSnapshotV1() error {
	if h == nil || h.servingSnapshot == nil {
		return nil
	}
	return h.servingSnapshot.InvalidateV1()
}

func (h *VectorPartitionProductionTopologyV1) PublishServingSnapshotV1(ctx context.Context) error {
	if h == nil || h.servingSnapshot == nil {
		return nil
	}
	return h.servingSnapshot.PublishV1(ctx)
}

func (h *VectorPartitionProductionTopologyV1) Status() VectorPartitionProductionTopologyStatusV1 {
	if h == nil {
		return VectorPartitionProductionTopologyStatusV1{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ready := !h.closed && h.coordinator != nil
	for group := range h.listeners {
		ready = ready && h.serving[group]
	}
	status := VectorPartitionProductionTopologyStatusV1{Ready: ready, Closed: h.closed, Endpoints: make(map[raftcluster.GroupID]string, len(h.endpoints))}
	for group, endpoint := range h.endpoints {
		status.Endpoints[group] = endpoint
	}
	for group := range h.listeners {
		status.ShardGroups = append(status.ShardGroups, group)
	}
	slices.Sort(status.ShardGroups)
	return status
}

func vectorPartitionProductionEndpointMatchesListenerV1(endpoint string, listener net.Listener) bool {
	advertised, err := vectorPartitionProductionEndpointAddressesV1(endpoint)
	if err != nil {
		return false
	}
	bound, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return false
	}
	var local []net.Addr
	if bound.IP.IsUnspecified() {
		local, err = net.InterfaceAddrs()
		if err != nil {
			return false
		}
	}
	for _, address := range advertised {
		if !vectorPartitionProductionEndpointAddressMatchesListenerV1(address, listener) ||
			(bound.IP.IsUnspecified() && !vectorPartitionProductionEndpointAddressIsLocalV1(address, local)) {
			return false
		}
	}
	return len(advertised) != 0
}

func vectorPartitionProductionEndpointAddressMatchesListenerV1(advertised *net.TCPAddr, listener net.Listener) bool {
	bound, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return false
	}
	if advertised == nil || advertised.Port != bound.Port || len(advertised.IP) == 0 || advertised.IP.IsUnspecified() {
		return false
	}
	if !bound.IP.IsUnspecified() {
		if !bound.IP.Equal(advertised.IP) || bound.IP.To4() != nil {
			return bound.IP.Equal(advertised.IP)
		}
		if bound.Zone == advertised.Zone {
			return true
		}
		boundInterface, boundErr := vectorPartitionProductionInterfaceForZoneV1(bound.Zone)
		advertisedInterface, advertisedErr := vectorPartitionProductionInterfaceForZoneV1(advertised.Zone)
		return boundErr == nil && advertisedErr == nil && boundInterface.Index == advertisedInterface.Index
	}
	if bound.IP.To4() != nil || advertised.IP.To4() == nil {
		return (bound.IP.To4() != nil) == (advertised.IP.To4() != nil)
	}
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		return false
	}
	ipv6Only, ok := vectorPartitionProductionListenerIPv6OnlyV1(tcpListener)
	return ok && !ipv6Only
}

func vectorPartitionProductionEndpointUsesListenerV1(endpoint string, listener net.Listener) (bool, error) {
	advertised, err := vectorPartitionProductionEndpointAddressesV1(endpoint)
	if err != nil {
		return false, err
	}
	compatible := advertised[:0]
	for _, address := range advertised {
		if vectorPartitionProductionEndpointAddressMatchesListenerV1(address, listener) {
			compatible = append(compatible, address)
		}
	}
	if len(compatible) == 0 {
		return false, nil
	}
	bound := listener.Addr().(*net.TCPAddr)
	if !bound.IP.IsUnspecified() {
		return true, nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, err
	}
	for _, address := range compatible {
		if vectorPartitionProductionEndpointAddressIsLocalV1(address, addrs) {
			return true, nil
		}
	}
	return false, nil
}

func vectorPartitionProductionEndpointAddressIsLocalV1(address *net.TCPAddr, local []net.Addr) bool {
	if address.IP.IsLoopback() {
		return true
	}
	if address.IP.IsLinkLocalUnicast() {
		if address.Zone == "" {
			return false
		}
		iface, err := vectorPartitionProductionInterfaceForZoneV1(address.Zone)
		if err != nil {
			return false
		}
		local, err = iface.Addrs()
		if err != nil {
			return false
		}
	}
	for _, addr := range local {
		var ip net.IP
		switch addr := addr.(type) {
		case *net.IPNet:
			ip = addr.IP
		case *net.IPAddr:
			ip = addr.IP
		}
		if ip.Equal(address.IP) {
			return true
		}
	}
	return false
}

func vectorPartitionProductionEndpointAddressesV1(endpoint string) ([]*net.TCPAddr, error) {
	host, service, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, errors.New("TCP endpoint host is unspecified")
	}
	port, err := net.LookupPort("tcp", service)
	if err != nil {
		return nil, err
	}
	if port == 0 {
		return nil, errors.New("TCP endpoint port is zero")
	}
	resolved, err := net.DefaultResolver.LookupIPAddr(context.Background(), host)
	if err != nil {
		return nil, err
	}
	addresses := make([]*net.TCPAddr, 0, len(resolved))
	seen := make(map[string]struct{}, len(resolved))
	for _, addr := range resolved {
		if len(addr.IP) == 0 || addr.IP.IsUnspecified() {
			continue
		}
		address := &net.TCPAddr{IP: addr.IP, Port: port, Zone: addr.Zone}
		if address.IP.IsLinkLocalUnicast() && address.Zone == "" {
			return nil, errors.New("TCP endpoint link-local address requires a zone")
		}
		if address.Zone != "" && !address.IP.IsLinkLocalUnicast() {
			return nil, errors.New("TCP endpoint zone requires a link-local address")
		}
		if address.Zone != "" {
			iface, err := vectorPartitionProductionInterfaceForZoneV1(address.Zone)
			if err != nil {
				return nil, fmt.Errorf("TCP endpoint zone %q is invalid: %w", address.Zone, err)
			}
			address.Zone = strconv.Itoa(iface.Index)
		}
		key := address.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		addresses = append(addresses, address)
	}
	if len(addresses) == 0 {
		return nil, errors.New("TCP endpoint host is unspecified")
	}
	return addresses, nil
}

func vectorPartitionProductionInterfaceForZoneV1(zone string) (*net.Interface, error) {
	if index, err := strconv.Atoi(zone); err == nil {
		return net.InterfaceByIndex(index)
	}
	return net.InterfaceByName(zone)
}

func vectorPartitionProductionEndpointKeysV1(endpoint string) ([]string, error) {
	resolved, err := vectorPartitionProductionEndpointAddressesV1(endpoint)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(resolved))
	for i, address := range resolved {
		keys[i] = address.String()
	}
	return keys, nil
}

func (h *VectorPartitionProductionTopologyV1) Close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		var errs []error
		h.mu.Lock()
		h.closed = true
		for _, listener := range h.listeners {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		for conn := range h.conns {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		h.mu.Unlock()
		h.wg.Wait()
		if h.dispatcher != nil {
			errs = append(errs, h.dispatcher.Close())
		}
		if h.servingSnapshot != nil {
			errs = append(errs, h.servingSnapshot.Close())
		}
		if h.coordinator != nil {
			errs = append(errs, h.coordinator.Close())
		}
		h.closeErr = errors.Join(errs...)
	})
	return h.closeErr
}
