package nativewire

// Production vector-partition topology composes already-authoritative catalog,
// lifecycle, read-coordinator, and TCP service dependencies.  It deliberately
// does not create Raft groups, catalogs, or vector assets.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

type VectorPartitionProductionShardV1 struct {
	GroupID  raftcluster.GroupID
	Listener net.Listener
	Service  *VectorPartitionShardSearchServiceV1
}

type VectorPartitionProductionTopologyOptionsV1 struct {
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
	coordinator *VectorPartitionCoordinatorV1
	dispatcher  *VectorPartitionShardSearchTCPDispatcherV1
	listeners   map[raftcluster.GroupID]net.Listener
	endpoints   map[raftcluster.GroupID]string
	services    map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1
	serving     map[raftcluster.GroupID]bool
	mu          sync.Mutex
	conns       map[net.Conn]struct{}
	wg          sync.WaitGroup
	closed      bool
	closeOnce   sync.Once
	closeErr    error
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
	h := &VectorPartitionProductionTopologyV1{listeners: make(map[raftcluster.GroupID]net.Listener), endpoints: make(map[raftcluster.GroupID]string, len(opts.Endpoints)), services: make(map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1), serving: make(map[raftcluster.GroupID]bool), conns: make(map[net.Conn]struct{})}
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
		h.listeners[shard.GroupID], h.services[shard.GroupID] = shard.Listener, shard.Service
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
	for group, listener := range h.listeners {
		// Keep accepted sockets bounded without letting one dispatcher pool consume
		// the whole listener while its keep-alives are idle.
		h.serve(group, listener, h.services[group], opts.ShardIdleTimeout, coordinatorLimits.MaxRequests, dispatcher.maxRequestFrame, dispatcher.maxResponseFrame)
	}
	return h, nil
}

func (h *VectorPartitionProductionTopologyV1) serve(group raftcluster.GroupID, listener net.Listener, service *VectorPartitionShardSearchServiceV1, idleTimeout time.Duration, maxConnections int, maxRequestFrame, maxResponseFrame uint32) {
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
				(VectorPartitionShardSearchTCPServerV1{Service: service, MaxFrame: maxRequestFrame, MaxResponseFrame: maxResponseFrame, InitialTimeout: idleTimeout}).ServeConn(context.Background(), conn)
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
		return bound.IP.Equal(advertised.IP)
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
		if h.coordinator != nil {
			errs = append(errs, h.coordinator.Close())
		}
		h.closeErr = errors.Join(errs...)
	})
	return h.closeErr
}
