package nativewire

// Production vector-partition topology composes already-authoritative catalog,
// lifecycle, read-coordinator, and TCP service dependencies.  It deliberately
// does not create Raft groups, catalogs, or vector assets.

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	Shards              []VectorPartitionProductionShardV1
	CoordinatorLimits   VectorPartitionCoordinatorLimitsV1
	ShardLimits         VectorPartitionShardSearchLimitsV1
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
	listeners   map[raftcluster.GroupID]net.Listener
	endpoints   map[raftcluster.GroupID]string
	services    map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1
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
	if err := opts.Catalog.ValidateVectorPartitionPlacementV1(opts.Placement); err != nil {
		return nil, fmt.Errorf("nativewire: production vector topology placement: %w", err)
	}
	if len(opts.Endpoints) == 0 || len(opts.Shards) == 0 {
		return nil, errors.New("nativewire: production vector topology requires endpoints and local shards")
	}
	h := &VectorPartitionProductionTopologyV1{listeners: make(map[raftcluster.GroupID]net.Listener), endpoints: make(map[raftcluster.GroupID]string, len(opts.Endpoints)), services: make(map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1), conns: make(map[net.Conn]struct{})}
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
	for _, shard := range opts.Shards {
		if shard.GroupID == "" || shard.Listener == nil || shard.Service == nil || !owners[shard.GroupID] || h.listeners[shard.GroupID] != nil {
			return nil, fmt.Errorf("nativewire: production vector topology shard %q is invalid", shard.GroupID)
		}
		if endpoint := h.endpoints[shard.GroupID]; endpoint == "" || endpoint != shard.Listener.Addr().String() {
			return nil, fmt.Errorf("nativewire: production vector topology shard %q endpoint does not match listener", shard.GroupID)
		}
		h.listeners[shard.GroupID], h.services[shard.GroupID] = shard.Listener, shard.Service
	}
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(h.endpoints)
	if err != nil {
		return nil, err
	}
	h.coordinator, err = NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1{Catalog: opts.Catalog, Placement: opts.Placement, RouterSource: opts.RouterSource, Dispatcher: dispatcher, ReplicatedLifecycle: opts.ReplicatedLifecycle, RequireReplicatedLifecycle: true, Limits: opts.CoordinatorLimits, ShardLimits: opts.ShardLimits})
	if err != nil {
		return nil, err
	}
	for group, listener := range h.listeners {
		h.serve(group, listener, h.services[group])
	}
	return h, nil
}

func (h *VectorPartitionProductionTopologyV1) serve(_ raftcluster.GroupID, listener net.Listener, service *VectorPartitionShardSearchServiceV1) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			h.mu.Lock()
			if h.closed {
				h.mu.Unlock()
				_ = conn.Close()
				return
			}
			h.conns[conn] = struct{}{}
			h.wg.Add(1)
			h.mu.Unlock()
			go func() {
				defer h.wg.Done()
				defer func() { h.mu.Lock(); delete(h.conns, conn); h.mu.Unlock() }()
				(VectorPartitionShardSearchTCPServerV1{Service: service, InitialTimeout: 2 * time.Second}).ServeConn(context.Background(), conn)
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
	status := VectorPartitionProductionTopologyStatusV1{Ready: !h.closed && h.coordinator != nil && len(h.listeners) != 0, Closed: h.closed, Endpoints: make(map[raftcluster.GroupID]string, len(h.endpoints))}
	for group, endpoint := range h.endpoints {
		status.Endpoints[group] = endpoint
	}
	for group := range h.listeners {
		status.ShardGroups = append(status.ShardGroups, group)
	}
	slices.Sort(status.ShardGroups)
	return status
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
		if h.coordinator != nil {
			errs = append(errs, h.coordinator.Close())
		}
		h.closeErr = errors.Join(errs...)
	})
	return h.closeErr
}
