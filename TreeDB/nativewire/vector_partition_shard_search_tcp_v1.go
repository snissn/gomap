package nativewire

// This file provides the M8 production-topology transport boundary for M5
// shard search.  It deliberately carries the existing validated M5 envelope
// rather than introducing a second shard protocol.  The coordinator still
// owns fanout, retry, and no-partial-result semantics.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	vectorPartitionShardSearchTCPMaxFrameBytesV1      uint32 = 64 << 20
	vectorPartitionShardSearchTCPMinFrameBytesV1      uint64 = 4 << 10
	vectorPartitionShardSearchTCPFrameVersionV1       byte   = 1
	vectorPartitionShardSearchTCPFrameRequestV1       byte   = 1
	vectorPartitionShardSearchTCPFrameResponseV1      byte   = 2
	vectorPartitionShardSearchTCPFrameErrorV1         byte   = 3
	vectorPartitionShardSearchTCPFrameProbeV1         byte   = 4
	vectorPartitionShardSearchTCPFrameProbeResponseV1 byte   = 5
	// Fixed bytes include the frame-body header and every fixed-width request
	// field plus the length prefix for each request string.
	vectorPartitionShardSearchTCPRequestFixedBytesV1 uint64 = 151
	// The capability fixed bytes include its six string length prefixes.
	vectorPartitionStrictCapabilityFixedBytesV1 uint64 = 84
	// Response byte accounting excludes the request ID and six proof strings.
	vectorPartitionShardSearchTCPResponseIdentityFieldsV1   uint64 = 7
	vectorPartitionShardSearchTCPResponsePartialMinBytesV1         = 60
	vectorPartitionShardSearchTCPResponseNeighborMinBytesV1        = 8
	vectorPartitionShardSearchTCPProbeResponseFixedBytesV1  uint64 = 934
)

// VectorPartitionShardSearchTCPDispatcherV1 is a bounded, length-framed TCP
// dispatcher for distinct M5 service endpoints.  It is intentionally not an
// in-process registry: every request and response crosses a serialized socket
// boundary before the coordinator can consume it.
type VectorPartitionShardSearchTCPDispatcherV1 struct {
	endpoints                 map[raftcluster.GroupID]string
	nodeEndpoints             map[raftcluster.GroupID]map[raftcluster.NodeID]string
	dial                      func(context.Context, string, string) (net.Conn, error)
	maxRequestFrame           uint32
	maxResponseFrame          uint32
	maxConnectionsPerEndpoint int
	mu                        sync.Mutex
	pools                     map[string]*vectorPartitionShardSearchTCPEndpointPoolV1
	closed                    bool
}

type vectorPartitionShardSearchTCPEndpointPoolV1 struct {
	slots  chan struct{}
	mu     sync.Mutex
	idle   []net.Conn
	all    map[net.Conn]struct{}
	closed bool
}

type vectorPartitionShardSearchTCPTransportFailureV1 struct{ err error }

func (e *vectorPartitionShardSearchTCPTransportFailureV1) Error() string { return e.err.Error() }
func (e *vectorPartitionShardSearchTCPTransportFailureV1) Unwrap() error { return e.err }

// NewVectorPartitionShardSearchTCPDispatcherV1 validates and copies one TCP
// endpoint per group.  Endpoints are normally loopback addresses for the M8 CI
// topology and may be separate hosts in a deeper deployment.
func NewVectorPartitionShardSearchTCPDispatcherV1(endpoints map[raftcluster.GroupID]string) (*VectorPartitionShardSearchTCPDispatcherV1, error) {
	return NewVectorPartitionShardSearchTCPDispatcherWithNodeEndpointsV1(endpoints, nil)
}

// NewVectorPartitionShardSearchTCPDispatcherWithNodeEndpointsV1 additionally
// routes a coordinator retry to the leader-hinted node when that endpoint is
// known. Group endpoints remain the safe fallback for one service per group.
func NewVectorPartitionShardSearchTCPDispatcherWithNodeEndpointsV1(endpoints map[raftcluster.GroupID]string, nodeEndpoints map[raftcluster.GroupID]map[raftcluster.NodeID]string) (*VectorPartitionShardSearchTCPDispatcherV1, error) {
	return newVectorPartitionShardSearchTCPDispatcherV1(endpoints, nodeEndpoints, DefaultVectorPartitionCoordinatorLimitsV1().MaxConcurrentRequests, DefaultVectorPartitionShardSearchLimitsV1())
}

func newVectorPartitionShardSearchTCPDispatcherV1(endpoints map[raftcluster.GroupID]string, nodeEndpoints map[raftcluster.GroupID]map[raftcluster.NodeID]string, maxConnectionsPerEndpoint int, limits VectorPartitionShardSearchLimitsV1) (*VectorPartitionShardSearchTCPDispatcherV1, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("nativewire: M5 TCP dispatcher requires endpoints")
	}
	if maxConnectionsPerEndpoint < 1 {
		return nil, errors.New("nativewire: M5 TCP dispatcher requires a positive per-endpoint connection limit")
	}
	limits, err := normalizeVectorPartitionShardSearchLimitsV1(limits)
	if err != nil {
		return nil, err
	}
	maxRequestFrame, err := vectorPartitionShardSearchTCPFrameBoundV1(limits.MaxRequestBytes)
	if err != nil {
		return nil, err
	}
	maxResponseFrame, err := vectorPartitionShardSearchTCPResponseFrameBoundV1(limits)
	if err != nil {
		return nil, err
	}
	copyEndpoints := make(map[raftcluster.GroupID]string, len(endpoints))
	for group, endpoint := range endpoints {
		if group == "" || endpoint == "" {
			return nil, errors.New("nativewire: M5 TCP dispatcher endpoint is incomplete")
		}
		copyEndpoints[group] = endpoint
	}
	dialer := &net.Dialer{}
	copyNodeEndpoints := make(map[raftcluster.GroupID]map[raftcluster.NodeID]string, len(nodeEndpoints))
	for group, nodes := range nodeEndpoints {
		if _, ok := copyEndpoints[group]; !ok {
			return nil, errors.New("nativewire: M5 TCP node endpoint owner is unknown")
		}
		copyNodes := make(map[raftcluster.NodeID]string, len(nodes))
		for node, endpoint := range nodes {
			if node == "" || endpoint == "" {
				return nil, errors.New("nativewire: M5 TCP node endpoint is incomplete")
			}
			copyNodes[node] = endpoint
		}
		copyNodeEndpoints[group] = copyNodes
	}
	return &VectorPartitionShardSearchTCPDispatcherV1{
		endpoints:                 copyEndpoints,
		nodeEndpoints:             copyNodeEndpoints,
		dial:                      dialer.DialContext,
		maxRequestFrame:           maxRequestFrame,
		maxResponseFrame:          maxResponseFrame,
		maxConnectionsPerEndpoint: maxConnectionsPerEndpoint,
		pools:                     make(map[string]*vectorPartitionShardSearchTCPEndpointPoolV1),
	}, nil
}

func (d *VectorPartitionShardSearchTCPDispatcherV1) DispatchVectorPartitionShardSearchV1(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	if d == nil || d.dial == nil || d.maxRequestFrame == 0 || d.maxResponseFrame == 0 {
		return VectorPartitionShardSearchResponseV1{}, errors.New("nativewire: M5 TCP dispatcher is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 0; ; attempt++ {
		response, err := d.dispatchVectorPartitionShardSearchOnceV1(ctx, request)
		if err == nil || attempt != 0 || ctx.Err() != nil || !vectorPartitionShardSearchTCPReconnectableV1(err) {
			return response, err
		}
		d.discardIdle(request.TargetGroupID, request.TargetNodeID)
	}
}

func (d *VectorPartitionShardSearchTCPDispatcherV1) dispatchVectorPartitionShardSearchOnceV1(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	requestCtx, cancel := vectorPartitionShardSearchTCPRequestContextV1(ctx, request.DeadlineUnixNano)
	defer cancel()
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, GroupID: request.TargetGroupID, Err: errors.New("nativewire: M5 TCP dispatcher is closed")}
	}
	endpoint, ok := d.endpoint(request.TargetGroupID, request.TargetNodeID)
	if !ok {
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorUnknownOwnerV1, GroupID: request.TargetGroupID, Err: ErrVectorPartitionShardSearchRouteMismatch}
	}
	pool, conn, err := d.acquire(requestCtx, endpoint)
	if err != nil {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPRetryableTransportErrorV1(requestCtx, request.TargetGroupID, err)
	}
	reusable := false
	defer func() { pool.release(conn, reusable) }()
	stopCancelIO := vectorPartitionShardSearchTCPInterruptOnCancelV1(requestCtx, conn)
	defer stopCancelIO()
	if err := vectorPartitionShardSearchTCPDeadlineV1(requestCtx, request.DeadlineUnixNano, conn); err != nil {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPRetryableTransportErrorV1(requestCtx, request.TargetGroupID, err)
	}
	frame := vectorPartitionShardSearchTCPFrameV1{Request: &request}
	if err := writeVectorPartitionShardSearchTCPFrameV1(conn, frame, d.maxRequestFrame); err != nil {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPRetryableTransportErrorV1(requestCtx, request.TargetGroupID, err)
	}
	frame, err = readVectorPartitionShardSearchTCPResponseFrameV1(conn, d.maxResponseFrame, request)
	if err != nil {
		if request.DeadlineUnixNano != 0 && !time.Now().Before(time.Unix(0, request.DeadlineUnixNano)) {
			return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorDeadlineV1, GroupID: request.TargetGroupID, Err: context.DeadlineExceeded}
		}
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPRetryableTransportErrorV1(requestCtx, request.TargetGroupID, err)
	}
	if frame.Request != nil || frame.Probe != nil || frame.ProbeResponse != nil || (frame.Response == nil) == (frame.Error == nil) {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPTransportErrorV1(ctx, request.TargetGroupID, errors.New("ambiguous M5 response frame"))
	}
	stopCancelIO()
	reusable = conn.SetDeadline(time.Time{}) == nil
	if frame.Error != nil {
		return VectorPartitionShardSearchResponseV1{}, frame.Error.toError()
	}
	return *frame.Response, nil
}

func (d *VectorPartitionShardSearchTCPDispatcherV1) endpoint(group raftcluster.GroupID, node raftcluster.NodeID) (string, bool) {
	if nodes := d.nodeEndpoints[group]; nodes != nil && node != "" {
		if endpoint, ok := nodes[node]; ok {
			return endpoint, true
		}
	}
	endpoint, ok := d.endpoints[group]
	return endpoint, ok
}
func (d *VectorPartitionShardSearchTCPDispatcherV1) acquire(ctx context.Context, endpoint string) (*vectorPartitionShardSearchTCPEndpointPoolV1, net.Conn, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, nil, net.ErrClosed
	}
	pool := d.pools[endpoint]
	if pool == nil {
		pool = &vectorPartitionShardSearchTCPEndpointPoolV1{
			slots: make(chan struct{}, d.maxConnectionsPerEndpoint),
			all:   make(map[net.Conn]struct{}),
		}
		d.pools[endpoint] = pool
	}
	d.mu.Unlock()
	conn, err := pool.acquire(ctx, d.dial, endpoint)
	return pool, conn, err
}

func (d *VectorPartitionShardSearchTCPDispatcherV1) discardIdle(group raftcluster.GroupID, node raftcluster.NodeID) {
	endpoint, ok := d.endpoint(group, node)
	if !ok {
		return
	}
	d.mu.Lock()
	pool := d.pools[endpoint]
	d.mu.Unlock()
	if pool != nil {
		pool.discardIdle()
	}
}

func (p *vectorPartitionShardSearchTCPEndpointPoolV1) acquire(ctx context.Context, dial func(context.Context, string, string) (net.Conn, error), endpoint string) (net.Conn, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.slots
		return nil, net.ErrClosed
	}
	if n := len(p.idle); n != 0 {
		conn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()
	conn, err := dial(ctx, "tcp", endpoint)
	if err != nil {
		<-p.slots
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = conn.Close()
		<-p.slots
		return nil, net.ErrClosed
	}
	p.all[conn] = struct{}{}
	p.mu.Unlock()
	return conn, nil
}
func (p *vectorPartitionShardSearchTCPEndpointPoolV1) release(conn net.Conn, reusable bool) {
	p.mu.Lock()
	closed := p.closed
	if reusable && !closed {
		p.idle = append(p.idle, conn)
	} else {
		delete(p.all, conn)
	}
	p.mu.Unlock()
	if !reusable || closed {
		_ = conn.Close()
	}
	<-p.slots
}

func (p *vectorPartitionShardSearchTCPEndpointPoolV1) discardIdle() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	for _, conn := range idle {
		delete(p.all, conn)
	}
	p.mu.Unlock()
	for _, conn := range idle {
		_ = conn.Close()
	}
}
func (p *vectorPartitionShardSearchTCPEndpointPoolV1) close() error {
	p.mu.Lock()
	p.closed = true
	conns := p.all
	p.all = make(map[net.Conn]struct{})
	p.idle = nil
	p.mu.Unlock()
	var errs []error
	for conn := range conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}
func (d *VectorPartitionShardSearchTCPDispatcherV1) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	pools := d.pools
	d.pools = make(map[string]*vectorPartitionShardSearchTCPEndpointPoolV1)
	d.mu.Unlock()
	var errs []error
	for _, pool := range pools {
		errs = append(errs, pool.close())
	}
	return errors.Join(errs...)
}

func vectorPartitionShardSearchTCPReconnectableV1(err error) bool {
	var transportErr *vectorPartitionShardSearchTCPTransportFailureV1
	if !errors.As(err, &transportErr) {
		return false
	}
	var shardErr *VectorPartitionShardSearchErrorV1
	return errors.As(err, &shardErr) && shardErr.Code == VectorPartitionShardSearchErrorGroupUnavailableV1
}

// VectorPartitionShardSearchTCPServerV1 serves one M5 service over the same
// bounded framing contract used by the dispatcher.
type VectorPartitionShardSearchTCPServerV1 struct {
	Service                  VectorPartitionShardSearchHandlerV1
	EndpointIdentity         VectorPartitionShardEndpointIdentityV1
	EndpointIdentityProvider func() VectorPartitionShardEndpointIdentityV1
	MaxFrame                 uint32
	MaxResponseFrame         uint32
	InitialTimeout           time.Duration
}

// VectorPartitionShardEndpointIdentityV1 binds a live shard listener to the
// retained node configuration that opened it.
type VectorPartitionShardEndpointIdentityV1 struct {
	Version              uint32
	GroupID              string
	InstanceIdentity     string
	CatalogMetaReadStats raftcluster.CatalogMetaLinearizableReadStatsV1
	ProcessRuntimeStats  VectorPartitionProcessRuntimeStatsV1
}

type VectorPartitionProcessRuntimeStatsV1 struct {
	SampleUnixNano              uint64 `json:"sample_unix_nano"`
	CPUTimeNanos                uint64 `json:"cpu_time_nanos"`
	RunQueueDelayNanos          uint64 `json:"run_queue_delay_nanos"`
	Timeslices                  uint64 `json:"timeslices"`
	VoluntaryContextSwitches    uint64 `json:"voluntary_context_switches"`
	NonvoluntaryContextSwitches uint64 `json:"nonvoluntary_context_switches"`
	RSSBytes                    uint64 `json:"rss_bytes"`
	PeakRSSBytes                uint64 `json:"peak_rss_bytes"`
	HeapAllocBytes              uint64 `json:"heap_alloc_bytes"`
	HeapObjects                 uint64 `json:"heap_objects"`
	TotalAllocBytes             uint64 `json:"total_alloc_bytes"`
	Mallocs                     uint64 `json:"mallocs"`
	Frees                       uint64 `json:"frees"`
	NumGC                       uint64 `json:"num_gc"`
	PauseTotalNanos             uint64 `json:"pause_total_nanos"`
	Goroutines                  uint64 `json:"goroutines"`
	LogicalCPUs                 int    `json:"logical_cpus"`
	GOMAXPROCS                  int    `json:"gomaxprocs"`
	GoMemoryLimitBytes          int64  `json:"go_memory_limit_bytes"`
	EffectiveCPUSet             string `json:"effective_cpu_set,omitempty"`
}

// VectorPartitionCatalogMetaLinearizableReadStatsV1 is the bounded catalog
// read evidence published by a production shard endpoint.
type VectorPartitionCatalogMetaLinearizableReadStatsV1 = raftcluster.CatalogMetaLinearizableReadStatsV1

// VectorPartitionCatalogMetaLinearizableReadStageStatsV1 is one attributed
// source within the published catalog read evidence.
type VectorPartitionCatalogMetaLinearizableReadStageStatsV1 = raftcluster.CatalogMetaLinearizableReadStageStatsV1

type vectorPartitionShardEndpointProbeV1 struct {
	Version uint32
}

// VectorPartitionShardSearchHandlerV1 is the narrow server dependency needed
// by the TCP boundary.  The production service satisfies it directly.
type VectorPartitionShardSearchHandlerV1 interface {
	Search(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error)
}

func (s VectorPartitionShardSearchTCPServerV1) ServeConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	maxFrame := s.MaxFrame
	if maxFrame == 0 {
		maxFrame = vectorPartitionShardSearchTCPMaxFrameBytesV1
	}
	maxResponseFrame := s.MaxResponseFrame
	if maxResponseFrame == 0 {
		var err error
		maxResponseFrame, err = vectorPartitionShardSearchTCPResponseFrameBoundV1(DefaultVectorPartitionShardSearchLimitsV1())
		if err != nil {
			return
		}
	}
	initialTimeout := s.InitialTimeout
	if initialTimeout < 0 {
		return
	}
	if initialTimeout == 0 {
		initialTimeout = 5 * time.Second
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(initialTimeout))
		frame, err := readVectorPartitionShardSearchTCPFrameV1(conn, maxFrame)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			return
		}
		if frame.Probe != nil && frame.Request == nil && frame.Response == nil && frame.Error == nil && frame.ProbeResponse == nil {
			identity := s.EndpointIdentity
			if s.EndpointIdentityProvider != nil {
				identity = s.EndpointIdentityProvider()
			}
			if frame.Probe.Version != 1 || identity.Version != 1 || identity.GroupID == "" || identity.InstanceIdentity == "" {
				_ = s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorInvalidRequestV1, Message: "M5 endpoint identity is unavailable"}}, maxResponseFrame, time.Now().Add(initialTimeout))
				return
			}
			if s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{ProbeResponse: &identity}, maxResponseFrame, time.Now().Add(initialTimeout)) != nil {
				return
			}
			continue
		}
		if frame.Request == nil || frame.Probe != nil || frame.Response != nil || frame.Error != nil || frame.ProbeResponse != nil {
			// A peer that sends no frame (or never reads) must not strand this server
			// goroutine while we try to report the bounded framing failure.
			_ = conn.SetWriteDeadline(time.Now().Add(initialTimeout))
			_ = writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorInvalidRequestV1, Message: "invalid M5 TCP request"}}, maxResponseFrame)
			return
		}
		if s.Service == nil {
			_ = s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, GroupID: frame.Request.TargetGroupID, Message: "M5 service is unavailable"}}, maxResponseFrame, time.Now().Add(initialTimeout))
			return
		}
		requestCtx, cancel := vectorPartitionShardSearchTCPRequestContextV1(ctx, frame.Request.DeadlineUnixNano)
		stopPeerMonitor := vectorPartitionShardSearchTCPMonitorPeerDisconnectV1(conn, requestCtx, cancel)
		response, err := s.Service.Search(requestCtx, *frame.Request)
		peerInput := stopPeerMonitor()
		cancel()
		if peerInput {
			return
		}
		writeDeadline := time.Now().Add(initialTimeout)
		if deadline, ok := requestCtx.Deadline(); ok {
			writeDeadline = deadline
		}
		if err != nil {
			if s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{Error: vectorPartitionShardSearchTCPErrorFromErrorV1(err)}, maxResponseFrame, writeDeadline) != nil {
				return
			}
			continue
		}
		if s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{Response: &response}, maxResponseFrame, writeDeadline) != nil {
			return
		}
	}
}

func (s VectorPartitionShardSearchTCPServerV1) writeFrame(conn net.Conn, frame vectorPartitionShardSearchTCPFrameV1, maxFrame uint32, deadline time.Time) error {
	_ = conn.SetWriteDeadline(deadline)
	return writeVectorPartitionShardSearchTCPFrameV1(conn, frame, maxFrame)
}

type vectorPartitionShardSearchTCPFrameV1 struct {
	Request       *VectorPartitionShardSearchRequestV1
	Response      *VectorPartitionShardSearchResponseV1
	Probe         *vectorPartitionShardEndpointProbeV1
	ProbeResponse *VectorPartitionShardEndpointIdentityV1
	Error         *vectorPartitionShardSearchTCPErrorV1
}

// ProbeVectorPartitionShardEndpointV1 returns the identity published by the
// live shard listener without executing a search.
func ProbeVectorPartitionShardEndpointV1(ctx context.Context, endpoint string) (VectorPartitionShardEndpointIdentityV1, error) {
	var identity VectorPartitionShardEndpointIdentityV1
	if endpoint == "" {
		return identity, errors.New("nativewire: M5 endpoint probe requires an endpoint")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(probeCtx, "tcp", endpoint)
	if err != nil {
		return identity, err
	}
	defer conn.Close()
	if err := vectorPartitionShardSearchTCPDeadlineV1(probeCtx, 0, conn); err != nil {
		return identity, err
	}
	if err := writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{Probe: &vectorPartitionShardEndpointProbeV1{Version: 1}}, uint32(vectorPartitionShardSearchTCPMinFrameBytesV1)); err != nil {
		return identity, err
	}
	frame, err := readVectorPartitionShardSearchTCPFrameV1(conn, uint32(vectorPartitionShardSearchTCPMinFrameBytesV1))
	if err != nil {
		return identity, err
	}
	if frame.Error != nil {
		return identity, frame.Error.toError()
	}
	if frame.ProbeResponse == nil || frame.Request != nil || frame.Response != nil || frame.Probe != nil || frame.ProbeResponse.Version != 1 || frame.ProbeResponse.GroupID == "" || frame.ProbeResponse.InstanceIdentity == "" {
		return identity, errors.New("nativewire: invalid M5 endpoint identity response")
	}
	return *frame.ProbeResponse, nil
}

type vectorPartitionShardSearchTCPErrorV1 struct {
	Code       VectorPartitionShardSearchErrorCodeV1
	GroupID    raftcluster.GroupID
	LeaderHint raftcluster.NodeID
	Message    string
}

func vectorPartitionShardSearchTCPErrorFromErrorV1(err error) *vectorPartitionShardSearchTCPErrorV1 {
	var shardErr *VectorPartitionShardSearchErrorV1
	if errors.As(err, &shardErr) {
		return &vectorPartitionShardSearchTCPErrorV1{Code: shardErr.Code, GroupID: shardErr.GroupID, LeaderHint: shardErr.LeaderHint, Message: shardErr.Error()}
	}
	return &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, Message: err.Error()}
}

func (e vectorPartitionShardSearchTCPErrorV1) toError() error {
	if e.Code == "" {
		e.Code = VectorPartitionShardSearchErrorGroupUnavailableV1
	}
	return &VectorPartitionShardSearchErrorV1{Code: e.Code, GroupID: e.GroupID, LeaderHint: e.LeaderHint, Err: errors.New(e.Message)}
}

func writeVectorPartitionShardSearchTCPFrameV1(w io.Writer, frame vectorPartitionShardSearchTCPFrameV1, max uint32) error {
	raw, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, frame)
	if err != nil {
		return err
	}
	if len(raw) == 0 || uint64(len(raw)) > uint64(max) {
		return fmt.Errorf("M5 TCP frame is outside %d-byte bound", max)
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	if err := writeVectorPartitionShardSearchTCPAllV1(w, prefix[:]); err != nil {
		return err
	}
	return writeVectorPartitionShardSearchTCPAllV1(w, raw)
}

func writeVectorPartitionShardSearchTCPAllV1(w io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := w.Write(raw)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}

func readVectorPartitionShardSearchTCPFrameV1(r io.Reader, max uint32) (vectorPartitionShardSearchTCPFrameV1, error) {
	return readVectorPartitionShardSearchTCPFrameWithResponseBoundsV1(r, max, math.MaxInt, math.MaxInt)
}

func readVectorPartitionShardSearchTCPResponseFrameV1(r io.Reader, max uint32, request VectorPartitionShardSearchRequestV1) (vectorPartitionShardSearchTCPFrameV1, error) {
	return readVectorPartitionShardSearchTCPFrameWithResponseBoundsV1(r, max, len(request.PartitionIDs), request.TopK)
}

func readVectorPartitionShardSearchTCPFrameWithResponseBoundsV1(r io.Reader, max uint32, maxPartials, maxNeighbors int) (vectorPartitionShardSearchTCPFrameV1, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size == 0 || size > max {
		return vectorPartitionShardSearchTCPFrameV1{}, fmt.Errorf("M5 TCP frame size %d is outside %d-byte bound", size, max)
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	return decodeVectorPartitionShardSearchTCPFrameBodyWithResponseBoundsV1(raw, maxPartials, maxNeighbors)
}

type vectorPartitionShardSearchTCPBodyWriterV1 struct {
	body []byte
	err  error
}

func (w *vectorPartitionShardSearchTCPBodyWriterV1) u8(v byte) {
	w.body = append(w.body, v)
}

func (w *vectorPartitionShardSearchTCPBodyWriterV1) u32(v uint32) {
	w.body = binary.LittleEndian.AppendUint32(w.body, v)
}

func (w *vectorPartitionShardSearchTCPBodyWriterV1) u64(v uint64) {
	w.body = binary.LittleEndian.AppendUint64(w.body, v)
}

func (w *vectorPartitionShardSearchTCPBodyWriterV1) i64(v int64) {
	w.u64(uint64(v))
}

func (w *vectorPartitionShardSearchTCPBodyWriterV1) f32(v float32) {
	if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
		w.err = errors.New("M5 TCP float is nonfinite")
		return
	}
	w.u32(math.Float32bits(v))
}

func (w *vectorPartitionShardSearchTCPBodyWriterV1) string(v string) {
	if uint64(len(v)) > math.MaxUint32 && w.err == nil {
		w.err = errors.New("M5 TCP string exceeds uint32")
		return
	}
	w.u32(uint32(len(v)))
	w.body = append(w.body, v...)
}

type vectorPartitionShardSearchTCPBodyReaderV1 struct {
	body  []byte
	owned string
	off   int
	err   error
}

func (r *vectorPartitionShardSearchTCPBodyReaderV1) remaining() int {
	if r.err != nil {
		return 0
	}
	return len(r.body) - r.off
}

func (r *vectorPartitionShardSearchTCPBodyReaderV1) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.body)-r.off {
		r.err = errors.New("M5 TCP frame is truncated")
		return nil
	}
	out := r.body[r.off : r.off+n]
	r.off += n
	return out
}
func (r *vectorPartitionShardSearchTCPBodyReaderV1) u8() byte {
	b := r.bytes(1)
	if len(b) == 0 {
		return 0
	}
	return b[0]
}
func (r *vectorPartitionShardSearchTCPBodyReaderV1) u32() uint32 {
	b := r.bytes(4)
	if len(b) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}
func (r *vectorPartitionShardSearchTCPBodyReaderV1) u64() uint64 {
	b := r.bytes(8)
	if len(b) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (r *vectorPartitionShardSearchTCPBodyReaderV1) i64() int64 {
	return int64(r.u64())
}

func (r *vectorPartitionShardSearchTCPBodyReaderV1) f32() float32 {
	v := math.Float32frombits(r.u32())
	if r.err == nil && (math.IsNaN(float64(v)) || math.IsInf(float64(v), 0)) {
		r.err = errors.New("M5 TCP float is nonfinite")
	}
	return v
}
func (r *vectorPartitionShardSearchTCPBodyReaderV1) string() string {
	n := r.u32()
	if r.err != nil {
		return ""
	}
	if uint64(n) > uint64(len(r.body)-r.off) {
		r.err = errors.New("M5 TCP string exceeds remaining body")
		return ""
	}
	start := r.off
	r.bytes(int(n))
	return r.owned[start:r.off]
}
func (r *vectorPartitionShardSearchTCPBodyReaderV1) count(maximum int) int {
	n := r.u32()
	if r.err != nil {
		return 0
	}
	if maximum < 0 || uint64(n) > uint64(maximum) {
		r.err = errors.New("M5 TCP count exceeds bound")
		return 0
	}
	return int(n)
}
func (r *vectorPartitionShardSearchTCPBodyReaderV1) done() error {
	if r.err != nil {
		return r.err
	}
	if r.off != len(r.body) {
		return errors.New("M5 TCP frame has trailing bytes")
	}
	return nil
}

func appendVectorPartitionShardSearchTCPFrameBodyV1(dst []byte, frame vectorPartitionShardSearchTCPFrameV1) ([]byte, error) {
	count := 0
	typ := byte(0)
	if frame.Request != nil {
		count++
		typ = vectorPartitionShardSearchTCPFrameRequestV1
	}
	if frame.Response != nil {
		count++
		typ = vectorPartitionShardSearchTCPFrameResponseV1
	}
	if frame.Error != nil {
		count++
		typ = vectorPartitionShardSearchTCPFrameErrorV1
	}
	if frame.Probe != nil {
		count++
		typ = vectorPartitionShardSearchTCPFrameProbeV1
	}
	if frame.ProbeResponse != nil {
		count++
		typ = vectorPartitionShardSearchTCPFrameProbeResponseV1
	}
	if count != 1 {
		return nil, errors.New("M5 TCP frame requires exactly one body")
	}
	if dst == nil {
		capacity := 128
		switch typ {
		case vectorPartitionShardSearchTCPFrameRequestV1:
			capacity = 1024
			if requestBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(*frame.Request); err == nil && requestBytes <= uint64(maxInt) {
				capacity = int(requestBytes)
			}
		case vectorPartitionShardSearchTCPFrameResponseV1, vectorPartitionShardSearchTCPFrameProbeResponseV1:
			capacity = 2048
		case vectorPartitionShardSearchTCPFrameProbeV1:
			capacity = 16
		}
		dst = make([]byte, 0, capacity)
	}
	w := vectorPartitionShardSearchTCPBodyWriterV1{body: dst}
	w.u8(vectorPartitionShardSearchTCPFrameVersionV1)
	w.u8(typ)
	w.u32(0)
	switch typ {
	case vectorPartitionShardSearchTCPFrameRequestV1:
		appendVectorPartitionShardSearchTCPRequestV1(&w, *frame.Request)
	case vectorPartitionShardSearchTCPFrameResponseV1:
		appendVectorPartitionShardSearchTCPResponseV1(&w, *frame.Response)
	case vectorPartitionShardSearchTCPFrameErrorV1:
		appendVectorPartitionShardSearchTCPErrorV1(&w, *frame.Error)
	case vectorPartitionShardSearchTCPFrameProbeV1:
		w.u32(frame.Probe.Version)
	case vectorPartitionShardSearchTCPFrameProbeResponseV1:
		appendVectorPartitionShardSearchTCPEndpointIdentityV1(&w, *frame.ProbeResponse)
	}
	if w.err != nil {
		return nil, w.err
	}
	return w.body, nil
}

func decodeVectorPartitionShardSearchTCPFrameBodyV1(body []byte) (vectorPartitionShardSearchTCPFrameV1, error) {
	return decodeVectorPartitionShardSearchTCPFrameBodyWithResponseBoundsV1(body, math.MaxInt, math.MaxInt)
}

func decodeVectorPartitionShardSearchTCPFrameBodyWithResponseBoundsV1(body []byte, maxPartials, maxNeighbors int) (vectorPartitionShardSearchTCPFrameV1, error) {
	r := vectorPartitionShardSearchTCPBodyReaderV1{body: body}
	version, typ, flags := r.u8(), r.u8(), r.u32()
	if r.err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, r.err
	}
	if version != vectorPartitionShardSearchTCPFrameVersionV1 || flags != 0 {
		return vectorPartitionShardSearchTCPFrameV1{}, errors.New("unsupported M5 TCP frame version or flags")
	}
	if typ < vectorPartitionShardSearchTCPFrameRequestV1 || typ > vectorPartitionShardSearchTCPFrameProbeResponseV1 {
		return vectorPartitionShardSearchTCPFrameV1{}, errors.New("unknown M5 TCP frame type")
	}
	if typ != vectorPartitionShardSearchTCPFrameProbeV1 {
		r.owned = string(body)
	}
	var frame vectorPartitionShardSearchTCPFrameV1
	switch typ {
	case vectorPartitionShardSearchTCPFrameRequestV1:
		value := readVectorPartitionShardSearchTCPRequestV1(&r)
		frame.Request = &value
	case vectorPartitionShardSearchTCPFrameResponseV1:
		value := readVectorPartitionShardSearchTCPResponseWithBoundsV1(&r, maxPartials, maxNeighbors)
		frame.Response = &value
	case vectorPartitionShardSearchTCPFrameErrorV1:
		value := readVectorPartitionShardSearchTCPErrorV1(&r)
		frame.Error = &value
	case vectorPartitionShardSearchTCPFrameProbeV1:
		frame.Probe = &vectorPartitionShardEndpointProbeV1{Version: r.u32()}
		if r.err == nil && frame.Probe.Version != 1 {
			r.err = errors.New("unsupported M5 probe version")
		}
	case vectorPartitionShardSearchTCPFrameProbeResponseV1:
		value := readVectorPartitionShardSearchTCPEndpointIdentityV1(&r)
		frame.ProbeResponse = &value
		if r.err == nil && value.Version != 1 {
			r.err = errors.New("unsupported M5 probe response version")
		}
	}
	if err := r.done(); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	return frame, nil
}

func appendVectorPartitionShardSearchTCPRequestV1(w *vectorPartitionShardSearchTCPBodyWriterV1, r VectorPartitionShardSearchRequestV1) {
	w.u32(r.Version)
	for _, value := range []string{r.RequestID, r.CancellationID, r.Database, r.Catalog, r.Collection, r.IndexName, r.IndexDefinitionDigest, r.ReadySetDigest, string(r.TargetGroupID), string(r.TargetNodeID)} {
		w.string(value)
	}
	for _, value := range []uint64{r.SourceGeneration, r.SourceChecksum, r.SourceSchemaHash, r.SourceRowCount, r.PartitionGeneration, r.RouterGeneration} {
		w.u64(value)
	}
	if uint64(len(r.PartitionIDs)) > math.MaxUint32 {
		w.err = errors.New("M5 TCP partition count exceeds uint32")
		return
	}
	w.u32(uint32(len(r.PartitionIDs)))
	for _, value := range r.PartitionIDs {
		w.u32(value)
	}
	if uint64(len(r.Query)) > math.MaxUint32 {
		w.err = errors.New("M5 TCP query count exceeds uint32")
		return
	}
	w.u32(uint32(len(r.Query)))
	for _, value := range r.Query {
		w.f32(value)
	}
	metric := vectorPartitionShardSearchTCPMetricV1(r.Metric)
	mode := vectorPartitionShardSearchTCPModeV1(r.Mode)
	consistency := vectorPartitionShardSearchTCPConsistencyV1(r.Consistency)
	stats := vectorPartitionShardSearchTCPStatsV1(r.StatsMode)
	if (metric == 0 && r.Metric != "") || (mode == 0 && r.Mode != "") ||
		(consistency == 0 && r.Consistency != "") || (stats == 0 && r.StatsMode != "") {
		w.err = errors.New("M5 TCP request enum is invalid")
		return
	}
	w.u8(metric)
	w.u8(mode)
	w.u8(consistency)
	w.u8(stats)
	if r.TopK < 0 || uint64(r.TopK) > math.MaxUint32 || r.EfSearch < 0 || uint64(r.EfSearch) > math.MaxUint32 {
		w.err = errors.New("M5 TCP search count is outside uint32")
		return
	}
	w.u32(uint32(r.TopK))
	w.u32(uint32(r.EfSearch))
	w.i64(r.DeadlineUnixNano)
	w.u64(r.RequestBytesLimit)
	w.u64(r.CandidateBytesLimit)
	w.u64(r.ResponseBytesLimit)
	if r.StrictCapability == nil {
		w.u8(0)
	} else {
		w.u8(1)
		appendVectorPartitionStrictCapabilityBinaryV1(w, *r.StrictCapability)
	}
}

func readVectorPartitionShardSearchTCPRequestV1(r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchRequestV1 {
	value := VectorPartitionShardSearchRequestV1{Version: r.u32()}
	if r.err == nil && value.Version != 0 && value.Version != VectorPartitionShardSearchVersionV1 {
		r.err = errors.New("unsupported M5 request version")
		return value
	}
	fields := [10]string{}
	for i := range fields {
		fields[i] = r.string()
	}
	value.RequestID, value.CancellationID, value.Database, value.Catalog = fields[0], fields[1], fields[2], fields[3]
	value.Collection, value.IndexName, value.IndexDefinitionDigest, value.ReadySetDigest = fields[4], fields[5], fields[6], fields[7]
	value.TargetGroupID, value.TargetNodeID = raftcluster.GroupID(fields[8]), raftcluster.NodeID(fields[9])
	value.SourceGeneration, value.SourceChecksum, value.SourceSchemaHash = r.u64(), r.u64(), r.u64()
	value.SourceRowCount, value.PartitionGeneration, value.RouterGeneration = r.u64(), r.u64(), r.u64()
	count := r.count(r.remaining() / 4)
	if r.err == nil {
		value.PartitionIDs = make([]uint32, count)
		for i := range value.PartitionIDs {
			value.PartitionIDs[i] = r.u32()
		}
	}
	count = r.count(r.remaining() / 4)
	if r.err == nil {
		value.Query = make([]float32, count)
		for i := range value.Query {
			value.Query[i] = r.f32()
		}
	}
	value.Metric = readVectorPartitionShardSearchTCPMetricV1(r.u8(), r)
	value.Mode = readVectorPartitionShardSearchTCPModeV1(r.u8(), r)
	value.Consistency = readVectorPartitionShardSearchTCPConsistencyV1(r.u8(), r)
	value.StatsMode = readVectorPartitionShardSearchTCPStatsV1(r.u8(), r)
	topK, efSearch := r.u32(), r.u32()
	if r.err == nil && (uint64(topK) > uint64(math.MaxInt) || uint64(efSearch) > uint64(math.MaxInt)) {
		r.err = errors.New("M5 TCP search count overflows int")
	}
	value.TopK, value.EfSearch = int(topK), int(efSearch)
	value.DeadlineUnixNano = r.i64()
	value.RequestBytesLimit, value.CandidateBytesLimit, value.ResponseBytesLimit = r.u64(), r.u64(), r.u64()
	switch r.u8() {
	case 0:
	case 1:
		capability := readVectorPartitionStrictCapabilityBinaryV1(r)
		value.StrictCapability = &capability
	default:
		if r.err == nil {
			r.err = errors.New("M5 TCP strict capability flag is invalid")
		}
	}
	return value
}

func vectorPartitionShardSearchTCPMetricV1(v VectorPartitionShardSearchMetricV1) byte {
	if v == VectorPartitionShardSearchMetricCosineV1 {
		return 1
	}
	return 0
}

func vectorPartitionShardSearchTCPModeV1(v VectorPartitionShardSearchModeV1) byte {
	if v == VectorPartitionShardSearchModeNoDocumentV1 {
		return 1
	}
	return 0
}

func vectorPartitionShardSearchTCPConsistencyV1(v VectorPartitionShardSearchConsistencyV1) byte {
	if v == VectorPartitionShardSearchConsistencySnapshotV1 {
		return 1
	}
	return 0
}

func vectorPartitionShardSearchTCPStatsV1(v VectorPartitionShardSearchStatsModeV1) byte {
	switch v {
	case VectorPartitionShardSearchStatsNoneV1:
		return 1
	case VectorPartitionShardSearchStatsBasicV1:
		return 2
	default:
		return 0
	}
}

func readVectorPartitionShardSearchTCPMetricV1(v byte, r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchMetricV1 {
	if v == 0 {
		return ""
	}
	if v == 1 {
		return VectorPartitionShardSearchMetricCosineV1
	}
	if r.err == nil {
		r.err = errors.New("invalid M5 metric")
	}
	return ""
}

func readVectorPartitionShardSearchTCPModeV1(v byte, r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchModeV1 {
	if v == 0 {
		return ""
	}
	if v == 1 {
		return VectorPartitionShardSearchModeNoDocumentV1
	}
	if r.err == nil {
		r.err = errors.New("invalid M5 mode")
	}
	return ""
}

func readVectorPartitionShardSearchTCPConsistencyV1(v byte, r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchConsistencyV1 {
	if v == 0 {
		return ""
	}
	if v == 1 {
		return VectorPartitionShardSearchConsistencySnapshotV1
	}
	if r.err == nil {
		r.err = errors.New("invalid M5 consistency")
	}
	return ""
}

func readVectorPartitionShardSearchTCPStatsV1(v byte, r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchStatsModeV1 {
	switch v {
	case 0:
		return ""
	case 1:
		return VectorPartitionShardSearchStatsNoneV1
	case 2:
		return VectorPartitionShardSearchStatsBasicV1
	default:
		if r.err == nil {
			r.err = errors.New("invalid M5 stats mode")
		}
		return ""
	}
}

func appendVectorPartitionStrictCapabilityBinaryV1(w *vectorPartitionShardSearchTCPBodyWriterV1, c vectorPartitionStrictSearchCapabilityV1) {
	w.u32(c.Version)
	w.string(c.ServingIdentityDigest)
	w.u64(c.CatalogEpoch)
	w.string(c.CatalogDigest)
	w.string(string(c.ProofNodeID))
	w.string(string(c.ProofGroupID))
	w.u64(c.ProofLeaderTerm)
	w.u64(c.CatalogAppliedIndex)
	w.u64(c.CatalogCommitIndex)
	w.u64(c.CatalogRaftAppliedIndex)
	w.i64(c.ValidThroughUnixNano)
	w.string(string(c.TargetGroupID))
	w.u64(c.GroupAppliedIndex)
	w.string(c.MAC)
}

func readVectorPartitionStrictCapabilityBinaryV1(r *vectorPartitionShardSearchTCPBodyReaderV1) vectorPartitionStrictSearchCapabilityV1 {
	return vectorPartitionStrictSearchCapabilityV1{
		Version: r.u32(), ServingIdentityDigest: r.string(), CatalogEpoch: r.u64(), CatalogDigest: r.string(),
		ProofNodeID: raftcluster.NodeID(r.string()), ProofGroupID: raftcluster.GroupID(r.string()), ProofLeaderTerm: r.u64(),
		CatalogAppliedIndex: r.u64(), CatalogCommitIndex: r.u64(), CatalogRaftAppliedIndex: r.u64(),
		ValidThroughUnixNano: r.i64(), TargetGroupID: raftcluster.GroupID(r.string()), GroupAppliedIndex: r.u64(), MAC: r.string(),
	}
}

func appendVectorPartitionShardSearchTCPResponseV1(w *vectorPartitionShardSearchTCPBodyWriterV1, v VectorPartitionShardSearchResponseV1) {
	w.u32(v.Version)
	w.string(v.RequestID)
	appendVectorPartitionShardSearchTCPProofV1(w, v.Proof)
	if uint64(len(v.Partials)) > math.MaxUint32 {
		w.err = errors.New("M5 TCP partial count exceeds uint32")
		return
	}
	w.u32(uint32(len(v.Partials)))
	for _, partial := range v.Partials {
		w.u32(partial.PartitionID)
		if uint64(len(partial.Neighbors)) > math.MaxUint32 {
			w.err = errors.New("M5 TCP neighbor count exceeds uint32")
			return
		}
		w.u32(uint32(len(partial.Neighbors)))
		for _, neighbor := range partial.Neighbors {
			w.string(neighbor.ID)
			w.f32(neighbor.Score)
		}
		w.u64(partial.Candidates)
		w.u64(partial.Edges)
		w.string(partial.SearchRoute)
		w.u64(partial.PackBytes)
		w.u64(partial.MappedBytes)
		w.u64(partial.HeapBytes)
		w.u64(partial.OpenNanos)
	}
	for _, value := range []uint64{v.Partitions, v.ReadProofs, v.GenerationPins, v.PartitionOpens, v.Candidates, v.Edges, v.ResponseBytes, v.Timing.RouteOwnerNanos, v.Timing.ReadIndexApplyNanos, v.Timing.GenerationOpenNanos, v.Timing.SearchNanos, v.Timing.ResponseCopyNanos, v.Timing.TotalNanos} {
		w.u64(value)
	}
}
func readVectorPartitionShardSearchTCPResponseV1(r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchResponseV1 {
	return readVectorPartitionShardSearchTCPResponseWithBoundsV1(r, math.MaxInt, math.MaxInt)
}

func readVectorPartitionShardSearchTCPResponseWithBoundsV1(r *vectorPartitionShardSearchTCPBodyReaderV1, maxPartials, maxNeighbors int) VectorPartitionShardSearchResponseV1 {
	v := VectorPartitionShardSearchResponseV1{Version: r.u32()}
	if r.err == nil && v.Version != 0 && v.Version != VectorPartitionShardSearchVersionV1 {
		r.err = errors.New("unsupported M5 response version")
		return v
	}
	v.RequestID = r.string()
	v.Proof = readVectorPartitionShardSearchTCPProofV1(r)
	count := r.count(min(r.remaining()/vectorPartitionShardSearchTCPResponsePartialMinBytesV1, maxPartials))
	if r.err == nil {
		v.Partials = make([]VectorPartitionShardSearchPartialV1, count)
		for i := range v.Partials {
			partial := &v.Partials[i]
			partial.PartitionID = r.u32()
			neighbors := r.count(min(r.remaining()/vectorPartitionShardSearchTCPResponseNeighborMinBytesV1, maxNeighbors))
			if r.err == nil {
				for range neighbors {
					neighbor := VectorPartitionShardSearchNeighborV1{ID: r.string(), Score: r.f32()}
					if r.err != nil {
						return v
					}
					partial.Neighbors = append(partial.Neighbors, neighbor)
				}
			}
			partial.Candidates, partial.Edges = r.u64(), r.u64()
			partial.SearchRoute = r.string()
			partial.PackBytes, partial.MappedBytes, partial.HeapBytes, partial.OpenNanos = r.u64(), r.u64(), r.u64(), r.u64()
		}
	}
	v.Partitions, v.ReadProofs, v.GenerationPins, v.PartitionOpens, v.Candidates, v.Edges, v.ResponseBytes = r.u64(), r.u64(), r.u64(), r.u64(), r.u64(), r.u64(), r.u64()
	v.Timing = VectorPartitionShardSearchTimingV1{RouteOwnerNanos: r.u64(), ReadIndexApplyNanos: r.u64(), GenerationOpenNanos: r.u64(), SearchNanos: r.u64(), ResponseCopyNanos: r.u64(), TotalNanos: r.u64()}
	return v
}
func appendVectorPartitionShardSearchTCPProofV1(w *vectorPartitionShardSearchTCPBodyWriterV1, p VectorPartitionShardSearchProofV1) {
	for _, value := range []string{p.Kind, string(p.ServingNode), string(p.LeaderNode), string(p.GroupID), p.ReadySetDigest, p.ServingIdentityDigest} {
		w.string(value)
	}
	for _, value := range []uint64{p.ReadTerm, p.ReadIndex, p.AppliedTerm, p.AppliedIndex, p.CatalogAppliedIndex, p.GroupAppliedIndex, p.SourceGeneration, p.SourceChecksum, p.SourceSchemaHash, p.SourceRowCount, p.PartitionGeneration, p.RouterGeneration} {
		w.u64(value)
	}
}

func readVectorPartitionShardSearchTCPProofV1(r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchProofV1 {
	fields := [6]string{}
	for i := range fields {
		fields[i] = r.string()
	}
	return VectorPartitionShardSearchProofV1{
		Kind: fields[0], ServingNode: raftcluster.NodeID(fields[1]), LeaderNode: raftcluster.NodeID(fields[2]), GroupID: raftcluster.GroupID(fields[3]),
		ReadySetDigest: fields[4], ServingIdentityDigest: fields[5],
		ReadTerm: r.u64(), ReadIndex: r.u64(), AppliedTerm: r.u64(), AppliedIndex: r.u64(), CatalogAppliedIndex: r.u64(), GroupAppliedIndex: r.u64(),
		SourceGeneration: r.u64(), SourceChecksum: r.u64(), SourceSchemaHash: r.u64(), SourceRowCount: r.u64(), PartitionGeneration: r.u64(), RouterGeneration: r.u64(),
	}
}

func appendVectorPartitionShardSearchTCPErrorV1(w *vectorPartitionShardSearchTCPBodyWriterV1, e vectorPartitionShardSearchTCPErrorV1) {
	code := vectorPartitionShardSearchTCPErrorCodeV1(e.Code)
	if code == 0 {
		w.err = errors.New("M5 TCP error code is invalid")
		return
	}
	w.u8(code)
	w.string(string(e.GroupID))
	w.string(string(e.LeaderHint))
	w.string(e.Message)
}
func readVectorPartitionShardSearchTCPErrorV1(r *vectorPartitionShardSearchTCPBodyReaderV1) vectorPartitionShardSearchTCPErrorV1 {
	return vectorPartitionShardSearchTCPErrorV1{
		Code: readVectorPartitionShardSearchTCPErrorCodeV1(r.u8(), r), GroupID: raftcluster.GroupID(r.string()),
		LeaderHint: raftcluster.NodeID(r.string()), Message: r.string(),
	}
}

func vectorPartitionShardSearchTCPErrorCodeV1(v VectorPartitionShardSearchErrorCodeV1) byte {
	for i, code := range vectorPartitionShardSearchTCPErrorCodesV1 {
		if code == v {
			return byte(i + 1)
		}
	}
	return 0
}

func readVectorPartitionShardSearchTCPErrorCodeV1(v byte, r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardSearchErrorCodeV1 {
	if v > 0 && int(v) <= len(vectorPartitionShardSearchTCPErrorCodesV1) {
		return vectorPartitionShardSearchTCPErrorCodesV1[v-1]
	}
	if r.err == nil {
		r.err = errors.New("invalid M5 error code")
	}
	return ""
}

var vectorPartitionShardSearchTCPErrorCodesV1 = []VectorPartitionShardSearchErrorCodeV1{
	VectorPartitionShardSearchErrorInvalidRequestV1, VectorPartitionShardSearchErrorUnsupportedConsistencyV1,
	VectorPartitionShardSearchErrorMissingOwnerV1, VectorPartitionShardSearchErrorUnknownOwnerV1,
	VectorPartitionShardSearchErrorRemoteOwnerV1, VectorPartitionShardSearchErrorRouteMismatchV1,
	VectorPartitionShardSearchErrorNotLeaderV1, VectorPartitionShardSearchErrorGroupUnavailableV1,
	VectorPartitionShardSearchErrorGenerationMismatchV1, VectorPartitionShardSearchErrorAssetsUnavailableV1,
	VectorPartitionShardSearchErrorResponseTooLargeV1, VectorPartitionShardSearchErrorCanceledV1,
	VectorPartitionShardSearchErrorDeadlineV1,
}

func appendVectorPartitionShardSearchTCPEndpointIdentityV1(w *vectorPartitionShardSearchTCPBodyWriterV1, v VectorPartitionShardEndpointIdentityV1) {
	identityBytes := uint64(len(v.GroupID)) + uint64(len(v.InstanceIdentity)) + uint64(len(v.ProcessRuntimeStats.EffectiveCPUSet))
	if identityBytes > vectorPartitionShardSearchTCPMinFrameBytesV1-vectorPartitionShardSearchTCPProbeResponseFixedBytesV1 {
		w.err = errors.New("M5 TCP probe response exceeds 4096 bytes")
		return
	}
	w.u32(v.Version)
	w.string(v.GroupID)
	w.string(v.InstanceIdentity)
	stats := v.CatalogMetaReadStats
	for _, stage := range []raftcluster.CatalogMetaLinearizableReadStageStatsV1{
		stats.Total,
		stats.OperationsHealth,
		stats.StrictSearch,
		stats.ServingRefresh,
		stats.CoordinatorLifecycle,
		stats.ShardLifecycle,
		stats.Unknown,
	} {
		appendVectorPartitionShardSearchTCPReadStageV1(w, stage)
	}
	for _, value := range []uint64{
		stats.LastTerm,
		stats.LastCatalogApplied,
		stats.LastRaftApplied,
		stats.LastRaftLog,
	} {
		w.u64(value)
	}
	p := v.ProcessRuntimeStats
	for _, value := range []uint64{
		p.SampleUnixNano,
		p.CPUTimeNanos,
		p.RunQueueDelayNanos,
		p.Timeslices,
		p.VoluntaryContextSwitches,
		p.NonvoluntaryContextSwitches,
		p.RSSBytes,
		p.PeakRSSBytes,
		p.HeapAllocBytes,
		p.HeapObjects,
		p.TotalAllocBytes,
		p.Mallocs,
		p.Frees,
		p.NumGC,
		p.PauseTotalNanos,
		p.Goroutines,
	} {
		w.u64(value)
	}
	w.i64(int64(p.LogicalCPUs))
	w.i64(int64(p.GOMAXPROCS))
	w.i64(p.GoMemoryLimitBytes)
	w.string(p.EffectiveCPUSet)
}

func readVectorPartitionShardSearchTCPEndpointIdentityV1(r *vectorPartitionShardSearchTCPBodyReaderV1) VectorPartitionShardEndpointIdentityV1 {
	v := VectorPartitionShardEndpointIdentityV1{
		Version:          r.u32(),
		GroupID:          r.string(),
		InstanceIdentity: r.string(),
	}
	stages := [7]raftcluster.CatalogMetaLinearizableReadStageStatsV1{}
	for i := range stages {
		stages[i] = readVectorPartitionShardSearchTCPReadStageV1(r)
	}
	v.CatalogMetaReadStats = raftcluster.CatalogMetaLinearizableReadStatsV1{
		Total:                stages[0],
		OperationsHealth:     stages[1],
		StrictSearch:         stages[2],
		ServingRefresh:       stages[3],
		CoordinatorLifecycle: stages[4],
		ShardLifecycle:       stages[5],
		Unknown:              stages[6],
		LastTerm:             r.u64(),
		LastCatalogApplied:   r.u64(),
		LastRaftApplied:      r.u64(),
		LastRaftLog:          r.u64(),
	}
	p := &v.ProcessRuntimeStats
	p.SampleUnixNano = r.u64()
	p.CPUTimeNanos = r.u64()
	p.RunQueueDelayNanos = r.u64()
	p.Timeslices = r.u64()
	p.VoluntaryContextSwitches = r.u64()
	p.NonvoluntaryContextSwitches = r.u64()
	p.RSSBytes = r.u64()
	p.PeakRSSBytes = r.u64()
	p.HeapAllocBytes = r.u64()
	p.HeapObjects = r.u64()
	p.TotalAllocBytes = r.u64()
	p.Mallocs = r.u64()
	p.Frees = r.u64()
	p.NumGC = r.u64()
	p.PauseTotalNanos = r.u64()
	p.Goroutines = r.u64()
	logical, gomax, memory := r.i64(), r.i64(), r.i64()
	if r.err == nil && (int64(int(logical)) != logical || int64(int(gomax)) != gomax) {
		r.err = errors.New("M5 runtime integer overflows")
	}
	p.LogicalCPUs = int(logical)
	p.GOMAXPROCS = int(gomax)
	p.GoMemoryLimitBytes = memory
	p.EffectiveCPUSet = r.string()
	return v
}

func appendVectorPartitionShardSearchTCPReadStageV1(w *vectorPartitionShardSearchTCPBodyWriterV1, s raftcluster.CatalogMetaLinearizableReadStageStatsV1) {
	for _, value := range []uint64{
		s.Reads,
		s.Successes,
		s.Failures,
		s.VerifyLeaderCalls,
		s.LogBarriers,
		s.NoLogProofs,
		s.TotalNanos,
		s.AdmissionNanos,
		s.VerifyLeaderNanos,
		s.BarrierNanos,
		s.CurrentTermNanos,
		s.RaftApplyNanos,
		s.AppliedReadNanos,
	} {
		w.u64(value)
	}
}

func readVectorPartitionShardSearchTCPReadStageV1(r *vectorPartitionShardSearchTCPBodyReaderV1) raftcluster.CatalogMetaLinearizableReadStageStatsV1 {
	return raftcluster.CatalogMetaLinearizableReadStageStatsV1{
		Reads:             r.u64(),
		Successes:         r.u64(),
		Failures:          r.u64(),
		VerifyLeaderCalls: r.u64(),
		LogBarriers:       r.u64(),
		NoLogProofs:       r.u64(),
		TotalNanos:        r.u64(),
		AdmissionNanos:    r.u64(),
		VerifyLeaderNanos: r.u64(),
		BarrierNanos:      r.u64(),
		CurrentTermNanos:  r.u64(),
		RaftApplyNanos:    r.u64(),
		AppliedReadNanos:  r.u64(),
	}
}

func vectorPartitionShardSearchTCPFrameBoundV1(logicalBytes uint64) (uint32, error) {
	if logicalBytes == 0 || logicalBytes > uint64(^uint32(0)) {
		return 0, errors.New("nativewire: M5 TCP logical byte limit exceeds the frame bound")
	}
	frameBytes := max(logicalBytes, vectorPartitionShardSearchTCPMinFrameBytesV1)
	return uint32(frameBytes), nil
}

func vectorPartitionShardSearchTCPResponseFrameBoundV1(limits VectorPartitionShardSearchLimitsV1) (uint32, error) {
	identityBytes, ok := mulUint64V1(vectorPartitionShardSearchTCPResponseIdentityFieldsV1, uint64(limits.MaxIdentityBytes))
	if !ok {
		return 0, errors.New("nativewire: M5 TCP response identity headroom overflows")
	}
	frameBytes, ok := addUint64V1(limits.MaxResponseBytes, identityBytes)
	if !ok {
		return 0, errors.New("nativewire: M5 TCP response frame bound overflows")
	}
	return vectorPartitionShardSearchTCPFrameBoundV1(frameBytes)
}

func vectorPartitionShardSearchTCPDeadlineV1(ctx context.Context, deadlineUnixNano int64, conn net.Conn) error {
	deadline := time.Time{}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if value, ok := ctx.Deadline(); ok {
			deadline = value
		}
	}
	if deadlineUnixNano != 0 {
		requestDeadline := time.Unix(0, deadlineUnixNano)
		if deadline.IsZero() || requestDeadline.Before(deadline) {
			deadline = requestDeadline
		}
	}
	if !deadline.IsZero() {
		return conn.SetDeadline(deadline)
	}
	return nil
}

func vectorPartitionShardSearchTCPInterruptOnCancelV1(ctx context.Context, conn net.Conn) func() {
	if ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			// A deadline wakes both Read and Write without racing the deferred Close.
			_ = conn.SetDeadline(time.Now())
		case <-stop:
		}
	}()
	return func() {
		stopOnce.Do(func() {
			close(stop)
			<-done
		})
	}
}

func vectorPartitionShardSearchTCPRequestContextV1(ctx context.Context, deadlineUnixNano int64) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if deadlineUnixNano == 0 {
		return context.WithCancel(ctx)
	}
	deadline := time.Unix(0, deadlineUnixNano)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(ctx, deadline)
}

// vectorPartitionShardSearchTCPMonitorPeerDisconnectV1 is safe because the
// framing contract does not permit pipelining. After the current request has
// been decoded no further peer bytes are valid until its response, so a bounded
// read
// loop can turn an EOF/reset into cancellation of remote M5 work. Stopping the
// monitor interrupts its outstanding read before waiting, so an ordinary
// successful response is not delayed by the polling deadline. Read deadlines
// are cleared when the normal request completes and do not affect the response
// write deadline.
func vectorPartitionShardSearchTCPMonitorPeerDisconnectV1(conn net.Conn, ctx context.Context, cancel context.CancelFunc) func() bool {
	if conn == nil || cancel == nil {
		return func() bool { return false }
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	var deadlineMu sync.Mutex
	var stopOnce sync.Once
	peerInput := false
	go func() {
		defer close(done)
		var discard [1]byte
		for {
			deadlineMu.Lock()
			select {
			case <-stop:
				deadlineMu.Unlock()
				return
			default:
			}
			_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			deadlineMu.Unlock()
			_, err := conn.Read(discard[:])
			if err == nil {
				// Extra input is invalid under the one-request contract. Treat it
				// as a disconnected/abandoned caller rather than extending work.
				peerInput = true
				cancel()
				return
			}
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
					return
				}
				cancel()
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				select {
				case <-stop:
					return
				default:
					continue
				}
			}
			cancel()
			return
		}
	}()
	return func() bool {
		stopOnce.Do(func() {
			close(stop)
			deadlineMu.Lock()
			// Wake a currently blocked Read immediately; otherwise waiting for done
			// can impose the full peer-poll timeout on a successful response.
			_ = conn.SetReadDeadline(time.Now())
			deadlineMu.Unlock()
			<-done
			_ = conn.SetReadDeadline(time.Time{})
		})
		return peerInput
	}
}

func vectorPartitionShardSearchTCPTransportErrorV1(ctx context.Context, groupID raftcluster.GroupID, err error) error {
	if ctx != nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorCanceledV1, GroupID: groupID, Err: err}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed) {
		return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorDeadlineV1, GroupID: groupID, Err: err}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorDeadlineV1, GroupID: groupID, Err: err}
	}
	return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, GroupID: groupID, Err: err}
}

func vectorPartitionShardSearchTCPRetryableTransportErrorV1(ctx context.Context, groupID raftcluster.GroupID, err error) error {
	return &vectorPartitionShardSearchTCPTransportFailureV1{err: vectorPartitionShardSearchTCPTransportErrorV1(ctx, groupID, err)}
}
