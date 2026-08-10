package nativewire

// This file provides the M8 production-topology transport boundary for M5
// shard search.  It deliberately carries the existing validated M5 envelope
// rather than introducing a second shard protocol.  The coordinator still
// owns fanout, retry, and no-partial-result semantics.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	vectorPartitionShardSearchTCPMaxFrameBytesV1      uint32 = 64 << 20
	vectorPartitionShardSearchTCPMinFrameBytesV1      uint64 = 4 << 10
	vectorPartitionShardSearchTCPJSONExpansionBoundV1 uint64 = 6
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
	maxResponseFrame, err := vectorPartitionShardSearchTCPFrameBoundV1(limits.MaxResponseBytes)
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
	frame, err = readVectorPartitionShardSearchTCPFrameV1(conn, d.maxResponseFrame)
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
		maxResponseFrame, err = vectorPartitionShardSearchTCPFrameBoundV1(DefaultVectorPartitionShardSearchLimitsV1().MaxResponseBytes)
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
	Request       *VectorPartitionShardSearchRequestV1    `json:"request,omitempty"`
	Response      *VectorPartitionShardSearchResponseV1   `json:"response,omitempty"`
	Probe         *vectorPartitionShardEndpointProbeV1    `json:"probe,omitempty"`
	ProbeResponse *VectorPartitionShardEndpointIdentityV1 `json:"probe_response,omitempty"`
	Error         *vectorPartitionShardSearchTCPErrorV1   `json:"error,omitempty"`
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
	Code       VectorPartitionShardSearchErrorCodeV1 `json:"code"`
	GroupID    raftcluster.GroupID                   `json:"group_id,omitempty"`
	LeaderHint raftcluster.NodeID                    `json:"leader_hint,omitempty"`
	Message    string                                `json:"message"`
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
	raw, err := json.Marshal(frame)
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
	var frame vectorPartitionShardSearchTCPFrameV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frame); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return vectorPartitionShardSearchTCPFrameV1{}, errors.New("M5 TCP frame has trailing JSON values")
	}
	return frame, nil
}

func vectorPartitionShardSearchTCPFrameBoundV1(logicalBytes uint64) (uint32, error) {
	// encoding/json can expand one input byte to a six-byte Unicode escape;
	// logical request/response accounting already reserves fixed envelopes.
	if logicalBytes == 0 || logicalBytes > uint64(^uint32(0))/vectorPartitionShardSearchTCPJSONExpansionBoundV1 {
		return 0, errors.New("nativewire: M5 TCP logical byte limit exceeds the frame bound")
	}
	frameBytes := logicalBytes * vectorPartitionShardSearchTCPJSONExpansionBoundV1
	frameBytes = max(frameBytes, vectorPartitionShardSearchTCPMinFrameBytesV1)
	return uint32(frameBytes), nil
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
