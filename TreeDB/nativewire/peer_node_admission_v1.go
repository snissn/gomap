package nativewire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// PeerNodeLimitsV1 bounds all transports sharing one PeerTransportV1. Zero
// fields select conservative defaults. Limits are local capacity, not authority.
type PeerNodeLimitsV1 struct {
	Connections int64
	Requests int64
	InflightBytes int64
	Proposals int64
	Snapshots int64
	GroupConnections int64
	GroupRequests int64
	GroupInflightBytes int64
}

const (
	peerConnectionsV1 = iota
	peerRequestsV1
	peerBytesV1
	peerProposalsV1
	peerSnapshotsV1
	peerResourceKindsV1
)

type peerResourceAmountsV1 [peerResourceKindsV1]int64

// PeerNodeResourceStatsV1 reports bounded current/peak admission and actual
// socket bytes. Bytes include TLS records and handshakes, not just payloads.
type PeerNodeResourceStatsV1 struct {
	Current peerResourceAmountsV1
	Peak peerResourceAmountsV1
	Rejected [peerResourceKindsV1]uint64
	ReadBytes uint64
	WrittenBytes uint64
	Closed bool
}

type peerResourceScopeV1 struct {
	used peerResourceAmountsV1
	reserved peerResourceAmountsV1
	limits peerResourceAmountsV1
}

type peerNodeAdmissionV1 struct {
	mu sync.Mutex
	limits peerResourceAmountsV1
	sharedLimits peerResourceAmountsV1
	shared peerResourceAmountsV1
	scopes map[string]*peerResourceScopeV1
	stats PeerNodeResourceStatsV1
	conns map[*peerNodeConnV1]struct{}
	listeners map[*peerNodeListenerV1]struct{}
	ctx context.Context
	cancel context.CancelFunc
	read atomic.Uint64
	written atomic.Uint64
}

func normalizePeerNodeLimitsV1(value PeerNodeLimitsV1) (PeerNodeLimitsV1, error) {
	fields := []*int64{&value.Connections, &value.Requests, &value.InflightBytes, &value.Proposals, &value.Snapshots, &value.GroupConnections, &value.GroupRequests, &value.GroupInflightBytes}
	defaults := []int64{512, 512, 256<<20, 64, 4, 64, 64, 64<<20}
	maxima := []int64{8192, 8192, 16<<30, 1024, 32, 1024, 1024, 1<<30}
	for i, field := range fields {
		if *field == 0 { *field = defaults[i] }
		if *field < 1 || *field > maxima[i] { return value, fmt.Errorf("%w: invalid node resource limit %d", raftcluster.ErrInvalidConfig, i) }
	}
	if value.GroupConnections < 4 || value.GroupInflightBytes < 16<<20 || value.GroupConnections >= value.Connections || value.GroupRequests >= value.Requests || value.GroupInflightBytes >= value.InflightBytes || value.Proposals < 2 || value.Snapshots < 2 {
		return value, fmt.Errorf("%w: one group must not consume the whole node budget", raftcluster.ErrInvalidConfig)
	}
	return value, nil
}

func newPeerNodeAdmissionV1(config FixedPeerTCPConfigV1) (*peerNodeAdmissionV1, error) {
	var limits PeerNodeLimitsV1
	if config.ResourceLimits != nil { limits = *config.ResourceLimits }
	limits, err := normalizePeerNodeLimitsV1(limits)
	if err != nil { return nil, err }
	all := peerResourceAmountsV1{limits.Connections, limits.Requests, limits.InflightBytes, limits.Proposals, limits.Snapshots}
	group := peerResourceAmountsV1{limits.GroupConnections, limits.GroupRequests, limits.GroupInflightBytes, min(8, limits.Proposals-1), 1}
	a := &peerNodeAdmissionV1{limits: all, sharedLimits: all, scopes: make(map[string]*peerResourceScopeV1), conns: make(map[*peerNodeConnV1]struct{}), listeners: make(map[*peerNodeListenerV1]struct{})}
	add := func(key string, reserved peerResourceAmountsV1) {
		if a.scopes[key] != nil { return }
		a.scopes[key] = &peerResourceScopeV1{reserved: reserved, limits: group}
		for i := range reserved { a.sharedLimits[i] -= reserved[i] }
	}
	add("control", peerResourceAmountsV1{4, 0, 0})
	add("control-read", peerResourceAmountsV1{0, 4, 16<<20})
	add("control-write", peerResourceAmountsV1{0, 1, 64<<10})
	add("control-forward", peerResourceAmountsV1{0, 1, 64<<10})
	add("native", peerResourceAmountsV1{1, 1, 64<<10})
	for _, group := range config.Groups { add("shard:"+string(group.ID), peerResourceAmountsV1{1, 1, 64<<10}) }
	for group := range config.RaftListen { add("raft:"+string(group), peerResourceAmountsV1{4, 1, 64<<10}) }
	for kind, remaining := range a.sharedLimits { if remaining <= 0 { return nil, fmt.Errorf("%w: node limit %d cannot cover declared group reserves", raftcluster.ErrInvalidConfig, kind) } }
	a.ctx, a.cancel = context.WithCancel(context.Background())
	return a, nil
}

type peerResourceLeaseV1 struct {
	owner *peerNodeAdmissionV1
	scope *peerResourceScopeV1
	kind int
	amount int64
}

func (a *peerNodeAdmissionV1) acquire(scope string, kind int, amount int64) (peerResourceLeaseV1, error) {
	var lease peerResourceLeaseV1
	if a == nil { return lease, nil }
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.scopes[scope]
	if a.stats.Closed { return lease, net.ErrClosed }
	if s == nil || kind < 0 || kind >= peerResourceKindsV1 || amount < 0 { return lease, raftcluster.ErrInvalidConfig }
	if amount == 0 { return lease, nil }
	if amount > s.limits[kind] { a.stats.Rejected[kind]++; return lease, raftcluster.ErrAdmissionUnavailable }
	extra := max(0, s.used[kind]+amount-s.reserved[kind])-max(0, s.used[kind]-s.reserved[kind])
	if amount > s.limits[kind]-s.used[kind] || extra > a.sharedLimits[kind]-a.shared[kind] {
		a.stats.Rejected[kind]++
		return lease, fmt.Errorf("%w: node resource %d exhausted for %s", raftcluster.ErrAdmissionUnavailable, kind, scope)
	}
	s.used[kind] += amount
	a.shared[kind] += extra
	a.stats.Current[kind] += amount
	a.stats.Peak[kind] = max(a.stats.Peak[kind], a.stats.Current[kind])
	return peerResourceLeaseV1{owner: a, scope: s, kind: kind, amount: amount}, nil
}

func (l *peerResourceLeaseV1) release() {
	if l.owner == nil || l.amount == 0 { return }
	a := l.owner
	a.mu.Lock()
	s, kind, amount := l.scope, l.kind, l.amount
	usedShared := max(0, s.used[kind]-s.reserved[kind])-max(0, s.used[kind]-amount-s.reserved[kind])
	s.used[kind] -= amount
	a.shared[kind] -= usedShared
	a.stats.Current[kind] -= amount
	l.amount = 0
	a.mu.Unlock()
}

func (a *peerNodeAdmissionV1) snapshot() PeerNodeResourceStatsV1 {
	if a == nil { return PeerNodeResourceStatsV1{} }
	a.mu.Lock()
	stats := a.stats
	a.mu.Unlock()
	stats.ReadBytes, stats.WrittenBytes = a.read.Load(), a.written.Load()
	return stats
}

// Reserve before dialing and retain the lease for active and idle lifetime.
func (a *peerNodeAdmissionV1) dial(ctx context.Context, scope string, dial func(context.Context) (net.Conn, error)) (net.Conn, error) {
	if a == nil { return nil, errPeerAuthenticationV1 }
	lease, err := a.acquire(scope, peerConnectionsV1, 1)
	if err != nil { return nil, err }
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.ctx, cancel)
	defer func() { stop(); cancel() }()
	conn, err := dial(ctx)
	if err != nil { lease.release(); return nil, err }
	return a.track(conn, lease)
}

func (a *peerNodeAdmissionV1) track(raw net.Conn, lease peerResourceLeaseV1) (net.Conn, error) {
	conn := &peerNodeConnV1{Conn: raw, owner: a, lease: lease}
	a.mu.Lock()
	if a.stats.Closed { a.mu.Unlock(); raw.Close(); lease.release(); return nil, net.ErrClosed }
	a.conns[conn] = struct{}{}
	a.mu.Unlock()
	return conn, nil
}

func (a *peerNodeAdmissionV1) accept(raw net.Conn, scope string) (net.Conn, error) {
	if a == nil { raw.Close(); return nil, errPeerAuthenticationV1 }
	lease, err := a.acquire(scope, peerConnectionsV1, 1)
	if err != nil { raw.Close(); return nil, err }
	return a.track(raw, lease)
}

func (a *peerNodeAdmissionV1) close() error {
	if a == nil { return nil }
	a.mu.Lock()
	if a.stats.Closed { a.mu.Unlock(); return nil }
	a.stats.Closed = true
	conns := make([]*peerNodeConnV1, 0, len(a.conns))
	for conn := range a.conns { conns = append(conns, conn) }
	listeners := make([]*peerNodeListenerV1, 0, len(a.listeners))
	for listener := range a.listeners { listeners = append(listeners, listener) }
	a.mu.Unlock()
	a.cancel()
	var err error
	for _, listener := range listeners { if e := listener.Close(); !errors.Is(e, net.ErrClosed) { err = errors.Join(err, e) } }
	for _, conn := range conns { if e := conn.Close(); !errors.Is(e, net.ErrClosed) { err = errors.Join(err, e) } }
	return err
}

type peerNodeConnV1 struct {
	net.Conn
	owner *peerNodeAdmissionV1
	lease peerResourceLeaseV1
	once sync.Once
	err error
}

func (c *peerNodeConnV1) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.mu.Unlock()
		c.lease.release()
	})
	return c.err
}

func (c *peerNodeConnV1) Read(p []byte) (int, error) { n, err := c.Conn.Read(p); c.owner.read.Add(uint64(n)); return n, err }
func (c *peerNodeConnV1) Write(p []byte) (int, error) { n, err := c.Conn.Write(p); c.owner.written.Add(uint64(n)); return n, err }

// Listener ownership is separate from accepted/dialed connection accounting.
// The fixed 256-listener cap bounds file descriptors before any accept loop.
func (a *peerNodeAdmissionV1) listener(raw net.Listener) (net.Listener, error) {
	if a == nil { raw.Close(); return nil, errPeerAuthenticationV1 }
	a.mu.Lock()
	var err error
	if a.stats.Closed { err = net.ErrClosed } else if len(a.listeners) >= 256 { err = raftcluster.ErrAdmissionUnavailable }
	if err != nil { a.mu.Unlock(); raw.Close(); return nil, err }
	listener := &peerNodeListenerV1{Listener: raw, owner: a}
	a.listeners[listener] = struct{}{}
	a.mu.Unlock()
	return listener, nil
}

type peerNodeListenerV1 struct {
	net.Listener
	owner *peerNodeAdmissionV1
	once sync.Once
	err error
}

func (l *peerNodeListenerV1) Close() error {
	l.once.Do(func() {
		l.err = l.Listener.Close()
		l.owner.mu.Lock()
		delete(l.owner.listeners, l)
		l.owner.mu.Unlock()
	})
	return l.err
}
