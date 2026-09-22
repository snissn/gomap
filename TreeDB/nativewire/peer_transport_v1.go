package nativewire

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/netip"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// PeerTransportV1 shares one validated node identity across the existing
// fixed-peer, native-wire and shard protocols. It does not own a second
// consensus or routing implementation. Reuse the runtime's transport on all
// listeners and dispatchers belonging to that node.
type PeerTransportV1 struct {
	security *peerTransportSecurityV1
	node raftcluster.NodeID
	groups map[raftcluster.GroupID]map[raftcluster.NodeID]bool
}

// NewPeerTransportV1 loads a credentialed fixed-peer configuration without
// opening stores or bootstrapping Raft. Credential-free configuration refuses.
func NewPeerTransportV1(config FixedPeerTCPConfigV1) (*PeerTransportV1, error) {
	config, _, err := validateFixedPeerConfigV1(config)
	if err != nil { return nil, err }
	if config.Credentials == nil { return nil, errPeerAuthenticationV1 }
	nodes := make([]raftcluster.NodeID, len(config.Nodes))
	for i, node := range config.Nodes { nodes[i] = node.ID }
	security, err := newPeerTransportSecurityV1(config.ClusterID, config.NodeID, *config.Credentials, nodes)
	if err != nil { return nil, err }
	return peerTransportFromSecurityV1(config, security), nil
}

func peerTransportFromSecurityV1(config FixedPeerTCPConfigV1, security *peerTransportSecurityV1) *PeerTransportV1 {
	if security == nil { return nil }
	transport := &PeerTransportV1{security: security, node: config.NodeID, groups: make(map[raftcluster.GroupID]map[raftcluster.NodeID]bool, len(config.Groups)+1)}
	add := func(group FixedPeerTCPGroupV1) {
		members := make(map[raftcluster.NodeID]bool, len(group.Peers))
		for _, member := range group.Peers { members[member.ID] = true }
		transport.groups[group.ID] = members
	}
	add(config.Catalog)
	for _, group := range config.Groups { add(group) }
	return transport
}

// PeerTransportV1 returns the identity shared by this runtime's transports.
// It is nil only for the explicit legacy trusted-network mode.
func (r *FixedPeerTCPRuntimeV1) PeerTransportV1() *PeerTransportV1 {
	if r == nil || r.client == nil { return nil }
	return r.client.peerTransport
}

func peerPrivateEndpointV1(address string) bool {
	if len(address) > 64 { return false }
	value, err := netip.ParseAddrPort(address)
	return err == nil && value.String() == address && value.Port() != 0 && !value.Addr().Is4In6() && (value.Addr().IsPrivate() || value.Addr().IsLoopback())
}

func (p *PeerTransportV1) dial(ctx context.Context, address string, node raftcluster.NodeID) (net.Conn, error) {
	if p == nil || p.security == nil || !peerPrivateEndpointV1(address) { return nil, errPeerAuthenticationV1 }
	if ctx == nil { ctx = context.Background() }
	return p.security.dial(ctx, address, node)
}

func (p *PeerTransportV1) accept(ctx context.Context, raw net.Conn, group raftcluster.GroupID) (net.Conn, error) {
	if p == nil || p.security == nil || (group != "" && !p.groups[group][p.node]) { return nil, errPeerAuthenticationV1 }
	if ctx == nil { ctx = context.Background() }
	ctx, cancel := context.WithTimeout(ctx, peerHandshakeTimeoutV1)
	defer cancel()
	conn := p.security.serverConn(raw, nil).(*tls.Conn)
	if err := conn.HandshakeContext(ctx); err != nil { return nil, err }
	return conn, nil
}

// DialNativeContextV1 authenticates the destination before the native-wire
// hello. No plaintext fallback or mutation retry is attempted.
func (p *PeerTransportV1) DialNativeContextV1(ctx context.Context, address string, node raftcluster.NodeID) (*Client, error) {
	conn, err := p.dial(ctx, address, node)
	if err != nil { return nil, err }
	client := NewClient(conn)
	if err := client.Hello(ctx); err != nil { _ = client.Close(); return nil, err }
	return client, nil
}

// NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1 requires every
// fallback and leader-hint endpoint to name a configured member of its group.
// Coordinators authenticate as inventory nodes; the existing M5 proof still
// authorizes the requested ownership epoch and read.
func NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(transport *PeerTransportV1, endpoints map[raftcluster.GroupID]string, nodeEndpoints map[raftcluster.GroupID]map[raftcluster.NodeID]string) (*VectorPartitionShardSearchTCPDispatcherV1, error) {
	if transport == nil || transport.security == nil || len(endpoints) == 0 || len(endpoints) > 128 || len(nodeEndpoints) != len(endpoints) { return nil, errPeerAuthenticationV1 }
	identities := make(map[string]raftcluster.NodeID)
	for group, endpoint := range endpoints {
		members := nodeEndpoints[group]
		if len(group) > 128 || len(members) == 0 || len(members) > fixedPeerMaxPeersV1 { return nil, raftcluster.ErrInvalidConfig }
		found := false
		for node, address := range members {
			if !transport.groups[group][node] || !peerPrivateEndpointV1(address) { return nil, errPeerAuthenticationV1 }
			if old := identities[address]; old != "" && old != node { return nil, raftcluster.ErrInvalidConfig }
			identities[address] = node
			found = found || endpoint == address
		}
		if !found { return nil, fmt.Errorf("%w: shard fallback lacks an authenticated group owner", raftcluster.ErrInvalidConfig) }
	}
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherWithNodeEndpointsV1(endpoints, nodeEndpoints)
	if err != nil { return nil, err }
	dispatcher.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || identities[address] == "" { return nil, errPeerAuthenticationV1 }
		return transport.dial(ctx, address, identities[address])
	}
	return dispatcher, nil
}
