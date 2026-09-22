package nativewire

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestPeerSecurityNodeConnectionsCrossProtocolsV1(t *testing.T) {
	fixture, config := peerTransportFixtureV1(t)
	fixture.Close()
	config.ResourceLimits = &PeerNodeLimitsV1{Connections: 24, GroupConnections: 8}
	runtime, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil { t.Fatal(err) }
	defer runtime.Close()
	transport := runtime.PeerTransportV1()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nativeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	native := NewServer(ServerOptions{PeerTransport: transport})
	defer native.Close()
	go func() { _ = native.Serve(ctx, nativeListener) }()
	var clients []*Client
	defer func() { for _, client := range clients { client.Close() } }()
	for i := 0; i < 12; i++ {
		client, err := transport.DialNativeContextV1(ctx, nativeListener.Addr().String(), config.NodeID)
		if err != nil { break }
		clients = append(clients, client)
	}
	if len(clients) != 4 { t.Fatalf("native shared client+server limit: got %d pairs", len(clients)) }
	shardListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	defer shardListener.Close()
	shard := VectorPartitionShardSearchTCPServerV1{PeerTransport: transport, PeerGroupID: config.Groups[0].ID}
	go func() { _ = shard.Serve(ctx, shardListener) }()
	var sockets []net.Conn
	defer func() { for _, conn := range sockets { conn.Close() } }()
	for i := 0; i < 8; i++ {
		conn, err := transport.dialScope(ctx, shardListener.Addr().String(), config.NodeID, "shard:"+string(config.Groups[0].ID))
		if err != nil { break }
		sockets = append(sockets, conn)
	}
	// Native sockets consume the shared reserve before this group's own cap.
	// Independent per-protocol ledgers would permit another four pairs.
	if len(sockets) == 0 || len(sockets) >= 4 { t.Fatalf("cross-protocol admission allowed %d shard pairs", len(sockets)) }
	if _, err := runtime.client.Status(ctx, config.NodeID); err != nil { t.Fatalf("reserved control capacity starved by native/shard sockets: %v", err) }
	stats := transport.ResourceStatsV1()
	if stats.Current[peerConnectionsV1] > 24 || stats.Peak[peerConnectionsV1] > 24 || stats.Rejected[peerConnectionsV1] == 0 || stats.ReadBytes == 0 || stats.WrittenBytes == 0 { t.Fatalf("node admission evidence: %+v", stats) }
	if err := transport.Close(); err != nil { t.Fatal(err) }
	stats = transport.ResourceStatsV1()
	if stats.Current[peerConnectionsV1] != 0 || !stats.Closed { t.Fatalf("node close leaked charged sockets: %+v", stats) }
	if _, err := transport.DialNativeContextV1(ctx, nativeListener.Addr().String(), config.NodeID); err == nil { t.Fatal("closed node transport reopened a socket") }
}

func TestPeerSecurityInvalidNodeBudgetRefusesBeforeStoresV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	transport.Close()
	for _, limits := range []PeerNodeLimitsV1{
		{Connections: -1}, {Connections: 64, GroupConnections: 64},
		{Connections: 12, GroupConnections: 8}, {Snapshots: 1},
	} {
		config.ResourceLimits = &limits
		if transport, err := NewPeerTransportV1(config); err == nil { transport.Close(); t.Fatalf("accepted invalid/insufficient node budget: %+v", limits) }
	}
}
