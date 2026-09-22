package nativewire

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func peerTransportFixtureV1(t *testing.T) (*PeerTransportV1, FixedPeerTCPConfigV1) {
	t.Helper()
	config := fixedPeerTestConfigsV1(t)[0]
	config.Nodes = config.Nodes[:1]
	config.Catalog.Peers = config.Catalog.Peers[:1]
	config.Groups = config.Groups[:1]
	config.ClusterID = "all-wire-surfaces"
	config.Credentials = peerCredentialsFixtureV1(t, config.ClusterID, string(config.NodeID))
	transport, err := NewPeerTransportV1(config)
	if err != nil { t.Fatal(err) }
	return transport, config
}

func TestPeerSecurityNativeBoundaryV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	defer listener.Close()
	server := NewServer(ServerOptions{PeerTransport: transport})
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	plain, err := DialContext(ctx, "tcp", listener.Addr().String())
	if err == nil {
		plain.Close()
		t.Fatal("credential-free native client reached the application hello")
	}
	client, err := transport.DialNativeContextV1(ctx, listener.Addr().String(), config.NodeID)
	if err != nil { t.Fatal(err) }
	defer client.Close()
	if err := client.Ping(ctx); err != nil { t.Fatal(err) }
	if wrong, err := transport.DialNativeContextV1(ctx, listener.Addr().String(), "another-node"); err == nil {
		wrong.Close()
		t.Fatal("native client accepted a different destination identity")
	}
	server.Close()
	select { case <-done: case <-ctx.Done(): t.Fatal(ctx.Err()) }
}

func TestPeerSecurityShardBoundaryV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	defer listener.Close()
	var calls atomic.Int64
	server := VectorPartitionShardSearchTCPServerV1{
		PeerTransport: transport, PeerGroupID: config.Groups[0].ID,
		Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			calls.Add(1)
			return VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID}, nil
		}),
	}
	go func() {
		for { conn, err := listener.Accept(); if err != nil { return }; go server.ServeConn(ctx, conn) }
	}()
	endpoints := map[raftcluster.GroupID]string{config.Groups[0].ID: listener.Addr().String()}
	plain, err := NewVectorPartitionShardSearchTCPDispatcherV1(endpoints)
	if err != nil { t.Fatal(err) }
	defer plain.Close()
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1})
	request.TargetGroupID = config.Groups[0].ID
	_, err = plain.DispatchVectorPartitionShardSearchV1(ctx, request)
	if err == nil || calls.Load() != 0 { t.Fatalf("unauthenticated shard reached handler: calls=%d err=%v", calls.Load(), err) }
	nodes := map[raftcluster.GroupID]map[raftcluster.NodeID]string{config.Groups[0].ID: {config.NodeID: listener.Addr().String()}}
	secure, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(transport, endpoints, nodes)
	if err != nil { t.Fatal(err) }
	defer secure.Close()
	if _, err := secure.DispatchVectorPartitionShardSearchV1(ctx, request); err != nil { t.Fatal(err) }
	if calls.Load() != 1 { t.Fatalf("authorized shard calls=%d", calls.Load()) }
	nodes[config.Groups[0].ID] = map[raftcluster.NodeID]string{"another-node": listener.Addr().String()}
	if wrong, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(transport, endpoints, nodes); err == nil {
		wrong.Close()
		t.Fatal("shard endpoint accepted a node outside its Raft group")
	}
}
