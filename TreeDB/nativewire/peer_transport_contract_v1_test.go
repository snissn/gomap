package nativewire

import (
	"context"
	"errors"
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
	if err != nil {
		t.Fatal(err)
	}
	return transport, config
}

func TestPeerSecurityNativeBoundaryV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if wrong, err := transport.DialNativeContextV1(ctx, listener.Addr().String(), "another-node"); err == nil {
		wrong.Close()
		t.Fatal("native client accepted a different destination identity")
	}
	server.Close()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestPeerSecurityShardBoundaryV1(t *testing.T)        { testPeerSecurityShardBoundaryV1(t, false) }
func TestPeerSecurityShardDependencyDrainV1(t *testing.T) { testPeerSecurityShardBoundaryV1(t, true) }

func testPeerSecurityShardBoundaryV1(t *testing.T, draining bool) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var calls atomic.Int64
	server := VectorPartitionShardSearchTCPServerV1{
		PeerTransport: transport, PeerGroupID: config.Groups[0].ID,
		Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			calls.Add(1)
			return VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID}, nil
		}),
	}
	go func() { _ = server.Serve(ctx, listener) }()
	caller := transport
	if draining {
		transport.admission.beginDrain()
		caller, err = NewPeerTransportV1(config)
		if err != nil {
			t.Fatal(err)
		}
		defer caller.Close()
	}
	endpoints := map[raftcluster.GroupID]string{config.Groups[0].ID: listener.Addr().String()}
	plain, err := NewVectorPartitionShardSearchTCPDispatcherV1(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1})
	request.TargetGroupID = config.Groups[0].ID
	_, err = plain.DispatchVectorPartitionShardSearchV1(ctx, request)
	if err == nil || calls.Load() != 0 {
		t.Fatalf("unauthenticated shard reached handler: calls=%d err=%v", calls.Load(), err)
	}
	// A TLS-authenticated frame cannot name a different group, including
	// while authenticated internal dependencies are admitted during drain.
	wrongGroup, err := caller.dialScope(ctx, listener.Addr().String(), config.NodeID, "shard:"+string(config.Groups[0].ID))
	if err != nil {
		t.Fatal(err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = wrongGroup.SetDeadline(deadline)
	}
	invalid := request
	invalid.TargetGroupID = "another-group"
	err = writeVectorPartitionShardSearchTCPFrameV1(wrongGroup, vectorPartitionShardSearchTCPFrameV1{Request: &invalid}, vectorPartitionShardSearchTCPMaxFrameBytesV1)
	if err == nil {
		_, err = readVectorPartitionShardSearchTCPFrameV1(wrongGroup, vectorPartitionShardSearchTCPMaxFrameBytesV1)
	}
	_ = wrongGroup.Close()
	if err == nil || calls.Load() != 0 {
		t.Fatalf("wrong-group frame reached handler: calls=%d err=%v", calls.Load(), err)
	}
	nodes := map[raftcluster.GroupID]map[raftcluster.NodeID]string{config.Groups[0].ID: {config.NodeID: listener.Addr().String()}}
	secure, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(caller, endpoints, nodes)
	if err != nil {
		t.Fatal(err)
	}
	defer secure.Close()
	if _, err := secure.DispatchVectorPartitionShardSearchV1(ctx, request); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("authorized shard calls=%d", calls.Load())
	}
	nodes[config.Groups[0].ID] = map[raftcluster.NodeID]string{"another-node": listener.Addr().String()}
	if wrong, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(caller, endpoints, nodes); err == nil {
		wrong.Close()
		t.Fatal("shard endpoint accepted a node outside its Raft group")
	}
}

func TestPeerSecurityShardPreflightReturnsTypedErrorsV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	group := config.Groups[0].ID
	dispatcher, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(transport, map[raftcluster.GroupID]string{group: config.ListenAddress}, map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {config.NodeID: config.ListenAddress}})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	for _, oversized := range []bool{false, true} {
		request := vectorPartitionShardSearchRequestTestV1([]uint32{1})
		request.TargetGroupID = group
		if oversized {
			dispatcher.maxRequestFrame = 1
		} else {
			request.TopK = 0
		}
		_, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), request)
		var typed *VectorPartitionShardSearchErrorV1
		if !errors.As(err, &typed) || typed.Code != VectorPartitionShardSearchErrorInvalidRequestV1 || typed.GroupID != group || !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
			t.Fatalf("preflight lost typed request failure: %v", err)
		}
	}
	if stats := transport.ResourceStatsV1(); stats.WrittenBytes != 0 {
		t.Fatalf("preflight sent bytes: %v", stats)
	}
}
