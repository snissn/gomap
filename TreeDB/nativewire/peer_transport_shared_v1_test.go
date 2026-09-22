package nativewire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestPeerSecuritySharedControlClientOwnsOnlyItsPoolsV1(t *testing.T) {
	fixture, config := peerTransportFixtureV1(t)
	fixture.Close()
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	shared := node.PeerTransportV1()
	client, err := NewFixedPeerTCPClientWithTransportV1(config, shared)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Status(ctx, config.NodeID); err != nil {
		t.Fatal(err)
	}
	client.Close()
	if shared.ResourceStatsV1().Closed {
		t.Fatal("client closed the node's shared transport")
	}
	if _, err := node.client.Status(ctx, config.NodeID); err != nil {
		t.Fatalf("closing one client broke node control traffic: %v", err)
	}
	changed := config
	changed.ClusterID += "-other"
	if _, err := NewFixedPeerTCPClientWithTransportV1(changed, shared); !errors.Is(err, raftcluster.ErrInvalidConfig) {
		t.Fatalf("shared identity mismatch accepted: %v", err)
	}
}
