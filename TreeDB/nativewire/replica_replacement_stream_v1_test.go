package nativewire

import (
	"errors"
	"net"
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestReplacementPeerRemovalRevokesStreamsAndInflightTrackingV1(t *testing.T) {
	stream := &fixedPeerTCPStreamV1{conns: make(map[*fixedPeerTCPConnV1]struct{}), peerNodes: map[hraft.ServerAddress]raftcluster.NodeID{"old:1": "old", "survivor:1": "survivor"}}
	left, right := net.Pipe()
	defer right.Close()
	tracked, err := stream.track(left, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer tracked.Close()
	if err := stream.replaceAuthorizedPeersV1([]raftcluster.Peer{{ID: "old", Address: "old:1"}, {ID: "survivor", Address: "survivor:1"}, {ID: "new", Address: "new:1"}}); err != nil {
		t.Fatal(err)
	}
	if stream.generation != 0 || len(stream.conns) != 1 {
		t.Fatal("addition disrupted existing streams")
	}
	if err := stream.replaceAuthorizedPeersV1([]raftcluster.Peer{{ID: "survivor", Address: "survivor:1"}, {ID: "new", Address: "new:1"}}); err != nil {
		t.Fatal(err)
	}
	if stream.generation != 1 || len(stream.conns) != 0 {
		t.Fatal("removal retained stream")
	}
	late, remote := net.Pipe()
	defer remote.Close()
	if _, err := stream.track(late, 0); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("old handshake survived revocation: %v", err)
	}
	fresh, remoteFresh := net.Pipe()
	defer remoteFresh.Close()
	accepted, err := stream.track(fresh, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}
}
