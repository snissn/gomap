package nativewire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestPeerSecurityByteRefusalIsDefiniteBeforeSendV1(t *testing.T) {
	fixture, config := peerTransportFixtureV1(t)
	fixture.Close()
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := node.client
	if _, err := client.Status(ctx, config.NodeID); err != nil {
		t.Fatal(err)
	}
	blocked, err := client.peerTransport.admission.acquire("control-write", peerBytesV1, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.release()
	before := client.peerTransport.ResourceStatsV1().WrittenBytes
	if _, err := client.Submit(ctx, config.NodeID, nil, ClusterRequestMetadata{}); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("pre-send byte refusal: %v", err)
	}
	if after := client.peerTransport.ResourceStatsV1().WrittenBytes; after != before {
		t.Fatalf("refused mutation sent network bytes: %d -> %d", before, after)
	}
	if _, err := client.Status(ctx, config.NodeID); err != nil {
		t.Fatalf("hot write bytes starved catalog/status reserve: %v", err)
	}
	blocked.release()
	if current := client.peerTransport.ResourceStatsV1().Current; current[peerRequestsV1] != 0 || current[peerBytesV1] != 0 {
		t.Fatalf("request leases leaked: %v", current)
	}
}

func TestPeerSecurityRequestExpansionRefusesBeforeAllocationV1(t *testing.T) {
	body := fixedPeerRequestV1{}
	body.Route.Collection = strings.Repeat("\x00", 2<<20)
	if _, err := preflightPeerRequestBytesV1(body); !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
		t.Fatalf("unbounded JSON escaping accepted: %v", err)
	}
}
