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
	node, err := fixedPeerOpenTestRuntimeV1(t, config)
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

func TestPeerSecurityDrainKeepsAdmittedForwardingV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	a := transport.admission
	ingress, err := a.request(context.Background(), "control-write", 64<<10, peerRequestIngressV1)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.release()
	a.beginDrain()
	for _, scope := range []string{"control-forward", "control-read", "native", "shard:" + string(config.Groups[0].ID)} {
		work, err := a.request(ingress.ctx, scope, 64<<10, peerRequestDescendantV1)
		if err != nil {
			t.Fatalf("admitted %s refused: %v", scope, err)
		}
		work.release()
		if work, err := a.request(context.Background(), scope, 64<<10, peerRequestDescendantV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
			work.release()
			t.Fatalf("fresh %s accepted: %v", scope, err)
		}
	}
	// A capability never authorizes fresh ingress, even on its owning transport.
	if work, err := a.request(ingress.ctx, "native", 1, peerRequestIngressV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		work.release()
		t.Fatalf("fresh ingress reused capability: %v", err)
	}
}

func TestSplitProofNodeReservePreservesNestedReadsV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Credentials = &PeerCredentialsV1{}
	config.Vector = &FixedPeerTCPVectorConfigV1{}
	a, err := newPeerNodeAdmissionV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer a.cancel()
	var held []peerResourceLeaseV1
	defer func() {
		for i := range held {
			held[i].release()
		}
	}()
	// Consume actual shared leases while leaving only reserved dependency work.
	for _, kind := range []int{peerRequestsV1, peerBytesV1} {
		amount := int64(1)
		if kind == peerBytesV1 {
			amount = 1 << 16
		}
		for scope := range a.scopes {
			if scope == "control-proof" || scope == "control-read" {
				continue
			}
			for {
				lease, err := a.acquire(scope, kind, amount)
				if err != nil {
					break
				}
				held = append(held, lease)
			}
		}
		if a.shared[kind] != a.sharedLimits[kind] {
			t.Fatalf("fixture did not saturate shared resource %d: %d/%d", kind, a.shared[kind], a.sharedLimits[kind])
		}
	}
	bound, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{Entry: make([]byte, 128<<10)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		work, err := a.work("control-proof", peerRequestsV1, peerControlBytesV1(bound))
		if err != nil {
			t.Fatalf("reserved proof leg %d: %v", i, err)
		}
		defer work.release()
	}
	if work, err := a.work("control-proof", peerRequestsV1, peerControlBytesV1(bound)); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		work.release()
		t.Fatalf("third proof exceeded reserved capacity: %v", err)
	}
	read, err := a.work("control-read", peerRequestsV1, 4<<20)
	if err != nil {
		t.Fatalf("proofs starved reserved leaf read: %v", err)
	}
	read.release()
	for _, operation := range []string{"vector-split-source-proof", "vector-split-receipt", "/v1/vector-split-source-proof", "/v1/vector-split-receipt"} {
		if peerControlScopeV1(operation) != "control-proof" || peerControlRequestModeV1(operation, peerRequestIngressV1) != peerRequestInternalV1 {
			t.Fatalf("proof dependency classification changed: %s", operation)
		}
	}
}
