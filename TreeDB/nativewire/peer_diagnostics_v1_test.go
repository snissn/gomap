package nativewire

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestPeerSecurityDiagnosticsAndBoundedNetworkAttributionV1(t *testing.T) {
	fixture, config := peerTransportFixtureV1(t)
	fixture.Close()
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	report, err := node.client.DiagnosticsV1(ctx, config.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status.NodeID != config.NodeID || report.Process.SampleUnixNano == 0 || report.Process.Goroutines == 0 {
		t.Fatalf("missing process identity: %+v", report)
	}
	if runtime.GOOS == "linux" && (report.Process.RSSBytes == 0 || report.FileDescriptors == 0 || len(report.HostInterfaces) == 0 || len(report.Disk) < 2) {
		t.Fatalf("missing live host observations: %+v", report)
	}
	network := node.PeerTransportV1().NetworkStatsV1()
	if len(network) != 2 || network[0].RemoteIP != "127.0.0.1" || network[0].ReadBytes == 0 || network[0].WrittenBytes == 0 || network[1].RemoteIP != "unknown" {
		t.Fatalf("TLS/control bytes not attributed: %+v", network)
	}
	// An unlisted physical address cannot add attacker-controlled cardinality.
	a := node.PeerTransportV1().admission
	before := len(a.network)
	for i := 0; i < 4096; i++ {
		if got := a.networkForV1(&net.TCPAddr{IP: net.IPv4(10, byte(i>>8), byte(i), 1), Port: i + 1}); got != &a.unknown {
			t.Fatal("unknown peer gained a bucket")
		}
	}
	if len(a.network) != before {
		t.Fatal("network inventory grew after startup")
	}
	node.BeginDrainV1()
	if _, err := node.client.DiagnosticsV1(ctx, config.NodeID); err != nil {
		t.Fatalf("drain blocked diagnostics: %v", err)
	}
}
