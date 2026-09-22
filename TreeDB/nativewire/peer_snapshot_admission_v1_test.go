package nativewire

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestPeerSecurityStalledSnapshotPreservesOtherGroupV1(t *testing.T) {
	fixture, config := peerTransportFixtureV1(t)
	fixture.Close()
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil { t.Fatal(err) }
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	group := config.Groups[0]
	fixedPeerWaitV1(t, ctx, func() bool { status, err := node.Status(ctx); return err == nil && status.CatalogRaft.State == "Leader" && len(status.Groups) == 1 && status.Groups[0].State == "Leader" })
	probe := func(timeout time.Duration) *hraft.NetworkTransport {
		transport, err := newFixedPeerTCPTransportWithSecurityV1("127.0.0.1:0", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, timeout, node.client.security, group.Peers)
		if err != nil { t.Fatal(err) }
		t.Cleanup(func() { transport.Close(); transport.CloseStreams() })
		return transport
	}
	sender := probe(3*time.Second)
	state, err := node.data[group.ID].provider.RuntimeStatusV1(ctx)
	if err != nil { t.Fatal(err) }
	servers := hraft.Configuration{Servers: []hraft.Server{{ID: hraft.ServerID(config.NodeID), Address: hraft.ServerAddress(config.RaftListen[group.ID]), Suffrage: hraft.Voter}}}
	request := hraft.InstallSnapshotRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax, ID: []byte(config.NodeID), Addr: []byte(config.RaftListen[group.ID])}, SnapshotVersion: 1, Term: state.Term+100, LastLogIndex: state.LastIndex+1, LastLogTerm: state.Term, Configuration: hraft.EncodeConfiguration(servers), ConfigurationIndex: 1, Size: 1<<20}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	done := make(chan error, 1)
	go func() { var response hraft.InstallSnapshotResponse; done <- sender.InstallSnapshot(hraft.ServerID(config.NodeID), hraft.ServerAddress(config.RaftListen[group.ID]), &request, &response, reader) }()
	// The term transition proves this real Raft group entered snapshot install;
	// its archive stream is deliberately stalled before any payload is supplied.
	fixedPeerWaitV1(t, ctx, func() bool { state, err := node.data[group.ID].provider.RuntimeStatusV1(ctx); return err == nil && state.Term == request.Term })
	second := probe(500*time.Millisecond)
	var response hraft.InstallSnapshotResponse
	stale := hraft.InstallSnapshotRequest{RPCHeader: request.RPCHeader, SnapshotVersion: 1, Term: 0, Size: 0}
	if err := second.InstallSnapshot(hraft.ServerID(config.NodeID), hraft.ServerAddress(config.RaftListen[group.ID]), &stale, &response, bytes.NewReader(nil)); err == nil || !strings.Contains(err.Error(), "admission unavailable") {
		t.Fatalf("second snapshot was not promptly refused by group admission: %v", err)
	}
	if stats := node.PeerTransportV1().ResourceStatsV1(); stats.Current[peerSnapshotsV1] != 1 || stats.Rejected[peerSnapshotsV1] == 0 { t.Fatalf("stalled snapshot admission: %+v", stats) }
	catalog := raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: group.ID, Members: []raftcluster.NodeID{config.NodeID}}}}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil { t.Fatal(err) }
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil { t.Fatal(err) }
	if _, err := node.client.PublishCatalog(ctx, config.NodeID, command); err != nil { t.Fatalf("stalled data snapshot starved real catalog commit: %v", err) }
	writer.Close()
	select { case <-done: case <-ctx.Done(): t.Fatal(ctx.Err()) }
	fixedPeerWaitV1(t, ctx, func() bool { return node.PeerTransportV1().ResourceStatsV1().Current[peerSnapshotsV1] == 0 })
}
