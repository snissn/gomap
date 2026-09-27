package nativewire

import (
	"context"
	"io"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

func TestPeerRaftPipelineBackpressureAndCloseV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	network, err := hraft.NewTCPTransport("127.0.0.1:0", nil, 4, time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	owner := newPeerRaftTransportV1(network, transport.admission, "raft:"+string(config.Catalog.ID), true)
	defer owner.Close()
	server, err := hraft.NewTCPTransport("127.0.0.1:0", nil, 4, time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer server.CloseStreams()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		for {
			select {
			case rpc := <-server.Consumer():
				rpc.Respond(&hraft.AppendEntriesResponse{Success: true}, nil)
			case <-ctx.Done():
				return
			}
		}
	}()
	pipeline, err := owner.AppendEntriesPipeline("receiver", server.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()
	sent := make(chan error, 1)
	go func() {
		for i := 0; i < 64; i++ {
			_, err := pipeline.AppendEntries(&hraft.AppendEntriesRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax}}, &hraft.AppendEntriesResponse{})
			if err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	// Let completions fill both wrapper/upstream queues before draining them.
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 64; i++ {
		select {
		case future := <-pipeline.Consumer():
			if err := future.Error(); err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("pipeline stalled under backpressure")
		}
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := 0; i < 64; i++ {
			if _, err := pipeline.AppendEntries(&hraft.AppendEntriesRequest{}, &hraft.AppendEntriesResponse{}); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	time.Sleep(20 * time.Millisecond)
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatal("shutdown did not release blocked sender")
	}
	stats := transport.ResourceStatsV1().Current
	if stats[peerRequestsV1] != 0 || stats[peerBytesV1] != 0 {
		t.Fatalf("pipeline leaked leases: %v", stats)
	}
}
