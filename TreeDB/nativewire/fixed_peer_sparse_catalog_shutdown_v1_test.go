package nativewire

import (
	"io"
	"net"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

type sparseCatalogRecordingStreamV1 struct {
	net.Listener
	dialed chan net.Conn
}

func (s *sparseCatalogRecordingStreamV1) Dial(address hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err == nil {
		s.dialed <- conn
	}
	return conn, err
}

func TestSparseCatalogRuntimeCloseInterruptsIdleRaftConnectionV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Nodes = config.Nodes[:1]
	config.Catalog.Peers = config.Catalog.Peers[:1]
	config.Groups = config.Groups[:1]
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stream := &sparseCatalogRecordingStreamV1{Listener: listener, dialed: make(chan net.Conn, 1)}
	client := hraft.NewNetworkTransport(stream, 1, time.Second, io.Discard)
	defer client.Close()
	defer client.CloseStreams()
	request := hraft.AppendEntriesRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax}}
	var response hraft.AppendEntriesResponse
	// A completed RPC proves the server accepted and handled the connection.
	// Term zero cannot advance the running group's term or apply any entries.
	if err := client.AppendEntries(hraft.ServerID(config.NodeID), hraft.ServerAddress(config.RaftListen[config.Catalog.ID]), &request, &response); err != nil {
		t.Fatal(err)
	}
	conn := <-stream.dialed
	defer conn.Close()
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	n, err := conn.Read(one[:])
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("closed runtime retained an accepted idle Raft connection: %v", err)
	}
	if n != 0 || err == nil {
		t.Fatalf("expected connection closure after runtime close: bytes=%d error=%v", n, err)
	}
}
