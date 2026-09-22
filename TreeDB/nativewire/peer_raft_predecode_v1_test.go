package nativewire

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestPeerSecurityRaftAdmissionPrecedesDecodeV1(t *testing.T) {
	fixture, config := peerTransportFixtureV1(t)
	fixture.Close()
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil { t.Fatal(err) }
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	group := config.Groups[0]
	held, err := node.PeerTransportV1().admission.acquire("raft:"+string(group.ID), peerBytesV1, 64<<20)
	if err != nil { t.Fatal(err) }
	defer held.release()
	conn, err := node.client.security.dial(ctx, config.RaftListen[group.ID], config.NodeID)
	if err != nil { t.Fatal(err) }
	defer conn.Close()
	// An authenticated AppendEntries header declares a huge entry array, then
	// stalls. Admission must refuse before HashiCorp's MessagePack decoder can
	// start allocating or wait for the absent array elements.
	if _, err := conn.Write([]byte{0, 0x81, 0xa7, 'E','n','t','r','i','e','s', 0xdd, 0xff,0xff,0xff,0xff}); err != nil { t.Fatal(err) }
	conn.SetReadDeadline(time.Now().Add(500*time.Millisecond))
	var one [1]byte
	_, err = conn.Read(one[:])
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() { t.Fatalf("Raft decode bypassed byte admission and waited for attacker-controlled payload: %v", err) }
	if err == nil || (err != io.EOF && node.PeerTransportV1().ResourceStatsV1().Rejected[peerBytesV1] == 0) { t.Fatalf("missing predecode admission refusal: %v", err) }
}
