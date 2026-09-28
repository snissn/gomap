package nativewire

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type replacementCloseNotifyFailureConnV1 struct {
	net.Conn
	failWrites atomic.Bool
	closed     atomic.Bool
	writeErr   error
	closeErr   error
}

func (c *replacementCloseNotifyFailureConnV1) Write(p []byte) (int, error) {
	if c.failWrites.Load() {
		return 0, c.writeErr
	}
	return c.Conn.Write(p)
}

func (c *replacementCloseNotifyFailureConnV1) Close() error {
	c.closed.Store(true)
	return errors.Join(c.Conn.Close(), c.closeErr)
}

type replacementUnknownCloseFailureConnV1 struct {
	net.Conn
	err error
}

func (c *replacementUnknownCloseFailureConnV1) Close() error {
	return errors.Join(c.Conn.Close(), c.err)
}

func TestReplacementRevocationIgnoresClosedTLSAlertFailureV1(t *testing.T) {
	credentials := peerCredentialsFixtureV1(t, "revocation-close", "old")
	certificate, err := tls.LoadX509KeyPair(credentials.CertificateFile, credentials.PrivateKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	unknown := errors.New("underlying TLS socket close failure")
	for _, tc := range []struct {
		name     string
		wrapped  bool
		writeErr error
		closeErr error
	}{{name: "direct", writeErr: syscall.EPIPE}, {name: "admitted", wrapped: true, writeErr: syscall.EPIPE}, {name: "closed-pipe-alert", wrapped: true, writeErr: io.ErrClosedPipe}, {name: "underlying-close-failure", wrapped: true, writeErr: syscall.EPIPE, closeErr: unknown}} {
		t.Run(tc.name, func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			defer right.Close()
			raw := &replacementCloseNotifyFailureConnV1{Conn: left, writeErr: tc.writeErr, closeErr: tc.closeErr}
			client := tls.Client(raw, &tls.Config{InsecureSkipVerify: true, SessionTicketsDisabled: true}) // test-only peer certificate
			server := tls.Server(right, &tls.Config{Certificates: []tls.Certificate{certificate}, SessionTicketsDisabled: true})
			handshake := make(chan error, 1)
			go func() { handshake <- server.Handshake() }()
			if err := client.Handshake(); err != nil {
				t.Fatal(err)
			}
			if err := <-handshake; err != nil {
				t.Fatal(err)
			}
			var conn net.Conn = client
			if tc.wrapped {
				conn = &peerRaftWireConnV1{Conn: client}
			}
			stream := &fixedPeerTCPStreamV1{conns: make(map[*fixedPeerTCPConnV1]struct{}), peerNodes: map[hraft.ServerAddress]raftcluster.NodeID{"old:1": "old", "survivor:1": "survivor"}}
			if _, err := stream.track(conn, 0); err != nil {
				t.Fatal(err)
			}
			raw.failWrites.Store(true)
			err = stream.replaceAuthorizedPeersV1([]raftcluster.Peer{{ID: "survivor", Address: "survivor:1"}})
			if !raw.closed.Load() || stream.generation != 1 || len(stream.conns) != 0 {
				t.Fatal("revocation did not close the TLS socket and advance authorization")
			}
			if tc.closeErr != nil && !errors.Is(err, tc.closeErr) {
				t.Fatalf("underlying TLS close failure was lost: %v", err)
			}
			if tc.closeErr == nil && err != nil {
				t.Fatalf("closed TLS socket prevented replacement reconciliation: %v", err)
			}
		})
	}
}

func TestReplacementRevocationKeepsUnknownCloseFailureV1(t *testing.T) {
	for _, closeErr := range []error{errors.New("unknown socket close failure"), fmt.Errorf("tls: failed to send closeNotify alert (but connection was closed anyway): %w", syscall.EPIPE)} {
		stream := &fixedPeerTCPStreamV1{conns: make(map[*fixedPeerTCPConnV1]struct{}), peerNodes: map[hraft.ServerAddress]raftcluster.NodeID{"old:1": "old"}}
		left, right := net.Pipe()
		defer right.Close()
		if _, err := stream.track(&replacementUnknownCloseFailureConnV1{Conn: left, err: closeErr}, 0); err != nil {
			t.Fatal(err)
		}
		err := stream.replaceAuthorizedPeersV1(nil)
		if !errors.Is(err, closeErr) || stream.generation != 1 || len(stream.conns) != 0 {
			t.Fatalf("unknown close failure or revocation was lost: %v", err)
		}
	}
}

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
