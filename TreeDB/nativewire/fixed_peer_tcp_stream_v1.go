package nativewire

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// The upstream NetworkTransport closes its listener, but an accepted connection
// can remain blocked reading its next command. Own accepted and dialed sockets
// so runtime shutdown interrupts idle reads and pending dials on this node.
type fixedPeerTCPStreamV1 struct {
	net.Listener
	advertised net.Addr
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	generation uint64
	conns      map[*fixedPeerTCPConnV1]struct{}
	closeOnce  sync.Once
	closeErr   error
	security   *peerTransportSecurityV1
	admission  *peerNodeAdmissionV1
	scope      string
	peerNodes  map[hraft.ServerAddress]raftcluster.NodeID
	timeout    time.Duration
}

type fixedPeerTCPConnV1 struct {
	net.Conn
	owner     *fixedPeerTCPStreamV1
	closeOnce sync.Once
	closeErr  error
}

func newFixedPeerTCPTransportV1(listen string, advertised net.Addr, timeout time.Duration) (*hraft.NetworkTransport, error) {
	return newFixedPeerTCPTransportWithSecurityV1(listen, advertised, timeout, nil, nil)
}

func newFixedPeerTCPTransportWithSecurityV1(listen string, advertised net.Addr, timeout time.Duration, security *peerTransportSecurityV1, peers []raftcluster.Peer) (*hraft.NetworkTransport, error) {
	return newFixedPeerTCPTransportWithAdmissionV1(listen, advertised, timeout, security, peers, nil, "")
}

func newFixedPeerTCPTransportWithAdmissionV1(listen string, advertised net.Addr, timeout time.Duration, security *peerTransportSecurityV1, peers []raftcluster.Peer, admission *peerNodeAdmissionV1, scope string) (*hraft.NetworkTransport, error) {
	transport, _, err := newFixedPeerTCPTransportOwnedV1(listen, advertised, timeout, security, peers, admission, scope)
	return transport, err
}

func newFixedPeerTCPTransportOwnedV1(listen string, advertised net.Addr, timeout time.Duration, security *peerTransportSecurityV1, peers []raftcluster.Peer, admission *peerNodeAdmissionV1, scope string) (*hraft.NetworkTransport, *fixedPeerTCPStreamV1, error) {
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, nil, err
	}
	return newFixedPeerTCPTransportListenerV1(listener, advertised, timeout, security, peers, admission, scope)
}

func newFixedPeerTCPTransportListenerV1(listener net.Listener, advertised net.Addr, timeout time.Duration, security *peerTransportSecurityV1, peers []raftcluster.Peer, admission *peerNodeAdmissionV1, scope string) (*hraft.NetworkTransport, *fixedPeerTCPStreamV1, error) {
	var err error
	if admission != nil {
		listener, err = admission.listener(listener)
		if err != nil {
			return nil, nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream := &fixedPeerTCPStreamV1{
		Listener: listener, advertised: advertised, ctx: ctx, cancel: cancel,
		conns: make(map[*fixedPeerTCPConnV1]struct{}), security: security, admission: admission, scope: scope, timeout: timeout,
	}
	stream.peerNodes = make(map[hraft.ServerAddress]raftcluster.NodeID, len(peers))
	for _, peer := range peers {
		stream.peerNodes[hraft.ServerAddress(peer.Address)] = peer.ID
	}
	if security != nil {
		allowed := make(map[raftcluster.NodeID]bool, len(peers))
		stream.peerNodes = make(map[hraft.ServerAddress]raftcluster.NodeID, len(peers))
		for _, peer := range peers {
			allowed[peer.ID] = true
			stream.peerNodes[hraft.ServerAddress(peer.Address)] = peer.ID
		}
		stream.Listener = &peerSecureListenerV1{Listener: listener, security: security, allowed: allowed, admission: admission, scope: scope}
	}
	return hraft.NewNetworkTransport(stream, 4, timeout, io.Discard), stream, nil
}

func (s *fixedPeerTCPStreamV1) Addr() net.Addr { return s.advertised }

func (s *fixedPeerTCPStreamV1) track(conn net.Conn, generation uint64) (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.generation != generation {
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	tracked := &fixedPeerTCPConnV1{Conn: conn, owner: s}
	s.conns[tracked] = struct{}{}
	return tracked, nil
}

func (s *fixedPeerTCPStreamV1) Accept() (net.Conn, error) {
	for {
		s.mu.Lock()
		generation := s.generation
		s.mu.Unlock()
		conn, err := s.Listener.Accept()
		if err != nil {
			return nil, err
		}
		conn, err = newPeerRaftWireConnV1(conn, s.admission, s.scope, true, s.timeout)
		if err != nil {
			return nil, err
		}
		tracked, err := s.track(conn, generation)
		if err == nil {
			return tracked, nil
		}
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return nil, err
		}
		// A removal raced this accept/handshake. Drop its old authorization and
		// accept again rather than terminating the native transport listener.
	}
}

func (s *fixedPeerTCPStreamV1) Dial(address hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	s.mu.Lock()
	generation, node := s.generation, s.peerNodes[address]
	s.mu.Unlock()
	if s.security != nil {
		if node == "" {
			return nil, errPeerAuthenticationV1
		}
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		defer cancel()
		dial := func(ctx context.Context, address string) (net.Conn, error) {
			plain := func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp", address) }
			if s.admission != nil {
				return s.admission.dial(ctx, s.scope, plain)
			}
			return plain(ctx)
		}
		conn, err := s.security.dialUsing(ctx, string(address), node, dial)
		if err != nil {
			return nil, err
		}
		conn, err = newPeerRaftWireConnV1(conn, s.admission, s.scope, false, timeout)
		if err != nil {
			return nil, err
		}
		return s.track(conn, generation)
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(s.ctx, "tcp", string(address))
	if err != nil {
		return nil, err
	}
	return s.track(conn, generation)
}

func (c *fixedPeerTCPConnV1) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.mu.Unlock()
	})
	return c.closeErr
}

func (s *fixedPeerTCPStreamV1) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		connections := make([]*fixedPeerTCPConnV1, 0, len(s.conns))
		for conn := range s.conns {
			connections = append(connections, conn)
		}
		s.mu.Unlock()
		s.cancel()
		s.closeErr = s.Listener.Close()
		for _, conn := range connections {
			s.closeErr = errors.Join(s.closeErr, conn.Close())
		}
	})
	return s.closeErr
}

// replaceAuthorizedPeersV1 installs an immutable transport identity view only
// after the runtime proves committed replacement authority. Removal revokes all
// tracked connections and in-flight handshakes; additions preserve live streams.
func (s *fixedPeerTCPStreamV1) replaceAuthorizedPeersV1(peers []raftcluster.Peer) error {
	nodes := make(map[hraft.ServerAddress]raftcluster.NodeID, len(peers))
	allowed := make(map[raftcluster.NodeID]bool, len(peers))
	for _, peer := range peers {
		nodes[hraft.ServerAddress(peer.Address)] = peer.ID
		allowed[peer.ID] = true
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	removed := false
	for address, node := range s.peerNodes {
		if nodes[address] != node {
			removed = true
			break
		}
	}
	var connections []*fixedPeerTCPConnV1
	if removed {
		s.generation++
		connections = make([]*fixedPeerTCPConnV1, 0, len(s.conns))
		for conn := range s.conns {
			connections = append(connections, conn)
		}
	}
	s.peerNodes = nodes
	if listener, ok := s.Listener.(*peerSecureListenerV1); ok {
		listener.replaceAllowedV1(allowed)
	}
	s.mu.Unlock()
	var err error
	for _, conn := range connections {
		closeErr := conn.Close()
		if !closedTLSAlertOnRevocationV1(conn.Conn, closeErr) {
			err = errors.Join(err, closeErr)
		}
	}
	return err
}

// Go's TLS Close returns this alert error only after its underlying Conn.Close
// succeeded. A peer revocation has already advanced the generation and removed
// the tracked socket, so this exact result cannot undo that revocation.
func closedTLSAlertOnRevocationV1(conn net.Conn, err error) bool {
	if err == nil || !strings.HasPrefix(err.Error(), "tls: failed to send closeNotify alert (but connection was closed anyway): ") {
		return false
	}
	if wire, ok := conn.(*peerRaftWireConnV1); ok {
		conn = wire.Conn
	}
	_, ok := conn.(*tls.Conn)
	return ok
}
