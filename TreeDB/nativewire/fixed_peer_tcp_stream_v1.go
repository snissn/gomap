package nativewire

import (
	"context"
	"errors"
	"io"
	"net"
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
	conns      map[*fixedPeerTCPConnV1]struct{}
	closeOnce  sync.Once
	closeErr   error
	security   *peerTransportSecurityV1
	admission  *peerNodeAdmissionV1
	scope      string
	peerNodes  map[hraft.ServerAddress]raftcluster.NodeID
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
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	if admission != nil {
		listener, err = admission.listener(listener)
		if err != nil { return nil, err }
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream := &fixedPeerTCPStreamV1{
		Listener: listener, advertised: advertised, ctx: ctx, cancel: cancel,
		conns: make(map[*fixedPeerTCPConnV1]struct{}), security: security, admission: admission, scope: scope,
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
	return hraft.NewNetworkTransport(stream, 4, timeout, io.Discard), nil
}

func (s *fixedPeerTCPStreamV1) Addr() net.Addr { return s.advertised }

func (s *fixedPeerTCPStreamV1) track(conn net.Conn) (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	tracked := &fixedPeerTCPConnV1{Conn: conn, owner: s}
	s.conns[tracked] = struct{}{}
	return tracked, nil
}

func (s *fixedPeerTCPStreamV1) Accept() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return s.track(conn)
}

func (s *fixedPeerTCPStreamV1) Dial(address hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if s.security != nil {
		node := s.peerNodes[address]
		if node == "" {
			return nil, errPeerAuthenticationV1
		}
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		defer cancel()
		dial := func(ctx context.Context, address string) (net.Conn, error) {
			plain := func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp", address) }
			if s.admission != nil { return s.admission.dial(ctx, s.scope, plain) }
			return plain(ctx)
		}
		conn, err := s.security.dialUsing(ctx, string(address), node, dial)
		if err != nil {
			return nil, err
		}
		return s.track(conn)
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(s.ctx, "tcp", string(address))
	if err != nil {
		return nil, err
	}
	return s.track(conn)
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
