package nativewire

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

var errPeerAuthenticationV1 = errors.New("nativewire: peer authentication failed")

const peerHandshakeTimeoutV1 = 5 * time.Second

type peerTransportSecurityV1 struct {
	certificate tls.Certificate
	roots       *x509.CertPool
	identities  map[string]raftcluster.NodeID
	expires     time.Time
}

func peerCertificateIdentityV1(cluster string, node raftcluster.NodeID) string {
	return (&url.URL{Scheme: "spiffe", Host: "treedb", Path: "/cluster/" + cluster + "/node/" + string(node), RawPath: "/cluster/" + url.PathEscape(cluster) + "/node/" + url.PathEscape(string(node))}).String()
}

func readPeerCredentialV1(path string, limit int64, private bool) ([]byte, error) {
	if !filepath.IsAbs(path) || len(path) > 4096 {
		return nil, fmt.Errorf("%w: absolute bounded credential path required", raftcluster.ErrInvalidConfig)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > limit || (private && runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return nil, fmt.Errorf("%w: credential file type, size or private-key permissions", raftcluster.ErrInvalidConfig)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || (private && runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return nil, fmt.Errorf("%w: opened credential file changed", raftcluster.ErrInvalidConfig)
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("%w: credential exceeds byte limit", raftcluster.ErrInvalidConfig)
	}
	return raw, nil
}

func newPeerTransportSecurityV1(cluster string, node raftcluster.NodeID, credentials PeerCredentialsV1, nodes []raftcluster.NodeID) (*peerTransportSecurityV1, error) {
	if cluster == "" || len(cluster) > 128 || !utf8.ValidString(cluster) || node == "" || len(node) > 128 || !utf8.ValidString(string(node)) || len(nodes) == 0 || len(nodes) > 1024 {
		return nil, fmt.Errorf("%w: bounded explicit cluster/node inventory required for TLS", raftcluster.ErrInvalidConfig)
	}
	caPEM, err := readPeerCredentialV1(credentials.TrustRootsFile, 128<<10, false)
	if err != nil {
		return nil, err
	}
	certificatePEM, err := readPeerCredentialV1(credentials.CertificateFile, 64<<10, false)
	if err != nil {
		return nil, err
	}
	keyPEM, err := readPeerCredentialV1(credentials.PrivateKeyFile, 16<<10, true)
	if err != nil {
		return nil, err
	}
	defer clear(keyPEM)
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, err
	}
	if len(certificate.Certificate) == 0 || len(certificate.Certificate) > 8 {
		return nil, fmt.Errorf("%w: bounded certificate chain required", raftcluster.ErrInvalidConfig)
	}
	if certificate.Leaf == nil {
		certificate.Leaf, err = x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return nil, err
		}
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%w: empty trust roots", errPeerAuthenticationV1)
	}
	intermediates := x509.NewCertPool()
	for _, raw := range certificate.Certificate[1:] {
		parsed, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, err
		}
		intermediates.AddCert(parsed)
	}
	expires := certificate.Leaf.NotAfter
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		chains, err := certificate.Leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}})
		if err != nil {
			return nil, errors.Join(errPeerAuthenticationV1, err)
		}
		expires = peerEarlierDeadlineV1(expires, peerVerifiedChainExpiryV1(chains))
	}
	security := &peerTransportSecurityV1{certificate: certificate, roots: roots, identities: make(map[string]raftcluster.NodeID, len(nodes)), expires: expires}
	for _, id := range nodes {
		if len(id) > 128 || !utf8.ValidString(string(id)) {
			return nil, fmt.Errorf("%w: bounded TLS node identity required", raftcluster.ErrInvalidConfig)
		}
		identity := peerCertificateIdentityV1(cluster, id)
		if id == "" || security.identities[identity] != "" {
			return nil, fmt.Errorf("%w: duplicate/empty TLS inventory node", raftcluster.ErrInvalidConfig)
		}
		security.identities[identity] = id
	}
	local, err := security.identity(certificate.Leaf)
	if err != nil || local != node {
		return nil, fmt.Errorf("%w: local certificate does not name configured cluster/node", errPeerAuthenticationV1)
	}
	return security, nil
}

func (s *peerTransportSecurityV1) identity(certificate *x509.Certificate) (raftcluster.NodeID, error) {
	if certificate == nil || len(certificate.URIs) != 1 || time.Now().Before(certificate.NotBefore) || !time.Now().Before(certificate.NotAfter) {
		return "", errPeerAuthenticationV1
	}
	node := s.identities[certificate.URIs[0].String()]
	if node == "" {
		return "", errPeerAuthenticationV1
	}
	return node, nil
}

// TLS performs ordinary chain, usage, validity and server-address validation.
// The additional URI identity selects a configured node and group permission;
// neither a configuration digest nor a certificate CommonName grants access.
func (s *peerTransportSecurityV1) serverConn(conn net.Conn, allowed map[raftcluster.NodeID]bool) net.Conn {
	guard := &peerCredentialDeadlineConnV1{Conn: conn, expires: s.expires}
	// This raw deadline bounds unauthenticated handshakes. Successful peer
	// verification restores the caller's recorded deadline (or certificate
	// expiry), without leaving the handshake timeout on pooled Raft traffic.
	if err := conn.SetDeadline(peerEarlierDeadlineV1(time.Now().Add(peerHandshakeTimeoutV1), guard.expires)); err != nil {
		_ = conn.Close()
	}
	config := &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{s.certificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: s.roots,
		SessionTicketsDisabled: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
				return errPeerAuthenticationV1
			}
			node, err := s.identity(state.PeerCertificates[0])
			if err != nil || (allowed != nil && !allowed[node]) {
				return errPeerAuthenticationV1
			}
			return guard.expireAt(peerVerifiedChainExpiryV1(state.VerifiedChains))
		},
	}
	return tls.Server(guard, config)
}

func (s *peerTransportSecurityV1) dial(ctx context.Context, address string, expected raftcluster.NodeID) (net.Conn, error) {
	return s.dialUsing(ctx, address, expected, func(ctx context.Context, address string) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp", address) })
}

func (s *peerTransportSecurityV1) dialUsing(ctx context.Context, address string, expected raftcluster.NodeID, dial func(context.Context, string) (net.Conn, error)) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, peerHandshakeTimeoutV1)
	defer cancel()
	host, _, err := net.SplitHostPort(address)
	if err != nil || expected == "" {
		return nil, raftcluster.ErrInvalidConfig
	}
	raw, err := dial(ctx, address)
	if err != nil {
		return nil, err
	}
	guard := &peerCredentialDeadlineConnV1{Conn: raw, expires: s.expires}
	config := &tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: s.roots, ServerName: host,
		Certificates: []tls.Certificate{s.certificate}, SessionTicketsDisabled: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
				return errPeerAuthenticationV1
			}
			node, err := s.identity(state.PeerCertificates[0])
			if err != nil || node != expected {
				return errPeerAuthenticationV1
			}
			return guard.expireAt(peerVerifiedChainExpiryV1(state.VerifiedChains))
		},
	}
	conn := tls.Client(guard, config)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}

type peerSecureListenerV1 struct {
	net.Listener
	security *peerTransportSecurityV1
	allowed  map[raftcluster.NodeID]bool
	admission *peerNodeAdmissionV1
	scope string
}

func peerVerifiedChainExpiryV1(chains [][]*x509.Certificate) time.Time {
	expires := chains[0][0].NotAfter
	for _, certificate := range chains[0] {
		if certificate.NotAfter.Before(expires) {
			expires = certificate.NotAfter
		}
	}
	return expires
}

func (l *peerSecureListenerV1) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil { return nil, err }
		if l.admission != nil {
			conn, err = l.admission.accept(conn, l.scope)
			if err != nil { if errors.Is(err, net.ErrClosed) { return nil, err }; continue }
		}
		return l.security.serverConn(conn, l.allowed), nil
	}
}

// Keep socket deadlines no later than either certificate's expiry. This also
// interrupts an idle socket at expiry without a timer goroutine per socket.
type peerCredentialDeadlineConnV1 struct {
	net.Conn
	mu            sync.Mutex
	expires       time.Time
	readDeadline  time.Time
	writeDeadline time.Time
}

func peerEarlierDeadlineV1(deadline, expires time.Time) time.Time {
	if deadline.IsZero() || expires.Before(deadline) {
		return expires
	}
	return deadline
}

func (c *peerCredentialDeadlineConnV1) expireAt(expiry time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if expiry.Before(c.expires) {
		c.expires = expiry
	}
	return errors.Join(c.Conn.SetReadDeadline(peerEarlierDeadlineV1(c.readDeadline, c.expires)), c.Conn.SetWriteDeadline(peerEarlierDeadlineV1(c.writeDeadline, c.expires)))
}

func (c *peerCredentialDeadlineConnV1) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = deadline
	return c.Conn.SetReadDeadline(peerEarlierDeadlineV1(deadline, c.expires))
}

func (c *peerCredentialDeadlineConnV1) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = deadline
	return c.Conn.SetWriteDeadline(peerEarlierDeadlineV1(deadline, c.expires))
}

func (c *peerCredentialDeadlineConnV1) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline, c.writeDeadline = deadline, deadline
	return c.Conn.SetDeadline(peerEarlierDeadlineV1(deadline, c.expires))
}
