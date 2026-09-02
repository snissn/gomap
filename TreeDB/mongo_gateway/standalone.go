package mongogateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	treenativewire "github.com/snissn/gomap/TreeDB/nativewire"
)

const (
	DefaultStandaloneAddr           = "127.0.0.1:27017"
	DefaultStandaloneDocumentFormat = collections.DocumentFormatBSON
)

const defaultTLSHandshakeTimeout = 10 * time.Second

// TransportStatus describes the listener policy selected for a standalone
// gateway. It deliberately contains no certificate or key material.
type TransportStatus struct {
	Mode                     string
	InsecureRemoteOverride   bool
	TLSMinimumVersion        uint16
	TLSCertificateNotAfter   time.Time
	TLSClientCertificateMode string
}

// TransportMetrics durations and handshake counts cover TLS listeners; connection
// counts cover both plaintext and TLS listeners. ActiveConnections includes
// accepted connections while their TLS handshake is pending.
type TransportMetrics struct {
	HandshakesStarted, HandshakesSucceeded, HandshakesFailed, ActiveHandshakes uint64
	HandshakeTotalNanoseconds, HandshakeMaxNanoseconds                         uint64
	ConnectionsAccepted, ConnectionsClosed, ActiveConnections                  uint64
}
type transportMetrics struct {
	started, succeeded, failed, active, totalNS, maxNS atomic.Uint64
	accepted, closed, activeConnections                atomic.Uint64
}

// recordHandshakeDuration records a completed TLS handshake. A handshake can
// complete within the clock's nanosecond resolution, but completed handshakes
// must remain visible in both duration metrics.
func (m *transportMetrics) recordHandshakeDuration(elapsed time.Duration) {
	nanoseconds := uint64(elapsed)
	if nanoseconds == 0 {
		nanoseconds = 1
	}
	m.totalNS.Add(nanoseconds)
	for prior := m.maxNS.Load(); nanoseconds > prior && !m.maxNS.CompareAndSwap(prior, nanoseconds); prior = m.maxNS.Load() {
	}
}

// StandaloneOptions configures a TreeDB-backed MongoDB gateway process.
//
// The standalone server intentionally exposes a small MongoDB-compatible subset
// implemented by Server. It is suitable for local clients and benchmarks, not
// for claiming full MongoDB feature parity.
type StandaloneOptions struct {
	Dir     string
	Profile treedb.Profile

	DefaultCollectionOptions  collections.CollectionOptions
	DefaultIndexStoragePolicy collections.RootStoragePolicy

	MaxMessageLength         int32
	MaxFindScanDocuments     int
	MaxCursorRetainedBytes   int
	MaxOpenCursors           int
	CursorIdleTimeout        time.Duration
	UpdateCoalescingMaxDelay time.Duration
	// UpdateCoalescingMaxBatchSet distinguishes an unset zero value from an
	// explicit zero, which disables update coalescing.
	UpdateCoalescingMaxBatchSet bool
	UpdateCoalescingMaxBatch    int
	UpdateCoalescingIdleTTL     time.Duration
	InsertCoalescingMaxDelay    time.Duration
	// InsertCoalescingMaxBatchSet distinguishes an unset zero value from an
	// explicit zero, which disables insert coalescing.
	InsertCoalescingMaxBatchSet bool
	InsertCoalescingMaxBatch    int
	InsertCoalescingIdleTTL     time.Duration
	ClusterSubmitter            treenativewire.ClusterSubmitter
	ClusterCatalogVersion       ClusterCatalogVersionProvider

	// TLSCertFile and TLSKeyFile enable TLS for the standalone listener. They
	// must be supplied together. TLSCAFile optionally supplies client-CA roots;
	// RequireClientCert makes a verified client certificate mandatory.
	TLSCertFile         string
	TLSKeyFile          string
	TLSCAFile           string
	RequireClientCert   bool
	TLSMinVersion       uint16
	TLSHandshakeTimeout time.Duration
	// AllowInsecureRemote permits plaintext only for an explicitly opted-in
	// non-loopback listener. It is intended for controlled development only.
	AllowInsecureRemote bool
	// AuthenticationEnabled enables SCRAM-SHA-256 connection authentication.
	// Call Server.AuthCatalog.UpsertPassword before serving to bootstrap users.
	AuthenticationEnabled bool
}

// StandaloneServer owns the TreeDB backend, collection manager, and MongoDB
// gateway server used by the standalone gateway executable.
type StandaloneServer struct {
	Options     StandaloneOptions
	Backend     *backenddb.DB
	Collections *collections.CollectionManager
	Server      *Server

	cleanup          func() error
	closeOnce        sync.Once
	closeErr         error
	serveMu          sync.Mutex
	serveWG          sync.WaitGroup
	closing          bool
	serving          bool
	transportMu      sync.RWMutex
	transport        TransportStatus
	tlsConfig        *tls.Config
	transportMetrics *transportMetrics
}

// NormalizeStandaloneOptions applies standalone defaults and validates options.
func NormalizeStandaloneOptions(opts StandaloneOptions) (StandaloneOptions, error) {
	if opts.Dir == "" {
		return opts, errors.New("mongo gateway standalone: TreeDB Dir is required")
	}
	profile, err := normalizeStandaloneProfile(opts.Profile)
	if err != nil {
		return opts, err
	}
	opts.Profile = profile

	format, err := normalizeStandaloneDocumentFormat(opts.DefaultCollectionOptions.DocumentFormat)
	if err != nil {
		return opts, err
	}
	opts.DefaultCollectionOptions.DocumentFormat = format

	dataRoot, err := normalizeStandaloneRootStoragePolicy(opts.DefaultCollectionOptions.DataRootStoragePolicy)
	if err != nil {
		return opts, fmt.Errorf("mongo gateway standalone: data root storage policy: %w", err)
	}
	opts.DefaultCollectionOptions.DataRootStoragePolicy = dataRoot

	indexState, err := normalizeStandaloneRootStoragePolicy(opts.DefaultCollectionOptions.IndexStateStoragePolicy)
	if err != nil {
		return opts, fmt.Errorf("mongo gateway standalone: index state storage policy: %w", err)
	}
	opts.DefaultCollectionOptions.IndexStateStoragePolicy = indexState

	indexRoot, err := normalizeStandaloneRootStoragePolicy(opts.DefaultIndexStoragePolicy)
	if err != nil {
		return opts, fmt.Errorf("mongo gateway standalone: index root storage policy: %w", err)
	}
	opts.DefaultIndexStoragePolicy = indexRoot
	if opts.MaxMessageLength < 0 {
		return opts, errors.New("mongo gateway standalone: MaxMessageLength must be >= 0")
	}
	if opts.MaxFindScanDocuments < 0 {
		return opts, errors.New("mongo gateway standalone: MaxFindScanDocuments must be >= 0")
	}
	if opts.MaxCursorRetainedBytes < 0 {
		return opts, errors.New("mongo gateway standalone: MaxCursorRetainedBytes must be >= 0")
	}
	if opts.MaxOpenCursors < 0 {
		return opts, errors.New("mongo gateway standalone: MaxOpenCursors must be >= 0")
	}
	if opts.UpdateCoalescingMaxBatch < 0 {
		return opts, errors.New("mongo gateway standalone: UpdateCoalescingMaxBatch must be >= 0")
	}
	if opts.InsertCoalescingMaxBatch < 0 {
		return opts, errors.New("mongo gateway standalone: InsertCoalescingMaxBatch must be >= 0")
	}
	if opts.ClusterSubmitter != nil && opts.ClusterCatalogVersion == nil {
		return opts, errors.New("mongo gateway standalone: ClusterCatalogVersion is required when ClusterSubmitter is configured")
	}
	if (opts.TLSCertFile == "") != (opts.TLSKeyFile == "") {
		return opts, errors.New("mongo gateway standalone: TLSCertFile and TLSKeyFile must be supplied together")
	}
	if opts.RequireClientCert && opts.TLSCAFile == "" {
		return opts, errors.New("mongo gateway standalone: TLSCAFile is required when RequireClientCert is enabled")
	}
	if opts.RequireClientCert && opts.TLSCertFile == "" {
		return opts, errors.New("mongo gateway standalone: TLSCertFile and TLSKeyFile are required when RequireClientCert is enabled")
	}
	if opts.TLSMinVersion == 0 {
		opts.TLSMinVersion = tls.VersionTLS12
	}
	if opts.TLSMinVersion != tls.VersionTLS12 && opts.TLSMinVersion != tls.VersionTLS13 {
		return opts, errors.New("mongo gateway standalone: TLSMinVersion must be TLS 1.2 or TLS 1.3")
	}
	if opts.TLSHandshakeTimeout < 0 {
		return opts, errors.New("mongo gateway standalone: TLSHandshakeTimeout must be >= 0")
	}
	if opts.TLSHandshakeTimeout == 0 {
		opts.TLSHandshakeTimeout = defaultTLSHandshakeTimeout
	}

	return opts, nil
}

// OpenStandaloneServer opens TreeDB and returns a standalone MongoDB gateway.
func OpenStandaloneServer(opts StandaloneOptions) (*StandaloneServer, error) {
	normalized, err := NormalizeStandaloneOptions(opts)
	if err != nil {
		return nil, err
	}

	treeOpts := treedb.OptionsFor(normalized.Profile, normalized.Dir)
	open := treedb.OpenBackend
	if treeOpts.IndexOuterLeavesInValueLog {
		open = treedb.OpenBackendWithCachedLeafLog
	}
	backend, cleanup, err := open(treeOpts)
	if err != nil {
		return nil, err
	}

	manager := collections.NewCollectionManager(backend)
	server := NewServer()
	server.Collections = manager
	server.diagnosticCommandWALEnabled = backend.CommandWALEnabled
	server.DefaultCollectionOptions = normalized.DefaultCollectionOptions
	server.DefaultIndexStoragePolicy = normalized.DefaultIndexStoragePolicy
	server.ClusterSubmitter = normalized.ClusterSubmitter
	server.ClusterCatalogVersion = normalized.ClusterCatalogVersion
	server.AuthenticationEnabled = normalized.AuthenticationEnabled
	if normalized.AuthenticationEnabled {
		catalog, err := NewAuthCatalog(backend)
		if err != nil {
			errs := []error{err}
			if cleanup != nil {
				errs = append(errs, cleanup())
			}
			return nil, errors.Join(errs...)
		}
		server.AuthCatalog = catalog
	}
	if normalized.MaxMessageLength > 0 {
		server.MaxMessageLength = normalized.MaxMessageLength
	}
	server.MaxFindScanDocuments = normalized.MaxFindScanDocuments
	server.MaxCursorRetainedBytes = normalized.MaxCursorRetainedBytes
	server.MaxOpenCursors = normalized.MaxOpenCursors
	server.CursorIdleTimeout = normalized.CursorIdleTimeout
	server.UpdateCoalescingMaxDelay = normalized.UpdateCoalescingMaxDelay
	if normalized.UpdateCoalescingMaxBatchSet || normalized.UpdateCoalescingMaxBatch > 0 {
		server.UpdateCoalescingMaxBatch = normalized.UpdateCoalescingMaxBatch
	}
	server.UpdateCoalescingIdleTTL = normalized.UpdateCoalescingIdleTTL
	server.InsertCoalescingMaxDelay = normalized.InsertCoalescingMaxDelay
	if normalized.InsertCoalescingMaxBatchSet || normalized.InsertCoalescingMaxBatch > 0 {
		server.InsertCoalescingMaxBatch = normalized.InsertCoalescingMaxBatch
	}
	server.InsertCoalescingIdleTTL = normalized.InsertCoalescingIdleTTL

	tlsConfig, transport, err := loadStandaloneTLS(normalized)
	if err != nil {
		errs := []error{err}
		if cleanup != nil {
			errs = append(errs, cleanup())
		}
		return nil, errors.Join(errs...)
	}
	return &StandaloneServer{
		Options:          normalized,
		Backend:          backend,
		Collections:      manager,
		Server:           server,
		cleanup:          cleanup,
		transport:        transport,
		tlsConfig:        tlsConfig,
		transportMetrics: &transportMetrics{},
	}, nil
}

// Serve accepts MongoDB wire-protocol connections on ln until ctx is canceled,
// ln is closed, or the gateway returns an accept error. Serve owns ln and closes
// it before returning.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s == nil {
		if ln != nil {
			_ = ln.Close()
		}
		return errServerClosed
	}
	if ln == nil {
		return errors.New("mongo gateway server: nil listener")
	}
	if !s.registerListener(ln) {
		_ = ln.Close()
		return errServerClosed
	}
	defer s.unregisterListener(ln)
	defer func() { _ = ln.Close() }()
	if ctx == nil {
		ctx = context.Background()
	}
	serveCtx, cancel := context.WithCancel(ctx)

	listenerClosed := make(chan struct{})
	go func() {
		select {
		case <-serveCtx.Done():
			_ = ln.Close()
		case <-listenerClosed:
		}
	}()
	defer close(listenerClosed)

	var wg sync.WaitGroup
	var serveConnMu sync.Mutex
	serveConns := make(map[net.Conn]struct{})
	closeServeConns := func() {
		serveConnMu.Lock()
		conns := make([]net.Conn, 0, len(serveConns))
		for conn := range serveConns {
			conns = append(conns, conn)
		}
		serveConnMu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	}
	defer func() {
		cancel()
		closeServeConns()
		wg.Wait()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if serveCtx.Err() != nil || s.isClosed() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		serveConnMu.Lock()
		serveConns[conn] = struct{}{}
		serveConnMu.Unlock()
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer func() {
				serveConnMu.Lock()
				delete(serveConns, conn)
				serveConnMu.Unlock()
			}()
			_ = s.ServeConn(serveCtx, conn)
		}(conn)
	}
}

// ListenAndServe listens on addr and serves MongoDB wire-protocol clients.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	if s == nil || s.isClosed() {
		return errServerClosed
	}
	if addr == "" {
		addr = DefaultStandaloneAddr
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve accepts MongoDB clients for a standalone TreeDB gateway.
// Serve owns ln and closes it before returning, matching Server.Serve.
func (s *StandaloneServer) Serve(ctx context.Context, ln net.Listener) error {
	if s == nil || s.Server == nil {
		if ln != nil {
			_ = ln.Close()
		}
		return errServerClosed
	}
	s.serveMu.Lock()
	closing, serving := s.closing, s.serving
	if closing || serving {
		s.serveMu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
		if serving {
			return errors.New("mongo gateway standalone: a listener is already serving")
		}
		return errServerClosed
	}
	s.serving = true
	s.serveWG.Add(1)
	s.serveMu.Unlock()
	defer func() {
		s.serveMu.Lock()
		s.serving = false
		s.serveMu.Unlock()
		s.serveWG.Done()
	}()
	if err := s.validateListener(ln); err != nil {
		if ln != nil {
			_ = ln.Close()
		}
		return err
	}
	if s.Options.TLSCertFile != "" {
		ln = newTLSHandshakeListener(ln, s.tlsConfig.Clone(), s.Options.TLSHandshakeTimeout, s.transportMetrics)
	} else {
		ln = &countingListener{Listener: ln, metrics: s.transportMetrics}
	}
	return s.Server.Serve(ctx, ln)
}

// ListenAndServe listens on addr and serves MongoDB clients for a standalone
// TreeDB gateway.
func (s *StandaloneServer) ListenAndServe(ctx context.Context, addr string) error {
	if s == nil || s.Server == nil {
		return errServerClosed
	}
	s.serveMu.Lock()
	closing := s.closing
	s.serveMu.Unlock()
	if closing || s.Server.isClosed() {
		return errServerClosed
	}
	if addr == "" {
		addr = DefaultStandaloneAddr
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Close stops active MongoDB connections and closes the TreeDB backend. It is
// safe to call multiple times.
func (s *StandaloneServer) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		var errs []error
		s.serveMu.Lock()
		s.closing = true
		s.serveMu.Unlock()
		if s.Server != nil {
			errs = append(errs, s.Server.Close())
		}
		s.serveWG.Wait()
		if s.Collections != nil {
			errs = append(errs, s.Collections.FlushAll())
		}
		if s.cleanup != nil {
			errs = append(errs, s.cleanup())
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

// TransportStatus returns safe listener diagnostics for operator status output.
func (s *StandaloneServer) TransportStatus() TransportStatus {
	if s == nil {
		return TransportStatus{}
	}
	s.transportMu.RLock()
	defer s.transportMu.RUnlock()
	return s.transport
}

func (s *StandaloneServer) TransportMetrics() TransportMetrics {
	if s == nil || s.transportMetrics == nil {
		return TransportMetrics{}
	}
	m := s.transportMetrics
	return TransportMetrics{
		HandshakesStarted:         m.started.Load(),
		HandshakesSucceeded:       m.succeeded.Load(),
		HandshakesFailed:          m.failed.Load(),
		ActiveHandshakes:          m.active.Load(),
		HandshakeTotalNanoseconds: m.totalNS.Load(),
		HandshakeMaxNanoseconds:   m.maxNS.Load(),
		ConnectionsAccepted:       m.accepted.Load(),
		ConnectionsClosed:         m.closed.Load(),
		ActiveConnections:         m.activeConnections.Load(),
	}
}

func (s *StandaloneServer) validateListener(ln net.Listener) error {
	if ln == nil {
		return errors.New("mongo gateway standalone: nil listener")
	}
	loopback := isLoopbackListener(ln.Addr())
	if s.Options.AuthenticationEnabled && !loopback && s.Options.TLSCertFile == "" {
		return fmt.Errorf("mongo gateway standalone: refusing password authentication on plaintext non-loopback listener %q; configure TLS", ln.Addr())
	}
	if !loopback && s.Options.TLSCertFile == "" && !s.Options.AllowInsecureRemote {
		return fmt.Errorf("mongo gateway standalone: refusing plaintext non-loopback listener %q; configure TLS or set AllowInsecureRemote", ln.Addr())
	}
	s.transportMu.Lock()
	if s.Options.TLSCertFile != "" {
		s.transport.Mode, s.transport.InsecureRemoteOverride = "tls", false
	} else if loopback {
		s.transport.Mode, s.transport.InsecureRemoteOverride = "plaintext-loopback", false
	} else {
		s.transport.Mode, s.transport.InsecureRemoteOverride = "plaintext-insecure-remote", true
	}
	s.transportMu.Unlock()
	return nil
}

// ValidateListener applies standalone transport policy before announcing startup.
func (s *StandaloneServer) ValidateListener(ln net.Listener) error {
	if s == nil {
		return errServerClosed
	}
	s.serveMu.Lock()
	defer s.serveMu.Unlock()
	if s.closing || s.Server == nil || s.Server.isClosed() {
		return errServerClosed
	}
	if s.serving {
		return errors.New("mongo gateway standalone: a listener is already serving")
	}
	// Keep serveMu through publication in validateListener: otherwise a
	// concurrent Serve could set serving and publish its status between a probe's
	// check and its status update.
	return s.validateListener(ln)
}

func isLoopbackListener(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}

func loadStandaloneTLS(opts StandaloneOptions) (*tls.Config, TransportStatus, error) {
	status := TransportStatus{Mode: "plaintext-loopback", TLSMinimumVersion: opts.TLSMinVersion}
	if opts.TLSCertFile == "" {
		return nil, status, nil
	}
	cert, err := tls.LoadX509KeyPair(opts.TLSCertFile, opts.TLSKeyFile)
	if err != nil {
		return nil, status, fmt.Errorf("mongo gateway standalone: load TLS certificate: %w", err)
	}
	if len(cert.Certificate) == 0 {
		return nil, status, errors.New("mongo gateway standalone: TLS certificate has no leaf")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, status, fmt.Errorf("mongo gateway standalone: parse TLS certificate: %w", err)
	}
	status.Mode, status.TLSCertificateNotAfter = "tls", leaf.NotAfter
	if opts.RequireClientCert {
		status.TLSClientCertificateMode = "require-and-verify"
	} else {
		status.TLSClientCertificateMode = "none"
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: opts.TLSMinVersion}
	if opts.TLSCAFile != "" {
		pem, err := os.ReadFile(opts.TLSCAFile)
		if err != nil {
			return nil, status, fmt.Errorf("mongo gateway standalone: read TLS CA bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, status, errors.New("mongo gateway standalone: TLS CA bundle contains no certificates")
		}
		cfg.ClientCAs = pool
	}
	if opts.RequireClientCert {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, status, nil
}

type tlsHandshakeListener struct {
	net.Listener
	config  *tls.Config
	timeout time.Duration
	metrics *transportMetrics

	mu     sync.Mutex
	closed bool
	conns  map[*tlsHandshakeConn]struct{}
}

func newTLSHandshakeListener(ln net.Listener, config *tls.Config, timeout time.Duration, metrics *transportMetrics) *tlsHandshakeListener {
	return &tlsHandshakeListener{
		Listener: ln,
		config:   config,
		timeout:  timeout,
		metrics:  metrics,
		conns:    make(map[*tlsHandshakeConn]struct{}),
	}
}

func (l *tlsHandshakeListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tlsConn := &tlsHandshakeConn{Conn: newCountingConn(conn, l.metrics), config: l.config, timeout: l.timeout, metrics: l.metrics}
	tlsConn.onClose = func() {
		l.mu.Lock()
		delete(l.conns, tlsConn)
		l.mu.Unlock()
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		_ = tlsConn.Close()
		return nil, net.ErrClosed
	}
	l.conns[tlsConn] = struct{}{}
	l.mu.Unlock()
	return tlsConn, nil
}

// Close also closes accepted TLS connections that may still be entering the
// generic server's connection registry. This makes shutdown interrupt a
// pending handshake even during accept/register races.
func (l *tlsHandshakeListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return l.Listener.Close()
	}
	l.closed = true
	conns := make([]*tlsHandshakeConn, 0, len(l.conns))
	for conn := range l.conns {
		conns = append(conns, conn)
	}
	l.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	return l.Listener.Close()
}

type countingListener struct {
	net.Listener
	metrics *transportMetrics
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newCountingConn(conn, l.metrics), nil
}

func newCountingConn(conn net.Conn, metrics *transportMetrics) *countingConn {
	metrics.accepted.Add(1)
	metrics.activeConnections.Add(1)
	return &countingConn{Conn: conn, metrics: metrics}
}

type countingConn struct {
	net.Conn
	metrics   *transportMetrics
	closeOnce sync.Once
}

func (c *countingConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() {
		c.metrics.activeConnections.Add(^uint64(0))
		c.metrics.closed.Add(1)
	})
	return err
}

type tlsHandshakeConn struct {
	net.Conn
	config       *tls.Config
	timeout      time.Duration
	metrics      *transportMetrics
	once         sync.Once
	closeOnce    sync.Once
	tlsConn      *tls.Conn
	handshakeErr error
	onClose      func()
}

func (c *tlsHandshakeConn) handshake() error {
	c.once.Do(func() {
		c.metrics.started.Add(1)
		c.metrics.active.Add(1)
		started := time.Now()
		defer func() {
			c.metrics.active.Add(^uint64(0))
			c.metrics.recordHandshakeDuration(time.Since(started))
		}()
		c.tlsConn = tls.Server(c.Conn, c.config)
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		c.handshakeErr = c.tlsConn.HandshakeContext(ctx)
		cancel()
		if c.handshakeErr == nil {
			c.metrics.succeeded.Add(1)
		} else {
			c.metrics.failed.Add(1)
			_ = c.Close()
		}
	})
	return c.handshakeErr
}
func (c *tlsHandshakeConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
	})
	return err
}
func (c *tlsHandshakeConn) Read(p []byte) (int, error) {
	if err := c.handshake(); err != nil {
		return 0, err
	}
	return c.tlsConn.Read(p)
}
func (c *tlsHandshakeConn) Write(p []byte) (int, error) {
	if err := c.handshake(); err != nil {
		return 0, err
	}
	return c.tlsConn.Write(p)
}

func normalizeStandaloneProfile(profile treedb.Profile) (treedb.Profile, error) {
	normalized, ok := treedb.ParsePublicProfile(string(profile), treedb.ProfileCommandWALDurable)
	if !ok {
		return "", fmt.Errorf("mongo gateway standalone: unsupported TreeDB profile %q; allowed: %s", profile, treedb.ProfileFlagHelp)
	}
	return normalized, nil
}

func normalizeStandaloneDocumentFormat(format collections.DocumentFormat) (collections.DocumentFormat, error) {
	normalized := collections.DocumentFormat(strings.ToLower(strings.TrimSpace(string(format))))
	switch normalized {
	case collections.DocumentFormatDefault, collections.DocumentFormatBSON:
		return collections.DocumentFormatBSON, nil
	case collections.DocumentFormatJSON:
		return collections.DocumentFormatJSON, nil
	case collections.DocumentFormatTemplateV1:
		return collections.DocumentFormatTemplateV1, nil
	default:
		return "", fmt.Errorf("mongo gateway standalone: unsupported collection document format %q", format)
	}
}

func normalizeStandaloneRootStoragePolicy(policy collections.RootStoragePolicy) (collections.RootStoragePolicy, error) {
	normalized := collections.RootStoragePolicy(strings.ToLower(strings.TrimSpace(string(policy))))
	switch normalized {
	case collections.RootStorageDefault:
		return collections.RootStorageDefault, nil
	case collections.RootStorageFast:
		return collections.RootStorageFast, nil
	case collections.RootStorageCompressed:
		return collections.RootStorageCompressed, nil
	default:
		return "", fmt.Errorf("unsupported root storage policy %q", policy)
	}
}
