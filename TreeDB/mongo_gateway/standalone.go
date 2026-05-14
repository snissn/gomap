package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	DefaultStandaloneAddr           = "127.0.0.1:27017"
	DefaultStandaloneDocumentFormat = collections.DocumentFormatBSON
)

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
}

// StandaloneServer owns the TreeDB backend, collection manager, and MongoDB
// gateway server used by the standalone gateway executable.
type StandaloneServer struct {
	Options     StandaloneOptions
	Backend     *backenddb.DB
	Collections *collections.CollectionManager
	Server      *Server

	cleanup   func() error
	closeOnce sync.Once
	closeErr  error
	serveMu   sync.Mutex
	serveWG   sync.WaitGroup
	closing   bool
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
	server.DefaultCollectionOptions = normalized.DefaultCollectionOptions
	server.DefaultIndexStoragePolicy = normalized.DefaultIndexStoragePolicy
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

	return &StandaloneServer{
		Options:     normalized,
		Backend:     backend,
		Collections: manager,
		Server:      server,
		cleanup:     cleanup,
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
	if s.closing {
		s.serveMu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
		return errServerClosed
	}
	s.serveWG.Add(1)
	s.serveMu.Unlock()
	defer s.serveWG.Done()
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

func normalizeStandaloneProfile(profile treedb.Profile) (treedb.Profile, error) {
	normalized := treedb.Profile(strings.ToLower(strings.TrimSpace(string(profile))))
	switch normalized {
	case "", treedb.ProfileDurable:
		return treedb.ProfileDurable, nil
	case treedb.ProfileFast:
		return treedb.ProfileFast, nil
	case treedb.ProfileWALOnFast:
		return treedb.ProfileWALOnFast, nil
	case treedb.ProfileBench:
		return treedb.ProfileBench, nil
	default:
		return "", fmt.Errorf("mongo gateway standalone: unsupported TreeDB profile %q", profile)
	}
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
