package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// This internal HTTP protocol is for fixed, trusted private peers. It is not a
// public mutation API. Both it and HashiCorp's TCP transport require a trusted
// network; neither authenticates peers cryptographically.
const fixedPeerMaxRPCBytesV1 = 8 << 20

type FixedPeerTCPNodeV1 struct {
	ID      raftcluster.NodeID
	Address string
}
type FixedPeerTCPGroupV1 struct {
	ID            raftcluster.GroupID
	Peers         []raftcluster.Peer
	BootstrapNode raftcluster.NodeID
	Features      raftcluster.FeatureSet
}

// Every process supplies the same Nodes/Catalog/Groups/timeouts and its own
// absolute roots/listen addresses. Only members open a data group. A single
// designated member bootstraps each group; restarts use existing Raft stores.
type FixedPeerTCPConfigV1 struct {
	NodeID                            raftcluster.NodeID
	DataRoot, RaftRoot, ListenAddress string
	RaftListen                        map[raftcluster.GroupID]string
	Nodes                             []FixedPeerTCPNodeV1
	Catalog                           FixedPeerTCPGroupV1
	Groups                            []FixedPeerTCPGroupV1
	RequestTimeout, RaftTimeout       time.Duration
}

type FixedPeerTCPStatusV1 struct {
	NodeID                               raftcluster.NodeID
	Address, ConfigDigest, RecoveryState string
	Catalog                              raftplacement.CatalogMetaStatusV1
	CatalogRaft                          raftcluster.RuntimeStatusV1
	Groups                               []FixedPeerTCPGroupStatusV1
}

type FixedPeerTCPGroupStatusV1 struct {
	raftcluster.RuntimeStatusV1
	CatalogVersion uint64
}

type fixedPeerDataV1 struct {
	db       *backenddb.DB
	fsm      *raftfsm.FSM
	provider *raftcluster.HashicorpRaftProvider
}

type FixedPeerTCPRuntimeV1 struct {
	config        FixedPeerTCPConfigV1
	client        *FixedPeerTCPClientV1
	authority     *raftplacement.CatalogMetaAuthorityV1
	meta          *raftcluster.CatalogMetaRaftProviderV1
	data          map[raftcluster.GroupID]*fixedPeerDataV1
	local, routed *raftcluster.GroupRoutedSubmitter
	transports    []*hraft.NetworkTransport
	server        *http.Server
	listener      net.Listener
	reopened      bool
	closeOnce     sync.Once
	closeErr      error
	requests      chan struct{}
}

type FixedPeerTCPClientV1 struct {
	config FixedPeerTCPConfigV1
	digest string
	http   *http.Client
}

func validateFixedPeerConfigV1(c FixedPeerTCPConfigV1) (FixedPeerTCPConfigV1, string, error) {
	// Copy caller-owned slices/maps before normalizing or starting goroutines.
	raw, err := json.Marshal(c)
	if err != nil {
		return c, "", err
	}
	c = FixedPeerTCPConfigV1{}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, "", err
	}
	invalid := func(reason string) (FixedPeerTCPConfigV1, string, error) {
		return c, "", fmt.Errorf("%w: %s", raftcluster.ErrInvalidConfig, reason)
	}
	if !filepath.IsAbs(c.DataRoot) || !filepath.IsAbs(c.RaftRoot) || c.DataRoot == c.RaftRoot {
		return invalid("distinct absolute data and raft roots required")
	}
	for _, pair := range [][2]string{{c.DataRoot, c.RaftRoot}, {c.RaftRoot, c.DataRoot}} {
		rel, e := filepath.Rel(filepath.Clean(pair[0]), filepath.Clean(pair[1]))
		if e != nil || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return invalid("data and raft roots overlap")
		}
	}
	if c.RequestTimeout < time.Millisecond || c.RequestTimeout > time.Minute || c.RaftTimeout < 50*time.Millisecond || c.RaftTimeout > 30*time.Second {
		return invalid("bounded request and raft timeouts required")
	}
	if len(c.Nodes) == 0 || len(c.Nodes) > 32 || len(c.Groups) == 0 || len(c.Groups) > 32 {
		return invalid("require 1..32 nodes and data groups")
	}
	addresses := map[string]bool{}
	addressOK := func(address string) bool {
		addr, e := net.ResolveTCPAddr("tcp", address)
		if e != nil || addr.IP == nil || addr.IP.IsUnspecified() || addr.Port <= 0 || addresses[addr.String()] || addr.String() != address {
			return false
		}
		addresses[addr.String()] = true
		return true
	}
	nodes := map[raftcluster.NodeID]bool{}
	for _, n := range c.Nodes {
		if nodes[n.ID] || n.ID == "" || !addressOK(n.Address) {
			return invalid("duplicate/invalid node identity or address")
		}
		nodes[n.ID] = true
		if n.ID == c.NodeID && n.Address != c.ListenAddress {
			return invalid("RPC listen must equal local advertised address")
		}
	}
	if !nodes[c.NodeID] {
		return invalid("local node absent")
	}
	if a, e := net.ResolveTCPAddr("tcp", c.ListenAddress); e != nil || a.Port <= 0 {
		return invalid("invalid RPC listen address")
	}
	groupIDs := map[raftcluster.GroupID]bool{}
	localGroups := map[raftcluster.GroupID]bool{}
	for i, g := range append([]FixedPeerTCPGroupV1{c.Catalog}, c.Groups...) {
		if groupIDs[g.ID] || len(g.Peers) == 0 || len(g.Peers) > 32 {
			return invalid("duplicate group or invalid member count")
		}
		groupIDs[g.ID] = true
		bootstrap := false
		if i == 0 && len(g.Peers) != len(c.Nodes) {
			return invalid("catalog membership must cover every configured node")
		}
		for _, p := range g.Peers {
			if !nodes[p.ID] || !addressOK(p.Address) {
				return invalid("unknown peer or duplicate/invalid raft address")
			}
			if p.ID == g.BootstrapNode {
				bootstrap = true
			}
			if p.ID == c.NodeID {
				localGroups[g.ID] = true
				if c.RaftListen[g.ID] != p.Address {
					return invalid("raft listen must equal local advertised address")
				}
			}
		}
		if !bootstrap {
			return invalid("bootstrap responsibility must name one voter")
		}
		resolved, e := raftcluster.Validate(raftcluster.Config{Dir: filepath.Join(c.DataRoot, string(g.ID)), ClusterDir: c.RaftRoot, NodeID: g.Peers[0].ID, GroupID: g.ID, Peers: g.Peers, Features: g.Features, DisableSideStores: true})
		if e != nil {
			return c, "", e
		}
		if raftcluster.FeatureSetRequiresV1(resolved.Features, raftcluster.FeatureVectorPartitionLifecycle) {
			return invalid("vector lifecycle is not supported by this runtime")
		}
		g.Peers, g.Features = resolved.Peers, resolved.Features
		for _, p := range g.Peers {
			for _, feature := range g.Features.Required {
				if !raftcluster.FeatureSetRequiresV1(p.Capabilities, feature.Name) {
					return invalid("voter lacks required feature floor")
				}
			}
			if i == 0 && !raftcluster.FeatureSetRequiresV1(p.Capabilities, raftcluster.FeatureCatalogMetaAuthority) {
				return invalid("meta voter lacks catalog authority feature")
			}
		}
		if i == 0 {
			c.Catalog = g
		} else {
			c.Groups[i-1] = g
		}
	}
	if !localGroups[c.Catalog.ID] {
		return invalid("every runtime must join the shared catalog")
	}
	if len(localGroups) != len(c.RaftListen) {
		return invalid("listen map must match hosted groups")
	}
	for g := range localGroups {
		a, e := net.ResolveTCPAddr("tcp", c.RaftListen[g])
		if e != nil || a.Port <= 0 {
			return invalid("explicit raft listen address required")
		}
	}
	slices.SortFunc(c.Nodes, func(a, b FixedPeerTCPNodeV1) int { return bytes.Compare([]byte(a.ID), []byte(b.ID)) })
	slices.SortFunc(c.Groups, func(a, b FixedPeerTCPGroupV1) int { return bytes.Compare([]byte(a.ID), []byte(b.ID)) })
	shared := c
	shared.NodeID = ""
	shared.DataRoot = ""
	shared.RaftRoot = ""
	shared.ListenAddress = ""
	shared.RaftListen = nil
	raw, _ = json.Marshal(shared)
	digest := sha256.Sum256(raw)
	return c, hex.EncodeToString(digest[:]), nil
}

func NewFixedPeerTCPClientV1(config FixedPeerTCPConfigV1) (*FixedPeerTCPClientV1, error) {
	c, digest, err := validateFixedPeerConfigV1(config)
	if err != nil {
		return nil, err
	}
	return &FixedPeerTCPClientV1{config: c, digest: digest, http: &http.Client{Timeout: c.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{Proxy: nil, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 4, IdleConnTimeout: c.RequestTimeout, ResponseHeaderTimeout: c.RequestTimeout}}}, nil
}

func OpenFixedPeerTCPRuntimeV1(config FixedPeerTCPConfigV1) (*FixedPeerTCPRuntimeV1, error) {
	client, err := NewFixedPeerTCPClientV1(config)
	if err != nil {
		return nil, err
	}
	r := &FixedPeerTCPRuntimeV1{config: client.config, client: client, authority: raftplacement.NewCatalogMetaAuthorityV1(), data: map[raftcluster.GroupID]*fixedPeerDataV1{}, requests: make(chan struct{}, 32)}
	fail := func(err error) (*FixedPeerTCPRuntimeV1, error) { _ = r.Close(); return nil, err }
	// Persist exact local configuration before opening any stores. A partial or
	// modified manifest refuses startup instead of silently reusing identities.
	if err := os.MkdirAll(r.config.RaftRoot, 0700); err != nil {
		return fail(err)
	}
	manifest := filepath.Join(r.config.RaftRoot, "fixed-peer-v1.json")
	raw, _ := json.Marshal(r.config)
	old, err := os.ReadFile(manifest)
	switch {
	case err == nil:
		if !bytes.Equal(old, raw) {
			return fail(fmt.Errorf("%w: persisted fixed-peer configuration mismatch", raftcluster.ErrInvalidConfig))
		}
		r.reopened = true
	case errors.Is(err, os.ErrNotExist):
		file, e := os.OpenFile(manifest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return fail(e)
		}
		_, e = file.Write(raw)
		e = errors.Join(e, file.Sync(), file.Close())
		if e != nil {
			return fail(e)
		}
	default:
		return fail(err)
	}
	// Sync the name as well as its contents before any identity-dependent store
	// is opened. Repeat on reopen so a previous failed directory sync is retried.
	if err := syncFixedPeerDirectoryV1(r.config.RaftRoot); err != nil {
		return fail(err)
	}
	var localEntries, routedEntries []raftcluster.GroupSubmitterV1
	for i, g := range append([]FixedPeerTCPGroupV1{r.config.Catalog}, r.config.Groups...) {
		if i > 0 {
			routedEntries = append(routedEntries, raftcluster.GroupSubmitterV1{GroupID: g.ID, Submitter: fixedPeerRemoteSubmitterV1{runtime: r, group: g}})
		}
		listen, hosted := r.config.RaftListen[g.ID]
		if !hosted {
			continue
		}
		var advertised string
		for _, p := range g.Peers {
			if p.ID == r.config.NodeID {
				advertised = p.Address
			}
		}
		addr, e := net.ResolveTCPAddr("tcp", advertised)
		if e != nil {
			return fail(e)
		}
		transport, e := hraft.NewTCPTransport(listen, addr, 4, r.config.RequestTimeout, io.Discard)
		if e != nil {
			return fail(e)
		}
		r.transports = append(r.transports, transport)
		cfg := raftcluster.Config{Dir: filepath.Join(r.config.DataRoot, string(g.ID)), ClusterDir: r.config.RaftRoot, DisableSideStores: true, NodeID: r.config.NodeID, GroupID: g.ID, Peers: g.Peers, Features: g.Features}
		raftConfig := hraft.DefaultConfig()
		raftConfig.HeartbeatTimeout = r.config.RaftTimeout
		raftConfig.ElectionTimeout = r.config.RaftTimeout
		raftConfig.LeaderLeaseTimeout = r.config.RaftTimeout
		raftConfig.LogOutput = io.Discard
		bootstrap := g.BootstrapNode == r.config.NodeID
		if i == 0 {
			r.meta, e = raftcluster.OpenCatalogMetaRaftProviderV1(raftcluster.CatalogMetaRaftProviderOptionsV1{Cluster: cfg, State: r.authority, Transport: transport, RaftConfig: raftConfig, Bootstrap: bootstrap, ApplyTimeout: r.config.RequestTimeout})
			if e != nil {
				return fail(e)
			}
			continue
		}
		d := &fixedPeerDataV1{}
		r.data[g.ID] = d
		d.db, e = backenddb.Open(backenddb.Options{Dir: cfg.Dir, CommandWAL: true, CommandWALStatsScan: true})
		if e != nil {
			return fail(e)
		}
		d.fsm, e = raftfsm.Open(raftfsm.Options{DB: d.db, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
		if e != nil {
			return fail(e)
		}
		d.provider, e = raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{Cluster: cfg, Applier: d.fsm, Transport: transport, RaftConfig: raftConfig, Bootstrap: bootstrap, ApplyTimeout: r.config.RequestTimeout})
		if e != nil {
			return fail(e)
		}
		submitter, e := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{Cluster: cfg, AdmissionProvider: d.provider, CommitSource: d.provider, Preflight: d.fsm, Applier: d.fsm, CatalogVersionProvider: d.fsm})
		if e != nil {
			return fail(e)
		}
		localEntries = append(localEntries, raftcluster.GroupSubmitterV1{GroupID: g.ID, Submitter: submitter})
	}
	if len(localEntries) > 0 {
		registry, e := raftcluster.NewGroupSubmitterRegistryV1(localEntries)
		if e != nil {
			return fail(e)
		}
		r.local, e = raftcluster.NewCatalogMetaGroupRoutedSubmitter(registry, r)
		if e != nil {
			return fail(e)
		}
	}
	registry, err := raftcluster.NewGroupSubmitterRegistryV1(routedEntries)
	if err != nil {
		return fail(err)
	}
	r.routed, err = raftcluster.NewCatalogMetaGroupRoutedSubmitter(registry, r)
	if err != nil {
		return fail(err)
	}
	r.listener, err = net.Listen("tcp", r.config.ListenAddress)
	if err != nil {
		return fail(err)
	}
	r.server = &http.Server{Handler: http.HandlerFunc(r.serve), ReadHeaderTimeout: r.config.RequestTimeout, ReadTimeout: r.config.RequestTimeout, WriteTimeout: 2 * r.config.RequestTimeout, IdleTimeout: r.config.RequestTimeout, MaxHeaderBytes: 4096}
	go func() { _ = r.server.Serve(r.listener) }()
	return r, nil
}

func syncFixedPeerDirectoryV1(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (r *FixedPeerTCPRuntimeV1) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		var errs []error
		if r.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), r.config.RequestTimeout)
			err := r.server.Shutdown(ctx)
			cancel()
			if err != nil {
				_ = r.server.Close()
			}
			errs = append(errs, err)
		} else if r.listener != nil {
			errs = append(errs, r.listener.Close())
		}
		if r.meta != nil {
			errs = append(errs, r.meta.Close())
		}
		for _, d := range r.data {
			if d.provider != nil {
				errs = append(errs, d.provider.Close())
			}
		}
		for _, t := range r.transports {
			errs = append(errs, t.Close())
		}
		for _, d := range r.data {
			if d.fsm != nil {
				errs = append(errs, d.fsm.Close())
			}
			if d.db != nil {
				errs = append(errs, d.db.Close())
			}
		}
		if r.client != nil {
			r.client.http.CloseIdleConnections()
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

func (r *FixedPeerTCPRuntimeV1) Status(ctx context.Context) (FixedPeerTCPStatusV1, error) {
	s := FixedPeerTCPStatusV1{NodeID: r.config.NodeID, ConfigDigest: r.client.digest, CatalogRaft: r.meta.RuntimeStatusV1(), RecoveryState: "new"}
	if r.reopened {
		s.RecoveryState = "reopened"
	}
	for _, n := range r.config.Nodes {
		if n.ID == r.config.NodeID {
			s.Address = n.Address
		}
	}
	s.Catalog, _ = r.authority.Status()
	for _, g := range r.config.Groups {
		if d := r.data[g.ID]; d != nil {
			status, err := d.provider.RuntimeStatusV1(ctx)
			if err != nil {
				return s, err
			}
			v, _, err := d.fsm.CurrentCatalogVersion(ctx)
			if err != nil {
				return s, err
			}
			s.Groups = append(s.Groups, FixedPeerTCPGroupStatusV1{RuntimeStatusV1: status, CatalogVersion: v})
		}
	}
	return s, nil
}

func (r *FixedPeerTCPRuntimeV1) catalogFence(ctx context.Context) (raftplacement.CatalogMetaStatusV1, error) {
	leader, err := r.client.leader(ctx, r.config.Catalog)
	if err != nil {
		return raftplacement.CatalogMetaStatusV1{}, err
	}
	reply, err := r.client.call(ctx, leader, "catalog-read", fixedPeerRequestV1{}, false)
	if err != nil {
		return raftplacement.CatalogMetaStatusV1{}, err
	}
	local, ok := r.authority.Status()
	if !ok || local.AppliedIndex < reply.Catalog.AppliedIndex || local.Epoch != reply.Catalog.Epoch || local.Digest != reply.Catalog.Digest {
		return local, raftplacement.ErrCatalogMetaUnavailable
	}
	return local, nil
}

func (r *FixedPeerTCPRuntimeV1) ValidateCatalogRouteMetadata(ctx context.Context, metadata raftentry.RequestMetadataV1) error {
	if _, err := r.catalogFence(ctx); err != nil {
		return err
	}
	return r.authority.ValidateCatalogRouteMetadata(ctx, metadata)
}

func (r *FixedPeerTCPRuntimeV1) route(ctx context.Context, request ClusterRouteRequest) (ClusterRouteTarget, error) {
	if _, err := r.catalogFence(ctx); err != nil {
		return ClusterRouteTarget{}, err
	}
	provider, err := NewCatalogMetaClusterRouteProvider(r.authority, r.authority.CurrentCatalogProof, r.meta)
	if err != nil {
		return ClusterRouteTarget{}, err
	}
	return provider.ClusterRoute(ctx, request)
}

func validateFixedPeerEntryRouteV1(entry []byte, metadata raftentry.RequestMetadataV1) error {
	decoded, err := iwire.DecodeDeterministicEntry(entry, iwire.Limits{})
	if err != nil {
		return err
	}
	request, err := clusterMutationRouteRequest(iwire.ValidatedCommand{Header: iwire.CommandHeader{ID: decoded.CommandID, Version: decoded.CommandVersion}, Known: decoded.Sections}, iwire.Limits{})
	if err != nil {
		return err
	}
	// Preserve the existing token/ring mutation refusal until authoritative
	// collection/index metadata is bound to the owner proof (#3473).
	if metadata.ClusterRoutePlacementMode != "collection" || metadata.ClusterRouteShape != "collection" {
		return raftcluster.ErrRouteTargetUnsupported
	}
	// Collection placement also collapses single/multiple document token inputs
	// to one collection route. Bind the actual command identity here; the existing
	// dispatcher performs the sole authoritative fence/re-resolution afterward.
	if !metadata.ClusterRouteKnown || metadata.ClusterRouteDatabase != request.Database || metadata.ClusterRouteCatalog != request.Catalog || metadata.ClusterRouteCollection != request.Collection {
		return raftplacement.ErrCatalogMetaRouteMismatch
	}
	return nil
}

type fixedPeerRemoteSubmitterV1 struct {
	runtime *FixedPeerTCPRuntimeV1
	group   FixedPeerTCPGroupV1
}

func (s fixedPeerRemoteSubmitterV1) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (raftcluster.SubmitResultV1, error) {
	leader, err := s.runtime.client.leader(ctx, s.group)
	if err != nil {
		return raftcluster.SubmitResultV1{}, err
	}
	// Exactly one forwarding attempt. Once a mutation request is sent, a lost
	// response is commit-ambiguous and must never trigger an automatic retry.
	reply, err := s.runtime.client.call(ctx, leader, "forward", fixedPeerRequestV1{Entry: entry, Metadata: metadata}, true)
	return reply.Submit, err
}

type fixedPeerRequestV1 struct {
	Entry    []byte
	Metadata raftentry.RequestMetadataV1
	Route    ClusterRouteRequest
}
type fixedPeerReplyV1 struct {
	NodeID       raftcluster.NodeID
	ConfigDigest string
	Error        string
	ErrorCode    string
	RouteError   *raftcluster.RouteErrorMetadata
	Status       FixedPeerTCPStatusV1
	Catalog      raftplacement.CatalogMetaStatusV1
	Submit       raftcluster.SubmitResultV1
	Route        ClusterRouteTarget
}

func (r *FixedPeerTCPRuntimeV1) serve(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	reply := fixedPeerReplyV1{NodeID: r.config.NodeID, ConfigDigest: r.client.digest}
	var err error
	defer func() {
		if err != nil {
			reply.Error = err.Error()
			reply.ErrorCode = fixedPeerErrorCodeV1(err)
			if m, ok := raftcluster.RouteErrorMetadataOf(err); ok {
				reply.RouteError = &m
			}
		}
		_ = json.NewEncoder(w).Encode(reply)
	}()
	select {
	case r.requests <- struct{}{}:
		defer func() { <-r.requests }()
	default:
		err = raftcluster.ErrAdmissionUnavailable
		return
	}
	if request.Method != "POST" || request.Header.Get("X-TreeDB-Node") != string(r.config.NodeID) || request.Header.Get("X-TreeDB-Config") != r.client.digest {
		err = raftcluster.ErrInvalidConfig
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), r.config.RequestTimeout)
	defer cancel()
	var body fixedPeerRequestV1
	decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, fixedPeerMaxRPCBytesV1))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&body); err != nil {
		return
	}
	var trailing any
	if e := decoder.Decode(&trailing); e != io.EOF {
		err = fmt.Errorf("invalid trailing RPC payload")
		return
	}
	switch request.URL.Path {
	case "/v1/status":
		reply.Status, err = r.Status(ctx)
	case "/v1/catalog-read":
		_, err = r.meta.LinearizableCatalogMetaAppliedIndexV1(ctx)
		if err == nil {
			reply.Catalog, _ = r.authority.Status()
		}
	case "/v1/catalog-publish":
		var command raftplacement.CatalogMetaCommandV1
		command, err = raftplacement.DecodeCatalogMetaCommandV1(body.Entry)
		if err != nil {
			return
		}
		if err = r.validateCatalog(command.Record.Catalog); err != nil {
			return
		}
		_, _, err = r.meta.SubmitCatalogMetaCommandV1(ctx, body.Entry)
		if err == nil {
			reply.Catalog, _ = r.authority.Status()
		}
	case "/v1/route":
		reply.Route, err = r.route(ctx, body.Route)
	case "/v1/submit", "/v1/forward":
		if err = validateFixedPeerEntryRouteV1(body.Entry, body.Metadata); err != nil {
			return
		}
		submitter := r.routed
		if request.URL.Path == "/v1/forward" {
			submitter = r.local
		}
		if submitter == nil {
			err = raftcluster.ErrRouteTargetUnknown
			return
		}
		reply.Submit, err = submitter.SubmitCommandEntryV1(ctx, body.Entry, body.Metadata)
	default:
		err = raftcluster.ErrRouteTargetUnsupported
	}
}

func (r *FixedPeerTCPRuntimeV1) validateCatalog(c raftplacement.CatalogV1) error {
	if raftcluster.FeatureSetRequiresV1(c.Features, raftcluster.FeatureVectorPartitionLifecycle) {
		return raftcluster.ErrUnsupportedFeature
	}
	for _, g := range c.Groups {
		var expected []raftcluster.NodeID
		for _, fixed := range r.config.Groups {
			if fixed.ID == g.ID {
				for _, p := range fixed.Peers {
					expected = append(expected, p.ID)
				}
			}
		}
		members := slices.Clone(g.Members)
		slices.Sort(members)
		slices.Sort(expected)
		if len(expected) == 0 || !slices.Equal(members, expected) {
			return fmt.Errorf("%w: catalog group differs from fixed membership", raftcluster.ErrInvalidConfig)
		}
	}
	return nil
}

func (c *FixedPeerTCPClientV1) call(ctx context.Context, node raftcluster.NodeID, operation string, body fixedPeerRequestV1, mutation bool) (fixedPeerReplyV1, error) {
	var reply fixedPeerReplyV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return reply, err
	}
	address := ""
	for _, n := range c.config.Nodes {
		if n.ID == node {
			address = n.Address
		}
	}
	if address == "" {
		return reply, raftcluster.ErrRouteTargetUnknown
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return reply, err
	}
	if len(raw) > fixedPeerMaxRPCBytesV1 {
		return reply, raftcluster.ErrRouteTargetUnsupported
	}
	var sent atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { sent.Store(true) }})
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+address+"/v1/"+operation, bytes.NewReader(raw))
	if err != nil {
		return reply, err
	}
	req.Header.Set("X-TreeDB-Node", string(node))
	req.Header.Set("X-TreeDB-Config", c.digest)
	response, err := c.http.Do(req)
	if err == nil {
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			err = fmt.Errorf("unexpected RPC HTTP status %d", response.StatusCode)
		}
		data, e := io.ReadAll(io.LimitReader(response.Body, fixedPeerMaxRPCBytesV1+1))
		err = errors.Join(err, e)
		if err == nil && len(data) > fixedPeerMaxRPCBytesV1 {
			err = errors.New("oversized RPC reply")
		}
		if err == nil {
			err = json.Unmarshal(data, &reply)
		}
		if err == nil && (reply.NodeID != node || reply.ConfigDigest != c.digest) {
			err = raftcluster.ErrInvalidConfig
		}
	}
	if err != nil {
		if mutation && sent.Load() {
			return reply, errors.Join(raftcluster.ErrCommitAmbiguous, err)
		}
		return reply, err
	}
	if reply.Error != "" {
		return reply, &fixedPeerRemoteErrorV1{message: reply.Error, code: reply.ErrorCode, route: reply.RouteError}
	}
	return reply, nil
}

func (c *FixedPeerTCPClientV1) leader(ctx context.Context, group FixedPeerTCPGroupV1) (raftcluster.NodeID, error) {
	for _, p := range group.Peers {
		s, err := c.Status(ctx, p.ID)
		if err != nil {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			continue
		}
		statuses := []raftcluster.RuntimeStatusV1{s.CatalogRaft}
		for _, g := range s.Groups {
			statuses = append(statuses, g.RuntimeStatusV1)
		}
		for _, status := range statuses {
			if status.GroupID == group.ID && status.LeaderID != "" {
				for _, member := range group.Peers {
					if member.ID == status.LeaderID {
						return status.LeaderID, nil
					}
				}
			}
		}
	}
	return "", raftcluster.ErrAdmissionUnavailable
}

func (c *FixedPeerTCPClientV1) Status(ctx context.Context, node raftcluster.NodeID) (FixedPeerTCPStatusV1, error) {
	r, e := c.call(ctx, node, "status", fixedPeerRequestV1{}, false)
	return r.Status, e
}
func (c *FixedPeerTCPClientV1) Route(ctx context.Context, node raftcluster.NodeID, request ClusterRouteRequest) (ClusterRouteTarget, error) {
	r, e := c.call(ctx, node, "route", fixedPeerRequestV1{Route: request}, false)
	return r.Route, e
}
func (c *FixedPeerTCPClientV1) PublishCatalog(ctx context.Context, node raftcluster.NodeID, command []byte) (raftplacement.CatalogMetaStatusV1, error) {
	r, e := c.call(ctx, node, "catalog-publish", fixedPeerRequestV1{Entry: command}, true)
	return r.Catalog, e
}
func (c *FixedPeerTCPClientV1) Submit(ctx context.Context, node raftcluster.NodeID, entry []byte, metadata ClusterRequestMetadata) (raftcluster.SubmitResultV1, error) {
	r, e := c.call(ctx, node, "submit", fixedPeerRequestV1{Entry: entry, Metadata: metadata}, true)
	return r.Submit, e
}

func (c *FixedPeerTCPClientV1) Close() {
	if c != nil {
		c.http.CloseIdleConnections()
	}
}

// Preserve retry/admission-relevant sentinels; unclassified validation failures
// remain definite rejections with their diagnostic message, not retriable codes.
var fixedPeerErrorsV1 = []error{
	raftcluster.ErrCommitAmbiguous, raftcluster.ErrNotLeader, raftcluster.ErrAdmissionUnavailable, raftcluster.ErrHashicorpRaftUnavailable,
	raftcluster.ErrCommitNotProven, raftcluster.ErrLocalApplyNotRecoverable, raftcluster.ErrUnsupportedSubmitAck,
	raftcluster.ErrMissingCatalogVersion, raftcluster.ErrCatalogVersionMismatch,
	raftcluster.ErrRouteTargetMissing, raftcluster.ErrRouteTargetUnknown, raftcluster.ErrRouteTargetUnsupported, raftcluster.ErrRouteGroupMismatch, raftcluster.ErrRouteFanoutRequired,
	raftcluster.ErrInvalidConfig, raftcluster.ErrUnsupportedFeature,
	raftplacement.ErrCatalogMetaUnavailable, raftplacement.ErrCatalogMetaStaleEpoch, raftplacement.ErrCatalogMetaSkippedEpoch, raftplacement.ErrCatalogMetaConflict,
	raftplacement.ErrCatalogMetaTopologyChange, raftplacement.ErrCatalogMetaDigestMismatch, raftplacement.ErrCatalogMetaProofMissing, raftplacement.ErrCatalogMetaRouteMismatch,
	context.Canceled, context.DeadlineExceeded,
}

func fixedPeerErrorCodeV1(err error) string {
	for _, known := range fixedPeerErrorsV1 {
		if errors.Is(err, known) {
			return known.Error()
		}
	}
	return "rejected"
}

type fixedPeerRemoteErrorV1 struct {
	message, code string
	route         *raftcluster.RouteErrorMetadata
}

func (e *fixedPeerRemoteErrorV1) Error() string { return e.message }
func (e *fixedPeerRemoteErrorV1) Is(target error) bool {
	return target != nil && target.Error() == e.code
}
func (e *fixedPeerRemoteErrorV1) RouteErrorMetadata() raftcluster.RouteErrorMetadata {
	if e.route != nil {
		return *e.route
	}
	return raftcluster.RouteErrorMetadata{}
}
