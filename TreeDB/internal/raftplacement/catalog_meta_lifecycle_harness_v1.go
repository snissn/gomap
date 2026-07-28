package raftplacement

// CatalogMetaLifecycleHarnessV1 is a bounded integration/benchmark topology
// helper. It is intentionally not a deployment manager: it uses in-memory
// HashiCorp Raft transports, owns all temporary state, and exposes only the
// leader-facing M7 seams needed by a local multi-group topology.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

var catalogMetaLifecycleHarnessSequenceV1 atomic.Uint64

const catalogMetaLifecycleHarnessCoordinationTimeoutV1 = 5 * time.Second
const catalogMetaLifecycleHarnessLeaderDwellV1 = catalogMetaLifecycleHarnessCoordinationTimeoutV1

type CatalogMetaLifecycleHarnessOptionsV1 struct {
	Catalog CatalogV1
	Prefix  string
}

type CatalogMetaLifecycleHarnessV1 struct {
	root        string
	groupID     raftcluster.GroupID
	peers       []raftcluster.Peer
	transports  map[raftcluster.NodeID]*hraft.InmemTransport
	providers   map[raftcluster.NodeID]*raftcluster.CatalogMetaRaftProviderV1
	authorities map[raftcluster.NodeID]*CatalogMetaAuthorityV1
	leader      raftcluster.NodeID
}

func OpenCatalogMetaLifecycleHarnessV1(ctx context.Context, opts CatalogMetaLifecycleHarnessOptionsV1) (*CatalogMetaLifecycleHarnessV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	features := catalogMetaLifecycleHarnessFeaturesV1()
	if !catalogMetaLifecycleHarnessHasFeatureV1(opts.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
		return nil, errors.New("catalog lifecycle harness requires M7 lifecycle feature")
	}
	if _, err := Validate(opts.Catalog); err != nil {
		return nil, fmt.Errorf("catalog lifecycle harness catalog: %w", err)
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = fmt.Sprintf("m8-meta-%d", catalogMetaLifecycleHarnessSequenceV1.Add(1))
	}
	root, err := os.MkdirTemp("", "treedb-catalog-meta-harness-*")
	if err != nil {
		return nil, err
	}
	h := &CatalogMetaLifecycleHarnessV1{root: root, groupID: "catalog-meta", transports: map[raftcluster.NodeID]*hraft.InmemTransport{}, providers: map[raftcluster.NodeID]*raftcluster.CatalogMetaRaftProviderV1{}, authorities: map[raftcluster.NodeID]*CatalogMetaAuthorityV1{}}
	fail := func(err error) (*CatalogMetaLifecycleHarnessV1, error) { _ = h.Close(); return nil, err }
	for _, suffix := range []string{"a", "b", "c"} {
		id := raftcluster.NodeID(prefix + "-" + suffix)
		h.peers = append(h.peers, raftcluster.Peer{ID: id, Address: string(id), Capabilities: features})
		_, tr := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(id), catalogMetaLifecycleHarnessCoordinationTimeoutV1)
		h.transports[id] = tr
	}
	for _, from := range h.peers {
		for _, to := range h.peers {
			if from.ID != to.ID {
				h.transports[from.ID].Connect(hraft.ServerAddress(to.Address), h.transports[to.ID])
			}
		}
	}
	for _, peer := range h.peers {
		authority := NewCatalogMetaAuthorityV1()
		provider, openErr := raftcluster.OpenCatalogMetaRaftProviderV1(raftcluster.CatalogMetaRaftProviderOptionsV1{Cluster: raftcluster.Config{Dir: filepath.Join(root, string(peer.ID), "db"), NodeID: peer.ID, GroupID: h.groupID, Peers: slices.Clone(h.peers), Features: features}, State: authority, Transport: h.transports[peer.ID], RaftConfig: catalogMetaLifecycleHarnessRaftConfigV1(), Bootstrap: true, ApplyTimeout: 3 * time.Second})
		if openErr != nil {
			return fail(fmt.Errorf("open catalog-meta node %s: %w", peer.ID, openErr))
		}
		h.authorities[peer.ID] = authority
		h.providers[peer.ID] = provider
	}
	if err := h.waitLeader(ctx); err != nil {
		return fail(err)
	}
	record, err := NewCatalogMetaRecordV1(1, opts.Catalog)
	if err != nil {
		return fail(err)
	}
	raw, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		return fail(err)
	}
	if _, _, err = h.providers[h.leader].SubmitCatalogMetaCommandV1(ctx, raw); err != nil {
		return fail(fmt.Errorf("install catalog meta record: %w", err))
	}
	if err := h.waitAuthorities(ctx, func(a *CatalogMetaAuthorityV1) bool { status, ok := a.Status(); return ok && status.Epoch == 1 }); err != nil {
		return fail(err)
	}
	return h, nil
}

func catalogMetaLifecycleHarnessFeaturesV1() raftcluster.FeatureSet {
	return raftcluster.FeatureSet{ConfigVersion: raftcluster.SupportedConfigVersion, Required: []raftcluster.RequiredFeature{{Name: raftcluster.FeatureSingleGroupProvider, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureSingleGroupProvider]}, {Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]}, {Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]}}}
}
func catalogMetaLifecycleHarnessRaftConfigV1() *hraft.Config {
	cfg := hraft.DefaultConfig()
	cfg.HeartbeatTimeout = catalogMetaLifecycleHarnessCoordinationTimeoutV1
	cfg.ElectionTimeout = catalogMetaLifecycleHarnessCoordinationTimeoutV1
	cfg.LeaderLeaseTimeout = catalogMetaLifecycleHarnessCoordinationTimeoutV1
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.SnapshotInterval = time.Hour
	cfg.SnapshotThreshold = ^uint64(0)
	cfg.TrailingLogs = 0
	cfg.LogOutput = io.Discard
	cfg.LogLevel = "ERROR"
	cfg.NoLegacyTelemetry = true
	return cfg
}
func (h *CatalogMetaLifecycleHarnessV1) waitLeader(ctx context.Context) error {
	var prior raftcluster.NodeID
	var since time.Time
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var leader raftcluster.NodeID
		for id, p := range h.providers {
			status, err := p.ClusterAdmissionStatus(ctx)
			if err != nil {
				continue
			}
			if status.Leader {
				if leader != "" {
					leader = ""
					break
				}
				leader = id
			}
		}
		now := time.Now()
		if leader == "" {
			prior = ""
		} else if prior != leader {
			prior, since = leader, now
		} else if now.Sub(since) >= catalogMetaLifecycleHarnessLeaderDwellV1 {
			h.leader = leader
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait catalog-meta leader: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
func (h *CatalogMetaLifecycleHarnessV1) waitAuthorities(ctx context.Context, ready func(*CatalogMetaAuthorityV1) bool) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		all := true
		for _, a := range h.authorities {
			if !ready(a) {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait catalog-meta authority convergence: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
func (h *CatalogMetaLifecycleHarnessV1) GroupID() raftcluster.GroupID {
	if h == nil {
		return ""
	}
	return h.groupID
}
func (h *CatalogMetaLifecycleHarnessV1) NodeIDs() []raftcluster.NodeID {
	if h == nil {
		return nil
	}
	out := make([]raftcluster.NodeID, 0, len(h.peers))
	for _, p := range h.peers {
		out = append(out, p.ID)
	}
	return out
}
func (h *CatalogMetaLifecycleHarnessV1) LeaderID() raftcluster.NodeID {
	if h == nil {
		return ""
	}
	return h.leader
}
func (h *CatalogMetaLifecycleHarnessV1) LeaderAuthority() *CatalogMetaAuthorityV1 {
	if h == nil {
		return nil
	}
	return h.authorities[h.leader]
}
func (h *CatalogMetaLifecycleHarnessV1) LeaderFence() *raftcluster.CatalogMetaRaftProviderV1 {
	if h == nil {
		return nil
	}
	return h.providers[h.leader]
}

func catalogMetaLifecycleHarnessHasFeatureV1(features raftcluster.FeatureSet, want raftcluster.FeatureName) bool {
	for _, feature := range features.Required {
		if feature.Name == want {
			return true
		}
	}
	return false
}
func (h *CatalogMetaLifecycleHarnessV1) LifecycleCoordinator() VectorPartitionLifecycleCoordinatorV1 {
	return VectorPartitionLifecycleCoordinatorV1{Authority: h.LeaderAuthority(), Committer: h.providers[h.leader]}
}
func (h *CatalogMetaLifecycleHarnessV1) Authorities() map[raftcluster.NodeID]*CatalogMetaAuthorityV1 {
	out := map[raftcluster.NodeID]*CatalogMetaAuthorityV1{}
	if h != nil {
		for id, a := range h.authorities {
			out[id] = a
		}
	}
	return out
}
func (h *CatalogMetaLifecycleHarnessV1) WaitForAuthorities(ctx context.Context, ready func(*CatalogMetaAuthorityV1) bool) error {
	if h == nil {
		return errors.New("catalog lifecycle harness unavailable")
	}
	return h.waitAuthorities(ctx, ready)
}
func (h *CatalogMetaLifecycleHarnessV1) Close() error {
	if h == nil {
		return nil
	}
	var errs []error
	for id, p := range h.providers {
		if p != nil {
			errs = append(errs, p.Close())
			delete(h.providers, id)
		}
	}
	for id, tr := range h.transports {
		if tr != nil {
			errs = append(errs, tr.Close())
			delete(h.transports, id)
		}
	}
	if h.root != "" {
		errs = append(errs, os.RemoveAll(h.root))
		h.root = ""
	}
	return errors.Join(errs...)
}
