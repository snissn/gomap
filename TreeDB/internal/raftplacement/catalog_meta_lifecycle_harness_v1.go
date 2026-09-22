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
const catalogMetaLeaderObservationMaxGapV1 = time.Second

// catalogMetaLeaderDwellV1 records continuous observation of one unique
// leader. A long scheduling gap cannot prove that the same node retained
// leadership: it could have stepped down and been re-elected while no probe
// ran. Reset the dwell in that case instead of carrying wall-clock time
// through the gap.
type catalogMetaLeaderDwellV1 struct {
	leader          raftcluster.NodeID
	since           time.Time
	lastObservation time.Time
}

func (d *catalogMetaLeaderDwellV1) Observe(now time.Time, complete bool, leader raftcluster.NodeID, dwell, maxGap time.Duration) bool {
	gapTooLarge := !d.lastObservation.IsZero() && now.Sub(d.lastObservation) > maxGap
	if !complete || leader == "" {
		d.leader = ""
		d.since = time.Time{}
		d.lastObservation = now
		return false
	}
	if leader != d.leader || gapTooLarge {
		d.leader = leader
		d.since = now
		d.lastObservation = now
		return false
	}
	d.lastObservation = now
	return !d.since.IsZero() && now.Sub(d.since) >= dwell
}

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
	h := &CatalogMetaLifecycleHarnessV1{root: root, groupID: "catalog-meta"}
	fail := func(err error) (*CatalogMetaLifecycleHarnessV1, error) { _ = h.Close(); return nil, err }
	for _, suffix := range []string{"a", "b", "c"} {
		id := raftcluster.NodeID(prefix + "-" + suffix)
		h.peers = append(h.peers, raftcluster.Peer{ID: id, Address: string(id), Capabilities: features})
	}
	if err := h.openRuntimeV1(); err != nil {
		return fail(err)
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

func (h *CatalogMetaLifecycleHarnessV1) openRuntimeV1() error {
	h.transports = make(map[raftcluster.NodeID]*hraft.InmemTransport, len(h.peers))
	h.providers = make(map[raftcluster.NodeID]*raftcluster.CatalogMetaRaftProviderV1, len(h.peers))
	h.authorities = make(map[raftcluster.NodeID]*CatalogMetaAuthorityV1, len(h.peers))
	features := catalogMetaLifecycleHarnessFeaturesV1()
	for _, peer := range h.peers {
		_, transport := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(peer.ID), catalogMetaLifecycleHarnessCoordinationTimeoutV1)
		h.transports[peer.ID] = transport
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
		provider, err := raftcluster.OpenCatalogMetaRaftProviderV1(raftcluster.CatalogMetaRaftProviderOptionsV1{Cluster: raftcluster.Config{Dir: filepath.Join(h.root, string(peer.ID), "db"), NodeID: peer.ID, GroupID: h.groupID, Peers: slices.Clone(h.peers), Features: features}, State: authority, Transport: h.transports[peer.ID], RaftConfig: catalogMetaLifecycleHarnessRaftConfigV1(), Bootstrap: true, ApplyTimeout: 3 * time.Second})
		if err != nil {
			_ = h.closeRuntimeV1()
			return fmt.Errorf("open catalog-meta node %s: %w", peer.ID, err)
		}
		h.authorities[peer.ID] = authority
		h.providers[peer.ID] = provider
	}
	return nil
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
	var dwell catalogMetaLeaderDwellV1
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var leader raftcluster.NodeID
		complete := true
		for id, p := range h.providers {
			status, err := p.ClusterAdmissionStatus(ctx)
			if err != nil {
				complete = false
				break
			}
			if status.Leader {
				if leader != "" {
					leader = ""
					break
				}
				leader = id
			}
		}
		if dwell.Observe(time.Now(), complete, leader, catalogMetaLifecycleHarnessLeaderDwellV1, catalogMetaLeaderObservationMaxGapV1) {
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

// RestartV1 closes and reopens every harness node over the same durable Raft
// directories, then waits for a leader and replay through the last committed
// catalog index.
func (h *CatalogMetaLifecycleHarnessV1) RestartV1(ctx context.Context) error {
	if h == nil || h.root == "" || h.LeaderFence() == nil {
		return errors.New("catalog lifecycle harness unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	applied, err := h.LeaderFence().LinearizableCatalogMetaAppliedIndexV1(ctx)
	if err != nil {
		return fmt.Errorf("fence catalog-meta before restart: %w", err)
	}
	if err := h.closeRuntimeV1(); err != nil {
		return fmt.Errorf("close catalog-meta runtime for restart: %w", err)
	}
	if err := h.openRuntimeV1(); err != nil {
		return err
	}
	fail := func(err error) error {
		return errors.Join(err, h.closeRuntimeV1())
	}
	if err := h.waitLeader(ctx); err != nil {
		return fail(err)
	}
	if err := h.waitAuthorities(ctx, func(authority *CatalogMetaAuthorityV1) bool {
		replayed, ok := authority.CatalogMetaAppliedIndexV1()
		return ok && replayed >= applied
	}); err != nil {
		return fail(fmt.Errorf("wait catalog-meta replay through index %d: %w", applied, err))
	}
	return nil
}

func (h *CatalogMetaLifecycleHarnessV1) closeRuntimeV1() error {
	var errs []error
	for _, provider := range h.providers {
		if provider != nil {
			errs = append(errs, provider.Close())
		}
	}
	for _, transport := range h.transports {
		if transport != nil {
			errs = append(errs, transport.Close())
		}
	}
	h.providers = nil
	h.transports = nil
	h.authorities = nil
	h.leader = ""
	return errors.Join(errs...)
}

func (h *CatalogMetaLifecycleHarnessV1) Close() error {
	if h == nil {
		return nil
	}
	err := h.closeRuntimeV1()
	if h.root != "" {
		err = errors.Join(err, os.RemoveAll(h.root))
		h.root = ""
	}
	return err
}
