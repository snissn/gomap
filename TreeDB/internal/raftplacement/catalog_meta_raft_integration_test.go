package raftplacement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestCatalogMetaBackupRestoresFreshThreeAuthorityClusterAndSurvivesReopenFailoverRejoin(t *testing.T) {
	source := newRealCatalogMetaClusterV1(t, "source")
	sourceLeader := source.waitLeader(t)
	command1 := mustCatalogMetaCommand(t, 0, 1, realCatalogMetaIntegrationCatalogV1("group-a"))
	if _, _, err := source.providers[sourceLeader].SubmitCatalogMetaCommandV1(context.Background(), command1); err != nil {
		t.Fatalf("source epoch 1: %v", err)
	}
	source.waitEpoch(t, 1)
	firstStatus, ok := source.authorities[sourceLeader].Status()
	if !ok {
		t.Fatal("source leader status unavailable")
	}

	// An exact command retry is idempotent even though Raft commits another
	// entry; the catalog's original applied index remains authoritative.
	if _, _, err := source.providers[sourceLeader].SubmitCatalogMetaCommandV1(context.Background(), command1); err != nil {
		t.Fatalf("source exact retry: %v", err)
	}
	source.waitEpoch(t, 1)
	retryStatus, _ := source.authorities[sourceLeader].Status()
	if retryStatus.AppliedIndex != firstStatus.AppliedIndex {
		t.Fatalf("exact retry applied index=%d want original %d", retryStatus.AppliedIndex, firstStatus.AppliedIndex)
	}

	stale := mustCatalogMetaCommand(t, 0, 2, realCatalogMetaIntegrationCatalogV1("group-b"))
	if _, _, err := source.providers[sourceLeader].SubmitCatalogMetaCommandV1(context.Background(), stale); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale source transition error=%v want ErrCatalogMetaStaleEpoch", err)
	}
	source.assertEpochAndRoute(t, 1, "group-a")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backup, err := source.providers[sourceLeader].ExportCatalogMetaBackupV1(ctx)
	if err != nil {
		t.Fatalf("export source backup: %v", err)
	}
	if len(backup) <= 64 {
		t.Fatalf("backup size=%d want payload", len(backup))
	}

	target := newRealCatalogMetaClusterV1(t, "target")
	targetLeader := target.waitLeader(t)
	follower := target.anyFollower(targetLeader)
	target.waitFollower(t, follower)
	if err := target.providers[follower].RestoreCatalogMetaBackupV1(ctx, backup); !errors.Is(err, raftcluster.ErrNotLeader) {
		t.Fatalf("follower restore error=%v want ErrNotLeader", err)
	}
	for id, authority := range target.authorities {
		if _, ok := authority.Status(); ok {
			t.Fatalf("%s published catalog after follower restore refusal", id)
		}
	}
	corrupt := slices.Clone(backup)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := target.providers[targetLeader].RestoreCatalogMetaBackupV1(ctx, corrupt); !errors.Is(err, raftcluster.ErrInvalidCatalogMetaBackup) {
		t.Fatalf("corrupt restore error=%v want ErrInvalidCatalogMetaBackup", err)
	}
	if err := target.providers[targetLeader].RestoreCatalogMetaBackupV1(ctx, backup); err != nil {
		t.Fatalf("restore fresh target leader: %v", err)
	}
	target.waitEpoch(t, 1)
	target.assertEpochAndRoute(t, 1, "group-a")

	// The restored last command retains exact-retry semantics in the new
	// cluster, including its original applied-index identity.
	restoredStatus, _ := target.authorities[targetLeader].Status()
	if _, _, err := target.providers[targetLeader].SubmitCatalogMetaCommandV1(ctx, command1); err != nil {
		t.Fatalf("restored exact retry: %v", err)
	}
	target.waitEpoch(t, 1)
	afterRetry, _ := target.authorities[targetLeader].Status()
	if afterRetry.AppliedIndex != restoredStatus.AppliedIndex {
		t.Fatalf("restored exact retry applied index=%d want %d", afterRetry.AppliedIndex, restoredStatus.AppliedIndex)
	}

	// Reopen a restored follower from its persisted Raft snapshot.
	target.closeNode(t, follower)
	target.reopenNode(t, follower)
	target.connectAll()
	target.waitEpochOn(t, follower, 1)
	target.assertAuthorityIdentityAndRoute(t, follower, 1, "group-a")

	// Fail over and prove that an epoch-valid owner move still fails closed:
	// M4A has no migration workflow that could transfer the collection data,
	// apply progress, and idempotency state before publishing the new route.
	target.closeNode(t, targetLeader)
	newLeader := target.waitLeader(t)
	unsafeCommand2 := mustCatalogMetaCommand(t, 1, 2, realCatalogMetaIntegrationCatalogV1("group-b"))
	if _, _, err := target.providers[newLeader].SubmitCatalogMetaCommandV1(ctx, unsafeCommand2); !errors.Is(err, ErrCatalogMetaTopologyChange) {
		t.Fatalf("target owner move error=%v want ErrCatalogMetaTopologyChange", err)
	}
	target.assertEpochAndRoute(t, 1, "group-a")

	// The refused log entry does not prevent a safe metadata generation from
	// advancing monotonically, and the old backup still cannot roll it back.
	command2 := mustCatalogMetaCommand(t, 1, 2, realCatalogMetaIntegrationCatalogV1("group-a"))
	if _, _, err := target.providers[newLeader].SubmitCatalogMetaCommandV1(ctx, command2); err != nil {
		t.Fatalf("target epoch 2 after failover: %v", err)
	}
	target.waitEpoch(t, 2)
	target.assertEpochAndRoute(t, 2, "group-a")
	if err := target.providers[newLeader].RestoreCatalogMetaBackupV1(ctx, backup); !errors.Is(err, raftcluster.ErrCatalogMetaBackupRestoreTarget) || !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("live rollback restore error=%v want restore-target conflict", err)
	}
	target.assertEpochAndRoute(t, 2, "group-a")

	// Rejoin the former leader with a new real authority and prove it receives
	// the newer snapshot/log state and exact identity.
	target.reopenNode(t, targetLeader)
	target.connectAll()
	target.waitEpochOn(t, targetLeader, 2)
	target.assertEpochAndRoute(t, 2, "group-a")
}

type realCatalogMetaClusterV1 struct {
	peers       []raftcluster.Peer
	configs     map[raftcluster.NodeID]raftcluster.Config
	transports  map[raftcluster.NodeID]*hraft.InmemTransport
	providers   map[raftcluster.NodeID]*raftcluster.CatalogMetaRaftProviderV1
	authorities map[raftcluster.NodeID]*CatalogMetaAuthorityV1
}

func newRealCatalogMetaClusterV1(t *testing.T, prefix string) *realCatalogMetaClusterV1 {
	t.Helper()
	features := raftcluster.FeatureSet{
		ConfigVersion: raftcluster.SupportedConfigVersion,
		Required: []raftcluster.RequiredFeature{
			{Name: raftcluster.FeatureSingleGroupProvider, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureSingleGroupProvider]},
			{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
		},
	}
	cluster := &realCatalogMetaClusterV1{
		configs:     make(map[raftcluster.NodeID]raftcluster.Config, 3),
		transports:  make(map[raftcluster.NodeID]*hraft.InmemTransport, 3),
		providers:   make(map[raftcluster.NodeID]*raftcluster.CatalogMetaRaftProviderV1, 3),
		authorities: make(map[raftcluster.NodeID]*CatalogMetaAuthorityV1, 3),
	}
	root := t.TempDir()
	for _, suffix := range []string{"a", "b", "c"} {
		id := raftcluster.NodeID(prefix + "-" + suffix)
		cluster.peers = append(cluster.peers, raftcluster.Peer{
			ID:           id,
			Address:      string(id),
			Capabilities: features,
		})
		_, transport := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(id), 2*time.Second)
		cluster.transports[id] = transport
	}
	cluster.connectAll()
	for _, peer := range cluster.peers {
		cluster.configs[peer.ID] = raftcluster.Config{
			Dir:      filepath.Join(root, string(peer.ID), "db"),
			NodeID:   peer.ID,
			GroupID:  "catalog-meta",
			Peers:    slices.Clone(cluster.peers),
			Features: features,
		}
		cluster.reopenNode(t, peer.ID)
	}
	t.Cleanup(func() {
		for id, provider := range cluster.providers {
			if provider != nil {
				_ = provider.Close()
				delete(cluster.providers, id)
			}
		}
		for _, transport := range cluster.transports {
			_ = transport.Close()
		}
	})
	return cluster
}

func (c *realCatalogMetaClusterV1) reopenNode(t *testing.T, id raftcluster.NodeID) {
	t.Helper()
	if c.providers[id] != nil {
		t.Fatalf("%s provider is already open", id)
	}
	authority := NewCatalogMetaAuthorityV1()
	provider, err := raftcluster.OpenCatalogMetaRaftProviderV1(raftcluster.CatalogMetaRaftProviderOptionsV1{
		Cluster:      c.configs[id],
		State:        authority,
		Transport:    c.transports[id],
		RaftConfig:   realCatalogMetaRaftConfigV1(),
		Bootstrap:    true,
		ApplyTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("%s reopen catalog provider: %v", id, err)
	}
	c.authorities[id] = authority
	c.providers[id] = provider
}

func (c *realCatalogMetaClusterV1) closeNode(t *testing.T, id raftcluster.NodeID) {
	t.Helper()
	provider := c.providers[id]
	if provider == nil {
		t.Fatalf("%s provider is not open", id)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("%s close catalog provider: %v", id, err)
	}
	delete(c.providers, id)
	c.transports[id].DisconnectAll()
	for peerID, transport := range c.transports {
		if peerID != id {
			transport.Disconnect(hraft.ServerAddress(id))
		}
	}
}

func (c *realCatalogMetaClusterV1) connectAll() {
	for _, from := range c.peers {
		for _, to := range c.peers {
			if from.ID != to.ID {
				c.transports[from.ID].Connect(hraft.ServerAddress(to.Address), c.transports[to.ID])
			}
		}
	}
}

func (c *realCatalogMetaClusterV1) waitLeader(t *testing.T) raftcluster.NodeID {
	t.Helper()
	var leader raftcluster.NodeID
	waitRealCatalogMetaConditionV1(t, func() bool {
		leader = ""
		for id, provider := range c.providers {
			status, err := provider.ClusterAdmissionStatus(context.Background())
			if err == nil && status.Leader {
				if leader != "" {
					return false
				}
				leader = id
			}
		}
		return leader != ""
	}, "catalog meta cluster has no unique leader")
	return leader
}

func (c *realCatalogMetaClusterV1) anyFollower(leader raftcluster.NodeID) raftcluster.NodeID {
	for id := range c.providers {
		if id != leader {
			return id
		}
	}
	return ""
}

func (c *realCatalogMetaClusterV1) waitFollower(t *testing.T, id raftcluster.NodeID) {
	t.Helper()
	waitRealCatalogMetaConditionV1(t, func() bool {
		status, err := c.providers[id].ClusterAdmissionStatus(context.Background())
		return err == nil && !status.Leader && !status.Unavailable && status.LeaderHint != ""
	}, fmt.Sprintf("%s did not observe the catalog leader", id))
}

func (c *realCatalogMetaClusterV1) waitEpoch(t *testing.T, epoch uint64) {
	t.Helper()
	waitRealCatalogMetaConditionV1(t, func() bool {
		for id := range c.providers {
			status, ok := c.authorities[id].Status()
			if !ok || status.Epoch != epoch {
				return false
			}
		}
		return true
	}, fmt.Sprintf("catalog authorities did not converge to epoch %d", epoch))
}

func (c *realCatalogMetaClusterV1) waitEpochOn(t *testing.T, id raftcluster.NodeID, epoch uint64) {
	t.Helper()
	waitRealCatalogMetaConditionV1(t, func() bool {
		status, ok := c.authorities[id].Status()
		return ok && status.Epoch == epoch
	}, fmt.Sprintf("%s did not converge to epoch %d", id, epoch))
}

func (c *realCatalogMetaClusterV1) assertEpochAndRoute(t *testing.T, epoch uint64, groupID raftcluster.GroupID) {
	t.Helper()
	for id := range c.providers {
		c.assertAuthorityIdentityAndRoute(t, id, epoch, groupID)
	}
}

func (c *realCatalogMetaClusterV1) assertAuthorityIdentityAndRoute(t *testing.T, id raftcluster.NodeID, epoch uint64, groupID raftcluster.GroupID) {
	t.Helper()
	authority := c.authorities[id]
	status, ok := authority.Status()
	if !ok || status.Epoch != epoch || len(status.Digest) != 64 {
		t.Fatalf("%s status=%+v available=%v want epoch %d and digest", id, status, ok, epoch)
	}
	if status.Features.ConfigVersion != SupportedCatalogVersion ||
		len(status.Features.Required) != 1 ||
		status.Features.Required[0].Name != FeatureCollectionGroups ||
		status.Features.Required[0].Version != SupportedFeatureFloors[FeatureCollectionGroups] {
		t.Fatalf("%s catalog features=%+v want collection-groups v1", id, status.Features)
	}
	proof := CatalogProofV1{Epoch: status.Epoch, Digest: status.Digest}
	decision, err := authority.Route(context.Background(), proof, RouteRequestV1{
		Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
		Shape:      RouteShapeCollectionV1,
	})
	if err != nil {
		t.Fatalf("%s route users: %v", id, err)
	}
	if decision.GroupID() != groupID {
		t.Fatalf("%s route group=%q want %q", id, decision.GroupID(), groupID)
	}
	wantMembers := []raftcluster.NodeID{"node-a", "node-c"}
	wantLeader := raftcluster.NodeID("node-a")
	if groupID == "group-b" {
		wantMembers = []raftcluster.NodeID{"node-b", "node-c"}
		wantLeader = "node-b"
	}
	if !slices.Equal(decision.Group.Members, wantMembers) || decision.LeaderHint() != wantLeader {
		t.Fatalf("%s route identity=%+v want members %v leader %s", id, decision, wantMembers, wantLeader)
	}
}

func realCatalogMetaIntegrationCatalogV1(usersGroup raftcluster.GroupID) CatalogV1 {
	catalog := validCatalog()
	catalog.Features = DefaultFeatureSet()
	catalog.Placements[0].GroupID = usersGroup
	catalog.Placements[0].Mode = PlacementModeCollectionV1
	return catalog
}

func realCatalogMetaRaftConfigV1() *hraft.Config {
	config := hraft.DefaultConfig()
	config.HeartbeatTimeout = 300 * time.Millisecond
	config.ElectionTimeout = 300 * time.Millisecond
	config.LeaderLeaseTimeout = 300 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond
	config.SnapshotInterval = time.Hour
	config.SnapshotThreshold = ^uint64(0)
	config.TrailingLogs = 0
	config.LogOutput = io.Discard
	config.LogLevel = "ERROR"
	config.NoLegacyTelemetry = true
	return config
}

func waitRealCatalogMetaConditionV1(t *testing.T, ready func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(message)
}
