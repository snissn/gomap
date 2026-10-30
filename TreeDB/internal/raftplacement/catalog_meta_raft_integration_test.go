package raftplacement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestCatalogMetaBackupRestoresFreshThreeAuthorityClusterAndSurvivesReopenFailoverRejoin(t *testing.T) {
	progress := catalogMetaLeaderWaitProgressV1{started: time.Now()}
	var source, target *realCatalogMetaClusterV1
	ctx, cancel := context.WithTimeout(context.Background(), realCatalogMetaIntegrationTestTimeoutV1)
	defer cancel()
	emitStacks := sync.OnceFunc(func() {
		stack := make([]byte, 64<<10)
		n := runtime.Stack(stack, true)
		t.Logf("catalog meta failure stacks: bytes=%d possibly_truncated=%t\n%s", n, n == len(stack), stack[:n])
	})
	// Observe the original deadline even if a native Open/Restore/Close is
	// blocked outside the polling helper. The witness takes no producer lock.
	stopStacks, stacksDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stacksDone)
		select {
		case <-ctx.Done():
			emitStacks()
		case <-stopStacks:
		}
	}()
	defer func() { close(stopStacks); <-stacksDone }()
	// Emit the bounded receipts at test exit so a passing diagnostic also
	// records phase budgets without logging between readiness observations.
	defer func() {
		t.Logf("catalog meta leader waits: total_elapsed=%s completed=%+v phases=%+v", time.Since(progress.started), progress.completed[:progress.count], progress.phases[:progress.phaseCount])
		if t.Failed() {
			// Capture before t.Cleanup shuts down Raft and erases blocked paths.
			emitStacks()
			for _, cluster := range []*realCatalogMetaClusterV1{source, target} {
				if cluster != nil {
					cluster.logRaftFailure(t)
				}
			}
		}
	}()
	progress.mark(ctx, "source-open")
	source = newRealCatalogMetaClusterV1(t, "source")
	sourceLeader := source.waitLeader(t, ctx, &progress, "source-initial")
	progress.mark(ctx, "source-epoch-1")
	command1 := mustCatalogMetaCommand(t, 0, 1, realCatalogMetaIntegrationCatalogV1("group-a"))
	if _, _, err := source.providers[sourceLeader].SubmitCatalogMetaCommandV1(ctx, command1); err != nil {
		t.Fatalf("source epoch 1: %v", err)
	}
	source.waitEpoch(t, ctx, 1)
	firstStatus, ok := source.authorities[sourceLeader].Status()
	if !ok {
		t.Fatal("source leader status unavailable")
	}

	// An exact command retry is idempotent even though Raft commits another
	// entry; the catalog's original applied index remains authoritative.
	progress.mark(ctx, "source-exact-retry")
	if _, _, err := source.providers[sourceLeader].SubmitCatalogMetaCommandV1(ctx, command1); err != nil {
		t.Fatalf("source exact retry: %v", err)
	}
	source.waitEpoch(t, ctx, 1)
	retryStatus, _ := source.authorities[sourceLeader].Status()
	if retryStatus.AppliedIndex != firstStatus.AppliedIndex {
		t.Fatalf("exact retry applied index=%d want original %d", retryStatus.AppliedIndex, firstStatus.AppliedIndex)
	}

	progress.mark(ctx, "source-stale-refusal")
	stale := mustCatalogMetaCommand(t, 0, 2, realCatalogMetaIntegrationCatalogV1("group-b"))
	if _, _, err := source.providers[sourceLeader].SubmitCatalogMetaCommandV1(ctx, stale); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale source transition error=%v want ErrCatalogMetaStaleEpoch", err)
	}
	source.assertEpochAndRoute(t, ctx, 1, "group-a")
	progress.mark(ctx, "source-export")
	backup, err := source.providers[sourceLeader].ExportCatalogMetaBackupV1(ctx)
	if err != nil {
		t.Fatalf("export source backup: %v", err)
	}
	if len(backup) <= 64 {
		t.Fatalf("backup size=%d want payload", len(backup))
	}

	progress.mark(ctx, "target-open")
	target = newRealCatalogMetaClusterV1(t, "target")
	targetLeader := target.waitLeader(t, ctx, &progress, "target-initial")
	follower := target.anyFollower(targetLeader)
	target.waitFollower(t, ctx, follower)
	progress.mark(ctx, "target-follower-refusal")
	if err := target.providers[follower].RestoreCatalogMetaBackupV1(ctx, backup); !errors.Is(err, raftcluster.ErrNotLeader) {
		t.Fatalf("follower restore error=%v want ErrNotLeader", err)
	}
	for id, authority := range target.authorities {
		if _, ok := authority.Status(); ok {
			t.Fatalf("%s published catalog after follower restore refusal", id)
		}
	}
	progress.mark(ctx, "target-corrupt-refusal")
	corrupt := slices.Clone(backup)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := target.providers[targetLeader].RestoreCatalogMetaBackupV1(ctx, corrupt); !errors.Is(err, raftcluster.ErrInvalidCatalogMetaBackup) {
		t.Fatalf("corrupt restore error=%v want ErrInvalidCatalogMetaBackup", err)
	}
	// The invalid archive is rejected before Raft admission. Reacquire a
	// stable admission-ready leader before exercising the valid restore so a
	// hosted-runner scheduling stall cannot turn harness readiness into the
	// production ErrAdmissionUnavailable path under test.
	targetLeader = target.waitLeader(t, ctx, &progress, "target-before-restore")
	follower = target.anyFollower(targetLeader)
	target.waitFollower(t, ctx, follower)
	progress.mark(ctx, "target-restore")
	if err := target.providers[targetLeader].RestoreCatalogMetaBackupV1(ctx, backup); err != nil {
		t.Fatalf("restore fresh target leader: %v", err)
	}
	target.waitEpoch(t, ctx, 1)
	target.assertEpochAndRoute(t, ctx, 1, "group-a")

	// The restored last command retains exact-retry semantics in the new
	// cluster, including its original applied-index identity.
	progress.mark(ctx, "target-exact-retry")
	restoredStatus, _ := target.authorities[targetLeader].Status()
	if _, _, err := target.providers[targetLeader].SubmitCatalogMetaCommandV1(ctx, command1); err != nil {
		t.Fatalf("restored exact retry: %v", err)
	}
	target.waitEpoch(t, ctx, 1)
	afterRetry, _ := target.authorities[targetLeader].Status()
	if afterRetry.AppliedIndex != restoredStatus.AppliedIndex {
		t.Fatalf("restored exact retry applied index=%d want %d", afterRetry.AppliedIndex, restoredStatus.AppliedIndex)
	}

	// Reopen a restored follower from its persisted Raft snapshot.
	progress.mark(ctx, "target-follower-close")
	target.closeNode(t, follower)
	progress.mark(ctx, "target-follower-reopen")
	target.reopenNode(t, follower)
	target.connectAll()
	progress.mark(ctx, "target-follower-converge")
	target.waitEpochOn(t, ctx, follower, 1)
	target.assertAuthorityIdentityAndRoute(t, ctx, follower, 1, "group-a")

	// Fail over and prove that an epoch-valid owner move still fails closed:
	// M4A has no migration workflow that could transfer the collection data,
	// apply progress, and idempotency state before publishing the new route.
	progress.mark(ctx, "target-leader-close")
	target.closeNode(t, targetLeader)
	newLeader := target.waitLeader(t, ctx, &progress, "target-after-failover")
	progress.mark(ctx, "target-topology-refusal")
	unsafeCommand2 := mustCatalogMetaCommand(t, 1, 2, realCatalogMetaIntegrationCatalogV1("group-b"))
	if _, _, err := target.providers[newLeader].SubmitCatalogMetaCommandV1(ctx, unsafeCommand2); !errors.Is(err, ErrCatalogMetaTopologyChange) {
		t.Fatalf("target owner move error=%v want ErrCatalogMetaTopologyChange", err)
	}
	target.assertEpochAndRoute(t, ctx, 1, "group-a")

	// The refused log entry does not prevent a safe metadata generation from
	// advancing monotonically, and the old backup still cannot roll it back.
	newLeader = target.waitLeader(t, ctx, &progress, "target-after-topology-refusal")
	progress.mark(ctx, "target-epoch-2")
	command2 := mustCatalogMetaCommand(t, 1, 2, realCatalogMetaIntegrationCatalogV1("group-a"))
	if _, _, err := target.providers[newLeader].SubmitCatalogMetaCommandV1(ctx, command2); err != nil {
		t.Fatalf("target epoch 2 after failover: %v", err)
	}
	target.waitEpoch(t, ctx, 2)
	target.assertEpochAndRoute(t, ctx, 2, "group-a")
	progress.mark(ctx, "target-rollback-refusal")
	if err := target.providers[newLeader].RestoreCatalogMetaBackupV1(ctx, backup); !errors.Is(err, raftcluster.ErrCatalogMetaBackupRestoreTarget) || !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("live rollback restore error=%v want restore-target conflict", err)
	}
	target.assertEpochAndRoute(t, ctx, 2, "group-a")

	// Rejoin the former leader with a new real authority and prove it receives
	// the newer snapshot/log state and exact identity.
	progress.mark(ctx, "target-leader-rejoin")
	target.reopenNode(t, targetLeader)
	target.connectAll()
	progress.mark(ctx, "target-rejoin-converge")
	target.waitEpochOn(t, ctx, targetLeader, 2)
	target.assertEpochAndRoute(t, ctx, 2, "group-a")
	progress.mark(ctx, "complete")
}

const realCatalogMetaIntegrationCoordinationTimeoutV1 = 5 * time.Second
const realCatalogMetaIntegrationLeaderDwellV1 = realCatalogMetaIntegrationCoordinationTimeoutV1
const realCatalogMetaIntegrationTestTimeoutV1 = 4*realCatalogMetaIntegrationCoordinationTimeoutV1 + 60*time.Second

type realCatalogMetaClusterV1 struct {
	peers       []raftcluster.Peer
	configs     map[raftcluster.NodeID]raftcluster.Config
	transports  map[raftcluster.NodeID]*hraft.InmemTransport
	providers   map[raftcluster.NodeID]*raftcluster.CatalogMetaRaftProviderV1
	authorities map[raftcluster.NodeID]*CatalogMetaAuthorityV1
	logs        map[raftcluster.NodeID]*catalogMetaRaftLogTailV1
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
		logs:        make(map[raftcluster.NodeID]*catalogMetaRaftLogTailV1, 3),
	}
	defer func() {
		if t.Failed() {
			cluster.logRaftFailure(t)
		}
	}()
	root := t.TempDir()
	for _, suffix := range []string{"a", "b", "c"} {
		id := raftcluster.NodeID(prefix + "-" + suffix)
		cluster.peers = append(cluster.peers, raftcluster.Peer{
			ID:           id,
			Address:      string(id),
			Capabilities: features,
		})
		_, transport := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(id), realCatalogMetaIntegrationCoordinationTimeoutV1)
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
	if c.logs[id] == nil {
		c.logs[id] = &catalogMetaRaftLogTailV1{}
	}
	config := realCatalogMetaRaftConfigV1()
	// Keep native election/lease/storage errors in fixed memory. No RPC or
	// readiness call is added; output is copied only after an actual failure.
	config.LogOutput, config.LogLevel = c.logs[id], "INFO"
	provider, err := raftcluster.OpenCatalogMetaRaftProviderV1(raftcluster.CatalogMetaRaftProviderOptionsV1{
		Cluster:      c.configs[id],
		State:        authority,
		Transport:    c.transports[id],
		RaftConfig:   config,
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

// Diagnostic receipts retain the five existing waits and fixed lifecycle
// boundaries. They do not
// extend the shared context, retry commands or alter the dwell discriminator.
// Status is the provider's local Raft-state observation, not a quorum proof.
type catalogMetaLeaderWaitProgressV1 struct {
	started    time.Time
	completed  [5]catalogMetaLeaderWaitReceiptV1
	count      int
	phases     [32]catalogMetaPhaseReceiptV1
	phaseCount int
}

type catalogMetaPhaseReceiptV1 struct {
	Phase                  string
	Entered, BudgetAtEntry time.Duration
}

func (p *catalogMetaLeaderWaitProgressV1) mark(ctx context.Context, phase string) {
	if p.phaseCount == len(p.phases) {
		panic("catalog meta phase receipt capacity exceeded")
	}
	now := time.Now()
	r := catalogMetaPhaseReceiptV1{Phase: phase, Entered: now.Sub(p.started)}
	if deadline, ok := ctx.Deadline(); ok {
		r.BudgetAtEntry = deadline.Sub(now)
	}
	p.phases[p.phaseCount] = r
	p.phaseCount++
}

type catalogMetaLeaderProbeV1 struct {
	ID     raftcluster.NodeID
	Status raftcluster.AdmissionStatus
	Err    error
}
type catalogMetaLeaderWaitReceiptV1 struct {
	Phase                                                                                   string
	Entered, BudgetAtEntry, Elapsed, MaxProbeGap                                            time.Duration
	Polls, ProbeErrors, NoUniqueLeader, NoLeader, MultipleLeaders, GapResets, LeaderChanges uint64
	LastComplete                                                                            bool
	LastLeader                                                                              raftcluster.NodeID
	DwellLeader                                                                             raftcluster.NodeID
	DwellElapsed                                                                            time.Duration
	LastProbes                                                                              [3]catalogMetaLeaderProbeV1
	ProbeCount                                                                              int
}

func (c *realCatalogMetaClusterV1) waitLeader(t *testing.T, ctx context.Context, progress *catalogMetaLeaderWaitProgressV1, phase string) raftcluster.NodeID {
	t.Helper()
	var dwell catalogMetaLeaderDwellV1
	progress.mark(ctx, "wait-"+phase)
	entered := time.Now()
	receipt := catalogMetaLeaderWaitReceiptV1{Phase: phase, Entered: entered.Sub(progress.started)}
	if deadline, ok := ctx.Deadline(); ok {
		receipt.BudgetAtEntry = deadline.Sub(entered)
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var leader raftcluster.NodeID
		multiple := false
		complete := true
		receipt.LastProbes = [3]catalogMetaLeaderProbeV1{}
		receipt.ProbeCount = 0
		for id, provider := range c.providers {
			status, err := provider.ClusterAdmissionStatus(ctx)
			if receipt.ProbeCount < len(receipt.LastProbes) {
				receipt.LastProbes[receipt.ProbeCount] = catalogMetaLeaderProbeV1{ID: id, Status: status, Err: err}
				receipt.ProbeCount++
			}
			if err != nil {
				receipt.ProbeErrors++
				complete = false
				break
			}
			if status.Leader {
				if leader != "" {
					multiple = true
					leader = ""
					break
				}
				leader = id
			}
		}
		now := time.Now()
		receipt.Polls++
		receipt.Elapsed = now.Sub(entered)
		receipt.LastComplete, receipt.LastLeader = complete, leader
		if !complete || leader == "" {
			receipt.NoUniqueLeader++
		}
		if multiple {
			receipt.MultipleLeaders++
		} else if complete && leader == "" {
			receipt.NoLeader++
		}
		if !dwell.lastObservation.IsZero() {
			gap := now.Sub(dwell.lastObservation)
			receipt.MaxProbeGap = max(receipt.MaxProbeGap, gap)
			if complete && leader != "" && gap > catalogMetaLeaderObservationMaxGapV1 {
				receipt.GapResets++
			}
		}
		if complete && leader != "" && leader != dwell.leader {
			receipt.LeaderChanges++
		}
		ready := dwell.Observe(now, complete, leader, realCatalogMetaIntegrationLeaderDwellV1, catalogMetaLeaderObservationMaxGapV1)
		receipt.DwellLeader = dwell.leader
		receipt.DwellElapsed = 0
		if !dwell.since.IsZero() {
			receipt.DwellElapsed = now.Sub(dwell.since)
		}
		if ready {
			if progress.count < len(progress.completed) {
				progress.completed[progress.count] = receipt
				progress.count++
			}
			return leader
		}
		select {
		case <-ctx.Done():
			t.Fatalf("catalog meta cluster has no admission-ready leader stable for %s: %v; total_elapsed=%s current=%+v prior_completed=%+v", realCatalogMetaIntegrationLeaderDwellV1, ctx.Err(), time.Since(progress.started), receipt, progress.completed[:progress.count])
		case <-tick.C:
		}
	}
}

func (c *realCatalogMetaClusterV1) anyFollower(leader raftcluster.NodeID) raftcluster.NodeID {
	for id := range c.providers {
		if id != leader {
			return id
		}
	}
	return ""
}

func (c *realCatalogMetaClusterV1) waitFollower(t *testing.T, ctx context.Context, id raftcluster.NodeID) {
	t.Helper()
	waitRealCatalogMetaConditionV1(t, ctx, func() bool {
		status, err := c.providers[id].ClusterAdmissionStatus(ctx)
		return err == nil && !status.Leader && !status.Unavailable && status.LeaderHint != ""
	}, fmt.Sprintf("%s did not observe the catalog leader", id))
}

func (c *realCatalogMetaClusterV1) waitEpoch(t *testing.T, ctx context.Context, epoch uint64) {
	t.Helper()
	waitRealCatalogMetaConditionV1(t, ctx, func() bool {
		for id := range c.providers {
			status, ok := c.authorities[id].Status()
			if !ok || status.Epoch != epoch {
				return false
			}
		}
		return true
	}, fmt.Sprintf("catalog authorities did not converge to epoch %d", epoch))
}

func (c *realCatalogMetaClusterV1) waitEpochOn(t *testing.T, ctx context.Context, id raftcluster.NodeID, epoch uint64) {
	t.Helper()
	waitRealCatalogMetaConditionV1(t, ctx, func() bool {
		status, ok := c.authorities[id].Status()
		return ok && status.Epoch == epoch
	}, fmt.Sprintf("%s did not converge to epoch %d", id, epoch))
}

func (c *realCatalogMetaClusterV1) assertEpochAndRoute(t *testing.T, ctx context.Context, epoch uint64, groupID raftcluster.GroupID) {
	t.Helper()
	for id := range c.providers {
		c.assertAuthorityIdentityAndRoute(t, ctx, id, epoch, groupID)
	}
}

func (c *realCatalogMetaClusterV1) assertAuthorityIdentityAndRoute(t *testing.T, ctx context.Context, id raftcluster.NodeID, epoch uint64, groupID raftcluster.GroupID) {
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
	decision, err := authority.Route(ctx, proof, RouteRequestV1{
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
	// These are scheduling headroom for contended hosted Windows runners, not
	// a production semantic change. The readiness barrier above still proves a
	// continuously stable leader before an operation under test is attempted.
	config.HeartbeatTimeout = realCatalogMetaIntegrationCoordinationTimeoutV1
	config.ElectionTimeout = realCatalogMetaIntegrationCoordinationTimeoutV1
	config.LeaderLeaseTimeout = realCatalogMetaIntegrationCoordinationTimeoutV1
	config.CommitTimeout = 5 * time.Millisecond
	config.SnapshotInterval = time.Hour
	config.SnapshotThreshold = ^uint64(0)
	config.TrailingLogs = 0
	config.LogOutput = io.Discard
	config.LogLevel = "ERROR"
	config.NoLegacyTelemetry = true
	return config
}

func TestRealCatalogMetaRaftConfigAddsSchedulingHeadroom(t *testing.T) {
	config := realCatalogMetaRaftConfigV1()
	const minimum = 5 * time.Second
	if config.HeartbeatTimeout < minimum || config.ElectionTimeout < minimum || config.LeaderLeaseTimeout < minimum {
		t.Fatalf("coordination timeouts heartbeat=%s election=%s lease=%s want each at least %s", config.HeartbeatTimeout, config.ElectionTimeout, config.LeaderLeaseTimeout, minimum)
	}
	if realCatalogMetaIntegrationLeaderDwellV1 < config.LeaderLeaseTimeout {
		t.Fatalf("leader dwell=%s want at least leader lease=%s", realCatalogMetaIntegrationLeaderDwellV1, config.LeaderLeaseTimeout)
	}
	if catalogMetaLeaderObservationMaxGapV1 >= config.ElectionTimeout || catalogMetaLeaderObservationMaxGapV1 >= config.LeaderLeaseTimeout {
		t.Fatalf("leader observation max gap=%s want below election=%s and lease=%s", catalogMetaLeaderObservationMaxGapV1, config.ElectionTimeout, config.LeaderLeaseTimeout)
	}
}

func waitRealCatalogMetaConditionV1(t *testing.T, ctx context.Context, ready func() bool, message string) {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", message, ctx.Err())
		case <-tick.C:
		}
	}
}

// Each node retains at most 16KiB across reopen. Write never logs, calls Raft,
// or waits on a producer; snapshot copies under only this test-local mutex.
type catalogMetaRaftLogTailV1 struct {
	mu         sync.Mutex
	bytes      [16 << 10]byte
	next, size int
	total      uint64
}

func (b *catalogMetaRaftLogTailV1) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	b.total += uint64(n)
	if len(p) >= len(b.bytes) {
		copy(b.bytes[:], p[len(p)-len(b.bytes):])
		b.next, b.size = 0, len(b.bytes)
		return n, nil
	}
	first := copy(b.bytes[b.next:], p)
	copy(b.bytes[:], p[first:])
	b.next = (b.next + len(p)) % len(b.bytes)
	b.size = min(len(b.bytes), b.size+len(p))
	return n, nil
}

func (b *catalogMetaRaftLogTailV1) snapshot() (string, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, b.size)
	start := (b.next - b.size + len(b.bytes)) % len(b.bytes)
	first := copy(out, b.bytes[start:])
	copy(out[first:], b.bytes[:])
	return string(out), b.total - uint64(b.size)
}

func (c *realCatalogMetaClusterV1) logRaftFailure(t *testing.T) {
	t.Helper()
	for _, peer := range c.peers {
		if logs := c.logs[peer.ID]; logs != nil {
			tail, dropped := logs.snapshot()
			t.Logf("catalog meta native Raft log node=%s dropped_prefix_bytes=%d\n%s", peer.ID, dropped, tail)
		}
	}
}

func TestCatalogMetaRaftLogTailRetainsBoundedSuffixV1(t *testing.T) {
	var logs catalogMetaRaftLogTailV1
	want := make([]byte, len(logs.bytes)+27)
	for i := range want {
		want[i] = byte(i % 251)
	}
	// Exercise both a single oversized write and subsequent wraparound.
	if n, err := logs.Write(want); n != len(want) || err != nil {
		t.Fatalf("oversized write=%d/%v", n, err)
	}
	suffix := []byte("after-reopen")
	if n, err := logs.Write(suffix); n != len(suffix) || err != nil {
		t.Fatalf("wrapped write=%d/%v", n, err)
	}
	want = append(want, suffix...)
	tail, dropped := logs.snapshot()
	if tail != string(want[len(want)-len(logs.bytes):]) || dropped != uint64(len(want)-len(logs.bytes)) {
		t.Fatalf("native log tail bytes=%d dropped=%d", len(tail), dropped)
	}
}
