package raftcluster_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	. "github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

func TestHashicorpRaftProviderThreeNodeLeaderSubmitAppliesFollowers(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)

	entry := testClusterCreateCollectionEntry(t, 7)
	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer leaderCancel()
	result, err := leader.submitter.SubmitCommandEntryV1(leaderCtx, entry, raftentry.RequestMetadataV1{
		RequestID: 701,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("leader SubmitCommandEntryV1: %v", err)
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable {
		t.Fatalf("ack/recoverable=%d/%v want raft_committed/true", result.ActualAck, result.CommittedRecoverable)
	}
	if !result.Evidence.ProvesProductionConsensus() || result.Evidence.Kind != CommitEvidenceProductionConsensusV1 {
		t.Fatalf("evidence=%+v does not prove production consensus", result.Evidence)
	}
	if result.Evidence.LeaderID != leader.id {
		t.Fatalf("evidence leader=%q want %q", result.Evidence.LeaderID, leader.id)
	}
	if result.CommittedEntry.Term == 0 || result.CommittedEntry.Index == 0 {
		t.Fatalf("committed entry id=%d/%d, want non-zero", result.CommittedEntry.Term, result.CommittedEntry.Index)
	}
	if result.ApplyResult.Status != raftentry.ApplyStatusApplied && result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied {
		t.Fatalf("leader apply status=%s want applied or already-applied", result.ApplyResult.Status)
	}

	cluster.waitApplied(t, result.CommittedEntry.EntryID())
	for _, node := range cluster.nodes {
		if _, err := collections.NewCollectionManager(node.db).OpenCollection("users"); err != nil {
			t.Fatalf("%s OpenCollection users after quorum commit: %v", node.id, err)
		}
	}

	follower := cluster.firstFollower(t, leader.id)
	cluster.waitFollowerHint(t, follower, leader.id)
	followerCtx, followerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer followerCancel()
	_, err = follower.submitter.SubmitCommandEntryV1(followerCtx, testClusterCreateCollectionEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 702,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower SubmitCommandEntryV1 err=%v want ErrNotLeader", err)
	}
	if !strings.Contains(err.Error(), "leader_hint="+string(leader.id)) {
		t.Fatalf("follower error=%v missing leader hint %q", err, leader.id)
	}
}

func TestHashicorpRaftProviderReadIndexReturnsProductionProofForLeader(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := leader.submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 703,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1: %v", err)
	}
	cluster.waitApplied(t, result.CommittedEntry.EntryID())

	target := ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"}
	proof, err := leader.provider.ReadIndex(ctx, target)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if proof.NodeID != leader.id ||
		proof.GroupID != "group-a" ||
		proof.EvidenceKind != ReadIndexEvidenceProduction ||
		!proof.HasQuorum ||
		proof.Term == 0 ||
		proof.Index < result.CommittedEntry.Index {
		t.Fatalf("proof=%+v committed=%+v", proof, result.CommittedEntry)
	}
	if err := target.Check(proof); err != nil {
		t.Fatalf("target.Check: %v", err)
	}
	progress, err := leader.fsm.WaitAppliedIndex(ctx, proof.AppliedIndexBarrier())
	if err != nil {
		t.Fatalf("WaitAppliedIndex(%+v): %v progress=%+v", proof.AppliedIndexBarrier(), err, progress)
	}
	if progress.NodeID != leader.id || progress.GroupID != "group-a" || progress.Index < proof.Index || !progress.HasApplied {
		t.Fatalf("progress=%+v proof=%+v", progress, proof)
	}
}

func TestHashicorpRaftProviderReadIndexUsesAppliedProgressIndex(t *testing.T) {
	applier := &progressReportingApplier{
		progress: AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       3,
			Index:      42,
			HasApplied: true,
		},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)
	if leader.id != "node-a" {
		applier.progress.NodeID = leader.id
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	proof, err := leader.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if proof.Index != applier.progress.Index {
		t.Fatalf("proof index=%d want applied progress index %d", proof.Index, applier.progress.Index)
	}
	if !proof.HasQuorum || proof.EvidenceKind != ReadIndexEvidenceProduction {
		t.Fatalf("proof=%+v want production quorum proof", proof)
	}
}

func TestHashicorpRaftProviderReadIndexRejectsFollowerStrongRead(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)
	follower := cluster.firstFollower(t, leader.id)
	cluster.waitFollowerHint(t, follower, leader.id)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := follower.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: follower.id, GroupID: "group-a"})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower ReadIndex err=%v want ErrNotLeader", err)
	}
	if !strings.Contains(err.Error(), "leader_hint="+string(leader.id)) {
		t.Fatalf("follower ReadIndex err=%v missing leader hint %q", err, leader.id)
	}
}

func TestHashicorpRaftProviderReadIndexRejectsTargetMismatch(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)

	tests := []struct {
		name   string
		target ReadIndexBarrier
	}{
		{name: "node", target: ReadIndexBarrier{NodeID: "node-z", GroupID: "group-a"}},
		{name: "group", target: ReadIndexBarrier{NodeID: leader.id, GroupID: "group-b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := leader.provider.ReadIndex(context.Background(), tt.target)
			if !errors.Is(err, ErrReadBarrierTargetMismatch) {
				t.Fatalf("ReadIndex err=%v want ErrReadBarrierTargetMismatch", err)
			}
		})
	}
}

func BenchmarkHashicorpRaftProviderReadIndex(b *testing.B) {
	cluster := newHashicorpRaftDBCluster(b)
	leader := cluster.waitForLeader(b)
	ctx := context.Background()
	result, err := leader.submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntry(b, 7), raftentry.RequestMetadataV1{
		RequestID: 704,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		b.Fatalf("SubmitCommandEntryV1: %v", err)
	}
	cluster.waitApplied(b, result.CommittedEntry.EntryID())

	target := ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"}
	proof, err := leader.provider.ReadIndex(ctx, target)
	if err != nil {
		b.Fatalf("warm ReadIndex: %v", err)
	}
	if proof.Index < result.CommittedEntry.Index {
		b.Fatalf("warm proof=%+v committed=%+v", proof, result.CommittedEntry)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proof, err := leader.provider.ReadIndex(ctx, target)
		if err != nil {
			b.Fatalf("ReadIndex: %v", err)
		}
		if proof.Index < result.CommittedEntry.Index {
			b.Fatalf("proof=%+v committed=%+v", proof, result.CommittedEntry)
		}
	}
}

func TestHashicorpRaftProviderLocalRecoverabilityFailureDoesNotReportCommittedRecoverable(t *testing.T) {
	applyErr := errors.New("local command WAL sync failed")
	failingApplier := CommittedCommandApplierFunc(func(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
		return raftentry.ApplyResultV1{
			Status:                 raftentry.ApplyStatusRecoveryRequired,
			DeterministicErrorCode: raftentry.ErrorUnsafeDurabilityModeV1,
		}, applyErr
	})
	cluster := newHashicorpRaftProviderOnlyCluster(t, failingApplier)
	leader := cluster.waitForLeader(t)
	submitter, err := NewSingleGroupSubmitter(SingleGroupSubmitterOptions{
		Cluster:           leader.cfg,
		AdmissionProvider: leader.provider,
		CommitSource:      leader.provider,
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			return CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                failingApplier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})
	if err != nil {
		t.Fatalf("NewSingleGroupSubmitter: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 801,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if !errors.Is(err, ErrHashicorpRaftLogEntry) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want ErrHashicorpRaftLogEntry", err)
	}
	if result.CommittedRecoverable {
		t.Fatalf("CommittedRecoverable=true after local apply failure")
	}
	if result.Evidence.ProvesProductionConsensus() {
		t.Fatalf("evidence=%+v should not prove the quorum commit after local apply failure", result.Evidence)
	}
	if result.CommittedEntry.Term != 0 || result.CommittedEntry.Index != 0 {
		t.Fatalf("committed entry id=%d/%d, want zero after local apply failure", result.CommittedEntry.Term, result.CommittedEntry.Index)
	}
}

func TestHashicorpRaftProviderDoesNotEnqueueCanceledContext(t *testing.T) {
	applier := &countingClusterApplier{
		result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := leader.provider.CommitCommandEntryV1(ctx, CommitCommandEntryV1Request{
		GroupID:                  "group-a",
		NodeID:                   leader.id,
		EntryBytes:               testClusterCreateCollectionEntry(t, 7),
		CurrentCatalogVersion:    7,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      true,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitCommandEntryV1 err=%v want context.Canceled", err)
	}
	if errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("CommitCommandEntryV1 err=%v should not be ErrCommitAmbiguous before Raft.Apply enqueue", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := applier.count(); got != 0 {
		t.Fatalf("applier calls=%d want 0 for canceled context before enqueue", got)
	}
}

func TestHashicorpRaftProviderEvidenceFailsClosedWhenNotProven(t *testing.T) {
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(_ context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			result := productionCommittedResult(req, 4, 2)
			result.Evidence.ProductionConsensus = false
			return result, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})
	_, err := submitter.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 901,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if !errors.Is(err, ErrCommitNotProven) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want ErrCommitNotProven", err)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("applier calls=%d want 0", got)
	}
}

func TestHashicorpRaftProviderRejectsFSMWithoutInitialIndexGapSupport(t *testing.T) {
	root := t.TempDir()
	cfg := Config{
		Dir:     filepath.Join(root, "db"),
		NodeID:  "node-a",
		GroupID: "group-a",
		Peers: []Peer{
			{ID: "node-a", Address: "node-a"},
		},
	}
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          cfg.Dir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	defer db.Close()
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:      db,
		Cluster: cfg,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	defer fsm.Close()
	_, transport := hraft.NewInmemTransport(hraft.ServerAddress("node-a"))
	defer transport.Close()

	provider, err := OpenHashicorpRaftProvider(HashicorpRaftProviderOptions{
		Cluster:   cfg,
		Applier:   fsm,
		Transport: transport,
	})
	if provider != nil {
		_ = provider.Close()
		t.Fatalf("OpenHashicorpRaftProvider returned provider with invalid FSM gap support")
	}
	if !errors.Is(err, ErrInvalidHashicorpRaftProvider) {
		t.Fatalf("OpenHashicorpRaftProvider err=%v want ErrInvalidHashicorpRaftProvider", err)
	}
	if !strings.Contains(err.Error(), "initial Raft log index gaps") {
		t.Fatalf("OpenHashicorpRaftProvider err=%v missing initial gap reason", err)
	}
}

type hashicorpRaftTestCluster struct {
	nodes map[NodeID]*hashicorpRaftTestNode
}

type hashicorpRaftTestNode struct {
	id        NodeID
	cfg       Config
	db        *backenddb.DB
	fsm       *raftfsm.FSM
	provider  *HashicorpRaftProvider
	submitter *SingleGroupSubmitter
	transport *hraft.InmemTransport
}

func newHashicorpRaftDBCluster(tb testing.TB) *hashicorpRaftTestCluster {
	tb.Helper()
	cluster := newHashicorpRaftCluster(tb, func(tb testing.TB, cfg Config) (*backenddb.DB, CommittedCommandApplierV1, CommandEntryPreflightV1) {
		tb.Helper()
		db, fsm := openHashicorpRaftTestDBAndFSM(tb, cfg)
		return db, fsm, fsm
	})
	for _, node := range cluster.nodes {
		submitter, err := NewSingleGroupSubmitter(SingleGroupSubmitterOptions{
			Cluster:                node.cfg,
			AdmissionProvider:      node.provider,
			CommitSource:           node.provider,
			Preflight:              node.fsm,
			Applier:                node.fsm,
			CatalogVersionProvider: staticCatalogVersion(7),
		})
		if err != nil {
			tb.Fatalf("%s NewSingleGroupSubmitter: %v", node.id, err)
		}
		node.submitter = submitter
	}
	return cluster
}

func newHashicorpRaftProviderOnlyCluster(tb testing.TB, applier CommittedCommandApplierV1) *hashicorpRaftTestCluster {
	tb.Helper()
	return newHashicorpRaftCluster(tb, func(tb testing.TB, _ Config) (*backenddb.DB, CommittedCommandApplierV1, CommandEntryPreflightV1) {
		tb.Helper()
		return nil, applier, nil
	})
}

func newHashicorpRaftCluster(tb testing.TB, nodeStores func(testing.TB, Config) (*backenddb.DB, CommittedCommandApplierV1, CommandEntryPreflightV1)) *hashicorpRaftTestCluster {
	tb.Helper()
	root := tb.TempDir()
	peers := []Peer{
		{ID: "node-a", Address: "node-a"},
		{ID: "node-b", Address: "node-b"},
		{ID: "node-c", Address: "node-c"},
	}
	transports := make(map[NodeID]*hraft.InmemTransport, len(peers))
	for _, peer := range peers {
		_, transport := hraft.NewInmemTransport(hraft.ServerAddress(peer.Address))
		transports[peer.ID] = transport
	}
	for _, from := range peers {
		for _, to := range peers {
			transports[from.ID].Connect(hraft.ServerAddress(to.Address), transports[to.ID])
		}
	}
	cluster := &hashicorpRaftTestCluster{nodes: make(map[NodeID]*hashicorpRaftTestNode, len(peers))}
	for _, peer := range peers {
		cfg := Config{
			Dir:     filepath.Join(root, string(peer.ID), "db"),
			NodeID:  peer.ID,
			GroupID: "group-a",
			Peers:   peers,
		}
		db, applier, _ := nodeStores(tb, cfg)
		var applyFailureHandler func(error)
		if db == nil {
			applyFailureHandler = func(error) {}
		}
		provider, err := OpenHashicorpRaftProvider(HashicorpRaftProviderOptions{
			Cluster:             cfg,
			Applier:             applier,
			Transport:           transports[peer.ID],
			RaftConfig:          hashicorpRaftFastTestConfig(),
			Bootstrap:           true,
			ApplyTimeout:        2 * time.Second,
			ApplyFailureHandler: applyFailureHandler,
		})
		if err != nil {
			tb.Fatalf("%s OpenHashicorpRaftProvider: %v", peer.ID, err)
		}
		node := &hashicorpRaftTestNode{
			id:        peer.ID,
			cfg:       cfg,
			db:        db,
			provider:  provider,
			transport: transports[peer.ID],
		}
		if fsm, ok := applier.(*raftfsm.FSM); ok {
			node.fsm = fsm
		}
		cluster.nodes[peer.ID] = node
	}
	tb.Cleanup(func() {
		for _, node := range cluster.nodes {
			if node.provider != nil {
				_ = node.provider.Close()
			}
		}
		for _, node := range cluster.nodes {
			if node.fsm != nil {
				_ = node.fsm.Close()
			}
			if node.db != nil {
				_ = node.db.Close()
			}
			if node.transport != nil {
				_ = node.transport.Close()
			}
		}
	})
	return cluster
}

func openHashicorpRaftTestDBAndFSM(tb testing.TB, cfg Config) (*backenddb.DB, *raftfsm.FSM) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          cfg.Dir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		tb.Fatalf("%s Open DB: %v", cfg.NodeID, err)
	}
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:      db,
		Cluster: cfg,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync:          true,
			AllowInitialIndexGap: true,
		},
	})
	if err != nil {
		_ = db.Close()
		tb.Fatalf("%s Open FSM: %v", cfg.NodeID, err)
	}
	return db, fsm
}

func hashicorpRaftFastTestConfig() *hraft.Config {
	conf := hraft.DefaultConfig()
	conf.HeartbeatTimeout = 200 * time.Millisecond
	conf.ElectionTimeout = 200 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond
	conf.CommitTimeout = 10 * time.Millisecond
	conf.SnapshotInterval = time.Hour
	conf.SnapshotThreshold = ^uint64(0)
	conf.LogOutput = io.Discard
	conf.LogLevel = "ERROR"
	conf.NoLegacyTelemetry = true
	return conf
}

func (c *hashicorpRaftTestCluster) waitForLeader(tb testing.TB) *hashicorpRaftTestNode {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leader *hashicorpRaftTestNode
		for _, node := range c.nodes {
			status, err := node.provider.ClusterAdmissionStatus(context.Background())
			if err != nil {
				tb.Fatalf("%s ClusterAdmissionStatus: %v", node.id, err)
			}
			if status.Leader {
				if leader != nil {
					tb.Fatalf("multiple leaders: %s and %s", leader.id, node.id)
				}
				leader = node
			}
		}
		if leader != nil {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for leader; states=%s", c.admissionSummary())
	return nil
}

func (c *hashicorpRaftTestCluster) firstFollower(tb testing.TB, leaderID NodeID) *hashicorpRaftTestNode {
	tb.Helper()
	for _, node := range c.nodes {
		if node.id != leaderID {
			return node
		}
	}
	tb.Fatalf("no follower found for leader %q", leaderID)
	return nil
}

func (c *hashicorpRaftTestCluster) waitFollowerHint(tb testing.TB, node *hashicorpRaftTestNode, leaderID NodeID) {
	tb.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := node.provider.ClusterAdmissionStatus(context.Background())
		if err != nil {
			tb.Fatalf("%s ClusterAdmissionStatus: %v", node.id, err)
		}
		if !status.Leader && status.LeaderHint == leaderID {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("%s did not report leader hint %q; states=%s", node.id, leaderID, c.admissionSummary())
}

func (c *hashicorpRaftTestCluster) waitApplied(tb testing.TB, id raftentry.ApplyEntryID) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, node := range c.nodes {
			got, ok := node.fsm.LastApplied()
			if !ok || got.Index < id.Index || got.Term != id.Term {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var states []string
	for _, node := range c.nodes {
		got, ok := node.fsm.LastApplied()
		states = append(states, fmt.Sprintf("%s=%d/%d ok=%t", node.id, got.Term, got.Index, ok))
	}
	tb.Fatalf("timed out waiting for apply %d/%d; applied=%s", id.Term, id.Index, strings.Join(states, ", "))
}

func (c *hashicorpRaftTestCluster) admissionSummary() string {
	var states []string
	for _, node := range c.nodes {
		status, err := node.provider.ClusterAdmissionStatus(context.Background())
		if err != nil {
			states = append(states, fmt.Sprintf("%s err=%v", node.id, err))
			continue
		}
		states = append(states, fmt.Sprintf("%s leader=%t unavailable=%t hint=%q reason=%q", node.id, status.Leader, status.Unavailable, status.LeaderHint, status.Reason))
	}
	return strings.Join(states, "; ")
}

func staticCatalogVersion(version uint64) CatalogVersionProvider {
	return CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) {
		return version, true, nil
	})
}

func newTestSingleGroupSubmitter(tb testing.TB, opts SingleGroupSubmitterOptions) *SingleGroupSubmitter {
	tb.Helper()
	root := tb.TempDir()
	opts.Cluster = Config{
		Dir:     filepath.Join(root, "db"),
		NodeID:  "node-a",
		GroupID: "group-a",
		Peers: []Peer{
			{ID: "node-a", Address: "127.0.0.1:7000"},
			{ID: "node-b", Address: "127.0.0.1:7001"},
			{ID: "node-c", Address: "127.0.0.1:7002"},
		},
	}
	if opts.Preflight == nil {
		opts.Preflight = CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			return CommandEntryPreflightResultV1{}, nil
		})
	}
	submitter, err := NewSingleGroupSubmitter(opts)
	if err != nil {
		tb.Fatalf("NewSingleGroupSubmitter: %v", err)
	}
	return submitter
}

func productionCommittedResult(req CommitCommandEntryV1Request, term, index uint64) CommitCommandEntryV1Result {
	expectedTarget := cloneExpectedTargetForTest(req.ExpectedTarget)
	entry := CommittedCommandEntryV1{
		Term:                     term,
		Index:                    index,
		Bytes:                    bytes.Clone(req.EntryBytes),
		CurrentCatalogVersion:    req.CurrentCatalogVersion,
		HasCurrentCatalogVersion: req.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      req.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataForTest(req.RequestMetadata),
		ExpectedTarget:           expectedTarget,
	}
	return CommitCommandEntryV1Result{
		Entry: entry,
		Evidence: CommitEvidenceV1{
			Kind:                CommitEvidenceProductionConsensusV1,
			GroupID:             req.GroupID,
			NodeID:              req.NodeID,
			LeaderID:            req.NodeID,
			Term:                term,
			Index:               index,
			Committed:           true,
			ProductionConsensus: true,
		},
	}
}

type recordingClusterApplier struct {
	entries []CommittedCommandEntryV1
	result  raftentry.ApplyResultV1
}

func (a *recordingClusterApplier) ApplyCommittedCommandEntryV1(_ context.Context, entry CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	a.entries = append(a.entries, entry.Clone())
	return a.result, nil
}

func (a *recordingClusterApplier) snapshot() []CommittedCommandEntryV1 {
	return append([]CommittedCommandEntryV1(nil), a.entries...)
}

type progressReportingApplier struct {
	progress AppliedProgress
}

func (a *progressReportingApplier) ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}, nil
}

func (a *progressReportingApplier) AppliedProgress(ctx context.Context) (AppliedProgress, error) {
	if err := ctx.Err(); err != nil {
		return a.progress, err
	}
	return a.progress, nil
}

type countingClusterApplier struct {
	mu     sync.Mutex
	result raftentry.ApplyResultV1
	counts int
}

func (a *countingClusterApplier) ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts++
	return a.result, nil
}

func (a *countingClusterApplier) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.counts
}

func testClusterCommandEntry(tb testing.TB, catalogVersion uint64) []byte {
	tb.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("raftcluster/insert/u1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "users"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada"}`))},
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		tb.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		tb.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func testClusterCreateCollectionEntry(tb testing.TB, catalogVersion uint64) []byte {
	tb.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("raftcluster/create/users")},
		{ID: iwire.SectionCollectionMeta, Bytes: testClusterCollectionMetaPayload("users")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		tb.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		tb.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func testClusterCollectionMetaPayload(collection string) []byte {
	dst := binary.AppendUvarint(nil, 1)
	dst = appendTestString(dst, collection)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = append(dst, 0)
	dst = append(dst, 0)
	dst = append(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = append(dst, 0)
	dst = append(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	return dst
}

func appendTestString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func cloneRequestMetadataForTest(meta raftentry.RequestMetadataV1) raftentry.RequestMetadataV1 {
	meta.TraceContext = bytes.Clone(meta.TraceContext)
	meta.ClusterRouteMembers = append([]string(nil), meta.ClusterRouteMembers...)
	return meta
}

func cloneExpectedTargetForTest(target *raftentry.TargetIdentityV1) *raftentry.TargetIdentityV1 {
	if target == nil {
		return nil
	}
	cloned := target.Clone()
	return &cloned
}
