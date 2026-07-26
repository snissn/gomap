package raftcluster

// This bounded harness is for integration/benchmark topology construction. It
// runs actual HashiCorp Raft state machines, but deliberately leaves payload
// construction to its caller so it does not become an application command API.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const ThreeNodeHarnessCoordinationTimeout = 5 * time.Second

type ThreeNodeHarness struct {
	root, groupID string
	nodes         []*threeNodeHarnessNode
	leader        *threeNodeHarnessNode
	coordinator   *GroupRoutedReadIndexCoordinator
}

type threeNodeHarnessNode struct {
	id        NodeID
	provider  *HashicorpRaftProvider
	applier   *threeNodeHarnessProgressApplier
	transport *hraft.InmemTransport
}

type threeNodeHarnessProgressApplier struct {
	nodeID   NodeID
	groupID  GroupID
	mu       sync.Mutex
	progress AppliedProgress
	notify   chan struct{}
}

func newThreeNodeHarnessProgressApplier(nodeID NodeID, groupID GroupID) *threeNodeHarnessProgressApplier {
	return &threeNodeHarnessProgressApplier{nodeID: nodeID, groupID: groupID, progress: AppliedProgress{NodeID: nodeID, GroupID: groupID}, notify: make(chan struct{})}
}
func (*threeNodeHarnessProgressApplier) AllowsInitialIndexGapV1() bool { return true }
func (a *threeNodeHarnessProgressApplier) ApplyCommittedCommandEntryV1(ctx context.Context, entry CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	if err := ctx.Err(); err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if entry.Term == 0 || entry.Index == 0 || len(entry.Bytes) == 0 {
		return raftentry.ApplyResultV1{}, ErrInvalidCommittedEntry
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.progress.HasApplied && entry.Index <= a.progress.Index {
		return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusAlreadyApplied}, nil
	}
	a.progress = AppliedProgress{NodeID: a.nodeID, GroupID: a.groupID, Term: entry.Term, Index: entry.Index, HasApplied: true}
	close(a.notify)
	a.notify = make(chan struct{})
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}, nil
}
func (a *threeNodeHarnessProgressApplier) AppliedProgress(ctx context.Context) (AppliedProgress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return AppliedProgress{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.progress, nil
}
func (a *threeNodeHarnessProgressApplier) WaitAppliedIndex(ctx context.Context, barrier AppliedIndexReadBarrier) (AppliedProgress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		a.mu.Lock()
		progress, notify := a.progress, a.notify
		a.mu.Unlock()
		if err := barrier.Check(progress); err == nil {
			return progress, nil
		} else if errors.Is(err, ErrReadBarrierTargetMismatch) {
			return progress, err
		}
		select {
		case <-ctx.Done():
			return progress, ctx.Err()
		case <-notify:
		}
	}
}

func OpenThreeNodeHarness(ctx context.Context, groupID GroupID) (*ThreeNodeHarness, error) {
	if groupID == "" {
		return nil, errors.New("three-node Raft harness group is empty")
	}
	root, err := os.MkdirTemp("", "treedb-three-node-raft-*")
	if err != nil {
		return nil, err
	}
	h := &ThreeNodeHarness{root: root, groupID: string(groupID)}
	fail := func(err error) (*ThreeNodeHarness, error) { _ = h.Close(); return nil, err }
	peers := []Peer{{ID: "node-a", Address: "node-a"}, {ID: "node-b", Address: "node-b"}, {ID: "node-c", Address: "node-c"}}
	transports := map[NodeID]*hraft.InmemTransport{}
	for _, peer := range peers {
		_, tr := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(peer.Address), ThreeNodeHarnessCoordinationTimeout)
		transports[peer.ID] = tr
	}
	for _, from := range peers {
		for _, to := range peers {
			transports[from.ID].Connect(hraft.ServerAddress(to.Address), transports[to.ID])
		}
	}
	for _, peer := range peers {
		applier := newThreeNodeHarnessProgressApplier(peer.ID, groupID)
		provider, openErr := OpenHashicorpRaftProvider(HashicorpRaftProviderOptions{Cluster: Config{Dir: filepath.Join(root, string(peer.ID), "db"), ClusterDir: filepath.Join(root, string(peer.ID), "raft"), NodeID: peer.ID, GroupID: groupID, Peers: peers}, Applier: applier, Transport: transports[peer.ID], RaftConfig: threeNodeHarnessRaftConfig(), LogStore: hraft.NewInmemStore(), StableStore: hraft.NewInmemStore(), SnapshotStore: hraft.NewInmemSnapshotStore(), Bootstrap: true, ApplyTimeout: ThreeNodeHarnessCoordinationTimeout})
		if openErr != nil {
			return fail(fmt.Errorf("open Raft node %s: %w", peer.ID, openErr))
		}
		h.nodes = append(h.nodes, &threeNodeHarnessNode{id: peer.ID, provider: provider, applier: applier, transport: transports[peer.ID]})
	}
	if err := transports["node-b"].TimeoutNow(hraft.ServerID("node-a"), hraft.ServerAddress("node-a"), &hraft.TimeoutNowRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax, ID: []byte("node-b"), Addr: []byte("node-b")}}, &hraft.TimeoutNowResponse{}); err != nil {
		return fail(fmt.Errorf("start Raft election: %w", err))
	}
	leader, err := h.waitLeader(ctx)
	if err != nil {
		return fail(err)
	}
	h.leader = leader
	h.coordinator, err = NewGroupRoutedReadIndexCoordinator([]GroupReadIndexCoordinatorV1{{GroupID: groupID, NodeID: leader.id, ReadIndexProvider: leader.provider, AppliedIndexWaiter: leader.applier}})
	if err != nil {
		return fail(err)
	}
	return h, nil
}

func threeNodeHarnessRaftConfig() *hraft.Config {
	cfg := hraft.DefaultConfig()
	cfg.HeartbeatTimeout = ThreeNodeHarnessCoordinationTimeout
	cfg.ElectionTimeout = ThreeNodeHarnessCoordinationTimeout
	cfg.LeaderLeaseTimeout = ThreeNodeHarnessCoordinationTimeout
	cfg.CommitTimeout = 10 * time.Millisecond
	cfg.SnapshotInterval = time.Hour
	cfg.SnapshotThreshold = ^uint64(0)
	cfg.TrailingLogs = 0
	cfg.LogOutput = io.Discard
	cfg.LogLevel = "ERROR"
	cfg.NoLegacyTelemetry = true
	return cfg
}
func (h *ThreeNodeHarness) waitLeader(ctx context.Context) (*threeNodeHarnessNode, error) {
	var prior NodeID
	var since time.Time
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var leader *threeNodeHarnessNode
		for _, node := range h.nodes {
			status, err := node.provider.ClusterAdmissionStatus(ctx)
			if err != nil {
				return nil, err
			}
			if status.Leader {
				if leader != nil {
					leader = nil
					break
				}
				leader = node
			}
		}
		now := time.Now()
		if leader == nil {
			prior = ""
		} else if prior != leader.id {
			prior, since = leader.id, now
		} else if now.Sub(since) >= time.Second {
			return leader, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for stable Raft leader: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
func (h *ThreeNodeHarness) GroupID() GroupID {
	if h == nil {
		return ""
	}
	return GroupID(h.groupID)
}
func (h *ThreeNodeHarness) LeaderID() NodeID {
	if h == nil || h.leader == nil {
		return ""
	}
	return h.leader.id
}

// NodeIDs returns the fixed three-member identity set for topology evidence.
func (h *ThreeNodeHarness) NodeIDs() []NodeID {
	if h == nil {
		return nil
	}
	out := make([]NodeID, 0, len(h.nodes))
	for _, node := range h.nodes {
		out = append(out, node.id)
	}
	return out
}
func (h *ThreeNodeHarness) ReadCoordinator() RoutedReadIndexCoordinator {
	if h == nil {
		return nil
	}
	return h.coordinator
}
func (h *ThreeNodeHarness) CommitAndProve(ctx context.Context, entry []byte) (CommitCommandEntryV1Result, error) {
	if h == nil || h.leader == nil {
		return CommitCommandEntryV1Result{}, errors.New("three-node Raft harness leader unavailable")
	}
	result, err := h.leader.provider.CommitCommandEntryV1(ctx, CommitCommandEntryV1Request{GroupID: h.GroupID(), NodeID: h.leader.id, EntryBytes: entry})
	if err != nil {
		return CommitCommandEntryV1Result{}, err
	}
	if !result.Evidence.ProvesProductionConsensus() {
		return CommitCommandEntryV1Result{}, errors.New("Raft commit lacks production consensus evidence")
	}
	for _, node := range h.nodes {
		if _, err := node.applier.WaitAppliedIndex(ctx, AppliedIndexReadBarrier{NodeID: node.id, GroupID: h.GroupID(), MinAppliedIndex: result.Entry.Index}); err != nil {
			return CommitCommandEntryV1Result{}, err
		}
	}
	return result, nil
}
func (h *ThreeNodeHarness) Close() error {
	if h == nil {
		return nil
	}
	var errs []error
	for _, node := range h.nodes {
		if node.provider != nil {
			errs = append(errs, node.provider.Close())
			node.provider = nil
		}
	}
	for _, node := range h.nodes {
		if node.transport != nil {
			errs = append(errs, node.transport.Close())
			node.transport = nil
		}
	}
	h.nodes = nil
	if h.root != "" {
		errs = append(errs, os.RemoveAll(h.root))
		h.root = ""
	}
	return errors.Join(errs...)
}
