package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const localRaftCoordinationTimeout = 5 * time.Second

type localProgressApplier struct {
	nodeID  raftcluster.NodeID
	groupID raftcluster.GroupID

	mu       sync.Mutex
	progress raftcluster.AppliedProgress
	notify   chan struct{}
}

func newLocalProgressApplier(nodeID raftcluster.NodeID, groupID raftcluster.GroupID) *localProgressApplier {
	return &localProgressApplier{
		nodeID:  nodeID,
		groupID: groupID,
		progress: raftcluster.AppliedProgress{
			NodeID:  nodeID,
			GroupID: groupID,
		},
		notify: make(chan struct{}),
	}
}

func (a *localProgressApplier) AllowsInitialIndexGapV1() bool {
	return true
}

func (a *localProgressApplier) ApplyCommittedCommandEntryV1(ctx context.Context, entry raftcluster.CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	if err := ctx.Err(); err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if entry.Term == 0 || entry.Index == 0 || len(entry.Bytes) == 0 {
		return raftentry.ApplyResultV1{}, raftcluster.ErrInvalidCommittedEntry
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.progress.HasApplied && entry.Index <= a.progress.Index {
		return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusAlreadyApplied}, nil
	}
	a.progress = raftcluster.AppliedProgress{
		NodeID:     a.nodeID,
		GroupID:    a.groupID,
		Term:       entry.Term,
		Index:      entry.Index,
		HasApplied: true,
	}
	close(a.notify)
	a.notify = make(chan struct{})
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}, nil
}

func (a *localProgressApplier) AppliedProgress(ctx context.Context) (raftcluster.AppliedProgress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return raftcluster.AppliedProgress{}, err
	}
	a.mu.Lock()
	progress := a.progress
	a.mu.Unlock()
	return progress, nil
}

func (a *localProgressApplier) WaitAppliedIndex(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedProgress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		a.mu.Lock()
		progress := a.progress
		notify := a.notify
		a.mu.Unlock()
		if err := barrier.Check(progress); err == nil {
			return progress, nil
		} else if errors.Is(err, raftcluster.ErrReadBarrierTargetMismatch) {
			return progress, err
		}
		select {
		case <-ctx.Done():
			return progress, ctx.Err()
		case <-notify:
		}
	}
}

type localRaftNode struct {
	id        raftcluster.NodeID
	provider  *raftcluster.HashicorpRaftProvider
	applier   *localProgressApplier
	transport *hraft.InmemTransport
}

type localRaftCluster struct {
	root        string
	groupID     raftcluster.GroupID
	nodes       []*localRaftNode
	leader      *localRaftNode
	coordinator *raftcluster.GroupRoutedReadIndexCoordinator
}

func openLocalRaftCluster(ctx context.Context, group string) (*localRaftCluster, error) {
	if group == "" {
		return nil, errors.New("M5 target manifest group is empty")
	}
	root, err := os.MkdirTemp("", "treedb-m5-real-raft-*")
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(root)
		}
	}()
	groupID := raftcluster.GroupID(group)
	peers := []raftcluster.Peer{
		{ID: "node-a", Address: "node-a"},
		{ID: "node-b", Address: "node-b"},
		{ID: "node-c", Address: "node-c"},
	}
	transports := make(map[raftcluster.NodeID]*hraft.InmemTransport, len(peers))
	for _, peer := range peers {
		_, transport := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(peer.Address), localRaftCoordinationTimeout)
		transports[peer.ID] = transport
	}
	closeTransports := true
	defer func() {
		if closeTransports {
			for _, transport := range transports {
				_ = transport.Close()
			}
		}
	}()
	for _, from := range peers {
		for _, to := range peers {
			transports[from.ID].Connect(hraft.ServerAddress(to.Address), transports[to.ID])
		}
	}
	cluster := &localRaftCluster{root: root, groupID: groupID}
	for _, peer := range peers {
		dir := filepath.Join(root, string(peer.ID), "db")
		clusterDir := filepath.Join(root, string(peer.ID), "raft")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			cluster.Close()
			return nil, err
		}
		applier := newLocalProgressApplier(peer.ID, groupID)
		provider, openErr := raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{
			Cluster: raftcluster.Config{
				Dir:        dir,
				ClusterDir: clusterDir,
				NodeID:     peer.ID,
				GroupID:    groupID,
				Peers:      peers,
			},
			Applier:       applier,
			Transport:     transports[peer.ID],
			RaftConfig:    localRaftConfig(),
			LogStore:      hraft.NewInmemStore(),
			StableStore:   hraft.NewInmemStore(),
			SnapshotStore: hraft.NewInmemSnapshotStore(),
			Bootstrap:     true,
			ApplyTimeout:  5 * time.Second,
		})
		if openErr != nil {
			cluster.Close()
			return nil, fmt.Errorf("open Raft node %s: %w", peer.ID, openErr)
		}
		cluster.nodes = append(cluster.nodes, &localRaftNode{
			id:        peer.ID,
			provider:  provider,
			applier:   applier,
			transport: transports[peer.ID],
		})
	}
	if err := transports["node-b"].TimeoutNow(
		hraft.ServerID("node-a"),
		hraft.ServerAddress("node-a"),
		&hraft.TimeoutNowRequest{RPCHeader: hraft.RPCHeader{
			ProtocolVersion: hraft.ProtocolVersionMax,
			ID:              []byte("node-b"),
			Addr:            []byte("node-b"),
		}},
		&hraft.TimeoutNowResponse{},
	); err != nil {
		cluster.Close()
		return nil, fmt.Errorf("start local Raft election: %w", err)
	}
	leader, err := cluster.waitForStableLeader(ctx)
	if err != nil {
		cluster.Close()
		return nil, err
	}
	cluster.leader = leader
	coordinator, err := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{{
		GroupID:            groupID,
		NodeID:             leader.id,
		ReadIndexProvider:  leader.provider,
		AppliedIndexWaiter: leader.applier,
	}})
	if err != nil {
		cluster.Close()
		return nil, err
	}
	cluster.coordinator = coordinator
	cleanup = false
	closeTransports = false
	return cluster, nil
}

func localRaftConfig() *hraft.Config {
	cfg := hraft.DefaultConfig()
	cfg.HeartbeatTimeout = localRaftCoordinationTimeout
	cfg.ElectionTimeout = localRaftCoordinationTimeout
	cfg.LeaderLeaseTimeout = localRaftCoordinationTimeout
	cfg.CommitTimeout = 10 * time.Millisecond
	cfg.SnapshotInterval = time.Hour
	cfg.SnapshotThreshold = ^uint64(0)
	cfg.TrailingLogs = 0
	cfg.LogOutput = io.Discard
	cfg.LogLevel = "ERROR"
	cfg.NoLegacyTelemetry = true
	return cfg
}

func (c *localRaftCluster) waitForStableLeader(ctx context.Context) (*localRaftNode, error) {
	type observation struct {
		id      raftcluster.NodeID
		started time.Time
	}
	var previous observation
	const stableWindow = time.Second
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var leader *localRaftNode
		for _, node := range c.nodes {
			status, err := node.provider.ClusterAdmissionStatus(ctx)
			if err != nil {
				return nil, err
			}
			if !status.Leader {
				continue
			}
			if leader != nil {
				leader = nil
				break
			}
			leader = node
		}
		now := time.Now()
		if leader == nil {
			previous = observation{}
		} else if previous.id != leader.id {
			previous = observation{id: leader.id, started: now}
		} else if now.Sub(previous.started) >= stableWindow {
			return leader, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for stable local Raft leader: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *localRaftCluster) commitProofCommand(ctx context.Context) (raftcluster.CommitCommandEntryV1Result, error) {
	if c == nil || c.leader == nil {
		return raftcluster.CommitCommandEntryV1Result{}, errors.New("local Raft leader is unavailable")
	}
	entry, err := proofCommandEntry()
	if err != nil {
		return raftcluster.CommitCommandEntryV1Result{}, err
	}
	result, err := c.leader.provider.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{
		GroupID:    c.groupID,
		NodeID:     c.leader.id,
		EntryBytes: entry,
	})
	if err != nil {
		return raftcluster.CommitCommandEntryV1Result{}, fmt.Errorf("commit proof command: %w", err)
	}
	if !result.Evidence.ProvesProductionConsensus() || result.Entry.Term == 0 || result.Entry.Index == 0 {
		return raftcluster.CommitCommandEntryV1Result{}, errors.New("proof command did not return production consensus evidence")
	}
	barrier := raftcluster.AppliedIndexReadBarrier{
		GroupID:         c.groupID,
		MinAppliedIndex: result.Entry.Index,
	}
	for _, node := range c.nodes {
		nodeBarrier := barrier
		nodeBarrier.NodeID = node.id
		if _, err := node.applier.WaitAppliedIndex(ctx, nodeBarrier); err != nil {
			return raftcluster.CommitCommandEntryV1Result{}, fmt.Errorf("wait for %s apply: %w", node.id, err)
		}
	}
	return result, nil
}

func proofCommandEntry() ([]byte, error) {
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("m5-real-raft-proof-v1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 1)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "m5_benchmark_proof"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("proof"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"proof":true}`))},
	}
	command, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, err
	}
	return iwire.AppendDeterministicEntry(nil, command)
}

func (c *localRaftCluster) Close() error {
	if c == nil {
		return nil
	}
	var errs []error
	for _, node := range c.nodes {
		if node.provider != nil {
			errs = append(errs, node.provider.Close())
			node.provider = nil
		}
	}
	for _, node := range c.nodes {
		if node.transport != nil {
			errs = append(errs, node.transport.Close())
			node.transport = nil
		}
	}
	c.nodes = nil
	if c.root != "" {
		root := c.root
		c.root = ""
		errs = append(errs, os.RemoveAll(root))
	}
	return errors.Join(errs...)
}

func sortedNodeIDs(nodes []*localRaftNode) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, string(node.id))
	}
	sort.Strings(ids)
	return ids
}
