package raftcluster

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

type replacementReadyProgressApplierV1 struct{ beforeReport func() }

func (*replacementReadyProgressApplierV1) ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}, nil
}

func (a *replacementReadyProgressApplierV1) AppliedProgress(context.Context) (AppliedProgress, error) {
	if a.beforeReport != nil {
		a.beforeReport()
	}
	return AppliedProgress{NodeID: "node-a", GroupID: "group-a", Term: 1, Index: 1, HasApplied: true}, nil
}

func TestReplacementSnapshotReadyWaitsForFSMConfigurationV1(t *testing.T) {
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	config := hraft.DefaultConfig()
	config.HeartbeatTimeout = 100 * time.Millisecond
	config.ElectionTimeout = 100 * time.Millisecond
	config.LeaderLeaseTimeout = 100 * time.Millisecond
	config.LogOutput = io.Discard
	applier := &replacementReadyProgressApplierV1{}
	provider, err := OpenHashicorpRaftProvider(HashicorpRaftProviderOptions{
		Cluster: Config{Dir: filepath.Join(t.TempDir(), "db"), NodeID: "node-a", GroupID: "group-a", Peers: []Peer{{ID: "node-a", Address: "node-a"}}},
		Applier: applier, Transport: transport, RaftConfig: config,
		LogStore: hraft.NewInmemStore(), StableStore: hraft.NewInmemStore(), SnapshotStore: hraft.NewInmemSnapshotStore(), Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		status, err := provider.RuntimeStatusV1(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.State == "Leader" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	committed, err := provider.CommittedConfigurationV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for provider.raft.AppliedIndex() < committed.ConfigurationIndex {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if installed, _ := provider.raft.InstalledSnapshotBoundary(); installed >= committed.ConfigurationIndex {
		t.Fatalf("fixture has installed snapshot %d at/after configuration %d", installed, committed.ConfigurationIndex)
	}
	// Model the queue-send window documented by Raft's AppliedIndex API: the
	// native applied index is ahead, but StoreConfiguration has not returned.
	provider.fsmConfigIndex.Store(0)
	waitCtx, waitCancel := context.WithCancel(ctx)
	applier.beforeReport = waitCancel
	if err := provider.ReplacementSnapshotReadyV1(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("queue-send index was accepted before FSM consumption: %v", err)
	}
	applier.beforeReport = nil
	provider.fsmConfigIndex.Store(committed.ConfigurationIndex)
	if err := provider.ReplacementSnapshotReadyV1(ctx); err != nil {
		t.Fatalf("FSM-consumed configuration was refused: %v", err)
	}
}
