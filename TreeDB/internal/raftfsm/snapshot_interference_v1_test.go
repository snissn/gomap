package raftfsm

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// Exercise HashiCorp's real runFSM loop: releasing f.mu is insufficient if
// archive streaming still runs synchronously inside FSM.Snapshot.
func TestSnapshotStreamingDoesNotHoldApplyForWholeArchiveV1(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	root := t.TempDir()
	db := openRaftSnapshotFSMTestDBWithOptions(t, filepath.Join(root, "data"), true, true)
	defer db.Close()
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	cfg := raftcluster.Config{Dir: filepath.Join(root, "data"), ClusterDir: filepath.Join(root, "raft"), NodeID: "node-a", GroupID: "default", DisableSideStores: true, Peers: []raftcluster.Peer{{ID: "node-a", Address: "node-a"}}}
	fsm, err := Open(Options{DB: db, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()
	rc := hraft.DefaultConfig()
	rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout = 50*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond
	rc.LogOutput = io.Discard
	provider, err := raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{Cluster: cfg, Applier: fsm, Transport: transport, RaftConfig: rc, Bootstrap: true})
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
	commit := func(ctx context.Context, raw []byte) error {
		_, err := provider.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{NodeID: cfg.NodeID, GroupID: cfg.GroupID, EntryBytes: raw, CurrentCatalogVersion: testCatalogVersionStart, HasCurrentCatalogVersion: true, SyncLocalCommandWAL: true})
		return err
	}
	if err := commit(ctx, deterministicCreateCollectionEntry(t, "users", "snapshot-interference-create")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock() // Always unblock archive before provider/FSM teardown.
	raftSnapshotBeforeCopyForTest = func(context.Context) { enterOnce.Do(func() { close(entered) }); <-release }
	defer func() { raftSnapshotBeforeCopyForTest = nil }()
	snapshotDone := make(chan error, 1)
	go func() { _, err := provider.Snapshot(ctx); snapshotDone <- err }()
	select {
	case <-entered:
	case err := <-snapshotDone:
		t.Fatalf("snapshot completed before streaming barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	admission := make(chan error, 1)
	go func() {
		captured, err := fsm.CaptureRaftSnapshotV1()
		if err == nil {
			_ = captured.Release()
		}
		admission <- err
	}()
	select {
	case err := <-admission:
		if err == nil {
			t.Fatal("live materializer admitted a second capture")
		}
	case <-time.After(time.Second):
		unblock()
		t.Fatal("live capture admission waited behind materializer")
	}
	tail := deterministicInsertBatchEntry(t, "users", "snapshot-interference-tail", nativewire.DocumentFormatJSON, [][]byte{[]byte("tail")}, [][]byte{[]byte(`{"_id":"tail","value":1}`)})
	applyCtx, applyCancel := context.WithTimeout(ctx, time.Second)
	err = commit(applyCtx, tail)
	applyCancel()
	unblock()
	if snapshotErr := <-snapshotDone; snapshotErr != nil {
		t.Fatalf("snapshot: %v", snapshotErr)
	}
	if err != nil {
		t.Fatalf("committed apply did not progress while archive streaming was stalled: %v", err)
	}
	assertSnapshotDocument(t, fsm, "tail", []byte(`{"_id":"tail","value":1}`))
}
