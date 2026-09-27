package raftfsm

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func snapshotOwnerProviderForTest(t *testing.T, store hraft.SnapshotStore) (*FSM, *raftcluster.HashicorpRaftProvider, Options) {
	t.Helper()
	root := t.TempDir()
	database := openRaftSnapshotFSMTestDBWithOptions(t, filepath.Join(root, "data"), true, true)
	t.Cleanup(func() { _ = database.Close() })
	_, transport := hraft.NewInmemTransport("node-a")
	t.Cleanup(func() { _ = transport.Close() })
	cfg := raftcluster.Config{Dir: filepath.Join(root, "data"), ClusterDir: filepath.Join(root, "raft"), NodeID: "node-a", GroupID: "default", DisableSideStores: true, Peers: []raftcluster.Peer{{ID: "node-a", Address: "node-a"}}}
	opts := Options{DB: database, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}}
	fsm, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsm.Close() })
	rc := hraft.DefaultConfig()
	rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout = 50*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond
	rc.LogOutput = io.Discard
	provider, err := raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{Cluster: cfg, Applier: fsm, Transport: transport, RaftConfig: rc, Bootstrap: true, SnapshotStore: store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	_, err = provider.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{NodeID: cfg.NodeID, GroupID: cfg.GroupID, EntryBytes: deterministicCreateCollectionEntry(t, "users", "snapshot-owner-create"), CurrentCatalogVersion: testCatalogVersionStart, HasCurrentCatalogVersion: true, SyncLocalCommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	return fsm, provider, opts
}

func TestCapturedRaftSnapshotV1ProviderCleanupFailureRemainsRetryable(t *testing.T) {
	source, provider, opts := snapshotOwnerProviderForTest(t, nil)
	denied := errors.New("injected stage cleanup failure")
	var fail atomic.Bool
	fail.Store(true)
	raftSnapshotBeforeCleanupForTest = func(string) error {
		if fail.Load() {
			return denied
		}
		return nil
	}
	defer func() { raftSnapshotBeforeCleanupForTest = nil }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// HashiCorp v1.7.3 snapshot.go formats Persist errors with %v. The
	// provider preserves its own unavailable sentinel, but not this cause.
	if _, err := provider.Snapshot(ctx); !errors.Is(err, raftcluster.ErrHashicorpRaftUnavailable) || !strings.Contains(err.Error(), "failed to persist snapshot: "+denied.Error()) {
		t.Fatalf("snapshot cleanup failure: %v", err)
	}
	// HashiCorp has a void Release API: the FSM must still own the failed work.
	if !source.snapshotOperationActive.Load() {
		t.Fatal("provider discarded cleanup admission")
	}
	if _, err := source.CaptureRaftSnapshotV1(); !errors.Is(err, denied) {
		t.Fatal("retry lost cleanup error", err)
	}
	if err := source.Close(); !errors.Is(err, denied) {
		t.Fatal("Close lost cleanup error", err)
	}
	if reopened, err := Open(opts); err == nil {
		reopened.Close()
		t.Fatal("reopen bypassed failed cleanup")
	}
	fail.Store(false)
	if err := source.Close(); err != nil {
		t.Fatal("repeated Close failed cleanup retry", err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
}

type blockedOwnerSnapshotStoreV1 struct {
	hraft.SnapshotStore
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (s *blockedOwnerSnapshotStoreV1) Create(version hraft.SnapshotVersion, index, term uint64, configuration hraft.Configuration, configurationIndex uint64, transport hraft.Transport) (hraft.SnapshotSink, error) {
	sink, err := s.SnapshotStore.Create(version, index, term, configuration, configurationIndex, transport)
	if err != nil {
		return nil, err
	}
	return &blockedOwnerSnapshotSinkV1{SnapshotSink: sink, store: s}, nil
}

type blockedOwnerSnapshotSinkV1 struct {
	hraft.SnapshotSink
	store *blockedOwnerSnapshotStoreV1
}

func (s *blockedOwnerSnapshotSinkV1) Write(p []byte) (int, error) {
	s.store.once.Do(func() { close(s.store.entered); <-s.store.resume })
	return s.SnapshotSink.Write(p)
}

func TestCapturedRaftSnapshotV1ProviderBlockedSinkCloseReopen(t *testing.T) {
	fileStore, err := hraft.NewFileSnapshotStore(t.TempDir(), 2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	store := &blockedOwnerSnapshotStoreV1{SnapshotStore: fileStore, entered: make(chan struct{}), resume: make(chan struct{})}
	source, provider, opts := snapshotOwnerProviderForTest(t, store)
	var once sync.Once
	resume := func() { once.Do(func() { close(store.resume) }) }
	defer resume()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := provider.Snapshot(ctx); done <- err }()
	select {
	case <-store.entered:
	case err := <-done:
		t.Fatal("snapshot did not block", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	source.snapshotMu.Lock()
	owner := source.snapshotOwner
	source.snapshotMu.Unlock()
	ready, err := owner.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- source.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited on blocked sink")
	}
	if reopened, err := Open(opts); err == nil {
		reopened.Close()
		t.Fatal("reopen admitted while sink retained archive")
	}
	if _, err := os.Stat(ready.ArchivePath); err != nil {
		t.Fatal("active sink lost archive", err)
	}
	resume()
	select {
	case err := <-done:
		if !errors.Is(err, raftcluster.ErrHashicorpRaftUnavailable) || !strings.Contains(err.Error(), "failed to persist snapshot: "+context.Canceled.Error()) {
			t.Fatal("sink failed to observe Close", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Snapshot future completion precedes its deferred Release in HashiCorp.
	for source.snapshotOperationActive.Load() && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if source.snapshotOperationActive.Load() {
		t.Fatal("provider retained completed cleanup")
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
}
