package raftfsm

import (
	"context"
	"errors"
	"io"
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

func TestReplacementSnapshotCancellationOwnsNativeCompletionV1(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	store, err := hraft.NewFileSnapshotStore(t.TempDir(), 2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	blocked := &blockedOwnerSnapshotStoreV1{SnapshotStore: store, entered: make(chan struct{}), resume: make(chan struct{})}
	fsm, provider, _ := snapshotOwnerProviderForTest(t, blocked)
	var once sync.Once
	resume := func() { once.Do(func() { close(blocked.resume) }) }
	defer resume()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := provider.SnapshotForReplacementV1(ctx); done <- err }()
	select {
	case <-blocked.entered:
	case err := <-done:
		t.Fatalf("snapshot returned before native write: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("native write did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("cancellation released outstanding native future: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if !fsm.snapshotOperationActive.Load() {
		t.Fatal("blocked native write lost capture ownership")
	}
	resume()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("completion lost cancellation: %v", err)
		}
		if raftcluster.ReplacementSnapshotCleanupRequiredV1(err) {
			t.Fatal("successful native completion became poisoned cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native completion did not return")
	}
	if fsm.snapshotOperationActive.Load() {
		t.Fatal("native future responded before deferred carrier release")
	}
}

func TestReplacementSnapshotAdmissionHandoffCleanupRetryV1(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	fsm, provider, _ := snapshotOwnerProviderForTest(t, nil)
	denied := errors.New("injected capture cleanup debt")
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
	// Use the ordinary provider call to isolate known FSM cleanup debt. An
	// opaque replacement-native future error is separately restart-required.
	if _, err := provider.Snapshot(ctx); err == nil {
		t.Fatal("cleanup failure accepted")
	}
	var releases atomic.Int32
	release := func() {
		if !fsm.snapshotMu.TryLock() {
			t.Error("admission callback invoked under snapshotMu")
			return
		}
		fsm.snapshotMu.Unlock()
		releases.Add(1)
	}
	if err := fsm.HandoffSnapshotWorkReleaseV1(release); err != nil {
		t.Fatal(err)
	}
	if err := fsm.HandoffSnapshotWorkReleaseV1(func() { t.Error("overwritten callback ran") }); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("occupied callback accepted: %v", err)
	}
	if err := fsm.Close(); !errors.Is(err, denied) || releases.Load() != 0 {
		t.Fatalf("failed cleanup released lease: %v / %d", err, releases.Load())
	}
	fail.Store(false)
	if err := fsm.Close(); err != nil || releases.Load() != 1 {
		t.Fatalf("successful retry did not release once: %v / %d", err, releases.Load())
	}
	if err := fsm.Close(); err != nil || releases.Load() != 1 {
		t.Fatalf("repeat Close released twice: %v / %d", err, releases.Load())
	}
	if err := fsm.HandoffSnapshotWorkReleaseV1(release); err != nil || releases.Load() != 2 {
		t.Fatalf("idle handoff did not release outside lock: %v / %d", err, releases.Load())
	}
}

func TestReplacementSnapshotNativeFailureRequiresRestartV1(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	_, provider, _ := snapshotOwnerProviderForTest(t, nil)
	denied := errors.New("injected native snapshot persistence failure")
	raftSnapshotBeforeCleanupForTest = func(string) error { return denied }
	defer func() { raftSnapshotBeforeCleanupForTest = nil }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := provider.SnapshotForReplacementV1(ctx)
	if !raftcluster.ReplacementSnapshotCleanupRequiredV1(err) || !strings.Contains(err.Error(), denied.Error()) {
		t.Fatalf("native future failure lost unresolved owner: %v", err)
	}
}

func TestReplacementSnapshotConfigOnlyGapBindsNativeAndCommandBoundariesV1(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	fsm, provider, _ := snapshotOwnerProviderForTest(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	initial, err := provider.CommittedConfigurationV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := provider.AddReplacementNonvoterV1(ctx, "node-a", raftcluster.Peer{ID: "node-b", Address: "node-b"}, initial.ConfigurationIndex)
	if err != nil || changed.ConfigurationIndex <= initial.ConfigurationIndex {
		t.Fatalf("committed configuration change: %+v %v", changed, err)
	}
	command, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{})
	if err != nil {
		t.Fatal(err)
	}
	if command.LastIncludedIndex >= changed.ConfigurationIndex {
		t.Fatalf("fixture lacks configuration-only gap: command=%d config=%d", command.LastIncludedIndex, changed.ConfigurationIndex)
	}
	if err := provider.ReplacementSnapshotReadyV1(ctx); err != nil {
		t.Fatalf("configuration-only gap refused: %v", err)
	}
	selected, err := provider.SnapshotForReplacementV1(ctx)
	if err != nil {
		t.Fatalf("native configuration-only snapshot: %v", err)
	}
	manifest := selected.Manifest
	commandTerm, commandIndex := manifest.CommandBoundaryV1()
	if manifest.Version != raftcluster.SnapshotManifestVersion2 || manifest.LastIncludedIndex < changed.ConfigurationIndex || commandTerm != command.LastIncludedTerm || commandIndex != command.LastIncludedIndex {
		t.Fatalf("native/command boundary mismatch: selected=%+v command=%+v config=%+v", manifest, command, changed)
	}
	if err := fsm.VerifyInstalledSnapshotManifestV1(manifest); err != nil {
		t.Fatalf("snapshot digest/progress did not bind command boundary: %v", err)
	}
	if !fsm.SnapshotCarrierReleasedV1() {
		t.Fatal("native future returned before FSM carrier release")
	}
}

func TestReplacementSnapshotAdmissionHandoffRacesCompletionV1(t *testing.T) {
	for i := 0; i < 100; i++ {
		fsm := &FSM{}
		fsm.snapshotOperationActive.Store(true)
		var releases atomic.Int32
		start := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-start
			done <- fsm.HandoffSnapshotWorkReleaseV1(func() { releases.Add(1) })
		}()
		close(start)
		fsm.releaseSnapshotOperationV1()
		if err := <-done; err != nil || releases.Load() != 1 {
			t.Fatalf("completion/handoff lost ownership: %v / %d", err, releases.Load())
		}
	}
}

type replacementFaultSnapshotStoreV1 struct {
	hraft.SnapshotStore
	writeErr, closeErr, cancelErr error
	cancels                       int
}

func (s *replacementFaultSnapshotStoreV1) Create(version hraft.SnapshotVersion, index, term uint64, config hraft.Configuration, configIndex uint64, transport hraft.Transport) (hraft.SnapshotSink, error) {
	sink, err := s.SnapshotStore.Create(version, index, term, config, configIndex, transport)
	if err != nil {
		return nil, err
	}
	return &replacementFaultSnapshotSinkV1{SnapshotSink: sink, store: s}, nil
}

type replacementFaultSnapshotSinkV1 struct {
	hraft.SnapshotSink
	store *replacementFaultSnapshotStoreV1
}

func (s *replacementFaultSnapshotSinkV1) Write(p []byte) (int, error) {
	if s.store.writeErr != nil {
		return 0, s.store.writeErr
	}
	return s.SnapshotSink.Write(p)
}
func (s *replacementFaultSnapshotSinkV1) Close() error {
	return errors.Join(s.SnapshotSink.Close(), s.store.closeErr)
}
func (s *replacementFaultSnapshotSinkV1) Cancel() error {
	s.store.cancels++
	return errors.Join(s.SnapshotSink.Cancel(), s.store.cancelErr)
}

func TestReplacementRetainedCopyCleanupProofV1(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	_, source, opts := snapshotOwnerProviderForTest(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	selected, err := source.SnapshotForReplacementV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	denied := errors.New("injected native sink failure")
	for _, test := range []struct {
		name                          string
		writeErr, closeErr, cancelErr error
		poison                        bool
		cancels                       int
	}{
		{"write-clean-cancel", denied, nil, nil, false, 1},
		{"write-unknown-cancel", denied, nil, denied, true, 1},
		{"unknown-close", nil, denied, nil, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			files, err := hraft.NewFileSnapshotStore(t.TempDir(), 1, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			store := &replacementFaultSnapshotStoreV1{SnapshotStore: files, writeErr: test.writeErr, closeErr: test.closeErr, cancelErr: test.cancelErr}
			_, err = source.RetainReplacementSnapshotSeedV1(ctx, selected, store, transport, opts.Cluster.NodeID, "node-b", "node-b", 64<<20)
			if !errors.Is(err, denied) || raftcluster.ReplacementSnapshotCleanupRequiredV1(err) != test.poison || store.cancels != test.cancels {
				t.Fatalf("cleanup proof: error=%v poison=%v cancels=%d", err, raftcluster.ReplacementSnapshotCleanupRequiredV1(err), store.cancels)
			}
		})
	}
}

// The native snapshot file is durable before HashiCorp invokes FSM.Restore.
// A successful logical replacement followed by scratch-cleanup failure must
// retain ownership and cannot be mistaken for native completion. Restart may
// recover that exact seed; this test never resends an ambiguous installation.
func TestReplacementNativeInstallCleanupDebtRecoversExactSeedV1(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	_, source, sourceOpts := snapshotOwnerProviderForTest(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	targetPeer := raftcluster.Peer{ID: "node-b", Address: "node-b"}
	initial, err := source.CommittedConfigurationV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := source.AddReplacementNonvoterV1(ctx, sourceOpts.Cluster.NodeID, raftcluster.Peer{ID: "node-c", Address: "node-c"}, initial.ConfigurationIndex)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := source.SnapshotForReplacementV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, commandIndex := selected.Manifest.CommandBoundaryV1()
	if selected.Manifest.Version != raftcluster.SnapshotManifestVersion2 || commandIndex >= changed.ConfigurationIndex || selected.Manifest.LastIncludedIndex < changed.ConfigurationIndex {
		t.Fatalf("fixture lacks native/command gap: %+v config=%+v", selected.Manifest, changed)
	}
	_, sender := hraft.NewInmemTransport("node-a")
	defer sender.Close()
	store, err := hraft.NewFileSnapshotStore(t.TempDir(), 1, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := source.RetainReplacementSnapshotSeedV1(ctx, selected, store, sender, sourceOpts.Cluster.NodeID, targetPeer.ID, targetPeer.Address, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cfg := raftcluster.Config{Dir: filepath.Join(root, "data"), ClusterDir: filepath.Join(root, "raft"), NodeID: targetPeer.ID, GroupID: sourceOpts.Cluster.GroupID, DisableSideStores: true, Peers: []raftcluster.Peer{{ID: "node-a", Address: "node-a"}, targetPeer}}
	var receiver *raftcluster.HashicorpRaftProvider
	var fsm *FSM
	var transport *hraft.InmemTransport
	closeReceiver := func() {
		sender.Disconnect(hraft.ServerAddress(targetPeer.Address))
		if receiver != nil {
			_ = receiver.Close()
			receiver = nil
		}
		if transport != nil {
			_ = transport.Close()
			transport = nil
		}
		if fsm != nil {
			_ = fsm.Close()
		}
	}
	defer closeReceiver()
	openReceiver := func() {
		t.Helper()
		db := openRaftSnapshotFSMTestDBWithOptions(t, cfg.Dir, true, true)
		t.Cleanup(func() { _ = db.Close() })
		fsm, err = Open(Options{DB: db, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
		if err != nil {
			t.Fatal(err)
		}
		_, transport = hraft.NewInmemTransport(hraft.ServerAddress(targetPeer.Address))
		rc := hraft.DefaultConfig()
		rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout = 50*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond
		rc.LogOutput = io.Discard
		receiver, err = raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{Cluster: cfg, Applier: fsm, Transport: transport, RaftConfig: rc, Bootstrap: false})
		if err != nil {
			t.Fatal(err)
		}
		sender.Connect(hraft.ServerAddress(targetPeer.Address), transport)
	}
	openReceiver()
	denied := errors.New("injected receiver scratch cleanup failure")
	var fail atomic.Bool
	fail.Store(true)
	raftSnapshotBeforeCleanupForTest = func(string) error {
		if fail.Load() {
			return denied
		}
		return nil
	}
	defer func() { raftSnapshotBeforeCleanupForTest = nil }()
	err = source.InstallReplacementSeedV1(ctx, seed, store, sender, sourceOpts.Cluster.NodeID, targetPeer)
	if err == nil || !strings.Contains(err.Error(), denied.Error()) {
		t.Fatalf("native cleanup error: %v", err)
	}
	if err := fsm.VerifyInstalledSnapshotManifestV1(seed.Manifest); err != nil {
		t.Fatalf("logical install did not complete before cleanup failure: %v", err)
	}
	if !fsm.snapshotOperationActive.Load() {
		t.Fatal("cleanup lost operation ownership")
	}
	if _, err := fsm.CaptureRaftSnapshotV1(); !errors.Is(err, denied) {
		t.Fatalf("cleanup did not block new export: %v", err)
	}
	if err := receiver.VerifyReplacementSnapshotInstalledV1(ctx, seed); err == nil {
		t.Fatal("failed native completion produced installed receipt")
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiver = nil
	if err := fsm.Close(); !errors.Is(err, denied) {
		t.Fatalf("Close lost cleanup debt: %v", err)
	}
	fail.Store(false)
	if err := fsm.Close(); err != nil {
		t.Fatalf("Close cleanup retry: %v", err)
	}
	if fsm.snapshotOperationActive.Load() {
		t.Fatal("successful cleanup retained admission")
	}
	closeReceiver()
	openReceiver()
	if err := receiver.VerifyReplacementSnapshotInstalledV1(ctx, seed); err != nil {
		t.Fatalf("restart lost exact native seed: %v", err)
	}
	if err := fsm.VerifyInstalledSnapshotManifestV1(seed.Manifest); err != nil {
		t.Fatalf("restart lost exact FSM seed: %v", err)
	}
	if fsm.snapshotOperationActive.Load() {
		t.Fatal("restarted install retained clean scratch")
	}
}
