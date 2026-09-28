package nativewire

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Lose only the reply, after the real native installer and receiver validation
// returned. This is not a mock installer or a bytes-received receipt.
type replacementLostSeedReplyV1 struct{ hraft.Transport }

func (t replacementLostSeedReplyV1) InstallSnapshot(id hraft.ServerID, target hraft.ServerAddress, request *hraft.InstallSnapshotRequest, response *hraft.InstallSnapshotResponse, reader io.Reader) error {
	if err := t.Transport.InstallSnapshot(id, target, request, response, reader); err != nil {
		return err
	}
	if !response.Success {
		return nil
	}
	return errors.New("injected loss of completed seed reply")
}

type replacementCountSeedSendV1 struct {
	hraft.Transport
	sends *atomic.Int32
}

func (t replacementCountSeedSendV1) InstallSnapshot(id hraft.ServerID, target hraft.ServerAddress, request *hraft.InstallSnapshotRequest, response *hraft.InstallSnapshotResponse, reader io.Reader) error {
	t.sends.Add(1)
	return t.Transport.InstallSnapshot(id, target, request, response, reader)
}

func TestReplacementPrejoinRealNativeInstallRestartAndTailV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		// The durable owner test asserts the explicit unsupported error.
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	root := t.TempDir()
	old := raftcluster.Peer{ID: "old", Address: "127.0.0.1:19001"}
	target := raftcluster.Peer{ID: "target", Address: "127.0.0.1:19002"}
	_, sourceTransport := hraft.NewInmemTransport(hraft.ServerAddress(old.Address))
	defer sourceTransport.Close()
	config := func(id raftcluster.NodeID, peers []raftcluster.Peer) raftcluster.Config {
		return raftcluster.Config{NodeID: id, GroupID: "default", Dir: filepath.Join(root, string(id), "data"), ClusterDir: filepath.Join(root, string(id), "raft"), DisableSideStores: true, Peers: peers}
	}
	openFSM := func(cfg raftcluster.Config) (*backenddb.DB, *raftfsm.FSM) {
		t.Helper()
		db, err := backenddb.Open(backenddb.Options{Dir: cfg.Dir, CommandWAL: true, CommandWALStatsScan: true})
		if err != nil {
			t.Fatal(err)
		}
		fsm, err := raftfsm.Open(raftfsm.Options{DB: db, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		return db, fsm
	}
	openProviderWithElection := func(cfg raftcluster.Config, fsm *raftfsm.FSM, transport hraft.Transport, bootstrap bool, election time.Duration) *raftcluster.HashicorpRaftProvider {
		t.Helper()
		rc := hraft.DefaultConfig()
		rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout = election, election, election
		rc.LogOutput = io.Discard
		provider, err := raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{Cluster: cfg, Applier: fsm, Transport: transport, RaftConfig: rc, Bootstrap: bootstrap})
		if err != nil {
			t.Fatal(err)
		}
		return provider
	}
	openProvider := func(cfg raftcluster.Config, fsm *raftfsm.FSM, transport hraft.Transport, bootstrap bool) *raftcluster.HashicorpRaftProvider {
		return openProviderWithElection(cfg, fsm, transport, bootstrap, 50*time.Millisecond)
	}
	sourceConfig := config(old.ID, []raftcluster.Peer{old})
	sourceDB, sourceFSM := openFSM(sourceConfig)
	defer sourceDB.Close()
	defer sourceFSM.Close()
	source := openProvider(sourceConfig, sourceFSM, sourceTransport, true)
	defer func() { _ = source.Close() }()
	for {
		status, err := source.RuntimeStatusV1(ctx)
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
	commit := func(name string) raftcluster.CommitCommandEntryV1Result {
		t.Helper()
		version, known, err := sourceFSM.CurrentCatalogVersion(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := source.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{NodeID: old.ID, GroupID: sourceConfig.GroupID, EntryBytes: fixedPeerCreateEntryV1(t, name, version), CurrentCatalogVersion: version, HasCurrentCatalogVersion: known, SyncLocalCommandWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	commit("before_seed")
	selected, err := source.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The seed is installed even though the native retained log could have
	// caught this fresh learner up without sending a snapshot.
	if selected.FirstLogIndexAfter == 0 || selected.FirstLogIndexAfter > selected.LastIncludedIndex {
		t.Fatalf("fixture lost retained-log alternative: %+v", selected)
	}
	seedStore, err := hraft.NewFileSnapshotStore(filepath.Join(root, "operation-seed"), 1, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := source.RetainReplacementSnapshotSeedV1(ctx, selected, seedStore, sourceTransport, old.ID, target.ID, target.Address, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	begin := replacementReceiverBeginForTestV1()
	begin.GroupID, begin.NewPeer = sourceConfig.GroupID, target
	receiverDir := t.TempDir()
	targetConfig := config(target.ID, []raftcluster.Peer{old, target})
	var targetDB *backenddb.DB
	var targetFSM *raftfsm.FSM
	var receiver *raftcluster.HashicorpRaftProvider
	var receiverTransport *hraft.InmemTransport
	var gate *replacementPrejoinTransportV1
	var owner *replacementReceiverOwnerV1
	closeReceiver := func() {
		sourceTransport.Disconnect(hraft.ServerAddress(target.Address))
		if receiver != nil {
			_ = receiver.Close()
			receiver = nil
		}
		if gate != nil {
			gate.providerStopped()
			gate = nil
		}
		if receiverTransport != nil {
			_ = receiverTransport.Close()
			receiverTransport = nil
		}
		if targetFSM != nil {
			_ = targetFSM.Close()
			targetFSM = nil
		}
		if targetDB != nil {
			_ = targetDB.Close()
			targetDB = nil
		}
		if owner != nil {
			_ = owner.Close()
			owner = nil
		}
	}
	defer closeReceiver()
	openReceiver := func() {
		t.Helper()
		owner, err = openReplacementReceiverOwnerV1(receiverDir, begin, seed)
		if err != nil {
			t.Fatal(err)
		}
		targetDB, targetFSM = openFSM(targetConfig)
		_, receiverTransport = hraft.NewInmemTransport(hraft.ServerAddress(target.Address))
		gate, err = newReplacementPrejoinTransportV1(receiverTransport, seed, owner.record.Phase, owner.persistPhase, func(actual raftcluster.ReplacementSnapshotSeedV1) error {
			if err := receiver.VerifyReplacementSnapshotInstalledV1(ctx, actual); err != nil {
				return err
			}
			return targetFSM.VerifyInstalledSnapshotManifestV1(actual.Manifest)
		})
		if err != nil {
			t.Fatal(err)
		}
		receiver = openProvider(targetConfig, targetFSM, gate, false)
		sourceTransport.Connect(hraft.ServerAddress(target.Address), receiverTransport)
		receiverTransport.Connect(hraft.ServerAddress(old.Address), sourceTransport)
	}
	openReceiver()
	if err := gate.allowEnrollment(); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("uninstalled enrollment=%v", err)
	}
	// Restart the retained source before its new election. This forces the
	// leader-only preflight refusal without relying on a short election race;
	// the exact retained seed must remain usable when leadership returns.
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	source = openProviderWithElection(sourceConfig, sourceFSM, sourceTransport, false, time.Second)
	// Shutdown closes the in-memory transport and clears its peer routes.
	// Reconnect this test link before retrying against the same receiver.
	sourceTransport.Connect(hraft.ServerAddress(target.Address), receiverTransport)
	var sends atomic.Int32
	counted := replacementCountSeedSendV1{Transport: sourceTransport, sends: &sends}
	if err := source.InstallReplacementSeedV1(ctx, seed, seedStore, counted, old.ID, target); !errors.Is(err, raftcluster.ErrReplacementInstallNotSentV1) || !errors.Is(err, raftcluster.ErrNotLeader) {
		t.Fatalf("pre-election install refusal=%v", err)
	}
	if sends.Load() != 0 || owner.record.Phase != replacementReceiverPreparedV1 {
		t.Fatalf("pre-election refusal sent snapshot or changed receiver: sends=%d phase=%s", sends.Load(), owner.record.Phase)
	}
	for {
		status, err := source.RuntimeStatusV1(ctx)
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
	if err := source.InstallReplacementSeedV1(ctx, seed, seedStore, replacementLostSeedReplyV1{Transport: counted}, old.ID, target); !errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("lost native response=%v", err)
	}
	if sends.Load() != 1 {
		t.Fatalf("exact seed install sends=%d", sends.Load())
	}
	if owner.record.Phase != replacementReceiverInstalledV1 || gate.ordinaryAllowed() {
		t.Fatal("native completion did not leave durable quarantined receipt")
	}
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(seed.Manifest); err != nil {
		t.Fatal(err)
	}
	closeReceiver()
	openReceiver()
	if err := gate.allowEnrollment(); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("restart accepted unverified cached receipt=%v", err)
	}
	if err := gate.reconcileInstalled(); err != nil {
		t.Fatal(err)
	}
	if err := gate.allowEnrollment(); err != nil {
		t.Fatal(err)
	}
	configuration, err := source.AddReplacementNonvoterV1(ctx, old.ID, target, seed.ConfigurationIndex)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, member := range configuration.Members {
		if member.ID == target.ID {
			found = true
			if member.Voter {
				t.Fatal("learner became voter")
			}
		}
	}
	if !found {
		t.Fatal("native learner absent")
	}
	tail := commit("after_seed")
	for {
		progress, err := targetFSM.AppliedProgress(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if progress.Index >= tail.Evidence.Index {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	// A delayed old native RPC cannot roll the durable tail back after add.
	metas, err := seedStore.List()
	if err != nil || len(metas) != 1 {
		t.Fatal(metas, err)
	}
	reader, err := raftcluster.OpenReplacementSnapshotSeedV1(ctx, seedStore, seed)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	status, err := source.RuntimeStatusV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := &hraft.InstallSnapshotRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax, ID: []byte(old.ID), Addr: []byte(old.Address)}, SnapshotVersion: seed.Version, Term: status.Term, Leader: []byte(old.Address), LastLogIndex: seed.Index, LastLogTerm: seed.Term, ConfigurationIndex: seed.ConfigurationIndex, Configuration: hraft.EncodeConfiguration(metas[0].Configuration), Size: seed.SizeBytes}
	var response hraft.InstallSnapshotResponse
	if err := sourceTransport.InstallSnapshot(hraft.ServerID(target.ID), hraft.ServerAddress(target.Address), request, &response, reader); err == nil {
		t.Fatal("delayed seed replay accepted")
	}
	progress, err := targetFSM.AppliedProgress(ctx)
	if err != nil || progress.Index < tail.Evidence.Index {
		t.Fatalf("delayed seed rolled back durable tail: %+v %v", progress, err)
	}
}
