package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func TestCatalogMetaRaftProviderCommitsOnlyAfterLeaderApplyAndSnapshots(t *testing.T) {
	state := &catalogMetaRaftTestState{}
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	p, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{Cluster: Config{Dir: t.TempDir(), NodeID: "node-a", GroupID: "meta", Peers: []Peer{{ID: "node-a", Address: "node-a", Capabilities: FeatureSet{Required: []RequiredFeature{{Name: FeatureCatalogMetaAuthority, Version: Version{Major: 1, Minor: 0}}}}}}}, State: state, Transport: transport, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		status, e := p.ClusterAdmissionStatus(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if status.Leader {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	term, index, err := p.SubmitCatalogMetaCommandV1(ctx, []byte("generation-1"))
	if err != nil {
		t.Fatal(err)
	}
	if term == 0 || index == 0 || !bytes.Equal(state.command, []byte("generation-1")) || state.index != index {
		t.Fatalf("term/index/state=%d/%d/%q/%d", term, index, state.command, state.index)
	}
	lastLogBeforeReads := p.raft.LastIndex()
	proof, err := p.LinearizableCatalogMetaReadProofV1(WithCatalogMetaReadSourceV1(ctx, CatalogMetaReadSourceOperationsHealthV1))
	if err != nil || proof.CatalogAppliedIndex != index || proof.CommitIndex == 0 || proof.RaftAppliedIndex < proof.CommitIndex || !proof.QuorumVerified {
		t.Fatalf("linearizable proof=%+v err=%v want catalog index %d", proof, err, index)
	}
	if err := p.ValidateCatalogMetaReadProofLeaseV1(proof); err != nil {
		t.Fatalf("validate proof lease: %v", err)
	}
	for _, source := range []CatalogMetaReadSourceV1{CatalogMetaReadSourceStrictSearchV1, CatalogMetaReadSourceServingRefreshV1, CatalogMetaReadSourceCoordinatorLifecycleV1, CatalogMetaReadSourceShardLifecycleV1, CatalogMetaReadSourceUnknownV1} {
		if got, err := p.LinearizableCatalogMetaAppliedIndexV1(WithCatalogMetaReadSourceV1(ctx, source)); err != nil || got != index {
			t.Fatalf("attributed linearizable applied index=%d err=%v want %d", got, err, index)
		}
	}
	stats := p.CatalogMetaLinearizableReadStatsV1()
	if stats.Total.Reads != 6 || stats.Total.Successes != 6 || stats.Total.Failures != 0 || stats.Total.VerifyLeaderCalls != 6 || stats.Total.LogBarriers != 0 || stats.Total.NoLogProofs != 6 || stats.OperationsHealth.Reads != 1 || stats.StrictSearch.Reads != 1 || stats.ServingRefresh.Reads != 1 || stats.CoordinatorLifecycle.Reads != 1 || stats.ShardLifecycle.Reads != 1 || stats.Unknown.Reads != 1 || stats.LastTerm == 0 || stats.LastCatalogApplied != index || stats.LastRaftApplied < index || stats.LastRaftLog < index {
		t.Fatalf("catalog read stats=%+v", stats)
	}
	if got := p.raft.LastIndex(); got != lastLogBeforeReads {
		t.Fatalf("catalog reads advanced raft log from %d to %d", lastLogBeforeReads, got)
	}
	expired := proof
	expired.validThrough = time.Now().Add(-time.Nanosecond)
	if err := p.ValidateCatalogMetaReadProofLeaseV1(expired); !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("expired proof err=%v want ErrReadBarrierNotSatisfied", err)
	}
	proofLease := p.proofLease
	p.proofLease = 0
	if _, err := p.LinearizableCatalogMetaReadProofV1(ctx); !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("proof whose lease expires during capture err=%v want ErrReadBarrierNotSatisfied", err)
	}
	p.proofLease = proofLease
	if err := p.SnapshotCatalogMetaV1(ctx); err != nil {
		t.Fatal(err)
	}
	if len(state.snapshot) == 0 {
		t.Fatal("snapshot did not export state")
	}
	if _, _, err := p.SubmitCatalogMetaCommandV1(context.Background(), []byte("generation-2")); err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateCatalogMetaReadProofLeaseV1(proof); !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("stale proof err=%v want ErrReadBarrierNotSatisfied", err)
	}
	if err := state.InstallCatalogMetaSnapshotBytesV1(catalogMetaRestoreCapabilityV1(), state.snapshot); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.command, []byte("generation-1")) {
		t.Fatalf("restore command=%q", state.command)
	}
}

func TestCatalogMetaRaftProviderFollowerAndCancellationFailClosed(t *testing.T) {
	p := &CatalogMetaRaftProviderV1{}
	if _, _, err := p.SubmitCatalogMetaCommandV1(context.Background(), []byte("x")); !errors.Is(err, ErrInvalidHashicorpRaftProvider) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.LinearizableCatalogMetaAppliedIndexV1(context.Background()); !errors.Is(err, ErrInvalidHashicorpRaftProvider) {
		t.Fatalf("linearizable read err=%v want ErrInvalidHashicorpRaftProvider", err)
	}
}

func TestCatalogMetaRaftProviderClosesOwnedStoresOnBootstrapPreflightErrors(t *testing.T) {
	t.Run("has existing state", func(t *testing.T) {
		config := catalogMetaTestConfig(t.TempDir(), "node-a", []Peer{catalogMetaCapablePeer("node-a")})
		resolved, err := Validate(config)
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		sentinel := errors.New("last-index sentinel")
		_, transport := hraft.NewInmemTransport("node-a")
		defer transport.Close()

		provider, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
			Cluster:       config,
			State:         &catalogMetaRaftTestState{},
			Transport:     transport,
			LogStore:      &catalogMetaFailingLastIndexStore{InmemStore: hraft.NewInmemStore(), err: sentinel},
			SnapshotStore: hraft.NewInmemSnapshotStore(),
			Bootstrap:     true,
		})
		if provider != nil || !errors.Is(err, ErrInvalidHashicorpRaftProvider) || !strings.Contains(err.Error(), sentinel.Error()) {
			t.Fatalf("provider=%v err=%v want nil provider and sentinel error", provider, err)
		}
		assertCatalogMetaBoltStoreUnlocked(t, filepath.Join(resolved.Layout.StableDir, "raft-stable.bolt"))
	})

	t.Run("bootstrap cluster", func(t *testing.T) {
		config := catalogMetaTestConfig(t.TempDir(), "node-a", []Peer{catalogMetaCapablePeer("node-a")})
		resolved, err := Validate(config)
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		sentinel := errors.New("set-uint64 sentinel")
		_, transport := hraft.NewInmemTransport("node-a")
		defer transport.Close()

		provider, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
			Cluster:       config,
			State:         &catalogMetaRaftTestState{},
			Transport:     transport,
			StableStore:   &catalogMetaFailingSetUint64Store{InmemStore: hraft.NewInmemStore(), err: sentinel},
			SnapshotStore: hraft.NewInmemSnapshotStore(),
			Bootstrap:     true,
		})
		if provider != nil || !errors.Is(err, ErrInvalidHashicorpRaftProvider) || !strings.Contains(err.Error(), sentinel.Error()) {
			t.Fatalf("provider=%v err=%v want nil provider and sentinel error", provider, err)
		}
		assertCatalogMetaBoltStoreUnlocked(t, filepath.Join(resolved.Layout.LogDir, "raft-log.bolt"))
	})
}

func TestCatalogMetaRaftProviderDistinguishesPreEnqueueCancellationFromAmbiguousApply(t *testing.T) {
	state := &blockingCatalogMetaRaftTestState{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	// This test isolates apply/cancellation semantics; persistence and owned
	// Bolt-store cleanup are exercised separately.
	p, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
		Cluster:       catalogMetaTestConfig(t.TempDir(), "node-a", []Peer{catalogMetaCapablePeer("node-a")}),
		State:         state,
		Transport:     transport,
		LogStore:      hraft.NewInmemStore(),
		StableStore:   hraft.NewInmemStore(),
		SnapshotStore: hraft.NewInmemSnapshotStore(),
		RaftConfig:    catalogMetaFastRaftConfig(),
		Bootstrap:     true,
		ApplyTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("OpenCatalogMetaRaftProviderV1: %v", err)
	}
	defer p.Close()
	waitCatalogMetaLeader(t, map[NodeID]*CatalogMetaRaftProviderV1{"node-a": p})

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := p.SubmitCatalogMetaCommandV1(canceled, []byte("not-enqueued")); !errors.Is(err, context.Canceled) || errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("pre-enqueue cancellation error=%v want context.Canceled without ambiguity", err)
	}
	if got := state.applyCount(); got != 0 {
		t.Fatalf("pre-enqueue apply count=%d want 0", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := p.SubmitCatalogMetaCommandV1(ctx, []byte("enqueued"))
		result <- err
	}()
	select {
	case <-state.started:
	case <-time.After(2 * time.Second):
		t.Fatal("catalog command did not reach committed apply")
	}
	err = <-result
	if !errors.Is(err, ErrCommitAmbiguous) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("post-enqueue cancellation error=%v want ErrCommitAmbiguous and deadline", err)
	}
	close(state.release)
	waitCatalogMetaCondition(t, func() bool {
		return bytes.Equal(state.currentCommand(), []byte("enqueued"))
	}, "enqueued command did not finish applying after ambiguous caller outcome")
}

type catalogMetaFailingLastIndexStore struct {
	*hraft.InmemStore
	err error
}

func (s *catalogMetaFailingLastIndexStore) LastIndex() (uint64, error) {
	return 0, s.err
}

type catalogMetaFailingSetUint64Store struct {
	*hraft.InmemStore
	err error
}

func (s *catalogMetaFailingSetUint64Store) SetUint64([]byte, uint64) error {
	return s.err
}

func assertCatalogMetaBoltStoreUnlocked(t *testing.T, path string) {
	t.Helper()
	store, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		t.Fatalf("reopen owned Bolt store %q after failed provider startup: %v", path, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close reopened Bolt store %q: %v", path, err)
	}
}

func TestCatalogMetaRaftProviderFixedPeersFailoverSnapshotReopenAndRejoin(t *testing.T) {
	root := t.TempDir()
	peers := []Peer{
		catalogMetaCapablePeer("node-a"),
		catalogMetaCapablePeer("node-b"),
		catalogMetaCapablePeer("node-c"),
	}
	transports := make(map[NodeID]*hraft.InmemTransport, len(peers))
	for _, peer := range peers {
		_, transport := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(peer.Address), 2*time.Second)
		transports[peer.ID] = transport
	}
	connectCatalogMetaTransports(peers, transports)
	t.Cleanup(func() {
		for _, transport := range transports {
			_ = transport.Close()
		}
	})

	providers := make(map[NodeID]*CatalogMetaRaftProviderV1, len(peers))
	states := make(map[NodeID]*concurrentCatalogMetaRaftTestState, len(peers))
	configs := make(map[NodeID]Config, len(peers))
	for _, peer := range peers {
		cfg := catalogMetaTestConfig(filepath.Join(root, string(peer.ID)), peer.ID, peers)
		state := &concurrentCatalogMetaRaftTestState{}
		provider, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
			Cluster:      cfg,
			State:        state,
			Transport:    transports[peer.ID],
			RaftConfig:   catalogMetaFastRaftConfig(),
			Bootstrap:    true,
			ApplyTimeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("%s OpenCatalogMetaRaftProviderV1: %v", peer.ID, err)
		}
		if got, want := provider.proofLease, catalogMetaFastRaftConfig().LeaderLeaseTimeout/2; got != want {
			t.Fatalf("%s proof lease=%s want configured leader lease safety margin %s", peer.ID, got, want)
		}
		providers[peer.ID], states[peer.ID], configs[peer.ID] = provider, state, cfg
	}
	t.Cleanup(func() {
		for _, provider := range providers {
			_ = provider.Close()
		}
	})

	leaderID := waitCatalogMetaLeader(t, providers)
	if _, _, err := providers[leaderID].SubmitCatalogMetaCommandV1(context.Background(), []byte("generation-1")); err != nil {
		t.Fatalf("leader submit generation-1: %v", err)
	}
	waitCatalogMetaStates(t, states, []NodeID{"node-a", "node-b", "node-c"}, []byte("generation-1"))
	oldProof, err := providers[leaderID].LinearizableCatalogMetaReadProofV1(context.Background())
	if err != nil {
		t.Fatalf("old leader proof: %v", err)
	}
	for id, provider := range providers {
		if id == leaderID {
			continue
		}
		if _, _, err := provider.SubmitCatalogMetaCommandV1(context.Background(), []byte("follower-write")); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("%s follower submit error=%v want ErrNotLeader", id, err)
		}
		break
	}

	oldLeader := providers[leaderID]
	if err := oldLeader.Close(); err != nil {
		t.Fatalf("close old leader %s: %v", leaderID, err)
	}
	if err := oldLeader.ValidateCatalogMetaReadProofLeaseV1(oldProof); err == nil {
		t.Fatal("closed leader accepted retained proof")
	}
	delete(providers, leaderID)
	disconnectCatalogMetaTransport(leaderID, peers, transports)
	newLeaderID := waitCatalogMetaLeader(t, providers)
	newProof, err := providers[newLeaderID].LinearizableCatalogMetaReadProofV1(context.Background())
	if err != nil || newProof.LeaderTerm == oldProof.LeaderTerm || newProof.CommitIndex == 0 {
		t.Fatalf("new leader proof=%+v err=%v old term=%d", newProof, err, oldProof.LeaderTerm)
	}
	lastLog := providers[newLeaderID].raft.LastIndex()
	if _, err := providers[newLeaderID].LinearizableCatalogMetaReadProofV1(context.Background()); err != nil {
		t.Fatalf("repeat new leader proof: %v", err)
	}
	if got := providers[newLeaderID].raft.LastIndex(); got != lastLog {
		t.Fatalf("repeat new leader proof advanced raft log from %d to %d", lastLog, got)
	}
	if _, _, err := providers[newLeaderID].SubmitCatalogMetaCommandV1(context.Background(), []byte("generation-2")); err != nil {
		t.Fatalf("new leader submit generation-2: %v", err)
	}
	running := make([]NodeID, 0, len(providers))
	for id := range providers {
		running = append(running, id)
	}
	waitCatalogMetaStates(t, states, running, []byte("generation-2"))

	unsupported := configs[leaderID]
	unsupported.Peers = append([]Peer(nil), unsupported.Peers...)
	for i := range unsupported.Peers {
		if unsupported.Peers[i].ID == leaderID {
			unsupported.Peers[i].Capabilities = DefaultFeatureSet()
		}
	}
	if rejected, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
		Cluster: unsupported, State: &concurrentCatalogMetaRaftTestState{}, Transport: transports[leaderID], Bootstrap: true,
	}); rejected != nil || !errors.Is(err, ErrUnsupportedFeature) {
		if rejected != nil {
			_ = rejected.Close()
		}
		t.Fatalf("unsupported voter reopen provider/error=%v/%v want nil ErrUnsupportedFeature", rejected, err)
	}

	rejoinedState := &concurrentCatalogMetaRaftTestState{}
	rejoined, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
		Cluster:      configs[leaderID],
		State:        rejoinedState,
		Transport:    transports[leaderID],
		RaftConfig:   catalogMetaFastRaftConfig(),
		Bootstrap:    true,
		ApplyTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("rejoin old leader %s: %v", leaderID, err)
	}
	providers[leaderID], states[leaderID] = rejoined, rejoinedState
	connectCatalogMetaTransports(peers, transports)
	waitCatalogMetaStates(t, states, []NodeID{leaderID}, []byte("generation-2"))

	if err := rejoined.SnapshotCatalogMetaV1(context.Background()); err != nil {
		t.Fatalf("snapshot rejoined node: %v", err)
	}
	if err := rejoined.Close(); err != nil {
		t.Fatalf("close snapshotted node: %v", err)
	}
	delete(providers, leaderID)
	reopenedState := &concurrentCatalogMetaRaftTestState{}
	reopened, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
		Cluster:      configs[leaderID],
		State:        reopenedState,
		Transport:    transports[leaderID],
		RaftConfig:   catalogMetaFastRaftConfig(),
		Bootstrap:    true,
		ApplyTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("reopen snapshotted node: %v", err)
	}
	providers[leaderID], states[leaderID] = reopened, reopenedState
	connectCatalogMetaTransports(peers, transports)
	waitCatalogMetaStates(t, states, []NodeID{leaderID}, []byte("generation-2"))
}

func TestCatalogMetaRaftProviderRefusesUnsupportedFixedVoterBeforeBootstrap(t *testing.T) {
	peers := []Peer{catalogMetaCapablePeer("node-a"), catalogMetaCapablePeer("node-b")}
	peers[1].Capabilities = DefaultFeatureSet()
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	provider, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
		Cluster:   catalogMetaTestConfig(t.TempDir(), "node-a", peers),
		State:     &catalogMetaRaftTestState{},
		Transport: transport,
		Bootstrap: true,
	})
	if provider != nil {
		_ = provider.Close()
		t.Fatal("unsupported fixed voter returned a provider")
	}
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("OpenCatalogMetaRaftProviderV1 error=%v want ErrUnsupportedFeature", err)
	}
}

func TestCatalogMetaRaftProviderUnavailableWithoutMetaQuorum(t *testing.T) {
	peers := []Peer{
		catalogMetaCapablePeer("node-a"),
		catalogMetaCapablePeer("node-b"),
		catalogMetaCapablePeer("node-c"),
	}
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	state := &concurrentCatalogMetaRaftTestState{}
	provider, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{
		Cluster:      catalogMetaTestConfig(t.TempDir(), "node-a", peers),
		State:        state,
		Transport:    transport,
		RaftConfig:   catalogMetaFastRaftConfig(),
		Bootstrap:    true,
		ApplyTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("OpenCatalogMetaRaftProviderV1: %v", err)
	}
	defer provider.Close()
	waitCatalogMetaCondition(t, func() bool {
		status, err := provider.ClusterAdmissionStatus(context.Background())
		return err == nil && status.Unavailable && !status.Leader
	}, "isolated catalog meta voter did not report unavailable")
	if _, _, err := provider.SubmitCatalogMetaCommandV1(context.Background(), []byte("must-not-commit")); !errors.Is(err, ErrAdmissionUnavailable) {
		t.Fatalf("submit without meta quorum error=%v want ErrAdmissionUnavailable", err)
	}
	if got := state.currentCommand(); len(got) != 0 {
		t.Fatalf("unavailable meta voter applied %q", got)
	}
}

func TestCatalogMetaRaftRestoreBoundsSnapshotBeforeStateInstall(t *testing.T) {
	state := &catalogMetaRaftTestState{}
	fsm := catalogMetaRaftFSMV1{state: state}
	raw := bytes.Repeat([]byte("x"), catalogMetaRaftSnapshotMaxBytesV1+1)
	err := fsm.Restore(io.NopCloser(bytes.NewReader(raw)))
	if !errors.Is(err, ErrHashicorpRaftLogEntry) {
		t.Fatalf("Restore error=%v want ErrHashicorpRaftLogEntry", err)
	}
	if state.command != nil {
		t.Fatalf("oversized snapshot reached state install: %d bytes", len(state.command))
	}
}

type catalogMetaRaftTestState struct {
	command, snapshot []byte
	index             uint64
}

type blockingCatalogMetaRaftTestState struct {
	mu      sync.Mutex
	command []byte
	applies int
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingCatalogMetaRaftTestState) ApplyCatalogMetaCommittedV1(capability CatalogMetaApplyCapabilityV1, b []byte, _ uint64) error {
	if !capability.Granted() {
		return ErrHashicorpRaftLogEntry
	}
	s.once.Do(func() { close(s.started) })
	<-s.release
	s.mu.Lock()
	s.command = bytes.Clone(b)
	s.applies++
	s.mu.Unlock()
	return nil
}

func (s *blockingCatalogMetaRaftTestState) ExportCatalogMetaSnapshotBytesV1() ([]byte, error) {
	return s.currentCommand(), nil
}

func (s *blockingCatalogMetaRaftTestState) InstallCatalogMetaSnapshotBytesV1(capability CatalogMetaRestoreCapabilityV1, b []byte) error {
	if !capability.Granted() {
		return ErrHashicorpRaftLogEntry
	}
	s.mu.Lock()
	s.command = bytes.Clone(b)
	s.mu.Unlock()
	return nil
}

func (s *blockingCatalogMetaRaftTestState) currentCommand() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.command)
}

func (s *blockingCatalogMetaRaftTestState) applyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applies
}

type concurrentCatalogMetaRaftTestState struct {
	mu      sync.Mutex
	command []byte
	index   uint64
}

func (s *concurrentCatalogMetaRaftTestState) ApplyCatalogMetaCommittedV1(capability CatalogMetaApplyCapabilityV1, b []byte, index uint64) error {
	if !capability.Granted() {
		return ErrHashicorpRaftLogEntry
	}
	s.mu.Lock()
	s.command, s.index = bytes.Clone(b), index
	s.mu.Unlock()
	return nil
}

func (s *concurrentCatalogMetaRaftTestState) ExportCatalogMetaSnapshotBytesV1() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.command), nil
}

func (s *concurrentCatalogMetaRaftTestState) InstallCatalogMetaSnapshotBytesV1(capability CatalogMetaRestoreCapabilityV1, b []byte) error {
	if !capability.Granted() {
		return ErrHashicorpRaftLogEntry
	}
	s.mu.Lock()
	s.command = bytes.Clone(b)
	s.mu.Unlock()
	return nil
}

func (s *concurrentCatalogMetaRaftTestState) currentCommand() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.command)
}

func (s *concurrentCatalogMetaRaftTestState) CatalogMetaAppliedIndexV1() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.index, s.index != 0
}

func catalogMetaCapablePeer(id NodeID) Peer {
	return Peer{
		ID:      id,
		Address: string(id),
		Capabilities: FeatureSet{
			ConfigVersion: SupportedConfigVersion,
			Required: []RequiredFeature{
				{Name: FeatureSingleGroupProvider, Version: SupportedFeatureFloors[FeatureSingleGroupProvider]},
				{Name: FeatureCatalogMetaAuthority, Version: SupportedFeatureFloors[FeatureCatalogMetaAuthority]},
			},
		},
	}
}

func catalogMetaTestConfig(root string, id NodeID, peers []Peer) Config {
	return Config{
		Dir:      filepath.Join(root, "db"),
		NodeID:   id,
		GroupID:  "meta",
		Peers:    append([]Peer(nil), peers...),
		Features: catalogMetaCapablePeer(id).Capabilities,
	}
}

func catalogMetaFastRaftConfig() *hraft.Config {
	config := hraft.DefaultConfig()
	config.HeartbeatTimeout = 2 * time.Second
	config.ElectionTimeout = 2 * time.Second
	config.LeaderLeaseTimeout = 2 * time.Second
	config.CommitTimeout = 5 * time.Millisecond
	config.SnapshotInterval = time.Hour
	config.SnapshotThreshold = ^uint64(0)
	config.TrailingLogs = 0
	config.LogOutput = io.Discard
	config.LogLevel = "ERROR"
	config.NoLegacyTelemetry = true
	return config
}

func TestCatalogMetaFastRaftConfigProvidesSchedulingHeadroom(t *testing.T) {
	config := catalogMetaFastRaftConfig()
	const minimum = 2 * time.Second
	if config.HeartbeatTimeout < minimum || config.ElectionTimeout < minimum || config.LeaderLeaseTimeout < minimum {
		t.Fatalf("coordination timeouts heartbeat=%s election=%s lease=%s want each at least %s", config.HeartbeatTimeout, config.ElectionTimeout, config.LeaderLeaseTimeout, minimum)
	}
}

func waitCatalogMetaLeader(t *testing.T, providers map[NodeID]*CatalogMetaRaftProviderV1) NodeID {
	t.Helper()
	var leader NodeID
	waitCatalogMetaCondition(t, func() bool {
		leader = ""
		for id, provider := range providers {
			status, err := provider.ClusterAdmissionStatus(context.Background())
			if err == nil && status.Leader {
				if leader != "" {
					return false
				}
				leader = id
			}
		}
		if leader == "" {
			return false
		}
		provider := providers[leader]
		ctx, cancel := context.WithTimeout(context.Background(), provider.applyTimeout)
		err := waitHashicorpRaftFuture(ctx, provider.raft.VerifyLeader())
		cancel()
		return err == nil && provider.raft.State() == hraft.Leader
	}, fmt.Sprintf("catalog meta cluster has no unique leader among %d providers", len(providers)))
	return leader
}

func waitCatalogMetaStates(t *testing.T, states map[NodeID]*concurrentCatalogMetaRaftTestState, ids []NodeID, command []byte) {
	t.Helper()
	waitCatalogMetaCondition(t, func() bool {
		for _, id := range ids {
			state := states[id]
			if state == nil || !bytes.Equal(state.currentCommand(), command) {
				return false
			}
		}
		return true
	}, fmt.Sprintf("catalog states %v did not converge to %q", ids, command))
}

func waitCatalogMetaCondition(t *testing.T, ready func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(message)
}

func connectCatalogMetaTransports(peers []Peer, transports map[NodeID]*hraft.InmemTransport) {
	for _, from := range peers {
		for _, to := range peers {
			if from.ID == to.ID {
				continue
			}
			transports[from.ID].Connect(hraft.ServerAddress(to.Address), transports[to.ID])
		}
	}
}

func disconnectCatalogMetaTransport(id NodeID, peers []Peer, transports map[NodeID]*hraft.InmemTransport) {
	transports[id].DisconnectAll()
	for _, peer := range peers {
		if peer.ID != id {
			transports[peer.ID].Disconnect(hraft.ServerAddress(id))
		}
	}
}

func (s *catalogMetaRaftTestState) ApplyCatalogMetaCommittedV1(_ CatalogMetaApplyCapabilityV1, b []byte, index uint64) error {
	s.command = bytes.Clone(b)
	s.index = index
	return nil
}
func (s *catalogMetaRaftTestState) ExportCatalogMetaSnapshotBytesV1() ([]byte, error) {
	s.snapshot = bytes.Clone(s.command)
	return bytes.Clone(s.command), nil
}
func (s *catalogMetaRaftTestState) InstallCatalogMetaSnapshotBytesV1(capability CatalogMetaRestoreCapabilityV1, b []byte) error {
	if !capability.Granted() {
		return ErrRaftSnapshotUnsupported
	}
	s.command = bytes.Clone(b)
	return nil
}
func (s *catalogMetaRaftTestState) CatalogMetaAppliedIndexV1() (uint64, bool) {
	return s.index, true
}

var _ io.Closer = (*catalogMetaNoopCloser)(nil)

type catalogMetaNoopCloser struct{}

func (*catalogMetaNoopCloser) Close() error { return nil }
