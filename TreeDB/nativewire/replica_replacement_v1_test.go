package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This boundary uses authenticated TCP, real catalog/data providers, and a
// fresh Nodes-only spare. An actual native snapshot must be installed before
// nonvoter enrollment; neither configured peers nor a logs-only join is proof.
func TestReplacementCannotVoteBeforeSnapshotAndTailReadyV1(t *testing.T) {
	testReplacementPublicInstallV1(t, false, false)
}

func TestReplacementPublicInstallSurvivesCallerDeadlineV1(t *testing.T) {
	testReplacementPublicInstallV1(t, true, false)
}

func testReplacementPublicInstallV1(t *testing.T, stallInstall, promote bool, completion ...bool) {
	complete := len(completion) != 0 && completion[0]
	configs := fixedPeerTestConfigsV1(t)
	if complete {
		// HashiCorp bounds leadership transfer by ElectionTimeout, independently
		// of the request context. Retirement tests correctness on loaded/race
		// runners, not a 300ms election/transfer latency target.
		for i := range configs {
			configs[i].RaftTimeout = time.Second
		}
	}
	used := map[string]bool{}
	for _, node := range configs[0].Nodes {
		used[node.Address] = true
	}
	for _, group := range append([]FixedPeerTCPGroupV1{configs[0].Catalog}, configs[0].Groups...) {
		for _, peer := range group.Peers {
			used[peer.Address] = true
		}
	}
	address := func() string {
		result := fixedPeerReserveTestAddressV1(t, used)
		used[result] = true
		return result
	}
	group := configs[0].Groups[1]
	group.Peers = append([]raftcluster.Peer{configs[0].Groups[0].Peers[0]}, group.Peers...)
	spare := FixedPeerTCPNodeV1{ID: "replacement", Address: address()}
	nodes := append(append([]FixedPeerTCPNodeV1(nil), configs[0].Nodes...), spare)
	if complete {
		for _, id := range []raftcluster.NodeID{"replacement-2", "replacement-3", "replacement-4"} {
			nodes = append(nodes, FixedPeerTCPNodeV1{ID: id, Address: address()})
		}
	}
	for i := range configs {
		configs[i].Nodes = nodes
		configs[i].Groups = []FixedPeerTCPGroupV1{group}
		delete(configs[i].RaftListen, "group-a")
		for _, peer := range group.Peers {
			if peer.ID == configs[i].NodeID {
				configs[i].RaftListen[group.ID] = peer.Address
			}
		}
	}
	spareConfig := configs[0]
	spareConfig.NodeID, spareConfig.ListenAddress = spare.ID, spare.Address
	spareRoot := t.TempDir()
	spareConfig.DataRoot, spareConfig.RaftRoot = filepath.Join(spareRoot, "data"), filepath.Join(spareRoot, "raft")
	spareConfig.RaftListen = map[raftcluster.GroupID]string{}
	configs = append(configs, spareConfig)
	if complete {
		for _, node := range nodes[4:] {
			extra := spareConfig
			extra.NodeID, extra.ListenAddress = node.ID, node.Address
			extraRoot := t.TempDir()
			extra.DataRoot, extra.RaftRoot = filepath.Join(extraRoot, "data"), filepath.Join(extraRoot, "raft")
			configs = append(configs, extra)
		}
	}
	ca := newPeerCAFixtureV1(t)
	for i := range configs {
		configs[i].ClusterID = "replacement-conformance"
		configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	}
	runtimes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	for i, cfg := range configs {
		runtime, err := fixedPeerOpenTestRuntimeV1(t, cfg)
		if err != nil {
			t.Fatal(err)
		}
		runtimes[i] = runtime
		t.Cleanup(func() {
			if err := runtime.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	leader := fixedPeerWaitCatalogLeaderV1(t, ctx, runtimes[:3])
	members := []raftcluster.NodeID{"ingress", "owner-1", "owner-2"}
	catalog := raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: group.ID, Members: members}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"}, GroupID: group.ID}}}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, configs[leader].NodeID, initial); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, cfg := range configs[:3] {
			status, err := client.Status(ctx, cfg.NodeID)
			if err != nil || status.Catalog.Epoch != 1 {
				return false
			}
		}
		return true
	})
	spareStatus, err := client.Status(ctx, spare.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(spareStatus.Groups) != 0 || runtimes[3].meta != nil {
		t.Fatal("fresh replacement bootstrapped a group")
	}
	// Existing public catalog commands cannot authorize membership changes. This
	// bounded envelope is the first new operation admitted by P5's existing apply
	// dispatcher; it carries no caller assertion of snapshot or tail readiness.
	oldNode := raftcluster.NodeID("owner-2")
	if complete {
		// Catalog readiness does not imply the data group has committed a
		// current-term leader. A follower's discovery hint is insufficient for
		// choosing the old voter in this completion fixture.
		initialLeader := fixedPeerWaitDataLeaderV1(t, ctx, runtimes[:3], group)
		for _, peer := range group.Peers {
			if peer.ID != initialLeader {
				oldNode = peer.ID
				break
			}
		}
	}
	begin, err := json.Marshal(struct {
		Format        uint16              `json:"format"`
		Kind          string              `json:"kind"`
		OperationID   string              `json:"operation_id"`
		ConfigDigest  string              `json:"config_digest"`
		ExpectedEpoch uint64              `json:"expected_epoch"`
		CatalogDigest string              `json:"catalog_digest"`
		GroupID       raftcluster.GroupID `json:"group_id"`
		OldNodeID     raftcluster.NodeID  `json:"old_node_id"`
		NewPeer       raftcluster.Peer    `json:"new_peer"`
	}{1, "replica-replacement-begin-v1", "replace-owner-2", client.digest, record.Epoch, record.Digest, group.ID, oldNode, raftcluster.Peer{ID: spare.ID, Address: address()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtimes[leader].meta.SubmitCatalogMetaCommandV1(ctx, begin); err != nil {
		t.Fatalf("replacement BEGIN not admitted by committed catalog; cannot safely enroll nonvoter or prove snapshot/tail promotion: %v", err)
	}
	operation, err := raftplacement.DecodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	fixedPeerAdoptTestAddressV1(runtimes[3], operation.NewPeer.Address)
	reservedTargetRaft := fixedPeerReservedTestListenerV1(runtimes[3], operation.NewPeer.Address)
	// Catalog election is independent of the data group's election. Snapshot
	// capture also requires a real applied command, not a config/no-op index.
	sourceNode := fixedPeerWaitDataLeaderV1(t, ctx, runtimes[:3], group)
	var source *fixedPeerDataV1
	for _, runtime := range runtimes[:3] {
		if runtime.config.NodeID == sourceNode {
			source = runtime.localDataV1(group.ID)
		}
	}
	version, known, err := source.fsm.CurrentCatalogVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.provider.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{NodeID: sourceNode, GroupID: group.ID, EntryBytes: fixedPeerCreateEntryV1(t, "users", version), CurrentCatalogVersion: version, HasCurrentCatalogVersion: known, SyncLocalCommandWAL: true}); err != nil {
		t.Fatal(err)
	}
	if stallInstall && rootpublication.StableRelativeNamespaceSupported() {
		seedReply, err := client.pollReplacementV1(ctx, sourceNode, "replacement-seed", begin)
		if err != nil || seedReply.ReplacementSeed == nil {
			t.Fatalf("prepare stalled seed: %+v %v", seedReply, err)
		}
		state := raftplacement.ReplicaReplacementStateV1{Begin: operation, Phase: raftplacement.ReplicaReplacementSeededV1, Seed: seedReply.ReplacementSeed}
		if err := client.commitReplacementPhaseV1(ctx, configs[leader].NodeID, state); err != nil {
			t.Fatal(err)
		}
		if _, err := client.call(ctx, spare.ID, "replacement-prepare", fixedPeerRequestV1{Entry: begin}, true); err != nil {
			t.Fatal(err)
		}
		target := runtimes[3].localDataV1(group.ID)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseInstall := func() { releaseOnce.Do(func() { close(release) }) }
		defer releaseInstall()
		var nativeCompletions atomic.Int32
		target.prejoin.mu.Lock()
		originalVerify := target.prejoin.verify
		target.prejoin.verify = func(seed raftcluster.ReplacementSnapshotSeedV1) error {
			nativeCompletions.Add(1)
			close(entered)
			<-release
			return originalVerify(seed)
		}
		target.prejoin.mu.Unlock()
		// Lose a reply even if the underlying authenticated native transport
		// survives its own deadline. The receiver remains the only authority.
		source.transport = replacementLostSeedReplyV1{Transport: source.transport}
		leader = fixedPeerWaitCatalogLeaderV1(t, ctx, runtimes[:3])
		caller, cancelCaller := context.WithCancel(ctx)
		defer cancelCaller()
		returned := make(chan error, 1)
		go func() {
			_, err := client.PrepareReplicaReplacementV1(caller, configs[leader].NodeID, operation)
			returned <- err
		}()
		select {
		case <-entered:
		case err := <-returned:
			t.Fatalf("install returned before native completion: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// The real native FSM restore has finished; its verification/completion
		// still owns the gate past the short control/native network timeout.
		timer := time.NewTimer(configs[0].RequestTimeout + 100*time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
		cancelCaller()
		select {
		case err := <-returned:
			if err == nil {
				t.Fatal("disconnected caller reported completed preparation")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		target.prejoin.mu.Lock()
		inflight := target.prejoin.inflight
		target.prejoin.mu.Unlock()
		if !inflight {
			t.Fatal("request timeout released native install fence")
		}
		before, err := source.provider.CommittedConfigurationV1(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, member := range before.Members {
			if member.ID == spare.ID {
				t.Fatal("target enrolled before native completion")
			}
		}
		short, cancelShort := context.WithTimeout(ctx, 50*time.Millisecond)
		_, retryErr := client.PrepareReplicaReplacementV1(short, configs[leader].NodeID, operation)
		cancelShort()
		if retryErr == nil || nativeCompletions.Load() != 1 {
			t.Fatalf("retry duplicated/completed blocked install: %d %v", nativeCompletions.Load(), retryErr)
		}
		releaseInstall()
		fixedPeerWaitV1(t, ctx, func() bool {
			target.prejoin.mu.Lock()
			defer target.prejoin.mu.Unlock()
			return target.prejoin.phase == replacementReceiverInstalledV1 && target.prejoin.verified && !target.prejoin.inflight
		})
		for _, phase := range []raftplacement.ReplicaReplacementPhaseV1{raftplacement.ReplicaReplacementInstalledV1, raftplacement.ReplicaReplacementAddIntentV1} {
			state.Phase = phase
			if err := client.commitReplacementPhaseV1(ctx, configs[leader].NodeID, state); err != nil {
				t.Fatal(err)
			}
		}
		dataLeader, err := client.leader(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.call(ctx, dataLeader, "replacement-enroll", fixedPeerRequestV1{Entry: begin}, true); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
			t.Fatalf("authorized enrollment bypassed receiver cutoff: %v", err)
		}

	}
	// Data election/setup may outlast the earlier catalog leader observation.
	leader = fixedPeerWaitCatalogLeaderV1(t, ctx, runtimes[:3])
	membership, err := client.PrepareReplicaReplacementV1(ctx, configs[leader].NodeID, operation)
	if !rootpublication.StableRelativeNamespaceSupported() {
		if err == nil || runtimes[3].localDataV1(group.ID) != nil {
			t.Fatalf("unsupported namespace admitted seeded target: %v", err)
		}
		return
	}
	if err != nil {
		t.Logf("replacement Prepare failure: catalog_leader=%s data_source=%s group=%s raft_timeout=%s request_timeout=%s ctx_err=%v", configs[leader].NodeID, sourceNode, group.ID, configs[0].RaftTimeout, configs[0].RequestTimeout, ctx.Err())
		for _, runtime := range runtimes[:3] {
			if runtime.meta == nil {
				t.Logf("replacement catalog failure: node=%s meta=nil closed=%t", runtime.config.NodeID, runtime.closed.Load())
				continue
			}
			catalog, known := runtime.authority.Status()
			t.Logf("replacement catalog failure: node=%s closed=%t runtime=%+v catalog=%+v catalog_known=%t read_stats=%+v", runtime.config.NodeID, runtime.closed.Load(), runtime.meta.RuntimeStatusV1(), catalog, known, runtime.meta.CatalogMetaLinearizableReadStatsV1())
		}
		t.Fatalf("prepare actual nonvoter: %v", err)
	}
	if reservedTargetRaft == nil || fixedPeerRawTestListenerV1(runtimes[3].localDataV1(group.ID).stream.Listener) != reservedTargetRaft {
		t.Fatal("future Raft reservation did not reach enrolled target transport")
	}
	admission := runtimes[3].client.peerTransport.admission
	admission.mu.Lock()
	raftScope := admission.scopes["raft:"+string(group.ID)]
	admitted := raftScope != nil && raftScope.reserved == (peerResourceAmountsV1{4, 1, 4 << 20})
	admission.mu.Unlock()
	if !admitted {
		t.Fatal("public replacement omitted node-wide hosted Raft reserve")
	}
	voters, learners := 0, 0
	for _, member := range membership.Members {
		if member.Voter {
			voters++
		} else {
			learners++
		}
		if member.ID == spare.ID && (member.Voter || member.Address != operation.NewPeer.Address) {
			t.Fatal("fresh target is not the exact nonvoter")
		}
	}
	if voters != 3 || learners != 1 || membership.ConfigurationIndex == 0 || membership.CommitIndex < membership.ConfigurationIndex {
		t.Fatalf("actual committed membership=%+v", membership)
	}
	committed, err := runtimes[leader].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || committed.Phase != raftplacement.ReplicaReplacementAddIntentV1 || committed.Seed == nil {
		t.Fatalf("missing installed seed/add intent: %+v %v", committed, err)
	}
	target := runtimes[3].localDataV1(group.ID)
	if target == nil || target.prejoin == nil || target.replacementReceiver == nil {
		t.Fatal("target lacks native receiver owner")
	}
	target.prejoin.mu.Lock()
	phase, verified := target.prejoin.phase, target.prejoin.verified
	target.prejoin.mu.Unlock()
	if phase != replacementReceiverAddIntentV1 || !verified {
		t.Fatalf("unverified receiver enrolled: %s %v", phase, verified)
	}
	if _, err := client.call(ctx, committed.Seed.SourceNodeID, "replacement-install", fixedPeerRequestV1{Entry: begin}, true); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("seed replay permitted after add intent: %v", err)
	}
	// Restart restores the durable quarantine/cutoff record rather than deriving
	// authority from the new request. The fixed manifest remains byte-identical.
	if err := runtimes[3].Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := fixedPeerOpenTestRuntimeV1(t, configs[3])
	if err != nil {
		t.Fatal(err)
	}
	runtimes[3] = restarted
	t.Cleanup(func() {
		if err := restarted.Close(); err != nil {
			t.Error(err)
		}
	})
	repeated, err := client.PrepareReplicaReplacementV1(ctx, configs[leader].NodeID, operation)
	if err != nil || repeated.ConfigurationIndex != membership.ConfigurationIndex {
		t.Fatalf("exact prepare retry changed config: %+v %v", repeated, err)
	}
	if runtimes[3].localDataV1(group.ID) == nil {
		t.Fatal("target did not open the operation-authorized group")
	}
	readiness, readyErr := runtimes[3].ReadinessV1(ctx)
	if readyErr == nil || readiness.Ready {
		t.Fatalf("unverified replacement reported ready: %+v %v", readiness, readyErr)
	}
	dataLeader, err := client.leader(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	for _, runtime := range runtimes[:3] {
		if runtime.config.NodeID != dataLeader {
			continue
		}
		for _, peer := range group.Peers {
			if peer.ID == operation.OldNodeID {
				continue
			}
			_, err := runtime.localDataV1(group.ID).provider.AddReplacementNonvoterV1(ctx, operation.OldNodeID, peer, membership.ConfigurationIndex)
			if !errors.Is(err, raftcluster.ErrInvalidConfig) {
				t.Fatalf("existing voter admitted as learner: %v", err)
			}
			break
		}
	}
	unknown := operation
	unknown.NewPeer.ID = "not-in-global-node-anchor"
	if _, err := client.PrepareReplicaReplacementV1(ctx, configs[leader].NodeID, unknown); err == nil {
		t.Fatal("unknown global node admitted")
	}
	if promote {
		testReplacementPromotionTailV1(t, ctx, client, configs, runtimes, leader, group, operation, membership)
	}
	if complete {
		testReplacementCompleteAndSequentialV1(t, ctx, client, configs, runtimes, group, operation)
		return
	}
	// Seeded nonvoter enrollment cannot publish a routing/ownership change.
	// Promotion also retains every original voter and leaves catalog routing unchanged.
	current, err := runtimes[leader].authority.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	var currentRecord raftplacement.CatalogMetaRecordV1
	if err := json.Unmarshal(current.Record, &currentRecord); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(currentRecord.Catalog, record.Catalog) {
		t.Fatal("BEGIN changed members or placements before recovery")
	}
}

func fixedPeerRuntimeWithBlockedSnapshotV1(t *testing.T) (*FixedPeerTCPRuntimeV1, raftcluster.RaftSnapshotV1, string) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("snapshot requires relative namespace support")
	}
	c := fixedPeerTestConfigsV1(t)[0]
	c.Nodes, c.Catalog.Peers, c.Groups = c.Nodes[:1], c.Catalog.Peers[:1], c.Groups[:1]
	r, err := fixedPeerOpenTestRuntimeV1(t, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	client, err := NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := client.Status(ctx, c.NodeID)
		return err == nil && status.CatalogRaft.State == "Leader" && len(status.Groups) == 1 && status.Groups[0].State == "Leader"
	})
	catalog := raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{c.NodeID}}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"}, GroupID: "group-a"}}}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, c.NodeID, command); err != nil {
		t.Fatal(err)
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "users", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, c.NodeID, request)
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Status(ctx, c.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	if _, err := client.Submit(ctx, c.NodeID, fixedPeerCreateEntryV1(t, "users", status.Groups[0].CatalogVersion), metadata); err != nil {
		t.Fatal(err)
	}
	snapshot, err := r.localDataV1("group-a").fsm.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(snapshot.ArchivePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(snapshot.ArchivePath, 0700); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(snapshot.ArchivePath, "blocked")
	if err := os.WriteFile(blocker, []byte("retain cleanup debt"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(blocker); _ = snapshot.Release() })
	return r, snapshot, blocker
}

// Runtime shutdown remains once-only for consensus, transports and stores, but
// it must retain and retry the FSM's failed archive cleanup on later Close calls.
func TestFixedPeerRuntimeCloseRetriesSnapshotCleanupV1(t *testing.T) {
	r, snapshot, blocker := fixedPeerRuntimeWithBlockedSnapshotV1(t)
	first := r.Close()
	if first == nil {
		t.Fatal("cleanup failure lost by runtime Close")
	}
	var cleanupErr *os.PathError
	if !errors.As(first, &cleanupErr) || cleanupErr.Path != snapshot.ArchivePath {
		t.Fatalf("first Close error=%v, want archive cleanup failure", first)
	}
	if _, err := os.Stat(blocker); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); !errors.Is(err, cleanupErr) {
		t.Fatalf("cleanup retry lost first Close error: %v", err)
	}
	if _, err := os.Stat(snapshot.ArchivePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup target remains: %v", err)
	}
	if err := r.Close(); !errors.Is(err, cleanupErr) {
		t.Fatalf("idempotent Close lost first error: %v", err)
	}
}

func TestFixedPeerRuntimeClosePreservesFirstFSMErrorAfterInternalRetryV1(t *testing.T) {
	r, snapshot, blocker := fixedPeerRuntimeWithBlockedSnapshotV1(t)
	// FSM.Close first fails to remove the nonempty archive. The original DB's
	// close hook then clears the blocker, so the runtime's internal FSM retry
	// succeeds during this same Close call.
	r.localDataV1("group-a").db.RegisterCloseHook(func() error { return os.Remove(blocker) })
	first := r.Close()
	var cleanupErr *os.PathError
	if !errors.As(first, &cleanupErr) || cleanupErr.Path != snapshot.ArchivePath {
		t.Fatalf("first Close error=%v, want archive cleanup failure", first)
	}
	if _, err := os.Stat(snapshot.ArchivePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("internal retry left cleanup target: %v", err)
	}
	if err := r.Close(); !errors.Is(err, cleanupErr) {
		t.Fatalf("later Close lost first error: %v", err)
	}
}

// Status and leader hints are observations; fixture mutations start only after
// an existing quorum/current-term read fence succeeds. No mutation is retried.
func fixedPeerWaitCatalogLeaderV1(t testing.TB, ctx context.Context, runtimes []*FixedPeerTCPRuntimeV1) int {
	t.Helper()
	leader := -1
	fixedPeerWaitV1(t, ctx, func() bool {
		for i, runtime := range runtimes {
			if runtime.meta == nil || runtime.closed.Load() {
				continue
			}
			status := runtime.meta.RuntimeStatusV1()
			if status.State != "Leader" || status.LeaderID != runtime.config.NodeID {
				continue
			}
			if _, err := runtime.meta.LinearizableCatalogMetaReadProofV1(ctx); err != nil {
				continue
			}
			// Peer preparation also discovers this leader from each live voter's
			// hint. Wait for those observations after election/restart, too.
			agree := true
			for _, peer := range runtimes {
				if peer.meta != nil && !peer.closed.Load() && peer.meta.RuntimeStatusV1().LeaderID != runtime.config.NodeID {
					agree = false
				}
			}
			if agree {
				leader = i
				return true
			}
		}
		return false
	})
	return leader
}

func fixedPeerWaitDataLeaderV1(t testing.TB, ctx context.Context, runtimes []*FixedPeerTCPRuntimeV1, group FixedPeerTCPGroupV1) raftcluster.NodeID {
	t.Helper()
	var leader raftcluster.NodeID
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, runtime := range runtimes {
			local := runtime.localDataV1(group.ID)
			if runtime.closed.Load() || local == nil || local.provider == nil {
				continue
			}
			status, err := local.provider.RuntimeStatusV1(ctx)
			if err != nil || status.State != "Leader" || status.LeaderID != runtime.config.NodeID {
				continue
			}
			committed, err := local.provider.CommittedConfigurationV1(ctx)
			if err == nil && committed.GroupID == group.ID && committed.LeaderID == runtime.config.NodeID && len(committed.Members) == len(group.Peers) {
				leader = runtime.config.NodeID
				return true
			}
		}
		return false
	})
	return leader
}
