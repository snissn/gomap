package nativewire

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestReplacementCompletesOfflineOldAndSequentialSourceV1(t *testing.T) {
	testReplacementPublicInstallV1(t, false, true, true)
}

func TestReplacementRequiresSelectedOldSourcePreparationV1(t *testing.T) {
	for _, phase := range []raftplacement.ReplicaReplacementPhaseV1{raftplacement.ReplicaReplacementBegunV1, raftplacement.ReplicaReplacementSeededV1} {
		t.Run(string(phase), func(t *testing.T) {
			begin := replacementReceiverTestBeginV1(t)
			state := raftplacement.ReplicaReplacementStateV1{Begin: begin, Phase: phase}
			if phase == raftplacement.ReplicaReplacementSeededV1 {
				state.Seed = &raftcluster.ReplacementSnapshotSeedV1{SourceNodeID: begin.OldNodeID}
			}
			var oldPrepared atomic.Bool
			var sourceWork atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				id := raftcluster.NodeID(request.Header.Get("X-TreeDB-Node"))
				reply := fixedPeerReplyV1{NodeID: id, ConfigDigest: begin.ConfigDigest}
				switch request.URL.Path {
				case "/v1/replacement-begin":
				case "/v1/replacement-read":
					reply.ReplacementState = &state
				case "/v1/status":
					reply.Status.CatalogRaft = raftcluster.RuntimeStatusV1{GroupID: begin.GroupID, LeaderID: begin.OldNodeID}
				case "/v1/replacement-prepare":
					if id == begin.OldNodeID && !oldPrepared.Load() {
						reply.Error, reply.ErrorCode = raftcluster.ErrAdmissionUnavailable.Error(), raftcluster.ErrAdmissionUnavailable.Error()
					}
				case "/v1/replacement-receiver":
				case "/v1/replacement-seed", "/v1/replacement-install":
					sourceWork.Add(1)
					reply.Error, reply.ErrorCode = raftcluster.ErrAdmissionUnavailable.Error(), raftcluster.ErrAdmissionUnavailable.Error()
				default:
					t.Errorf("unexpected request %s", request.URL.Path)
				}
				if err := json.NewEncoder(w).Encode(reply); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			address := strings.TrimPrefix(server.URL, "http://")
			group := FixedPeerTCPGroupV1{ID: begin.GroupID, Peers: []raftcluster.Peer{{ID: begin.OldNodeID, Address: "127.0.0.1:7001"}, {ID: "survivor", Address: "127.0.0.1:7003"}}}
			client := &FixedPeerTCPClientV1{config: FixedPeerTCPConfigV1{RequestTimeout: time.Second, Groups: []FixedPeerTCPGroupV1{group}}, digest: begin.ConfigDigest, http: server.Client(), readHTTP: server.Client(), addresses: map[raftcluster.NodeID]string{begin.OldNodeID: address, "survivor": address, begin.NewPeer.ID: address}, calls: make(chan struct{}, 4), readCalls: make(chan struct{}, 4)}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := client.PrepareReplicaReplacementV1(ctx, begin.OldNodeID, begin); err == nil || !strings.Contains(err.Error(), "prepare") {
				t.Fatalf("unprepared source error=%v", err)
			}
			if got := sourceWork.Load(); got != 0 {
				t.Fatalf("unprepared source started %d seed/install workers", got)
			}
			oldPrepared.Store(true)
			_, _ = client.PrepareReplicaReplacementV1(ctx, begin.OldNodeID, begin)
			if got := sourceWork.Load(); got != 1 {
				t.Fatalf("same operation did not retry source work after preparation: %d", got)
			}
		})
	}
}

func TestReplacementBeginRefusesUnreconstructablePeerCapabilitiesV1(t *testing.T) {
	begin := replacementReceiverTestBeginV1(t)
	defaults := raftcluster.DefaultFeatureSet()
	base := raftcluster.Config{Dir: t.TempDir(), ClusterDir: t.TempDir(), NodeID: begin.OldNodeID, GroupID: begin.GroupID,
		Peers: []raftcluster.Peer{{ID: begin.OldNodeID, Address: "127.0.0.1:7001"}, {ID: "survivor", Address: "127.0.0.1:7003"}}, Features: defaults}
	resolved, err := raftcluster.Validate(base)
	if err != nil {
		t.Fatal(err)
	}
	r := &FixedPeerTCPRuntimeV1{config: FixedPeerTCPConfigV1{Groups: []FixedPeerTCPGroupV1{{ID: begin.GroupID, Peers: resolved.Peers, Features: resolved.Features}}},
		client: &FixedPeerTCPClientV1{digest: begin.ConfigDigest, addresses: map[raftcluster.NodeID]string{begin.NewPeer.ID: begin.NewPeer.Address}}}
	if _, err := r.validateReplacementBeginV1(begin); err != nil {
		t.Fatalf("normalized default peers refused: %v", err)
	}
	// The ordinary runtime accepts a survivor with an additional supported
	// capability, but an ID/address-only completed roster cannot rederive it.
	base.Peers[1].Capabilities = raftcluster.FeatureSet{ConfigVersion: defaults.ConfigVersion, Required: []raftcluster.RequiredFeature{
		defaults.Required[0], {Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
	}}
	resolved, err = raftcluster.Validate(base)
	if err != nil {
		t.Fatalf("ordinary runtime rejected supported peer capability: %v", err)
	}
	r.config.Groups[0].Peers = resolved.Peers
	if _, err := r.validateReplacementBeginV1(begin); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
		t.Fatalf("nondefault peer accepted before BEGIN: %v", err)
	}
	base.Peers[0].Capabilities = base.Peers[1].Capabilities
	base.Features = base.Peers[1].Capabilities
	resolved, err = raftcluster.Validate(base)
	if err != nil {
		t.Fatalf("ordinary runtime rejected supported group feature: %v", err)
	}
	r.config.Groups[0].Peers, r.config.Groups[0].Features = resolved.Peers, resolved.Features
	if _, err := r.validateReplacementBeginV1(begin); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
		t.Fatalf("nondefault group accepted before BEGIN: %v", err)
	}
}

func TestReplacementCompletionReturnsChangedCatalogLeaderV1(t *testing.T) {
	const old, next, target = raftcluster.NodeID("old"), raftcluster.NodeID("next"), raftcluster.NodeID("target")
	digest := strings.Repeat("a", 64)
	var oldCompletions, nextCompletions atomic.Int32
	var begin raftplacement.ReplicaReplacementBeginV1
	var promoted, completed raftplacement.ReplicaReplacementStateV1
	membership := raftcluster.CommittedRaftConfigurationV1{GroupID: "data", LeaderID: next, Term: 3, CommitIndex: 12, ConfigurationIndex: 12}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		id := raftcluster.NodeID(request.Header.Get("X-TreeDB-Node"))
		reply := fixedPeerReplyV1{NodeID: id, ConfigDigest: digest}
		switch request.URL.Path {
		case "/v1/replacement-read":
			reply.ReplacementState = &promoted
		case "/v1/replacement-prepare", "/v1/replacement-removal-intent", "/v1/replacement-reconcile":
		case "/v1/replacement-complete":
			if id == old {
				oldCompletions.Add(1)
				reply.Error, reply.ErrorCode = raftcluster.ErrNotLeader.Error(), raftcluster.ErrNotLeader.Error()
			} else if id == next {
				nextCompletions.Add(1)
				reply.ReplacementState, reply.Membership = &completed, &membership
			} else {
				t.Errorf("unexpected completion recipient %s", id)
			}
		default:
			t.Errorf("unexpected request %s", request.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Error(err)
		}
	})
	servers := []*httptest.Server{httptest.NewServer(handler), httptest.NewServer(handler), httptest.NewServer(handler)}
	for _, server := range servers {
		defer server.Close()
	}
	addresses := map[raftcluster.NodeID]string{old: strings.TrimPrefix(servers[0].URL, "http://"), next: strings.TrimPrefix(servers[1].URL, "http://"), target: strings.TrimPrefix(servers[2].URL, "http://")}
	begin = raftplacement.ReplicaReplacementBeginV1{OperationID: "leader-change", ConfigDigest: digest, ExpectedEpoch: 1, CatalogDigest: digest, GroupID: "data", OldNodeID: old, NewPeer: raftcluster.Peer{ID: target, Address: addresses[target]}}
	peers := []raftcluster.Peer{{ID: old, Address: addresses[old]}, {ID: next, Address: addresses[next]}}
	promoted = raftplacement.ReplicaReplacementStateV1{Begin: begin, Phase: raftplacement.ReplicaReplacementPromotedV1}
	completed = raftplacement.ReplicaReplacementStateV1{Begin: begin, Phase: raftplacement.ReplicaReplacementCompletedV1, RemovalIndex: 11, Result: &raftplacement.ReplicaReplacementResultV1{ConfigurationIndex: membership.ConfigurationIndex}, Peers: []raftcluster.Peer{peers[1], begin.NewPeer}}
	membership.Members = []raftcluster.RaftMemberV1{{ID: next, Address: addresses[next], Voter: true}, {ID: target, Address: addresses[target], Voter: true}}
	if err := validateReplacementFinalMembershipV1(FixedPeerTCPGroupV1{ID: "data", Peers: completed.Peers}, membership, completed); err != nil {
		t.Fatal(err)
	}
	client := &FixedPeerTCPClientV1{config: FixedPeerTCPConfigV1{RequestTimeout: time.Second, Groups: []FixedPeerTCPGroupV1{{ID: "data", Peers: peers}}}, digest: digest, http: servers[0].Client(), readHTTP: servers[0].Client(), addresses: addresses, calls: make(chan struct{}, 4), readCalls: make(chan struct{}, 4)}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.CompleteReplicaReplacementV1(ctx, old, begin); !errors.Is(err, raftcluster.ErrNotLeader) {
		t.Fatalf("changed leader was hidden by fixed-address retry: %v", err)
	}
	if ctx.Err() != nil || oldCompletions.Load() != 1 || nextCompletions.Load() != 0 {
		t.Fatalf("automatic retry crossed leader boundary: old=%d next=%d context=%v", oldCompletions.Load(), nextCompletions.Load(), ctx.Err())
	}
	result, err := client.CompleteReplicaReplacementV1(ctx, next, begin)
	if err != nil || result.ConfigurationIndex != membership.ConfigurationIndex || nextCompletions.Load() != 1 {
		t.Fatalf("exact-operation retry at new leader: %+v %v", result, err)
	}
}

func testReplacementCompleteAndSequentialV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1, configs []FixedPeerTCPConfigV1, runtimes []*FixedPeerTCPRuntimeV1, group FixedPeerTCPGroupV1, operation raftplacement.ReplicaReplacementBeginV1) {
	t.Helper()
	catalogLeader := func() raftcluster.NodeID {
		t.Helper()
		var result raftcluster.NodeID
		fixedPeerWaitV1(t, ctx, func() bool {
			// Discovery returns a follower's leader hint, which can still name
			// the just-stopped catalog voter. Wait for a live leader and its
			// committed catalog fence before beginning the next operation.
			for _, peer := range configs[0].Catalog.Peers {
				status, err := client.Status(ctx, peer.ID)
				if err != nil || status.CatalogRaft.State != "Leader" || status.CatalogRaft.LeaderID != peer.ID {
					continue
				}
				_, err = client.call(ctx, peer.ID, "catalog-read", fixedPeerRequestV1{}, false)
				if err == nil {
					result = peer.ID
					return true
				}
			}
			return false
		})
		return result
	}
	find := func(id raftcluster.NodeID) int {
		for i, cfg := range configs {
			if cfg.NodeID == id {
				return i
			}
		}
		t.Fatalf("unknown node %s", id)
		return -1
	}
	restart := func(i int) {
		t.Helper()
		if err := runtimes[i].Close(); err != nil {
			t.Fatal(err)
		}
		runtime, err := OpenFixedPeerTCPRuntimeV1(configs[i])
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
	leader := catalogLeader()
	complete := func(command raftplacement.ReplicaReplacementBeginV1) (raftcluster.CommittedRaftConfigurationV1, error) {
		t.Helper()
		for {
			leader = catalogLeader()
			result, err := client.CompleteReplicaReplacementV1(ctx, leader, command)
			if err == nil || !errors.Is(err, raftcluster.ErrNotLeader) && !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
				return result, err
			}
			// A new leader must re-prove this same committed operation. Conflicts,
			// ambiguous mutations, and admission failures are never retried here.
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return raftcluster.CommittedRaftConfigurationV1{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	first, err := runtimes[find(leader)].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	source := first.Seed.SourceNodeID
	if source == operation.OldNodeID {
		t.Fatal("fixture must retain seed source for next operation")
	}
	// Removal must re-prove the surviving target, even after promotion. The
	// original voters remain available while this target is offline.
	before, err := runtimes[find(source)].localDataV1(group.ID).provider.CommittedConfigurationV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtimes[3].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompleteReplicaReplacementV1(ctx, leader, operation); err == nil {
		t.Fatal("removal accepted offline surviving target")
	}
	unchanged, err := runtimes[find(leader)].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || unchanged.Phase != raftplacement.ReplicaReplacementPromotedV1 {
		t.Fatalf("offline-target removal changed phase %+v %v", unchanged, err)
	}
	after, err := runtimes[find(source)].localDataV1(group.ID).provider.CommittedConfigurationV1(ctx)
	if err != nil || after.ConfigurationIndex != before.ConfigurationIndex {
		t.Fatalf("offline-target removal changed native configuration %+v %v", after, err)
	}
	oldVoter := false
	for _, member := range after.Members {
		oldVoter = oldVoter || member.ID == operation.OldNodeID && member.Voter
	}
	if !oldVoter {
		t.Fatal("offline-target refusal lost old voter")
	}
	restart(3)
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(operation)
	if err != nil {
		t.Fatal(err)
	}
	// Reopen the promoted target before taking the old voter offline. Its
	// native replica must be available to preserve the surviving quorum.
	if _, err := client.call(ctx, operation.NewPeer.ID, "replacement-prepare", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		t.Fatalf("reopen surviving target: %v", err)
	}
	old := find(operation.OldNodeID)
	t.Logf("offline-old fixture: old=%s catalog leader=%s seed source=%s", operation.OldNodeID, leader, source)
	// Keep this replica offline through native removal and catalog completion.
	// Its disk still has the pre-removal group state when it restarts below.
	if err := runtimes[old].Close(); err != nil {
		t.Fatal(err)
	}
	completed, err := complete(operation)
	if err != nil {
		t.Fatalf("offline-old completion: %v", err)
	}
	if leader == operation.OldNodeID {
		t.Fatal("stopped catalog voter selected as live leader")
	}
	if len(completed.Members) != len(group.Peers) {
		t.Fatalf("completion roster %+v", completed)
	}
	for _, member := range completed.Members {
		if member.ID == operation.OldNodeID || !member.Voter {
			t.Fatalf("unsafe final member %+v", member)
		}
	}
	state, err := runtimes[find(leader)].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || state.Phase != raftplacement.ReplicaReplacementCompletedV1 || state.Result.ConfigurationIndex != completed.ConfigurationIndex {
		t.Fatalf("terminal state %+v %v", state, err)
	}
	for _, peer := range state.Peers {
		if peer.Capabilities.ConfigVersion != (raftcluster.Version{}) || len(peer.Capabilities.Required) != 0 {
			t.Fatalf("durable roster retained runtime-only capabilities %+v", peer)
		}
	}
	withCapabilities := state
	withCapabilities.Peers = append([]raftcluster.Peer(nil), state.Peers...)
	withCapabilities.Peers[0].Capabilities = raftcluster.DefaultFeatureSet()
	if _, err := raftplacement.EncodeReplicaReplacementStateV1(withCapabilities); !errors.Is(err, raftplacement.ErrInvalidCatalogMeta) {
		t.Fatalf("completed state accepted unbound capabilities: %v", err)
	}
	group.Peers = state.Peers
	repeated, err := complete(operation)
	if err != nil || repeated.ConfigurationIndex != completed.ConfigurationIndex {
		t.Fatalf("terminal retry %+v %v", repeated, err)
	}
	restart(old)
	if ready, err := runtimes[old].ReadinessV1(ctx); err == nil || ready.Ready {
		t.Fatalf("offline retired replica ready %+v %v", ready, err)
	}
	// A restarted dynamic member requires fresh committed authority to reopen.
	restart(3)
	if ready, err := runtimes[3].ReadinessV1(ctx); err == nil || ready.Ready {
		t.Fatalf("unopened current member ready %+v %v", ready, err)
	}
	if _, err := complete(operation); err != nil {
		t.Fatalf("completed target restart: %v", err)
	}
	fixedPeerWaitV1(t, ctx, func() bool { ready, err := runtimes[3].ReadinessV1(ctx); return err == nil && ready.Ready })
	// The same source owns a second real seed. Completion of the first operation
	// cannot require that source to delete its old seed synchronously.
	dataLeader, err := client.leader(ctx, group)
	if err != nil || dataLeader != source {
		t.Fatalf("same-source fixture leader %s want %s: %v", dataLeader, source, err)
	}
	sourceData := runtimes[find(source)].localDataV1(group.ID)
	if err := sourceData.provider.ReplacementSnapshotReadyV1(ctx); !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
		t.Fatalf("configuration-only boundary unexpectedly snapshot-ready: %v", err)
	}
	retained, err := runtimes[find(source)].deriveReplacementSeedV1(ctx, operation, sourceData)
	if err != nil || retained == nil || !raftcluster.SameReplacementSnapshotSeedV1(*retained, *first.Seed) {
		t.Fatalf("retained seed was not an exact retry source across the configuration gap: %+v %v", retained, err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	second := operation
	second.OperationID += "-second"
	second.ExpectedEpoch, second.CatalogDigest = state.Result.Epoch, state.Result.Digest
	second.OldNodeID, second.NewPeer = source, raftcluster.Peer{ID: configs[4].NodeID, Address: address}
	secondRaw, err := raftplacement.EncodeReplicaReplacementBeginV1(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.call(ctx, leader, "replacement-begin", fixedPeerRequestV1{Entry: secondRaw}, true); err != nil {
		t.Fatal(err)
	}
	// Configuration-only logs do not advance TreeDB's native FSM snapshot
	// boundary. An idle second replacement must refuse before caching a seed
	// worker; its exact BEGIN can be retried after real committed document work.
	if _, err := client.call(ctx, source, "replacement-seed", fixedPeerRequestV1{Entry: secondRaw}, true); !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
		t.Fatalf("idle second seed was not a typed refusal: %v", err)
	}
	if work := runtimes[find(source)].localDataV1(group.ID).replacementWork.work; work != nil {
		t.Fatalf("idle refusal cached native seed work: %+v", work)
	}
	// Interrupt immediately after BEGIN2: no seed exists yet, and BEGIN1 has
	// been compacted out of the catalog. The current survivor must reopen from
	// its durable AddIntent receiver before a seed-producing quorum is needed.
	restart(3)
	if ready, err := runtimes[3].ReadinessV1(ctx); err == nil || ready.Ready {
		t.Fatalf("unopened compacted-history survivor ready %+v %v", ready, err)
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "users", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, leader, request)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	// BEGIN2 is committed at the catalog leader, but this data source may
	// still be applying it. A routed mutation must retain its single attempt:
	// wait until the source can prove the exact route catalog generation.
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := runtimes[find(source)].catalogFence(ctx)
		return err == nil && status.Epoch == route.CatalogMetaEpoch && status.Digest == route.CatalogMetaDigest
	})
	sourceData = runtimes[find(source)].localDataV1(group.ID)
	version, _, err := sourceData.fsm.CurrentCatalogVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	guard := []iwire.Section{{ID: iwire.SectionIdempotencyKey, Bytes: []byte("between-replacements")}, {ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)}}
	body, err := appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON, [][]byte{[]byte("between-replacements")}, [][]byte{[]byte(`{"value":"between-replacements"}`)}, AckRaftCommitted, 0, guard)
	if err != nil {
		t.Fatal(err)
	}
	sections, err := iwire.DecodeSections(body, iwire.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Submit(ctx, source, entry, metadata); err != nil {
		t.Fatalf("post-configuration document command: %v", err)
	}
	fixedPeerWaitV1(t, ctx, func() bool { return sourceData.provider.ReplacementSnapshotReadyV1(ctx) == nil })
	if _, err := client.PrepareReplicaReplacementV1(ctx, leader, second); err != nil {
		t.Fatalf("second real seed/enrollment: %v", err)
	}
	secondState, err := runtimes[find(leader)].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || secondState.Seed == nil || secondState.Seed.SourceNodeID != source || secondState.Seed.SnapshotID == first.Seed.SnapshotID {
		t.Fatalf("second seed %+v %v", secondState, err)
	}
	if _, err := client.PromoteReplicaReplacementV1(ctx, leader, second); err != nil {
		t.Fatalf("second promotion: %v", err)
	}
	fixedPeerWaitV1(t, ctx, func() bool { ready, err := runtimes[3].ReadinessV1(ctx); return err == nil && ready.Ready })
	// This time the old replica is leader. Completion must transfer leadership
	// before its native removal, then reconcile through the new leader.
	dataLeader, err = client.leader(ctx, group)
	if err != nil || dataLeader != source {
		t.Fatalf("old-leader fixture %s %v", dataLeader, err)
	}
	secondFinal, err := complete(second)
	if err != nil {
		t.Fatalf("old-leader completion: %v", err)
	}
	terminal, err := runtimes[find(leader)].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	group.Peers = terminal.Peers
	dataLeader, err = client.leader(ctx, group)
	if err != nil || dataLeader != second.NewPeer.ID {
		t.Fatalf("new target leadership %s %v", dataLeader, err)
	}
	if secondFinal.ConfigurationIndex != terminal.Result.ConfigurationIndex {
		t.Fatal("native result changed")
	}
	request = ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "users", Shape: ClusterRouteShapeCollection}
	route, err = client.Route(ctx, leader, request)
	if err != nil {
		t.Fatal(err)
	}
	metadata = ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	d := runtimes[4].localDataV1(group.ID)
	version, _, err = d.fsm.CurrentCatalogVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Normal ingress uses dispatcher-validated current IDs, including the new
	// leader that never appeared in the original fixed group manifest.
	guard = []iwire.Section{{ID: iwire.SectionIdempotencyKey, Bytes: []byte("after-second-replacement")}, {ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)}}
	body, err = appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatJSON, [][]byte{[]byte("replacement-row")}, [][]byte{[]byte(`{"value":"after-replacement"}`)}, AckRaftCommitted, 0, guard)
	if err != nil {
		t.Fatal(err)
	}
	sections, err = iwire.DecodeSections(body, iwire.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	validated, err = iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err = iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Submit(ctx, operation.OldNodeID, entry, metadata); err == nil {
		t.Fatal("retired member accepted routed write")
	}
	if _, err := client.Submit(ctx, configs[3].NodeID, entry, metadata); err != nil {
		t.Fatalf("ordinary current-roster forwarding to new leader: %v", err)
	}
	if ready, err := runtimes[find(source)].ReadinessV1(ctx); err == nil || ready.Ready {
		t.Fatalf("second retired leader ready %+v %v", ready, err)
	}
	newAddress := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		return address
	}
	// Retire a formerly dynamic member, not one from the fixed RaftListen map.
	// Compact that removal with BEGIN4 before reopening its old receiver data.
	third := second
	third.OperationID = operation.OperationID + "-third"
	third.ExpectedEpoch, third.CatalogDigest = terminal.Result.Epoch, terminal.Result.Digest
	third.OldNodeID, third.NewPeer = configs[3].NodeID, raftcluster.Peer{ID: configs[5].NodeID, Address: newAddress()}
	if _, err := client.PrepareReplicaReplacementV1(ctx, leader, third); err != nil {
		t.Fatalf("dynamic retirement enrollment: %v", err)
	}
	if _, err := client.PromoteReplicaReplacementV1(ctx, leader, third); err != nil {
		t.Fatalf("dynamic retirement promotion: %v", err)
	}
	if _, err := complete(third); err != nil {
		t.Fatalf("dynamic retirement completion: %v", err)
	}
	thirdState, err := runtimes[find(leader)].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	fourth := third
	fourth.OperationID = operation.OperationID + "-fourth"
	fourth.ExpectedEpoch, fourth.CatalogDigest = thirdState.Result.Epoch, thirdState.Result.Digest
	fourth.OldNodeID, fourth.NewPeer = configs[4].NodeID, raftcluster.Peer{ID: configs[6].NodeID, Address: newAddress()}
	fourthRaw, err := raftplacement.EncodeReplicaReplacementBeginV1(fourth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.call(ctx, leader, "replacement-begin", fixedPeerRequestV1{Entry: fourthRaw}, true); err != nil {
		t.Fatal(err)
	}
	restart(3)
	marker := runtimes[3].localDataV1(group.ID)
	if marker == nil || marker.startErr != errReplacementReceiverUnopenedV1 || marker.provider != nil {
		t.Fatalf("retired dynamic receiver lost unopened marker: %+v", marker)
	}
	if ready, err := runtimes[3].ReadinessV1(ctx); err == nil || ready.Ready {
		t.Fatalf("retired dynamic replica became ready after compacted restart: %+v %v", ready, err)
	}
	route, err = client.Route(ctx, leader, request)
	if err != nil {
		t.Fatal(err)
	}
	ApplyClusterRouteMetadata(&metadata, request, route)
	if _, err := client.Submit(ctx, configs[3].NodeID, entry, metadata); err == nil {
		t.Fatal("retired dynamic replica became a routing gateway after restart")
	}
}

func TestReplacementFinalMembershipRejectsChangedRosterV1(t *testing.T) {
	group := FixedPeerTCPGroupV1{ID: "g", Peers: []raftcluster.Peer{{ID: "a", Address: "a:1"}, {ID: "b", Address: "b:1"}}}
	configuration := raftcluster.CommittedRaftConfigurationV1{GroupID: "g", ConfigurationIndex: 7, Members: []raftcluster.RaftMemberV1{{ID: "a", Address: "a:1", Voter: true}, {ID: "b", Address: "b:1", Voter: true}}}
	state := raftplacement.ReplicaReplacementStateV1{RemovalIndex: 6, Result: &raftplacement.ReplicaReplacementResultV1{ConfigurationIndex: 7}}
	if err := validateReplacementFinalMembershipV1(group, configuration, state); err != nil {
		t.Fatal(err)
	}
	configuration.ConfigurationIndex++
	if err := validateReplacementFinalMembershipV1(group, configuration, state); err == nil {
		t.Fatal("later identical roster substituted for immutable terminal index")
	}
	configuration.ConfigurationIndex--
	configuration.Members[1].Address = "foreign:1"
	if err := validateReplacementFinalMembershipV1(group, configuration, state); err == nil {
		t.Fatal("changed survivor address accepted")
	}
	configuration.Members[1].Address, configuration.Members[1].Voter = "b:1", false
	if err := validateReplacementFinalMembershipV1(group, configuration, state); err == nil {
		t.Fatal("nonvoter final member accepted")
	}
}
