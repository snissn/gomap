package nativewire

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestImmutableOwnerReplacementPrivateANNQualificationV1(t *testing.T) {
	testImmutableOwnerReplacementPrivateQualificationV1(t, true, true, false)
}

func replacementOwnerQualificationRequestV1(t *testing.T, ctx context.Context, target *FixedPeerTCPRuntimeV1, command raftplacement.ReplicaReplacementBeginV1) VectorPartitionShardSearchRequestV1 {
	t.Helper()
	_, record, err := target.replacementOwnerCatalogV1(ctx, command, target.localDataV1(command.GroupID))
	if err != nil {
		t.Fatal(err)
	}
	identity := command.OwnerPreparation
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	request.Database, request.Catalog, request.Collection = identity.Index.Collection.Database, identity.Index.Collection.Catalog, identity.Index.Collection.Collection
	request.IndexName, request.IndexDefinitionDigest = identity.Index.IndexName, identity.Index.IndexDefinitionDigest
	request.SourceGeneration, request.SourceChecksum, request.SourceSchemaHash, request.SourceRowCount = identity.Source.Generation, identity.Source.Checksum, identity.Source.SchemaHash, identity.Source.RowCount
	request.PartitionGeneration, request.RouterGeneration, request.ReadySetDigest = identity.Generation, identity.Generation, record.ReadySetDigest
	request.TargetGroupID, request.TargetNodeID = command.GroupID, command.NewPeer.ID
	return request
}

func assertReplacementOwnerQualificationParityV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1, target, oldOwner *FixedPeerTCPRuntimeV1, command raftplacement.ReplicaReplacementBeginV1, ordinary VectorPartitionShardSearchResponseV1) VectorPartitionShardSearchResponseV1 {
	t.Helper()
	request := replacementOwnerQualificationRequestV1(t, ctx, target, command)
	var first ReplicaReplacementOwnerQualificationV1
	for i := 0; i < 2; i++ {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		result, err := client.QualifyReplicaReplacementOwnerV1(ctx, command, request)
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatalf("private ANN qualification: %v", err)
		}
		t.Logf("private_ann_qualification sample=%d elapsed_ns=%d global_bytes=%d global_allocs=%d scope=caller_five_raft_nodes_background_tls_authority_readindex_applied_and_ann cache=%+v", i, elapsed.Nanoseconds(), after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, target.localDataV1(command.GroupID).replacementWork.ownerSource.Stats())
		assertReplacementOwnerANNRouteV1(t, result.Search)
		proof := result.Search.Proof
		if proof.Kind != vectorPartitionShardSearchProofPrivateOwnerV1 || proof.LeaderNode != command.OldNodeID ||
			proof.ServingNode != command.NewPeer.ID || proof.AppliedIndex < proof.ReadIndex || result.Search.ReadProofs != 1 ||
			len(result.Search.Partials) != 1 || len(result.Search.Partials[0].Neighbors) == 0 {
			t.Fatalf("private qualification evidence=%+v", result)
		}
		if i == 0 {
			first = result
		} else if !reflect.DeepEqual(first.Search.Partials[0].Neighbors, result.Search.Partials[0].Neighbors) {
			t.Fatalf("cached private ANN differs: cold=%+v cached=%+v", first, result)
		}
	}
	// Capture a genuine ordinary oracle before snapshot recovery retires its
	// startup DB. Later comparisons reuse only immutable results, never authority.
	if ordinary.Version == 0 {
		topology, err := oldOwner.vector.ensureImmutableTopologyV1(ctx)
		if err != nil {
			t.Fatal(err)
		}
		group, ok := topology.coordinator.groups[command.GroupID]
		if !ok {
			t.Fatal("original owner group missing")
		}
		ordinaryRequest := request
		ordinaryRequest.TargetNodeID = command.OldNodeID
		task := vectorPartitionCoordinatorTaskV1{group: group, partitionIDs: request.PartitionIDs, candidateRows: []uint64{4}}
		if err := topology.coordinator.validateShardResponse(ctx, task, ordinaryRequest, first.Search); !errors.Is(err, ErrVectorPartitionCoordinatorMalformedResponse) {
			t.Fatalf("public coordinator accepted private target evidence: %v", err)
		}
		memberShaped := first.Search
		memberShaped.Proof.ServingNode, memberShaped.Proof.LeaderNode = command.OldNodeID, command.OldNodeID
		if err := topology.coordinator.validateShardResponse(ctx, task, ordinaryRequest, memberShaped); !errors.Is(err, ErrVectorPartitionCoordinatorMalformedResponse) {
			t.Fatalf("private proof kind acquired ordinary M5 authority: %v", err)
		}
		endpoints := client.config.Vector.ShardAddresses[command.GroupID]
		dispatcher, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(client.peerTransport,
			map[raftcluster.GroupID]string{command.GroupID: endpoints[command.OldNodeID]},
			map[raftcluster.GroupID]map[raftcluster.NodeID]string{command.GroupID: {command.OldNodeID: endpoints[command.OldNodeID]}},
		)
		if err != nil {
			t.Fatal(err)
		}
		defer dispatcher.Close()
		ordinary, err = dispatcher.DispatchVectorPartitionShardSearchV1(ctx, ordinaryRequest)
		if err != nil {
			t.Fatalf("original owner ordinary shard search: %v", err)
		}
		if ordinary.Proof.Kind != vectorPartitionShardSearchProofReadIndexV1 || ordinary.Proof.ServingNode != command.OldNodeID ||
			ordinary.Proof.LeaderNode != command.OldNodeID || len(ordinary.Partials) != len(first.Search.Partials) {
			t.Fatalf("ordinary proof or private parity shape=%+v", ordinary)
		}
	}
	proof := ordinary.Proof
	if proof.GroupID != request.TargetGroupID || proof.SourceGeneration != request.SourceGeneration ||
		proof.SourceChecksum != request.SourceChecksum || proof.SourceSchemaHash != request.SourceSchemaHash ||
		proof.SourceRowCount != request.SourceRowCount || proof.PartitionGeneration != request.PartitionGeneration ||
		proof.RouterGeneration != request.RouterGeneration || proof.ReadySetDigest != request.ReadySetDigest ||
		len(ordinary.Partials) != len(request.PartitionIDs) {
		t.Fatalf("ordinary oracle identity changed: request=%+v oracle=%+v", request, ordinary)
	}
	for i, partial := range ordinary.Partials {
		if partial.PartitionID != request.PartitionIDs[i] {
			t.Fatalf("ordinary oracle partition changed: request=%+v oracle=%+v", request, ordinary)
		}
	}
	assertReplacementOwnerANNRouteV1(t, ordinary)
	for i, partial := range ordinary.Partials {
		got := first.Search.Partials[i].Neighbors
		if len(got) != len(partial.Neighbors) {
			t.Fatalf("ANN neighbor count private=%+v ordinary=%+v", got, partial.Neighbors)
		}
		for j, neighbor := range partial.Neighbors {
			if got[j].ID != neighbor.ID || math.Abs(float64(got[j].Score-neighbor.Score)) > 1e-6 {
				t.Fatalf("ANN parity private=%+v ordinary=%+v", got, partial.Neighbors)
			}
		}
	}
	return ordinary
}

func TestReplacementPrivateANNFrameDiscriminatorsV1(t *testing.T) {
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	response := VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID, Partials: []VectorPartitionShardSearchPartialV1{}}
	for _, frame := range []vectorPartitionShardSearchTCPFrameV1{{PrivateRequest: &request, PrivateBegin: []byte("begin")}, {PrivateResponse: &response}} {
		raw, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, frame)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeVectorPartitionShardSearchTCPFrameBodyV1(raw)
		if err != nil || decoded.Request != nil || decoded.Response != nil || !reflect.DeepEqual(frame, decoded) {
			t.Fatalf("private frame acquired ordinary authority: %+v %v", decoded, err)
		}
	}
	// An ordinary dispatcher must reject the distinct private response envelope.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{request.TargetGroupID: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	dispatcher.dial = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	done := make(chan error, 1)
	go func() {
		_, err := readVectorPartitionShardSearchTCPFrameV1(server, vectorPartitionShardSearchTCPMaxFrameBytesV1)
		if err == nil {
			err = writeVectorPartitionShardSearchTCPFrameV1(server, vectorPartitionShardSearchTCPFrameV1{PrivateResponse: &response}, vectorPartitionShardSearchTCPMaxFrameBytesV1)
		}
		done <- err
	}()
	if result, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request); err == nil || len(result.Partials) != 0 {
		t.Fatalf("ordinary dispatcher accepted private response: %+v %v", result, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{Request: &request, PrivateBegin: []byte("begin")}); err == nil {
		t.Fatal("ordinary frame accepted a private operation binding")
	}
}

func assertReplacementOwnerANNRouteV1(t *testing.T, response VectorPartitionShardSearchResponseV1) {
	t.Helper()
	var counters VectorPartitionCoordinatorCountersV1
	if !accumulateVectorPartitionCoordinatorResponseCountersV1(&counters, response) ||
		response.Partitions != 1 || response.PartitionOpens != 1 || len(response.Partials) != 1 ||
		counters.HNSWServedPartitions != 1 || counters.ExactScanPartitions != 0 ||
		response.Partials[0].SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("qualification must traverse one hosted domain graph, without exact fallback: response=%+v counters=%+v", response, counters)
	}
}

func TestReplacementPrivateANNLateAdmissionClearsResponseV1(t *testing.T) {
	service, source, _ := newVectorPartitionShardSearchTestServiceV1(t,
		[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}},
		map[uint32]collections.VectorPartitionSearchAssetV1{
			0: vectorPartitionShardSearchAssetTestV1(0, []string{"a", "b"}, [][]float32{{1, 0}, {0, 1}}),
		})
	var admissionErr error
	service.preparedOwnerAdmission = func(context.Context) error { return admissionErr }
	// Deterministic unit evidence exercises the shared body and final guard;
	// the real fixture above obtains its issuer proof through authenticated Raft.
	service.privateOwnerReadBarrier = func(context.Context) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, error) {
		return raftcluster.ReadIndexProof{NodeID: "node-b", GroupID: "group-a", Term: 3, Index: 41, HasQuorum: true, EvidenceKind: raftcluster.ReadIndexEvidenceProduction},
			raftcluster.AppliedProgress{NodeID: "node-a", GroupID: "group-a", Term: 3, Index: 43, HasApplied: true}, nil
	}
	service.testBeforeResponseCopy = func() { admissionErr = ErrFixedPeerVectorProofStaleV1 }
	response, err := service.searchPrivateOwnerV1(t.Context(), vectorPartitionShardSearchRequestTestV1([]uint32{0}))
	if !errors.Is(err, ErrFixedPeerVectorProofStaleV1) || !reflect.DeepEqual(response, VectorPartitionShardSearchResponseV1{}) ||
		source.pins != 1 || source.releases != 1 || source.opens != 1 {
		t.Fatalf("late refusal retained hits or generation lease: response=%+v err=%v pins=%d releases=%d opens=%d", response, err, source.pins, source.releases, source.opens)
	}
}

// Exercise shared admission without another Raft cluster. Identity is codec-valid;
// disabled TLS dial refuses, so this unit fixture claims no authority/ANN success.
func TestReplacementPrivateANNRequestAdmissionV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	group, node := config.Groups[0].ID, config.NodeID
	digest := strings.Repeat("a", 64)
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection:            raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"},
			CollectionIncarnation: 1, IndexName: "embedding", IndexDefinitionDigest: digest,
			IndexEpoch: 1, CatalogEpoch: 1, CatalogDigest: digest,
		},
		Source:     raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 11, Checksum: 22, SchemaHash: 33, RowCount: 2},
		Generation: 7,
		Immutable:  raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{ManifestDigest: digest, PlacementDigest: digest},
	}
	command := raftplacement.ReplicaReplacementBeginV1{
		OperationID: "admission-only", ConfigDigest: digest, ExpectedEpoch: 1, CatalogDigest: digest,
		GroupID: group, OldNodeID: "different-old-node", NewPeer: raftcluster.Peer{ID: node, Address: "127.0.0.1:1"}, OwnerPreparation: &identity,
	}
	if _, err := raftplacement.EncodeReplicaReplacementBeginV1(command); err != nil {
		t.Fatal(err)
	}
	config.Vector = &FixedPeerTCPVectorConfigV1{ShardAddresses: map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {node: "127.0.0.1:1"}}}
	client := &FixedPeerTCPClientV1{config: config, digest: command.ConfigDigest, peerTransport: transport}
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	request.TargetGroupID, request.TargetNodeID = group, node
	ordinaryBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(request)
	if err != nil {
		t.Fatal(err)
	}
	bounded := request
	bounded.RequestBytesLimit = ordinaryBytes
	if result, err := client.QualifyReplicaReplacementOwnerV1(t.Context(), command, bounded); !errors.Is(err, ErrVectorPartitionShardSearchInvalidRequest) || !reflect.DeepEqual(result, ReplicaReplacementOwnerQualificationV1{}) || transport.ResourceStatsV1().Current != (peerResourceAmountsV1{}) {
		t.Fatalf("private BEGIN bypassed request byte limit: %+v %v %+v", result, err, transport.ResourceStatsV1())
	}
	a := transport.admission
	held, err := a.acquire("shard:"+string(group), peerBytesV1, a.scopes["shard:"+string(group)].limits[peerBytesV1])
	if err != nil {
		t.Fatal(err)
	}
	defer held.release()
	before := transport.ResourceStatsV1().Current
	transport.security = nil // resource refusal must precede the disabled dial
	result, err := client.QualifyReplicaReplacementOwnerV1(t.Context(), command, request)
	if !errors.Is(err, raftcluster.ErrAdmissionUnavailable) || len(result.Search.Partials) != 0 || transport.ResourceStatsV1().Current != before {
		t.Fatalf("byte exhaustion reached dial or leaked request: result=%+v err=%v resources=%+v", result, err, transport.ResourceStatsV1())
	}
	held.release()
	result, err = client.QualifyReplicaReplacementOwnerV1(t.Context(), command, request)
	if !errors.Is(err, errPeerAuthenticationV1) || len(result.Search.Partials) != 0 || transport.ResourceStatsV1().Current != (peerResourceAmountsV1{}) {
		t.Fatalf("pre-catalog authentication refusal leaked qualification request: result=%+v err=%v resources=%+v", result, err, transport.ResourceStatsV1())
	}
	for _, test := range []struct {
		name   string
		mutate func(*VectorPartitionShardSearchRequestV1)
	}{
		{"stats_none", func(r *VectorPartitionShardSearchRequestV1) { r.StatsMode = VectorPartitionShardSearchStatsNoneV1 }},
		{"live_revision", func(r *VectorPartitionShardSearchRequestV1) { r.LiveRevision = 1 }},
		{"live_coverage", func(r *VectorPartitionShardSearchRequestV1) { r.LiveCoverage = 1 }},
		{"live_domains", func(r *VectorPartitionShardSearchRequestV1) { r.LiveDomainIDs = []uint32{0} }},
		{"strict_capability", func(r *VectorPartitionShardSearchRequestV1) {
			r.StrictCapability = &vectorPartitionStrictSearchCapabilityV1{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := request
			test.mutate(&invalid)
			result, err := client.QualifyReplicaReplacementOwnerV1(t.Context(), command, invalid)
			if !errors.Is(err, raftcluster.ErrUnsupportedFeature) || !reflect.DeepEqual(result, ReplicaReplacementOwnerQualificationV1{}) ||
				transport.ResourceStatsV1().Current != (peerResourceAmountsV1{}) {
				t.Fatalf("unsupported qualification consumed resources: %+v %v %+v", result, err, transport.ResourceStatsV1())
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.QualifyReplicaReplacementOwnerV1(ctx, command, request); !errors.Is(err, context.Canceled) || transport.ResourceStatsV1().Current != (peerResourceAmountsV1{}) {
		t.Fatalf("canceled request consumed resources: %v %+v", err, transport.ResourceStatsV1())
	}
}

// Deterministic client-boundary evidence only; the real fixture above proves
// authenticated quorum, current-FSM application and native ANN execution.
func TestReplacementPrivateANNResponseValidationV1(t *testing.T) {
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	command := raftplacement.ReplicaReplacementBeginV1{
		GroupID: request.TargetGroupID, OldNodeID: "node-b",
		NewPeer: raftcluster.Peer{ID: request.TargetNodeID},
	}
	client := &FixedPeerTCPClientV1{config: FixedPeerTCPConfigV1{Groups: []FixedPeerTCPGroupV1{{
		ID: command.GroupID, Peers: []raftcluster.Peer{{ID: command.OldNodeID}},
	}}}}
	makeResponse := func() VectorPartitionShardSearchResponseV1 {
		response := VectorPartitionShardSearchResponseV1{
			Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID,
			Proof: VectorPartitionShardSearchProofV1{
				Kind: vectorPartitionShardSearchProofPrivateOwnerV1, ServingNode: command.NewPeer.ID, LeaderNode: command.OldNodeID,
				GroupID: command.GroupID, ReadySetDigest: request.ReadySetDigest, ReadTerm: 3, ReadIndex: 41, AppliedTerm: 2, AppliedIndex: 43,
				SourceGeneration: request.SourceGeneration, SourceChecksum: request.SourceChecksum, SourceSchemaHash: request.SourceSchemaHash, SourceRowCount: request.SourceRowCount,
				PartitionGeneration: request.PartitionGeneration, RouterGeneration: request.RouterGeneration,
			},
			Partials: []VectorPartitionShardSearchPartialV1{{
				PartitionID: 0, SearchRoute: collections.VectorPartitionSearchRouteHNSWSearchPackV1,
				Candidates: 2, ScoreCalls: 2, Edges: 1,
				Neighbors: []VectorPartitionShardSearchNeighborV1{{ID: "a", Score: 1}, {ID: "b", Score: 0}},
			}},
			Partitions: 1, ReadProofs: 1, GenerationPins: 1, PartitionOpens: 1,
			ScoreCalls: 2, Candidates: 2, BaseCandidates: 2, BaseResults: 2, Edges: 1,
		}
		var err error
		response.ResponseBytes, err = MeasureVectorPartitionShardSearchResponseBytesV1(response.Partials)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	valid := makeResponse()
	if result, err := client.validateReplacementOwnerQualificationResponseV1(t.Context(), command, client.config.Groups[0], request, &valid); err != nil || !reflect.DeepEqual(result.Search, valid) {
		t.Fatalf("valid private ANN payload refused: %+v %v", result, err)
	}
	// Authenticated catalog transport with controlled semantic replies. This
	// proves roster/operation validation, not Raft completion or quorum; the
	// real fixture and existing sequential-replacement tests provide those.
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	digest := strings.Repeat("a", 64)
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection:            raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"},
			CollectionIncarnation: 1, IndexName: "embedding", IndexDefinitionDigest: digest,
			IndexEpoch: 1, CatalogEpoch: 1, CatalogDigest: digest,
		},
		Source:     raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 11, Checksum: 22, SchemaHash: 33, RowCount: 2},
		Generation: 7, Immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{ManifestDigest: digest, PlacementDigest: digest},
	}
	boundCommand := raftplacement.ReplicaReplacementBeginV1{
		OperationID: "next-preparation", ConfigDigest: digest, ExpectedEpoch: 1, CatalogDigest: digest,
		GroupID: command.GroupID, OldNodeID: "previous-replacement", NewPeer: raftcluster.Peer{ID: command.NewPeer.ID, Address: "127.0.0.1:19002"}, OwnerPreparation: &identity,
	}
	begin, err := raftplacement.EncodeReplicaReplacementBeginV1(boundCommand)
	if err != nil {
		t.Fatal(err)
	}
	seed, _, _ := replacementGateSeedForTestV1()
	current := raftplacement.ReplicaReplacementStateV1{
		Begin: boundCommand, Phase: raftplacement.ReplicaReplacementAddIntentV1, Seed: &seed,
		Peers: []raftcluster.Peer{{ID: boundCommand.OldNodeID, Address: "127.0.0.1:19001"}, {ID: "survivor", Address: "127.0.0.1:19003"}},
	}
	var observedMu sync.Mutex
	var observed *raftplacement.ReplicaReplacementStateV1 = &current
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
			t.Error("catalog request was not authenticated")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if node, err := transport.security.identity(r.TLS.PeerCertificates[0]); err != nil || node != config.NodeID {
			t.Errorf("catalog client identity: %s %v", node, err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		reply := fixedPeerReplyV1{NodeID: config.NodeID, ConfigDigest: digest}
		switch r.URL.Path {
		case "/v1/status":
			reply.Status.CatalogRaft = raftcluster.RuntimeStatusV1{GroupID: config.Catalog.ID, LeaderID: config.NodeID}
		case "/v1/replacement-read":
			var request fixedPeerRequestV1
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || string(request.Entry) != string(begin) {
				t.Errorf("catalog BEGIN mismatch: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			observedMu.Lock()
			if observed != nil {
				snapshot := *observed
				reply.ReplacementState = &snapshot
			}
			observedMu.Unlock()
		default:
			t.Errorf("unexpected catalog operation: %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Error(err)
		}
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{transport.security.certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: transport.security.roots}
	server.StartTLS()
	defer server.Close()
	address := server.Listener.Addr().String()
	httpTransport := &http.Transport{DialTLSContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return transport.dialScope(ctx, address, config.NodeID, "control")
	}}
	defer httpTransport.CloseIdleConnections()
	httpClient := &http.Client{Transport: httpTransport, Timeout: config.RequestTimeout}
	config.Vector = &FixedPeerTCPVectorConfigV1{Identity: identity}
	config.Groups = client.config.Groups // Deliberately retain the removed startup peer.
	authorityClient := &FixedPeerTCPClientV1{config: config, digest: digest, security: transport.security, peerTransport: transport,
		http: httpClient, readHTTP: httpClient, calls: make(chan struct{}, 4), readCalls: make(chan struct{}, 4),
		addresses: map[raftcluster.NodeID]string{config.NodeID: address, command.OldNodeID: address, boundCommand.OldNodeID: address, "survivor": address},
	}
	for _, test := range []struct {
		name     string
		mutate   func(*raftplacement.ReplicaReplacementStateV1)
		missing  bool
		issuer   raftcluster.NodeID
		accepted bool
	}{
		{name: "committed_previous_replacement", issuer: boundCommand.OldNodeID, accepted: true},
		{name: "removed_startup_issuer", issuer: command.OldNodeID},
		{name: "stale_begin", mutate: func(s *raftplacement.ReplicaReplacementStateV1) { s.Begin.OperationID += "-stale" }},
		{name: "wrong_phase", mutate: func(s *raftplacement.ReplicaReplacementStateV1) {
			s.Phase = raftplacement.ReplicaReplacementInstalledV1
		}},
		{name: "missing_seed", mutate: func(s *raftplacement.ReplicaReplacementStateV1) { s.Seed = nil }},
		{name: "missing_authority", missing: true},
		{name: "unanchored_committed_peer", mutate: func(s *raftplacement.ReplicaReplacementStateV1) { s.Peers[1].ID = "unanchored" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := current
			state.Peers = append([]raftcluster.Peer(nil), current.Peers...)
			if test.mutate != nil {
				test.mutate(&state)
			}
			observedMu.Lock()
			observed = &state
			if test.missing {
				observed = nil
			}
			observedMu.Unlock()
			group, err := authorityClient.replacementOwnerQualificationGroupV1(t.Context(), boundCommand, begin)
			var result ReplicaReplacementOwnerQualificationV1
			if err == nil {
				response := makeResponse()
				response.Proof.LeaderNode = test.issuer
				result, err = authorityClient.validateReplacementOwnerQualificationResponseV1(t.Context(), boundCommand, group, request, &response)
			}
			if test.accepted {
				if err != nil || result.Search.Proof.LeaderNode != boundCommand.OldNodeID {
					t.Fatalf("committed replacement issuer refused: %+v %v", result, err)
				}
			} else if err == nil || !reflect.DeepEqual(result, ReplicaReplacementOwnerQualificationV1{}) {
				t.Fatalf("stale roster/authority retained private hits: %+v %v", result, err)
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*VectorPartitionShardSearchRequestV1)
	}{
		{"candidate_bytes_budget", func(r *VectorPartitionShardSearchRequestV1) { r.CandidateBytesLimit = 127 }},
		{"response_bytes_budget", func(r *VectorPartitionShardSearchRequestV1) { r.ResponseBytesLimit = valid.ResponseBytes - 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bounded := request
			test.mutate(&bounded)
			result, err := client.validateReplacementOwnerQualificationResponseV1(t.Context(), command, client.config.Groups[0], bounded, &valid)
			if !errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded) || !reflect.DeepEqual(result, ReplicaReplacementOwnerQualificationV1{}) {
				t.Fatalf("over-budget private result accepted: %+v %v", result, err)
			}
		})
	}
	empty := makeResponse()
	empty.Partials[0].Neighbors = nil
	empty.Partials[0].ScoreCalls, empty.Partials[0].Candidates, empty.Partials[0].Edges = 0, 0, 0
	empty.ScoreCalls, empty.Candidates, empty.BaseCandidates, empty.BaseResults, empty.Edges = 0, 0, 0, 0, 0
	empty.ResponseBytes, _ = MeasureVectorPartitionShardSearchResponseBytesV1(empty.Partials)
	if result, err := client.validateReplacementOwnerQualificationResponseV1(t.Context(), command, client.config.Groups[0], request, &empty); err != nil || !reflect.DeepEqual(result.Search, empty) {
		t.Fatalf("coherent empty ANN result refused: %+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := client.validateReplacementOwnerQualificationResponseV1(ctx, command, client.config.Groups[0], request, &empty); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(result, ReplicaReplacementOwnerQualificationV1{}) {
		t.Fatalf("canceled empty result accepted: %+v %v", result, err)
	}
	if result, err := client.validateReplacementOwnerQualificationResponseV1(t.Context(), command, client.config.Groups[0], request, nil); err == nil || !reflect.DeepEqual(result, ReplicaReplacementOwnerQualificationV1{}) {
		t.Fatalf("nil private payload accepted: %+v %v", result, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*VectorPartitionShardSearchResponseV1)
	}{
		{"missing_partials", func(r *VectorPartitionShardSearchResponseV1) { r.Partials = nil }},
		{"wrong_partition", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].PartitionID++ }},
		{"partition_count", func(r *VectorPartitionShardSearchResponseV1) { r.Partitions = 0 }},
		{"proof_count", func(r *VectorPartitionShardSearchResponseV1) { r.ReadProofs = 0 }},
		{"generation_pins", func(r *VectorPartitionShardSearchResponseV1) { r.GenerationPins = 0 }},
		{"partition_opens", func(r *VectorPartitionShardSearchResponseV1) { r.PartitionOpens = 0 }},
		{"zero_traversal", func(r *VectorPartitionShardSearchResponseV1) {
			r.Partials[0].Candidates = 0
			r.Partials[0].ScoreCalls = 0
			r.Candidates = 0
			r.BaseCandidates = 0
			r.ScoreCalls = 0
		}},
		{"exact_fallback", func(r *VectorPartitionShardSearchResponseV1) {
			r.Partials[0].SearchRoute = collections.VectorPartitionSearchRouteExactFP32ScanV1
		}},
		{"unknown_route", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].SearchRoute = "unknown" }},
		{"chunk_coherence", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].RequiredChunks = 1 }},
		{"aggregate_candidates", func(r *VectorPartitionShardSearchResponseV1) { r.Candidates++; r.BaseCandidates++ }},
		{"aggregate_edges", func(r *VectorPartitionShardSearchResponseV1) { r.Edges++ }},
		{"aggregate_scores", func(r *VectorPartitionShardSearchResponseV1) { r.ScoreCalls++ }},
		{"candidates_exceed_source", func(r *VectorPartitionShardSearchResponseV1) {
			r.Partials[0].Candidates = request.SourceRowCount + 1
			r.Partials[0].ScoreCalls = r.Partials[0].Candidates
			r.Candidates = r.Partials[0].Candidates
			r.BaseCandidates = r.Candidates
			r.ScoreCalls = r.Partials[0].ScoreCalls
		}},
		{"candidates_exceed_scores", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].ScoreCalls = 1; r.ScoreCalls = 1 }},
		{"candidate_overflow", func(r *VectorPartitionShardSearchResponseV1) {
			r.Candidates = ^uint64(0)
			r.BaseCandidates = r.Candidates
			r.Partials[0].Candidates = r.Candidates
		}},
		{"score_budget", func(r *VectorPartitionShardSearchResponseV1) {
			r.ScoreCalls = request.ScoreCallsLimit + 1
			r.Partials[0].ScoreCalls = r.ScoreCalls
		}},
		{"response_bytes", func(r *VectorPartitionShardSearchResponseV1) { r.ResponseBytes++ }},
		{"too_few_neighbors", func(r *VectorPartitionShardSearchResponseV1) {
			r.Partials[0].Neighbors = r.Partials[0].Neighbors[:1]
			r.BaseResults = 1
		}},
		{"too_many_neighbors", func(r *VectorPartitionShardSearchResponseV1) {
			r.Partials[0].Neighbors = append(r.Partials[0].Neighbors, VectorPartitionShardSearchNeighborV1{ID: "c", Score: -1})
		}},
		{"empty_id", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].Neighbors[0].ID = "" }},
		{"oversized_id", func(r *VectorPartitionShardSearchResponseV1) {
			r.Partials[0].Neighbors[0].ID = strings.Repeat("a", DefaultVectorPartitionCoordinatorLimitsV1().MaxStableIDBytes+1)
		}},
		{"nan_score", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].Neighbors[0].Score = float32(math.NaN()) }},
		{"inf_score", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].Neighbors[0].Score = float32(math.Inf(1)) }},
		{"duplicate_id", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].Neighbors[1].ID = "a" }},
		{"score_order", func(r *VectorPartitionShardSearchResponseV1) { r.Partials[0].Neighbors[1].Score = 2 }},
		{"tie_order", func(r *VectorPartitionShardSearchResponseV1) {
			r.Partials[0].Neighbors[0] = VectorPartitionShardSearchNeighborV1{ID: "b", Score: 1}
			r.Partials[0].Neighbors[1] = VectorPartitionShardSearchNeighborV1{ID: "a", Score: 1}
		}},
		{"unknown_issuer", func(r *VectorPartitionShardSearchResponseV1) { r.Proof.LeaderNode = "node-other" }},
		{"target_issuer", func(r *VectorPartitionShardSearchResponseV1) { r.Proof.LeaderNode = command.NewPeer.ID }},
		{"ordinary_kind", func(r *VectorPartitionShardSearchResponseV1) {
			r.Proof.Kind = vectorPartitionShardSearchProofReadIndexV1
		}},
		{"zero_applied_term", func(r *VectorPartitionShardSearchResponseV1) { r.Proof.AppliedTerm = 0 }},
		{"live_proof", func(r *VectorPartitionShardSearchResponseV1) { r.Proof.LiveRevision = 1 }},
		{"live_counters", func(r *VectorPartitionShardSearchResponseV1) { r.LiveIDs = 1 }},
		{"strict_grant", func(r *VectorPartitionShardSearchResponseV1) { r.Proof.CatalogAppliedIndex = 1 }},
		{"base_results", func(r *VectorPartitionShardSearchResponseV1) { r.BaseResults = 0 }},
		{"delta_results", func(r *VectorPartitionShardSearchResponseV1) { r.DeltaResults = 1 }},
		{"timing", func(r *VectorPartitionShardSearchResponseV1) { r.Timing.SearchNanos = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := makeResponse()
			test.mutate(&response)
			result, err := client.validateReplacementOwnerQualificationResponseV1(t.Context(), command, client.config.Groups[0], request, &response)
			if err == nil || !reflect.DeepEqual(result, ReplicaReplacementOwnerQualificationV1{}) {
				t.Fatalf("malformed private ANN retained hits: %+v %v", result, err)
			}
		})
	}
}

func TestReplacementPrivateANNServiceRejectsMutableShapeV1(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*VectorPartitionShardSearchRequestV1)
	}{
		{"stats_none", func(r *VectorPartitionShardSearchRequestV1) { r.StatsMode = VectorPartitionShardSearchStatsNoneV1 }},
		{"live_revision", func(r *VectorPartitionShardSearchRequestV1) { r.LiveRevision = 1 }},
		{"live_coverage", func(r *VectorPartitionShardSearchRequestV1) { r.LiveCoverage = 1 }},
		{"live_domains", func(r *VectorPartitionShardSearchRequestV1) { r.LiveDomainIDs = []uint32{0} }},
		{"strict_capability", func(r *VectorPartitionShardSearchRequestV1) {
			r.StrictCapability = &vectorPartitionStrictSearchCapabilityV1{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t,
				[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}}, nil)
			admissions, proofs := 0, 0
			service.preparedOwnerAdmission = func(context.Context) error { admissions++; return nil }
			service.privateOwnerReadBarrier = func(context.Context) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, error) {
				proofs++
				return raftcluster.ReadIndexProof{}, raftcluster.AppliedProgress{}, nil
			}
			request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
			test.mutate(&request)
			response, err := service.searchPrivateOwnerV1(t.Context(), request)
			assertVectorPartitionShardSearchCodeV1(t, err, VectorPartitionShardSearchErrorInvalidRequestV1)
			if !reflect.DeepEqual(response, VectorPartitionShardSearchResponseV1{}) || admissions != 0 || proofs != 0 || coordinator.callCount() != 0 || source.pins != 0 || source.opens != 0 || source.releases != 0 {
				t.Fatalf("private shape refusal reached authority or source: response=%+v admissions=%d proofs=%d source=%+v", response, admissions, proofs, source)
			}
		})
	}
}

// These credentialed direct frames bypass the client. Accepted boundary evidence
// hands off to a refusal-only callback, without operation authority or ANN success.
func TestReplacementPrivateANNReceiveValidationV1(t *testing.T) {
	for _, test := range []struct {
		name          string
		mutate        func(*VectorPartitionShardSearchRequestV1)
		accepted      bool
		frameTooSmall bool
	}{
		{name: "stats_none", mutate: func(r *VectorPartitionShardSearchRequestV1) { r.StatsMode = VectorPartitionShardSearchStatsNoneV1 }},
		{name: "live_revision", mutate: func(r *VectorPartitionShardSearchRequestV1) { r.LiveRevision = 1 }},
		{name: "live_coverage", mutate: func(r *VectorPartitionShardSearchRequestV1) { r.LiveCoverage = 1 }},
		{name: "live_domains", mutate: func(r *VectorPartitionShardSearchRequestV1) { r.LiveDomainIDs = []uint32{0} }},
		{name: "strict_capability", mutate: func(r *VectorPartitionShardSearchRequestV1) {
			r.StrictCapability = &vectorPartitionStrictSearchCapabilityV1{}
		}},
		{name: "ordinary_only_budget"},
		{name: "below_augmented_budget"},
		{name: "exact_augmented_budget", accepted: true},
		{name: "configured_frame_limit", frameTooSmall: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport, config := peerTransportFixtureV1(t)
			defer transport.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			service, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t,
				[]raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}}, nil)
			var calls atomic.Int64
			server := VectorPartitionShardSearchTCPServerV1{
				PeerTransport: transport, PeerGroupID: config.Groups[0].ID, Service: service,
				preparedOwnerAdmission: func(context.Context) error { return nil },
				privateOwnerSearch: func(context.Context, []byte, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
					calls.Add(1)
					return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, Err: raftcluster.ErrAdmissionUnavailable}
				},
			}
			request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
			request.TargetGroupID, request.TargetNodeID = config.Groups[0].ID, config.NodeID
			if test.mutate != nil {
				test.mutate(&request)
			}
			begin := []byte("receive-boundary-only")
			ordinaryBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(request)
			if err != nil {
				t.Fatal(err)
			}
			augmentedBytes := ordinaryBytes + 4 + uint64(len(begin))
			switch test.name {
			case "ordinary_only_budget":
				request.RequestBytesLimit = ordinaryBytes
			case "below_augmented_budget":
				request.RequestBytesLimit = augmentedBytes - 1
			case "exact_augmented_budget", "configured_frame_limit":
				request.RequestBytesLimit = augmentedBytes
			}
			encoded, err := appendVectorPartitionShardSearchTCPFrameBodyV1(nil, vectorPartitionShardSearchTCPFrameV1{PrivateRequest: &request, PrivateBegin: begin})
			if err != nil || uint64(len(encoded)) != augmentedBytes {
				t.Fatalf("private wire bytes=%d augmented budget=%d err=%v", len(encoded), augmentedBytes, err)
			}
			if test.frameTooSmall {
				server.MaxFrame = uint32(augmentedBytes - 1)
			}
			done := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					server.ServeConn(ctx, conn)
				}
				done <- err
			}()
			conn, err := transport.dialScope(ctx, listener.Addr().String(), config.NodeID, "shard:"+string(config.Groups[0].ID))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if deadline, ok := ctx.Deadline(); ok {
				_ = conn.SetDeadline(deadline)
			}
			if err := writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{PrivateRequest: &request, PrivateBegin: begin}, uint32(DefaultVectorPartitionShardSearchLimitsV1().MaxRequestBytes)); err != nil && !test.frameTooSmall {
				t.Fatal(err)
			}
			frame, readErr := readVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPMaxFrameBytesV1)
			if test.frameTooSmall {
				if readErr == nil {
					t.Fatalf("configured frame limit accepted request: %+v", frame)
				}
			} else {
				want := VectorPartitionShardSearchErrorInvalidRequestV1
				if test.accepted {
					want = VectorPartitionShardSearchErrorGroupUnavailableV1
				}
				if readErr != nil || frame.Error == nil || frame.Error.Code != want || frame.Response != nil || frame.PrivateResponse != nil {
					t.Fatalf("direct private frame response=%+v err=%v want=%s", frame, readErr, want)
				}
			}
			_ = conn.Close()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			wantCalls := int64(0)
			if test.accepted {
				wantCalls = 1
			}
			if calls.Load() != wantCalls || source.pins != 0 || source.opens != 0 || source.releases != 0 || coordinator.callCount() != 0 || transport.ResourceStatsV1().Current != (peerResourceAmountsV1{}) {
				t.Fatalf("direct frame leaked authority/source/resource work: calls=%d want=%d source=%+v resources=%+v", calls.Load(), wantCalls, source, transport.ResourceStatsV1())
			}
		})
	}
}
