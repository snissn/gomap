package nativewire

import (
	"context"
	"errors"
	"math"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestImmutableOwnerReplacementPrivateANNQualificationV1(t *testing.T) {
	testImmutableOwnerReplacementPrivateQualificationV1(t, true, true)
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

func assertReplacementOwnerQualificationParityV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1, target, oldOwner *FixedPeerTCPRuntimeV1, command raftplacement.ReplicaReplacementBeginV1) {
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
	// The original owner remains the serving leader and ordinary shard route.
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
	request.TargetNodeID = command.OldNodeID
	ordinary, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request)
	if err != nil {
		t.Fatalf("original owner ordinary shard search: %v", err)
	}
	if ordinary.Proof.Kind != vectorPartitionShardSearchProofReadIndexV1 || ordinary.Proof.ServingNode != command.OldNodeID ||
		ordinary.Proof.LeaderNode != command.OldNodeID || len(ordinary.Partials) != len(first.Search.Partials) {
		t.Fatalf("ordinary proof or private parity shape=%+v", ordinary)
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
