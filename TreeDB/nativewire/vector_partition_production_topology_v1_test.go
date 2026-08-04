package nativewire

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestVectorPartitionProductionTopologyTwoGroupTCPGenerationPinnedV1(t *testing.T) {
	topology, request, reads := newVectorPartitionProductionTopologyTwoGroupTestV1(t)
	defer topology.Close()
	response, err := topology.Coordinator().Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Neighbors) != 2 || reads["group-a"].callCount() != 1 || reads["group-b"].callCount() != 1 {
		t.Fatalf("response=%+v read calls a=%d b=%d", response, reads["group-a"].callCount(), reads["group-b"].callCount())
	}
	if _, err := topology.Coordinator().Search(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	if response, err := topology.Coordinator().Search(context.Background(), request); err == nil || len(response.Neighbors) != 0 {
		t.Fatalf("closed topology returned partial response=%+v err=%v", response, err)
	}
}

func TestVectorPartitionProductionTopologyRollsBackPartialStartAndRestartsV1(t *testing.T) {
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "catalog", Collection: "docs"}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 1), SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, PartitionCount: 1, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &VectorPartitionShardSearchServiceV1{localGroup: "group-a", localNodeID: "node-a", route: vectorPartitionShardSearchRouteV1{placement: placement}}
	opts := VectorPartitionProductionTopologyOptionsV1{Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{}, ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}, Endpoints: map[raftcluster.GroupID]string{"group-a": listener.Addr().String()}, Shards: []VectorPartitionProductionShardV1{{GroupID: "group-a", Listener: listener, Service: service}, {GroupID: "group-a", Listener: listener, Service: service}}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("partial start unexpectedly succeeded")
	}
	if _, err := listener.Accept(); err == nil {
		t.Fatal("partial-start listener was not rolled back")
	}
	topology, _, _ := newVectorPartitionProductionTopologyTwoGroupTestV1(t)
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	// A fresh topology after a complete shutdown exercises the restart surface.
	restarted, _, _ := newVectorPartitionProductionTopologyTwoGroupTestV1(t)
	if !restarted.Status().Ready {
		t.Fatal("restarted topology is not ready")
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
}

func newVectorPartitionProductionTopologyTwoGroupTestV1(t *testing.T) (*VectorPartitionProductionTopologyV1, VectorPartitionCoordinatorRequestV1, map[raftcluster.GroupID]*fakeVectorPartitionReadCoordinatorV1) {
	t.Helper()
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}
	groups := []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}, {ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"}}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: groups, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1, SourceGeneration: 11, SourceChecksum: 22, SourceSchemaHash: 33, SourceRowCount: 2, PartitionGeneration: 7, PartitionCount: 2, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}}}
	manifest := collections.VectorPartitionManifestV1{Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: ref.Collection, IndexName: placement.IndexName, IndexDefinitionDigest: placement.IndexDefinitionDigest, SourceGeneration: placement.SourceGeneration, SourceChecksum: placement.SourceChecksum, SourceSchemaHash: placement.SourceSchemaHash, SourceRowCount: placement.SourceRowCount, Generation: placement.PartitionGeneration, RouterGeneration: placement.PartitionGeneration, ReadySetDigest: strings.Repeat("b", 64), PartitionCount: 2, Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}}, Memberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 1}}}
	reads := make(map[raftcluster.GroupID]*fakeVectorPartitionReadCoordinatorV1, 2)
	service := func(group raftcluster.GroupID, node raftcluster.NodeID, partition uint32) *VectorPartitionShardSearchServiceV1 {
		read := &fakeVectorPartitionReadCoordinatorV1{proof: raftcluster.ReadIndexProof{NodeID: node, GroupID: group, Term: 1, Index: 9, HasQuorum: true, EvidenceKind: raftcluster.ReadIndexEvidenceProduction}, progress: raftcluster.AppliedProgress{NodeID: node, GroupID: group, Term: 1, Index: 9, HasApplied: true}}
		reads[group] = read
		source := &fakeVectorPartitionGenerationSourceV1{manifest: manifest, assets: map[uint32]collections.VectorPartitionSearchAssetV1{partition: vectorPartitionShardSearchAssetTestV1(partition, []string{string(group)}, [][]float32{{1, 0}})}, openErr: map[uint32]error{}, searchers: map[uint32]*collections.VectorPartitionLocalSearcherV1{}}
		s, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{Catalog: catalog, Placement: placement, LocalNodeID: node, LocalGroupID: group, ReadCoordinator: read, GenerationSource: source})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listenerA.Close()
		t.Fatal(err)
	}
	endpointA := listenerA.Addr().String()
	boundA := listenerA.Addr().(*net.TCPAddr)
	listenerA = &temporaryWildcardTCPListenerV1{Listener: listenerA, addr: &net.TCPAddr{IP: net.IPv4zero, Port: boundA.Port}}
	router := &testVectorPartitionCoordinatorRouterV1{status: collections.VectorPartitionRouterRuntimeStatusV1{Manifest: manifest, ModelDigest: strings.Repeat("c", 64), Representatives: 2, Partitions: 2}, partitions: []collections.VectorPartitionRouterPartitionScoreV1{{PartitionID: 0}, {PartitionID: 1, Distance: 0.1}}}
	topology, err := NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{router: router}, ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}, Endpoints: map[raftcluster.GroupID]string{"group-a": endpointA, "group-b": listenerB.Addr().String()}, Shards: []VectorPartitionProductionShardV1{{GroupID: "group-a", Listener: listenerA, Service: service("group-a", "node-a", 0)}, {GroupID: "group-b", Listener: listenerB, Service: service("group-b", "node-b", 1)}}})
	if err != nil {
		_ = listenerA.Close()
		_ = listenerB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = topology.Close() })
	request := testVectorPartitionCoordinatorRequestV1(2)
	request.IndexDefinitionDigest = vectorPartitionShardSearchDigestTestV1
	return topology, request, reads
}

type temporaryWildcardTCPListenerV1 struct {
	net.Listener
	addr     net.Addr
	injected atomic.Bool
}

func (l *temporaryWildcardTCPListenerV1) Accept() (net.Conn, error) {
	if !l.injected.Swap(true) {
		return nil, temporaryAcceptErrorV1{}
	}
	return l.Listener.Accept()
}

func (l *temporaryWildcardTCPListenerV1) Addr() net.Addr { return l.addr }

type temporaryAcceptErrorV1 struct{}

func (temporaryAcceptErrorV1) Error() string   { return "temporary accept error" }
func (temporaryAcceptErrorV1) Timeout() bool   { return false }
func (temporaryAcceptErrorV1) Temporary() bool { return true }

func TestVectorPartitionProductionTopologyRequiresLiveAuthorityAndOwnsLifecycleV1(t *testing.T) {
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "catalog", Collection: "docs"}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 1), SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, PartitionCount: 1, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	router := &testVectorPartitionCoordinatorRouterV1{status: collections.VectorPartitionRouterRuntimeStatusV1{Manifest: collections.VectorPartitionManifestV1{Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: ref.Collection, IndexName: placement.IndexName, IndexDefinitionDigest: placement.IndexDefinitionDigest, SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, Generation: 5, RouterGeneration: 5, PartitionCount: 1, ReadySetDigest: fmt.Sprintf("%064x", 2), Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}}}, Representatives: 1, Partitions: 1}, partitions: []collections.VectorPartitionRouterPartitionScoreV1{{PartitionID: 0}}}
	opts := VectorPartitionProductionTopologyOptionsV1{Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{router: router}, Endpoints: map[raftcluster.GroupID]string{"group-a": listener.Addr().String()}, Shards: []VectorPartitionProductionShardV1{{GroupID: "group-a", Listener: listener, Service: &VectorPartitionShardSearchServiceV1{}}}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("missing lifecycle authority succeeded")
	}
	opts.ReplicatedLifecycle = &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}
	validService, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{Catalog: catalog, Placement: placement, LocalNodeID: "node-a", LocalGroupID: "group-a", ReadCoordinator: &fakeVectorPartitionReadCoordinatorV1{}, GenerationSource: &fakeVectorPartitionGenerationSourceV1{}})
	if err != nil {
		t.Fatal(err)
	}
	badPlacement := placement
	badPlacement.PartitionGeneration++
	badService, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{Catalog: catalog, Placement: badPlacement, LocalNodeID: "node-a", LocalGroupID: "group-a", ReadCoordinator: &fakeVectorPartitionReadCoordinatorV1{}, GenerationSource: &fakeVectorPartitionGenerationSourceV1{}})
	if err != nil {
		t.Fatal(err)
	}
	opts.Shards[0].Service = badService
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("mismatched shard service succeeded")
	}
	opts.Shards[0].Service = validService
	topology, err := NewVectorPartitionProductionTopologyV1(opts)
	if err != nil {
		t.Fatal(err)
	}
	status := topology.Status()
	if !status.Ready || status.Closed || len(status.ShardGroups) != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	if status := topology.Status(); !status.Closed || status.Ready {
		t.Fatalf("close status: %+v", status)
	}
	if _, err := topology.Coordinator().Search(context.Background(), VectorPartitionCoordinatorRequestV1{}); err == nil {
		t.Fatal("closed coordinator accepted request")
	}
}

func TestVectorPartitionProductionTopologyAllowsCoordinatorOnlyAndRejectsIncompleteOwnersV1(t *testing.T) {
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "catalog", Collection: "docs"}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}, {ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 1), SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, PartitionCount: 2, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}}}
	router := &testVectorPartitionCoordinatorRouterV1{}
	opts := VectorPartitionProductionTopologyOptionsV1{Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{router: router}, ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}, Endpoints: map[raftcluster.GroupID]string{"group-a": "127.0.0.1:1"}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("incomplete owner endpoint coverage succeeded")
	}
	opts.Endpoints["group-b"] = "127.0.0.1:2"
	opts.NodeEndpoints = map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node-z": "127.0.0.1:3"}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("foreign node endpoint succeeded")
	}
	opts.NodeEndpoints = map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node-a": "127.0.0.1:1"}}
	topology, err := NewVectorPartitionProductionTopologyV1(opts)
	if err != nil {
		t.Fatal(err)
	}
	if status := topology.Status(); !status.Ready || len(status.ShardGroups) != 0 {
		t.Fatalf("coordinator-only status=%+v", status)
	}
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionProductionTopologyRejectsNonOwnerEndpointV1(t *testing.T) {
	_, err := NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{Endpoints: map[raftcluster.GroupID]string{"not-an-owner": "127.0.0.1:1"}, Shards: []VectorPartitionProductionShardV1{{GroupID: "not-an-owner"}}, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{}, ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}})
	if err == nil {
		t.Fatal("invalid production topology succeeded")
	}
}
