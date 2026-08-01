package nativewire

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

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
