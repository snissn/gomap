package nativewire

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	service := &VectorPartitionShardSearchServiceV1{localGroup: "group-a", localNodeID: "node-a", limits: DefaultVectorPartitionShardSearchLimitsV1(), route: vectorPartitionShardSearchRouteV1{placement: placement, hints: map[raftcluster.GroupID]raftcluster.NodeID{"group-a": "node-a"}}}
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

func newVectorPartitionProductionTopologyTwoGroupTestV1(t testing.TB) (*VectorPartitionProductionTopologyV1, VectorPartitionCoordinatorRequestV1, map[raftcluster.GroupID]*fakeVectorPartitionReadCoordinatorV1) {
	return newVectorPartitionProductionTopologyTwoGroupWithLifecycleTestV1(t, &recordingVectorPartitionReplicatedLifecycleAuthorityV1{})
}

func newVectorPartitionProductionTopologyTwoGroupWithLifecycleTestV1(t testing.TB, lifecycle VectorPartitionReplicatedLifecycleAuthorityV1) (*VectorPartitionProductionTopologyV1, VectorPartitionCoordinatorRequestV1, map[raftcluster.GroupID]*fakeVectorPartitionReadCoordinatorV1) {
	return newVectorPartitionProductionTopologyTwoGroupWithLifecycleReadySetTestV1(t, lifecycle, strings.Repeat("b", 64))
}

func newVectorPartitionProductionTopologyTwoGroupWithLifecycleReadySetTestV1(t testing.TB, lifecycle VectorPartitionReplicatedLifecycleAuthorityV1, readySetDigest string) (*VectorPartitionProductionTopologyV1, VectorPartitionCoordinatorRequestV1, map[raftcluster.GroupID]*fakeVectorPartitionReadCoordinatorV1) {
	t.Helper()
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}
	groups := []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}, {ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"}}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: groups, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1, SourceGeneration: 11, SourceChecksum: 22, SourceSchemaHash: 33, SourceRowCount: 2, PartitionGeneration: 7, PartitionCount: 2, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}}}
	manifest := collections.VectorPartitionManifestV1{Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: ref.Collection, IndexName: placement.IndexName, IndexDefinitionDigest: placement.IndexDefinitionDigest, SourceGeneration: placement.SourceGeneration, SourceChecksum: placement.SourceChecksum, SourceSchemaHash: placement.SourceSchemaHash, SourceRowCount: placement.SourceRowCount, Generation: placement.PartitionGeneration, RouterGeneration: placement.PartitionGeneration, ReadySetDigest: readySetDigest, PartitionCount: 2, Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}}, Memberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 1}}}
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
	topology, err := NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{router: router}, ReplicatedLifecycle: lifecycle, Endpoints: map[raftcluster.GroupID]string{"group-a": endpointA, "group-b": listenerB.Addr().String()}, Shards: []VectorPartitionProductionShardV1{{GroupID: "group-a", Listener: listenerA, Service: service("group-a", "node-a", 0)}, {GroupID: "group-b", Listener: listenerB, Service: service("group-b", "node-b", 1)}}})
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

type nonTCPAddrV1 string

func (a nonTCPAddrV1) Network() string { return "unix" }
func (a nonTCPAddrV1) String() string  { return string(a) }

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
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-b", "node-c"}, LeaderHint: "node-b"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 1), SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, PartitionCount: 1, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}}}
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	localAlias := fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
	router := &testVectorPartitionCoordinatorRouterV1{status: collections.VectorPartitionRouterRuntimeStatusV1{Manifest: collections.VectorPartitionManifestV1{Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: ref.Collection, IndexName: placement.IndexName, IndexDefinitionDigest: placement.IndexDefinitionDigest, SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, Generation: 5, RouterGeneration: 5, PartitionCount: 1, ReadySetDigest: fmt.Sprintf("%064x", 2), Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}}}, Representatives: 1, Partitions: 1}, partitions: []collections.VectorPartitionRouterPartitionScoreV1{{PartitionID: 0}}}
	opts := VectorPartitionProductionTopologyOptionsV1{Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{router: router}, Endpoints: map[raftcluster.GroupID]string{"group-a": localAlias}, Shards: []VectorPartitionProductionShardV1{{GroupID: "group-a", Listener: listener, Service: &VectorPartitionShardSearchServiceV1{}}}}
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
	strictLimits := DefaultVectorPartitionShardSearchLimitsV1()
	strictLimits.MaxPartitions = 1
	strictService, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{Catalog: catalog, Placement: placement, LocalNodeID: "node-a", LocalGroupID: "group-a", ReadCoordinator: &fakeVectorPartitionReadCoordinatorV1{}, GenerationSource: &fakeVectorPartitionGenerationSourceV1{}, Limits: strictLimits})
	if err != nil {
		t.Fatal(err)
	}
	opts.Shards[0].Service = strictService
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("mismatched shard limits succeeded")
	}
	staleHintCatalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-b", "node-c"}, LeaderHint: "node-c"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	staleHintService, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{Catalog: staleHintCatalog, Placement: placement, LocalNodeID: "node-a", LocalGroupID: "group-a", ReadCoordinator: &fakeVectorPartitionReadCoordinatorV1{}, GenerationSource: &fakeVectorPartitionGenerationSourceV1{}})
	if err != nil {
		t.Fatal(err)
	}
	opts.Shards[0].Service = staleHintService
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("mismatched shard leader hints succeeded")
	}
	opts.Shards[0].Service = validService
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("local shard succeeded without remote member endpoints")
	}
	opts.NodeEndpoints = map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node-b": localAlias, "node-c": "127.0.0.1:2"}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("remote shard member used local fallback")
	}
	opts.NodeEndpoints["group-a"]["node-b"] = "127.0.0.1:2"
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("distinct shard members shared an endpoint")
	}
	opts.NodeEndpoints["group-a"]["node-c"] = "127.0.0.1:3"
	opts.NodeEndpoints["group-a"]["node-a"] = "127.0.0.1:1"
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("mismatched local node endpoint succeeded")
	}
	opts.NodeEndpoints["group-a"]["node-a"] = localAlias
	port := listener.Addr().(*net.TCPAddr).Port
	opts.NodeEndpoints["group-a"]["node-b"] = fmt.Sprintf("127.0.0.2:%d", port)
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("remote shard member used a wildcard listener alias")
	}
	opts.NodeEndpoints["group-a"]["node-b"] = fmt.Sprintf("192.0.2.2:%d", port)
	opts.NodeEndpoints["group-a"]["node-c"] = fmt.Sprintf("192.0.2.3:%d", port)
	opts.Endpoints["group-a"] = fmt.Sprintf("192.0.2.4:%d", port)
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("wildcard listener accepted a non-local owner endpoint")
	}
	opts.Endpoints["group-a"] = localAlias
	opts.NodeEndpoints["group-a"]["node-a"] = fmt.Sprintf("192.0.2.4:%d", port)
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("wildcard listener accepted a non-local local-node endpoint")
	}
	opts.NodeEndpoints["group-a"]["node-a"] = localAlias
	opts.CoordinatorLimits.MaxConcurrentRequests = 1
	opts.CoordinatorLimits.MaxRequests = 2
	topology, err := NewVectorPartitionProductionTopologyV1(opts)
	if err != nil {
		t.Fatal(err)
	}
	status := topology.Status()
	if !status.Ready || status.Closed || len(status.ShardGroups) != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if endpoint, ok := topology.dispatcher.endpoint("group-a", "node-b"); !ok || endpoint != opts.NodeEndpoints["group-a"]["node-b"] {
		t.Fatalf("remote member endpoint=%q ok=%t", endpoint, ok)
	}
	first, err := net.Dial("tcp4", localAlias)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	deadline := time.Now().Add(time.Second)
	for {
		topology.mu.Lock()
		connections := len(topology.conns)
		topology.mu.Unlock()
		if connections == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first shard connection was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	second, err := net.Dial("tcp4", localAlias)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	deadline = time.Now().Add(time.Second)
	for {
		topology.mu.Lock()
		connections := len(topology.conns)
		topology.mu.Unlock()
		if connections == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second shard connection was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	third, err := net.Dial("tcp4", localAlias)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	if err := third.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := third.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess shard connection remained open")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("excess shard connection was not rejected")
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

func TestVectorPartitionProductionTopologyRejectsSharedShardListenerV1(t *testing.T) {
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "catalog", Collection: "docs"}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}}, {ID: "group-b", Members: []raftcluster.NodeID{"node-b"}}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 1), SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, PartitionCount: 2, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}}}
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	endpointA := fmt.Sprintf("127.0.0.1:%d", port)
	endpointB := fmt.Sprintf("127.0.0.2:%d", port)
	service := func(group raftcluster.GroupID, node raftcluster.NodeID) *VectorPartitionShardSearchServiceV1 {
		return &VectorPartitionShardSearchServiceV1{localGroup: group, localNodeID: node, limits: DefaultVectorPartitionShardSearchLimitsV1(), route: vectorPartitionShardSearchRouteV1{placement: placement, hints: map[raftcluster.GroupID]raftcluster.NodeID{"group-a": "", "group-b": ""}}}
	}
	_, err = NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{}, ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}, Endpoints: map[raftcluster.GroupID]string{"group-a": endpointA, "group-b": endpointB}, Shards: []VectorPartitionProductionShardV1{{GroupID: "group-a", Listener: listener, Service: service("group-a", "node-a")}, {GroupID: "group-b", Listener: listener, Service: service("group-b", "node-b")}}})
	if err == nil || !strings.Contains(err.Error(), "uses shard") {
		t.Fatalf("shared shard listener error=%v", err)
	}
}

func TestVectorPartitionProductionEndpointMatchesListenerAddressFamilyV1(t *testing.T) {
	tcpListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpListener.Close()
	endpoint := tcpListener.Addr().String()
	if vectorPartitionProductionEndpointMatchesListenerV1(endpoint, &temporaryWildcardTCPListenerV1{Listener: tcpListener, addr: nonTCPAddrV1(endpoint)}) {
		t.Fatal("non-TCP listener matched TCP endpoint")
	}
	scoped := &temporaryWildcardTCPListenerV1{Listener: tcpListener, addr: &net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 1, Zone: "eth0"}}
	if !vectorPartitionProductionEndpointAddressMatchesListenerV1(&net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 1, Zone: "eth0"}, scoped) {
		t.Fatal("matching scoped IPv6 endpoint was rejected")
	}
	if vectorPartitionProductionEndpointAddressMatchesListenerV1(&net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 1, Zone: "lo"}, scoped) {
		t.Fatal("mismatched scoped IPv6 zone was accepted")
	}
	interfaces, err := net.Interfaces()
	if err != nil || len(interfaces) == 0 {
		t.Fatalf("network interfaces=%v error=%v", interfaces, err)
	}
	iface := interfaces[0]
	scoped.addr = &net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 1, Zone: iface.Name}
	if !vectorPartitionProductionEndpointAddressMatchesListenerV1(&net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 1, Zone: fmt.Sprint(iface.Index)}, scoped) {
		t.Fatal("equivalent scoped IPv6 zone forms did not match")
	}
	nameKeys, err := vectorPartitionProductionEndpointKeysV1(fmt.Sprintf("[fe80::1%%%s]:1", iface.Name))
	if err != nil {
		t.Fatal(err)
	}
	indexKeys, err := vectorPartitionProductionEndpointKeysV1(fmt.Sprintf("[fe80::1%%%d]:1", iface.Index))
	if err != nil {
		t.Fatal(err)
	}
	if len(nameKeys) != 1 || len(indexKeys) != 1 || nameKeys[0] != indexKeys[0] {
		t.Fatalf("scoped endpoint keys name=%v index=%v", nameKeys, indexKeys)
	}
	linkLocal := net.ParseIP("fe80::1")
	local := []net.Addr{&net.IPNet{IP: linkLocal, Mask: net.CIDRMask(64, 128)}}
	if vectorPartitionProductionEndpointAddressIsLocalV1(&net.TCPAddr{IP: linkLocal}, local) {
		t.Fatal("link-local endpoint without a zone was accepted")
	}
	if vectorPartitionProductionEndpointAddressIsLocalV1(&net.TCPAddr{IP: linkLocal, Zone: "no-such-vector-interface"}, local) {
		t.Fatal("link-local endpoint with a foreign zone was accepted")
	}
	wildcard, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer wildcard.Close()
	wildcardEndpoint := fmt.Sprintf("localhost:%d", wildcard.Addr().(*net.TCPAddr).Port)
	addresses, err := vectorPartitionProductionEndpointAddressesV1(wildcardEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	want, err := net.DefaultResolver.LookupIPAddr(context.Background(), "localhost")
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		got[address.IP.String()] = true
	}
	for _, address := range want {
		if !address.IP.IsUnspecified() && !got[address.IP.String()] {
			t.Fatalf("resolved endpoint omitted localhost address %s: %+v", address.IP, addresses)
		}
	}
	allCompatible := true
	for _, address := range addresses {
		allCompatible = allCompatible && vectorPartitionProductionEndpointAddressMatchesListenerV1(address, wildcard)
	}
	if matches := vectorPartitionProductionEndpointMatchesListenerV1(wildcardEndpoint, wildcard); matches != allCompatible {
		t.Fatalf("wildcard listener hostname match=%t want=%t addresses=%+v", matches, allCompatible, addresses)
	}
	keys, err := vectorPartitionProductionEndpointKeysV1(wildcardEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != len(addresses) {
		t.Fatalf("endpoint keys=%v addresses=%+v", keys, addresses)
	}
	for i, address := range addresses {
		if keys[i] != address.String() {
			t.Fatalf("endpoint key[%d]=%q want %q", i, keys[i], address)
		}
	}
	if uses, err := vectorPartitionProductionEndpointUsesListenerV1(wildcardEndpoint, wildcard); err != nil || !uses {
		t.Fatalf("wildcard listener hostname match=%t error=%v addresses=%+v", uses, err, addresses)
	}

	listener, err := net.Listen("tcp6", "[::]:0")
	if err != nil {
		t.Skipf("IPv6 unavailable: %v", err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	if vectorPartitionProductionEndpointMatchesListenerV1(fmt.Sprintf("127.0.0.1:%d", port), listener) {
		t.Fatal("IPv6 listener matched IPv4 endpoint")
	}
	if vectorPartitionProductionEndpointMatchesListenerV1(fmt.Sprintf(":%d", port), listener) {
		t.Fatal("IPv6 listener matched hostless endpoint")
	}
	if !vectorPartitionProductionEndpointMatchesListenerV1(fmt.Sprintf("[::1]:%d", port), listener) {
		t.Fatal("IPv6 listener rejected IPv6 endpoint")
	}
	dualStack, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Skipf("dual-stack listener unavailable: %v", err)
	}
	defer dualStack.Close()
	bound := dualStack.Addr().(*net.TCPAddr)
	if bound.IP.To4() != nil {
		t.Skip("platform selected an IPv4 listener")
	}
	accepted := make(chan error, 1)
	go func() {
		conn, acceptErr := dualStack.Accept()
		if acceptErr == nil {
			acceptErr = conn.Close()
		}
		accepted <- acceptErr
	}()
	ipv4Endpoint := fmt.Sprintf("127.0.0.1:%d", bound.Port)
	conn, err := net.DialTimeout("tcp4", ipv4Endpoint, time.Second)
	if err != nil {
		_ = dualStack.Close()
		<-accepted
		t.Skipf("dual-stack IPv4 unavailable: %v", err)
	}
	_ = conn.Close()
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	if !vectorPartitionProductionEndpointMatchesListenerV1(ipv4Endpoint, dualStack) {
		t.Fatal("dual-stack listener rejected reachable IPv4 endpoint")
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
	opts.Endpoints["group-b"] = "missing-port"
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("malformed owner endpoint succeeded")
	}
	for _, endpoint := range []string{":2", "0.0.0.0:2", "[::]:2", "[fe80::1]:2", "[::1%lo]:2"} {
		opts.Endpoints["group-b"] = endpoint
		if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
			t.Fatalf("unspecified owner endpoint %q succeeded", endpoint)
		}
	}
	opts.Endpoints["group-b"] = "127.0.0.1:2"
	opts.NodeEndpoints = map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node-z": "127.0.0.1:3"}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("foreign node endpoint succeeded")
	}
	opts.NodeEndpoints = map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node-a": "127.0.0.1:1"}, "group-b": {"node-b": "127.0.0.1:1"}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("cross-group node endpoint collision succeeded")
	}
	opts.NodeEndpoints["group-b"]["node-b"] = "127.0.0.1:2"
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
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	service, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{Catalog: catalog, Placement: placement, LocalNodeID: "node-a", LocalGroupID: "group-a", ReadCoordinator: &fakeVectorPartitionReadCoordinatorV1{}, GenerationSource: &fakeVectorPartitionGenerationSourceV1{}})
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	opts.Shards = []VectorPartitionProductionShardV1{{GroupID: "group-a", Listener: listener, Service: service}}
	opts.Endpoints = map[raftcluster.GroupID]string{"group-a": fmt.Sprintf("127.0.0.1:%d", port), "group-b": fmt.Sprintf("127.0.0.2:%d", port)}
	opts.NodeEndpoints = nil
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("foreign owner endpoint used wildcard shard listener")
	}
	opts.Endpoints["group-b"] = "127.0.0.1:2"
	opts.NodeEndpoints = map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-b": {"node-b": fmt.Sprintf("127.0.0.2:%d", port)}}
	if _, err := NewVectorPartitionProductionTopologyV1(opts); err == nil {
		t.Fatal("foreign node endpoint used wildcard shard listener")
	}
}

func TestVectorPartitionProductionTopologyRejectsNonOwnerEndpointV1(t *testing.T) {
	_, err := NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{Endpoints: map[raftcluster.GroupID]string{"not-an-owner": "127.0.0.1:1"}, Shards: []VectorPartitionProductionShardV1{{GroupID: "not-an-owner"}}, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{}, ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{}})
	if err == nil {
		t.Fatal("invalid production topology succeeded")
	}
}

func TestVectorPartitionProductionNodeUsesActiveReadySetDigestV1(t *testing.T) {
	manifest := collections.VectorPartitionManifestV1{IndexName: "embedding", Generation: 1, IntegrityDigest: strings.Repeat("a", 64), ReadySetDigest: strings.Repeat("a", 64)}
	want := strings.Repeat("b", 64)
	logical := vectorPartitionProductionNodePinnedManifestV1(manifest, want)
	logical.IntegrityDigest = strings.Repeat("c", 64)
	logical.Placements = []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}}
	physical := manifest
	source := vectorPartitionProductionNodeGenerationSourceV1{source: &fakeVectorPartitionGenerationSourceV1{manifest: physical}, manifest: logical}
	pinned, err := source.PinVectorPartitionGenerationV1(t.Context(), manifest.IndexName, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	got := pinned.Manifest()
	if got.ReadySetDigest != want || got.IntegrityDigest != logical.IntegrityDigest || !slices.Equal(got.Placements, logical.Placements) {
		t.Fatalf("pinned manifest = %+v, want ready=%q integrity=%q placements=%v", got, want, logical.IntegrityDigest, logical.Placements)
	}
}
