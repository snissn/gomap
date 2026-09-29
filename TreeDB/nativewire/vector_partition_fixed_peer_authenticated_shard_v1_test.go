package nativewire

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestFixedPeerAuthenticatedImmutableShardAddressPreflightV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	group := config.Groups[0].ID
	config.Vector = &FixedPeerTCPVectorConfigV1{
		Identity:        raftplacement.VectorPartitionLifecycleIdentityV1{Immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{ManifestDigest: strings.Repeat("a", 64)}},
		PublicAddresses: map[raftcluster.NodeID]string{config.NodeID: "127.0.0.1:20101"},
		ShardAddresses:  map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {config.NodeID: "192.168.0.185:20102"}},
	}
	if err := preflightFixedPeerConfigV1(config); err != nil {
		t.Fatalf("authenticated immutable private shard refused: %v", err)
	}
	if _, err := newPeerTransportSecurityV1(config); err == nil {
		t.Fatal("private shard admitted without local certificate hostname coverage")
	}
	ca := newPeerCAFixtureV1(t)
	config.Credentials = ca.issue(t, config.ClusterID, string(config.NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour), net.ParseIP("127.0.0.1"), net.ParseIP("192.168.0.185"))
	if _, err := newPeerTransportSecurityV1(config); err != nil {
		t.Fatalf("private shard with certificate hostname coverage refused: %v", err)
	}
	config.Vector.PublicAddresses[config.NodeID] = "192.168.0.185:20101"
	if err := preflightFixedPeerConfigV1(config); err == nil {
		t.Fatal("authenticated public listener left loopback")
	}
	config.Vector.PublicAddresses[config.NodeID] = "127.0.0.1:20101"
	config.Vector.ShardAddresses[group][config.NodeID] = "8.8.8.8:20102"
	if err := preflightFixedPeerConfigV1(config); err == nil {
		t.Fatal("public shard endpoint admitted")
	}
	config.Vector.ShardAddresses[group][config.NodeID] = "192.168.0.185:20102"
	config.Vector.Identity.Immutable = raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}
	if err := preflightFixedPeerConfigV1(config); err == nil {
		t.Fatal("credentialed legacy shard endpoint left loopback")
	}
}

// The two roles run as the same test binary on separate admitted Linux hosts.
// The caller supplies only fixture files and a private bind address; no SSH or
// machine-specific address belongs in the repository test.
func TestProductionTopologyDistinctHostAuthenticatedShardSearchV1(t *testing.T) {
	role := os.Getenv("GOMAP_AUTH_SHARD_ROLE")
	if role == "" {
		t.Skip("two-host authenticated shard fixture")
	}
	dir := os.Getenv("GOMAP_AUTH_SHARD_DIR")
	if dir == "" {
		t.Fatal("missing two-host fixture directory")
	}
	const cluster = "two-host-auth-shard"
	const group raftcluster.GroupID = "group-a"
	const serverNode raftcluster.NodeID = "ingress"
	if role == "server" {
		bind := os.Getenv("GOMAP_AUTH_SHARD_BIND")
		host, _, err := net.SplitHostPort(bind)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsPrivate() {
			t.Fatalf("private bind %q: %v", bind, err)
		}
		listener, err := net.Listen("tcp", bind)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ca := newPeerCAFixtureV1(t)
		for node, addresses := range map[raftcluster.NodeID][]net.IP{
			serverNode: {net.ParseIP("127.0.0.1"), net.ParseIP(host)},
			"owner-1":  {net.ParseIP("127.0.0.1")},
		} {
			issued := ca.issue(t, cluster, string(node), time.Now().Add(-time.Hour), time.Now().Add(time.Hour), addresses...)
			if err := copyTwoHostCredentialsV1(dir, string(node), issued); err != nil {
				t.Fatal(err)
			}
		}
		config := fixedPeerTestConfigsV1(t)[0]
		config.ClusterID = cluster
		config.Credentials = twoHostCredentialsV1(dir, string(serverNode))
		transport, err := NewPeerTransportV1(config)
		if err != nil {
			t.Fatal(err)
		}
		defer transport.Close()
		ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}
		catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
			Groups:     []raftplacement.GroupV1{{ID: group, Members: []raftcluster.NodeID{serverNode}, LeaderHint: serverNode}},
			Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: group, Mode: raftplacement.PlacementModeCollectionV1}},
		})
		if err != nil {
			t.Fatal(err)
		}
		placement := raftplacement.VectorPartitionPlacementRecordV1{
			Collection: ref, IndexName: "embedding", IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1,
			SourceGeneration: 11, SourceChecksum: 22, SourceSchemaHash: 33, SourceRowCount: 5,
			PartitionGeneration: 7, PartitionCount: 1,
			Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: group}},
		}
		_, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t, placement.Partitions,
			map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"remote-hit"}, [][]float32{{1, 0}})})
		coordinator.proof.NodeID, coordinator.progress.NodeID = serverNode, serverNode
		service, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
			Catalog: catalog, Placement: placement, LocalNodeID: serverNode, LocalGroupID: group,
			ReadCoordinator: coordinator, GenerationSource: source,
		})
		if err != nil {
			t.Fatal(err)
		}
		endpoint := listener.Addr().String()
		if !peerPrivateEndpointV1(endpoint) {
			t.Fatalf("listener escaped private endpoint: %q", endpoint)
		}
		topology, err := NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{
			Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{},
			ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{},
			Endpoints:           map[raftcluster.GroupID]string{group: endpoint},
			NodeEndpoints:       map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {serverNode: endpoint}},
			PeerTransport:       transport,
			Shards:              []VectorPartitionProductionShardV1{{GroupID: group, Listener: listener, Service: service, EndpointIdentity: "two-host"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer topology.Close()
		if err := os.WriteFile(filepath.Join(dir, "ready"), []byte(endpoint), 0600); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(dir, "done")); err == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("two-host client did not finish")
	}
	if role != "client" {
		t.Fatalf("unknown two-host role %q", role)
	}
	endpoint := os.Getenv("GOMAP_AUTH_SHARD_ENDPOINT")
	if !peerPrivateEndpointV1(endpoint) {
		t.Fatalf("invalid private endpoint %q", endpoint)
	}
	config := fixedPeerTestConfigsV1(t)[1]
	config.ClusterID = cluster
	config.Credentials = twoHostCredentialsV1(dir, "owner-1")
	transport, err := NewPeerTransportV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	dispatcher, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(transport,
		map[raftcluster.GroupID]string{group: endpoint},
		map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {serverNode: endpoint}})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	request.TargetNodeID = serverNode
	response, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Partials) != 1 || len(response.Partials[0].Neighbors) != 1 || response.Partials[0].Neighbors[0].ID != "remote-hit" ||
		response.Proof.ServingNode != serverNode || response.Proof.GroupID != group {
		t.Fatalf("two-host search result/proof: %+v", response)
	}
	t.Logf("two-host authenticated shard result=%s group=%s node=%s", response.Partials[0].Neighbors[0].ID, response.Proof.GroupID, response.Proof.ServingNode)
}

func twoHostCredentialsV1(dir, node string) *PeerCredentialsV1 {
	return &PeerCredentialsV1{
		TrustRootsFile:  filepath.Join(dir, "ca.pem"),
		CertificateFile: filepath.Join(dir, node+".pem"),
		PrivateKeyFile:  filepath.Join(dir, node+"-key.pem"),
	}
}

func copyTwoHostCredentialsV1(dir, node string, issued *PeerCredentialsV1) error {
	paths := [][2]string{{issued.TrustRootsFile, filepath.Join(dir, "ca.pem")}, {issued.CertificateFile, filepath.Join(dir, node+".pem")}, {issued.PrivateKeyFile, filepath.Join(dir, node+"-key.pem")}}
	for _, pair := range paths {
		raw, err := os.ReadFile(pair[0])
		if err != nil {
			return err
		}
		if err := os.WriteFile(pair[1], raw, 0600); err != nil {
			return fmt.Errorf("copy two-host credential: %w", err)
		}
	}
	return nil
}

func TestProductionTopologyAuthenticatedShardBoundaryV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	group := config.Groups[0].ID
	node := config.NodeID
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Groups:     []raftplacement.GroupV1{{ID: group, Members: []raftcluster.NodeID{node}, LeaderHint: node}},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: group, Mode: raftplacement.PlacementModeCollectionV1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: "embedding", IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1,
		SourceGeneration: 11, SourceChecksum: 22, SourceSchemaHash: 33, SourceRowCount: 5,
		PartitionGeneration: 7, PartitionCount: 1,
		Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: group}},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := listener.Addr().String()
	_, source, coordinator := newVectorPartitionShardSearchTestServiceV1(t, placement.Partitions,
		map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"local-hit"}, [][]float32{{1, 0}})})
	coordinator.proof.NodeID, coordinator.progress.NodeID = node, node
	service, err := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
		Catalog: catalog, Placement: placement, LocalNodeID: node, LocalGroupID: group,
		ReadCoordinator: coordinator, GenerationSource: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	topology, err := NewVectorPartitionProductionTopologyV1(VectorPartitionProductionTopologyOptionsV1{
		Catalog: catalog, Placement: placement,
		RouterSource:        &testVectorPartitionCoordinatorRouterSourceV1{},
		ReplicatedLifecycle: &recordingVectorPartitionReplicatedLifecycleAuthorityV1{},
		Endpoints:           map[raftcluster.GroupID]string{group: endpoint},
		NodeEndpoints:       map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {node: endpoint}},
		PeerTransport:       transport,
		Shards:              []VectorPartitionProductionShardV1{{GroupID: group, Listener: listener, Service: service, EndpointIdentity: "installed"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer topology.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	identity, err := transport.ProbeShardEndpointV1(ctx, endpoint, node, group)
	if err != nil || identity.GroupID != string(group) || identity.InstanceIdentity != "installed" {
		t.Fatalf("authenticated topology probe identity=%+v err=%v", identity, err)
	}
	dispatcher, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(transport,
		map[raftcluster.GroupID]string{group: endpoint},
		map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {node: endpoint}})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	request := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	request.TargetNodeID = node
	response, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request)
	if err != nil || len(response.Partials) != 1 || len(response.Partials[0].Neighbors) != 1 ||
		response.Partials[0].Neighbors[0].ID != "local-hit" || response.Proof.GroupID != group || response.Proof.ServingNode != node {
		t.Fatalf("authenticated same-group search response=%+v err=%v", response, err)
	}
	plain, err := net.Dial("tcp", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_, err = probeVectorPartitionShardConnV1(ctx, plain)
	_ = plain.Close()
	if err == nil {
		t.Fatal("plaintext probe reached authenticated shard")
	}
	if wrong, err := transport.dialScope(ctx, endpoint, "different-node", "shard:"+string(group)); err == nil {
		wrong.Close()
		t.Fatal("shard accepted the wrong certificate node identity")
	}
	untrustedConfig := config
	untrustedConfig.Credentials = peerCredentialsFixtureV1(t, config.ClusterID, string(node))
	untrusted, err := NewPeerTransportV1(untrustedConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if wrong, err := untrusted.ProbeShardEndpointV1(ctx, endpoint, node, group); err == nil {
		t.Fatalf("shard accepted a client certificate from another CA: %+v", wrong)
	}
	conn, err := transport.dialScope(ctx, endpoint, node, "shard:"+string(group))
	if err != nil {
		t.Fatal(err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	request.TargetGroupID = "different-group"
	err = writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{Request: &request}, vectorPartitionShardSearchTCPMaxFrameBytesV1)
	if err == nil {
		_, err = readVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPMaxFrameBytesV1)
	}
	_ = conn.Close()
	if err == nil {
		t.Fatal("authenticated shard accepted a wrong-group request")
	}
	if stats := service.Stats(); stats.Requests != 1 || stats.Successes != 1 {
		t.Fatalf("wrong-group request reached search service: %+v", stats)
	}
}
