package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This first developmental boundary must admit a committed replacement BEGIN
// before nonvoter enrollment and snapshot/tail promotion can be exercised. It
// deliberately uses the real catalog Raft provider and a fresh Nodes-only spare;
// configured member lists are not treated as actual Raft suffrage evidence.
func TestReplacementCannotVoteBeforeSnapshotAndTailReadyV1(t *testing.T) {
	configs := fixedPeerTestConfigsV1(t)
	address := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		result := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	group := configs[0].Groups[1]
	group.Peers = append([]raftcluster.Peer{configs[0].Groups[0].Peers[0]}, group.Peers...)
	spare := FixedPeerTCPNodeV1{ID: "replacement", Address: address()}
	nodes := append(append([]FixedPeerTCPNodeV1(nil), configs[0].Nodes...), spare)
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
	runtimes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	for i, cfg := range configs {
		runtime, err := OpenFixedPeerTCPRuntimeV1(cfg)
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
	leader := -1
	fixedPeerWaitV1(t, ctx, func() bool {
		for i := range configs[:3] {
			status, err := client.Status(ctx, configs[i].NodeID)
			if err == nil && status.CatalogRaft.State == "Leader" {
				leader = i
				return true
			}
		}
		return false
	})
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
	}{1, "replica-replacement-begin-v1", "replace-owner-2", client.digest, record.Epoch, record.Digest, group.ID, "owner-2", raftcluster.Peer{ID: spare.ID, Address: address()}})
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
	membership, err := client.PrepareReplicaReplacementV1(ctx, configs[leader].NodeID, operation)
	if err != nil {
		t.Fatalf("prepare actual nonvoter: %v", err)
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
	// BEGIN only authorizes preparation: it cannot publish a routing or ownership
	// change. Subsequent implementation extends this test through installed-snapshot
	// and durable receiver readiness, rather than inferring them here.
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

// Runtime shutdown remains once-only for consensus, transports and stores, but
// it must retain and retry the FSM's failed archive cleanup on later Close calls.
func TestFixedPeerRuntimeCloseRetriesSnapshotCleanupV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("snapshot requires relative namespace support")
	}
	c := fixedPeerTestConfigsV1(t)[0]
	c.Nodes, c.Catalog.Peers, c.Groups = c.Nodes[:1], c.Catalog.Peers[:1], c.Groups[:1]
	r, err := OpenFixedPeerTCPRuntimeV1(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
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
	if err := r.Close(); err == nil {
		t.Fatal("cleanup failure lost by runtime Close")
	}
	if _, err := os.Stat(blocker); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if _, err := os.Stat(snapshot.ArchivePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup target remains: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
}
