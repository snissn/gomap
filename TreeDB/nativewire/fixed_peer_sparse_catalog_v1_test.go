package nativewire

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// The fourth process consumes catalog decisions but is neither a catalog voter
// nor a data replica. Merely changing configuration limits cannot satisfy the
// route, production-consensus, durable apply and no-local-store assertions.
func TestSparseCatalogNonVoterIngressRoutesWithVerifiedProofV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs := sparseCatalogTestConfigsV1(t)
	client, err := NewFixedPeerTCPClientV1(configs[3])
	if err != nil {
		t.Fatalf("nonvoting ingress configuration must be accepted: %v", err)
	}
	defer client.Close()
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	for i := range configs {
		processes[i] = fixedPeerStartTestProcessV1(t, configs[i])
	}
	var leader raftcluster.NodeID
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, c := range configs[:3] {
			s, e := client.Status(ctx, c.NodeID)
			if e == nil && s.CatalogRaft.State == "Leader" && s.CatalogRaft.LeaderID == c.NodeID {
				leader = c.NodeID
				return true
			}
		}
		return false
	})
	catalog := raftplacement.CatalogV1{
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"ingress"}},
			{ID: "group-b", Members: []raftcluster.NodeID{"owner-1", "owner-2"}},
		},
		Placements: []raftplacement.CollectionPlacementV1{
			{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "sparse-users"}, GroupID: "group-b"},
		},
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, leader, command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for i, c := range configs {
			s, e := client.Status(ctx, c.NodeID)
			if e != nil {
				return false
			}
			if i < 3 && s.Catalog.Epoch != 1 {
				return false
			}
			for _, group := range s.Groups {
				if group.LeaderID == "" {
					return false
				}
			}
		}
		return true
	})
	consumer := configs[3]
	status, err := client.Status(ctx, consumer.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if status.CatalogRaft.GroupID != "" || status.Catalog.Epoch != 0 || len(status.Groups) != 0 {
		t.Fatalf("consumer falsely reports local replicated state: %+v", status)
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "sparse-users", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, consumer.NodeID, request)
	if err != nil {
		t.Fatal(err)
	}
	if route.GroupID != "group-b" || route.CatalogMetaEpoch != 1 || route.CatalogMetaDigest != record.Digest {
		t.Fatalf("route lacks current catalog authority: %+v", route)
	}
	owner, err := client.Status(ctx, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	entry := fixedPeerCreateEntryV1(t, request.Collection, owner.Groups[0].CatalogVersion)
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	result, err := client.Submit(ctx, consumer.NodeID, entry, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != "group-b" ||
		result.Evidence.NodeID == consumer.NodeID || !result.CommittedApplied || !result.CommittedRecoverable {
		t.Fatalf("missing remote durable consensus evidence: %+v", result)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, id := range []raftcluster.NodeID{"owner-1", "owner-2"} {
			s, e := client.Status(ctx, id)
			if e != nil || len(s.Groups) != 1 || s.Groups[0].Applied.Index < result.Evidence.Index {
				return false
			}
		}
		return true
	})
	if retry, err := client.Submit(ctx, consumer.NodeID, entry, metadata); err != nil || retry.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied {
		t.Fatalf("retry lost idempotency: %+v, %v", retry, err)
	}
	if _, err := os.Stat(consumer.DataRoot); !os.IsNotExist(err) {
		t.Fatalf("storage-free consumer opened a data root: %v", err)
	}
	for _, group := range append([]FixedPeerTCPGroupV1{consumer.Catalog}, consumer.Groups...) {
		if _, err := os.Stat(filepath.Join(consumer.RaftRoot, string(group.ID))); !os.IsNotExist(err) {
			t.Fatalf("consumer opened unassigned Raft group %s: %v", group.ID, err)
		}
	}
	for _, p := range processes {
		p.stop(t)
	}
	// Reopen the actual stores after process exit, rather than inferring
	// replication solely from client acknowledgement.
	for i, c := range configs[:3] {
		group := "group-b"
		if i == 0 {
			group = "group-a"
		}
		db, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(c.DataRoot, group), CommandWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		_, openErr := collections.NewCollectionManager(db).OpenCollection(request.Collection)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if !errors.Is(openErr, collections.ErrCollectionNotFound) {
				t.Fatalf("wrong-group mutation: %v", openErr)
			}
		} else if openErr != nil {
			t.Fatalf("%s lost replicated collection: %v", c.NodeID, openErr)
		}
	}
	for _, c := range configs {
		for _, address := range append([]string{c.ListenAddress}, fixedPeerTestLocalRaftAddressesV1(c)...) {
			listener, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatalf("listener leaked %s: %v", address, err)
			}
			listener.Close()
		}
	}
}

func sparseCatalogTestConfigsV1(t testing.TB) []FixedPeerTCPConfigV1 {
	t.Helper()
	configs := fixedPeerTestConfigsV1(t)
	used := make(map[string]bool)
	for _, c := range configs {
		used[c.ListenAddress] = true
		for _, address := range c.RaftListen {
			used[address] = true
		}
	}
	var address string
	for attempt := 0; attempt < 32; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		candidate := listener.Addr().String()
		listener.Close()
		if !used[candidate] {
			address = candidate
			break
		}
	}
	if address == "" {
		t.Fatal("could not allocate a distinct consumer endpoint")
	}
	root := t.TempDir()
	consumer := configs[0]
	consumer.NodeID = "consumer"
	consumer.ListenAddress = address
	consumer.DataRoot = filepath.Join(root, "data")
	consumer.RaftRoot = filepath.Join(root, "raft")
	consumer.RaftListen = map[raftcluster.GroupID]string{}
	nodes := append(append([]FixedPeerTCPNodeV1(nil), configs[0].Nodes...), FixedPeerTCPNodeV1{ID: consumer.NodeID, Address: address})
	configs = append(configs, consumer)
	for i := range configs {
		configs[i].Nodes = nodes
	}
	return configs
}
