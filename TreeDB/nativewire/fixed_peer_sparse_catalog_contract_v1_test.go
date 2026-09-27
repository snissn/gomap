package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestSparseCatalogConfigAcceptsMoreThan32NodesWithThreeVotersV1(t *testing.T) {
	c := fixedPeerTestConfigsV1(t)[0]
	for i := len(c.Nodes); i < 40; i++ {
		id := raftcluster.NodeID(fmt.Sprintf("data-%d", i))
		c.Nodes = append(c.Nodes, FixedPeerTCPNodeV1{ID: id, Address: fmt.Sprintf("127.2.0.%d:19000", i)})
		c.Groups = append(c.Groups, FixedPeerTCPGroupV1{
			ID: raftcluster.GroupID(fmt.Sprintf("data-group-%d", i)), BootstrapNode: id,
			Peers: []raftcluster.Peer{{ID: id, Address: fmt.Sprintf("127.2.1.%d:19000", i)}},
		})
	}
	client, err := NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if len(client.config.Nodes) != 40 || len(client.config.Catalog.Peers) != 3 || len(client.config.Groups) != 39 || len(client.config.RaftListen) != 2 {
		t.Fatalf("inventory changed quorum or hosting: %+v", client.config)
	}
	if client.addresses["data-39"] != "127.2.0.39:19000" {
		t.Fatal("missing indexed inventory endpoint")
	}
	c.Nodes[0].Address = "changed"
	c.Catalog.Peers[0].Address = "changed"
	c.Catalog.Features.Required[0].Name = "changed"
	c.RaftListen["meta"] = "changed"
	if client.config.Nodes[0].Address == "changed" || client.config.Catalog.Peers[0].Address == "changed" ||
		client.config.Catalog.Features.Required[0].Name == "changed" || client.config.RaftListen["meta"] == "changed" {
		t.Fatal("client retained mutable caller configuration")
	}
}

func TestSparseCatalogResourceBoundsRefuseBeforeOpeningV1(t *testing.T) {
	for _, kind := range []string{"nodes", "groups", "hosted", "identity", "utf8-identity", "utf8-path", "features"} {
		t.Run(kind, func(t *testing.T) {
			c := fixedPeerTestConfigsV1(t)[0]
			switch kind {
			case "nodes":
				c.Nodes = make([]FixedPeerTCPNodeV1, fixedPeerMaxNodesV1+1)
			case "groups":
				c.Groups = make([]FixedPeerTCPGroupV1, fixedPeerMaxDataGroupsV1+1)
			case "hosted":
				for i := 0; i < fixedPeerMaxHostedDataGroupsV1; i++ {
					id := raftcluster.GroupID(fmt.Sprintf("local-%d", i))
					address := fmt.Sprintf("127.3.0.%d:19000", i+1)
					c.Groups = append(c.Groups, FixedPeerTCPGroupV1{ID: id, BootstrapNode: c.NodeID, Peers: []raftcluster.Peer{{ID: c.NodeID, Address: address}}})
					c.RaftListen[id] = address
				}
			case "identity":
				c.ClusterID = "bad\ncluster"
			case "utf8-identity":
				c.ClusterID = string([]byte{0xff})
			case "utf8-path":
				c.DataRoot += string([]byte{0xff})
			case "features":
				c.Catalog.Features.Required = make([]raftcluster.RequiredFeature, 65)
			}
			runtime, err := OpenFixedPeerTCPRuntimeV1(c)
			if err == nil {
				runtime.Close()
				t.Fatal("accepted inadmissible resource inventory")
			}
			if !errors.Is(err, raftcluster.ErrInvalidConfig) {
				t.Fatalf("unclassified resource refusal: %v", err)
			}
			if _, err := os.Stat(c.RaftRoot); !os.IsNotExist(err) {
				t.Fatalf("opened a root before admission: %v", err)
			}
		})
	}
}

func TestSparseCatalogExplicitIdentityRequiresExactReopenV1(t *testing.T) {
	c := sparseCatalogTestConfigsV1(t)[3]
	c.ClusterID = "sparse-catalog-test"
	runtime, err := OpenFixedPeerTCPRuntimeV1(c)
	if err != nil {
		t.Fatal(err)
	}
	first, err := runtime.Status(context.Background())
	if err != nil || first.ClusterID != c.ClusterID || first.CatalogRole != "consumer" {
		t.Fatalf("explicit identity: %+v, %v", first, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime, err = OpenFixedPeerTCPRuntimeV1(c)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.Status(context.Background())
	if err != nil || second.RecoveryState != "reopened" || second.ConfigDigest != first.ConfigDigest {
		t.Fatalf("exact reopen: %+v, %v", second, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"cluster", "topology"} {
		changed := c
		if change == "cluster" {
			changed.ClusterID = "another-cluster"
		} else {
			changed.Nodes = append([]FixedPeerTCPNodeV1(nil), c.Nodes...)
			changed.Nodes = append(changed.Nodes, FixedPeerTCPNodeV1{ID: "new-node", Address: "127.4.0.1:19000"})
		}
		opened, err := OpenFixedPeerTCPRuntimeV1(changed)
		if opened != nil {
			opened.Close()
		}
		if !errors.Is(err, raftcluster.ErrInvalidConfig) {
			t.Fatalf("%s edit reused persistent identity: %v", change, err)
		}
	}
}

func sparseCatalogStartV1(t testing.TB, ctx context.Context, configs []FixedPeerTCPConfigV1, placements []raftplacement.CollectionPlacementV1) ([]*fixedPeerTestProcessV1, *FixedPeerTCPClientV1, raftplacement.CatalogV1) {
	t.Helper()
	client, err := NewFixedPeerTCPClientV1(configs[len(configs)-1])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	for i, c := range configs {
		processes[i] = fixedPeerStartTestProcessV1(t, c)
	}
	var leader raftcluster.NodeID
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, peer := range configs[0].Catalog.Peers {
			status, err := client.Status(ctx, peer.ID)
			if err == nil && status.CatalogRaft.State == "Leader" {
				leader = peer.ID
				return true
			}
		}
		return false
	})
	catalog := raftplacement.CatalogV1{Placements: placements}
	for _, group := range configs[0].Groups {
		entry := raftplacement.GroupV1{ID: group.ID}
		for _, peer := range group.Peers {
			entry.Members = append(entry.Members, peer.ID)
		}
		catalog.Groups = append(catalog.Groups, entry)
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
		for _, c := range configs {
			status, err := client.Status(ctx, c.NodeID)
			if err != nil {
				return false
			}
			for _, group := range status.Groups {
				if group.LeaderID == "" {
					return false
				}
			}
			if _, hosted := c.RaftListen[c.Catalog.ID]; hosted && status.Catalog.Epoch != 1 {
				return false
			}
		}
		return true
	})
	return processes, client, catalog
}

func sparseCatalogPlacementV1(name string, group raftcluster.GroupID) raftplacement.CollectionPlacementV1 {
	return raftplacement.CollectionPlacementV1{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: name}, GroupID: group}
}

func sparseCatalogUnusedAddressV1(t testing.TB, configs []FixedPeerTCPConfigV1) string {
	t.Helper()
	used := make(map[string]bool)
	for _, c := range configs {
		used[c.ListenAddress] = true
		for _, address := range c.RaftListen {
			used[address] = true
		}
	}
	for attempt := 0; attempt < 32; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		listener.Close()
		if !used[address] {
			return address
		}
	}
	t.Fatal("could not allocate a distinct Raft endpoint")
	return ""
}

func TestSparseCatalogConsumerRejectsTamperedRouteV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	configs := sparseCatalogTestConfigsV1(t)
	_, client, catalog := sparseCatalogStartV1(t, ctx, configs, []raftplacement.CollectionPlacementV1{sparseCatalogPlacementV1("sparse-users", "group-b")})
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "sparse-users", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, "consumer", request)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := client.Status(ctx, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	entry := fixedPeerCreateEntryV1(t, request.Collection, owner.Groups[0].CatalogVersion)
	valid := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&valid, request, route)
	for _, kind := range []string{"missing", "stale", "future", "digest", "group", "members", "partition", "token", "collection"} {
		t.Run(kind, func(t *testing.T) {
			metadata := valid
			metadata.ClusterRouteMembers = append([]string(nil), valid.ClusterRouteMembers...)
			switch kind {
			case "missing":
				metadata.CatalogMetaEpoch = 0
			case "stale":
				metadata.CatalogMetaDigest = "stale"
			case "future":
				metadata.CatalogMetaEpoch++
			case "digest":
				metadata.CatalogMetaDigest = fmt.Sprintf("%064d", 0)
			case "group":
				metadata.ClusterRouteGroupID = "group-a"
			case "members":
				metadata.ClusterRouteMembers[0] = "consumer"
			case "partition":
				metadata.ClusterRoutePartitionID = "other"
			case "token":
				metadata.ClusterRouteTokenKnown = true
				metadata.ClusterRouteToken = 42
			case "collection":
				metadata.ClusterRouteCollection = "other"
			}
			if result, err := client.Submit(ctx, "consumer", entry, metadata); err == nil || errors.Is(err, raftcluster.ErrCommitAmbiguous) || result.CommittedApplied {
				t.Fatalf("did not definitely reject tampered metadata: %+v, %v", result, err)
			}
		})
	}
	for _, operation := range []string{"catalog-read", "catalog-route", "catalog-validate"} {
		if _, err := client.call(ctx, "consumer", operation, fixedPeerRequestV1{Route: request, Metadata: valid}, false); !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
			t.Fatalf("%s on a nonparticipant: %v", operation, err)
		}
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(2, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{ExpectedEpoch: 1, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, "consumer", command); !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("nonparticipant accepted publication or lost definite refusal: %v", err)
	}
	leader, err := client.leader(ctx, configs[0].Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, leader, command); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Submit(ctx, "consumer", entry, valid); err == nil {
		t.Fatal("stale route bypassed the new quorum fence")
	}
	fresh, err := client.Route(ctx, "consumer", request)
	if err != nil || fresh.CatalogMetaEpoch != 2 || fresh.CatalogMetaDigest != record.Digest {
		t.Fatalf("consumer did not reacquire current authority: %+v, %v", fresh, err)
	}
}

func TestSparseCatalogNonVoterDataOwnerFailoverAndAuthorityLossV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	configs := sparseCatalogTestConfigsV1(t)
	consumer := &configs[3]
	address := sparseCatalogUnusedAddressV1(t, configs)
	group := FixedPeerTCPGroupV1{ID: "group-c", BootstrapNode: consumer.NodeID, Peers: []raftcluster.Peer{{ID: consumer.NodeID, Address: address}}}
	consumer.RaftListen[group.ID] = address
	for i := range configs {
		configs[i].Groups = append(append([]FixedPeerTCPGroupV1(nil), configs[i].Groups...), group)
	}
	processes, client, _ := sparseCatalogStartV1(t, ctx, configs, []raftplacement.CollectionPlacementV1{sparseCatalogPlacementV1("owned", "group-c"), sparseCatalogPlacementV1("refused", "group-c")})
	status, err := client.Status(ctx, "consumer")
	if err != nil || status.CatalogRole != "consumer" || status.CatalogRaft.GroupID != "" || status.Catalog.Epoch != 0 || len(status.Groups) != 1 {
		t.Fatalf("nonvoting data owner status: %+v, %v", status, err)
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "owned", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, "consumer", request)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	entry := fixedPeerCreateEntryV1(t, request.Collection, status.Groups[0].CatalogVersion)
	result, err := client.Submit(ctx, "consumer", entry, metadata)
	if err != nil || !result.CommittedApplied || !result.CommittedRecoverable || !result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != "group-c" {
		t.Fatalf("nonvoting owner failed durable submit: %+v, %v", result, err)
	}
	// Restart the consumer from exact roots. It must reacquire catalog
	// authority, rather than reconstruct authority from its configuration.
	processes[3].stop(t)
	processes[3] = fixedPeerStartTestProcessV1(t, configs[3])
	fixedPeerWaitV1(t, ctx, func() bool {
		s, e := client.Status(ctx, "consumer")
		return e == nil && s.RecoveryState == "reopened" && len(s.Groups) == 1 && s.Groups[0].LeaderID != "" && s.Groups[0].Applied.Index >= result.Evidence.Index
	})
	leader, err := client.leader(ctx, configs[0].Catalog)
	if err != nil {
		t.Fatal(err)
	}
	stopped := -1
	for i := 0; i < 3; i++ {
		if configs[i].NodeID == leader {
			stopped = i
			processes[i].stop(t)
			break
		}
	}
	if stopped < 0 {
		t.Fatal("catalog leader not in voter inventory")
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		_, err := client.Route(ctx, "consumer", request)
		return err == nil
	})
	// Obtain a valid route before losing quorum. The data group stays alive.
	request.Collection = "refused"
	route, err = client.Route(ctx, "consumer", request)
	if err != nil {
		t.Fatal(err)
	}
	ApplyClusterRouteMetadata(&metadata, request, route)
	status, err = client.Status(ctx, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	entry = fixedPeerCreateEntryV1(t, request.Collection, status.Groups[0].CatalogVersion)
	for i := 0; i < 3; i++ {
		if i != stopped {
			processes[i].stop(t)
			break
		}
	}
	if _, err := client.Route(ctx, "consumer", request); err == nil {
		t.Fatal("route succeeded without catalog quorum")
	}
	if _, err := client.Submit(ctx, "consumer", entry, metadata); err == nil {
		t.Fatal("previous route authorized a write without catalog quorum")
	}
	for _, process := range processes {
		process.stop(t)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(configs[3].DataRoot, "group-c"), CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.OpenCollection("owned"); err != nil {
		t.Fatalf("acknowledged collection lost on restart: %v", err)
	}
	if _, err := manager.OpenCollection("refused"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("mutation crossed lost catalog authority: %v", err)
	}
}

func TestSparseCatalogClientAdmissionIsBoundedAndIndependentV1(t *testing.T) {
	var client *FixedPeerTCPClientV1
	ready := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		<-ready
		json.NewEncoder(w).Encode(fixedPeerReplyV1{NodeID: client.config.NodeID, ConfigDigest: client.digest})
	}))
	defer server.Close()
	c := fixedPeerTestConfigsV1(t)[0]
	address := server.Listener.Addr().String()
	c.Nodes[0].Address, c.ListenAddress = address, address
	var err error
	client, err = NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	defer client.Close()
	for i := 0; i < cap(client.calls); i++ {
		client.calls <- struct{}{}
	}
	// Full mutation admission refuses before sending, while reads still work.
	if _, err := client.Submit(context.Background(), c.NodeID, nil, ClusterRequestMetadata{}); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("mutation admission: %v", err)
	}
	if _, err := client.Status(context.Background(), c.NodeID); err != nil {
		t.Fatalf("mutation saturation starved reads: %v", err)
	}
	for i := 0; i < cap(client.readCalls); i++ {
		client.readCalls <- struct{}{}
	}
	if _, err := client.Status(context.Background(), c.NodeID); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("read admission: %v", err)
	}
	if cap(client.calls) != fixedPeerClientInflightV1 || cap(client.readCalls) != fixedPeerClientInflightV1 ||
		client.http.Transport.(*http.Transport).MaxIdleConns != fixedPeerClientInflightV1 ||
		client.readHTTP.Transport.(*http.Transport).MaxIdleConns != fixedPeerClientInflightV1 {
		t.Fatal("client resources scale with inventory")
	}
}

func TestSparseCatalogSaturationPreservesAuthoritativeReadProgressV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	all := sparseCatalogTestConfigsV1(t)
	configs := []FixedPeerTCPConfigV1{all[0], all[3]}
	nodes := []FixedPeerTCPNodeV1{all[0].Nodes[0], all[0].Nodes[3]}
	for i := range configs {
		configs[i].Nodes = nodes
		configs[i].Catalog.Peers = configs[i].Catalog.Peers[:1]
		configs[i].Groups = configs[i].Groups[:1]
	}
	runtimes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	for i, config := range configs {
		runtime, err := OpenFixedPeerTCPRuntimeV1(config)
		if err != nil {
			t.Fatal(err)
		}
		runtimes[i] = runtime
		t.Cleanup(func() {
			if err := runtime.Close(); err != nil {
				t.Error(err)
			}
		})
		runtime.client.http.Transport.(*http.Transport).MaxConnsPerHost = 1
		runtime.client.readHTTP.Transport.(*http.Transport).MaxConnsPerHost = 1
	}
	client, err := NewFixedPeerTCPClientV1(configs[1])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var version uint64
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := client.Status(ctx, "ingress")
		if err != nil || status.CatalogRaft.State != "Leader" || len(status.Groups) != 1 || status.Groups[0].State != "Leader" {
			return false
		}
		version = status.Groups[0].CatalogVersion
		return true
	})
	catalog := raftplacement.CatalogV1{
		Groups:     []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"ingress"}}},
		Placements: []raftplacement.CollectionPlacementV1{sparseCatalogPlacementV1("saturated", "group-a")},
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, "ingress", command); err != nil {
		t.Fatal(err)
	}
	hold := func(slots chan struct{}, count int) func() {
		for i := 0; i < count; i++ {
			slots <- struct{}{}
		}
		return func() {
			for i := 0; i < count; i++ {
				<-slots
			}
		}
	}
	// No ordinary ingress slot is available on the catalog voter. Exactly
	// one read slot remains, so a nested self-read from an authoritative read
	// handler would fail. Forwarding and consumer ingress each retain one slot.
	defer hold(runtimes[0].requests, cap(runtimes[0].requests))()
	defer hold(runtimes[0].reads, cap(runtimes[0].reads)-1)()
	defer hold(runtimes[0].forwards, cap(runtimes[0].forwards)-1)()
	defer hold(runtimes[1].requests, cap(runtimes[1].requests)-1)()
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "saturated", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, "consumer", request)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	result, err := client.Submit(ctx, "consumer", fixedPeerCreateEntryV1(t, request.Collection, version), metadata)
	if err != nil || !result.CommittedApplied || !result.Evidence.ProvesProductionConsensus() {
		t.Fatalf("nested RPC admission lost progress: %+v, %v", result, err)
	}
}
