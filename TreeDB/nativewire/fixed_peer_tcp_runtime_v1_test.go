package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func fixedPeerReadyV1(t testing.TB, ctx context.Context, names []string) ([]FixedPeerTCPConfigV1, []*fixedPeerTestProcessV1, *FixedPeerTCPClientV1) {
	t.Helper()
	configs := fixedPeerTestConfigsV1(t)
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	for i := range configs {
		processes[i] = fixedPeerStartTestProcessV1(t, configs[i])
	}
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	wait := func(check func() bool) { fixedPeerWaitV1(t, ctx, check) }
	var metaLeader raftcluster.NodeID
	wait(func() bool {
		for _, c := range configs {
			s, e := client.Status(ctx, c.NodeID)
			// Raft can elect a leader before that process finishes opening its
			// data stores and HTTP listener. A follower's hint is not RPC readiness.
			if e == nil && s.CatalogRaft.State == "Leader" && s.CatalogRaft.LeaderID == c.NodeID {
				metaLeader = s.CatalogRaft.LeaderID
				return true
			}
		}
		return false
	})
	catalog := raftplacement.CatalogV1{
		Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"ingress"}}, {ID: "group-b", Members: []raftcluster.NodeID{"owner-1", "owner-2"}}},
	}
	for _, name := range names {
		catalog.Placements = append(catalog.Placements, raftplacement.CollectionPlacementV1{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: name}, GroupID: "group-b"})
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, metaLeader, command); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		for _, c := range configs {
			s, e := client.Status(ctx, c.NodeID)
			if e != nil || s.Catalog.Epoch != 1 {
				return false
			}
			for _, g := range s.Groups {
				if g.LeaderID == "" {
					return false
				}
			}
		}
		return true
	})
	return configs, processes, client
}

func fixedPeerWaitV1(t testing.TB, ctx context.Context, check func() bool) {
	t.Helper()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestFixedPeerTCPAdmissionSaturationPreservesNestedRPCsV1(t *testing.T) {
	c := fixedPeerTestConfigsV1(t)[0]
	c.Nodes = c.Nodes[:1]
	c.Catalog.Peers = c.Catalog.Peers[:1]
	c.Groups = c.Groups[:1]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := OpenFixedPeerTCPRuntimeV1(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	// One forward occupies the entire mutation connection pool. Its catalog
	// reads must progress independently, even when the leader is this process.
	r.client.http.Transport.(*http.Transport).MaxConnsPerHost = 1
	client, err := NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		s, err := client.Status(ctx, c.NodeID)
		return err == nil && s.CatalogRaft.State == "Leader" && len(s.Groups) == 1 && s.Groups[0].State == "Leader"
	})
	catalog := raftplacement.CatalogV1{
		Groups:     []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{c.NodeID}}},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"}, GroupID: "group-a"}},
	}
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
	// Deterministically reserve all but one outer-handler slot; each real TCP
	// request below takes the last slot while issuing its nested requests.
	reserved := cap(r.requests) - 1
	for i := 0; i < reserved; i++ {
		r.requests <- struct{}{}
	}
	defer func() {
		for i := 0; i < reserved; i++ {
			<-r.requests
		}
	}()
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "users", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, c.NodeID, request)
	if err != nil {
		t.Fatalf("admitted route starved its nested catalog RPC: %v", err)
	}
	status, err := client.Status(ctx, c.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	result, err := client.Submit(ctx, c.NodeID, fixedPeerCreateEntryV1(t, "users", status.Groups[0].CatalogVersion), metadata)
	if err != nil || result.Evidence.Index == 0 {
		t.Fatalf("admitted submit starved its nested forward/read RPC: result=%+v err=%v", result, err)
	}
	r.requests <- struct{}{}
	reserved++
	if _, err := client.Route(ctx, c.NodeID, request); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("outer admission overflow=%v", err)
	}
	if _, err := client.Status(ctx, c.NodeID); err != nil {
		t.Fatalf("saturated ingress blocked independent status: %v", err)
	}
}

func TestFixedPeerTCPSnapshotRestoreTracksCurrentCatalogVersionV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("Raft snapshot install requires durable rename and removal namespaces")
	}
	// Keep normal snapshot settings; explicitly persist a provider snapshot and
	// restart so HashiCorp restores it onto the replacement FSM-owned DB.
	c := fixedPeerTestConfigsV1(t)[0]
	c.Nodes = c.Nodes[:1]
	c.Catalog.Peers = c.Catalog.Peers[:1]
	c.Groups = c.Groups[:1]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	open := func() *FixedPeerTCPRuntimeV1 {
		r, err := OpenFixedPeerTCPRuntimeV1(c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		})
		fixedPeerWaitV1(t, ctx, func() bool {
			s, err := r.Status(ctx)
			return err == nil && s.CatalogRaft.State == "Leader" && len(s.Groups) == 1 && s.Groups[0].State == "Leader"
		})
		return r
	}
	r := open()
	client, err := NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	catalog := raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{c.NodeID}}}}
	for _, name := range []string{"users", "after", "stale"} {
		catalog.Placements = append(catalog.Placements, raftplacement.CollectionPlacementV1{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: name}, GroupID: "group-a"})
	}
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
	submit := func(name string, version uint64) (raftcluster.SubmitResultV1, error) {
		request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: name, Shape: ClusterRouteShapeCollection}
		route, err := client.Route(ctx, c.NodeID, request)
		if err != nil {
			t.Fatal(err)
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		return client.Submit(ctx, c.NodeID, fixedPeerCreateEntryV1(t, name, version), metadata)
	}
	before, err := r.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := submit("users", before.Groups[0].CatalogVersion)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := r.data["group-a"].provider.Snapshot(ctx)
	if err != nil || snapshot.LastIncludedIndex < first.Evidence.Index || snapshot.SizeBytes == 0 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r = open()
	if _, err := r.data["group-a"].db.Get([]byte("snapshot-restore-check")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("snapshot did not replace the caller-owned DB: %v", err)
	}
	before, err = r.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.RecoveryState != "reopened" || before.Groups[0].Applied.Index < snapshot.LastIncludedIndex {
		t.Fatalf("snapshot did not recover: %+v", before)
	}
	second, err := submit("after", before.Groups[0].CatalogVersion)
	if err != nil {
		t.Fatal(err)
	}
	if !second.CommittedRecoverable || second.CatalogVersion <= before.Groups[0].CatalogVersion {
		t.Errorf("post-restore submit version=%d, before=%d; evidence=%+v", second.CatalogVersion, before.Groups[0].CatalogVersion, second.Evidence)
	}
	after, err := r.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Groups[0].CatalogVersion <= before.Groups[0].CatalogVersion || after.Groups[0].CatalogVersion != second.CatalogVersion {
		t.Errorf("post-restore status version=%d, before=%d, submit=%d", after.Groups[0].CatalogVersion, before.Groups[0].CatalogVersion, second.CatalogVersion)
	}
	if _, err := submit("stale", before.Groups[0].CatalogVersion); !errors.Is(err, raftcluster.ErrCatalogVersionMismatch) {
		t.Fatalf("fresh stale-guard write after snapshot restore: %v", err)
	}
	t.Logf("restored snapshot index=%d, old DB closed, current catalog advanced %d -> %d; stale guard rejected", snapshot.LastIncludedIndex, before.Groups[0].CatalogVersion, after.Groups[0].CatalogVersion)
}

// The ingress has no group-b database or provider. A successful write can only
// reach group-b through the runtime's TCP forwarding boundary.
func TestFixedPeerTCPRuntimeRemoteOwnerWriteCommitsAppliesAndReplicatesV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, processes, client := fixedPeerReadyV1(t, ctx, []string{"users", "users2"})
	wait := func(check func() bool) { fixedPeerWaitV1(t, ctx, check) }
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "users", Shape: ClusterRouteShapeCollection}
	route, err := client.Route(ctx, "ingress", request)
	if err != nil {
		t.Fatal(err)
	}
	if route.GroupID != "group-b" {
		t.Fatalf("route=%+v", route)
	}
	owner, err := client.Status(ctx, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	entry := fixedPeerCreateEntryV1(t, "users", owner.Groups[0].CatalogVersion)
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	started := time.Now()
	result, err := client.Submit(ctx, "ingress", entry, metadata)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("remote route/commit/apply duration=%s evidence=%+v", time.Since(started), result.Evidence)
	if !result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != "group-b" || result.Evidence.NodeID != result.Evidence.LeaderID || result.Evidence.NodeID == "ingress" || !result.CommittedApplied || !result.CommittedRecoverable || result.Evidence.Index == 0 {
		t.Fatalf("result=%+v", result)
	}
	wait(func() bool {
		for _, id := range []raftcluster.NodeID{"owner-1", "owner-2"} {
			s, e := client.Status(ctx, id)
			if e != nil || len(s.Groups) != 1 || s.Groups[0].Applied.Index < result.Evidence.Index || s.Groups[0].CommitIndex < result.Evidence.Index {
				return false
			}
		}
		return true
	})
	before, err := client.Status(ctx, "ingress")
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Groups) != 1 || before.Groups[0].GroupID != "group-a" || before.Groups[0].Applied.HasApplied {
		t.Fatalf("local bypass: %+v", before)
	}
	if _, err := os.Stat(filepath.Join(configs[0].DataRoot, "group-b")); !os.IsNotExist(err) {
		t.Fatalf("ingress unexpectedly opened owner root: %v", err)
	}
	// An exact retry must preserve the durable idempotency result.
	if retry, err := client.Submit(ctx, "ingress", entry, metadata); err != nil || retry.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied {
		t.Fatalf("retry: %+v, %v", retry, err)
	}
	otherRequest := request
	otherRequest.Collection = "users2"
	otherRoute, err := client.Route(ctx, "ingress", otherRequest)
	if err != nil {
		t.Fatal(err)
	}
	otherMetadata := metadata
	ApplyClusterRouteMetadata(&otherMetadata, otherRequest, otherRoute)
	if _, err := client.Submit(ctx, "ingress", entry, otherMetadata); !errors.Is(err, raftplacement.ErrCatalogMetaRouteMismatch) {
		t.Fatalf("accepted command with another collection's valid proof: %v", err)
	}
	metadata.CatalogMetaDigest = "stale"
	if _, err := client.Submit(ctx, "ingress", entry, metadata); err == nil {
		t.Fatal("accepted stale catalog proof")
	}
	metadata.CatalogMetaDigest = route.CatalogMetaDigest
	metadata.ClusterRouteShape = "token_batch"
	if _, err := client.Submit(ctx, "ingress", entry, metadata); err == nil {
		t.Fatal("accepted token fanout")
	}
	metadata.ClusterRouteShape = "collection"
	// Restart a follower from exactly the same data/Raft roots and sockets.
	restart := 1
	if result.Evidence.LeaderID == configs[restart].NodeID {
		restart = 2
	}
	old, err := client.Status(ctx, configs[restart].NodeID)
	if err != nil {
		t.Fatal(err)
	}
	processes[restart].stop(t)
	started = time.Now()
	processes[restart] = fixedPeerStartTestProcessV1(t, configs[restart])
	wait(func() bool {
		s, e := client.Status(ctx, configs[restart].NodeID)
		return e == nil && s.RecoveryState == "reopened" && s.Catalog.Epoch >= old.Catalog.Epoch && s.Catalog.Digest == old.Catalog.Digest && len(s.Groups) == 1 && s.Groups[0].Applied.Index >= old.Groups[0].Applied.Index && s.Groups[0].CommitIndex >= result.Evidence.Index
	})
	t.Logf("persistent follower recovery=%s", time.Since(started))
	// A two-voter owner group cannot commit with one member stopped.
	request.Collection = "users2"
	freshRoute, err := client.Route(ctx, "ingress", request)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = client.Status(ctx, result.Evidence.LeaderID)
	if err != nil {
		t.Fatal(err)
	}
	freshEntry := fixedPeerCreateEntryV1(t, "users2", owner.Groups[0].CatalogVersion)
	ApplyClusterRouteMetadata(&metadata, request, freshRoute)
	processes[restart].stop(t)
	// The remaining two catalog voters must still authorize the route; this
	// rejection must not merely be a transient missing meta leader on restart.
	wait(func() bool { _, err := client.Route(ctx, "ingress", request); return err == nil })
	bounded, cancelBounded := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancelBounded()
	if _, err := client.Submit(bounded, "ingress", freshEntry, metadata); err == nil {
		t.Fatal("accepted fresh write without owner quorum")
	} else if !errors.Is(err, raftcluster.ErrCommitAmbiguous) && !errors.Is(err, raftcluster.ErrNotLeader) && !errors.Is(err, raftcluster.ErrAdmissionUnavailable) && !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
		t.Fatalf("unclassified quorum failure: %v", err)
	}
	for _, p := range processes {
		p.stop(t)
	}
	t.Logf("persistent bytes after replicated create/retry/restart=%d", fixedPeerPersistentBytesV1(t, configs))
	for _, c := range configs {
		group := "group-b"
		if c.NodeID == "ingress" {
			group = "group-a"
		}
		db, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(c.DataRoot, group), CommandWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
		closeErr := db.Close()
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if c.NodeID == "ingress" {
			if !errors.Is(openErr, collections.ErrCollectionNotFound) {
				t.Fatalf("wrong-group mutation: %v", openErr)
			}
		} else if openErr != nil {
			t.Fatalf("%s lost replicated collection: %v", c.NodeID, openErr)
		}
	}
	for _, c := range configs {
		for _, address := range append([]string{c.ListenAddress}, fixedPeerTestLocalRaftAddressesV1(c)...) {
			listener, e := net.Listen("tcp", address)
			if e != nil {
				t.Fatalf("listener leaked %s: %v", address, e)
			}
			listener.Close()
		}
	}
}

// This intentionally bounded benchmark measures the real client path; only
// parent/client allocations are reported, not allocations in the three nodes.
func BenchmarkFixedPeerTCPRemoteOwnerCreateV1(b *testing.B) {
	b.StopTimer()
	if b.N > 1000 {
		b.Skip("bounded conformance benchmark: use -benchtime=10x")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	names := make([]string, b.N)
	for i := range names {
		names[i] = fmt.Sprintf("users-%d", i)
	}
	configs, processes, client := fixedPeerReadyV1(b, ctx, names)
	metadata := make([]ClusterRequestMetadata, b.N)
	for i, name := range names {
		request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: name, Shape: ClusterRouteShapeCollection}
		route, err := client.Route(ctx, "ingress", request)
		if err != nil {
			b.Fatal(err)
		}
		metadata[i].AckPolicy = iwire.AckRaftCommitted
		ApplyClusterRouteMetadata(&metadata[i], request, route)
	}
	owner, err := client.Status(ctx, "owner-1")
	if err != nil {
		b.Fatal(err)
	}
	version := owner.Groups[0].CatalogVersion
	traffic := &fixedPeerMeasuredTransportV1{Transport: client.http.Transport.(*http.Transport)}
	client.http.Transport = traffic
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i, name := range names {
		entry := fixedPeerCreateEntryV1(b, name, version)
		result, err := client.Submit(ctx, "ingress", entry, metadata[i])
		if err != nil {
			b.Fatal(err)
		}
		if !result.CommittedApplied || !result.CommittedRecoverable || !result.Evidence.ProvesProductionConsensus() {
			b.Fatalf("missing proof: %+v", result)
		}
		version = result.CatalogVersion
	}
	b.StopTimer()
	for _, p := range processes {
		p.stop(b)
	}
	b.ReportMetric(float64(fixedPeerPersistentBytesV1(b, configs)), "persistent_bytes")
	b.ReportMetric(float64(traffic.requestBytes.Load())/float64(b.N), "ingress_req_B/op")
}

// Count only outer client request bodies; this is not internal Raft/network
// traffic. Embedding preserves CloseIdleConnections for normal client cleanup.
type fixedPeerMeasuredTransportV1 struct {
	*http.Transport
	requestBytes atomic.Int64
}

func (t *fixedPeerMeasuredTransportV1) RoundTrip(request *http.Request) (*http.Response, error) {
	t.requestBytes.Add(request.ContentLength)
	return t.Transport.RoundTrip(request)
}

func fixedPeerPersistentBytesV1(t testing.TB, configs []FixedPeerTCPConfigV1) int64 {
	t.Helper()
	var total int64
	for _, c := range configs {
		for _, root := range []string{c.DataRoot, c.RaftRoot} {
			err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
				if err == nil && info.Mode().IsRegular() {
					total += info.Size()
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	return total
}

func TestFixedPeerTCPConfigRefusesInvalidAndChangedIdentityV1(t *testing.T) {
	for _, name := range []string{"duplicate-node", "duplicate-address", "duplicate-raft", "feature-floor", "missing-bootstrap", "overlapping-roots", "missing-listen", "rpc-listen-mismatch", "raft-listen-mismatch", "unknown-peer"} {
		t.Run(name, func(t *testing.T) {
			c := fixedPeerTestConfigsV1(t)[0]
			switch name {
			case "duplicate-node":
				c.Nodes[1].ID = c.Nodes[0].ID
			case "duplicate-address":
				c.Nodes[1].Address = c.Nodes[0].Address
			case "duplicate-raft":
				c.Groups[0].Peers[0].Address = c.Catalog.Peers[0].Address
			case "feature-floor":
				c.Catalog.Peers[1].Capabilities.Required[1].Version.Major = 99
			case "missing-bootstrap":
				c.Catalog.BootstrapNode = "absent"
			case "overlapping-roots":
				c.RaftRoot = filepath.Join(c.DataRoot, "raft")
			case "missing-listen":
				delete(c.RaftListen, c.Catalog.ID)
			case "rpc-listen-mismatch":
				c.ListenAddress = c.Nodes[1].Address
			case "raft-listen-mismatch":
				c.RaftListen[c.Catalog.ID] = c.Catalog.Peers[1].Address
			case "unknown-peer":
				c.Catalog.Peers[2].ID = "unknown"
			}
			if client, err := NewFixedPeerTCPClientV1(c); err == nil {
				client.Close()
				t.Fatal("accepted invalid fixed configuration")
			}
		})
	}
	c := fixedPeerTestConfigsV1(t)[0]
	r, err := OpenFixedPeerTCPRuntimeV1(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	changed := c
	changed.RequestTimeout += time.Millisecond
	if other, err := OpenFixedPeerTCPRuntimeV1(changed); err == nil {
		other.Close()
		t.Fatal("reopened with changed identity")
	}
	if err := os.WriteFile(filepath.Join(c.RaftRoot, "fixed-peer-v1.json"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenFixedPeerTCPRuntimeV1(c); err == nil {
		other.Close()
		t.Fatal("reopened corrupt configuration")
	}
}

func TestFixedPeerTCPPostSendCancellationIsCommitAmbiguousV1(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-release
	}))
	defer server.Close()
	defer close(release)
	c := fixedPeerTestConfigsV1(t)[0]
	c.Nodes[0].Address = strings.TrimPrefix(server.URL, "http://")
	c.ListenAddress = c.Nodes[0].Address
	client, err := NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.Submit(ctx, c.Nodes[0].ID, []byte("entry"), ClusterRequestMetadata{})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request not received")
	}
	cancel()
	if err := <-done; !errors.Is(err, raftcluster.ErrCommitAmbiguous) || !errors.Is(err, context.Canceled) {
		t.Fatalf("post-send outcome=%v", err)
	}
	if _, err := client.Submit(ctx, c.Nodes[0].ID, nil, ClusterRequestMetadata{}); !errors.Is(err, context.Canceled) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("pre-send outcome=%v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("automatic retry: %d", requests.Load())
	}
}

func TestFixedPeerTCPConnectionRefusedIsNotCommitAmbiguousV1(t *testing.T) {
	c := fixedPeerTestConfigsV1(t)[0] // No process is listening on these ports.
	client, err := NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Submit(context.Background(), c.NodeID, nil, ClusterRequestMetadata{}); err == nil || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("definite connection failure=%v", err)
	}
}

func TestFixedPeerTCPLeaderDiscoveryCancelsBlackholedFirstPeerV1(t *testing.T) {
	c := fixedPeerTestConfigsV1(t)[0]
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var blackhole atomic.Bool
	blackhole.Store(true)
	requests := make([]atomic.Int32, len(c.Nodes))
	for i := range c.Nodes {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			requests[i].Add(1)
			_, _ = io.Copy(io.Discard, request.Body)
			if request.URL.Path != "/v1/status" {
				t.Errorf("discovery issued non-status request: %s", request.URL.Path)
				return
			}
			if i == 0 && blackhole.Load() {
				// Accept the first peer's TCP request but never send a response.
				close(entered)
				select {
				case <-request.Context().Done():
					close(canceled)
				case <-release:
				}
				return
			}
			// Ensure a successful discovery cannot bypass the blackholed probe.
			select {
			case <-entered:
			case <-request.Context().Done():
				return
			}
			_ = json.NewEncoder(w).Encode(fixedPeerReplyV1{
				NodeID: raftcluster.NodeID(request.Header.Get("X-TreeDB-Node")), ConfigDigest: request.Header.Get("X-TreeDB-Config"),
				Status: FixedPeerTCPStatusV1{CatalogRaft: raftcluster.RuntimeStatusV1{GroupID: c.Catalog.ID, LeaderID: c.Nodes[1].ID}},
			})
		}))
		t.Cleanup(server.Close)
		c.Nodes[i].Address = strings.TrimPrefix(server.URL, "http://")
	}
	c.ListenAddress = c.Nodes[0].Address
	client, err := NewFixedPeerTCPClientV1(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leader, err := client.leader(ctx, c.Catalog)
	if err != nil || leader != c.Nodes[1].ID {
		t.Fatalf("healthy later peer did not supply leader within outer deadline: leader=%q err=%v", leader, err)
	}
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("blackholed probe was not canceled before the outer deadline")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("discovery exhausted outer deadline: %v", err)
	}
	// Once the first peer responds normally, discovery must not fan out to
	// later peers. Keep the same TCP endpoints and a fresh caller deadline.
	blackhole.Store(false)
	before := make([]int32, len(requests))
	for i := range requests {
		before[i] = requests[i].Load()
	}
	healthyCtx, healthyCancel := context.WithTimeout(context.Background(), time.Second)
	defer healthyCancel()
	if leader, err := client.leader(healthyCtx, c.Catalog); err != nil || leader != c.Nodes[1].ID {
		t.Fatalf("healthy-first discovery: leader=%q err=%v", leader, err)
	}
	for i := range requests {
		want := int32(0)
		if i == 0 {
			want = 1
		}
		if got := requests[i].Load() - before[i]; got != want {
			t.Errorf("healthy-first peer %d requests=%d, want %d", i, got, want)
		}
	}
}

func TestFixedPeerTCPRejectsMalformedFramesV1(t *testing.T) {
	r := &FixedPeerTCPRuntimeV1{config: FixedPeerTCPConfigV1{NodeID: "node-a", RequestTimeout: time.Second}, client: &FixedPeerTCPClientV1{digest: "config"}, reads: make(chan struct{}, 1)}
	for _, tt := range []struct{ name, body, node, digest string }{
		{"unknown-field", `{"unknown":true}`, "node-a", "config"},
		{"trailing", `{} {}`, "node-a", "config"},
		{"oversized", `{"Entry":"` + strings.Repeat("A", fixedPeerMaxRPCBytesV1) + `"}`, "node-a", "config"},
		{"wrong-identity", `{}`, "node-b", "config"},
		{"wrong-config", `{}`, "node-a", "different"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/status", strings.NewReader(tt.body))
			request.Header.Set("X-TreeDB-Node", tt.node)
			request.Header.Set("X-TreeDB-Config", tt.digest)
			recorder := httptest.NewRecorder()
			r.serve(recorder, request)
			var reply fixedPeerReplyV1
			if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil || reply.Error == "" {
				t.Fatalf("accepted malformed frame: %+v, %v", reply, err)
			}
		})
	}
}

func TestFixedPeerTCPRemoteErrorClassesV1(t *testing.T) {
	for _, sentinel := range fixedPeerErrorsV1 {
		err := &fixedPeerRemoteErrorV1{message: "remote refusal", code: fixedPeerErrorCodeV1(fmt.Errorf("wrapped: %w", sentinel))}
		if !errors.Is(err, sentinel) || err.Is(nil) {
			t.Fatalf("lost error class %v: %v", sentinel, err)
		}
	}
	if code := fixedPeerErrorCodeV1(errors.Join(context.Canceled, raftcluster.ErrCommitAmbiguous)); code != raftcluster.ErrCommitAmbiguous.Error() {
		t.Fatalf("ambiguous outcome lost: %s", code)
	}
}

func TestFixedPeerTCPRemoteErrorRouteMetadataPresenceV1(t *testing.T) {
	for _, tt := range []struct {
		name     string
		sentinel error
		route    *raftcluster.RouteErrorMetadata
	}{
		{name: "non-route", sentinel: raftcluster.ErrCommitAmbiguous},
		{name: "routed", sentinel: raftcluster.ErrRouteTargetUnknown, route: &raftcluster.RouteErrorMetadata{
			Class: raftcluster.RouteErrorClassUnknownOwner, GroupID: "group-b", Members: []string{"owner-1", "owner-2"}, LeaderHint: "owner-1",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(fixedPeerReplyV1{
					NodeID: raftcluster.NodeID(r.Header.Get("X-TreeDB-Node")), ConfigDigest: r.Header.Get("X-TreeDB-Config"),
					Error: "remote refusal", ErrorCode: fixedPeerErrorCodeV1(tt.sentinel), RouteError: tt.route,
				})
			}))
			defer server.Close()
			config := fixedPeerTestConfigsV1(t)[0]
			config.Nodes[0].Address = strings.TrimPrefix(server.URL, "http://")
			config.ListenAddress = config.Nodes[0].Address
			client, err := NewFixedPeerTCPClientV1(config)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			_, err = client.Submit(context.Background(), config.NodeID, nil, ClusterRequestMetadata{})
			if !errors.Is(err, tt.sentinel) || err.Error() != "remote refusal" {
				t.Fatalf("lost remote error class or message: %v", err)
			}
			wantRoute := tt.route != nil
			if route, ok := raftcluster.RouteErrorMetadataOf(err); ok != wantRoute || (ok && !reflect.DeepEqual(route, *tt.route)) {
				t.Errorf("raft route metadata=(%+v, %v), want %v", route, ok, tt.route)
			}
			if route, ok := ClusterRouteErrorMetadataOf(err); ok != wantRoute || (ok && !reflect.DeepEqual(route, clusterRouteErrorMetadataFromRaft(*tt.route))) {
				t.Errorf("nativewire route metadata=(%+v, %v), want %v", route, ok, tt.route)
			}
		})
	}
}

func TestFixedPeerTCPDirectorySyncV1(t *testing.T) {
	root := t.TempDir()
	if err := syncFixedPeerDirectoryV1(root); err != nil {
		t.Fatal(err)
	}
	if err := syncFixedPeerDirectoryV1(filepath.Join(root, "missing")); err == nil {
		t.Fatal("ignored directory-open failure")
	}
}

func TestFixedPeerTCPRuntimeProcessV1(t *testing.T) {
	raw := os.Getenv("GOMAP_FIXED_PEER_TEST_CONFIG")
	if raw == "" {
		return
	}
	var config FixedPeerTCPConfigV1
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	runtime, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

type fixedPeerTestProcessV1 struct {
	command *exec.Cmd
	input   io.Closer
	log     *os.File
	stopped bool
}

func fixedPeerStartTestProcessV1(t testing.TB, config FixedPeerTCPConfigV1) *fixedPeerTestProcessV1 {
	t.Helper()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.CreateTemp(t.TempDir(), "node-log-")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestFixedPeerTCPRuntimeProcessV1$", "-test.v")
	command.Env = append(os.Environ(), "GOMAP_FIXED_PEER_TEST_CONFIG="+string(raw))
	command.Stdout, command.Stderr = log, log
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &fixedPeerTestProcessV1{command: command, input: input, log: log}
	t.Cleanup(func() { p.stop(t) })
	return p
}

func (p *fixedPeerTestProcessV1) stop(t testing.TB) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	p.input.Close()
	done := make(chan error, 1)
	go func() { done <- p.command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			raw, _ := os.ReadFile(p.log.Name())
			t.Errorf("child: %v\n%s", err, raw)
		}
		t.Logf("child resources user=%s system=%s usage=%+v", p.command.ProcessState.UserTime(), p.command.ProcessState.SystemTime(), p.command.ProcessState.SysUsage())
	case <-time.After(8 * time.Second):
		p.command.Process.Kill()
		<-done
		t.Error("child required forced cleanup")
	}
	p.log.Close()
}

func fixedPeerTestConfigsV1(t testing.TB) []FixedPeerTCPConfigV1 {
	t.Helper()
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	address := func() string {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		listeners = append(listeners, l)
		return l.Addr().String()
	}
	features := raftcluster.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.Version{Major: 1}})
	nodes := []FixedPeerTCPNodeV1{}
	meta := FixedPeerTCPGroupV1{ID: "meta", BootstrapNode: "ingress", Features: features}
	a := FixedPeerTCPGroupV1{ID: "group-a", BootstrapNode: "ingress"}
	b := FixedPeerTCPGroupV1{ID: "group-b", BootstrapNode: "owner-1"}
	for _, id := range []raftcluster.NodeID{"ingress", "owner-1", "owner-2"} {
		nodes = append(nodes, FixedPeerTCPNodeV1{ID: id, Address: address()})
		meta.Peers = append(meta.Peers, raftcluster.Peer{ID: id, Address: address(), Capabilities: features})
		peer := raftcluster.Peer{ID: id, Address: address()}
		if id == "ingress" {
			a.Peers = append(a.Peers, peer)
		} else {
			b.Peers = append(b.Peers, peer)
		}
	}
	root := t.TempDir()
	configs := make([]FixedPeerTCPConfigV1, len(nodes))
	for i, n := range nodes {
		configs[i] = FixedPeerTCPConfigV1{NodeID: n.ID, DataRoot: filepath.Join(root, string(n.ID), "data"), RaftRoot: filepath.Join(root, string(n.ID), "raft"), ListenAddress: n.Address, Nodes: nodes, Catalog: meta, Groups: []FixedPeerTCPGroupV1{a, b}, RequestTimeout: 2 * time.Second, RaftTimeout: 300 * time.Millisecond}
		configs[i].RaftListen = map[raftcluster.GroupID]string{}
		for _, g := range []FixedPeerTCPGroupV1{meta, a, b} {
			for _, p := range g.Peers {
				if p.ID == n.ID {
					configs[i].RaftListen[g.ID] = p.Address
				}
			}
		}
	}
	return configs
}

func fixedPeerTestLocalRaftAddressesV1(c FixedPeerTCPConfigV1) []string {
	var addresses []string
	for _, v := range c.RaftListen {
		addresses = append(addresses, v)
	}
	return addresses
}

func fixedPeerCreateEntryV1(t testing.TB, name string, version uint64) []byte {
	t.Helper()
	sections := append([]iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})}}, raftClusterCreateCollectionSections(name, version, AckRaftCommitted)...)
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
