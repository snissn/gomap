package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// This prerequisite does not prove transfer, fencing, cutover, receipt
// lineage or reclamation. TestDomainMoveCrashAtEveryCutoverBoundaryHasOneOwnerV1
// remains the full checkpoint control once the operator transfer API exists.
// No source genesis copy or collection/graph reconstruction is permitted here.
func TestQuiescedANNDomainMoveEmptyDestinationPublicRuntimeV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	destination, seed := quiescedANNMoveDestinationFixtureV1(t)
	vector := destination.Vector

	if _, _, err := validateFixedPeerConfigV1(destination); err != nil {
		t.Fatalf("destination config must be valid before opening storage: %v", err)
	}
	runtime, err := OpenFixedPeerTCPRuntimeV1(destination)
	if err != nil {
		t.Fatalf("configured empty ANN destination cannot open dormant status: %v", err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	}()
	client, err := DialContext(ctx, "tcp", vector.PublicAddresses[destination.NodeID])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, err := client.VectorStatusV1(ctx)
	if err != nil {
		t.Fatalf("empty destination must expose public unready status: %v", err)
	}
	if status.Health.Ready || status.Health.Reason != "dormant_destination" {
		t.Fatalf("empty destination health=%+v", status.Health)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "docs"}); err == nil {
		t.Fatal("dormant native metadata mutation created canonical collection")
	}
	fixture := fixedPeerVectorReadyFixtureV1{
		Generation: public.GenerationIDV1{Index: seed.manifest.IndexName, Generation: seed.manifest.Generation},
	}
	var typed *public.ErrorV1
	if _, err := client.VectorSearchStrictV1(ctx, fixture.SearchRequest([]float32{0, 1}, 1)); !errors.As(err, &typed) || typed.Code != public.ErrorUnavailableV1 {
		t.Fatalf("empty destination public search must refuse unavailable: %v", err)
	}
	typed = nil
	if _, err := client.VectorInsertV1(ctx, splitInsertRequestV1(fixture, 1)); !errors.As(err, &typed) || typed.Code != public.ErrorUnavailableV1 {
		t.Fatalf("empty destination public insert must refuse unavailable: %v", err)
	}
	// Exercise authenticated production HTTP dispatch, including generic
	// mutation ingress that does not pass through the public vector backend.
	for _, operation := range []string{
		"submit", "forward", "catalog-publish", "vector-forward", "vector-search",
		"vector-split-project", "vector-split-source-proof", "vector-split-receipt",
		"vector-lifecycle", "replacement-begin", "replacement-install", "replacement-promote",
	} {
		if _, err := runtime.client.call(ctx, destination.NodeID, operation, fixedPeerRequestV1{}, true); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
			t.Fatalf("dormant %s must refuse unavailable: %v", operation, err)
		}
	}
	// The existing public operations service must not construct a backend or
	// publish lifecycle state while the destination has no collection.
	id := fixture.Generation
	for _, operation := range []func() error{
		func() error {
			_, err := runtime.vector.server.vectorPartitionOperations.Register(ctx, public.GenerationRegistrationV1{
				GenerationIDV1: id, SourceGeneration: vector.Identity.Source.Generation,
				SourceChecksum: vector.Identity.Source.Checksum, SourceSchemaHash: vector.Identity.Source.SchemaHash,
				SourceRowCount: vector.Identity.Source.RowCount,
			})
			return err
		},
		func() error { _, err := runtime.vector.server.vectorPartitionOperations.Prepare(ctx, id); return err },
		func() error { _, err := runtime.vector.server.vectorPartitionOperations.Activate(ctx, id); return err },
		func() error {
			_, err := runtime.vector.server.vectorPartitionOperations.Invalidate(ctx, id, "test")
			return err
		},
		func() error {
			_, err := runtime.vector.server.vectorPartitionOperations.RequestRebuild(ctx, id)
			return err
		},
		func() error { _, err := runtime.vector.server.vectorPartitionOperations.Retire(ctx, id); return err },
	} {
		typed = nil
		if err := operation(); !errors.As(err, &typed) || typed.Code != public.ErrorUnavailableV1 {
			t.Fatalf("dormant lifecycle operation must refuse unavailable: %v", err)
		}
	}
	if runtime.vector == nil || runtime.vector.dataGroup != "group-c" {
		t.Fatal("destination did not retain its configured local data group")
	}
	data := runtime.data["group-c"]
	if data == nil || data.db == nil {
		t.Fatal("destination durable data store is missing")
	}
	if _, err := collections.NewCollectionManager(data.db).OpenCollection("docs"); err == nil {
		t.Fatal("empty destination created canonical collection or carrier before transfer")
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime, err = OpenFixedPeerTCPRuntimeV1(destination)
	if err != nil {
		t.Fatalf("empty destination reopen: %v", err)
	}
	reopenedClient, err := DialContext(ctx, "tcp", vector.PublicAddresses[destination.NodeID])
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedClient.Close()
	status, err = reopenedClient.VectorStatusV1(ctx)
	if err != nil || status.Health.Ready || status.Health.Reason != "dormant_destination" {
		t.Fatalf("reopened destination health=%+v err=%v", status.Health, err)
	}
	if err := reopenedClient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	changed := destination
	admission := *destination.QuiescedANNMoveDestination
	admission.MoveID = "move-2"
	changed.QuiescedANNMoveDestination = &admission
	other, err := OpenFixedPeerTCPRuntimeV1(changed)
	if err == nil {
		_ = other.Close()
		t.Fatal("changed local admission reused persisted root identity")
	}
	if !errors.Is(err, raftcluster.ErrInvalidConfig) ||
		!strings.Contains(err.Error(), "persistent root identity mismatch") {
		t.Fatalf("changed admission must fail persisted identity validation before open: %v", err)
	}
}

func TestQuiescedANNDomainMoveOrdinaryOwnerStartupStillStrictV1(t *testing.T) {
	seed := fixedPeerVectorSeedV1(t)
	seed.catalog.Placements[0].GroupID = "group-a"
	configs := fixedPeerVectorTestConfigsV1(t, seed)
	ca := newPeerCAFixtureV1(t)
	for _, node := range []raftcluster.NodeID{"ingress", "owner-1"} {
		t.Run(string(node), func(t *testing.T) {
			var config FixedPeerTCPConfigV1
			for _, candidate := range configs {
				if candidate.NodeID == node {
					config = candidate
				}
			}
			config.ClusterID = "quiesced-ann-domain-move-ordinary-owner-guard"
			config.Credentials = ca.issue(t, config.ClusterID, string(node),
				time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
			if _, _, err := validateFixedPeerConfigV1(config); err != nil {
				t.Fatalf("ordinary owner config must be valid before storage refusal: %v", err)
			}
			runtime, err := OpenFixedPeerTCPRuntimeV1(config)
			if err == nil {
				_ = runtime.Close()
				t.Fatal("ordinary canonical/ANN owner opened without its required durable source/carrier")
			}
		})
	}
}

func quiescedANNMoveDestinationFixtureV1(t testing.TB) (FixedPeerTCPConfigV1, fixedPeerVectorSeedFixtureV1) {
	t.Helper()
	seed := fixedPeerVectorSeedV1(t)
	seed.catalog.Placements[0].GroupID = "group-a"
	configs := fixedPeerVectorTestConfigsV1(t, seed)
	var destination FixedPeerTCPConfigV1
	for _, config := range configs {
		if config.NodeID == "owner-3" {
			destination = config
		}
	}
	if destination.NodeID == "" {
		t.Fatal("destination fixture node is missing")
	}
	// Source remains group-a; the complete ANN domain remains group-b.
	// The third node becomes a configured catalog voter hosting only the
	// empty group-c, without joining the existing ANN or canonical groups.
	groups := make([]FixedPeerTCPGroupV1, 0, len(destination.Groups)+1)
	var destinationPeer raftcluster.Peer
	for _, group := range destination.Groups {
		if group.ID != "group-b" {
			groups = append(groups, group)
			continue
		}
		owner := group
		owner.Peers = nil
		for _, peer := range group.Peers {
			if peer.ID == destination.NodeID {
				destinationPeer = peer
			} else {
				owner.Peers = append(owner.Peers, peer)
			}
		}
		groups = append(groups, owner)
	}
	if destinationPeer.ID == "" {
		t.Fatal("destination data-group peer is missing")
	}
	groups = append(groups, FixedPeerTCPGroupV1{
		ID: "group-c", Peers: []raftcluster.Peer{destinationPeer},
		BootstrapNode: destination.NodeID,
	})
	destination.Groups = groups
	delete(destination.RaftListen, "group-b")
	destination.RaftListen["group-c"] = destinationPeer.Address
	vector := *destination.Vector
	vector.Catalog.Groups = append([]raftplacement.GroupV1(nil), vector.Catalog.Groups...)
	for i, group := range vector.Catalog.Groups {
		if group.ID == "group-b" {
			vector.Catalog.Groups[i].Members = []raftcluster.NodeID{"owner-1", "owner-2"}
			vector.Catalog.Groups[i].LeaderHint = "owner-1"
		}
	}
	vector.Catalog.Groups = append(vector.Catalog.Groups, raftplacement.GroupV1{
		ID: "group-c", Members: []raftcluster.NodeID{destination.NodeID}, LeaderHint: destination.NodeID,
	})
	delete(vector.ShardAddresses["group-b"], destination.NodeID)
	record, err := raftplacement.NewCatalogMetaRecordV1(1, vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	vector.Identity.Index.CatalogDigest = record.Digest
	destination.Vector = &vector
	destination.ClusterID = "quiesced-ann-domain-move-prerequisite"
	ca := newPeerCAFixtureV1(t)
	destination.Credentials = ca.issue(t, destination.ClusterID, string(destination.NodeID),
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	destination.RequestTimeout = 10 * time.Second

	destination.QuiescedANNMoveDestination = &FixedPeerQuiescedANNMoveDestinationV1{
		Identity: vector.Identity, SourceGroup: "group-a", ANNGroup: "group-b",
		DestinationGroup: "group-c", MoveID: "move-1",
	}
	return destination, seed

}

func TestQuiescedANNDomainMoveAdmissionRejectsInvalidScopeV1(t *testing.T) {
	original, _ := quiescedANNMoveDestinationFixtureV1(t)
	base, digest, err := validateFixedPeerConfigV1(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*FixedPeerTCPConfigV1)
	}{
		{"canonical_destination", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.DestinationGroup = "group-a" }},
		{"current_owner_destination", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.DestinationGroup = "group-b" }},
		{"unknown_destination", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.DestinationGroup = "group-missing" }},
		{"wrong_source", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.SourceGroup = "group-b" }},
		{"wrong_ann_owner", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.ANNGroup = "group-a" }},
		{"wrong_catalog_identity", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.Identity.Index.CatalogEpoch++ }},
		{"wrong_collection_identity", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.Identity.Index.CollectionIncarnation++ }},
		{"wrong_source_identity", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.Identity.Source.Checksum++ }},
		{"wrong_generation", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.Identity.Generation++ }},
		{"empty_move", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.MoveID = "" }},
		{"oversized_move", func(c *FixedPeerTCPConfigV1) { c.QuiescedANNMoveDestination.MoveID = strings.Repeat("m", 129) }},
		{"no_credentials", func(c *FixedPeerTCPConfigV1) { c.Credentials = nil }},
		{"no_vector", func(c *FixedPeerTCPConfigV1) { c.Vector = nil }},
		{"non_inline_source", func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.SourceFormat = 2 }},
		{"immutable_generation", func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Immutable.ManifestDigest = strings.Repeat("a", 64) }},
		{"wrong_manifest_format", func(c *FixedPeerTCPConfigV1) { c.Vector.Manifest.Format = "unsupported" }},
		{"paged_generation", func(c *FixedPeerTCPConfigV1) {
			c.Vector.Manifest.PagedRootV2 = &collections.VectorPartitionPagedRootV2{}
		}},
		{"multiple_domains", func(c *FixedPeerTCPConfigV1) { c.Vector.Manifest.DomainCount = 2 }},
		{"overlapping_domains", func(c *FixedPeerTCPConfigV1) { c.Vector.Manifest.BalancePolicy = "overlap_v1" }},
		{"multiple_destination_voters", func(c *FixedPeerTCPConfigV1) {
			for i := range c.Groups {
				if c.Groups[i].ID == "group-c" {
					c.Groups[i].Peers = append(c.Groups[i].Peers, raftcluster.Peer{ID: "owner-1", Address: sparseCatalogUnusedAddressV1(t, []FixedPeerTCPConfigV1{*c})})
				}
			}
			for i := range c.Vector.Catalog.Groups {
				if c.Vector.Catalog.Groups[i].ID == "group-c" {
					c.Vector.Catalog.Groups[i].Members = append(c.Vector.Catalog.Groups[i].Members, "owner-1")
				}
			}
			record, err := raftplacement.NewCatalogMetaRecordV1(c.Vector.Identity.Index.CatalogEpoch, c.Vector.Catalog)
			if err != nil {
				t.Fatal(err)
			}
			c.Vector.Identity.Index.CatalogDigest = record.Digest
			c.QuiescedANNMoveDestination.Identity = c.Vector.Identity
		}},
		{"multiple_local_groups", func(c *FixedPeerTCPConfigV1) {
			for i := range c.Groups {
				if c.Groups[i].ID == "group-a" {
					c.Groups[i].Peers[0].ID = c.NodeID
					c.Groups[i].BootstrapNode = c.NodeID
					c.RaftListen["group-a"] = c.Groups[i].Peers[0].Address
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, _, err := validateFixedPeerConfigV1(base)
			if err != nil {
				t.Fatal(err)
			}
			tc.change(&config)
			if tc.name == "multiple_destination_voters" {
				ordinary := config
				ordinary.QuiescedANNMoveDestination = nil
				if _, _, err := validateFixedPeerConfigV1(ordinary); err != nil {
					t.Fatalf("multiple-voter fixture must otherwise be valid: %v", err)
				}
			}
			runtime, err := OpenFixedPeerTCPRuntimeV1(config)
			if err == nil {
				_ = runtime.Close()
				t.Fatal("invalid destination admission opened")
			}
			if !errors.Is(err, raftcluster.ErrInvalidConfig) {
				t.Fatalf("invalid admission must fail config validation: %v", err)
			}
			if _, err := os.Stat(filepath.Join(config.RaftRoot, "fixed-peer-v1.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid config touched persistent runtime: %v", err)
			}
		})
	}
	withoutAdmission := base
	withoutAdmission.QuiescedANNMoveDestination = nil
	_, ordinaryDigest, err := validateFixedPeerConfigV1(withoutAdmission)
	if err != nil || ordinaryDigest != digest {
		t.Fatalf("local admission changed shared peer digest: %s %s %v", digest, ordinaryDigest, err)
	}
	// The runtime-owned pointer is independent of mutable caller configuration.
	original.QuiescedANNMoveDestination.MoveID = "caller-mutated"
	if base.QuiescedANNMoveDestination.MoveID != "move-1" {
		t.Fatal("validated admission aliases caller memory")
	}
	ordinary, err := OpenFixedPeerTCPRuntimeV1(withoutAdmission)
	if err == nil {
		_ = ordinary.Close()
		t.Fatal("missing collection implicitly admitted an empty destination")
	}
}

func TestQuiescedANNDomainMoveNonemptyStorageRefusesBeforeListenersV1(t *testing.T) {
	base, _ := quiescedANNMoveDestinationFixtureV1(t)
	for _, kind := range []string{"unmarked", "user", "collection"} {
		t.Run(kind, func(t *testing.T) {
			config := base
			config.DataRoot = filepath.Join(t.TempDir(), "data")
			config.RaftRoot = filepath.Join(t.TempDir(), "raft")
			config, _, err := validateFixedPeerConfigV1(config)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "unmarked" {
				if err := os.MkdirAll(config.DataRoot, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(config.DataRoot, "unrelated"), []byte("retain"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				raw, err := json.Marshal(config)
				if err != nil {
					t.Fatal(err)
				}
				if err := preparePeerStorageV1(config, raw); err != nil {
					t.Fatal(err)
				}
				database, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(config.DataRoot, "group-c"), CommandWAL: true})
				if err != nil {
					t.Fatal(err)
				}
				if kind == "user" {
					err = database.SetSync([]byte("unrelated"), []byte("retain"))
				} else {
					_, err = collections.NewCollectionManager(database).CreateCollection(&collections.CollectionMeta{Name: "unrelated"})
				}
				if err := errors.Join(err, database.Close()); err != nil {
					t.Fatal(err)
				}
			}
			// Occupy every prospective listener. Storage refusal must win,
			// demonstrating that no Raft/control/public listener was attempted.
			addresses := []string{config.ListenAddress, config.Vector.PublicAddresses[config.NodeID]}
			for _, address := range config.RaftListen {
				addresses = append(addresses, address)
			}
			for _, address := range addresses {
				listener, err := net.Listen("tcp", address)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
			}
			runtime, err := OpenFixedPeerTCPRuntimeV1(config)
			if err == nil {
				_ = runtime.Close()
				t.Fatal("nonempty destination opened")
			}
			expected := "destination contains"
			if kind == "unmarked" {
				expected = "nonempty root"
			}
			if !errors.Is(err, raftcluster.ErrInvalidConfig) || !strings.Contains(err.Error(), expected) {
				t.Fatalf("nonempty storage must refuse before occupied listener: %v", err)
			}
		})
	}
}

func BenchmarkQuiescedANNMoveDestinationOpenCloseV1(b *testing.B) {
	if b.N > 100 {
		b.Skip("startup benchmark requires bounded -benchtime=10x")
	}
	config, _ := quiescedANNMoveDestinationFixtureV1(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		config.DataRoot = filepath.Join(b.TempDir(), "data")
		config.RaftRoot = filepath.Join(b.TempDir(), "raft")
		b.StartTimer()
		runtime, err := OpenFixedPeerTCPRuntimeV1(config)
		if err != nil {
			b.Fatal(err)
		}
		if err := runtime.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
