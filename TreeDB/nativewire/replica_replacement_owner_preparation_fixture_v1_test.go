package nativewire

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// Base-compatible genuine capability producer: the baseline reaches public
// replacement BEGIN and refuses the owner profile before seed/install. The
// candidate must really install a native snapshot and enroll only a nonvoter.
func TestImmutableOwnerReplacementPreparationCapabilityV1(t *testing.T) {
	ctx, client, _, configs, command := immutableOwnerReplacementFixtureV1(t)
	t.Log("immutable owner preparation producer: BUILD/stage/ACTIVE complete; old=owner-b catalog_voter=false target=replacement nodes_only=true group=group-b")
	membership, err := client.PrepareReplicaReplacementV1(ctx, "source-holder", command)
	if err != nil {
		t.Fatalf("immutable owner preparation at public replacement BEGIN/install/enrollment: %v", err)
	}
	assertImmutableOwnerNonvoterV1(t, membership, command)
	if ready, err := client.ReadinessV1(ctx, command.NewPeer.ID); err == nil || ready.Ready {
		t.Fatalf("prepared nonvoter claimed readiness: %+v err=%v", ready, err)
	}
	listener, err := net.Listen("tcp", configs[len(configs)-1].Vector.PublicAddresses[command.NewPeer.ID])
	if err != nil {
		t.Fatalf("prepared nonvoter advertised a public vector listener: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertImmutableOwnerNonvoterV1(t *testing.T, membership raftcluster.CommittedRaftConfigurationV1, command raftplacement.ReplicaReplacementBeginV1) {
	t.Helper()
	old, target := false, false
	for _, member := range membership.Members {
		if member.ID == command.OldNodeID {
			old = member.Voter
		}
		if member.ID == command.NewPeer.ID {
			target = !member.Voter && member.Address == command.NewPeer.Address
		}
	}
	if !old || !target {
		t.Fatalf("owner preparation changed voting authority: %+v", membership)
	}
}

// Reuses the existing hosted-only genesis, credential, catalog-consumer and
// real-Raft TCP fixtures. No child-process helper or public serving flow changes.
func immutableOwnerReplacementFixtureV1(t *testing.T) (context.Context, *FixedPeerTCPClientV1, []*FixedPeerTCPRuntimeV1, []FixedPeerTCPConfigV1, raftplacement.ReplicaReplacementBeginV1) {
	return immutableOwnerReplacementEndpointFixtureV1(t, false)
}

// The endpoint case only preauthorizes an address; ordinary fixtures remain cold.
func immutableOwnerReplacementEndpointFixtureV1(t *testing.T, endpoint bool) (context.Context, *FixedPeerTCPClientV1, []*FixedPeerTCPRuntimeV1, []FixedPeerTCPConfigV1, raftplacement.ReplicaReplacementBeginV1) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0}, {id: "b", vector: []float32{.8, .2}, home: 1}, {id: "c", vector: []float32{0, 1}, home: 2}, {id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	if err := seed.collection.EnsureVectorPartitionLiveBindingV1(ctx, seed.manifest); err != nil {
		t.Fatal(err)
	}
	coverage := seed.database.State().AppliedCommandLSN
	meta := seed.collection.MetaView()
	column := *meta.Options.ColumnStore
	column.ActiveManifest, column.RecoveryAuthoritativeManifest = nil, nil
	column.RecoveryAuthoritativeAppliedCommandLSN = 0
	meta.Options.ColumnStore = &column
	configs := fixedPeerMultiOwnerSearchConfigsV1(t, seed.manifest, meta)
	fixedPeerCatalogConsumerOwnerConfigV1(t, configs)
	count := 3
	if endpoint {
		count++
	}
	addresses := fixedPeerFixtureUnusedAddressesV1(t, configs, count)
	const target raftcluster.NodeID = "replacement"
	targetAddress, targetRaft := addresses[0], addresses[1]
	nodes := append(append([]FixedPeerTCPNodeV1(nil), configs[0].Nodes...), FixedPeerTCPNodeV1{ID: target, Address: targetAddress})
	vector := cloneFixedPeerVectorConfigV1(configs[0].Vector)
	vector.PublicAddresses[target] = addresses[2]
	if endpoint {
		vector.ShardAddresses["group-b"][target] = addresses[3]
	}
	for i := range configs {
		configs[i].Nodes, configs[i].Vector = nodes, vector
	}
	spare := configs[1]
	spare.NodeID, spare.ListenAddress, spare.RaftListen = target, targetAddress, map[raftcluster.GroupID]string{}
	root := t.TempDir()
	spare.DataRoot, spare.RaftRoot = filepath.Join(root, "data"), filepath.Join(root, "raft")
	configs = append(configs, spare)
	ca := newPeerCAFixtureV1(t)
	for i := range configs {
		configs[i].ClusterID = "immutable-owner-replacement-preparation"
		configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		validated, _, err := validateFixedPeerConfigV1(configs[i])
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(validated)
		if err != nil {
			t.Fatal(err)
		}
		if err := preparePeerStorageV1(validated, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}
	owners := map[uint32]raftcluster.GroupID{}
	segments := map[raftcluster.GroupID]map[uint32]bool{"group-b": {}, "group-c": {}}
	for _, placement := range seed.manifest.Placements {
		owners[placement.PartitionID] = raftcluster.GroupID(placement.GroupID)
	}
	for _, asset := range seed.manifest.Assets {
		segments[owners[asset.PartitionID]][asset.Ref.FileID] = true
	}
	for _, config := range configs[:len(configs)-1] {
		group := raftcluster.GroupID("group-a")
		switch config.NodeID {
		case "owner-b":
			group = "group-b"
		case "owner-c":
			group = "group-c"
		case "source-holder":
			group = "group-d"
		}
		dir := filepath.Join(config.DataRoot, string(group))
		if config.NodeID == "source-holder" {
			if err := os.CopyFS(dir, os.DirFS(seed.dir)); err != nil {
				t.Fatal(err)
			}
			if err := backenddb.RebindDurableRootSnapshotV1(dir); err != nil {
				t.Fatal(err)
			}
			bootstrapFixedPeerVectorTrustedGenesisV1(t, config, coverage)
		} else {
			fixedPeerCopyHostedVectorAssetsV1(t, seed.dir, dir, seed.manifest, segments[group], group == "group-a")
			fixedPeerBootstrapHostedVectorMetadataV1(t, config, meta, dir)
		}
	}
	runtimes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	t.Cleanup(func() {
		for _, runtime := range runtimes {
			if runtime != nil {
				if err := runtime.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	for i, config := range configs {
		opened, err := OpenFixedPeerTCPRuntimeV1(config)
		if err != nil {
			t.Fatal(err)
		}
		runtimes[i] = opened
	}
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	if leader := fixedPeerWaitCatalogLeaderV1(t, ctx, runtimes[:len(configs)-1]); configs[leader].NodeID != "source-holder" {
		t.Fatal("owner fixture changed catalog leader")
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, "source-holder", initial); err != nil {
		t.Fatal(err)
	}
	// Trusted bootstrap has no semantic result receipt. Commit a real command
	// before BUILD, using the generic native replacement fixture's producer.
	owner := runtimes[1].localDataV1("group-b")
	// Single-member owner hints can appear before their current-term prefix.
	// Retry only quorum-fenced reads before the one-shot BUILD/stage/ACTIVE.
	for _, config := range configs[:len(configs)-1] {
		if leader := fixedPeerWaitDataLeaderV1(t, ctx, runtimes[:len(configs)-1], config.Groups[0]); leader != config.NodeID {
			t.Fatalf("owner fixture group %s leader=%s want %s", config.Groups[0].ID, leader, config.NodeID)
		}
	}
	version, known, err := owner.fsm.CurrentCatalogVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.provider.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{NodeID: "owner-b", GroupID: "group-b", EntryBytes: fixedPeerCreateEntryV1(t, "owner-preparation-proof", version), CurrentCatalogVersion: version, HasCurrentCatalogVersion: known, SyncLocalCommandWAL: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
		t.Fatalf("immutable producer BUILD/stage/ACTIVE: %v", err)
	}
	command := raftplacement.ReplicaReplacementBeginV1{OperationID: "prepare-owner-b", ConfigDigest: client.digest, ExpectedEpoch: record.Epoch, CatalogDigest: record.Digest, GroupID: "group-b", OldNodeID: "owner-b", NewPeer: raftcluster.Peer{ID: target, Address: targetRaft}}
	return ctx, client, runtimes, configs, command
}
