package nativewire

import (
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestImmutableOwnerReplacementInstalledAssetsTailAndRestartV1(t *testing.T) {
	ctx, client, runtimes, configs, command := immutableOwnerReplacementFixtureV1(t)
	preparationStart := time.Now()
	membership, err := client.PrepareReplicaReplacementV1(ctx, "source-holder", command)
	if err != nil {
		t.Fatal(err)
	}
	assertImmutableOwnerNonvoterV1(t, membership, command)
	preparationElapsed := time.Since(preparationStart)
	identity := configs[0].Vector.Identity
	command.OwnerPreparation = &identity
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		t.Fatal(err)
	}
	target := len(runtimes) - 1
	assertTarget := func() {
		t.Helper()
		state, err := runtimes[3].authority.ReplicaReplacementStateV1(command.GroupID)
		if err != nil || state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil {
			t.Fatalf("prepared operation=%+v err=%v", state, err)
		}
		local := runtimes[target].localDataV1(command.GroupID)
		if local == nil || runtimes[target].vector != nil || runtimes[target].meta != nil {
			t.Fatal("prepared target acquired public/vector/catalog authority")
		}
		meta, manifest, scope, err := local.fsm.PreparedVectorPartitionScopedManifestFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: command.NewPeer.ID, GroupID: command.GroupID, MinAppliedIndex: state.Seed.Manifest.LastIncludedIndex}, identity.Index.Collection.Collection, identity.Index.IndexName, identity.Generation)
		if err != nil || scope.Router || scope.HostedGroup != string(command.GroupID) || scope.ManifestDigest != identity.Immutable.ManifestDigest || !vectorPartitionReplicatedLiveManifestMatchesV1(manifest, configs[0].Vector.Manifest) {
			t.Fatalf("native installed hosted assets: scope=%+v manifest=%+v err=%v", scope, manifest, err)
		}
		if err := fixedPeerVectorImmutableDefinitionV1(meta, identity); err != nil {
			t.Fatal(err)
		}
		fixedPeerWaitV1(t, ctx, func() bool {
			reply, err := client.call(ctx, "owner-b", "replacement-tail", fixedPeerRequestV1{Entry: raw}, false)
			return err == nil && reply.ReplacementTail != nil && reply.ReplacementTail.Progress.Validate() == nil && reply.ReplacementTail.Progress.CommandDigest != (raftentry.CommandDigestV1{}) && reply.ReplacementTail.Progress.Result.CommandDigest == reply.ReplacementTail.Progress.CommandDigest && reply.ReplacementTail.Progress.Result.ResultDigest != (raftentry.CommandDigestV1{})
		})
		if _, err := os.Stat(filepath.Join(configs[target].RaftRoot, "nodes", string(command.NewPeer.ID), "groups", string(configs[target].Catalog.ID))); !os.IsNotExist(err) {
			t.Fatalf("prepared owner acquired local catalog files: %v", err)
		}
		if ready, err := runtimes[target].ReadinessV1(ctx); err == nil || ready.Ready {
			t.Fatalf("preparation warmed readiness: %+v err=%v", ready, err)
		}
		if _, err := runtimes[target].searchVectorPartitionStrictV1(ctx, public.SearchRequestV1{}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
			t.Fatalf("prepared target served public hits: %v", err)
		}
		listener, err := net.Listen("tcp", configs[target].Vector.PublicAddresses[command.NewPeer.ID])
		if err != nil {
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		for _, operation := range []string{"replacement-promotion-intent", "replacement-promote", "replacement-complete-promotion", "replacement-removal-intent", "replacement-remove", "replacement-complete"} {
			if _, err := client.call(ctx, "source-holder", operation, fixedPeerRequestV1{Entry: raw}, false); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
				t.Fatalf("prepared owner admitted %s: %v", operation, err)
			}
		}
	}
	assertTarget()
	prepared, err := runtimes[3].authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		t.Fatal(err)
	}
	var hostedBytes int64
	files := map[string]bool{}
	for _, asset := range configs[0].Vector.Manifest.Assets {
		for _, placement := range configs[0].Vector.Manifest.Placements {
			if placement.PartitionID != asset.PartitionID || placement.GroupID != string(command.GroupID) {
				continue
			}
			rel := filepath.Join(filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
			if files[rel] {
				continue
			}
			files[rel] = true
			info, err := os.Stat(filepath.Join(backenddb.ColumnAssetRootDirPath(filepath.Join(configs[target].DataRoot, string(command.GroupID))), rel))
			if err != nil {
				t.Fatal(err)
			}
			hostedBytes += info.Size()
		}
	}
	t.Logf("owner_preparation_cost scope=BEGIN_native_install_nonvoter_enrollment elapsed_ns=%d native_archive_bytes=%d hosted_segment_bytes=%d hosted_files=%d process_scope=five_in_process_raft_nodes", preparationElapsed.Nanoseconds(), prepared.Seed.SizeBytes, hostedBytes, len(files))
	const tailSamples = 3
	var beforeAlloc, afterAlloc runtime.MemStats
	runtime.ReadMemStats(&beforeAlloc)
	tailStart := time.Now()
	for range tailSamples {
		if _, err := client.call(ctx, "owner-b", "replacement-tail", fixedPeerRequestV1{Entry: raw}, false); err != nil {
			t.Fatal(err)
		}
	}
	tailElapsed := time.Since(tailStart)
	runtime.ReadMemStats(&afterAlloc)
	t.Logf("owner_preparation_tail_cost samples=%d ns/op=%d global_B/op=%d global_allocs/op=%d allocation_scope=caller_all_five_servers_background_included", tailSamples, tailElapsed.Nanoseconds()/tailSamples, (afterAlloc.TotalAlloc-beforeAlloc.TotalAlloc)/tailSamples, (afterAlloc.Mallocs-beforeAlloc.Mallocs)/tailSamples)
	persisted := filepath.Join(configs[target].RaftRoot, "fixed-peer-v1.json")
	before, err := os.ReadFile(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtimes[target].Close(); err != nil {
		t.Fatal(err)
	}
	runtimes[target] = nil
	reopened, err := OpenFixedPeerTCPRuntimeV1(configs[target])
	if err != nil {
		t.Fatal(err)
	}
	runtimes[target] = reopened
	if _, err := client.call(ctx, command.NewPeer.ID, "replacement-prepare", fixedPeerRequestV1{Entry: raw}, false); err != nil {
		t.Fatal(err)
	}
	assertTarget()
	after, err := os.ReadFile(persisted)
	if err != nil || string(after) != string(before) {
		t.Fatalf("replacement edited static manifest: %v", err)
	}
	conflict := command
	conflict.OperationID = "conflicting-owner-preparation"
	changed, err := raftplacement.EncodeReplicaReplacementBeginV1(conflict)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.call(ctx, "source-holder", "replacement-begin", fixedPeerRequestV1{Entry: changed}, false); !errors.Is(err, raftplacement.ErrCatalogMetaConflict) {
		t.Fatalf("conflicting BEGIN=%v", err)
	}
}

func TestImmutableOwnerReplacementValidTailWithoutHostedAssetRefusesV1(t *testing.T) {
	ctx, client, runtimes, configs, command := immutableOwnerReplacementFixtureV1(t)
	membership, err := client.PrepareReplicaReplacementV1(ctx, "source-holder", command)
	if err != nil {
		t.Fatal(err)
	}
	assertImmutableOwnerNonvoterV1(t, membership, command)
	identity := configs[0].Vector.Identity
	command.OwnerPreparation = &identity
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		t.Fatal(err)
	}
	var observed fixedPeerReplyV1
	fixedPeerWaitV1(t, ctx, func() bool {
		var err error
		observed, err = client.call(ctx, "owner-b", "replacement-tail", fixedPeerRequestV1{Entry: raw}, false)
		return err == nil && observed.ReplacementTail != nil && observed.ReplacementTail.Progress.Validate() == nil && observed.ReplacementTail.Progress.CommandDigest != (raftentry.CommandDigestV1{}) && observed.ReplacementTail.Progress.Result.ResultDigest != (raftentry.CommandDigestV1{})
	})
	target := runtimes[len(runtimes)-1]
	local := target.localDataV1(command.GroupID)
	// Durable native command progress remains valid while one hosted graph file
	// disappears. That progress alone must never be accepted as owner readiness.
	var removed bool
	for _, asset := range configs[0].Vector.Manifest.Assets {
		for _, placement := range configs[0].Vector.Manifest.Placements {
			if placement.PartitionID == asset.PartitionID && placement.GroupID == string(command.GroupID) {
				rel := filepath.Join(filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
				path := filepath.Join(backenddb.ColumnAssetRootDirPath(filepath.Join(configs[len(configs)-1].DataRoot, string(command.GroupID))), rel)
				err := os.Remove(path)
				removed = err == nil
				if err != nil {
					t.Fatal(err)
				}
				break
			}
		}
		if removed {
			break
		}
	}
	if !removed {
		t.Fatal("did not remove a hosted graph asset")
	}
	if _, err := local.fsm.ReplacementTailProgressV1(ctx, observed.ReplacementTail.Progress.EntryID); err != nil {
		t.Fatalf("negative control lost native semantic tail: %v", err)
	}
	if _, err := client.call(ctx, command.NewPeer.ID, "replacement-cutoff", fixedPeerRequestV1{Entry: raw}, false); err == nil {
		t.Fatal("missing hosted asset passed cutoff")
	}
	if _, err := client.call(ctx, "owner-b", "replacement-tail", fixedPeerRequestV1{Entry: raw}, false); err == nil {
		t.Fatal("valid native tail without hosted asset passed owner proof")
	}
	if _, err := client.call(ctx, "source-holder", "replacement-promotion-intent", fixedPeerRequestV1{Entry: raw}, false); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
		t.Fatalf("missing asset promoted: %v", err)
	}
	if ready, err := target.ReadinessV1(ctx); err == nil || ready.Ready {
		t.Fatalf("missing asset gained READY: %+v %v", ready, err)
	}
}
