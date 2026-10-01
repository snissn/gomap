package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestImmutableOwnerReplacementPrivateSourceWarmV1(t *testing.T) {
	testImmutableOwnerReplacementPrivateSourceWarmV1(t, false)
}

func testImmutableOwnerReplacementPrivateSourceWarmV1(t *testing.T, endpoint bool) {
	testImmutableOwnerReplacementPrivateQualificationV1(t, endpoint, false)
}

func testImmutableOwnerReplacementPrivateQualificationV1(t *testing.T, endpoint, qualification bool) {
	t.Helper()
	ctx, client, runtimes, configs, command := immutableOwnerReplacementEndpointFixtureV1(t, endpoint)
	identity := configs[0].Vector.Identity
	command.OwnerPreparation = &identity
	if err := client.WarmReplicaReplacementOwnerV1(ctx, command); !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
		t.Fatalf("unprepared owner warm=%v", err)
	}
	membership, err := client.PrepareReplicaReplacementV1(ctx, "source-holder", command)
	if err != nil {
		t.Fatal(err)
	}
	assertImmutableOwnerNonvoterV1(t, membership, command)
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		t.Fatal(err)
	}
	targetIndex := len(runtimes) - 1
	target := runtimes[targetIndex]
	local := target.localDataV1(command.GroupID)
	source := func() *CollectionVectorPartitionGenerationSourceV1 {
		t.Helper()
		local.replacementWork.mu.Lock()
		defer local.replacementWork.mu.Unlock()
		return local.replacementWork.ownerSource
	}
	waitTail := func() {
		t.Helper()
		fixedPeerWaitV1(t, ctx, func() bool {
			reply, err := client.call(ctx, command.OldNodeID, "replacement-tail", fixedPeerRequestV1{Entry: raw}, false)
			return err == nil && reply.ReplacementTail != nil &&
				reply.ReplacementTail.Progress.CommandDigest != (raftentry.CommandDigestV1{}) &&
				reply.ReplacementTail.Progress.Result.ResultDigest != (raftentry.CommandDigestV1{})
		})
	}
	assertPrivate := func() {
		t.Helper()
		state, err := runtimes[3].authority.ReplicaReplacementStateV1(command.GroupID)
		if err != nil || state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil {
			t.Fatalf("warm advanced operation: %+v %v", state, err)
		}
		reply, err := client.call(ctx, command.OldNodeID, "replacement-tail", fixedPeerRequestV1{Entry: raw}, false)
		if err != nil || reply.ReplacementTail == nil {
			t.Fatalf("warm lost semantic tail: %v", err)
		}
		// Obtain the actual native roster, not a catalog role copied into the target.
		current, _, err := runtimes[1].localDataV1(command.GroupID).provider.ReplacementReadFenceV1(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assertImmutableOwnerNonvoterV1(t, current, command)
		if target.vector != nil || target.meta != nil {
			t.Fatal("private warm attached serving/catalog runtime")
		}
		if ready, err := target.ReadinessV1(ctx); err == nil || ready.Ready {
			t.Fatalf("warm acquired readiness: %+v %v", ready, err)
		}
		if _, err := target.searchVectorPartitionStrictV1(ctx, public.SearchRequestV1{}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
			t.Fatalf("warm served public hits: %v", err)
		}
		listener, err := net.Listen("tcp", configs[targetIndex].Vector.PublicAddresses[command.NewPeer.ID])
		if err != nil {
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		if endpoint {
			local.replacementWork.mu.Lock()
			preparedEndpoint := local.replacementWork.ownerEndpoint
			local.replacementWork.mu.Unlock()
			if preparedEndpoint == nil {
				t.Fatal("explicit preparation did not retain endpoint")
			}
			assertReplacementOwnerEndpointSearchV1(t, ctx, client, target, runtimes[1], command)
			if qualification {
				assertReplacementOwnerQualificationParityV1(t, ctx, client, target, runtimes[1], command)
			}
		}
		for _, operation := range []string{"replacement-promotion-intent", "replacement-promote", "replacement-complete-promotion", "replacement-removal-intent", "replacement-remove", "replacement-complete"} {
			if _, err := client.call(ctx, "source-holder", operation, fixedPeerRequestV1{Entry: raw}, false); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
				t.Fatalf("warm admitted %s: %v", operation, err)
			}
		}
	}
	if source() != nil {
		t.Fatal("preparation or observation warmed source")
	}
	// Genuine native seed installation has replaced the startup DB. First warm
	// must capture the current FSM DB rather than admitting this stale d.db.
	if local.fsm.HasCurrentDBV1(local.db) {
		t.Fatal("seed installation did not swap startup DB")
	}
	waitTail()
	targetClient, err := NewFixedPeerTCPClientV1(configs[targetIndex])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(targetClient.Close)
	tailReply, err := targetClient.call(ctx, command.OldNodeID, "replacement-tail", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil || tailReply.ReplacementTail == nil {
		t.Fatalf("authenticated exact prepared target tail read: %v", err)
	}
	unmarkedTail := command
	unmarkedTail.OwnerPreparation = nil
	unmarkedTailRaw, err := raftplacement.EncodeReplicaReplacementBeginV1(unmarkedTail)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := targetClient.call(ctx, command.OldNodeID, "replacement-tail", fixedPeerRequestV1{Entry: unmarkedTailRaw}, false); !errors.Is(err, errPeerAuthenticationV1) {
		t.Fatalf("target unmarked tail read=%v", err)
	}
	targetClient.Close()
	var hostedPacks, hostedDomains, hostedArtifactBytes uint64
	hostedRefs := make(map[string]bool)
	for _, placement := range configs[0].Vector.Manifest.Placements {
		if placement.GroupID != string(command.GroupID) {
			continue
		}
		hostedPacks++
		for _, asset := range configs[0].Vector.Manifest.Assets {
			if asset.PartitionID != placement.PartitionID {
				continue
			}
			ref := asset.Ref
			key := fmt.Sprintf("%s/%d/%d/%d", ref.Namespace, ref.FileID, ref.Offset, ref.Length)
			if !hostedRefs[key] {
				hostedRefs[key] = true
				hostedArtifactBytes += uint64(ref.Length)
			}
		}
	}
	manifest := configs[0].Vector.Manifest
	offsets, err := vectorPartitionCoordinatorDomainPackOffsetsV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for domain := 0; domain+1 < len(offsets); domain++ {
		anchor := manifest.DomainPacks[offsets[domain]].PackID
		if manifest.Placements[anchor].GroupID == string(command.GroupID) {
			hostedDomains++
		}
	}
	if !vectorPartitionCoordinatorUsesDomainGraphsV1(manifest) || hostedDomains != 1 || hostedPacks != 2 || hostedArtifactBytes == 0 {
		t.Fatalf("fixture must host one two-pack domain: domains=%d packs=%d bytes=%d", hostedDomains, hostedPacks, hostedArtifactBytes)
	}
	startWarm := func() *replacementNativeWorkV1 {
		t.Helper()
		reply, err := client.call(ctx, command.NewPeer.ID, "replacement-owner-warm", fixedPeerRequestV1{Entry: raw}, false)
		if err != nil || !reply.ReplacementPending {
			t.Fatalf("start actual warm worker: pending=%v err=%v", reply.ReplacementPending, err)
		}
		local.replacementWork.mu.Lock()
		work := local.replacementWork.work
		local.replacementWork.mu.Unlock()
		if work == nil || work.phase != "owner-warm" {
			t.Fatal("control did not start actual owner warm")
		}
		return work
	}
	warm := func(label string) {
		t.Helper()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		var err error
		if endpoint {
			err = client.PrepareReplicaReplacementOwnerEndpointV1(ctx, command)
		} else {
			err = client.WarmReplicaReplacementOwnerV1(ctx, command)
		}
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		t.Logf("private_owner_warm label=%s elapsed_ns=%d global_bytes=%d global_allocs=%d scope=caller_five_raft_nodes_background_and_control_polling cache=%+v hosted_search_domains=%d hosted_physical_packs=%d verified_declared_hosted_artifact_bytes=%d unique_hosted_refs=%d", label, elapsed.Nanoseconds(), after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, source().Stats(), hostedDomains, hostedPacks, hostedArtifactBytes, len(hostedRefs))
	}
	warm("cold")
	retained := source()
	if retained == nil {
		t.Fatal("warm did not retain source")
	}
	var coldEndpoint *replacementOwnerEndpointV1
	if endpoint {
		local.replacementWork.mu.Lock()
		coldEndpoint = local.replacementWork.ownerEndpoint
		local.replacementWork.mu.Unlock()
		if coldEndpoint == nil {
			t.Fatal("cold preparation did not retain endpoint")
		}
	}
	cold := retained.Stats()
	if cold.GenerationMisses != 1 || cold.PartitionMisses != hostedDomains {
		t.Fatalf("hosted domain searchers not opened: %+v", cold)
	}
	local.replacementWork.mu.Lock()
	installedDB := local.replacementWork.ownerDB
	local.replacementWork.mu.Unlock()
	if installedDB == local.db || !local.fsm.HasCurrentDBV1(installedDB) {
		t.Fatal("private source bound stale startup DB")
	}
	warm("cached")
	if endpoint {
		local.replacementWork.mu.Lock()
		cachedEndpoint := local.replacementWork.ownerEndpoint
		local.replacementWork.mu.Unlock()
		if cachedEndpoint != coldEndpoint {
			t.Fatal("cached preparation replaced endpoint")
		}
	}
	cached := retained.Stats()
	if source() != retained || cached.GenerationMisses != cold.GenerationMisses || cached.PartitionMisses != cold.PartitionMisses ||
		cached.GenerationHits <= cold.GenerationHits || cached.PartitionHits-cold.PartitionHits != hostedDomains {
		t.Fatalf("warm did not reuse every hosted domain searcher: cold=%+v cached=%+v", cold, cached)
	}
	assertPrivate()
	if qualification {
		request := replacementOwnerQualificationRequestV1(t, ctx, target, command)
		wrong := command
		wrong.OperationID += "-other"
		if result, err := client.QualifyReplicaReplacementOwnerV1(ctx, wrong, request); err == nil || len(result.Search.Partials) != 0 {
			t.Fatalf("different BEGIN acquired private endpoint: %+v %v", result, err)
		}
		unmarked := command
		unmarked.OwnerPreparation = nil
		if result, err := client.QualifyReplicaReplacementOwnerV1(ctx, unmarked, request); !errors.Is(err, raftcluster.ErrUnsupportedFeature) || len(result.Search.Partials) != 0 {
			t.Fatalf("unmarked BEGIN acquired qualification: %+v %v", result, err)
		}
		wrongNode := request
		wrongNode.TargetNodeID = command.OldNodeID
		if result, err := client.QualifyReplicaReplacementOwnerV1(ctx, command, wrongNode); !errors.Is(err, raftcluster.ErrUnsupportedFeature) || len(result.Search.Partials) != 0 {
			t.Fatalf("wrong target acquired qualification: %+v %v", result, err)
		}
		lifecycle := raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: runtimes[3].authority, Committer: runtimes[3].meta}
		if _, err := lifecycle.InvalidateGenerationBeforeRelevantMutationV1(ctx, *command.OwnerPreparation, "private qualification pending replacement"); !errors.Is(err, raftplacement.ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("pending preparation admitted ACTIVE invalidation producer: %v", err)
		}
		// The original single-voter leader is the actual quorum. Cached ANN state
		// cannot supply its proof after that process closes.
		if err := runtimes[1].Close(); err != nil {
			t.Fatal(err)
		}
		runtimes[1] = nil
		short, cancel := context.WithTimeout(ctx, time.Second)
		result, err := client.QualifyReplicaReplacementOwnerV1(short, command, request)
		cancel()
		if err == nil || len(result.Search.Partials) != 0 {
			t.Fatalf("lost leader quorum returned private hits: %+v %v", result, err)
		}
		runtimes[1], err = OpenFixedPeerTCPRuntimeV1(configs[1])
		if err != nil {
			t.Fatal(err)
		}
		fixedPeerWaitV1(t, ctx, func() bool {
			status, err := client.Status(ctx, command.OldNodeID)
			return err == nil && len(status.Groups) == 1 && status.Groups[0].LeaderID == command.OldNodeID
		})
		waitTail()
		assertReplacementOwnerQualificationParityV1(t, ctx, client, target, runtimes[1], command)
	}
	if _, err := os.Stat(filepath.Join(configs[targetIndex].RaftRoot, "nodes", string(command.NewPeer.ID), "groups", string(configs[targetIndex].Catalog.ID))); !os.IsNotExist(err) {
		t.Fatalf("warm created catalog storage: %v", err)
	}
	unmarked := command
	unmarked.OwnerPreparation = nil
	if err := client.WarmReplicaReplacementOwnerV1(ctx, unmarked); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
		t.Fatalf("unmarked warm=%v", err)
	}
	changed := command
	changedIdentity := identity
	changedIdentity.Generation++
	changed.OwnerPreparation = &changedIdentity
	if err := client.WarmReplicaReplacementOwnerV1(ctx, changed); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
		t.Fatalf("changed identity warm=%v", err)
	}
	ownerClient, err := NewFixedPeerTCPClientV1(configs[1])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ownerClient.Close)
	if _, err := ownerClient.call(ctx, command.OldNodeID, "replacement-tail", fixedPeerRequestV1{Entry: raw}, false); !errors.Is(err, errPeerAuthenticationV1) {
		t.Fatalf("wrong authenticated node tail read=%v", err)
	}
	if err := ownerClient.WarmReplicaReplacementOwnerV1(ctx, command); !errors.Is(err, errPeerAuthenticationV1) {
		t.Fatalf("noncatalog caller warm=%v", err)
	}
	if endpoint {
		if _, err := ownerClient.call(ctx, command.NewPeer.ID, "replacement-owner-endpoint", fixedPeerRequestV1{Entry: raw}, false); !errors.Is(err, errPeerAuthenticationV1) {
			t.Fatalf("noncatalog caller endpoint=%v", err)
		}
		unmarkedRaw, err := raftplacement.EncodeReplicaReplacementBeginV1(unmarked)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.call(ctx, command.NewPeer.ID, "replacement-owner-endpoint", fixedPeerRequestV1{Entry: unmarkedRaw}, false); !errors.Is(err, raftcluster.ErrUnsupportedFeature) {
			t.Fatalf("unmarked endpoint=%v", err)
		}
		if _, err := client.call(ctx, command.OldNodeID, "replacement-owner-endpoint", fixedPeerRequestV1{Entry: raw}, false); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
			t.Fatalf("wrong target endpoint=%v", err)
		}
		if _, err := client.peerTransport.ProbeShardEndpointV1(ctx, configs[targetIndex].Vector.ShardAddresses[command.GroupID][command.NewPeer.ID], command.NewPeer.ID, command.GroupID); !errors.Is(err, errPeerAuthenticationV1) {
			t.Fatalf("ordinary probe widened membership=%v", err)
		}
	}
	ownerClient.Close()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := client.WarmReplicaReplacementOwnerV1(canceled, command); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled warm=%v", err)
	}

	// Block the actual cached Pin authority callback, not the client's context.
	// Runtime Close must cancel and wait for this worker before closing its cache/DB.
	if source() != retained {
		t.Fatal("refusal controls changed the cache used by cancellation test")
	}
	entered := make(chan struct{})
	retained.testValidateActive = func(workCtx context.Context, _ collectionVectorPartitionGenerationKeyV1) error {
		close(entered)
		<-workCtx.Done()
		return workCtx.Err()
	}
	blocked := startWarm()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Restart retains receiver authority but no process-local ANN cache.
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	runtimes[targetIndex] = nil
	select {
	case <-blocked.done:
		if !errors.Is(blocked.err, context.Canceled) {
			t.Fatalf("actual worker not canceled by Close: %v", blocked.err)
		}
	default:
		t.Fatal("Close returned before actual warm worker")
	}
	if source() != nil {
		t.Fatal("Close retained private cache")
	}
	if _, err := retained.PinVectorPartitionGenerationV1(ctx, identity.Index.IndexName, identity.Generation); err == nil {
		t.Fatal("runtime close retained live source")
	}
	target, err = OpenFixedPeerTCPRuntimeV1(configs[targetIndex])
	if err != nil {
		t.Fatal(err)
	}
	runtimes[targetIndex] = target
	if _, err := client.call(ctx, command.NewPeer.ID, "replacement-prepare", fixedPeerRequestV1{Entry: raw}, false); err != nil {
		t.Fatal(err)
	}
	local = target.localDataV1(command.GroupID)
	waitTail()
	if endpoint {
		local.replacementWork.mu.Lock()
		restartedEndpoint := local.replacementWork.ownerEndpoint
		local.replacementWork.mu.Unlock()
		if restartedEndpoint != nil {
			t.Fatal("restart created endpoint by observation")
		}
		listener, err := net.Listen("tcp", configs[targetIndex].Vector.ShardAddresses[command.GroupID][command.NewPeer.ID])
		if err != nil {
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		assertPrivate()
	}
	if source() != nil {
		t.Fatal("restart/observation warmed source")
	}
	warm("restart-cold")
	retained = source()
	if qualification {
		assertReplacementOwnerQualificationParityV1(t, ctx, client, target, runtimes[1], command)
	}
	// Finish actual cached work without consuming its result. Mutation after
	// worker completion must still be refused by the later completion poll.
	completed := startWarm()
	fixedPeerWaitV1(t, ctx, func() bool {
		select {
		case <-completed.done:
			return true
		default:
			return false
		}
	})
	if completed.err != nil {
		t.Fatalf("unconsumed actual warm failed: %v", completed.err)
	}
	// Remove a declared hosted segment while native semantic progress stays valid.
	// Rename back restores the exact bytes; the refused cache must already retire.
	var assetPath string
	for _, asset := range configs[0].Vector.Manifest.Assets {
		for _, placement := range configs[0].Vector.Manifest.Placements {
			if placement.PartitionID == asset.PartitionID && placement.GroupID == string(command.GroupID) {
				assetPath = filepath.Join(backenddb.ColumnAssetRootDirPath(filepath.Join(configs[targetIndex].DataRoot, string(command.GroupID))), filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
				break
			}
		}
		if assetPath != "" {
			break
		}
	}
	if assetPath == "" {
		t.Fatal("no hosted segment")
	}
	var qualificationRequest VectorPartitionShardSearchRequestV1
	if qualification {
		qualificationRequest = replacementOwnerQualificationRequestV1(t, ctx, target, command)
	}
	hidden := assetPath + ".private-warm-test"
	if err := os.Rename(assetPath, hidden); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(hidden); err == nil {
			_ = os.Rename(hidden, assetPath)
		}
	})
	if endpoint {
		conn, err := client.peerTransport.dialScope(ctx, configs[targetIndex].Vector.ShardAddresses[command.GroupID][command.NewPeer.ID], command.NewPeer.ID, "shard:"+string(command.GroupID))
		if err == nil {
			_, err = probeVectorPartitionShardConnV1(ctx, conn)
			_ = conn.Close()
		}
		if err == nil {
			t.Fatal("endpoint probe accepted missing hosted asset")
		}
	}
	if qualification {
		result, err := client.QualifyReplicaReplacementOwnerV1(ctx, command, qualificationRequest)
		if err == nil || len(result.Search.Partials) != 0 {
			t.Fatalf("missing assets returned private hits: %+v %v", result, err)
		}
	}
	if err := client.WarmReplicaReplacementOwnerV1(ctx, command); err == nil {
		t.Fatal("completed warm accepted subsequently missing asset")
	}
	if endpoint {
		local.replacementWork.mu.Lock()
		staleEndpoint := local.replacementWork.ownerEndpoint
		local.replacementWork.mu.Unlock()
		if staleEndpoint != nil {
			t.Fatal("missing asset retained endpoint")
		}
	}
	if source() != nil {
		t.Fatal("missing asset retained cache")
	}
	if _, err := retained.PinVectorPartitionGenerationV1(ctx, identity.Index.IndexName, identity.Generation); err == nil {
		t.Fatal("retired source still admits generation")
	}
	if err := os.Rename(hidden, assetPath); err != nil {
		t.Fatal(err)
	}
	warm("asset-restored-cold")
	retained = source()

	if qualification {
		qualificationRequest = replacementOwnerQualificationRequestV1(t, ctx, target, command)
	}
	// A real native snapshot swap invalidates the exact retained DB identity.
	snapshot, err := local.fsm.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := snapshot.Release(); err != nil {
			t.Error(err)
		}
	})
	archive, err := snapshot.OpenArchive()
	if err != nil {
		t.Fatal(err)
	}
	payload, readErr := io.ReadAll(archive)
	if err := errors.Join(readErr, archive.Close(), snapshot.Release()); err != nil {
		t.Fatal(err)
	}
	if err := local.fsm.InstallRaftSnapshotV1(bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if endpoint {
		conn, err := client.peerTransport.dialScope(ctx, configs[targetIndex].Vector.ShardAddresses[command.GroupID][command.NewPeer.ID], command.NewPeer.ID, "shard:"+string(command.GroupID))
		if err == nil {
			_, err = probeVectorPartitionShardConnV1(ctx, conn)
			_ = conn.Close()
		}
		if err == nil {
			t.Fatal("endpoint probe accepted snapshot-replaced DB")
		}
	}
	if qualification {
		result, err := client.QualifyReplicaReplacementOwnerV1(ctx, command, qualificationRequest)
		if err == nil || len(result.Search.Partials) != 0 {
			t.Fatalf("DB swap returned private hits: %+v %v", result, err)
		}
	}
	if err := client.WarmReplicaReplacementOwnerV1(ctx, command); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("snapshot-replaced cached DB=%v", err)
	}
	if endpoint {
		local.replacementWork.mu.Lock()
		staleEndpoint := local.replacementWork.ownerEndpoint
		local.replacementWork.mu.Unlock()
		if staleEndpoint != nil {
			t.Fatal("DB swap retained endpoint")
		}
	}
	if source() != nil {
		t.Fatal("snapshot replacement retained cache")
	}
	if _, err := retained.PinVectorPartitionGenerationV1(ctx, identity.Index.IndexName, identity.Generation); err == nil {
		t.Fatal("DB swap left live source")
	}
}
