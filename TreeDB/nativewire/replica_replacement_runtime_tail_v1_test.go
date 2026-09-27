package nativewire

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestReplacementPromotesOnlyDurableTailAndRetainsOldVoterV1(t *testing.T) {
	testReplacementPublicInstallV1(t, false, true)
}

func testReplacementPromotionTailV1(t *testing.T, ctx context.Context, client *FixedPeerTCPClientV1, configs []FixedPeerTCPConfigV1, runtimes []*FixedPeerTCPRuntimeV1, catalogLeader int, group FixedPeerTCPGroupV1, operation raftplacement.ReplicaReplacementBeginV1, enrolled raftcluster.CommittedRaftConfigurationV1) {
	t.Helper()
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(operation)
	if err != nil {
		t.Fatal(err)
	}
	dataLeader, err := client.leader(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	var source *fixedPeerDataV1
	for _, runtime := range runtimes[:3] {
		if runtime.config.NodeID == dataLeader {
			source = runtime.localDataV1(group.ID)
		}
	}
	if source == nil {
		t.Fatal("missing data leader")
	}
	if _, err := client.call(ctx, dataLeader, "replacement-promote", fixedPeerRequestV1{Entry: raw}, true); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("native promotion before committed intent: %v", err)
	}
	// The old three voters can commit a new command while the learner is absent.
	// That learner has the installed snapshot but cannot prove this durable tail.
	if err := runtimes[3].Close(); err != nil {
		t.Fatal(err)
	}
	version, known, err := source.fsm.CurrentCatalogVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.provider.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{NodeID: dataLeader, GroupID: group.ID, EntryBytes: fixedPeerCreateEntryV1(t, "orders", version), CurrentCatalogVersion: version, HasCurrentCatalogVersion: known, SyncLocalCommandWAL: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PromoteReplicaReplacementV1(ctx, configs[catalogLeader].NodeID, operation); err == nil {
		t.Fatal("offline learner promoted")
	}
	state, err := runtimes[catalogLeader].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || state.Phase != raftplacement.ReplicaReplacementAddIntentV1 {
		t.Fatalf("failed proof changed authority: %+v %v", state, err)
	}
	configuration, progress, err := source.provider.ReplacementReadFenceV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := source.fsm.ReplacementTailProgressV1(ctx, raftentry.ApplyEntryID{Term: progress.Term, Index: progress.Index})
	if err != nil {
		t.Fatal(err)
	}
	if proof.EntryID.Index <= state.Seed.Index {
		t.Fatalf("fixture did not create a post-snapshot command: %+v", proof)
	}
	restart := func(i int) {
		t.Helper()
		if err := runtimes[i].Close(); err != nil {
			t.Fatal(err)
		}
		runtime, err := OpenFixedPeerTCPRuntimeV1(configs[i])
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
	restart(3)
	// Public preparation must reopen the operation-owned receiver after restart.
	if _, err := client.PrepareReplicaReplacementV1(ctx, configs[catalogLeader].NodeID, operation); err != nil {
		t.Fatal(err)
	}
	target := runtimes[3].localDataV1(group.ID)
	fixedPeerWaitV1(t, ctx, func() bool {
		got, err := target.fsm.ReplacementTailProgressV1(ctx, proof.EntryID)
		return err == nil && got == proof
	})
	tail := raftcluster.ReplacementTailV1{GroupID: group.ID, LeaderID: dataLeader, LeaderTerm: configuration.Term, CommitIndex: configuration.CommitIndex, ConfigurationIndex: configuration.ConfigurationIndex, Progress: proof}
	candidate := state
	candidate.Phase = raftplacement.ReplicaReplacementPromoteIntentV1
	candidate.Tail = &tail
	changed := tail
	changed.Progress.ProgressDigest[0] ^= 1
	candidate.Tail = &changed
	mismatch, err := raftplacement.EncodeReplicaReplacementStateV1(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.call(ctx, operation.NewPeer.ID, "replacement-tail-check", fixedPeerRequestV1{Entry: mismatch}, false); !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
		t.Fatalf("mismatched semantic tail accepted: %v", err)
	}
	candidate.Tail = &tail
	if err := client.commitReplacementPhaseV1(ctx, configs[catalogLeader].NodeID, candidate); err == nil {
		t.Fatal("caller-supplied promotion proof admitted")
	}
	// The shared catalog mutation seam must charge phase commits too. Reads and
	// durable evidence can proceed, but saturation must leave intent uncommitted.
	admission := runtimes[catalogLeader].client.peerTransport.admission
	scope := "raft:" + string(configs[catalogLeader].Catalog.ID)
	admission.mu.Lock()
	limit := admission.scopes[scope].limits[peerProposalsV1]
	admission.mu.Unlock()
	held, err := admission.acquire(scope, peerProposalsV1, limit)
	if err != nil {
		t.Fatal(err)
	}
	_, promotionErr := client.call(ctx, configs[catalogLeader].NodeID, "replacement-promotion-intent", fixedPeerRequestV1{Entry: raw}, true)
	held.release()
	if !errors.Is(promotionErr, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("saturated promotion commit: %v", promotionErr)
	}
	state, err = runtimes[catalogLeader].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || state.Phase != raftplacement.ReplicaReplacementAddIntentV1 {
		t.Fatalf("saturation changed phase: %+v %v", state, err)
	}
	if _, err := client.call(ctx, configs[catalogLeader].NodeID, "replacement-promotion-intent", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		t.Fatal(err)
	}
	intent, err := runtimes[catalogLeader].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || intent.Tail == nil || intent.Phase != raftplacement.ReplicaReplacementPromoteIntentV1 {
		t.Fatalf("intent=%+v %v", intent, err)
	}
	promoted, err := client.call(ctx, dataLeader, "replacement-promote", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil || promoted.Membership == nil {
		t.Fatalf("actual promotion=%+v %v", promoted, err)
	}
	assertPromoted := func(configuration raftcluster.CommittedRaftConfigurationV1) {
		t.Helper()
		if len(configuration.Members) != 4 || configuration.ConfigurationIndex <= enrolled.ConfigurationIndex {
			t.Fatalf("promotion=%+v", configuration)
		}
		old, new := false, false
		for _, member := range configuration.Members {
			if !member.Voter {
				t.Fatal("promotion left nonvoter")
			}
			old = old || member.ID == operation.OldNodeID
			new = new || member.ID == operation.NewPeer.ID
		}
		if !old || !new {
			t.Fatal("promotion removed old voter or omitted target")
		}
	}
	assertPromoted(*promoted.Membership)
	// Model native success whose coordinator never records completion. Restart
	// target and an original peer before the public retry restores peer allowlists.
	restart(3)
	original := 0
	for configs[original].NodeID == dataLeader || original == catalogLeader {
		original++
	}
	restart(original)
	reconciled, err := client.PromoteReplicaReplacementV1(ctx, configs[catalogLeader].NodeID, operation)
	if err != nil || reconciled.ConfigurationIndex != promoted.Membership.ConfigurationIndex {
		t.Fatalf("restart promotion reconciliation=%+v %v", reconciled, err)
	}
	assertPromoted(reconciled)
	completed, err := runtimes[catalogLeader].authority.ReplicaReplacementStateV1(group.ID)
	if err != nil || completed.Phase != raftplacement.ReplicaReplacementPromotedV1 || completed.Tail == nil || *completed.Tail != *intent.Tail {
		t.Fatalf("completion changed proof: %+v %v", completed, err)
	}
	if _, err := client.call(ctx, completed.Seed.SourceNodeID, "replacement-install", fixedPeerRequestV1{Entry: raw}, true); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("promotion reopened seed install: %v", err)
	}
}
