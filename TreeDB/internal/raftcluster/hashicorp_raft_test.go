package raftcluster_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	. "github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestHashicorpRaftProviderThreeNodeLeaderSubmitAppliesFollowers(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)

	entry := testClusterCreateCollectionEntry(t, 7)
	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer leaderCancel()
	result, err := leader.submitter.SubmitCommandEntryV1(leaderCtx, entry, raftentry.RequestMetadataV1{
		RequestID: 701,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("leader SubmitCommandEntryV1: %v", err)
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable {
		t.Fatalf("ack/recoverable=%d/%v want raft_committed/true", result.ActualAck, result.CommittedRecoverable)
	}
	if !result.Evidence.ProvesProductionConsensus() || result.Evidence.Kind != CommitEvidenceProductionConsensusV1 {
		t.Fatalf("evidence=%+v does not prove production consensus", result.Evidence)
	}
	if result.Evidence.LeaderID != leader.id {
		t.Fatalf("evidence leader=%q want %q", result.Evidence.LeaderID, leader.id)
	}
	if result.CommittedEntry.Term == 0 || result.CommittedEntry.Index == 0 {
		t.Fatalf("committed entry id=%d/%d, want non-zero", result.CommittedEntry.Term, result.CommittedEntry.Index)
	}
	if result.ApplyResult.Status != raftentry.ApplyStatusApplied && result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied {
		t.Fatalf("leader apply status=%s want applied or already-applied", result.ApplyResult.Status)
	}

	cluster.waitApplied(t, result.CommittedEntry.EntryID())
	for _, node := range cluster.nodes {
		if _, err := collections.NewCollectionManager(node.db).OpenCollection("users"); err != nil {
			t.Fatalf("%s OpenCollection users after quorum commit: %v", node.id, err)
		}
	}

	follower := cluster.firstFollower(t, leader.id)
	cluster.waitFollowerHint(t, follower, leader.id)
	followerCtx, followerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer followerCancel()
	_, err = follower.submitter.SubmitCommandEntryV1(followerCtx, testClusterCreateCollectionEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 702,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower SubmitCommandEntryV1 err=%v want ErrNotLeader", err)
	}
	if !strings.Contains(err.Error(), "leader_hint="+string(leader.id)) {
		t.Fatalf("follower error=%v missing leader hint %q", err, leader.id)
	}
}

func TestHashicorpRaftProviderReadIndexReturnsProductionProofForLeader(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := leader.submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 703,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1: %v", err)
	}
	cluster.waitApplied(t, result.CommittedEntry.EntryID())

	target := ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"}
	proof, err := leader.provider.ReadIndex(ctx, target)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if proof.NodeID != leader.id ||
		proof.GroupID != "group-a" ||
		proof.EvidenceKind != ReadIndexEvidenceProduction ||
		!proof.HasQuorum ||
		proof.Term == 0 ||
		proof.Index < result.CommittedEntry.Index {
		t.Fatalf("proof=%+v committed=%+v", proof, result.CommittedEntry)
	}
	if err := target.Check(proof); err != nil {
		t.Fatalf("target.Check: %v", err)
	}
	progress, err := leader.fsm.WaitAppliedIndex(ctx, proof.AppliedIndexBarrier())
	if err != nil {
		t.Fatalf("WaitAppliedIndex(%+v): %v progress=%+v", proof.AppliedIndexBarrier(), err, progress)
	}
	if progress.NodeID != leader.id || progress.GroupID != "group-a" || progress.Index < proof.Index || !progress.HasApplied {
		t.Fatalf("progress=%+v proof=%+v", progress, proof)
	}
}

func TestHashicorpRaftProviderReadIndexUsesAppliedProgressIndex(t *testing.T) {
	applier := &progressReportingApplier{
		progress: AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       3,
			Index:      42,
			HasApplied: true,
		},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)
	if leader.id != "node-a" {
		applier.setProgressNodeID(leader.id)
	}
	wantIndex := applier.progressIndex()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	proof, err := leader.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if proof.Index != wantIndex {
		t.Fatalf("proof index=%d want applied progress index %d", proof.Index, wantIndex)
	}
	if !proof.HasQuorum || proof.EvidenceKind != ReadIndexEvidenceProduction {
		t.Fatalf("proof=%+v want production quorum proof", proof)
	}
}

func TestHashicorpRaftProviderReadIndexRejectsAppliedProgressIdentityMismatch(t *testing.T) {
	tests := []struct {
		name     string
		progress AppliedProgress
	}{
		{
			name: "node",
			progress: AppliedProgress{
				NodeID:     "node-z",
				GroupID:    "group-a",
				Term:       3,
				Index:      42,
				HasApplied: true,
			},
		},
		{
			name: "group",
			progress: AppliedProgress{
				NodeID:     "node-a",
				GroupID:    "group-z",
				Term:       3,
				Index:      42,
				HasApplied: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applier := &progressReportingApplier{progress: tt.progress}
			cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
			leader := cluster.waitForLeader(t)
			if tt.name == "group" {
				applier.setProgressNodeID(leader.id)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := leader.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
			if !errors.Is(err, ErrReadBarrierTargetMismatch) {
				t.Fatalf("ReadIndex err=%v want ErrReadBarrierTargetMismatch", err)
			}
		})
	}
}

func TestHashicorpRaftProviderReadIndexRejectsMissingAppliedProgressIndex(t *testing.T) {
	applier := &progressReportingApplier{
		progress: AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       3,
			HasApplied: true,
		},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)
	applier.setProgressNodeID(leader.id)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := leader.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("ReadIndex err=%v want ErrReadBarrierNotSatisfied", err)
	}
}

func TestHashicorpRaftProviderReadIndexRejectsWithoutBarrierWhenAppliedProgressUnavailable(t *testing.T) {
	tests := []struct {
		name    string
		applier CommittedCommandApplierV1
	}{
		{
			name:    "reader-missing",
			applier: &recordingClusterApplier{},
		},
		{
			name: "no-applied-command",
			applier: &progressReportingApplier{
				progress: AppliedProgress{
					NodeID:  "node-a",
					GroupID: "group-a",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newHashicorpRaftProviderOnlyCluster(t, tt.applier)
			leader := cluster.waitForLeader(t)
			if applier, ok := tt.applier.(*progressReportingApplier); ok {
				applier.setProgressNodeID(leader.id)
			}
			if leader.barrierLogStore == nil {
				t.Fatalf("%s has no barrier-counting log store", leader.id)
			}
			before := leader.barrierLogStore.barrierLogCount()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := leader.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
			if !errors.Is(err, ErrReadBarrierNotSatisfied) {
				t.Fatalf("ReadIndex err=%v want ErrReadBarrierNotSatisfied", err)
			}
			if after := leader.barrierLogStore.barrierLogCount(); after != before {
				t.Fatalf("ReadIndex appended %d barrier logs before proving applied progress", after-before)
			}
		})
	}
}

func TestHashicorpRaftProviderReadIndexDoesNotAppendBarrierLog(t *testing.T) {
	applier := &progressReportingApplier{
		progress: AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       3,
			Index:      42,
			HasApplied: true,
		},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)
	applier.setProgressNodeID(leader.id)
	if leader.barrierLogStore == nil {
		t.Fatalf("%s has no barrier-counting log store", leader.id)
	}
	before := leader.barrierLogStore.barrierLogCount()
	wantIndex := applier.progressIndex()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	target := ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"}
	for i := 0; i < 3; i++ {
		proof, err := leader.provider.ReadIndex(ctx, target)
		if err != nil {
			t.Fatalf("ReadIndex #%d: %v", i+1, err)
		}
		if proof.Index != wantIndex ||
			proof.EvidenceKind != ReadIndexEvidenceProduction ||
			!proof.HasQuorum {
			t.Fatalf("ReadIndex #%d proof=%+v want production proof at applied progress index %d", i+1, proof, wantIndex)
		}
	}
	if after := leader.barrierLogStore.barrierLogCount(); after != before {
		t.Fatalf("ReadIndex appended %d barrier logs", after-before)
	}
}

func TestHashicorpRaftProviderReadIndexWaitsForTreeDBCommandProgressInGap(t *testing.T) {
	applier := &progressReportingApplier{
		progress: AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       1,
			Index:      1,
			HasApplied: true,
		},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)
	applier.setProgressNodeID(leader.id)

	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	result, err := leader.provider.CommitCommandEntryV1(commitCtx, CommitCommandEntryV1Request{
		GroupID:                  "group-a",
		NodeID:                   leader.id,
		EntryBytes:               testClusterCreateCollectionEntry(t, 7),
		CurrentCatalogVersion:    7,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      true,
	})
	if err != nil {
		t.Fatalf("CommitCommandEntryV1: %v", err)
	}
	if result.Entry.Index < 2 {
		t.Fatalf("committed command index=%d, want room for stale progress", result.Entry.Index)
	}
	staleProgress := AppliedProgress{
		NodeID:     leader.id,
		GroupID:    "group-a",
		Term:       result.Entry.Term,
		Index:      result.Entry.Index - 1,
		HasApplied: true,
	}
	applier.setProgress(staleProgress)

	readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer readCancel()
	_, err = leader.provider.ReadIndex(readCtx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
	if err == nil {
		t.Fatalf("ReadIndex succeeded with committed command index %d ahead of TreeDB progress %d", result.Entry.Index, staleProgress.Index)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("ReadIndex err=%v want fail-closed deadline or ErrReadBarrierNotSatisfied", err)
	}
}

func TestHashicorpRaftProviderReadIndexWaitsForInitialAppliedProgress(t *testing.T) {
	applier := &progressReportingApplier{
		progress: AppliedProgress{
			NodeID:  "node-a",
			GroupID: "group-a",
		},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)
	applier.setProgress(AppliedProgress{
		NodeID:  leader.id,
		GroupID: "group-a",
	})

	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	result, err := leader.provider.CommitCommandEntryV1(commitCtx, CommitCommandEntryV1Request{
		GroupID:                  "group-a",
		NodeID:                   leader.id,
		EntryBytes:               testClusterCreateCollectionEntry(t, 7),
		CurrentCatalogVersion:    7,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      true,
	})
	if err != nil {
		t.Fatalf("CommitCommandEntryV1: %v", err)
	}
	if leader.barrierLogStore == nil {
		t.Fatalf("%s has no barrier-counting log store", leader.id)
	}
	before := leader.barrierLogStore.barrierLogCount()

	var mu sync.Mutex
	progressReports := 0
	applier.setBeforeReport(func() {
		mu.Lock()
		progressReports++
		current := progressReports
		mu.Unlock()
		if current < 2 {
			return
		}
		applier.setProgress(AppliedProgress{
			NodeID:     leader.id,
			GroupID:    "group-a",
			Term:       result.Entry.Term,
			Index:      result.Entry.Index,
			HasApplied: true,
		})
	})

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	proof, err := leader.provider.ReadIndex(readCtx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if proof.Index != result.Entry.Index {
		t.Fatalf("proof index=%d want first applied command index %d", proof.Index, result.Entry.Index)
	}
	mu.Lock()
	gotReports := progressReports
	mu.Unlock()
	if gotReports < 2 {
		t.Fatalf("AppliedProgress reports=%d want at least 2", gotReports)
	}
	if after := leader.barrierLogStore.barrierLogCount(); after != before {
		t.Fatalf("ReadIndex appended %d barrier logs while waiting for initial progress", after-before)
	}
}

func TestHashicorpRaftProviderReadIndexRejectsLeadershipLostBeforeProof(t *testing.T) {
	applier := &progressReportingApplier{
		progress: AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       3,
			Index:      42,
			HasApplied: true,
		},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)
	applier.setProgressNodeID(leader.id)

	var closeOnce sync.Once
	applier.setBeforeReport(func() {
		closeOnce.Do(func() {
			_ = leader.provider.Close()
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := leader.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
	if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrHashicorpRaftUnavailable) {
		t.Fatalf("ReadIndex err=%v want not-leader or unavailable after leadership loss", err)
	}
}

func TestHashicorpRaftProviderReadIndexRejectsFollowerStrongRead(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)
	follower := cluster.firstFollower(t, leader.id)
	cluster.waitFollowerHint(t, follower, leader.id)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := follower.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: follower.id, GroupID: "group-a"})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower ReadIndex err=%v want ErrNotLeader", err)
	}
	if !strings.Contains(err.Error(), "leader_hint="+string(leader.id)) {
		t.Fatalf("follower ReadIndex err=%v missing leader hint %q", err, leader.id)
	}
}

func TestHashicorpRaftProviderReadIndexRejectsTargetMismatch(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)

	tests := []struct {
		name   string
		target ReadIndexBarrier
	}{
		{name: "node", target: ReadIndexBarrier{NodeID: "node-z", GroupID: "group-a"}},
		{name: "group", target: ReadIndexBarrier{NodeID: leader.id, GroupID: "group-b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := leader.provider.ReadIndex(context.Background(), tt.target)
			if !errors.Is(err, ErrReadBarrierTargetMismatch) {
				t.Fatalf("ReadIndex err=%v want ErrReadBarrierTargetMismatch", err)
			}
		})
	}
}

func TestHashicorpRaftProviderSnapshotRejoinsLaggingFollowerAndCompactsLogs(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("Raft snapshot rejoin requires durable rename and removal namespaces")
	}
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)
	lagging := cluster.firstFollower(t, leader.id)
	healthy := cluster.firstFollowerExcept(t, leader.id, lagging.id)
	cluster.disconnectNode(t, lagging.id)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	createResult, err := leader.submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 705,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("Submit create: %v", err)
	}
	cluster.waitAppliedOn(t, createResult.CommittedEntry.EntryID(), leader.id, healthy.id)
	insertResult, err := leader.submitter.SubmitCommandEntryV1(ctx, testClusterCommandEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 706,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("Submit insert: %v", err)
	}
	cluster.waitAppliedOn(t, insertResult.CommittedEntry.EntryID(), leader.id, healthy.id)
	if got, ok := lagging.fsm.LastApplied(); ok && got.Index >= insertResult.CommittedEntry.Index {
		t.Fatalf("lagging follower applied %d/%d while disconnected", got.Term, got.Index)
	}

	snapshot, err := leader.provider.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snapshot.LastIncludedIndex < insertResult.CommittedEntry.Index ||
		snapshot.Manifest.LastIncludedIndex != insertResult.CommittedEntry.Index ||
		snapshot.Manifest.GroupID != "group-a" ||
		snapshot.SizeBytes == 0 {
		t.Fatalf("snapshot result=%+v insert=%+v", snapshot, insertResult.CommittedEntry)
	}
	if !snapshot.LogCompacted || (snapshot.FirstLogIndexAfter != 0 && snapshot.FirstLogIndexAfter <= snapshot.FirstLogIndexBefore) {
		t.Fatalf("snapshot did not compact logs: %+v", snapshot)
	}
	proof, err := leader.provider.ReadIndex(ctx, ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"})
	if err != nil {
		t.Fatalf("ReadIndex after snapshot compaction: %v", err)
	}
	if proof.Index < insertResult.CommittedEntry.Index ||
		proof.EvidenceKind != ReadIndexEvidenceProduction ||
		!proof.HasQuorum {
		t.Fatalf("post-snapshot read-index proof=%+v want production proof at or beyond %d", proof, insertResult.CommittedEntry.Index)
	}

	cluster.connectAllTransports(t)
	cluster.waitApplied(t, insertResult.CommittedEntry.EntryID())
	leaderDigest, err := leader.fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("leader LogicalDigestV1: %v", err)
	}
	laggingDigest, err := lagging.fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("lagging LogicalDigestV1: %v", err)
	}
	if laggingDigest != leaderDigest {
		t.Fatalf("lagging digest=%s leader=%s after snapshot rejoin", laggingDigest.Hex(), leaderDigest.Hex())
	}
}

func BenchmarkHashicorpRaftProviderReadIndex(b *testing.B) {
	cluster := newHashicorpRaftDBCluster(b)
	leader := cluster.waitForLeader(b)
	ctx := context.Background()
	result, err := leader.submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntry(b, 7), raftentry.RequestMetadataV1{
		RequestID: 704,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		b.Fatalf("SubmitCommandEntryV1: %v", err)
	}
	cluster.waitApplied(b, result.CommittedEntry.EntryID())

	target := ReadIndexBarrier{NodeID: leader.id, GroupID: "group-a"}
	proof, err := leader.provider.ReadIndex(ctx, target)
	if err != nil {
		b.Fatalf("warm ReadIndex: %v", err)
	}
	if proof.Index < result.CommittedEntry.Index {
		b.Fatalf("warm proof=%+v committed=%+v", proof, result.CommittedEntry)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proof, err := leader.provider.ReadIndex(ctx, target)
		if err != nil {
			b.Fatalf("ReadIndex: %v", err)
		}
		if proof.Index < result.CommittedEntry.Index {
			b.Fatalf("proof=%+v committed=%+v", proof, result.CommittedEntry)
		}
	}
}

func TestHashicorpRaftProviderFollowerRejectsWithoutDirectMutation(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)
	follower := cluster.firstFollower(t, leader.id)
	cluster.waitFollowerHint(t, follower, leader.id)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := follower.submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntryNamed(t, "users", "raftcluster/create/users/follower", 7), raftentry.RequestMetadataV1{
		RequestID: 711,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower SubmitCommandEntryV1 err=%v result=%+v want ErrNotLeader", err, result)
	}
	if result.CommittedRecoverable || result.Evidence.ProvesProductionConsensus() || result.CommittedEntry.Index != 0 {
		t.Fatalf("follower rejected result=%+v, want no committed success", result)
	}
	for _, node := range cluster.nodes {
		assertClusterCollectionMissing(t, node, "users")
	}
}

func TestHashicorpRaftProviderLeaderCrashAfterCommitRestartCatchUpConverges(t *testing.T) {
	testHashicorpRaftProviderLeaderCrashRestartCatchUp(t, false)
}

func TestHashicorpRaftProviderLeaderCrashRestartCatchUpSurvivesReplicationBackoff(t *testing.T) {
	testHashicorpRaftProviderLeaderCrashRestartCatchUp(t, true)
}

func testHashicorpRaftProviderLeaderCrashRestartCatchUp(t *testing.T, waitForBackoff bool) {
	t.Helper()
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)
	acknowledged := testClusterCreateCollectionEntryNamed(t, "users", "raftcluster/create/users/failover", 7)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := leader.submitter.SubmitCommandEntryV1(ctx, acknowledged, raftentry.RequestMetadataV1{
		RequestID: 721,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("leader SubmitCommandEntryV1 acknowledged write: %v", err)
	}
	if !first.CommittedRecoverable || !first.Evidence.ProvesProductionConsensus() {
		t.Fatalf("first result=%+v want recoverable production consensus", first)
	}
	cluster.waitApplied(t, first.CommittedEntry.EntryID())
	for _, node := range cluster.nodes {
		assertClusterCollectionExists(t, node, "users")
	}
	firstDigest := cluster.assertLogicalDigestsEqual(t)

	crashedID := leader.id
	cluster.shutdownNode(t, crashedID)
	assertHashicorpRaftStoreFiles(t, leader, "after committed leader shutdown")

	newLeader := cluster.waitForLeader(t)
	if newLeader.id == crashedID {
		t.Fatalf("leader after crash=%s, want surviving node", newLeader.id)
	}
	for _, id := range cluster.runningNodeIDs() {
		assertClusterCollectionExists(t, cluster.nodes[id], "users")
		assertClusterLastApplied(t, cluster.nodes[id], first.CommittedEntry.EntryID())
	}

	newLeader.submitter = newHashicorpRaftDBSubmitter(t, newLeader, collectionCountCatalogVersion(newLeader, 7))
	secondEntry := testClusterCreateCollectionEntryNamed(t, "orders", "raftcluster/create/orders/failover", 8)
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer secondCancel()
	second, err := newLeader.submitter.SubmitCommandEntryV1(secondCtx, secondEntry, raftentry.RequestMetadataV1{
		RequestID: 722,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("new leader SubmitCommandEntryV1 while old leader down: %v", err)
	}
	if second.ActualAck != iwire.AckRaftCommitted || !second.CommittedRecoverable {
		t.Fatalf("second ack/recoverable=%d/%v want raft_committed/true", second.ActualAck, second.CommittedRecoverable)
	}
	if !second.Evidence.ProvesProductionConsensus() || !second.Evidence.Committed {
		t.Fatalf("second evidence=%+v want committed production consensus", second.Evidence)
	}
	if second.CatalogVersion != 9 || !second.HasCatalogVersion {
		t.Fatalf("second catalog version=%d has=%t want 9/true", second.CatalogVersion, second.HasCatalogVersion)
	}
	restarted := cluster.restartDBNode(t, crashedID)
	if waitForBackoff {
		cluster.waitRestartBackoff(t, newLeader.id, crashedID, second.CommittedEntry.EntryID())
	}
	cluster.connectAllTransports(t)
	cluster.waitRestartCatchUp(t, crashedID, second.CommittedEntry.EntryID())
	assertHashicorpRaftStoreFiles(t, restarted, "after leader restart")
	for _, node := range cluster.nodes {
		assertClusterLastApplied(t, node, second.CommittedEntry.EntryID())
		assertClusterCollectionExists(t, node, "users")
		assertClusterCollectionExists(t, node, "orders")
	}

	replayed, err := restarted.fsm.ApplyCommittedCommandEntryV1(context.Background(), first.CommittedEntry.Clone())
	if err != nil {
		t.Fatalf("restarted node replay acknowledged entry from durable result record: %v result=%+v", err, replayed)
	}
	if replayed.CommandDigest != first.ApplyResult.CommandDigest || replayed.ResultDigest != first.ApplyResult.ResultDigest {
		t.Fatalf("replayed result=%+v want original command/result digests %+v", replayed, first.ApplyResult)
	}
	if got := cluster.assertLogicalDigestsEqual(t); got == firstDigest {
		t.Fatalf("digest after catch-up=%s still equals first-only digest; want second committed command reflected", got.Hex())
	}
}

func TestHashicorpRaftProviderLeaderCrashBeforeCommitDoesNotReportSuccessOrMutate(t *testing.T) {
	cluster := newHashicorpRaftDBCluster(t)
	leader := cluster.waitForLeader(t)

	preflightReady := make(chan struct{})
	releasePreflight := make(chan struct{})
	var releasePreflightOnce sync.Once
	releaseBlockedPreflight := func() {
		releasePreflightOnce.Do(func() {
			close(releasePreflight)
		})
	}
	defer releaseBlockedPreflight()

	inflightSubmitter := newHashicorpRaftDBSubmitterWithPreflight(t, leader, staticCatalogVersion(7), &gatedCommandEntryPreflight{
		delegate: leader.fsm,
		ready:    preflightReady,
		release:  releasePreflight,
	})
	entry := testClusterCreateCollectionEntryNamed(t, "users", "raftcluster/create/users/precommit-crash", 7)
	submitCtx, submitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer submitCancel()
	type submitAttempt struct {
		result SubmitResultV1
		err    error
	}
	submitDone := make(chan submitAttempt, 1)
	go func() {
		result, err := inflightSubmitter.SubmitCommandEntryV1(submitCtx, entry, raftentry.RequestMetadataV1{
			RequestID: 731,
			AckPolicy: iwire.AckRaftCommitted,
		})
		submitDone <- submitAttempt{result: result, err: err}
	}()

	select {
	case <-preflightReady:
	case attempt := <-submitDone:
		t.Fatalf("SubmitCommandEntryV1 finished before pre-commit partition: err=%v result=%+v", attempt.err, attempt.result)
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for live leader preflight; states=%s", cluster.admissionSummary())
	}
	status, err := leader.provider.ClusterAdmissionStatus(context.Background())
	if err != nil {
		t.Fatalf("%s ClusterAdmissionStatus before partition: %v", leader.id, err)
	}
	if !status.Leader || status.Unavailable {
		t.Fatalf("%s admission before partition=%+v, want live leader", leader.id, status)
	}

	cluster.disconnectNode(t, leader.id)
	releaseBlockedPreflight()

	var attempt submitAttempt
	select {
	case attempt = <-submitDone:
	// The submit remains bounded by its five-second context. Give its result
	// delivery enough slack that the test's own timeout cannot race that bound
	// now that the partitioned leader intentionally retains its lease longer.
	case <-time.After(8 * time.Second):
		t.Fatalf("timed out waiting for partitioned pre-commit submit to fail; states=%s", cluster.admissionSummary())
	}
	if attempt.err == nil {
		t.Fatalf("SubmitCommandEntryV1 on partitioned leader succeeded: %+v", attempt.result)
	}
	if attempt.result.CommittedRecoverable || attempt.result.Evidence.ProvesProductionConsensus() || attempt.result.CommittedEntry.Index != 0 {
		t.Fatalf("pre-commit crash result=%+v err=%v, want no committed success", attempt.result, attempt.err)
	}
	for _, node := range cluster.nodes {
		assertClusterCollectionMissing(t, node, "users")
	}
}

func TestHashicorpRaftProviderLocalRecoverabilityFailureDoesNotReportCommittedRecoverable(t *testing.T) {
	applyErr := errors.New("local command WAL sync failed")
	failingApplier := CommittedCommandApplierFunc(func(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
		return raftentry.ApplyResultV1{
			Status:                 raftentry.ApplyStatusRecoveryRequired,
			DeterministicErrorCode: raftentry.ErrorUnsafeDurabilityModeV1,
		}, applyErr
	})
	cluster := newHashicorpRaftProviderOnlyCluster(t, failingApplier)
	leader := cluster.waitForLeader(t)
	submitter, err := NewSingleGroupSubmitter(SingleGroupSubmitterOptions{
		Cluster:           leader.cfg,
		AdmissionProvider: leader.provider,
		CommitSource:      leader.provider,
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			return CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                failingApplier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})
	if err != nil {
		t.Fatalf("NewSingleGroupSubmitter: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := submitter.SubmitCommandEntryV1(ctx, testClusterCreateCollectionEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 801,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if !errors.Is(err, ErrHashicorpRaftLogEntry) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want ErrHashicorpRaftLogEntry", err)
	}
	if result.CommittedRecoverable {
		t.Fatalf("CommittedRecoverable=true after local apply failure")
	}
	if result.Evidence.ProvesProductionConsensus() {
		t.Fatalf("evidence=%+v should not prove the quorum commit after local apply failure", result.Evidence)
	}
	if result.CommittedEntry.Term != 0 || result.CommittedEntry.Index != 0 {
		t.Fatalf("committed entry id=%d/%d, want zero after local apply failure", result.CommittedEntry.Term, result.CommittedEntry.Index)
	}
}

func TestHashicorpRaftProviderDoesNotEnqueueCanceledContext(t *testing.T) {
	applier := &countingClusterApplier{
		result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1},
	}
	cluster := newHashicorpRaftProviderOnlyCluster(t, applier)
	leader := cluster.waitForLeader(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := leader.provider.CommitCommandEntryV1(ctx, CommitCommandEntryV1Request{
		GroupID:                  "group-a",
		NodeID:                   leader.id,
		EntryBytes:               testClusterCreateCollectionEntry(t, 7),
		CurrentCatalogVersion:    7,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      true,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitCommandEntryV1 err=%v want context.Canceled", err)
	}
	if errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("CommitCommandEntryV1 err=%v should not be ErrCommitAmbiguous before Raft.Apply enqueue", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := applier.count(); got != 0 {
		t.Fatalf("applier calls=%d want 0 for canceled context before enqueue", got)
	}
}

func TestHashicorpRaftProviderEvidenceFailsClosedWhenNotProven(t *testing.T) {
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(_ context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			result := productionCommittedResult(req, 4, 2)
			result.Evidence.ProductionConsensus = false
			return result, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})
	_, err := submitter.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), raftentry.RequestMetadataV1{
		RequestID: 901,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if !errors.Is(err, ErrCommitNotProven) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want ErrCommitNotProven", err)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("applier calls=%d want 0", got)
	}
}

func TestHashicorpRaftProviderRejectsFSMWithoutInitialIndexGapSupport(t *testing.T) {
	root := t.TempDir()
	cfg := Config{
		Dir:     filepath.Join(root, "db"),
		NodeID:  "node-a",
		GroupID: "group-a",
		Peers: []Peer{
			{ID: "node-a", Address: "node-a"},
		},
	}
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          cfg.Dir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	defer db.Close()
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:      db,
		Cluster: cfg,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	defer fsm.Close()
	_, transport := hraft.NewInmemTransport(hraft.ServerAddress("node-a"))
	defer transport.Close()

	provider, err := OpenHashicorpRaftProvider(HashicorpRaftProviderOptions{
		Cluster:   cfg,
		Applier:   fsm,
		Transport: transport,
	})
	if provider != nil {
		_ = provider.Close()
		t.Fatalf("OpenHashicorpRaftProvider returned provider with invalid FSM gap support")
	}
	if !errors.Is(err, ErrInvalidHashicorpRaftProvider) {
		t.Fatalf("OpenHashicorpRaftProvider err=%v want ErrInvalidHashicorpRaftProvider", err)
	}
	if !strings.Contains(err.Error(), "initial Raft log index gaps") {
		t.Fatalf("OpenHashicorpRaftProvider err=%v missing initial gap reason", err)
	}
}

type hashicorpRaftTestCluster struct {
	nodes map[NodeID]*hashicorpRaftTestNode
}

type hashicorpRaftTestNode struct {
	id              NodeID
	cfg             Config
	db              *backenddb.DB
	fsm             *raftfsm.FSM
	provider        *HashicorpRaftProvider
	submitter       *SingleGroupSubmitter
	transport       *hraft.InmemTransport
	raftTransport   *countingAppendEntriesTransport
	barrierLogStore *barrierCountingLogStore
}

const hashicorpRaftTestCoordinationTimeout = 5 * time.Second

const (
	// HashiCorp Raft v1.7.3 caps failed ordinary replication backoff at 10.24s.
	// Keep the existing 5s coordination headroom for scheduling and the
	// succeeding AppendEntries round trip, but only for restart catch-up.
	hashicorpRaftTestRestartCatchUpTimeout = 10_240*time.Millisecond + hashicorpRaftTestCoordinationTimeout

	// Reaching the controlled generation is scheduling-sensitive on Windows,
	// but remains a bounded, disconnected setup phase. Give it two catch-up
	// windows; the post-reconnect correctness bound stays unchanged.
	hashicorpRaftTestRestartBackoffSetupTimeout = 2 * hashicorpRaftTestRestartCatchUpTimeout

	// After eleven failed ordinary AppendEntries RPCs, HashiCorp Raft's next
	// retry is in its upstream generation that sleeps for more than five
	// seconds. This is a fixed regression seam, not a reimplementation of
	// HashiCorp Raft backoff arithmetic.
	hashicorpRaftTestRestartBackoffFailureCount = 11
)

// countingAppendEntriesTransport is a test-only controlled fault seam. It
// leaves InmemTransport behavior intact while exposing failed ordinary
// replication to a disconnected peer.
type countingAppendEntriesTransport struct {
	*hraft.InmemTransport

	mu       sync.Mutex
	failures map[hraft.ServerAddress]uint64
}

func newCountingAppendEntriesTransport(transport *hraft.InmemTransport) *countingAppendEntriesTransport {
	return &countingAppendEntriesTransport{
		InmemTransport: transport,
		failures:       make(map[hraft.ServerAddress]uint64),
	}
}

func (t *countingAppendEntriesTransport) AppendEntries(id hraft.ServerID, target hraft.ServerAddress, args *hraft.AppendEntriesRequest, resp *hraft.AppendEntriesResponse) error {
	err := t.InmemTransport.AppendEntries(id, target, args, resp)
	if err != nil && len(args.Entries) != 0 {
		t.mu.Lock()
		t.failures[target]++
		t.mu.Unlock()
	}
	return err
}

func (t *countingAppendEntriesTransport) appendFailures(target hraft.ServerAddress) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failures[target]
}

func newHashicorpRaftDBCluster(tb testing.TB) *hashicorpRaftTestCluster {
	tb.Helper()
	cluster := newHashicorpRaftCluster(tb, func(tb testing.TB, cfg Config) (*backenddb.DB, CommittedCommandApplierV1, CommandEntryPreflightV1) {
		tb.Helper()
		db, fsm := openHashicorpRaftTestDBAndFSM(tb, cfg)
		return db, fsm, fsm
	})
	for _, node := range cluster.nodes {
		node.submitter = newHashicorpRaftDBSubmitter(tb, node, staticCatalogVersion(7))
	}
	return cluster
}

func newHashicorpRaftDBSubmitter(tb testing.TB, node *hashicorpRaftTestNode, catalogProvider CatalogVersionProvider) *SingleGroupSubmitter {
	tb.Helper()
	return newHashicorpRaftDBSubmitterWithPreflight(tb, node, catalogProvider, node.fsm)
}

func newHashicorpRaftDBSubmitterWithPreflight(tb testing.TB, node *hashicorpRaftTestNode, catalogProvider CatalogVersionProvider, preflight CommandEntryPreflightV1) *SingleGroupSubmitter {
	tb.Helper()
	submitter, err := NewSingleGroupSubmitter(SingleGroupSubmitterOptions{
		Cluster:                node.cfg,
		AdmissionProvider:      node.provider,
		CommitSource:           node.provider,
		Preflight:              preflight,
		Applier:                node.fsm,
		CatalogVersionProvider: catalogProvider,
	})
	if err != nil {
		tb.Fatalf("%s NewSingleGroupSubmitter: %v", node.id, err)
	}
	return submitter
}

type gatedCommandEntryPreflight struct {
	delegate CommandEntryPreflightV1
	ready    chan<- struct{}
	release  <-chan struct{}
	once     sync.Once
}

func (p *gatedCommandEntryPreflight) PreflightCommandEntryV1(ctx context.Context, req CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
	result, err := p.delegate.PreflightCommandEntryV1(ctx, req)
	p.once.Do(func() {
		close(p.ready)
	})
	if err != nil {
		return result, err
	}
	select {
	case <-p.release:
		return result, nil
	case <-ctx.Done():
		return CommandEntryPreflightResultV1{}, ctx.Err()
	}
}

func newHashicorpRaftProviderOnlyCluster(tb testing.TB, applier CommittedCommandApplierV1) *hashicorpRaftTestCluster {
	tb.Helper()
	return newHashicorpRaftCluster(tb, func(tb testing.TB, _ Config) (*backenddb.DB, CommittedCommandApplierV1, CommandEntryPreflightV1) {
		tb.Helper()
		return nil, applier, nil
	})
}

func newHashicorpRaftCluster(tb testing.TB, nodeStores func(testing.TB, Config) (*backenddb.DB, CommittedCommandApplierV1, CommandEntryPreflightV1)) *hashicorpRaftTestCluster {
	tb.Helper()
	root := tb.TempDir()
	peers := []Peer{
		{ID: "node-a", Address: "node-a"},
		{ID: "node-b", Address: "node-b"},
		{ID: "node-c", Address: "node-c"},
	}
	transports := make(map[NodeID]*hraft.InmemTransport, len(peers))
	raftTransports := make(map[NodeID]*countingAppendEntriesTransport, len(peers))
	for _, peer := range peers {
		_, transport := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(peer.Address), hashicorpRaftTestCoordinationTimeout)
		transports[peer.ID] = transport
		raftTransports[peer.ID] = newCountingAppendEntriesTransport(transport)
	}
	for _, from := range peers {
		for _, to := range peers {
			transports[from.ID].Connect(hraft.ServerAddress(to.Address), transports[to.ID])
		}
	}
	cluster := &hashicorpRaftTestCluster{nodes: make(map[NodeID]*hashicorpRaftTestNode, len(peers))}
	for _, peer := range peers {
		cfg := Config{
			Dir:     filepath.Join(root, string(peer.ID), "db"),
			NodeID:  peer.ID,
			GroupID: "group-a",
			Peers:   peers,
		}
		db, applier, _ := nodeStores(tb, cfg)
		var applyFailureHandler func(error)
		var logStore hraft.LogStore
		var stableStore hraft.StableStore
		var snapshotStore hraft.SnapshotStore
		var barrierLogStore *barrierCountingLogStore
		if db == nil {
			applyFailureHandler = func(error) {}
			barrierLogStore = newBarrierCountingLogStore()
			logStore = barrierLogStore
			stableStore = hraft.NewInmemStore()
			snapshotStore = hraft.NewInmemSnapshotStore()
		}
		provider, err := OpenHashicorpRaftProvider(HashicorpRaftProviderOptions{
			Cluster:             cfg,
			Applier:             applier,
			Transport:           raftTransports[peer.ID],
			RaftConfig:          hashicorpRaftFastTestConfig(),
			LogStore:            logStore,
			StableStore:         stableStore,
			SnapshotStore:       snapshotStore,
			Bootstrap:           true,
			ApplyTimeout:        2 * time.Second,
			ApplyFailureHandler: applyFailureHandler,
		})
		if err != nil {
			tb.Fatalf("%s OpenHashicorpRaftProvider: %v", peer.ID, err)
		}
		node := &hashicorpRaftTestNode{
			id:              peer.ID,
			cfg:             cfg,
			db:              db,
			provider:        provider,
			transport:       transports[peer.ID],
			raftTransport:   raftTransports[peer.ID],
			barrierLogStore: barrierLogStore,
		}
		if fsm, ok := applier.(*raftfsm.FSM); ok {
			node.fsm = fsm
		}
		cluster.nodes[peer.ID] = node
	}
	// The larger follower timeout would otherwise add a randomized 5-10 second
	// delay to every initial election. TimeoutNow starts the normal HashiCorp
	// election path immediately; waitForLeader still proves live quorum and a
	// stable term before returning a leader to the test.
	electionTarget := peers[0]
	electionSource := peers[1]
	if err := transports[electionSource.ID].TimeoutNow(
		hraft.ServerID(electionTarget.ID),
		hraft.ServerAddress(electionTarget.Address),
		&hraft.TimeoutNowRequest{RPCHeader: hraft.RPCHeader{
			ProtocolVersion: hraft.ProtocolVersionMax,
			ID:              []byte(electionSource.ID),
			Addr:            []byte(electionSource.Address),
		}},
		&hraft.TimeoutNowResponse{},
	); err != nil {
		tb.Fatalf("start initial HashiCorp Raft test election: %v", err)
	}
	tb.Cleanup(func() {
		for _, node := range cluster.nodes {
			_ = node.closeLocal()
			if node.transport != nil {
				_ = node.transport.Close()
			}
		}
	})
	return cluster
}

func openHashicorpRaftTestDBAndFSM(tb testing.TB, cfg Config) (*backenddb.DB, *raftfsm.FSM) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          cfg.Dir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		tb.Fatalf("%s Open DB: %v", cfg.NodeID, err)
	}
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:      db,
		Cluster: cfg,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync:          true,
			AllowInitialIndexGap: true,
		},
	})
	if err != nil {
		_ = db.Close()
		tb.Fatalf("%s Open FSM: %v", cfg.NodeID, err)
	}
	return db, fsm
}

func hashicorpRaftFastTestConfig() *hraft.Config {
	conf := hraft.DefaultConfig()
	// Give these in-memory integration clusters enough coordination headroom to
	// survive multi-second scheduling stalls on the contended Windows shard.
	// HashiCorp Raft uses HeartbeatTimeout to start follower elections and
	// LeaderLeaseTimeout to make a leader step down when it cannot contact a
	// quorum, so retaining the defaults still allowed legitimate leadership
	// churn immediately after the state-based readiness barrier.
	conf.HeartbeatTimeout = hashicorpRaftTestCoordinationTimeout
	conf.ElectionTimeout = hashicorpRaftTestCoordinationTimeout
	conf.LeaderLeaseTimeout = hashicorpRaftTestCoordinationTimeout
	// The readiness barrier below deliberately retains the default one-second
	// observation window. These larger coordination timeouts protect a later
	// scheduler stall without turning every readiness proof into a five-second
	// wait. CommitTimeout, SnapshotInterval, SnapshotThreshold, and TrailingLogs
	// retain the fast test behavior; the output and telemetry overrides only
	// keep tests quiet.
	conf.CommitTimeout = 10 * time.Millisecond
	conf.SnapshotInterval = time.Hour
	conf.SnapshotThreshold = ^uint64(0)
	conf.TrailingLogs = 0
	conf.LogOutput = io.Discard
	conf.LogLevel = "ERROR"
	conf.NoLegacyTelemetry = true
	return conf
}

func TestHashicorpRaftFastTestConfigAddsSchedulingHeadroom(t *testing.T) {
	defaults := hraft.DefaultConfig()
	got := hashicorpRaftFastTestConfig()
	const minCoordinationTimeout = 5 * time.Second
	if got.HeartbeatTimeout < minCoordinationTimeout {
		t.Fatalf("HeartbeatTimeout=%s want at least CI scheduling headroom %s (HashiCorp default %s)", got.HeartbeatTimeout, minCoordinationTimeout, defaults.HeartbeatTimeout)
	}
	if got.ElectionTimeout < minCoordinationTimeout {
		t.Fatalf("ElectionTimeout=%s want at least CI scheduling headroom %s (HashiCorp default %s)", got.ElectionTimeout, minCoordinationTimeout, defaults.ElectionTimeout)
	}
	if got.LeaderLeaseTimeout < minCoordinationTimeout {
		t.Fatalf("LeaderLeaseTimeout=%s want at least CI scheduling headroom %s (HashiCorp default %s)", got.LeaderLeaseTimeout, minCoordinationTimeout, defaults.LeaderLeaseTimeout)
	}
	validated := *got
	validated.LocalID = "test-node"
	if err := hraft.ValidateConfig(&validated); err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	wantSettlingWindow := hashicorpRaftLeaderSettlingWindow(defaults)
	if gotSettlingWindow := hashicorpRaftTestLeaderSettlingWindow(); gotSettlingWindow != wantSettlingWindow {
		t.Fatalf("readiness settling window=%s want HashiCorp default coordination window %s", gotSettlingWindow, wantSettlingWindow)
	}
}

// waitForLeader requires continuous live-quorum observations of one leader and
// term across HashiCorp Raft's default coordination window. A single Leader
// admission observation, or two observations one poll apart, can be sampled
// while the election/current-term no-op is still settling. The test cluster's
// larger timeouts protect later scheduling stalls rather than extending this
// readiness dwell. This is a test-harness barrier only; it deliberately does
// not retry the operation under test.
func (c *hashicorpRaftTestCluster) waitForLeader(tb testing.TB) *hashicorpRaftTestNode {
	tb.Helper()
	// Opening three durable test nodes can consume most of the old 15-second
	// budget on a contended Windows runner. Keep failure bounded, but leave
	// enough time for an election before requiring the state-based quorum/term
	// barrier below.
	deadline := time.Now().Add(30 * time.Second)
	readinessCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	// The live-quorum proof only needs to show that a term remained settled
	// across HashiCorp's normal coordination window. The test cluster's larger
	// timeouts are later scheduling headroom, not additional readiness evidence.
	settlingWindow := hashicorpRaftTestLeaderSettlingWindow()
	var previous leaderReadinessObservation
	var lastErr error
	for readinessCtx.Err() == nil {
		leader := c.waitForSingleLeader(tb)
		if leader == nil {
			previous = leaderReadinessObservation{}
			waitForLeaderReadinessPoll(readinessCtx)
			continue
		}
		term, err := HashicorpRaftTestLeaderReady(readinessCtx, leader.provider)
		if err != nil {
			lastErr = err
			previous = leaderReadinessObservation{}
			waitForLeaderReadinessPoll(readinessCtx)
			continue
		}
		current := leaderReadinessObservation{nodeID: leader.id, term: term, observedAt: time.Now(), valid: true}
		if sameStableLeaderTerm(previous, current, settlingWindow) {
			return leader
		}
		if !sameLeaderTerm(previous, current) {
			previous = current
		}
		waitForLeaderReadinessPoll(readinessCtx)
	}
	stableFor := time.Duration(0)
	if previous.valid {
		stableFor = time.Since(previous.observedAt)
	}
	tb.Fatalf("timed out waiting for leader-ready stable term; last_readiness_err=%v last_stable_leader=%s last_stable_term=%d stable_for=%s required=%s states=%s", lastErr, previous.nodeID, previous.term, stableFor, settlingWindow, c.admissionSummary())
	return nil
}

func waitForLeaderReadinessPoll(ctx context.Context) {
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
	}
}

type leaderReadinessObservation struct {
	nodeID     NodeID
	term       uint64
	observedAt time.Time
	valid      bool
}

func sameLeaderTerm(previous, current leaderReadinessObservation) bool {
	return previous.valid && current.valid && previous.nodeID == current.nodeID && previous.nodeID != "" && previous.term == current.term && current.term != 0
}

func sameStableLeaderTerm(previous, current leaderReadinessObservation, settlingWindow time.Duration) bool {
	return settlingWindow > 0 && sameLeaderTerm(previous, current) && current.observedAt.Sub(previous.observedAt) >= settlingWindow
}

func hashicorpRaftLeaderSettlingWindow(conf *hraft.Config) time.Duration {
	if conf == nil {
		return 0
	}
	window := conf.LeaderLeaseTimeout
	if conf.HeartbeatTimeout > window {
		window = conf.HeartbeatTimeout
	}
	if conf.ElectionTimeout > window {
		window = conf.ElectionTimeout
	}
	return window
}

func hashicorpRaftTestLeaderSettlingWindow() time.Duration {
	return hashicorpRaftLeaderSettlingWindow(hraft.DefaultConfig())
}

func (c *hashicorpRaftTestCluster) waitForSingleLeader(tb testing.TB) *hashicorpRaftTestNode {
	tb.Helper()
	var leader *hashicorpRaftTestNode
	for _, node := range c.nodes {
		if node.provider == nil {
			continue
		}
		status, err := node.provider.ClusterAdmissionStatus(context.Background())
		if err != nil {
			tb.Fatalf("%s ClusterAdmissionStatus: %v", node.id, err)
		}
		if !status.Leader {
			continue
		}
		if leader != nil {
			tb.Fatalf("multiple leaders: %s and %s", leader.id, node.id)
		}
		leader = node
	}
	return leader
}

func TestHashicorpRaftLeaderReadinessRequiresASettledTermWindow(t *testing.T) {
	startedAt := time.Unix(1_000, 0)
	settlingWindow := hashicorpRaftTestLeaderSettlingWindow()
	ready := leaderReadinessObservation{nodeID: "node-a", term: 4, observedAt: startedAt, valid: true}
	if sameStableLeaderTerm(leaderReadinessObservation{}, ready, settlingWindow) {
		t.Fatal("a single leader/proof observation must not satisfy the readiness barrier")
	}
	if sameStableLeaderTerm(ready, leaderReadinessObservation{nodeID: "node-a", term: 4, observedAt: startedAt.Add(20 * time.Millisecond), valid: true}, settlingWindow) {
		t.Fatal("two observations inside the Raft timing window must not satisfy the readiness barrier")
	}
	if sameStableLeaderTerm(ready, leaderReadinessObservation{nodeID: "node-a", term: 5, observedAt: startedAt.Add(settlingWindow), valid: true}, settlingWindow) {
		t.Fatal("a term transition must reset the readiness barrier")
	}
	if sameStableLeaderTerm(ready, leaderReadinessObservation{nodeID: "node-b", term: 4, observedAt: startedAt.Add(settlingWindow), valid: true}, settlingWindow) {
		t.Fatal("a leader transition must reset the readiness barrier")
	}
	if sameStableLeaderTerm(ready, leaderReadinessObservation{nodeID: "node-a", term: 4, observedAt: startedAt.Add(settlingWindow)}, settlingWindow) {
		t.Fatal("an invalid observation must not satisfy the readiness barrier")
	}
	if !sameStableLeaderTerm(ready, leaderReadinessObservation{nodeID: "node-a", term: 4, observedAt: startedAt.Add(settlingWindow), valid: true}, settlingWindow) {
		t.Fatal("a continuously verified leader term spanning the Raft timing window must satisfy the readiness barrier")
	}
}

func (c *hashicorpRaftTestCluster) firstFollower(tb testing.TB, leaderID NodeID) *hashicorpRaftTestNode {
	tb.Helper()
	for _, node := range c.nodes {
		if node.id != leaderID && node.provider != nil {
			return node
		}
	}
	tb.Fatalf("no follower found for leader %q", leaderID)
	return nil
}

func (c *hashicorpRaftTestCluster) firstFollowerExcept(tb testing.TB, leaderID, excludedID NodeID) *hashicorpRaftTestNode {
	tb.Helper()
	for _, node := range c.nodes {
		if node.id != leaderID && node.id != excludedID {
			return node
		}
	}
	tb.Fatalf("no follower found for leader %q excluding %q", leaderID, excludedID)
	return nil
}

func (c *hashicorpRaftTestCluster) waitFollowerHint(tb testing.TB, node *hashicorpRaftTestNode, leaderID NodeID) {
	tb.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := node.provider.ClusterAdmissionStatus(context.Background())
		if err != nil {
			tb.Fatalf("%s ClusterAdmissionStatus: %v", node.id, err)
		}
		if !status.Leader && status.LeaderHint == leaderID {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("%s did not report leader hint %q; states=%s", node.id, leaderID, c.admissionSummary())
}

func (c *hashicorpRaftTestCluster) waitApplied(tb testing.TB, id raftentry.ApplyEntryID) {
	tb.Helper()
	c.waitAppliedOn(tb, id, c.allNodeIDs()...)
}

func (c *hashicorpRaftTestCluster) waitAppliedOn(tb testing.TB, id raftentry.ApplyEntryID, nodeIDs ...NodeID) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, nodeID := range nodeIDs {
			node := c.nodes[nodeID]
			if node == nil || node.fsm == nil {
				allApplied = false
				break
			}
			got, ok := node.fsm.LastApplied()
			if !ok || got.Index < id.Index || got.Term != id.Term {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var states []string
	for _, nodeID := range nodeIDs {
		node := c.nodes[nodeID]
		if node == nil || node.fsm == nil {
			states = append(states, fmt.Sprintf("%s=stopped", nodeID))
			continue
		}
		got, ok := node.fsm.LastApplied()
		states = append(states, fmt.Sprintf("%s=%d/%d ok=%t", node.id, got.Term, got.Index, ok))
	}
	tb.Fatalf("timed out waiting for apply %d/%d; applied=%s", id.Term, id.Index, strings.Join(states, ", "))
}

func (c *hashicorpRaftTestCluster) waitRestartCatchUp(tb testing.TB, restartedID NodeID, id raftentry.ApplyEntryID) {
	tb.Helper()
	nodeIDs := c.allNodeIDs()
	started := time.Now()
	deadline := started.Add(hashicorpRaftTestRestartCatchUpTimeout)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, nodeID := range nodeIDs {
			node := c.nodes[nodeID]
			if node == nil || node.fsm == nil {
				allApplied = false
				break
			}
			got, ok := node.fsm.LastApplied()
			if !ok || got.Index < id.Index || got.Term != id.Term {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("restart catch-up timed out: phase=post-restart elapsed=%s restarted_id=%q target=%d/%d configured_peers=%s nodes=%s", time.Since(started), restartedID, id.Term, id.Index, c.configuredPeers(restartedID), c.restartCatchUpFrontier())
}

func (c *hashicorpRaftTestCluster) waitRestartBackoff(tb testing.TB, leaderID, restartedID NodeID, id raftentry.ApplyEntryID) {
	tb.Helper()
	leader := c.nodes[leaderID]
	transport := leader.raftTransport
	restarted := c.nodes[restartedID]
	if restarted == nil {
		tb.Fatalf("restart backoff seam missing restarted node %q", restartedID)
	}
	var target hraft.ServerAddress
	for _, peer := range restarted.cfg.Peers {
		if peer.ID == restartedID {
			target = hraft.ServerAddress(peer.Address)
			break
		}
	}
	if target == "" {
		tb.Fatalf("restart backoff seam missing configured peer %q", restartedID)
	}
	started := time.Now()
	deadline := started.Add(hashicorpRaftTestRestartBackoffSetupTimeout)
	for time.Now().Before(deadline) {
		if transport.appendFailures(target) >= hashicorpRaftTestRestartBackoffFailureCount {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("restart backoff seam timed out: phase=pre-reconnect-backoff elapsed=%s leader_id=%q restarted_id=%q target=%d/%d ordinary_append_failures=%d want_at_least=%d configured_peers=%s nodes=%s", time.Since(started), leaderID, restartedID, id.Term, id.Index, transport.appendFailures(target), hashicorpRaftTestRestartBackoffFailureCount, c.configuredPeers(restartedID), c.restartCatchUpFrontier())
}

func (c *hashicorpRaftTestCluster) configuredPeers(nodeID NodeID) string {
	node := c.nodes[nodeID]
	if node == nil {
		return "<missing restarted node>"
	}
	peers := make([]string, 0, len(node.cfg.Peers))
	for _, peer := range node.cfg.Peers {
		peers = append(peers, fmt.Sprintf("%s@%s", peer.ID, peer.Address))
	}
	return strings.Join(peers, ",")
}

func (c *hashicorpRaftTestCluster) restartCatchUpFrontier() string {
	ids := c.allNodeIDs()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	states := make([]string, 0, len(ids))
	for _, nodeID := range ids {
		node := c.nodes[nodeID]
		if node == nil || node.provider == nil {
			states = append(states, fmt.Sprintf("%s=stopped", nodeID))
			continue
		}
		frontier, err := HashicorpRaftTestFrontier(node.provider)
		if err != nil {
			states = append(states, fmt.Sprintf("%s raft_error=%v", nodeID, err))
			continue
		}
		fsmApplied, fsmAppliedOK := raftentry.ApplyEntryID{}, false
		if node.fsm != nil {
			fsmApplied, fsmAppliedOK = node.fsm.LastApplied()
		}
		admission, admissionErr := node.provider.ClusterAdmissionStatus(context.Background())
		if admissionErr != nil {
			states = append(states, fmt.Sprintf("%s raft_state=%s term=%d leader_id=%q last_log=%d commit=%d applied=%d fsm_applied=%d/%d fsm_applied_ok=%t admission_error=%v", nodeID, frontier.State, frontier.Term, frontier.LeaderID, frontier.LastLog, frontier.Commit, frontier.Applied, fsmApplied.Term, fsmApplied.Index, fsmAppliedOK, admissionErr))
			continue
		}
		states = append(states, fmt.Sprintf("%s raft_state=%s term=%d leader_id=%q last_log=%d commit=%d applied=%d fsm_applied=%d/%d fsm_applied_ok=%t admission={leader=%t unavailable=%t hint=%q reason=%q}", nodeID, frontier.State, frontier.Term, frontier.LeaderID, frontier.LastLog, frontier.Commit, frontier.Applied, fsmApplied.Term, fsmApplied.Index, fsmAppliedOK, admission.Leader, admission.Unavailable, admission.LeaderHint, admission.Reason))
	}
	return strings.Join(states, "; ")
}

func (c *hashicorpRaftTestCluster) admissionSummary() string {
	var states []string
	for _, node := range c.nodes {
		if node.provider == nil {
			states = append(states, fmt.Sprintf("%s stopped", node.id))
			continue
		}
		status, err := node.provider.ClusterAdmissionStatus(context.Background())
		if err != nil {
			states = append(states, fmt.Sprintf("%s err=%v", node.id, err))
			continue
		}
		states = append(states, fmt.Sprintf("%s leader=%t unavailable=%t hint=%q reason=%q", node.id, status.Leader, status.Unavailable, status.LeaderHint, status.Reason))
	}
	return strings.Join(states, "; ")
}

func (c *hashicorpRaftTestCluster) allNodeIDs() []NodeID {
	ids := make([]NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (c *hashicorpRaftTestCluster) runningNodeIDs() []NodeID {
	ids := make([]NodeID, 0, len(c.nodes))
	for id, node := range c.nodes {
		if node.provider != nil && node.fsm != nil {
			ids = append(ids, id)
		}
	}
	return ids
}

func (c *hashicorpRaftTestCluster) shutdownNode(t *testing.T, id NodeID) {
	t.Helper()
	node := c.nodes[id]
	if node == nil {
		t.Fatalf("node %s not found", id)
	}
	c.disconnectNode(t, id)
	if err := node.closeLocal(); err != nil {
		t.Fatalf("%s close local state: %v", id, err)
	}
}

func (c *hashicorpRaftTestCluster) restartDBNode(t *testing.T, id NodeID) *hashicorpRaftTestNode {
	t.Helper()
	node := c.nodes[id]
	if node == nil {
		t.Fatalf("node %s not found", id)
	}
	db, fsm := openHashicorpRaftTestDBAndFSM(t, node.cfg)
	provider, err := OpenHashicorpRaftProvider(HashicorpRaftProviderOptions{
		Cluster:      node.cfg,
		Applier:      fsm,
		Transport:    node.raftTransport,
		RaftConfig:   hashicorpRaftFastTestConfig(),
		Bootstrap:    true,
		ApplyTimeout: 2 * time.Second,
	})
	if err != nil {
		_ = fsm.Close()
		_ = db.Close()
		t.Fatalf("%s restart OpenHashicorpRaftProvider: %v", id, err)
	}
	submitter, err := NewSingleGroupSubmitter(SingleGroupSubmitterOptions{
		Cluster:                node.cfg,
		AdmissionProvider:      provider,
		CommitSource:           provider,
		Preflight:              fsm,
		Applier:                fsm,
		CatalogVersionProvider: staticCatalogVersion(7),
	})
	if err != nil {
		_ = provider.Close()
		_ = fsm.Close()
		_ = db.Close()
		t.Fatalf("%s restart NewSingleGroupSubmitter: %v", id, err)
	}
	node.db = db
	node.fsm = fsm
	node.provider = provider
	node.submitter = submitter
	return node
}

func (c *hashicorpRaftTestCluster) disconnectNode(t *testing.T, id NodeID) {
	t.Helper()
	target := c.nodes[id]
	if target == nil || target.transport == nil {
		return
	}
	target.transport.DisconnectAll()
	targetAddr := hraft.ServerAddress(peerAddress(t, target.cfg, id))
	for _, node := range c.nodes {
		if node.id == id || node.transport == nil {
			continue
		}
		node.transport.Disconnect(targetAddr)
	}
}

func (c *hashicorpRaftTestCluster) connectAllTransports(t *testing.T) {
	t.Helper()
	for _, from := range c.nodes {
		if from.transport == nil {
			continue
		}
		for _, to := range c.nodes {
			if from.id == to.id || to.transport == nil {
				continue
			}
			from.transport.Connect(hraft.ServerAddress(peerAddress(t, to.cfg, to.id)), to.transport)
		}
	}
}

func (c *hashicorpRaftTestCluster) assertLogicalDigestsEqual(t *testing.T) raftapply.LogicalDigestV1 {
	t.Helper()
	var want raftapply.LogicalDigestV1
	first := true
	for _, node := range c.nodes {
		if node.fsm == nil {
			t.Fatalf("%s has no FSM for digest comparison", node.id)
		}
		digest, err := node.fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
		if err != nil {
			t.Fatalf("%s LogicalDigestV1: %v", node.id, err)
		}
		if first {
			want = digest
			first = false
			continue
		}
		if digest != want {
			t.Fatalf("logical digest mismatch: %s=%s want %s", node.id, digest.Hex(), want.Hex())
		}
	}
	return want
}

func (n *hashicorpRaftTestNode) closeLocal() error {
	if n == nil {
		return nil
	}
	var err error
	if n.provider != nil {
		err = errors.Join(err, n.provider.Close())
		n.provider = nil
	}
	if n.fsm != nil {
		err = errors.Join(err, n.fsm.Close())
		n.fsm = nil
	}
	if n.db != nil {
		err = errors.Join(err, n.db.Close())
		n.db = nil
	}
	n.submitter = nil
	return err
}

func peerAddress(t *testing.T, cfg Config, id NodeID) string {
	t.Helper()
	for _, peer := range cfg.Peers {
		if peer.ID == id {
			return peer.Address
		}
	}
	t.Fatalf("peer address for node %s not found in config", id)
	return ""
}

func assertHashicorpRaftStoreFiles(t *testing.T, node *hashicorpRaftTestNode, label string) {
	t.Helper()
	resolved, err := Validate(node.cfg)
	if err != nil {
		t.Fatalf("%s resolve raftcluster layout: %v", label, err)
	}
	for _, path := range []string{
		filepath.Join(resolved.Layout.LogDir, "raft-log.bolt"),
		filepath.Join(resolved.Layout.StableDir, "raft-stable.bolt"),
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("%s %s stat: %v", label, path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s %s is empty", label, path)
		}
	}
}

func assertClusterCollectionExists(t *testing.T, node *hashicorpRaftTestNode, collection string) {
	t.Helper()
	if node == nil || node.db == nil {
		t.Fatalf("%s has no open DB", collection)
	}
	if _, err := collections.NewCollectionManager(node.db).OpenCollection(collection); err != nil {
		t.Fatalf("%s OpenCollection %s: %v", node.id, collection, err)
	}
}

func assertClusterCollectionMissing(t *testing.T, node *hashicorpRaftTestNode, collection string) {
	t.Helper()
	if node == nil || node.db == nil {
		t.Fatalf("missing node/open DB while checking collection %s is absent", collection)
	}
	if _, err := collections.NewCollectionManager(node.db).OpenCollection(collection); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("%s OpenCollection %s err=%v, want ErrCollectionNotFound", node.id, collection, err)
	}
}

func assertClusterLastApplied(t *testing.T, node *hashicorpRaftTestNode, want raftentry.ApplyEntryID) {
	t.Helper()
	if node == nil || node.fsm == nil {
		t.Fatalf("missing node/FSM while checking last applied %d/%d", want.Term, want.Index)
	}
	got, ok := node.fsm.LastApplied()
	if !ok || got != want {
		t.Fatalf("%s last applied=%d/%d ok=%t want %d/%d", node.id, got.Term, got.Index, ok, want.Term, want.Index)
	}
}

func staticCatalogVersion(version uint64) CatalogVersionProvider {
	return CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) {
		return version, true, nil
	})
}

func collectionCountCatalogVersion(node *hashicorpRaftTestNode, base uint64) CatalogVersionProvider {
	return CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) {
		if node == nil || node.db == nil {
			return 0, false, fmt.Errorf("missing DB for catalog version provider")
		}
		metas, err := collections.NewCollectionManager(node.db).ListCollections()
		if err != nil {
			return 0, false, err
		}
		return base + uint64(len(metas)), true, nil
	})
}

func newTestSingleGroupSubmitter(tb testing.TB, opts SingleGroupSubmitterOptions) *SingleGroupSubmitter {
	tb.Helper()
	root := tb.TempDir()
	opts.Cluster = Config{
		Dir:     filepath.Join(root, "db"),
		NodeID:  "node-a",
		GroupID: "group-a",
		Peers: []Peer{
			{ID: "node-a", Address: "127.0.0.1:7000"},
			{ID: "node-b", Address: "127.0.0.1:7001"},
			{ID: "node-c", Address: "127.0.0.1:7002"},
		},
	}
	if opts.Preflight == nil {
		opts.Preflight = CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			return CommandEntryPreflightResultV1{}, nil
		})
	}
	submitter, err := NewSingleGroupSubmitter(opts)
	if err != nil {
		tb.Fatalf("NewSingleGroupSubmitter: %v", err)
	}
	return submitter
}

func productionCommittedResult(req CommitCommandEntryV1Request, term, index uint64) CommitCommandEntryV1Result {
	expectedTarget := cloneExpectedTargetForTest(req.ExpectedTarget)
	entry := CommittedCommandEntryV1{
		Term:                     term,
		Index:                    index,
		Bytes:                    bytes.Clone(req.EntryBytes),
		CurrentCatalogVersion:    req.CurrentCatalogVersion,
		HasCurrentCatalogVersion: req.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      req.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataForTest(req.RequestMetadata),
		ExpectedTarget:           expectedTarget,
	}
	return CommitCommandEntryV1Result{
		Entry: entry,
		Evidence: CommitEvidenceV1{
			Kind:                CommitEvidenceProductionConsensusV1,
			GroupID:             req.GroupID,
			NodeID:              req.NodeID,
			LeaderID:            req.NodeID,
			Term:                term,
			Index:               index,
			Committed:           true,
			ProductionConsensus: true,
		},
	}
}

type recordingClusterApplier struct {
	entries []CommittedCommandEntryV1
	result  raftentry.ApplyResultV1
}

func (a *recordingClusterApplier) ApplyCommittedCommandEntryV1(_ context.Context, entry CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	a.entries = append(a.entries, entry.Clone())
	return a.result, nil
}

func (a *recordingClusterApplier) snapshot() []CommittedCommandEntryV1 {
	return append([]CommittedCommandEntryV1(nil), a.entries...)
}

type progressReportingApplier struct {
	mu           sync.Mutex
	progress     AppliedProgress
	beforeReport func()
}

func (a *progressReportingApplier) ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}, nil
}

func (a *progressReportingApplier) AppliedProgress(ctx context.Context) (AppliedProgress, error) {
	progress, beforeReport := a.snapshot()
	if err := ctx.Err(); err != nil {
		return progress, err
	}
	if beforeReport != nil {
		beforeReport()
	}
	progress, _ = a.snapshot()
	return progress, nil
}

func (a *progressReportingApplier) setProgress(progress AppliedProgress) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.progress = progress
}

func (a *progressReportingApplier) setProgressNodeID(id NodeID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.progress.NodeID = id
}

func (a *progressReportingApplier) progressIndex() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.progress.Index
}

func (a *progressReportingApplier) setBeforeReport(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.beforeReport = fn
}

func (a *progressReportingApplier) snapshot() (AppliedProgress, func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.progress, a.beforeReport
}

type barrierCountingLogStore struct {
	*hraft.InmemStore
	mu          sync.Mutex
	barrierLogs int
}

func newBarrierCountingLogStore() *barrierCountingLogStore {
	return &barrierCountingLogStore{InmemStore: hraft.NewInmemStore()}
}

func (s *barrierCountingLogStore) StoreLog(log *hraft.Log) error {
	return s.StoreLogs([]*hraft.Log{log})
}

func (s *barrierCountingLogStore) StoreLogs(logs []*hraft.Log) error {
	var barriers int
	for _, log := range logs {
		if log != nil && log.Type == hraft.LogBarrier {
			barriers++
		}
	}
	if err := s.InmemStore.StoreLogs(logs); err != nil {
		return err
	}
	if barriers > 0 {
		s.mu.Lock()
		s.barrierLogs += barriers
		s.mu.Unlock()
	}
	return nil
}

func (s *barrierCountingLogStore) barrierLogCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.barrierLogs
}

type countingClusterApplier struct {
	mu     sync.Mutex
	result raftentry.ApplyResultV1
	counts int
}

func (a *countingClusterApplier) ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts++
	return a.result, nil
}

func (a *countingClusterApplier) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.counts
}

func testClusterCommandEntry(tb testing.TB, catalogVersion uint64) []byte {
	tb.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("raftcluster/insert/u1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "users"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada"}`))},
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		tb.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		tb.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func testClusterCreateCollectionEntry(tb testing.TB, catalogVersion uint64) []byte {
	tb.Helper()
	return testClusterCreateCollectionEntryNamed(tb, "users", "raftcluster/create/users", catalogVersion)
}

func testClusterCreateCollectionEntryNamed(tb testing.TB, collection, idempotency string, catalogVersion uint64) []byte {
	tb.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: iwire.SectionCollectionMeta, Bytes: testClusterCollectionMetaPayload(collection)},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		tb.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		tb.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func testClusterCollectionMetaPayload(collection string) []byte {
	dst := binary.AppendUvarint(nil, 1)
	dst = appendTestString(dst, collection)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = append(dst, 0)
	dst = append(dst, 0)
	dst = append(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = append(dst, 0)
	dst = append(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	return dst
}

func appendTestString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func cloneRequestMetadataForTest(meta raftentry.RequestMetadataV1) raftentry.RequestMetadataV1 {
	meta.TraceContext = bytes.Clone(meta.TraceContext)
	meta.ClusterRouteMembers = append([]string(nil), meta.ClusterRouteMembers...)
	return meta
}

func cloneExpectedTargetForTest(target *raftentry.TargetIdentityV1) *raftentry.TargetIdentityV1 {
	if target == nil {
		return nil
	}
	cloned := target.Clone()
	return &cloned
}
