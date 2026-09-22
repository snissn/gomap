package raftcluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestThreeNodeHarnessProgressApplierAppliesAndWaits(t *testing.T) {
	applier := newThreeNodeHarnessProgressApplier("node-a", "group-a")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := applier.ApplyCommittedCommandEntryV1(ctx, CommittedCommandEntryV1{Term: 2, Index: 7, Bytes: []byte("entry")})
	if err != nil || result.Status != raftentry.ApplyStatusApplied {
		t.Fatalf("apply result=%+v err=%v", result, err)
	}
	progress, err := applier.WaitAppliedIndex(ctx, AppliedIndexReadBarrier{NodeID: "node-a", GroupID: "group-a", MinAppliedIndex: 7})
	if err != nil || progress.Term != 2 || progress.Index != 7 || !progress.HasApplied {
		t.Fatalf("wait progress=%+v err=%v", progress, err)
	}
	result, err = applier.ApplyCommittedCommandEntryV1(ctx, CommittedCommandEntryV1{Term: 2, Index: 7, Bytes: []byte("entry")})
	if err != nil || result.Status != raftentry.ApplyStatusAlreadyApplied {
		t.Fatalf("repeat result=%+v err=%v", result, err)
	}
	_, err = applier.WaitAppliedIndex(ctx, AppliedIndexReadBarrier{NodeID: "node-b", GroupID: "group-a"})
	if !errors.Is(err, ErrReadBarrierTargetMismatch) {
		t.Fatalf("identity mismatch err=%v", err)
	}
}

func TestThreeNodeHarnessTwoIndependentGroupsProveReadIndexAndApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	left, err := OpenThreeNodeHarness(ctx, "group-left")
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := OpenThreeNodeHarness(ctx, "group-right")
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if left.GroupID() == right.GroupID() || left.LeaderID() == "" || right.LeaderID() == "" {
		t.Fatalf("independent topology is incomplete: left=%q/%q right=%q/%q", left.GroupID(), left.LeaderID(), right.GroupID(), right.LeaderID())
	}
	entry := threeNodeHarnessProofEntry(t)
	for _, harness := range []*ThreeNodeHarness{left, right} {
		commit, err := harness.CommitAndProve(ctx, entry)
		if err != nil || !commit.Evidence.ProvesProductionConsensus() || commit.Entry.Index == 0 {
			t.Fatalf("commit group=%q result=%+v err=%v", harness.GroupID(), commit, err)
		}
		proof, progress, err := harness.ReadCoordinator().CoordinateRoutedReadIndex(ctx, ReadIndexBarrier{NodeID: harness.LeaderID(), GroupID: harness.GroupID()})
		if err != nil || proof.EvidenceKind != ReadIndexEvidenceProduction || !proof.HasQuorum || progress.Index < proof.Index || progress.GroupID != harness.GroupID() {
			t.Fatalf("read proof group=%q proof=%+v progress=%+v err=%v", harness.GroupID(), proof, progress, err)
		}
	}
}

func TestThreeNodeHarnessPreferredLeadersRotateAcrossGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	preferred := []NodeID{"node-a", "node-b", "node-c", "node-a"}
	leaders := map[NodeID]bool{}
	entry := threeNodeHarnessProofEntry(t)
	for i, want := range preferred {
		harness, err := OpenThreeNodeHarnessWithOptions(ctx, GroupID(fmt.Sprintf("rotated-group-%d", i)), ThreeNodeHarnessOptions{PreferredLeader: want})
		if err != nil {
			t.Fatalf("open group %d: %v", i, err)
		}
		if got := harness.LeaderID(); got != want {
			_ = harness.Close()
			t.Fatalf("group %d leader=%q want %q", i, got, want)
		}
		commit, commitErr := harness.CommitAndProve(ctx, entry)
		closeErr := harness.Close()
		if commitErr != nil || closeErr != nil || !commit.Evidence.ProvesProductionConsensus() {
			t.Fatalf("group %d commit=%+v err=%v close=%v", i, commit, commitErr, closeErr)
		}
		leaders[want] = true
	}
	if len(leaders) != 3 {
		t.Fatalf("leaders=%v want all fixed members", leaders)
	}
}

func TestThreeNodeHarnessReadCoordinatorRestoresPreferredLeaderV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	harness, err := OpenThreeNodeHarnessWithOptions(ctx, "restored-leader", ThreeNodeHarnessOptions{PreferredLeader: "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	if _, err := harness.CommitBenchmarkProofV1(ctx); err != nil {
		t.Fatal(err)
	}
	var source *threeNodeHarnessNode
	for _, node := range harness.nodes {
		if node.id == "node-a" {
			source = node
			break
		}
	}
	if source == nil {
		t.Fatal("preferred leader transport is absent")
	}
	if err := source.transport.TimeoutNow(hraft.ServerID("node-b"), hraft.ServerAddress("node-b"), &hraft.TimeoutNowRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax, ID: []byte(source.id), Addr: []byte(source.id)}}, &hraft.TimeoutNowResponse{}); err != nil {
		t.Fatal(err)
	}
	changed, err := harness.waitLeader(ctx)
	if err != nil || changed.id != "node-b" {
		t.Fatalf("changed leader=%v err=%v", changed, err)
	}
	proof, progress, err := harness.ReadCoordinator().CoordinateRoutedReadIndex(ctx, ReadIndexBarrier{NodeID: "node-a", GroupID: harness.GroupID()})
	if err != nil || proof.NodeID != "node-a" || progress.NodeID != "node-a" || harness.LeaderID() != "node-a" {
		t.Fatalf("restored read proof=%+v progress=%+v leader=%q err=%v", proof, progress, harness.LeaderID(), err)
	}
}

func TestThreeNodeHarnessPreferredLeaderRejectsNonMember(t *testing.T) {
	if _, err := OpenThreeNodeHarnessWithOptions(context.Background(), "invalid-preferred", ThreeNodeHarnessOptions{PreferredLeader: "node-z"}); err == nil {
		t.Fatal("accepted non-member preferred leader")
	}
}

func threeNodeHarnessProofEntry(t *testing.T) []byte {
	t.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("three-node-harness-proof-v1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 1)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "harness_proof"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("proof"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"proof":true}`))},
	}
	command, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
