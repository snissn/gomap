package raftharness

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	testCatalogVersionStart        = 7
	deterministicCollectionRefName = 1
)

func TestInjectedCommittedSequenceConvergesLeaderFollowerDigest(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 11)
	evidence, err := h.CommitAndApply([]raftcluster.NodeID{"node-a", "node-b"}, entries...)
	if err != nil {
		t.Fatalf("CommitAndApply: %v evidence=%+v", err, evidence)
	}
	assertInjectedCommittedEvidence(t, evidence)
	for nodeID, results := range evidence.Applied {
		assertAppliedResults(t, nodeID, results, []int64{1, 2, 1, 1})
	}
	assertLastApplied(t, h, "node-a", raftentry.ApplyEntryID{Term: 11, Index: 4})
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 11, Index: 4})

	leaderDigest := logicalDigest(t, h, "node-a")
	followerDigest := logicalDigest(t, h, "node-b")
	if leaderDigest != followerDigest {
		t.Fatalf("leader/follower digest mismatch leader=%s follower=%s", leaderDigest.Hex(), followerDigest.Hex())
	}
}

func TestCloseReopenPreservesProgressReplayAndIdempotency(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "harness:create:users:reopen")
	entry1 := committedCommand(12, 1, raw)
	evidence, err := h.CommitAndApply([]raftcluster.NodeID{"node-a"}, entry1)
	if err != nil {
		t.Fatalf("CommitAndApply entry1: %v evidence=%+v", err, evidence)
	}
	first := evidence.Applied["node-a"][0]
	assertAppliedResult(t, first, raftentry.ApplyStatusApplied, 1)
	if err := h.CloseNode("node-a"); err != nil {
		t.Fatalf("CloseNode: %v", err)
	}
	if _, err := h.ReopenNode("node-a"); err != nil {
		t.Fatalf("ReopenNode: %v", err)
	}
	assertLastApplied(t, h, "node-a", raftentry.ApplyEntryID{Term: 12, Index: 1})

	replayed, err := h.ApplyCommittedEntriesToNode("node-a", entry1)
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesToNode replay: %v results=%+v", err, replayed)
	}
	if len(replayed) != 1 || replayed[0] != first {
		t.Fatalf("replayed=%+v, want stored first result %+v", replayed, first)
	}

	entry2 := committedCommand(12, 2, raw)
	evidence, err = h.CommitAndApply([]raftcluster.NodeID{"node-a"}, entry2)
	if err != nil {
		t.Fatalf("CommitAndApply duplicate idempotency: %v evidence=%+v", err, evidence)
	}
	duplicate := evidence.Applied["node-a"][0]
	assertAppliedResult(t, duplicate, raftentry.ApplyStatusAlreadyApplied, 0)
	if duplicate.CommandDigest != first.CommandDigest || duplicate.ResultDigest != first.ResultDigest {
		t.Fatalf("duplicate result=%+v, want original digest/result %+v", duplicate, first)
	}
	assertLastApplied(t, h, "node-a", raftentry.ApplyEntryID{Term: 12, Index: 2})

	if _, err := h.ReopenNode("node-a"); err != nil {
		t.Fatalf("ReopenNode after duplicate: %v", err)
	}
	assertLastApplied(t, h, "node-a", raftentry.ApplyEntryID{Term: 12, Index: 2})
	replayedDuplicate, err := h.ApplyCommittedEntriesToNode("node-a", entry2)
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesToNode duplicate replay: %v results=%+v", err, replayedDuplicate)
	}
	if len(replayedDuplicate) != 1 || replayedDuplicate[0] != duplicate {
		t.Fatalf("replayed duplicate=%+v, want stored duplicate %+v", replayedDuplicate, duplicate)
	}
}

func TestFollowerCatchUpAppliesMissingSparseEntriesAndRejectsInvalidSequences(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(13, 1, deterministicCreateCollectionEntryWithOptions(t, "users", "harness:create:users:catchup", testCreateCollectionMetaOptions{
			documentFormat: uint64(nativewire.DocumentFormatBSON),
		})),
		committedCommand(13, 2, deterministicInsertBatchEntry(t, "users", "harness:insert:users:catchup", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{
			testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}}),
		})),
		committedCommand(14, 4, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:catchup")),
	}
	if evidence, err := h.CommitAndApply([]raftcluster.NodeID{"node-a"}, entries...); err != nil {
		t.Fatalf("CommitAndApply leader: %v evidence=%+v", err, evidence)
	}
	seeded, err := h.ApplyCommittedEntriesToNode("node-b", entries[0])
	if err != nil {
		t.Fatalf("seed follower: %v results=%+v", err, seeded)
	}
	assertAppliedResults(t, "node-b seed", seeded, []int64{1})
	catchup, err := h.CatchUpNode("node-b")
	if err != nil {
		t.Fatalf("CatchUpNode: %v results=%+v", err, catchup)
	}
	assertAppliedResults(t, "node-b catchup", catchup, []int64{1, 1, 1})
	if catchup[0] != seeded[0] {
		t.Fatalf("node-b catchup first result=%+v, want replayed last result %+v", catchup[0], seeded[0])
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 14, Index: 4})
	if leaderDigest, followerDigest := logicalDigest(t, h, "node-a"), logicalDigest(t, h, "node-b"); leaderDigest != followerDigest {
		t.Fatalf("digest after catch-up mismatch leader=%s follower=%s", leaderDigest.Hex(), followerDigest.Hex())
	}

	bad := openTestHarnessAt(t, filepath.Join(t.TempDir(), "bad"))
	defer func() { _ = bad.Close() }()
	if evidence, err := bad.CommitAndApply([]raftcluster.NodeID{"node-b"}, entries[0]); err != nil {
		t.Fatalf("seed bad follower: %v evidence=%+v", err, evidence)
	}
	if _, err := bad.Commit(entries[2]); err != nil {
		t.Fatalf("Commit sparse entry: %v", err)
	}
	divergent := committedCommand(13, 1, deterministicCreateCollectionEntry(t, "admins", "harness:create:admins:divergent"))
	if _, err := bad.Commit(divergent); !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("Commit divergent err=%v, want ErrCommittedLogConflict", err)
	}

	missing := committedCommand(13, 3, deterministicCreateCollectionEntry(t, "audits", "harness:create:audits:missing-commit"))
	gapResults, err := bad.ApplyCommittedEntriesToNode("node-b", missing)
	if !errors.Is(err, ErrCommittedLogGap) {
		t.Fatalf("gap apply err=%v results=%+v, want ErrCommittedLogGap", err, gapResults)
	}
	if len(gapResults) != 0 {
		t.Fatalf("gap apply results=%+v, want none before node mutation", gapResults)
	}
	assertLastApplied(t, bad, "node-b", raftentry.ApplyEntryID{Term: 13, Index: 1})
	assertCollectionMissing(t, bad, "node-b", "audits")
	assertCollectionMissing(t, bad, "node-b", "orders")

	divergentResults, err := bad.ApplyCommittedEntriesToNode("node-b", divergent)
	if !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("divergent apply err=%v results=%+v, want ErrCommittedLogConflict", err, divergentResults)
	}
	if len(divergentResults) != 0 {
		t.Fatalf("divergent apply results=%+v, want none before node mutation", divergentResults)
	}
	assertLastApplied(t, bad, "node-b", raftentry.ApplyEntryID{Term: 13, Index: 1})
	assertCollectionMissing(t, bad, "node-b", "admins")
}

func TestCatchUpNodeRejectsDivergentLastAppliedEntry(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	committed := []raftfsm.CommittedEntryV1{
		committedCommand(19, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:committed")),
		committedCommand(19, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:after-divergence")),
	}
	divergent := committedCommand(19, 1, deterministicCreateCollectionEntry(t, "admins", "harness:create:admins:divergent-last"))
	seeded := applyEntriesDirectlyToNode(t, h, "node-b", divergent)
	assertAppliedResults(t, "node-b divergent seed", seeded, []int64{1})
	if _, err := h.Commit(committed...); err != nil {
		t.Fatalf("Commit committed log: %v", err)
	}

	catchup, err := h.CatchUpNode("node-b")
	assertRejectedResult(t, "divergent last-applied catch-up", catchup, err, raftentry.ErrorRejectedConflictV1)
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 19, Index: 1})
	assertCollectionMissing(t, h, "node-b", "users")
	assertCollectionMissing(t, h, "node-b", "orders")
}

func TestCommitClonesAndComparesClusterRouteMetadata(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "harness:create:users:route-metadata")
	entry := committedCommand(29, 1, raw)
	entry.RequestMetadata = raftentry.RequestMetadataV1{
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      "default",
		ClusterRouteCatalog:       "default",
		ClusterRouteCollection:    "users",
		ClusterRouteShape:         "token",
		ClusterRouteGroupID:       "group-a",
		ClusterRouteMembers:       []string{"node-a", "node-b"},
		ClusterRouteLeaderHint:    "node-a",
		ClusterRoutePlacementMode: "collection",
		ClusterRouteKey:           "_id",
		ClusterRouteTokenKnown:    true,
		ClusterRouteToken:         42,
		ClusterRoutePartitionID:   "token-000042",
	}
	if _, err := h.Commit(entry); err != nil {
		t.Fatalf("Commit route metadata entry: %v", err)
	}
	entry.RequestMetadata.ClusterRouteMembers[0] = "mutated-node"

	want := committedCommand(29, 1, raw)
	want.RequestMetadata = raftentry.RequestMetadataV1{
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      "default",
		ClusterRouteCatalog:       "default",
		ClusterRouteCollection:    "users",
		ClusterRouteShape:         "token",
		ClusterRouteGroupID:       "group-a",
		ClusterRouteMembers:       []string{"node-a", "node-b"},
		ClusterRouteLeaderHint:    "node-a",
		ClusterRoutePlacementMode: "collection",
		ClusterRouteKey:           "_id",
		ClusterRouteTokenKnown:    true,
		ClusterRouteToken:         42,
		ClusterRoutePartitionID:   "token-000042",
	}
	results, err := h.ApplyCommittedEntriesToNode("node-a", want)
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesToNode original route metadata: %v results=%+v", err, results)
	}
	assertAppliedResults(t, "route metadata apply", results, []int64{1})

	divergent := want
	divergent.RequestMetadata.ClusterRoutePartitionID = "token-000043"
	if _, err := h.Commit(divergent); !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("Commit divergent route metadata err=%v want ErrCommittedLogConflict", err)
	}

	divergent = want
	divergent.RequestMetadata.ClusterRouteKey = "tenant_id"
	if _, err := h.Commit(divergent); !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("Commit divergent route key err=%v want ErrCommittedLogConflict", err)
	}
}

func TestCatchUpNodeRejectsDivergentAppliedPrefix(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	committed := []raftfsm.CommittedEntryV1{
		committedCommand(20, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:prefix")),
		committedCommand(20, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:prefix")),
		committedCommand(20, 3, deterministicCreateCollectionEntry(t, "audits", "harness:create:audits:prefix")),
		committedCommand(20, 4, deterministicCreateCollectionEntry(t, "products", "harness:create:products:prefix")),
	}
	divergent := []raftfsm.CommittedEntryV1{
		committed[0],
		committedCommand(20, 2, deterministicCreateCollectionEntry(t, "admins", "harness:create:admins:divergent-prefix")),
		committed[2],
	}
	seeded := applyEntriesDirectlyToNode(t, h, "node-b", divergent...)
	assertAppliedResults(t, "node-b divergent prefix seed", seeded, []int64{1, 1, 1})
	if _, err := h.Commit(committed...); err != nil {
		t.Fatalf("Commit committed log: %v", err)
	}

	catchup, err := h.CatchUpNode("node-b")
	assertRejectedResult(t, "divergent applied-prefix catch-up", catchup, err, raftentry.ErrorRejectedConflictV1)
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 20, Index: 3})
	assertCollectionMissing(t, h, "node-b", "orders")
	assertCollectionMissing(t, h, "node-b", "products")
}

func TestCatchUpNodeRejectsAppliedPrefixTermMismatch(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	createUsers := deterministicCreateCollectionEntry(t, "users", "harness:create:users:term-prefix")
	createOrders := deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:term-prefix")
	local := []raftfsm.CommittedEntryV1{
		committedCommand(21, 1, createUsers),
		committedCommand(21, 2, createOrders),
	}
	committed := []raftfsm.CommittedEntryV1{
		committedCommand(20, 1, createUsers),
		committedCommand(20, 2, createOrders),
		committedCommand(21, 3, deterministicCreateCollectionEntry(t, "audits", "harness:create:audits:term-prefix")),
	}
	seeded := applyEntriesDirectlyToNode(t, h, "node-b", local...)
	assertAppliedResults(t, "node-b term-mismatch prefix seed", seeded, []int64{1, 1})
	if _, err := h.Commit(committed...); err != nil {
		t.Fatalf("Commit committed log: %v", err)
	}

	catchup, err := h.CatchUpNode("node-b")
	if !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("term-mismatch applied-prefix catch-up err=%v results=%+v, want ErrCommittedLogConflict", err, catchup)
	}
	if len(catchup) != 0 {
		t.Fatalf("term-mismatch applied-prefix catch-up results=%+v, want none", catchup)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 21, Index: 2})
	assertCollectionMissing(t, h, "node-b", "audits")
}

func TestCatchUpNodeRejectsLastAppliedBeyondCommittedLog(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(22, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:short-log")),
		committedCommand(22, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:short-log")),
	}
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit committed log: %v", err)
	}
	seeded, err := h.ApplyCommittedEntriesToNode("node-b", entries...)
	if err != nil {
		t.Fatalf("seed follower through committed log: %v results=%+v", err, seeded)
	}
	assertAppliedResults(t, "node-b shortened-log seed", seeded, []int64{1, 1})
	h.mu.Lock()
	h.committed = h.committed[:1]
	h.mu.Unlock()

	catchup, err := h.CatchUpNode("node-b")
	if !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("CatchUpNode shortened log err=%v results=%+v, want ErrCommittedLogConflict", err, catchup)
	}
	if len(catchup) != 0 {
		t.Fatalf("CatchUpNode shortened log results=%+v, want none", catchup)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 22, Index: 2})
}

func TestCommitRejectsInvalidBatchAtomically(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	valid := committedCommand(16, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:atomic"))
	lowerTerm := committedCommand(15, 3, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:atomic-term"))
	evidence, err := h.Commit(valid, lowerTerm)
	if !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("Commit invalid batch err=%v, want ErrCommittedLogConflict evidence=%+v", err, evidence)
	}
	if evidence.Committed {
		t.Fatalf("Commit invalid batch evidence=%+v, want not committed", evidence)
	}
	catchup, err := h.CatchUpNode("node-a")
	if err != nil {
		t.Fatalf("CatchUpNode after rejected batch: %v results=%+v", err, catchup)
	}
	if len(catchup) != 0 {
		t.Fatalf("CatchUpNode after rejected batch results=%+v, want no committed entries", catchup)
	}
	assertNoLastApplied(t, h, "node-a")
	assertCollectionMissing(t, h, "node-a", "users")
	assertCollectionMissing(t, h, "node-a", "orders")
}

func TestCommitRejectsTermRegressionBeforeMutation(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	highTerm := committedCommand(5, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:term-regression"))
	lowerTerm := committedCommand(4, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:term-regression"))
	evidence, err := h.Commit(highTerm, lowerTerm)
	if !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("Commit term regression batch err=%v, want ErrCommittedLogConflict evidence=%+v", err, evidence)
	}
	if evidence.Committed {
		t.Fatalf("Commit term regression batch evidence=%+v, want not committed", evidence)
	}
	catchup, err := h.CatchUpNode("node-a")
	if err != nil {
		t.Fatalf("CatchUpNode after rejected term regression: %v results=%+v", err, catchup)
	}
	if len(catchup) != 0 {
		t.Fatalf("CatchUpNode after rejected term regression results=%+v, want no committed entries", catchup)
	}
	assertNoLastApplied(t, h, "node-a")
	assertCollectionMissing(t, h, "node-a", "users")
	assertCollectionMissing(t, h, "node-a", "orders")

	if _, err := h.Commit(highTerm); err != nil {
		t.Fatalf("Commit high-term seed after rejected batch: %v", err)
	}
	if _, err := h.Commit(lowerTerm); !errors.Is(err, ErrCommittedLogConflict) {
		t.Fatalf("Commit lower-term continuation err=%v, want ErrCommittedLogConflict", err)
	}
	catchup, err = h.CatchUpNode("node-a")
	if err != nil {
		t.Fatalf("CatchUpNode after rejected continuation: %v results=%+v", err, catchup)
	}
	assertAppliedResults(t, "node-a catchup after rejected continuation", catchup, []int64{1})
	assertLastApplied(t, h, "node-a", raftentry.ApplyEntryID{Term: 5, Index: 1})
	assertCollectionMissing(t, h, "node-a", "orders")
}

func TestPostCommitCrashReadableAndPreCommitCrashNoSuccess(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	create := committedCommand(15, 1, deterministicCreateCollectionEntryWithOptions(t, "users", "harness:create:users:crash", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	}))
	insert := committedCommand(15, 2, deterministicInsertBatchEntry(t, "users", "harness:insert:users:crash", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{
		testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}}),
	}))

	pre := h.PreCommitEvidence(create)
	if pre.Kind != EvidenceInjectedCommittedEntryReplayV1 || pre.Committed || pre.HasCommittedSuccess() || pre.ProvesProductionConsensus() {
		t.Fatalf("pre-commit evidence=%+v, want no committed success and no production consensus claim", pre)
	}
	if err := h.CloseNode("node-a"); err != nil {
		t.Fatalf("CloseNode leader before commit: %v", err)
	}
	assertCollectionMissing(t, h, "node-b", "users")
	assertCollectionMissing(t, h, "node-c", "users")

	if _, err := h.ReopenNode("node-a"); err != nil {
		t.Fatalf("ReopenNode leader: %v", err)
	}
	post, err := h.CommitAndApply([]raftcluster.NodeID{"node-b", "node-c"}, create, insert)
	if err != nil {
		t.Fatalf("CommitAndApply surviving quorum: %v evidence=%+v", err, post)
	}
	assertInjectedCommittedEvidence(t, post)
	if err := h.CloseNode("node-a"); err != nil {
		t.Fatalf("CloseNode leader after commit: %v", err)
	}
	for _, nodeID := range []raftcluster.NodeID{"node-b", "node-c"} {
		if _, err := h.ReopenNode(nodeID); err != nil {
			t.Fatalf("ReopenNode %s: %v", nodeID, err)
		}
		assertDocumentCity(t, h, nodeID, "users", "u1", "hnl")
		assertLastApplied(t, h, nodeID, raftentry.ApplyEntryID{Term: 15, Index: 2})
	}
}

func TestMixedMutationSequenceSurvivesReopenAndCatchUp(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 17)
	evidence, err := h.CommitAndApply([]raftcluster.NodeID{"node-a"}, entries...)
	if err != nil {
		t.Fatalf("CommitAndApply mixed sequence: %v evidence=%+v", err, evidence)
	}
	assertInjectedCommittedEvidence(t, evidence)
	assertAppliedResults(t, "node-a mixed sequence", evidence.Applied["node-a"], []int64{1, 2, 1, 1})
	if err := h.CloseNode("node-a"); err != nil {
		t.Fatalf("CloseNode node-a: %v", err)
	}
	if _, err := h.ReopenNode("node-a"); err != nil {
		t.Fatalf("ReopenNode node-a: %v", err)
	}
	assertLastApplied(t, h, "node-a", raftentry.ApplyEntryID{Term: 17, Index: 4})

	catchup, err := h.CatchUpNode("node-b")
	if err != nil {
		t.Fatalf("CatchUpNode node-b: %v results=%+v", err, catchup)
	}
	assertAppliedResults(t, "node-b mixed catchup", catchup, []int64{1, 2, 1, 1})
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 17, Index: 4})
	if leaderDigest, followerDigest := logicalDigest(t, h, "node-a"), logicalDigest(t, h, "node-b"); leaderDigest != followerDigest {
		t.Fatalf("digest after mixed reopen/catch-up mismatch leader=%s follower=%s", leaderDigest.Hex(), followerDigest.Hex())
	}
	assertDocumentCity(t, h, "node-a", "users", "u1", "sfo")
	assertDocumentCity(t, h, "node-b", "users", "u1", "sfo")
	assertDocumentMissing(t, h, "node-a", "users", "u2")
	assertDocumentMissing(t, h, "node-b", "users", "u2")
}

func TestOpenRejectsInvalidNodeIDBeforeOpeningPath(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	escaped := filepath.Join(base, "outside")
	h, err := Open(Options{
		RootDir: root,
		Nodes: []NodeConfig{
			{ID: "../../outside"},
		},
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err == nil {
		if h != nil {
			_ = h.Close()
		}
		t.Fatalf("Open with invalid node id succeeded, want error")
	}
	if got := err.Error(); !strings.Contains(got, "invalid node id") || !strings.Contains(got, "unsupported character '/'") {
		t.Fatalf("Open err=%v, want invalid-node-id path-segment diagnostic", err)
	}
	if _, statErr := os.Stat(escaped); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("escaped node path stat err=%v, want not exist", statErr)
	}
}

func TestClosedHarnessRejectsMutatingOperations(t *testing.T) {
	h := openTestHarness(t)
	entry := committedCommand(18, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:closed"))
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := h.ReopenNode("node-a"); !errors.Is(err, ErrHarnessClosed) {
		t.Fatalf("ReopenNode after Close err=%v, want ErrHarnessClosed", err)
	}
	if err := h.CloseNode("node-a"); !errors.Is(err, ErrHarnessClosed) {
		t.Fatalf("CloseNode after Close err=%v, want ErrHarnessClosed", err)
	}
	evidence, err := h.Commit(entry)
	if !errors.Is(err, ErrHarnessClosed) {
		t.Fatalf("Commit after Close err=%v, want ErrHarnessClosed evidence=%+v", err, evidence)
	}
	if evidence.Committed {
		t.Fatalf("Commit after Close evidence=%+v, want not committed", evidence)
	}
	if _, err := h.ApplyCommittedEntriesToNode("node-a", entry); !errors.Is(err, ErrHarnessClosed) {
		t.Fatalf("ApplyCommittedEntriesToNode after Close err=%v, want ErrHarnessClosed", err)
	}
	if _, err := h.CatchUpNode("node-a"); !errors.Is(err, ErrHarnessClosed) {
		t.Fatalf("CatchUpNode after Close err=%v, want ErrHarnessClosed", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func openTestHarness(t testing.TB) *Harness {
	t.Helper()
	return openTestHarnessAt(t, t.TempDir())
}

func openTestHarnessAt(t testing.TB, root string) *Harness {
	t.Helper()
	h, err := Open(Options{
		RootDir: root,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Open harness: %v", err)
	}
	return h
}

func applyEntriesDirectlyToNode(t testing.TB, h *Harness, nodeID raftcluster.NodeID, entries ...raftfsm.CommittedEntryV1) []raftentry.ApplyResultV1 {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s not found", nodeID)
	}
	results, err := node.ApplyCommittedEntriesV1(entries...)
	if err != nil {
		t.Fatalf("directly apply entries to %s: %v results=%+v", nodeID, err, results)
	}
	return results
}

func mixedUserEntries(t *testing.T, term uint64) []raftfsm.CommittedEntryV1 {
	t.Helper()
	create := deterministicCreateCollectionEntryWithOptions(t, "users", "harness:create:users:mixed", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	})
	insert := deterministicInsertBatchEntry(t, "users", "harness:insert:users:mixed", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
		testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}}),
		testBSONDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "sea"}}),
	})
	replace := deterministicReplaceBatchEntry(t, "users", "harness:replace:users:mixed", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{
		testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "sfo"}}),
	})
	deleteEntry := deterministicDeleteBatchEntry(t, "users", "harness:delete:users:mixed", [][]byte{[]byte("u2")})
	return []raftfsm.CommittedEntryV1{
		committedCommand(term, 1, create),
		committedCommand(term, 2, insert),
		committedCommand(term, 3, replace),
		committedCommand(term, 4, deleteEntry),
	}
}

func committedCommand(term, index uint64, raw []byte) raftfsm.CommittedEntryV1 {
	return raftfsm.CommittedEntryV1{
		Type:                     raftfsm.EntryTypeCommandEntryV1,
		Term:                     term,
		Index:                    index,
		Bytes:                    raw,
		CurrentCatalogVersion:    testCatalogVersionStart,
		HasCurrentCatalogVersion: true,
	}
}

func deterministicCreateCollectionEntry(t *testing.T, collection, idempotency string) []byte {
	t.Helper()
	return deterministicCreateCollectionEntryWithOptions(t, collection, idempotency, testCreateCollectionMetaOptions{})
}

type testCreateCollectionMetaOptions struct {
	documentFormat uint64
}

func deterministicCreateCollectionEntryWithOptions(t *testing.T, collection, idempotency string, opts testCreateCollectionMetaOptions) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: testCreateCollectionMetaPayload(collection, opts)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func testCreateCollectionMetaPayload(collection string, opts testCreateCollectionMetaOptions) []byte {
	dst := binary.AppendUvarint(nil, 1)
	dst = appendTestString(dst, collection)
	dst = binary.AppendUvarint(dst, opts.documentFormat)
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

func deterministicInsertBatchEntry(t *testing.T, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	return deterministicMutationEntry(t, nativewire.CommandInsertBatch, collection, idempotency, format, ids, documents, nil)
}

func deterministicReplaceBatchEntry(t *testing.T, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	extra := []nativewire.Section{{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)}}
	return deterministicMutationEntry(t, nativewire.CommandReplaceBatch, collection, idempotency, format, ids, documents, extra)
}

func deterministicDeleteBatchEntry(t *testing.T, collection, idempotency string, ids [][]byte) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandDeleteBatch, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(collection)},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, ids...)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func deterministicMutationEntry(t *testing.T, command nativewire.CommandID, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte, extra []nativewire.Section) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: command, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(collection)},
		{ID: nativewire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(format))},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, ids...)},
		{ID: nativewire.SectionDocuments, Bytes: nativewire.AppendByteVector(nil, documents...)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	sections = append(sections, extra...)
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func deterministicTestCollectionNameRef(collection string) []byte {
	return append([]byte{deterministicCollectionRefName}, collection...)
}

func testBSONDocument(t *testing.T, document bson.D) []byte {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("Marshal BSON: %v", err)
	}
	return encoded
}

func appendTestString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func assertInjectedCommittedEvidence(t *testing.T, evidence CommitEvidenceV1) {
	t.Helper()
	if evidence.Kind != EvidenceInjectedCommittedEntryReplayV1 || !evidence.Committed || evidence.ProvesProductionConsensus() || !evidence.HasCommittedSuccess() {
		t.Fatalf("evidence=%+v, want committed injected replay evidence with no production consensus claim", evidence)
	}
}

func assertAppliedResults(t *testing.T, label any, results []raftentry.ApplyResultV1, wantAffected []int64) {
	t.Helper()
	if len(results) != len(wantAffected) {
		t.Fatalf("%v results=%d, want %d", label, len(results), len(wantAffected))
	}
	for i, want := range wantAffected {
		assertAppliedResult(t, results[i], raftentry.ApplyStatusApplied, want)
	}
}

func assertAppliedResult(t *testing.T, result raftentry.ApplyResultV1, wantStatus raftentry.ApplyStatusV1, wantAffected int64) {
	t.Helper()
	if result.Status != wantStatus ||
		result.DeterministicErrorCode != raftentry.ErrorNoneV1 ||
		result.AffectedCount != wantAffected ||
		result.CommandDigest == (raftentry.CommandDigestV1{}) ||
		result.ResultDigest == (raftentry.CommandDigestV1{}) {
		t.Fatalf("result=%+v, want status=%s affected=%d non-zero digests", result, wantStatus, wantAffected)
	}
}

func assertRejectedResult(t *testing.T, label string, results []raftentry.ApplyResultV1, err error, wantCode raftentry.DeterministicErrorCodeV1) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s err=nil results=%+v, want rejection", label, results)
	}
	if code, ok := raftfsm.ErrorCodeOf(err); !ok || code != wantCode {
		t.Fatalf("%s err=%v code=(%s,%t), want %s", label, err, code, ok, wantCode)
	}
	if len(results) != 1 || results[0].Status != raftentry.ApplyStatusRejectedConflict || results[0].DeterministicErrorCode != wantCode {
		t.Fatalf("%s results=%+v, want single rejected-conflict result with %s", label, results, wantCode)
	}
}

func assertLastApplied(t *testing.T, h *Harness, nodeID raftcluster.NodeID, want raftentry.ApplyEntryID) {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s not found", nodeID)
	}
	got, ok := node.LastApplied()
	if !ok || got != want {
		t.Fatalf("%s LastApplied=(%+v,%t), want %+v", nodeID, got, ok, want)
	}
}

func assertNoLastApplied(t *testing.T, h *Harness, nodeID raftcluster.NodeID) {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s not found", nodeID)
	}
	if got, ok := node.LastApplied(); ok {
		t.Fatalf("%s LastApplied=(%+v,%t), want none", nodeID, got, ok)
	}
}

func logicalDigest(t *testing.T, h *Harness, nodeID raftcluster.NodeID) raftapply.LogicalDigestV1 {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s not found", nodeID)
	}
	digest, err := node.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("%s LogicalDigestV1: %v", nodeID, err)
	}
	return digest
}

func assertCollectionMissing(t *testing.T, h *Harness, nodeID raftcluster.NodeID, collection string) {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s not found", nodeID)
	}
	_, err := collections.NewCollectionManager(node.DB()).OpenCollection(collection)
	if !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("%s OpenCollection %s err=%v, want ErrCollectionNotFound", nodeID, collection, err)
	}
}

func assertDocumentCity(t *testing.T, h *Harness, nodeID raftcluster.NodeID, collection, id, wantCity string) {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s not found", nodeID)
	}
	opened, err := collections.NewCollectionManager(node.DB()).OpenCollection(collection)
	if err != nil {
		t.Fatalf("%s OpenCollection %s: %v", nodeID, collection, err)
	}
	got, err := opened.Get([]byte(id))
	if err != nil {
		t.Fatalf("%s %s.Get(%q): %v", nodeID, collection, id, err)
	}
	if got == nil {
		t.Fatalf("%s %s.Get(%q)=nil, want document", nodeID, collection, id)
	}
	if city := bson.Raw(got).Lookup("city").StringValue(); city != wantCity {
		t.Fatalf("%s %s.Get(%q).city=%q, want %q", nodeID, collection, id, city, wantCity)
	}
}

func assertDocumentMissing(t *testing.T, h *Harness, nodeID raftcluster.NodeID, collection, id string) {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s not found", nodeID)
	}
	opened, err := collections.NewCollectionManager(node.DB()).OpenCollection(collection)
	if err != nil {
		t.Fatalf("%s OpenCollection %s: %v", nodeID, collection, err)
	}
	got, err := opened.Get([]byte(id))
	if err != nil {
		t.Fatalf("%s %s.Get(%q): %v", nodeID, collection, id, err)
	}
	if got != nil {
		t.Fatalf("%s %s.Get(%q)=%x, want missing document", nodeID, collection, id, got)
	}
}
