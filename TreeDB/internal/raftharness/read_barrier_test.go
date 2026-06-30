package raftharness

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

func TestReadBarrierCatchesUpNodeBeforeProvingAppliedIndex(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(23, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier")),
		committedCommand(23, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-barrier")),
	}
	if evidence, err := h.Commit(entries[0], entries[1]); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
	assertNoLastApplied(t, h, "node-b")

	barrier := h.ReadBarrier("node-b")
	progress, err := barrier.WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 2,
	})
	if err != nil {
		t.Fatalf("WaitAppliedIndex: %v progress=%+v", err, progress)
	}
	if progress.NodeID != "node-b" || progress.GroupID != "default" || progress.Term != 23 || progress.Index != 2 || !progress.HasApplied {
		t.Fatalf("progress=%+v, want node-b default 23/2", progress)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 23, Index: 2})
}

func TestReadBarrierCatchesUpSparseCommittedCommandIndex(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(30, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-sparse")),
		committedCommand(30, 3, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-barrier-sparse")),
	}
	if evidence, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit sparse read-barrier entries: %v evidence=%+v", err, evidence)
	}
	assertNoLastApplied(t, h, "node-b")

	progress, err := h.ReadBarrier("node-b").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 3,
	})
	if err != nil {
		t.Fatalf("WaitAppliedIndex sparse command: %v progress=%+v", err, progress)
	}
	if progress.NodeID != "node-b" || progress.GroupID != "default" || progress.Term != 30 || progress.Index != 3 || !progress.HasApplied {
		t.Fatalf("progress=%+v, want node-b default 30/3", progress)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 30, Index: 3})
}

func TestReadBarrierFailsClosedWhenBarrierIndexIsSparseNonCommandGap(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(31, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-gap")),
		committedCommand(31, 3, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-barrier-gap")),
	}
	if evidence, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit sparse read-barrier gap entries: %v evidence=%+v", err, evidence)
	}

	progress, err := h.ReadBarrier("node-b").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 2,
	})
	if !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
		t.Fatalf("WaitAppliedIndex sparse gap err=%v progress=%+v, want ErrReadBarrierNotSatisfied", err, progress)
	}
	if progress.NodeID != "node-b" || progress.GroupID != "default" || progress.Term != 31 || progress.Index != 1 || !progress.HasApplied {
		t.Fatalf("progress after sparse gap barrier=%+v, want node-b default 31/1", progress)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 31, Index: 1})
	assertCollectionMissing(t, h, "node-b", "orders")
}

func TestReadBarrierFailsClosedWhenCommittedLogCannotSatisfyIndex(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entry := committedCommand(24, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-unsatisfied"))
	if evidence, err := h.Commit(entry); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}

	progress, err := h.ReadBarrier("node-c").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 2,
	})
	if !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
		t.Fatalf("WaitAppliedIndex err=%v progress=%+v, want ErrReadBarrierNotSatisfied", err, progress)
	}
	if progress.NodeID != "node-c" || progress.Index != 1 || !progress.HasApplied {
		t.Fatalf("progress after failed barrier=%+v, want node-c applied through 1", progress)
	}
	assertLastApplied(t, h, "node-c", raftentry.ApplyEntryID{Term: 24, Index: 1})
}

func TestReadBarrierValidatesCommittedPrefixBeforeSatisfiedAppliedIndex(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	committed := []raftfsm.CommittedEntryV1{
		committedCommand(25, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-prefix")),
		committedCommand(25, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-barrier-prefix")),
	}
	divergent := []raftfsm.CommittedEntryV1{
		committed[0],
		committedCommand(25, 2, deterministicCreateCollectionEntry(t, "admins", "harness:create:admins:read-barrier-divergent-prefix")),
	}
	seeded := applyEntriesDirectlyToNode(t, h, "node-b", divergent...)
	assertAppliedResults(t, "node-b divergent read-barrier seed", seeded, []int64{1, 1})
	if _, err := h.Commit(committed...); err != nil {
		t.Fatalf("Commit committed log: %v", err)
	}

	progress, err := h.ReadBarrier("node-b").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 2,
	})
	if code, ok := raftfsm.ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("WaitAppliedIndex err=%v code=(%s,%t), want %s", err, code, ok, raftentry.ErrorRejectedConflictV1)
	}
	if progress.NodeID != "node-b" || progress.Index != 2 || !progress.HasApplied {
		t.Fatalf("progress after rejected read barrier=%+v, want node-b applied through 2", progress)
	}
	assertCollectionMissing(t, h, "node-b", "orders")
}

func TestReadBarrierRefreshesProgressAfterPartialCatchUpError(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	first := committedCommand(26, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-partial"))
	second := committedCommand(26, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-barrier-unsupported"))
	second.Type = raftfsm.EntryTypeV1("snapshot-v1")
	if evidence, err := h.Commit(first, second); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
	assertNoLastApplied(t, h, "node-b")

	progress, err := h.ReadBarrier("node-b").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 2,
	})
	if code, ok := raftfsm.ErrorCodeOf(err); !ok || code != raftentry.ErrorUnsupportedVersionV1 {
		t.Fatalf("WaitAppliedIndex err=%v code=(%s,%t), want %s", err, code, ok, raftentry.ErrorUnsupportedVersionV1)
	}
	if progress.NodeID != "node-b" || progress.GroupID != "default" || progress.Term != 26 || progress.Index != 1 || !progress.HasApplied {
		t.Fatalf("progress after partial catch-up error=%+v, want node-b default 26/1", progress)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 26, Index: 1})
}

func TestReadBarrierAllowsSatisfiedPrefixWhenLaterCatchUpEntryFails(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	first := committedCommand(27, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-satisfied-prefix"))
	second := committedCommand(27, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-barrier-later-unsupported"))
	second.Type = raftfsm.EntryTypeV1("snapshot-v1")
	if evidence, err := h.Commit(first, second); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
	assertNoLastApplied(t, h, "node-b")

	progress, err := h.ReadBarrier("node-b").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 1,
	})
	if err != nil {
		t.Fatalf("WaitAppliedIndex: %v progress=%+v", err, progress)
	}
	if progress.NodeID != "node-b" || progress.GroupID != "default" || progress.Term != 27 || progress.Index != 1 || !progress.HasApplied {
		t.Fatalf("progress after satisfied-prefix catch-up error=%+v, want node-b default 27/1", progress)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 27, Index: 1})
}

func TestReadBarrierDoesNotApplyBeyondAlreadySatisfiedIndex(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	first := committedCommand(28, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-already-satisfied"))
	second := committedCommand(28, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-barrier-already-satisfied-unsupported"))
	second.Type = raftfsm.EntryTypeV1("snapshot-v1")
	if evidence, err := h.Commit(first, second); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
	seeded, err := h.ApplyCommittedEntriesToNode("node-b", first)
	if err != nil {
		t.Fatalf("seed node-b: %v results=%+v", err, seeded)
	}
	assertAppliedResults(t, "node-b already-satisfied seed", seeded, []int64{1})

	progress, err := h.ReadBarrier("node-b").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		MinAppliedIndex: 1,
	})
	if err != nil {
		t.Fatalf("WaitAppliedIndex: %v progress=%+v", err, progress)
	}
	if progress.NodeID != "node-b" || progress.GroupID != "default" || progress.Term != 28 || progress.Index != 1 || !progress.HasApplied {
		t.Fatalf("progress after already-satisfied barrier=%+v, want node-b default 28/1", progress)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 28, Index: 1})
	assertCollectionMissing(t, h, "node-b", "orders")
}

func TestReadBarrierZeroIndexDoesNotCatchUpCommittedLog(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	unsupported := committedCommand(29, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-barrier-zero"))
	unsupported.Type = raftfsm.EntryTypeV1("snapshot-v1")
	if evidence, err := h.Commit(unsupported); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
	assertNoLastApplied(t, h, "node-b")

	progress, err := h.ReadBarrier("node-b").WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{})
	if err != nil {
		t.Fatalf("WaitAppliedIndex zero barrier: %v progress=%+v", err, progress)
	}
	if progress.NodeID != "node-b" || progress.GroupID != "default" || progress.HasApplied || progress.Index != 0 {
		t.Fatalf("progress after zero barrier=%+v, want node-b default with no applied index", progress)
	}
	assertNoLastApplied(t, h, "node-b")
	assertCollectionMissing(t, h, "node-b", "users")
}

func TestNodeReadBarrierClosedNodeErrorsAreDistinct(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()
	node, ok := h.Node("node-b")
	if !ok {
		t.Fatal("node-b not found")
	}
	if err := h.CloseNode("node-b"); err != nil {
		t.Fatalf("CloseNode: %v", err)
	}

	if _, err := node.AppliedProgress(context.Background()); !errors.Is(err, ErrNodeClosed) || errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("AppliedProgress closed node err=%v, want ErrNodeClosed and not ErrNodeNotFound", err)
	}
	if _, err := node.WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		NodeID:          "node-b",
		MinAppliedIndex: 1,
	}); !errors.Is(err, ErrNodeClosed) || errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("WaitAppliedIndex closed node err=%v, want ErrNodeClosed and not ErrNodeNotFound", err)
	}
}
