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
