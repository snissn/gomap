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
