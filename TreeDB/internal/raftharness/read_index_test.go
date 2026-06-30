package raftharness

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

func TestReadIndexProviderReturnsLatestCommittedProofWithoutApplying(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(30, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-index")),
		committedCommand(31, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-index")),
	}
	if evidence, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
	assertNoLastApplied(t, h, "node-b")

	proof, err := h.ReadIndexProvider("node-b").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
		NodeID:  "node-b",
		GroupID: "default",
	})
	if err != nil {
		t.Fatalf("ReadIndex: %v proof=%+v", err, proof)
	}
	if proof.NodeID != "node-b" || proof.GroupID != "default" || proof.Term != 31 || proof.Index != 2 ||
		!proof.HasQuorum || proof.EvidenceKind != raftcluster.ReadIndexEvidenceTestHarness {
		t.Fatalf("proof=%+v, want node-b/default 31/2 quorum test_harness evidence", proof)
	}
	assertNoLastApplied(t, h, "node-b")
	assertCollectionMissing(t, h, "node-b", "users")
	assertCollectionMissing(t, h, "node-b", "orders")
}

func TestReadIndexProviderAllowsUnconstrainedTargetFields(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()
	commitReadIndexEntry(t, h, 32, 1, "users", "unconstrained-target")

	proof, err := h.ReadIndexProvider("node-a").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{})
	if err != nil {
		t.Fatalf("ReadIndex unconstrained target: %v proof=%+v", err, proof)
	}
	if proof.NodeID != "node-a" || proof.GroupID != "default" || proof.Term != 32 || proof.Index != 1 ||
		!proof.HasQuorum || proof.EvidenceKind != raftcluster.ReadIndexEvidenceTestHarness {
		t.Fatalf("proof=%+v, want node-a/default 32/1 quorum test_harness evidence", proof)
	}
}

func TestReadIndexProviderFailsClosed(t *testing.T) {
	t.Run("empty log", func(t *testing.T) {
		h := openTestHarness(t)
		defer func() { _ = h.Close() }()

		_, err := h.ReadIndexProvider("node-a").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
			NodeID:  "node-a",
			GroupID: "default",
		})
		if !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
			t.Fatalf("ReadIndex err=%v, want ErrReadBarrierNotSatisfied", err)
		}
	})

	t.Run("target node mismatch", func(t *testing.T) {
		h := openTestHarness(t)
		defer func() { _ = h.Close() }()
		commitReadIndexEntry(t, h, 32, 1, "users", "target-node-mismatch")

		_, err := h.ReadIndexProvider("node-a").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
			NodeID:  "node-b",
			GroupID: "default",
		})
		if !errors.Is(err, raftcluster.ErrReadBarrierTargetMismatch) {
			t.Fatalf("ReadIndex err=%v, want ErrReadBarrierTargetMismatch", err)
		}
	})

	t.Run("target group mismatch", func(t *testing.T) {
		h := openTestHarness(t)
		defer func() { _ = h.Close() }()
		commitReadIndexEntry(t, h, 32, 1, "users", "target-group-mismatch")

		_, err := h.ReadIndexProvider("node-a").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
			NodeID:  "node-a",
			GroupID: "group-b",
		})
		if !errors.Is(err, raftcluster.ErrReadBarrierTargetMismatch) {
			t.Fatalf("ReadIndex err=%v, want ErrReadBarrierTargetMismatch", err)
		}
	})

	t.Run("unknown node", func(t *testing.T) {
		h := openTestHarness(t)
		defer func() { _ = h.Close() }()
		commitReadIndexEntry(t, h, 32, 1, "users", "unknown-node")

		_, err := h.ReadIndexProvider("node-z").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
			NodeID:  "node-z",
			GroupID: "default",
		})
		if !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("ReadIndex err=%v, want ErrNodeNotFound", err)
		}
	})

	t.Run("closed node", func(t *testing.T) {
		h := openTestHarness(t)
		defer func() { _ = h.Close() }()
		commitReadIndexEntry(t, h, 32, 1, "users", "closed-node")
		if err := h.CloseNode("node-a"); err != nil {
			t.Fatalf("CloseNode: %v", err)
		}

		_, err := h.ReadIndexProvider("node-a").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
			NodeID:  "node-a",
			GroupID: "default",
		})
		if !errors.Is(err, ErrNodeClosed) || errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("ReadIndex err=%v, want ErrNodeClosed and not ErrNodeNotFound", err)
		}
	})

	t.Run("closed harness", func(t *testing.T) {
		h := openTestHarness(t)
		commitReadIndexEntry(t, h, 32, 1, "users", "closed-harness")
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		_, err := h.ReadIndexProvider("node-a").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
			NodeID:  "node-a",
			GroupID: "default",
		})
		if !errors.Is(err, ErrHarnessClosed) {
			t.Fatalf("ReadIndex err=%v, want ErrHarnessClosed", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		h := openTestHarness(t)
		defer func() { _ = h.Close() }()
		commitReadIndexEntry(t, h, 32, 1, "users", "canceled")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := h.ReadIndexProvider("node-a").ReadIndex(ctx, raftcluster.ReadIndexBarrier{
			NodeID:  "node-a",
			GroupID: "default",
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadIndex err=%v, want context.Canceled", err)
		}
	})

	t.Run("zero index proof", func(t *testing.T) {
		h := openTestHarness(t)
		defer func() { _ = h.Close() }()
		h.mu.Lock()
		h.committed = []raftfsm.CommittedEntryV1{{Term: 32}}
		h.mu.Unlock()

		_, err := h.ReadIndexProvider("node-a").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
			NodeID:  "node-a",
			GroupID: "default",
		})
		if !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
			t.Fatalf("ReadIndex err=%v, want ErrReadBarrierNotSatisfied", err)
		}
	})
}

func TestReadIndexProviderComposesWithReadBarrierCatchUp(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(33, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:read-index-catchup")),
		committedCommand(33, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:read-index-catchup")),
	}
	if evidence, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
	assertNoLastApplied(t, h, "node-c")

	proof, err := h.ReadIndexProvider("node-c").ReadIndex(context.Background(), raftcluster.ReadIndexBarrier{
		NodeID:  "node-c",
		GroupID: "default",
	})
	if err != nil {
		t.Fatalf("ReadIndex: %v proof=%+v", err, proof)
	}
	if proof.EvidenceKind != raftcluster.ReadIndexEvidenceTestHarness {
		t.Fatalf("proof evidence=%s want test_harness", proof.EvidenceKind)
	}
	assertNoLastApplied(t, h, "node-c")

	progress, err := h.ReadBarrier("node-c").WaitAppliedIndex(context.Background(), proof.AppliedIndexBarrier())
	if err != nil {
		t.Fatalf("WaitAppliedIndex: %v progress=%+v", err, progress)
	}
	if progress.NodeID != "node-c" || progress.GroupID != "default" || progress.Term != 33 || progress.Index != 2 || !progress.HasApplied {
		t.Fatalf("progress=%+v, want node-c/default 33/2", progress)
	}
	assertLastApplied(t, h, "node-c", raftentry.ApplyEntryID{Term: 33, Index: 2})
}

func commitReadIndexEntry(t *testing.T, h *Harness, term, index uint64, collection, suffix string) {
	t.Helper()
	entry := committedCommand(term, index, deterministicCreateCollectionEntry(t, collection, "harness:create:"+collection+":read-index-"+suffix))
	if evidence, err := h.Commit(entry); err != nil {
		t.Fatalf("Commit: %v evidence=%+v", err, evidence)
	}
}
