package raftfsm

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestAppliedProgressReportsCurrentIndexAndBarrierErrors(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	progress, err := fsm.AppliedProgress(context.Background())
	if err != nil {
		t.Fatalf("AppliedProgress before apply: %v", err)
	}
	if progress.NodeID != "node-a" || progress.GroupID != "default" || progress.HasApplied || progress.Index != 0 {
		t.Fatalf("progress before apply=%+v, want node identity without applied index", progress)
	}

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:read-barrier")
	if _, err := fsm.ApplyCommittedEntryV1(committedCommand(4, 1, raw)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v", err)
	}
	progress, err = fsm.WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		NodeID:          "node-a",
		GroupID:         "default",
		MinAppliedIndex: 1,
	})
	if err != nil {
		t.Fatalf("WaitAppliedIndex satisfied: %v", err)
	}
	if progress.Term != 4 || progress.Index != 1 || !progress.HasApplied {
		t.Fatalf("progress after apply=%+v, want 4/1", progress)
	}

	progress, err = fsm.WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		NodeID:          "node-a",
		GroupID:         "default",
		MinAppliedIndex: 2,
	})
	if !errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied) {
		t.Fatalf("WaitAppliedIndex ahead err=%v progress=%+v, want ErrReadBarrierNotSatisfied", err, progress)
	}
	if progress.Index != 1 {
		t.Fatalf("progress after unsatisfied barrier=%+v, want current index 1", progress)
	}

	_, err = fsm.WaitAppliedIndex(context.Background(), raftcluster.AppliedIndexReadBarrier{
		NodeID:          "node-b",
		GroupID:         "default",
		MinAppliedIndex: 1,
	})
	if !errors.Is(err, raftcluster.ErrReadBarrierTargetMismatch) {
		t.Fatalf("WaitAppliedIndex wrong node err=%v, want ErrReadBarrierTargetMismatch", err)
	}
}
