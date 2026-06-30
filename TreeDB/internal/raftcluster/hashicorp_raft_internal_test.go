package raftcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestHashicorpRaftApplyTimeoutIsCommitAmbiguous(t *testing.T) {
	err := mapHashicorpRaftApplyError(context.DeadlineExceeded)
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("mapHashicorpRaftApplyError=%v want ErrCommitAmbiguous", err)
	}
	if errors.Is(err, ErrHashicorpRaftUnavailable) {
		t.Fatalf("mapHashicorpRaftApplyError=%v should not be ErrHashicorpRaftUnavailable", err)
	}
}

func TestHashicorpRaftConfigSuppressesSnapshots(t *testing.T) {
	src := hraft.DefaultConfig()
	src.SnapshotInterval = time.Millisecond
	src.SnapshotThreshold = 1

	conf := hashicorpRaftConfig("node-a", src)
	if conf.SnapshotInterval < 5*time.Millisecond {
		t.Fatalf("SnapshotInterval=%s is invalid for hashicorp raft", conf.SnapshotInterval)
	}
	if conf.SnapshotThreshold != ^uint64(0) {
		t.Fatalf("SnapshotThreshold=%d want max uint64", conf.SnapshotThreshold)
	}
	if err := hraft.ValidateConfig(conf); err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
}

func TestHashicorpRaftFSMApplyFailureDefaultPanics(t *testing.T) {
	applyErr := errors.New("command WAL fsync failed")
	fsm := hashicorpRaftFSM{
		groupID: "group-a",
		applier: CommittedCommandApplierFunc(func(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
			return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusRecoveryRequired}, applyErr
		}),
	}
	payload, err := encodeHashicorpRaftCommandEntryV1(CommitCommandEntryV1Request{
		GroupID:    "group-a",
		NodeID:     "node-a",
		EntryBytes: []byte("deterministic-entry"),
	}, ResolvedConfig{GroupID: "group-a", NodeID: "node-a"})
	if err != nil {
		t.Fatalf("encodeHashicorpRaftCommandEntryV1: %v", err)
	}

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Apply did not panic")
		}
		err, ok := recovered.(error)
		if !ok {
			t.Fatalf("panic=%T %[1]v, want error", recovered)
		}
		if !errors.Is(err, ErrHashicorpRaftLogEntry) || !errors.Is(err, applyErr) {
			t.Fatalf("panic error=%v want ErrHashicorpRaftLogEntry and apply error", err)
		}
	}()

	_ = fsm.Apply(&hraft.Log{Type: hraft.LogCommand, Term: 1, Index: 1, Data: payload})
}
