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

func TestHashicorpRaftLeadershipLostIsCommitAmbiguous(t *testing.T) {
	err := mapHashicorpRaftApplyError(hraft.ErrLeadershipLost)
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("mapHashicorpRaftApplyError=%v want ErrCommitAmbiguous", err)
	}
	if errors.Is(err, ErrAdmissionUnavailable) {
		t.Fatalf("mapHashicorpRaftApplyError=%v should not be ErrAdmissionUnavailable", err)
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

func TestHashicorpRaftReadIndexGapHasNoCommands(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLogs([]*hraft.Log{
		{Index: 1, Term: 1, Type: hraft.LogNoop},
		{Index: 2, Term: 1, Type: hraft.LogConfiguration},
		{Index: 3, Term: 1, Type: hraft.LogBarrier},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(1, 3)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if !noCommands {
		t.Fatalf("readIndexGapHasNoCommands=false, want true for non-command gap")
	}
	if firstCmd != 0 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 0 for non-command gap", firstCmd)
	}
}

func TestHashicorpRaftReadIndexGapDetectsCommand(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLogs([]*hraft.Log{
		{Index: 1, Term: 1, Type: hraft.LogNoop},
		{Index: 2, Term: 1, Type: hraft.LogCommand, Data: []byte("entry")},
		{Index: 3, Term: 1, Type: hraft.LogBarrier},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(1, 3)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if noCommands {
		t.Fatalf("readIndexGapHasNoCommands=true, want false for command gap")
	}
	if firstCmd != 2 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 2", firstCmd)
	}
}

func TestHashicorpRaftReadIndexGapMissingLogFailsClosed(t *testing.T) {
	provider := &HashicorpRaftProvider{logStore: hraft.NewInmemStore()}

	_, _, err := provider.readIndexGapHasNoCommands(1, 1)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("readIndexGapHasNoCommands err=%v want ErrReadBarrierNotSatisfied", err)
	}
}

func TestHashicorpRaftReadIndexCommitIndexHasCurrentTerm(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLogs([]*hraft.Log{
		{Index: 1, Term: 1, Type: hraft.LogCommand},
		{Index: 2, Term: 2, Type: hraft.LogNoop},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	ok, err := provider.readIndexCommitIndexHasCurrentTerm(2, 2)
	if err != nil {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm current term: %v", err)
	}
	if !ok {
		t.Fatal("readIndexCommitIndexHasCurrentTerm=false, want true for current-term committed entry")
	}
}

func TestHashicorpRaftReadIndexCommitIndexPreviousTermNotEnough(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLog(&hraft.Log{Index: 1, Term: 1, Type: hraft.LogCommand}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	ok, err := provider.readIndexCommitIndexHasCurrentTerm(1, 2)
	if err != nil {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm previous term: %v", err)
	}
	if ok {
		t.Fatal("readIndexCommitIndexHasCurrentTerm=true, want false for previous-term committed prefix")
	}
}

func TestHashicorpRaftReadIndexCommitIndexMissingLogFailsClosed(t *testing.T) {
	provider := &HashicorpRaftProvider{logStore: hraft.NewInmemStore()}

	_, err := provider.readIndexCommitIndexHasCurrentTerm(2, 2)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm err=%v want ErrReadBarrierNotSatisfied", err)
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
