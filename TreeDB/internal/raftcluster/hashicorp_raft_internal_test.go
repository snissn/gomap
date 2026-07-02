package raftcluster

import (
	"context"
	"errors"
	"math"
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

func TestHashicorpRaftConfigPreservesConfiguredSnapshots(t *testing.T) {
	src := hraft.DefaultConfig()
	src.SnapshotInterval = time.Millisecond
	src.SnapshotThreshold = 1
	src.TrailingLogs = 3

	conf := hashicorpRaftConfig("node-a", src)
	if conf.SnapshotInterval < 5*time.Millisecond {
		t.Fatalf("SnapshotInterval=%s is invalid for hashicorp raft", conf.SnapshotInterval)
	}
	if conf.SnapshotThreshold != src.SnapshotThreshold {
		t.Fatalf("SnapshotThreshold=%d want configured %d", conf.SnapshotThreshold, src.SnapshotThreshold)
	}
	if conf.TrailingLogs != src.TrailingLogs {
		t.Fatalf("TrailingLogs=%d want configured %d", conf.TrailingLogs, src.TrailingLogs)
	}
	if err := hraft.ValidateConfig(conf); err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
}

func TestHashicorpRaftConfigDefaultsAvoidAutomaticSnapshots(t *testing.T) {
	conf := hashicorpRaftConfig("node-a", nil)
	if conf.SnapshotThreshold != ^uint64(0) {
		t.Fatalf("default SnapshotThreshold=%d want max uint64", conf.SnapshotThreshold)
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

func TestHashicorpRaftReadIndexGapStopsAtFirstCommand(t *testing.T) {
	inner := hraft.NewInmemStore()
	if err := inner.StoreLogs([]*hraft.Log{
		{Index: 2, Term: 1, Type: hraft.LogNoop},
		{Index: 3, Term: 1, Type: hraft.LogConfiguration},
		{Index: 4, Term: 1, Type: hraft.LogCommand, Data: []byte("first")},
		{Index: 5, Term: 1, Type: hraft.LogCommand, Data: []byte("second")},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	store := &countingReadIndexLogStore{LogStore: inner}
	provider := &HashicorpRaftProvider{logStore: store}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(2, 5)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if noCommands {
		t.Fatalf("readIndexGapHasNoCommands=true, want false for command gap")
	}
	if firstCmd != 4 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 4", firstCmd)
	}
	if store.getLogCount != 3 {
		t.Fatalf("GetLog count=%d, want scan through first command only", store.getLogCount)
	}
}

func TestHashicorpRaftReadIndexGapMissingLogFailsClosed(t *testing.T) {
	provider := &HashicorpRaftProvider{logStore: hraft.NewInmemStore()}

	_, _, err := provider.readIndexGapHasNoCommands(1, 1)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("readIndexGapHasNoCommands err=%v want ErrReadBarrierNotSatisfied", err)
	}
}

func TestHashicorpRaftReadIndexGapMaxIndexNoOverflow(t *testing.T) {
	store := hraft.NewInmemStore()
	lastIndex := uint64(math.MaxUint64)
	if err := store.StoreLog(&hraft.Log{Index: lastIndex, Term: 1, Type: hraft.LogNoop}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	counting := &countingReadIndexLogStore{LogStore: store}
	provider := &HashicorpRaftProvider{logStore: counting}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(lastIndex, lastIndex)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if !noCommands {
		t.Fatalf("readIndexGapHasNoCommands=false, want true")
	}
	if firstCmd != 0 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 0", firstCmd)
	}
	if counting.getLogCount != 1 {
		t.Fatalf("GetLog count=%d, want 1", counting.getLogCount)
	}
}

func TestHashicorpRaftReadIndexGapFirstIndexAfterAppliedMaxFailsClosed(t *testing.T) {
	_, err := readIndexGapFirstIndexAfterApplied(uint64(math.MaxUint64))
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("readIndexGapFirstIndexAfterApplied err=%v want ErrReadBarrierNotSatisfied", err)
	}

	firstIndex, err := readIndexGapFirstIndexAfterApplied(41)
	if err != nil {
		t.Fatalf("readIndexGapFirstIndexAfterApplied: %v", err)
	}
	if firstIndex != 42 {
		t.Fatalf("firstIndex=%d want 42", firstIndex)
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
	if !errors.Is(err, hraft.ErrLogNotFound) {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm err=%v want raft ErrLogNotFound", err)
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

type countingReadIndexLogStore struct {
	hraft.LogStore
	getLogCount int
}

func (s *countingReadIndexLogStore) GetLog(index uint64, log *hraft.Log) error {
	s.getLogCount++
	return s.LogStore.GetLog(index, log)
}
