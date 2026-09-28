package raftcluster

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestReadIndexSnapshotGapCachesOnlyCurrentInstalledBoundaryV1(t *testing.T) {
	cache := &readIndexSnapshotGapV1{}
	progress := AppliedProgress{Term: 2, Index: 5, HasApplied: true}
	index, term := uint64(7), uint64(2)
	loads := 0
	current := func() (uint64, uint64) { return index, term }
	verify := func(context.Context) (readIndexSnapshotGapProofV1, error) {
		loads++
		return readIndexSnapshotGapProofV1{commandTerm: 2, commandIndex: 5, nativeTerm: term, nativeIndex: index}, nil
	}
	for i := 0; i < 3; i++ {
		proof, err := cache.get(context.Background(), progress, 6, current, verify)
		if err != nil || proof.nativeIndex != 7 {
			t.Fatalf("cached proof %d: %+v %v", i, proof, err)
		}
	}
	if loads != 1 {
		t.Fatalf("verification calls=%d, want one for repeated read fences", loads)
	}
	index = 8
	proof, err := cache.get(context.Background(), progress, 6, current, verify)
	if err != nil || proof.nativeIndex != 8 || loads != 2 {
		t.Fatalf("changed installed boundary: proof=%+v calls=%d err=%v", proof, loads, err)
	}
	cache.invalidate()
	if _, err := cache.get(context.Background(), progress, 6, current, verify); err != nil || loads != 3 {
		t.Fatalf("explicit Restore/Persist invalidation: calls=%d err=%v", loads, err)
	}
	if _, err := cache.get(context.Background(), AppliedProgress{Term: 2, Index: 4, HasApplied: true}, 6, current, verify); !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("changed durable command boundary accepted: %v", err)
	}
	if _, err := cache.get(context.Background(), progress, 9, current, verify); !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("log beyond installed snapshot boundary accepted: %v", err)
	}
}

type restoringReadIndexGapApplierV1 struct{ restored string }

func (*restoringReadIndexGapApplierV1) ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	return raftentry.ApplyResultV1{}, nil
}

func (a *restoringReadIndexGapApplierV1) InstallRaftSnapshotV1(src io.Reader) error {
	data, err := io.ReadAll(src)
	a.restored = string(data)
	return err
}

func TestReadIndexSnapshotGapRestoreInvalidatesCachedProofV1(t *testing.T) {
	cache := &readIndexSnapshotGapV1{proof: readIndexSnapshotGapProofV1{commandTerm: 2, commandIndex: 5, nativeTerm: 2, nativeIndex: 7}}
	applier := &restoringReadIndexGapApplierV1{}
	fsm := hashicorpRaftFSM{applier: applier, snapshotGap: cache}
	if err := fsm.Restore(io.NopCloser(strings.NewReader("replacement snapshot"))); err != nil {
		t.Fatal(err)
	}
	if applier.restored != "replacement snapshot" || cache.proof != (readIndexSnapshotGapProofV1{}) || cache.generation != 2 {
		t.Fatalf("Restore retained stale gap proof: payload=%q proof=%+v generation=%d", applier.restored, cache.proof, cache.generation)
	}
}

func TestReadIndexSnapshotGapRejectsMissingRetainedLogV1(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLogs([]*hraft.Log{
		{Index: 6, Term: 2, Type: hraft.LogConfiguration},
		{Index: 8, Term: 2, Type: hraft.LogNoop},
	}); err != nil {
		t.Fatal(err)
	}
	provider := &HashicorpRaftProvider{logStore: store, snapshotGap: &readIndexSnapshotGapV1{}}
	_, _, err := provider.readIndexGapHasNoCommandsWithProgress(context.Background(), AppliedProgress{Term: 2, Index: 5, HasApplied: true}, 6, 8)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("interior missing log accepted: %v", err)
	}
}

func TestReadIndexSnapshotGapStopsAtNextRetainedCommandV1(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLog(&hraft.Log{Index: 8, Term: 2, Type: hraft.LogCommand}); err != nil {
		t.Fatal(err)
	}
	provider := &HashicorpRaftProvider{logStore: store}
	noCommands, firstCommand, err := provider.readIndexGapHasNoCommandsWithProgress(context.Background(), AppliedProgress{Term: 2, Index: 7, HasApplied: true}, 8, 8)
	if err != nil || noCommands || firstCommand != 8 {
		t.Fatalf("command after compacted snapshot was skipped: noCommands=%v first=%d err=%v", noCommands, firstCommand, err)
	}
}
