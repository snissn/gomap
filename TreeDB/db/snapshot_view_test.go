package db

import (
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
)

func TestAcquireSnapshot_UsesPublishedCoherentView(t *testing.T) {
	idx1 := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx1.refs.Store(1)
	idx2 := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx2.refs.Store(1)

	state1 := &DBState{CommitSeq: 101, RootPageID: 11}
	state2 := &DBState{CommitSeq: 202, RootPageID: 22}

	db := &DB{snapPool: NewSnapshotPool()}
	db.idx.Store(idx1)
	db.state.Store(state1)
	db.publishSnapshotView(idx1, state1, nil)

	// Simulate an in-flight raw idx/state update without publishing a new view.
	db.idx.Store(idx2)
	db.state.Store(state2)

	snap1 := db.AcquireSnapshot()
	if snap1 == nil {
		t.Fatal("expected snapshot from published view")
	}
	if snap1.idx != idx1 {
		t.Fatalf("expected idx1 from published view, got %p want %p", snap1.idx, idx1)
	}
	if snap1.state != state1 {
		t.Fatalf("expected state1 from published view, got %+v want %+v", snap1.state, state1)
	}
	if min := idx1.registry.MinPinnedSeq(); min != state1.CommitSeq {
		t.Fatalf("expected idx1 min pinned %d, got %d", state1.CommitSeq, min)
	}
	if min := idx2.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected idx2 to remain unpinned, got %d", min)
	}
	if err := snap1.Close(); err != nil {
		t.Fatalf("close snapshot1: %v", err)
	}
	if min := idx1.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected idx1 unpinned after close, got %d", min)
	}

	// Flip published snapshot metadata to idx2, then make idx1 the live index so
	// idx2 reads are stale and must use registry pinning.
	db.publishSnapshotView(idx2, state2, nil)
	db.idx.Store(idx1)

	snap2 := db.AcquireSnapshot()
	if snap2 == nil {
		t.Fatal("expected snapshot from updated published view")
	}
	if snap2.idx != idx2 {
		t.Fatalf("expected idx2 from updated published view, got %p want %p", snap2.idx, idx2)
	}
	if snap2.state != state2 {
		t.Fatalf("expected state2 from updated published view, got %+v want %+v", snap2.state, state2)
	}
	if min := idx2.registry.MinPinnedSeq(); min != state2.CommitSeq {
		t.Fatalf("expected idx2 min pinned %d, got %d", state2.CommitSeq, min)
	}
	if err := snap2.Close(); err != nil {
		t.Fatalf("close snapshot2: %v", err)
	}
	if min := idx2.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected idx2 unpinned after close, got %d", min)
	}
}

func TestAcquireSnapshot_ReleasesPinnedValueLogSetOnRegistryNil(t *testing.T) {
	idx := &indexGen{}
	idx.refs.Store(1)

	seg := &valuelog.File{}
	seg.RefCount.Store(1)
	set := &valuelog.Set{
		Files: map[uint32]*valuelog.File{
			1: seg,
		},
	}
	state := &DBState{
		CommitSeq:   1,
		RootPageID:  1,
		ValueLogSet: set,
	}
	vm := &valuelog.Manager{}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, vm)

	if snap := db.AcquireSnapshot(); snap != nil {
		t.Fatal("expected nil snapshot when registry is unavailable")
	}
	if got := set.RefCount.Load(); got != 0 {
		t.Fatalf("expected balanced value-log set pin count, got %d", got)
	}
	if got := seg.RefCount.Load(); got != 0 {
		t.Fatalf("expected balanced value-log file pin count, got %d", got)
	}
}

func TestAcquireSnapshot_ReturnsNilWhenDBIsClosing(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	state := &DBState{CommitSeq: 7, RootPageID: 1}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, nil)
	db.closing.Store(true)

	if snap := db.AcquireSnapshot(); snap != nil {
		t.Fatal("expected nil snapshot while close is in progress")
	}
	if got := db.snapshotAcquireInFlight(); got != 0 {
		t.Fatalf("expected no in-flight acquisitions after early return, got %d", got)
	}
	if min := idx.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected registry to remain unpinned, got %d", min)
	}
}

func TestAcquireSnapshot_PinsLeafGenerationView(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	state := &DBState{
		CommitSeq:  1,
		RootPageID: 1,
		LeafGenerations: &leafGenerationView{
			CurrentGenerationID: 2,
			GenerationOrder:     []uint64{1, 2},
			Generations: map[uint64]leafGenerationViewGeneration{
				1: {State: leafGenerationStateSealed, FileIDs: []uint32{111}},
				2: {State: leafGenerationStateWritable, FileIDs: []uint32{222}},
			},
			FileToGeneration: map[uint32]uint64{111: 1, 222: 2},
		},
	}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, nil)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if got, want := len(snap.leafGenerationIDs), 2; got != want {
		t.Fatalf("len(leafGenerationIDs)=%d, want %d", got, want)
	}
	if got, want := db.leafGenerationPinCountForTesting(1), uint64(1); got != want {
		t.Fatalf("pin count for generation 1=%d, want %d", got, want)
	}
	if got, want := db.leafGenerationPinCountForTesting(2), uint64(1); got != want {
		t.Fatalf("pin count for generation 2=%d, want %d", got, want)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		t.Fatalf("pin count for generation 1 after close=%d, want 0", got)
	}
	if got := db.leafGenerationPinCountForTesting(2); got != 0 {
		t.Fatalf("pin count for generation 2 after close=%d, want 0", got)
	}
}

func TestAcquireSnapshot_DoesNotLeakLeafGenerationPinsOnRegistryNil(t *testing.T) {
	idx := &indexGen{}
	idx.refs.Store(1)
	state := &DBState{
		CommitSeq:  1,
		RootPageID: 1,
		LeafGenerations: &leafGenerationView{
			CurrentGenerationID: 1,
			GenerationOrder:     []uint64{1},
			Generations: map[uint64]leafGenerationViewGeneration{
				1: {State: leafGenerationStateWritable, FileIDs: []uint32{111}},
			},
			FileToGeneration: map[uint32]uint64{111: 1},
		},
	}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, nil)

	if snap := db.AcquireSnapshot(); snap != nil {
		t.Fatal("expected nil snapshot when registry is unavailable")
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		t.Fatalf("pin count for generation 1=%d, want 0", got)
	}
}

func TestLeafGenerationPinTracker_PrunesInactiveZeroCountRefs(t *testing.T) {
	var tracker leafGenerationPinTracker

	refs := tracker.refsForGenerationIDs([]uint64{1, 2, 3})
	tracker.pinRefs(refs)
	tracker.unpinRefs(refs)
	tracker.pruneInactiveGenerationIDs([]uint64{1})

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	if _, ok := tracker.refs[1]; !ok {
		t.Fatalf("expected active generation ref to be retained")
	}
	if _, ok := tracker.refs[2]; ok {
		t.Fatalf("expected inactive zero-count ref for generation 2 to be pruned")
	}
	if _, ok := tracker.refs[3]; ok {
		t.Fatalf("expected inactive zero-count ref for generation 3 to be pruned")
	}
}
