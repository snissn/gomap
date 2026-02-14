package db

import (
	"math"
	"testing"

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
